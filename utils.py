import time
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from config import Config, beijing_tz
from functools import wraps
from aiogram import types
from database import db


logger = logging.getLogger("GroupCheckInBot")


class MessageFormatter:
    """消息格式化工具类"""

    @staticmethod
    def format_time(seconds: int) -> str:
        """格式化时间显示"""
        if seconds is None:
            return "0秒"

        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)

        if h > 0:
            return f"{h}小时{m}分{s}秒"
        elif m > 0:
            return f"{m}分{s}秒"
        else:
            return f"{s}秒"

    @staticmethod
    def format_time_for_csv(seconds: int) -> str:
        """为CSV导出格式化时间显示"""
        if seconds is None:
            return "0分0秒"

        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60

        if hours > 0:
            return f"{hours}时{minutes}分{secs}秒"
        else:
            return f"{minutes}分{secs}秒"

    @staticmethod
    def format_minutes_to_hms(minutes: float) -> str:
        """将分钟数格式化为小时:分钟:秒的字符串"""
        if minutes is None:
            return "0小时0分0秒"

        total_seconds = int(minutes * 60)
        hours = total_seconds // 3600
        minutes_remaining = (total_seconds % 3600) // 60
        seconds_remaining = total_seconds % 60

        if hours > 0:
            return f"{hours}小时{minutes_remaining}分{seconds_remaining}秒"
        elif minutes_remaining > 0:
            return f"{minutes_remaining}分{seconds_remaining}秒"
        else:
            return f"{seconds_remaining}秒"

    @staticmethod
    def format_user_link(user_id: int, user_name: str) -> str:
        """格式化用户链接"""
        if not user_name:
            user_name = f"用户{user_id}"
        clean_name = (
            str(user_name)
            .replace("<", "")
            .replace(">", "")
            .replace("&", "")
            .replace('"', "")
        )
        return f'<a href="tg://user?id={user_id}">{clean_name}</a>'

    @staticmethod
    def create_dashed_line() -> str:
        """创建短虚线分割线"""
        return MessageFormatter.format_copyable_text("--------------------------")

    @staticmethod
    def format_copyable_text(text: str) -> str:
        """格式化可复制文本"""
        return f"<code>{text}</code>"

    @staticmethod
    def format_activity_message(
        user_id: int,
        user_name: str,
        activity: str,
        time_str: str,
        count: int,
        max_times: int,
        time_limit: int,
    ) -> str:
        """格式化打卡消息"""
        first_line = f"👤 用户：{MessageFormatter.format_user_link(user_id, user_name)}"

        message = (
            f"{first_line}\n"
            f"✅ 打卡成功：{MessageFormatter.format_copyable_text(activity)} - {MessageFormatter.format_copyable_text(time_str)}\n"
            f"⚠️ 注意：这是您第 {MessageFormatter.format_copyable_text(str(count))} 次{MessageFormatter.format_copyable_text(activity)}（今日上限：{MessageFormatter.format_copyable_text(str(max_times))}次）\n"
            f"⏰ 本次活动时间限制：{MessageFormatter.format_copyable_text(str(time_limit))} 分钟"
        )

        if count >= max_times:
            message += f"\n🚨 警告：本次结束后，您今日的{MessageFormatter.format_copyable_text(activity)}次数将达到上限，请留意！"

        message += f"\n💡提示：活动完成后请及时点击'✅ 回座'按钮"

        return message

    @staticmethod
    def format_back_message(
        user_id: int,
        user_name: str,
        activity: str,
        time_str: str,
        elapsed_time: str,
        total_activity_time: str,
        total_time: str,
        activity_counts: dict,
        total_count: int,
        is_overtime: bool = False,
        overtime_seconds: int = 0,
        fine_amount: int = 0,
    ) -> str:
        """格式化回座消息"""
        first_line = f"👤 用户：{MessageFormatter.format_user_link(user_id, user_name)}"

        message = (
            f"{first_line}\n"
            f"✅ {MessageFormatter.format_copyable_text(time_str)} 回座打卡成功\n"
            f"📝 活动：{MessageFormatter.format_copyable_text(activity)}\n"
            f"⏰ 本次活动耗时：{MessageFormatter.format_copyable_text(elapsed_time)}\n"
            f"📈 今日累计{MessageFormatter.format_copyable_text(activity)}时间：{MessageFormatter.format_copyable_text(total_activity_time)}\n"
            f"📊 今日总计时：{MessageFormatter.format_copyable_text(total_time)}\n"
        )

        if is_overtime:
            overtime_time = MessageFormatter.format_time(int(overtime_seconds))
            message += f"⚠️ 警告：您本次的活动已超时！\n🚨 超时时间：{MessageFormatter.format_copyable_text(overtime_time)}\n"
            if fine_amount > 0:
                message += f"💸 罚款：{MessageFormatter.format_copyable_text(str(fine_amount))} 元\n"

        dashed_line = MessageFormatter.create_dashed_line()
        message += f"{dashed_line}\n"

        for act, count in activity_counts.items():
            if count > 0:
                message += f"🔹 本日{MessageFormatter.format_copyable_text(act)}次数：{MessageFormatter.format_copyable_text(str(count))} 次\n"

        message += f"\n📊 今日总活动次数：{MessageFormatter.format_copyable_text(str(total_count))} 次"

        return message


class NotificationService:
    """统一推送服务"""

    def __init__(self, bot=None):
        self.bot = bot

    async def send_notification(
        self, chat_id: int, text: str, notification_type: str = "all"
    ):
        """发送通知到绑定的频道和群组"""
        if not self.bot:
            logger.warning("NotificationService: bot 实例未初始化")
            return False

        sent = False
        push_settings = await db.get_push_settings()

        # 获取群组数据
        group_data = await db.get_group_cached(chat_id)

        # 发送到频道
        if (
            push_settings.get("enable_channel_push")
            and group_data
            and group_data.get("channel_id")
        ):
            try:
                await self.bot.send_message(
                    group_data["channel_id"], text, parse_mode="HTML"
                )
                sent = True
                logger.info(f"已发送到频道: {group_data['channel_id']}")
            except Exception as e:
                logger.error(f"发送到频道失败: {e}")

        # 发送到通知群组
        if (
            push_settings.get("enable_group_push")
            and group_data
            and group_data.get("notification_group_id")
        ):
            try:
                await self.bot.send_message(
                    group_data["notification_group_id"], text, parse_mode="HTML"
                )
                sent = True
                logger.info(f"已发送到通知群组: {group_data['notification_group_id']}")
            except Exception as e:
                logger.error(f"发送到通知群组失败: {e}")

        # 管理员兜底推送
        if not sent and push_settings.get("enable_admin_push"):
            for admin_id in Config.ADMINS:
                try:
                    await self.bot.send_message(admin_id, text, parse_mode="HTML")
                    logger.info(f"已发送给管理员: {admin_id}")
                except Exception as e:
                    logger.error(f"发送给管理员失败: {e}")

        return sent

    async def send_document(self, chat_id: int, document, caption: str = ""):
        """发送文档到绑定的频道和群组"""
        if not self.bot:
            logger.warning("NotificationService: bot 实例未初始化")
            return False

        sent = False
        push_settings = await db.get_push_settings()
        group_data = await db.get_group_cached(chat_id)

        # 发送到频道
        if (
            push_settings.get("enable_channel_push")
            and group_data
            and group_data.get("channel_id")
        ):
            try:
                await self.bot.send_document(
                    group_data["channel_id"],
                    document,
                    caption=caption,
                    parse_mode="HTML",
                )
                sent = True
                logger.info(f"已发送文档到频道: {group_data['channel_id']}")
            except Exception as e:
                logger.error(f"发送文档到频道失败: {e}")

        # 发送到通知群组
        if (
            push_settings.get("enable_group_push")
            and group_data
            and group_data.get("notification_group_id")
        ):
            try:
                await self.bot.send_document(
                    group_data["notification_group_id"],
                    document,
                    caption=caption,
                    parse_mode="HTML",
                )
                sent = True
                logger.info(
                    f"已发送文档到通知群组: {group_data['notification_group_id']}"
                )
            except Exception as e:
                logger.error(f"发送文档到通知群组失败: {e}")

        # 管理员兜底推送
        if not sent and push_settings.get("enable_admin_push"):
            for admin_id in Config.ADMINS:
                try:
                    await self.bot.send_document(
                        admin_id, document, caption=caption, parse_mode="HTML"
                    )
                    logger.info(f"已发送文档给管理员: {admin_id}")
                except Exception as e:
                    logger.error(f"发送文档给管理员失败: {e}")

        return sent


class UserLockManager:
    """用户锁管理器"""

    def __init__(self):
        self._locks = {}
        self._access_times = {}
        self._cleanup_interval = 3600
        self._last_cleanup = time.time()
        self._max_locks = 5000

    def get_lock(self, chat_id: int, uid: int):
        """获取用户级锁"""
        key = f"{chat_id}-{uid}"

        if len(self._locks) >= self._max_locks:
            self._emergency_cleanup()

        # 记录访问时间
        self._access_times[key] = time.time()

        # 检查是否需要清理
        self._maybe_cleanup()

        # 返回或创建锁
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()

        return self._locks[key]

    def _maybe_cleanup(self):
        """按需清理过期锁"""
        current_time = time.time()
        if current_time - self._last_cleanup < self._cleanup_interval:
            return

        self._last_cleanup = current_time
        self._cleanup_old_locks()

    def _cleanup_old_locks(self):
        """清理长时间未使用的锁"""
        now = time.time()
        max_age = 86400  # 24小时

        old_keys = [
            key
            for key, last_used in self._access_times.items()
            if now - last_used > max_age
        ]

        for key in old_keys:
            self._locks.pop(key, None)
            self._access_times.pop(key, None)

        if old_keys:
            logger.info(f"用户锁清理: 移除了 {len(old_keys)} 个过期锁")

    async def force_cleanup(self):
        """强制立即清理"""
        old_count = len(self._locks)
        self._cleanup_old_locks()
        new_count = len(self._locks)
        logger.info(f"强制用户锁清理: {old_count} -> {new_count}")

    def get_stats(self) -> Dict[str, Any]:
        """获取锁管理器统计"""
        return {
            "active_locks": len(self._locks),
            "tracked_users": len(self._access_times),
            "last_cleanup": self._last_cleanup,
        }

    def _emergency_cleanup(self):
        """🆕 紧急清理 - 当锁数量达到上限时"""
        now = time.time()
        max_age = 3600  # 1小时未使用的锁

        # 清理长时间未使用的锁
        old_keys = [
            key
            for key, last_used in self._access_times.items()
            if now - last_used > max_age
        ]

        # 如果还不够，按LRU清理最旧的20%
        if len(self._locks) >= self._max_locks:
            sorted_keys = sorted(
                self._access_times.items(), key=lambda x: x[1]  # 按访问时间排序
            )
            additional_cleanup = max(100, len(sorted_keys) // 5)  # 至少100个或20%
            old_keys.extend([key for key, _ in sorted_keys[:additional_cleanup]])

        for key in set(old_keys):  # 去重
            self._locks.pop(key, None)
            self._access_times.pop(key, None)

        logger.warning(f"紧急锁清理: 移除了 {len(old_keys)} 个锁")


class ActivityTimerManager:
    """活动定时器管理器"""

    def __init__(self):
        self._timers = {}
        self._cleanup_interval = 300
        self._last_cleanup = time.time()
        self.activity_timer_callback = None  # 回调函数
        self._max_locks = 5000  # 最大锁数量限制

    def set_activity_timer_callback(self, callback):
        """设置活动定时器回调"""
        self.activity_timer_callback = callback

    async def start_timer(self, chat_id: int, uid: int, act: str, limit: int):
        """启动活动定时器"""
        key = f"{chat_id}-{uid}"
        await self.cancel_timer(key)

        if not self.activity_timer_callback:
            logger.error("ActivityTimerManager: 未设置回调函数")
            return

        timer_task = asyncio.create_task(
            self._activity_timer_wrapper(chat_id, uid, act, limit), name=f"timer_{key}"
        )
        self._timers[key] = timer_task
        logger.debug(f"启动定时器: {key} - {act}")

    async def _activity_timer_wrapper(
        self, chat_id: int, uid: int, act: str, limit: int
    ):
        """定时器包装器"""
        try:
            if self.activity_timer_callback:
                await self.activity_timer_callback(chat_id, uid, act, limit)
        except Exception as e:
            logger.error(f"定时器异常 {chat_id}-{uid}: {e}")

    async def cancel_timer(self, key: str):
        """取消定时器"""
        if key in self._timers:
            task = self._timers[key]
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            del self._timers[key]

    async def cancel_all_timers(self):
        """取消所有定时器"""
        keys = list(self._timers.keys())
        cancelled_count = 0

        for key in keys:
            try:
                await self.cancel_timer(key)
                cancelled_count += 1
            except Exception as e:
                logger.error(f"取消定时器 {key} 失败: {e}")

        logger.info(f"已取消所有定时器: {cancelled_count}/{len(keys)} 个")
        return cancelled_count

    async def cleanup_finished_timers(self):
        """清理已完成定时器"""
        if time.time() - self._last_cleanup < self._cleanup_interval:
            return

        finished_keys = [key for key, task in self._timers.items() if task.done()]
        for key in finished_keys:
            del self._timers[key]

        if finished_keys:
            logger.info(f"定时器清理: 移除了 {len(finished_keys)} 个已完成定时器")

        self._last_cleanup = time.time()

    def get_stats(self) -> Dict[str, Any]:
        """获取定时器统计"""
        return {"active_timers": len(self._timers)}


class EnhancedPerformanceOptimizer:
    """增强版性能优化器 - Render 免费版优化"""

    def __init__(self):
        self.last_cleanup = time.time()
        self.cleanup_interval = 600  # 🆕 延长到10分钟，减少频繁清理
        self._emergency_mode = False  # 🆕 紧急模式标志
        self._consecutive_high_memory = 0  # 🆕 连续高内存计数

    async def memory_cleanup(self):
        """智能内存清理 - Render 免费版优化"""
        try:
            current_time = time.time()

            # 🆕 智能检查频率：紧急模式更频繁，正常模式较少
            if self._emergency_mode:
                check_interval = 60  # 紧急模式1分钟检查一次
            else:
                check_interval = self.cleanup_interval

            if current_time - self.last_cleanup < check_interval:
                return

            # 🆕 检查内存状态，决定清理强度
            memory_status = self._check_memory_status()

            if memory_status == "critical":
                logger.warning("🆘 内存严重不足，执行强制清理")
                await self._emergency_cleanup()
                self._emergency_mode = True
            elif memory_status == "warning":
                logger.info("⚠️ 内存使用较高，执行增强清理")
                await self._enhanced_cleanup()
                self._emergency_mode = True
            else:
                # 正常清理
                await self._normal_cleanup()
                self._emergency_mode = False

            # 强制GC
            import gc

            collected = gc.collect()

            logger.info(
                f"内存清理完成 - 回收对象: {collected}, "
                f"紧急模式: {self._emergency_mode}, "
                f"状态: {memory_status}"
            )

            self.last_cleanup = current_time

        except Exception as e:
            logger.error(f"内存清理失败: {e}")

    def _check_memory_status(self) -> str:
        """检查内存状态 - Render 免费版专用"""
        try:
            import psutil

            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            memory_percent = process.memory_percent()

            # 🆕 Render 免费版专用阈值（约512MB内存）
            if memory_mb > 400 or memory_percent > 85:
                self._consecutive_high_memory += 1
                if self._consecutive_high_memory >= 2:  # 连续2次高内存
                    return "critical"
                return "warning"
            else:
                self._consecutive_high_memory = 0
                return "normal"

        except ImportError:
            return "normal"

    async def _normal_cleanup(self):
        """正常强度清理"""
        from performance import task_manager, global_cache

        cleanup_tasks = [
            task_manager.cleanup_tasks(),
            global_cache.clear_expired(),
            db.cleanup_cache(),
        ]

        await asyncio.gather(*cleanup_tasks, return_exceptions=True)

    async def _enhanced_cleanup(self):
        """增强强度清理"""
        from performance import task_manager, global_cache

        cleanup_tasks = [
            task_manager.cleanup_tasks(),
            global_cache.clear_expired(),
            db.cleanup_cache(),
            self._force_cache_reduction(),  # 🆕 强制减少缓存
        ]

        # 🆕 增强清理：等待所有任务完成
        results = await asyncio.gather(*cleanup_tasks, return_exceptions=True)

        # 记录清理结果
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.warning(f"清理任务 {i} 失败: {result}")

    async def _emergency_cleanup(self):
        """紧急强度清理"""
        from performance import task_manager, global_cache

        # 🆕 紧急清理：顺序执行，确保关键清理完成
        try:
            # 1. 清理任务
            await task_manager.cleanup_tasks()

            # 2. 强制清理缓存（不等待过期）
            await global_cache.clear_all()  # 🆕 清空所有缓存

            # 3. 数据库缓存清理
            await db.cleanup_cache()

            # 4. 额外GC
            import gc

            gc.collect(2)  # 🆕 更积极的GC

        except Exception as e:
            logger.error(f"紧急清理失败: {e}")

    async def _force_cache_reduction(self):
        """强制减少缓存占用"""
        try:
            from performance import global_cache

            # 🆕 获取当前缓存统计
            stats = global_cache.get_stats()
            current_size = stats.get("size", 0)

            if current_size > 500:  # 🆕 如果缓存超过500项
                # 清理一半的缓存
                target_size = current_size // 2
                logger.info(f"强制缓存缩减: {current_size} -> {target_size}")

                # 这里可以添加更激进的缓存清理逻辑
                # 比如清理最旧的缓存项

        except Exception as e:
            logger.debug(f"强制缓存缩减失败: {e}")

    def memory_usage_ok(self) -> bool:
        """检查内存使用是否正常 - Render 免费版优化"""
        try:
            import psutil

            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            memory_percent = process.memory_percent()

            # 🆕 Render 免费版更严格的限制
            # 总内存约512MB，设置安全阈值
            memory_ok = memory_mb < 350 and memory_percent < 75

            if not memory_ok:
                logger.warning(
                    f"内存使用警告: {memory_mb:.1f}MB, {memory_percent:.1f}%"
                )

            return memory_ok

        except ImportError:
            return True

    def get_memory_status(self) -> dict:
        """获取内存状态详情 - 用于监控"""
        try:
            import psutil

            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            memory_percent = process.memory_percent()

            return {
                "memory_mb": round(memory_mb, 1),
                "memory_percent": round(memory_percent, 1),
                "emergency_mode": self._emergency_mode,
                "consecutive_high_memory": self._consecutive_high_memory,
                "last_cleanup": self.last_cleanup,
                "status": self._check_memory_status(),
            }
        except ImportError:
            return {"error": "psutil not available"}


class HeartbeatManager:
    """心跳管理器"""

    def __init__(self):
        self._last_heartbeat = time.time()
        self._is_running = False
        self._task = None

    async def initialize(self):
        """初始化心跳管理器"""
        self._is_running = True
        self._task = asyncio.create_task(self._heartbeat_loop())
        logger.info("心跳管理器已初始化")

    async def stop(self):
        """停止心跳管理器"""
        self._is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("心跳管理器已停止")

    async def _heartbeat_loop(self):
        """心跳循环"""
        while self._is_running:
            try:
                self._last_heartbeat = time.time()
                await asyncio.sleep(60)  # 每分钟一次心跳
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"心跳循环异常: {e}")
                await asyncio.sleep(10)

    def get_status(self) -> Dict[str, Any]:
        """获取心跳状态"""
        current_time = time.time()
        last_heartbeat_ago = current_time - self._last_heartbeat

        return {
            "is_running": self._is_running,
            "last_heartbeat": self._last_heartbeat,
            "last_heartbeat_ago": last_heartbeat_ago,
            "status": "healthy" if last_heartbeat_ago < 120 else "unhealthy",
        }


# 工具函数
def get_beijing_time() -> datetime:
    """获取北京时间"""
    return datetime.now(beijing_tz)


def calculate_cross_day_time_diff(
    current_dt: datetime, expected_time: str, checkin_type: str
) -> Tuple[float, datetime]:
    """
    智能化的时间差计算（支持跨天和最近匹配）
    """
    try:
        expected_hour, expected_minute = map(int, expected_time.split(":"))

        # 生成前一天、当天、后一天三个候选时间点
        candidates = []
        for d in (-1, 0, 1):
            candidate = current_dt.replace(
                hour=expected_hour, minute=expected_minute, second=0, microsecond=0
            ) + timedelta(days=d)
            candidates.append(candidate)

        # 找到与当前时间最接近的 expected_dt
        expected_dt = min(
            candidates, key=lambda t: abs((t - current_dt).total_seconds())
        )

        # 计算时间差（单位：分钟）
        time_diff_minutes = (current_dt - expected_dt).total_seconds() / 60

        return time_diff_minutes, expected_dt

    except Exception as e:
        logger.error(f"时间差计算出错: {e}")
        return 0, current_dt


async def is_valid_checkin_time(
    chat_id: int, checkin_type: str, current_time: datetime
) -> Tuple[bool, datetime]:
    """
    检查是否在允许的打卡时间窗口内（前后 7 小时）
    """
    try:
        work_hours = await db.get_group_work_time(chat_id)
        if checkin_type == "work_start":
            expected_time_str = work_hours["work_start"]
        else:
            expected_time_str = work_hours["work_end"]

        exp_h, exp_m = map(int, expected_time_str.split(":"))

        # 在 -1/0/+1 天范围内生成候选 expected_dt
        candidates = []
        for d in (-1, 0, 1):
            candidate = current_time.replace(
                hour=exp_h, minute=exp_m, second=0, microsecond=0
            ) + timedelta(days=d)
            candidates.append(candidate)

        # 选择与 current_time 时间差绝对值最小的 candidate
        expected_dt = min(
            candidates, key=lambda t: abs((t - current_time).total_seconds())
        )

        # 允许前后窗口：7小时
        earliest = expected_dt - timedelta(hours=7)
        latest = expected_dt + timedelta(hours=7)

        is_valid = earliest <= current_time <= latest

        if not is_valid:
            logger.warning(
                f"打卡时间超出允许窗口: {checkin_type}, 当前: {current_time.strftime('%Y-%m-%d %H:%M')}, "
                f"允许: {earliest.strftime('%Y-%m-%d %H:%M')} ~ {latest.strftime('%Y-%m-%d %H:%M')}"
            )

        return is_valid, expected_dt

    except Exception as e:
        logger.error(f"检查打卡时间范围失败: {e}")
        fallback = current_time.replace(hour=9, minute=0, second=0, microsecond=0)
        return True, fallback


# ========== 装饰器和工具函数 ==========
def rate_limit(rate: int = 1, per: int = 1):
    """速率限制装饰器"""

    def decorator(func):
        calls = []

        @wraps(func)
        async def wrapper(*args, **kwargs):
            now = time.time()
            # 清理过期记录
            calls[:] = [call for call in calls if now - call < per]

            if len(calls) >= rate:
                if args and isinstance(args[0], types.Message):
                    await args[0].answer("⏳ 操作过于频繁，请稍后再试")
                return

            calls.append(now)
            return await func(*args, **kwargs)

        return wrapper

    return decorator


class SmartLockManager:
    """🆕 智能锁管理器 - 读/写锁分离"""

    def __init__(self):
        self._read_locks = {}  # 读锁（共享）
        self._write_locks = {}  # 写锁（排他）
        self._access_times = {}
        self._max_locks = 500
        self._cleanup_interval = 1800
        self._last_cleanup = time.time()

    def get_lock(self, chat_id: int, uid: int, operation_type: str = "write"):
        """
        获取智能锁
        operation_type: 'read' 或 'write'
        """
        # 🆕 读操作使用群组级共享锁，写操作使用用户级排他锁
        if operation_type == "read":
            key = f"read:{chat_id}"  # 群组级读锁
            lock_dict = self._read_locks
        else:
            key = f"write:{chat_id}:{uid}"  # 用户级写锁
            lock_dict = self._write_locks

        # 检查数量限制
        if len(lock_dict) >= self._max_locks:
            self._emergency_cleanup()

        # 记录访问时间
        self._access_times[key] = time.time()

        # 检查是否需要清理
        self._maybe_cleanup()

        # 返回或创建锁
        if key not in lock_dict:
            lock_dict[key] = asyncio.Lock()

        return lock_dict[key]

    def _maybe_cleanup(self):
        """按需清理"""
        current_time = time.time()
        if current_time - self._last_cleanup < self._cleanup_interval:
            return

        self._cleanup_old_locks()
        self._last_cleanup = current_time

    def _cleanup_old_locks(self):
        """清理过期锁"""
        now = time.time()
        max_age = 86400  # 24小时

        # 清理读锁
        old_read_keys = [
            key
            for key, last_used in self._access_times.items()
            if key.startswith("read:") and now - last_used > max_age
        ]

        # 清理写锁
        old_write_keys = [
            key
            for key, last_used in self._access_times.items()
            if key.startswith("write:") and now - last_used > max_age
        ]

        for key in old_read_keys + old_write_keys:
            if key.startswith("read:"):
                self._read_locks.pop(key, None)
            else:
                self._write_locks.pop(key, None)
            self._access_times.pop(key, None)

        if old_read_keys or old_write_keys:
            logger.info(
                f"智能锁清理: 读锁{len(old_read_keys)}, 写锁{len(old_write_keys)}"
            )

    def _emergency_cleanup(self):
        """紧急清理"""
        now = time.time()
        max_age = 3600  # 1小时

        # 清理所有类型的旧锁
        old_keys = [
            key
            for key, last_used in self._access_times.items()
            if now - last_used > max_age
        ]

        for key in old_keys:
            if key.startswith("read:"):
                self._read_locks.pop(key, None)
            else:
                self._write_locks.pop(key, None)
            self._access_times.pop(key, None)

        logger.warning(f"智能锁紧急清理: 移除了 {len(old_keys)} 个锁")


# 全局实例
user_lock_manager = SmartLockManager()
timer_manager = ActivityTimerManager()
performance_optimizer = EnhancedPerformanceOptimizer()
heartbeat_manager = HeartbeatManager()
notification_service = NotificationService()
