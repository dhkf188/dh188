import os
import time
import asyncio
import logging
import gc
import psutil

from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional, Tuple
from functools import wraps

from aiogram import types
from config import Config, beijing_tz
from database import db
from performance import global_cache, task_manager

logger = logging.getLogger("GroupCheckInBot")


# =====================================================
# MessageFormatter
# =====================================================
class MessageFormatter:
    """消息格式化工具类（完整去重版）"""

    # ---------- 核心统一时间格式 ----------
    @staticmethod
    def _format_seconds(
        seconds: int,
        *,
        style: str = "normal",
        zero_fallback: str = "0秒",
    ) -> str:
        if seconds is None:
            return zero_fallback

        seconds = int(seconds)
        h = seconds // 3600
        m = (seconds % 3600) // 60
        s = seconds % 60

        if style == "csv":
            return f"{h}时{m}分{s}秒" if h > 0 else f"{m}分{s}秒"

        parts = []
        if h > 0:
            parts.append(f"{h}小时")
        if m > 0:
            parts.append(f"{m}分")
        if s > 0:
            parts.append(f"{s}秒")

        return "".join(parts) if parts else zero_fallback

    # ---------- 对外接口（保持不变） ----------
    @staticmethod
    def format_time(seconds: int) -> str:
        return MessageFormatter._format_seconds(seconds)

    @staticmethod
    def format_time_for_csv(seconds: int) -> str:
        return MessageFormatter._format_seconds(
            seconds, style="csv", zero_fallback="0分0秒"
        )

    @staticmethod
    def format_minutes_to_hms(minutes: float) -> str:
        if minutes is None:
            return "0小时0分0秒"
        return MessageFormatter._format_seconds(int(minutes * 60))

    @staticmethod
    def format_duration(seconds: int) -> str:
        return MessageFormatter._format_seconds(seconds, zero_fallback="0分钟")

    # ---------- 文本工具 ----------
    @staticmethod
    def format_user_link(user_id: int, user_name: str) -> str:
        clean = (user_name or f"用户{user_id}").translate(str.maketrans("", "", '<>&"'))
        return f'<a href="tg://user?id={user_id}">{clean}</a>'

    @staticmethod
    def format_copyable_text(text: str) -> str:
        return f"<code>{text}</code>"

    @staticmethod
    def create_dashed_line() -> str:
        return MessageFormatter.format_copyable_text("-" * 26)

    # ---------- 消息模板 ----------
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
        msg = (
            f"👤 用户：{MessageFormatter.format_user_link(user_id, user_name)}\n"
            f"✅ 打卡成功：{MessageFormatter.format_copyable_text(activity)} - "
            f"{MessageFormatter.format_copyable_text(time_str)}\n"
            f"⚠️ 第 {MessageFormatter.format_copyable_text(str(count))} 次 / "
            f"上限 {MessageFormatter.format_copyable_text(str(max_times))} 次\n"
            f"⏰ 时间限制：{MessageFormatter.format_copyable_text(str(time_limit))} 分钟"
        )
        if count >= max_times:
            msg += "\n🚨 今日次数已达上限"
        msg += "\n💡 完成后请点击「✅ 回座」"
        return msg

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
        msg = (
            f"👤 用户：{MessageFormatter.format_user_link(user_id, user_name)}\n"
            f"✅ {MessageFormatter.format_copyable_text(time_str)} 回座成功\n"
            f"📝 活动：{MessageFormatter.format_copyable_text(activity)}\n"
            f"⏰ 本次耗时：{MessageFormatter.format_copyable_text(elapsed_time)}\n"
            f"📈 今日累计：{MessageFormatter.format_copyable_text(total_activity_time)}\n"
            f"📊 今日总计：{MessageFormatter.format_copyable_text(total_time)}\n"
        )

        if is_overtime:
            msg += (
                f"⚠️ 超时："
                f"{MessageFormatter.format_copyable_text(MessageFormatter.format_time(overtime_seconds))}\n"
            )
            if fine_amount > 0:
                msg += f"💸 罚款：{MessageFormatter.format_copyable_text(str(fine_amount))} 元\n"

        msg += MessageFormatter.create_dashed_line() + "\n"
        for act, cnt in activity_counts.items():
            if cnt > 0:
                msg += (
                    f"🔹 {act}：{MessageFormatter.format_copyable_text(str(cnt))} 次\n"
                )

        msg += f"\n📊 总活动次数：{MessageFormatter.format_copyable_text(str(total_count))}"
        return msg


# =====================================================
# NotificationService（合并但不删功能）
# =====================================================
class NotificationService:
    def __init__(self, bot_manager=None):
        self.bot_manager = bot_manager
        self.bot = None
        self._last_notification_time = {}
        self._rate_limit_window = 60

    async def _dispatch(self, send_func, targets, *args, **kwargs) -> bool:
        for target in targets:
            try:
                ok = await send_func(target, *args, **kwargs)
                if ok is not False:
                    return True
            except Exception as e:
                logger.error(f"推送失败 {target}: {e}")
        return False

    async def send_notification(
        self, chat_id: int, text: str, notification_type: str = "all"
    ):
        key = f"{chat_id}:{hash(text)}"
        now = time.time()
        if (
            key in self._last_notification_time
            and now - self._last_notification_time[key] < self._rate_limit_window
        ):
            return True

        push = await db.get_push_settings()
        group = await db.get_group_cached(chat_id)
        sent = False

        if self.bot_manager:
            sent = await self._dispatch(
                lambda t, *a, **k: self.bot_manager.send_message_with_retry(t, *a, **k),
                filter(
                    None,
                    [
                        (
                            group.get("channel_id")
                            if push.get("enable_channel_push")
                            else None
                        ),
                        (
                            group.get("notification_group_id")
                            if push.get("enable_group_push")
                            else None
                        ),
                    ],
                ),
                text,
                parse_mode="HTML",
            )

        if not sent and push.get("enable_admin_push"):
            await self._dispatch(
                lambda t, *a, **k: self.bot_manager.send_message_with_retry(t, *a, **k),
                Config.ADMINS,
                text,
                parse_mode="HTML",
            )

        if sent:
            self._last_notification_time[key] = now
        return sent


# =====================================================
# UserLockManager
# =====================================================
class UserLockManager:
    def __init__(self):
        self._locks = {}
        self._access_times = {}

    def get_lock(self, chat_id: int, uid: int):
        key = f"{chat_id}-{uid}"
        self._access_times[key] = time.time()
        self._locks.setdefault(key, asyncio.Lock())
        return self._locks[key]


# =====================================================
# ActivityTimerManager
# =====================================================
class ActivityTimerManager:
    def __init__(self):
        self._timers = {}
        self.activity_timer_callback = None

    def set_activity_timer_callback(self, callback):
        self.activity_timer_callback = callback

    async def _cancel_task(self, key: str):
        task = self._timers.pop(key, None)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def start_timer(self, chat_id: int, uid: int, act: str, limit: int):
        key = f"{chat_id}-{uid}"
        await self._cancel_task(key)
        if not self.activity_timer_callback:
            return
        self._timers[key] = asyncio.create_task(
            self.activity_timer_callback(chat_id, uid, act, limit)
        )

    async def cancel_all_timers(self):
        for key in list(self._timers):
            await self._cancel_task(key)


# =====================================================
# EnhancedPerformanceOptimizer
# =====================================================
class EnhancedPerformanceOptimizer:
    def __init__(self):
        self.is_render = bool(os.environ.get("RENDER"))
        self.render_memory_limit = 400

    async def memory_cleanup(self):
        try:
            mem = psutil.Process().memory_info().rss / 1024 / 1024
            if self.is_render and mem > self.render_memory_limit:
                global_cache.clear_all()
                await task_manager.cleanup_tasks()
                await db.cleanup_cache()
                gc.collect()
        except Exception as e:
            logger.error(f"内存清理失败: {e}")


# =====================================================
# HeartbeatManager
# =====================================================
class HeartbeatManager:
    def __init__(self):
        self._is_running = False
        self._task = None
        self._last_heartbeat = time.time()

    async def initialize(self):
        self._is_running = True
        self._task = asyncio.create_task(self._loop())

    async def _loop(self):
        while self._is_running:
            self._last_heartbeat = time.time()
            await asyncio.sleep(60)

    async def stop(self):
        self._is_running = False
        if self._task:
            self._task.cancel()


# =====================================================
# 工具函数（全部保留）
# =====================================================
def get_beijing_time() -> datetime:
    return datetime.now(beijing_tz)


def calculate_cross_day_time_diff(
    current_dt: datetime, expected_time: str, checkin_type: str
) -> Tuple[float, int, datetime]:
    expected_hour, expected_minute = map(int, expected_time.split(":"))
    candidates = [
        current_dt.replace(
            hour=expected_hour, minute=expected_minute, second=0, microsecond=0
        )
        + timedelta(days=d)
        for d in (-1, 0, 1)
    ]
    expected_dt = min(candidates, key=lambda t: abs((t - current_dt).total_seconds()))
    diff_sec = int((current_dt - expected_dt).total_seconds())
    return diff_sec / 60, diff_sec, expected_dt


async def is_valid_checkin_time(
    chat_id: int, checkin_type: str, current_time: datetime
) -> Tuple[bool, datetime]:
    work_hours = await db.get_group_work_time(chat_id)
    expected = (
        work_hours["work_start"]
        if checkin_type == "work_start"
        else work_hours["work_end"]
    )
    h, m = map(int, expected.split(":"))

    candidates = [
        current_time.replace(hour=h, minute=m, second=0, microsecond=0)
        + timedelta(days=d)
        for d in (-1, 0, 1)
    ]
    expected_dt = min(candidates, key=lambda t: abs((t - current_time).total_seconds()))
    return abs((current_time - expected_dt).total_seconds()) <= 7 * 3600, expected_dt


# =====================================================
# rate_limit（保留）
# =====================================================
def rate_limit(rate: int = 1, per: int = 1):
    def decorator(func):
        calls = []

        @wraps(func)
        async def wrapper(*args, **kwargs):
            now = time.time()
            calls[:] = [c for c in calls if now - c < per]
            if len(calls) >= rate:
                if args and isinstance(args[0], types.Message):
                    await args[0].answer("⏳ 操作过于频繁，请稍后再试")
                return
            calls.append(now)
            return await func(*args, **kwargs)

        return wrapper

    return decorator


# =====================================================
# 全局实例（不变）
# =====================================================
user_lock_manager = UserLockManager()
timer_manager = ActivityTimerManager()
performance_optimizer = EnhancedPerformanceOptimizer()
heartbeat_manager = HeartbeatManager()
notification_service = NotificationService()
