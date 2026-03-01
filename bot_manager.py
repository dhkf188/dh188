import asyncio
import logging
import time
import os
import uuid
from typing import Optional, Dict, Any
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import (
    TelegramRetryAfter,
    TelegramNetworkError,
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramUnauthorizedError,
    TelegramNotFound,
    TelegramConflictError,  # 添加这个
)
from config import Config
import re

logger = logging.getLogger("GroupCheckInBot")


class RobustBotManager:
    """企业级健壮Bot管理器 - 单例 + 启动锁 + 健康监控 + 智能重试 + 冲突处理"""

    # 单例 & 启动锁
    _instance = None
    _start_lock = asyncio.Lock()
    _global_started = False
    _active_token = None  # 记录当前活跃的 token
    _active_instance_id = None  # 记录当前活跃的实例ID

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, token: str):
        if getattr(self, "_initialized", False):
            return

        self.token = token
        self.bot: Optional[Bot] = None
        self.dispatcher: Optional[Dispatcher] = None
        self._is_running = False
        self._polling_task: Optional[asyncio.Task] = None
        self._health_monitor_task: Optional[asyncio.Task] = None

        self._max_retries = 15
        self._base_delay = 2.0
        self._current_retry = 0
        self._last_successful_connection = 0
        self._connection_check_interval = 300
        self._shutdown_event = asyncio.Event()
        self._instance_id = str(uuid.uuid4())[:8]

        # 冲突处理相关
        self._conflict_count = 0
        self._max_conflict_retries = 5
        self._last_conflict_time = 0

        # 统计
        self._total_polls = 0
        self._failed_polls = 0
        self._error_counts = {"flood": 0, "network": 0, "timeout": 0, "other": 0, "conflict": 0}

        self._initialized = True
        logger.info(f"Bot管理器实例创建 [ID: {self._instance_id}]")

    async def initialize(self):
        """初始化Bot - 增强版"""
        # 检查是否有其他实例在运行
        async with RobustBotManager._start_lock:
            if RobustBotManager._active_token == self.token:
                logger.warning(
                    f"[{self._instance_id}] ⚠️ 检测到相同 token 的活跃实例 "
                    f"[ID: {RobustBotManager._active_instance_id}]"
                )
                # 等待旧实例完全停止
                await asyncio.sleep(5)

        if self.bot and self._polling_task and not self._polling_task.done():
            logger.info(f"[{self._instance_id}] Bot已在运行，跳过初始化")
            return

        await self._safe_cleanup()
        self.bot = Bot(token=self.token)
        self.dispatcher = Dispatcher(storage=MemoryStorage())

        try:
            me = await self.bot.get_me()
            logger.info(
                f"[{self._instance_id}] ✅ Bot初始化完成 - @{me.username} (ID: {me.id})"
            )
        except Exception as e:
            logger.error(f"[{self._instance_id}] ❌ Bot初始化失败: {e}")
            raise

    async def _safe_cleanup(self):
        """安全清理资源 - 增强版"""
        try:
            if self._polling_task and not self._polling_task.done():
                self._polling_task.cancel()
                try:
                    await self._polling_task
                except (asyncio.CancelledError, Exception):
                    pass
                self._polling_task = None

            if self.bot and self.bot.session:
                try:
                    await self.bot.session.close()
                except Exception:
                    pass
                self.bot = None

            self.dispatcher = None
            self._is_running = False
            self._current_retry = 0
            await asyncio.sleep(2)  # 增加等待时间确清理
        except Exception as e:
            logger.warning(f"[{self._instance_id}] 清理旧资源出错: {e}")

    async def _ensure_clean_telegram_state(self):
        """确保 Telegram 端是干净状态 - 新增"""
        if not self.bot:
            return

        try:
            # 先获取 webhook 信息
            webhook_info = await self.bot.get_webhook_info()
            if webhook_info.url:
                logger.info(
                    f"[{self._instance_id}] 📡 当前有 webhook: {webhook_info.url}"
                )

            # 多次尝试删除 webhook
            for attempt in range(5):
                try:
                    await self.bot.delete_webhook(drop_pending_updates=True)
                    logger.info(
                        f"[{self._instance_id}] ✅ Webhook已删除 (尝试 {attempt+1}/5)"
                    )
                    await asyncio.sleep(2)
                    break
                except Exception as e:
                    logger.warning(
                        f"[{self._instance_id}] ⚠️ 删除Webhook失败 (尝试 {attempt+1}/5): {e}"
                    )
                    if attempt < 4:
                        await asyncio.sleep(5)

            # 额外等待确保生效
            await asyncio.sleep(2)

        except Exception as e:
            logger.error(f"[{self._instance_id}] ❌ 确保干净状态失败: {e}")

    async def _force_resolve_conflict(self):
        """强制解决冲突 - 终极增强版"""
        logger.warning(f"[{self._instance_id}] 🚨 执行强制冲突解决...")

        # 1. 尝试获取当前 webhook 信息
        try:
            webhook_info = await self.bot.get_webhook_info()
            logger.info(f"[{self._instance_id}] 📡 当前 webhook: {webhook_info.url}")
        except:
            pass

        # 2. 多次尝试删除 webhook
        for attempt in range(5):
            try:
                await self.bot.delete_webhook(drop_pending_updates=True)
                logger.info(f"[{self._instance_id}] ✅ 强制删除 webhook 成功 (尝试 {attempt+1}/5)")
                break
            except Exception as e:
                logger.warning(f"[{self._instance_id}] ⚠️ 删除 webhook 失败 (尝试 {attempt+1}/5): {e}")
                if attempt < 4:
                    await asyncio.sleep(3)

        # 3. 关闭当前连接
        if self.bot and self.bot.session:
            try:
                await self.bot.session.close()
                logger.info(f"[{self._instance_id}] ✅ 关闭旧会话成功")
            except Exception as e:
                logger.warning(f"[{self._instance_id}] ⚠️ 关闭旧会话失败: {e}")

        # 4. 等待更长时间确保连接完全释放
        await asyncio.sleep(10)

        # 5. 重新创建 bot
        self.bot = Bot(token=self.token)
        logger.info(f"[{self._instance_id}] ✅ 重新创建 bot 实例")

        # 6. 再次删除 webhook
        for attempt in range(3):
            try:
                await self.bot.delete_webhook(drop_pending_updates=True)
                logger.info(f"[{self._instance_id}] ✅ 重启后删除 webhook 成功 (尝试 {attempt+1}/3)")
                break
            except Exception as e:
                logger.warning(f"[{self._instance_id}] ⚠️ 重启后删除 webhook 失败 (尝试 {attempt+1}/3): {e}")
                if attempt < 2:
                    await asyncio.sleep(2)

        # 7. 验证 webhook 状态
        try:
            webhook_info = await self.bot.get_webhook_info()
            logger.info(f"[{self._instance_id}] 📡 最终 webhook 状态: {webhook_info}")
        except Exception as e:
            logger.warning(f"[{self._instance_id}] ⚠️ 获取 webhook 状态失败: {e}")

        # 8. 重置冲突计数
        self._conflict_count = 0
        self._last_conflict_time = time.time()

        logger.info(f"[{self._instance_id}] ✅ 强制冲突解决完成")
        await asyncio.sleep(5)

    async def start_polling_with_retry(self):
        """稳定轮询 - 带启动锁和冲突处理（增强版）"""
        # 启动锁检查 - 增强版：如果已启动，尝试强制接管
        async with RobustBotManager._start_lock:
            if RobustBotManager._global_started:
                logger.warning(
                    f"[{self._instance_id}] ⚠️ 检测到全局已启动 [ID: {RobustBotManager._active_instance_id}]，尝试强制接管..."
                )
                # 等待一下，让旧实例有机会自己清理
                await asyncio.sleep(3)

                # 如果还是标记为已启动，强制重置
                if RobustBotManager._global_started:
                    logger.warning(f"[{self._instance_id}] ⚠️ 强制重置全局启动状态")
                    RobustBotManager._global_started = False
                    RobustBotManager._active_token = None
                    RobustBotManager._active_instance_id = None

            RobustBotManager._global_started = True
            RobustBotManager._active_token = self.token
            RobustBotManager._active_instance_id = self._instance_id
            self._is_running = True
            self._current_retry = 0
            self._shutdown_event.clear()

        if not self.bot:
            await self.initialize()

        # 确保 Telegram 端是干净状态 - 增强版：多次尝试
        await self._ensure_clean_telegram_state()

        # 额外等待确保 webhook 完全清除
        await asyncio.sleep(2)

        is_render = "RENDER" in os.environ
        polling_config = {
            "timeout": 60 if is_render else 30,
            "allowed_updates": ["message", "callback_query", "chat_member"],
        }

        consecutive_errors = 0
        self._conflict_count = 0
        force_resolve_attempted = False

        while self._is_running and self._current_retry < self._max_retries:
            try:
                if self._shutdown_event.is_set():
                    break

                self._current_retry += 1
                self._total_polls += 1

                if consecutive_errors > 0:
                    cooldown = min(30, consecutive_errors * 5)
                    logger.info(f"[{self._instance_id}] ❄️ 错误冷却 {cooldown}s")
                    for _ in range(cooldown):
                        if self._shutdown_event.is_set():
                            break
                        await asyncio.sleep(1)
                    if self._shutdown_event.is_set():
                        break

                await self.dispatcher.start_polling(
                    self.bot, skip_updates=True, **polling_config
                )

                # 成功
                self._last_successful_connection = time.time()
                self._current_retry = 0
                consecutive_errors = 0
                self._conflict_count = 0
                force_resolve_attempted = False
                logger.info(f"[{self._instance_id}] ✅ Bot轮询成功")
                break

            except asyncio.CancelledError:
                logger.info(f"[{self._instance_id}] Bot轮询被取消")
                break

            except TelegramConflictError as e:
                self._conflict_count += 1
                consecutive_errors += 1
                self._failed_polls += 1
                self._error_counts["conflict"] += 1
                self._last_conflict_time = time.time()

                logger.error(
                    f"[{self._instance_id}] ❌ 冲突错误 (第{self._conflict_count}次): {e}"
                )

                # ===== 增强的冲突处理 =====
                # 第一次冲突就尝试快速恢复
                if self._conflict_count >= 1 and not force_resolve_attempted:
                    logger.warning(f"[{self._instance_id}] 🔄 尝试快速恢复...")

                    # 快速删除 webhook
                    try:
                        await self.bot.delete_webhook(drop_pending_updates=True)
                        logger.info(f"[{self._instance_id}] ✅ 快速删除 webhook 成功")
                        await asyncio.sleep(3)
                    except Exception as we:
                        logger.warning(f"快速删除 webhook 失败: {we}")

                # 连续2次冲突，执行中等强度恢复
                if self._conflict_count >= 2:
                    logger.warning(f"[{self._instance_id}] 🔄 尝试中等强度恢复...")

                    # 关闭当前连接
                    if self.bot and self.bot.session:
                        try:
                            await self.bot.session.close()
                        except:
                            pass

                    await asyncio.sleep(5)

                    # 重新创建 bot
                    self.bot = Bot(token=self.token)

                    # 删除 webhook
                    try:
                        await self.bot.delete_webhook(drop_pending_updates=True)
                        logger.info(f"[{self._instance_id}] ✅ 重建后删除 webhook 成功")
                    except Exception as we:
                        logger.error(f"重建后删除 webhook 失败: {we}")

                # 连续3次冲突，执行强制解决
                if self._conflict_count >= 3:
                    logger.critical(
                        f"[{self._instance_id}] 🚨 连续{self._conflict_count}次冲突，执行强制解决..."
                    )
                    await self._force_resolve_conflict()
                    self._conflict_count = 0
                    force_resolve_attempted = True
                # ===== 结束增强冲突处理 =====

                if self._current_retry >= self._max_retries:
                    logger.critical(
                        f"[{self._instance_id}] 🚨 达到最大重试次数，停止尝试"
                    )
                    break

                # 冲突等待时间逐步增加 - 增加最大等待时间
                delay = min(30 * self._conflict_count, 300)  # 最大5分钟
                logger.info(
                    f"[{self._instance_id}] ⏳ 冲突等待 {delay}s (第{self._conflict_count}次)..."
                )

                # 等待时可以响应取消
                for _ in range(int(delay)):
                    if self._shutdown_event.is_set():
                        break
                    await asyncio.sleep(1)

            except Exception as e:
                consecutive_errors += 1
                self._failed_polls += 1
                error_msg = str(e).lower()

                # 错误分类
                if "flood control" in error_msg or "too many requests" in error_msg:
                    self._error_counts["flood"] += 1
                elif "connection" in error_msg or "network" in error_msg:
                    self._error_counts["network"] += 1
                elif "timeout" in error_msg:
                    self._error_counts["timeout"] += 1
                else:
                    self._error_counts["other"] += 1

                logger.error(f"[{self._instance_id}] ❌ 轮询失败: {e}")

                if self._current_retry >= self._max_retries:
                    logger.critical(
                        f"[{self._instance_id}] 🚨 达到最大重试次数，停止尝试"
                    )
                    break

                delay = self._calculate_delay(error_msg, consecutive_errors)

                # 等待时可以响应取消
                for _ in range(int(delay)):
                    if self._shutdown_event.is_set():
                        break
                    await asyncio.sleep(1)

        # 清理全局状态
        await self._cleanup_global_state()

        # 关闭会话
        if self.bot and self.bot.session:
            try:
                await self.bot.session.close()
                self.bot = None
            except Exception as e:
                logger.warning(f"[{self._instance_id}] ⚠️ 关闭session失败: {e}")

    async def _cleanup_global_state(self):
        """清理全局状态 - 新增"""
        async with RobustBotManager._start_lock:
            if RobustBotManager._active_token == self.token:
                RobustBotManager._active_token = None
                RobustBotManager._active_instance_id = None
            RobustBotManager._global_started = False
            self._is_running = False
            self._polling_task = None

    def _calculate_delay(self, error_msg: str, consecutive_errors: int) -> float:
        """计算延迟时间 - 增强版"""
        if "flood control" in error_msg or "too many requests" in error_msg:
            delay = 60
            retry_match = re.search(r"retry after (\d+)", error_msg)
            if retry_match:
                delay = int(retry_match.group(1)) + 5
        elif "bad gateway" in error_msg or "502" in error_msg:
            delay = 30
        elif "connection" in error_msg or "timeout" in error_msg:
            delay = min(self._base_delay * (2 ** (consecutive_errors - 1)), 60)
        else:
            delay = min(self._base_delay * (2 ** (consecutive_errors - 1)), 120)
        return delay

    async def stop(self):
        """停止Bot - 增强版"""
        logger.info(f"[{self._instance_id}] 🛑 停止Bot...")
        self._shutdown_event.set()
        self._is_running = False

        # 取消健康监控
        if self._health_monitor_task and not self._health_monitor_task.done():
            self._health_monitor_task.cancel()
            try:
                await self._health_monitor_task
            except asyncio.CancelledError:
                pass

        # 取消轮询任务
        if self._polling_task and not self._polling_task.done():
            self._polling_task.cancel()
            try:
                await self._polling_task
            except asyncio.CancelledError:
                pass
            self._polling_task = None

        # 关闭会话
        if self.bot and self.bot.session:
            try:
                await self.bot.session.close()
                self.bot = None
            except Exception as e:
                logger.warning(f"[{self._instance_id}] ⚠️ 关闭session失败: {e}")

        # 清理全局状态
        await self._cleanup_global_state()

    async def send_message_with_retry(
        self, chat_id: int, text: str, **kwargs
    ) -> bool:
        """带完整错误处理的消息发送"""
        max_attempts = 3
        base_delay = 2

        for attempt in range(1, max_attempts + 1):
            try:
                await self.bot.send_message(chat_id, text, **kwargs)
                return True

            except TelegramRetryAfter as e:
                if attempt == max_attempts:
                    return False
                delay = e.retry_after + 1
                logger.warning(f"📤 Flood Control，等待{delay}秒后重试")
                await asyncio.sleep(delay)

            except (TelegramNetworkError, ConnectionError) as e:
                if attempt == max_attempts:
                    return False
                delay = min(base_delay * (2 ** (attempt - 1)), 30)
                await asyncio.sleep(delay)

            except TelegramForbiddenError:
                return False

            except TelegramConflictError:
                # 发送消息时也可能遇到冲突
                if attempt == max_attempts:
                    return False
                logger.warning(f"📤 发送消息时遇到冲突，等待10秒后重试")
                await asyncio.sleep(10)

            except Exception as e:
                if attempt == max_attempts:
                    return False
                delay = base_delay * attempt
                await asyncio.sleep(delay)

        return False

    async def send_document_with_retry(
        self, chat_id: int, document, caption: str = "", **kwargs
    ) -> bool:
        """带完整错误处理的文档发送"""
        max_attempts = 3
        base_delay = 2

        for attempt in range(1, max_attempts + 1):
            try:
                await self.bot.send_document(
                    chat_id, document, caption=caption, **kwargs
                )
                return True

            except TelegramRetryAfter as e:
                if attempt == max_attempts:
                    return False
                delay = e.retry_after + 1
                await asyncio.sleep(delay)

            except (TelegramNetworkError, ConnectionError) as e:
                if attempt == max_attempts:
                    return False
                delay = min(base_delay * (2 ** (attempt - 1)), 30)
                await asyncio.sleep(delay)

            except TelegramForbiddenError:
                return False

            except TelegramConflictError:
                if attempt == max_attempts:
                    return False
                logger.warning(f"📎 发送文档时遇到冲突，等待10秒后重试")
                await asyncio.sleep(10)

            except Exception as e:
                if attempt == max_attempts:
                    return False
                delay = base_delay * attempt
                await asyncio.sleep(delay)

        return False

    def is_healthy(self) -> bool:
        """检查Bot健康状态"""
        if not self._last_successful_connection:
            return False
        delta = time.time() - self._last_successful_connection
        return delta < self._connection_check_interval

    async def restart_polling(self):
        """重启轮询 - 增强版"""
        logger.info(f"[{self._instance_id}] 🔄 重启轮询...")
        await self.stop()
        await asyncio.sleep(5)  # 增加等待时间
        await self.initialize()
        self._polling_task = asyncio.create_task(self.start_polling_with_retry())

    async def start_health_monitor(self):
        """启动健康监控"""
        self._health_monitor_task = asyncio.create_task(self._health_monitor_loop())

    async def _health_monitor_loop(self):
        """健康监控循环 - 增强版"""
        consecutive_failures = 0
        while True:
            try:
                await asyncio.sleep(60)

                if not self._is_running:
                    continue

                if not self.is_healthy():
                    consecutive_failures += 1
                    logger.warning(
                        f"[{self._instance_id}] 健康检查失败 ({consecutive_failures}/3)"
                    )

                    if consecutive_failures >= 3:
                        logger.error(
                            f"[{self._instance_id}] 🚨 连续3次健康检查失败，尝试重启..."
                        )
                        # 使用 create_task 避免阻塞
                        asyncio.create_task(self.restart_polling())
                        consecutive_failures = 0
                else:
                    consecutive_failures = 0

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self._instance_id}] 健康监控异常: {e}")
                await asyncio.sleep(30)

    def get_status(self) -> Dict[str, Any]:
        """获取详细状态 - 增强版"""
        return {
            "instance_id": self._instance_id,
            "is_running": self._is_running,
            "bot_exists": self.bot is not None,
            "dispatcher_exists": self.dispatcher is not None,
            "current_retry": self._current_retry,
            "last_successful_connection": self._last_successful_connection,
            "global_started": RobustBotManager._global_started,
            "active_token": RobustBotManager._active_token,
            "active_instance": RobustBotManager._active_instance_id,
            "conflict_count": self._conflict_count,
            "total_polls": self._total_polls,
            "failed_polls": self._failed_polls,
            "error_counts": self._error_counts.copy(),
        }


# 全局单例实例
bot_manager = RobustBotManager(Config.TOKEN)
