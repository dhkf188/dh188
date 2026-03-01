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
    TelegramNotFound
)
from config import Config
import re

logger = logging.getLogger("GroupCheckInBot")


class RobustBotManager:
    """企业级健壮Bot管理器 - 单例 + 启动锁 + 健康监控 + 智能重试"""

    # 单例 & 启动锁
    _instance = None
    _start_lock = asyncio.Lock()
    _global_started = False

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

        # 统计
        self._total_polls = 0
        self._failed_polls = 0
        self._error_counts = {"flood": 0, "network": 0, "timeout": 0, "other": 0}

        self._initialized = True
        logger.info(f"Bot管理器实例创建 [ID: {self._instance_id}]")

    async def initialize(self):
        """初始化Bot"""
        if self.bot and self._polling_task and not self._polling_task.done():
            logger.info(f"[{self._instance_id}] Bot已在运行，跳过初始化")
            return

        await self._safe_cleanup()
        self.bot = Bot(token=self.token)
        self.dispatcher = Dispatcher(storage=MemoryStorage())

        try:
            me = await self.bot.get_me()
            logger.info(f"[{self._instance_id}] ✅ Bot初始化完成 - @{me.username} (ID: {me.id})")
        except Exception as e:
            logger.error(f"[{self._instance_id}] ❌ Bot初始化失败: {e}")
            raise

    async def _safe_cleanup(self):
        """安全清理资源"""
        try:
            if self._polling_task and not self._polling_task.done():
                self._polling_task.cancel()
                try:
                    await self._polling_task
                except (asyncio.CancelledError, Exception):
                    pass
                self._polling_task = None

            if self.bot and self.bot.session:
                await self.bot.session.close()
                self.bot = None

            self.dispatcher = None
            self._is_running = False
            self._current_retry = 0
            await asyncio.sleep(1)
        except Exception as e:
            logger.warning(f"[{self._instance_id}] 清理旧资源出错: {e}")

    async def start_polling_with_retry(self):
        """稳定轮询 - 带启动锁"""
        async with RobustBotManager._start_lock:
            if RobustBotManager._global_started:
                logger.warning(f"[{self._instance_id}] ⏭️ 全局已启动，跳过")
                return
            RobustBotManager._global_started = True
            self._is_running = True
            self._current_retry = 0
            self._shutdown_event.clear()

        if not self.bot:
            await self.initialize()

        # 删除webhook
        for attempt in range(3):
            try:
                await self.bot.delete_webhook(drop_pending_updates=True)
                logger.info(f"[{self._instance_id}] ✅ Webhook已删除 (尝试 {attempt+1}/3)")
                await asyncio.sleep(2)
                break
            except Exception as e:
                logger.warning(f"[{self._instance_id}] ⚠️ 删除Webhook失败 (尝试 {attempt+1}/3): {e}")
                if attempt < 2:
                    await asyncio.sleep(5)

        is_render = "RENDER" in os.environ
        polling_config = {
            "timeout": 60 if is_render else 30,
            "allowed_updates": ["message", "callback_query", "chat_member"],
        }

        consecutive_errors = 0
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
                    self.bot,
                    skip_updates=True,
                    **polling_config
                )

                # 成功
                self._last_successful_connection = time.time()
                self._current_retry = 0
                consecutive_errors = 0
                logger.info(f"[{self._instance_id}] ✅ Bot轮询成功")
                break

            except asyncio.CancelledError:
                logger.info(f"[{self._instance_id}] Bot轮询被取消")
                break
            except Exception as e:
                consecutive_errors += 1
                self._failed_polls += 1
                error_msg = str(e).lower()
                
                # 修复：正确更新错误计数
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
                    logger.critical(f"[{self._instance_id}] 🚨 达到最大重试次数")
                    break

                delay = self._calculate_delay(error_msg, consecutive_errors)
                for _ in range(int(delay)):
                    if self._shutdown_event.is_set():
                        break
                    await asyncio.sleep(1)

        async with RobustBotManager._start_lock:
            RobustBotManager._global_started = False
            self._is_running = False
            self._polling_task = None

        if self.bot and self.bot.session:
            await self.bot.session.close()
            self.bot = None

    def _calculate_delay(self, error_msg: str, consecutive_errors: int) -> float:
        if "flood control" in error_msg or "too many requests" in error_msg:
            delay = 60
            retry_match = re.search(r'retry after (\d+)', error_msg)
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
        logger.info(f"[{self._instance_id}] 🛑 停止Bot...")
        self._shutdown_event.set()
        self._is_running = False

        if self._polling_task and not self._polling_task.done():
            self._polling_task.cancel()
            try:
                await self._polling_task
            except asyncio.CancelledError:
                pass
            self._polling_task = None

        if self.bot and self.bot.session:
            try:
                await self.bot.session.close()
                self.bot = None
            except Exception as e:
                logger.warning(f"[{self._instance_id}] ⚠️ 关闭session失败: {e}")

        async with RobustBotManager._start_lock:
            RobustBotManager._global_started = False

    async def send_message_with_retry(self, chat_id: int, text: str, **kwargs) -> bool:
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
                
            except Exception as e:
                if attempt == max_attempts:
                    return False
                delay = base_delay * attempt
                await asyncio.sleep(delay)

        return False

    async def send_document_with_retry(self, chat_id: int, document, caption: str = "", **kwargs) -> bool:
        """带完整错误处理的文档发送"""
        max_attempts = 3
        base_delay = 2

        for attempt in range(1, max_attempts + 1):
            try:
                await self.bot.send_document(chat_id, document, caption=caption, **kwargs)
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
                
            except Exception as e:
                if attempt == max_attempts:
                    return False
                delay = base_delay * attempt
                await asyncio.sleep(delay)

        return False

    def is_healthy(self) -> bool:
        if not self._last_successful_connection:
            return False
        delta = time.time() - self._last_successful_connection
        return delta < self._connection_check_interval

    async def restart_polling(self):
        await self.stop()
        await asyncio.sleep(3)
        await self.initialize()
        await self.start_polling_with_retry()

    async def start_health_monitor(self):
        self._health_monitor_task = asyncio.create_task(self._health_monitor_loop())

    async def _health_monitor_loop(self):
        while True:
            await asyncio.sleep(60)
            if not self.is_healthy() and self._is_running:
                logger.warning(f"[{self._instance_id}] 健康检查失败，尝试重启...")
                asyncio.create_task(self.restart_polling())

    def get_status(self) -> Dict[str, Any]:
        return {
            "instance_id": self._instance_id,
            "is_running": self._is_running,
            "bot_exists": self.bot is not None,
            "dispatcher_exists": self.dispatcher is not None,
            "current_retry": self._current_retry,
            "last_successful_connection": self._last_successful_connection,
            "global_started": RobustBotManager._global_started,
        }


# 全局单例实例
bot_manager = RobustBotManager(Config.TOKEN)