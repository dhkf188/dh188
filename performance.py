import os
import time
import asyncio
import logging
import threading  # ✅ 需要添加这个导入
from typing import Dict, Any, Callable, Optional, List
from functools import wraps
from dataclasses import dataclass
from datetime import datetime, timedelta

logger = logging.getLogger("GroupCheckInBot")


@dataclass
class PerformanceMetrics:
    """性能指标"""

    count: int = 0
    total_time: float = 0
    avg_time: float = 0
    max_time: float = 0
    min_time: float = float("inf")
    last_updated: float = 0


class PerformanceMonitor:
    """性能监控器"""

    def __init__(self):
        self.metrics: Dict[str, PerformanceMetrics] = {}
        self.slow_operations_count = 0
        self.start_time = time.time()

    def track(self, operation_name: str):
        """性能跟踪装饰器"""

        def decorator(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = await func(*args, **kwargs)
                    return result
                finally:
                    execution_time = time.time() - start_time
                    self._record_metrics(operation_name, execution_time)

            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    return result
                finally:
                    execution_time = time.time() - start_time
                    self._record_metrics(operation_name, execution_time)

            return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper

        return decorator

    def _record_metrics(self, operation_name: str, execution_time: float):
        """记录性能指标"""
        if operation_name not in self.metrics:
            self.metrics[operation_name] = PerformanceMetrics()

        metrics = self.metrics[operation_name]
        metrics.count += 1
        metrics.total_time += execution_time
        metrics.avg_time = metrics.total_time / metrics.count
        metrics.max_time = max(metrics.max_time, execution_time)
        metrics.min_time = min(metrics.min_time, execution_time)
        metrics.last_updated = time.time()

        if execution_time > 1.0:
            self.slow_operations_count += 1
            logger.warning(
                f"⏱️ 慢操作检测: {operation_name} 耗时 {execution_time:.3f}秒"
            )

    def get_metrics(self, operation_name: str) -> Optional[PerformanceMetrics]:
        """获取指定操作的性能指标"""
        return self.metrics.get(operation_name)

    def get_performance_report(self) -> Dict[str, Any]:
        """获取性能报告"""
        uptime = time.time() - self.start_time

        try:
            import psutil

            process = psutil.Process()
            memory_usage_mb = process.memory_info().rss / 1024 / 1024
        except ImportError:
            memory_usage_mb = 0

        metrics_summary = {}
        for op_name, metrics in self.metrics.items():
            if metrics.count > 0:
                metrics_summary[op_name] = {
                    "count": metrics.count,
                    "avg": metrics.avg_time,
                    "max": metrics.max_time,
                    "min": metrics.min_time if metrics.min_time != float("inf") else 0,
                }

        return {
            "uptime": uptime,
            "memory_usage_mb": memory_usage_mb,
            "slow_operations_count": self.slow_operations_count,
            "total_operations": sum(m.count for m in self.metrics.values()),
            "metrics_summary": metrics_summary,
        }

    def reset_metrics(self):
        """重置性能指标"""
        self.metrics.clear()
        self.slow_operations_count = 0


class RetryManager:
    """重试管理器"""

    def __init__(self, max_retries: int = 3, base_delay: float = 1.0):
        self.max_retries = max_retries
        self.base_delay = base_delay

    def with_retry(self, operation_name: str = "unknown"):
        """重试装饰器"""

        def decorator(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                last_exception = None
                for attempt in range(self.max_retries + 1):
                    try:
                        return await func(*args, **kwargs)
                    except Exception as e:
                        last_exception = e
                        if attempt == self.max_retries:
                            break

                        delay = self.base_delay * (2**attempt)
                        logger.warning(
                            f"🔄 重试 {operation_name} (尝试 {attempt + 1}/{self.max_retries}): {e}"
                        )
                        await asyncio.sleep(delay)

                logger.error(
                    f"❌ {operation_name} 重试{self.max_retries}次后失败: {last_exception}"
                )
                raise last_exception

            return async_wrapper

        return decorator


class GlobalCache:
    """全局缓存管理器"""

    def __init__(self, default_ttl: int = 300):
        self._cache: Dict[str, Any] = {}
        self._cache_ttl: Dict[str, float] = {}
        self._hits = 0
        self._misses = 0
        self.default_ttl = default_ttl
        self._lock = threading.Lock()  # ✅ 添加线程锁保证原子性

    def get(self, key: str) -> Any:
        """获取缓存值"""
        with self._lock:  # ✅ 加锁保证线程安全
            if key in self._cache_ttl and time.time() < self._cache_ttl[key]:
                self._hits += 1
                return self._cache.get(key)
            else:
                self._misses += 1
                self._cache.pop(key, None)
                self._cache_ttl.pop(key, None)
                return None

    def set(self, key: str, value: Any, ttl: int = None):
        """设置缓存值"""
        with self._lock:  # ✅ 加锁保证线程安全
            if ttl is None:
                ttl = self.default_ttl

            self._cache[key] = value
            self._cache_ttl[key] = time.time() + ttl

    def setnx(self, key: str, value: Any, ttl: int = None) -> bool:
        """
        原子性的 set if not exists

        Args:
            key: 缓存键
            value: 缓存值
            ttl: 过期时间（秒）

        Returns:
            True: 设置成功
            False: key已存在
        """
        with self._lock:  # ✅ 使用锁保证原子性
            # 检查key是否存在且未过期
            if key in self._cache_ttl and time.time() < self._cache_ttl[key]:
                return False
            # 不存在，设置值
            if ttl is None:
                ttl = self.default_ttl
            self._cache[key] = value
            self._cache_ttl[key] = time.time() + ttl
            return True

    def delete(self, key: str):
        """删除缓存值"""
        with self._lock:  # ✅ 加锁保证线程安全
            self._cache.pop(key, None)
            self._cache_ttl.pop(key, None)

    def clear_expired(self):
        """清理过期缓存"""
        with self._lock:  # ✅ 加锁保证线程安全
            current_time = time.time()
            expired_keys = [
                key for key, expiry in self._cache_ttl.items() if current_time >= expiry
            ]
            for key in expired_keys:
                self._cache.pop(key, None)
                self._cache_ttl.pop(key, None)

            if expired_keys:
                logger.debug(f"清理了 {len(expired_keys)} 个过期缓存")

    def clear_all(self):
        """清理所有缓存"""
        with self._lock:  # ✅ 加锁保证线程安全
            self._cache.clear()
            self._cache_ttl.clear()
            logger.info("所有缓存已清理")

    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计"""
        with self._lock:  # ✅ 加锁保证线程安全
            total = self._hits + self._misses
            hit_rate = self._hits / total if total > 0 else 0

            return {
                "size": len(self._cache),
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": hit_rate,
                "total_operations": total,
            }


class TaskManager:
    """任务管理器"""

    def __init__(self):
        self._tasks: Dict[str, asyncio.Task] = {}
        self._task_count = 0

    async def create_task(self, coro, name: str = None) -> asyncio.Task:
        """创建并跟踪任务"""
        if not name:
            self._task_count += 1
            name = f"task_{self._task_count}"

        task = asyncio.create_task(coro, name=name)
        self._tasks[name] = task

        task.add_done_callback(lambda t: self._tasks.pop(name, None))

        return task

    async def cancel_task(self, name: str):
        """取消指定任务"""
        task = self._tasks.get(name)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            self._tasks.pop(name, None)

    async def cancel_all_tasks(self):
        """取消所有任务"""
        tasks_to_cancel = list(self._tasks.values())
        for task in tasks_to_cancel:
            if not task.done():
                task.cancel()

        if tasks_to_cancel:
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
            self._tasks.clear()

    def get_task_count(self) -> int:
        """获取任务数量"""
        return len(self._tasks)

    def get_active_tasks(self) -> List[str]:
        """获取活跃任务列表"""
        return [name for name, task in self._tasks.items() if not task.done()]

    async def cleanup_tasks(self):
        """清理已完成的任务"""
        completed_tasks = [name for name, task in self._tasks.items() if task.done()]
        for name in completed_tasks:
            self._tasks.pop(name, None)

        if completed_tasks:
            logger.debug(f"清理了 {len(completed_tasks)} 个已完成任务")


class MessageDeduplicate:
    """消息去重管理器"""

    def __init__(self, ttl: int = 60):
        self._messages: Dict[str, float] = {}
        self.ttl = ttl

    def is_duplicate(self, message_id: str) -> bool:
        """检查消息是否重复"""
        current_time = time.time()

        expired_messages = [
            msg_id
            for msg_id, timestamp in self._messages.items()
            if current_time - timestamp > self.ttl
        ]
        for msg_id in expired_messages:
            self._messages.pop(msg_id, None)

        if message_id in self._messages:
            return True

        self._messages[message_id] = current_time
        return False

    def clear_expired(self):
        """清理过期消息"""
        current_time = time.time()
        expired_messages = [
            msg_id
            for msg_id, timestamp in self._messages.items()
            if current_time - timestamp > self.ttl
        ]
        for msg_id in expired_messages:
            self._messages.pop(msg_id, None)


def handle_database_errors(func):
    """数据库错误处理装饰器"""

    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"数据库操作失败 {func.__name__}: {e}")
            raise

    return async_wrapper


def handle_telegram_errors(func):
    """Telegram API错误处理装饰器"""

    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Telegram API操作失败 {func.__name__}: {e}")
            raise

    return async_wrapper


performance_monitor = PerformanceMonitor()
retry_manager = RetryManager(max_retries=3, base_delay=1.0)
global_cache = GlobalCache(default_ttl=300)
task_manager = TaskManager()
message_deduplicate = MessageDeduplicate(ttl=60)


def track_performance(operation_name: str):
    """性能跟踪装饰器"""
    return performance_monitor.track(operation_name)


def with_retry(operation_name: str = "unknown", max_retries: int = 3):
    """重试装饰器"""
    retry_mgr = RetryManager(max_retries=max_retries)
    return retry_mgr.with_retry(operation_name)


def message_deduplicate_decorator(ttl: int = 60):
    """消息去重装饰器"""
    deduplicate = MessageDeduplicate(ttl=ttl)

    def decorator(func):
        @wraps(func)
        async def wrapper(message, *args, **kwargs):
            message_id = f"{message.chat.id}_{message.message_id}"
            if deduplicate.is_duplicate(message_id):
                logger.debug(f"跳过重复消息: {message_id}")
                return
            return await func(message, *args, **kwargs)

        return wrapper

    return decorator


class DistributedLock:
    """基于global_cache的分布式锁

    特性：
    1. 分布式锁 - 支持多实例部署
    2. 自动过期 - 防止死锁
    3. 本地缓存 - 减少对global_cache的访问
    4. 可重入 - 同一个进程可重入
    5. 监控统计 - 便于调试
    """

    def __init__(self):
        self._local_locks: Dict[str, float] = {}  # 本地锁缓存
        self._local_owner: Dict[str, str] = {}  # 锁持有者标识
        self._renewing: Dict[str, bool] = {}
        self._cleanup_task: Optional[asyncio.Task] = None
        self._lock_metrics: Dict[str, Dict] = {}  # 锁统计

    async def start_cleanup(self):
        """启动本地锁清理任务"""
        if not self._cleanup_task:
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
            logger.info("✅ 分布式锁清理任务已启动")

    async def _cleanup_loop(self):
        """定期清理过期本地锁"""
        while True:
            try:
                await asyncio.sleep(30)  # 每30秒清理一次
                await self._cleanup_local_locks()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ 清理本地锁失败: {e}")

    async def _cleanup_local_locks(self):
        """清理过期本地锁"""
        now = time.time()
        expired = []

        for key, ts in self._local_locks.items():
            if now - ts > 60:  # 60秒过期
                # ✅ 检查分布式锁是否还存在
                lock_key = f"distributed_lock:{key}"
                if global_cache.get(lock_key) is None:
                    # 分布式锁已不存在，可以安全清理
                    expired.append(key)
                    logger.debug(f"🧹 清理过期本地锁（分布式锁已不存在）: {key}")
                else:
                    # 分布式锁还在，说明是正常持有，不清理
                    logger.debug(
                        f"⏳ 本地锁过期但分布式锁仍在: {key}, 年龄: {now-ts:.1f}s"
                    )

        for key in expired:
            self._local_locks.pop(key, None)
            self._local_owner.pop(key, None)

        if expired:
            logger.info(f"🧹 已清理 {len(expired)} 个过期本地锁")

    def _generate_lock_value(self) -> str:
        """生成唯一的锁持有者标识"""
        try:
            task = asyncio.current_task()
            task_id = id(task) if task else 0
        except RuntimeError:
            task_id = 0  # 不在事件循环中
        return f"{os.getpid()}:{time.time()}:{task_id}"

    async def acquire(self, key: str, timeout: int = 30, owner: str = None) -> bool:
        """
        获取分布式锁

        Args:
            key: 锁的键名
            timeout: 锁超时时间（秒）
            owner: 锁持有者标识（自动生成）

        Returns:
            是否成功获取锁
        """
        lock_key = f"distributed_lock:{key}"

        # 1. 检查本地锁（快速失败）
        if key in self._local_locks:
            lock_age = time.time() - self._local_locks[key]
            if lock_age < timeout:
                logger.debug(f"⏳ 本地锁存在: {key}, 年龄: {lock_age:.1f}s")
                self._record_metrics(key, "local_blocked")
                return False
            else:
                # 本地锁过期，清理掉
                self._local_locks.pop(key, None)
                self._local_owner.pop(key, None)

        # 2. 生成锁持有者标识
        lock_value = owner or self._generate_lock_value()

        # ✅ 3. 使用setnx原子操作获取锁
        if global_cache.setnx(lock_key, lock_value, ttl=timeout):
            self._local_locks[key] = time.time()
            self._local_owner[key] = lock_value
            logger.info(f"🔒 获取锁成功: {key}, 超时: {timeout}s")
            self._record_metrics(key, "acquired")
            return True

        # 4. 锁被持有
        existing = global_cache.get(lock_key)
        logger.debug(f"🔒 锁被持有: {key}, 持有者: {existing}")
        self._record_metrics(key, "blocked")
        return False

    async def renew(self, key: str, timeout: int = 30) -> bool:
        """
        续期锁（防止业务处理时间超过锁超时时间）

        Args:
            key: 锁的键名
            timeout: 新的超时时间（秒）

        Returns:
            是否续期成功
        """
        lock_key = f"distributed_lock:{key}"

        # 1. 获取当前锁的值
        current_lock = global_cache.get(lock_key)

        # 2. 获取预期的持有者
        expected_owner = self._local_owner.get(key)

        # 3. 只续期自己持有的锁
        if current_lock and expected_owner and current_lock == expected_owner:
            global_cache.set(lock_key, current_lock, ttl=timeout)  # 重新设置
            self._local_locks[key] = time.time()  # 更新本地锁时间
            logger.debug(f"🔄 锁续期成功: {key}, 新超时: {timeout}s")
            self._record_metrics(key, "renewed")
            return True
        else:
            logger.warning(f"⚠️ 锁续期失败（不是锁持有者）: {key}")
            return False

    async def release(self, key: str, owner: str = None):
        """
        释放分布式锁

        Args:
            key: 锁的键名
            owner: 锁持有者标识（用于验证）
        """
        lock_key = f"distributed_lock:{key}"

        # 1. 获取当前锁的值
        current_lock = global_cache.get(lock_key)

        # 2. 获取预期的持有者
        expected_owner = owner or self._local_owner.get(key)

        # 3. 只释放自己持有的锁
        if current_lock and expected_owner and current_lock == expected_owner:
            global_cache.delete(lock_key)
            self._local_locks.pop(key, None)
            self._local_owner.pop(key, None)
            logger.info(f"🔓 释放锁成功: {key}")
            self._record_metrics(key, "released")
        elif current_lock:
            logger.debug(
                f"🔓 锁已被其他进程持有: {key}, 当前: {current_lock}, 期望: {expected_owner}"
            )
            self._local_locks.pop(key, None)
            self._local_owner.pop(key, None)
        else:
            logger.debug(f"🔓 锁已不存在: {key}")
            self._local_locks.pop(key, None)
            self._local_owner.pop(key, None)

    async def force_release(self, key: str):
        """
        强制释放锁（管理员用）

        Args:
            key: 锁的键名
        """
        lock_key = f"distributed_lock:{key}"
        global_cache.delete(lock_key)
        self._local_locks.pop(key, None)
        self._local_owner.pop(key, None)
        logger.warning(f"⚠️ 强制释放锁: {key}")
        self._record_metrics(key, "force_released")

    def is_locked(self, key: str) -> bool:
        """检查锁是否被持有"""
        lock_key = f"distributed_lock:{key}"
        return global_cache.get(lock_key) is not None

    def get_owner(self, key: str) -> Optional[str]:
        """获取锁持有者"""
        lock_key = f"distributed_lock:{key}"
        return global_cache.get(lock_key)

    def _record_metrics(self, key: str, action: str):
        """记录锁的统计信息"""
        if key not in self._lock_metrics:
            self._lock_metrics[key] = {
                "acquired": 0,
                "released": 0,
                "blocked": 0,
                "local_blocked": 0,
                "force_released": 0,
                "renewed": 0,  # ✅ 添加续期统计
                "last_action": None,
                "last_time": 0,
            }

        metrics = self._lock_metrics[key]
        if action in metrics:
            metrics[action] += 1
        metrics["last_action"] = action
        metrics["last_time"] = time.time()

    def get_metrics(self, key: str = None) -> Dict:
        """获取锁统计信息"""
        if key:
            return self._lock_metrics.get(key, {})

        # 汇总统计
        total = {
            "total_locks": len(
                [
                    k
                    for k in global_cache._cache.keys()
                    if k.startswith("distributed_lock:")
                ]
            ),
            "local_locks": len(self._local_locks),
            "metrics_summary": {},
        }

        for lock_key, metrics in self._lock_metrics.items():
            total["metrics_summary"][lock_key] = metrics

        return total

    async def cleanup_stale_locks(self, max_age: int = 300):
        """
        清理陈旧的锁（超过max_age秒未更新）

        Args:
            max_age: 最大存活时间（秒）
        """
        now = time.time()
        stale_keys = []

        for key in list(global_cache._cache.keys()):
            if key.startswith("distributed_lock:"):
                lock_age = now - global_cache._cache_ttl.get(key, now)
                if lock_age > max_age:
                    stale_keys.append(key)

        for key in stale_keys:
            global_cache.delete(key)
            logger.warning(f"⚠️ 清理陈旧锁: {key}, 年龄: {max_age}+秒")

        return len(stale_keys)


# 创建全局实例
distributed_lock = DistributedLock()


class LockContext:
    """锁的上下文管理器，确保锁一定会被释放"""

    def __init__(
        self,
        lock: DistributedLock,
        key: str,
        timeout: int = 30,
        auto_renew: bool = False,
    ):
        self.lock = lock
        self.key = key
        self.timeout = timeout
        self.auto_renew = auto_renew  # ✅ 是否自动续期
        self.acquired = False
        self.owner = None
        self._renew_task: Optional[asyncio.Task] = None  # ✅ 续期任务

    async def __aenter__(self):
        self.owner = f"{os.getpid()}:{time.time()}:{id(self)}"
        self.acquired = await self.lock.acquire(self.key, self.timeout, self.owner)

        # ✅ 如果需要自动续期，启动续期任务
        if self.acquired and self.auto_renew:
            self._start_renew_task()

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # ✅ 取消续期任务
        if self._renew_task:
            self._renew_task.cancel()
            try:
                await self._renew_task
            except asyncio.CancelledError:
                pass

        if self.acquired:
            await self.lock.release(self.key, self.owner)

    def _start_renew_task(self):
        """启动自动续期任务"""

        async def renew_loop():
            try:
                while True:
                    await asyncio.sleep(self.timeout * 0.6)  # 在过期前60%时间续期
                    if self.acquired:
                        success = await self.lock.renew(self.key, self.timeout)
                        if not success:
                            logger.warning(
                                f"⚠️ 锁续期失败，可能已被其他进程抢占: {self.key}"
                            )
                            self.acquired = False
                            break
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"❌ 续期任务异常: {e}")

        self._renew_task = asyncio.create_task(renew_loop())

    def __bool__(self):
        return self.acquired
