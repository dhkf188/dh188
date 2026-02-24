import time
import asyncio
import logging
import inspect
import hashlib
import pickle
from typing import Dict, Any, Callable, Optional, List
from functools import wraps
from dataclasses import dataclass

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

            # ✅ 修复：使用 inspect.iscoroutinefunction
            return async_wrapper if inspect.iscoroutinefunction(func) else sync_wrapper

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

        # 记录慢操作
        if execution_time > 1.0:  # 超过1秒视为慢操作
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

        # 计算内存使用（近似值）
        try:
            import psutil

            process = psutil.Process()
            memory_usage_mb = process.memory_info().rss / 1024 / 1024
        except ImportError:
            memory_usage_mb = 0

        # 汇总指标
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

                        delay = self.base_delay * (2**attempt)  # 指数退避
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

    def __init__(self, default_ttl: int = 300, maxsize: int = 10000):
        self._cache: Dict[str, Any] = {}
        self._cache_ttl: Dict[str, float] = {}
        self._cache_access: Dict[str, float] = {}  # 用于 LRU
        self._hits = 0
        self._misses = 0
        self.default_ttl = default_ttl
        self.maxsize = maxsize

    def get(self, key: str) -> Any:
        """获取缓存值"""
        self._clean_expired()

        if key in self._cache_ttl and time.time() < self._cache_ttl[key]:
            self._hits += 1
            self._cache_access[key] = time.time()
            return self._cache.get(key)
        else:
            self._misses += 1
            self._cache.pop(key, None)
            self._cache_ttl.pop(key, None)
            self._cache_access.pop(key, None)
            return None

    def set(self, key: str, value: Any, ttl: int = None):
        """设置缓存值"""
        if ttl is None:
            ttl = self.default_ttl

        # LRU 清理
        if len(self._cache) >= self.maxsize:
            self._lru_cleanup()

        self._cache[key] = value
        self._cache_ttl[key] = time.time() + ttl
        self._cache_access[key] = time.time()

    def delete(self, key: str):
        """删除缓存值"""
        self._cache.pop(key, None)
        self._cache_ttl.pop(key, None)
        self._cache_access.pop(key, None)

    def _clean_expired(self):
        """清理过期缓存"""
        current_time = time.time()
        expired_keys = [
            key for key, expiry in self._cache_ttl.items() if current_time >= expiry
        ]
        for key in expired_keys:
            self._cache.pop(key, None)
            self._cache_ttl.pop(key, None)
            self._cache_access.pop(key, None)

    def _lru_cleanup(self):
        """LRU 清理"""
        if len(self._cache) <= self.maxsize:
            return

        # 清理 20% 的最久未使用条目
        items_to_remove = max(1, int(self.maxsize * 0.2))
        sorted_keys = sorted(self._cache_access.items(), key=lambda x: x[1])
        keys_to_remove = [key for key, _ in sorted_keys[:items_to_remove]]

        for key in keys_to_remove:
            self._cache.pop(key, None)
            self._cache_ttl.pop(key, None)
            self._cache_access.pop(key, None)

        logger.debug(f"LRU清理: 移除了 {len(keys_to_remove)} 个最久未使用的缓存")

    def clear_expired(self):
        """清理过期缓存"""
        self._clean_expired()

    def clear_all(self):
        """清理所有缓存"""
        self._cache.clear()
        self._cache_ttl.clear()
        self._cache_access.clear()
        logger.info("所有缓存已清理")

    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计"""
        total = self._hits + self._misses
        hit_rate = self._hits / total if total > 0 else 0

        return {
            "size": len(self._cache),
            "maxsize": self.maxsize,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": round(hit_rate, 3),
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

        # 任务完成后自动清理
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

        # 清理过期消息
        expired_messages = [
            msg_id
            for msg_id, timestamp in self._messages.items()
            if current_time - timestamp > self.ttl
        ]
        for msg_id in expired_messages:
            self._messages.pop(msg_id, None)

        # 检查重复
        if message_id in self._messages:
            return True

        # 记录新消息
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


# 错误处理装饰器
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


# 全局实例
performance_monitor = PerformanceMonitor()
retry_manager = RetryManager(max_retries=3, base_delay=1.0)
global_cache = GlobalCache(default_ttl=300, maxsize=10000)  # 添加 maxsize 参数
task_manager = TaskManager()
message_deduplicate = MessageDeduplicate(ttl=60)  # ✅ 只保留这一个实例


# 便捷装饰器
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


# ========== 缓存装饰器 ==========
def cached(ttl: int = 60, maxsize: int = 1000):
    """优化的缓存装饰器"""

    def decorator(func):
        # 为每个函数创建独立的缓存空间
        func_cache = {}
        func_cache_ttl = {}
        func_cache_access_time = {}

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            cache_key = _generate_safe_cache_key(func.__name__, args, kwargs)

            # 清理过期缓存
            _clean_expired_cache(func_cache, func_cache_ttl, func_cache_access_time)

            # LRU 清理
            if len(func_cache) >= maxsize:
                _lru_cleanup(
                    func_cache, func_cache_ttl, func_cache_access_time, maxsize
                )

            # 检查缓存
            if cache_key in func_cache_ttl:
                if time.time() < func_cache_ttl[cache_key]:
                    func_cache_access_time[cache_key] = time.time()
                    if hasattr(func, "_cache_hits"):
                        func._cache_hits += 1
                    return func_cache[cache_key]
                else:
                    func_cache.pop(cache_key, None)
                    func_cache_ttl.pop(cache_key, None)
                    func_cache_access_time.pop(cache_key, None)

            if hasattr(func, "_cache_misses"):
                func._cache_misses += 1

            try:
                result = await func(*args, **kwargs)
            except Exception as e:
                logger.error(f"缓存函数执行失败 {func.__name__}: {e}")
                raise

            if result is not None:
                func_cache[cache_key] = result
                func_cache_ttl[cache_key] = time.time() + ttl
                func_cache_access_time[cache_key] = time.time()

            return result

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            cache_key = _generate_safe_cache_key(func.__name__, args, kwargs)

            _clean_expired_cache(func_cache, func_cache_ttl, func_cache_access_time)

            if len(func_cache) >= maxsize:
                _lru_cleanup(
                    func_cache, func_cache_ttl, func_cache_access_time, maxsize
                )

            if cache_key in func_cache_ttl:
                if time.time() < func_cache_ttl[cache_key]:
                    func_cache_access_time[cache_key] = time.time()
                    if hasattr(func, "_cache_hits"):
                        func._cache_hits += 1
                    return func_cache[cache_key]
                else:
                    func_cache.pop(cache_key, None)
                    func_cache_ttl.pop(cache_key, None)
                    func_cache_access_time.pop(cache_key, None)

            if hasattr(func, "_cache_misses"):
                func._cache_misses += 1

            try:
                result = func(*args, **kwargs)
            except Exception as e:
                logger.error(f"缓存函数执行失败 {func.__name__}: {e}")
                raise

            if result is not None:
                func_cache[cache_key] = result
                func_cache_ttl[cache_key] = time.time() + ttl
                func_cache_access_time[cache_key] = time.time()

            return result

        wrapper = async_wrapper if inspect.iscoroutinefunction(func) else sync_wrapper

        # 添加缓存统计属性
        wrapper._cache_hits = 0
        wrapper._cache_misses = 0
        wrapper.cache_info = lambda: {
            "hits": wrapper._cache_hits,
            "misses": wrapper._cache_misses,
            "size": len(func_cache),
            "maxsize": maxsize,
            "ttl": ttl,
            "hit_rate": (
                wrapper._cache_hits / (wrapper._cache_hits + wrapper._cache_misses)
                if (wrapper._cache_hits + wrapper._cache_misses) > 0
                else 0
            ),
        }
        wrapper.cache_clear = lambda: (
            func_cache.clear(),
            func_cache_ttl.clear(),
            func_cache_access_time.clear(),
            setattr(wrapper, "_cache_hits", 0),
            setattr(wrapper, "_cache_misses", 0),
        )

        return wrapper

    return decorator


def _generate_safe_cache_key(func_name: str, args: tuple, kwargs: dict) -> str:
    """生成安全的缓存键"""
    try:
        key_data = {
            "func": func_name,
            "args": args,
            "kwargs": {k: v for k, v in sorted(kwargs.items())},
        }
        key_bytes = pickle.dumps(key_data, protocol=pickle.HIGHEST_PROTOCOL)
        return f"cache:{func_name}:{hashlib.md5(key_bytes).hexdigest()}"
    except:
        key_parts = [func_name]
        for arg in args:
            if arg is not None:
                key_parts.append(str(arg))
        if kwargs:
            for k, v in sorted(kwargs.items()):
                if v is not None:
                    key_parts.append(f"{k}={v}")
        key_str = ":".join(key_parts)
        return f"cache:{func_name}:{hashlib.md5(key_str.encode()).hexdigest()}"


def _clean_expired_cache(cache: dict, ttl_dict: dict, access_time: dict):
    """清理过期缓存"""
    current_time = time.time()
    expired_keys = [key for key, expiry in ttl_dict.items() if current_time >= expiry]

    for key in expired_keys:
        cache.pop(key, None)
        ttl_dict.pop(key, None)
        access_time.pop(key, None)


def _lru_cleanup(cache: dict, ttl_dict: dict, access_time: dict, maxsize: int):
    """LRU 清理"""
    if len(cache) <= maxsize:
        return

    items_to_remove = len(cache) - maxsize
    if items_to_remove <= 0:
        return

    sorted_keys = sorted(access_time.items(), key=lambda x: x[1])
    keys_to_remove = [key for key, _ in sorted_keys[:items_to_remove]]

    for key in keys_to_remove:
        cache.pop(key, None)
        ttl_dict.pop(key, None)
        access_time.pop(key, None)
