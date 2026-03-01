import time
import asyncio
import logging
import weakref
from typing import Dict, Any, Callable, Optional, List, Union
from functools import wraps
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
import sys

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
    
    def update(self, execution_time: float):
        """更新指标"""
        self.count += 1
        self.total_time += execution_time
        self.avg_time = self.total_time / self.count
        self.max_time = max(self.max_time, execution_time)
        self.min_time = min(self.min_time, execution_time)
        self.last_updated = time.time()


class PerformanceMonitor:
    """性能监控器 - 增强版"""

    def __init__(self, slow_threshold: float = 1.0):
        self.metrics: Dict[str, PerformanceMetrics] = {}
        self.slow_operations_count = 0
        self.start_time = time.time()
        self.slow_threshold = slow_threshold
        
        # 新增：记录最近10次慢操作
        self.recent_slow_ops: List[Dict[str, Any]] = []
        self.max_slow_logs = 10

    def track(self, operation_name: str):
        """性能跟踪装饰器 - 同时支持异步和同步"""
        def decorator(func):
            is_async = asyncio.iscoroutinefunction(func)
            
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                start = time.perf_counter()  # 使用更精确的时间
                try:
                    return await func(*args, **kwargs)
                finally:
                    execution_time = time.perf_counter() - start
                    self._record_metrics(operation_name, execution_time, func.__name__)

            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                start = time.perf_counter()
                try:
                    return func(*args, **kwargs)
                finally:
                    execution_time = time.perf_counter() - start
                    self._record_metrics(operation_name, execution_time, func.__name__)

            return async_wrapper if is_async else sync_wrapper

        return decorator

    @asynccontextmanager
    async def track_async_context(self, operation_name: str):
        """异步上下文管理器 - 用于跟踪代码块"""
        start = time.perf_counter()
        try:
            yield
        finally:
            execution_time = time.perf_counter() - start
            self._record_metrics(operation_name, execution_time, "context")

    def _record_metrics(self, operation_name: str, execution_time: float, func_name: str = ""):
        """记录性能指标 - 优化版"""
        if operation_name not in self.metrics:
            self.metrics[operation_name] = PerformanceMetrics()
        
        self.metrics[operation_name].update(execution_time)

        # 慢操作检测
        if execution_time > self.slow_threshold:
            self.slow_operations_count += 1
            
            # 记录慢操作详情
            slow_info = {
                "operation": operation_name,
                "function": func_name,
                "time": execution_time,
                "timestamp": time.time()
            }
            self.recent_slow_ops.append(slow_info)
            # 保持最近N条记录
            if len(self.recent_slow_ops) > self.max_slow_logs:
                self.recent_slow_ops.pop(0)
            
            logger.warning(
                f"⏱️ 慢操作检测 [{operation_name}] {execution_time:.3f}秒"
            )

    def get_metrics(self, operation_name: str) -> Optional[PerformanceMetrics]:
        """获取指定操作的性能指标"""
        return self.metrics.get(operation_name)

    def get_performance_report(self) -> Dict[str, Any]:
        """获取性能报告 - 增强版"""
        uptime = time.time() - self.start_time

        # 内存信息
        try:
            import psutil
            process = psutil.Process()
            memory_info = process.memory_info()
            memory_usage_mb = memory_info.rss / 1024 / 1024
            memory_percent = process.memory_percent()
        except ImportError:
            memory_usage_mb = 0
            memory_percent = 0

        # 指标汇总
        metrics_summary = {}
        total_operations = 0
        for op_name, metrics in self.metrics.items():
            if metrics.count > 0:
                metrics_summary[op_name] = {
                    "count": metrics.count,
                    "avg": round(metrics.avg_time, 3),
                    "max": round(metrics.max_time, 3),
                    "min": round(metrics.min_time if metrics.min_time != float("inf") else 0, 3),
                    "total": round(metrics.total_time, 3),
                }
                total_operations += metrics.count

        # 计算平均响应时间
        avg_response_time = sum(m.avg_time for m in self.metrics.values()) / len(self.metrics) if self.metrics else 0

        return {
            "uptime": self._format_uptime(uptime),
            "uptime_seconds": uptime,
            "memory_usage_mb": round(memory_usage_mb, 2),
            "memory_percent": round(memory_percent, 2),
            "slow_operations_count": self.slow_operations_count,
            "total_operations": total_operations,
            "avg_response_time": round(avg_response_time, 3),
            "metrics_summary": metrics_summary,
            "recent_slow_ops": self.recent_slow_ops.copy(),
        }

    def _format_uptime(self, seconds: float) -> str:
        """格式化运行时间"""
        days = int(seconds // 86400)
        hours = int((seconds % 86400) // 3600)
        minutes = int((seconds % 3600) // 60)
        if days > 0:
            return f"{days}天{hours}小时{minutes}分钟"
        elif hours > 0:
            return f"{hours}小时{minutes}分钟"
        else:
            return f"{minutes}分钟"

    def reset_metrics(self):
        """重置性能指标"""
        self.metrics.clear()
        self.slow_operations_count = 0
        self.recent_slow_ops.clear()
        self.start_time = time.time()


class RetryManager:
    """重试管理器 - 增强版"""

    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, 
                 max_delay: float = 60.0, jitter: bool = True):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.jitter = jitter  # 增加随机抖动避免同时重试

    def with_retry(self, operation_name: str = "unknown", 
                   retry_on: Optional[List[Exception]] = None):
        """重试装饰器 - 可指定重试的异常类型"""
        def decorator(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                last_exception = None
                for attempt in range(self.max_retries + 1):
                    try:
                        return await func(*args, **kwargs)
                    except Exception as e:
                        last_exception = e
                        
                        # 检查是否需要重试此异常
                        if retry_on and not any(isinstance(e, exc) for exc in retry_on):
                            raise e
                            
                        if attempt == self.max_retries:
                            break

                        # 计算延迟（带随机抖动）
                        delay = self.base_delay * (2 ** attempt)
                        delay = min(delay, self.max_delay)
                        
                        if self.jitter:
                            import random
                            delay = delay * (0.5 + random.random())
                        
                        logger.warning(
                            f"🔄 重试 {operation_name} "
                            f"(尝试 {attempt + 1}/{self.max_retries}): {e}"
                        )
                        await asyncio.sleep(delay)

                logger.error(
                    f"❌ {operation_name} 重试{self.max_retries}次后失败: {last_exception}"
                )
                raise last_exception

            return async_wrapper

        return decorator


class GlobalCache:
    """全局缓存管理器 - 增强版"""

    def __init__(self, default_ttl: int = 300, max_size: int = 1000):
        self._cache: Dict[str, Any] = {}
        self._cache_ttl: Dict[str, float] = {}
        self._hits = 0
        self._misses = 0
        self.default_ttl = default_ttl
        self.max_size = max_size
        self._access_order: List[str] = []  # LRU跟踪

    def get(self, key: str) -> Any:
        """获取缓存值 - LRU感知"""
        current_time = time.time()
        
        if key in self._cache_ttl and current_time < self._cache_ttl[key]:
            self._hits += 1
            # 更新访问顺序（LRU）
            if key in self._access_order:
                self._access_order.remove(key)
            self._access_order.append(key)
            return self._cache.get(key)
        else:
            self._misses += 1
            self._cache.pop(key, None)
            self._cache_ttl.pop(key, None)
            if key in self._access_order:
                self._access_order.remove(key)
            return None

    def set(self, key: str, value: Any, ttl: int = None):
        """设置缓存值 - 带LRU淘汰"""
        if ttl is None:
            ttl = self.default_ttl

        # 检查容量，执行LRU淘汰
        if len(self._cache) >= self.max_size and key not in self._cache:
            # 淘汰最早访问的1/4
            remove_count = self.max_size // 4
            for old_key in self._access_order[:remove_count]:
                self._cache.pop(old_key, None)
                self._cache_ttl.pop(old_key, None)
            self._access_order = self._access_order[remove_count:]

        self._cache[key] = value
        self._cache_ttl[key] = time.time() + ttl
        
        # 更新访问顺序
        if key in self._access_order:
            self._access_order.remove(key)
        self._access_order.append(key)

    def get_or_set(self, key: str, factory: Callable, ttl: int = None) -> Any:
        """获取或设置（原子操作）"""
        value = self.get(key)
        if value is None:
            value = factory()
            self.set(key, value, ttl)
        return value

    async def get_or_set_async(self, key: str, factory: Callable, ttl: int = None) -> Any:
        """异步获取或设置"""
        value = self.get(key)
        if value is None:
            if asyncio.iscoroutinefunction(factory):
                value = await factory()
            else:
                value = factory()
            self.set(key, value, ttl)
        return value

    def delete(self, key: str):
        """删除缓存值"""
        self._cache.pop(key, None)
        self._cache_ttl.pop(key, None)
        if key in self._access_order:
            self._access_order.remove(key)

    def clear_expired(self):
        """清理过期缓存 - 优化版"""
        current_time = time.time()
        expired_keys = [
            key for key, expiry in self._cache_ttl.items() 
            if current_time >= expiry
        ]
        for key in expired_keys:
            self._cache.pop(key, None)
            self._cache_ttl.pop(key, None)
            if key in self._access_order:
                self._access_order.remove(key)

        if expired_keys:
            logger.debug(f"🧹 清理了 {len(expired_keys)} 个过期缓存")

    def clear_all(self):
        """清理所有缓存"""
        self._cache.clear()
        self._cache_ttl.clear()
        self._access_order.clear()
        logger.info("🧹 所有缓存已清理")

    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计"""
        total = self._hits + self._misses
        hit_rate = self._hits / total if total > 0 else 0

        return {
            "size": len(self._cache),
            "max_size": self.max_size,
            "usage_percent": round(len(self._cache) / self.max_size * 100, 2),
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": round(hit_rate, 3),
            "total_operations": total,
        }


class TaskManager:
    """任务管理器 - 增强版"""

    def __init__(self):
        self._tasks: Dict[str, asyncio.Task] = {}
        self._task_count = 0
        self._cleanup_interval = 300
        self._last_cleanup = time.time()

    def create_task(self, coro, name: str = None) -> asyncio.Task:
        """创建并跟踪任务"""
        if not name:
            self._task_count += 1
            name = f"task_{self._task_count}"

        task = asyncio.create_task(coro, name=name)
        self._tasks[name] = task

        # 使用 weakref 避免循环引用
        def cleanup_callback(t):
            self._tasks.pop(name, None)

        task.add_done_callback(cleanup_callback)

        # 自动清理
        self._maybe_cleanup()
        
        return task

    async def cancel_task(self, name: str, wait: bool = True):
        """取消指定任务"""
        task = self._tasks.get(name)
        if task and not task.done():
            task.cancel()
            if wait:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            self._tasks.pop(name, None)

    async def cancel_all_tasks(self, wait: bool = True):
        """取消所有任务"""
        tasks_to_cancel = list(self._tasks.values())
        for task in tasks_to_cancel:
            if not task.done():
                task.cancel()

        if wait and tasks_to_cancel:
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
        
        self._tasks.clear()

    def get_task_count(self) -> int:
        """获取任务数量"""
        self._maybe_cleanup()
        return len(self._tasks)

    def get_active_tasks(self) -> List[str]:
        """获取活跃任务列表"""
        self._maybe_cleanup()
        return [name for name, task in self._tasks.items() if not task.done()]

    def _maybe_cleanup(self):
        """按需清理"""
        now = time.time()
        if now - self._last_cleanup > self._cleanup_interval:
            self._cleanup_finished_tasks()
            self._last_cleanup = now

    def _cleanup_finished_tasks(self):
        """清理已完成的任务"""
        completed_tasks = [
            name for name, task in self._tasks.items() 
            if task.done()
        ]
        for name in completed_tasks:
            self._tasks.pop(name, None)

        if completed_tasks:
            logger.debug(f"🧹 清理了 {len(completed_tasks)} 个已完成任务")


class MessageDeduplicate:
    """消息去重管理器 - 增强版"""

    def __init__(self, ttl: int = 60, max_size: int = 1000):
        self._messages: Dict[str, float] = {}
        self.ttl = ttl
        self.max_size = max_size
        self._hits = 0
        self._total = 0

    def is_duplicate(self, message_id: str) -> bool:
        """检查消息是否重复 - 带容量控制"""
        self._total += 1
        current_time = time.time()

        # 清理过期
        self._clear_expired(current_time)

        # 检查是否重复
        if message_id in self._messages:
            self._hits += 1
            return True

        # 容量控制（超过80%时清理最旧的20%）
        if len(self._messages) >= self.max_size * 0.8:
            self._trim_oldest()

        self._messages[message_id] = current_time
        return False

    def _clear_expired(self, current_time: float = None):
        """清理过期消息"""
        if current_time is None:
            current_time = time.time()
            
        expired = [
            msg_id for msg_id, timestamp in self._messages.items()
            if current_time - timestamp > self.ttl
        ]
        for msg_id in expired:
            self._messages.pop(msg_id, None)

    def _trim_oldest(self):
        """清理最旧的20%消息"""
        if not self._messages:
            return
            
        # 按时间戳排序
        sorted_items = sorted(self._messages.items(), key=lambda x: x[1])
        remove_count = max(1, len(sorted_items) // 5)
        
        for msg_id, _ in sorted_items[:remove_count]:
            self._messages.pop(msg_id, None)
            
        logger.debug(f"🧹 LRU清理了 {remove_count} 条消息记录")

    def get_stats(self) -> Dict[str, Any]:
        """获取去重统计"""
        return {
            "size": len(self._messages),
            "max_size": self.max_size,
            "usage_percent": round(len(self._messages) / self.max_size * 100, 2),
            "dedup_rate": round(self._hits / self._total * 100, 2) if self._total > 0 else 0,
            "hits": self._hits,
            "total": self._total,
        }


def handle_database_errors(func=None, *, retry: int = 0):
    """数据库错误处理装饰器 - 带可选重试"""
    def decorator(f):
        @wraps(f)
        async def async_wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(retry + 1):
                try:
                    return await f(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    if attempt < retry:
                        await asyncio.sleep(1 * (2 ** attempt))
                        continue
                    logger.error(f"❌ 数据库操作失败 {f.__name__}: {e}")
                    raise
        return async_wrapper
    
    if func is None:
        return decorator
    return decorator(func)


def handle_telegram_errors(func=None, *, retry: int = 0):
    """Telegram API错误处理装饰器 - 带可选重试"""
    def decorator(f):
        @wraps(f)
        async def async_wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(retry + 1):
                try:
                    return await f(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    if attempt < retry:
                        await asyncio.sleep(1 * (2 ** attempt))
                        continue
                    logger.error(f"❌ Telegram API操作失败 {f.__name__}: {e}")
                    raise
        return async_wrapper
    
    if func is None:
        return decorator
    return decorator(func)


# 全局实例
performance_monitor = PerformanceMonitor(slow_threshold=1.0)
retry_manager = RetryManager(max_retries=3, base_delay=1.0)
global_cache = GlobalCache(default_ttl=300, max_size=1000)
task_manager = TaskManager()
message_deduplicate = MessageDeduplicate(ttl=60, max_size=1000)


def track_performance(operation_name: str):
    """性能跟踪装饰器"""
    return performance_monitor.track(operation_name)


def with_retry(operation_name: str = "unknown", max_retries: int = 3, 
               retry_on: Optional[List[Exception]] = None):
    """重试装饰器"""
    retry_mgr = RetryManager(max_retries=max_retries)
    return retry_mgr.with_retry(operation_name, retry_on)


def message_deduplicate_decorator(ttl: int = 60):
    """消息去重装饰器"""
    # 每个装饰器实例有自己的去重器
    deduplicate = MessageDeduplicate(ttl=ttl)

    def decorator(func):
        @wraps(func)
        async def wrapper(message, *args, **kwargs):
            message_id = f"{message.chat.id}_{message.message_id}"
            if deduplicate.is_duplicate(message_id):
                logger.debug(f"⏭️ 跳过重复消息: {message_id}")
                return
            return await func(message, *args, **kwargs)

        return wrapper

    return decorator


# 为了保持向后兼容
message_deduplicate = message_deduplicate_decorator()