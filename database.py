import logging
import asyncio
import time
from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional, Union
from config import Config, beijing_tz
import asyncpg
from asyncpg.pool import Pool

logger = logging.getLogger("GroupCheckInBot")


class PostgreSQLDatabase:
    """PostgreSQL数据库管理器"""

    def __init__(self, database_url: str = None):
        self.database_url = database_url or Config.DATABASE_URL
        self.pool: Optional[Pool] = None
        self._initialized = False
        self._cache = {}
        self._cache_ttl = {}

        # 重连相关属性
        self._last_connection_check = 0
        self._connection_check_interval = 30
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 5
        self._reconnect_base_delay = 1.0

        self._maintenance_running = False
        self._maintenance_task = None
        self._connection_maintenance_task = None

        self._cache_max_size = 10000
        self._cache_access_order = []

    # ========== 重连机制 ==========
    async def _ensure_healthy_connection(self):
        """确保连接健康"""
        current_time = time.time()
        if current_time - self._last_connection_check < self._connection_check_interval:
            return True

        try:
            is_healthy = await self.connection_health_check()
            if not is_healthy:
                logger.warning("数据库连接不健康，尝试重连...")
                await self._reconnect()

            self._last_connection_check = current_time
            return True
        except Exception as e:
            logger.error(f"数据库连接检查失败: {e}")
            return False

    async def _reconnect(self):
        """重新建立数据库连接"""
        self._reconnect_attempts += 1

        if self._reconnect_attempts > self._max_reconnect_attempts:
            logger.error(
                f"数据库重连尝试次数超过上限 ({self._max_reconnect_attempts})，停止重连"
            )
            raise ConnectionError("数据库重连失败")

        try:
            delay = self._reconnect_base_delay * (
                2 ** (self._reconnect_attempts - 1)
            )  # 指数退避
            logger.info(f"{delay}秒后尝试第{self._reconnect_attempts}次数据库重连...")
            await asyncio.sleep(delay)

            # 关闭旧连接
            if self.pool:
                await self.pool.close()

            # 重新初始化
            self.pool = None
            self._initialized = False
            await self._initialize_impl()

            # 重置重连计数
            self._reconnect_attempts = 0
            logger.info("✅ 数据库重连成功")

        except Exception as e:
            logger.error(f"数据库第{self._reconnect_attempts}次重连失败: {e}")
            if self._reconnect_attempts >= self._max_reconnect_attempts:
                logger.critical("数据库重连最终失败，服务可能无法正常工作")
            raise

    async def execute_with_retry(
        self,
        operation_name: str,
        query: str,
        *args,
        fetch: bool = False,
        fetchrow: bool = False,
        fetchval: bool = False,  # 🆕 新增 fetchval 支持
        max_retries: int = 2,
        timeout: int = 30,
        slow_threshold: float = 1.0,  # 🆕 可配置慢查询阈值
    ):
        """带重试和超时的查询执行 - 终极优化版"""
        if not await self._ensure_healthy_connection():
            raise ConnectionError("数据库连接不健康")

        # 🆕 验证参数组合
        if sum([fetch, fetchrow, fetchval]) > 1:
            raise ValueError("只能指定一种查询类型: fetch, fetchrow 或 fetchval")

        for attempt in range(max_retries + 1):
            start_time = time.time()
            try:
                async with self.pool.acquire() as conn:
                    await conn.execute(f"SET statement_timeout = {timeout * 1000}")

                    if fetch:
                        result = await conn.fetch(query, *args)
                    elif fetchrow:
                        result = await conn.fetchrow(query, *args)
                    elif fetchval:
                        result = await conn.fetchval(query, *args)
                    else:
                        result = await conn.execute(query, *args)

                    execution_time = time.time() - start_time

                    # 🆕 动态慢查询日志
                    if execution_time > slow_threshold:
                        log_level = (
                            logging.WARNING if execution_time < 5.0 else logging.ERROR
                        )
                        logger.log(
                            log_level,
                            f"⏱️ 慢查询: {operation_name} 耗时 {execution_time:.3f}秒 "
                            f"(SQL: {query[:100]}{'...' if len(query) > 100 else ''})",
                        )

                    return result

            except (
                asyncpg.PostgresConnectionError,
                asyncpg.ConnectionDoesNotExistError,
                asyncpg.InterfaceError,
                ConnectionError,
            ) as e:
                if attempt == max_retries:
                    logger.error(
                        f"{operation_name} 数据库重试{max_retries}次后失败: {e}"
                    )
                    raise

                retry_delay = min(1 * (2**attempt), 5)
                logger.warning(
                    f"{operation_name} 数据库连接异常，{retry_delay}秒后第{attempt + 1}次重试: {e}"
                )
                await self._reconnect()
                await asyncio.sleep(retry_delay)

            except asyncpg.QueryCanceledError:
                logger.error(f"{operation_name} 查询超时被取消 (超时设置: {timeout}秒)")
                if attempt == max_retries:
                    raise
                await asyncio.sleep(1)

            except Exception as e:
                # 🆕 区分数据库错误和其他错误
                if "database" in str(e).lower() or "sql" in str(e).lower():
                    logger.error(f"{operation_name} 数据库操作失败: {e}")
                else:
                    logger.error(f"{operation_name} 操作失败: {e}")
                raise

    async def fetch_with_retry(
        self, operation_name: str, query: str, *args, max_retries: int = 2
    ):
        """带重试的查询获取"""
        for attempt in range(max_retries + 1):
            try:
                # 确保连接健康
                if not await self._ensure_healthy_connection():
                    raise ConnectionError("数据库连接不健康")

                async with self.pool.acquire() as conn:
                    return await conn.fetch(query, *args)

            except (
                asyncpg.PostgresConnectionError,
                asyncpg.ConnectionDoesNotExistError,
                asyncpg.InterfaceError,
                ConnectionError,
            ) as e:
                if attempt == max_retries:
                    logger.error(
                        f"{operation_name} 数据库重试{max_retries}次后失败: {e}"
                    )
                    raise

                logger.warning(
                    f"{operation_name} 数据库连接异常，第{attempt + 1}次重试: {e}"
                )
                await self._reconnect()
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"{operation_name} 数据库查询失败: {e}")
                raise

    async def fetchrow_with_retry(
        self, operation_name: str, query: str, *args, max_retries: int = 2
    ):
        """带重试的单行查询"""
        for attempt in range(max_retries + 1):
            try:
                # 确保连接健康
                if not await self._ensure_healthy_connection():
                    raise ConnectionError("数据库连接不健康")

                async with self.pool.acquire() as conn:
                    return await conn.fetchrow(query, *args)

            except (
                asyncpg.PostgresConnectionError,
                asyncpg.ConnectionDoesNotExistError,
                asyncpg.InterfaceError,
                ConnectionError,
            ) as e:
                if attempt == max_retries:
                    logger.error(
                        f"{operation_name} 数据库重试{max_retries}次后失败: {e}"
                    )
                    raise

                logger.warning(
                    f"{operation_name} 数据库连接异常，第{attempt + 1}次重试: {e}"
                )
                await self._reconnect()
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"{operation_name} 数据库查询失败: {e}")
                raise

    # ========== 定期维护任务 ==========
    async def start_connection_maintenance(self):
        """启动连接维护任务"""
        if hasattr(self, "_maintenance_running") and self._maintenance_running:
            logger.info("连接维护任务已在运行")
            return

        self._maintenance_running = True
        self._maintenance_task = asyncio.create_task(
            self._connection_maintenance_loop()
        )
        logger.info("✅ 数据库连接维护任务已启动")

    async def stop_connection_maintenance(self):
        """停止连接维护任务"""
        self._maintenance_running = False
        if hasattr(self, "_maintenance_task") and self._maintenance_task:
            self._maintenance_task.cancel()
            try:
                await self._maintenance_task
            except asyncio.CancelledError:
                pass
            self._maintenance_task = None
        logger.info("数据库连接维护任务已停止")

    async def _connection_maintenance_loop(self):
        """连接维护循环"""
        logger.info("开始数据库连接维护循环...")

        while self._maintenance_running:
            try:
                await asyncio.sleep(60)  # 每分钟检查一次

                # 执行连接健康检查
                if not await self._ensure_healthy_connection():
                    logger.warning("连接维护: 数据库连接不健康")

                # 清理过期缓存
                await self.cleanup_cache()

                # 定期清理月度数据（可选）
                current_time = time.time()
                if current_time % 3600 < 60:  # 每小时执行一次
                    try:
                        await self.cleanup_old_data(days=Config.DATA_RETENTION_DAYS)
                        logger.debug("定期数据清理完成")
                    except Exception as e:
                        logger.error(f"定期数据清理失败: {e}")

            except asyncio.CancelledError:
                logger.info("数据库连接维护任务被取消")
                break
            except Exception as e:
                logger.error(f"连接维护任务异常: {e}")
                await asyncio.sleep(30)  # 异常后等待30秒再继续

    # ========== 时区相关方法 ==========
    def get_beijing_time(self):
        """获取北京时间"""
        return datetime.now(beijing_tz)

    def get_beijing_date(self):
        """获取北京日期"""
        return self.get_beijing_time().date()

    # ========= 统一时间 =========
    async def get_reset_period_date(
        self, chat_id: int, target_datetime: datetime = None
    ) -> date:
        """根据群组重置时间获取重置周期日期"""
        if target_datetime is None:
            target_datetime = self.get_beijing_time()

        try:
            group_data = await self.get_group_cached(chat_id)
            if not group_data:
                await self.init_group(chat_id)
                group_data = await self.get_group_cached(chat_id)

            reset_hour = group_data.get("reset_hour", Config.DAILY_RESET_HOUR)
            reset_minute = group_data.get("reset_minute", Config.DAILY_RESET_MINUTE)

            # 计算今天的重置时间点
            reset_time_today = target_datetime.replace(
                hour=reset_hour, minute=reset_minute, second=0, microsecond=0
            )

            # 判断当前时间与重置时间的关系
            if target_datetime < reset_time_today:
                # 当前时间在今天重置时间之前，属于昨天的周期
                return (reset_time_today - timedelta(days=1)).date()
            else:
                # 当前时间在今天重置时间之后，属于今天的周期
                return reset_time_today.date()

        except Exception as e:
            logger.error(f"计算重置周期日期失败 {chat_id}: {e}")
            # 出错时返回自然日
            return target_datetime.date()

    async def has_reset_executed_today(
        self, chat_id: int, target_datetime: datetime = None
    ) -> tuple[bool, str]:
        """
        返回：(是否执行过, 原因)
        """
        if target_datetime is None:
            target_datetime = self.get_beijing_time()

        today = target_datetime.date()

        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            # 获取当前配置和最后记录
            row = await conn.fetchrow(
                """
                SELECT 
                    g.reset_hour,
                    g.reset_minute,
                    g.last_reset_date,
                    g.last_reset_config
                FROM groups g
                WHERE g.chat_id = $1
                """,
                chat_id,
            )

            if not row or not row["last_reset_date"]:
                return False, "从未重置过"

            last_date = row["last_reset_date"]
            last_config = row["last_reset_config"] or ""
            current_config = f"{row['reset_hour']}:{row['reset_minute']}"

            if last_date != today:
                return False, "今天未重置过"

            if last_config != current_config:
                return False, "重置配置已变更"

            return True, "今天已按相同配置重置过"

    async def get_last_reset_info(self, chat_id: int) -> Optional[Dict]:
        """获取群组最后一次重置的信息"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT last_reset_date, last_reset_config 
                FROM groups 
                WHERE chat_id = $1
                """,
                chat_id,
            )
            if not row or not row["last_reset_date"]:
                return None

            info = {
                "reset_date": row["last_reset_date"],
                "reset_config": row["last_reset_config"],
            }

            # 解析配置字符串
            if row["last_reset_config"]:
                try:
                    hour_str, minute_str = row["last_reset_config"].split(":")
                    info["reset_hour"] = int(hour_str)
                    info["reset_minute"] = int(minute_str)
                except:
                    pass

            return info

    async def mark_reset_executed(
        self,
        chat_id: int,
        reset_hour: int,
        reset_minute: int,
        execution_time: datetime = None,
    ):
        """标记重置已执行"""
        if execution_time is None:
            execution_time = self.get_beijing_time()

        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE groups SET 
                    last_reset_date = $1,
                    last_reset_config = $2,
                    updated_at = CURRENT_TIMESTAMP
                WHERE chat_id = $3
                """,
                execution_time.date(),
                f"{reset_hour}:{reset_minute}",
                chat_id,
            )
            logger.debug(
                f"重置标记已保存: 群组{chat_id}, 日期{execution_time.date()}, 配置{reset_hour}:{reset_minute}"
            )

    async def clear_today_reset_record(self, chat_id: int):
        """清除今天的重置记录（用于重置时间变更时）"""
        today = self.get_beijing_time().date()

        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE groups SET 
                    last_reset_date = NULL,
                    last_reset_config = NULL
                WHERE chat_id = $1 AND last_reset_date = $2
                """,
                chat_id,
                today,
            )
            logger.info(f"清除今天重置记录: 群组{chat_id}")

    async def has_work_record_in_period(
        self, chat_id: int, user_id: int, checkin_type: str, target_datetime: datetime
    ) -> bool:
        """检查当前重置周期内是否有上下班记录"""
        try:
            period_date = await self.get_reset_period_date(chat_id, target_datetime)

            self._ensure_pool_initialized()
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT 1 FROM work_records 
                    WHERE chat_id = $1 AND user_id = $2 
                    AND checkin_type = $3 AND record_date = $4
                    """,
                    chat_id,
                    user_id,
                    checkin_type,
                    period_date,
                )
                return row is not None
        except Exception as e:
            logger.error(f"检查工作记录失败 {chat_id}-{user_id}: {e}")
            return False

    async def get_work_records_in_period(
        self, chat_id: int, user_id: int, period_date: date
    ) -> Dict[str, Dict]:
        """获取当前重置周期的上下班记录"""
        try:
            self._ensure_pool_initialized()
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT * FROM work_records 
                    WHERE chat_id = $1 AND user_id = $2 AND record_date = $3
                    ORDER BY checkin_type
                    """,
                    chat_id,
                    user_id,
                    period_date,
                )

                records = {}
                for row in rows:
                    records[row["checkin_type"]] = dict(row)
                return records
        except Exception as e:
            logger.error(f"获取工作记录失败 {chat_id}-{user_id}: {e}")
            return {}

    # ========== 初始化方法 ==========
    async def initialize(self):
        """初始化数据库"""
        if self._initialized:
            return

        max_retries = 5
        for attempt in range(max_retries):
            try:
                logger.info(f"连接PostgreSQL数据库 (尝试 {attempt + 1}/{max_retries})")
                await self._initialize_impl()
                logger.info("PostgreSQL数据库初始化完成")
                self._initialized = True
                return
            except Exception as e:
                logger.warning(f"数据库初始化第 {attempt + 1} 次失败: {e}")
                if attempt == max_retries - 1:
                    logger.error(f"数据库初始化重试{max_retries}次后失败: {e}")
                    # 尝试强制重建表
                    try:
                        await self._force_recreate_tables()
                        self._initialized = True
                        logger.info("✅ 数据库表强制重建成功")
                        return
                    except Exception as rebuild_error:
                        logger.error(f"数据库表强制重建失败: {rebuild_error}")
                        raise e
                await asyncio.sleep(2**attempt)

    async def _initialize_impl(self):
        """实际的数据库初始化实现"""
        self.pool = await asyncpg.create_pool(
            self.database_url,
            min_size=Config.DB_MIN_CONNECTIONS,
            max_size=Config.DB_MAX_CONNECTIONS,
            max_inactive_connection_lifetime=Config.DB_POOL_RECYCLE,
            command_timeout=Config.DB_CONNECTION_TIMEOUT,
            timeout=60,
        )
        logger.info("PostgreSQL连接池创建成功")

        async with self.pool.acquire() as conn:
            await conn.execute("SET statement_timeout = 30000")
            await conn.execute("SET idle_in_transaction_session_timeout = 60000")

        # 创建表和索引 - 添加重试机制
        max_retries = 3
        for attempt in range(max_retries):
            try:
                await self._create_tables()
                await self._create_indexes()
                await self._initialize_default_data()
                logger.info("✅ 数据库表初始化完成")
                break
            except Exception as e:
                logger.warning(f"数据库表初始化第 {attempt + 1} 次失败: {e}")
                if attempt == max_retries - 1:
                    logger.error("数据库表初始化最终失败，尝试强制重建...")
                    await self._force_recreate_tables()
                await asyncio.sleep(1)

    async def _force_recreate_tables(self):
        """强制重新创建所有表（用于修复损坏的数据库）"""
        logger.warning("🔄 强制重新创建数据库表...")

        async with self.pool.acquire() as conn:
            # 删除所有表（按依赖顺序）
            tables = [
                "monthly_statistics",
                "activity_user_limits",
                "push_settings",
                "work_fine_configs",
                "fine_configs",
                "activity_configs",
                "work_records",
                "user_activities",
                "users",
                "groups",
            ]

            for table in tables:
                try:
                    await conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                    logger.info(f"✅ 删除表: {table}")
                except Exception as e:
                    logger.warning(f"删除表 {table} 失败: {e}")

            # 重新创建表
            await self._create_tables()
            await self._create_indexes()
            await self._initialize_default_data()
            logger.info("🎉 数据库表强制重建完成")

    def _extract_table_name(self, table_sql: str) -> str:
        """安全提取表名"""
        try:
            # 使用更稳定的方式提取表名
            words = table_sql.upper().split()
            if "TABLE" in words:
                table_index = words.index("TABLE") + 1
                if table_index < len(words) and words[table_index] == "IF":
                    table_index += 3  # 跳过 IF NOT EXISTS
                elif table_index < len(words) and words[table_index] == "NOT":
                    table_index += 2  # 跳过 NOT EXISTS
                return words[table_index] if table_index < len(words) else "unknown"
        except Exception:
            pass
        return "unknown"

    async def _create_tables(self):
        """创建所有必要的表"""
        async with self.pool.acquire() as conn:
            tables = [
                # groups表
                """
                CREATE TABLE IF NOT EXISTS groups (
                    chat_id BIGINT PRIMARY KEY,
                    channel_id BIGINT,
                    notification_group_id BIGINT,
                    reset_hour INTEGER DEFAULT 0,
                    reset_minute INTEGER DEFAULT 0,
                    work_start_time TEXT DEFAULT '09:00',
                    work_end_time TEXT DEFAULT '18:00',
                    last_reset_date DATE,
                    last_reset_config TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,
                # users表
                """
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT NOT NULL,
                    user_id BIGINT NOT NULL,
                    nickname TEXT,
                    current_activity TEXT,
                    activity_start_time TEXT,
                    total_accumulated_time INTEGER DEFAULT 0,
                    total_activity_count INTEGER DEFAULT 0,
                    total_fines INTEGER DEFAULT 0,
                    overtime_count INTEGER DEFAULT 0,
                    total_overtime_time INTEGER DEFAULT 0,
                    last_updated DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(chat_id, user_id)
                )
                """,
                # user_activities表
                """
                CREATE TABLE IF NOT EXISTS user_activities (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT,
                    user_id BIGINT,
                    activity_date DATE,
                    activity_name TEXT,
                    activity_count INTEGER DEFAULT 0,
                    accumulated_time INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(chat_id, user_id, activity_date, activity_name)
                )
                """,
                # work_records表
                """
                CREATE TABLE IF NOT EXISTS work_records (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT,
                    user_id BIGINT,
                    record_date DATE,
                    checkin_type TEXT,
                    checkin_time TEXT,
                    status TEXT,
                    time_diff_minutes REAL,
                    fine_amount INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(chat_id, user_id, record_date, checkin_type)
                )
                """,
                # activity_configs表
                """
                CREATE TABLE IF NOT EXISTS activity_configs (
                    activity_name TEXT PRIMARY KEY,
                    max_times INTEGER,
                    time_limit INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,
                # fine_configs表
                """
                CREATE TABLE IF NOT EXISTS fine_configs (
                    id SERIAL PRIMARY KEY,
                    activity_name TEXT,
                    time_segment TEXT,
                    fine_amount INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(activity_name, time_segment)
                )
                """,
                # work_fine_configs表
                """
                CREATE TABLE IF NOT EXISTS work_fine_configs (
                    id SERIAL PRIMARY KEY,
                    checkin_type TEXT,
                    time_segment TEXT,
                    fine_amount INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(checkin_type, time_segment)
                )
                """,
                # push_settings表
                """
                CREATE TABLE IF NOT EXISTS push_settings (
                    setting_key TEXT PRIMARY KEY,
                    setting_value INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,
                # monthly_statistics表
                """
                CREATE TABLE IF NOT EXISTS monthly_statistics (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT,
                    user_id BIGINT,
                    statistic_date DATE,
                    activity_name TEXT,
                    activity_count INTEGER DEFAULT 0,
                    accumulated_time INTEGER DEFAULT 0,
                    work_days INTEGER DEFAULT 0,
                    work_hours INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(chat_id, user_id, statistic_date, activity_name)
                )
                """,
                # activity_user_limits表
                """
                CREATE TABLE IF NOT EXISTS activity_user_limits (
                    activity_name TEXT PRIMARY KEY,
                    max_users INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,
            ]

            for table_sql in tables:
                try:
                    await conn.execute(table_sql)
                    table_name = self._extract_table_name(table_sql)
                    logger.info(f"✅ 创建表: {table_name}")
                except Exception as e:
                    logger.error(f"❌ 创建表失败: {e}")
                    # 记录失败的SQL用于调试
                    logger.error(f"失败的SQL: {table_sql[:100]}...")
                    raise  # 重新抛出异常让上层处理
            logger.info("数据库表创建完成")

    async def _create_indexes(self):
        """创建性能索引"""
        async with self.pool.acquire() as conn:
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_user_activities_main ON user_activities (chat_id, user_id, activity_date)",
                "CREATE INDEX IF NOT EXISTS idx_work_records_main ON work_records (chat_id, user_id, record_date)",
                "CREATE INDEX IF NOT EXISTS idx_users_main ON users (chat_id, user_id)",
                "CREATE INDEX IF NOT EXISTS idx_monthly_stats_main ON monthly_statistics (chat_id, user_id, statistic_date)",
            ]

            for index_sql in indexes:
                try:
                    await conn.execute(index_sql)
                    index_name = index_sql.split()[5]  # 获取索引名
                    logger.info(f"✅ 创建索引: {index_name}")
                except Exception as e:
                    logger.warning(f"创建索引失败: {e}")
                    # 索引创建失败不阻止程序启动
            logger.info("数据库索引创建完成")

    async def _initialize_default_data(self):
        """初始化默认数据"""
        async with self.pool.acquire() as conn:
            # 初始化活动配置
            for activity, limits in Config.DEFAULT_ACTIVITY_LIMITS.items():
                await conn.execute(
                    "INSERT INTO activity_configs (activity_name, max_times, time_limit) VALUES ($1, $2, $3) ON CONFLICT (activity_name) DO NOTHING",
                    activity,
                    limits["max_times"],
                    limits["time_limit"],
                )
                logger.info(f"✅ 初始化活动配置: {activity}")

            # 初始化罚款配置
            for activity, fines in Config.DEFAULT_FINE_RATES.items():
                for time_segment, amount in fines.items():
                    await conn.execute(
                        "INSERT INTO fine_configs (activity_name, time_segment, fine_amount) VALUES ($1, $2, $3) ON CONFLICT (activity_name, time_segment) DO NOTHING",
                        activity,
                        time_segment,
                        amount,
                    )
                logger.info(f"✅ 初始化罚款配置: {activity}")

            # 初始化推送设置
            for key, value in Config.AUTO_EXPORT_SETTINGS.items():
                await conn.execute(
                    "INSERT INTO push_settings (setting_key, setting_value) VALUES ($1, $2) ON CONFLICT (setting_key) DO NOTHING",
                    key,
                    1 if value else 0,
                )
                logger.info(f"✅ 初始化推送设置: {key}")

            logger.info("默认数据初始化完成")

    async def health_check(self) -> bool:
        """完整的数据库健康检查 - 增强版"""
        if not self.pool or not self._initialized:
            logger.warning("数据库未初始化")
            return False

        try:
            async with self.pool.acquire() as conn:
                # 检查连接是否有效
                result = await conn.fetchval("SELECT 1")
                if result != 1:
                    return False

                # 检查关键表是否存在且可访问
                critical_tables = ["users", "groups", "activity_configs"]
                for table in critical_tables:
                    try:
                        await conn.fetchval(f"SELECT 1 FROM {table} LIMIT 1")
                    except Exception as e:
                        logger.error(f"❌ 关键表 {table} 访问失败: {e}")
                        return False

                return True
        except Exception as e:
            logger.error(f"❌ 数据库健康检查失败: {e}")
            return False

    # ========== 连接管理 ==========
    def _ensure_pool_initialized(self):
        """确保连接池已初始化"""
        if not self.pool or not self._initialized:
            raise RuntimeError("数据库连接池尚未初始化，请先调用 initialize() 方法")

    async def get_connection(self):
        """获取数据库连接"""
        self._ensure_pool_initialized()
        return await self.pool.acquire()

    async def close(self):
        """关闭数据库连接"""
        try:
            if self.pool:
                await self.pool.close()
                logger.info("PostgreSQL连接池已关闭")
        except Exception as e:
            logger.warning(f"关闭数据库连接时出现异常: {e}")

    # ========== 缓存管理 ==========
    def _get_cached(self, key: str):
        """获取缓存数据"""
        if key in self._cache_ttl and time.time() < self._cache_ttl[key]:
            return self._cache.get(key)
        else:
            # 清理过期缓存
            if key in self._cache:
                del self._cache[key]
            if key in self._cache_ttl:
                del self._cache_ttl[key]
            return None

    def _set_cached(self, key: str, value: Any, ttl: int = 60):
        """设置缓存数据"""
        self._cache[key] = value
        self._cache_ttl[key] = time.time() + ttl

    async def cleanup_cache(self):
        """🆕 增强的缓存清理 - 过期清理 + LRU清理"""
        current_time = time.time()
        expired_keys = [
            key for key, expiry in self._cache_ttl.items() if current_time >= expiry
        ]

        for key in expired_keys:
            self._cache.pop(key, None)
            self._cache_ttl.pop(key, None)
            if key in self._cache_access_order:
                self._cache_access_order.remove(key)

        # 🆕 额外清理：如果缓存仍然过大，移除最旧的一些条目
        if len(self._cache) > self._cache_max_size * 0.8:  # 80%阈值
            excess = len(self._cache) - int(self._cache_max_size * 0.7)  # 清理到70%
            if excess > 0 and self._cache_access_order:
                keys_to_remove = self._cache_access_order[:excess]
                for key in keys_to_remove:
                    self._cache.pop(key, None)
                    self._cache_ttl.pop(key, None)
                self._cache_access_order = self._cache_access_order[excess:]
                logger.info(f"LRU强制清理: 移除了 {len(keys_to_remove)} 个旧缓存")

        if expired_keys:
            logger.debug(
                f"缓存清理完成: {len(expired_keys)}个过期, 当前大小: {len(self._cache)}"
            )

    async def force_refresh_activity_cache(self):
        """强制刷新活动配置缓存"""
        cache_keys_to_remove = ["activity_limits", "push_settings", "fine_rates"]
        for key in cache_keys_to_remove:
            self._cache.pop(key, None)
            self._cache_ttl.pop(key, None)
        await self.get_activity_limits()
        await self.get_fine_rates()
        logger.info("活动配置缓存已强制刷新")

    # ========== 群组相关操作 ==========
    async def init_group(self, chat_id: int):
        """初始化群组 - 带重试"""
        await self.execute_with_retry(
            "初始化群组",
            "INSERT INTO groups (chat_id) VALUES ($1) ON CONFLICT (chat_id) DO NOTHING",
            chat_id,
        )
        self._cache.pop(f"group:{chat_id}", None)

    async def get_group(self, chat_id: int) -> Optional[Dict]:
        """获取群组配置"""
        cache_key = f"group:{chat_id}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM groups WHERE chat_id = $1", chat_id
            )
            if row:
                result = dict(row)
                self._set_cached(cache_key, result, 300)
                return result
            return None

    async def get_group_cached(self, chat_id: int) -> Optional[Dict]:
        """带缓存的获取群组配置"""
        return await self.get_group(chat_id)

    async def update_group_channel(self, chat_id: int, channel_id: int):
        """更新群组频道ID"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE groups SET channel_id = $1, updated_at = CURRENT_TIMESTAMP WHERE chat_id = $2",
                channel_id,
                chat_id,
            )
            self._cache.pop(f"group:{chat_id}", None)

    async def update_group_notification(self, chat_id: int, group_id: int):
        """更新群组通知群组ID"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE groups SET notification_group_id = $1, updated_at = CURRENT_TIMESTAMP WHERE chat_id = $2",
                group_id,
                chat_id,
            )
            self._cache.pop(f"group:{chat_id}", None)

    async def update_group_reset_time(self, chat_id: int, hour: int, minute: int):
        """更新群组重置时间"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE groups SET 
                    reset_hour = $1, 
                    reset_minute = $2, 
                    updated_at = CURRENT_TIMESTAMP
                WHERE chat_id = $3
                """,
                hour,
                minute,
                chat_id,
            )

            # 🎯 关键：清除今天的重置记录，确保新时间能生效
            await self.clear_today_reset_record(chat_id)

            # 🧹 清理缓存
            self._cache.pop(f"group:{chat_id}", None)

            logger.info(f"重置时间更新: 群组{chat_id} -> {hour:02d}:{minute:02d}")

    async def update_group_work_time(
        self, chat_id: int, work_start: str, work_end: str
    ):
        """更新群组上下班时间"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE groups SET work_start_time = $1, work_end_time = $2, updated_at = CURRENT_TIMESTAMP WHERE chat_id = $3",
                work_start,
                work_end,
                chat_id,
            )
            self._cache.pop(f"group:{chat_id}", None)

    async def get_group_work_time(self, chat_id: int) -> Dict[str, str]:
        """获取群组上下班时间 - 带重试"""
        row = await self.fetchrow_with_retry(
            "获取工作时间",
            "SELECT work_start_time, work_end_time FROM groups WHERE chat_id = $1",
            chat_id,
        )
        if row and row["work_start_time"] and row["work_end_time"]:
            return {
                "work_start": row["work_start_time"],
                "work_end": row["work_end_time"],
            }
        return Config.DEFAULT_WORK_HOURS.copy()

    async def has_work_hours_enabled(self, chat_id: int) -> bool:
        """检查是否启用了上下班功能"""
        work_hours = await self.get_group_work_time(chat_id)
        return (
            work_hours["work_start"] != Config.DEFAULT_WORK_HOURS["work_start"]
            or work_hours["work_end"] != Config.DEFAULT_WORK_HOURS["work_end"]
        )

    # ========== 用户相关操作 ==========
    async def init_user(
        self,
        chat_id: int,
        user_id: int,
        nickname: str = None,
        target_datetime: datetime = None,
    ):
        """初始化用户 - 使用重置周期日期"""
        if target_datetime is None:
            target_datetime = self.get_beijing_time()

        # 🎯 使用重置周期日期
        period_date = await self.get_reset_period_date(chat_id, target_datetime)

        await self.execute_with_retry(
            "初始化用户",
            """
            INSERT INTO users (chat_id, user_id, nickname, last_updated) 
            VALUES ($1, $2, $3, $4) 
            ON CONFLICT (chat_id, user_id) 
            DO UPDATE SET 
                nickname = COALESCE($3, users.nickname),
                last_updated = $4,
                updated_at = CURRENT_TIMESTAMP
            """,
            chat_id,
            user_id,
            nickname,
            period_date,  # 🎯 使用重置周期日期
        )
        self._cache.pop(f"user:{chat_id}:{user_id}", None)

    async def update_user_last_updated(
        self, chat_id: int, user_id: int, update_date: date
    ):
        """更新用户最后更新时间"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET last_updated = $1 WHERE chat_id = $2 AND user_id = $3",
                update_date,
                chat_id,
                user_id,
            )

    async def get_user(self, chat_id: int, user_id: int) -> Optional[Dict]:
        cache_key = f"user:{chat_id}:{user_id}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        row = await self.execute_with_retry(
            "获取用户数据",
            """
            SELECT user_id, nickname, current_activity, activity_start_time, 
                total_accumulated_time, total_activity_count, total_fines,
                overtime_count, total_overtime_time, last_updated
            FROM users WHERE chat_id = $1 AND user_id = $2
            """,
            chat_id,
            user_id,
            fetchrow=True,  # 🎯 明确指定查询类型
            timeout=10,  # 🎯 用户查询设置较短超时
            slow_threshold=0.5,  # 🎯 用户查询要求更高性能
        )

        if row:
            result = dict(row)
            self._set_cached(cache_key, result, 30)
            return result
        return None

    async def get_user_cached(
        self, chat_id: int, user_id: int, target_datetime: datetime = None
    ) -> Optional[Dict]:
        """
        带缓存的获取用户数据 - 修复版：使用重置周期日期
        """

        if target_datetime is None:
            target_datetime = self.get_beijing_time()

        cache_key = f"user:{chat_id}:{user_id}"
        cached = self._get_cached(cache_key)

        # 🎯 获取当前重置周期日期（而不是自然日）
        current_period_date = await self.get_reset_period_date(chat_id, target_datetime)

        # --- 🎯 缓存命中但必须校验周期 ---
        if cached is not None:
            db_date = cached.get("last_updated")

            # 日期类型转换，确保可比较
            if hasattr(db_date, "date"):
                db_date = db_date.date()

            # 🎯 只有在同一重置周期内，缓存才有效
            if db_date == current_period_date:
                return cached
            else:
                # 缓存跨周期，立即清理
                self._cache.pop(cache_key, None)

        # --- 🎯 从数据库读取 ---
        row = await self.fetchrow_with_retry(
            "获取用户数据",
            """
            SELECT 
                user_id,
                nickname,
                current_activity,
                activity_start_time,
                total_accumulated_time,
                total_activity_count,
                total_fines,
                overtime_count,
                total_overtime_time,
                last_updated
            FROM users 
            WHERE chat_id = $1 AND user_id = $2
            """,
            chat_id,
            user_id,
        )

        if row:
            result = dict(row)
            db_date = result.get("last_updated")

            # 日期类型转换
            if hasattr(db_date, "date"):
                db_date = db_date.date()

            # 🎯 核心兜底：数据库日期不在当前重置周期
            if db_date != current_period_date:
                result.update(
                    {
                        "total_accumulated_time": 0,
                        "total_activity_count": 0,
                        "total_fines": 0,
                        "overtime_count": 0,
                        "total_overtime_time": 0,
                        "current_activity": None,
                        "activity_start_time": None,
                        # 仅修正内存数据，不写库
                        "last_updated": current_period_date,
                    }
                )

            self._set_cached(cache_key, result, 30)  # 30 秒缓存
            return result

        return None

    async def update_user_activity(
        self,
        chat_id: int,
        user_id: int,
        activity: str,
        start_time: str | datetime,
        nickname: str = None,
        target_datetime: datetime = None,
    ) -> int:
        """
        原子性更新用户活动状态并返回更新后的计数 - 加强版
        添加更多调试日志和验证
        """
        try:
            # 1️⃣ 统一时间入口
            if target_datetime is None:
                target_datetime = self.get_beijing_time()

            # 2️⃣ 获取业务周期日期
            period_date = await self.get_reset_period_date(chat_id, target_datetime)

            # 🎯 调试日志：记录周期信息
            logger.debug(
                f"🔍 update_user_activity: {chat_id}-{user_id}-{activity}, "
                f"周期日期: {period_date}, 目标时间: {target_datetime.strftime('%Y-%m-%d %H:%M:%S')}"
            )

            # 3️⃣ 标准化 start_time
            if hasattr(start_time, "isoformat"):
                if start_time.tzinfo is None:
                    start_time = beijing_tz.localize(start_time)
                start_time_str = start_time.isoformat()
            elif isinstance(start_time, str):
                start_time_str = self._normalize_time_string(
                    start_time, target_datetime
                )
            else:
                start_time_str = str(start_time)

            self._ensure_pool_initialized()
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # 🎯 调试：检查当前是否有记录
                    existing_record = await conn.fetchrow(
                        """
                        SELECT activity_count FROM user_activities 
                        WHERE chat_id = $1 AND user_id = $2 
                        AND activity_date = $3 AND activity_name = $4
                        """,
                        chat_id,
                        user_id,
                        period_date,
                        activity,
                    )

                    logger.debug(
                        f"📊 更新前检查: {chat_id}-{user_id}-{activity}, "
                        f"现有记录: {'有' if existing_record else '无'}, "
                        f"现有次数: {existing_record['activity_count'] if existing_record else 0}"
                    )

                    # 4️⃣ 原子性更新 user_activities 并返回最新计数
                    row = await conn.fetchrow(
                        """
                        INSERT INTO user_activities
                            (chat_id, user_id, activity_date, activity_name, activity_count, accumulated_time)
                        VALUES ($1, $2, $3, $4, 1, 0)
                        ON CONFLICT (chat_id, user_id, activity_date, activity_name)
                        DO UPDATE SET
                            activity_count = user_activities.activity_count + 1,
                            updated_at = CURRENT_TIMESTAMP
                        RETURNING activity_count
                        """,
                        chat_id,
                        user_id,
                        period_date,
                        activity,
                    )
                    updated_count = row["activity_count"] if row else 1

                    # 5️⃣ 更新 users 表（支持 nickname）
                    update_query = """
                        UPDATE users
                        SET current_activity = $1,
                            activity_start_time = $2,
                            last_updated = $3,
                            updated_at = CURRENT_TIMESTAMP
                    """
                    update_params = [
                        activity,
                        start_time_str,
                        period_date,
                        chat_id,
                        user_id,
                    ]

                    if nickname:
                        update_query = update_query.replace(
                            "updated_at = CURRENT_TIMESTAMP",
                            "nickname = $6, updated_at = CURRENT_TIMESTAMP",
                        )
                        update_params.append(nickname)

                    update_query += " WHERE chat_id = $4 AND user_id = $5"
                    await conn.execute(update_query, *update_params)

                    # 🎯 验证更新
                    verify_row = await conn.fetchrow(
                        "SELECT activity_count FROM user_activities WHERE chat_id = $1 AND user_id = $2 AND activity_date = $3 AND activity_name = $4",
                        chat_id,
                        user_id,
                        period_date,
                        activity,
                    )

                    if verify_row and verify_row["activity_count"] != updated_count:
                        logger.error(
                            f"⚠️ 验证失败: 期望{updated_count}, 实际{verify_row['activity_count']}"
                        )

            # 6️⃣ 缓存清理（事务外执行）
            cache_keys_to_remove = [
                f"user:{chat_id}:{user_id}",
                f"user_all_activities:{chat_id}:{user_id}",
                f"activity_count:{chat_id}:{user_id}:{activity}:{period_date}",
            ]

            # 清理所有活动计数相关的缓存
            pattern = f"activity_count:{chat_id}:{user_id}:"
            for cache_key in list(self._cache.keys()):
                if cache_key.startswith(pattern):
                    cache_keys_to_remove.append(cache_key)

            for key in set(cache_keys_to_remove):  # 去重
                self._cache.pop(key, None)
                self._cache_ttl.pop(key, None)

            logger.info(
                f"✅ 用户活动更新: {chat_id}-{user_id}, 活动: {activity}, "
                f"周期: {period_date}, 计数: {updated_count}"
            )

            return updated_count

        except Exception as e:
            logger.error(f"❌ 更新用户活动失败 {chat_id}-{user_id}: {e}")
            import traceback

            logger.error(f"堆栈跟踪: {traceback.format_exc()}")
            return 0

    # ===================== 辅助方法：时间标准化 =====================
    def _normalize_time_string(
        self, time_str: str, reference_time: datetime = None
    ) -> str:
        """标准化时间字符串为 ISO 格式"""
        try:

            clean_str = time_str.strip()

            # 处理时区
            if clean_str.endswith("Z"):
                clean_str = clean_str.replace("Z", "+00:00")

            # 尝试 ISO 格式
            try:
                dt = datetime.fromisoformat(clean_str)
            except ValueError:
                # 尝试常见格式
                formats = [
                    "%Y-%m-%d %H:%M:%S.%f",
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d %H:%M",
                    "%m/%d %H:%M:%S",
                    "%m/%d %H:%M",
                    "%H:%M:%S",
                    "%H:%M",
                ]

                for fmt in formats:
                    try:
                        dt = datetime.strptime(clean_str, fmt)
                        # 补充缺失的日期部分
                        if fmt.startswith("%H:%M"):
                            dt = dt.replace(
                                year=reference_time.year,
                                month=reference_time.month,
                                day=reference_time.day,
                            )
                        elif fmt.startswith("%m/%d"):
                            dt = dt.replace(year=reference_time.year)
                        break
                    except ValueError:
                        continue
                else:
                    raise ValueError(f"无法解析时间格式: {clean_str}")

            # 确保有时区
            if dt.tzinfo is None:
                dt = beijing_tz.localize(dt)

            return dt.isoformat()

        except Exception as e:
            logger.warning(f"⚠️ 时间标准化失败，返回原始值: {e}")
            return time_str

    async def complete_user_activity(
        self,
        chat_id: int,
        user_id: int,
        activity: str,
        elapsed_time: int,
        fine_amount: int = 0,
        is_overtime: bool = False,
    ):
        """完成用户活动 - 最终优化版（修复字段问题）"""
        # 1️⃣ 统一时间入口
        current_time = self.get_beijing_time()
        period_date = await self.get_reset_period_date(chat_id, current_time)  # 日周期
        statistic_date = current_time.date().replace(day=1)  # 月统计日期 🎯 新增

        # 保证 elapsed_time 非负
        elapsed_time = max(0, elapsed_time)
        overtime_seconds = 0

        if is_overtime:
            time_limit = await self.get_activity_time_limit(activity)
            time_limit_seconds = time_limit * 60
            overtime_seconds = max(0, elapsed_time - time_limit_seconds)

        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            try:
                async with conn.transaction():
                    # --- A. 确保 users 表存在记录（使用周期日期）---
                    await conn.execute(
                        """
                        INSERT INTO users (chat_id, user_id, last_updated)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (chat_id, user_id)
                        DO UPDATE SET last_updated = EXCLUDED.last_updated
                        """,
                        chat_id,
                        user_id,
                        period_date,
                    )

                    # --- B. user_activities 日流水累加 ---
                    await conn.execute(
                        """
                        INSERT INTO user_activities
                        (chat_id, user_id, activity_date, activity_name, activity_count, accumulated_time)
                        VALUES ($1, $2, $3, $4, 1, $5)
                        ON CONFLICT (chat_id, user_id, activity_date, activity_name)
                        DO UPDATE SET
                            activity_count = user_activities.activity_count + 1,
                            accumulated_time = user_activities.accumulated_time + EXCLUDED.accumulated_time,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        chat_id,
                        user_id,
                        period_date,
                        activity,
                        elapsed_time,
                    )

                    # --- C. monthly_statistics 月度累加（使用 statistic_date）---
                    await conn.execute(
                        """
                        INSERT INTO monthly_statistics
                        (chat_id, user_id, statistic_date, activity_name, activity_count, accumulated_time)
                        VALUES ($1, $2, $3, $4, 1, $5)
                        ON CONFLICT (chat_id, user_id, statistic_date, activity_name)
                        DO UPDATE SET
                            activity_count = monthly_statistics.activity_count + 1,
                            accumulated_time = monthly_statistics.accumulated_time + EXCLUDED.accumulated_time,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        chat_id,
                        user_id,
                        statistic_date,
                        activity,
                        elapsed_time,
                    )

                    # --- D. 超时统计（合并更新）---
                    if is_overtime and overtime_seconds > 0:
                        await conn.execute(
                            """
                            INSERT INTO monthly_statistics
                            (chat_id, user_id, statistic_date, activity_name, activity_count, accumulated_time)
                            VALUES
                            ($1, $2, $3, 'overtime_count', 1, 0),
                            ($1, $2, $3, 'overtime_time', 0, $4)
                            ON CONFLICT (chat_id, user_id, statistic_date, activity_name)
                            DO UPDATE SET
                                activity_count = monthly_statistics.activity_count + EXCLUDED.activity_count,
                                accumulated_time = monthly_statistics.accumulated_time + EXCLUDED.accumulated_time,
                                updated_at = CURRENT_TIMESTAMP
                            """,
                            chat_id,
                            user_id,
                            statistic_date,
                            overtime_seconds,
                        )

                    # --- E. 罚款统计 ---
                    if fine_amount > 0:
                        await conn.execute(
                            """
                            INSERT INTO monthly_statistics
                            (chat_id, user_id, statistic_date, activity_name, accumulated_time)
                            VALUES ($1, $2, $3, 'total_fines', $4)
                            ON CONFLICT (chat_id, user_id, statistic_date, activity_name)
                            DO UPDATE SET
                                accumulated_time = monthly_statistics.accumulated_time + EXCLUDED.accumulated_time,
                                updated_at = CURRENT_TIMESTAMP
                            """,
                            chat_id,
                            user_id,
                            statistic_date,
                            fine_amount,
                        )

                    # --- F. users 主表累计更新 ---
                    update_fields = [
                        "total_accumulated_time = total_accumulated_time + $1",
                        "total_activity_count = total_activity_count + 1",
                        "current_activity = NULL",
                        "activity_start_time = NULL",
                        "last_updated = $2",
                        "updated_at = CURRENT_TIMESTAMP",
                    ]
                    params = [elapsed_time, period_date]
                    idx = 3

                    if fine_amount > 0:
                        update_fields.append(f"total_fines = total_fines + ${idx}")
                        params.append(fine_amount)
                        idx += 1

                    if is_overtime:
                        update_fields.append("overtime_count = overtime_count + 1")
                        update_fields.append(
                            f"total_overtime_time = total_overtime_time + ${idx}"
                        )
                        params.append(overtime_seconds)
                        idx += 1

                    params.extend([chat_id, user_id])
                    query = f"UPDATE users SET {', '.join(update_fields)} WHERE chat_id = ${idx} AND user_id = ${idx+1}"
                    await conn.execute(query, *params)

            finally:
                # --- G. 缓存清理（必须执行）---
                cache_keys = [
                    f"user:{chat_id}:{user_id}",
                    f"user_all_activities:{chat_id}:{user_id}",
                ]
                for key in cache_keys:
                    self._cache.pop(key, None)

        logger.info(f"✅ 用户 {user_id} 活动完成: {activity} (时长: {elapsed_time}秒)")

    # ========= 重置前批量完成所有未结束活动 =========
    async def complete_all_pending_activities_before_reset(
        self, chat_id: int, reset_time: datetime
    ) -> Dict[str, Any]:
        """在重置前批量完成所有未结束活动 - 完整版本"""
        try:
            completed_count = 0
            total_fines = 0

            self._ensure_pool_initialized()
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # 🎯 批量获取所有未结束活动
                    active_users = await conn.fetch(
                        """
                        SELECT user_id, nickname, current_activity, activity_start_time 
                        FROM users 
                        WHERE chat_id = $1 AND current_activity IS NOT NULL
                    """,
                        chat_id,
                    )

                    if not active_users:
                        return {"completed_count": 0, "total_fines": 0, "details": []}

                    completion_details = []
                    statistic_date = reset_time.date().replace(day=1)

                    for user in active_users:
                        user_id = user["user_id"]
                        nickname = user["nickname"]
                        activity = user["current_activity"]
                        start_time_str = user["activity_start_time"]

                        try:
                            # 解析开始时间
                            start_time = datetime.fromisoformat(start_time_str)

                            # 计算活动时长（到重置时间为止）
                            elapsed = int((reset_time - start_time).total_seconds())

                            # 计算超时和罚款
                            time_limit = await self.get_activity_time_limit(activity)
                            time_limit_seconds = time_limit * 60
                            is_overtime = elapsed > time_limit_seconds
                            overtime_seconds = max(0, elapsed - time_limit_seconds)
                            overtime_minutes = overtime_seconds / 60

                            fine_amount = 0
                            if is_overtime and overtime_seconds > 0:
                                fine_amount = await self.calculate_fine_for_activity(
                                    activity, overtime_minutes
                                )

                            # 🎯 更新月度统计表
                            await self._update_monthly_statistics_for_activity(
                                conn,
                                chat_id,
                                user_id,
                                statistic_date,
                                activity,
                                elapsed,
                                fine_amount,
                                is_overtime,
                                overtime_seconds,
                            )

                            completed_count += 1
                            total_fines += fine_amount

                            completion_details.append(
                                {
                                    "user_id": user_id,
                                    "nickname": nickname,
                                    "activity": activity,
                                    "elapsed_time": elapsed,
                                    "fine_amount": fine_amount,
                                    "is_overtime": is_overtime,
                                }
                            )

                            logger.info(
                                f"重置前结束活动: {chat_id}-{user_id} - {activity} (时长: {elapsed}秒, 罚款: {fine_amount}元)"
                            )

                        except Exception as e:
                            logger.error(f"结束用户活动失败 {chat_id}-{user_id}: {e}")
                            # 记录错误但继续处理其他用户

                    # 🎯 批量清空活动状态
                    await conn.execute(
                        """
                        UPDATE users 
                        SET current_activity = NULL, activity_start_time = NULL 
                        WHERE chat_id = $1 AND current_activity IS NOT NULL
                    """,
                        chat_id,
                    )

                    return {
                        "completed_count": completed_count,
                        "total_fines": total_fines,
                        "details": completion_details,
                    }

        except Exception as e:
            logger.error(f"批量结束活动失败 {chat_id}: {e}")
            return {"completed_count": 0, "total_fines": 0, "details": []}

    async def _update_monthly_statistics_for_activity(
        self,
        conn,
        chat_id: int,
        user_id: int,
        statistic_date: date,
        activity: str,
        elapsed: int,
        fine_amount: int,
        is_overtime: bool,
        overtime_seconds: int,
    ):
        """更新月度统计的辅助方法"""
        # 更新活动统计
        await conn.execute(
            """
            INSERT INTO monthly_statistics 
            (chat_id, user_id, statistic_date, activity_name, activity_count, accumulated_time)
            VALUES ($1, $2, $3, $4, 1, $5)
            ON CONFLICT (chat_id, user_id, statistic_date, activity_name) 
            DO UPDATE SET 
                activity_count = monthly_statistics.activity_count + 1,
                accumulated_time = monthly_statistics.accumulated_time + EXCLUDED.accumulated_time,
                updated_at = CURRENT_TIMESTAMP
        """,
            chat_id,
            user_id,
            statistic_date,
            activity,
            elapsed,
        )

        # 更新罚款统计
        if fine_amount > 0:
            await conn.execute(
                """
                INSERT INTO monthly_statistics 
                (chat_id, user_id, statistic_date, activity_name, accumulated_time)
                VALUES ($1, $2, $3, 'total_fines', $4)
                ON CONFLICT (chat_id, user_id, statistic_date, activity_name) 
                DO UPDATE SET 
                    accumulated_time = monthly_statistics.accumulated_time + EXCLUDED.accumulated_time,
                    updated_at = CURRENT_TIMESTAMP
            """,
                chat_id,
                user_id,
                statistic_date,
                fine_amount,
            )

        # 更新超时统计
        if is_overtime:
            await conn.execute(
                """
                INSERT INTO monthly_statistics 
                (chat_id, user_id, statistic_date, activity_name, activity_count)
                VALUES ($1, $2, $3, 'overtime_count', 1)
                ON CONFLICT (chat_id, user_id, statistic_date, activity_name) 
                DO UPDATE SET 
                    activity_count = monthly_statistics.activity_count + 1,
                    updated_at = CURRENT_TIMESTAMP
            """,
                chat_id,
                user_id,
                statistic_date,
            )

            await conn.execute(
                """
                INSERT INTO monthly_statistics 
                (chat_id, user_id, statistic_date, activity_name, accumulated_time)
                VALUES ($1, $2, $3, 'overtime_time', $4)
                ON CONFLICT (chat_id, user_id, statistic_date, activity_name) 
                DO UPDATE SET 
                    accumulated_time = monthly_statistics.accumulated_time + EXCLUDED.accumulated_time,
                    updated_at = CURRENT_TIMESTAMP
            """,
                chat_id,
                user_id,
                statistic_date,
                overtime_seconds,
            )

    async def reset_user_daily_data(
        self,
        chat_id: int,
        user_id: int,
        target_datetime: Optional[datetime] = None,
    ) -> bool:
        """彻底重置用户每日数据：使用重置周期日期 + 数据安全"""

        try:
            # 🎯 1. 统一时间入口
            if target_datetime is None:
                target_datetime = self.get_beijing_time()

            # 🎯 2. 获取业务重置周期日期
            period_date = await self.get_reset_period_date(chat_id, target_datetime)

            # 🎯 调试：记录重置开始
            logger.info(
                f"🔄 开始重置用户数据: {chat_id}-{user_id}, "
                f"周期日期: {period_date}, "
                f"目标时间: {target_datetime.strftime('%Y-%m-%d %H:%M:%S')}"
            )

            # 🎯 3. 确保用户存在且 last_updated 对齐周期
            await self.init_user(chat_id, user_id, None, target_datetime)

            self._ensure_pool_initialized()

            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # 🎯 调试：重置前检查数据
                    pre_reset_check = await conn.fetchrow(
                        """
                        SELECT 
                            (SELECT COUNT(*) FROM user_activities 
                             WHERE chat_id = $1 AND user_id = $2 AND activity_date = $3) as activity_count,
                            (SELECT COUNT(*) FROM work_records 
                             WHERE chat_id = $1 AND user_id = $2 AND record_date = $3) as work_record_count,
                            (SELECT total_activity_count FROM users 
                             WHERE chat_id = $1 AND user_id = $2) as user_total_count
                        """,
                        chat_id,
                        user_id,
                        period_date,
                    )

                    logger.debug(
                        f"📊 重置前数据状态: "
                        f"活动记录数={pre_reset_check['activity_count']}, "
                        f"工作记录数={pre_reset_check['work_record_count']}, "
                        f"用户总次数={pre_reset_check['user_total_count']}"
                    )

                    # --- 🧹 A: 删除当期活动流水 ---
                    delete_result = await conn.execute(
                        """
                        DELETE FROM user_activities
                        WHERE chat_id = $1
                          AND user_id = $2
                          AND activity_date = $3
                        """,
                        chat_id,
                        user_id,
                        period_date,
                    )
                    logger.debug(f"删除活动记录结果: {delete_result}")

                    # --- 🧹 B: 删除当期上下班记录 ---
                    delete_work_result = await conn.execute(
                        """
                        DELETE FROM work_records
                        WHERE chat_id = $1
                          AND user_id = $2
                          AND record_date = $3
                        """,
                        chat_id,
                        user_id,
                        period_date,
                    )
                    logger.debug(f"删除工作记录结果: {delete_work_result}")

                    # --- 🧮 C: 主表彻底归零 ---
                    update_result = await conn.execute(
                        """
                        UPDATE users SET
                            total_activity_count = 0,
                            total_accumulated_time = 0,
                            total_fines = 0,
                            total_overtime_time = 0,
                            overtime_count = 0,
                            current_activity = NULL,
                            activity_start_time = NULL,
                            last_updated = $3,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE chat_id = $1
                          AND user_id = $2
                        """,
                        chat_id,
                        user_id,
                        period_date,
                    )
                    logger.debug(f"更新用户表结果: {update_result}")

                    # 🎯 验证重置效果
                    post_reset_check = await conn.fetchrow(
                        """
                        SELECT 
                            (SELECT COUNT(*) FROM user_activities 
                             WHERE chat_id = $1 AND user_id = $2 AND activity_date = $3) as activity_count,
                            (SELECT total_activity_count FROM users 
                             WHERE chat_id = $1 AND user_id = $2) as user_total_count
                        """,
                        chat_id,
                        user_id,
                        period_date,
                    )

                    if (
                        post_reset_check["activity_count"] > 0
                        or post_reset_check["user_total_count"] > 0
                    ):
                        logger.warning(
                            f"⚠️ 重置后数据非零: "
                            f"活动记录数={post_reset_check['activity_count']}, "
                            f"用户总次数={post_reset_check['user_total_count']}"
                        )

                # 🎯 提交事务后，确保用户数据被正确更新
                await conn.execute(
                    """
                    UPDATE users SET last_updated = $3 
                    WHERE chat_id = $1 AND user_id = $2
                    """,
                    chat_id,
                    user_id,
                    period_date,
                )

            # --- 🧽 D: 缓存清理 ---
            cache_keys_to_remove = set()

            # 1. 固定要清理的键
            fixed_keys = [
                f"user:{chat_id}:{user_id}",
                f"user_all_activities:{chat_id}:{user_id}",
                f"group:{chat_id}",
            ]
            cache_keys_to_remove.update(fixed_keys)

            # 2. 动态匹配要清理的键
            all_cache_keys = list(self._cache.keys())
            for cache_key in all_cache_keys:
                if f"activity_count:{chat_id}:{user_id}:" in cache_key:
                    cache_keys_to_remove.add(cache_key)
                if f"activity_limit:" in cache_key:
                    cache_keys_to_remove.add(cache_key)

            # 3. 执行清理
            cleaned_count = 0
            for key in cache_keys_to_remove:
                if key in self._cache:
                    self._cache.pop(key, None)
                    cleaned_count += 1
                if key in self._cache_ttl:
                    self._cache_ttl.pop(key, None)

            logger.info(f"🧹 缓存清理: 清理了 {cleaned_count} 个缓存项")

            # 🎯 重置后立即验证
            try:
                async with self.pool.acquire() as conn:
                    verify_row = await conn.fetchrow(
                        """
                        SELECT activity_count FROM user_activities 
                        WHERE chat_id = $1 AND user_id = $2 
                        AND activity_date = $3 AND activity_name = $4
                        """,
                        chat_id,
                        user_id,
                        period_date,
                        "小厕",  # 示例活动，可按实际调整
                    )
                    verify_count = verify_row["activity_count"] if verify_row else 0

                    if verify_count == 0:
                        logger.info(
                            f"✅ 重置验证通过: {chat_id}-{user_id}, 活动计数={verify_count}"
                        )
                    else:
                        logger.warning(
                            f"⚠️ 重置验证异常: {chat_id}-{user_id}, 活动计数={verify_count} (应该是0)"
                        )
            except Exception as verify_error:
                logger.error(f"❌ 重置验证失败: {verify_error}")

            logger.info(f"✅ 数据重置完成: {chat_id}-{user_id}, 周期={period_date}")
            return True

        except Exception as e:
            logger.error(f"❌ 重置失败 {chat_id}-{user_id}: {e}")
            import traceback

            logger.error(f"详细错误: {traceback.format_exc()}")
            return False

    async def get_user_activity_count(
        self,
        chat_id: int,
        user_id: int,
        activity: str,
        target_datetime: datetime = None,
        cache_ttl: int = 3,  # 短期缓存秒数，可调整
    ) -> int:

        try:
            # 1️⃣ 统一时间入口
            if target_datetime is None:
                target_datetime = self.get_beijing_time()

            # 2️⃣ 获取业务周期日期
            period_date = await self.get_reset_period_date(chat_id, target_datetime)

            # 3️⃣ 构造缓存 key（与 update_user_activity 保持一致）
            cache_key = f"activity_count:{chat_id}:{user_id}:{activity}:{period_date}"

            # 🎯 调试日志：记录关键信息
            logger.debug(
                f"🔍 get_user_activity_count: {cache_key}, "
                f"周期: {period_date}, 时间: {target_datetime.strftime('%Y-%m-%d %H:%M:%S')}"
            )

            current_time = time.time()

            # 4️⃣ 检查缓存有效性
            if cache_key in self._cache_ttl:
                if current_time < self._cache_ttl[cache_key]:
                    cached_value = self._cache.get(cache_key)
                    if cached_value is not None:
                        logger.debug(f"✅ 缓存命中: {cache_key} = {cached_value}")
                        return cached_value
                else:
                    # TTL 过期，清理缓存
                    logger.debug(f"🔄 缓存过期: {cache_key}")
                    self._cache.pop(cache_key, None)
                    self._cache_ttl.pop(cache_key, None)

            # 5️⃣ 查询数据库
            self._ensure_pool_initialized()
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT activity_count 
                    FROM user_activities 
                    WHERE chat_id = $1 
                      AND user_id = $2 
                      AND activity_date = $3 
                      AND activity_name = $4
                    """,
                    chat_id,
                    user_id,
                    period_date,
                    activity,
                )
                count = row["activity_count"] if row else 0

            # 6️⃣ 设置短期缓存
            self._cache[cache_key] = count
            self._cache_ttl[cache_key] = current_time + cache_ttl

            logger.debug(f"📊 数据库查询: {cache_key} = {count}")
            return count

        except Exception as e:
            logger.error(f"❌ 获取用户活动次数失败 {chat_id}-{user_id}-{activity}: {e}")
            import traceback

            logger.error(f"堆栈跟踪: {traceback.format_exc()}")
            return 0

    async def get_user_all_activities(
        self, chat_id: int, user_id: int, target_datetime: datetime = None
    ) -> Dict[str, Dict]:
        """获取用户当前重置周期内的所有活动数据"""
        try:
            # 🎯 使用重置周期日期，而不是自然日
            period_date = await self.get_reset_period_date(chat_id, target_datetime)

            self._ensure_pool_initialized()
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT activity_name, activity_count, accumulated_time 
                    FROM user_activities 
                    WHERE chat_id = $1 AND user_id = $2 AND activity_date = $3
                    """,
                    chat_id,
                    user_id,
                    period_date,  # 🎯 关键：使用重置周期日期
                )

                activities = {}
                for row in rows:
                    activities[row["activity_name"]] = {
                        "count": row["activity_count"],
                        "time": row["accumulated_time"],
                    }
                return activities

        except Exception as e:
            logger.error(f"获取用户所有活动数据失败 {chat_id}-{user_id}: {e}")
            return {}

    # ========== 上下班记录操作 ==========
    async def add_work_record(
        self,
        chat_id: int,
        user_id: int,
        record_date,
        checkin_type: str,
        checkin_time: str,
        status: str,
        time_diff_minutes: float,
        fine_amount: int = 0,
    ):
        """添加上下班记录 - 修复版"""
        if isinstance(record_date, str):
            record_date = datetime.strptime(record_date, "%Y-%m-%d").date()
        elif isinstance(record_date, datetime):
            record_date = record_date.date()

        statistic_date = record_date.replace(day=1)

        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # 添加上下班记录
                await conn.execute(
                    """
                    INSERT INTO work_records 
                    (chat_id, user_id, record_date, checkin_type, checkin_time, status, time_diff_minutes, fine_amount)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (chat_id, user_id, record_date, checkin_type) 
                    DO UPDATE SET 
                        checkin_time = EXCLUDED.checkin_time,
                        status = EXCLUDED.status,
                        time_diff_minutes = EXCLUDED.time_diff_minutes,
                        fine_amount = EXCLUDED.fine_amount,
                        created_at = CURRENT_TIMESTAMP
                    """,
                    chat_id,
                    user_id,
                    record_date,
                    checkin_type,
                    checkin_time,
                    status,
                    time_diff_minutes,
                    fine_amount,
                )

                # 🆕 修复：完整的工作天数和工作时长统计
                if checkin_type == "work_end":
                    # 获取对应的上班记录
                    work_start_record = await conn.fetchrow(
                        "SELECT checkin_time FROM work_records WHERE chat_id = $1 AND user_id = $2 AND record_date = $3 AND checkin_type = 'work_start'",
                        chat_id,
                        user_id,
                        record_date,
                    )

                    if work_start_record:
                        try:
                            # 计算工作时长
                            start_time_str = work_start_record["checkin_time"]
                            end_time_str = checkin_time

                            # 解析时间（格式为 HH:MM）
                            start_time = datetime.strptime(start_time_str, "%H:%M")
                            end_time = datetime.strptime(end_time_str, "%H:%M")

                            # 计算工作时长（分钟）
                            work_duration_minutes = (
                                end_time - start_time
                            ).total_seconds() / 60

                            # 处理跨天情况（如果下班时间小于上班时间，说明跨天了）
                            if work_duration_minutes < 0:
                                work_duration_minutes += 24 * 60  # 加上24小时

                            # 转换为秒
                            work_duration_seconds = int(work_duration_minutes * 60)

                            # 🆕 更新工作天数到月度统计表
                            await conn.execute(
                                """
                                INSERT INTO monthly_statistics 
                                (chat_id, user_id, statistic_date, activity_name, activity_count, accumulated_time)
                                VALUES ($1, $2, $3, 'work_days', 1, 0)
                                ON CONFLICT (chat_id, user_id, statistic_date, activity_name) 
                                DO UPDATE SET 
                                    activity_count = monthly_statistics.activity_count + 1,
                                    updated_at = CURRENT_TIMESTAMP
                                """,
                                chat_id,
                                user_id,
                                statistic_date,
                            )

                            # 🆕 更新工作时长到月度统计表
                            await conn.execute(
                                """
                                INSERT INTO monthly_statistics 
                                (chat_id, user_id, statistic_date, activity_name, accumulated_time, activity_count)
                                VALUES ($1, $2, $3, 'work_hours', $4, 0)
                                ON CONFLICT (chat_id, user_id, statistic_date, activity_name) 
                                DO UPDATE SET 
                                    accumulated_time = monthly_statistics.accumulated_time + EXCLUDED.accumulated_time,
                                    updated_at = CURRENT_TIMESTAMP
                                """,
                                chat_id,
                                user_id,
                                statistic_date,
                                work_duration_seconds,
                            )

                            logger.info(
                                f"工作统计更新: 用户{user_id} 工作时长{work_duration_minutes:.1f}分钟"
                            )

                        except Exception as e:
                            logger.error(f"计算工作时长失败: {e}")
                            # 即使计算失败，也记录工作天数（但不记录时长）
                            await conn.execute(
                                """
                                INSERT INTO monthly_statistics 
                                (chat_id, user_id, statistic_date, activity_name, activity_count, accumulated_time)
                                VALUES ($1, $2, $3, 'work_days', 1, 0)
                                ON CONFLICT (chat_id, user_id, statistic_date, activity_name) 
                                DO UPDATE SET 
                                    activity_count = monthly_statistics.activity_count + 1,
                                    updated_at = CURRENT_TIMESTAMP
                                """,
                                chat_id,
                                user_id,
                                statistic_date,
                            )

                # 更新罚款统计
                if fine_amount > 0:
                    await conn.execute(
                        "UPDATE users SET total_fines = total_fines + $1 WHERE chat_id = $2 AND user_id = $3",
                        fine_amount,
                        chat_id,
                        user_id,
                    )

            self._cache.pop(f"user:{chat_id}:{user_id}", None)

    # ========== 活动配置操作 ==========
    async def get_activity_limits(self) -> Dict:
        """获取所有活动限制 - 优化版"""
        # 使用更长的缓存时间，因为这些数据不常变化
        cache_key = "activity_limits"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        # 检查数据库连接状态
        if not await self._ensure_healthy_connection():
            logger.warning("数据库连接不健康，返回默认活动配置")
            return Config.DEFAULT_ACTIVITY_LIMITS.copy()

        try:
            # 使用更快的查询，只获取需要的字段
            rows = await self.fetch_with_retry(
                "获取活动限制",
                "SELECT activity_name, max_times, time_limit FROM activity_configs",
            )
            limits = {
                row["activity_name"]: {
                    "max_times": row["max_times"],
                    "time_limit": row["time_limit"],
                }
                for row in rows
            }
            # 设置较长缓存时间，因为活动配置不常变化
            self._set_cached(cache_key, limits, 600)  # 10分钟缓存
            return limits
        except Exception as e:
            logger.error(f"获取活动配置失败: {e}，返回默认配置")
            return Config.DEFAULT_ACTIVITY_LIMITS.copy()

    async def get_activity_limits_cached(self) -> Dict:
        """带缓存的获取活动限制"""
        try:
            return await self.get_activity_limits()
        except Exception as e:
            logger.error(f"获取活动配置缓存失败: {e}，返回默认配置")
            return Config.DEFAULT_ACTIVITY_LIMITS.copy()

    async def get_activity_time_limit(self, activity: str) -> int:
        """获取活动时间限制"""
        limits = await self.get_activity_limits()
        return limits.get(activity, {}).get("time_limit", 0)

    async def get_activity_max_times(self, activity: str) -> int:
        """获取活动最大次数"""
        limits = await self.get_activity_limits()
        return limits.get(activity, {}).get("max_times", 0)

    async def activity_exists(self, activity: str) -> bool:
        """检查活动是否存在"""
        cache_key = "activity_limits"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return activity in cached

        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT 1 FROM activity_configs WHERE activity_name = $1", activity
            )
            return row is not None

    async def update_activity_config(
        self, activity: str, max_times: int, time_limit: int
    ):
        """更新活动配置"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO activity_configs (activity_name, max_times, time_limit)
                VALUES ($1, $2, $3)
                ON CONFLICT (activity_name) 
                DO UPDATE SET 
                    max_times = EXCLUDED.max_times,
                    time_limit = EXCLUDED.time_limit,
                    created_at = CURRENT_TIMESTAMP
                """,
                activity,
                max_times,
                time_limit,
            )
        self._cache.pop("activity_limits", None)

    async def delete_activity_config(self, activity: str):
        """删除活动配置"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM activity_configs WHERE activity_name = $1", activity
            )
            await conn.execute(
                "DELETE FROM fine_configs WHERE activity_name = $1", activity
            )
        self._cache.pop("activity_limits", None)

    # ========== 罚款配置操作 ==========
    async def get_fine_rates(self) -> Dict:
        """获取所有罚款费率"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM fine_configs")
            fines = {}
            for row in rows:
                activity = row["activity_name"]
                if activity not in fines:
                    fines[activity] = {}
                fines[activity][row["time_segment"]] = row["fine_amount"]
            return fines

    async def get_fine_rates_for_activity(self, activity: str) -> Dict:
        """获取指定活动的罚款费率"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT time_segment, fine_amount FROM fine_configs WHERE activity_name = $1",
                activity,
            )
            return {row["time_segment"]: row["fine_amount"] for row in rows}

    async def update_fine_config(
        self, activity: str, time_segment: str, fine_amount: int
    ):
        """更新罚款配置"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO fine_configs (activity_name, time_segment, fine_amount)
                VALUES ($1, $2, $3)
                ON CONFLICT (activity_name, time_segment) 
                DO UPDATE SET 
                    fine_amount = EXCLUDED.fine_amount,
                    created_at = CURRENT_TIMESTAMP
                """,
                activity,
                time_segment,
                fine_amount,
            )

    async def calculate_fine_for_activity(
        self, activity: str, overtime_minutes: float
    ) -> int:
        """计算活动罚款金额 - 数据库内部版本"""
        fine_rates = await self.get_fine_rates_for_activity(activity)
        if not fine_rates:
            return 0

        # 处理罚款时间段
        segments = []
        for time_key in fine_rates.keys():
            try:
                if isinstance(time_key, str) and "min" in time_key.lower():
                    time_value = int(time_key.lower().replace("min", "").strip())
                else:
                    time_value = int(time_key)
                segments.append(time_value)
            except (ValueError, TypeError):
                continue

        if not segments:
            return 0

        segments.sort()

        applicable_fine = 0
        for segment in segments:
            if overtime_minutes <= segment:
                original_key = str(segment)
                if original_key not in fine_rates:
                    original_key = f"{segment}min"
                applicable_fine = fine_rates.get(original_key, 0)
                break

        if applicable_fine == 0 and segments:
            max_segment = segments[-1]
            original_key = str(max_segment)
            if original_key not in fine_rates:
                original_key = f"{max_segment}min"
            applicable_fine = fine_rates.get(original_key, 0)

        return applicable_fine

    async def get_work_fine_rates(self) -> Dict:
        """获取上下班罚款费率"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM work_fine_configs")
            fines = {}
            for row in rows:
                checkin_type = row["checkin_type"]
                if checkin_type not in fines:
                    fines[checkin_type] = {}
                fines[checkin_type][row["time_segment"]] = row["fine_amount"]
            return fines

    async def get_work_fine_rates_for_type(self, checkin_type: str) -> Dict:
        """获取指定类型的上下班罚款费率"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT time_segment, fine_amount FROM work_fine_configs WHERE checkin_type = $1",
                checkin_type,
            )
            return {row["time_segment"]: row["fine_amount"] for row in rows}

    async def update_work_fine_rate(
        self, checkin_type: str, time_segment: str, fine_amount: int
    ):
        """更新上下班罚款费率"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO work_fine_configs (checkin_type, time_segment, fine_amount)
                VALUES ($1, $2, $3)
                ON CONFLICT (checkin_type, time_segment)
                DO UPDATE SET fine_amount = EXCLUDED.fine_amount
                """,
                checkin_type,
                time_segment,
                fine_amount,
            )

    async def clear_work_fine_rates(self, checkin_type: str):
        """清空上下班罚款配置"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM work_fine_configs WHERE checkin_type = $1", checkin_type
            )

    # ========== 推送设置操作 ==========
    async def get_push_settings(self) -> Dict:
        """获取推送设置"""
        cache_key = "push_settings"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM push_settings")
            settings = {row["setting_key"]: bool(row["setting_value"]) for row in rows}
            self._set_cached(cache_key, settings, 300)
            return settings

    async def update_push_setting(self, key: str, value: bool):
        """更新推送设置"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO push_settings (setting_key, setting_value)
                VALUES ($1, $2)
                ON CONFLICT (setting_key) 
                DO UPDATE SET 
                    setting_value = EXCLUDED.setting_value,
                    created_at = CURRENT_TIMESTAMP
                """,
                key,
                1 if value else 0,
            )
        self._cache.pop("push_settings", None)

    # ========== 统计和导出相关 ==========
    async def get_group_statistics(
        self, chat_id: int, target_date: Optional[date] = None
    ) -> List[Dict]:
        """获取群组统计信息"""
        if target_date is None:
            target_date = self.get_beijing_date()

        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            users = await conn.fetch(
                """
                SELECT DISTINCT u.user_id, u.nickname, 
                    COALESCE(ua_total.total_accumulated_time, 0) as total_accumulated_time,
                    COALESCE(ua_total.total_activity_count, 0) as total_activity_count,
                    COALESCE(u.total_fines, 0) as total_fines,
                    COALESCE(u.overtime_count, 0) as overtime_count,
                    COALESCE(u.total_overtime_time, 0) as total_overtime_time
                FROM users u
                LEFT JOIN (
                    SELECT user_id, 
                        SUM(accumulated_time) as total_accumulated_time,
                        SUM(activity_count) as total_activity_count
                    FROM user_activities 
                    WHERE chat_id = $1 AND activity_date = $2
                    GROUP BY user_id
                ) ua_total ON u.user_id = ua_total.user_id
                WHERE u.chat_id = $1 
                AND EXISTS (
                    SELECT 1 FROM user_activities 
                    WHERE chat_id = $1 AND user_id = u.user_id AND activity_date = $2
                )
                """,
                chat_id,
                target_date,
            )

            result = []
            for user in users:
                user_data = dict(user)

                # 获取活动详情
                activities = await conn.fetch(
                    """
                    SELECT activity_name, activity_count, accumulated_time
                    FROM user_activities
                    WHERE chat_id = $1 AND user_id = $2 AND activity_date = $3
                    """,
                    chat_id,
                    user["user_id"],
                    target_date,
                )

                user_data["activities"] = {}
                for row in activities:
                    user_data["activities"][row["activity_name"]] = {
                        "count": row["activity_count"],
                        "time": row["accumulated_time"],
                    }

                result.append(user_data)

            return result

    async def get_all_groups(self) -> List[int]:
        """获取所有群组ID"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT chat_id FROM groups")
            return [row["chat_id"] for row in rows]

    async def get_group_members(
        self, chat_id: int, target_datetime: datetime = None
    ) -> List[Dict]:
        """获取群组成员 - 根据重置周期"""
        if target_datetime is None:
            target_datetime = self.get_beijing_time()

        # 🎯 获取当前重置周期日期
        period_date = await self.get_reset_period_date(chat_id, target_datetime)

        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT user_id, nickname, current_activity, activity_start_time, 
                    total_accumulated_time, total_activity_count, total_fines, 
                    overtime_count, total_overtime_time, last_updated 
                FROM users 
                WHERE chat_id = $1 AND last_updated = $2
                """,
                chat_id,
                period_date,  # 🎯 使用重置周期日期
            )
            return [dict(row) for row in rows]

    # ========== 月度统计 ==========

    async def get_monthly_statistics(
        self, chat_id: int, year: int = None, month: int = None
    ) -> List[Dict]:
        """修复版：获取月度统计 - 采用分类预聚合，彻底解决重复计算与性能问题"""

        if year is None or month is None:
            # 统一使用 datetime 以确保时区正确
            today = self.get_beijing_time()
            year = today.year
            month = today.month

        # 该月 1 号作为统计周期
        statistic_date = date(year, month, 1)

        self._ensure_pool_initialized()

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                WITH grouped_stats AS (
                    SELECT 
                        user_id,
                        activity_name,
                        SUM(activity_count) AS sum_count,
                        SUM(accumulated_time) AS sum_time
                    FROM monthly_statistics
                    WHERE chat_id = $1 AND statistic_date = $2
                    GROUP BY user_id, activity_name
                ),
                user_metrics AS (
                    SELECT 
                        user_id,
                        MAX(CASE WHEN activity_name = 'work_days' THEN sum_count ELSE 0 END) AS work_days,
                        MAX(CASE WHEN activity_name = 'work_hours' THEN sum_time ELSE 0 END) AS work_hours,
                        MAX(CASE WHEN activity_name = 'total_fines' THEN sum_time ELSE 0 END) AS total_fines,
                        MAX(CASE WHEN activity_name = 'overtime_count' THEN sum_count ELSE 0 END) AS overtime_count,
                        MAX(CASE WHEN activity_name = 'overtime_time' THEN sum_time ELSE 0 END) AS total_overtime_time,

                        SUM(CASE 
                            WHEN activity_name NOT IN (
                                'work_days','work_hours','total_fines',
                                'overtime_count','overtime_time'
                            ) THEN sum_count ELSE 0 END
                        ) AS total_activity_count,

                        SUM(CASE 
                            WHEN activity_name NOT IN (
                                'work_days','work_hours','total_fines',
                                'overtime_count','overtime_time'
                            ) THEN sum_time ELSE 0 END
                        ) AS total_accumulated_time
                    FROM grouped_stats
                    GROUP BY user_id
                ),
                activity_json AS (
                    SELECT
                        user_id,
                        jsonb_object_agg(
                            activity_name,
                            jsonb_build_object('count', sum_count, 'time', sum_time)
                        ) AS activities
                    FROM grouped_stats
                    WHERE activity_name NOT IN (
                        'work_days','work_hours','total_fines',
                        'overtime_count','overtime_time'
                    )
                    GROUP BY user_id
                )
                SELECT 
                    um.*,
                    u.nickname,
                    COALESCE(aj.activities, '{}'::jsonb) AS activities
                FROM user_metrics um
                LEFT JOIN users u 
                    ON u.chat_id = $1 AND u.user_id = um.user_id
                LEFT JOIN activity_json aj 
                    ON um.user_id = aj.user_id
                ORDER BY um.total_accumulated_time DESC
                """,
                chat_id,
                statistic_date,
            )

        result: List[Dict] = []
        for row in rows:
            data = dict(row)

            # 确保 activities 永远是 dict
            if isinstance(data.get("activities"), str):
                import json

                data["activities"] = json.loads(data["activities"])
            elif data.get("activities") is None:
                data["activities"] = {}

            result.append(data)

        return result

    async def get_monthly_work_statistics(
        self, chat_id: int, year: int = None, month: int = None
    ) -> List[Dict]:
        """获取月度上下班统计"""
        if year is None or month is None:
            today = self.get_beijing_time()
            year = today.year
            month = today.month

        start_date = date(year, month, 1)
        if month == 12:
            end_date = date(year + 1, 1, 1)
        else:
            end_date = date(year, month + 1, 1)

        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT 
                    wr.user_id,
                    u.nickname,
                    COUNT(CASE WHEN wr.checkin_type = 'work_start' THEN 1 END) as work_start_count,
                    COUNT(CASE WHEN wr.checkin_type = 'work_end' THEN 1 END) as work_end_count,
                    SUM(CASE WHEN wr.checkin_type = 'work_start' THEN wr.fine_amount ELSE 0 END) as work_start_fines,
                    SUM(CASE WHEN wr.checkin_type = 'work_end' THEN wr.fine_amount ELSE 0 END) as work_end_fines
                FROM work_records wr
                JOIN users u ON wr.chat_id = u.chat_id AND wr.user_id = u.user_id
                WHERE wr.chat_id = $1 AND wr.record_date >= $2 AND wr.record_date < $3
                GROUP BY wr.user_id, u.nickname
                """,
                chat_id,
                start_date,
                end_date,
            )
            return [dict(row) for row in rows]

    async def get_monthly_activity_ranking(
        self, chat_id: int, year: int = None, month: int = None
    ) -> Dict[str, List]:
        """获取月度活动排行榜"""
        if year is None or month is None:
            today = self.get_beijing_time()
            year = today.year
            month = today.month

        statistic_date = date(year, month, 1)
        activity_limits = await self.get_activity_limits()

        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            rankings = {}
            for activity in activity_limits.keys():
                rows = await conn.fetch(
                    """
                    SELECT 
                        ms.user_id,
                        u.nickname,
                        ms.accumulated_time as total_time,
                        ms.activity_count as total_count
                    FROM monthly_statistics ms
                    JOIN users u ON ms.chat_id = u.chat_id AND ms.user_id = u.user_id
                    WHERE ms.chat_id = $1 AND ms.activity_name = $2 
                        AND ms.statistic_date = $3
                    ORDER BY ms.accumulated_time DESC
                    LIMIT 10
                    """,
                    chat_id,
                    activity,
                    statistic_date,
                )
                rankings[activity] = [dict(row) for row in rows]
            return rankings

    async def get_user_late_early_counts(
        self, chat_id: int, user_id: int, year: int, month: int
    ) -> Dict[str, int]:
        """获取用户的迟到早退次数统计"""
        start_date = date(year, month, 1)
        if month == 12:
            end_date = date(year + 1, 1, 1)
        else:
            end_date = date(year, month + 1, 1)

        async with self.pool.acquire() as conn:
            # 获取迟到次数（上班时间差>0）
            late_count = (
                await conn.fetchval(
                    """
                SELECT COUNT(*) FROM work_records 
                WHERE chat_id = $1 AND user_id = $2 
                AND record_date >= $3 AND record_date < $4
                AND checkin_type = 'work_start' AND time_diff_minutes > 0
                """,
                    chat_id,
                    user_id,
                    start_date,
                    end_date,
                )
                or 0
            )

            # 获取早退次数（下班时间差<0）
            early_count = (
                await conn.fetchval(
                    """
                SELECT COUNT(*) FROM work_records 
                WHERE chat_id = $1 AND user_id = $2 
                AND record_date >= $3 AND record_date < $4
                AND checkin_type = 'work_end' AND time_diff_minutes < 0
                """,
                    chat_id,
                    user_id,
                    start_date,
                    end_date,
                )
                or 0
            )

            return {"late_count": late_count, "early_count": early_count}

    # ========== 数据清理 ==========
    async def cleanup_old_data(self, days: int = 30):
        """清理旧数据"""
        cutoff_date = (self.get_beijing_time() - timedelta(days=days)).date()

        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "DELETE FROM user_activities WHERE activity_date < $1", cutoff_date
                )
                await conn.execute(
                    "DELETE FROM work_records WHERE record_date < $1", cutoff_date
                )
                await conn.execute(
                    "DELETE FROM users WHERE last_updated < $1", cutoff_date
                )

    async def cleanup_monthly_data(self, target_date: date = None):
        """清理月度统计数据"""
        if target_date is None:
            today = self.get_beijing_time()
            # 使用配置而不是硬编码90天
            monthly_cutoff = (
                (today - timedelta(days=Config.MONTHLY_DATA_RETENTION_DAYS))
                .date()
                .replace(day=1)
            )
            target_date = monthly_cutoff

        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM monthly_statistics WHERE statistic_date < $1", target_date
            )
            return (
                int(result.split()[-1]) if result and result.startswith("DELETE") else 0
            )

    async def cleanup_specific_month(self, year: int, month: int):
        """清理指定年月的月度统计数据"""
        target_date = date(year, month, 1)
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM monthly_statistics WHERE statistic_date = $1", target_date
            )
            return (
                int(result.split()[-1]) if result and result.startswith("DELETE") else 0
            )

    async def cleanup_inactive_users(self, days: int = 30):
        """清理长期未活动用户及其记录（安全版）"""

        cutoff_date = (self.get_beijing_time() - timedelta(days=days)).date()

        async with self.pool.acquire() as conn:
            async with conn.transaction():

                # 找出要删除的用户列表（避免直接删）
                users_to_delete = await conn.fetch(
                    """
                        SELECT user_id 
                        FROM users
                        WHERE last_updated < $1
                        AND NOT EXISTS (
                            SELECT 1 FROM monthly_statistics 
                            WHERE monthly_statistics.chat_id = users.chat_id 
                            AND monthly_statistics.user_id = users.user_id
                        )
                        """,
                    cutoff_date,
                )

                user_ids = [u["user_id"] for u in users_to_delete]

                if not user_ids:
                    logger.info("🧹 无需清理用户")
                    return 0

                # 删除用户的日常记录
                await conn.execute(
                    "DELETE FROM user_activities WHERE user_id = ANY($1)",
                    user_ids,
                )

                # 删除上下班记录（如果你需要）
                await conn.execute(
                    "DELETE FROM work_records WHERE user_id = ANY($1)",
                    user_ids,
                )

                # 最后删除用户
                deleted_count = await conn.execute(
                    "DELETE FROM users WHERE user_id = ANY($1)",
                    user_ids,
                )

        logger.info(f"🧹 清理了 {deleted_count} 个长期未活动的用户以及他们的所有记录")
        return deleted_count

    # ========== 活动人数限制 ==========
    async def set_activity_user_limit(self, activity: str, max_users: int):
        """设置活动人数限制"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO activity_user_limits (activity_name, max_users)
                VALUES ($1, $2)
                ON CONFLICT (activity_name)
                DO UPDATE SET 
                    max_users = EXCLUDED.max_users,
                    updated_at = CURRENT_TIMESTAMP
                """,
                activity,
                max_users,
            )
        self._cache.pop(f"activity_limit:{activity}", None)

    async def get_activity_user_limit(self, activity: str) -> int:
        """获取活动人数限制"""
        cache_key = f"activity_limit:{activity}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT max_users FROM activity_user_limits WHERE activity_name = $1",
                activity,
            )
            limit = row["max_users"] if row else 0
            self._set_cached(cache_key, limit, 60)
            return limit

    async def get_current_activity_users(self, chat_id: int, activity: str) -> int:
        """获取当前正在进行指定活动的用户数量"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM users WHERE chat_id = $1 AND current_activity = $2",
                chat_id,
                activity,
            )
            return count or 0

    async def get_all_activity_limits(self) -> Dict[str, int]:
        """获取所有活动的人数限制"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT activity_name, max_users FROM activity_user_limits"
            )
            return {row["activity_name"]: row["max_users"] for row in rows}

    async def remove_activity_user_limit(self, activity: str):
        """移除活动人数限制"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM activity_user_limits WHERE activity_name = $1", activity
            )
        self._cache.pop(f"activity_limit:{activity}", None)

    # ========== 工具方法 ==========
    @staticmethod
    def format_time_for_csv(seconds: int) -> str:
        """为CSV导出格式化时间显示"""
        if not seconds:
            return "0分0秒"

        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60

        if hours > 0:
            return f"{hours}时{minutes}分{secs}秒"
        else:
            return f"{minutes}分{secs}秒"

    async def connection_health_check(self) -> bool:
        """快速连接健康检查"""
        if not self.pool:
            return False

        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
                return result == 1
        except Exception as e:
            logger.debug(f"数据库连接健康检查失败: {e}")
            return False

    # ========= 验证数据完整性 =========
    async def validate_system_integrity(self, chat_id: int):
        """验证系统数据完整性"""
        now = self.get_beijing_time()

        # 1️⃣ 验证重置周期计算
        period_date = await self.get_reset_period_date(chat_id, now)
        logger.info(f"验证群组 {chat_id} 重置周期: {period_date}")

        # 2️⃣ 验证用户数据一致性
        members = await self.get_group_members(chat_id, now)
        inconsistencies = []

        for member in members:
            uid = member["user_id"]

            # 检查 users 表和 user_activities 表的数据是否一致
            user_data = await self.get_user_cached(chat_id, uid, now)

            if not user_data:
                logger.warning(f"用户 {uid} 数据不存在")
                continue

            # 🎯 使用相同的重置周期日期查询活动数据
            activities = {}
            try:
                async with self.pool.acquire() as conn:
                    rows = await conn.fetch(
                        """
                        SELECT activity_name, activity_count, accumulated_time 
                        FROM user_activities 
                        WHERE chat_id = $1 AND user_id = $2 AND activity_date = $3
                        """,
                        chat_id,
                        uid,
                        period_date,
                    )
                    for row in rows:
                        activities[row["activity_name"]] = {
                            "count": row["activity_count"],
                            "time": row["accumulated_time"],
                        }
            except Exception as e:
                logger.error(f"查询用户 {uid} 活动数据失败: {e}")
                continue

            # 计算总次数
            total_from_activities = sum(
                info.get("count", 0) for info in activities.values()
            )

            user_total = user_data.get("total_activity_count", 0)

            if user_total != total_from_activities:
                inconsistency = {
                    "user_id": uid,
                    "nickname": user_data.get("nickname", "未知"),
                    "users_table_count": user_total,
                    "activities_table_count": total_from_activities,
                    "difference": abs(user_total - total_from_activities),
                }
                inconsistencies.append(inconsistency)
                logger.warning(
                    f"数据不一致: 用户{uid}({inconsistency['nickname']}), "
                    f"users表={user_total}, activities表={total_from_activities}"
                )

        # 3️⃣ 验证月度统计与日常统计的一致性
        monthly_inconsistencies = []
        if inconsistencies:
            # 检查月度统计是否包含这些数据
            today = now.date()
            statistic_date = today.replace(day=1)

            for inc in inconsistencies:
                uid = inc["user_id"]
                # 检查月度统计是否有该用户的数据
                async with self.pool.acquire() as conn:
                    monthly_exists = await conn.fetchval(
                        """
                        SELECT 1 FROM monthly_statistics 
                        WHERE chat_id = $1 AND user_id = $2 AND statistic_date = $3
                        LIMIT 1
                        """,
                        chat_id,
                        uid,
                        statistic_date,
                    )
                    if not monthly_exists:
                        monthly_inconsistencies.append(uid)
                        logger.warning(f"用户 {uid} 月度统计缺失")

        return {
            "period_date": period_date,
            "total_members": len(members),
            "inconsistent_users": len(inconsistencies),
            "inconsistencies": inconsistencies,
            "monthly_missing_users": len(monthly_inconsistencies),
            "monthly_missing_user_ids": monthly_inconsistencies,
            "timestamp": now.isoformat(),
        }


# 全局数据库实例
db = PostgreSQLDatabase()
