# database.py - 纯 PostgreSQL 版本（最终完整版）
import logging
import asyncio
import time
from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional
from config import Config
import asyncpg
from asyncpg.pool import Pool
from datetime import date, datetime

logger = logging.getLogger("GroupCheckInBot")


class PostgreSQLDatabase:
    """纯 PostgreSQL 数据库管理器"""

    def __init__(self, database_url: str = None):
        self.database_url = database_url or Config.DATABASE_URL
        self.pool: Optional[Pool] = None
        self._initialized = False
        self._cache = {}
        self._cache_ttl = {}

    # ========== 初始化方法 ==========
    async def initialize(self):
        """带重试的数据库初始化"""
        if self._initialized:
            return

        max_retries = 5
        for attempt in range(max_retries):
            try:
                logger.info(
                    f"🔗 尝试连接 PostgreSQL 数据库 (尝试 {attempt + 1}/{max_retries})"
                )
                await self._initialize_impl()
                logger.info("✅ PostgreSQL 数据库初始化完成")
                self._initialized = True
                return
            except Exception as e:
                logger.warning(f"⚠️ 数据库初始化第 {attempt + 1} 次失败: {e}")
                if attempt == max_retries - 1:
                    logger.error(f"❌ 数据库初始化重试{max_retries}次后失败: {e}")
                    raise
                retry_delay = 2**attempt
                logger.info(f"⏳ {retry_delay}秒后重试数据库初始化...")
                await asyncio.sleep(retry_delay)

    async def _initialize_impl(self):
        """实际的数据库初始化实现"""
        try:
            # 创建连接池
            self.pool = await asyncpg.create_pool(
                self.database_url,
                min_size=Config.DB_MIN_CONNECTIONS,
                max_size=Config.DB_MAX_CONNECTIONS,
                max_inactive_connection_lifetime=Config.DB_POOL_RECYCLE,
                command_timeout=Config.DB_CONNECTION_TIMEOUT,
                statement_cache_size=0,
            )
            logger.info("✅ PostgreSQL 连接池创建成功")

            # 测试连接并获取数据库信息
            async with self.pool.acquire() as conn:
                db_version = await conn.fetchval("SELECT version()")
                db_name = await conn.fetchval("SELECT current_database()")
                active_connections = await conn.fetchval(
                    "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()"
                )

                logger.info("📊 数据库连接信息:")
                logger.info(f"   - 数据库: {db_name}")
                logger.info(f"   - 版本: {str(db_version).split(',')[0]}")
                logger.info(f"   - 当前连接数: {active_connections}")

            # 创建表和索引
            await self._create_tables()
            await self._create_indexes()
            await self._initialize_default_data()

        except Exception as e:
            logger.error(f"❌ PostgreSQL 连接失败: {e}")
            if "connection" in str(e).lower() or "authentication" in str(e).lower():
                logger.error("💡 请检查 DATABASE_URL 环境变量是否正确配置")
                logger.error("💡 请检查数据库服务是否正常运行")
                logger.error("💡 请检查网络连接和防火墙设置")
            raise

    async def _create_tables(self):
        """创建所有必要的表"""
        async with self.pool.acquire() as conn:
            tables = [
                """
                CREATE TABLE IF NOT EXISTS groups (
                    chat_id BIGINT PRIMARY KEY,
                    channel_id BIGINT,
                    notification_group_id BIGINT,
                    reset_hour INTEGER DEFAULT 0,
                    reset_minute INTEGER DEFAULT 0,
                    work_start_time TEXT DEFAULT '09:00',
                    work_end_time TEXT DEFAULT '18:00',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT,
                    user_id BIGINT,
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
                """
                CREATE TABLE IF NOT EXISTS activity_configs (
                    activity_name TEXT PRIMARY KEY,
                    max_times INTEGER,
                    time_limit INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,
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
                """
                CREATE TABLE IF NOT EXISTS push_settings (
                    setting_key TEXT PRIMARY KEY,
                    setting_value INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,
            ]

            for table_sql in tables:
                await conn.execute(table_sql)

            logger.info("✅ 数据库表创建完成")

    async def _create_indexes(self):
        """创建性能索引"""
        async with self.pool.acquire() as conn:
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_user_activities_main ON user_activities (chat_id, user_id, activity_date)",
                "CREATE INDEX IF NOT EXISTS idx_user_activities_activity ON user_activities (activity_name)",
                "CREATE INDEX IF NOT EXISTS idx_work_records_main ON work_records (chat_id, user_id, record_date)",
                "CREATE INDEX IF NOT EXISTS idx_users_main ON users (chat_id, user_id)",
                "CREATE INDEX IF NOT EXISTS idx_users_updated ON users (last_updated)",
                "CREATE INDEX IF NOT EXISTS idx_user_activities_date ON user_activities (activity_date)",
                "CREATE INDEX IF NOT EXISTS idx_work_records_date ON work_records (record_date)",
            ]

            for index_sql in indexes:
                try:
                    await conn.execute(index_sql)
                except Exception as e:
                    logger.warning(f"创建索引失败: {e}")

            logger.info("✅ 数据库索引创建完成")

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

            # 初始化罚款配置
            for activity, fines in Config.DEFAULT_FINE_RATES.items():
                for time_segment, amount in fines.items():
                    await conn.execute(
                        "INSERT INTO fine_configs (activity_name, time_segment, fine_amount) VALUES ($1, $2, $3) ON CONFLICT (activity_name, time_segment) DO NOTHING",
                        activity,
                        time_segment,
                        amount,
                    )

            # 初始化上下班罚款配置
            for checkin_type, fines in Config.DEFAULT_WORK_FINE_RATES.items():
                for time_segment, amount in fines.items():
                    await conn.execute(
                        "INSERT INTO work_fine_configs (checkin_type, time_segment, fine_amount) VALUES ($1, $2, $3) ON CONFLICT (checkin_type, time_segment) DO NOTHING",
                        checkin_type,
                        time_segment,
                        amount,
                    )

            # 初始化推送设置
            for key, value in Config.AUTO_EXPORT_SETTINGS.items():
                await conn.execute(
                    "INSERT INTO push_settings (setting_key, setting_value) VALUES ($1, $2) ON CONFLICT (setting_key) DO NOTHING",
                    key,
                    1 if value else 0,
                )

            logger.info("✅ 默认数据初始化完成")

    # ========== 数据库连接管理 ==========
    async def get_connection(self):
        """获取数据库连接"""
        if not self.pool:
            raise RuntimeError("数据库连接池尚未初始化")
        return await self.pool.acquire()

    async def release_connection(self, conn):
        """释放数据库连接"""
        await self.pool.release(conn)

    async def close(self):
        """安全关闭数据库连接池"""
        try:
            if self.pool:
                await self.pool.close()
                logger.info("✅ PostgreSQL 连接池已安全关闭")
        except Exception as e:
            logger.warning(f"⚠️ 关闭数据库连接时出现异常: {e}")

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
        """清理缓存"""
        current_time = time.time()
        expired_keys = [
            key for key, expiry in self._cache_ttl.items() if current_time >= expiry
        ]
        for key in expired_keys:
            self._cache.pop(key, None)
            self._cache_ttl.pop(key, None)

        if expired_keys:
            logger.debug(f"清理了 {len(expired_keys)} 个过期缓存")

    # 🆕 新增：强制刷新活动配置缓存
    async def force_refresh_activity_cache(self):
        """强制刷新活动配置缓存"""
        # 清理活动相关的所有缓存
        cache_keys_to_remove = ["activity_limits", "push_settings", "fine_rates"]

        for key in cache_keys_to_remove:
            self._cache.pop(key, None)
            self._cache_ttl.pop(key, None)

        # 重新加载活动配置
        await self.get_activity_limits()
        await self.get_fine_rates()

        logger.info("🔄 活动配置缓存已强制刷新")

        # ========== 群组相关操作 ==========

    async def init_group(self, chat_id: int):
        """初始化群组"""
        async with self.pool.acquire() as conn:
            await conn.execute(
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

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM groups WHERE chat_id = $1", chat_id
            )
            if row:
                result = dict(row)
                self._set_cached(cache_key, result, 300)
                return result
            return None

    async def update_group_channel(self, chat_id: int, channel_id: int):
        """更新群组频道ID"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE groups SET channel_id = $1, updated_at = CURRENT_TIMESTAMP WHERE chat_id = $2",
                channel_id,
                chat_id,
            )
            self._cache.pop(f"group:{chat_id}", None)

    async def update_group_notification(self, chat_id: int, group_id: int):
        """更新群组通知群组ID"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE groups SET notification_group_id = $1, updated_at = CURRENT_TIMESTAMP WHERE chat_id = $2",
                group_id,
                chat_id,
            )
            self._cache.pop(f"group:{chat_id}", None)

    async def update_group_reset_time(self, chat_id: int, hour: int, minute: int):
        """更新群组重置时间"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE groups SET reset_hour = $1, reset_minute = $2, updated_at = CURRENT_TIMESTAMP WHERE chat_id = $3",
                hour,
                minute,
                chat_id,
            )
            self._cache.pop(f"group:{chat_id}", None)

    async def update_group_work_time(
        self, chat_id: int, work_start: str, work_end: str
    ):
        """更新群组上下班时间"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE groups SET work_start_time = $1, work_end_time = $2, updated_at = CURRENT_TIMESTAMP WHERE chat_id = $3",
                work_start,
                work_end,
                chat_id,
            )
            self._cache.pop(f"group:{chat_id}", None)

    async def get_group_work_time(self, chat_id: int) -> Dict[str, str]:
        """获取群组上下班时间"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
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
    async def init_user(self, chat_id: int, user_id: int, nickname: str = None):
        """初始化用户"""
        today = datetime.now().date()
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO users (chat_id, user_id, nickname, last_updated) VALUES ($1, $2, $3, $4) ON CONFLICT (chat_id, user_id) DO NOTHING",
                chat_id,
                user_id,
                nickname,
                today,
            )
            self._cache.pop(f"user:{chat_id}:{user_id}", None)

    async def get_user(self, chat_id: int, user_id: int) -> Optional[Dict]:
        """获取用户数据"""
        cache_key = f"user:{chat_id}:{user_id}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM users WHERE chat_id = $1 AND user_id = $2",
                chat_id,
                user_id,
            )
            if row:
                result = dict(row)
                self._set_cached(cache_key, result, 30)
                return result
            return None

    async def get_user_cached(self, chat_id: int, user_id: int) -> Optional[Dict]:
        """带缓存的获取用户数据"""
        return await self.get_user(chat_id, user_id)

    async def get_group_cached(self, chat_id: int) -> Optional[Dict]:
        """带缓存的获取群组配置"""
        return await self.get_group(chat_id)

    async def update_user_activity(
        self,
        chat_id: int,
        user_id: int,
        activity: str,
        start_time: str,
        nickname: str = None,
    ):
        """更新用户活动状态"""
        async with self.pool.acquire() as conn:
            if nickname:
                await conn.execute(
                    "UPDATE users SET current_activity = $1, activity_start_time = $2, nickname = $3, updated_at = CURRENT_TIMESTAMP WHERE chat_id = $4 AND user_id = $5",
                    activity,
                    start_time,
                    nickname,
                    chat_id,
                    user_id,
                )
            else:
                await conn.execute(
                    "UPDATE users SET current_activity = $1, activity_start_time = $2, updated_at = CURRENT_TIMESTAMP WHERE chat_id = $3 AND user_id = $4",
                    activity,
                    start_time,
                    chat_id,
                    user_id,
                )
            self._cache.pop(f"user:{chat_id}:{user_id}", None)

    async def complete_user_activity(
        self,
        chat_id: int,
        user_id: int,
        activity: str,
        elapsed_time: int,
        fine_amount: int = 0,
        is_overtime: bool = False,
    ):
        """完成用户活动 - 修复计数问题版本"""
        today = datetime.now().date()

        logger.info(
            f"🔍 [数据库操作开始] 用户{user_id} 活动{activity} 时长{elapsed_time}s"
        )

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # 确保用户记录存在并更新日期
                await conn.execute(
                    """
                    INSERT INTO users (chat_id, user_id, last_updated) 
                    VALUES ($1, $2, $3)
                    ON CONFLICT (chat_id, user_id) 
                    DO UPDATE SET last_updated = EXCLUDED.last_updated
                    """,
                    chat_id,
                    user_id,
                    today,
                )

                # 使用 ON CONFLICT 原子更新活动计数
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
                    today,
                    activity,
                    elapsed_time,
                )

                # 更新用户总体统计
                update_fields = [
                    "total_accumulated_time = total_accumulated_time + $1",
                    "total_activity_count = total_activity_count + 1",
                    "current_activity = NULL",
                    "activity_start_time = NULL",
                    "last_updated = $2",
                ]
                params = [elapsed_time, today]

                if fine_amount > 0:
                    update_fields.append("total_fines = total_fines + $3")
                    params.append(fine_amount)

                if is_overtime:
                    update_fields.append("overtime_count = overtime_count + 1")
                    time_limit = await self.get_activity_time_limit(activity)
                    overtime_seconds = max(0, elapsed_time - (time_limit * 60))
                    update_fields.append(
                        "total_overtime_time = total_overtime_time + $4"
                    )
                    params.append(overtime_seconds)

                update_fields.append("updated_at = CURRENT_TIMESTAMP")
                params.extend([chat_id, user_id])

                placeholders = ", ".join(update_fields)
                query = f"UPDATE users SET {placeholders} WHERE chat_id = ${len(params)-1} AND user_id = ${len(params)}"
                await conn.execute(query, *params)

            self._cache.pop(f"user:{chat_id}:{user_id}", None)

        logger.info(f"🔍 [数据库操作完成] 用户{user_id} 活动{activity} 完成更新")

    async def reset_user_daily_data(
        self, chat_id: int, user_id: int, target_date: date | None = None
    ):
        """
        ✅ 修复版：重置用户每日数据但保留历史记录
        只重置累计统计和当前状态，不删除历史记录
        """
        try:
            # 验证和设置目标日期
            if target_date is None:
                target_date = datetime.now().date()
            elif not isinstance(target_date, date):
                raise ValueError(
                    f"target_date必须是date类型，得到: {type(target_date)}"
                )

            # 获取重置前的用户状态（用于日志）
            user_before = await self.get_user(chat_id, user_id)

            # 🆕 计算新的日期（重置后的日期）
            new_date = target_date
            # 如果是重置昨天的数据，那么新的日期应该是今天
            if target_date < datetime.now().date():
                new_date = datetime.now().date()

            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # 🆕 关键修改：不再删除历史记录！
                    # ❌ 删除这2个DELETE操作：
                    # - 不要删除 user_activities 记录（保留导出所需的历史数据）
                    # - 不要删除 work_records 记录（保留上下班打卡历史）

                    # 3. 只重置用户统计数据和状态
                    await conn.execute(
                        """
                        UPDATE users SET
                            total_activity_count = 0,
                            total_accumulated_time = 0,
                            total_overtime_time = 0,
                            overtime_count = 0,
                            total_fines = 0,
                            current_activity = NULL,
                            activity_start_time = NULL,
                            last_updated = $3,  
                            updated_at = CURRENT_TIMESTAMP
                        WHERE chat_id = $1 AND user_id = $2
                        """,
                        chat_id,
                        user_id,
                        new_date,  # 🆕 使用新的日期
                    )

            # 4. 清理相关缓存
            cache_keys = [
                f"user:{chat_id}:{user_id}",
                f"group:{chat_id}",
                "activity_limits",
            ]
            for key in cache_keys:
                self._cache.pop(key, None)
                self._cache_ttl.pop(key, None)

            # 记录详细的重置日志
            logger.info(
                f"✅ 数据重置完成（保留历史记录）: 用户 {user_id} (群组 {chat_id})\n"
                f"   📅 重置日期: {target_date} → {new_date}\n"
                f"   💾 历史记录: 已保留（支持后续导出）\n"
                f"   📊 重置前状态:\n"
                f"       - 活动次数: {user_before.get('total_activity_count', 0) if user_before else 0}\n"
                f"       - 累计时长: {user_before.get('total_accumulated_time', 0) if user_before else 0}秒\n"
                f"       - 罚款金额: {user_before.get('total_fines', 0) if user_before else 0}元\n"
                f"       - 超时次数: {user_before.get('overtime_count', 0) if user_before else 0}\n"
                f"       - 当前活动: {user_before.get('current_activity', '无') if user_before else '无'}"
            )

            return True

        except Exception as e:
            logger.error(f"❌ 重置用户数据失败 {chat_id}-{user_id}: {e}")
            return False

    async def update_user_last_updated(
        self, chat_id: int, user_id: int, date_obj: date
    ):
        """
        更新用户最后更新时间
        """
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE users 
                    SET last_updated = $1, updated_at = CURRENT_TIMESTAMP 
                    WHERE chat_id = $2 AND user_id = $3
                    """,
                    date_obj,
                    chat_id,
                    user_id,
                )

            # 清理用户缓存
            self._cache.pop(f"user:{chat_id}:{user_id}", None)
            logger.debug(f"✅ 更新最后更新时间: {chat_id}-{user_id} -> {date_obj}")

        except Exception as e:
            logger.error(f"❌ 更新最后更新时间失败 {chat_id}-{user_id}: {e}")

    async def get_user_activity_count(
        self, chat_id: int, user_id: int, activity: str
    ) -> int:
        """获取用户今日活动次数"""
        today = datetime.now().date()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT activity_count FROM user_activities WHERE chat_id = $1 AND user_id = $2 AND activity_date = $3 AND activity_name = $4",
                chat_id,
                user_id,
                today,
                activity,
            )
            count = row["activity_count"] if row else 0
            logger.debug(f"📊 获取活动计数: 用户{user_id} 活动{activity} 计数{count}")
            return count

    async def get_user_activity_time(
        self, chat_id: int, user_id: int, activity: str
    ) -> int:
        """获取用户今日活动累计时间"""
        today = datetime.now().date()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT accumulated_time FROM user_activities WHERE chat_id = $1 AND user_id = $2 AND activity_date = $3 AND activity_name = $4",
                chat_id,
                user_id,
                today,
                activity,
            )
            return row["accumulated_time"] if row else 0

    async def get_user_all_activities(
        self, chat_id: int, user_id: int
    ) -> Dict[str, Dict]:
        """获取用户所有活动数据"""
        today = datetime.now().date()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT activity_name, activity_count, accumulated_time FROM user_activities WHERE chat_id = $1 AND user_id = $2 AND activity_date = $3",
                chat_id,
                user_id,
                today,
            )

            activities = {}
            for row in rows:
                activities[row["activity_name"]] = {
                    "count": row["activity_count"],
                    "time": row["accumulated_time"],
                    "time_formatted": self.format_seconds_to_hms(
                        row["accumulated_time"]
                    ),
                }
            return activities

    # ========== 上下班记录操作 ==========
    async def add_work_record(
        self,
        chat_id: int,
        user_id: int,
        record_date,  # 移除类型注解，让Python自动处理
        checkin_type: str,
        checkin_time: str,
        status: str,
        time_diff_minutes: float,
        fine_amount: int = 0,
    ):
        """添加上下班记录"""
        if isinstance(record_date, str):
            record_date = datetime.strptime(record_date, "%Y-%m-%d").date()
        elif isinstance(record_date, datetime):
            record_date = record_date.date()

        async with self.pool.acquire() as conn:
            async with conn.transaction():
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

                # 更新用户罚款总额
                if fine_amount > 0:
                    await conn.execute(
                        "UPDATE users SET total_fines = total_fines + $1 WHERE chat_id = $2 AND user_id = $3",
                        fine_amount,
                        chat_id,
                        user_id,
                    )

            self._cache.pop(f"user:{chat_id}:{user_id}", None)

    async def get_user_work_records(
        self, chat_id: int, user_id: int, limit: int = 7
    ) -> List[Dict]:
        """获取用户上下班记录"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM work_records WHERE chat_id = $1 AND user_id = $2 ORDER BY record_date DESC, checkin_type LIMIT $3",
                chat_id,
                user_id,
                limit * 2,
            )

            result = []
            for row in rows:
                record = dict(row)
                if record["time_diff_minutes"]:
                    record["time_diff_formatted"] = self.format_minutes_to_hm(
                        record["time_diff_minutes"]
                    )
                else:
                    record["time_diff_formatted"] = "0小时0分钟"
                result.append(record)

            return result

    async def has_work_record_today(
        self, chat_id: int, user_id: int, checkin_type: str
    ) -> bool:
        """检查今天是否有指定类型的上下班记录"""
        today = datetime.now().date()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT 1 FROM work_records WHERE chat_id = $1 AND user_id = $2 AND record_date = $3 AND checkin_type = $4",
                chat_id,
                user_id,
                today,
                checkin_type,
            )
            return row is not None

    async def get_today_work_records(
        self, chat_id: int, user_id: int
    ) -> Dict[str, Dict]:
        """获取用户今天的上下班记录"""
        today = datetime.now().date()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM work_records WHERE chat_id = $1 AND user_id = $2 AND record_date = $3",
                chat_id,
                user_id,
                today,
            )

            records = {}
            for row in rows:
                record = dict(row)
                if record["time_diff_minutes"]:
                    record["time_diff_formatted"] = self.format_minutes_to_hm(
                        record["time_diff_minutes"]
                    )
                else:
                    record["time_diff_formatted"] = "0小时0分钟"
                records[row["checkin_type"]] = record
            return records

    # ========== 活动配置操作 ==========
    async def get_activity_limits(self) -> Dict:
        """获取所有活动限制"""
        cache_key = "activity_limits"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM activity_configs")

            limits = {
                row["activity_name"]: {
                    "max_times": row["max_times"],
                    "time_limit": row["time_limit"],
                }
                for row in rows
            }
            self._set_cached(cache_key, limits, 300)
            return limits

    async def get_activity_limits_cached(self) -> Dict:
        """带缓存的获取活动限制"""
        return await self.get_activity_limits()

    async def get_activity_time_limit(self, activity: str) -> int:
        """获取活动时间限制"""
        limits = await self.get_activity_limits()
        return limits.get(activity, {}).get("time_limit", 0)

    async def get_activity_max_times(self, activity: str) -> int:
        """获取活动最大次数"""
        limits = await self.get_activity_limits()
        return limits.get(activity, {}).get("max_times", 0)

    async def activity_exists(self, activity: str) -> bool:
        """检查活动是否存在 - 修复版本"""
        # 先检查缓存
        cache_key = "activity_limits"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return activity in cached

        # 如果缓存不存在，直接从数据库查询
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT 1 FROM activity_configs WHERE activity_name = $1", activity
            )
            return row is not None

    async def update_activity_config(
        self, activity: str, max_times: int, time_limit: int
    ):
        """更新活动配置 - 修复新增活动无法打卡问题"""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # 更新或新增活动配置
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

                # ✅ 初始化默认罚款配置，避免新增活动无法打卡
                default_fines = getattr(Config, "DEFAULT_FINE_RATES", {}).get(
                    "default", {}
                )
                if not default_fines:
                    default_fines = {"30min": 5, "60min": 10, "120min": 20}

                # 批量插入罚款配置
                values = [(activity, ts, amt) for ts, amt in default_fines.items()]
                await conn.executemany(
                    """
                    INSERT INTO fine_configs (activity_name, time_segment, fine_amount)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (activity_name, time_segment) DO NOTHING
                    """,
                    values,
                )

            # 清理缓存
            self._cache.pop("activity_limits", None)
            logger.info(f"✅ 活动配置更新完成: {activity}，并初始化罚款配置")

    async def delete_activity_config(self, activity: str):
        """删除活动配置"""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "DELETE FROM activity_configs WHERE activity_name = $1", activity
                )
                await conn.execute(
                    "DELETE FROM fine_configs WHERE activity_name = $1", activity
                )
        self._cache.pop("activity_limits", None)
        logger.info(f"🗑 已删除活动配置及罚款: {activity}")

    # ========== 罚款配置操作 ==========
    async def get_fine_rates(self) -> Dict:
        """获取所有罚款费率"""
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

    async def get_work_fine_rates(self) -> Dict:
        """获取上下班罚款费率"""
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
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT time_segment, fine_amount FROM work_fine_configs WHERE checkin_type = $1",
                checkin_type,
            )
            return {row["time_segment"]: row["fine_amount"] for row in rows}

    async def update_work_fine_rate(
        self, checkin_type: str, time_segment: str, fine_amount: int
    ):
        """插入或更新上下班罚款规则"""
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
            logger.info(
                f"✅ 已更新罚款配置: 类型={checkin_type}, 阈值={time_segment}, 金额={fine_amount}"
            )

    async def update_work_fine_config(
        self, checkin_type: str, time_segment: str, fine_amount: int
    ):
        """更新上下班罚款配置"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO work_fine_configs (checkin_type, time_segment, fine_amount)
                VALUES ($1, $2, $3)
                ON CONFLICT (checkin_type, time_segment) 
                DO UPDATE SET 
                    fine_amount = EXCLUDED.fine_amount,
                    created_at = CURRENT_TIMESTAMP
            """,
                checkin_type,
                time_segment,
                fine_amount,
            )

    async def clear_work_fine_rates(self, checkin_type: str):
        """清空指定类型的上下班罚款配置"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM work_fine_configs WHERE checkin_type = $1",
                checkin_type,
            )
            logger.info(f"🧹 已清空 {checkin_type} 的旧罚款配置")

    # ========== 推送设置操作 ==========
    async def get_push_settings(self) -> Dict:
        """获取推送设置"""
        cache_key = "push_settings"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM push_settings")
            settings = {row["setting_key"]: bool(row["setting_value"]) for row in rows}
            self._set_cached(cache_key, settings, 300)
            return settings

    async def update_push_setting(self, key: str, value: bool):
        """更新推送设置"""
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
        """获取群组统计信息，按指定日期查询 - 修复重置后查询问题"""
        if target_date is None:
            target_date = datetime.now().date()

        async with self.pool.acquire() as conn:
            # 🆕 关键修复：不依赖 last_updated，直接查询 user_activities 表
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
                user_data["total_accumulated_time_formatted"] = (
                    self.format_seconds_to_hms(user_data["total_accumulated_time"])
                )
                user_data["total_overtime_time_formatted"] = self.format_seconds_to_hms(
                    user_data["total_overtime_time"]
                )

                # 获取用户在 target_date 的活动详情
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
                        "time_formatted": self.format_seconds_to_hms(
                            row["accumulated_time"]
                        ),
                    }

                result.append(user_data)

            return result

    async def get_all_groups(self, retries: int = 3, delay: float = 2.0) -> List[int]:
        """
        获取所有群组ID（带超时与自愈机制）
        """
        for attempt in range(1, retries + 1):
            try:
                async with self.pool.acquire() as conn:
                    # ✅ 增加超时保护（最多等待10秒）
                    rows = await asyncio.wait_for(
                        conn.fetch("SELECT chat_id FROM groups"), timeout=10
                    )
                    return [row["chat_id"] for row in rows]

            except (
                asyncpg.InterfaceError,
                asyncpg.PostgresConnectionError,
                asyncio.TimeoutError,
            ) as e:
                logger.warning(f"⚠️ 第 {attempt} 次获取群组失败: {e}")

                # ✅ 使用新的重连机制替换旧的连接池重置
                reconnect_success = await self.reconnect()

                if reconnect_success and attempt < retries:
                    sleep_time = delay * attempt  # 指数退避
                    logger.info(f"⏳ {sleep_time:.1f}s 后重试（第 {attempt} 次）...")
                    await asyncio.sleep(sleep_time)
                else:
                    logger.error("❌ 重试次数耗尽或重连失败，放弃操作。")
                    return []

            except Exception as e:
                logger.error(f"💥 未知错误（get_all_groups）：{e}")
                return []


async def get_group_members(
    self, chat_id: int, target_date: None = None, include_all: bool = False
):
    """Compatibility wrapper for fetching group members.
    - Default behavior (target_date is None and include_all is False): preserves legacy behavior (filters by last_updated == today).
    - target_date provided: filters last_updated == target_date.
    - include_all=True: returns all users in the group (no last_updated filter) — useful for scheduled full-group resets.
    """
    from datetime import datetime

    async with self.pool.acquire() as conn:
        if include_all:
            rows = await conn.fetch(
                """
                SELECT user_id, nickname, current_activity, activity_start_time,
                       total_accumulated_time, total_activity_count, total_fines,
                       overtime_count, total_overtime_time, last_updated
                FROM users
                WHERE chat_id = $1
                """,
                chat_id,
            )
        else:
            if target_date is None:
                # Use a neutral local date if caller didn't provide one.
                # Prefer caller to pass in a timezone-aware date (e.g., from get_beijing_time()).
                today = datetime.now().date()
            else:
                today = target_date
            rows = await conn.fetch(
                """
                SELECT user_id, nickname, current_activity, activity_start_time,
                       total_accumulated_time, total_activity_count, total_fines,
                       overtime_count, total_overtime_time, last_updated
                FROM users
                WHERE chat_id = $1 AND last_updated = $2
                """,
                chat_id,
                today,
            )

        result = []
        for r in rows:
            row = dict(r)
            # Ensure numeric fields are not None
            row["total_accumulated_time"] = row.get("total_accumulated_time") or 0
            row["total_overtime_time"] = row.get("total_overtime_time") or 0
            # Optional formatting helper compatibility
            if hasattr(self, "format_seconds_to_hms"):
                try:
                    row["total_accumulated_time_formatted"] = (
                        self.format_seconds_to_hms(row["total_accumulated_time"])
                    )
                    row["total_overtime_time_formatted"] = self.format_seconds_to_hms(
                        row["total_overtime_time"]
                    )
                except Exception:
                    pass
            result.append(row)
        return result
