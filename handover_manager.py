# handover_manager.py

import logging
import asyncio
from datetime import datetime, date, timedelta, time as dt_time
from typing import Dict, Optional, Tuple, Any, List

from database import db
from config import beijing_tz, Config

logger = logging.getLogger("GroupCheckInBot.HandoverManager")


class HandoverManager:
    """换班管理器 - 处理月末夜班跨月和月初白班跨日的18小时工作制"""

    def __init__(self):
        self._cache = {}
        self._cache_ttl = {}
        self._user_cycle_cache = {}  # 用户周期缓存

    # ========== 配置管理 ==========

    async def init_handover_config(self, chat_id: int) -> bool:
        """初始化换班配置"""
        try:
            await db.execute_with_retry(
                "初始化换班配置",
                """
                INSERT INTO shift_handover_configs 
                (chat_id, handover_enabled, night_start_time, day_start_time,
                 handover_night_hours, handover_day_hours, normal_night_hours, normal_day_hours)
                VALUES ($1, TRUE, '21:00', '09:00', 18, 18, 12, 12)
                ON CONFLICT (chat_id) DO NOTHING
                """,
                chat_id,
            )
            self._invalidate_cache(chat_id)
            return True
        except Exception as e:
            logger.error(f"初始化换班配置失败 {chat_id}: {e}")
            return False

    async def get_handover_config(self, chat_id: int) -> Dict[str, Any]:
        """获取换班配置"""
        cache_key = f"handover_config:{chat_id}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        try:
            row = await db.execute_with_retry(
                "获取换班配置",
                "SELECT * FROM shift_handover_configs WHERE chat_id = $1",
                chat_id,
                fetchrow=True,
            )

            if not row:
                await self.init_handover_config(chat_id)
                row = await db.execute_with_retry(
                    "获取换班配置",
                    "SELECT * FROM shift_handover_configs WHERE chat_id = $1",
                    chat_id,
                    fetchrow=True,
                )

            result = dict(row) if row else self._get_default_config()
            self._set_cached(cache_key, result, 300)
            return result

        except Exception as e:
            logger.error(f"获取换班配置失败 {chat_id}: {e}")
            return self._get_default_config()

    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            "chat_id": 0,
            "handover_enabled": True,
            "night_start_time": "21:00",
            "day_start_time": "09:00",
            "handover_night_hours": 18,
            "handover_day_hours": 18,
            "normal_night_hours": 12,
            "normal_day_hours": 12,
        }

    # ========== 核心时间判定 ==========

    async def determine_current_period(
        self,
        chat_id: int,
        current_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        根据时间判定当前属于哪个时期

        返回: {
            'period_type': 'handover_night'|'handover_day'|'normal_night'|'normal_day',
            'business_date': date,      # 业务日期（用于计数）
            'actual_date': date,         # 实际日期（用于存储）
            'cycle': 1|2,                # 当前周期
            'hours_elapsed': float,      # 已过小时数
            'total_hours': int,          # 总工作时长
            'is_handover': bool,          # 是否是换班日
            'next_reset_time': datetime   # 下次重置时间
        }
        """
        if current_time is None:
            current_time = db.get_beijing_time()

        config = await self.get_handover_config(chat_id)
        night_start = config.get("night_start_time", "21:00")
        day_start = config.get("day_start_time", "09:00")

        night_h, night_m = map(int, night_start.split(":"))
        day_h, day_m = map(int, day_start.split(":"))

        current_date = current_time.date()
        current_hour = current_time.hour
        current_minute = current_time.minute
        current_decimal = current_hour + current_minute / 60

        # 获取本月最后一天
        if current_date.month == 12:
            next_month = date(current_date.year + 1, 1, 1)
        else:
            next_month = date(current_date.year, current_date.month + 1, 1)
        last_day = (next_month - timedelta(days=1)).day

        # 是否是月末最后一天
        is_last_day = current_date.day == last_day
        # 是否是月初第一天
        is_first_day = current_date.day == 1
        # 是否是月初第二天
        is_second_day = current_date.day == 2

        # ========== 换班夜班判定 ==========

        # 场景1：月末最后一天 21:00 之后
        if is_last_day and current_decimal >= night_h + night_m / 60:
            start_dt = datetime.combine(
                current_date, dt_time(night_h, night_m)
            ).replace(tzinfo=current_time.tzinfo)

            elapsed_hours = (current_time - start_dt).total_seconds() / 3600

            cycle = 1 if elapsed_hours < 12 else 2
            next_reset = (
                start_dt + timedelta(hours=12)
                if cycle == 1
                else start_dt + timedelta(hours=18)
            )

            logger.info(
                f"🌙 [换班夜班] 场景1: 已过{elapsed_hours:.1f}小时, 周期{cycle}"
            )

            return {
                "period_type": "handover_night",
                "business_date": current_date,
                "actual_date": current_date,
                "cycle": cycle,
                "hours_elapsed": elapsed_hours,
                "total_hours": config.get("handover_night_hours", 18),
                "is_handover": True,
                "next_reset_time": next_reset,
            }

        # 场景2：次月第一天 00:00 - 15:00（夜班跨天）
        if is_first_day and current_decimal < 15:
            last_day_date = current_date - timedelta(days=1)
            start_dt = datetime.combine(
                last_day_date, dt_time(night_h, night_m)
            ).replace(tzinfo=current_time.tzinfo)

            elapsed_hours = (current_time - start_dt).total_seconds() / 3600

            cycle = 1 if elapsed_hours < 12 else 2
            next_reset = (
                start_dt + timedelta(hours=12)
                if cycle == 1
                else start_dt + timedelta(hours=18)
            )

            logger.info(
                f"🌙 [换班夜班跨天] 场景2: 已过{elapsed_hours:.1f}小时, 周期{cycle}"
            )

            return {
                "period_type": "handover_night",
                "business_date": last_day_date,  # 业务日期是上月最后一天
                "actual_date": current_date,
                "cycle": cycle,
                "hours_elapsed": elapsed_hours,
                "total_hours": config.get("handover_night_hours", 18),
                "is_handover": True,
                "next_reset_time": next_reset,
            }

        # ========== 换班白班判定 ==========

        # 场景3：次月第一天 15:00 之后
        if is_first_day and current_decimal >= 15:
            start_dt = datetime.combine(current_date, dt_time(15, 0)).replace(
                tzinfo=current_time.tzinfo
            )

            elapsed_hours = (current_time - start_dt).total_seconds() / 3600

            cycle = 1 if elapsed_hours < 12 else 2
            next_reset = (
                start_dt + timedelta(hours=12)
                if cycle == 1
                else start_dt + timedelta(hours=18)
            )

            logger.info(f"☀️ [换班白班] 场景3: 已过{elapsed_hours:.1f}小时, 周期{cycle}")

            return {
                "period_type": "handover_day",
                "business_date": current_date,
                "actual_date": current_date,
                "cycle": cycle,
                "hours_elapsed": elapsed_hours,
                "total_hours": config.get("handover_day_hours", 18),
                "is_handover": True,
                "next_reset_time": next_reset,
            }

        # 场景4：次月第二天 00:00 - 09:00（白班跨天）
        if is_second_day and current_decimal < 9:
            first_day_date = current_date - timedelta(days=1)
            start_dt = datetime.combine(first_day_date, dt_time(15, 0)).replace(
                tzinfo=current_time.tzinfo
            )

            elapsed_hours = (current_time - start_dt).total_seconds() / 3600

            cycle = 1 if elapsed_hours < 12 else 2
            next_reset = (
                start_dt + timedelta(hours=12)
                if cycle == 1
                else start_dt + timedelta(hours=18)
            )

            logger.info(
                f"☀️ [换班白班跨天] 场景4: 已过{elapsed_hours:.1f}小时, 周期{cycle}"
            )

            return {
                "period_type": "handover_day",
                "business_date": first_day_date,  # 业务日期是次月第一天
                "actual_date": current_date,
                "cycle": cycle,
                "hours_elapsed": elapsed_hours,
                "total_hours": config.get("handover_day_hours", 18),
                "is_handover": True,
                "next_reset_time": next_reset,
            }

        # ========== 正常班次 ==========

        # 正常夜班：每天21:00 - 次日09:00
        if (current_decimal >= night_h + night_m / 60) or (
            current_decimal < day_h + day_m / 60
        ):
            if current_decimal >= night_h + night_m / 60:
                business_date = current_date
                start_dt = datetime.combine(
                    current_date, dt_time(night_h, night_m)
                ).replace(tzinfo=current_time.tzinfo)
            else:
                business_date = current_date - timedelta(days=1)
                start_dt = datetime.combine(
                    current_date - timedelta(days=1), dt_time(night_h, night_m)
                ).replace(tzinfo=current_time.tzinfo)

            elapsed_hours = (current_time - start_dt).total_seconds() / 3600
            next_reset = start_dt + timedelta(
                hours=config.get("normal_night_hours", 12)
            )

            logger.debug(f"🌙 [正常夜班] 业务日期: {business_date}")

            return {
                "period_type": "normal_night",
                "business_date": business_date,
                "actual_date": current_date,
                "cycle": 1,
                "hours_elapsed": elapsed_hours,
                "total_hours": config.get("normal_night_hours", 12),
                "is_handover": False,
                "next_reset_time": next_reset,
            }
        else:
            # 正常白班：每天09:00 - 21:00
            start_dt = datetime.combine(current_date, dt_time(day_h, day_m)).replace(
                tzinfo=current_time.tzinfo
            )
            elapsed_hours = (current_time - start_dt).total_seconds() / 3600
            next_reset = start_dt + timedelta(hours=config.get("normal_day_hours", 12))

            logger.debug(f"☀️ [正常白班] 业务日期: {current_date}")

            return {
                "period_type": "normal_day",
                "business_date": current_date,
                "actual_date": current_date,
                "cycle": 1,
                "hours_elapsed": elapsed_hours,
                "total_hours": config.get("normal_day_hours", 12),
                "is_handover": False,
                "next_reset_time": next_reset,
            }

    # ========== 用户周期管理 ==========

    async def get_user_cycle(
        self,
        chat_id: int,
        user_id: int,
        business_date: date,
        period_type: str,
        cycle: int,
    ) -> Optional[Dict[str, Any]]:
        """获取用户指定周期的数据"""
        cache_key = (
            f"user_cycle:{chat_id}:{user_id}:{business_date}:{period_type}:{cycle}"
        )

        if cache_key in self._user_cycle_cache:
            return self._user_cycle_cache[cache_key]

        try:
            shift_type = "night" if "night" in period_type else "day"

            row = await db.execute_with_retry(
                "获取用户周期",
                """
                SELECT * FROM user_handover_cycles 
                WHERE chat_id = $1 AND user_id = $2 
                  AND handover_date = $3 AND shift_type = $4 AND cycle_number = $5
                """,
                chat_id,
                user_id,
                business_date,
                shift_type,
                cycle,
                fetchrow=True,
            )

            if row:
                result = dict(row)
                self._user_cycle_cache[cache_key] = result
                return result
            return None

        except Exception as e:
            logger.error(f"获取用户周期失败 {chat_id}-{user_id}: {e}")
            return None

    async def create_user_cycle(
        self,
        chat_id: int,
        user_id: int,
        business_date: date,
        period_type: str,
        cycle: int,
        start_time: Optional[datetime] = None,
    ) -> bool:
        """创建用户新周期"""
        if start_time is None:
            start_time = db.get_beijing_time()

        shift_type = "night" if "night" in period_type else "day"

        try:
            await db.execute_with_retry(
                "创建用户周期",
                """
                INSERT INTO user_handover_cycles 
                (chat_id, user_id, handover_date, shift_type, cycle_number, 
                 cycle_start_time, total_work_seconds)
                VALUES ($1, $2, $3, $4, $5, $6, 0)
                ON CONFLICT (chat_id, user_id, handover_date, shift_type, cycle_number) 
                DO NOTHING
                """,
                chat_id,
                user_id,
                business_date,
                shift_type,
                cycle,
                start_time,
            )

            cache_key = (
                f"user_cycle:{chat_id}:{user_id}:{business_date}:{period_type}:{cycle}"
            )
            self._user_cycle_cache.pop(cache_key, None)

            return True
        except Exception as e:
            logger.error(f"创建用户周期失败 {chat_id}-{user_id}: {e}")
            return False

    async def update_user_cycle_time(
        self,
        chat_id: int,
        user_id: int,
        business_date: date,
        period_type: str,
        cycle: int,
        elapsed_seconds: int,
    ) -> Tuple[int, bool]:
        """
        更新用户周期工作时间

        返回: (新累计时间, 是否达到周期切换点)
        """
        cycle_data = await self.get_user_cycle(
            chat_id, user_id, business_date, period_type, cycle
        )

        if not cycle_data and cycle == 1:
            # 周期1不存在，创建它
            await self.create_user_cycle(
                chat_id, user_id, business_date, period_type, 1
            )
            cycle_data = await self.get_user_cycle(
                chat_id, user_id, business_date, period_type, 1
            )

        if not cycle_data:
            logger.warning(f"用户 {user_id} 周期 {cycle} 不存在")
            return 0, False

        new_total = cycle_data["total_work_seconds"] + elapsed_seconds
        threshold_seconds = 12 * 3600  # 12小时

        try:
            await db.execute_with_retry(
                "更新用户周期工作时间",
                """
                UPDATE user_handover_cycles 
                SET total_work_seconds = $1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE chat_id = $2 AND user_id = $3 
                  AND handover_date = $4 AND shift_type = $5 AND cycle_number = $6
                """,
                new_total,
                chat_id,
                user_id,
                business_date,
                "night" if "night" in period_type else "day",
                cycle,
            )

            # 更新缓存
            cache_key = (
                f"user_cycle:{chat_id}:{user_id}:{business_date}:{period_type}:{cycle}"
            )
            if cache_key in self._user_cycle_cache:
                self._user_cycle_cache[cache_key]["total_work_seconds"] = new_total

            # 判断是否达到12小时阈值（从小于12小时变为大于等于12小时）
            reached_threshold = (
                cycle_data["total_work_seconds"] < threshold_seconds
                and new_total >= threshold_seconds
            )

            return new_total, reached_threshold

        except Exception as e:
            logger.error(f"更新用户周期工作时间失败 {chat_id}-{user_id}: {e}")
            return cycle_data["total_work_seconds"], False

    # ========== 对外核心接口 ==========

    async def get_activity_count(
        self,
        chat_id: int,
        user_id: int,
        activity: str,
        shift: str,
        current_time: Optional[datetime] = None,
    ) -> int:
        """
        获取用户在当前周期的活动计数

        关键：在换班日，12小时后计数重置
        """
        if current_time is None:
            current_time = db.get_beijing_time()

        # 获取当前时期信息
        period = await self.determine_current_period(chat_id, current_time)

        # 查询该业务日期的所有计数
        all_counts = await db.get_user_activity_count_by_shift(
            chat_id, user_id, activity, shift, query_date=period["business_date"]
        )

        if not period["is_handover"]:
            # 非换班日，直接返回
            logger.debug(f"📊 [计数] 正常日: {all_counts}")
            return all_counts

        # 换班日，获取当前周期
        cycle_data = await self.get_user_cycle(
            chat_id,
            user_id,
            period["business_date"],
            period["period_type"],
            period["cycle"],
        )

        if period["cycle"] == 1:
            # 周期1：返回所有计数（周期2还未开始）
            logger.debug(
                f"🔄 [计数-周期1] 业务日期 {period['business_date']}, 计数: {all_counts}"
            )
            return all_counts
        else:
            # 周期2：计数重置，从0开始
            logger.debug(
                f"🔄 [计数-周期2] 业务日期 {period['business_date']}, 计数重置"
            )
            return 0

    async def record_activity(
        self,
        chat_id: int,
        user_id: int,
        activity: str,
        elapsed_seconds: int,
        current_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        记录一次活动完成

        返回: {
            'business_date': date,      # 应该使用的业务日期
            'cycle': int,                # 当前周期
            'should_reset_count': bool,  # 是否应该重置计数（达到12小时）
            'period_type': str           # 时期类型
        }
        """
        if current_time is None:
            current_time = db.get_beijing_time()

        # 获取当前时期信息
        period = await self.determine_current_period(chat_id, current_time)

        result = {
            "business_date": period["business_date"],
            "cycle": period["cycle"],
            "should_reset_count": False,
            "period_type": period["period_type"],
            "is_handover": period["is_handover"],
        }

        if not period["is_handover"]:
            # 非换班日，直接返回
            return result

        # 换班日，更新周期时间
        new_total, reached_threshold = await self.update_user_cycle_time(
            chat_id,
            user_id,
            period["business_date"],
            period["period_type"],
            period["cycle"],
            elapsed_seconds,
        )

        result["should_reset_count"] = reached_threshold

        logger.info(
            f"📝 [换班记录] 用户{user_id} {period['period_type']} "
            f"周期{period['cycle']} 累计 {new_total//60}分钟, "
            f"阈值达到: {reached_threshold}"
        )

        return result

    async def should_reset_data(
        self,
        chat_id: int,
        current_time: Optional[datetime] = None,
    ) -> Tuple[bool, date, str]:
        """
        判断是否应该执行数据重置导出

        返回: (是否重置, 目标日期, 原因)
        """
        if current_time is None:
            current_time = db.get_beijing_time()

        period = await self.determine_current_period(chat_id, current_time)

        # 检查是否达到总工作时长
        if period["hours_elapsed"] >= period["total_hours"] - 0.1:  # 加0.1容差
            if "handover" in period["period_type"]:
                reason = f"handover_complete_{period['period_type']}"
            else:
                reason = "normal_complete"

            logger.info(
                f"🔄 [重置触发] {period['period_type']} 已完成 {period['total_hours']}小时"
            )
            return True, period["business_date"], reason

        return False, period["business_date"], "not_complete"

    # ========== 缓存管理 ==========

    def _get_cached(self, key: str):
        import time

        if key in self._cache_ttl and time.time() < self._cache_ttl[key]:
            return self._cache.get(key)
        return None

    def _set_cached(self, key: str, value: Any, ttl: int = 300):
        import time

        self._cache[key] = value
        self._cache_ttl[key] = time.time() + ttl

    def _invalidate_cache(self, chat_id: int):
        cache_key = f"handover_config:{chat_id}"
        self._cache.pop(cache_key, None)
        self._cache_ttl.pop(cache_key, None)


# 全局实例
handover_manager = HandoverManager()
