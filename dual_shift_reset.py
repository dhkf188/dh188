"""
双班硬重置 - 单文件完整实现
放置在与 main.py、database.py 同级目录

使用规范：
- 不修改原有单班逻辑
- 所有时间动态计算，无硬编码
- 完全复用已有导出函数
- 最小侵入：只需修改1处命令入口
"""

import logging
from datetime import datetime, timedelta, date
from typing import Dict, Optional, Any

# 直接导入同级模块
from database import db
from main import export_and_push_csv, auto_end_current_activity

logger = logging.getLogger("GroupCheckInBot.DualShiftReset")


# ========== 1. 调度入口（供cmd_setresettime调用） ==========


async def handle_hard_reset(chat_id: int, operator_id: Optional[int] = None) -> bool:
    """
    硬重置总调度入口 - 单班/双班分流
    这是唯一需要从外部调用的函数

    返回:
        True - 双班模式已处理完成，调用方不应再执行原逻辑
        False - 单班模式或出错，调用方应继续执行原逻辑
    """
    try:
        # 1. 获取班次配置，判断模式
        shift_config = await db.get_shift_config(chat_id)
        is_dual_mode = shift_config.get("dual_mode", False)

        # 2. 单班模式 - 完全走原有逻辑
        if not is_dual_mode:
            logger.info(f"🔄 [单班模式] 群组 {chat_id} 继续执行原有硬重置逻辑")
            return False

        # 3. 双班模式 - 执行新的双班硬重置流程
        logger.info(f"🔄 [双班模式] 群组 {chat_id} 执行双班硬重置")
        success = await _dual_shift_hard_reset(chat_id, operator_id)

        if success:
            logger.info(f"✅ [双班硬重置] 群组 {chat_id} 完成")
        else:
            logger.error(f"❌ [双班硬重置] 群组 {chat_id} 失败")

        return True  # 已处理，调用方不应再执行原逻辑

    except Exception as e:
        logger.error(f"❌ 硬重置调度失败 {chat_id}: {e}")
        return False  # 异常时降级，走原逻辑


# ========== 2. 双班硬重置核心流程 ==========


async def _dual_shift_hard_reset(
    chat_id: int, operator_id: Optional[int] = None
) -> bool:
    """
    双班硬重置主流程 - 严格按照规范顺序执行
    """
    try:
        logger.info(f"🚀 [双班硬重置] 开始处理群组 {chat_id}")

        # ===== 0. 获取所有必要配置 =====
        now = db.get_beijing_time()
        group_data = await db.get_group_cached(chat_id)
        shift_config = await db.get_shift_config(chat_id)

        # 重置时间
        reset_hour = group_data.get("reset_hour", 0)
        reset_minute = group_data.get("reset_minute", 0)
        reset_time_today = now.replace(
            hour=reset_hour, minute=reset_minute, second=0, microsecond=0
        )

        # 计算今天起点（白班上班允许窗口）
        today_start = await _calculate_today_start(chat_id, shift_config, now)
        today_date = today_start.date()

        # 昨天日期（用于导出和清理）
        yesterday = (now - timedelta(days=1)).date()

        logger.info(
            f"📅 [时间计算] 群组{chat_id}: "
            f"重置时间={reset_time_today.strftime('%H:%M')}, "
            f"今天起点={today_start.strftime('%m-%d %H:%M')}, "
            f"昨天={yesterday}"
        )

        # ===== 1. 强制结束白班未下班 =====
        white_force_count = await _force_end_white_shift(chat_id, now, yesterday)

        # ===== 2. 强制结束夜班未下班（reset_time + 2h） =====
        night_force_count = await _force_end_night_shift(
            chat_id, now, reset_time_today, yesterday
        )

        # ===== 3. 导出昨天数据（白班+夜班） =====
        export_success = await _export_yesterday_data(chat_id, yesterday)

        # ===== 4. 清理旧数据 =====
        cleanup_stats = await _cleanup_old_data(chat_id, yesterday, today_date)

        # ===== 5. 记录操作日志 =====
        logger.info(
            f"📊 [双班硬重置完成] 群组{chat_id}\n"
            f"   • 强制结束白班: {white_force_count} 人\n"
            f"   • 强制结束夜班: {night_force_count} 人\n"
            f"   • 数据导出: {'✅成功' if export_success else '❌失败'}\n"
            f"   • 清理记录: {cleanup_stats}\n"
            f"   • 操作员: {operator_id or '系统'}"
        )

        return True

    except Exception as e:
        logger.error(f"❌ [双班硬重置] 失败 {chat_id}: {e}")
        logger.exception(e)
        return False


# ========== 3. 时间计算函数 ==========


async def _calculate_today_start(
    chat_id: int, shift_config: Dict[str, Any], now: datetime
) -> datetime:
    """
    计算"今天"的业务起点
    使用白班上班允许窗口的开始时间
    """
    # 获取白班上班时间
    day_start_str = shift_config.get("day_start", "09:00")
    grace_before = shift_config.get("grace_before", 120)

    # 解析时间
    try:
        day_start_time = datetime.strptime(day_start_str, "%H:%M").time()
    except (ValueError, TypeError):
        day_start_time = datetime.strptime("09:00", "%H:%M").time()

    # 构建今天的时间点
    today = now.date()
    day_start_dt = datetime.combine(today, day_start_time).replace(tzinfo=now.tzinfo)

    # 计算上班允许窗口起点
    today_start = day_start_dt - timedelta(minutes=grace_before)

    # 如果当前时间早于今天起点，说明还没进入今天业务周期
    if now < today_start:
        return today_start - timedelta(days=1)

    return today_start


async def get_dual_business_date(chat_id: int, now: datetime = None) -> date:
    """
    获取双班模式的业务日期
    供 reset_daily_data_if_needed 函数调用
    """
    if now is None:
        now = db.get_beijing_time()

    shift_config = await db.get_shift_config(chat_id)
    today_start = await _calculate_today_start(chat_id, shift_config, now)
    return today_start.date()


async def is_in_today_period(chat_id: int, now: datetime = None) -> bool:
    """
    判断当前时间是否属于"今天"的业务周期
    """
    if now is None:
        now = db.get_beijing_time()

    shift_config = await db.get_shift_config(chat_id)
    today_start = await _calculate_today_start(chat_id, shift_config, now)
    return now >= today_start


# ========== 4. 强制结束白班 ==========


async def _force_end_white_shift(chat_id: int, now: datetime, yesterday: date) -> int:
    """
    强制结束昨天白班未下班的用户
    1. 查找昨天白班已上班但未下班的用户
    2. 调用 auto_end_current_activity 强制结束
    """
    force_count = 0

    try:
        # 查找昨天白班上班但未下班的用户
        async with db.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT u.user_id, u.nickname, u.current_activity, 
                       u.activity_start_time
                FROM users u
                LEFT JOIN work_records wr 
                    ON u.chat_id = wr.chat_id 
                    AND u.user_id = wr.user_id 
                    AND wr.record_date = $2 
                    AND wr.checkin_type = 'work_end'
                    AND wr.shift = 'day'
                WHERE u.chat_id = $1
                  AND u.current_activity IS NOT NULL
                  AND (u.shift = 'day' OR u.shift IS NULL)
                  AND wr.id IS NULL
                """,
                chat_id,
                yesterday,
            )

            for row in rows:
                try:
                    user_id = row["user_id"]
                    user_data = dict(row)

                    # 调用已有函数强制结束活动
                    await auto_end_current_activity(
                        chat_id, user_id, user_data, now, None
                    )
                    force_count += 1
                    logger.info(
                        f"🟡 [白班强制结束] 用户{user_id} 活动{row['current_activity']}"
                    )

                except Exception as e:
                    logger.error(f"强制结束白班用户失败 {user_id}: {e}")
                    continue

        if force_count > 0:
            logger.info(
                f"✅ [白班强制结束] 群组{chat_id} 共处理 {force_count} 个未下班用户"
            )

    except Exception as e:
        logger.error(f"❌ [白班强制结束] 失败 {chat_id}: {e}")

    return force_count


# ========== 5. 强制结束夜班 ==========


async def _force_end_night_shift(
    chat_id: int, now: datetime, reset_time_today: datetime, yesterday: date
) -> int:
    """
    强制结束夜班未下班的用户
    强制时间 = reset_time + 2小时
    """
    force_count = 0

    try:
        # 计算强制结束时间
        night_force_close_time = reset_time_today + timedelta(hours=2)

        # 如果当前时间未到强制结束时间，不执行
        if now < night_force_close_time:
            logger.debug(
                f"群组{chat_id} 未到夜班强制结束时间: {night_force_close_time.strftime('%H:%M')}"
            )
            return 0

        # 查找夜班未下班的用户
        async with db.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT u.user_id, u.nickname, u.current_activity, 
                       u.activity_start_time
                FROM users u
                LEFT JOIN work_records wr 
                    ON u.chat_id = wr.chat_id 
                    AND u.user_id = wr.user_id 
                    AND wr.record_date = $2 
                    AND wr.checkin_type = 'work_end'
                    AND wr.shift = 'night'
                WHERE u.chat_id = $1
                  AND u.current_activity IS NOT NULL
                  AND u.shift = 'night'
                  AND wr.id IS NULL
                """,
                chat_id,
                yesterday,
            )

            for row in rows:
                try:
                    user_id = row["user_id"]
                    user_data = dict(row)

                    await auto_end_current_activity(
                        chat_id, user_id, user_data, now, None
                    )
                    force_count += 1
                    logger.info(
                        f"🌙 [夜班强制结束] 用户{user_id} 活动{row['current_activity']}"
                    )

                except Exception as e:
                    logger.error(f"强制结束夜班用户失败 {user_id}: {e}")
                    continue

        if force_count > 0:
            logger.info(
                f"✅ [夜班强制结束] 群组{chat_id} 共处理 {force_count} 个未下班用户"
            )

    except Exception as e:
        logger.error(f"❌ [夜班强制结束] 失败 {chat_id}: {e}")

    return force_count


# ========== 6. 导出昨天数据 ==========


async def _export_yesterday_data(chat_id: int, yesterday: date) -> bool:
    """
    导出昨天白班+夜班数据
    完全复用已有 export_and_push_csv 函数
    """
    try:
        # 生成文件名
        file_name = f"dual_shift_backup_{chat_id}_{yesterday.strftime('%Y%m%d')}.csv"

        # 调用已有导出函数
        success = await export_and_push_csv(
            chat_id=chat_id,
            target_date=yesterday,
            file_name=file_name,
            is_daily_reset=True,
            from_monthly_table=False,
        )

        if success:
            logger.info(f"✅ [数据导出] 群组{chat_id} 昨日{yesterday} 数据导出成功")
        else:
            logger.warning(f"⚠️ [数据导出] 群组{chat_id} 昨日无数据或导出失败")

        return success

    except Exception as e:
        logger.error(f"❌ [数据导出] 失败 {chat_id}: {e}")
        return False


# ========== 7. 数据清理 ==========


async def _cleanup_old_data(
    chat_id: int, yesterday: date, today_date: date
) -> Dict[str, int]:
    """
    清理旧数据，仅保留今天的数据
    规则：
    - 昨天之前的数据：直接删除
    - 昨天的数据：已导出，删除
    - 今天的数据：保留
    """
    stats = {
        "user_activities": 0,
        "work_records": 0,
        "daily_statistics": 0,
        "before_yesterday": 0,
    }

    try:
        async with db.pool.acquire() as conn:
            async with conn.transaction():

                # 1. 删除昨天之前的所有数据
                before_yesterday = yesterday - timedelta(days=1)

                # user_activities
                result = await conn.execute(
                    "DELETE FROM user_activities WHERE chat_id = $1 AND activity_date <= $2",
                    chat_id,
                    before_yesterday,
                )
                stats["before_yesterday"] += _parse_delete_count(result)

                # work_records
                result = await conn.execute(
                    "DELETE FROM work_records WHERE chat_id = $1 AND record_date <= $2",
                    chat_id,
                    before_yesterday,
                )
                stats["before_yesterday"] += _parse_delete_count(result)

                # daily_statistics
                result = await conn.execute(
                    "DELETE FROM daily_statistics WHERE chat_id = $1 AND record_date <= $2",
                    chat_id,
                    before_yesterday,
                )
                stats["before_yesterday"] += _parse_delete_count(result)

                # 2. 删除昨天的数据（已导出）
                result = await conn.execute(
                    "DELETE FROM user_activities WHERE chat_id = $1 AND activity_date = $2",
                    chat_id,
                    yesterday,
                )
                stats["user_activities"] = _parse_delete_count(result)

                result = await conn.execute(
                    "DELETE FROM work_records WHERE chat_id = $1 AND record_date = $2",
                    chat_id,
                    yesterday,
                )
                stats["work_records"] = _parse_delete_count(result)

                result = await conn.execute(
                    "DELETE FROM daily_statistics WHERE chat_id = $1 AND record_date = $2",
                    chat_id,
                    yesterday,
                )
                stats["daily_statistics"] = _parse_delete_count(result)

                # 3. 清理 users 表中的昨日活动状态
                await conn.execute(
                    """
                    UPDATE users 
                    SET current_activity = NULL, 
                        activity_start_time = NULL,
                        last_updated = $2
                    WHERE chat_id = $1 
                      AND (shift = 'day' OR shift = 'night')
                      AND last_updated <= $3
                    """,
                    chat_id,
                    today_date,
                    yesterday,
                )

        total_deleted = (
            stats["user_activities"]
            + stats["work_records"]
            + stats["daily_statistics"]
            + stats["before_yesterday"]
        )

        if total_deleted > 0:
            logger.info(
                f"🧹 [数据清理] 群组{chat_id}: "
                f"删除昨日活动{stats['user_activities']}条, "
                f"工作记录{stats['work_records']}条, "
                f"日统计{stats['daily_statistics']}条, "
                f"更早数据{stats['before_yesterday']}条"
            )

    except Exception as e:
        logger.error(f"❌ [数据清理] 失败 {chat_id}: {e}")

    return stats


# ========== 8. 辅助函数 ==========


def _parse_delete_count(result: str) -> int:
    """解析 DELETE 语句返回的行数"""
    if not result or not isinstance(result, str):
        return 0
    try:
        parts = result.split()
        if len(parts) >= 2 and parts[0] == "DELETE":
            return int(parts[-1])
    except (ValueError, IndexError):
        pass
    return 0
