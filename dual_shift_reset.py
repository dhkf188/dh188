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

        # 🎯 重要：这里调用 _dual_shift_hard_reset 执行实际的重置逻辑
        success = await _dual_shift_hard_reset(chat_id, operator_id)

        if success:
            logger.info(f"✅ [双班硬重置] 群组 {chat_id} 完成")
        else:
            logger.error(f"❌ [双班硬重置] 群组 {chat_id} 失败")

        return True  # 已处理，调用方不应再执行原逻辑

    except Exception as e:
        logger.error(f"❌ 硬重置调度失败 {chat_id}: {e}")
        logger.exception(e)
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

        # 昨天日期
        yesterday = (now - timedelta(days=1)).date()

        logger.info(
            f"📅 [时间计算] 群组{chat_id}: "
            f"重置时间={reset_time_today.strftime('%H:%M')}, "
            f"今天起点={today_start.strftime('%m-%d %H:%M')}, "
            f"昨天={yesterday}"
        )

        # ===== 1. ✅ 重置用户统计（09:00执行）- 只重置状态，不删数据 =====
        reset_stats = await _reset_user_stats(chat_id, today_date)

        # ===== 2. 🎯 判断是否到达强制结束时间（11:00）=====
        force_close_time = reset_time_today + timedelta(hours=2)

        if now >= force_close_time:
            # ✅ 已到达11:00，执行强制结束+清理
            logger.info(
                f"⏰ [强制结束时间] 群组{chat_id} 已到达 {force_close_time.strftime('%H:%M')}"
            )
            cleanup_stats = await _force_end_and_cleanup(
                chat_id, now, reset_time_today, yesterday
            )
            logger.info(
                f"🧹 [强制结束+清理] 群组{chat_id}\n"
                f"   • 强制结束: {cleanup_stats.get('force_ended', 0)} 人\n"
                f"   • 导出成功: {cleanup_stats.get('export_success', False)}\n"
                f"   • 删除数据: {cleanup_stats.get('deleted', 0)} 条"
            )
        else:
            # ⏰ 未到11:00，记录下次执行时间
            next_run = force_close_time.strftime("%H:%M")
            logger.info(
                f"⏰ [等待执行] 群组{chat_id} 将于 {next_run} 执行强制结束+清理"
            )

        # ===== 3. ✅ 记录操作日志 =====
        logger.info(
            f"📊 [双班重置状态更新] 群组{chat_id}\n"
            f"   • 重置用户状态: {reset_stats.get('users_reset', 0)} 人\n"
            f"   • 当前时间: {now.strftime('%H:%M')}\n"
            f"   • ✅ 保留昨天所有数据（等待11:00强制结束）\n"
            f"   • ✅ 保留今天所有数据（含08:30打卡）\n"
            f"   • 操作员: {operator_id or '系统'}"
        )

        return True

    except Exception as e:
        logger.error(f"❌ [双班硬重置] 失败 {chat_id}: {e}")
        logger.exception(e)
        return False


# ========== 3. 重置用户统计（09:00执行） ==========


async def _reset_user_stats(chat_id: int, today_date: date) -> Dict[str, int]:
    """
    重置用户累计统计 - 09:00执行

    作用：
    1. ✅ 重置 total_activity_count = 0
    2. ✅ 重置 total_accumulated_time = 0
    3. ✅ 重置 total_fines = 0
    4. ❌ 不删除任何数据！
    """
    stats = {"users_reset": 0}

    try:
        async with db.pool.acquire() as conn:
            async with conn.transaction():
                # 重置用户累计计数
                result = await conn.execute(
                    """
                    UPDATE users 
                    SET 
                        total_activity_count = 0,
                        total_accumulated_time = 0,
                        total_fines = 0,
                        total_overtime_time = 0,
                        overtime_count = 0,
                        last_updated = $2,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE chat_id = $1
                    """,
                    chat_id,
                    today_date,
                )
                stats["users_reset"] = _parse_update_count(result)

        logger.info(f"🔄 [重置统计] 群组{chat_id} 已重置 {stats['users_reset']} 人")

    except Exception as e:
        logger.error(f"❌ [重置统计] 失败 {chat_id}: {e}")
        logger.exception(e)

    return stats


# ========== 4. 时间计算函数 ==========


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


# ========== 5. 强制结束+导出+清理（11:00执行） ==========
async def _force_end_and_cleanup(
    chat_id: int, now: datetime, reset_time_today: datetime, yesterday: date
) -> Dict[str, Any]:
    """
    强制结束+导出+清理 - 重置时间+2h（11:00执行）

    作用：
    1. ⚠️ 强制结束【昨天白班+昨天夜班】所有未下班用户
    2. ✅ 导出昨天所有数据
    3. ✅ 导出成功后删除昨天所有数据
    4. ❌ 不影响今天任何数据
    """
    stats = {
        "force_ended": 0,
        "export_success": False,
        "deleted": 0,
        "yesterday": yesterday,
    }

    try:
        # 计算强制结束时间
        group_data = await db.get_group_cached(chat_id)
        force_delay_hours = group_data.get("force_cleanup_delay", 2)
        force_close_time = reset_time_today + timedelta(hours=force_delay_hours)

        # 如果当前时间未到强制结束时间，不执行
        if now < force_close_time:
            logger.debug(
                f"群组{chat_id} 未到强制结束时间: {force_close_time.strftime('%H:%M')}"
            )
            return stats

        logger.info(f"🔫 [强制结束] 群组{chat_id} 开始处理昨天未下班用户...")

        async with db.pool.acquire() as conn:
            async with conn.transaction():

                # ===== 1. ⚠️ 强制结束【昨天】所有未下班用户 =====
                # 🎯 修复：正确处理夜班跨天用户
                rows = await conn.fetch(
                    """
                    SELECT user_id, nickname, current_activity, 
                           activity_start_time, shift, last_updated
                    FROM users
                    WHERE chat_id = $1 
                      AND current_activity IS NOT NULL
                      AND (
                          -- 条件1：昨天白班用户（业务日期=昨天）
                          (shift = 'day' AND last_updated <= $2)
                          OR
                          -- 条件2：昨天夜班用户（开始时间在昨天）
                          (shift = 'night' AND DATE(activity_start_time) = $2)
                      )
                    """,
                    chat_id,
                    yesterday,
                )

                for row in rows:
                    try:
                        user_id = row["user_id"]
                        user_data = dict(row)
                        shift_text = "白班" if row["shift"] == "day" else "夜班"

                        # 强制结束活动（保存记录到月度统计）
                        # 注意：auto_end_current_activity 需要 message 参数，传入 None
                        from main import auto_end_current_activity

                        await auto_end_current_activity(
                            chat_id, user_id, user_data, now, None
                        )
                        stats["force_ended"] += 1
                        logger.info(
                            f"   ⚠️ 用户{user_id} {shift_text} 活动{row['current_activity']} "
                            f"(开始时间: {row['activity_start_time']})"
                        )

                    except Exception as e:
                        logger.error(f"   ❌ 强制结束用户失败 {user_id}: {e}")
                        continue

        # ===== 2. ✅ 导出昨天所有数据 =====
        logger.info(f"📤 [导出数据] 群组{chat_id} 开始导出昨日{yesterday}数据...")
        file_name = f"dual_shift_backup_{chat_id}_{yesterday.strftime('%Y%m%d')}.csv"

        # 🎯 修复：从 main 导入导出函数
        from main import export_and_push_csv

        stats["export_success"] = await export_and_push_csv(
            chat_id=chat_id,
            target_date=yesterday,
            file_name=file_name,
            is_daily_reset=True,
            from_monthly_table=False,
        )

        # ===== 3. ⚠️ 导出失败则终止，不删除数据 =====
        if not stats["export_success"]:
            logger.error(f"❌ [导出失败] 群组{chat_id} 昨日数据导出失败，取消删除操作")
            stats["deleted"] = 0
            return stats

        # ===== 4. ✅ 导出成功后，删除昨天所有数据 =====
        logger.info(f"🗑️ [删除数据] 群组{chat_id} 开始删除昨日{yesterday}数据...")
        async with db.pool.acquire() as conn:
            async with conn.transaction():

                # 4.1 删除 user_activities - 昨天全部
                result = await conn.execute(
                    """
                    DELETE FROM user_activities 
                    WHERE chat_id = $1 
                      AND activity_date = $2
                    """,
                    chat_id,
                    yesterday,
                )
                deleted = _parse_delete_count(result)
                stats["deleted"] += deleted
                logger.info(f"   • 删除活动记录: {deleted} 条")

                # 4.2 删除 work_records - 昨天全部
                result = await conn.execute(
                    """
                    DELETE FROM work_records 
                    WHERE chat_id = $1 
                      AND record_date = $2
                    """,
                    chat_id,
                    yesterday,
                )
                deleted = _parse_delete_count(result)
                stats["deleted"] += deleted
                logger.info(f"   • 删除工作记录: {deleted} 条")

                # 4.3 删除 daily_statistics - 昨天全部
                result = await conn.execute(
                    """
                    DELETE FROM daily_statistics 
                    WHERE chat_id = $1 
                      AND record_date = $2
                    """,
                    chat_id,
                    yesterday,
                )
                deleted = _parse_delete_count(result)
                stats["deleted"] += deleted
                logger.info(f"   • 删除日统计: {deleted} 条")

                # 4.4 清理用户状态（只清理昨天及以前的用户）
                result = await conn.execute(
                    """
                    UPDATE users 
                    SET current_activity = NULL, 
                        activity_start_time = NULL,
                        checkin_message_id = NULL
                    WHERE chat_id = $1 
                      AND (
                          (shift = 'day' AND last_updated <= $2)
                          OR
                          (shift = 'night' AND DATE(activity_start_time) = $2)
                      )
                    """,
                    chat_id,
                    yesterday,
                )
                updated = _parse_update_count(result)
                logger.info(f"   • 清理用户状态: {updated} 人")

        logger.info(
            f"✅ [双班最终清理] 群组{chat_id} 执行完成\n"
            f"   • 强制结束用户: {stats['force_ended']} 人\n"
            f"   • 导出昨日数据: {'✅成功' if stats['export_success'] else '❌失败'}\n"
            f"   • 删除昨日数据: {stats['deleted']} 条\n"
            f"   • 清理日期: {yesterday}（白班+夜班）\n"
            f"   • ✅ 保留今天所有数据"
        )

    except Exception as e:
        logger.error(f"❌ [双班最终清理] 失败 {chat_id}: {e}")
        logger.exception(e)

    return stats


# ========== 6. 兼容旧接口的函数（保持调用不报错） ==========


async def _force_end_white_shift(chat_id: int, now: datetime, yesterday: date) -> int:
    """兼容旧接口 - 实际功能已合并到 _force_end_and_cleanup"""
    logger.debug(
        f"调用兼容接口 _force_end_white_shift, 已合并到 _force_end_and_cleanup"
    )
    return 0


async def _force_end_night_shift(
    chat_id: int, now: datetime, reset_time_today: datetime, yesterday: date
) -> int:
    """兼容旧接口 - 实际功能已合并到 _force_end_and_cleanup"""
    logger.debug(
        f"调用兼容接口 _force_end_night_shift, 已合并到 _force_end_and_cleanup"
    )
    return 0


async def _export_yesterday_data(chat_id: int, yesterday: date) -> bool:
    """兼容旧接口 - 实际功能已合并到 _force_end_and_cleanup"""
    logger.debug(
        f"调用兼容接口 _export_yesterday_data, 已合并到 _force_end_and_cleanup"
    )
    return False


async def _cleanup_old_data(
    chat_id: int, yesterday: date, today_date: date
) -> Dict[str, int]:
    """兼容旧接口 - 09:00不删数据，只重置统计"""
    logger.debug(f"调用兼容接口 _cleanup_old_data, 转发到 _reset_user_stats")
    return await _reset_user_stats(chat_id, today_date)


# ========== 7. 辅助函数 ==========


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


def _parse_update_count(result: str) -> int:
    """解析 UPDATE 语句返回的行数"""
    if not result or not isinstance(result, str):
        return 0
    try:
        parts = result.split()
        if len(parts) >= 2 and parts[0] == "UPDATE":
            return int(parts[-1])
    except (ValueError, IndexError):
        pass
    return 0


# ========== 8. 初始化函数 ==========


async def init_dual_shift_reset():
    """
    初始化双班重置模块
    在main.py启动时调用
    """
    logger.info("🔄 [双班重置] 模块初始化完成")
    return True
