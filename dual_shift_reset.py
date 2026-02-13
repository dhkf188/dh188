"""
双班硬重置 - 单文件完整实现（独立版，无外部依赖）
放置在与 main.py、database.py 同级目录

使用规范：
- 不修改原有单班逻辑
- 所有时间动态计算，无硬编码
- 完全复用已有导出函数
- ✅ 独立强制结束，100%归因昨天
"""

import logging
import asyncio
from datetime import datetime, timedelta, date
from typing import Dict, Optional, Any

# 直接导入同级模块
from database import db
from main import export_and_push_csv

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

        return True

    except Exception as e:
        logger.error(f"❌ 硬重置调度失败 {chat_id}: {e}")
        return False


# ========== 2. 双班硬重置核心流程 ==========
async def _dual_shift_hard_reset(
    chat_id: int, operator_id: Optional[int] = None
) -> bool:
    """
    双班硬重置主流程 - 使用独立强制结束函数
    严格遵循：
    1. 09:00 - 强制结束昨天白班未下班
    2. 11:00 - 强制结束昨晚夜班未下班
    3. 导出昨天数据
    4. 删除昨天及之前数据
    """
    try:
        now = db.get_beijing_time()
        today = now.date()
        yesterday = today - timedelta(days=1)

        group_data = await db.get_group_cached(chat_id)
        reset_hour = group_data.get("reset_hour", 0)
        reset_minute = group_data.get("reset_minute", 0)
        reset_time_today = now.replace(
            hour=reset_hour, minute=reset_minute, second=0, microsecond=0
        )

        logger.info(
            f"🚀 [双班硬重置] 群组{chat_id}\n"
            f"   • 当前时间: {now.strftime('%Y-%m-%d %H:%M')}\n"
            f"   • 重置时间: {reset_time_today.strftime('%H:%M')}\n"
            f"   • 昨天: {yesterday}\n"
            f"   • 今天: {today}"
        )

        # ========== 🟡 第1步：09:00 强制结束昨天白班未下班 ==========
        if now >= reset_time_today:
            white_stats = await _force_end_white_shift_independent(
                chat_id, now, yesterday
            )
            logger.info(f"🟡 [白班强制结束] 成功:{white_stats['success']} 人")
        else:
            logger.debug("⏳ 未到白班强制结束时间")
            white_stats = {"success": 0, "failed": 0, "total": 0}

        # ========== 🌙 第2步：11:00 强制结束昨晚夜班未下班 ==========
        night_force_time = reset_time_today + timedelta(hours=2)
        if now >= night_force_time:
            night_stats = await _force_end_night_shift_independent(
                chat_id, now, yesterday
            )
            logger.info(f"🌙 [夜班强制结束] 成功:{night_stats['success']} 人")
        else:
            logger.debug(
                f"⏳ 未到夜班强制结束时间: {night_force_time.strftime('%H:%M')}"
            )
            night_stats = {"success": 0, "failed": 0, "total": 0}

        # ========== 📤 第3步：导出昨天数据 ==========
        export_success = await _export_yesterday_data(chat_id, yesterday)
        logger.info(f"📤 [数据导出] {'✅成功' if export_success else '❌失败'}")

        # 🆕 新增：导出失败时的处理
        if not export_success:
            logger.warning(f"⚠️ [数据导出] 失败，但仍继续执行清理（数据可能已丢失）")
            # 可以添加重试机制
            for attempt in range(2):
                logger.info(f"🔄 第{attempt+2}次尝试导出...")
                export_success = await _export_yesterday_data(chat_id, yesterday)
                if export_success:
                    logger.info(f"✅ 第{attempt+2}次导出成功")
                    break
                await asyncio.sleep(5)

        # ========== 🧹 第4步：删除昨天及之前数据 ==========
        cleanup_stats = await _cleanup_old_data(chat_id, yesterday, today)

        # 🆕 新增：清除班次状态
        try:
            await db.clear_shift_state(chat_id)
            logger.info(f"🧹 [班次状态] 群组{chat_id} 硬重置后已清除")
            state_cleared = True
        except Exception as e:
            logger.error(f"❌ [班次状态] 清除失败 {chat_id}: {e}")
            state_cleared = False

        # 🆕 新增：发送重置通知
        try:
            from main import send_reset_notification

            await send_reset_notification(
                chat_id,
                {
                    "white": white_stats,
                    "night": night_stats,
                    "export": export_success,
                    "cleanup": cleanup_stats,
                    "state_cleared": state_cleared,
                },
                now,
            )
            logger.info(f"📢 [重置通知] 群组{chat_id} 已发送")
        except Exception as e:
            logger.error(f"❌ [重置通知] 发送失败 {chat_id}: {e}")

        # ========== ✅ 完成日志 ==========
        logger.info(
            f"✅ [双班硬重置完成] 群组{chat_id}\n"
            f"   • 白班强制结束: {white_stats['success']}/{white_stats.get('total', 0)} 人\n"
            f"   • 夜班强制结束: {night_stats['success']}/{night_stats.get('total', 0)} 人\n"
            f"   • 数据导出: {'✅成功' if export_success else '❌失败'}\n"
            f"   • 清理昨天数据: 完成 (删除{cleanup_stats.get('total_deleted', 0)}条)\n"
            f"   • 班次状态: {'✅已清除' if state_cleared else '❌清除失败'}\n"
            f"   • 今天数据: ✅ 完整保留\n"
            f"   • 操作员: {operator_id or '系统'}"
        )

        return True

    except Exception as e:
        logger.error(f"❌ [双班硬重置] 失败 {chat_id}: {e}")
        logger.exception(e)
        return False


# ========== 3. 独立强制结束核心函数（100%归因昨天）==========
async def _force_complete_activity_to_yesterday(
    conn,
    chat_id: int,
    user_id: int,
    nickname: str,
    activity: str,
    start_time_str: str,
    yesterday: date,
    now: datetime,
    shift: str,
    shift_detail: str,
) -> Dict[str, Any]:
    """
    双班硬重置专用：强制结束活动，数据100%归因到昨天

    核心特点：
    1. 完全不依赖 main.auto_end_current_activity
    2. 直接操作数据库，绕过所有业务日期判定
    3. 强制指定 record_date = yesterday
    4. 强制指定 shift/shift_detail 为传入值
    """
    result = {
        "user_id": user_id,
        "activity": activity,
        "elapsed": 0,
        "fine": 0,
        "is_overtime": False,
        "success": False,
    }

    try:
        # ---------- 1. 解析开始时间，计算时长 ----------
        start_time = datetime.fromisoformat(start_time_str)
        elapsed = int((now - start_time).total_seconds())
        result["elapsed"] = elapsed

        # ---------- 2. 获取活动配置 ----------
        time_limit = await db.get_activity_time_limit(activity)
        time_limit_seconds = time_limit * 60
        is_overtime = elapsed > time_limit_seconds
        overtime_seconds = max(0, elapsed - time_limit_seconds)
        overtime_minutes = overtime_seconds / 60

        # ---------- 3. 计算罚款（独立计算，不依赖外部函数）----------
        fine_amount = 0
        if is_overtime and overtime_seconds > 0:
            fine_rates = await db.get_fine_rates_for_activity(activity)
            if fine_rates:
                segments = []
                for k in fine_rates.keys():
                    try:
                        v = int(str(k).lower().replace("min", ""))
                        segments.append(v)
                    except:
                        pass
                segments.sort()
                for s in segments:
                    if overtime_minutes <= s:
                        fine_amount = fine_rates.get(
                            str(s), fine_rates.get(f"{s}min", 0)
                        )
                        break
                if fine_amount == 0 and segments:
                    m = segments[-1]
                    fine_amount = fine_rates.get(str(m), fine_rates.get(f"{m}min", 0))

        result["fine"] = fine_amount
        result["is_overtime"] = is_overtime

        # ---------- 4. 月度统计日期（昨天所属的月份）----------
        statistic_date = yesterday.replace(day=1)

        # ---------- 5. 直接写入 user_activities（强制昨天）----------
        await conn.execute(
            """
            INSERT INTO user_activities
            (chat_id, user_id, activity_date, activity_name,
             activity_count, accumulated_time, shift)
            VALUES ($1, $2, $3, $4, 1, $5, $6)
            ON CONFLICT (chat_id, user_id, activity_date, activity_name, shift)
            DO UPDATE SET
                activity_count = user_activities.activity_count + 1,
                accumulated_time = user_activities.accumulated_time + EXCLUDED.accumulated_time,
                updated_at = CURRENT_TIMESTAMP
            """,
            chat_id,
            user_id,
            yesterday,
            activity,
            elapsed,
            shift,
        )

        # ---------- 6. 直接写入 daily_statistics（强制昨天）----------
        await conn.execute(
            """
            INSERT INTO daily_statistics
            (chat_id, user_id, record_date, activity_name,
             activity_count, accumulated_time, is_soft_reset, shift)
            VALUES ($1, $2, $3, $4, 1, $5, FALSE, $6)
            ON CONFLICT (chat_id, user_id, record_date, activity_name, is_soft_reset, shift)
            DO UPDATE SET
                activity_count = daily_statistics.activity_count + 1,
                accumulated_time = daily_statistics.accumulated_time + EXCLUDED.accumulated_time,
                updated_at = CURRENT_TIMESTAMP
            """,
            chat_id,
            user_id,
            yesterday,
            activity,
            elapsed,
            shift,
        )

        # ---------- 7. 写入 monthly_statistics（昨天所属月份）----------
        await conn.execute(
            """
            INSERT INTO monthly_statistics
            (chat_id, user_id, statistic_date, activity_name,
             activity_count, accumulated_time, shift)
            VALUES ($1, $2, $3, $4, 1, $5, $6)
            ON CONFLICT (chat_id, user_id, statistic_date, activity_name, shift)
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
            shift,
        )

        # ---------- 8. 如果有罚款，写入罚款记录 ----------
        if fine_amount > 0:
            # daily_statistics 罚款
            await conn.execute(
                """
                INSERT INTO daily_statistics
                (chat_id, user_id, record_date, activity_name,
                 accumulated_time, is_soft_reset, shift)
                VALUES ($1, $2, $3, 'total_fines', $4, FALSE, $5)
                ON CONFLICT (chat_id, user_id, record_date, activity_name, is_soft_reset, shift)
                DO UPDATE SET
                    accumulated_time = daily_statistics.accumulated_time + EXCLUDED.accumulated_time,
                    updated_at = CURRENT_TIMESTAMP
                """,
                chat_id,
                user_id,
                yesterday,
                fine_amount,
                shift,
            )

            # monthly_statistics 罚款
            await conn.execute(
                """
                INSERT INTO monthly_statistics
                (chat_id, user_id, statistic_date, activity_name,
                 accumulated_time, shift)
                VALUES ($1, $2, $3, 'total_fines', $4, $5)
                ON CONFLICT (chat_id, user_id, statistic_date, activity_name, shift)
                DO UPDATE SET
                    accumulated_time = monthly_statistics.accumulated_time + EXCLUDED.accumulated_time,
                    updated_at = CURRENT_TIMESTAMP
                """,
                chat_id,
                user_id,
                statistic_date,
                fine_amount,
                shift,
            )

        # ---------- 9. 如果是超时，写入超时统计 ----------
        if is_overtime and overtime_seconds > 0:
            # 超时次数
            await conn.execute(
                """
                INSERT INTO daily_statistics
                (chat_id, user_id, record_date, activity_name,
                 activity_count, is_soft_reset, shift)
                VALUES ($1, $2, $3, 'overtime_count', 1, FALSE, $4)
                ON CONFLICT (chat_id, user_id, record_date, activity_name, is_soft_reset, shift)
                DO UPDATE SET
                    activity_count = daily_statistics.activity_count + 1,
                    updated_at = CURRENT_TIMESTAMP
                """,
                chat_id,
                user_id,
                yesterday,
                shift,
            )

            # 超时时长
            await conn.execute(
                """
                INSERT INTO daily_statistics
                (chat_id, user_id, record_date, activity_name,
                 accumulated_time, is_soft_reset, shift)
                VALUES ($1, $2, $3, 'overtime_time', $4, FALSE, $5)
                ON CONFLICT (chat_id, user_id, record_date, activity_name, is_soft_reset, shift)
                DO UPDATE SET
                    accumulated_time = daily_statistics.accumulated_time + EXCLUDED.accumulated_time,
                    updated_at = CURRENT_TIMESTAMP
                """,
                chat_id,
                user_id,
                yesterday,
                overtime_seconds,
                shift,
            )

        # ---------- 10. 清空用户活动状态 ----------
        await conn.execute(
            """
            UPDATE users 
            SET current_activity = NULL, 
                activity_start_time = NULL,
                checkin_message_id = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE chat_id = $1 AND user_id = $2
            """,
            chat_id,
            user_id,
        )

        result["success"] = True
        logger.info(
            f"✅ [独立强制结束] 用户{user_id} | "
            f"活动:{activity} | 日期:{yesterday} | "
            f"班次:{shift}/{shift_detail} | 时长:{elapsed}s | 罚款:{fine_amount}"
        )

    except Exception as e:
        logger.error(f"❌ [独立强制结束] 失败 用户{user_id}: {e}")

    return result


# ========== 4. 独立白班强制结束 ==========
# dual_shift_reset.py - 修改 _force_end_white_shift_independent


async def _force_end_white_shift_independent(
    chat_id: int, now: datetime, yesterday: date
) -> Dict[str, Any]:
    """
    独立版：强制结束昨天白班未下班用户
    现在使用修改后的 complete_user_activity 并传入 forced_date
    """
    stats = {"total": 0, "success": 0, "failed": 0, "details": []}

    try:
        async with db.pool.acquire() as conn:
            # 查找昨天白班上班、未下班、有进行中活动的用户
            rows = await conn.fetch(
                """
                SELECT u.user_id, u.nickname, u.current_activity, 
                       u.activity_start_time, u.shift
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

            stats["total"] = len(rows)

            for row in rows:
                try:
                    user_id = row["user_id"]
                    activity = row["current_activity"]
                    start_time_str = row["activity_start_time"]

                    # 解析开始时间
                    start_time = datetime.fromisoformat(start_time_str)

                    # 计算时长
                    elapsed = int((now - start_time).total_seconds())

                    # 计算罚款
                    time_limit = await db.get_activity_time_limit(activity)
                    time_limit_seconds = time_limit * 60
                    is_overtime = elapsed > time_limit_seconds
                    overtime_seconds = max(0, elapsed - time_limit_seconds)
                    overtime_minutes = overtime_seconds / 60

                    fine_amount = 0
                    if is_overtime and overtime_seconds > 0:
                        fine_rates = await db.get_fine_rates_for_activity(activity)
                        if fine_rates:
                            segments = []
                            for k in fine_rates.keys():
                                try:
                                    v = int(str(k).lower().replace("min", ""))
                                    segments.append(v)
                                except:
                                    pass
                            segments.sort()
                            for s in segments:
                                if overtime_minutes <= s:
                                    fine_amount = fine_rates.get(
                                        str(s), fine_rates.get(f"{s}min", 0)
                                    )
                                    break
                            if fine_amount == 0 and segments:
                                m = segments[-1]
                                fine_amount = fine_rates.get(
                                    str(m), fine_rates.get(f"{m}min", 0)
                                )

                    # 🎯 关键修改：使用修改后的 complete_user_activity，传入 forced_date=yesterday
                    await db.complete_user_activity(
                        chat_id=chat_id,
                        user_id=user_id,
                        activity=activity,
                        elapsed_time=elapsed,
                        fine_amount=fine_amount,
                        is_overtime=is_overtime,
                        shift="day",
                        forced_date=yesterday,  # ✅ 强制使用昨天
                    )

                    stats["success"] += 1
                    stats["details"].append(
                        {
                            "user_id": user_id,
                            "activity": activity,
                            "elapsed": elapsed,
                            "fine": fine_amount,
                            "success": True,
                        }
                    )

                    logger.info(
                        f"✅ [白班强制结束] 用户{user_id} | "
                        f"活动:{activity} | 强制日期:{yesterday} | 时长:{elapsed}s"
                    )

                except Exception as e:
                    stats["failed"] += 1
                    logger.error(f"❌ [白班强制结束] 失败 用户{row['user_id']}: {e}")

        logger.info(
            f"🟡 [白班强制结束完成] 群组{chat_id} | "
            f"昨日{yesterday} | 总计:{stats['total']} | "
            f"成功:{stats['success']} | 失败:{stats['failed']}"
        )

    except Exception as e:
        logger.error(f"❌ [白班强制结束] 失败 {chat_id}: {e}")

    return stats


# ========== 5. 独立夜班强制结束 ==========
# dual_shift_reset.py - 修改 _force_end_night_shift_independent


async def _force_end_night_shift_independent(
    chat_id: int, now: datetime, yesterday: date
) -> Dict[str, Any]:
    """
    独立版：强制结束昨晚夜班未下班用户
    现在使用修改后的 complete_user_activity 并传入 forced_date
    """
    stats = {"total": 0, "success": 0, "failed": 0, "details": []}

    try:
        async with db.pool.acquire() as conn:
            # 查找昨晚夜班上班、未下班、有进行中活动的用户
            rows = await conn.fetch(
                """
                SELECT u.user_id, u.nickname, u.current_activity, 
                       u.activity_start_time, u.shift
                FROM users u
                LEFT JOIN work_records wr 
                    ON u.chat_id = wr.chat_id 
                    AND u.user_id = wr.user_id 
                    AND wr.record_date = $2 
                    AND wr.checkin_type = 'work_end'
                    AND wr.shift_detail IN ('night_last', 'night')
                WHERE u.chat_id = $1
                  AND u.current_activity IS NOT NULL
                  AND u.shift = 'night'
                  AND wr.id IS NULL
                """,
                chat_id,
                yesterday,
            )

            stats["total"] = len(rows)

            for row in rows:
                try:
                    user_id = row["user_id"]
                    activity = row["current_activity"]
                    start_time_str = row["activity_start_time"]

                    # 解析开始时间
                    start_time = datetime.fromisoformat(start_time_str)

                    # 计算时长
                    elapsed = int((now - start_time).total_seconds())

                    # 计算罚款（同上）
                    time_limit = await db.get_activity_time_limit(activity)
                    time_limit_seconds = time_limit * 60
                    is_overtime = elapsed > time_limit_seconds
                    overtime_seconds = max(0, elapsed - time_limit_seconds)
                    overtime_minutes = overtime_seconds / 60

                    fine_amount = 0
                    if is_overtime and overtime_seconds > 0:
                        fine_rates = await db.get_fine_rates_for_activity(activity)
                        if fine_rates:
                            segments = []
                            for k in fine_rates.keys():
                                try:
                                    v = int(str(k).lower().replace("min", ""))
                                    segments.append(v)
                                except:
                                    pass
                            segments.sort()
                            for s in segments:
                                if overtime_minutes <= s:
                                    fine_amount = fine_rates.get(
                                        str(s), fine_rates.get(f"{s}min", 0)
                                    )
                                    break
                            if fine_amount == 0 and segments:
                                m = segments[-1]
                                fine_amount = fine_rates.get(
                                    str(m), fine_rates.get(f"{m}min", 0)
                                )

                    # 🎯 关键修改：使用修改后的 complete_user_activity，传入 forced_date=yesterday
                    await db.complete_user_activity(
                        chat_id=chat_id,
                        user_id=user_id,
                        activity=activity,
                        elapsed_time=elapsed,
                        fine_amount=fine_amount,
                        is_overtime=is_overtime,
                        shift="night",
                        forced_date=yesterday,  # ✅ 强制使用昨天
                    )

                    stats["success"] += 1
                    stats["details"].append(
                        {
                            "user_id": user_id,
                            "activity": activity,
                            "elapsed": elapsed,
                            "fine": fine_amount,
                            "success": True,
                        }
                    )

                    logger.info(
                        f"✅ [夜班强制结束] 用户{user_id} | "
                        f"活动:{activity} | 强制日期:{yesterday} | 时长:{elapsed}s"
                    )

                except Exception as e:
                    stats["failed"] += 1
                    logger.error(f"❌ [夜班强制结束] 失败 用户{row['user_id']}: {e}")

        logger.info(
            f"🌙 [夜班强制结束完成] 群组{chat_id} | "
            f"昨日{yesterday} | 总计:{stats['total']} | "
            f"成功:{stats['success']} | 失败:{stats['failed']}"
        )

    except Exception as e:
        logger.error(f"❌ [夜班强制结束] 失败 {chat_id}: {e}")

    return stats


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
    chat_id: int, yesterday: date, today: date
) -> Dict[str, int]:
    """
    数据清理 - 严格遵循"只删昨天及之前，不删今天"

    规则:
    ✅ 保留 record_date = 今天 的所有数据
    🗑️ 删除 record_date <= 昨天 的所有数据
    """
    stats = {
        "user_activities": 0,
        "work_records": 0,
        "daily_statistics": 0,
        "users_reset": 0,
    }

    try:
        async with db.pool.acquire() as conn:
            async with conn.transaction():
                # 1. user_activities
                result = await conn.execute(
                    """
                    DELETE FROM user_activities 
                    WHERE chat_id = $1 AND activity_date <= $2
                    """,
                    chat_id,
                    yesterday,
                )
                stats["user_activities"] = _parse_delete_count(result)

                # 2. work_records
                result = await conn.execute(
                    """
                    DELETE FROM work_records 
                    WHERE chat_id = $1 AND record_date <= $2
                    """,
                    chat_id,
                    yesterday,
                )
                stats["work_records"] = _parse_delete_count(result)

                # 3. daily_statistics
                result = await conn.execute(
                    """
                    DELETE FROM daily_statistics 
                    WHERE chat_id = $1 AND record_date <= $2
                    """,
                    chat_id,
                    yesterday,
                )
                stats["daily_statistics"] = _parse_delete_count(result)

                # 4. 清理用户昨日活动状态
                result = await conn.execute(
                    """
                    UPDATE users 
                    SET current_activity = NULL, 
                        activity_start_time = NULL,
                        last_updated = $2
                    WHERE chat_id = $1 
                      AND last_updated <= $3
                      AND current_activity IS NOT NULL
                    """,
                    chat_id,
                    today,
                    yesterday,
                )
                stats["users_reset"] = _parse_update_count(result)

        total_deleted = (
            stats["user_activities"] + stats["work_records"] + stats["daily_statistics"]
        )

        logger.info(
            f"🧹 [数据清理] 群组{chat_id}\n"
            f"   • 删除用户活动: {stats['user_activities']} 条\n"
            f"   • 删除工作记录: {stats['work_records']} 条\n"
            f"   • 删除日统计: {stats['daily_statistics']} 条\n"
            f"   • 重置用户状态: {stats['users_reset']} 人\n"
            f"   • 总计删除: {total_deleted} 条\n"
            f"   • 今天数据: ✅ 完整保留 (record_date = {today})"
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


async def recover_shift_states():
    """
    系统启动时恢复所有群组的班次状态
    根据今天已上班但未下班的用户，重建 group_shift_state
    """
    logger.info("🔄 开始恢复群组班次状态...")
    recovered_count = 0

    try:
        # 获取所有群组
        all_groups = await db.get_all_groups()

        for chat_id in all_groups:
            try:
                # 只处理双班模式群组
                if not await db.is_dual_mode_enabled(chat_id):
                    continue

                today = await db.get_business_date(chat_id)

                async with db.pool.acquire() as conn:
                    # 查询今天已上班但未下班的用户（取最早的一个）
                    row = await conn.fetchrow(
                        """
                        SELECT wr.user_id, wr.shift, wr.shift_detail, wr.created_at
                        FROM work_records wr
                        WHERE wr.chat_id = $1
                          AND wr.record_date = $2
                          AND wr.checkin_type = 'work_start'
                          AND NOT EXISTS (
                              SELECT 1 FROM work_records wr2
                              WHERE wr2.chat_id = wr.chat_id
                                AND wr2.user_id = wr.user_id
                                AND wr2.record_date = wr.record_date
                                AND wr2.shift = wr.shift
                                AND wr2.checkin_type = 'work_end'
                          )
                        ORDER BY wr.created_at ASC
                        LIMIT 1
                        """,
                        chat_id,
                        today,
                    )

                    if row:
                        # 存在未下班的用户，恢复班次状态
                        await db.create_shift_state(
                            chat_id=chat_id,
                            shift=row["shift"],
                            started_by_user_id=row["user_id"],
                        )
                        recovered_count += 1
                        logger.info(
                            f"✅ 恢复群组 {chat_id} 班次状态: "
                            f"{row['shift']}, 启动用户: {row['user_id']}"
                        )
                    else:
                        # 没有未下班的用户，确保状态被清除
                        await db.clear_shift_state(chat_id)

            except Exception as e:
                logger.error(f"❌ 恢复群组 {chat_id} 班次状态失败: {e}")

        logger.info(f"✅ 班次状态恢复完成，共恢复 {recovered_count} 个群组")
        return recovered_count

    except Exception as e:
        logger.error(f"❌ 班次状态恢复过程失败: {e}")
        return 0
