"""
双班硬重置 - 单文件完整实现（独立版，无外部依赖）
放置在与 main.py、database.py 同级目录

使用规范：
- 不修改原有单班逻辑
- 所有时间动态计算，无硬编码
- 完全复用已有导出函数
- ✅ 独立强制结束，100%归因昨天
- ✅ 统一处理白班+夜班未下班用户
- ✅ 自动补全下班记录
- ✅ 并发优化处理
"""

import logging
import asyncio
import time
import traceback
from datetime import datetime, timedelta, date
from typing import Dict, Optional, Any, List
from performance import global_cache

# 直接导入同级模块
from database import db
from main import export_and_push_csv

logger = logging.getLogger("GroupCheckInBot.DualShiftReset")


# ========== 1. 调度入口（供cmd_setresettime调用） ==========
async def handle_hard_reset(
    chat_id: int, operator_id: Optional[int] = None
) -> Optional[bool]:
    """
    硬重置总调度入口 - 单班/双班分流

    返回值:
        True  - 双班模式执行成功
        False - 双班模式执行失败
        None  - 单班模式，调用方应继续执行原有逻辑
    """
    try:
        # 1. 获取班次配置，判断模式
        shift_config = await db.get_shift_config(chat_id)
        is_dual_mode = shift_config.get("dual_mode", False)

        # 2. 单班模式 - 返回None表示未处理
        if not is_dual_mode:
            logger.info(f"🔄 [单班模式] 群组 {chat_id} 需继续执行原有硬重置逻辑")
            return None

        # 3. 双班模式 - 执行双班硬重置
        logger.info(f"🔄 [双班模式] 群组 {chat_id} 执行双班硬重置")

        try:
            success = await _dual_shift_hard_reset(chat_id, operator_id)

            if success:
                logger.info(f"✅ [双班硬重置] 群组 {chat_id} 执行成功")
            else:
                logger.error(f"❌ [双班硬重置] 群组 {chat_id} 执行失败")

            return success  # 返回实际执行结果

        except Exception as e:
            logger.error(f"❌ [双班硬重置] 群组 {chat_id} 异常: {e}")
            logger.error(traceback.format_exc())
            return False  # 双班模式执行异常

    except Exception as e:
        logger.error(f"❌ 硬重置调度失败 {chat_id}: {e}")
        logger.error(traceback.format_exc())
        return None  # 调度失败，让调用方自行处理


# ========== 2. 双班硬重置核心流程 ==========
async def _dual_shift_hard_reset(
    chat_id: int, operator_id: Optional[int] = None
) -> bool:
    """
    双班硬重置主流程（带幂等性）
    在重置时间+2小时执行
    """
    try:
        now = db.get_beijing_time()
        today = now.date()
        yesterday = today - timedelta(days=1)

        # ==================== 幂等性检查 ====================
        reset_flag_key = f"dual_reset_executed:{chat_id}:{today.strftime('%Y%m%d')}"
        if global_cache.get(reset_flag_key):
            logger.info(f"⏭️ 群组 {chat_id} 今天已完成双班重置，跳过")
            return True

        # 初始化群组数据
        await db.init_group(chat_id)
        group_data = await db.get_group_cached(chat_id)
        if not group_data:
            logger.warning(f"⚠️ [双班硬重置] 群组 {chat_id} 没有配置数据，跳过重置")
            return False

        reset_hour = group_data.get("reset_hour", 0)
        reset_minute = group_data.get("reset_minute", 0)

        # ==================== 🎯 修复：正确计算执行时间 ====================
        # 计算今天的重置时间
        reset_time_today = now.replace(
            hour=reset_hour, minute=reset_minute, second=0, microsecond=0
        )

        # 计算昨天的重置时间
        reset_time_yesterday = reset_time_today - timedelta(days=1)

        # 计算两个执行时间点
        execute_yesterday = reset_time_yesterday + timedelta(hours=2)
        execute_today = reset_time_today + timedelta(hours=2)

        # 判断应该用哪个执行窗口
        # 逻辑：如果当前时间在昨天的执行窗口内，用昨天的重置时间
        if execute_yesterday <= now < execute_today:
            reset_time = reset_time_yesterday
            execute_time = execute_yesterday
            target_date = reset_time_yesterday.date()
            period_info = "昨天"
            logger.debug(
                f"📅 使用昨天重置时间: {reset_time_yesterday.strftime('%Y-%m-%d %H:%M')}"
            )
        else:
            reset_time = reset_time_today
            execute_time = execute_today
            target_date = reset_time_today.date()
            period_info = "今天"
            logger.debug(
                f"📅 使用今天重置时间: {reset_time_today.strftime('%Y-%m-%d %H:%M')}"
            )

        # ==================== 检查是否到达执行时间 ====================
        if now < execute_time:
            minutes_left = int((execute_time - now).total_seconds() / 60)
            seconds_left = int((execute_time - now).total_seconds() % 60)
            logger.info(
                f"⏳ [双班硬重置] 群组 {chat_id} 等待执行\n"
                f"   • 当前时间: {now.strftime('%H:%M:%S')}\n"
                f"   • 执行时间: {execute_time.strftime('%H:%M:%S')} ({period_info})\n"
                f"   • 剩余时间: {minutes_left}分{seconds_left}秒"
            )
            return False  # 还未到执行时间

        logger.info(
            f"🚀 [双班硬重置] 开始执行\n"
            f"   ┌─────────────────────────────────\n"
            f"   ├─ 群组ID: {chat_id}\n"
            f"   ├─ 当前时间: {now.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"   ├─ 重置时间: {reset_time.strftime('%H:%M')} ({period_info})\n"
            f"   ├─ 执行时间: {execute_time.strftime('%H:%M')}\n"
            f"   ├─ 目标日期: {target_date}\n"
            f"   └─ 操作员: {operator_id or '系统'}"
        )

        total_start_time = time.time()

        # ==================== 初始化统计变量 ====================
        force_stats = {
            "total": 0,
            "success": 0,
            "failed": 0,
            "day_shift": {"total": 0, "success": 0, "failed": 0},
            "night_shift": {"total": 0, "success": 0, "failed": 0},
            "details": [],
        }

        complete_stats = {
            "total": 0,
            "success": 0,
            "failed": 0,
            "day_shift": {"total": 0, "success": 0, "failed": 0},
            "night_shift": {"total": 0, "success": 0, "failed": 0},
            "details": [],
        }

        # ==================== 1-2 步骤并发执行 ====================
        logger.info(f"📊 [步骤1-2/5] 并发处理未完成活动及补全下班记录...")
        task1 = asyncio.create_task(
            _force_end_all_unfinished_shifts(chat_id, now, target_date)
        )
        task2 = asyncio.create_task(_complete_missing_work_ends(chat_id, target_date))

        # 等待任务完成并处理结果
        results = await asyncio.gather(task1, task2, return_exceptions=True)

        # 处理 task1 结果
        if not isinstance(results[0], Exception):
            force_stats = results[0]
            logger.info(
                f"✅ 强制结束活动完成: {force_stats['success']}/{force_stats['total']}"
            )
        else:
            logger.error(f"❌ [强制结束活动] 失败: {results[0]}")
            logger.error(traceback.format_exc())

        # 处理 task2 结果
        if not isinstance(results[1], Exception):
            complete_stats = results[1]
            logger.info(
                f"✅ 补全下班记录完成: {complete_stats['success']}/{complete_stats['total']}"
            )
        else:
            logger.error(f"❌ [补全下班记录] 失败: {results[1]}")
            logger.error(traceback.format_exc())

        # ==================== 3. 导出目标日期数据 ====================
        logger.info(f"📊 [步骤3/5] 导出目标日期数据 (白班+夜班)...")
        export_start = time.time()
        try:
            export_success = await _export_yesterday_data_concurrent(
                chat_id, target_date
            )
        except Exception as e:
            logger.error(f"❌ [数据导出] 失败: {e}")
            logger.error(traceback.format_exc())
            export_success = False
        export_time = time.time() - export_start

        # ==================== 4. 清理目标日期数据 ====================
        logger.info(f"📊 [步骤4/5] 清除目标日期数据...")
        cleanup_start = time.time()
        try:
            cleanup_stats = await _cleanup_old_data(chat_id, target_date, today)
        except Exception as e:
            logger.error(f"❌ [数据清理] 失败: {e}")
            logger.error(traceback.format_exc())
            cleanup_stats = {
                "user_activities": 0,
                "work_records": 0,
                "daily_statistics": 0,
                "users_reset": 0,
            }
        cleanup_time = time.time() - cleanup_start

        # ==================== 5. 清除班次状态 ====================
        try:
            await db.clear_shift_state(chat_id)
            logger.info(f"✅ 班次状态已清除")
        except Exception as e:
            logger.error(f"❌ [清除班次状态] 失败: {e}")

        # ==================== 异步通知 ====================
        try:
            asyncio.create_task(
                _send_reset_notification(
                    chat_id,
                    force_stats,
                    complete_stats,
                    export_success,
                    cleanup_stats,
                    now,
                )
            )
        except Exception as e:
            logger.error(f"❌ [发送通知] 失败: {e}")

        # ==================== 设置幂等标记 ====================
        global_cache.set(reset_flag_key, True, ttl=86400)
        logger.info(f"✅ [双班重置] 群组 {chat_id} 执行成功，已设置幂等标记")

        # ==================== 总耗时日志 ====================
        total_time = time.time() - total_start_time
        logger.info(
            f"🎉 [双班硬重置完成] 群组 {chat_id}\n"
            f"   ├─ 目标日期: {target_date}\n"
            f"   ├─ 强制结束: {force_stats['success']}/{force_stats['total']}\n"
            f"   ├─ 补全下班: {complete_stats['success']}/{complete_stats['total']}\n"
            f"   ├─ 导出成功: {export_success}\n"
            f"   ├─ 清理记录: {cleanup_stats.get('user_activities', 0)}条\n"
            f"   └─ 总耗时: {total_time:.2f}秒"
        )

        return True

    except Exception as e:
        logger.error(
            f"❌ [双班硬重置] 失败 {chat_id}\n"
            f"   ├─ 错误类型: {type(e).__name__}\n"
            f"   ├─ 错误信息: {e}\n"
            f"   └─ 堆栈: {traceback.format_exc()}"
        )
        return False


# ========== 3. 统一强制结束所有未完成活动 ==========
async def _force_end_all_unfinished_shifts(
    chat_id: int, now: datetime, yesterday: date
) -> Dict[str, Any]:
    """
    统一强制结束所有进行中的活动（白班+夜班）
    使用并发处理提升效率
    """
    stats = {
        "total": 0,
        "success": 0,
        "failed": 0,
        "day_shift": {"total": 0, "success": 0, "failed": 0},
        "night_shift": {"total": 0, "success": 0, "failed": 0},
        "details": [],
    }

    try:
        async with db.pool.acquire() as conn:
            # 查询所有进行中的活动
            rows = await conn.fetch(
                """
                SELECT user_id, nickname, current_activity, 
                       activity_start_time, shift
                FROM users 
                WHERE chat_id = $1 
                  AND current_activity IS NOT NULL
                """,
                chat_id,
            )

            stats["total"] = len(rows)

            if not rows:
                logger.info(f"📊 群组 {chat_id} 没有进行中的活动")
                return stats

            logger.info(f"📊 发现 {len(rows)} 个进行中的活动，开始并发处理...")

            # ========== 并发处理所有活动 ==========
            tasks = []
            for row in rows:
                task = asyncio.create_task(
                    _force_end_single_activity(conn, chat_id, row, now, yesterday)
                )
                tasks.append(task)

            # 并发执行所有任务
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # ========== 统计结果 ==========
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    stats["failed"] += 1
                    if rows[i]["shift"] == "day":
                        stats["day_shift"]["failed"] += 1
                    else:
                        stats["night_shift"]["failed"] += 1
                    logger.error(f"❌ 处理用户 {rows[i]['user_id']} 失败: {result}")
                    logger.error(traceback.format_exc())  # 添加堆栈
                else:
                    stats["success"] += 1
                    if result["shift"] == "day":
                        stats["day_shift"]["success"] += 1
                    else:
                        stats["night_shift"]["success"] += 1
                    stats["details"].append(result)

            # 按班次统计总数
            stats["day_shift"]["total"] = sum(1 for r in rows if r["shift"] == "day")
            stats["night_shift"]["total"] = sum(
                1 for r in rows if r["shift"] == "night"
            )

        logger.info(
            f"✅ [强制结束活动完成] 群组 {chat_id}\n"
            f"   ├─ 总计: {stats['total']} 人\n"
            f"   ├─ 成功: {stats['success']} 人\n"
            f"   ├─ 失败: {stats['failed']} 人\n"
            f"   ├─ 白班: {stats['day_shift']['success']}/{stats['day_shift']['total']}\n"
            f"   └─ 夜班: {stats['night_shift']['success']}/{stats['night_shift']['total']}"
        )

    except Exception as e:
        logger.error(f"❌ [强制结束活动] 失败 {chat_id}: {e}")
        logger.error(traceback.format_exc())

    return stats


async def _force_end_single_activity(
    conn, chat_id: int, user_row: dict, now: datetime, yesterday: date
) -> Dict[str, Any]:
    """强制结束单个活动"""
    result = {
        "user_id": user_row["user_id"],
        "shift": user_row["shift"],
        "activity": user_row["current_activity"],
        "elapsed": 0,
        "fine": 0,
        "is_overtime": False,
        "success": False,
    }

    try:
        activity = user_row["current_activity"]
        start_time = datetime.fromisoformat(user_row["activity_start_time"])
        elapsed = int((now - start_time).total_seconds())

        # 获取活动限制
        time_limit = await db.get_activity_time_limit(activity)
        time_limit_seconds = time_limit * 60
        is_overtime = elapsed > time_limit_seconds
        overtime_seconds = max(0, elapsed - time_limit_seconds)
        overtime_minutes = overtime_seconds / 60

        # 计算罚款
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

        result["elapsed"] = elapsed
        result["fine"] = fine_amount
        result["is_overtime"] = is_overtime

        # 使用 complete_user_activity 强制归到昨天
        await db.complete_user_activity(
            chat_id=chat_id,
            user_id=user_row["user_id"],
            activity=activity,
            elapsed_time=elapsed,
            fine_amount=fine_amount,
            is_overtime=is_overtime,
            shift=user_row["shift"],
            forced_date=yesterday,
        )

        result["success"] = True

        logger.info(
            f"✅ [强制结束] 用户{user_row['user_id']} | "
            f"活动:{activity} | 班次:{user_row['shift']} | "
            f"日期:{yesterday} | 时长:{elapsed}s | 罚款:{fine_amount}"
        )

    except Exception as e:
        logger.error(f"❌ [强制结束] 用户{user_row['user_id']} 失败: {e}")
        raise

    return result


# ========== 4. 补全未打卡的下班记录 ==========
async def _complete_missing_work_ends(chat_id: int, yesterday: date) -> Dict[str, Any]:
    """
    为昨天有上班记录但没有下班记录的用户补全下班记录
    使用并发处理提升效率
    """
    stats = {
        "total": 0,
        "success": 0,
        "failed": 0,
        "day_shift": {"total": 0, "success": 0, "failed": 0},
        "night_shift": {"total": 0, "success": 0, "failed": 0},
        "details": [],
    }

    try:
        async with db.pool.acquire() as conn:
            # 查询昨天有上班记录但没有下班记录的用户
            rows = await conn.fetch(
                """
                SELECT 
                    wr.user_id,
                    wr.shift,
                    wr.shift_detail,
                    wr.checkin_time as work_start_time,
                    u.nickname
                FROM work_records wr
                JOIN users u ON wr.chat_id = u.chat_id AND wr.user_id = u.user_id
                WHERE wr.chat_id = $1
                  AND wr.record_date = $2
                  AND wr.checkin_type = 'work_start'
                  AND NOT EXISTS(
                      SELECT 1 FROM work_records wr2
                      WHERE wr2.chat_id = wr.chat_id
                        AND wr2.user_id = wr.user_id
                        AND wr2.record_date = wr.record_date
                        AND wr2.shift = wr.shift
                        AND wr2.checkin_type = 'work_end'
                  )
            """,
                chat_id,
                yesterday,
            )

            stats["total"] = len(rows)

            if not rows:
                logger.info(f"📝 群组 {chat_id} 昨日没有未下班的用户")
                return stats

            logger.info(f"📝 发现 {len(rows)} 个昨日未下班的用户，开始补全记录...")

            # ========== 获取群组配置 ==========
            group_data = await db.get_group_cached(chat_id)
            reset_hour = group_data.get("reset_hour", 0)
            reset_minute = group_data.get("reset_minute", 0)
            auto_end_time = f"{reset_hour:02d}:{reset_minute:02d}"

            shift_config = await db.get_shift_config(chat_id)

            # ========== 并发处理所有用户 ==========
            tasks = []
            for row in rows:
                task = asyncio.create_task(
                    _complete_single_work_end(
                        conn, chat_id, row, yesterday, auto_end_time, shift_config
                    )
                )
                tasks.append(task)

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # ========== 统计结果 ==========
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    stats["failed"] += 1
                    if rows[i]["shift"] == "day":
                        stats["day_shift"]["failed"] += 1
                    else:
                        stats["night_shift"]["failed"] += 1
                    logger.error(
                        f"❌ 补全用户 {rows[i]['user_id']} 下班记录失败: {result}"
                    )
                else:
                    stats["success"] += 1
                    if result["shift"] == "day":
                        stats["day_shift"]["success"] += 1
                    else:
                        stats["night_shift"]["success"] += 1
                    stats["details"].append(result)

            # 按班次统计总数
            stats["day_shift"]["total"] = sum(1 for r in rows if r["shift"] == "day")
            stats["night_shift"]["total"] = sum(
                1 for r in rows if r["shift"] == "night"
            )

        logger.info(
            f"✅ [补全下班记录完成] 群组 {chat_id}\n"
            f"   ├─ 总计: {stats['total']} 人\n"
            f"   ├─ 成功: {stats['success']} 人\n"
            f"   ├─ 失败: {stats['failed']} 人\n"
            f"   ├─ 白班: {stats['day_shift']['success']}/{stats['day_shift']['total']}\n"
            f"   └─ 夜班: {stats['night_shift']['success']}/{stats['night_shift']['total']}"
        )

    except Exception as e:
        logger.error(f"❌ [补全下班记录] 失败 {chat_id}: {e}")
        logger.error(traceback.format_exc())

    return stats


async def _complete_single_work_end(
    conn,
    chat_id: int,
    row: dict,
    yesterday: date,
    auto_end_time: str,
    shift_config: dict,
) -> Dict[str, Any]:
    """补单单个用户的下班记录"""
    result = {
        "user_id": row["user_id"],
        "shift": row["shift"],
        "work_start_time": row["work_start_time"],
        "work_end_time": auto_end_time,
        "fine": 0,
        "success": False,
    }

    try:
        # 获取该班次的期望下班时间
        if row["shift"] == "day":
            expected_end_time = shift_config.get("day_end", "18:00")
            work_end_date = yesterday
        else:
            expected_end_time = shift_config.get(
                "day_start", "09:00"
            )  # 夜班下班是第二天早上
            work_end_date = yesterday + timedelta(days=1)

        # 计算时间差
        work_start_time = datetime.strptime(row["work_start_time"], "%H:%M").time()
        work_start_dt = datetime.combine(yesterday, work_start_time)

        expected_end_dt = datetime.combine(
            work_end_date, datetime.strptime(expected_end_time, "%H:%M").time()
        )

        auto_end_dt = datetime.combine(
            work_end_date, datetime.strptime(auto_end_time, "%H:%M").time()
        )

        # 计算时间差（秒）
        time_diff_seconds = int((auto_end_dt - expected_end_dt).total_seconds())
        time_diff_minutes = time_diff_seconds / 60

        # 计算早退罚款
        fine_amount = 0
        if time_diff_seconds < 0:  # 早退
            fine_rates = await db.get_work_fine_rates_for_type("work_end")
            if fine_rates:
                thresholds = sorted([int(k) for k in fine_rates.keys()])
                for threshold in thresholds:
                    if abs(time_diff_minutes) >= threshold:
                        fine_amount = fine_rates[str(threshold)]

        # 计算工作时长（用于统计）
        work_duration = int((auto_end_dt - work_start_dt).total_seconds())

        # 构建状态文本
        if time_diff_seconds < 0:
            status = f"🚨 自动下班（早退 {abs(time_diff_minutes):.1f}分钟）"
        elif time_diff_seconds > 0:
            status = f"✅ 自动下班（加班 {time_diff_minutes:.1f}分钟）"
        else:
            status = "✅ 自动下班（准时）"

        # 创建下班记录
        await db.add_work_record(
            chat_id=chat_id,
            user_id=row["user_id"],
            record_date=yesterday,
            checkin_type="work_end",
            checkin_time=auto_end_time,
            status=status,
            time_diff_minutes=time_diff_minutes,
            fine_amount=fine_amount,
            shift=row["shift"],
            shift_detail=row.get("shift_detail", row["shift"]),
        )

        # 更新用户的工作时长统计（通过 daily_statistics）
        await conn.execute(
            """
            INSERT INTO daily_statistics
            (chat_id, user_id, record_date, activity_name, accumulated_time, is_soft_reset, shift)
            VALUES ($1, $2, $3, 'work_hours', $4, FALSE, $5)
            ON CONFLICT (chat_id, user_id, record_date, activity_name, is_soft_reset, shift)
            DO UPDATE SET
                accumulated_time = daily_statistics.accumulated_time + EXCLUDED.accumulated_time,
                updated_at = CURRENT_TIMESTAMP
        """,
            chat_id,
            row["user_id"],
            yesterday,
            work_duration,
            row["shift"],
        )

        result["fine"] = fine_amount
        result["success"] = True

        logger.info(
            f"✅ [补全下班] 用户{row['user_id']} | "
            f"班次:{row['shift']} | 上班:{row['work_start_time']} | "
            f"自动下班:{auto_end_time} | 罚款:{fine_amount}"
        )

    except Exception as e:
        logger.error(f"❌ [补全下班] 用户{row['user_id']} 失败: {e}")
        raise

    return result


# ========== 5. 导出昨天数据（并发重试版） ==========
async def _export_yesterday_data_concurrent(
    chat_id: int, yesterday: date, from_monthly: bool = False
) -> bool:
    """
    并发导出昨天白班+夜班数据，成功一次就推送，其余成功只记录日志且不生成文件
    """
    source = "月度表" if from_monthly else "日常表"
    already_sent = False
    success_count = 0

    async def task_wrapper(attempt: int) -> bool:
        nonlocal already_sent
        file_name = f"dual_shift_backup_{chat_id}_{yesterday.strftime('%Y%m%d')}.csv"

        # 第一次成功推送后，其余任务不再生成 CSV
        push_file = not already_sent

        try:
            result = await export_and_push_csv(
                chat_id=chat_id,
                target_date=yesterday,
                file_name=file_name,
                is_daily_reset=True,
                from_monthly_table=from_monthly,
                push_file=push_file,  # ✅ 控制是否实际推送/生成文件
            )

            if result:
                if not already_sent:
                    already_sent = True
                    logger.info(
                        f"✅ [数据导出] 群组{chat_id} 第{attempt+1}次尝试成功 (从{source})，已推送"
                    )
                else:
                    logger.info(
                        f"✅ [数据导出] 群组{chat_id} 第{attempt+1}次尝试成功 (从{source})，已跳过生成/推送"
                    )
                return True
            else:
                logger.warning(f"⚠️ [数据导出] 第{attempt+1}次尝试返回 False")
                return False

        except Exception as e:
            logger.warning(f"⚠️ [数据导出] 第{attempt+1}次尝试失败: {e}")
            return False

    # 创建并发任务
    tasks = [asyncio.create_task(task_wrapper(i)) for i in range(3)]

    results = await asyncio.gather(*tasks)

    success_count = sum(1 for r in results if r is True)

    if already_sent:
        logger.info(f"📊 [数据导出] 群组{chat_id} 共 {success_count} 次成功，已推送1次")
        return True
    else:
        logger.error(f"❌ [数据导出] 群组{chat_id} 所有3次尝试均失败")
        return False


# ========== 6. 数据清理 ==========
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
        logger.error(traceback.format_exc())

    return stats


# ========== 7. 发送通知 ==========
async def _send_reset_notification(
    chat_id: int,
    force_stats: Dict[str, Any],
    complete_stats: Dict[str, Any],
    export_success: bool,
    cleanup_stats: Dict[str, int],
    reset_time: datetime,
):
    """发送重置通知"""
    try:
        from main import send_reset_notification

        notification_data = {
            "force_activities": force_stats,
            "complete_records": complete_stats,
            "export": export_success,
            "cleanup": cleanup_stats,
            "reset_time": reset_time.strftime("%Y-%m-%d %H:%M:%S"),
            "day_shift": {
                "forced": force_stats.get("day_shift", {}).get("success", 0),
                "completed": complete_stats.get("day_shift", {}).get("success", 0),
            },
            "night_shift": {
                "forced": force_stats.get("night_shift", {}).get("success", 0),
                "completed": complete_stats.get("night_shift", {}).get("success", 0),
            },
        }

        await send_reset_notification(chat_id, notification_data, reset_time)
        logger.info(f"   ✅ 重置通知已发送")

    except Exception as e:
        logger.warning(f"   ⚠️ 发送重置通知失败: {e}")


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


# ========== 9. 恢复班次状态 ==========
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
