import asyncio
import logging
import sys
import os
import time
import aiofiles
import csv
import json
import re
import gc
import aiohttp
import traceback
from functools import wraps
from datetime import datetime, timedelta, date
from typing import Dict, Optional, List
from contextlib import suppress
from datetime import timedelta
from aiogram.types import BotCommand, BotCommandScopeAllChatAdministrators


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log", encoding="utf-8", mode="a"),
    ],
)
logger = logging.getLogger("GroupCheckInBot")

# 禁用过于详细的日志
logging.getLogger("aiohttp").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)

# 导入配置和模块
from config import Config, beijing_tz
from database import db
from performance import (
    performance_monitor,
    task_manager,
    retry_manager,
    global_cache,
    track_performance,
    with_retry,
    message_deduplicate,
    handle_database_errors,
    handle_telegram_errors,
)
from utils import (
    MessageFormatter,
    user_lock_manager,
    timer_manager,
    performance_optimizer,
    heartbeat_manager,
    notification_service,
    NotificationService,
    get_beijing_time,
    calculate_cross_day_time_diff,
    # is_valid_checkin_time,
    rate_limit,
    send_reset_notification,
    determine_shift_id,
    is_time_in_day_shift,
    determine_activity_shift_id
)

from bot_manager import bot_manager

from aiogram import Bot, Dispatcher, types, BaseMiddleware
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
    FSInputFile,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    Message,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiohttp import web


from io import StringIO


# # 初始化bot
# bot = Bot(token=Config.TOKEN)
# dp = Dispatcher(storage=MemoryStorage())

# 使用新的管理器
# bot = bot_manager.bot
# dp = bot_manager.dispatcher

bot = None
dp = None

# 记录程序启动时间
start_time = time.time()

# 防重入全局表
active_back_processing: Dict[str, bool] = {}


# ========== 日志中间件 ==========
class LoggingMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: types.Message, data):
        if event.text:
            logger.info(
                f"📨 收到消息: chat_id={event.chat.id}, uid={event.from_user.id}, text='{event.text}'"
            )
        return await handler(event, data)


# ========== 上下班打卡辅助函数 ==========
def get_user_lock(chat_id: int, uid: int):
    """获取用户锁的便捷函数"""
    return user_lock_manager.get_lock(chat_id, uid)


async def auto_end_current_activity(
    chat_id: int, uid: int, user_data: dict, now: datetime, message: types.Message
):
    """自动结束当前活动"""
    try:
        act = user_data["current_activity"]
        start_time_dt = datetime.fromisoformat(user_data["activity_start_time"])
        elapsed = int((now - start_time_dt).total_seconds())

        # 完成活动（不计算罚款，因为是自动结束）
        await db.complete_user_activity(chat_id, uid, act, elapsed, 0, False)

        # 取消定时器
        await timer_manager.cancel_timer(f"{chat_id}-{uid}")

        logger.info(f"自动结束活动: {chat_id}-{uid} - {act}")

    except Exception as e:
        logger.error(f"自动结束活动失败 {chat_id}-{uid}: {e}")


# ========== 特殊按钮定义 ==========
SPECIAL_BUTTONS = {
    "👑 管理员面板": "admin_panel",
    "🔙 返回主菜单": "back_to_main",
    "📤 导出数据": "export_data",
    "📊 我的记录": "my_record",
    "🏆 排行榜": "rank",
    "✅ 回座": "back",
    "🟢 上班": "work_start",
    "🔴 下班": "work_end",
}

# 🆕 官方指令映射配置 (英文指令: 中文活动名)
# Telegram 指令只支持小写字母、数字和下划线
ACTIVITY_MAP = {
    "wc_small": "小厕",
    "wc_large": "大厕",
    "smoke": "抽烟",
    "eat": "吃饭",
}


class AdminStates(StatesGroup):
    """管理员状态"""

    waiting_for_channel_id = State()
    waiting_for_group_id = State()


# ========== 工具函数 ==========
async def is_admin(uid: int) -> bool:
    """检查用户是否为管理员"""
    return uid in Config.ADMINS


async def calculate_work_fine(checkin_type: str, late_minutes: float) -> int:
    """根据分钟阈值动态计算上下班罚款金额"""
    work_fine_rates = await db.get_work_fine_rates_for_type(checkin_type)
    if not work_fine_rates:
        return 0

    # 转换键为整数并排序
    thresholds = sorted([int(k) for k in work_fine_rates.keys() if str(k).isdigit()])
    late_minutes_abs = abs(late_minutes)

    applicable_fine = 0
    for threshold in thresholds:
        if late_minutes_abs >= threshold:
            applicable_fine = work_fine_rates[str(threshold)]
        else:
            break

    return applicable_fine


# ========== 通知函数 ==========
async def send_startup_notification():
    """发送启动通知给管理员"""
    try:
        startup_time = get_beijing_time().strftime("%Y-%m-%d %H:%M:%S")
        message = (
            f"🤖 <b>打卡机器人已启动</b>\n"
            f"⏰ 启动时间: <code>{startup_time}</code>\n"
            f"🟢 系统状态: 正常运行\n"
            f"💾 数据库: {'已连接' if await db.health_check() else '连接异常'}\n"
            f"🔧 模式: 自动重连模式"
        )

        for admin_id in Config.ADMINS:
            try:
                success = await bot_manager.send_message_with_retry(
                    admin_id, message, parse_mode="HTML"
                )
                if success:
                    logger.info(f"✅ 启动通知已发送给管理员 {admin_id}")
                else:
                    logger.error(f"❌ 发送启动通知给管理员 {admin_id} 失败")
            except Exception as e:
                logger.error(f"发送启动通知给管理员 {admin_id} 失败: {e}")

    except Exception as e:
        logger.error(f"发送启动通知失败: {e}")


async def send_shutdown_notification():
    """发送关闭通知给管理员"""
    try:
        shutdown_time = get_beijing_time().strftime("%Y-%m-%d %H:%M:%S")
        uptime = time.time() - start_time
        uptime_str = MessageFormatter.format_time(int(uptime))

        message = (
            f"🛑 <b>打卡机器人已关闭</b>\n"
            f"⏰ 关闭时间: <code>{shutdown_time}</code>\n"
            f"⏱️ 运行时长: <code>{uptime_str}</code>\n"
            f"🔴 系统状态: 已停止"
        )

        for admin_id in Config.ADMINS:
            try:
                # 使用带重试的发送
                success = await bot_manager.send_message_with_retry(
                    admin_id, message, parse_mode="HTML"
                )
                if success:
                    logger.info(f"✅ 关闭通知已发送给管理员 {admin_id}")
                else:
                    logger.debug(f"发送关闭通知给管理员 {admin_id} 失败")
            except Exception as e:
                logger.debug(f"发送关闭通知失败: {e}")

    except Exception as e:
        logger.debug(f"准备关闭通知失败: {e}")


# ========== 生成月度报告函数 =========
async def generate_monthly_report(chat_id: int, year: int = None, month: int = None):
    """生成月度报告 - 基于新的月度统计表"""
    if year is None or month is None:
        today = get_beijing_time()
        year = today.year
        month = today.month

    # 🆕 使用新的月度统计方法（基于 monthly_statistics 表）
    monthly_stats = await db.get_monthly_statistics(chat_id, year, month)
    work_stats = await db.get_monthly_work_statistics(chat_id, year, month)
    activity_ranking = await db.get_monthly_activity_ranking(chat_id, year, month)

    if not monthly_stats and not work_stats:
        return None

    chat_title = str(chat_id)
    try:
        chat_info = await bot.get_chat(chat_id)
        chat_title = chat_info.title or chat_title
    except:
        pass

    # 生成报告文本
    report = (
        f"📊 <b>{year}年{month}月打卡统计报告</b>\n"
        f"🏢 群组：<code>{chat_title}</code>\n"
        f"📅 生成时间：<code>{get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
        f"{MessageFormatter.create_dashed_line()}\n"
    )

    # 总体统计
    total_users = len(monthly_stats)
    total_activity_time = sum(stat.get("total_time", 0) for stat in monthly_stats)
    total_activity_count = sum(stat.get("total_count", 0) for stat in monthly_stats)
    total_fines = sum(stat.get("total_fines", 0) for stat in monthly_stats)

    # 🆕 新增：工作天数和工作时长统计
    total_work_days = sum(stat.get("work_days", 0) for stat in monthly_stats)
    total_work_hours = sum(stat.get("work_hours", 0) for stat in monthly_stats)

    report += (
        f"👥 <b>总体统计</b>\n"
        f"• 活跃用户：<code>{total_users}</code> 人\n"
        f"• 总活动时长：<code>{MessageFormatter.format_time(int(total_activity_time))}</code>\n"
        f"• 总活动次数：<code>{total_activity_count}</code> 次\n"
        f"• 总工作天数：<code>{total_work_days}</code> 天\n"
        f"• 总工作时长：<code>{MessageFormatter.format_time(int(total_work_hours))}</code>\n"
        f"• 总罚款金额：<code>{total_fines}</code> 元\n\n"
    )

    # 上下班统计
    total_work_start = sum(stat.get("work_start_count", 0) for stat in work_stats)
    total_work_end = sum(stat.get("work_end_count", 0) for stat in work_stats)
    total_work_fines = sum(
        stat.get("work_start_fines", 0) + stat.get("work_end_fines", 0)
        for stat in work_stats
    )

    if total_work_start > 0 or total_work_end > 0:
        report += (
            f"🕒 <b>上下班统计</b>\n"
            f"• 上班打卡：<code>{total_work_start}</code> 次\n"
            f"• 下班打卡：<code>{total_work_end}</code> 次\n"
            f"• 上下班罚款：<code>{total_work_fines}</code> 元\n\n"
        )

    # 🆕 新增：个人工作统计排行
    if monthly_stats:
        report += f"👤 <b>个人工作统计</b>\n"

        # 按工作时长排行
        work_hours_ranking = sorted(
            [stat for stat in monthly_stats if stat.get("work_hours", 0) > 0],
            key=lambda x: x.get("work_hours", 0),
            reverse=True,
        )[:5]

        for i, stat in enumerate(work_hours_ranking, 1):
            work_hours_str = MessageFormatter.format_time(
                int(stat.get("work_hours", 0))
            )
            work_days = stat.get("work_days", 0)
            nickname = stat.get("nickname", f"用户{stat.get('user_id')}")
            report += (
                f"  <code>{i}.</code> {nickname} - {work_hours_str} ({work_days}天)\n"
            )
        report += "\n"

    # 活动排行榜
    report += f"🏆 <b>月度活动排行榜</b>\n"
    has_activity_data = False

    for activity, ranking in activity_ranking.items():
        if ranking:
            has_activity_data = True
            report += f"📈 <code>{activity}</code>：\n"
            for i, user in enumerate(ranking[:3], 1):
                time_str = MessageFormatter.format_time(int(user.get("total_time", 0)))
                count = user.get("total_count", 0)
                nickname = user.get("nickname", "未知用户")
                report += f"  <code>{i}.</code> {nickname} - {time_str} ({count}次)\n"
            report += "\n"

    if not has_activity_data:
        report += "暂无活动数据\n\n"

    # 🆕 新增：月度总结
    report += f"📈 <b>月度总结</b>\n"

    if total_activity_count > 0:
        avg_activity_time = (
            total_activity_time / total_activity_count
            if total_activity_count > 0
            else 0
        )
        report += f"• 平均每次活动时长：<code>{MessageFormatter.format_time(int(avg_activity_time))}</code>\n"

    if total_work_days > 0:
        avg_work_hours_per_day = (
            total_work_hours / total_work_days if total_work_days > 0 else 0
        )
        report += f"• 平均每日工作时长：<code>{MessageFormatter.format_time(int(avg_work_hours_per_day))}</code>\n"

    if total_users > 0:
        avg_activity_per_user = (
            total_activity_count / total_users if total_users > 0 else 0
        )
        report += f"• 人均活动次数：<code>{avg_activity_per_user:.1f}</code> 次\n"

        avg_work_days_per_user = total_work_days / total_users if total_users > 0 else 0
        report += f"• 人均工作天数：<code>{avg_work_days_per_user:.1f}</code> 天\n"

    # 🆕 新增：数据来源说明
    report += f"\n{MessageFormatter.create_dashed_line()}\n"
    report += f"💡 <i>注：本报告基于月度统计表生成，不受日常重置操作影响</i>"

    return report


# ========== 导出月度数据函数 =========
async def export_monthly_csv(
    chat_id: int,
    year: int = None,
    month: int = None,
    to_admin_if_no_group: bool = True,
    file_name: str = None,
):
    """导出月度数据为 CSV 并推送 - 优化版本"""
    if year is None or month is None:
        today = get_beijing_time()
        year = today.year
        month = today.month

    if not file_name:
        file_name = f"group_{chat_id}_monthly_{year:04d}{month:02d}.csv"

    # 使用优化版导出
    csv_content = await optimized_monthly_export(chat_id, year, month)

    if not csv_content:
        await bot.send_message(chat_id, f"⚠️ {year}年{month}月没有数据需要导出")
        return

    temp_file = f"temp_{file_name}"
    try:
        async with aiofiles.open(temp_file, "w", encoding="utf-8-sig") as f:
            await f.write(csv_content)

        chat_title = str(chat_id)
        try:
            chat_info = await bot.get_chat(chat_id)
            chat_title = chat_info.title or chat_title
        except:
            pass

        caption = (
            f"📊 月度数据导出\n"
            f"🏢 群组：<code>{chat_title}</code>\n"
            f"📅 统计月份：<code>{year}年{month}月</code>\n"
            f"⏰ 导出时间：<code>{get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
            f"{MessageFormatter.create_dashed_line()}\n"
            f"💾 包含每个用户的月度活动统计"
        )

        try:
            csv_input_file = FSInputFile(temp_file, filename=file_name)
            await bot.send_document(
                chat_id, csv_input_file, caption=caption, parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"❌ 发送到当前聊天失败: {e}")

        # ✅ 使用全局实例（推荐）
        await notification_service.send_document(
            chat_id, FSInputFile(temp_file, filename=file_name), caption=caption
        )

        logger.info(f"✅ 月度数据导出并推送完成: {file_name}")

    except Exception as e:
        logger.error(f"❌ 月度导出过程出错: {e}")
        await bot.send_message(chat_id, f"❌ 月度导出失败：{e}")
    finally:
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except:
            pass


# ========== 活动恢复函数 ==========
async def handle_expired_activity(
    chat_id: int, user_id: int, activity: str, start_time: datetime
):
    """处理已过期的活动（用于服务重启后的恢复）"""
    try:
        now = datetime.now(beijing_tz)
        elapsed = int((now - start_time).total_seconds())
        nickname = "用户"

        # 获取用户信息
        user_data = await db.get_user_cached(chat_id, user_id)
        if user_data:
            nickname = user_data.get("nickname", str(user_id))

        # 计算罚款
        time_limit = await db.get_activity_time_limit(activity)
        time_limit_seconds = time_limit * 60
        is_overtime = elapsed > time_limit_seconds
        overtime_seconds = max(0, elapsed - time_limit_seconds)
        overtime_minutes = overtime_seconds / 60

        fine_amount = 0
        if is_overtime and overtime_seconds > 0:
            fine_amount = await calculate_fine(activity, overtime_minutes)

        # 完成活动
        await db.complete_user_activity(
            chat_id, user_id, activity, elapsed, fine_amount, is_overtime
        )

        # 发送恢复通知
        timeout_msg = (
            f"🔄 <b>系统恢复通知</b>\n"
            f"👤 用户：{MessageFormatter.format_user_link(user_id, nickname)}\n"
            f"📝 检测到未结束的活动：<code>{activity}</code>\n"
            f"⏰ 活动开始时间：<code>{start_time.strftime('%m/%d %H:%M:%S')}</code>\n"
            f"⏱️ 活动总时长：<code>{MessageFormatter.format_time(int(elapsed))}</code>\n"
            f"⚠️ 由于服务重启，您的活动已自动结束"
        )

        if fine_amount > 0:
            timeout_msg += f"\n💰 超时罚款：<code>{fine_amount}</code> 元"

        await bot.send_message(chat_id, timeout_msg, parse_mode="HTML")
        logger.info(f"已处理过期活动: {chat_id}-{user_id} - {activity}")

    except Exception as e:
        logger.error(f"处理过期活动失败 {chat_id}-{user_id}: {e}")


async def recover_expired_activities():
    """恢复服务重启前的过期活动 - 添加返回值"""
    try:
        logger.info("🔄 检查并恢复过期活动...")
        all_groups = await db.get_all_groups()
        recovered_count = 0

        for chat_id in all_groups:
            try:
                group_members = await db.get_group_members(chat_id)
                for user_data in group_members:
                    if user_data.get("current_activity") and user_data.get(
                        "activity_start_time"
                    ):
                        activity = user_data["current_activity"]
                        start_time = datetime.fromisoformat(
                            user_data["activity_start_time"]
                        )
                        user_id = user_data["user_id"]

                        # 处理过期活动
                        await handle_expired_activity(
                            chat_id, user_id, activity, start_time
                        )
                        recovered_count += 1

            except Exception as e:
                logger.error(f"恢复群组 {chat_id} 活动失败: {e}")

        if recovered_count > 0:
            logger.info(f"✅ 已恢复 {recovered_count} 个过期活动")
        else:
            logger.info("✅ 没有需要恢复的过期活动")

        return recovered_count  # 添加返回值

    except Exception as e:
        logger.error(f"恢复过期活动失败: {e}")
        return 0


# ========== 每日重置逻辑 =========
async def reset_daily_data_if_needed(chat_id: int, uid: int):
    """业务日期统一版每日重置（完全对齐业务日期体系）"""
    try:
        now = datetime.now(beijing_tz)

        # 🧠 获取业务日期（系统唯一的“今天”）
        business_date = await db.get_business_date(chat_id, now)

        # 获取用户数据
        user_data = await db.get_user_cached(chat_id, uid)
        if not user_data:
            await db.init_user(chat_id, uid, "用户")
            await db.update_user_last_updated(chat_id, uid, business_date)
            return

        last_updated_raw = user_data.get("last_updated")

        # 解析 last_updated
        if isinstance(last_updated_raw, datetime):
            last_updated = last_updated_raw.date()
        elif isinstance(last_updated_raw, str):
            try:
                last_updated = datetime.fromisoformat(
                    last_updated_raw.replace("Z", "+00:00")
                ).date()
            except Exception:
                try:
                    last_updated = datetime.strptime(
                        last_updated_raw, "%Y-%m-%d"
                    ).date()
                except Exception:
                    last_updated = business_date
        else:
            last_updated = business_date

        # 🎯 唯一重置规则：是否跨了业务日期
        if last_updated < business_date:
            logger.info(f"🔄 重置用户数据: {chat_id}-{uid} | 业务日期 {business_date}")
            await db.reset_shift_data(chat_id, uid, business_date)
            await db.update_user_last_updated(chat_id, uid, business_date)

    except Exception as e:
        logger.error(f"重置检查失败 {chat_id}-{uid}: {e}")
        try:
            await db.init_user(chat_id, uid, "用户")
            await db.update_user_last_updated(chat_id, uid, datetime.now().date())
        except Exception as init_error:
            logger.error(f"用户初始化也失败: {init_error}")


async def check_activity_limit(
    chat_id: int, uid: int, act: str, shift_id: int = None
) -> tuple[bool, int, int]:
    """检查活动次数是否达到上限 - 支持按班次"""
    await db.init_group(chat_id)
    await db.init_user(chat_id, uid)

    # 🆕 传入 shift_id
    current_count = await db.get_user_activity_count(chat_id, uid, act, shift_id)
    max_times = await db.get_activity_max_times(act)

    return current_count < max_times, current_count, max_times


async def has_active_activity(chat_id: int, uid: int) -> tuple[bool, Optional[str]]:
    """检查用户是否有活动正在进行"""
    await db.init_group(chat_id)
    await db.init_user(chat_id, uid)
    user_data = await db.get_user_cached(chat_id, uid)
    return user_data["current_activity"] is not None, user_data["current_activity"]


async def can_perform_activities(chat_id: int, uid: int) -> tuple[bool, str]:
    """快速检查是否可以执行活动 - 修复版：使用重置周期"""
    if not await db.has_work_hours_enabled(chat_id):
        return True, ""

    # 🆕 先执行重置检查，确保数据状态正确
    await reset_daily_data_if_needed(chat_id, uid)

    # 使用修复后的 get_today_work_records（现在基于重置周期）
    today_records = await db.get_today_work_records_fixed(chat_id, uid)  # 使用修复版

    if "work_start" not in today_records:
        return False, "❌ 请先打上班卡！"

    if "work_end" in today_records:
        return False, "❌ 已下班，无法进行活动！"

    return True, ""


async def calculate_fine(activity: str, overtime_minutes: float) -> int:
    """计算罚款金额"""
    fine_rates = await db.get_fine_rates_for_activity(activity)
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


# ========== 键盘生成 ==========
async def get_main_keyboard(
    chat_id: int = None, show_admin: bool = False
) -> ReplyKeyboardMarkup:
    """获取主回复键盘"""
    try:
        activity_limits = await db.get_activity_limits_cached()
    except Exception as e:
        logger.error(f"获取活动配置失败: {e}")
        activity_limits = await db.get_activity_limits_cached()

    dynamic_buttons = []
    current_row = []

    for act in activity_limits.keys():
        current_row.append(KeyboardButton(text=act))
        if len(current_row) >= 3:
            dynamic_buttons.append(current_row)
            current_row = []

    # 添加上下班按钮（如果启用）
    if chat_id and await db.has_work_hours_enabled(chat_id):
        current_row.append(KeyboardButton(text="🟢 上班"))
        current_row.append(KeyboardButton(text="🔴 下班"))
        if len(current_row) >= 3:
            dynamic_buttons.append(current_row)
            current_row = []

    if current_row:
        dynamic_buttons.append(current_row)

    fixed_buttons = []
    fixed_buttons.append([KeyboardButton(text="✅ 回座")])

    bottom_buttons = []
    if show_admin:
        bottom_buttons.append(
            [
                KeyboardButton(text="👑 管理员面板"),
                KeyboardButton(text="📊 我的记录"),
                KeyboardButton(text="🏆 排行榜"),
            ]
        )
    else:
        bottom_buttons.append(
            [KeyboardButton(text="📊 我的记录"), KeyboardButton(text="🏆 排行榜")]
        )

    keyboard = dynamic_buttons + fixed_buttons + bottom_buttons

    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder="请选择操作或输入活动名称...",
    )


def get_admin_keyboard() -> ReplyKeyboardMarkup:
    """管理员专用键盘"""
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="👑 管理员面板"),
                KeyboardButton(text="📤 导出数据"),
            ],
            [KeyboardButton(text="🔙 返回主菜单")],
        ],
        resize_keyboard=True,
    )
    logger.debug("生成管理员键盘")
    return keyboard


# ========== 活动定时提醒 ==========
async def activity_timer(chat_id: int, uid: int, act: str, limit: int):
    """最终无遗漏 + 稳健可用版活动定时器
    (引用回复 + 自动降级 + 自动重试 + 每10分钟超时提醒 + 2小时强制回座)
    """
    try:
        # ===== 状态标记 =====
        one_minute_warning_sent = False
        timeout_immediate_sent = False
        timeout_5min_sent = False
        last_reminder_minute = 0
        force_back_sent = False  # 防止重复强制回座

        # ===== 群消息发送封装（引用 + 自动降级 + 自动重试） =====
        async def send_group_message(text: str, kb=None):
            checkin_message_id = await db.get_user_checkin_message_id(chat_id, uid)
            if checkin_message_id:
                try:
                    return await bot.send_message(
                        chat_id=chat_id,
                        text=text,
                        parse_mode="HTML",
                        reply_markup=kb,
                        reply_to_message_id=checkin_message_id,
                    )
                except Exception as e:
                    logger.warning(f"⚠️ 引用发送失败，重试一次: {e}")
                    await asyncio.sleep(1)
                    try:
                        return await bot.send_message(
                            chat_id=chat_id,
                            text=text,
                            parse_mode="HTML",
                            reply_markup=kb,
                            reply_to_message_id=checkin_message_id,
                        )
                    except Exception as e2:
                        logger.warning(f"⚠️ 引用发送重试失败，降级普通发送: {e2}")
            # 没有 message_id 或引用失败则普通发送
            return await bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode="HTML",
                reply_markup=kb,
            )

        # ===== 快速回座按钮 =====
        def build_quick_back_kb():
            return InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="👉 点击✅立即回座 👈",
                            callback_data=f"quick_back:{chat_id}:{uid}",
                        )
                    ]
                ]
            )

        # ===== 强制回座通知封装 =====
        async def push_force_back_notification(nickname, elapsed, fine_amount):
            try:
                chat_title = str(chat_id)
                try:
                    info = await bot.get_chat(chat_id)
                    chat_title = info.title or chat_title
                except:
                    pass

                notification_text = (
                    f"🚨 <b>超时强制回座通知</b>\n"
                    f"🏢 群组：<code>{chat_title}</code>\n"
                    f"{MessageFormatter.create_dashed_line()}\n"
                    f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
                    f"📝 活动：<code>{act}</code>\n"
                    f"⏰ 自动回座时间：<code>{get_beijing_time().strftime('%m/%d %H:%M:%S')}</code>\n"
                    f"⏱️ 总活动时长：<code>{MessageFormatter.format_time(elapsed)}</code>\n"
                    f"⚠️ 系统自动回座原因：超时超过2小时\n"
                    f"💰 本次罚款：<code>{fine_amount}</code> 元"
                )

                if not notification_service.bot_manager and bot_manager:
                    notification_service.bot_manager = bot_manager
                if not notification_service.bot and bot:
                    notification_service.bot = bot

                await notification_service.send_notification(
                    chat_id,
                    notification_text,
                    notification_type="channel",
                )
                logger.info(f"✅ 强制回座通知推送成功: chat={chat_id}, uid={uid}")
                return True
            except Exception as e:
                logger.error(f"❌ 强制回座通知推送失败: {e}")
                return False

        # ===== 主循环 =====
        while True:
            # 🔒 锁内获取用户数据
            user_lock = user_lock_manager.get_lock(chat_id, uid)
            async with user_lock:
                user_data = await db.get_user_cached(chat_id, uid)
                if not user_data or user_data["current_activity"] != act:
                    break

                start_time = datetime.fromisoformat(user_data["activity_start_time"])
                now = datetime.now(beijing_tz)
                elapsed = int((now - start_time).total_seconds())
                remaining = limit * 60 - elapsed
                nickname = user_data.get("nickname", str(uid))

                # ===== 强制回座 2 小时 =====
                break_force = False
                if elapsed >= 120 * 60 and not force_back_sent:
                    force_back_sent = True
                    fine_amount = await calculate_fine(act, 120)
                    await db.complete_user_activity(
                        chat_id, uid, act, elapsed, fine_amount, True
                    )
                    break_force = True

            # ===== 锁外处理 =====
            if break_force:
                msg = (
                    f"🛑 <b>自动安全回座</b>\n"
                    f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
                    f"📝 活动：<code>{act}</code>\n"
                    f"⚠️ 超时超过2小时，系统已自动回座\n"
                    f"💰 本次罚款：<code>{fine_amount}</code> 元"
                )
                await send_group_message(msg)

                # 推送通知（最多3次重试）
                for attempt in range(3):
                    if await push_force_back_notification(
                        nickname, elapsed, fine_amount
                    ):
                        break
                    logger.warning(f"⚠️ 强制回座通知发送失败，重试 {attempt + 1}/3")
                    await asyncio.sleep(2)

                await db.clear_user_checkin_message(chat_id, uid)
                await timer_manager.cancel_timer(f"{chat_id}-{uid}")
                break

            # ===== 即将超时 1 分钟提醒 =====
            if 0 < remaining <= 60 and not one_minute_warning_sent:
                msg = (
                    f"⏳ <b>即将超时警告</b>\n"
                    f"👤 {MessageFormatter.format_user_link(uid, nickname)}\n"
                    f"🕓 本次 {MessageFormatter.format_copyable_text(act)} 还有 <code>1</code> 分钟！\n"
                    f"💡 请及时回座，避免超时罚款"
                )
                await send_group_message(msg, build_quick_back_kb())
                one_minute_warning_sent = True

            # ===== 超时提醒 =====
            if remaining <= 0:
                overtime_minutes = int(-remaining // 60)
                msg = None

                # 0 分钟超时
                if overtime_minutes == 0 and not timeout_immediate_sent:
                    timeout_immediate_sent = True
                    msg = (
                        f"⚠️ <b>超时警告</b>\n"
                        f"👤 {MessageFormatter.format_user_link(uid, nickname)} 已超时！\n"
                        f"🏃‍♂️ 请立即回座，避免产生更多罚款！"
                    )
                    last_reminder_minute = 0

                # 5 分钟超时
                elif overtime_minutes == 5 and not timeout_5min_sent:
                    timeout_5min_sent = True
                    msg = (
                        f"🔔 <b>超时警告</b>\n"
                        f"👤 {MessageFormatter.format_user_link(uid, nickname)} 已超时 <code>5</code> 分钟！\n"
                        f"😤 罚款正在累积，请立即回座！"
                    )
                    last_reminder_minute = 5

                # >=10 分钟，每10分钟提醒一次
                elif (
                    overtime_minutes >= 10
                    and overtime_minutes % 10 == 0
                    and overtime_minutes != last_reminder_minute
                ):
                    last_reminder_minute = overtime_minutes
                    msg = (
                        f"🚨 <b>超时警告</b>\n"
                        f"👤 {MessageFormatter.format_user_link(uid, nickname)} 已超时 <code>{overtime_minutes}</code> 分钟！\n"
                        f"💢 请立刻回座，系统将持续记录超时,避免产生更多罚款！"
                    )

                if msg:
                    await send_group_message(msg, build_quick_back_kb())

            await asyncio.sleep(30)

    except asyncio.CancelledError:
        logger.info(f"定时器 {chat_id}-{uid} 被取消")
    except Exception as e:
        logger.error(f"定时器错误: {e}")
    finally:
        try:
            await db.clear_user_checkin_message(chat_id, uid)
        except:
            pass

# 在main.py中找到定时任务部分，修改为：

async def shift_based_daily_reset():
    """基于班次的每日重置调度器 - 修复版"""
    logger.info("⏰ 班次重置调度器启动...")
    
    while True:
        try:
            now = get_beijing_time()
            current_hour = now.hour
            current_minute = now.minute
            
            # 每分钟检查一次
            all_groups = await db.get_all_groups()
            
            for chat_id in all_groups:
                try:
                    # 获取群组配置
                    group_data = await db.get_group_cached(chat_id)
                    
                    # 🎯 分别获取两个重置时间
                    # 白班重置时间
                    day_reset_hour = group_data.get("reset_hour", 23)
                    day_reset_minute = group_data.get("reset_minute", 0)
                    
                    # 夜班重置时间
                    night_reset_hour = group_data.get("soft_reset_hour", 11)  # 默认11点
                    night_reset_minute = group_data.get("soft_reset_minute", 0)
                    
                    # 🎯 白班重置（23:00 或自定义时间）
                    if current_hour == day_reset_hour and current_minute == day_reset_minute:
                        logger.info(f"⏰ {day_reset_hour:02d}:{day_reset_minute:02d} 执行白班重置: 群组{chat_id}")
                        
                        # 1. 先完成所有未结束的活动
                        await db.complete_all_pending_activities_before_reset(chat_id, now)
                        
                        # 2. 执行白班重置
                        await db.reset_shift_data(chat_id, 0, is_hard_reset=True)
                        
                        # 3. 发送重置通知
                        await send_reset_notification(chat_id, "白班重置完成", now)
                    
                    # 🎯 夜班重置（11:00 或自定义时间）
                    elif current_hour == night_reset_hour and current_minute == night_reset_minute:
                        logger.info(f"⏰ {night_reset_hour:02d}:{night_reset_minute:02d} 执行夜班重置: 群组{chat_id}")
                        
                        # 1. 导出昨天的数据
                        yesterday = now.date() - timedelta(days=1)
                        logger.info(f"💾 准备导出昨日数据: {yesterday}")
                        
                        # 2. 执行夜班重置
                        await db.reset_shift_data(chat_id, 1, is_hard_reset=False)
                        
                        # 3. 安全清理已导出的daily_statistics
                        await db.cleanup_exported_daily_stats(chat_id, now)
                        
                        # 4. 发送重置通知
                        await send_reset_notification(chat_id, "夜班重置完成，数据已清理", now)
                    
                    # 🎯 额外的保护：如果夜班重置时间设为 0:0（禁用）
                    elif night_reset_hour == 0 and night_reset_minute == 0:
                        logger.debug(f"群组{chat_id} 夜班重置已禁用")
                    
                except Exception as e:
                    logger.error(f"群组{chat_id}重置失败: {e}")
                    
            # 每分钟检查一次
            await asyncio.sleep(60)
            
        except Exception as e:
            logger.error(f"班次重置调度器异常: {e}")
            await asyncio.sleep(60)

# ========== 核心打卡功能 ==========
async def start_activity(message: types.Message, act: str):
    """开始活动 - 按班次计算次数（完整整合版）"""
    chat_id = message.chat.id
    uid = message.from_user.id

    user_lock = user_lock_manager.get_lock(chat_id, uid)
    async with user_lock:
        await reset_daily_data_if_needed(chat_id, uid)

        # ===== 1️⃣ 快速检查活动是否存在 =====
        if not await db.activity_exists(act):
            await message.answer(
                f"❌ 活动 '{act}' 不存在",
                reply_to_message_id=message.message_id
            )
            return

        # ===== 2️⃣ 检查用户是否允许操作（封禁等）=====
        can_perform, reason = await can_perform_activities(chat_id, uid)
        if not can_perform:
            await message.answer(reason)
            return

        name = message.from_user.full_name
        now = datetime.now(beijing_tz)

        # ===== 3️⃣ 检查活动人数限制 =====
        user_limit = await db.get_activity_user_limit(act)
        if user_limit > 0:
            current_users = await db.get_current_activity_users(chat_id, act)
            if current_users >= user_limit:
                await message.answer(
                    f"❌ 打卡失败~ 活动 '<code>{act}</code>' 人数已满！\n\n"
                    f"📊 当前状态：\n"
                    f"• 限制人数：<code>{user_limit}</code> 人\n"
                    f"• 当前进行：<code>{current_users}</code> 人\n"
                    f"• 剩余名额：<code>0</code> 人\n\n"
                    f"💡 请等待其他用户回座后再打卡进行此活动",
                    reply_markup=await get_main_keyboard(
                        chat_id=chat_id,
                        show_admin=await is_admin(uid),
                    ),
                    reply_to_message_id=message.message_id,
                    parse_mode="HTML",
                )
                return

        # ===== 4️⃣ 检查是否已有进行中的活动 =====
        has_active, current_act = await has_active_activity(chat_id, uid)
        if has_active:
            await message.answer(
                Config.MESSAGES["has_activity"].format(current_act),
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id,
                    show_admin=await is_admin(uid),
                ),
                reply_to_message_id=message.message_id,
            )
            return

        # ===== 5️⃣ 再次重置每日数据（保持你原逻辑）=====
        await reset_daily_data_if_needed(chat_id, uid)

        # =====================================================
        # 🆕 6️⃣ 【关键新增】确保用户“本次活动所属班次”已确定
        # =====================================================
        user_status = await db.get_user_status(chat_id, uid)

        if not user_status or user_status.get('on_duty_shift') is None:
            try:
                shift_id = await determine_activity_shift_id(chat_id, uid, now, db)
                if now.tzinfo is None:
                    now = beijing_tz.localize(now)
                await db.update_user_shift_status(
                    chat_id,
                    uid,
                    shift_id,
                    checkin_time=now,
                    checkout_time=None
                )
            except Exception as e:
                logger.error(f"确定活动班次失败: {e}")
                shift_id = 0

        else:
            shift_id = user_status.get('on_duty_shift')
    
        # =====================================================
        # 🆕 7️⃣ 按【班次】检查活动次数限制
        # =====================================================
        can_start, current_count, max_times = await check_activity_limit(
            chat_id, uid, act, shift_id
        )

        if not can_start:
            await message.answer(
                f"❌ 本班次活动 '<code>{act}</code>' 次数已达上限 "
                f"({current_count}/{max_times})",
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id,
                    show_admin=await is_admin(uid),
                ),
                reply_to_message_id=message.message_id,
                parse_mode="HTML",
            )
            return

        # ===== 8️⃣ 更新用户活动状态 =====
        await db.update_user_activity(
            chat_id, uid, act, str(now), name
        )

        # ===== 9️⃣ 获取活动时长限制 =====
        time_limit = await db.get_activity_time_limit(act)

        # ===== 🔟 启动计时器 =====
        await timer_manager.start_timer(
            chat_id, uid, act, time_limit
        )

        # ===== 1️⃣1️⃣ 发送活动打卡消息 =====
        sent_message = await message.answer(
            MessageFormatter.format_activity_message(
                uid,
                name,
                act,
                now.strftime("%m/%d %H:%M:%S"),
                current_count + 1,
                max_times,
                time_limit,
            ),
            reply_markup=await get_main_keyboard(
                chat_id=chat_id,
                show_admin=await is_admin(uid),
            ),
            reply_to_message_id=message.message_id,
            parse_mode="HTML",
        )

        # ===== 1️⃣2️⃣ 保存打卡消息 ID =====
        await db.update_user_checkin_message(
            chat_id, uid, sent_message.message_id
        )

        logger.info(
            f"📝 用户 {uid} 开始活动 {act}（班次 {shift_id}），消息ID: {sent_message.message_id}"
        )

        # ==================== ✨ 吃饭推送（原逻辑完整保留）✨ ====================
        if act == "吃饭":
            try:
                chat_title = str(chat_id)
                try:
                    chat_info = await bot.get_chat(chat_id)
                    chat_title = chat_info.title or chat_title
                except Exception:
                    pass

                eat_notification_text = (
                    f"🍽️ <b>吃饭通知</b>\n"
                    f" {MessageFormatter.format_user_link(uid, name)} 去吃饭了\n"
                    f"⏰ 吃饭时间：<code>{now.strftime('%H:%M:%S')}</code>\n"
                )

                asyncio.create_task(
                    notification_service.send_notification(
                        chat_id, eat_notification_text
                    )
                )

                logger.info(f"🍽️ 已触发用户 {uid} 的吃饭推送")

            except Exception as e:
                logger.error(f"❌ 吃饭推送失败: {e}")


# ========== 回座功能 ==========
async def process_back(message: types.Message):
    """回座打卡"""
    chat_id = message.chat.id
    uid = message.from_user.id

    user_lock = user_lock_manager.get_lock(chat_id, uid)
    async with user_lock:
        await _process_back_locked(message, chat_id, uid)


async def _process_back_locked(message: types.Message, chat_id: int, uid: int):
    """线程安全的回座逻辑 - 带引用回复优先 + 调试日志"""
    start_time = time.time()
    key = f"{chat_id}:{uid}"

    # 防重入检测
    if active_back_processing.get(key):
        await message.answer(
            "⚠️ 您的回座请求正在处理中，请稍候。", reply_to_message_id=message.message_id
        )
        return
    active_back_processing[key] = True

    try:
        now = datetime.now(beijing_tz)

        # 获取用户数据
        user_data = await db.get_user_cached(chat_id, uid)
        logger.debug(f"🔍 用户数据: {user_data}")

        if not user_data or not user_data.get("current_activity"):
            await message.answer(
                Config.MESSAGES["no_activity"],
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(uid)
                ),
                reply_to_message_id=message.message_id,
            )
            return

        act = user_data["current_activity"]
        activity_start_time_str = user_data["activity_start_time"]
        nickname = user_data.get("nickname", "未知用户")

        # 获取打卡消息ID
        checkin_message_id = await db.get_user_checkin_message_id(chat_id, uid)
        logger.info(f"📝 回座: 用户 {uid}，原打卡消息ID: {checkin_message_id}")

        # fallback 从缓存/数据库字段获取
        if not checkin_message_id and user_data.get("checkin_message_id"):
            checkin_message_id = user_data.get("checkin_message_id")
            logger.debug(f"📝 从user_data获取消息ID: {checkin_message_id}")

        if not checkin_message_id:
            logger.warning(f"⚠️ 用户 {uid} 没有找到打卡消息ID")

        # 解析活动开始时间
        start_time_dt = None
        try:
            if activity_start_time_str:
                clean_str = str(activity_start_time_str).strip()
                if clean_str.endswith("Z"):
                    clean_str = clean_str.replace("Z", "+00:00")
                try:
                    start_time_dt = datetime.fromisoformat(clean_str)
                    if start_time_dt.tzinfo is None:
                        start_time_dt = beijing_tz.localize(start_time_dt)
                except ValueError:
                    formats = [
                        "%Y-%m-%d %H:%M:%S.%f",
                        "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%d %H:%M",
                        "%m/%d %H:%M:%S",
                        "%m/%d %H:%M",
                    ]
                    for fmt in formats:
                        try:
                            start_time_dt = datetime.strptime(clean_str, fmt)
                            if fmt.startswith("%m/%d"):
                                start_time_dt = start_time_dt.replace(year=now.year)
                            break
                        except ValueError:
                            continue
                    if start_time_dt and start_time_dt.tzinfo is None:
                        start_time_dt = beijing_tz.localize(start_time_dt)
        except Exception as e:
            logger.error(f"解析开始时间失败: {activity_start_time_str}, 错误: {e}")

        if not start_time_dt:
            logger.warning("时间解析失败，使用当前时间作为备用")
            start_time_dt = now

        # 计算经过时间
        elapsed = (now - start_time_dt).total_seconds()

        # 并行获取时间限制
        time_limit_task = asyncio.create_task(db.get_activity_time_limit(act))
        time_limit_minutes = await time_limit_task
        time_limit_seconds = time_limit_minutes * 60

        is_overtime = elapsed > time_limit_seconds
        overtime_seconds = max(0, int(elapsed - time_limit_seconds))
        overtime_minutes = overtime_seconds / 60

        fine_amount = 0
        if is_overtime and overtime_seconds > 0:
            fine_amount = await calculate_fine(act, overtime_minutes)

        # 准备消息数据
        elapsed_time_str = MessageFormatter.format_time(int(elapsed))
        time_str = now.strftime("%m/%d %H:%M:%S")
        activity_start_time_for_notification = activity_start_time_str

        # 完成活动
        await db.complete_user_activity(
            chat_id, uid, act, int(elapsed), fine_amount, is_overtime
        )

        # 取消计时器
        await timer_manager.cancel_timer(f"{chat_id}-{uid}")

        # 🆕 先获取用户状态以确定班次ID
        user_status_task = asyncio.create_task(db.get_user_status(chat_id, uid))
        user_status = await user_status_task
        
        # 🆕 确定班次ID (默认0=白班)
        shift_id = 0
        if user_status and user_status.get('on_duty_shift') is not None:
            shift_id = user_status['on_duty_shift']
        else:
            # 如果用户没有班次状态，根据当前时间判断
            try:
                shift_id = await determine_activity_shift_id(chat_id, uid, now, db)
            except Exception as e:
                logger.error(f"自动判定班次失败，使用默认白班: {e}")
                shift_id = 0

        # 获取最新数据和按班次的活动数据
        user_data_task = asyncio.create_task(db.get_user_cached(chat_id, uid))
        user_activities_task = asyncio.create_task(
            db.get_user_activities_by_shift(chat_id, uid, shift_id)
        )
        user_data = await user_data_task
        user_activities = await user_activities_task

        activity_counts = {
            a: info.get("count", 0) for a, info in user_activities.items()
        }

        # 构建回座消息
        back_message = MessageFormatter.format_back_message(
            user_id=uid,
            user_name=user_data.get("nickname", nickname),
            activity=act,
            time_str=time_str,
            elapsed_time=elapsed_time_str,
            total_activity_time=MessageFormatter.format_time(
                int(user_activities.get(act, {}).get("time", 0))
            ),
            total_time=MessageFormatter.format_time(
                int(user_data.get("total_accumulated_time", 0))
            ),
            activity_counts=activity_counts,
            total_count=user_data.get("total_activity_count", 0),
            is_overtime=is_overtime,
            overtime_seconds=overtime_seconds,
            fine_amount=fine_amount,
        )

        # 优先尝试引用回复
        send_success = False
        if checkin_message_id:
            try:
                await message.answer(
                    back_message,
                    reply_to_message_id=checkin_message_id,
                    reply_markup=await get_main_keyboard(
                        chat_id=chat_id, show_admin=await is_admin(uid)
                    ),
                    parse_mode="HTML",
                )
                send_success = True
                logger.info(f"✅ 成功引用回复到消息 {checkin_message_id}")
            except Exception as e:
                error_msg = str(e).lower()
                if any(
                    k in error_msg
                    for k in [
                        "message to reply not found",
                        "message can't be replied",
                        "message not found",
                        "bad request: replied message not found",
                    ]
                ):
                    logger.warning(
                        f"⚠️ 打卡消息 {checkin_message_id} 不可用，降级普通回复"
                    )
                else:
                    logger.error(f"❌ 引用回复未知错误: {e}")
                    raise

        if not send_success:
            await message.answer(
                back_message,
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(uid)
                ),
                parse_mode="HTML",
                reply_to_message_id=message.message_id,  # fallback引用用户最新消息
            )
            logger.info(f"ℹ️ 降级发送回座消息，没有引用打卡消息")

        # 异步发送超时通知
        if is_overtime and fine_amount > 0:
            notification_user_data = user_data.copy() if user_data else {}
            notification_user_data["activity_start_time"] = (
                activity_start_time_for_notification
            )
            notification_user_data["nickname"] = nickname
            asyncio.create_task(
                send_overtime_notification_async(
                    chat_id, uid, notification_user_data, act, fine_amount, now
                )
            )

        # ==================== ✨ 吃饭回座推送 (优化版) ✨ ====================
        if act == "吃饭":
            try:
                # 1. 获取群名
                chat_title = str(chat_id)
                try:
                    chat_info = await message.bot.get_chat(chat_id)
                    chat_title = chat_info.title or chat_title
                except Exception:
                    pass

                # 2. 构建推送文案
                # 使用已经计算好的 elapsed_time_str (例如 "15分30秒")
                eat_end_notification_text = (
                    f"🍽️ <b>吃饭结束通知</b>\n"
                    f"{MessageFormatter.format_user_link(uid, user_data.get('nickname', '用户'))} 回来了\n"
                    f"⏱️ 吃饭耗时：<code>{elapsed_time_str}</code>\n"
                )

                # 3. 异步发送
                asyncio.create_task(
                    notification_service.send_notification(
                        chat_id, eat_end_notification_text
                    )
                )
                logger.info(f"🍽️ 已触发用户 {uid} 的吃饭回座推送")

            except Exception as e:
                logger.error(f"❌ 吃饭回座推送失败: {e}")

    except Exception as e:
        logger.error(f"回座处理异常: {e}")
        await message.answer(
            "❌ 回座失败，请稍后重试。", reply_to_message_id=message.message_id
        )

    finally:
        # finally 清理打卡消息ID
        try:
            await db.clear_user_checkin_message(chat_id, uid)
            logger.info(f"🧹 finally 兜底清理用户 {uid} 的打卡消息ID")
        except Exception as e:
            logger.warning(f"⚠️ finally 兜底清理失败 chat_id={chat_id}, uid={uid}: {e}")

        # 释放锁
        active_back_processing.pop(key, None)
        duration = round(time.time() - start_time, 2)
        logger.info(f"回座结束 chat_id={chat_id}, uid={uid}，耗时 {duration}s")

# 🎯 【新增】异步发送超时通知函数
async def send_overtime_notification_async(
    chat_id: int, uid: int, user_data: dict, act: str, fine_amount: int, now: datetime
):
    """异步发送超时通知 - 优化版本"""
    try:
        chat_title = str(chat_id)
        try:
            chat_info = await bot.get_chat(chat_id)
            chat_title = chat_info.title or chat_title
        except Exception:
            pass

        # 🎯 直接从传入的 user_data 获取开始时间
        activity_start_time = user_data.get("activity_start_time")
        nickname = user_data.get("nickname", "未知用户")

        overtime_str = "未知时长"

        if activity_start_time:
            try:
                # 内联时间解析
                start_time = None
                clean_str = str(activity_start_time).strip()

                if clean_str.endswith("Z"):
                    clean_str = clean_str.replace("Z", "+00:00")

                # 尝试ISO格式
                try:
                    start_time = datetime.fromisoformat(clean_str)
                    if start_time.tzinfo is None:
                        start_time = beijing_tz.localize(start_time)
                except ValueError:
                    # 尝试常见格式
                    formats = [
                        "%Y-%m-%d %H:%M:%S.%f",
                        "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%d %H:%M",
                        "%m/%d %H:%M:%S",
                        "%m/%d %H:%M",
                    ]

                    for fmt in formats:
                        try:
                            start_time = datetime.strptime(clean_str, fmt)
                            if fmt.startswith("%m/%d"):
                                start_time = start_time.replace(year=now.year)
                            if start_time.tzinfo is None:
                                start_time = beijing_tz.localize(start_time)
                            break
                        except ValueError:
                            continue

                if start_time:
                    # 获取活动时间限制
                    time_limit_minutes = await db.get_activity_time_limit(act)
                    time_limit_seconds = time_limit_minutes * 60

                    # 计算总时长
                    total_elapsed = int((now - start_time).total_seconds())

                    # 计算超时时长
                    if total_elapsed > time_limit_seconds:
                        overtime_seconds = total_elapsed - time_limit_seconds
                        overtime_str = MessageFormatter.format_time(overtime_seconds)
                        logger.info(
                            f"✅ 超时计算: {overtime_seconds}秒 ({overtime_str})"
                        )
                    else:
                        overtime_str = "未超时"
                else:
                    overtime_str = "时间解析失败"

            except Exception as e:
                logger.error(f"时间计算失败: {e}")
                overtime_str = "计算失败"
        else:
            logger.warning(f"开始时间为空")
            overtime_str = "开始时间缺失"

        # 格式化通知消息
        notif_text = (
            f"🚨 <b>超时回座通知</b>\n"
            f"🏢 群组：<code>{chat_title}</code>\n"
            f"{MessageFormatter.create_dashed_line()}\n"
            f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
            f"📝 活动：<code>{act}</code>\n"
            f"⏰ 回座时间：<code>{now.strftime('%m/%d %H:%M:%S')}</code>\n"
            f"⏱️ 超时时长：<code>{overtime_str}</code>\n"
            f"💰 罚款金额：<code>{fine_amount}</code> 元"
        )

        # 添加调试信息（管理员可见）
        # if await is_admin(uid):
        #     notif_text += f"\n\n🔍 调试信息：\n开始时间：{activity_start_time}"

        await notification_service.send_notification(chat_id, notif_text)
        logger.info(f"✅ 超时通知发送成功: {chat_id} - 用户{uid} - {act}")

    except Exception as e:
        logger.error(f"❌ 超时通知推送异常: {e}")

        logger.error(f"完整堆栈：{traceback.format_exc()}")


# ========== 上下班打卡功能 ==========
async def process_work_checkin(message: types.Message, checkin_type: str):
    """智能化上下班打卡系统（跨天安全 + 业务日期统一版 + 动态领土班次）"""

    chat_id = message.chat.id
    uid = message.from_user.id
    name = message.from_user.full_name

    if not await db.has_work_hours_enabled(chat_id):
        await message.answer(
            "❌ 本群组尚未启用上下班打卡功能\n\n"
            "👑 请联系管理员使用命令：\n"
            "<code>/setworktime 09:00 18:00</code>\n"
            "设置上下班时间后即可使用",
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(uid)
            ),
            reply_to_message_id=message.message_id,
            parse_mode="HTML",
        )
        logger.info(f"❌ 群组 {chat_id} 未启用上下班功能，用户 {uid} 尝试打卡")
        return

    now = datetime.now(beijing_tz)
    current_time = now.strftime("%H:%M")

    # 🧠 使用业务日期代替自然日
    business_date = await db.get_business_date(chat_id, now)

    trace_id = f"{chat_id}-{uid}-{int(time.time())}"
    logger.info(f"🟢[{trace_id}] 开始处理 {checkin_type} 打卡请求：{name}({uid})")

    user_lock = get_user_lock(chat_id, uid)
    async with user_lock:

        # ✅ 并行预计算
        work_hours_task = asyncio.create_task(db.get_group_work_time(chat_id))
        shift_check_task = asyncio.create_task(
            determine_shift_id(chat_id, uid, checkin_type, now, db)
        )

        # ✅ 初始化群组与用户数据
        try:
            await db.init_group(chat_id)
            await db.init_user(chat_id, uid)
            await reset_daily_data_if_needed(chat_id, uid)
            user_data = await db.get_user_cached(chat_id, uid)
        except Exception as e:
            logger.error(f"[{trace_id}] ❌ 初始化用户/群组失败: {e}")
            await message.answer(
                "⚠️ 数据初始化失败，请稍后再试。", reply_to_message_id=message.message_id
            )
            return

        # ✅ 获取群组配置判断是否双班模式
        group_config = await db.get_group_shift_config(chat_id)
        is_dual_mode = group_config.get('dual_mode', False)

        # =====================================
        # 🎯 动态领土班次判定（必须先执行）
        # =====================================
        valid_time, expected_dt, shift_name, shift_id, error_msg = await shift_check_task

        if not valid_time:
            # 🎯 原版时间窗口提示 - 保留并改进
            # 根据班次决定时间窗口
            if shift_name in ["白班☀️", "夜班🌙"]:
                hours_before = 2
                hours_after = 6
            else:
                hours_before = 7
                hours_after = 7

            allowed_start = (expected_dt - timedelta(hours=hours_before)).strftime(
                "%Y-%m-%d %H:%M"
            )
            allowed_end = (expected_dt + timedelta(hours=hours_after)).strftime(
                "%Y-%m-%d %H:%M"
            )

            await message.answer(
                f"⏰ 当前时间不在允许的打卡范围内！\n\n"
                f"📅 班次：{shift_name}\n"
                f"🕒 期望打卡时间：<code>{expected_dt.strftime('%H:%M')}</code>\n"
                f"📋 允许范围（含日期）：\n"
                f"   • 开始：<code>{allowed_start}</code>\n"
                f"   • 结束：<code>{allowed_end}</code>\n\n"
                f"💡 打卡规则：\n"
                f"• 允许时间：期望时间前后 {hours_before}/{hours_after} 小时\n"
                f"• 当前时间：<code>{now.strftime('%H:%M')}</code>",
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(uid)
                ),
                reply_to_message_id=message.message_id,
                parse_mode="HTML",
            )
            logger.info(
                f"[{trace_id}] ⏰ 打卡时间范围检查失败（不在 ±{hours_before}/{hours_after} 小时内），终止处理"
            )
            return

        logger.info(
            f"[{trace_id}] 班次判定结果:\n"
            f"   当前时间: {now.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"   打卡类型: {checkin_type}\n"
            f"   班次名称: {shift_name}\n"
            f"   班次ID: {shift_id}\n"
            f"   期望时间: {expected_dt.strftime('%H:%M')}\n"
            f"   群组配置: {group_config}"
        )

        # =====================================
        # ✅ 检查是否重复打卡（基于业务日期和班次模式）
        # =====================================
        try:
            has_record_today = await db.has_work_record_today(
                chat_id, uid, checkin_type
            )
        except Exception as e:
            logger.error(f"[{trace_id}] ❌ 检查重复打卡失败: {e}")
            has_record_today = False

        if has_record_today and not is_dual_mode:
            # 🎯 原版单班模式重复打卡提示 - 保留
            today_records = await db.get_today_work_records_fixed(chat_id, uid)
            existing_record = today_records.get(checkin_type)
            action_text = "上班" if checkin_type == "work_start" else "下班"
            status_msg = f"🚫 您今天已经打过{action_text}卡了！"

            if existing_record:
                existing_time = existing_record["checkin_time"]
                existing_status = existing_record["status"]
                status_msg += f"\n⏰ 打卡时间：<code>{existing_time}</code>"
                status_msg += f"\n📊 状态：{existing_status}"

            await message.answer(
                status_msg,
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(uid)
                ),
                reply_to_message_id=message.message_id,
                parse_mode="HTML",
            )
            logger.info(f"[{trace_id}] 🔁 单班模式检测到重复{action_text}打卡，终止处理。")
            return
        elif has_record_today and is_dual_mode:
            # 🎯 原版双班模式重复打卡日志 - 保留
            logger.info(f"[{trace_id}] 🔁 双班模式检测到重复{checkin_type}打卡，但允许继续。")

        # =====================================
        # 🎯 原版上班打卡异常检查 - 保留并改进
        # =====================================
        user_status = await db.get_user_status(chat_id, uid)
        current_shift = user_status.get('on_duty_shift') if user_status else None
        
        if checkin_type == "work_start":
            # 🎯 检查是否已经下班但又打上班卡（仅单班模式限制）
            if not is_dual_mode:
                has_work_end_today = await db.has_work_record_today(
                    chat_id, uid, "work_end"
                )
                if has_work_end_today:
                    today_records = await db.get_today_work_records_fixed(chat_id, uid)
                    end_record = today_records.get("work_end")
                    end_time = end_record["checkin_time"] if end_record else "未知时间"

                    await message.answer(
                        f"🚫 您今天已经在 <code>{end_time}</code> 打过下班卡，无法再打上班卡！\n"
                        f"💡 如需重新打卡，请联系管理员或等待次日自动重置",
                        reply_markup=await get_main_keyboard(
                            chat_id=chat_id, show_admin=await is_admin(uid)
                        ),
                        reply_to_message_id=message.message_id,
                        parse_mode="HTML",
                    )
                    logger.info(f"[{trace_id}] 🔁 单班模式检测到异常：下班后再次上班打卡")
                    return
            elif is_dual_mode:
                # 🎯 双班模式：允许下班后重新打上班卡 - 原版逻辑
                has_work_end_today = await db.has_work_record_today(
                    chat_id, uid, "work_end"
                )
                if has_work_end_today:
                    logger.info(f"[{trace_id}] 🔁 双班模式：用户已下班，允许重新打上班卡")
            
            # 🎯 双班模式上班跨班次切换提示 - 新增
            if is_dual_mode and current_shift is not None and current_shift != shift_id:
                current_shift_name = "白班☀️" if current_shift == 0 else "夜班🌙"
                new_shift_name = shift_name
                
                await message.answer(
                    f"🔄 <b>班次切换提示</b>\n\n"
                    f"📊 当前状态：您在{current_shift_name}在岗\n"
                    f"🎯 尝试操作：开始{new_shift_name}\n\n"
                    f"💡 是否确认切换到{new_shift_name}？\n"
                    f"• 当前班次的活动将自动结束\n"
                    f"• 罚款将按原班次规则计算\n"
                    f"• 如需切换，请先使用'✅ 回座'结束当前活动，然后重新打卡",
                    reply_markup=await get_main_keyboard(
                        chat_id=chat_id, show_admin=await is_admin(uid)
                    ),
                    reply_to_message_id=message.message_id,
                    parse_mode="HTML",
                )
                logger.info(f"[{trace_id}] 🔄 双班模式检测到跨班次切换：{current_shift_name} → {new_shift_name}")
                return
                
        elif checkin_type == "work_end":
            # ✅ 下班前检查上班记录（双班模式也检查，确保有上班记录）
            has_work_start_today = await db.has_work_record_today(
                chat_id, uid, "work_start"
            )
            if not has_work_start_today:
                await message.answer(
                    "❌ 您今天还没有打上班卡，无法打下班卡！\n"
                    "💡 请先使用'🟢 上班'按钮或 /workstart 命令打上班卡",
                    reply_markup=await get_main_keyboard(
                        chat_id=chat_id, show_admin=await is_admin(uid)
                    ),
                    reply_to_message_id=message.message_id,
                    parse_mode="HTML",
                )
                logger.warning(f"[{trace_id}] ⚠️ 用户试图下班打卡但未上班")
                return

        # =====================================
        # 🟢 更新用户班次状态
        # =====================================
        if checkin_type == "work_start":
            if now.tzinfo is None:
                now = beijing_tz.localize(now)
            await db.update_user_shift_status(
                chat_id, uid, shift_id, checkin_time=now
            )
            logger.info(f"[{trace_id}] 用户{uid}开始班次{shift_id} ({shift_name})")
        else:
            if now.tzinfo is None:
                now = beijing_tz.localize(now)
            await db.update_user_shift_status(
                chat_id, uid, None, checkout_time=now
            )
            logger.info(f"[{trace_id}] 用户{uid}结束班次{shift_id} ({shift_name})")

        # =====================================
        # 🕒 迟到/早退计算（使用动态领土的期望时间）
        # =====================================
        # 🆕 使用动态领土判定的期望时间
        expected_dt_adjusted = expected_dt.replace(
            year=now.year, month=now.month, day=now.day
        )
        
        # 如果期望时间比当前时间早很多，可能是前一天的期望时间
        if expected_dt_adjusted.hour < now.hour - 12:
            expected_dt_adjusted += timedelta(days=1)
        
        # 计算时间差（秒）
        time_diff_seconds = (now - expected_dt_adjusted).total_seconds()
        time_diff_minutes = time_diff_seconds / 60

        # ✅ 自动结束活动（仅下班）
        current_activity = user_data.get("current_activity")
        activity_auto_ended = False
        if checkin_type == "work_end" and current_activity:
            with suppress(Exception):
                await auto_end_current_activity(chat_id, uid, user_data, now, message)
                activity_auto_ended = True
                logger.info(f"[{trace_id}] 🔄 已自动结束活动：{current_activity}")

        # =====================================
        # 🎯 迟到 / 早退展示修复区
        # =====================================
        fine_amount = 0
        is_late_early = False

        if checkin_type == "work_start":
            if time_diff_seconds > 0:
                fine_amount = await calculate_work_fine("work_start", time_diff_minutes)
                duration = MessageFormatter.format_duration(time_diff_seconds)
                status = f"🚨 迟到 {duration}"
                if fine_amount:
                    status += f"（💰罚款 {fine_amount}元）"
                emoji = "😅"
                is_late_early = True
            else:
                status = "✅ 准时"
                emoji = "👍"
            action_text = "上班"
        else:
            # 注意：下班时 time_diff_seconds < 0 表示早退
            if time_diff_seconds < 0:
                fine_amount = await calculate_work_fine("work_end", abs(time_diff_minutes))
                duration = MessageFormatter.format_duration(abs(time_diff_seconds))
                status = f"🚨 早退 {duration}"
                if fine_amount:
                    status += f"（💰罚款 {fine_amount}元）"
                emoji = "🏃"
                is_late_early = True
            else:
                status = "✅ 准时"
                emoji = "👍"
            action_text = "下班"

        # =====================================
        # 💾 安全写入数据库（业务日期 + 班次信息）
        # =====================================
        for attempt in range(2):
            try:
                # 🆕 修改：传入班次ID
                await db.add_work_record(
                    chat_id=chat_id,
                    user_id=uid,
                    record_date=business_date,
                    checkin_type=checkin_type,
                    checkin_time=current_time,
                    status=status,
                    time_diff_minutes=time_diff_minutes,
                    fine_amount=fine_amount,
                    shift_id=shift_id  # 🆕 添加班次信息
                )
                logger.info(f"[{trace_id}] ✅ 打卡记录已保存：班次{shift_id}，状态{status}")
                break
            except Exception as e:
                logger.error(f"[{trace_id}] ❌ 数据写入失败，第{attempt+1}次尝试: {e}")
                if attempt == 1:
                    await message.answer(
                        "⚠️ 数据保存失败，请稍后再试。",
                        reply_markup=await get_main_keyboard(
                            chat_id=chat_id, show_admin=await is_admin(uid)
                        ),
                        reply_to_message_id=message.message_id,
                    )
                    return
                await asyncio.sleep(0.5)

        # =====================================
        # 📱 所有数据操作成功后，立即显示完整结果
        # =====================================
        expected_time_display = expected_dt.strftime("%m/%d %H:%M")
        result_msg = (
            f"{emoji} <b>{shift_name}{action_text}打卡完成</b>\n"
            f"👤 用户：{MessageFormatter.format_user_link(uid, name)}\n"
            f"⏰ 打卡时间：<code>{current_time}</code>\n"
            f"📅 期望时间：<code>{expected_time_display}</code>\n"
            f"📊 状态：{status}"
        )

        if checkin_type == "work_end" and activity_auto_ended and current_activity:
            result_msg += (
                f"\n\n🔄 检测到未结束活动 <code>{current_activity}</code>，已自动结束"
            )

        await message.answer(
            result_msg,
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(uid)
            ),
            reply_to_message_id=message.message_id,
            parse_mode="HTML",
        )

        logger.info(
            f"[{trace_id}] ✅ 打卡完成通知已发送：\n"
            f"   班次：{shift_name}\n"
            f"   动作：{action_text}\n"
            f"   状态：{status}\n"
            f"   罚款：{fine_amount}元\n"
            f"   活动自动结束：{activity_auto_ended}"
        )

        # =====================================
        # 📢 智能通知模块
        # =====================================
        if is_late_early:
            try:
                status_type = "迟到" if checkin_type == "work_start" else "早退"
                duration = MessageFormatter.format_duration(abs(time_diff_seconds))

                with suppress(Exception):
                    chat_info = await bot.get_chat(chat_id)
                    chat_title = getattr(chat_info, "title", str(chat_id))

                notif_text = (
                    f"⚠️ <b>{shift_name}{status_type}通知</b>\n"
                    f"🏢 群组：<code>{chat_title}</code>\n"
                    f"{MessageFormatter.create_dashed_line()}\n"
                    f"👤 用户：{MessageFormatter.format_user_link(uid, name)}\n"
                    f"⏰ 打卡时间：<code>{current_time}</code>\n"
                    f"📅 期望时间：<code>{expected_time_display}</code>\n"
                    f"⏱️ {status_type} {duration}"
                )

                if fine_amount:
                    notif_text += f"\n💰 罚款金额：<code>{fine_amount}</code> 元"

                asyncio.create_task(
                    notification_service.send_notification(chat_id, notif_text)
                )
                
                logger.info(f"[{trace_id}] 📢 迟到/早退通知已发送到通知服务")
                
            except Exception as e:
                logger.error(f"[{trace_id}] ❌ 通知发送失败: {e}")

    logger.info(f"✅[{trace_id}] {shift_name}{action_text}打卡流程完成")

# ========== 管理员装饰器 ==========
def admin_required(func):
    """管理员权限检查装饰器"""

    @wraps(func)
    async def wrapper(message: types.Message, *args, **kwargs):
        if not await is_admin(message.from_user.id):
            await message.answer(
                Config.MESSAGES["no_permission"],
                reply_markup=await get_main_keyboard(
                    message.chat.id, await is_admin(message.from_user.id)
                ),
            )
            return
        return await func(message, *args, **kwargs)

    return wrapper


# ========== 消息处理器 ==========
@rate_limit(rate=5, per=60)
@message_deduplicate
async def cmd_start(message: types.Message):
    """开始命令"""
    uid = message.from_user.id
    is_admin_user = await is_admin(uid)

    await message.answer(
        Config.MESSAGES["welcome"],
        reply_markup=await get_main_keyboard(message.chat.id, is_admin_user),
        reply_to_message_id=message.message_id,
    )


@rate_limit(rate=5, per=60)
async def cmd_menu(message: types.Message):
    """显示主菜单"""
    uid = message.from_user.id
    await message.answer(
        "📋 主菜单",
        reply_markup=await get_main_keyboard(
            chat_id=message.chat.id, show_admin=await is_admin(uid)
        ),
        reply_to_message_id=message.message_id,
    )


@rate_limit(rate=5, per=60)
async def cmd_help(message: types.Message):
    """帮助命令"""
    uid = message.from_user.id

    help_text = (
        "📋 打卡机器人使用帮助\n\n"
        "🟢 开始活动打卡：\n"
        "• 直接输入活动名称\n"
        "• 或使用命令：/ci 活动名\n"
        "• 或点击下方活动按钮\n\n"
        "🔴 结束活动回座：\n"
        "• 直接输入：回座\n"
        "• 或使用命令：/at\n"
        "• 或点击下方 ✅ 回座 按钮\n\n"
        "🕒 上下班打卡：\n"
        "• /workstart - 上班打卡\n"
        "• /workend - 下班打卡\n"
        "• 或点击 🟢 上班 和 🔴 下班 按钮\n\n"
        "📊 查看记录：\n"
        "• 点击 📊 我的记录 查看个人统计\n"
        "• 点击 🏆 排行榜 查看群内排名\n\n"
        "🔧 其他命令：\n"
        "• /start - 开始使用机器人\n"
        "• /menu - 显示主菜单\n"
        "• /help - 显示此帮助信息"
    )

    await message.answer(
        help_text,
        reply_markup=await get_main_keyboard(
            chat_id=message.chat.id, show_admin=await is_admin(uid)
        ),
        reply_to_message_id=message.message_id,
        parse_mode="HTML",
    )


# 🆕 ========== 新增：我的记录和排行榜命令 ==========
@rate_limit(rate=10, per=60)
@track_performance("cmd_myinfo")
async def handle_myinfo_command(message: types.Message):
    """处理 /myinfo 命令 - 显示我的记录"""
    chat_id = message.chat.id
    uid = message.from_user.id

    user_lock = user_lock_manager.get_lock(chat_id, uid)
    async with user_lock:
        await show_history(message)


@rate_limit(rate=10, per=60)
@track_performance("cmd_ranking")
async def handle_ranking_command(message: types.Message):
    """处理 /ranking 命令 - 显示排行榜"""
    chat_id = message.chat.id
    uid = message.from_user.id

    user_lock = user_lock_manager.get_lock(chat_id, uid)
    async with user_lock:
        await show_rank(message)


@rate_limit(rate=10, per=60)
@message_deduplicate
@with_retry("cmd_ci", max_retries=2)
@track_performance("cmd_ci")
async def cmd_ci(message: types.Message):
    """指令打卡"""
    args = message.text.split(maxsplit=1)
    if len(args) != 2:
        await message.answer(
            "❌ 用法：/ci <活动名>",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=await is_admin(message.from_user.id)
            ),
            reply_to_message_id=message.message_id,
        )
        return

    act = args[1].strip()

    activity_aliases = {
        "抽烟": "抽烟或休息",
        "休息": "抽烟或休息",
        "smoke": "抽烟或休息",
        "吸烟": "抽烟或休息",
    }
    if act in activity_aliases:
        act = activity_aliases[act]

    if not await db.activity_exists(act):
        await message.answer(
            f"❌ 活动 '<code>{act}</code>' 不存在，请先使用 /addactivity 添加或检查拼写",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=await is_admin(message.from_user.id)
            ),
            reply_to_message_id=message.message_id,
            parse_mode="HTML",
        )
        return

    await start_activity(message, act)


@rate_limit(rate=10, per=60)
@message_deduplicate
@with_retry("cmd_at", max_retries=2)
@track_performance("cmd_at")
async def cmd_at(message: types.Message):
    """指令回座"""
    await process_back(message)


@rate_limit(rate=5, per=60)
@message_deduplicate
@with_retry("work_start", max_retries=2)
@track_performance("work_start")
async def cmd_workstart(message: types.Message):
    """上班打卡"""
    await process_work_checkin(message, "work_start")


@rate_limit(rate=5, per=60)
@message_deduplicate
@with_retry("work_end", max_retries=2)
@track_performance("work_end")
async def cmd_workend(message: types.Message):
    """下班打卡"""
    await process_work_checkin(message, "work_end")


# ========== 管理员命令 ==========
@admin_required
@rate_limit(rate=5, per=60)
async def cmd_admin(message: types.Message):
    """管理员命令"""
    await message.answer(
        "👑 管理员面板",
        reply_markup=get_admin_keyboard(),
        reply_to_message_id=message.message_id,
    )


# ========== 修复消息引用 ==========
@admin_required
@rate_limit(rate=2, per=60)
async def cmd_fix_message_refs(message: types.Message):
    """修复消息引用（清除所有消息ID）"""
    chat_id = message.chat.id

    try:
        await message.answer("⏳ 正在清除所有消息引用记录...")

        # 使用数据库的 execute_with_retry 方法
        result = await db.execute_with_retry(
            "修复消息引用",
            """
            UPDATE users 
            SET checkin_message_id = NULL, updated_at = CURRENT_TIMESTAMP 
            WHERE chat_id = $1 AND checkin_message_id IS NOT NULL
            """,
            chat_id,
        )

        # 解析受影响的行数
        updated_count = 0
        if result and result.startswith("UPDATE"):
            parts = result.split()
            if len(parts) >= 2:
                updated_count = int(parts[-1])

        await message.answer(
            f"✅ 已清除 {updated_count} 个消息引用记录\n"
            f"💡 下次打卡将重新建立正确的消息引用",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            parse_mode="HTML",
        )
        logger.info(
            f"管理员 {message.from_user.id} 清除了群组 {chat_id} 的 {updated_count} 个消息引用"
        )

    except Exception as e:
        logger.error(f"修复消息引用失败: {e}")
        await message.answer(
            f"❌ 修复失败：{str(e)[:200]}",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
        )


# ========== 月度数据清理命令 ==========
@admin_required
@rate_limit(rate=2, per=60)
async def cmd_cleanup_monthly(message: types.Message):
    """清理月度统计数据"""
    args = message.text.split()

    target_date = None
    if len(args) >= 3:
        try:
            year = int(args[1])
            month = int(args[2])
            if month < 1 or month > 12:
                await message.answer("❌ 月份必须在1-12之间")
                return
            target_date = date(year, month, 1)
        except ValueError:
            await message.answer("❌ 请输入有效的年份和月份")
            return
    elif len(args) == 2 and args[1].lower() == "all":
        # 特殊命令：清理所有月度数据（谨慎使用）
        await message.answer(
            "⚠️ <b>危险操作确认</b>\n\n"
            "您即将删除<u>所有</u>月度统计数据！\n"
            "此操作不可恢复！\n\n"
            "请输入 <code>/cleanup_monthly confirm_all</code> 确认执行",
            parse_mode="HTML",
            reply_to_message_id=message.message_id,
        )
        return
    elif len(args) == 2 and args[1].lower() == "confirm_all":
        # 确认清理所有数据
        try:
            async with db.pool.acquire() as conn:
                result = await conn.execute("DELETE FROM monthly_statistics")
                deleted_count = (
                    int(result.split()[-1])
                    if result and result.startswith("DELETE")
                    else 0
                )

            await message.answer(
                f"🗑️ <b>已清理所有月度统计数据</b>\n"
                f"删除记录: <code>{deleted_count}</code> 条\n\n"
                f"⚠️ 所有月度统计已被清空，月度报告将无法生成历史数据",
                parse_mode="HTML",
                reply_to_message_id=message.message_id,
            )
            logger.warning(f"👑 管理员 {message.from_user.id} 清理了所有月度统计数据")
            return
        except Exception as e:
            await message.answer(
                f"❌ 清理所有数据失败: {e}", reply_to_message_id=message.message_id
            )
            return

    await message.answer(
        "⏳ 正在清理月度统计数据...", reply_to_message_id=message.message_id
    )

    try:
        if target_date:
            # 清理指定月份
            deleted_count = await db.cleanup_specific_month(
                target_date.year, target_date.month
            )
            date_str = target_date.strftime("%Y年%m月")
            await message.answer(
                f"✅ <b>月度统计清理完成</b>\n"
                f"📅 清理月份: <code>{date_str}</code>\n"
                f"🗑️ 删除记录: <code>{deleted_count}</code> 条",
                parse_mode="HTML",
                reply_to_message_id=message.message_id,
            )
        else:
            # 默认清理3个月前的数据
            deleted_count = await db.cleanup_monthly_data()
            today = get_beijing_time()
            cutoff_date = (today - timedelta(days=90)).date().replace(day=1)
            cutoff_str = cutoff_date.strftime("%Y年%m月")

            await message.answer(
                f"✅ <b>月度统计自动清理完成</b>\n"
                f"📅 清理截止: <code>{cutoff_str}</code> 之前\n"
                f"🗑️ 删除记录: <code>{deleted_count}</code> 条\n\n"
                f"💡 保留了最近3个月的月度统计数据",
                parse_mode="HTML",
                reply_to_message_id=message.message_id,
            )

    except Exception as e:
        logger.error(f"❌ 清理月度数据失败: {e}")
        await message.answer(
            f"❌ 清理月度数据失败: {e}", reply_to_message_id=message.message_id
        )


@admin_required
@rate_limit(rate=5, per=60)
async def cmd_monthly_stats_status(message: types.Message):
    """查看月度统计数据状态 - 高精度版"""
    chat_id = message.chat.id

    try:
        async with db.pool.acquire() as conn:
            # 查询每个月的总记录数、活跃用户数和活动类型数
            monthly_rows = await conn.fetch(
                """
                SELECT
                    DATE_TRUNC('month', statistic_date) AS month,
                    COUNT(*) AS total_records,
                    COUNT(DISTINCT user_id) AS monthly_users,
                    COUNT(DISTINCT activity_name) AS monthly_activities
                FROM monthly_statistics
                WHERE chat_id = $1
                GROUP BY month
                ORDER BY month DESC
                """,
                chat_id,
            )

            # 总计信息
            total_records = await conn.fetchval(
                "SELECT COUNT(*) FROM monthly_statistics WHERE chat_id = $1",
                chat_id,
            )
            total_users = await conn.fetchval(
                "SELECT COUNT(DISTINCT user_id) FROM monthly_statistics WHERE chat_id = $1",
                chat_id,
            )
            total_activities = await conn.fetchval(
                "SELECT COUNT(DISTINCT activity_name) FROM monthly_statistics WHERE chat_id = $1",
                chat_id,
            )

        if not monthly_rows:
            await message.answer(
                "📊 <b>月度统计数据状态</b>\n\n暂无月度统计数据",
                parse_mode="HTML",
                reply_to_message_id=message.message_id,
            )
            return

        earliest = min(row["month"] for row in monthly_rows)
        latest = max(row["month"] for row in monthly_rows)

        status_text = (
            f"📊 <b>月度统计数据状态</b>\n\n"
            f"📅 数据范围: <code>{earliest.strftime('%Y年%m月')}</code> - <code>{latest.strftime('%Y年%m月')}</code>\n"
            f"👥 总用户数: <code>{total_users}</code> 人\n"
            f"📝 活动类型总数: <code>{total_activities}</code> 种\n"
            f"💾 总记录数: <code>{total_records}</code> 条\n\n"
            f"<b>最近12个月数据量:</b>\n"
        )

        for row in monthly_rows[:12]:
            month_str = row["month"].strftime("%Y年%m月")
            total = row["total_records"]
            users = row["monthly_users"]
            acts = row["monthly_activities"]
            status_text += f"• {month_str}: <code>{total}</code> 条, 用户 <code>{users}</code> 人, 活动类型 <code>{acts}</code> 种\n"

        if len(monthly_rows) > 12:
            status_text += f"• ... 还有 {len(monthly_rows) - 12} 个月份\n"

        status_text += (
            "\n💡 <b>可用命令:</b>\n"
            "• <code>/cleanup_monthly</code> - 自动清理（保留最近3个月）\n"
            "• <code>/cleanup_monthly 年 月</code> - 清理指定月份\n"
            "• <code>/cleanup_monthly all</code> - 清理所有数据（危险）"
        )

        await message.answer(
            status_text,
            parse_mode="HTML",
            reply_to_message_id=message.message_id,
        )

    except Exception as e:
        logger.error(f"❌ 查看月度统计状态失败: {e}")
        await message.answer(
            "❌ 查看月度统计状态失败，请稍后重试",
            reply_to_message_id=message.message_id,
        )


@admin_required
@rate_limit(rate=1, per=60)
async def cmd_cleanup_inactive(message: types.Message):
    """清理长期未活动的用户数据"""
    args = message.text.split()
    days = 30  # 默认30天未活动

    # 用户指定天数
    if len(args) > 1:
        try:
            days = int(args[1])
            if days < 7:
                await message.answer(
                    "❌ 天数不能少于7天，避免误删活跃用户",
                    reply_to_message_id=message.message_id,
                )
                return
        except ValueError:
            await message.answer(
                "❌ 天数必须是数字，例如：/cleanup_inactive 60",
                reply_to_message_id=message.message_id,
            )
            return

    await message.answer(
        f"⏳ 正在清理 {days} 天未活动的用户，请稍候...",
        reply_to_message_id=message.message_id,
    )

    cutoff_date = (get_beijing_time() - timedelta(days=days)).date()

    try:
        async with db.pool.acquire() as conn:
            # 删除用户
            result_users = await conn.execute(
                "DELETE FROM users WHERE last_updated < $1", cutoff_date
            )
            deleted_users = (
                int(result_users.split()[-1])
                if result_users.startswith("DELETE")
                else 0
            )

            # 删除活动记录
            result_activities = await conn.execute(
                "DELETE FROM user_activities WHERE activity_date < $1", cutoff_date
            )
            deleted_activities = (
                int(result_activities.split()[-1])
                if result_activities.startswith("DELETE")
                else 0
            )

            # 删除工作记录
            result_work = await conn.execute(
                "DELETE FROM work_records WHERE record_date < $1", cutoff_date
            )
            deleted_work_records = (
                int(result_work.split()[-1]) if result_work.startswith("DELETE") else 0
            )

        total_deleted = deleted_users + deleted_activities + deleted_work_records

        await message.answer(
            f"🧹 <b>长期未活动用户清理完成</b>\n\n"
            f"📅 清理截止: <code>{cutoff_date}</code> 之前\n"
            f"🗑️ 删除用户: <code>{deleted_users}</code> 个\n"
            f"🗑️ 删除活动记录: <code>{deleted_activities}</code> 条\n"
            f"🗑️ 删除工作记录: <code>{deleted_work_records}</code> 条\n\n"
            f"📊 总计删除: <code>{total_deleted}</code> 条记录\n"
            f"⚠️ 此操作不可撤销",
            parse_mode="HTML",
            reply_to_message_id=message.message_id,
        )

        logger.info(
            f"👑 管理员 {message.from_user.id} 清理 {days} 天未活动用户: "
            f"{deleted_users} 用户, {deleted_activities} 活动, {deleted_work_records} 工作记录"
        )

    except Exception as e:
        logger.exception("❌ 清理未活动用户失败")
        await message.answer(
            f"❌ 清理未活动用户失败: {e}", reply_to_message_id=message.message_id
        )


# ========== 重置用户命令 ==========
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_reset_user(message: types.Message):
    """重置指定用户的今日数据"""
    args = message.text.split()
    if len(args) < 2:
        await message.answer(
            "❌ 用法：/resetuser <用户ID> [confirm]\n"
            "💡 示例：/resetuser 123456789 confirm",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
        return

    try:
        chat_id = message.chat.id
        target_user_id = int(args[1])
        confirm = len(args) == 3 and args[2].lower() == "confirm"

        if not confirm:
            await message.answer(
                f"⚠️ 确认重置用户 <code>{target_user_id}</code> 的今日数据？\n"
                f"请输入 <code>/resetuser {target_user_id} confirm</code> 执行",
                parse_mode="HTML",
                reply_to_message_id=message.message_id,
            )
            return

        await message.answer(
            f"⏳ 正在重置用户 {target_user_id} 的今日数据...",
            reply_to_message_id=message.message_id,
        )

        # 执行重置
        success = await db.reset_shift_data(chat_id, target_user_id)

        if success:
            await message.answer(
                f"✅ 已重置用户 <code>{target_user_id}</code> 的今日数据\n\n"
                f"🗑️ 已清除：今日活动记录 | 今日统计计数 | 当前活动状态 | 罚款计数（保留总罚款）",
                parse_mode="HTML",
                reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
                reply_to_message_id=message.message_id,
            )
            logger.info(
                f"👑 管理员 {message.from_user.id} 在群 {chat_id} 重置了用户 {target_user_id} 的今日数据"
            )
        else:
            await message.answer(
                f"❌ 重置用户 {target_user_id} 数据失败",
                reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
                reply_to_message_id=message.message_id,
            )

    except ValueError:
        await message.answer(
            "❌ 用户ID必须是数字",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
    except Exception as e:
        logger.exception(f"重置用户数据失败")
        await message.answer(
            f"❌ 重置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )

# ========== 双班管理命令 ==========
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setdualmode(message: types.Message):
    """设置双班模式 - 完整整合版
    用法：
    1. 开启双班并设置时间：/setdualmode on 09:00 21:00
    2. 使用默认时间开启：/setdualmode on
    3. 关闭双班：/setdualmode off
    4. 查看状态：/setdualmode status
    5. 修改时间（已开启时）：/setdualmode 09:00 21:00
    """
    args = message.text.split()
    
    if len(args) == 1:
        # 显示帮助
        await message.answer(
            "📋 <b>双班模式设置命令</b>\n\n"
            "🔄 <b>开启双班：</b>\n"
            "• <code>/setdualmode on 09:00 21:00</code> - 开启并设置时间\n"
            "• <code>/setdualmode on</code> - 开启（使用默认时间 09:00-21:00）\n\n"
            "⚙️ <b>修改时间（双班已开启时）：</b>\n"
            "<code>/setdualmode 09:00 21:00</code>\n\n"
            "🚫 <b>关闭双班：</b>\n"
            "<code>/setdualmode off</code>\n\n"
            "📊 <b>查看状态：</b>\n"
            "<code>/setdualmode status</code>\n\n"
            "💡 <b>说明：</b>\n"
            "• 白班时间内打卡 → 白班记录\n"
            "• 白班时间外打卡 → 夜班记录\n"
            "• 支持提前30分钟打卡\n"
            "• 班次一旦确定，当日活动都按此班次记录",
            parse_mode="HTML",
            reply_to_message_id=message.message_id,
        )
        return
    
    chat_id = message.chat.id
    
    try:
        # ========== 情况1：查看状态 ==========
        if args[1].lower() == "status":
            await show_shift_status_inline(message)
            return
        
        # ========== 情况2：关闭双班 ==========
        elif args[1].lower() == "off":
            # 获取当前配置
            group_config = await db.get_group_shift_config(chat_id)
            
            # 更新配置（关闭双班，但保留时间设置）
            await db.update_group_shift_config(
                chat_id, 
                False,  # dual_mode = False
                group_config['day_start'],  # 保持原有时间
                group_config['day_end']     # 保持原有时间
            )
            
            await message.answer(
                f"✅ <b>双班模式已关闭</b>\n\n"
                f"📊 <b>当前状态：单班制</b>\n"
                f"• 所有记录归为白班\n"
                f"• 不再区分白班/夜班\n\n"
                f"💡 如需再次开启：\n"
                f"<code>/setdualmode on 09:00 21:00</code>",
                parse_mode="HTML",
                reply_to_message_id=message.message_id,
            )
            return
        
        # ========== 情况3：开启双班（带时间参数） ==========
        elif args[1].lower() == "on":
            if len(args) == 4:
                # 格式：/setdualmode on 09:00 21:00
                start_time = args[2]
                end_time = args[3]
            elif len(args) == 2:
                # 格式：/setdualmode on（使用默认时间）
                start_time = "09:00"
                end_time = "21:00"
            else:
                await message.answer(
                    "❌ 参数错误！正确格式：\n"
                    "• <code>/setdualmode on 09:00 21:00</code>\n"
                    "• <code>/setdualmode on</code>（使用默认时间）",
                    parse_mode="HTML",
                    reply_to_message_id=message.message_id,
                )
                return
            
            # 验证时间格式
            time_pattern = re.compile(r"^([0-1]?[0-9]|2[0-3]):([0-5][0-9])$")
            if not time_pattern.match(start_time) or not time_pattern.match(end_time):
                await message.answer(
                    "❌ 时间格式错误！请使用 HH:MM 格式（24小时制）\n"
                    "📝 示例：09:00、21:00、22:00",
                    reply_to_message_id=message.message_id,
                )
                return
            
            # 更新配置
            await db.update_group_shift_config(
                chat_id, 
                True,  # dual_mode = True
                start_time, 
                end_time
            )
            
            # 判断是否跨天
            from utils import parse_time_to_minutes
            start_min = parse_time_to_minutes(start_time)
            end_min = parse_time_to_minutes(end_time)
            is_cross_day = start_min >= end_min
            
            await message.answer(
                f"✅ <b>双班模式已开启</b>\n\n"
                f"☀️ <b>白班配置</b>\n"
                f"• 时间窗口：<code>{start_time}</code> - <code>{end_time}</code>\n"
                f"• 时段类型：{'跨天' if is_cross_day else '非跨天'}\n\n"
                f"🌙 <b>夜班配置</b>\n"
                f"• 时间窗口：白班以外时段\n\n"
                f"💡 <b>使用说明：</b>\n"
                f"• 在此时间段内打卡归为白班\n"
                f"• 其余时间打卡归为夜班\n"
                f"• 班次确定后，当日活动都按此班次记录\n"
                f"• 支持提前30分钟打卡\n\n"
                f"📋 <b>管理命令：</b>\n"
                f"• <code>/setdualmode 10:00 22:00</code> - 修改时间\n"
                f"• <code>/setdualmode off</code> - 关闭双班\n"
                f"• <code>/setdualmode status</code> - 查看状态\n"
                f"• <code>/shiftreset 0/1</code> - 重置班次数据",
                parse_mode="HTML",
                reply_to_message_id=message.message_id,
            )
            return
        
        # ========== 情况4：直接修改时间（无on/off参数） ==========
        elif len(args) == 3:
            # 格式：/setdualmode 09:00 21:00（直接修改时间）
            start_time = args[1]
            end_time = args[2]
            
            # 验证时间格式
            time_pattern = re.compile(r"^([0-1]?[0-9]|2[0-3]):([0-5][0-9])$")
            if not time_pattern.match(start_time) or not time_pattern.match(end_time):
                await message.answer(
                    "❌ 时间格式错误！请使用 HH:MM 格式（24小时制）\n"
                    "📝 示例：09:00、21:00、22:00",
                    reply_to_message_id=message.message_id,
                )
                return
            
            # 获取当前配置
            group_config = await db.get_group_shift_config(chat_id)
            
            if not group_config['dual_mode']:
                await message.answer(
                    "❌ 双班模式未开启！\n\n"
                    "💡 请先开启双班模式：\n"
                    "<code>/setdualmode on 09:00 21:00</code>",
                    parse_mode="HTML",
                    reply_to_message_id=message.message_id,
                )
                return
            
            # 更新配置
            await db.update_group_shift_config(
                chat_id, 
                True,  # 保持双班开启
                start_time, 
                end_time
            )
            
            # 判断是否跨天
            from utils import parse_time_to_minutes
            start_min = parse_time_to_minutes(start_time)
            end_min = parse_time_to_minutes(end_time)
            is_cross_day = start_min >= end_min
            
            await message.answer(
                f"✅ <b>双班时间已更新</b>\n\n"
                f"☀️ <b>白班配置</b>\n"
                f"• 时间窗口：<code>{start_time}</code> - <code>{end_time}</code>\n"
                f"• 时段类型：{'跨天' if is_cross_day else '非跨天'}\n\n"
                f"🌙 <b>夜班配置</b>\n"
                f"• 时间窗口：白班以外时段\n\n"
                f"💡 <b>注意：</b>\n"
                f"• 已打卡用户的班次不会自动变更\n"
                f"• 新用户将按新时间判断班次\n"
                f"• 如需重置现有用户班次，请使用：\n"
                f"  <code>/shiftreset 0</code>（重置白班）\n"
                f"  <code>/shiftreset 1</code>（重置夜班）",
                parse_mode="HTML",
                reply_to_message_id=message.message_id,
            )
            return
        
        else:
            await message.answer(
                "❌ 参数错误！请使用以下格式：\n\n"
                "🔄 <b>开启双班：</b>\n"
                "• <code>/setdualmode on 09:00 21:00</code>\n"
                "• <code>/setdualmode on</code>（默认时间）\n\n"
                "⚙️ <b>修改时间：</b>\n"
                "<code>/setdualmode 09:00 21:00</code>\n\n"
                "🚫 <b>关闭双班：</b>\n"
                "<code>/setdualmode off</code>\n\n"
                "📊 <b>查看状态：</b>\n"
                "<code>/setdualmode status</code>",
                parse_mode="HTML",
                reply_to_message_id=message.message_id,
            )
            
    except Exception as e:
        logger.error(f"设置双班模式失败: {e}")
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_to_message_id=message.message_id,
        )


async def show_shift_status_inline(message: types.Message):
    """显示班次状态（内联版）"""
    chat_id = message.chat.id
    
    try:
        # 获取配置
        group_config = await db.get_group_shift_config(chat_id)
        now = datetime.now(beijing_tz)
        
        from utils import is_time_in_day_shift
        current_shift = "白班" if is_time_in_day_shift(now, group_config['day_start'], group_config['day_end']) else "夜班"
        
        # 获取各班次在线人数
        async with db.pool.acquire() as conn:
            # 白班在线人数
            day_shift_online = await conn.fetchval(
                """
                SELECT COUNT(DISTINCT u.user_id) 
                FROM users u
                JOIN user_status us ON u.chat_id = us.chat_id AND u.user_id = us.user_id
                WHERE u.chat_id = $1 
                  AND us.on_duty_shift = 0
                  AND u.current_activity IS NOT NULL
                """,
                chat_id,
            ) or 0
            
            # 夜班在线人数
            night_shift_online = await conn.fetchval(
                """
                SELECT COUNT(DISTINCT u.user_id) 
                FROM users u
                JOIN user_status us ON u.chat_id = us.chat_id AND u.user_id = us.user_id
                WHERE u.chat_id = $1 
                  AND us.on_duty_shift = 1
                  AND u.current_activity IS NOT NULL
                """,
                chat_id,
            ) or 0
            
            # 各班次总人数
            day_shift_total = await conn.fetchval(
                "SELECT COUNT(*) FROM user_status WHERE chat_id = $1 AND on_duty_shift = 0",
                chat_id,
            ) or 0
            
            night_shift_total = await conn.fetchval(
                "SELECT COUNT(*) FROM user_status WHERE chat_id = $1 AND on_duty_shift = 1",
                chat_id,
            ) or 0
        
        status_text = (
            f"📊 <b>班次系统状态</b>\n\n"
            f"🏢 群组：<code>{chat_id}</code>\n"
            f"🔄 模式：{'双班制' if group_config['dual_mode'] else '单班制'}\n"
            f"⏰ 当前时间：<code>{now.strftime('%H:%M')}</code>（{current_shift}时段）\n\n"
        )
        
        if group_config['dual_mode']:
            status_text += (
                f"☀️ <b>白班配置</b>\n"
                f"• 时间窗口：<code>{group_config['day_start']}</code> - <code>{group_config['day_end']}</code>\n"
                f"• 在班人数：<code>{day_shift_total}</code> 人\n"
                f"• 在线活动：<code>{day_shift_online}</code> 人\n\n"
                
                f"🌙 <b>夜班配置</b>\n"
                f"• 时间窗口：白班以外时段\n"
                f"• 在班人数：<code>{night_shift_total}</code> 人\n"
                f"• 在线活动：<code>{night_shift_online}</code> 人\n\n"
            )
        else:
            status_text += (
                f"📝 <b>单班模式</b>\n"
                f"• 所有记录归为白班\n"
                f"• 在班人数：<code>{day_shift_total}</code> 人\n"
                f"• 在线活动：<code>{day_shift_online}</code> 人\n\n"
            )
        
        status_text += (
            f"💡 <b>管理命令</b>\n"
            f"• <code>/setdualmode on 09:00 21:00</code> - 开启双班\n"
            f"• <code>/setdualmode 08:00 20:00</code> - 修改时间\n"
            f"• <code>/setdualmode off</code> - 关闭双班\n"
            f"• <code>/setdualmode status</code> - 查看此状态\n"
            f"• <code>/shiftreset 0/1</code> - 重置班次数据"
        )
        
        await message.answer(
            status_text,
            parse_mode="HTML",
            reply_to_message_id=message.message_id,
        )
        
    except Exception as e:
        logger.error(f"查看班次状态失败: {e}")
        await message.answer(
            f"❌ 获取状态失败：{e}",
            reply_to_message_id=message.message_id,
        )


@admin_required
@rate_limit(rate=5, per=60)
async def cmd_shiftstatus(message: types.Message):
    """查看班次状态"""
    chat_id = message.chat.id
    
    try:
        # 获取配置
        group_config = await db.get_group_shift_config(chat_id)
        
        # 获取各班次在线人数
        async with db.pool.acquire() as conn:
            # 白班在线人数
            day_shift_online = await conn.fetchval(
                """
                SELECT COUNT(DISTINCT u.user_id) 
                FROM users u
                JOIN user_status us ON u.chat_id = us.chat_id AND u.user_id = us.user_id
                WHERE u.chat_id = $1 
                  AND us.on_duty_shift = 0
                  AND u.current_activity IS NOT NULL
                """,
                chat_id,
            ) or 0
            
            # 夜班在线人数
            night_shift_online = await conn.fetchval(
                """
                SELECT COUNT(DISTINCT u.user_id) 
                FROM users u
                JOIN user_status us ON u.chat_id = us.chat_id AND u.user_id = us.user_id
                WHERE u.chat_id = $1 
                  AND us.on_duty_shift = 1
                  AND u.current_activity IS NOT NULL
                """,
                chat_id,
            ) or 0
            
            # 各班次总人数
            day_shift_total = await conn.fetchval(
                "SELECT COUNT(*) FROM user_status WHERE chat_id = $1 AND on_duty_shift = 0",
                chat_id,
            ) or 0
            
            night_shift_total = await conn.fetchval(
                "SELECT COUNT(*) FROM user_status WHERE chat_id = $1 AND on_duty_shift = 1",
                chat_id,
            ) or 0
        
        now = datetime.now(beijing_tz)
        from utils import is_time_in_day_shift
        current_shift = "白班" if is_time_in_day_shift(now, group_config['day_start'], group_config['day_end']) else "夜班"
        
        status_text = (
            f"📊 <b>班次系统状态</b>\n\n"
            f"🏢 群组：<code>{chat_id}</code>\n"
            f"🔄 模式：{'双班制' if group_config['dual_mode'] else '单班制'}\n"
            f"⏰ 当前时间：<code>{now.strftime('%H:%M')}</code>（{current_shift}时段）\n\n"
        )
        
        if group_config['dual_mode']:
            status_text += (
                f"☀️ <b>白班配置</b>\n"
                f"• 时间窗口：<code>{group_config['day_start']}</code> - <code>{group_config['day_end']}</code>\n"
                f"• 在班人数：<code>{day_shift_total}</code> 人\n"
                f"• 在线活动：<code>{day_shift_online}</code> 人\n\n"
                
                f"🌙 <b>夜班配置</b>\n"
                f"• 时间窗口：白班以外时段\n"
                f"• 在班人数：<code>{night_shift_total}</code> 人\n"
                f"• 在线活动：<code>{night_shift_online}</code> 人\n\n"
            )
        else:
            status_text += (
                f"📝 <b>单班模式</b>\n"
                f"• 所有记录归为白班\n"
                f"• 在班人数：<code>{day_shift_total}</code> 人\n"
                f"• 在线活动：<code>{day_shift_online}</code> 人\n\n"
            )
        
        status_text += (
            f"💡 <b>管理命令</b>\n"
            f"• /setdualmode on/off - 开关双班\n"
            f"• /setdaytime HH:MM HH:MM - 设置白班时间\n"
            f"• /shiftreset 0/1 - 重置班次数据"
        )
        
        await message.answer(
            status_text,
            parse_mode="HTML",
            reply_to_message_id=message.message_id,
        )
        
    except Exception as e:
        logger.error(f"查看班次状态失败: {e}")
        await message.answer(
            f"❌ 获取状态失败：{e}",
            reply_to_message_id=message.message_id,
        )


# 在main.py中修改现有的reset_shift命令

@admin_required
@rate_limit(rate=2, per=60)
async def cmd_shiftreset(message: types.Message):
    """班次重置命令 - 更新版"""
    args = message.text.split()
    if len(args) != 2:
        await message.answer(
            "🔄 班次重置系统\n\n"
            "⏰ 自动执行时间：\n"
            "• 23:00 - 白班重置（硬重置）\n"
            "• 11:00 - 夜班重置（软重置）\n\n"
            "🛠 手动命令：\n"
            "/shiftreset 0    # 手动重置白班\n"
            "/shiftreset 1    # 手动重置夜班\n"
            "/shiftreset clean # 手动清理已导出数据\n\n"
            "💡 注意：清理操作只删除昨天的daily_statistics数据"
        )
        return
    
    try:
        chat_id = message.chat.id
        now = datetime.now(beijing_tz)
        
        if args[1] == "0":
            # 白班重置
            await message.answer("⏳ 正在执行白班重置...")
            await db.reset_shift_data(chat_id, 0, is_hard_reset=True)
            await message.answer(
                "✅ 白班重置完成\n\n"
                "🗑 已清理：\n"
                "• 白班活动记录\n"
                "• 白班工作记录\n"
                "• 白班统计记录\n\n"
                "🔄 已重置：\n"
                "• 用户活动状态\n"
                "• 用户统计计数"
            )
            
        elif args[1] == "1":
            # 夜班重置
            await message.answer("⏳ 正在执行夜班重置...")
            await db.reset_shift_data(chat_id, 1, is_hard_reset=False)
            await message.answer(
                "✅ 夜班重置完成\n\n"
                "🗑 已清理：\n"
                "• 夜班活动记录\n"
                "• 夜班工作记录\n"
                "• 夜班统计记录\n\n"
                "🔄 已重置：\n"
                "• 用户活动状态"
            )
            
        elif args[1] == "clean":
            # 安全清理
            await message.answer("⏳ 正在安全清理已导出数据...")
            success = await db.cleanup_exported_daily_stats(chat_id, now)
            if success:
                await message.answer(
                    "✅ 安全清理完成\n\n"
                    "🧹 已清理：昨天的daily_statistics数据\n"
                    "🔒 已保护：今天09:00之后的新数据"
                )
            else:
                await message.answer("❌ 清理失败，请查看日志")
                
        elif args[1] == "export":
            # 手动导出
            yesterday = now.date() - timedelta(days=1)
            await message.answer(f"⏳ 正在导出昨日数据: {yesterday}")
            # 这里可以调用导出函数
            await message.answer("✅ 导出完成（功能待实现）")
            
    except Exception as e:
        logger.error(f"班次重置命令失败: {e}")
        await message.answer(f"❌ 操作失败: {str(e)[:100]}")

# ========== 导出每日数据命令 ==========
@admin_required
@rate_limit(rate=2, per=60)
@track_performance("cmd_export")
async def cmd_export(message: types.Message):
    """导出数据"""
    chat_id = message.chat.id
    await message.answer(
        "⏳ 正在导出数据，请稍候...", reply_to_message_id=message.message_id
    )
    try:
        await export_and_push_csv(chat_id)
        await message.answer(
            "✅ 数据已导出并推送！", reply_to_message_id=message.message_id
        )
    except Exception as e:
        await message.answer(
            f"❌ 导出失败：{e}", reply_to_message_id=message.message_id
        )


# ========== 月度报告函数 ==========
async def optimized_monthly_export(chat_id: int, year: int, month: int):
    """稳定版月度数据导出 - 完整工作数据 + 兼容旧接口"""

    try:
        # ===== 1. 活动配置 =====
        activity_limits = await db.get_activity_limits_cached()
        activity_names = list(activity_limits.keys())

        csv_buffer = StringIO()
        writer = csv.writer(csv_buffer)

        # ===== 2. 构建表头 =====
        headers = ["用户ID", "用户昵称"]
        for act in activity_names:
            headers.extend([f"{act}次数", f"{act}总时长"])

        headers.extend(
            [
                "活动次数总计",
                "活动用时总计",
                "罚款总金额",
                "超时次数",
                "总超时时间",
                "工作天数",
                "工作时长",
                "上班次数",
                "下班次数",
                "迟到次数",
                "早退次数",
                "上下班罚款",
            ]
        )
        writer.writerow(headers)

        # ===== 3. 获取月度统计 =====
        monthly_stats = await db.get_monthly_statistics(chat_id, year, month)
        if not monthly_stats:
            logger.warning(f"月度统计表中没有找到 {year}年{month}月 的数据")
            return None

        # ===== 4. 获取工作统计 =====
        work_stats = await db.get_monthly_work_statistics(chat_id, year, month)
        work_stats_dict = {stat["user_id"]: stat for stat in work_stats}

        # ===== 5. 填充数据 =====
        for user_stat in monthly_stats:
            if not isinstance(user_stat, dict):
                continue

            user_id = user_stat.get("user_id", "未知")
            nickname = user_stat.get("nickname", "未知用户")

            row = [user_id, nickname]

            # 活动数据安全解析
            user_activities = user_stat.get("activities", {})
            if isinstance(user_activities, str):
                try:

                    user_activities = json.loads(user_activities)
                except:
                    user_activities = {}
            elif not isinstance(user_activities, dict):
                user_activities = {}

            # 填充活动数据
            for act in activity_names:
                activity_info = user_activities.get(act, {})
                if not isinstance(activity_info, dict):
                    activity_info = {}

                count = activity_info.get("count", 0)
                time_seconds = activity_info.get("time", 0)
                row.append(count)
                row.append(db.format_time_for_csv(time_seconds))

            # 工作相关统计
            work_data = work_stats_dict.get(user_id, {})
            late_early_counts = await db.get_user_late_early_counts(
                chat_id, user_id, year, month
            )

            row.extend(
                [
                    user_stat.get("total_activity_count", 0),
                    db.format_time_for_csv(user_stat.get("total_accumulated_time", 0)),
                    user_stat.get("total_fines", 0),
                    user_stat.get("overtime_count", 0),
                    db.format_time_for_csv(user_stat.get("total_overtime_time", 0)),
                    user_stat.get("work_days", 0),
                    db.format_time_for_csv(user_stat.get("work_hours", 0)),
                    work_data.get("work_start_count", 0),
                    work_data.get("work_end_count", 0),
                    late_early_counts.get("late_count", 0),
                    late_early_counts.get("early_count", 0),
                    work_data.get("work_start_fines", 0)
                    + work_data.get("work_end_fines", 0),
                ]
            )

            writer.writerow(row)

        return csv_buffer.getvalue()

    except Exception as e:
        logger.error(f"❌ 月度导出失败: {e}")

        logger.error(traceback.format_exc())
        return None


# ========= 导出月度报告命令 ========
@admin_required
@rate_limit(rate=2, per=60)
async def cmd_monthlyreport(message: types.Message):
    """生成月度报告 - 优化版本"""
    args = message.text.split()
    chat_id = message.chat.id

    year = None
    month = None

    if len(args) >= 3:
        try:
            year = int(args[1])
            month = int(args[2])
            if month < 1 or month > 12:
                await message.answer(
                    "❌ 月份必须在1-12之间", reply_to_message_id=message.message_id
                )
                return
        except ValueError:
            await message.answer(
                "❌ 请输入有效的年份和月份", reply_to_message_id=message.message_id
            )
            return

    await message.answer(
        "⏳ 正在生成月度报告，请稍候...", reply_to_message_id=message.message_id
    )

    try:
        # 生成报告
        report = await generate_monthly_report(chat_id, year, month)
        if report:
            await message.answer(
                report, parse_mode="HTML", reply_to_message_id=message.message_id
            )

            # 导出CSV
            await export_monthly_csv(chat_id, year, month)
            await message.answer(
                "✅ 月度数据已导出并推送！", reply_to_message_id=message.message_id
            )
        else:
            time_desc = f"{year}年{month}月" if year and month else "最近一个月"
            await message.answer(
                f"⚠️ {time_desc}没有数据需要报告", reply_to_message_id=message.message_id
            )

    except Exception as e:
        await message.answer(
            f"❌ 生成月度报告失败：{e}", reply_to_message_id=message.message_id
        )


@admin_required
@rate_limit(rate=2, per=60)
async def cmd_exportmonthly(message: types.Message):
    """导出月度数据 - 优化版本"""
    args = message.text.split()
    chat_id = message.chat.id

    year = None
    month = None

    if len(args) >= 3:
        try:
            year = int(args[1])
            month = int(args[2])
            if month < 1 or month > 12:
                await message.answer("❌ 月份必须在1-12之间")
                return
        except ValueError:
            await message.answer("❌ 请输入有效的年份和月份")
            return

    await message.answer("⏳ 正在导出月度数据，请稍候...")

    try:
        await export_monthly_csv(chat_id, year, month)
        await message.answer("✅ 月度数据已导出并推送！")
    except Exception as e:
        await message.answer(f"❌ 导出月度数据失败：{e}")


# ========== 添加活动命令 ==========
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_addactivity(message: types.Message):
    """添加新活动"""
    args = message.text.split()
    if len(args) != 4:
        await message.answer(
            Config.MESSAGES["addactivity_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
        return

    try:
        act, max_times, time_limit = args[1], int(args[2]), int(args[3])
        existed = await db.activity_exists(act)
        await db.update_activity_config(act, max_times, time_limit)
        await db.force_refresh_activity_cache()

        if existed:
            await message.answer(
                f"✅ 已修改活动 <code>{act}</code>，次数上限 <code>{max_times}</code>，时间限制 <code>{time_limit}</code> 分钟",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                reply_to_message_id=message.message_id,
                parse_mode="HTML",
            )
        else:
            await message.answer(
                f"✅ 已添加新活动 <code>{act}</code>，次数上限 <code>{max_times}</code>，时间限制 <code>{time_limit}</code> 分钟",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                reply_to_message_id=message.message_id,
                parse_mode="HTML",
            )
    except Exception as e:
        await message.answer(
            f"❌ 添加/修改活动失败：{e}", reply_to_message_id=message.message_id
        )


@admin_required
@rate_limit(rate=3, per=30)
async def cmd_delactivity(message: types.Message):
    """删除活动 - 优化版本"""
    args = message.text.split()
    if len(args) != 2:
        await message.answer(
            "❌ 用法：/delactivity <活动名>",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
        return
    act = args[1]
    if not await db.activity_exists(act):
        await message.answer(
            f"❌ 活动 <code>{act}</code> 不存在",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
            parse_mode="HTML",
        )
        return

    await db.delete_activity_config(act)
    await db.force_refresh_activity_cache()  # 确保缓存立即更新

    await message.answer(
        f"✅ 活动 <code>{act}</code> 已删除",
        reply_markup=await get_main_keyboard(chat_id=message.chat.id, show_admin=True),
        reply_to_message_id=message.message_id,
        parse_mode="HTML",
    )
    logger.info(f"删除活动: {act}")


# ========= 上下班指令 ========
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setworktime(message: types.Message):
    """设置上下班时间"""
    args = message.text.split()
    if len(args) != 3:
        await message.answer(
            "❌ 用法：/setworktime <上班时间> <下班时间>\n"
            "📝 示例：/setworktime 09:00 18:00\n"
            "💡 时间格式：HH:MM (24小时制)",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
        return

    try:
        work_start = args[1]
        work_end = args[2]

        # 验证时间格式

        time_pattern = re.compile(r"^([0-1]?[0-9]|2[0-3]):([0-5][0-9])$")

        if not time_pattern.match(work_start) or not time_pattern.match(work_end):
            await message.answer(
                "❌ 时间格式错误！请使用 HH:MM 格式（24小时制）\n"
                "📝 示例：09:00、18:30",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                reply_to_message_id=message.message_id,
            )
            return

        # 更新工作时间
        chat_id = message.chat.id
        await db.update_group_work_time(chat_id, work_start, work_end)

        await message.answer(
            f"✅ 上下班时间设置成功！\n\n"
            f"🟢 上班时间：<code>{work_start}</code>\n"
            f"🔴 下班时间：<code>{work_end}</code>\n\n"
            f"💡 上下班打卡功能已启用",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
            parse_mode="HTML",
        )

    except Exception as e:
        logger.error(f"设置工作时间失败: {e}")
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )


# ============= 白班重置命令 ==============
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setresettime(message: types.Message):
    """设置白班重置时间"""
    args = message.text.split()
    if len(args) != 3:
        await message.answer(
            "🔄 <b>白班重置时间设置</b>\n\n"
            "⏰ 用法：/setresettime <小时> <分钟>\n"
            "📝 示例：/setresettime 23 0\n\n"
            "💡 <b>白班重置特点：</b>\n"
            "• 重置白班用户的所有数据\n"
            "• 清理活动记录、工作记录、统计\n"
            "• 发生在23:00（默认）\n"
            "• 是硬重置，数据会导出到月度统计",
            parse_mode="HTML",
            reply_to_message_id=message.message_id,
        )
        return

    try:
        hour = int(args[1])
        minute = int(args[2])

        if 0 <= hour <= 23 and 0 <= minute <= 59:
            chat_id = message.chat.id
            await db.init_group(chat_id)
            await db.update_group_reset_time(chat_id, hour, minute)
            
            await message.answer(
                f"✅ <b>白班重置时间已设置</b>\n\n"
                f"⏰ 重置时间：<code>{hour:02d}:{minute:02d}</code>\n"
                f"📊 影响班次：白班（shift_id=0）\n\n"
                f"💡 <b>白班重置特点：</b>\n"
                f"• 重置所有用户的白班数据\n"
                f"• 清理活动、工作、统计记录\n"
                f"• 是硬重置，数据会保存到月度统计",
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=True
                ),
                parse_mode="HTML",
                reply_to_message_id=message.message_id,
            )
            logger.info(f"白班重置时间设置成功: 群组 {chat_id} -> {hour:02d}:{minute:02d}")
        else:
            await message.answer(
                "❌ 小时必须在0-23之间，分钟必须在0-59之间！\n"
                "💡 示例：/setresettime 23 0（晚上11点白班重置）",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                reply_to_message_id=message.message_id,
            )
    except ValueError:
        await message.answer(
            "❌ 请输入有效的数字！\n" "💡 示例：/setresettime 23 0",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
    except Exception as e:
        logger.error(f"设置白班重置时间失败: {e}")
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )

# ========== 设置夜班重置时间命令 ==========

@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setsoftresettime(message: types.Message):
    """设置夜班重置时间"""
    args = message.text.split()
    if len(args) != 3:
        await message.answer(
            "🔄 <b>夜班重置时间设置</b>\n\n"
            "⏰ 用法：/setsoftresettime <小时> <分钟>\n"
            "📝 示例：/setsoftresettime 11 0\n\n"
            "💡 <b>夜班重置特点：</b>\n"
            "• 重置夜班用户的活动状态\n"
            "• 只清理活动状态，保留统计数据\n"
            "• 发生在11:00（默认）\n"
            "• 是软重置，daily_statistics会导出后清理",
            parse_mode="HTML",
            reply_to_message_id=message.message_id,
        )
        return

    try:
        hour = int(args[1])
        minute = int(args[2])

        if 0 <= hour <= 23 and 0 <= minute <= 59:
            chat_id = message.chat.id
            await db.init_group(chat_id)
            await db.update_group_soft_reset_time(chat_id, hour, minute)
            
            await message.answer(
                f"✅ <b>夜班重置时间已设置</b>\n\n"
                f"⏰ 重置时间：<code>{hour:02d}:{minute:02d}</code>\n"
                f"📊 影响班次：夜班（shift_id=1）\n\n"
                f"💡 <b>夜班重置特点：</b>\n"
                f"• 重置夜班用户的活动状态\n"
                f"• 清理活动状态，保留统计\n"
                f"• 是软重置，daily_statistics会导出\n"
                f"• 设为 0 0 可禁用夜班重置",
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=True
                ),
                parse_mode="HTML",
                reply_to_message_id=message.message_id,
            )
            logger.info(f"夜班重置时间设置成功: 群组 {chat_id} -> {hour:02d}:{minute:02d}")
        else:
            await message.answer(
                "❌ 小时必须在0-23之间，分钟必须在0-59之间！\n"
                "💡 示例：/setsoftresettime 11 0（上午11点夜班重置）",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                reply_to_message_id=message.message_id,
            )
    except ValueError:
        await message.answer(
            "❌ 请输入有效的数字！\n" "💡 示例：/setsoftresettime 11 0",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
    except Exception as e:
        logger.error(f"设置夜班重置时间失败: {e}")
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )

@admin_required
@rate_limit(rate=5, per=60)
async def cmd_resettime(message: types.Message):
    """查看当前重置时间"""
    chat_id = message.chat.id
    try:
        group_data = await db.get_group_cached(chat_id)
        reset_hour = group_data.get("reset_hour", Config.DAILY_RESET_HOUR)
        reset_minute = group_data.get("reset_minute", Config.DAILY_RESET_MINUTE)

        await message.answer(
            f"⏰ 当前重置时间设置\n\n"
            f"🕒 重置时间：<code>{reset_hour:02d}:{reset_minute:02d}</code>\n"
            f"📅 每天此时自动重置用户数据\n\n"
            f"💡 使用 /setresettime <小时> <分钟> 修改",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            parse_mode="HTML",
            reply_to_message_id=message.message_id,
        )
    except Exception as e:
        logger.error(f"查看重置时间失败: {e}")
        await message.answer(
            f"❌ 获取重置时间失败：{e}", reply_to_message_id=message.message_id
        )


@admin_required
@rate_limit(rate=3, per=30)
async def cmd_delwork_clear(message: types.Message):
    """移除上下班功能并清除所有记录 - 优化版"""
    chat_id = message.chat.id

    # 检查功能状态
    if not await db.has_work_hours_enabled(chat_id):
        await message.answer(
            "❌ 当前群组没有设置上下班功能",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            reply_to_message_id=message.message_id,
        )
        return

    # 获取当前设置用于显示
    work_hours = await db.get_group_work_time(chat_id)
    old_start = work_hours.get("work_start")
    old_end = work_hours.get("work_end")

    await message.answer("⏳ 正在移除上下班功能并清除记录...")

    try:
        # 重置为默认时间（禁用功能）
        await db.update_group_work_time(
            chat_id,
            Config.DEFAULT_WORK_HOURS["work_start"],
            Config.DEFAULT_WORK_HOURS["work_end"],
        )

        # 🆕 改进：使用数据库的带重试方法
        records_cleared = 0
        try:
            result = await db.execute_with_retry(
                "清除工作记录", "DELETE FROM work_records WHERE chat_id = $1", chat_id
            )
            records_cleared = (
                int(result.split()[-1]) if result and result.startswith("DELETE") else 0
            )
        except Exception as e:
            logger.warning(f"清除工作记录时出现异常: {e}")
            # 不阻止主要功能，继续执行

        # 🆕 改进：同时清理月度统计中的工作数据
        try:
            await db.execute_with_retry(
                "清理月度工作统计",
                "DELETE FROM monthly_statistics WHERE chat_id = $1 AND activity_name IN ('work_days', 'work_hours')",
                chat_id,
            )
        except Exception as e:
            logger.warning(f"清理月度工作统计时出现异常: {e}")

        # 清理用户缓存确保立即生效
        await db.force_refresh_activity_cache()  # 🆕 强制刷新活动缓存
        db._cache.pop(f"group:{chat_id}", None)  # 🆕 清理群组缓存

        success_msg = (
            f"✅ <b>上下班功能已移除</b>\n\n"
            f"🗑️ <b>删除的设置：</b>\n"
            f"   • 上班时间: <code>{old_start}</code>\n"
            f"   • 下班时间: <code>{old_end}</code>\n"
            f"   • 清除记录: <code>{records_cleared}</code> 条\n\n"
            f"🔧 <b>当前状态：</b>\n"
            f"   • 上下班按钮已隐藏\n"
            f"   • 工作相关统计已重置\n"
            f"   • 可正常进行其他活动打卡\n\n"
            f"💡 如需重新启用，请使用 /setworktime 命令"
        )

        await message.answer(
            success_msg,
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            parse_mode="HTML",
            reply_to_message_id=message.message_id,
        )

        logger.info(
            f"👤 管理员 {message.from_user.id} 移除了群组 {chat_id} 的上下班功能，清除 {records_cleared} 条记录"
        )

    except Exception as e:
        logger.error(f"移除上下班功能失败: {e}")
        await message.answer(
            f"❌ 移除失败：{e}",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            reply_to_message_id=message.message_id,
        )


# ========= 绑定频道与群组命令 ==========
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setchannel(message: types.Message):
    """绑定提醒频道 - 优化版本"""
    chat_id = message.chat.id
    args = message.text.split(maxsplit=1)

    if len(args) < 2:
        await message.answer(
            Config.MESSAGES["setchannel_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
        return

    try:
        channel_id = int(args[1].strip())

        # 基本格式验证（确保是有效的频道ID格式）
        if channel_id > 0:
            await message.answer(
                "❌ 频道ID应该是负数格式（如 -100xxx）",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                reply_to_message_id=message.message_id,
            )
            return

        await db.init_group(chat_id)
        await db.update_group_channel(chat_id, channel_id)

        await message.answer(
            f"✅ 已绑定超时提醒推送频道：<code>{channel_id}</code>\n\n"
            f"💡 超时打卡和迟到/早退通知将推送到此频道\n"
            f"⚠️ 如果推送失败，请检查：\n"
            f"• 频道ID是否正确\n"
            f"• 机器人是否已加入频道\n"
            f"• 机器人是否有发送消息权限",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
            parse_mode="HTML",
        )

        logger.info(f"频道绑定成功: 群组 {chat_id} -> 频道 {channel_id}")

    except ValueError:
        await message.answer(
            "❌ 频道ID必须是数字格式\n" "💡 示例：/setchannel -1001234567890",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
    except Exception as e:
        logger.error(f"设置频道失败: {e}")
        await message.answer(
            f"❌ 绑定频道失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )


@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setgroup(message: types.Message):
    """绑定通知群组 - 优化版本"""
    chat_id = message.chat.id
    args = message.text.split(maxsplit=1)

    if len(args) < 2:
        await message.answer(
            Config.MESSAGES["setgroup_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
        return

    try:
        group_id = int(args[1].strip())
        await db.init_group(chat_id)
        await db.update_group_notification(chat_id, group_id)

        await message.answer(
            f"✅ 已绑定通知群组：<code>{group_id}</code>\n\n"
            f"💡 打卡通知将推送到此群组",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
            parse_mode="HTML",
        )

        logger.info(f"群组绑定成功: 主群组 {chat_id} -> 通知群组 {group_id}")

    except ValueError:
        await message.answer(
            "❌ 群组ID必须是数字格式",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
    except Exception as e:
        logger.error(f"设置群组失败: {e}")
        await message.answer(
            f"❌ 绑定群组失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )


# ========== 活动人数限制命令 =========
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_actnum(message: types.Message):
    """设置活动人数限制"""
    args = message.text.split()
    if len(args) != 3:
        await message.answer(
            "❌ 用法：/actnum <活动名> <人数限制>\n"
            "例如：/actnum 小厕 3\n"
            "💡 设置为0表示取消限制",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
        return

    try:
        activity = args[1]
        max_users = int(args[2])

        # 检查活动是否存在
        if not await db.activity_exists(activity):
            await message.answer(
                f"❌ 活动 '<code>{activity}</code>' 不存在！",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                reply_to_message_id=message.message_id,
                parse_mode="HTML",
            )
            return

        if max_users < 0:
            await message.answer(
                "❌ 人数限制不能为负数！",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
            )
            return

        chat_id = message.chat.id

        if max_users == 0:
            # 取消限制
            await db.remove_activity_user_limit(activity)
            await message.answer(
                f"✅ 已取消活动 '<code>{activity}</code>' 的人数限制",
                reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
                parse_mode="HTML",
                reply_to_message_id=message.message_id,
            )
            logger.info(f"取消活动人数限制: {activity}")
        else:
            # 设置限制
            await db.set_activity_user_limit(activity, max_users)

            # 获取当前活动人数
            current_users = await db.get_current_activity_users(chat_id, activity)

            await message.answer(
                f"✅ 已设置活动 '<code>{activity}</code>' 的人数限制为 <code>{max_users}</code> 人\n\n"
                f"📊 当前状态：\n"
                f"• 限制人数：<code>{max_users}</code> 人\n"
                f"• 当前进行：<code>{current_users}</code> 人\n"
                f"• 剩余名额：<code>{max_users - current_users}</code> 人",
                reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
                parse_mode="HTML",
                reply_to_message_id=message.message_id,
            )
            logger.info(f"设置活动人数限制: {activity} -> {max_users}人")

    except ValueError:
        await message.answer(
            "❌ 人数限制必须是数字！",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
    except Exception as e:
        logger.error(f"设置活动人数限制失败: {e}")
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )


@admin_required
@rate_limit(rate=5, per=60)
async def cmd_actstatus(message: types.Message):
    """查看活动人数状态"""
    chat_id = message.chat.id

    try:
        activity_limits = await db.get_all_activity_limits()

        if not activity_limits:
            await message.answer(
                "📊 当前没有设置任何活动人数限制\n"
                "💡 使用 /actnum <活动名> <人数> 来设置限制",
                reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
                reply_to_message_id=message.message_id,
            )
            return

        status_text = "📊 活动人数限制状态\n\n"

        for activity, max_users in activity_limits.items():
            current_users = await db.get_current_activity_users(chat_id, activity)
            remaining = max(0, max_users - current_users) if max_users > 0 else "无限制"

            status_icon = "🟢" if remaining == "无限制" or remaining > 0 else "🔴"
            limit_display = f"{max_users}" if max_users > 0 else "无限制"

            status_text += (
                f"{status_icon} <code>{activity}</code>\n"
                f"   • 限制：<code>{limit_display}</code>\n"
                f"   • 当前：<code>{current_users}</code> 人\n"
                f"   • 剩余：<code>{remaining}</code> 人\n\n"
            )

        status_text += "💡 绿色表示还有名额，红色表示已满员"

        await message.answer(
            status_text,
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            parse_mode="HTML",
            reply_to_message_id=message.message_id,
        )

        logger.info(f"查看活动状态: {chat_id}")

    except Exception as e:
        logger.error(f"获取活动状态失败: {e}")
        await message.answer(
            f"❌ 获取状态失败：{e}",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            reply_to_message_id=message.message_id,
        )


# ========== 罚款管理命令 ==========
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setfines_all(message: types.Message):
    """为所有活动统一设置分段罚款 - 优化版本"""
    args = message.text.split()
    if len(args) < 3 or (len(args) - 1) % 2 != 0:
        await message.answer(
            Config.MESSAGES["setfines_all_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
        return

    try:
        pairs = args[1:]
        segments = {}
        for i in range(0, len(pairs), 2):
            t = int(pairs[i])
            f = int(pairs[i + 1])
            if t <= 0 or f < 0:
                await message.answer(
                    "❌ 时间段必须为正整数，罚款金额不能为负数",
                    reply_markup=await get_main_keyboard(
                        chat_id=message.chat.id, show_admin=True
                    ),
                    reply_to_message_id=message.message_id,
                )
                return
            segments[t] = f

        activity_limits = await db.get_activity_limits_cached()
        if not activity_limits:
            await message.answer(
                "⚠️ 当前没有活动，无法设置罚款",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                reply_to_message_id=message.message_id,
            )
            return

        for act in activity_limits.keys():
            for time_segment, amount in segments.items():
                await db.update_fine_config(act, str(time_segment), amount)

        segments_text = " ".join(
            [f"<code>{t}</code>:<code>{f}</code>" for t, f in segments.items()]
        )
        await message.answer(
            f"✅ 已为所有活动设置分段罚款：{segments_text}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
            parse_mode="HTML",
        )

        logger.info(f"群 {message.chat.id} 已统一设置所有活动罚款: {segments_text}")

    except ValueError:
        await message.answer(
            "❌ 时间段和金额必须是数字！",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
    except Exception as e:
        logger.error(f"设置所有活动罚款失败: {e}")
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )


@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setfine(message: types.Message):
    """设置单个活动的罚款费率"""
    args = message.text.split()
    if len(args) != 4:
        await message.answer(
            Config.MESSAGES["setfine_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
        return

    try:
        activity = args[1]
        time_segment = int(args[2])
        amount = int(args[3])

        if not await db.activity_exists(activity):
            await message.answer(
                f"❌ 活动 '<code>{activity}</code>' 不存在！",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                reply_to_message_id=message.message_id,
                parse_mode="HTML",
            )
            return

        if time_segment <= 0 or amount < 0:
            await message.answer(
                "❌ 时间段必须为正整数，罚款金额不能为负数",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                reply_to_message_id=message.message_id,
            )
            return

        await db.update_fine_config(activity, str(time_segment), amount)

        await message.answer(
            f"✅ 已设置活动 '<code>{activity}</code>' 的罚款：\n"
            f"⏱️ 时间段：<code>{time_segment}</code>\n"
            f"💰 金额：<code>{amount}</code> 元",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
            parse_mode="HTML",
        )

        logger.info(
            f"群 {message.chat.id} 已设置活动罚款: {activity} {time_segment} -> {amount}元"
        )

    except ValueError:
        await message.answer(
            "❌ 时间段和金额必须是数字！",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
    except Exception as e:
        logger.error(f"设置单个活动罚款失败: {e}")
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )


@admin_required
@rate_limit(rate=5, per=60)
async def cmd_finesstatus(message: types.Message):
    """查看所有活动的罚款设置状态"""
    chat_id = message.chat.id
    try:
        # 获取所有活动和罚款配置
        activity_limits = await db.get_activity_limits_cached()
        fine_rates = await db.get_fine_rates()

        if not activity_limits:
            await message.answer(
                "⚠️ 当前没有配置任何活动",
                reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
                reply_to_message_id=message.message_id,
            )
            return

        status_text = "💰 活动罚款设置状态\n\n"

        for activity in activity_limits.keys():
            activity_fines = fine_rates.get(activity, {})
            status_text += f"🔹 <code>{activity}</code>\n"

            if activity_fines:
                # 按时间段排序
                for time_seg, amount in sorted(
                    activity_fines.items(), key=lambda x: int(x[0])
                ):
                    status_text += f"   • 时间段 <code>{time_seg}</code> 分钟：<code>{amount}</code> 元\n"
            else:
                status_text += f"   • 未设置罚款\n"

            status_text += "\n"

        status_text += "💡 设置命令：\n"
        status_text += "• /setfine <活动> <时间> <金额> - 设置单个活动\n"
        status_text += "• /setfines_all <t1> <f1> [t2 f2...] - 统一设置所有活动"

        await message.answer(
            status_text,
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            reply_to_message_id=message.message_id,
            parse_mode="HTML",
        )

        logger.info(f"群 {chat_id} 查看了活动罚款状态")

    except Exception as e:
        logger.error(f"查看罚款状态失败: {e}")
        await message.answer(
            f"❌ 获取罚款状态失败：{e}",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            reply_to_message_id=message.message_id,
        )


# =========== 上下班罚款指令 ===========
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setworkfine(message: types.Message):
    """
    设置上下班罚款规则
    用法：
    /setworkfine work_start 1 100 10 200 30 500
    表示：
        迟到1分钟以上罚100，
        迟到10分钟以上罚200，
        迟到30分钟以上罚500
    """
    args = message.text.split()

    # 1. 检查参数长度和格式
    if len(args) < 4 or (len(args) - 2) % 2 != 0:
        await message.answer(
            "❌ 用法错误\n正确格式：/setworkfine <work_start|work_end> <分钟1> <罚款1> [分钟2 罚款2 ...]",
            reply_markup=get_admin_keyboard(),  # 已移除 await
            reply_to_message_id=message.message_id,
        )
        return

    checkin_type = args[1]
    if checkin_type not in ["work_start", "work_end"]:
        await message.answer(
            "❌ 类型必须是 work_start 或 work_end",
            reply_markup=get_admin_keyboard(),  # 已移除 await
            reply_to_message_id=message.message_id,
        )
        return

    # 2. 解析分钟阈值和罚款金额
    fine_segments = {}
    try:
        for i in range(2, len(args), 2):
            minute = int(args[i])
            amount = int(args[i + 1])
            if minute <= 0 or amount < 0:
                await message.answer(
                    "❌ 分钟必须大于0，罚款金额不能为负数",
                    reply_markup=get_admin_keyboard(),  # 已移除 await
                    reply_to_message_id=message.message_id,
                )
                return
            fine_segments[str(minute)] = amount

        # 3. 更新数据库配置（重写整个罚款配置）
        await db.clear_work_fine_rates(checkin_type)
        for minute_str, fine_amount in fine_segments.items():
            await db.update_work_fine_rate(checkin_type, minute_str, fine_amount)

        # 4. 生成反馈文本
        segments_text = "\n".join(
            [
                f"⏰ 超过 {m} 分钟 → 💰 {a} 元"
                for m, a in sorted(fine_segments.items(), key=lambda x: int(x[0]))
            ]
        )

        type_text = "上班迟到" if checkin_type == "work_start" else "下班早退"

        await message.answer(
            f"✅ 已设置{type_text}罚款规则：\n{segments_text}",
            reply_markup=get_admin_keyboard(),  # 已移除 await
            reply_to_message_id=message.message_id,
        )

        logger.info(f"设置上下班罚款成功: {checkin_type} -> {fine_segments}")

    except ValueError:
        await message.answer(
            "❌ 分钟和罚款必须是数字",
            reply_markup=get_admin_keyboard(),  # 已移除 await
            reply_to_message_id=message.message_id,
        )
    except Exception as e:
        logger.error(f"设置上下班罚款失败: {e}")
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=get_admin_keyboard(),  # 已移除 await
            reply_to_message_id=message.message_id,
        )


@admin_required
@rate_limit(rate=5, per=60)
async def cmd_showsettings(message: types.Message):
    """显示目前的设置 - 优化版本"""
    chat_id = message.chat.id
    await db.init_group(chat_id)
    group_data = await db.get_group_cached(chat_id) or {}

    activity_limits = await db.get_activity_limits_cached()
    fine_rates = await db.get_fine_rates()
    work_fine_rates = await db.get_work_fine_rates()

    # 生成输出文本
    text = f"🔧 当前群设置（群ID {chat_id}）\n\n"

    # 基本设置
    text += "📋 基本设置：\n"
    text += f"• 绑定频道ID: <code>{group_data.get('channel_id', '未设置')}</code>\n"
    text += f"• 通知群组ID: <code>{group_data.get('notification_group_id', '未设置')}</code>\n\n"

    # 重置与上下班时间
    text += "⏰ 重置与工作时间：\n"
    text += f"• 每日重置时间: <code>{group_data.get('reset_hour', Config.DAILY_RESET_HOUR):02d}:{group_data.get('reset_minute', Config.DAILY_RESET_MINUTE):02d}</code>\n"
    text += f"• 上班时间: <code>{group_data.get('work_start_time', '09:00')}</code>\n"
    text += f"• 下班时间: <code>{group_data.get('work_end_time', '18:00')}</code>\n\n"

    # 活动设置
    text += "🎯 活动设置：\n"
    if activity_limits:
        for act, v in activity_limits.items():
            text += f"• <code>{act}</code>：次数上限 <code>{v['max_times']}</code>，时间限制 <code>{v['time_limit']}</code> 分钟\n"
    else:
        text += "• 暂无活动设置\n"

    # 活动罚款设置
    text += "\n💰 活动罚款分段：\n"
    if fine_rates:
        for act, fr in fine_rates.items():
            if fr:
                try:
                    sorted_fines = sorted(
                        fr.items(), key=lambda x: int(x[0].replace("min", ""))
                    )
                    fines_text = " | ".join([f"{k}:{v}元" for k, v in sorted_fines])
                    text += f"• <code>{act}</code>：{fines_text}\n"
                except Exception:
                    text += f"• <code>{act}</code>：配置异常\n"
            else:
                text += f"• <code>{act}</code>：未设置\n"
    else:
        text += "• 暂无活动罚款设置\n"

    # 上下班罚款
    text += "\n⏰ 上下班罚款设置：\n"
    for key, label in [("work_start", "上班迟到"), ("work_end", "下班早退")]:
        wf = work_fine_rates.get(key, {})
        if wf:
            try:
                sorted_wf = sorted(wf.items(), key=lambda x: int(x[0]))
                wf_text = " | ".join([f"{k}分:{v}元" for k, v in sorted_wf])
                text += f"• {label}：{wf_text}\n"
            except Exception:
                text += f"• {label}：配置异常\n"
        else:
            text += f"• {label}：未设置\n"

    await message.answer(
        text,
        reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
        parse_mode="HTML",
        reply_to_message_id=message.message_id,
    )


# ========== 查看工作时间命令 =========
@admin_required
@rate_limit(rate=5, per=60)
async def cmd_worktime(message: types.Message):
    """查看当前工作时间设置"""
    chat_id = message.chat.id
    try:
        work_hours = await db.get_group_work_time(chat_id) or {}
        has_enabled = await db.has_work_hours_enabled(chat_id)

        work_start = work_hours.get("work_start", "09:00")
        work_end = work_hours.get("work_end", "18:00")
        status = "🟢 已启用" if has_enabled else "🔴 未启用（使用默认时间）"

        await message.answer(
            f"🕒 当前工作时间设置\n\n"
            f"📊 状态：{status}\n"
            f"🟢 上班时间：<code>{work_start}</code>\n"
            f"🔴 下班时间：<code>{work_end}</code>\n\n"
            f"💡 使用 /setworktime 09:00 18:00 来修改",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            reply_to_message_id=message.message_id,
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error(f"查看工作时间失败: {e}")
        await message.answer(
            "❌ 获取工作时间失败，请稍后重试",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            reply_to_message_id=message.message_id,
        )


# ========== 按钮处理 ==========
@rate_limit(rate=10, per=60)
async def handle_back_command(message: types.Message):
    """处理回座命令"""
    await process_back(message)


@rate_limit(rate=5, per=60)
async def handle_work_buttons(message: types.Message):
    """处理上下班按钮"""
    chat_id = message.chat.id
    uid = message.from_user.id
    text = message.text.strip()

    # 🎯 新增检查：是否启用了上下班功能
    if not await db.has_work_hours_enabled(chat_id):
        await message.answer(
            "❌ 本群组尚未启用上下班打卡功能\n\n" "👑 请联系管理员设置上下班时间",
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(uid)
            ),
            reply_to_message_id=message.message_id,
        )
        return

    if text == "🟢 上班":
        await process_work_checkin(message, "work_start")
    elif text == "🔴 下班":
        await process_work_checkin(message, "work_end")


@admin_required
@rate_limit(rate=2, per=60)
@track_performance("handle_export_button")
async def handle_export_button(message: types.Message):
    """处理导出数据按钮"""
    chat_id = message.chat.id
    await message.answer(
        "⏳ 正在导出数据，请稍候...", reply_to_message_id=message.message_id
    )
    try:
        await export_and_push_csv(chat_id)
        await message.answer(
            "✅ 数据已导出并推送！", reply_to_message_id=message.message_id
        )
    except Exception as e:
        await message.answer(
            f"❌ 导出失败：{e}", reply_to_message_id=message.message_id
        )


@rate_limit(rate=10, per=60)
@track_performance("handle_my_record")
async def handle_my_record(message: types.Message):
    """处理我的记录按钮"""
    chat_id = message.chat.id
    uid = message.from_user.id

    user_lock = user_lock_manager.get_lock(chat_id, uid)
    async with user_lock:
        await show_history(message)


@rate_limit(rate=10, per=60)
@track_performance("handle_rank")
async def handle_rank(message: types.Message):
    """处理排行榜按钮"""
    chat_id = message.chat.id
    uid = message.from_user.id

    user_lock = user_lock_manager.get_lock(chat_id, uid)
    async with user_lock:
        await show_rank(message)


@rate_limit(rate=5, per=60)
async def handle_admin_panel_button(message: types.Message):
    """处理管理员面板按钮 - 简洁手机版"""
    if not await is_admin(message.from_user.id):
        markup = await get_main_keyboard(chat_id=message.chat.id, show_admin=False)
        await message.answer(
            Config.MESSAGES["no_permission"],
            reply_markup=markup,
            reply_to_message_id=message.message_id,
            parse_mode=None,
        )
        return

    admin_text = (
        "👑 *管理员面板*\n"
        "━━━━━━━━━━━━━━━━\n\n"
        "📢 *频道与推送*\n"
        "├ `/setchannel` \\[ID\\]\n"
        "├ `/setgroup` \\[ID\\]\n"
        "├ `/setpush` \\[目标\\] \\[开关\\]\n"
        "├ `/showpush`\n"
        "│ 目标: ch\\|gr\\|ad\n"
        "│ 开关: on\\|off\n\n"
        "🎯 *活动管理*\n"
        "├ `/addactivity` \\[名\\] \\[次\\] \\[分\\]\n"
        "├ `/delactivity` \\[名\\]\n"
        "├ `/actnum` \\[名\\] \\[人数\\]\n"
        "└ `/actstatus`\n\n"
        "💰 *罚款管理*\n"
        "├ `/setfine` \\[名\\] \\[段\\] \\[元\\]\n"
        "├ `/setfines\\_all` \\[段1\\] \\[元1\\] \\.\\.\\.\n"
        "├ `/setworkfine` \\[类型\\] \\[分\\] \\[元\\]\n"
        "└ `/finesstatus`\n"
        "  类型: start\\|end\n\n"
        "🔄 *重置设置*\n"
        "├ `/setresettime` \\[时\\] \\[分\\]\n"
        "├ `/setsoftresettime` \\[时\\] \\[分\\]\n"
        "├ `/resetuser` \\[用户ID\\]\n"
        "└ `/resettime`\n\n"
        "⏰ *上下班管理*\n"
        "├ `/setworktime` \\[上\\] \\[下\\]\n"
        "├ `/worktime`\n"
        "├ `/delwork`\n"
        "└ `/delwork\\_clear`\n\n"
        "📊 *数据管理*\n"
        "├ `/export`\n"
        "├ `/exportmonthly` \\[年\\] \\[月\\]\n"
        "├ `/monthlyreport` \\[年\\] \\[月\\]\n"
        "├ `/cleanup\\_monthly` \\[年\\] \\[月\\]\n"
        "├ `/monthly\\_stats\\_status`\n"
        "└ `/cleanup\\_inactive` \\[天\\]\n\n"
        "💾 *数据显示*\n"
        "└ `/showsettings`\n\n"
        "━━━━━━━━━━━━━━━━\n"
        "_💡 提示：发送 /help \\[命令\\] 查看详情_"
    )

    await message.answer(
        admin_text,
        reply_markup=get_admin_keyboard(),
        reply_to_message_id=message.message_id,
        parse_mode="MarkdownV2",
    )


# ========== 返回主菜单按钮处理 ==========
@rate_limit(rate=5, per=60)
async def handle_back_to_main_menu(message: types.Message):
    """处理返回主菜单按钮"""
    chat_id = message.chat.id
    uid = message.from_user.id

    logger.info(f"用户 {uid} 点击了返回主菜单按钮")

    await message.answer(
        "📋 主菜单",
        reply_markup=await get_main_keyboard(
            chat_id=chat_id, show_admin=await is_admin(uid)
        ),
        reply_to_message_id=message.message_id,
    )
    logger.info(f"已为用户 {uid} 返回主菜单")


# ========== 动态活动按钮处理 ==========
@rate_limit(rate=10, per=60)
async def handle_all_text_messages(message: types.Message):
    """统一处理所有文本消息"""
    text = message.text.strip()
    chat_id = message.chat.id
    uid = message.from_user.id

    # 如果是特殊按钮，直接返回让专门的处理程序处理
    if text in SPECIAL_BUTTONS:
        logger.debug(f"特殊按钮被点击: {text} - 用户 {uid}")
        return

    # 检查是否是活动按钮
    try:
        activity_limits = await db.get_activity_limits_cached()
        if text in activity_limits.keys():
            logger.info(f"活动按钮点击: {text} - 用户 {uid}")
            await start_activity(message, text)
            return
    except Exception as e:
        logger.error(f"处理活动按钮时出错: {e}")

    # 如果不是活动按钮，显示帮助信息
    await message.answer(
        "请使用下方按钮或直接输入活动名称进行操作：\n\n"
        "📝 使用方法：\n"
        "• 点击活动按钮开始打卡\n"
        "• 输入'回座'或点击'✅ 回座'按钮结束当前活动\n"
        "• 点击'📊 我的记录'查看个人统计\n"
        "• 点击'🏆 排行榜'查看群内排名",
        reply_markup=await get_main_keyboard(
            chat_id=chat_id, show_admin=await is_admin(uid)
        ),
        reply_to_message_id=message.message_id,
        parse_mode="HTML",
    )


# ========== 固定活动命令处理器 ==========
@rate_limit(rate=10, per=60)
@message_deduplicate
@with_retry("fixed_activity", max_retries=2)
@track_performance("fixed_activity")
async def handle_fixed_activity(message: types.Message):
    """处理固定活动命令（支持带用户名格式）"""
    # 获取完整的命令文本
    command_text = message.text.strip()
    logger.info(f"🔍 收到命令: {command_text}")

    # 定义活动映射
    activity_map = {
        "/wc": "小厕",
        "/bigwc": "大厕",
        "/eat": "吃饭",
        "/smoke": "抽烟或休息",
        "/rest": "休息",
    }

    # 1. 检查纯命令（如 /wc）
    if command_text in activity_map:
        act = activity_map[command_text]
        logger.info(f"✅ 匹配到纯命令: {command_text} -> {act}")
        await start_activity(message, act)
        return

    # 2. 检查带用户名的命令（如 /wc@dh188_bot）
    for cmd, act in activity_map.items():
        if command_text.startswith(cmd + "@"):
            logger.info(f"✅ 匹配到带用户名命令: {command_text} -> {act}")
            await start_activity(message, act)
            return

    # 3. 都不是，让其他处理器处理
    logger.warning(f"❌ 未匹配的命令: {command_text}")


# ========== 用户功能 ==========
async def show_history(message: types.Message):
    """显示用户历史记录 - 统一业务周期版本（兼容软/硬重置）"""

    chat_id = message.chat.id
    uid = message.from_user.id

    # 🧠 获取业务日期
    business_date = await db.get_business_date(chat_id)

    # 读取重置时间设置用于显示
    group_data = await db.get_group_cached(chat_id)
    reset_hour = group_data.get("reset_hour", Config.DAILY_RESET_HOUR)
    reset_minute = group_data.get("reset_minute", Config.DAILY_RESET_MINUTE)

    await db.init_group(chat_id)
    await db.init_user(chat_id, uid)

    # 🔹 获取群组配置判断是否双班模式
    group_config = await db.get_group_shift_config(chat_id)
    is_dual_mode = group_config.get('dual_mode', False)

    # 🔹 智能确定班次ID
    # 1. 先尝试从用户状态获取
    user_status = await db.get_user_status(chat_id, uid)
    shift_id = 0  # 默认白班
    
    if user_status and user_status.get('on_duty_shift') is not None:
        # 用户已经有确定的班次
        shift_id = user_status['on_duty_shift']
    elif is_dual_mode:
        # 双班模式但用户还没有上班打卡，根据当前时间智能判断班次
        try:
            now = datetime.now(beijing_tz)
            # 使用 utils 中的函数判断班次
            shift_id = await determine_activity_shift_id(chat_id, uid, now, db)
        except Exception as e:
            logger.error(f"智能判断班次失败: {e}")
            # 降级：使用白班时间窗口判断
            try:
                from utils import is_time_in_day_shift
                if is_time_in_day_shift(now, group_config['day_start'], group_config['day_end']):
                    shift_id = 0  # 白班
                else:
                    shift_id = 1  # 夜班
            except Exception:
                shift_id = 0  # 默认白班
    # 单班模式保持 shift_id = 0

    # 🔹 用户基础信息（只用于展示昵称 & 罚款等字段）
    user_data = await db.get_user_cached(chat_id, uid)

    # 根据班次获取用户活动数据
    user_activities = await db.get_user_activities_by_shift(chat_id, uid, shift_id)

    if not user_data:
        await message.answer(
            "暂无记录，请先进行打卡活动",
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(uid)
            ),
            reply_to_message_id=message.message_id,
        )
        return

    # 确定班次名称显示
    if is_dual_mode:
        shift_name = "白班☀️" if shift_id == 0 else "夜班🌙"
    else:
        shift_name = "单班"

    # 🎯 构建标题行：如果是双班模式显示班次图标，单班模式不显示
    if is_dual_mode:
        first_line = f"👤 {shift_name}：{MessageFormatter.format_user_link(uid, user_data['nickname'])}"
    else:
        first_line = f"👤 {MessageFormatter.format_user_link(uid, user_data['nickname'])}"

    text = (
        f"{first_line}\n"
        f"📅 统计周期：<code>{business_date.strftime('%Y-%m-%d')}</code>\n"
        f"⏰ 重置时间：{reset_hour:02d}:{reset_minute:02d}\n"
    )
    
    # 如果是双班模式，在标题中添加班次信息
    if is_dual_mode:
        text += f"📊 当前班次记录（{shift_name}）：\n\n"
    else:
        text += f"📊 当前周期记录：\n\n"

    has_records = False
    activity_limits = await db.get_activity_limits_cached()

    # 🧮 从 user_activities 表获取权威数据（按班次过滤）
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT activity_name, activity_count, accumulated_time
            FROM user_activities
            WHERE chat_id = $1 AND user_id = $2 AND activity_date = $3 AND shift_id = $4
            """,
            chat_id,
            uid,
            business_date,
            shift_id,
        )

    user_activities_dict = {}
    total_time_all = 0
    total_count_all = 0

    for row in rows:
        user_activities_dict[row["activity_name"]] = {
            "count": row["activity_count"],
            "time": row["accumulated_time"],
        }
        total_time_all += row["accumulated_time"]
        total_count_all += row["activity_count"]

    # 🧾 输出活动明细（只显示当前班次的数据）
    for act in activity_limits.keys():
        activity_info = user_activities_dict.get(act, {})
        total_time = activity_info.get("time", 0)
        count = activity_info.get("count", 0)
        max_times = activity_limits[act]["max_times"]

        if total_time > 0 or count > 0:
            status = "✅" if count < max_times else "❌"
            time_str = MessageFormatter.format_time(int(total_time))
            text += (
                f"• <code>{act}</code>：<code>{time_str}</code>，"
                f"次数：<code>{count}</code>/<code>{max_times}</code> {status}\n"
            )
            has_records = True

    # 🧮 汇总统计（来自当前班次的流水数据）
    if total_time_all > 0 or total_count_all > 0:
        text += f"\n📈 当前班次统计：\n"
        text += f"• 累计时间：<code>{MessageFormatter.format_time(int(total_time_all))}</code>\n"
        text += f"• 活动次数：<code>{total_count_all}</code> 次\n"
    else:
        text += "\n📊 当前班次暂无活动记录\n"

    # 💰 罚款显示仍来自 users 表（因为罚款是跨周期累计的）
    total_fine = user_data.get("total_fines", 0)
    if total_fine > 0:
        text += f"• 累计罚款：<code>{total_fine}</code> 元"

    # 如果是双班模式，添加切换班次提示
    if is_dual_mode and not has_records:
        other_shift_id = 1 if shift_id == 0 else 0
        other_shift_name = "夜班🌙" if shift_id == 0 else "白班☀️"
        
        # 检查另一个班次是否有数据
        try:
            async with db.pool.acquire() as conn:
                other_rows = await conn.fetch(
                    """
                    SELECT COUNT(*) as count
                    FROM user_activities
                    WHERE chat_id = $1 AND user_id = $2 AND activity_date = $3 AND shift_id = $4
                    """,
                    chat_id,
                    uid,
                    business_date,
                    other_shift_id,
                )
                
                other_count = other_rows[0]["count"] if other_rows else 0
                if other_count > 0:
                    text += f"\n\n💡 提示：您可能在{other_shift_name}有活动记录"
        except Exception:
            pass

    # 如果没有任何记录
    if not has_records and total_count_all == 0 and total_fine == 0:
        if is_dual_mode:
            text += "\n💪 开始第一个活动吧！"
        else:
            text += "\n💪 开始第一个活动吧！"

    await message.answer(
        text,
        reply_markup=await get_main_keyboard(
            chat_id=chat_id, show_admin=await is_admin(uid)
        ),
        reply_to_message_id=message.message_id,
        parse_mode="HTML",
    )


async def show_rank(message: types.Message):
    """显示排行榜 - 支持单/双班模式，从数据库获取聚合数据"""
    chat_id = message.chat.id
    uid = message.from_user.id

    await db.init_group(chat_id)
    activity_limits = await db.get_activity_limits_cached()

    if not activity_limits:
        await message.answer(
            "⚠️ 当前没有配置任何活动，无法生成排行榜。",
            reply_to_message_id=message.message_id,
        )
        return

    # 🧠 获取业务日期和群组配置
    business_date = await db.get_business_date(chat_id)
    group_data = await db.get_group_cached(chat_id)
    
    # 读取重置配置和班次模式
    reset_hour = group_data.get("reset_hour", Config.DAILY_RESET_HOUR)
    reset_minute = group_data.get("reset_minute", Config.DAILY_RESET_MINUTE)
    
    # 获取班次配置 (假设你的 db 有这个方法)
    group_config = await db.get_group_shift_config(chat_id)
    is_dual_mode = group_config.get('dual_mode', False)

    # 1. 构建标题部分
    mode_title = "（双班模式）" if is_dual_mode else ""
    rank_text = (
        f"🏆 当前周期活动排行榜{mode_title}\n"
        f"📅 统计周期：<code>{business_date.strftime('%Y-%m-%d')}</code>\n"
        f"⏰ 重置时间：<code>{reset_hour:02d}:{reset_minute:02d}</code>\n\n"
    )

    found_any_data = False
    
    # 2. 确定需要遍历的班次
    # 单班模式只查 shift_id=0 (白班)，双班模式查 0 和 1
    shifts_to_show = [(0, "白班")] if not is_dual_mode else [(0, "白班☀️"), (1, "夜班🌙")]

    for shift_id, shift_label in shifts_to_show:
        if is_dual_mode:
            rank_text += f"📊 <b>{shift_label}</b>\n"
            
        shift_has_data = False
        
        for act in activity_limits.keys():
            try:
                # 🎯 核心修改：在 SQL 中增加 shift_id 的过滤
                rows = await db.fetch_with_retry(
                    f"获取{shift_label}-{act}排行榜",
                    """
                    SELECT 
                        ua.user_id,
                        u.nickname,
                        SUM(ua.accumulated_time) as total_time,
                        SUM(ua.activity_count) as total_count,
                        CASE 
                            WHEN u.current_activity = $1 
                            THEN TRUE 
                            ELSE FALSE 
                        END as is_active
                    FROM user_activities ua
                    LEFT JOIN users u ON ua.chat_id = u.chat_id AND ua.user_id = u.user_id
                    WHERE ua.chat_id = $2 
                    AND ua.activity_date = $3 
                    AND ua.activity_name = $4
                    AND ua.shift_id = $5
                    GROUP BY ua.user_id, u.nickname, u.current_activity
                    HAVING SUM(ua.accumulated_time) > 0 OR u.current_activity = $1
                    ORDER BY total_time DESC
                    LIMIT 10
                    """,
                    act,
                    chat_id,
                    business_date,
                    act,
                    shift_id,
                )

                if rows:
                    found_any_data = True
                    shift_has_data = True
                    rank_text += f"📈 <code>{act}</code>：\n"

                    for i, row in enumerate(rows, 1):
                        user_id = row["user_id"]
                        nickname = row["nickname"] or f"用户{user_id}"
                        total_time = row["total_time"] or 0
                        count = row["total_count"] or 0
                        is_active = row["is_active"]

                        if is_active:
                            rank_text += f"  <code>{i}.</code> 🟡 {MessageFormatter.format_user_link(user_id, nickname)} - 进行中\n"
                        elif total_time > 0:
                            time_str = MessageFormatter.format_time(int(total_time))
                            rank_text += f"  <code>{i}.</code> 🟢 {MessageFormatter.format_user_link(user_id, nickname)} - {time_str} ({count}次)\n"
                    
                    rank_text += "\n"

            except Exception as e:
                logger.error(f"查询班次 {shift_id} 活动 {act} 排行榜失败: {e}")
                continue
        
        if is_dual_mode and not shift_has_data:
            rank_text += "  暂无记录\n\n"

    # 3. 如果全程没数据，显示空提示
    if not found_any_data:
        rank_text = (
            f"🏆 当前周期活动排行榜\n"
            f"📅 统计周期：<code>{business_date.strftime('%Y-%m-%d')}</code>\n"
            f"⏰ 重置时间：<code>{reset_hour:02d}:{reset_minute:02d}</code>\n\n"
            f"📊 当前周期还没有活动记录\n"
            f"💪 开始第一个活动吧！\n\n"
            f"💡 提示：开始活动后会立即显示在这里"
        )

    # 4. 发送消息
    await message.answer(
        rank_text,
        reply_markup=await get_main_keyboard(chat_id, await is_admin(uid)),
        parse_mode="HTML",
        reply_to_message_id=message.message_id,
    )

# ========== 快速回座回调 ==========
async def handle_quick_back(callback_query: types.CallbackQuery):
    """处理快速回座按钮"""
    try:
        data_parts = callback_query.data.split(":")
        if len(data_parts) < 3:
            await callback_query.answer("❌ 数据格式错误", show_alert=True)
            return

        chat_id = int(data_parts[1])
        uid = int(data_parts[2])

        # 检查消息是否过期
        msg_ts = callback_query.message.date.timestamp()
        if time.time() - msg_ts > 600:
            await callback_query.answer(
                "⚠️ 此按钮已过期，请重新输入 /回座", show_alert=True
            )
            return

        # 检查是否是用户本人点击
        if callback_query.from_user.id != uid:
            await callback_query.answer("❌ 这不是您的回座按钮！", show_alert=True)
            return

        # 执行回座逻辑
        user_lock = user_lock_manager.get_lock(chat_id, uid)
        async with user_lock:
            user_data = await db.get_user_cached(chat_id, uid)
            if not user_data or not user_data.get("current_activity"):
                await callback_query.answer("❌ 您当前没有活动在进行", show_alert=True)
                return

            await _process_back_locked(callback_query.message, chat_id, uid)

        # 更新按钮状态
        try:
            await callback_query.message.edit_reply_markup(reply_markup=None)
        except Exception as e:
            logger.warning(f"无法更新按钮状态: {e}")

        await callback_query.answer("✅ 已成功回座")

    except Exception as e:
        logger.error(f"快速回座失败: {e}")
        try:
            await callback_query.answer(
                "❌ 回座失败，请手动输入 /回座", show_alert=True
            )
        except Exception:
            pass


# ========== 日常数据导出处理函数 =========
async def get_group_stats_from_monthly(chat_id: int, target_date: date) -> List[Dict]:
    """从月度统计表获取群组统计数据（用于重置后导出）"""
    try:
        # 获取目标日期对应的月份
        month_start = target_date.replace(day=1)

        logger.info(
            f"🔍 从月度表查询数据: 群组{chat_id}, 日期{target_date}, 月份{month_start}"
        )

        # 从月度表获取数据
        monthly_stats = await db.get_monthly_statistics(
            chat_id, month_start.year, month_start.month
        )

        if not monthly_stats:
            logger.warning(f"⚠️ 月度表中没有找到 {month_start} 的数据")
            return []

        result = []
        for stat in monthly_stats:
            # 🆕 调试日志：检查工作相关字段
            logger.debug(
                f"📊 用户 {stat['user_id']} 工作数据: "
                f"工作天数={stat.get('work_days', 0)}, "
                f"工作时长={stat.get('work_hours', 0)}秒"
            )

            user_data = {
                "user_id": stat["user_id"],
                "nickname": stat.get("nickname", f"用户{stat['user_id']}"),
                "total_accumulated_time": stat.get("total_accumulated_time", 0),
                "total_activity_count": stat.get("total_activity_count", 0),
                "total_fines": stat.get("total_fines", 0),
                "overtime_count": stat.get("overtime_count", 0),
                "total_overtime_time": stat.get("total_overtime_time", 0),
                "work_days": stat.get("work_days", 0),  # 🆕 新增工作天数
                "work_hours": stat.get("work_hours", 0),  # 🆕 新增工作时长
                "activities": stat.get("activities", {}),
            }

            result.append(user_data)

        logger.info(
            f"✅ 从月度表成功获取 {target_date} 的数据，共 {len(result)} 个用户"
        )
        return result

    except Exception as e:
        logger.error(f"❌ 从月度表获取数据失败: {e}")
        return []


# ========== 数据导出功能 ==========
async def export_and_push_csv(
    chat_id: int,
    to_admin_if_no_group: bool = True,
    file_name: str = None,
    target_date=None,
    is_daily_reset: bool = False,
    from_monthly_table: bool = False,
    is_night_shift_export: bool = False,  # 🆕 新增：夜班重置导出标记
) -> bool:
    """
    导出群组数据为 CSV 并推送 - 终极完整整合版（支持智能业务周期）
    返回: True/False 表示导出是否成功
    """
    # ========== 0. 前置检查 ==========
    try:
        if not await db._ensure_healthy_connection():
            logger.error(f"❌ 数据库连接不健康，无法导出 {chat_id}")
            return False
        if not bot or not hasattr(bot, "send_document"):
            logger.error(f"❌ Bot不可用，无法导出 {chat_id}")
            return False
    except Exception as e:
        logger.error(f"❌ 前置检查失败 {chat_id}: {e}")
        logger.error(traceback.format_exc())
        return False

    # ========== 1. 性能监控开始 ==========
    start_time = time.time()
    operation_id = f"export_{chat_id}_{int(start_time)}"
    logger.info(f"🚀 [{operation_id}] 开始导出群组 {chat_id} 的数据...")

    # 初始化变量
    temp_file = None
    group_stats = []
    activity_limits = Config.DEFAULT_ACTIVITY_LIMITS.copy()
    
    # 🆕 新增：业务周期信息变量
    business_period_info = ""
    export_type_info = "手动导出"
    period_start = None
    period_end = None

    try:
        # 初始化群组
        await db.init_group(chat_id)

        # ========== 2. 安全转换函数 ==========
        def safe_int(value, default=0):
            """安全转换为整数"""
            if value is None:
                return default
            try:
                if isinstance(value, str):
                    if value.isdigit():
                        return int(value)
                    elif value.replace(".", "", 1).isdigit():
                        return int(float(value))
                    else:
                        return default
                return int(value)
            except (ValueError, TypeError):
                return default

        def safe_format_time(seconds):
            """安全格式化时间"""
            try:
                return MessageFormatter.format_time_for_csv(safe_int(seconds))
            except Exception:
                return "0分0秒"

        # ========== 3. 智能计算导出日期和业务周期 ==========
        beijing_now = datetime.now(beijing_tz)

        # 🆕 新增：夜班重置导出的智能计算
        if is_night_shift_export:
            export_type_info = "夜班重置导出"
            
            # 计算昨天的业务周期
            yesterday_dt = beijing_now - timedelta(days=1)
            
            try:
                # 获取昨天的业务日期
                yesterday_business_date = await db.get_business_date(chat_id, yesterday_dt)
                logger.info(f"📅 昨天业务日期: {yesterday_business_date}")
                
                # 获取昨天业务周期的完整时间范围
                try:
                    # 🆕 新增：尝试使用新的 get_business_period 方法
                    period_start, period_end = await db.get_business_period(chat_id, yesterday_business_date)
                    
                    business_period_info = (
                        f"📊 <b>业务周期详情</b>\n"
                        f"• 业务日期：<code>{yesterday_business_date.strftime('%Y-%m-%d')}</code>\n"
                        f"• 周期范围：<code>{period_start.strftime('%m/%d %H:%M')}</code> - "
                        f"<code>{period_end.strftime('%m/%d %H:%M')}</code>\n"
                        f"• 周期时长：<code>{(period_end - period_start).total_seconds() / 3600:.1f}</code> 小时"
                    )
                    
                    logger.info(f"📅 昨天业务周期: {period_start} - {period_end}")
                    
                except AttributeError as e:
                    # 如果数据库没有 get_business_period 方法，降级处理
                    logger.warning(f"⚠️ 无法获取详细业务周期: {e}")
                    business_period_info = (
                        f"📊 <b>业务周期详情</b>\n"
                        f"• 业务日期：<code>{yesterday_business_date.strftime('%Y-%m-%d')}</code>\n"
                        f"• 周期类型：自动计算（基于群组配置）"
                    )
                
                target_date = yesterday_business_date
                
                # 🆕 获取群组配置信息
                try:
                    group_config = await db.get_group_shift_config(chat_id)
                    work_hours = await db.get_group_work_time(chat_id)
                    has_work_hours = await db.has_work_hours_enabled(chat_id)
                    
                    if group_config.get('dual_mode', False):
                        day_start = group_config.get('day_start', '09:00')
                        day_end = group_config.get('day_end', '21:00')
                        config_info = f"双班模式 ({day_start}-{day_end})"
                    elif has_work_hours:
                        work_start = work_hours.get("work_start", "09:00")
                        work_end = work_hours.get("work_end", "18:00")
                        config_info = f"上下班模式 ({work_start}-{work_end})"
                    else:
                        soft_hour, soft_minute = await db.get_group_soft_reset_time(chat_id)
                        config_info = f"默认模式 (重置时间: {soft_hour:02d}:{soft_minute:02d})"
                    
                    business_period_info += f"\n• 群组配置：{config_info}"
                    
                except Exception as e:
                    logger.warning(f"⚠️ 获取群组配置失败: {e}")
                    config_info = "默认配置"
                
            except Exception as e:
                logger.error(f"❌ 计算昨天业务周期失败: {e}")
                # 降级处理：使用昨天的日期
                target_date = yesterday_dt.date()
                business_period_info = f"📅 <b>业务日期</b>：<code>{target_date.strftime('%Y-%m-%d')}</code>"
                
        # 处理 target_date 参数
        elif target_date is not None:
            if hasattr(target_date, "date"):
                target_date = target_date.date()
            elif not isinstance(target_date, date):
                try:
                    if isinstance(target_date, str):
                        target_date = datetime.strptime(target_date, "%Y-%m-%d").date()
                except Exception as e:
                    logger.warning(f"⚠️ 无法解析target_date: {target_date}, 错误: {e}")
                    target_date = None
        
        # 默认：使用当前业务日期
        if target_date is None:
            target_date = await db.get_business_date(chat_id)
        
        # 🆕 判断导出类型
        if is_daily_reset and not is_night_shift_export:
            export_type_info = "日常重置导出"
        elif is_daily_reset:
            export_type_info = "夜班重置导出"

        # ========== 4. 生成智能文件名 ==========
        if not file_name:
            if is_night_shift_export:
                # 夜班重置导出：包含配置信息
                try:
                    group_config = await db.get_group_shift_config(chat_id)
                    if group_config.get('dual_mode', False):
                        shift_suffix = "dual"
                    elif await db.has_work_hours_enabled(chat_id):
                        shift_suffix = "work"
                    else:
                        shift_suffix = "default"
                    
                    file_name = f"night_export_{chat_id}_{target_date.strftime('%Y%m%d')}_{shift_suffix}.csv"
                    
                except Exception as e:
                    logger.warning(f"⚠️ 生成智能文件名失败: {e}")
                    file_name = f"night_export_{chat_id}_{target_date.strftime('%Y%m%d')}.csv"
                    
            elif is_daily_reset:
                # 日常重置导出
                file_name = f"daily_backup_{chat_id}_{target_date.strftime('%Y%m%d')}.csv"
            else:
                # 手动导出
                file_name = f"manual_export_{chat_id}_{beijing_now.strftime('%Y%m%d_%H%M%S')}.csv"

        logger.info(
            f"📊 [{operation_id}] 导出配置:\n"
            f"   群组: {chat_id}\n"
            f"   导出类型: {export_type_info}\n"
            f"   目标日期: {target_date}\n"
            f"   文件名: {file_name}"
        )

        # ========== 5. 获取统计数据 ==========
        logger.info(
            f"🔍 [{operation_id}] 获取群组 {chat_id} 的统计数据，日期: {target_date}"
        )

        if from_monthly_table:
            logger.info(f"📊 [{operation_id}] 尝试从月度表获取数据")
            try:
                group_stats = await get_group_stats_from_monthly(chat_id, target_date)
                if group_stats:
                    logger.info(
                        f"✅ [{operation_id}] 从月度表获取到 {len(group_stats)} 条数据"
                    )
                    activity_limits = Config.DEFAULT_ACTIVITY_LIMITS.copy()
                else:
                    logger.warning(f"⚠️ [{operation_id}] 月度表无数据，回退到常规表")
                    from_monthly_table = False
            except Exception as e:
                logger.error(f"❌ [{operation_id}] 从月度表获取数据失败: {e}")
                logger.error(traceback.format_exc())
                from_monthly_table = False

        if not from_monthly_table:
            try:
                # 并发获取活动配置和统计数据
                activity_task = asyncio.create_task(db.get_activity_limits_cached())
                stats_task = asyncio.create_task(
                    db.get_group_statistics(chat_id, target_date)
                )

                results = await asyncio.gather(
                    activity_task, stats_task, return_exceptions=True
                )

                # 处理活动配置结果
                if isinstance(results[0], Exception):
                    logger.error(f"❌ [{operation_id}] 获取活动配置失败: {results[0]}")
                    try:
                        activity_limits = await db.get_activity_limits()
                        if not activity_limits:
                            activity_limits = Config.DEFAULT_ACTIVITY_LIMITS.copy()
                    except Exception as e:
                        logger.error(f"❌ [{operation_id}] 获取活动配置回退失败: {e}")
                        activity_limits = Config.DEFAULT_ACTIVITY_LIMITS.copy()
                elif results[0]:
                    activity_limits = results[0]
                else:
                    logger.warning(f"⚠️ [{operation_id}] 活动配置为空，使用默认配置")
                    activity_limits = Config.DEFAULT_ACTIVITY_LIMITS.copy()

                # 处理统计数据结果
                if isinstance(results[1], Exception):
                    logger.error(f"❌ [{operation_id}] 获取统计数据失败: {results[1]}")
                    group_stats = []
                elif results[1]:
                    group_stats = results[1]
                else:
                    group_stats = []

            except Exception as e:
                logger.error(f"❌ [{operation_id}] 并发获取数据失败: {e}")
                logger.error(traceback.format_exc())
                # 回退到顺序获取
                try:
                    activity_limits = await db.get_activity_limits_cached()
                    if not activity_limits:
                        activity_limits = await db.get_activity_limits()
                    group_stats = await db.get_group_statistics(chat_id, target_date)
                except Exception as inner_e:
                    logger.error(f"❌ [{operation_id}] 回退获取数据也失败: {inner_e}")
                    activity_limits = Config.DEFAULT_ACTIVITY_LIMITS.copy()
                    group_stats = []

        # 最终验证
        if not activity_limits:
            logger.warning(f"⚠️ [{operation_id}] 没有活动配置，使用默认配置")
            activity_limits = Config.DEFAULT_ACTIVITY_LIMITS.copy()

        if not group_stats or not isinstance(group_stats, list):
            logger.warning(f"⚠️ [{operation_id}] 获取统计数据为空或不是列表")
            group_stats = []

        logger.info(f"📊 [{operation_id}] 获取到 {len(group_stats)} 条统计数据")

        # ========== 6. 数据验证 ==========
        if len(group_stats) == 0:
            logger.warning(f"⚠️ [{operation_id}] 群组 {chat_id} 没有数据需要导出")
            if not is_daily_reset:
                try:
                    no_data_msg = Config.MESSAGES.get(
                        "no_data_to_export", "⚠️ 当前没有数据需要导出"
                    )
                    await bot.send_message(chat_id, no_data_msg)
                except Exception as e:
                    logger.debug(f"[{operation_id}] 发送无数据消息失败: {e}")
            return True  # 没有数据也算成功

        # ========== 7. 构造CSV表头 ==========
        csv_buffer = StringIO()
        writer = csv.writer(csv_buffer)

        headers = ["用户ID", "用户昵称", "重置类型"]
        activity_names = sorted(activity_limits.keys())
        for act in activity_names:
            headers.extend([f"{act}次数", f"{act}总时长"])

        headers.extend(
            [
                "活动次数总计",
                "活动用时总计",
                "罚款总金额",
                "超时次数",
                "总超时时间",
                "工作天数",
                "工作时长",
            ]
        )

        writer.writerow(headers)

        # ========== 8. 数据处理和统计 ==========
        unique_users = set()
        total_records = 0
        reset_type_counts = {"A班": 0, "B班": 0}
        has_valid_data = False

        for idx, user_data in enumerate(group_stats):
            if not isinstance(user_data, dict):
                logger.warning(f"⚠️ [{operation_id}] 跳过第 {idx} 条非字典数据")
                continue

            total_records += 1

            # 判定 A/B 班
            is_soft_reset = user_data.get("is_soft_reset", False)
            reset_type = "B班" if is_soft_reset else "A班"
            reset_type_counts[reset_type] += 1

            # 统计独立用户
            user_id = user_data.get("user_id")
            if user_id:
                unique_users.add(str(user_id))

            # 安全获取活动数据
            user_activities = user_data.get("activities", {})
            if not isinstance(user_activities, dict):
                user_activities = {}

            # 检查是否有有效数据
            total_activity_count = safe_int(user_data.get("total_activity_count"))
            total_accumulated_time = safe_int(user_data.get("total_accumulated_time"))
            total_fines = safe_int(user_data.get("total_fines"))

            if (
                total_activity_count > 0
                or total_accumulated_time > 0
                or total_fines > 0
            ):
                has_valid_data = True

            # 构建行数据
            row = [
                user_data.get("user_id", "未知"),
                user_data.get("nickname", "未知用户"),
                reset_type,
            ]

            # 按排序后的活动名填充数据
            for act in activity_names:
                activity_info = user_activities.get(act, {})
                if not isinstance(activity_info, dict):
                    activity_info = {}

                count = safe_int(activity_info.get("count"))
                time_seconds = safe_int(activity_info.get("time"))

                row.append(count)
                row.append(safe_format_time(time_seconds))

            # 填充通用统计数据
            overtime_count = safe_int(user_data.get("overtime_count"))
            total_overtime_time = safe_int(user_data.get("total_overtime_time"))
            work_days = safe_int(user_data.get("work_days", 0))
            work_hours = safe_int(user_data.get("work_hours", 0))

            row.extend(
                [
                    total_activity_count,
                    safe_format_time(total_accumulated_time),
                    total_fines,
                    overtime_count,
                    safe_format_time(total_overtime_time),
                    work_days,
                    safe_format_time(work_hours),
                ]
            )

            writer.writerow(row)

        # ========== 9. 最终数据验证 ==========
        if not has_valid_data and total_records == 0:
            logger.warning(f"⚠️ [{operation_id}] 群组 {chat_id} 没有有效数据需要导出")
            if not is_daily_reset:
                try:
                    no_data_msg = Config.MESSAGES.get(
                        "no_data_to_export", "⚠️ 当前没有数据需要导出"
                    )
                    await bot.send_message(chat_id, no_data_msg)
                except Exception as e:
                    logger.debug(f"[{operation_id}] 发送无数据消息失败: {e}")
            return True

        # ========== 10. 生成CSV文件 ==========
        csv_content = csv_buffer.getvalue()
        csv_buffer.close()

        # 创建临时文件名
        temp_file = f"temp_{operation_id}_{file_name}"

        # ========== 11. 并行执行文件操作 ==========
        async def write_file_async():
            """异步写入文件"""
            try:
                async with aiofiles.open(temp_file, "w", encoding="utf-8-sig") as f:
                    await f.write(csv_content)
                logger.info(
                    f"✅ [{operation_id}] CSV文件已生成: {temp_file}, 大小: {len(csv_content)} 字节"
                )
                return True
            except Exception as e:
                logger.error(f"❌ [{operation_id}] 异步写入文件失败: {e}")
                logger.error(traceback.format_exc())
                # 回退到同步写入
                try:
                    with open(temp_file, "w", encoding="utf-8-sig") as f:
                        f.write(csv_content)
                    logger.info(f"✅ [{operation_id}] 同步写入文件成功")
                    return True
                except Exception as sync_e:
                    logger.error(f"❌ [{operation_id}] 同步写入文件也失败: {sync_e}")
                    logger.error(traceback.format_exc())
                    return False

        async def get_chat_title_async():
            """异步获取群组标题"""
            try:
                chat_info = await bot.get_chat(chat_id)
                return chat_info.title or f"群组 {chat_id}"
            except Exception as e:
                logger.debug(f"[{operation_id}] 获取群组标题失败: {e}")
                return f"群组 {chat_id}"

        # 并发执行
        write_result, chat_title = await asyncio.gather(
            write_file_async(), get_chat_title_async()
        )

        if not write_result:
            # 文件写入失败
            try:
                error_msg = Config.MESSAGES.get(
                    "export_process_failed", "❌ 导出过程失败"
                )
                await bot.send_message(chat_id, f"{error_msg}\n错误: 文件写入失败")
            except Exception as msg_e:
                logger.debug(f"[{operation_id}] 发送错误消息失败: {msg_e}")
            return False

        # ========== 12. 构建富文本描述 ==========
        display_date = target_date.strftime("%Y年%m月%d日")
        
        # 使用 MessageFormatter 的工具方法
        try:
            if hasattr(MessageFormatter, "create_dashed_line"):
                dashed_line = MessageFormatter.create_dashed_line()
            else:
                dashed_line = "─" * 30
        except Exception:
            dashed_line = "─" * 30

        # 🆕 构建智能描述
        caption = (
            f"📊 <b>数据导出报告</b>\n"
            f"🏢 群组：<code>{chat_title}</code>\n"
            f"📅 统计日期：<code>{display_date}</code>\n"
            f"⏰ 导出时间：<code>{beijing_now.strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
            f"🔁 导出类型：<code>{export_type_info}</code>\n"
            f"{dashed_line}\n"
        )
        
        # 🆕 添加业务周期信息
        if business_period_info:
            caption += f"{business_period_info}\n{dashed_line}\n"
        
        caption += f"💾 <i>包含每个用户每日的活动统计及工作时长</i>"

        # ========== 13. 发送到当前群组 ==========
        input_file = FSInputFile(temp_file, filename=file_name)
        send_to_group_success = False
        send_to_admin_success = False

        try:
            await bot.send_document(
                chat_id=chat_id,
                document=input_file,
                caption=caption,
                parse_mode="HTML",
                reply_to_message_id=None,
            )
            send_to_group_success = True
            logger.info(f"✅ [{operation_id}] CSV文件已发送到群组 {chat_id}")
        except Exception as e:
            logger.error(f"❌ [{operation_id}] 发送到群组失败: {e}")
            logger.error(traceback.format_exc())

            # 尝试发送错误消息到群组
            try:
                error_msg = Config.MESSAGES.get(
                    "export_failed", "❌ 数据导出失败，请稍后重试"
                )
                await bot.send_message(chat_id, f"{error_msg}\n错误: {str(e)[:100]}")
            except Exception as msg_e:
                logger.debug(f"[{operation_id}] 发送错误消息失败: {msg_e}")

        # ========== 14. 推送到通知服务 ==========
        if to_admin_if_no_group and notification_service:
            try:
                # 确保通知服务有正确的实例
                if (
                    hasattr(notification_service, "bot_manager")
                    and not notification_service.bot_manager
                    and bot_manager
                ):
                    notification_service.bot_manager = bot_manager
                if (
                    hasattr(notification_service, "bot")
                    and not notification_service.bot
                    and bot
                ):
                    notification_service.bot = bot

                # 调用通知服务
                if hasattr(notification_service, "send_document"):
                    await notification_service.send_document(
                        chat_id, input_file, caption=caption
                    )
                    send_to_admin_success = True
                    logger.info(f"✅ [{operation_id}] 数据已推送到通知服务")
                else:
                    logger.warning(
                        f"⚠️ [{operation_id}] 通知服务没有 send_document 方法"
                    )
            except Exception as e:
                logger.warning(f"⚠️ [{operation_id}] 推送到通知服务失败: {e}")

        # ========== 15. 后台清理 ==========
        async def cleanup_background():
            """后台清理临时文件"""
            try:
                await asyncio.sleep(2)

                if temp_file and os.path.exists(temp_file):
                    os.remove(temp_file)
                    logger.debug(f"🧹 [{operation_id}] 已清理临时文件: {temp_file}")
                elif temp_file:
                    logger.debug(f"🧹 [{operation_id}] 临时文件不存在: {temp_file}")
            except Exception as e:
                logger.debug(f"🧹 [{operation_id}] 清理临时文件失败: {e}")

        # 在后台执行清理任务
        asyncio.create_task(cleanup_background())

        # ========== 16. 性能统计和日志 ==========
        duration = time.time() - start_time
        logger.info(
            f"✅ [{operation_id}] 数据导出处理完成\n"
            f"   文件: {file_name}\n"
            f"   导出类型: {export_type_info}\n"
            f"   用户数: {len(unique_users)}, 数据行: {total_records}\n"
            f"   A班: {reset_type_counts['A班']}, B班: {reset_type_counts['B班']}\n"
            f"   耗时: {duration:.2f}秒\n"
            f"   发送结果: 群组={send_to_group_success}, 通知服务={send_to_admin_success}"
        )

        return send_to_group_success

    except Exception as e:
        logger.error(f"❌ [{operation_id}] 导出过程发生未捕获异常: {e}")
        logger.error(traceback.format_exc())

        # 尝试发送错误消息
        try:
            error_msg = Config.MESSAGES.get(
                "export_failed", "❌ 数据导出失败，请稍后重试"
            )
            await bot.send_message(chat_id, f"{error_msg}\n错误: {str(e)[:100]}")
        except Exception as msg_e:
            logger.debug(f"[{operation_id}] 发送错误消息失败: {msg_e}")

        # 清理临时文件
        try:
            if temp_file and os.path.exists(temp_file):
                os.remove(temp_file)
                logger.debug(f"🧹 [{operation_id}] 异常时清理临时文件: {temp_file}")
        except Exception as cleanup_e:
            logger.debug(f"[{operation_id}] 异常时清理文件失败: {cleanup_e}")

        return False

# ========== 定时任务 ==========


async def daily_reset_task():
    """每日自动重置任务 - 性能优化与高可用版"""
    logger.info("🚀 每日重置监控任务已启动")

    # 限制同时处理的群组数量，防止 IO 阻塞
    sem = asyncio.Semaphore(10)

    async def process_single_group(chat_id, now):
        async with sem:
            try:
                group_data = await db.get_group_cached(chat_id)
                # 允许每个群组自定义重置小时，默认为配置值
                reset_hour = group_data.get("reset_hour", Config.DAILY_RESET_HOUR)

                # 1. 幂等性检查 (Key 包含日期和小时)
                reset_flag_key = f"last_reset:{chat_id}:{now.strftime('%Y%m%d')}"
                if global_cache.get(reset_flag_key) == now.hour:
                    return

                # 2. 触发判断
                if now.hour != reset_hour:
                    return

                logger.info(f"⏰ 群组 {chat_id} 开始重置...")

                # 3. 计算业务日期
                # 凌晨重置通常是为了结算昨天的账单/数据
                business_date = (
                    now.date() if now.hour >= 12 else (now - timedelta(days=1)).date()
                )
                file_name = f"backup_{chat_id}_{business_date.strftime('%Y%m%d')}.csv"

                # 4. 执行核心任务流
                # 导出备份
                try:
                    await export_and_push_csv(
                        chat_id, target_date=business_date, file_name=file_name
                    )
                except Exception as e:
                    logger.error(f"群组 {chat_id} 备份失败(跳过备份继续重置): {e}")

                # 业务数据操作 (建议封装成事务)
                completion_result = (
                    await db.complete_all_pending_activities_before_reset(chat_id, now)
                )
                await db.force_reset_all_users_in_group(
                    chat_id, target_date=business_date
                )

                # 清理定时器
                if hasattr(timer_manager, "cancel_all_timers_for_group"):
                    await timer_manager.cancel_all_timers_for_group(chat_id)

                # 5. 发送通知
                try:

                    await send_reset_notification(chat_id, completion_result, now)
                except Exception as e:
                    logger.error(f"群组 {chat_id} 通知发送失败: {e}")

                # 6. 成功后标记 (TTL 设为 24 小时更安全)
                global_cache.set(reset_flag_key, now.hour, ttl=86400)
                logger.info(f"✅ 群组 {chat_id} 重置完成")

            except Exception as e:
                logger.error(f"❌ 处理群组 {chat_id} 严重失败: {e}")

    while True:
        try:
            now = datetime.now(beijing_tz)
            all_groups = await db.get_all_groups()

            # 并发处理所有群组，但受 Semaphore 控制
            tasks = [process_single_group(cid, now) for cid in all_groups]
            await asyncio.gather(*tasks)

        except Exception as e:
            logger.error(f"❌ daily_reset_task 循环主逻辑出错: {e}")

        # 建议检查频率：如果重置任务很多，30-60秒是合理的
        await asyncio.sleep(60)


# ========== 软重置定时任务 ==========
async def soft_reset_task():
    """
    每日软重置任务 - 只重置 users 表累计字段，保留 user_activities
    ✅ 每群每天只执行一次
    """
    executed_cache: dict[int, date] = {}  # 记录每个群最后一次软重置业务日期

    while True:
        now = datetime.now(beijing_tz)
        logger.debug(f"软重置任务检查，当前时间: {now}")

        try:
            all_groups = await db.get_all_groups()
        except Exception as e:
            logger.error(f"获取群组列表失败: {e}")
            await asyncio.sleep(60)
            continue

        for chat_id in all_groups:
            try:
                # 获取软重置时间
                soft_hour, soft_minute = await db.get_group_soft_reset_time(chat_id)

                # 未设置软重置，跳过
                if soft_hour == 0 and soft_minute == 0:
                    continue

                # 业务日期
                today = await db.get_business_date(chat_id)

                # 每群每天只执行一次
                if executed_cache.get(chat_id) == today:
                    continue

                # 判断是否到达软重置时间
                if now.hour == soft_hour and now.minute == soft_minute:
                    logger.info(
                        f"群组 {chat_id} 到达软重置时间 {soft_hour:02d}:{soft_minute:02d}，开始处理..."
                    )

                    # 获取群组成员
                    group_members = await db.get_group_members(chat_id)
                    reset_count = 0

                    for user_data in group_members:
                        user_lock = user_lock_manager.get_lock(
                            chat_id, user_data["user_id"]
                        )
                        async with user_lock:
                            success = await db.reset_shift_data(
                                chat_id, user_data["user_id"]
                            )
                            if success:
                                reset_count += 1

                    # 查询当日活动记录数量
                    async with db.pool.acquire() as conn:
                        activity_count = await conn.fetchval(
                            """
                            SELECT COUNT(*) FROM user_activities
                            WHERE chat_id = $1 AND activity_date = $2
                            """,
                            chat_id,
                            today,
                        )

                    # 取消定时器
                    cancelled_count = 0
                    try:
                        if hasattr(timer_manager, "cancel_all_timers_for_group"):
                            cancelled_count = (
                                await timer_manager.cancel_all_timers_for_group(chat_id)
                            )
                    except Exception as e:
                        logger.error(f"取消定时器失败 {chat_id}: {e}")

                    # 发送通知
                    notification_text = (
                        f"🔄 <b>软重置完成</b>\n"
                        f"🏢 群组: <code>{chat_id}</code>\n"
                        f"⏰ 重置时间: <code>{soft_hour:02d}:{soft_minute:02d}</code>\n"
                        f"👥 重置用户: <code>{reset_count}</code> 人\n"
                        f"📊 保留活动记录: <code>{activity_count}</code> 条\n"
                        f"⏱️ 取消定时器: <code>{cancelled_count}</code> 个\n\n"
                        f"💡 软重置特点：\n"
                        f"• 只清除了展示/累计字段（打卡次数、我的记录）\n"
                        f"• 保留 {activity_count} 条历史活动记录\n"
                        f"• 用户可以重新打卡，历史数据已安全保存"
                    )
                    try:
                        await notification_service.send_notification(
                            chat_id, notification_text
                        )
                    except Exception as e:
                        logger.error(f"发送软重置通知失败: {e}")

                    # 标记已执行
                    executed_cache[chat_id] = today
                    logger.info(f"✅ 群组 {chat_id} 软重置完成")

            except Exception as e:
                logger.error(f"处理群组 {chat_id} 软重置失败: {e}")

        # 每分钟检查一次
        await asyncio.sleep(60)


async def memory_cleanup_task():
    """定期内存清理任务"""
    while True:
        try:
            await asyncio.sleep(Config.CLEANUP_INTERVAL)
            await performance_optimizer.memory_cleanup()
            logger.debug("定期内存清理任务完成")
        except Exception as e:
            logger.error(f"内存清理任务失败: {e}")
            await asyncio.sleep(300)


async def health_monitoring_task():
    """健康监控任务"""
    while True:
        try:
            # 检查内存使用
            if not performance_optimizer.memory_usage_ok():
                logger.warning("内存使用过高，执行紧急清理")
                await performance_optimizer.memory_cleanup()

            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"健康监控任务失败: {e}")
            await asyncio.sleep(60)


# ========== Web服务器 ==========
async def health_check(request):
    """增强版健康检查接口"""
    try:
        # 检查数据库连接
        db_healthy = await db.health_check()

        # 检查Bot状态
        bot_healthy = (
            bot_manager.is_healthy() if hasattr(bot_manager, "is_healthy") else True
        )

        # 检查内存状态
        memory_ok = performance_optimizer.memory_usage_ok()

        status = "healthy" if all([db_healthy, bot_healthy, memory_ok]) else "degraded"

        return web.json_response(
            {
                "status": status,
                "timestamp": time.time(),
                "services": {
                    "database": db_healthy,
                    "bot": bot_healthy,
                    "memory": memory_ok,
                },
                "version": "1.0",
                "environment": os.environ.get("BOT_MODE", "polling"),
            }
        )
    except Exception as e:
        logger.error(f"健康检查失败: {e}")
        return web.json_response(
            {"status": "unhealthy", "error": str(e), "timestamp": time.time()},
            status=500,
        )


async def start_health_server():
    """优化后的健康检查服务器 - 解决 404 并保留完整功能"""
    port = int(os.getenv("PORT", 10000))
    app = web.Application()

    # 1. 根路径处理函数
    async def root_handle(request):
        return web.Response(text="Bot is running!", status=200)

    # 2. 绑定路由 (核心修复)
    app.router.add_get("/", root_handle)
    # 完美对接 keepalive_loop 的请求路径
    app.router.add_get("/health", health_check)

    runner = web.AppRunner(app)
    await runner.setup()

    # 监听 0.0.0.0 确保外部可穿透
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"✅ 健康检查服务器已在端口 {port} 启动: / 和 /health")
    return site


# ========== 服务初始化 ==========
async def initialize_services():
    """初始化所有服务 - 最终完整版"""
    logger.info("🔄 初始化服务...")

    try:
        # 1. 初始化数据库
        await db.initialize()
        logger.info("✅ 数据库初始化完成")

        # 2. 启动数据库连接维护任务
        await db.start_connection_maintenance()
        logger.info("✅ 数据库维护任务已启动")

        # 3. 初始化Bot管理器
        await bot_manager.initialize()
        logger.info("✅ Bot管理器初始化完成")

        # 4. 重新获取初始化的bot和dispatcher
        global bot, dp
        bot = bot_manager.bot
        dp = bot_manager.dispatcher

        # 🎯 关键：验证 bot 和 bot_manager 是否真的初始化了
        global notification_service
        notification_service = NotificationService(bot_manager=bot_manager)
        notification_service.bot = bot

        # 5. 🎯 核心修复：双重设置 NotificationService
        notification_service.bot_manager = bot_manager
        notification_service.bot = bot  # 直接使用上面获取的 bot 实例

        # 🎯 验证设置是否成功
        if not notification_service.bot_manager:
            logger.error("❌ notification_service.bot_manager 设置失败")
        if not notification_service.bot:
            logger.error("❌ notification_service.bot 设置失败")

        logger.info("✅ 通知服务配置完成")

        # 6. 设置定时器回调
        timer_manager.set_activity_timer_callback(activity_timer)
        logger.info("✅ 定时器管理器配置完成")

        # 7. 初始化心跳管理器
        await heartbeat_manager.initialize()
        logger.info("✅ 心跳管理器初始化完成")

        # 8. 启动Bot健康监控
        await bot_manager.start_health_monitor()
        logger.info("✅ Bot健康监控已启动")

        # 9. 注册日志中间件
        dp.message.middleware(LoggingMiddleware())
        logger.info("✅ 日志中间件已注册")

        # 10. 注册所有消息处理器
        await register_handlers()
        logger.info("✅ 消息处理器注册完成")

        # 11. 恢复过期活动
        recovered_count = await recover_expired_activities()
        logger.info(f"✅ 过期活动恢复完成: {recovered_count} 个活动已处理")

        # 12. 🎯 最终健康检查
        health_status = await check_services_health()
        if all(health_status.values()):
            logger.info("🎉 所有服务初始化完成且健康")
        else:
            logger.warning(f"⚠️ 服务初始化完成但有警告: {health_status}")

    except Exception as e:
        logger.error(f"❌ 服务初始化失败: {e}")
        # 🎯 记录详细的调试信息
        logger.error(f"调试信息 - bot: {bot}, bot_manager: {bot_manager}")
        logger.error(
            f"调试信息 - notification_service.bot_manager: {getattr(notification_service, 'bot_manager', '未设置')}"
        )
        logger.error(
            f"调试信息 - notification_service.bot: {getattr(notification_service, 'bot', '未设置')}"
        )
        raise


async def check_services_health():
    """完整的服务健康检查"""

    health_status = {
        "database": await db.health_check(),
        "bot_manager_exists": bot_manager is not None,
        "bot_manager_has_bot": hasattr(bot_manager, "bot") if bot_manager else False,
        "bot_instance": bot is not None,
        "notification_service_bot_manager": notification_service.bot_manager
        is not None,
        "notification_service_bot": notification_service.bot is not None,
        "notification_service_has_methods": all(
            hasattr(notification_service, attr)
            for attr in ["_last_notification_time", "_rate_limit_window"]
        ),
        "timestamp": time.time(),
    }

    # 记录详细的健康状态
    healthy_services = [k for k, v in health_status.items() if v]
    unhealthy_services = [
        k for k, v in health_status.items() if not v and k != "timestamp"
    ]

    if unhealthy_services:
        logger.warning(f"⚠️ 不健康服务: {unhealthy_services}")
    else:
        logger.info(f"✅ 所有服务健康: {healthy_services}")

    return health_status


async def register_handlers():
    """注册所有消息处理器"""
    # 命令处理器
    dp.message.register(cmd_start, Command("start"))
    dp.message.register(cmd_menu, Command("menu"))
    dp.message.register(cmd_help, Command("help"))
    dp.message.register(cmd_ci, Command("ci"))
    dp.message.register(cmd_at, Command("at"))
    dp.message.register(cmd_workstart, Command("workstart"))
    dp.message.register(cmd_workend, Command("workend"))
    dp.message.register(cmd_admin, Command("admin"))

    # ========== 固定活动命令处理器 ==========
    dp.message.register(handle_fixed_activity, Command("wc"))
    dp.message.register(handle_fixed_activity, Command("bigwc"))
    dp.message.register(handle_fixed_activity, Command("eat"))
    dp.message.register(handle_fixed_activity, Command("smoke"))
    dp.message.register(handle_fixed_activity, Command("rest"))
    dp.message.register(handle_myinfo_command, Command("myinfo"))
    dp.message.register(handle_ranking_command, Command("ranking"))
    # =======================================

    dp.message.register(cmd_export, Command("export"))
    dp.message.register(cmd_monthlyreport, Command("monthlyreport"))
    dp.message.register(cmd_exportmonthly, Command("exportmonthly"))
    dp.message.register(cmd_addactivity, Command("addactivity"))
    dp.message.register(cmd_delactivity, Command("delactivity"))
    dp.message.register(cmd_setworktime, Command("setworktime"))
    dp.message.register(cmd_setresettime, Command("setresettime"))
    dp.message.register(cmd_resettime, Command("resettime"))
    dp.message.register(cmd_setchannel, Command("setchannel"))
    dp.message.register(cmd_setgroup, Command("setgroup"))
    dp.message.register(cmd_actnum, Command("actnum"))
    dp.message.register(cmd_actstatus, Command("actstatus"))
    dp.message.register(cmd_setfines_all, Command("setfines_all"))
    dp.message.register(cmd_setfine, Command("setfine"))
    dp.message.register(cmd_finesstatus, Command("finesstatus"))
    dp.message.register(cmd_setworkfine, Command("setworkfine"))
    dp.message.register(cmd_showsettings, Command("showsettings"))
    dp.message.register(cmd_worktime, Command("worktime"))
    dp.message.register(cmd_delwork_clear, Command("delwork_clear"))
    dp.message.register(cmd_cleanup_monthly, Command("cleanup_monthly"))
    dp.message.register(cmd_monthly_stats_status, Command("monthly_stats_status"))
    dp.message.register(cmd_cleanup_inactive, Command("cleanup_inactive"))
    dp.message.register(cmd_reset_user, Command("resetuser"))
    dp.message.register(cmd_setsoftresettime, Command("setsoftresettime"))
    dp.message.register(cmd_softresettime, Command("softresettime"))
    dp.message.register(cmd_fix_message_refs, Command("fixmessages"))
    dp.message.register(cmd_setdualmode, Command("setdualmode"))
    dp.message.register(cmd_shiftstatus, Command("shiftstatus"))
    dp.message.register(cmd_shiftreset, Command("shiftreset"))

    # 按钮处理器
    dp.message.register(
        handle_back_command,
        lambda message: message.text and message.text.strip() in ["✅ 回座", "回座"],
    )
    dp.message.register(
        handle_work_buttons,
        lambda message: message.text and message.text.strip() in ["🟢 上班", "🔴 下班"],
    )
    dp.message.register(
        handle_export_button,
        lambda message: message.text and message.text.strip() in ["📤 导出数据"],
    )
    dp.message.register(
        handle_my_record,
        lambda message: message.text and message.text.strip() in ["📊 我的记录"],
    )
    dp.message.register(
        handle_rank,
        lambda message: message.text and message.text.strip() in ["🏆 排行榜"],
    )
    dp.message.register(
        handle_admin_panel_button,
        lambda message: message.text and message.text.strip() in ["👑 管理员面板"],
    )
    dp.message.register(
        handle_back_to_main_menu,
        lambda message: message.text and message.text.strip() in ["🔙 返回主菜单"],
    )
    dp.message.register(
        handle_all_text_messages, lambda message: message.text and message.text.strip()
    )

    # 回调处理器
    dp.callback_query.register(
        handle_quick_back, lambda c: c.data.startswith("quick_back:")
    )

    logger.info("✅ 所有消息处理器注册完成")


# ========= render部署用的代码 ========
async def external_keepalive():
    """外部保活服务调用 - 防止 Render 休眠"""
    keepalive_urls = [
        # 可以添加 UptimeRobot 或其他免费监控服务
    ]

    for url in keepalive_urls:
        try:
            # 使用 aiohttp 发起请求
            pass
        except Exception as e:
            logger.debug(f"保活请求失败 {url}: {e}")


# async def keepalive_loop():
#     """Render 专用保活循环 - 防止免费服务休眠"""
#     while True:
#         try:
#             # 🆕 每5分钟执行一次保活（Render 免费版15分钟不活动会休眠）
#             await asyncio.sleep(300)

#             current_time = get_beijing_time()
#             logger.debug(
#                 f"🔵 Render 保活检查: {current_time.strftime('%Y-%m-%d %H:%M:%S')}"
#             )

#             # 1. 调用自己的健康检查端点
#             try:

#                 port = int(os.environ.get("PORT", 8080))
#                 async with aiohttp.ClientSession(
#                     timeout=aiohttp.ClientTimeout(total=10)
#                 ) as session:
#                     async with session.get(f"http://localhost:{port}/health") as resp:
#                         if resp.status == 200:
#                             logger.debug("✅ 内部健康检查保活成功")
#             except Exception as e:
#                 logger.warning(f"内部保活检查失败: {e}")

#             # 2. 数据库连接保活
#             try:
#                 await db.connection_health_check()
#                 logger.debug("✅ 数据库连接保活成功")
#             except Exception as e:
#                 logger.warning(f"数据库保活失败: {e}")

#             # 3. 内存清理
#             try:
#                 await performance_optimizer.memory_cleanup()
#                 # 🆕 强制垃圾回收

#                 collected = gc.collect()
#                 if collected > 0:
#                     logger.debug(f"🧹 保活期间GC回收 {collected} 个对象")
#             except Exception as e:
#                 logger.debug(f"保活期间内存清理失败: {e}")

#         except asyncio.CancelledError:
#             break
#         except Exception as e:
#             logger.error(f"Render 保活循环异常: {e}")
#             await asyncio.sleep(60)  # 异常后等待1分钟


async def keepalive_loop():
    """完整的保活循环: 外部保活 + 内部检查 + 数据库保活 + 内存回收"""
    external_url = os.environ.get("RENDER_EXTERNAL_URL") or getattr(
        Config, "WEBHOOK_URL", None
    )
    if external_url:
        external_url = external_url.rstrip("/")

    port = int(os.environ.get("PORT", 10000))
    logger.info(f"🚀 保活循环启动 | 外部URL: {external_url or '未设置'} | 端口: {port}")

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=20),
        headers={"User-Agent": "Bot-KeepAlive-Service"},
    ) as session:
        while True:
            try:
                # 保持原有的 5 分钟频率
                await asyncio.sleep(300)

                # 1. 外部公网保活
                if external_url:
                    try:
                        async with session.get(f"{external_url}/health") as resp:
                            if resp.status != 200:
                                logger.warning(
                                    f"🌍 外部保活异常 | 状态码: {resp.status}"
                                )
                            else:
                                logger.debug("🌍 外部保活成功")
                    except Exception as e:
                        logger.warning(f"🌍 外部保活失败: {e}")

                # 2. 内部健康检查
                try:
                    async with session.get(f"http://127.0.0.1:{port}/health") as resp:
                        if resp.status != 200:
                            logger.warning(
                                f"🏠 内部健康检查异常 | 状态码: {resp.status}"
                            )
                except Exception as e:
                    logger.warning(f"🏠 内部健康检查失败: {e}")

                # 3. ✅ 补回：数据库连接保活
                try:
                    if hasattr(db, "connection_health_check"):
                        await db.connection_health_check()
                except Exception as e:
                    logger.warning(f"🗄️ 数据库保活异常: {e}")

                # 4. ✅ 补回：内存回收 (GC)
                try:
                    collected = gc.collect()
                    if collected > 0:
                        logger.debug(f"🧹 GC 回收对象数: {collected}")
                except Exception:
                    pass

            except asyncio.CancelledError:
                logger.info("🛑 保活循环已取消")
                break
            except Exception as e:
                logger.error(f"⚠️ 保活循环遇到异常: {e}")
                await asyncio.sleep(60)


# ========== 启动流程 =========
async def on_startup():
    """启动时执行 - 解决冲突并保留完整指令逻辑"""
    logger.info("🎯 机器人启动中...")
    try:
        # ✅ 新增：强行踢掉其他冲突实例，确保线上唯一运行
        await bot_manager.bot.delete_webhook(drop_pending_updates=True)

        # 1. 定义指令列表
        user_commands = [
            BotCommand(command="wc", description="🚽 小厕"),
            BotCommand(command="bigwc", description="🚻 大厕"),
            BotCommand(command="eat", description="🍚 吃饭"),
            BotCommand(command="smoke", description="🚬 抽烟"),
            BotCommand(command="rest", description="🛌 休息"),
            BotCommand(command="workstart", description="🟢 上班打卡"),
            BotCommand(command="workend", description="🔴 下班打卡"),
            BotCommand(command="at", description="✅ 回座"),
            BotCommand(command="myinfo", description="📊 我的记录"),
            BotCommand(command="ranking", description="🏆 排行榜"),
            BotCommand(command="help", description="❓ 使用帮助"),
        ]

        admin_commands = user_commands + [
            BotCommand(command="actstatus", description="📊 活跃活动统计"),
            BotCommand(command="showsettings", description="⚙️ 查看系统配置"),
            BotCommand(command="finesstatus", description="📈 罚款费率查询"),
            BotCommand(command="worktime", description="⌚ 考勤时间设置"),
            BotCommand(command="export", description="📤 导出今日报表"),
            BotCommand(command="checkdb", description="🏥 数据库体检"),
            BotCommand(command="adminhelp", description="🛠 管理员全指令指南"),
        ]

        # ✅ 打印你需要的注册日志
        logger.info(f"📋 要注册的命令列表: {[cmd.command for cmd in user_commands]}")

        # 2. 注册普通用户菜单
        res_user = await bot_manager.bot.set_my_commands(commands=user_commands)
        logger.info(f"✅ 普通用户命令注册结果: {res_user}")

        # 3. 注册管理员菜单
        res_admin = await bot_manager.bot.set_my_commands(
            commands=admin_commands, scope=BotCommandScopeAllChatAdministrators()
        )
        logger.info(f"✅ 管理员指令菜单注册结果: {res_admin}")

        # 4. 初始化数据库
        if hasattr(db, "init"):
            await db.init()

        await send_startup_notification()
        logger.info("✅ 系统启动完成，准备接收消息")

    except Exception as e:
        logger.error(f"❌ 启动过程异常: {e}")
        raise


async def on_shutdown():
    """关闭时执行 - 更新版本"""
    logger.info("🛑 机器人正在关闭...")
    try:
        # 停止数据库维护任务
        await db.stop_connection_maintenance()
        logger.info("✅ 数据库维护任务已停止")

        # 停止Bot管理器
        await bot_manager.stop()
        logger.info("✅ Bot管理器已停止")

        # 取消所有定时器
        cancelled_count = await timer_manager.cancel_all_timers()
        logger.info(f"✅ 已取消 {cancelled_count} 个活动定时器")

        # 停止心跳管理器
        await heartbeat_manager.stop()
        logger.info("✅ 心跳管理器已停止")

        # 发送关闭通知
        await send_shutdown_notification()
        logger.info("✅ 关闭通知已发送")

        logger.info("🎉 所有服务已优雅关闭")
    except Exception as e:
        logger.error(f"关闭清理过程中出错: {e}")


# async def main():
#     """主函数 - Render 适配版"""
#     # Render 环境检测
#     is_render = os.environ.get("RENDER", False) or "RENDER" in os.environ

#     if is_render:
#         logger.info("🎯 检测到 Render 环境，应用优化配置")
#         # 应用 Render 特定配置
#         Config.DB_MAX_CONNECTIONS = 3
#         Config.ENABLE_FILE_LOGGING = False

#     try:
#         logger.info("🚀 启动打卡机器人系统...")

#         # 初始化服务
#         await initialize_services()

#         # 启动健康检查服务器（Render 必需）
#         await start_health_server()

#         # 🆕 Render 必需：更频繁的保活
#         keepalive_task = asyncio.create_task(keepalive_loop(), name="render_keepalive")

#         # 启动定时任务
#         asyncio.create_task(daily_reset_task(), name="daily_reset")
#         asyncio.create_task(soft_reset_task(), name="soft_reset")
#         asyncio.create_task(memory_cleanup_task(), name="memory_cleanup")
#         asyncio.create_task(health_monitoring_task(), name="health_monitoring")

#         # 启动机器人
#         logger.info("🤖 启动机器人（带自动重连机制）...")
#         await on_startup()

#         # 开始轮询
#         await bot_manager.start_polling_with_retry()

#     except KeyboardInterrupt:
#         logger.info("🛑 机器人被用户中断")
#     except Exception as e:
#         logger.error(f"❌ 机器人启动失败: {e}")
#         # 🆕 Render 环境下需要正常退出码
#         if is_render:
#             sys.exit(1)
#         raise
#     finally:
#         # 🆕 确保保活任务被正确取消
#         if "keepalive_task" in locals():
#             keepalive_task.cancel()
#             try:
#                 await keepalive_task
#             except asyncio.CancelledError:
#                 pass

#         await on_shutdown()


# async def main():
#     """Render-safe 主函数（Polling 版）"""

#     is_render = "RENDER" in os.environ

#     if is_render:
#         logger.info("🎯 Render 环境检测成功，启用安全模式")
#         Config.DB_MAX_CONNECTIONS = 3
#         Config.ENABLE_FILE_LOGGING = False

#     logger.info("🚀 启动打卡机器人系统（Render-safe polling 模式）")

#     # 1️⃣ 初始化
#     await initialize_services()

#     # 2️⃣ 启动健康检查服务器（必须最先）
#     await start_health_server()

#     # 3️⃣ 启动后台周期任务（允许失败，不影响主循环）
#     asyncio.create_task(daily_reset_task(), name="daily_reset")
#     asyncio.create_task(soft_reset_task(), name="soft_reset")
#     asyncio.create_task(memory_cleanup_task(), name="memory_cleanup")
#     asyncio.create_task(health_monitoring_task(), name="health_monitor")

#     # 4️⃣ 启动机器人（初始化）
#     await on_startup()

#     # 5️⃣ ⚠️ 关键：Polling 必须作为独立 Task
#     polling_task = asyncio.create_task(
#         bot_manager.start_polling_with_retry(), name="telegram_polling"
#     )

#     logger.info("🤖 Telegram polling 已启动（Render-safe）")

#     # 6️⃣ ⚠️ Render-safe 核心：主协程永远阻塞
#     # Render 只关心 HTTP 是否活着
#     try:
#         await asyncio.Event().wait()
#     finally:
#         logger.info("🛑 Render 正在关闭实例，开始清理...")
#         polling_task.cancel()
#         await on_shutdown()


async def main():
    """全环境通用 - 工业级稳固版 (适配 Render/VPS/Docker)"""
    # 1. 环境检测
    is_render = "RENDER" in os.environ
    health_server_site = None  # 用于存储健康服务器实例

    if is_render:
        logger.info("🎯 检测到 Render 环境，应用低功耗安全配置")
        Config.DB_MAX_CONNECTIONS = 3
        Config.ENABLE_FILE_LOGGING = False

    try:
        logger.info("🚀 启动打卡机器人系统...")

        # 2. 初始化核心服务（数据库等）
        await initialize_services()

        # 3. 启动健康检查服务器 (适配 Render 端口)
        # 修改点：保存返回值 site，以便后续安全关闭
        health_server_site = await start_health_server()

        # 4. 启动周期性后台任务
        # 使用 list 存储任务引用，防止被垃圾回收
        background_tasks = [
            asyncio.create_task(daily_reset_task(), name="daily_reset"),
            asyncio.create_task(soft_reset_task(), name="soft_reset"),
            asyncio.create_task(memory_cleanup_task(), name="memory_cleanup"),
            asyncio.create_task(health_monitoring_task(), name="health_monitor"),
        ]

        # 针对 Render 的保活任务
        if is_render:
            background_tasks.append(
                asyncio.create_task(keepalive_loop(), name="render_keepalive")
            )

        # 5. 启动机器人逻辑
        await on_startup()

        # 将 Polling 放入后台独立任务
        polling_task = asyncio.create_task(
            bot_manager.start_polling_with_retry(), name="telegram_polling"
        )

        logger.info("🤖 机器人系统全功能已就绪")

        # 6. 核心：钉死进程，不让程序退出
        # 这样即便 Polling 崩溃重启，主程序和 Web Server 依然活着
        await asyncio.Event().wait()

    except asyncio.CancelledError:
        logger.info("👋 收到系统关闭指令")
    except Exception as e:
        logger.error(f"❌ 系统运行异常: {e}")
        if is_render:
            sys.exit(1)  # 告诉 Render 启动失败，触发自动重启
    finally:
        logger.info("🛑 开始清理并优雅关闭...")

        # A. 停止轮询
        if "polling_task" in locals():
            polling_task.cancel()
            with suppress(asyncio.CancelledError):
                await polling_task

        # B. 关闭健康服务器（关键：防止重启时端口占用）
        if health_server_site:
            with suppress(Exception):
                await health_server_site.stop()
                logger.info("✅ 健康检查服务器已释放端口")

        # C. 停止所有后台任务
        if "background_tasks" in locals():
            for task in background_tasks:
                task.cancel()

        # D. 执行统一的清理逻辑（关闭数据库等）
        await on_shutdown()
        logger.info("🎉 进程已安全结束")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("机器人已被用户中断")
    except Exception as e:
        logger.error(f"机器人运行异常: {e}")
