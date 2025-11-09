# render_deploy.py - 修复版本（不启动轮询）
import os
import asyncio
import logging
import time
import signal
from aiohttp import web

# ✅ 导入所有需要的组件（移除 dp，因为不需要轮询）
from main import (
    bot,  # 只保留 bot 用于 webhook 清理
    db,
    heartbeat_manager,
    memory_cleanup_task,
    health_monitoring_task,
    daily_reset_task,
    efficient_monthly_export_task,
    monthly_report_task,
    simple_on_startup,
)

from config import Config

# ===========================
# 日志配置
# ===========================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RenderBot")

# ===========================
# 全局状态管理
# ===========================
class AppState:
    def __init__(self):
        self.running = True
        self.web_server_started = False  # 改为 web 服务器状态


app_state = AppState()

# ===========================
# 信号处理
# ===========================
def handle_sigterm(signum, frame):
    """处理 SIGTERM 信号"""
    logger.info(f"📡 收到信号 {signum}，准备优雅关闭...")
    app_state.running = False

# 注册信号处理器
signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)

# ===========================
# Render 保活健康检查接口
# ===========================
async def health_check(request):
    """健康检查端点"""
    return web.json_response(
        {
            "status": "healthy" if app_state.running else "shutting_down",
            "service": "telegram-bot-web",  # 修改服务名称
            "timestamp": time.time(),
            "web_server_active": app_state.web_server_started,  # 改为 web 服务器状态
        }
    )

# ===========================
# Render 必需 Web 服务（动态端口）
# ===========================
async def start_web_server():
    """启动 Web 服务器（Render FREE 必需）"""
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)
    app.router.add_get("/status", health_check)

    # ✅ Render 提供动态端口，不可写死
    port = int(os.environ.get("PORT", 8080))

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    app_state.web_server_started = True  # 设置状态
    logger.info(f"✅ Web server started on Render dynamic port: {port}")
    return runner, site

# ===========================
# 初始化所有关键服务（数据库 / 心跳 / 配置）
# ===========================
async def initialize_services():
    logger.info("🔄 Initializing services...")

    # ✅ 初始化数据库
    await db.initialize()
    logger.info("✅ Database initialized")

    # ✅ 初始化心跳服务
    await heartbeat_manager.initialize()
    logger.info("✅ Heartbeat initialized")

    # ✅ 确保删除所有 webhook，避免冲突
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("✅ Webhook deleted → Telegram polling will be handled by main.py")

        # 额外等待确保 webhook 完全删除
        await asyncio.sleep(2)
    except Exception as e:
        logger.warning(f"⚠️ 删除 webhook 时出现警告: {e}")

    # 🆕 执行启动流程
    await simple_on_startup()
    logger.info("✅ All services initialized with activity recovery")

# ===========================
# 启动后台任务（不会阻塞主线程）
# ===========================
async def start_background_tasks():
    """启动所有后台任务（不阻塞）"""

    # ✅ 所有后台任务都应该使用 create_task()
    asyncio.create_task(heartbeat_manager.start_heartbeat_loop())
    asyncio.create_task(memory_cleanup_task())
    asyncio.create_task(health_monitoring_task())
    asyncio.create_task(daily_reset_task())
    asyncio.create_task(auto_daily_export_task())
    asyncio.create_task(efficient_monthly_export_task())
    asyncio.create_task(monthly_report_task())

    logger.info("✅ All background tasks started")

# ===========================
# 主程序入口 - 只启动 Web 服务，不启动轮询
# ===========================
async def main():
    web_runner = None
    web_site = None

    try:
        # ✅ Render 必须先启动该 Web 服务，否则会 Deployment Timed Out
        web_runner, web_site = await start_web_server()

        # ✅ 初始化所有服务
        await initialize_services()

        # ✅ 启动后台任务（不阻塞）
        await start_background_tasks()

        logger.info("🎉 Render Web 服务启动完成！")
        logger.info("💡 Telegram 轮询将在主程序 (main.py) 中启动")
        logger.info("🌐 Web 服务保持运行中...")

        # ✅ 关键修改：只保持 Web 服务运行，不启动轮询
        while app_state.running:
            await asyncio.sleep(10)

    except Exception as e:
        logger.error(f"💥 Web service failed to start: {e}")
        raise

    finally:
        logger.info("🛑 Web service shutdown complete")

        # 清理资源
        try:
            if web_runner:
                await web_runner.cleanup()
                logger.info("✅ Web runner cleaned up")
        except Exception as e:
            logger.warning(f"⚠️ 清理 web runner 时出错: {e}")

# ===========================
# 程序启动
# ===========================
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 收到键盘中断信号")
    except Exception as e:
        logger.error(f"💥 主程序异常: {e}")
        raise
