# render_deploy.py - 完整修复版本（确保无遗漏）
import os
import asyncio
import logging
import time
import signal
import sys
from aiohttp import web
from datetime import datetime

# ✅ 导入所有需要的组件
from main import (
    db,
    heartbeat_manager,
    memory_cleanup_task,
    health_monitoring_task,
    daily_reset_task,
    efficient_monthly_export_task,
    monthly_report_task,
    simple_on_startup,
    # ✅ 性能相关组件
    performance_monitor,
    task_manager,
    performance_optimizer,
    timer_manager,
    user_lock_manager,
    global_cache,
    # ✅ 工具函数
    get_beijing_time,
    # ✅ 新增：导入 bot 用于 webhook 清理
    bot,
)

from config import Config, beijing_tz

# ===========================
# 日志配置
# ===========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("render_bot.log", encoding="utf-8", mode="a"),
    ],
)
logger = logging.getLogger("RenderBot")

# ===========================
# 全局状态管理
# ===========================
class AppState:
    def __init__(self):
        self.running = True
        self.web_server_started = False
        self.services_initialized = False
        self.background_tasks = []
        self.start_time = time.time()

app_state = AppState()

# ===========================
# 信号处理
# ===========================
def handle_sigterm(signum, frame):
    logger.info(f"📡 收到信号 {signum}，准备优雅关闭...")
    app_state.running = False

def handle_sigint(signum, frame):
    logger.info("👋 收到键盘中断信号")
    app_state.running = False

# 注册信号处理器
signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigint)

# ===========================
# 健康检查接口
# ===========================
async def health_check(request):
    """基础健康检查端点"""
    status = "healthy" if app_state.running else "shutting_down"
    
    return web.json_response({
        "status": status,
        "service": "telegram-bot-web",
        "timestamp": time.time(),
        "beijing_time": get_beijing_time().isoformat(),
        "web_server_active": app_state.web_server_started,
        "services_initialized": app_state.services_initialized,
        "uptime_seconds": int(time.time() - app_state.start_time),
        "environment": "render"
    })

async def detailed_health_check(request):
    """详细健康检查"""
    try:
        # 检查数据库连接
        db_healthy = await db.connection_health_check()
        
        # 检查心跳状态
        heartbeat_status = heartbeat_manager.get_status()
        
        # 获取性能统计
        perf_report = performance_monitor.get_performance_report()
        cache_stats = global_cache.get_stats()
        lock_stats = user_lock_manager.get_stats()
        timer_stats = timer_manager.get_stats()
        
        return web.json_response({
            "status": "healthy" if db_healthy else "degraded",
            "timestamp": time.time(),
            "beijing_time": get_beijing_time().isoformat(),
            "components": {
                "database": db_healthy,
                "heartbeat": heartbeat_status,
                "web_server": app_state.web_server_started,
                "services": app_state.services_initialized,
                "performance": {
                    "memory_ok": performance_optimizer.memory_usage_ok(),
                    "uptime": perf_report.get('uptime', 0),
                    "slow_operations": perf_report.get('slow_operations_count', 0)
                }
            },
            "resources": {
                "background_tasks": len(app_state.background_tasks),
                "user_locks": lock_stats.get('active_locks', 0),
                "active_timers": timer_stats.get('active_timers', 0),
                "cache_hit_rate": cache_stats.get('hit_rate', 0)
            },
            "environment": "render"
        })
    except Exception as e:
        logger.error(f"健康检查失败: {e}")
        return web.json_response({
            "status": "unhealthy",
            "error": str(e),
            "timestamp": time.time()
        }, status=500)

async def metrics_endpoint(request):
    """Prometheus 格式指标端点"""
    try:
        # 获取内存使用
        memory_bytes = 0
        try:
            import psutil
            memory_bytes = psutil.Process().memory_info().rss
        except:
            pass

        # 获取各种统计
        perf_report = performance_monitor.get_performance_report()
        cache_stats = global_cache.get_stats()
        lock_stats = user_lock_manager.get_stats()
        timer_stats = timer_manager.get_stats()

        metrics = [
            "# HELP render_web_service_status Web 服务状态",
            "# TYPE render_web_service_status gauge",
            f"render_web_service_status {1 if app_state.running else 0}",
            
            "# HELP render_services_initialized 服务初始化状态",
            "# TYPE render_services_initialized gauge", 
            f"render_services_initialized {1 if app_state.services_initialized else 0}",
            
            "# HELP render_background_tasks 后台任务数量",
            "# TYPE render_background_tasks gauge",
            f"render_background_tasks {len(app_state.background_tasks)}",
            
            "# HELP render_memory_usage_bytes 内存使用量",
            "# TYPE render_memory_usage_bytes gauge",
            f"render_memory_usage_bytes {memory_bytes}",
            
            "# HELP render_uptime_seconds 运行时间",
            "# TYPE render_uptime_seconds gauge",
            f"render_uptime_seconds {int(time.time() - app_state.start_time)}",
            
            "# HELP render_user_locks 用户锁数量",
            "# TYPE render_user_locks gauge",
            f"render_user_locks {lock_stats.get('active_locks', 0)}",
            
            "# HELP render_active_timers 活跃定时器数量",
            "# TYPE render_active_timers gauge",
            f"render_active_timers {timer_stats.get('active_timers', 0)}",
            
            "# HELP render_cache_hit_rate 缓存命中率",
            "# TYPE render_cache_hit_rate gauge",
            f"render_cache_hit_rate {cache_stats.get('hit_rate', 0)}",
            
            "# HELP render_slow_operations 慢操作数量",
            "# TYPE render_slow_operations gauge",
            f"render_slow_operations {perf_report.get('slow_operations_count', 0)}",
        ]

        return web.Response(text="\n".join(metrics), content_type="text/plain")
    except Exception as e:
        logger.error(f"指标端点错误: {e}")
        return web.Response(text=f"error: {e}", status=500)

# ===========================
# Render Web 服务器
# ===========================
async def start_render_web_server():
    """启动 Render 必需的 Web 服务器"""
    app = web.Application()
    
    # 注册路由
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)
    app.router.add_get("/status", detailed_health_check)
    app.router.add_get("/metrics", metrics_endpoint)
    app.router.add_get("/ping", lambda request: web.Response(text="pong"))
    
    # Render 提供动态端口
    port = int(os.environ.get("PORT", 8080))
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    
    app_state.web_server_started = True
    logger.info(f"🌐 Render Web 服务器已在端口 {port} 启动")
    
    return runner, site

# ===========================
# 服务初始化（不启动轮询）
# ===========================
async def initialize_services_without_polling():
    """初始化服务但不启动 Telegram 轮询"""
    logger.info("🔄 初始化服务（不启动轮询）...")
    
    try:
        # 数据库初始化
        await db.initialize()
        logger.info("✅ 数据库初始化完成")
        
        # 心跳服务初始化
        await heartbeat_manager.initialize()
        logger.info("✅ 心跳服务初始化完成")
        
        # 确保删除 webhook，避免冲突
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("✅ Webhook 已删除，为 Polling 模式做准备")
            await asyncio.sleep(2)
        except Exception as e:
            logger.warning(f"⚠️ 删除 webhook 时出现警告: {e}")
        
        # 执行启动流程（恢复活动定时器等）
        await simple_on_startup()
        
        app_state.services_initialized = True
        logger.info("✅ 所有服务初始化完成（等待主程序启动轮询）")
        
    except Exception as e:
        logger.error(f"❌ 服务初始化失败: {e}")
        raise

# ===========================
# 启动后台任务
# ===========================
async def start_background_tasks():
    """启动所有必要的后台任务"""
    tasks = [
        asyncio.create_task(memory_cleanup_task(), name="memory_cleanup"),
        asyncio.create_task(health_monitoring_task(), name="health_monitoring"),
        asyncio.create_task(heartbeat_manager.start_heartbeat_loop(), name="heartbeat"),
        asyncio.create_task(daily_reset_task(), name="daily_reset"),
        asyncio.create_task(efficient_monthly_export_task(), name="monthly_export"),
        asyncio.create_task(monthly_report_task(), name="monthly_report"),
    ]
    
    # 保存任务引用
    app_state.background_tasks = tasks
    
    logger.info(f"✅ 后台任务已启动: {len(tasks)} 个任务")
    
    # 记录任务详情
    for task in tasks:
        logger.debug(f"   - {task.get_name()}")
    
    return tasks

# ===========================
# 停止后台任务
# ===========================
async def stop_background_tasks():
    """安全停止所有后台任务"""
    if not app_state.background_tasks:
        return
    
    logger.info(f"🛑 停止 {len(app_state.background_tasks)} 个后台任务...")
    
    stopped_count = 0
    for task in app_state.background_tasks:
        if not task.done():
            task.cancel()
            try:
                await task
                stopped_count += 1
                logger.debug(f"   ✅ 已停止: {task.get_name()}")
            except asyncio.CancelledError:
                stopped_count += 1
                logger.debug(f"   ✅ 已取消: {task.get_name()}")
            except Exception as e:
                logger.warning(f"⚠️ 停止任务 {task.get_name()} 时出错: {e}")
    
    logger.info(f"✅ 已停止 {stopped_count} 个后台任务")
    app_state.background_tasks = []

# ===========================
# 资源清理函数
# ===========================
async def cleanup_render_resources():
    """Render 专用的资源清理函数"""
    logger.info("🧹 开始清理 Render 资源...")
    
    cleanup_steps = [
        ("心跳服务", heartbeat_manager.stop),
        ("数据库连接", db.close),
        ("Bot Session", bot.session.close),
        ("性能监控缓存", performance_monitor.cleanup_old_data),
        ("全局缓存", global_cache.clear_expired),
        ("用户锁清理", user_lock_manager.force_cleanup),
        ("定时器清理", timer_manager.cleanup_finished_timers),
    ]
    
    for name, cleanup_func in cleanup_steps:
        try:
            if asyncio.iscoroutinefunction(cleanup_func):
                await cleanup_func()
            else:
                cleanup_func()
            logger.info(f"✅ {name} 已清理")
        except Exception as e:
            logger.warning(f"⚠️ 清理 {name} 时出错: {e}")

# ===========================
# 环境检查
# ===========================
def check_render_environment():
    """检查 Render 环境配置"""
    required_vars = ["DATABASE_URL", "BOT_TOKEN"]
    missing_vars = []
    
    for var in required_vars:
        if not os.environ.get(var):
            missing_vars.append(var)
    
    if missing_vars:
        logger.error(f"❌ 缺少必要的环境变量: {', '.join(missing_vars)}")
        return False
    
    # 检查数据库URL格式
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url.startswith("postgresql://"):
        logger.error("❌ DATABASE_URL 必须是 PostgreSQL 连接字符串")
        return False
    
    logger.info("✅ 环境变量检查通过")
    return True

# ===========================
# 内存保护任务
# ===========================
async def memory_protection_task():
    """内存保护任务 - 防止内存泄漏"""
    while app_state.running:
        try:
            await asyncio.sleep(1800)  # 每30分钟检查一次
            
            # 强制清理各种缓存和锁
            await user_lock_manager.force_cleanup()
            await performance_optimizer.memory_cleanup()
            await global_cache.clear_expired()
            await timer_manager.cleanup_finished_timers()
            
            logger.debug("🧹 内存保护任务执行完成")
            
        except Exception as e:
            logger.warning(f"⚠️ 内存保护任务执行失败: {e}")
            await asyncio.sleep(300)  # 出错后等待5分钟

# ===========================
# 主服务函数
# ===========================
async def render_web_service():
    """
    Render Web 服务主函数
    只启动 Web 服务器和后台服务，不启动 Telegram 轮询
    """
    web_runner = None
    memory_protection_task_instance = None
    
    try:
        logger.info("🚀 启动 Render Web 服务...")
        logger.info(f"📊 启动时间: {get_beijing_time().isoformat()}")
        
        # 检查环境
        if not check_render_environment():
            logger.error("❌ 环境检查失败，退出服务")
            sys.exit(1)
        
        # 1. 必须先启动 Web 服务器（Render 要求）
        logger.info("🌐 启动 Web 服务器...")
        web_runner, web_site = await start_render_web_server()
        
        # 2. 初始化业务服务（不启动轮询）
        logger.info("🔄 初始化业务服务...")
        await initialize_services_without_polling()
        
        # 3. 启动后台任务
        logger.info("🚀 启动后台任务...")
        await start_background_tasks()
        
        # 4. 启动内存保护任务
        memory_protection_task_instance = asyncio.create_task(
            memory_protection_task(), 
            name="memory_protection"
        )
        
        logger.info("🎉 Render Web 服务启动完成！")
        logger.info("💡 Telegram 轮询将在主程序 (main.py) 中启动")
        logger.info("🌐 Web 服务保持运行中...")
        logger.info("📊 可通过以下端点监控服务:")
        logger.info("   - /health    基础健康检查")
        logger.info("   - /status    详细状态检查") 
        logger.info("   - /metrics   Prometheus指标")
        logger.info("   - /ping      连通性测试")
        
        # 5. 保持服务运行（不启动轮询）
        keepalive_count = 0
        last_status_log = time.time()
        
        while app_state.running:
            await asyncio.sleep(30)
            keepalive_count += 1
            
            # 每10分钟记录一次状态
            if time.time() - last_status_log > 600:
                logger.info("🌐 Web 服务运行中...")
                last_status_log = time.time()
                
                # 定期检查关键服务状态
                try:
                    db_ok = await db.connection_health_check()
                    if not db_ok:
                        logger.warning("⚠️ 数据库连接检查失败，尝试重连...")
                        await db.reconnect()
                except Exception as e:
                    logger.warning(f"⚠️ 服务状态检查失败: {e}")
                
    except Exception as e:
        logger.error(f"💥 Render Web 服务启动失败: {e}")
        # 在 Render 中，即使失败也要保持进程运行
        try:
            error_count = 0
            while app_state.running and error_count < 10:  # 最多重试10次
                await asyncio.sleep(30)
                error_count += 1
                logger.info(f"🔄 服务启动失败，但保持进程运行... ({error_count}/10)")
        except:
            pass
        raise
        
    finally:
        logger.info("🛑 开始关闭 Render Web 服务...")
        logger.info(f"📊 总运行时间: {int(time.time() - app_state.start_time)} 秒")
        
        # 停止内存保护任务
        if memory_protection_task_instance and not memory_protection_task_instance.done():
            memory_protection_task_instance.cancel()
            try:
                await memory_protection_task_instance
            except asyncio.CancelledError:
                pass
            logger.info("✅ 内存保护任务已停止")
        
        # 停止后台任务
        await stop_background_tasks()
        
        # 清理资源
        await cleanup_render_resources()
        
        # 关闭 Web 服务器
        if web_runner:
            try:
                await web_runner.cleanup()
                logger.info("✅ Web 服务器已关闭")
            except Exception as e:
                logger.warning(f"⚠️ 关闭 Web 服务器时出错: {e}")
        
        logger.info("🎉 Render Web 服务关闭完成")

# ===========================
# 程序启动
# ===========================
if __name__ == "__main__":
    try:
        # 设置更详细的日志级别
        logging.getLogger().setLevel(logging.INFO)
        
        # 记录启动信息
        logger.info("=" * 50)
        logger.info("🚀 启动 Render 专用 Web 服务")
        logger.info(f"📅 启动时间: {get_beijing_time().isoformat()}")
        logger.info(f"🐍 Python 版本: {sys.version}")
        logger.info(f"📁 工作目录: {os.getcwd()}")
        logger.info("=" * 50)
        
        # 启动服务
        asyncio.run(render_web_service())
        
    except KeyboardInterrupt:
        logger.info("👋 收到键盘中断信号")
    except Exception as e:
        logger.error(f"💥 Render Web 服务异常退出: {e}")
        # 在 Render 中，即使异常也要确保进程不会立即退出
        try:
            # 等待一段时间让 Render 捕获错误
            import time as sync_time
            sync_time.sleep(10)
        except:
            pass
        sys.exit(1)
    finally:
        logger.info("🎯 Render Web 服务进程结束")
