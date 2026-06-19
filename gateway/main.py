import os
import sys
import socket
import signal
import asyncio
import logging
from aiohttp import web
from aiohttp.web_log import AccessLogger
from typing import Any

from .config import GatewayConfig
from .infrastructure.sqlite.state import SQLiteStateRepository
from .infrastructure.queue_factory import init_queue
from .infrastructure.system.program import ExternalProgramManager
from .infrastructure.system.power import PowerManagement
from .infrastructure.system.timer import AsyncioTimer
from .domain.gateway import Gateway
from .infrastructure.comfyui.downstream import ComfyUIDownstreamClient
from .infrastructure.in_memory.event_bus import InMemoryEventBus
from .infrastructure.comfyui.dispatcher import ComfyUITaskDispatcher
from .application.facade import AppFacade
from .presentation.handlers import GatewayHandlers
from .presentation.routes import setup_routes
from .shared.events import DownstreamCrashedEvent, StatusChangedEvent

logging.basicConfig(
    level=(logging.DEBUG if os.getenv("GATEWAY_DEBUG") == "true" else logging.INFO),
    format="[GATEWAY] %(message)s",
)
logger = logging.getLogger(__name__)


class DebugAccessLogger(AccessLogger):
    """自定义请求日志记录器，将请求日志级别降低为 DEBUG，以防默认输出太嘈杂"""

    def log(
        self, request: web.BaseRequest, response: web.StreamResponse, time: float
    ) -> None:
        try:
            fmt_info: Any = self._format_line(request, response, time)
            values: list[str] = []
            extra: dict[str, Any] = {}
            for key, value in fmt_info:
                values.append(value)
                if isinstance(key, str):
                    extra[key] = value
                else:
                    k1, k2 = key
                    dct = extra.get(k1, {})
                    dct[k2] = value
                    extra[k1] = dct
            self.logger.debug(self._log_format % tuple(values), extra=extra)
        except Exception:
            self.logger.exception("Error in logging")


async def main() -> None:
    # 1. 实例化依赖配置
    config = GatewayConfig()

    # 2. 提前进行端口占用检查
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((config.proxy_host, config.proxy_port))
    except OSError:
        logger.error(
            f"❌ 端口 {config.proxy_port} 正被占用，请检查是否已有网关或 ComfyUI 实例在运行。"
        )
        sys.exit(1)

    # 3. 实例化技术持久化和工厂队列（读写接口分离）
    os.makedirs(config.data_dir, exist_ok=True)
    state_repo = SQLiteStateRepository(os.path.join(config.data_dir, "state.db"))
    queue_reader, queue_writer, close_queue = init_queue(config)

    # 4. 实例化系统休眠管理器和定时器
    power_manager = PowerManagement()
    timer = AsyncioTimer()

    # 5. 实例化外部程序管理器
    process_manager = ExternalProgramManager(config.idle_program, config.busy_program)

    # 6. 实例化事件总线和下游客户端
    event_bus = InMemoryEventBus()
    downstream_service = ComfyUIDownstreamClient(
        config=config,
        event_bus=event_bus,
        process_manager=process_manager,
    )

    # 7. 实例化应用层任务分派器
    dispatcher = ComfyUITaskDispatcher(
        config=config,
        queue_reader=queue_reader,
        queue_writer=queue_writer,
        downstream=downstream_service,
        event_bus=event_bus,
    )

    # 8. 实例化领域模型聚合根 (Gateway)，直接注入所有依赖
    gateway = Gateway(
        state_repo=state_repo,
        queue_reader=queue_reader,
        process_manager=process_manager,
        power_controller=power_manager,
        timer=timer,
        downstream=downstream_service,
        dispatcher=dispatcher,
        event_bus=event_bus,
        idle_restart_timeout=config.idle_restart_timeout,
    )

    # 9. 任务分发器完成 Gateway 注入以消解循环引用
    dispatcher.set_gateway(gateway)

    # 10. 绑定崩溃处理和状态更新
    # 启动时重入之前崩溃可能残留的正在运行任务
    queue_writer.requeue_running()

    def handle_crashed(ev: DownstreamCrashedEvent) -> None:
        if queue_writer.requeue_running_if_exists():
            gateway.increment_attempt_count()
        event_bus.publish(StatusChangedEvent())

    event_bus.subscribe(DownstreamCrashedEvent, handle_crashed)

    # 11. 根据初始状态，同步阻止系统休眠和外挂脚本行为
    gateway.sync_infrastructure()

    # 12. 实例化应用层 Facade 门面（组装所有 Command 与 Query 处理器）
    app_facade = AppFacade.create(
        gateway=gateway,
        queue_reader=queue_reader,
        queue_writer=queue_writer,
        downstream_client=downstream_service,
    )

    # 13. 绑定异步事件循环与信号量
    loop = asyncio.get_running_loop()
    downstream_service.loop = loop
    exit_event = asyncio.Event()

    def signal_handler(signum: Any, frame: Any) -> None:
        if signum == signal.SIGINT and downstream_service.exiting:
            logger.info("⚡ Forced exit requested by user. Exiting immediately...")
            sys.exit(1)
        logger.info("👋 Received termination signal, exiting...")
        downstream_service.exiting = True
        loop.call_soon_threadsafe(exit_event.set)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # 14. 后台拉起下游服务
    async def bootstrap_downstream() -> None:
        if downstream_service.exiting:
            return
        downstream_service.start_downstream()
        await downstream_service.wait_downstream_ready()

    asyncio.create_task(bootstrap_downstream())

    # 15. 构建表示层 (GatewayHandlers) 并注册 Web 路由
    app = web.Application(client_max_size=1024**3)
    handlers = GatewayHandlers(
        app=app_facade,
        downstream_service=downstream_service,
        queue_reader=queue_reader,
        event_bus=event_bus,
    )
    setup_routes(app, handlers)

    runner = web.AppRunner(app, access_log_class=DebugAccessLogger)
    await runner.setup()
    site = web.TCPSite(runner, config.proxy_host, config.proxy_port)
    await site.start()

    logger.info(f"🌐 Proxy server running on {config.proxy_host}:{config.proxy_port}")

    # 16. 后台启动监控与首次分发
    asyncio.create_task(downstream_service.monitor_downstream())
    dispatcher.try_dispatch()

    # 13. 等待信号并优雅清理
    try:
        await exit_event.wait()
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("🛑 Gateway shutting down...")
        handlers.shutdown()
        await downstream_service.shutdown()

        # 构建方释放自己创建的基础设施资源
        try:
            power_manager.allow_sleep()
        except Exception:
            pass
        try:
            close_queue()
        except Exception:
            pass
        try:
            state_repo.close()
        except Exception:
            pass
        try:
            process_manager.cleanup()
        except Exception:
            pass

        try:
            await runner.cleanup()
        except Exception as e:
            logger.error(f"Error during runner cleanup: {e}")
