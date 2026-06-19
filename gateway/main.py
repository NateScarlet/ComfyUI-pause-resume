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
from .domain.gateway import Gateway
from .application.services.downstream import DownstreamAppService
from .application.facade import AppFacade
from .presentation.handlers import GatewayHandlers
from .presentation.routes import setup_routes

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
    queue_reader, queue_writer = init_queue(config)

    # 4. 实例化外部程序管理器
    process_manager = ExternalProgramManager(config.idle_program, config.busy_program)

    # 5. 实例化领域模型聚合根 (Gateway)，从持久化仓储恢复暂停状态
    gateway = Gateway(state_repo=state_repo)

    # 6. 实例化应用服务 (DownstreamAppService) 并依赖注入
    downstream_service = DownstreamAppService(
        gateway=gateway,
        config=config,
        queue_reader=queue_reader,
        queue_writer=queue_writer,
        process_manager=process_manager,
    )

    # 7. 根据初始状态，同步阻止系统休眠和外挂脚本行为
    downstream_service.sync_state_to_infrastructure()

    # 8. 实例化应用层 Facade 门面（组装所有 Command 与 Query 处理器）
    app_facade = AppFacade.create(
        gateway=gateway,
        queue_reader=queue_reader,
        queue_writer=queue_writer,
        downstream_service=downstream_service,
    )

    # 9. 绑定异步事件循环与信号量
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

    # 10. 后台拉起下游服务
    async def bootstrap_downstream() -> None:
        if downstream_service.exiting:
            return
        downstream_service.start_downstream()
        await downstream_service.wait_downstream_ready()

    asyncio.create_task(bootstrap_downstream())

    # 11. 构建表示层 (GatewayHandlers) 并注册 Web 路由
    app = web.Application(client_max_size=1024**3)
    handlers = GatewayHandlers(
        app=app_facade,
        downstream_service=downstream_service,
        queue_reader=queue_reader,
    )
    setup_routes(app, handlers)

    runner = web.AppRunner(app, access_log_class=DebugAccessLogger)
    await runner.setup()
    site = web.TCPSite(runner, config.proxy_host, config.proxy_port)
    await site.start()

    logger.info(f"🌐 Proxy server running on {config.proxy_host}:{config.proxy_port}")

    # 12. 后台启动监控与首次分发
    asyncio.create_task(downstream_service.monitor_downstream())
    downstream_service.try_dispatch()

    # 13. 等待信号并优雅清理
    try:
        await exit_event.wait()
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("🛑 Gateway shutting down...")
        handlers.shutdown()
        await downstream_service.shutdown()
        try:
            await runner.cleanup()
        except Exception as e:
            logger.error(f"Error during runner cleanup: {e}")
