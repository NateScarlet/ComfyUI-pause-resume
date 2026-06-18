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
from .models import init_queue, GatewayStateManager
from .gateway import Gateway
from .server import GatewayHandlers, setup_routes

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

    # 3. 创建网关状态管理器与任务队列
    os.makedirs(config.data_dir, exist_ok=True)
    state_db_path = os.path.join(config.data_dir, "state.db")
    state_manager = GatewayStateManager(state_db_path)
    queue = init_queue(config)

    # 4. 创建 Gateway 实例 (构造函数依赖注入)
    gateway = Gateway(config, state_manager, queue)

    # 5. 绑定事件循环及退出信号处理器
    loop = asyncio.get_running_loop()
    gateway.loop = loop
    exit_event = asyncio.Event()

    def signal_handler(signum: Any, frame: Any) -> None:
        if signum == signal.SIGINT and gateway.exiting:
            logger.info("⚡ Forced exit requested by user. Exiting immediately...")
            sys.exit(1)
        logger.info("👋 Received termination signal, exiting...")
        gateway.exiting = True
        loop.call_soon_threadsafe(exit_event.set)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # 6. 后台拉起下游服务
    async def bootstrap_downstream() -> None:
        if gateway.exiting:
            return
        gateway.start_downstream()
        await gateway.wait_downstream_ready()

    asyncio.create_task(bootstrap_downstream())

    # 7. 构建 Aiohttp 服务器及处理器依赖项并注册路由
    app = web.Application(client_max_size=1024**3)  # 最大 1GB 上传包大小限制
    handlers = GatewayHandlers(gateway)
    setup_routes(app, handlers)

    runner = web.AppRunner(app, access_log_class=DebugAccessLogger)
    await runner.setup()
    site = web.TCPSite(runner, config.proxy_host, config.proxy_port)
    await site.start()

    logger.info(f"🌐 Proxy server running on {config.proxy_host}:{config.proxy_port}")

    # 8. 启动队列分发与监控后台进程
    asyncio.create_task(gateway.queue_dispatcher())

    # 9. 挂起并等待退出信号触发优雅退出机制
    try:
        await exit_event.wait()
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("🛑 Gateway shutting down...")
        await gateway.shutdown(runner)
