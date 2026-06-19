from aiohttp import web
from .handlers import GatewayHandlers


def setup_routes(app: web.Application, handlers: GatewayHandlers) -> None:
    """注册网关自定义控制端点以及默认通配反向代理路由。"""
    app.router.add_post(
        "/io.github.natescarlet.pause-resume/pause", handlers.handle_pause
    )
    app.router.add_post(
        "/io.github.natescarlet.pause-resume/resume", handlers.handle_resume
    )
    app.router.add_get(
        "/io.github.natescarlet.pause-resume/state", handlers.handle_state
    )
    app.router.add_get("/io.github.natescarlet.pause-resume/sse", handlers.handle_sse)

    # 注册默认代理通配路由，将未命中特定命名空间的其他所有请求交由代理处理器转发
    app.router.add_route("*", "/{tail:.*}", handlers.proxy_handler)
