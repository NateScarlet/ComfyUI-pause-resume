import asyncio
import unittest
from unittest.mock import MagicMock

from aiohttp import web
from aiohttp.test_utils import make_mocked_request

from gateway.presentation.handlers import GatewayHandlers
from gateway.presentation.routes import setup_routes


class TestGatewayRoutes(unittest.TestCase):
    """测试网关新控制端点不被通配代理路由吞掉（#20）。"""

    def test_restart_route_resolves_to_restart_endpoint(self):
        """POST restart 必须命中重启端点自身的路由（#20）。"""

        async def scenario() -> None:
            app = web.Application()
            setup_routes(app, MagicMock(spec=GatewayHandlers))
            request = make_mocked_request(
                "POST", "/io.github.natescarlet.pause-resume/restart"
            )
            match_info = await app.router.resolve(request)
            self.assertEqual(
                match_info.route.resource.canonical,
                "/io.github.natescarlet.pause-resume/restart",
            )
            self.assertEqual(match_info.route.method, "POST")

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
