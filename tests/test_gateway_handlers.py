import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock

from aiohttp import web
from aiohttp.test_utils import make_mocked_request

from gateway.presentation.handlers import GatewayHandlers, _LOADING_HTML
from gateway.shared.events import (
    DownstreamReadyChangedEvent,
    StateChangedEvent,
    StatusChangedEvent,
)
from gateway.shared.interfaces import DownstreamClient


class _SyncEventBus:
    """测试用同步事件总线：publish 立即同步触发订阅回调。"""

    def __init__(self) -> None:
        self._subs = {}
        self.subscribed_events = []

    def subscribe(self, event_class, callback):
        self._subs.setdefault(event_class, []).append(callback)
        self.subscribed_events.append(event_class)

        def unsubscribe() -> None:
            self._subs[event_class].remove(callback)

        return unsubscribe

    def publish(self, event) -> None:
        for cb in list(self._subs.get(type(event), [])):
            cb(event)


class _ProxyStubHandlers(GatewayHandlers):
    """重写默认代理以返回哨兵响应，避免测试触发真实 HTTP 转发。"""

    async def _handle_default_proxy(self, request, method, downstream_url):
        return web.Response(status=200, text="proxied")


class TestGatewayHandlersProxyReadiness(unittest.TestCase):
    """测试下游未就绪（启动/重启中）时 proxy_handler 的等待行为。"""

    def _make_handlers(self, ready=False, wait_sec=0.05):
        app = MagicMock()
        app.get_jobs.handle = AsyncMock(return_value=[])
        app.get_job_count.handle = AsyncMock(return_value=0)
        downstream = MagicMock(spec=DownstreamClient)
        downstream.downstream_ready = ready
        downstream.downstream_port = 8188
        bus = _SyncEventBus()
        handlers = _ProxyStubHandlers(
            app=app,
            downstream_service=downstream,
            queue_reader=MagicMock(),
            event_bus=bus,
            startup_wait_sec=wait_sec,
        )
        return handlers, downstream, bus

    def test_html_request_returns_loading_page_without_waiting(self):
        """下游未就绪时，HTML 提示页仍应立即返回，不应等待就绪事件。"""

        async def scenario() -> None:
            handlers, _, bus = self._make_handlers(ready=False)
            req = make_mocked_request(
                "GET", "/", headers={"Accept": "text/html"}
            )
            resp = await handlers.proxy_handler(req)
            self.assertEqual(resp.status, 200)
            self.assertEqual(resp.content_type, "text/html")
            self.assertEqual(resp.text, _LOADING_HTML)
            self.assertNotIn(DownstreamReadyChangedEvent, bus.subscribed_events)

        asyncio.run(scenario())

    def test_api_request_waits_for_ready_event_then_proxies(self):
        """下游未就绪时，API 请求应等待就绪事件，就绪后继续转发。"""

        async def scenario() -> None:
            handlers, downstream, bus = self._make_handlers(ready=False)
            req = make_mocked_request(
                "GET", "/system_stats", headers={"Accept": "application/json"}
            )
            task = asyncio.create_task(handlers.proxy_handler(req))
            await asyncio.sleep(0)
            downstream.downstream_ready = True
            bus.publish(DownstreamReadyChangedEvent(ready=True))
            resp = await asyncio.wait_for(task, timeout=1)
            self.assertEqual(resp.status, 200)
            self.assertEqual(resp.text, "proxied")

        asyncio.run(scenario())

    def test_api_request_keeps_waiting_after_stale_ready_event(self):
        """等待期间收到就绪事件后下游又立即重启，应继续等待而非误判就绪。"""

        async def scenario() -> None:
            handlers, downstream, bus = self._make_handlers(ready=False)
            req = make_mocked_request(
                "GET", "/system_stats", headers={"Accept": "application/json"}
            )
            task = asyncio.create_task(handlers.proxy_handler(req))
            await asyncio.sleep(0)

            # 就绪后立即再次重启
            downstream.downstream_ready = True
            bus.publish(DownstreamReadyChangedEvent(ready=True))
            downstream.downstream_ready = False
            bus.publish(DownstreamReadyChangedEvent(ready=False))
            await asyncio.sleep(0)

            # 此时下游并未真正就绪，handler 不应误判就绪而完成
            self.assertFalse(task.done())

            # 再次就绪后才应放行
            downstream.downstream_ready = True
            bus.publish(DownstreamReadyChangedEvent(ready=True))
            resp = await asyncio.wait_for(task, timeout=1)
            self.assertEqual(resp.status, 200)
            self.assertEqual(resp.text, "proxied")

        asyncio.run(scenario())

    def test_api_request_returns_503_after_wait_timeout(self):
        """下游未就绪且等待超时仍未就绪，API 请求应返回错误。"""

        async def scenario() -> None:
            handlers, _, _ = self._make_handlers(ready=False, wait_sec=0.05)
            req = make_mocked_request(
                "GET", "/system_stats", headers={"Accept": "application/json"}
            )
            resp = await handlers.proxy_handler(req)
            self.assertEqual(resp.status, 503)

        asyncio.run(scenario())

    def test_local_jobs_api_bypasses_wait_when_not_ready(self):
        """网关本地 jobs API 不应受下游未就绪影响，无需等待就绪事件。"""

        async def scenario() -> None:
            handlers, _, bus = self._make_handlers(ready=False)
            req = make_mocked_request(
                "GET", "/api/jobs", headers={"Accept": "application/json"}
            )
            resp = await handlers.proxy_handler(req)
            self.assertEqual(resp.status, 200)
            self.assertNotIn(DownstreamReadyChangedEvent, bus.subscribed_events)

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
