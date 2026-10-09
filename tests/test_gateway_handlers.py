import asyncio
import json
import unittest
from contextlib import suppress
from unittest.mock import AsyncMock, MagicMock, patch

from aiohttp import web
from aiohttp.test_utils import make_mocked_request

from gateway.presentation.handlers import (
    GatewayHandlers,
    _EXT_JS_URL,
    _LOADING_HTML,
)
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
    """重写默认代理与扩展列表代理以返回哨兵响应，避免测试触发真实 HTTP 转发。"""

    async def _handle_default_proxy(self, request, method, downstream_url):
        return web.Response(status=200, text="proxied")

    async def _handle_extensions_proxy(self, downstream_url):
        return web.Response(status=200, text="[]")


class _FakeWebSocket:
    """测试用假 WebSocket：连接建立后立即结束，不产生任何消息。"""

    closed = False

    async def prepare(self, request):
        return None

    async def send_str(self, data):
        return None

    async def send_bytes(self, data):
        return None

    async def ping(self):
        return None

    async def pong(self):
        return None

    async def close(self, code=None, message=b""):
        self.closed = True

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration


class _FakeClientSession:
    """测试用假 HTTP 会话：WebSocket 连接立即建立并结束。"""

    def __init__(self, *args, **kwargs) -> None:
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False

    def ws_connect(self, url):
        return _FakeWebSocket()


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
        return handlers, app, downstream, bus

    def test_html_request_returns_loading_page_without_waiting(self):
        """下游未就绪时，HTML 提示页仍应立即返回，不应等待就绪事件。"""

        async def scenario() -> None:
            handlers, _, _, bus = self._make_handlers(ready=False)
            req = make_mocked_request("GET", "/", headers={"Accept": "text/html"})
            resp = await handlers.proxy_handler(req)
            self.assertEqual(resp.status, 200)
            self.assertEqual(resp.content_type, "text/html")
            self.assertEqual(resp.text, _LOADING_HTML)
            self.assertNotIn(DownstreamReadyChangedEvent, bus.subscribed_events)

        asyncio.run(scenario())

    def test_api_request_waits_for_ready_event_then_proxies(self):
        """下游未就绪时，API 请求应等待就绪事件，就绪后继续转发。"""

        async def scenario() -> None:
            handlers, _, downstream, bus = self._make_handlers(ready=False)
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
            handlers, _, downstream, bus = self._make_handlers(ready=False)
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
            handlers, _, _, _ = self._make_handlers(ready=False, wait_sec=0.05)
            req = make_mocked_request(
                "GET", "/system_stats", headers={"Accept": "application/json"}
            )
            resp = await handlers.proxy_handler(req)
            self.assertEqual(resp.status, 503)

        asyncio.run(scenario())

    def test_local_jobs_api_bypasses_wait_when_not_ready(self):
        """网关本地 jobs API 不应受下游未就绪影响，无需等待就绪事件。"""

        async def scenario() -> None:
            handlers, _, _, bus = self._make_handlers(ready=False)
            req = make_mocked_request(
                "GET", "/api/jobs", headers={"Accept": "application/json"}
            )
            resp = await handlers.proxy_handler(req)
            self.assertEqual(resp.status, 200)
            self.assertNotIn(DownstreamReadyChangedEvent, bus.subscribed_events)

        asyncio.run(scenario())


class TestGatewayHandlersActivity(unittest.TestCase):
    """测试 proxy_handler 的活动来源判定（#11）。"""

    def _make_handlers(self, ready=True):
        app = MagicMock()
        app.get_jobs.handle = AsyncMock(return_value=[])
        app.get_job_count.handle = AsyncMock(return_value=0)
        app.get_state.handle = MagicMock(return_value=False)
        app.add_job.handle = MagicMock(
            return_value=MagicMock(prompt_id="prompt_1", number=1)
        )
        downstream = MagicMock(spec=DownstreamClient)
        downstream.downstream_ready = ready
        downstream.downstream_port = 8188
        handlers = _ProxyStubHandlers(
            app=app,
            downstream_service=downstream,
            queue_reader=MagicMock(),
            event_bus=_SyncEventBus(),
            startup_wait_sec=0.05,
        )
        return handlers, app

    def test_proxied_request_reports_activity(self):
        """转发给下游的普通请求应上报一次活动（#11）。"""

        async def scenario() -> None:
            handlers, app = self._make_handlers()
            req = make_mocked_request("GET", "/system_stats")
            resp = await handlers.proxy_handler(req)

            self.assertEqual(resp.text, "proxied")
            app.report_activity.assert_called_once_with()

        asyncio.run(scenario())

    def test_local_jobs_api_reports_activity(self):
        """网关本地处理的 jobs API 也应上报活动（#11）。"""

        async def scenario() -> None:
            handlers, app = self._make_handlers()
            resp = await handlers.proxy_handler(make_mocked_request("GET", "/api/jobs"))

            self.assertEqual(resp.status, 200)
            app.report_activity.assert_called_once_with()

        asyncio.run(scenario())

    def test_prompt_submission_reports_activity(self):
        """任务提交应上报一次活动（#11）。"""

        async def scenario() -> None:
            handlers, app = self._make_handlers()
            payload = MagicMock()
            payload.readany = AsyncMock(
                side_effect=[json.dumps({"prompt": {}}).encode(), b""]
            )
            req = make_mocked_request(
                "POST",
                "/prompt",
                headers={"Content-Type": "application/json"},
                payload=payload,
            )
            resp = await handlers.proxy_handler(req)

            self.assertEqual(resp.status, 200)
            app.report_activity.assert_called_once_with()

        asyncio.run(scenario())

    def test_extension_pipeline_does_not_report_activity(self):
        """扩展加载管道不是用户活动（#11）。"""

        async def scenario() -> None:
            handlers, app = self._make_handlers()

            resp = await handlers.proxy_handler(
                make_mocked_request("GET", "/api/extensions")
            )
            self.assertEqual(resp.status, 200)
            app.report_activity.assert_not_called()

            resp = await handlers.proxy_handler(make_mocked_request("GET", _EXT_JS_URL))
            self.assertEqual(resp.content_type, "application/javascript")
            app.report_activity.assert_not_called()

        asyncio.run(scenario())

    def test_loading_page_does_not_report_activity(self):
        """下游未就绪时返回的 HTML 提示页不是活动（#11）。"""

        async def scenario() -> None:
            handlers, app = self._make_handlers(ready=False)
            req = make_mocked_request("GET", "/", headers={"Accept": "text/html"})
            resp = await handlers.proxy_handler(req)

            self.assertEqual(resp.text, _LOADING_HTML)
            app.report_activity.assert_not_called()

        asyncio.run(scenario())

    def test_control_endpoints_do_not_report_activity(self):
        """网关自身的控制与状态接口不是活动（#11）。"""

        async def scenario() -> None:
            handlers, app = self._make_handlers()
            req = make_mocked_request("GET", "/ws")

            await handlers.handle_pause(make_mocked_request("POST", "/pause"))
            await handlers.handle_resume(req)
            self.assertEqual((await handlers.handle_state(req)).status, 200)

            sse_task = asyncio.create_task(handlers.handle_sse(req))
            await asyncio.sleep(0.01)
            sse_task.cancel()
            with suppress(asyncio.CancelledError):
                await sse_task

            app.report_activity.assert_not_called()

        asyncio.run(scenario())

    def test_ws_proxy_reports_activity_once(self):
        """下游 WebSocket 代理连接建立时计一次活动，连接存活期间不再产生（#11）。"""

        async def scenario() -> None:
            handlers, app = self._make_handlers()
            req = make_mocked_request("GET", "/ws", headers={"Upgrade": "websocket"})
            with (
                patch("aiohttp.ClientSession", _FakeClientSession),
                patch("aiohttp.web.WebSocketResponse", _FakeWebSocket),
            ):
                resp = await asyncio.wait_for(handlers.proxy_handler(req), timeout=1)

            self.assertIsInstance(resp, _FakeWebSocket)
            app.report_activity.assert_called_once_with()

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
