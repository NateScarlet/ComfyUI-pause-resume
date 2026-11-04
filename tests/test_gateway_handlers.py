import asyncio
import json
import unittest
from contextlib import suppress
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
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
from gateway.shared.models import DownstreamState, GatewayState


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

    async def ws_connect(self, url):
        return _FakeWebSocket()


_EOF = object()


class _WsMsg:
    """测试用 WebSocket 消息，仅携带转发逻辑关心的字段。"""

    def __init__(self, type_, data=None):
        self.type = type_
        self.data = data


class _ScriptedWebSocket:
    """测试用脚本化 WebSocket：按预设队列产出消息，队列未喂入时挂起等待。

    队列元素可以是消息、`_EOF`（模拟对端正常 EOF）或异常实例（模拟异常断开）。
    """

    def __init__(self, messages=()):
        self._inbox: asyncio.Queue = asyncio.Queue()
        for msg in messages:
            self._inbox.put_nowait(msg)
        self.closed = False
        self.close_attempts = 0
        self.sent_text = []
        self.sent_bytes = []
        self.pings = 0
        self.pongs = 0

    async def prepare(self, request):
        return None

    async def send_str(self, data):
        self.sent_text.append(data)

    async def send_bytes(self, data):
        self.sent_bytes.append(data)

    async def ping(self):
        self.pings += 1

    async def pong(self):
        self.pongs += 1

    async def close(self, code=None, message=b""):
        self.close_attempts += 1
        if self.closed:
            return False
        self.closed = True
        return True

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self._inbox.get()
        if item is _EOF:
            raise StopAsyncIteration
        if isinstance(item, BaseException):
            raise item
        return item


class _ScriptedClientSession:
    """测试用 HTTP 会话：返回预先构造的下游 WebSocket。"""

    def __init__(self, ws):
        self._ws = ws

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False

    async def ws_connect(self, url):
        return self._ws


class _FailingClientSession:
    """测试用 HTTP 会话：下游 WebSocket 握手直接失败。"""

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False

    async def ws_connect(self, url):
        raise aiohttp.ClientError("downstream unavailable")


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

    def test_shutdown_page_request_wakes_downstream_and_returns_loading_page(self):
        """停机期间页面 GET 必须先触发唤醒，再返回自动刷新的加载页（#16）。"""

        async def scenario() -> None:
            handlers, app, _, _ = self._make_handlers(ready=False, wait_sec=0.05)
            req = make_mocked_request("GET", "/", headers={"Accept": "text/html"})
            resp = await handlers.proxy_handler(req)

            app.wake_downstream.handle.assert_called_once_with()
            self.assertEqual(resp.status, 200)
            self.assertEqual(resp.text, _LOADING_HTML)

        asyncio.run(scenario())

    def test_api_request_wakes_downstream_before_waiting(self):
        """停机期间 API 请求先触发唤醒，等待超时后返回 503 而非 502（#16）。"""

        async def scenario() -> None:
            handlers, app, _, _ = self._make_handlers(ready=False, wait_sec=0.05)
            req = make_mocked_request(
                "GET", "/system_stats", headers={"Accept": "application/json"}
            )
            resp = await handlers.proxy_handler(req)

            app.wake_downstream.handle.assert_called_once_with()
            self.assertEqual(resp.status, 503)

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


class TestGatewayHandlersState(unittest.TestCase):
    """测试 /state 对下游停机状态的呈现（#16）。"""

    def test_state_reports_downstream_shutdown(self):
        """下游处于 Shutdown 时，/state 必须如实反映且包含授权勾选。"""

        async def scenario() -> None:
            app = MagicMock()
            app.get_state.handle = MagicMock(
                return_value=GatewayState(
                    paused=False,
                    allow_idle_shutdown=True,
                    downstream_state=DownstreamState.SHUTDOWN,
                )
            )
            handlers = GatewayHandlers(
                app=app,
                downstream_service=MagicMock(spec=DownstreamClient),
                queue_reader=MagicMock(),
                event_bus=_SyncEventBus(),
                startup_wait_sec=0.05,
            )
            resp = await handlers.handle_state(
                make_mocked_request("GET", "/state")
            )
            payload = json.loads(resp.text)
            self.assertEqual(payload["paused"], False)
            self.assertEqual(payload["allow_idle_shutdown"], True)
            self.assertEqual(payload["downstream"], "shutdown")

        asyncio.run(scenario())


class TestGatewayHandlersActivity(unittest.TestCase):
    """测试 proxy_handler 的活动来源判定（#11）。"""

    def _make_handlers(self, ready=True):
        app = MagicMock()
        app.get_jobs.handle = AsyncMock(return_value=[])
        app.get_job_count.handle = AsyncMock(return_value=0)
        app.get_state.handle = MagicMock(
            return_value=GatewayState(
                paused=False,
                allow_idle_shutdown=False,
                downstream_state=DownstreamState.READY,
            )
        )
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


class TestGatewayHandlersWsProxyLifecycle(unittest.TestCase):
    """WS 代理的成对生命周期：任一侧结束都必须收敛并关闭两侧（#18）。"""

    def _make_handlers(self):
        app = MagicMock()
        downstream = MagicMock(spec=DownstreamClient)
        downstream.downstream_ready = True
        downstream.downstream_port = 8188
        return GatewayHandlers(
            app=app,
            downstream_service=downstream,
            queue_reader=MagicMock(),
            event_bus=_SyncEventBus(),
            startup_wait_sec=0.05,
        )

    def _assert_no_pending_forward_tasks(self):
        """断言当前事件循环中没有遗留的转发任务。"""
        current = asyncio.current_task()
        pending = [t for t in asyncio.all_tasks() if t is not current and not t.done()]
        self.assertEqual(pending, [])

    def test_browser_is_closed_when_downstream_ends_without_close_frame(self):
        """下游以 EOF 结束且未发 CLOSE 帧时，浏览器侧仍必须被 close（#18）。"""

        async def scenario() -> None:
            handlers = self._make_handlers()
            browser = _ScriptedWebSocket()
            downstream = _ScriptedWebSocket(messages=[_EOF])
            req = make_mocked_request("GET", "/ws", headers={"Upgrade": "websocket"})
            with (
                patch(
                    "aiohttp.ClientSession", lambda: _ScriptedClientSession(downstream)
                ),
                patch("aiohttp.web.WebSocketResponse", lambda: browser),
            ):
                resp = await asyncio.wait_for(handlers.proxy_handler(req), timeout=1)

            self.assertIs(resp, browser)
            self.assertTrue(browser.closed)
            self.assertEqual(browser.close_attempts, 1)
            self.assertEqual(handlers.ws_clients, set())
            self._assert_no_pending_forward_tasks()

        asyncio.run(scenario())

    def test_downstream_is_closed_when_browser_disconnects(self):
        """浏览器侧异常断开时，下游连接也必须被关闭（#18）。"""

        async def scenario() -> None:
            handlers = self._make_handlers()
            browser = _ScriptedWebSocket(
                messages=[ConnectionResetError("browser disconnected")]
            )
            downstream = _ScriptedWebSocket()
            req = make_mocked_request("GET", "/ws", headers={"Upgrade": "websocket"})
            with (
                patch(
                    "aiohttp.ClientSession", lambda: _ScriptedClientSession(downstream)
                ),
                patch("aiohttp.web.WebSocketResponse", lambda: browser),
            ):
                await asyncio.wait_for(handlers.proxy_handler(req), timeout=1)

            self.assertTrue(downstream.closed)
            self.assertEqual(downstream.close_attempts, 1)
            self.assertEqual(browser.close_attempts, 1)
            self.assertEqual(handlers.ws_clients, set())
            self._assert_no_pending_forward_tasks()

        asyncio.run(scenario())

    def test_normal_close_frame_closes_both_sides_once(self):
        """正常 CLOSE 帧路径两侧都被关闭，且每侧只 close 一次（#18）。"""

        async def scenario() -> None:
            handlers = self._make_handlers()
            browser = _ScriptedWebSocket()
            downstream = _ScriptedWebSocket(
                messages=[_WsMsg(aiohttp.WSMsgType.CLOSE)]
            )
            req = make_mocked_request("GET", "/ws", headers={"Upgrade": "websocket"})
            with (
                patch(
                    "aiohttp.ClientSession", lambda: _ScriptedClientSession(downstream)
                ),
                patch("aiohttp.web.WebSocketResponse", lambda: browser),
            ):
                await asyncio.wait_for(handlers.proxy_handler(req), timeout=1)

            self.assertTrue(browser.closed)
            self.assertTrue(downstream.closed)
            self.assertEqual(browser.close_attempts, 1)
            self.assertEqual(downstream.close_attempts, 1)
            self.assertEqual(handlers.ws_clients, set())
            self._assert_no_pending_forward_tasks()

        asyncio.run(scenario())

    def test_cancelled_handler_closes_both_sides(self):
        """处理任务被取消时同样必须收敛：关闭两侧且不留下转发任务（#18）。"""

        async def scenario() -> None:
            handlers = self._make_handlers()
            browser = _ScriptedWebSocket()
            downstream = _ScriptedWebSocket()
            req = make_mocked_request("GET", "/ws", headers={"Upgrade": "websocket"})
            with (
                patch(
                    "aiohttp.ClientSession", lambda: _ScriptedClientSession(downstream)
                ),
                patch("aiohttp.web.WebSocketResponse", lambda: browser),
            ):
                task = asyncio.create_task(handlers.proxy_handler(req))
                # 让两个转发方向都进入等待状态后再取消
                await asyncio.sleep(0.01)
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

            self.assertTrue(browser.closed)
            self.assertTrue(downstream.closed)
            self.assertEqual(handlers.ws_clients, set())
            self._assert_no_pending_forward_tasks()

        asyncio.run(scenario())

    def test_ws_handshake_fails_before_upgrade_when_downstream_unavailable(self):
        """下游连不上时必须在升级浏览器连接之前快速失败，不得接住后静默丢弃（#18）。"""

        async def scenario() -> None:
            handlers = self._make_handlers()
            req = make_mocked_request("GET", "/ws", headers={"Upgrade": "websocket"})
            with (
                patch("aiohttp.ClientSession", _FailingClientSession),
                patch("aiohttp.web.WebSocketResponse") as ws_response,
            ):
                resp = await asyncio.wait_for(handlers.proxy_handler(req), timeout=1)

            self.assertEqual(resp.status, 503)
            ws_response.assert_not_called()
            self.assertEqual(handlers.ws_clients, set())

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
