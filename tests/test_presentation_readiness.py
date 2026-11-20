import asyncio
import json
import unittest
from contextlib import suppress
from unittest.mock import AsyncMock, MagicMock, patch

from aiohttp.test_utils import make_mocked_request

from gateway.presentation.handlers import GatewayHandlers
from gateway.shared.events import (
    DownstreamReadyChangedEvent,
    DownstreamStateChanged,
)
from gateway.shared.interfaces import DownstreamClient
from gateway.shared.models import DownstreamState, GatewayState, Reason
from tests.test_gateway_handlers import (
    _FakeSseResponse,
    _ProxyStubHandlers,
    _SyncEventBus,
)


class _MutableState:
    """可被测试翻转的门面状态快照。"""

    def __init__(self, state: DownstreamState) -> None:
        self.state = state
        self.handle = MagicMock(side_effect=self._snapshot)

    def _snapshot(self) -> GatewayState:
        return GatewayState(
            paused=False,
            allow_idle_shutdown=False,
            downstream_state=self.state,
        )


def _make_handlers(state: DownstreamState, wait_sec: float = 0.05):
    app = MagicMock()
    app.get_state = _MutableState(state)
    app.get_jobs.handle = AsyncMock(return_value=[])
    app.get_job_count.handle = AsyncMock(return_value=0)
    downstream = MagicMock(spec=DownstreamClient)
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


class TestProxyGateUsesAuthoritativeState(unittest.TestCase):
    """代理门控读门面查询的领域状态，就绪等待订阅权威事件。"""

    def test_gate_blocks_when_domain_state_not_ready(self):
        """门面状态未就绪时直接等待并超时返回 503，不读基础设施就绪属性。"""

        async def scenario() -> None:
            handlers, app, _, bus = _make_handlers(DownstreamState.STARTING)
            req = make_mocked_request(
                "GET", "/system_stats", headers={"Accept": "application/json"}
            )
            resp = await handlers.proxy_handler(req)
            self.assertEqual(resp.status, 503)
            app.get_state.handle.assert_called()
            self.assertIn(DownstreamStateChanged, bus.subscribed_events)
            self.assertNotIn(DownstreamReadyChangedEvent, bus.subscribed_events)

        asyncio.run(scenario())

    def test_authoritative_ready_event_releases_wait(self):
        """权威状态事件到达且门面确认 READY 后才放行代理请求。"""

        async def scenario() -> None:
            handlers, app, _, _ = _make_handlers(DownstreamState.STARTING)
            req = make_mocked_request(
                "GET", "/system_stats", headers={"Accept": "application/json"}
            )
            task = asyncio.create_task(handlers.proxy_handler(req))
            await asyncio.sleep(0)
            app.get_state.state = DownstreamState.READY
            app.get_state.handle()
            handlers._event_bus.publish(
                DownstreamStateChanged(
                    state=DownstreamState.READY, reason=Reason.READY_POLL
                )
            )
            resp = await asyncio.wait_for(task, timeout=1)
            self.assertEqual(resp.status, 200)
            self.assertEqual(resp.text, "proxied")

        asyncio.run(scenario())

    def test_stale_ready_event_does_not_release_wait(self):
        """权威事件说 READY 但门面仍是 STARTING 时应继续等待。"""

        async def scenario() -> None:
            handlers, app, _, _ = _make_handlers(DownstreamState.STARTING)
            req = make_mocked_request(
                "GET", "/system_stats", headers={"Accept": "application/json"}
            )
            task = asyncio.create_task(handlers.proxy_handler(req))
            await asyncio.sleep(0)
            handlers._event_bus.publish(
                DownstreamStateChanged(
                    state=DownstreamState.READY, reason=Reason.READY_POLL
                )
            )
            await asyncio.sleep(0)
            self.assertFalse(task.done())

            app.get_state.state = DownstreamState.READY
            handlers._event_bus.publish(
                DownstreamStateChanged(
                    state=DownstreamState.READY, reason=Reason.READY_POLL
                )
            )
            resp = await asyncio.wait_for(task, timeout=1)
            self.assertEqual(resp.status, 200)

        asyncio.run(scenario())


class TestSseReadsAuthoritativeState(unittest.TestCase):
    """SSE 输出的状态值来自同一权威状态，且不携带 reason。"""

    def test_sse_frames_carry_authoritative_downstream_state(self):
        """连接快照与增量推送都携带 ready/starting/shutdown 且不暴露 reason。"""

        async def scenario() -> None:
            handlers, app, _, bus = _make_handlers(DownstreamState.READY)
            req = make_mocked_request("GET", "/sse")
            response = _FakeSseResponse()
            with patch(
                "gateway.presentation.handlers.web.StreamResponse",
                return_value=response,
            ):
                task = asyncio.create_task(handlers.handle_sse(req))
                for _ in range(200):
                    if response.frames():
                        break
                    await asyncio.sleep(0.005)
                self.assertEqual(
                    response.frames()[0],
                    {"paused": False, "queue_remaining": 0, "downstream": "ready"},
                )

                bus.publish(
                    DownstreamStateChanged(
                        state=DownstreamState.STARTING, reason=Reason.RESTART
                    )
                )
                for _ in range(200):
                    if len(response.frames()) >= 2:
                        break
                    await asyncio.sleep(0.005)
                self.assertEqual(
                    response.frames()[1],
                    {
                        "paused": False,
                        "queue_remaining": 0,
                        "downstream": "starting",
                    },
                )
                self.assertNotIn("reason", json.dumps(response.frames()[1]))
                app.get_state.handle.assert_called()

            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
