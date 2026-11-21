import asyncio
import json
from dataclasses import dataclass, replace
from typing import Callable, Optional, Set

import aiohttp
from aiohttp import web

from gateway.application.facade import AppFacade
from gateway.shared.events import (
    DownstreamStateChanged,
    StateChangedEvent,
    StatusChangedEvent,
)
from gateway.shared.interfaces import EventBus, JobQueueReader, Timer
from gateway.shared.models import DownstreamState, JobFilters, JobStatus


def _active_job_filter() -> JobFilters:
    """构造 SSE/WS 共用的「待执行」筛选条件：排队中与正在运行之和。

    JobFilters 可变，每次返回新实例，避免共享实例被某处意外修改而污染广播。
    """
    return JobFilters([JobStatus.PENDING, JobStatus.RUNNING])


# WS 状态帧防抖窗口：合并短时间内的连续队列变化
_DEFAULT_WS_DEBOUNCE_SEC = 0.1


@dataclass(frozen=True)
class ClientStatus:
    """客户端可见状态的单一快照，由它序列化 SSE 帧。"""

    paused: bool
    queue_remaining: int
    downstream: Optional[DownstreamState]

    def to_sse_data(self) -> str:
        """序列化为 SSE data 帧的 JSON 内容。"""
        return json.dumps(
            {
                "paused": self.paused,
                "queue_remaining": self.queue_remaining,
                "downstream": (
                    self.downstream.value if self.downstream is not None else None
                ),
            }
        )


class StatusFanout:
    """客户端可见状态的生产、快照与广播枢纽。

    自主订阅领域事件，把 `paused` / `queue_remaining` / `downstream` 收敛到单一
    `ClientStatus` 快照，并向 SSE/WS 广播。领域层仍是队列计数的唯一权威：建连
    初始快照与 WS 改写都经 `JobQueueReader` 读取实时计数，不依赖事件值。
    """

    def __init__(
        self,
        app: AppFacade,
        queue_reader: JobQueueReader,
        event_bus: EventBus,
        timer: Timer,
        loop: asyncio.AbstractEventLoop,
        debounce_sec: float = _DEFAULT_WS_DEBOUNCE_SEC,
    ) -> None:
        self._app = app
        self._queue_reader = queue_reader
        self._timer = timer
        self._loop = loop
        self._debounce_sec = debounce_sec

        self._status = ClientStatus(paused=False, queue_remaining=0, downstream=None)
        self._sse_clients: Set[asyncio.Queue[str]] = set()
        self._ws_clients: Set[web.WebSocketResponse] = set()
        self._cancel_debounce: Optional[Callable[[], None]] = None
        self._broadcast_task: Optional[asyncio.Task[None]] = None

        # 领域事件可能来自任意线程，先调度回事件循环线程再改快照与广播，
        # 避免跨线程操作 asyncio.Queue 与非线程安全的状态。
        self._unsub_status = event_bus.subscribe(
            StatusChangedEvent, self._on_status_changed
        )
        self._unsub_state = event_bus.subscribe(
            StateChangedEvent, self._on_state_changed
        )
        self._unsub_downstream = event_bus.subscribe(
            DownstreamStateChanged, self._on_downstream_changed
        )

    @property
    def sse_clients(self) -> Set[asyncio.Queue[str]]:
        """活跃 SSE 帧队列，供表示层检查连接生命周期。"""
        return self._sse_clients

    @property
    def ws_clients(self) -> Set[web.WebSocketResponse]:
        """活跃 WS 连接，供表示层检查连接生命周期。"""
        return self._ws_clients

    def connect_sse(self) -> tuple[asyncio.Queue[str], ClientStatus]:
        """建立 SSE 逻辑连接：刷新权威快照并注册帧队列，返回队列与初始快照。"""
        state = self._app.get_state.handle()
        self._status = ClientStatus(
            paused=state.paused,
            queue_remaining=self._queue_reader.count(filter_by=_active_job_filter()),
            downstream=state.downstream_state,
        )
        q: asyncio.Queue[str] = asyncio.Queue()
        self._sse_clients.add(q)
        return q, self._status

    def disconnect_sse(self, q: asyncio.Queue[str]) -> None:
        """SSE 连接结束时注销帧队列。"""
        self._sse_clients.discard(q)

    def add_ws_client(self, ws: web.WebSocketResponse) -> None:
        """注册 WS 客户端，使其接收防抖后的状态帧。"""
        self._ws_clients.add(ws)

    def discard_ws_client(self, ws: web.WebSocketResponse) -> None:
        """WS 连接结束时注销客户端。"""
        self._ws_clients.discard(ws)

    def get_queue_remaining(self) -> int:
        """返回改写瞬间的实时队列计数（PENDING + RUNNING）。"""
        return self._queue_reader.count(filter_by=_active_job_filter())

    def dispose(self) -> None:
        """释放订阅与定时器，通知 SSE 客户端关闭并收敛 WS 连接。"""
        self._unsub_status()
        self._unsub_state()
        self._unsub_downstream()
        if self._cancel_debounce is not None:
            self._cancel_debounce()
            self._cancel_debounce = None
        if self._broadcast_task is not None and not self._broadcast_task.done():
            self._broadcast_task.cancel()

        for q in list(self._sse_clients):
            try:
                q.put_nowait("shutdown")
            except asyncio.QueueFull:
                pass

        if self._ws_clients:
            self._loop.create_task(self._close_ws_clients())

    # #region 事件处理
    def _on_status_changed(self, ev: StatusChangedEvent) -> None:
        self._dispatch_to_loop(self._apply_status, ev.queue_remaining)

    def _on_state_changed(self, ev: StateChangedEvent) -> None:
        self._dispatch_to_loop(self._apply_paused, ev.paused)

    def _on_downstream_changed(self, ev: DownstreamStateChanged) -> None:
        self._dispatch_to_loop(self._apply_downstream, ev.state)

    def _dispatch_to_loop(self, action: Callable[..., None], *args: object) -> None:
        """把状态变更投递到事件循环线程，保证快照与广播串行化。"""
        self._loop.call_soon_threadsafe(action, *args)

    def _apply_status(self, remaining: int) -> None:
        self._status = replace(self._status, queue_remaining=remaining)
        self._broadcast_sse()
        self._schedule_ws_broadcast()

    def _apply_paused(self, paused: bool) -> None:
        self._status = replace(self._status, paused=paused)
        self._broadcast_sse()

    def _apply_downstream(self, state: DownstreamState) -> None:
        self._status = replace(self._status, downstream=state)
        self._broadcast_sse()

    # #endregion

    # #region 广播
    def _broadcast_sse(self) -> None:
        data = self._status.to_sse_data()
        for q in list(self._sse_clients):
            try:
                q.put_nowait(data)
            except asyncio.QueueFull:
                pass

    def _schedule_ws_broadcast(self) -> None:
        if self._cancel_debounce is not None:
            return
        self._cancel_debounce = self._timer.start_timeout(
            self._debounce_sec, self._on_debounce_elapsed
        )

    def _on_debounce_elapsed(self) -> None:
        self._cancel_debounce = None
        self._broadcast_task = self._loop.create_task(self._broadcast_ws_frame())

    async def _broadcast_ws_frame(self) -> None:
        msg = json.dumps(
            {
                "type": "status",
                "data": {
                    "status": {
                        "exec_info": {"queue_remaining": self._status.queue_remaining}
                    }
                },
            }
        )
        for ws in list(self._ws_clients):
            try:
                if not ws.closed:
                    await ws.send_str(msg)
            except (ConnectionResetError, aiohttp.WebSocketError):
                pass

    async def _close_ws_clients(self) -> None:
        async def close(ws: web.WebSocketResponse) -> None:
            try:
                if not ws.closed:
                    await ws.close(code=1001, message=b"Server shutting down")
            except (ConnectionResetError, RuntimeError):
                pass

        await asyncio.gather(
            *(close(ws) for ws in list(self._ws_clients)), return_exceptions=True
        )

    # #endregion
