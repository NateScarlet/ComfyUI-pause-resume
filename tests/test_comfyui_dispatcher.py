import asyncio
import datetime
import os
import socket
import tempfile
import unittest
from typing import Any, Callable, List, Type, TypeVar
from unittest.mock import AsyncMock, MagicMock

from gateway.config import GatewayConfig
from gateway.infrastructure.comfyui.dispatcher import ComfyUIJobDispatcher
from gateway.infrastructure.comfyui.downstream import ComfyUIDownstreamClient
from gateway.infrastructure.file.json_queue import JSONFileQueue
from gateway.shared.events import DispatchFailedEvent
from gateway.shared.exceptions import DownstreamError
from gateway.shared.interfaces import (
    DownstreamClient,
    EventBus,
    ProcessManager,
)
from gateway.shared.models import Job, JobStatus, RawJSON
from tests.fakes import FakeTimer

T = TypeVar("T")


class _RecordingEventBus(EventBus):
    """测试用事件总线：同步记录所有发布的事件。"""

    def __init__(self) -> None:
        self.events: List[object] = []

    def subscribe(
        self, event_class: Type[T], callback: Callable[[T], Any]
    ) -> Callable[[], None]:
        return lambda: None

    def publish(self, event: object) -> None:
        self.events.append(event)


def _closed_local_port() -> int:
    """返回一个无人监听的本地端口，用于制造真实的连接失败。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class TestComfyUIJobDispatcherFailureClassification(unittest.TestCase):
    """派发失败分类：传输层失败退回队列，真实 HTTP 错误码维持永久失败。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._config = MagicMock(spec=GatewayConfig)
        self._config.data_dir = self._tmp.name
        self._queue = JSONFileQueue(os.path.join(self._tmp.name, "queue.json"))
        self._bus = _RecordingEventBus()
        self._loop = asyncio.new_event_loop()
        self.addCleanup(self._loop.close)

    def _enqueue_job(self) -> Job:
        job = Job(
            number=1,
            prompt_id="prompt-under-test",
            prompt=RawJSON("{}"),
            extra_data=RawJSON("{}"),
            outputs_to_execute=[],
            create_time=0,
        )
        self._queue.add(job)
        return job

    def _make_dispatcher(self, downstream: DownstreamClient) -> ComfyUIJobDispatcher:
        return ComfyUIJobDispatcher(
            config=self._config,
            queue_reader=self._queue,
            queue_writer=self._queue,
            downstream=downstream,
            event_bus=self._bus,
            loop=self._loop,
        )

    def _dispatch_once(self, dispatcher: ComfyUIJobDispatcher) -> None:
        asyncio.run(dispatcher._try_send(0))

    def _failed_workflow_dirs(self) -> List[str]:
        root = os.path.join(self._tmp.name, "failed_workflows")
        if not os.path.isdir(root):
            return []
        return sorted(os.listdir(root))

    def _single_failed_event(self) -> DispatchFailedEvent:
        events = [e for e in self._bus.events if isinstance(e, DispatchFailedEvent)]
        self.assertEqual(len(events), 1)
        return events[0]

    def _assert_transient_failure(self, job: Job) -> None:
        queued = self._queue.get(job.prompt_id)
        self.assertIsNotNone(queued)
        assert queued is not None
        self.assertEqual(queued.status, JobStatus.PENDING)
        self.assertFalse(self._single_failed_event().is_permanent)
        self.assertEqual(self._failed_workflow_dirs(), [])

    def _assert_permanent_failure(self, job: Job, status_code: int) -> None:
        self.assertIsNone(self._queue.get(job.prompt_id))
        self.assertTrue(self._single_failed_event().is_permanent)
        expected = f"{datetime.date.today().isoformat()}-{status_code}-{job.prompt_id}"
        self.assertEqual(self._failed_workflow_dirs(), [expected])

    def test_network_error_during_send_prompt_requeues_job(self):
        """派发途中下游被杀掉或重启（未获得任何 HTTP 响应）时，任务应退回队列等待重试。

        真实客户端在这种情况下抛出的是传输层失败，不能伪装成 HTTP 500 而被归档。
        """
        job = self._enqueue_job()
        client = ComfyUIDownstreamClient(
            config=self._config,
            event_bus=self._bus,
            process_manager=MagicMock(spec=ProcessManager),
            timer=FakeTimer(),
            loop=self._loop,
        )
        client._downstream_port = _closed_local_port()

        self._dispatch_once(self._make_dispatcher(client))

        self._assert_transient_failure(job)

    def _failing_downstream(self, status_code: int, message: str) -> DownstreamClient:
        downstream = MagicMock(spec=DownstreamClient)
        downstream.send_prompt = AsyncMock(
            side_effect=DownstreamError(status_code, message)
        )
        return downstream

    def test_http_400_still_fails_job_permanently(self):
        """下游返回 4xx 时行为不变：任务写入 failed_workflows 并移出队列。"""
        job = self._enqueue_job()
        self._dispatch_once(
            self._make_dispatcher(self._failing_downstream(400, "bad prompt"))
        )
        self._assert_permanent_failure(job, 400)

    def test_http_500_still_fails_job_permanently(self):
        """下游返回 5xx 时行为不变：任务写入 failed_workflows 并移出队列。"""
        job = self._enqueue_job()
        self._dispatch_once(
            self._make_dispatcher(self._failing_downstream(500, "server error"))
        )
        self._assert_permanent_failure(job, 500)


if __name__ == "__main__":
    unittest.main()
