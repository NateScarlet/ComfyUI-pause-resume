import asyncio
from typing import Callable, List
from unittest.mock import MagicMock

from gateway.presentation.status_fanout import StatusFanout
from gateway.shared.interfaces import MemorySampler, Timer


class FakeMemorySampler(MemorySampler):
    """可控的可用内存采样替身：默认内存充足，可显式设定采样值。"""

    def __init__(self, available_mb: int = 8192) -> None:
        self.available_mb = available_mb
        self.sample_count = 0

    def available_memory_mb(self) -> int:
        self.sample_count += 1
        return self.available_mb


class FakeTimer(Timer):
    """可控时间的测试定时器：只有显式推进时间才会触发到期的回调。"""

    @property
    def now(self) -> float:
        return self._now

    class _Task:
        def __init__(self, deadline: float, callback: Callable[[], None]) -> None:
            self.deadline = deadline
            self.callback = callback
            self.done = False

    def __init__(self) -> None:
        self._now = 0.0
        self._tasks: List[FakeTimer._Task] = []

    def start_timeout(
        self, seconds: float, callback: Callable[[], None]
    ) -> Callable[[], None]:
        task = FakeTimer._Task(self._now + seconds, callback)
        self._tasks.append(task)

        def cancel() -> None:
            task.done = True

        return cancel

    @property
    def pending_count(self) -> int:
        return sum(1 for t in self._tasks if not t.done)

    def advance(self, seconds: float) -> None:
        """推进时间，并按到期先后顺序触发所有尚未取消的回调。"""
        deadline = self._now + seconds
        while True:
            due = [t for t in self._tasks if not t.done and t.deadline <= deadline]
            if not due:
                break
            task = min(due, key=lambda t: t.deadline)
            task.done = True
            self._now = task.deadline
            task.callback()
        self._now = deadline


def make_fanout(app, bus, queue_remaining=0, timer=None, loop=None):
    """构造测试用 fanout；queue_reader 用替身，计数由参数给定。"""
    reader = MagicMock()
    reader.count.return_value = queue_remaining
    fanout = StatusFanout(
        app=app,
        queue_reader=reader,
        event_bus=bus,
        timer=timer if timer is not None else FakeTimer(),
        loop=loop if loop is not None else asyncio.get_running_loop(),
    )
    return fanout, reader