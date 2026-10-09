import unittest
from dataclasses import replace
from typing import Callable, List, Optional
from unittest.mock import MagicMock
from gateway.domain.gateway import Gateway
from gateway.domain.estimation_service import EstimationService
from gateway.domain.crash_skip_policy import CrashSkipPolicy
from gateway.shared.models import Job, JobStatus, JobFilters, QueueState, RawJSON
from gateway.shared.interfaces import (
    StateRepository,
    JobQueueReader,
    JobQueueWriter,
    ProcessManager,
    SystemPowerController,
    Timer,
    DownstreamClient,
    JobDispatcher,
    EventBus,
)
from gateway.shared.events import (
    StateChangedEvent,
    StatusChangedEvent,
    DownstreamExecutingChangedEvent,
    DownstreamReadyChangedEvent,
    DownstreamCrashedEvent,
    QueueModifiedEvent,
    DispatchSuccessEvent,
    DispatchFailedEvent,
    ScriptStateChangedEvent,
)


class MockEventBus(EventBus):
    """用于测试的 Mock 事件总线。"""

    def __init__(self) -> None:
        self.subscribers = {}
        self.published = []

    def subscribe(self, event_class, callback):
        if event_class not in self.subscribers:
            self.subscribers[event_class] = []
        self.subscribers[event_class].append(callback)
        return lambda: self.subscribers[event_class].remove(callback)

    def publish(self, event):
        self.published.append(event)
        event_class = type(event)
        if event_class in self.subscribers:
            for cb in self.subscribers[event_class]:
                cb(event)


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


def _make_gateway(**kwargs) -> Gateway:  # type: ignore[no-untyped-def]
    """用 mock 的各个接口依赖快速构造测试用 Gateway。"""
    paused = kwargs.pop("paused", False)
    pending_count = kwargs.pop("pending_count", 0)
    pending = kwargs.pop("pending", None)
    running = kwargs.pop("running", None)
    crash_count = kwargs.pop("crash_count", 0)
    dispatch_skip_offset = kwargs.pop("dispatch_skip_offset", 0)

    if pending is None:
        pending = []
        for i in range(pending_count):
            t = MagicMock(spec=Job)
            t.prompt_id = f"pending_id_{i}"
            pending.append(t)
    else:
        pending_count = len(pending)

    if running is None:
        running = []

    repo = MagicMock(spec=StateRepository)
    repo.get_paused.return_value = paused

    reader = MagicMock(spec=JobQueueReader)

    def _default_list(filter_by=None, limit=None, offset=None, desc=False):
        statuses = filter_by.statuses if filter_by is not None else None
        result = []
        if not statuses:
            result.extend(running)
            result.extend(pending)
        else:
            if JobStatus.RUNNING in statuses:
                result.extend(running)
            if JobStatus.PENDING in statuses:
                result.extend(pending)
        return result

    reader.list.side_effect = _default_list

    def _default_count(filter_by=None, limit=None):
        statuses = filter_by.statuses if filter_by is not None else None
        if not statuses:
            cnt = pending_count + len(running)
        else:
            cnt = 0
            if JobStatus.PENDING in statuses:
                cnt += pending_count
            if JobStatus.RUNNING in statuses:
                cnt += len(running)
        if limit is not None:
            return min(cnt, limit)
        return cnt

    reader.count.side_effect = _default_count

    writer = MagicMock(spec=JobQueueWriter)
    writer.requeue_running_if_exists.return_value = False

    pm = MagicMock(spec=ProcessManager)
    pm.is_running.return_value = False

    power = MagicMock(spec=SystemPowerController)

    timer = kwargs.pop("timer", None)
    timer_cancel = MagicMock()
    if timer is None:
        timer = MagicMock(spec=Timer)
        timer.now = 0.0
        timer.start_timeout.return_value = timer_cancel

    downstream = MagicMock(spec=DownstreamClient)
    dispatcher = MagicMock(spec=JobDispatcher)
    event_bus = MockEventBus()

    # 创建 mock 的 EstimationService
    estimation_service = MagicMock(spec=EstimationService)
    estimation_service.calculate_estimation.return_value = None

    g = Gateway(
        state_repo=repo,
        queue_reader=reader,
        queue_writer=writer,
        process_manager=pm,
        power_controller=power,
        timer=timer,
        downstream=downstream,
        dispatcher=dispatcher,
        event_bus=event_bus,
        estimation_service=estimation_service,
        crash_skip_policy=CrashSkipPolicy(
            crash_count=crash_count,
            dispatch_skip_offset=dispatch_skip_offset,
        ),
        **kwargs,
    )
    # 绑定 mock 物件方便后续断言
    g._mock_repo = repo
    g._mock_reader = reader
    g._mock_writer = writer
    g._mock_pm = pm
    g._mock_power = power
    g._mock_timer = timer
    g._mock_timer_cancel = timer_cancel
    g._mock_downstream = downstream
    g._mock_dispatcher = dispatcher
    g._mock_event_bus = event_bus
    g._mock_estimation = estimation_service

    return g


def _make_job(prompt_id: str, status: JobStatus = JobStatus.PENDING) -> Job:
    """构造一个最小可用的任务实体，供队列状态机使用。"""
    return Job(
        number=1,
        prompt_id=prompt_id,
        prompt=RawJSON("{}"),
        extra_data=RawJSON("{}"),
        outputs_to_execute=[],
        create_time=0,
        status=status,
    )


class _QueueState:
    """把队列读写 mock 接到同一份任务状态上，使重入列这类状态迁移可被观察。

    静态 mock 无法表达"重入列后任务回到 PENDING"，因此这里用真实任务实体维护一份状态，
    只实现网关在派发与中断场景中真正使用到的读写操作。
    """

    def __init__(self, jobs: List[Job]) -> None:
        self._jobs = jobs
        self._statuses: List[JobStatus] = [job.status for job in jobs]

    def list(self, filter_by=None, limit=None, offset=None, desc=False) -> List[Job]:
        result = [
            replace(self._jobs[i], status=self._statuses[i])
            for i in range(len(self._jobs))
            if self._matches(i, filter_by)
        ]
        if desc:
            result.reverse()
        if offset:
            result = result[offset:]
        if limit is not None:
            result = result[:limit]
        return result

    def count(self, filter_by=None, limit=None) -> int:
        total = sum(
            1 for i in range(len(self._jobs)) if self._matches(i, filter_by)
        )
        return min(total, limit) if limit is not None else total

    def requeue_running_if_exists(self) -> bool:
        for i, status in enumerate(self._statuses):
            if status == JobStatus.RUNNING:
                self._statuses[i] = JobStatus.PENDING
                return True
        return False

    def update_status(
        self,
        new_status: JobStatus,
        prompt_id: Optional[str] = None,
        filter_status: Optional[JobStatus] = None,
    ) -> bool:
        changed = False
        for i, status in enumerate(self._statuses):
            if prompt_id is not None and self._jobs[i].prompt_id != prompt_id:
                continue
            if filter_status is not None and status != filter_status:
                continue
            self._statuses[i] = new_status
            changed = True
        return changed

    def status_of(self, prompt_id: str) -> Optional[JobStatus]:
        for i, job in enumerate(self._jobs):
            if job.prompt_id == prompt_id:
                return self._statuses[i]
        return None

    def statuses(self) -> List[JobStatus]:
        return list(self._statuses)

    def _matches(self, index: int, filter_by: Optional[JobFilters]) -> bool:
        if filter_by is None or filter_by.statuses is None:
            return True
        return self._statuses[index] in filter_by.statuses


def _bind_queue_state(g: Gateway, jobs: List[Job]) -> _QueueState:
    """让网关使用的队列读写 mock 共享同一份状态，便于断言任务是否被丢失。"""
    state = _QueueState(jobs)
    g._mock_reader.list.side_effect = state.list
    g._mock_reader.count.side_effect = state.count
    g._mock_writer.requeue_running_if_exists.side_effect = state.requeue_running_if_exists
    g._mock_writer.update_status.side_effect = state.update_status
    return state


class TestDomainGateway(unittest.TestCase):
    """测试 Gateway 聚合根核心业务逻辑与决策引擎。"""

    def test_initial_state(self):
        """测试初始状态。"""
        g = _make_gateway(paused=False)
        self.assertFalse(g.paused)
        self.assertFalse(g._downstream_executing)
        self.assertFalse(g._downstream_ready)
        self.assertEqual(g._crash_skip.crash_count, 0)
        self.assertEqual(g._crash_skip.dispatch_skip_offset, 0)

    def test_pause_decision(self):
        """测试暂停时的决策流。"""
        # 情况 1：暂停且当时不闲置，不应该触发立即重启
        g = _make_gateway(paused=False, downstream_executing=True)
        g.pause(restart_after_idle=True)
        self.assertTrue(g.paused)
        self.assertTrue(g._restart_after_idle_on_pause)
        g._mock_downstream.restart.assert_not_called()

        # 情况 2：暂停且当时已闲置，应触发立即重启
        g = _make_gateway(
            paused=False, downstream_executing=False, downstream_ready=True
        )
        g.pause(restart_after_idle=True)
        self.assertTrue(g.paused)
        self.assertFalse(g._restart_after_idle_on_pause)
        g._mock_downstream.restart.assert_called_once()

    def test_pause_persists_state(self):
        """测试暂停操作会自动持久化状态。"""
        g = _make_gateway(paused=False)
        g.pause(restart_after_idle=False)
        g._mock_repo.set_paused.assert_called_once_with(True)

    def test_resume_decision(self):
        """测试恢复的决策流。"""
        g = _make_gateway(paused=True, restart_after_idle_on_pause=True)
        g.resume()
        self.assertFalse(g.paused)
        self.assertFalse(g._restart_after_idle_on_pause)
        g._mock_dispatcher.dispatch.assert_called_once()

    def test_resume_persists_state(self):
        """测试恢复操作会自动持久化状态。"""
        g = _make_gateway(paused=True)
        g.resume()
        g._mock_repo.set_paused.assert_called_once_with(False)

    def test_set_downstream_executing_decision(self):
        """测试下游繁忙程度变化触发的决策。"""
        g = _make_gateway(paused=False, downstream_ready=True)

        # 下游开始执行
        g._mock_event_bus.publish(DownstreamExecutingChangedEvent(executing=True))
        self.assertTrue(g._downstream_executing)
        self.assertTrue(g._ever_active)
        g._mock_power.prevent_sleep.assert_called_once()

        # 下游执行完毕变为空闲，重置尝试计数并触发派发
        g2 = _make_gateway(
            paused=False, downstream_executing=True, crash_count=5, pending_count=1
        )
        g2._mock_event_bus.publish(DownstreamExecutingChangedEvent(executing=False))
        self.assertFalse(g2._downstream_executing)
        self.assertEqual(g2._crash_skip.crash_count, 0)
        self.assertTrue(
            any(isinstance(e, StatusChangedEvent) for e in g2._mock_event_bus.published)
        )
        g2._mock_dispatcher.dispatch.assert_called_once()

    def test_set_downstream_ready_decision(self):
        """测试下游就绪变化的派发决策。"""
        g = _make_gateway(paused=False)
        g._mock_event_bus.publish(DownstreamReadyChangedEvent(ready=True))
        self.assertTrue(g._downstream_ready)
        g._mock_dispatcher.dispatch.assert_called_once()

        g = _make_gateway(paused=True)
        g._mock_event_bus.publish(DownstreamReadyChangedEvent(ready=True))
        self.assertTrue(g._downstream_ready)
        g._mock_dispatcher.dispatch.assert_not_called()

    def test_dispatch_failed_decision(self):
        """测试任务派发失败的重试决策。"""
        g = _make_gateway(paused=False, pending_count=1)

        # 永久不可恢复错误：不重试
        g._mock_event_bus.publish(
            DispatchFailedEvent(prompt_id="job_test", is_permanent=True)
        )
        self.assertEqual(g._crash_skip.dispatch_skip_offset, 0)
        self.assertTrue(
            any(isinstance(e, StatusChangedEvent) for e in g._mock_event_bus.published)
        )

        # 临时错误：触发重试，累加偏移
        g._mock_event_bus.published.clear()
        g._mock_event_bus.publish(
            DispatchFailedEvent(prompt_id="job_test", is_permanent=False)
        )
        self.assertEqual(g._crash_skip.dispatch_skip_offset, 1)
        self.assertTrue(
            any(isinstance(e, StatusChangedEvent) for e in g._mock_event_bus.published)
        )

    def test_downstream_crashed_decision(self):
        """测试下游物理崩溃时的重入列与尝试计数逻辑。"""
        running_job = MagicMock(spec=Job)
        running_job.prompt_id = "test_job"
        g = _make_gateway(paused=False, running=[running_job])

        # 模拟物理重入列成功
        g._mock_writer.requeue_running_if_exists.return_value = True
        g._mock_reader.count.side_effect = lambda status=None, limit=None: 1
        g._mock_event_bus.publish(DownstreamCrashedEvent())

        self.assertEqual(g._crash_skip.crash_count, 1)
        g._mock_writer.requeue_running_if_exists.assert_called_once()
        self.assertTrue(
            any(isinstance(e, StatusChangedEvent) for e in g._mock_event_bus.published)
        )

        # 模拟没有正在运行的任务，重入列不应被调用
        g2 = _make_gateway(paused=False)
        g2._mock_event_bus.publish(DownstreamCrashedEvent())

        self.assertEqual(g2._crash_skip.crash_count, 0)
        g2._mock_writer.requeue_running_if_exists.assert_not_called()

    def test_idle_timeout_restart_and_cancel(self):
        """测试超时重启与定时器取消流程。"""
        # 测试用闭包辅助
        timer_callback = None
        cancelled = False

        def mock_start_timeout(seconds, callback):
            nonlocal timer_callback
            timer_callback = callback

            def cancel():
                nonlocal cancelled
                cancelled = True

            return cancel

        timer = MagicMock(spec=Timer)
        timer.now = 0.0
        timer.start_timeout.side_effect = mock_start_timeout

        g = _make_gateway(
            paused=False,
            ever_active=True,
            idle_restart_timeout=10,
            timer=timer,
            downstream_ready=True,
        )

        # 1. 一出生就是空闲状态，应该已经调用了定时器
        self.assertTrue(g._is_idle)
        self.assertIsNotNone(timer_callback)

        # 2. 模拟超时到达，手工执行回调
        timer_callback()
        g._mock_downstream.restart.assert_called_once()

        # 3. 测试离开空闲时能够正常取消定时器
        timer2 = MagicMock(spec=Timer)
        timer2.now = 0.0
        timer2.start_timeout.side_effect = mock_start_timeout
        g2 = _make_gateway(
            paused=False,
            ever_active=True,
            idle_restart_timeout=10,
            timer=timer2,
            downstream_ready=True,
        )

        # 离开空闲 (如因为有排队任务)
        g2._mock_reader.count.side_effect = lambda status=None, limit=None: 1
        g2._mock_event_bus.publish(QueueModifiedEvent())

        self.assertFalse(g2._is_idle)
        self.assertTrue(cancelled)  # 成功调用了 cancel 闭包

    def test_calculate_dispatch_skip(self):
        """测试计算派发 skip 偏移量。"""
        # 情况 1：网关暂停，不能分发
        g = _make_gateway(paused=True, downstream_ready=True, pending_count=10)
        self.assertIsNone(g.get_dispatch_skip())

        # 情况 2：下游繁忙，不能分发
        g = _make_gateway(
            paused=False,
            downstream_executing=True,
            downstream_ready=True,
            pending_count=10,
        )
        self.assertIsNone(g.get_dispatch_skip())

        # 情况 3：下游未就绪，不能分发
        g = _make_gateway(
            paused=False,
            downstream_executing=False,
            downstream_ready=False,
            pending_count=10,
        )
        self.assertIsNone(g.get_dispatch_skip())

        # 情况 4：队列为空，不能分发
        g = _make_gateway(
            paused=False,
            downstream_executing=False,
            downstream_ready=True,
            dispatch_skip_offset=3,
            pending_count=0,
        )
        self.assertIsNone(g.get_dispatch_skip())

        # 情况 5：正常派发，指定 pending_count=3 避免构造时重置 dispatch_skip_offset
        g = _make_gateway(
            paused=False,
            downstream_executing=False,
            downstream_ready=True,
            dispatch_skip_offset=5,
            pending_count=3,
        )
        skip = g.get_dispatch_skip()
        self.assertEqual(skip, 2)  # 5 % 3 = 2

    def test_determine_busy_state(self):
        """测试繁忙和空闲业务状态计算。"""
        # 1. 暂停状态下，即使有任务也不视为繁忙
        g = _make_gateway(paused=True)
        self.assertFalse(g._is_busy(has_jobs=True))

        # 2. 下游正在执行：视为繁忙
        g = _make_gateway(paused=False, downstream_executing=True)
        self.assertTrue(g._is_busy(has_jobs=False))

        # 3. 未暂停且有排队任务：视为繁忙
        g = _make_gateway(paused=False, downstream_executing=False)
        self.assertTrue(g._is_busy(has_jobs=True))

        # 4. 未暂停且无任务：闲置
        self.assertFalse(g._is_busy(has_jobs=False))

    def test_determine_sleep_prevention(self):
        """测试是否阻止系统休眠决策。"""
        g = _make_gateway(paused=False, downstream_ready=True)

        # 繁忙 且 无脚本在跑 -> 阻止休眠
        self.assertTrue(g._should_prevent_sleep(has_jobs=True, scripts_running=False))

        # 空闲 但 有脚本在跑 -> 阻止休眠
        self.assertTrue(g._should_prevent_sleep(has_jobs=False, scripts_running=True))

        # 空闲 且 无脚本在跑 -> 允许休眠
        self.assertFalse(g._should_prevent_sleep(has_jobs=False, scripts_running=False))

    def test_downstream_not_ready_prevents_sleep(self):
        """测试下游未就绪（启动/重启中）时，即使空闲也应阻止系统休眠，防止休眠打断下游启动。"""
        g = _make_gateway(paused=False, downstream_ready=False)

        # 空闲 且 无脚本在跑，但下游未就绪 -> 阻止休眠
        self.assertTrue(g._should_prevent_sleep(has_jobs=False, scripts_running=False))

        # 下游就绪后恢复允许休眠
        g._downstream_ready = True
        self.assertFalse(g._should_prevent_sleep(has_jobs=False, scripts_running=False))

    def test_downstream_ready_change_drives_sleep_prevention(self):
        """测试下游就绪状态变化驱动阻止/允许系统休眠（重启期间阻止休眠）。"""
        # 下游就绪且空闲：允许休眠
        g = _make_gateway(paused=False, downstream_ready=True)
        g._mock_power.allow_sleep.assert_called()

        # 下游进入重启（未就绪）：阻止休眠
        g._mock_event_bus.publish(DownstreamReadyChangedEvent(ready=False))
        g._mock_power.prevent_sleep.assert_called()

        # 重启完成（重新就绪）：恢复允许休眠
        g._mock_power.allow_sleep.reset_mock()
        g._mock_event_bus.publish(DownstreamReadyChangedEvent(ready=True))
        g._mock_power.allow_sleep.assert_called()

    def test_idle_timeout_restart_prevents_sleep_immediately(self):
        """测试空闲超时发起重启时立即阻止休眠，而非等待异步就绪事件，防止系统休眠打断重启。"""
        g = _make_gateway(
            paused=False,
            ever_active=True,
            idle_restart_timeout=10,
            downstream_ready=True,
        )
        g._mock_power.prevent_sleep.reset_mock()
        g._mock_power.allow_sleep.reset_mock()

        g._on_idle_timeout()

        self.assertFalse(g._downstream_ready)
        g._mock_downstream.restart.assert_called_once()
        g._mock_power.prevent_sleep.assert_called()

    def test_downstream_crashed_sets_not_ready_for_sleep_prevention(self):
        """测试下游崩溃时立即置为未就绪，避免崩溃检测到重启开始之间的空窗期允许休眠。"""
        g = _make_gateway(paused=False, downstream_ready=True)
        g._mock_power.prevent_sleep.reset_mock()

        g._mock_event_bus.publish(DownstreamCrashedEvent())

        self.assertFalse(g._downstream_ready)
        g._mock_power.prevent_sleep.assert_called()

    def test_script_state_changed_decision(self):
        """测试外挂辅助程序状态发生变化时的刷新决策。"""
        g = _make_gateway(paused=False)
        g._mock_pm.is_running.return_value = True

        g._mock_event_bus.published.clear()
        g._mock_event_bus.publish(ScriptStateChangedEvent())

        # 应该是阻止休眠状态被调用
        g._mock_power.prevent_sleep.assert_called()
        self.assertTrue(
            any(isinstance(e, StatusChangedEvent) for e in g._mock_event_bus.published)
        )

    def test_downstream_crashed_triggers_refresh(self):
        """测试下游物理崩溃时，不仅重入列还会触发 _refresh。"""
        g = _make_gateway(paused=False)
        g._mock_writer.requeue_running_if_exists.return_value = True

        # 初始化时调用过一次 _refresh（每次 _refresh 调用 count 一次）
        # 并且触发一次 _publish_status_changed 里的 count
        initial_count_pending = g._mock_reader.count.call_count
        initial_count_running = g._mock_pm.is_running.call_count
        self.assertEqual(initial_count_pending, 2)
        self.assertEqual(initial_count_running, 1)

        # 触发崩溃事件
        g._mock_event_bus.publish(DownstreamCrashedEvent())

        # 崩溃后 _refresh 被触发，并且触发了 _publish_status_changed
        self.assertEqual(g._mock_reader.count.call_count, 4)
        self.assertEqual(g._mock_pm.is_running.call_count, 2)

    def test_refresh_reentrancy_prevention(self):
        """测试 _refresh 能够有效防范事件回调触发的无限递归。"""
        g = _make_gateway(paused=False)

        # 模拟在 update_state 触发时再次执行 _refresh (比如触发了 ScriptStateChangedEvent)
        # 如果没有防重入机制，这里会造成无限递归导致栈溢出
        call_count = 0

        def mock_update_state(is_busy, ever_active):
            nonlocal call_count
            call_count += 1
            if call_count < 5:
                g._refresh()

        g._mock_pm.update_state.side_effect = mock_update_state
        g._refresh()

        # 验证因为引入了“脏标志+延迟刷新循环”模式，重入调用不会导致递归栈溢出，
        # 而是以循环迭代的形式在当前刷新结束后依次补执行，因此会执行完所有请求（共5次调用）。
        self.assertEqual(call_count, 5)

    def test_idle_timeout_state_validation(self):
        """测试在超时到达前已非空闲状态，不会触发下游重启。"""
        g = _make_gateway(paused=False, ever_active=True, idle_restart_timeout=10)

        # 退出空闲状态
        g._is_idle = False

        # 此时触发空闲超时
        g._on_idle_timeout()

        # 应该直接返回，不触发重启
        g._mock_downstream.restart.assert_not_called()

    def test_ever_active_reset_on_restarts_and_crashes(self):
        """测试在下游重启与崩溃时，_ever_active 会正确复位为 False（干净状态）。"""
        # 情况 1：从空闲超时重启
        g1 = _make_gateway(
            paused=False,
            ever_active=True,
            idle_restart_timeout=10,
            downstream_ready=True,
        )
        g1._on_idle_timeout()
        self.assertFalse(g1._ever_active)
        g1._mock_downstream.restart.assert_called_once()

        # 情况 2：从 pause 带有重启参数且已空闲的立即重启
        g2 = _make_gateway(paused=False, ever_active=True, downstream_ready=True)
        g2._is_idle = True
        g2.pause(restart_after_idle=True)
        self.assertFalse(g2._ever_active)
        g2._mock_downstream.restart.assert_called_once()

        # 情况 3：崩溃重启
        g3 = _make_gateway(paused=False, ever_active=True)
        g3._mock_event_bus.publish(DownstreamCrashedEvent())
        self.assertFalse(g3._ever_active)

    def test_crashed_executing_prevents_attempt_count_reset(self):
        """测试崩溃后下游重启导致的 executing=False 不会重置崩溃计数（任务尚未重新执行）。"""
        running_job = MagicMock(spec=Job)
        running_job.prompt_id = "test_job"
        # 必须传入 pending_count=1，否则 _refresh 时会因为队列无任务而清零 crash_count
        g = _make_gateway(
            paused=False,
            downstream_executing=True,
            crash_count=0,
            pending_count=1,
            running=[running_job],
        )

        # 1. 模拟崩溃：requeue 成功，增加 crash_count
        g._mock_writer.requeue_running_if_exists.return_value = True
        g._mock_event_bus.publish(DownstreamCrashedEvent())
        self.assertEqual(g._crash_skip.crash_count, 1)
        self.assertTrue(g._crash_skip.crashed_executing)

        # 2. 模拟下游重启后发布 executing = False（此时 _ever_active 为 False，任务尚未重新执行）
        g._mock_event_bus.publish(DownstreamExecutingChangedEvent(executing=False))

        # 验证此时 crash_count 依然保留为 1（因为 _ever_active 为 False，说明任务未完成），崩溃标志被清空
        self.assertEqual(g._crash_skip.crash_count, 1)
        self.assertFalse(g._crash_skip.crashed_executing)

    def test_dispatch_failed_retry_and_cancel(self):
        """测试临时派发失败会触发定时器延迟重试，且派发成功时可以取消该定时器。"""
        # 场景 A：派发失败后，在重试触发前收到成功事件，应取消定时器
        g = _make_gateway(paused=False, pending_count=2, downstream_ready=True)
        g._mock_event_bus.publish(
            DispatchFailedEvent(prompt_id="job_test", is_permanent=False)
        )
        self.assertEqual(g._crash_skip.dispatch_skip_offset, 1)
        g._mock_timer.start_timeout.assert_called_once_with(5.0, g._retry_dispatch)

        g._mock_timer_cancel.assert_not_called()
        g._mock_event_bus.publish(DispatchSuccessEvent(prompt_id="job_test"))
        g._mock_timer_cancel.assert_called_once()

        # 场景 B：重试回调触发时，应清除取消函数并以最新 skip 触发派发
        g2 = _make_gateway(paused=False, pending_count=2, downstream_ready=True)
        g2._mock_event_bus.publish(
            DispatchFailedEvent(prompt_id="job_test", is_permanent=False)
        )
        g2._mock_dispatcher.dispatch.assert_not_called()
        g2._retry_dispatch()
        g2._mock_dispatcher.dispatch.assert_called_once_with(1)

    def test_single_job_crash_loop_fallback(self):
        """测试单任务队列下，多次崩溃后触发降级策略，将坏任务标记为永久失败并移出队列。"""
        # 构造网关：有 1 个正在运行的任务，崩溃次数已达 2（即第3次崩溃触发降级）
        t = MagicMock(spec=Job)
        t.prompt_id = "bad_job_123"
        g = _make_gateway(paused=False, pending_count=0, running=[t], crash_count=2)

        # 触发物理崩溃事件
        g._mock_event_bus.publish(DownstreamCrashedEvent())

        # 确认：运行中任务被清除，崩溃计数被清零，且调用了 handle_failed_job 备份
        g._mock_writer.update_status.assert_called_once_with(
            JobStatus.FAILED, prompt_id="bad_job_123"
        )
        self.assertEqual(g._crash_skip.crash_count, 0)
        g._mock_dispatcher.handle_failed_job.assert_called_once_with(
            t, "Downstream crashed 3 times during execution."
        )
        # 确认 crashed_executing 未被设为 True
        self.assertFalse(g._crash_skip.crashed_executing)

    def test_permanent_fail_accumulates_across_readiness_transitions(self):
        """测试崩溃计数跨重启周期累积：每次崩溃都伴随一次未就绪迁移，第 3 次崩溃应永久失败。"""
        t = MagicMock(spec=Job)
        t.prompt_id = "bad_job"
        g = _make_gateway(paused=False, running=[t])
        g._mock_writer.requeue_running_if_exists.return_value = True

        # 模拟两次完整的崩溃重启周期：未就绪 → 崩溃 → 重新就绪
        for _ in range(2):
            g._mock_event_bus.publish(DownstreamReadyChangedEvent(ready=False))
            g._mock_event_bus.publish(DownstreamCrashedEvent())
            g._mock_event_bus.publish(DownstreamReadyChangedEvent(ready=True))

        # 前两次崩溃只应重入列，不应标记失败
        g._mock_writer.update_status.assert_not_called()

        # 第 3 次崩溃（同样先经历未就绪迁移）应触发永久失败
        g._mock_event_bus.publish(DownstreamReadyChangedEvent(ready=False))
        g._mock_event_bus.publish(DownstreamCrashedEvent())

        g._mock_writer.update_status.assert_called_once_with(
            JobStatus.FAILED, prompt_id="bad_job"
        )

    def test_attempt_count_reset_when_last_failed_deleted(self):
        """测试先前派发失败的坏任务被手动删除（不再处于队列中）后，崩溃计数、派发偏移及失败 ID 自动复位。"""
        # 1. 派发失败，记录坏任务 ID
        t = MagicMock(spec=Job)
        t.prompt_id = "job_999"
        g = _make_gateway(paused=False, pending=[t], running=[t])
        g._mock_event_bus.publish(
            DispatchFailedEvent(prompt_id="job_999", is_permanent=False)
        )
        self.assertEqual(g._crash_skip.dispatch_skip_offset, 1)
        self.assertEqual(g._crash_skip.last_failed_job_id, "job_999")

        # 2. 从 pending 和 running 中将该任务彻底移除（模拟手动删除），触发队列刷新
        g._mock_reader.list.side_effect = lambda filter_by=None: []
        g._mock_reader.count.side_effect = lambda filter_by=None, limit=None: 0
        g._mock_event_bus.publish(QueueModifiedEvent())

        # 确认崩溃计数、派发偏移与失败 ID 被清零
        self.assertEqual(g._crash_skip.crash_count, 0)
        self.assertEqual(g._crash_skip.dispatch_skip_offset, 0)
        self.assertIsNone(g._crash_skip.last_failed_job_id)

    def test_downstream_ready_changed_cancels_retry(self):
        """测试当下游变为未就绪（offline）时，自动取消正在挂起的延迟派发重试。"""
        g = _make_gateway(paused=False, pending_count=1)
        g._mock_event_bus.publish(
            DispatchFailedEvent(prompt_id="job_test", is_permanent=False)
        )
        g._mock_timer.start_timeout.assert_called_once()

        # 下游变为未就绪
        g._mock_timer_cancel.assert_not_called()
        g._mock_event_bus.publish(DownstreamReadyChangedEvent(ready=False))
        g._mock_timer_cancel.assert_called_once()

    def test_full_normal_dispatch_cycle(self):
        """测试完整的正常派发周期：got prompt → Prompt executed → 自动派发下一个任务。"""
        t = MagicMock(spec=Job)
        t.prompt_id = "running_job_id"
        g = _make_gateway(
            paused=False, downstream_ready=True, pending_count=2, running=[t]
        )

        # 第 1 步：下游开始执行（got prompt）
        g._mock_event_bus.publish(DownstreamExecutingChangedEvent(executing=True))
        self.assertTrue(g._downstream_executing)
        self.assertTrue(g._ever_active)

        # 第 2 步：下游执行完毕（Prompt executed in）
        g._mock_event_bus.publish(DownstreamExecutingChangedEvent(executing=False))
        self.assertFalse(g._downstream_executing)

        # 验证：执行完毕后应该自动触发派发下一个任务
        g._mock_dispatcher.dispatch.assert_called()
        g._mock_writer.update_status.assert_called_once_with(
            JobStatus.COMPLETED, filter_status=JobStatus.RUNNING
        )

        # 验证 get_dispatch_skip 返回了合法的 skip 值
        skip = g.get_dispatch_skip()
        self.assertIsNotNone(skip)
        self.assertEqual(skip, 0)

    def test_executing_false_guards_against_duplicate_events(self):
        """测试 _downstream_executing 已是 False 时再收到 executing=False 不会重复处理。"""
        t = MagicMock(spec=Job)
        t.prompt_id = "running_job_id"
        # 但也不应该跳过关键逻辑——如果下游从未收到过 executing=True，
        # 则 _downstream_executing 初始就是 False，此时收到 executing=False
        # 会因为 guard 而直接返回，导致 clear_running 和 dispatch 都不执行。
        # 这个测试确认 guard 的行为。
        g = _make_gateway(
            paused=False,
            downstream_ready=True,
            pending_count=2,
            downstream_executing=False,
            running=[t],
        )

        # 在 _downstream_executing 已是 False 时发布 executing=False
        g._mock_event_bus.publish(DownstreamExecutingChangedEvent(executing=False))

        # guard 生效：因为 _downstream_executing == executing，直接返回
        # dispatch 不应被调用
        g._mock_dispatcher.dispatch.assert_not_called()
        g._mock_writer.update_status.assert_not_called()

    def test_multiple_jobs_auto_dispatch_chain(self):
        """测试多任务自动链式派发：一个任务完成后自动派发下一个，再下一个。"""
        t1 = MagicMock(spec=Job)
        t1.prompt_id = "job_1"
        t2 = MagicMock(spec=Job)
        t2.prompt_id = "job_2"
        g = _make_gateway(
            paused=False, downstream_ready=True, pending_count=3, running=[t1]
        )

        # 预置 _downstream_executing = True 模拟下游正在执行第一个任务
        g._downstream_executing = True
        g._ever_active = True

        # 第一次 "Prompt executed in" → 应该触发 dispatch
        g._mock_dispatcher.dispatch.reset_mock()
        g._mock_writer.update_status.reset_mock()
        g._mock_event_bus.publish(DownstreamExecutingChangedEvent(executing=False))
        self.assertFalse(g._downstream_executing)
        g._mock_dispatcher.dispatch.assert_called_once()
        g._mock_writer.update_status.assert_called_once_with(
            JobStatus.COMPLETED, filter_status=JobStatus.RUNNING
        )

        # 模拟 dispatch 成功后下游再次开始执行（got prompt）
        g._mock_dispatcher.dispatch.reset_mock()
        g._mock_writer.update_status.reset_mock()
        g._mock_reader.list.side_effect = lambda statuses=None: [t2]
        g._mock_event_bus.publish(DownstreamExecutingChangedEvent(executing=True))
        self.assertTrue(g._downstream_executing)

        # 第二次 "Prompt executed in" → 应该再次触发 dispatch
        g._mock_event_bus.publish(DownstreamExecutingChangedEvent(executing=False))
        self.assertFalse(g._downstream_executing)
        g._mock_dispatcher.dispatch.assert_called_once()
        g._mock_writer.update_status.assert_called_once_with(
            JobStatus.COMPLETED, filter_status=JobStatus.RUNNING
        )

    def test_pause_resume_cancels_restart_after_idle(self):
        """测试 pause(restart_after_idle=True) 后立即 resume 会取消挂起的重启请求。"""
        # 下游正在执行时暂停，并指定空闲时重启
        g = _make_gateway(paused=False, downstream_executing=True)
        g.pause(restart_after_idle=True)

        self.assertTrue(g.paused)
        self.assertTrue(g._restart_after_idle_on_pause)
        g._mock_downstream.restart.assert_not_called()

        # 恢复队列
        g.resume()
        self.assertFalse(g.paused)
        self.assertFalse(g._restart_after_idle_on_pause)

        # 模拟下游执行完成变为空闲，不应该触发重启
        g._mock_event_bus.publish(DownstreamExecutingChangedEvent(executing=False))
        g._mock_downstream.restart.assert_not_called()

    def test_pause_resume_cancels_idle_timeout_restart(self):
        """测试 pause 后产生空闲超时定时器，resume 会取消该定时器防止非预期重启。"""
        timer_callback = None
        cancelled = False

        def mock_start_timeout(seconds, callback):
            nonlocal timer_callback
            timer_callback = callback

            def cancel():
                nonlocal cancelled
                cancelled = True

            return cancel

        timer = MagicMock(spec=Timer)
        timer.now = 0.0
        timer.start_timeout.side_effect = mock_start_timeout

        # 1. 处于暂停状态，且 ever_active，会启动空闲超时定时器
        g = _make_gateway(
            paused=True,
            ever_active=True,
            idle_restart_timeout=10,
            timer=timer,
            downstream_ready=True,
        )
        self.assertTrue(g._is_idle)
        self.assertIsNotNone(timer_callback)
        self.assertFalse(cancelled)

        # 2. 恢复队列，应该取消该空闲超时定时器
        g.resume()
        self.assertTrue(cancelled)

    def test_paused_reports_paused_queue_state(self):
        """暂停且队列仍有任务时，应上报 PAUSED 队列状态而非 IDLE/BUSY。"""
        g = _make_gateway(
            paused=True, pending_count=2, downstream_ready=True, ever_active=True
        )
        g._mock_pm.update_state.assert_called_with(QueueState.PAUSED, True)

    def test_executing_reports_busy_queue_state_even_when_paused(self):
        """暂停瞬间下游仍在执行任务时，应上报 BUSY 直到任务完成。"""
        g = _make_gateway(
            paused=True,
            downstream_executing=True,
            downstream_ready=True,
            ever_active=True,
        )
        g._mock_pm.update_state.assert_called_with(QueueState.BUSY, True)

    def test_drained_queue_reports_idle_queue_state(self):
        """未暂停、队列排空且下游执行过任务时，应上报 IDLE 队列状态。"""
        g = _make_gateway(
            paused=False, downstream_ready=True, ever_active=True, pending_count=0
        )
        g._mock_pm.update_state.assert_called_with(QueueState.IDLE, True)

    def test_script_running_keeps_gateway_idle(self):
        """外挂程序运行不应改变空闲判定：暂停且无任务执行时仍视为空闲。"""
        g = _make_gateway(paused=True, downstream_ready=True, ever_active=True)
        self.assertTrue(g._is_idle)

        # 外挂程序启动后触发刷新，空闲状态不应被外挂程序破坏
        g._mock_pm.is_running.return_value = True
        g._mock_event_bus.publish(ScriptStateChangedEvent())
        self.assertTrue(g._is_idle)

    def test_restart_after_idle_fires_while_external_program_running(self):
        """暂停等待重启期间外部程序在运行：当前任务完成后仍必须触发下游重启（#8）。"""
        # 下游正在执行时请求暂停并等待空闲重启
        g = _make_gateway(
            paused=False,
            downstream_executing=True,
            downstream_ready=True,
            ever_active=True,
        )
        g.pause(restart_after_idle=True)
        self.assertTrue(g._restart_after_idle_on_pause)
        g._mock_downstream.restart.assert_not_called()

        # 任务完成，外挂程序随之启动并保持运行（旧逻辑会因此卡死重启）
        g._mock_pm.is_running.return_value = True
        g._mock_event_bus.publish(DownstreamExecutingChangedEvent(executing=False))

        self.assertFalse(g._restart_after_idle_on_pause)
        g._mock_downstream.restart.assert_called_once()

    def test_idle_window_restart_deferred_by_latest_activity(self):
        """空闲窗口内的活动应把重启推迟到"最后一次活动 + idle_restart_timeout"（#11）。"""
        timer = FakeTimer()
        g = _make_gateway(
            paused=False,
            ever_active=True,
            idle_restart_timeout=600,
            downstream_ready=True,
            timer=timer,
        )
        self.assertTrue(g._is_idle)

        # 第一次活动：原定的 600s 重启时刻应推迟到 590 + 600
        timer.advance(590)
        g.report_activity()
        timer.advance(590)
        g._mock_downstream.restart.assert_not_called()

        # 第二次活动：窗口起点继续推迟到最后一次活动
        g.report_activity()
        timer.advance(590)
        g._mock_downstream.restart.assert_not_called()

        # 最后一次活动满 600s 后才允许重启
        timer.advance(10)
        g._mock_downstream.restart.assert_called_once()

    def test_activity_without_pending_restart_never_restarts(self):
        """没有挂起的重启决策时，活动不得凭空触发下游重启（#11）。"""
        timer = FakeTimer()
        g = _make_gateway(paused=False, downstream_ready=True, timer=timer)
        self.assertEqual(timer.pending_count, 0)

        g.report_activity()

        self.assertEqual(timer.pending_count, 0)
        timer.advance(3600)
        g._mock_downstream.restart.assert_not_called()

    def test_activity_defers_restart_after_idle_on_pause(self):
        """暂停后的空闲立即重启同样受空闲窗口约束（#11）。"""
        timer = FakeTimer()
        g = _make_gateway(
            paused=False,
            ever_active=True,
            idle_restart_timeout=600,
            downstream_ready=True,
            timer=timer,
        )

        timer.advance(590)
        g.report_activity()
        g.pause(restart_after_idle=True)

        # 空闲窗口内仍有活动：重启被推迟而不是立即执行
        self.assertTrue(g.paused)
        self.assertTrue(g._restart_after_idle_on_pause)
        g._mock_downstream.restart.assert_not_called()

        timer.advance(599)
        g._mock_downstream.restart.assert_not_called()

        # 窗口结束后才重启，且不会在下次进入空闲时重复重启
        timer.advance(1)
        g._mock_downstream.restart.assert_called_once()
        self.assertFalse(g._restart_after_idle_on_pause)

        g._mock_event_bus.publish(DownstreamReadyChangedEvent(ready=True))
        timer.advance(3600)
        g._mock_downstream.restart.assert_called_once()

    def test_activity_during_execution_defers_restart_after_idle_on_pause(self):
        """下游执行期间的活动同样推迟暂停后的空闲重启（#11）。"""
        timer = FakeTimer()
        g = _make_gateway(
            paused=False,
            downstream_executing=True,
            ever_active=True,
            idle_restart_timeout=600,
            downstream_ready=True,
            timer=timer,
        )
        g.pause(restart_after_idle=True)
        g.report_activity()

        # 任务执行完毕进入空闲，但窗口内仍有活动，重启被推迟
        g._mock_event_bus.publish(DownstreamExecutingChangedEvent(executing=False))
        self.assertTrue(g._is_idle)
        self.assertTrue(g._restart_after_idle_on_pause)
        g._mock_downstream.restart.assert_not_called()

        timer.advance(600)
        g._mock_downstream.restart.assert_called_once()

    def test_activity_keeps_ever_active_and_sleep_prevention(self):
        """活动不改变 _ever_active，也不影响是否阻止系统休眠（#11）。"""
        timer = FakeTimer()
        g = _make_gateway(
            paused=False,
            ever_active=True,
            idle_restart_timeout=600,
            downstream_ready=True,
            timer=timer,
        )

        g.report_activity()

        self.assertTrue(g._ever_active)
        self.assertTrue(g._is_idle)
        self.assertFalse(g._should_prevent_sleep(has_jobs=False, scripts_running=False))

    def test_activity_outside_window_does_not_delay_pause_restart(self):
        """活动已超出空闲窗口时，暂停后的空闲重启必须立即执行（#11）。"""
        timer = FakeTimer()
        g = _make_gateway(
            paused=False,
            ever_active=True,
            idle_restart_timeout=600,
            downstream_ready=True,
            timer=timer,
        )

        # 活动发生在窗口内，随后 resume 放弃了该窗口的评估
        timer.advance(100)
        g.report_activity()
        timer.advance(100)
        g.resume()
        self.assertEqual(timer.pending_count, 0)

        # 此时活动早已超出空闲窗口，不应再次推迟重启
        timer.advance(600)
        g.pause(restart_after_idle=True)

        self.assertFalse(g._restart_after_idle_on_pause)
        g._mock_downstream.restart.assert_called_once()

    def test_activity_during_execution_still_restarts_after_idle(self):
        """执行期间的活动不改变"进入空闲后再等待一个完整窗口"的重启时刻（#11）。"""
        timer = FakeTimer()
        g = _make_gateway(
            paused=False,
            downstream_executing=True,
            ever_active=True,
            idle_restart_timeout=600,
            downstream_ready=True,
            timer=timer,
        )
        g.report_activity()

        timer.advance(100)
        g._mock_event_bus.publish(DownstreamExecutingChangedEvent(executing=False))

        timer.advance(599)
        g._mock_downstream.restart.assert_not_called()
        timer.advance(1)
        g._mock_downstream.restart.assert_called_once()

    def test_activity_after_resume_does_not_schedule_restart(self):
        """resume 取消了空闲重启评估后，活动不得凭空重新安排重启（#11）。"""
        timer = FakeTimer()
        g = _make_gateway(
            paused=True,
            ever_active=True,
            idle_restart_timeout=600,
            downstream_ready=True,
            timer=timer,
        )
        g.resume()
        self.assertEqual(timer.pending_count, 0)

        g.report_activity()

        self.assertEqual(timer.pending_count, 0)
        timer.advance(3600)
        g._mock_downstream.restart.assert_not_called()

    def test_forced_restart_requeues_running_job(self):
        """强制重启属于中断：执行中的任务回到 PENDING，不得被当作完成而丢失（#12）。"""
        running_job = _make_job("running_1", JobStatus.RUNNING)
        pending_job = _make_job("pending_1")
        g = _make_gateway(
            paused=True,
            downstream_executing=True,
            downstream_ready=True,
            ever_active=True,
            running=[running_job],
            pending=[pending_job],
            restart_after_idle_on_pause=True,
        )
        queue = _bind_queue_state(g, [running_job, pending_job])

        g.restart_now()

        self.assertEqual(queue.status_of("running_1"), JobStatus.PENDING)
        self.assertEqual(
            g._mock_reader.count(JobFilters([JobStatus.PENDING, JobStatus.RUNNING])),
            2,
        )

        # 重启流程随后发布的"下游停止执行"事件不得被当作完成处理
        g._mock_event_bus.publish(DownstreamExecutingChangedEvent(executing=False))

        g._mock_writer.update_status.assert_not_called()
        self.assertEqual(queue.status_of("running_1"), JobStatus.PENDING)
        self.assertNotIn(JobStatus.COMPLETED, queue.statuses())
        self.assertNotIn(JobStatus.FAILED, queue.statuses())

    def test_forced_restart_never_touches_crash_count_or_estimation(self):
        """反复强制重启不得把任务推向永久失败，也不得写入预估样本（#12）。"""
        running_job = _make_job("running_1", JobStatus.RUNNING)
        g = _make_gateway(
            paused=True,
            downstream_executing=True,
            downstream_ready=True,
            ever_active=True,
            crash_count=1,
            running=[running_job],
        )
        queue = _bind_queue_state(g, [running_job])

        for _ in range(3):
            g.restart_now()
            # 重启流程发布的"下游停止执行"事件
            g._mock_event_bus.publish(DownstreamExecutingChangedEvent(executing=False))
            # 重新就绪并再次执行同一个任务
            g._mock_event_bus.publish(DownstreamReadyChangedEvent(ready=True))
            g._mock_event_bus.publish(DownstreamExecutingChangedEvent(executing=True))

            self.assertEqual(queue.status_of("running_1"), JobStatus.PENDING)
            self.assertEqual(g._crash_skip.crash_count, 1)

        self.assertNotIn(JobStatus.FAILED, queue.statuses())
        g._mock_estimation.record_completion.assert_not_called()

    def test_forced_restart_keeps_queue_paused_without_dispatch(self):
        """强制重启期间队列保持暂停且不派发任务，重启完成后由用户手动恢复（#12）。"""
        running_job = _make_job("running_1", JobStatus.RUNNING)
        g = _make_gateway(
            paused=True,
            downstream_executing=True,
            downstream_ready=True,
            ever_active=True,
            running=[running_job],
        )
        _bind_queue_state(g, [running_job])
        g._mock_dispatcher.reset_mock()

        g.restart_now()

        self.assertTrue(g.paused)
        self.assertFalse(g._downstream_ready)
        g._mock_dispatcher.dispatch.assert_not_called()
        g._mock_downstream.restart.assert_called_once()

    def test_forced_restart_consumes_pending_restart_after_idle(self):
        """强制重启应消费挂起的「暂停后空闲重启」决策，避免重启完成后再次排程重启（#12）。"""
        timer = FakeTimer()
        running_job = _make_job("running_1", JobStatus.RUNNING)
        g = _make_gateway(
            paused=True,
            downstream_executing=True,
            downstream_ready=True,
            ever_active=True,
            idle_restart_timeout=600,
            timer=timer,
            running=[running_job],
        )
        g.pause(restart_after_idle=True)
        _bind_queue_state(g, [running_job])

        g.restart_now()

        # 重启完成重新就绪，并进入空闲
        g._mock_event_bus.publish(DownstreamReadyChangedEvent(ready=True))
        timer.advance(3600)

        self.assertFalse(g._restart_after_idle_on_pause)
        g._mock_downstream.restart.assert_called_once()

    def test_forced_restart_never_allows_sleep_during_restart(self):
        """强制重启全程必须阻止系统休眠：就绪标志翻转与刷新都不得放行休眠（#12）。"""
        running_job = _make_job("running_1", JobStatus.RUNNING)
        g = _make_gateway(
            paused=True,
            downstream_executing=True,
            downstream_ready=True,
            ever_active=True,
            running=[running_job],
        )
        _bind_queue_state(g, [running_job])
        g._mock_power.reset_mock()

        g.restart_now()

        g._mock_power.allow_sleep.assert_not_called()
        g._mock_power.prevent_sleep.assert_called()


if __name__ == "__main__":
    unittest.main()
