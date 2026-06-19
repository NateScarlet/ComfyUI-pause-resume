import unittest
from unittest.mock import MagicMock
from gateway.domain.gateway import Gateway
from gateway.shared.models import Task
from gateway.shared.interfaces import (
    StateRepository,
    TaskQueueReader,
    TaskQueueWriter,
    ProcessManager,
    SystemPowerController,
    Timer,
    DownstreamClient,
    TaskDispatcher,
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


def _make_gateway(**kwargs) -> Gateway:  # type: ignore[no-untyped-def]
    """用 mock 的各个接口依赖快速构造测试用 Gateway。"""
    paused = kwargs.pop("paused", False)
    pending_count = kwargs.pop("pending_count", 0)
    pending_tasks = kwargs.pop("pending_tasks", None)
    running_tasks = kwargs.pop("running_tasks", None)

    if pending_tasks is None:
        pending_tasks = []
        for i in range(pending_count):
            t = MagicMock(spec=Task)
            t.prompt_id = f"pending_id_{i}"
            pending_tasks.append(t)
    else:
        pending_count = len(pending_tasks)

    if running_tasks is None:
        running_tasks = []

    repo = MagicMock(spec=StateRepository)
    repo.get_paused.return_value = paused

    reader = MagicMock(spec=TaskQueueReader)
    reader.get_pending_count.return_value = pending_count
    reader.get_pending.return_value = pending_tasks
    reader.get_running.return_value = running_tasks

    writer = MagicMock(spec=TaskQueueWriter)
    writer.requeue_running_if_exists.return_value = False

    pm = MagicMock(spec=ProcessManager)
    pm.is_running.return_value = False

    power = MagicMock(spec=SystemPowerController)

    timer = kwargs.pop("timer", None)
    timer_cancel = MagicMock()
    if timer is None:
        timer = MagicMock(spec=Timer)
        timer.start_timeout.return_value = timer_cancel

    downstream = MagicMock(spec=DownstreamClient)
    dispatcher = MagicMock(spec=TaskDispatcher)
    event_bus = MockEventBus()

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
        **kwargs
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

    return g


class TestDomainGateway(unittest.TestCase):
    """测试 Gateway 聚合根核心业务逻辑与决策引擎。"""

    def test_initial_state(self):
        """测试初始状态。"""
        g = _make_gateway(paused=False)
        self.assertFalse(g.paused)
        self.assertFalse(g._downstream_executing)
        self.assertFalse(g._downstream_ready)
        self.assertEqual(g._crash_count, 0)
        self.assertEqual(g._dispatch_skip_offset, 0)

    def test_pause_decision(self):
        """测试暂停时的决策流。"""
        # 情况 1：暂停且当时不闲置，不应该触发立即重启
        g = _make_gateway(paused=False, downstream_executing=True)
        g.pause(restart_after_idle=True)
        self.assertTrue(g.paused)
        self.assertTrue(g._restart_after_idle_on_pause)
        g._mock_downstream.restart.assert_not_called()

        # 情况 2：暂停且当时已闲置，应触发立即重启
        g = _make_gateway(paused=False, downstream_executing=False)
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
        g = _make_gateway(paused=False)

        # 下游开始执行
        g._mock_event_bus.publish(DownstreamExecutingChangedEvent(executing=True))
        self.assertTrue(g._downstream_executing)
        self.assertTrue(g._ever_active)
        g._mock_power.prevent_sleep.assert_called_once()

        # 下游执行完毕变为空闲，重置尝试计数并触发派发
        g2 = _make_gateway(paused=False, downstream_executing=True, crash_count=5, pending_count=1)
        g2._mock_event_bus.publish(DownstreamExecutingChangedEvent(executing=False))
        self.assertFalse(g2._downstream_executing)
        self.assertEqual(g2._crash_count, 0)
        self.assertTrue(any(isinstance(e, StatusChangedEvent) for e in g2._mock_event_bus.published))
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
        g._mock_event_bus.publish(DispatchFailedEvent(task_id="task_test", is_permanent=True))
        self.assertEqual(g._dispatch_skip_offset, 0)
        self.assertTrue(any(isinstance(e, StatusChangedEvent) for e in g._mock_event_bus.published))

        # 临时错误：触发重试，累加偏移
        g._mock_event_bus.published.clear()
        g._mock_event_bus.publish(DispatchFailedEvent(task_id="task_test", is_permanent=False))
        self.assertEqual(g._dispatch_skip_offset, 1)
        self.assertTrue(any(isinstance(e, StatusChangedEvent) for e in g._mock_event_bus.published))

    def test_downstream_crashed_decision(self):
        """测试下游物理崩溃时的重入列与尝试计数逻辑。"""
        g = _make_gateway(paused=False)

        # 模拟物理重入列成功
        g._mock_writer.requeue_running_if_exists.return_value = True
        g._mock_reader.get_pending_count.return_value = 1
        g._mock_event_bus.publish(DownstreamCrashedEvent())

        self.assertEqual(g._crash_count, 1)
        g._mock_writer.requeue_running_if_exists.assert_called_once()
        self.assertTrue(any(isinstance(e, StatusChangedEvent) for e in g._mock_event_bus.published))

        # 模拟没有正在运行的任务，重入列未发生
        g2 = _make_gateway(paused=False)
        g2._mock_writer.requeue_running_if_exists.return_value = False
        g2._mock_event_bus.publish(DownstreamCrashedEvent())

        self.assertEqual(g2._crash_count, 0)
        g2._mock_writer.requeue_running_if_exists.assert_called_once()

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
        timer.start_timeout.side_effect = mock_start_timeout

        g = _make_gateway(paused=False, ever_active=True, idle_restart_timeout=10, timer=timer)

        # 1. 一出生就是空闲状态，应该已经调用了定时器
        self.assertTrue(g._is_idle)
        self.assertIsNotNone(timer_callback)

        # 2. 模拟超时到达，手工执行回调
        timer_callback()
        g._mock_downstream.restart.assert_called_once()

        # 3. 测试离开空闲时能够正常取消定时器
        timer2 = MagicMock(spec=Timer)
        timer2.start_timeout.side_effect = mock_start_timeout
        g2 = _make_gateway(paused=False, ever_active=True, idle_restart_timeout=10, timer=timer2)

        # 离开空闲 (如因为有排队任务)
        g2._mock_reader.get_pending_count.return_value = 1
        g2._mock_event_bus.publish(QueueModifiedEvent())

        self.assertFalse(g2._is_idle)
        self.assertTrue(cancelled)  # 成功调用了 cancel 闭包

    def test_calculate_dispatch_skip(self):
        """测试计算派发 skip 偏移量。"""
        # 情况 1：网关暂停，不能分发
        g = _make_gateway(paused=True, downstream_ready=True, pending_count=10)
        self.assertIsNone(g.get_dispatch_skip())

        # 情况 2：下游繁忙，不能分发
        g = _make_gateway(paused=False, downstream_executing=True, downstream_ready=True, pending_count=10)
        self.assertIsNone(g.get_dispatch_skip())

        # 情况 3：下游未就绪，不能分发
        g = _make_gateway(paused=False, downstream_executing=False, downstream_ready=False, pending_count=10)
        self.assertIsNone(g.get_dispatch_skip())

        # 情况 4：队列为空，不能分发
        g = _make_gateway(paused=False, downstream_executing=False, downstream_ready=True, dispatch_skip_offset=3, pending_count=0)
        self.assertIsNone(g.get_dispatch_skip())

        # 情况 5：正常派发，指定 pending_count=3 避免构造时重置 dispatch_skip_offset
        g = _make_gateway(paused=False, downstream_executing=False, downstream_ready=True, dispatch_skip_offset=5, pending_count=3)
        skip = g.get_dispatch_skip()
        self.assertEqual(skip, 2)  # 5 % 3 = 2

    def test_determine_busy_state(self):
        """测试繁忙和空闲业务状态计算。"""
        # 1. 暂停状态下，即使有任务也不视为繁忙
        g = _make_gateway(paused=True)
        self.assertFalse(g._is_busy(has_pending=True))

        # 2. 下游正在执行：视为繁忙
        g = _make_gateway(paused=False, downstream_executing=True)
        self.assertTrue(g._is_busy(has_pending=False))

        # 3. 未暂停且有排队任务：视为繁忙
        g = _make_gateway(paused=False, downstream_executing=False)
        self.assertTrue(g._is_busy(has_pending=True))

        # 4. 未暂停且无任务：闲置
        self.assertFalse(g._is_busy(has_pending=False))

    def test_determine_sleep_prevention(self):
        """测试是否阻止系统休眠决策。"""
        g = _make_gateway(paused=False)

        # 繁忙 且 无脚本在跑 -> 阻止休眠
        self.assertTrue(g._should_prevent_sleep(has_pending=True, scripts_running=False))

        # 空闲 但 有脚本在跑 -> 阻止休眠
        self.assertTrue(g._should_prevent_sleep(has_pending=False, scripts_running=True))

        # 空闲 且 无脚本在跑 -> 允许休眠
        self.assertFalse(g._should_prevent_sleep(has_pending=False, scripts_running=False))

    def test_script_state_changed_decision(self):
        """测试外挂辅助程序状态发生变化时的刷新决策。"""
        g = _make_gateway(paused=False)
        g._mock_pm.is_running.return_value = True

        g._mock_event_bus.published.clear()
        g._mock_event_bus.publish(ScriptStateChangedEvent())

        # 应该是阻止休眠状态被调用
        g._mock_power.prevent_sleep.assert_called()
        self.assertTrue(any(isinstance(e, StatusChangedEvent) for e in g._mock_event_bus.published))

    def test_downstream_crashed_triggers_refresh(self):
        """测试下游物理崩溃时，不仅重入列还会触发 _refresh。"""
        g = _make_gateway(paused=False)
        g._mock_writer.requeue_running_if_exists.return_value = True

        # 初始化时调用过一次 _refresh
        initial_count_pending = g._mock_reader.get_pending_count.call_count
        initial_count_running = g._mock_pm.is_running.call_count
        self.assertEqual(initial_count_pending, 1)
        self.assertEqual(initial_count_running, 1)

        # 触发崩溃事件
        g._mock_event_bus.publish(DownstreamCrashedEvent())

        # 崩溃后 _refresh 被触发，各计数应该加 1
        self.assertEqual(g._mock_reader.get_pending_count.call_count, 2)
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
        g1 = _make_gateway(paused=False, ever_active=True, idle_restart_timeout=10)
        g1._on_idle_timeout()
        self.assertFalse(g1._ever_active)
        g1._mock_downstream.restart.assert_called_once()

        # 情况 2：从 pause 带有重启参数且已空闲的立即重启
        g2 = _make_gateway(paused=False, ever_active=True)
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
        # 必须传入 pending_count=1，否则 _refresh 时会因为队列无任务而清零 crash_count
        g = _make_gateway(paused=False, downstream_executing=True, crash_count=0, pending_count=1)
        
        # 1. 模拟崩溃：requeue 成功，增加 crash_count
        g._mock_writer.requeue_running_if_exists.return_value = True
        g._mock_event_bus.publish(DownstreamCrashedEvent())
        self.assertEqual(g._crash_count, 1)
        self.assertTrue(g._crashed_executing)
        
        # 2. 模拟下游重启后发布 executing = False（此时 _ever_active 为 False，任务尚未重新执行）
        g._mock_event_bus.publish(DownstreamExecutingChangedEvent(executing=False))
        
        # 验证此时 crash_count 依然保留为 1（因为 _ever_active 为 False，说明任务未完成），崩溃标志被清空
        self.assertEqual(g._crash_count, 1)
        self.assertFalse(g._crashed_executing)

    def test_dispatch_failed_retry_and_cancel(self):
        """测试临时派发失败会触发定时器延迟重试，且派发成功时可以取消该定时器。"""
        # 场景 A：派发失败后，在重试触发前收到成功事件，应取消定时器
        g = _make_gateway(paused=False, pending_count=2, downstream_ready=True)
        g._mock_event_bus.publish(DispatchFailedEvent(task_id="task_test", is_permanent=False))
        self.assertEqual(g._dispatch_skip_offset, 1)
        g._mock_timer.start_timeout.assert_called_once_with(5.0, g._retry_dispatch)

        g._mock_timer_cancel.assert_not_called()
        g._mock_event_bus.publish(DispatchSuccessEvent())
        g._mock_timer_cancel.assert_called_once()

        # 场景 B：重试回调触发时，应清除取消函数并以最新 skip 触发派发
        g2 = _make_gateway(paused=False, pending_count=2, downstream_ready=True)
        g2._mock_event_bus.publish(DispatchFailedEvent(task_id="task_test", is_permanent=False))
        g2._mock_dispatcher.dispatch.assert_not_called()
        g2._retry_dispatch()
        g2._mock_dispatcher.dispatch.assert_called_once_with(1)

    def test_single_task_crash_loop_fallback(self):
        """测试单任务队列下，多次崩溃后触发降级策略，将坏任务标记为永久失败并移出队列。"""
        # 构造网关：有 1 个正在运行的任务，0 个排队任务
        t = MagicMock(spec=Task)
        t.prompt_id = "bad_task_123"
        g = _make_gateway(paused=False, pending_count=0, running_tasks=[t])
        
        # 在构造后，将崩溃次数设为 2（即第3次执行前崩溃）
        g._crash_count = 2
        
        # 触发物理崩溃事件
        g._mock_event_bus.publish(DownstreamCrashedEvent())
        
        # 确认：运行中任务被清除，崩溃计数被清零，且调用了 handle_failed_task 备份
        g._mock_writer.clear_running.assert_called_once()
        self.assertEqual(g._crash_count, 0)
        g._mock_dispatcher.handle_failed_task.assert_called_once_with(t, "Downstream crashed 3 times during execution.")
        # 确认 crashed_executing 未被设为 True
        self.assertFalse(g._crashed_executing)

    def test_attempt_count_reset_when_last_failed_deleted(self):
        """测试先前派发失败的坏任务被手动删除（不再处于队列中）后，崩溃计数、派发偏移及失败 ID 自动复位。"""
        # 1. 派发失败，记录坏任务 ID
        t = MagicMock(spec=Task)
        t.prompt_id = "task_999"
        g = _make_gateway(paused=False, pending_tasks=[t], running_tasks=[t])
        g._mock_event_bus.publish(DispatchFailedEvent(task_id="task_999", is_permanent=False))
        self.assertEqual(g._dispatch_skip_offset, 1)
        self.assertEqual(g._last_failed_task_id, "task_999")
        
        # 2. 从 pending 和 running 中将该任务彻底移除（模拟手动删除），触发队列刷新
        g._mock_reader.get_pending.return_value = []
        g._mock_reader.get_running.return_value = []
        g._mock_reader.get_pending_count.return_value = 0
        g._mock_event_bus.publish(QueueModifiedEvent())
        
        # 确认崩溃计数、派发偏移与失败 ID 被清零
        self.assertEqual(g._crash_count, 0)
        self.assertEqual(g._dispatch_skip_offset, 0)
        self.assertIsNone(g._last_failed_task_id)

    def test_downstream_ready_changed_cancels_retry(self):
        """测试当下游变为未就绪（offline）时，自动取消正在挂起的延迟派发重试。"""
        g = _make_gateway(paused=False, pending_count=1)
        g._mock_event_bus.publish(DispatchFailedEvent(task_id="task_test", is_permanent=False))
        g._mock_timer.start_timeout.assert_called_once()
        
        # 下游变为未就绪
        g._mock_timer_cancel.assert_not_called()
        g._mock_event_bus.publish(DownstreamReadyChangedEvent(ready=False))
        g._mock_timer_cancel.assert_called_once()

    def test_full_normal_dispatch_cycle(self):
        """测试完整的正常派发周期：got prompt → Prompt executed → 自动派发下一个任务。"""
        g = _make_gateway(paused=False, downstream_ready=True, pending_count=2)

        # 第 1 步：下游开始执行（got prompt）
        g._mock_event_bus.publish(DownstreamExecutingChangedEvent(executing=True))
        self.assertTrue(g._downstream_executing)
        self.assertTrue(g._ever_active)

        # 第 2 步：下游执行完毕（Prompt executed in）
        g._mock_event_bus.publish(DownstreamExecutingChangedEvent(executing=False))
        self.assertFalse(g._downstream_executing)

        # 验证：执行完毕后应该自动触发派发下一个任务
        g._mock_dispatcher.dispatch.assert_called()
        g._mock_writer.clear_running.assert_called_once()

        # 验证 get_dispatch_skip 返回了合法的 skip 值
        skip = g.get_dispatch_skip()
        self.assertIsNotNone(skip)
        self.assertEqual(skip, 0)

    def test_executing_false_guards_against_duplicate_events(self):
        """测试 _downstream_executing 已是 False 时再收到 executing=False 不会重复处理。"""
        # 但也不应该跳过关键逻辑——如果下游从未收到过 executing=True，
        # 则 _downstream_executing 初始就是 False，此时收到 executing=False
        # 会因为 guard 而直接返回，导致 clear_running 和 dispatch 都不执行。
        # 这个测试确认 guard 的行为。
        g = _make_gateway(paused=False, downstream_ready=True, pending_count=2,
                          downstream_executing=False)

        # 在 _downstream_executing 已是 False 时发布 executing=False
        g._mock_event_bus.publish(DownstreamExecutingChangedEvent(executing=False))

        # guard 生效：因为 _downstream_executing == executing，直接返回
        # dispatch 不应被调用
        g._mock_dispatcher.dispatch.assert_not_called()
        g._mock_writer.clear_running.assert_not_called()

    def test_multiple_tasks_auto_dispatch_chain(self):
        """测试多任务自动链式派发：一个任务完成后自动派发下一个，再下一个。"""
        g = _make_gateway(paused=False, downstream_ready=True, pending_count=3)

        # 预置 _downstream_executing = True 模拟下游正在执行第一个任务
        g._downstream_executing = True
        g._ever_active = True

        # 第一次 "Prompt executed in" → 应该触发 dispatch
        g._mock_dispatcher.dispatch.reset_mock()
        g._mock_event_bus.publish(DownstreamExecutingChangedEvent(executing=False))
        self.assertFalse(g._downstream_executing)
        g._mock_dispatcher.dispatch.assert_called_once()
        g._mock_writer.clear_running.assert_called_once()

        # 模拟 dispatch 成功后下游再次开始执行（got prompt）
        g._mock_dispatcher.dispatch.reset_mock()
        g._mock_writer.clear_running.reset_mock()
        g._mock_event_bus.publish(DownstreamExecutingChangedEvent(executing=True))
        self.assertTrue(g._downstream_executing)

        # 第二次 "Prompt executed in" → 应该再次触发 dispatch
        g._mock_event_bus.publish(DownstreamExecutingChangedEvent(executing=False))
        self.assertFalse(g._downstream_executing)
        g._mock_dispatcher.dispatch.assert_called_once()
        g._mock_writer.clear_running.assert_called_once()


if __name__ == "__main__":
    unittest.main()
