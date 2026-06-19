import unittest
from unittest.mock import MagicMock
from gateway.domain.gateway import Gateway
from gateway.shared.interfaces import (
    StateRepository,
    TaskQueueReader,
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
    repo = MagicMock(spec=StateRepository)
    repo.get_paused.return_value = paused

    reader = MagicMock(spec=TaskQueueReader)
    reader.get_pending_count.return_value = 0

    pm = MagicMock(spec=ProcessManager)
    pm.is_running.return_value = False

    power = MagicMock(spec=SystemPowerController)

    timer = MagicMock(spec=Timer)
    timer_cancel = MagicMock()
    timer.start_timeout.return_value = timer_cancel

    downstream = MagicMock(spec=DownstreamClient)
    dispatcher = MagicMock(spec=TaskDispatcher)
    event_bus = MockEventBus()

    g = Gateway(
        state_repo=repo,
        queue_reader=reader,
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
        self.assertFalse(g.downstream_executing)
        self.assertFalse(g.downstream_ready)
        self.assertEqual(g.attempt_count, 0)

    def test_pause_decision(self):
        """测试暂停时的决策流。"""
        # 情况 1：暂停且当时不闲置，不应该触发立即重启
        g = _make_gateway(paused=False, downstream_executing=True)
        g.pause(restart_after_idle=True)
        self.assertTrue(g.paused)
        self.assertTrue(g.restart_after_idle_on_pause)
        g._mock_downstream.restart.assert_not_called()

        # 情况 2：暂停且当时已闲置，应触发立即重启
        g = _make_gateway(paused=False, downstream_executing=False)
        g.pause(restart_after_idle=True)
        self.assertTrue(g.paused)
        self.assertFalse(g.restart_after_idle_on_pause)
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
        self.assertFalse(g.restart_after_idle_on_pause)
        g._mock_dispatcher.try_dispatch.assert_called_once()

    def test_resume_persists_state(self):
        """测试恢复操作会自动持久化状态。"""
        g = _make_gateway(paused=True)
        g.resume()
        g._mock_repo.set_paused.assert_called_once_with(False)

    def test_set_downstream_executing_decision(self):
        """测试下游繁忙程度变化触发的决策。"""
        g = _make_gateway(paused=False)

        # 下游开始执行
        g.set_downstream_executing(True)
        self.assertTrue(g.downstream_executing)
        self.assertTrue(g.ever_active)
        g._mock_power.prevent_sleep.assert_called_once()

        # 下游执行完毕变为空闲，重置尝试计数并触发派发
        g.attempt_count = 5
        g.set_downstream_executing(False)
        self.assertFalse(g.downstream_executing)
        self.assertEqual(g.attempt_count, 0)
        self.assertTrue(any(isinstance(e, StatusChangedEvent) for e in g._mock_event_bus.published))
        g._mock_dispatcher.try_dispatch.assert_called_once()

    def test_set_downstream_ready_decision(self):
        """测试下游就绪变化的派发决策。"""
        g = _make_gateway(paused=False)
        g.set_downstream_ready(True)
        self.assertTrue(g.downstream_ready)
        g._mock_dispatcher.try_dispatch.assert_called_once()

        g = _make_gateway(paused=True)
        g.set_downstream_ready(True)
        self.assertTrue(g.downstream_ready)
        g._mock_dispatcher.try_dispatch.assert_not_called()

    def test_on_dispatch_failed_decision(self):
        """测试任务派发失败的重试决策。"""
        g = _make_gateway(paused=False)

        # 永久不可恢复错误：不重试
        should_requeue = g.on_dispatch_failed(is_permanent=True)
        self.assertFalse(should_requeue)
        self.assertEqual(g.attempt_count, 0)
        self.assertTrue(any(isinstance(e, StatusChangedEvent) for e in g._mock_event_bus.published))

        # 临时错误：触发重试，累加计数
        g._mock_event_bus.published.clear()
        should_requeue = g.on_dispatch_failed(is_permanent=False)
        self.assertTrue(should_requeue)
        self.assertEqual(g.attempt_count, 1)
        self.assertTrue(any(isinstance(e, StatusChangedEvent) for e in g._mock_event_bus.published))

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

        g = _make_gateway(paused=False, ever_active=True, idle_restart_timeout=10)
        g._mock_timer.start_timeout.side_effect = mock_start_timeout

        # 1. 模拟进入空闲
        g._mock_reader.get_pending_count.return_value = 0
        g._mock_pm.is_running.return_value = False
        g.refresh()

        self.assertTrue(g._is_idle)
        self.assertIsNotNone(timer_callback)

        # 2. 模拟超时到达，手工执行回调
        timer_callback()
        g._mock_downstream.restart.assert_called_once()

        # 3. 测试离开空闲时能够正常取消定时器
        g = _make_gateway(paused=False, ever_active=True, idle_restart_timeout=10)
        g._mock_timer.start_timeout.side_effect = mock_start_timeout
        g._mock_reader.get_pending_count.return_value = 0
        g._mock_pm.is_running.return_value = False
        g.refresh()

        # 离开空闲 (如因为有排队任务)
        g._mock_reader.get_pending_count.return_value = 1
        g.refresh()

        self.assertFalse(g._is_idle)
        self.assertTrue(cancelled)  # 成功调用了 cancel 闭包

    def test_calculate_dispatch_skip(self):
        """测试计算派发 skip 偏移量。"""
        # 情况 1：网关暂停，不能分发
        g = _make_gateway(paused=True, downstream_ready=True)
        self.assertIsNone(g.get_dispatch_skip(pending_count=10))

        # 情况 2：下游繁忙，不能分发
        g = _make_gateway(paused=False, downstream_executing=True, downstream_ready=True)
        self.assertIsNone(g.get_dispatch_skip(pending_count=10))

        # 情况 3：下游未就绪，不能分发
        g = _make_gateway(paused=False, downstream_executing=False, downstream_ready=False)
        self.assertIsNone(g.get_dispatch_skip(pending_count=10))

        # 情况 4：队列为空，不能分发并重置尝试计数
        g = _make_gateway(paused=False, downstream_executing=False, downstream_ready=True, attempt_count=3)
        self.assertIsNone(g.get_dispatch_skip(pending_count=0))
        self.assertEqual(g.attempt_count, 0)

        # 情况 5：正常派发
        g = _make_gateway(paused=False, downstream_executing=False, downstream_ready=True, attempt_count=5)
        skip = g.get_dispatch_skip(pending_count=3)
        self.assertEqual(skip, 2)  # 5 % 3 = 2

    def test_determine_busy_state(self):
        """测试繁忙和空闲业务状态计算。"""
        # 1. 暂停状态下，即使有任务也不视为繁忙
        g = _make_gateway(paused=True)
        self.assertFalse(g.is_busy(has_pending=True))

        # 2. 下游正在执行：视为繁忙
        g = _make_gateway(paused=False, downstream_executing=True)
        self.assertTrue(g.is_busy(has_pending=False))

        # 3. 未暂停且有排队任务：视为繁忙
        g = _make_gateway(paused=False, downstream_executing=False)
        self.assertTrue(g.is_busy(has_pending=True))

        # 4. 未暂停且无任务：闲置
        self.assertFalse(g.is_busy(has_pending=False))

    def test_determine_sleep_prevention(self):
        """测试是否阻止系统休眠决策。"""
        g = _make_gateway(paused=False)

        # 繁忙 且 无脚本在跑 -> 阻止休眠
        self.assertTrue(g.should_prevent_sleep(has_pending=True, scripts_running=False))

        # 空闲 但 有脚本在跑 -> 阻止休眠
        self.assertTrue(g.should_prevent_sleep(has_pending=False, scripts_running=True))

        # 空闲 且 无脚本在跑 -> 允许休眠
        self.assertFalse(g.should_prevent_sleep(has_pending=False, scripts_running=False))


if __name__ == "__main__":
    unittest.main()
