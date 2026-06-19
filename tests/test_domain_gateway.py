import unittest
from gateway.domain.gateway import Gateway

class TestDomainGateway(unittest.TestCase):
    """测试 Gateway 聚合根核心业务逻辑与决策引擎。"""

    def test_initial_state(self):
        """测试初始状态。"""
        g = Gateway(paused=False)
        self.assertFalse(g.paused)
        self.assertFalse(g.downstream_executing)
        self.assertFalse(g.downstream_ready)
        self.assertEqual(g.attempt_count, 0)

    def test_pause_decision(self):
        """测试暂停时的决策流。"""
        # 情况 1：暂停且当时不闲置，不应该触发立即重启
        g = Gateway(paused=False, downstream_executing=True)
        decision = g.pause(restart_after_idle=True, is_currently_idle=False)
        self.assertTrue(g.paused)
        self.assertTrue(g.restart_after_idle_on_pause)
        self.assertIsNone(decision)

        # 情况 2：暂停且当时已闲置，应返回立即重启
        g = Gateway(paused=False, downstream_executing=False)
        decision = g.pause(restart_after_idle=True, is_currently_idle=True)
        self.assertTrue(g.paused)
        self.assertFalse(g.restart_after_idle_on_pause)
        self.assertEqual(decision, "RESTART_IMMEDIATELY")

    def test_resume_decision(self):
        """测试恢复的决策流。"""
        g = Gateway(paused=True, restart_after_idle_on_pause=True)
        should_dispatch = g.resume()
        self.assertFalse(g.paused)
        self.assertFalse(g.restart_after_idle_on_pause)
        self.assertTrue(should_dispatch)

    def test_set_downstream_executing_decision(self):
        """测试下游繁忙程度变化触发的决策。"""
        g = Gateway(paused=False)
        
        # 下游开始执行
        decision = g.set_downstream_executing(True)
        self.assertTrue(g.downstream_executing)
        self.assertTrue(g.ever_active)
        self.assertEqual(decision, "ENTER_BUSY")

        # 下游执行完毕变为空闲，重置尝试计数并清零运行队列
        g.attempt_count = 5
        decision = g.set_downstream_executing(False)
        self.assertFalse(g.downstream_executing)
        self.assertEqual(g.attempt_count, 0)
        self.assertEqual(decision, "CLEAR_RUNNING_AND_DISPATCH")

    def test_set_downstream_ready_decision(self):
        """测试下游就绪变化的派发决策。"""
        g = Gateway(paused=False)
        should_dispatch = g.set_downstream_ready(True)
        self.assertTrue(g.downstream_ready)
        self.assertTrue(should_dispatch)

        g = Gateway(paused=True)
        should_dispatch = g.set_downstream_ready(True)
        self.assertTrue(g.downstream_ready)
        self.assertFalse(should_dispatch)

    def test_on_dispatch_failed_decision(self):
        """测试任务派发失败的重试决策。"""
        g = Gateway(paused=False)

        # 永久不可恢复错误：不重试
        should_requeue = g.on_dispatch_failed(is_permanent=True)
        self.assertFalse(should_requeue)
        self.assertEqual(g.attempt_count, 0)

        # 临时错误：触发重试，累加计数
        should_requeue = g.on_dispatch_failed(is_permanent=False)
        self.assertTrue(should_requeue)
        self.assertEqual(g.attempt_count, 1)

    def test_calculate_dispatch_skip(self):
        """测试计算派发 skip 偏移量。"""
        # 情况 1：网关暂停，不能分发
        g = Gateway(paused=True, downstream_ready=True)
        self.assertIsNone(g.calculate_dispatch_skip(pending_count=10))

        # 情况 2：下游繁忙，不能分发
        g = Gateway(paused=False, downstream_executing=True, downstream_ready=True)
        self.assertIsNone(g.calculate_dispatch_skip(pending_count=10))

        # 情况 3：下游未就绪，不能分发
        g = Gateway(paused=False, downstream_executing=False, downstream_ready=False)
        self.assertIsNone(g.calculate_dispatch_skip(pending_count=10))

        # 情况 4：队列为空，不能分发并重置尝试计数
        g = Gateway(paused=False, downstream_executing=False, downstream_ready=True, attempt_count=3)
        self.assertIsNone(g.calculate_dispatch_skip(pending_count=0))
        self.assertEqual(g.attempt_count, 0)

        # 情况 5：正常派发
        g = Gateway(paused=False, downstream_executing=False, downstream_ready=True, attempt_count=5)
        skip = g.calculate_dispatch_skip(pending_count=3)
        self.assertEqual(skip, 2) # 5 % 3 = 2

    def test_determine_busy_state(self):
        """测试繁忙和空闲业务状态计算。"""
        # 1. 暂停状态下，即使有任务也不视为繁忙
        g = Gateway(paused=True)
        self.assertFalse(g.determine_busy_state(has_pending=True))

        # 2. 下游正在执行：视为繁忙
        g = Gateway(paused=False, downstream_executing=True)
        self.assertTrue(g.determine_busy_state(has_pending=False))

        # 3. 未暂停且有排队任务：视为繁忙
        g = Gateway(paused=False, downstream_executing=False)
        self.assertTrue(g.determine_busy_state(has_pending=True))

        # 4. 未暂停且无任务：闲置
        self.assertFalse(g.determine_busy_state(has_pending=False))

    def test_determine_sleep_prevention(self):
        """测试是否阻止系统休眠决策。"""
        g = Gateway(paused=False)

        # 繁忙 且 无脚本在跑 -> 阻止休眠
        self.assertTrue(g.determine_sleep_prevention(has_pending=True, scripts_running=False))

        # 空闲 但 有脚本在跑 -> 阻止休眠
        self.assertTrue(g.determine_sleep_prevention(has_pending=False, scripts_running=True))

        # 空闲 且 无脚本在跑 -> 允许休眠
        self.assertFalse(g.determine_sleep_prevention(has_pending=False, scripts_running=False))

if __name__ == "__main__":
    unittest.main()
