import unittest
from typing import Dict

from gateway.domain.downstream_resource_decision import DownstreamResourceDecision
from gateway.shared.models import DownstreamAction


NOW = 1000.0
WINDOW = 600.0
THRESHOLD_MB = 1024

# 窗口到期入口全组合真值表，键依次为 I(空闲) A(授权) Q(队列空) R(生命周期执行过任务)
# E(显式指令) S(连续低采样达标)，期望值直接来自 issue #27 的判定规则。
_EXPIRY_TABLE = """
I0A0Q0R0E0S0 NONE
I0A0Q0R0E0S1 NONE
I0A0Q0R0E1S0 NONE
I0A0Q0R0E1S1 NONE
I0A0Q0R1E0S0 NONE
I0A0Q0R1E0S1 NONE
I0A0Q0R1E1S0 NONE
I0A0Q0R1E1S1 NONE
I0A0Q1R0E0S0 NONE
I0A0Q1R0E0S1 NONE
I0A0Q1R0E1S0 NONE
I0A0Q1R0E1S1 NONE
I0A0Q1R1E0S0 NONE
I0A0Q1R1E0S1 NONE
I0A0Q1R1E1S0 NONE
I0A0Q1R1E1S1 NONE
I0A1Q0R0E0S0 NONE
I0A1Q0R0E0S1 NONE
I0A1Q0R0E1S0 NONE
I0A1Q0R0E1S1 NONE
I0A1Q0R1E0S0 NONE
I0A1Q0R1E0S1 NONE
I0A1Q0R1E1S0 NONE
I0A1Q0R1E1S1 NONE
I0A1Q1R0E0S0 NONE
I0A1Q1R0E0S1 NONE
I0A1Q1R0E1S0 NONE
I0A1Q1R0E1S1 NONE
I0A1Q1R1E0S0 NONE
I0A1Q1R1E0S1 NONE
I0A1Q1R1E1S0 NONE
I0A1Q1R1E1S1 NONE
I1A0Q0R0E0S0 NONE
I1A0Q0R0E0S1 NONE
I1A0Q0R0E1S0 NONE
I1A0Q0R0E1S1 NONE
I1A0Q0R1E0S0 RESTART
I1A0Q0R1E0S1 RESTART
I1A0Q0R1E1S0 RESTART
I1A0Q0R1E1S1 RESTART
I1A0Q1R0E0S0 NONE
I1A0Q1R0E0S1 NONE
I1A0Q1R0E1S0 NONE
I1A0Q1R0E1S1 NONE
I1A0Q1R1E0S0 RESTART
I1A0Q1R1E0S1 RESTART
I1A0Q1R1E1S0 RESTART
I1A0Q1R1E1S1 RESTART
I1A1Q0R0E0S0 NONE
I1A1Q0R0E0S1 NONE
I1A1Q0R0E1S0 NONE
I1A1Q0R0E1S1 NONE
I1A1Q0R1E0S0 RESTART
I1A1Q0R1E0S1 RESTART
I1A1Q0R1E1S0 RESTART
I1A1Q0R1E1S1 RESTART
I1A1Q1R0E0S0 NONE
I1A1Q1R0E0S1 SHUTDOWN
I1A1Q1R0E1S0 NONE
I1A1Q1R0E1S1 SHUTDOWN
I1A1Q1R1E0S0 RESTART
I1A1Q1R1E0S1 SHUTDOWN
I1A1Q1R1E1S0 RESTART
I1A1Q1R1E1S1 RESTART
"""

# 采样补做入口全组合真值表，键依次为 D(窗口已到期) E(显式指令) A(授权)
# I(空闲) Q(队列空) S(连续低采样达标)。allow_restart 与本入口无关，固定为真。
_SAMPLING_TABLE = """
D0E0A0I0Q0S0 NONE
D0E0A0I0Q0S1 NONE
D0E0A0I0Q1S0 NONE
D0E0A0I0Q1S1 NONE
D0E0A0I1Q0S0 NONE
D0E0A0I1Q0S1 NONE
D0E0A0I1Q1S0 NONE
D0E0A0I1Q1S1 NONE
D0E0A1I0Q0S0 NONE
D0E0A1I0Q0S1 NONE
D0E0A1I0Q1S0 NONE
D0E0A1I0Q1S1 NONE
D0E0A1I1Q0S0 NONE
D0E0A1I1Q0S1 NONE
D0E0A1I1Q1S0 NONE
D0E0A1I1Q1S1 NONE
D0E1A0I0Q0S0 NONE
D0E1A0I0Q0S1 NONE
D0E1A0I0Q1S0 NONE
D0E1A0I0Q1S1 NONE
D0E1A0I1Q0S0 NONE
D0E1A0I1Q0S1 NONE
D0E1A0I1Q1S0 NONE
D0E1A0I1Q1S1 NONE
D0E1A1I0Q0S0 NONE
D0E1A1I0Q0S1 NONE
D0E1A1I0Q1S0 NONE
D0E1A1I0Q1S1 NONE
D0E1A1I1Q0S0 NONE
D0E1A1I1Q0S1 NONE
D0E1A1I1Q1S0 NONE
D0E1A1I1Q1S1 NONE
D1E0A0I0Q0S0 NONE
D1E0A0I0Q0S1 NONE
D1E0A0I0Q1S0 NONE
D1E0A0I0Q1S1 NONE
D1E0A0I1Q0S0 NONE
D1E0A0I1Q0S1 NONE
D1E0A0I1Q1S0 NONE
D1E0A0I1Q1S1 NONE
D1E0A1I0Q0S0 NONE
D1E0A1I0Q0S1 NONE
D1E0A1I0Q1S0 NONE
D1E0A1I0Q1S1 NONE
D1E0A1I1Q0S0 NONE
D1E0A1I1Q0S1 NONE
D1E0A1I1Q1S0 NONE
D1E0A1I1Q1S1 SHUTDOWN
D1E1A0I0Q0S0 NONE
D1E1A0I0Q0S1 NONE
D1E1A0I0Q1S0 NONE
D1E1A0I0Q1S1 NONE
D1E1A0I1Q0S0 NONE
D1E1A0I1Q0S1 NONE
D1E1A0I1Q1S0 NONE
D1E1A0I1Q1S1 NONE
D1E1A1I0Q0S0 NONE
D1E1A1I0Q0S1 NONE
D1E1A1I0Q1S0 NONE
D1E1A1I0Q1S1 NONE
D1E1A1I1Q0S0 NONE
D1E1A1I1Q0S1 NONE
D1E1A1I1Q1S0 NONE
D1E1A1I1Q1S1 NONE
"""


def _table(text: str) -> Dict[str, DownstreamAction]:
    """把真值表文本解析成 {组合键: 期望动作}，期望值来自规格而非实现。"""
    return {
        parts[0]: DownstreamAction[parts[1]]
        for line in text.strip().splitlines()
        for parts in [line.split()]
    }


def _decision(
    *,
    now: float = NOW,
    timeout: float = WINDOW,
    restart_after_idle: bool = False,
    low_samples: int = 0,
) -> DownstreamResourceDecision:
    """构造一个空闲窗口已到期的决策对象。"""
    decision = DownstreamResourceDecision(
        idle_restart_timeout=timeout,
        shutdown_memory_threshold_mb=THRESHOLD_MB,
        now=now,
        restart_after_idle=restart_after_idle,
        low_memory_samples=low_samples,
    )
    decision.defer_window(now - timeout - 1)
    return decision


class TestWindowExpiryDecision(unittest.TestCase):
    """窗口到期入口：穷举空闲/授权/队列/生命周期/显式指令/采样各组合。"""

    def test_evaluate_decision_table(self) -> None:
        for key, expected in _table(_EXPIRY_TABLE).items():
            with self.subTest(key=key):
                idle, allow_shutdown, queue_empty, allow_restart, explicit, samples_ok = (
                    flag == "1" for flag in key[1::2]
                )
                decision = _decision(
                    restart_after_idle=explicit, low_samples=3 if samples_ok else 0
                )
                self.assertEqual(
                    decision.evaluate(
                        NOW, idle, queue_empty, allow_shutdown, allow_restart
                    ),
                    expected,
                )

    def test_disabled_window_only_decides_when_qualification_exists(self) -> None:
        # 无窗口、无重启资格时窗口必要性判据不成立，显式指令不得在此入口被消费
        decision = _decision(timeout=0, restart_after_idle=True)
        self.assertEqual(
            decision.evaluate(NOW, True, True, False, False), DownstreamAction.NONE
        )
        self.assertTrue(decision.restart_after_idle)

        # 无窗口且无任务时，授权的停机补做也不成立
        decision = _decision(timeout=0, low_samples=3)
        self.assertEqual(
            decision.evaluate(NOW, True, True, True, False), DownstreamAction.NONE
        )

    def test_explicit_instruction_is_consumed_once(self) -> None:
        decision = _decision(restart_after_idle=True)
        self.assertEqual(
            decision.evaluate(NOW, True, True, False, True), DownstreamAction.RESTART
        )
        self.assertFalse(decision.restart_after_idle)
        self.assertEqual(
            decision.evaluate(NOW, True, True, False, True), DownstreamAction.RESTART
        )


class TestAfterSamplingDecision(unittest.TestCase):
    """采样后补做入口：穷举窗口到期/显式指令/授权/空闲/队列/采样各组合。"""

    def test_evaluate_after_sampling_decision_table(self) -> None:
        for key, expected in _table(_SAMPLING_TABLE).items():
            with self.subTest(key=key):
                expired, explicit, allow_shutdown, idle, queue_empty, samples_ok = (
                    flag == "1" for flag in key[1::2]
                )
                decision = _decision(
                    restart_after_idle=explicit, low_samples=3 if samples_ok else 0
                )
                if not expired:
                    decision.defer_window(NOW - 10)
                self.assertEqual(
                    decision.evaluate_after_sampling(
                        NOW, idle, queue_empty, allow_shutdown, True
                    ),
                    expected,
                )

    def test_disabled_window_never_follows_up(self) -> None:
        decision = _decision(timeout=0, low_samples=3)
        self.assertEqual(
            decision.evaluate_after_sampling(NOW, True, True, True, True),
            DownstreamAction.NONE,
        )

    def test_follow_up_is_independent_of_allow_restart(self) -> None:
        with_restart = _decision(low_samples=3)
        without_restart = _decision(low_samples=3)
        self.assertEqual(
            with_restart.evaluate_after_sampling(NOW, True, True, True, True),
            without_restart.evaluate_after_sampling(NOW, True, True, True, False),
        )

    def test_explicit_instruction_survives_after_sampling_suppression(self) -> None:
        decision = _decision(restart_after_idle=True, low_samples=3)
        decision.evaluate_after_sampling(NOW, True, True, True, True)
        self.assertTrue(decision.restart_after_idle)


class TestDecisionState(unittest.TestCase):
    """seam 自有的窗口时间、采样计数与显式指令状态。"""

    def test_remaining_uses_deferred_window(self) -> None:
        decision = _decision(now=NOW)
        decision.defer_window(NOW - 100)
        self.assertEqual(decision.remaining(NOW), WINDOW - 100)
        decision.defer_window(NOW - WINDOW - 1)
        self.assertEqual(decision.remaining(NOW), 0.0)
        self.assertEqual(_decision(timeout=0).remaining(NOW), 0.0)

    def test_record_sample_counts_consecutive_low_samples(self) -> None:
        decision = _decision()
        decision.record_sample(THRESHOLD_MB - 1)
        decision.record_sample(THRESHOLD_MB - 1)
        self.assertEqual(decision.low_memory_samples, 2)
        decision.record_sample(THRESHOLD_MB)
        self.assertEqual(decision.low_memory_samples, 0)

    def test_sample_threshold_is_three(self) -> None:
        self.assertEqual(
            _decision(low_samples=2).evaluate_after_sampling(
                NOW, True, True, True, False
            ),
            DownstreamAction.NONE,
        )
        self.assertEqual(
            _decision(low_samples=3).evaluate_after_sampling(
                NOW, True, True, True, False
            ),
            DownstreamAction.SHUTDOWN,
        )

    def test_window_needed_truth_table(self) -> None:
        self.assertTrue(_decision().window_needed(False, True))
        self.assertTrue(_decision(restart_after_idle=True).window_needed(True, True))
        self.assertTrue(_decision(restart_after_idle=True).window_needed(True, False))
        self.assertFalse(_decision(restart_after_idle=True).window_needed(False, False))
        self.assertFalse(_decision(timeout=0).window_needed(True, True))

    def test_explicit_instruction_set_and_clear(self) -> None:
        decision = _decision()
        decision.set_restart_after_idle(True)
        self.assertTrue(decision.restart_after_idle)
        decision.clear_restart_after_idle()
        self.assertFalse(decision.restart_after_idle)

    def test_clear_samples(self) -> None:
        decision = _decision(low_samples=3)
        decision.clear_samples()
        self.assertEqual(decision.low_memory_samples, 0)


if __name__ == "__main__":
    unittest.main()
