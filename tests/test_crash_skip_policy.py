import unittest

from gateway.domain.crash_skip_policy import CrashSkipPolicy
from gateway.shared.models import CompletionAction, CompletionOutcome, CrashOutcome


class TestCrashSkipPolicyOutcomes(unittest.TestCase):
    """策略 seam：直接断言类型化结果，覆盖完成与崩溃的全部语义分支。"""

    def test_outcomes_are_typed_not_bare_strings(self):
        """策略结果必须是显式类型，不能再被当作任意字符串比较。"""
        policy = CrashSkipPolicy()

        completion = policy.record_completion(allow_restart=True)
        crash = policy.record_crash("job_1")

        self.assertNotIsInstance(completion, str)
        self.assertNotIsInstance(crash, str)

    def test_normal_completion_marks_completed_with_sample(self):
        """正常完成：标记完成并记录样本，崩溃计数清零。"""
        policy = CrashSkipPolicy()

        outcome = policy.record_completion(allow_restart=True)

        self.assertEqual(
            outcome,
            CompletionOutcome(
                action=CompletionAction.MARK_COMPLETED, record_sample=True
            ),
        )
        self.assertEqual(policy.crash_count, 0)

    def test_requeue_completion_marks_completed_without_sample(self):
        """崩溃后 requeue 的任务完成：同样标记完成，但该次执行不记录样本。"""
        policy = CrashSkipPolicy()
        policy.increment_crash()

        outcome = policy.record_completion(allow_restart=True)

        self.assertEqual(
            outcome,
            CompletionOutcome(
                action=CompletionAction.MARK_COMPLETED, record_sample=False
            ),
        )
        self.assertEqual(policy.crash_count, 0)
        self.assertFalse(policy.crashed_executing)

    def test_interruption_completion_is_noop_and_keeps_crash_count(self):
        """中断后下游尚未真正执行任务：noop，不清零崩溃计数、不记录样本。"""
        policy = CrashSkipPolicy()
        policy.increment_crash()

        outcome = policy.record_completion(allow_restart=False)

        self.assertEqual(
            outcome,
            CompletionOutcome(action=CompletionAction.NOOP, record_sample=False),
        )
        self.assertEqual(policy.crash_count, 1)
        self.assertFalse(policy.crashed_executing)

    def test_crash_below_threshold_requeues_and_counts(self):
        """崩溃未超阈值：返回重新入列并递增计数的动作。"""
        policy = CrashSkipPolicy()

        outcome = policy.record_crash("job_1")

        self.assertIs(outcome, CrashOutcome.REQUEUE_AND_COUNT_CRASH)
        self.assertEqual(policy.last_failed_job_id, "job_1")

    def test_crash_at_threshold_marks_permanent_failed(self):
        """崩溃达到阈值：返回标记永久失败的动作，并复位策略状态。"""
        policy = CrashSkipPolicy(crash_count=2, last_failed_job_id="job_1")

        outcome = policy.record_crash("job_1")

        self.assertIs(outcome, CrashOutcome.MARK_PERMANENT_FAILED)
        self.assertEqual(policy.crash_count, 0)
        self.assertIsNone(policy.last_failed_job_id)

    def test_crash_from_a_new_failed_job_resets_previous_state(self):
        """换了一个失败任务时，先复位先前任务的崩溃计数再判定动作。"""
        policy = CrashSkipPolicy(crash_count=1, last_failed_job_id="old_job")

        outcome = policy.record_crash("new_job")

        self.assertIs(outcome, CrashOutcome.REQUEUE_AND_COUNT_CRASH)
        self.assertEqual(policy.crash_count, 0)
        self.assertEqual(policy.last_failed_job_id, "new_job")
