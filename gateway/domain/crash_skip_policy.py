from typing import Optional, Set


class CrashSkipPolicy:
    """崩溃跳过策略：管理任务崩溃计数、派发跳过偏移和永久失败判断。

    这是一个纯状态机，不依赖任何外部接口或副作用。所有决策基于输入参数返回结果。
    """

    _MAX_CRASH_COUNT = 2

    def __init__(
        self,
        crash_count: int = 0,
        dispatch_skip_offset: int = 0,
        last_failed_job_id: Optional[str] = None,
        crashed_executing: bool = False,
    ):
        self._crash_count = crash_count
        self._dispatch_skip_offset = dispatch_skip_offset
        self._last_failed_job_id = last_failed_job_id
        self._crashed_executing = crashed_executing

    @property
    def crash_count(self) -> int:
        return self._crash_count

    @property
    def dispatch_skip_offset(self) -> int:
        return self._dispatch_skip_offset

    @property
    def last_failed_job_id(self) -> Optional[str]:
        return self._last_failed_job_id

    @property
    def crashed_executing(self) -> bool:
        return self._crashed_executing

    def reset(self) -> None:
        """全部计数器归零，清除失败任务记录与崩溃执行状态。"""
        self._crash_count = 0
        self._dispatch_skip_offset = 0
        self._last_failed_job_id = None
        self._crashed_executing = False

    def clear_if_job_gone(self, active_job_ids: Set[str]) -> None:
        """如果先前失败的任务已不在活跃队列中，清除崩溃状态。"""
        if (
            self._last_failed_job_id is not None
            and self._last_failed_job_id not in active_job_ids
        ):
            self.reset()

    def record_dispatch_failed(self, prompt_id: str) -> int:
        """记录派发失败，递增跳过偏移。"""
        self._dispatch_skip_offset += 1
        self._last_failed_job_id = prompt_id
        return self._dispatch_skip_offset

    def record_dispatch_success(self) -> None:
        """派发成功后重置跳过偏移。"""
        self._dispatch_skip_offset = 0

    def record_crash(self, job_id: str) -> str:
        """记录下游崩溃事件，返回下一步动作。

        Returns:
            'skip': 崩溃次数未超阈值，任务应被重新入队（requeue）并跳过。
            'permanent_fail': 崩溃次数超过阈值，任务应标记为永久失败。
        """
        if self._last_failed_job_id is not None and self._last_failed_job_id != job_id:
            self.reset()

        self._last_failed_job_id = job_id

        if self._crash_count >= self._MAX_CRASH_COUNT:
            self.reset()
            return "permanent_fail"

        return "skip"

    def increment_crash(self) -> None:
        """递增崩溃计数并标记下游崩溃执行状态（仅在 requeue 成功后调用）。"""
        self._crash_count += 1
        self._crashed_executing = True

    def record_completion(self, ever_active: bool) -> str:
        """任务执行结束（非崩溃），返回完成类型。

        Returns:
            'complete': 正常完成，清零崩溃计数。
            'requeue_complete': 崩溃后 requeue 的任务成功完成，清零崩溃计数。
        """
        if self._crashed_executing:
            self._crashed_executing = False
            if ever_active:
                self.reset()
                return "requeue_complete"
            return "noop"

        self.reset()
        return "complete"

    def get_skip(self, pending_count: int) -> Optional[int]:
        """计算当前应跳过的 pending 任务数。"""
        if pending_count <= 0:
            return None
        return self._dispatch_skip_offset % pending_count
