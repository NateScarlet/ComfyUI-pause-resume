from gateway.shared.models import DownstreamAction

# 判定内存紧张所需的连续低样本次数
_LOW_SAMPLE_COUNT = 3


class DownstreamResourceDecision:
    """下游资源释放决策的领域内深模块，聚合根是唯一调用方。

    本对象只回答「该做什么」，不执行进程控制、定时器排程或事件发布。
    空闲窗口到期时刻、连续低内存采样计数、显式「暂停后重启」指令由本对象持有；
    队列是否为空与生命周期标志由聚合根读取后作为参数传入。
    """

    def __init__(
        self,
        idle_restart_timeout: float,
        shutdown_memory_threshold_mb: int,
        now: float,
        restart_after_idle: bool = False,
        low_memory_samples: int = 0,
    ) -> None:
        self._idle_restart_timeout = idle_restart_timeout
        self._shutdown_memory_threshold_mb = shutdown_memory_threshold_mb
        self._idle_window_end = now + idle_restart_timeout
        self._restart_after_idle = restart_after_idle
        self._low_memory_samples = low_memory_samples

    @property
    def low_memory_samples(self) -> int:
        """连续低内存采样计数，供入口级测试观察。"""
        return self._low_memory_samples

    @property
    def restart_after_idle(self) -> bool:
        """当前挂起的显式「暂停后重启」指令。"""
        return self._restart_after_idle

    def set_restart_after_idle(self, value: bool) -> None:
        """设置显式「暂停后重启」指令。"""
        self._restart_after_idle = value

    def clear_restart_after_idle(self) -> None:
        """清除显式「暂停后重启」指令。"""
        self._restart_after_idle = False

    def clear_samples(self) -> None:
        """停止采样时清空连续低样本计数。"""
        self._low_memory_samples = 0

    def record_sample(self, available_mb: int) -> None:
        """按注入的阈值更新连续低内存采样计数。"""
        if available_mb < self._shutdown_memory_threshold_mb:
            self._low_memory_samples += 1
        else:
            self._low_memory_samples = 0

    def defer_window(self, now: float) -> None:
        """把空闲窗口起点推迟到此刻，重新计满一个完整窗口。"""
        self._idle_window_end = now + self._idle_restart_timeout

    def remaining(self, now: float) -> float:
        """返回空闲窗口剩余秒数；窗口禁用或已到期时返回 0。"""
        if self._idle_restart_timeout <= 0:
            return 0.0
        return max(0.0, self._idle_window_end - now)

    def _window_can_follow_up(self, now: float) -> bool:
        """采样补做只发生在窗口已走完的情况；窗口禁用时不存在「到期」。"""
        return self._idle_restart_timeout > 0 and self.remaining(now) <= 0

    def _take_restart_instruction(self) -> bool:
        """读取并消费显式指令，返回消费前的值；避免多个入口重复做消费。"""
        pending = self._restart_after_idle
        self._restart_after_idle = False
        return pending

    def _restart_wanted(self, allow_restart: bool) -> bool:
        """当前生命周期执行过任务，且存在显式指令或空闲窗口时，重启才是候选动作。

        没有任务的下游必须在每个空闲窗口被反复重启之前被这道门控挡住。
        """
        return allow_restart and (
            self._restart_after_idle or self._idle_restart_timeout > 0
        )

    def window_needed(self, allow_shutdown: bool, allow_restart: bool) -> bool:
        """判定是否需要为资源释放排程空闲窗口，供聚合根决定是否安排定时器。

        空闲停机针对下游常驻的启动基线内存，与是否执行过任务无关；即使下游从未
        执行过任务，只要用户授权了停机就需要窗口。
        """
        return self._restart_wanted(allow_restart) or (
            allow_shutdown and self._idle_restart_timeout > 0
        )

    def _shutdown_ready(
        self, idle: bool, queue_empty: bool, allow_shutdown: bool
    ) -> bool:
        """停机是用户授权下的许可动作：空闲、队列空、连续低采样达标。"""
        return (
            allow_shutdown
            and idle
            and queue_empty
            and self._low_memory_samples >= _LOW_SAMPLE_COUNT
        )

    def evaluate(
        self,
        now: float,
        idle: bool,
        queue_empty: bool,
        allow_shutdown: bool,
        allow_restart: bool,
    ) -> DownstreamAction:
        """窗口到期入口：读取并消费显式指令，给出唯一的资源释放动作。"""
        if not idle:
            return DownstreamAction.NONE
        if not self.window_needed(allow_shutdown, allow_restart):
            return DownstreamAction.NONE

        # 显式「暂停后重启」是用户直接指令，但仅在本生命周期执行过任务时才优先于停机
        explicit_restart = self._take_restart_instruction() and allow_restart

        if not explicit_restart and self._shutdown_ready(
            idle, queue_empty, allow_shutdown
        ):
            return DownstreamAction.SHUTDOWN
        if self._restart_wanted(allow_restart) or explicit_restart:
            return DownstreamAction.RESTART
        # 内存充足但下游从未执行过任务：等待下一次判定时机
        return DownstreamAction.NONE

    def evaluate_on_idle_entered(self, now: float) -> DownstreamAction:
        """非空闲→空闲的边沿入口：消费挂起的显式指令或推迟窗口起点。

        显式指令在窗口已走完时立即重启；窗口未走完则只保留指令由聚合根排程。
        这里沿用既有语义：该边沿不判断内存与授权，重启只取决于窗口是否已到期。
        """
        if not self._restart_after_idle:
            self.defer_window(now)
            return DownstreamAction.NONE
        if self.remaining(now) > 0:
            return DownstreamAction.NONE
        self._take_restart_instruction()
        return DownstreamAction.RESTART

    def evaluate_after_sampling(
        self,
        now: float,
        idle: bool,
        queue_empty: bool,
        allow_shutdown: bool,
        allow_restart: bool,
    ) -> DownstreamAction:
        """采样后补做入口：窗口禁用、未到期或有显式指令时一律不补做。

        allow_restart 在本入口不参与判定：停机针对启动基线内存，与是否执行过任务无关。
        """
        if not self._window_can_follow_up(now):
            return DownstreamAction.NONE
        if self._restart_after_idle:
            return DownstreamAction.NONE
        if self._shutdown_ready(idle, queue_empty, allow_shutdown):
            return DownstreamAction.SHUTDOWN
        return DownstreamAction.NONE
