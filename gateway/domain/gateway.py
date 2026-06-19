from typing import Optional
from gateway.shared.interfaces import StateRepository


class Gateway:
    """网关核心业务领域的聚合根，控制网关生命周期、状态调度与决策逻辑。

    本类不依赖任何 I/O、HTTP 框架或操作系统底层 API，仅关注纯粹的业务状态转换。
    持久化状态的读写通过注入的 StateRepository 抽象接口完成。
    """

    def __init__(
        self,
        state_repo: StateRepository,
        restart_after_idle_on_pause: bool = False,
        downstream_executing: bool = False,
        downstream_ready: bool = False,
        ever_active: bool = False,
        attempt_count: int = 0,
    ):
        self._state_repo = state_repo
        self.paused = state_repo.get_paused()
        self.restart_after_idle_on_pause = restart_after_idle_on_pause
        self.downstream_executing = downstream_executing
        self.downstream_ready = downstream_ready
        self.ever_active = ever_active
        self.attempt_count = attempt_count

    def pause(self, restart_after_idle: bool, is_currently_idle: bool) -> Optional[str]:
        """暂停队列。

        如果传入了闲置后重启标志，且网关当前已经是空闲状态，则决策为立即重启下游。
        """
        self.paused = True
        self.restart_after_idle_on_pause = restart_after_idle
        self._state_repo.set_paused(True)

        if restart_after_idle and is_currently_idle:
            self.restart_after_idle_on_pause = False
            return "RESTART_IMMEDIATELY"
        return None

    def resume(self) -> bool:
        """恢复队列，决策是否应当尝试向下一代发任务。"""
        self.paused = False
        self.restart_after_idle_on_pause = False
        self._state_repo.set_paused(False)
        return True

    def set_downstream_executing(self, executing: bool) -> Optional[str]:
        """设置下游的执行状态，并决策触发的业务动作。

        如果下游执行完任务变为空闲，则重置尝试次数，并决策清除运行队列且触发派发。
        """
        if self.downstream_executing == executing:
            return None

        self.downstream_executing = executing
        if executing:
            self.ever_active = True
            return "ENTER_BUSY"
        else:
            self.attempt_count = 0
            return "CLEAR_RUNNING_AND_DISPATCH"

    def set_downstream_ready(self, ready: bool) -> bool:
        """设置下游就绪状态，返回是否应该尝试派发任务。"""
        self.downstream_ready = ready
        return ready and not self.paused

    def on_dispatch_success(self) -> None:
        """当派发任务成功时的业务反馈。"""
        pass

    def on_dispatch_failed(self, is_permanent: bool) -> bool:
        """当派发任务失败时的处理决策。

        对于永久性不可恢复的错误（如 400 状态码），直接丢弃（返回 False 告知不重入队列）。
        对于临时性错误，累加尝试计数，并返回 True 告知需要重入队列。
        """
        if is_permanent:
            return False

        self.attempt_count += 1
        return True

    def calculate_dispatch_skip(self, pending_count: int) -> Optional[int]:
        """核心派发调度决策。

        根据当前网关暂停状态、下游执行状态和就绪状态，计算是否可派发。
        若可派发，计算得出待弹出任务的 skip 偏移量（用于实现循环/重试机制）。
        """
        if self.paused or self.downstream_executing or not self.downstream_ready:
            return None

        if pending_count <= 0:
            self.attempt_count = 0
            return None

        return self.attempt_count % pending_count

    def determine_busy_state(self, has_pending: bool) -> bool:
        """根据网关当前的状态和是否有任务，判定是否处于繁忙业务状态。

        若已被暂停，即使队列里有待处理任务且下游未执行，也不视为繁忙，以便允许系统休眠省电。
        """
        return self.downstream_executing or (not self.paused and has_pending)

    def determine_sleep_prevention(
        self, has_pending: bool, scripts_running: bool
    ) -> bool:
        """决策当前网关是否应当阻止操作系统进入休眠。"""
        is_busy = self.determine_busy_state(has_pending)
        return is_busy or scripts_running
