from typing import Optional, Callable
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


class Gateway:
    """网关核心业务领域的聚合根，控制网关生命周期、状态调度与决策逻辑。

    本类不依赖任何 I/O、HTTP 框架或操作系统底层 API，仅关注纯粹的业务状态转换与驱动。
    所有依赖的抽象接口都在构造时注入，不为空，确保领域层无状态防守。
    """

    def __init__(
        self,
        state_repo: StateRepository,
        queue_reader: TaskQueueReader,
        process_manager: ProcessManager,
        power_controller: SystemPowerController,
        timer: Timer,
        downstream: DownstreamClient,
        dispatcher: TaskDispatcher,
        event_bus: EventBus,
        idle_restart_timeout: float = 0,
        restart_after_idle_on_pause: bool = False,
        downstream_executing: bool = False,
        downstream_ready: bool = False,
        ever_active: bool = False,
        attempt_count: int = 0,
    ):
        self._state_repo = state_repo
        self._queue_reader = queue_reader
        self._process_manager = process_manager
        self._power_controller = power_controller
        self._timer = timer
        self._idle_restart_timeout = idle_restart_timeout

        self.paused = state_repo.get_paused()
        self.restart_after_idle_on_pause = restart_after_idle_on_pause
        self.downstream_executing = downstream_executing
        self.downstream_ready = downstream_ready
        self.ever_active = ever_active
        self.attempt_count = attempt_count

        self._is_idle = False
        self._cancel_idle_timeout: Optional[Callable[[], None]] = None

        self._downstream = downstream
        self._dispatcher = dispatcher
        self._event_bus = event_bus

        # 领域聚合根在构造时，自己订阅感兴趣的事件，保持高内聚
        self._event_bus.subscribe(
            DownstreamExecutingChangedEvent,
            lambda ev: self.set_downstream_executing(ev.executing),
        )
        self._event_bus.subscribe(
            DownstreamReadyChangedEvent,
            lambda ev: self.set_downstream_ready(ev.ready),
        )

    def refresh(self) -> None:
        """根据网关当前的业务决策，刷新阻止系统休眠、空闲重启超时与外挂脚本状态。"""
        has_pending = self._queue_reader.get_pending_count(limit=1) > 0
        scripts_running = self._process_manager.is_running()

        should_prevent = self.should_prevent_sleep(has_pending, scripts_running)
        is_busy = self.is_busy(has_pending)

        # 更新空闲/繁忙外挂脚本运行状态
        self._process_manager.update_state(is_busy, self.ever_active)

        if should_prevent:
            self._power_controller.prevent_sleep()
        else:
            self._power_controller.allow_sleep()

        self._downstream.on_sleep_prevention_changed(should_prevent)

        # 检测并触发进入或退出空闲状态
        is_idle = not should_prevent
        if is_idle and not self._is_idle:
            self._is_idle = True
            self._on_idle_entered()
        elif not is_idle and self._is_idle:
            self._is_idle = False
            self._on_idle_exited()

    def _on_idle_entered(self) -> None:
        """进入业务空闲状态时的决策逻辑。"""
        if self.restart_after_idle_on_pause:
            self.restart_after_idle_on_pause = False
            self._downstream.restart()
            self._event_bus.publish(StateChangedEvent(paused=self.paused))
            return

        if self.ever_active and self._idle_restart_timeout > 0:
            self._cancel_idle_timeout = self._timer.start_timeout(
                self._idle_restart_timeout, self._on_idle_timeout
            )

    def _on_idle_exited(self) -> None:
        """退出业务空闲状态时的决策逻辑（取消超时定时器）。"""
        if self._cancel_idle_timeout:
            self._cancel_idle_timeout()
            self._cancel_idle_timeout = None

    def _on_idle_timeout(self) -> None:
        """空闲超时到达时的自动重启决策。"""
        self._cancel_idle_timeout = None
        self._downstream.restart()

    def pause(self, restart_after_idle: bool) -> None:
        """暂停队列。"""
        self.paused = True
        self.restart_after_idle_on_pause = restart_after_idle
        self._state_repo.set_paused(True)

        self._event_bus.publish(StateChangedEvent(paused=True))
        self.refresh()

        # 如果在此之前系统已经处于空闲状态（且请求立即重启），由于 sync_infrastructure
        # 不会重复触发 _on_idle_entered，因此我们需要在这里直接触发重启
        if restart_after_idle and self._is_idle and self.restart_after_idle_on_pause:
            self.restart_after_idle_on_pause = False
            self._downstream.restart()

    def resume(self) -> None:
        """恢复队列，并自动触发副作用。"""
        self.paused = False
        self.restart_after_idle_on_pause = False
        self._state_repo.set_paused(False)

        self._event_bus.publish(StateChangedEvent(paused=False))
        self.refresh()
        self._dispatcher.try_dispatch()

    def set_downstream_executing(self, executing: bool) -> None:
        """设置下游的执行状态，并决策触发相应的业务动作。"""
        if self.downstream_executing == executing:
            return

        self.downstream_executing = executing
        if executing:
            self.ever_active = True
            self.refresh()
        else:
            self.attempt_count = 0
            self.refresh()
            self._event_bus.publish(StatusChangedEvent())
            self._dispatcher.try_dispatch()

    def set_downstream_ready(self, ready: bool) -> None:
        """设置下游就绪状态，并在合适时触发派发。"""
        self.downstream_ready = ready
        if ready and not self.paused:
            self._dispatcher.try_dispatch()

    def on_dispatch_success(self) -> None:
        """当派发任务成功时的业务反馈。"""
        self._event_bus.publish(StatusChangedEvent())

    def on_dispatch_failed(self, is_permanent: bool) -> bool:
        """当派发任务失败时的处理决策。"""
        self._event_bus.publish(StatusChangedEvent())
        if is_permanent:
            return False

        self.attempt_count += 1
        return True

    def increment_attempt_count(self) -> None:
        """物理崩溃时增加重试计数。"""
        self.attempt_count += 1

    def on_task_added(self) -> None:
        """当新任务入队时的业务逻辑与副作用驱动。"""
        self.refresh()
        self._event_bus.publish(StatusChangedEvent())
        self._dispatcher.try_dispatch()

    def on_queue_modified(self) -> None:
        """当队列内容被修改时的业务逻辑与副作用驱动。"""
        self.refresh()
        self._event_bus.publish(StatusChangedEvent())
        self._dispatcher.try_dispatch()

    def get_dispatch_skip(self, pending_count: int) -> Optional[int]:
        """核心派发调度决策。"""
        if self.paused or self.downstream_executing or not self.downstream_ready:
            return None

        if pending_count <= 0:
            self.attempt_count = 0
            return None

        return self.attempt_count % pending_count

    def is_busy(self, has_pending: bool) -> bool:
        """根据网关当前的状态和是否有任务，判定是否处于繁忙业务状态。"""
        return self.downstream_executing or (not self.paused and has_pending)

    def should_prevent_sleep(
        self, has_pending: bool, scripts_running: bool
    ) -> bool:
        """决策当前网关是否应当阻止操作系统进入休眠。"""
        is_busy = self.is_busy(has_pending)
        return is_busy or scripts_running
