from typing import Optional, Callable
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


class Gateway:
    """网关核心业务领域的聚合根，控制网关生命周期、状态调度与决策逻辑。

    本类不依赖任何 I/O、HTTP 框架或操作系统底层 API，仅关注纯粹的业务状态转换与驱动。
    所有依赖的抽象接口都在构造时注入，不为空，确保领域层无状态防守。
    """

    def __init__(
        self,
        state_repo: StateRepository,
        queue_reader: TaskQueueReader,
        queue_writer: TaskQueueWriter,
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
        self._queue_writer = queue_writer
        self._process_manager = process_manager
        self._power_controller = power_controller
        self._timer = timer
        self._idle_restart_timeout = idle_restart_timeout

        self._paused = state_repo.get_paused()
        self._restart_after_idle_on_pause = restart_after_idle_on_pause
        self._downstream_executing = downstream_executing
        self._downstream_ready = downstream_ready

        # _ever_active 指的是下游在当前运行生命周期内是否曾经收到或执行过任务（即决定是否占用了显存/系统资源）。
        # 当下游发生重启（包括空闲超时重启、被动崩溃重启）时，代表下游进程返回干净的初始状态，
        # 此时该标志必须被重置为 False，直到下游下一次真正开始执行任务时才置为 True。
        self._ever_active = ever_active

        self._attempt_count = attempt_count

        self._is_idle = False
        self._cancel_idle_timeout: Optional[Callable[[], None]] = None
        self._refreshing = False
        self._crashed_executing = False

        self._downstream = downstream
        self._dispatcher = dispatcher
        self._event_bus = event_bus

        # 领域聚合根在构造时，自己订阅感兴趣的物理与业务事件，保持高内聚
        self._event_bus.subscribe(
            DownstreamExecutingChangedEvent,
            self._handle_downstream_executing_changed,
        )
        self._event_bus.subscribe(
            DownstreamReadyChangedEvent,
            self._handle_downstream_ready_changed,
        )
        self._event_bus.subscribe(
            DownstreamCrashedEvent,
            self._handle_downstream_crashed,
        )
        self._event_bus.subscribe(
            QueueModifiedEvent,
            self._handle_queue_modified,
        )
        self._event_bus.subscribe(
            DispatchSuccessEvent,
            self._handle_dispatch_success,
        )
        self._event_bus.subscribe(
            DispatchFailedEvent,
            self._handle_dispatch_failed,
        )
        self._event_bus.subscribe(
            ScriptStateChangedEvent,
            self._handle_script_state_changed,
        )

        # 初始同步阻止系统休眠和外挂脚本状态
        self._refresh()

    @property
    def paused(self) -> bool:
        """向外部提供网关当前是否暂停的只读状态。"""
        return self._paused

    def _refresh(self) -> None:
        """根据网关当前的业务决策，刷新阻止系统休眠、空闲重启超时与外挂脚本状态。"""
        if self._refreshing:
            return
        self._refreshing = True
        try:
            pending_count = self._queue_reader.get_pending_count(limit=1)
            has_pending = pending_count > 0

            # 如果没有排队任务，重置尝试计数
            if not has_pending:
                self._attempt_count = 0

            is_busy = self._is_busy(has_pending)

            # 更新空闲/繁忙外挂脚本运行状态
            self._process_manager.update_state(is_busy, self._ever_active)

            # 必须在 update_state 触发运行状态变更之后，重新获取外挂脚本最新的实际运行状态，以保证后续判断基于最新数据
            scripts_running = self._process_manager.is_running()

            should_prevent = self._should_prevent_sleep(has_pending, scripts_running)

            if should_prevent:
                self._power_controller.prevent_sleep()
            else:
                self._power_controller.allow_sleep()

            # 检测并触发进入或退出空闲状态
            is_idle = not should_prevent
            if is_idle and not self._is_idle:
                self._is_idle = True
                self._on_idle_entered()
            elif not is_idle and self._is_idle:
                self._is_idle = False
                self._on_idle_exited()
        finally:
            self._refreshing = False

    def _on_idle_entered(self) -> None:
        """进入业务空闲状态时的决策逻辑。"""
        if self._restart_after_idle_on_pause:
            self._restart_after_idle_on_pause = False
            # 重启下游代表重启到干净状态，重置 _ever_active
            self._ever_active = False
            self._downstream.restart()
            self._event_bus.publish(StateChangedEvent(paused=self._paused))
            self._refresh()
            return

        if self._ever_active and self._idle_restart_timeout > 0:
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
        # 超时回调执行时必须进行防竞态空闲状态校验
        if not self._is_idle:
            return
        self._cancel_idle_timeout = None
        # 重启下游代表重启到干净状态，重置 _ever_active
        self._ever_active = False
        self._downstream.restart()
        self._refresh()

    def pause(self, restart_after_idle: bool) -> None:
        """暂停队列。"""
        self._paused = True
        self._restart_after_idle_on_pause = restart_after_idle
        self._state_repo.set_paused(True)

        self._event_bus.publish(StateChangedEvent(paused=True))
        self._refresh()

        # 如果在此之前系统已经处于空闲状态（且请求立即重启），由于 sync_infrastructure
        # 不会重复触发 _on_idle_entered，因此我们需要在这里直接触发重启
        if restart_after_idle and self._is_idle and self._restart_after_idle_on_pause:
            self._restart_after_idle_on_pause = False
            # 重启下游代表重启到干净状态，重置 _ever_active
            self._ever_active = False
            self._downstream.restart()
            self._refresh()

    def resume(self) -> None:
        """恢复队列，并自动触发副作用。"""
        self._paused = False
        self._restart_after_idle_on_pause = False
        self._state_repo.set_paused(False)

        self._event_bus.publish(StateChangedEvent(paused=False))
        self._refresh()
        self._dispatcher.try_dispatch()

    def _handle_downstream_executing_changed(
        self, ev: DownstreamExecutingChangedEvent
    ) -> None:
        """设置下游的执行状态，并决策触发相应的业务动作。"""
        executing = ev.executing
        if self._downstream_executing == executing:
            return

        self._downstream_executing = executing
        if executing:
            # 只有在下游真正开始执行任务时，才将 _ever_active 设为 True
            self._ever_active = True
            self._refresh()
        else:
            # 如果是因为崩溃导致的执行结束，不能清零失败计数，否则无法完成崩溃跳过逻辑
            if self._crashed_executing:
                self._crashed_executing = False
            else:
                self._attempt_count = 0
            self._refresh()
            self._event_bus.publish(StatusChangedEvent())
            self._dispatcher.try_dispatch()

    def _handle_downstream_ready_changed(self, ev: DownstreamReadyChangedEvent) -> None:
        """设置下游就绪状态，并在合适时触发派发。"""
        self._downstream_ready = ev.ready
        if ev.ready and not self._paused:
            self._dispatcher.try_dispatch()

    def _handle_downstream_crashed(self, ev: DownstreamCrashedEvent) -> None:
        """当下游物理进程发生非预期崩溃时，自行决策并执行物理重入列并刷新状态。"""
        if self._queue_writer.requeue_running_if_exists():
            self._attempt_count += 1
        # 记录崩溃导致的执行中断，以便在随后的 executing=False 事件中保留 attempt_count
        self._crashed_executing = True
        # 崩溃后会被重新拉起，拉起后重置为干净状态，因此将 _ever_active 设为 False
        self._ever_active = False
        self._event_bus.publish(StatusChangedEvent())
        self._refresh()

    def _handle_script_state_changed(self, ev: ScriptStateChangedEvent) -> None:
        """当外挂辅助程序状态发生变化时，决策并重新刷新阻止休眠与空闲超时等状态。"""
        self._refresh()
        self._event_bus.publish(StatusChangedEvent())

    def _handle_queue_modified(self, ev: QueueModifiedEvent) -> None:
        """当队列内容被修改（新任务入队、清空或删除）时的业务逻辑与副作用驱动。"""
        self._refresh()
        self._event_bus.publish(StatusChangedEvent())
        self._dispatcher.try_dispatch()

    def _handle_dispatch_success(self, ev: DispatchSuccessEvent) -> None:
        """当派发任务成功时的业务反馈。"""
        self._event_bus.publish(StatusChangedEvent())

    def _handle_dispatch_failed(self, ev: DispatchFailedEvent) -> None:
        """当派发任务失败时的处理决策。"""
        self._event_bus.publish(StatusChangedEvent())
        if not ev.is_permanent:
            self._attempt_count += 1

    def get_dispatch_skip(self) -> Optional[int]:
        """核心派发调度决策（无副作用纯查询）。"""
        if self._paused or self._downstream_executing or not self._downstream_ready:
            return None

        pending_count = self._queue_reader.get_pending_count()
        if pending_count <= 0:
            return None

        return self._attempt_count % pending_count

    def _is_busy(self, has_pending: bool) -> bool:
        """根据网关当前的状态和是否有任务，判定是否处于繁忙业务状态。"""
        return self._downstream_executing or (not self._paused and has_pending)

    def _should_prevent_sleep(self, has_pending: bool, scripts_running: bool) -> bool:
        """决策当前网关是否应当阻止操作系统进入休眠。"""
        is_busy = self._is_busy(has_pending)
        return is_busy or scripts_running
