import logging
import time
from typing import Optional, Callable, List
from gateway.shared.interfaces import (
    StateRepository,
    JobQueueReader,
    JobQueueWriter,
    ProcessManager,
    SystemPowerController,
    Timer,
    DownstreamClient,
    JobDispatcher,
    EventBus,
)
from gateway.shared.models import Job, JobStatus, JobFilters
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
from gateway.domain.estimation_service import EstimationService

logger = logging.getLogger(__name__)


class Gateway:
    """网关核心业务领域的聚合根，控制网关生命周期、状态调度与决策逻辑。

    本类不依赖任何 I/O、HTTP 框架或操作系统底层 API，仅关注纯粹的业务状态转换与驱动。
    所有依赖的抽象接口都在构造时注入，不为空，确保领域层无状态防守。
    """

    def __init__(
        self,
        state_repo: StateRepository,
        queue_reader: JobQueueReader,
        queue_writer: JobQueueWriter,
        process_manager: ProcessManager,
        power_controller: SystemPowerController,
        timer: Timer,
        downstream: DownstreamClient,
        dispatcher: JobDispatcher,
        event_bus: EventBus,
        estimation_service: EstimationService,
        idle_restart_timeout: float = 0,
        restart_after_idle_on_pause: bool = False,
        downstream_executing: bool = False,
        downstream_ready: bool = False,
        ever_active: bool = False,
        crash_count: int = 0,
        dispatch_skip_offset: int = 0,
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

        # 崩溃计数：当前任务连续崩溃的次数，仅用于崩溃跳过阈值判断
        self._crash_count = crash_count
        # 派发跳过偏移：派发失败后跳过已失败任务的偏移量，用于 get_dispatch_skip
        self._dispatch_skip_offset = dispatch_skip_offset

        self._is_idle = False
        self._cancel_idle_timeout: Optional[Callable[[], None]] = None
        self._cancel_dispatch_retry: Optional[Callable[[], None]] = None
        self._last_failed_job_id: Optional[str] = None
        self._refreshing = False
        self._refresh_needed = False
        self._crashed_executing = False
        self._job_start_time: Optional[int] = None  # 当前任务开始执行时间（毫秒）

        self._downstream = downstream
        self._dispatcher = dispatcher
        self._event_bus = event_bus
        self._estimation_service = estimation_service

        # 领域聚合根在构造时，自己订阅感兴趣的物理与业务事件，保持高内聚
        self._unsubscribe_callbacks: List[Callable[[], None]] = [
            self._event_bus.subscribe(
                DownstreamExecutingChangedEvent,
                self._handle_downstream_executing_changed,
            ),
            self._event_bus.subscribe(
                DownstreamReadyChangedEvent,
                self._handle_downstream_ready_changed,
            ),
            self._event_bus.subscribe(
                DownstreamCrashedEvent,
                self._handle_downstream_crashed,
            ),
            self._event_bus.subscribe(
                QueueModifiedEvent,
                self._handle_queue_modified,
            ),
            self._event_bus.subscribe(
                DispatchSuccessEvent,
                self._handle_dispatch_success,
            ),
            self._event_bus.subscribe(
                DispatchFailedEvent,
                self._handle_dispatch_failed,
            ),
            self._event_bus.subscribe(
                ScriptStateChangedEvent,
                self._handle_script_state_changed,
            ),
        ]

        # 初始同步阻止系统休眠和外挂脚本状态
        self._refresh()
        self._event_bus.publish(StateChangedEvent(paused=self._paused))
        self._publish_status_changed()

    @property
    def paused(self) -> bool:
        """向外部提供网关当前是否暂停的只读状态。"""
        return self._paused

    def _refresh(self) -> None:
        """根据网关当前的业务决策，刷新阻止系统休眠、空闲重启超时与外挂脚本状态。"""
        if self._refreshing:
            self._refresh_needed = True
            return
        self._refreshing = True
        self._refresh_needed = False
        try:
            for _ in range(10):

                total_count = self._queue_reader.count(
                    JobFilters([JobStatus.PENDING, JobStatus.RUNNING]), limit=1
                )
                has_jobs = total_count > 0

                # 如果没有任何任务，重置所有计数
                if not has_jobs:
                    self._crash_count = 0
                    self._dispatch_skip_offset = 0
                    self._last_failed_job_id = None
                    if self._cancel_dispatch_retry:
                        self._cancel_dispatch_retry()
                        self._cancel_dispatch_retry = None
                elif self._last_failed_job_id is not None:
                    # 检查先前失败的任务是否已经不在队列中（例如被用户手动删除）
                    all_jobs = [
                        t
                        for _, t in self._queue_reader.list(
                            JobFilters([JobStatus.PENDING, JobStatus.RUNNING])
                        )
                    ]
                    active_ids = {t.prompt_id for t in all_jobs}
                    if self._last_failed_job_id not in active_ids:
                        self._crash_count = 0
                        self._dispatch_skip_offset = 0
                        self._last_failed_job_id = None

                is_busy = self._is_busy(has_jobs)

                # 更新空闲/繁忙外挂脚本运行状态
                self._process_manager.update_state(is_busy, self._ever_active)

                # 必须在 update_state 触发运行状态变更之后，重新获取外挂脚本最新的实际运行状态，以保证后续判断基于最新数据
                scripts_running = self._process_manager.is_running()

                should_prevent = self._should_prevent_sleep(has_jobs, scripts_running)

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

                if not self._refresh_needed:
                    break
                self._refresh_needed = False
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
            self._cancel_idle_timeout = None
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

        # 暂停时取消未完成的派发重试，避免在暂停期间无意义调用 dispatch
        if self._cancel_dispatch_retry:
            self._cancel_dispatch_retry()
            self._cancel_dispatch_retry = None

        self._event_bus.publish(StateChangedEvent(paused=True))
        self._refresh()

        # 如果此时系统已处于空闲状态，_refresh 不会触发 _on_idle_entered 转换，
        # 因此需要在此处直接处理空闲立即重启逻辑
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

        # 恢复时取消空闲超时定时器，避免在恢复后仍然空闲时触发非预期的下游重启
        if self._cancel_idle_timeout:
            self._cancel_idle_timeout()
            self._cancel_idle_timeout = None

        # 恢复时取消未完成的派发重试，恢复后会通过 dispatch 重新触发
        if self._cancel_dispatch_retry:
            self._cancel_dispatch_retry()
            self._cancel_dispatch_retry = None

        self._event_bus.publish(StateChangedEvent(paused=False))
        self._refresh()
        self._dispatcher.dispatch(self.get_dispatch_skip())

    def _handle_downstream_executing_changed(
        self, ev: DownstreamExecutingChangedEvent
    ) -> None:
        """设置下游的执行状态，并决策触发相应的业务动作。"""
        executing = ev.executing
        if self._downstream_executing == executing:
            logger.debug(
                "DownstreamExecutingChanged ignored: already %s (crashed=%s paused=%s ready=%s)",
                executing,
                self._crashed_executing,
                self._paused,
                self._downstream_ready,
            )
            return

        logger.info(
            "DownstreamExecutingChanged: %s → %s (crashed=%s paused=%s ready=%s ever_active=%s)",
            self._downstream_executing,
            executing,
            self._crashed_executing,
            self._paused,
            self._downstream_ready,
            self._ever_active,
        )
        self._downstream_executing = executing
        if executing:
            # 只有在下游真正开始执行任务时，才将 _ever_active 设为 True
            self._ever_active = True
            self._job_start_time = int(time.time() * 1000)
            self._refresh()
        else:
            # 任务完成时记录执行时长（崩溃时不记录，避免污染预估数据）
            if not self._crashed_executing and self._job_start_time is not None:
                duration_ms = int(time.time() * 1000) - self._job_start_time
                self._estimation_service.record_completion(duration_ms)
            self._job_start_time = None

            # 如果是因为崩溃导致的执行结束，不能清零失败计数，否则无法完成崩溃跳过逻辑
            if self._crashed_executing:
                self._crashed_executing = False
                # 如果崩溃 requeue 的任务已经重新执行并完成（_ever_active 为 True），
                # 说明任务成功完成，应重置崩溃计数并清理运行队列
                if self._ever_active:
                    self._crash_count = 0
                    self._last_failed_job_id = None
                    self._queue_writer.update_status(
                        JobStatus.COMPLETED, filter_status=JobStatus.RUNNING
                    )
            else:
                self._crash_count = 0
                self._last_failed_job_id = None
                self._queue_writer.update_status(
                    JobStatus.COMPLETED, filter_status=JobStatus.RUNNING
                )
            self._refresh()
            skip = self.get_dispatch_skip()
            logger.info(
                "DownstreamExecutingChanged: executing=False done, dispatch_skip=%s "
                "(pending=%s crashed=%s paused=%s ready=%s)",
                skip,
                self._queue_reader.count(JobFilters([JobStatus.PENDING])),
                self._crashed_executing,
                self._paused,
                self._downstream_ready,
            )
            self._publish_status_changed()
            self._dispatcher.dispatch(skip)

    def _handle_downstream_ready_changed(self, ev: DownstreamReadyChangedEvent) -> None:
        """设置下游就绪状态，并在合适时触发派发。"""
        self._downstream_ready = ev.ready
        self._refresh()
        self._publish_status_changed()
        if ev.ready:
            if not self._paused:
                self._dispatcher.dispatch(self.get_dispatch_skip())
        else:
            # 当下游变为不可用时，取消处于等待中的延迟重试，并重置派发跳过状态
            if self._cancel_dispatch_retry:
                self._cancel_dispatch_retry()
                self._cancel_dispatch_retry = None
            self._dispatch_skip_offset = 0
            self._last_failed_job_id = None

    def _handle_downstream_crashed(self, ev: DownstreamCrashedEvent) -> None:
        """当下游物理进程发生非预期崩溃时，自行决策并执行物理重入列并刷新状态。"""
        running_jobs: List[Job] = [
            t for _, t in self._queue_reader.list(JobFilters([JobStatus.RUNNING]))
        ]
        self._ever_active = False

        if running_jobs:
            job = running_jobs[0]
            # 如果崩溃的任务与上次失败的任务不同，重置崩溃计数（按任务隔离）
            if (
                self._last_failed_job_id is not None
                and self._last_failed_job_id != job.prompt_id
            ):
                self._crash_count = 0
            self._last_failed_job_id = job.prompt_id
            # 崩溃超过阈值直接标记为永久失败，避免阻塞整个队列
            if self._crash_count >= 2:
                self._queue_writer.update_status(
                    JobStatus.FAILED, prompt_id=job.prompt_id
                )
                self._crash_count = 0
                self._last_failed_job_id = None
                self._dispatcher.handle_failed_job(
                    job, "Downstream crashed 3 times during execution."
                )
                self._crashed_executing = False
            else:
                if self._queue_writer.requeue_running_if_exists():
                    self._crash_count += 1
                    self._crashed_executing = True

        self._publish_status_changed()
        self._refresh()

    def _handle_script_state_changed(self, ev: ScriptStateChangedEvent) -> None:
        """当外挂辅助程序状态发生变化时，决策并重新刷新阻止休眠与空闲超时等状态。"""
        self._refresh()
        self._publish_status_changed()

    def _handle_queue_modified(self, ev: QueueModifiedEvent) -> None:
        """当队列内容被修改（新任务入队、清空或删除）时的业务逻辑与副作用驱动。"""
        self._refresh()
        self._publish_status_changed()
        self._dispatcher.dispatch(self.get_dispatch_skip())

    def _handle_dispatch_success(self, ev: DispatchSuccessEvent) -> None:
        """当派发任务成功时的业务反馈。"""
        self._dispatch_skip_offset = 0
        if self._cancel_dispatch_retry:
            self._cancel_dispatch_retry()
            self._cancel_dispatch_retry = None
        self._publish_status_changed()

    def _handle_dispatch_failed(self, ev: DispatchFailedEvent) -> None:
        """当派发任务失败时的处理决策。"""
        self._publish_status_changed()
        if not ev.is_permanent:
            self._dispatch_skip_offset += 1
            self._last_failed_job_id = ev.prompt_id

            # 启动延迟重试驱动，避免瞬时网络异常引起高频无效空转
            if self._cancel_dispatch_retry:
                self._cancel_dispatch_retry()
            self._cancel_dispatch_retry = self._timer.start_timeout(
                5.0, self._retry_dispatch
            )

    def _retry_dispatch(self) -> None:
        """定时器超时后的重试派发回调，重新获取最新的 skip 并触发派发。"""
        self._cancel_dispatch_retry = None
        self._dispatcher.dispatch(self.get_dispatch_skip())

    def get_dispatch_skip(self) -> Optional[int]:
        """核心派发调度决策（无副作用纯查询）。"""
        if self._paused or self._downstream_executing or not self._downstream_ready:
            return None

        pending_count = self._queue_reader.count(JobFilters([JobStatus.PENDING]))
        if pending_count <= 0:
            return None

        return self._dispatch_skip_offset % pending_count

    def _is_busy(self, has_jobs: bool) -> bool:
        """根据网关当前的状态和是否有任务，判定是否处于繁忙业务状态。"""
        return self._downstream_executing or (not self._paused and has_jobs)

    def _should_prevent_sleep(self, has_jobs: bool, scripts_running: bool) -> bool:
        """决策当前网关是否应当阻止操作系统进入休眠。"""
        is_busy = self._is_busy(has_jobs)
        return is_busy or scripts_running

    def _publish_status_changed(self) -> None:
        """发布状态变更事件，并输出包含队列长度和预估时间的 INFO 日志。"""
        remaining = self._queue_reader.count(
            JobFilters([JobStatus.PENDING, JobStatus.RUNNING])
        )
        avg_ms = self._estimation_service.calculate_estimation()
        # 计算所有剩余任务的总预估时间
        estimated_ms = avg_ms * remaining if avg_ms is not None else None

        if estimated_ms is not None:
            seconds = estimated_ms // 1000
            minutes, secs = divmod(seconds, 60)
            hours, mins = divmod(minutes, 60)
            if hours > 0:
                time_str = f"{hours}h{mins}m{secs}s"
            elif minutes > 0:
                time_str = f"{minutes}m{secs}s"
            else:
                time_str = f"{secs}s"
            logger.info(
                "Status: queue_remaining=%d, estimated_time=%s", remaining, time_str
            )
        else:
            logger.info("Status: queue_remaining=%d", remaining)

        self._event_bus.publish(
            StatusChangedEvent(
                queue_remaining=remaining, estimated_time_ms=estimated_ms
            )
        )

    def dispose(self) -> None:
        """取消所有事件订阅，释放聚合根持有的引用，防止内存泄漏。"""
        for unsubscribe in self._unsubscribe_callbacks:
            unsubscribe()
        self._unsubscribe_callbacks.clear()
        if self._cancel_idle_timeout:
            self._cancel_idle_timeout()
            self._cancel_idle_timeout = None
        if self._cancel_dispatch_retry:
            self._cancel_dispatch_retry()
            self._cancel_dispatch_retry = None
