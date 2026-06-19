import logging
from gateway.domain.gateway import Gateway
from gateway.shared.interfaces import StateRepository
from gateway.application.services.downstream import DownstreamAppService

logger = logging.getLogger(__name__)


class PauseQueueCommandHandler:
    """暂停任务队列的 Command Handler。"""

    def __init__(
        self,
        gateway: Gateway,
        state_repo: StateRepository,
        downstream_service: DownstreamAppService,
    ):
        self._gateway = gateway
        self._state_repo = state_repo
        self._downstream_service = downstream_service

    def handle(self, restart_after_idle: bool = False) -> None:
        """执行网关暂停，根据聚合根决定是否立即重启下游服务。"""
        has_pending = (
            self._downstream_service.queue_reader.get_pending_count(limit=1) > 0
        )
        is_currently_idle = not self._gateway.determine_busy_state(has_pending)

        decision = self._gateway.pause(restart_after_idle, is_currently_idle)
        self._state_repo.set_paused(True)

        if decision == "RESTART_IMMEDIATELY":
            logger.info(
                "🔄 Pause-and-restart: already paused and idle, restarting now..."
            )
            if (
                self._downstream_service.loop is not None
                and self._downstream_service.loop.is_running()
            ):
                self._downstream_service.loop.create_task(
                    self._downstream_service.restart_downstream()
                )

        if restart_after_idle:
            logger.info("⏸️ Queue Paused (will restart downstream when idle)")
        else:
            logger.info("⏸️ Queue Paused")

        self._downstream_service.sync_state_to_infrastructure()
        self._downstream_service.notify_state_changed(True)


class ResumeQueueCommandHandler:
    """恢复任务队列的 Command Handler。"""

    def __init__(
        self,
        gateway: Gateway,
        state_repo: StateRepository,
        downstream_service: DownstreamAppService,
    ):
        self._gateway = gateway
        self._state_repo = state_repo
        self._downstream_service = downstream_service

    def handle(self) -> None:
        """执行恢复暂停，更新持久化状态并尝试分发派发任务。"""
        should_dispatch = self._gateway.resume()
        self._state_repo.set_paused(False)

        logger.info("▶️ Queue Resumed")

        self._downstream_service.sync_state_to_infrastructure()
        self._downstream_service.notify_state_changed(False)

        if should_dispatch:
            self._downstream_service.try_dispatch()
