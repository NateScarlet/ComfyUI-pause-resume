import logging
from gateway.domain.gateway import Gateway

logger = logging.getLogger(__name__)


class PauseQueueCommandHandler:
    """暂停任务队列的 Command Handler。"""

    def __init__(self, gateway: Gateway):
        self._gateway = gateway

    def handle(self, restart_after_idle: bool = False) -> None:
        """执行网关暂停。"""
        self._gateway.pause(restart_after_idle)
        if restart_after_idle:
            logger.info("⏸️ Queue Paused (will restart downstream when idle)")
        else:
            logger.info("⏸️ Queue Paused")


class ResumeQueueCommandHandler:
    """恢复任务队列的 Command Handler。"""

    def __init__(self, gateway: Gateway):
        self._gateway = gateway

    def handle(self) -> None:
        """执行恢复暂停。"""
        self._gateway.resume()
        logger.info("▶️ Queue Resumed")
