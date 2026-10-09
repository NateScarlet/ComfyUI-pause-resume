import logging
from gateway.domain.gateway import Gateway

logger = logging.getLogger(__name__)


class RestartDownstreamCommandHandler:
    """强制重启下游的 Command Handler。"""

    def __init__(self, gateway: Gateway):
        self._gateway = gateway

    def handle(self) -> None:
        """执行强制重启。"""
        self._gateway.restart_now()
        logger.info("🔄 Downstream force restart issued (running jobs requeued)")
