from gateway.domain.gateway import Gateway


class WakeDownstreamCommandHandler:
    """由活动触发唤醒下游的 Command Handler。"""

    def __init__(self, gateway: Gateway):
        self._gateway = gateway

    def handle(self) -> None:
        """若下游处于停机状态则触发冷启动，否则不产生任何作用。"""
        self._gateway.wake_downstream()
