from gateway.domain.gateway import Gateway
from gateway.shared.models import GatewayState


class GetStateQueryHandler:
    """获取当前网关状态的 Query Handler。"""

    def __init__(self, gateway: Gateway):
        self._gateway = gateway

    def handle(self) -> GatewayState:
        """返回网关当前业务状态的快照。"""
        return GatewayState(
            paused=self._gateway.paused,
            allow_idle_shutdown=self._gateway.allow_idle_shutdown,
            downstream_state=self._gateway.downstream_state,
        )
