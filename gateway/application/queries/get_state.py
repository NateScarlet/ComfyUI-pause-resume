from gateway.domain.gateway import Gateway


class GetStateQueryHandler:
    """获取当前网关状态的 Query Handler。"""

    def __init__(self, gateway: Gateway):
        self._gateway = gateway

    def handle(self) -> bool:
        """返回网关当前是否处于暂停状态。"""
        return self._gateway.paused
