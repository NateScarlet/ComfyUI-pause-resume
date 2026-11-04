from gateway.domain.gateway import Gateway


class SetAllowIdleShutdownCommandHandler:
    """更新「空闲时允许下游停机」授权勾选的 Command Handler。"""

    def __init__(self, gateway: Gateway):
        self._gateway = gateway

    def handle(self, allow: bool) -> None:
        """更新授权并持久化用户选择。"""
        self._gateway.set_allow_idle_shutdown(allow)
