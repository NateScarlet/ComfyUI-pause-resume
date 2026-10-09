from gateway.domain.gateway import Gateway


class ReportActivityCommandHandler:
    """上报一次活动的 Command Handler。

    活动是空闲判定的第三个输入：领域层据此推迟空闲窗口起点，从而把下游重启
    推迟到用户停止使用网关之后。活动来源由接口层翻译，本层只感知活动本身。
    """

    def __init__(self, gateway: Gateway):
        self._gateway = gateway

    def handle(self) -> None:
        """记录一次活动。"""
        self._gateway.report_activity()
