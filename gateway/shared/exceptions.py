class GatewayError(Exception):
    """网关项目中的系统级基类异常。"""

    pass


class JobNotFoundError(GatewayError):
    """请求的任务在队列中未找到时抛出此异常。"""

    pass


class DownstreamError(GatewayError):
    """下游请求失败异常，包含 HTTP 状态码和响应内容。"""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(f"Downstream error {status_code}: {message}")
        self.status_code = status_code
        self.message = message


class DownstreamStartupTimeout(GatewayError):
    """等待下游进程就绪超时。"""

    pass
