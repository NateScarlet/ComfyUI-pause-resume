class GatewayError(Exception):
    """网关项目中的系统级基类异常。"""

    pass


class TaskNotFoundError(GatewayError):
    """请求的任务在队列中未找到时抛出此异常。"""

    pass
