from typing import Dict, Any
from gateway.shared.interfaces import TaskQueueReader


class GetQueueQueryHandler:
    """获取网关当前真实队列信息的 Query Handler。"""

    def __init__(self, queue_reader: TaskQueueReader):
        self._queue_reader = queue_reader

    def handle(self) -> Dict[str, Any]:
        """返回网关当前的待处理任务与运行中任务（已格式化为 /queue 兼容的数据包）。"""
        return {
            "queue_running": [t.to_list() for t in self._queue_reader.get_running()],
            "queue_pending": [t.to_list() for t in self._queue_reader.get_pending()],
        }
