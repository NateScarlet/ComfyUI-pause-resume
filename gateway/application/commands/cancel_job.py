from gateway.shared.interfaces import (
    JobQueueReader,
    JobQueueWriter,
    DownstreamClient,
    EventBus,
)
from gateway.shared.models import JobStatus
from gateway.shared.events import QueueModifiedEvent
from gateway.shared.exceptions import JobNotFoundError
from gateway.shared.responses import CancelJobResponse


class CancelJobCommandHandler:
    """取消指定任务（作业）的 Command Handler。"""

    def __init__(
        self,
        queue_reader: JobQueueReader,
        queue_writer: JobQueueWriter,
        downstream_client: DownstreamClient,
        event_bus: EventBus,
    ):
        self._queue_reader = queue_reader
        self._queue_writer = queue_writer
        self._downstream_client = downstream_client
        self._event_bus = event_bus

    async def handle(self, job_id: str) -> CancelJobResponse:
        """取消待处理（pending）或执行中（running）的作业。

        如果成功取消，返回 CancelJobResponse(cancelled=True)；
        若作业已经是终态（已完成/失败/已取消），返回 CancelJobResponse(cancelled=False)；
        如果未找到此作业，则抛出 JobNotFoundError。
        """
        # 1. 查找任务及其状态
        res = self._queue_reader.get(job_id)
        if res is None:
            raise JobNotFoundError(f"Job {job_id} not found.")

        target_status, _ = res

        # 2. 如果任务已经是终态，则返回 cancelled=False 表示未做更改
        if target_status in (
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
        ):
            return CancelJobResponse(cancelled=False)

        # 3. 更新任务状态为 CANCELLED
        self._queue_writer.update_status(JobStatus.CANCELLED, prompt_id=job_id)

        # 4. 如果该任务正在运行，还要向 ComfyUI 发送物理中断信号
        if target_status == JobStatus.RUNNING:
            await self._downstream_client.interrupt(job_id)

        # 5. 广播队列修改事件，驱动网关刷新及状态推送
        self._event_bus.publish(QueueModifiedEvent())

        return CancelJobResponse(cancelled=True)
