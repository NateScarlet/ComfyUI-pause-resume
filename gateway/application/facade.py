from typing import Optional, Any
from gateway.domain.gateway import Gateway
from gateway.shared.interfaces import (
    JobQueueReader,
    JobQueueWriter,
    DownstreamClient,
    EventBus,
)
from .commands.add_job import AddJobCommandHandler
from .commands.pause_resume import PauseQueueCommandHandler, ResumeQueueCommandHandler
from .commands.modify_queue import ModifyQueueCommandHandler
from .commands.cancel_job import CancelJobCommandHandler
from .queries.get_queue import GetQueueQueryHandler
from .queries.get_jobs import GetJobsQueryHandler
from .queries.get_job_detail import GetJobDetailQueryHandler
from .queries.get_job_count import GetJobCountQueryHandler
from .queries.get_state import GetStateQueryHandler


class AppFacade:
    """应用层 Command 与 Query Handlers 的门面集成类 (Facade)。

    用于将原本零散的命令与查询 Handler 聚合为一个统一的入口对象，极大简化表示层的依赖注入和维护复杂度。
    """

    def __init__(
        self,
        add_job: AddJobCommandHandler,
        pause_queue: PauseQueueCommandHandler,
        resume_queue: ResumeQueueCommandHandler,
        modify_queue: ModifyQueueCommandHandler,
        cancel_job: CancelJobCommandHandler,
        get_queue: GetQueueQueryHandler,
        get_jobs: GetJobsQueryHandler,
        get_job_detail: GetJobDetailQueryHandler,
        get_job_count: GetJobCountQueryHandler,
        get_state: GetStateQueryHandler,
    ):
        self.add_job = add_job
        self.pause_queue = pause_queue
        self.resume_queue = resume_queue
        self.modify_queue = modify_queue
        self.cancel_job = cancel_job
        self.get_queue = get_queue
        self.get_jobs = get_jobs
        self.get_job_detail = get_job_detail
        self.get_job_count = get_job_count
        self.get_state = get_state
        self._outputs_importer: Optional[Any] = None

    @classmethod
    def create(
        cls,
        gateway: Gateway,
        queue_reader: JobQueueReader,
        queue_writer: JobQueueWriter,
        downstream_client: DownstreamClient,
        event_bus: EventBus,
    ) -> "AppFacade":
        """快速实例化门面，在内部组装所有的命令与查询处理器，极大减轻启动根的代码量。"""
        from .sync import JobDownstreamSyncer

        facade = cls(
            add_job=AddJobCommandHandler(queue_writer, event_bus),
            pause_queue=PauseQueueCommandHandler(gateway),
            resume_queue=ResumeQueueCommandHandler(gateway),
            modify_queue=ModifyQueueCommandHandler(queue_writer, event_bus),
            cancel_job=CancelJobCommandHandler(
                queue_reader, queue_writer, downstream_client, event_bus
            ),
            get_queue=GetQueueQueryHandler(queue_reader),
            get_jobs=GetJobsQueryHandler(queue_reader),
            get_job_detail=GetJobDetailQueryHandler(queue_reader),
            get_job_count=GetJobCountQueryHandler(queue_reader),
            get_state=GetStateQueryHandler(gateway),
        )

        # 实例化后台同步服务，订阅事件以从下游同步省略信息及 assets
        facade._outputs_importer = JobDownstreamSyncer(
            queue_reader, queue_writer, downstream_client, event_bus
        )
        return facade
