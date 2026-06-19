from gateway.domain.gateway import Gateway
from gateway.shared.interfaces import TaskQueueReader, TaskQueueWriter, StateRepository
from gateway.application.services.downstream import DownstreamAppService
from .commands.add_task import AddTaskCommandHandler
from .commands.pause_resume import PauseQueueCommandHandler, ResumeQueueCommandHandler
from .commands.modify_queue import ModifyQueueCommandHandler
from .queries.get_queue import GetQueueQueryHandler
from .queries.get_jobs import GetJobsQueryHandler, GetJobDetailQueryHandler
from .queries.get_state import GetStateQueryHandler


class AppFacade:
    """应用层 Command 与 Query Handlers 的门面集成类 (Facade)。

    用于将原本零散的命令与查询 Handler 聚合为一个统一的入口对象，极大简化表示层的依赖注入和维护复杂度。
    """

    def __init__(
        self,
        add_task: AddTaskCommandHandler,
        pause_queue: PauseQueueCommandHandler,
        resume_queue: ResumeQueueCommandHandler,
        modify_queue: ModifyQueueCommandHandler,
        get_queue: GetQueueQueryHandler,
        get_jobs: GetJobsQueryHandler,
        get_job_detail: GetJobDetailQueryHandler,
        get_state: GetStateQueryHandler,
    ):
        self.add_task = add_task
        self.pause_queue = pause_queue
        self.resume_queue = resume_queue
        self.modify_queue = modify_queue
        self.get_queue = get_queue
        self.get_jobs = get_jobs
        self.get_job_detail = get_job_detail
        self.get_state = get_state

    @classmethod
    def create(
        cls,
        gateway: Gateway,
        queue_reader: TaskQueueReader,
        queue_writer: TaskQueueWriter,
        state_repo: StateRepository,
        downstream_service: DownstreamAppService,
    ) -> "AppFacade":
        """快速实例化门面，在内部组装所有的命令与查询处理器，极大减轻启动根的代码量。"""
        return cls(
            add_task=AddTaskCommandHandler(queue_writer, downstream_service),
            pause_queue=PauseQueueCommandHandler(
                gateway, state_repo, downstream_service
            ),
            resume_queue=ResumeQueueCommandHandler(
                gateway, state_repo, downstream_service
            ),
            modify_queue=ModifyQueueCommandHandler(queue_writer, downstream_service),
            get_queue=GetQueueQueryHandler(queue_reader),
            get_jobs=GetJobsQueryHandler(queue_reader, downstream_service),
            get_job_detail=GetJobDetailQueryHandler(queue_reader, downstream_service),
            get_state=GetStateQueryHandler(gateway),
        )
