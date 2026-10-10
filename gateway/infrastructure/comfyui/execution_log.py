import logging
from typing import Callable, List, Optional

from gateway.shared.interfaces import EventBus, Timer
from gateway.shared.events import DownstreamExecutingChangedEvent

logger = logging.getLogger(__name__)

# ComfyUI 在节点执行抛出异常时打印的日志标记（comfy/execution.py 的 execute 异常分支）
FAILURE_MARKER = "!!! Exception during processing !!!"
# ComfyUI 在一次任务正常收尾后打印的日志标记（main.py 的 prompt_worker）
FINISH_MARKER = "Prompt executed in"
# ComfyUI 在收到任务时打印的日志标记（server.py 的 POST /prompt）
START_MARKER = "got prompt"


class ComfyUIExecutionLogWatcher:
    """把下游日志行翻译成执行状态事件，并在异常后收尾超时时判定下游卡死。

    下游是否仍在执行只能从日志文本判断：消费任务队列的工作线程是唯一的，
    当它因异常逃逸而死亡时收尾日志永不打印，而进程本身仍然存活。

    因此这里在观察到执行异常后开启一次性收尾计时：若超时内仍未出现收尾日志，
    说明下游已失去执行能力（卡死），把异常标记之后的日志作为错误摘要交给调用方。

    计时器只在事件循环线程上启停，调用方必须保证 feed 在该线程被调用。

    观察者的生命周期与一个下游进程一一对应：进程退出后调用 close 作废，
    此后即使旧进程的读取线程仍在投递管道里的残留日志也不再产生任何判定，
    避免同一次执行异常既被记为崩溃又被记为卡死。
    """

    # 错误摘要最多保留的行数；异常之后的下游输出没有上界，
    # 不设上限会让一个刷屏的下游把内存吃光
    _MAX_SUMMARY_LINES = 1000

    def __init__(
        self,
        event_bus: EventBus,
        timer: Timer,
        stall_timeout_sec: float,
        on_stall: Callable[[str], None],
    ) -> None:
        self._event_bus = event_bus
        self._timer = timer
        self._stall_timeout_sec = stall_timeout_sec
        self._on_stall = on_stall
        # 是否已观察到执行异常、仍在等待下游自行收尾
        self._awaiting_finalization = False
        # 异常标记之后的日志行，作为该任务的错误摘要
        self._error_summary: List[str] = []
        self._cancel_stall_timer: Optional[Callable[[], None]] = None
        self._closed = False

    def feed(self, line: str) -> None:
        """消费一行下游日志，更新执行状态并维护卡死判定。"""
        if self._closed:
            return
        if FAILURE_MARKER in line:
            self._on_failure(line)
        elif FINISH_MARKER in line:
            self._on_finished()
        elif START_MARKER in line:
            logger.debug("Detected 'got prompt' in downstream log")
            self._event_bus.publish(DownstreamExecutingChangedEvent(executing=True))
        elif self._awaiting_finalization:
            self._append_error_summary(line)

    def close(self) -> None:
        """作废本观察者：撤销挂起的卡死判定，此后不再接受任何日志行。"""
        if self._cancel_stall_timer:
            self._cancel_stall_timer()
            self._cancel_stall_timer = None
        self._awaiting_finalization = False
        self._error_summary.clear()
        self._closed = True

    def _append_error_summary(self, line: str) -> None:
        """把日志行并入错误摘要；超出上限时用最后一行位置记录截断。"""
        if len(self._error_summary) < self._MAX_SUMMARY_LINES:
            self._error_summary.append(line)
            return
        self._error_summary[-1] = (
            f"... error summary truncated after {self._MAX_SUMMARY_LINES} lines"
        )

    # #region 日志标记处理

    def _on_failure(self, line: str) -> None:
        """观察到执行异常：记录错误摘要，并在首个异常处开启收尾计时。"""
        if self._awaiting_finalization:
            # 同一次执行的后续异常不重新计时，收尾窗口始终锚定在首个异常
            self._append_error_summary(line)
            return
        self._awaiting_finalization = True
        self._error_summary = [line]
        self._cancel_stall_timer = self._timer.start_timeout(
            self._stall_timeout_sec, self._on_stall_timeout
        )

    def _on_finished(self) -> None:
        """观察到正常收尾：撤销挂起的卡死判定，本次执行不算卡死。"""
        logger.debug("Detected 'Prompt executed in' in downstream log")
        if self._cancel_stall_timer:
            self._cancel_stall_timer()
            self._cancel_stall_timer = None
        self._awaiting_finalization = False
        self._error_summary.clear()
        self._event_bus.publish(DownstreamExecutingChangedEvent(executing=False))

    def _on_stall_timeout(self) -> None:
        """收尾超时仍未观察到正常收尾：判定下游卡死并交出错误摘要。

        计时器只在 _awaiting_finalization 为真时挂起，且撤销判定时必定同步取消，
        因此能走到这里的必然是仍在等待收尾的那次执行。
        """
        self._cancel_stall_timer = None
        self._awaiting_finalization = False
        error_summary = "\n".join(self._error_summary)
        self._error_summary.clear()
        logger.error(
            "Downstream raised an execution error but did not finish within %s seconds",
            self._stall_timeout_sec,
        )
        self._on_stall(error_summary)

    # #endregion
