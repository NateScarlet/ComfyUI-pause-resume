import logging
from typing import List, Optional

from gateway.shared.signals import (
    ExecutingFalse,
    ExecutingTrue,
    Signal,
    Stall,
    StallWatchStart,
)

logger = logging.getLogger(__name__)

# ComfyUI 在节点执行抛出异常时打印的日志标记（comfy/execution.py 的 execute 异常分支）
FAILURE_MARKER = "!!! Exception during processing !!!"
# ComfyUI 在一次任务正常收尾后打印的日志标记（main.py 的 prompt_worker）
FINISH_MARKER = "Prompt executed in"
# ComfyUI 在收到任务时打印的日志标记（server.py 的 POST /prompt）
START_MARKER = "got prompt"


class ComfyUIExecutionLogWatcher:
    """把下游日志行纯翻译成执行信号，并在异常后维护收尾等待状态。

    下游是否仍在执行只能从日志文本判断：消费任务队列的工作线程是唯一的，
    当它因异常逃逸而死亡时收尾日志永不打印，而进程本身仍然存活。

    本类只持有解析状态（是否等待收尾、有上限的错误摘要、作废标志），
    不注入事件总线、不注入计时器、不产生回调副作用；事件的发布、计时的
    启停与卡死反应链统一由下游客户端的 _handle_signal seam 完成。

    观察者的生命周期与一个下游进程一一对应：进程退出后调用 close 作废，
    此后即使旧进程的读取线程仍在投递管道里的残留日志也不再产生任何信号，
    避免同一次执行异常既被记为崩溃又被记为卡死。
    """

    # 错误摘要最多保留的行数；异常之后的下游输出没有上界，
    # 不设上限会让一个刷屏的下游把内存吃光
    _MAX_SUMMARY_LINES = 1000

    def __init__(self) -> None:
        # 是否已观察到执行异常、仍在等待下游自行收尾
        self._awaiting_finalization = False
        # 异常标记之后的日志行，作为该任务的错误摘要
        self._error_summary: List[str] = []
        self._closed = False

    def feed(self, line: str) -> Optional[Signal]:
        """消费一行下游日志，返回该行翻译出的信号（无语义变化时返回 None）。"""
        if self._closed:
            return None
        if FAILURE_MARKER in line:
            return self._on_failure(line)
        if FINISH_MARKER in line:
            return self._on_finished()
        if START_MARKER in line:
            logger.debug("Detected 'got prompt' in downstream log")
            return ExecutingTrue()
        if self._awaiting_finalization:
            self._append_error_summary(line)
        return None

    def on_stall_timeout(self) -> Optional[Stall]:
        """收尾超时且仍在等待收尾时产出卡死信号并交出错误摘要，否则返回 None。

        计时器由下游客户端持有；本方法只负责翻译器自身的判定，
        到期时若本翻译器已作废或已正常收尾，则不再产出卡死信号。
        """
        if self._closed or not self._awaiting_finalization:
            return None
        self._awaiting_finalization = False
        error_summary = "\n".join(self._error_summary)
        self._error_summary.clear()
        return Stall(summary=error_summary)

    def close(self) -> None:
        """作废本翻译器：清摘要、忽略迟到日志；计时由下游客户端显式取消。"""
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

    def _on_failure(self, line: str) -> Optional[Signal]:
        """观察到执行异常：记录错误摘要，首个异常开启收尾窗口。"""
        if self._awaiting_finalization:
            # 同一次执行的后续异常不重新计时，收尾窗口始终锚定在首个异常
            self._append_error_summary(line)
            return None
        self._awaiting_finalization = True
        self._error_summary = [line]
        return StallWatchStart()

    def _on_finished(self) -> ExecutingFalse:
        """观察到正常收尾：撤销本次的收尾等待，本次执行不算卡死。"""
        logger.debug("Detected 'Prompt executed in' in downstream log")
        self._awaiting_finalization = False
        self._error_summary.clear()
        return ExecutingFalse()

    # #endregion
