import sys
import shlex
import subprocess
import logging
import threading
from typing import Optional, Any

from gateway.shared.interfaces import ProcessManager, EventBus
from gateway.shared.events import ScriptStateChangedEvent
from gateway.shared.models import QueueState

logger = logging.getLogger(__name__)


class ExternalProgramManager(ProcessManager):
    """根据网关队列的运行状态，管理需要运行的外部程序（例如监控、挖矿程序等）。

    外部程序自身没有状态，本类只是把"队列状态 → 应运行的程序命令"的配置映射
    应用到进程上：队列状态变化时终止不再需要的程序并启动新状态的程序。
    目标程序与正在运行的程序是同一条命令时忽略切换，让进程原样继续运行。
    """

    def __init__(
        self,
        idle_path: str,
        busy_path: str,
        pause_path: str,
        event_bus: Optional[EventBus] = None,
    ):
        # 各队列状态下应运行的程序命令（空表示该状态下不运行任何程序）
        self._paths = {
            QueueState.IDLE: idle_path,
            QueueState.BUSY: busy_path,
            QueueState.PAUSED: pause_path,
        }
        self._event_bus = event_bus
        # 同一时刻至多只有一个受管进程，(所属队列状态, 进程) 元组直接表达该不变式
        self._process: Optional[tuple[QueueState, subprocess.Popen[Any]]] = None
        self._last_state: Optional[tuple[QueueState, bool]] = None

    def is_running(self) -> bool:
        """检查是否有由本管理器启动的外部程序正在运行。"""
        return self._process is not None and self._process[1].poll() is None

    def update_state(self, state: QueueState, ever_active: bool) -> None:
        """接收领域层上报的队列状态，调度外部程序的启动/停止。"""
        reported = (state, ever_active)
        if reported == self._last_state:
            return
        self._last_state = reported

        # 规格决策（#8 D1 反向约束）：空闲维持既有门控——下游从未执行过任务时
        # 显存未被占用，无需启动闲置程序；暂停是显式人工意图，不受门控
        if state == QueueState.IDLE and not ever_active:
            return
        self._apply(state)

    def _apply(self, target: QueueState) -> None:
        """应用目标队列状态对应的程序配置：停掉现有程序，启动目标程序。

        目标路径为空表示该状态下不运行任何程序。
        现有正在运行的进程若与本状态要启动的是同一条命令，
        则直接保留该进程继续运行（用户可能把暂停程序配成与闲置程序一样）。
        """
        cmd = self._paths[target]
        carried_over: Optional[subprocess.Popen[Any]] = None
        if self._process is not None:
            state, proc = self._process
            self._process = None
            if proc.poll() is None:
                if cmd and self._paths[state] == cmd:
                    carried_over = proc
                else:
                    self._terminate(proc)

        if carried_over is not None:
            self._process = (target, carried_over)
            return

        if cmd:
            self._start(target)

    def _start(self, state: QueueState) -> None:
        """启动指定队列状态下应运行的外部程序并挂载退出监控线程。"""
        cmd = self._paths[state]
        logger.info(f"🚀 Starting {state.value} program: {cmd}")
        proc = self._run_program(cmd)
        if proc is None:
            return
        self._process = (state, proc)
        threading.Thread(
            target=self._monitor_process,
            args=(proc,),
            daemon=True,
        ).start()

    def _terminate(self, proc: subprocess.Popen[Any]) -> None:
        """终止进程，忽略其已自行退出的情况。"""
        try:
            proc.kill()
        except OSError:
            pass  # 进程已自行退出，无需清理

    def _run_program(self, cmd_str: str) -> Optional[subprocess.Popen[Any]]:
        """在后台静默运行指定的命令字符串，避免弹出 Windows 命令行窗口。"""
        if not cmd_str or not cmd_str.strip():
            return None

        is_win = sys.platform == "win32"
        startupinfo = None
        if is_win:
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = 0

        try:
            args = shlex.split(cmd_str, posix=not is_win)
            if not args:
                return None
            if is_win and args[0].lower().endswith((".bat", ".cmd")):
                args = ["cmd", "/c"] + args
            return subprocess.Popen(args, startupinfo=startupinfo)
        except (OSError, ValueError) as e:
            logger.error(f"Failed to start program '{cmd_str}': {e}")
            return None

    def _monitor_process(self, proc: subprocess.Popen[Any]) -> None:
        """在后台守护线程中等待受控进程退出，若是当前受管进程，则触发事件。"""
        proc.wait()
        if not self._event_bus:
            return
        if self._process is not None and self._process[1] is proc:
            state, _ = self._process
            logger.info(
                f"External program {state.value} terminated with exit code {proc.poll()}"
            )
            self._event_bus.publish(ScriptStateChangedEvent())

    def cleanup(self) -> None:
        """终止由本管理器启动的外部进程。"""
        if self._process is None:
            return
        _, proc = self._process
        self._process = None
        self._terminate(proc)
