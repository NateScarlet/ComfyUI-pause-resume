import os
import sys
import socket
import threading
import subprocess
import asyncio
import logging
import aiohttp
from typing import Optional, Any, List, Dict, cast

from gateway.config import BASE_DIR, GatewayConfig
from gateway.shared.utils import raw_json_dumps
from gateway.infrastructure.comfyui.execution_log import ComfyUIExecutionLogWatcher
from gateway.shared.interfaces import (
    DownstreamClient,
    EventBus,
    ProcessManager,
    Timer,
)
from gateway.shared.exceptions import DownstreamError, DownstreamStartupTimeout
from gateway.shared.events import (
    DownstreamExecutingChangedEvent,
    DownstreamReadyChangedEvent,
    DownstreamCrashedEvent,
)

logger = logging.getLogger(__name__)


class ComfyUIDownstreamClient(DownstreamClient):
    """下游 ComfyUI 进程管理器及网络请求基础设施客户端。

    实现 DownstreamClient 接口。
    负责拉起进程、监听日志、转发网络请求、上报事件到事件总线。
    """

    def __init__(
        self,
        config: GatewayConfig,
        event_bus: EventBus,
        process_manager: ProcessManager,
        timer: Timer,
        loop: asyncio.AbstractEventLoop,
    ):
        self.config = config
        self.event_bus = event_bus
        self.process_manager = process_manager
        self._timer = timer
        self._loop = loop

        # 运行时状态变量
        self._downstream_port: int = 0
        self.exiting: bool = False

        self._process: Optional[subprocess.Popen[str]] = None
        self._is_restarting: bool = False
        self._downstream_ready: bool = False

        # 每个下游进程配一个日志观察者，随进程创建与作废；
        # 日志读取在后台线程上进行，而卡死判定用的定时器只能在事件循环线程启停，
        # 因此由本客户端把日志行跨线程投递到事件循环上再喂给观察者
        self._log_watcher: Optional[ComfyUIExecutionLogWatcher] = None

    @property
    def downstream_ready(self) -> bool:
        """下游服务是否已就绪（接口实现）。"""
        return self._downstream_ready

    @property
    def downstream_port(self) -> int:
        """下游服务监听的物理端口（接口实现）。"""
        return self._downstream_port

    def restart(self) -> None:
        """重启下游服务（接口实现）。"""
        if self._loop.is_running():
            self._loop.create_task(self.restart_downstream())

    async def send_prompt(self, prompt_id: str, body: Dict[str, Any]) -> None:
        """向下游发送任务数据（接口实现）。如果发送失败，抛出 DownstreamError。"""
        url = f"http://127.0.0.1:{self._downstream_port}/prompt"
        body_str = raw_json_dumps(body)
        headers = {"Content-Type": "application/json"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=body_str, headers=headers) as resp:
                    if resp.status == 200:
                        return
                    else:
                        txt = await resp.text()
                        raise DownstreamError(resp.status, txt)
        except DownstreamError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            raise DownstreamError(500, f"Network error: {str(e)}")

    async def get_jobs(self, query_params: Dict[str, str]) -> List[Dict[str, Any]]:
        """从下游 ComfyUI 原生 API 获取历史作业列表（接口实现）。"""
        downstream_jobs_url = f"http://127.0.0.1:{self._downstream_port}/api/jobs"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    downstream_jobs_url, params=query_params
                ) as resp:
                    if resp.status == 200:
                        resp_json = await resp.json()
                        if isinstance(resp_json, dict):
                            resp_dict = cast(Dict[str, Any], resp_json)
                            raw_jobs = resp_dict.get("jobs", [])
                            if isinstance(raw_jobs, list):
                                return cast(List[Dict[str, Any]], raw_jobs)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.error(f"Error fetching jobs from downstream: {e}")
        return []

    async def interrupt(self, prompt_id: Optional[str] = None) -> None:
        """向物理 ComfyUI 服务发送中断执行信号（接口实现）。"""
        url = f"http://127.0.0.1:{self._downstream_port}/interrupt"
        body = {"prompt_id": prompt_id} if prompt_id else {}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=body) as resp:
                    if resp.status != 200:
                        logger.warning(
                            f"Failed to send interrupt to downstream: status={resp.status}"
                        )
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.error(f"Network error sending interrupt to downstream: {e}")

    def start_downstream(self) -> None:
        """在系统空闲端口上拉起下游进程。"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            self._downstream_port = s.getsockname()[1]

        python_exe = sys.executable
        cmd = [
            python_exe,
            "-s",
            "ComfyUI/main.py",
            "--port",
            str(self._downstream_port),
        ]
        if self.config.downstream_args:
            cmd.extend(self.config.downstream_args)

        logger.info(f"🚀 Starting downstream ComfyUI on port {self._downstream_port}")
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUTF8"] = "1"
        self._process = subprocess.Popen(
            cmd,
            cwd=BASE_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            encoding="utf-8",
            errors="replace",
            env=env,
        )
        watcher = self._create_log_watcher()

        def log_reader(pipe: Any, is_stderr: bool) -> None:
            for line in iter(pipe.readline, ""):
                line = line.rstrip()
                if not line:
                    continue
                if is_stderr:
                    print(f"[{self._downstream_port}] STDERR: {line}", file=sys.stderr)
                else:
                    print(f"[{self._downstream_port}] STDOUT: {line}")

                self._feed_log_line(watcher, line)

        threading.Thread(
            target=log_reader, args=(self._process.stdout, False), daemon=True
        ).start()
        threading.Thread(
            target=log_reader, args=(self._process.stderr, True), daemon=True
        ).start()

    def _create_log_watcher(self) -> ComfyUIExecutionLogWatcher:
        """为新拉起的下游进程创建执行日志观察者。"""
        self._discard_log_watcher()
        self._log_watcher = ComfyUIExecutionLogWatcher(
            event_bus=self.event_bus,
            timer=self._timer,
            stall_timeout_sec=self.config.stall_timeout_sec,
            on_stall=self._on_downstream_stall,
        )
        return self._log_watcher

    def _discard_log_watcher(self) -> None:
        """作废当前下游进程的日志观察者（进程生命周期结束时调用）。"""
        if self._log_watcher is not None:
            self._log_watcher.close()
            self._log_watcher = None

    def _feed_log_line(self, watcher: ComfyUIExecutionLogWatcher, line: str) -> None:
        """把一行下游日志转交给产生它的那个观察者。

        读取日志的是后台线程，而卡死判定用的定时器只能在事件循环线程启停，
        因此统一跨线程投递到事件循环上处理，保持观察者的状态只被单线程触碰。
        """
        self._loop.call_soon_threadsafe(watcher.feed, line)

    def _on_downstream_stall(self, error_summary: str) -> None:
        """下游抛出执行异常后迟迟不收尾：判定卡死并按崩溃语义处置。

        卡死时进程仍然存活，崩溃监控（依赖进程退出）察觉不到；而消费队列的工作
        线程已死，不重启下游就会永久失去执行能力。因此这里复用崩溃事件的领域
        处理路径（重新入列 + 崩溃降级），并立即拉起新进程。
        """
        logger.error("❌ Downstream stalled after an execution error, restarting...")
        # 卡死被检测到的瞬间立即翻转就绪状态，让代理层从空窗期一开始就让
        # API 请求进入等待，而不是把请求转发到已失去执行能力的下游上
        self._mark_downstream_unready()
        self.event_bus.publish(DownstreamCrashedEvent(error_summary=error_summary))
        self.restart()

    async def wait_downstream_ready(self) -> None:
        """以轮询 GET 的形式等待下游进程就绪。

        每轮循环动态构建 URL，以应对重启期间 _downstream_port 变化：
        老 wait 会自动追随新端口，最终在新进程就绪后返回。
        """
        logger.info(
            f"⌛ Waiting for downstream service (port {self._downstream_port})..."
        )
        async with aiohttp.ClientSession() as session:
            for _ in range(300):
                if self.exiting:
                    return
                # 每次循环动态构建 URL，应对重启期间 port 变化
                url = f"http://127.0.0.1:{self._downstream_port}"
                try:
                    async with session.get(
                        url, timeout=aiohttp.ClientTimeout(total=3)
                    ) as resp:
                        if resp.status == 200:
                            logger.info("✅ Downstream service ready")
                            self._downstream_ready = True
                            self.event_bus.publish(
                                DownstreamReadyChangedEvent(ready=True)
                            )
                            return
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    pass
                await asyncio.sleep(1)
        logger.error("❌ Downstream wait timeout")
        raise DownstreamStartupTimeout("Downstream wait timeout")

    def _mark_downstream_unready(self) -> None:
        """将下游置为未就绪；仅在就绪→未就绪的真实状态迁移时发布事件。

        事件语义保持为状态迁移通知：已处于未就绪时再次重启（如崩溃流程中
        检测阶段已翻转过）不再重复广播，避免订阅方做冗余的刷新与状态发布。
        """
        if not self._downstream_ready:
            return
        self._downstream_ready = False
        self.event_bus.publish(DownstreamReadyChangedEvent(ready=False))

    async def _terminate_current_process(self) -> None:
        """终止当前持有的下游进程（若有）并等待其退出。"""
        if not self._process:
            return
        # 一旦决定终止本进程，其日志判定即告作废
        self._discard_log_watcher()
        try:
            self._process.kill()
            for _ in range(100):
                if self._process.poll() is not None:
                    break
                await asyncio.sleep(0.1)
        except (ProcessLookupError, OSError):
            pass
        self._process = None

    async def restart_downstream(self) -> None:
        """优雅关闭下游老进程并拉起新的下游，直到就绪或网关退出。

        就绪等待超时视为本次尝试失败：终止半启动的候选进程后按崩溃重启延迟
        重试拉起，形成自愈闭环。启动超时不向外传播，避免依赖重启结果的
        监控协程与优雅重启任务被异常杀死后永久失去自愈能力。
        """
        if self._is_restarting:
            return
        self._is_restarting = True
        self._mark_downstream_unready()
        self.event_bus.publish(DownstreamExecutingChangedEvent(executing=False))
        try:
            while not self.exiting:
                await self._terminate_current_process()
                self.start_downstream()
                try:
                    await self.wait_downstream_ready()
                    return
                except DownstreamStartupTimeout:
                    logger.error(
                        f"❌ Downstream failed to become ready, retrying in "
                        f"{self.config.restart_delay_sec} seconds..."
                    )
                    await asyncio.sleep(self.config.restart_delay_sec)
        finally:
            self._is_restarting = False

    async def monitor_downstream(self) -> None:
        """下游非预期崩溃监控。"""
        while not self.exiting:
            if self._process and not self._is_restarting:
                await asyncio.get_event_loop().run_in_executor(None, self._process.wait)
                if self.exiting or self._is_restarting:
                    continue
                exit_code = self._process.poll()
                # 进程已经退出，其日志判定一并作废：否则抛出过执行异常的进程
                # 会被同时记为崩溃与卡死，让同一次失败在降级阈值上被数两次
                self._discard_log_watcher()
                logger.error(
                    f"❌ Downstream process exited unexpectedly with code {exit_code}. "
                    f"Restarting in {self.config.restart_delay_sec} seconds..."
                )
                # 崩溃被检测到的瞬间立即翻转就绪状态，让代理层从空窗期一开始就让
                # API 请求进入等待，而不是把请求转发到已死进程上立即返回 502
                self._mark_downstream_unready()
                self.event_bus.publish(DownstreamCrashedEvent())
                await asyncio.sleep(self.config.restart_delay_sec)
                await self.restart_downstream()
            else:
                await asyncio.sleep(1)

    async def get_queue(self) -> Optional[Dict[str, Any]]:
        """从下游获取原生队列数据（接口实现）。"""
        if not self._downstream_ready:
            return None
        url = f"http://127.0.0.1:{self._downstream_port}/queue"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        res = await resp.json()
                        if isinstance(res, dict):
                            return cast(Dict[str, Any], res)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.debug(f"Error fetching /queue from downstream: {e}")
        return None

    async def get_history(
        self, max_items: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """从下游获取原生历史数据（接口实现）。"""
        if not self._downstream_ready:
            return None
        url = f"http://127.0.0.1:{self._downstream_port}/history"
        params: Dict[str, str] = {}
        if max_items is not None:
            params["max_items"] = str(max_items)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as resp:
                    if resp.status == 200:
                        res = await resp.json()
                        if isinstance(res, dict):
                            return cast(Dict[str, Any], res)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.debug(f"Error fetching /history from downstream: {e}")
        return None

    async def shutdown(self) -> None:
        """停止下游子程序。"""
        self.exiting = True
        self._discard_log_watcher()
        if self._process:
            try:
                logger.info("Cleaning up downstream process...")
                self._process.kill()
            except (ProcessLookupError, OSError):
                pass
