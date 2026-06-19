import os
import sys
import socket
import time
import json
import datetime
import threading
import subprocess
import asyncio
import logging
import aiohttp
from typing import Optional, Any, List, Dict, Callable, cast

from gateway.config import BASE_DIR, GatewayConfig
from gateway.shared.models import Task
from gateway.shared.utils import raw_json_dumps
from gateway.shared.interfaces import (
    TaskQueueReader,
    TaskQueueWriter,
    ProcessManager,
    StateRepository,
)
from gateway.domain.gateway import Gateway

logger = logging.getLogger(__name__)


class DownstreamAppService:
    """下游 ComfyUI 进程管理器及网络请求应用协调服务。

    作为应用编排层，本身不承载业务逻辑判定，只负责拉起进程、监听日志、转发网络请求，并根据聚合根决策操作基础设施。
    """

    def __init__(
        self,
        gateway: Gateway,
        config: GatewayConfig,
        queue_reader: TaskQueueReader,
        queue_writer: TaskQueueWriter,
        state_repo: StateRepository,
        process_manager: ProcessManager,
    ):
        self.gateway = gateway
        self.config = config
        self.queue_reader = queue_reader
        self.queue_writer = queue_writer
        self.state_repo = state_repo
        self.process_manager = process_manager

        # 运行时状态变量
        self.downstream_port: int = 0
        self.exiting: bool = False
        self.loop: Optional[asyncio.AbstractEventLoop] = None

        self._process: Optional[subprocess.Popen[str]] = None
        self._is_restarting: bool = False
        self._preventing_sleep: bool = False
        self._idle_start_time: Optional[float] = None
        self._idle_timeout_task: Optional[asyncio.Task[None]] = None
        self._dispatching: bool = False

        self.queue_lock = threading.Lock()

        # 外部订阅状态变更事件的回调（用于解耦表示层的 SSE 和 WS 广播）
        self._ws_broadcast_callbacks: List[Callable[[], None]] = []
        self._sse_broadcast_callbacks: List[Callable[[bool], None]] = []

    def register_ws_callback(self, callback: Callable[[], None]) -> None:
        """注册前端 WebSocket 状态广播回调。"""
        self._ws_broadcast_callbacks.append(callback)

    def register_sse_callback(self, callback: Callable[[bool], None]) -> None:
        """注册 SSE 状态广播回调。"""
        self._sse_broadcast_callbacks.append(callback)

    def notify_status_changed(self) -> None:
        """通知执行队列数量等状态变更。"""
        for cb in self._ws_broadcast_callbacks:
            try:
                cb()
            except Exception:
                pass

    def notify_state_changed(self, paused: bool) -> None:
        """通知暂停/恢复状态变更。"""
        for cb in self._sse_broadcast_callbacks:
            try:
                cb(paused)
            except Exception:
                pass

    def sync_state_to_infrastructure(self) -> None:
        """根据聚合根当前的业务决定，同步阻止系统休眠、空闲重启超时与外挂脚本状态。"""
        was_idle = self._idle_start_time is not None and not self._preventing_sleep

        has_pending = self.queue_reader.get_pending_count(limit=1) > 0
        scripts_running = self.process_manager.is_running()

        should_prevent = self.gateway.determine_sleep_prevention(
            has_pending, scripts_running
        )
        is_busy = self.gateway.determine_busy_state(has_pending)

        # 更新空闲/繁忙外挂脚本运行状态
        self.process_manager.update_state(is_busy, self.gateway.ever_active)

        if should_prevent:
            self._idle_start_time = None
            if not self._preventing_sleep:
                logger.info("☕ Preventing system sleep")
                from gateway.infrastructure.system.power import PowerManagement

                PowerManagement.prevent_sleep()
                self._preventing_sleep = True
        else:
            if self._idle_start_time is None:
                self._idle_start_time = time.time()

            if self._preventing_sleep:
                logger.info("💤 Allowing system sleep")
                from gateway.infrastructure.system.power import PowerManagement

                PowerManagement.allow_sleep()
                self._preventing_sleep = False

        is_idle = self._idle_start_time is not None and not self._preventing_sleep
        if not was_idle and is_idle:
            self._on_idle_entered()
        elif was_idle and not is_idle:
            self._on_idle_exited()

    def _on_idle_entered(self) -> None:
        """进入业务空闲状态的后续调度行为。"""
        if self.gateway.restart_after_idle_on_pause:
            logger.info(
                "🔄 Pause-and-restart: system idle, restarting downstream immediately..."
            )
            self.gateway.restart_after_idle_on_pause = False
            if self.loop is not None and self.loop.is_running():
                self.loop.create_task(self.restart_downstream())
                self.notify_state_changed(self.gateway.paused)
            return

        if self.gateway.ever_active and self.config.idle_restart_timeout > 0:
            if self.loop is not None and self.loop.is_running():
                self._idle_timeout_task = self.loop.create_task(
                    self._idle_timeout_wait()
                )

    def _on_idle_exited(self) -> None:
        """退出业务空闲状态时取消超时计时。"""
        if self._idle_timeout_task:
            self._idle_timeout_task.cancel()
            self._idle_timeout_task = None

    async def _idle_timeout_wait(self) -> None:
        idle_start = self._idle_start_time
        timeout = self.config.idle_restart_timeout
        try:
            await asyncio.sleep(timeout)
        except asyncio.CancelledError:
            return
        if self._idle_start_time == idle_start and not self._preventing_sleep:
            logger.info(
                f"🕒 Idle timeout ({timeout}s), restarting downstream to free resources..."
            )
            await self.restart_downstream()

    def try_dispatch(self) -> None:
        """线程安全地触发一次任务派发尝试。"""
        loop = self.loop
        if loop is not None and loop.is_running():

            def run() -> None:
                loop.create_task(self._try_send_task())

            loop.call_soon_threadsafe(run)

    async def _try_send_task(self) -> None:
        """执行派发任务的具体副作用。"""
        if self._dispatching or self.exiting:
            return
        self._dispatching = True
        try:
            pending_count = self.queue_reader.get_pending_count()
            skip = self.gateway.calculate_dispatch_skip(pending_count)
            if skip is None:
                return

            with self.queue_lock:
                task = self.queue_writer.pop_task(skip)

            if task is None:
                return

            extra_data = json.loads(task.extra_data)
            body: dict[str, Any] = {
                "prompt": task.prompt,
                "prompt_id": task.prompt_id,
                "extra_data": task.extra_data,
            }
            if extra_data.get("client_id"):
                body["client_id"] = extra_data["client_id"]

            url = f"http://127.0.0.1:{self.downstream_port}/prompt"
            try:
                body_str = raw_json_dumps(body)
                headers = {"Content-Type": "application/json"}
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        url, data=body_str, headers=headers
                    ) as resp:
                        if resp.status == 200:
                            logger.info(
                                f"📤 Sent workflow {task.prompt_id} to downstream"
                            )
                            self.gateway.on_dispatch_success()
                            self.notify_status_changed()
                        else:
                            txt = await resp.text()
                            logger.error(
                                f"Failed to send workflow {task.prompt_id}: {resp.status} - {txt}"
                            )
                            is_permanent = 400 <= resp.status <= 500
                            should_requeue = self.gateway.on_dispatch_failed(
                                is_permanent
                            )

                            if not should_requeue:
                                with self.queue_lock:
                                    self.queue_writer.clear_running()
                                try:
                                    self._save_failed_workflow(
                                        task, txt, body, extra_data, resp.status
                                    )
                                except Exception as save_err:
                                    logger.error(
                                        f"Failed to save failed workflow details: {save_err}"
                                    )
                            else:
                                with self.queue_lock:
                                    self.queue_writer.requeue_running()
                            self.notify_status_changed()
            except Exception as e:
                logger.error(f"Error sending workflow: {e}")
                should_requeue = self.gateway.on_dispatch_failed(is_permanent=False)
                if should_requeue:
                    with self.queue_lock:
                        self.queue_writer.requeue_running()
                self.notify_status_changed()
        finally:
            self._dispatching = False

    def _save_failed_workflow(
        self,
        task: Task,
        error_msg: str,
        body: Dict[str, Any],
        extra_data: Dict[str, Any],
        status_code: int,
    ) -> None:
        """将失败的工作流保存至网关数据目录的 failed_workflows 子目录下备份，避免死循环。"""
        date_str = datetime.date.today().isoformat()
        dir_name = f"{date_str}-{status_code}-{task.prompt_id}"
        failed_dir = os.path.join(self.config.data_dir, "failed_workflows", dir_name)
        os.makedirs(failed_dir, exist_ok=True)

        with open(os.path.join(failed_dir, "error.txt"), "w", encoding="utf-8") as f:
            f.write(error_msg)

        with open(os.path.join(failed_dir, "request.json"), "w", encoding="utf-8") as f:
            json.dump(body, f, ensure_ascii=False, indent=2)

        workflow: Optional[Dict[str, Any]] = None
        extra_pnginfo = extra_data.get("extra_pnginfo")
        if isinstance(extra_pnginfo, dict):
            extra_pnginfo_dict = cast(Dict[str, Any], extra_pnginfo)
            workflow = cast(
                Optional[Dict[str, Any]], extra_pnginfo_dict.get("workflow")
            )
        if not workflow:
            workflow = cast(Optional[Dict[str, Any]], extra_data.get("workflow"))

        if workflow:
            with open(
                os.path.join(failed_dir, "workflow.json"), "w", encoding="utf-8"
            ) as f:
                json.dump(workflow, f, ensure_ascii=False, indent=2)

        rel_failed_dir = os.path.relpath(failed_dir, BASE_DIR).replace(os.sep, "/")
        logger.info(f"💾 Failed workflow {task.prompt_id} saved to {rel_failed_dir}")

    def start_downstream(self) -> None:
        """在系统空闲端口上拉起下游进程。"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            self.downstream_port = s.getsockname()[1]

        # 启动时重入之前崩溃可能残留的正在运行任务
        self.queue_writer.requeue_running()

        python_exe = sys.executable
        cmd = [python_exe, "-s", "ComfyUI/main.py", "--port", str(self.downstream_port)]
        if self.config.downstream_args:
            cmd.extend(self.config.downstream_args)

        logger.info(f"🚀 Starting downstream ComfyUI on port {self.downstream_port}")
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

        def log_reader(pipe: Any, is_stderr: bool) -> None:
            for line in iter(pipe.readline, ""):
                line = line.rstrip()
                if not line:
                    continue
                if is_stderr:
                    print(f"[{self.downstream_port}] STDERR: {line}", file=sys.stderr)
                else:
                    print(f"[{self.downstream_port}] STDOUT: {line}")

                # 仅仅传递日志的事件通知给聚合根
                if "got prompt" in line:
                    decision = self.gateway.set_downstream_executing(True)
                    if decision == "ENTER_BUSY":
                        self.sync_state_to_infrastructure()
                elif "Prompt executed in" in line:
                    decision = self.gateway.set_downstream_executing(False)
                    if decision == "CLEAR_RUNNING_AND_DISPATCH":
                        self.sync_state_to_infrastructure()
                        self.notify_status_changed()
                        self.try_dispatch()

        threading.Thread(
            target=log_reader, args=(self._process.stdout, False), daemon=True
        ).start()
        threading.Thread(
            target=log_reader, args=(self._process.stderr, True), daemon=True
        ).start()

    async def wait_downstream_ready(self) -> None:
        """以轮询 GET 的形式等待下游进程就绪。"""
        url = f"http://127.0.0.1:{self.downstream_port}"
        logger.info(f"⌛ Waiting for downstream service ({url})...")
        async with aiohttp.ClientSession() as session:
            for _ in range(300):
                if self.exiting:
                    return
                try:
                    async with session.get(
                        url, timeout=aiohttp.ClientTimeout(total=3)
                    ) as resp:
                        if resp.status == 200:
                            logger.info("✅ Downstream service ready")
                            should_dispatch = self.gateway.set_downstream_ready(True)
                            if should_dispatch:
                                self.try_dispatch()
                            return
                except Exception:
                    pass
                await asyncio.sleep(1)
        logger.error("❌ Downstream wait timeout")
        sys.exit(1)

    async def restart_downstream(self) -> None:
        """优雅关闭下游老进程并拉起新的下游。"""
        if self._is_restarting:
            return
        self._is_restarting = True
        self.gateway.set_downstream_ready(False)
        try:
            if self._process:
                try:
                    self._process.kill()
                    for _ in range(100):
                        if self._process.poll() is not None:
                            break
                        await asyncio.sleep(0.1)
                except Exception:
                    pass
                self._process = None
            self.gateway.set_downstream_executing(False)
            self.start_downstream()
            await self.wait_downstream_ready()
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
                logger.error(
                    f"❌ Downstream process exited unexpectedly with code {exit_code}. "
                    f"Restarting in {self.config.restart_delay_sec} seconds..."
                )
                with self.queue_lock:
                    if self.queue_reader.get_running_count(limit=1) > 0:
                        self.queue_writer.requeue_running()
                        self.gateway.attempt_count += 1
                self.notify_status_changed()
                await asyncio.sleep(self.config.restart_delay_sec)
                await self.restart_downstream()
            else:
                await asyncio.sleep(1)

    async def shutdown(self) -> None:
        """优雅清理释放持有的资源与停止下游子程序。"""
        self.exiting = True
        if self.state_repo:
            try:
                self.state_repo.close()
            except Exception:
                pass
        if self.queue_writer:
            try:
                self.queue_writer.close()
            except Exception:
                pass
        if self._process:
            try:
                logger.info("Cleaning up downstream process...")
                self._process.kill()
            except Exception:
                pass
        if self.process_manager:
            try:
                logger.info("Cleaning up external programs...")
                self.process_manager.cleanup()
            except Exception:
                pass
        from gateway.infrastructure.system.power import PowerManagement

        PowerManagement.allow_sleep()
