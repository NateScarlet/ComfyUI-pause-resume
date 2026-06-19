import json
import logging
import traceback
import asyncio
import time
from pathlib import Path
import aiohttp
from aiohttp import web
from typing import List, Dict, Any, Set, Union, cast

from .gateway import Gateway
from .models import Task, raw_json_dumps

# 预加载静态资源文件，避免每次请求时重复读取磁盘
_STATIC_DIR = Path(__file__).parent / "static"
_LOADING_HTML = (_STATIC_DIR / "loading.html").read_text(encoding="utf-8")
_INJECT_JS = (_STATIC_DIR / "inject.js").read_text(encoding="utf-8")

logger = logging.getLogger(__name__)


class GatewayHandlers:
    """管理所有 API 接口、Web 页面注入拦截和 WebSocket/SSE 反向代理的请求处理器"""

    def __init__(self, gateway: Gateway):
        self.gateway = gateway

    async def handle_pause(self, request: web.Request) -> web.Response:
        """暂停队列接口 POST /io.github.natescarlet.pause-resume/pause

        JSON body 可选参数: {"restart_after_idle": true}
        为 true 时暂停后等系统闲置便立即重启下游 ComfyUI。
        """
        restart_after_idle = False
        if request.body_exists:
            try:
                body: Any = await request.json()
                if isinstance(body, dict):
                    body_dict: Dict[str, Any] = cast(Dict[str, Any], body)
                    val: Any = body_dict.get("restart_after_idle")
                    if isinstance(val, bool):
                        restart_after_idle = val
                    elif isinstance(val, str) and val.lower() in ("1", "true", "yes"):
                        restart_after_idle = True
            except Exception:
                pass

        self.gateway.pause(restart_after_idle=restart_after_idle)
        return web.json_response({"status": "ok", "paused": True})

    async def handle_resume(self, request: web.Request) -> web.Response:
        """恢复队列接口 POST /io.github.natescarlet.pause-resume/resume"""
        self.gateway.resume()
        return web.json_response({"status": "ok", "paused": False})

    async def handle_state(self, request: web.Request) -> web.Response:
        """查询暂停状态接口 GET /io.github.natescarlet.pause-resume/state"""
        return web.json_response({"paused": self.gateway.paused})

    async def handle_sse(self, request: web.Request) -> web.StreamResponse:
        """建立 SSE 长连接以推送网关状态变更 GET /io.github.natescarlet.pause-resume/sse"""
        response = web.StreamResponse(
            status=200,
            reason="OK",
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            },
        )
        await response.prepare(request)

        q: asyncio.Queue[str] = asyncio.Queue()
        self.gateway.sse_clients.add(q)
        try:
            # 告知浏览器断线后每 3 秒重连一次
            await response.write(b"retry: 3000\n\n")
            # 立即推送一次当前的初始暂停状态
            initial_data = json.dumps({"paused": self.gateway.paused})
            await response.write(f"data: {initial_data}\n\n".encode("utf-8"))

            while True:
                data = await q.get()
                if data == "shutdown":
                    q.task_done()
                    break
                await response.write(f"data: {data}\n\n".encode("utf-8"))
                q.task_done()
        except (asyncio.CancelledError, ConnectionResetError):
            pass
        finally:
            self.gateway.sse_clients.discard(q)
        return response

    async def proxy_handler(
        self, request: web.Request
    ) -> Union[web.Response, web.StreamResponse]:
        """核心反向代理和拦截器，处理与下游 ComfyUI 的通信"""
        path = request.path
        # 拦截我们自定义命名空间下的非法请求直接返回 404
        if path == "/io.github.natescarlet.pause-resume" or path.startswith(
            "/io.github.natescarlet.pause-resume/"
        ):
            return web.Response(status=404, text="Not Found")

        # 若下游 ComfyUI 进程未准备好，如果是 HTML 请求则渲染正在启动页面，如果是 API 请求则返回 503
        if not self.gateway.downstream_ready:
            method = request.method
            accept = request.headers.get("Accept", "")
            if method == "GET" and (
                path == "/" or path == "/index.html" or "text/html" in accept
            ):
                return web.Response(
                    content_type="text/html",
                    text=_LOADING_HTML,
                )
            return web.Response(
                status=503, text="Service Unavailable: Downstream is booting up"
            )

        method = request.method
        headers = dict(request.headers)

        downstream_url = (
            f"http://127.0.0.1:{self.gateway.downstream_port}{request.path_qs}"
        )

        # 拦截并重写 POST /prompt，以便网关自主接管任务队列
        if method == "POST" and path in ("/prompt", "/api/prompt"):
            _t_total_start = time.perf_counter()
            try:
                _t_json_start = time.perf_counter()
                body = await request.json()
                _t_json = (time.perf_counter() - _t_json_start) * 1000

                prompt = body.get("prompt", {})
                extra_data_raw = body.get("extra_data", {})
                extra_data = (
                    dict(cast(dict[str, Any], extra_data_raw))
                    if isinstance(extra_data_raw, dict)
                    else {}
                )
                prompt_id = body.get("prompt_id")
                if prompt_id is not None:
                    prompt_id = str(prompt_id)

                number = None
                if "number" in body:
                    try:
                        number = float(body["number"])
                    except (ValueError, TypeError):
                        pass

                front = bool(body.get("front", False))

                result = self.gateway.add_task(
                    prompt=prompt,
                    extra_data=extra_data,
                    prompt_id=prompt_id,
                    number=number,
                    front=front,
                )

                _t_total = (time.perf_counter() - _t_total_start) * 1000
                logger.info(
                    f"📥 Intercepted workflow {result['prompt_id']} "
                    f"(json={_t_json:.1f}ms total={_t_total:.1f}ms)"
                )

                return web.json_response(
                    {
                        "prompt_id": result["prompt_id"],
                        "number": result["number"],
                        "node_errors": {},
                    }
                )
            except Exception as e:
                logger.error(f"Error processing {path}: {e}")
                traceback.print_exc()
                return web.Response(status=400, text=str(e))

        # 拦截 GET /queue 请求，由网关合并下游及自己所接管的真实队列状态并返回给前端
        if method == "GET" and path in ("/queue", "/api/queue"):
            with self.gateway.queue_lock:
                return web.json_response(
                    {
                        "queue_running": [
                            t.to_list() for t in self.gateway.queue.get_running()
                        ],
                        "queue_pending": [
                            t.to_list() for t in self.gateway.queue.get_pending()
                        ],
                    },
                    dumps=raw_json_dumps,
                )

        # 拦截 GET /api/jobs (ComfyUI 新版历史队列及排队列表查询端点)
        if method == "GET" and path in ("/api/jobs", "/api/jobs/"):
            query_params = request.rel_url.query
            downstream_jobs_url = (
                f"http://127.0.0.1:{self.gateway.downstream_port}/api/jobs"
            )

            downstream_jobs: List[Dict[str, Any]] = []
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
                                    downstream_jobs = cast(
                                        List[Dict[str, Any]], raw_jobs
                                    )
            except Exception as e:
                logger.error(f"Error fetching jobs from downstream: {e}")

            def make_job_dict(task: Task, status_str: str) -> Dict[str, Any]:
                workflow_id = None
                extra_data = json.loads(task.extra_data)
                extra_pnginfo = extra_data.get("extra_pnginfo", {})
                if isinstance(extra_pnginfo, dict):
                    extra_pnginfo_dict = cast(Dict[str, Any], extra_pnginfo)
                    workflow = extra_pnginfo_dict.get("workflow", {})
                    if isinstance(workflow, dict):
                        workflow_dict = cast(Dict[str, Any], workflow)
                        workflow_id = workflow_dict.get("id")
                return {
                    "id": task.prompt_id,
                    "status": status_str,
                    "priority": task.number,
                    "create_time": task.create_time,
                    "outputs_count": 0,
                    "workflow_id": workflow_id,
                }

            with self.gateway.queue_lock:
                running_tasks = self.gateway.queue.get_running()
                pending_tasks = self.gateway.queue.get_pending()

            gateway_running_jobs = [
                make_job_dict(t, "in_progress") for t in running_tasks
            ]
            gateway_pending_jobs = [make_job_dict(t, "pending") for t in pending_tasks]

            # 支持状态过滤
            status_param = query_params.get("status")
            if status_param:
                status_filter = [
                    s.strip().lower() for s in status_param.split(",") if s.strip()
                ]
                gateway_running_jobs = [
                    j for j in gateway_running_jobs if j["status"] in status_filter
                ]
                gateway_pending_jobs = [
                    j for j in gateway_pending_jobs if j["status"] in status_filter
                ]

            # 支持工作流 ID 过滤
            workflow_id_param = query_params.get("workflow_id")
            if workflow_id_param:
                gateway_running_jobs = [
                    j
                    for j in gateway_running_jobs
                    if j["workflow_id"] == workflow_id_param
                ]
                gateway_pending_jobs = [
                    j
                    for j in gateway_pending_jobs
                    if j["workflow_id"] == workflow_id_param
                ]

            seen_ids: Set[str] = set()
            merged_jobs: List[Dict[str, Any]] = []

            # 合并网关接管的任务与下游实际已执行过的历史任务
            for j in gateway_running_jobs + gateway_pending_jobs:
                job_id_val = j.get("id")
                if isinstance(job_id_val, str) and job_id_val not in seen_ids:
                    seen_ids.add(job_id_val)
                    merged_jobs.append(j)

            for j in downstream_jobs:
                job_id_val = j.get("id")
                if isinstance(job_id_val, str) and job_id_val not in seen_ids:
                    seen_ids.add(job_id_val)
                    merged_jobs.append(j)

            # 排序处理
            sort_by = query_params.get("sort_by", "created_at").lower()
            sort_order = query_params.get("sort_order", "desc").lower()

            reverse = sort_order == "desc"
            if sort_by == "execution_duration":

                def get_sort_key(job: Dict[str, Any]) -> float:
                    start = job.get("execution_start_time", 0)
                    end = job.get("execution_end_time", 0)
                    try:
                        return float(end) - float(start) if end and start else 0.0
                    except (ValueError, TypeError):
                        return 0.0

            else:

                def get_sort_key(job: Dict[str, Any]) -> float:
                    try:
                        return float(job.get("create_time", 0))
                    except (ValueError, TypeError):
                        return 0.0

            merged_jobs = sorted(merged_jobs, key=get_sort_key, reverse=reverse)

            # 分页截断处理
            total = len(merged_jobs)
            limit = query_params.get("limit")
            offset = query_params.get("offset")

            limit_val = None
            if limit:
                try:
                    limit_val = int(limit)
                except ValueError:
                    pass

            offset_val = 0
            if offset:
                try:
                    offset_val = int(offset)
                except ValueError:
                    pass

            if limit_val is not None:
                jobs_page = merged_jobs[offset_val : offset_val + limit_val]
            else:
                jobs_page = merged_jobs[offset_val:]

            has_more = (offset_val + len(jobs_page)) < total

            return web.json_response(
                {
                    "jobs": jobs_page,
                    "pagination": {
                        "offset": offset_val,
                        "limit": limit_val,
                        "total": total,
                        "has_more": has_more,
                    },
                }
            )

        # 拦截并查询具体 job 详情
        if method == "GET" and path.startswith("/api/jobs/"):
            parts = path.strip("/").split("/")
            if len(parts) == 3:  # 匹配 ["api", "jobs", "job_id"]
                job_id = parts[2]

                with self.gateway.queue_lock:
                    running_tasks = self.gateway.queue.get_running()
                    pending_tasks = self.gateway.queue.get_pending()

                target_task: Task | None = None
                status_str = None
                for t in running_tasks:
                    if t.prompt_id == job_id:
                        target_task = t
                        status_str = "in_progress"
                        break
                if not target_task:
                    for t in pending_tasks:
                        if t.prompt_id == job_id:
                            target_task = t
                            status_str = "pending"
                            break

                if target_task:
                    workflow_id = None
                    extra_data = json.loads(target_task.extra_data)
                    extra_pnginfo = extra_data.get("extra_pnginfo", {})
                    if isinstance(extra_pnginfo, dict):
                        extra_pnginfo_dict = cast(Dict[str, Any], extra_pnginfo)
                        workflow = extra_pnginfo_dict.get("workflow", {})
                        if isinstance(workflow, dict):
                            workflow_dict = cast(Dict[str, Any], workflow)
                            workflow_id = workflow_dict.get("id")

                    job_dict: Dict[str, Any] = {
                        "id": target_task.prompt_id,
                        "status": status_str,
                        "priority": target_task.number,
                        "create_time": target_task.create_time,
                        "outputs_count": 0,
                        "workflow_id": workflow_id,
                        "workflow": {
                            "prompt": target_task.prompt,
                            "extra_data": target_task.extra_data,
                        },
                    }
                    return web.json_response(job_dict, dumps=raw_json_dumps)

        # 拦截任务的取消和清空操作 POST /queue (带有 clear / delete 属性)
        if method == "POST" and path in ("/queue", "/api/queue"):
            try:
                body_json: Dict[str, Any] = await request.json()
            except Exception:
                body_json = {}

            clear = bool(body_json.get("clear"))
            raw_delete = body_json.get("delete")
            delete_ids = None
            if isinstance(raw_delete, list):
                delete_ids = [str(item) for item in cast(List[Any], raw_delete)]

            self.gateway.modify_queue(clear=clear, delete_ids=delete_ids)
            return web.Response(status=200)

        # 代理 WebSocket 连接，并在推送状态数据包中合并网关的队列信息
        if request.headers.get("Upgrade", "").lower() == "websocket":
            ws_server = web.WebSocketResponse()
            await ws_server.prepare(request)

            self.gateway.ws_clients.add(ws_server)

            async with aiohttp.ClientSession() as session:
                try:
                    async with session.ws_connect(downstream_url) as ws_client:

                        async def ws_forward(
                            ws_from: Union[
                                web.WebSocketResponse, aiohttp.ClientWebSocketResponse
                            ],
                            ws_to: Union[
                                web.WebSocketResponse, aiohttp.ClientWebSocketResponse
                            ],
                        ) -> None:
                            async for msg in ws_from:
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    data_str = msg.data
                                    if ws_from == ws_client:
                                        try:
                                            # 如果是下游返回的状态包，注入网关本身的待处理数
                                            data_json = json.loads(data_str)
                                            if isinstance(data_json, dict):
                                                data_dict = cast(
                                                    Dict[str, Any], data_json
                                                )
                                                if data_dict.get("type") == "status":
                                                    if (
                                                        "data" in data_dict
                                                        and isinstance(
                                                            data_dict["data"], dict
                                                        )
                                                    ):
                                                        if "status" in data_dict[
                                                            "data"
                                                        ] and isinstance(
                                                            data_dict["data"]["status"],
                                                            dict,
                                                        ):
                                                            if "exec_info" in data_dict[
                                                                "data"
                                                            ]["status"] and isinstance(
                                                                data_dict["data"][
                                                                    "status"
                                                                ]["exec_info"],
                                                                dict,
                                                            ):
                                                                # 采用轻量计数方法，避免在广播状态时加载和反序列化所有排队大对象
                                                                remaining = (
                                                                    self.gateway.queue.get_pending_count()
                                                                    + self.gateway.queue.get_running_count()
                                                                )
                                                                data_dict["data"][
                                                                    "status"
                                                                ]["exec_info"][
                                                                    "queue_remaining"
                                                                ] = remaining
                                                                data_str = json.dumps(
                                                                    data_dict
                                                                )
                                        except Exception:
                                            pass
                                    await ws_to.send_str(data_str)
                                elif msg.type == aiohttp.WSMsgType.BINARY:
                                    await ws_to.send_bytes(msg.data)
                                elif msg.type == aiohttp.WSMsgType.PING:
                                    await ws_to.ping()
                                elif msg.type == aiohttp.WSMsgType.PONG:
                                    await ws_to.pong()
                                elif msg.type == aiohttp.WSMsgType.CLOSE:
                                    await ws_to.close()
                                    break

                        t1 = asyncio.create_task(ws_forward(ws_server, ws_client))
                        t2 = asyncio.create_task(ws_forward(ws_client, ws_server))
                        await asyncio.gather(t1, t2)
                except Exception as e:
                    logger.error(f"WebSocket Error: {e}")
                finally:
                    self.gateway.ws_clients.discard(ws_server)
            return ws_server

        # 拦截 Web 主页请求以动态注入客户端扩展控制按钮 JS
        if method == "GET" and (path == "/" or path == "/index.html"):
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(downstream_url, headers=headers) as resp:
                        body = await resp.read()
                        html = body.decode("utf-8", errors="replace")

                        # 注入控制按钮及前端 SSE 同步监听逻辑
                        injection = (
                            '\n<script type="module">\n' + _INJECT_JS + "\n</script>\n"
                        )
                        if "</body>" in html:
                            html = html.replace("</body>", injection + "</body>")
                        else:
                            html += injection

                        # 过滤引起冲突的 headers
                        resp_headers = {
                            k: v
                            for k, v in resp.headers.items()
                            if k.lower()
                            not in (
                                "content-type",
                                "content-length",
                                "content-encoding",
                            )
                        }
                        return web.Response(
                            body=html,
                            status=resp.status,
                            headers=resp_headers,
                            content_type=resp.content_type,
                        )
                except Exception as e:
                    return web.Response(status=502, text=f"Bad Gateway: {e}")

        # 默认普通代理流：将请求直接原样传递给下游 ComfyUI，并以流形式写回响应
        try:
            if request.body_exists:
                body = await request.read()
            else:
                body = None

            async with aiohttp.ClientSession() as session:
                async with session.request(
                    method, downstream_url, headers=headers, data=body
                ) as resp:
                    proxy_resp = web.StreamResponse(
                        status=resp.status, headers=resp.headers
                    )
                    await proxy_resp.prepare(request)
                    async for chunk in resp.content.iter_any():
                        await proxy_resp.write(chunk)
                    await proxy_resp.write_eof()
                    return proxy_resp
        except Exception as e:
            return web.Response(status=502, text=f"Bad Gateway: {e}")


def setup_routes(app: web.Application, handlers: GatewayHandlers) -> None:
    """注册网关自定义控制端点以及默认代理路由"""
    app.router.add_post(
        "/io.github.natescarlet.pause-resume/pause", handlers.handle_pause
    )
    app.router.add_post(
        "/io.github.natescarlet.pause-resume/resume", handlers.handle_resume
    )
    app.router.add_get(
        "/io.github.natescarlet.pause-resume/state", handlers.handle_state
    )
    app.router.add_get("/io.github.natescarlet.pause-resume/sse", handlers.handle_sse)

    # 通配路由：将其余所有请求交由代理处理器转发
    app.router.add_route("*", "/{tail:.*}", handlers.proxy_handler)
