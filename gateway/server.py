import json
import logging
import traceback
import asyncio
import time
import uuid
import aiohttp
from aiohttp import web
from typing import List, Dict, Any, Set, Union, cast

from .gateway import Gateway
from .models import Task, raw_json_dumps, RawJSON

logger = logging.getLogger(__name__)

class GatewayHandlers:
    """管理所有 API 接口、Web 页面注入拦截和 WebSocket/SSE 反向代理的请求处理器"""
    def __init__(self, gateway: Gateway):
        self.gateway = gateway

    async def handle_pause(self, request: web.Request) -> web.Response:
        """暂停队列接口 POST /io.github.natescarlet.pause-resume/pause"""
        self.gateway.paused = True
        self.gateway.state_manager.set_paused(True)
        logger.info("⏸️ Queue Paused")
        self.gateway.update_sleep_and_programs()
        self.gateway.broadcast_state()
        return web.json_response({"status": "ok", "paused": True})

    async def handle_resume(self, request: web.Request) -> web.Response:
        """恢复队列接口 POST /io.github.natescarlet.pause-resume/resume"""
        self.gateway.paused = False
        self.gateway.state_manager.set_paused(False)
        logger.info("▶️ Queue Resumed")
        self.gateway.update_sleep_and_programs()
        self.gateway.broadcast_state()
        return web.json_response({"status": "ok", "paused": False})

    async def handle_state(self, request: web.Request) -> web.Response:
        """查询暂停状态接口 GET /io.github.natescarlet.pause-resume/state"""
        return web.json_response({"paused": self.gateway.paused})

    async def handle_sse(self, request: web.Request) -> web.StreamResponse:
        """建立 SSE 长连接以推送网关状态变更 GET /io.github.natescarlet.pause-resume/sse"""
        response = web.StreamResponse(
            status=200,
            reason='OK',
            headers={
                'Content-Type': 'text/event-stream',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
            }
        )
        await response.prepare(request)
        
        q: asyncio.Queue[str] = asyncio.Queue()
        self.gateway.sse_clients.add(q)
        try:
            # 立即推送一次当前的初始暂停状态
            initial_data = json.dumps({"paused": self.gateway.paused})
            await response.write(f"data: {initial_data}\n\n".encode('utf-8'))
            
            while True:
                data = await q.get()
                if data == "shutdown":
                    q.task_done()
                    break
                await response.write(f"data: {data}\n\n".encode('utf-8'))
                q.task_done()
        except (asyncio.CancelledError, ConnectionResetError):
            pass
        finally:
            self.gateway.sse_clients.discard(q)
        return response

    async def proxy_handler(self, request: web.Request) -> Union[web.Response, web.StreamResponse]:
        """核心反向代理和拦截器，处理与下游 ComfyUI 的通信"""
        path = request.path
        # 拦截我们自定义命名空间下的非法请求直接返回 404
        if path == '/io.github.natescarlet.pause-resume' or path.startswith('/io.github.natescarlet.pause-resume/'):
            return web.Response(status=404, text="Not Found")
            
        # 若下游 ComfyUI 进程未准备好，如果是 HTML 请求则渲染正在启动页面，如果是 API 请求则返回 503
        if not self.gateway.downstream_ready:
            method = request.method
            accept = request.headers.get("Accept", "")
            if method == "GET" and (path == "/" or path == "/index.html" or "text/html" in accept):
                return web.Response(
                    content_type="text/html",
                    text="""<!DOCTYPE html>
<html>
<head>
    <title>ComfyUI Gateway</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            height: 100vh;
            background-color: #121212;
            color: #e0e0e0;
            margin: 0;
        }
        .loader {
            border: 4px solid #222;
            border-top: 4px solid #3498db;
            border-radius: 50%;
            width: 40px;
            height: 40px;
            animation: spin 1s linear infinite;
            margin-bottom: 20px;
        }
        h2 {
            font-weight: 500;
            margin: 10px 0;
        }
        p {
            color: #888;
            font-size: 14px;
        }
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
    </style>
    <script>
        // 每隔两秒自动刷新以监测下游服务是否已就绪
        setTimeout(() => {
            window.location.reload();
        }, 2000);
    </script>
</head>
<body>
    <div class="loader"></div>
    <h2>ComfyUI 正在启动中...</h2>
    <p>代理网关已就绪。您可以照常提交任务，本页面将在服务就绪后自动载入。</p>
</body>
</html>
"""
                )
            return web.Response(status=503, text="Service Unavailable: Downstream is booting up")

        method = request.method
        headers = dict(request.headers)
        
        downstream_url = f"http://127.0.0.1:{self.gateway.downstream_port}{request.path_qs}"

        # 拦截并重写 POST /prompt，以便网关自主接管任务队列
        if method == "POST" and path in ("/prompt", "/api/prompt"):
            _t_total_start = time.perf_counter()
            try:
                _t_json_start = time.perf_counter()
                body = await request.json()
                _t_json = (time.perf_counter() - _t_json_start) * 1000
                prompt = body.get("prompt", {})
                extra_data_raw = body.get("extra_data", {})
                extra_data: dict[str, Any] = dict(cast(dict[str, Any], extra_data_raw)) if isinstance(extra_data_raw, dict) else {}
                prompt_id = str(body.get("prompt_id", uuid.uuid4()))
                
                _t_number: float = 0.0
                _t_lock_wait = time.perf_counter()
                with self.gateway.queue_lock:
                    _t_lock_acquired = (time.perf_counter() - _t_lock_wait) * 1000
                    if "number" in body:
                        number = float(body["number"])
                    else:
                        _t_number_start = time.perf_counter()
                        number = float(self.gateway.queue.new_task_number())
                        _t_number = (time.perf_counter() - _t_number_start) * 1000
                        if body.get("front", False):
                            number = -number
                    
                    create_time = int(extra_data.pop("create_time", int(time.time() * 1000)))
                    task = Task(
                        number=number,
                        prompt_id=prompt_id,
                        prompt=RawJSON(json.dumps(prompt, ensure_ascii=False)),
                        extra_data=RawJSON(json.dumps(extra_data, ensure_ascii=False)),
                        outputs_to_execute=[],
                        create_time=create_time,
                    )
                    _t_add_start = time.perf_counter()
                    self.gateway.queue.add_task(task)
                    _t_add = (time.perf_counter() - _t_add_start) * 1000
                    
                _t_total = (time.perf_counter() - _t_total_start) * 1000
                logger.info(
                    f"📥 Intercepted workflow {prompt_id} "
                    f"(json={_t_json:.1f}ms lock_wait={_t_lock_acquired:.1f}ms "
                    f"new_number={_t_number:.1f}ms add_task={_t_add:.1f}ms total={_t_total:.1f}ms)"
                )
                    
                # 异步执行后续的状态变更、WebSocket 广播和日志输出，加速 HTTP 响应返回
                async def post_process_task():
                    _t_post_start = time.perf_counter()
                    try:
                        self.gateway.update_sleep_and_programs()
                        _t_sleep_done = (time.perf_counter() - _t_post_start) * 1000
                        self.gateway.broadcast_ws_status()
                        _t_broadcast = (time.perf_counter() - _t_post_start) * 1000 - _t_sleep_done
                        pending_cnt = self.gateway.queue.get_pending_count()
                        _t_post_total = (time.perf_counter() - _t_post_start) * 1000
                        logger.info(
                            f"📬 Post-process {prompt_id} "
                            f"(sleep={_t_sleep_done:.1f}ms broadcast={_t_broadcast:.1f}ms "
                            f"total={_t_post_total:.1f}ms Queue: {pending_cnt})"
                        )
                    except Exception as ex:
                        logger.error(f"Error in post_process_task for {prompt_id}: {ex}")

                asyncio.create_task(post_process_task())
                
                return web.json_response({
                    "prompt_id": prompt_id,
                    "number": number,
                    "node_errors": {}
                })
            except Exception as e:
                logger.error(f"Error processing {path}: {e}")
                traceback.print_exc()
                return web.Response(status=400, text=str(e))

        # 拦截 GET /queue 请求，由网关合并下游及自己所接管的真实队列状态并返回给前端
        if method == "GET" and path in ("/queue", "/api/queue"):
            with self.gateway.queue_lock:
                return web.json_response({
                    "queue_running": [t.to_list() for t in self.gateway.queue.get_running()],
                    "queue_pending": [t.to_list() for t in self.gateway.queue.get_pending()]
                }, dumps=raw_json_dumps)

        # 拦截 GET /api/jobs (ComfyUI 新版历史队列及排队列表查询端点)
        if method == "GET" and path in ("/api/jobs", "/api/jobs/"):
            query_params = request.rel_url.query
            downstream_jobs_url = f"http://127.0.0.1:{self.gateway.downstream_port}/api/jobs"
            
            downstream_jobs: List[Dict[str, Any]] = []
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(downstream_jobs_url, params=query_params) as resp:
                        if resp.status == 200:
                            resp_json = await resp.json()
                            if isinstance(resp_json, dict):
                                resp_dict = cast(Dict[str, Any], resp_json)
                                raw_jobs = resp_dict.get("jobs", [])
                                if isinstance(raw_jobs, list):
                                    downstream_jobs = cast(List[Dict[str, Any]], raw_jobs)
            except Exception as e:
                logger.error(f"Error fetching jobs from downstream: {e}")
                
            def make_job_dict(task: Task, status_str: str) -> Dict[str, Any]:
                workflow_id = None
                extra_data = json.loads(task.extra_data)
                extra_pnginfo = extra_data.get('extra_pnginfo', {})
                if isinstance(extra_pnginfo, dict):
                    extra_pnginfo_dict = cast(Dict[str, Any], extra_pnginfo)
                    workflow = extra_pnginfo_dict.get('workflow', {})
                    if isinstance(workflow, dict):
                        workflow_dict = cast(Dict[str, Any], workflow)
                        workflow_id = workflow_dict.get('id')
                return {
                    'id': task.prompt_id,
                    'status': status_str,
                    'priority': task.number,
                    'create_time': task.create_time,
                    'outputs_count': 0,
                    'workflow_id': workflow_id
                }

            with self.gateway.queue_lock:
                running_tasks = self.gateway.queue.get_running()
                pending_tasks = self.gateway.queue.get_pending()
                
            gateway_running_jobs = [make_job_dict(t, 'in_progress') for t in running_tasks]
            gateway_pending_jobs = [make_job_dict(t, 'pending') for t in pending_tasks]
            
            # 支持状态过滤
            status_param = query_params.get('status')
            if status_param:
                status_filter = [s.strip().lower() for s in status_param.split(',') if s.strip()]
                gateway_running_jobs = [j for j in gateway_running_jobs if j['status'] in status_filter]
                gateway_pending_jobs = [j for j in gateway_pending_jobs if j['status'] in status_filter]
                
            # 支持工作流 ID 过滤
            workflow_id_param = query_params.get('workflow_id')
            if workflow_id_param:
                gateway_running_jobs = [j for j in gateway_running_jobs if j['workflow_id'] == workflow_id_param]
                gateway_pending_jobs = [j for j in gateway_pending_jobs if j['workflow_id'] == workflow_id_param]

            seen_ids: Set[str] = set()
            merged_jobs: List[Dict[str, Any]] = []
            
            # 合并网关接管的任务与下游实际已执行过的历史任务
            for j in gateway_running_jobs + gateway_pending_jobs:
                job_id_val = j.get('id')
                if isinstance(job_id_val, str) and job_id_val not in seen_ids:
                    seen_ids.add(job_id_val)
                    merged_jobs.append(j)
                    
            for j in downstream_jobs:
                job_id_val = j.get('id')
                if isinstance(job_id_val, str) and job_id_val not in seen_ids:
                    seen_ids.add(job_id_val)
                    merged_jobs.append(j)
                    
            # 排序处理
            sort_by = query_params.get('sort_by', 'created_at').lower()
            sort_order = query_params.get('sort_order', 'desc').lower()
            
            reverse = (sort_order == 'desc')
            if sort_by == 'execution_duration':
                def get_sort_key(job: Dict[str, Any]) -> float:
                    start = job.get('execution_start_time', 0)
                    end = job.get('execution_end_time', 0)
                    try:
                        return float(end) - float(start) if end and start else 0.0
                    except (ValueError, TypeError):
                        return 0.0
            else:
                def get_sort_key(job: Dict[str, Any]) -> float:
                    try:
                        return float(job.get('create_time', 0))
                    except (ValueError, TypeError):
                        return 0.0
                    
            merged_jobs = sorted(merged_jobs, key=get_sort_key, reverse=reverse)
            
            # 分页截断处理
            total = len(merged_jobs)
            limit = query_params.get('limit')
            offset = query_params.get('offset')
            
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
            
            return web.json_response({
                'jobs': jobs_page,
                'pagination': {
                    'offset': offset_val,
                    'limit': limit_val,
                    'total': total,
                    'has_more': has_more
                }
            })

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
                        status_str = 'in_progress'
                        break
                if not target_task:
                    for t in pending_tasks:
                        if t.prompt_id == job_id:
                            target_task = t
                            status_str = 'pending'
                            break
                            
                if target_task:
                    workflow_id = None
                    extra_data = json.loads(target_task.extra_data)
                    extra_pnginfo = extra_data.get('extra_pnginfo', {})
                    if isinstance(extra_pnginfo, dict):
                        extra_pnginfo_dict = cast(Dict[str, Any], extra_pnginfo)
                        workflow = extra_pnginfo_dict.get('workflow', {})
                        if isinstance(workflow, dict):
                            workflow_dict = cast(Dict[str, Any], workflow)
                            workflow_id = workflow_dict.get('id')
                    
                    job_dict: Dict[str, Any] = {
                        'id': target_task.prompt_id,
                        'status': status_str,
                        'priority': target_task.number,
                        'create_time': target_task.create_time,
                        'outputs_count': 0,
                        'workflow_id': workflow_id,
                        'workflow': {
                            'prompt': target_task.prompt,
                            'extra_data': target_task.extra_data
                        }
                    }
                    return web.json_response(job_dict, dumps=raw_json_dumps)

        # 拦截任务的取消和清空操作 POST /queue (带有 clear / delete 属性)
        if method == "POST" and path in ("/queue", "/api/queue"):
            try:
                body_json: Dict[str, Any] = await request.json()
            except Exception:
                body_json = {}
                
            with self.gateway.queue_lock:
                if body_json.get("clear"):
                    self.gateway.queue.clear_pending()
                raw_delete = body_json.get("delete")
                if isinstance(raw_delete, list):
                    delete_list: List[str] = [str(item) for item in cast(List[Any], raw_delete)]
                    self.gateway.queue.delete_pending(delete_list)
            
            self.gateway.update_sleep_and_programs()
            self.gateway.broadcast_ws_status()
            return web.Response(status=200)

        # 代理 WebSocket 连接，并在推送状态数据包中合并网关的队列信息
        if request.headers.get("Upgrade", "").lower() == "websocket":
            ws_server = web.WebSocketResponse()
            await ws_server.prepare(request)
            
            self.gateway.ws_clients.add(ws_server)
            
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.ws_connect(downstream_url) as ws_client:

                        async def ws_forward(ws_from: Union[web.WebSocketResponse, aiohttp.ClientWebSocketResponse], 
                                           ws_to: Union[web.WebSocketResponse, aiohttp.ClientWebSocketResponse]) -> None:
                            async for msg in ws_from:
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    data_str = msg.data
                                    if ws_from == ws_client:
                                        try:
                                            # 如果是下游返回的状态包，注入网关本身的待处理数
                                            data_json = json.loads(data_str)
                                            if isinstance(data_json, dict):
                                                data_dict = cast(Dict[str, Any], data_json)
                                                if data_dict.get("type") == "status":
                                                    if "data" in data_dict and isinstance(data_dict["data"], dict):
                                                        if "status" in data_dict["data"] and isinstance(data_dict["data"]["status"], dict):
                                                            if "exec_info" in data_dict["data"]["status"] and isinstance(data_dict["data"]["status"]["exec_info"], dict):
                                                                # 采用轻量计数方法，避免在广播状态时加载和反序列化所有排队大对象
                                                                remaining = self.gateway.queue.get_pending_count() + self.gateway.queue.get_running_count()
                                                                data_dict["data"]["status"]["exec_info"]["queue_remaining"] = remaining
                                                                data_str = json.dumps(data_dict)
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
                        injection = """
                        <script type="module">
                        import { app } from "/scripts/app.js";

                        app.registerExtension({
                            name: "io.github.natescarlet.pause-resume",
                            async setup() {
                                let proxyPaused = false;
                                let btnPause = null;
                                
                                function setButtonState(btn) {
                                    if (!btn) return;
                                    const isNewUI = !!document.getElementById('vue-app');
                                    if (isNewUI) {
                                        btn.className = "relative inline-flex items-center justify-center gap-1.5 cursor-pointer touch-manipulation whitespace-nowrap appearance-none border-none font-medium font-inter transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:pointer-events-none disabled:opacity-50 h-8 rounded-lg p-2 text-xs px-3 " + 
                                            (proxyPaused 
                                                ? "bg-destructive-background text-base-foreground hover:bg-destructive-background-hover" 
                                                : "bg-secondary-background text-secondary-foreground hover:bg-secondary-background-hover");
                                    } else {
                                        btn.className = "";
                                        btn.style.backgroundColor = 'var(--bg-color)';
                                        btn.style.color = 'var(--fg-color)';
                                        btn.style.border = proxyPaused ? '1px solid #e74c3c' : '1px solid #2ecc71';
                                    }
                                    btn.innerText = proxyPaused ? '▶️ Resume' : '⏸️ Pause';
                                }
                                
                                function createPauseButton() {
                                    let btn = document.createElement('button');
                                    btn.onclick = async () => {
                                        let action = proxyPaused ? 'resume' : 'pause';
                                        await fetch(`/io.github.natescarlet.pause-resume/${action}`, {method: 'POST'});
                                    };
                                    
                                    setButtonState(btn);
                                    return btn;
                                }

                                const isNewUI = !!document.getElementById('vue-app');
                                
                                if (isNewUI) {
                                    if (app.menu && app.menu.settingsGroup && app.menu.settingsGroup.element) {
                                        btnPause = createPauseButton();
                                        btnPause.style.alignSelf = 'center';
                                        app.menu.settingsGroup.element.appendChild(btnPause);
                                    }
                                } else {
                                    let qMenu = document.querySelector('.comfy-menu');
                                    if (qMenu) {
                                        btnPause = createPauseButton();
                                        btnPause.style.marginTop = '4px';
                                        qMenu.appendChild(btnPause);
                                    }
                                }
                                
                                let eventSource = null;
                                function connectSSE() {
                                    if (eventSource) {
                                        eventSource.close();
                                    }
                                    eventSource = new EventSource('/io.github.natescarlet.pause-resume/sse');
                                    eventSource.onmessage = (event) => {
                                        try {
                                            let data = JSON.parse(event.data);
                                            proxyPaused = data.paused;
                                            if (btnPause) {
                                                setButtonState(btnPause);
                                            }
                                        } catch (e) {
                                            console.error("Error parsing SSE data", e);
                                        }
                                    };
                                    eventSource.onerror = (err) => {
                                        console.error("SSE connection error, retrying...", err);
                                    };
                                }
                                connectSSE();
                            }
                        });
                        </script>
                        """
                        if "</body>" in html:
                            html = html.replace("</body>", injection + "</body>")
                        else:
                            html += injection
                        
                        # 过滤引起冲突的 headers
                        resp_headers = {
                            k: v for k, v in resp.headers.items()
                            if k.lower() not in ("content-type", "content-length", "content-encoding")
                        }
                        return web.Response(body=html, status=resp.status, headers=resp_headers, content_type=resp.content_type)
                except Exception as e:
                    return web.Response(status=502, text=f"Bad Gateway: {e}")

        # 默认普通代理流：将请求直接原样传递给下游 ComfyUI，并以流形式写回响应
        try:
            if request.body_exists:
                body = await request.read()
            else:
                body = None
                
            async with aiohttp.ClientSession() as session:
                async with session.request(method, downstream_url, headers=headers, data=body) as resp:
                    proxy_resp = web.StreamResponse(status=resp.status, headers=resp.headers)
                    await proxy_resp.prepare(request)
                    async for chunk in resp.content.iter_any():
                        await proxy_resp.write(chunk)
                    await proxy_resp.write_eof()
                    return proxy_resp
        except Exception as e:
            return web.Response(status=502, text=f"Bad Gateway: {e}")


def setup_routes(app: web.Application, handlers: GatewayHandlers) -> None:
    """注册网关自定义控制端点以及默认代理路由"""
    app.router.add_post('/io.github.natescarlet.pause-resume/pause', handlers.handle_pause)
    app.router.add_post('/io.github.natescarlet.pause-resume/resume', handlers.handle_resume)
    app.router.add_get('/io.github.natescarlet.pause-resume/state', handlers.handle_state)
    app.router.add_get('/io.github.natescarlet.pause-resume/sse', handlers.handle_sse)
    
    # 通配路由：将其余所有请求交由代理处理器转发
    app.router.add_route('*', '/{tail:.*}', handlers.proxy_handler)
