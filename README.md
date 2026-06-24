# ComfyUI 可暂停队列代理网关

将仓库代码放置在便携版 ComfyUI 的根目录中，使用 `start.cmd` 代替原本的启动脚本启动，界面上会多出暂停恢复按钮

启动配置可以在 `.env` 文件或环境变量中调整。

## 功能特点

- **自动恢复**：启动时自动恢复上次暂停时保存的队列
- **队列备份**：运行过程中自动备份队列，防止意外中断导致任务丢失
- **进程管理**：完善的进程启动、停止和监控机制
- **灵活配置**：支持 `.env` 文件和环境变量配置（如 `COMFYUI_PORT`, `COMFYUI_EXTRA_ARGS`）

## 使用说明

### 启动 ComfyUI

```cmd
start.cmd
```

## 文件说明

- `start.cmd` - 启动脚本，直接运行 `gateway/__main__.py` 启动代理网关和 ComfyUI 本身，支持直接 UI 上控制队列暂停
- `gateway/` - 代理网关的核心业务包目录
- `gateway_data/` - 默认的数据存储目录（自动生成，可通过环境变量修改），其中包含：
  - `queue.db` - SQLite 队列数据库文件（启用 WAL 模式）
  - `state.db` - SQLite 网关状态数据库文件，用于持久化暂停/恢复等运行状态和任务预估时间数据
  - `queue.json` - 传统 JSON 格式的保存队列文件（禁用 SQLite 队列时自动生成）
  - `queue.json.tmp` - 队列保存时产生的临时文件
  - `failed_workflows/` - 保存提交失败（如 400-500 错误）的任务信息的目录
- `queue.json~<随机后缀>` - 旧版本 JSON 队列数据自动迁移到新数据目录后的备份文件（生成于根目录）

## 配置选项

### 环境变量与 .env

支持在脚本同目录下创建 `.env` 文件或直接设置环境变量：

- `COMFYUI_PORT`: 服务端口（默认 `8188`）
- `COMFYUI_EXTRA_ARGS`: 传递给 ComfyUI 的额外参数（例如 `--preview-method auto`）
- `COMFYUI_RESTART_DELAY_SEC`: 进程异常退出后重启延迟（秒，默认 `10`）
- `COMFYUI_IDLE_RESTART_SEC`: 队列空闲后强制重启服务的超时时间（秒，默认 `600`，设置为 0 则禁用）
- `COMFYUI_IDLE_PROGRAM`: 闲置时启动的程序路径（例如矿工程序，在有任务时会自动停止）
- `COMFYUI_BUSY_PROGRAM`: 繁忙时启动的程序路径（例如 GPU 监控或风扇控制程序，在闲置时会自动停止）
- `COMFYUI_QUEUE_TYPE`: 队列实现类型，支持 `sqlite`（默认值，启用 WAL 模式，推荐）或 `json`（传统 JSONFile 队列实现）
- `COMFYUI_GATEWAY_DATA_DIR`: 网关数据存储目录（默认值 `gateway_data`，支持绝对路径或相对路径，相对路径会相对于启动脚本所在根目录解析）
- `COMFYUI_ESTIMATION_BUCKET_CAPACITY`: 预估时间桶容量（默认 `100`），控制双桶轮换算法中每个桶的任务记录数量上限

## 技术说明

此实现使用 HTTP API 进行队列的保存和恢复，相比 [yara](https://github.com/Satellile/yara) 的方案，能够完整保存工作流的 `extra_data` 信息，确保队列恢复的准确性。

## 注意事项

1. 脚本需要放置在 ComfyUI 便携版的根目录
2. 暂停操作会等待当前任务完成，如果需要立即中断可使用原生的中断当前工作流操作
3. 如果进程异常退出，脚本会自动尝试重启
4. 提交新任务时会跳过校验总是返回成功，实际校验将延迟到执行前，有错误的工作流（例如遇到 400-500 错误时）会被保存至数据存储目录下的 `failed_workflows/` 目录下（包括错误信息、原始请求数据以及工作流 JSON）供排查，并从队列中丢弃。
5. GET /queue 会总是返回空的 outputs_to_execute， 因为现在收到任务时没有立即解析
