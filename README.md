[**中文**](README.zh.md)

# ComfyUI Pause/Resume Proxy Gateway

A proxy/gateway that wraps ComfyUI to add pause/resume queue controls with persistent state across restarts.

## Features

- **Auto Recovery**: Automatically restores the saved queue on startup
- **Queue Backup**: Backs up the queue in real-time to prevent task loss from unexpected interruptions
- **Instant Queueing**: Tasks are queued instantly without validation — validation is deferred until execution, so submissions complete in milliseconds
- **Crash Recovery**: Automatically retries dispatch on transient errors; re-queues running tasks after a crash (up to 3 attempts per task before permanent failure)
- **Idle Restart**: Optionally restarts the downstream ComfyUI process after a configurable idle timeout to free resources
- **System Tray** (requires `pip install .[tray]`): Dynamic tray icon with queue count and estimated-time progress ring; right-click menu for pause/resume/restart/exit; auto-restores icon after display changes
- **Power Management**: Automatically prevents system sleep while tasks are running or external scripts are active
- **External Program Hooks**: Launches user-defined programs when the queue becomes busy or idle (e.g. GPU monitor, fan control, miner); programs are stopped when the opposite state triggers
- **Process Management**: Complete process startup, stop and monitoring
- **Flexible Configuration**: Supports `.env` file and environment variables (e.g. `COMFYUI_PORT`, `COMFYUI_EXTRA_ARGS`)

## Installation

### Option 1: Deploy script

```powershell
.\scripts\deploy.ps1 -TargetDir "C:\ComfyUI"
```

This copies `start.cmd` and `gateway/` to the target directory and validates that a Python runtime exists.

### Option 2: Manual copy

Copy `start.cmd` and the `gateway/` folder into your ComfyUI portable root directory, next to `python_embeded/`.

## Usage

```cmd
start.cmd
```

The UI will show pause/resume buttons. Startup configuration can be adjusted in `.env` file or environment variables.

## File Structure

- `start.cmd` - Entry script that runs `gateway/__main__.py` to start both the proxy gateway and ComfyUI, with pause/resume controls in the UI
- `gateway/` - Core gateway package
- `gateway_data/` - Default data storage directory (auto-created, configurable via environment variable), containing:
  - `queue.db` - SQLite queue database (WAL mode)
  - `state.db` - SQLite gateway state database for persisting pause/resume state and estimated completion times
  - `queue.json` - Legacy JSON-format queue file (auto-generated when SQLite queue is disabled)
  - `queue.json.tmp` - Temporary file during queue save
  - `failed_workflows/` - Directory for storing failed task info (e.g. 400-500 errors)
- `queue.json~<random_suffix>` - Backup file after automatic migration of legacy JSON queue data to the new data directory (generated in root)

## Configuration

### Environment Variables & .env

Create a `.env` file in the same directory as the script, or set environment variables directly:

- `COMFYUI_PORT`: Service port (default `8188`)
- `COMFYUI_EXTRA_ARGS`: Extra arguments passed to ComfyUI (e.g. `--preview-method auto`)
- `COMFYUI_RESTART_DELAY_SEC`: Delay before restarting after abnormal process exit (seconds, default `10`)
- `COMFYUI_IDLE_RESTART_SEC`: Timeout to force restart after queue becomes idle (seconds, default `600`, set to `0` to disable)
- `COMFYUI_IDLE_PROGRAM`: Path to a program to launch when idle (e.g. miner, auto-stopped when tasks arrive)
- `COMFYUI_BUSY_PROGRAM`: Path to a program to launch when busy (e.g. GPU monitor or fan control, auto-stopped when idle)
- `COMFYUI_QUEUE_TYPE`: Queue implementation type, supports `sqlite` (default, WAL mode, recommended) or `json` (legacy JSON file queue)
- `COMFYUI_GATEWAY_DATA_DIR`: Gateway data storage directory (default `gateway_data`, supports absolute or relative paths — relative paths are resolved from the script root)
- `COMFYUI_HOST`: Gateway listening address (default `127.0.0.1`)
- `COMFYUI_HISTORY_RETENTION_DAYS`: History retention for completed/failed/cancelled jobs in days (default `90`)
- `COMFYUI_LANG`: Force UI language (`zh` or `en`); auto-detected from system locale by default
- `GATEWAY_DEBUG`: Set to `true` to enable detailed debug logging
- `COMFYUI_ESTIMATION_BUCKET_CAPACITY`: Estimation time bucket capacity (default `100`), controls the per-bucket task record limit in the dual-bucket rotation algorithm

## Technical Notes

This implementation uses the HTTP API for queue save and restore. Compared to [yara](https://github.com/Satellile/yara), it fully preserves the workflow's `extra_data`, ensuring accurate queue restoration.

## Caveats

1. `start.cmd` and `gateway/` must be deployed to the ComfyUI portable root directory (use `scripts\deploy.ps1` or manual copy)
2. Pause waits for the current task to finish. To abandon current progress: use ComfyUI's native interrupt (the running task is discarded and won't re-execute on restart), or Ctrl+C / tray Exit (the running task will re-execute on next start)
3. If the process exits abnormally, the script will automatically attempt a restart
4. New task submissions always return success immediately (skipping validation); actual validation is deferred until execution. Workflows that encounter errors (e.g. 400-500 responses) are saved to the `failed_workflows/` directory under the data directory (including error details, original request data, and workflow JSON) for inspection and removed from the queue.
5. GET /queue always returns an empty `outputs_to_execute` since tasks are not parsed immediately on submission.
