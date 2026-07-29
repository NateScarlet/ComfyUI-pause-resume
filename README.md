[**中文**](README.zh.md)

# ComfyUI Pause/Resume Proxy Gateway

Place this repository in the root of your ComfyUI portable installation. Use `start.cmd` instead of the original startup script — the UI will show pause/resume buttons.

Startup configuration can be adjusted in `.env` file or environment variables.

## Features

- **Auto Recovery**: Automatically restores the saved queue on startup
- **Queue Backup**: Periodically backs up the queue during operation to prevent task loss from unexpected interruptions
- **Process Management**: Complete process startup, stop and monitoring
- **Flexible Configuration**: Supports `.env` file and environment variables (e.g. `COMFYUI_PORT`, `COMFYUI_EXTRA_ARGS`)

## Usage

### Start ComfyUI

```cmd
start.cmd
```

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
- `COMFYUI_ESTIMATION_BUCKET_CAPACITY`: Estimation time bucket capacity (default `100`), controls the per-bucket task record limit in the dual-bucket rotation algorithm

## Technical Notes

This implementation uses the HTTP API for queue save and restore. Compared to [yara](https://github.com/Satellile/yara), it fully preserves the workflow's `extra_data`, ensuring accurate queue restoration.

## Caveats

1. The script must be placed in the ComfyUI portable edition root directory
2. Pause waits for the current task to finish; use ComfyUI's native interrupt workflow action for immediate interruption
3. If the process exits abnormally, the script will automatically attempt a restart
4. New task submissions always return success immediately (skipping validation); actual validation is deferred until execution. Workflows that encounter errors (e.g. 400-500 responses) are saved to the `failed_workflows/` directory under the data directory (including error details, original request data, and workflow JSON) for inspection and removed from the queue.
5. GET /queue always returns an empty `outputs_to_execute` since tasks are not parsed immediately on submission.
