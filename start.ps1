$port = $env:COMFYUI_PORT ?? 8188
$url = "http://localhost:$port"
$info_file = "$PSScriptRoot\.process_info"
$queue_file = "$PSScriptRoot\queue.json"
$program = "$PSScriptRoot\python_embeded\python.exe"
$program_args = @("-s", "ComfyUI\main.py", "--port", $port)

function Wait-ServerReady() {
    $timeout = 300  # 最长等待时间（秒）
    $interval = 1   # 检测间隔（秒）
    $elapsed = 0

    Write-Host "⌛ 等待服务启动 (http://localhost:$port)..." -ForegroundColor Cyan

    # 循环检测服务状态
    while ($elapsed -lt $timeout) {
        try {
            # 尝试访问服务端点
            $response = Invoke-WebRequest -Uri $url -Method Get -TimeoutSec 3 -ErrorAction Stop
            if ($response.StatusCode -eq 200) {
                return
            }
        }
        catch {
            # 忽略连接错误
        }

        # 等待间隔
        Start-Sleep -Seconds $interval
        $elapsed += $interval
    }

    Write-Host "❌ 等待服务启动超时 ($timeout 秒)" -ForegroundColor Red
    throw "Wait Timeout"
}

function Send-Workflow {
    param (
        [Parameter(Mandatory = $true, ValueFromPipeline = $true)]
        [PSObject]$workflow  # 直接接收 JSON 数组的单个元素（长度为5的数组）
    )
    $number, $id, $prompt, $extra_data, $_ = $workflow
    Write-Host "处理工作流 $number ($id)"
    $body = @{
        number     = $number
        prompt     = $prompt
        extra_data = $extra_data
    }
    if ($extra_data.client_id) {
        $body.client_id = $extra_data.client_id
    }
    $body = $body | ConvertTo-Json -Compress -Depth 100
    
    $response = Invoke-WebRequest -Uri "$url/prompt" -Method Post -Body $body -ContentType "application/json"
    if ($response.StatusCode -ne 200) {
        Write-Error "工作流入列失败 状态码: $($response.StatusCode), 响应: $($response.Content)"
    }
}


try {
    $response = Invoke-WebRequest -Uri $url -Method Get -TimeoutSec 1 -ErrorAction SilentlyContinue
    if ($response -and $response.StatusCode -eq 200) {
        Write-Host "✅ 服务已在运行 ($url)" -ForegroundColor Green
        exit 0
    }    
}
catch {
    # 忽略检测出错
}

# 保存进程信息
$process = Start-Process -FilePath $program `
    -ArgumentList $program_args `
    -PassThru `
    -ErrorAction Stop

@{
    PID            = $process.Id
    Port           = $port
    ProcessName    = $process.ProcessName
    StartTimeTicks = $process.StartTime.Ticks
} | ConvertTo-Json | Set-Content -Path $info_file -Force


if (Test-Path $queue_file) {
    try {
        Wait-ServerReady
        $queue = Get-Content $queue_file -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
        Write-Host "获取到 $($queue.queue_running.Length) + $($queue.queue_pending.Length) 工作流"
        
        $queue.queue_running | ForEach-Object { Send-Workflow $_ -ErrorAction Stop }
        $queue.queue_pending | ForEach-Object { Send-Workflow $_ -ErrorAction Stop }
        # 保留多一个版本
        Move-Item $queue_file "${queue_file}~" -Force -ErrorAction Ignore
    }
    catch {
        Write-Host "恢复队列文件 ${queue_file} 过程中遇到错误，中止服务"
        $process.Kill()
        Remove-Item $info_file -ErrorAction SilentlyContinue
        throw
    }
}
