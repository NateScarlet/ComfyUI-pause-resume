$port = $env:COMFYUI_PORT ?? 8188
$url = "http://localhost:$port"

try {
    $response = Invoke-WebRequest -Uri $url -Method Get -TimeoutSec 1 -ErrorAction SilentlyContinue
    if ($response -and $response.StatusCode -eq 200) {
        Write-Host "✅ 服务已在运行 ($url)" -ForegroundColor Green
        exit 0
    }    
}
catch {
    <#Do this if a terminating exception happens#>
}

# 保存进程信息
$process = Start-Process -FilePath "$PSScriptRoot\python_embeded\python.exe" `
    -ArgumentList @("-s", "ComfyUI\main.py", "--port", $port, "--fast", "--cache-classic", "--preview-method", "taesd", "--preview-size", "1024") `
    -PassThru `
    -ErrorAction Stop

@{
    PID         = $process.Id
    Port        = $port
    ProcessName = $process.ProcessName
    StartTimeTicks   = $process.StartTime.Ticks
} | ConvertTo-Json | Set-Content -Path "$PSScriptRoot\.process_info" -Force

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
            # 不关心错误，用户可以自己手动再 load
            yara load last_session
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
