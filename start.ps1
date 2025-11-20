#region 配置

$port = $env:COMFYUI_PORT ?? 8188
$url = "http://localhost:$port"
$info_file = "$PSScriptRoot\.process_info"
$queue_file = "$PSScriptRoot\queue.json"
$program = "$PSScriptRoot\python_embeded\python.exe"
$program_args = @("-s", "ComfyUI\main.py", "--port", $port)
# 备份
$backup_debounce_interval = 5  # 防抖间隔（秒）
$max_backup_delay = 30         # 最大备份延迟（秒）

#endregion

#region 辅助函数
function Wait-ServerReady {
    param([int]$Timeout = 300)
    
    $interval = 1
    $elapsed = 0

    Write-Host "⌛ 等待服务启动 (http://localhost:$port)..." -ForegroundColor Cyan

    while ($elapsed -lt $Timeout) {
        try {
            $response = Invoke-WebRequest -Uri $url -Method Get -TimeoutSec 3 -ErrorAction Stop
            if ($response.StatusCode -eq 200) {
                Write-Host "✅ 服务已就绪" -ForegroundColor Green
                return
            }
        }
        catch {
            # 忽略连接错误
        }

        Start-Sleep -Seconds $interval
        $elapsed += $interval
    }

    Write-Host "❌ 等待服务启动超时 ($Timeout 秒)" -ForegroundColor Red
    throw "Wait Timeout"
}

function Send-Workflow {
    param (
        [Parameter(Mandatory = $true, ValueFromPipeline = $true)]
        [PSObject]$workflow
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

#endregion

#region 主程序
try {
    # 检查服务是否已运行
    $response = Invoke-WebRequest -Uri $url -Method Get -TimeoutSec 1 -ErrorAction SilentlyContinue
    if ($response -and $response.StatusCode -eq 200) {
        Write-Host "✅ 服务已在运行 ($url)" -ForegroundColor Green
        exit 0
    }    
}
catch {
    # 忽略检测出错
}



# 创建进程对象
$process = New-Object System.Diagnostics.Process
$process.StartInfo.FileName = $program
$process.StartInfo.Arguments = $program_args -join " "
$process.StartInfo.WorkingDirectory = $PSScriptRoot
$process.StartInfo.CreateNoWindow = $true
$process.StartInfo.RedirectStandardOutput = $true
$process.StartInfo.RedirectStandardError = $true
$process.StartInfo.UseShellExecute = $false

# 创建共享状态对象（解决变量作用域问题）
$sharedState = [PSCustomObject]@{
    EnableBackup           = $false
    LastStderrTime         = $null
    BackupTimer            = $null
    BackupScheduled        = $false
    BackupDebounceInterval = $backup_debounce_interval
    MaxBackupDelay         = $max_backup_delay
}

# 定义备份调度函数（使用共享状态对象）
$scheduleBackup = {
    if (-not $sharedState.EnableBackup) {
        return
    }
    
    $currentTime = Get-Date
    
    # 取消现有计时器
    if ($sharedState.BackupTimer) {
        $sharedState.BackupTimer.Dispose()
        $sharedState.BackupTimer = $null
    }
    
    # 计算延迟时间（防抖逻辑）
    $delay = $sharedState.BackupDebounceInterval
    if ($sharedState.LastStderrTime) {
        $timeSinceLastOutput = ($currentTime - $sharedState.LastStderrTime).TotalSeconds
        if ($timeSinceLastOutput -gt $sharedState.MaxBackupDelay) {
            $delay = 1  # 如果已经很久没有输出，立即备份
        }
    }
    
    $sharedState.BackupScheduled = $true
    
    $sharedState.BackupTimer = New-Object System.Timers.Timer
    $sharedState.BackupTimer.Interval = $delay * 1000
    $sharedState.BackupTimer.AutoReset = $false
    $sharedState.BackupTimer.Add_Elapsed({
            if ($sharedState.BackupScheduled) {
                $sharedState.BackupScheduled = $false
                Write-Host "💾 备份队列到 $queue_file" -ForegroundColor Yellow
    
                try {
                    # 保存当前备份
                    if (Test-Path $queue_file) {
                        Move-Item $queue_file "${queue_file}~" -Force -ErrorAction Ignore
                    }
        
                    # 获取最新队列并保存
                    Invoke-WebRequest -Uri "${url}/queue" -Method Get -OutFile $queue_file -ErrorAction Stop
                    Write-Host "✅ 队列备份完成" -ForegroundColor Green
                }
                catch {
                    Write-Host "❌ 队列备份失败: $($_.Exception.Message)" -ForegroundColor Red
                }
            }
        })
    $sharedState.BackupTimer.Start()
}

# 标准输出处理
$stdoutEvent = Register-ObjectEvent -InputObject $process -EventName OutputDataReceived -Action {
    $data = $Event.SourceEventArgs.Data
    if ($data) {
        Write-Host $data
    }
}

# 标准错误处理（触发备份）
$stderrEvent = Register-ObjectEvent -InputObject $process -EventName ErrorDataReceived -Action {
    $data = $Event.SourceEventArgs.Data
    if ($data) {
        Write-Host $data -ForegroundColor Red
        
        # 更新最后错误输出时间并安排备份
        $sharedState.LastStderrTime = Get-Date
        & $scheduleBackup
    }
}

# 启动进程
Write-Host "🚀 启动 ComfyUI 进程..." -ForegroundColor Green
$process.Start() | Out-Null

# 保存进程信息
@{
    PID            = $process.Id
    Port           = $port
    ProcessName    = $process.ProcessName
    StartTimeTicks = $process.StartTime.Ticks
} | ConvertTo-Json | Set-Content -Path $info_file -Force

# 开始异步读取输出
$process.BeginOutputReadLine()
$process.BeginErrorReadLine()

try {
    # 等待服务就绪
    Wait-ServerReady

    # 恢复队列（如果存在）
    if (Test-Path $queue_file) {
        Write-Host "🔄 恢复队列..." -ForegroundColor Cyan
        $queue = Get-Content $queue_file -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
        Write-Host "获取到 $($queue.queue_running.Length) 运行中 + $($queue.queue_pending.Length) 等待中 工作流"
        
        if ($queue.queue_running.Length -gt 0 -or $queue.queue_pending.Length -gt 0) {
            $queue.queue_running | ForEach-Object { Send-Workflow $_ -ErrorAction Stop }
            $queue.queue_pending | ForEach-Object { Send-Workflow $_ -ErrorAction Stop }
            
            # 保留备份
            Move-Item $queue_file "${queue_file}~" -Force -ErrorAction Ignore
            Write-Host "✅ 队列恢复完成" -ForegroundColor Green
        }
        else {
            Write-Host "ℹ️ 队列文件为空，无需恢复" -ForegroundColor Gray
        }
    }
    
    # 队列恢复完成，启用备份功能
    Write-Host "🔔 启用队列自动备份功能" -ForegroundColor Green
    $sharedState.EnableBackup = $true
    Write-Host "⏰ 备份配置: 防抖间隔 ${backup_debounce_interval}秒, 最大延迟 ${max_backup_delay}秒" -ForegroundColor Gray
    
    # 等待进程退出
    Write-Host "🔍 监控运行中..." -ForegroundColor Cyan
    # XXX: $process.WaitForExit() 会阻塞事件循环，导致 stderr 事件不处理
    while (-not $process.HasExited) {
        Start-Sleep -Milliseconds 10
    }
    $exitCode = $process.ExitCode
    Write-Host "🔚 进程已退出，退出码: $exitCode" -ForegroundColor Cyan
    # 删除进程信息文件
    if (Test-Path $info_file) {
        Remove-Item $info_file -ErrorAction SilentlyContinue
    }
    exit $exitCode
}
finally {
    # 清理资源
    Write-Host "🧹 清理资源..." -ForegroundColor Gray
    Unregister-Event -SourceIdentifier $stdoutEvent.Name -ErrorAction SilentlyContinue
    Unregister-Event -SourceIdentifier $stderrEvent.Name -ErrorAction SilentlyContinue
    
    if ($sharedState.BackupTimer) {
        $sharedState.BackupTimer.Dispose()
    }
}

#endregion
