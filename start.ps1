#region 配置

$port = $env:COMFYUI_PORT ?? 8188
$url = "http://localhost:$port"
$info_file = "$PSScriptRoot\.process_info"
$queue_file = "$PSScriptRoot\queue.json"
$program = "$PSScriptRoot\python_embeded\python.exe"
$program_args = @("-s", "ComfyUI\main.py", "--port", $port)
$backup_debounce_interval_secs = 5  
$max_backup_delay_secs = 60
$restart_delay_secs = 60

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
        prompt_id  = $id
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

# 备份调度器类
class BackupScheduler {
    [bool]$Enabled = $false
    [datetime]$LastExecute
    [System.Timers.Timer]$Timer
    [bool]$Scheduled = $false
    [int]$MaxDelaySecs
    [string]$QueueFile
    [string]$QueueTempFile
    [string]$Url
    [int]$LastBackupQueueSize = -1

    BackupScheduler([int]$debounceIntervalSecs, [int]$maxDelaySecs, [string]$queueFile, [string]$url) {
        $this.MaxDelaySecs = $maxDelaySecs
        $this.QueueFile = $queueFile
        $this.QueueTempFile = "$($queueFile).$([System.IO.Path]::GetRandomFileName())"
        $this.Url = $url
        $this.Timer = New-Object System.Timers.Timer
        $this.Timer.Interval = $debounceIntervalSecs * 1000
        $this.Timer.AutoReset = $false
        Register-ObjectEvent -InputObject $this.Timer -EventName Elapsed  -MessageData $this  -Action {
            try {
                $scheduler = $Event.MessageData
                if ($scheduler.Scheduled) {
                    $scheduler.Scheduled = $false
                    $scheduler.Execute()
                }
            }
            catch {
                Write-Host "备份计时器回调出错: $_" -ForegroundColor Yellow
            }
        }
    }

    [void]Schedule() {
        if (-not $this.Enabled) {
            return
        }
        
        $this.Timer.Stop()
        
        $currentTime = Get-Date
        $sinceLastOutput = ($currentTime - $this.LastExecute).TotalSeconds
        if ($sinceLastOutput -gt $this.MaxDelaySecs) {
            # 达到最大延迟，立即执行备份
            $this.Execute()
            return
        }
        
        $this.Scheduled = $true
        $this.Timer.Start()
    }

    [void]Execute() {
        $this.LastExecute = Get-Date
        Write-Host "💾 备份队列到 $($this.QueueFile)" -ForegroundColor Yellow

        try {
            Invoke-WebRequest -Uri "$($this.Url)/queue" -Method Get -OutFile $this.QueueTempFile -ErrorAction Stop
            $data = Get-Content $this.QueueTempFile -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
            $this.LastBackupQueueSize = $data.queue_running.Length + $data.queue_pending.Length
            Move-Item  $this.QueueTempFile $this.QueueFile -Force -ErrorAction Stop
            Write-Host "✅ 队列备份完成 ($($this.LastBackupQueueSize) 任务)" -ForegroundColor Green
        }
        catch {
            Write-Host "❌ 队列备份失败: $($_.Exception.Message)" -ForegroundColor Red
        }
    }

    [void]Dispose() {
        $this.Timer.Dispose()
    }
}

#endregion

#region 主程序

# 检查端口占用（服务是否已运行）
if (Get-NetTCPConnection -LocalPort $port -ErrorAction SilentlyContinue) {
    Write-Host "端口 $port 正被占用" -ForegroundColor Red
    exit 1
}    

# 创建备份调度器实例
$backupScheduler = [BackupScheduler]::new($backup_debounce_interval_secs, $max_backup_delay_secs, $queue_file, $url)
$errorCount = 0;
while ($true) {
    # 创建进程对象
    $process = New-Object System.Diagnostics.Process
    $process.StartInfo.FileName = $program
    $process.StartInfo.Arguments = $program_args -join " "
    $process.StartInfo.WorkingDirectory = $PSScriptRoot
    $process.StartInfo.CreateNoWindow = $true
    $process.StartInfo.RedirectStandardOutput = $true
    $process.StartInfo.RedirectStandardError = $true
    $process.StartInfo.UseShellExecute = $false


    # 标准输出处理
    $stdoutEvent = Register-ObjectEvent -InputObject $process -EventName OutputDataReceived -Action {
        $data = $Event.SourceEventArgs.Data
        Write-Host $data
    }

    # 标准错误处理（触发备份）
    $stderrEvent = Register-ObjectEvent -InputObject $process -EventName ErrorDataReceived -Action {
        try {
            $data = $Event.SourceEventArgs.Data
            Write-Host $data -ForegroundColor Red
            $backupScheduler.Schedule()
        }
        catch {
            Write-Host "STDERR事件回调出错: $_" -ForegroundColor Yellow
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

    $exitCode = 0;
    try {
        # 等待服务就绪
        Wait-ServerReady

        # 恢复队列（如果存在）
        if (Test-Path $queue_file) {
            Write-Host "🔄 恢复队列..." -ForegroundColor Cyan
            $queue = Get-Content $queue_file -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
            Write-Host "获取到 $($queue.queue_running.Length) 运行中 + $($queue.queue_pending.Length) 等待中 工作流"
        
            if ($queue.queue_running.Length -gt 0 -or $queue.queue_pending.Length -gt 0) {
                $combinedQueue = $queue.queue_running + $queue.queue_pending
                # 进行偏移，避免一直卡在无法进行的任务上
                $startOffset = $errorCount % $combinedQueue.Length
                if ($startOffset) {
                    $combinedQueue = $combinedQueue[$startOffset..($combinedQueue.Length - 1)] + $combinedQueue[0..$startOffset]
                }            
                $combinedQueue | ForEach-Object { Send-Workflow $_ -ErrorAction Stop }
        
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
        $backupScheduler.LastExecute = Get-Date
        $backupScheduler.Enabled = $true
        Write-Host "⏰ 备份配置: 防抖间隔 ${backup_debounce_interval_secs}秒, 最大延迟 ${max_backup_delay_secs}秒" -ForegroundColor Gray
    
        # 等待进程退出
        Write-Host "🔍 监控运行中..." -ForegroundColor Cyan
        # XXX: $process.WaitForExit() 会阻塞事件循环，导致 stderr 事件不处理
        while (-not $process.HasExited) {
            Start-Sleep -Seconds 1
            if ($backupScheduler.LastBackupQueueSize -eq 0) {
                # 成功处理完所有任务，重置错误计数
                $errorCount = 0
            }
        }
        $exitCode = $process.ExitCode
        Write-Host "🔚 进程已退出，退出码: $exitCode" -ForegroundColor Cyan
        # 删除进程信息文件
        if (Test-Path $info_file) {
            Remove-Item $info_file -ErrorAction SilentlyContinue
        }
  
    }
    catch {
        $errorCount += 1
        Write-Host "监控出错：$_"
    }
    finally {
        Write-Host "🧹 清理资源..." -ForegroundColor Gray
        if ($process.HasExited) {
            $exitCode = $process.ExitCode
        }
        else {
            $process.Kill()
        }
        Unregister-Event -SourceIdentifier $stdoutEvent.Name -ErrorAction SilentlyContinue
        Unregister-Event -SourceIdentifier $stderrEvent.Name -ErrorAction SilentlyContinue
        $backupScheduler.Enabled = $false
        $backupScheduler.Scheduled = $false
    }

    if ($exitCode -in -1, 0) {
        exit $exitCode
    }
    else {
        Write-Host "⚠️ 非正常退出码 $exitCode，$restart_delay_secs 秒后自动重启..." -ForegroundColor Yellow
        Start-Sleep -Seconds $restart_delay_secs
    }
}
#endregion
