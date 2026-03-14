#Requires -Version 7.0
$ErrorActionPreference = "Stop"

#region 配置


# 加载 .env 文件
if (Test-Path "$PSScriptRoot\.env") {
    Get-Content "$PSScriptRoot\.env" | ForEach-Object {
        $line = $_.Trim()
        if ($line -and $line[0] -ne '#') {
            $parts = $line -split '=', 2
            if ($parts.Count -eq 2) {
                $name = $parts[0].Trim()
                $value = $parts[1].Trim()
                # 去除可能的引号
                if ($value.Length -ge 2 -and (($value.StartsWith('"') -and $value.EndsWith('"')) -or ($value.StartsWith("'") -and $value.EndsWith("'")))) {
                    $value = $value.Substring(1, $value.Length - 2)
                }
                [Environment]::SetEnvironmentVariable($name, $value, "Process")
            }
        }
    }
}

$port = $env:COMFYUI_PORT ?? 8188
$url = "http://127.0.0.1:$port"
$info_file = "$PSScriptRoot\.process_info"
$queue_file = "$PSScriptRoot\queue.json"
$program = "$PSScriptRoot\python_embeded\python.exe"
$extra_args = if ($env:COMFYUI_EXTRA_ARGS) { $env:COMFYUI_EXTRA_ARGS -split "\s+" } else { @() }
$program_args = @("-s", "ComfyUI\main.py", "--port", $port) + $extra_args
$backup_debounce_interval_secs = $env:COMFYUI_BACKUP_DEBOUNCE_SEC ?? 30
$max_backup_delay_secs = $env:COMFYUI_MAX_BACKUP_DELAY_SEC ?? 300
$restart_delay_secs = $env:COMFYUI_RESTART_DELAY_SEC ?? 10
$idle_program = $env:COMFYUI_IDLE_PROGRAM
$busy_program = $env:COMFYUI_BUSY_PROGRAM

#endregion

#region 辅助函数
function Wait-ServerReady {
    param([int]$Timeout = 300)
    
    $interval = 1
    $elapsed = 0

    Write-Host "⌛ 等待服务启动 ($url)..." -ForegroundColor Cyan

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
        [PSObject]$workflow,
        [System.Net.Http.HttpClient]$HttpClient
    )
    $number, $id, $prompt, $extra_data, $_ = $workflow
    $body = @{
        # 忽略序号让服务端重新排序
        # number     = $number 
        prompt     = $prompt
        prompt_id  = $id
        extra_data = $extra_data
    }
    if ($extra_data.client_id) {
        $body.client_id = $extra_data.client_id
    }
    $body = $body | ConvertTo-Json -Compress -Depth 100
    
    if ($HttpClient) {
        $content = New-Object System.Net.Http.StringContent($body, [System.Text.Encoding]::UTF8, "application/json")
        try {
            $task = $HttpClient.PostAsync("/prompt", $content)
            $task.Wait()
            $response = $task.Result
            
            if (-not $response.IsSuccessStatusCode) {
                Write-Error "工作流入列失败 状态码: $($response.StatusCode)"
            }
        }
        finally {
            if ($null -ne $content) {
                $content.Dispose()
            }
        }
        return
    }

    $response = Invoke-WebRequest -Uri "$url/prompt" -Method Post -Body $body -ContentType "application/json"
    if ($response.StatusCode -ne 200) {
        Write-Error "工作流入列失败 状态码: $($response.StatusCode), 响应: $($response.Content)"
    }
}

# 备份调度器类
class BackupScheduler {
    [datetime]$LastExecuted
    [System.Timers.Timer]$Timer
    [bool]$Enabled 
    [bool]$Scheduled = $false
    [int]$MaxDelaySecs
    [string]$QueueFile
    [string]$QueueTempFile
    [string]$Url
    [int]$LastQueueSize = -1
    [array]$PendingWorkflows = @()
    [int]$IgnoreCount = 0
    [System.Net.Http.HttpClient]$HttpClient
    [object]$SyncRoot = [object]::new()

    BackupScheduler([int]$debounceIntervalSecs, [int]$maxDelaySecs, [string]$queueFile, [string]$url) {
        $this.MaxDelaySecs = $maxDelaySecs
        $this.QueueFile = $queueFile
        $this.QueueTempFile = "$($queueFile).$([System.IO.Path]::GetRandomFileName())"
        $this.Url = $url
        $this.Timer = New-Object System.Timers.Timer
        $this.Timer.Interval = $debounceIntervalSecs * 1000
        $this.Timer.AutoReset = $false

        $this.HttpClient = New-Object System.Net.Http.HttpClient
        $this.HttpClient.BaseAddress = [Uri]$url

        Register-ObjectEvent -InputObject $this.Timer -EventName Elapsed  -MessageData $this  -Action {
            try {
                $scheduler = $Event.MessageData
                [System.Threading.Monitor]::Enter($scheduler.SyncRoot)
                try {
                    if ($scheduler.Scheduled) {
                        $scheduler.Scheduled = $false
                        Write-Host "计时器触发备份"
                        $scheduler.ExecuteLocked()
                    }
                }
                finally {
                    [System.Threading.Monitor]::Exit($scheduler.SyncRoot)
                }
            }
            catch {
                Write-Host "备份计时器回调出错: $_" -ForegroundColor Yellow
            }
        }
    }

    [void]Schedule([bool]$immediate = $false) {
        [System.Threading.Monitor]::Enter($this.SyncRoot)
        try {
            $this.ScheduleLocked($immediate)
        }
        finally {
            [System.Threading.Monitor]::Exit($this.SyncRoot)
        }
    }

    [void]Execute() {
        [System.Threading.Monitor]::Enter($this.SyncRoot)
        try {
            $this.ExecuteLocked()
        }
        finally {
            [System.Threading.Monitor]::Exit($this.SyncRoot)
        }
    }

    # 内部调用的执行逻辑，假设已持有锁
    [void]ExecuteLocked() {
        if (-not $this.Enabled) {
            return 
        }
        if ($this.IgnoreCount -gt 0) {
            # 忽略备份
            $this.IgnoreCount --
            return
        }
        # 保存 PendingWorkflows 的快照，避免在网络 IO 时持有锁太久
        $pending = $this.PendingWorkflows
        $this.LastExecuted = Get-Date
        
        # 释放锁以进行网络操作（可选，但推荐）
        [System.Threading.Monitor]::Exit($this.SyncRoot)
        try {
            Write-Host "💾 备份队列到 $($this.QueueFile)" -ForegroundColor Yellow

            $task = $this.HttpClient.GetAsync("/queue")
            $task.Wait()
            $response = $task.Result
            
            if (-not $response.IsSuccessStatusCode) {
                throw "获取队列失败: $($response.StatusCode)"
            }

            $contentTask = $response.Content.ReadAsStringAsync()
            $contentTask.Wait()
            $json = $contentTask.Result
            $data = $json | ConvertFrom-Json -ErrorAction Stop
            
            # 将未恢复的任务附加到queue_pending后面
            if ($pending.Length -gt 0) {
                Write-Host "📋 附加 $($pending.Length) 个未恢复任务到备份队列" -ForegroundColor Cyan
                $data.queue_pending = $data.queue_pending + $pending
            }
            
            $queueSize = $data.queue_running.Length + $data.queue_pending.Length
            
            # 将修改后的数据写回临时文件
            $data | ConvertTo-Json -Compress -Depth 100 | Set-Content $this.QueueTempFile -Force
            
            Move-Item $this.QueueTempFile $this.QueueFile -Force -ErrorAction Stop
            Write-Host "✅ 队列备份完成 ($($queueSize) 任务)" -ForegroundColor Green
            
            # 重新获取锁以更新状态
            [System.Threading.Monitor]::Enter($this.SyncRoot)
            try {
                $this.LastQueueSize = $queueSize
            }
            finally {
                [System.Threading.Monitor]::Exit($this.SyncRoot)
            }
        }
        catch {
            Write-Host "❌ 队列备份失败: $($_.Exception.Message)" -ForegroundColor Red
        }
        finally {
            # 重新获取锁以便外部的 finally 能够正确释放它
            [System.Threading.Monitor]::Enter($this.SyncRoot)
        }
    }
    [void]UpdatePending([array]$pending, [int]$ignoreIncrement) {
        [System.Threading.Monitor]::Enter($this.SyncRoot)
        try {
            $this.PendingWorkflows = $pending
            $this.IgnoreCount += $ignoreIncrement
        }
        finally {
            [System.Threading.Monitor]::Exit($this.SyncRoot)
        }
    }

    [int]GetQueueSize() {
        [System.Threading.Monitor]::Enter($this.SyncRoot)
        try {
            return $this.LastQueueSize
        }
        finally {
            [System.Threading.Monitor]::Exit($this.SyncRoot)
        }
    }

    [void]SetQueueSize([int]$size) {
        [System.Threading.Monitor]::Enter($this.SyncRoot)
        try {
            $this.LastQueueSize = $size
        }
        finally {
            [System.Threading.Monitor]::Exit($this.SyncRoot)
        }
    }

    [void]SetEnabled([bool]$enabled) {
        [System.Threading.Monitor]::Enter($this.SyncRoot)
        try {
            $this.Enabled = $enabled
            if (-not $enabled) {
                $this.Scheduled = $false
                $this.IgnoreCount = 0
                $this.PendingWorkflows = @()
            }
        }
        finally {
            [System.Threading.Monitor]::Exit($this.SyncRoot)
        }
    }

    [datetime]GetLastExecuted() {
        [System.Threading.Monitor]::Enter($this.SyncRoot)
        try { return $this.LastExecuted } finally { [System.Threading.Monitor]::Exit($this.SyncRoot) }
    }

    [void]ScheduleIfMatch([datetime]$timeGenerated, [string]$data) {
        [System.Threading.Monitor]::Enter($this.SyncRoot)
        try {
            if ($timeGenerated -gt $this.LastExecuted) {
                $this.ScheduleLocked($data -match "got prompt|Prompt executed in")
            }
        }
        finally {
            [System.Threading.Monitor]::Exit($this.SyncRoot)
        }
    }

    # 内部调用的调度逻辑，假设已持有锁
    [void]ScheduleLocked([bool]$immediate) {
        $this.Scheduled = $false
        $this.Timer.Stop()

        if (-not $immediate -and $this.LastExecuted.Ticks -gt 0) {
            $currentTime = Get-Date
            $sinceLastOutput = ($currentTime - $this.LastExecuted).TotalSeconds
            if ($sinceLastOutput -gt $this.MaxDelaySecs) {
                $immediate = $true
                Write-Host "最大时长触发备份（距离上次备份：$sinceLastOutput 秒）"
            }
        }
        if ($immediate) {
            $this.ExecuteLocked()
            return
        }
        
        $this.Scheduled = $true
        $this.Timer.Start()
    }


    [void]Dispose() {
        $this.Timer.Dispose()
        $this.HttpClient.Dispose()
    }
}

if (-not ("PowerManagement_54709e2a07a2" -as [type])) {
    try {
        Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public class PowerManagement_54709e2a07a2 {
    [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
    public static extern uint SetThreadExecutionState(uint esFlags);
    public const uint ES_CONTINUOUS = 0x80000000;
    public const uint ES_SYSTEM_REQUIRED = 0x00000001;
    
    public static void PreventSleep() {
        SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED);
    }
    
    public static void AllowSleep() {
        SetThreadExecutionState(ES_CONTINUOUS);
    }
}
'@
    } catch {
        Write-Host "⚠️ 定义 PowerManagement 类时出错：$_" -ForegroundColor Yellow
    }
}

class ExternalProgramManager {
    [string]$IdlePath
    [string]$BusyPath
    [System.Diagnostics.Process]$IdleProcess
    [System.Diagnostics.Process]$BusyProcess

    ExternalProgramManager([string]$idlePath, [string]$busyPath) {
        $this.IdlePath = $idlePath
        $this.BusyPath = $busyPath
    }

    [void]UpdateState([int]$queueSize) {
        if ($queueSize -eq -1) { return }

        if ($queueSize -eq 0) {
            $this.StopBusy()
            $this.StartIdle()
        } else {
            $this.StopIdle()
            $this.StartBusy()
        }
    }

    [void]StartIdle() {
        if ($this.IdlePath -and -not ($this.IdleProcess -and -not $this.IdleProcess.HasExited)) {
            Write-Host "🌙 启动闲置程序: $($this.IdlePath)" -ForegroundColor Gray
            try { $this.IdleProcess = Start-Process -FilePath $this.IdlePath -PassThru }
            catch { Write-Host "❌ 启动闲置程序失败: $_" -ForegroundColor Red; $this.IdleProcess = $null }
        }
    }

    [void]StopIdle() {
        if ($this.IdleProcess -and -not $this.IdleProcess.HasExited) {
            Write-Host "☀️ 停止闲置程序" -ForegroundColor Yellow
            try { $this.IdleProcess.Kill() } catch {}
            $this.IdleProcess = $null
        }
    }

    [void]StartBusy() {
        if ($this.BusyPath -and -not ($this.BusyProcess -and -not $this.BusyProcess.HasExited)) {
            Write-Host "🔥 启动繁忙程序: $($this.BusyPath)" -ForegroundColor Yellow
            try { $this.BusyProcess = Start-Process -FilePath $this.BusyPath -PassThru }
            catch { Write-Host "❌ 启动繁忙程序失败: $_" -ForegroundColor Red; $this.BusyProcess = $null }
        }
    }

    [void]StopBusy() {
        if ($this.BusyProcess -and -not $this.BusyProcess.HasExited) {
            Write-Host "⏸️ 退出繁忙状态，停止程序" -ForegroundColor Gray
            try { $this.BusyProcess.Kill() } catch {}
            $this.BusyProcess = $null
        }
    }

    [void]Dispose() {
        $this.StopIdle()
        $this.StopBusy()
    }
}

#endregion

#region 主程序


# 进程清理钩子：确保关闭窗口时也能结束子进程
$script:current_process = $null
$script:programManager = $null
$exit_event = "ComfyUI_Process_Exit_Handler"
Get-EventSubscriber -SourceIdentifier $exit_event -ErrorAction SilentlyContinue | Unregister-Event
Register-EngineEvent -SourceIdentifier PowerShell.Exiting -SupportEvent -Action {
    if ($script:current_process -and -not $script:current_process.HasExited) {
        $script:current_process.Kill()
    }
    if ($script:programManager) {
        $script:programManager.Dispose()
    }
}

# 检查端口占用（服务是否已运行）
if (Get-NetTCPConnection -LocalPort $port -ErrorAction SilentlyContinue) {
    Write-Host "🚫 端口 $port 正被占用" -ForegroundColor Red
    exit 1
}    

# 创建备份调度器实例
$backupScheduler = [BackupScheduler]::new($backup_debounce_interval_secs, $max_backup_delay_secs, $queue_file, $url)
$attemptCount = 0;
[bool]$wasPreventingSleep = $false

while ($true) {
    # 每次启动进程时创建程序管理器，确保 disposal 后重启可用
    $script:programManager = [ExternalProgramManager]::new($idle_program, $busy_program)
    $errorOccurred = $false
    # 创建进程对象
    $process = New-Object System.Diagnostics.Process
    $script:current_process = $process
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
    $stderrEvent = Register-ObjectEvent -InputObject $process -EventName ErrorDataReceived -MessageData $backupScheduler -Action {
        try {
            [System.Management.Automation.PSEventArgs]$e = $Event
            [BackupScheduler]$scheduler = $e.MessageData
            $data = $e.SourceEventArgs.Data
            Write-Host $data -ForegroundColor Red
            $scheduler.ScheduleIfMatch($e.TimeGenerated, $data)
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

        Write-Host "⏰ 备份配置: 防抖间隔 ${backup_debounce_interval_secs}秒, 最大延迟 ${max_backup_delay_secs}秒" -ForegroundColor Gray
        $backupScheduler.SetEnabled($true)

        # 恢复队列（如果存在）
        if (Test-Path $queue_file) {
            Write-Host "🔄 恢复队列..." -ForegroundColor Cyan
            $queue = Get-Content $queue_file -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
            Write-Host "📊 获取到 $($queue.queue_running.Length) 运行中 + $($queue.queue_pending.Length) 等待中 工作流"
        
            if ($queue.queue_running.Length -gt 0 -or $queue.queue_pending.Length -gt 0) {
                $workflows = $queue.queue_running + $queue.queue_pending

                # 进行偏移，避免一直卡在无法进行的任务上
                $startOffset = $attemptCount % $workflows.Length
                if ($startOffset) {
                    $workflows = $workflows[$startOffset..($workflows.Length - 1)] + $workflows[0..($startOffset - 1)]
                }
                
                $seenID = @{}
                # 在开始发送前先设置队列大小，确保监控循环立即阻止休眠
                $backupScheduler.SetQueueSize($workflows.Length)
                # 逐个发送工作流，每次发送后更新剩余队列
                for ($i = 0; $i -lt $workflows.Length; $i++) {
                    $workflow = $workflows[$i]
                    $id = $workflow[1]
                    if ($seenID.ContainsKey($id)) {
                        Write-Host "⏭️ 跳过重复的工作流 $($workflow[0]) ($($id)) ($i/$($workflows.Length))" -ForegroundColor Cyan            
                        continue
                    }
                    $seenID[$id] = $true
                    Write-Host "📤 发送工作流 $($workflow[0]) ($($id)) ($i/$($workflows.Length))" -ForegroundColor Cyan            
                    # 设置剩余未发送的工作流
                    $backupScheduler.UpdatePending($workflows[($i + 1)..$workflows.Length], 1)
                    Send-Workflow -workflow $workflow -HttpClient $backupScheduler.HttpClient -ErrorAction Stop
                }
                

                Write-Host "✅ 队列恢复完成" -ForegroundColor Green
            }
            else {
                Write-Host "ℹ️ 队列文件为空，无需恢复" -ForegroundColor Gray
            }
        }
    
        # 等待进程退出
        Write-Host "🔍 监控运行中..." -ForegroundColor Cyan
        while (-not $process.HasExited) {
            Start-Sleep -Seconds 1
            
            # 更新外部程序状态
            $queueSize = $backupScheduler.GetQueueSize()
            $script:programManager.UpdateState($queueSize)

            if ($queueSize -eq 0) {
                # 成功处理完所有任务，重置尝试计数
                $attemptCount = 0
                if ($wasPreventingSleep) {
                    Write-Host "💤 队列已空，允许系统休眠" -ForegroundColor Gray
                    if ("PowerManagement_54709e2a07a2" -as [type]) {
                        [PowerManagement_54709e2a07a2]::AllowSleep()
                    }
                    $wasPreventingSleep = $false
                }
            } elseif ($queueSize -gt 0) {
                if (-not $wasPreventingSleep) {
                    Write-Host "☕ 队列有任务，阻止系统休眠" -ForegroundColor Yellow
                    if ("PowerManagement_54709e2a07a2" -as [type]) {
                        [PowerManagement_54709e2a07a2]::PreventSleep()
                    }
                    $wasPreventingSleep = $true
                }
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
        $errorOccurred = $true
        Write-Host "🚨 服务出错(第 $($attemptCount+1) 次)：$_ " -ForegroundColor Red
    }
    finally {
        Write-Host "🧹 清理资源..." -ForegroundColor Gray
        # 1. 注销事件，避免重复注册或重启后旧事件仍在触发
        if ($stdoutEvent) { Unregister-Event -SourceIdentifier $stdoutEvent.Name -ErrorAction SilentlyContinue }
        if ($stderrEvent) { Unregister-Event -SourceIdentifier $stderrEvent.Name -ErrorAction SilentlyContinue }
        
        # 2. 检查电源状态
        if ($wasPreventingSleep) {
            if ("PowerManagement_54709e2a07a2" -as [type]) {
                try { [PowerManagement_54709e2a07a2]::AllowSleep() } catch {}
            }
            $wasPreventingSleep = $false
        }

        # 3. 释放程序管理器（在循环内初始化）
        if ($script:programManager) {
            $script:programManager.Dispose()
            $script:programManager = $null
        }

        # 4. 确保子进程被清理并获取最后的退出码
        if ($process) {
            if (-not $process.HasExited) {
                try { $process.Kill() } catch {}
                $exitCode = -1
            } else {
                $exitCode = $process.ExitCode
            }
        }

        # 5. 禁用备份调度器（会重置 IgnoreCount 和 PendingWorkflows，修复问题3）
        $backupScheduler.SetEnabled($false)
    }

    if (-not $errorOccurred -and $exitCode -in -1, 0) {
        exit $exitCode
    }

    Write-Host "⚠️ 非正常退出码 $exitCode，$restart_delay_secs 秒后自动重启..." -ForegroundColor Yellow
    Start-Sleep -Seconds $restart_delay_secs
    $attemptCount ++
}
#endregion
