$info_file = "$PSScriptRoot\.process_info"
$lock_file = "$PSScriptRoot\.stop_lock"
$queue_file = "$PSScriptRoot\queue.json"

try {
    $file = [System.IO.File]::Open($lock_file, 'CreateNew', 'Write', 'None')
    $file.Close()
}
catch {
    Write-Warning "已有停止操作在进行中（检测到锁文件 $lock_file），当前操作已退出"
    exit 0  # 静默退出而非错误
}
try {
    if (Test-Path $info_file) {
        $info = Get-Content $info_file | ConvertFrom-Json
    
        # 使用文件中的端口构建URL
        $url = "http://localhost:$($info.Port)"
    
        $process = Get-Process -Id $info.PID -ErrorAction SilentlyContinue
        # 多重校验
        if ($process -and 
            $process.ProcessName -eq $info.ProcessName -and
            $process.StartTime.Ticks -eq $info.StartTimeTicks) {   
        
            # 保存当前队列
            Invoke-WebRequest -Uri "${url}/queue" -Method Get -OutFile $queue_file -ErrorAction Stop

            # 停止进程
            Stop-Process $info.PID -ErrorAction Stop 
            Remove-Item $info_file -Force -ErrorAction Stop 
        } else {
            Write-Warning "当前对应 PID 进程不匹配，跳过处理"
        }
    }
}
finally {
    # 始终释放锁（即使操作失败）
    Remove-Item $lock_file -Force -ErrorAction SilentlyContinue
}
