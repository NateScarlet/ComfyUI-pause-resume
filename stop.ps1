$info_file = "$PSScriptRoot\.process_info"
$lock_file = "$PSScriptRoot\.stop_lock"
$force = $true

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
    
        $targetProcess = Get-Process -Id $info.PID -ErrorAction SilentlyContinue
        # 多重校验
        if ($targetProcess -and 
            $targetProcess.ProcessName -eq $info.ProcessName -and
            $targetProcess.StartTime.Ticks -eq $info.StartTimeTicks) {   
        
            if ($force) {
                # 保存所有任务，进行中任务之后要从头开始，用户可以自己选择合适的时机停止
                yara save -wr last_session
                if ($LASTEXITCODE) {
                    exit $LASTEXITCODE
                }
            }
            else {
                # 保存后续任务
                yara save last_session
                if ($LASTEXITCODE) {
                    exit $LASTEXITCODE
                }
        
                # 清空刚才已经保存的任务
                try {
                    Invoke-WebRequest -Uri "${url}/queue" -Method 'POST' -ContentType 'application/json' -Body '{"clear":true}' -UseBasicParsing -ErrorAction Stop | Out-Null
                }
                catch {
                    Write-Warning "清空队列失败: $($_.Exception.Message)"
                }
        
                # 等待正在处理的任务完成
                yara wait
                if ($LASTEXITCODE) {
                    exit $LASTEXITCODE
                }
            }
        
            # 停止进程
            Stop-Process $info.PID
        }
        Remove-Item $info_file -Force
    }
}
finally {
    # 始终释放锁（即使操作失败）
    Remove-Item $lock_file -Force -ErrorAction SilentlyContinue
}
