$info_file = "$PSScriptRoot\.process_info"
$queue_file = "$PSScriptRoot\saved_queue.json"

if (Test-Path $info_file) {
    $info = Get-Content $info_file | ConvertFrom-Json
    # 使用文件中的端口构建URL
    $url = "http://localhost:$($info.Port)"
}
else {
    $port = $env:COMFYUI_PORT ?? 8188
    $url = "http://localhost:$port"
}

Invoke-WebRequest -Uri "${url}/queue" -Method Get -OutFile $queue_file -ErrorAction Stop
