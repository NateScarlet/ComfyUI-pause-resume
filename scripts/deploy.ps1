param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$TargetDir
)

$ErrorActionPreference = "Stop"
$SourceRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Definition)

$sourceGateway = Join-Path $SourceRoot "gateway"
$sourceStartCmd = Join-Path $SourceRoot "start.cmd"

if (-not (Test-Path $sourceGateway)) {
    Write-Error "Source gateway directory not found: $sourceGateway"
    exit 1
}
if (-not (Test-Path $sourceStartCmd)) {
    Write-Error "Source start.cmd not found: $sourceStartCmd"
    exit 1
}

$targetPython = Join-Path $TargetDir "python_embeded\python.exe"
if (-not (Test-Path $targetPython)) {
    Write-Error "Target directory '$TargetDir' does not contain python_embeded\python.exe. Please point to a ComfyUI portable root."
    exit 1
}

$targetGateway = Join-Path $TargetDir "gateway"
Write-Host "Copying gateway/ to $targetGateway ..."
robocopy $sourceGateway $targetGateway /E /XD __pycache__ /R:3 /W:3 /NFL /NDL

Write-Host "Copying start.cmd to $TargetDir ..."
Copy-Item -Path $sourceStartCmd -Destination $TargetDir -Force

Write-Host "Deploy complete. Run 'start.cmd' in '$TargetDir' to start the gateway."
