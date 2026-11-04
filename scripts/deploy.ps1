#Requires -Version 7
<#
.SYNOPSIS
  把 gateway/ 与 start.cmd 部署到 ComfyUI 便携版根目录。
.DESCRIPTION
  部署位置优先取 -TargetDir；未指定时读取 git 配置 comfyui.deployPath，
  因此脚本内不含本机字面路径，可直接提交到仓库。
.PARAMETER TargetDir
  ComfyUI 便携版根目录。省略时使用 git config comfyui.deployPath。
.EXAMPLE
  git config comfyui.deployPath C:\ComfyUI
  pwsh -File scripts/deploy.ps1
.EXAMPLE
  pwsh -File scripts/deploy.ps1 -TargetDir D:\ComfyUI
#>
param(
    [Parameter(Position = 0)]
    [string]$TargetDir
)

$ErrorActionPreference = "Stop"

# robocopy 的 0-7 都表示复制成功（存在新增/改动文件等），>=8 才是失败
$ROBOCOPY_FAILURE_EXIT_CODE = 8

$SourceRoot = Split-Path -Parent $PSScriptRoot
$sourceGateway = Join-Path $SourceRoot "gateway"
$sourceStartCmd = Join-Path $SourceRoot "start.cmd"

if (-not (Test-Path -LiteralPath $sourceGateway)) {
    throw "Source gateway directory not found: $sourceGateway"
}
if (-not (Test-Path -LiteralPath $sourceStartCmd)) {
    throw "Source start.cmd not found: $sourceStartCmd"
}

# 未显式指定部署位置时从 git 配置读取，避免把本机路径写进仓库
if (-not $TargetDir) {
    $TargetDir = (git -C $SourceRoot config --get comfyui.deployPath)
    if ($LASTEXITCODE -ne 0 -or -not "$TargetDir".Trim()) {
        throw "未指定部署位置，且未配置 git config comfyui.deployPath。请先运行：git config comfyui.deployPath <ComfyUI 便携版根目录>"
    }
    $TargetDir = "$TargetDir".Trim()
}

if (-not (Test-Path -LiteralPath $TargetDir)) {
    throw "Target directory not found: $TargetDir"
}

$targetPython = Join-Path $TargetDir "python_embeded\python.exe"
if (-not (Test-Path -LiteralPath $targetPython)) {
    throw "Target directory '$TargetDir' does not contain python_embeded\python.exe. Please point to a ComfyUI portable root."
}

$targetGateway = Join-Path $TargetDir "gateway"
Write-Host "Copying gateway/ to $targetGateway ..."
robocopy $sourceGateway $targetGateway /E /XD __pycache__ /R:3 /W:3 /NFL /NDL
if ($LASTEXITCODE -ge $ROBOCOPY_FAILURE_EXIT_CODE) {
    throw "robocopy failed with exit code $LASTEXITCODE while copying gateway/ to $targetGateway"
}

Write-Host "Copying start.cmd to $TargetDir ..."
Copy-Item -LiteralPath $sourceStartCmd -Destination $TargetDir -Force

Write-Host "Deploy complete. Run 'start.cmd' in '$TargetDir' to start the gateway."
