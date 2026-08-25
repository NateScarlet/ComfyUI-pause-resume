$envFile = Join-Path $PSScriptRoot "..\.env.ps1"
if (Test-Path $envFile) {
    . $envFile
}

# 在受限沙箱中，系统临时目录下动态创建的子目录内部不可访问（sqlite/listdir 均被拒绝），
# 因此将 Python 的临时目录重定向到工作区内，测试代码通过 tempfile 标准机制自动生效
$scratchTmp = Join-Path $PSScriptRoot "..\.scratch\tmp"
New-Item -ItemType Directory -Force $scratchTmp | Out-Null
$env:TMPDIR = (Resolve-Path $scratchTmp).Path

# 受限沙箱中 tempfile.mkdtemp 以 0o700 权限创建的目录同样不可访问，
# 注入 sitecustomize 兼容层将临时目录创建放宽为默认权限
$env:PYTHONPATH = Join-Path $PSScriptRoot "python-sitecustomize"

# 开发统一使用项目根 .venv（pyright 通过 pyrightconfig 的 venvPath 静态锚定该环境）；
# 生产运行共享 ComfyUI 运行时，不经此环境
$pythonExe = Join-Path $PSScriptRoot "..\.venv\Scripts\python.exe"

& $pythonExe -m pyright gateway/
if ($LASTEXITCODE) {
    exit $LASTEXITCODE
}
& $pythonExe -m unittest discover -s tests -t .
$unittestExitCode = $LASTEXITCODE
Remove-Item Env:\PYTHONPATH -ErrorAction SilentlyContinue
if ($unittestExitCode) {
    exit $unittestExitCode
}
& $pythonExe -m black gateway/ --target-version py312
if ($LASTEXITCODE) {
    exit $LASTEXITCODE
}
