$envFile = Join-Path $PSScriptRoot "..\.env.ps1"
if (Test-Path $envFile) {
    . $envFile
}

& pyright
