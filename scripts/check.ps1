$envFile = Join-Path $PSScriptRoot "..\.env.ps1"
if (Test-Path $envFile) {
    . $envFile
}

& pyright gateway/
if ($LASTEXITCODE) {
    exit $LASTEXITCODE
}
& python -m unittest discover -s tests -t .
if ($LASTEXITCODE) {
    exit $LASTEXITCODE
}
& black gateway/
if ($LASTEXITCODE) {
    exit $LASTEXITCODE
}
