[CmdletBinding()]
param(
    [switch]$Dev,
    [switch]$Test,
    [switch]$Recreate,
    [switch]$Check,
    [string]$Python = "py"
)

$ErrorActionPreference = "Stop"
$ProjectRoot = $PSScriptRoot
$VenvDir = if ($env:MCP_VENV_DIR) { $env:MCP_VENV_DIR } else { Join-Path $ProjectRoot ".venv" }
$VenvPython = Join-Path $VenvDir "Scripts\python.exe"

function Test-Runtime([string]$Executable) {
    if (-not (Test-Path -LiteralPath $Executable)) { return $false }
    & $Executable -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)"
    return $LASTEXITCODE -eq 0
}

if ($Check) {
    if (-not (Test-Runtime $VenvPython)) { throw "Python 3.11+ virtual environment is missing at $VenvDir." }
    & $VenvPython -c "import mcp, uvicorn, starlette, httpx"
    & $VenvPython -m pip check
    Write-Host "Environment is ready: $VenvPython"
    exit 0
}

if ($Recreate -and (Test-Path -LiteralPath $VenvDir)) {
    Remove-Item -LiteralPath $VenvDir -Recurse -Force
}
if (-not (Test-Runtime $VenvPython)) {
    if ($Python -eq "py") {
        $Created = $false
        foreach ($Version in @("3.14", "3.13", "3.12", "3.11")) {
            & py "-$Version" -m venv $VenvDir 2>$null
            if ($LASTEXITCODE -eq 0) { $Created = $true; break }
        }
        if (-not $Created) { throw "Could not find Python 3.11+ through the Windows Python launcher." }
    } else {
        & $Python -m venv $VenvDir
        if ($LASTEXITCODE -ne 0) { throw "Could not create a Python 3.11+ virtual environment." }
    }
}

& $VenvPython -m pip install --disable-pip-version-check --upgrade pip setuptools wheel
$Requirements = if ($Dev -or $Test) { "requirements-dev.txt" } else { "requirements.txt" }
& $VenvPython -m pip install --disable-pip-version-check -r (Join-Path $ProjectRoot $Requirements)
& $VenvPython -m pip check
if ($Test) { & $VenvPython -m pytest }
if ($LASTEXITCODE -ne 0) { throw "Setup validation failed." }
Write-Host "Setup complete: $VenvPython"
