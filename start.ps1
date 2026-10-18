[CmdletBinding()]
param(
    [switch]$LocalOnly,
    [switch]$SetupOnly,
    [switch]$Check,
    [switch]$AllowUnauthenticatedPublic
)

$ErrorActionPreference = "Stop"
$ProjectRoot = $PSScriptRoot

# Load the simple KEY=VALUE format used by .env (comments and blank lines ignored).
$EnvFile = Join-Path $ProjectRoot ".env"
if (Test-Path -LiteralPath $EnvFile) {
    foreach ($Line in Get-Content -LiteralPath $EnvFile) {
        if ($Line -match '^\s*([^#=\s]+)\s*=\s*(.*)\s*$') {
            $Value = $Matches[2].Trim('"', "'")
            $Value = $Value.Replace('${HOME}', $HOME).Replace('$HOME', $HOME)
            if ($Value.StartsWith('~/')) { $Value = Join-Path $HOME $Value.Substring(2) }
            [Environment]::SetEnvironmentVariable($Matches[1], $Value, "Process")
        }
    }
}

$VenvDir = if ($env:MCP_VENV_DIR) { $env:MCP_VENV_DIR } else { Join-Path $ProjectRoot ".venv" }
$Python = Join-Path $VenvDir "Scripts\python.exe"
if ($SetupOnly) { & (Join-Path $ProjectRoot "setup.ps1"); exit $LASTEXITCODE }
if (-not (Test-Path -LiteralPath $Python)) { & (Join-Path $ProjectRoot "setup.ps1") }
if ($Check) { & (Join-Path $ProjectRoot "setup.ps1") -Check; exit $LASTEXITCODE }

$HostAddress = if ($env:MCP_HOST) { $env:MCP_HOST } else { "127.0.0.1" }
$Port = if ($env:MCP_PORT) { [int]$env:MCP_PORT } else { 8011 }
$Transport = if ($env:MCP_TRANSPORT) { $env:MCP_TRANSPORT } else { "streamable-http" }
$McpPath = if ($env:MCP_PATH) { $env:MCP_PATH } else { "/mcp" }
$RuntimeDir = if ($env:MCP_RUNTIME_DIR) { $env:MCP_RUNTIME_DIR } else { Join-Path $ProjectRoot ".run" }
$LogDir = if ($env:MCP_LOG_DIR) { $env:MCP_LOG_DIR } else { Join-Path $RuntimeDir "logs" }
New-Item -ItemType Directory -Force -Path $RuntimeDir, $LogDir | Out-Null
if (-not $env:MCP_WORKSPACE) { $env:MCP_WORKSPACE = Join-Path $HOME "mcp_workspace" }
$env:MCP_LOG_DIR = $LogDir

if ($AllowUnauthenticatedPublic) {
    # This intentionally disables the server's bearer middleware for this launch.
    $env:MCP_BEARER_TOKEN = ""
    $env:MCP_ALLOW_UNAUTHENTICATED_PUBLIC = "1"
    Write-Warning "The public MCP endpoint will accept unauthenticated terminal-control requests."
}

if (-not $LocalOnly -and $env:MCP_SKIP_NGROK -ne "1" -and -not $env:MCP_BEARER_TOKEN -and -not $env:NGROK_TRAFFIC_POLICY_FILE -and $env:MCP_ALLOW_UNAUTHENTICATED_PUBLIC -ne "1") {
    throw "Refusing to expose a full terminal MCP without MCP_BEARER_TOKEN. Use -LocalOnly for local access."
}

$ServerLog = Join-Path $LogDir "server.log"
$ServerArgs = @((Join-Path $ProjectRoot "terminal_mcp.py"), "--transport", $Transport, "--host", $HostAddress, "--port", $Port)
$Server = Start-Process -FilePath $Python -ArgumentList $ServerArgs -PassThru -NoNewWindow -RedirectStandardOutput $ServerLog -RedirectStandardError (Join-Path $LogDir "server-error.log")
Write-Host "Local MCP endpoint: http://${HostAddress}:${Port}${McpPath}"
Write-Host "Server log: $ServerLog"

if ($LocalOnly -or $env:MCP_SKIP_NGROK -eq "1") {
    Write-Host "Press Ctrl+C to stop the server."
    try { Wait-Process -Id $Server.Id } finally { if (-not $Server.HasExited) { Stop-Process -Id $Server.Id } }
    exit $Server.ExitCode
}

if (-not (Get-Command ngrok -ErrorAction SilentlyContinue)) { Stop-Process -Id $Server.Id; throw "ngrok is not installed; use -LocalOnly or install ngrok." }
$NgrokLog = Join-Path $LogDir "ngrok.log"
$NgrokArgs = @("http", "http://127.0.0.1:$Port", "--log=stdout", "--log-format=json", "--inspect=false")
if ($env:NGROK_AUTHTOKEN) { $NgrokArgs += @("--authtoken", $env:NGROK_AUTHTOKEN) }
if ($env:NGROK_URL) { $NgrokArgs += @("--url", $env:NGROK_URL) }
if ($env:NGROK_TRAFFIC_POLICY_FILE) { $NgrokArgs += @("--traffic-policy-file", $env:NGROK_TRAFFIC_POLICY_FILE) }
$Ngrok = Start-Process -FilePath "ngrok" -ArgumentList $NgrokArgs -PassThru -NoNewWindow -RedirectStandardOutput $NgrokLog -RedirectStandardError (Join-Path $LogDir "ngrok-error.log")

$PublicUrl = $null
$Deadline = (Get-Date).AddSeconds($(if ($env:MCP_START_TIMEOUT) { [int]$env:MCP_START_TIMEOUT } else { 30 }))
while ((Get-Date) -lt $Deadline -and -not $Ngrok.HasExited) {
    if (Test-Path -LiteralPath $NgrokLog) {
        $Match = Select-String -LiteralPath $NgrokLog -Pattern 'https://[^\s"'']+' | Select-Object -Last 1
        if ($Match) { $PublicUrl = $Match.Matches[0].Value.TrimEnd('/'); break }
    }
    Start-Sleep -Milliseconds 250
    $Ngrok.Refresh()
}
if (-not $PublicUrl) {
    Stop-Process -Id $Server.Id -ErrorAction SilentlyContinue
    throw "ngrok did not publish an HTTPS URL. Check $NgrokLog and $(Join-Path $LogDir 'ngrok-error.log')."
}
Write-Host "Public MCP endpoint: $PublicUrl$McpPath"
if ($env:MCP_BEARER_TOKEN) { Write-Host "Authentication: Authorization: Bearer <MCP_BEARER_TOKEN>" }
elseif ($env:NGROK_TRAFFIC_POLICY_FILE) { Write-Host "Authentication: delegated to ngrok Traffic Policy" }
Write-Host "Press Ctrl+C to stop both processes."
try {
    while (-not $Server.HasExited -and -not $Ngrok.HasExited) {
        Start-Sleep -Seconds 1
        $Server.Refresh(); $Ngrok.Refresh()
    }
} finally {
    foreach ($Process in @($Ngrok, $Server)) { if (-not $Process.HasExited) { Stop-Process -Id $Process.Id } }
}
