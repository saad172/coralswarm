# dev-launch.ps1
# Start all CoralSwarm dev services for local development.
#
# Opens three windows:
#   1. Fly.io DB proxy  -> localhost:5432
#   2. Rust backend     -> http://localhost:8080
#   3. Next.js frontend -> http://localhost:3000

$Root = Split-Path $PSScriptRoot -Parent
$EnvPath = "$Root\.env"

# --- Generate MASTER_KEK if missing ---
$envContent = Get-Content $EnvPath -Raw
if ($envContent -notmatch "MASTER_KEK") {
    $rng = New-Object System.Security.Cryptography.RNGCryptoServiceProvider
    $bytes = New-Object byte[] 32
    $rng.GetBytes($bytes)
    $kek = ($bytes | ForEach-Object { $_.ToString("x2") }) -join ""
    Add-Content -Path $EnvPath -Value "MASTER_KEK=$kek"
    Write-Host "Generated new MASTER_KEK"
}

# --- Load .env vars into a hash ---
$envVars = @{}
Get-Content $EnvPath | ForEach-Object {
    if ($_ -match "^([A-Za-z_][A-Za-z0-9_]*)=(.*)$") {
        $envVars[$Matches[1]] = $Matches[2]
    }
}

# Override DATABASE_URL for local proxy instead of flycast
$dbUrl = $envVars["DATABASE_URL"]
if ($dbUrl -match "://([^:]+):([^@]+)@[^/]+/(.+)") {
    $dbUser = $Matches[1]
    $dbPass = $Matches[2]
    $dbName = $Matches[3]
    $envVars["DATABASE_URL"] = "postgres://${dbUser}:${dbPass}@localhost:5432/${dbName}"
}

# Build semicolon-joined env-set commands for backend window
$envLines = New-Object System.Collections.Generic.List[string]
foreach ($pair in $envVars.GetEnumerator()) {
    $safeVal = $pair.Value -replace '"', '`"'
    $envLines.Add('$env:' + $pair.Key + ' = "' + $safeVal + '"')
}
$envSetCmds = $envLines -join "; "

# --- Window 1: flyctl DB proxy ---
Write-Host "Starting Fly.io DB proxy..."
$proxyArgs = @("-NoExit", "-NoProfile", "-Command", "flyctl proxy 5432 -a coralswarm-db")
Start-Process powershell -ArgumentList $proxyArgs

# --- Wait for port 5432 ---
Write-Host "Waiting for DB proxy on localhost:5432 (max 30s)..."
$proxyReady = $false
$waited = 0
while ($waited -lt 30) {
    $tcp = $null
    try {
        $tcp = New-Object System.Net.Sockets.TcpClient
        $tcp.Connect("127.0.0.1", 5432)
        $proxyReady = $true
        Write-Host "DB proxy ready after ${waited}s"
        break
    } catch {
        Start-Sleep 1
        $waited++
    } finally {
        if ($tcp) { $tcp.Close() }
    }
}
if (-not $proxyReady) {
    Write-Warning "DB proxy did not become ready in 30s -- backend may fail to connect"
}

# --- Window 2: Rust backend ---
Write-Host "Starting Rust backend..."
$backendRoot = "$Root\backend"
$backendCmd = $envSetCmds + "; Write-Host 'Building backend...'; Set-Location '" + $backendRoot + "'; cargo run"
$backendArgs = @("-NoExit", "-NoProfile", "-Command", $backendCmd)
Start-Process powershell -ArgumentList $backendArgs

# --- Window 3: Next.js dev server ---
Write-Host "Starting Next.js dev server..."
$webRoot = "$Root\web"
$webCmd = "Set-Location '" + $webRoot + "'; pnpm dev"
$webArgs = @("-NoExit", "-NoProfile", "-Command", $webCmd)
Start-Process powershell -ArgumentList $webArgs

Write-Host ""
Write-Host "All three services launched in separate windows."
Write-Host "  Backend:   http://localhost:8080"
Write-Host "  Frontend:  http://localhost:3000"
