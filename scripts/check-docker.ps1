# CSC 894 Lab 2: Docker environment check for Windows.
# Usage (from the repository folder, in PowerShell):
#   powershell -ExecutionPolicy Bypass -File scripts\check-docker.ps1 | Tee-Object lab02-check.txt
# Nothing here changes your system; it only reads settings and runs the hello-world test image.

$script:passes = 0; $script:warns = 0; $script:fails = 0
function Pass($msg) { Write-Output "PASS  $msg"; $script:passes++ }
function Warn($msg, $fix) { Write-Output "WARN  $msg"; Write-Output "      -> $fix"; $script:warns++ }
function Fail($msg, $fix) { Write-Output "FAIL  $msg"; Write-Output "      -> $fix"; $script:fails++ }
function Line { Write-Output "----------------------------------------------------------------" }

Write-Output "CSC 894 Lab 2 environment check"
Write-Output ("Date: {0}   System: Windows {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm'), [Environment]::OSVersion.Version)
Line

# 1. WSL installed (Docker Desktop runs on WSL 2)
$wsl = Get-Command wsl.exe -ErrorAction SilentlyContinue
if ($wsl) {
    wsl.exe --status *> $null
    if ($LASTEXITCODE -eq 0) { Pass "WSL is installed" }
    else { Fail "WSL is installed but not set up" "Open PowerShell as Administrator, run 'wsl --update', then restart your computer." }
} else {
    Fail "WSL is not installed" "Open PowerShell as Administrator, run 'wsl --install', then restart your computer."
}

# 2. Docker command available
$docker = Get-Command docker -ErrorAction SilentlyContinue
if (-not $docker) {
    Fail "Docker command not found" "Install Docker Desktop, then close and reopen PowerShell."
    Line
    Write-Output ("Result: {0} passed, {1} warnings, {2} failed" -f $script:passes, $script:warns, $script:fails)
    exit 1
}
Pass ("Docker command found: " + (docker --version))

# 3. Docker engine running
docker info *> $null
$engine = ($LASTEXITCODE -eq 0)
if ($engine) {
    Pass ("Docker engine is running (server version " + (docker version --format '{{.Server.Version}}') + ")")
} else {
    Fail "Docker engine is not running" "Start Docker Desktop from the Start menu and wait for 'Engine running'."
}

# 4. Docker Compose v2.30 or newer
$composeRaw = (docker compose version --short 2>$null | Out-String).Trim()
if ($composeRaw) {
    $composeText = ($composeRaw -replace '^v', '').Split('-')[0]
    try { $composeVer = [version]$composeText } catch { $composeVer = $null }
    if ($composeVer -and $composeVer -ge [version]'2.30') { Pass "Docker Compose $composeText" }
    else { Fail "Docker Compose $composeText is too old" "Update Docker Desktop (Settings > Software updates)." }
} else {
    Fail "Docker Compose not found" "Reinstall or update Docker Desktop; it includes Compose."
}

if ($engine) {
    # 5. Memory available to Docker
    $memGB = [math]::Round([double](docker info --format '{{.MemTotal}}') / 1GB, 1)
    if ($memGB -ge 7.5) { Pass "Memory available to Docker: $memGB GB" }
    else { Warn "Memory available to Docker: $memGB GB" "Labs 1-3 need about 6 GB. Close other apps, or see docs\troubleshooting.md (.wslconfig)." }

    # 6. CPUs available to Docker
    $cpus = [int](docker info --format '{{.NCPU}}')
    if ($cpus -ge 4) { Pass "CPUs available to Docker: $cpus" }
    else { Warn "CPUs available to Docker: $cpus" "4 or more is recommended." }
}

# 7. Free disk space on this drive
$drive = (Get-Location).Drive
if ($drive) {
    $freeGB = [math]::Round($drive.Free / 1GB, 0)
    if ($freeGB -ge 30) { Pass "Free disk space on $($drive.Name): $freeGB GB" }
    else { Warn "Free disk space on $($drive.Name): $freeGB GB" "The course needs about 30 GB free. Delete large files or empty the Recycle Bin." }
}

# 8. Test container
if ($engine) {
    $hello = (docker run --rm hello-world 2>&1 | Out-String)
    if ($hello -match 'Hello from Docker') { Pass "Test container ran: hello-world printed 'Hello from Docker!'" }
    else { Fail "Test container did not run" "Check your internet connection, then run: docker run hello-world" }
}

Line
Write-Output ("Result: {0} passed, {1} warnings, {2} failed" -f $script:passes, $script:warns, $script:fails)
if ($script:fails -eq 0) { Write-Output "Your computer is ready for Lab 3." }
else { Write-Output "Fix each FAIL line above, then run this check again. See docs\troubleshooting.md." }
if ($script:fails -gt 0) { exit 1 }
