#!/usr/bin/env bash
# CSC 894 Lab 2: Docker environment check for macOS and Linux.
# Usage (from the repository folder):
#   bash scripts/check-docker.sh | tee lab02-check.txt
# Nothing here changes your system; it only reads settings and runs the hello-world test image.

set -u
passes=0; warns=0; fails=0
pass() { printf 'PASS  %s\n' "$1"; passes=$((passes + 1)); }
warn() { printf 'WARN  %s\n      -> %s\n' "$1" "$2"; warns=$((warns + 1)); }
fail() { printf 'FAIL  %s\n      -> %s\n' "$1" "$2"; fails=$((fails + 1)); }

echo "CSC 894 Lab 2 environment check"
echo "Date: $(date '+%Y-%m-%d %H:%M')   System: $(uname -sm)"
echo "----------------------------------------------------------------"

# 1. Docker command available
if command -v docker >/dev/null 2>&1; then
  pass "Docker command found: $(docker --version)"
else
  fail "Docker command not found" "Install Docker Desktop (macOS) or Docker Engine (Linux), then open a new terminal."
  echo "----------------------------------------------------------------"
  echo "Result: $passes passed, $warns warnings, $fails failed"
  exit 1
fi

# 2. Docker engine running
if docker info >/dev/null 2>&1; then
  pass "Docker engine is running (server version $(docker version --format '{{.Server.Version}}' 2>/dev/null))"
  engine=1
else
  fail "Docker engine is not running" "Start Docker Desktop and wait for 'Engine running'. On Linux: sudo systemctl start docker."
  engine=0
fi

# 3. Docker Compose v2.30 or newer
compose=$(docker compose version --short 2>/dev/null | sed 's/^v//; s/-.*//')
if [ -n "$compose" ]; then
  major=${compose%%.*}; rest=${compose#*.}; minor=${rest%%.*}
  if [ "$major" -gt 2 ] 2>/dev/null || { [ "$major" -eq 2 ] && [ "$minor" -ge 30 ]; } 2>/dev/null; then
    pass "Docker Compose $compose"
  else
    fail "Docker Compose $compose is too old" "Update Docker Desktop, or install the latest docker-compose-plugin on Linux."
  fi
else
  fail "Docker Compose not found" "Docker Desktop includes it. On Linux, install the docker-compose-plugin package."
fi

if [ "$engine" -eq 1 ]; then
  # 4. Memory available to Docker
  mem_bytes=$(docker info --format '{{.MemTotal}}' 2>/dev/null)
  mem_gb=$(awk -v b="$mem_bytes" 'BEGIN { printf "%.1f", b / 1073741824 }')
  if awk -v g="$mem_gb" 'BEGIN { exit !(g >= 7.5) }'; then
    pass "Memory available to Docker: $mem_gb GB"
  else
    warn "Memory available to Docker: $mem_gb GB" "Labs 1-3 need about 6 GB. Raise it in Docker Desktop > Settings > Resources (8 GB or more)."
  fi

  # 5. CPUs available to Docker
  cpus=$(docker info --format '{{.NCPU}}' 2>/dev/null)
  if [ "${cpus:-0}" -ge 4 ] 2>/dev/null; then
    pass "CPUs available to Docker: $cpus"
  else
    warn "CPUs available to Docker: $cpus" "4 or more is recommended. Raise it in Docker Desktop > Settings > Resources."
  fi
fi

# 6. Free disk space in this folder
free_kb=$(df -Pk . | awk 'NR == 2 { print $4 }')
free_gb=$(awk -v k="$free_kb" 'BEGIN { printf "%.0f", k / 1048576 }')
if [ "${free_gb:-0}" -ge 30 ] 2>/dev/null; then
  pass "Free disk space: $free_gb GB"
else
  warn "Free disk space: $free_gb GB" "The course needs about 30 GB free. Delete large files or empty the trash."
fi

# 7. Test container
if [ "$engine" -eq 1 ]; then
  if docker run --rm hello-world 2>/dev/null | grep -q "Hello from Docker"; then
    pass "Test container ran: hello-world printed 'Hello from Docker!'"
  else
    fail "Test container did not run" "Check your internet connection, then run: docker run hello-world"
  fi
fi

echo "----------------------------------------------------------------"
echo "Result: $passes passed, $warns warnings, $fails failed"
if [ "$fails" -eq 0 ]; then
  echo "Your computer is ready for Lab 3."
else
  echo "Fix each FAIL line above, then run this check again. See docs/troubleshooting.md."
fi
[ "$fails" -eq 0 ]
