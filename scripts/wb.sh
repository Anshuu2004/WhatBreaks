#!/usr/bin/env bash
# ==============================================================================
# WhatBreaks — Local Development Stack Management Script
# ==============================================================================
# Manages the full WhatBreaks local developer stack:
#   - PostgreSQL:         Port 5432  (Docker Compose or local PostgreSQL service)
#   - Rust Core Engine:   Port 50051 (gRPC engine bridging graph state)
#   - Kubernetes Proxy:   Port 8001  (Local authenticated kubectl proxy)
#   - Go Platform API:    Port 8080  (HTTP API & answer service)
#   - Go Health Probe:    Port 8081  (Kubernetes healthz/readyz probe)
#   - Frontend Dashboard: Port 3000  (Vite + React UI)
#
# Commands:
#   scripts/wb.sh start      Start stack components in dependency order
#   scripts/wb.sh stop       Stop only script-managed processes safely
#   scripts/wb.sh restart    Restart script-managed processes
#   scripts/wb.sh status     Check operational status of all components
#   scripts/wb.sh doctor     Diagnose environment prerequisites and connectivity
#   scripts/wb.sh --help     Show usage and command details
#
# Safety invariants:
#   - Reuses compatible existing services without restarting or killing them.
#   - Detects and replaces incompatible/broken processes safely.
#   - Tracks only processes started by this script in .run/*.pid.
#   - stop and Ctrl+C NEVER kill pre-existing or external processes.
#   - Docker uses persistent named volume: whatbreaks_postgres_data (never destroyed).
#   - Never modifies or mutates external Kubernetes clusters.
#   - Never leaks database passwords, API tokens, or secrets to stdout.
# ==============================================================================

set -meuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

RUN_DIR="${REPO_ROOT}/.run"
mkdir -p "${RUN_DIR}"

# PID files
PID_ENGINE="${RUN_DIR}/core-engine.pid"
PID_PROXY="${RUN_DIR}/kubectl-proxy.pid"
PID_GO="${RUN_DIR}/go-api.pid"
PID_FRONTEND="${RUN_DIR}/frontend.pid"

# Log files
LOG_ENGINE="${RUN_DIR}/core-engine.log"
LOG_PROXY="${RUN_DIR}/kubectl-proxy.log"
LOG_GO="${RUN_DIR}/go-api.log"
LOG_FRONTEND="${RUN_DIR}/frontend.log"

# Tracking processes started in current execution
declare -a NEWLY_STARTED_PIDS=()

# Color output helpers (respect NO_COLOR and non-interactive pipes)
if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then
  C_RESET=$'\033[0m'
  C_BOLD=$'\033[1m'
  C_GREEN=$'\033[32m'
  C_RED=$'\033[31m'
  C_YELLOW=$'\033[33m'
  C_CYAN=$'\033[36m'
  C_GRAY=$'\033[90m'
else
  C_RESET=''
  C_BOLD=''
  C_GREEN=''
  C_RED=''
  C_YELLOW=''
  C_CYAN=''
  C_GRAY=''
fi

usage() {
  cat <<EOF
${C_BOLD}WhatBreaks Stack Manager${C_RESET}

Usage:
  $(basename "$0") <command> [options]

Commands:
  start       Start all stack components in dependency order
  stop        Stop script-managed services safely
  restart     Restart script-managed services
  status      Inspect health and process status across all components
  doctor      Diagnose environment prerequisites and connectivity
  -h, --help  Show this help message and exit

Ports:
  5432        PostgreSQL State Store (Docker Compose or local service)
  50051       Rust Core Engine (gRPC)
  8001        Kubernetes API Proxy (HTTP)
  8080        Go Platform API Server (HTTP)
  8081        Go Health & Liveness Probes (HTTP)
  3000        Frontend Dashboard (HTTP)

Runtime Files:
  Logs and PIDs are saved in ${RUN_DIR}/ (gitignored)

Safety:
  This script tracks only the processes it creates. Existing external
  services running before 'start' was executed will NOT be killed or stopped.
EOF
}

# Check tool prerequisites
check_prerequisites() {
  local missing=()
  if ! command -v go >/dev/null 2>&1; then
    missing+=("Go (go) - install from https://go.dev/dl/")
  fi
  if ! command -v cargo >/dev/null 2>&1; then
    missing+=("Rust / Cargo (cargo) - install via https://rustup.rs/")
  fi
  if ! command -v node >/dev/null 2>&1; then
    missing+=("Node.js (node) - install from https://nodejs.org/")
  fi
  if ! command -v npm >/dev/null 2>&1; then
    missing+=("npm (npm) - bundled with Node.js")
  fi
  if ! command -v curl >/dev/null 2>&1; then
    missing+=("curl (curl) - required for readiness checks")
  fi
  if ! command -v nc >/dev/null 2>&1; then
    missing+=("netcat (nc) - required for TCP port checks")
  fi
  if ! command -v docker >/dev/null 2>&1; then
    missing+=("Docker (docker) - install Docker Desktop: https://www.docker.com/products/docker-desktop/")
  fi
  if ! command -v kubectl >/dev/null 2>&1; then
    missing+=("kubectl (kubectl) - install via: brew install kubectl")
  fi
  if ! command -v k3d >/dev/null 2>&1; then
    missing+=("k3d (k3d) - install via: brew install k3d")
  fi

  if [[ ${#missing[@]} -gt 0 ]]; then
    echo -e "${C_RED}${C_BOLD}Error: Missing required system tools:${C_RESET}" >&2
    for item in "${missing[@]}"; do
      echo -e "  - ${item}" >&2
    done
    exit 1
  fi
}

# Ensure Docker daemon is responsive (auto-launch on macOS with bounded timeout)
ensure_docker_daemon() {
  echo -e "\n${C_BOLD}[1/7] Ensuring Docker Daemon is Responsive...${C_RESET}"
  if ! command -v docker >/dev/null 2>&1; then
    echo -e "  ↳ [${C_RED}FAILED${C_RESET}] Docker CLI is not installed." >&2
    echo -e "\n${C_RED}${C_BOLD}Prerequisite Missing:${C_RESET} Docker is required to run the WhatBreaks local stack." >&2
    echo -e "  Please install Docker Desktop: https://www.docker.com/products/docker-desktop/" >&2
    exit 1
  fi

  if docker info >/dev/null 2>&1; then
    echo -e "  ↳ [${C_GREEN}READY${C_RESET}] Docker daemon is responsive."
    return 0
  fi

  echo -e "  Docker daemon not responding. Attempting to start Docker Desktop..."
  if [[ "$(uname)" == "Darwin" && -d "/Applications/Docker.app" ]]; then
    open -a /Applications/Docker.app || open -a Docker || true
    local docker_wait=0
    local docker_max=60
    while ! docker info >/dev/null 2>&1; do
      if [[ ${docker_wait} -ge ${docker_max} ]]; then
        echo -e "  ↳ [${C_RED}FAILED${C_RESET}] Timed out waiting for Docker daemon to become responsive (${docker_max}s)." >&2
        echo -e "\n${C_RED}${C_BOLD}Action Required:${C_RESET} Please ensure Docker Desktop is started and responsive." >&2
        exit 1
      fi
      sleep 2
      docker_wait=$((docker_wait + 2))
      printf "    Waiting for Docker socket (%ds/%ds)...\n" "${docker_wait}" "${docker_max}"
    done
    echo -e "  ↳ [${C_GREEN}READY${C_RESET}] Docker daemon is responsive."
  else
    echo -e "  ↳ [${C_RED}FAILED${C_RESET}] Docker daemon is not running." >&2
    echo -e "\n${C_RED}${C_BOLD}Action Required:${C_RESET} Please start the Docker service before launching WhatBreaks." >&2
    exit 1
  fi
}

# TCP port connection check
is_port_open() {
  local port="$1"
  nc -z 127.0.0.1 "${port}" >/dev/null 2>&1
}

# Return listening PID for a local TCP port
get_port_pid() {
  local port="$1"
  lsof -iTCP:"${port}" -sTCP:LISTEN -t 2>/dev/null | head -n 1 || true
}

# Return command line args for a PID
get_process_cmd() {
  local pid="$1"
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    ps -p "${pid}" -o args= 2>/dev/null || echo ""
  else
    echo ""
  fi
}

# Wait for TCP port with timeout
wait_for_tcp() {
  local port="$1"
  local max_seconds="$2"
  local desc="$3"

  local count=0
  while ! is_port_open "${port}"; do
    sleep 0.5
    count=$((count + 1))
    if [[ $((count / 2)) -ge "${max_seconds}" ]]; then
      echo -e "  ↳ [${C_RED}TIMEOUT${C_RESET}] Timed out waiting for ${desc} on port ${port} (${max_seconds}s)" >&2
      return 1
    fi
  done
  return 0
}

# Wait for HTTP endpoint with timeout
wait_for_http() {
  local url="$1"
  local max_seconds="$2"
  local desc="$3"

  local count=0
  while ! curl -s -f -o /dev/null "${url}" 2>/dev/null; do
    sleep 0.5
    count=$((count + 1))
    if [[ $((count / 2)) -ge "${max_seconds}" ]]; then
      echo -e "  ↳ [${C_RED}TIMEOUT${C_RESET}] Timed out waiting for ${desc} at ${url} (${max_seconds}s)" >&2
      return 1
    fi
  done
  return 0
}

# Wait for Core Engine connectivity through Go API
wait_for_core_engine_connectivity() {
  local max_seconds="$1"
  local count=0
  while true; do
    local res
    res="$(curl -s http://127.0.0.1:8080/api/v1/system/health 2>/dev/null || echo "")"
    if echo "${res}" | grep -q '"core_engine":{"status":"healthy"'; then
      return 0
    fi
    sleep 0.5
    count=$((count + 1))
    if [[ $((count / 2)) -ge "${max_seconds}" ]]; then
      echo -e "  ↳ [${C_YELLOW}WARNING${C_RESET}] Core Engine connectivity check timed out on API system health endpoint." >&2
      return 1
    fi
  done
}

# Cleanup only processes launched in the current invocation upon failure/SIGINT
cleanup_newly_started() {
  if [[ ${#NEWLY_STARTED_PIDS[@]} -gt 0 ]]; then
    echo -e "\n${C_YELLOW}Cleaning up processes started during this failed or interrupted run...${C_RESET}"
    for pid in "${NEWLY_STARTED_PIDS[@]}"; do
      if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
        echo -e "  Stopping process ${pid}..."
        kill -TERM "${pid}" 2>/dev/null || true
      fi
    done
    rm -f "${PID_ENGINE}" "${PID_PROXY}" "${PID_GO}" "${PID_FRONTEND}" "${PID_FRONTEND}.parent"
  fi
}

# Stop a single managed process safely by PID file
stop_managed_process() {
  local name="$1"
  local pid_file="$2"
  local expected_pattern="$3"
  local port="$4"

  if [[ ! -f "${pid_file}" ]]; then
    local current_pid
    current_pid="$(get_port_pid "${port}")"
    if [[ -n "${current_pid}" ]]; then
      echo -e "  ${C_GRAY}○${C_RESET} ${name}: Port ${port} is occupied by PID ${current_pid} (not started by this script; leaving running)"
    else
      echo -e "  ${C_GRAY}○${C_RESET} ${name}: Not running"
    fi
    rm -f "${pid_file}.parent"
    return 0
  fi

  local target_pid
  target_pid="$(cat "${pid_file}" 2>/dev/null || echo "")"
  target_pid="$(echo "${target_pid}" | tr -d '[:space:]')"

  if [[ -z "${target_pid}" ]]; then
    rm -f "${pid_file}" "${pid_file}.parent"
    return 0
  fi

  # Check if PID is alive
  if ! kill -0 "${target_pid}" 2>/dev/null; then
    echo -e "  ${C_GRAY}○${C_RESET} ${name}: Removing stale PID file (${target_pid} no longer running)"
    rm -f "${pid_file}" "${pid_file}.parent"
    return 0
  fi

  # Validate process command against expected pattern to prevent PID-reuse bugs
  local cmd
  cmd="$(get_process_cmd "${target_pid}")"
  if ! echo "${cmd}" | grep -qE "${expected_pattern}"; then
    echo -e "  ${C_YELLOW}⚠${C_RESET} ${name}: PID ${target_pid} has been reused by unrelated command '${cmd}'. Skipping."
    rm -f "${pid_file}" "${pid_file}.parent"
    return 0
  fi

  echo -e "  ${C_CYAN}■${C_RESET} Stopping ${name} (PID ${target_pid})..."
  kill -TERM "${target_pid}" 2>/dev/null || true

  # Also signal parent wrapper process if recorded
  if [[ -f "${pid_file}.parent" ]]; then
    local parent_pid
    parent_pid="$(cat "${pid_file}.parent" 2>/dev/null || echo "")"
    parent_pid="$(echo "${parent_pid}" | tr -d '[:space:]')"
    if [[ -n "${parent_pid}" ]] && kill -0 "${parent_pid}" 2>/dev/null; then
      kill -TERM "${parent_pid}" 2>/dev/null || true
    fi
    rm -f "${pid_file}.parent"
  fi

  local waited=0
  while kill -0 "${target_pid}" 2>/dev/null && [[ ${waited} -lt 10 ]]; do
    sleep 0.5
    waited=$((waited + 1))
  done

  if kill -0 "${target_pid}" 2>/dev/null; then
    echo -e "    Force stopping ${name} (PID ${target_pid})..."
    kill -KILL "${target_pid}" 2>/dev/null || true
  fi

  rm -f "${pid_file}"
  echo -e "  ↳ [${C_GREEN}STOPPED${C_RESET}] ${name}"
}

# ------------------------------------------------------------------------------
# Action: start
# ------------------------------------------------------------------------------
cmd_start() {
  check_prerequisites
  ensure_docker_daemon
  trap 'cleanup_newly_started; exit 1' ERR INT TERM

  echo -e "${C_BOLD}================================================================================${C_RESET}"
  echo -e "${C_BOLD}                          Starting WhatBreaks Stack                             ${C_RESET}"
  echo -e "${C_BOLD}================================================================================${C_RESET}"

  # 1. PostgreSQL (Port 5432)
  echo -e "\n${C_BOLD}[1/6] Checking PostgreSQL State Store (Port 5432)...${C_RESET}"
  if is_port_open 5432; then
    local pg_pid
    pg_pid="$(get_port_pid 5432)"
    echo -e "  ↳ [${C_GREEN}HEALTHY${C_RESET}] PostgreSQL detected listening on port 5432${pg_pid:+ (PID ${pg_pid})}"
  else
    # Try starting via Docker Compose if available
    if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
      if ! docker info >/dev/null 2>&1; then
        echo -e "  Docker daemon not responding. Attempting to start Docker Desktop..."
        if [[ "$(uname)" == "Darwin" && -d "/Applications/Docker.app" ]]; then
          open -a Docker || true
          local docker_wait=0
          while ! docker info >/dev/null 2>&1 && [[ ${docker_wait} -lt 30 ]]; do
            sleep 1
            docker_wait=$((docker_wait + 1))
          done
        fi
      fi

      if docker info >/dev/null 2>&1; then
        echo -e "  Starting PostgreSQL via Docker Compose (docker compose up -d postgres)..."
        docker compose up -d postgres
        if wait_for_tcp 5432 25 "PostgreSQL (Docker)"; then
          echo -e "  ↳ [${C_GREEN}HEALTHY${C_RESET}] PostgreSQL started via Docker Compose (persistent volume)"
        else
          echo -e "  ↳ [${C_RED}FAILED${C_RESET}] PostgreSQL container failed to bind port 5432." >&2
          exit 1
        fi
      else
        echo -e "  ↳ [${C_RED}FAILED${C_RESET}] Docker daemon is not running and port 5432 is closed." >&2
        echo -e "\n${C_RED}${C_BOLD}Action Required:${C_RESET} Start Docker Desktop or local PostgreSQL service:" >&2
        echo -e "  macOS: brew services start postgresql@16" >&2
        echo -e "  Linux: sudo systemctl start postgresql" >&2
        exit 1
      fi
    else
      echo -e "  ↳ [${C_RED}FAILED${C_RESET}] PostgreSQL is not listening on port 5432 and Docker is unavailable." >&2
      echo -e "\n${C_RED}${C_BOLD}Action Required:${C_RESET} Please start your PostgreSQL server on port 5432." >&2
      exit 1
    fi
  fi

  # 2. Rust Core Engine (Port 50051)
  echo -e "\n${C_BOLD}[2/6] Initializing Rust Core Engine (Port 50051)...${C_RESET}"
  local engine_port_pid
  engine_port_pid="$(get_port_pid 50051)"

  if [[ -n "${engine_port_pid}" ]]; then
    local cmd
    cmd="$(get_process_cmd "${engine_port_pid}")"
    if echo "${cmd}" | grep -qE "wb-core-server|core-engine"; then
      echo -e "  ↳ [${C_GREEN}REUSING${C_RESET}] Core Engine is already running on port 50051 (PID ${engine_port_pid})"
    else
      echo -e "  ↳ [${C_RED}CONFLICT${C_RESET}] Port 50051 is occupied by unrelated process PID ${engine_port_pid}:" >&2
      echo -e "    ${cmd}" >&2
      echo -e "  Please free port 50051 before starting." >&2
      exit 1
    fi
  else
    # Build if missing
    if [[ ! -x "${REPO_ROOT}/target/debug/wb-core-server" ]]; then
      echo -e "  Building Rust Core Engine binary..."
      cargo build -p wb-core-server
    fi

    echo -e "  Launching Rust Core Engine gRPC server on 127.0.0.1:50051..."
    nohup "${REPO_ROOT}/target/debug/wb-core-server" </dev/null > "${LOG_ENGINE}" 2>&1 &
    local new_engine_pid=$!
    disown "${new_engine_pid}" 2>/dev/null || true
    echo "${new_engine_pid}" > "${PID_ENGINE}"
    NEWLY_STARTED_PIDS+=("${new_engine_pid}")

    if wait_for_tcp 50051 15 "Rust Core Engine"; then
      echo -e "  ↳ [${C_GREEN}READY${C_RESET}] Rust Core Engine online (PID ${new_engine_pid}, log: .run/core-engine.log)"
    else
      echo -e "  ↳ [${C_RED}FAILED${C_RESET}] Core Engine failed to bind port 50051. Check ${LOG_ENGINE}" >&2
      cat "${LOG_ENGINE}" >&2
      exit 1
    fi
  fi

  # 3. Kubernetes Cluster Context & Proxy (Port ${K8S_PROXY_PORT:-8001})
  local proxy_port="${K8S_PROXY_PORT:-8001}"
  echo -e "\n${C_BOLD}[3/6] Inspecting Kubernetes Cluster Context...${C_RESET}"
  local k8s_context=""
  local k8s_connected=false

  # If k3d is available, check if the project's development cluster exists or needs starting
  if command -v k3d >/dev/null 2>&1; then
    local cluster_exists
    cluster_exists="$(k3d cluster list -o json 2>/dev/null | jq -r '.[].name' 2>/dev/null | grep -E '^license-service$' || echo "")"
    if [[ -z "${cluster_exists}" ]]; then
      if ! kubectl cluster-info >/dev/null 2>&1; then
        echo -e "  Creating local k3d development cluster ('license-service')..."
        k3d cluster create license-service --wait
      fi
    else
      local running_servers
      running_servers="$(k3d cluster list -o json 2>/dev/null | jq -r '.[] | select(.name=="license-service") | .serversRunning' 2>/dev/null || echo "0")"
      if [[ "${running_servers}" -eq 0 ]]; then
        echo -e "  Starting existing k3d development cluster ('license-service')..."
        k3d cluster start license-service
      fi
    fi
  fi

  if command -v kubectl >/dev/null 2>&1; then
    k8s_context="$(kubectl config current-context 2>/dev/null || echo "")"
  fi

  if [[ -n "${k8s_context}" ]]; then
    echo -e "  Active kubeconfig context: ${C_CYAN}${k8s_context}${C_RESET}"
    if is_port_open "${proxy_port}" && curl -s -f "http://127.0.0.1:${proxy_port}/readyz" >/dev/null 2>&1; then
      local proxy_pid
      proxy_pid="$(get_port_pid "${proxy_port}")"
      echo -e "  ↳ [${C_GREEN}READY${C_RESET}] Kubernetes API proxy active on port ${proxy_port}${proxy_pid:+ (PID ${proxy_pid})}"
      k8s_connected=true
    else
      if kubectl cluster-info >/dev/null 2>&1; then
        echo -e "  Starting authenticated local proxy (kubectl proxy --port=${proxy_port})..."
        nohup kubectl proxy --port="${proxy_port}" </dev/null > "${LOG_PROXY}" 2>&1 &
        local proxy_pid=$!
        disown "${proxy_pid}" 2>/dev/null || true
        echo "${proxy_pid}" > "${PID_PROXY}"
        NEWLY_STARTED_PIDS+=("${proxy_pid}")
        if wait_for_http "http://127.0.0.1:${proxy_port}/readyz" 10 "Kubernetes API Proxy"; then
          echo -e "  ↳ [${C_GREEN}READY${C_RESET}] Kubernetes API proxy online (PID ${proxy_pid})"
          k8s_connected=true
        else
          echo -e "  ↳ [${C_YELLOW}WARNING${C_RESET}] kubectl proxy did not respond ready within 10s."
        fi
      else
        echo -e "  ↳ [${C_YELLOW}OFFLINE${C_RESET}] Cluster context '${k8s_context}' is unreachable. Continuing without active discovery."
      fi
    fi
  else
    echo -e "  ↳ [${C_GRAY}NONE${C_RESET}] No kubeconfig context configured. Continuing in standalone mode."
  fi

  # 4. Go Platform API (Port 8080 & 8081)
  echo -e "\n${C_BOLD}[4/6] Initializing Go Platform API (Ports 8080 & 8081)...${C_RESET}"
  local api_pid health_pid
  api_pid="$(get_port_pid 8080)"
  health_pid="$(get_port_pid 8081)"

  local need_api_start=true
  if [[ -n "${api_pid}" || -n "${health_pid}" ]]; then
    local health_json
    health_json="$(curl -s http://127.0.0.1:8080/api/v1/system/health 2>/dev/null || echo "")"
    if echo "${health_json}" | grep -q '"database":{"status":"healthy"'; then
      echo -e "  ↳ [${C_GREEN}REUSING${C_RESET}] Go Platform API is running and compatible (PID ${api_pid:-$health_pid})"
      need_api_start=false
    else
      echo -e "  ↳ [${C_YELLOW}INCOMPATIBLE${C_RESET}] Process PID ${api_pid:-$health_pid} is outdated or has failed database connectivity."
      local target_pid="${api_pid:-$health_pid}"
      if [[ -f "${PID_GO}" && "$(cat "${PID_GO}" 2>/dev/null | tr -d '[:space:]')" == "${target_pid}" ]]; then
        stop_managed_process "Go Platform API" "${PID_GO}" "wb|cmd/wb" 8080
      else
        echo -e "    Stopping incompatible process to ensure truthful health reporting..."
        kill -TERM "${target_pid}" 2>/dev/null || true
        sleep 1
        kill -9 "${target_pid}" 2>/dev/null || true
      fi
    fi
  fi

  if [[ "${need_api_start}" == true ]]; then
    # Compile fresh binary
    echo -e "  Building Go API binary..."
    go build -o "${REPO_ROOT}/bin/wb" "${REPO_ROOT}/cmd/wb"

    echo -e "  Launching Go Platform API (--api-port=8080, --health-port=8081)..."
    nohup env TEST_DB_PASSWORD="${TEST_DB_PASSWORD:-testsecret}" \
      "${REPO_ROOT}/bin/wb" \
        --api-port=8080 \
        --health-port=8081 \
        --core-engine-addr=127.0.0.1:50051 \
        --k8s-cluster-id="${k8s_context:-k8s-cluster}" \
        --k8s-api-url="http://127.0.0.1:${proxy_port}" \
        </dev/null > "${LOG_GO}" 2>&1 &
    local new_go_pid=$!
    disown "${new_go_pid}" 2>/dev/null || true
    echo "${new_go_pid}" > "${PID_GO}"
    NEWLY_STARTED_PIDS+=("${new_go_pid}")

    if wait_for_http "http://127.0.0.1:8081/readyz" 15 "Go Health Probe" && \
       wait_for_http "http://127.0.0.1:8080/health" 15 "Go Platform API"; then
      echo -e "  ↳ [${C_GREEN}READY${C_RESET}] Go Platform API online (PID ${new_go_pid}, log: .run/go-api.log)"
      if wait_for_core_engine_connectivity 10; then
        echo -e "  ↳ [${C_GREEN}CONNECTED${C_RESET}] Core Engine gRPC connection verified via Go Platform API"
      fi
    else
      echo -e "  ↳ [${C_RED}FAILED${C_RESET}] Go Platform API failed to respond. Check ${LOG_GO}" >&2
      cat "${LOG_GO}" >&2
      exit 1
    fi
  fi

  # 5. Frontend Dashboard (Port 3000)
  echo -e "\n${C_BOLD}[5/6] Initializing Frontend Dashboard (Port 3000)...${C_RESET}"
  local fe_pid
  fe_pid="$(get_port_pid 3000)"

  if [[ -n "${fe_pid}" ]]; then
    if curl -s -f http://localhost:3000/ >/dev/null 2>&1; then
      echo -e "  ↳ [${C_GREEN}REUSING${C_RESET}] Frontend is already running on port 3000 (PID ${fe_pid})"
    else
      echo -e "  ↳ [${C_RED}CONFLICT${C_RESET}] Port 3000 is occupied by PID ${fe_pid} but not responding to HTTP." >&2
      echo -e "  Please free port 3000 before starting." >&2
      exit 1
    fi
  else
    echo -e "  Launching Frontend Vite server on http://localhost:3000..."
    nohup npm --prefix "${REPO_ROOT}/web" run dev </dev/null > "${LOG_FRONTEND}" 2>&1 &
    local new_fe_parent_pid=$!
    disown "${new_fe_parent_pid}" 2>/dev/null || true
    NEWLY_STARTED_PIDS+=("${new_fe_parent_pid}")

    if wait_for_http "http://localhost:3000/" 15 "Frontend Dashboard"; then
      local new_fe_pid
      new_fe_pid="$(get_port_pid 3000)"
      if [[ -n "${new_fe_pid}" ]]; then
        disown "${new_fe_pid}" 2>/dev/null || true
        echo "${new_fe_pid}" > "${PID_FRONTEND}"
        echo "${new_fe_parent_pid}" > "${PID_FRONTEND}.parent"
        NEWLY_STARTED_PIDS+=("${new_fe_pid}")
      else
        echo "${new_fe_parent_pid}" > "${PID_FRONTEND}"
      fi
      echo -e "  ↳ [${C_GREEN}READY${C_RESET}] Frontend Dashboard online (PID ${new_fe_pid:-$new_fe_parent_pid}, log: .run/frontend.log)"
    else
      echo -e "  ↳ [${C_RED}FAILED${C_RESET}] Frontend Dashboard failed to start. Check ${LOG_FRONTEND}" >&2
      cat "${LOG_FRONTEND}" >&2
      exit 1
    fi
  fi

  # 6. Discovery Verification & Initial Sync
  echo -e "\n${C_BOLD}[6/6] Verifying Discovery Inventory & Workspace State...${C_RESET}"
  local ws_id="00000000-0000-0000-0000-000000000001"
  local res_json
  res_json="$(curl -s "http://127.0.0.1:8080/api/v1/resources?workspace_id=${ws_id}" 2>/dev/null || echo "")"
  local total_res
  total_res="$(echo "${res_json}" | grep -o '"total":[0-9]*' | head -n 1 | cut -d: -f2 || echo "0")"

  if [[ "${total_res:-0}" -gt 0 ]]; then
    echo -e "  ↳ [${C_GREEN}SYNCED${C_RESET}] Workspace contains ${total_res} discovered resources in PostgreSQL."
  elif [[ "${k8s_connected}" == true ]]; then
    echo -e "  Triggering initial discovery synchronization sweep..."
    local cookie_file="${RUN_DIR}/cookie.txt"
    local csrf_token
    csrf_token="$(curl -s -c "${cookie_file}" http://127.0.0.1:8080/api/csrf-token 2>/dev/null | grep -o '"csrfToken":"[^"]*' | cut -d'"' -f4 || echo "")"
    local sync_res
    sync_res="$(curl -s -b "${cookie_file}" -X POST "http://127.0.0.1:8080/api/v1/discovery/sync?workspace_id=${ws_id}" \
      -H "Content-Type: application/json" \
      -H "X-CSRF-Token: ${csrf_token}" \
      -H "X-Workspace-ID: ${ws_id}" 2>/dev/null || echo "")"
    rm -f "${cookie_file}"
    local synced_count
    synced_count="$(echo "${sync_res}" | grep -o '"resources_total":[0-9]*' | head -n 1 | cut -d: -f2 || echo "0")"
    if [[ "${synced_count:-0}" -gt 0 ]]; then
      echo -e "  ↳ [${C_GREEN}DISCOVERED${C_RESET}] Initial sweep discovered ${synced_count} resources and synchronized state."
    else
      echo -e "  ↳ [${C_YELLOW}EMPTY${C_RESET}] Discovery completed; 0 resources found in monitored cluster scope."
    fi
  else
    echo -e "  ↳ [${C_GRAY}STANDALONE${C_RESET}] No reachable cluster connected. Dashboard will display guided connection state."
  fi

  # Clear failure trap on successful complete startup
  trap - ERR INT TERM

  echo -e "\n${C_GREEN}${C_BOLD}WhatBreaks stack started successfully!${C_RESET}"
  cmd_status
}

# ------------------------------------------------------------------------------
# Action: stop
# ------------------------------------------------------------------------------
cmd_stop() {
  echo -e "${C_BOLD}================================================================================${C_RESET}"
  echo -e "${C_BOLD}                          Stopping WhatBreaks Stack                             ${C_RESET}"
  echo -e "${C_BOLD}================================================================================${C_RESET}"

  stop_managed_process "Frontend Dashboard" "${PID_FRONTEND}" "vite|node" 3000
  stop_managed_process "Go Platform API"   "${PID_GO}"       "wb|cmd/wb"  8080
  stop_managed_process "Kubernetes Proxy"  "${PID_PROXY}"    "kubectl"    "${K8S_PROXY_PORT:-8001}"
  stop_managed_process "Rust Core Engine"  "${PID_ENGINE}"   "wb-core-server" 50051

  echo -e "\n${C_BOLD}PostgreSQL (Port 5432):${C_RESET} Preserved (data retained in persistent storage)."
  echo -e "${C_GREEN}${C_BOLD}Managed stack stopped cleanly.${C_RESET}\n"
}

# ------------------------------------------------------------------------------
# Action: restart
# ------------------------------------------------------------------------------
cmd_restart() {
  echo -e "${C_BOLD}Restarting WhatBreaks managed services...${C_RESET}\n"
  cmd_stop
  sleep 1
  cmd_start
}

# ------------------------------------------------------------------------------
# Action: doctor
# ------------------------------------------------------------------------------
cmd_doctor() {
  echo -e "${C_BOLD}================================================================================${C_RESET}"
  echo -e "${C_BOLD}                        WhatBreaks Stack Diagnostics                            ${C_RESET}"
  echo -e "${C_BOLD}================================================================================${C_RESET}"

  echo -e "\n${C_BOLD}1. System Prerequisites:${C_RESET}"
  for cmd in go cargo node npm docker kubectl curl nc; do
    if command -v "${cmd}" >/dev/null 2>&1; then
      printf "  %-12s [%bPASS%b] %s\n" "${cmd}" "${C_GREEN}" "${C_RESET}" "$(command -v "${cmd}")"
    else
      printf "  %-12s [%bWARN%b] not found\n" "${cmd}" "${C_YELLOW}" "${C_RESET}"
    fi
  done

  echo -e "\n${C_BOLD}2. Port Bindings & Services:${C_RESET}"
  for port in 5432 50051 8001 8080 8081 3000; do
    if is_port_open "${port}"; then
      local pid
      pid="$(get_port_pid "${port}")"
      printf "  Port %-6s [%bOPEN%b] listening (PID %s)\n" "${port}" "${C_GREEN}" "${C_RESET}" "${pid:--}"
    else
      printf "  Port %-6s [%bFREE%b] not listening\n" "${port}" "${C_GRAY}" "${C_RESET}"
    fi
  done

  echo -e "\n${C_BOLD}3. Telemetry Verification:${C_RESET}"
  if curl -s -f http://127.0.0.1:8080/health >/dev/null 2>&1; then
    local health_json
    health_json="$(curl -s http://127.0.0.1:8080/api/v1/system/health 2>/dev/null || echo "{}")"
    echo -e "  API Endpoint:     [${C_GREEN}HEALTHY${C_RESET}]"
    if echo "${health_json}" | grep -q '"database":{"status":"healthy"'; then
      echo -e "  Database Store:   [${C_GREEN}HEALTHY${C_RESET}] Connected (SELECT 1 OK)"
    else
      echo -e "  Database Store:   [${C_RED}FAILED${C_RESET}] Database ping verification failed"
    fi
    if echo "${health_json}" | grep -q '"core_engine":{"status":"healthy"'; then
      echo -e "  Rust Core Engine: [${C_GREEN}HEALTHY${C_RESET}] gRPC Liveness OK"
    else
      echo -e "  Rust Core Engine: [${C_RED}FAILED${C_RESET}] gRPC check failed"
    fi
  else
    echo -e "  API Endpoint:     [${C_YELLOW}OFFLINE${C_RESET}] Go Platform API not running on port 8080"
  fi

  echo -e "================================================================================\n"
}

# ------------------------------------------------------------------------------
# Action: status
# ------------------------------------------------------------------------------
cmd_status() {
  echo -e "\n${C_BOLD}================================================================================${C_RESET}"
  echo -e "${C_BOLD}                           WhatBreaks Stack Status                              ${C_RESET}"
  echo -e "${C_BOLD}================================================================================${C_RESET}"
  printf "%-24s %-8s %-12s %-14s %s\n" "Component" "Port" "PID (Owner)" "Status" "URL / Endpoint"
  echo -e "--------------------------------------------------------------------------------"

  # Query health API once if reachable for truthful statuses
  local health_json=""
  if curl -s -f http://127.0.0.1:8080/health >/dev/null 2>&1; then
    health_json="$(curl -s http://127.0.0.1:8080/api/v1/system/health 2>/dev/null || echo "")"
  fi

  # 1. PostgreSQL
  local pg_port=5432
  local pg_pid
  pg_pid="$(get_port_pid ${pg_port})"
  local pg_stat="${C_RED}DOWN${C_RESET}"
  if echo "${health_json}" | grep -q '"database":{"status":"healthy"'; then
    pg_stat="${C_GREEN}HEALTHY${C_RESET}"
  elif is_port_open ${pg_port}; then
    pg_stat="${C_YELLOW}LISTENING${C_RESET}"
  fi
  printf "%-24s %-8s %-12s %-23b %s\n" \
    "PostgreSQL" "${pg_port}" "${pg_pid:--}" "${pg_stat}" "localhost:${pg_port}"

  # 2. Rust Core Engine
  local engine_port=50051
  local engine_pid
  engine_pid="$(get_port_pid ${engine_port})"
  local engine_owner="external"
  if [[ -f "${PID_ENGINE}" && "$(cat "${PID_ENGINE}" 2>/dev/null)" == "${engine_pid}" ]]; then
    engine_owner="managed"
  fi
  local engine_stat="${C_RED}DOWN${C_RESET}"
  if echo "${health_json}" | grep -q '"core_engine":{"status":"healthy"'; then
    engine_stat="${C_GREEN}HEALTHY${C_RESET}"
  elif is_port_open ${engine_port}; then
    engine_stat="${C_YELLOW}LISTENING${C_RESET}"
  fi
  printf "%-24s %-8s %-12s %-23b %s\n" \
    "Rust Core Engine" "${engine_port}" "${engine_pid:+${engine_pid} (${engine_owner})}" "${engine_stat}" "127.0.0.1:${engine_port}"

  # 3. Kubernetes API Proxy
  local proxy_port=8001
  local proxy_pid
  proxy_pid="$(get_port_pid ${proxy_port})"
  local proxy_owner="external"
  if [[ -f "${PID_PROXY}" && "$(cat "${PID_PROXY}" 2>/dev/null)" == "${proxy_pid}" ]]; then
    proxy_owner="managed"
  fi
  local proxy_stat="${C_GRAY}STANDALONE${C_RESET}"
  if curl -s -f http://127.0.0.1:8001/readyz >/dev/null 2>&1; then
    proxy_stat="${C_GREEN}READY${C_RESET}"
  elif is_port_open ${proxy_port}; then
    proxy_stat="${C_YELLOW}LISTENING${C_RESET}"
  fi
  printf "%-24s %-8s %-12s %-23b %s\n" \
    "Kubernetes Proxy" "${proxy_port}" "${proxy_pid:+${proxy_pid} (${proxy_owner})}" "${proxy_stat}" "http://127.0.0.1:${proxy_port}/readyz"

  # 4. Go Platform API
  local api_port=8080
  local api_pid
  api_pid="$(get_port_pid ${api_port})"
  local api_owner="external"
  if [[ -f "${PID_GO}" && "$(cat "${PID_GO}" 2>/dev/null)" == "${api_pid}" ]]; then
    api_owner="managed"
  fi
  local api_stat="${C_RED}DOWN${C_RESET}"
  if [[ -n "${health_json}" ]]; then
    api_stat="${C_GREEN}HEALTHY${C_RESET}"
  elif is_port_open ${api_port}; then
    api_stat="${C_YELLOW}DEGRADED${C_RESET}"
  fi
  printf "%-24s %-8s %-12s %-23b %s\n" \
    "Go Platform API" "${api_port}" "${api_pid:+${api_pid} (${api_owner})}" "${api_stat}" "http://127.0.0.1:${api_port}"

  # 5. Go Health Probe Server
  local health_port=8081
  local health_pid
  health_pid="$(get_port_pid ${health_port})"
  local health_stat="${C_RED}DOWN${C_RESET}"
  if curl -s -f http://127.0.0.1:8081/readyz >/dev/null 2>&1; then
    health_stat="${C_GREEN}READY${C_RESET}"
  fi
  printf "%-24s %-8s %-12s %-23b %s\n" \
    "Go Health Server" "${health_port}" "${health_pid:--}" "${health_stat}" "http://127.0.0.1:${health_port}/readyz"

  # 6. Frontend Dashboard 
  local fe_port=3000
  local fe_pid
  fe_pid="$(get_port_pid ${fe_port})"
  local fe_owner="external"
  if [[ -f "${PID_FRONTEND}" && "$(cat "${PID_FRONTEND}" 2>/dev/null)" == "${fe_pid}" ]] || \
     [[ -f "${PID_FRONTEND}.parent" && "$(cat "${PID_FRONTEND}.parent" 2>/dev/null)" == "${fe_pid}" ]]; then
    fe_owner="managed"
  fi
  local fe_stat="${C_RED}DOWN${C_RESET}"
  if curl -s -f http://localhost:3000/ >/dev/null 2>&1; then
    fe_stat="${C_GREEN}HEALTHY${C_RESET}"
  fi
  printf "%-24s %-8s %-12s %-23b %s\n" \
    "Frontend Dashboard" "${fe_port}" "${fe_pid:+${fe_pid} (${fe_owner})}" "${fe_stat}" "http://localhost:${fe_port}"

  echo -e "================================================================================"
  echo -e "  Dashboard URL:  ${C_CYAN}${C_BOLD}http://localhost:3000${C_RESET}"
  echo -e "  Health API:     ${C_CYAN}http://127.0.0.1:8080/api/v1/system/health${C_RESET}"
  echo -e "================================================================================\n"
}

# ------------------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------------------
ACTION="${1:-}"

case "${ACTION}" in
  start)
    cmd_start
    ;;
  stop)
    cmd_stop
    ;;
  restart)
    cmd_restart
    ;;
  status)
    cmd_status
    ;;
  doctor)
    cmd_doctor
    ;;
  -h|--help|help)
    usage
    exit 0
    ;;
  "")
    usage
    exit 1
    ;;
  *)
    echo -e "${C_RED}Error: Unknown command '${ACTION}'${C_RESET}\n" >&2
    usage
    exit 1
    ;;
esac
