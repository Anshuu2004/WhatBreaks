#!/usr/bin/env bash
# ==============================================================================
# WhatBreaks — Developer Test & Verification Entrypoint
# ==============================================================================
# Executes project-wide verification:
#   1. Go unit & integration tests (`go test -count=1 ./...`)
#   2. Go race detector on critical packages (internal/api, answer, state)
#   3. Rust workspace tests (`cargo test --workspace`)
#   4. Frontend linter (`npm run lint` in web/)
#   5. Frontend production build (`npm run build` in web/)
#   6. Git diff whitespace & format check (`git diff --check`)
#   7. Optional real Kubernetes acceptance tests (explicit opt-in only)
#
# Safety invariants:
#   - NEVER touches or modifies the 'k3d-license-service' cluster.
#   - Real cluster tests require explicit --with-k8s and dedicated 'whatbreaks-e2e' context.
#   - Produces structured timing and pass/fail summary without leaking credentials.
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

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

# Options & flags
SKIP_GO=false
SKIP_RACE=false
SKIP_RUST=false
SKIP_FRONTEND=false
SKIP_LINT=false
SKIP_BUILD=false
SKIP_GIT=false
WITH_K8S=false
VERBOSE=false

usage() {
  cat <<EOF
${C_BOLD}WhatBreaks Test Suite Runner${C_RESET}

Usage:
  $(basename "$0") [options]

Suites:
  1. Go Tests            Unit and integration tests across all Go packages
  2. Go Race Tests       Race condition detector on internal/api, internal/answer, internal/state
  3. Rust Workspace      Cargo workspace test suite (wb-core-engine, wb-core-proto, wb-core-server)
  4. Frontend Lint       TypeScript & ESLint validation (oxlint in web/)
  5. Frontend Build      Production Vite & TypeScript compilation (web/dist/)
  6. Git Diff Check      Whitespace and merge artifact verification (git diff --check)
  7. Kubernetes E2E      Real Kubernetes acceptance tests (opt-in with --with-k8s)

Options:
  --skip-go              Skip Go unit and integration tests
  --skip-race            Skip Go race detector tests
  --skip-rust            Skip Rust workspace tests (cargo test)
  --skip-frontend, --skip-web
                         Skip both frontend lint and production build
  --skip-lint            Skip frontend lint only
  --skip-build           Skip frontend build only
  --skip-git             Skip git diff --check
  --with-k8s, --k8s-e2e  Opt-in to real Kubernetes acceptance tests (requires 'whatbreaks-e2e' context)
  -v, --verbose          Print full stdout/stderr from test commands
  -h, --help             Show this help message and exit

Safety Notice:
  Real Kubernetes acceptance tests are destructive and will NEVER execute against
  the existing 'k3d-license-service' cluster. They strictly require the dedicated
  'whatbreaks-e2e' kubeconfig context.
EOF
}

# Parse CLI arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-go)
      SKIP_GO=true
      shift
      ;;
    --skip-race)
      SKIP_RACE=true
      shift
      ;;
    --skip-rust)
      SKIP_RUST=true
      shift
      ;;
    --skip-frontend|--skip-web)
      SKIP_FRONTEND=true
      shift
      ;;
    --skip-lint)
      SKIP_LINT=true
      shift
      ;;
    --skip-build)
      SKIP_BUILD=true
      shift
      ;;
    --skip-git)
      SKIP_GIT=true
      shift
      ;;
    --with-k8s|--k8s-e2e)
      WITH_K8S=true
      shift
      ;;
    -v|--verbose)
      VERBOSE=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo -e "${C_RED}Error: Unknown argument '$1'${C_RESET}" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ "${SKIP_FRONTEND}" == true ]]; then
  SKIP_LINT=true
  SKIP_BUILD=true
fi

# Track test results
declare -a SUITE_NAMES=()
declare -a SUITE_STATUS=()
declare -a SUITE_DURATIONS=()
declare -a SUITE_NOTES=()

OVERALL_EXIT_CODE=0
START_ALL_TIME=$(date +%s)

# Format elapsed duration in seconds with one decimal
format_duration() {
  local seconds=$1
  printf "%ds" "${seconds}"
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
  if ! command -v git >/dev/null 2>&1; then
    missing+=("Git (git) - install via OS package manager")
  fi

  if [[ ! -f "${REPO_ROOT}/go.mod" ]]; then
    missing+=("go.mod missing at repository root (${REPO_ROOT})")
  fi
  if [[ ! -f "${REPO_ROOT}/Cargo.toml" ]]; then
    missing+=("Cargo.toml missing at repository root (${REPO_ROOT})")
  fi
  if [[ ! -f "${REPO_ROOT}/web/package.json" ]]; then
    missing+=("web/package.json missing at ${REPO_ROOT}/web")
  fi

  if [[ ${#missing[@]} -gt 0 ]]; then
    echo -e "${C_RED}${C_BOLD}Error: Missing tool prerequisites or project files:${C_RESET}" >&2
    for item in "${missing[@]}"; do
      echo -e "  - ${item}" >&2
    done
    exit 1
  fi

  # Pre-warm Rust test server fixtures if missing so Go integration tests don't timeout compiling under parallel test load
  if [[ ! -x "${REPO_ROOT}/target/debug/wb-core-server" || ! -x "${REPO_ROOT}/target/debug/examples/fixture_server" ]]; then
    cargo build --workspace --bins --examples >/dev/null 2>&1 || true
  fi
}

# Run a single suite with timing and status capture
run_suite() {
  local name="$1"
  local notes="$2"
  local cmd="$3"

  echo -e "\n${C_BOLD}Running suite:${C_RESET} ${C_CYAN}${name}${C_RESET} ..."
  local start_ts
  start_ts=$(date +%s)

  local suite_exit=0
  local temp_log
  temp_log="$(mktemp)"

  if [[ "${VERBOSE}" == true ]]; then
    # Stream in real-time and tee to log
    if ! bash -c "${cmd}" 2>&1 | tee "${temp_log}"; then
      suite_exit=1
    fi
  else
    # Run silently, save output to temp log
    if ! bash -c "${cmd}" > "${temp_log}" 2>&1; then
      suite_exit=1
    fi
  fi

  local end_ts
  end_ts=$(date +%s)
  local duration=$((end_ts - start_ts))

  SUITE_NAMES+=("${name}")
  SUITE_DURATIONS+=("$(format_duration "${duration}")")
  SUITE_NOTES+=("${notes}")

  if [[ ${suite_exit} -eq 0 ]]; then
    echo -e "  ↳ [${C_GREEN}${C_BOLD}PASS${C_RESET}] (${duration}s)"
    SUITE_STATUS+=("PASS")
  else
    echo -e "  ↳ [${C_RED}${C_BOLD}FAIL${C_RESET}] (${duration}s)"
    SUITE_STATUS+=("FAIL")
    OVERALL_EXIT_CODE=1
    if [[ "${VERBOSE}" == false ]]; then
      echo -e "${C_RED}--- Error Output (${name}) ---${C_RESET}" >&2
      cat "${temp_log}" >&2
      echo -e "${C_RED}-----------------------------------${C_RESET}" >&2
    fi
  fi
  rm -f "${temp_log}"
}

record_skipped_suite() {
  local name="$1"
  local notes="$2"
  SUITE_NAMES+=("${name}")
  SUITE_DURATIONS+=("--")
  SUITE_STATUS+=("SKIPPED")
  SUITE_NOTES+=("${notes}")
  echo -e "\n${C_BOLD}Skipping suite:${C_RESET} ${C_YELLOW}${name}${C_RESET} (${notes})"
}

# ------------------------------------------------------------------------------
# Execution Pipeline
# ------------------------------------------------------------------------------
check_prerequisites

echo -e "${C_BOLD}================================================================================${C_RESET}"
echo -e "${C_BOLD}                        WhatBreaks Verification Runner                         ${C_RESET}"
echo -e "${C_BOLD}================================================================================${C_RESET}"
echo -e "Repository root: ${REPO_ROOT}"
echo -e "Timestamp:       $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
echo -e "Options:         skip-go=${SKIP_GO}, skip-race=${SKIP_RACE}, skip-rust=${SKIP_RUST}, skip-frontend=${SKIP_FRONTEND}, with-k8s=${WITH_K8S}"

# 1. Go Unit & Integration Tests (excluding real Kubernetes cluster e2e by default)
if [[ "${SKIP_GO}" == true ]]; then
  record_skipped_suite "Go Unit & Integration Tests" "Skipped via --skip-go"
else
  run_suite \
    "Go Unit & Integration Tests" \
    "All packages (excl. real k8s e2e)" \
    "go test -count=1 -skip 'Test(Kubernetes_RealEndToEndFlow|RealKubernetes_ReconciliationLifecycle)' ./..."
fi

# 2. Go Race Tests
if [[ "${SKIP_RACE}" == true ]]; then
  record_skipped_suite "Go Race Detector" "Skipped via --skip-race"
else
  run_suite \
    "Go Race Detector" \
    "internal/api, internal/answer, internal/state" \
    "go test -race -count=1 ./internal/api ./internal/answer ./internal/state"
fi

# 3. Rust Workspace Tests
if [[ "${SKIP_RUST}" == true ]]; then
  record_skipped_suite "Rust Workspace (cargo test)" "Skipped via --skip-rust"
else
  run_suite \
    "Rust Workspace (cargo test)" \
    "wb-core-engine, proto, server" \
    "cargo test --workspace"
fi

# 4. Frontend Lint
if [[ "${SKIP_LINT}" == true ]]; then
  record_skipped_suite "Frontend Linter (oxlint)" "Skipped via option"
else
  run_suite \
    "Frontend Linter (oxlint)" \
    "web/ src files" \
    "npm --prefix web run lint"
fi

# 5. Frontend Production Build
if [[ "${SKIP_BUILD}" == true ]]; then
  record_skipped_suite "Frontend Build (tsc + vite)" "Skipped via option"
else
  run_suite \
    "Frontend Build (tsc + vite)" \
    "web/dist production bundle" \
    "npm --prefix web run build"
fi

# 6. Git Diff Whitespace Check
if [[ "${SKIP_GIT}" == true ]]; then
  record_skipped_suite "Git Diff Check" "Skipped via --skip-git"
else
  run_suite \
    "Git Diff Check" \
    "Trailing whitespace & syntax checks" \
    "git diff --check"
fi

# 7. Real Kubernetes Acceptance Tests (Opt-in only)
if [[ "${WITH_K8S}" == false ]]; then
  record_skipped_suite "Kubernetes Acceptance E2E" "Opt-in only (pass --with-k8s)"
else
  # Enforce safety constraints
  if ! command -v kubectl >/dev/null 2>&1; then
    echo -e "${C_RED}Error: --with-k8s requested but 'kubectl' CLI is not installed.${C_RESET}" >&2
    OVERALL_EXIT_CODE=1
    SUITE_NAMES+=("Kubernetes Acceptance E2E")
    SUITE_DURATIONS+=("--")
    SUITE_STATUS+=("FAIL")
    SUITE_NOTES+=("kubectl CLI not found")
  else
    active_ctx="$(kubectl config current-context 2>/dev/null || echo '')"
    if [[ "${active_ctx}" == "k3d-license-service" ]]; then
      echo -e "${C_RED}${C_BOLD}SAFETY VIOLATION:${C_RESET} Active context is 'k3d-license-service'." >&2
      echo -e "This cluster is reserved and cannot be modified by destructive tests." >&2
      echo -e "Switch to or configure the dedicated 'whatbreaks-e2e' context first." >&2
      OVERALL_EXIT_CODE=1
      SUITE_NAMES+=("Kubernetes Acceptance E2E")
      SUITE_DURATIONS+=("--")
      SUITE_STATUS+=("FAIL")
      SUITE_NOTES+=("Refused: active context is k3d-license-service")
    else
      # Check if whatbreaks-e2e context exists
      all_contexts="$(kubectl config get-contexts -o name 2>/dev/null || echo '')"
      if ! echo "${all_contexts}" | grep -q "^whatbreaks-e2e$"; then
        echo -e "${C_RED}Error: Dedicated context 'whatbreaks-e2e' not found in kubectl contexts.${C_RESET}" >&2
        echo -e "Available contexts:\n${all_contexts}" >&2
        OVERALL_EXIT_CODE=1
        SUITE_NAMES+=("Kubernetes Acceptance E2E")
        SUITE_DURATIONS+=("--")
        SUITE_STATUS+=("FAIL")
        SUITE_NOTES+=("Context 'whatbreaks-e2e' not configured")
      else
        run_suite \
          "Kubernetes Acceptance E2E" \
          "Targeting whatbreaks-e2e context" \
          "kubectl config use-context whatbreaks-e2e >/dev/null 2>&1 && go test -v -count=1 -run 'Test(Kubernetes_RealEndToEndFlow|RealKubernetes_ReconciliationLifecycle)' ./test/acceptance"
      fi
    fi
  fi
fi

# ------------------------------------------------------------------------------
# Final Summary Table
# ------------------------------------------------------------------------------
TOTAL_PASSED=0
TOTAL_FAILED=0
TOTAL_SKIPPED=0

for st in "${SUITE_STATUS[@]}"; do
  if [[ "${st}" == "PASS" ]]; then
    TOTAL_PASSED=$((TOTAL_PASSED + 1))
  elif [[ "${st}" == "FAIL" ]]; then
    TOTAL_FAILED=$((TOTAL_FAILED + 1))
  elif [[ "${st}" == "SKIPPED" ]]; then
    TOTAL_SKIPPED=$((TOTAL_SKIPPED + 1))
  fi
done

TOTAL_END_TIME=$(date +%s)
TOTAL_ELAPSED=$((TOTAL_END_TIME - START_ALL_TIME))

echo -e "\n${C_BOLD}================================================================================${C_RESET}"
echo -e "${C_BOLD}                            Verification Summary Table                          ${C_RESET}"
echo -e "${C_BOLD}================================================================================${C_RESET}"
printf "%-34s %-12s %-10s %s\n" "Suite" "Status" "Duration" "Notes"
echo -e "--------------------------------------------------------------------------------"

for i in "${!SUITE_NAMES[@]}"; do
  s_name="${SUITE_NAMES[$i]}"
  s_stat="${SUITE_STATUS[$i]}"
  s_dur="${SUITE_DURATIONS[$i]}"
  s_note="${SUITE_NOTES[$i]}"

  case "${s_stat}" in
    PASS)
      s_stat_colored="${C_GREEN}${C_BOLD}PASS${C_RESET}"
      ;;
    FAIL)
      s_stat_colored="${C_RED}${C_BOLD}FAIL${C_RESET}"
      ;;
    SKIPPED)
      s_stat_colored="${C_YELLOW}SKIPPED${C_RESET}"
      ;;
    *)
      s_stat_colored="${s_stat}"
      ;;
  esac

  printf "%-34s %-21b %-10s %s\n" "${s_name}" "${s_stat_colored}" "${s_dur}" "${s_note}"
done

echo -e "================================================================================"

if [[ ${OVERALL_EXIT_CODE} -eq 0 ]]; then
  echo -e "${C_GREEN}${C_BOLD}RESULT: ALL CHECKS PASSED${C_RESET} (${TOTAL_PASSED} passed, ${TOTAL_FAILED} failed, ${TOTAL_SKIPPED} skipped) in ${TOTAL_ELAPSED}s"
else
  echo -e "${C_RED}${C_BOLD}RESULT: VERIFICATION FAILED${C_RESET} (${TOTAL_PASSED} passed, ${TOTAL_FAILED} failed, ${TOTAL_SKIPPED} skipped) in ${TOTAL_ELAPSED}s"
fi
echo -e "================================================================================\n"

exit ${OVERALL_EXIT_CODE}
