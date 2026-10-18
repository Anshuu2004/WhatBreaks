# WhatBreaks Developer & Contributor Guide

This guide describes how to develop, build, test, and contribute to WhatBreaks (WB).

---

## 1. Repository Layout & Component Architecture

```text
WhatBreaks/
├── cmd/
│   └── wb/                 # Go Platform API & health probe binary entrypoint
├── core-engine/            # Provider-agnostic Rust Core Engine library (graph, traversal, impact)
├── core-engine-proto/      # Tonic gRPC code generation for Rust
├── core-engine-server/     # Rust gRPC server binary (`wb-core-server`)
├── internal/               # Go internal packages
│   ├── answer/             # Impact analysis answer service & validation
│   ├── api/                # REST HTTP router, middleware pipeline, diagnostics
│   ├── auth/               # Workspace access policies and machine token validation
│   ├── collector/k8s/      # Read-only Kubernetes REST collector & normalizer
│   ├── config/             # Environment & configuration loading
│   ├── coreclient/         # gRPC client connecting to Rust Core Engine
│   ├── database/           # PostgreSQL connection pool & health ping
│   ├── discovery/          # Discovery coordinator & sweep synchronization
│   ├── health/             # Probe HTTP server (/readyz, /healthz)
│   ├── lifecycle/          # Graceful shutdown & in-flight work tracker
│   ├── logging/            # Structured JSON / console logger with secret scrubbing
│   ├── platform/           # Subsystem lifecycle orchestrator
│   ├── scheduler/          # Background cron & periodic interval scheduler
│   └── state/              # PostgreSQL store, schema, reconciliation, & recovery
├── proto/
│   ├── gen.sh              # Protobuf code generation script for Go
│   └── wb/core/v1/         # Protobuf service definitions (`core.proto`)
├── scripts/
│   ├── test.sh             # Developer test suite runner
│   └── wb.sh               # Local development stack manager
├── test/
│   └── acceptance/         # End-to-end integration & acceptance tests
└── web/                    # React 19 + TypeScript + Vite frontend dashboard
```

---

## 2. Technology Stack

- **Go Platform**: Go `1.22+` (stdlib HTTP, `pgx`/`lib/pq` compatible database driver, Google gRPC / Protobuf).
- **Rust Core Engine**: Rust `1.75+` (edition 2021, Tokio async runtime, Tonic gRPC, Serde, Petgraph-inspired traversal).
- **Frontend Dashboard**: React 19, TypeScript 5.8+, Vite 6+, Lucide icons, Oxlint linter.
- **Database**: PostgreSQL 16 (persisted via Docker named volume `whatbreaks_postgres_data`).

---

## 3. Building Binaries

### A. Build the Go Platform API Binary
```bash
go build -o bin/wb ./cmd/wb
```
Verify the build:
```bash
./bin/wb --version
```

### B. Build the Rust Core Engine Server
```bash
cargo build -p wb-core-server
```
The compiled binary is located at `./target/debug/wb-core-server`.

### C. Build the Frontend Production Bundle
```bash
cd web
npm install
npm run build
```
Compiled static assets are generated in `web/dist/`.

---

## 4. Running the Test Suite

WhatBreaks includes test suites across Go, Rust, and TypeScript.

### Method 1: The Unified Test Runner (Recommended)
Use [`scripts/test.sh`](../scripts/test.sh) to run the full verification pipeline:

```bash
./scripts/test.sh
```

Supported test suite options:
```bash
./scripts/test.sh --skip-rust       # Skip Rust workspace tests
./scripts/test.sh --skip-go         # Skip Go unit and integration tests
./scripts/test.sh --skip-race       # Skip Go race detector
./scripts/test.sh --skip-frontend   # Skip frontend lint and build checks
./scripts/test.sh -v                # Verbose output
```

### Method 2: Running Individual Test Suites

#### 1. Go Unit & Integration Tests
```bash
go test -count=1 ./...
```

#### 2. Go Race Condition Detection
Run the Go race detector on concurrency-critical packages:
```bash
go test -race -count=1 ./internal/api ./internal/answer ./internal/state
```

#### 3. Rust Workspace Tests
Run unit and integration tests across all Rust crates:
```bash
cargo test --workspace
```

#### 4. Frontend Lint & Build Checks
In the `web/` directory:
```bash
# Run the Oxlint linter
npm run lint

# Compile TypeScript and build production bundle
npm run build

# Run frontend component unit tests
npm test
```

#### 5. Git Whitespace & Format Check
Ensure no trailing whitespace or merge conflict markers remain:
```bash
git diff --check
```

---

## 5. Optional Real Kubernetes Acceptance Tests

WhatBreaks includes end-to-end acceptance tests that run against real Kubernetes clusters ([`test/acceptance/k8s_e2e_test.go`](../test/acceptance/k8s_e2e_test.go)).

> [!CAUTION]
> **Safety Guard**:
> Real Kubernetes acceptance tests create and delete test namespaces. To prevent accidental disruption to existing developer environments:
> - Acceptance tests will **NEVER** run against your existing clusters (such as `k3d-license-service`).
> - Acceptance tests strictly require an explicit opt-in flag (`--with-k8s`) and a dedicated kubeconfig context named `whatbreaks-e2e`.

### How to Run Acceptance Tests Safely:
1. Create a dedicated test cluster:
   ```bash
   k3d cluster create whatbreaks-e2e --wait
   ```
2. Run the test script with the opt-in flag:
   ```bash
   ./scripts/test.sh --with-k8s
   ```
3. Tear down the test cluster when finished:
   ```bash
   k3d cluster delete whatbreaks-e2e
   ```

---

## 6. Protobuf Code Generation

The gRPC contract between Go and Rust is defined in [`proto/wb/core/v1/core.proto`](../proto/wb/core/v1/core.proto).

### Regenerating Go Code
If you modify `core.proto`, regenerate the Go bindings:
```bash
./proto/gen.sh
```
This updates:
- `gen/go/wb/core/v1/core.pb.go`
- `gen/go/wb/core/v1/core_grpc.pb.go`

### Regenerating Rust Code
Rust bindings are generated automatically during Cargo builds via `core-engine-proto/build.rs`:
```bash
cargo build -p wb-core-proto
```

---

## 7. Contribution Workflow

1. **Fork & Branch**: Create a feature branch from `main`:
   ```bash
   git checkout -b feature/my-new-feature
   ```
2. **Implement & Test**: Write your changes, along with tests covering new logic.
3. **Run Full Verification**: Before opening a pull request, verify that all test suites pass cleanly:
   ```bash
   ./scripts/test.sh
   ```
4. **Submit PR**: Open a pull request against `aaradhychinche-alt/WhatBreaks` describing the problem solved and test coverage.
