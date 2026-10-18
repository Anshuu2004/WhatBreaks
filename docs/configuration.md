# WhatBreaks Configuration Reference

This document provides a comprehensive reference of all command-line flags, environment variables, default values, port allocations, and safety rules for WhatBreaks (WB).

---

## 1. Port Allocations & Default Bindings

| Port | Protocol | Component | Default Binding | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| **`3000`** | HTTP | Frontend Dashboard | `0.0.0.0:3000` | Vite + React 19 web interface |
| **`5432`** | TCP | PostgreSQL 16 Store | `127.0.0.1:5432` | Source-of-truth storage for resources, evidence, and relationships |
| **`8001`** | HTTP | Kubernetes API Proxy | `127.0.0.1:8001` | Authenticated local proxy (`kubectl proxy --port=8001`) |
| **`8080`** | HTTP | Go Platform API | `0.0.0.0:8080` | REST API routes, CSRF token endpoint, discovery coordinator |
| **`8081`** | HTTP | Go Health Server | `0.0.0.0:8081` | Kubernetes liveness probe (`/healthz`) and readiness probe (`/readyz`) |
| **`50051`**| gRPC | Rust Core Engine | `127.0.0.1:50051` | High-performance graph traversal and blast-radius calculation |

---

## 2. Command-Line Flags (`cmd/wb`)

The Go Platform API binary (`wb`) accepts the following flags:

| Flag | Default Value | Required | Purpose | Example |
| :--- | :--- | :--- | :--- | :--- |
| `--api-port` | `8080` | No | Port on which the HTTP REST API server listens. | `--api-port=8080` |
| `--health-port` | `8081` | No | Port on which `/readyz` and `/healthz` probe endpoints listen. | `--health-port=8081` |
| `--core-engine-addr` | `127.0.0.1:50051` | No | Host and port of the Rust Core Engine gRPC server. | `--core-engine-addr=127.0.0.1:50051` |
| `--workspace-id` | `00000000-0000-0000-0000-000000000001` | No | Workspace UUID identifier scoping resources and queries. | `--workspace-id=00000000-0000-0000-0000-000000000001` |
| `--enable-k8s` | `true` | No | Enables the Kubernetes discovery collector subsystem. | `--enable-k8s=true` |
| `--k8s-api-url` | `""` | Conditional | Base URL of Kubernetes API server or proxy (e.g. `http://127.0.0.1:8001`). | `--k8s-api-url=http://127.0.0.1:8001` |
| `--k8s-cluster-id` | Auto-detected | No | Identifier for the cluster in canonical identities (`<cluster_id>/...`). | `--k8s-cluster-id=prod-us-east-1` |
| `--k8s-namespaces` | `""` (all namespaces) | No | Comma-separated list of namespaces to observe. | `--k8s-namespaces=default,production` |
| `--k8s-cluster-wide`| `false` | No | When true, scans cluster-scoped resources (`nodes`, `namespaces`, `pvs`). | `--k8s-cluster-wide=true` |
| `--help` | `false` | No | Displays usage flags and exits. | `--help` |
| `--version` | `false` | No | Prints binary version string and exits. | `--version` |

---

## 3. Environment Variables Reference

### A. Database Configuration (`internal/config/database.go`)

| Variable | Default Value | Required | Purpose | Example |
| :--- | :--- | :--- | :--- | :--- |
| `DB_HOST` | `localhost` | No | PostgreSQL server host address. | `DB_HOST=127.0.0.1` |
| `DB_PORT` | `5432` | No | PostgreSQL server port. | `DB_PORT=5432` |
| `DB_NAME` | `whatbreaks` | No | PostgreSQL database name. | `DB_NAME=whatbreaks` |
| `DB_USER` | `whatbreaks` | No | Database user role. | `DB_USER=whatbreaks` |
| `DB_PASSWORD` | `""` | Conditional | Database password (required in production). | `DB_PASSWORD=secret` |
| `TEST_DB_NAME` | `""` | No | Fallback DB name in test/development environments. | `TEST_DB_NAME=postgres` |
| `TEST_DB_USER` | `""` | No | Fallback DB user in test/development environments. | `TEST_DB_USER=postgres` |
| `TEST_DB_PASSWORD`| `testsecret` | No | Fallback DB password in development. | `TEST_DB_PASSWORD=testsecret` |
| `DB_POOL_MAX` | `10` | No | Maximum open connections in the pool. | `DB_POOL_MAX=20` |
| `DB_POOL_MIN` | `2` | No | Minimum idle connections kept open. | `DB_POOL_MIN=4` |
| `DB_POOL_IDLE_TIMEOUT`| `30000` (ms) | No | Connection idle timeout in milliseconds. | `DB_POOL_IDLE_TIMEOUT=60000` |
| `DB_CONNECTION_TIMEOUT`| `5000` (ms) | No | TCP connection timeout in milliseconds. | `DB_CONNECTION_TIMEOUT=10000` |
| `DB_ACQUIRE_TIMEOUT` | `60000` (ms) | No | Max wait duration to acquire a connection from pool. | `DB_ACQUIRE_TIMEOUT=30000` |
| `DB_SSL` | `disable` | No | PostgreSQL TLS mode (`disable`, `require`, `verify`). | `DB_SSL=disable` |
| `PGSSLROOTCERT` | `""` | No | Path to trusted CA root certificate file. | `PGSSLROOTCERT=/etc/ssl/certs/ca.pem` |

### B. Rust Core Engine Configuration

| Variable | Default Value | Required | Purpose | Example |
| :--- | :--- | :--- | :--- | :--- |
| `WB_CORE_GRPC_ADDR` | `127.0.0.1:50051` | No | Network address for `wb-core-server` gRPC listener. | `WB_CORE_GRPC_ADDR=0.0.0.0:50051` |

### C. Server Environment & Web Security

| Variable | Default Value | Required | Purpose | Example |
| :--- | :--- | :--- | :--- | :--- |
| `NODE_ENV` / `WB_ENV` | `development` | No | Runtime mode: `development`, `test`, `production`. | `WB_ENV=production` |
| `ALLOWED_ORIGINS` | `http://localhost:3000` | No | Comma-delimited list of CORS allowed origins. | `ALLOWED_ORIGINS=https://app.whatbreaks.dev` |
| `RATE_LIMIT_RPS` | `100` | No | Max requests per second per IP address. | `RATE_LIMIT_RPS=200` |
| `MAX_BODY_BYTES` | `10485760` (10MB) | No | Maximum allowed HTTP request body size in bytes. | `MAX_BODY_BYTES=10485760` |
| `SESSION_SECRET` | auto-generated | No | Secret seed for session cookie / CSRF derivation. | `SESSION_SECRET=a-secure-random-string` |
| `CSRF_COOKIE_NAME`| `_wb_csrf` | No | Cookie name storing the CSRF token. | `CSRF_COOKIE_NAME=_wb_csrf` |

### D. Frontend Dashboard Configuration (`web/`)

| Variable | Default Value | Required | Purpose | Example |
| :--- | :--- | :--- | :--- | :--- |
| `VITE_API_URL` | `""` (proxied to `/api`) | No | Custom base URL for WhatBreaks API in frontend. | `VITE_API_URL=http://localhost:8080` |
| `VITE_WORKSPACE_ID`| `00000000-0000-0000-0000-000000000001` | No | Default active workspace UUID in frontend. | `VITE_WORKSPACE_ID=00000000-0000-0000-0000-000000000001` |

---

## 4. Forbidden Environment Variables

To protect against credential leakage and environment pollution, WhatBreaks enforces strict **startup rejection invariants** in [`cmd/wb/main.go`](../cmd/wb/main.go#L85-L94) and [`internal/config/config.go`](../internal/config/config.go#L11-L27):

```go
if getenv("WB_API_TOKEN") != "" || getenv("TT_API_TOKEN") != "" {
    logger.Error("raw API tokens in environment variables are forbidden; mount token as secret and use WB_API_TOKEN_FILE")
    return 1
}
if getenv("KUBECONFIG") != "" {
    logger.Error("explicit KUBECONFIG environment variable is forbidden; controller must use in-cluster config or default kubeconfig")
    return 1
}
```

### Prohibited Variables List
| Forbidden Variable | Reason for Prohibition | Safe Alternative |
| :--- | :--- | :--- |
| `WB_API_TOKEN` | Raw API tokens in environment variables leak into `/proc`, crash dumps, and child process tables. | Mount the token as a protected file with `0600` permissions and pass `WB_API_TOKEN_FILE`. |
| `TT_API_TOKEN` | Legacy TokenTimer token variable. | Use `WB_API_TOKEN_FILE`. |
| `TOKENTIMER_API_TOKEN` | Legacy TokenTimer token variable. | Use `WB_API_TOKEN_FILE`. |
| `KUBECONFIG` | Explicit kubeconfig paths risk picking up elevated personal credentials or pointing to incorrect remote clusters. | Use local proxy (`kubectl proxy --port=8001`), in-cluster ServiceAccount, or `--k8s-api-url`. |

---

## 5. Token File Permissions Guard

When using `WB_API_TOKEN_FILE`, WhatBreaks enforces filesystem permission checks on POSIX systems:
- The token file must **not** be readable or writable by group or other users (i.e. permissions must be `0600` or `0400`).
- If permissions are too open (e.g. `0644`), the process refuses to start unless `ALLOW_INSECURE_TOKEN_FILE_PERMISSIONS=true` is explicitly provided.
