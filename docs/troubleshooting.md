# WhatBreaks Troubleshooting Guide

This guide provides actionable diagnosis and resolution steps for operational issues encountered when running WhatBreaks (WB).

Whenever troubleshooting, start by running the stack diagnosis tool:
```bash
./scripts/wb.sh doctor
./scripts/wb.sh status
```

Runtime logs for all managed components are saved under `.run/`:
- Go Platform API: `.run/go-api.log`
- Rust Core Engine: `.run/core-engine.log`
- Kubernetes Proxy: `.run/kubectl-proxy.log`
- Frontend Dashboard: `.run/frontend.log`

---

## 1. Quick Diagnostic Checklist

| Check | Healthy Output | Diagnostic Command |
| :--- | :--- | :--- |
| **PostgreSQL** | Port 5432 open | `nc -z 127.0.0.1 5432` |
| **Core Engine** | Port 50051 open | `nc -z 127.0.0.1 50051` |
| **Kubernetes Proxy** | Port 8001 returns HTTP 200 | `curl -s http://127.0.0.1:8001/readyz` |
| **Go API Health** | Status "healthy" | `curl -s http://127.0.0.1:8080/health` |
| **System Health** | All components "healthy" | `curl -s http://127.0.0.1:8080/api/v1/system/health` |
| **Frontend** | Port 3000 returns HTML | `curl -s -I http://localhost:3000/` |

---

## 2. Issues & Solutions

### A. Docker Daemon Not Running
**Symptoms:**
- `./scripts/wb.sh start` fails with: `[FAILED] Docker daemon is not running.`
- `docker info` exits with an error or hangs.

**Likely Causes:**
- Docker Desktop is not started, or the Docker socket is unresponsive.

**Diagnosis:**
```bash
docker info
```

**Resolution:**
- **macOS**: Start Docker Desktop from `/Applications/Docker.app` or run:
  ```bash
  open -a Docker
  ```
  Wait until the whale icon in the menu bar is steady, then re-run `./scripts/wb.sh start`.
- **Linux**: Start the Docker daemon service:
  ```bash
  sudo systemctl start docker
  ```

---

### B. PostgreSQL Connection Failures
**Symptoms:**
- Go Platform API logs report: `PostgreSQL not connected or auth failed, running in stateless mode`.
- `GET /api/v1/system/health` reports `"database": {"status": "unavailable"}`.
- Dashboard shows database disconnected badge.

**Likely Causes:**
- The PostgreSQL container is stopped.
- Password mismatch (`TEST_DB_PASSWORD` does not match the running container).
- Another process is occupying port 5432.

**Diagnosis:**
```bash
# Check if port 5432 is open
nc -z 127.0.0.1 5432

# Check Docker container status
docker ps --filter "name=whatbreaks-postgres"

# Test direct connection
PGPASSWORD=testsecret psql -h 127.0.0.1 -U whatbreaks -d whatbreaks -c "SELECT 1;"
```

**Resolution:**
1. Start the containerized PostgreSQL service:
   ```bash
   docker compose up -d postgres
   ```
2. If you are using a local PostgreSQL service instead of Docker, ensure the database user `whatbreaks` exists, or set:
   ```bash
   export DB_HOST=localhost
   export DB_USER=postgres
   export DB_PASSWORD=yourpassword
   export DB_NAME=whatbreaks
   ```
3. Use guided recovery from the UI or trigger reconnect:
   ```bash
   curl -s -X POST "http://127.0.0.1:8080/api/v1/system/recover?workspace_id=00000000-0000-0000-0000-000000000001" \
     -H "Content-Type: application/json" \
     -d '{"component":"database","action":"reconnect"}' | jq .
   ```

---

### C. Rust Core Engine Unavailable
**Symptoms:**
- API logs report: `Rust Core Engine not reachable, impact analysis disabled`.
- Calling `POST /api/v1/impact` returns `503 Service Unavailable` with code `CORE_ENGINE_UNAVAILABLE`.
- `GET /api/v1/system/health` reports `"core_engine": {"status": "unavailable"}`.

**Likely Causes:**
- The `wb-core-server` process was terminated or never compiled.
- Port 50051 is blocked or used by an unrelated process.

**Diagnosis:**
```bash
# Check if port 50051 is listening
nc -z 127.0.0.1 50051

# Inspect Core Engine log
cat .run/core-engine.log
```

**Resolution:**
1. Manually build the Core Engine binary:
   ```bash
   cargo build -p wb-core-server
   ```
2. Start the Core Engine server:
   ```bash
   ./target/debug/wb-core-server &
   ```
3. Verify gRPC liveness from the Go API:
   ```bash
   curl -s -X POST "http://127.0.0.1:8080/api/v1/system/recover?workspace_id=00000000-0000-0000-0000-000000000001" \
     -H "Content-Type: application/json" \
     -d '{"component":"core_engine","action":"retry_connection"}' | jq .
   ```

---

### D. Kubernetes API Authentication & Connectivity Errors
**Symptoms:**
- API logs report: `Kubernetes API ping check returned error: connection refused` or `403 Forbidden`.
- System Health reports `"collector": {"status": "degraded"}` or `"unavailable"`.
- Triggering discovery sweep returns `502 Bad Gateway` (`DISCOVERY_FAILED`).

**Likely Causes:**
- `kubectl proxy` is not running on port 8001.
- Active kubeconfig context points to an offline or deleted cluster.
- The ServiceAccount lacks required RBAC permissions.

**Diagnosis:**
```bash
# 1. Check if kubectl proxy is listening
nc -z 127.0.0.1 8001

# 2. Check cluster readiness through proxy
curl -s http://127.0.0.1:8001/readyz

# 3. Test cluster connectivity directly
kubectl cluster-info

# 4. Run automated WhatBreaks collector diagnostics
curl -s "http://127.0.0.1:8080/api/v1/system/diagnose?workspace_id=00000000-0000-0000-0000-000000000001&component=collector" | jq .
```

**Resolution:**
1. If proxy is offline:
   ```bash
   kubectl proxy --port=8001 &
   ```
2. If cluster context is invalid:
   ```bash
   kubectl config get-contexts
   kubectl config use-context <valid-cluster-context>
   ```
3. If permissions are missing (HTTP 403 on `/api/v1/pods`):
   Apply the read-only ClusterRole from [`docs/kubernetes-setup.md`](file:///Users/aaradhychinche/Documents/WB/whatbreaks/docs/kubernetes-setup.md#2-required-kubernetes-permissions-strictly-read-only).

---

### E. Empty Resource Inventory (0 Resources Discovered)
**Symptoms:**
- The Resources page displays 0 total resources.
- `GET /api/v1/resources` returns `{"total": 0, "resources": []}`.

**Likely Causes:**
- Initial sync has not completed.
- Namespace filter (`--k8s-namespaces`) specifies namespaces that do not exist or contain no workloads.
- The connected cluster has no running pods or deployments in the monitored namespaces.

**Diagnosis:**
```bash
# Check what namespaces have running pods
kubectl get pods -A

# Trigger manual discovery sweep
./scripts/wb.sh restart
```

**Resolution:**
1. Trigger a discovery sweep from the UI or via API:
   ```bash
   CSRF_TOKEN=$(curl -s -c cookie.txt http://127.0.0.1:8080/api/csrf-token | jq -r .csrfToken)
   curl -s -b cookie.txt -X POST "http://127.0.0.1:8080/api/v1/discovery/sync?workspace_id=00000000-0000-0000-0000-000000000001" \
     -H "Content-Type: application/json" \
     -H "X-CSRF-Token: ${CSRF_TOKEN}" \
     -H "X-Workspace-ID: 00000000-0000-0000-0000-000000000001" | jq .
   ```
2. If `--k8s-namespaces` was set, verify that workloads exist in those namespaces or clear the flag to monitor all namespaces.

---

### F. No Dependency Relationships Found (Isolated Nodes)
**Symptoms:**
- Resources exist in the inventory, but the Dependencies page shows isolated nodes with 0 edges.
- `GET /api/v1/dependencies` returns `{"total": 0, "relationships": []}`.

**Likely Causes:**
- Workloads do not have semantic connections (e.g. standalone Pods without Services, or Services whose label selectors do not match Pod labels).
- Core Engine was offline during the discovery sweep, so discovery rules were not applied.

**Diagnosis:**
```bash
# Inspect whether state_relationships contains records in PostgreSQL
PGPASSWORD=testsecret psql -h 127.0.0.1 -U whatbreaks -d whatbreaks -c "SELECT COUNT(*) FROM state_relationships;"
```

**Resolution:**
1. Verify the Core Engine is running on port 50051 (`scripts/wb.sh status`).
2. Trigger a fresh discovery sync sweep:
   ```bash
   curl -s -b cookie.txt -X POST "http://127.0.0.1:8080/api/v1/discovery/sync?workspace_id=00000000-0000-0000-0000-000000000001" ...
   ```
3. Deploy a sample connected workload (Deployment + Service) to verify relationship synthesis:
   ```bash
   kubectl create deployment web --image=nginx:alpine
   kubectl expose deployment web --port=80 --target-port=80
   ```

---

### G. Identity Integrity Rejected (`IDENTITY_INTEGRITY_REJECTED`)
**Symptoms:**
- Querying `/api/v1/resources` or `/api/v1/impact` returns HTTP `422 Unprocessable Entity` with:
  `"code": "IDENTITY_INTEGRITY_REJECTED"`, `"error": "workspace contains un-migrated legacy state"`.

**Likely Causes:**
- The PostgreSQL database contains legacy un-prefixed records (e.g. `default/my-service` instead of canonical `k8s-cluster/default/my-service`) from older development versions.

**Diagnosis:**
```bash
# Inspect if legacy un-prefixed identities exist
PGPASSWORD=testsecret psql -h 127.0.0.1 -U whatbreaks -d whatbreaks -c \
  "SELECT provider_id FROM state_resources WHERE provider_id NOT LIKE '%/%/%' AND provider_id NOT LIKE '%/%';"
```

**Resolution:**
Run the administrative recovery procedure to safely archive legacy state and recollect canonical records:
```bash
CSRF_TOKEN=$(curl -s -c cookie.txt http://127.0.0.1:8080/api/csrf-token | jq -r .csrfToken)

curl -s -b cookie.txt -X POST "http://127.0.0.1:8080/api/v1/system/recover?workspace_id=00000000-0000-0000-0000-000000000001" \
  -H "Content-Type: application/json" \
  -H "X-CSRF-Token: ${CSRF_TOKEN}" \
  -H "X-Workspace-ID: 00000000-0000-0000-0000-000000000001" \
  -d '{"component":"collector","action":"sync_discovery","confirmed":true}' | jq .
```

---

### H. Port Conflicts on Startup
**Symptoms:**
- `./scripts/wb.sh start` fails with: `[CONFLICT] Port 50051 is occupied by unrelated process`.
- Or `bind: address already in use` in `.run/go-api.log`.

**Likely Causes:**
- A previous or orphaned instance of `wb`, `wb-core-server`, or another local service is holding the port.

**Diagnosis:**
```bash
# Find what process is holding a specific port (e.g. 50051 or 8080)
lsof -iTCP:50051 -sTCP:LISTEN
lsof -iTCP:8080 -sTCP:LISTEN
lsof -iTCP:3000 -sTCP:LISTEN
```

**Resolution:**
1. Stop script-managed processes:
   ```bash
   ./scripts/wb.sh stop
   ```
2. If an orphaned external process is still holding the port:
   ```bash
   kill -TERM <PID>
   ```
3. Re-run `./scripts/wb.sh start`.
