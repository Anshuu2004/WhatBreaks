# WhatBreaks User Guide & Workflows

This guide walks through using WhatBreaks (WB) through both the web dashboard (`http://localhost:3000`) and the REST API. It covers the four core user interfaces, configuring proposed changes, interpreting blast-radius results, and operating system diagnostics.

---

## 1. Web Dashboard Overview

The WhatBreaks dashboard is organized into four main functional pages accessible via the left navigation sidebar:

| Page | Purpose | Primary Actions |
| :--- | :--- | :--- |
| **Investigate** | Core impact analysis and blast-radius simulation. | Select a resource, choose a proposed change operation, configure traversal direction & depth, evaluate blast radius, and inspect supporting evidence paths. |
| **Resources** | Searchable infrastructure inventory. | Filter by cluster or resource type, search by identifier, inspect active relationships, and jump directly to impact investigation. |
| **Dependencies** | Interactive topology visualization. | Explore the directed dependency graph, inspect semantic edges, and understand relationship kinds. |
| **System Health** | Component telemetry & automated diagnostics. | View health status of API, DB, Engine, and Collector; trigger manual discovery sweeps; execute automated diagnostics; run guided recovery. |

---

## 2. Walkthrough: Impact Analysis (Investigate Page)

The Investigate page is the primary tool for answering: *"If I change this resource, what could break?"*

```
+---------------------------------------------------------------------------------+
|  INVESTIGATE                                                                    |
+--------------------------+------------------------------------------------------+
| Resource Selection       | 1. Propose Infrastructure Change                     |
| [Search...]              | Operation: [ DELETE ] [ UPDATE ] [ SCALE ] [ REPLACE]|
| - k8s-cluster/default/   | Direction: [ Incoming (Callers) v ]  Depth: [ 3 ]    |
|   order-service (Service)| Details:   [ Updating replica endpoint...          ] |
| - k8s-cluster/default/   |            [ Analyze DELETE Impact ]                 |
|   order-deployment (Dep) +------------------------------------------------------+
| - k8s-cluster/default/   | 2. Blast Radius Results                              |
|   order-db (ConfigMap)   | Total: 3  | Direct: 1 | Indirect: 2 | Max Depth: 2   |
|                          | Paths: Ingress -> Service -> Pod                    |
|                          | Evidence: 4 observations                             |
+--------------------------+------------------------------------------------------+
```

### Step 1: Select a Target Resource
- Browse or search the inventory on the left sidebar.
- Click a resource (e.g. `k8s-cluster/default/order-service`).
- Alternatively, select a resource on the **Resources** or **Dependencies** page and click **"Investigate"**.

### Step 2: Choose a Proposed Change Operation
WhatBreaks provides four change operations with deterministic propagation policies:

| Operation | Typical Scenario | Traversal Depth Policy | Propagation Semantics |
| :--- | :--- | :--- | :--- |
| **`DELETE`** | Decommissioning or deleting a resource. | Configurable (`1` to `5` in UI, up to `50` via API). | Full multi-hop propagation upward through traffic routing and ownership links. |
| **`UPDATE`** | Modifying spec, environment variables, or config. | **Clamped to depth `1`** (enforced by policy). | Evaluates immediate direct dependents. Non-structural changes rarely propagate indefinitely. |
| **`SCALE`** | Changing replica count (e.g. scaling down to 0). | Configurable (`1` to `5`). | Affects downstream capacity and immediate upstream routing pools. |
| **`REPLACE`** | Destroying and recreating a resource. | **Clamped to depth `1`** (enforced by policy). | Evaluates immediate direct callers affected during the recreation transition. |

### Step 3: Configure Traversal Direction & Depth
- **Direction**:
  - `Incoming (Dependents / Callers)` *(Default)*: Traverses upstream callers. Use this to find out who relies on this resource and would break if it changes.
  - `Outgoing (Dependencies)`: Traverses downstream components. Use this to see what this resource requires to function.
- **Max Depth**:
  - Sets the maximum traversal hop distance (`1` to `5`).
  - Note: For `UPDATE` and `REPLACE`, depth is locked to `1` by design.
- **Optional Details**: Enter freeform notes (e.g. `"scaling down for maintenance"`).

### Step 4: Run the Analysis
Click **"Analyze Impact"**. The Rust Core Engine executes an in-memory graph traversal and returns deterministic results in milliseconds.

---

## 3. Interpreting Impact Results

The analysis response is divided into clear factual sections:

### A. Summary KPI Cards
- **Total Impacted**: Total count of unique resources in the candidate blast radius.
- **Direct Impacted**: Resources connected directly to the target (`depth = 1`).
- **Indirect Impacted**: Resources affected via multi-hop transitive dependencies (`depth > 1`).
- **Max Depth**: The maximum hop distance reached in this traversal.

### B. Impacted Resources List
Every affected resource is listed with:
- Canonical identity (`provider`, `resource_type`, `provider_id`).
- Hop distance (shortest path length from target).
- Impact reason (e.g., `"upstream routing dependent"`, `"ownership parent"`).

### C. Dependency Paths & Visual Graph
- Renders the exact directed path from the target to each impacted resource (e.g. `order-service` $\rightarrow$ `frontend-deployment` $\rightarrow$ `api-ingress`).
- The interactive graph view highlights the target node in cyan, impacted nodes in red/orange, and non-impacted nodes in muted gray.

### D. Evidence & Provenance
Every relationship in the path is backed by point-in-time `Evidence` records:
- Observation type (`CONFIGURATION`, `RESOURCE_REFERENCE`, `OWNERSHIP_REFERENCE`, etc.).
- Collector identifier and observation timestamp.
- Evidence IDs proving why WhatBreaks linked the resources.

### E. Change Assessment, Assumptions, & Limitations
- **Assumptions**: Transparently records analysis assumptions (e.g., assuming synchronous call paths for HTTP services).
- **Limitations**: Clarifies constraints (e.g., unobserved external clients or third-party DNS routers).

### F. Analysis Completeness
Indicates whether the analysis was based on a full or partial cluster sweep:
- `COMPLETE`: All required scopes were successfully gathered.
- `PARTIAL` / `DEGRADED`: A non-critical scope (e.g. `ingresses` or `endpointslices`) was unreachable, and the reasons are clearly itemized.

> [!NOTE]
> **Potential vs. Guaranteed Outages**:
> WhatBreaks identifies the **potential blast radius** (candidate impacted resources). It does not claim that every affected resource is guaranteed to fail—resilient workloads with retries, fallback caches, or circuit breakers may absorb the change.

---

## 4. Resources Inventory Page

The Resources page provides an inventory of all discovered infrastructure objects:

1. **Cluster Selector**: Filter resources by active cluster (e.g. `k3d-license-service`, `prod-east`).
2. **Type Filter**: Filter by resource kind (`Deployment`, `Pod`, `Service`, `ConfigMap`, `Secret`, `Ingress`, etc.).
3. **Search Bar**: Instant substring search across provider IDs.
4. **Resource Inspector Drawer**: Click any resource row to open the inspector panel:
   - Displays first and last observed timestamps.
   - Shows all active incoming and outgoing relationships.
   - Displays supporting evidence IDs.
   - Click **"Investigate Resource"** to jump directly into blast-radius simulation.

---

## 5. Dependencies Topology Page

The Dependencies page renders an interactive graph of the entire workspace topology:

- **Cluster Filtering**: View the topology of a specific cluster or inspect multi-cluster boundaries.
- **Edge Highlighting**: Hover over edges to see the relationship kind (`OWNS`, `SELECTS`, `MOUNTS`, `CONNECTS_TO`) and category (`OWNERSHIP`, `TRAFFIC`, `CONFIGURATION`, `RUNTIME`).
- **Node Selection**: Clicking any node opens the relationship inspector.

---

## 6. System Health & Diagnostics Page

The System Health page gives platform engineers deep operational visibility into WhatBreaks:

### A. Live Component Health Cards
Shows real-time connection status, versions, and addresses for:
- **Go Platform API Server**: Uptime and API port.
- **PostgreSQL Database**: Connection status, host, and database name.
- **Rust Core Engine**: gRPC liveness check on port 50051.
- **Kubernetes Collector**: Reachability ping, configured cluster ID, and last sync timestamp.

### B. Manual Discovery Sync
Click the **"Sync Discovery"** button in the top right to trigger an immediate, on-demand Kubernetes sweep without waiting for the background schedule.

### C. Automated Read-Only Diagnostic Probes
Select a component (e.g. `collector`) and click **"Run Component Diagnostics"**:
- Executes real read-only checks:
  1. `/version` endpoint reachability and Kubernetes version extraction.
  2. `/readyz` readiness probe verification.
  3. RBAC permission checks across required resource scopes.
  4. Namespace filtering verification.
  5. Cluster context alignment.
- Reports duration in milliseconds, pass/fail status, and supporting evidence.
- Provides an automated root-cause analysis with recommended actions.

### D. Guided Component Recovery
When an issue is detected, WhatBreaks offers safe, guided recovery actions:
- **Collector**: `Retry Reachability Probe`, `In-Process Collector Restart`, or `Trigger Discovery Sync`.
- **Database**: `Reconnect to Database`.
- **Core Engine**: `Retry gRPC Liveness Probe`.

---

## 7. Working with the REST API (cURL Examples)

All dashboard actions are available via standard HTTP REST endpoints.

### 1. Retrieve CSRF Token
```bash
curl -s -c cookie.txt http://127.0.0.1:8080/api/csrf-token
```
Response:
```json
{"csrfToken":"4a9f...b2"}
```

### 2. Trigger Discovery Synchronization
```bash
CSRF_TOKEN=$(curl -s -c cookie.txt http://127.0.0.1:8080/api/csrf-token | jq -r .csrfToken)

curl -s -b cookie.txt -X POST "http://127.0.0.1:8080/api/v1/discovery/sync?workspace_id=00000000-0000-0000-0000-000000000001" \
  -H "Content-Type: application/json" \
  -H "X-CSRF-Token: ${CSRF_TOKEN}" \
  -H "X-Workspace-ID: 00000000-0000-0000-0000-000000000001" | jq .
```

### 3. List Resources
```bash
curl -s "http://127.0.0.1:8080/api/v1/resources?workspace_id=00000000-0000-0000-0000-000000000001&type=service" | jq .
```

### 4. Execute Impact Analysis
```bash
curl -s -b cookie.txt -X POST "http://127.0.0.1:8080/api/v1/impact" \
  -H "Content-Type: application/json" \
  -H "X-CSRF-Token: ${CSRF_TOKEN}" \
  -H "X-Workspace-ID: 00000000-0000-0000-0000-000000000001" \
  -d '{
    "workspace_id": "00000000-0000-0000-0000-000000000001",
    "target": {
      "provider": "kubernetes",
      "resource_type": "service",
      "provider_id": "k3d-license-service/default/order-service"
    },
    "direction": "incoming",
    "max_depth": 3,
    "proposed_change": {
      "change_type": "DELETE",
      "details": "decommissioning service"
    }
  }' | jq .
```

### 5. Run Automated Diagnostics
```bash
curl -s "http://127.0.0.1:8080/api/v1/system/diagnose?workspace_id=00000000-0000-0000-0000-000000000001&component=collector" | jq .
```
