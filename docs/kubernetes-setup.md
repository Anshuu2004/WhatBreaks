# Kubernetes Setup & Cluster Integration Guide

WhatBreaks (WB) discovers infrastructure resources from Kubernetes clusters, normalizes them into immutable point-in-time evidence, derives semantic relationships, and materializes them into a dependency graph for impact analysis.

This guide explains how to connect WhatBreaks to your Kubernetes cluster safely, configure read-only permissions, configure the collector, and verify discovery.

---

## 1. Architectural Model & Connection Behavior

### How WhatBreaks Connects to Kubernetes
Unlike tools that embed heavy client libraries or silently switch clusters behind the scenes, the WhatBreaks Kubernetes Collector is a **pure HTTP/HTTPS REST client** implemented in Go standard library ([`internal/collector/k8s/client.go`](../internal/collector/k8s/client.go)).

### Connection Modes
1. **Local Development (via Proxy)**: Connects to `http://127.0.0.1:8001`, which is served by an authenticated local `kubectl proxy --port=8001`. This is the default workflow used by `scripts/wb.sh`.
2. **In-Cluster Pod Execution**: Connects to `https://kubernetes.default.svc` using the pod's service account token mounted at `/var/run/secrets/kubernetes.io/serviceaccount/token` and the cluster CA certificate.
3. **Direct Endpoint / Remote Cluster**: Connects to a secure API server URL specified in `--k8s-api-url` using a token file mounted at `WB_API_TOKEN_FILE`.

> [!IMPORTANT]
> **Kubeconfig Discovery Invariant**:
> WhatBreaks does **not** parse or use your `KUBECONFIG` file for API authentication. In fact, setting the `KUBECONFIG` environment variable is explicitly forbidden to prevent credential leakage.
> When running locally, WhatBreaks uses `kubectl config current-context` solely to detect a human-readable cluster identifier string for `--k8s-cluster-id`. All network traffic flows through the authenticated `--k8s-api-url` (e.g. `http://127.0.0.1:8001`).

---

## 2. Required Kubernetes Permissions (Strictly Read-Only)

WhatBreaks observes infrastructure passively. It **never modifies, scales, creates, or deletes** any Kubernetes resource.

### ClusterRole Definition (Read-Only)
Apply the following minimal RBAC policy to allow WhatBreaks to discover workloads and their dependencies:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: whatbreaks-viewer
  namespace: default
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: whatbreaks-viewer
rules:
  # Core workloads and configuration
  - apiGroups: [""]
    resources:
      - namespaces
      - pods
      - services
      - configmaps
      - serviceaccounts
      - persistentvolumes
      - persistentvolumeclaims
      - nodes
    verbs: ["get", "list"]

  # Secret metadata (strictly key names; payloads are never stored)
  - apiGroups: [""]
    resources:
      - secrets
    verbs: ["get", "list"]

  # Apps controller workloads
  - apiGroups: ["apps"]
    resources:
      - deployments
      - replicasets
      - statefulsets
      - daemonsets
    verbs: ["get", "list"]

  # Ingress networking
  - apiGroups: ["networking.k8s.io"]
    resources:
      - ingresses
    verbs: ["get", "list"]

  # Endpoint routing
  - apiGroups: ["discovery.k8s.io"]
    resources:
      - endpointslices
    verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: whatbreaks-viewer-binding
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: whatbreaks-viewer
subjects:
  - kind: ServiceAccount
    name: whatbreaks-viewer
    namespace: default
```

---

## 3. The Zero Secret Custody Guarantee

A common concern with Kubernetes inventory tools is access to `Secret` resources. WhatBreaks enforces a strict architectural guarantee: **Zero Secret Custody**.

### How Secrets are Handled
In [`internal/collector/k8s/client.go`](../internal/collector/k8s/client.go#L419-L460):
1. The collector executes a `GET` request against `/api/v1/namespaces/{ns}/secrets`.
2. When decoding the response JSON, it strictly unmarshals the secret metadata (`name`, `namespace`, `uid`) and maps the keys of the `data` field into an array of string key names:
   ```go
   keys := make([]string, 0, len(item.Data))
   for key := range item.Data {
       keys = append(keys, key)
   }
   ```
3. The underlying values in `.data` and `.stringData` are **immediately discarded during JSON stream decoding**.
4. The normalized evidence record contains only `{ "name": "db-secret", "key_names": ["password", "username"] }`.
5. Workload references (such as Pod environment variables referencing `SecretKeySelector`) capture only the reference name (`secret_name: "db-secret"`, `key: "password"`), proving the dependency exists without inspecting the secret payload.

> [!NOTE]
> This invariant is tested in unit test [`internal/collector/k8s/secret_safety_test.go`](../internal/collector/k8s/secret_safety_test.go), verifying that raw tokens, private keys, and passwords never appear in evidence buffers.

---

## 4. Resource Scope Policy (Required vs. Optional)

WhatBreaks categorizes Kubernetes resource scopes according to the policy in [`internal/collector/k8s/scopes.go`](../internal/collector/k8s/scopes.go):

| Scope | Resource Type | Requirement Level | Impact if Permission Missing |
| :--- | :--- | :--- | :--- |
| **`pods`** | `Pod` | **Required** | Discovery sweep marked incomplete / failed |
| **`services`** | `Service` | **Required** | Discovery sweep marked incomplete / failed |
| **`deployments`** | `Deployment` | **Required** | Discovery sweep marked incomplete / failed |
| **`replicasets`** | `ReplicaSet` | **Required** | Discovery sweep marked incomplete / failed |
| **`configmaps`** | `ConfigMap` | **Required** | Discovery sweep marked incomplete / failed |
| **`secrets`** | `Secret` (metadata) | **Required** | Discovery sweep marked incomplete / failed |
| **`ingresses`** | `Ingress` | *Optional* | Permitted; ingress routes omitted from topology |
| **`nodes`** | `Node` | *Optional* | Permitted; node placement omitted from topology |
| **`namespaces`** | `Namespace` | *Optional* | Permitted; namespace metadata omitted |
| **`persistentvolumes`** | `PersistentVolume` | *Optional* | Permitted; PV storage omitted |
| **`persistentvolumeclaims`** | `PersistentVolumeClaim` | *Optional* | Permitted; claim bindings omitted |
| **`serviceaccounts`** | `ServiceAccount` | *Optional* | Permitted; identity links omitted |
| **`endpointslices`** | `EndpointSlice` | *Optional* | Permitted; endpoint routes fallback to service selectors |

If any **Required** scope returns HTTP 403 Forbidden or connection error, WhatBreaks flags the collection run as partial (`discovery.ErrRequiredScopeFailed`) to prevent incorrect impact analysis on incomplete graph state.

---

## 5. Configuring the Collector

### CLI Flags (`cmd/wb`)

Pass the following flags when starting the platform binary:

```bash
./bin/wb \
  --api-port=8080 \
  --health-port=8081 \
  --core-engine-addr=127.0.0.1:50051 \
  --enable-k8s=true \
  --k8s-api-url=http://127.0.0.1:8001 \
  --k8s-cluster-id="k3d-license-service" \
  --k8s-namespaces="" \
  --k8s-cluster-wide=true
```

| Flag | Default Value | Description |
| :--- | :--- | :--- |
| `--enable-k8s` | `true` | Enables the Kubernetes collector subsystem. |
| `--k8s-api-url` | `""` | Base URL of the Kubernetes API server or authenticated proxy. |
| `--k8s-cluster-id` | Auto-detected | Unique cluster name prefixed to all canonical resource identities. |
| `--k8s-namespaces` | `""` (all namespaces) | Comma-separated list of namespaces to observe (e.g. `default,prod`). |
| `--k8s-cluster-wide`| `false` | When true, scans cluster-scoped resources (`nodes`, `namespaces`, `pvs`). |

---

## 6. Verifying Discovery & Inventory

### Triggering a Manual Discovery Sweep
To trigger an immediate discovery sweep from the command line:

```bash
# 1. Fetch CSRF token
CSRF_TOKEN=$(curl -s -c cookie.txt http://127.0.0.1:8080/api/csrf-token | jq -r .csrfToken)

# 2. Trigger sync
curl -s -b cookie.txt -X POST "http://127.0.0.1:8080/api/v1/discovery/sync?workspace_id=00000000-0000-0000-0000-000000000001" \
  -H "Content-Type: application/json" \
  -H "X-CSRF-Token: ${CSRF_TOKEN}" \
  -H "X-Workspace-ID: 00000000-0000-0000-0000-000000000001" | jq .
```

Expected response:
```json
{
  "workspace_id": "00000000-0000-0000-0000-000000000001",
  "status": "COMPLETED",
  "evidence_count": 84,
  "resources_created": 32,
  "resources_updated": 0,
  "resources_total": 32,
  "relationships_created": 46,
  "relationships_total": 46,
  "conflicts_count": 0,
  "duration_ms": 310
}
```

### Inspecting Discovered Resources
```bash
curl -s "http://127.0.0.1:8080/api/v1/resources?workspace_id=00000000-0000-0000-0000-000000000001" | jq .
```

---

## 7. Troubleshooting Common Kubernetes Issues

### Issue 1: Empty Resource Inventory (0 resources)
- **Likely Cause**: The collector has not performed a sync sweep yet, or `--k8s-namespaces` filters out all active workloads.
- **Diagnosis**: Check the collector status on the System Health page or query `GET /api/v1/system/health`.
- **Fix**: Run `scripts/wb.sh status` to ensure `kubectl proxy` is online, or trigger a manual sync in the UI.

### Issue 2: Collector Unreachable (`connection refused on port 8001`)
- **Likely Cause**: `kubectl proxy` is not running.
- **Diagnosis**: Run `curl -s http://127.0.0.1:8001/readyz`. If netcat fails (`nc -z 127.0.0.1 8001`), the proxy is down.
- **Fix**: Launch the proxy manually: `kubectl proxy --port=8001` or run `./scripts/wb.sh start`.

### Issue 3: RBAC 403 Forbidden
- **Likely Cause**: The ServiceAccount or client certificate lacks `list` permissions for one of the Required scopes.
- **Diagnosis**: Run `curl -s http://127.0.0.1:8080/api/v1/system/diagnose` to inspect the exact failing scope.
- **Fix**: Apply the ClusterRole shown in Section 2.

---

## 8. Safe Local Testing with k3d or kind

To test WhatBreaks safely without touching production clusters:

```bash
# Create a local isolated test cluster with k3d
k3d cluster create wb-dev --wait

# Deploy a sample workload
kubectl create deployment nginx --image=nginx:alpine --replicas=2
kubectl expose deployment nginx --port=80 --target-port=80

# Launch WhatBreaks against wb-dev
./scripts/wb.sh start
```
WhatBreaks will discover the `nginx` Deployment, ReplicaSet, Pods, and Service, and render the complete dependency topology.
