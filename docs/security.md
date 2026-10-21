# WhatBreaks Security & Safety Model

This document specifies the security boundaries, data privacy guarantees, credential safety invariants, and architectural safeguards implemented in WhatBreaks (WB) V1.

---

## 1. Core Security Guarantees

WhatBreaks adheres to three foundational security invariants:

1. **Strictly Read-Only Infrastructure Operations**: WhatBreaks observes infrastructure passively. It never creates, updates, scales, mutates, or deletes any Kubernetes resource.
2. **Zero Secret Custody**: WhatBreaks never extracts, inspects, stores, or transmits sensitive secret payloads, passwords, private keys, or certificates.
3. **Anti-Credential Leakage**: Raw API tokens and kubeconfig files are prohibited in environment variables to prevent accidental leakage into process listings, logs, or crash dumps.

---

## 2. Zero Secret Custody Technical Implementation

In many infrastructure analysis tools, inspecting Kubernetes resources poses a severe security risk: reading `v1/Secret` resources could inadvertently pull database passwords, API tokens, and private keys into memory, logs, or persistent databases.

WhatBreaks guarantees **Zero Secret Custody** at the JSON unmarshaling layer:

### A. Sanitized Unmarshaling in `internal/collector/k8s/client.go`
When querying the Kubernetes Secrets API (`GET /api/v1/namespaces/{ns}/secrets`), the REST client does **not** decode the secret values:

```go
// rawSecretItem is used strictly during unmarshaling to extract metadata and key names only.
type rawSecretItem struct {
    Metadata ObjectMeta                 `json:"metadata"`
    Type     string                     `json:"type,omitempty"`
    Data     map[string]json.RawMessage `json:"data,omitempty"`
}
```

During stream decoding, the client iterates over the keys of the `Data` map to extract only the key names, immediately discarding the byte slices:

```go
keys := make([]string, 0, len(item.Data))
for key := range item.Data {
    keys = append(keys, key)
}
sanitized[i] = SecretMetadata{
    ObjectMeta: item.Metadata,
    Type:       item.Type,
    KeyNames:   keys,
}
```

### B. What is Stored vs. What is Discarded
| Field | Handled By WhatBreaks | Example | Stored in PostgreSQL? |
| :--- | :--- | :--- | :--- |
| `metadata.name` | Retained (Identity) | `db-credentials` | Yes |
| `metadata.namespace`| Retained (Scope) | `production` | Yes |
| `type` | Retained (Classification) | `Opaque` | Yes |
| Map keys of `.data` | Retained (Key Names) | `["username", "password"]` | Yes (Metadata only) |
| **Values of `.data`** | **DISCARDED AT UNMARSHALING** | Base64-encoded password | **NEVER** |
| **Values of `.stringData`**| **DISCARDED AT UNMARSHALING** | Plaintext password | **NEVER** |
| **Private Keys (PEM)** | **DISCARDED AT UNMARSHALING** | `-----BEGIN RSA PRIVATE KEY-----` | **NEVER** |

### C. Test Verification
This invariant is enforced by automated security tests in [`internal/collector/k8s/secret_safety_test.go`](../internal/collector/k8s/secret_safety_test.go):
- The test returns synthetic Kubernetes secrets containing passwords, RSA private keys, and service account JWTs.
- The test asserts that the resulting Evidence and database payloads do **not** contain any secret strings:
  ```go
  if bytes.Contains(ev.Data, []byte(superSecretPassword)) {
      t.Fatalf("SECURITY VIOLATION: secret password leaked into Evidence.Data")
  }
  ```

---

## 3. Strictly Read-Only Kubernetes Interface

The Go Kubernetes client interface ([`internal/collector/k8s/client.go`](../internal/collector/k8s/client.go)) provides an immutable, read-only contract:

```go
type Client interface {
    ListNamespaces(ctx context.Context) ([]Namespace, error)
    ListNodes(ctx context.Context) ([]Node, error)
    ListPods(ctx context.Context, namespace string) ([]Pod, error)
    ListDeployments(ctx context.Context, namespace string) ([]Deployment, error)
    ListReplicaSets(ctx context.Context, namespace string) ([]ReplicaSet, error)
    ListServices(ctx context.Context, namespace string) ([]Service, error)
    ListIngresses(ctx context.Context, namespace string) ([]Ingress, error)
    ListConfigMaps(ctx context.Context, namespace string) ([]ConfigMap, error)
    ListServiceAccounts(ctx context.Context, namespace string) ([]ServiceAccount, error)
    ListSecretsMetadata(ctx context.Context, namespace string) ([]SecretMetadata, error)
    ListPVCs(ctx context.Context, namespace string) ([]PersistentVolumeClaim, error)
    ListPVs(ctx context.Context) ([]PersistentVolume, error)
    ListEndpointSlices(ctx context.Context, namespace string) ([]EndpointSlice, error)
    Watch(ctx context.Context, resourcePath string, resourceVersion string) (<-chan WatchEvent, <-chan error, error)
    Ping(ctx context.Context) error
}
```

There are zero `Create`, `Update`, `Patch`, `Delete`, `Scale`, or `Exec` methods exposed anywhere in the client codebase.

---

## 4. Anti-Credential Leakage Invariants

To eliminate accidental exposure of tokens and certificates:

1. **Forbidden Raw Environment Variables**:
   If `WB_API_TOKEN`, `TT_API_TOKEN`, or `KUBECONFIG` are detected in the environment during process startup, `cmd/wb/main.go` terminates execution immediately:
   ```text
   ERROR: raw API tokens in environment variables are forbidden; mount token as secret and use WB_API_TOKEN_FILE
   ```
2. **Token File Filesystem Permissions**:
   When using file-based machine tokens via `WB_API_TOKEN_FILE`, WhatBreaks verifies file permissions:
   - On POSIX platforms, the file must not have group or world read/write permissions (`0600` or `0400` required).
   - If permissions are too open, the process refuses to boot unless explicitly bypassed via `ALLOW_INSECURE_TOKEN_FILE_PERMISSIONS=true`.
3. **No Kubeconfig Ingestion**:
   WhatBreaks does not parse `~/.kube/config` for credentials. It communicates over an explicit HTTP/HTTPS endpoint (`--k8s-api-url`) or via local proxy (`kubectl proxy`).

---

## 5. Workspace Isolation & Identity Integrity

### Multi-Tenant Workspace Scoping
- All stored resources, evidence observations, relationships, and diagnostics are partitioned by `workspace_id` (UUIDv4).
- The REST API extracts the workspace ID from the `X-Workspace-ID` header or `?workspace_id=` query parameter.
- Cross-workspace queries are rejected by SQL partitioning.

### Identity Integrity Guard (`IDENTITY_INTEGRITY_REJECTED`)
To prevent namespace collision or cross-cluster confusion in multi-cluster environments:
- WhatBreaks requires canonical, cluster-prefixed identities:
  - Cluster-scoped: `<cluster_id>/<name>`
  - Namespaced: `<cluster_id>/<namespace>/<name>`
- If an un-migrated or legacy unqualified identity (`<namespace>/<name>`) is detected in a workspace, the API halts graph materialization and impact analysis with HTTP `422 Unprocessable Entity` (`IDENTITY_INTEGRITY_REJECTED`) until an operator runs the administrative recovery workflow.

---

## 6. HTTP API Security Stack

The Go Platform API enforces a multi-layer HTTP defense pipeline:

```mermaid
flowchart LR
    Request["Incoming HTTP Request"] --> PanicRec["1. Panic Recovery"]
    PanicRec --> SecHeaders["2. Security Headers (Helmet)"]
    SecHeaders --> CORS["3. CORS Origin Validation"]
    CORS --> BodyLimit["4. 10MB Body Limit"]
    BodyLimit --> RateLimit["5. IP Rate Limiting"]
    RateLimit --> CSRF["6. CSRF Double-Submit"]
    CSRF --> Router["7. Route Handlers"]
```

1. **Panic Recovery**: Recovers from panics gracefully without leaking Go runtime stack traces in production (`isDevelopment = false`).
2. **Security Headers**: Injects standard HTTP protection headers:
   - `X-Content-Type-Options: nosniff`
   - `X-Frame-Options: SAMEORIGIN`
   - `X-XSS-Protection: 0`
   - `Referrer-Policy: strict-origin-when-cross-origin`
   - `Content-Security-Policy: default-src 'self'`
3. **CORS Validation**: Restricts cross-origin requests to configured domains (`ALLOWED_ORIGINS`).
4. **Body Size Limiter**: Limits incoming payloads to 10MB (`MAX_BODY_BYTES`) to prevent memory exhaustion attacks.
5. **Rate Limiting**: Sliding-window rate limiter per client IP address (`RATE_LIMIT_RPS`, default: 100 req/min).
6. **CSRF Protection**: Double-submit cookie verification. Mutating POST/PUT/DELETE requests require a valid `X-CSRF-Token` header matching the encrypted session cookie.

---

## 7. Current Security Model Boundaries & Limitations

To maintain technical accuracy, the following boundaries of the V1 security model should be noted:

- **Local / Single-Node Deployment**: WhatBreaks V1 is designed for local development and private internal platform deployment. It does not include built-in public multi-tenant user authentication (SSO / OIDC).
- **Communication Encryption**: Communication between the Go Platform API and the Rust Core Engine on `127.0.0.1:50051` is unencrypted plaintext gRPC intended for local loopback or sidecar networks. In cross-host environments, an mTLS proxy (e.g. Envoy or Linkerd) is recommended.
- **Certifications**: WhatBreaks does not claim compliance certifications (e.g. SOC2, FedRAMP, HIPAA, ISO 27001) in V1.

---

## 8. Vulnerability Reporting

If you discover a potential security vulnerability, please do not open a public issue. Refer to our [Security Policy](../SECURITY.md) for instructions on confidential disclosure.
