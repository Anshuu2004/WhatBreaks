# WhatBreaks REST & Probe API Specification

This document provides a verified reference for all HTTP endpoints exposed by the WhatBreaks Go Platform API (Port `8080`) and Health Probe Server (Port `8081`).

---

## 1. General Conventions

### Ports & Base URLs
- **Platform REST API**: `http://127.0.0.1:8080`
- **Controller Health Probes**: `http://127.0.0.1:8081`

### Common Request Headers
| Header | Type | Description | Required |
| :--- | :--- | :--- | :--- |
| `Content-Type` | string | Must be `application/json` for POST/PUT/PATCH bodies. | Yes (for POST) |
| `X-Workspace-ID`| UUID | Tenant UUID. Defaults to `00000000-0000-0000-0000-000000000001` in local mode. | Conditional |
| `X-CSRF-Token` | string | CSRF token obtained from `GET /api/csrf-token`. | Required on mutating POSTs |

### Standard Error Response Format
All error responses adhere to the standard JSON structure:
```json
{
  "error": "Human readable error description",
  "code": "MACHINE_READABLE_ERROR_CODE"
}
```

---

## 2. Health & Probe Endpoints

### `GET /health` (or `GET /`)
- **Port**: `8080`
- **Purpose**: General API server liveness and PostgreSQL database ping check.
- **Headers**: None required.
- **Response `200 OK`**:
```json
{
  "status": "healthy",
  "environment": "development",
  "uptime": 128.45
}
```

---

### `GET /readyz`
- **Port**: `8081`
- **Purpose**: Kubernetes readiness probe endpoint.
- **Response `200 OK`**:
```json
{
  "status": "ready"
}
```
- **Response `503 Service Unavailable`**:
```json
{
  "status": "not_ready"
}
```

---

### `GET /healthz`
- **Port**: `8081`
- **Purpose**: Kubernetes liveness probe endpoint.
- **Response `200 OK`**:
```json
{
  "status": "ok"
}
```

---

## 3. Authentication & Security Endpoints

### `GET /api/csrf-token` (or `GET /api/v1/csrf`)
- **Port**: `8080`
- **Purpose**: Issues a CSRF token and sets the HTTP-only session cookie.
- **Headers**: None required.
- **Response `200 OK`**:
```json
{
  "csrfToken": "18f92110c73e04746ef323b7..."
}
```
*Sets cookie: `_wb_csrf=<encrypted_session_payload>; Path=/; SameSite=Lax; HttpOnly`*

---

## 4. Resource & Topology Endpoints

### `GET /api/v1/resources`
- **Port**: `8080`
- **Purpose**: Lists discovered infrastructure resources for a workspace with optional filtering.
- **Query Parameters**:
  - `workspace_id` *(optional)*: Workspace UUID (defaults to configured local workspace).
  - `type` / `resource_type` *(optional)*: Case-insensitive filter (e.g. `service`, `deployment`, `pod`).
  - `cluster` *(optional)*: Filter by cluster name (e.g. `k3d-license-service`).
  - `search` *(optional)*: Substring search across `provider_id`.
- **Response `200 OK`**:
```json
{
  "workspace_id": "00000000-0000-0000-0000-000000000001",
  "total": 2,
  "clusters": [
    "k3d-license-service"
  ],
  "resources": [
    {
      "provider": "kubernetes",
      "resource_type": "service",
      "provider_id": "k3d-license-service/default/order-service",
      "first_observed_at": "2026-10-18T10:00:00Z",
      "last_observed_at": "2026-10-18T10:05:00Z"
    },
    {
      "provider": "kubernetes",
      "resource_type": "deployment",
      "provider_id": "k3d-license-service/default/order-deployment",
      "first_observed_at": "2026-10-18T10:00:00Z",
      "last_observed_at": "2026-10-18T10:05:00Z"
    }
  ]
}
```

---

### `GET /api/v1/resources/detail`
- **Port**: `8080`
- **Purpose**: Returns detailed metadata and connected incoming/outgoing relationships for a specific resource.
- **Query Parameters**:
  - `workspace_id` *(optional)*: Workspace UUID.
  - `provider` *(required)*: e.g. `kubernetes`.
  - `resource_type` *(required)*: e.g. `service`.
  - `provider_id` *(required)*: Canonical identifier (e.g. `k3d-license-service/default/order-service`).
- **Response `200 OK`**:
```json
{
  "workspace_id": "00000000-0000-0000-0000-000000000001",
  "resource": {
    "provider": "kubernetes",
    "resource_type": "service",
    "provider_id": "k3d-license-service/default/order-service",
    "first_observed_at": "2026-10-18T10:00:00Z",
    "last_observed_at": "2026-10-18T10:05:00Z"
  },
  "relationships": [
    {
      "source": {
        "provider": "kubernetes",
        "resource_type": "ingress",
        "provider_id": "k3d-license-service/default/order-ingress"
      },
      "target": {
        "provider": "kubernetes",
        "resource_type": "service",
        "provider_id": "k3d-license-service/default/order-service"
      },
      "kind": "ROUTES_TO",
      "category": "TRAFFIC",
      "direction": "INCOMING",
      "peer": {
        "provider": "kubernetes",
        "resource_type": "ingress",
        "provider_id": "k3d-license-service/default/order-ingress"
      },
      "evidence_ids": [
        "13b839bb-1324-4f2b-871d-55734a6e87f1"
      ],
      "first_observed_at": "2026-10-18T10:00:00Z",
      "last_observed_at": "2026-10-18T10:05:00Z"
    }
  ]
}
```

---

### `GET /api/v1/dependencies`
- **Port**: `8080`
- **Purpose**: Lists all active directed semantic relationships in the workspace graph.
- **Query Parameters**:
  - `workspace_id` *(optional)*: Workspace UUID.
  - `cluster` *(optional)*: Filter relationships where source or target belongs to the cluster.
- **Response `200 OK`**:
```json
{
  "workspace_id": "00000000-0000-0000-0000-000000000001",
  "total": 1,
  "relationships": [
    {
      "source": {
        "provider": "kubernetes",
        "resource_type": "deployment",
        "provider_id": "k3d-license-service/default/order-deployment"
      },
      "target": {
        "provider": "kubernetes",
        "resource_type": "replicaset",
        "provider_id": "k3d-license-service/default/order-deployment-55d6797"
      },
      "kind": "OWNS",
      "category": "OWNERSHIP",
      "first_observed_at": "2026-10-18T10:00:00Z",
      "last_observed_at": "2026-10-18T10:05:00Z"
    }
  ]
}
```

---

### `GET /api/v1/clusters`
- **Port**: `8080`
- **Purpose**: Lists all discovered Kubernetes clusters in the workspace, along with reachability status and resource counts.
- **Response `200 OK`**:
```json
{
  "workspace_id": "00000000-0000-0000-0000-000000000001",
  "active_cluster": "k3d-license-service",
  "configured": true,
  "connected": true,
  "status": "connected_with_resources",
  "clusters": [
    {
      "name": "k3d-license-service",
      "active": true,
      "connected": true,
      "status": "connected_with_resources",
      "resource_count": 32,
      "has_synced": true,
      "last_synced_at": "2026-10-18T10:05:00Z"
    }
  ]
}
```

---

## 5. Discovery & Impact Analysis Endpoints

### `POST /api/v1/discovery/sync`
- **Port**: `8080`
- **Purpose**: Triggers an on-demand discovery sweep against the configured Kubernetes cluster.
- **Headers**:
  - `Content-Type: application/json`
  - `X-CSRF-Token: <token>`
  - `X-Workspace-ID: <workspace-uuid>`
- **Response `200 OK`**:
```json
{
  "workspace_id": "00000000-0000-0000-0000-000000000001",
  "status": "COMPLETED",
  "evidence_count": 78,
  "resources_created": 24,
  "resources_updated": 0,
  "resources_total": 24,
  "relationships_created": 36,
  "relationships_total": 36,
  "conflicts_count": 0,
  "duration_ms": 285
}
```

---

### `POST /api/v1/impact`
- **Port**: `8080`
- **Purpose**: Evaluates candidate blast radius and returns structured, explainable impact facts.
- **Headers**:
  - `Content-Type: application/json`
  - `X-CSRF-Token: <token>`
  - `X-Workspace-ID: <workspace-uuid>`
- **Request Body**:
```json
{
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
}
```
- **Response `200 OK`**:
```json
{
  "target": {
    "provider": "kubernetes",
    "resource_type": "service",
    "provider_id": "k3d-license-service/default/order-service"
  },
  "summary": {
    "impacted_count": 2,
    "direct_count": 1,
    "indirect_count": 1,
    "max_depth": 2
  },
  "impacted_resources": [
    {
      "resource": {
        "provider": "kubernetes",
        "resource_type": "ingress",
        "provider_id": "k3d-license-service/default/order-ingress"
      },
      "depth": 1,
      "impact_type": "ROUTING_FAILURE",
      "impact_reason": "direct routing target will be deleted"
    },
    {
      "resource": {
        "provider": "kubernetes",
        "resource_type": "service",
        "provider_id": "k3d-license-service/default/gateway-service"
      },
      "depth": 2,
      "impact_type": "UPSTREAM_CALLER",
      "impact_reason": "transitively routes to impacted ingress"
    }
  ],
  "relationships": [
    {
      "relationship": {
        "source": {
          "provider": "kubernetes",
          "resource_type": "ingress",
          "provider_id": "k3d-license-service/default/order-ingress"
        },
        "target": {
          "provider": "kubernetes",
          "resource_type": "service",
          "provider_id": "k3d-license-service/default/order-service"
        },
        "kind": "ROUTES_TO",
        "category": "TRAFFIC"
      },
      "state": "ACTIVE",
      "evidence_ids": [
        "13b839bb-1324-4f2b-871d-55734a6e87f1"
      ]
    }
  ],
  "paths": [
    {
      "resources": [
        {
          "provider": "kubernetes",
          "resource_type": "service",
          "provider_id": "k3d-license-service/default/order-service"
        },
        {
          "provider": "kubernetes",
          "resource_type": "ingress",
          "provider_id": "k3d-license-service/default/order-ingress"
        }
      ],
      "relationships": [
        {
          "source": {
            "provider": "kubernetes",
            "resource_type": "ingress",
            "provider_id": "k3d-license-service/default/order-ingress"
          },
          "target": {
            "provider": "kubernetes",
            "resource_type": "service",
            "provider_id": "k3d-license-service/default/order-service"
          },
          "kind": "ROUTES_TO",
          "category": "TRAFFIC"
        }
      ]
    }
  ],
  "evidence": [
    {
      "id": "13b839bb-1324-4f2b-871d-55734a6e87f1",
      "source": {
        "provider": "kubernetes",
        "collector": "k8s-runtime"
      },
      "observed_at": "2026-10-18T10:00:00Z",
      "observation_type": "RESOURCE_REFERENCE"
    }
  ],
  "change_assessment": {
    "change_type": "DELETE",
    "impact_nature": "SERVICE_DISRUPTION",
    "assumptions": [
      "Assumes direct and transitive callers rely on active service reachability"
    ],
    "limitations": [
      "External clients outside the monitored cluster are not observed"
    ]
  },
  "completeness": {
    "is_complete": true,
    "status": "COMPLETE"
  }
}
```

---

## 6. System Diagnostics & Recovery Endpoints

### `GET /api/v1/system/health`
- **Port**: `8080`
- **Purpose**: Returns health status and details for API, Database, Core Engine, and Collector.
- **Response `200 OK`**:
```json
{
  "status": "healthy",
  "timestamp": "2026-10-18T10:15:30.123456789Z",
  "uptime": 930.2,
  "environment": "development",
  "components": {
    "api": {
      "status": "healthy",
      "details": {
        "port": 8080,
        "version": "0.1.0-dev"
      }
    },
    "database": {
      "status": "healthy",
      "details": {
        "connected": true,
        "database": "whatbreaks",
        "host": "localhost",
        "port": 5432
      }
    },
    "core_engine": {
      "status": "healthy",
      "details": {
        "connected": true,
        "transport": "grpc (127.0.0.1:50051)"
      }
    },
    "collector": {
      "status": "healthy",
      "details": {
        "alive": true,
        "ready": true,
        "connected": true,
        "cluster_id": "k3d-license-service",
        "collection_status": "synced",
        "has_synced": true
      }
    }
  }
}
```

---

### `GET /api/v1/system/diagnose`
- **Port**: `8080`
- **Purpose**: Runs automated read-only verification checks across system components.
- **Query Parameters**:
  - `workspace_id` *(optional)*: Workspace UUID.
  - `component` *(optional)*: Target specific component (`collector`, `database`, `core_engine`, `api`).
- **Response `200 OK`**:
```json
{
  "workspace_id": "00000000-0000-0000-0000-000000000001",
  "timestamp": "2026-10-18T10:16:00.123456789Z",
  "component": "collector",
  "results": {
    "collector": {
      "component": "collector",
      "status": "healthy",
      "timestamp": "2026-10-18T10:16:00.123456789Z",
      "steps": [
        {
          "name": "Kubernetes API Reachability (/version)",
          "status": "ok",
          "message": "Connected (v1.28.2+k3s1, platform: linux/arm64)",
          "duration_ms": 12,
          "details": {
            "base_url": "http://127.0.0.1:8001",
            "status_code": 200,
            "version": "v1.28.2+k3s1"
          }
        },
        {
          "name": "Readiness Probe Check (/readyz)",
          "status": "ok",
          "message": "Kubernetes API server reports healthy readiness (readyz OK)",
          "duration_ms": 8,
          "details": {
            "status_code": 200
          }
        }
      ],
      "analysis": {
        "probable_cause": "Component verified healthy; all diagnostic steps passed successfully.",
        "confidence": "HIGH",
        "category": "NONE",
        "supporting_evidence": [
          "Step 1 'Kubernetes API Reachability (/version)' returned ok",
          "Step 2 'Readiness Probe Check (/readyz)' returned ok"
        ],
        "recommended_action": "No remediation required. Collector is connected and operational.",
        "fix_type": "NONE",
        "fix_available": false,
        "mutating": false
      }
    }
  }
}
```

---

### `POST /api/v1/system/recover`
- **Port**: `8080`
- **Purpose**: Executes a safe, guided recovery action on a degraded component.
- **Headers**:
  - `Content-Type: application/json`
  - `X-CSRF-Token: <token>`
  - `X-Workspace-ID: <workspace-uuid>`
- **Request Body**:
```json
{
  "component": "collector",
  "action": "retry_probe",
  "confirmed": true
}
```
*Supported Actions by Component:*
- `collector`: `retry_probe`, `restart_collector` *(requires `"confirmed": true`)*, `sync_discovery`.
- `database`: `reconnect`, `retry_probe`.
- `core_engine`: `retry_connection`, `retry_probe`.

- **Response `200 OK`**:
```json
{
  "workspace_id": "00000000-0000-0000-0000-000000000001",
  "component": "collector",
  "action": "retry_probe",
  "status": "recovered",
  "verified_healthy": true,
  "message": "Collector verified cluster reachability and is now healthy.",
  "duration_ms": 25,
  "health": {
    "status": "healthy"
  }
}
```
