package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aaradhychinche-alt/WhatBreaks/internal/api"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/collector/k8s"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/logging"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/state"
)

func TestListResources_SuccessAndFiltering(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	memStore := state.NewMemoryStore()

	now := time.Now().UTC()
	err := memStore.SaveResources(context.Background(), wsID, []state.Resource{
		{
			WorkspaceID: wsID,
			Identity: state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "deployment",
				ProviderID:   "k3d-cluster/default/backend",
			},
			FirstObservedAt: now.Add(-5 * time.Minute),
			LastObservedAt:  now,
		},
		{
			WorkspaceID: wsID,
			Identity: state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "pod",
				ProviderID:   "k3d-cluster/default/backend-pod-1",
			},
			FirstObservedAt: now.Add(-5 * time.Minute),
			LastObservedAt:  now,
		},
		{
			WorkspaceID: wsID,
			Identity: state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "service",
				ProviderID:   "k3d-cluster/default/backend-svc",
			},
			FirstObservedAt: now.Add(-5 * time.Minute),
			LastObservedAt:  now,
		},
	})
	if err != nil {
		t.Fatalf("failed to seed resources: %v", err)
	}

	cfg := api.DefaultConfig()
	cfg.IsTest = true
	cfg.Store = memStore
	cfg.ConfiguredWorkspaceID = wsID
	cfg.Logger = logging.NewStandardLogger(nil, logging.LevelWarn)

	srv := api.NewServer(cfg)

	// 1. List all resources
	req := httptest.NewRequest(http.MethodGet, "/api/v1/resources?workspace_id="+wsID, nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp api.ListResourcesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if resp.Total != 3 {
		t.Errorf("expected 3 resources, got %d", resp.Total)
	}
	if resp.WorkspaceID != wsID {
		t.Errorf("expected workspace ID %s, got %s", wsID, resp.WorkspaceID)
	}

	// 2. Filter by type
	reqType := httptest.NewRequest(http.MethodGet, "/api/v1/resources?workspace_id="+wsID+"&type=deployment", nil)
	recType := httptest.NewRecorder()
	srv.Handler().ServeHTTP(recType, reqType)

	if recType.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", recType.Code)
	}
	var respType api.ListResourcesResponse
	_ = json.Unmarshal(recType.Body.Bytes(), &respType)
	if respType.Total != 1 || respType.Resources[0].ResourceType != "deployment" {
		t.Errorf("expected 1 deployment, got %d", respType.Total)
	}

	// 3. Search by name
	reqSearch := httptest.NewRequest(http.MethodGet, "/api/v1/resources?workspace_id="+wsID+"&search=svc", nil)
	recSearch := httptest.NewRecorder()
	srv.Handler().ServeHTTP(recSearch, reqSearch)

	if recSearch.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", recSearch.Code)
	}
	var respSearch api.ListResourcesResponse
	_ = json.Unmarshal(recSearch.Body.Bytes(), &respSearch)
	if respSearch.Total != 1 || respSearch.Resources[0].ResourceType != "service" {
		t.Errorf("expected 1 service matching 'svc', got %d", respSearch.Total)
	}
}

func TestListResources_WorkspaceSecurity(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	attackerWS := "00000000-0000-0000-0000-000000000002"
	memStore := state.NewMemoryStore()

	cfg := api.DefaultConfig()
	cfg.IsTest = true
	cfg.Store = memStore
	cfg.ConfiguredWorkspaceID = wsID
	cfg.Logger = logging.NewStandardLogger(nil, logging.LevelWarn)

	srv := api.NewServer(cfg)

	// 1. Cross-workspace request targeting unauthorized workspace
	reqCross := httptest.NewRequest(http.MethodGet, "/api/v1/resources?workspace_id="+attackerWS, nil)
	recCross := httptest.NewRecorder()
	srv.Handler().ServeHTTP(recCross, reqCross)

	if recCross.Code != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden for unconfigured workspace, got %d", recCross.Code)
	}

	// 2. Malformed UUID
	reqBadUUID := httptest.NewRequest(http.MethodGet, "/api/v1/resources?workspace_id=invalid-uuid-123", nil)
	recBadUUID := httptest.NewRecorder()
	srv.Handler().ServeHTTP(recBadUUID, reqBadUUID)

	if recBadUUID.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for malformed UUID, got %d", recBadUUID.Code)
	}
}

func TestResourceDetail_SuccessAndProvenance(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	memStore := state.NewMemoryStore()

	deployID := state.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "deployment",
		ProviderID:   "k3d-cluster/default/backend",
	}
	podID := state.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "pod",
		ProviderID:   "k3d-cluster/default/backend-pod",
	}

	now := time.Now().UTC()
	_ = memStore.SaveResources(context.Background(), wsID, []state.Resource{
		{WorkspaceID: wsID, Identity: deployID, FirstObservedAt: now, LastObservedAt: now},
		{WorkspaceID: wsID, Identity: podID, FirstObservedAt: now, LastObservedAt: now},
	})

	relKey := state.RelationshipKey{
		Source: deployID,
		Target: podID,
		Kind:   "OWNS",
	}
	_ = memStore.SaveRelationships(context.Background(), wsID, []state.Relationship{
		{
			WorkspaceID:     wsID,
			Source:          deployID,
			Target:          podID,
			Kind:            "OWNS",
			Category:        "STRUCTURAL",
			FirstObservedAt: now,
			LastObservedAt:  now,
		},
	})
	_ = memStore.SaveProvenance(context.Background(), wsID, []state.ProvenanceAssociation{
		{
			WorkspaceID:  wsID,
			Relationship: relKey,
			EvidenceID:   "ev-owner-ref-123",
		},
	})

	cfg := api.DefaultConfig()
	cfg.IsTest = true
	cfg.Store = memStore
	cfg.ConfiguredWorkspaceID = wsID
	cfg.Logger = logging.NewStandardLogger(nil, logging.LevelWarn)

	srv := api.NewServer(cfg)

	url := "/api/v1/resources/detail?workspace_id=" + wsID +
		"&provider=kubernetes&resource_type=deployment&provider_id=k3d-cluster/default/backend"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}

	var detail api.ResourceDetailResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &detail); err != nil {
		t.Fatalf("failed to decode detail response: %v", err)
	}

	if detail.Resource.ProviderID != deployID.ProviderID {
		t.Errorf("expected provider ID %s, got %s", deployID.ProviderID, detail.Resource.ProviderID)
	}
	if len(detail.Relationships) != 1 {
		t.Fatalf("expected 1 relationship, got %d", len(detail.Relationships))
	}

	rel := detail.Relationships[0]
	if rel.Direction != "OUTGOING" {
		t.Errorf("expected direction OUTGOING, got %s", rel.Direction)
	}
	if rel.Kind != "OWNS" {
		t.Errorf("expected kind OWNS, got %s", rel.Kind)
	}
	if len(rel.EvidenceIDs) == 0 || rel.EvidenceIDs[0] != "ev-owner-ref-123" {
		t.Errorf("expected provenance evidence ID ev-owner-ref-123, got %v", rel.EvidenceIDs)
	}
}

func TestResourceDetail_NotFound(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	memStore := state.NewMemoryStore()

	cfg := api.DefaultConfig()
	cfg.IsTest = true
	cfg.Store = memStore
	cfg.ConfiguredWorkspaceID = wsID
	cfg.Logger = logging.NewStandardLogger(nil, logging.LevelWarn)

	srv := api.NewServer(cfg)

	url := "/api/v1/resources/detail?workspace_id=" + wsID +
		"&provider=kubernetes&resource_type=pod&provider_id=k3d-cluster/default/nonexistent-pod"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404 Not Found, got %d", rec.Code)
	}
}

func TestResourceDetail_LegacyID_Rejected(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	memStore := state.NewMemoryStore()

	cfg := api.DefaultConfig()
	cfg.IsTest = true
	cfg.Store = memStore
	cfg.ConfiguredWorkspaceID = wsID
	cfg.Logger = logging.NewStandardLogger(nil, logging.LevelWarn)

	srv := api.NewServer(cfg)

	// Single segment legacy ID for namespaced pod should be rejected with 422
	url := "/api/v1/resources/detail?workspace_id=" + wsID +
		"&provider=kubernetes&resource_type=pod&provider_id=legacy-pod"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 Unprocessable Entity, got %d", rec.Code)
	}

	var errResp map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if code, _ := errResp["code"].(string); code != "IDENTITY_INTEGRITY_REJECTED" {
		t.Errorf("expected code IDENTITY_INTEGRITY_REJECTED, got %s", code)
	}
}

func TestListResources_WorkspaceSecurity_NoAuthorizer_NoConfiguredWS(t *testing.T) {
	memStore := state.NewMemoryStore()

	cfg := api.DefaultConfig()
	cfg.IsTest = true
	cfg.Store = memStore
	cfg.ConfiguredWorkspaceID = "" // No configured workspace
	cfg.WorkspaceAuthorizer = nil  // No authorizer
	cfg.Logger = logging.NewStandardLogger(nil, logging.LevelWarn)

	srv := api.NewServer(cfg)

	// An arbitrary valid UUID must still be rejected!
	req := httptest.NewRequest(http.MethodGet, "/api/v1/resources?workspace_id=00000000-0000-0000-0000-000000000001", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden when neither authorizer nor configured WS exists, got %d", rec.Code)
	}
}

func TestListResources_ClusterFilteringAndClustersList(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	memStore := state.NewMemoryStore()
	now := time.Now().UTC()

	err := memStore.SaveResources(context.Background(), wsID, []state.Resource{
		{
			WorkspaceID: wsID,
			Identity: state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "deployment",
				ProviderID:   "cluster-a/default/backend",
			},
			FirstObservedAt: now,
			LastObservedAt:  now,
		},
		{
			WorkspaceID: wsID,
			Identity: state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "deployment",
				ProviderID:   "cluster-b/default/backend",
			},
			FirstObservedAt: now,
			LastObservedAt:  now,
		},
	})
	if err != nil {
		t.Fatalf("failed to seed resources: %v", err)
	}

	cfg := api.DefaultConfig()
	cfg.IsTest = true
	cfg.Store = memStore
	cfg.ConfiguredWorkspaceID = wsID
	cfg.Logger = logging.NewStandardLogger(nil, logging.LevelWarn)

	srv := api.NewServer(cfg)

	// 1. List all: should return both and include Clusters list ["cluster-a", "cluster-b"]
	req := httptest.NewRequest(http.MethodGet, "/api/v1/resources?workspace_id="+wsID, nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}
	var resp api.ListResourcesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Total != 2 {
		t.Errorf("expected 2 resources, got %d", resp.Total)
	}
	if len(resp.Clusters) != 2 || resp.Clusters[0] != "cluster-a" || resp.Clusters[1] != "cluster-b" {
		t.Errorf("expected clusters [cluster-a cluster-b], got %v", resp.Clusters)
	}

	// 2. Filter cluster-a: should only return cluster-a resource
	reqA := httptest.NewRequest(http.MethodGet, "/api/v1/resources?workspace_id="+wsID+"&cluster=cluster-a", nil)
	recA := httptest.NewRecorder()
	srv.Handler().ServeHTTP(recA, reqA)

	if recA.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", recA.Code)
	}
	var respA api.ListResourcesResponse
	if err := json.Unmarshal(recA.Body.Bytes(), &respA); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if respA.Total != 1 || respA.Resources[0].ProviderID != "cluster-a/default/backend" {
		t.Errorf("expected only cluster-a resource, got %+v", respA.Resources)
	}
}

func TestListDependencies_SuccessAndClusterFiltering(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	memStore := state.NewMemoryStore()
	now := time.Now().UTC()

	err := memStore.SaveRelationships(context.Background(), wsID, []state.Relationship{
		{
			WorkspaceID: wsID,
			Source: state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "deployment",
				ProviderID:   "cluster-a/prod/web",
			},
			Target: state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "service",
				ProviderID:   "cluster-a/prod/web-svc",
			},
			Kind:            "ROUTES_TO",
			Category:        "Network",
			FirstObservedAt: now,
			LastObservedAt:  now,
		},
		{
			WorkspaceID: wsID,
			Source: state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "deployment",
				ProviderID:   "cluster-b/prod/web",
			},
			Target: state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "service",
				ProviderID:   "cluster-b/prod/web-svc",
			},
			Kind:            "ROUTES_TO",
			Category:        "Network",
			FirstObservedAt: now,
			LastObservedAt:  now,
		},
	})
	if err != nil {
		t.Fatalf("failed to seed relationships: %v", err)
	}

	cfg := api.DefaultConfig()
	cfg.IsTest = true
	cfg.Store = memStore
	cfg.ConfiguredWorkspaceID = wsID
	cfg.Logger = logging.NewStandardLogger(nil, logging.LevelWarn)

	srv := api.NewServer(cfg)

	// 1. List all dependencies
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dependencies?workspace_id="+wsID, nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}
	var resp api.ListDependenciesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Total != 2 {
		t.Errorf("expected 2 dependencies, got %d", resp.Total)
	}

	// 2. Filter cluster-a
	reqA := httptest.NewRequest(http.MethodGet, "/api/v1/dependencies?workspace_id="+wsID+"&cluster=cluster-a", nil)
	recA := httptest.NewRecorder()
	srv.Handler().ServeHTTP(recA, reqA)

	if recA.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", recA.Code)
	}
	var respA api.ListDependenciesResponse
	if err := json.Unmarshal(recA.Body.Bytes(), &respA); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if respA.Total != 1 || respA.Relationships[0].Source.ProviderID != "cluster-a/prod/web" {
		t.Errorf("expected only cluster-a dependency, got %+v", respA.Relationships)
	}
}

func TestSystemHealth_Endpoint(t *testing.T) {
	cfg := api.DefaultConfig()
	cfg.IsTest = true
	cfg.Logger = logging.NewStandardLogger(nil, logging.LevelWarn)
	cfg.HealthPinger = &mockPinger{}
	cfg.CoreChecker = func(ctx context.Context) error {
		return nil
	}

	srv := api.NewServer(cfg)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/system/health", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}
	var resp api.SystemHealthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Status != "healthy" {
		t.Errorf("expected healthy status, got %s", resp.Status)
	}
	if resp.Components["api"].Status != "healthy" {
		t.Errorf("expected api healthy, got %s", resp.Components["api"].Status)
	}
	if resp.Components["database"].Status != "healthy" {
		t.Errorf("expected db healthy, got %s", resp.Components["database"].Status)
	}
	if resp.Components["core_engine"].Status != "healthy" {
		t.Errorf("expected core healthy, got %s", resp.Components["core_engine"].Status)
	}
}

type mockPinger struct{
	err error
}

func (m *mockPinger) Ping(ctx context.Context) error {
	return m.err
}

func TestSystemHealth_DatabaseStates(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"

	// 1. Missing pinger -> not_configured
	cfg := api.DefaultConfig()
	cfg.IsTest = true
	cfg.ConfiguredWorkspaceID = wsID
	cfg.Logger = logging.NewStandardLogger(nil, logging.LevelWarn)
	srv := api.NewServer(cfg)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/system/health", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	var resp api.SystemHealthResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Components["database"].Status != "not_configured" {
		t.Errorf("expected db not_configured, got %s", resp.Components["database"].Status)
	}

	// 2. Failed ping -> unavailable
	cfg.HealthPinger = &mockPinger{err: errors.New("connection refused")}
	srvFail := api.NewServer(cfg)
	recFail := httptest.NewRecorder()
	srvFail.Handler().ServeHTTP(recFail, req)
	var respFail api.SystemHealthResponse
	_ = json.Unmarshal(recFail.Body.Bytes(), &respFail)
	if respFail.Components["database"].Status != "unavailable" {
		t.Errorf("expected db unavailable, got %s", respFail.Components["database"].Status)
	}
	if respFail.Components["database"].Details["connected"] != false {
		t.Errorf("expected connected=false on failed ping")
	}
}

func TestListClusters_States(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"

	// 1. No collector configured -> not_configured
	cfg := api.DefaultConfig()
	cfg.IsTest = true
	cfg.ConfiguredWorkspaceID = wsID
	cfg.Logger = logging.NewStandardLogger(nil, logging.LevelWarn)
	srv := api.NewServer(cfg)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/clusters?workspace_id="+wsID, nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}
	var resp api.ListClustersResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if resp.Configured {
		t.Errorf("expected configured=false")
	}
	if resp.Status != "not_configured" {
		t.Errorf("expected status=not_configured, got %s", resp.Status)
	}

	// 2. Collector configured with mock client
	mockK8s := &mockK8sClient{}
	col, err := k8s.New(
		k8s.WithClient(mockK8s),
		k8s.WithClusterID("test-cluster"),
		k8s.WithWorkspaceID(wsID),
		k8s.WithLogger(logging.NewStandardLogger(nil, logging.LevelWarn)),
	)
	if err != nil {
		t.Fatalf("failed to create collector: %v", err)
	}
	_ = col.Start(context.Background())

	memStore := state.NewMemoryStore()
	cfg.Store = memStore
	cfg.K8sCollector = col

	srv2 := api.NewServer(cfg)

	// A. Reachable, but no sync yet -> pending_sync
	recA := httptest.NewRecorder()
	srv2.Handler().ServeHTTP(recA, req)
	var respA api.ListClustersResponse
	_ = json.Unmarshal(recA.Body.Bytes(), &respA)
	if len(respA.Clusters) != 1 || respA.Clusters[0].Status != "pending_sync" {
		t.Errorf("expected pending_sync, got %+v", respA.Clusters)
	}

	// B. Record authority with 0 resources -> connected_empty
	_, _ = memStore.ReconcileAuthoritativeBatch(context.Background(), state.AuthoritativeBatchParams{
		WorkspaceID:       wsID,
		ClusterID:         "test-cluster",
		RunID:             "run-1",
		SamplingStartedAt: time.Now().UTC(),
	})
	recB := httptest.NewRecorder()
	srv2.Handler().ServeHTTP(recB, req)
	var respB api.ListClustersResponse
	_ = json.Unmarshal(recB.Body.Bytes(), &respB)
	if len(respB.Clusters) != 1 || respB.Clusters[0].Status != "connected_empty" {
		t.Errorf("expected connected_empty, got %+v", respB.Clusters)
	}

	// C. Add resource -> connected_with_resources
	_ = memStore.SaveResources(context.Background(), wsID, []state.Resource{
		{
			WorkspaceID: wsID,
			Identity: state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "deployment",
				ProviderID:   "test-cluster/default/app",
			},
			FirstObservedAt: time.Now().UTC(),
			LastObservedAt:  time.Now().UTC(),
		},
	})
	recC := httptest.NewRecorder()
	srv2.Handler().ServeHTTP(recC, req)
	var respC api.ListClustersResponse
	_ = json.Unmarshal(recC.Body.Bytes(), &respC)
	if len(respC.Clusters) != 1 || respC.Clusters[0].Status != "connected_with_resources" {
		t.Errorf("expected connected_with_resources, got %+v", respC.Clusters)
	}
	if respC.Clusters[0].ResourceCount != 1 {
		t.Errorf("expected resource_count=1, got %d", respC.Clusters[0].ResourceCount)
	}

	// D. Unreachable cluster -> unreachable
	mockK8s.pingErr = errors.New("connection refused: 6443")
	recD := httptest.NewRecorder()
	srv2.Handler().ServeHTTP(recD, req)
	var respD api.ListClustersResponse
	_ = json.Unmarshal(recD.Body.Bytes(), &respD)
	if len(respD.Clusters) != 1 || respD.Clusters[0].Status != "unreachable" {
		t.Errorf("expected unreachable, got %+v", respD.Clusters)
	}
	if respD.Clusters[0].Connected {
		t.Errorf("expected connected=false on unreachable cluster")
	}
}
