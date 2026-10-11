package k8s

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aaradhychinche-alt/WhatBreaks/internal/discovery"
)

func createMockKubeServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		p := r.URL.Path

		switch {
		case p == "/api/v1/namespaces":
			_ = json.NewEncoder(w).Encode(ResourceList[Namespace]{
				Items: []Namespace{{ObjectMeta: ObjectMeta{Name: "app-prod"}}},
			})
		case p == "/api/v1/nodes":
			_ = json.NewEncoder(w).Encode(ResourceList[Node]{
				Items: []Node{{ObjectMeta: ObjectMeta{Name: "k8s-node-1"}}},
			})
		case p == "/api/v1/persistentvolumes":
			_ = json.NewEncoder(w).Encode(ResourceList[PersistentVolume]{
				Items: []PersistentVolume{{ObjectMeta: ObjectMeta{Name: "pv-storage-1"}}},
			})
		case strings.Contains(p, "/deployments"):
			_ = json.NewEncoder(w).Encode(ResourceList[Deployment]{
				Items: []Deployment{{ObjectMeta: ObjectMeta{Name: "web-deploy", Namespace: "app-prod"}}},
			})
		case strings.Contains(p, "/replicasets"):
			_ = json.NewEncoder(w).Encode(ResourceList[ReplicaSet]{
				Items: []ReplicaSet{{ObjectMeta: ObjectMeta{Name: "web-rs", Namespace: "app-prod"}}},
			})
		case strings.Contains(p, "/services"):
			_ = json.NewEncoder(w).Encode(ResourceList[Service]{
				Items: []Service{{
					ObjectMeta: ObjectMeta{Name: "web-svc", Namespace: "app-prod"},
					Spec:       ServiceSpec{ClusterIP: "10.0.0.100", Ports: []ServicePort{{Port: 80}}},
				}},
			})
		case strings.Contains(p, "/ingresses"):
			_ = json.NewEncoder(w).Encode(ResourceList[Ingress]{
				Items: []Ingress{{ObjectMeta: ObjectMeta{Name: "web-ing", Namespace: "app-prod"}}},
			})
		case strings.Contains(p, "/pods"):
			_ = json.NewEncoder(w).Encode(ResourceList[Pod]{
				Items: []Pod{{
					ObjectMeta: ObjectMeta{Name: "web-pod-xyz", Namespace: "app-prod"},
					Status:     PodStatus{PodIP: "10.244.0.5", Phase: "Running"},
				}},
			})
		case strings.Contains(p, "/configmaps"):
			_ = json.NewEncoder(w).Encode(ResourceList[ConfigMap]{
				Items: []ConfigMap{{ObjectMeta: ObjectMeta{Name: "app-cfg", Namespace: "app-prod"}}},
			})
		case strings.Contains(p, "/secrets"):
			_ = json.NewEncoder(w).Encode(ResourceList[SecretMetadata]{
				Items: []SecretMetadata{{ObjectMeta: ObjectMeta{Name: "app-sec", Namespace: "app-prod"}}},
			})
		case strings.Contains(p, "/persistentvolumeclaims"):
			_ = json.NewEncoder(w).Encode(ResourceList[PersistentVolumeClaim]{
				Items: []PersistentVolumeClaim{{ObjectMeta: ObjectMeta{Name: "app-pvc", Namespace: "app-prod"}}},
			})
		case p == "/readyz":
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
}

func TestCollector_ClusterWideCollect(t *testing.T) {
	server := createMockKubeServer(t)
	defer server.Close()

	client, err := NewRESTClient(ClientConfig{BaseURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	collector, err := New(
		WithClusterID("prod-cluster"),
		WithWorkspaceID("ws-123"),
		WithClusterWide(true),
		WithNamespaces("app-prod"),
		WithClient(client),
	)
	if err != nil {
		t.Fatalf("failed to create collector: %v", err)
	}

	ctx := context.Background()
	if err := collector.Start(ctx); err != nil {
		t.Fatalf("failed to start collector: %v", err)
	}
	defer func() { _ = collector.Stop(ctx) }()

	evs, err := collector.Collect(ctx)
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	if len(evs) == 0 {
		t.Fatalf("expected evidence to be collected, got 0")
	}

	// Verify cluster-scoped evidence exists
	var foundNS, foundNode, foundDeploy, foundRS, foundSvc bool
	for _, ev := range evs {
		if ev.Subject.ResourceType == TypeNamespace {
			foundNS = true
		}
		if ev.Subject.ResourceType == TypeNode {
			foundNode = true
		}
		if ev.Subject.ResourceType == TypeDeployment {
			foundDeploy = true
		}
		if ev.Subject.ResourceType == TypeReplicaSet {
			foundRS = true
		}
		if ev.Subject.ResourceType == TypeService {
			foundSvc = true
		}
	}

	if !foundNS || !foundNode || !foundDeploy || !foundRS || !foundSvc {
		t.Errorf("missing expected resources in sweep: ns=%v, node=%v, deploy=%v, rs=%v, svc=%v",
			foundNS, foundNode, foundDeploy, foundRS, foundSvc)
	}
}

func TestCollector_NamespaceScoped_GracefulRBACDenial(t *testing.T) {
	// Mock server that returns 403 Forbidden for "forbidden-ns" but 200 OK for "allowed-ns"
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "forbidden-ns") {
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]string{"message": "forbidden namespace"})
			return
		}
		if strings.Contains(r.URL.Path, "/deployments") {
			_ = json.NewEncoder(w).Encode(ResourceList[Deployment]{
				Items: []Deployment{{ObjectMeta: ObjectMeta{Name: "allowed-app", Namespace: "allowed-ns"}}},
			})
			return
		}
		_ = json.NewEncoder(w).Encode(ResourceList[Deployment]{Items: []Deployment{}})
	}))
	defer mockServer.Close()

	client, err := NewRESTClient(ClientConfig{BaseURL: mockServer.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	collector, err := New(
		WithClusterID("prod-1"),
		WithClusterWide(false),
		WithNamespaces("forbidden-ns", "allowed-ns"),
		WithClient(client),
	)
	if err != nil {
		t.Fatalf("failed to create collector: %v", err)
	}

	ctx := context.Background()

	// 1. Verify CollectDetailed records partial evidence without aborting, but marks required failures
	colResult, err := collector.CollectDetailed(ctx)
	if err != nil {
		t.Fatalf("CollectDetailed should not return fatal error on RBAC denial: %v", err)
	}
	if !colResult.HasRequiredFailures() {
		t.Fatalf("expected required failures due to forbidden-ns, got none")
	}
	if colResult.Status != discovery.CollectionStatusFailed {
		t.Errorf("expected CollectionStatusFailed, got %v", colResult.Status)
	}
	if colResult.Complete {
		t.Errorf("expected Complete to be false")
	}

	// Should have collected allowed-app despite forbidden-ns error
	foundAllowed := false
	for _, ev := range colResult.Evidence {
		if ev.Subject.ProviderId == "prod-1/allowed-ns/allowed-app" {
			foundAllowed = true
		}
	}
	if !foundAllowed {
		t.Errorf("expected to collect allowed-ns resources despite forbidden-ns RBAC error")
	}

	// 2. Verify legacy Collect returns gathered evidence alongside ErrRequiredScopeFailed
	evs, err := collector.Collect(ctx)
	if !errors.Is(err, discovery.ErrRequiredScopeFailed) {
		t.Fatalf("expected ErrRequiredScopeFailed, got: %v", err)
	}
	if len(evs) == 0 {
		t.Fatalf("expected evidence to still be returned with error, got 0")
	}
}

func TestCollector_ScopePolicy_Completeness(t *testing.T) {
	// Mock server that returns 403 Forbidden only for Ingresses (optional scope)
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		p := r.URL.Path
		if strings.Contains(p, "/ingresses") {
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]string{"message": "ingress forbidden"})
			return
		}
		if strings.Contains(p, "/pods") {
			_ = json.NewEncoder(w).Encode(ResourceList[Pod]{
				Items: []Pod{{ObjectMeta: ObjectMeta{Name: "pod-1", Namespace: "default"}, Status: PodStatus{Phase: "Running"}}},
			})
			return
		}
		_ = json.NewEncoder(w).Encode(ResourceList[Deployment]{Items: []Deployment{}})
	}))
	defer mockServer.Close()

	client, err := NewRESTClient(ClientConfig{BaseURL: mockServer.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	collector, err := New(
		WithClusterID("prod-1"),
		WithClusterWide(false),
		WithNamespaces("default"),
		WithClient(client),
	)
	if err != nil {
		t.Fatalf("failed to create collector: %v", err)
	}

	ctx := context.Background()
	colResult, err := collector.CollectDetailed(ctx)
	if err != nil {
		t.Fatalf("CollectDetailed failed: %v", err)
	}

	// Ingress failure is optional: Status should be PARTIAL, no required failures
	if colResult.HasRequiredFailures() {
		t.Errorf("did not expect required failures, got: %v", colResult.RequiredFailures)
	}
	if colResult.Status != discovery.CollectionStatusPartial {
		t.Errorf("expected CollectionStatusPartial, got: %v", colResult.Status)
	}
	if colResult.Complete {
		t.Errorf("expected Complete to be false")
	}
	if len(colResult.OptionalFailures) == 0 || colResult.OptionalFailures[0] != ScopeIngresses {
		t.Errorf("expected ScopeIngresses in OptionalFailures, got: %v", colResult.OptionalFailures)
	}
}

func TestCollector_CoreClientNotBound(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer mockServer.Close()

	client, _ := NewRESTClient(ClientConfig{BaseURL: mockServer.URL})
	collector, _ := New(WithClient(client))

	_, err := collector.SubmitToCore(context.Background(), nil)
	if !strings.Contains(err.Error(), "core client is not configured") {
		t.Errorf("expected core client error, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Step 3 Completeness Tests: Cases A, B, C, D, Cancellation, Secret Safety
// ---------------------------------------------------------------------------

// Case A: A successful list returning zero resources.
// Invariant: Empty evidence from a healthy cluster is authoritative completeness.
func TestCollector_Completeness_CaseA_SuccessfulEmptyList(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// All endpoints return 200 OK with empty items list
		_ = json.NewEncoder(w).Encode(ResourceList[Pod]{Items: []Pod{}})
	}))
	defer mockServer.Close()

	client, err := NewRESTClient(ClientConfig{BaseURL: mockServer.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	collector, err := New(
		WithClusterID("empty-cluster"),
		WithClusterWide(true),
		WithNamespaces("default"),
		WithClient(client),
	)
	if err != nil {
		t.Fatalf("failed to create collector: %v", err)
	}

	ctx := context.Background()
	colResult, err := collector.CollectDetailed(ctx)
	if err != nil {
		t.Fatalf("CollectDetailed failed on successful empty cluster: %v", err)
	}

	if len(colResult.Evidence) != 0 {
		t.Errorf("expected 0 evidence records, got %d", len(colResult.Evidence))
	}
	if !colResult.Complete {
		t.Errorf("expected Complete to be true for successful empty cluster")
	}
	if colResult.Status != discovery.CollectionStatusComplete {
		t.Errorf("expected CollectionStatusComplete, got %v", colResult.Status)
	}
	if colResult.HasRequiredFailures() {
		t.Errorf("expected zero required failures, got %v", colResult.RequiredFailures)
	}
	if len(colResult.FailedScopes) != 0 {
		t.Errorf("expected zero failed scopes, got %v", colResult.FailedScopes)
	}
	if len(colResult.SuccessfulScopes) == 0 {
		t.Errorf("expected non-empty SuccessfulScopes list for successful sweep")
	}
}

// Case B: A failed list returning no resources.
// Invariant: Scope failure must be recorded in ScopeErrors and mark collection FAILED.
func TestCollector_Completeness_CaseB_FailedEmptyList(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		p := r.URL.Path
		if strings.Contains(p, "/pods") {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"message": "apiserver internal error"})
			return
		}
		_ = json.NewEncoder(w).Encode(ResourceList[Deployment]{Items: []Deployment{}})
	}))
	defer mockServer.Close()

	client, err := NewRESTClient(ClientConfig{BaseURL: mockServer.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	collector, err := New(
		WithClusterID("fail-cluster"),
		WithClusterWide(false),
		WithNamespaces("default"),
		WithClient(client),
	)
	if err != nil {
		t.Fatalf("failed to create collector: %v", err)
	}

	ctx := context.Background()
	colResult, err := collector.CollectDetailed(ctx)
	if err != nil {
		t.Fatalf("CollectDetailed should not return fatal error on scope failure: %v", err)
	}

	if colResult.Complete {
		t.Errorf("expected Complete to be false when pods list fails")
	}
	if colResult.Status != discovery.CollectionStatusFailed {
		t.Errorf("expected CollectionStatusFailed, got %v", colResult.Status)
	}
	if !colResult.HasRequiredFailures() {
		t.Errorf("expected HasRequiredFailures to be true")
	}
	if len(colResult.RequiredFailures) == 0 || colResult.RequiredFailures[0] != ScopePods {
		t.Errorf("expected ScopePods in RequiredFailures, got %v", colResult.RequiredFailures)
	}
	if len(colResult.ScopeErrors) == 0 {
		t.Fatalf("expected ScopeErrors to record error details")
	}
	foundPodErr := false
	for _, se := range colResult.ScopeErrors {
		if se.Scope == ScopePods && se.Required && strings.Contains(se.Error, "500") {
			foundPodErr = true
		}
	}
	if !foundPodErr {
		t.Errorf("expected ScopeErrors to contain detailed pod error, got: %+v", colResult.ScopeErrors)
	}
}

// Case C: An optional scope failure.
// Invariant: Optional scope failure produces PARTIAL status and allows reconciliation.
func TestCollector_Completeness_CaseC_OptionalScopeFailure(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		p := r.URL.Path
		if strings.Contains(p, "/ingresses") {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"message": "the server could not find the requested resource"})
			return
		}
		if strings.Contains(p, "/pods") {
			_ = json.NewEncoder(w).Encode(ResourceList[Pod]{
				Items: []Pod{{ObjectMeta: ObjectMeta{Name: "pod-1", Namespace: "default"}, Status: PodStatus{Phase: "Running"}}},
			})
			return
		}
		_ = json.NewEncoder(w).Encode(ResourceList[Deployment]{Items: []Deployment{}})
	}))
	defer mockServer.Close()

	client, err := NewRESTClient(ClientConfig{BaseURL: mockServer.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	collector, err := New(
		WithClusterID("opt-cluster"),
		WithClusterWide(false),
		WithNamespaces("default"),
		WithClient(client),
	)
	if err != nil {
		t.Fatalf("failed to create collector: %v", err)
	}

	ctx := context.Background()
	colResult, err := collector.CollectDetailed(ctx)
	if err != nil {
		t.Fatalf("CollectDetailed failed: %v", err)
	}

	if colResult.Status != discovery.CollectionStatusPartial {
		t.Errorf("expected CollectionStatusPartial, got %v", colResult.Status)
	}
	if colResult.Complete {
		t.Errorf("expected Complete to be false")
	}
	if colResult.HasRequiredFailures() {
		t.Errorf("expected zero required failures, got %v", colResult.RequiredFailures)
	}
	if len(colResult.OptionalFailures) != 1 || colResult.OptionalFailures[0] != ScopeIngresses {
		t.Errorf("expected ScopeIngresses in OptionalFailures, got %v", colResult.OptionalFailures)
	}
}

// Case D: A required scope failure despite other scopes returning evidence.
// Invariant: Do NOT infer completeness from a non-empty evidence slice.
func TestCollector_Completeness_CaseD_RequiredScopeFailureWithAbundantEvidence(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		p := r.URL.Path
		// Secrets fails with 403 Forbidden
		if strings.Contains(p, "/secrets") {
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]string{"message": "forbidden"})
			return
		}
		// Pods returns 10 pods
		if strings.Contains(p, "/pods") {
			var pods []Pod
			for i := 1; i <= 10; i++ {
				pods = append(pods, Pod{
					ObjectMeta: ObjectMeta{Name: fmt.Sprintf("pod-%d", i), Namespace: "default"},
					Status:     PodStatus{Phase: "Running"},
				})
			}
			_ = json.NewEncoder(w).Encode(ResourceList[Pod]{Items: pods})
			return
		}
		// Deployments returns 5 deployments
		if strings.Contains(p, "/deployments") {
			var deps []Deployment
			for i := 1; i <= 5; i++ {
				deps = append(deps, Deployment{
					ObjectMeta: ObjectMeta{Name: fmt.Sprintf("deploy-%d", i), Namespace: "default"},
				})
			}
			_ = json.NewEncoder(w).Encode(ResourceList[Deployment]{Items: deps})
			return
		}
		_ = json.NewEncoder(w).Encode(ResourceList[Deployment]{Items: []Deployment{}})
	}))
	defer mockServer.Close()

	client, err := NewRESTClient(ClientConfig{BaseURL: mockServer.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	collector, err := New(
		WithClusterID("abundant-fail-cluster"),
		WithClusterWide(false),
		WithNamespaces("default"),
		WithClient(client),
	)
	if err != nil {
		t.Fatalf("failed to create collector: %v", err)
	}

	ctx := context.Background()
	colResult, err := collector.CollectDetailed(ctx)
	if err != nil {
		t.Fatalf("CollectDetailed failed: %v", err)
	}

	// CRITICAL INVARIANT: len(Evidence) > 0 does NOT mean complete!
	if len(colResult.Evidence) == 0 {
		t.Fatalf("expected evidence to be gathered from pods and deployments")
	}
	if colResult.Complete {
		t.Errorf("SAFETY VIOLATION: completeness inferred from non-empty evidence slice!")
	}
	if colResult.Status != discovery.CollectionStatusFailed {
		t.Errorf("expected CollectionStatusFailed, got %v", colResult.Status)
	}
	if !colResult.HasRequiredFailures() {
		t.Errorf("expected HasRequiredFailures to be true")
	}
	if len(colResult.RequiredFailures) == 0 || colResult.RequiredFailures[0] != ScopeSecrets {
		t.Errorf("expected ScopeSecrets in RequiredFailures, got %v", colResult.RequiredFailures)
	}
}

// Context cancellation aborts collection immediately.
func TestCollector_Completeness_ContextCancellationAborts(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ResourceList[Pod]{Items: []Pod{}})
	}))
	defer mockServer.Close()

	client, err := NewRESTClient(ClientConfig{BaseURL: mockServer.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	collector, err := New(
		WithClusterID("cancel-cluster"),
		WithClusterWide(false),
		WithNamespaces("default"),
		WithClient(client),
	)
	if err != nil {
		t.Fatalf("failed to create collector: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel context before collection

	_, err = collector.CollectDetailed(ctx)
	if err == nil {
		t.Fatalf("expected error on cancelled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got: %v", err)
	}
}

// Zero Secret Custody: Secret metadata must never contain secret values.
func TestCollector_SecretMetadata_MetadataOnlySafety(t *testing.T) {
	norm := NewNormalizer("cluster-prod", "ws-1", "collector-1")
	sec := SecretMetadata{
		ObjectMeta: ObjectMeta{Name: "db-secret", Namespace: "prod"},
		Type:       "Opaque",
		KeyNames:   []string{"username", "password", "api-key"},
	}

	evs := norm.NormalizeSecret(&sec)
	if len(evs) != 1 {
		t.Fatalf("expected 1 evidence record, got %d", len(evs))
	}

	dataStr := string(evs[0].Data)
	if strings.Contains(dataStr, "testsecret") || strings.Contains(dataStr, "supersecret") {
		t.Fatalf("SECURITY VIOLATION: secret values exposed in evidence data!")
	}
	if !strings.Contains(dataStr, "username") || !strings.Contains(dataStr, "password") {
		t.Errorf("expected key names to be recorded, got: %s", dataStr)
	}
}

