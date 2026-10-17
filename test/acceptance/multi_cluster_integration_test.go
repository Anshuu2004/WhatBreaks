package acceptance_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	corev1 "github.com/aaradhychinche-alt/WhatBreaks/gen/go/wb/core/v1"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/answer"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/api"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/collector/k8s"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/logging"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/state"
	"google.golang.org/grpc"
)

// makeIsolatedKubeServer constructs a mock Kubernetes API server representing an isolated cluster.
func makeIsolatedKubeServer(clusterID string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		p := r.URL.Path

		switch {
		case p == "/api/v1/namespaces":
			_ = json.NewEncoder(w).Encode(k8s.ResourceList[k8s.Namespace]{
				Items: []k8s.Namespace{{ObjectMeta: k8s.ObjectMeta{Name: "production"}}},
			})
		case strings.Contains(p, "/deployments"):
			var vols []k8s.Volume
			if clusterID == "cluster-a" {
				vols = []k8s.Volume{
					{
						Name:      "cfg",
						ConfigMap: &k8s.ConfigMapVolumeSource{Name: "order-config"},
					},
				}
			} else {
				vols = []k8s.Volume{
					{
						Name:   "sec",
						Secret: &k8s.SecretVolumeSource{SecretName: "order-secret"},
					},
				}
			}
			_ = json.NewEncoder(w).Encode(k8s.ResourceList[k8s.Deployment]{
				Items: []k8s.Deployment{
					{
						ObjectMeta: k8s.ObjectMeta{Name: "order-service", Namespace: "production"},
						Spec: k8s.DeploymentSpec{
							Template: k8s.PodTemplateSpec{
								Spec: k8s.PodSpec{
									Volumes: vols,
								},
							},
						},
					},
				},
			})
		case strings.Contains(p, "/configmaps"):
			_ = json.NewEncoder(w).Encode(k8s.ResourceList[k8s.ConfigMap]{
				Items: []k8s.ConfigMap{
					{
						ObjectMeta: k8s.ObjectMeta{Name: "order-config", Namespace: "production"},
					},
				},
			})
		case strings.Contains(p, "/secrets"):
			_ = json.NewEncoder(w).Encode(k8s.ResourceList[k8s.SecretMetadata]{
				Items: []k8s.SecretMetadata{
					{
						ObjectMeta: k8s.ObjectMeta{Name: "order-secret", Namespace: "production"},
					},
				},
			})
		case strings.Contains(p, "/services"):
			_ = json.NewEncoder(w).Encode(k8s.ResourceList[k8s.Service]{
				Items: []k8s.Service{
					{
						ObjectMeta: k8s.ObjectMeta{Name: "order-service", Namespace: "production"},
						Spec:       k8s.ServiceSpec{ClusterIP: "10.0.0.50"},
					},
				},
			})
		case strings.Contains(p, "/endpointslices"):
			_ = json.NewEncoder(w).Encode(k8s.ResourceList[k8s.EndpointSlice]{
				Items: []k8s.EndpointSlice{
					{
						ObjectMeta: k8s.ObjectMeta{
							Name:      "order-slice",
							Namespace: "production",
							Labels:    map[string]string{"kubernetes.io/service-name": "order-service"},
						},
						Endpoints: []k8s.Endpoint{
							{Addresses: []string{"10.244.0.10"}},
						},
					},
				},
			})
		default:
			_, _ = w.Write([]byte(`{"items":[]}`))
		}
	}))
}

func TestMultiCluster_Integration_AllRequirements(t *testing.T) {
	ctx := context.Background()
	logger := logging.NewStandardLogger(nil, logging.LevelWarn)
	wsID := "00000000-0000-0000-0000-000000000001"
	store := state.NewMemoryStore()

	// Spin up two logically distinct Kubernetes cluster mock API servers
	serverA := makeIsolatedKubeServer("cluster-a")
	defer serverA.Close()

	serverB := makeIsolatedKubeServer("cluster-b")
	defer serverB.Close()

	// Initialize two independent collectors
	clientA, err := k8s.NewRESTClient(k8s.ClientConfig{BaseURL: serverA.URL, Logger: logger})
	if err != nil {
		t.Fatalf("failed to create clientA: %v", err)
	}
	collectorA, err := k8s.New(
		k8s.WithClient(clientA),
		k8s.WithClusterID("cluster-a"),
		k8s.WithWorkspaceID(wsID),
		k8s.WithNamespaces("production"),
		k8s.WithLogger(logger),
	)
	if err != nil {
		t.Fatalf("failed to create collectorA: %v", err)
	}

	clientB, err := k8s.NewRESTClient(k8s.ClientConfig{BaseURL: serverB.URL, Logger: logger})
	if err != nil {
		t.Fatalf("failed to create clientB: %v", err)
	}
	collectorB, err := k8s.New(
		k8s.WithClient(clientB),
		k8s.WithClusterID("cluster-b"),
		k8s.WithWorkspaceID(wsID),
		k8s.WithNamespaces("production"),
		k8s.WithLogger(logger),
	)
	if err != nil {
		t.Fatalf("failed to create collectorB: %v", err)
	}

	reconciler := state.NewReconciler(store, nil, nil, logger)

	// -------------------------------------------------------------------------
	// 1. Collect from Cluster A and reconcile
	// -------------------------------------------------------------------------
	evidenceA, err := collectorA.Collect(ctx)
	if err != nil {
		t.Fatalf("collectorA Collect failed: %v", err)
	}
	_, err = reconciler.Reconcile(ctx, state.ObservationBatch{
		WorkspaceID: wsID,
		ClusterID:   "cluster-a",
		Evidence:    evidenceA,
	})
	if err != nil {
		t.Fatalf("reconcile A failed: %v", err)
	}

	// -------------------------------------------------------------------------
	// 2. Collect from Cluster B and reconcile into the same workspace
	// -------------------------------------------------------------------------
	evidenceB, err := collectorB.Collect(ctx)
	if err != nil {
		t.Fatalf("collectorB Collect failed: %v", err)
	}
	_, err = reconciler.Reconcile(ctx, state.ObservationBatch{
		WorkspaceID: wsID,
		ClusterID:   "cluster-b",
		Evidence:    evidenceB,
	})
	if err != nil {
		t.Fatalf("reconcile B failed: %v", err)
	}

	// -------------------------------------------------------------------------
	// REQUIREMENT 1 & 2:
	// Resources from cluster A and cluster B are assigned their correct cluster-qualified
	// canonical identities, and the same namespace/name in both clusters remains distinguishable.
	// -------------------------------------------------------------------------
	idA := state.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "deployment",
		ProviderID:   "cluster-a/production/order-service",
	}
	idB := state.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "deployment",
		ProviderID:   "cluster-b/production/order-service",
	}

	resA, err := store.GetResource(ctx, wsID, idA)
	if err != nil {
		t.Fatalf("GetResource idA failed: %v", err)
	}
	if resA.Identity.ProviderID != "cluster-a/production/order-service" {
		t.Errorf("expected cluster-a qualified ID, got %s", resA.Identity.ProviderID)
	}

	resB, err := store.GetResource(ctx, wsID, idB)
	if err != nil {
		t.Fatalf("GetResource idB failed: %v", err)
	}
	if resB.Identity.ProviderID != "cluster-b/production/order-service" {
		t.Errorf("expected cluster-b qualified ID, got %s", resB.Identity.ProviderID)
	}

	if resA.Identity.ProviderID == resB.Identity.ProviderID {
		t.Fatalf("identical namespace/name across clusters must produce distinct canonical IDs!")
	}

	// -------------------------------------------------------------------------
	// REQUIREMENT 3:
	// Selecting cluster A displays only cluster A resources; selecting cluster B displays only B.
	// -------------------------------------------------------------------------
	cfg := api.DefaultConfig()
	cfg.IsTest = true
	cfg.Store = store
	cfg.ConfiguredWorkspaceID = wsID
	cfg.Logger = logger

	apiServer := api.NewServer(cfg)

	// A) Query cluster-a
	reqA := httptest.NewRequest(http.MethodGet, "/api/v1/resources?workspace_id="+wsID+"&cluster=cluster-a", nil)
	recA := httptest.NewRecorder()
	apiServer.Handler().ServeHTTP(recA, reqA)
	if recA.Code != http.StatusOK {
		t.Fatalf("query cluster A failed: %d", recA.Code)
	}
	var respA api.ListResourcesResponse
	_ = json.Unmarshal(recA.Body.Bytes(), &respA)
	for _, r := range respA.Resources {
		if !strings.HasPrefix(r.ProviderID, "cluster-a/") {
			t.Errorf("resource from another cluster leaked into cluster-a query: %s", r.ProviderID)
		}
	}
	if respA.Total == 0 {
		t.Errorf("expected cluster-a resources, got 0")
	}

	// B) Query cluster-b
	reqB := httptest.NewRequest(http.MethodGet, "/api/v1/resources?workspace_id="+wsID+"&cluster=cluster-b", nil)
	recB := httptest.NewRecorder()
	apiServer.Handler().ServeHTTP(recB, reqB)
	if recB.Code != http.StatusOK {
		t.Fatalf("query cluster B failed: %d", recB.Code)
	}
	var respB api.ListResourcesResponse
	_ = json.Unmarshal(recB.Body.Bytes(), &respB)
	for _, r := range respB.Resources {
		if !strings.HasPrefix(r.ProviderID, "cluster-b/") {
			t.Errorf("resource from another cluster leaked into cluster-b query: %s", r.ProviderID)
		}
	}
	if respB.Total == 0 {
		t.Errorf("expected cluster-b resources, got 0")
	}

	// C) Query All clusters
	reqAll := httptest.NewRequest(http.MethodGet, "/api/v1/resources?workspace_id="+wsID, nil)
	recAll := httptest.NewRecorder()
	apiServer.Handler().ServeHTTP(recAll, reqAll)
	var respAll api.ListResourcesResponse
	_ = json.Unmarshal(recAll.Body.Bytes(), &respAll)
	if respAll.Total != respA.Total+respB.Total {
		t.Errorf("expected total %d, got %d", respA.Total+respB.Total, respAll.Total)
	}

	// -------------------------------------------------------------------------
	// REQUIREMENT 4:
	// Workspace isolation remains intact.
	// -------------------------------------------------------------------------
	wsOther := "00000000-0000-0000-0000-000000000099"
	otherResources, err := store.ListResources(ctx, wsOther)
	if err != nil {
		t.Fatalf("ListResources other workspace failed: %v", err)
	}
	if len(otherResources) != 0 {
		t.Errorf("expected 0 resources in empty workspace %s, got %d", wsOther, len(otherResources))
	}

	// -------------------------------------------------------------------------
	// REQUIREMENT 5:
	// Relationship evidence and impact analysis do not accidentally cross cluster boundaries.
	// -------------------------------------------------------------------------
	allRels, err := store.ListRelationships(ctx, wsID)
	if err != nil {
		t.Fatalf("ListRelationships failed: %v", err)
	}
	for _, rel := range allRels {
		clusterSrc, _, _, errSrc := state.ParseKubernetesProviderID(rel.Source.ProviderID, false)
		clusterTgt, _, _, errTgt := state.ParseKubernetesProviderID(rel.Target.ProviderID, false)
		if errSrc == nil && errTgt == nil {
			if clusterSrc != clusterTgt {
				t.Fatalf("detected cross-cluster relationship without link semantics: %s -> %s", rel.Source.ProviderID, rel.Target.ProviderID)
			}
		}
	}

	// Explicitly attempt to reconcile a cross-cluster relationship: must be rejected fail-closed
	crossRelBatch := state.AuthoritativeBatchParams{
		WorkspaceID:       wsID,
		ClusterID:         "cluster-a",
		RunID:             "run-cross-fail",
		SamplingStartedAt: time.Now().UTC(),
		Relationships: []state.Relationship{
			{
				WorkspaceID: wsID,
				Source: state.ResourceIdentity{
					Provider:     "kubernetes",
					ResourceType: "deployment",
					ProviderID:   "cluster-a/production/order-service",
				},
				Target: state.ResourceIdentity{
					Provider:     "kubernetes",
					ResourceType: "secret",
					ProviderID:   "cluster-b/production/order-secret", // Cross-cluster!
				},
				Kind:     "DEPENDS_ON",
				Category: "Dependency",
			},
		},
	}
	_, errCross := store.ReconcileAuthoritativeBatch(ctx, crossRelBatch)
	if errCross != nil {
		if !errors.Is(errCross, state.ErrCrossClusterRelationship) && !errors.Is(errCross, state.ErrClusterMismatch) {
			t.Errorf("expected ErrCrossClusterRelationship or ErrClusterMismatch, got %v", errCross)
		}
	} else {
		// If raw batch was stored, CheckWorkspaceIntegrity must detect the cross-cluster violation and fail closed
		errInteg := store.CheckWorkspaceIntegrity(ctx, wsID)
		if !errors.Is(errInteg, state.ErrCrossClusterRelationship) {
			t.Fatalf("expected CheckWorkspaceIntegrity to fail with ErrCrossClusterRelationship, got %v", errInteg)
		}
	}

	// -------------------------------------------------------------------------
	// REQUIREMENT 6:
	// Runtime CALLS edges are created ONLY from genuine runtime connection evidence,
	// NEVER from declarative Kubernetes configuration or EndpointSlice membership.
	// -------------------------------------------------------------------------
	for _, rel := range allRels {
		if strings.EqualFold(rel.Kind, "CALLS") {
			t.Fatalf("illegal fabrication of CALLS relationship from declarative Kubernetes configuration: %+v", rel)
		}
	}

	// -------------------------------------------------------------------------
	// REQUIREMENT 7:
	// A missing observation is NOT treated as deletion.
	// -------------------------------------------------------------------------
	omitBatch := state.AuthoritativeBatchParams{
		WorkspaceID:       wsID,
		ClusterID:         "cluster-a",
		RunID:             "run-omit-test",
		SamplingStartedAt: time.Now().UTC(),
		Resources: []state.Resource{
			{
				WorkspaceID: wsID,
				Identity: state.ResourceIdentity{
					Provider:     "kubernetes",
					ResourceType: "namespace",
					ProviderID:   "cluster-a/production",
				},
				FirstObservedAt: time.Now().UTC(),
				LastObservedAt:  time.Now().UTC(),
			},
		},
	}
	_, errOmit := store.ReconcileAuthoritativeBatch(ctx, omitBatch)
	if errOmit != nil {
		t.Fatalf("omitBatch reconcile failed: %v", errOmit)
	}

	// order-service must still exist!
	stillExists, err := store.GetResource(ctx, wsID, idA)
	if err != nil || stillExists == nil {
		t.Fatalf("missing observation was incorrectly treated as deletion! Resource disappeared: %v", err)
	}

	// -------------------------------------------------------------------------
	// REQUIREMENT 8:
	// Failure or partial collection does NOT appear as a successful, complete zero-impact analysis.
	// -------------------------------------------------------------------------
	mockCore := &mockCompletenessCoreClient{}
	ansSvc := answer.NewService(mockCore, logger)

	ans, err := ansSvc.AnalyzeImpact(ctx, answer.ImpactRequest{
		WorkspaceID: wsID,
		Target: &answer.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "deployment",
			ProviderID:   "cluster-a/production/unobserved-workload",
		},
		Direction: "incoming",
		MaxDepth:  3,
	})
	if err != nil {
		t.Logf("partial/unobserved query correctly failed closed: %v", err)
	} else {
		if ans.Completeness != nil && ans.Completeness.IsComplete && ans.Summary.ImpactedCount == 0 {
			t.Errorf("failure or partial collection must not claim complete zero-impact!")
		}
	}
}

type mockCompletenessCoreClient struct{}

func (m *mockCompletenessCoreClient) AnalyzeImpact(ctx context.Context, req *corev1.AnalyzeImpactRequest, opts ...grpc.CallOption) (*corev1.AnalyzeImpactResponse, error) {
	return &corev1.AnalyzeImpactResponse{
		Target: req.Target,
		Summary: &corev1.ImpactSummary{
			ImpactedCount: 0,
		},
		Completeness: &corev1.AnalysisCompleteness{
			IsComplete: false,
			Status:     corev1.CompletenessStatus_COMPLETENESS_STATUS_PARTIAL,
			Reasons:    []string{"target workload not observed in authoritative graph"},
		},
	}, nil
}

func (m *mockCompletenessCoreClient) InvalidateWorkspace(ctx context.Context, req *corev1.InvalidateWorkspaceRequest, opts ...grpc.CallOption) (*corev1.InvalidateWorkspaceResponse, error) {
	return &corev1.InvalidateWorkspaceResponse{}, nil
}
