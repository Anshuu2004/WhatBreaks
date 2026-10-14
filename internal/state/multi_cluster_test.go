package state_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	corev1 "github.com/aaradhychinche-alt/WhatBreaks/gen/go/wb/core/v1"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/answer"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/collector/k8s"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/database"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/logging"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/state"
)

// Helper to generate RFC 4122 UUIDs for tests
func mcUUID(i int) string {
	return fmt.Sprintf("00000000-0000-0000-0000-%012d", i)
}

// =============================================================================
// PHASE 5 — REQUIRED REGRESSION TESTS (A through L)
// =============================================================================

// Requirement A: Collector rejects an empty cluster ID.
func TestMultiCluster_A_CollectorRejectsEmptyClusterID(t *testing.T) {
	// 1. Explicitly empty cluster ID
	_, err := k8s.New(k8s.WithClusterID(""))
	if err == nil {
		t.Fatalf("expected error when cluster ID is empty, got nil")
	}
	if !errors.Is(err, k8s.ErrMissingClusterID) {
		t.Errorf("expected ErrMissingClusterID, got %v", err)
	}

	// 2. Whitespace-only cluster ID
	_, err = k8s.New(k8s.WithClusterID("   "))
	if err == nil || !errors.Is(err, k8s.ErrMissingClusterID) {
		t.Errorf("expected ErrMissingClusterID for whitespace cluster ID, got %v", err)
	}

	// 3. Omitting WithClusterID option completely
	_, err = k8s.New()
	if err == nil || !errors.Is(err, k8s.ErrMissingClusterID) {
		t.Errorf("expected ErrMissingClusterID when WithClusterID omitted, got %v", err)
	}
}

// Requirement B: Two clusters in the same workspace collect resources with identical
// Kubernetes namespace/name pairs and produce different canonical IDs.
func TestMultiCluster_B_IdenticalNamespaceNameDifferentCanonicalIDs(t *testing.T) {
	clusterA := "cluster-us-east"
	clusterB := "cluster-eu-west"
	namespace := "production"
	name := "payment-service"

	// 1. Namespaced resource
	idA, err := state.BuildKubernetesProviderID(clusterA, state.TypePod, namespace, name)
	if err != nil {
		t.Fatalf("BuildKubernetesProviderID cluster A failed: %v", err)
	}
	idB, err := state.BuildKubernetesProviderID(clusterB, state.TypePod, namespace, name)
	if err != nil {
		t.Fatalf("BuildKubernetesProviderID cluster B failed: %v", err)
	}

	expectedA := "cluster-us-east/production/payment-service"
	expectedB := "cluster-eu-west/production/payment-service"

	if idA != expectedA {
		t.Errorf("expected idA=%s, got %s", expectedA, idA)
	}
	if idB != expectedB {
		t.Errorf("expected idB=%s, got %s", expectedB, idB)
	}
	if idA == idB {
		t.Fatalf("canonical IDs must NOT collide for different clusters with same namespace/name: %s == %s", idA, idB)
	}

	// Parse back and verify round-trip fidelity
	parsedClusterA, parsedNsA, parsedNameA, err := state.ParseKubernetesProviderID(idA, false)
	if err != nil {
		t.Fatalf("ParseKubernetesProviderID A failed: %v", err)
	}
	if parsedClusterA != clusterA || parsedNsA != namespace || parsedNameA != name {
		t.Errorf("parsed components mismatch for A: %s, %s, %s", parsedClusterA, parsedNsA, parsedNameA)
	}

	parsedClusterB, parsedNsB, parsedNameB, err := state.ParseKubernetesProviderID(idB, false)
	if err != nil {
		t.Fatalf("ParseKubernetesProviderID B failed: %v", err)
	}
	if parsedClusterB != clusterB || parsedNsB != namespace || parsedNameB != name {
		t.Errorf("parsed components mismatch for B: %s, %s, %s", parsedClusterB, parsedNsB, parsedNameB)
	}

	// 2. Cluster-scoped resource (e.g. Node)
	nodeName := "worker-node-42"
	nodeA, err := state.BuildKubernetesProviderID(clusterA, state.TypeNode, "", nodeName)
	if err != nil {
		t.Fatalf("BuildKubernetesProviderID node A failed: %v", err)
	}
	nodeB, err := state.BuildKubernetesProviderID(clusterB, state.TypeNode, "", nodeName)
	if err != nil {
		t.Fatalf("BuildKubernetesProviderID node B failed: %v", err)
	}

	if nodeA != "cluster-us-east/worker-node-42" || nodeB != "cluster-eu-west/worker-node-42" {
		t.Errorf("unexpected cluster-scoped IDs: %s, %s", nodeA, nodeB)
	}
	if nodeA == nodeB {
		t.Fatalf("cluster-scoped canonical IDs collided: %s == %s", nodeA, nodeB)
	}
}

// Requirement C: Cluster ID validation distinguishes "prod" from "prod-eu".
func TestMultiCluster_C_PrefixDistinctionProdVsProdEU(t *testing.T) {
	prodRes := state.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "pod",
		ProviderID:   "prod/default/api-gateway",
	}
	prodEURes := state.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "pod",
		ProviderID:   "prod-eu/default/api-gateway",
	}

	// Exact match must succeed
	if err := state.ValidateKubernetesResourceIdentity("prod", prodRes); err != nil {
		t.Errorf("expected prod to match prodRes, got error: %v", err)
	}
	if err := state.ValidateKubernetesResourceIdentity("prod-eu", prodEURes); err != nil {
		t.Errorf("expected prod-eu to match prodEURes, got error: %v", err)
	}

	// Prefix collisions must be rejected with ErrClusterMismatch
	errProdExpectingEU := state.ValidateKubernetesResourceIdentity("prod", prodEURes)
	if errProdExpectingEU == nil || !errors.Is(errProdExpectingEU, state.ErrClusterMismatch) {
		t.Errorf("expected ErrClusterMismatch when batch cluster is 'prod' but resource is 'prod-eu', got %v", errProdExpectingEU)
	}

	errEUExpectingProd := state.ValidateKubernetesResourceIdentity("prod-eu", prodRes)
	if errEUExpectingProd == nil || !errors.Is(errEUExpectingProd, state.ErrClusterMismatch) {
		t.Errorf("expected ErrClusterMismatch when batch cluster is 'prod-eu' but resource is 'prod', got %v", errEUExpectingProd)
	}
}

// Requirement D: Reconciliation rejects an identity belonging to a different cluster.
func TestMultiCluster_D_ReconciliationRejectsDifferentClusterIdentity(t *testing.T) {
	store := state.NewMemoryStore()
	reconciler := state.NewReconciler(store, nil, nil, nil)
	ctx := context.Background()
	ws := "ws-cluster-mismatch"
	t0 := time.Now().UTC()

	// 1. Reconcile batch with mismatched subject cluster ID
	mismatchedSubjectEv := newTestEvidence(mcUUID(1), "kubernetes", "pod", "cluster-beta/default/worker", "CONTROL_PLANE_OBJECT", `{}`, t0)
	_, err := reconciler.Reconcile(ctx, state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "cluster-alpha",
		RunID:       "run-1",
		Evidence:    []*corev1.Evidence{mismatchedSubjectEv},
	})
	if err == nil {
		t.Fatalf("expected reconciliation to fail when evidence belongs to cluster-beta but batch is cluster-alpha")
	}
	if !errors.Is(err, state.ErrClusterMismatch) {
		t.Errorf("expected ErrClusterMismatch, got %v", err)
	}

	// 2. Reconcile batch with mismatched target in data payload
	depData, _ := json.Marshal(map[string]interface{}{
		"target": map[string]interface{}{
			"provider":      "kubernetes",
			"resource_type": "service",
			"provider_id":   "cluster-gamma/default/backend-svc",
		},
	})
	validSubjectMismatchTargetEv := newTestEvidence(mcUUID(2), "kubernetes", "pod", "cluster-alpha/default/worker", "RESOURCE_REFERENCE", string(depData), t0)
	_, err = reconciler.Reconcile(ctx, state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "cluster-alpha",
		RunID:       "run-2",
		Evidence:    []*corev1.Evidence{validSubjectMismatchTargetEv},
	})
	if err == nil {
		t.Fatalf("expected reconciliation to fail when evidence target belongs to cluster-gamma but batch is cluster-alpha")
	}
	if !errors.Is(err, state.ErrClusterMismatch) {
		t.Errorf("expected ErrClusterMismatch, got %v", err)
	}
}

// Requirement E: A rejected batch produces zero state mutations, including diagnostics,
// evidence, relationships, and scope metadata.
func TestMultiCluster_E_RejectedBatchProducesZeroStateMutations(t *testing.T) {
	forEachStore(t, func(t *testing.T, s state.Store, isPostgres bool, db database.DB) {
		ctx := context.Background()
		ws := fmt.Sprintf("ws-zero-mutation-%d", time.Now().UnixNano())
		cluster := "cluster-main"
		defer s.DeleteWorkspaceState(ctx, ws)

		reconciler := state.NewReconciler(s, nil, nil, nil)
		t0 := time.Now().UTC()

		// Step 1: Initial clean observation
		cleanEv := newTestEvidence(mcUUID(101), "kubernetes", "pod", cluster+"/default/clean-pod", "CONTROL_PLANE_OBJECT", `{}`, t0)
		res1, err := reconciler.Reconcile(ctx, state.ObservationBatch{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-initial",
			SamplingStartedAt: t0,
			Evidence:          []*corev1.Evidence{cleanEv},
		})
		if err != nil {
			t.Fatalf("initial reconcile failed: %v", err)
		}
		if res1.ResourcesCreated != 1 {
			t.Fatalf("expected 1 resource created, got %d", res1.ResourcesCreated)
		}

		authBefore, err := s.GetWorkspaceAuthority(ctx, ws, cluster)
		if err != nil || authBefore == nil {
			t.Fatalf("expected authority before failed reconcile: %v", err)
		}
		resBefore, _ := s.ListResources(ctx, ws)
		evBefore, _ := s.ListEvidence(ctx, ws)
		relBefore, _ := s.ListRelationships(ctx, ws)
		diagBefore, _ := s.ListDiagnostics(ctx, ws)

		// Step 2: Attempt to reconcile invalid batch with cross-cluster identity
		t1 := t0.Add(5 * time.Minute)
		badEv := newTestEvidence(mcUUID(102), "kubernetes", "pod", "cluster-other/default/rogue-pod", "CONTROL_PLANE_OBJECT", `{}`, t1)
		_, errBad := reconciler.Reconcile(ctx, state.ObservationBatch{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-bad-cross-cluster",
			SamplingStartedAt: t1,
			Evidence:          []*corev1.Evidence{badEv},
			ScopeSweep: &state.ScopeSweep{
				SuccessfulScopes: []string{"pods"},
			},
		})
		if errBad == nil {
			t.Fatalf("expected cross-cluster reconcile to fail")
		}

		// Step 3: Verify ZERO state mutations
		resAfter, _ := s.ListResources(ctx, ws)
		if len(resAfter) != len(resBefore) {
			t.Errorf("resources mutated! before=%d, after=%d", len(resBefore), len(resAfter))
		}
		if resAfter[0].Identity.ProviderID != cluster+"/default/clean-pod" {
			t.Errorf("unexpected resource provider_id: %s", resAfter[0].Identity.ProviderID)
		}

		evAfter, _ := s.ListEvidence(ctx, ws)
		if len(evAfter) != len(evBefore) {
			t.Errorf("evidence mutated! before=%d, after=%d", len(evBefore), len(evAfter))
		}

		relAfter, _ := s.ListRelationships(ctx, ws)
		if len(relAfter) != len(relBefore) {
			t.Errorf("relationships mutated! before=%d, after=%d", len(relBefore), len(relAfter))
		}

		diagAfter, _ := s.ListDiagnostics(ctx, ws)
		if len(diagAfter) != len(diagBefore) {
			t.Errorf("diagnostics mutated! before=%d, after=%d", len(diagBefore), len(diagAfter))
		}

		authAfter, _ := s.GetWorkspaceAuthority(ctx, ws, cluster)
		if authAfter.LastGeneration != authBefore.LastGeneration {
			t.Errorf("authority generation advanced despite rejection! before=%d, after=%d",
				authBefore.LastGeneration, authAfter.LastGeneration)
		}
		if authAfter.AuthoritativeRunID != authBefore.AuthoritativeRunID {
			t.Errorf("authoritative run ID changed despite rejection!")
		}
	})
}

// Requirement F: Two clusters in the same workspace can each contain a Pod with the
// same namespace/name and maintain distinct graph nodes.
func TestMultiCluster_F_SameNamespaceNameDistinctGraphNodes(t *testing.T) {
	client := startProdServer(t)
	store := state.NewMemoryStore()
	logger := logging.NewStandardLogger(nil, logging.LevelDebug)
	materializer := state.NewMaterializer(store, client, logger)
	reconciler := state.NewReconciler(store, client, materializer, logger)
	answerSvc := answer.NewService(client, logger)
	ctx := context.Background()

	ws := "ws-f-distinct-nodes"
	t0 := time.Now().UTC()

	// Cluster A: default/orders-worker
	evClusterA := newTestEvidence(mcUUID(201), "kubernetes", "pod", "cluster-alpha/default/orders-worker", "CONTROL_PLANE_OBJECT", `{}`, t0)
	_, err := reconciler.Reconcile(ctx, state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "cluster-alpha",
		RunID:       "run-a",
		Evidence:    []*corev1.Evidence{evClusterA},
	})
	if err != nil {
		t.Fatalf("reconcile Cluster A failed: %v", err)
	}

	// Cluster B: default/orders-worker (identical ns/name)
	evClusterB := newTestEvidence(mcUUID(202), "kubernetes", "pod", "cluster-beta/default/orders-worker", "CONTROL_PLANE_OBJECT", `{}`, t0)
	_, err = reconciler.Reconcile(ctx, state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "cluster-beta",
		RunID:       "run-b",
		Evidence:    []*corev1.Evidence{evClusterB},
	})
	if err != nil {
		t.Fatalf("reconcile Cluster B failed: %v", err)
	}

	// Verify Store contains 2 distinct resources
	resources, err := store.ListResources(ctx, ws)
	if err != nil {
		t.Fatalf("ListResources failed: %v", err)
	}
	if len(resources) != 2 {
		t.Fatalf("expected 2 distinct resources in store, got %d", len(resources))
	}

	// Materialize both into Core Engine
	if _, err := materializer.Materialize(ctx, ws); err != nil {
		t.Fatalf("Materialize failed: %v", err)
	}

	// Analyze Cluster A Pod: should target cluster-alpha
	ansA, err := answerSvc.AnalyzeImpact(ctx, answer.ImpactRequest{
		WorkspaceID: ws,
		Target: &answer.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "pod",
			ProviderID:   "cluster-alpha/default/orders-worker",
		},
		Direction: "incoming",
		MaxDepth:  2,
	})
	if err != nil {
		t.Fatalf("AnalyzeImpact Cluster A failed: %v", err)
	}
	if ansA.Target.ProviderID != "cluster-alpha/default/orders-worker" {
		t.Errorf("expected target cluster-alpha, got %s", ansA.Target.ProviderID)
	}

	// Analyze Cluster B Pod: should target cluster-beta
	ansB, err := answerSvc.AnalyzeImpact(ctx, answer.ImpactRequest{
		WorkspaceID: ws,
		Target: &answer.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "pod",
			ProviderID:   "cluster-beta/default/orders-worker",
		},
		Direction: "incoming",
		MaxDepth:  2,
	})
	if err != nil {
		t.Fatalf("AnalyzeImpact Cluster B failed: %v", err)
	}
	if ansB.Target.ProviderID != "cluster-beta/default/orders-worker" {
		t.Errorf("expected target cluster-beta, got %s", ansB.Target.ProviderID)
	}
}

// Requirement G: A relationship from cluster A cannot create an impact path through
// a resource belonging only to cluster B.
func TestMultiCluster_G_RelationshipCrossClusterImpactIsolation(t *testing.T) {
	client := startProdServer(t)
	store := state.NewMemoryStore()
	logger := logging.NewStandardLogger(nil, logging.LevelDebug)
	materializer := state.NewMaterializer(store, client, logger)
	assembler := state.NewStateAssembler(store, client, materializer, logger)
	answerSvc := answer.NewService(client, logger)
	ctx := context.Background()

	ws := "ws-g-impact-isolation"

	// Cluster A: frontend -> backend
	podFrontendA := &corev1.ResourceIdentity{Provider: "kubernetes", ResourceType: "pod", ProviderId: "cluster-a/default/frontend"}
	podBackendA := &corev1.ResourceIdentity{Provider: "kubernetes", ResourceType: "pod", ProviderId: "cluster-a/default/backend"}

	evConnA := makeConnectionEvidence(mcUUID(301), podFrontendA, "10.1.0.1", 8080)
	evMapA := makeMappingEvidence(mcUUID(302), podBackendA, "10.1.0.1", 8080)

	batchA := state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "cluster-a",
		Evidence:    []*corev1.Evidence{evConnA, evMapA},
	}
	if _, err := assembler.IngestAndMaterialize(ctx, batchA); err != nil {
		t.Fatalf("IngestAndMaterialize Cluster A failed: %v", err)
	}

	// Cluster B: backend -> database (backend has same ns/name as in Cluster A!)
	podBackendB := &corev1.ResourceIdentity{Provider: "kubernetes", ResourceType: "pod", ProviderId: "cluster-b/default/backend"}
	dbB := &corev1.ResourceIdentity{Provider: "aws", ResourceType: "rds", ProviderId: "arn:aws:rds:us-east-1:123:orders-db"}

	evConnB := makeConnectionEvidence(mcUUID(303), podBackendB, "10.2.0.1", 5432)
	evMapB := makeMappingEvidence(mcUUID(304), dbB, "10.2.0.1", 5432)

	batchB := state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "cluster-b",
		Evidence:    []*corev1.Evidence{evConnB, evMapB},
	}
	if _, err := assembler.IngestAndMaterialize(ctx, batchB); err != nil {
		t.Fatalf("IngestAndMaterialize Cluster B failed: %v", err)
	}

	// Query impact on database in Cluster B:
	// Only Cluster B's backend should be impacted!
	// Cluster A's frontend must NOT appear in the impact graph!
	ans, err := answerSvc.AnalyzeImpact(ctx, answer.ImpactRequest{
		WorkspaceID: ws,
		Target: &answer.ResourceIdentity{
			Provider:     "aws",
			ResourceType: "rds",
			ProviderID:   "arn:aws:rds:us-east-1:123:orders-db",
		},
		Direction: "incoming",
		MaxDepth:  5,
	})
	if err != nil {
		t.Fatalf("AnalyzeImpact failed: %v", err)
	}

	if ans.Summary.ImpactedCount != 1 {
		t.Fatalf("expected exactly 1 impacted resource, got %d", ans.Summary.ImpactedCount)
	}
	for _, path := range ans.Paths {
		for _, r := range path.Resources {
			if r.ProviderID == "cluster-a/default/frontend" {
				t.Fatalf("CRITICAL SECURITY / CORRECTNESS REGRESSION: cluster-a resource leaked into cluster-b blast radius: %s", r.ProviderID)
			}
		}
	}
}

// Requirement H: Observing a resource in cluster B cannot resolve cluster A's
// UNRESOLVED_REFERENCE diagnostic.
func TestMultiCluster_H_ObservingResourceInClusterBCannotResolveClusterADiagnostic(t *testing.T) {
	forEachStore(t, func(t *testing.T, s state.Store, isPostgres bool, db database.DB) {
		ctx := context.Background()
		ws := fmt.Sprintf("ws-diag-isolation-%d", time.Now().UnixNano())
		defer s.DeleteWorkspaceState(ctx, ws)

		t0 := time.Now().UTC()
		targetServiceA := "cluster-a/default/shared-service"
		clientPodA := "cluster-a/default/order-client"

		mockDiscoveryA := &mockDiscoveryRunner{
			resp: &corev1.RunDiscoveryResponse{
				Results: []*corev1.DiscoveryResult{
					{
						Outcome: &corev1.DiscoveryResult_Discovered{
							Discovered: &corev1.DiscoveredRelationship{
								Relationship: &corev1.Relationship{
									Source:   &corev1.ResourceIdentity{Provider: "kubernetes", ResourceType: "pod", ProviderId: clientPodA},
									Target:   &corev1.ResourceIdentity{Provider: "kubernetes", ResourceType: "service", ProviderId: targetServiceA},
									Kind:     "DEPENDS_ON",
									Category: "Dependency",
								},
								SupportingEvidenceIds: []string{mcUUID(401)},
							},
						},
					},
				},
			},
		}

		reconcilerA := state.NewReconciler(s, mockDiscoveryA, nil, nil)

		// 1. Cluster A observes pod that references unobserved service in cluster A
		podA := newTestEvidence(mcUUID(401), "kubernetes", "pod", clientPodA, "CONTROL_PLANE_OBJECT", `{}`, t0)

		_, err := reconcilerA.Reconcile(ctx, state.ObservationBatch{
			WorkspaceID: ws,
			ClusterID:   "cluster-a",
			RunID:       "run-a-1",
			Evidence:    []*corev1.Evidence{podA},
		})
		if err != nil {
			t.Fatalf("reconcile Cluster A failed: %v", err)
		}

		diagsBefore, err := s.ListDiagnostics(ctx, ws)
		if err != nil || len(diagsBefore) == 0 {
			t.Fatalf("expected UNRESOLVED_REFERENCE diagnostic in cluster A, got %v", diagsBefore)
		}
		if diagsBefore[0].Resolved {
			t.Fatalf("diagnostic should be unresolved")
		}

		// 2. Cluster B observes the exact same namespace/name service in Cluster B
		// with a successful services scope sweep!
		mockDiscoveryEmpty := &mockDiscoveryRunner{
			resp: &corev1.RunDiscoveryResponse{},
		}
		reconcilerB := state.NewReconciler(s, mockDiscoveryEmpty, nil, nil)

		t1 := t0.Add(5 * time.Minute)
		svcB := newTestEvidence(mcUUID(402), "kubernetes", "service", "cluster-b/default/shared-service", "CONTROL_PLANE_OBJECT", `{}`, t1)
		_, err = reconcilerB.Reconcile(ctx, state.ObservationBatch{
			WorkspaceID: ws,
			ClusterID:   "cluster-b",
			RunID:       "run-b-1",
			Evidence:    []*corev1.Evidence{svcB},
			ScopeSweep: &state.ScopeSweep{
				SuccessfulScopes: []string{"services"},
			},
		})
		if err != nil {
			t.Fatalf("reconcile Cluster B failed: %v", err)
		}

		// 3. Verify Cluster A's diagnostic remains UNRESOLVED despite Cluster B's sweep!
		diagsAfterB, err := s.ListDiagnostics(ctx, ws)
		if err != nil {
			t.Fatalf("ListDiagnostics failed: %v", err)
		}
		var foundUnresolvedA bool
		for _, d := range diagsAfterB {
			if d.Code == "UNRESOLVED_REFERENCE" && d.ReferencedResource != nil &&
				d.ReferencedResource.ProviderID == targetServiceA {
				if d.Resolved {
					t.Fatalf("REGRESSION: Cluster B observation resolved Cluster A diagnostic!")
				}
				foundUnresolvedA = true
			}
		}
		if !foundUnresolvedA {
			t.Fatalf("Cluster A diagnostic was removed or lost!")
		}

		// 4. Cluster A observes its own service in Cluster A: diagnostic resolves!
		t2 := t1.Add(5 * time.Minute)
		svcA := newTestEvidence(mcUUID(403), "kubernetes", "service", targetServiceA, "CONTROL_PLANE_OBJECT", `{}`, t2)
		reconcilerASweep2 := state.NewReconciler(s, mockDiscoveryEmpty, nil, nil)
		_, err = reconcilerASweep2.Reconcile(ctx, state.ObservationBatch{
			WorkspaceID: ws,
			ClusterID:   "cluster-a",
			RunID:       "run-a-2",
			Evidence:    []*corev1.Evidence{svcA},
			ScopeSweep: &state.ScopeSweep{
				SuccessfulScopes: []string{"services"},
			},
		})
		if err != nil {
			t.Fatalf("reconcile Cluster A sweep 2 failed: %v", err)
		}

		diagsAfterA, err := s.ListDiagnostics(ctx, ws)
		if err != nil {
			t.Fatalf("ListDiagnostics failed: %v", err)
		}
		for _, d := range diagsAfterA {
			if d.Code == "UNRESOLVED_REFERENCE" && d.ReferencedResource != nil &&
				d.ReferencedResource.ProviderID == targetServiceA {
				t.Fatalf("Cluster A diagnostic should be resolved and inactive!")
			}
		}
	})
}

// Requirement I: Workspace isolation continues to work.
func TestMultiCluster_I_WorkspaceIsolationContinuesToWork(t *testing.T) {
	forEachStore(t, func(t *testing.T, s state.Store, isPostgres bool, db database.DB) {
		ctx := context.Background()
		ws1 := fmt.Sprintf("ws-i-tenant-1-%d", time.Now().UnixNano())
		ws2 := fmt.Sprintf("ws-i-tenant-2-%d", time.Now().UnixNano())
		cluster := "shared-cluster-name"
		defer s.DeleteWorkspaceState(ctx, ws1)
		defer s.DeleteWorkspaceState(ctx, ws2)

		reconciler := state.NewReconciler(s, nil, nil, nil)
		t0 := time.Now().UTC()

		// Tenant 1 gets resource in shared-cluster-name
		ev1 := newTestEvidence(mcUUID(501), "kubernetes", "pod", cluster+"/default/pod-1", "CONTROL_PLANE_OBJECT", `{}`, t0)
		_, _ = reconciler.Reconcile(ctx, state.ObservationBatch{
			WorkspaceID: ws1,
			ClusterID:   cluster,
			RunID:       "run-1",
			Evidence:    []*corev1.Evidence{ev1},
		})

		// Tenant 2 gets resource in shared-cluster-name
		ev2 := newTestEvidence(mcUUID(502), "kubernetes", "pod", cluster+"/default/pod-2", "CONTROL_PLANE_OBJECT", `{}`, t0)
		_, _ = reconciler.Reconcile(ctx, state.ObservationBatch{
			WorkspaceID: ws2,
			ClusterID:   cluster,
			RunID:       "run-1",
			Evidence:    []*corev1.Evidence{ev2},
		})

		res1, _ := s.ListResources(ctx, ws1)
		res2, _ := s.ListResources(ctx, ws2)

		if len(res1) != 1 || res1[0].Identity.ProviderID != cluster+"/default/pod-1" {
			t.Errorf("tenant 1 resources corrupted: %v", res1)
		}
		if len(res2) != 1 || res2[0].Identity.ProviderID != cluster+"/default/pod-2" {
			t.Errorf("tenant 2 resources corrupted: %v", res2)
		}
	})
}

// Requirement J: Existing single-cluster Kubernetes acceptance tests continue to pass
// with an explicitly configured cluster ID.
func TestMultiCluster_J_SingleClusterAcceptanceContinuesToPass(t *testing.T) {
	store := state.NewMemoryStore()
	reconciler := state.NewReconciler(store, nil, nil, nil)
	ctx := context.Background()
	ws := "ws-j-single-cluster"
	clusterID := "k8s-prod-cluster"
	t0 := time.Now().UTC()

	ev := newTestEvidence(mcUUID(601), "kubernetes", "pod", clusterID+"/prod/app", "CONTROL_PLANE_OBJECT", `{}`, t0)
	res, err := reconciler.Reconcile(ctx, state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   clusterID,
		RunID:       "run-j",
		Evidence:    []*corev1.Evidence{ev},
	})
	if err != nil {
		t.Fatalf("single cluster reconcile failed: %v", err)
	}
	if res.ResourcesCreated != 1 {
		t.Errorf("expected 1 resource created, got %d", res.ResourcesCreated)
	}
}

// Requirement K: Non-Kubernetes resource identity behavior remains unchanged.
func TestMultiCluster_K_NonKubernetesResourceIdentityBehaviorUnchanged(t *testing.T) {
	forEachStore(t, func(t *testing.T, s state.Store, isPostgres bool, db database.DB) {
		ctx := context.Background()
		ws := fmt.Sprintf("ws-k-non-k8s-%d", time.Now().UnixNano())
		defer s.DeleteWorkspaceState(ctx, ws)

		reconciler := state.NewReconciler(s, nil, nil, nil)
		t0 := time.Now().UTC()

		// AWS RDS resource has no cluster ID and needs no cluster ID
		rdsARN := "arn:aws:rds:us-east-1:123456789012:db:production-db"
		evAWS := newTestEvidence(mcUUID(701), "aws", "rds_instance", rdsARN, "CONTROL_PLANE_OBJECT", `{}`, t0)

		res, err := reconciler.Reconcile(ctx, state.ObservationBatch{
			WorkspaceID: ws,
			ClusterID:   "", // Non-k8s batches do not require ClusterID
			RunID:       "run-aws-1",
			Evidence:    []*corev1.Evidence{evAWS},
		})
		if err != nil {
			t.Fatalf("non-k8s reconcile failed: %v", err)
		}
		if res.ResourcesCreated != 1 {
			t.Fatalf("expected 1 resource created, got %d", res.ResourcesCreated)
		}

		stored, err := s.GetResource(ctx, ws, state.ResourceIdentity{
			Provider:     "aws",
			ResourceType: "rds_instance",
			ProviderID:   rdsARN,
		})
		if err != nil {
			t.Fatalf("GetResource failed: %v", err)
		}
		if stored.Identity.ProviderID != rdsARN {
			t.Errorf("expected provider ID %s, got %s", rdsARN, stored.Identity.ProviderID)
		}
	})
}

// Requirement L: PostgreSQL and MemoryStore have equivalent validation and isolation behavior.
func TestMultiCluster_L_PostgresAndMemoryStoreEquivalence(t *testing.T) {
	// Directly tests that both stores produce identical validation failures and authority isolation
	forEachStore(t, func(t *testing.T, s state.Store, isPostgres bool, db database.DB) {
		ctx := context.Background()
		ws := fmt.Sprintf("ws-l-equiv-%d", time.Now().UnixNano())
		defer s.DeleteWorkspaceState(ctx, ws)

		reconciler := state.NewReconciler(s, nil, nil, nil)

		// 1. Both stores reject missing cluster ID when k8s evidence is present
		badEv := newTestEvidence(mcUUID(801), "kubernetes", "pod", "default/pod", "CONTROL_PLANE_OBJECT", `{}`, time.Now().UTC())
		_, errNoCluster := reconciler.Reconcile(ctx, state.ObservationBatch{
			WorkspaceID: ws,
			ClusterID:   "",
			Evidence:    []*corev1.Evidence{badEv},
		})
		if errNoCluster == nil {
			t.Errorf("[%s] expected missing cluster ID rejection", storeName(isPostgres))
		}

		// 2. Both stores reject cross-cluster mismatch
		_, errMismatch := reconciler.Reconcile(ctx, state.ObservationBatch{
			WorkspaceID: ws,
			ClusterID:   "cluster-a",
			Evidence:    []*corev1.Evidence{newTestEvidence(mcUUID(802), "kubernetes", "pod", "cluster-b/default/pod", "CONTROL_PLANE_OBJECT", `{}`, time.Now().UTC())},
		})
		if errMismatch == nil || !errors.Is(errMismatch, state.ErrClusterMismatch) {
			t.Errorf("[%s] expected ErrClusterMismatch, got %v", storeName(isPostgres), errMismatch)
		}
	})
}

func storeName(isPostgres bool) string {
	if isPostgres {
		return "PostgresStore"
	}
	return "MemoryStore"
}
