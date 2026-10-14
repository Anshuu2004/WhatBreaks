package state_test

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "github.com/aaradhychinche-alt/WhatBreaks/gen/go/wb/core/v1"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/auth"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/logging"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/state"
	"google.golang.org/grpc"
)

// ============================================================================
// AREA A: Detection: Legacy Namespaced Identity (2-segment)
// ============================================================================

func TestAreaA_LegacyNamespacedIdentity(t *testing.T) {
	// 2-segment namespaced resource missing cluster prefix
	cls, err := state.ClassifyKubernetesProviderID("pod", "default/my-pod")
	if err != nil {
		t.Fatalf("unexpected classification error: %v", err)
	}
	if !cls.IsLegacy {
		t.Errorf("expected IsLegacy=true, got false")
	}
	if cls.Cluster != "" {
		t.Errorf("expected empty cluster, got %q", cls.Cluster)
	}
	if cls.Namespace != "default" {
		t.Errorf("expected namespace 'default', got %q", cls.Namespace)
	}
	if cls.Name != "my-pod" {
		t.Errorf("expected name 'my-pod', got %q", cls.Name)
	}

	// ValidatePersistedKubernetesIdentity returns ErrLegacyNamespacedID
	res := state.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "pod",
		ProviderID:   "default/my-pod",
	}
	err = state.ValidatePersistedKubernetesIdentity(res)
	if !errors.Is(err, state.ErrLegacyNamespacedID) {
		t.Errorf("expected ErrLegacyNamespacedID, got %v", err)
	}
}

// ============================================================================
// AREA B: Detection: Legacy Cluster-Scoped Identity (1-segment)
// ============================================================================

func TestAreaB_LegacyClusterScopedIdentity(t *testing.T) {
	testCases := []struct {
		resType string
		id      string
	}{
		{"node", "worker-node-1"},
		{"clusterrole", "admin"},
		{"clusterrolebinding", "admin-binding"},
		{"persistentvolume", "pv-data-volume"},
		{"storageclass", "fast-ssd"},
		{"namespace", "production"},
	}

	for _, tc := range testCases {
		t.Run(tc.resType, func(t *testing.T) {
			cls, err := state.ClassifyKubernetesProviderID(tc.resType, tc.id)
			if err != nil {
				t.Fatalf("unexpected classification error: %v", err)
			}
			if !cls.IsLegacy {
				t.Errorf("expected IsLegacy=true for %s/%s", tc.resType, tc.id)
			}
			if cls.Cluster != "" {
				t.Errorf("expected empty cluster, got %q", cls.Cluster)
			}
			if cls.Name != tc.id {
				t.Errorf("expected name %q, got %q", tc.id, cls.Name)
			}

			res := state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: tc.resType,
				ProviderID:   tc.id,
			}
			err = state.ValidatePersistedKubernetesIdentity(res)
			if !errors.Is(err, state.ErrLegacyClusterScopedID) {
				t.Errorf("expected ErrLegacyClusterScopedID for %s/%s, got %v", tc.resType, tc.id, err)
			}
		})
	}
}

// ============================================================================
// AREA C: Detection: Ambiguous Identity (2-segment custom/unknown resource)
// ============================================================================

func TestAreaC_AmbiguousScopeIdentity(t *testing.T) {
	// A 2-segment ID for an unknown/custom resource could be:
	// 1. legacy namespaced: "<namespace>/<name>" (missing cluster)
	// 2. canonical cluster-scoped: "<cluster>/<name>"
	// Since scope is unknown, it MUST be classified as ambiguous and rejected.
	cls, err := state.ClassifyKubernetesProviderID("mycustomcrd.example.com", "foo/bar")
	if err != nil {
		t.Fatalf("unexpected classification error: %v", err)
	}
	if !cls.IsAmbiguous {
		t.Errorf("expected IsAmbiguous=true, got false")
	}

	res := state.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "mycustomcrd.example.com",
		ProviderID:   "foo/bar",
	}
	err = state.ValidatePersistedKubernetesIdentity(res)
	if !errors.Is(err, state.ErrAmbiguousResourceScope) {
		t.Errorf("expected ErrAmbiguousResourceScope, got %v", err)
	}
}

// ============================================================================
// AREA D: Detection: Malformed Identity
// ============================================================================

func TestAreaD_MalformedIdentity(t *testing.T) {
	testCases := []struct {
		name    string
		resType string
		id      string
	}{
		{"empty string", "pod", ""},
		{"whitespace only", "pod", "   "},
		{"leading slash", "pod", "/cluster-1/default/pod"},
		{"trailing slash", "pod", "cluster-1/default/pod/"},
		{"empty segment in middle", "pod", "cluster-1//pod"},
		{"whitespace in segment", "pod", "cluster 1/default/pod"},
		{"4 segments for namespaced", "pod", "cluster/ns/subns/pod"},
		{"1 segment for namespaced", "pod", "just-a-pod"},
		{"3 segments for cluster-scoped", "node", "cluster/ns/node"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := state.ClassifyKubernetesProviderID(tc.resType, tc.id)
			if err == nil {
				t.Fatalf("expected error for malformed ID %q, got nil", tc.id)
			}
			res := state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: tc.resType,
				ProviderID:   tc.id,
			}
			err = state.ValidatePersistedKubernetesIdentity(res)
			if err == nil {
				t.Fatalf("expected error in ValidatePersistedKubernetesIdentity for %q, got nil", tc.id)
			}
		})
	}
}

// ============================================================================
// AREA E: Validation: ValidateAssembledStateIntegrity Detects Legacy Resources
// ============================================================================

func TestAreaE_ValidateAssembledStateIntegrity_LegacyResources(t *testing.T) {
	// AssembledState containing 1 legacy namespaced resource
	st := &state.AssembledState{
		Resources: []state.Resource{
			{
				WorkspaceID: "ws-1",
				Identity: state.ResourceIdentity{
					Provider:     "kubernetes",
					ResourceType: "pod",
					ProviderID:   "default/legacy-pod",
				},
			},
		},
	}
	err := state.ValidateAssembledStateIntegrity(st)
	if !errors.Is(err, state.ErrLegacyStateIncompatible) {
		t.Errorf("expected ErrLegacyStateIncompatible, got %v", err)
	}
}

// ============================================================================
// AREA F: Validation: ValidateAssembledStateIntegrity Detects Legacy Relationships
// ============================================================================

func TestAreaF_ValidateAssembledStateIntegrity_LegacyRelationships(t *testing.T) {
	canonicalPod := state.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "pod",
		ProviderID:   "cluster-1/default/my-pod",
	}
	legacySvc := state.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "service",
		ProviderID:   "default/my-svc",
	}

	st := &state.AssembledState{
		Resources: []state.Resource{
			{WorkspaceID: "ws-1", Identity: canonicalPod},
		},
		Relationships: []state.Relationship{
			{
				WorkspaceID: "ws-1",
				Source:      canonicalPod,
				Target:      legacySvc,
				Kind:        "CONNECTS_TO",
			},
		},
	}
	err := state.ValidateAssembledStateIntegrity(st)
	if !errors.Is(err, state.ErrLegacyStateIncompatible) {
		t.Errorf("expected ErrLegacyStateIncompatible for legacy relationship target, got %v", err)
	}
}

// ============================================================================
// AREA G: Validation: ValidateAssembledStateIntegrity Detects Cross-Cluster Relationships
// ============================================================================

func TestAreaG_ValidateAssembledStateIntegrity_CrossCluster(t *testing.T) {
	podClusterA := state.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "pod",
		ProviderID:   "cluster-alpha/default/my-pod",
	}
	svcClusterB := state.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "service",
		ProviderID:   "cluster-beta/default/my-svc",
	}

	st := &state.AssembledState{
		Resources: []state.Resource{
			{WorkspaceID: "ws-1", Identity: podClusterA},
			{WorkspaceID: "ws-1", Identity: svcClusterB},
		},
		Relationships: []state.Relationship{
			{
				WorkspaceID: "ws-1",
				Source:      podClusterA,
				Target:      svcClusterB,
				Kind:        "CONNECTS_TO",
			},
		},
	}
	err := state.ValidateAssembledStateIntegrity(st)
	if !errors.Is(err, state.ErrCrossClusterRelationship) {
		t.Errorf("expected ErrCrossClusterRelationship, got %v", err)
	}
}

// ============================================================================
// AREA H: Validation: ValidateAssembledStateIntegrity Detects Legacy Diagnostics & Endpoints
// ============================================================================

func TestAreaH_ValidateAssembledStateIntegrity_DiagnosticsAndEndpoints(t *testing.T) {
	canonicalPod := state.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "pod",
		ProviderID:   "cluster-1/default/my-pod",
	}
	legacyPod := state.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "pod",
		ProviderID:   "default/my-pod",
	}

	// 1. Diagnostic with legacy subject
	stDiag := &state.AssembledState{
		Resources: []state.Resource{{WorkspaceID: "ws-1", Identity: canonicalPod}},
		Diagnostics: []state.DiagnosticItem{
			{
				WorkspaceID: "ws-1",
				Subject:     &legacyPod,
				Message:     "degraded",
			},
		},
	}
	if err := state.ValidateAssembledStateIntegrity(stDiag); !errors.Is(err, state.ErrLegacyStateIncompatible) {
		t.Errorf("expected ErrLegacyStateIncompatible for legacy diagnostic subject, got %v", err)
	}

	// 2. Diagnostic with legacy referenced resource
	stRef := &state.AssembledState{
		Resources: []state.Resource{{WorkspaceID: "ws-1", Identity: canonicalPod}},
		Diagnostics: []state.DiagnosticItem{
			{
				WorkspaceID:        "ws-1",
				Subject:            &canonicalPod,
				ReferencedResource: &legacyPod,
				Message:            "referenced legacy",
			},
		},
	}
	if err := state.ValidateAssembledStateIntegrity(stRef); !errors.Is(err, state.ErrLegacyStateIncompatible) {
		t.Errorf("expected ErrLegacyStateIncompatible for legacy referenced resource, got %v", err)
	}
}

// ============================================================================
// AREA J & K: Materialization Gate
// ============================================================================

type fakeCoreLoader struct {
	called bool
	req    *corev1.LoadStateRequest
}

func (f *fakeCoreLoader) LoadState(ctx context.Context, in *corev1.LoadStateRequest, opts ...grpc.CallOption) (*corev1.LoadStateResponse, error) {
	f.called = true
	f.req = in
	return &corev1.LoadStateResponse{WorkspaceId: in.WorkspaceId}, nil
}

func (f *fakeCoreLoader) InvalidateWorkspace(ctx context.Context, in *corev1.InvalidateWorkspaceRequest, opts ...grpc.CallOption) (*corev1.InvalidateWorkspaceResponse, error) {
	return &corev1.InvalidateWorkspaceResponse{WorkspaceId: in.WorkspaceId, Invalidated: true}, nil
}

func TestAreaJK_MaterializerGate(t *testing.T) {
	ctx := context.Background()
	wsID := "00000000-0000-0000-0000-000000000001"
	logger := logging.NewStandardLogger(nil, logging.LevelWarn)

	// Subtest J: State with legacy resource is refused BEFORE calling Core
	t.Run("RefuseLegacyState", func(t *testing.T) {
		memStore := state.NewMemoryStore()
		_ = memStore.SaveResources(ctx, wsID, []state.Resource{
			{
				WorkspaceID: wsID,
				Identity: state.ResourceIdentity{
					Provider:     "kubernetes",
					ResourceType: "pod",
					ProviderID:   "default/old-pod", // legacy
				},
			},
		})

		fakeLoader := &fakeCoreLoader{}
		mat := state.NewMaterializer(memStore, fakeLoader, logger)

		resp, err := mat.Materialize(ctx, wsID)
		if !errors.Is(err, state.ErrLegacyStateIncompatible) {
			t.Fatalf("expected ErrLegacyStateIncompatible, got resp=%v, err=%v", resp, err)
		}
		if fakeLoader.called {
			t.Fatalf("core engine LoadState MUST NOT be called for legacy state")
		}
	})

	// Subtest K: State with 100% canonical identities is materialized cleanly
	t.Run("AllowCanonicalState", func(t *testing.T) {
		memStore := state.NewMemoryStore()
		_ = memStore.SaveResources(ctx, wsID, []state.Resource{
			{
				WorkspaceID: wsID,
				Identity: state.ResourceIdentity{
					Provider:     "kubernetes",
					ResourceType: "pod",
					ProviderID:   "cluster-1/default/new-pod", // canonical
				},
			},
		})

		fakeLoader := &fakeCoreLoader{}
		mat := state.NewMaterializer(memStore, fakeLoader, logger)

		resp, err := mat.Materialize(ctx, wsID)
		if err != nil {
			t.Fatalf("unexpected materialization error: %v", err)
		}
		if resp == nil {
			t.Fatalf("expected successful materialization response")
		}
		if !fakeLoader.called {
			t.Fatalf("expected core engine LoadState to be called for canonical state")
		}
	})
}

// ============================================================================
// AREA L, M, N, O, P, Q, R: Administrative Legacy State Recovery
// ============================================================================

func TestAreaL_Recovery_RequiresExplicitAuthorization(t *testing.T) {
	memStore := state.NewMemoryStore()
	wsID := "00000000-0000-0000-0000-000000000001"

	_, err := memStore.RecoverWorkspaceLegacyState(context.Background(), state.LegacyRecoveryParams{
		WorkspaceID:          wsID,
		AuthorizeDestructive: false, // NOT authorized
	})
	if !errors.Is(err, state.ErrUnauthorizedDestructiveAction) {
		t.Fatalf("expected ErrUnauthorizedDestructiveAction, got %v", err)
	}
}

func TestAreaM_N_O_P_Q_R_MemoryStoreRecoveryComprehensive(t *testing.T) {
	ctx := auth.WithUser(context.Background(), &auth.User{ID: "admin-user", Role: auth.RoleAdmin})
	wsID := "00000000-0000-0000-0000-000000000001"
	store := state.NewMemoryStore()
	now := time.Now().UTC()

	// 1. Seed State:
	// - 2 Legacy K8s resources
	// - 1 Canonical K8s resource
	// - 1 Non-K8s resource (AWS VPC)
	legacyPod := state.Resource{
		WorkspaceID: wsID,
		Identity: state.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "pod",
			ProviderID:   "default/payment-pod",
		},
		FirstObservedAt: now,
		LastObservedAt:  now,
	}
	legacyNode := state.Resource{
		WorkspaceID: wsID,
		Identity: state.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "node",
			ProviderID:   "node-worker-1",
		},
		FirstObservedAt: now,
		LastObservedAt:  now,
	}
	canonicalPod := state.Resource{
		WorkspaceID: wsID,
		Identity: state.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "pod",
			ProviderID:   "prod-cluster/default/payment-pod-v2",
		},
		FirstObservedAt: now,
		LastObservedAt:  now,
	}
	awsVPC := state.Resource{
		WorkspaceID: wsID,
		Identity: state.ResourceIdentity{
			Provider:     "aws",
			ResourceType: "vpc",
			ProviderID:   "vpc-0123456789abcdef0",
		},
		FirstObservedAt: now,
		LastObservedAt:  now,
	}

	if err := store.SaveResources(ctx, wsID, []state.Resource{legacyPod, legacyNode, canonicalPod, awsVPC}); err != nil {
		t.Fatalf("failed to seed resources: %v", err)
	}

	// - Relationships
	legacyRel := state.Relationship{
		WorkspaceID:     wsID,
		Source:          legacyPod.Identity,
		Target:          legacyNode.Identity,
		Kind:            "RUNS_ON",
		Category:        "STRUCTURAL",
		FirstObservedAt: now,
		LastObservedAt:  now,
	}
	canonicalRel := state.Relationship{
		WorkspaceID: wsID,
		Source:      canonicalPod.Identity,
		Target: state.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "node",
			ProviderID:   "prod-cluster/node-worker-1",
		},
		Kind:            "RUNS_ON",
		Category:        "STRUCTURAL",
		FirstObservedAt: now,
		LastObservedAt:  now,
	}
	awsRel := state.Relationship{
		WorkspaceID: wsID,
		Source:      awsVPC.Identity,
		Target: state.ResourceIdentity{
			Provider:     "aws",
			ResourceType: "subnet",
			ProviderID:   "subnet-0123456789",
		},
		Kind:            "CONTAINS",
		Category:        "STRUCTURAL",
		FirstObservedAt: now,
		LastObservedAt:  now,
	}
	if err := store.SaveRelationships(ctx, wsID, []state.Relationship{legacyRel, canonicalRel, awsRel}); err != nil {
		t.Fatalf("failed to seed relationships: %v", err)
	}

	// - Provenance
	if err := store.SaveProvenance(ctx, wsID, []state.ProvenanceAssociation{
		{WorkspaceID: wsID, Relationship: state.RelationshipKey{Source: legacyPod.Identity, Target: legacyNode.Identity, Kind: "RUNS_ON"}, EvidenceID: "ev-legacy-1"},
		{WorkspaceID: wsID, Relationship: state.RelationshipKey{Source: canonicalPod.Identity, Target: canonicalRel.Target, Kind: "RUNS_ON"}, EvidenceID: "ev-canonical-1"},
	}); err != nil {
		t.Fatalf("failed to seed provenance: %v", err)
	}

	// - Evidence (Strictly Immutable per Invariant C)
	if err := store.SaveEvidence(ctx, wsID, []state.Evidence{
		{
			ID:          "ev-legacy-1",
			WorkspaceID: wsID,
			Source:      state.EvidenceSource{Provider: "kubernetes", Collector: "k8s-collector"},
			Data:        []byte(`{"status":"old"}`),
			ObservedAt:  now,
		},
		{
			ID:          "ev-canonical-1",
			WorkspaceID: wsID,
			Source:      state.EvidenceSource{Provider: "kubernetes", Collector: "k8s-collector"},
			Data:        []byte(`{"status":"new"}`),
			ObservedAt:  now,
		},
	}); err != nil {
		t.Fatalf("failed to seed evidence: %v", err)
	}

	// 2. Initial Integrity Check
	check1, err := store.InspectWorkspaceIntegrity(ctx, wsID)
	if err != nil {
		t.Fatalf("initial integrity check failed: %v", err)
	}
	if check1.IsClean {
		t.Fatalf("expected workspace to be UNCLEAN initially")
	}
	if check1.LegacyResourcesCount != 2 {
		t.Errorf("expected 2 legacy resources, got %d", check1.LegacyResourcesCount)
	}
	if check1.LegacyRelationshipsCount != 1 {
		t.Errorf("expected 1 legacy relationship, got %d", check1.LegacyRelationshipsCount)
	}

	// 3. Perform Administrative Recovery with Recollection Batch
	recollectedPod := state.Resource{
		WorkspaceID: wsID,
		Identity: state.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "pod",
			ProviderID:   "prod-cluster/default/payment-pod", // Authoritative canonical identity
		},
		FirstObservedAt: now,
		LastObservedAt:  now,
	}

	recResult, err := store.RecoverWorkspaceLegacyState(ctx, state.LegacyRecoveryParams{
		WorkspaceID:          wsID,
		AuthorizeDestructive: true,
		TargetClusterIDs:     []string{"prod-cluster"},
		Reason:               "Legacy identity upgrade",
		AuthoritativeBatch: &state.AuthoritativeBatch{
			Resources: []state.Resource{recollectedPod},
		},
	})
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	// Verify counts from recovery result
	if recResult.ArchivedResourcesCount != 2 {
		t.Errorf("expected 2 archived resources, got %d", recResult.ArchivedResourcesCount)
	}
	if recResult.ArchivedRelationshipsCount != 1 {
		t.Errorf("expected 1 archived relationship, got %d", recResult.ArchivedRelationshipsCount)
	}
	if recResult.RecollectedResourcesCount != 1 {
		t.Errorf("expected 1 recollected resource, got %d", recResult.RecollectedResourcesCount)
	}

	// 4. Verify Active State Post-Recovery
	activeResources, err := store.ListResources(ctx, wsID)
	if err != nil {
		t.Fatalf("failed to list active resources: %v", err)
	}

	// Should have 3 active resources:
	// 1. canonicalPod (preserved)
	// 2. awsVPC (AREA O: Non-K8s provider preserved)
	// 3. recollectedPod (AREA P: authoritative batch)
	if len(activeResources) != 3 {
		t.Fatalf("expected 3 active resources, got %d: %+v", len(activeResources), activeResources)
	}

	// Verify AWS resource is intact
	hasAWS := false
	for _, r := range activeResources {
		if r.Identity.Provider == "aws" && r.Identity.ProviderID == "vpc-0123456789abcdef0" {
			hasAWS = true
		}
	}
	if !hasAWS {
		t.Errorf("AREA O VIOLATION: AWS VPC was not preserved!")
	}

	// Verify Non-K8s relationships preserved
	activeRels, err := store.GetOutgoingRelationships(ctx, wsID, awsVPC.Identity)
	if err != nil {
		t.Fatalf("failed to get AWS relationships: %v", err)
	}
	if len(activeRels) != 1 || activeRels[0].Target.ProviderID != "subnet-0123456789" {
		t.Errorf("AREA O VIOLATION: AWS relationship was not preserved!")
	}

	// Verify AREA N: Historical evidence was NOT deleted
	evLegacy, err := store.GetEvidence(ctx, wsID, "ev-legacy-1")
	if err != nil || evLegacy == nil {
		t.Fatalf("AREA N VIOLATION: Historical evidence ev-legacy-1 was deleted or missing: %v", err)
	}
	evCanonical, err := store.GetEvidence(ctx, wsID, "ev-canonical-1")
	if err != nil || evCanonical == nil {
		t.Fatalf("AREA N VIOLATION: Historical evidence ev-canonical-1 was deleted or missing: %v", err)
	}

	// 5. Verify Clean Post-Recovery State
	check2, err := store.InspectWorkspaceIntegrity(ctx, wsID)
	if err != nil {
		t.Fatalf("post-recovery integrity check failed: %v", err)
	}
	if !check2.IsClean {
		t.Fatalf("expected workspace to be clean post-recovery, got issues: %v", check2.IntegrityErrors)
	}

	// 6. AREA R: Recovery Idempotency
	recResult2, err := store.RecoverWorkspaceLegacyState(ctx, state.LegacyRecoveryParams{
		WorkspaceID:          wsID,
		AuthorizeDestructive: true,
		TargetClusterIDs:     []string{"prod-cluster"},
		Reason:               "Second run should be no-op",
	})
	if err != nil {
		t.Fatalf("idempotent second recovery failed: %v", err)
	}
	if recResult2.ArchivedResourcesCount != 0 {
		t.Errorf("expected 0 archived resources on second run, got %d", recResult2.ArchivedResourcesCount)
	}
	if recResult2.ArchivedRelationshipsCount != 0 {
		t.Errorf("expected 0 archived relationships on second run, got %d", recResult2.ArchivedRelationshipsCount)
	}
}
