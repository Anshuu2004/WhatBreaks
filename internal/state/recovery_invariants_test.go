package state_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aaradhychinche-alt/WhatBreaks/internal/auth"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/state"
)

// TestRecoveryInvariants_InterruptedRecoveryFailsClosed verifies that if recovery is interrupted
// or left in an incomplete state, the durable recovery state machine prevents the workspace from
// serving queries or running normal reconciliation until cleanly recovered.
func TestRecoveryInvariants_InterruptedRecoveryFailsClosed(t *testing.T) {
	memStore := state.NewMemoryStore()
	wsID := "ws-interrupted-recovery-test"
	now := time.Now().UTC()

	// Seed workspace with legacy resource
	err := memStore.SaveResources(context.Background(), wsID, []state.Resource{
		{
			WorkspaceID: wsID,
			Identity: state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "pod",
				ProviderID:   "default/old-pod",
			},
			FirstObservedAt: now,
			LastObservedAt:  now,
		},
	})
	if err != nil {
		t.Fatalf("failed to seed resources: %v", err)
	}

	adminCtx := auth.WithUser(context.Background(), &auth.User{ID: "admin-1", Role: auth.RoleAdmin})

	// Attempt recovery with an invalid authoritative recollection batch that fails post-recovery integrity
	_, recErr := memStore.RecoverWorkspaceLegacyState(adminCtx, state.LegacyRecoveryParams{
		WorkspaceID:          wsID,
		ClusterID:            "prod-cluster",
		AuthorizeDestructive: true,
		Reason:               "Test failure halfway",
		AuthoritativeBatch: &state.AuthoritativeBatch{
			Resources: []state.Resource{
				{
					WorkspaceID: wsID,
					Identity: state.ResourceIdentity{
						Provider:     "kubernetes",
						ResourceType: "pod",
						ProviderID:   "still-legacy/pod", // Invalid!
					},
					FirstObservedAt: now,
					LastObservedAt:  now,
				},
			},
		},
	})
	if recErr == nil {
		t.Fatalf("expected recovery to fail on invalid post-recovery batch")
	}

	// 1. Verify workspace fails closed on integrity check
	check, err := memStore.InspectWorkspaceIntegrity(context.Background(), wsID)
	if err != nil {
		t.Fatalf("integrity check returned unexpected system error: %v", err)
	}
	if check.IsClean {
		t.Errorf("expected workspace to be marked UNCLEAN after failed recovery")
	}

	// 2. Verify CheckWorkspaceIntegrity returns error
	if chkErr := memStore.CheckWorkspaceIntegrity(context.Background(), wsID); chkErr == nil {
		t.Errorf("expected CheckWorkspaceIntegrity to fail closed on failed recovery")
	}

	// 3. Verify normal reconciliation refuses to run on failed recovery workspace
	_, bErr := memStore.ReconcileAuthoritativeBatch(context.Background(), state.AuthoritativeBatchParams{
		WorkspaceID: wsID,
		ClusterID:   "prod-cluster",
		Resources: []state.Resource{
			{
				WorkspaceID: wsID,
				Identity: state.ResourceIdentity{
					Provider:     "kubernetes",
					ResourceType: "pod",
					ProviderID:   "prod-cluster/default/pod-1",
				},
				FirstObservedAt: now,
				LastObservedAt:  now,
			},
		},
	})
	if !errors.Is(bErr, state.ErrWorkspaceRecoveryRequired) {
		t.Errorf("expected ErrWorkspaceRecoveryRequired when reconciling failed recovery workspace, got: %v", bErr)
	}

	// 4. Verify durable audit survived the failed attempt
	audits, err := memStore.ListRecoveryAudits(context.Background(), wsID)
	if err != nil {
		t.Fatalf("failed to list audits: %v", err)
	}
	if len(audits) == 0 {
		t.Fatalf("expected audit entry for failed recovery")
	}
	lastAudit := audits[len(audits)-1]
	if lastAudit.Status != "FAILED" {
		t.Errorf("expected audit status FAILED, got %s", lastAudit.Status)
	}
	if lastAudit.FailureCategory != "RECOVERY_EXECUTION" {
		t.Errorf("expected failure category RECOVERY_EXECUTION, got %s", lastAudit.FailureCategory)
	}

	// 5. Clean recovery with valid authoritative batch restores healthy state
	cleanRec, err := memStore.RecoverWorkspaceLegacyState(adminCtx, state.LegacyRecoveryParams{
		WorkspaceID:          wsID,
		ClusterID:            "prod-cluster",
		AuthorizeDestructive: true,
		Reason:               "Clean resolution",
		AuthoritativeBatch: &state.AuthoritativeBatch{
			Resources: []state.Resource{
				{
					WorkspaceID: wsID,
					Identity: state.ResourceIdentity{
						Provider:     "kubernetes",
						ResourceType: "pod",
						ProviderID:   "prod-cluster/default/clean-pod",
					},
					FirstObservedAt: now,
					LastObservedAt:  now,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("clean recovery failed: %v", err)
	}
	if cleanRec.RecollectedResourcesCount != 1 {
		t.Errorf("expected 1 recollected resource, got %d", cleanRec.RecollectedResourcesCount)
	}

	// Now workspace is clean
	if chkErr := memStore.CheckWorkspaceIntegrity(context.Background(), wsID); chkErr != nil {
		t.Errorf("expected workspace to be clean after successful recovery, got: %v", chkErr)
	}
}

// TestRecoveryInvariants_AuthorizationEnforcement verifies Phase 6 requirements:
// - Server-side authorization check rejects unauthenticated callers.
// - Spoofed OperatorIdentity in request body without context principal is rejected.
// - Non-admin users are rejected.
func TestRecoveryInvariants_AuthorizationEnforcement(t *testing.T) {
	memStore := state.NewMemoryStore()
	wsID := "ws-auth-enforcement"

	// Case 1: Unauthenticated context with AuthorizeDestructive: true and spoofed OperatorIdentity
	_, err := memStore.RecoverWorkspaceLegacyState(context.Background(), state.LegacyRecoveryParams{
		WorkspaceID:          wsID,
		ClusterID:            "prod-cluster",
		AuthorizeDestructive: true,
		OperatorIdentity:     "root-admin-impersonator", // Spoofed string
		Reason:               "Unauthorized recovery attempt",
	})
	if !errors.Is(err, state.ErrUnauthenticatedOperator) {
		t.Errorf("expected ErrUnauthenticatedOperator for unauthenticated context, got %v", err)
	}

	// Case 2: Authenticated non-admin role (RoleMember)
	viewerCtx := auth.WithUser(context.Background(), &auth.User{ID: "member-1", Role: auth.RoleMember})
	_, err = memStore.RecoverWorkspaceLegacyState(viewerCtx, state.LegacyRecoveryParams{
		WorkspaceID:          wsID,
		ClusterID:            "prod-cluster",
		AuthorizeDestructive: true,
		Reason:               "Viewer attempting destructive action",
	})
	if !errors.Is(err, state.ErrForbiddenOperator) {
		t.Errorf("expected ErrForbiddenOperator for viewer, got %v", err)
	}

	// Case 3: Verify audit captured both failed authorization attempts
	audits, err := memStore.ListRecoveryAudits(context.Background(), wsID)
	if err != nil {
		t.Fatalf("failed to list recovery audits: %v", err)
	}
	if len(audits) != 2 {
		t.Fatalf("expected 2 audit records for unauthorized attempts, got %d", len(audits))
	}
	for i, a := range audits {
		if a.Status != "FAILED" {
			t.Errorf("audit[%d]: expected status FAILED, got %s", i, a.Status)
		}
		if a.FailureCategory != "AUTHENTICATION_AUTHORIZATION" {
			t.Errorf("audit[%d]: expected failure category AUTHENTICATION_AUTHORIZATION, got %s", i, a.FailureCategory)
		}
	}
}

// TestRecoveryInvariants_ConcurrentRecoverySerialization verifies Phase 2:
// Concurrent recovery calls are serialized cleanly without data races or corruption.
func TestRecoveryInvariants_ConcurrentRecoverySerialization(t *testing.T) {
	memStore := state.NewMemoryStore()
	wsID := "ws-concurrent-recovery"
	now := time.Now().UTC()

	adminCtx := auth.WithUser(context.Background(), &auth.User{ID: "admin-1", Role: auth.RoleAdmin})

	const goroutines = 10
	var wg sync.WaitGroup
	errs := make([]error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = memStore.RecoverWorkspaceLegacyState(adminCtx, state.LegacyRecoveryParams{
				WorkspaceID:          wsID,
				ClusterID:            "prod-cluster",
				AuthorizeDestructive: true,
				Reason:               fmt.Sprintf("Concurrent recovery run %d", idx),
				AuthoritativeBatch: &state.AuthoritativeBatch{
					Resources: []state.Resource{
						{
							WorkspaceID: wsID,
							Identity: state.ResourceIdentity{
								Provider:     "kubernetes",
								ResourceType: "pod",
								ProviderID:   fmt.Sprintf("prod-cluster/default/pod-%d", idx),
							},
							FirstObservedAt: now,
							LastObservedAt:  now,
						},
					},
				},
			})
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d failed recovery: %v", i, err)
		}
	}

	// State must be clean post-recovery
	if err := memStore.CheckWorkspaceIntegrity(context.Background(), wsID); err != nil {
		t.Errorf("expected clean workspace post-recovery, got: %v", err)
	}
}

// TestRecoveryInvariants_CRDScopeClassification verifies Phase 5:
// - Built-in resources
// - Namespaced CRDs
// - Cluster-scoped CRDs
// - Unknown scopes (refuses to guess on slash count)
func TestRecoveryInvariants_CRDScopeClassification(t *testing.T) {
	state.ResetCustomResourceScopes()
	defer state.ResetCustomResourceScopes()

	// 1. Built-in namespaced resource (pod)
	// Canonical: 3 segments <cluster>/<namespace>/<name>
	cls, err := state.ClassifyKubernetesProviderID("pod", "c1/ns1/p1")
	if err != nil || !cls.IsCanonical || cls.Cluster != "c1" || cls.Namespace != "ns1" || cls.Name != "p1" {
		t.Errorf("pod canonical classification failed: cls=%+v, err=%v", cls, err)
	}
	// Legacy: 2 segments <namespace>/<name>
	cls, err = state.ClassifyKubernetesProviderID("pod", "ns1/p1")
	if err != nil || !cls.IsLegacy || cls.Cluster != "" || cls.Namespace != "ns1" || cls.Name != "p1" {
		t.Errorf("pod legacy classification failed: cls=%+v, err=%v", cls, err)
	}

	// 2. Built-in cluster-scoped resource (node)
	// Canonical: 2 segments <cluster>/<name>
	cls, err = state.ClassifyKubernetesProviderID("node", "c1/worker-1")
	if err != nil || !cls.IsCanonical || cls.Cluster != "c1" || cls.Namespace != "" || cls.Name != "worker-1" {
		t.Errorf("node canonical classification failed: cls=%+v, err=%v", cls, err)
	}
	// Legacy: 1 segment <name>
	cls, err = state.ClassifyKubernetesProviderID("node", "worker-1")
	if err != nil || !cls.IsLegacy || cls.Cluster != "" || cls.Name != "worker-1" {
		t.Errorf("node legacy classification failed: cls=%+v, err=%v", cls, err)
	}

	// 3. Register a Namespaced Custom Resource Definition (CRD)
	state.RegisterResourceScope("vaultauth.hashicorp.com", state.ScopeNamespaced)
	// Canonical CRD
	cls, err = state.ClassifyKubernetesProviderID("vaultauth.hashicorp.com", "prod-cluster/sec-ns/vault-jwt")
	if err != nil || !cls.IsCanonical || cls.Cluster != "prod-cluster" || cls.Namespace != "sec-ns" || cls.Name != "vault-jwt" {
		t.Errorf("custom namespaced CRD canonical classification failed: cls=%+v, err=%v", cls, err)
	}
	// Legacy CRD
	cls, err = state.ClassifyKubernetesProviderID("vaultauth.hashicorp.com", "sec-ns/vault-jwt")
	if err != nil || !cls.IsLegacy || cls.Cluster != "" || cls.Namespace != "sec-ns" || cls.Name != "vault-jwt" {
		t.Errorf("custom namespaced CRD legacy classification failed: cls=%+v, err=%v", cls, err)
	}

	// 4. Register a Cluster-scoped Custom Resource Definition (CRD)
	state.RegisterResourceScope("clusterissuer.cert-manager.io", state.ScopeCluster)
	// Canonical Cluster CRD
	cls, err = state.ClassifyKubernetesProviderID("clusterissuer.cert-manager.io", "prod-cluster/letsencrypt-prod")
	if err != nil || !cls.IsCanonical || cls.Cluster != "prod-cluster" || cls.Namespace != "" || cls.Name != "letsencrypt-prod" {
		t.Errorf("custom cluster CRD canonical classification failed: cls=%+v, err=%v", cls, err)
	}
	// Legacy Cluster CRD
	cls, err = state.ClassifyKubernetesProviderID("clusterissuer.cert-manager.io", "letsencrypt-prod")
	if err != nil || !cls.IsLegacy || cls.Cluster != "" || cls.Name != "letsencrypt-prod" {
		t.Errorf("custom cluster CRD legacy classification failed: cls=%+v, err=%v", cls, err)
	}

	// 5. Unknown Scope: Cannot guess whether 2-segments is cluster-scoped or namespaced!
	cls, err = state.ClassifyKubernetesProviderID("unknowncrd.example.com", "segment-1/segment-2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cls.IsAmbiguous {
		t.Errorf("expected unknown 2-segment CRD to be classified as IsAmbiguous=true, got: %+v", cls)
	}

	// 6. Unknown Scope 3-segment is canonical namespaced
	cls, err = state.ClassifyKubernetesProviderID("unknowncrd.example.com", "cluster/namespace/name")
	if err != nil || !cls.IsCanonical {
		t.Errorf("unknown 3-segment should be canonical namespaced, got: %+v, err=%v", cls, err)
	}
}
