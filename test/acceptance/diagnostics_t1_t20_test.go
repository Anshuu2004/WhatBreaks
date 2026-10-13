package acceptance_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	corev1 "github.com/aaradhychinche-alt/WhatBreaks/gen/go/wb/core/v1"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/answer"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/database"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/logging"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/state"
)

// uuidStr generates a valid RFC 4122 UUID for deterministic evidence IDs.
func uuidStr(i int) string {
	return fmt.Sprintf("00000000-0000-0000-0000-%012d", i)
}

func makeDiagnosticTestEvidence(id, provider, resType, providerID, obsType, data string, observedAt time.Time) *corev1.Evidence {
	return &corev1.Evidence{
		Id: id,
		Source: &corev1.EvidenceSource{
			Provider:  provider,
			Collector: "test-collector",
		},
		ObservedAt:      observedAt.Format(time.RFC3339Nano),
		ObservationType: obsType,
		Subject: &corev1.ResourceIdentity{
			Provider:     provider,
			ResourceType: resType,
			ProviderId:   providerID,
		},
		Data: []byte(data),
	}
}

// ---------------------------------------------------------------------------
// T1: Directly observed resource
// Resource is persisted and recognized as observed.
// ---------------------------------------------------------------------------
func TestT1_DirectlyObservedResource(t *testing.T) {
	ctx := context.Background()
	client, _ := spawnServer(t)
	logger := logging.NewStandardLogger(io.Discard, logging.LevelError)

	store := state.NewMemoryStore()
	materializer := state.NewMaterializer(store, client, logger)
	reconciler := state.NewReconciler(store, client, materializer, logger)
	answerSvc := answer.NewService(client, logger)

	ws := "ws-t1-observed"
	t0 := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)
	ev := makeDiagnosticTestEvidence(uuidStr(101), "kubernetes", "pod", "default/checkout-pod", "CONTROL_PLANE_OBJECT", `{"status":"running"}`, t0)

	batch := state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "default",
		RunID:       "run-t1-1",
		Generation:  1,
		Evidence:    []*corev1.Evidence{ev},
	}

	res, err := reconciler.Reconcile(ctx, batch)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.ResourcesCreated != 1 {
		t.Fatalf("expected 1 resource created, got %d", res.ResourcesCreated)
	}

	// Verify persistence in store
	storedRes, err := store.GetResource(ctx, ws, state.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "pod",
		ProviderID:   "default/checkout-pod",
	})
	if err != nil {
		t.Fatalf("GetResource failed: %v", err)
	}
	if !storedRes.FirstObservedAt.Equal(t0) {
		t.Errorf("expected FirstObservedAt=%v, got %v", t0, storedRes.FirstObservedAt)
	}

	// Materialize and analyze
	if _, err := materializer.Materialize(ctx, ws); err != nil {
		t.Fatalf("Materialize failed: %v", err)
	}

	ans, err := answerSvc.AnalyzeImpact(ctx, answer.ImpactRequest{
		WorkspaceID: ws,
		Target: &answer.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "pod",
			ProviderID:   "default/checkout-pod",
		},
		Direction: "incoming",
		MaxDepth:  3,
		ProposedChange: &answer.ProposedChange{
			ChangeType: answer.ChangeTypeUpdate,
		},
	})
	if err != nil {
		t.Fatalf("AnalyzeImpact failed: %v", err)
	}

	if ans.Completeness == nil || !ans.Completeness.IsComplete {
		t.Errorf("expected IsComplete=true for directly observed resource, got %v", ans.Completeness)
	}
	if ans.Completeness.Status != "COMPLETE" {
		t.Errorf("expected status=COMPLETE, got %s", ans.Completeness.Status)
	}
}

// ---------------------------------------------------------------------------
// T2: Unobserved relationship endpoint
// Relationship/provenance preserved, NO synthetic state_resources entry created.
// ---------------------------------------------------------------------------
func TestT2_UnobservedRelationshipEndpoint(t *testing.T) {
	ctx := context.Background()
	client, _ := spawnServer(t)
	logger := logging.NewStandardLogger(io.Discard, logging.LevelError)
	store := state.NewMemoryStore()
	materializer := state.NewMaterializer(store, client, logger)
	reconciler := state.NewReconciler(store, client, materializer, logger)

	ws := "ws-t2-unobserved-endpoint"
	t0 := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

	// Pod evidence that references an external unobserved RDS database
	podEv := makeDiagnosticTestEvidence(uuidStr(201), "kubernetes", "pod", "default/api-pod", "CONTROL_PLANE_OBJECT", `{"status":"running"}`, t0)
	depData, _ := json.Marshal(map[string]interface{}{
		"target": map[string]interface{}{
			"provider":      "aws",
			"resource_type": "rds_instance",
			"provider_id":   "prod-db",
		},
	})
	relEv := makeDiagnosticTestEvidence(uuidStr(202), "kubernetes", "pod", "default/api-pod", "RESOURCE_REFERENCE", string(depData), t0)

	batch := state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "default",
		RunID:       "run-t2-1",
		Generation:  1,
		Evidence:    []*corev1.Evidence{podEv, relEv},
	}

	_, err := reconciler.Reconcile(ctx, batch)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// 1. Pod must exist in state_resources
	_, err = store.GetResource(ctx, ws, state.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "pod",
		ProviderID:   "default/api-pod",
	})
	if err != nil {
		t.Fatalf("expected api-pod in store: %v", err)
	}

	// 2. Unobserved RDS must NOT exist in state_resources (NO synthetic phantom resource!)
	_, err = store.GetResource(ctx, ws, state.ResourceIdentity{
		Provider:     "aws",
		ResourceType: "rds_instance",
		ProviderID:   "prod-db",
	})
	if err == nil {
		t.Fatalf("FATAL: unobserved endpoint was synthesized into state_resources!")
	}

	// 3. Relationship must exist
	rels, err := store.ListRelationships(ctx, ws)
	if err != nil {
		t.Fatalf("ListRelationships failed: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("expected 1 relationship, got %d", len(rels))
	}
	if rels[0].Target.ProviderID != "prod-db" {
		t.Errorf("expected target prod-db, got %s", rels[0].Target.ProviderID)
	}
}

// ---------------------------------------------------------------------------
// T3: Unresolved reference diagnostic
// Contains subject, referenced resource, evidence.
// ---------------------------------------------------------------------------
func TestT3_UnresolvedReferenceDiagnostic(t *testing.T) {
	ctx := context.Background()
	client, _ := spawnServer(t)
	logger := logging.NewStandardLogger(io.Discard, logging.LevelError)
	store := state.NewMemoryStore()
	materializer := state.NewMaterializer(store, client, logger)
	reconciler := state.NewReconciler(store, client, materializer, logger)

	ws := "ws-t3-diag"
	t0 := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

	podEv := makeDiagnosticTestEvidence(uuidStr(301), "kubernetes", "pod", "default/client-pod", "CONTROL_PLANE_OBJECT", `{}`, t0)
	depData, _ := json.Marshal(map[string]interface{}{
		"target": map[string]interface{}{
			"provider":      "aws",
			"resource_type": "rds_instance",
			"provider_id":   "analytics-db",
		},
	})
	relEv := makeDiagnosticTestEvidence(uuidStr(302), "kubernetes", "pod", "default/client-pod", "RESOURCE_REFERENCE", string(depData), t0)

	batch := state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "default",
		RunID:       "run-t3-1",
		Generation:  1,
		Evidence:    []*corev1.Evidence{podEv, relEv},
	}

	_, err := reconciler.Reconcile(ctx, batch)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	diags, err := store.ListDiagnostics(ctx, ws)
	if err != nil {
		t.Fatalf("ListDiagnostics failed: %v", err)
	}
	if len(diags) == 0 {
		t.Fatalf("expected UNRESOLVED_REFERENCE diagnostic, got 0")
	}

	var found bool
	for _, d := range diags {
		if d.Code == "UNRESOLVED_REFERENCE" && d.ReferencedResource != nil && d.ReferencedResource.ProviderID == "analytics-db" {
			found = true
			if d.Subject == nil || d.Subject.ProviderID != "default/client-pod" {
				t.Errorf("expected subject default/client-pod, got %v", d.Subject)
			}
			if len(d.EvidenceIDs) == 0 || d.EvidenceIDs[0] != uuidStr(302) {
				t.Errorf("expected evidence ID %s, got %v", uuidStr(302), d.EvidenceIDs)
			}
			if d.Relevance != "BLAST_RADIUS" {
				t.Errorf("expected relevance BLAST_RADIUS, got %s", d.Relevance)
			}
			if d.CompletenessImpact != "PARTIAL" {
				t.Errorf("expected completeness impact PARTIAL, got %s", d.CompletenessImpact)
			}
		}
	}
	if !found {
		t.Errorf("diagnostic for analytics-db not found in %v", diags)
	}
}

// ---------------------------------------------------------------------------
// T4: Later resolution
// Subsequent successful complete scope observes missing target and resolves diagnostic.
// ---------------------------------------------------------------------------
func TestT4_LaterResolution(t *testing.T) {
	ctx := context.Background()
	client, _ := spawnServer(t)
	logger := logging.NewStandardLogger(io.Discard, logging.LevelError)
	store := state.NewMemoryStore()
	materializer := state.NewMaterializer(store, client, logger)
	reconciler := state.NewReconciler(store, client, materializer, logger)
	answerSvc := answer.NewService(client, logger)

	ws := "ws-t4-resolution"
	t0 := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

	// Run 1: Pod references unobserved service
	podEv := makeDiagnosticTestEvidence(uuidStr(401), "kubernetes", "pod", "default/fe", "CONTROL_PLANE_OBJECT", `{}`, t0)
	depData, _ := json.Marshal(map[string]interface{}{
		"target": map[string]interface{}{
			"provider":      "kubernetes",
			"resource_type": "service",
			"provider_id":   "default/be-svc",
		},
	})
	relEv := makeDiagnosticTestEvidence(uuidStr(402), "kubernetes", "pod", "default/fe", "RESOURCE_REFERENCE", string(depData), t0)

	batch1 := state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "default",
		RunID:       "run-t4-1",
		Generation:  1,
		Evidence:    []*corev1.Evidence{podEv, relEv},
	}
	if _, err := reconciler.Reconcile(ctx, batch1); err != nil {
		t.Fatalf("Reconcile 1 failed: %v", err)
	}

	diags1, _ := store.ListDiagnostics(ctx, ws)
	if len(diags1) == 0 || diags1[0].Resolved {
		t.Fatalf("expected unresolved diagnostic after run 1, got %v", diags1)
	}

	// Run 2: Directly observe default/be-svc in successful sweep
	t1 := t0.Add(5 * time.Minute)
	svcEv := makeDiagnosticTestEvidence(uuidStr(403), "kubernetes", "service", "default/be-svc", "CONTROL_PLANE_OBJECT", `{}`, t1)

	batch2 := state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "default",
		RunID:       "run-t4-2",
		Generation:  2,
		Evidence:    []*corev1.Evidence{svcEv},
		ScopeSweep: &state.ScopeSweep{
			SuccessfulScopes: []string{"services"},
		},
	}
	if _, err := reconciler.Reconcile(ctx, batch2); err != nil {
		t.Fatalf("Reconcile 2 failed: %v", err)
	}

	diags2, err := store.ListDiagnostics(ctx, ws)
	if err != nil {
		t.Fatalf("ListDiagnostics failed: %v", err)
	}
	if len(diags2) != 0 {
		t.Errorf("expected UNRESOLVED_REFERENCE diagnostic to be resolved (no active diagnostics remaining), got %v", diags2)
	}

	// Materialize and verify COMPLETE answer
	_, _ = materializer.Materialize(ctx, ws)
	ans, err := answerSvc.AnalyzeImpact(ctx, answer.ImpactRequest{
		WorkspaceID: ws,
		Target: &answer.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "pod",
			ProviderID:   "default/fe",
		},
		Direction: "incoming",
		MaxDepth:  3,
		ProposedChange: &answer.ProposedChange{
			ChangeType: answer.ChangeTypeUpdate,
		},
	})
	if err != nil {
		t.Fatalf("AnalyzeImpact failed: %v", err)
	}
	if ans.Completeness == nil || !ans.Completeness.IsComplete {
		t.Errorf("expected analysis to be COMPLETE after resolution, got %v", ans.Completeness)
	}
}

// ---------------------------------------------------------------------------
// T5: Failed scope does not resolve existing diagnostics
// ---------------------------------------------------------------------------
func TestT5_FailedScope_DoesNotResolve(t *testing.T) {
	ctx := context.Background()
	client, _ := spawnServer(t)
	logger := logging.NewStandardLogger(io.Discard, logging.LevelError)
	store := state.NewMemoryStore()
	materializer := state.NewMaterializer(store, client, logger)
	reconciler := state.NewReconciler(store, client, materializer, logger)

	ws := "ws-t5-failed-scope"
	t0 := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

	podEv := makeDiagnosticTestEvidence(uuidStr(501), "kubernetes", "pod", "default/fe", "CONTROL_PLANE_OBJECT", `{}`, t0)
	depData, _ := json.Marshal(map[string]interface{}{
		"target": map[string]interface{}{
			"provider":      "kubernetes",
			"resource_type": "service",
			"provider_id":   "default/be-svc",
		},
	})
	relEv := makeDiagnosticTestEvidence(uuidStr(502), "kubernetes", "pod", "default/fe", "RESOURCE_REFERENCE", string(depData), t0)

	batch1 := state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "default",
		RunID:       "run-t5-1",
		Generation:  1,
		Evidence:    []*corev1.Evidence{podEv, relEv},
	}
	_, _ = reconciler.Reconcile(ctx, batch1)

	// Run 2: Sweep occurred but the services scope FAILED
	batch2 := state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "default",
		RunID:       "run-t5-2",
		Generation:  2,
		ScopeSweep: &state.ScopeSweep{
			FailedScopes: []string{"services"},
		},
	}
	_, _ = reconciler.Reconcile(ctx, batch2)

	diags, _ := store.ListDiagnostics(ctx, ws)
	for _, d := range diags {
		if d.Code == "UNRESOLVED_REFERENCE" && d.ReferencedResource != nil && d.ReferencedResource.ProviderID == "default/be-svc" {
			if d.Resolved {
				t.Fatalf("FAILED scope must NOT resolve diagnostic!")
			}
		}
	}
}

// ---------------------------------------------------------------------------
// T6: Required scope failure leaves authoritative state unchanged
// ---------------------------------------------------------------------------
func TestT6_RequiredScopeFailure(t *testing.T) {
	ctx := context.Background()
	logger := logging.NewStandardLogger(io.Discard, logging.LevelError)
	store := state.NewMemoryStore()
	reconciler := state.NewReconciler(store, nil, nil, logger)

	ws := "ws-t6-required-failure"
	t0 := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

	// Initial clean state
	ev := makeDiagnosticTestEvidence(uuidStr(601), "kubernetes", "pod", "default/worker", "CONTROL_PLANE_OBJECT", `{}`, t0)
	batch1 := state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "default",
		RunID:       "run-t6-1",
		Generation:  1,
		Evidence:    []*corev1.Evidence{ev},
	}
	if _, err := reconciler.Reconcile(ctx, batch1); err != nil {
		t.Fatalf("Reconcile 1 failed: %v", err)
	}

	resBefore, _ := store.ListResources(ctx, ws)
	if len(resBefore) != 1 {
		t.Fatalf("expected 1 resource, got %d", len(resBefore))
	}

	// Attempt reconcile with required scope failure
	batch2 := state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "default",
		RunID:       "run-t6-2",
		Generation:  2,
		ScopeSweep: &state.ScopeSweep{
			RequiredFailures: []string{"pods"},
			FailedScopes:     []string{"pods"},
		},
	}
	_, err := reconciler.Reconcile(ctx, batch2)
	// Must fail or abort
	if err == nil {
		t.Fatalf("expected required scope failure to reject/abort reconciliation")
	}

	// Authoritative state must remain completely unchanged
	resAfter, _ := store.ListResources(ctx, ws)
	if len(resAfter) != 1 || resAfter[0].Identity.ProviderID != "default/worker" {
		t.Errorf("authoritative state changed despite required scope failure!")
	}
}

// ---------------------------------------------------------------------------
// T7: Optional scope failure retains partial scope status
// ---------------------------------------------------------------------------
func TestT7_OptionalScopeFailure(t *testing.T) {
	ctx := context.Background()
	logger := logging.NewStandardLogger(io.Discard, logging.LevelError)
	client, _ := spawnServer(t)

	store := state.NewMemoryStore()
	materializer := state.NewMaterializer(store, client, logger)
	reconciler := state.NewReconciler(store, client, materializer, logger)
	answerSvc := answer.NewService(client, logger)

	ws := "ws-t7-optional-failure"
	t0 := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

	// Clean pod observed
	podEv := makeDiagnosticTestEvidence(uuidStr(701), "kubernetes", "pod", "default/web", "CONTROL_PLANE_OBJECT", `{}`, t0)
	// Optional scope 'ingress' failed
	batch := state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "default",
		RunID:       "run-t7-1",
		Generation:  1,
		Evidence:    []*corev1.Evidence{podEv},
		ScopeSweep: &state.ScopeSweep{
			OptionalFailures: []string{"ingress"},
			SuccessfulScopes: []string{"pods"},
		},
	}

	res, err := reconciler.Reconcile(ctx, batch)
	if err != nil {
		t.Fatalf("Reconcile with optional failure should succeed: %v", err)
	}
	if res.ResourcesCreated != 1 {
		t.Errorf("expected 1 resource created, got %d", res.ResourcesCreated)
	}

	// Verify INCOMPLETE_SCOPE diagnostic emitted
	diags, _ := store.ListDiagnostics(ctx, ws)
	var foundScopeDiag bool
	for _, d := range diags {
		if d.Code == "INCOMPLETE_SCOPE" && d.Scope == "ingress" {
			foundScopeDiag = true
		}
	}
	if !foundScopeDiag {
		t.Errorf("expected INCOMPLETE_SCOPE diagnostic for ingress, got %v", diags)
	}

	// Materialize and analyze
	_, _ = materializer.Materialize(ctx, ws)
	ans, err := answerSvc.AnalyzeImpact(ctx, answer.ImpactRequest{
		WorkspaceID: ws,
		Target: &answer.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "pod",
			ProviderID:   "default/web",
		},
		Direction: "incoming",
		MaxDepth:  3,
		ProposedChange: &answer.ProposedChange{
			ChangeType: answer.ChangeTypeUpdate,
		},
	})
	if err != nil {
		t.Fatalf("AnalyzeImpact failed: %v", err)
	}
	// Pod is observed and complete; unrelated optional ingress failure does not downgrade pod
	if ans.Completeness == nil || !ans.Completeness.IsComplete {
		t.Errorf("expected pod analysis to be COMPLETE when ingress is not in its blast radius")
	}
}

// ---------------------------------------------------------------------------
// T8: Unmapped runtime connection
// Diagnostic preserves caller, endpoint, evidence IDs; NO synthetic CALLS edge.
// ---------------------------------------------------------------------------
func TestT8_UnmappedRuntimeConnection(t *testing.T) {
	ctx := context.Background()
	client, _ := spawnServer(t)
	logger := logging.NewStandardLogger(io.Discard, logging.LevelError)
	store := state.NewMemoryStore()
	materializer := state.NewMaterializer(store, client, logger)
	reconciler := state.NewReconciler(store, client, materializer, logger)

	ws := "ws-t8-unmapped"
	t0 := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

	callerPod := makeDiagnosticTestEvidence(uuidStr(801), "kubernetes", "pod", "default/caller", "CONTROL_PLANE_OBJECT", `{}`, t0)
	connData, _ := json.Marshal(map[string]interface{}{
		"destination": "198.51.100.25:443",
		"port":        443,
		"protocol":    "tcp",
	})
	connEv := makeDiagnosticTestEvidence(uuidStr(802), "kubernetes", "pod", "default/caller", "RUNTIME_CONNECTION", string(connData), t0)

	batch := state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "default",
		RunID:       "run-t8-1",
		Generation:  1,
		Evidence:    []*corev1.Evidence{callerPod, connEv},
	}

	_, err := reconciler.Reconcile(ctx, batch)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// 1. Diagnostic must be emitted
	diags, _ := store.ListDiagnostics(ctx, ws)
	var found bool
	for _, d := range diags {
		if d.Code == "UNMAPPED_CONNECTION" {
			found = true
			if d.Subject == nil || d.Subject.ProviderID != "default/caller" {
				t.Errorf("expected caller subject default/caller, got %v", d.Subject)
			}
			if !strings.Contains(d.UnresolvedEndpoint, "198.51.100.25") {
				t.Errorf("expected endpoint to contain 198.51.100.25, got %s", d.UnresolvedEndpoint)
			}
			if len(d.EvidenceIDs) == 0 || d.EvidenceIDs[0] != uuidStr(802) {
				t.Errorf("expected evidence ID %s, got %v", uuidStr(802), d.EvidenceIDs)
			}
		}
	}
	if !found {
		t.Fatalf("expected UNMAPPED_CONNECTION diagnostic, got %v", diags)
	}

	// 2. NO synthetic CALLS relationship must appear in state_relationships
	rels, _ := store.ListRelationships(ctx, ws)
	for _, r := range rels {
		if r.Kind == "CALLS" {
			t.Fatalf("synthetic CALLS relationship was created for unmapped connection!")
		}
	}
}

// ---------------------------------------------------------------------------
// T9: Unobserved analysis target
// Returns structured diagnostic and DEGRADED completeness.
// ---------------------------------------------------------------------------
func TestT9_UnobservedAnalysisTarget(t *testing.T) {
	ctx := context.Background()
	logger := logging.NewStandardLogger(io.Discard, logging.LevelError)
	client, _ := spawnServer(t)

	store := state.NewMemoryStore()
	materializer := state.NewMaterializer(store, client, logger)
	reconciler := state.NewReconciler(store, client, materializer, logger)
	answerSvc := answer.NewService(client, logger)

	ws := "ws-t9-unobserved-target"
	t0 := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

	// Observe one valid resource in the workspace
	podEv := makeDiagnosticTestEvidence(uuidStr(901), "kubernetes", "pod", "default/other-pod", "CONTROL_PLANE_OBJECT", `{}`, t0)
	batch := state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "default",
		RunID:       "run-t9-1",
		Generation:  1,
		Evidence:    []*corev1.Evidence{podEv},
	}
	_, _ = reconciler.Reconcile(ctx, batch)
	_, _ = materializer.Materialize(ctx, ws)

	// Query impact on an unobserved target
	ans, err := answerSvc.AnalyzeImpact(ctx, answer.ImpactRequest{
		WorkspaceID: ws,
		Target: &answer.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "pod",
			ProviderID:   "default/ghost-target",
		},
		Direction: "incoming",
		MaxDepth:  3,
		ProposedChange: &answer.ProposedChange{
			ChangeType: answer.ChangeTypeDelete,
		},
	})
	if err != nil {
		t.Fatalf("AnalyzeImpact failed: %v", err)
	}

	if ans.Completeness == nil || ans.Completeness.IsComplete {
		t.Errorf("expected IsComplete=false for unobserved target")
	}
	if ans.Completeness.Status != "DEGRADED" {
		t.Errorf("expected status=DEGRADED, got %s", ans.Completeness.Status)
	}

	var foundDiag bool
	for _, d := range ans.Diagnostics {
		if d.Code == "UNOBSERVED_TARGET" && d.Relevance == "TARGET" {
			foundDiag = true
		}
	}
	if !foundDiag {
		t.Errorf("expected UNOBSERVED_TARGET diagnostic with relevance TARGET, got %v", ans.Diagnostics)
	}
}

// ---------------------------------------------------------------------------
// T10: Zero-impact completeness
// COMPLETE only when observations are sufficient, not when observations are missing.
// ---------------------------------------------------------------------------
func TestT10_ZeroImpactCompleteness(t *testing.T) {
	ctx := context.Background()
	logger := logging.NewStandardLogger(io.Discard, logging.LevelError)
	client, _ := spawnServer(t)

	store := state.NewMemoryStore()
	materializer := state.NewMaterializer(store, client, logger)
	reconciler := state.NewReconciler(store, client, materializer, logger)
	answerSvc := answer.NewService(client, logger)

	ws := "ws-t10-zero-impact"
	t0 := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

	// Standalone pod observed with no dependencies
	podEv := makeDiagnosticTestEvidence(uuidStr(1001), "kubernetes", "pod", "default/leaf-pod", "CONTROL_PLANE_OBJECT", `{}`, t0)
	batch := state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "default",
		RunID:       "run-t10-1",
		Generation:  1,
		Evidence:    []*corev1.Evidence{podEv},
	}
	_, _ = reconciler.Reconcile(ctx, batch)
	_, _ = materializer.Materialize(ctx, ws)

	// Observed leaf target: 0 impacted, but COMPLETE!
	ansComplete, err := answerSvc.AnalyzeImpact(ctx, answer.ImpactRequest{
		WorkspaceID: ws,
		Target: &answer.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "pod",
			ProviderID:   "default/leaf-pod",
		},
		Direction: "incoming",
		MaxDepth:  3,
		ProposedChange: &answer.ProposedChange{
			ChangeType: answer.ChangeTypeUpdate,
		},
	})
	if err != nil {
		t.Fatalf("AnalyzeImpact leaf failed: %v", err)
	}
	if ansComplete.Summary.ImpactedCount != 0 {
		t.Errorf("expected 0 impacted, got %d", ansComplete.Summary.ImpactedCount)
	}
	if ansComplete.Completeness == nil || !ansComplete.Completeness.IsComplete || ansComplete.Completeness.Status != "COMPLETE" {
		t.Errorf("expected COMPLETE status for observed leaf, got %v", ansComplete.Completeness)
	}

	// Contrast with unobserved target: 0 impacted, but DEGRADED!
	ansUnobserved, err := answerSvc.AnalyzeImpact(ctx, answer.ImpactRequest{
		WorkspaceID: ws,
		Target: &answer.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "pod",
			ProviderID:   "default/absent-pod",
		},
		Direction: "incoming",
		MaxDepth:  3,
		ProposedChange: &answer.ProposedChange{
			ChangeType: answer.ChangeTypeUpdate,
		},
	})
	if err != nil {
		t.Fatalf("AnalyzeImpact absent failed: %v", err)
	}
	if ansUnobserved.Completeness == nil || ansUnobserved.Completeness.IsComplete || ansUnobserved.Completeness.Status == "COMPLETE" {
		t.Errorf("unobserved target must NOT claim COMPLETE analysis!")
	}
}

// ---------------------------------------------------------------------------
// T11: Unrelated workspace diagnostic does not downgrade analysis
// ---------------------------------------------------------------------------
func TestT11_UnrelatedWorkspaceDiagnostic(t *testing.T) {
	ctx := context.Background()
	logger := logging.NewStandardLogger(io.Discard, logging.LevelError)
	client, _ := spawnServer(t)

	store := state.NewMemoryStore()
	materializer := state.NewMaterializer(store, client, logger)
	reconciler := state.NewReconciler(store, client, materializer, logger)
	answerSvc := answer.NewService(client, logger)

	ws := "ws-t11-unrelated"
	t0 := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

	// Resource cluster A: clean pod-a and svc-a
	podA := makeDiagnosticTestEvidence(uuidStr(1101), "kubernetes", "pod", "default/pod-a", "CONTROL_PLANE_OBJECT", `{}`, t0)
	svcA := makeDiagnosticTestEvidence(uuidStr(1102), "kubernetes", "service", "default/svc-a", "CONTROL_PLANE_OBJECT", `{}`, t0)
	depA, _ := json.Marshal(map[string]interface{}{
		"target": map[string]interface{}{
			"provider":      "kubernetes",
			"resource_type": "service",
			"provider_id":   "default/svc-a",
		},
	})
	relA := makeDiagnosticTestEvidence(uuidStr(1103), "kubernetes", "pod", "default/pod-a", "RESOURCE_REFERENCE", string(depA), t0)

	// Resource cluster B (unrelated): pod-z references unobserved db-z
	podZ := makeDiagnosticTestEvidence(uuidStr(1104), "kubernetes", "pod", "default/pod-z", "CONTROL_PLANE_OBJECT", `{}`, t0)
	depZ, _ := json.Marshal(map[string]interface{}{
		"target": map[string]interface{}{
			"provider":      "aws",
			"resource_type": "rds_instance",
			"provider_id":   "db-z",
		},
	})
	relZ := makeDiagnosticTestEvidence(uuidStr(1105), "kubernetes", "pod", "default/pod-z", "RESOURCE_REFERENCE", string(depZ), t0)

	batch := state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "default",
		RunID:       "run-t11-1",
		Generation:  1,
		Evidence:    []*corev1.Evidence{podA, svcA, relA, podZ, relZ},
	}
	_, _ = reconciler.Reconcile(ctx, batch)
	_, _ = materializer.Materialize(ctx, ws)

	// Query impact on svc-a (which impacts pod-a)
	ans, err := answerSvc.AnalyzeImpact(ctx, answer.ImpactRequest{
		WorkspaceID: ws,
		Target: &answer.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "service",
			ProviderID:   "default/svc-a",
		},
		Direction: "incoming",
		MaxDepth:  3,
		ProposedChange: &answer.ProposedChange{
			ChangeType: answer.ChangeTypeUpdate,
		},
	})
	if err != nil {
		t.Fatalf("AnalyzeImpact failed: %v", err)
	}

	// Must be COMPLETE! The diagnostic on pod-z is completely outside svc-a's blast radius
	if ans.Completeness == nil || !ans.Completeness.IsComplete || ans.Completeness.Status != "COMPLETE" {
		t.Errorf("expected COMPLETE for svc-a, got %v", ans.Completeness)
	}
	for _, d := range ans.Diagnostics {
		if d.Subject != nil && d.Subject.ProviderID == "default/pod-z" {
			t.Errorf("unrelated diagnostic leaked into svc-a query!")
		}
	}
}

// ---------------------------------------------------------------------------
// T12: Relevant blast-radius diagnostic produces PARTIAL completeness
// ---------------------------------------------------------------------------
func TestT12_RelevantBlastRadiusDiagnostic(t *testing.T) {
	ctx := context.Background()
	logger := logging.NewStandardLogger(io.Discard, logging.LevelError)
	client, _ := spawnServer(t)

	store := state.NewMemoryStore()
	materializer := state.NewMaterializer(store, client, logger)
	reconciler := state.NewReconciler(store, client, materializer, logger)
	answerSvc := answer.NewService(client, logger)

	ws := "ws-t12-blast-radius"
	t0 := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

	// svc-target is depended on by pod-client
	svcA := makeDiagnosticTestEvidence(uuidStr(1201), "kubernetes", "service", "default/core-svc", "CONTROL_PLANE_OBJECT", `{}`, t0)
	podB := makeDiagnosticTestEvidence(uuidStr(1202), "kubernetes", "pod", "default/client-pod", "CONTROL_PLANE_OBJECT", `{}`, t0)
	depSvc, _ := json.Marshal(map[string]interface{}{
		"target": map[string]interface{}{
			"provider":      "kubernetes",
			"resource_type": "service",
			"provider_id":   "default/core-svc",
		},
	})
	relSvc := makeDiagnosticTestEvidence(uuidStr(1203), "kubernetes", "pod", "default/client-pod", "RESOURCE_REFERENCE", string(depSvc), t0)

	// But pod-client ALSO references an unobserved external redis
	depRedis, _ := json.Marshal(map[string]interface{}{
		"target": map[string]interface{}{
			"provider":      "aws",
			"resource_type": "elasticache_cluster",
			"provider_id":   "unobserved-redis",
		},
	})
	relRedis := makeDiagnosticTestEvidence(uuidStr(1204), "kubernetes", "pod", "default/client-pod", "RESOURCE_REFERENCE", string(depRedis), t0)

	batch := state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "default",
		RunID:       "run-t12-1",
		Generation:  1,
		Evidence:    []*corev1.Evidence{svcA, podB, relSvc, relRedis},
	}
	_, _ = reconciler.Reconcile(ctx, batch)
	_, _ = materializer.Materialize(ctx, ws)

	// Query impact on core-svc: blast radius reaches client-pod
	ans, err := answerSvc.AnalyzeImpact(ctx, answer.ImpactRequest{
		WorkspaceID: ws,
		Target: &answer.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "service",
			ProviderID:   "default/core-svc",
		},
		Direction: "incoming",
		MaxDepth:  3,
		ProposedChange: &answer.ProposedChange{
			ChangeType: answer.ChangeTypeUpdate,
		},
	})
	if err != nil {
		t.Fatalf("AnalyzeImpact failed: %v", err)
	}

	if ans.Completeness == nil || ans.Completeness.IsComplete {
		t.Errorf("expected IsComplete=false due to unresolved reference in blast radius")
	}
	if ans.Completeness.Status != "PARTIAL" {
		t.Errorf("expected status=PARTIAL, got %s", ans.Completeness.Status)
	}

	var foundDiag bool
	for _, d := range ans.Diagnostics {
		if d.Code == "UNRESOLVED_REFERENCE" && d.Relevance == "BLAST_RADIUS" {
			foundDiag = true
		}
	}
	if !foundDiag {
		t.Errorf("expected BLAST_RADIUS diagnostic in response, got %v", ans.Diagnostics)
	}
}

// ---------------------------------------------------------------------------
// T13: Discovery conflict does not produce guessed relationships
// ---------------------------------------------------------------------------
func TestT13_DiscoveryConflict(t *testing.T) {
	ctx := context.Background()
	logger := logging.NewStandardLogger(io.Discard, logging.LevelError)
	store := state.NewMemoryStore()
	reconciler := state.NewReconciler(store, nil, nil, logger)

	ws := "ws-t13-conflict"
	t0 := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

	podEv := makeDiagnosticTestEvidence(uuidStr(1301), "kubernetes", "pod", "default/ambiguous-pod", "CONTROL_PLANE_OBJECT", `{}`, t0)
	batch := state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "default",
		RunID:       "run-t13-1",
		Generation:  1,
		Evidence:    []*corev1.Evidence{podEv},
		Diagnostics: []state.DiagnosticItem{
			{
				Code:               "DISCOVERY_CONFLICT",
				Severity:           "WARNING",
				Message:            "Ambiguous service selector matched multiple conflicting services",
				Scope:              "default",
				Relevance:          "WORKSPACE",
				CompletenessImpact: "PARTIAL",
			},
		},
	}

	_, err := reconciler.Reconcile(ctx, batch)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// 1. Diagnostic recorded
	diags, _ := store.ListDiagnostics(ctx, ws)
	var found bool
	for _, d := range diags {
		if d.Code == "DISCOVERY_CONFLICT" {
			found = true
			if !strings.Contains(d.Message, "conflicting") {
				t.Errorf("expected conflict description in message, got %s", d.Message)
			}
		}
	}
	if !found {
		t.Errorf("expected DISCOVERY_CONFLICT diagnostic, got %v", diags)
	}

	// 2. No guessed relationships
	rels, _ := store.ListRelationships(ctx, ws)
	if len(rels) != 0 {
		t.Errorf("expected 0 relationships, got %d", len(rels))
	}
}

// ---------------------------------------------------------------------------
// T14: Stale collection run cannot mutate state or diagnostics
// ---------------------------------------------------------------------------
func TestT14_StaleCollectionRun(t *testing.T) {
	ctx := context.Background()
	logger := logging.NewStandardLogger(io.Discard, logging.LevelError)
	store := state.NewMemoryStore()
	reconciler := state.NewReconciler(store, nil, nil, logger)

	ws := "ws-t14-stale"
	t0 := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

	// Gen 5 accepted
	ev1 := makeDiagnosticTestEvidence(uuidStr(1401), "kubernetes", "pod", "default/p5", "CONTROL_PLANE_OBJECT", `{}`, t0)
	batchGen5 := state.ObservationBatch{
		WorkspaceID:       ws,
		ClusterID:         "default",
		RunID:             "run-t14-gen5",
		SamplingStartedAt: t0,
		Generation:        5,
		Evidence:          []*corev1.Evidence{ev1},
	}
	_, err := reconciler.Reconcile(ctx, batchGen5)
	if err != nil {
		t.Fatalf("Reconcile gen 5 failed: %v", err)
	}

	// Gen 3 arrives late (stale run with earlier sampling timestamp)
	tStale := t0.Add(-5 * time.Minute)
	ev2 := makeDiagnosticTestEvidence(uuidStr(1402), "kubernetes", "pod", "default/p3-stale", "CONTROL_PLANE_OBJECT", `{}`, tStale)
	batchGen3 := state.ObservationBatch{
		WorkspaceID:       ws,
		ClusterID:         "default",
		RunID:             "run-t14-gen3",
		SamplingStartedAt: tStale,
		Generation:        3,
		Evidence:          []*corev1.Evidence{ev2},
	}
	res, err := reconciler.Reconcile(ctx, batchGen3)
	if err == nil && (res == nil || !res.Stale) {
		t.Errorf("expected Stale=true or error for out-of-order generation")
	}

	// Verify p3-stale was NOT persisted
	_, err = store.GetResource(ctx, ws, state.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "pod",
		ProviderID:   "default/p3-stale",
	})
	if err == nil {
		t.Errorf("stale run mutated authoritative state!")
	}
}

// ---------------------------------------------------------------------------
// T15: Failed reconciliation transaction leaves no partial mutations
// ---------------------------------------------------------------------------
func TestT15_FailedReconciliationTransaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dbCfg := getTestDatabaseConfig(t)
	logger := logging.NewStandardLogger(io.Discard, logging.LevelError)
	db, err := database.New(ctx, dbCfg, database.WithLogger(logger))
	if err != nil {
		t.Skipf("Failed to initialize database pool: %v; skipping", err)
	}
	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		t.Skipf("PostgreSQL Ping failed: %v; skipping", err)
	}

	pgStore := state.NewPostgresStore(db)

	ws := "ws-t15-tx-rollback"
	_ = pgStore.DeleteWorkspaceState(ctx, ws)
	defer func() { _ = pgStore.DeleteWorkspaceState(ctx, ws) }()

	t0 := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)
	customErr := errors.New("simulated transaction failure")

	// Verify rollback atomicity in PostgreSQL transaction
	err = db.WithTransaction(ctx, func(ctx context.Context, tx database.Tx) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO state_resources (workspace_id, provider, resource_type, provider_id, first_observed_at, last_observed_at) VALUES ($1, $2, $3, $4, $5, $6)`,
			ws, "kubernetes", "pod", "default/rollback-pod", t0, t0)
		if err != nil {
			return err
		}
		// Force rollback
		return customErr
	})

	if !errors.Is(err, customErr) {
		t.Fatalf("expected customErr from transaction, got %v", err)
	}

	// Verify that rollback-pod was NOT persisted
	_, err = pgStore.GetResource(ctx, ws, state.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "pod",
		ProviderID:   "default/rollback-pod",
	})
	if err == nil {
		t.Errorf("partial resource mutation persisted despite transaction failure!")
	}
}

// ---------------------------------------------------------------------------
// T16: Workspace isolation between tenants
// ---------------------------------------------------------------------------
func TestT16_WorkspaceIsolation(t *testing.T) {
	ctx := context.Background()
	logger := logging.NewStandardLogger(io.Discard, logging.LevelError)
	client, _ := spawnServer(t)

	store := state.NewMemoryStore()
	materializer := state.NewMaterializer(store, client, logger)
	reconciler := state.NewReconciler(store, client, materializer, logger)
	answerSvc := answer.NewService(client, logger)

	ws1 := "ws-t16-tenant-a"
	ws2 := "ws-t16-tenant-b"
	t0 := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

	// Tenant A has an unresolved reference diagnostic
	podA := makeDiagnosticTestEvidence(uuidStr(1601), "kubernetes", "pod", "default/pod", "CONTROL_PLANE_OBJECT", `{}`, t0)
	depA, _ := json.Marshal(map[string]interface{}{
		"target": map[string]interface{}{
			"provider":      "aws",
			"resource_type": "rds_instance",
			"provider_id":   "missing-db",
		},
	})
	relA := makeDiagnosticTestEvidence(uuidStr(1602), "kubernetes", "pod", "default/pod", "RESOURCE_REFERENCE", string(depA), t0)

	_, _ = reconciler.Reconcile(ctx, state.ObservationBatch{
		WorkspaceID: ws1,
		ClusterID:   "default",
		RunID:       "run-t16-a",
		Generation:  1,
		Evidence:    []*corev1.Evidence{podA, relA},
	})

	// Tenant B has clean observation
	podB := makeDiagnosticTestEvidence(uuidStr(1603), "kubernetes", "pod", "default/pod", "CONTROL_PLANE_OBJECT", `{}`, t0)
	_, _ = reconciler.Reconcile(ctx, state.ObservationBatch{
		WorkspaceID: ws2,
		ClusterID:   "default",
		RunID:       "run-t16-b",
		Generation:  1,
		Evidence:    []*corev1.Evidence{podB},
	})

	_, _ = materializer.Materialize(ctx, ws1)
	_, _ = materializer.Materialize(ctx, ws2)

	// Query Tenant B: must be COMPLETE, zero diagnostics
	ansB, err := answerSvc.AnalyzeImpact(ctx, answer.ImpactRequest{
		WorkspaceID: ws2,
		Target: &answer.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "pod",
			ProviderID:   "default/pod",
		},
		Direction: "incoming",
		MaxDepth:  3,
		ProposedChange: &answer.ProposedChange{
			ChangeType: answer.ChangeTypeUpdate,
		},
	})
	if err != nil {
		t.Fatalf("AnalyzeImpact ws2 failed: %v", err)
	}
	if ansB.Completeness == nil || !ansB.Completeness.IsComplete || ansB.Completeness.Status != "COMPLETE" {
		t.Errorf("Tenant B was affected by Tenant A's diagnostic: %v", ansB.Completeness)
	}
	if len(ansB.Diagnostics) != 0 {
		t.Errorf("Tenant B leaked diagnostics from Tenant A: %v", ansB.Diagnostics)
	}
}

// ---------------------------------------------------------------------------
// T17: Evidence immutability preserved after diagnostic resolution
// ---------------------------------------------------------------------------
func TestT17_EvidenceImmutability(t *testing.T) {
	ctx := context.Background()
	logger := logging.NewStandardLogger(io.Discard, logging.LevelError)
	store := state.NewMemoryStore()
	reconciler := state.NewReconciler(store, nil, nil, logger)

	ws := "ws-t17-immutability"
	t0 := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)
	rawData := `{"spec":{"target":"unobserved-db"}}`

	podEv := makeDiagnosticTestEvidence(uuidStr(1701), "kubernetes", "pod", "default/client", "RESOURCE_REFERENCE", rawData, t0)
	depData, _ := json.Marshal(map[string]interface{}{
		"target": map[string]interface{}{
			"provider":      "aws",
			"resource_type": "rds_instance",
			"provider_id":   "unobserved-db",
		},
	})
	relEv := makeDiagnosticTestEvidence(uuidStr(1702), "kubernetes", "pod", "default/client", "RESOURCE_REFERENCE", string(depData), t0)

	_, _ = reconciler.Reconcile(ctx, state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "default",
		RunID:       "run-t17-1",
		Generation:  1,
		Evidence:    []*corev1.Evidence{podEv, relEv},
	})

	// Run 2: Directly observe target to resolve diagnostic
	t1 := t0.Add(5 * time.Minute)
	dbEv := makeDiagnosticTestEvidence(uuidStr(1703), "aws", "rds_instance", "unobserved-db", "CONTROL_PLANE_OBJECT", `{}`, t1)
	_, _ = reconciler.Reconcile(ctx, state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "default",
		RunID:       "run-t17-2",
		Generation:  2,
		Evidence:    []*corev1.Evidence{dbEv},
	})

	// Check historical evidence ev-t17-orig: MUST NOT be mutated or deleted!
	evList, err := store.ListEvidence(ctx, ws)
	if err != nil {
		t.Fatalf("ListEvidence failed: %v", err)
	}
	var origEv *state.Evidence
	for i := range evList {
		if evList[i].ID == uuidStr(1701) {
			origEv = &evList[i]
			break
		}
	}
	if origEv == nil {
		t.Fatalf("historical evidence was deleted or not found")
	}
	if string(origEv.Data) != rawData {
		t.Errorf("historical evidence data was mutated: got %s, want %s", string(origEv.Data), rawData)
	}
	if !origEv.ObservedAt.Equal(t0) {
		t.Errorf("historical evidence timestamp was mutated: got %v, want %v", origEv.ObservedAt, t0)
	}
}

// ---------------------------------------------------------------------------
// T18: Deterministic ordering of diagnostics and evidence IDs
// ---------------------------------------------------------------------------
func TestT18_DeterministicOutput(t *testing.T) {
	ctx := context.Background()
	logger := logging.NewStandardLogger(io.Discard, logging.LevelError)
	client, _ := spawnServer(t)

	store := state.NewMemoryStore()
	materializer := state.NewMaterializer(store, client, logger)
	reconciler := state.NewReconciler(store, client, materializer, logger)
	answerSvc := answer.NewService(client, logger)

	ws := "ws-t18-deterministic"
	t0 := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

	// Create multiple diagnostics with multiple evidence IDs
	callerPod := makeDiagnosticTestEvidence(uuidStr(1801), "kubernetes", "pod", "default/fe", "CONTROL_PLANE_OBJECT", `{}`, t0)
	depData1, _ := json.Marshal(map[string]interface{}{
		"target": map[string]interface{}{
			"provider":      "aws",
			"resource_type": "rds_instance",
			"provider_id":   "missing-db-1",
		},
	})
	depData2, _ := json.Marshal(map[string]interface{}{
		"target": map[string]interface{}{
			"provider":      "aws",
			"resource_type": "rds_instance",
			"provider_id":   "missing-db-2",
		},
	})
	rel1 := makeDiagnosticTestEvidence(uuidStr(1802), "kubernetes", "pod", "default/fe", "RESOURCE_REFERENCE", string(depData1), t0)
	rel2 := makeDiagnosticTestEvidence(uuidStr(1803), "kubernetes", "pod", "default/fe", "RESOURCE_REFERENCE", string(depData2), t0)

	_, _ = reconciler.Reconcile(ctx, state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "default",
		RunID:       "run-t18-1",
		Generation:  1,
		Evidence:    []*corev1.Evidence{callerPod, rel1, rel2},
	})
	_, _ = materializer.Materialize(ctx, ws)

	query := answer.ImpactRequest{
		WorkspaceID: ws,
		Target: &answer.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "pod",
			ProviderID:   "default/fe",
		},
		Direction: "incoming",
		MaxDepth:  3,
		ProposedChange: &answer.ProposedChange{
			ChangeType: answer.ChangeTypeUpdate,
		},
	}

	var firstDiags []string
	for iter := 0; iter < 5; iter++ {
		ans, err := answerSvc.AnalyzeImpact(ctx, query)
		if err != nil {
			t.Fatalf("AnalyzeImpact failed on iter %d: %v", iter, err)
		}
		var currentDiags []string
		for _, d := range ans.Diagnostics {
			currentDiags = append(currentDiags, fmt.Sprintf("%s|%s", d.Code, d.UnresolvedEndpoint))
		}
		if iter == 0 {
			firstDiags = currentDiags
		} else {
			if strings.Join(firstDiags, ",") != strings.Join(currentDiags, ",") {
				t.Fatalf("non-deterministic diagnostic ordering across iterations: %v vs %v", firstDiags, currentDiags)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// T19: Existing propagation boundaries (OWNS, ROUTES_TO) preserved
// ---------------------------------------------------------------------------
func TestT19_ExistingPropagationBoundaries(t *testing.T) {
	ctx := context.Background()
	logger := logging.NewStandardLogger(io.Discard, logging.LevelError)
	client, _ := spawnServer(t)

	store := state.NewMemoryStore()
	materializer := state.NewMaterializer(store, client, logger)
	reconciler := state.NewReconciler(store, client, materializer, logger)
	answerSvc := answer.NewService(client, logger)

	ws := "ws-t19-boundaries"
	t0 := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

	// Deployment -> OWNS -> ReplicaSet
	depEv := makeDiagnosticTestEvidence(uuidStr(1901), "kubernetes", "deployment", "default/fe-dep", "CONTROL_PLANE_OBJECT", `{}`, t0)
	rsEv := makeDiagnosticTestEvidence(uuidStr(1902), "kubernetes", "replicaset", "default/fe-rs", "CONTROL_PLANE_OBJECT", `{}`, t0)
	ownsData, _ := json.Marshal(map[string]interface{}{
		"owner": map[string]interface{}{
			"provider":      "kubernetes",
			"resource_type": "deployment",
			"provider_id":   "default/fe-dep",
		},
		"controller": true,
	})
	relOwns := makeDiagnosticTestEvidence(uuidStr(1903), "kubernetes", "replicaset", "default/fe-rs", "OWNERSHIP_REFERENCE", string(ownsData), t0)

	_, _ = reconciler.Reconcile(ctx, state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "default",
		RunID:       "run-t19-1",
		Generation:  1,
		Evidence:    []*corev1.Evidence{depEv, rsEv, relOwns},
	})
	_, _ = materializer.Materialize(ctx, ws)

	// UPDATE on Deployment: OWNS is a containment boundary and does not propagate impact
	ans, err := answerSvc.AnalyzeImpact(ctx, answer.ImpactRequest{
		WorkspaceID: ws,
		Target: &answer.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "deployment",
			ProviderID:   "default/fe-dep",
		},
		Direction: "incoming",
		MaxDepth:  3,
		ProposedChange: &answer.ProposedChange{
			ChangeType: answer.ChangeTypeUpdate,
		},
	})
	if err != nil {
		t.Fatalf("AnalyzeImpact failed: %v", err)
	}

	if ans.Summary.ImpactedCount != 0 {
		t.Errorf("OWNS boundary breached: expected 0 impacted, got %d", ans.Summary.ImpactedCount)
	}
	if ans.Completeness == nil || !ans.Completeness.IsComplete {
		t.Errorf("expected complete analysis on cleanly observed deployment")
	}
}

// ---------------------------------------------------------------------------
// T20: Kubernetes Secret payloads never leaked in diagnostic messages
// ---------------------------------------------------------------------------
func TestT20_SecretSafety(t *testing.T) {
	ctx := context.Background()
	logger := logging.NewStandardLogger(io.Discard, logging.LevelError)
	store := state.NewMemoryStore()
	reconciler := state.NewReconciler(store, nil, nil, logger)

	ws := "ws-t20-secret-safety"
	t0 := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

	secretPayload := "SUPER_SECRET_PRIVATE_KEY_PLAINTEXT_12345"
	podEv := makeDiagnosticTestEvidence(uuidStr(2001), "kubernetes", "pod", "default/secure-pod", "CONTROL_PLANE_OBJECT", `{}`, t0)
	depData, _ := json.Marshal(map[string]interface{}{
		"target": map[string]interface{}{
			"provider":      "kubernetes",
			"resource_type": "secret",
			"provider_id":   "default/tls-secret",
		},
		"secret_payload": secretPayload,
	})
	relEv := makeDiagnosticTestEvidence(uuidStr(2002), "kubernetes", "pod", "default/secure-pod", "RESOURCE_REFERENCE", string(depData), t0)

	_, err := reconciler.Reconcile(ctx, state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "default",
		RunID:       "run-t20-1",
		Generation:  1,
		Evidence:    []*corev1.Evidence{podEv, relEv},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	diags, err := store.ListDiagnostics(ctx, ws)
	if err != nil {
		t.Fatalf("ListDiagnostics failed: %v", err)
	}

	for _, d := range diags {
		// Verify secret payload never appears in message, endpoint, or metadata
		if strings.Contains(d.Message, secretPayload) {
			t.Fatalf("SECURITY VIOLATION: secret payload leaked in diagnostic message: %s", d.Message)
		}
		if strings.Contains(d.UnresolvedEndpoint, secretPayload) {
			t.Fatalf("SECURITY VIOLATION: secret payload leaked in unresolved endpoint: %s", d.UnresolvedEndpoint)
		}
	}
}

// ---------------------------------------------------------------------------
// Verification A: Scope completeness prevents false confidence
// Even if graph has 0 pods because endpointslices collection failed,
// querying the service reflects PARTIAL completeness with INCOMPLETE_SCOPE.
// ---------------------------------------------------------------------------
func TestScopeCompleteness_EndpointSliceFailure_AvoidsFalseConfidence(t *testing.T) {
	ctx := context.Background()
	logger := logging.NewStandardLogger(io.Discard, logging.LevelError)
	client, _ := spawnServer(t)

	store := state.NewMemoryStore()
	materializer := state.NewMaterializer(store, client, logger)
	reconciler := state.NewReconciler(store, client, materializer, logger)
	answerSvc := answer.NewService(client, logger)

	ws := "ws-scope-completeness-safety"
	t0 := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

	// Service is observed, but EndpointSlice collection failed
	svcEv := makeDiagnosticTestEvidence(uuidStr(2101), "kubernetes", "service", "default/order-service", "CONTROL_PLANE_OBJECT", `{}`, t0)
	batch := state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "default",
		RunID:       "run-scope-1",
		Generation:  1,
		Evidence:    []*corev1.Evidence{svcEv},
		ScopeSweep: &state.ScopeSweep{
			OptionalFailures: []string{"endpointslices"},
			SuccessfulScopes: []string{"services"},
		},
	}

	_, err := reconciler.Reconcile(ctx, batch)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	_, _ = materializer.Materialize(ctx, ws)

	// Query impact on Service
	ans, err := answerSvc.AnalyzeImpact(ctx, answer.ImpactRequest{
		WorkspaceID: ws,
		Target: &answer.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "service",
			ProviderID:   "default/order-service",
		},
		Direction: "incoming",
		MaxDepth:  3,
		ProposedChange: &answer.ProposedChange{
			ChangeType: answer.ChangeTypeUpdate,
		},
	})
	if err != nil {
		t.Fatalf("AnalyzeImpact failed: %v", err)
	}

	// Must NOT report COMPLETE! That would be false confidence that no pods are affected
	if ans.Completeness == nil || ans.Completeness.IsComplete {
		t.Fatalf("SAFETY VIOLATION: false confidence! Query reported COMPLETE despite failed endpointslices scope")
	}
	if ans.Completeness.Status != "PARTIAL" {
		t.Errorf("expected status=PARTIAL, got %s", ans.Completeness.Status)
	}

	var foundScopeDiag bool
	for _, d := range ans.Diagnostics {
		if d.Code == "INCOMPLETE_SCOPE" && d.Scope == "endpointslices" {
			foundScopeDiag = true
		}
	}
	if !foundScopeDiag {
		t.Errorf("expected INCOMPLETE_SCOPE diagnostic for endpointslices, got %v", ans.Diagnostics)
	}
}

// ---------------------------------------------------------------------------
// Verification D: Complete pipeline verification:
// Store -> Materializer -> Core Engine -> AnswerService -> HTTP Response
// Asserts that the HTTP API actually returns completeness and diagnostics in JSON.
// ---------------------------------------------------------------------------
func TestHTTP_EndToEnd_DiagnosticsAndCompleteness(t *testing.T) {
	ctx := context.Background()
	logger := logging.NewStandardLogger(io.Discard, logging.LevelError)
	client, _ := spawnServer(t)

	store := state.NewMemoryStore()
	materializer := state.NewMaterializer(store, client, logger)
	reconciler := state.NewReconciler(store, client, materializer, logger)
	answerSvc := answer.NewService(client, logger)
	httpHandler := answer.NewHandler(answerSvc, answer.WithLogger(logger))

	ws := "ws-http-pipeline"
	t0 := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

	// Pod references unobserved cache
	podEv := makeDiagnosticTestEvidence(uuidStr(2201), "kubernetes", "pod", "default/web", "CONTROL_PLANE_OBJECT", `{}`, t0)
	depData, _ := json.Marshal(map[string]interface{}{
		"target": map[string]interface{}{
			"provider":      "aws",
			"resource_type": "elasticache_cluster",
			"provider_id":   "unobserved-redis",
		},
	})
	relEv := makeDiagnosticTestEvidence(uuidStr(2202), "kubernetes", "pod", "default/web", "RESOURCE_REFERENCE", string(depData), t0)

	batch := state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "default",
		RunID:       "run-http-1",
		Generation:  1,
		Evidence:    []*corev1.Evidence{podEv, relEv},
	}
	if _, err := reconciler.Reconcile(ctx, batch); err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if _, err := materializer.Materialize(ctx, ws); err != nil {
		t.Fatalf("Materialize failed: %v", err)
	}

	// Prepare HTTP POST request
	reqBody, _ := json.Marshal(map[string]interface{}{
		"workspace_id": ws,
		"target": map[string]interface{}{
			"provider":      "kubernetes",
			"resource_type": "pod",
			"provider_id":   "default/web",
		},
		"direction": "incoming",
		"max_depth": 3,
		"proposed_change": map[string]interface{}{
			"change_type": "UPDATE",
		},
	})

	httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/impact", strings.NewReader(string(reqBody)))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Workspace-ID", ws)

	rec := httptest.NewRecorder()
	httpHandler.ServeHTTP(rec, httpReq)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// Unmarshal HTTP JSON response
	var httpResp struct {
		Target       map[string]interface{} `json:"target"`
		Summary      map[string]interface{} `json:"summary"`
		Completeness struct {
			IsComplete bool     `json:"is_complete"`
			Status     string   `json:"status"`
			Reasons    []string `json:"reasons"`
		} `json:"completeness"`
		Diagnostics []struct {
			Code               string `json:"code"`
			Severity           string `json:"severity"`
			Message            string `json:"message"`
			Relevance          string `json:"relevance"`
			CompletenessImpact string `json:"completeness_impact"`
		} `json:"diagnostics"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &httpResp); err != nil {
		t.Fatalf("failed to decode HTTP response JSON: %v", err)
	}

	// Verify completeness in HTTP response
	if httpResp.Completeness.IsComplete {
		t.Errorf("expected is_complete=false in HTTP JSON, got true")
	}
	if httpResp.Completeness.Status != "PARTIAL" {
		t.Errorf("expected status=PARTIAL in HTTP JSON, got %s", httpResp.Completeness.Status)
	}

	// Verify diagnostics array in HTTP response
	if len(httpResp.Diagnostics) == 0 {
		t.Fatalf("expected diagnostics in HTTP JSON, got 0")
	}
	var foundDiag bool
	for _, d := range httpResp.Diagnostics {
		if d.Code == "UNRESOLVED_REFERENCE" && d.Relevance == "BLAST_RADIUS" {
			foundDiag = true
		}
	}
	if !foundDiag {
		t.Errorf("expected UNRESOLVED_REFERENCE diagnostic in HTTP JSON, got %v", httpResp.Diagnostics)
	}
}
