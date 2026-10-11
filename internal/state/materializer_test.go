package state_test

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "github.com/aaradhychinche-alt/WhatBreaks/gen/go/wb/core/v1"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/state"
	"google.golang.org/grpc"
)

type mockCoreLoader struct {
	lastReq   *corev1.LoadStateRequest
	resp      *corev1.LoadStateResponse
	err       error
	callCount int
}

func (m *mockCoreLoader) LoadState(ctx context.Context, req *corev1.LoadStateRequest, opts ...grpc.CallOption) (*corev1.LoadStateResponse, error) {
	m.callCount++
	m.lastReq = req
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &corev1.LoadStateResponse{
		WorkspaceId:           req.WorkspaceId,
		RelationshipsLoaded:   uint32(len(req.Relationships)),
		EvidenceLoaded:        uint32(len(req.Evidence)),
		ProvenanceLinksLoaded: uint32(len(req.Associations)),
	}, nil
}

func TestMaterializer_Materialize_Success(t *testing.T) {
	ctx := context.Background()
	store := state.NewMemoryStore()
	workspaceID := "ws-mat-test-1"

	// Setup data in store
	res1 := state.Resource{
		WorkspaceID: workspaceID,
		Identity: state.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "service",
			ProviderID:   "frontend",
		},
	}
	res2 := state.Resource{
		WorkspaceID: workspaceID,
		Identity: state.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "service",
			ProviderID:   "backend",
		},
	}
	if err := store.SaveResources(ctx, workspaceID, []state.Resource{res1, res2}); err != nil {
		t.Fatalf("failed to save resources: %v", err)
	}

	evID := "11111111-1111-1111-1111-111111111111"
	ev := state.Evidence{
		WorkspaceID: workspaceID,
		ID:          evID,
		Source: state.EvidenceSource{
			Provider:  "kubernetes",
			Collector: "k8s-informer",
		},
		ObservedAt:      time.Now().UTC(),
		ObservationType: "CONFIG_REFERENCE",
		Subject:         res1.Identity,
		Data:            []byte(`{"spec": "clusterIP"}`),
	}
	if err := store.SaveEvidence(ctx, workspaceID, []state.Evidence{ev}); err != nil {
		t.Fatalf("failed to save evidence: %v", err)
	}

	rel := state.Relationship{
		WorkspaceID:     workspaceID,
		Source:          res1.Identity,
		Target:          res2.Identity,
		Category:        "network",
		Kind:            "routes_to",
		FirstObservedAt: time.Now().UTC(),
		LastObservedAt:  time.Now().UTC(),
	}
	if err := store.SaveRelationships(ctx, workspaceID, []state.Relationship{rel}); err != nil {
		t.Fatalf("failed to save relationships: %v", err)
	}

	prov := state.ProvenanceAssociation{
		WorkspaceID:  workspaceID,
		Relationship: rel.Key(),
		EvidenceID:   evID,
	}
	if err := store.SaveProvenance(ctx, workspaceID, []state.ProvenanceAssociation{prov}); err != nil {
		t.Fatalf("failed to save provenance: %v", err)
	}

	loader := &mockCoreLoader{}
	mat := state.NewMaterializer(store, loader, nil)

	res, err := mat.Materialize(ctx, workspaceID)
	if err != nil {
		t.Fatalf("Materialize failed: %v", err)
	}

	if res.RelationshipsLoaded != 1 {
		t.Errorf("expected 1 relationship loaded, got %d", res.RelationshipsLoaded)
	}
	if res.EvidenceLoaded != 1 {
		t.Errorf("expected 1 evidence loaded, got %d", res.EvidenceLoaded)
	}
	if res.ProvenanceLinksLoaded != 1 {
		t.Errorf("expected 1 provenance entry loaded, got %d", res.ProvenanceLinksLoaded)
	}

	// Verify request sent to loader
	req := loader.lastReq
	if req == nil {
		t.Fatalf("expected lastReq to be non-nil")
	}
	if req.WorkspaceId != workspaceID {
		t.Errorf("expected workspace ID %q, got %q", workspaceID, req.WorkspaceId)
	}
	if len(req.Relationships) != 1 {
		t.Errorf("expected 1 proto relationship, got %d", len(req.Relationships))
	}
	if req.Relationships[0].Source.ProviderId != "frontend" {
		t.Errorf("unexpected relationship source: %v", req.Relationships[0].Source)
	}
	if len(req.Evidence) != 1 {
		t.Errorf("expected 1 proto evidence, got %d", len(req.Evidence))
	}
	if req.Evidence[0].Id != evID {
		t.Errorf("expected proto evidence ID %s, got %s", evID, req.Evidence[0].Id)
	}
	if len(req.Associations) != 1 {
		t.Errorf("expected 1 proto association, got %d", len(req.Associations))
	}
}

func TestMaterializer_Errors(t *testing.T) {
	ctx := context.Background()

	t.Run("nil loader", func(t *testing.T) {
		mat := state.NewMaterializer(state.NewMemoryStore(), nil, nil)
		_, err := mat.Materialize(ctx, "ws-1")
		if err == nil {
			t.Fatalf("expected error when loader is nil")
		}
	})

	t.Run("loader returns error", func(t *testing.T) {
		loader := &mockCoreLoader{err: errors.New("rpc connection refused")}
		mat := state.NewMaterializer(state.NewMemoryStore(), loader, nil)
		_, err := mat.Materialize(ctx, "ws-1")
		if err == nil {
			t.Fatalf("expected error when loader fails")
		}
	})
}

func TestMaterializer_FreshAuthoritativeSnapshotOnSubsequentCalls(t *testing.T) {
	ctx := context.Background()
	store := state.NewMemoryStore()
	ws := "ws-fresh-snapshot"

	res1 := state.Resource{
		WorkspaceID: ws,
		Identity: state.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "service",
			ProviderID:   "svc-1",
		},
	}
	_ = store.SaveResources(ctx, ws, []state.Resource{res1})

	loader := &mockCoreLoader{}
	mat := state.NewMaterializer(store, loader, nil)

	// First materialization
	_, err := mat.Materialize(ctx, ws)
	if err != nil {
		t.Fatalf("first Materialize failed: %v", err)
	}
	if loader.callCount != 1 {
		t.Errorf("expected callCount 1, got %d", loader.callCount)
	}
	if len(loader.lastReq.Relationships) != 0 {
		t.Errorf("expected 0 relationships in first call")
	}

	// Update authoritative store with a new relationship
	rel1 := state.Relationship{
		WorkspaceID: ws,
		Source:      res1.Identity,
		Target: state.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "service",
			ProviderID:   "svc-2",
		},
		Category: "network",
		Kind:     "routes_to",
	}
	_ = store.SaveRelationships(ctx, ws, []state.Relationship{rel1})

	// Second materialization (e.g. recovery retry or next cycle): MUST load fresh state from store!
	_, err = mat.Materialize(ctx, ws)
	if err != nil {
		t.Fatalf("second Materialize failed: %v", err)
	}
	if loader.callCount != 2 {
		t.Errorf("expected callCount 2, got %d", loader.callCount)
	}
	if len(loader.lastReq.Relationships) != 1 {
		t.Fatalf("expected fresh snapshot with 1 relationship, got %d", len(loader.lastReq.Relationships))
	}
	if loader.lastReq.Relationships[0].Target.ProviderId != "svc-2" {
		t.Errorf("expected updated relationship to svc-2, got %s", loader.lastReq.Relationships[0].Target.ProviderId)
	}
}

func TestReconciler_Rematerialize_Validation(t *testing.T) {
	ctx := context.Background()
	store := state.NewMemoryStore()

	// 1. Empty workspace
	recNoMat := state.NewReconciler(store, nil, nil, nil)
	if err := recNoMat.Rematerialize(ctx, ""); !errors.Is(err, state.ErrEmptyWorkspace) {
		t.Errorf("expected ErrEmptyWorkspace for empty string, got: %v", err)
	}
	if err := recNoMat.Rematerialize(ctx, "   "); !errors.Is(err, state.ErrEmptyWorkspace) {
		t.Errorf("expected ErrEmptyWorkspace for whitespace, got: %v", err)
	}

	// 2. Unconfigured materializer
	if err := recNoMat.Rematerialize(ctx, "ws-1"); err == nil {
		t.Errorf("expected error when materializer is unconfigured, got nil")
	}

	// 3. Configured materializer
	loader := &mockCoreLoader{}
	mat := state.NewMaterializer(store, loader, nil)
	recWithMat := state.NewReconciler(store, nil, mat, nil)

	if err := recWithMat.Rematerialize(ctx, "ws-1"); err != nil {
		t.Fatalf("expected successful Rematerialize, got: %v", err)
	}
	if loader.callCount != 1 {
		t.Errorf("expected 1 call to CoreLoader, got %d", loader.callCount)
	}
}

