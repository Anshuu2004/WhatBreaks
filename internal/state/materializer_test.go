package state_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	corev1 "github.com/aaradhychinche-alt/WhatBreaks/gen/go/wb/core/v1"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/state"
	"google.golang.org/grpc"
)

type mockCoreLoader struct {
	mu          sync.Mutex
	lastReq     *corev1.LoadStateRequest
	resp        *corev1.LoadStateResponse
	err         error
	callCount   int
	onLoadState func(ctx context.Context, req *corev1.LoadStateRequest) error
}

func (m *mockCoreLoader) LoadState(ctx context.Context, req *corev1.LoadStateRequest, opts ...grpc.CallOption) (*corev1.LoadStateResponse, error) {
	m.mu.Lock()
	m.callCount++
	m.lastReq = req
	onLoad := m.onLoadState
	err := m.err
	resp := m.resp
	m.mu.Unlock()

	if onLoad != nil {
		if hookErr := onLoad(ctx, req); hookErr != nil {
			return nil, hookErr
		}
	}

	if err != nil {
		return nil, err
	}
	if resp != nil {
		return resp, nil
	}
	return &corev1.LoadStateResponse{
		WorkspaceId:           req.WorkspaceId,
		RelationshipsLoaded:   uint32(len(req.Relationships)),
		EvidenceLoaded:        uint32(len(req.Evidence)),
		ProvenanceLinksLoaded: uint32(len(req.Associations)),
	}, nil
}

func (m *mockCoreLoader) getLastReq() *corev1.LoadStateRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastReq
}

type hookStore struct {
	state.Store
	onLoadAssembledState func(ctx context.Context, workspaceID string)
	onSaveResources      func(ctx context.Context, workspaceID string, resources []state.Resource)
}

func (h *hookStore) LoadAssembledState(ctx context.Context, workspaceID string) (*state.AssembledState, error) {
	if h.onLoadAssembledState != nil {
		h.onLoadAssembledState(ctx, workspaceID)
	}
	return h.Store.LoadAssembledState(ctx, workspaceID)
}

func (h *hookStore) SaveResources(ctx context.Context, workspaceID string, resources []state.Resource) error {
	err := h.Store.SaveResources(ctx, workspaceID, resources)
	if h.onSaveResources != nil {
		h.onSaveResources(ctx, workspaceID, resources)
	}
	return err
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

// ---------------------------------------------------------------------------
// CONCURRENCY & MATERIALIZATION ORDERING REGRESSION TESTS
// ---------------------------------------------------------------------------

// TEST A: SAME-WORKSPACE MATERIALIZATION SERIALIZATION
// Proves that materializations targeting the same workspace cannot interleave,
// and that B reads its authoritative snapshot only after A releases the lock.
func TestMaterializer_Concurrency_SameWorkspaceSerialized(t *testing.T) {
	ctx := context.Background()
	baseStore := state.NewMemoryStore()
	ws := "ws-serialize-test"

	res := state.Resource{
		WorkspaceID: ws,
		Identity: state.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "service",
			ProviderID:   "svc-a",
		},
	}
	_ = baseStore.SaveResources(ctx, ws, []state.Resource{res})

	aEnteredLoadState := make(chan struct{})
	aReleaseLoadState := make(chan struct{})
	bEnteredSnapshotRead := make(chan struct{}, 1)

	hStore := &hookStore{
		Store: baseStore,
		onLoadAssembledState: func(ctx context.Context, workspaceID string) {
			select {
			case <-aEnteredLoadState:
				// If A already entered LoadState, this is B reading its snapshot
				bEnteredSnapshotRead <- struct{}{}
			default:
				// This is A reading snapshot initially
			}
		},
	}

	loader := &mockCoreLoader{
		onLoadState: func(ctx context.Context, req *corev1.LoadStateRequest) error {
			if req.WorkspaceId == ws {
				select {
				case <-aEnteredLoadState:
					// Second call (B)
				default:
					close(aEnteredLoadState)
					<-aReleaseLoadState
				}
			}
			return nil
		},
	}

	mat := state.NewMaterializer(hStore, loader, nil)

	// Start Materialization A
	errA := make(chan error, 1)
	go func() {
		_, err := mat.Materialize(ctx, ws)
		errA <- err
	}()

	// Wait until A has acquired lock, loaded snapshot, and is holding lock inside LoadState
	<-aEnteredLoadState

	// Now start Materialization B in background
	errB := make(chan error, 1)
	go func() {
		_, err := mat.Materialize(ctx, ws)
		errB <- err
	}()

	// Verify B CANNOT enter snapshot read while A is holding lock
	time.Sleep(20 * time.Millisecond)
	select {
	case <-bEnteredSnapshotRead:
		t.Fatalf("RACE VIOLATION: Materialization B entered snapshot read before A released lock!")
	default:
		// Expected: B is blocked waiting for lock
	}

	// Release Materialization A
	close(aReleaseLoadState)

	if err := <-errA; err != nil {
		t.Fatalf("Materialization A failed: %v", err)
	}

	// Now B must unblock, read snapshot, and complete
	select {
	case <-bEnteredSnapshotRead:
		// Expected! B read snapshot after A released lock
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for B to enter snapshot read")
	}

	if err := <-errB; err != nil {
		t.Fatalf("Materialization B failed: %v", err)
	}

	if loader.callCount != 2 {
		t.Errorf("expected 2 LoadState calls, got %d", loader.callCount)
	}
}

// TEST B: NEWEST STATE WINS
// Simulates an older materialization overlapping with a newer reconciliation,
// proving that the final state observed by Core Engine represents the newest
// committed PostgreSQL state and an older snapshot cannot overwrite it.
func TestMaterializer_Concurrency_NewestStateWins(t *testing.T) {
	ctx := context.Background()
	baseStore := state.NewMemoryStore()
	ws := "ws-newest-wins"

	// Initial State 1 in store: pod-v1
	resV1 := state.Resource{
		WorkspaceID: ws,
		Identity: state.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "pod",
			ProviderID:   "pod-v1",
		},
	}
	_ = baseStore.SaveResources(ctx, ws, []state.Resource{resV1})

	aInLoadState := make(chan struct{})
	aRelease := make(chan struct{})

	loader := &mockCoreLoader{
		onLoadState: func(ctx context.Context, req *corev1.LoadStateRequest) error {
			// If this is request containing only pod-v1 (older snapshot)
			if len(req.Relationships) == 0 && len(req.Associations) == 0 {
				select {
				case <-aInLoadState:
				default:
					close(aInLoadState)
					<-aRelease
				}
			}
			return nil
		},
	}

	bCommittedStore := make(chan struct{}, 1)
	hStore := &hookStore{
		Store: baseStore,
		onSaveResources: func(ctx context.Context, workspaceID string, resources []state.Resource) {
			if workspaceID == ws {
				select {
				case bCommittedStore <- struct{}{}:
				default:
				}
			}
		},
	}

	mat := state.NewMaterializer(hStore, loader, nil)

	bWaitingForLock := make(chan struct{}, 1)
	mat.SetOnLockWait(func(workspaceID string) {
		if workspaceID == ws {
			select {
			case bWaitingForLock <- struct{}{}:
			default:
			}
		}
	})

	reconciler := state.NewReconciler(hStore, nil, mat, nil)

	// Start Materialization A (older state)
	doneA := make(chan error, 1)
	go func() {
		_, err := mat.Materialize(ctx, ws)
		doneA <- err
	}()

	// Sequence Step a: Operation A reads its database snapshot and pauses before completing
	// its Rust LoadState operation.
	select {
	case <-aInLoadState:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for Operation A to enter LoadState")
	}

	// Now while A is paused in LoadState, Reconciliation B commits State 2 to PostgreSQL/store
	// and invokes ReconcileAndMaterialize
	t0 := time.Now().UTC()
	podV2Ev := newTestEvidence("ev-v2", "kubernetes", "pod", "pod-v2", "CONTROL_PLANE_OBJECT", `{}`, t0)
	batchB := state.ObservationBatch{
		WorkspaceID: ws,
		Evidence:    []*corev1.Evidence{podV2Ev},
	}

	doneB := make(chan error, 1)
	go func() {
		_, err := reconciler.ReconcileAndMaterialize(ctx, batchB)
		doneB <- err
	}()

	// Sequence Step b: Operation B completes its newer database reconciliation.
	select {
	case <-bCommittedStore:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for Operation B to commit newer state to store")
	}

	// Sequence Step c: Operation B demonstrably reaches the point where it is waiting for
	// the same workspace's materialization lock.
	select {
	case <-bWaitingForLock:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for Operation B to wait for workspace materialization lock")
	}

	// Sequence Step d: Operation A is released.
	close(aRelease)

	// Sequence Step e: Operation B completes its materialization afterward.
	select {
	case err := <-doneA:
		if err != nil {
			t.Fatalf("Materialization A failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for Materialization A to complete")
	}

	select {
	case err := <-doneB:
		if err != nil {
			t.Fatalf("ReconcileAndMaterialize B failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for ReconcileAndMaterialize B to complete")
	}

	// Sequence Step f: The final Rust state contains the newer state from B.
	lastReq := loader.getLastReq()
	if lastReq == nil {
		t.Fatalf("expected lastReq to be non-nil")
	}

	hasPodV2 := false
	for _, ev := range lastReq.Evidence {
		if ev.Subject != nil && ev.Subject.ProviderId == "pod-v2" {
			hasPodV2 = true
			break
		}
	}
	if !hasPodV2 {
		t.Fatalf("CORRECTNESS REGRESSION: older snapshot A overwrote newer snapshot B! lastReq evidence: %+v", lastReq.Evidence)
	}
}

// TEST C: WORKSPACE ISOLATION
// Verifies that materialization for workspace B can proceed and finish
// while workspace A is blocked inside materialization.
func TestMaterializer_Concurrency_WorkspaceIsolation(t *testing.T) {
	ctx := context.Background()
	store := state.NewMemoryStore()

	wsAlpha := "ws-alpha"
	wsBeta := "ws-beta"

	_ = store.SaveResources(ctx, wsAlpha, []state.Resource{{WorkspaceID: wsAlpha, Identity: state.ResourceIdentity{Provider: "k8s", ResourceType: "pod", ProviderID: "p-a"}}})
	_ = store.SaveResources(ctx, wsBeta, []state.Resource{{WorkspaceID: wsBeta, Identity: state.ResourceIdentity{Provider: "k8s", ResourceType: "pod", ProviderID: "p-b"}}})

	alphaPaused := make(chan struct{})
	alphaProceed := make(chan struct{})

	loader := &mockCoreLoader{
		onLoadState: func(ctx context.Context, req *corev1.LoadStateRequest) error {
			if req.WorkspaceId == wsAlpha {
				close(alphaPaused)
				<-alphaProceed
			}
			return nil
		},
	}

	mat := state.NewMaterializer(store, loader, nil)

	// Start Materialize for wsAlpha in background
	doneAlpha := make(chan error, 1)
	go func() {
		_, err := mat.Materialize(ctx, wsAlpha)
		doneAlpha <- err
	}()

	// Wait until wsAlpha is blocked in LoadState
	<-alphaPaused

	// Now call Materialize for wsBeta on main thread: MUST NOT BLOCK!
	betaDone := make(chan error, 1)
	go func() {
		_, err := mat.Materialize(ctx, wsBeta)
		betaDone <- err
	}()

	select {
	case err := <-betaDone:
		if err != nil {
			t.Fatalf("wsBeta failed: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("ISOLATION VIOLATION: wsBeta blocked on wsAlpha materialization lock!")
	}

	// Release wsAlpha
	close(alphaProceed)
	if err := <-doneAlpha; err != nil {
		t.Fatalf("wsAlpha failed: %v", err)
	}
}

// TEST D: ERROR AND CANCELLATION
// Verifies that locks are released upon errors and cancellations, allowing
// subsequent materializations to proceed, and that lock entries are pruned.
func TestMaterializer_Concurrency_ErrorAndCancellation(t *testing.T) {
	ctx := context.Background()
	store := state.NewMemoryStore()
	ws := "ws-err-cancel"

	_ = store.SaveResources(ctx, ws, []state.Resource{{WorkspaceID: ws, Identity: state.ResourceIdentity{Provider: "k8s", ResourceType: "pod", ProviderID: "p-1"}}})

	// Part 1: Error release
	loaderErr := errors.New("gRPC core load error")
	loader := &mockCoreLoader{err: loaderErr}
	mat := state.NewMaterializer(store, loader, nil)

	_, err := mat.Materialize(ctx, ws)
	if err == nil {
		t.Fatalf("expected error from loader, got nil")
	}

	// Verify lock was released and registry cleaned up
	if count := mat.ActiveLockCount(); count != 0 {
		t.Errorf("expected 0 active locks after error return, got %d", count)
	}

	// Immediate next materialization must succeed
	loader.err = nil
	resp, err := mat.Materialize(ctx, ws)
	if err != nil {
		t.Fatalf("subsequent materialization after error failed: %v", err)
	}
	if resp == nil {
		t.Fatalf("expected non-nil response")
	}
	if count := mat.ActiveLockCount(); count != 0 {
		t.Errorf("expected 0 active locks after success, got %d", count)
	}

	// Part 2: Context cancellation while waiting for lock
	aInLoadState := make(chan struct{})
	aProceed := make(chan struct{})

	loader.onLoadState = func(ctx context.Context, req *corev1.LoadStateRequest) error {
		select {
		case <-aInLoadState:
		default:
			close(aInLoadState)
			<-aProceed
		}
		return nil
	}

	go func() {
		_, _ = mat.Materialize(context.Background(), ws)
	}()

	<-aInLoadState

	// Start B with cancellable context
	ctxB, cancelB := context.WithCancel(context.Background())
	errB := make(chan error, 1)
	go func() {
		_, err := mat.Materialize(ctxB, ws)
		errB <- err
	}()

	// Cancel B while waiting
	time.Sleep(10 * time.Millisecond)
	cancelB()

	select {
	case err := <-errB:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled for B, got %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for cancelled B to return")
	}

	// Unblock A
	close(aProceed)

	// Subsequent materialization C with valid context succeeds
	respC, errC := mat.Materialize(context.Background(), ws)
	if errC != nil {
		t.Fatalf("Materialization C failed: %v", errC)
	}
	if respC == nil {
		t.Fatalf("expected non-nil response for C")
	}

	// When all complete, lock registry must be 0
	if count := mat.ActiveLockCount(); count != 0 {
		t.Errorf("expected 0 active locks after all completed, got %d", count)
	}
}

// TEST E: ALL ENTRY POINTS SHARE THE SAME LOCK
// Proves that direct Materialize, ReconcileAndMaterialize, and Rematerialize all
// serialize on the same lock and cannot bypass each other.
func TestMaterializer_Concurrency_AllEntryPointsShareLock(t *testing.T) {
	ctx := context.Background()
	store := state.NewMemoryStore()
	ws := "ws-entrypoints"

	_ = store.SaveResources(ctx, ws, []state.Resource{{WorkspaceID: ws, Identity: state.ResourceIdentity{Provider: "k8s", ResourceType: "pod", ProviderID: "p-entry"}}})

	blockLoadState := make(chan struct{})
	releaseLoadState := make(chan struct{})

	loader := &mockCoreLoader{
		onLoadState: func(ctx context.Context, req *corev1.LoadStateRequest) error {
			select {
			case <-blockLoadState:
			default:
				close(blockLoadState)
				<-releaseLoadState
			}
			return nil
		},
	}

	mat := state.NewMaterializer(store, loader, nil)
	reconciler := state.NewReconciler(store, nil, mat, nil)

	// 1. Start direct Materialize call, pause it
	go func() {
		_, _ = mat.Materialize(ctx, ws)
	}()
	<-blockLoadState

	// 2. Call reconciler.Rematerialize: MUST BLOCK because Materialize holds the lock
	rematStarted := make(chan struct{})
	rematDone := make(chan error, 1)
	go func() {
		close(rematStarted)
		rematDone <- reconciler.Rematerialize(ctx, ws)
	}()
	<-rematStarted

	time.Sleep(20 * time.Millisecond)
	select {
	case <-rematDone:
		t.Fatalf("LOCK BYPASS: reconciler.Rematerialize did not wait for Materializer lock!")
	default:
		// Expected to be blocked
	}

	// 3. Call reconciler.ReconcileAndMaterialize: MUST ALSO BLOCK at materialization
	recMatDone := make(chan error, 1)
	go func() {
		batch := state.ObservationBatch{
			WorkspaceID: ws,
			Evidence: []*corev1.Evidence{
				newTestEvidence("ev-entry-2", "k8s", "pod", "p-entry", "CONTROL_PLANE_OBJECT", `{}`, time.Now().UTC()),
			},
		}
		_, err := reconciler.ReconcileAndMaterialize(ctx, batch)
		recMatDone <- err
	}()

	time.Sleep(20 * time.Millisecond)
	select {
	case <-recMatDone:
		t.Fatalf("LOCK BYPASS: ReconcileAndMaterialize did not wait for Materializer lock!")
	default:
		// Expected to be blocked
	}

	// Now release the initial lock
	close(releaseLoadState)

	// Both Rematerialize and ReconcileAndMaterialize must finish cleanly
	if err := <-rematDone; err != nil {
		t.Fatalf("Rematerialize failed: %v", err)
	}
	if err := <-recMatDone; err != nil {
		t.Fatalf("ReconcileAndMaterialize failed: %v", err)
	}
}


