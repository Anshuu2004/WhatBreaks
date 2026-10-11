package discovery_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "github.com/aaradhychinche-alt/WhatBreaks/gen/go/wb/core/v1"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/discovery"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/logging"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/state"
)

// mockCollector implements discovery.ClusterCollector for unit testing.
type mockCollector struct {
	mu                    sync.Mutex
	clusterID             string
	workspaceID           string
	pingErr               error
	collectErr            error
	collectDetailedResult *discovery.CollectionResult
	evidence              []*corev1.Evidence
	pingCount             int
	collectWait           chan struct{}
}

func (m *mockCollector) ClusterID() string {
	return m.clusterID
}

func (m *mockCollector) WorkspaceID() string {
	return m.workspaceID
}

func (m *mockCollector) Ping(ctx context.Context) error {
	m.mu.Lock()
	m.pingCount++
	m.mu.Unlock()
	return m.pingErr
}

func (m *mockCollector) Collect(ctx context.Context) ([]*corev1.Evidence, error) {
	if m.collectWait != nil {
		select {
		case <-m.collectWait:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if m.collectErr != nil {
		return nil, m.collectErr
	}
	return m.evidence, nil
}

func (m *mockCollector) CollectDetailed(ctx context.Context) (*discovery.CollectionResult, error) {
	if m.collectWait != nil {
		select {
		case <-m.collectWait:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if m.collectDetailedResult != nil {
		return m.collectDetailedResult, m.collectErr
	}
	if m.collectErr != nil {
		return nil, m.collectErr
	}
	return &discovery.CollectionResult{
		Evidence:         m.evidence,
		Status:           discovery.CollectionStatusComplete,
		Complete:         true,
		SuccessfulScopes: []string{"pods", "deployments"},
	}, nil
}

// mockReconciler implements state.Reconciler for unit testing.
type mockReconciler struct {
	mu                 sync.Mutex
	reconcileErr       error
	reconciled         bool
	batch              state.ObservationBatch
	rematerialized     bool
	rematerializeCount int
	rematerializeErr   error
	nilResultOnErr     bool
	onReconcile        func(ctx context.Context)
	onRematerialize    func(ctx context.Context)
}

func (m *mockReconciler) Reconcile(ctx context.Context, batch state.ObservationBatch) (*state.ReconciliationResult, error) {
	return m.ReconcileAndMaterialize(ctx, batch)
}

func (m *mockReconciler) ReconcileAndMaterialize(ctx context.Context, batch state.ObservationBatch) (*state.ReconciliationResult, error) {
	m.mu.Lock()
	m.reconciled = true
	m.batch = batch
	onRec := m.onReconcile
	recErr := m.reconcileErr
	nilOnErr := m.nilResultOnErr
	m.mu.Unlock()

	if onRec != nil {
		onRec(ctx)
	}

	if recErr != nil {
		// If nilOnErr is requested or error is NOT a materialization error, simulate failure before DB commit
		if nilOnErr || !strings.Contains(strings.ToLower(recErr.Error()), "materializ") {
			return nil, recErr
		}
		// Otherwise simulate successful DB commit followed by materialization failure
		return &state.ReconciliationResult{
			WorkspaceID:          batch.WorkspaceID,
			EvidenceCount:        len(batch.Evidence),
			ResourcesCreated:     1,
			ResourcesUpdated:     2,
			ResourcesTotal:       3,
			RelationshipsCreated: 1,
			RelationshipsTotal:   2,
			Duration:             5 * time.Millisecond,
		}, recErr
	}
	return &state.ReconciliationResult{
		WorkspaceID:          batch.WorkspaceID,
		EvidenceCount:        len(batch.Evidence),
		ResourcesCreated:     1,
		ResourcesUpdated:     2,
		ResourcesTotal:       3,
		RelationshipsCreated: 1,
		RelationshipsTotal:   2,
		Duration:             5 * time.Millisecond,
	}, nil
}

func (m *mockReconciler) Rematerialize(ctx context.Context, workspaceID string) error {
	m.mu.Lock()
	m.rematerialized = true
	m.rematerializeCount++
	onRemat := m.onRematerialize
	rematErr := m.rematerializeErr
	m.mu.Unlock()

	if onRemat != nil {
		onRemat(ctx)
	}
	return rematErr
}

func sampleEvidence(id string) *corev1.Evidence {
	return &corev1.Evidence{
		Id: id,
		Source: &corev1.EvidenceSource{
			Provider:  "kubernetes",
			Collector: "test-collector",
		},
		ObservedAt:      time.Now().UTC().Format(time.RFC3339Nano),
		ObservationType: "CONTROL_PLANE_OBJECT",
		Subject: &corev1.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "pod",
			ProviderId:   "test-cluster/default/pod-1",
		},
	}
}

// ---------------------------------------------------------------------------
// 1. Success Coordinator Execution
// ---------------------------------------------------------------------------

func TestCoordinator_Discover_Success(t *testing.T) {
	logger := logging.NewStandardLogger(nil, logging.LevelWarn)
	wsID := "00000000-0000-0000-0000-000000000001"
	clusterID := "prod-cluster"

	col := &mockCollector{
		clusterID:   clusterID,
		workspaceID: wsID,
		evidence:    []*corev1.Evidence{sampleEvidence("ev-1")},
	}
	rec := &mockReconciler{}

	coord := discovery.NewCoordinator(
		discovery.WithLogger(logger),
		discovery.WithCollector(col),
		discovery.WithReconciler(rec),
	)

	req := discovery.SyncRequest{
		WorkspaceID:   wsID,
		ClusterID:     clusterID,
		Trigger:       discovery.TriggerManual,
		CorrelationID: "run-test-1",
	}

	res, err := coord.Discover(context.Background(), req)
	if err != nil {
		t.Fatalf("expected successful discovery, got error: %v", err)
	}

	if res.Status != "COMPLETED" {
		t.Errorf("expected status COMPLETED, got %s", res.Status)
	}
	if res.Stage != discovery.StageCompleted {
		t.Errorf("expected stage COMPLETED, got %s", res.Stage)
	}
	if res.WorkspaceID != wsID {
		t.Errorf("expected workspace ID %s, got %s", wsID, res.WorkspaceID)
	}
	if res.ClusterID != clusterID {
		t.Errorf("expected cluster ID %s, got %s", clusterID, res.ClusterID)
	}
	if res.EvidenceCount != 1 {
		t.Errorf("expected evidence count 1, got %d", res.EvidenceCount)
	}
	if res.ResourcesTotal != 3 {
		t.Errorf("expected resources total 3, got %d", res.ResourcesTotal)
	}
	if res.RunID != "run-test-1" {
		t.Errorf("expected run ID run-test-1, got %s", res.RunID)
	}
	if !rec.reconciled {
		t.Errorf("expected reconciler to have been called")
	}
}

// ---------------------------------------------------------------------------
// 2. Default Cluster Resolution
// ---------------------------------------------------------------------------

func TestCoordinator_Discover_DefaultClusterResolution(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	col := &mockCollector{
		clusterID:   "single-cluster",
		workspaceID: wsID,
		evidence:    []*corev1.Evidence{sampleEvidence("ev-1")},
	}
	rec := &mockReconciler{}

	coord := discovery.NewCoordinator(
		discovery.WithCollector(col),
		discovery.WithReconciler(rec),
	)

	// Omit ClusterID in request
	req := discovery.SyncRequest{
		WorkspaceID: wsID,
		Trigger:     discovery.TriggerManual,
	}

	res, err := coord.Discover(context.Background(), req)
	if err != nil {
		t.Fatalf("expected auto-resolution of single cluster, got error: %v", err)
	}
	if res.ClusterID != "single-cluster" {
		t.Errorf("expected cluster ID 'single-cluster', got %s", res.ClusterID)
	}
}

// ---------------------------------------------------------------------------
// 3. Cluster Reachability Failure (Ping Check)
// ---------------------------------------------------------------------------

func TestCoordinator_Discover_ReachabilityFailure_PreservesState(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	col := &mockCollector{
		clusterID:   "unreachable-cluster",
		workspaceID: wsID,
		pingErr:     errors.New("connection refused to API server"),
	}
	rec := &mockReconciler{}

	coord := discovery.NewCoordinator(
		discovery.WithCollector(col),
		discovery.WithReconciler(rec),
	)

	req := discovery.SyncRequest{
		WorkspaceID: wsID,
		ClusterID:   "unreachable-cluster",
		Trigger:     discovery.TriggerManual,
	}

	res, err := coord.Discover(context.Background(), req)
	if err == nil {
		t.Fatalf("expected error on unreachable cluster, got nil")
	}
	if !errors.Is(err, discovery.ErrClusterUnreachable) {
		t.Errorf("expected ErrClusterUnreachable, got: %v", err)
	}
	if res == nil {
		t.Fatalf("expected structured result even on failure")
	}
	if res.Status != "FAILED" {
		t.Errorf("expected status FAILED, got %s", res.Status)
	}
	if res.Stage != discovery.StageReachability {
		t.Errorf("expected stage REACHABILITY, got %s", res.Stage)
	}
	// Verify reconciler was never invoked (zero state modified)
	if rec.reconciled {
		t.Errorf("PERSISTENCE VIOLATION: reconciler was invoked after ping failure")
	}
}

// ---------------------------------------------------------------------------
// 4. Collection Failure
// ---------------------------------------------------------------------------

func TestCoordinator_Discover_CollectionFailure_PreservesState(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	col := &mockCollector{
		clusterID:   "failing-collector",
		workspaceID: wsID,
		collectErr:  errors.New("timed out reading pods stream"),
	}
	rec := &mockReconciler{}

	coord := discovery.NewCoordinator(
		discovery.WithCollector(col),
		discovery.WithReconciler(rec),
	)

	req := discovery.SyncRequest{
		WorkspaceID: wsID,
		ClusterID:   "failing-collector",
		Trigger:     discovery.TriggerManual,
	}

	res, err := coord.Discover(context.Background(), req)
	if err == nil {
		t.Fatalf("expected error on collection failure, got nil")
	}
	if !errors.Is(err, discovery.ErrCollectionFailed) {
		t.Errorf("expected ErrCollectionFailed, got: %v", err)
	}
	if res.Status != "FAILED" {
		t.Errorf("expected status FAILED, got %s", res.Status)
	}
	if res.Stage != discovery.StageCollection {
		t.Errorf("expected stage COLLECTION, got %s", res.Stage)
	}
	// Verify reconciler was never invoked
	if rec.reconciled {
		t.Errorf("PERSISTENCE VIOLATION: reconciler was invoked after collection failure")
	}
}

// ---------------------------------------------------------------------------
// 5. Reconciliation Failure
// ---------------------------------------------------------------------------

func TestCoordinator_Discover_ReconciliationFailure(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	col := &mockCollector{
		clusterID:   "test-cluster",
		workspaceID: wsID,
		evidence:    []*corev1.Evidence{sampleEvidence("ev-1")},
	}
	rec := &mockReconciler{
		reconcileErr: errors.New("database connection pool closed"),
	}

	coord := discovery.NewCoordinator(
		discovery.WithCollector(col),
		discovery.WithReconciler(rec),
	)

	req := discovery.SyncRequest{
		WorkspaceID: wsID,
		ClusterID:   "test-cluster",
		Trigger:     discovery.TriggerManual,
	}

	res, err := coord.Discover(context.Background(), req)
	if err == nil {
		t.Fatalf("expected error on reconciliation failure, got nil")
	}
	if !errors.Is(err, discovery.ErrReconciliationFailed) {
		t.Errorf("expected ErrReconciliationFailed, got: %v", err)
	}
	if res.Status != "FAILED" {
		t.Errorf("expected status FAILED, got %s", res.Status)
	}
	if res.Stage != discovery.StageReconciliation {
		t.Errorf("expected stage RECONCILIATION, got %s", res.Stage)
	}
}

// ---------------------------------------------------------------------------
// 6. Materialization Failure
// ---------------------------------------------------------------------------

func TestCoordinator_Discover_MaterializationFailure(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	col := &mockCollector{
		clusterID:   "test-cluster",
		workspaceID: wsID,
		evidence:    []*corev1.Evidence{sampleEvidence("ev-1")},
	}
	rec := &mockReconciler{
		reconcileErr:     fmt.Errorf("materializer failed to load state: gRPC unavailable"),
		rematerializeErr: fmt.Errorf("materializer failed to load state: gRPC unavailable"),
	}

	coord := discovery.NewCoordinator(
		discovery.WithCollector(col),
		discovery.WithReconciler(rec),
	)

	req := discovery.SyncRequest{
		WorkspaceID: wsID,
		ClusterID:   "test-cluster",
		Trigger:     discovery.TriggerManual,
	}

	res, err := coord.Discover(context.Background(), req)
	if err == nil {
		t.Fatalf("expected error on materialization failure, got nil")
	}
	if !errors.Is(err, discovery.ErrMaterializationFailed) {
		t.Errorf("expected ErrMaterializationFailed, got: %v", err)
	}
	if res.Status != "FAILED" {
		t.Errorf("expected status FAILED, got %s", res.Status)
	}
	if res.Stage != discovery.StageMaterialization {
		t.Errorf("expected stage MATERIALIZATION, got %s", res.Stage)
	}
}

// ---------------------------------------------------------------------------
// 7. Context Cancellation
// ---------------------------------------------------------------------------

func TestCoordinator_Discover_ContextCancellation(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	waitCh := make(chan struct{})
	col := &mockCollector{
		clusterID:   "test-cluster",
		workspaceID: wsID,
		collectWait: waitCh,
	}
	rec := &mockReconciler{}

	coord := discovery.NewCoordinator(
		discovery.WithCollector(col),
		discovery.WithReconciler(rec),
	)

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after brief delay
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	req := discovery.SyncRequest{
		WorkspaceID: wsID,
		ClusterID:   "test-cluster",
		Trigger:     discovery.TriggerManual,
	}

	_, err := coord.Discover(ctx, req)
	if err == nil {
		t.Fatalf("expected context cancellation error, got nil")
	}
	if !errors.Is(err, context.Canceled) && !errors.Is(err, discovery.ErrCollectionFailed) {
		t.Errorf("expected cancellation-related error, got: %v", err)
	}
	if rec.reconciled {
		t.Errorf("reconciler should not have been called when collection was cancelled")
	}
}

// ---------------------------------------------------------------------------
// 8. Same-Cluster Concurrency Guard (Overlapping runs blocked)
// ---------------------------------------------------------------------------

func TestCoordinator_Discover_SameCluster_BlocksOverlap(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	waitCh := make(chan struct{})
	col := &mockCollector{
		clusterID:   "locked-cluster",
		workspaceID: wsID,
		collectWait: waitCh,
	}
	rec := &mockReconciler{}

	coord := discovery.NewCoordinator(
		discovery.WithCollector(col),
		discovery.WithReconciler(rec),
	)

	req := discovery.SyncRequest{
		WorkspaceID: wsID,
		ClusterID:   "locked-cluster",
		Trigger:     discovery.TriggerManual,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = coord.Discover(context.Background(), req)
	}()

	// Wait briefly for first run to acquire cluster lock
	time.Sleep(30 * time.Millisecond)

	// Second concurrent attempt for SAME cluster must fail with ErrDiscoveryInProgress
	_, err2 := coord.Discover(context.Background(), req)
	if err2 == nil {
		t.Fatalf("expected ErrDiscoveryInProgress for concurrent run on same cluster, got nil")
	}
	if !errors.Is(err2, discovery.ErrDiscoveryInProgress) {
		t.Errorf("expected ErrDiscoveryInProgress, got: %v", err2)
	}

	// Release first run
	close(waitCh)
	wg.Wait()

	// Third attempt after completion succeeds
	res3, err3 := coord.Discover(context.Background(), req)
	if err3 != nil {
		t.Fatalf("expected clean run after previous run settled, got error: %v", err3)
	}
	if res3.Status != "COMPLETED" {
		t.Errorf("expected COMPLETED, got %s", res3.Status)
	}
}

// ---------------------------------------------------------------------------
// 9. Different-Cluster Concurrency (Independent clusters do NOT block)
// ---------------------------------------------------------------------------

func TestCoordinator_Discover_DifferentClusters_ExecuteConcurrently(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	waitChA := make(chan struct{})
	waitChB := make(chan struct{})

	colA := &mockCollector{
		clusterID:   "cluster-a",
		workspaceID: wsID,
		collectWait: waitChA,
	}
	colB := &mockCollector{
		clusterID:   "cluster-b",
		workspaceID: wsID,
		collectWait: waitChB,
	}
	rec := &mockReconciler{}

	coord := discovery.NewCoordinator(
		discovery.WithCollectors(colA, colB),
		discovery.WithReconciler(rec),
	)

	var wg sync.WaitGroup
	var errA, errB error
	var resA, resB *discovery.Result

	// Start Cluster A
	wg.Add(1)
	go func() {
		defer wg.Done()
		resA, errA = coord.Discover(context.Background(), discovery.SyncRequest{
			WorkspaceID: wsID,
			ClusterID:   "cluster-a",
		})
	}()

	// Wait briefly for Cluster A to start
	time.Sleep(20 * time.Millisecond)

	// Start Cluster B while Cluster A is still blocked in collection
	wg.Add(1)
	go func() {
		defer wg.Done()
		resB, errB = coord.Discover(context.Background(), discovery.SyncRequest{
			WorkspaceID: wsID,
			ClusterID:   "cluster-b",
		})
	}()

	// Wait briefly then unblock both
	time.Sleep(20 * time.Millisecond)
	close(waitChA)
	close(waitChB)
	wg.Wait()

	if errA != nil {
		t.Errorf("Cluster A failed: %v", errA)
	}
	if errB != nil {
		t.Errorf("Cluster B failed (unexpected serialization or interference): %v", errB)
	}
	if resA == nil || resA.Status != "COMPLETED" {
		t.Errorf("Cluster A result incomplete: %+v", resA)
	}
	if resB == nil || resB.Status != "COMPLETED" {
		t.Errorf("Cluster B result incomplete: %+v", resB)
	}
}

// ---------------------------------------------------------------------------
// 10. Workspace / Cluster Binding Validation
// ---------------------------------------------------------------------------

func TestCoordinator_Discover_WorkspaceBindingMismatch(t *testing.T) {
	col := &mockCollector{
		clusterID:   "bound-cluster",
		workspaceID: "00000000-0000-0000-0000-000000000001",
	}
	rec := &mockReconciler{}

	coord := discovery.NewCoordinator(
		discovery.WithCollector(col),
		discovery.WithReconciler(rec),
	)

	// Attempt sync with different workspace
	req := discovery.SyncRequest{
		WorkspaceID: "00000000-0000-0000-0000-000000000002",
		ClusterID:   "bound-cluster",
	}

	_, err := coord.Discover(context.Background(), req)
	if err == nil {
		t.Fatalf("expected error on workspace mismatch, got nil")
	}
	if !errors.Is(err, discovery.ErrWorkspaceMismatch) {
		t.Errorf("expected ErrWorkspaceMismatch, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// 11. Configuration Guards
// ---------------------------------------------------------------------------

func TestCoordinator_Discover_MissingDependencies(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"

	// 1. Missing workspace ID
	coord1 := discovery.NewCoordinator()
	_, err1 := coord1.Discover(context.Background(), discovery.SyncRequest{})
	if !errors.Is(err1, discovery.ErrWorkspaceRequired) {
		t.Errorf("expected ErrWorkspaceRequired, got: %v", err1)
	}

	// 2. No collectors registered
	_, err2 := coord1.Discover(context.Background(), discovery.SyncRequest{WorkspaceID: wsID})
	if !errors.Is(err2, discovery.ErrCollectorUnavailable) {
		t.Errorf("expected ErrCollectorUnavailable, got: %v", err2)
	}

	// 3. Unknown cluster requested
	col := &mockCollector{clusterID: "cluster-1", workspaceID: wsID}
	coord3 := discovery.NewCoordinator(discovery.WithCollector(col))
	_, err3 := coord3.Discover(context.Background(), discovery.SyncRequest{
		WorkspaceID: wsID,
		ClusterID:   "cluster-unknown",
	})
	if !errors.Is(err3, discovery.ErrCollectorNotFound) {
		t.Errorf("expected ErrCollectorNotFound, got: %v", err3)
	}

	// 4. Missing reconciler
	_, err4 := coord3.Discover(context.Background(), discovery.SyncRequest{
		WorkspaceID: wsID,
		ClusterID:   "cluster-1",
	})
	if !errors.Is(err4, discovery.ErrReconcilerUnavailable) {
		t.Errorf("expected ErrReconcilerUnavailable, got: %v", err4)
	}

	// 5. Multiple clusters registered, cluster ID omitted in request
	col2 := &mockCollector{clusterID: "cluster-2", workspaceID: wsID}
	rec := &mockReconciler{}
	coord5 := discovery.NewCoordinator(
		discovery.WithCollectors(col, col2),
		discovery.WithReconciler(rec),
	)
	_, err5 := coord5.Discover(context.Background(), discovery.SyncRequest{
		WorkspaceID: wsID,
	})
	if !errors.Is(err5, discovery.ErrClusterRequired) {
		t.Errorf("expected ErrClusterRequired when multiple clusters registered, got: %v", err5)
	}
}

// ---------------------------------------------------------------------------
// 12. Phase 4A.1 Collection Completeness Contract Tests
// ---------------------------------------------------------------------------

func TestCoordinator_Completeness_CompleteCollection_ProceedsToReconciliation(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	col := &mockCollector{
		clusterID:   "cluster-complete",
		workspaceID: wsID,
		collectDetailedResult: &discovery.CollectionResult{
			Evidence:         []*corev1.Evidence{sampleEvidence("ev-1")},
			Status:           discovery.CollectionStatusComplete,
			Complete:         true,
			SuccessfulScopes: []string{"pods", "deployments", "services"},
		},
	}
	rec := &mockReconciler{}

	coord := discovery.NewCoordinator(
		discovery.WithCollector(col),
		discovery.WithReconciler(rec),
	)

	res, err := coord.Discover(context.Background(), discovery.SyncRequest{
		WorkspaceID: wsID,
		ClusterID:   "cluster-complete",
	})
	if err != nil {
		t.Fatalf("Discover failed: %v", err)
	}

	if res.Status != "COMPLETED" {
		t.Errorf("expected status COMPLETED, got %s", res.Status)
	}
	if !res.DatabaseCommitted {
		t.Errorf("expected DatabaseCommitted to be true")
	}
	if !rec.reconciled {
		t.Errorf("expected reconciliation to be invoked")
	}
	if rec.batch.Scope == nil || !rec.batch.Scope.Complete {
		t.Errorf("expected batch.Scope.Complete to be true for complete collection, got %+v", rec.batch.Scope)
	}
}

func TestCoordinator_Completeness_RequiredScopeFailure_BlocksReconciliation(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	col := &mockCollector{
		clusterID:   "cluster-required-fail",
		workspaceID: wsID,
		collectDetailedResult: &discovery.CollectionResult{
			Evidence:         []*corev1.Evidence{sampleEvidence("ev-partial-pod")},
			Status:           discovery.CollectionStatusFailed,
			Complete:         false,
			RequiredFailures: []string{"deployments", "services"},
			ScopeErrors: []discovery.ScopeError{
				{Scope: "deployments", Resource: "Deployment", Error: "403 Forbidden", Required: true},
				{Scope: "services", Resource: "Service", Error: "500 Internal", Required: true},
			},
		},
	}
	rec := &mockReconciler{}

	coord := discovery.NewCoordinator(
		discovery.WithCollector(col),
		discovery.WithReconciler(rec),
	)

	res, err := coord.Discover(context.Background(), discovery.SyncRequest{
		WorkspaceID: wsID,
		ClusterID:   "cluster-required-fail",
	})
	if err == nil {
		t.Fatalf("expected error on required scope failure, got nil")
	}
	if !errors.Is(err, discovery.ErrRequiredScopeFailed) {
		t.Errorf("expected ErrRequiredScopeFailed, got: %v", err)
	}

	if res.Status != "FAILED" {
		t.Errorf("expected status FAILED, got %s", res.Status)
	}
	if res.Stage != discovery.StageCollection {
		t.Errorf("expected StageCollection, got %s", res.Stage)
	}
	if res.DatabaseCommitted {
		t.Errorf("expected DatabaseCommitted to be false when reconciliation is blocked")
	}

	// CRITICAL SAFETY REQUIREMENT: Persisted state intact, reconciler NEVER called
	if rec.reconciled {
		t.Fatalf("SAFETY VIOLATION: Reconciler was called despite required scope failure!")
	}
}

func TestCoordinator_Completeness_PartialCollection_ReconcilesAsIncomplete(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	col := &mockCollector{
		clusterID:   "cluster-optional-fail",
		workspaceID: wsID,
		collectDetailedResult: &discovery.CollectionResult{
			Evidence:         []*corev1.Evidence{sampleEvidence("ev-pod-1")},
			Status:           discovery.CollectionStatusPartial,
			Complete:         false,
			SuccessfulScopes: []string{"pods", "deployments", "services", "secrets", "configmaps", "replicasets"},
			OptionalFailures: []string{"ingresses", "nodes"},
			ScopeErrors: []discovery.ScopeError{
				{Scope: "ingresses", Resource: "Ingress", Error: "404 Not Found", Required: false},
			},
		},
	}
	rec := &mockReconciler{}

	coord := discovery.NewCoordinator(
		discovery.WithCollector(col),
		discovery.WithReconciler(rec),
	)

	res, err := coord.Discover(context.Background(), discovery.SyncRequest{
		WorkspaceID: wsID,
		ClusterID:   "cluster-optional-fail",
	})
	if err != nil {
		t.Fatalf("Discover failed on partial collection: %v", err)
	}

	if res.Status != "PARTIAL" {
		t.Errorf("expected status PARTIAL, got %s", res.Status)
	}
	if !res.DatabaseCommitted {
		t.Errorf("expected DatabaseCommitted to be true")
	}
	if !rec.reconciled {
		t.Errorf("expected reconciler to be called for optional failure")
	}
	if rec.batch.Scope == nil || rec.batch.Scope.Complete {
		t.Errorf("expected batch.Scope.Complete to be false for partial collection, got %+v", rec.batch.Scope)
	}
}

func TestCoordinator_Completeness_NonEmptyEvidenceDoesNotInferCompleteness(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	// 50 evidence items collected, but required scope "secrets" failed
	evidenceSlice := make([]*corev1.Evidence, 50)
	for i := 0; i < 50; i++ {
		evidenceSlice[i] = sampleEvidence(fmt.Sprintf("ev-%d", i))
	}

	col := &mockCollector{
		clusterID:   "cluster-big-partial",
		workspaceID: wsID,
		collectDetailedResult: &discovery.CollectionResult{
			Evidence:         evidenceSlice,
			Status:           discovery.CollectionStatusFailed,
			Complete:         false,
			RequiredFailures: []string{"secrets"},
		},
	}
	rec := &mockReconciler{}

	coord := discovery.NewCoordinator(
		discovery.WithCollector(col),
		discovery.WithReconciler(rec),
	)

	_, err := coord.Discover(context.Background(), discovery.SyncRequest{
		WorkspaceID: wsID,
		ClusterID:   "cluster-big-partial",
	})
	if !errors.Is(err, discovery.ErrRequiredScopeFailed) {
		t.Fatalf("expected ErrRequiredScopeFailed, got: %v", err)
	}

	if rec.reconciled {
		t.Fatalf("reconciler should NOT be called even with 50 evidence records if required scope failed")
	}
}

// ---------------------------------------------------------------------------
// 13. Phase 4A.1 Materialization Recovery Tests
// ---------------------------------------------------------------------------

func TestCoordinator_MaterializationRecovery_TransientFailureRecovers(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	col := &mockCollector{
		clusterID:   "cluster-mat-retry",
		workspaceID: wsID,
		evidence:    []*corev1.Evidence{sampleEvidence("ev-1")},
	}
	// Initial ReconcileAndMaterialize fails at materialization, but Rematerialize retry succeeds
	rec := &mockReconciler{
		reconcileErr:     errors.New("materialization failed: connection reset by peer"),
		rematerializeErr: nil, // Retry will succeed!
	}

	coord := discovery.NewCoordinator(
		discovery.WithCollector(col),
		discovery.WithReconciler(rec),
	)

	res, err := coord.Discover(context.Background(), discovery.SyncRequest{
		WorkspaceID: wsID,
		ClusterID:   "cluster-mat-retry",
	})
	if err != nil {
		t.Fatalf("expected Discover to recover via retry, got: %v", err)
	}

	if res.Status != "COMPLETED" {
		t.Errorf("expected status COMPLETED after recovery, got %s", res.Status)
	}
	if !res.DatabaseCommitted {
		t.Errorf("expected DatabaseCommitted to be true")
	}
	if rec.rematerializeCount == 0 {
		t.Errorf("expected Rematerialize retry to be attempted, count: %d", rec.rematerializeCount)
	}
}

func TestCoordinator_MaterializationRecovery_PersistentFailureReportsDatabaseCommitted(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	col := &mockCollector{
		clusterID:   "cluster-mat-fail",
		workspaceID: wsID,
		evidence:    []*corev1.Evidence{sampleEvidence("ev-1")},
	}
	// Both initial materialization and retries persistently fail
	rec := &mockReconciler{
		reconcileErr:     errors.New("materialization failed: core engine down"),
		rematerializeErr: errors.New("materialization failed: core engine down"),
	}

	coord := discovery.NewCoordinator(
		discovery.WithCollector(col),
		discovery.WithReconciler(rec),
	)

	res, err := coord.Discover(context.Background(), discovery.SyncRequest{
		WorkspaceID: wsID,
		ClusterID:   "cluster-mat-fail",
	})
	if err == nil {
		t.Fatalf("expected error on persistent materialization failure, got nil")
	}
	if !errors.Is(err, discovery.ErrMaterializationFailed) {
		t.Errorf("expected ErrMaterializationFailed, got: %v", err)
	}

	if res.Status != "FAILED" {
		t.Errorf("expected status FAILED, got %s", res.Status)
	}
	if res.Stage != discovery.StageMaterialization {
		t.Errorf("expected StageMaterialization, got %s", res.Stage)
	}
	// CRITICAL: DB state was committed before materialization failed!
	if !res.DatabaseCommitted {
		t.Errorf("expected DatabaseCommitted to be true when DB succeeded but materialization failed")
	}
	if rec.rematerializeCount < 2 {
		t.Errorf("expected at least 2 recovery retries, got %d", rec.rematerializeCount)
	}
}

func TestCoordinator_Rematerialize_Success(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	col := &mockCollector{clusterID: "cluster-1", workspaceID: wsID}
	rec := &mockReconciler{}

	coord := discovery.NewCoordinator(
		discovery.WithCollector(col),
		discovery.WithReconciler(rec),
	)

	if err := coord.Rematerialize(context.Background(), wsID); err != nil {
		t.Fatalf("Rematerialize failed: %v", err)
	}

	if !rec.rematerialized {
		t.Errorf("expected reconciler.Rematerialize to be invoked")
	}
}

func TestCoordinator_Rematerialize_Validations(t *testing.T) {
	// 1. Missing workspace
	coord1 := discovery.NewCoordinator()
	if err := coord1.Rematerialize(context.Background(), ""); !errors.Is(err, discovery.ErrWorkspaceRequired) {
		t.Errorf("expected ErrWorkspaceRequired, got: %v", err)
	}

	// 2. Missing reconciler
	if err := coord1.Rematerialize(context.Background(), "ws-123"); !errors.Is(err, discovery.ErrReconcilerUnavailable) {
		t.Errorf("expected ErrReconcilerUnavailable, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// 14. Phase 4A.1 Cancellation Tests
// ---------------------------------------------------------------------------

func TestCoordinator_Cancellation_DuringCollection(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	waitCh := make(chan struct{})
	col := &mockCollector{
		clusterID:   "cluster-cancel",
		workspaceID: wsID,
		collectWait: waitCh,
	}
	rec := &mockReconciler{}

	coord := discovery.NewCoordinator(
		discovery.WithCollector(col),
		discovery.WithReconciler(rec),
	)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	res, err := coord.Discover(ctx, discovery.SyncRequest{
		WorkspaceID: wsID,
		ClusterID:   "cluster-cancel",
	})

	if err == nil {
		t.Fatalf("expected context cancellation error, got nil")
	}
	if !errors.Is(err, context.Canceled) && !errors.Is(err, discovery.ErrCollectionFailed) {
		t.Errorf("expected context.Canceled or ErrCollectionFailed, got: %v", err)
	}
	if res == nil || res.Status != "FAILED" {
		t.Errorf("expected FAILED status on cancellation, got %+v", res)
	}
	if rec.reconciled {
		t.Errorf("reconciler should NOT be called after cancellation during collection")
	}
}

func TestCoordinator_Cancellation_BeforeCommit_PreReconciliation(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	col := &mockCollector{
		clusterID:   "cluster-cancel-pre-rec",
		workspaceID: wsID,
		evidence:    []*corev1.Evidence{sampleEvidence("ev-1")},
	}
	rec := &mockReconciler{}

	coord := discovery.NewCoordinator(
		discovery.WithCollector(col),
		discovery.WithReconciler(rec),
	)

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel before coordinator reaches reconciliation
	cancel()

	res, err := coord.Discover(ctx, discovery.SyncRequest{
		WorkspaceID: wsID,
		ClusterID:   "cluster-cancel-pre-rec",
	})

	if err == nil {
		t.Fatalf("expected context cancellation error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got: %v", err)
	}
	if res.DatabaseCommitted {
		t.Errorf("expected DatabaseCommitted to be false on pre-commit cancellation")
	}
	if rec.reconciled {
		t.Errorf("reconciler should NOT be called when context was cancelled before reconciliation")
	}
}

func TestCoordinator_Cancellation_BeforeCommit_DuringReconcile(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	col := &mockCollector{
		clusterID:   "cluster-cancel-during-rec",
		workspaceID: wsID,
		evidence:    []*corev1.Evidence{sampleEvidence("ev-1")},
	}
	// Reconciler fails with cancellation during database transaction (nil result)
	rec := &mockReconciler{
		reconcileErr:   context.Canceled,
		nilResultOnErr: true,
	}

	coord := discovery.NewCoordinator(
		discovery.WithCollector(col),
		discovery.WithReconciler(rec),
	)

	res, err := coord.Discover(context.Background(), discovery.SyncRequest{
		WorkspaceID: wsID,
		ClusterID:   "cluster-cancel-during-rec",
	})

	if err == nil {
		t.Fatalf("expected error on reconciliation cancellation, got nil")
	}
	if !errors.Is(err, discovery.ErrReconciliationFailed) {
		t.Errorf("expected ErrReconciliationFailed, got: %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected underlying context.Canceled in error chain, got: %v", err)
	}
	if res.DatabaseCommitted {
		t.Errorf("expected DatabaseCommitted to be false when cancellation occurs during DB transaction")
	}
	if res.Stage != discovery.StageReconciliation {
		t.Errorf("expected StageReconciliation, got %s", res.Stage)
	}
}

func TestCoordinator_Cancellation_ImmediatelyAfterCommit(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	col := &mockCollector{
		clusterID:   "cluster-cancel-post-commit",
		workspaceID: wsID,
		evidence:    []*corev1.Evidence{sampleEvidence("ev-1")},
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Reconciler simulates successful DB commit, but context is canceled immediately after commit during materialization
	rec := &mockReconciler{
		reconcileErr: fmt.Errorf("materialization failed: %w", context.Canceled),
		onReconcile: func(c context.Context) {
			// Cancel context right during/after commit
			cancel()
		},
	}

	coord := discovery.NewCoordinator(
		discovery.WithCollector(col),
		discovery.WithReconciler(rec),
	)

	res, err := coord.Discover(ctx, discovery.SyncRequest{
		WorkspaceID: wsID,
		ClusterID:   "cluster-cancel-post-commit",
	})

	if err == nil {
		t.Fatalf("expected error on post-commit materialization cancellation, got nil")
	}
	if !errors.Is(err, discovery.ErrMaterializationFailed) {
		t.Errorf("expected ErrMaterializationFailed, got: %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled in error chain, got: %v", err)
	}

	// CRITICAL INVARIANT: A post-commit cancellation is NOT reported as a database rollback!
	if !res.DatabaseCommitted {
		t.Fatalf("CRITICAL REGRESSION: DatabaseCommitted must be TRUE when DB committed before cancellation!")
	}
	if res.Stage != discovery.StageMaterialization {
		t.Errorf("expected StageMaterialization, got %s", res.Stage)
	}

	// CRITICAL INVARIANT: Committed counts remain available after materialization failure
	if res.ResourcesCreated != 1 || res.ResourcesTotal != 3 {
		t.Errorf("committed resource counts lost! created: %d, total: %d", res.ResourcesCreated, res.ResourcesTotal)
	}
	if res.RelationshipsCreated != 1 || res.RelationshipsTotal != 2 {
		t.Errorf("committed relationship counts lost! created: %d, total: %d", res.RelationshipsCreated, res.RelationshipsTotal)
	}

	// CRITICAL INVARIANT: Cancellation stops retries safely (no unbounded retries)
	if rec.rematerializeCount > 0 {
		t.Errorf("expected 0 rematerialize retries when context is cancelled, got %d", rec.rematerializeCount)
	}
}

func TestCoordinator_Cancellation_DuringMaterializationRetry(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	col := &mockCollector{
		clusterID:   "cluster-cancel-retry",
		workspaceID: wsID,
		evidence:    []*corev1.Evidence{sampleEvidence("ev-1")},
	}

	ctx, cancel := context.WithCancel(context.Background())

	rec := &mockReconciler{
		reconcileErr:     errors.New("materialization failed: transient network issue"),
		rematerializeErr: errors.New("materialization failed: still failing"),
		onRematerialize: func(c context.Context) {
			// Cancel context during retry
			cancel()
		},
	}

	coord := discovery.NewCoordinator(
		discovery.WithCollector(col),
		discovery.WithReconciler(rec),
	)

	res, err := coord.Discover(ctx, discovery.SyncRequest{
		WorkspaceID: wsID,
		ClusterID:   "cluster-cancel-retry",
	})

	if err == nil {
		t.Fatalf("expected error on cancelled retry, got nil")
	}
	if !errors.Is(err, discovery.ErrMaterializationFailed) {
		t.Errorf("expected ErrMaterializationFailed, got: %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled in error chain, got: %v", err)
	}

	if !res.DatabaseCommitted {
		t.Errorf("expected DatabaseCommitted to be true")
	}
	if res.Stage != discovery.StageMaterialization {
		t.Errorf("expected StageMaterialization, got %s", res.Stage)
	}

	// Retries must not exceed 1 because cancellation stops further retries
	if rec.rematerializeCount > 1 {
		t.Errorf("expected retry loop to halt after cancellation, count was %d", rec.rematerializeCount)
	}
}

func TestCoordinator_MaterializationRecovery_RequiresCoreConfirmation(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	col := &mockCollector{
		clusterID:   "cluster-no-false-success",
		workspaceID: wsID,
		evidence:    []*corev1.Evidence{sampleEvidence("ev-1")},
	}
	// Initial materialization fails, retry returns an error from Rust Core Engine
	rec := &mockReconciler{
		reconcileErr:     errors.New("materialization failed: initial timeout"),
		rematerializeErr: errors.New("grpc: core engine rejected snapshot with code 13"),
	}

	coord := discovery.NewCoordinator(
		discovery.WithCollector(col),
		discovery.WithReconciler(rec),
	)

	res, err := coord.Discover(context.Background(), discovery.SyncRequest{
		WorkspaceID: wsID,
		ClusterID:   "cluster-no-false-success",
	})

	if err == nil {
		t.Fatalf("expected error, got nil (must NOT falsely report success!)")
	}
	if !errors.Is(err, discovery.ErrMaterializationFailed) {
		t.Errorf("expected ErrMaterializationFailed, got: %v", err)
	}
	if res.Status != "FAILED" {
		t.Errorf("expected FAILED status when Core Engine fails to load, got %s", res.Status)
	}
	if !res.DatabaseCommitted {
		t.Errorf("expected DatabaseCommitted to be true")
	}
}


