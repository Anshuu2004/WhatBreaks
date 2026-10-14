package state_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	corev1 "github.com/aaradhychinche-alt/WhatBreaks/gen/go/wb/core/v1"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/database"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/logging"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/state"
)

// helper to initialize Postgres store if test db is accessible
func initTestPostgresStore(t *testing.T) (*state.PostgresStore, database.DB, func()) {
	t.Helper()
	cfg := getTestDatabaseConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	logger := logging.NewJSONLogger(nil, logging.LevelDebug, "pg-authority-test")
	db, err := database.New(ctx, cfg, database.WithLogger(logger))
	if err != nil {
		t.Skipf("Failed to connect to postgres: %v", err)
		return nil, nil, nil
	}
	if err := db.Ping(ctx); err != nil {
		db.Close()
		t.Skipf("Postgres Ping failed: %v", err)
		return nil, nil, nil
	}
	if err := state.EnsureSchema(ctx, db); err != nil {
		db.Close()
		t.Fatalf("EnsureSchema failed: %v", err)
	}

	store := state.NewPostgresStore(db)
	cleanup := func() {
		db.Close()
	}
	return store, db, cleanup
}

func forEachStore(t *testing.T, testFn func(t *testing.T, s state.Store, isPostgres bool, db database.DB)) {
	t.Run("MemoryStore", func(t *testing.T) {
		memStore := state.NewMemoryStore()
		testFn(t, memStore, false, nil)
	})

	t.Run("PostgresStore", func(t *testing.T) {
		pgStore, db, cleanup := initTestPostgresStore(t)
		if pgStore == nil {
			return
		}
		defer cleanup()
		testFn(t, pgStore, true, db)
	})
}

// -----------------------------------------------------------------------------
// 1. Concurrent first-run initialization
// -----------------------------------------------------------------------------
func TestAuthority_ConcurrentFirstRunInitialization(t *testing.T) {
	forEachStore(t, func(t *testing.T, s state.Store, isPostgres bool, db database.DB) {
		ctx := context.Background()
		ws := fmt.Sprintf("ws-init-%d", time.Now().UnixNano())
		cluster := "cluster-main"
		defer s.DeleteWorkspaceState(ctx, ws)

		const concurrency = 8
		startGate := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(concurrency)

		errs := make([]error, concurrency)
		results := make([]*state.AuthoritativeBatchResult, concurrency)

		t0 := time.Now().UTC()

		for i := 0; i < concurrency; i++ {
			idx := i
			go func() {
				defer wg.Done()
				<-startGate

				// Each run has a slightly advancing or identical sampling timestamp
				samplingTime := t0.Add(time.Duration(idx) * time.Millisecond)
				runID := fmt.Sprintf("run-init-%d", idx)

				res := state.Resource{
					WorkspaceID: ws,
					Identity: state.ResourceIdentity{
						Provider:     "kubernetes",
						ResourceType: "pod",
						ProviderID:   fmt.Sprintf("pod-%d", idx),
					},
					FirstObservedAt: samplingTime,
					LastObservedAt:  samplingTime,
				}

				r, err := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
					WorkspaceID:       ws,
					ClusterID:         cluster,
					RunID:             runID,
					SamplingStartedAt: samplingTime,
					Resources:         []state.Resource{res},
				})
				errs[idx] = err
				results[idx] = r
			}()
		}

		// Release all goroutines simultaneously
		close(startGate)
		wg.Wait()

		// Verify no unhandled SQL errors or deadlocks
		var successfulRuns int
		for i := 0; i < concurrency; i++ {
			if errs[i] == nil {
				successfulRuns++
			} else if !errors.Is(errs[i], state.ErrStaleCollectionRunIgnored) {
				t.Fatalf("unexpected error in concurrent init run %d: %v", i, errs[i])
			}
		}

		if successfulRuns == 0 {
			t.Fatalf("expected at least one successful run, got 0")
		}

		auth, err := s.GetWorkspaceAuthority(ctx, ws, cluster)
		if err != nil {
			t.Fatalf("failed to get authority: %v", err)
		}
		if auth == nil {
			t.Fatalf("expected authority to exist after init")
		}
		if auth.LastGeneration < 1 {
			t.Errorf("expected LastGeneration >= 1, got %d", auth.LastGeneration)
		}
	})
}

// -----------------------------------------------------------------------------
// 2. Concurrent runs for the same workspace and cluster
// -----------------------------------------------------------------------------
func TestAuthority_ConcurrentRunsSameWorkspaceCluster(t *testing.T) {
	forEachStore(t, func(t *testing.T, s state.Store, isPostgres bool, db database.DB) {
		ctx := context.Background()
		ws := fmt.Sprintf("ws-same-cluster-%d", time.Now().UnixNano())
		cluster := "cluster-main"
		defer s.DeleteWorkspaceState(ctx, ws)

		t0 := time.Now().UTC()
		t1 := t0.Add(1 * time.Minute)

		res1 := state.Resource{
			WorkspaceID: ws,
			Identity: state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "pod",
				ProviderID:   "pod-run-1",
			},
			FirstObservedAt: t0,
			LastObservedAt:  t0,
		}

		res2 := state.Resource{
			WorkspaceID: ws,
			Identity: state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "pod",
				ProviderID:   "pod-run-2",
			},
			FirstObservedAt: t1,
			LastObservedAt:  t1,
		}

		// Sequential / serialized concurrent runs
		r1, err1 := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-1",
			SamplingStartedAt: t0,
			Resources:         []state.Resource{res1},
		})
		if err1 != nil {
			t.Fatalf("Run 1 failed: %v", err1)
		}
		if r1.AssignedGeneration != 1 {
			t.Errorf("expected Run 1 to receive Generation 1, got %d", r1.AssignedGeneration)
		}

		r2, err2 := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-2",
			SamplingStartedAt: t1,
			Resources:         []state.Resource{res2},
		})
		if err2 != nil {
			t.Fatalf("Run 2 failed: %v", err2)
		}
		if r2.AssignedGeneration != 2 {
			t.Errorf("expected Run 2 to receive Generation 2, got %d", r2.AssignedGeneration)
		}

		auth, err := s.GetWorkspaceAuthority(ctx, ws, cluster)
		if err != nil || auth == nil {
			t.Fatalf("failed to retrieve authority: %v", err)
		}
		if auth.LastGeneration != 2 {
			t.Errorf("expected final LastGeneration 2, got %d", auth.LastGeneration)
		}
		if auth.AuthoritativeRunID != "run-2" {
			t.Errorf("expected authoritative run-2, got %s", auth.AuthoritativeRunID)
		}
	})
}

// -----------------------------------------------------------------------------
// 3. Older runs finishing after newer runs
// -----------------------------------------------------------------------------
func TestAuthority_OlderRunsFinishingAfterNewerRuns(t *testing.T) {
	forEachStore(t, func(t *testing.T, s state.Store, isPostgres bool, db database.DB) {
		ctx := context.Background()
		ws := fmt.Sprintf("ws-older-after-newer-%d", time.Now().UnixNano())
		cluster := "cluster-main"
		defer s.DeleteWorkspaceState(ctx, ws)

		t0 := time.Now().UTC()
		t1 := t0.Add(10 * time.Minute)

		// Run 2 is newer and finishes FIRST
		res2 := state.Resource{
			WorkspaceID: ws,
			Identity: state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "pod",
				ProviderID:   "pod-newer",
			},
			FirstObservedAt: t1,
			LastObservedAt:  t1,
		}

		r2, err := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-newer",
			SamplingStartedAt: t1,
			Resources:         []state.Resource{res2},
		})
		if err != nil {
			t.Fatalf("newer run failed: %v", err)
		}
		if r2.AssignedGeneration != 1 {
			t.Errorf("expected newer run generation 1, got %d", r2.AssignedGeneration)
		}

		// Run 1 is older and finishes AFTER Run 2
		res1 := state.Resource{
			WorkspaceID: ws,
			Identity: state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "pod",
				ProviderID:   "pod-older",
			},
			FirstObservedAt: t0,
			LastObservedAt:  t0,
		}

		_, errOld := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-older",
			SamplingStartedAt: t0,
			Resources:         []state.Resource{res1},
		})

		// Objective 6 & 7: Reject stale runs
		if !errors.Is(errOld, state.ErrStaleCollectionRunIgnored) {
			t.Fatalf("expected ErrStaleCollectionRunIgnored for older run, got: %v", errOld)
		}

		// Verify authoritative state remains at Run 2
		auth, err := s.GetWorkspaceAuthority(ctx, ws, cluster)
		if err != nil || auth == nil {
			t.Fatalf("failed to get authority: %v", err)
		}
		if auth.AuthoritativeRunID != "run-newer" {
			t.Errorf("expected authoritative run to remain 'run-newer', got %s", auth.AuthoritativeRunID)
		}

		// Verify older resource was NOT written
		resources, err := s.ListResources(ctx, ws)
		if err != nil {
			t.Fatalf("failed to list resources: %v", err)
		}
		if len(resources) != 1 {
			t.Fatalf("expected exactly 1 resource, got %d", len(resources))
		}
		if resources[0].Identity.ProviderID != "pod-newer" {
			t.Errorf("expected only 'pod-newer', found %s", resources[0].Identity.ProviderID)
		}
	})
}

// -----------------------------------------------------------------------------
// 4. Equal sampling timestamps with different generations
// -----------------------------------------------------------------------------
func TestAuthority_EqualSamplingTimestampsDifferentGenerations(t *testing.T) {
	forEachStore(t, func(t *testing.T, s state.Store, isPostgres bool, db database.DB) {
		ctx := context.Background()
		ws := fmt.Sprintf("ws-equal-sampling-%d", time.Now().UnixNano())
		cluster := "cluster-main"
		defer s.DeleteWorkspaceState(ctx, ws)

		tSame := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)

		// Run 1 establishes authority at tSame, receives generation 1
		res1 := state.Resource{
			WorkspaceID: ws,
			Identity: state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "pod",
				ProviderID:   "pod-gen1",
			},
			FirstObservedAt: tSame,
			LastObservedAt:  tSame,
		}

		r1, err := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-1",
			SamplingStartedAt: tSame,
			Generation:        1,
			Resources:         []state.Resource{res1},
		})
		if err != nil {
			t.Fatalf("Run 1 failed: %v", err)
		}
		if r1.AssignedGeneration != 1 {
			t.Errorf("expected generation 1, got %d", r1.AssignedGeneration)
		}

		// Run 2 has EQUAL sampling timestamp, but older/equal candidate generation (<= 1)
		res2 := state.Resource{
			WorkspaceID: ws,
			Identity: state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "pod",
				ProviderID:   "pod-gen-older",
			},
			FirstObservedAt: tSame,
			LastObservedAt:  tSame,
		}

		_, errStale := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-stale-equal",
			SamplingStartedAt: tSame,
			Generation:        1, // Candidate gen equal to current gen
			Resources:         []state.Resource{res2},
		})

		// Objective 7: Equal sampling timestamps must not allow an older generation to win.
		if !errors.Is(errStale, state.ErrStaleCollectionRunIgnored) {
			t.Fatalf("expected ErrStaleCollectionRunIgnored for equal timestamp with <= generation, got %v", errStale)
		}

		// Check authority was not taken over
		auth, _ := s.GetWorkspaceAuthority(ctx, ws, cluster)
		if auth.AuthoritativeRunID != "run-1" {
			t.Errorf("expected authoritative run-1, got %s", auth.AuthoritativeRunID)
		}
	})
}

// -----------------------------------------------------------------------------
// 5. Rejection before authoritative mutations
// -----------------------------------------------------------------------------
func TestAuthority_RejectionBeforeAuthoritativeMutations(t *testing.T) {
	forEachStore(t, func(t *testing.T, s state.Store, isPostgres bool, db database.DB) {
		ctx := context.Background()
		ws := fmt.Sprintf("ws-rejection-before-mutation-%d", time.Now().UnixNano())
		cluster := "cluster-main"
		defer s.DeleteWorkspaceState(ctx, ws)

		tAuth := time.Now().UTC()
		tStale := tAuth.Add(-1 * time.Hour)

		// Authoritative Run
		authRes := state.Resource{
			WorkspaceID: ws,
			Identity: state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "service",
				ProviderID:   "svc-authoritative",
			},
			FirstObservedAt: tAuth,
			LastObservedAt:  tAuth,
		}

		_, err := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-auth",
			SamplingStartedAt: tAuth,
			Resources:         []state.Resource{authRes},
		})
		if err != nil {
			t.Fatalf("authoritative run failed: %v", err)
		}

		// Stale run attempting to write resources, relationships, and provenance
		staleRes := state.Resource{
			WorkspaceID: ws,
			Identity: state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "service",
				ProviderID:   "svc-stale",
			},
			FirstObservedAt: tStale,
			LastObservedAt:  tStale,
		}

		relKey := state.RelationshipKey{
			Source: state.ResourceIdentity{Provider: "kubernetes", ResourceType: "pod", ProviderID: "pod-1"},
			Target: state.ResourceIdentity{Provider: "kubernetes", ResourceType: "service", ProviderID: "svc-1"},
			Kind:   "DEPENDS_ON",
		}
		staleRel := state.Relationship{
			WorkspaceID:     ws,
			Source:          relKey.Source,
			Target:          relKey.Target,
			Kind:            relKey.Kind,
			Category:        "Dependency",
			FirstObservedAt: tStale,
			LastObservedAt:  tStale,
		}

		staleProv := state.ProvenanceAssociation{
			WorkspaceID:  ws,
			Relationship: relKey,
			EvidenceID:   "ev-stale-001",
		}

		staleEv := state.Evidence{
			WorkspaceID:     ws,
			ID:              "ev-stale-001",
			Source:          state.EvidenceSource{Provider: "kubernetes", Collector: "collector-1"},
			Subject:         relKey.Source,
			ObservedAt:      tStale,
			ObservationType: "TEST",
			Data:            []byte(`{}`),
		}

		_, errStale := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-stale",
			SamplingStartedAt: tStale,
			Resources:         []state.Resource{staleRes},
			Evidence:          []state.Evidence{staleEv},
			Relationships:     []state.Relationship{staleRel},
			Provenance:        []state.ProvenanceAssociation{staleProv},
		})

		if !errors.Is(errStale, state.ErrStaleCollectionRunIgnored) {
			t.Fatalf("expected ErrStaleCollectionRunIgnored, got %v", errStale)
		}

		// Objective 6: Verify NONE of the stale run's resources, evidence, relationships or provenance were written
		resources, _ := s.ListResources(ctx, ws)
		if len(resources) != 1 || resources[0].Identity.ProviderID != "svc-authoritative" {
			t.Errorf("resources were modified by stale run: %+v", resources)
		}

		rels, _ := s.ListRelationships(ctx, ws)
		if len(rels) != 0 {
			t.Errorf("relationships were inserted by stale run: %+v", rels)
		}

		evs, _ := s.ListEvidence(ctx, ws)
		if len(evs) != 0 {
			t.Errorf("evidence was inserted by stale run: %+v", evs)
		}

		provs, _ := s.GetProvenanceForRelationship(ctx, ws, relKey)
		if len(provs) != 0 {
			t.Errorf("provenance was inserted by stale run: %+v", provs)
		}
	})
}

// -----------------------------------------------------------------------------
// 6. Failure halfway through reconciliation and complete rollback
// -----------------------------------------------------------------------------
func TestAuthority_FailureHalfwayThroughRollback(t *testing.T) {
	forEachStore(t, func(t *testing.T, s state.Store, isPostgres bool, db database.DB) {
		ctx := context.Background()
		ws := fmt.Sprintf("ws-rollback-%d", time.Now().UnixNano())
		cluster := "cluster-main"
		defer s.DeleteWorkspaceState(ctx, ws)

		t0 := time.Now().UTC()

		// If postgres, we can test complete rollback of transaction on context cancellation
		// or error during execution
		cancelCtx, cancel := context.WithCancel(ctx)
		// Cancel immediately so that transaction fails before commit
		cancel()

		res := state.Resource{
			WorkspaceID: ws,
			Identity: state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "pod",
				ProviderID:   "pod-failed",
			},
			FirstObservedAt: t0,
			LastObservedAt:  t0,
		}

		_, err := s.ReconcileAuthoritativeBatch(cancelCtx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-fail",
			SamplingStartedAt: t0,
			Resources:         []state.Resource{res},
		})

		if err == nil {
			t.Fatalf("expected error on cancelled context, got nil")
		}

		// Objective 9: Complete rollback: no resources and no authority exist
		resources, err := s.ListResources(ctx, ws)
		if err != nil {
			t.Fatalf("ListResources failed: %v", err)
		}
		if len(resources) != 0 {
			t.Errorf("expected 0 resources after rollback, got %d", len(resources))
		}

		auth, err := s.GetWorkspaceAuthority(ctx, ws, cluster)
		if err != nil && !errors.Is(err, state.ErrAuthorityNotFound) {
			t.Fatalf("GetWorkspaceAuthority failed: %v", err)
		}
		if auth != nil {
			t.Errorf("expected no authority after complete rollback, got %+v", auth)
		}
	})
}

// -----------------------------------------------------------------------------
// 7. Failed or cancelled discovery preserving authoritative state
// -----------------------------------------------------------------------------
func TestAuthority_FailedOrCancelledDiscoveryPreservesState(t *testing.T) {
	forEachStore(t, func(t *testing.T, s state.Store, isPostgres bool, db database.DB) {
		ctx := context.Background()
		ws := fmt.Sprintf("ws-discovery-fail-%d", time.Now().UnixNano())
		cluster := "cluster-main"
		defer s.DeleteWorkspaceState(ctx, ws)

		t0 := time.Now().UTC()

		// 1. Initial successful run
		mockDisc := &mockDiscoveryRunner{
			resp: &corev1.RunDiscoveryResponse{Results: nil},
		}
		reconciler := state.NewReconciler(s, mockDisc, nil, nil)

		evInitial := newTestEvidence("ev-001", "kubernetes", "pod", cluster+"/default/pod-initial", "CONTROL_PLANE_OBJECT", `{}`, t0)
		res1, err := reconciler.Reconcile(ctx, state.ObservationBatch{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-initial",
			SamplingStartedAt: t0,
			Evidence:          []*corev1.Evidence{evInitial},
		})
		if err != nil {
			t.Fatalf("initial reconcile failed: %v", err)
		}
		if res1.Generation != 1 {
			t.Errorf("expected generation 1, got %d", res1.Generation)
		}

		authBefore, err := s.GetWorkspaceAuthority(ctx, ws, cluster)
		if err != nil || authBefore == nil {
			t.Fatalf("expected authority before failed discovery: %v", err)
		}

		// 2. Subsequent run with FAILING discovery
		failingDisc := &mockDiscoveryRunner{
			err: errors.New("discovery engine panic or rpc failure"),
		}
		reconcilerFail := state.NewReconciler(s, failingDisc, nil, nil)

		t1 := t0.Add(5 * time.Minute)
		evNew := newTestEvidence("ev-002", "kubernetes", "pod", cluster+"/default/pod-new", "CONTROL_PLANE_OBJECT", `{}`, t1)

		_, errFail := reconcilerFail.Reconcile(ctx, state.ObservationBatch{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-failing-disc",
			SamplingStartedAt: t1,
			Evidence:          []*corev1.Evidence{evNew},
		})

		// Objective 8: Failed or cancelled discovery must not advance authority or modify authoritative state.
		if errFail == nil {
			t.Fatalf("expected discovery failure error, got nil")
		}

		authAfter, err := s.GetWorkspaceAuthority(ctx, ws, cluster)
		if err != nil || authAfter == nil {
			t.Fatalf("failed to retrieve authority: %v", err)
		}

		if authAfter.LastGeneration != authBefore.LastGeneration {
			t.Errorf("generation advanced despite discovery failure! before=%d, after=%d",
				authBefore.LastGeneration, authAfter.LastGeneration)
		}
		if authAfter.AuthoritativeRunID != "run-initial" {
			t.Errorf("authoritative run ID changed despite discovery failure: %s", authAfter.AuthoritativeRunID)
		}

		// Resources should only contain the initial resource
		resources, _ := s.ListResources(ctx, ws)
		if len(resources) != 1 || resources[0].Identity.ProviderID != cluster+"/default/pod-initial" {
			t.Errorf("resources were modified despite discovery failure: %+v", resources)
		}
	})
}

// -----------------------------------------------------------------------------
// 8. Stale-run audit recording
// -----------------------------------------------------------------------------
func TestAuthority_StaleRunAuditRecording(t *testing.T) {
	forEachStore(t, func(t *testing.T, s state.Store, isPostgres bool, db database.DB) {
		ctx := context.Background()
		ws := fmt.Sprintf("ws-stale-audit-%d", time.Now().UnixNano())
		cluster := "cluster-main"
		defer s.DeleteWorkspaceState(ctx, ws)

		t0 := time.Now().UTC()
		tStale := t0.Add(-30 * time.Minute)

		// Authoritative run at t0
		res1 := state.Resource{
			WorkspaceID: ws,
			Identity: state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "pod",
				ProviderID:   "pod-current",
			},
			FirstObservedAt: t0,
			LastObservedAt:  t0,
		}

		_, err := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-current",
			SamplingStartedAt: t0,
			Resources:         []state.Resource{res1},
		})
		if err != nil {
			t.Fatalf("initial run failed: %v", err)
		}

		// Stale run at tStale
		resStale := state.Resource{
			WorkspaceID: ws,
			Identity: state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "pod",
				ProviderID:   "pod-stale",
			},
			FirstObservedAt: tStale,
			LastObservedAt:  tStale,
		}

		staleRunID := "run-stale-audit-test"
		_, errStale := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             staleRunID,
			SamplingStartedAt: tStale,
			Resources:         []state.Resource{resStale},
		})

		if !errors.Is(errStale, state.ErrStaleCollectionRunIgnored) {
			t.Fatalf("expected ErrStaleCollectionRunIgnored, got %v", errStale)
		}

		// Objective 10: Record stale-run audit information separately
		audits, err := s.ListStaleRunAudits(ctx, ws)
		if err != nil {
			t.Fatalf("ListStaleRunAudits failed: %v", err)
		}

		if len(audits) != 1 {
			t.Fatalf("expected exactly 1 stale audit record, got %d", len(audits))
		}

		audit := audits[0]
		if audit.RunID != staleRunID {
			t.Errorf("expected audit RunID %s, got %s", staleRunID, audit.RunID)
		}
		if audit.ClusterID != cluster {
			t.Errorf("expected audit ClusterID %s, got %s", cluster, audit.ClusterID)
		}
		if audit.Status != "STALE_IGNORED" {
			t.Errorf("expected audit Status STALE_IGNORED, got %s", audit.Status)
		}
		if audit.Reason == "" {
			t.Errorf("expected audit Reason to be non-empty")
		}

		// Verify authoritative state and resources were not mutated
		resources, _ := s.ListResources(ctx, ws)
		if len(resources) != 1 || resources[0].Identity.ProviderID != "pod-current" {
			t.Errorf("authoritative resources mutated: %+v", resources)
		}
	})
}

// -----------------------------------------------------------------------------
// 9. Independent workspace/cluster concurrency
// -----------------------------------------------------------------------------
func TestAuthority_IndependentWorkspaceClusterConcurrency(t *testing.T) {
	forEachStore(t, func(t *testing.T, s state.Store, isPostgres bool, db database.DB) {
		ctx := context.Background()
		ws1 := fmt.Sprintf("ws-indep-1-%d", time.Now().UnixNano())
		ws2 := fmt.Sprintf("ws-indep-2-%d", time.Now().UnixNano())
		cluster1 := "cluster-east"
		cluster2 := "cluster-west"
		defer s.DeleteWorkspaceState(ctx, ws1)
		defer s.DeleteWorkspaceState(ctx, ws2)

		var wg sync.WaitGroup
		wg.Add(2)

		t0 := time.Now().UTC()

		res1 := state.Resource{
			WorkspaceID: ws1,
			Identity: state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "pod",
				ProviderID:   "pod-east",
			},
			FirstObservedAt: t0,
			LastObservedAt:  t0,
		}

		res2 := state.Resource{
			WorkspaceID: ws2,
			Identity: state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "pod",
				ProviderID:   "pod-west",
			},
			FirstObservedAt: t0,
			LastObservedAt:  t0,
		}

		errCh := make(chan error, 2)

		go func() {
			defer wg.Done()
			_, err := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
				WorkspaceID:       ws1,
				ClusterID:         cluster1,
				RunID:             "run-east",
				SamplingStartedAt: t0,
				Resources:         []state.Resource{res1},
			})
			errCh <- err
		}()

		go func() {
			defer wg.Done()
			_, err := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
				WorkspaceID:       ws2,
				ClusterID:         cluster2,
				RunID:             "run-west",
				SamplingStartedAt: t0,
				Resources:         []state.Resource{res2},
			})
			errCh <- err
		}()

		wg.Wait()
		close(errCh)

		for err := range errCh {
			if err != nil {
				t.Fatalf("independent concurrency run failed: %v", err)
			}
		}

		auth1, err := s.GetWorkspaceAuthority(ctx, ws1, cluster1)
		if err != nil || auth1 == nil {
			t.Fatalf("failed to retrieve auth1: %v", err)
		}
		auth2, err := s.GetWorkspaceAuthority(ctx, ws2, cluster2)
		if err != nil || auth2 == nil {
			t.Fatalf("failed to retrieve auth2: %v", err)
		}

		if auth1.ClusterID != cluster1 || auth2.ClusterID != cluster2 {
			t.Errorf("cluster IDs mismatched in authorities")
		}
	})
}

// -----------------------------------------------------------------------------
// 10. Workspace isolation
// -----------------------------------------------------------------------------
func TestAuthority_WorkspaceIsolation(t *testing.T) {
	forEachStore(t, func(t *testing.T, s state.Store, isPostgres bool, db database.DB) {
		ctx := context.Background()
		wsA := fmt.Sprintf("ws-iso-a-%d", time.Now().UnixNano())
		wsB := fmt.Sprintf("ws-iso-b-%d", time.Now().UnixNano())
		cluster := "shared-cluster"
		defer s.DeleteWorkspaceState(ctx, wsA)
		defer s.DeleteWorkspaceState(ctx, wsB)

		t0 := time.Now().UTC()

		// Both workspaces have a resource with IDENTICAL provider, resource_type, and provider_id
		resIdentical := state.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "service",
			ProviderID:   "production/api-gateway",
		}

		_, errA := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       wsA,
			ClusterID:         cluster,
			RunID:             "run-a",
			SamplingStartedAt: t0,
			Resources: []state.Resource{{
				WorkspaceID:     wsA,
				Identity:        resIdentical,
				FirstObservedAt: t0,
				LastObservedAt:  t0,
			}},
		})
		if errA != nil {
			t.Fatalf("workspace A reconcile failed: %v", errA)
		}

		t1 := t0.Add(10 * time.Minute)
		_, errB := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       wsB,
			ClusterID:         cluster,
			RunID:             "run-b",
			SamplingStartedAt: t1,
			Resources: []state.Resource{{
				WorkspaceID:     wsB,
				Identity:        resIdentical,
				FirstObservedAt: t1,
				LastObservedAt:  t1,
			}},
		})
		if errB != nil {
			t.Fatalf("workspace B reconcile failed: %v", errB)
		}

		// Verify isolation in authority
		authA, err := s.GetWorkspaceAuthority(ctx, wsA, cluster)
		if err != nil || authA == nil {
			t.Fatalf("failed to get authA: %v", err)
		}
		authB, err := s.GetWorkspaceAuthority(ctx, wsB, cluster)
		if err != nil || authB == nil {
			t.Fatalf("failed to get authB: %v", err)
		}

		if authA.WorkspaceID != wsA || authB.WorkspaceID != wsB {
			t.Errorf("authority workspace IDs mismatched")
		}
		if authA.AuthoritativeRunID != "run-a" || authB.AuthoritativeRunID != "run-b" {
			t.Errorf("authority runs crossed: authA=%s, authB=%s", authA.AuthoritativeRunID, authB.AuthoritativeRunID)
		}

		// Delete workspace A and verify workspace B is completely intact
		if err := s.DeleteWorkspaceState(ctx, wsA); err != nil {
			t.Fatalf("DeleteWorkspaceState wsA failed: %v", err)
		}

		resListA, _ := s.ListResources(ctx, wsA)
		if len(resListA) != 0 {
			t.Errorf("expected wsA resources to be empty, got %d", len(resListA))
		}

		resListB, _ := s.ListResources(ctx, wsB)
		if len(resListB) != 1 {
			t.Errorf("expected wsB resources to be unaffected, got %d", len(resListB))
		}
	})
}

// =============================================================================
// REGRESSION SUITE D: COLLECTION-RUN GENERATION & AUTHORITY ORDERING CONTRACT
// =============================================================================

// D1. First run with Generation == 0
func TestAuthority_Regression_D1_FirstRunWithGenerationZero(t *testing.T) {
	forEachStore(t, func(t *testing.T, s state.Store, isPostgres bool, db database.DB) {
		ctx := context.Background()
		ws := fmt.Sprintf("ws-d1-%d", time.Now().UnixNano())
		cluster := "cluster-d1"
		defer s.DeleteWorkspaceState(ctx, ws)

		t0 := time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC)
		res := state.Resource{
			WorkspaceID: ws,
			Identity:    state.ResourceIdentity{Provider: "k8s", ResourceType: "pod", ProviderID: "pod-d1"},
			FirstObservedAt: t0, LastObservedAt: t0,
		}

		r, err := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-d1",
			SamplingStartedAt: t0,
			Generation:        0, // caller passes 0
			Resources:         []state.Resource{res},
		})
		if err != nil {
			t.Fatalf("first run with generation 0 failed: %v", err)
		}
		if r.AssignedGeneration != 1 {
			t.Errorf("expected AssignedGeneration 1, got %d", r.AssignedGeneration)
		}

		// Assert persisted authority
		auth, err := s.GetWorkspaceAuthority(ctx, ws, cluster)
		if err != nil || auth == nil {
			t.Fatalf("failed to retrieve authority: %v", err)
		}
		if auth.LastGeneration != 1 {
			t.Errorf("expected persisted LastGeneration 1, got %d", auth.LastGeneration)
		}
		if auth.AuthoritativeRunID != "run-d1" {
			t.Errorf("expected run-d1, got %s", auth.AuthoritativeRunID)
		}
		if !auth.AuthoritativeSamplingStartedAt.Equal(t0) {
			t.Errorf("expected sampling timestamp %v, got %v", t0, auth.AuthoritativeSamplingStartedAt)
		}

		// Assert persisted domain state
		list, _ := s.ListResources(ctx, ws)
		if len(list) != 1 || list[0].Identity.ProviderID != "pod-d1" {
			t.Errorf("unexpected resources: %+v", list)
		}
	})
}

// D2. Newer sampling timestamp with Generation == 0
func TestAuthority_Regression_D2_NewerSamplingWithGenerationZero(t *testing.T) {
	forEachStore(t, func(t *testing.T, s state.Store, isPostgres bool, db database.DB) {
		ctx := context.Background()
		ws := fmt.Sprintf("ws-d2-%d", time.Now().UnixNano())
		cluster := "cluster-d2"
		defer s.DeleteWorkspaceState(ctx, ws)

		t0 := time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC)
		t1 := t0.Add(5 * time.Minute)

		// Run 1: Generation == 0 -> receives generation 1
		_, err1 := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-d2-1",
			SamplingStartedAt: t0,
			Generation:        0,
			Resources: []state.Resource{{
				WorkspaceID: ws, Identity: state.ResourceIdentity{Provider: "k8s", ResourceType: "pod", ProviderID: "pod-1"},
				FirstObservedAt: t0, LastObservedAt: t0,
			}},
		})
		if err1 != nil {
			t.Fatalf("run 1 failed: %v", err1)
		}

		// Run 2: newer timestamp with Generation == 0 -> must advance to generation 2
		r2, err2 := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-d2-2",
			SamplingStartedAt: t1,
			Generation:        0,
			Resources: []state.Resource{{
				WorkspaceID: ws, Identity: state.ResourceIdentity{Provider: "k8s", ResourceType: "pod", ProviderID: "pod-2"},
				FirstObservedAt: t1, LastObservedAt: t1,
			}},
		})
		if err2 != nil {
			t.Fatalf("run 2 failed: %v", err2)
		}
		if r2.AssignedGeneration != 2 {
			t.Errorf("expected AssignedGeneration 2, got %d", r2.AssignedGeneration)
		}

		// Assert persisted authority
		auth, err := s.GetWorkspaceAuthority(ctx, ws, cluster)
		if err != nil || auth == nil {
			t.Fatalf("failed to retrieve authority: %v", err)
		}
		if auth.LastGeneration != 2 {
			t.Errorf("expected persisted LastGeneration 2, got %d", auth.LastGeneration)
		}
		if auth.AuthoritativeRunID != "run-d2-2" {
			t.Errorf("expected run-d2-2, got %s", auth.AuthoritativeRunID)
		}
		if !auth.AuthoritativeSamplingStartedAt.Equal(t1) {
			t.Errorf("expected sampling timestamp %v, got %v", t1, auth.AuthoritativeSamplingStartedAt)
		}

		// Assert persisted domain state contains both
		list, _ := s.ListResources(ctx, ws)
		if len(list) != 2 {
			t.Errorf("expected 2 resources, got %d", len(list))
		}
	})
}

// D3. Equal timestamp with Generation == 0 must be rejected
func TestAuthority_Regression_D3_EqualTimestampWithGenerationZero(t *testing.T) {
	forEachStore(t, func(t *testing.T, s state.Store, isPostgres bool, db database.DB) {
		ctx := context.Background()
		ws := fmt.Sprintf("ws-d3-%d", time.Now().UnixNano())
		cluster := "cluster-d3"
		defer s.DeleteWorkspaceState(ctx, ws)

		t0 := time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC)

		// Run 1 establishes authority at t0
		_, err1 := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-d3-1",
			SamplingStartedAt: t0,
			Generation:        0,
			Resources: []state.Resource{{
				WorkspaceID: ws, Identity: state.ResourceIdentity{Provider: "k8s", ResourceType: "pod", ProviderID: "pod-auth"},
				FirstObservedAt: t0, LastObservedAt: t0,
			}},
		})
		if err1 != nil {
			t.Fatalf("initial run failed: %v", err1)
		}

		// Run 2 has exact EQUAL timestamp and Generation == 0
		_, err2 := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-d3-2-stale",
			SamplingStartedAt: t0,
			Generation:        0,
			Resources: []state.Resource{{
				WorkspaceID: ws, Identity: state.ResourceIdentity{Provider: "k8s", ResourceType: "pod", ProviderID: "pod-stale"},
				FirstObservedAt: t0, LastObservedAt: t0,
			}},
		})

		// Contract: Must be rejected as stale! Equal timestamp cannot automatically advance generation.
		if !errors.Is(err2, state.ErrStaleCollectionRunIgnored) {
			t.Fatalf("expected ErrStaleCollectionRunIgnored for equal timestamp with generation 0, got %v", err2)
		}

		// Assert authority was NOT modified
		auth, _ := s.GetWorkspaceAuthority(ctx, ws, cluster)
		if auth.LastGeneration != 1 {
			t.Errorf("expected LastGeneration to remain 1, got %d", auth.LastGeneration)
		}
		if auth.AuthoritativeRunID != "run-d3-1" {
			t.Errorf("expected authoritative run to remain run-d3-1, got %s", auth.AuthoritativeRunID)
		}

		// Assert pod-stale was NOT written
		list, _ := s.ListResources(ctx, ws)
		if len(list) != 1 || list[0].Identity.ProviderID != "pod-auth" {
			t.Errorf("domain state was mutated by stale run: %+v", list)
		}

		// Assert stale audit was recorded
		audits, _ := s.ListStaleRunAudits(ctx, ws)
		if len(audits) != 1 || audits[0].RunID != "run-d3-2-stale" {
			t.Errorf("stale audit not recorded correctly: %+v", audits)
		}
	})
}

// D4. Equal timestamp with explicit lower generation
func TestAuthority_Regression_D4_EqualTimestampWithExplicitLowerGeneration(t *testing.T) {
	forEachStore(t, func(t *testing.T, s state.Store, isPostgres bool, db database.DB) {
		ctx := context.Background()
		ws := fmt.Sprintf("ws-d4-%d", time.Now().UnixNano())
		cluster := "cluster-d4"
		defer s.DeleteWorkspaceState(ctx, ws)

		t0 := time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC)

		// Run 1 establishes authority with explicit generation 5
		_, err1 := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-d4-1",
			SamplingStartedAt: t0,
			Generation:        5,
			Resources: []state.Resource{{
				WorkspaceID: ws, Identity: state.ResourceIdentity{Provider: "k8s", ResourceType: "pod", ProviderID: "pod-1"},
				FirstObservedAt: t0, LastObservedAt: t0,
			}},
		})
		if err1 != nil {
			t.Fatalf("run 1 failed: %v", err1)
		}

		// Run 2 has EQUAL timestamp and explicit lower generation 2
		_, err2 := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-d4-2",
			SamplingStartedAt: t0,
			Generation:        2,
			Resources: []state.Resource{{
				WorkspaceID: ws, Identity: state.ResourceIdentity{Provider: "k8s", ResourceType: "pod", ProviderID: "pod-2"},
				FirstObservedAt: t0, LastObservedAt: t0,
			}},
		})
		if !errors.Is(err2, state.ErrStaleCollectionRunIgnored) {
			t.Fatalf("expected ErrStaleCollectionRunIgnored, got %v", err2)
		}

		auth, _ := s.GetWorkspaceAuthority(ctx, ws, cluster)
		if auth.LastGeneration != 5 || auth.AuthoritativeRunID != "run-d4-1" {
			t.Errorf("authority modified by lower generation: %+v", auth)
		}
	})
}

// D5. Equal timestamp with explicit higher generation
func TestAuthority_Regression_D5_EqualTimestampWithExplicitHigherGeneration(t *testing.T) {
	forEachStore(t, func(t *testing.T, s state.Store, isPostgres bool, db database.DB) {
		ctx := context.Background()
		ws := fmt.Sprintf("ws-d5-%d", time.Now().UnixNano())
		cluster := "cluster-d5"
		defer s.DeleteWorkspaceState(ctx, ws)

		t0 := time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC)

		// Run 1 establishes authority with generation 2
		_, err1 := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-d5-1",
			SamplingStartedAt: t0,
			Generation:        2,
			Resources: []state.Resource{{
				WorkspaceID: ws, Identity: state.ResourceIdentity{Provider: "k8s", ResourceType: "pod", ProviderID: "pod-1"},
				FirstObservedAt: t0, LastObservedAt: t0,
			}},
		})
		if err1 != nil {
			t.Fatalf("run 1 failed: %v", err1)
		}

		// Run 2 has EQUAL timestamp and explicit HIGHER generation 8
		r2, err2 := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-d5-2",
			SamplingStartedAt: t0,
			Generation:        8,
			Resources: []state.Resource{{
				WorkspaceID: ws, Identity: state.ResourceIdentity{Provider: "k8s", ResourceType: "pod", ProviderID: "pod-2"},
				FirstObservedAt: t0, LastObservedAt: t0,
			}},
		})
		if err2 != nil {
			t.Fatalf("run 2 with higher generation failed: %v", err2)
		}
		if r2.AssignedGeneration != 8 {
			t.Errorf("expected AssignedGeneration 8, got %d", r2.AssignedGeneration)
		}

		auth, _ := s.GetWorkspaceAuthority(ctx, ws, cluster)
		if auth.LastGeneration != 8 || auth.AuthoritativeRunID != "run-d5-2" {
			t.Errorf("authority not advanced to higher generation: %+v", auth)
		}
	})
}

// D6. Older timestamp with explicit higher generation
func TestAuthority_Regression_D6_OlderTimestampWithExplicitHigherGeneration(t *testing.T) {
	forEachStore(t, func(t *testing.T, s state.Store, isPostgres bool, db database.DB) {
		ctx := context.Background()
		ws := fmt.Sprintf("ws-d6-%d", time.Now().UnixNano())
		cluster := "cluster-d6"
		defer s.DeleteWorkspaceState(ctx, ws)

		t0 := time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC)
		t1 := t0.Add(10 * time.Minute)

		// Run 1 establishes authority at newer timestamp t1, generation 2
		_, err1 := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-d6-1",
			SamplingStartedAt: t1,
			Generation:        2,
			Resources: []state.Resource{{
				WorkspaceID: ws, Identity: state.ResourceIdentity{Provider: "k8s", ResourceType: "pod", ProviderID: "pod-1"},
				FirstObservedAt: t1, LastObservedAt: t1,
			}},
		})
		if err1 != nil {
			t.Fatalf("run 1 failed: %v", err1)
		}

		// Run 2 has strictly OLDER timestamp t0, but explicit high generation 99
		_, err2 := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-d6-2",
			SamplingStartedAt: t0,
			Generation:        99,
			Resources: []state.Resource{{
				WorkspaceID: ws, Identity: state.ResourceIdentity{Provider: "k8s", ResourceType: "pod", ProviderID: "pod-2"},
				FirstObservedAt: t0, LastObservedAt: t0,
			}},
		})

		// Contract: Strictly older timestamp MUST be rejected, regardless of caller generation.
		if !errors.Is(err2, state.ErrStaleCollectionRunIgnored) {
			t.Fatalf("expected ErrStaleCollectionRunIgnored, got %v", err2)
		}

		auth, _ := s.GetWorkspaceAuthority(ctx, ws, cluster)
		if auth.LastGeneration != 2 || auth.AuthoritativeRunID != "run-d6-1" {
			t.Errorf("authority overwritten by older run: %+v", auth)
		}
	})
}

// D7. AssignedGeneration exactly matches committed last_generation
func TestAuthority_Regression_D7_AssignedGenerationMatchesCommittedLastGeneration(t *testing.T) {
	forEachStore(t, func(t *testing.T, s state.Store, isPostgres bool, db database.DB) {
		ctx := context.Background()
		ws := fmt.Sprintf("ws-d7-%d", time.Now().UnixNano())
		cluster := "cluster-d7"
		defer s.DeleteWorkspaceState(ctx, ws)

		t0 := time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC)

		// Step a: Run with Generation == 0
		r1, err := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-d7-1",
			SamplingStartedAt: t0,
			Generation:        0,
			Resources: []state.Resource{{
				WorkspaceID: ws, Identity: state.ResourceIdentity{Provider: "k8s", ResourceType: "pod", ProviderID: "p1"},
				FirstObservedAt: t0, LastObservedAt: t0,
			}},
		})
		if err != nil {
			t.Fatalf("step a failed: %v", err)
		}
		auth1, _ := s.GetWorkspaceAuthority(ctx, ws, cluster)
		if r1.AssignedGeneration != auth1.LastGeneration {
			t.Fatalf("mismatch: AssignedGeneration=%d, committed LastGeneration=%d",
				r1.AssignedGeneration, auth1.LastGeneration)
		}

		// Step b: Run with explicit higher generation 7
		t1 := t0.Add(1 * time.Minute)
		r2, err := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-d7-2",
			SamplingStartedAt: t1,
			Generation:        7,
			Resources: []state.Resource{{
				WorkspaceID: ws, Identity: state.ResourceIdentity{Provider: "k8s", ResourceType: "pod", ProviderID: "p2"},
				FirstObservedAt: t1, LastObservedAt: t1,
			}},
		})
		if err != nil {
			t.Fatalf("step b failed: %v", err)
		}
		auth2, _ := s.GetWorkspaceAuthority(ctx, ws, cluster)
		if r2.AssignedGeneration != auth2.LastGeneration {
			t.Fatalf("mismatch: AssignedGeneration=%d, committed LastGeneration=%d",
				r2.AssignedGeneration, auth2.LastGeneration)
		}

		// Step c: Subsequent run with Generation == 0 advancing from 7 -> 8
		t2 := t1.Add(1 * time.Minute)
		r3, err := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-d7-3",
			SamplingStartedAt: t2,
			Generation:        0,
			Resources: []state.Resource{{
				WorkspaceID: ws, Identity: state.ResourceIdentity{Provider: "k8s", ResourceType: "pod", ProviderID: "p3"},
				FirstObservedAt: t2, LastObservedAt: t2,
			}},
		})
		if err != nil {
			t.Fatalf("step c failed: %v", err)
		}
		auth3, _ := s.GetWorkspaceAuthority(ctx, ws, cluster)
		if r3.AssignedGeneration != auth3.LastGeneration || r3.AssignedGeneration != 8 {
			t.Fatalf("mismatch: AssignedGeneration=%d, committed LastGeneration=%d (expected 8)",
				r3.AssignedGeneration, auth3.LastGeneration)
		}
	})
}

// D8. Two concurrent runs with equal sampling timestamps
func TestAuthority_Regression_D8_TwoConcurrentRunsWithEqualSamplingTimestamps(t *testing.T) {
	forEachStore(t, func(t *testing.T, s state.Store, isPostgres bool, db database.DB) {
		ctx := context.Background()
		ws := fmt.Sprintf("ws-d8-%d", time.Now().UnixNano())
		cluster := "cluster-d8"
		defer s.DeleteWorkspaceState(ctx, ws)

		tSame := time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC)

		startGate := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)

		errs := make([]error, 2)
		results := make([]*state.AuthoritativeBatchResult, 2)

		for i := 0; i < 2; i++ {
			idx := i
			go func() {
				defer wg.Done()
				<-startGate

				r, err := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
					WorkspaceID:       ws,
					ClusterID:         cluster,
					RunID:             fmt.Sprintf("run-d8-%d", idx),
					SamplingStartedAt: tSame,
					Generation:        0,
					Resources: []state.Resource{{
						WorkspaceID: ws, Identity: state.ResourceIdentity{Provider: "k8s", ResourceType: "pod", ProviderID: fmt.Sprintf("pod-%d", idx)},
						FirstObservedAt: tSame, LastObservedAt: tSame,
					}},
				})
				errs[idx] = err
				results[idx] = r
			}()
		}

		close(startGate)
		wg.Wait()

		var successCount int
		var staleCount int
		var winningIdx int
		for i := 0; i < 2; i++ {
			if errs[i] == nil {
				successCount++
				winningIdx = i
			} else if errors.Is(errs[i], state.ErrStaleCollectionRunIgnored) {
				staleCount++
			} else {
				t.Fatalf("unexpected error in run %d: %v", i, errs[i])
			}
		}

		if successCount != 1 || staleCount != 1 {
			t.Fatalf("expected exactly 1 success and 1 stale rejection, got %d successes and %d stale",
				successCount, staleCount)
		}

		// Assert persisted authority
		auth, _ := s.GetWorkspaceAuthority(ctx, ws, cluster)
		if auth.LastGeneration != 1 {
			t.Errorf("expected LastGeneration 1, got %d", auth.LastGeneration)
		}
		expectedWinningRunID := fmt.Sprintf("run-d8-%d", winningIdx)
		if auth.AuthoritativeRunID != expectedWinningRunID {
			t.Errorf("expected authoritative run %s, got %s", expectedWinningRunID, auth.AuthoritativeRunID)
		}

		// Assert only the winning resource is persisted
		list, _ := s.ListResources(ctx, ws)
		if len(list) != 1 || list[0].Identity.ProviderID != fmt.Sprintf("pod-%d", winningIdx) {
			t.Errorf("unexpected resources persisted: %+v", list)
		}
	})
}

// D9. An older run finishing after a newer run
func TestAuthority_Regression_D9_OlderRunFinishingAfterNewerRun(t *testing.T) {
	forEachStore(t, func(t *testing.T, s state.Store, isPostgres bool, db database.DB) {
		ctx := context.Background()
		ws := fmt.Sprintf("ws-d9-%d", time.Now().UnixNano())
		cluster := "cluster-d9"
		defer s.DeleteWorkspaceState(ctx, ws)

		tOld := time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC)
		tNew := tOld.Add(15 * time.Minute)

		// Run 2 (newer) completes and commits first
		rNew, errNew := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-newer",
			SamplingStartedAt: tNew,
			Generation:        0,
			Resources: []state.Resource{{
				WorkspaceID: ws, Identity: state.ResourceIdentity{Provider: "k8s", ResourceType: "pod", ProviderID: "pod-newer"},
				FirstObservedAt: tNew, LastObservedAt: tNew,
			}},
		})
		if errNew != nil {
			t.Fatalf("newer run failed: %v", errNew)
		}
		if rNew.AssignedGeneration != 1 {
			t.Errorf("expected newer run generation 1, got %d", rNew.AssignedGeneration)
		}

		// Run 1 (older) completes afterward
		_, errOld := s.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "run-older",
			SamplingStartedAt: tOld,
			Generation:        0,
			Resources: []state.Resource{{
				WorkspaceID: ws, Identity: state.ResourceIdentity{Provider: "k8s", ResourceType: "pod", ProviderID: "pod-older"},
				FirstObservedAt: tOld, LastObservedAt: tOld,
			}},
		})
		if !errors.Is(errOld, state.ErrStaleCollectionRunIgnored) {
			t.Fatalf("expected older run to be rejected with ErrStaleCollectionRunIgnored, got %v", errOld)
		}

		// Assert authority is preserved at newer run
		auth, _ := s.GetWorkspaceAuthority(ctx, ws, cluster)
		if auth.AuthoritativeRunID != "run-newer" || auth.LastGeneration != 1 {
			t.Errorf("authoritative state overwritten by older run: %+v", auth)
		}

		// Assert only newer resource is persisted
		list, _ := s.ListResources(ctx, ws)
		if len(list) != 1 || list[0].Identity.ProviderID != "pod-newer" {
			t.Errorf("resources modified by older run: %+v", list)
		}
	})
}

// D10. Standard coordinator sweeps using Generation == 0
func TestAuthority_Regression_D10_StandardCoordinatorSweepsGenerationZero(t *testing.T) {
	forEachStore(t, func(t *testing.T, s state.Store, isPostgres bool, db database.DB) {
		ctx := context.Background()
		ws := fmt.Sprintf("ws-d10-%d", time.Now().UnixNano())
		cluster := "cluster-d10"
		defer s.DeleteWorkspaceState(ctx, ws)

		mockDisc := &mockDiscoveryRunner{
			resp: &corev1.RunDiscoveryResponse{Results: nil},
		}
		reconciler := state.NewReconciler(s, mockDisc, nil, nil)

		baseTime := time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC)

		// Sweep 1
		t1 := baseTime
		ev1 := newTestEvidence("ev-1", "kubernetes", "pod", cluster+"/default/pod-sweep", "CONTROL_PLANE_OBJECT", `{}`, t1)
		res1, err := reconciler.Reconcile(ctx, state.ObservationBatch{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "sweep-1",
			SamplingStartedAt: t1,
			Generation:        0, // Standard coordinator default
			Evidence:          []*corev1.Evidence{ev1},
		})
		if err != nil || res1.Generation != 1 {
			t.Fatalf("sweep 1 failed: err=%v, gen=%d", err, res1.Generation)
		}

		// Sweep 2
		t2 := baseTime.Add(2 * time.Minute)
		ev2 := newTestEvidence("ev-2", "kubernetes", "pod", cluster+"/default/pod-sweep", "CONTROL_PLANE_OBJECT", `{}`, t2)
		res2, err := reconciler.Reconcile(ctx, state.ObservationBatch{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "sweep-2",
			SamplingStartedAt: t2,
			Generation:        0,
			Evidence:          []*corev1.Evidence{ev2},
		})
		if err != nil || res2.Generation != 2 {
			t.Fatalf("sweep 2 failed: err=%v, gen=%d", err, res2.Generation)
		}

		// Sweep 3
		t3 := baseTime.Add(4 * time.Minute)
		ev3 := newTestEvidence("ev-3", "kubernetes", "pod", cluster+"/default/pod-sweep", "CONTROL_PLANE_OBJECT", `{}`, t3)
		res3, err := reconciler.Reconcile(ctx, state.ObservationBatch{
			WorkspaceID:       ws,
			ClusterID:         cluster,
			RunID:             "sweep-3",
			SamplingStartedAt: t3,
			Generation:        0,
			Evidence:          []*corev1.Evidence{ev3},
		})
		if err != nil || res3.Generation != 3 {
			t.Fatalf("sweep 3 failed: err=%v, gen=%d", err, res3.Generation)
		}

		// Assert persisted authority matches sweep 3
		auth, err := s.GetWorkspaceAuthority(ctx, ws, cluster)
		if err != nil || auth == nil {
			t.Fatalf("failed to retrieve authority: %v", err)
		}
		if auth.LastGeneration != 3 {
			t.Errorf("expected LastGeneration 3, got %d", auth.LastGeneration)
		}
		if auth.AuthoritativeRunID != "sweep-3" {
			t.Errorf("expected AuthoritativeRunID sweep-3, got %s", auth.AuthoritativeRunID)
		}
		if !auth.AuthoritativeSamplingStartedAt.Equal(t3) {
			t.Errorf("expected sampling timestamp %v, got %v", t3, auth.AuthoritativeSamplingStartedAt)
		}
	})
}
