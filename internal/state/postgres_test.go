package state_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "github.com/aaradhychinche-alt/WhatBreaks/gen/go/wb/core/v1"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/answer"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/auth"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/config"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/database"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/logging"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/state"
)

func getTestDatabaseConfig(t *testing.T) *config.DatabaseConfig {
	t.Helper()

	host := os.Getenv("TEST_DB_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := 5432
	if portStr := os.Getenv("TEST_DB_PORT"); portStr != "" {
		if p, err := strconv.Atoi(portStr); err == nil && p > 0 {
			port = p
		}
	}

	timeout := 200 * time.Millisecond
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(port)), timeout)
	if err != nil {
		t.Skipf("PostgreSQL is not reachable at %s:%d (%v); skipping integration test", host, port, err)
	}
	_ = conn.Close()

	user := os.Getenv("TEST_DB_USER")
	if user == "" {
		user = "postgres"
	}
	dbName := os.Getenv("TEST_DB_NAME")
	if dbName == "" {
		dbName = "postgres"
	}
	password := os.Getenv("TEST_DB_PASSWORD")

	return &config.DatabaseConfig{
		Host:              host,
		Port:              port,
		Database:          dbName,
		User:              user,
		Password:          password,
		PoolMax:           5,
		PoolMin:           1,
		PoolIdleTimeout:   10 * time.Second,
		ConnectionTimeout: 2 * time.Second,
	}
}

func TestPostgresStore_Integration(t *testing.T) {
	cfg := getTestDatabaseConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	logger := logging.NewJSONLogger(nil, logging.LevelDebug, "pg-state-test")
	db, err := database.New(ctx, cfg, database.WithLogger(logger))
	if err != nil {
		t.Skipf("Failed to initialize database pool: %v; skipping", err)
	}
	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		t.Skipf("PostgreSQL Ping failed: %v; skipping", err)
	}

	pgStore := state.NewPostgresStore(db)

	// -----------------------------------------------------------------------
	// 1. Schema Creation & Verification
	// -----------------------------------------------------------------------
	t.Run("SchemaVerification", func(t *testing.T) {
		if err := state.EnsureSchema(ctx, db); err != nil {
			t.Fatalf("EnsureSchema failed: %v", err)
		}

		expectedTables := []string{
			"state_resources",
			"state_evidence",
			"state_relationships",
			"state_provenance",
			"state_conflicts",
			"state_workspace_authority",
			"state_reconciliation_audit",
		}

		for _, table := range expectedTables {
			query := `SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1`
			var exists int
			err := db.QueryRow(ctx, query, table).Scan(&exists)
			if err != nil || exists != 1 {
				t.Fatalf("expected table %s to exist in PostgreSQL: %v", table, err)
			}
		}
	})

	ws1 := "ws-pg-test-tenant-1"
	ws2 := "ws-pg-test-tenant-2"

	// Cleanup any previous run
	_ = pgStore.DeleteWorkspaceState(ctx, ws1)
	_ = pgStore.DeleteWorkspaceState(ctx, ws2)
	t.Cleanup(func() {
		_ = pgStore.DeleteWorkspaceState(context.Background(), ws1)
		_ = pgStore.DeleteWorkspaceState(context.Background(), ws2)
	})

	now := time.Now().UTC().Truncate(time.Microsecond)
	res1 := state.Resource{
		WorkspaceID: ws1,
		Identity: state.ResourceIdentity{
			Provider:     "aws",
			ResourceType: "rds_instance",
			ProviderID:   "db-prod-main",
		},
		FirstObservedAt: now,
		LastObservedAt:  now,
	}
	res2 := state.Resource{
		WorkspaceID: ws1,
		Identity: state.ResourceIdentity{
			Provider:     "aws",
			ResourceType: "ecs_service",
			ProviderID:   "order-api",
		},
		FirstObservedAt: now,
		LastObservedAt:  now,
	}

	evID := "22222222-2222-2222-2222-222222222222"
	initialData := []byte(`{"metric": "db_connections", "val": 45}`)
	ev := state.Evidence{
		WorkspaceID: ws1,
		ID:          evID,
		Source: state.EvidenceSource{
			Provider:  "aws",
			Collector: "aws-cloudwatch",
		},
		ObservedAt:      now,
		ObservationType: "METRIC_CORRELATION",
		Subject:         res2.Identity,
		Data:            initialData,
	}

	rel := state.Relationship{
		WorkspaceID:     ws1,
		Source:          res2.Identity,
		Target:          res1.Identity,
		Category:        "storage",
		Kind:            "queries",
		FirstObservedAt: now,
		LastObservedAt:  now,
	}

	prov := state.ProvenanceAssociation{
		WorkspaceID:  ws1,
		Relationship: rel.Key(),
		EvidenceID:   evID,
	}

	conflict := state.DiscoveryConflict{
		WorkspaceID: ws1,
		Description: "conflicting target port observed",
		ObservedAt:  now,
	}

	// -----------------------------------------------------------------------
	// 2. Persistence & Reconstruction
	// -----------------------------------------------------------------------
	t.Run("PersistenceAndReconstruction", func(t *testing.T) {
		// Save resources
		if err := pgStore.SaveResources(ctx, ws1, []state.Resource{res1, res2}); err != nil {
			t.Fatalf("SaveResources failed: %v", err)
		}
		// Save evidence
		if err := pgStore.SaveEvidence(ctx, ws1, []state.Evidence{ev}); err != nil {
			t.Fatalf("SaveEvidence failed: %v", err)
		}
		// Save relationship
		if err := pgStore.SaveRelationships(ctx, ws1, []state.Relationship{rel}); err != nil {
			t.Fatalf("SaveRelationships failed: %v", err)
		}
		// Save provenance
		if err := pgStore.SaveProvenance(ctx, ws1, []state.ProvenanceAssociation{prov}); err != nil {
			t.Fatalf("SaveProvenance failed: %v", err)
		}
		// Save conflict
		if err := pgStore.SaveConflict(ctx, conflict); err != nil {
			t.Fatalf("SaveConflict failed: %v", err)
		}

		// Reconstruct full AssembledState
		assembled, err := pgStore.LoadAssembledState(ctx, ws1)
		if err != nil {
			t.Fatalf("LoadAssembledState failed: %v", err)
		}
		if len(assembled.Resources) != 2 {
			t.Errorf("expected 2 resources in ws1, got %d", len(assembled.Resources))
		}
		if len(assembled.Evidence) != 1 {
			t.Errorf("expected 1 evidence in ws1, got %d", len(assembled.Evidence))
		}
		if len(assembled.Relationships) != 1 {
			t.Errorf("expected 1 relationship in ws1, got %d", len(assembled.Relationships))
		}
		if len(assembled.Provenance) != 1 {
			t.Errorf("expected 1 provenance entry in ws1, got %d", len(assembled.Provenance))
		}

		// Verify individual query lookups
		gotRes, err := pgStore.GetResource(ctx, ws1, res1.Identity)
		if err != nil || gotRes.Identity.ProviderID != res1.Identity.ProviderID {
			t.Errorf("GetResource failed: %v, got %+v", err, gotRes)
		}

		evList, err := pgStore.ListEvidence(ctx, ws1)
		if err != nil || len(evList) != 1 || evList[0].ID != evID {
			t.Errorf("ListEvidence failed: %v, got %+v", err, evList)
		}

		provEvIDs, err := pgStore.GetProvenanceForRelationship(ctx, ws1, rel.Key())
		if err != nil || len(provEvIDs) != 1 || provEvIDs[0] != evID {
			t.Errorf("GetProvenanceForRelationship failed: %v", err)
		}

		provRels, err := pgStore.GetRelationshipsForEvidence(ctx, ws1, evID)
		if err != nil || len(provRels) != 1 || provRels[0] != rel.Key() {
			t.Errorf("GetRelationshipsForEvidence failed: %v", err)
		}
	})

	// -----------------------------------------------------------------------
	// 3. Idempotency & Immutability
	// -----------------------------------------------------------------------
	t.Run("IdempotencyAndEvidenceImmutability", func(t *testing.T) {
		laterTime := now.Add(5 * time.Minute)

		// Repeated resource write with later timestamp
		updatedRes := res1
		updatedRes.LastObservedAt = laterTime
		if err := pgStore.SaveResources(ctx, ws1, []state.Resource{updatedRes}); err != nil {
			t.Fatalf("repeated SaveResources failed: %v", err)
		}

		storedRes, err := pgStore.GetResource(ctx, ws1, res1.Identity)
		if err != nil {
			t.Fatalf("GetResource failed: %v", err)
		}
		if !storedRes.LastObservedAt.Equal(laterTime) {
			t.Errorf("expected LastObservedAt updated to %v, got %v", laterTime, storedRes.LastObservedAt)
		}

		// Repeated relationship write with later timestamp
		updatedRel := rel
		updatedRel.LastObservedAt = laterTime
		if err := pgStore.SaveRelationships(ctx, ws1, []state.Relationship{updatedRel}); err != nil {
			t.Fatalf("repeated SaveRelationships failed: %v", err)
		}

		rels, err := pgStore.ListRelationships(ctx, ws1)
		if err != nil || len(rels) != 1 {
			t.Fatalf("expected 1 relationship, got %d (err: %v)", len(rels), err)
		}
		if !rels[0].LastObservedAt.Equal(laterTime) {
			t.Errorf("expected relationship LastObservedAt updated to %v, got %v", laterTime, rels[0].LastObservedAt)
		}

		// Repeated provenance associations: duplicate ignored
		if err := pgStore.SaveProvenance(ctx, ws1, []state.ProvenanceAssociation{prov}); err != nil {
			t.Fatalf("repeated SaveProvenance failed: %v", err)
		}
		provIDs, err := pgStore.GetProvenanceForRelationship(ctx, ws1, rel.Key())
		if err != nil || len(provIDs) != 1 {
			t.Fatalf("expected still exactly 1 provenance link after repeated write, got %d", len(provIDs))
		}

		// Evidence Immutability: attempting to overwrite evidence data MUST NOT change existing data
		mutatedEvidence := ev
		mutatedEvidence.Data = []byte(`{"metric": "TAMPERED_DATA", "val": 9999}`)
		if err := pgStore.SaveEvidence(ctx, ws1, []state.Evidence{mutatedEvidence}); err != nil {
			t.Fatalf("SaveEvidence duplicate write failed: %v", err)
		}

		evList, err := pgStore.ListEvidence(ctx, ws1)
		if err != nil || len(evList) != 1 {
			t.Fatalf("ListEvidence failed: %v", err)
		}
		if !bytes.Equal(evList[0].Data, initialData) {
			t.Fatalf("Evidence immutability VIOLATED! Data was overwritten: %s", string(evList[0].Data))
		}
	})

	// -----------------------------------------------------------------------
	// 4. Workspace Isolation & Identical Resource Identities
	// -----------------------------------------------------------------------
	t.Run("WorkspaceIsolationWithIdenticalIdentities", func(t *testing.T) {
		// Insert identical ResourceIdentity in ws2
		resInWs2 := state.Resource{
			WorkspaceID:     ws2,
			Identity:        res1.Identity, // exact same (aws, rds_instance, db-prod-main)
			FirstObservedAt: now.Add(1 * time.Hour),
			LastObservedAt:  now.Add(2 * time.Hour),
		}
		if err := pgStore.SaveResources(ctx, ws2, []state.Resource{resInWs2}); err != nil {
			t.Fatalf("SaveResources in ws2 failed: %v", err)
		}

		// Query ws1: must have 2 resources
		ws1Resources, err := pgStore.ListResources(ctx, ws1)
		if err != nil || len(ws1Resources) != 2 {
			t.Fatalf("expected 2 resources in ws1, got %d", len(ws1Resources))
		}

		// Query ws2: must have only 1 resource
		ws2Resources, err := pgStore.ListResources(ctx, ws2)
		if err != nil || len(ws2Resources) != 1 {
			t.Fatalf("expected 1 resource in ws2, got %d", len(ws2Resources))
		}

		// Query ws2 state: relationships, evidence, provenance must be completely empty
		ws2State, err := pgStore.LoadAssembledState(ctx, ws2)
		if err != nil {
			t.Fatalf("LoadAssembledState for ws2 failed: %v", err)
		}
		if len(ws2State.Relationships) != 0 || len(ws2State.Evidence) != 0 || len(ws2State.Provenance) != 0 {
			t.Fatalf("Cross-tenant leakage! ws2 contains ws1 relationships or evidence: %+v", ws2State)
		}

		// Delete ws1: ws2 resource must remain intact
		if err := pgStore.DeleteWorkspaceState(ctx, ws1); err != nil {
			t.Fatalf("DeleteWorkspaceState ws1 failed: %v", err)
		}

		ws2Remaining, err := pgStore.ListResources(ctx, ws2)
		if err != nil || len(ws2Remaining) != 1 {
			t.Fatalf("expected ws2 resource to survive ws1 deletion, got %d", len(ws2Remaining))
		}
	})

	// -----------------------------------------------------------------------
	// 5. Transaction Rollback Atomicity
	// -----------------------------------------------------------------------
	t.Run("TransactionRollbackAtomicity", func(t *testing.T) {
		wsTx := "ws-tx-rollback-test"
		t.Cleanup(func() {
			_ = pgStore.DeleteWorkspaceState(context.Background(), wsTx)
		})

		customErr := errors.New("simulated batch failure")
		err := db.WithTransaction(ctx, func(ctx context.Context, tx database.Tx) error {
			_, err := tx.Exec(ctx,
				`INSERT INTO state_resources (workspace_id, provider, resource_type, provider_id, first_observed_at, last_observed_at) VALUES ($1, $2, $3, $4, $5, $6)`,
				wsTx, "aws", "s3_bucket", "temp-bucket-tx", now, now)
			if err != nil {
				return err
			}
			// Intentionally abort the transaction
			return customErr
		})

		if !errors.Is(err, customErr) {
			t.Fatalf("expected customErr from transaction, got %v", err)
		}

		// Assert that the resource was not committed
		resources, err := pgStore.ListResources(ctx, wsTx)
		if err != nil {
			t.Fatalf("ListResources failed: %v", err)
		}
		if len(resources) != 0 {
			t.Fatalf("Transaction rollback failed: expected 0 resources, got %d", len(resources))
		}
	})

	// -----------------------------------------------------------------------
	// 6. Reconstruction & Materialization into Live Rust Core Engine
	// -----------------------------------------------------------------------
	t.Run("MaterializationToRustCoreEngine", func(t *testing.T) {
		client := startProdServer(t)

		wsMat := "ws-pg-materialize-test"
		t.Cleanup(func() {
			_ = pgStore.DeleteWorkspaceState(context.Background(), wsMat)
		})

		// Insert topology in Postgres: service-a -[DEPENDS_ON]-> service-b
		serviceA := state.ResourceIdentity{Provider: "kubernetes", ResourceType: "service", ProviderID: "cluster-pg/default/service-a"}
		serviceB := state.ResourceIdentity{Provider: "kubernetes", ResourceType: "service", ProviderID: "cluster-pg/default/service-b"}

		resA := state.Resource{WorkspaceID: wsMat, Identity: serviceA, FirstObservedAt: now, LastObservedAt: now}
		resB := state.Resource{WorkspaceID: wsMat, Identity: serviceB, FirstObservedAt: now, LastObservedAt: now}
		if err := pgStore.SaveResources(ctx, wsMat, []state.Resource{resA, resB}); err != nil {
			t.Fatalf("SaveResources failed: %v", err)
		}

		matEvID := "99999999-9999-9999-9999-999999999999"
		matEv := state.Evidence{
			WorkspaceID:     wsMat,
			ID:              matEvID,
			Source:          state.EvidenceSource{Provider: "kubernetes", Collector: "k8s-runtime"},
			ObservedAt:      now,
			ObservationType: "RUNTIME_CONNECTION",
			Subject:         serviceA,
			Data:            []byte(`{"target": "cluster-pg/default/service-b"}`),
		}
		if err := pgStore.SaveEvidence(ctx, wsMat, []state.Evidence{matEv}); err != nil {
			t.Fatalf("SaveEvidence failed: %v", err)
		}

		matRel := state.Relationship{
			WorkspaceID:     wsMat,
			Source:          serviceA,
			Target:          serviceB,
			Category:        "Invocation",
			Kind:            "CALLS",
			FirstObservedAt: now,
			LastObservedAt:  now,
		}
		if err := pgStore.SaveRelationships(ctx, wsMat, []state.Relationship{matRel}); err != nil {
			t.Fatalf("SaveRelationships failed: %v", err)
		}

		matProv := state.ProvenanceAssociation{
			WorkspaceID:  wsMat,
			Relationship: matRel.Key(),
			EvidenceID:   matEvID,
		}
		if err := pgStore.SaveProvenance(ctx, wsMat, []state.ProvenanceAssociation{matProv}); err != nil {
			t.Fatalf("SaveProvenance failed: %v", err)
		}

		// Materialize from PostgreSQL into Rust Core Engine
		mat := state.NewMaterializer(pgStore, client, logger)
		resp, err := mat.Materialize(ctx, wsMat)
		if err != nil {
			t.Fatalf("Materialize from PostgreSQL failed: %v", err)
		}
		if resp.RelationshipsLoaded != 1 || resp.EvidenceLoaded != 1 || resp.ProvenanceLinksLoaded != 1 {
			t.Fatalf("unexpected Materialize response: %+v", resp)
		}

		// Execute impact query on Rust Core Engine
		answerSvc := answer.NewService(client, logger)
		ans, err := answerSvc.AnalyzeImpact(ctx, answer.ImpactRequest{
			WorkspaceID: wsMat,
			Target: &answer.ResourceIdentity{
				Provider:     serviceB.Provider,
				ResourceType: serviceB.ResourceType,
				ProviderID:   serviceB.ProviderID,
			},
			Direction: "incoming",
			MaxDepth:  5,
		})
		if err != nil {
			t.Fatalf("AnalyzeImpact failed: %v", err)
		}

		if ans.Summary.ImpactedCount != 1 {
			t.Errorf("expected 1 impacted resource, got %d", ans.Summary.ImpactedCount)
		}
		if len(ans.Relationships) != 1 || ans.Relationships[0].State != "supported" {
			t.Errorf("expected 1 supported relationship, got %+v", ans.Relationships)
		}
		if len(ans.Evidence) != 1 || ans.Evidence[0].ID != matEvID {
			t.Errorf("expected evidence %s, got %+v", matEvID, ans.Evidence)
		}
	})
}

func TestPostgresStore_ReconcilerLifecycle(t *testing.T) {
	cfg := getTestDatabaseConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	logger := logging.NewJSONLogger(nil, logging.LevelDebug, "pg-reconciler-test")
	db, err := database.New(ctx, cfg, database.WithLogger(logger))
	if err != nil {
		t.Skipf("Failed to initialize database pool: %v; skipping", err)
	}
	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		t.Skipf("PostgreSQL Ping failed: %v; skipping", err)
	}

	if err := state.EnsureSchema(ctx, db); err != nil {
		t.Fatalf("EnsureSchema failed: %v", err)
	}

	pgStore := state.NewPostgresStore(db)
	client := startProdServer(t)

	materializer := state.NewMaterializer(pgStore, client, logger)
	reconciler := state.NewReconciler(pgStore, client, materializer, logger)

	ws := "ws-pg-rec-lifecycle"
	_ = pgStore.DeleteWorkspaceState(ctx, ws)
	defer func() {
		_ = pgStore.DeleteWorkspaceState(ctx, ws)
	}()

	frontendSubj := &corev1.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "pod",
		ProviderId:   "test-cluster/payments/frontend",
	}
	backendSubj := &corev1.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "pod",
		ProviderId:   "test-cluster/payments/backend",
	}

	connEv := makeConnectionEvidence("550e8400-e29b-41d4-a716-446655440071", frontendSubj, "10.0.1.50", 8080)
	mapEv := makeMappingEvidence("550e8400-e29b-41d4-a716-446655440072", backendSubj, "10.0.1.50", 8080)

	// 1. Initial Observation Sweep into PostgreSQL
	batch1 := state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "test-cluster",
		Evidence:    []*corev1.Evidence{connEv, mapEv},
	}
	res1, err := reconciler.Reconcile(ctx, batch1)
	if err != nil {
		t.Fatalf("Cycle 1 Reconcile failed: %v", err)
	}
	if res1.ResourcesCreated != 2 || res1.RelationshipsCreated != 1 || res1.ProvenanceLinksAdded != 2 {
		t.Fatalf("unexpected Cycle 1 result: %+v", res1)
	}

	// 2. Repeated Sweep (Idempotency) into PostgreSQL
	res2, err := reconciler.Reconcile(ctx, batch1)
	if err != nil {
		t.Fatalf("Cycle 2 Reconcile failed: %v", err)
	}
	if res2.ResourcesCreated != 0 || res2.ResourcesUpdated != 2 {
		t.Errorf("expected 0 created, 2 updated on repeat; got %+v", res2)
	}
	if res2.RelationshipsCreated != 0 || res2.RelationshipsUpdated != 1 {
		t.Errorf("expected 0 rel created, 1 rel updated on repeat; got %+v", res2)
	}
	if res2.ResourcesTotal != 2 || res2.RelationshipsTotal != 1 {
		t.Errorf("expected 2 total resources, 1 total relationship; got %+v", res2)
	}

	// 3. Partial Gap Sweep (backend omitted) into PostgreSQL
	// CRITICAL: backend and relationship must NOT be deleted!
	connEvLater := makeConnectionEvidence("550e8400-e29b-41d4-a716-446655440073", frontendSubj, "10.0.1.50", 8080)
	batch3 := state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "test-cluster",
		Evidence:    []*corev1.Evidence{connEvLater},
	}
	res3, err := reconciler.ReconcileAndMaterialize(ctx, batch3)
	if err != nil {
		t.Fatalf("Cycle 3 ReconcileAndMaterialize failed: %v", err)
	}
	if res3.ResourcesCreated != 0 || res3.ResourcesUpdated != 1 {
		t.Errorf("expected 0 created, 1 updated on partial sweep; got %+v", res3)
	}
	if res3.ResourcesTotal != 2 || res3.RelationshipsTotal != 1 {
		t.Errorf("CRITICAL REGRESSION: unobserved resources or relationships were deleted from PostgreSQL! got %+v", res3)
	}

	// Verify in database that backend is still present
	backendResource, err := pgStore.GetResource(ctx, ws, state.ResourceIdentity{
		Provider:     backendSubj.Provider,
		ResourceType: backendSubj.ResourceType,
		ProviderID:   backendSubj.ProviderId,
	})
	if err != nil {
		t.Fatalf("backend resource was deleted from PostgreSQL: %v", err)
	}
	if backendResource == nil {
		t.Fatalf("backend resource must not be nil in PostgreSQL")
	}

	// 4. Verify Rust Core Engine impact remains intact after materialization
	answerSvc := answer.NewService(client, logger)
	ans, err := answerSvc.AnalyzeImpact(ctx, answer.ImpactRequest{
		WorkspaceID: ws,
		Target: &answer.ResourceIdentity{
			Provider:     backendSubj.Provider,
			ResourceType: backendSubj.ResourceType,
			ProviderID:   backendSubj.ProviderId,
		},
		Direction: "incoming",
		MaxDepth:  5,
	})
	if err != nil {
		t.Fatalf("AnalyzeImpact failed: %v", err)
	}
	if ans.Summary.ImpactedCount != 1 {
		t.Errorf("expected 1 impacted resource (frontend) in Rust Core Engine, got %d", ans.Summary.ImpactedCount)
	}
}

func TestPostgresStore_PartialCollectionSafety(t *testing.T) {
	cfg := getTestDatabaseConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	logger := logging.NewJSONLogger(nil, logging.LevelDebug, "pg-partial-safety-test")
	db, err := database.New(ctx, cfg, database.WithLogger(logger))
	if err != nil {
		t.Skipf("Failed to initialize database pool: %v; skipping", err)
	}
	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		t.Skipf("PostgreSQL Ping failed: %v; skipping", err)
	}

	if err := state.EnsureSchema(ctx, db); err != nil {
		t.Fatalf("EnsureSchema failed: %v", err)
	}

	pgStore := state.NewPostgresStore(db)
	client := startProdServer(t)

	materializer := state.NewMaterializer(pgStore, client, logger)
	reconciler := state.NewReconciler(pgStore, client, materializer, logger)

	ws := "ws-pg-partial-safety"
	_ = pgStore.DeleteWorkspaceState(ctx, ws)
	defer func() {
		_ = pgStore.DeleteWorkspaceState(context.Background(), ws)
	}()

	frontendSubj := &corev1.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "pod",
		ProviderId:   "test-cluster/payments/frontend",
	}
	backendSubj := &corev1.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "pod",
		ProviderId:   "test-cluster/payments/backend",
	}
	dbSubj := &corev1.ResourceIdentity{
		Provider:     "aws",
		ResourceType: "rds_instance",
		ProviderId:   "payments/database",
	}

	// Initial sweep with 3 resources and 2 relationships:
	// frontend -[CALLS]-> backend -[CALLS]-> db
	ev101 := makeConnectionEvidence("550e8400-e29b-41d4-a716-446655440101", frontendSubj, "10.0.1.50", 8080)
	ev102 := makeMappingEvidence("550e8400-e29b-41d4-a716-446655440102", backendSubj, "10.0.1.50", 8080)
	ev103 := makeConnectionEvidence("550e8400-e29b-41d4-a716-446655440103", backendSubj, "10.0.2.10", 5432)
	ev104 := makeMappingEvidence("550e8400-e29b-41d4-a716-446655440104", dbSubj, "10.0.2.10", 5432)

	batch1 := state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "test-cluster",
		Evidence:    []*corev1.Evidence{ev101, ev102, ev103, ev104},
	}
	res1, err := reconciler.ReconcileAndMaterialize(ctx, batch1)
	if err != nil {
		t.Fatalf("Cycle 1 ReconcileAndMaterialize failed: %v", err)
	}
	if res1.ResourcesCreated != 3 || res1.RelationshipsCreated != 2 || res1.ProvenanceLinksAdded != 4 {
		t.Fatalf("unexpected Cycle 1 result: %+v", res1)
	}

	// Verify Rust Core Engine impact before partial sweep
	answerSvc := answer.NewService(client, logger)
	ansInitial, err := answerSvc.AnalyzeImpact(ctx, answer.ImpactRequest{
		WorkspaceID: ws,
		Target: &answer.ResourceIdentity{
			Provider:     dbSubj.Provider,
			ResourceType: dbSubj.ResourceType,
			ProviderID:   dbSubj.ProviderId,
		},
		Direction: "incoming",
		MaxDepth:  5,
	})
	if err != nil {
		t.Fatalf("AnalyzeImpact failed on initial state: %v", err)
	}
	if ansInitial.Summary.ImpactedCount != 2 {
		t.Errorf("expected 2 impacted resources (frontend and backend) on db, got %d", ansInitial.Summary.ImpactedCount)
	}

	// Step 2: Run a partial collection that ONLY observes frontend.
	// Backend and Database are COMPLETELY OMITTED from the collection batch.
	ev105 := makeConnectionEvidence("550e8400-e29b-41d4-a716-446655440105", frontendSubj, "10.0.1.50", 8080)
	batch2 := state.ObservationBatch{
		WorkspaceID: ws,
		ClusterID:   "test-cluster",
		Evidence:    []*corev1.Evidence{ev105},
	}
	res2, err := reconciler.ReconcileAndMaterialize(ctx, batch2)
	if err != nil {
		t.Fatalf("Cycle 2 Partial ReconcileAndMaterialize failed: %v", err)
	}

	// 1. Valid observed updates applied
	if res2.ResourcesCreated != 0 || res2.ResourcesUpdated != 1 {
		t.Errorf("expected 0 created, 1 updated (frontend); got %+v", res2)
	}

	// 2. Authoritative PostgreSQL totals preserved (missing observations do NOT delete)
	if res2.ResourcesTotal != 3 {
		t.Errorf("expected 3 total resources preserved in PG, got %d", res2.ResourcesTotal)
	}
	if res2.RelationshipsTotal != 2 {
		t.Errorf("expected 2 total relationships preserved in PG, got %d", res2.RelationshipsTotal)
	}

	// 3. Verify in PostgreSQL that omitted resources remain intact
	backendRes, err := pgStore.GetResource(ctx, ws, state.ResourceIdentity{
		Provider:     backendSubj.Provider,
		ResourceType: backendSubj.ResourceType,
		ProviderID:   backendSubj.ProviderId,
	})
	if err != nil || backendRes == nil {
		t.Fatalf("omitted backend resource was deleted or deactivated: %v", err)
	}

	dbRes, err := pgStore.GetResource(ctx, ws, state.ResourceIdentity{
		Provider:     dbSubj.Provider,
		ResourceType: dbSubj.ResourceType,
		ProviderID:   dbSubj.ProviderId,
	})
	if err != nil || dbRes == nil {
		t.Fatalf("omitted db resource was deleted or deactivated: %v", err)
	}

	// 4. Verify in PostgreSQL that omitted relationships remain intact
	rels, err := pgStore.ListRelationships(ctx, ws)
	if err != nil {
		t.Fatalf("ListRelationships failed: %v", err)
	}
	if len(rels) != 2 {
		t.Fatalf("expected 2 relationships intact in PG, got %d", len(rels))
	}

	// 5. Verify historical evidence remains intact (all 4 original + 1 new = 5)
	evList, err := pgStore.ListEvidence(ctx, ws)
	if err != nil {
		t.Fatalf("ListEvidence failed: %v", err)
	}
	if len(evList) != 5 {
		t.Fatalf("historical evidence deleted! expected 5 evidence records in PG, got %d", len(evList))
	}

	// 6. Verify materialization reflects the authoritative PostgreSQL state
	// The materialized graph in Rust must still contain the full transitive chain:
	// frontend -> backend -> database
	ansAfterPartial, err := answerSvc.AnalyzeImpact(ctx, answer.ImpactRequest{
		WorkspaceID: ws,
		Target: &answer.ResourceIdentity{
			Provider:     dbSubj.Provider,
			ResourceType: dbSubj.ResourceType,
			ProviderID:   dbSubj.ProviderId,
		},
		Direction: "incoming",
		MaxDepth:  5,
	})
	if err != nil {
		t.Fatalf("AnalyzeImpact after partial sweep failed: %v", err)
	}
	if ansAfterPartial.Summary.ImpactedCount != 2 {
		t.Errorf("expected 2 impacted resources preserved in Rust Core Engine, got %d", ansAfterPartial.Summary.ImpactedCount)
	}

	ansBackend, err := answerSvc.AnalyzeImpact(ctx, answer.ImpactRequest{
		WorkspaceID: ws,
		Target: &answer.ResourceIdentity{
			Provider:     backendSubj.Provider,
			ResourceType: backendSubj.ResourceType,
			ProviderID:   backendSubj.ProviderId,
		},
		Direction: "incoming",
		MaxDepth:  5,
	})
	if err != nil {
		t.Fatalf("AnalyzeImpact on backend after partial sweep failed: %v", err)
	}
	if ansBackend.Summary.ImpactedCount != 1 {
		t.Errorf("expected 1 impacted resource (frontend) on backend in Rust Core Engine, got %d", ansBackend.Summary.ImpactedCount)
	}
}

func TestPostgresStore_ListWorkspaces(t *testing.T) {
	cfg := getTestDatabaseConfig(t)
	ctx := context.Background()

	db, err := database.New(ctx, cfg)
	if err != nil {
		t.Fatalf("database.New failed: %v", err)
	}
	defer db.Close()

	if err := state.EnsureSchema(ctx, db); err != nil {
		t.Fatalf("EnsureSchema failed: %v", err)
	}

	pgStore := state.NewPostgresStore(db)

	ws1 := "test-list-ws-1"
	ws2 := "test-list-ws-2"
	ws3 := "test-list-ws-3"

	// Cleanup before & after
	_ = pgStore.DeleteWorkspaceState(ctx, ws1)
	_ = pgStore.DeleteWorkspaceState(ctx, ws2)
	_ = pgStore.DeleteWorkspaceState(ctx, ws3)
	defer func() {
		_ = pgStore.DeleteWorkspaceState(ctx, ws1)
		_ = pgStore.DeleteWorkspaceState(ctx, ws2)
		_ = pgStore.DeleteWorkspaceState(ctx, ws3)
	}()

	now := time.Now().UTC()
	err = pgStore.SaveResources(ctx, ws2, []state.Resource{
		{
			Identity:        state.ResourceIdentity{Provider: "k8s", ResourceType: "pod", ProviderID: "pod-2"},
			FirstObservedAt: now,
			LastObservedAt:  now,
		},
	})
	if err != nil {
		t.Fatalf("SaveResources failed: %v", err)
	}

	err = pgStore.SaveRelationships(ctx, ws1, []state.Relationship{
		{
			Source:   state.ResourceIdentity{Provider: "k8s", ResourceType: "pod", ProviderID: "pod-1"},
			Target:   state.ResourceIdentity{Provider: "k8s", ResourceType: "svc", ProviderID: "svc-1"},
			Kind:     "DEPENDS_ON",
			Category: "Dependency",
		},
	})
	if err != nil {
		t.Fatalf("SaveRelationships failed: %v", err)
	}

	// ws3 has evidence only (no resources, no relationships)
	err = pgStore.SaveEvidence(ctx, ws3, []state.Evidence{
		{
			ID:              "ev-ws3-1",
			Source:          state.EvidenceSource{Provider: "k8s", Collector: "runtime"},
			ObservedAt:      now,
			ObservationType: "RUNTIME_OBSERVATION",
			Subject:         state.ResourceIdentity{Provider: "k8s", ResourceType: "pod", ProviderID: "pod-3"},
			Data:            []byte("{}"),
		},
	})
	if err != nil {
		t.Fatalf("SaveEvidence failed: %v", err)
	}

	workspaces, err := pgStore.ListWorkspaces(ctx)
	if err != nil {
		t.Fatalf("ListWorkspaces failed: %v", err)
	}

	// Must contain test-list-ws-1, test-list-ws-2, test-list-ws-3 in sorted order
	found1, found2, found3 := -1, -1, -1
	for i, w := range workspaces {
		if w == ws1 {
			found1 = i
		}
		if w == ws2 {
			found2 = i
		}
		if w == ws3 {
			found3 = i
		}
	}

	if found1 == -1 {
		t.Errorf("expected %s in workspaces: %v", ws1, workspaces)
	}
	if found2 == -1 {
		t.Errorf("expected %s in workspaces: %v", ws2, workspaces)
	}
	if found3 == -1 {
		t.Errorf("expected %s in workspaces (evidence-only): %v", ws3, workspaces)
	}
	if found1 != -1 && found2 != -1 && found3 != -1 {
		if !(found1 < found2 && found2 < found3) {
			t.Errorf("expected %s < %s < %s in ordered workspaces, got indices %d, %d, %d", ws1, ws2, ws3, found1, found2, found3)
		}
	}
}

func TestPostgresStore_RecoveryInvariantsAndAudit(t *testing.T) {
	cfg := getTestDatabaseConfig(t)
	ctx := context.Background()

	db, err := database.New(ctx, cfg)
	if err != nil {
		t.Fatalf("database.New failed: %v", err)
	}
	defer db.Close()

	if err := state.EnsureSchema(ctx, db); err != nil {
		t.Fatalf("EnsureSchema failed: %v", err)
	}

	pgStore := state.NewPostgresStore(db)

	ws := "test-recovery-invariants-pg"
	_ = pgStore.DeleteWorkspaceState(ctx, ws)
	defer func() {
		_ = pgStore.DeleteWorkspaceState(context.Background(), ws)
		_, _ = db.Exec(context.Background(), "DELETE FROM state_workspace_recovery WHERE workspace_id = $1", ws)
		_, _ = db.Exec(context.Background(), "DELETE FROM state_recovery_audit WHERE workspace_id = $1", ws)
		_, _ = db.Exec(context.Background(), "DELETE FROM state_legacy_resources_archive WHERE workspace_id = $1", ws)
		_, _ = db.Exec(context.Background(), "DELETE FROM state_legacy_relationships_archive WHERE workspace_id = $1", ws)
	}()

	now := time.Now().UTC().Truncate(time.Microsecond)

	// 1. Seed legacy K8s resource, non-K8s resource, and evidence
	legacyK8sRes := state.Resource{
		WorkspaceID: ws,
		Identity: state.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "pod",
			ProviderID:   "default/api-server",
		},
		FirstObservedAt: now,
		LastObservedAt:  now,
	}
	nonK8sRes := state.Resource{
		WorkspaceID: ws,
		Identity: state.ResourceIdentity{
			Provider:     "aws",
			ResourceType: "rds_instance",
			ProviderID:   "arn:aws:rds:us-east-1:123456789012:db:prod-db",
		},
		FirstObservedAt: now,
		LastObservedAt:  now,
	}
	if err := pgStore.SaveResources(ctx, ws, []state.Resource{legacyK8sRes, nonK8sRes}); err != nil {
		t.Fatalf("SaveResources failed: %v", err)
	}

	ev1 := state.Evidence{
		WorkspaceID: ws,
		ID:          "ev-k8s-hist",
		Source:      state.EvidenceSource{Provider: "kubernetes", Collector: "k8s-watcher"},
		ObservedAt:  now,
		Subject:     legacyK8sRes.Identity,
		Data:        []byte(`{"status":"running"}`),
	}
	ev2 := state.Evidence{
		WorkspaceID: ws,
		ID:          "ev-aws-hist",
		Source:      state.EvidenceSource{Provider: "aws", Collector: "aws-inventory"},
		ObservedAt:  now,
		Subject:     nonK8sRes.Identity,
		Data:        []byte(`{"allocated_storage":100}`),
	}
	if err := pgStore.SaveEvidence(ctx, ws, []state.Evidence{ev1, ev2}); err != nil {
		t.Fatalf("SaveEvidence failed: %v", err)
	}

	rel1 := state.Relationship{
		WorkspaceID: ws,
		Source:      legacyK8sRes.Identity,
		Target:      nonK8sRes.Identity,
		Kind:        "CONNECTS_TO",
		Category:    "Network",
	}
	if err := pgStore.SaveRelationships(ctx, ws, []state.Relationship{rel1}); err != nil {
		t.Fatalf("SaveRelationships failed: %v", err)
	}

	// 2. Test Authorization Gate: unauthenticated caller must fail
	unauthReq := state.LegacyRecoveryParams{
		WorkspaceID:          ws,
		ClusterID:            "cluster-prod-1",
		AuthorizeDestructive: true,
		OperatorIdentity:     "operator@company.com",
		Reason:               "Test recovery auth",
	}
	_, err = pgStore.RecoverWorkspaceLegacyState(ctx, unauthReq)
	if !errors.Is(err, state.ErrUnauthenticatedOperator) {
		t.Fatalf("expected ErrUnauthenticatedOperator for unauthenticated context, got: %v", err)
	}

	// Test Authorization Gate: unauthorized caller (RoleMember) must fail
	memberCtx := auth.WithUser(ctx, &auth.User{
		ID:    "user-1",
		Email: "member@company.com",
		Role:  auth.RoleMember,
	})
	_, err = pgStore.RecoverWorkspaceLegacyState(memberCtx, unauthReq)
	if !errors.Is(err, state.ErrForbiddenOperator) {
		t.Fatalf("expected ErrForbiddenOperator for RoleMember, got: %v", err)
	}

	// Test Authorization Gate: spoofed operator identity must fail
	adminCtx := auth.WithUser(ctx, &auth.User{
		ID:    "admin-1",
		Email: "admin@company.com",
		Role:  auth.RoleAdmin,
	})
	spoofedReq := unauthReq
	spoofedReq.OperatorIdentity = "someone-else@company.com"
	_, err = pgStore.RecoverWorkspaceLegacyState(adminCtx, spoofedReq)
	if !errors.Is(err, state.ErrForbiddenOperator) {
		t.Fatalf("expected ErrForbiddenOperator for spoofed OperatorIdentity, got: %v", err)
	}

	// 3. Test Authorized Recovery Execution
	authReq := state.LegacyRecoveryParams{
		WorkspaceID:          ws,
		ClusterID:            "cluster-prod-1",
		AuthorizeDestructive: true,
		OperatorIdentity:     "admin@company.com",
		Reason:               "Legitimate administrative recovery",
		AuthoritativeBatch: &state.AuthoritativeBatch{
			Resources: []state.Resource{
				{
					WorkspaceID: ws,
					Identity: state.ResourceIdentity{
						Provider:     "kubernetes",
						ResourceType: "pod",
						ProviderID:   "cluster-prod-1/default/api-server",
					},
					FirstObservedAt: now,
					LastObservedAt:  now,
				},
			},
			Relationships: []state.Relationship{
				{
					WorkspaceID: ws,
					Source: state.ResourceIdentity{
						Provider:     "kubernetes",
						ResourceType: "pod",
						ProviderID:   "cluster-prod-1/default/api-server",
					},
					Target:   nonK8sRes.Identity,
					Kind:     "CONNECTS_TO",
					Category: "Network",
				},
			},
		},
	}

	recovResult, err := pgStore.RecoverWorkspaceLegacyState(adminCtx, authReq)
	if err != nil {
		t.Fatalf("RecoverWorkspaceLegacyState failed: %v", err)
	}

	if recovResult.LegacyResourcesArchived != 1 {
		t.Errorf("expected 1 archived resource, got %d", recovResult.LegacyResourcesArchived)
	}
	if recovResult.RecollectedResourcesCount != 1 {
		t.Errorf("expected 1 recollected resource, got %d", recovResult.RecollectedResourcesCount)
	}

	// 4. Verify durable state machine table state_workspace_recovery is COMPLETED
	var status string
	err = db.QueryRow(ctx, "SELECT status FROM state_workspace_recovery WHERE workspace_id = $1", ws).Scan(&status)
	if err != nil {
		t.Fatalf("query state_workspace_recovery failed: %v", err)
	}
	if status != "COMPLETED" {
		t.Fatalf("expected state_workspace_recovery status COMPLETED, got %s", status)
	}

	// 5. Verify non-K8s resource and historical evidence are preserved
	activeResources, err := pgStore.ListResources(ctx, ws)
	if err != nil {
		t.Fatalf("ListResources failed: %v", err)
	}
	if len(activeResources) != 2 {
		t.Fatalf("expected 2 active resources (1 canonical k8s, 1 aws), got %d", len(activeResources))
	}
	var hasCanonicalK8s, hasAws bool
	for _, r := range activeResources {
		if r.Identity.Provider == "kubernetes" && r.Identity.ProviderID == "cluster-prod-1/default/api-server" {
			hasCanonicalK8s = true
		}
		if r.Identity.Provider == "aws" && r.Identity.ProviderID == nonK8sRes.Identity.ProviderID {
			hasAws = true
		}
	}
	if !hasCanonicalK8s || !hasAws {
		t.Fatalf("expected active resources to contain canonical k8s and aws, got: %+v", activeResources)
	}

	// Evidence intact
	evList, err := pgStore.ListEvidence(ctx, ws)
	if err != nil {
		t.Fatalf("ListEvidence failed: %v", err)
	}
	if len(evList) != 2 {
		t.Fatalf("expected 2 historical evidence records intact, got %d", len(evList))
	}

	// Archive tables verification
	var archiveResCount int
	err = db.QueryRow(ctx, "SELECT count(*) FROM state_legacy_resources_archive WHERE workspace_id = $1", ws).Scan(&archiveResCount)
	if err != nil || archiveResCount != 1 {
		t.Fatalf("expected 1 record in state_legacy_resources_archive, got %d (err: %v)", archiveResCount, err)
	}

	// Audit table verification: operation_id, failure_category, recollected_resources
	audits, err := pgStore.ListRecoveryAudits(ctx, ws)
	if err != nil {
		t.Fatalf("ListRecoveryAudits failed: %v", err)
	}
	if len(audits) == 0 {
		t.Fatalf("expected audit records in state_recovery_audit")
	}
	lastAudit := audits[len(audits)-1]
	if lastAudit.OperationID == "" {
		t.Errorf("expected non-empty OperationID")
	}
	if lastAudit.OperatorIdentity != "admin@company.com" {
		t.Errorf("expected operator admin@company.com, got %s", lastAudit.OperatorIdentity)
	}
	if lastAudit.RecollectedResourcesCount != 1 {
		t.Errorf("expected RecollectedResourcesCount 1, got %d", lastAudit.RecollectedResourcesCount)
	}

	// 6. Test Fail-Closed behavior when recovery status is FAILED
	_, err = db.Exec(ctx, "UPDATE state_workspace_recovery SET status = 'FAILED' WHERE workspace_id = $1", ws)
	if err != nil {
		t.Fatalf("update state_workspace_recovery to FAILED failed: %v", err)
	}

	// CheckWorkspaceIntegrity must fail closed
	err = pgStore.CheckWorkspaceIntegrity(ctx, ws)
	if !errors.Is(err, state.ErrWorkspaceRecoveryRequired) {
		t.Fatalf("expected ErrWorkspaceRecoveryRequired from CheckWorkspaceIntegrity when recovery failed, got: %v", err)
	}

	// ReconcileAuthoritativeBatch must fail closed
	_, err = pgStore.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
		WorkspaceID: ws,
		ClusterID:   "cluster-prod-1",
		Resources: []state.Resource{
			{
				WorkspaceID: ws,
				Identity: state.ResourceIdentity{
					Provider:     "kubernetes",
					ResourceType: "pod",
					ProviderID:   "cluster-prod-1/default/pod-1",
				},
				FirstObservedAt: now,
				LastObservedAt:  now,
			},
		},
	})
	if !errors.Is(err, state.ErrWorkspaceRecoveryRequired) {
		t.Fatalf("expected ErrWorkspaceRecoveryRequired from ReconcileAuthoritativeBatch when recovery failed, got: %v", err)
	}
}

func TestPostgresStore_ReconciliationRecoveryStateSafety(t *testing.T) {
	cfg := getTestDatabaseConfig(t)
	ctx := context.Background()

	db, err := database.New(ctx, cfg)
	if err != nil {
		t.Fatalf("database.New failed: %v", err)
	}
	defer db.Close()

	if err := state.EnsureSchema(ctx, db); err != nil {
		t.Fatalf("EnsureSchema failed: %v", err)
	}

	pgStore := state.NewPostgresStore(db)

	ws := "test-recov-safety-pg"
	_ = pgStore.DeleteWorkspaceState(ctx, ws)
	defer func() {
		_ = pgStore.DeleteWorkspaceState(context.Background(), ws)
		_, _ = db.Exec(context.Background(), "DELETE FROM state_workspace_recovery WHERE workspace_id = $1", ws)
		_, _ = db.Exec(context.Background(), "DELETE FROM state_recovery_audit WHERE workspace_id = $1", ws)
	}()

	now := time.Now().UTC()
	canonicalPod := state.Resource{
		WorkspaceID: ws,
		Identity: state.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "pod",
			ProviderID:   "cluster-prod-1/default/api-1",
		},
		FirstObservedAt: now,
		LastObservedAt:  now,
	}

	// 1. Clean workspace (no recovery record in state_workspace_recovery): reconciliation succeeds
	_, _ = db.Exec(ctx, "DELETE FROM state_workspace_recovery WHERE workspace_id = $1", ws)
	batchRes, err := pgStore.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
		WorkspaceID: ws,
		ClusterID:   "cluster-prod-1",
		Resources:   []state.Resource{canonicalPod},
	})
	if err != nil {
		t.Fatalf("expected reconciliation to succeed when no recovery record exists, got: %v", err)
	}
	if batchRes == nil || batchRes.ResourcesCreated != 1 {
		t.Errorf("expected 1 resource created, got %+v", batchRes)
	}

	// 2. IN_PROGRESS recovery state: reconciliation must fail closed with ErrWorkspaceRecoveryRequired
	_, err = db.Exec(ctx, `
		INSERT INTO state_workspace_recovery (workspace_id, operation_id, cluster_id, status, operator_identity, error_message, started_at, updated_at)
		VALUES ($1, 'rec-inprog-test', 'cluster-prod-1', 'IN_PROGRESS', 'admin@example.com', '', NOW(), NOW())
		ON CONFLICT (workspace_id) DO UPDATE SET status = 'IN_PROGRESS', error_message = '';
	`, ws)
	if err != nil {
		t.Fatalf("failed to insert IN_PROGRESS recovery record: %v", err)
	}

	_, err = pgStore.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
		WorkspaceID: ws,
		ClusterID:   "cluster-prod-1",
		Resources:   []state.Resource{canonicalPod},
	})
	if !errors.Is(err, state.ErrWorkspaceRecoveryRequired) {
		t.Fatalf("expected ErrWorkspaceRecoveryRequired when recovery status is IN_PROGRESS, got: %v", err)
	}
	if !strings.Contains(err.Error(), "IN_PROGRESS") {
		t.Errorf("expected error message to mention IN_PROGRESS, got: %v", err)
	}

	// 3. FAILED recovery state: reconciliation must fail closed with ErrWorkspaceRecoveryRequired
	_, err = db.Exec(ctx, `
		UPDATE state_workspace_recovery
		SET status = 'FAILED', error_message = 'crashed midway through archival'
		WHERE workspace_id = $1;
	`, ws)
	if err != nil {
		t.Fatalf("failed to update recovery record to FAILED: %v", err)
	}

	_, err = pgStore.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
		WorkspaceID: ws,
		ClusterID:   "cluster-prod-1",
		Resources:   []state.Resource{canonicalPod},
	})
	if !errors.Is(err, state.ErrWorkspaceRecoveryRequired) {
		t.Fatalf("expected ErrWorkspaceRecoveryRequired when recovery status is FAILED, got: %v", err)
	}
	if !strings.Contains(err.Error(), "crashed midway through archival") {
		t.Errorf("expected error message to include stored error, got: %v", err)
	}

	// 4. COMPLETED recovery state: normal reconciliation succeeds
	_, err = db.Exec(ctx, `
		UPDATE state_workspace_recovery
		SET status = 'COMPLETED', error_message = ''
		WHERE workspace_id = $1;
	`, ws)
	if err != nil {
		t.Fatalf("failed to update recovery record to COMPLETED: %v", err)
	}

	batchRes, err = pgStore.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
		WorkspaceID: ws,
		ClusterID:   "cluster-prod-1",
		Resources:   []state.Resource{canonicalPod},
	})
	if err != nil {
		t.Fatalf("expected reconciliation to succeed when recovery status is COMPLETED, got: %v", err)
	}
	if batchRes == nil {
		t.Fatalf("expected non-nil batch result on COMPLETED reconciliation")
	}

	// 5. Database failure (canceled context): fails closed without continuing mutations
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel() // canceled immediately
	_, err = pgStore.ReconcileAuthoritativeBatch(canceledCtx, state.AuthoritativeBatchParams{
		WorkspaceID: ws,
		ClusterID:   "cluster-prod-1",
		Resources:   []state.Resource{canonicalPod},
	})
	if err == nil {
		t.Fatalf("expected error when context is canceled during reconciliation")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected error to preserve context.Canceled cause, got: %v", err)
	}

	// 6. Empty status: fails closed with ErrWorkspaceRecoveryRequired
	// We drop the CHECK constraint temporarily to simulate legacy/DBA/corrupted data
	_, _ = db.Exec(ctx, "ALTER TABLE state_workspace_recovery DROP CONSTRAINT IF EXISTS chk_state_workspace_recovery_status")
	defer func() {
		_ = state.EnsureSchema(context.Background(), db)
	}()

	_, err = db.Exec(ctx, `
		UPDATE state_workspace_recovery
		SET status = '', error_message = ''
		WHERE workspace_id = $1;
	`, ws)
	if err != nil {
		t.Fatalf("failed to update recovery record to empty status: %v", err)
	}

	_, err = pgStore.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
		WorkspaceID: ws,
		ClusterID:   "cluster-prod-1",
		Resources:   []state.Resource{canonicalPod},
	})
	if !errors.Is(err, state.ErrWorkspaceRecoveryRequired) {
		t.Fatalf("expected ErrWorkspaceRecoveryRequired for empty status, got: %v", err)
	}

	// 7. Unknown status: fails closed with ErrWorkspaceRecoveryRequired
	_, err = db.Exec(ctx, `
		UPDATE state_workspace_recovery
		SET status = 'UNKNOWN_STATE_VAL', error_message = ''
		WHERE workspace_id = $1;
	`, ws)
	if err != nil {
		t.Fatalf("failed to update recovery record to unknown status: %v", err)
	}

	_, err = pgStore.ReconcileAuthoritativeBatch(ctx, state.AuthoritativeBatchParams{
		WorkspaceID: ws,
		ClusterID:   "cluster-prod-1",
		Resources:   []state.Resource{canonicalPod},
	})
	if !errors.Is(err, state.ErrWorkspaceRecoveryRequired) {
		t.Fatalf("expected ErrWorkspaceRecoveryRequired for unknown status, got: %v", err)
	}

	// 8. Integrity inspection parity for empty and unknown recovery statuses
	rep, err := pgStore.InspectWorkspaceIntegrity(ctx, ws)
	if err != nil {
		t.Fatalf("unexpected error inspecting integrity: %v", err)
	}
	if rep.IsClean {
		t.Errorf("expected IsClean=false when status is unknown, got true")
	}
	if !rep.RecoveryRequired {
		t.Errorf("expected RecoveryRequired=true when status is unknown, got false")
	}

	// 9. Structural bypass protection: public ReconcileAuthoritativeBatch resets bypass
	_, _ = db.Exec(ctx, `UPDATE state_workspace_recovery SET status = 'FAILED', error_message = 'failed' WHERE workspace_id = $1`, ws)
	bParamsWithBypass := state.WithRecoveryBypassForTesting(state.AuthoritativeBatchParams{
		WorkspaceID: ws,
		ClusterID:   "cluster-prod-1",
		Resources:   []state.Resource{canonicalPod},
	})
	_, err = pgStore.ReconcileAuthoritativeBatch(ctx, bParamsWithBypass)
	if !errors.Is(err, state.ErrWorkspaceRecoveryRequired) {
		t.Fatalf("expected public ReconcileAuthoritativeBatch to enforce guard even if bypass was set, got: %v", err)
	}
}

func TestPostgresStore_RecoveryStatusWriteFailures(t *testing.T) {
	cfg := getTestDatabaseConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	logger := logging.NewJSONLogger(nil, logging.LevelDebug, "pg-status-err-test")
	db, err := database.New(ctx, cfg, database.WithLogger(logger))
	if err != nil {
		t.Skipf("Failed to initialize database pool: %v; skipping", err)
	}
	defer db.Close()

	if err := state.EnsureSchema(ctx, db); err != nil {
		t.Fatalf("EnsureSchema failed: %v", err)
	}

	pgStore := state.NewPostgresStore(db)
	ws := "test-status-write-err-ws"
	defer func() {
		_ = pgStore.DeleteWorkspaceState(context.Background(), ws)
		_, _ = db.Exec(context.Background(), "DELETE FROM state_workspace_recovery WHERE workspace_id = $1", ws)
		_, _ = db.Exec(context.Background(), "DELETE FROM state_recovery_audit WHERE workspace_id = $1", ws)
		_, _ = db.Exec(context.Background(), "DROP TRIGGER IF EXISTS trig_fail_status ON state_workspace_recovery")
		_, _ = db.Exec(context.Background(), "DROP FUNCTION IF EXISTS fail_recovery_status_write()")
	}()

	// 1. IN_PROGRESS write failure
	_, _ = db.Exec(ctx, `
		CREATE OR REPLACE FUNCTION fail_recovery_status_write() RETURNS TRIGGER AS $$
		BEGIN
			IF NEW.status = 'IN_PROGRESS' THEN
				RAISE EXCEPTION 'simulated IN_PROGRESS write error';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
		DROP TRIGGER IF EXISTS trig_fail_status ON state_workspace_recovery;
		CREATE TRIGGER trig_fail_status BEFORE INSERT OR UPDATE ON state_workspace_recovery
		FOR EACH ROW EXECUTE FUNCTION fail_recovery_status_write();
	`)

	adminCtx := auth.WithUser(ctx, &auth.User{Role: auth.RoleAdmin, ID: "admin@example.com", Email: "admin@example.com"})
	recParams := state.LegacyRecoveryParams{
		WorkspaceID:          ws,
		ClusterID:            "cluster-prod-1",
		AuthorizeDestructive: true,
		OperatorIdentity:     "admin@example.com",
		Reason:               "testing in_progress failure",
	}

	res, err := pgStore.RecoverWorkspaceLegacyState(adminCtx, recParams)
	if err == nil {
		t.Fatalf("expected error when IN_PROGRESS write fails, got nil")
	}
	if res != nil {
		t.Fatalf("expected nil result when IN_PROGRESS write fails")
	}
	if !strings.Contains(err.Error(), "IN_PROGRESS") || !strings.Contains(err.Error(), "simulated IN_PROGRESS write error") {
		t.Errorf("expected error to mention IN_PROGRESS and preserve underlying cause, got: %v", err)
	}

	// Verify audit was recorded as FAILED
	audits, aErr := pgStore.ListRecoveryAudits(ctx, ws)
	if aErr != nil || len(audits) == 0 {
		t.Fatalf("expected audit record after IN_PROGRESS write failure, got audits=%v, err=%v", audits, aErr)
	}
	if audits[len(audits)-1].Status != "FAILED" || audits[len(audits)-1].FailureCategory != "RECOVERY_EXECUTION" {
		t.Errorf("unexpected audit: %+v", audits[len(audits)-1])
	}

	// 2. FAILED write failure
	_, _ = db.Exec(ctx, `
		CREATE OR REPLACE FUNCTION fail_recovery_status_write() RETURNS TRIGGER AS $$
		BEGIN
			IF NEW.status = 'FAILED' THEN
				RAISE EXCEPTION 'simulated FAILED write error';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
	`)

	// Cause main recovery tx to fail by providing a recollection batch that fails validation
	badParams := state.LegacyRecoveryParams{
		WorkspaceID:          ws,
		ClusterID:            "cluster-prod-1",
		AuthorizeDestructive: true,
		OperatorIdentity:     "admin@example.com",
		Reason:               "testing failed write error",
		AuthoritativeBatch: &state.AuthoritativeBatchParams{
			WorkspaceID: ws,
			ClusterID:   "cluster-prod-1",
			Resources: []state.Resource{
				{
					WorkspaceID: ws,
					Identity: state.ResourceIdentity{
						Provider:     "kubernetes",
						ResourceType: "pod",
						ProviderID:   "bad-identity-not-canonical",
					},
					FirstObservedAt: time.Now().UTC(),
					LastObservedAt:  time.Now().UTC(),
				},
			},
		},
	}

	_, err = pgStore.RecoverWorkspaceLegacyState(adminCtx, badParams)
	if err == nil {
		t.Fatalf("expected error when recovery tx and FAILED write fail, got nil")
	}
	if !strings.Contains(err.Error(), "simulated FAILED write error") {
		t.Errorf("expected error to include FAILED write error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "failed to persist FAILED recovery status") {
		t.Errorf("expected error to state failed to persist FAILED recovery status, got: %v", err)
	}

	// 3. COMPLETED write failure
	_, _ = db.Exec(ctx, `
		CREATE OR REPLACE FUNCTION fail_recovery_status_write() RETURNS TRIGGER AS $$
		BEGIN
			IF NEW.status = 'COMPLETED' THEN
				RAISE EXCEPTION 'simulated COMPLETED write error';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
	`)

	goodParams := state.LegacyRecoveryParams{
		WorkspaceID:          ws,
		ClusterID:            "cluster-prod-1",
		AuthorizeDestructive: true,
		OperatorIdentity:     "admin@example.com",
		Reason:               "testing completed write error",
	}

	res, err = pgStore.RecoverWorkspaceLegacyState(adminCtx, goodParams)
	if err == nil {
		t.Fatalf("expected error when COMPLETED write fails, got nil")
	}
	if !strings.Contains(err.Error(), "COMPLETED") || !strings.Contains(err.Error(), "simulated COMPLETED write error") {
		t.Errorf("expected error to mention COMPLETED and preserve underlying cause, got: %v", err)
	}

	// Verify workspace status in DB is NOT marked COMPLETED (fails closed, remains IN_PROGRESS)
	var currStatus string
	_ = db.QueryRow(ctx, "SELECT status FROM state_workspace_recovery WHERE workspace_id = $1", ws).Scan(&currStatus)
	if currStatus == "COMPLETED" {
		t.Fatalf("workspace status must NOT be marked COMPLETED when COMPLETED write failed!")
	}
}

func TestPostgresStore_RecoveryStartupReconciliationRace(t *testing.T) {
	cfg := getTestDatabaseConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	logger := logging.NewJSONLogger(nil, logging.LevelDebug, "pg-race-test")
	db, err := database.New(ctx, cfg, database.WithLogger(logger))
	if err != nil {
		t.Skipf("Failed to initialize database pool: %v; skipping", err)
	}
	defer db.Close()

	if err := state.EnsureSchema(ctx, db); err != nil {
		t.Fatalf("EnsureSchema failed: %v", err)
	}

	pgStore := state.NewPostgresStore(db)
	ws := "test-startup-race-ws"
	defer func() {
		_ = pgStore.DeleteWorkspaceState(context.Background(), ws)
		_, _ = db.Exec(context.Background(), "DELETE FROM state_workspace_recovery WHERE workspace_id = $1", ws)
		_, _ = db.Exec(context.Background(), "DELETE FROM state_recovery_audit WHERE workspace_id = $1", ws)
	}()

	now := time.Now().UTC()
	canonicalPod := state.Resource{
		WorkspaceID: ws,
		Identity: state.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "pod",
			ProviderID:   "prod-cluster/default/app-pod",
		},
		FirstObservedAt: now,
		LastObservedAt:  now,
	}

	// Clean initial state: no recovery row
	_, _ = db.Exec(ctx, "DELETE FROM state_workspace_recovery WHERE workspace_id = $1", ws)

	lockAcquired := make(chan struct{})
	reconcileAttemptStarted := make(chan struct{})
	reconcileDone := make(chan error, 1)

	go func() {
		_ = db.WithConnection(ctx, func(cCtx context.Context, conn database.Connection) error {
			// Recovery Step 2: Acquire session-level advisory lock
			if _, err := conn.Exec(cCtx, "SELECT pg_advisory_lock(hashtext($1))", ws); err != nil {
				return err
			}
			defer func() {
				_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock(hashtext($1))", ws)
			}()

			close(lockAcquired)

			// Wait until reconciliation goroutine has started attempting to reconcile
			select {
			case <-reconcileAttemptStarted:
			case <-time.After(3 * time.Second):
			}

			// Verify that reconciliation cannot complete while Recovery holds the lock!
			select {
			case res := <-reconcileDone:
				t.Errorf("reconciliation completed while recovery held advisory lock! Scenario A race not prevented: res=%v", res)
			case <-time.After(300 * time.Millisecond):
				// Proven: reconciliation is blocked waiting for recovery lock!
			}

			// Recovery Step 3: Write IN_PROGRESS while lock is held
			_, err := conn.Exec(cCtx, `
				INSERT INTO state_workspace_recovery (workspace_id, operation_id, cluster_id, status, operator_identity, error_message, started_at, updated_at)
				VALUES ($1, 'op-race', 'prod-cluster', 'IN_PROGRESS', 'admin@example.com', '', NOW(), NOW())
				ON CONFLICT (workspace_id) DO UPDATE SET status = 'IN_PROGRESS';
			`, ws)
			if err != nil {
				return err
			}

			// Recovery Step 4 & 5: (Simulated recovery work inside lock)
			time.Sleep(100 * time.Millisecond)

			// Recovery Step 6: Mark COMPLETED before releasing lock
			_, err = conn.Exec(cCtx, `
				UPDATE state_workspace_recovery SET status = 'COMPLETED' WHERE workspace_id = $1;
			`, ws)
			return err
		})
	}()

	// Concurrently attempt reconciliation
	go func() {
		<-lockAcquired
		close(reconcileAttemptStarted)
		_, err := pgStore.ReconcileAuthoritativeBatch(context.Background(), state.AuthoritativeBatchParams{
			WorkspaceID: ws,
			ClusterID:   "prod-cluster",
			Resources:   []state.Resource{canonicalPod},
		})
		reconcileDone <- err
	}()

	// Wait for reconciliation to unblock after lock release and finish
	select {
	case err := <-reconcileDone:
		if err != nil {
			t.Fatalf("expected reconciliation to succeed after recovery completed, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for reconciliation to complete after lock release")
	}

	// Verify the canonical resource was written
	stateRes, err := pgStore.GetResource(ctx, ws, canonicalPod.Identity)
	if err != nil || stateRes == nil {
		t.Fatalf("expected resource to be safely persisted, got err=%v", err)
	}
}

func TestPostgresStore_ConcurrentRecoverySerialization(t *testing.T) {
	cfg := getTestDatabaseConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	logger := logging.NewJSONLogger(nil, logging.LevelDebug, "pg-conc-rec-test")
	db, err := database.New(ctx, cfg, database.WithLogger(logger))
	if err != nil {
		t.Skipf("Failed to initialize database pool: %v; skipping", err)
	}
	defer db.Close()

	if err := state.EnsureSchema(ctx, db); err != nil {
		t.Fatalf("EnsureSchema failed: %v", err)
	}

	pgStore := state.NewPostgresStore(db)
	ws := "test-conc-rec-serial-ws"
	defer func() {
		_ = pgStore.DeleteWorkspaceState(context.Background(), ws)
		_, _ = db.Exec(context.Background(), "DELETE FROM state_workspace_recovery WHERE workspace_id = $1", ws)
		_, _ = db.Exec(context.Background(), "DELETE FROM state_recovery_audit WHERE workspace_id = $1", ws)
	}()

	const nRoutines = 4
	var wg sync.WaitGroup
	errs := make([]error, nRoutines)

	for i := 0; i < nRoutines; i++ {
		wg.Add(1)
		idx := i
		go func() {
			defer wg.Done()
			opEmail := fmt.Sprintf("admin-%d@example.com", idx)
			gCtx := auth.WithUser(ctx, &auth.User{Role: auth.RoleAdmin, ID: opEmail, Email: opEmail})
			_, errs[idx] = pgStore.RecoverWorkspaceLegacyState(gCtx, state.LegacyRecoveryParams{
				WorkspaceID:          ws,
				ClusterID:            "cluster-prod-1",
				AuthorizeDestructive: true,
				OperatorIdentity:     opEmail,
				Reason:               fmt.Sprintf("concurrent recovery run %d", idx),
			})
		}()
	}

	wg.Wait()

	for i, e := range errs {
		if e != nil {
			t.Errorf("goroutine %d failed: %v", i, e)
		}
	}

	var status string
	err = db.QueryRow(ctx, "SELECT status FROM state_workspace_recovery WHERE workspace_id = $1", ws).Scan(&status)
	if err != nil || status != "COMPLETED" {
		t.Fatalf("expected final recovery status COMPLETED, got status=%q, err=%v", status, err)
	}

	audits, err := pgStore.ListRecoveryAudits(ctx, ws)
	if err != nil || len(audits) != nRoutines {
		t.Fatalf("expected %d audit records, got %d (err: %v)", nRoutines, len(audits), err)
	}
}

func TestPostgresStore_SchemaConstraintAndMigration(t *testing.T) {
	cfg := getTestDatabaseConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	logger := logging.NewJSONLogger(nil, logging.LevelDebug, "pg-schema-test")
	db, err := database.New(ctx, cfg, database.WithLogger(logger))
	if err != nil {
		t.Skipf("Failed to initialize database pool: %v; skipping", err)
	}
	defer db.Close()

	if err := state.EnsureSchema(ctx, db); err != nil {
		t.Fatalf("EnsureSchema failed: %v", err)
	}

	// 1. Verify constraint blocks inserting unsupported status
	_, err = db.Exec(ctx, `
		INSERT INTO state_workspace_recovery (workspace_id, operation_id, cluster_id, status, operator_identity, error_message, started_at, updated_at)
		VALUES ('ws-invalid-test', 'op-inv', 'c1', 'INVALID_STATUS', 'admin@example.com', '', NOW(), NOW())
		ON CONFLICT (workspace_id) DO NOTHING;
	`)
	if err == nil {
		t.Fatalf("expected database CHECK constraint to reject 'INVALID_STATUS', but INSERT succeeded")
	}

	// 2. Test existing table migration with invalid rows:
	// Drop constraint to simulate pre-migration table state
	_, err = db.Exec(ctx, "ALTER TABLE state_workspace_recovery DROP CONSTRAINT IF EXISTS chk_state_workspace_recovery_status")
	if err != nil {
		t.Fatalf("failed to drop constraint for migration test: %v", err)
	}

	badWS := "ws-mig-invalid-data"
	_, err = db.Exec(ctx, `
		INSERT INTO state_workspace_recovery (workspace_id, operation_id, cluster_id, status, operator_identity, error_message, started_at, updated_at)
		VALUES ($1, 'op-mig', 'c1', 'LEGACY_BOGUS_STATUS', 'admin@example.com', '', NOW(), NOW())
		ON CONFLICT (workspace_id) DO UPDATE SET status = 'LEGACY_BOGUS_STATUS';
	`, badWS)
	if err != nil {
		t.Fatalf("failed to insert invalid status row: %v", err)
	}
	defer func() {
		_, _ = db.Exec(context.Background(), "DELETE FROM state_workspace_recovery WHERE workspace_id = $1", badWS)
		_ = state.EnsureSchema(context.Background(), db)
	}()

	// EnsureSchema MUST FAIL clearly and provide actionable diagnostics
	migErr := state.EnsureSchema(ctx, db)
	if migErr == nil {
		t.Fatalf("expected EnsureSchema to fail when invalid status rows exist, but it succeeded")
	}
	if !strings.Contains(migErr.Error(), badWS) || !strings.Contains(migErr.Error(), "LEGACY_BOGUS_STATUS") {
		t.Errorf("expected migration error to include workspace ID and invalid status, got: %v", migErr)
	}

	// Verify row was NOT deleted or rewritten
	var rowStatus string
	err = db.QueryRow(ctx, "SELECT status FROM state_workspace_recovery WHERE workspace_id = $1", badWS).Scan(&rowStatus)
	if err != nil || rowStatus != "LEGACY_BOGUS_STATUS" {
		t.Fatalf("expected row to be preserved as 'LEGACY_BOGUS_STATUS', got status=%q, err=%v", rowStatus, err)
	}

	// Clean up invalid row
	_, _ = db.Exec(ctx, "DELETE FROM state_workspace_recovery WHERE workspace_id = $1", badWS)

	// EnsureSchema must now succeed and apply constraint
	if err := state.EnsureSchema(ctx, db); err != nil {
		t.Fatalf("EnsureSchema failed after cleaning up invalid row: %v", err)
	}

	// Repeated EnsureSchema must be idempotent
	if err := state.EnsureSchema(ctx, db); err != nil {
		t.Fatalf("repeated EnsureSchema failed: %v", err)
	}
}
