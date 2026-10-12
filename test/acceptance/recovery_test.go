package acceptance_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"testing"
	"time"

	corev1 "github.com/aaradhychinche-alt/WhatBreaks/gen/go/wb/core/v1"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/answer"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/api"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/config"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/coreclient"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/database"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/health"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/logging"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/platform"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestAcceptance_REC02_RustProcessRestart_AutomaticRecovery tests REC-02, REC-03, REC-04, and REC-05
// using a real Rust Core Engine subprocess that is stopped and restarted on the same port.
func TestAcceptance_REC02_RustProcessRestart_AutomaticRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := logging.NewStandardLogger(nil, logging.LevelWarn)

	// 1. Allocate a free address and start the first real Rust process (ms1)
	addr, err := allocateFreeAddr()
	if err != nil {
		t.Fatalf("failed to allocate free address: %v", err)
	}

	ms1 := startManagedServer(t, addr)
	defer ms1.Stop()

	client, err := coreclient.Connect(ctx, addr)
	if err != nil {
		t.Fatalf("failed to connect to Rust core engine: %v", err)
	}
	defer client.Close()

	// 2. Prepare store with persisted state for ws-alpha
	store := state.NewMemoryStore()
	wsAlpha := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	wsBeta := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	wsEmpty := "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"

	// Seed ws-alpha: svc-b -> svc-a (svc-b DEPENDS_ON svc-a)
	now := time.Now().UTC()
	resA := state.Resource{
		WorkspaceID: wsAlpha,
		Identity: state.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "service",
			ProviderID:   "cluster-1/prod/svc-a",
		},
		FirstObservedAt: now,
		LastObservedAt:  now,
	}
	resB := state.Resource{
		WorkspaceID: wsAlpha,
		Identity: state.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "service",
			ProviderID:   "cluster-1/prod/svc-b",
		},
		FirstObservedAt: now,
		LastObservedAt:  now,
	}
	relBA := state.Relationship{
		WorkspaceID:     wsAlpha,
		Source:          resB.Identity,
		Target:          resA.Identity,
		Kind:            "DEPENDS_ON",
		Category:        "structural",
		FirstObservedAt: now,
		LastObservedAt:  now,
	}
	ev := state.Evidence{
		WorkspaceID: wsAlpha,
		ID:          "11111111-1111-1111-1111-111111111111",
		Source: state.EvidenceSource{
			Provider:  "kubernetes",
			Collector: "k8s-collector",
		},
		ObservedAt:      now,
		ObservationType: "service_spec",
		Subject:         resB.Identity,
		Data:            []byte(`{}`),
	}
	prov := state.ProvenanceAssociation{
		WorkspaceID:  wsAlpha,
		Relationship: relBA.Key(),
		EvidenceID:   ev.ID,
	}

	if err := store.SaveResources(ctx, wsAlpha, []state.Resource{resA, resB}); err != nil {
		t.Fatalf("failed to save resources: %v", err)
	}
	if err := store.SaveRelationships(ctx, wsAlpha, []state.Relationship{relBA}); err != nil {
		t.Fatalf("failed to save relationships: %v", err)
	}
	if err := store.SaveEvidence(ctx, wsAlpha, []state.Evidence{ev}); err != nil {
		t.Fatalf("failed to save evidence: %v", err)
	}
	if err := store.SaveProvenance(ctx, wsAlpha, []state.ProvenanceAssociation{prov}); err != nil {
		t.Fatalf("failed to save provenance: %v", err)
	}

	// Setup RecoveryManager & Materializer
	healthState := health.NewState()
	materializer := state.NewMaterializer(store, client, logger)
	recoveryMgr := state.NewRecoveryManager(store, materializer, client, healthState, logger)

	// Materialize wsAlpha and wsEmpty
	if err := recoveryMgr.RecoverAll(ctx); err != nil {
		t.Fatalf("initial RecoverAll failed: %v", err)
	}

	// Materialize wsEmpty as an explicitly empty workspace in Rust
	_, err = client.LoadState(ctx, &corev1.LoadStateRequest{
		WorkspaceId: wsEmpty,
	})
	if err != nil {
		t.Fatalf("failed to materialize empty workspace: %v", err)
	}

	// 3. Verify REC-04: Materialized empty workspace returns 200 with 0 impacted
	targetEmpty := &corev1.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "service",
		ProviderId:   "non-existent",
	}
	respEmpty, err := client.AnalyzeImpact(ctx, &corev1.AnalyzeImpactRequest{
		Target:      targetEmpty,
		Direction:   "incoming",
		MaxDepth:    2,
		WorkspaceId: wsEmpty,
		ProposedChange: &corev1.ProposedChange{
			ChangeType: corev1.ChangeType_CHANGE_TYPE_UPDATE,
		},
	})
	if err != nil {
		t.Fatalf("REC-04 failed: expected success on materialized empty workspace, got: %v", err)
	}
	if respEmpty.Summary.ImpactedCount != 0 {
		t.Errorf("REC-04 failed: expected 0 impacted, got %d", respEmpty.Summary.ImpactedCount)
	}

	// 4. Verify REC-03: Unmaterialized workspace returns FAILED_PRECONDITION
	_, err = client.AnalyzeImpact(ctx, &corev1.AnalyzeImpactRequest{
		Target:      targetEmpty,
		Direction:   "incoming",
		MaxDepth:    2,
		WorkspaceId: wsBeta,
		ProposedChange: &corev1.ProposedChange{
			ChangeType: corev1.ChangeType_CHANGE_TYPE_UPDATE,
		},
	})
	if err == nil {
		t.Fatal("REC-03 failed: expected FAILED_PRECONDITION for unmaterialized workspace, got success")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		t.Fatalf("REC-03 failed: expected FAILED_PRECONDITION, got %v (code %v)", err, st.Code())
	}

	// 5. Verify ws-alpha produces impact on ms1
	targetA := &corev1.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "service",
		ProviderId:   "cluster-1/prod/svc-a",
	}
	respA, err := client.AnalyzeImpact(ctx, &corev1.AnalyzeImpactRequest{
		Target:      targetA,
		Direction:   "incoming",
		MaxDepth:    2,
		WorkspaceId: wsAlpha,
		ProposedChange: &corev1.ProposedChange{
			ChangeType: corev1.ChangeType_CHANGE_TYPE_UPDATE,
		},
	})
	if err != nil {
		t.Fatalf("AnalyzeImpact on ms1 failed: %v", err)
	}
	if respA.Summary.ImpactedCount != 1 {
		t.Fatalf("expected 1 impacted resource on ms1, got %d", respA.Summary.ImpactedCount)
	}

	// -------------------------------------------------------------------------
	// REAL RUST PROCESS RESTART
	// -------------------------------------------------------------------------
	t.Log("Stopping ms1 process to simulate crash/restart...")
	ms1.Stop()

	t.Log("Starting ms2 process on the exact same address...")
	ms2 := startManagedServer(t, addr)
	defer ms2.Stop()

	// 6. IMMEDIATELY AFTER RESTART (before recovery):
	// Verify that the new Rust process refuses to serve a false zero-impact result for ws-alpha!
	_, errAfterRestart := client.AnalyzeImpact(ctx, &corev1.AnalyzeImpactRequest{
		Target:      targetA,
		Direction:   "incoming",
		MaxDepth:    2,
		WorkspaceId: wsAlpha,
		ProposedChange: &corev1.ProposedChange{
			ChangeType: corev1.ChangeType_CHANGE_TYPE_UPDATE,
		},
	})
	if errAfterRestart == nil {
		t.Fatal("CRITICAL CORRECTNESS VIOLATION: restarted Rust process served a result before state was materialized!")
	}
	stAfterRestart, ok := status.FromError(errAfterRestart)
	if !ok || stAfterRestart.Code() != codes.FailedPrecondition {
		t.Fatalf("expected FAILED_PRECONDITION on restarted Rust, got %v", errAfterRestart)
	}
	t.Log("Verified: restarted Rust correctly returns FAILED_PRECONDITION (never false zero impact)")

	// 7. Test Answer Service / Handler mapping to HTTP 503
	answerSvc := answer.NewService(client, logger)
	answerHandler := answer.NewHandler(answerSvc, answer.WithLogger(logger))

	// Setup minimal HTTP test server
	apiCfg := api.DefaultConfig()
	apiCfg.IsTest = true
	apiCfg.Port = 0
	apiCfg.Store = store
	apiCfg.ImpactHandler = answerHandler
	apiCfg.Logger = logger
	apiCfg.ConfiguredWorkspaceID = wsAlpha

	apiServer := api.NewServer(apiCfg)
	if err := apiServer.Start(); err != nil {
		t.Fatalf("failed to start api server: %v", err)
	}
	defer func() { _ = apiServer.Shutdown(context.Background()) }()

	jar, _ := cookiejar.New(nil)
	httpClient := &http.Client{Jar: jar, Timeout: 5 * time.Second}
	apiBaseURL := fmt.Sprintf("http://%s", apiServer.Addr())

	// Fetch CSRF token
	csrfResp, err := httpClient.Get(apiBaseURL + "/api/csrf-token")
	if err != nil {
		t.Fatalf("failed to get csrf token: %v", err)
	}
	var csrfData struct {
		CSRFToken string `json:"csrf_token"`
	}
	_ = json.NewDecoder(csrfResp.Body).Decode(&csrfData)
	csrfResp.Body.Close()

	// Query impact on HTTP API before recovery -> MUST return HTTP 503
	impactPayload, _ := json.Marshal(map[string]any{
		"target": map[string]string{
			"provider":      "kubernetes",
			"resource_type": "service",
			"provider_id":   "cluster-1/prod/svc-a",
		},
		"direction":    "incoming",
		"max_depth":    2,
		"workspace_id": wsAlpha,
		"proposed_change": map[string]string{
			"change_type": "UPDATE",
		},
	})
	httpReq503, _ := http.NewRequestWithContext(ctx, http.MethodPost, apiBaseURL+"/api/v1/impact", bytes.NewReader(impactPayload))
	httpReq503.Header.Set("Content-Type", "application/json")
	httpReq503.Header.Set("X-Workspace-ID", wsAlpha)
	httpReq503.Header.Set("X-CSRF-Token", csrfData.CSRFToken)

	httpResp503, err := httpClient.Do(httpReq503)
	if err != nil {
		t.Fatalf("failed HTTP impact query: %v", err)
	}
	defer httpResp503.Body.Close()

	if httpResp503.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected HTTP 503 for unrecovered workspace, got %d", httpResp503.StatusCode)
	}
	var errBody map[string]any
	_ = json.NewDecoder(httpResp503.Body).Decode(&errBody)
	if errBody["code"] != "WORKSPACE_NOT_MATERIALIZED" {
		t.Fatalf("expected code WORKSPACE_NOT_MATERIALIZED, got %v", errBody["code"])
	}
	t.Log("Verified: HTTP API returns HTTP 503 WORKSPACE_NOT_MATERIALIZED")

	// 8. TRIGGER RECOVERY (REC-02)
	t.Log("Triggering RecoverAll after Rust restart...")
	if err := recoveryMgr.RecoverAll(ctx); err != nil {
		t.Fatalf("RecoverAll failed after restart: %v", err)
	}

	// 9. QUERY AFTER RECOVERY -> MUST RETURN HTTP 200 WITH IMPACT
	httpReq200, _ := http.NewRequestWithContext(ctx, http.MethodPost, apiBaseURL+"/api/v1/impact", bytes.NewReader(impactPayload))
	httpReq200.Header.Set("Content-Type", "application/json")
	httpReq200.Header.Set("X-Workspace-ID", wsAlpha)
	httpReq200.Header.Set("X-CSRF-Token", csrfData.CSRFToken)

	httpResp200, err := httpClient.Do(httpReq200)
	if err != nil {
		t.Fatalf("failed HTTP impact query after recovery: %v", err)
	}
	defer httpResp200.Body.Close()

	if httpResp200.StatusCode != http.StatusOK {
		t.Fatalf("expected HTTP 200 after recovery, got %d", httpResp200.StatusCode)
	}
	var impactAnswer answer.ImpactAnswer
	_ = json.NewDecoder(httpResp200.Body).Decode(&impactAnswer)
	if impactAnswer.Summary.ImpactedCount != 1 {
		t.Fatalf("expected 1 impacted resource after recovery, got %d", impactAnswer.Summary.ImpactedCount)
	}
	if impactAnswer.ImpactedResources[0].Resource.ProviderID != "cluster-1/prod/svc-b" {
		t.Errorf("expected svc-b to be impacted, got %s", impactAnswer.ImpactedResources[0].Resource.ProviderID)
	}
	t.Log("Verified: after recovery, HTTP API returns HTTP 200 with 1 impacted resource!")
}

// TestAcceptance_REC02_RustRestart_MonitorAutomaticRecovery tests that RecoveryManager's
// background monitor detects the Rust restart and automatically restores state.
func TestAcceptance_REC02_RustRestart_MonitorAutomaticRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := logging.NewStandardLogger(nil, logging.LevelWarn)

	addr, err := allocateFreeAddr()
	if err != nil {
		t.Fatalf("failed to allocate free address: %v", err)
	}

	ms1 := startManagedServer(t, addr)
	defer ms1.Stop()

	client, err := coreclient.Connect(ctx, addr)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer client.Close()

	store := state.NewMemoryStore()
	wsID := "cccccccc-cccc-cccc-cccc-cccccccccccc"
	now := time.Now().UTC()
	resA := state.Resource{
		WorkspaceID: wsID,
		Identity: state.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "service",
			ProviderID:   "c1/prod/app-a",
		},
		FirstObservedAt: now,
		LastObservedAt:  now,
	}
	resB := state.Resource{
		WorkspaceID: wsID,
		Identity: state.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "service",
			ProviderID:   "c1/prod/app-b",
		},
		FirstObservedAt: now,
		LastObservedAt:  now,
	}
	rel := state.Relationship{
		WorkspaceID:     wsID,
		Source:          resB.Identity,
		Target:          resA.Identity,
		Kind:            "CALLS",
		Category:        "runtime",
		FirstObservedAt: now,
		LastObservedAt:  now,
	}
	ev := state.Evidence{
		WorkspaceID: wsID,
		ID:          "22222222-2222-2222-2222-222222222222",
		Source: state.EvidenceSource{
			Provider:  "kubernetes",
			Collector: "k8s-collector",
		},
		ObservedAt:      now,
		ObservationType: "network_flow",
		Subject:         resB.Identity,
		Data:            []byte(`{}`),
	}
	prov := state.ProvenanceAssociation{
		WorkspaceID:  wsID,
		Relationship: rel.Key(),
		EvidenceID:   ev.ID,
	}

	_ = store.SaveResources(ctx, wsID, []state.Resource{resA, resB})
	_ = store.SaveRelationships(ctx, wsID, []state.Relationship{rel})
	_ = store.SaveEvidence(ctx, wsID, []state.Evidence{ev})
	_ = store.SaveProvenance(ctx, wsID, []state.ProvenanceAssociation{prov})

	healthState := health.NewState()
	materializer := state.NewMaterializer(store, client, logger)
	recoveryCfg := state.DefaultRecoveryConfig()
	recoveryCfg.CheckInterval = 100 * time.Millisecond
	recoveryMgr := state.NewRecoveryManager(
		store,
		materializer,
		client,
		healthState,
		logger,
		state.WithRecoveryConfig(recoveryCfg),
	)

	// Initial recovery
	if err := recoveryMgr.RecoverAll(ctx); err != nil {
		t.Fatalf("initial RecoverAll failed: %v", err)
	}

	// Start background monitor
	recoveryMgr.StartMonitor(ctx)
	defer recoveryMgr.Stop()

	// Verify impact on ms1
	target := &corev1.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "service",
		ProviderId:   "c1/prod/app-a",
	}
	resp, err := client.AnalyzeImpact(ctx, &corev1.AnalyzeImpactRequest{
		Target:      target,
		Direction:   "incoming",
		MaxDepth:    2,
		WorkspaceId: wsID,
		ProposedChange: &corev1.ProposedChange{
			ChangeType: corev1.ChangeType_CHANGE_TYPE_UPDATE,
		},
	})
	if err != nil || resp.Summary.ImpactedCount != 1 {
		t.Fatalf("expected 1 impacted on ms1, got resp=%v, err=%v", resp, err)
	}

	// Terminate ms1 and start ms2 on same addr
	t.Log("Stopping ms1 and starting ms2 to trigger automatic monitor recovery...")
	ms1.Stop()

	ms2 := startManagedServer(t, addr)
	defer ms2.Stop()

	// Wait for monitor to detect restart and restore graph (poll until success or deadline)
	deadline := time.Now().Add(6 * time.Second)
	recovered := false
	for time.Now().Before(deadline) {
		respAfter, errAfter := client.AnalyzeImpact(ctx, &corev1.AnalyzeImpactRequest{
			Target:      target,
			Direction:   "incoming",
			MaxDepth:    2,
			WorkspaceId: wsID,
			ProposedChange: &corev1.ProposedChange{
				ChangeType: corev1.ChangeType_CHANGE_TYPE_UPDATE,
			},
		})
		if errAfter == nil && respAfter.Summary.ImpactedCount == 1 {
			recovered = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !recovered {
		t.Fatal("background monitor failed to detect Rust restart and automatically recover workspace state within deadline")
	}
	t.Log("Verified: background monitor automatically detected Rust restart and recovered workspace graph")
}

// TestAcceptance_REC01_ColdBoot_PostgresRecovery tests REC-01:
// Cold boot with existing PostgreSQL data. Persisted workspace graphs are loaded before readiness.
func TestAcceptance_REC01_ColdBoot_PostgresRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := logging.NewStandardLogger(nil, logging.LevelWarn)

	// 1. Connect to PostgreSQL
	dbCfg := getTestDatabaseConfig(t)
	db, err := database.New(ctx, dbCfg, database.WithLogger(logger))
	if err != nil {
		t.Fatalf("failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	if err := state.EnsureSchema(ctx, db); err != nil {
		t.Fatalf("EnsureSchema failed: %v", err)
	}
	pgStore := state.NewPostgresStore(db)

	wsID := "00000000-0000-0000-0000-000000000099"
	_ = pgStore.DeleteWorkspaceState(ctx, wsID)
	defer func() {
		_ = pgStore.DeleteWorkspaceState(context.Background(), wsID)
	}()

	// 2. Persist state in PostgreSQL prior to platform startup
	now := time.Now().UTC()
	resA := state.Resource{
		WorkspaceID: wsID,
		Identity: state.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "service",
			ProviderID:   "cold-cluster/prod/cold-svc-a",
		},
		FirstObservedAt: now,
		LastObservedAt:  now,
	}
	resB := state.Resource{
		WorkspaceID: wsID,
		Identity: state.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "service",
			ProviderID:   "cold-cluster/prod/cold-svc-b",
		},
		FirstObservedAt: now,
		LastObservedAt:  now,
	}
	rel := state.Relationship{
		WorkspaceID:     wsID,
		Source:          resB.Identity,
		Target:          resA.Identity,
		Kind:            "DEPENDS_ON",
		Category:        "structural",
		FirstObservedAt: now,
		LastObservedAt:  now,
	}
	ev := state.Evidence{
		WorkspaceID: wsID,
		ID:          "33333333-3333-3333-3333-333333333333",
		Source: state.EvidenceSource{
			Provider:  "kubernetes",
			Collector: "k8s-collector",
		},
		ObservedAt:      now,
		ObservationType: "service_spec",
		Subject:         resB.Identity,
		Data:            []byte(`{}`),
	}
	prov := state.ProvenanceAssociation{
		WorkspaceID:  wsID,
		Relationship: rel.Key(),
		EvidenceID:   ev.ID,
	}

	if err := pgStore.SaveResources(ctx, wsID, []state.Resource{resA, resB}); err != nil {
		t.Fatalf("failed to save PostgreSQL resources: %v", err)
	}
	if err := pgStore.SaveRelationships(ctx, wsID, []state.Relationship{rel}); err != nil {
		t.Fatalf("failed to save PostgreSQL relationships: %v", err)
	}
	if err := pgStore.SaveEvidence(ctx, wsID, []state.Evidence{ev}); err != nil {
		t.Fatalf("failed to save PostgreSQL evidence: %v", err)
	}
	if err := pgStore.SaveProvenance(ctx, wsID, []state.ProvenanceAssociation{prov}); err != nil {
		t.Fatalf("failed to save PostgreSQL provenance: %v", err)
	}

	// 3. Start Rust Core Engine
	addr, err := allocateFreeAddr()
	if err != nil {
		t.Fatalf("failed to allocate free address: %v", err)
	}
	ms := startManagedServer(t, addr)
	defer ms.Stop()

	client, err := coreclient.Connect(ctx, addr)
	if err != nil {
		t.Fatalf("failed to connect to Rust: %v", err)
	}
	defer client.Close()

	// 4. Start Platform with RecoveryManager
	healthState := health.NewState()
	materializer := state.NewMaterializer(pgStore, client, logger)
	recoveryMgr := state.NewRecoveryManager(pgStore, materializer, client, healthState, logger)

	platCfg := &config.PlatformConfig{}
	plat := platform.New(
		platCfg,
		logger,
		client,
		platform.WithHealthState(healthState),
		platform.WithRecoveryManager(recoveryMgr),
	)

	// Platform.Start runs RecoverAll before declaring readiness
	if err := plat.Start(ctx); err != nil {
		t.Fatalf("platform Start failed: %v", err)
	}
	defer func() { _ = plat.Stop(context.Background()) }()

	// Verify platform is ready
	if !healthState.IsReady() {
		t.Fatal("REC-01 failed: platform must be ready after startup recovery")
	}

	// Verify workspace graph is fully available in Rust
	target := &corev1.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "service",
		ProviderId:   "cold-cluster/prod/cold-svc-a",
	}
	resp, err := client.AnalyzeImpact(ctx, &corev1.AnalyzeImpactRequest{
		Target:      target,
		Direction:   "incoming",
		MaxDepth:    2,
		WorkspaceId: wsID,
		ProposedChange: &corev1.ProposedChange{
			ChangeType: corev1.ChangeType_CHANGE_TYPE_UPDATE,
		},
	})
	if err != nil {
		t.Fatalf("REC-01 failed: AnalyzeImpact failed on cold booted workspace: %v", err)
	}
	if resp.Summary.ImpactedCount != 1 {
		t.Fatalf("REC-01 failed: expected 1 impacted resource, got %d", resp.Summary.ImpactedCount)
	}
	if resp.ImpactedResources[0].Resource.ProviderId != "cold-cluster/prod/cold-svc-b" {
		t.Errorf("REC-01 failed: expected cold-svc-b impacted, got %s", resp.ImpactedResources[0].Resource.ProviderId)
	}
	t.Log("Verified: REC-01 cold boot with existing PostgreSQL loaded graph before readiness")
}
