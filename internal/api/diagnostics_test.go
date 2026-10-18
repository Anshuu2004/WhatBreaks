package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aaradhychinche-alt/WhatBreaks/internal/api"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/collector/k8s"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/logging"
)

type diagnosticMockClient struct {
	mockK8sClient
	baseURL          string
	versionStatus    int
	versionBody      string
	versionErr       error
	readyzStatus     int
	readyzBody       string
	readyzErr        error
	fallbackStatus   int
	fallbackBody     string
	fallbackErr      error
	nsStatus         int
	nsList           []k8s.Namespace
	nsErr            error
}

func (m *diagnosticMockClient) BaseURL() string {
	if m.baseURL != "" {
		return m.baseURL
	}
	return "http://127.0.0.1:8001"
}

func (m *diagnosticMockClient) ProbeEndpoint(ctx context.Context, path string) (int, string, error) {
	switch path {
	case "/version":
		return m.versionStatus, m.versionBody, m.versionErr
	case "/readyz":
		return m.readyzStatus, m.readyzBody, m.readyzErr
	case "/readyz?exclude=log":
		return m.fallbackStatus, m.fallbackBody, m.fallbackErr
	default:
		return 404, "not found", nil
	}
}

func (m *diagnosticMockClient) ListNamespaces(ctx context.Context) ([]k8s.Namespace, error) {
	if m.nsErr != nil {
		return nil, m.nsErr
	}
	return m.nsList, nil
}

func (m *diagnosticMockClient) Ping(ctx context.Context) error {
	if m.pingErr != nil {
		return m.pingErr
	}
	if m.readyzStatus == http.StatusOK || m.fallbackStatus == http.StatusOK {
		return nil
	}
	if m.readyzStatus != 0 {
		return errors.New("kubernetes api ping returned status 500")
	}
	return nil
}

func TestSystemDiagnose_HealthyCollector(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	mock := &diagnosticMockClient{
		versionStatus:  http.StatusOK,
		versionBody:    `{"gitVersion":"v1.31.5+k3s1","platform":"linux/arm64"}`,
		readyzStatus:   http.StatusOK,
		fallbackStatus: http.StatusOK,
		nsList: []k8s.Namespace{
			{ObjectMeta: k8s.ObjectMeta{Name: "default"}},
			{ObjectMeta: k8s.ObjectMeta{Name: "kube-system"}},
		},
	}

	col, err := k8s.New(
		k8s.WithClient(mock),
		k8s.WithClusterID("k3d-license-service"),
		k8s.WithWorkspaceID(wsID),
		k8s.WithLogger(logging.NewStandardLogger(nil, logging.LevelWarn)),
	)
	if err != nil {
		t.Fatalf("failed to create collector: %v", err)
	}
	_ = col.Start(context.Background())

	cfg := api.DefaultConfig()
	cfg.IsTest = true
	cfg.ConfiguredWorkspaceID = wsID
	cfg.K8sCollector = col
	cfg.Logger = logging.NewStandardLogger(nil, logging.LevelWarn)

	srv := api.NewServer(cfg)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/system/diagnose?component=collector&workspace_id="+wsID, nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp api.SystemDiagnoseResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	colDiag, exists := resp.Results["collector"]
	if !exists {
		t.Fatalf("expected collector diagnostics in response")
	}

	if colDiag.Status != "healthy" {
		t.Errorf("expected healthy status, got %s", colDiag.Status)
	}
	if colDiag.Analysis.Category != "HEALTHY" {
		t.Errorf("expected HEALTHY category, got %s", colDiag.Analysis.Category)
	}
	if colDiag.Analysis.FixAvailable {
		t.Errorf("expected fix_available=false for healthy component")
	}
}

func TestSystemDiagnose_DegradedCollector_LogSubsystem500(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	mock := &diagnosticMockClient{
		versionStatus:  http.StatusOK,
		versionBody:    `{"gitVersion":"v1.31.5+k3s1","platform":"linux/arm64"}`,
		readyzStatus:   http.StatusInternalServerError,
		readyzBody:     "[+]ping ok\n[-]log failed: reason withheld\n[+]etcd ok\nreadyz check failed",
		fallbackStatus: http.StatusOK,
		fallbackBody:   "ok",
		nsList: []k8s.Namespace{
			{ObjectMeta: k8s.ObjectMeta{Name: "default"}},
			{ObjectMeta: k8s.ObjectMeta{Name: "license-system"}},
		},
		mockK8sClient: mockK8sClient{
			pingErr: errors.New("kubernetes api ping returned status 500: [-]log failed: reason withheld"),
		},
	}

	col, err := k8s.New(
		k8s.WithClient(mock),
		k8s.WithClusterID("k3d-license-service"),
		k8s.WithWorkspaceID(wsID),
		k8s.WithLogger(logging.NewStandardLogger(nil, logging.LevelWarn)),
	)
	if err != nil {
		t.Fatalf("failed to create collector: %v", err)
	}
	_ = col.Start(context.Background())

	cfg := api.DefaultConfig()
	cfg.IsTest = true
	cfg.ConfiguredWorkspaceID = wsID
	cfg.K8sCollector = col
	cfg.Logger = logging.NewStandardLogger(nil, logging.LevelWarn)

	srv := api.NewServer(cfg)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/system/diagnose?component=collector&workspace_id="+wsID, nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	var resp api.SystemDiagnoseResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	colDiag := resp.Results["collector"]
	if colDiag.Status != "degraded" {
		t.Errorf("expected degraded status, got %s", colDiag.Status)
	}
	if colDiag.Analysis.Category != "API_SERVER_SUBSYSTEM" {
		t.Errorf("expected API_SERVER_SUBSYSTEM, got %s", colDiag.Analysis.Category)
	}
	if colDiag.Analysis.Confidence != "HIGH" {
		t.Errorf("expected HIGH confidence, got %s", colDiag.Analysis.Confidence)
	}
	if !colDiag.Analysis.FixAvailable {
		t.Errorf("expected fix_available=true")
	}
	if len(colDiag.Analysis.SupportingEvidence) == 0 {
		t.Errorf("expected supporting evidence items")
	}
}

func TestSystemDiagnose_UnavailableCollector_ConnectionRefused(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	mock := &diagnosticMockClient{
		versionErr: errors.New("dial tcp 127.0.0.1:8001: connect: connection refused"),
		readyzErr:  errors.New("dial tcp 127.0.0.1:8001: connect: connection refused"),
		nsErr:      errors.New("dial tcp 127.0.0.1:8001: connect: connection refused"),
		mockK8sClient: mockK8sClient{
			pingErr: errors.New("dial tcp 127.0.0.1:8001: connect: connection refused"),
		},
	}

	col, err := k8s.New(
		k8s.WithClient(mock),
		k8s.WithClusterID("k3d-license-service"),
		k8s.WithWorkspaceID(wsID),
		k8s.WithLogger(logging.NewStandardLogger(nil, logging.LevelWarn)),
	)
	if err != nil {
		t.Fatalf("failed to create collector: %v", err)
	}
	_ = col.Start(context.Background())

	cfg := api.DefaultConfig()
	cfg.IsTest = true
	cfg.ConfiguredWorkspaceID = wsID
	cfg.K8sCollector = col
	cfg.Logger = logging.NewStandardLogger(nil, logging.LevelWarn)

	srv := api.NewServer(cfg)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/system/diagnose?component=collector&workspace_id="+wsID, nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	var resp api.SystemDiagnoseResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)

	colDiag := resp.Results["collector"]
	if colDiag.Analysis.Category != "API_SERVER_UNAVAILABLE" {
		t.Errorf("expected API_SERVER_UNAVAILABLE, got %s", colDiag.Analysis.Category)
	}
	if colDiag.Analysis.FixAvailable {
		t.Errorf("expected fix_available=false when external process is down")
	}
}

func TestSystemDiagnose_AuthorizationFailure(t *testing.T) {
	cfg := api.DefaultConfig()
	cfg.IsTest = true
	cfg.ConfiguredWorkspaceID = "00000000-0000-0000-0000-000000000001"
	cfg.Logger = logging.NewStandardLogger(nil, logging.LevelWarn)
	srv := api.NewServer(cfg)

	// Unauthorized workspace UUID
	req := httptest.NewRequest(http.MethodGet, "/api/v1/system/diagnose?workspace_id=99999999-9999-9999-9999-999999999999", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden for unauthorized workspace, got %d", rec.Code)
	}
}

func TestSystemRecover_ConfirmationRequired(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	mock := &diagnosticMockClient{
		mockK8sClient: mockK8sClient{pingErr: errors.New("ping failed")},
	}
	col, _ := k8s.New(
		k8s.WithClient(mock),
		k8s.WithClusterID("k3d-license-service"),
		k8s.WithWorkspaceID(wsID),
		k8s.WithLogger(logging.NewStandardLogger(nil, logging.LevelWarn)),
	)
	_ = col.Start(context.Background())

	cfg := api.DefaultConfig()
	cfg.IsTest = true
	cfg.ConfiguredWorkspaceID = wsID
	cfg.K8sCollector = col
	cfg.Logger = logging.NewStandardLogger(nil, logging.LevelWarn)
	srv := api.NewServer(cfg)

	// Attempt mutating restart without confirmation
	body, _ := json.Marshal(api.RecoveryRequest{
		Component: "collector",
		Action:    "restart_collector",
		Confirmed: false,
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/system/recover?workspace_id="+wsID, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	var resp api.RecoveryResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)

	if resp.Status != "failed" {
		t.Errorf("expected failed status when confirmation is missing, got %s", resp.Status)
	}
	if resp.VerifiedHealthy {
		t.Errorf("expected verified_healthy=false")
	}
}

func TestSystemRecover_SuccessfulRecovery(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	mock := &diagnosticMockClient{
		readyzStatus:   http.StatusOK,
		fallbackStatus: http.StatusOK,
	}
	col, _ := k8s.New(
		k8s.WithClient(mock),
		k8s.WithClusterID("k3d-license-service"),
		k8s.WithWorkspaceID(wsID),
		k8s.WithLogger(logging.NewStandardLogger(nil, logging.LevelWarn)),
	)
	_ = col.Start(context.Background())

	cfg := api.DefaultConfig()
	cfg.IsTest = true
	cfg.ConfiguredWorkspaceID = wsID
	cfg.K8sCollector = col
	cfg.Logger = logging.NewStandardLogger(nil, logging.LevelWarn)
	srv := api.NewServer(cfg)

	body, _ := json.Marshal(api.RecoveryRequest{
		Component: "collector",
		Action:    "restart_collector",
		Confirmed: true,
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/system/recover?workspace_id="+wsID, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp api.RecoveryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Status != "recovered" {
		t.Errorf("expected recovered, got %s", resp.Status)
	}
	if !resp.VerifiedHealthy {
		t.Errorf("expected verified_healthy=true")
	}
}

func TestSecretSafety_DiagnosticsSanitization(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	cfg := api.DefaultConfig()
	cfg.IsTest = true
	cfg.ConfiguredWorkspaceID = wsID
	cfg.DatabaseInfo = map[string]any{
		"host":     "localhost",
		"port":     5432,
		"database": "whatbreaks",
		"password": "SUPER_SECRET_DB_PASSWORD_12345",
	}
	cfg.HealthPinger = &mockPinger{err: nil}
	cfg.Logger = logging.NewStandardLogger(nil, logging.LevelWarn)
	srv := api.NewServer(cfg)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/system/diagnose?component=database&workspace_id="+wsID, nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	bodyStr := rec.Body.String()
	if bytes.Contains([]byte(bodyStr), []byte("SUPER_SECRET_DB_PASSWORD_12345")) {
		t.Fatalf("SECRET LEAK DETECTED: password found in diagnostic output!")
	}
}

func TestSystemRecover_FailedRecovery(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	mock := &diagnosticMockClient{
		mockK8sClient: mockK8sClient{pingErr: errors.New("cluster offline permanently")},
	}
	col, _ := k8s.New(
		k8s.WithClient(mock),
		k8s.WithClusterID("k3d-license-service"),
		k8s.WithWorkspaceID(wsID),
		k8s.WithLogger(logging.NewStandardLogger(nil, logging.LevelWarn)),
	)
	_ = col.Start(context.Background())

	cfg := api.DefaultConfig()
	cfg.IsTest = true
	cfg.ConfiguredWorkspaceID = wsID
	cfg.K8sCollector = col
	cfg.Logger = logging.NewStandardLogger(nil, logging.LevelWarn)
	srv := api.NewServer(cfg)

	body, _ := json.Marshal(api.RecoveryRequest{
		Component: "collector",
		Action:    "retry_probe",
		Confirmed: false,
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/system/recover?workspace_id="+wsID, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 Unprocessable Entity for failed recovery, got %d", rec.Code)
	}

	var resp api.RecoveryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Status != "failed" {
		t.Errorf("expected failed status, got %s", resp.Status)
	}
	if resp.VerifiedHealthy {
		t.Errorf("expected verified_healthy=false")
	}
	if resp.NextSteps == "" {
		t.Errorf("expected next_steps guidance on failure")
	}
}

func TestSystemDiagnoseAndRecover_Database(t *testing.T) {
	wsID := "00000000-0000-0000-0000-000000000001"
	pinger := &mockPinger{err: errors.New("connection refused")}
	cfg := api.DefaultConfig()
	cfg.IsTest = true
	cfg.ConfiguredWorkspaceID = wsID
	cfg.HealthPinger = pinger
	cfg.Logger = logging.NewStandardLogger(nil, logging.LevelWarn)
	srv := api.NewServer(cfg)

	// 1. Diagnose reports unavailable
	req := httptest.NewRequest(http.MethodGet, "/api/v1/system/diagnose?component=database&workspace_id="+wsID, nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	var diagResp api.SystemDiagnoseResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &diagResp)
	if diagResp.Results["database"].Status != "unavailable" {
		t.Errorf("expected database unavailable, got %s", diagResp.Results["database"].Status)
	}

	// 2. Recover reconnect with healthy pinger
	pinger.err = nil
	recBody, _ := json.Marshal(api.RecoveryRequest{
		Component: "database",
		Action:    "reconnect",
	})
	recReq := httptest.NewRequest(http.MethodPost, "/api/v1/system/recover?workspace_id="+wsID, bytes.NewReader(recBody))
	recReq.Header.Set("Content-Type", "application/json")
	recRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(recRec, recReq)

	var recResp api.RecoveryResponse
	_ = json.Unmarshal(recRec.Body.Bytes(), &recResp)
	if recResp.Status != "recovered" || !recResp.VerifiedHealthy {
		t.Errorf("expected database recovered, got %+v", recResp)
	}
}

