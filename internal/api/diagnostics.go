package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aaradhychinche-alt/WhatBreaks/internal/collector/k8s"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/discovery"
)

// DiagnosticStep represents a single verification check executed during component diagnostics.
type DiagnosticStep struct {
	Name       string         `json:"name"`
	Status     string         `json:"status"` // "ok" | "warn" | "failed" | "skipped"
	Message    string         `json:"message"`
	DurationMs int64          `json:"duration_ms"`
	Details    map[string]any `json:"details,omitempty"`
}

// DiagnosticAnalysis encapsulates the evidence-based root cause analysis.
type DiagnosticAnalysis struct {
	ProbableCause      string   `json:"probable_cause"`
	Confidence         string   `json:"confidence"` // "HIGH" | "MEDIUM" | "LOW"
	Category           string   `json:"category"`
	SupportingEvidence []string `json:"supporting_evidence"`
	RecommendedAction  string   `json:"recommended_action"`
	FixType            string   `json:"fix_type"`
	FixAvailable       bool     `json:"fix_available"`
	Mutating           bool     `json:"mutating"`
	PotentialEffects   string   `json:"potential_effects,omitempty"`
}

// ComponentDiagnosticResult represents full diagnostics for a single platform component.
type ComponentDiagnosticResult struct {
	Component    string             `json:"component"`
	Status       string             `json:"status"`
	Timestamp    string             `json:"timestamp"`
	CurrentError string             `json:"current_error,omitempty"`
	Steps        []DiagnosticStep   `json:"steps"`
	Analysis     DiagnosticAnalysis `json:"analysis"`
}

// SystemDiagnoseResponse is returned by GET /api/v1/system/diagnose.
type SystemDiagnoseResponse struct {
	WorkspaceID string                                `json:"workspace_id"`
	Timestamp   string                                `json:"timestamp"`
	Component   string                                `json:"component,omitempty"`
	Results     map[string]ComponentDiagnosticResult `json:"results"`
}

// RecoveryRequest payload for POST /api/v1/system/recover.
type RecoveryRequest struct {
	Component string `json:"component"`
	Action    string `json:"action"`
	Confirmed bool   `json:"confirmed"`
}

// RecoveryResponse is returned by POST /api/v1/system/recover.
type RecoveryResponse struct {
	WorkspaceID     string          `json:"workspace_id"`
	Component       string          `json:"component"`
	Action          string          `json:"action"`
	Status          string          `json:"status"` // "recovered" | "failed"
	VerifiedHealthy bool            `json:"verified_healthy"`
	Message         string          `json:"message"`
	DurationMs      int64           `json:"duration_ms"`
	Health          ComponentHealth `json:"health"`
	NextSteps       string          `json:"next_steps,omitempty"`
}

// handleSystemDiagnose handles GET /api/v1/system/diagnose.
// It executes read-only diagnostic checks against configured components.
func (s *Server) handleSystemDiagnose() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed", "METHOD_NOT_ALLOWED")
			return
		}

		wsID, err := extractAndAuthorizeWorkspace(r, s.cfg.WorkspaceAuthorizer, s.cfg.ConfiguredWorkspaceID)
		if err != nil {
			handleWorkspaceAuthError(w, err)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		targetComponent := strings.TrimSpace(r.URL.Query().Get("component"))
		results := make(map[string]ComponentDiagnosticResult)

		if targetComponent == "" || targetComponent == "collector" {
			results["collector"] = s.diagnoseCollector(ctx, wsID)
		}
		if targetComponent == "" || targetComponent == "database" {
			results["database"] = s.diagnoseDatabase(ctx)
		}
		if targetComponent == "" || targetComponent == "core_engine" {
			results["core_engine"] = s.diagnoseCoreEngine(ctx)
		}
		if targetComponent == "" || targetComponent == "api" {
			results["api"] = s.diagnoseAPI()
		}

		if targetComponent != "" && len(results) == 0 {
			writeErrorResponse(w, http.StatusBadRequest, "Unknown component: "+targetComponent, "INVALID_COMPONENT")
			return
		}

		for k, r := range results {
			if r.Steps == nil {
				r.Steps = []DiagnosticStep{}
			}
			if r.Analysis.SupportingEvidence == nil {
				r.Analysis.SupportingEvidence = []string{}
			}
			results[k] = r
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(SystemDiagnoseResponse{
			WorkspaceID: wsID,
			Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
			Component:   targetComponent,
			Results:     results,
		})
	}
}

// handleSystemRecover handles POST /api/v1/system/recover.
// It executes safe, confirmed recovery actions and verifies resulting health.
func (s *Server) handleSystemRecover() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed", "METHOD_NOT_ALLOWED")
			return
		}

		wsID, err := extractAndAuthorizeWorkspace(r, s.cfg.WorkspaceAuthorizer, s.cfg.ConfiguredWorkspaceID)
		if err != nil {
			handleWorkspaceAuthError(w, err)
			return
		}

		var req RecoveryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "INVALID_REQUEST")
			return
		}

		start := time.Now()
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()

		var resp RecoveryResponse
		resp.WorkspaceID = wsID
		resp.Component = req.Component
		resp.Action = req.Action

		switch req.Component {
		case "collector":
			s.recoverCollector(ctx, wsID, req, &resp)
		case "database":
			s.recoverDatabase(ctx, req, &resp)
		case "core_engine":
			s.recoverCoreEngine(ctx, req, &resp)
		default:
			writeErrorResponse(w, http.StatusBadRequest, "Recovery not supported for component: "+req.Component, "UNSUPPORTED_COMPONENT")
			return
		}

		resp.DurationMs = time.Since(start).Milliseconds()

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		if resp.Status == "failed" {
			w.WriteHeader(http.StatusUnprocessableEntity)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		_ = json.NewEncoder(w).Encode(resp)
	}
}

// ---------------------------------------------------------------------------
// Collector Diagnostics & Recovery
// ---------------------------------------------------------------------------

type endpointProber interface {
	ProbeEndpoint(ctx context.Context, path string) (int, string, error)
	BaseURL() string
}

func (s *Server) diagnoseCollector(ctx context.Context, wsID string) ComponentDiagnosticResult {
	res := ComponentDiagnosticResult{
		Component: "collector",
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Steps:     []DiagnosticStep{},
	}

	col := s.cfg.K8sCollector
	if col == nil {
		res.Status = "not_configured"
		res.Analysis = DiagnosticAnalysis{
			ProbableCause:      "Kubernetes discovery collector is not enabled or configured on this platform instance.",
			Confidence:         "HIGH",
			Category:           "CONFIGURATION_ERROR",
			SupportingEvidence: []string{"K8sCollector instance is nil in server runtime config"},
			RecommendedAction:  "Start wb with --k8s-cluster-id and --k8s-api-url or configure kubernetes discovery.",
			FixAvailable:       false,
		}
		return res
	}

	clusterID := col.ClusterID()
	client := col.Client()

	var prober endpointProber
	if p, ok := client.(endpointProber); ok {
		prober = p
	}

	currentPingErr := col.Ping(ctx)
	if currentPingErr != nil {
		res.CurrentError = currentPingErr.Error()
		res.Status = "degraded"
	} else {
		res.Status = "healthy"
	}

	// Step 1: Endpoint & API Server Reachability (/version)
	step1Start := time.Now()
	step1 := DiagnosticStep{
		Name:    "Kubernetes API Reachability (/version)",
		Details: make(map[string]any),
	}
	versionStatus := 0
	var versionBody string
	var versionErr error
	if prober != nil {
		step1.Details["base_url"] = prober.BaseURL()
		versionStatus, versionBody, versionErr = prober.ProbeEndpoint(ctx, "/version")
		step1.Details["status_code"] = versionStatus
	}

	if versionErr != nil {
		step1.Status = "failed"
		step1.Message = fmt.Sprintf("TCP/HTTP connection failed: %v", versionErr)
	} else if versionStatus == http.StatusOK {
		step1.Status = "ok"
		var vMap map[string]any
		if err := json.Unmarshal([]byte(versionBody), &vMap); err == nil {
			step1.Message = fmt.Sprintf("Connected (v%v, platform: %v)", vMap["gitVersion"], vMap["platform"])
			step1.Details["version"] = vMap["gitVersion"]
			step1.Details["platform"] = vMap["platform"]
		} else {
			step1.Message = "Connected to Kubernetes API (HTTP 200 OK)"
		}
	} else {
		step1.Status = "failed"
		step1.Message = fmt.Sprintf("Unexpected response from /version (HTTP %d)", versionStatus)
	}
	step1.DurationMs = time.Since(step1Start).Milliseconds()
	res.Steps = append(res.Steps, step1)

	// Step 2: Readiness Probe (/readyz)
	step2Start := time.Now()
	step2 := DiagnosticStep{
		Name:    "Readiness Probe Check (/readyz)",
		Details: make(map[string]any),
	}
	readyzStatus := 0
	var readyzBody string
	var readyzErr error
	var failedChecks []string
	if prober != nil {
		readyzStatus, readyzBody, readyzErr = prober.ProbeEndpoint(ctx, "/readyz")
		step2.Details["status_code"] = readyzStatus
	}

	if readyzErr != nil {
		step2.Status = "failed"
		step2.Message = fmt.Sprintf("Readiness probe request failed: %v", readyzErr)
	} else if readyzStatus == http.StatusOK {
		step2.Status = "ok"
		step2.Message = "Kubernetes API server reports healthy readiness (readyz OK)"
	} else {
		step2.Status = "failed"
		for _, line := range strings.Split(readyzBody, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "[-]") {
				failedChecks = append(failedChecks, strings.TrimPrefix(line, "[-]"))
			}
		}
		step2.Details["failed_checks"] = failedChecks
		if len(failedChecks) > 0 {
			step2.Message = fmt.Sprintf("HTTP %d: Failed subsystems: %s", readyzStatus, strings.Join(failedChecks, ", "))
		} else {
			step2.Message = fmt.Sprintf("Readiness probe returned HTTP %d", readyzStatus)
		}
	}
	step2.DurationMs = time.Since(step2Start).Milliseconds()
	res.Steps = append(res.Steps, step2)

	// Step 3: Readiness Fallback Probe (/readyz?exclude=log)
	step3Start := time.Now()
	step3 := DiagnosticStep{
		Name:    "Readiness Fallback Check (/readyz?exclude=log)",
		Details: make(map[string]any),
	}
	fallbackStatus := 0
	if prober != nil && readyzStatus != http.StatusOK {
		fallbackStatus, _, _ = prober.ProbeEndpoint(ctx, "/readyz?exclude=log")
		step3.Details["status_code"] = fallbackStatus
		if fallbackStatus == http.StatusOK {
			step3.Status = "ok"
			step3.Message = "Readiness probe succeeds when non-critical log check is excluded"
		} else {
			step3.Status = "warn"
			step3.Message = fmt.Sprintf("Readiness probe with exclude=log returned HTTP %d", fallbackStatus)
		}
	} else {
		step3.Status = "skipped"
		step3.Message = "Fallback probe not required (/readyz is healthy or prober unavailable)"
	}
	step3.DurationMs = time.Since(step3Start).Milliseconds()
	res.Steps = append(res.Steps, step3)

	// Step 4: Resource API & Authentication (/api/v1/namespaces)
	step4Start := time.Now()
	step4 := DiagnosticStep{
		Name:    "Kubernetes Resource API Access (/api/v1/namespaces)",
		Details: make(map[string]any),
	}
	nsList, nsErr := client.ListNamespaces(ctx)
	if nsErr != nil {
		step4.Status = "failed"
		step4.Message = fmt.Sprintf("Failed to list namespaces: %v", nsErr)
		step4.Details["error"] = nsErr.Error()
	} else {
		step4.Status = "ok"
		step4.Message = fmt.Sprintf("Successfully queried namespaces (%d found)", len(nsList))
		step4.Details["namespace_count"] = len(nsList)
		var names []string
		for i, ns := range nsList {
			if i >= 5 {
				break
			}
			names = append(names, ns.ObjectMeta.Name)
		}
		step4.Details["sample_namespaces"] = names
	}
	step4.DurationMs = time.Since(step4Start).Milliseconds()
	res.Steps = append(res.Steps, step4)

	// Step 5: Cluster Context & Configuration Safety
	step5Start := time.Now()
	step5 := DiagnosticStep{
		Name:    "Cluster Context & Identity Verification",
		Details: make(map[string]any),
	}
	activeContext := detectKubeContext()
	step5.Details["configured_cluster_id"] = clusterID
	step5.Details["active_kube_context"] = activeContext

	if activeContext != "" && clusterID != "" && activeContext != clusterID && clusterID != "k8s-cluster" {
		step5.Status = "warn"
		step5.Message = fmt.Sprintf("Context mismatch: configured '%s' vs active context '%s'", clusterID, activeContext)
	} else {
		step5.Status = "ok"
		step5.Message = fmt.Sprintf("Cluster context aligned: %s", clusterID)
	}
	step5.DurationMs = time.Since(step5Start).Milliseconds()
	res.Steps = append(res.Steps, step5)

	// Step 6: Collector In-Memory Process State
	step6Start := time.Now()
	step6 := DiagnosticStep{
		Name: "Collector Process State",
		Details: map[string]any{
			"alive": col.IsAlive(),
			"ready": col.IsReady(),
		},
	}
	if !col.IsAlive() {
		step6.Status = "failed"
		step6.Message = "Collector process is stopped or dead in-memory"
	} else if !col.IsReady() {
		step6.Status = "warn"
		step6.Message = "Collector process is alive but marked unready"
	} else {
		step6.Status = "ok"
		step6.Message = "Collector process is alive and ready"
	}
	step6.DurationMs = time.Since(step6Start).Milliseconds()
	res.Steps = append(res.Steps, step6)

	// Evidence-Based Root Cause Classification
	analysis := DiagnosticAnalysis{
		SupportingEvidence: []string{},
	}

	if res.Status == "healthy" {
		analysis.ProbableCause = "The Kubernetes API server and collector are operational and healthy."
		analysis.Confidence = "HIGH"
		analysis.Category = "HEALTHY"
		analysis.SupportingEvidence = []string{
			fmt.Sprintf("Connected to Kubernetes API (%s)", step1.Details["version"]),
			fmt.Sprintf("Readiness probe succeeded and authenticated namespaces are queryable (%d namespaces)", len(nsList)),
			"Collector in-memory process is alive and marked ready",
		}
		analysis.RecommendedAction = "No action required. Trigger Sync Discovery if updated resource inventory is desired."
		analysis.FixAvailable = false
	} else if versionErr != nil || versionStatus == 0 {
		analysis.ProbableCause = "The Kubernetes API server or kubectl proxy is unreachable at the configured address."
		analysis.Confidence = "HIGH"
		analysis.Category = "API_SERVER_UNAVAILABLE"
		analysis.SupportingEvidence = append(analysis.SupportingEvidence, fmt.Sprintf("TCP/HTTP connection to API server failed: %v", versionErr))
		analysis.RecommendedAction = "Verify that 'kubectl proxy --port=8001' or your Kubernetes API server is running on the expected host and port."
		analysis.FixAvailable = false
		analysis.PotentialEffects = "External process startup required; WhatBreaks cannot start external OS proxies automatically."
	} else if errors.Is(nsErr, k8s.ErrUnauthorized) {
		analysis.ProbableCause = "Authentication failed: bearer token or service account credentials were rejected (HTTP 401)."
		analysis.Confidence = "HIGH"
		analysis.Category = "AUTHENTICATION_FAILURE"
		analysis.SupportingEvidence = append(analysis.SupportingEvidence, "Kubernetes API returned HTTP 401 Unauthorized for namespace queries.")
		analysis.RecommendedAction = "Refresh kubeconfig bearer token or verify service account token credentials."
		analysis.FixAvailable = false
	} else if errors.Is(nsErr, k8s.ErrForbidden) {
		analysis.ProbableCause = "Authorization failed: collector identity lacks required RBAC permissions (HTTP 403)."
		analysis.Confidence = "HIGH"
		analysis.Category = "AUTHORIZATION_FAILURE"
		analysis.SupportingEvidence = append(analysis.SupportingEvidence, "Kubernetes API returned HTTP 403 Forbidden for namespace queries.")
		analysis.RecommendedAction = "Grant ClusterRole with read-only permissions (get/list/watch on pods, deployments, services, namespaces) to the collector service account."
		analysis.FixAvailable = false
	} else if readyzStatus == 500 && (containsLogFailure(failedChecks) || fallbackStatus == http.StatusOK) && nsErr == nil {
		analysis.ProbableCause = "The Kubernetes API server is fully functional and responding to authenticated API requests, but its internal logging subsystem probe (/readyz/log) is reporting failure. This commonly occurs in containerized k3s/k3d distributions where log directory mounts or journald sockets are withheld."
		analysis.Confidence = "HIGH"
		analysis.Category = "API_SERVER_SUBSYSTEM"
		analysis.SupportingEvidence = append(analysis.SupportingEvidence,
			fmt.Sprintf("HTTP /readyz returned 500 with: %s", strings.Join(failedChecks, ", ")),
			fmt.Sprintf("HTTP /version returned 200 OK (connected to %s)", step1.Details["version"]),
			fmt.Sprintf("HTTP /api/v1/namespaces returned 200 OK (%d namespaces queryable)", len(nsList)),
			"Fallback probe /readyz?exclude=log succeeded with HTTP 200 OK",
		)
		analysis.RecommendedAction = "Execute probe retry with fallback check or perform a controlled collector restart to verify cluster readiness and unblock discovery sweeps."
		analysis.FixType = "PROBE_RETRY_FALLBACK"
		analysis.FixAvailable = true
		analysis.Mutating = false
		analysis.PotentialEffects = "Re-evaluates reachability with fallback probe logic. Marks collector ready if API responses succeed. Zero mutations to cluster workloads or secrets."
	} else if !col.IsAlive() {
		analysis.ProbableCause = "The in-process collector instance was stopped or failed to initialize."
		analysis.Confidence = "HIGH"
		analysis.Category = "COLLECTOR_INTERNAL_ERROR"
		analysis.SupportingEvidence = append(analysis.SupportingEvidence, "Collector IsAlive() returned false.")
		analysis.RecommendedAction = "Restart the internal collector instance."
		analysis.FixType = "RESTART_COLLECTOR"
		analysis.FixAvailable = true
		analysis.Mutating = true
		analysis.PotentialEffects = "Restarts the in-process collector routine. Safe and contained to WhatBreaks; no Kubernetes cluster state is altered."
	} else {
		analysis.ProbableCause = fmt.Sprintf("Kubernetes API reported readiness failure: %s", res.CurrentError)
		analysis.Confidence = "MEDIUM"
		analysis.Category = "API_SERVER_UNHEALTHY"
		analysis.SupportingEvidence = append(analysis.SupportingEvidence, fmt.Sprintf("Probe failure: %v", currentPingErr))
		analysis.RecommendedAction = "Inspect Kubernetes API server logs and verify cluster health."
		analysis.FixAvailable = true
		analysis.FixType = "RETRY_PROBE"
		analysis.Mutating = false
	}

	res.Analysis = analysis
	return res
}

func detectKubeContext() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	kcPath := filepath.Join(home, ".kube", "config")
	content, err := os.ReadFile(kcPath)
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(content), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "current-context:") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				return strings.Trim(strings.TrimSpace(parts[1]), `"'`)
			}
		}
	}
	return ""
}

func containsLogFailure(checks []string) bool {
	for _, c := range checks {
		if strings.Contains(strings.ToLower(c), "log") {
			return true
		}
	}
	return false
}

func (s *Server) recoverCollector(ctx context.Context, wsID string, req RecoveryRequest, resp *RecoveryResponse) {
	col := s.cfg.K8sCollector
	if col == nil {
		resp.Status = "failed"
		resp.Message = "Kubernetes collector is not configured on this platform instance."
		resp.NextSteps = "Configure k8s collector options in server runtime."
		return
	}

	switch req.Action {
	case "retry_probe":
		// Safe, non-mutating reachability re-check with fallback
		pingErr := col.Ping(ctx)
		if pingErr == nil && col.IsReady() {
			resp.Status = "recovered"
			resp.VerifiedHealthy = true
			resp.Message = "Collector verified cluster reachability and is now healthy."
		} else {
			resp.Status = "failed"
			resp.VerifiedHealthy = false
			if pingErr != nil {
				resp.Message = "Reachability probe failed: " + pingErr.Error()
			} else {
				resp.Message = "Collector remains not ready after probe check."
			}
			resp.NextSteps = "Check that kubectl proxy is running and Kubernetes API is accessible."
		}

	case "restart_collector":
		// Mutating: requires explicit user confirmation
		if !req.Confirmed {
			resp.Status = "failed"
			resp.Message = "Explicit confirmation is required before restarting the collector instance."
			resp.NextSteps = "Confirm the action by enabling the confirmation checkbox before executing."
			return
		}

		restartErr := col.Restart(ctx)
		if restartErr != nil {
			resp.Status = "failed"
			resp.VerifiedHealthy = false
			resp.Message = "Collector restart failed: " + restartErr.Error()
			resp.NextSteps = "Inspect server logs for initialization errors."
			return
		}

		pingErr := col.Ping(ctx)
		if pingErr == nil && col.IsReady() {
			resp.Status = "recovered"
			resp.VerifiedHealthy = true
			resp.Message = "Collector restarted successfully in-process and verified ready."
		} else {
			resp.Status = "failed"
			resp.VerifiedHealthy = false
			if pingErr != nil {
				resp.Message = "Collector restarted but cluster reachability failed: " + pingErr.Error()
			} else {
				resp.Message = "Collector restarted but remained not ready."
			}
			resp.NextSteps = "Check Kubernetes API server readiness and proxy connectivity."
		}

	case "sync_discovery":
		if s.coordinator == nil {
			resp.Status = "failed"
			resp.Message = "Discovery coordinator is not available."
			return
		}
		syncRes, syncErr := s.coordinator.Discover(ctx, discovery.SyncRequest{
			WorkspaceID: wsID,
			ClusterID:   col.ClusterID(),
			Trigger:     discovery.TriggerManual,
		})
		if syncErr != nil {
			resp.Status = "failed"
			resp.VerifiedHealthy = false
			resp.Message = "Discovery sweep failed: " + syncErr.Error()
			resp.NextSteps = "Verify reachability and collector readiness before triggering sync."
		} else {
			resp.Status = "recovered"
			resp.VerifiedHealthy = col.IsReady()
			resp.Message = fmt.Sprintf("Discovery sweep completed successfully: %d resources discovered.", syncRes.ResourcesTotal)
		}

	default:
		resp.Status = "failed"
		resp.Message = "Unknown recovery action: " + req.Action
		resp.NextSteps = "Supported actions for collector: retry_probe, restart_collector, sync_discovery."
	}

	// Update health summary
	resp.Health = ComponentHealth{
		Status: func() string {
			if col.IsReady() {
				return "healthy"
			}
			return "degraded"
		}(),
		Details: map[string]any{
			"alive":      col.IsAlive(),
			"ready":      col.IsReady(),
			"cluster_id": col.ClusterID(),
		},
	}
}

// ---------------------------------------------------------------------------
// Database Diagnostics & Recovery
// ---------------------------------------------------------------------------

func (s *Server) diagnoseDatabase(ctx context.Context) ComponentDiagnosticResult {
	res := ComponentDiagnosticResult{
		Component: "database",
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Steps:     []DiagnosticStep{},
	}

	if s.cfg.HealthPinger == nil {
		res.Status = "not_configured"
		res.Analysis = DiagnosticAnalysis{
			ProbableCause:      "PostgreSQL database health pinger is not configured on this server.",
			Confidence:         "HIGH",
			Category:           "CONFIGURATION_ERROR",
			SupportingEvidence: []string{"HealthPinger interface is nil in server runtime config"},
			RecommendedAction:  "Provide database configuration via DB_* environment variables or --db-* flags.",
			FixAvailable:       false,
		}
		return res
	}

	step1Start := time.Now()
	pingErr := s.cfg.HealthPinger.Ping(ctx)
	step1 := DiagnosticStep{
		Name:    "PostgreSQL Ping Verification (SELECT 1)",
		Details: make(map[string]any),
	}
	if s.cfg.DatabaseInfo != nil {
		for k, v := range s.cfg.DatabaseInfo {
			if k != "password" { // NEVER include password
				step1.Details[k] = v
			}
		}
	}

	if pingErr != nil {
		step1.Status = "failed"
		step1.Message = fmt.Sprintf("Ping query failed: %v", pingErr)
		res.Status = "unavailable"
		res.CurrentError = pingErr.Error()
		res.Analysis = DiagnosticAnalysis{
			ProbableCause: "PostgreSQL state store is unreachable or rejecting connections.",
			Confidence:    "HIGH",
			Category:      "DATABASE_UNAVAILABLE",
			SupportingEvidence: []string{
				fmt.Sprintf("Health pinger returned error: %v", pingErr),
			},
			RecommendedAction: "Start or verify local PostgreSQL service (docker compose up -d postgres or brew services start postgresql@16).",
			FixType:           "RECONNECT",
			FixAvailable:      true,
			Mutating:          false,
			PotentialEffects:  "Re-tests the database connection pool; no data is modified or purged.",
		}
	} else {
		step1.Status = "ok"
		step1.Message = "Connected to PostgreSQL state store (SELECT 1 OK)"
		res.Status = "healthy"
		res.Analysis = DiagnosticAnalysis{
			ProbableCause: "PostgreSQL database is operational and responsive.",
			Confidence:    "HIGH",
			Category:      "HEALTHY",
			SupportingEvidence: []string{
				"PostgreSQL ping probe (SELECT 1) succeeded",
				"Database connection pool is responsive and active",
			},
			RecommendedAction: "No action required.",
			FixAvailable:      false,
		}
	}
	step1.DurationMs = time.Since(step1Start).Milliseconds()
	res.Steps = append(res.Steps, step1)

	return res
}

func (s *Server) recoverDatabase(ctx context.Context, req RecoveryRequest, resp *RecoveryResponse) {
	if s.cfg.HealthPinger == nil {
		resp.Status = "failed"
		resp.Message = "Database health pinger not configured."
		return
	}

	switch req.Action {
	case "reconnect", "retry_probe":
		err := s.cfg.HealthPinger.Ping(ctx)
		if err == nil {
			resp.Status = "recovered"
			resp.VerifiedHealthy = true
			resp.Message = "PostgreSQL connection successfully verified."
			resp.Health = ComponentHealth{Status: "healthy", Details: map[string]any{"connected": true}}
		} else {
			resp.Status = "failed"
			resp.VerifiedHealthy = false
			resp.Message = "Database ping failed: " + err.Error()
			resp.NextSteps = "Start PostgreSQL container: docker compose up -d postgres"
			resp.Health = ComponentHealth{Status: "unavailable", Details: map[string]any{"connected": false}}
		}
	default:
		resp.Status = "failed"
		resp.Message = "Unknown database recovery action: " + req.Action
		resp.NextSteps = "Supported action: reconnect"
	}
}

// ---------------------------------------------------------------------------
// Core Engine Diagnostics & Recovery
// ---------------------------------------------------------------------------

func (s *Server) diagnoseCoreEngine(ctx context.Context) ComponentDiagnosticResult {
	res := ComponentDiagnosticResult{
		Component: "core_engine",
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Steps:     []DiagnosticStep{},
	}

	if s.cfg.CoreChecker == nil {
		res.Status = "not_configured"
		res.Analysis = DiagnosticAnalysis{
			ProbableCause:      "Rust Core Engine gRPC checker not configured.",
			Confidence:         "HIGH",
			Category:           "CONFIGURATION_ERROR",
			SupportingEvidence: []string{"CoreChecker function is nil in server runtime config"},
			RecommendedAction:  "Start wb with --core-engine-addr=127.0.0.1:50051.",
			FixAvailable:       false,
		}
		return res
	}

	step1Start := time.Now()
	err := s.cfg.CoreChecker(ctx)
	step1 := DiagnosticStep{
		Name:    "Core Engine gRPC Liveness Check",
		Details: map[string]any{"transport": "grpc (127.0.0.1:50051)"},
	}

	if err != nil {
		step1.Status = "failed"
		step1.Message = fmt.Sprintf("Liveness RPC check failed: %v", err)
		res.Status = "unavailable"
		res.CurrentError = err.Error()
		res.Analysis = DiagnosticAnalysis{
			ProbableCause: "Rust Core Engine gRPC server is offline or unreachable on port 50051.",
			Confidence:    "HIGH",
			Category:      "GRPC_UNAVAILABLE",
			SupportingEvidence: []string{
				fmt.Sprintf("CheckLiveness RPC returned error: %v", err),
			},
			RecommendedAction: "Start Rust Core Engine via 'target/debug/wb-core-server' or run 'scripts/wb.sh start'.",
			FixType:           "RETRY_CONNECTION",
			FixAvailable:      true,
			Mutating:          false,
			PotentialEffects:  "Re-attempts gRPC connection to 127.0.0.1:50051; does not launch external processes.",
		}
	} else {
		step1.Status = "ok"
		step1.Message = "Core Engine gRPC Liveness check passed"
		res.Status = "healthy"
		res.Analysis = DiagnosticAnalysis{
			ProbableCause: "Rust Core Engine is connected and healthy.",
			Confidence:    "HIGH",
			Category:      "HEALTHY",
			SupportingEvidence: []string{
				"gRPC Liveness check returned OK status from 127.0.0.1:50051",
				"Protobuf RPC transport is operational and responsive",
			},
			RecommendedAction: "No action required.",
			FixAvailable:      false,
		}
	}
	step1.DurationMs = time.Since(step1Start).Milliseconds()
	res.Steps = append(res.Steps, step1)

	return res
}

func (s *Server) recoverCoreEngine(ctx context.Context, req RecoveryRequest, resp *RecoveryResponse) {
	if s.cfg.CoreChecker == nil {
		resp.Status = "failed"
		resp.Message = "Core Engine checker not configured."
		return
	}

	switch req.Action {
	case "retry_connection", "retry_probe":
		err := s.cfg.CoreChecker(ctx)
		if err == nil {
			resp.Status = "recovered"
			resp.VerifiedHealthy = true
			resp.Message = "Core Engine gRPC connection successfully verified."
			resp.Health = ComponentHealth{Status: "healthy", Details: map[string]any{"connected": true}}
		} else {
			resp.Status = "failed"
			resp.VerifiedHealthy = false
			resp.Message = "Core Engine check failed: " + err.Error()
			resp.NextSteps = "Start Core Engine server: target/debug/wb-core-server"
			resp.Health = ComponentHealth{Status: "unavailable", Details: map[string]any{"connected": false}}
		}
	default:
		resp.Status = "failed"
		resp.Message = "Unknown Core Engine recovery action: " + req.Action
		resp.NextSteps = "Supported action: retry_connection"
	}
}

// ---------------------------------------------------------------------------
// API Diagnostics
// ---------------------------------------------------------------------------

func (s *Server) diagnoseAPI() ComponentDiagnosticResult {
	return ComponentDiagnosticResult{
		Component: "api",
		Status:    "healthy",
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Steps: []DiagnosticStep{
			{
				Name:       "Go HTTP API Server Runtime",
				Status:     "ok",
				Message:    "HTTP server is actively serving requests",
				DurationMs: 0,
				Details: map[string]any{
					"port":        s.cfg.Port,
					"environment": s.cfg.Environment,
					"version":     "0.1.0-dev",
				},
			},
		},
		Analysis: DiagnosticAnalysis{
			ProbableCause: "Go HTTP API server is operational.",
			Confidence:    "HIGH",
			Category:      "HEALTHY",
			SupportingEvidence: []string{
				"HTTP listener is actively handling requests on configured port",
				"Internal routing and middleware pipeline are operational",
			},
			RecommendedAction: "No action required.",
			FixAvailable:      false,
		},
	}
}
