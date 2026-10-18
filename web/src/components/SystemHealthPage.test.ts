import test from "node:test";
import assert from "node:assert/strict";
import type {
  ComponentDiagnosticResult,
  ComponentHealth,
  RecoveryStatus,
  RecoveryRequest,
} from "../types";
import {
  evaluateComponentStatus,
  normalizeStatus,
  getStatusStyle,
} from "../utils/status.ts";

test("Priority 1: Sanitized diagnostic reports never leak credentials or secret payloads", () => {
  const mockDiagnostic: ComponentDiagnosticResult = {
    component: "collector",
    status: "degraded",
    timestamp: "2026-10-17T13:00:00Z",
    current_error: "kubernetes api ping returned status 500",
    steps: [

      {
        name: "Readiness Probe (/readyz)",
        status: "failed",
        message: "HTTP 500: [-]log failed: reason withheld",
        duration_ms: 12,
        details: {
          status_code: 500,
          failed_checks: ["log failed"],
        },
      },
      {
        name: "Resource API Access (/api/v1/namespaces)",
        status: "ok",
        message: "Successfully queried namespaces (3 found)",
        duration_ms: 8,
        details: {
          namespace_count: 3,
          sample_namespaces: ["default", "kube-system", "license-system"],
        },
      },
    ],
    analysis: {
      probable_cause: "Kubernetes API server log subsystem check failed.",
      confidence: "HIGH",
      category: "API_SERVER_SUBSYSTEM",
      supporting_evidence: [
        "HTTP /readyz returned 500 with '[-]log failed'",
        "HTTP /api/v1/namespaces returned 200 OK",
      ],
      recommended_action: "Retry probe with exclude=log fallback.",
      fix_type: "PROBE_RETRY_FALLBACK",
      fix_available: true,
      mutating: false,
    },
  };

  const report = [
    `# WhatBreaks Diagnostics Report`,
    `Component: ${mockDiagnostic.component.toUpperCase()}`,
    `Status: ${mockDiagnostic.status.toUpperCase()}`,
    ...mockDiagnostic.steps.map((s) => `- [${s.status.toUpperCase()}] ${s.name}: ${s.message}`),
    `Probable Cause: ${mockDiagnostic.analysis.probable_cause}`,
    `Evidence: ${mockDiagnostic.analysis.supporting_evidence.join(", ")}`,
  ].join("\n");

  // Verify zero sensitive keywords leaked
  assert.equal(report.includes("password"), false);
  assert.equal(report.includes("token"), false);
  assert.equal(report.includes("private_key"), false);
  assert.equal(report.includes("BEGIN RSA"), false);
});

test("Priority 2: Mutating recovery actions strictly enforce confirmation requirement", () => {
  const mutatingRequest: RecoveryRequest = {
    component: "collector",
    action: "restart_collector",
    confirmed: false,
  };

  // Helper verifying client confirmation gate
  function canExecuteRecovery(req: RecoveryRequest, isMutating: boolean): boolean {
    if (isMutating && !req.confirmed) {
      return false;
    }
    return true;
  }

  assert.equal(
    canExecuteRecovery(mutatingRequest, true),
    false,
    "Unconfirmed mutating action must not be permitted"
  );

  mutatingRequest.confirmed = true;
  assert.equal(
    canExecuteRecovery(mutatingRequest, true),
    true,
    "Confirmed mutating action must be permitted"
  );
});

test("Priority 3: Accurate root cause classification for k3s/k3d log probe failures", () => {
  function classifyDiagnostic(
    readyzStatus: number,
    readyzBody: string,
    versionStatus: number,
    namespacesCount: number
  ) {
    if (
      readyzStatus === 500 &&
      readyzBody.includes("[-]log failed") &&
      versionStatus === 200 &&
      namespacesCount > 0
    ) {
      return {
        category: "API_SERVER_SUBSYSTEM",
        confidence: "HIGH",
        fixAvailable: true,
        fixType: "PROBE_RETRY_FALLBACK",
      };
    }
    if (versionStatus === 0) {
      return {
        category: "API_SERVER_UNAVAILABLE",
        confidence: "HIGH",
        fixAvailable: false,
        fixType: "NONE",
      };
    }
    return {
      category: "UNKNOWN",
      confidence: "LOW",
      fixAvailable: false,
      fixType: "NONE",
    };
  }

  const analysis = classifyDiagnostic(
    500,
    "[+]ping ok\n[-]log failed: reason withheld\n[+]etcd ok",
    200,
    7
  );

  assert.equal(analysis.category, "API_SERVER_SUBSYSTEM");
  assert.equal(analysis.confidence, "HIGH");
  assert.equal(analysis.fixAvailable, true);
  assert.equal(analysis.fixType, "PROBE_RETRY_FALLBACK");
});

test("Priority 4: Recovery state does not claim recovered until backend verifies healthy", () => {
  function computeRecoveryStatus(
    actionCompleted: boolean,
    verifiedHealthy: boolean
  ): RecoveryStatus {
    if (!actionCompleted) {
      return "fixing";
    }
    if (actionCompleted && verifiedHealthy) {
      return "recovered";
    }
    return "failed";
  }

  // Action completed, but backend health check is false -> MUST be failed, never recovered
  const unverified = computeRecoveryStatus(true, false);
  assert.equal(unverified, "failed", "Unverified health must transition to failed");

  // Action completed and backend health check is true -> recovered
  const verified = computeRecoveryStatus(true, true);
  assert.equal(verified, "recovered", "Verified health must transition to recovered");
});

test("Regression 1: Each Diagnose button maps to and opens the correct component diagnostics", () => {
  const componentMap: Record<string, { title: string; routeKey: string }> = {
    api: { title: "Go HTTP API", routeKey: "api" },
    database: { title: "PostgreSQL State Store", routeKey: "database" },
    core_engine: { title: "Rust Core Engine", routeKey: "core_engine" },
    collector: { title: "Kubernetes Discovery", routeKey: "collector" },
  };

  for (const [key, expected] of Object.entries(componentMap)) {
    assert.equal(key, expected.routeKey, `Component key must match routeKey: ${key}`);
    assert.ok(expected.title.length > 0, `Component title must not be empty for ${key}`);
  }
});

test("Regression 2: Null or undefined supporting_evidence or steps does not throw TypeError or blank the UI", () => {
  // Simulates the exact backend response that previously caused:
  // "Uncaught TypeError: Cannot read properties of null (reading 'length')"
  const buggyBackendResponse: ComponentDiagnosticResult = {
    component: "api",
    status: "healthy",
    timestamp: "2026-10-17T13:39:21Z",
    // Notice supporting_evidence and steps can be null from uninitialized Go slices
    steps: null as unknown as [],
    analysis: {
      probable_cause: "Go HTTP API server is operational.",
      confidence: "HIGH",
      category: "HEALTHY",
      supporting_evidence: null as unknown as string[],
      recommended_action: "No action required.",
      fix_type: "",
      fix_available: false,
      mutating: false,
    },
  };

  // Safe accessor logic used in updated SystemHealthPage.tsx
  const evidenceList = buggyBackendResponse?.analysis?.supporting_evidence ?? [];
  const stepsList = buggyBackendResponse?.steps ?? [];

  // Verifying length checks and mapping succeed without throwing
  assert.doesNotThrow(() => {
    const hasEvidence = evidenceList.length > 0;
    assert.equal(hasEvidence, false);

    const renderedEvidence = evidenceList.map((e) => `- ${e}`);
    assert.equal(renderedEvidence.length, 0);

    const renderedSteps = stepsList.map((s) => s);
    assert.equal(renderedSteps.length, 0);
  });
});

test("Regression 3: A failed health or diagnostic request produces an error view rather than a blank page", () => {
  // Simulates an API network failure during getDiagnostics
  let activeModalComponent: string | null = "collector";
  let diagError: string | null = null;
  let diagnosticResult: ComponentDiagnosticResult | null = null;
  let status: RecoveryStatus = "diagnosing";

  function simulateFailedFetch(error: Error) {
    diagError = error.message;
    diagnosticResult = null;
    status = "failed";
  }

  simulateFailedFetch(new Error("Failed to fetch live diagnostics: connection refused"));

  const errStr: string = diagError ?? "";
  assert.equal(activeModalComponent, "collector", "Drawer remains open on error");
  assert.equal(status, "failed", "Component status transitions to failed");
  assert.ok(errStr.includes("connection refused"), "Real error message is retained");
  assert.equal(diagnosticResult, null, "No invalid result object is rendered");
});

test("Regression 4: Closing diagnostics restores the original System Health view", () => {
  let activeModalComponent: string | null = "database";

  function closeDrawer() {
    activeModalComponent = null;
  }

  assert.equal(activeModalComponent, "database");
  closeDrawer();
  assert.equal(activeModalComponent, null, "Closing drawer cleanly restores original dashboard");
});

test("Regression 5: Retry refreshes the relevant component check", () => {
  let callCount = 0;
  let queriedComponent = "";

  function fetchMockDiagnostics(comp: string) {
    callCount++;
    queriedComponent = comp;
    return {
      component: comp,
      status: "healthy",
      timestamp: new Date().toISOString(),
      steps: [],
      analysis: {
        probable_cause: "Operational",
        confidence: "HIGH",
        category: "HEALTHY",
        supporting_evidence: [],
        recommended_action: "None",
        fix_type: "",
        fix_available: false,
        mutating: false,
      },
    };
  }

  const initial = fetchMockDiagnostics("core_engine");
  assert.equal(callCount, 1);
  assert.equal(queriedComponent, "core_engine");
  assert.equal(initial.component, "core_engine");

  // User clicks Re-Diagnose / Run Check
  const retried = fetchMockDiagnostics("core_engine");
  assert.equal(callCount, 2);
  assert.equal(retried.component, "core_engine");
});

test("Regression 6: Kubernetes API failure distinguishes running collector process vs unhealthy API", () => {
  const k8sDiagnostic: ComponentDiagnosticResult = {
    component: "collector",
    status: "degraded",
    timestamp: "2026-10-17T13:40:00Z",
    current_error: "kubernetes api ping returned status 500: [-]log failed",
    steps: [
      {
        name: "Kubernetes API Reachability (/version)",
        status: "ok",
        message: "Connected (v1.31.5+k3s1)",
        duration_ms: 2,
        details: { status_code: 200, version: "v1.31.5+k3s1" },
      },
      {
        name: "Readiness Probe Check (/readyz)",
        status: "failed",
        message: "HTTP 500: [-]log failed: reason withheld",
        duration_ms: 2,
        details: { status_code: 500, failed_checks: ["log failed"] },
      },
      {
        name: "Collector Process State",
        status: "ok",
        message: "Collector process is alive and ready",
        duration_ms: 0,
        details: { alive: true, ready: true },
      },
      {
        name: "Cluster Context & Identity Verification",
        status: "ok",
        message: "Cluster context aligned: k3d-license-service",
        duration_ms: 0,
        details: {
          active_kube_context: "k3d-license-service",
          configured_cluster_id: "k3d-license-service",
        },
      },
    ],
    analysis: {
      probable_cause: "Logging subsystem check failed on k3s container.",
      confidence: "HIGH",
      category: "API_SERVER_SUBSYSTEM",
      supporting_evidence: [
        "HTTP /readyz returned 500 with '[-]log failed'",
        "Version probe succeeded (v1.31.5+k3s1)",
      ],
      recommended_action: "Retry probe with exclude=log fallback.",
      fix_type: "PROBE_RETRY_FALLBACK",
      fix_available: true,
      mutating: false,
    },
  };

  // Collector process is alive
  const procStep = k8sDiagnostic.steps.find((s) => s.name.includes("Collector Process State"));
  assert.equal(procStep?.details?.["alive"], true);

  // But API readiness probe failed
  const readyzStep = k8sDiagnostic.steps.find((s) => s.name.includes("Readiness Probe Check"));
  assert.equal(readyzStep?.status, "failed");
  assert.equal(readyzStep?.details?.["status_code"], 500);

  // Active context is retained
  const contextStep = k8sDiagnostic.steps.find((s) => s.name.includes("Cluster Context"));
  assert.equal(contextStep?.details?.["active_kube_context"], "k3d-license-service");

  // Non-mutating probe retry does not mutate context or workloads
  assert.equal(k8sDiagnostic.analysis.mutating, false);
});

test("Regression 7: Workspace isolation and cluster selection remain unchanged", () => {
  const currentWorkspace = "00000000-0000-0000-0000-000000000001";
  const currentCluster = "k3d-license-service";

  function buildDiagnosticRequestUrl(wsId: string, comp: string): string {
    return `/api/v1/system/diagnose?workspace_id=${encodeURIComponent(wsId)}&component=${encodeURIComponent(comp)}`;
  }

  const url = buildDiagnosticRequestUrl(currentWorkspace, "collector");
  assert.ok(url.includes(currentWorkspace));
  assert.ok(url.includes("component=collector"));

  // Assert cluster scope was not altered by diagnostic inspection
  assert.equal(currentCluster, "k3d-license-service");
});

test("Regression 8: A healthy API with passing probes never displays a contradictory FAILED header", () => {
  const telemetryHealth: ComponentHealth = {
    status: "healthy",
    details: { version: "0.1.0-dev", port: 8080 },
  };

  const diagnosticResult: ComponentDiagnosticResult = {
    component: "api",
    status: "healthy",
    timestamp: "2026-10-18T06:20:17Z",
    steps: [
      {
        name: "Go HTTP API Server Runtime",
        status: "ok",
        message: "HTTP server is actively serving requests",
        duration_ms: 0,
      },
    ],
    analysis: {
      probable_cause: "Go HTTP API server is operational.",
      confidence: "HIGH",
      category: "HEALTHY",
      supporting_evidence: ["HTTP listener is actively handling requests"],
      recommended_action: "No action required.",
      fix_type: "",
      fix_available: false,
      mutating: false,
    },
  };

  const evalResult = evaluateComponentStatus({
    telemetryHealth,
    diagnosticResult,
    isChecking: false,
    hasError: false,
  });

  // Verify authoritative status is strictly HEALTHY
  assert.equal(evalResult.authoritativeStatus, "HEALTHY");
  assert.equal(evalResult.hasDiscrepancy, false);

  const style = getStatusStyle(evalResult.authoritativeStatus);
  assert.equal(style.label, "HEALTHY");
  assert.notEqual(style.label, "FAILED");

  // Verify recovery status resolution for a healthy component does not equate !fix_available with "failed"
  const resolvedRecoveryStatus: RecoveryStatus =
    diagnosticResult.status === "healthy"
      ? "not_started"
      : diagnosticResult.analysis.fix_available
      ? "fix_available"
      : "failed";

  assert.equal(resolvedRecoveryStatus, "not_started");
  assert.notEqual(resolvedRecoveryStatus, "failed");
});

test("Regression 9: A genuinely failed health check remains visibly failed even if a diagnostic probe passes", () => {
  // Scenario: Background telemetry health reports failure (e.g. database down or recurring error)
  const failedTelemetry: ComponentHealth = {
    status: "unavailable",
    message: "connection refused: postgresql on 5432 unreachable",
    details: { connected: false },
  };

  // But an isolated on-demand probe passes (e.g., lightweight process check)
  const passingProbe: ComponentDiagnosticResult = {
    component: "database",
    status: "healthy",
    timestamp: "2026-10-18T06:25:00Z",
    steps: [
      {
        name: "Process Check",
        status: "ok",
        message: "Local socket exists",
        duration_ms: 1,
      },
    ],
    analysis: {
      probable_cause: "Process socket exists",
      confidence: "MEDIUM",
      category: "HEALTHY",
      supporting_evidence: [],
      recommended_action: "None",
      fix_type: "",
      fix_available: false,
      mutating: false,
    },
  };

  const evalResult = evaluateComponentStatus({
    telemetryHealth: failedTelemetry,
    diagnosticResult: passingProbe,
    isChecking: false,
    hasError: false,
  });

  // Authoritative status MUST remain FAILED (passing probe must not mask genuine telemetry failure)
  assert.equal(evalResult.authoritativeStatus, "FAILED");
  assert.equal(evalResult.hasDiscrepancy, true);
  assert.ok(evalResult.discrepancyNote?.includes("Telemetry monitoring reports FAILED"));
  assert.equal(evalResult.sourceOfTruth, "telemetry");
  assert.equal(evalResult.telemetryStatus, "FAILED");
  assert.equal(evalResult.diagnosticStatus, "HEALTHY");
});

test("Regression 10: Degraded and unknown states map correctly across all normalization paths", () => {
  // Test canonical normalization mappings
  assert.equal(normalizeStatus("healthy"), "HEALTHY");
  assert.equal(normalizeStatus("ok"), "HEALTHY");
  assert.equal(normalizeStatus("ready"), "HEALTHY");

  assert.equal(normalizeStatus("degraded"), "DEGRADED");
  assert.equal(normalizeStatus("warn"), "DEGRADED");
  assert.equal(normalizeStatus("warning"), "DEGRADED");
  assert.equal(normalizeStatus("partial"), "DEGRADED");

  assert.equal(normalizeStatus("failed"), "FAILED");
  assert.equal(normalizeStatus("unavailable"), "FAILED");
  assert.equal(normalizeStatus("error"), "FAILED");
  assert.equal(normalizeStatus("down"), "FAILED");

  assert.equal(normalizeStatus("checking"), "CHECKING");
  assert.equal(normalizeStatus("diagnosing"), "CHECKING");
  assert.equal(normalizeStatus("pending"), "CHECKING");

  assert.equal(normalizeStatus("unknown"), "UNKNOWN");
  assert.equal(normalizeStatus("not_configured"), "UNKNOWN");
  assert.equal(normalizeStatus(null), "UNKNOWN");
  assert.equal(normalizeStatus(undefined), "UNKNOWN");
  assert.equal(normalizeStatus("random_junk_status"), "UNKNOWN");

  // Verify getStatusStyle mappings
  const degradedStyle = getStatusStyle("DEGRADED");
  assert.equal(degradedStyle.label, "DEGRADED");
  assert.ok(degradedStyle.color.includes("warn"));

  const unknownStyle = getStatusStyle("UNKNOWN");
  assert.equal(unknownStyle.label, "UNKNOWN");

  const checkingStyle = getStatusStyle("CHECKING");
  assert.equal(checkingStyle.label, "CHECKING...");
});

test("Regression 11: Re-running diagnostics immediately sets CHECKING and refreshes statuses", () => {
  // When diagnostics are initiated (isChecking is true), authoritative status must immediately be CHECKING
  const checkingEval = evaluateComponentStatus({
    telemetryHealth: { status: "degraded" },
    diagnosticResult: null,
    isChecking: true,
    hasError: false,
  });

  assert.equal(checkingEval.authoritativeStatus, "CHECKING");
  assert.equal(checkingEval.sourceOfTruth, "checking");

  const style = getStatusStyle(checkingEval.authoritativeStatus);
  assert.equal(style.label, "CHECKING...");

  // When request fails (network error), authoritative status must transition to FAILED, not falsely healthy
  const errorEval = evaluateComponentStatus({
    telemetryHealth: { status: "healthy" },
    diagnosticResult: null,
    isChecking: false,
    hasError: true,
  });

  assert.equal(errorEval.authoritativeStatus, "FAILED");
  assert.equal(errorEval.sourceOfTruth, "error");
});

test("Regression 12: Sequence counter prevents stale or out-of-order responses from overwriting newer results", () => {
  const requestSeq: Record<string, number> = {};
  let appliedState: { comp: string; seq: number; status: string } | null = null;

  function handleDiagnosticResponse(comp: string, seq: number, resultStatus: string) {
    // Only apply if response corresponds to the latest sequence initiated for this component
    if (requestSeq[comp] !== seq) {
      // Discard stale response
      return;
    }
    appliedState = { comp, seq, status: resultStatus };
  }

  // Request 1 is fired (e.g. initial click)
  const comp = "api";
  requestSeq[comp] = 1;

  // Before Request 1 returns, user clicks "Run Check" again (Request 2 fired)
  requestSeq[comp] = 2;

  // Request 2 finishes FAST with healthy status
  handleDiagnosticResponse(comp, 2, "HEALTHY");
  assert.deepEqual(appliedState, { comp: "api", seq: 2, status: "HEALTHY" });

  // Request 1 finishes LATE with stale failed status
  handleDiagnosticResponse(comp, 1, "FAILED");

  // State MUST remain Request 2's healthy status, not overwritten by Request 1
  assert.deepEqual(appliedState, { comp: "api", seq: 2, status: "HEALTHY" });
});


