import React, { useState, useEffect, useCallback, useRef } from "react";
import {
  Server,
  Database,
  Cpu,
  Layers,
  RefreshCw,
  AlertTriangle,
  CheckCircle,
  XCircle,
  Clock,
  Radio,
  Wrench,
  Search,
  Copy,
  Check,
  ChevronRight,
  ChevronDown,
  ShieldAlert,
  Terminal,
  X,
  ArrowLeft,
} from "lucide-react";
import type {
  SystemHealthResponse,
  ComponentHealth,
  ComponentDiagnosticResult,
  RecoveryStatus,
  RecoveryResponse,
} from "../types";
import { getSystemHealth, syncDiscovery, getDiagnostics, recoverComponent } from "../api/client";
import { ErrorBoundary } from "./ErrorBoundary";
import {
  normalizeStatus,
  evaluateComponentStatus,
  getStatusStyle,
} from "../utils/status";

const COMPONENT_TITLES: Record<string, string> = {
  api: "Go HTTP API",
  database: "PostgreSQL State Store",
  core_engine: "Rust Core Engine",
  collector: "Kubernetes Discovery",
};

interface SystemHealthPageProps {
  workspaceId: string;
}

export const SystemHealthPage: React.FC<SystemHealthPageProps> = ({ workspaceId }) => {
  const [health, setHealth] = useState<SystemHealthResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [syncing, setSyncing] = useState(false);
  const [syncResult, setSyncResult] = useState<string | null>(null);

  // Recovery tracking per component
  const [recoveryStatuses, setRecoveryStatuses] = useState<Record<string, RecoveryStatus>>({
    api: "not_started",
    database: "not_started",
    core_engine: "not_started",
    collector: "not_started",
  });

  // Modal / Drawer state for guided recovery & diagnostics
  const [activeModalComponent, setActiveModalComponent] = useState<string | null>(null);
  const [modalMode, setModalMode] = useState<"diagnose" | "fix">("diagnose");
  const [diagnosticResult, setDiagnosticResult] = useState<ComponentDiagnosticResult | null>(null);
  const [diagnosing, setDiagnosing] = useState(false);
  const [diagError, setDiagError] = useState<string | null>(null);

  // Recovery execution state
  const [executingFix, setExecutingFix] = useState(false);
  const [confirmMutating, setConfirmMutating] = useState(false);
  const [recoveryResult, setRecoveryResult] = useState<RecoveryResponse | null>(null);
  const [copySuccess, setCopySuccess] = useState(false);
  const [expandedSteps, setExpandedSteps] = useState<Record<number, boolean>>({});

  const fetchHealth = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await getSystemHealth();
      setHealth(data);

      // Update statuses if components became healthy
      setRecoveryStatuses((prev) => {
        const next = { ...prev };
        for (const [key, comp] of Object.entries(data.components || {})) {
          if (comp.status === "healthy" && (next[key] === "recovered" || next[key] === "fixing")) {
            next[key] = "recovered";
          } else if (comp.status === "healthy") {
            // Keep not_started or recovered
            if (next[key] !== "recovered") next[key] = "not_started";
          }
        }
        return next;
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to fetch health telemetry");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchHealth();
    const interval = setInterval(fetchHealth, 15_000);
    return () => clearInterval(interval);
  }, [fetchHealth]);

  // Close drawer on ESC
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape" && activeModalComponent) {
        setActiveModalComponent(null);
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [activeModalComponent]);

  const handleSync = async () => {
    setSyncing(true);
    setSyncResult(null);
    try {
      const res = await syncDiscovery(workspaceId);
      setSyncResult(
        `Discovery sweep completed: ${res.resources_total} resources, ${res.relationships_total} relationships (${res.duration_ms}ms)`
      );
      await fetchHealth();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Sync discovery failed");
    } finally {
      setSyncing(false);
    }
  };

  // Request sequence ref to prevent older or out-of-order responses from overwriting newer results
  const requestSeqRef = useRef<Record<string, number>>({});

  // Run diagnostics for a component
  const runDiagnostics = async (compKey: string, mode: "diagnose" | "fix" = "diagnose") => {
    setActiveModalComponent(compKey);
    setModalMode(mode);
    setDiagnosing(true);
    setDiagError(null);
    setDiagnosticResult(null);
    setRecoveryResult(null);
    setConfirmMutating(false);
    setExpandedSteps({ 0: true, 1: true });

    const seq = (requestSeqRef.current[compKey] || 0) + 1;
    requestSeqRef.current[compKey] = seq;

    setRecoveryStatuses((prev) => ({ ...prev, [compKey]: "diagnosing" }));

    try {
      const resp = await getDiagnostics(workspaceId, compKey);
      if (requestSeqRef.current[compKey] !== seq) {
        // Discard stale response
        return;
      }
      const res = resp?.results?.[compKey];
      if (res) {
        setDiagnosticResult(res);
        const norm = normalizeStatus(res.status);
        if (norm === "HEALTHY") {
          setRecoveryStatuses((prev) => ({
            ...prev,
            [compKey]: prev[compKey] === "fixing" ? "recovered" : "not_started",
          }));
        } else if (res.analysis?.fix_available) {
          setRecoveryStatuses((prev) => ({
            ...prev,
            [compKey]: "fix_available",
          }));
        } else {
          setRecoveryStatuses((prev) => ({
            ...prev,
            [compKey]: "failed",
          }));
        }
      } else {
        throw new Error(`Diagnostic result missing for ${compKey}`);
      }
    } catch (err) {
      if (requestSeqRef.current[compKey] !== seq) {
        return;
      }
      const msg = err instanceof Error ? err.message : "Diagnostics execution failed";
      setDiagError(msg);
      setRecoveryStatuses((prev) => ({ ...prev, [compKey]: "failed" }));
    } finally {
      if (requestSeqRef.current[compKey] === seq) {
        setDiagnosing(false);
      }
    }
  };

  // Execute recovery fix
  const handleExecuteFix = async () => {
    if (!activeModalComponent || !diagnosticResult) return;

    const action = (diagnosticResult.analysis?.fix_type || "").toLowerCase();
    const isMutating = !!diagnosticResult.analysis?.mutating;

    if (isMutating && !confirmMutating) {
      setDiagError("Explicit confirmation is required before restarting the internal collector.");
      return;
    }

    setExecutingFix(true);
    setDiagError(null);
    setRecoveryStatuses((prev) => ({ ...prev, [activeModalComponent]: "fixing" }));

    try {
      let recoveryAction = "retry_probe";
      if (activeModalComponent === "collector") {
        recoveryAction = action.includes("restart") ? "restart_collector" : "retry_probe";
      } else if (activeModalComponent === "database") {
        recoveryAction = "reconnect";
      } else if (activeModalComponent === "core_engine") {
        recoveryAction = "retry_connection";
      }

      // Step 1: Execute recovery action
      const res = await recoverComponent(workspaceId, {
        component: activeModalComponent,
        action: recoveryAction,
        confirmed: confirmMutating,
      });

      // Step 2: Immediate Backend Health Verification Re-check
      const freshHealth = await getSystemHealth();
      setHealth(freshHealth);
      const verifiedComp = freshHealth.components[activeModalComponent];

      const isVerifiedHealthy = verifiedComp?.status === "healthy" && res.verified_healthy;

      setRecoveryResult({
        ...res,
        verified_healthy: isVerifiedHealthy,
        status: isVerifiedHealthy ? "recovered" : "failed",
      });

      setRecoveryStatuses((prev) => ({
        ...prev,
        [activeModalComponent]: isVerifiedHealthy ? "recovered" : "failed",
      }));
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Recovery execution failed";
      setDiagError(msg);
      setRecoveryStatuses((prev) => ({ ...prev, [activeModalComponent]: "failed" }));
    } finally {
      setExecutingFix(false);
    }
  };

  // Copy Sanitized Diagnostics to clipboard
  const handleCopyDiagnostics = () => {
    if (!diagnosticResult) return;

    const reportLines = [
      `# WhatBreaks Diagnostics Report`,
      `Component: ${(diagnosticResult.component || "").toUpperCase()}`,
      `Status: ${(diagnosticResult.status || "").toUpperCase()}`,
      `Timestamp: ${diagnosticResult.timestamp || new Date().toISOString()}`,
      `Current Error: ${diagnosticResult.current_error || "None"}`,
      ``,
      `## Diagnostic Steps:`,
      ...(diagnosticResult.steps ?? []).map(
        (s) =>
          `- [${(s.status || "").toUpperCase()}] ${s.name} (${s.duration_ms ?? 0}ms): ${s.message}`
      ),
      ``,
      `## Analysis:`,
      `- Probable Cause: ${diagnosticResult.analysis?.probable_cause || "None"}`,
      `- Category: ${diagnosticResult.analysis?.category || "UNKNOWN"}`,
      `- Confidence: ${diagnosticResult.analysis?.confidence || "UNKNOWN"}`,
      `- Recommended Action: ${diagnosticResult.analysis?.recommended_action || "None"}`,
      ``,
      `## Supporting Evidence:`,
      ...(diagnosticResult.analysis?.supporting_evidence ?? []).map((e) => `- ${e}`),
      ``,
      `## Potential Effects:`,
      diagnosticResult.analysis?.potential_effects || "None",
      ``,
      `-- Sanitized: Credentials, bearer tokens, and secret payloads omitted --`,
    ];

    if (navigator?.clipboard?.writeText) {
      navigator.clipboard.writeText(reportLines.join("\n"));
      setCopySuccess(true);
      setTimeout(() => setCopySuccess(false), 3000);
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case "healthy":
        return <CheckCircle size={16} style={{ color: "var(--status-ok)" }} />;
      case "degraded":
        return <AlertTriangle size={16} style={{ color: "var(--status-warn)" }} />;
      case "unavailable":
        return <XCircle size={16} style={{ color: "var(--status-error)" }} />;
      default:
        return <Clock size={16} style={{ color: "var(--text-dim)" }} />;
    }
  };

  const formatUptime = (seconds?: number): string => {
    if (!seconds && seconds !== 0) return "—";
    const d = Math.floor(seconds / 86400);
    const h = Math.floor((seconds % 86400) / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = Math.floor(seconds % 60);
    if (d > 0) return `${d}d ${h}h ${m}m`;
    if (h > 0) return `${h}h ${m}m ${s}s`;
    return `${m}m ${s}s`;
  };

  const apiComp = health?.components?.["api"];
  const dbComp = health?.components?.["database"];
  const coreComp = health?.components?.["core_engine"];
  const colComp = health?.components?.["collector"];

  return (
    <div style={{ padding: "24px 32px", height: "100%", overflowY: "auto", position: "relative" }}>
      {/* ── Page Header ── */}
      <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", marginBottom: 20 }}>
        <div>
          <div className="page-section-label">Telemetry & Operations</div>
          <h1 className="page-heading" style={{ margin: "2px 0 6px 0" }}>System Health</h1>
          <p className="page-subtitle" style={{ margin: 0 }}>
            Real-time status, guided root-cause diagnostics, and verifiable recovery actions.
          </p>
        </div>
        <div style={{ display: "flex", gap: 10 }}>
          <button
            className="graph-ctrl-btn"
            onClick={fetchHealth}
            disabled={loading}
            style={{ padding: "6px 12px" }}
          >
            <RefreshCw size={12} className={loading ? "animate-spin" : ""} style={{ marginRight: 6 }} />
            Refresh
          </button>
          <button
            className="analyze-btn"
            onClick={handleSync}
            disabled={syncing}
            style={{ padding: "6px 14px", fontSize: 12 }}
          >
            <Radio size={12} className={syncing ? "animate-spin" : ""} style={{ marginRight: 6 }} />
            {syncing ? "Sweeping Cluster…" : "Sync Discovery"}
          </button>
        </div>
      </div>

      {syncResult && (
        <div
          style={{
            background: "rgba(34, 197, 94, 0.1)",
            border: "1px solid var(--status-ok)",
            color: "var(--status-ok)",
            padding: "8px 14px",
            fontSize: 12,
            fontFamily: "var(--font-mono)",
            marginBottom: 16,
            borderRadius: 2,
          }}
        >
          ✓ {syncResult}
        </div>
      )}

      {error && (
        <div className="error-state" style={{ marginBottom: 16 }}>
          <AlertTriangle size={16} />
          <div>
            <strong>Telemetry Error:</strong> {error}
          </div>
        </div>
      )}

      {/* ── Overall Status Banner ── */}
      <div
        style={{
          background: "var(--bg-panel)",
          border: "1px solid var(--border)",
          padding: "16px 20px",
          marginBottom: 24,
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          flexWrap: "wrap",
          gap: 16,
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          {getStatusIcon(health?.status || "unavailable")}
          <div>
            <div style={{ fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase" }}>
              Overall Operational Status
            </div>
            <div style={{ fontSize: 16, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.04em" }}>
              {health?.status || (loading ? "CONNECTING..." : "UNAVAILABLE")}
            </div>
          </div>
        </div>
        <div style={{ display: "flex", gap: 24, fontSize: 12, color: "var(--text-secondary)" }}>
          <div>
            <span style={{ color: "var(--text-dim)" }}>Uptime: </span>
            <strong style={{ fontFamily: "var(--font-mono)" }}>{formatUptime(health?.uptime)}</strong>
          </div>
          <div>
            <span style={{ color: "var(--text-dim)" }}>Environment: </span>
            <strong style={{ fontFamily: "var(--font-mono)" }}>{health?.environment || "—"}</strong>
          </div>
          <div>
            <span style={{ color: "var(--text-dim)" }}>Telemetry Timestamp: </span>
            <strong style={{ fontFamily: "var(--font-mono)" }}>
              {health?.timestamp ? new Date(health.timestamp).toLocaleTimeString() : "—"}
            </strong>
          </div>
        </div>
      </div>

      {/* ── Component Health Cards ── */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))", gap: 16 }}>
        {/* 1. Go API Server */}
        <ComponentCard
          id="api"
          title="Go HTTP API"
          icon={<Server size={18} style={{ color: "var(--text-secondary)" }} />}
          comp={apiComp}
          recoveryStatus={recoveryStatuses["api"]}
          onDiagnose={() => runDiagnostics("api", "diagnose")}
          onFix={() => runDiagnostics("api", "fix")}
        />

        {/* 2. PostgreSQL Store */}
        <ComponentCard
          id="database"
          title="PostgreSQL State Store"
          icon={<Database size={18} style={{ color: "var(--text-secondary)" }} />}
          comp={dbComp}
          recoveryStatus={recoveryStatuses["database"]}
          onDiagnose={() => runDiagnostics("database", "diagnose")}
          onFix={() => runDiagnostics("database", "fix")}
          extraDetails={
            dbComp?.details?.["database"] ? (
              <div style={{ display: "flex", justifyContent: "space-between" }}>
                <span style={{ color: "var(--text-dim)" }}>Database</span>
                <span style={{ fontFamily: "var(--font-mono)", fontSize: 11 }}>
                  {String(dbComp.details["host"])}:{String(dbComp.details["port"])}/{String(dbComp.details["database"])}
                </span>
              </div>
            ) : null
          }
        />

        {/* 3. Rust Core Engine */}
        <ComponentCard
          id="core_engine"
          title="Rust Core Engine"
          icon={<Cpu size={18} style={{ color: "var(--text-secondary)" }} />}
          comp={coreComp}
          recoveryStatus={recoveryStatuses["core_engine"]}
          onDiagnose={() => runDiagnostics("core_engine", "diagnose")}
          onFix={() => runDiagnostics("core_engine", "fix")}
          extraDetails={
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span style={{ color: "var(--text-dim)" }}>Transport</span>
              <span style={{ fontFamily: "var(--font-mono)" }}>gRPC / Protobuf (127.0.0.1:50051)</span>
            </div>
          }
        />

        {/* 4. Kubernetes Discovery Collector */}
        <ComponentCard
          id="collector"
          title="Kubernetes Discovery"
          icon={<Layers size={18} style={{ color: "var(--text-secondary)" }} />}
          comp={colComp}
          recoveryStatus={recoveryStatuses["collector"]}
          onDiagnose={() => runDiagnostics("collector", "diagnose")}
          onFix={() => runDiagnostics("collector", "fix")}
          extraDetails={
            <>
              <div style={{ display: "flex", justifyContent: "space-between" }}>
                <span style={{ color: "var(--text-dim)" }}>Cluster Context</span>
                <span style={{ fontFamily: "var(--font-mono)" }}>
                  {String(colComp?.details?.["cluster_id"] || "k8s-cluster")}
                </span>
              </div>
              <div style={{ display: "flex", justifyContent: "space-between" }}>
                <span style={{ color: "var(--text-dim)" }}>API Connection</span>
                <span style={{ fontFamily: "var(--font-mono)" }}>
                  {colComp?.details?.["connected"] ? "Connected (readyz OK)" : "Disconnected"}
                </span>
              </div>
              <div style={{ display: "flex", justifyContent: "space-between" }}>
                <span style={{ color: "var(--text-dim)" }}>Process / Ready</span>
                <span style={{ fontFamily: "var(--font-mono)" }}>
                  {colComp?.details?.["alive"] ? "Alive" : "Dead"} · {colComp?.details?.["ready"] ? "Ready" : "Not Ready"}
                </span>
              </div>
            </>
          }
        />
      </div>

      {/* ── Focused Slide-Out Diagnostic & Recovery Drawer ── */}
      {activeModalComponent && (() => {
        const currentCompEval = evaluateComponentStatus({
          telemetryHealth: health?.components?.[activeModalComponent],
          diagnosticResult,
          isChecking: diagnosing,
          hasError: !!diagError,
        });

        return (
          <div
            className="drawer-overlay"
            onClick={(e) => {
              if (e.target === e.currentTarget) setActiveModalComponent(null);
            }}
          >
            <div className="drawer-panel" role="dialog" aria-modal="true" aria-label="Diagnostic Drawer">
              <ErrorBoundary
                fallbackTitle={`Diagnostic drawer encountered an error for ${COMPONENT_TITLES[activeModalComponent] || activeModalComponent}`}
                onReset={() => runDiagnostics(activeModalComponent, modalMode)}
              >
                {/* Drawer Header */}
                <div
                  style={{
                    padding: "16px 20px",
                    borderBottom: "1px solid var(--border)",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "space-between",
                    background: "var(--bg-card)",
                  }}
                >
                  <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                    <button
                      className="graph-ctrl-btn"
                      onClick={() => setActiveModalComponent(null)}
                      style={{ padding: "4px 8px", display: "flex", alignItems: "center", gap: 5, fontSize: 11 }}
                      title="Back to System Health (ESC)"
                      data-testid="close-drawer-btn"
                    >
                      <ArrowLeft size={13} />
                      Back
                    </button>
                    <div style={{ width: 1, height: 16, background: "var(--border)" }} />
                    <Terminal size={16} style={{ color: "var(--text-secondary)" }} />
                    <span style={{ fontWeight: 600, fontSize: 13, letterSpacing: "0.02em" }}>
                      {COMPONENT_TITLES[activeModalComponent] || activeModalComponent.toUpperCase()}
                    </span>
                    {/* Authoritative Status Badge */}
                    <span
                      className="badge"
                      data-testid="header-authoritative-badge"
                      style={{
                        background: getStatusStyle(currentCompEval.authoritativeStatus).background,
                        color: getStatusStyle(currentCompEval.authoritativeStatus).color,
                        border: `1px solid ${getStatusStyle(currentCompEval.authoritativeStatus).borderColor}`,
                      }}
                    >
                      {diagnosing ? (
                        <RefreshCw size={10} className="animate-spin" style={{ marginRight: 4 }} />
                      ) : currentCompEval.authoritativeStatus === "HEALTHY" ? (
                        <CheckCircle size={10} style={{ marginRight: 4 }} />
                      ) : currentCompEval.authoritativeStatus === "DEGRADED" ? (
                        <AlertTriangle size={10} style={{ marginRight: 4 }} />
                      ) : currentCompEval.authoritativeStatus === "FAILED" ? (
                        <XCircle size={10} style={{ marginRight: 4 }} />
                      ) : (
                        <Clock size={10} style={{ marginRight: 4 }} />
                      )}
                      {getStatusStyle(currentCompEval.authoritativeStatus).label}
                    </span>
                    {recoveryStatuses[activeModalComponent] === "fix_available" &&
                      currentCompEval.authoritativeStatus !== "HEALTHY" && (
                        <span
                          className="badge"
                          style={{
                            background: "rgba(234, 179, 8, 0.15)",
                            color: "var(--status-warn)",
                            border: "1px solid var(--status-warn)",
                          }}
                        >
                          <Wrench size={10} style={{ marginRight: 4 }} />
                          FIX AVAILABLE
                        </span>
                      )}
                    {recoveryStatuses[activeModalComponent] === "fixing" && (
                      <span
                        className="badge"
                        style={{
                          background: "rgba(168, 85, 247, 0.15)",
                          color: "#a855f7",
                          border: "1px solid #a855f7",
                        }}
                      >
                        <RefreshCw size={10} className="animate-spin" style={{ marginRight: 4 }} />
                        FIXING...
                      </span>
                    )}
                  </div>
                  <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                    <button
                      className="graph-ctrl-btn"
                      onClick={() => runDiagnostics(activeModalComponent, modalMode)}
                      disabled={diagnosing || executingFix}
                      style={{ padding: "5px 12px", fontSize: 11, display: "flex", alignItems: "center", gap: 5 }}
                      title="Rerun Diagnostics"
                      data-testid="retry-diagnostics-btn"
                    >
                      <RefreshCw size={12} className={diagnosing ? "animate-spin" : ""} />
                      {diagnosing ? "Checking..." : "Run Check"}
                    </button>
                    <button
                      className="graph-ctrl-btn"
                      onClick={() => setActiveModalComponent(null)}
                      style={{ padding: 4 }}
                      title="Close (ESC)"
                    >
                      <X size={16} />
                    </button>
                  </div>
                </div>

                {/* Drawer Scrollable Content */}
                <div style={{ padding: "20px 24px", overflowY: "auto", flex: 1, display: "flex", flexDirection: "column", gap: 16 }}>
                  {/* 1. Component Status & Timestamp Summary */}
                  <div
                    style={{
                      background: "var(--bg-card)",
                      border: "1px solid var(--border)",
                      padding: "14px 16px",
                      borderRadius: 2,
                      display: "flex",
                      justifyContent: "space-between",
                      alignItems: "center",
                      flexWrap: "wrap",
                      gap: 12,
                    }}
                  >
                    <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                      {getStatusIcon(currentCompEval.authoritativeStatus.toLowerCase())}
                      <div>
                        <div style={{ fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase" }}>Authoritative Component Status</div>
                        <div style={{ fontSize: 14, fontWeight: 600, fontFamily: "var(--font-mono)" }}>
                          {currentCompEval.authoritativeStatus}
                        </div>
                      </div>
                    </div>
                    <div style={{ textAlign: "right", fontSize: 11, color: "var(--text-dim)" }}>
                      <div>TELEMETRY TIMESTAMP</div>
                      <div style={{ fontFamily: "var(--font-mono)", color: "var(--text-secondary)" }}>
                        {diagnosticResult?.timestamp
                          ? new Date(diagnosticResult.timestamp).toLocaleTimeString()
                          : health?.timestamp
                          ? new Date(health.timestamp).toLocaleTimeString()
                          : "—"}
                      </div>
                    </div>
                  </div>

                  {/* Discrepancy Callout between Telemetry and On-Demand Probe */}
                  {currentCompEval.hasDiscrepancy && (
                    <div
                      data-testid="status-discrepancy-callout"
                      style={{
                        background: "rgba(234, 179, 8, 0.08)",
                        border: "1px solid var(--status-warn)",
                        padding: "12px 16px",
                        borderRadius: 2,
                        display: "flex",
                        flexDirection: "column",
                        gap: 6,
                        fontSize: 12,
                      }}
                    >
                      <div style={{ display: "flex", alignItems: "center", gap: 8, color: "var(--status-warn)", fontWeight: 600 }}>
                        <AlertTriangle size={15} />
                        <span>Status Discrepancy: Telemetry vs. On-Demand Diagnostic Probe</span>
                      </div>
                      <div style={{ color: "var(--text-secondary)", lineHeight: 1.5 }}>
                        {currentCompEval.discrepancyNote}
                      </div>
                      <div style={{ display: "flex", gap: 16, marginTop: 4, fontSize: 11, fontFamily: "var(--font-mono)" }}>
                        <span>Background Telemetry: <strong style={{ color: getStatusStyle(currentCompEval.telemetryStatus).color }}>{currentCompEval.telemetryStatus}</strong></span>
                        <span>Live Diagnostic Probe: <strong style={{ color: currentCompEval.diagnosticStatus ? getStatusStyle(currentCompEval.diagnosticStatus).color : "inherit" }}>{currentCompEval.diagnosticStatus || "N/A"}</strong></span>
                      </div>
                    </div>
                  )}

                {/* 2. Failure State or Healthy Confirmation Banner */}
                {diagnosticResult?.status === "healthy" && !diagnosticResult?.current_error ? (
                  <div
                    style={{
                      background: "rgba(34, 197, 94, 0.08)",
                      border: "1px solid var(--status-ok)",
                      padding: "12px 16px",
                      borderRadius: 2,
                      display: "flex",
                      alignItems: "center",
                      gap: 10,
                    }}
                  >
                    <CheckCircle size={16} style={{ color: "var(--status-ok)", flexShrink: 0 }} />
                    <div style={{ fontSize: 12, color: "var(--text-primary)" }}>
                      All verification probes passed. Component is operating normally within health parameters.
                    </div>
                  </div>
                ) : (
                  <div
                    style={{
                      background: "rgba(239, 68, 68, 0.08)",
                      border: "1px solid var(--status-error)",
                      padding: "12px 16px",
                      borderRadius: 2,
                    }}
                  >
                    <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
                      <ShieldAlert size={16} style={{ color: "var(--status-error)" }} />
                      <span style={{ fontSize: 11, fontWeight: 600, color: "var(--status-error)", textTransform: "uppercase" }}>
                        Observed Failure / Reason
                      </span>
                    </div>
                    <div style={{ fontFamily: "var(--font-mono)", fontSize: 12, color: "var(--text-primary)", wordBreak: "break-all" }}>
                      {diagnosticResult?.current_error ||
                        (health?.components[activeModalComponent]?.details?.["error"]
                          ? String(health.components[activeModalComponent].details["error"])
                          : health?.components[activeModalComponent]?.message || "Component degraded or health probe failed.")}
                    </div>
                  </div>
                )}

                {/* 3. Kubernetes Discovery Specific Verification & Context Check */}
                {activeModalComponent === "collector" && (
                  <div
                    style={{
                      background: "var(--bg-card)",
                      border: "1px solid var(--border)",
                      padding: "14px 16px",
                      borderRadius: 2,
                    }}
                  >
                    <div style={{ fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase", marginBottom: 10, fontWeight: 600 }}>
                      Kubernetes Discovery State & Reachability Analysis
                    </div>
                    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10, fontSize: 12, marginBottom: 12 }}>
                      <div>
                        <span style={{ color: "var(--text-dim)" }}>In-Process Collector: </span>
                        <strong style={{ fontFamily: "var(--font-mono)" }}>
                          {colComp?.details?.["alive"] ? "Alive (Running)" : "Stopped"} · {colComp?.details?.["ready"] ? "Ready" : "Not Ready"}
                        </strong>
                      </div>
                      <div>
                        <span style={{ color: "var(--text-dim)" }}>Kubernetes API Reachability: </span>
                        <strong style={{ fontFamily: "var(--font-mono)" }}>
                          {colComp?.details?.["connected"] ? "Connected (readyz OK)" : "Degraded / Subsystem Check"}
                        </strong>
                      </div>
                      <div>
                        <span style={{ color: "var(--text-dim)" }}>Configured Cluster: </span>
                        <span style={{ fontFamily: "var(--font-mono)" }}>
                          {String(colComp?.details?.["cluster_id"] || "k3d-license-service")}
                        </span>
                      </div>
                      <div>
                        <span style={{ color: "var(--text-dim)" }}>Active Kube Context: </span>
                        <span style={{ fontFamily: "var(--font-mono)" }}>
                          {String(
                            diagnosticResult?.steps?.find((s) => s.name.includes("Context"))?.details?.["active_kube_context"] ||
                            colComp?.details?.["cluster_id"] ||
                            "k3d-license-service"
                          )}
                        </span>
                      </div>
                    </div>
                    <div
                      style={{
                        padding: "8px 12px",
                        background: "var(--bg-panel)",
                        border: "1px solid var(--border)",
                        fontSize: 11,
                        color: "var(--text-secondary)",
                        lineHeight: 1.5,
                      }}
                    >
                      🛡 <strong>Safe Invariant:</strong> WhatBreaks Discovery maintains strict read-only observation. Credentials, kubeconfig contexts, and Kubernetes workloads are never automatically altered.
                    </div>
                  </div>
                )}

                {/* 4. Diagnosing Spinner */}
                {diagnosing && (
                  <div style={{ padding: "30px 0", textAlign: "center", color: "var(--text-dim)" }}>
                    <RefreshCw size={24} className="animate-spin" style={{ margin: "0 auto 12px auto", display: "block" }} />
                    <div style={{ fontSize: 13, fontFamily: "var(--font-mono)" }}>
                      Executing backend verification probes & reachability checks...
                    </div>
                  </div>
                )}

                {/* 5. Diagnostic Error Banner (if fetch failed) */}
                {diagError && (
                  <div className="error-state" style={{ marginBottom: 12 }}>
                    <AlertTriangle size={16} />
                    <div style={{ flex: 1 }}>
                      <strong>Diagnostic Error:</strong> {diagError}
                    </div>
                    <button
                      className="graph-ctrl-btn"
                      onClick={() => runDiagnostics(activeModalComponent, modalMode)}
                      style={{ fontSize: 11, padding: "4px 8px" }}
                    >
                      Retry
                    </button>
                  </div>
                )}

                {/* 6. Diagnostic Results Body (when loaded) */}
                {diagnosticResult && !diagnosing && (
                  <>
                    {/* Executed Probes */}
                    <div>
                      <div style={{ fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase", marginBottom: 8, fontWeight: 600 }}>
                        Executed Verification Probes ({(diagnosticResult.steps ?? []).length})
                      </div>
                      <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                        {(diagnosticResult.steps ?? []).map((step, idx) => {
                          const isExpanded = !!expandedSteps[idx];
                          return (
                            <div
                              key={idx}
                              style={{
                                border: "1px solid var(--border)",
                                background: "var(--bg-card)",
                                borderRadius: 2,
                                overflow: "hidden",
                              }}
                            >
                              <div
                                onClick={() => setExpandedSteps((p) => ({ ...p, [idx]: !p[idx] }))}
                                style={{
                                  padding: "10px 14px",
                                  display: "flex",
                                  alignItems: "center",
                                  justifyContent: "space-between",
                                  cursor: "pointer",
                                  userSelect: "none",
                                }}
                              >
                                <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                                  {step.status === "ok" ? (
                                    <CheckCircle size={15} style={{ color: "var(--status-ok)" }} />
                                  ) : step.status === "warn" ? (
                                    <AlertTriangle size={15} style={{ color: "var(--status-warn)" }} />
                                  ) : step.status === "failed" ? (
                                    <XCircle size={15} style={{ color: "var(--status-error)" }} />
                                  ) : (
                                    <Clock size={15} style={{ color: "var(--text-dim)" }} />
                                  )}
                                  <span style={{ fontSize: 13, fontWeight: 500 }}>{step.name}</span>
                                </div>
                                <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                                  <span style={{ fontSize: 11, color: "var(--text-dim)", fontFamily: "var(--font-mono)" }}>
                                    {step.duration_ms ?? 0}ms
                                  </span>
                                  {isExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                                </div>
                              </div>
                              {isExpanded && (
                                <div
                                  style={{
                                    padding: "8px 14px 12px 14px",
                                    borderTop: "1px solid var(--border)",
                                    background: "rgba(0,0,0,0.2)",
                                    fontSize: 12,
                                    fontFamily: "var(--font-mono)",
                                  }}
                                >
                                  <div style={{ color: "var(--text-secondary)", marginBottom: 6 }}>{step.message}</div>
                                  {step.details && Object.keys(step.details).length > 0 && (
                                    <pre
                                      style={{
                                        margin: 0,
                                        padding: "8px 10px",
                                        background: "var(--bg-panel)",
                                        border: "1px solid var(--border)",
                                        fontSize: 11,
                                        overflowX: "auto",
                                        color: "var(--text-secondary)",
                                      }}
                                    >
                                      {JSON.stringify(step.details, null, 2)}
                                    </pre>
                                  )}
                                </div>
                              )}
                            </div>
                          );
                        })}
                      </div>
                    </div>

                    {/* Root Cause Assessment */}
                    <div
                      style={{
                        background: "var(--bg-card)",
                        border: "1px solid var(--border)",
                        padding: "14px 16px",
                        borderRadius: 2,
                      }}
                    >
                      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 8 }}>
                        <div style={{ fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase", fontWeight: 600 }}>
                          Root Cause Assessment
                        </div>
                        <div style={{ display: "flex", gap: 8 }}>
                          <span className="badge" style={{ background: "rgba(59, 130, 246, 0.15)", color: "var(--status-info)" }}>
                            {diagnosticResult.analysis?.category || "UNKNOWN"}
                          </span>
                          <span
                            className="badge"
                            style={{
                              background:
                                diagnosticResult.analysis?.confidence === "HIGH"
                                  ? "rgba(34, 197, 94, 0.15)"
                                  : "rgba(234, 179, 8, 0.15)",
                              color:
                                diagnosticResult.analysis?.confidence === "HIGH"
                                  ? "var(--status-ok)"
                                  : "var(--status-warn)",
                            }}
                          >
                            {diagnosticResult.analysis?.confidence || "MEDIUM"} CONFIDENCE
                          </span>
                        </div>
                      </div>
                      <p style={{ margin: "0 0 10px 0", fontSize: 13, lineHeight: 1.5 }}>
                        {diagnosticResult.analysis?.probable_cause || "No probable cause identified."}
                      </p>

                      {(diagnosticResult.analysis?.supporting_evidence ?? []).length > 0 && (
                        <div style={{ borderTop: "1px solid var(--border)", paddingTop: 10 }}>
                          <div style={{ fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase", marginBottom: 6 }}>
                            Supporting Diagnostic Evidence:
                          </div>
                          <ul style={{ margin: 0, paddingLeft: 18, fontSize: 12, color: "var(--text-secondary)", lineHeight: 1.6 }}>
                            {(diagnosticResult.analysis?.supporting_evidence ?? []).map((ev, i) => (
                              <li key={i} style={{ fontFamily: "var(--font-mono)" }}>
                                {ev}
                              </li>
                            ))}
                          </ul>
                        </div>
                      )}
                    </div>

                    {/* Recommended Action & Remediation */}
                    <div
                      style={{
                        background: "rgba(34, 197, 94, 0.05)",
                        border: "1px solid rgba(34, 197, 94, 0.3)",
                        padding: "14px 16px",
                        borderRadius: 2,
                      }}
                    >
                      <div style={{ fontSize: 11, color: "var(--status-ok)", textTransform: "uppercase", fontWeight: 600, marginBottom: 6 }}>
                        Actionable Remediation:
                      </div>
                      <div style={{ fontSize: 13, color: "var(--text-primary)", marginBottom: 8, lineHeight: 1.5 }}>
                        {diagnosticResult.analysis?.recommended_action || "Verify component configuration and connectivity."}
                      </div>

                      {diagnosticResult.analysis?.potential_effects && (
                        <div style={{ fontSize: 11, color: "var(--text-dim)", fontFamily: "var(--font-mono)" }}>
                          <strong>Effects:</strong> {diagnosticResult.analysis.potential_effects}
                        </div>
                      )}

                      {diagnosticResult.analysis?.mutating && (
                        <div
                          style={{
                            marginTop: 12,
                            padding: "10px 12px",
                            background: "var(--bg-panel)",
                            border: "1px solid var(--border)",
                            display: "flex",
                            alignItems: "center",
                            gap: 10,
                          }}
                        >
                          <input
                            type="checkbox"
                            id="confirm-mutating-checkbox"
                            checked={confirmMutating}
                            onChange={(e) => setConfirmMutating(e.target.checked)}
                            style={{ cursor: "pointer", accentColor: "var(--accent)" }}
                          />
                          <label htmlFor="confirm-mutating-checkbox" style={{ fontSize: 12, cursor: "pointer" }}>
                            I confirm execution of in-process collector restart (safe in-memory reset; does not alter Kubernetes resources or workloads).
                          </label>
                        </div>
                      )}
                    </div>

                    {/* Post-Fix Outcome Banner */}
                    {recoveryResult && (
                      <div
                        style={{
                          padding: "12px 16px",
                          background: recoveryResult.verified_healthy ? "rgba(34, 197, 94, 0.1)" : "rgba(239, 68, 68, 0.1)",
                          border: `1px solid ${recoveryResult.verified_healthy ? "var(--status-ok)" : "var(--status-error)"}`,
                          borderRadius: 2,
                        }}
                      >
                        <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
                          {recoveryResult.verified_healthy ? (
                            <CheckCircle size={16} style={{ color: "var(--status-ok)" }} />
                          ) : (
                            <XCircle size={16} style={{ color: "var(--status-error)" }} />
                          )}
                          <span
                            style={{
                              fontSize: 12,
                              fontWeight: 600,
                              color: recoveryResult.verified_healthy ? "var(--status-ok)" : "var(--status-error)",
                              textTransform: "uppercase",
                            }}
                          >
                            {recoveryResult.verified_healthy ? "Recovery Verified Healthy by Backend" : "Recovery Verification Failed"}
                          </span>
                        </div>
                        <div style={{ fontSize: 12, fontFamily: "var(--font-mono)", color: "var(--text-primary)" }}>
                          {recoveryResult.message}
                        </div>
                        {recoveryResult.next_steps && (
                          <div style={{ marginTop: 6, fontSize: 11, color: "var(--text-secondary)" }}>
                            <strong>Next Steps:</strong> {recoveryResult.next_steps}
                          </div>
                        )}
                      </div>
                    )}
                  </>
                )}
              </div>

              {/* Drawer Footer Controls */}
              <div
                style={{
                  padding: "14px 20px",
                  borderTop: "1px solid var(--border)",
                  background: "var(--bg-card)",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "space-between",
                  flexWrap: "wrap",
                  gap: 12,
                }}
              >
                <div style={{ display: "flex", gap: 8 }}>
                  <button
                    className="graph-ctrl-btn"
                    onClick={handleCopyDiagnostics}
                    disabled={!diagnosticResult}
                    style={{ padding: "6px 14px", display: "flex", alignItems: "center", gap: 6 }}
                  >
                    {copySuccess ? <Check size={14} style={{ color: "var(--status-ok)" }} /> : <Copy size={14} />}
                    {copySuccess ? "Copied Report!" : "Copy Diagnostics"}
                  </button>
                  <button
                    className="graph-ctrl-btn"
                    onClick={() => setActiveModalComponent(null)}
                    style={{ padding: "6px 14px" }}
                  >
                    Close
                  </button>
                </div>

                <div style={{ display: "flex", gap: 10 }}>
                  <button
                    className="graph-ctrl-btn"
                    onClick={() => runDiagnostics(activeModalComponent, modalMode)}
                    disabled={diagnosing || executingFix}
                    style={{ padding: "6px 14px", display: "flex", alignItems: "center", gap: 6 }}
                  >
                    <RefreshCw size={12} className={diagnosing ? "animate-spin" : ""} />
                    Re-Diagnose
                  </button>

                  {diagnosticResult?.analysis?.fix_available && (
                    <button
                      className="analyze-btn"
                      onClick={handleExecuteFix}
                      disabled={executingFix || diagnosing || (diagnosticResult.analysis?.mutating && !confirmMutating)}
                      style={{ padding: "6px 16px", display: "flex", alignItems: "center", gap: 6, fontSize: 12 }}
                    >
                      <Wrench size={13} className={executingFix ? "animate-spin" : ""} />
                      {executingFix ? "Executing Fix & Verifying..." : "Execute Fix"}
                    </button>
                  )}
                </div>
              </div>
            </ErrorBoundary>
          </div>
        </div>
      );
    })()}
    </div>
  );
};

interface ComponentCardProps {
  id: string;
  title: string;
  icon: React.ReactNode;
  comp?: ComponentHealth;
  recoveryStatus: RecoveryStatus;
  onDiagnose: () => void;
  onFix: () => void;
  extraDetails?: React.ReactNode;
}

const ComponentCard: React.FC<ComponentCardProps> = ({
  id,
  title,
  icon,
  comp,
  recoveryStatus,
  onDiagnose,
  onFix,
  extraDetails,
}) => {
  const isDegraded = comp?.status === "degraded" || comp?.status === "unavailable";
  const errorMessage = comp?.details?.["error"] ? String(comp.details["error"]) : comp?.message;

  const getStatusIcon = (status: string) => {
    switch (status) {
      case "healthy":
        return <CheckCircle size={16} style={{ color: "var(--status-ok)" }} />;
      case "degraded":
        return <AlertTriangle size={16} style={{ color: "var(--status-warn)" }} />;
      case "unavailable":
        return <XCircle size={16} style={{ color: "var(--status-error)" }} />;
      default:
        return <Clock size={16} style={{ color: "var(--text-dim)" }} />;
    }
  };

  const getRecoveryBadge = (st: RecoveryStatus) => {
    const isHealthy = comp?.status === "healthy";
    if (isHealthy && st === "failed") {
      return null;
    }
    switch (st) {
      case "diagnosing":
        return (
          <span className="badge" style={{ background: "rgba(59, 130, 246, 0.15)", color: "var(--status-info)", border: "1px solid var(--status-info)" }}>
            DIAGNOSING
          </span>
        );
      case "fix_available":
        if (isHealthy) return null;
        return (
          <span className="badge" style={{ background: "rgba(234, 179, 8, 0.15)", color: "var(--status-warn)", border: "1px solid var(--status-warn)" }}>
            FIX AVAILABLE
          </span>
        );
      case "fixing":
        return (
          <span className="badge" style={{ background: "rgba(168, 85, 247, 0.15)", color: "#a855f7", border: "1px solid #a855f7" }}>
            FIXING...
          </span>
        );
      case "recovered":
        return (
          <span className="badge" style={{ background: "rgba(34, 197, 94, 0.15)", color: "var(--status-ok)", border: "1px solid var(--status-ok)" }}>
            RECOVERED
          </span>
        );
      case "failed":
        if (isHealthy) return null;
        return (
          <span className="badge" style={{ background: "rgba(239, 68, 68, 0.15)", color: "var(--status-error)", border: "1px solid var(--status-error)" }}>
            FAILED
          </span>
        );
      default:
        return null;
    }
  };

  return (
    <div
      style={{
        background: "var(--bg-panel)",
        border: isDegraded ? "1px solid var(--status-warn)" : "1px solid var(--border)",
        padding: "16px 18px",
        borderRadius: 2,
        display: "flex",
        flexDirection: "column",
        justifyContent: "space-between",
      }}
    >
      <div>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 12 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            {icon}
            <span style={{ fontWeight: 600, fontSize: 13 }}>{title}</span>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
            {getRecoveryBadge(recoveryStatus)}
            {getStatusIcon(comp?.status || "unavailable")}
          </div>
        </div>

        <div style={{ fontSize: 12, display: "flex", flexDirection: "column", gap: 6 }}>
          <div style={{ display: "flex", justifyContent: "space-between" }}>
            <span style={{ color: "var(--text-dim)" }}>Status</span>
            <span style={{ fontFamily: "var(--font-mono)", fontWeight: 500 }}>
              {comp?.status?.toUpperCase() || "UNAVAILABLE"}
            </span>
          </div>

          {extraDetails}

          {errorMessage && (
            <div
              style={{
                color: "var(--status-error)",
                fontSize: 11,
                marginTop: 6,
                padding: "6px 8px",
                background: "rgba(239, 68, 68, 0.08)",
                border: "1px solid rgba(239, 68, 68, 0.2)",
                borderRadius: 2,
                fontFamily: "var(--font-mono)",
                wordBreak: "break-all",
              }}
            >
              {errorMessage}
            </div>
          )}
        </div>
      </div>

      {/* Action Footer */}
      <div
        style={{
          marginTop: 16,
          paddingTop: 12,
          borderTop: "1px solid var(--border)",
          display: "flex",
          alignItems: "center",
          justifyContent: "flex-end",
          gap: 8,
        }}
      >
        <button
          id={`diagnose-btn-${id}`}
          data-testid={`diagnose-btn-${id}`}
          className="graph-ctrl-btn"
          onClick={onDiagnose}
          style={{ padding: "5px 12px", fontSize: 11, display: "flex", alignItems: "center", gap: 5 }}
        >
          <Search size={12} />
          Diagnose
        </button>

        {isDegraded && (
          <button
            className="analyze-btn"
            onClick={onFix}
            style={{ padding: "5px 14px", fontSize: 11, display: "flex", alignItems: "center", gap: 5 }}
          >
            <Wrench size={12} />
            Fix issue
          </button>
        )}
      </div>
    </div>
  );
};
