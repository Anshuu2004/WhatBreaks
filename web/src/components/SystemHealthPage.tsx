import React, { useState, useEffect, useCallback } from "react";
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
} from "lucide-react";
import type { SystemHealthResponse } from "../types";
import { getSystemHealth, syncDiscovery } from "../api/client";

interface SystemHealthPageProps {
  workspaceId: string;
}

export const SystemHealthPage: React.FC<SystemHealthPageProps> = ({ workspaceId }) => {
  const [health, setHealth] = useState<SystemHealthResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [syncing, setSyncing] = useState(false);
  const [syncResult, setSyncResult] = useState<string | null>(null);

  const fetchHealth = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await getSystemHealth();
      setHealth(data);
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
    <div style={{ padding: "24px 32px", height: "100%", overflowY: "auto" }}>
      {/* ── Page Header ── */}
      <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", marginBottom: 20 }}>
        <div>
          <div className="page-section-label">Telemetry & Operations</div>
          <h1 className="page-heading" style={{ margin: "2px 0 6px 0" }}>System Health</h1>
          <p className="page-subtitle" style={{ margin: 0 }}>
            Real-time status of the WhatBreaks Go API, PostgreSQL persistence, Rust Core Engine, and Kubernetes collector.
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
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))", gap: 16 }}>
        {/* 1. Go API Server */}
        <div
          style={{
            background: "var(--bg-panel)",
            border: "1px solid var(--border)",
            padding: "16px 18px",
            borderRadius: 2,
          }}
        >
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 12 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <Server size={18} style={{ color: "var(--text-secondary)" }} />
              <span style={{ fontWeight: 600, fontSize: 13 }}>Go HTTP API</span>
            </div>
            {getStatusIcon(apiComp?.status || "unavailable")}
          </div>
          <div style={{ fontSize: 12, display: "flex", flexDirection: "column", gap: 6 }}>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span style={{ color: "var(--text-dim)" }}>Status</span>
              <span style={{ fontFamily: "var(--font-mono)", fontWeight: 500 }}>
                {apiComp?.status?.toUpperCase() || "UNAVAILABLE"}
              </span>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span style={{ color: "var(--text-dim)" }}>Port</span>
              <span style={{ fontFamily: "var(--font-mono)" }}>{String(apiComp?.details?.["port"] || 8080)}</span>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span style={{ color: "var(--text-dim)" }}>Version</span>
              <span style={{ fontFamily: "var(--font-mono)" }}>{String(apiComp?.details?.["version"] || "0.1.0-dev")}</span>
            </div>
          </div>
        </div>

        {/* 2. PostgreSQL Store */}
        <div
          style={{
            background: "var(--bg-panel)",
            border: "1px solid var(--border)",
            padding: "16px 18px",
            borderRadius: 2,
          }}
        >
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 12 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <Database size={18} style={{ color: "var(--text-secondary)" }} />
              <span style={{ fontWeight: 600, fontSize: 13 }}>PostgreSQL State Store</span>
            </div>
            {getStatusIcon(dbComp?.status || "unavailable")}
          </div>
          <div style={{ fontSize: 12, display: "flex", flexDirection: "column", gap: 6 }}>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span style={{ color: "var(--text-dim)" }}>Status</span>
              <span style={{ fontFamily: "var(--font-mono)", fontWeight: 500 }}>
                {dbComp?.status?.toUpperCase() || "UNAVAILABLE"}
              </span>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span style={{ color: "var(--text-dim)" }}>Ping Verification</span>
              <span style={{ fontFamily: "var(--font-mono)" }}>
                {dbComp?.details?.["connected"] ? "Connected (SELECT 1 OK)" : "Failed"}
              </span>
            </div>
            {dbComp?.details?.["database"] && (
              <div style={{ display: "flex", justifyContent: "space-between" }}>
                <span style={{ color: "var(--text-dim)" }}>Database</span>
                <span style={{ fontFamily: "var(--font-mono)", fontSize: 11 }}>
                  {String(dbComp.details["host"])}:{String(dbComp.details["port"])}/{String(dbComp.details["database"])}
                </span>
              </div>
            )}
            {dbComp?.message && (
              <div style={{ color: "var(--status-error)", fontSize: 11, marginTop: 4 }}>
                {dbComp.message}
              </div>
            )}
          </div>
        </div>

        {/* 3. Rust Core Engine */}
        <div
          style={{
            background: "var(--bg-panel)",
            border: "1px solid var(--border)",
            padding: "16px 18px",
            borderRadius: 2,
          }}
        >
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 12 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <Cpu size={18} style={{ color: "var(--text-secondary)" }} />
              <span style={{ fontWeight: 600, fontSize: 13 }}>Rust Core Engine</span>
            </div>
            {getStatusIcon(coreComp?.status || "unavailable")}
          </div>
          <div style={{ fontSize: 12, display: "flex", flexDirection: "column", gap: 6 }}>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span style={{ color: "var(--text-dim)" }}>Status</span>
              <span style={{ fontFamily: "var(--font-mono)", fontWeight: 500 }}>
                {coreComp?.status?.toUpperCase() || "UNAVAILABLE"}
              </span>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span style={{ color: "var(--text-dim)" }}>Transport</span>
              <span style={{ fontFamily: "var(--font-mono)" }}>gRPC / Protobuf (127.0.0.1:50051)</span>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span style={{ color: "var(--text-dim)" }}>Liveness Check</span>
              <span style={{ fontFamily: "var(--font-mono)" }}>
                {coreComp?.details?.["connected"] ? "Liveness RPC OK" : "Failed"}
              </span>
            </div>
            {coreComp?.message && (
              <div style={{ color: "var(--status-error)", fontSize: 11, marginTop: 4 }}>
                {coreComp.message}
              </div>
            )}
          </div>
        </div>

        {/* 4. Kubernetes Discovery Collector */}
        <div
          style={{
            background: "var(--bg-panel)",
            border: "1px solid var(--border)",
            padding: "16px 18px",
            borderRadius: 2,
          }}
        >
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 12 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <Layers size={18} style={{ color: "var(--text-secondary)" }} />
              <span style={{ fontWeight: 600, fontSize: 13 }}>Kubernetes Discovery</span>
            </div>
            {getStatusIcon(colComp?.status || "unavailable")}
          </div>
          <div style={{ fontSize: 12, display: "flex", flexDirection: "column", gap: 6 }}>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span style={{ color: "var(--text-dim)" }}>Status</span>
              <span style={{ fontFamily: "var(--font-mono)", fontWeight: 500 }}>
                {colComp?.status?.toUpperCase() || "NOT CONFIGURED"}
              </span>
            </div>
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
            {colComp?.details?.["error"] && (
              <div style={{ color: "var(--status-error)", fontSize: 11, marginTop: 4 }}>
                {String(colComp.details["error"])}
              </div>
            )}
            {colComp?.details?.["last_sampling_at"] && (
              <div style={{ display: "flex", justifyContent: "space-between" }}>
                <span style={{ color: "var(--text-dim)" }}>Last Collection</span>
                <span style={{ fontFamily: "var(--font-mono)", fontSize: 11 }}>
                  {new Date(String(colComp.details["last_sampling_at"])).toLocaleTimeString()}
                </span>
              </div>
            )}
            {colComp?.details?.["last_run_id"] && (
              <div style={{ display: "flex", justifyContent: "space-between" }}>
                <span style={{ color: "var(--text-dim)" }}>Authority Run ID</span>
                <span
                  style={{
                    fontFamily: "var(--font-mono)",
                    fontSize: 10,
                    maxWidth: 160,
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    whiteSpace: "nowrap",
                  }}
                  title={String(colComp.details["last_run_id"])}
                >
                  {String(colComp.details["last_run_id"])}
                </span>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};
