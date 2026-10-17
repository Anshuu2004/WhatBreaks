import React, { useState, useEffect, useMemo, useCallback } from "react";
import {
  Search,
  RefreshCw,
  AlertCircle,
  XCircle,
  Clock,
  Database,
  Play,
  X,
  Filter,
} from "lucide-react";
import type { ResourceItem, ResourceDetailResponse } from "../types";
import { listResources, getResourceDetail, syncDiscovery } from "../api/client";
import { parseProviderID } from "../utils/resource";

interface ResourcesPageProps {
  workspaceId: string;
  clusterId: string;
  clustersData?: import("../types").ListClustersResponse | null;
  onRefreshClusters?: () => void;
  onNavigateToInvestigate: (res: ResourceItem) => void;
}

export const ResourcesPage: React.FC<ResourcesPageProps> = ({
  workspaceId,
  clusterId,
  clustersData,
  onRefreshClusters,
  onNavigateToInvestigate,
}) => {
  const [resources, setResources] = useState<ResourceItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Filters
  const [search, setSearch] = useState("");
  const [typeFilter, setTypeFilter] = useState("all");
  const [namespaceFilter, setNamespaceFilter] = useState("all");

  // Detail drawer
  const [selectedDetail, setSelectedDetail] = useState<ResourceDetailResponse | null>(null);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);

  // Sync state
  const [syncing, setSyncing] = useState(false);
  const [syncMessage, setSyncMessage] = useState<string | null>(null);

  const fetchResources = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await listResources(workspaceId, {
        cluster: clusterId || undefined,
      });
      setResources(res.resources);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load resources");
      setResources([]);
    } finally {
      setLoading(false);
    }
  }, [workspaceId, clusterId]);

  useEffect(() => {
    fetchResources();
  }, [fetchResources]);

  // Revalidate or clear drawer if cluster scope changes
  useEffect(() => {
    if (!selectedDetail) return;
    const parsed = parseProviderID(selectedDetail.resource.provider_id);
    if (clusterId && clusterId !== "all" && parsed.cluster && parsed.cluster !== clusterId) {
      setSelectedDetail(null);
      setDetailError(null);
    }
  }, [clusterId, selectedDetail]);

  // Handle Sync Discovery
  const handleSync = async () => {
    setSyncing(true);
    setSyncMessage(null);
    try {
      const res = await syncDiscovery(workspaceId);
      setSyncMessage(
        `Discovered ${res.resources_total} resources and ${res.relationships_total} relationships (${res.duration_ms}ms)`
      );
      await fetchResources();
      if (onRefreshClusters) {
        onRefreshClusters();
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Discovery sync failed");
    } finally {
      setSyncing(false);
    }
  };

  // Inspect detail drawer
  const handleOpenDetail = async (res: ResourceItem) => {
    setLoadingDetail(true);
    setDetailError(null);
    setSelectedDetail(null);
    try {
      const detail = await getResourceDetail(workspaceId, {
        provider: res.provider,
        resource_type: res.resource_type,
        provider_id: res.provider_id,
      });
      setSelectedDetail(detail);
    } catch (err) {
      setDetailError(err instanceof Error ? err.message : "Failed to load resource details");
    } finally {
      setLoadingDetail(false);
    }
  };

  // Distinct types & namespaces for filters
  const availableTypes = useMemo(() => {
    const s = new Set<string>();
    resources.forEach((r) => s.add(r.resource_type));
    return Array.from(s).sort();
  }, [resources]);

  const availableNamespaces = useMemo(() => {
    const s = new Set<string>();
    resources.forEach((r) => {
      const parsed = parseProviderID(r.provider_id);
      if (parsed.namespace) s.add(parsed.namespace);
    });
    return Array.from(s).sort();
  }, [resources]);

  // Filtered resources
  const filteredResources = useMemo(() => {
    return resources.filter((r) => {
      const parsed = parseProviderID(r.provider_id);
      if (clusterId && clusterId !== "all" && parsed.cluster && parsed.cluster !== clusterId) {
        return false;
      }
      if (typeFilter !== "all" && r.resource_type !== typeFilter) {
        return false;
      }
      if (namespaceFilter !== "all" && parsed.namespace !== namespaceFilter) {
        return false;
      }
      if (search.trim()) {
        const q = search.toLowerCase();
        return (
          r.provider_id.toLowerCase().includes(q) ||
          r.resource_type.toLowerCase().includes(q) ||
          parsed.name.toLowerCase().includes(q)
        );
      }
      return true;
    });
  }, [resources, clusterId, typeFilter, namespaceFilter, search]);

  return (
    <div style={{ padding: "24px 32px", height: "100%", overflowY: "auto", position: "relative" }}>
      {/* ── Page Header ── */}
      <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", marginBottom: 20 }}>
        <div>
          <div className="page-section-label">Inventory</div>
          <h1 className="page-heading" style={{ margin: "2px 0 6px 0" }}>Discovered Infrastructure</h1>
          <p className="page-subtitle" style={{ margin: 0 }}>
            {clusterId && clusterId !== "all"
              ? `Displaying resources scoped to cluster: ${clusterId}`
              : "Displaying resources across all discovered clusters in workspace."}
          </p>
        </div>
        <div style={{ display: "flex", gap: 10 }}>
          <button
            className="graph-ctrl-btn"
            onClick={fetchResources}
            disabled={loading}
            title="Refresh resource list"
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
            title="Trigger an on-demand Kubernetes cluster discovery sweep"
          >
            <RefreshCw size={12} className={syncing ? "animate-spin" : ""} style={{ marginRight: 6 }} />
            {syncing ? "Syncing Cluster…" : "Sync Discovery"}
          </button>
        </div>
      </div>

      {syncMessage && (
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
          ✓ {syncMessage}
        </div>
      )}

      {error && (
        <div className="error-state" style={{ marginBottom: 16 }}>
          <AlertCircle size={16} />
          <div>{error}</div>
        </div>
      )}

      {/* ── Filter Controls ── */}
      <div
        style={{
          display: "flex",
          gap: 12,
          marginBottom: 16,
          background: "var(--bg-panel)",
          border: "1px solid var(--border)",
          padding: "10px 14px",
          alignItems: "center",
          flexWrap: "wrap",
        }}
      >
        {/* Search */}
        <div style={{ position: "relative", flex: "1 1 240px" }}>
          <Search size={13} style={{ position: "absolute", left: 10, top: 10, color: "var(--text-dim)" }} />
          <input
            type="text"
            className="resource-search-input"
            placeholder="Filter by name, namespace, or ID..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            style={{ paddingLeft: 30 }}
          />
        </div>

        {/* Type Filter */}
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          <Filter size={12} style={{ color: "var(--text-dim)" }} />
          <span style={{ fontSize: 11, color: "var(--text-dim)" }}>Type:</span>
          <select
            className="sidebar-select"
            value={typeFilter}
            onChange={(e) => setTypeFilter(e.target.value)}
            style={{ width: "auto", minWidth: 120 }}
          >
            <option value="all">All Types ({resources.length})</option>
            {availableTypes.map((t) => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </select>
        </div>

        {/* Namespace Filter */}
        {availableNamespaces.length > 0 && (
          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <span style={{ fontSize: 11, color: "var(--text-dim)" }}>Namespace:</span>
            <select
              className="sidebar-select"
              value={namespaceFilter}
              onChange={(e) => setNamespaceFilter(e.target.value)}
              style={{ width: "auto", minWidth: 140 }}
            >
              <option value="all">All Namespaces</option>
              {availableNamespaces.map((ns) => (
                <option key={ns} value={ns}>
                  {ns}
                </option>
              ))}
            </select>
          </div>
        )}

        <div style={{ marginLeft: "auto", fontSize: 11, color: "var(--text-dim)", fontFamily: "var(--font-mono)" }}>
          Showing {filteredResources.length} of {resources.length}
        </div>
      </div>

      {/* ── Table ── */}
      {loading ? (
        <div className="empty-state" style={{ padding: "60px 0" }}>
          <span className="loading-dots">
            <span className="loading-dot" />
            <span className="loading-dot" />
            <span className="loading-dot" />
          </span>
          <div className="empty-state-title">Loading resource inventory…</div>
        </div>
      ) : filteredResources.length === 0 ? (
        (() => {
          const selectedCluster = clustersData?.clusters?.find((c) => c.name === clusterId);
          const activeCluster = clustersData?.clusters?.find((c) => c.active) || clustersData?.clusters?.[0];
          const targetCluster = selectedCluster || (clusterId ? null : activeCluster);
          const isConfigured = Boolean(clustersData && clustersData.configured);
          const clusterStatus = !isConfigured ? "not_configured" : (targetCluster?.status || "pending_sync");

          if (!isConfigured || clusterStatus === "not_configured") {
            return (
              <div className="empty-state" style={{ padding: "60px 0" }}>
                <AlertCircle size={32} style={{ color: "var(--text-dim)" }} />
                <div className="empty-state-title">No Kubernetes Cluster Configured</div>
                <div className="empty-state-sub">
                  No Kubernetes cluster context is configured for this workspace. Configure a cluster context or start WhatBreaks with Kubernetes discovery enabled.
                </div>
              </div>
            );
          }

          if (clusterStatus === "unreachable") {
            return (
              <div className="empty-state" style={{ padding: "60px 0" }}>
                <XCircle size={32} style={{ color: "var(--status-error)" }} />
                <div className="empty-state-title" style={{ color: "var(--status-error)" }}>
                  Cluster Unreachable
                </div>
                <div className="empty-state-sub">
                  Target cluster <strong>{targetCluster?.name || clusterId}</strong> cannot be reached: {targetCluster?.error || "Connection refused"}.
                  <br />
                  <span style={{ display: "inline-block", marginTop: 8 }}>
                    Action: Verify your cluster is running and <code>kubectl proxy --port=8001</code> is active.
                  </span>
                </div>
              </div>
            );
          }

          if (clusterStatus === "pending_sync") {
            return (
              <div className="empty-state" style={{ padding: "60px 0" }}>
                <Clock size={32} style={{ color: "var(--status-warn)" }} />
                <div className="empty-state-title">Connected · Initial Collection Required</div>
                <div className="empty-state-sub">
                  Connected to cluster <strong>{targetCluster?.name || clusterId || "k8s-cluster"}</strong>, but resources have not yet been synchronized into workspace <code>{workspaceId}</code>.
                </div>
                <button
                  className="analyze-btn"
                  onClick={handleSync}
                  disabled={syncing}
                  style={{ marginTop: 14, padding: "8px 18px", fontSize: 13 }}
                >
                  <RefreshCw size={14} className={syncing ? "animate-spin" : ""} style={{ marginRight: 8 }} />
                  {syncing ? "Sweeping Cluster…" : "Run Initial Discovery Sweep"}
                </button>
              </div>
            );
          }

          if (clusterStatus === "connected_empty") {
            return (
              <div className="empty-state" style={{ padding: "60px 0" }}>
                <Database size={32} style={{ color: "var(--text-dim)" }} />
                <div className="empty-state-title">Connected · 0 Resources Discovered</div>
                <div className="empty-state-sub">
                  Cluster <strong>{targetCluster?.name || clusterId}</strong> is online and collection completed, but no resources were found within the monitored namespace scope.
                </div>
                <button
                  className="graph-ctrl-btn"
                  onClick={handleSync}
                  disabled={syncing}
                  style={{ marginTop: 12, padding: "6px 14px" }}
                >
                  <RefreshCw size={12} className={syncing ? "animate-spin" : ""} style={{ marginRight: 6 }} />
                  Re-sweep Cluster
                </button>
              </div>
            );
          }

          return (
            <div className="empty-state" style={{ padding: "60px 0" }}>
              <Search size={32} style={{ color: "var(--text-dim)" }} />
              <div className="empty-state-title">No Matching Resources</div>
              <div className="empty-state-sub">
                {clusterId && clusterId !== "all"
                  ? `No resources match the active filters in cluster "${clusterId}". Try resetting search or namespace filters.`
                  : "No resources match the active filters across discovered clusters."}
              </div>
            </div>
          );
        })()
      ) : (
        <div style={{ background: "var(--bg-panel)", border: "1px solid var(--border)", overflowX: "auto" }}>
          <table className="impact-table">
            <thead>
              <tr>
                <th>Resource Name</th>
                <th>Type</th>
                <th>Namespace</th>
                <th>Cluster</th>
                <th>Last Observed</th>
                <th style={{ textAlign: "right" }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {filteredResources.map((res) => {
                const parsed = parseProviderID(res.provider_id);
                return (
                  <tr key={`${res.provider}/${res.resource_type}/${res.provider_id}`}>
                    <td>
                      <div
                        className="cell-resource"
                        style={{ cursor: "pointer" }}
                        onClick={() => handleOpenDetail(res)}
                        title="Click to view details and active relationships"
                      >
                        <Database size={12} style={{ color: "var(--text-dim)", flexShrink: 0 }} />
                        <span style={{ fontFamily: "var(--font-mono)", fontWeight: 500 }}>
                          {parsed.name}
                        </span>
                      </div>
                    </td>
                    <td className="type-label">{res.resource_type}</td>
                    <td style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--text-secondary)" }}>
                      {parsed.namespace || "—"}
                    </td>
                    <td style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--text-dim)" }}>
                      {parsed.cluster || "default"}
                    </td>
                    <td style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--text-dim)" }}>
                      {res.last_observed_at ? new Date(res.last_observed_at).toLocaleTimeString() : "—"}
                    </td>
                    <td style={{ textAlign: "right" }}>
                      <div style={{ display: "inline-flex", gap: 6 }}>
                        <button
                          className="graph-ctrl-btn"
                          onClick={() => handleOpenDetail(res)}
                          title="View resource connections"
                          style={{ padding: "3px 8px", fontSize: 10 }}
                        >
                          Details
                        </button>
                        <button
                          className="graph-ctrl-btn"
                          onClick={() => onNavigateToInvestigate(res)}
                          title="Analyze impact of proposed change to this resource"
                          style={{
                            padding: "3px 8px",
                            fontSize: 10,
                            borderColor: "var(--text-secondary)",
                            color: "var(--text-primary)",
                          }}
                        >
                          <Play size={10} style={{ marginRight: 3 }} />
                          Analyze
                        </button>
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {/* ── Detail Drawer ── */}
      {(selectedDetail || loadingDetail || detailError) && (
        <div
          style={{
            position: "fixed",
            top: 0,
            right: 0,
            width: "480px",
            height: "100%",
            background: "var(--bg-panel)",
            borderLeft: "1px solid var(--border)",
            zIndex: 100,
            boxShadow: "-8px 0 24px rgba(0,0,0,0.5)",
            display: "flex",
            flexDirection: "column",
          }}
        >
          {/* Drawer Header */}
          <div
            style={{
              padding: "16px 20px",
              borderBottom: "1px solid var(--border)",
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
            }}
          >
            <div>
              <div className="page-section-label" style={{ marginBottom: 2 }}>Resource Details</div>
              <div style={{ fontFamily: "var(--font-mono)", fontSize: 14, fontWeight: 600 }}>
                {selectedDetail ? parseProviderID(selectedDetail.resource.provider_id).name : "Loading..."}
              </div>
            </div>
            <button
              onClick={() => {
                setSelectedDetail(null);
                setDetailError(null);
              }}
              style={{
                background: "transparent",
                border: "none",
                color: "var(--text-dim)",
                cursor: "pointer",
              }}
            >
              <X size={16} />
            </button>
          </div>

          {/* Drawer Content */}
          <div style={{ padding: "20px", flex: 1, overflowY: "auto" }}>
            {loadingDetail && (
              <div className="empty-state">
                <span className="loading-dots">
                  <span className="loading-dot" />
                  <span className="loading-dot" />
                  <span className="loading-dot" />
                </span>
                <p>Loading resource relationship graph...</p>
              </div>
            )}

            {detailError && (
              <div className="error-state">
                <AlertCircle size={14} />
                <div>{detailError}</div>
              </div>
            )}

            {selectedDetail && (
              <>
                <div style={{ marginBottom: 20 }}>
                  <div style={{ fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase", marginBottom: 6 }}>
                    Canonical Identity
                  </div>
                  <div
                    style={{
                      fontFamily: "var(--font-mono)",
                      fontSize: 11,
                      background: "var(--bg-elevated)",
                      padding: "8px 12px",
                      border: "1px solid var(--border)",
                      wordBreak: "break-all",
                    }}
                  >
                    {selectedDetail.resource.provider_id}
                  </div>
                  <div style={{ display: "flex", gap: 16, marginTop: 8, fontSize: 11 }}>
                    <span style={{ color: "var(--text-dim)" }}>Type: <strong>{selectedDetail.resource.resource_type}</strong></span>
                    <span style={{ color: "var(--text-dim)" }}>Provider: <strong>{selectedDetail.resource.provider}</strong></span>
                  </div>
                </div>

                <div style={{ marginBottom: 20 }}>
                  <button
                    className="analyze-btn"
                    onClick={() => {
                      onNavigateToInvestigate(selectedDetail.resource);
                      setSelectedDetail(null);
                    }}
                    style={{ width: "100%", justifyContent: "center", padding: "8px 12px" }}
                  >
                    <Play size={13} style={{ marginRight: 6 }} />
                    Analyze Impact of Changing This Resource
                  </button>
                </div>

                <div>
                  <div style={{ fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase", marginBottom: 8 }}>
                    Active Relationships ({selectedDetail.relationships.length})
                  </div>
                  {selectedDetail.relationships.length === 0 ? (
                    <div className="empty-state" style={{ padding: "20px 0", background: "var(--bg-elevated)" }}>
                      <p style={{ margin: 0, fontSize: 12 }}>No explicit dependencies or dependents discovered.</p>
                    </div>
                  ) : (
                    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                      {selectedDetail.relationships.map((rel, idx) => {
                        const parsedPeer = parseProviderID(rel.peer.provider_id);
                        return (
                          <div
                            key={idx}
                            style={{
                              background: "var(--bg-elevated)",
                              border: "1px solid var(--border)",
                              padding: "10px 12px",
                            }}
                          >
                            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 4 }}>
                              <span
                                className={`risk-badge ${rel.direction === "OUTGOING" ? "medium" : "low"}`}
                                style={{ fontSize: 9 }}
                              >
                                {rel.direction}
                              </span>
                              <span style={{ fontFamily: "var(--font-mono)", fontSize: 10, color: "var(--text-dim)" }}>
                                {rel.kind}
                              </span>
                            </div>
                            <div style={{ fontFamily: "var(--font-mono)", fontSize: 12, fontWeight: 500 }}>
                              {parsedPeer.name}
                            </div>
                            <div style={{ fontSize: 10, color: "var(--text-dim)", marginTop: 2 }}>
                              {rel.peer.resource_type} · {parsedPeer.namespace || "cluster"}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>
              </>
            )}
          </div>
        </div>
      )}
    </div>
  );
};
