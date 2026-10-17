import React, { useState, useEffect, useCallback, useMemo } from "react";
import {
  GitBranch,
  RefreshCw,
  AlertCircle,
  Play,
  Table as TableIcon,
  Network,
  Search,
  Filter,
  Layers,
} from "lucide-react";
import type { DependencyRelationship, ResourceItem } from "../types";
import { listDependencies } from "../api/client";
import { TopologyGraph } from "./TopologyGraph";
import { parseProviderID, isImpactPropagating } from "../utils/resource";

interface DependenciesPageProps {
  workspaceId: string;
  clusterId: string;
  onNavigateToInvestigate: (res: ResourceItem) => void;
}

export const DependenciesPage: React.FC<DependenciesPageProps> = ({
  workspaceId,
  clusterId,
  onNavigateToInvestigate,
}) => {
  const [relationships, setRelationships] = useState<DependencyRelationship[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Filters
  const [search, setSearch] = useState("");
  const [kindFilter, setKindFilter] = useState("all");
  const [namespaceFilter, setNamespaceFilter] = useState("all");

  // Primary view mode: "graph" (default) or "table" fallback
  const [viewMode, setViewMode] = useState<"graph" | "table">("graph");

  const fetchDependencies = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await listDependencies(workspaceId, clusterId || undefined);
      setRelationships(res.relationships);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load dependencies");
      setRelationships([]);
    } finally {
      setLoading(false);
    }
  }, [workspaceId, clusterId]);

  useEffect(() => {
    fetchDependencies();
  }, [fetchDependencies]);

  // Derived namespaces from relationship endpoints
  const availableNamespaces = useMemo(() => {
    const s = new Set<string>();
    relationships.forEach((r) => {
      const src = parseProviderID(r.source.provider_id);
      const tgt = parseProviderID(r.target.provider_id);
      if (src.namespace) s.add(src.namespace);
      if (tgt.namespace) s.add(tgt.namespace);
    });
    return Array.from(s).sort();
  }, [relationships]);

  // Derived relationship kinds
  const availableKinds = useMemo(() => {
    const s = new Set<string>();
    relationships.forEach((r) => s.add(r.kind));
    return Array.from(s).sort();
  }, [relationships]);

  // Filtered relationships
  const filteredRelationships = useMemo(() => {
    return relationships.filter((rel) => {
      // Kind filter
      if (kindFilter === "propagating" && !isImpactPropagating(rel.kind)) {
        return false;
      }
      if (kindFilter === "boundary" && isImpactPropagating(rel.kind)) {
        return false;
      }
      if (
        kindFilter !== "all" &&
        kindFilter !== "propagating" &&
        kindFilter !== "boundary" &&
        rel.kind !== kindFilter
      ) {
        return false;
      }

      // Namespace filter
      const src = parseProviderID(rel.source.provider_id);
      const tgt = parseProviderID(rel.target.provider_id);
      if (namespaceFilter !== "all") {
        if (src.namespace !== namespaceFilter && tgt.namespace !== namespaceFilter) {
          return false;
        }
      }

      // Search query
      if (search.trim()) {
        const q = search.toLowerCase();
        const matches =
          rel.source.provider_id.toLowerCase().includes(q) ||
          rel.target.provider_id.toLowerCase().includes(q) ||
          src.name.toLowerCase().includes(q) ||
          tgt.name.toLowerCase().includes(q) ||
          rel.kind.toLowerCase().includes(q) ||
          rel.category.toLowerCase().includes(q);
        if (!matches) return false;
      }

      return true;
    });
  }, [relationships, kindFilter, namespaceFilter, search]);

  // Aggregate stats across discovered topology
  const stats = useMemo(() => {
    const nodeSet = new Set<string>();
    let propagatingCount = 0;
    let boundaryCount = 0;

    relationships.forEach((r) => {
      nodeSet.add(`${r.source.provider}/${r.source.resource_type}/${r.source.provider_id}`);
      nodeSet.add(`${r.target.provider}/${r.target.resource_type}/${r.target.provider_id}`);
      if (isImpactPropagating(r.kind)) {
        propagatingCount++;
      } else {
        boundaryCount++;
      }
    });

    return {
      nodeCount: nodeSet.size,
      edgeCount: relationships.length,
      propagatingCount,
      boundaryCount,
    };
  }, [relationships]);

  return (
    <div
      style={{
        padding: "20px 28px",
        height: "100%",
        display: "flex",
        flexDirection: "column",
        overflowY: "auto",
        boxSizing: "border-box",
      }}
    >
      {/* ── Page Header ── */}
      <div
        style={{
          display: "flex",
          alignItems: "flex-start",
          justifyContent: "space-between",
          marginBottom: 16,
          flexShrink: 0,
        }}
      >
        <div>
          <div className="page-section-label">Topology</div>
          <h1 className="page-heading" style={{ margin: "2px 0 4px 0" }}>
            System Dependencies
          </h1>
          <p className="page-subtitle" style={{ margin: 0 }}>
            {clusterId && clusterId !== "all"
              ? `Interactive directed topology graph scoped to cluster: ${clusterId}`
              : "Interactive directed topology graph across all discovered clusters in workspace."}
          </p>
        </div>
        <div style={{ display: "flex", gap: 10 }}>
          <button
            className="graph-ctrl-btn"
            onClick={fetchDependencies}
            disabled={loading}
            style={{ padding: "6px 12px" }}
            title="Refresh relationships from state store"
          >
            <RefreshCw
              size={12}
              className={loading ? "animate-spin" : ""}
              style={{ marginRight: 6 }}
            />
            Refresh
          </button>
        </div>
      </div>

      {error && (
        <div className="error-state" style={{ marginBottom: 14, flexShrink: 0 }}>
          <AlertCircle size={16} />
          <div>{error}</div>
        </div>
      )}

      {/* ── Stats Summary Bar ── */}
      <div className="summary-grid" style={{ marginBottom: 16, flexShrink: 0 }}>
        <div className="summary-stat">
          <GitBranch size={18} className="summary-stat-icon" />
          <div className="summary-stat-body">
            <div className="summary-stat-value">{stats.edgeCount}</div>
            <div className="summary-stat-label">Active Relationships</div>
          </div>
        </div>
        <div className="summary-stat">
          <Network size={18} className="summary-stat-icon" />
          <div className="summary-stat-body">
            <div className="summary-stat-value">{stats.nodeCount}</div>
            <div className="summary-stat-label">Connected Resources</div>
          </div>
        </div>
        <div className="summary-stat">
          <div className="summary-stat-body">
            <div className="summary-stat-value" style={{ color: "var(--status-ok)" }}>
              {stats.propagatingCount}
            </div>
            <div className="summary-stat-label">Impact-Propagating (DEPENDS_ON)</div>
          </div>
        </div>
        <div className="summary-stat">
          <div className="summary-stat-body">
            <div className="summary-stat-value" style={{ color: "var(--text-secondary)" }}>
              {stats.boundaryCount}
            </div>
            <div className="summary-stat-label">Lifecycle Boundaries (OWNS)</div>
          </div>
        </div>
      </div>

      {/* ── Filters & Controls Bar ── */}
      <div
        style={{
          display: "flex",
          gap: 12,
          marginBottom: 14,
          background: "var(--bg-panel)",
          border: "1px solid var(--border)",
          padding: "8px 12px",
          alignItems: "center",
          flexWrap: "wrap",
          flexShrink: 0,
        }}
      >
        {/* Search */}
        <div style={{ position: "relative", flex: "1 1 240px" }}>
          <Search
            size={13}
            style={{ position: "absolute", left: 10, top: 10, color: "var(--text-dim)" }}
          />
          <input
            type="text"
            className="resource-search-input"
            placeholder="Search connections by resource or relationship..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            style={{ paddingLeft: 30 }}
          />
        </div>

        {/* Relationship Kind Filter */}
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          <Filter size={12} style={{ color: "var(--text-dim)" }} />
          <span style={{ fontSize: 11, color: "var(--text-dim)" }}>Kind:</span>
          <select
            className="sidebar-select"
            value={kindFilter}
            onChange={(e) => setKindFilter(e.target.value)}
            style={{ width: "auto", minWidth: 150 }}
          >
            <option value="all">All Relationships ({relationships.length})</option>
            <option value="propagating">Propagating Only ({stats.propagatingCount})</option>
            <option value="boundary">Boundaries Only ({stats.boundaryCount})</option>
            {availableKinds.map((k) => (
              <option key={k} value={k}>
                {k}
              </option>
            ))}
          </select>
        </div>

        {/* Namespace Filter */}
        {availableNamespaces.length > 0 && (
          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <Layers size={12} style={{ color: "var(--text-dim)" }} />
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

        {/* View Mode Toggle: Graph vs Table Fallback */}
        <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 6 }}>
          <button
            className="graph-ctrl-btn"
            onClick={() => setViewMode("graph")}
            aria-pressed={viewMode === "graph"}
            style={{
              padding: "5px 10px",
              background: viewMode === "graph" ? "var(--bg-elevated)" : "transparent",
              color: viewMode === "graph" ? "var(--text-primary)" : "var(--text-dim)",
              borderColor: viewMode === "graph" ? "var(--border-strong)" : "var(--border)",
            }}
            title="Switch to Interactive Directed Graph View"
          >
            <Network size={12} style={{ marginRight: 4 }} />
            Graph View
          </button>
          <button
            className="graph-ctrl-btn"
            onClick={() => setViewMode("table")}
            aria-pressed={viewMode === "table"}
            style={{
              padding: "5px 10px",
              background: viewMode === "table" ? "var(--bg-elevated)" : "transparent",
              color: viewMode === "table" ? "var(--text-primary)" : "var(--text-dim)",
              borderColor: viewMode === "table" ? "var(--border-strong)" : "var(--border)",
            }}
            title="Switch to Structured Relationship Table Fallback"
          >
            <TableIcon size={12} style={{ marginRight: 4 }} />
            Table Fallback
          </button>
        </div>
      </div>

      {/* ── Main View Content Area ── */}
      <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column" }}>
        {loading ? (
          <div className="empty-state" style={{ padding: "80px 0" }}>
            <span className="loading-dots">
              <span className="loading-dot" />
              <span className="loading-dot" />
              <span className="loading-dot" />
            </span>
            <div className="empty-state-title">
              Loading topology relationships from state store…
            </div>
          </div>
        ) : filteredRelationships.length === 0 ? (
          <div className="empty-state" style={{ padding: "80px 0" }}>
            <GitBranch size={32} style={{ color: "var(--text-dim)" }} />
            <div className="empty-state-title">No Active Dependencies Discovered</div>
            <div className="empty-state-sub">
              {clusterId && clusterId !== "all"
                ? `No relationships found matching filters for cluster "${clusterId}". Resources may have no dependencies or a discovery sweep is pending.`
                : "No relationships found matching the active filters in this workspace. Try adjusting your search or trigger a discovery sweep."}
            </div>
          </div>
        ) : viewMode === "graph" ? (
          /* Graph-First Topology View */
          <div style={{ flex: 1, minHeight: 0, position: "relative" }}>
            <TopologyGraph
              relationships={filteredRelationships}
              onNavigateToInvestigate={onNavigateToInvestigate}
            />
          </div>
        ) : (
          /* Structured Table Fallback */
          <div
            style={{
              background: "var(--bg-panel)",
              border: "1px solid var(--border)",
              overflowX: "auto",
              flex: 1,
            }}
          >
            <table className="impact-table" style={{ width: "100%" }}>
              <thead>
                <tr>
                  <th>Source Resource</th>
                  <th>Relationship Kind</th>
                  <th>Target Resource</th>
                  <th>Category</th>
                  <th>Failure Propagation</th>
                  <th style={{ textAlign: "right" }}>Actions</th>
                </tr>
              </thead>
              <tbody>
                {filteredRelationships.map((rel, idx) => {
                  const src = parseProviderID(rel.source.provider_id);
                  const tgt = parseProviderID(rel.target.provider_id);
                  const propagates = isImpactPropagating(rel.kind);

                  return (
                    <tr key={idx}>
                      <td>
                        <div>
                          <span style={{ fontFamily: "var(--font-mono)", fontWeight: 500 }}>
                            {src.name}
                          </span>
                          <div style={{ fontSize: 10, color: "var(--text-dim)" }}>
                            {rel.source.resource_type} · {src.namespace || "cluster"} (
                            {src.cluster || "default"})
                          </div>
                        </div>
                      </td>
                      <td>
                        <span
                          style={{
                            fontFamily: "var(--font-mono)",
                            fontSize: 9,
                            padding: "2px 6px",
                            background: propagates
                              ? "var(--status-ok-bg)"
                              : "rgba(255,255,255,0.06)",
                            color: propagates ? "var(--status-ok)" : "var(--text-dim)",
                            border: `1px solid ${
                              propagates ? "var(--status-ok)" : "var(--border)"
                            }`,
                          }}
                        >
                          {rel.kind}
                        </span>
                      </td>
                      <td>
                        <div>
                          <span style={{ fontFamily: "var(--font-mono)", fontWeight: 500 }}>
                            {tgt.name}
                          </span>
                          <div style={{ fontSize: 10, color: "var(--text-dim)" }}>
                            {rel.target.resource_type} · {tgt.namespace || "cluster"} (
                            {tgt.cluster || "default"})
                          </div>
                        </div>
                      </td>
                      <td style={{ fontSize: 11, color: "var(--text-secondary)" }}>
                        {rel.category}
                      </td>
                      <td>
                        <span
                          style={{
                            fontSize: 10,
                            color: propagates ? "var(--status-ok)" : "var(--text-dim)",
                            fontWeight: propagates ? 600 : 400,
                          }}
                        >
                          {propagates
                            ? "Propagates Upstream"
                            : "Lifecycle Boundary Only"}
                        </span>
                      </td>
                      <td style={{ textAlign: "right" }}>
                        <div style={{ display: "inline-flex", gap: 6 }}>
                          <button
                            className="graph-ctrl-btn"
                            onClick={() =>
                              onNavigateToInvestigate({
                                provider: rel.target.provider,
                                resource_type: rel.target.resource_type,
                                provider_id: rel.target.provider_id,
                                first_observed_at: rel.first_observed_at,
                                last_observed_at: rel.last_observed_at,
                              })
                            }
                            title="Analyze impact if target resource is modified"
                            style={{ padding: "3px 8px", fontSize: 10 }}
                          >
                            <Play size={10} style={{ marginRight: 3 }} />
                            Analyze Target
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
      </div>
    </div>
  );
};
