import React from "react";
import {
  Search,
  Database,
  GitBranch,
  Activity,
  ChevronRight,
} from "lucide-react";
import { NavIcon } from "./NavIcon";

export type NavPage = "investigate" | "resources" | "dependencies" | "health";

interface SidebarProps {
  activePage: NavPage;
  onNavigate: (page: NavPage) => void;
  workspaceId: string;
  clusterId: string;
  availableClusters?: string[];
  clustersData?: import("../types").ListClustersResponse | null;
  onWorkspaceChange: (ws: string) => void;
  onClusterChange: (cl: string) => void;
  onRefreshClusters?: () => void;
  userName: string;
}

const NAV_ITEMS: { id: NavPage; label: string; Icon: React.FC<{ size?: number }> }[] = [
  { id: "investigate",  label: "Investigate",     Icon: Search     },
  { id: "resources",   label: "Resources",        Icon: Database   },
  { id: "dependencies",label: "Dependencies",     Icon: GitBranch  },
  { id: "health",      label: "System Health",    Icon: Activity   },
];

// Known workspaces — populated from env or hardcoded fallback
const KNOWN_WORKSPACES = [
  { id: "00000000-0000-0000-0000-000000000001", label: "00000000-0000-0000-0000-000000000001 (default)" },
  { id: "production",  label: "production"  },
  { id: "staging",     label: "staging"     },
  { id: "development", label: "development" },
];

export const Sidebar: React.FC<SidebarProps> = ({
  activePage,
  onNavigate,
  workspaceId,
  clusterId,
  availableClusters,
  clustersData,
  onWorkspaceChange,
  onClusterChange,
  onRefreshClusters,
  userName,
}) => {
  const initials = userName ? userName[0].toUpperCase() : "?";
  const roleLabel = "admin";

  // Build cluster list prioritizing real cluster data from backend
  const clusters = clustersData?.clusters || [];
  const activeCluster = clusters.find((c) => c.active) || clusters[0];
  const selectedCluster = clusters.find((c) => c.name === clusterId);
  const currentCluster = selectedCluster || (clusterId ? null : activeCluster);

  return (
    <nav className="sidebar" aria-label="Application navigation">
      {/* Logo */}
      <div className="sidebar-logo">
        <div className="sidebar-logo-mark" aria-hidden="true">
          <NavIcon size={18} />
        </div>
        <div className="sidebar-logo-text">
          <span className="sidebar-logo-name">WhatBreaks</span>
          <span className="sidebar-logo-sub">Infrastructure Impact Analysis</span>
        </div>
      </div>

      {/* Navigation */}
      <div className="nav" role="list">
        {NAV_ITEMS.map(({ id, label, Icon }) => (
          <button
            key={id}
            role="listitem"
            className={`nav-item ${activePage === id ? "active" : ""}`}
            onClick={() => onNavigate(id)}
            aria-current={activePage === id ? "page" : undefined}
          >
            {activePage === id ? (
              <ChevronRight size={12} />
            ) : (
              <Icon size={14} />
            )}
            {label}
          </button>
        ))}
      </div>

      {/* Workspace + Cluster selectors */}
      <div className="sidebar-bottom">
        <div className="sidebar-selector-group">
          <label className="sidebar-selector-label" htmlFor="workspace-select">
            Workspace
          </label>
          <select
            id="workspace-select"
            className="sidebar-select"
            value={workspaceId}
            onChange={(e) => onWorkspaceChange(e.target.value)}
          >
            {KNOWN_WORKSPACES.map((ws) => (
              <option key={ws.id} value={ws.id}>
                {ws.label}
              </option>
            ))}
            {!KNOWN_WORKSPACES.find((w) => w.id === workspaceId) && (
              <option value={workspaceId}>{workspaceId}</option>
            )}
          </select>
        </div>

        <div className="sidebar-selector-group">
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <label className="sidebar-selector-label" htmlFor="cluster-select" style={{ marginBottom: 0 }}>
              Cluster (Scope)
            </label>
            {onRefreshClusters && (
              <button
                type="button"
                onClick={onRefreshClusters}
                style={{
                  background: "transparent",
                  border: "none",
                  color: "var(--text-dim)",
                  cursor: "pointer",
                  fontSize: 10,
                  padding: "0 2px",
                }}
                title="Refresh cluster status"
              >
                ↻
              </button>
            )}
          </div>
          <select
            id="cluster-select"
            className="sidebar-select"
            value={clusterId}
            onChange={(e) => onClusterChange(e.target.value)}
          >
            <option value="">All clusters</option>
            {clusters.length > 0 ? (
              clusters.map((cl) => {
                let suffix = "";
                if (cl.status === "unreachable") suffix = " (offline)";
                else if (cl.status === "pending_sync") suffix = " (pending sync)";
                else if (cl.status === "connected_empty") suffix = " (0 resources)";
                else if (cl.status === "connected_with_resources") suffix = ` (${cl.resource_count})`;
                return (
                  <option key={cl.name} value={cl.name}>
                    {cl.name}{suffix}
                  </option>
                );
              })
            ) : availableClusters && availableClusters.length > 0 ? (
              availableClusters.map((cl) => (
                <option key={cl} value={cl}>
                  {cl}
                </option>
              ))
            ) : null}
            {clusterId &&
              !clusters.some((c) => c.name === clusterId) &&
              (!availableClusters || !availableClusters.includes(clusterId)) && (
                <option value={clusterId}>{clusterId}</option>
              )}
          </select>

          {/* Real Status Indicator Badge */}
          <div style={{ marginTop: 6, fontSize: 11, display: "flex", alignItems: "center", gap: 6 }}>
            {!clustersData || !clustersData.configured || clustersData.status === "not_configured" ? (
              <span style={{ color: "var(--text-dim)", display: "flex", alignItems: "center", gap: 5 }}>
                <span style={{ width: 6, height: 6, borderRadius: "50%", background: "var(--text-dim)", display: "inline-block" }} />
                No cluster configured
              </span>
            ) : currentCluster?.status === "unreachable" ? (
              <span style={{ color: "var(--status-error)", display: "flex", alignItems: "center", gap: 5 }} title={currentCluster.error || "Cluster unreachable"}>
                <span style={{ width: 6, height: 6, borderRadius: "50%", background: "var(--status-error)", display: "inline-block" }} />
                Unreachable
              </span>
            ) : currentCluster?.status === "pending_sync" ? (
              <span style={{ color: "var(--status-warn)", display: "flex", alignItems: "center", gap: 5 }}>
                <span style={{ width: 6, height: 6, borderRadius: "50%", background: "var(--status-warn)", display: "inline-block" }} />
                Pending Sync
              </span>
            ) : currentCluster?.status === "connected_empty" ? (
              <span style={{ color: "var(--text-dim)", display: "flex", alignItems: "center", gap: 5 }}>
                <span style={{ width: 6, height: 6, borderRadius: "50%", background: "var(--status-ok)", display: "inline-block" }} />
                Connected (0 resources)
              </span>
            ) : (
              <span style={{ color: "var(--status-ok)", display: "flex", alignItems: "center", gap: 5 }}>
                <span style={{ width: 6, height: 6, borderRadius: "50%", background: "var(--status-ok)", display: "inline-block" }} />
                Connected ({currentCluster?.resource_count ?? 0} resources)
              </span>
            )}
          </div>
        </div>
      </div>

      {/* User profile */}
      <div className="sidebar-user" role="button" tabIndex={0} aria-label="User profile">
        <div className="user-avatar">{initials}</div>
        <div className="user-info">
          <span className="user-name">{userName || "unknown"}</span>
          <span className="user-role">{roleLabel}</span>
        </div>
        <ChevronRight size={12} style={{ color: "var(--text-dim)", flexShrink: 0 }} />
      </div>
    </nav>
  );
};
