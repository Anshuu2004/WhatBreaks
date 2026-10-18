import { useState, useEffect, useCallback } from "react";
import { Sidebar } from "./components/Sidebar";
import type { NavPage } from "./components/Sidebar";
import { TopBar } from "./components/TopBar";
import { InvestigatePage } from "./components/InvestigatePage";
import { ResourcesPage } from "./components/ResourcesPage";
import { DependenciesPage } from "./components/DependenciesPage";
import { SystemHealthPage } from "./components/SystemHealthPage";
import { ErrorBoundary } from "./components/ErrorBoundary";
import { fetchCSRFToken, listClusters, DEFAULT_WORKSPACE_ID } from "./api/client";
import type { ResourceItem, ListClustersResponse } from "./types";
import { parseProviderID } from "./utils/resource";

// Page-level breadcrumb config
const BREADCRUMBS: Record<NavPage, string[]> = {
  investigate:  ["Investigate", "Impact Analysis"],
  resources:    ["Resources", "Inventory"],
  dependencies: ["Dependencies", "Topology"],
  health:       ["System Health", "Telemetry"],
};

export default function App() {
  const [activePage, setActivePage] = useState<NavPage>("investigate");
  const [workspaceId, setWorkspaceId] = useState<string>(DEFAULT_WORKSPACE_ID);
  const [clusterId, setClusterId] = useState<string>(""); // Default: "All clusters"
  const [clustersData, setClustersData] = useState<ListClustersResponse | null>(null);
  const [investigateTarget, setInvestigateTarget] = useState<ResourceItem | null>(null);

  // Bootstrap CSRF token once on mount
  useEffect(() => {
    fetchCSRFToken();
  }, []);

  // Discover available clusters dynamically for the selected workspace
  const refreshClusters = useCallback(async () => {
    try {
      const res = await listClusters(workspaceId);
      setClustersData(res);
    } catch (e) {
      console.warn("Could not discover clusters for workspace", e);
    }
  }, [workspaceId]);

  useEffect(() => {
    refreshClusters();
  }, [refreshClusters]);

  const handleNavigateToInvestigate = (res: ResourceItem) => {
    setInvestigateTarget({ ...res });
    setActivePage("investigate");
  };

  const breadcrumb = BREADCRUMBS[activePage];

  return (
    <div className="shell">
      {/* ── Left sidebar ── */}
      <Sidebar
        activePage={activePage}
        onNavigate={setActivePage}
        workspaceId={workspaceId}
        clusterId={clusterId}
        clustersData={clustersData}
        onWorkspaceChange={(ws) => {
          setWorkspaceId(ws);
          setInvestigateTarget(null);
        }}
        onClusterChange={(cl) => {
          setClusterId(cl);
          if (investigateTarget) {
            const parsed = parseProviderID(investigateTarget.provider_id);
            if (cl && cl !== "all" && parsed.cluster && parsed.cluster !== cl) {
              setInvestigateTarget(null);
            }
          }
        }}
        onRefreshClusters={refreshClusters}
        userName="aaradhy"
      />

      {/* ── Main area ── */}
      <div className="main-area">
        <TopBar breadcrumb={breadcrumb} />

        <main className="page-content">
          <ErrorBoundary fallbackTitle="Page encounter an unexpected rendering error">
            {activePage === "investigate" && (
              <InvestigatePage
                workspaceId={workspaceId}
                clusterId={clusterId}
                initialResource={investigateTarget}
              />
            )}
            {activePage === "resources" && (
              <ResourcesPage
                workspaceId={workspaceId}
                clusterId={clusterId}
                clustersData={clustersData}
                onRefreshClusters={refreshClusters}
                onNavigateToInvestigate={handleNavigateToInvestigate}
              />
            )}
            {activePage === "dependencies" && (
              <DependenciesPage
                workspaceId={workspaceId}
                clusterId={clusterId}
                onNavigateToInvestigate={handleNavigateToInvestigate}
              />
            )}
            {activePage === "health" && (
              <SystemHealthPage workspaceId={workspaceId} />
            )}
          </ErrorBoundary>
        </main>
      </div>
    </div>
  );
}
