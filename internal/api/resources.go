package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/aaradhychinche-alt/WhatBreaks/internal/auth"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/state"
)

// ResourceItem models a discovered infrastructure resource for API responses.
type ResourceItem struct {
	Provider        string    `json:"provider"`
	ResourceType    string    `json:"resource_type"`
	ProviderID      string    `json:"provider_id"`
	FirstObservedAt time.Time `json:"first_observed_at"`
	LastObservedAt  time.Time `json:"last_observed_at"`
}

// ListResourcesResponse represents the payload returned by GET /api/v1/resources.
type ListResourcesResponse struct {
	WorkspaceID string         `json:"workspace_id"`
	Total       int            `json:"total"`
	Resources   []ResourceItem `json:"resources"`
	Clusters    []string       `json:"clusters,omitempty"`
}

// DependencyRelationship models an active relationship in the workspace dependency graph.
type DependencyRelationship struct {
	Source          state.ResourceIdentity `json:"source"`
	Target          state.ResourceIdentity `json:"target"`
	Kind            string                 `json:"kind"`
	Category        string                 `json:"category"`
	FirstObservedAt time.Time              `json:"first_observed_at"`
	LastObservedAt  time.Time              `json:"last_observed_at"`
}

// ListDependenciesResponse represents the payload returned by GET /api/v1/dependencies.
type ListDependenciesResponse struct {
	WorkspaceID   string                   `json:"workspace_id"`
	Total         int                      `json:"total"`
	Relationships []DependencyRelationship `json:"relationships"`
}

// ComponentHealth represents the health status and telemetry of an individual system component.
type ComponentHealth struct {
	Status  string         `json:"status"` // "healthy", "degraded", "unavailable", "not_configured"
	Message string         `json:"message,omitempty"`
	Details map[string]any `json:"details,omitempty"`
}

// SystemHealthResponse represents the payload returned by GET /api/v1/system/health.
type SystemHealthResponse struct {
	Status      string                     `json:"status"` // "healthy", "degraded", "unavailable"
	Timestamp   string                     `json:"timestamp"`
	Uptime      float64                    `json:"uptime"`
	Environment string                     `json:"environment"`
	Components  map[string]ComponentHealth `json:"components"`
}

// ResourceRelationshipItem represents an active relationship connected to an inspected resource.
type ResourceRelationshipItem struct {
	Source          state.ResourceIdentity `json:"source"`
	Target          state.ResourceIdentity `json:"target"`
	Kind            string                 `json:"kind"`
	Category        string                 `json:"category"`
	Direction       string                 `json:"direction"` // "OUTGOING" or "INCOMING"
	Peer            state.ResourceIdentity `json:"peer"`
	EvidenceIDs     []string               `json:"evidence_ids,omitempty"`
	FirstObservedAt time.Time              `json:"first_observed_at"`
	LastObservedAt  time.Time              `json:"last_observed_at"`
}

// ResourceDetailResponse represents the payload returned by GET /api/v1/resources/detail.
type ResourceDetailResponse struct {
	WorkspaceID   string                     `json:"workspace_id"`
	Resource      ResourceItem               `json:"resource"`
	Relationships []ResourceRelationshipItem `json:"relationships"`
}

// extractAndAuthorizeWorkspace validates and authorizes the target workspace ID.
// If an authorizer is provided, it delegates to AuthorizeWorkspaceAccess.
// If a configuredWS is set (e.g. single-tenant / local development), it ensures the caller
// cannot access arbitrary workspace state.
func extractAndAuthorizeWorkspace(
	r *http.Request,
	authorizer *auth.WorkspaceAuthorizer,
	configuredWS string,
) (string, error) {
	wsID := r.Header.Get("X-Workspace-ID")
	if wsID == "" {
		wsID = r.URL.Query().Get("workspace_id")
	}
	wsID = strings.TrimSpace(wsID)

	if wsID == "" {
		if configuredWS != "" {
			wsID = configuredWS
		} else {
			return "", auth.ErrInvalidWorkspaceID
		}
	}

	if !auth.IsValidUUID(wsID) {
		return "", auth.ErrInvalidWorkspaceID
	}

	if authorizer != nil {
		if err := authorizer.AuthorizeWorkspaceAccess(r.Context(), wsID); err != nil {
			return "", err
		}
	} else if configuredWS != "" {
		if wsID != configuredWS {
			// Prevent arbitrary workspace enumeration / access in configured-cluster mode
			return "", auth.ErrWorkspaceForbidden
		}
	} else {
		// Neither concrete authorizer nor configured single-tenant workspace is present.
		// UUID validation alone does not prove authorization.
		return "", auth.ErrWorkspaceForbidden
	}

	return wsID, nil
}

func handleWorkspaceAuthError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, auth.ErrNotAuthenticated):
		writeErrorResponse(w, http.StatusUnauthorized, "Not authenticated", "UNAUTHORIZED")
	case errors.Is(err, auth.ErrWorkspaceNotFound):
		writeErrorResponse(w, http.StatusNotFound, "Workspace not found", "NOT_FOUND")
	case errors.Is(err, auth.ErrInvalidWorkspaceID):
		writeErrorResponse(w, http.StatusBadRequest, "Invalid or missing workspace identifier", "INVALID_WORKSPACE")
	default:
		writeErrorResponse(w, http.StatusForbidden, "Access to workspace forbidden", "FORBIDDEN")
	}
}

// handleListResources handles GET /api/v1/resources.
func (s *Server) handleListResources() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed", "METHOD_NOT_ALLOWED")
			return
		}

		if s.cfg.Store == nil {
			writeErrorResponse(w, http.StatusServiceUnavailable, "State persistence store is not configured", "STORE_UNAVAILABLE")
			return
		}

		wsID, err := extractAndAuthorizeWorkspace(r, s.cfg.WorkspaceAuthorizer, s.cfg.ConfiguredWorkspaceID)
		if err != nil {
			handleWorkspaceAuthError(w, err)
			return
		}

		if checker, ok := s.cfg.Store.(state.WorkspaceIntegrityChecker); ok {
			if err := checker.CheckWorkspaceIntegrity(r.Context(), wsID); err != nil {
				s.logger.Warn("Refusing resource listing: workspace contains un-migrated legacy state", "workspace_id", wsID, "error", err)
				writeErrorResponse(w, http.StatusUnprocessableEntity, err.Error(), "IDENTITY_INTEGRITY_REJECTED")
				return
			}
		}

		resources, err := s.cfg.Store.ListResources(r.Context(), wsID)
		if err != nil {
			s.logger.Error("Failed to list resources from store", "workspace_id", wsID, "error", err)
			writeErrorResponse(w, http.StatusInternalServerError, "Failed to retrieve resource inventory", "INTERNAL_ERROR")
			return
		}

		typeFilter := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("type")))
		if typeFilter == "" {
			typeFilter = strings.ToLower(strings.TrimSpace(r.URL.Query().Get("resource_type")))
		}
		searchFilter := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("search")))
		clusterFilter := strings.TrimSpace(r.URL.Query().Get("cluster"))
		if clusterFilter == "all" {
			clusterFilter = ""
		}

		// Extract unique clusters across all discovered resources in this workspace
		clustersMap := make(map[string]struct{})
		for _, res := range resources {
			if strings.EqualFold(res.Identity.Provider, "kubernetes") || strings.EqualFold(res.Identity.Provider, "k8s") {
				if class, err := state.ClassifyKubernetesProviderID(res.Identity.ResourceType, res.Identity.ProviderID); err == nil && class.Cluster != "" {
					clustersMap[class.Cluster] = struct{}{}
				}
			}
		}
		clusters := make([]string, 0, len(clustersMap))
		for c := range clustersMap {
			clusters = append(clusters, c)
		}
		sort.Strings(clusters)

		items := make([]ResourceItem, 0, len(resources))
		for _, res := range resources {
			if typeFilter != "" && strings.ToLower(res.Identity.ResourceType) != typeFilter {
				continue
			}
			if searchFilter != "" && !strings.Contains(strings.ToLower(res.Identity.ProviderID), searchFilter) {
				continue
			}
			if clusterFilter != "" {
				if strings.EqualFold(res.Identity.Provider, "kubernetes") || strings.EqualFold(res.Identity.Provider, "k8s") {
					class, err := state.ClassifyKubernetesProviderID(res.Identity.ResourceType, res.Identity.ProviderID)
					if err == nil && class.Cluster != "" {
						if class.Cluster != clusterFilter {
							continue
						}
					} else if !strings.HasPrefix(res.Identity.ProviderID, clusterFilter+"/") {
						continue
					}
				}
			}
			items = append(items, ResourceItem{
				Provider:        res.Identity.Provider,
				ResourceType:    res.Identity.ResourceType,
				ProviderID:      res.Identity.ProviderID,
				FirstObservedAt: res.FirstObservedAt,
				LastObservedAt:  res.LastObservedAt,
			})
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ListResourcesResponse{
			WorkspaceID: wsID,
			Total:       len(items),
			Resources:   items,
			Clusters:    clusters,
		})
	}
}

// handleResourceDetail handles GET /api/v1/resources/detail.
func (s *Server) handleResourceDetail() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed", "METHOD_NOT_ALLOWED")
			return
		}

		if s.cfg.Store == nil {
			writeErrorResponse(w, http.StatusServiceUnavailable, "State persistence store is not configured", "STORE_UNAVAILABLE")
			return
		}

		wsID, err := extractAndAuthorizeWorkspace(r, s.cfg.WorkspaceAuthorizer, s.cfg.ConfiguredWorkspaceID)
		if err != nil {
			handleWorkspaceAuthError(w, err)
			return
		}

		if checker, ok := s.cfg.Store.(state.WorkspaceIntegrityChecker); ok {
			if err := checker.CheckWorkspaceIntegrity(r.Context(), wsID); err != nil {
				s.logger.Warn("Refusing resource detail: workspace contains un-migrated legacy state", "workspace_id", wsID, "error", err)
				writeErrorResponse(w, http.StatusUnprocessableEntity, err.Error(), "IDENTITY_INTEGRITY_REJECTED")
				return
			}
		}

		provider := strings.TrimSpace(r.URL.Query().Get("provider"))
		resourceType := strings.TrimSpace(r.URL.Query().Get("resource_type"))
		providerID := strings.TrimSpace(r.URL.Query().Get("provider_id"))

		if provider == "" || resourceType == "" || providerID == "" {
			writeErrorResponse(w, http.StatusBadRequest, "Query parameters 'provider', 'resource_type', and 'provider_id' are required", "VALIDATION_ERROR")
			return
		}

		prov := strings.ToLower(provider)
		if prov == "kubernetes" || prov == "k8s" {
			class, err := state.ClassifyKubernetesProviderID(resourceType, providerID)
			if err != nil || class.IsLegacy || class.IsAmbiguous {
				writeErrorResponse(w, http.StatusUnprocessableEntity, "Resource identity is legacy or ambiguous", "IDENTITY_INTEGRITY_REJECTED")
				return
			}
		}

		identity := state.ResourceIdentity{
			Provider:     provider,
			ResourceType: resourceType,
			ProviderID:   providerID,
		}

		res, err := s.cfg.Store.GetResource(r.Context(), wsID, identity)
		if err != nil {
			if errors.Is(err, state.ErrResourceNotFound) {
				writeErrorResponse(w, http.StatusNotFound, "Resource not found in workspace", "RESOURCE_NOT_FOUND")
				return
			}
			s.logger.Error("Failed to get resource from store", "workspace_id", wsID, "identity", identity, "error", err)
			writeErrorResponse(w, http.StatusInternalServerError, "Failed to retrieve resource details", "INTERNAL_ERROR")
			return
		}

		// Retrieve all relationships for workspace and filter connected edges
		allRels, err := s.cfg.Store.ListRelationships(r.Context(), wsID)
		if err != nil {
			s.logger.Error("Failed to list relationships from store", "workspace_id", wsID, "error", err)
			writeErrorResponse(w, http.StatusInternalServerError, "Failed to retrieve resource relationships", "INTERNAL_ERROR")
			return
		}

		relItems := make([]ResourceRelationshipItem, 0)
		for _, rel := range allRels {
			var direction string
			var peer state.ResourceIdentity

			if rel.Source == identity {
				direction = "OUTGOING"
				peer = rel.Target
			} else if rel.Target == identity {
				direction = "INCOMING"
				peer = rel.Source
			} else {
				continue
			}

			evidenceIDs, err := s.cfg.Store.GetProvenanceForRelationship(r.Context(), wsID, rel.Key())
			if err != nil {
				s.logger.Warn("Failed to resolve provenance for relationship", "rel", rel.Key(), "error", err)
			}

			relItems = append(relItems, ResourceRelationshipItem{
				Source:          rel.Source,
				Target:          rel.Target,
				Kind:            rel.Kind,
				Category:        rel.Category,
				Direction:       direction,
				Peer:            peer,
				EvidenceIDs:     evidenceIDs,
				FirstObservedAt: rel.FirstObservedAt,
				LastObservedAt:  rel.LastObservedAt,
			})
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ResourceDetailResponse{
			WorkspaceID: wsID,
			Resource: ResourceItem{
				Provider:        res.Identity.Provider,
				ResourceType:    res.Identity.ResourceType,
				ProviderID:      res.Identity.ProviderID,
				FirstObservedAt: res.FirstObservedAt,
				LastObservedAt:  res.LastObservedAt,
			},
			Relationships: relItems,
		})
	}
}

// handleListDependencies handles GET /api/v1/dependencies.
func (s *Server) handleListDependencies() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed", "METHOD_NOT_ALLOWED")
			return
		}

		if s.cfg.Store == nil {
			writeErrorResponse(w, http.StatusServiceUnavailable, "State persistence store is not configured", "STORE_UNAVAILABLE")
			return
		}

		wsID, err := extractAndAuthorizeWorkspace(r, s.cfg.WorkspaceAuthorizer, s.cfg.ConfiguredWorkspaceID)
		if err != nil {
			handleWorkspaceAuthError(w, err)
			return
		}

		if checker, ok := s.cfg.Store.(state.WorkspaceIntegrityChecker); ok {
			if err := checker.CheckWorkspaceIntegrity(r.Context(), wsID); err != nil {
				s.logger.Warn("Refusing dependency listing: workspace contains un-migrated legacy state", "workspace_id", wsID, "error", err)
				writeErrorResponse(w, http.StatusUnprocessableEntity, err.Error(), "IDENTITY_INTEGRITY_REJECTED")
				return
			}
		}

		allRels, err := s.cfg.Store.ListRelationships(r.Context(), wsID)
		if err != nil {
			s.logger.Error("Failed to list relationships from store", "workspace_id", wsID, "error", err)
			writeErrorResponse(w, http.StatusInternalServerError, "Failed to retrieve relationships", "INTERNAL_ERROR")
			return
		}

		clusterFilter := strings.TrimSpace(r.URL.Query().Get("cluster"))
		if clusterFilter == "all" {
			clusterFilter = ""
		}

		matchesCluster := func(id state.ResourceIdentity) bool {
			if clusterFilter == "" {
				return true
			}
			if strings.EqualFold(id.Provider, "kubernetes") || strings.EqualFold(id.Provider, "k8s") {
				class, err := state.ClassifyKubernetesProviderID(id.ResourceType, id.ProviderID)
				if err == nil && class.Cluster != "" {
					return class.Cluster == clusterFilter
				}
				return strings.HasPrefix(id.ProviderID, clusterFilter+"/")
			}
			return true
		}

		items := make([]DependencyRelationship, 0, len(allRels))
		for _, rel := range allRels {
			// If cluster filter is specified, include relationship if either source or target belongs to the cluster
			if clusterFilter != "" && !matchesCluster(rel.Source) && !matchesCluster(rel.Target) {
				continue
			}
			items = append(items, DependencyRelationship{
				Source:          rel.Source,
				Target:          rel.Target,
				Kind:            rel.Kind,
				Category:        rel.Category,
				FirstObservedAt: rel.FirstObservedAt,
				LastObservedAt:  rel.LastObservedAt,
			})
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ListDependenciesResponse{
			WorkspaceID:   wsID,
			Total:         len(items),
			Relationships: items,
		})
	}
}

// handleSystemHealth handles GET /api/v1/system/health.
func (s *Server) handleSystemHealth() http.HandlerFunc {
	apiStartTime := time.Now()
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed", "METHOD_NOT_ALLOWED")
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		components := make(map[string]ComponentHealth)
		overallStatus := "healthy"

		// 1. API
		components["api"] = ComponentHealth{
			Status: "healthy",
			Details: map[string]any{
				"version": "0.1.0-dev",
				"port":    s.cfg.Port,
			},
		}

		// 2. Database
		if s.cfg.HealthPinger != nil {
			dbDetails := map[string]any{"connected": false}
			if s.cfg.DatabaseInfo != nil {
				for k, v := range s.cfg.DatabaseInfo {
					dbDetails[k] = v
				}
			}
			if err := s.cfg.HealthPinger.Ping(ctx); err != nil {
				overallStatus = "degraded"
				components["database"] = ComponentHealth{
					Status:  "unavailable",
					Message: err.Error(),
					Details: dbDetails,
				}
			} else {
				dbDetails["connected"] = true
				components["database"] = ComponentHealth{
					Status:  "healthy",
					Details: dbDetails,
				}
			}
		} else {
			components["database"] = ComponentHealth{
				Status:  "not_configured",
				Message: "Database health pinger not configured",
				Details: map[string]any{"connected": false},
			}
		}

		// 3. Core Engine
		if s.cfg.CoreChecker != nil {
			if err := s.cfg.CoreChecker(ctx); err != nil {
				if overallStatus == "healthy" {
					overallStatus = "degraded"
				}
				components["core_engine"] = ComponentHealth{
					Status:  "unavailable",
					Message: err.Error(),
					Details: map[string]any{"connected": false, "transport": "grpc (127.0.0.1:50051)"},
				}
			} else {
				components["core_engine"] = ComponentHealth{
					Status:  "healthy",
					Details: map[string]any{"connected": true, "transport": "grpc (127.0.0.1:50051)"},
				}
			}
		} else if s.cfg.ImpactHandler != nil {
			components["core_engine"] = ComponentHealth{
				Status:  "healthy",
				Details: map[string]any{"connected": true, "transport": "grpc"},
			}
		} else {
			components["core_engine"] = ComponentHealth{
				Status:  "not_configured",
				Message: "Core Engine gRPC client not configured",
				Details: map[string]any{"connected": false},
			}
		}

		// 4. Kubernetes Collector
		if s.cfg.K8sCollector != nil {
			alive := s.cfg.K8sCollector.IsAlive()
			pingErr := s.cfg.K8sCollector.Ping(ctx)
			ready := s.cfg.K8sCollector.IsReady() && (pingErr == nil)
			colStatus := "healthy"
			if !alive {
				colStatus = "unavailable"
				if overallStatus == "healthy" {
					overallStatus = "degraded"
				}
			} else if pingErr != nil || !ready {
				colStatus = "degraded"
				if overallStatus == "healthy" {
					overallStatus = "degraded"
				}
			}

			colDetails := map[string]any{
				"alive":             alive,
				"ready":             ready,
				"connected":         pingErr == nil,
				"cluster_id":        s.cfg.K8sCollector.ClusterID(),
				"collection_status": "unreachable",
			}
			if pingErr != nil {
				colDetails["error"] = pingErr.Error()
			} else {
				colDetails["collection_status"] = "pending_sync"
			}

			if s.cfg.Store != nil {
				wsID := s.cfg.ConfiguredWorkspaceID
				if wsID == "" {
					wsID = "00000000-0000-0000-0000-000000000001"
				}
				auth, err := s.cfg.Store.GetWorkspaceAuthority(ctx, wsID, s.cfg.K8sCollector.ClusterID())
				if err == nil && auth != nil && auth.AuthoritativeRunID != "" {
					colDetails["last_run_id"] = auth.AuthoritativeRunID
					colDetails["last_sampling_at"] = auth.AuthoritativeSamplingStartedAt.Format(time.RFC3339)
					colDetails["updated_at"] = auth.UpdatedAt.Format(time.RFC3339)
					colDetails["last_generation"] = auth.LastGeneration
					colDetails["has_synced"] = true
					if pingErr == nil {
						colDetails["collection_status"] = "synced"
					}
				}
			}

			components["collector"] = ComponentHealth{
				Status:  colStatus,
				Details: colDetails,
			}
		} else {
			components["collector"] = ComponentHealth{
				Status:  "not_configured",
				Message: "Kubernetes collector is not enabled",
				Details: map[string]any{"connected": false},
			}
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		statusCode := http.StatusOK
		if overallStatus == "unavailable" {
			statusCode = http.StatusServiceUnavailable
		}
		w.WriteHeader(statusCode)
		_ = json.NewEncoder(w).Encode(SystemHealthResponse{
			Status:      overallStatus,
			Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
			Uptime:      time.Since(apiStartTime).Seconds(),
			Environment: s.cfg.Environment,
			Components:  components,
		})
	}
}

// ClusterDetail represents discovered or active cluster metadata.
type ClusterDetail struct {
	Name          string `json:"name"`
	Active        bool   `json:"active"`
	Connected     bool   `json:"connected"`
	Status        string `json:"status"` // "not_configured" | "unreachable" | "pending_sync" | "connected_empty" | "connected_with_resources"
	ResourceCount int    `json:"resource_count"`
	HasSynced     bool   `json:"has_synced"`
	LastSyncedAt  string `json:"last_synced_at,omitempty"`
	Error         string `json:"error,omitempty"`
}

// ListClustersResponse represents the response for GET /api/v1/clusters.
type ListClustersResponse struct {
	WorkspaceID   string          `json:"workspace_id"`
	ActiveCluster string          `json:"active_cluster,omitempty"`
	Configured    bool            `json:"configured"`
	Connected     bool            `json:"connected"`
	Status        string          `json:"status"`
	Error         string          `json:"error,omitempty"`
	Clusters      []ClusterDetail `json:"clusters"`
}

// handleListClusters handles GET /api/v1/clusters.
func (s *Server) handleListClusters() http.HandlerFunc {
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

		ctx := r.Context()
		resp := ListClustersResponse{
			WorkspaceID: wsID,
			Clusters:    []ClusterDetail{},
		}

		activeName := ""
		activeConnected := false
		activeStatus := "not_configured"
		activeErr := ""

		if s.cfg.K8sCollector != nil {
			resp.Configured = true
			activeName = s.cfg.K8sCollector.ClusterID()
			resp.ActiveCluster = activeName

			pingErr := s.cfg.K8sCollector.Ping(ctx)
			if pingErr != nil {
				activeConnected = false
				activeStatus = "unreachable"
				activeErr = pingErr.Error()
			} else {
				activeConnected = true
			}
		} else {
			resp.Configured = false
			activeStatus = "not_configured"
			activeErr = "No Kubernetes cluster is configured"
		}

		// Count resources per cluster in this workspace
		clusterCounts := make(map[string]int)
		if s.cfg.Store != nil {
			if resources, err := s.cfg.Store.ListResources(ctx, wsID); err == nil {
				for _, res := range resources {
					if strings.EqualFold(res.Identity.Provider, "kubernetes") || strings.EqualFold(res.Identity.Provider, "k8s") {
						if class, err := state.ClassifyKubernetesProviderID(res.Identity.ResourceType, res.Identity.ProviderID); err == nil && class.Cluster != "" {
							clusterCounts[class.Cluster]++
						}
					}
				}
			}
		}

		// Process active cluster detail
		if resp.Configured && activeName != "" {
			count := clusterCounts[activeName]
			hasSynced := false
			lastSyncedAt := ""

			if s.cfg.Store != nil {
				auth, authErr := s.cfg.Store.GetWorkspaceAuthority(ctx, wsID, activeName)
				if authErr == nil && auth != nil && auth.AuthoritativeRunID != "" {
					hasSynced = true
					lastSyncedAt = auth.UpdatedAt.Format(time.RFC3339)
				}
			}

			if activeConnected {
				if !hasSynced && count == 0 {
					activeStatus = "pending_sync"
				} else if count == 0 {
					activeStatus = "connected_empty"
				} else {
					activeStatus = "connected_with_resources"
				}
			}

			resp.Clusters = append(resp.Clusters, ClusterDetail{
				Name:          activeName,
				Active:        true,
				Connected:     activeConnected,
				Status:        activeStatus,
				ResourceCount: count,
				HasSynced:     hasSynced,
				LastSyncedAt:  lastSyncedAt,
				Error:         activeErr,
			})
		}

		// Add other clusters from stored resources
		for clName, count := range clusterCounts {
			if clName == activeName {
				continue
			}
			resp.Clusters = append(resp.Clusters, ClusterDetail{
				Name:          clName,
				Active:        false,
				Connected:     false,
				Status:        "connected_with_resources",
				ResourceCount: count,
				HasSynced:     true,
			})
		}

		sort.Slice(resp.Clusters, func(i, j int) bool {
			if resp.Clusters[i].Active != resp.Clusters[j].Active {
				return resp.Clusters[i].Active
			}
			return resp.Clusters[i].Name < resp.Clusters[j].Name
		})

		resp.Connected = activeConnected
		resp.Status = activeStatus
		resp.Error = activeErr

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	}
}
