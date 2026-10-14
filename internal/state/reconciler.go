package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "github.com/aaradhychinche-alt/WhatBreaks/gen/go/wb/core/v1"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/logging"
)

// Reconciler is the provider-agnostic contract for reconciling normalized observation
// batches with persistent workspace state.
type Reconciler interface {
	// Reconcile processes a batch of normalized evidence observations, synchronizing
	// resources, evidence, and discovered relationships while adhering to lifecycle,
	// immutability, and workspace-isolation invariants.
	Reconcile(ctx context.Context, batch ObservationBatch) (*ReconciliationResult, error)

	// ReconcileAndMaterialize reconciles an observation batch and refreshes the Rust Core Engine state.
	ReconcileAndMaterialize(ctx context.Context, batch ObservationBatch) (*ReconciliationResult, error)

	// Rematerialize loads the complete persisted state for workspaceID and refreshes the Rust Core Engine state.
	Rematerialize(ctx context.Context, workspaceID string) error
}

// ReconciliationResult provides detailed lifecycle and state synchronization telemetry.
type ReconciliationResult struct {
	WorkspaceID          string        `json:"workspace_id"`
	EvidenceCount        int           `json:"evidence_count"`
	ResourcesCreated     int           `json:"resources_created"`
	ResourcesUpdated     int           `json:"resources_updated"`
	ResourcesTotal       int           `json:"resources_total"`
	RelationshipsCreated int           `json:"relationships_created"`
	RelationshipsUpdated int           `json:"relationships_updated"`
	RelationshipsTotal   int           `json:"relationships_total"`
	DiscoveredCount      int           `json:"discovered_count"`
	ProvenanceLinksAdded int           `json:"provenance_links_added"`
	ConflictsCount       int           `json:"conflicts_count"`
	InsufficientCount    int           `json:"insufficient_count"`
	InvalidCount         int           `json:"invalid_count"`
	Duration             time.Duration `json:"duration"`
	Stale                bool          `json:"stale,omitempty"`
	Generation           int64         `json:"generation,omitempty"`
}

// DefaultReconciler coordinates observation ingestion, resource lifecycle tracking,
// relationship reconciliation, evidence immutability, and state materialization.
//
// Lifecycle & Deletion Guarantees:
// 1. Missing observation is NEVER interpreted as deletion.
// 2. No automatic reaper or timestamp-based tombstoning exists in v1.
// 3. No Inactive state is introduced (history preserves Supported vs Unknown).
// 4. Evidence records are strictly immutable.
// 5. Stable ResourceIdentity is preserved across updates.
type DefaultReconciler struct {
	store        Store
	discovery    DiscoveryRunner
	materializer *Materializer
	logger       logging.Logger

	mu           sync.Mutex
	workspaceMus map[string]*sync.Mutex
}

// NewReconciler constructs a DefaultReconciler instance.
func NewReconciler(
	store Store,
	discovery DiscoveryRunner,
	materializer *Materializer,
	logger logging.Logger,
) *DefaultReconciler {
	if logger == nil {
		logger = logging.NewJSONLogger(nil, logging.LevelInfo, "state-reconciler")
	}
	return &DefaultReconciler{
		store:        store,
		discovery:    discovery,
		materializer: materializer,
		logger:       logger,
		workspaceMus: make(map[string]*sync.Mutex),
	}
}

// getWorkspaceLock returns a dedicated mutex for workspaceID, enabling concurrent
// reconciliations across distinct workspaces while safely serializing concurrent
// reconciliations targeting the same workspace.
func (r *DefaultReconciler) getWorkspaceLock(ws string) *sync.Mutex {
	r.mu.Lock()
	defer r.mu.Unlock()
	lock, ok := r.workspaceMus[ws]
	if !ok {
		lock = &sync.Mutex{}
		r.workspaceMus[ws] = lock
	}
	return lock
}

// Reconcile processes a batch of normalized evidence observations for workspaceID.
func (r *DefaultReconciler) Reconcile(ctx context.Context, batch ObservationBatch) (*ReconciliationResult, error) {
	ws := strings.TrimSpace(batch.WorkspaceID)
	if ws == "" {
		return nil, ErrEmptyWorkspace
	}

	wsLock := r.getWorkspaceLock(ws)
	wsLock.Lock()
	defer wsLock.Unlock()

	start := time.Now()
	result := &ReconciliationResult{
		WorkspaceID:   ws,
		EvidenceCount: len(batch.Evidence),
	}

	if batch.ScopeSweep != nil && len(batch.ScopeSweep.RequiredFailures) > 0 {
		return nil, fmt.Errorf("reconciler: required collection scope(s) failed: %s", strings.Join(batch.ScopeSweep.RequiredFailures, ", "))
	}

	if len(batch.Evidence) == 0 {
		// Empty batch does NOT delete any state
		if existingRes, err := r.store.ListResources(ctx, ws); err == nil {
			result.ResourcesTotal = len(existingRes)
		}
		if existingRels, err := r.store.ListRelationships(ctx, ws); err == nil {
			result.RelationshipsTotal = len(existingRels)
		}
		result.Duration = time.Since(start)
		return result, nil
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if strings.Contains(batch.ClusterID, "/") {
		return nil, fmt.Errorf("%w: batch cluster ID %q cannot contain slashes", ErrMalformedProviderID, batch.ClusterID)
	}

	if batch.Scope != nil && strings.EqualFold(batch.Scope.Provider, "kubernetes") {
		if strings.TrimSpace(batch.ClusterID) == "" {
			return nil, fmt.Errorf("reconciler: kubernetes observation batch missing required cluster ID")
		}
		if batch.Scope.ScopeType == "cluster" && batch.Scope.ScopeID != "" && batch.Scope.ScopeID != batch.ClusterID {
			return nil, fmt.Errorf("reconciler: scope boundary ID %q does not match batch cluster boundary %q", batch.Scope.ScopeID, batch.ClusterID)
		}
	}
	if batch.ScopeSweep != nil && strings.TrimSpace(batch.ScopeSweep.ClusterID) != "" {
		if strings.TrimSpace(batch.ClusterID) != "" && batch.ScopeSweep.ClusterID != batch.ClusterID {
			return nil, fmt.Errorf("reconciler: scope sweep cluster ID %q does not match batch cluster boundary %q", batch.ScopeSweep.ClusterID, batch.ClusterID)
		}
	}

	// 1. Validate & Transform Evidence + Extract Subject Resources (In-Memory)
	domainEvidence := make([]Evidence, 0, len(batch.Evidence))
	incomingSubjectResources := make(map[string]struct {
		identity ResourceIdentity
		time     time.Time
	})

	for _, protoEv := range batch.Evidence {
		if protoEv == nil {
			continue
		}
		if protoEv.Id == "" {
			return nil, fmt.Errorf("reconciler: evidence missing required id")
		}
		if protoEv.Subject == nil || protoEv.Subject.Provider == "" || protoEv.Subject.ResourceType == "" || protoEv.Subject.ProviderId == "" {
			return nil, fmt.Errorf("reconciler: evidence %s missing required subject identity", protoEv.Id)
		}
		if protoEv.Source == nil || protoEv.Source.Provider == "" || protoEv.Source.Collector == "" {
			return nil, fmt.Errorf("reconciler: evidence %s missing required source", protoEv.Id)
		}

		observedAt, err := time.Parse(time.RFC3339Nano, protoEv.ObservedAt)
		if err != nil {
			observedAt, err = time.Parse(time.RFC3339, protoEv.ObservedAt)
			if err != nil {
				observedAt = time.Now().UTC()
			}
		}

		subjectID := ResourceIdentity{
			Provider:     protoEv.Subject.Provider,
			ResourceType: protoEv.Subject.ResourceType,
			ProviderID:   protoEv.Subject.ProviderId,
		}

		// Enforce Kubernetes identity contract and cluster boundary
		if strings.EqualFold(subjectID.Provider, "kubernetes") {
			if strings.TrimSpace(batch.ClusterID) == "" {
				return nil, fmt.Errorf("reconciler: kubernetes observation batch missing required cluster ID")
			}
			if err := ValidateKubernetesResourceIdentity(batch.ClusterID, subjectID); err != nil {
				return nil, fmt.Errorf("reconciler: evidence %s subject validation failed: %w", protoEv.Id, err)
			}
		}

		// Enforce validation on Kubernetes identities embedded in evidence Data (target, owner)
		if err := ValidateKubernetesEvidenceData(batch.ClusterID, protoEv.Data); err != nil {
			return nil, fmt.Errorf("reconciler: evidence %s payload validation failed: %w", protoEv.Id, err)
		}

		domainEvidence = append(domainEvidence, Evidence{
			WorkspaceID: ws,
			ID:          protoEv.Id,
			Source: EvidenceSource{
				Provider:  protoEv.Source.Provider,
				Collector: protoEv.Source.Collector,
			},
			ObservedAt:      observedAt,
			ObservationType: protoEv.ObservationType,
			Subject:         subjectID,
			Data:            protoEv.Data,
		})

		key := subjectID.String()
		if existing, ok := incomingSubjectResources[key]; !ok || observedAt.After(existing.time) {
			incomingSubjectResources[key] = struct {
				identity ResourceIdentity
				time     time.Time
			}{identity: subjectID, time: observedAt}
		}
	}

	// 2. DISCOVERY EXECUTION (In-Memory Pre-computation before ANY database transaction)
	// Objective 1: Discovery must finish before authoritative database writes begin.
	// Objective 8: Failed or cancelled discovery must not advance authority or modify authoritative state.
	var discoveredRels []Relationship
	var provenanceAssocs []ProvenanceAssociation
	var conflictsToSave []DiscoveryConflict
	mappedEvidenceIDs := make(map[string]bool)
	relEvidenceMap := make(map[RelationshipKey][]string)
	now := time.Now().UTC()

	if r.discovery != nil {
		discReq := &corev1.RunDiscoveryRequest{Evidence: batch.Evidence}
		discResp, err := r.discovery.RunDiscovery(ctx, discReq)
		if err != nil {
			r.logger.Error("Discovery execution failed in Core Engine",
				"workspace_id", ws,
				"evidence_count", len(batch.Evidence),
				"error", err.Error(),
			)
			return nil, fmt.Errorf("discovery execution failed: %w", err)
		}

		for _, res := range discResp.Results {
			if res == nil {
				continue
			}

			if disc := res.GetDiscovered(); disc != nil {
				result.DiscoveredCount++
				relProto := disc.Relationship
				if relProto == nil || relProto.Source == nil || relProto.Target == nil {
					continue
				}

				src := ResourceIdentity{
					Provider:     relProto.Source.Provider,
					ResourceType: relProto.Source.ResourceType,
					ProviderID:   relProto.Source.ProviderId,
				}
				tgt := ResourceIdentity{
					Provider:     relProto.Target.Provider,
					ResourceType: relProto.Target.ResourceType,
					ProviderID:   relProto.Target.ProviderId,
				}

				// Validate Kubernetes relationship endpoints against batch cluster boundary
				if strings.EqualFold(src.Provider, "kubernetes") {
					if err := ValidateKubernetesResourceIdentity(batch.ClusterID, src); err != nil {
						return nil, fmt.Errorf("reconciler: discovered relationship source validation failed: %w", err)
					}
				}
				if strings.EqualFold(tgt.Provider, "kubernetes") {
					if err := ValidateKubernetesResourceIdentity(batch.ClusterID, tgt); err != nil {
						return nil, fmt.Errorf("reconciler: discovered relationship target validation failed: %w", err)
					}
				}

				relKey := RelationshipKey{Source: src, Target: tgt, Kind: relProto.Kind}

				discoveredRels = append(discoveredRels, Relationship{
					WorkspaceID:     ws,
					Source:          src,
					Target:          tgt,
					Kind:            relProto.Kind,
					Category:        relProto.Category,
					FirstObservedAt: now,
					LastObservedAt:  now,
				})

				for _, evID := range disc.SupportingEvidenceIds {
					mappedEvidenceIDs[evID] = true
					provenanceAssocs = append(provenanceAssocs, ProvenanceAssociation{
						WorkspaceID:  ws,
						Relationship: relKey,
						EvidenceID:   evID,
					})
				}
				relEvidenceMap[relKey] = append(relEvidenceMap[relKey], disc.SupportingEvidenceIds...)

			} else if conf := res.GetConflict(); conf != nil {
				result.ConflictsCount++
				r.logger.Warn("Discovery conflict observed",
					"workspace_id", ws,
					"description", conf.Description,
				)
				conflictsToSave = append(conflictsToSave, DiscoveryConflict{
					WorkspaceID: ws,
					Description: conf.Description,
					ObservedAt:  now,
				})

			} else if res.GetInsufficient() != nil {
				result.InsufficientCount++
				r.logger.Debug("Evidence insufficient for relationship derivation",
					"workspace_id", ws,
				)

			} else if inv := res.GetInvalid(); inv != nil {
				result.InvalidCount++
				r.logger.Warn("Discovery encountered invalid evidence observation",
					"workspace_id", ws,
					"description", inv.Description,
				)
			}
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// 3. Assemble all resources to persist in memory (strictly directly observed subjects)
	// Architectural Invariant B: An unverified relationship endpoint must NOT be promoted into
	// state_resources merely because it appears in a relationship. No phantom resources are synthesized.
	var resourcesToSave []Resource
	for _, inc := range incomingSubjectResources {
		resourcesToSave = append(resourcesToSave, Resource{
			WorkspaceID:     ws,
			Identity:        inc.identity,
			FirstObservedAt: inc.time,
			LastObservedAt:  inc.time,
		})
	}

	// 4. Resolve collection run coordinates
	clusterID := batch.ClusterID
	if clusterID == "" && batch.Scope != nil && batch.Scope.ScopeID != "" {
		clusterID = batch.Scope.ScopeID
	}
	if clusterID == "" {
		clusterID = "default"
	}

	runID := batch.RunID
	if runID == "" {
		runID = fmt.Sprintf("run-%d", time.Now().UnixNano())
	}

	samplingStartedAt := batch.SamplingStartedAt
	if samplingStartedAt.IsZero() {
		samplingStartedAt = start
	}

	// 4b. Generate Structured Operational & Completeness Diagnostics
	// Determine all known observed resources: existing in store + incoming in batch
	observedResourcesMap := make(map[string]bool)
	if existingResList, err := r.store.ListResources(ctx, ws); err == nil {
		for _, er := range existingResList {
			observedResourcesMap[er.Identity.String()] = true
		}
	}
	for k := range incomingSubjectResources {
		observedResourcesMap[k] = true
	}

	diagMap := make(map[string]DiagnosticItem)

	// 4b.1. UNRESOLVED_REFERENCE diagnostics for unobserved relationship endpoints
	for _, rel := range discoveredRels {
		if !observedResourcesMap[rel.Target.String()] {
			d := DiagnosticItem{
				WorkspaceID:        ws,
				Code:               "UNRESOLVED_REFERENCE",
				Severity:           "WARNING",
				Message:            fmt.Sprintf("Referenced endpoint %s is unresolved", rel.Target.String()),
				Subject:            &rel.Source,
				ReferencedResource: &rel.Target,
				UnresolvedEndpoint: rel.Target.String(),
				Scope:              clusterID,
				EvidenceIDs:        relEvidenceMap[rel.Key()],
				Relevance:          "BLAST_RADIUS",
				CompletenessImpact: "PARTIAL",
				FirstObservedAt:    now,
				LastObservedAt:     now,
			}
			mergeDiagnostic(diagMap, d)
		}
		if !observedResourcesMap[rel.Source.String()] {
			d := DiagnosticItem{
				WorkspaceID:        ws,
				Code:               "UNRESOLVED_REFERENCE",
				Severity:           "WARNING",
				Message:            fmt.Sprintf("Source resource %s is unresolved", rel.Source.String()),
				Subject:            &rel.Source,
				ReferencedResource: &rel.Source,
				UnresolvedEndpoint: rel.Source.String(),
				Scope:              clusterID,
				EvidenceIDs:        relEvidenceMap[rel.Key()],
				Relevance:          "BLAST_RADIUS",
				CompletenessImpact: "PARTIAL",
				FirstObservedAt:    now,
				LastObservedAt:     now,
			}
			mergeDiagnostic(diagMap, d)
		}
	}

	// 4b.2. UNMAPPED_CONNECTION diagnostics for unmapped RUNTIME_CONNECTION evidence
	for _, protoEv := range batch.Evidence {
		if protoEv != nil && protoEv.ObservationType == "RUNTIME_CONNECTION" && !mappedEvidenceIDs[protoEv.Id] {
			endpointStr := extractEndpointFromConnectionData(protoEv.Data)
			caller := ResourceIdentity{
				Provider:     protoEv.Subject.Provider,
				ResourceType: protoEv.Subject.ResourceType,
				ProviderID:   protoEv.Subject.ProviderId,
			}
			d := DiagnosticItem{
				WorkspaceID:        ws,
				Code:               "UNMAPPED_CONNECTION",
				Severity:           "WARNING",
				Message:            fmt.Sprintf("Runtime connection from %s to endpoint %s could not be mapped to any known target", caller.String(), endpointStr),
				Subject:            &caller,
				UnresolvedEndpoint: endpointStr,
				Scope:              clusterID,
				EvidenceIDs:        []string{protoEv.Id},
				Relevance:          "BLAST_RADIUS",
				CompletenessImpact: "PARTIAL",
				FirstObservedAt:    now,
				LastObservedAt:     now,
			}
			mergeDiagnostic(diagMap, d)
		}
	}

	// 4b.3. INCOMPLETE_SCOPE diagnostics for optional scope failures
	if batch.ScopeSweep != nil {
		for _, sc := range batch.ScopeSweep.OptionalFailures {
			d := DiagnosticItem{
				WorkspaceID:        ws,
				Code:               "INCOMPLETE_SCOPE",
				Severity:           "WARNING",
				Message:            fmt.Sprintf("Optional discovery scope %s failed during sweep", sc),
				Scope:              sc,
				Relevance:          "WORKSPACE",
				CompletenessImpact: "PARTIAL",
				FirstObservedAt:    now,
				LastObservedAt:     now,
			}
			mergeDiagnostic(diagMap, d)
		}
	}

	// 4b.4. DISCOVERY_CONFLICT diagnostics
	for _, conf := range conflictsToSave {
		d := DiagnosticItem{
			WorkspaceID:        ws,
			Code:               "DISCOVERY_CONFLICT",
			Severity:           "WARNING",
			Message:            conf.Description,
			Scope:              clusterID,
			Relevance:          "WORKSPACE",
			CompletenessImpact: "PARTIAL",
			FirstObservedAt:    now,
			LastObservedAt:     now,
		}
		mergeDiagnostic(diagMap, d)
	}

	// 4b.5. Merge any caller-provided diagnostics
	for _, d := range batch.Diagnostics {
		if d.ReferencedResource != nil && strings.EqualFold(d.ReferencedResource.Provider, "kubernetes") {
			if err := ValidateKubernetesResourceIdentity(batch.ClusterID, *d.ReferencedResource); err != nil {
				return nil, fmt.Errorf("reconciler: diagnostic referenced resource validation failed: %w", err)
			}
		}
		d.WorkspaceID = ws
		mergeDiagnostic(diagMap, d)
	}

	var diagnosticsToSave []DiagnosticItem
	for _, d := range diagMap {
		diagnosticsToSave = append(diagnosticsToSave, d)
	}
	sort.Slice(diagnosticsToSave, func(i, j int) bool {
		return diagnosticsToSave[i].NaturalKey() < diagnosticsToSave[j].NaturalKey()
	})

	// 5. ATOMIC AUTHORITATIVE RECONCILIATION IN A SINGLE TRANSACTION
	// Objectives 2, 3, 4, 5, 6, 7, 9, 10
	batchParams := AuthoritativeBatchParams{
		WorkspaceID:       ws,
		ClusterID:         clusterID,
		RunID:             runID,
		SamplingStartedAt: samplingStartedAt,
		Generation:        batch.Generation,
		Evidence:          domainEvidence,
		Resources:         resourcesToSave,
		Relationships:     discoveredRels,
		Provenance:        provenanceAssocs,
		Conflicts:         conflictsToSave,
		Diagnostics:       diagnosticsToSave,
		ScopeSweep:        batch.ScopeSweep,
	}

	batchRes, err := r.store.ReconcileAuthoritativeBatch(ctx, batchParams)
	if err != nil {
		if errors.Is(err, ErrStaleCollectionRunIgnored) {
			result.Stale = true
			if batchRes != nil {
				result.Generation = batchRes.AssignedGeneration
			}
			r.logger.Warn("Reconciliation rejected: stale collection run",
				"workspace_id", ws,
				"cluster_id", clusterID,
				"run_id", runID,
				"sampling_started_at", samplingStartedAt,
				"error", err.Error(),
			)
			return result, err
		}
		return nil, err
	}

	result.ResourcesCreated = batchRes.ResourcesCreated
	result.ResourcesUpdated = batchRes.ResourcesUpdated
	result.RelationshipsCreated = batchRes.RelationshipsCreated
	result.RelationshipsUpdated = batchRes.RelationshipsUpdated
	result.ProvenanceLinksAdded = batchRes.ProvenanceLinksAdded
	result.Generation = batchRes.AssignedGeneration

	// 6. Query totals for telemetry
	if totalRes, err := r.store.ListResources(ctx, ws); err == nil {
		result.ResourcesTotal = len(totalRes)
	}
	if totalRels, err := r.store.ListRelationships(ctx, ws); err == nil {
		result.RelationshipsTotal = len(totalRels)
	}

	result.Duration = time.Since(start)
	r.logger.Info("Reconciliation completed successfully",
		"workspace_id", ws,
		"cluster_id", clusterID,
		"run_id", runID,
		"generation", result.Generation,
		"evidence_count", result.EvidenceCount,
		"resources_created", result.ResourcesCreated,
		"resources_updated", result.ResourcesUpdated,
		"resources_total", result.ResourcesTotal,
		"relationships_created", result.RelationshipsCreated,
		"relationships_updated", result.RelationshipsUpdated,
		"relationships_total", result.RelationshipsTotal,
		"duration_ms", result.Duration.Milliseconds(),
	)

	return result, nil
}

// ReconcileAndMaterialize reconciles an observation batch and refreshes the Rust Core Engine state.
func (r *DefaultReconciler) ReconcileAndMaterialize(ctx context.Context, batch ObservationBatch) (*ReconciliationResult, error) {
	res, err := r.Reconcile(ctx, batch)
	if err != nil {
		return res, err
	}
	if res != nil && res.Stale {
		return res, ErrStaleCollectionRunIgnored
	}

	if r.materializer != nil {
		if _, err := r.materializer.Materialize(ctx, batch.WorkspaceID); err != nil {
			r.logger.Error("Post-reconciliation state materialization failed",
				"workspace_id", batch.WorkspaceID,
				"error", err.Error(),
			)
			return res, fmt.Errorf("materialization failed: %w", err)
		}
	}

	return res, nil
}

// Rematerialize loads the complete persisted state for workspaceID from Store and materializes it into the Core Engine.
func (r *DefaultReconciler) Rematerialize(ctx context.Context, workspaceID string) error {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return ErrEmptyWorkspace
	}
	if r.materializer == nil {
		return fmt.Errorf("reconciler: materializer is not configured")
	}
	_, err := r.materializer.Materialize(ctx, ws)
	return err
}

func mergeDiagnostic(m map[string]DiagnosticItem, d DiagnosticItem) {
	k := d.NaturalKey()
	existing, found := m[k]
	if !found {
		evIDs := d.EvidenceIDs
		if evIDs == nil {
			evIDs = []string{}
		}
		sort.Strings(evIDs)
		d.EvidenceIDs = evIDs
		m[k] = d
		return
	}
	if d.LastObservedAt.After(existing.LastObservedAt) {
		existing.LastObservedAt = d.LastObservedAt
	}
	evMap := make(map[string]bool)
	for _, id := range existing.EvidenceIDs {
		evMap[id] = true
	}
	for _, id := range d.EvidenceIDs {
		evMap[id] = true
	}
	merged := make([]string, 0, len(evMap))
	for id := range evMap {
		merged = append(merged, id)
	}
	sort.Strings(merged)
	existing.EvidenceIDs = merged
	m[k] = existing
}

func extractEndpointFromConnectionData(data []byte) string {
	if len(data) == 0 {
		return "unknown"
	}
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return string(data)
	}
	if ep, ok := m["endpoint"].(string); ok && ep != "" {
		return ep
	}
	if dest, ok := m["dest"].(string); ok && dest != "" {
		if port, ok := m["port"]; ok {
			return fmt.Sprintf("%v:%v", dest, port)
		}
		return dest
	}
	if dest, ok := m["destination"].(string); ok && dest != "" {
		if port, ok := m["port"]; ok {
			return fmt.Sprintf("%v:%v", dest, port)
		}
		return dest
	}
	if call, ok := m["call"].(string); ok && call != "" {
		return call
	}
	return "unknown"
}

// LegacyStateRecoverer defines the administrative recovery capability for workspaces
// containing incompatible legacy Kubernetes state.
type LegacyStateRecoverer interface {
	RecoverLegacyState(ctx context.Context, params LegacyRecoveryParams) (*LegacyRecoveryResult, error)
}

// RecoverLegacyState performs safe, authoritative administrative legacy recovery for a workspace:
// 1. Validates that destructive recovery is explicitly authorized (params.AuthorizeDestructive == true).
// 2. Acquires the workspace serialization lock to prevent races with ongoing reconciliations or materializations.
// 3. Executes the underlying Store's transactional recovery (archiving legacy resources, relationships,
//    provenance, and diagnostics while strictly preserving historical evidence and non-K8s provider state).
// 4. Ingests any provided authoritative recollection batch.
// 5. If a materializer is configured, refreshes the Rust Core Engine with the authoritative clean state.
func (r *DefaultReconciler) RecoverLegacyState(ctx context.Context, params LegacyRecoveryParams) (*LegacyRecoveryResult, error) {
	if !params.AuthorizeDestructive {
		return nil, ErrUnauthorizedDestructiveAction
	}
	if strings.TrimSpace(params.WorkspaceID) == "" {
		return nil, errors.New("workspace_id is required")
	}

	wsLock := r.getWorkspaceLock(params.WorkspaceID)
	wsLock.Lock()
	defer wsLock.Unlock()

	// Invalidate workspace in Core Engine immediately before recovery so no queries can run against stale state.
	if r.materializer != nil {
		_ = r.materializer.InvalidateWorkspace(ctx, params.WorkspaceID)
	}

	result, err := r.store.RecoverWorkspaceLegacyState(ctx, params)
	if err != nil {
		return nil, err
	}

	// If materializer is available, rematerialize the newly clean state into the Core Engine.
	if r.materializer != nil {
		if _, matErr := r.materializer.Materialize(ctx, params.WorkspaceID); matErr != nil {
			r.logger.Warn("Failed to rematerialize workspace after legacy recovery",
				"workspace_id", params.WorkspaceID,
				"error", matErr.Error(),
			)
			return result, fmt.Errorf("workspace recovered but core engine rematerialization failed: %w", matErr)
		}
	}

	r.logger.Info("Legacy state recovery completed successfully",
		"workspace_id", params.WorkspaceID,
		"cluster_id", params.ClusterID,
		"resources_archived", result.LegacyResourcesArchived,
		"relationships_archived", result.LegacyRelationshipsArchived,
		"provenance_archived", result.LegacyProvenanceArchived,
		"diagnostics_archived", result.LegacyDiagnosticsArchived,
		"evidence_preserved", result.EvidencePreservedCount,
		"non_k8s_resources_preserved", result.NonK8sResourcesPreserved,
	)

	return result, nil
}
