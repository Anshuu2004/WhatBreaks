package state

import (
	"fmt"
	"strings"
	"time"

	corev1 "github.com/aaradhychinche-alt/WhatBreaks/gen/go/wb/core/v1"
)

// ResourceIdentity uniquely identifies an infrastructure resource across providers.
// Provider-agnostic: does not contain Kubernetes-specific identity fields.
type ResourceIdentity struct {
	Provider     string `json:"provider"`
	ResourceType string `json:"resource_type"`
	ProviderID   string `json:"provider_id"`
}

// String returns a canonical representation of the resource identity.
func (r ResourceIdentity) String() string {
	return fmt.Sprintf("%s:%s:%s", r.Provider, r.ResourceType, r.ProviderID)
}

// Resource represents a registered infrastructure resource with observation lifecycle timestamps.
type Resource struct {
	WorkspaceID     string           `json:"workspace_id"`
	Identity        ResourceIdentity `json:"identity"`
	FirstObservedAt time.Time        `json:"first_observed_at"`
	LastObservedAt  time.Time        `json:"last_observed_at"`
}

// EvidenceSource identifies the origin of an evidence observation.
type EvidenceSource struct {
	Provider  string `json:"provider"`
	Collector string `json:"collector"`
}

// Evidence is an immutable historical observation.
type Evidence struct {
	WorkspaceID     string           `json:"workspace_id"`
	ID              string           `json:"id"`
	Source          EvidenceSource   `json:"source"`
	ObservedAt      time.Time        `json:"observed_at"`
	ObservationType string           `json:"observation_type"`
	Subject         ResourceIdentity `json:"subject"`
	Data            []byte           `json:"data"`
}

// RelationshipKey identifies a relationship naturally by source, target, and kind.
type RelationshipKey struct {
	Source ResourceIdentity `json:"source"`
	Target ResourceIdentity `json:"target"`
	Kind   string           `json:"kind"`
}

// String returns a canonical string representation for hashing and debugging.
func (k RelationshipKey) String() string {
	return fmt.Sprintf("%s -[%s]-> %s", k.Source.String(), k.Kind, k.Target.String())
}

// Relationship represents a semantic directed link between two infrastructure resources.
type Relationship struct {
	WorkspaceID     string           `json:"workspace_id"`
	Source          ResourceIdentity `json:"source"`
	Target          ResourceIdentity `json:"target"`
	Kind            string           `json:"kind"`
	Category        string           `json:"category"`
	FirstObservedAt time.Time        `json:"first_observed_at"`
	LastObservedAt  time.Time        `json:"last_observed_at"`
}

// Key returns the natural identity key of the relationship.
func (r Relationship) Key() RelationshipKey {
	return RelationshipKey{
		Source: r.Source,
		Target: r.Target,
		Kind:   r.Kind,
	}
}

// ProvenanceAssociation models the many-to-many link between a relationship and a supporting evidence ID.
type ProvenanceAssociation struct {
	WorkspaceID  string          `json:"workspace_id"`
	Relationship RelationshipKey `json:"relationship"`
	EvidenceID   string          `json:"evidence_id"`
}

// DiscoveryConflict records a contradiction observed during discovery.
type DiscoveryConflict struct {
	WorkspaceID string    `json:"workspace_id"`
	Description string    `json:"description"`
	ObservedAt  time.Time `json:"observed_at"`
}

// AssembledState represents the complete materialized domain snapshot for a specific workspace.
type AssembledState struct {
	WorkspaceID   string                       `json:"workspace_id"`
	Resources     []Resource                   `json:"resources"`
	Relationships []Relationship               `json:"relationships"`
	Evidence      []Evidence                   `json:"evidence"`
	Provenance    map[RelationshipKey][]string `json:"provenance"`
	Diagnostics   []DiagnosticItem             `json:"diagnostics"`
}

// ScopeBoundary represents an optional authoritative scope declaration (e.g., a specific cluster or namespace inventory).
// In v1, WhatBreaks does NOT infer deletion from observation absence within a scope boundary
// because collectors do not provide authoritative deletion guarantees. This structure establishes
// the contract for future completeness-aware reconciliation.
type ScopeBoundary struct {
	Provider  string            `json:"provider"`
	ScopeType string            `json:"scope_type,omitempty"` // e.g., "namespace", "cluster", "account"
	ScopeID   string            `json:"scope_id,omitempty"`   // e.g., "default", "production"
	Complete  bool              `json:"complete"`             // whether this sweep asserts authoritative completeness
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// WorkspaceAuthority represents authoritative collection-run state for a workspace and cluster.
type WorkspaceAuthority struct {
	WorkspaceID                 string    `json:"workspace_id"`
	ClusterID                   string    `json:"cluster_id"`
	AuthoritativeRunID          string    `json:"authoritative_run_id"`
	AuthoritativeSamplingStartedAt time.Time `json:"authoritative_sampling_started_at"`
	LastGeneration              int64     `json:"last_generation"`
	UpdatedAt                   time.Time `json:"updated_at"`
}

// StaleRunAudit records rejected collection runs for auditability without mutating authoritative state.
type StaleRunAudit struct {
	ID                int64     `json:"id,omitempty"`
	WorkspaceID       string    `json:"workspace_id"`
	ClusterID         string    `json:"cluster_id"`
	RunID             string    `json:"run_id"`
	SamplingStartedAt time.Time `json:"sampling_started_at"`
	Generation        int64     `json:"generation"`
	Status            string    `json:"status"`
	Reason            string    `json:"reason"`
	ObservedAt        time.Time `json:"observed_at"`
}

// AuthoritativeBatchParams packages all entities to be reconciled within a single atomic database transaction.
type AuthoritativeBatchParams struct {
	WorkspaceID       string
	ClusterID         string
	RunID             string
	SamplingStartedAt time.Time
	Generation        int64
	Evidence          []Evidence
	Resources         []Resource
	Relationships     []Relationship
	Provenance        []ProvenanceAssociation
	Conflicts         []DiscoveryConflict
	Diagnostics       []DiagnosticItem
	ScopeSweep        *ScopeSweep
	// isRecovery is an internal flag that bypasses the FAILED/IN_PROGRESS recovery
	// guard for internal recollection batches during RecoverWorkspaceLegacyState.
	// It is unexported to prevent external callers or non-recovery paths from
	// bypassing the guard. The public ReconcileAuthoritativeBatch entry point
	// strictly resets this to false. Only RecoverWorkspaceLegacyState may enable it
	// via withRecoveryBypass after ValidateRecoveryAuthorization succeeds.
	isRecovery bool
}

// withRecoveryBypass returns a copy of params with isRecovery set to true.
// This is package-internal and must only be called from RecoverWorkspaceLegacyState
// after ValidateRecoveryAuthorization has succeeded.
func withRecoveryBypass(params AuthoritativeBatchParams) AuthoritativeBatchParams {
	params.isRecovery = true
	return params
}

// WithRecoveryBypassForTesting is exposed exclusively for tests to assert that
// public reconciliation entry points strictly reset the bypass flag.
func WithRecoveryBypassForTesting(params AuthoritativeBatchParams) AuthoritativeBatchParams {
	return withRecoveryBypass(params)
}

// AuthoritativeBatchResult reports the outcome of the authoritative batch transaction.
type AuthoritativeBatchResult struct {
	Stale                bool
	CurrentSamplingAt    time.Time
	CurrentGeneration    int64
	AssignedGeneration   int64
	ResourcesCreated     int
	ResourcesUpdated     int
	RelationshipsCreated int
	RelationshipsUpdated int
	ProvenanceLinksAdded int
}

// ObservationBatch packages normalized evidence records for ingestion into a specific workspace.
type ObservationBatch struct {
	WorkspaceID       string             `json:"workspace_id"`
	ClusterID         string             `json:"cluster_id,omitempty"`
	RunID             string             `json:"run_id,omitempty"`
	SamplingStartedAt time.Time          `json:"sampling_started_at,omitempty"`
	Generation        int64              `json:"generation,omitempty"`
	Evidence          []*corev1.Evidence `json:"evidence"`
	Scope             *ScopeBoundary     `json:"scope,omitempty"`
	ScopeSweep        *ScopeSweep        `json:"scope_sweep,omitempty"`
	Diagnostics       []DiagnosticItem   `json:"diagnostics,omitempty"`
}

// DiagnosticItem represents a durable operational or completeness diagnostic affecting a workspace or analysis.
type DiagnosticItem struct {
	WorkspaceID        string            `json:"workspace_id"`
	Code               string            `json:"code"`
	Severity           string            `json:"severity"`
	Message            string            `json:"message"`
	Subject            *ResourceIdentity `json:"subject,omitempty"`
	ReferencedResource *ResourceIdentity `json:"referenced_resource,omitempty"`
	UnresolvedEndpoint string            `json:"unresolved_endpoint,omitempty"`
	Scope              string            `json:"scope,omitempty"`
	EvidenceIDs        []string          `json:"evidence_ids,omitempty"`
	Relevance          string            `json:"relevance,omitempty"`
	CompletenessImpact string            `json:"completeness_impact,omitempty"`
	FirstObservedAt    time.Time         `json:"first_observed_at"`
	LastObservedAt     time.Time         `json:"last_observed_at"`
	Resolved           bool              `json:"resolved"`
	ResolvedAt         *time.Time        `json:"resolved_at,omitempty"`
}

// NaturalKey returns the deterministic logical identity key for deduplication and persistence.
func (d DiagnosticItem) NaturalKey() string {
	subStr := ""
	if d.Subject != nil {
		subStr = d.Subject.String()
	}
	refStr := ""
	if d.ReferencedResource != nil {
		refStr = d.ReferencedResource.String()
	}
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s",
		d.WorkspaceID,
		d.Code,
		subStr,
		refStr,
		d.UnresolvedEndpoint,
		d.Scope,
	)
}

// ToProto converts a domain DiagnosticItem into its protobuf representation.
func (d DiagnosticItem) ToProto() *corev1.DiagnosticItem {
	var sev corev1.DiagnosticSeverity
	switch strings.ToUpper(d.Severity) {
	case "INFO":
		sev = corev1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_INFO
	case "WARNING":
		sev = corev1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_WARNING
	case "ERROR":
		sev = corev1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_ERROR
	default:
		sev = corev1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_WARNING
	}

	var rel corev1.DiagnosticRelevance
	switch strings.ToUpper(d.Relevance) {
	case "TARGET":
		rel = corev1.DiagnosticRelevance_DIAGNOSTIC_RELEVANCE_TARGET
	case "BLAST_RADIUS":
		rel = corev1.DiagnosticRelevance_DIAGNOSTIC_RELEVANCE_BLAST_RADIUS
	case "WORKSPACE":
		rel = corev1.DiagnosticRelevance_DIAGNOSTIC_RELEVANCE_WORKSPACE
	default:
		rel = corev1.DiagnosticRelevance_DIAGNOSTIC_RELEVANCE_WORKSPACE
	}

	var comp corev1.CompletenessStatus
	switch strings.ToUpper(d.CompletenessImpact) {
	case "COMPLETE":
		comp = corev1.CompletenessStatus_COMPLETENESS_STATUS_COMPLETE
	case "PARTIAL":
		comp = corev1.CompletenessStatus_COMPLETENESS_STATUS_PARTIAL
	case "DEGRADED":
		comp = corev1.CompletenessStatus_COMPLETENESS_STATUS_DEGRADED
	default:
		comp = corev1.CompletenessStatus_COMPLETENESS_STATUS_PARTIAL
	}

	proto := &corev1.DiagnosticItem{
		Code:                  d.Code,
		Severity:              sev,
		Message:               d.Message,
		SupportingEvidenceIds: d.EvidenceIDs,
		Scope:                 d.Scope,
		UnresolvedEndpoint:    d.UnresolvedEndpoint,
		Relevance:             rel,
		CompletenessImpact:    comp,
	}

	if d.Subject != nil {
		proto.Target = &corev1.ResourceIdentity{
			Provider:     d.Subject.Provider,
			ResourceType: d.Subject.ResourceType,
			ProviderId:   d.Subject.ProviderID,
		}
	}

	if d.ReferencedResource != nil {
		proto.ReferencedResource = &corev1.ResourceIdentity{
			Provider:     d.ReferencedResource.Provider,
			ResourceType: d.ReferencedResource.ResourceType,
			ProviderId:   d.ReferencedResource.ProviderID,
		}
	}

	if !d.FirstObservedAt.IsZero() {
		proto.FirstObservedAt = d.FirstObservedAt.Format(time.RFC3339Nano)
	}
	if !d.LastObservedAt.IsZero() {
		proto.LastObservedAt = d.LastObservedAt.Format(time.RFC3339Nano)
	}

	return proto
}

// ScopeSweep records the completeness and outcome of an individual collection scope sweep.
type ScopeSweep struct {
	WorkspaceID       string            `json:"workspace_id"`
	ClusterID         string            `json:"cluster_id"`
	RunID             string            `json:"run_id"`
	SamplingStartedAt time.Time         `json:"sampling_started_at"`
	Generation        int64             `json:"generation"`
	SuccessfulScopes  []string          `json:"successful_scopes"`
	FailedScopes      []string          `json:"failed_scopes"`
	RequiredFailures  []string          `json:"required_failures"`
	OptionalFailures  []string          `json:"optional_failures"`
	ScopeErrors       map[string]string `json:"scope_errors"`
	ObservedAt        time.Time         `json:"observed_at"`
}
