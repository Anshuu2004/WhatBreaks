package state

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aaradhychinche-alt/WhatBreaks/internal/auth"
)

var (
	// ErrResourceNotFound is returned when a requested resource is not present.
	ErrResourceNotFound = errors.New("state store: resource not found")

	// ErrRelationshipNotFound is returned when a requested relationship is not present.
	ErrRelationshipNotFound = errors.New("state store: relationship not found")

	// ErrInvalidWorkspace is returned when workspace ID is missing or invalid.
	ErrInvalidWorkspace = errors.New("state store: workspace_id cannot be empty")

	// ErrStaleCollectionRunIgnored is returned when a collection run is older than the authoritative state.
	ErrStaleCollectionRunIgnored = errors.New("state store: collection run is stale and was rejected")

	// ErrAuthorityNotFound is returned when no authority record exists for a workspace and cluster.
	ErrAuthorityNotFound = errors.New("state store: authority not found")

	// ErrWorkspaceRecoveryRequired is returned when an interrupted or failed recovery blocks normal operations.
	ErrWorkspaceRecoveryRequired = errors.New("state store: workspace recovery required")
)

// Store defines the persistent storage contract for universal WhatBreaks state.
// All operations are explicitly partitioned by workspace_id to guarantee multi-tenant isolation.
type Store interface {
	// SaveResources registers or updates resources within the specified workspace.
	SaveResources(ctx context.Context, workspaceID string, resources []Resource) error

	// SaveEvidence stores immutable evidence observations within the specified workspace.
	SaveEvidence(ctx context.Context, workspaceID string, evidence []Evidence) error

	// SaveRelationships stores discovered relationships within the specified workspace.
	SaveRelationships(ctx context.Context, workspaceID string, relationships []Relationship) error

	// SaveProvenance records many-to-many associations between relationships and evidence IDs.
	SaveProvenance(ctx context.Context, workspaceID string, associations []ProvenanceAssociation) error

	// SaveConflict records a discovery contradiction for diagnostics.
	SaveConflict(ctx context.Context, conflict DiscoveryConflict) error

	// GetResource retrieves a specific resource by identity within the workspace.
	GetResource(ctx context.Context, workspaceID string, identity ResourceIdentity) (*Resource, error)

	// GetRelationship retrieves a specific relationship by key within the workspace.
	GetRelationship(ctx context.Context, workspaceID string, rel RelationshipKey) (*Relationship, error)

	// ListResources returns all resources registered in the workspace.
	ListResources(ctx context.Context, workspaceID string) ([]Resource, error)

	// ListRelationships returns all relationships active in the workspace.
	ListRelationships(ctx context.Context, workspaceID string) ([]Relationship, error)

	// ListEvidence returns all historical evidence observations in the workspace.
	ListEvidence(ctx context.Context, workspaceID string) ([]Evidence, error)

	// GetProvenanceForRelationship returns all evidence IDs supporting a specific relationship.
	GetProvenanceForRelationship(ctx context.Context, workspaceID string, rel RelationshipKey) ([]string, error)

	// GetRelationshipsForEvidence returns all relationships supported by an evidence ID.
	GetRelationshipsForEvidence(ctx context.Context, workspaceID string, evidenceID string) ([]RelationshipKey, error)

	// LoadAssembledState reconstructs the complete domain state snapshot for a workspace.
	LoadAssembledState(ctx context.Context, workspaceID string) (*AssembledState, error)

	// ListWorkspaces returns all distinct workspace IDs currently persisted in the store in deterministic order.
	ListWorkspaces(ctx context.Context) ([]string, error)

	// DeleteWorkspaceState clears all stored state for a workspace.
	DeleteWorkspaceState(ctx context.Context, workspaceID string) error

	// ReconcileAuthoritativeBatch atomically evaluates run authority under lock, rejects stale runs,
	// persists all authoritative domain entities within a single transaction, and advances authority.
	ReconcileAuthoritativeBatch(ctx context.Context, params AuthoritativeBatchParams) (*AuthoritativeBatchResult, error)

	// GetWorkspaceAuthority returns the current authority state for (workspace_id, cluster_id).
	GetWorkspaceAuthority(ctx context.Context, workspaceID string, clusterID string) (*WorkspaceAuthority, error)

	// RecordStaleRunAudit persists an audit trail entry for a rejected collection run.
	RecordStaleRunAudit(ctx context.Context, audit StaleRunAudit) error

	// ListStaleRunAudits returns all stale run audit records for a workspace.
	ListStaleRunAudits(ctx context.Context, workspaceID string) ([]StaleRunAudit, error)

	// ListDiagnostics returns all active diagnostics for a workspace.
	ListDiagnostics(ctx context.Context, workspaceID string) ([]DiagnosticItem, error)

	// SaveDiagnostics persists or updates diagnostics for a workspace.
	SaveDiagnostics(ctx context.Context, workspaceID string, diagnostics []DiagnosticItem) error

	// SaveScopeSweep persists scope sweep completeness metadata.
	SaveScopeSweep(ctx context.Context, sweep ScopeSweep) error

	// GetScopeSweep retrieves a scope sweep record by workspace, cluster, and run IDs.
	GetScopeSweep(ctx context.Context, workspaceID string, clusterID string, runID string) (*ScopeSweep, error)

	// ListScopeSweeps lists historical scope sweep records for a workspace.
	ListScopeSweeps(ctx context.Context, workspaceID string) ([]ScopeSweep, error)

	// CheckWorkspaceIntegrity inspects all identity-bearing state for workspaceID and reports whether it is safe for analysis.
	CheckWorkspaceIntegrity(ctx context.Context, workspaceID string) error

	// InspectWorkspaceIntegrity inspects all identity-bearing state for workspaceID and returns a detailed report.
	InspectWorkspaceIntegrity(ctx context.Context, workspaceID string) (*WorkspaceIntegrityReport, error)

	// GetEvidence retrieves a single evidence record by ID.
	GetEvidence(ctx context.Context, workspaceID string, evidenceID string) (*Evidence, error)

	// GetOutgoingRelationships returns relationships originating from source.
	GetOutgoingRelationships(ctx context.Context, workspaceID string, source ResourceIdentity) ([]Relationship, error)

	// RecoverWorkspaceLegacyState archives legacy Kubernetes state and safely restores canonical state.
	RecoverWorkspaceLegacyState(ctx context.Context, params LegacyRecoveryParams) (*LegacyRecoveryResult, error)

	// ListRecoveryAudits retrieves historical recovery audit records for a workspace.
	ListRecoveryAudits(ctx context.Context, workspaceID string) ([]RecoveryAuditRecord, error)
}

// WorkspaceIntegrityReport describes detailed metrics about workspace identity health.
type WorkspaceIntegrityReport struct {
	WorkspaceID                string   `json:"workspace_id"`
	IsClean                    bool     `json:"is_clean"`
	RecoveryRequired           bool     `json:"recovery_required"`
	LegacyResourcesCount       int      `json:"legacy_resources_count"`
	LegacyRelationshipsCount   int      `json:"legacy_relationships_count"`
	LegacyDiagnosticsCount     int      `json:"legacy_diagnostics_count"`
	CrossClusterRelationsCount int      `json:"cross_cluster_relations_count"`
	IntegrityErrors            []string `json:"integrity_errors,omitempty"`
}

// WorkspaceIntegrityChecker checks whether a workspace's state conforms to canonical identity invariants.
type WorkspaceIntegrityChecker interface {
	CheckWorkspaceIntegrity(ctx context.Context, workspaceID string) error
}

// AuthoritativeBatch is an alias for AuthoritativeBatchParams for legacy compatibility.
type AuthoritativeBatch = AuthoritativeBatchParams

// LegacyRecoveryParams configures an administrative legacy state recovery operation.
type LegacyRecoveryParams struct {
	WorkspaceID          string
	ClusterID            string
	TargetClusterIDs     []string // optional cluster boundaries
	AuthorizeDestructive bool
	OperatorIdentity     string
	Reason               string
	AuthoritativeBatch   *AuthoritativeBatchParams
}

// RecoveryAuditRecord models a durable audit entry for administrative legacy state recovery.
type RecoveryAuditRecord struct {
	ID                          int64     `json:"id"`
	OperationID                 string    `json:"operation_id,omitempty"`
	WorkspaceID                 string    `json:"workspace_id"`
	ClusterID                   string    `json:"cluster_id"`
	OperatorIdentity            string    `json:"operator_identity"`
	Reason                      string    `json:"reason"`
	Status                      string    `json:"status"` // "SUCCESS", "FAILED"
	FailureCategory             string    `json:"failure_category,omitempty"`
	ErrorMessage                string    `json:"error_message,omitempty"`
	LegacyResourcesArchived     int       `json:"legacy_resources_archived"`
	LegacyRelationshipsArchived int       `json:"legacy_relationships_archived"`
	LegacyProvenanceArchived    int       `json:"legacy_provenance_archived"`
	LegacyDiagnosticsArchived   int       `json:"legacy_diagnostics_archived"`
	RecollectedResourcesCount   int       `json:"recollected_resources_count"`
	EvidencePreservedCount      int       `json:"evidence_preserved_count"`
	StartedAt                   time.Time `json:"started_at"`
	CompletedAt                 time.Time `json:"completed_at"`
}

// LegacyRecoveryResult provides detailed metrics on archived legacy records and preserved provider state.
type LegacyRecoveryResult struct {
	WorkspaceID                  string                    `json:"workspace_id"`
	ClusterID                    string                    `json:"cluster_id"`
	LegacyResourcesArchived      int                       `json:"legacy_resources_archived"`
	LegacyRelationshipsArchived  int                       `json:"legacy_relationships_archived"`
	LegacyProvenanceArchived     int                       `json:"legacy_provenance_archived"`
	LegacyDiagnosticsArchived    int                       `json:"legacy_diagnostics_archived"`
	EvidencePreservedCount       int                       `json:"evidence_preserved_count"`
	NonK8sResourcesPreserved     int                       `json:"non_k8s_resources_preserved"`
	NonK8sRelationshipsPreserved int                       `json:"non_k8s_relationships_preserved"`
	ArchivedResourcesCount      int                       `json:"archived_resources_count"`
	ArchivedRelationshipsCount  int                       `json:"archived_relationships_count"`
	RecollectedResourcesCount   int                       `json:"recollected_resources_count"`
	RecollectionResult           *AuthoritativeBatchResult `json:"recollection_result,omitempty"`
	CompletedAt                  time.Time                 `json:"completed_at"`
}

// ValidateRecoveryAuthorization checks that an administrative recovery operation is explicitly
// authorized by a trusted server-side principal (Admin or Owner role) or verified internal worker.
// Caller-supplied OperatorIdentity strings without trusted context authentication are strictly rejected.
func ValidateRecoveryAuthorization(ctx context.Context, params LegacyRecoveryParams) (string, error) {
	if !params.AuthorizeDestructive {
		return "", ErrUnauthorizedDestructiveAction
	}

	// 1. Authenticated user in context
	if user, ok := auth.UserFromContext(ctx); ok && user != nil {
		if user.Role != auth.RoleAdmin && user.Role != auth.RoleOwner {
			return "", fmt.Errorf("%w: user %q with role %q cannot perform destructive recovery", ErrForbiddenOperator, user.Email, user.Role)
		}
		op := strings.TrimSpace(user.Email)
		if op == "" {
			op = strings.TrimSpace(user.ID)
		}
		if op == "" {
			op = "authenticated-admin"
		}

		// Enforce server-side authority: reject caller attempts to spoof another operator identity
		paramOp := strings.TrimSpace(params.OperatorIdentity)
		if paramOp != "" && paramOp != op && paramOp != user.ID && paramOp != user.Email {
			return "", fmt.Errorf("%w: operator identity %q does not match authenticated user %q", ErrForbiddenOperator, paramOp, op)
		}
		return op, nil
	}

	// 2. Verified internal worker invocation
	if auth.IsWorkerCallFromContext(ctx) {
		op := "internal-worker"
		paramOp := strings.TrimSpace(params.OperatorIdentity)
		if paramOp != "" {
			op = paramOp
		}
		return op, nil
	}

	// 3. Reject unauthenticated destructive requests - caller-supplied identity without auth is rejected
	return "", fmt.Errorf("%w: recovery requires an authenticated operator context (admin, owner, or internal worker)", ErrUnauthenticatedOperator)
}
