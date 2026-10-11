package discovery

import (
	"context"
	"time"

	corev1 "github.com/aaradhychinche-alt/WhatBreaks/gen/go/wb/core/v1"
)

// Trigger represents the initiator of a discovery sweep.
type Trigger string

const (
	// TriggerManual indicates discovery initiated by an explicit user or API action.
	TriggerManual Trigger = "manual"

	// TriggerScheduled indicates discovery initiated by an automated recurring schedule.
	TriggerScheduled Trigger = "scheduled"
)

// Stage indicates the execution phase of a discovery sweep.
type Stage string

const (
	// StageNone indicates no execution has begun.
	StageNone Stage = "NONE"

	// StageReachability indicates the pre-sweep cluster connectivity probe.
	StageReachability Stage = "REACHABILITY"

	// StageCollection indicates querying infrastructure manifests from the cluster.
	StageCollection Stage = "COLLECTION"

	// StageReconciliation indicates synchronizing observations into PostgreSQL store.
	StageReconciliation Stage = "RECONCILIATION"

	// StageMaterialization indicates pushing graph state to the Rust Core Engine over gRPC.
	StageMaterialization Stage = "MATERIALIZATION"

	// StageCompleted indicates the full discovery lifecycle finished successfully.
	StageCompleted Stage = "COMPLETED"
)

// CollectionStatus represents the completeness state of a collection sweep.
type CollectionStatus string

const (
	// CollectionStatusComplete indicates all attempted and required scopes succeeded.
	CollectionStatusComplete CollectionStatus = "COMPLETE"

	// CollectionStatusPartial indicates all required scopes succeeded, but one or more optional scopes failed.
	CollectionStatusPartial CollectionStatus = "PARTIAL"

	// CollectionStatusFailed indicates one or more required scopes failed, or a fatal error halted collection.
	CollectionStatusFailed CollectionStatus = "FAILED"
)

// ScopeError records a failure encountered during observation collection for a specific resource scope.
type ScopeError struct {
	Scope    string `json:"scope"`
	Resource string `json:"resource"`
	Error    string `json:"error"`
	Required bool   `json:"required"`
}

// CollectionResult represents the structured outcome of an infrastructure collection sweep.
type CollectionResult struct {
	Evidence         []*corev1.Evidence `json:"evidence"`
	Status           CollectionStatus   `json:"status"`
	Complete         bool               `json:"complete"`
	ScopeErrors      []ScopeError       `json:"scope_errors,omitempty"`
	SuccessfulScopes []string           `json:"successful_scopes"`
	FailedScopes     []string           `json:"failed_scopes,omitempty"`
	RequiredFailures []string           `json:"required_failures,omitempty"`
	OptionalFailures []string           `json:"optional_failures,omitempty"`
}

// HasRequiredFailures reports whether any required discovery scope failed during collection.
func (r *CollectionResult) HasRequiredFailures() bool {
	return len(r.RequiredFailures) > 0
}

// SyncRequest encapsulates input parameters for orchestrating a discovery sweep.
// HTTP-independent: does not contain HTTP-specific request or response objects.
type SyncRequest struct {
	WorkspaceID   string  `json:"workspace_id"`
	ClusterID     string  `json:"cluster_id"` // Optional if single collector registered
	Trigger       Trigger `json:"trigger"`    // "manual" or "scheduled"
	CorrelationID string  `json:"correlation_id,omitempty"`
}

// Result encapsulates structured telemetry and outcome metrics of a discovery sweep.
type Result struct {
	WorkspaceID          string            `json:"workspace_id"`
	ClusterID            string            `json:"cluster_id"`
	Trigger              Trigger           `json:"trigger"`
	RunID                string            `json:"run_id"`
	Status               string            `json:"status"` // "COMPLETED", "PARTIAL", "FAILED"
	Stage                Stage             `json:"stage"`
	EvidenceCount        int               `json:"evidence_count"`
	ResourcesCreated     int               `json:"resources_created"`
	ResourcesUpdated     int               `json:"resources_updated"`
	ResourcesTotal       int               `json:"resources_total"`
	RelationshipsCreated int               `json:"relationships_created"`
	RelationshipsUpdated int               `json:"relationships_updated"`
	RelationshipsTotal   int               `json:"relationships_total"`
	ConflictsCount       int               `json:"conflicts_count"`
	Duration             time.Duration     `json:"duration"`
	DurationMs           int64             `json:"duration_ms"`
	DatabaseCommitted    bool              `json:"database_committed"`
	Collection           *CollectionResult `json:"collection,omitempty"`
	Error                error             `json:"error,omitempty"`
}

// ClusterCollector defines the operational contract required from an infrastructure collector.
type ClusterCollector interface {
	ClusterID() string
	WorkspaceID() string
	Ping(ctx context.Context) error
	Collect(ctx context.Context) ([]*corev1.Evidence, error)
	CollectDetailed(ctx context.Context) (*CollectionResult, error)
}

// Coordinator defines the contract for coordinating discovery across infrastructure clusters.
type Coordinator interface {
	Discover(ctx context.Context, req SyncRequest) (*Result, error)
	RegisterCollector(collector ClusterCollector) error
	GetCollector(clusterID string) (ClusterCollector, error)
	ListCollectors() []ClusterCollector
	Rematerialize(ctx context.Context, workspaceID string) error
}
