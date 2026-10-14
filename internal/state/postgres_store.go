package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/aaradhychinche-alt/WhatBreaks/internal/database"
)

// PostgresStore implements Store backed by PostgreSQL.
type PostgresStore struct {
	db database.DB
}

// NewPostgresStore creates a PostgresStore backed by database.DB.
func NewPostgresStore(db database.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

// EnsureSchema creates the necessary state tables if they do not already exist.
func EnsureSchema(ctx context.Context, db database.DB) error {
	schema := `
	CREATE TABLE IF NOT EXISTS state_resources (
		workspace_id TEXT NOT NULL,
		provider TEXT NOT NULL,
		resource_type TEXT NOT NULL,
		provider_id TEXT NOT NULL,
		first_observed_at TIMESTAMPTZ NOT NULL,
		last_observed_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (workspace_id, provider, resource_type, provider_id)
	);

	CREATE TABLE IF NOT EXISTS state_evidence (
		workspace_id TEXT NOT NULL,
		id TEXT NOT NULL,
		source_provider TEXT NOT NULL,
		source_collector TEXT NOT NULL,
		observed_at TIMESTAMPTZ NOT NULL,
		observation_type TEXT NOT NULL,
		subject_provider TEXT NOT NULL,
		subject_resource_type TEXT NOT NULL,
		subject_provider_id TEXT NOT NULL,
		data BYTEA NOT NULL,
		PRIMARY KEY (workspace_id, id)
	);

	CREATE TABLE IF NOT EXISTS state_relationships (
		workspace_id TEXT NOT NULL,
		source_provider TEXT NOT NULL,
		source_resource_type TEXT NOT NULL,
		source_provider_id TEXT NOT NULL,
		target_provider TEXT NOT NULL,
		target_resource_type TEXT NOT NULL,
		target_provider_id TEXT NOT NULL,
		kind TEXT NOT NULL,
		category TEXT NOT NULL,
		first_observed_at TIMESTAMPTZ NOT NULL,
		last_observed_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (workspace_id, source_provider, source_resource_type, source_provider_id, target_provider, target_resource_type, target_provider_id, kind)
	);

	CREATE TABLE IF NOT EXISTS state_provenance (
		workspace_id TEXT NOT NULL,
		source_provider TEXT NOT NULL,
		source_resource_type TEXT NOT NULL,
		source_provider_id TEXT NOT NULL,
		target_provider TEXT NOT NULL,
		target_resource_type TEXT NOT NULL,
		target_provider_id TEXT NOT NULL,
		kind TEXT NOT NULL,
		evidence_id TEXT NOT NULL,
		PRIMARY KEY (workspace_id, source_provider, source_resource_type, source_provider_id, target_provider, target_resource_type, target_provider_id, kind, evidence_id)
	);

	CREATE TABLE IF NOT EXISTS state_conflicts (
		id BIGSERIAL PRIMARY KEY,
		workspace_id TEXT NOT NULL,
		description TEXT NOT NULL,
		observed_at TIMESTAMPTZ NOT NULL
	);

	CREATE TABLE IF NOT EXISTS state_workspace_authority (
		workspace_id TEXT NOT NULL,
		cluster_id TEXT NOT NULL,
		authoritative_run_id TEXT NOT NULL,
		authoritative_sampling_started_at TIMESTAMPTZ NOT NULL,
		last_generation BIGINT NOT NULL DEFAULT 0,
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		PRIMARY KEY (workspace_id, cluster_id)
	);

	CREATE TABLE IF NOT EXISTS state_reconciliation_audit (
		id BIGSERIAL PRIMARY KEY,
		workspace_id TEXT NOT NULL,
		cluster_id TEXT NOT NULL,
		run_id TEXT NOT NULL,
		sampling_started_at TIMESTAMPTZ NOT NULL,
		generation BIGINT NOT NULL,
		status TEXT NOT NULL,
		reason TEXT NOT NULL,
		observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS state_diagnostics (
		workspace_id TEXT NOT NULL,
		code TEXT NOT NULL,
		subject_provider TEXT NOT NULL DEFAULT '',
		subject_resource_type TEXT NOT NULL DEFAULT '',
		subject_provider_id TEXT NOT NULL DEFAULT '',
		referenced_provider TEXT NOT NULL DEFAULT '',
		referenced_resource_type TEXT NOT NULL DEFAULT '',
		referenced_provider_id TEXT NOT NULL DEFAULT '',
		unresolved_endpoint TEXT NOT NULL DEFAULT '',
		scope TEXT NOT NULL DEFAULT '',
		severity TEXT NOT NULL,
		message TEXT NOT NULL,
		relevance TEXT NOT NULL,
		completeness_impact TEXT NOT NULL,
		supporting_evidence_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
		first_observed_at TIMESTAMPTZ NOT NULL,
		last_observed_at TIMESTAMPTZ NOT NULL,
		resolved BOOLEAN NOT NULL DEFAULT FALSE,
		resolved_at TIMESTAMPTZ,
		PRIMARY KEY (workspace_id, code, subject_provider, subject_resource_type, subject_provider_id, referenced_provider, referenced_resource_type, referenced_provider_id, unresolved_endpoint, scope)
	);

	ALTER TABLE state_diagnostics ADD COLUMN IF NOT EXISTS resolved BOOLEAN NOT NULL DEFAULT FALSE;
	ALTER TABLE state_diagnostics ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ;

	CREATE TABLE IF NOT EXISTS state_scope_sweeps (
		workspace_id TEXT NOT NULL,
		cluster_id TEXT NOT NULL,
		run_id TEXT NOT NULL,
		sampling_started_at TIMESTAMPTZ NOT NULL,
		generation BIGINT NOT NULL DEFAULT 0,
		successful_scopes JSONB NOT NULL DEFAULT '[]'::jsonb,
		failed_scopes JSONB NOT NULL DEFAULT '[]'::jsonb,
		required_failures JSONB NOT NULL DEFAULT '[]'::jsonb,
		optional_failures JSONB NOT NULL DEFAULT '[]'::jsonb,
		scope_errors JSONB NOT NULL DEFAULT '[]'::jsonb,
		observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		PRIMARY KEY (workspace_id, cluster_id, run_id)
	);

	CREATE TABLE IF NOT EXISTS state_legacy_resources_archive (
		workspace_id TEXT NOT NULL,
		cluster_id TEXT NOT NULL,
		provider TEXT NOT NULL,
		resource_type TEXT NOT NULL,
		provider_id TEXT NOT NULL,
		first_observed_at TIMESTAMPTZ NOT NULL,
		last_observed_at TIMESTAMPTZ NOT NULL,
		archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		PRIMARY KEY (workspace_id, cluster_id, provider, resource_type, provider_id)
	);

	CREATE TABLE IF NOT EXISTS state_legacy_relationships_archive (
		workspace_id TEXT NOT NULL,
		cluster_id TEXT NOT NULL,
		source_provider TEXT NOT NULL,
		source_resource_type TEXT NOT NULL,
		source_provider_id TEXT NOT NULL,
		target_provider TEXT NOT NULL,
		target_resource_type TEXT NOT NULL,
		target_provider_id TEXT NOT NULL,
		kind TEXT NOT NULL,
		category TEXT NOT NULL,
		first_observed_at TIMESTAMPTZ NOT NULL,
		last_observed_at TIMESTAMPTZ NOT NULL,
		archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		PRIMARY KEY (workspace_id, cluster_id, source_provider, source_resource_type, source_provider_id, target_provider, target_resource_type, target_provider_id, kind)
	);

	CREATE TABLE IF NOT EXISTS state_legacy_provenance_archive (
		workspace_id TEXT NOT NULL,
		cluster_id TEXT NOT NULL,
		source_provider TEXT NOT NULL,
		source_resource_type TEXT NOT NULL,
		source_provider_id TEXT NOT NULL,
		target_provider TEXT NOT NULL,
		target_resource_type TEXT NOT NULL,
		target_provider_id TEXT NOT NULL,
		kind TEXT NOT NULL,
		evidence_id TEXT NOT NULL,
		archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		PRIMARY KEY (workspace_id, cluster_id, source_provider, source_resource_type, source_provider_id, target_provider, target_resource_type, target_provider_id, kind, evidence_id)
	);

	CREATE TABLE IF NOT EXISTS state_legacy_diagnostics_archive (
		workspace_id TEXT NOT NULL,
		cluster_id TEXT NOT NULL,
		code TEXT NOT NULL,
		subject_provider TEXT NOT NULL DEFAULT '',
		subject_resource_type TEXT NOT NULL DEFAULT '',
		subject_provider_id TEXT NOT NULL DEFAULT '',
		referenced_provider TEXT NOT NULL DEFAULT '',
		referenced_resource_type TEXT NOT NULL DEFAULT '',
		referenced_provider_id TEXT NOT NULL DEFAULT '',
		unresolved_endpoint TEXT NOT NULL DEFAULT '',
		scope TEXT NOT NULL DEFAULT '',
		severity TEXT NOT NULL,
		message TEXT NOT NULL,
		relevance TEXT NOT NULL,
		completeness_impact TEXT NOT NULL,
		supporting_evidence_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
		first_observed_at TIMESTAMPTZ NOT NULL,
		last_observed_at TIMESTAMPTZ NOT NULL,
		resolved BOOLEAN NOT NULL DEFAULT FALSE,
		resolved_at TIMESTAMPTZ,
		archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		PRIMARY KEY (workspace_id, cluster_id, code, subject_provider, subject_resource_type, subject_provider_id, referenced_provider, referenced_resource_type, referenced_provider_id, unresolved_endpoint, scope)
	);

	CREATE TABLE IF NOT EXISTS state_workspace_recovery (
		workspace_id TEXT PRIMARY KEY,
		operation_id TEXT NOT NULL,
		cluster_id TEXT NOT NULL,
		status TEXT NOT NULL,
		operator_identity TEXT NOT NULL,
		error_message TEXT NOT NULL DEFAULT '',
		started_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS state_recovery_audit (
		id BIGSERIAL PRIMARY KEY,
		operation_id TEXT NOT NULL DEFAULT '',
		workspace_id TEXT NOT NULL,
		cluster_id TEXT NOT NULL,
		operator_identity TEXT NOT NULL,
		reason TEXT NOT NULL,
		status TEXT NOT NULL,
		failure_category TEXT NOT NULL DEFAULT '',
		error_message TEXT NOT NULL DEFAULT '',
		resources_archived INT NOT NULL DEFAULT 0,
		relationships_archived INT NOT NULL DEFAULT 0,
		provenance_archived INT NOT NULL DEFAULT 0,
		diagnostics_archived INT NOT NULL DEFAULT 0,
		recollected_resources INT NOT NULL DEFAULT 0,
		evidence_preserved INT NOT NULL DEFAULT 0,
		started_at TIMESTAMPTZ NOT NULL,
		completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);
	CREATE INDEX IF NOT EXISTS idx_state_recovery_audit_ws ON state_recovery_audit(workspace_id);

	ALTER TABLE state_recovery_audit ADD COLUMN IF NOT EXISTS operation_id TEXT NOT NULL DEFAULT '';
	ALTER TABLE state_recovery_audit ADD COLUMN IF NOT EXISTS failure_category TEXT NOT NULL DEFAULT '';
	ALTER TABLE state_recovery_audit ADD COLUMN IF NOT EXISTS recollected_resources INT NOT NULL DEFAULT 0;
	`
	_, err := db.Exec(ctx, schema)
	return err
}

type staleRunError struct {
	reason string
	gen    int64
}

func (e *staleRunError) Error() string {
	return e.reason
}

func saveResourcesTx(ctx context.Context, tx database.Tx, ws string, resources []Resource) (int, int, error) {
	if len(resources) == 0 {
		return 0, 0, nil
	}

	query := `
	INSERT INTO state_resources (workspace_id, provider, resource_type, provider_id, first_observed_at, last_observed_at)
	VALUES ($1, $2, $3, $4, $5, $6)
	ON CONFLICT (workspace_id, provider, resource_type, provider_id)
	DO UPDATE SET last_observed_at = GREATEST(state_resources.last_observed_at, EXCLUDED.last_observed_at);
	`

	existingRows, err := tx.Query(ctx, `SELECT provider, resource_type, provider_id FROM state_resources WHERE workspace_id = $1`, ws)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to query existing resources: %w", err)
	}
	defer existingRows.Close()

	existing := make(map[string]bool)
	for existingRows.Next() {
		var p, rt, pid string
		if err := existingRows.Scan(&p, &rt, &pid); err == nil {
			existing[fmt.Sprintf("%s:%s:%s", p, rt, pid)] = true
		}
	}

	var created, updated int
	seen := make(map[string]bool)
	for _, r := range resources {
		k := r.Identity.String()
		if !seen[k] {
			seen[k] = true
			if existing[k] {
				updated++
			} else {
				created++
			}
		}
		_, err := tx.Exec(ctx, query, ws, r.Identity.Provider, r.Identity.ResourceType, r.Identity.ProviderID, r.FirstObservedAt, r.LastObservedAt)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to save resource %s: %w", r.Identity, err)
		}
	}
	return created, updated, nil
}

func saveEvidenceTx(ctx context.Context, tx database.Tx, ws string, evidence []Evidence) error {
	if len(evidence) == 0 {
		return nil
	}
	query := `
	INSERT INTO state_evidence (workspace_id, id, source_provider, source_collector, observed_at, observation_type, subject_provider, subject_resource_type, subject_provider_id, data)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	ON CONFLICT (workspace_id, id) DO NOTHING;
	`
	for _, ev := range evidence {
		_, err := tx.Exec(ctx, query, ws, ev.ID, ev.Source.Provider, ev.Source.Collector, ev.ObservedAt, ev.ObservationType, ev.Subject.Provider, ev.Subject.ResourceType, ev.Subject.ProviderID, ev.Data)
		if err != nil {
			return fmt.Errorf("failed to save evidence %s: %w", ev.ID, err)
		}
	}
	return nil
}

func saveRelationshipsTx(ctx context.Context, tx database.Tx, ws string, relationships []Relationship) (int, int, error) {
	if len(relationships) == 0 {
		return 0, 0, nil
	}

	query := `
	INSERT INTO state_relationships (workspace_id, source_provider, source_resource_type, source_provider_id, target_provider, target_resource_type, target_provider_id, kind, category, first_observed_at, last_observed_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	ON CONFLICT (workspace_id, source_provider, source_resource_type, source_provider_id, target_provider, target_resource_type, target_provider_id, kind)
	DO UPDATE SET last_observed_at = GREATEST(state_relationships.last_observed_at, EXCLUDED.last_observed_at);
	`

	existingRows, err := tx.Query(ctx, `SELECT source_provider, source_resource_type, source_provider_id, target_provider, target_resource_type, target_provider_id, kind FROM state_relationships WHERE workspace_id = $1`, ws)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to query existing relationships: %w", err)
	}
	defer existingRows.Close()

	existing := make(map[string]bool)
	for existingRows.Next() {
		var sp, srt, spid, tp, trt, tpid, k string
		if err := existingRows.Scan(&sp, &srt, &spid, &tp, &trt, &tpid, &k); err == nil {
			existing[fmt.Sprintf("%s:%s:%s-%s->%s:%s:%s", sp, srt, spid, k, tp, trt, tpid)] = true
		}
	}

	var created, updated int
	seen := make(map[string]bool)
	for _, rel := range relationships {
		relKey := fmt.Sprintf("%s:%s:%s-%s->%s:%s:%s",
			rel.Source.Provider, rel.Source.ResourceType, rel.Source.ProviderID, rel.Kind,
			rel.Target.Provider, rel.Target.ResourceType, rel.Target.ProviderID)
		if !seen[relKey] {
			seen[relKey] = true
			if existing[relKey] {
				updated++
			} else {
				created++
			}
		}
		_, err := tx.Exec(ctx, query, ws,
			rel.Source.Provider, rel.Source.ResourceType, rel.Source.ProviderID,
			rel.Target.Provider, rel.Target.ResourceType, rel.Target.ProviderID,
			rel.Kind, rel.Category, rel.FirstObservedAt, rel.LastObservedAt)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to save relationship %s: %w", rel.Key(), err)
		}
	}
	return created, updated, nil
}

func saveProvenanceTx(ctx context.Context, tx database.Tx, ws string, associations []ProvenanceAssociation) error {
	if len(associations) == 0 {
		return nil
	}
	query := `
	INSERT INTO state_provenance (workspace_id, source_provider, source_resource_type, source_provider_id, target_provider, target_resource_type, target_provider_id, kind, evidence_id)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	ON CONFLICT (workspace_id, source_provider, source_resource_type, source_provider_id, target_provider, target_resource_type, target_provider_id, kind, evidence_id) DO NOTHING;
	`
	for _, assoc := range associations {
		_, err := tx.Exec(ctx, query, ws,
			assoc.Relationship.Source.Provider, assoc.Relationship.Source.ResourceType, assoc.Relationship.Source.ProviderID,
			assoc.Relationship.Target.Provider, assoc.Relationship.Target.ResourceType, assoc.Relationship.Target.ProviderID,
			assoc.Relationship.Kind, assoc.EvidenceID)
		if err != nil {
			return fmt.Errorf("failed to save provenance association: %w", err)
		}
	}
	return nil
}

func saveConflictsTx(ctx context.Context, tx database.Tx, ws string, conflicts []DiscoveryConflict) error {
	if len(conflicts) == 0 {
		return nil
	}
	query := `INSERT INTO state_conflicts (workspace_id, description, observed_at) VALUES ($1, $2, $3)`
	for _, conf := range conflicts {
		_, err := tx.Exec(ctx, query, ws, conf.Description, conf.ObservedAt)
		if err != nil {
			return fmt.Errorf("failed to save conflict: %w", err)
		}
	}
	return nil
}

func saveDiagnosticsTx(ctx context.Context, tx database.Tx, ws string, diagnostics []DiagnosticItem) error {
	if len(diagnostics) == 0 {
		return nil
	}

	query := `
	INSERT INTO state_diagnostics (
		workspace_id, code,
		subject_provider, subject_resource_type, subject_provider_id,
		referenced_provider, referenced_resource_type, referenced_provider_id,
		unresolved_endpoint, scope,
		severity, message, relevance, completeness_impact,
		supporting_evidence_ids, first_observed_at, last_observed_at,
		resolved, resolved_at
	) VALUES (
		$1, $2,
		$3, $4, $5,
		$6, $7, $8,
		$9, $10,
		$11, $12, $13, $14,
		$15, $16, $17,
		FALSE, NULL
	)
	ON CONFLICT (workspace_id, code, subject_provider, subject_resource_type, subject_provider_id, referenced_provider, referenced_resource_type, referenced_provider_id, unresolved_endpoint, scope)
	DO UPDATE SET
		last_observed_at = GREATEST(state_diagnostics.last_observed_at, EXCLUDED.last_observed_at),
		severity = EXCLUDED.severity,
		message = EXCLUDED.message,
		relevance = EXCLUDED.relevance,
		completeness_impact = EXCLUDED.completeness_impact,
		supporting_evidence_ids = COALESCE((
			SELECT jsonb_agg(DISTINCT elem ORDER BY elem)
			FROM jsonb_array_elements_text(state_diagnostics.supporting_evidence_ids || EXCLUDED.supporting_evidence_ids) AS elem
		), '[]'::jsonb),
		resolved = FALSE,
		resolved_at = NULL;
	`

	for _, d := range diagnostics {
		subP, subRT, subPID := "", "", ""
		if d.Subject != nil {
			subP, subRT, subPID = d.Subject.Provider, d.Subject.ResourceType, d.Subject.ProviderID
		}
		refP, refRT, refPID := "", "", ""
		if d.ReferencedResource != nil {
			refP, refRT, refPID = d.ReferencedResource.Provider, d.ReferencedResource.ResourceType, d.ReferencedResource.ProviderID
		}

		evIDs := d.EvidenceIDs
		if evIDs == nil {
			evIDs = []string{}
		}
		evIDsJSON, err := json.Marshal(evIDs)
		if err != nil {
			evIDsJSON = []byte("[]")
		}

		firstAt := d.FirstObservedAt
		if firstAt.IsZero() {
			firstAt = time.Now().UTC()
		}
		lastAt := d.LastObservedAt
		if lastAt.IsZero() {
			lastAt = firstAt
		}

		_, err = tx.Exec(ctx, query,
			ws, d.Code,
			subP, subRT, subPID,
			refP, refRT, refPID,
			d.UnresolvedEndpoint, d.Scope,
			d.Severity, d.Message, d.Relevance, d.CompletenessImpact,
			evIDsJSON, firstAt, lastAt,
		)
		if err != nil {
			return fmt.Errorf("failed to save diagnostic %s: %w", d.NaturalKey(), err)
		}
	}
	return nil
}

func resolveDiagnosticsTx(ctx context.Context, tx database.Tx, ws string, resources []Resource, sweep *ScopeSweep) error {
	// T5: If the collection run has failed scopes, existing diagnostics are NOT resolved.
	if sweep != nil && len(sweep.FailedScopes) > 0 {
		return nil
	}

	// 1. Resolve UNRESOLVED_REFERENCE diagnostics for any directly observed resource
	for _, res := range resources {
		query := `
		UPDATE state_diagnostics
		SET resolved = TRUE, resolved_at = NOW()
		WHERE workspace_id = $1
		  AND code = 'UNRESOLVED_REFERENCE'
		  AND referenced_provider = $2
		  AND referenced_resource_type = $3
		  AND referenced_provider_id = $4
		  AND resolved = FALSE;
		`
		if _, err := tx.Exec(ctx, query, ws, res.Identity.Provider, res.Identity.ResourceType, res.Identity.ProviderID); err != nil {
			return fmt.Errorf("failed to resolve UNRESOLVED_REFERENCE for %s: %w", res.Identity, err)
		}
	}

	// 2. Resolve INCOMPLETE_SCOPE diagnostics for any successful scope
	if sweep != nil {
		for _, scope := range sweep.SuccessfulScopes {
			query := `
			UPDATE state_diagnostics
			SET resolved = TRUE, resolved_at = NOW()
			WHERE workspace_id = $1
			  AND code = 'INCOMPLETE_SCOPE'
			  AND scope = $2
			  AND resolved = FALSE;
			`
			if _, err := tx.Exec(ctx, query, ws, scope); err != nil {
				return fmt.Errorf("failed to resolve INCOMPLETE_SCOPE for %s: %w", scope, err)
			}
		}
	}

	return nil
}

func saveScopeSweepTx(ctx context.Context, tx database.Tx, ws string, sweep *ScopeSweep) error {
	if sweep == nil {
		return nil
	}
	succ := sweep.SuccessfulScopes
	if succ == nil {
		succ = []string{}
	}
	fail := sweep.FailedScopes
	if fail == nil {
		fail = []string{}
	}
	reqFail := sweep.RequiredFailures
	if reqFail == nil {
		reqFail = []string{}
	}
	optFail := sweep.OptionalFailures
	if optFail == nil {
		optFail = []string{}
	}
	errs := sweep.ScopeErrors
	if errs == nil {
		errs = map[string]string{}
	}

	succJSON, _ := json.Marshal(succ)
	failJSON, _ := json.Marshal(fail)
	reqFailJSON, _ := json.Marshal(reqFail)
	optFailJSON, _ := json.Marshal(optFail)
	errsJSON, _ := json.Marshal(errs)

	obsAt := sweep.ObservedAt
	if obsAt.IsZero() {
		obsAt = time.Now().UTC()
	}

	query := `
	INSERT INTO state_scope_sweeps (
		workspace_id, cluster_id, run_id, sampling_started_at, generation,
		successful_scopes, failed_scopes, required_failures, optional_failures, scope_errors, observed_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	ON CONFLICT (workspace_id, cluster_id, run_id)
	DO UPDATE SET
		generation = EXCLUDED.generation,
		successful_scopes = EXCLUDED.successful_scopes,
		failed_scopes = EXCLUDED.failed_scopes,
		required_failures = EXCLUDED.required_failures,
		optional_failures = EXCLUDED.optional_failures,
		scope_errors = EXCLUDED.scope_errors,
		observed_at = EXCLUDED.observed_at;
	`
	_, err := tx.Exec(ctx, query,
		ws, sweep.ClusterID, sweep.RunID, sweep.SamplingStartedAt, sweep.Generation,
		succJSON, failJSON, reqFailJSON, optFailJSON, errsJSON, obsAt,
	)
	return err
}

func (s *PostgresStore) SaveResources(ctx context.Context, workspaceID string, resources []Resource) error {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return ErrInvalidWorkspace
	}
	if len(resources) == 0 {
		return nil
	}

	return s.db.WithTransaction(ctx, func(ctx context.Context, tx database.Tx) error {
		_, _, err := saveResourcesTx(ctx, tx, ws, resources)
		return err
	})
}

func (s *PostgresStore) SaveEvidence(ctx context.Context, workspaceID string, evidence []Evidence) error {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return ErrInvalidWorkspace
	}
	if len(evidence) == 0 {
		return nil
	}

	return s.db.WithTransaction(ctx, func(ctx context.Context, tx database.Tx) error {
		return saveEvidenceTx(ctx, tx, ws, evidence)
	})
}

func (s *PostgresStore) SaveRelationships(ctx context.Context, workspaceID string, relationships []Relationship) error {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return ErrInvalidWorkspace
	}
	if len(relationships) == 0 {
		return nil
	}

	return s.db.WithTransaction(ctx, func(ctx context.Context, tx database.Tx) error {
		_, _, err := saveRelationshipsTx(ctx, tx, ws, relationships)
		return err
	})
}

func (s *PostgresStore) SaveProvenance(ctx context.Context, workspaceID string, associations []ProvenanceAssociation) error {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return ErrInvalidWorkspace
	}
	if len(associations) == 0 {
		return nil
	}

	return s.db.WithTransaction(ctx, func(ctx context.Context, tx database.Tx) error {
		return saveProvenanceTx(ctx, tx, ws, associations)
	})
}

func (s *PostgresStore) SaveConflict(ctx context.Context, conflict DiscoveryConflict) error {
	ws := strings.TrimSpace(conflict.WorkspaceID)
	if ws == "" {
		return ErrInvalidWorkspace
	}
	query := `INSERT INTO state_conflicts (workspace_id, description, observed_at) VALUES ($1, $2, $3)`
	_, err := s.db.Exec(ctx, query, ws, conflict.Description, conflict.ObservedAt)
	return err
}

func (s *PostgresStore) RecordStaleRunAudit(ctx context.Context, audit StaleRunAudit) error {
	ws := strings.TrimSpace(audit.WorkspaceID)
	if ws == "" {
		return ErrInvalidWorkspace
	}
	clusterID := strings.TrimSpace(audit.ClusterID)
	if clusterID == "" {
		clusterID = "default"
	}
	status := strings.TrimSpace(audit.Status)
	if status == "" {
		status = "STALE_IGNORED"
	}
	query := `
	INSERT INTO state_reconciliation_audit (workspace_id, cluster_id, run_id, sampling_started_at, generation, status, reason, observed_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
	`
	observedAt := audit.ObservedAt
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	_, err := s.db.Exec(ctx, query, ws, clusterID, audit.RunID, audit.SamplingStartedAt, audit.Generation, status, audit.Reason, observedAt)
	return err
}

func (s *PostgresStore) ListStaleRunAudits(ctx context.Context, workspaceID string) ([]StaleRunAudit, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}
	query := `
	SELECT id, workspace_id, cluster_id, run_id, sampling_started_at, generation, status, reason, observed_at
	FROM state_reconciliation_audit
	WHERE workspace_id = $1
	ORDER BY id ASC;
	`
	rows, err := s.db.Query(ctx, query, ws)
	if err != nil {
		return nil, fmt.Errorf("failed to list stale run audits: %w", err)
	}
	defer rows.Close()

	var audits []StaleRunAudit
	for rows.Next() {
		var a StaleRunAudit
		if err := rows.Scan(&a.ID, &a.WorkspaceID, &a.ClusterID, &a.RunID, &a.SamplingStartedAt, &a.Generation, &a.Status, &a.Reason, &a.ObservedAt); err != nil {
			return nil, fmt.Errorf("failed to scan stale run audit: %w", err)
		}
		audits = append(audits, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading stale run audit rows: %w", err)
	}
	if audits == nil {
		audits = []StaleRunAudit{}
	}
	return audits, nil
}

func (s *PostgresStore) GetWorkspaceAuthority(ctx context.Context, workspaceID string, clusterID string) (*WorkspaceAuthority, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}
	cid := strings.TrimSpace(clusterID)
	if cid == "" {
		cid = "default"
	}
	query := `
	SELECT workspace_id, cluster_id, authoritative_run_id, authoritative_sampling_started_at, last_generation, updated_at
	FROM state_workspace_authority
	WHERE workspace_id = $1 AND cluster_id = $2;
	`
	var a WorkspaceAuthority
	err := s.db.QueryRow(ctx, query, ws, cid).Scan(&a.WorkspaceID, &a.ClusterID, &a.AuthoritativeRunID, &a.AuthoritativeSamplingStartedAt, &a.LastGeneration, &a.UpdatedAt)
	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return nil, ErrAuthorityNotFound
		}
		return nil, err
	}
	return &a, nil
}

func (s *PostgresStore) ReconcileAuthoritativeBatch(ctx context.Context, params AuthoritativeBatchParams) (*AuthoritativeBatchResult, error) {
	var result *AuthoritativeBatchResult
	err := s.db.WithTransaction(ctx, func(ctx context.Context, tx database.Tx) error {
		var txErr error
		result, txErr = s.reconcileAuthoritativeBatchTx(ctx, tx, params)
		return txErr
	})

	if err != nil {
		var sErr *staleRunError
		if errors.As(err, &sErr) {
			ws := strings.TrimSpace(params.WorkspaceID)
			clusterID := strings.TrimSpace(params.ClusterID)
			if clusterID == "" {
				clusterID = "default"
			}
			samplingAt := params.SamplingStartedAt.UTC()
			if samplingAt.IsZero() {
				samplingAt = time.Now().UTC()
			}
			_ = s.RecordStaleRunAudit(ctx, StaleRunAudit{
				WorkspaceID:       ws,
				ClusterID:         clusterID,
				RunID:             params.RunID,
				SamplingStartedAt: samplingAt,
				Generation:        sErr.gen,
				Reason:            sErr.reason,
				ObservedAt:        time.Now().UTC(),
			})
			return result, fmt.Errorf("%w: %s", ErrStaleCollectionRunIgnored, sErr.reason)
		}
		return nil, err
	}

	return result, nil
}

func (s *PostgresStore) reconcileAuthoritativeBatchTx(ctx context.Context, tx database.Tx, params AuthoritativeBatchParams) (*AuthoritativeBatchResult, error) {
	ws := strings.TrimSpace(params.WorkspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}
	clusterID := strings.TrimSpace(params.ClusterID)
	if clusterID == "" {
		clusterID = "default"
	}
	samplingAt := params.SamplingStartedAt.UTC()
	if samplingAt.IsZero() {
		samplingAt = time.Now().UTC()
	}

	result := &AuthoritativeBatchResult{}

	// 0. Workspace-level advisory lock across application replicas
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1))", ws); err != nil {
		return nil, fmt.Errorf("failed to acquire reconciliation lock for workspace %s: %w", ws, err)
	}

	// Fail closed if workspace is in an incomplete or failed recovery state
	var recStatus, recErr string
	row := tx.QueryRow(ctx, "SELECT status, error_message FROM state_workspace_recovery WHERE workspace_id = $1", ws)
	if err := row.Scan(&recStatus, &recErr); err == nil {
		if recStatus == "FAILED" {
			return nil, fmt.Errorf("%w: workspace %q has a failed recovery that must be resolved before reconciliation: %s", ErrWorkspaceRecoveryRequired, ws, recErr)
		}
	}

	// 1. Race-safe authority row initialization
	initSQL := `
	INSERT INTO state_workspace_authority (
		workspace_id, cluster_id, authoritative_run_id, authoritative_sampling_started_at, last_generation, updated_at
	) VALUES ($1, $2, '', '1970-01-01 00:00:00Z', 0, NOW())
	ON CONFLICT (workspace_id, cluster_id) DO NOTHING;
	`
	if _, err := tx.Exec(ctx, initSQL, ws, clusterID); err != nil {
		return nil, fmt.Errorf("failed to initialize workspace authority: %w", err)
	}

	// 2. Lock authority row until tx commit/rollback
	lockSQL := `
	SELECT authoritative_run_id, authoritative_sampling_started_at, last_generation
	FROM state_workspace_authority
	WHERE workspace_id = $1 AND cluster_id = $2
	FOR UPDATE;
	`
	var currRunID string
	var currSampling time.Time
	var currGen int64
	if err := tx.QueryRow(ctx, lockSQL, ws, clusterID).Scan(&currRunID, &currSampling, &currGen); err != nil {
		return nil, fmt.Errorf("failed to lock workspace authority: %w", err)
	}
	currSampling = currSampling.UTC()

	result.CurrentSamplingAt = currSampling
	result.CurrentGeneration = currGen

	// 3. Stale run authority evaluation
	candidateGen := params.Generation
	if currGen > 0 {
		var isStale bool
		var staleReason string

		if samplingAt.Before(currSampling) {
			isStale = true
			staleReason = fmt.Sprintf("sampling timestamp %s is older than authoritative %s",
				samplingAt.Format(time.RFC3339Nano), currSampling.Format(time.RFC3339Nano))
		} else if samplingAt.Equal(currSampling) {
			if candidateGen <= currGen {
				isStale = true
				staleReason = fmt.Sprintf("equal sampling timestamp %s without strictly higher generation (%d <= %d)",
					samplingAt.Format(time.RFC3339Nano), candidateGen, currGen)
			}
		}

		if isStale {
			result.Stale = true
			result.AssignedGeneration = currGen
			return result, &staleRunError{reason: staleReason, gen: candidateGen}
		}
	}

	// 4. Authoritative mutations (Under the SAME single transaction)
	if err := saveEvidenceTx(ctx, tx, ws, params.Evidence); err != nil {
		return nil, err
	}
	createdRes, updatedRes, err := saveResourcesTx(ctx, tx, ws, params.Resources)
	if err != nil {
		return nil, err
	}
	result.ResourcesCreated = createdRes
	result.ResourcesUpdated = updatedRes

	createdRels, updatedRels, err := saveRelationshipsTx(ctx, tx, ws, params.Relationships)
	if err != nil {
		return nil, err
	}
	result.RelationshipsCreated = createdRels
	result.RelationshipsUpdated = updatedRels

	if err := saveProvenanceTx(ctx, tx, ws, params.Provenance); err != nil {
		return nil, err
	}
	result.ProvenanceLinksAdded = len(params.Provenance)

	if err := saveConflictsTx(ctx, tx, ws, params.Conflicts); err != nil {
		return nil, err
	}

	if err := saveDiagnosticsTx(ctx, tx, ws, params.Diagnostics); err != nil {
		return nil, err
	}

	if err := resolveDiagnosticsTx(ctx, tx, ws, params.Resources, params.ScopeSweep); err != nil {
		return nil, err
	}

	if err := saveScopeSweepTx(ctx, tx, ws, params.ScopeSweep); err != nil {
		return nil, err
	}

	// 5. Monotonically advance authority generation
	newGen := currGen + 1
	if candidateGen > newGen {
		newGen = candidateGen
	}
	result.AssignedGeneration = newGen

	updateAuthSQL := `
	UPDATE state_workspace_authority
	SET authoritative_run_id = $1,
	    authoritative_sampling_started_at = $2,
	    last_generation = $3,
	    updated_at = NOW()
	WHERE workspace_id = $4 AND cluster_id = $5;
	`
	if _, err := tx.Exec(ctx, updateAuthSQL, params.RunID, samplingAt, newGen, ws, clusterID); err != nil {
		return nil, fmt.Errorf("failed to update workspace authority: %w", err)
	}

	return result, nil
}

func (s *PostgresStore) GetResource(ctx context.Context, workspaceID string, identity ResourceIdentity) (*Resource, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}

	query := `
	SELECT provider, resource_type, provider_id, first_observed_at, last_observed_at
	FROM state_resources
	WHERE workspace_id = $1 AND provider = $2 AND resource_type = $3 AND provider_id = $4;
	`
	row := s.db.QueryRow(ctx, query, ws, identity.Provider, identity.ResourceType, identity.ProviderID)
	var r Resource
	r.WorkspaceID = ws
	err := row.Scan(&r.Identity.Provider, &r.Identity.ResourceType, &r.Identity.ProviderID, &r.FirstObservedAt, &r.LastObservedAt)
	if err != nil {
		return nil, ErrResourceNotFound
	}
	return &r, nil
}

func (s *PostgresStore) GetRelationship(ctx context.Context, workspaceID string, rel RelationshipKey) (*Relationship, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}

	query := `
	SELECT source_provider, source_resource_type, source_provider_id,
	       target_provider, target_resource_type, target_provider_id,
	       kind, category, first_observed_at, last_observed_at
	FROM state_relationships
	WHERE workspace_id = $1
	  AND source_provider = $2 AND source_resource_type = $3 AND source_provider_id = $4
	  AND target_provider = $5 AND target_resource_type = $6 AND target_provider_id = $7
	  AND kind = $8;
	`
	row := s.db.QueryRow(ctx, query, ws,
		rel.Source.Provider, rel.Source.ResourceType, rel.Source.ProviderID,
		rel.Target.Provider, rel.Target.ResourceType, rel.Target.ProviderID,
		rel.Kind)

	var r Relationship
	r.WorkspaceID = ws
	err := row.Scan(
		&r.Source.Provider, &r.Source.ResourceType, &r.Source.ProviderID,
		&r.Target.Provider, &r.Target.ResourceType, &r.Target.ProviderID,
		&r.Kind, &r.Category, &r.FirstObservedAt, &r.LastObservedAt,
	)
	if err != nil {
		return nil, ErrRelationshipNotFound
	}
	return &r, nil
}

func (s *PostgresStore) ListResources(ctx context.Context, workspaceID string) ([]Resource, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}

	query := `
	SELECT provider, resource_type, provider_id, first_observed_at, last_observed_at
	FROM state_resources
	WHERE workspace_id = $1
	ORDER BY provider, resource_type, provider_id;
	`
	rows, err := s.db.Query(ctx, query, ws)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Resource
	for rows.Next() {
		var r Resource
		r.WorkspaceID = ws
		if err := rows.Scan(&r.Identity.Provider, &r.Identity.ResourceType, &r.Identity.ProviderID, &r.FirstObservedAt, &r.LastObservedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *PostgresStore) ListRelationships(ctx context.Context, workspaceID string) ([]Relationship, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}

	query := `
	SELECT source_provider, source_resource_type, source_provider_id,
	       target_provider, target_resource_type, target_provider_id,
	       kind, category, first_observed_at, last_observed_at
	FROM state_relationships
	WHERE workspace_id = $1
	ORDER BY source_provider, source_resource_type, source_provider_id, target_provider, target_resource_type, target_provider_id, kind;
	`
	rows, err := s.db.Query(ctx, query, ws)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Relationship
	for rows.Next() {
		var rel Relationship
		rel.WorkspaceID = ws
		err := rows.Scan(
			&rel.Source.Provider, &rel.Source.ResourceType, &rel.Source.ProviderID,
			&rel.Target.Provider, &rel.Target.ResourceType, &rel.Target.ProviderID,
			&rel.Kind, &rel.Category, &rel.FirstObservedAt, &rel.LastObservedAt,
		)
		if err != nil {
			return nil, err
		}
		out = append(out, rel)
	}
	return out, rows.Err()
}

func (s *PostgresStore) ListEvidence(ctx context.Context, workspaceID string) ([]Evidence, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}

	query := `
	SELECT id, source_provider, source_collector, observed_at, observation_type,
	       subject_provider, subject_resource_type, subject_provider_id, data
	FROM state_evidence
	WHERE workspace_id = $1
	ORDER BY observed_at, id;
	`
	rows, err := s.db.Query(ctx, query, ws)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Evidence
	for rows.Next() {
		var ev Evidence
		ev.WorkspaceID = ws
		err := rows.Scan(
			&ev.ID, &ev.Source.Provider, &ev.Source.Collector, &ev.ObservedAt, &ev.ObservationType,
			&ev.Subject.Provider, &ev.Subject.ResourceType, &ev.Subject.ProviderID, &ev.Data,
		)
		if err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

func (s *PostgresStore) GetProvenanceForRelationship(ctx context.Context, workspaceID string, rel RelationshipKey) ([]string, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}

	query := `
	SELECT evidence_id
	FROM state_provenance
	WHERE workspace_id = $1
	  AND source_provider = $2 AND source_resource_type = $3 AND source_provider_id = $4
	  AND target_provider = $5 AND target_resource_type = $6 AND target_provider_id = $7
	  AND kind = $8
	ORDER BY evidence_id;
	`
	rows, err := s.db.Query(ctx, query, ws,
		rel.Source.Provider, rel.Source.ResourceType, rel.Source.ProviderID,
		rel.Target.Provider, rel.Target.ResourceType, rel.Target.ProviderID,
		rel.Kind)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *PostgresStore) GetRelationshipsForEvidence(ctx context.Context, workspaceID string, evidenceID string) ([]RelationshipKey, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}

	query := `
	SELECT source_provider, source_resource_type, source_provider_id,
	       target_provider, target_resource_type, target_provider_id, kind
	FROM state_provenance
	WHERE workspace_id = $1 AND evidence_id = $2
	ORDER BY source_provider, source_resource_type, source_provider_id, target_provider, target_resource_type, target_provider_id, kind;
	`
	rows, err := s.db.Query(ctx, query, ws, evidenceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []RelationshipKey
	for rows.Next() {
		var k RelationshipKey
		err := rows.Scan(
			&k.Source.Provider, &k.Source.ResourceType, &k.Source.ProviderID,
			&k.Target.Provider, &k.Target.ResourceType, &k.Target.ProviderID,
			&k.Kind,
		)
		if err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

func (s *PostgresStore) LoadAssembledState(ctx context.Context, workspaceID string) (*AssembledState, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}

	resources, err := s.ListResources(ctx, ws)
	if err != nil {
		return nil, err
	}

	relationships, err := s.ListRelationships(ctx, ws)
	if err != nil {
		return nil, err
	}

	evidence, err := s.ListEvidence(ctx, ws)
	if err != nil {
		return nil, err
	}

	// Query all provenance associations for the workspace
	provQuery := `
	SELECT source_provider, source_resource_type, source_provider_id,
	       target_provider, target_resource_type, target_provider_id, kind, evidence_id
	FROM state_provenance
	WHERE workspace_id = $1
	ORDER BY evidence_id;
	`
	rows, err := s.db.Query(ctx, provQuery, ws)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	prov := make(map[RelationshipKey][]string)
	for _, rel := range relationships {
		prov[rel.Key()] = []string{}
	}

	for rows.Next() {
		var k RelationshipKey
		var evID string
		err := rows.Scan(
			&k.Source.Provider, &k.Source.ResourceType, &k.Source.ProviderID,
			&k.Target.Provider, &k.Target.ResourceType, &k.Target.ProviderID,
			&k.Kind, &evID,
		)
		if err != nil {
			return nil, err
		}
		prov[k] = append(prov[k], evID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for k := range prov {
		sort.Strings(prov[k])
	}

	diagnostics, err := s.ListDiagnostics(ctx, ws)
	if err != nil {
		return nil, err
	}

	return &AssembledState{
		WorkspaceID:   ws,
		Resources:     resources,
		Relationships: relationships,
		Evidence:      evidence,
		Provenance:    prov,
		Diagnostics:   diagnostics,
	}, nil
}

func (s *PostgresStore) DeleteWorkspaceState(ctx context.Context, workspaceID string) error {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return ErrInvalidWorkspace
	}

	return s.db.WithTransaction(ctx, func(ctx context.Context, tx database.Tx) error {
		tables := []string{
			"state_conflicts", "state_provenance", "state_relationships",
			"state_evidence", "state_resources", "state_reconciliation_audit",
			"state_workspace_authority", "state_diagnostics", "state_scope_sweeps",
			"state_legacy_resources_archive", "state_legacy_relationships_archive",
			"state_legacy_provenance_archive", "state_legacy_diagnostics_archive",
			"state_recovery_audit",
		}
		for _, t := range tables {
			if _, err := tx.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE workspace_id = $1", t), ws); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *PostgresStore) ListDiagnostics(ctx context.Context, workspaceID string) ([]DiagnosticItem, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}

	query := `
	SELECT code, severity, message,
	       subject_provider, subject_resource_type, subject_provider_id,
	       referenced_provider, referenced_resource_type, referenced_provider_id,
	       unresolved_endpoint, scope, relevance, completeness_impact,
	       supporting_evidence_ids, first_observed_at, last_observed_at, resolved, resolved_at
	FROM state_diagnostics
	WHERE workspace_id = $1 AND resolved = FALSE
	ORDER BY code ASC, subject_provider ASC, subject_resource_type ASC, subject_provider_id ASC,
	         referenced_provider ASC, referenced_resource_type ASC, referenced_provider_id ASC,
	         unresolved_endpoint ASC, scope ASC;
	`
	rows, err := s.db.Query(ctx, query, ws)
	if err != nil {
		return nil, fmt.Errorf("failed to list diagnostics: %w", err)
	}
	defer rows.Close()

	var diagnostics []DiagnosticItem
	for rows.Next() {
		var d DiagnosticItem
		d.WorkspaceID = ws
		var subP, subRT, subPID, refP, refRT, refPID string
		var evIDsJSON []byte
		var resAt *time.Time
		if err := rows.Scan(
			&d.Code, &d.Severity, &d.Message,
			&subP, &subRT, &subPID,
			&refP, &refRT, &refPID,
			&d.UnresolvedEndpoint, &d.Scope, &d.Relevance, &d.CompletenessImpact,
			&evIDsJSON, &d.FirstObservedAt, &d.LastObservedAt, &d.Resolved, &resAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan diagnostic: %w", err)
		}
		d.ResolvedAt = resAt
		if subP != "" || subRT != "" || subPID != "" {
			d.Subject = &ResourceIdentity{Provider: subP, ResourceType: subRT, ProviderID: subPID}
		}
		if refP != "" || refRT != "" || refPID != "" {
			d.ReferencedResource = &ResourceIdentity{Provider: refP, ResourceType: refRT, ProviderID: refPID}
		}
		if len(evIDsJSON) > 0 {
			_ = json.Unmarshal(evIDsJSON, &d.EvidenceIDs)
		}
		if d.EvidenceIDs == nil {
			d.EvidenceIDs = []string{}
		}
		sort.Strings(d.EvidenceIDs)
		diagnostics = append(diagnostics, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading diagnostic rows: %w", err)
	}
	if diagnostics == nil {
		diagnostics = []DiagnosticItem{}
	}
	return diagnostics, nil
}

func (s *PostgresStore) SaveDiagnostics(ctx context.Context, workspaceID string, diagnostics []DiagnosticItem) error {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return ErrInvalidWorkspace
	}
	return s.db.WithTransaction(ctx, func(ctx context.Context, tx database.Tx) error {
		return saveDiagnosticsTx(ctx, tx, ws, diagnostics)
	})
}

func (s *PostgresStore) SaveScopeSweep(ctx context.Context, sweep ScopeSweep) error {
	ws := strings.TrimSpace(sweep.WorkspaceID)
	if ws == "" {
		return ErrInvalidWorkspace
	}
	return s.db.WithTransaction(ctx, func(ctx context.Context, tx database.Tx) error {
		return saveScopeSweepTx(ctx, tx, ws, &sweep)
	})
}

func (s *PostgresStore) GetScopeSweep(ctx context.Context, workspaceID string, clusterID string, runID string) (*ScopeSweep, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}
	cid := strings.TrimSpace(clusterID)
	if cid == "" {
		cid = "default"
	}
	query := `
	SELECT workspace_id, cluster_id, run_id, sampling_started_at, generation,
	       successful_scopes, failed_scopes, required_failures, optional_failures, scope_errors, observed_at
	FROM state_scope_sweeps
	WHERE workspace_id = $1 AND cluster_id = $2 AND run_id = $3;
	`
	var sweep ScopeSweep
	var succJSON, failJSON, reqJSON, optJSON, errsJSON []byte
	err := s.db.QueryRow(ctx, query, ws, cid, runID).Scan(
		&sweep.WorkspaceID, &sweep.ClusterID, &sweep.RunID, &sweep.SamplingStartedAt, &sweep.Generation,
		&succJSON, &failJSON, &reqJSON, &optJSON, &errsJSON, &sweep.ObservedAt,
	)
	if err != nil {
		return nil, err
	}
	_ = json.Unmarshal(succJSON, &sweep.SuccessfulScopes)
	_ = json.Unmarshal(failJSON, &sweep.FailedScopes)
	_ = json.Unmarshal(reqJSON, &sweep.RequiredFailures)
	_ = json.Unmarshal(optJSON, &sweep.OptionalFailures)
	_ = json.Unmarshal(errsJSON, &sweep.ScopeErrors)
	return &sweep, nil
}

func (s *PostgresStore) ListScopeSweeps(ctx context.Context, workspaceID string) ([]ScopeSweep, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}
	query := `
	SELECT workspace_id, cluster_id, run_id, sampling_started_at, generation,
	       successful_scopes, failed_scopes, required_failures, optional_failures, scope_errors, observed_at
	FROM state_scope_sweeps
	WHERE workspace_id = $1
	ORDER BY observed_at ASC;
	`
	rows, err := s.db.Query(ctx, query, ws)
	if err != nil {
		return nil, fmt.Errorf("failed to list scope sweeps: %w", err)
	}
	defer rows.Close()

	var sweeps []ScopeSweep
	for rows.Next() {
		var sweep ScopeSweep
		var succJSON, failJSON, reqJSON, optJSON, errsJSON []byte
		if err := rows.Scan(
			&sweep.WorkspaceID, &sweep.ClusterID, &sweep.RunID, &sweep.SamplingStartedAt, &sweep.Generation,
			&succJSON, &failJSON, &reqJSON, &optJSON, &errsJSON, &sweep.ObservedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan scope sweep: %w", err)
		}
		_ = json.Unmarshal(succJSON, &sweep.SuccessfulScopes)
		_ = json.Unmarshal(failJSON, &sweep.FailedScopes)
		_ = json.Unmarshal(reqJSON, &sweep.RequiredFailures)
		_ = json.Unmarshal(optJSON, &sweep.OptionalFailures)
		_ = json.Unmarshal(errsJSON, &sweep.ScopeErrors)
		sweeps = append(sweeps, sweep)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading scope sweep rows: %w", err)
	}
	if sweeps == nil {
		sweeps = []ScopeSweep{}
	}
	return sweeps, nil
}

// ListWorkspaces queries distinct workspace IDs persisted in state_resources, state_relationships,
// and state_evidence, returning them in deterministic ascending order.
func (s *PostgresStore) ListWorkspaces(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	query := `
	SELECT DISTINCT workspace_id FROM (
		SELECT workspace_id FROM state_resources
		UNION
		SELECT workspace_id FROM state_relationships
		UNION
		SELECT workspace_id FROM state_evidence
	) ws
	ORDER BY workspace_id ASC;
	`
	rows, err := s.db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list workspaces: %w", err)
	}
	defer rows.Close()

	var workspaces []string
	for rows.Next() {
		var ws string
		if err := rows.Scan(&ws); err != nil {
			return nil, fmt.Errorf("failed to scan workspace_id: %w", err)
		}
		workspaces = append(workspaces, ws)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating workspace rows: %w", err)
	}
	if workspaces == nil {
		return []string{}, nil
	}
	return workspaces, nil
}

// GetEvidence retrieves an evidence record by ID.
func (s *PostgresStore) GetEvidence(ctx context.Context, workspaceID, evidenceID string) (*Evidence, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}
	evID := strings.TrimSpace(evidenceID)
	if evID == "" {
		return nil, fmt.Errorf("empty evidence_id")
	}
	query := `
	SELECT id, source_provider, source_collector, observed_at, observation_type,
	       subject_provider, subject_resource_type, subject_provider_id, data
	FROM state_evidence
	WHERE workspace_id = $1 AND id = $2;
	`
	row := s.db.QueryRow(ctx, query, ws, evID)
	var ev Evidence
	ev.WorkspaceID = ws
	err := row.Scan(
		&ev.ID, &ev.Source.Provider, &ev.Source.Collector, &ev.ObservedAt, &ev.ObservationType,
		&ev.Subject.Provider, &ev.Subject.ResourceType, &ev.Subject.ProviderID, &ev.Data,
	)
	if err != nil {
		return nil, ErrResourceNotFound
	}
	return &ev, nil
}

// GetOutgoingRelationships returns active relationships originating from source.
func (s *PostgresStore) GetOutgoingRelationships(ctx context.Context, workspaceID string, source ResourceIdentity) ([]Relationship, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}
	query := `
	SELECT source_provider, source_resource_type, source_provider_id,
	       target_provider, target_resource_type, target_provider_id,
	       kind, category, first_observed_at, last_observed_at
	FROM state_relationships
	WHERE workspace_id = $1
	  AND source_provider = $2 AND source_resource_type = $3 AND source_provider_id = $4;
	`
	rows, err := s.db.Query(ctx, query, ws, source.Provider, source.ResourceType, source.ProviderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Relationship
	for rows.Next() {
		var r Relationship
		r.WorkspaceID = ws
		if err := rows.Scan(
			&r.Source.Provider, &r.Source.ResourceType, &r.Source.ProviderID,
			&r.Target.Provider, &r.Target.ResourceType, &r.Target.ProviderID,
			&r.Kind, &r.Category, &r.FirstObservedAt, &r.LastObservedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// InspectWorkspaceIntegrity inspects all identity-bearing state for workspaceID and returns a detailed report.
func (s *PostgresStore) InspectWorkspaceIntegrity(ctx context.Context, workspaceID string) (*WorkspaceIntegrityReport, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}

	rep := &WorkspaceIntegrityReport{
		WorkspaceID: ws,
		IsClean:     true,
	}

	// 1. Durable recovery state machine check
	var recStatus, recErr string
	row := s.db.QueryRow(ctx, "SELECT status, error_message FROM state_workspace_recovery WHERE workspace_id = $1", ws)
	if err := row.Scan(&recStatus, &recErr); err == nil {
		if recStatus == "IN_PROGRESS" {
			rep.IsClean = false
			rep.RecoveryRequired = true
			rep.IntegrityErrors = append(rep.IntegrityErrors, fmt.Sprintf("workspace %q recovery is currently in-progress or interrupted", ws))
		} else if recStatus == "FAILED" {
			rep.IsClean = false
			rep.RecoveryRequired = true
			rep.IntegrityErrors = append(rep.IntegrityErrors, fmt.Sprintf("workspace %q previous recovery failed: %s", ws, recErr))
		}
	}

	state, err := s.LoadAssembledState(ctx, ws)
	if err != nil {
		return nil, err
	}

	// 2. Inspect resources
	for _, r := range state.Resources {
		if IsKubernetesProvider(r.Identity.Provider) {
			if err := ValidatePersistedKubernetesIdentity(r.Identity); err != nil {
				rep.LegacyResourcesCount++
				rep.IsClean = false
				rep.IntegrityErrors = append(rep.IntegrityErrors, fmt.Sprintf("resource %s: %v", r.Identity, err))
			}
		}
	}

	// 3. Inspect relationships and cross-cluster links
	for _, rel := range state.Relationships {
		var srcCluster, tgtCluster string
		isLegacy := false
		if IsKubernetesProvider(rel.Source.Provider) {
			if err := ValidatePersistedKubernetesIdentity(rel.Source); err != nil {
				isLegacy = true
				rep.IntegrityErrors = append(rep.IntegrityErrors, fmt.Sprintf("relationship source %s: %v", rel.Source, err))
			} else {
				if cls, err := ClassifyKubernetesProviderID(rel.Source.ResourceType, rel.Source.ProviderID); err == nil {
					srcCluster = cls.Cluster
				}
			}
		}
		if IsKubernetesProvider(rel.Target.Provider) {
			if err := ValidatePersistedKubernetesIdentity(rel.Target); err != nil {
				isLegacy = true
				rep.IntegrityErrors = append(rep.IntegrityErrors, fmt.Sprintf("relationship target %s: %v", rel.Target, err))
			} else {
				if cls, err := ClassifyKubernetesProviderID(rel.Target.ResourceType, rel.Target.ProviderID); err == nil {
					tgtCluster = cls.Cluster
				}
			}
		}
		if isLegacy {
			rep.LegacyRelationshipsCount++
			rep.IsClean = false
		} else if srcCluster != "" && tgtCluster != "" && srcCluster != tgtCluster {
			rep.CrossClusterRelationsCount++
			rep.IsClean = false
			rep.IntegrityErrors = append(rep.IntegrityErrors, fmt.Sprintf("disallowed cross-cluster relationship from cluster %q to %q", srcCluster, tgtCluster))
		}
	}

	// 4. Inspect diagnostics
	for _, d := range state.Diagnostics {
		isLegacy := false
		if d.Subject != nil && IsKubernetesProvider(d.Subject.Provider) {
			if err := ValidatePersistedKubernetesIdentity(*d.Subject); err != nil {
				isLegacy = true
				rep.IntegrityErrors = append(rep.IntegrityErrors, fmt.Sprintf("diagnostic subject %s: %v", d.Subject, err))
			}
		}
		if d.ReferencedResource != nil && IsKubernetesProvider(d.ReferencedResource.Provider) {
			if err := ValidatePersistedKubernetesIdentity(*d.ReferencedResource); err != nil {
				isLegacy = true
				rep.IntegrityErrors = append(rep.IntegrityErrors, fmt.Sprintf("diagnostic referenced %s: %v", d.ReferencedResource, err))
			}
		}
		if isLegacy {
			rep.LegacyDiagnosticsCount++
			rep.IsClean = false
		}
	}

	// 5. Invariant 8: Check whether there are archived clusters awaiting canonical recollection
	var archivedCount int
	if err := s.db.QueryRow(ctx, "SELECT count(*) FROM state_legacy_resources_archive WHERE workspace_id = $1", ws).Scan(&archivedCount); err == nil && archivedCount > 0 {
		var activeCanonicalCount int
		if err := s.db.QueryRow(ctx, "SELECT count(*) FROM state_resources WHERE workspace_id = $1 AND provider = 'kubernetes'", ws).Scan(&activeCanonicalCount); err == nil && activeCanonicalCount == 0 {
			rep.IsClean = false
			rep.IntegrityErrors = append(rep.IntegrityErrors, fmt.Sprintf("workspace %q has archived legacy state awaiting canonical recollection", ws))
		}
	}

	return rep, nil
}

// CheckWorkspaceIntegrity inspects all identity-bearing state for workspaceID and reports whether it is safe for analysis.
func (s *PostgresStore) CheckWorkspaceIntegrity(ctx context.Context, workspaceID string) error {
	rep, err := s.InspectWorkspaceIntegrity(ctx, workspaceID)
	if err != nil {
		return err
	}
	if !rep.IsClean {
		if rep.RecoveryRequired {
			return fmt.Errorf("%w: %w: %s", ErrWorkspaceRecoveryRequired, ErrLegacyStateIncompatible, strings.Join(rep.IntegrityErrors, "; "))
		}
		if rep.CrossClusterRelationsCount > 0 && rep.LegacyResourcesCount == 0 && rep.LegacyRelationshipsCount == 0 && rep.LegacyDiagnosticsCount == 0 {
			return fmt.Errorf("%w: %s", ErrCrossClusterRelationship, strings.Join(rep.IntegrityErrors, "; "))
		}
		return fmt.Errorf("%w: %s", ErrLegacyStateIncompatible, strings.Join(rep.IntegrityErrors, "; "))
	}
	return nil
}

// RecoverWorkspaceLegacyState archives legacy Kubernetes state and safely restores canonical state via recollection.
func (s *PostgresStore) RecoverWorkspaceLegacyState(ctx context.Context, params LegacyRecoveryParams) (*LegacyRecoveryResult, error) {
	ws := strings.TrimSpace(params.WorkspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}

	startedAt := time.Now().UTC()
	opIdentity, authErr := ValidateRecoveryAuthorization(ctx, params)
	opID := fmt.Sprintf("rec-%d-%06d", startedAt.UnixNano(), time.Now().Nanosecond()%1000000)

	if authErr != nil {
		_ = s.recordRecoveryAudit(ctx, RecoveryAuditRecord{
			OperationID:      opID,
			WorkspaceID:      ws,
			ClusterID:        params.ClusterID,
			OperatorIdentity: params.OperatorIdentity,
			Reason:           params.Reason,
			Status:           "FAILED",
			FailureCategory:  "AUTHENTICATION_AUTHORIZATION",
			ErrorMessage:     authErr.Error(),
			StartedAt:        startedAt,
			CompletedAt:      time.Now().UTC(),
		})
		return nil, authErr
	}

	clusterID := strings.TrimSpace(params.ClusterID)
	if clusterID == "" && len(params.TargetClusterIDs) > 0 {
		clusterID = strings.TrimSpace(params.TargetClusterIDs[0])
	}
	if clusterID == "" {
		return nil, fmt.Errorf("recovery requires non-empty cluster_id")
	}
	if strings.Contains(clusterID, "/") {
		return nil, fmt.Errorf("%w: cluster_id %q cannot contain slashes", ErrMalformedProviderID, clusterID)
	}

	// Durable state machine: mark IN_PROGRESS immediately (detectable across processes/replicas)
	_, _ = s.db.Exec(ctx, `
		INSERT INTO state_workspace_recovery (workspace_id, operation_id, cluster_id, status, operator_identity, error_message, started_at, updated_at)
		VALUES ($1, $2, $3, 'IN_PROGRESS', $4, '', $5, NOW())
		ON CONFLICT (workspace_id)
		DO UPDATE SET operation_id = EXCLUDED.operation_id, cluster_id = EXCLUDED.cluster_id, status = 'IN_PROGRESS',
		              operator_identity = EXCLUDED.operator_identity, error_message = '', started_at = EXCLUDED.started_at, updated_at = NOW();
	`, ws, opID, clusterID, opIdentity, startedAt)

	result := &LegacyRecoveryResult{
		WorkspaceID: ws,
		ClusterID:   clusterID,
		CompletedAt: time.Now().UTC(),
	}

	err := s.db.WithTransaction(ctx, func(ctx context.Context, tx database.Tx) error {
		// 1. Transaction-scoped advisory lock for workspace
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1))", ws); err != nil {
			return fmt.Errorf("failed to acquire recovery lock for workspace %s: %w", ws, err)
		}

		// 2. Query and inspect resources
		resRows, err := tx.Query(ctx, `SELECT provider, resource_type, provider_id, first_observed_at, last_observed_at FROM state_resources WHERE workspace_id = $1`, ws)
		if err != nil {
			return fmt.Errorf("failed to inspect resources for recovery: %w", err)
		}
		defer resRows.Close()

		var legacyResources []Resource
		for resRows.Next() {
			var r Resource
			r.WorkspaceID = ws
			if err := resRows.Scan(&r.Identity.Provider, &r.Identity.ResourceType, &r.Identity.ProviderID, &r.FirstObservedAt, &r.LastObservedAt); err != nil {
				return err
			}
			if IsKubernetesProvider(r.Identity.Provider) {
				if err := ValidatePersistedKubernetesIdentity(r.Identity); err != nil {
					legacyResources = append(legacyResources, r)
				}
			} else {
				result.NonK8sResourcesPreserved++
			}
		}
		resRows.Close()

		// 3. Query and inspect relationships
		relRows, err := tx.Query(ctx, `SELECT source_provider, source_resource_type, source_provider_id, target_provider, target_resource_type, target_provider_id, kind, category, first_observed_at, last_observed_at FROM state_relationships WHERE workspace_id = $1`, ws)
		if err != nil {
			return fmt.Errorf("failed to inspect relationships for recovery: %w", err)
		}
		defer relRows.Close()

		var legacyRelationships []Relationship
		for relRows.Next() {
			var rel Relationship
			rel.WorkspaceID = ws
			if err := relRows.Scan(
				&rel.Source.Provider, &rel.Source.ResourceType, &rel.Source.ProviderID,
				&rel.Target.Provider, &rel.Target.ResourceType, &rel.Target.ProviderID,
				&rel.Kind, &rel.Category, &rel.FirstObservedAt, &rel.LastObservedAt,
			); err != nil {
				return err
			}
			isLegacy := false
			if IsKubernetesProvider(rel.Source.Provider) {
				if err := ValidatePersistedKubernetesIdentity(rel.Source); err != nil {
					isLegacy = true
				}
			}
			if IsKubernetesProvider(rel.Target.Provider) {
				if err := ValidatePersistedKubernetesIdentity(rel.Target); err != nil {
					isLegacy = true
				}
			}
			if isLegacy {
				legacyRelationships = append(legacyRelationships, rel)
			} else if !IsKubernetesProvider(rel.Source.Provider) && !IsKubernetesProvider(rel.Target.Provider) {
				result.NonK8sRelationshipsPreserved++
			}
		}
		relRows.Close()

		// 4. Query and inspect diagnostics
		diagRows, err := tx.Query(ctx, `
		SELECT code, subject_provider, subject_resource_type, subject_provider_id,
		       referenced_provider, referenced_resource_type, referenced_provider_id,
		       unresolved_endpoint, scope, severity, message, relevance, completeness_impact,
		       supporting_evidence_ids, first_observed_at, last_observed_at, resolved, resolved_at
		FROM state_diagnostics WHERE workspace_id = $1`, ws)
		if err != nil {
			return fmt.Errorf("failed to inspect diagnostics for recovery: %w", err)
		}
		defer diagRows.Close()

		var legacyDiagnostics []DiagnosticItem
		for diagRows.Next() {
			var d DiagnosticItem
			d.WorkspaceID = ws
			var subP, subRT, subPID, refP, refRT, refPID string
			var evIDsJSON []byte
			if err := diagRows.Scan(
				&d.Code, &subP, &subRT, &subPID, &refP, &refRT, &refPID,
				&d.UnresolvedEndpoint, &d.Scope, &d.Severity, &d.Message, &d.Relevance, &d.CompletenessImpact,
				&evIDsJSON, &d.FirstObservedAt, &d.LastObservedAt, &d.Resolved, &d.ResolvedAt,
			); err != nil {
				return err
			}
			if subP != "" {
				d.Subject = &ResourceIdentity{Provider: subP, ResourceType: subRT, ProviderID: subPID}
			}
			if refP != "" {
				d.ReferencedResource = &ResourceIdentity{Provider: refP, ResourceType: refRT, ProviderID: refPID}
			}
			_ = json.Unmarshal(evIDsJSON, &d.EvidenceIDs)

			isLegacy := false
			if d.Subject != nil && IsKubernetesProvider(d.Subject.Provider) {
				if err := ValidatePersistedKubernetesIdentity(*d.Subject); err != nil {
					isLegacy = true
				}
			}
			if d.ReferencedResource != nil && IsKubernetesProvider(d.ReferencedResource.Provider) {
				if err := ValidatePersistedKubernetesIdentity(*d.ReferencedResource); err != nil {
					isLegacy = true
				}
			}
			if isLegacy {
				legacyDiagnostics = append(legacyDiagnostics, d)
			}
		}
		diagRows.Close()

		// 5. Invariant 8: Check that if legacy state exists, an authoritative recollection batch is provided
		hasLegacy := len(legacyResources) > 0 || len(legacyRelationships) > 0 || len(legacyDiagnostics) > 0
		if hasLegacy && params.AuthoritativeBatch == nil {
			return fmt.Errorf("%w: legacy state cannot be archived without providing authoritative canonical batch for recollection", ErrLegacyStateIncompatible)
		}

		// 6. Verify evidence count (never deleted - Invariant C)
		var evCount int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM state_evidence WHERE workspace_id = $1`, ws).Scan(&evCount); err != nil {
			return err
		}
		result.EvidencePreservedCount = evCount

		// 7. Archive and remove legacy resources
		archiveResSQL := `
		INSERT INTO state_legacy_resources_archive (workspace_id, cluster_id, provider, resource_type, provider_id, first_observed_at, last_observed_at, archived_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
		ON CONFLICT (workspace_id, cluster_id, provider, resource_type, provider_id) DO NOTHING;
		`
		delResSQL := `DELETE FROM state_resources WHERE workspace_id = $1 AND provider = $2 AND resource_type = $3 AND provider_id = $4;`
		for _, r := range legacyResources {
			if _, err := tx.Exec(ctx, archiveResSQL, ws, clusterID, r.Identity.Provider, r.Identity.ResourceType, r.Identity.ProviderID, r.FirstObservedAt, r.LastObservedAt); err != nil {
				return fmt.Errorf("failed to archive legacy resource %s: %w", r.Identity, err)
			}
			if _, err := tx.Exec(ctx, delResSQL, ws, r.Identity.Provider, r.Identity.ResourceType, r.Identity.ProviderID); err != nil {
				return fmt.Errorf("failed to delete legacy resource %s: %w", r.Identity, err)
			}
			result.LegacyResourcesArchived++
		}

		// 8. Archive and remove legacy relationships and provenance
		archiveRelSQL := `
		INSERT INTO state_legacy_relationships_archive (workspace_id, cluster_id, source_provider, source_resource_type, source_provider_id, target_provider, target_resource_type, target_provider_id, kind, category, first_observed_at, last_observed_at, archived_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
		ON CONFLICT (workspace_id, cluster_id, source_provider, source_resource_type, source_provider_id, target_provider, target_resource_type, target_provider_id, kind) DO NOTHING;
		`
		delRelSQL := `
		DELETE FROM state_relationships
		WHERE workspace_id = $1
		  AND source_provider = $2 AND source_resource_type = $3 AND source_provider_id = $4
		  AND target_provider = $5 AND target_resource_type = $6 AND target_provider_id = $7
		  AND kind = $8;
		`
		archiveProvSQL := `
		INSERT INTO state_legacy_provenance_archive (workspace_id, cluster_id, source_provider, source_resource_type, source_provider_id, target_provider, target_resource_type, target_provider_id, kind, evidence_id, archived_at)
		SELECT workspace_id, $2, source_provider, source_resource_type, source_provider_id, target_provider, target_resource_type, target_provider_id, kind, evidence_id, NOW()
		FROM state_provenance
		WHERE workspace_id = $1
		  AND source_provider = $3 AND source_resource_type = $4 AND source_provider_id = $5
		  AND target_provider = $6 AND target_resource_type = $7 AND target_provider_id = $8
		  AND kind = $9
		ON CONFLICT DO NOTHING;
		`
		delProvSQL := `
		DELETE FROM state_provenance
		WHERE workspace_id = $1
		  AND source_provider = $2 AND source_resource_type = $3 AND source_provider_id = $4
		  AND target_provider = $5 AND target_resource_type = $6 AND target_provider_id = $7
		  AND kind = $8;
		`
		for _, rel := range legacyRelationships {
			if _, err := tx.Exec(ctx, archiveRelSQL, ws, clusterID,
				rel.Source.Provider, rel.Source.ResourceType, rel.Source.ProviderID,
				rel.Target.Provider, rel.Target.ResourceType, rel.Target.ProviderID,
				rel.Kind, rel.Category, rel.FirstObservedAt, rel.LastObservedAt,
			); err != nil {
				return fmt.Errorf("failed to archive legacy relationship: %w", err)
			}
			provRes, err := tx.Exec(ctx, archiveProvSQL, ws, clusterID,
				rel.Source.Provider, rel.Source.ResourceType, rel.Source.ProviderID,
				rel.Target.Provider, rel.Target.ResourceType, rel.Target.ProviderID,
				rel.Kind,
			)
			if err != nil {
				return fmt.Errorf("failed to archive legacy provenance: %w", err)
			}
			result.LegacyProvenanceArchived += int(provRes.RowsAffected())

			if _, err := tx.Exec(ctx, delProvSQL, ws,
				rel.Source.Provider, rel.Source.ResourceType, rel.Source.ProviderID,
				rel.Target.Provider, rel.Target.ResourceType, rel.Target.ProviderID,
				rel.Kind,
			); err != nil {
				return fmt.Errorf("failed to delete legacy provenance: %w", err)
			}

			if _, err := tx.Exec(ctx, delRelSQL, ws,
				rel.Source.Provider, rel.Source.ResourceType, rel.Source.ProviderID,
				rel.Target.Provider, rel.Target.ResourceType, rel.Target.ProviderID,
				rel.Kind,
			); err != nil {
				return fmt.Errorf("failed to delete legacy relationship: %w", err)
			}
			result.LegacyRelationshipsArchived++
		}

		// 9. Archive and remove legacy diagnostics
		archiveDiagSQL := `
		INSERT INTO state_legacy_diagnostics_archive (
			workspace_id, cluster_id, code,
			subject_provider, subject_resource_type, subject_provider_id,
			referenced_provider, referenced_resource_type, referenced_provider_id,
			unresolved_endpoint, scope, severity, message, relevance, completeness_impact,
			supporting_evidence_ids, first_observed_at, last_observed_at, resolved, resolved_at, archived_at
		) VALUES (
			$1, $2, $3,
			$4, $5, $6,
			$7, $8, $9,
			$10, $11, $12, $13, $14, $15,
			$16, $17, $18, $19, $20, NOW()
		)
		ON CONFLICT (workspace_id, cluster_id, code, subject_provider, subject_resource_type, subject_provider_id, referenced_provider, referenced_resource_type, referenced_provider_id, unresolved_endpoint, scope) DO NOTHING;
		`
		delDiagSQL := `
		DELETE FROM state_diagnostics
		WHERE workspace_id = $1 AND code = $2
		  AND subject_provider = $3 AND subject_resource_type = $4 AND subject_provider_id = $5
		  AND referenced_provider = $6 AND referenced_resource_type = $7 AND referenced_provider_id = $8
		  AND unresolved_endpoint = $9 AND scope = $10;
		`
		for _, d := range legacyDiagnostics {
			subP, subRT, subPID := "", "", ""
			if d.Subject != nil {
				subP, subRT, subPID = d.Subject.Provider, d.Subject.ResourceType, d.Subject.ProviderID
			}
			refP, refRT, refPID := "", "", ""
			if d.ReferencedResource != nil {
				refP, refRT, refPID = d.ReferencedResource.Provider, d.ReferencedResource.ResourceType, d.ReferencedResource.ProviderID
			}
			evJSON, _ := json.Marshal(d.EvidenceIDs)
			if _, err := tx.Exec(ctx, archiveDiagSQL, ws, clusterID, d.Code,
				subP, subRT, subPID, refP, refRT, refPID,
				d.UnresolvedEndpoint, d.Scope, d.Severity, d.Message, d.Relevance, d.CompletenessImpact,
				evJSON, d.FirstObservedAt, d.LastObservedAt, d.Resolved, d.ResolvedAt,
			); err != nil {
				return fmt.Errorf("failed to archive legacy diagnostic: %w", err)
			}
			if _, err := tx.Exec(ctx, delDiagSQL, ws, d.Code,
				subP, subRT, subPID, refP, refRT, refPID,
				d.UnresolvedEndpoint, d.Scope,
			); err != nil {
				return fmt.Errorf("failed to delete legacy diagnostic: %w", err)
			}
			result.LegacyDiagnosticsArchived++
		}

		// 10. If an authoritative recollection batch is provided, reconcile it inside this atomic transaction
		if params.AuthoritativeBatch != nil {
			bParams := *params.AuthoritativeBatch
			bParams.WorkspaceID = ws
			bParams.ClusterID = clusterID
			batchRes, err := s.reconcileAuthoritativeBatchTx(ctx, tx, bParams)
			if err != nil {
				return fmt.Errorf("recollection batch failed during recovery: %w", err)
			}
			result.RecollectionResult = batchRes
		}

		// 11. Final integrity validation on post-recovery state inside the transaction
		postState, err := loadAssembledStateTx(ctx, tx, ws)
		if err != nil {
			return fmt.Errorf("failed to load post-recovery assembled state: %w", err)
		}
		if err := ValidateAssembledStateIntegrity(postState); err != nil {
			return fmt.Errorf("post-recovery integrity validation failed: %w", err)
		}

		return nil
	})

	result.ArchivedResourcesCount = result.LegacyResourcesArchived
	result.ArchivedRelationshipsCount = result.LegacyRelationshipsArchived
	if result.RecollectionResult != nil {
		result.RecollectedResourcesCount = result.RecollectionResult.ResourcesCreated + result.RecollectionResult.ResourcesUpdated
	}

	if err != nil {
		// Durable recovery state machine: mark FAILED state across processes
		_, _ = s.db.Exec(context.Background(), `
			UPDATE state_workspace_recovery
			SET status = 'FAILED', error_message = $2, updated_at = NOW()
			WHERE workspace_id = $1;
		`, ws, err.Error())

		_ = s.recordRecoveryAudit(ctx, RecoveryAuditRecord{
			OperationID:      opID,
			WorkspaceID:      ws,
			ClusterID:        clusterID,
			OperatorIdentity: opIdentity,
			Reason:           params.Reason,
			Status:           "FAILED",
			FailureCategory:  "RECOVERY_EXECUTION",
			ErrorMessage:     err.Error(),
			StartedAt:        startedAt,
			CompletedAt:      time.Now().UTC(),
		})
		return nil, err
	}

	// Durable recovery state machine: mark COMPLETED
	_, _ = s.db.Exec(ctx, `
		UPDATE state_workspace_recovery
		SET status = 'COMPLETED', error_message = '', updated_at = NOW()
		WHERE workspace_id = $1;
	`, ws)

	_ = s.recordRecoveryAudit(ctx, RecoveryAuditRecord{
		OperationID:                 opID,
		WorkspaceID:                 ws,
		ClusterID:                   clusterID,
		OperatorIdentity:            opIdentity,
		Reason:                      params.Reason,
		Status:                      "SUCCESS",
		LegacyResourcesArchived:     result.LegacyResourcesArchived,
		LegacyRelationshipsArchived: result.LegacyRelationshipsArchived,
		LegacyProvenanceArchived:    result.LegacyProvenanceArchived,
		LegacyDiagnosticsArchived:   result.LegacyDiagnosticsArchived,
		RecollectedResourcesCount:   result.RecollectedResourcesCount,
		EvidencePreservedCount:      result.EvidencePreservedCount,
		StartedAt:                   startedAt,
		CompletedAt:                 time.Now().UTC(),
	})

	return result, nil
}

func (s *PostgresStore) recordRecoveryAudit(ctx context.Context, r RecoveryAuditRecord) error {
	query := `
	INSERT INTO state_recovery_audit (
		operation_id, workspace_id, cluster_id, operator_identity, reason, status, failure_category, error_message,
		resources_archived, relationships_archived, provenance_archived, diagnostics_archived, recollected_resources,
		evidence_preserved, started_at, completed_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16);
	`
	_, err := s.db.Exec(ctx, query,
		r.OperationID, r.WorkspaceID, r.ClusterID, r.OperatorIdentity, r.Reason, r.Status, r.FailureCategory, r.ErrorMessage,
		r.LegacyResourcesArchived, r.LegacyRelationshipsArchived, r.LegacyProvenanceArchived, r.LegacyDiagnosticsArchived,
		r.RecollectedResourcesCount, r.EvidencePreservedCount, r.StartedAt, r.CompletedAt,
	)
	return err
}

func (s *PostgresStore) ListRecoveryAudits(ctx context.Context, workspaceID string) ([]RecoveryAuditRecord, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}

	query := `
	SELECT id, operation_id, workspace_id, cluster_id, operator_identity, reason, status, failure_category, error_message,
	       resources_archived, relationships_archived, provenance_archived, diagnostics_archived, recollected_resources,
	       evidence_preserved, started_at, completed_at
	FROM state_recovery_audit
	WHERE workspace_id = $1
	ORDER BY id ASC;
	`
	rows, err := s.db.Query(ctx, query, ws)
	if err != nil {
		return nil, fmt.Errorf("failed to list recovery audits: %w", err)
	}
	defer rows.Close()

	var records []RecoveryAuditRecord
	for rows.Next() {
		var r RecoveryAuditRecord
		if err := rows.Scan(
			&r.ID, &r.OperationID, &r.WorkspaceID, &r.ClusterID, &r.OperatorIdentity, &r.Reason, &r.Status, &r.FailureCategory, &r.ErrorMessage,
			&r.LegacyResourcesArchived, &r.LegacyRelationshipsArchived, &r.LegacyProvenanceArchived, &r.LegacyDiagnosticsArchived,
			&r.RecollectedResourcesCount, &r.EvidencePreservedCount, &r.StartedAt, &r.CompletedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan recovery audit: %w", err)
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating recovery audits: %w", err)
	}
	if records == nil {
		return []RecoveryAuditRecord{}, nil
	}
	return records, nil
}

func loadAssembledStateTx(ctx context.Context, tx database.Tx, ws string) (*AssembledState, error) {
	// 1. Resources
	resRows, err := tx.Query(ctx, `SELECT provider, resource_type, provider_id, first_observed_at, last_observed_at FROM state_resources WHERE workspace_id = $1 ORDER BY provider, resource_type, provider_id`, ws)
	if err != nil {
		return nil, err
	}
	defer resRows.Close()

	var resources []Resource
	for resRows.Next() {
		var r Resource
		r.WorkspaceID = ws
		if err := resRows.Scan(&r.Identity.Provider, &r.Identity.ResourceType, &r.Identity.ProviderID, &r.FirstObservedAt, &r.LastObservedAt); err != nil {
			return nil, err
		}
		resources = append(resources, r)
	}
	if err := resRows.Err(); err != nil {
		return nil, err
	}

	// 2. Relationships
	relRows, err := tx.Query(ctx, `SELECT source_provider, source_resource_type, source_provider_id, target_provider, target_resource_type, target_provider_id, kind, category, first_observed_at, last_observed_at FROM state_relationships WHERE workspace_id = $1 ORDER BY source_provider, source_resource_type, source_provider_id, target_provider, target_resource_type, target_provider_id, kind`, ws)
	if err != nil {
		return nil, err
	}
	defer relRows.Close()

	var relationships []Relationship
	for relRows.Next() {
		var rel Relationship
		rel.WorkspaceID = ws
		if err := relRows.Scan(
			&rel.Source.Provider, &rel.Source.ResourceType, &rel.Source.ProviderID,
			&rel.Target.Provider, &rel.Target.ResourceType, &rel.Target.ProviderID,
			&rel.Kind, &rel.Category, &rel.FirstObservedAt, &rel.LastObservedAt,
		); err != nil {
			return nil, err
		}
		relationships = append(relationships, rel)
	}
	if err := relRows.Err(); err != nil {
		return nil, err
	}

	// 3. Evidence
	evRows, err := tx.Query(ctx, `SELECT id, source_provider, source_collector, observed_at, observation_type, subject_provider, subject_resource_type, subject_provider_id, data FROM state_evidence WHERE workspace_id = $1 ORDER BY observed_at, id`, ws)
	if err != nil {
		return nil, err
	}
	defer evRows.Close()

	var evidence []Evidence
	for evRows.Next() {
		var e Evidence
		e.WorkspaceID = ws
		if err := evRows.Scan(&e.ID, &e.Source.Provider, &e.Source.Collector, &e.ObservedAt, &e.ObservationType, &e.Subject.Provider, &e.Subject.ResourceType, &e.Subject.ProviderID, &e.Data); err != nil {
			return nil, err
		}
		evidence = append(evidence, e)
	}
	if err := evRows.Err(); err != nil {
		return nil, err
	}

	// 4. Provenance
	provRows, err := tx.Query(ctx, `SELECT source_provider, source_resource_type, source_provider_id, target_provider, target_resource_type, target_provider_id, kind, evidence_id FROM state_provenance WHERE workspace_id = $1 ORDER BY evidence_id`, ws)
	if err != nil {
		return nil, err
	}
	defer provRows.Close()

	prov := make(map[RelationshipKey][]string)
	for _, rel := range relationships {
		prov[rel.Key()] = []string{}
	}
	for provRows.Next() {
		var k RelationshipKey
		var evID string
		if err := provRows.Scan(&k.Source.Provider, &k.Source.ResourceType, &k.Source.ProviderID, &k.Target.Provider, &k.Target.ResourceType, &k.Target.ProviderID, &k.Kind, &evID); err != nil {
			return nil, err
		}
		prov[k] = append(prov[k], evID)
	}
	if err := provRows.Err(); err != nil {
		return nil, err
	}
	for k := range prov {
		sort.Strings(prov[k])
	}

	// 5. Diagnostics
	diagRows, err := tx.Query(ctx, `SELECT code, subject_provider, subject_resource_type, subject_provider_id, referenced_provider, referenced_resource_type, referenced_provider_id, unresolved_endpoint, scope, severity, message, relevance, completeness_impact, supporting_evidence_ids, first_observed_at, last_observed_at, resolved, resolved_at FROM state_diagnostics WHERE workspace_id = $1 ORDER BY code, subject_provider, subject_resource_type, subject_provider_id`, ws)
	if err != nil {
		return nil, err
	}
	defer diagRows.Close()

	var diagnostics []DiagnosticItem
	for diagRows.Next() {
		var d DiagnosticItem
		d.WorkspaceID = ws
		var subP, subRT, subPID, refP, refRT, refPID string
		var evIDsJSON []byte
		if err := diagRows.Scan(&d.Code, &subP, &subRT, &subPID, &refP, &refRT, &refPID, &d.UnresolvedEndpoint, &d.Scope, &d.Severity, &d.Message, &d.Relevance, &d.CompletenessImpact, &evIDsJSON, &d.FirstObservedAt, &d.LastObservedAt, &d.Resolved, &d.ResolvedAt); err != nil {
			return nil, err
		}
		if subP != "" {
			d.Subject = &ResourceIdentity{Provider: subP, ResourceType: subRT, ProviderID: subPID}
		}
		if refP != "" {
			d.ReferencedResource = &ResourceIdentity{Provider: refP, ResourceType: refRT, ProviderID: refPID}
		}
		_ = json.Unmarshal(evIDsJSON, &d.EvidenceIDs)
		diagnostics = append(diagnostics, d)
	}
	if err := diagRows.Err(); err != nil {
		return nil, err
	}

	return &AssembledState{
		WorkspaceID:   ws,
		Resources:     resources,
		Relationships: relationships,
		Evidence:      evidence,
		Provenance:    prov,
		Diagnostics:   diagnostics,
	}, nil
}
