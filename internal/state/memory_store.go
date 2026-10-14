package state

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// MemoryStore provides a thread-safe in-memory implementation of Store.
// Useful for fast unit tests, development, and disconnected runtime scenarios.
type MemoryStore struct {
	mu            sync.RWMutex
	resources     map[string]map[string]Resource
	evidence      map[string]map[string]Evidence
	relationships map[string]map[string]Relationship
	relToEvidence map[string]map[string][]string
	evToRel       map[string]map[string][]RelationshipKey
	conflicts     map[string][]DiscoveryConflict
	authorities   map[string]map[string]WorkspaceAuthority
	staleAudits   map[string][]StaleRunAudit
	diagnostics   map[string]map[string]DiagnosticItem
	scopeSweeps   map[string]map[string]ScopeSweep

	legacyResourcesArchive     map[string][]Resource
	legacyRelationshipsArchive map[string][]Relationship
	legacyProvenanceArchive    map[string][]ProvenanceAssociation
	legacyDiagnosticsArchive   map[string][]DiagnosticItem
	recoveryAudits             map[string][]RecoveryAuditRecord
	recoveryStatus             map[string]memoryRecoveryStatus
}

type memoryRecoveryStatus struct {
	WorkspaceID      string
	ClusterID        string
	OperationID      string
	Status           string // "IN_PROGRESS", "COMPLETED", "FAILED"
	OperatorIdentity string
	StartedAt        time.Time
	UpdatedAt        time.Time
	ErrorMessage     string
}

// NewMemoryStore initializes an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		resources:     make(map[string]map[string]Resource),
		evidence:      make(map[string]map[string]Evidence),
		relationships: make(map[string]map[string]Relationship),
		relToEvidence: make(map[string]map[string][]string),
		evToRel:       make(map[string]map[string][]RelationshipKey),
		conflicts:     make(map[string][]DiscoveryConflict),
		authorities:   make(map[string]map[string]WorkspaceAuthority),
		staleAudits:   make(map[string][]StaleRunAudit),
		diagnostics:   make(map[string]map[string]DiagnosticItem),
		scopeSweeps:   make(map[string]map[string]ScopeSweep),

		legacyResourcesArchive:     make(map[string][]Resource),
		legacyRelationshipsArchive: make(map[string][]Relationship),
		legacyProvenanceArchive:    make(map[string][]ProvenanceAssociation),
		legacyDiagnosticsArchive:   make(map[string][]DiagnosticItem),
		recoveryAudits:             make(map[string][]RecoveryAuditRecord),
		recoveryStatus:             make(map[string]memoryRecoveryStatus),
	}
}

func (s *MemoryStore) SaveResources(ctx context.Context, workspaceID string, resources []Resource) error {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return ErrInvalidWorkspace
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	wsMap, ok := s.resources[ws]
	if !ok {
		wsMap = make(map[string]Resource)
		s.resources[ws] = wsMap
	}

	for _, r := range resources {
		r.WorkspaceID = ws
		key := r.Identity.String()
		existing, found := wsMap[key]
		if !found {
			wsMap[key] = r
		} else {
			// Update last_observed_at without modifying first_observed_at
			if r.LastObservedAt.After(existing.LastObservedAt) {
				existing.LastObservedAt = r.LastObservedAt
			}
			wsMap[key] = existing
		}
	}
	return nil
}

func (s *MemoryStore) SaveEvidence(ctx context.Context, workspaceID string, evidence []Evidence) error {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return ErrInvalidWorkspace
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	wsMap, ok := s.evidence[ws]
	if !ok {
		wsMap = make(map[string]Evidence)
		s.evidence[ws] = wsMap
	}

	for _, ev := range evidence {
		ev.WorkspaceID = ws
		// Immutable: do not overwrite existing historical observation
		if _, exists := wsMap[ev.ID]; !exists {
			wsMap[ev.ID] = ev
		}
	}
	return nil
}

func (s *MemoryStore) SaveRelationships(ctx context.Context, workspaceID string, relationships []Relationship) error {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return ErrInvalidWorkspace
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	wsMap, ok := s.relationships[ws]
	if !ok {
		wsMap = make(map[string]Relationship)
		s.relationships[ws] = wsMap
	}

	for _, rel := range relationships {
		rel.WorkspaceID = ws
		key := rel.Key().String()
		existing, found := wsMap[key]
		if !found {
			wsMap[key] = rel
		} else {
			if rel.LastObservedAt.After(existing.LastObservedAt) {
				existing.LastObservedAt = rel.LastObservedAt
			}
			wsMap[key] = existing
		}
	}
	return nil
}

func (s *MemoryStore) SaveProvenance(ctx context.Context, workspaceID string, associations []ProvenanceAssociation) error {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return ErrInvalidWorkspace
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	relMap, ok := s.relToEvidence[ws]
	if !ok {
		relMap = make(map[string][]string)
		s.relToEvidence[ws] = relMap
	}

	evMap, ok := s.evToRel[ws]
	if !ok {
		evMap = make(map[string][]RelationshipKey)
		s.evToRel[ws] = evMap
	}

	for _, assoc := range associations {
		rKey := assoc.Relationship.String()
		evID := assoc.EvidenceID

		// Forward: rel -> evidence (idempotent)
		existingEvs := relMap[rKey]
		hasEv := false
		for _, e := range existingEvs {
			if e == evID {
				hasEv = true
				break
			}
		}
		if !hasEv {
			relMap[rKey] = append(existingEvs, evID)
		}

		// Reverse: evidence -> rel (idempotent)
		existingRels := evMap[evID]
		hasRel := false
		for _, r := range existingRels {
			if r.String() == rKey {
				hasRel = true
				break
			}
		}
		if !hasRel {
			evMap[evID] = append(existingRels, assoc.Relationship)
		}
	}
	return nil
}

func (s *MemoryStore) SaveConflict(ctx context.Context, conflict DiscoveryConflict) error {
	ws := strings.TrimSpace(conflict.WorkspaceID)
	if ws == "" {
		return ErrInvalidWorkspace
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	conflict.WorkspaceID = ws
	s.conflicts[ws] = append(s.conflicts[ws], conflict)
	return nil
}

func (s *MemoryStore) GetResource(ctx context.Context, workspaceID string, identity ResourceIdentity) (*Resource, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	wsMap, ok := s.resources[ws]
	if !ok {
		return nil, ErrResourceNotFound
	}

	res, found := wsMap[identity.String()]
	if !found {
		return nil, ErrResourceNotFound
	}
	return &res, nil
}

func (s *MemoryStore) GetRelationship(ctx context.Context, workspaceID string, rel RelationshipKey) (*Relationship, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	wsMap, ok := s.relationships[ws]
	if !ok {
		return nil, ErrRelationshipNotFound
	}

	r, found := wsMap[rel.String()]
	if !found {
		return nil, ErrRelationshipNotFound
	}
	return &r, nil
}

func (s *MemoryStore) ListResources(ctx context.Context, workspaceID string) ([]Resource, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	wsMap := s.resources[ws]
	out := make([]Resource, 0, len(wsMap))
	for _, r := range wsMap {
		out = append(out, r)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Identity.String() < out[j].Identity.String()
	})
	return out, nil
}

func (s *MemoryStore) ListRelationships(ctx context.Context, workspaceID string) ([]Relationship, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	wsMap := s.relationships[ws]
	out := make([]Relationship, 0, len(wsMap))
	for _, r := range wsMap {
		out = append(out, r)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Key().String() < out[j].Key().String()
	})
	return out, nil
}

func (s *MemoryStore) ListEvidence(ctx context.Context, workspaceID string) ([]Evidence, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	wsMap := s.evidence[ws]
	out := make([]Evidence, 0, len(wsMap))
	for _, e := range wsMap {
		out = append(out, e)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].ObservedAt.Equal(out[j].ObservedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].ObservedAt.Before(out[j].ObservedAt)
	})
	return out, nil
}

func (s *MemoryStore) GetProvenanceForRelationship(ctx context.Context, workspaceID string, rel RelationshipKey) ([]string, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	wsMap := s.relToEvidence[ws]
	if wsMap == nil {
		return []string{}, nil
	}

	evs := wsMap[rel.String()]
	out := make([]string, len(evs))
	copy(out, evs)
	sort.Strings(out)
	return out, nil
}

func (s *MemoryStore) GetRelationshipsForEvidence(ctx context.Context, workspaceID string, evidenceID string) ([]RelationshipKey, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	wsMap := s.evToRel[ws]
	if wsMap == nil {
		return []RelationshipKey{}, nil
	}

	rels := wsMap[evidenceID]
	out := make([]RelationshipKey, len(rels))
	copy(out, rels)
	sort.Slice(out, func(i, j int) bool {
		return out[i].String() < out[j].String()
	})
	return out, nil
}

func (s *MemoryStore) LoadAssembledState(ctx context.Context, workspaceID string) (*AssembledState, error) {
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

	s.mu.RLock()
	defer s.mu.RUnlock()

	prov := make(map[RelationshipKey][]string)
	wsProv := s.relToEvidence[ws]
	for _, rel := range relationships {
		k := rel.Key()
		evs := wsProv[k.String()]
		sortedEvs := make([]string, len(evs))
		copy(sortedEvs, evs)
		sort.Strings(sortedEvs)
		prov[k] = sortedEvs
	}

	var diagnostics []DiagnosticItem
	if diagMap, ok := s.diagnostics[ws]; ok {
		for _, d := range diagMap {
			if !d.Resolved {
				diagnostics = append(diagnostics, d)
			}
		}
		sort.Slice(diagnostics, func(i, j int) bool {
			return diagnostics[i].NaturalKey() < diagnostics[j].NaturalKey()
		})
	}
	if diagnostics == nil {
		diagnostics = []DiagnosticItem{}
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

func (s *MemoryStore) DeleteWorkspaceState(ctx context.Context, workspaceID string) error {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return ErrInvalidWorkspace
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.resources, ws)
	delete(s.evidence, ws)
	delete(s.relationships, ws)
	delete(s.relToEvidence, ws)
	delete(s.evToRel, ws)
	delete(s.conflicts, ws)
	delete(s.diagnostics, ws)
	delete(s.scopeSweeps, ws)
	delete(s.legacyResourcesArchive, ws)
	delete(s.legacyRelationshipsArchive, ws)
	delete(s.legacyProvenanceArchive, ws)
	delete(s.legacyDiagnosticsArchive, ws)
	delete(s.recoveryAudits, ws)

	return nil
}

func (s *MemoryStore) ListDiagnostics(ctx context.Context, workspaceID string) ([]DiagnosticItem, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	diagMap, ok := s.diagnostics[ws]
	if !ok {
		return []DiagnosticItem{}, nil
	}

	var out []DiagnosticItem
	for _, d := range diagMap {
		if !d.Resolved {
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].NaturalKey() < out[j].NaturalKey()
	})
	if out == nil {
		out = []DiagnosticItem{}
	}
	return out, nil
}

func (s *MemoryStore) SaveDiagnostics(ctx context.Context, workspaceID string, diagnostics []DiagnosticItem) error {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return ErrInvalidWorkspace
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	diagMap, ok := s.diagnostics[ws]
	if !ok {
		diagMap = make(map[string]DiagnosticItem)
		s.diagnostics[ws] = diagMap
	}

	for _, d := range diagnostics {
		d.WorkspaceID = ws
		k := d.NaturalKey()
		existing, found := diagMap[k]
		if !found {
			if d.FirstObservedAt.IsZero() {
				d.FirstObservedAt = time.Now().UTC()
			}
			if d.LastObservedAt.IsZero() {
				d.LastObservedAt = d.FirstObservedAt
			}
			if d.EvidenceIDs == nil {
				d.EvidenceIDs = []string{}
			}
			sort.Strings(d.EvidenceIDs)
			diagMap[k] = d
		} else {
			if d.LastObservedAt.After(existing.LastObservedAt) {
				existing.LastObservedAt = d.LastObservedAt
			}
			existing.Severity = d.Severity
			existing.Message = d.Message
			existing.Relevance = d.Relevance
			existing.CompletenessImpact = d.CompletenessImpact
			existing.Resolved = false
			existing.ResolvedAt = nil

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
			diagMap[k] = existing
		}
	}
	return nil
}

func (s *MemoryStore) SaveScopeSweep(ctx context.Context, sweep ScopeSweep) error {
	ws := strings.TrimSpace(sweep.WorkspaceID)
	if ws == "" {
		return ErrInvalidWorkspace
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	sweepMap, ok := s.scopeSweeps[ws]
	if !ok {
		sweepMap = make(map[string]ScopeSweep)
		s.scopeSweeps[ws] = sweepMap
	}
	if sweep.ObservedAt.IsZero() {
		sweep.ObservedAt = time.Now().UTC()
	}
	sweepMap[sweep.RunID] = sweep
	return nil
}

func (s *MemoryStore) GetScopeSweep(ctx context.Context, workspaceID string, clusterID string, runID string) (*ScopeSweep, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	sweepMap, ok := s.scopeSweeps[ws]
	if !ok {
		return nil, fmt.Errorf("scope sweep not found: %s", runID)
	}
	sweep, found := sweepMap[runID]
	if !found {
		return nil, fmt.Errorf("scope sweep not found: %s", runID)
	}
	return &sweep, nil
}

func (s *MemoryStore) ListScopeSweeps(ctx context.Context, workspaceID string) ([]ScopeSweep, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	sweepMap, ok := s.scopeSweeps[ws]
	if !ok {
		return []ScopeSweep{}, nil
	}

	var out []ScopeSweep
	for _, sw := range sweepMap {
		out = append(out, sw)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ObservedAt.Before(out[j].ObservedAt)
	})
	if out == nil {
		out = []ScopeSweep{}
	}
	return out, nil
}

// ListWorkspaces returns all distinct workspace IDs in memory, sorted ascending.
func (s *MemoryStore) ListWorkspaces(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	seen := make(map[string]struct{})
	for ws := range s.resources {
		seen[ws] = struct{}{}
	}
	for ws := range s.relationships {
		seen[ws] = struct{}{}
	}
	for ws := range s.evidence {
		seen[ws] = struct{}{}
	}

	workspaces := make([]string, 0, len(seen))
	for ws := range seen {
		workspaces = append(workspaces, ws)
	}
	sort.Strings(workspaces)
	return workspaces, nil
}

func (s *MemoryStore) GetWorkspaceAuthority(ctx context.Context, workspaceID string, clusterID string) (*WorkspaceAuthority, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}
	cid := strings.TrimSpace(clusterID)
	if cid == "" {
		cid = "default"
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	cMap, ok := s.authorities[ws]
	if !ok {
		return nil, ErrAuthorityNotFound
	}
	auth, ok := cMap[cid]
	if !ok {
		return nil, ErrAuthorityNotFound
	}
	return &auth, nil
}

func (s *MemoryStore) RecordStaleRunAudit(ctx context.Context, audit StaleRunAudit) error {
	ws := strings.TrimSpace(audit.WorkspaceID)
	if ws == "" {
		return ErrInvalidWorkspace
	}
	cid := strings.TrimSpace(audit.ClusterID)
	if cid == "" {
		cid = "default"
	}
	audit.WorkspaceID = ws
	audit.ClusterID = cid
	if audit.Status == "" {
		audit.Status = "STALE_IGNORED"
	}
	if audit.ObservedAt.IsZero() {
		audit.ObservedAt = time.Now().UTC()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	audit.ID = int64(len(s.staleAudits[ws]) + 1)
	s.staleAudits[ws] = append(s.staleAudits[ws], audit)
	return nil
}

func (s *MemoryStore) ListStaleRunAudits(ctx context.Context, workspaceID string) ([]StaleRunAudit, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	audits := s.staleAudits[ws]
	if audits == nil {
		return []StaleRunAudit{}, nil
	}
	res := make([]StaleRunAudit, len(audits))
	copy(res, audits)
	return res, nil
}

func (s *MemoryStore) ReconcileAuthoritativeBatch(ctx context.Context, params AuthoritativeBatchParams) (*AuthoritativeBatchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ws := strings.TrimSpace(params.WorkspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}
	params.isRecovery = false // public entry point strictly forces guard evaluation
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reconcileAuthoritativeBatchLocked(params)
}

func (s *MemoryStore) reconcileAuthoritativeBatchLocked(params AuthoritativeBatchParams) (*AuthoritativeBatchResult, error) {
	ws := strings.TrimSpace(params.WorkspaceID)
	clusterID := strings.TrimSpace(params.ClusterID)
	if clusterID == "" {
		clusterID = "default"
	}
	samplingAt := params.SamplingStartedAt.UTC()
	if samplingAt.IsZero() {
		samplingAt = time.Now().UTC()
	}

	// Fail closed if workspace is in an incomplete or failed recovery state
	if !params.isRecovery {
		if st, ok := s.recoveryStatus[ws]; ok {
			switch strings.TrimSpace(st.Status) {
			case "COMPLETED":
				// Allowed
			case "FAILED":
				return nil, fmt.Errorf("%w: workspace %q has a failed recovery (status %q) that must be resolved before reconciliation: %s", ErrWorkspaceRecoveryRequired, ws, st.Status, st.ErrorMessage)
			case "IN_PROGRESS":
				return nil, fmt.Errorf("%w: workspace %q recovery is currently in-progress or interrupted (status %q) and must be completed before reconciliation", ErrWorkspaceRecoveryRequired, ws, st.Status)
			case "":
				return nil, fmt.Errorf("%w: workspace %q has empty recovery status — manual intervention required", ErrWorkspaceRecoveryRequired, ws)
			default:
				return nil, fmt.Errorf("%w: workspace %q has unrecognized recovery status %q — manual intervention required", ErrWorkspaceRecoveryRequired, ws, st.Status)
			}
		}
	}

	// 1. Race-safe authority initialization
	cMap, ok := s.authorities[ws]
	if !ok {
		cMap = make(map[string]WorkspaceAuthority)
		s.authorities[ws] = cMap
	}
	currentAuth, hasAuth := cMap[clusterID]
	if !hasAuth {
		currentAuth = WorkspaceAuthority{
			WorkspaceID:                     ws,
			ClusterID:                       clusterID,
			AuthoritativeRunID:              "",
			AuthoritativeSamplingStartedAt: time.Unix(0, 0).UTC(),
			LastGeneration:                  0,
			UpdatedAt:                       time.Now().UTC(),
		}
	}

	currGen := currentAuth.LastGeneration
	currSampling := currentAuth.AuthoritativeSamplingStartedAt.UTC()

	result := &AuthoritativeBatchResult{
		CurrentSamplingAt: currSampling,
		CurrentGeneration: currGen,
	}

	candidateGen := params.Generation

	// 2. Stale run authority evaluation
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
			auditEntry := StaleRunAudit{
				ID:                int64(len(s.staleAudits[ws]) + 1),
				WorkspaceID:       ws,
				ClusterID:         clusterID,
				RunID:             params.RunID,
				SamplingStartedAt: samplingAt,
				Generation:        candidateGen,
				Status:            "STALE_IGNORED",
				Reason:            staleReason,
				ObservedAt:        time.Now().UTC(),
			}
			s.staleAudits[ws] = append(s.staleAudits[ws], auditEntry)
			return result, fmt.Errorf("%w: %s", ErrStaleCollectionRunIgnored, staleReason)
		}
	}

	// 3. Mutate domain state in memory
	// Evidence
	evMap, ok := s.evidence[ws]
	if !ok {
		evMap = make(map[string]Evidence)
		s.evidence[ws] = evMap
	}
	for _, ev := range params.Evidence {
		ev.WorkspaceID = ws
		if _, exists := evMap[ev.ID]; !exists {
			evMap[ev.ID] = ev
		}
	}

	// Resources
	resMap, ok := s.resources[ws]
	if !ok {
		resMap = make(map[string]Resource)
		s.resources[ws] = resMap
	}
	seenRes := make(map[string]bool)
	for _, r := range params.Resources {
		r.WorkspaceID = ws
		key := r.Identity.String()
		if !seenRes[key] {
			seenRes[key] = true
			existing, found := resMap[key]
			if !found {
				resMap[key] = r
				result.ResourcesCreated++
			} else {
				result.ResourcesUpdated++
				if r.LastObservedAt.After(existing.LastObservedAt) {
					existing.LastObservedAt = r.LastObservedAt
				}
				resMap[key] = existing
			}
		}
	}

	// Relationships
	relMap, ok := s.relationships[ws]
	if !ok {
		relMap = make(map[string]Relationship)
		s.relationships[ws] = relMap
	}
	seenRel := make(map[string]bool)
	for _, rel := range params.Relationships {
		rel.WorkspaceID = ws
		key := rel.Key().String()
		if !seenRel[key] {
			seenRel[key] = true
			existing, found := relMap[key]
			if !found {
				relMap[key] = rel
				result.RelationshipsCreated++
			} else {
				result.RelationshipsUpdated++
				if rel.LastObservedAt.After(existing.LastObservedAt) {
					existing.LastObservedAt = rel.LastObservedAt
				}
				relMap[key] = existing
			}
		}
	}

	// Provenance
	relToEv, ok := s.relToEvidence[ws]
	if !ok {
		relToEv = make(map[string][]string)
		s.relToEvidence[ws] = relToEv
	}
	evToR, ok := s.evToRel[ws]
	if !ok {
		evToR = make(map[string][]RelationshipKey)
		s.evToRel[ws] = evToR
	}
	for _, assoc := range params.Provenance {
		relKey := assoc.Relationship.String()
		existingEvs := relToEv[relKey]
		found := false
		for _, id := range existingEvs {
			if id == assoc.EvidenceID {
				found = true
				break
			}
		}
		if !found {
			relToEv[relKey] = append(relToEv[relKey], assoc.EvidenceID)
		}

		existingRels := evToR[assoc.EvidenceID]
		foundRel := false
		for _, k := range existingRels {
			if k == assoc.Relationship {
				foundRel = true
				break
			}
		}
		if !foundRel {
			evToR[assoc.EvidenceID] = append(evToR[assoc.EvidenceID], assoc.Relationship)
		}
	}
	result.ProvenanceLinksAdded = len(params.Provenance)

	// Conflicts
	for _, conf := range params.Conflicts {
		conf.WorkspaceID = ws
		s.conflicts[ws] = append(s.conflicts[ws], conf)
	}

	// Diagnostics
	diagMap, ok := s.diagnostics[ws]
	if !ok {
		diagMap = make(map[string]DiagnosticItem)
		s.diagnostics[ws] = diagMap
	}
	for _, d := range params.Diagnostics {
		d.WorkspaceID = ws
		natKey := d.NaturalKey()
		existing, found := diagMap[natKey]
		if !found {
			if d.FirstObservedAt.IsZero() {
				d.FirstObservedAt = time.Now().UTC()
			}
			if d.LastObservedAt.IsZero() {
				d.LastObservedAt = d.FirstObservedAt
			}
			if d.EvidenceIDs == nil {
				d.EvidenceIDs = []string{}
			}
			sort.Strings(d.EvidenceIDs)
			diagMap[natKey] = d
		} else {
			if d.LastObservedAt.After(existing.LastObservedAt) {
				existing.LastObservedAt = d.LastObservedAt
			}
			existing.Severity = d.Severity
			existing.Message = d.Message
			existing.Relevance = d.Relevance
			existing.CompletenessImpact = d.CompletenessImpact
			existing.Resolved = false
			existing.ResolvedAt = nil

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
			diagMap[natKey] = existing
		}
	}

	// Resolve Diagnostics (only when no failed scopes)
	if params.ScopeSweep == nil || len(params.ScopeSweep.FailedScopes) == 0 {
		now := time.Now().UTC()
		// 1. Resolve UNRESOLVED_REFERENCE for directly observed resources
		for _, res := range params.Resources {
			for k, diag := range diagMap {
				if diag.Code == "UNRESOLVED_REFERENCE" && diag.ReferencedResource != nil && *diag.ReferencedResource == res.Identity && !diag.Resolved {
					diag.Resolved = true
					diag.ResolvedAt = &now
					diagMap[k] = diag
				}
			}
		}
		// 2. Resolve INCOMPLETE_SCOPE for successful scopes
		if params.ScopeSweep != nil {
			for _, sc := range params.ScopeSweep.SuccessfulScopes {
				for k, diag := range diagMap {
					if diag.Code == "INCOMPLETE_SCOPE" && diag.Scope == sc && !diag.Resolved {
						diag.Resolved = true
						diag.ResolvedAt = &now
						diagMap[k] = diag
					}
				}
			}
		}
	}

	// Scope Sweep
	if params.ScopeSweep != nil {
		sweepMap, ok := s.scopeSweeps[ws]
		if !ok {
			sweepMap = make(map[string]ScopeSweep)
			s.scopeSweeps[ws] = sweepMap
		}
		sw := *params.ScopeSweep
		sw.WorkspaceID = ws
		sw.Generation = candidateGen
		if sw.ObservedAt.IsZero() {
			sw.ObservedAt = time.Now().UTC()
		}
		sweepMap[sw.RunID] = sw
	}

	// 4. Advance authority
	newGen := currGen + 1
	if candidateGen > newGen {
		newGen = candidateGen
	}
	result.AssignedGeneration = newGen

	cMap[clusterID] = WorkspaceAuthority{
		WorkspaceID:                     ws,
		ClusterID:                       clusterID,
		AuthoritativeRunID:              params.RunID,
		AuthoritativeSamplingStartedAt: samplingAt,
		LastGeneration:                  newGen,
		UpdatedAt:                       time.Now().UTC(),
	}

	return result, nil
}

func (s *MemoryStore) loadAssembledStateLocked(ws string) *AssembledState {
	var resources []Resource
	if resMap, ok := s.resources[ws]; ok {
		resources = make([]Resource, 0, len(resMap))
		for _, r := range resMap {
			resources = append(resources, r)
		}
		sort.Slice(resources, func(i, j int) bool {
			return resources[i].Identity.String() < resources[j].Identity.String()
		})
	}

	var relationships []Relationship
	if relMap, ok := s.relationships[ws]; ok {
		relationships = make([]Relationship, 0, len(relMap))
		for _, rel := range relMap {
			relationships = append(relationships, rel)
		}
		sort.Slice(relationships, func(i, j int) bool {
			return relationships[i].Key().String() < relationships[j].Key().String()
		})
	}

	var evidence []Evidence
	if evMap, ok := s.evidence[ws]; ok {
		evidence = make([]Evidence, 0, len(evMap))
		for _, ev := range evMap {
			evidence = append(evidence, ev)
		}
		sort.Slice(evidence, func(i, j int) bool {
			return evidence[i].ID < evidence[j].ID
		})
	}

	prov := make(map[RelationshipKey][]string)
	wsProv := s.relToEvidence[ws]
	for _, rel := range relationships {
		k := rel.Key()
		evs := wsProv[k.String()]
		sortedEvs := make([]string, len(evs))
		copy(sortedEvs, evs)
		sort.Strings(sortedEvs)
		prov[k] = sortedEvs
	}

	var diagnostics []DiagnosticItem
	if diagMap, ok := s.diagnostics[ws]; ok {
		for _, d := range diagMap {
			if !d.Resolved {
				diagnostics = append(diagnostics, d)
			}
		}
		sort.Slice(diagnostics, func(i, j int) bool {
			return diagnostics[i].NaturalKey() < diagnostics[j].NaturalKey()
		})
	}

	return &AssembledState{
		WorkspaceID:   ws,
		Resources:     resources,
		Relationships: relationships,
		Evidence:      evidence,
		Provenance:    prov,
		Diagnostics:   diagnostics,
	}
}

// GetEvidence retrieves a single evidence record by ID.
func (s *MemoryStore) GetEvidence(ctx context.Context, workspaceID string, evidenceID string) (*Evidence, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	if evMap, ok := s.evidence[ws]; ok {
		if ev, found := evMap[evidenceID]; found {
			evCopy := ev
			return &evCopy, nil
		}
	}
	return nil, ErrResourceNotFound
}

// GetOutgoingRelationships returns relationships originating from source.
func (s *MemoryStore) GetOutgoingRelationships(ctx context.Context, workspaceID string, source ResourceIdentity) ([]Relationship, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []Relationship
	if relMap, ok := s.relationships[ws]; ok {
		for _, rel := range relMap {
			if rel.Source == source {
				out = append(out, rel)
			}
		}
	}
	return out, nil
}

// InspectWorkspaceIntegrity inspects all identity-bearing state for workspaceID and returns a detailed report.
func (s *MemoryStore) InspectWorkspaceIntegrity(ctx context.Context, workspaceID string) (*WorkspaceIntegrityReport, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	rep := &WorkspaceIntegrityReport{
		WorkspaceID: ws,
		IsClean:     true,
	}

	// 1. Check durable recovery state machine: fail closed if interrupted or previously failed
	if st, ok := s.recoveryStatus[ws]; ok {
		switch strings.TrimSpace(st.Status) {
		case "COMPLETED":
			// Clean from recovery perspective
		case "IN_PROGRESS":
			rep.IsClean = false
			rep.RecoveryRequired = true
			rep.IntegrityErrors = append(rep.IntegrityErrors, fmt.Sprintf("workspace %q recovery is currently in-progress or interrupted", ws))
		case "FAILED":
			rep.IsClean = false
			rep.RecoveryRequired = true
			rep.IntegrityErrors = append(rep.IntegrityErrors, fmt.Sprintf("workspace %q previous recovery failed: %s", ws, st.ErrorMessage))
		case "":
			rep.IsClean = false
			rep.RecoveryRequired = true
			rep.IntegrityErrors = append(rep.IntegrityErrors, fmt.Sprintf("workspace %q has empty recovery status", ws))
		default:
			rep.IsClean = false
			rep.RecoveryRequired = true
			rep.IntegrityErrors = append(rep.IntegrityErrors, fmt.Sprintf("workspace %q has unrecognized recovery status %q", ws, st.Status))
		}
	}

	state := s.loadAssembledStateLocked(ws)

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
				srcCluster, _, _, _ = ParseKubernetesProviderID(rel.Source.ProviderID, IsClusterScoped(rel.Source.ResourceType))
			}
		}
		if IsKubernetesProvider(rel.Target.Provider) {
			if err := ValidatePersistedKubernetesIdentity(rel.Target); err != nil {
				isLegacy = true
				rep.IntegrityErrors = append(rep.IntegrityErrors, fmt.Sprintf("relationship target %s: %v", rel.Target, err))
			} else {
				tgtCluster, _, _, _ = ParseKubernetesProviderID(rel.Target.ProviderID, IsClusterScoped(rel.Target.ResourceType))
			}
		}
		if isLegacy {
			rep.LegacyRelationshipsCount++
			rep.IsClean = false
		}
		if srcCluster != "" && tgtCluster != "" && srcCluster != tgtCluster {
			rep.CrossClusterRelationsCount++
			rep.IsClean = false
			rep.IntegrityErrors = append(rep.IntegrityErrors, fmt.Sprintf("cross-cluster relationship: source cluster %q != target cluster %q", srcCluster, tgtCluster))
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
	if len(s.legacyResourcesArchive[ws]) > 0 {
		activeCanonical := 0
		if resMap, ok := s.resources[ws]; ok {
			for _, r := range resMap {
				if IsKubernetesProvider(r.Identity.Provider) {
					if err := ValidatePersistedKubernetesIdentity(r.Identity); err == nil {
						activeCanonical++
					}
				}
			}
		}
		if activeCanonical == 0 {
			rep.IsClean = false
			rep.IntegrityErrors = append(rep.IntegrityErrors, fmt.Sprintf("workspace %q has archived legacy state awaiting canonical recollection", ws))
		}
	}

	return rep, nil
}

// CheckWorkspaceIntegrity inspects all identity-bearing state for workspaceID and reports whether it is safe for analysis.
func (s *MemoryStore) CheckWorkspaceIntegrity(ctx context.Context, workspaceID string) error {
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
func (s *MemoryStore) RecoverWorkspaceLegacyState(ctx context.Context, params LegacyRecoveryParams) (*LegacyRecoveryResult, error) {
	ws := strings.TrimSpace(params.WorkspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}

	startedAt := time.Now().UTC()
	opID := fmt.Sprintf("rec-op-%d", time.Now().UnixNano())

	opIdentity, authErr := ValidateRecoveryAuthorization(ctx, params)
	if authErr != nil {
		s.mu.Lock()
		s.recoveryAudits[ws] = append(s.recoveryAudits[ws], RecoveryAuditRecord{
			ID:               int64(len(s.recoveryAudits[ws]) + 1),
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
		s.mu.Unlock()
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

	s.mu.Lock()
	defer s.mu.Unlock()

	// Enter durable in-progress state
	s.recoveryStatus[ws] = memoryRecoveryStatus{
		WorkspaceID:      ws,
		ClusterID:        clusterID,
		OperationID:      opID,
		Status:           "IN_PROGRESS",
		OperatorIdentity: opIdentity,
		StartedAt:        startedAt,
		UpdatedAt:        time.Now().UTC(),
	}

	result := &LegacyRecoveryResult{
		WorkspaceID: ws,
		ClusterID:   clusterID,
		CompletedAt: time.Now().UTC(),
	}

	// 1. Inspect resources
	resMap, hasRes := s.resources[ws]
	var legacyResources []Resource
	if hasRes {
		for _, r := range resMap {
			if IsKubernetesProvider(r.Identity.Provider) {
				if err := ValidatePersistedKubernetesIdentity(r.Identity); err != nil {
					legacyResources = append(legacyResources, r)
				}
			} else {
				result.NonK8sResourcesPreserved++
			}
		}
	}

	// 2. Inspect relationships
	relMap, hasRel := s.relationships[ws]
	var legacyRelationships []Relationship
	if hasRel {
		for _, rel := range relMap {
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
	}

	// 3. Inspect diagnostics
	diagMap, hasDiag := s.diagnostics[ws]
	var legacyDiagnostics []DiagnosticItem
	if hasDiag {
		for _, d := range diagMap {
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
	}

	// 4. Invariant 8: If legacy records exist, an authoritative canonical batch is required.
	hasLegacy := len(legacyResources) > 0 || len(legacyRelationships) > 0 || len(legacyDiagnostics) > 0
	if hasLegacy && params.AuthoritativeBatch == nil {
		err := fmt.Errorf("%w: legacy state cannot be archived without providing authoritative canonical batch for recollection", ErrLegacyStateIncompatible)
		s.recoveryStatus[ws] = memoryRecoveryStatus{
			WorkspaceID:      ws,
			ClusterID:        clusterID,
			OperationID:      opID,
			Status:           "FAILED",
			OperatorIdentity: opIdentity,
			StartedAt:        startedAt,
			UpdatedAt:        time.Now().UTC(),
			ErrorMessage:     err.Error(),
		}
		s.recoveryAudits[ws] = append(s.recoveryAudits[ws], RecoveryAuditRecord{
			ID:               int64(len(s.recoveryAudits[ws]) + 1),
			OperationID:      opID,
			WorkspaceID:      ws,
			ClusterID:        clusterID,
			OperatorIdentity: opIdentity,
			Reason:           params.Reason,
			Status:           "FAILED",
			ErrorMessage:     err.Error(),
			StartedAt:        startedAt,
			CompletedAt:      time.Now().UTC(),
		})
		return nil, err
	}

	// 5. Count immutable evidence (Invariant C)
	if evMap, ok := s.evidence[ws]; ok {
		result.EvidencePreservedCount = len(evMap)
	}

	// Create complete snapshot backups for safe rollback on failure
	backupRes := make(map[string]Resource)
	for k, v := range resMap {
		backupRes[k] = v
	}
	backupRel := make(map[string]Relationship)
	for k, v := range relMap {
		backupRel[k] = v
	}
	backupDiag := make(map[string]DiagnosticItem)
	for k, v := range diagMap {
		backupDiag[k] = v
	}
	backupRelToEv := make(map[string][]string)
	if r2e, ok := s.relToEvidence[ws]; ok {
		for k, v := range r2e {
			backupRelToEv[k] = append([]string(nil), v...)
		}
	}
	backupEvToRel := make(map[string][]RelationshipKey)
	if e2r, ok := s.evToRel[ws]; ok {
		for k, v := range e2r {
			backupEvToRel[k] = append([]RelationshipKey(nil), v...)
		}
	}
	backupArchivedRes := append([]Resource(nil), s.legacyResourcesArchive[ws]...)
	backupArchivedRel := append([]Relationship(nil), s.legacyRelationshipsArchive[ws]...)
	backupArchivedProv := append([]ProvenanceAssociation(nil), s.legacyProvenanceArchive[ws]...)
	backupArchivedDiag := append([]DiagnosticItem(nil), s.legacyDiagnosticsArchive[ws]...)

	recordFailure := func(err error) {
		s.resources[ws] = backupRes
		s.relationships[ws] = backupRel
		s.diagnostics[ws] = backupDiag
		s.relToEvidence[ws] = backupRelToEv
		s.evToRel[ws] = backupEvToRel
		s.legacyResourcesArchive[ws] = backupArchivedRes
		s.legacyRelationshipsArchive[ws] = backupArchivedRel
		s.legacyProvenanceArchive[ws] = backupArchivedProv
		s.legacyDiagnosticsArchive[ws] = backupArchivedDiag

		s.recoveryStatus[ws] = memoryRecoveryStatus{
			WorkspaceID:      ws,
			ClusterID:        clusterID,
			OperationID:      opID,
			Status:           "FAILED",
			OperatorIdentity: opIdentity,
			StartedAt:        startedAt,
			UpdatedAt:        time.Now().UTC(),
			ErrorMessage:     err.Error(),
		}
		s.recoveryAudits[ws] = append(s.recoveryAudits[ws], RecoveryAuditRecord{
			ID:               int64(len(s.recoveryAudits[ws]) + 1),
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
	}

	// 6. Archive and delete legacy resources
	for _, r := range legacyResources {
		s.legacyResourcesArchive[ws] = append(s.legacyResourcesArchive[ws], r)
		delete(s.resources[ws], r.Identity.String())
		result.LegacyResourcesArchived++
	}

	// 7. Archive and delete legacy relationships and provenance
	for _, rel := range legacyRelationships {
		s.legacyRelationshipsArchive[ws] = append(s.legacyRelationshipsArchive[ws], rel)
		relKey := rel.Key()
		relStr := relKey.String()
		if evIDs, ok := s.relToEvidence[ws][relStr]; ok {
			for _, id := range evIDs {
				s.legacyProvenanceArchive[ws] = append(s.legacyProvenanceArchive[ws], ProvenanceAssociation{
					WorkspaceID:  ws,
					Relationship: relKey,
					EvidenceID:   id,
				})
			}
			result.LegacyProvenanceArchived += len(evIDs)
			delete(s.relToEvidence[ws], relStr)
		}
		delete(s.relationships[ws], relStr)
		result.LegacyRelationshipsArchived++
	}

	// 8. Archive and delete legacy diagnostics
	for _, d := range legacyDiagnostics {
		s.legacyDiagnosticsArchive[ws] = append(s.legacyDiagnosticsArchive[ws], d)
		delete(s.diagnostics[ws], d.NaturalKey())
		result.LegacyDiagnosticsArchived++
	}

	// 9. If an authoritative batch is provided, reconcile it
	if params.AuthoritativeBatch != nil {
		bParams := withRecoveryBypass(*params.AuthoritativeBatch)
		bParams.WorkspaceID = ws
		bParams.ClusterID = clusterID
		batchRes, err := s.reconcileAuthoritativeBatchLocked(bParams)
		if err != nil {
			recordFailure(err)
			return nil, fmt.Errorf("recollection batch failed during recovery: %w", err)
		}
		result.RecollectionResult = batchRes
		result.RecollectedResourcesCount = len(bParams.Resources)
	}

	// 10. Final integrity validation on post-recovery state
	assembledState := s.loadAssembledStateLocked(ws)
	if err := ValidateAssembledStateIntegrity(assembledState); err != nil {
		recordFailure(err)
		return nil, fmt.Errorf("post-recovery integrity validation failed: %w", err)
	}

	// Update aliases
	result.ArchivedResourcesCount = result.LegacyResourcesArchived
	result.ArchivedRelationshipsCount = result.LegacyRelationshipsArchived

	// Transition to COMPLETED state
	s.recoveryStatus[ws] = memoryRecoveryStatus{
		WorkspaceID:      ws,
		ClusterID:        clusterID,
		OperationID:      opID,
		Status:           "COMPLETED",
		OperatorIdentity: opIdentity,
		StartedAt:        startedAt,
		UpdatedAt:        time.Now().UTC(),
	}

	// Record SUCCESS audit
	s.recoveryAudits[ws] = append(s.recoveryAudits[ws], RecoveryAuditRecord{
		ID:                          int64(len(s.recoveryAudits[ws]) + 1),
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

// ListRecoveryAudits retrieves historical recovery audit records for a workspace.
func (s *MemoryStore) ListRecoveryAudits(ctx context.Context, workspaceID string) ([]RecoveryAuditRecord, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrInvalidWorkspace
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	audits := s.recoveryAudits[ws]
	if len(audits) == 0 {
		return []RecoveryAuditRecord{}, nil
	}
	out := make([]RecoveryAuditRecord, len(audits))
	copy(out, audits)
	return out, nil
}

// SetRecoveryStatus sets the recovery status for a workspace (useful for simulating crash/recovery states in tests).
func (s *MemoryStore) SetRecoveryStatus(workspaceID, status, errorMessage string) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recoveryStatus[ws] = memoryRecoveryStatus{
		WorkspaceID:  ws,
		Status:       status,
		ErrorMessage: errorMessage,
		UpdatedAt:    time.Now().UTC(),
	}
}
