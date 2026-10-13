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

	return &AssembledState{
		WorkspaceID:   ws,
		Resources:     resources,
		Relationships: relationships,
		Evidence:      evidence,
		Provenance:    prov,
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

	return nil
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
	clusterID := strings.TrimSpace(params.ClusterID)
	if clusterID == "" {
		clusterID = "default"
	}
	samplingAt := params.SamplingStartedAt.UTC()
	if samplingAt.IsZero() {
		samplingAt = time.Now().UTC()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

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
