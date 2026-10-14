package state

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	corev1 "github.com/aaradhychinche-alt/WhatBreaks/gen/go/wb/core/v1"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/logging"
	"google.golang.org/grpc"
)

// CoreLoader defines the gRPC client contract required by Materializer.
type CoreLoader interface {
	LoadState(ctx context.Context, req *corev1.LoadStateRequest, opts ...grpc.CallOption) (*corev1.LoadStateResponse, error)
	InvalidateWorkspace(ctx context.Context, req *corev1.InvalidateWorkspaceRequest, opts ...grpc.CallOption) (*corev1.InvalidateWorkspaceResponse, error)
}

type workspaceLock struct {
	sem      chan struct{}
	refCount int
}

// Materializer loads authoritative state from Store and materializes it into the Rust Core Engine.
type Materializer struct {
	store      Store
	client     CoreLoader
	logger     logging.Logger
	registryMu sync.Mutex
	locks      map[string]*workspaceLock
	onLockWait func(workspaceID string)
}

// NewMaterializer constructs a Materializer instance.
func NewMaterializer(store Store, client CoreLoader, logger logging.Logger) *Materializer {
	if logger == nil {
		logger = logging.NewJSONLogger(nil, logging.LevelInfo, "state-materializer")
	}
	return &Materializer{
		store:  store,
		client: client,
		logger: logger,
		locks:  make(map[string]*workspaceLock),
	}
}

// SetOnLockWait registers a test hook invoked when acquireWorkspaceLock is about
// to wait on a semaphore currently held by another operation.
func (m *Materializer) SetOnLockWait(fn func(workspaceID string)) {
	m.registryMu.Lock()
	defer m.registryMu.Unlock()
	m.onLockWait = fn
}

// acquireWorkspaceLock serializes materializations for the same workspaceID while allowing
// different workspaces to materialize concurrently. It uses ref-counted channel semaphores
// to cleanly prune idle entries without memory leaks or unbounded map growth.
func (m *Materializer) acquireWorkspaceLock(ctx context.Context, ws string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.registryMu.Lock()
	if m.locks == nil {
		m.locks = make(map[string]*workspaceLock)
	}
	lock, ok := m.locks[ws]
	if !ok {
		lock = &workspaceLock{
			sem: make(chan struct{}, 1),
		}
		lock.sem <- struct{}{}
		m.locks[ws] = lock
	}
	lock.refCount++
	isWaiting := lock.refCount > 1
	waitHook := m.onLockWait
	m.registryMu.Unlock()

	if isWaiting && waitHook != nil {
		waitHook(ws)
	}

	select {
	case <-lock.sem:
		// Acquired workspace lock
	case <-ctx.Done():
		m.registryMu.Lock()
		lock.refCount--
		if lock.refCount == 0 {
			delete(m.locks, ws)
		}
		m.registryMu.Unlock()
		return nil, ctx.Err()
	}

	var once sync.Once
	unlock := func() {
		once.Do(func() {
			lock.sem <- struct{}{}
			m.registryMu.Lock()
			lock.refCount--
			if lock.refCount == 0 {
				delete(m.locks, ws)
			}
			m.registryMu.Unlock()
		})
	}

	return unlock, nil
}

// ActiveLockCount returns the count of active workspace lock entries in the registry.
// Useful for observability, monitoring, and test assertions.
func (m *Materializer) ActiveLockCount() int {
	m.registryMu.Lock()
	defer m.registryMu.Unlock()
	return len(m.locks)
}

// Materialize loads the full assembled state for workspaceID from the store and
// pushes it to the Rust Core Engine over gRPC. It acquires a workspace-scoped
// lock to prevent concurrent or out-of-order materializations for the same workspace.
func (m *Materializer) Materialize(ctx context.Context, workspaceID string) (*corev1.LoadStateResponse, error) {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return nil, ErrEmptyWorkspace
	}

	if m.client == nil {
		return nil, fmt.Errorf("materializer: core client is not configured")
	}

	unlock, err := m.acquireWorkspaceLock(ctx, ws)
	if err != nil {
		return nil, err
	}
	defer unlock()

	start := time.Now()
	state, err := m.store.LoadAssembledState(ctx, ws)
	if err != nil {
		m.logger.Error("Failed to load assembled state from store",
			"workspace_id", ws,
			"error", err.Error(),
		)
		return nil, fmt.Errorf("materializer failed to load state: %w", err)
	}

	if err := ValidateAssembledStateIntegrity(state); err != nil {
		m.logger.Error("State integrity validation failed: refusing to materialize unsafe legacy state",
			"workspace_id", ws,
			"error", err.Error(),
		)
		if m.client != nil {
			_, _ = m.client.InvalidateWorkspace(ctx, &corev1.InvalidateWorkspaceRequest{
				WorkspaceId: ws,
			})
		}
		return nil, err
	}

	protoReq := &corev1.LoadStateRequest{
		WorkspaceId:   ws,
		Relationships: make([]*corev1.Relationship, 0, len(state.Relationships)),
		Associations:  make([]*corev1.RelationshipEvidence, 0, len(state.Relationships)),
		Evidence:      make([]*corev1.Evidence, 0, len(state.Evidence)),
	}

	relCatMap := make(map[RelationshipKey]string, len(state.Relationships))
	// 1. Convert relationships
	for _, rel := range state.Relationships {
		relCatMap[rel.Key()] = rel.Category
		protoReq.Relationships = append(protoReq.Relationships, &corev1.Relationship{
			Source: &corev1.ResourceIdentity{
				Provider:     rel.Source.Provider,
				ResourceType: rel.Source.ResourceType,
				ProviderId:   rel.Source.ProviderID,
			},
			Target: &corev1.ResourceIdentity{
				Provider:     rel.Target.Provider,
				ResourceType: rel.Target.ResourceType,
				ProviderId:   rel.Target.ProviderID,
			},
			Kind:     rel.Kind,
			Category: rel.Category,
		})
	}

	// 2. Convert provenance associations
	for relKey, evIDs := range state.Provenance {
		protoReq.Associations = append(protoReq.Associations, &corev1.RelationshipEvidence{
			Relationship: &corev1.Relationship{
				Source: &corev1.ResourceIdentity{
					Provider:     relKey.Source.Provider,
					ResourceType: relKey.Source.ResourceType,
					ProviderId:   relKey.Source.ProviderID,
				},
				Target: &corev1.ResourceIdentity{
					Provider:     relKey.Target.Provider,
					ResourceType: relKey.Target.ResourceType,
					ProviderId:   relKey.Target.ProviderID,
				},
				Kind:     relKey.Kind,
				Category: relCatMap[relKey],
			},
			EvidenceIds: evIDs,
		})
	}

	// 3. Convert evidence
	for _, ev := range state.Evidence {
		protoReq.Evidence = append(protoReq.Evidence, &corev1.Evidence{
			Id: ev.ID,
			Source: &corev1.EvidenceSource{
				Provider:  ev.Source.Provider,
				Collector: ev.Source.Collector,
			},
			ObservedAt:      ev.ObservedAt.Format(time.RFC3339Nano),
			ObservationType: ev.ObservationType,
			Subject: &corev1.ResourceIdentity{
				Provider:     ev.Subject.Provider,
				ResourceType: ev.Subject.ResourceType,
				ProviderId:   ev.Subject.ProviderID,
			},
			Data: ev.Data,
		})
	}

	// 4. Convert directly observed resources
	for _, res := range state.Resources {
		protoReq.ObservedResources = append(protoReq.ObservedResources, &corev1.ResourceIdentity{
			Provider:     res.Identity.Provider,
			ResourceType: res.Identity.ResourceType,
			ProviderId:   res.Identity.ProviderID,
		})
	}

	// 5. Convert active diagnostics
	for _, diag := range state.Diagnostics {
		protoReq.Diagnostics = append(protoReq.Diagnostics, diag.ToProto())
	}

	resp, err := m.client.LoadState(ctx, protoReq)
	duration := time.Since(start)

	if err != nil {
		if m.client != nil {
			_, _ = m.client.InvalidateWorkspace(ctx, &corev1.InvalidateWorkspaceRequest{
				WorkspaceId: ws,
			})
		}
		m.logger.Error("Failed to materialize state to Core Engine",
			"workspace_id", ws,
			"duration_ms", duration.Milliseconds(),
			"error", err.Error(),
		)
		return nil, fmt.Errorf("materializer failed to load state into core: %w", err)
	}

	m.logger.Info("Materialized state to Core Engine successfully",
		"workspace_id", ws,
		"relationships_loaded", resp.RelationshipsLoaded,
		"evidence_loaded", resp.EvidenceLoaded,
		"provenance_links_loaded", resp.ProvenanceLinksLoaded,
		"duration_ms", duration.Milliseconds(),
	)

	return resp, nil
}

// InvalidateWorkspace sends an InvalidateWorkspaceRequest to the Core Engine to evict
// the in-memory graph for workspaceID.
func (m *Materializer) InvalidateWorkspace(ctx context.Context, workspaceID string) error {
	ws := strings.TrimSpace(workspaceID)
	if ws == "" {
		return ErrEmptyWorkspace
	}
	if m.client == nil {
		return nil
	}
	unlock, err := m.acquireWorkspaceLock(ctx, ws)
	if err != nil {
		return err
	}
	defer unlock()

	_, err = m.client.InvalidateWorkspace(ctx, &corev1.InvalidateWorkspaceRequest{
		WorkspaceId: ws,
	})
	if err != nil {
		m.logger.Error("Failed to invalidate workspace in Core Engine",
			"workspace_id", ws,
			"error", err.Error(),
		)
		return fmt.Errorf("failed to invalidate workspace in core: %w", err)
	}
	m.logger.Info("Invalidated workspace in Core Engine successfully", "workspace_id", ws)
	return nil
}
