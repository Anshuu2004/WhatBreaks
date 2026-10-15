package state_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	corev1 "github.com/aaradhychinche-alt/WhatBreaks/gen/go/wb/core/v1"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/auth"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/state"
	"google.golang.org/grpc"
)

type mockInvalidationCoreLoader struct {
	mu              sync.Mutex
	loadStateCalls  int
	invalidateCalls int
	invalidateErr   error
}

func (m *mockInvalidationCoreLoader) LoadState(ctx context.Context, req *corev1.LoadStateRequest, opts ...grpc.CallOption) (*corev1.LoadStateResponse, error) {
	m.mu.Lock()
	m.loadStateCalls++
	m.mu.Unlock()
	return &corev1.LoadStateResponse{
		WorkspaceId: req.WorkspaceId,
	}, nil
}

func (m *mockInvalidationCoreLoader) InvalidateWorkspace(ctx context.Context, req *corev1.InvalidateWorkspaceRequest, opts ...grpc.CallOption) (*corev1.InvalidateWorkspaceResponse, error) {
	m.mu.Lock()
	m.invalidateCalls++
	err := m.invalidateErr
	m.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &corev1.InvalidateWorkspaceResponse{
		WorkspaceId: req.WorkspaceId,
		Invalidated: true,
	}, nil
}

func (m *mockInvalidationCoreLoader) getLoadStateCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.loadStateCalls
}

func (m *mockInvalidationCoreLoader) getInvalidateCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.invalidateCalls
}

type recoveryCountingStore struct {
	state.Store
	mu           sync.Mutex
	recoverCalls int
}

func (s *recoveryCountingStore) RecoverWorkspaceLegacyState(ctx context.Context, params state.LegacyRecoveryParams) (*state.LegacyRecoveryResult, error) {
	s.mu.Lock()
	s.recoverCalls++
	s.mu.Unlock()
	return s.Store.RecoverWorkspaceLegacyState(ctx, params)
}

func (s *recoveryCountingStore) getRecoverCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recoverCalls
}

// TestRecoverLegacyState_RefusesRecoveryOnInvalidationFailure verifies that RecoverLegacyState
// fails closed if Core Engine workspace invalidation fails, aborting before any destructive
// store recovery or subsequent rematerialization can take place.
func TestRecoverLegacyState_RefusesRecoveryOnInvalidationFailure(t *testing.T) {
	sentinelInvalidationErr := errors.New("simulated core engine rpc error: connection refused")
	coreMock := &mockInvalidationCoreLoader{
		invalidateErr: sentinelInvalidationErr,
	}

	rawStore := state.NewMemoryStore()
	store := &recoveryCountingStore{Store: rawStore}

	materializer := state.NewMaterializer(store, coreMock, nil)
	reconciler := state.NewReconciler(store, nil, materializer, nil)

	adminUser := &auth.User{
		ID:    "admin-user-test",
		Email: "admin@whatbreaks.internal",
		Role:  auth.RoleAdmin,
	}
	adminCtx := auth.WithUser(context.Background(), adminUser)

	wsID := "ws-invalidation-failure-test"
	params := state.LegacyRecoveryParams{
		WorkspaceID:          wsID,
		ClusterID:            "prod-cluster-1",
		OperatorIdentity:     adminUser.Email,
		AuthorizeDestructive: true,
		Reason:               "security audit recovery test",
	}

	result, err := reconciler.RecoverLegacyState(adminCtx, params)

	// 1. RecoverLegacyState() returns an error.
	if err == nil {
		t.Fatalf("expected RecoverLegacyState() to fail when invalidation fails, got nil error and result: %+v", result)
	}

	// 2. errors.Is(returnedErr, sentinelInvalidationErr) is true.
	if !errors.Is(err, sentinelInvalidationErr) {
		t.Errorf("expected returned error to wrap sentinelInvalidationErr (%v), got: %v", sentinelInvalidationErr, err)
	}

	// 3. The underlying store's RecoverWorkspaceLegacyState() call count remains zero.
	if calls := store.getRecoverCalls(); calls != 0 {
		t.Errorf("expected 0 calls to Store.RecoverWorkspaceLegacyState(), got %d", calls)
	}

	// 4. The Core Engine LoadState() call count remains zero.
	if calls := coreMock.getLoadStateCalls(); calls != 0 {
		t.Errorf("expected 0 calls to CoreLoader.LoadState(), got %d", calls)
	}

	// Ensure InvalidateWorkspace was indeed called once.
	if calls := coreMock.getInvalidateCalls(); calls != 1 {
		t.Errorf("expected exactly 1 call to CoreLoader.InvalidateWorkspace(), got %d", calls)
	}
}

// TestRecoverLegacyState_AllowsRecoveryOnSuccessfulInvalidation verifies that when invalidation
// succeeds, recovery proceeds to execute store recovery and post-recovery rematerialization.
func TestRecoverLegacyState_AllowsRecoveryOnSuccessfulInvalidation(t *testing.T) {
	coreMock := &mockInvalidationCoreLoader{}

	rawStore := state.NewMemoryStore()
	store := &recoveryCountingStore{Store: rawStore}

	materializer := state.NewMaterializer(store, coreMock, nil)
	reconciler := state.NewReconciler(store, nil, materializer, nil)

	adminUser := &auth.User{
		ID:    "admin-user-test",
		Email: "admin@whatbreaks.internal",
		Role:  auth.RoleAdmin,
	}
	adminCtx := auth.WithUser(context.Background(), adminUser)

	wsID := "ws-invalidation-success-test"
	params := state.LegacyRecoveryParams{
		WorkspaceID:          wsID,
		ClusterID:            "prod-cluster-1",
		OperatorIdentity:     adminUser.Email,
		AuthorizeDestructive: true,
		Reason:               "clean recovery test",
	}

	result, err := reconciler.RecoverLegacyState(adminCtx, params)
	if err != nil {
		t.Fatalf("expected RecoverLegacyState() to succeed, got error: %v", err)
	}
	if result == nil {
		t.Fatalf("expected non-nil LegacyRecoveryResult")
	}

	if calls := store.getRecoverCalls(); calls != 1 {
		t.Errorf("expected 1 call to Store.RecoverWorkspaceLegacyState(), got %d", calls)
	}
	if calls := coreMock.getInvalidateCalls(); calls != 1 {
		t.Errorf("expected 1 call to CoreLoader.InvalidateWorkspace(), got %d", calls)
	}
	if calls := coreMock.getLoadStateCalls(); calls != 1 {
		t.Errorf("expected 1 call to CoreLoader.LoadState(), got %d", calls)
	}
}

// TestRecoverLegacyState_NilMaterializerAllowed verifies that when no materializer is configured,
// recovery proceeds directly to store recovery.
func TestRecoverLegacyState_NilMaterializerAllowed(t *testing.T) {
	rawStore := state.NewMemoryStore()
	store := &recoveryCountingStore{Store: rawStore}

	reconciler := state.NewReconciler(store, nil, nil, nil)

	adminUser := &auth.User{
		ID:    "admin-user-test",
		Email: "admin@whatbreaks.internal",
		Role:  auth.RoleAdmin,
	}
	adminCtx := auth.WithUser(context.Background(), adminUser)

	wsID := "ws-nil-mat-test"
	params := state.LegacyRecoveryParams{
		WorkspaceID:          wsID,
		ClusterID:            "prod-cluster-1",
		OperatorIdentity:     adminUser.Email,
		AuthorizeDestructive: true,
		Reason:               "nil materializer test",
	}

	result, err := reconciler.RecoverLegacyState(adminCtx, params)
	if err != nil {
		t.Fatalf("expected RecoverLegacyState() to succeed with nil materializer, got error: %v", err)
	}
	if result == nil {
		t.Fatalf("expected non-nil LegacyRecoveryResult")
	}

	if calls := store.getRecoverCalls(); calls != 1 {
		t.Errorf("expected 1 call to Store.RecoverWorkspaceLegacyState(), got %d", calls)
	}
}
