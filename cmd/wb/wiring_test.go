package main_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	corev1 "github.com/aaradhychinche-alt/WhatBreaks/gen/go/wb/core/v1"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/answer"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/api"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/auth"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/logging"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/state"
	"google.golang.org/grpc"
)

type mockGrpcCoreClient struct {
	invalidatedWorkspaces []string
}

func (m *mockGrpcCoreClient) AnalyzeImpact(ctx context.Context, req *corev1.AnalyzeImpactRequest, opts ...grpc.CallOption) (*corev1.AnalyzeImpactResponse, error) {
	return &corev1.AnalyzeImpactResponse{
		Summary: &corev1.ImpactSummary{
			ImpactedCount: 0,
		},
	}, nil
}

func (m *mockGrpcCoreClient) InvalidateWorkspace(ctx context.Context, req *corev1.InvalidateWorkspaceRequest, opts ...grpc.CallOption) (*corev1.InvalidateWorkspaceResponse, error) {
	m.invalidatedWorkspaces = append(m.invalidatedWorkspaces, req.WorkspaceId)
	return &corev1.InvalidateWorkspaceResponse{
		WorkspaceId: req.WorkspaceId,
		Invalidated: true,
	}, nil
}

// TestProductionWiring_ImpactHandlerGuards verifies Phase 3 and Phase 4 guarantees:
// 1. Workspace authorization precedes any integrity disclosure.
// 2. Un-migrated legacy state produces HTTP 422 with structured error, NOT a zero-impact response.
// 3. Stale graph in Rust engine is evicted upon integrity check failure.
// 4. Missing checker in production fails closed.
func TestProductionWiring_ImpactHandlerGuards(t *testing.T) {
	logger := logging.NewStandardLogger(nil, logging.LevelWarn)
	coreMock := &mockGrpcCoreClient{}

	memStore := state.NewMemoryStore()
	wsID := "00000000-0000-0000-0000-000000000001"

	// Seed legacy K8s resource in workspace
	now := time.Now().UTC()
	err := memStore.SaveResources(context.Background(), wsID, []state.Resource{
		{
			WorkspaceID: wsID,
			Identity: state.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "pod",
				ProviderID:   "default/payment-pod", // Unqualified 2-segment legacy
			},
			FirstObservedAt: now,
			LastObservedAt:  now,
		},
	})
	if err != nil {
		t.Fatalf("failed to seed resources: %v", err)
	}

	// Build authorizer
	memberships := auth.NewMemoryMembershipProvider()
	memberships.AddMember("user-1", wsID, auth.RoleMember)
	authorizer := auth.NewWorkspaceAuthorizer(memberships)

	// Construct answer service with production options (WithWorkspaceIntegrityChecker + WithRequireIntegrityChecker)
	answerSvc := answer.NewService(
		coreMock,
		logger,
		answer.WithWorkspaceIntegrityChecker(memStore),
		answer.WithRequireIntegrityChecker(),
	)

	handler := answer.NewHandler(
		answerSvc,
		answer.WithLogger(logger),
		answer.WithAuthorizer(authorizer, nil),
	)

	reqPayload := map[string]interface{}{
		"workspace_id": wsID,
		"target": map[string]string{
			"provider":      "kubernetes",
			"resource_type": "pod",
			"provider_id":   "cluster-1/default/payment-pod",
		},
		"direction": "incoming",
		"max_depth": 3,
	}
	bodyBytes, _ := json.Marshal(reqPayload)

	// Step 1: Unauthenticated request must return 401 Unauthorized before checking integrity
	{
		httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/impact", bytes.NewReader(bodyBytes))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httpReq)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("expected 401 Unauthorized for unauthenticated request, got %d", rec.Code)
		}
	}

	// Step 2: Authenticated request with legacy workspace state must return 422 Unprocessable Entity
	// and NEVER return a false-negative zero-impact answer!
	{
		httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/impact", bytes.NewReader(bodyBytes))
		// Authenticate with user-1
		ctx := auth.WithUser(httpReq.Context(), &auth.User{
			ID:    "user-1",
			Role:  auth.RoleMember,
			Email: "dev@whatbreaks.io",
		})
		httpReq = httpReq.WithContext(ctx)

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httpReq)

		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("expected 422 Unprocessable Entity, got %d: body=%s", rec.Code, rec.Body.String())
		}

		var resp api.ErrorResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to decode error response: %v", err)
		}
		if resp.Code != string(answer.CodeIdentityIntegrityRejected) {
			t.Errorf("expected error code %q, got %q", answer.CodeIdentityIntegrityRejected, resp.Code)
		}

		// Verify Phase 4: Core Engine received InvalidateWorkspace to evict any stale graph
		foundInv := false
		for _, inv := range coreMock.invalidatedWorkspaces {
			if inv == wsID {
				foundInv = true
				break
			}
		}
		if !foundInv {
			t.Errorf("expected Core Engine InvalidateWorkspace to be called for %s", wsID)
		}
	}

	// Step 3: Production AnswerService without checker must fail closed
	{
		uncheckSvc := answer.NewService(
			coreMock,
			logger,
			answer.WithRequireIntegrityChecker(),
		)
		uncheckHandler := answer.NewHandler(
			uncheckSvc,
			answer.WithLogger(logger),
			answer.WithAuthorizer(authorizer, nil),
		)

		httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/impact", bytes.NewReader(bodyBytes))
		ctx := auth.WithUser(httpReq.Context(), &auth.User{
			ID:    "user-1",
			Role:  auth.RoleMember,
			Email: "dev@whatbreaks.io",
		})
		httpReq = httpReq.WithContext(ctx)

		rec := httptest.NewRecorder()
		uncheckHandler.ServeHTTP(rec, httpReq)

		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("expected 422 fail-closed when integrity checker is missing, got %d: body=%s", rec.Code, rec.Body.String())
		}
	}
}
