package answer_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	corev1 "github.com/aaradhychinche-alt/WhatBreaks/gen/go/wb/core/v1"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/answer"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockCoreClient struct {
	lastReq         *corev1.AnalyzeImpactRequest
	resp            *corev1.AnalyzeImpactResponse
	err             error
	invalidateErr   error
	invalidateCalls int
	lastInvReq      *corev1.InvalidateWorkspaceRequest
}

func (m *mockCoreClient) AnalyzeImpact(ctx context.Context, req *corev1.AnalyzeImpactRequest, opts ...grpc.CallOption) (*corev1.AnalyzeImpactResponse, error) {
	m.lastReq = req
	if m.err != nil {
		return nil, m.err
	}
	return m.resp, nil
}

func (m *mockCoreClient) InvalidateWorkspace(ctx context.Context, req *corev1.InvalidateWorkspaceRequest, opts ...grpc.CallOption) (*corev1.InvalidateWorkspaceResponse, error) {
	m.invalidateCalls++
	m.lastInvReq = req
	if m.invalidateErr != nil {
		return nil, m.invalidateErr
	}
	return &corev1.InvalidateWorkspaceResponse{WorkspaceId: req.WorkspaceId, Invalidated: true}, nil
}

// ---------------------------------------------------------------------------
// Test 16: correct conversion to core request
// ---------------------------------------------------------------------------
func TestService_CorrectConversionToCoreRequest(t *testing.T) {
	mock := &mockCoreClient{
		resp: &corev1.AnalyzeImpactResponse{
			Target: &corev1.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "pod",
				ProviderId:   "cluster-1/payments/api",
			},
			Summary: &corev1.ImpactSummary{
				ImpactedCount: 0,
			},
		},
	}
	logger := logging.NewStandardLogger(nil, logging.LevelDebug)
	svc := answer.NewService(mock, logger)

	req := answer.ImpactRequest{
		Target: &answer.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "pod",
			ProviderID:   "cluster-1/payments/api",
		},
		Direction: "incoming",
		MaxDepth:  5,
	}

	_, err := svc.AnalyzeImpact(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.lastReq == nil {
		t.Fatal("expected CoreClient to receive a request")
	}

	if mock.lastReq.Target == nil {
		t.Fatal("expected non-nil target in core request")
	}
	if mock.lastReq.Target.Provider != "kubernetes" {
		t.Errorf("expected provider kubernetes, got %s", mock.lastReq.Target.Provider)
	}
	if mock.lastReq.Target.ResourceType != "pod" {
		t.Errorf("expected resource_type pod, got %s", mock.lastReq.Target.ResourceType)
	}
	if mock.lastReq.Target.ProviderId != "cluster-1/payments/api" {
		t.Errorf("expected provider_id cluster-1/payments/api, got %s", mock.lastReq.Target.ProviderId)
	}
	if mock.lastReq.Direction != "incoming" {
		t.Errorf("expected direction incoming, got %s", mock.lastReq.Direction)
	}
	if mock.lastReq.MaxDepth != 5 {
		t.Errorf("expected max_depth 5, got %d", mock.lastReq.MaxDepth)
	}
}

// ---------------------------------------------------------------------------
// Test 17: correct conversion from core response
// ---------------------------------------------------------------------------
func TestService_CorrectConversionFromCoreResponse(t *testing.T) {
	mock := &mockCoreClient{
		resp: &corev1.AnalyzeImpactResponse{
			Target: &corev1.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "pod",
				ProviderId:   "cluster-1/payments/api",
			},
			Summary: &corev1.ImpactSummary{
				ImpactedCount: 1,
				DirectCount:   1,
				IndirectCount: 0,
				MaxDepth:      1,
			},
			ImpactedResources: []*corev1.ImpactedResource{
				{
					Resource: &corev1.ResourceIdentity{
						Provider:     "kubernetes",
						ResourceType: "pod",
						ProviderId:   "cluster-1/payments/orders",
					},
					Depth: 1,
				},
			},
			Relationships: []*corev1.AnswerRelationship{
				{
					Relationship: &corev1.Relationship{
						Source: &corev1.ResourceIdentity{
							Provider:     "kubernetes",
							ResourceType: "pod",
							ProviderId:   "cluster-1/payments/orders",
						},
						Target: &corev1.ResourceIdentity{
							Provider:     "kubernetes",
							ResourceType: "pod",
							ProviderId:   "cluster-1/payments/api",
						},
						Kind:     "DEPENDS_ON",
						Category: "Dependency",
					},
					State:       "Supported",
					EvidenceIds: []string{"550e8400-e29b-41d4-a716-446655440001"},
				},
			},
			Paths: []*corev1.ImpactPath{
				{
					Resources: []*corev1.ResourceIdentity{
						{Provider: "kubernetes", ResourceType: "pod", ProviderId: "cluster-1/payments/api"},
						{Provider: "kubernetes", ResourceType: "pod", ProviderId: "cluster-1/payments/orders"},
					},
					Relationships: []*corev1.Relationship{
						{
							Source:   &corev1.ResourceIdentity{Provider: "kubernetes", ResourceType: "pod", ProviderId: "cluster-1/payments/orders"},
							Target:   &corev1.ResourceIdentity{Provider: "kubernetes", ResourceType: "pod", ProviderId: "cluster-1/payments/api"},
							Kind:     "DEPENDS_ON",
							Category: "Dependency",
						},
					},
				},
			},
			Evidence: []*corev1.AnswerEvidence{
				{
					Id: "550e8400-e29b-41d4-a716-446655440001",
					Source: &corev1.EvidenceSource{
						Provider:  "kubernetes",
						Collector: "k8s-runtime",
					},
					ObservedAt:      "2026-10-04T12:00:00Z",
					ObservationType: "RUNTIME_CONNECTION",
				},
			},
			ExplanationFacts: []*corev1.ExplanationFact{
				{
					Path: &corev1.ImpactPath{
						Resources: []*corev1.ResourceIdentity{
							{Provider: "kubernetes", ResourceType: "pod", ProviderId: "cluster-1/payments/api"},
							{Provider: "kubernetes", ResourceType: "pod", ProviderId: "cluster-1/payments/orders"},
						},
					},
					EvidenceIds: []string{"550e8400-e29b-41d4-a716-446655440001"},
				},
			},
		},
	}

	svc := answer.NewService(mock, nil)
	ans, err := svc.AnalyzeImpact(context.Background(), answer.ImpactRequest{
		Target: &answer.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "pod",
			ProviderID:   "cluster-1/payments/api",
		},
		Direction: "incoming",
		MaxDepth:  2,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify target
	if ans.Target.ProviderID != "cluster-1/payments/api" {
		t.Errorf("expected target cluster-1/payments/api, got %s", ans.Target.ProviderID)
	}
	// Verify summary
	if ans.Summary.ImpactedCount != 1 || ans.Summary.DirectCount != 1 || ans.Summary.MaxDepth != 1 {
		t.Errorf("unexpected summary: %+v", ans.Summary)
	}
	// Verify impacted resources
	if len(ans.ImpactedResources) != 1 || ans.ImpactedResources[0].Resource.ProviderID != "cluster-1/payments/orders" {
		t.Errorf("unexpected impacted resources: %+v", ans.ImpactedResources)
	}
	// Verify relationship and state lowercased
	if len(ans.Relationships) != 1 || ans.Relationships[0].State != "supported" {
		t.Errorf("unexpected relationships: %+v", ans.Relationships)
	}
	if len(ans.Relationships[0].EvidenceIDs) != 1 || ans.Relationships[0].EvidenceIDs[0] != "550e8400-e29b-41d4-a716-446655440001" {
		t.Errorf("unexpected evidence IDs: %+v", ans.Relationships[0].EvidenceIDs)
	}
	// Verify paths
	if len(ans.Paths) != 1 || len(ans.Paths[0].Resources) != 2 {
		t.Errorf("unexpected paths: %+v", ans.Paths)
	}
	// Verify evidence
	if len(ans.Evidence) != 1 || ans.Evidence[0].ID != "550e8400-e29b-41d4-a716-446655440001" {
		t.Errorf("unexpected evidence: %+v", ans.Evidence)
	}
	// Verify explanation facts
	if len(ans.ExplanationFacts) != 1 || len(ans.ExplanationFacts[0].EvidenceIDs) != 1 {
		t.Errorf("unexpected explanation facts: %+v", ans.ExplanationFacts)
	}
}

// ---------------------------------------------------------------------------
// Test 18: no business logic duplication
// ---------------------------------------------------------------------------
func TestService_NoBusinessLogicDuplication(t *testing.T) {
	// The service must delegate directly to the CoreClient without modifying
	// or recalculating the blast radius or inventing relationships.
	mock := &mockCoreClient{
		resp: &corev1.AnalyzeImpactResponse{
			Target: &corev1.ResourceIdentity{
				Provider:     "aws",
				ResourceType: "rds",
				ProviderId:   "arn:aws:rds:us-east-1:123:db",
			},
			Summary: &corev1.ImpactSummary{
				ImpactedCount: 42, // Non-trivial count returned by Rust
				DirectCount:   10,
				IndirectCount: 32,
				MaxDepth:      5,
			},
		},
	}

	svc := answer.NewService(mock, nil)
	ans, err := svc.AnalyzeImpact(context.Background(), answer.ImpactRequest{
		Target: &answer.ResourceIdentity{
			Provider:     "aws",
			ResourceType: "rds",
			ProviderID:   "arn:aws:rds:us-east-1:123:db",
		},
		Direction: "incoming",
		MaxDepth:  5,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Must be verbatim what the Core Engine returned, without Go recalculation
	if ans.Summary.ImpactedCount != 42 || ans.Summary.DirectCount != 10 || ans.Summary.IndirectCount != 32 {
		t.Errorf("business logic altered by service layer: %+v", ans.Summary)
	}
}

// ---------------------------------------------------------------------------
// Test 19: error propagation
// ---------------------------------------------------------------------------
func TestService_ErrorPropagation(t *testing.T) {
	tests := []struct {
		name     string
		grpcErr  error
		wantErr  error
		isValErr bool
	}{
		{
			name:    "unavailable",
			grpcErr: status.Error(codes.Unavailable, "core down"),
			wantErr: answer.ErrCoreUnavailable,
		},
		{
			name:    "failed precondition",
			grpcErr: status.Error(codes.FailedPrecondition, "unconnected"),
			wantErr: answer.ErrCoreUnavailable,
		},
		{
			name:    "failed precondition not materialized",
			grpcErr: status.Error(codes.FailedPrecondition, "workspace state not materialized for workspace_id: 'ws-1'"),
			wantErr: answer.ErrWorkspaceNotMaterialized,
		},
		{
			name:    "deadline exceeded",
			grpcErr: status.Error(codes.DeadlineExceeded, "timed out"),
			wantErr: answer.ErrCoreTimeout,
		},
		{
			name:    "canceled",
			grpcErr: status.Error(codes.Canceled, "client cancelled"),
			wantErr: answer.ErrRequestCanceled,
		},
		{
			name:     "invalid argument",
			grpcErr:  status.Error(codes.InvalidArgument, "invalid target provider"),
			isValErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockCoreClient{err: tt.grpcErr}
			svc := answer.NewService(mock, nil)

			_, err := svc.AnalyzeImpact(context.Background(), answer.ImpactRequest{
				Target: &answer.ResourceIdentity{
					Provider:     "kubernetes",
					ResourceType: "pod",
					ProviderID:   "cluster-1/payments/api",
				},
				Direction: "incoming",
				MaxDepth:  5,
			})

			if err == nil {
				t.Fatalf("expected error, got nil")
			}

			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("expected error %v, got %v", tt.wantErr, err)
			}

			if tt.isValErr {
				var vErr *answer.ValidationError
				if !errors.As(err, &vErr) {
					t.Errorf("expected ValidationError, got %T: %v", err, err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Test 20: deterministic response
// ---------------------------------------------------------------------------
func TestService_DeterministicResponse(t *testing.T) {
	mock := &mockCoreClient{
		resp: &corev1.AnalyzeImpactResponse{
			Target: &corev1.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "pod",
				ProviderId:   "cluster-1/payments/api",
			},
			Summary: &corev1.ImpactSummary{
				ImpactedCount: 2,
				DirectCount:   2,
				IndirectCount: 0,
				MaxDepth:      1,
			},
			ImpactedResources: []*corev1.ImpactedResource{
				{
					Resource: &corev1.ResourceIdentity{Provider: "kubernetes", ResourceType: "pod", ProviderId: "a"},
					Depth:    1,
				},
				{
					Resource: &corev1.ResourceIdentity{Provider: "kubernetes", ResourceType: "pod", ProviderId: "b"},
					Depth:    1,
				},
			},
		},
	}

	svc := answer.NewService(mock, nil)
	req := answer.ImpactRequest{
		Target: &answer.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "pod",
			ProviderID:   "cluster-1/payments/api",
		},
		Direction: "incoming",
		MaxDepth:  5,
	}

	ans1, err := svc.AnalyzeImpact(context.Background(), req)
	if err != nil {
		t.Fatalf("first call failed: %v", err)
	}

	ans2, err := svc.AnalyzeImpact(context.Background(), req)
	if err != nil {
		t.Fatalf("second call failed: %v", err)
	}

	b1, _ := json.Marshal(ans1)
	b2, _ := json.Marshal(ans2)

	if string(b1) != string(b2) {
		t.Fatalf("responses were non-deterministic:\nFirst:  %s\nSecond: %s", string(b1), string(b2))
	}
}

// ---------------------------------------------------------------------------
// Test: change-aware request and response translation
// ---------------------------------------------------------------------------
func TestService_ChangeAwareTranslation(t *testing.T) {
	mock := &mockCoreClient{
		resp: &corev1.AnalyzeImpactResponse{
			Target: &corev1.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "configmap",
				ProviderId:   "cluster-1/default/backend-config",
			},
			Summary: &corev1.ImpactSummary{
				ImpactedCount: 1,
				DirectCount:   1,
				IndirectCount: 0,
				MaxDepth:      1,
			},
			ImpactedResources: []*corev1.ImpactedResource{
				{
					Resource:     &corev1.ResourceIdentity{Provider: "kubernetes", ResourceType: "pod", ProviderId: "cluster-1/default/backend-pod"},
					Depth:        1,
					ImpactType:   "DIRECT",
					ImpactReason: "Direct dependent via DEPENDS_ON: may require reload or restart to consume updated target state",
				},
			},
			ChangeAssessment: &corev1.ChangeAssessment{
				ChangeType:   corev1.ChangeType_CHANGE_TYPE_UPDATE,
				ImpactNature: "POTENTIAL_IMPACT",
				Assumptions:  []string{"Target update modifies configuration or schema"},
				Limitations:  []string{"Multi-hop propagation beyond depth 1 is not modeled"},
			},
		},
	}

	svc := answer.NewService(mock, nil)
	req := answer.ImpactRequest{
		Target: &answer.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "configmap",
			ProviderID:   "cluster-1/default/backend-config",
		},
		Direction: "incoming",
		MaxDepth:  3,
		ProposedChange: &answer.ProposedChange{
			ChangeType: answer.ChangeTypeUpdate,
			Details:    "updating DB connection string",
		},
	}

	resp, err := svc.AnalyzeImpact(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify gRPC request had correct ProposedChange
	if mock.lastReq.ProposedChange == nil {
		t.Fatal("expected non-nil ProposedChange in core request")
	}
	if mock.lastReq.ProposedChange.ChangeType != corev1.ChangeType_CHANGE_TYPE_UPDATE {
		t.Errorf("expected CHANGE_TYPE_UPDATE, got %v", mock.lastReq.ProposedChange.ChangeType)
	}
	if mock.lastReq.ProposedChange.Details != "updating DB connection string" {
		t.Errorf("expected details 'updating DB connection string', got %q", mock.lastReq.ProposedChange.Details)
	}

	// Verify answer had correct ChangeAssessment
	if resp.ChangeAssessment == nil {
		t.Fatal("expected non-nil ChangeAssessment in response")
	}
	if resp.ChangeAssessment.ChangeType != answer.ChangeTypeUpdate {
		t.Errorf("expected ChangeType UPDATE, got %q", resp.ChangeAssessment.ChangeType)
	}
	if resp.ChangeAssessment.ImpactNature != "POTENTIAL_IMPACT" {
		t.Errorf("expected POTENTIAL_IMPACT, got %q", resp.ChangeAssessment.ImpactNature)
	}
	if len(resp.ChangeAssessment.Assumptions) == 0 {
		t.Error("expected non-empty assumptions")
	}
	if len(resp.ChangeAssessment.Limitations) == 0 {
		t.Error("expected non-empty limitations")
	}

	// Verify impacted resource fields
	if len(resp.ImpactedResources) != 1 {
		t.Fatalf("expected 1 impacted resource, got %d", len(resp.ImpactedResources))
	}
	ir := resp.ImpactedResources[0]
	if ir.ImpactType != "DIRECT" {
		t.Errorf("expected DIRECT impact_type, got %q", ir.ImpactType)
	}
	if ir.ImpactReason == "" {
		t.Error("expected non-empty impact_reason")
	}
}

func TestService_ChangeAware_ScaleZeroImpacts(t *testing.T) {
	mock := &mockCoreClient{
		resp: &corev1.AnalyzeImpactResponse{
			Target: &corev1.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "service",
				ProviderId:   "payments-api",
			},
			Summary: &corev1.ImpactSummary{
				ImpactedCount: 0,
			},
			ChangeAssessment: &corev1.ChangeAssessment{
				ChangeType:   corev1.ChangeType_CHANGE_TYPE_SCALE,
				ImpactNature: "POTENTIAL_IMPACT",
				Assumptions:  []string{"Target scale event alters runtime throughput"},
				Limitations: []string{
					"Target resource has declarative dependencies (e.g. DEPENDS_ON), but no runtime CALLS relationships were observed. Under the v1 SCALE policy, declarative dependencies are excluded from scaling impact; zero identified impacts reflects an absence of qualifying runtime caller evidence in the current model, not confirmed absence of operational scaling consequences.",
				},
			},
		},
	}

	svc := answer.NewService(mock, nil)
	req := answer.ImpactRequest{
		Target: &answer.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "service",
			ProviderID:   "cluster-1/default/payments-api",
		},
		Direction: "incoming",
		MaxDepth:  3,
		ProposedChange: &answer.ProposedChange{
			ChangeType: answer.ChangeTypeScale,
			Details:    "scaling to 0 replicas",
		},
	}

	resp, err := svc.AnalyzeImpact(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.Summary.ImpactedCount != 0 {
		t.Errorf("expected 0 impacted count, got %d", resp.Summary.ImpactedCount)
	}
	if resp.ChangeAssessment == nil {
		t.Fatal("expected non-nil ChangeAssessment")
	}
	if resp.ChangeAssessment.ChangeType != answer.ChangeTypeScale {
		t.Errorf("expected SCALE, got %s", resp.ChangeAssessment.ChangeType)
	}
	if len(resp.ChangeAssessment.Limitations) == 0 || !strings.Contains(resp.ChangeAssessment.Limitations[0], "declarative dependencies (e.g. DEPENDS_ON)") {
		t.Errorf("expected limitation explaining DEPENDS_ON exclusion, got: %+v", resp.ChangeAssessment.Limitations)
	}
}

func TestService_ChangeAware_ReplaceTranslation(t *testing.T) {
	mock := &mockCoreClient{
		resp: &corev1.AnalyzeImpactResponse{
			Target: &corev1.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "service",
				ProviderId:   "cluster-1/default/payments-api",
			},
			Summary: &corev1.ImpactSummary{
				ImpactedCount: 1,
				DirectCount:   1,
				MaxDepth:      1,
			},
			ImpactedResources: []*corev1.ImpactedResource{
				{
					Resource:     &corev1.ResourceIdentity{Provider: "kubernetes", ResourceType: "service", ProviderId: "cluster-1/default/checkout-service"},
					Depth:        1,
					ImpactType:   "DIRECT",
					ImpactReason: "Direct connection via DEPENDS_ON: subject to transient rollover or reconnection during replacement",
				},
			},
			ChangeAssessment: &corev1.ChangeAssessment{
				ChangeType:   corev1.ChangeType_CHANGE_TYPE_REPLACE,
				ImpactNature: "POTENTIAL_IMPACT",
				Assumptions:  []string{"Target replacement implies temporary disruption"},
				Limitations:  []string{"Impact is limited to direct connections during the replacement transition window"},
			},
		},
	}

	svc := answer.NewService(mock, nil)
	req := answer.ImpactRequest{
		Target: &answer.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "service",
			ProviderID:   "cluster-1/default/payments-api",
		},
		Direction: "incoming",
		MaxDepth:  5,
		ProposedChange: &answer.ProposedChange{
			ChangeType: answer.ChangeTypeReplace,
			Details:    "rolling replacement of payments-api",
		},
	}

	resp, err := svc.AnalyzeImpact(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.lastReq.ProposedChange == nil || mock.lastReq.ProposedChange.ChangeType != corev1.ChangeType_CHANGE_TYPE_REPLACE {
		t.Errorf("expected CHANGE_TYPE_REPLACE in gRPC request, got %+v", mock.lastReq.ProposedChange)
	}
	if resp.ChangeAssessment == nil || resp.ChangeAssessment.ChangeType != answer.ChangeTypeReplace {
		t.Errorf("expected REPLACE in ChangeAssessment, got %+v", resp.ChangeAssessment)
	}
	if len(resp.ImpactedResources) != 1 || resp.ImpactedResources[0].ImpactType != "DIRECT" {
		t.Errorf("expected DIRECT impacted resource, got %+v", resp.ImpactedResources)
	}
}

func TestService_LegacyRequest_PreservesNilAssessmentAndEmptyFields(t *testing.T) {
	mock := &mockCoreClient{
		resp: &corev1.AnalyzeImpactResponse{
			Target: &corev1.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "service",
				ProviderId:   "cluster-1/default/payments-api",
			},
			Summary: &corev1.ImpactSummary{
				ImpactedCount: 1,
				DirectCount:   1,
				MaxDepth:      1,
			},
			ImpactedResources: []*corev1.ImpactedResource{
				{
					Resource: &corev1.ResourceIdentity{Provider: "kubernetes", ResourceType: "service", ProviderId: "cluster-1/default/checkout-service"},
					Depth:    1,
				},
			},
		},
	}

	svc := answer.NewService(mock, nil)
	req := answer.ImpactRequest{
		Target: &answer.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "service",
			ProviderID:   "cluster-1/default/payments-api",
		},
		Direction: "incoming",
		MaxDepth:  3,
	}

	resp, err := svc.AnalyzeImpact(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.lastReq.ProposedChange != nil {
		t.Errorf("expected nil ProposedChange in gRPC request for legacy query, got %+v", mock.lastReq.ProposedChange)
	}
	if resp.ChangeAssessment != nil {
		t.Errorf("expected nil ChangeAssessment in response for legacy query, got %+v", resp.ChangeAssessment)
	}
	if len(resp.ImpactedResources) != 1 {
		t.Fatalf("expected 1 impacted resource, got %d", len(resp.ImpactedResources))
	}
	if resp.ImpactedResources[0].ImpactType != "" {
		t.Errorf("expected empty ImpactType for legacy query, got %q", resp.ImpactedResources[0].ImpactType)
	}
	if resp.ImpactedResources[0].ImpactReason != "" {
		t.Errorf("expected empty ImpactReason for legacy query, got %q", resp.ImpactedResources[0].ImpactReason)
	}
}

// ---------------------------------------------------------------------------
// Test: Core Engine invalidation failure after integrity rejection is observable
// ---------------------------------------------------------------------------
type mockIntegrityChecker struct {
	err    error
	calls  int
	lastWS string
}

func (m *mockIntegrityChecker) CheckWorkspaceIntegrity(ctx context.Context, workspaceID string) error {
	m.calls++
	m.lastWS = workspaceID
	return m.err
}

func TestService_IntegrityRejection_InvalidationFailureObservable(t *testing.T) {
	sentinelIntegrityErr := errors.New("workspace contains un-migrated legacy state")
	sentinelInvErr := errors.New("core engine gRPC connection lost")

	mockCore := &mockCoreClient{
		invalidateErr: sentinelInvErr,
	}
	checker := &mockIntegrityChecker{
		err: sentinelIntegrityErr,
	}

	var logBuf bytes.Buffer
	logger := logging.NewJSONLogger(&logBuf, logging.LevelDebug, "answer-service")

	svc := answer.NewService(
		mockCore,
		logger,
		answer.WithWorkspaceIntegrityChecker(checker),
	)

	wsID := "ws-integrity-invalidation-obs-test"
	req := answer.ImpactRequest{
		WorkspaceID: wsID,
		Target: &answer.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "pod",
			ProviderID:   "cluster-1/default/payments-api",
		},
		Direction: "incoming",
		MaxDepth:  3,
	}

	resp, err := svc.AnalyzeImpact(context.Background(), req)

	// 1. Must return an error, and response must be nil
	if resp != nil {
		t.Errorf("expected nil response on integrity rejection, got %+v", resp)
	}
	if err == nil {
		t.Fatalf("expected error on integrity rejection, got nil")
	}

	// 2. Original rejection error remains the returned error (IdentityIntegrityError wrapping ErrIdentityIntegrityRejected)
	if !errors.Is(err, answer.ErrIdentityIntegrityRejected) {
		t.Errorf("expected error to wrap ErrIdentityIntegrityRejected, got: %v", err)
	}
	var iErr *answer.IdentityIntegrityError
	if !errors.As(err, &iErr) {
		t.Fatalf("expected *answer.IdentityIntegrityError, got %T (%v)", err, err)
	}
	if iErr.WorkspaceID != wsID {
		t.Errorf("expected WorkspaceID %q, got %q", wsID, iErr.WorkspaceID)
	}
	if !strings.Contains(iErr.Reason, sentinelIntegrityErr.Error()) {
		t.Errorf("expected error Reason to contain %q, got %q", sentinelIntegrityErr.Error(), iErr.Reason)
	}

	// 3. Must NOT replace or wrap with the invalidation error in the returned error
	if errors.Is(err, sentinelInvErr) {
		t.Errorf("returned error must NOT wrap the Core Engine invalidation error")
	}

	// 4. Invalidation was attempted on the Core Client for this workspace
	if mockCore.invalidateCalls != 1 {
		t.Errorf("expected exactly 1 InvalidateWorkspace call, got %d", mockCore.invalidateCalls)
	}
	if mockCore.lastInvReq == nil || mockCore.lastInvReq.WorkspaceId != wsID {
		t.Errorf("expected InvalidateWorkspace request for workspace %q, got %+v", wsID, mockCore.lastInvReq)
	}

	// 5. Invalidation failure is observable in the logs
	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "Failed to invalidate workspace in Core Engine after integrity rejection") {
		t.Errorf("expected log output to record invalidation failure, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, wsID) {
		t.Errorf("expected log output to include workspace ID %q, got: %s", wsID, logOutput)
	}
	if !strings.Contains(logOutput, sentinelInvErr.Error()) {
		t.Errorf("expected log output to include invalidation error %q, got: %s", sentinelInvErr.Error(), logOutput)
	}
}

func TestService_IntegrityRejection_InvalidationSuccess(t *testing.T) {
	sentinelIntegrityErr := errors.New("workspace contains un-migrated legacy state")
	mockCore := &mockCoreClient{}
	checker := &mockIntegrityChecker{
		err: sentinelIntegrityErr,
	}

	var logBuf bytes.Buffer
	logger := logging.NewJSONLogger(&logBuf, logging.LevelDebug, "answer-service")

	svc := answer.NewService(
		mockCore,
		logger,
		answer.WithWorkspaceIntegrityChecker(checker),
	)

	wsID := "ws-integrity-invalidation-success-test"
	req := answer.ImpactRequest{
		WorkspaceID: wsID,
		Target: &answer.ResourceIdentity{
			Provider:     "kubernetes",
			ResourceType: "pod",
			ProviderID:   "cluster-1/default/payments-api",
		},
		Direction: "incoming",
		MaxDepth:  3,
	}

	resp, err := svc.AnalyzeImpact(context.Background(), req)
	if resp != nil {
		t.Errorf("expected nil response on integrity rejection, got %+v", resp)
	}
	if err == nil {
		t.Fatalf("expected error on integrity rejection, got nil")
	}
	if !errors.Is(err, answer.ErrIdentityIntegrityRejected) {
		t.Errorf("expected error to wrap ErrIdentityIntegrityRejected, got: %v", err)
	}

	if mockCore.invalidateCalls != 1 {
		t.Errorf("expected 1 InvalidateWorkspace call, got %d", mockCore.invalidateCalls)
	}

	logOutput := logBuf.String()
	if strings.Contains(logOutput, "Failed to invalidate workspace in Core Engine") {
		t.Errorf("unexpected invalidation failure log when invalidation succeeded: %s", logOutput)
	}
}
