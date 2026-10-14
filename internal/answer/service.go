package answer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	corev1 "github.com/aaradhychinche-alt/WhatBreaks/gen/go/wb/core/v1"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CoreClient defines the gRPC transport contract required by the AnswerService.
type CoreClient interface {
	AnalyzeImpact(ctx context.Context, req *corev1.AnalyzeImpactRequest, opts ...grpc.CallOption) (*corev1.AnalyzeImpactResponse, error)
	InvalidateWorkspace(ctx context.Context, req *corev1.InvalidateWorkspaceRequest, opts ...grpc.CallOption) (*corev1.InvalidateWorkspaceResponse, error)
}

// WorkspaceIntegrityChecker defines the contract to verify that a workspace state is clean and canonical.
type WorkspaceIntegrityChecker interface {
	CheckWorkspaceIntegrity(ctx context.Context, workspaceID string) error
}

// Option configures an AnswerService instance.
type Option func(*service)

// WithWorkspaceIntegrityChecker configures an integrity checker to guard impact queries.
func WithWorkspaceIntegrityChecker(checker WorkspaceIntegrityChecker) Option {
	return func(s *service) {
		s.checker = checker
	}
}

// WithRequireIntegrityChecker mandates that an integrity checker must be configured.
// Missing checker will cause impact queries to fail closed.
func WithRequireIntegrityChecker() Option {
	return func(s *service) {
		s.requireChecker = true
	}
}

// Service defines the application-level impact analysis operations.
type Service interface {
	AnalyzeImpact(ctx context.Context, req ImpactRequest) (*ImpactAnswer, error)
}

type service struct {
	client         CoreClient
	checker        WorkspaceIntegrityChecker
	logger         logging.Logger
	requireChecker bool
}

// NewService constructs an AnswerService backed by the given CoreClient and Logger.
func NewService(client CoreClient, logger logging.Logger, opts ...Option) Service {
	if logger == nil {
		logger = logging.NewJSONLogger(nil, logging.LevelInfo, "answer-service")
	}
	s := &service{
		client: client,
		logger: logger,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// AnalyzeImpact orchestrates the validation, protobuf translation, gRPC invocation,
// error translation, and response transformation for impact queries.
func (s *service) AnalyzeImpact(ctx context.Context, req ImpactRequest) (*ImpactAnswer, error) {
	if err := ValidateImpactRequest(&req); err != nil {
		return nil, err
	}

	// Refuse false-negative zero-impact queries if workspace has un-migrated legacy state or missing checker in production
	if s.checker == nil {
		if s.requireChecker {
			s.logger.Error("Refusing impact query: workspace integrity checker is required but not configured",
				"workspace_id", req.WorkspaceID,
			)
			return nil, &IdentityIntegrityError{
				WorkspaceID: req.WorkspaceID,
				Reason:      "workspace integrity checker is not configured; refusing to serve unchecked workspace state",
				Err:         ErrIdentityIntegrityRejected,
			}
		}
	} else if strings.TrimSpace(req.WorkspaceID) != "" {
		if err := s.checker.CheckWorkspaceIntegrity(ctx, req.WorkspaceID); err != nil {
			s.logger.Warn("Refusing impact query: workspace contains un-migrated legacy state",
				"workspace_id", req.WorkspaceID,
				"error", err.Error(),
			)
			// Evict the workspace graph from the Rust Core Engine so previously materialized state cannot be served
			if s.client != nil {
				_, _ = s.client.InvalidateWorkspace(ctx, &corev1.InvalidateWorkspaceRequest{
					WorkspaceId: req.WorkspaceID,
				})
			}
			return nil, &IdentityIntegrityError{
				WorkspaceID: req.WorkspaceID,
				Reason:      err.Error(),
				Err:         ErrIdentityIntegrityRejected,
			}
		}
	}

	start := time.Now()

	var protoChange *corev1.ProposedChange
	if req.ProposedChange != nil {
		var protoCT corev1.ChangeType
		switch req.ProposedChange.ChangeType {
		case ChangeTypeDelete:
			protoCT = corev1.ChangeType_CHANGE_TYPE_DELETE
		case ChangeTypeUpdate:
			protoCT = corev1.ChangeType_CHANGE_TYPE_UPDATE
		case ChangeTypeScale:
			protoCT = corev1.ChangeType_CHANGE_TYPE_SCALE
		case ChangeTypeReplace:
			protoCT = corev1.ChangeType_CHANGE_TYPE_REPLACE
		default:
			return nil, &ValidationError{
				Field:   "proposed_change.change_type",
				Message: fmt.Sprintf("unsupported change_type %q: must be DELETE, UPDATE, SCALE, or REPLACE", req.ProposedChange.ChangeType),
			}
		}
		protoChange = &corev1.ProposedChange{
			ChangeType: protoCT,
			Details:    req.ProposedChange.Details,
		}
	}

	protoReq := &corev1.AnalyzeImpactRequest{
		Target: &corev1.ResourceIdentity{
			Provider:     req.Target.Provider,
			ResourceType: req.Target.ResourceType,
			ProviderId:   req.Target.ProviderID,
		},
		Direction:      strings.ToLower(strings.TrimSpace(req.Direction)),
		MaxDepth:       req.MaxDepth,
		WorkspaceId:    req.WorkspaceID,
		ProposedChange: protoChange,
	}

	protoResp, err := s.client.AnalyzeImpact(ctx, protoReq)
	duration := time.Since(start)

	if err != nil {
		mappedErr := mapGRPCError(ctx, err)
		s.logger.Warn("Core engine impact query failed",
			"target_provider", req.Target.Provider,
			"target_type", req.Target.ResourceType,
			"target_id", req.Target.ProviderID,
			"direction", req.Direction,
			"max_depth", req.MaxDepth,
			"duration_ms", duration.Milliseconds(),
			"error", mappedErr.Error(),
		)
		return nil, mappedErr
	}

	if protoResp == nil || protoResp.Target == nil || protoResp.Summary == nil {
		s.logger.Error("Core engine returned incomplete response",
			"target_provider", req.Target.Provider,
			"target_type", req.Target.ResourceType,
			"target_id", req.Target.ProviderID,
		)
		return nil, ErrMalformedCoreResponse
	}

	answer := transformProtoResponse(protoResp)

	s.logger.Info("Analyzed impact successfully",
		"target_provider", answer.Target.Provider,
		"target_type", answer.Target.ResourceType,
		"target_id", answer.Target.ProviderID,
		"direction", req.Direction,
		"max_depth", req.MaxDepth,
		"duration_ms", duration.Milliseconds(),
		"impacted_count", answer.Summary.ImpactedCount,
		"direct_count", answer.Summary.DirectCount,
		"indirect_count", answer.Summary.IndirectCount,
	)

	return answer, nil
}

func mapGRPCError(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return ErrRequestCanceled
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return ErrCoreTimeout
	}

	st, ok := status.FromError(err)
	if !ok {
		return fmt.Errorf("transport failure: %w", err)
	}

	switch st.Code() {
	case codes.Canceled:
		return ErrRequestCanceled
	case codes.DeadlineExceeded:
		return ErrCoreTimeout
	case codes.Unavailable:
		return ErrCoreUnavailable
	case codes.FailedPrecondition:
		msg := strings.ToLower(st.Message())
		if strings.Contains(msg, "not materialized") {
			return ErrWorkspaceNotMaterialized
		}
		if strings.Contains(msg, "legacy") || strings.Contains(msg, "identity") || strings.Contains(msg, "incompatible") {
			return &IdentityIntegrityError{
				Reason: st.Message(),
				Err:    ErrIdentityIntegrityRejected,
			}
		}
		return ErrCoreUnavailable
	case codes.InvalidArgument:
		return &ValidationError{
			Field:   "core",
			Message: st.Message(),
		}
	default:
		return fmt.Errorf("core error (%s): %s", st.Code(), st.Message())
	}
}

func transformProtoResponse(resp *corev1.AnalyzeImpactResponse) *ImpactAnswer {
	target := transformResourceIdentity(resp.Target)

	summary := ImpactSummary{
		ImpactedCount: resp.Summary.ImpactedCount,
		DirectCount:   resp.Summary.DirectCount,
		IndirectCount: resp.Summary.IndirectCount,
		MaxDepth:      resp.Summary.MaxDepth,
	}

	impactedResources := make([]ImpactedResource, 0, len(resp.ImpactedResources))
	for _, ir := range resp.ImpactedResources {
		if ir != nil && ir.Resource != nil {
			impactedResources = append(impactedResources, ImpactedResource{
				Resource:     transformResourceIdentity(ir.Resource),
				Depth:        ir.Depth,
				ImpactType:   ir.ImpactType,
				ImpactReason: ir.ImpactReason,
			})
		}
	}

	relationships := make([]AnswerRelationship, 0, len(resp.Relationships))
	for _, r := range resp.Relationships {
		if r != nil && r.Relationship != nil {
			relationships = append(relationships, transformAnswerRelationship(r))
		}
	}

	paths := make([]ImpactPath, 0, len(resp.Paths))
	for _, p := range resp.Paths {
		if p != nil {
			paths = append(paths, transformImpactPath(p))
		}
	}

	evidence := make([]AnswerEvidence, 0, len(resp.Evidence))
	for _, e := range resp.Evidence {
		if e != nil {
			ev := AnswerEvidence{
				ID:              e.Id,
				ObservedAt:      e.ObservedAt,
				ObservationType: e.ObservationType,
			}
			if e.Source != nil {
				ev.Source = EvidenceSource{
					Provider:  e.Source.Provider,
					Collector: e.Source.Collector,
				}
			}
			evidence = append(evidence, ev)
		}
	}

	explanationFacts := make([]ExplanationFact, 0, len(resp.ExplanationFacts))
	for _, fact := range resp.ExplanationFacts {
		if fact != nil {
			ef := ExplanationFact{
				EvidenceIDs: copyStringSlice(fact.EvidenceIds),
			}
			if fact.Path != nil {
				ef.Path = transformImpactPath(fact.Path)
			}
			ef.Relationships = make([]AnswerRelationship, 0, len(fact.Relationships))
			for _, rel := range fact.Relationships {
				if rel != nil && rel.Relationship != nil {
					ef.Relationships = append(ef.Relationships, transformAnswerRelationship(rel))
				}
			}
			explanationFacts = append(explanationFacts, ef)
		}
	}

	var changeAssessment *ChangeAssessment
	if resp.ChangeAssessment != nil {
		var ct ChangeType
		switch resp.ChangeAssessment.ChangeType {
		case corev1.ChangeType_CHANGE_TYPE_DELETE:
			ct = ChangeTypeDelete
		case corev1.ChangeType_CHANGE_TYPE_UPDATE:
			ct = ChangeTypeUpdate
		case corev1.ChangeType_CHANGE_TYPE_SCALE:
			ct = ChangeTypeScale
		case corev1.ChangeType_CHANGE_TYPE_REPLACE:
			ct = ChangeTypeReplace
		}
		changeAssessment = &ChangeAssessment{
			ChangeType:   ct,
			ImpactNature: resp.ChangeAssessment.ImpactNature,
			Assumptions:  copyStringSlice(resp.ChangeAssessment.Assumptions),
			Limitations:  copyStringSlice(resp.ChangeAssessment.Limitations),
		}
	}

	var completeness *AnalysisCompleteness
	if resp.Completeness != nil {
		statusStr := "COMPLETE"
		switch resp.Completeness.Status {
		case corev1.CompletenessStatus_COMPLETENESS_STATUS_PARTIAL:
			statusStr = "PARTIAL"
		case corev1.CompletenessStatus_COMPLETENESS_STATUS_DEGRADED:
			statusStr = "DEGRADED"
		}
		completeness = &AnalysisCompleteness{
			IsComplete: resp.Completeness.IsComplete,
			Status:     statusStr,
			Reasons:    copyStringSlice(resp.Completeness.Reasons),
		}
	}

	var diagnostics []DiagnosticItem
	if len(resp.Diagnostics) > 0 {
		diagnostics = make([]DiagnosticItem, 0, len(resp.Diagnostics))
		for _, d := range resp.Diagnostics {
			if d == nil {
				continue
			}
			sevStr := "WARNING"
			switch d.Severity {
			case corev1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_INFO:
				sevStr = "INFO"
			case corev1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_WARNING:
				sevStr = "WARNING"
			case corev1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_ERROR:
				sevStr = "ERROR"
			}

			relStr := "WORKSPACE"
			switch d.Relevance {
			case corev1.DiagnosticRelevance_DIAGNOSTIC_RELEVANCE_TARGET:
				relStr = "TARGET"
			case corev1.DiagnosticRelevance_DIAGNOSTIC_RELEVANCE_BLAST_RADIUS:
				relStr = "BLAST_RADIUS"
			case corev1.DiagnosticRelevance_DIAGNOSTIC_RELEVANCE_WORKSPACE:
				relStr = "WORKSPACE"
			}

			impactStr := "PARTIAL"
			switch d.CompletenessImpact {
			case corev1.CompletenessStatus_COMPLETENESS_STATUS_COMPLETE:
				impactStr = "COMPLETE"
			case corev1.CompletenessStatus_COMPLETENESS_STATUS_PARTIAL:
				impactStr = "PARTIAL"
			case corev1.CompletenessStatus_COMPLETENESS_STATUS_DEGRADED:
				impactStr = "DEGRADED"
			}

			item := DiagnosticItem{
				Code:               d.Code,
				Severity:           sevStr,
				Message:            d.Message,
				UnresolvedEndpoint: d.UnresolvedEndpoint,
				Scope:              d.Scope,
				EvidenceIDs:        copyStringSlice(d.SupportingEvidenceIds),
				Relevance:          relStr,
				CompletenessImpact: impactStr,
				FirstObservedAt:    d.FirstObservedAt,
				LastObservedAt:     d.LastObservedAt,
			}
			if d.Target != nil {
				t := transformResourceIdentity(d.Target)
				item.Subject = &t
			}
			if d.ReferencedResource != nil {
				r := transformResourceIdentity(d.ReferencedResource)
				item.ReferencedResource = &r
			}
			diagnostics = append(diagnostics, item)
		}
	}

	return &ImpactAnswer{
		Target:            target,
		Summary:           summary,
		ImpactedResources: impactedResources,
		Relationships:     relationships,
		Paths:             paths,
		Evidence:          evidence,
		ExplanationFacts:  explanationFacts,
		ChangeAssessment:  changeAssessment,
		Completeness:      completeness,
		Diagnostics:       diagnostics,
	}
}

func transformResourceIdentity(proto *corev1.ResourceIdentity) ResourceIdentity {
	if proto == nil {
		return ResourceIdentity{}
	}
	return ResourceIdentity{
		Provider:     proto.Provider,
		ResourceType: proto.ResourceType,
		ProviderID:   proto.ProviderId,
	}
}

func transformRelationship(proto *corev1.Relationship) Relationship {
	if proto == nil {
		return Relationship{}
	}
	return Relationship{
		Source:   transformResourceIdentity(proto.Source),
		Target:   transformResourceIdentity(proto.Target),
		Kind:     proto.Kind,
		Category: proto.Category,
	}
}

func transformAnswerRelationship(proto *corev1.AnswerRelationship) AnswerRelationship {
	if proto == nil {
		return AnswerRelationship{
			EvidenceIDs: make([]string, 0),
		}
	}
	return AnswerRelationship{
		Relationship: transformRelationship(proto.Relationship),
		State:        strings.ToLower(proto.State),
		EvidenceIDs:  copyStringSlice(proto.EvidenceIds),
	}
}

func transformImpactPath(proto *corev1.ImpactPath) ImpactPath {
	if proto == nil {
		return ImpactPath{
			Resources:     make([]ResourceIdentity, 0),
			Relationships: make([]Relationship, 0),
		}
	}

	res := make([]ResourceIdentity, 0, len(proto.Resources))
	for _, r := range proto.Resources {
		if r != nil {
			res = append(res, transformResourceIdentity(r))
		}
	}

	rels := make([]Relationship, 0, len(proto.Relationships))
	for _, r := range proto.Relationships {
		if r != nil {
			rels = append(rels, transformRelationship(r))
		}
	}

	return ImpactPath{
		Resources:     res,
		Relationships: rels,
	}
}

func copyStringSlice(src []string) []string {
	if src == nil {
		return make([]string, 0)
	}
	out := make([]string, len(src))
	copy(out, src)
	return out
}
