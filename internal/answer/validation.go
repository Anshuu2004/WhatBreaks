package answer

import (
	"fmt"
	"strings"

	"github.com/aaradhychinche-alt/WhatBreaks/internal/state"
)

const (
	// MinMaxDepth defines the minimum safe traversal depth.
	MinMaxDepth = 1

	// MaxMaxDepth defines the maximum safe traversal depth.
	MaxMaxDepth = 50

	// DirectionIncoming traverses upstream callers/dependents.
	DirectionIncoming = "incoming"

	// DirectionOutgoing traverses downstream dependencies.
	DirectionOutgoing = "outgoing"
)

// ValidateImpactRequest verifies the syntactic and boundary constraints of an ImpactRequest.
func ValidateImpactRequest(req *ImpactRequest) error {
	if req == nil {
		return &ValidationError{
			Field:   "request",
			Message: "request body cannot be empty",
		}
	}

	if req.Target == nil {
		return &ValidationError{
			Field:   "target",
			Message: "target is required",
		}
	}

	if strings.TrimSpace(req.Target.Provider) == "" {
		return &ValidationError{
			Field:   "target.provider",
			Message: "provider cannot be empty",
		}
	}

	if strings.TrimSpace(req.Target.ResourceType) == "" {
		return &ValidationError{
			Field:   "target.resource_type",
			Message: "resource_type cannot be empty",
		}
	}

	if strings.TrimSpace(req.Target.ProviderID) == "" {
		return &ValidationError{
			Field:   "target.provider_id",
			Message: "provider_id cannot be empty",
		}
	}

	// Validate Kubernetes target identity format: reject legacy/ambiguous targets
	prov := strings.ToLower(strings.TrimSpace(req.Target.Provider))
	if prov == "kubernetes" || prov == "k8s" {
		class, err := state.ClassifyKubernetesProviderID(req.Target.ResourceType, req.Target.ProviderID)
		if err != nil || class.IsLegacy || class.IsAmbiguous {
			reason := "target has ambiguous or legacy kubernetes provider_id"
			if err != nil {
				reason = err.Error()
			} else {
				switch class.Kind {
				case state.ClassificationLegacyNamespaced:
					reason = fmt.Sprintf("target is a legacy namespaced kubernetes identity missing cluster prefix: %q", req.Target.ProviderID)
				case state.ClassificationLegacyClusterScoped:
					reason = fmt.Sprintf("target is a legacy cluster-scoped kubernetes identity missing cluster prefix: %q", req.Target.ProviderID)
				case state.ClassificationAmbiguous:
					reason = fmt.Sprintf("target kubernetes resource type %q has ambiguous scope for identity %q", req.Target.ResourceType, req.Target.ProviderID)
				}
			}
			return &IdentityIntegrityError{
				WorkspaceID: req.WorkspaceID,
				Reason:      reason,
				Err:         ErrIdentityIntegrityRejected,
			}
		}
	}

	dir := strings.ToLower(strings.TrimSpace(req.Direction))
	if dir != DirectionIncoming && dir != DirectionOutgoing {
		return &ValidationError{
			Field:   "direction",
			Message: fmt.Sprintf("invalid direction %q: must be %q or %q", req.Direction, DirectionIncoming, DirectionOutgoing),
		}
	}

	if req.MaxDepth < MinMaxDepth || req.MaxDepth > MaxMaxDepth {
		return &ValidationError{
			Field:   "max_depth",
			Message: fmt.Sprintf("max_depth must be between %d and %d, got %d", MinMaxDepth, MaxMaxDepth, req.MaxDepth),
		}
	}

	if req.ProposedChange != nil {
		raw := strings.TrimSpace(string(req.ProposedChange.ChangeType))
		details := strings.TrimSpace(req.ProposedChange.Details)
		if raw == "" && details == "" {
			// An empty request field preserves legacy impact-analysis behavior
			req.ProposedChange = nil
		} else if raw == "" {
			return &ValidationError{
				Field:   "proposed_change.change_type",
				Message: "change_type is required when proposed_change is provided",
			}
		} else {
			ct := strings.ToUpper(raw)
			switch ChangeType(ct) {
			case ChangeTypeDelete, ChangeTypeUpdate, ChangeTypeScale, ChangeTypeReplace:
				req.ProposedChange.ChangeType = ChangeType(ct)
			default:
				return &ValidationError{
					Field:   "proposed_change.change_type",
					Message: fmt.Sprintf("unsupported change_type %q: must be DELETE, UPDATE, SCALE, or REPLACE", req.ProposedChange.ChangeType),
				}
			}
		}
	}

	return nil
}
