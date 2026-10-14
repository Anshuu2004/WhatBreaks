package answer_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/aaradhychinche-alt/WhatBreaks/internal/answer"
)

func TestValidateImpactRequest_ProposedChange(t *testing.T) {
	validTarget := &answer.ResourceIdentity{
		Provider:     "kubernetes",
		ResourceType: "pod",
		ProviderID:   "cluster-1/default/backend-pod",
	}

	tests := []struct {
		name       string
		req        *answer.ImpactRequest
		wantErr    bool
		errField   string
		expectedCT answer.ChangeType
	}{
		{
			name: "no proposed change (backward compatible)",
			req: &answer.ImpactRequest{
				Target:    validTarget,
				Direction: "incoming",
				MaxDepth:  3,
			},
			wantErr: false,
		},
		{
			name: "valid DELETE uppercase",
			req: &answer.ImpactRequest{
				Target:    validTarget,
				Direction: "incoming",
				MaxDepth:  3,
				ProposedChange: &answer.ProposedChange{
					ChangeType: "DELETE",
				},
			},
			wantErr:    false,
			expectedCT: answer.ChangeTypeDelete,
		},
		{
			name: "valid update lowercase normalized",
			req: &answer.ImpactRequest{
				Target:    validTarget,
				Direction: "incoming",
				MaxDepth:  3,
				ProposedChange: &answer.ProposedChange{
					ChangeType: "update",
				},
			},
			wantErr:    false,
			expectedCT: answer.ChangeTypeUpdate,
		},
		{
			name: "valid SCALE mixed case",
			req: &answer.ImpactRequest{
				Target:    validTarget,
				Direction: "incoming",
				MaxDepth:  3,
				ProposedChange: &answer.ProposedChange{
					ChangeType: "Scale",
				},
			},
			wantErr:    false,
			expectedCT: answer.ChangeTypeScale,
		},
		{
			name: "valid REPLACE with details",
			req: &answer.ImpactRequest{
				Target:    validTarget,
				Direction: "incoming",
				MaxDepth:  3,
				ProposedChange: &answer.ProposedChange{
					ChangeType: "REPLACE",
					Details:    "rolling replacement",
				},
			},
			wantErr:    false,
			expectedCT: answer.ChangeTypeReplace,
		},
		{
			name: "empty proposed change struct normalized to nil (backward compatible)",
			req: &answer.ImpactRequest{
				Target:         validTarget,
				Direction:      "incoming",
				MaxDepth:       3,
				ProposedChange: &answer.ProposedChange{},
			},
			wantErr: false,
		},
		{
			name: "empty change type with details rejected",
			req: &answer.ImpactRequest{
				Target:    validTarget,
				Direction: "incoming",
				MaxDepth:  3,
				ProposedChange: &answer.ProposedChange{
					ChangeType: "",
					Details:    "updating database config",
				},
			},
			wantErr:  true,
			errField: "proposed_change.change_type",
		},
		{
			name: "whitespace change type with details rejected",
			req: &answer.ImpactRequest{
				Target:    validTarget,
				Direction: "incoming",
				MaxDepth:  3,
				ProposedChange: &answer.ProposedChange{
					ChangeType: "   ",
					Details:    "updating database config",
				},
			},
			wantErr:  true,
			errField: "proposed_change.change_type",
		},
		{
			name: "unsupported change type REBOOT rejected",
			req: &answer.ImpactRequest{
				Target:    validTarget,
				Direction: "incoming",
				MaxDepth:  3,
				ProposedChange: &answer.ProposedChange{
					ChangeType: "REBOOT",
				},
			},
			wantErr:  true,
			errField: "proposed_change.change_type",
		},
		{
			name: "unsupported change type MIGRATE rejected",
			req: &answer.ImpactRequest{
				Target:    validTarget,
				Direction: "incoming",
				MaxDepth:  3,
				ProposedChange: &answer.ProposedChange{
					ChangeType: "MIGRATE",
				},
			},
			wantErr:  true,
			errField: "proposed_change.change_type",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := answer.ValidateImpactRequest(tc.req)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				vErr, ok := err.(*answer.ValidationError)
				if !ok {
					t.Fatalf("expected ValidationError, got %T (%v)", err, err)
				}
				if vErr.Field != tc.errField {
					t.Errorf("expected error field %q, got %q", tc.errField, vErr.Field)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if tc.expectedCT != "" {
					if tc.req.ProposedChange.ChangeType != tc.expectedCT {
						t.Errorf("expected normalized ChangeType %q, got %q", tc.expectedCT, tc.req.ProposedChange.ChangeType)
					}
				}
			}
		})
	}
}

func TestValidateImpactRequest_RejectLegacyAndAmbiguousTargets(t *testing.T) {
	tests := []struct {
		name         string
		target       *answer.ResourceIdentity
		wantErrMatch string
	}{
		{
			name: "legacy namespaced 2-segment target missing cluster",
			target: &answer.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "pod",
				ProviderID:   "default/payments-api",
			},
			wantErrMatch: "legacy namespaced",
		},
		{
			name: "legacy cluster-scoped 1-segment target missing cluster",
			target: &answer.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "node",
				ProviderID:   "worker-node-1",
			},
			wantErrMatch: "legacy cluster-scoped",
		},
		{
			name: "malformed namespaced 1-segment target",
			target: &answer.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "service",
				ProviderID:   "payment-svc",
			},
			wantErrMatch: "malformed",
		},
		{
			name: "ambiguous custom resource 2-segment target",
			target: &answer.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "virtualservice",
				ProviderID:   "istio-system/vs-main",
			},
			wantErrMatch: "ambiguous",
		},
		{
			name: "canonical namespaced 3-segment target accepted",
			target: &answer.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "deployment",
				ProviderID:   "prod-cluster/default/payments-api",
			},
			wantErrMatch: "",
		},
		{
			name: "canonical cluster-scoped 2-segment target accepted",
			target: &answer.ResourceIdentity{
				Provider:     "kubernetes",
				ResourceType: "node",
				ProviderID:   "prod-cluster/node-1",
			},
			wantErrMatch: "",
		},
		{
			name: "non-k8s provider (aws) unconstrained by k8s rules",
			target: &answer.ResourceIdentity{
				Provider:     "aws",
				ResourceType: "rds",
				ProviderID:   "arn:aws:rds:us-east-1:123456789:db/orders",
			},
			wantErrMatch: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := &answer.ImpactRequest{
				Target:    tc.target,
				Direction: "incoming",
				MaxDepth:  3,
			}
			err := answer.ValidateImpactRequest(req)
			if tc.wantErrMatch != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantErrMatch)
				}
				var iErr *answer.IdentityIntegrityError
				if !errors.As(err, &iErr) {
					t.Fatalf("expected *answer.IdentityIntegrityError, got %T (%v)", err, err)
				}
				if !strings.Contains(strings.ToLower(iErr.Error()), strings.ToLower(tc.wantErrMatch)) {
					t.Errorf("expected error containing %q, got %q", tc.wantErrMatch, iErr.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}
}
