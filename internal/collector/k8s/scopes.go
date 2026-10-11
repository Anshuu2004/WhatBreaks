package k8s

const (
	ScopeDeployments            = "deployments"
	ScopeReplicaSets            = "replicasets"
	ScopeServices               = "services"
	ScopePods                   = "pods"
	ScopeConfigMaps             = "configmaps"
	ScopeSecrets                = "secrets"
	ScopeNamespaces             = "namespaces"
	ScopeNodes                  = "nodes"
	ScopeIngresses              = "ingresses"
	ScopePersistentVolumes      = "persistentvolumes"
	ScopePersistentVolumeClaims = "persistentvolumeclaims"
	ScopeServiceAccounts        = "serviceaccounts"
)

// ScopeRequirement defines whether a resource scope is mandatory for discovery completeness.
type ScopeRequirement int

const (
	// ScopeRequired means discovery collection is incomplete if this scope fails.
	ScopeRequired ScopeRequirement = iota

	// ScopeOptional means discovery collection can proceed to reconciliation if this scope fails.
	ScopeOptional
)

// ScopePolicy defines which resource types are required versus optional for discovery completeness.
type ScopePolicy struct {
	requirements map[string]ScopeRequirement
}

// DefaultScopePolicy returns the authoritative WhatBreaks discovery policy:
// Required: pods, services, deployments, replicasets, configmaps, secrets
// Optional: ingresses, nodes, persistentvolumes, persistentvolumeclaims, serviceaccounts, namespaces
func DefaultScopePolicy() *ScopePolicy {
	return &ScopePolicy{
		requirements: map[string]ScopeRequirement{
			ScopePods:                   ScopeRequired,
			ScopeServices:               ScopeRequired,
			ScopeDeployments:            ScopeRequired,
			ScopeReplicaSets:            ScopeRequired,
			ScopeConfigMaps:             ScopeRequired,
			ScopeSecrets:                ScopeRequired,
			ScopeIngresses:              ScopeOptional,
			ScopeNodes:                  ScopeOptional,
			ScopePersistentVolumes:      ScopeOptional,
			ScopePersistentVolumeClaims: ScopeOptional,
			ScopeServiceAccounts:        ScopeOptional,
			ScopeNamespaces:             ScopeOptional,
		},
	}
}

// IsRequired reports whether the given scope is marked required.
func (p *ScopePolicy) IsRequired(scope string) bool {
	if p == nil || p.requirements == nil {
		return DefaultScopePolicy().IsRequired(scope)
	}
	return p.requirements[scope] == ScopeRequired
}

// SetRequirement overrides the requirement level for a specific scope.
func (p *ScopePolicy) SetRequirement(scope string, req ScopeRequirement) {
	if p.requirements == nil {
		p.requirements = make(map[string]ScopeRequirement)
	}
	p.requirements[scope] = req
}
