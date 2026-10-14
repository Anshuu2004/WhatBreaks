package k8s

import (
	"strings"

	corev1 "github.com/aaradhychinche-alt/WhatBreaks/gen/go/wb/core/v1"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/state"
)

const (
	// ProviderKubernetes is the canonical provider string for WhatBreaks Kubernetes resources.
	ProviderKubernetes = "kubernetes"

	// DefaultCollectorID is the standard collector provenance name.
	DefaultCollectorID = "k8s-collector"
)

// Supported Resource Types
const (
	TypeNamespace             = state.TypeNamespace
	TypeNode                  = state.TypeNode
	TypeDeployment            = state.TypeDeployment
	TypeReplicaSet            = state.TypeReplicaSet
	TypeStatefulSet           = state.TypeStatefulSet
	TypeDaemonSet             = state.TypeDaemonSet
	TypePod                   = state.TypePod
	TypeService               = state.TypeService
	TypeIngress               = state.TypeIngress
	TypeConfigMap             = state.TypeConfigMap
	TypeSecret                = state.TypeSecret
	TypePersistentVolume      = state.TypePersistentVolume
	TypePersistentVolumeClaim = state.TypePersistentVolumeClaim
	TypeServiceAccount        = state.TypeServiceAccount
	TypeEndpointSlice         = state.TypeEndpointSlice
)

// IsClusterScoped reports whether a Kubernetes resource type is scoped to the cluster rather than a namespace.
func IsClusterScoped(resourceType string) bool {
	return state.IsClusterScoped(resourceType)
}

// BuildIdentity constructs a canonical ResourceIdentity adhering to the WhatBreaks model.
// For cluster-scoped resources: provider_id = "<cluster_id>/<name>".
// For namespaced resources: provider_id = "<cluster_id>/<namespace>/<name>".
// It uses state.BuildKubernetesProviderID to ensure a single consistent implementation.
func BuildIdentity(clusterID, resourceType, namespace, name string) *corev1.ResourceIdentity {
	resourceType = strings.ToLower(resourceType)
	providerID, err := state.BuildKubernetesProviderID(clusterID, resourceType, namespace, name)
	if err != nil {
		// Never silently convert a missing or invalid cluster ID into an unqualified identity
		return &corev1.ResourceIdentity{
			Provider:     ProviderKubernetes,
			ResourceType: resourceType,
			ProviderId:   "",
		}
	}
	return &corev1.ResourceIdentity{
		Provider:     ProviderKubernetes,
		ResourceType: resourceType,
		ProviderId:   providerID,
	}
}

// ParseProviderID extracts cluster, namespace, and resource name components from a provider_id.
// It delegates to state.ParseKubernetesProviderID to ensure a single consistent implementation.
func ParseProviderID(providerID string, isClusterScoped bool) (clusterID, namespace, name string, err error) {
	return state.ParseKubernetesProviderID(providerID, isClusterScoped)
}

// BuildSource constructs an EvidenceSource record for this collector.
func BuildSource(collectorID string) *corev1.EvidenceSource {
	if collectorID == "" {
		collectorID = DefaultCollectorID
	}
	return &corev1.EvidenceSource{
		Provider:  ProviderKubernetes,
		Collector: collectorID,
	}
}
