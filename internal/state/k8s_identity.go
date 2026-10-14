package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	// ErrMalformedProviderID indicates a Kubernetes provider_id has an invalid format, empty segments, or whitespace padding.
	ErrMalformedProviderID = errors.New("malformed kubernetes provider_id")

	// ErrUnqualifiedProviderID indicates a Kubernetes provider_id is missing its cluster identifier prefix.
	ErrUnqualifiedProviderID = errors.New("unqualified kubernetes provider_id: missing cluster identifier")

	// ErrLegacyNamespacedID indicates an unqualified namespaced identity (<namespace>/<name>).
	ErrLegacyNamespacedID = fmt.Errorf("%w: legacy namespaced kubernetes provider_id (<namespace>/<name>)", ErrUnqualifiedProviderID)

	// ErrLegacyClusterScopedID indicates an unqualified cluster-scoped identity (<name>).
	ErrLegacyClusterScopedID = fmt.Errorf("%w: legacy cluster-scoped kubernetes provider_id (<name>)", ErrUnqualifiedProviderID)

	// ErrAmbiguousResourceScope indicates a 2-segment identity (<segment1>/<segment2>) for an unknown or custom resource type where scope is not authoritative.
	ErrAmbiguousResourceScope = errors.New("ambiguous kubernetes resource scope: cannot determine whether 2-segment identity is canonical cluster-scoped or legacy namespaced")

	// ErrClusterMismatch indicates a Kubernetes resource identity does not belong to the expected batch cluster boundary.
	ErrClusterMismatch = errors.New("kubernetes resource cluster identifier does not match batch cluster boundary")

	// ErrCrossClusterRelationship indicates a relationship between different clusters without explicit multi-cluster support.
	ErrCrossClusterRelationship = errors.New("cross-cluster kubernetes relationship endpoints detected without explicit multi-cluster link semantics")

	// ErrLegacyStateIncompatible indicates a workspace contains legacy or un-migrated Kubernetes state that prevents safe graph materialization and impact analysis.
	ErrLegacyStateIncompatible = errors.New("incompatible legacy kubernetes state detected in workspace: run administrative recovery before materialization or analysis")

	// ErrUnauthorizedDestructiveAction indicates an operator attempted a recovery operation without explicit authorization.
	ErrUnauthorizedDestructiveAction = errors.New("recovery requires explicit operator authorization (AuthorizeDestructive=true)")

	// ErrUnauthenticatedOperator indicates an unauthenticated caller attempted an administrative recovery operation.
	ErrUnauthenticatedOperator = errors.New("recovery requires an authenticated operator identity")

	// ErrForbiddenOperator indicates an authenticated user lacks permissions to execute administrative recovery.
	ErrForbiddenOperator = errors.New("operator is not authorized to perform destructive recovery")
)

// Supported canonical Kubernetes resource types
const (
	TypeNamespace             = "namespace"
	TypeNode                  = "node"
	TypeDeployment            = "deployment"
	TypeReplicaSet            = "replicaset"
	TypeStatefulSet           = "statefulset"
	TypeDaemonSet             = "daemonset"
	TypePod                   = "pod"
	TypeService               = "service"
	TypeIngress               = "ingress"
	TypeConfigMap             = "configmap"
	TypeSecret                = "secret"
	TypePersistentVolume      = "persistentvolume"
	TypePersistentVolumeClaim = "persistentvolumeclaim"
	TypeServiceAccount        = "serviceaccount"
	TypeEndpointSlice         = "endpointslice"
)

// ResourceScope represents the authoritative scope of a Kubernetes resource.
type ResourceScope int

const (
	// ScopeUnknown indicates a resource type whose scoping is not authoritatively known (e.g. custom resource or unlisted extension).
	ScopeUnknown ResourceScope = iota
	// ScopeNamespaced indicates a resource scoped to a Kubernetes namespace.
	ScopeNamespaced
	// ScopeCluster indicates a cluster-scoped resource without a namespace.
	ScopeCluster
)

var (
	customScopesMu sync.RWMutex
	customScopes   = make(map[string]ResourceScope)
)

// RegisterResourceScope dynamically registers the authoritative scope for a custom Kubernetes resource type (e.g. CRD).
func RegisterResourceScope(resourceType string, scope ResourceScope) {
	k := strings.ToLower(strings.TrimSpace(resourceType))
	if k == "" {
		return
	}
	customScopesMu.Lock()
	defer customScopesMu.Unlock()
	customScopes[k] = scope
}

// ResetCustomResourceScopes resets all dynamically registered resource scopes (useful for test isolation).
func ResetCustomResourceScopes() {
	customScopesMu.Lock()
	defer customScopesMu.Unlock()
	customScopes = make(map[string]ResourceScope)
}

// LookupResourceScope reports the authoritative scoping for a resource type.
func LookupResourceScope(resourceType string) ResourceScope {
	normalized := strings.ToLower(strings.TrimSpace(resourceType))
	if normalized == "" {
		return ScopeUnknown
	}

	customScopesMu.RLock()
	if scope, ok := customScopes[normalized]; ok {
		customScopesMu.RUnlock()
		return scope
	}
	customScopesMu.RUnlock()

	switch normalized {
	case TypeNamespace, TypeNode, TypePersistentVolume, "clusterrole", "clusterrolebinding", "storageclass":
		return ScopeCluster
	case TypeDeployment, TypeReplicaSet, TypeStatefulSet, TypeDaemonSet,
		TypePod, TypeService, TypeIngress, TypeConfigMap, TypeSecret,
		TypePersistentVolumeClaim, TypeServiceAccount, TypeEndpointSlice,
		"role", "rolebinding":
		return ScopeNamespaced
	default:
		return ScopeUnknown
	}
}

// IsClusterScoped reports whether a Kubernetes resource type is scoped to the cluster rather than a namespace.
func IsClusterScoped(resourceType string) bool {
	return LookupResourceScope(resourceType) == ScopeCluster
}

// IsKubernetesProvider reports whether a provider identifier represents Kubernetes.
func IsKubernetesProvider(provider string) bool {
	p := strings.ToLower(strings.TrimSpace(provider))
	return p == "kubernetes" || p == "k8s"
}

// ClassificationKind categorizes the nature of a Kubernetes provider_id.
type ClassificationKind int

const (
	ClassificationUnknown ClassificationKind = iota
	ClassificationCanonicalNamespaced
	ClassificationCanonicalClusterScoped
	ClassificationLegacyNamespaced
	ClassificationLegacyClusterScoped
	ClassificationAmbiguous
	ClassificationMalformed
)

func (k ClassificationKind) String() string {
	switch k {
	case ClassificationCanonicalNamespaced:
		return "CANONICAL_NAMESPACED"
	case ClassificationCanonicalClusterScoped:
		return "CANONICAL_CLUSTER_SCOPED"
	case ClassificationLegacyNamespaced:
		return "LEGACY_NAMESPACED"
	case ClassificationLegacyClusterScoped:
		return "LEGACY_CLUSTER_SCOPED"
	case ClassificationAmbiguous:
		return "AMBIGUOUS"
	case ClassificationMalformed:
		return "MALFORMED"
	default:
		return "UNKNOWN"
	}
}

// IdentityClassification holds the detailed classification of a Kubernetes provider_id.
type IdentityClassification struct {
	Kind        ClassificationKind
	Scope       ResourceScope
	Cluster     string
	Namespace   string
	Name        string
	IsLegacy    bool
	IsAmbiguous bool
	IsCanonical bool
}

func (c IdentityClassification) String() string {
	return c.Kind.String()
}

// ClassifyKubernetesProviderID inspects resourceType and raw providerID using authoritative resource scoping.
// It explicitly distinguishes canonical, legacy namespaced, legacy cluster-scoped, ambiguous, and malformed identities.
// It NEVER classifies on slash count alone and never silently treats legacy namespaced IDs as cluster-scoped.
// Well-formed provider IDs (even if legacy or ambiguous) return a classification and nil error.
// Only malformed provider IDs return ErrMalformedProviderID.
func ClassifyKubernetesProviderID(resourceType, providerID string) (IdentityClassification, error) {
	if providerID == "" {
		return IdentityClassification{Kind: ClassificationMalformed}, fmt.Errorf("%w: empty provider_id", ErrMalformedProviderID)
	}

	trimmed := strings.TrimSpace(providerID)
	if trimmed != providerID {
		return IdentityClassification{Kind: ClassificationMalformed}, fmt.Errorf("%w: provider_id %q has leading or trailing whitespace", ErrMalformedProviderID, providerID)
	}

	parts := strings.Split(providerID, "/")
	for _, p := range parts {
		if p == "" {
			return IdentityClassification{Kind: ClassificationMalformed}, fmt.Errorf("%w: empty segment in %q", ErrMalformedProviderID, providerID)
		}
		if strings.ContainsAny(p, " \t\r\n") {
			return IdentityClassification{Kind: ClassificationMalformed}, fmt.Errorf("%w: segment %q in %q contains whitespace", ErrMalformedProviderID, p, providerID)
		}
	}

	scope := LookupResourceScope(resourceType)
	switch scope {
	case ScopeCluster:
		switch len(parts) {
		case 1:
			return IdentityClassification{
				Kind:     ClassificationLegacyClusterScoped,
				Scope:    ScopeCluster,
				Name:     parts[0],
				IsLegacy: true,
			}, nil
		case 2:
			return IdentityClassification{
				Kind:        ClassificationCanonicalClusterScoped,
				Scope:       ScopeCluster,
				Cluster:     parts[0],
				Name:        parts[1],
				IsCanonical: true,
			}, nil
		default:
			return IdentityClassification{Kind: ClassificationMalformed}, fmt.Errorf("%w: cluster-scoped resource %q has %d segments, expected 2 (<cluster_id>/<name>)", ErrMalformedProviderID, providerID, len(parts))
		}

	case ScopeNamespaced:
		switch len(parts) {
		case 1:
			return IdentityClassification{Kind: ClassificationMalformed}, fmt.Errorf("%w: namespaced resource %q has only 1 segment", ErrMalformedProviderID, providerID)
		case 2:
			return IdentityClassification{
				Kind:      ClassificationLegacyNamespaced,
				Scope:     ScopeNamespaced,
				Namespace: parts[0],
				Name:      parts[1],
				IsLegacy:  true,
			}, nil
		case 3:
			return IdentityClassification{
				Kind:        ClassificationCanonicalNamespaced,
				Scope:       ScopeNamespaced,
				Cluster:     parts[0],
				Namespace:   parts[1],
				Name:        parts[2],
				IsCanonical: true,
			}, nil
		default:
			return IdentityClassification{Kind: ClassificationMalformed}, fmt.Errorf("%w: namespaced resource %q has %d segments, expected 3 (<cluster_id>/<namespace>/<name>)", ErrMalformedProviderID, providerID, len(parts))
		}

	default: // ScopeUnknown
		switch len(parts) {
		case 1:
			return IdentityClassification{
				Kind:     ClassificationLegacyClusterScoped,
				Scope:    ScopeUnknown,
				Name:     parts[0],
				IsLegacy: true,
			}, nil
		case 2:
			// Highly ambiguous: cannot know whether parts[0] is a cluster_id or a namespace without authoritative scoping
			return IdentityClassification{
				Kind:        ClassificationAmbiguous,
				Scope:       ScopeUnknown,
				IsAmbiguous: true,
			}, nil
		case 3:
			return IdentityClassification{
				Kind:        ClassificationCanonicalNamespaced,
				Scope:       ScopeUnknown,
				Cluster:     parts[0],
				Namespace:   parts[1],
				Name:        parts[2],
				IsCanonical: true,
			}, nil
		default:
			return IdentityClassification{Kind: ClassificationMalformed}, fmt.Errorf("%w: resource %q has %d segments", ErrMalformedProviderID, providerID, len(parts))
		}
	}
}

// BuildKubernetesProviderID constructs a canonical cluster-qualified provider_id.
// For cluster-scoped resources: <cluster_id>/<name> (exactly 2 segments).
// For namespaced resources: <cluster_id>/<namespace>/<name> (exactly 3 segments).
func BuildKubernetesProviderID(clusterID, resourceType, namespace, name string) (string, error) {
	clusterID = strings.TrimSpace(clusterID)
	name = strings.TrimSpace(name)
	namespace = strings.TrimSpace(namespace)

	if clusterID == "" {
		return "", fmt.Errorf("%w: cluster_id cannot be empty", ErrUnqualifiedProviderID)
	}
	if strings.Contains(clusterID, "/") {
		return "", fmt.Errorf("%w: cluster_id %q cannot contain slashes", ErrMalformedProviderID, clusterID)
	}

	if name == "" {
		return "", fmt.Errorf("%w: resource name cannot be empty", ErrMalformedProviderID)
	}
	if strings.Contains(name, "/") {
		return "", fmt.Errorf("%w: resource name %q cannot contain slashes", ErrMalformedProviderID, name)
	}

	if IsClusterScoped(resourceType) || namespace == "" {
		return fmt.Sprintf("%s/%s", clusterID, name), nil
	}

	if strings.Contains(namespace, "/") {
		return "", fmt.Errorf("%w: namespace %q cannot contain slashes", ErrMalformedProviderID, namespace)
	}

	return fmt.Sprintf("%s/%s/%s", clusterID, namespace, name), nil
}

// ParseKubernetesProviderID extracts cluster, namespace, and resource name components from a provider_id.
// It enforces canonical formatting:
// - Cluster-scoped resources require exactly 2 non-empty segments (<cluster_id>/<name>).
// - Namespaced resources require exactly 3 non-empty segments (<cluster_id>/<namespace>/<name>).
// It returns ErrUnqualifiedProviderID (or wrapped ErrLegacyNamespacedID / ErrLegacyClusterScopedID) when segments are missing.
// It returns ErrMalformedProviderID for segment count or formatting violations.
func ParseKubernetesProviderID(providerID string, isClusterScoped bool) (clusterID, namespace, name string, err error) {
	if providerID == "" {
		return "", "", "", fmt.Errorf("%w: empty provider_id", ErrMalformedProviderID)
	}

	trimmed := strings.TrimSpace(providerID)
	if trimmed != providerID {
		return "", "", "", fmt.Errorf("%w: provider_id %q has whitespace padding", ErrMalformedProviderID, providerID)
	}

	parts := strings.Split(providerID, "/")
	for _, p := range parts {
		if p == "" || strings.TrimSpace(p) != p {
			return "", "", "", fmt.Errorf("%w: empty or whitespace segment in %q", ErrMalformedProviderID, providerID)
		}
	}

	if isClusterScoped {
		switch len(parts) {
		case 1:
			return "", "", parts[0], fmt.Errorf("%w: cluster-scoped resource %q missing cluster prefix (<cluster_id>/<name>)", ErrLegacyClusterScopedID, providerID)
		case 2:
			return parts[0], "", parts[1], nil
		default:
			return "", "", "", fmt.Errorf("%w: cluster-scoped resource %q has %d segments, expected 2 (<cluster_id>/<name>)", ErrMalformedProviderID, providerID, len(parts))
		}
	}

	switch len(parts) {
	case 1:
		return "", "", parts[0], fmt.Errorf("%w: namespaced resource %q missing cluster prefix", ErrMalformedProviderID, providerID)
	case 2:
		return "", parts[0], parts[1], fmt.Errorf("%w: namespaced resource %q missing cluster prefix (<cluster_id>/<namespace>/<name>)", ErrLegacyNamespacedID, providerID)
	case 3:
		return parts[0], parts[1], parts[2], nil
	default:
		return "", "", "", fmt.Errorf("%w: namespaced resource %q has %d segments, expected 3 (<cluster_id>/<namespace>/<name>)", ErrMalformedProviderID, providerID, len(parts))
	}
}

// ValidateKubernetesResourceIdentity validates that a ResourceIdentity conforms to the canonical
// Kubernetes identity contract and belongs strictly to expectedClusterID.
// For non-Kubernetes providers, it returns nil immediately (preserving provider semantics).
func ValidateKubernetesResourceIdentity(expectedClusterID string, res ResourceIdentity) error {
	if !IsKubernetesProvider(res.Provider) {
		return nil
	}

	expectedClusterID = strings.TrimSpace(expectedClusterID)
	if expectedClusterID == "" {
		return fmt.Errorf("kubernetes observation batch missing required cluster ID")
	}

	clusterID, _, _, err := ParseKubernetesProviderID(res.ProviderID, IsClusterScoped(res.ResourceType))
	if err != nil {
		return fmt.Errorf("invalid kubernetes identity %s for resource type %s: %w", res.ProviderID, res.ResourceType, err)
	}

	if clusterID != expectedClusterID {
		return fmt.Errorf("%w: identity cluster %q does not match batch cluster boundary %q (for %s)",
			ErrClusterMismatch, clusterID, expectedClusterID, res.String())
	}

	return nil
}

// ValidatePersistedKubernetesIdentity validates that a persisted Kubernetes ResourceIdentity conforms
// to the canonical specification (<cluster_id>/<namespace>/<name> or <cluster_id>/<name>).
// For non-Kubernetes providers, it returns nil immediately (preserving other providers' semantics).
func ValidatePersistedKubernetesIdentity(res ResourceIdentity) error {
	if !IsKubernetesProvider(res.Provider) {
		return nil
	}
	cls, err := ClassifyKubernetesProviderID(res.ResourceType, res.ProviderID)
	if err != nil {
		return err
	}
	if cls.IsLegacy {
		if cls.Scope == ScopeCluster || (cls.Namespace == "" && cls.Scope != ScopeNamespaced) {
			return fmt.Errorf("%w: %q", ErrLegacyClusterScopedID, res.ProviderID)
		}
		return fmt.Errorf("%w: %q", ErrLegacyNamespacedID, res.ProviderID)
	}
	if cls.IsAmbiguous {
		return fmt.Errorf("%w: resource %s with ID %q cannot be classified without authoritative resource scope", ErrAmbiguousResourceScope, res.ResourceType, res.ProviderID)
	}
	return nil
}

// StateIntegrityViolation describes an integrity violation found in persisted state.
type StateIntegrityViolation struct {
	Category string // "resource", "relationship", "evidence", "provenance", "diagnostic"
	Identity string
	Reason   error
}

func (v StateIntegrityViolation) String() string {
	return fmt.Sprintf("[%s] %s: %v", v.Category, v.Identity, v.Reason)
}

// ValidateAssembledStateIntegrity inspects all identity-bearing records in an AssembledState.
// It verifies that all Kubernetes records are canonical, detects any legacy or ambiguous identities,
// detects mixed legacy/canonical state, and prevents partitioned graphs.
func ValidateAssembledStateIntegrity(state *AssembledState) error {
	if state == nil {
		return nil
	}

	var violations []StateIntegrityViolation

	// 1. Validate directly observed resources
	for _, res := range state.Resources {
		if IsKubernetesProvider(res.Identity.Provider) {
			if err := ValidatePersistedKubernetesIdentity(res.Identity); err != nil {
				violations = append(violations, StateIntegrityViolation{
					Category: "resource",
					Identity: res.Identity.String(),
					Reason:   err,
				})
			}
		}
	}

	// 2. Validate relationships and cross-cluster integrity
	for _, rel := range state.Relationships {
		var srcCluster, tgtCluster string
		if IsKubernetesProvider(rel.Source.Provider) {
			if err := ValidatePersistedKubernetesIdentity(rel.Source); err != nil {
				violations = append(violations, StateIntegrityViolation{
					Category: "relationship.source",
					Identity: rel.Key().String(),
					Reason:   err,
				})
			} else {
				srcCluster, _, _, _ = ParseKubernetesProviderID(rel.Source.ProviderID, IsClusterScoped(rel.Source.ResourceType))
			}
		}
		if IsKubernetesProvider(rel.Target.Provider) {
			if err := ValidatePersistedKubernetesIdentity(rel.Target); err != nil {
				violations = append(violations, StateIntegrityViolation{
					Category: "relationship.target",
					Identity: rel.Key().String(),
					Reason:   err,
				})
			} else {
				tgtCluster, _, _, _ = ParseKubernetesProviderID(rel.Target.ProviderID, IsClusterScoped(rel.Target.ResourceType))
			}
		}

		if srcCluster != "" && tgtCluster != "" && srcCluster != tgtCluster {
			violations = append(violations, StateIntegrityViolation{
				Category: "relationship.cross_cluster",
				Identity: rel.Key().String(),
				Reason:   fmt.Errorf("%w: source cluster %q != target cluster %q", ErrCrossClusterRelationship, srcCluster, tgtCluster),
			})
		}
	}

	// 3. Validate active diagnostics
	for _, diag := range state.Diagnostics {
		if diag.Subject != nil && IsKubernetesProvider(diag.Subject.Provider) {
			if err := ValidatePersistedKubernetesIdentity(*diag.Subject); err != nil {
				violations = append(violations, StateIntegrityViolation{
					Category: "diagnostic.subject",
					Identity: diag.NaturalKey(),
					Reason:   err,
				})
			}
		}
		if diag.ReferencedResource != nil && IsKubernetesProvider(diag.ReferencedResource.Provider) {
			if err := ValidatePersistedKubernetesIdentity(*diag.ReferencedResource); err != nil {
				violations = append(violations, StateIntegrityViolation{
					Category: "diagnostic.referenced_resource",
					Identity: diag.NaturalKey(),
					Reason:   err,
				})
			}
		}
	}

	if len(violations) > 0 {
		errs := []error{ErrLegacyStateIncompatible}
		for _, v := range violations {
			if v.Reason != nil {
				errs = append(errs, v.Reason)
			}
		}

		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("%v for workspace %q (total violations: %d):\n", ErrLegacyStateIncompatible, state.WorkspaceID, len(violations)))
		for i, v := range violations {
			if i >= 10 {
				sb.WriteString(fmt.Sprintf("  ... and %d more violations\n", len(violations)-10))
				break
			}
			sb.WriteString(fmt.Sprintf("  - %s\n", v.String()))
		}
		sb.WriteString("Remediation: execute administrative legacy recovery (RecoverWorkspaceLegacyState) before materializing state into the core engine.")
		return fmt.Errorf("%w: %s", errors.Join(errs...), sb.String())
	}

	return nil
}

// ValidateKubernetesEvidenceData validates any Kubernetes identities contained in the evidence Data payload
// (e.g. target, owner references). Non-Kubernetes references are preserved intact.
func ValidateKubernetesEvidenceData(expectedClusterID string, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		// Not a JSON object payload, skip
		return nil
	}

	validateObj := func(key string) error {
		val, ok := m[key]
		if !ok || val == nil {
			return nil
		}
		objMap, ok := val.(map[string]any)
		if !ok {
			return nil
		}
		prov, _ := objMap["provider"].(string)
		if !IsKubernetesProvider(prov) {
			return nil
		}
		rt, _ := objMap["resource_type"].(string)
		pid, _ := objMap["provider_id"].(string)
		if strings.TrimSpace(pid) == "" {
			return nil
		}
		id := ResourceIdentity{
			Provider:     prov,
			ResourceType: rt,
			ProviderID:   pid,
		}
		if err := ValidateKubernetesResourceIdentity(expectedClusterID, id); err != nil {
			return fmt.Errorf("evidence data field %q validation failed: %w", key, err)
		}
		return nil
	}

	if err := validateObj("target"); err != nil {
		return err
	}
	if err := validateObj("owner"); err != nil {
		return err
	}
	return nil
}
