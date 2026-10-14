package k8s

import (
	"errors"
	"testing"

	"github.com/aaradhychinche-alt/WhatBreaks/internal/state"
)

func TestIdentity_Namespaced(t *testing.T) {
	clusterID := "prod-us-east-1"
	id := BuildIdentity(clusterID, TypeDeployment, "default", "frontend")

	if id.Provider != ProviderKubernetes {
		t.Errorf("expected provider %q, got %q", ProviderKubernetes, id.Provider)
	}
	if id.ResourceType != "deployment" {
		t.Errorf("expected resource_type %q, got %q", "deployment", id.ResourceType)
	}
	expectedProviderID := "prod-us-east-1/default/frontend"
	if id.ProviderId != expectedProviderID {
		t.Errorf("expected provider_id %q, got %q", expectedProviderID, id.ProviderId)
	}

	// Parse back
	parsedCluster, parsedNS, parsedName, err := ParseProviderID(id.ProviderId, false)
	if err != nil {
		t.Fatalf("failed to parse provider_id: %v", err)
	}
	if parsedCluster != clusterID || parsedNS != "default" || parsedName != "frontend" {
		t.Errorf("parsed mismatch: cluster=%q, ns=%q, name=%q", parsedCluster, parsedNS, parsedName)
	}
}

func TestIdentity_ClusterScoped(t *testing.T) {
	clusterID := "prod-us-east-1"
	id := BuildIdentity(clusterID, TypeNode, "", "node-1")

	if id.Provider != ProviderKubernetes {
		t.Errorf("expected provider %q, got %q", ProviderKubernetes, id.Provider)
	}
	if id.ResourceType != "node" {
		t.Errorf("expected resource_type %q, got %q", "node", id.ResourceType)
	}
	expectedProviderID := "prod-us-east-1/node-1"
	if id.ProviderId != expectedProviderID {
		t.Errorf("expected provider_id %q, got %q", expectedProviderID, id.ProviderId)
	}

	// Parse back
	parsedCluster, _, parsedName, err := ParseProviderID(id.ProviderId, true)
	if err != nil {
		t.Fatalf("failed to parse provider_id: %v", err)
	}
	if parsedCluster != clusterID || parsedName != "node-1" {
		t.Errorf("parsed mismatch: cluster=%q, name=%q", parsedCluster, parsedName)
	}
}

func TestIdentity_NoClusterID(t *testing.T) {
	// Construction with empty cluster ID must not silently proceed or create unqualified identities
	idNamespaced := BuildIdentity("", TypePod, "payments", "api-123")
	if idNamespaced.ProviderId != "" {
		t.Errorf("expected empty provider_id when cluster_id is missing, got %q", idNamespaced.ProviderId)
	}

	idCluster := BuildIdentity("", TypeNamespace, "", "kube-system")
	if idCluster.ProviderId != "" {
		t.Errorf("expected empty provider_id when cluster_id is missing, got %q", idCluster.ProviderId)
	}

	// Parsing legacy/unqualified IDs without cluster prefix must return ErrUnqualifiedProviderID
	_, _, _, err := ParseProviderID("payments/api-123", false)
	if !errors.Is(err, state.ErrUnqualifiedProviderID) {
		t.Errorf("expected ErrUnqualifiedProviderID for 2-segment namespaced ID, got %v", err)
	}

	_, _, _, err = ParseProviderID("kube-system", true)
	if !errors.Is(err, state.ErrUnqualifiedProviderID) {
		t.Errorf("expected ErrUnqualifiedProviderID for 1-segment cluster-scoped ID, got %v", err)
	}
}

func TestIdentity_MultiClusterIsolation(t *testing.T) {
	// Two resources with identical namespace and name in different clusters MUST have distinct ProviderIDs
	id1 := BuildIdentity("cluster-a", TypeService, "default", "database")
	id2 := BuildIdentity("cluster-b", TypeService, "default", "database")

	if id1.ProviderId == id2.ProviderId {
		t.Errorf("collision detected across clusters: %s == %s", id1.ProviderId, id2.ProviderId)
	}
}

func TestIdentity_BuildSource(t *testing.T) {
	src := BuildSource("")
	if src.Provider != ProviderKubernetes || src.Collector != DefaultCollectorID {
		t.Errorf("unexpected default source: %+v", src)
	}

	custom := BuildSource("custom-collector")
	if custom.Collector != "custom-collector" {
		t.Errorf("expected custom collector, got %+v", custom)
	}
}
