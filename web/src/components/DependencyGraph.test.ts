import test from "node:test";
import assert from "node:assert/strict";
import {
  buildGraph,
  shouldZoomOnWheel,
  shouldActivateNode,
  isImpactPropagating,
} from "../utils/graphBuilder.ts";
import type { ImpactAnswer, DependencyRelationship } from "../types.ts";

function createMockAnswer(partial: Partial<ImpactAnswer> & { target: ImpactAnswer["target"] }): ImpactAnswer {
  return {
    target: partial.target,
    summary: partial.summary ?? {
      impacted_count: partial.impacted_resources?.length ?? 0,
      direct_count: partial.impacted_resources?.filter((r) => r.depth === 1).length ?? 0,
      indirect_count: partial.impacted_resources?.filter((r) => r.depth > 1).length ?? 0,
      max_depth: partial.impacted_resources?.reduce((m, r) => Math.max(m, r.depth), 0) ?? 0,
    },
    impacted_resources: partial.impacted_resources ?? [],
    relationships: partial.relationships ?? [],
    paths: partial.paths ?? [],
    evidence: partial.evidence ?? [],
    explanation_facts: partial.explanation_facts ?? [],
    change_assessment: partial.change_assessment,
    completeness: partial.completeness ?? {
      is_complete: true,
      status: "COMPLETE",
      reasons: [],
    },
    diagnostics: partial.diagnostics,
  };
}

test("Priority 1: Mouse-wheel scrolling does not trigger graph zoom without modifier keys", () => {
  // Normal mouse wheel or trackpad scroll over the graph
  assert.equal(
    shouldZoomOnWheel({ ctrlKey: false, metaKey: false }),
    false,
    "Normal wheel event must return false to preserve normal page scrolling"
  );

  // Opt-in modifier zoom
  assert.equal(
    shouldZoomOnWheel({ ctrlKey: true, metaKey: false }),
    true,
    "Ctrl + Wheel must allow opt-in zoom"
  );
  assert.equal(
    shouldZoomOnWheel({ ctrlKey: false, metaKey: true }),
    true,
    "Meta (Cmd) + Wheel must allow opt-in zoom"
  );
});

test("Priority 1: Pan drag distance threshold prevents accidental node click activation", () => {
  // Clicking with slight jitter (<= 5px) should activate node
  assert.equal(shouldActivateNode(0), true, "Direct click should activate node");
  assert.equal(shouldActivateNode(3), true, "Click with minor jitter should activate node");
  assert.equal(shouldActivateNode(5), true, "Click up to threshold should activate node");

  // Dragging to pan (> 5px) must NOT activate node selection
  assert.equal(shouldActivateNode(6), false, "Dragging past 5px must suppress node activation");
  assert.equal(shouldActivateNode(45), false, "Substantial pan must suppress node activation");
});

test("Priority 2: Investigate graph displays target, impacted, and relevant connected non-impacted topology", () => {
  const answer = createMockAnswer({
    target: {
      provider: "kubernetes",
      resource_type: "service",
      provider_id: "cluster-1/license-system/license-service",
    },
    impacted_resources: [
      {
        resource: {
          provider: "kubernetes",
          resource_type: "ingress",
          provider_id: "cluster-1/license-system/license-ingress",
        },
        depth: 1,
        impact_type: "DIRECT_DEPENDENCY",
        impact_reason: "Ingress forwards traffic to target Service",
      },
    ],
  });

  const allRelationships: DependencyRelationship[] = [
    // 1. Target connected to a non-impacted pod (backend dependency)
    {
      source: {
        provider: "kubernetes",
        resource_type: "service",
        provider_id: "cluster-1/license-system/license-service",
      },
      target: {
        provider: "kubernetes",
        resource_type: "pod",
        provider_id: "cluster-1/license-system/license-pod-1",
      },
      kind: "DEPENDS_ON",
      category: "NETWORK",
      first_observed_at: "2026-10-17T00:00:00Z",
      last_observed_at: "2026-10-17T00:00:00Z",
    },
    // 2. Unrelated resource elsewhere in the cluster (should be bounded out)
    {
      source: {
        provider: "kubernetes",
        resource_type: "deployment",
        provider_id: "cluster-1/kube-system/coredns",
      },
      target: {
        provider: "kubernetes",
        resource_type: "pod",
        provider_id: "cluster-1/kube-system/coredns-pod",
      },
      kind: "OWNS",
      category: "LIFECYCLE",
      first_observed_at: "2026-10-17T00:00:00Z",
      last_observed_at: "2026-10-17T00:00:00Z",
    },
  ];

  const graph = buildGraph(answer, allRelationships);

  // Must include:
  // 1. Target (license-service)
  // 2. Impacted resource (license-ingress)
  // 3. Connected non-impacted resource (license-pod-1)
  // Must NOT include unrelated coredns resources
  const nodeIds = graph.nodes.map((n) => n.providerId);
  assert.ok(nodeIds.includes("cluster-1/license-system/license-service"), "Target must be present");
  assert.ok(nodeIds.includes("cluster-1/license-system/license-ingress"), "Impacted resource must be present");
  assert.ok(nodeIds.includes("cluster-1/license-system/license-pod-1"), "Connected non-impacted resource must be present");
  assert.ok(!nodeIds.includes("cluster-1/kube-system/coredns"), "Unrelated resources must NOT be loaded into graph");

  // Relationships
  assert.equal(graph.edges.length, 1);
  assert.equal(graph.edges[0].kind, "DEPENDS_ON");
  assert.equal(graph.edges[0].isPropagating, true);
});

test("Priority 3: Honest impact-status colors (Red for impacted, Green for confirmed non-impacted, Gray for inconclusive)", () => {
  const completeAnswer = createMockAnswer({
    target: {
      provider: "kubernetes",
      resource_type: "service",
      provider_id: "k8s/default/my-svc",
    },
    impacted_resources: [
      {
        resource: {
          provider: "kubernetes",
          resource_type: "deployment",
          provider_id: "k8s/default/client-deploy",
        },
        depth: 1,
        impact_type: "DIRECT_DEPENDENCY",
      },
    ],
    completeness: {
      is_complete: true,
      status: "COMPLETE",
      reasons: [],
    },
  });

  const relationships: DependencyRelationship[] = [
    {
      source: { provider: "kubernetes", resource_type: "service", provider_id: "k8s/default/my-svc" },
      target: { provider: "kubernetes", resource_type: "pod", provider_id: "k8s/default/backend-pod" },
      kind: "DEPENDS_ON",
      category: "NETWORK",
      first_observed_at: "2026-10-17T00:00:00Z",
      last_observed_at: "2026-10-17T00:00:00Z",
    },
  ];

  // 1. With complete analysis
  const completeGraph = buildGraph(completeAnswer, relationships);
  const targetNode = completeGraph.nodes.find((n) => n.isTarget);
  const impactedNode = completeGraph.nodes.find((n) => n.providerId === "k8s/default/client-deploy");
  const connectedNode = completeGraph.nodes.find((n) => n.providerId === "k8s/default/backend-pod");

  assert.equal(targetNode?.status, "target", "Target must have target status");
  assert.equal(impactedNode?.status, "impacted", "Impacted resource must have impacted status (red)");
  assert.equal(connectedNode?.status, "non_impacted", "Connected resource in complete analysis must have non_impacted status (green)");

  // 2. With incomplete or partial analysis
  const partialAnswer = createMockAnswer({
    ...completeAnswer,
    completeness: {
      is_complete: false,
      status: "PARTIAL",
      reasons: ["Collector timeout during reconciliation"],
    },
  });

  const partialGraph = buildGraph(partialAnswer, relationships);
  const partialConnectedNode = partialGraph.nodes.find((n) => n.providerId === "k8s/default/backend-pod");

  assert.equal(
    partialConnectedNode?.status,
    "inconclusive",
    "Connected resource in incomplete/partial analysis must be inconclusive (gray), NEVER green"
  );
});

test("Priority 3: Non-propagating relationships remain visible without impact propagation", () => {
  // Test propagation distinction
  assert.equal(isImpactPropagating("DEPENDS_ON"), true, "DEPENDS_ON must propagate impact");
  assert.equal(isImpactPropagating("CALLS"), true, "CALLS must propagate impact");
  assert.equal(isImpactPropagating("READS_FROM"), true, "READS_FROM must propagate impact");
  assert.equal(isImpactPropagating("WRITES_TO"), true, "WRITES_TO must propagate impact");

  // Lifecycle & ownership boundaries
  assert.equal(isImpactPropagating("OWNS"), false, "OWNS must NOT be treated as impact propagating");
  assert.equal(isImpactPropagating("RUNS_ON"), false, "RUNS_ON must NOT be treated as impact propagating");
  assert.equal(isImpactPropagating("AUTHORIZES"), false, "AUTHORIZES must NOT be treated as impact propagating");

  const answer = createMockAnswer({
    target: {
      provider: "kubernetes",
      resource_type: "deployment",
      provider_id: "k8s/default/deploy-a",
    },
  });

  const rels: DependencyRelationship[] = [
    {
      source: { provider: "kubernetes", resource_type: "deployment", provider_id: "k8s/default/deploy-a" },
      target: { provider: "kubernetes", resource_type: "replicaset", provider_id: "k8s/default/rs-a" },
      kind: "OWNS",
      category: "LIFECYCLE",
      first_observed_at: "2026-10-17T00:00:00Z",
      last_observed_at: "2026-10-17T00:00:00Z",
    },
  ];

  const graph = buildGraph(answer, rels);
  assert.equal(graph.edges.length, 1);
  assert.equal(graph.edges[0].isPropagating, false, "OWNS edge must have isPropagating=false");
  assert.equal(graph.edges[0].label, "OWNS");
});

test("Priority 4: Cluster isolation prevents cross-cluster resource or edge leaks", () => {
  const answerClusterA = createMockAnswer({
    target: {
      provider: "kubernetes",
      resource_type: "service",
      provider_id: "cluster-alpha/license-system/svc-alpha",
    },
  });

  // Scoped relationships for cluster-alpha (already cluster-scoped by API listDependencies)
  const scopedRelationships: DependencyRelationship[] = [
    {
      source: {
        provider: "kubernetes",
        resource_type: "service",
        provider_id: "cluster-alpha/license-system/svc-alpha",
      },
      target: {
        provider: "kubernetes",
        resource_type: "pod",
        provider_id: "cluster-alpha/license-system/pod-alpha",
      },
      kind: "DEPENDS_ON",
      category: "NETWORK",
      first_observed_at: "2026-10-17T00:00:00Z",
      last_observed_at: "2026-10-17T00:00:00Z",
    },
  ];

  const graph = buildGraph(answerClusterA, scopedRelationships);
  for (const node of graph.nodes) {
    assert.equal(node.cluster, "cluster-alpha", "All nodes must belong strictly to cluster-alpha");
  }
});

test("Priority 4: Empty responses display honest states without fabricating nodes or edges", () => {
  const emptyAnswer = createMockAnswer({
    target: {
      provider: "kubernetes",
      resource_type: "configmap",
      provider_id: "cluster-1/default/standalone-cm",
    },
  });

  const graph = buildGraph(emptyAnswer, []);
  assert.equal(graph.nodes.length, 1, "Graph must only contain the target node when no dependents or connections exist");
  assert.equal(graph.nodes[0].isTarget, true);
  assert.equal(graph.nodes[0].status, "target");
  assert.equal(graph.edges.length, 0, "No edges must be fabricated");
});
