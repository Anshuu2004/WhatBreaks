import React, {
  useRef,
  useState,
  useEffect,
  useCallback,
  useMemo,
} from "react";
import {
  Maximize2,
  Minus,
  Plus,
  Minimize2,
  Play,
  X,
  ArrowRight,
} from "lucide-react";
import type { DependencyRelationship, ResourceItem } from "../types";
import {
  parseProviderID,
  resourceKey,
  typeBadge,
  isImpactPropagating,
} from "../utils/resource";
import { shouldZoomOnWheel, shouldActivateNode } from "../utils/graphBuilder";

// ── Graph Layout Dimensions ──────────────────────────────────────────────────
const NODE_W = 210;
const NODE_H = 58;
const COL_GAP = 130;
const ROW_GAP = 26;
const PADDING = 40;
const ARROW_SIZE = 6;

export interface TopologyNode {
  id: string; // provider/resource_type/provider_id
  provider: string;
  type: string;
  providerId: string;
  cluster: string;
  namespace: string;
  name: string;
  layer: number;
  x: number;
  y: number;
}

export interface TopologyEdge {
  id: string;
  from: string; // source node id
  to: string; // target node id
  kind: string;
  category: string;
  isPropagating: boolean;
  first_observed_at: string;
  last_observed_at: string;
}

export interface TopologyGraphProps {
  relationships: DependencyRelationship[];
  onNavigateToInvestigate: (res: ResourceItem) => void;
  selectedResourceId?: string | null;
}

export const TopologyGraph: React.FC<TopologyGraphProps> = ({
  relationships,
  onNavigateToInvestigate,
  selectedResourceId,
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const [scale, setScale] = useState(0.85);
  const [offset, setOffset] = useState({ x: 20, y: 20 });
  const [dragging, setDragging] = useState(false);
  const [dragStart, setDragStart] = useState({ mx: 0, my: 0, ox: 0, oy: 0 });

  // Selected node for detail drawer
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(selectedResourceId ?? null);
  const [hoveredNodeId, setHoveredNodeId] = useState<string | null>(null);

  // Sync external selection if provided
  useEffect(() => {
    if (selectedResourceId) {
      setSelectedNodeId(selectedResourceId);
    }
  }, [selectedResourceId]);

  // ── Construct Graph Model & Deterministic Layered Coordinates ─────────────
  const { nodes, edges, width, height, nodeMap } = useMemo(() => {
    const nMap = new Map<string, TopologyNode>();
    const eList: TopologyEdge[] = [];
    const adj = new Map<string, Set<string>>();
    const inDegree = new Map<string, number>();

    // 1. Collect all distinct nodes and edges
    relationships.forEach((rel, idx) => {
      const sk = resourceKey(rel.source);
      const tk = resourceKey(rel.target);

      if (!nMap.has(sk)) {
        const parsed = parseProviderID(rel.source.provider_id);
        nMap.set(sk, {
          id: sk,
          provider: rel.source.provider,
          type: rel.source.resource_type,
          providerId: rel.source.provider_id,
          cluster: parsed.cluster,
          namespace: parsed.namespace,
          name: parsed.name,
          layer: 0,
          x: 0,
          y: 0,
        });
      }

      if (!nMap.has(tk)) {
        const parsed = parseProviderID(rel.target.provider_id);
        nMap.set(tk, {
          id: tk,
          provider: rel.target.provider,
          type: rel.target.resource_type,
          providerId: rel.target.provider_id,
          cluster: parsed.cluster,
          namespace: parsed.namespace,
          name: parsed.name,
          layer: 0,
          x: 0,
          y: 0,
        });
      }

      eList.push({
        id: `e-${idx}-${sk}->${tk}`,
        from: sk,
        to: tk,
        kind: rel.kind,
        category: rel.category,
        isPropagating: isImpactPropagating(rel.kind),
        first_observed_at: rel.first_observed_at,
        last_observed_at: rel.last_observed_at,
      });

      if (!adj.has(sk)) adj.set(sk, new Set());
      adj.get(sk)!.add(tk);
      inDegree.set(tk, (inDegree.get(tk) || 0) + 1);
    });

    // 2. Compute topological layers (longest path from in-degree 0 roots)
    const layers = new Map<string, number>();
    for (const [id] of nMap) {
      if ((inDegree.get(id) || 0) === 0) {
        layers.set(id, 0);
      }
    }

    // Propagate layers along directed edges
    let changed = true;
    let iterations = 0;
    while (changed && iterations < 50) {
      changed = false;
      iterations++;
      for (const [u, targets] of adj) {
        const uLayer = layers.get(u);
        if (uLayer !== undefined) {
          for (const v of targets) {
            const currentV = layers.get(v) ?? -1;
            if (currentV < uLayer + 1) {
              layers.set(v, uLayer + 1);
              changed = true;
            }
          }
        }
      }
    }

    // Default any remaining unlayered nodes to 0
    for (const [id] of nMap) {
      if (!layers.has(id)) {
        layers.set(id, 0);
      }
    }

    // Group nodes by layer
    const byLayer = new Map<number, string[]>();
    for (const [id, lyr] of layers) {
      if (!byLayer.has(lyr)) byLayer.set(lyr, []);
      byLayer.get(lyr)!.push(id);
    }

    // Sort nodes within each layer by namespace, then name
    const sortedLayers = Array.from(byLayer.keys()).sort((a, b) => a - b);
    sortedLayers.forEach((lyr) => {
      const ids = byLayer.get(lyr)!;
      ids.sort((a, b) => {
        const na = nMap.get(a)!;
        const nb = nMap.get(b)!;
        if (na.namespace !== nb.namespace) {
          return na.namespace.localeCompare(nb.namespace);
        }
        return na.name.localeCompare(nb.name);
      });

      ids.forEach((id, rowIdx) => {
        const node = nMap.get(id)!;
        node.layer = lyr;
        node.x = PADDING + lyr * (NODE_W + COL_GAP);
        node.y = PADDING + rowIdx * (NODE_H + ROW_GAP);
      });
    });

    const nodeList = Array.from(nMap.values());
    const maxX = nodeList.reduce((m, n) => Math.max(m, n.x + NODE_W), 0) + PADDING;
    const maxY = nodeList.reduce((m, n) => Math.max(m, n.y + NODE_H), 0) + PADDING;

    return {
      nodes: nodeList,
      edges: eList,
      width: Math.max(maxX, 600),
      height: Math.max(maxY, 400),
      nodeMap: nMap,
    };
  }, [relationships]);

  // ── Auto-fit graph into viewport ───────────────────────────────────────────
  const fitToView = useCallback(() => {
    if (!containerRef.current) return;
    const { clientWidth, clientHeight } = containerRef.current;
    if (width === 0 || height === 0 || clientWidth === 0 || clientHeight === 0) return;
    const scaleX = (clientWidth - 40) / width;
    const scaleY = (clientHeight - 40) / height;
    const s = Math.max(0.2, Math.min(scaleX, scaleY, 1.1));
    setScale(s);
    setOffset({
      x: Math.max(20, (clientWidth - width * s) / 2),
      y: Math.max(20, (clientHeight - height * s) / 2),
    });
  }, [width, height]);

  useEffect(() => {
    fitToView();
  }, [fitToView]);

  const dragDistanceRef = useRef(0);
  const isDraggingRef = useRef(false);

  // ── Canvas Dragging (Pan) ──────────────────────────────────────────────────
  const handleMouseDown = (e: React.MouseEvent) => {
    if (e.button !== 0) return;
    setDragging(true);
    isDraggingRef.current = true;
    dragDistanceRef.current = 0;
    setDragStart({ mx: e.clientX, my: e.clientY, ox: offset.x, oy: offset.y });
  };

  const handleMouseMove = useCallback(
    (e: MouseEvent) => {
      if (!isDraggingRef.current) return;
      const dx = e.clientX - dragStart.mx;
      const dy = e.clientY - dragStart.my;
      dragDistanceRef.current += Math.hypot(dx, dy);
      setOffset({
        x: dragStart.ox + dx,
        y: dragStart.oy + dy,
      });
    },
    [dragStart]
  );

  const handleMouseUp = useCallback(() => {
    setDragging(false);
    isDraggingRef.current = false;
  }, []);

  useEffect(() => {
    window.addEventListener("mousemove", handleMouseMove);
    window.addEventListener("mouseup", handleMouseUp);
    return () => {
      window.removeEventListener("mousemove", handleMouseMove);
      window.removeEventListener("mouseup", handleMouseUp);
    };
  }, [handleMouseMove, handleMouseUp]);

  // ── Opt-in Zoom via Wheel (Modifier Key Only) ──────────────────────────────
  const handleWheel = (e: React.WheelEvent) => {
    // Normal wheel/trackpad scrolling scrolls the page.
    // Zooming only occurs when Ctrl or Meta (Cmd on Mac) is intentionally held.
    if (shouldZoomOnWheel(e)) {
      e.preventDefault();
      const delta = e.deltaY > 0 ? 0.9 : 1.1;
      setScale((s) => Math.max(0.15, Math.min(2.2, s * delta)));
    }
  };

  // ── Edge Routing Calculation ───────────────────────────────────────────────
  const computeEdgePath = (from: TopologyNode, to: TopologyNode): string => {
    if (from.x < to.x) {
      // Left-to-right standard directed flow
      const x1 = from.x + NODE_W;
      const y1 = from.y + NODE_H / 2;
      const x2 = to.x;
      const y2 = to.y + NODE_H / 2;
      const cx = (x1 + x2) / 2;
      return `M ${x1} ${y1} C ${cx} ${y1}, ${cx} ${y2}, ${x2 - ARROW_SIZE} ${y2}`;
    }
    if (from.x > to.x) {
      // Right-to-left reverse flow
      const x1 = from.x;
      const y1 = from.y + NODE_H / 2;
      const x2 = to.x + NODE_W;
      const y2 = to.y + NODE_H / 2;
      const cx = (x1 + x2) / 2;
      return `M ${x1} ${y1} C ${cx} ${y1}, ${cx} ${y2}, ${x2 + ARROW_SIZE} ${y2}`;
    }
    // Same-column routing
    const x1 = from.x + NODE_W;
    const y1 = from.y + NODE_H / 2;
    const x2 = to.x + NODE_W;
    const y2 = to.y + NODE_H / 2;
    const loopX = x1 + 32;
    return `M ${x1} ${y1} C ${loopX} ${y1}, ${loopX} ${y2}, ${x2 + ARROW_SIZE} ${y2}`;
  };

  // ── Highlighted Connected Nodes & Edges ─────────────────────────────────────
  const { connectedNodeIds, connectedEdgeIds } = useMemo(() => {
    const activeId = hoveredNodeId || selectedNodeId;
    if (!activeId) {
      return { connectedNodeIds: new Set<string>(), connectedEdgeIds: new Set<string>() };
    }
    const cNodes = new Set<string>([activeId]);
    const cEdges = new Set<string>();

    edges.forEach((e) => {
      if (e.from === activeId || e.to === activeId) {
        cEdges.add(e.id);
        cNodes.add(e.from);
        cNodes.add(e.to);
      }
    });

    return { connectedNodeIds: cNodes, connectedEdgeIds: cEdges };
  }, [hoveredNodeId, selectedNodeId, edges]);

  // Selected node object for drawer
  const activeSelectedNode = selectedNodeId ? nodeMap.get(selectedNodeId) : null;

  // Connected relationships for selected node
  const selectedNodeRelationships = useMemo(() => {
    if (!selectedNodeId) return { incoming: [], outgoing: [] };
    const incoming: { edge: TopologyEdge; peer: TopologyNode }[] = [];
    const outgoing: { edge: TopologyEdge; peer: TopologyNode }[] = [];

    edges.forEach((e) => {
      if (e.to === selectedNodeId) {
        const peer = nodeMap.get(e.from);
        if (peer) incoming.push({ edge: e, peer });
      }
      if (e.from === selectedNodeId) {
        const peer = nodeMap.get(e.to);
        if (peer) outgoing.push({ edge: e, peer });
      }
    });

    return { incoming, outgoing };
  }, [selectedNodeId, edges, nodeMap]);

  return (
    <div
      style={{
        position: "relative",
        width: "100%",
        height: "100%",
        minHeight: "560px",
        background: "var(--bg-panel)",
        border: "1px solid var(--border)",
        overflow: "hidden",
        display: "flex",
        flexDirection: "column",
      }}
    >
      {/* ── Top Floating Controls Bar ──────────────────────────────────────── */}
      <div
        style={{
          position: "absolute",
          top: 12,
          right: 12,
          zIndex: 10,
          display: "flex",
          alignItems: "center",
          gap: 6,
          background: "rgba(17, 17, 17, 0.92)",
          border: "1px solid var(--border)",
          padding: "4px 8px",
          backdropFilter: "blur(4px)",
        }}
      >
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: 11,
            color: "var(--text-dim)",
            marginRight: 6,
          }}
        >
          {nodes.length} nodes · {edges.length} edges
        </span>
        <button
          className="graph-ctrl-btn"
          onClick={fitToView}
          title="Fit entire topology to screen"
          aria-label="Fit topology to screen"
        >
          <Maximize2 size={12} /> Fit
        </button>
        <button
          className="graph-ctrl-btn"
          onClick={() => setScale((s) => Math.max(0.15, s - 0.15))}
          title="Zoom out"
          aria-label="Zoom out"
        >
          <Minus size={12} />
        </button>
        <button
          className="graph-ctrl-btn"
          onClick={() => setScale((s) => Math.min(2.2, s + 0.15))}
          title="Zoom in"
          aria-label="Zoom in"
        >
          <Plus size={12} />
        </button>
        <button
          className="graph-ctrl-btn"
          onClick={() => setScale(1)}
          title="Reset zoom to 100%"
          aria-label="Reset zoom"
        >
          <Minimize2 size={12} />
        </button>
      </div>

      {/* ── Interactive SVG Canvas ─────────────────────────────────────────── */}
      <div
        ref={containerRef}
        style={{
          flex: 1,
          width: "100%",
          height: "100%",
          cursor: dragging ? "grabbing" : "grab",
          userSelect: "none",
          overflow: "hidden",
          position: "relative",
        }}
        onMouseDown={handleMouseDown}
        onWheel={handleWheel}
        role="img"
        aria-label={`Interactive system dependency topology graph showing ${nodes.length} nodes and ${edges.length} directed relationships`}
      >
        <svg
          width={width}
          height={height}
          style={{
            transform: `translate(${offset.x}px, ${offset.y}px) scale(${scale})`,
            transformOrigin: "0 0",
            transition: dragging ? "none" : "transform 0.05s ease-out",
          }}
        >
          <defs>
            {/* Impact-Propagating Arrow Marker (Solid green) */}
            <marker
              id="topo-arrow-propagating"
              viewBox="0 0 10 10"
              refX="8"
              refY="5"
              markerWidth={ARROW_SIZE}
              markerHeight={ARROW_SIZE}
              orient="auto-start-reverse"
            >
              <path d="M 0 1.5 L 8 5 L 0 8.5 z" fill="var(--status-ok)" />
            </marker>

            {/* Non-Propagating Boundary Marker (Muted gray) */}
            <marker
              id="topo-arrow-boundary"
              viewBox="0 0 10 10"
              refX="8"
              refY="5"
              markerWidth={ARROW_SIZE}
              markerHeight={ARROW_SIZE}
              orient="auto-start-reverse"
            >
              <path d="M 0 1.5 L 8 5 L 0 8.5 z" fill="var(--text-dim)" />
            </marker>

            {/* Highlight Marker (White/Accent) */}
            <marker
              id="topo-arrow-highlight"
              viewBox="0 0 10 10"
              refX="8"
              refY="5"
              markerWidth={ARROW_SIZE + 1}
              markerHeight={ARROW_SIZE + 1}
              orient="auto-start-reverse"
            >
              <path d="M 0 1.5 L 8 5 L 0 8.5 z" fill="var(--text-primary)" />
            </marker>
          </defs>

          {/* 1. Render Directed Edges */}
          <g className="topo-edges">
            {edges.map((edge) => {
              const fromNode = nodeMap.get(edge.from);
              const toNode = nodeMap.get(edge.to);
              if (!fromNode || !toNode) return null;

              const hasActiveSelection = Boolean(hoveredNodeId || selectedNodeId);
              const isHighlighted = connectedEdgeIds.has(edge.id);
              const d = computeEdgePath(fromNode, toNode);

              const strokeColor = isHighlighted
                ? "var(--text-primary)"
                : edge.isPropagating
                ? "var(--status-ok)"
                : "var(--border-strong)";

              const markerId = isHighlighted
                ? "url(#topo-arrow-highlight)"
                : edge.isPropagating
                ? "url(#topo-arrow-propagating)"
                : "url(#topo-arrow-boundary)";

              const opacity = hasActiveSelection
                ? isHighlighted
                  ? 1
                  : 0.2
                : edge.isPropagating
                ? 0.75
                : 0.45;

              return (
                <g key={edge.id} opacity={opacity}>
                  <path
                    d={d}
                    fill="none"
                    stroke={strokeColor}
                    strokeWidth={isHighlighted ? 2 : edge.isPropagating ? 1.5 : 1.2}
                    strokeDasharray={edge.isPropagating ? undefined : "4 4"}
                    markerEnd={markerId}
                  />
                </g>
              );
            })}
          </g>

          {/* 2. Render Topology Nodes */}
          <g className="topo-nodes">
            {nodes.map((node) => {
              const isSelected = selectedNodeId === node.id;
              const isHovered = hoveredNodeId === node.id;
              const hasActive = Boolean(hoveredNodeId || selectedNodeId);
              const isConnected = connectedNodeIds.has(node.id);

              const opacity = hasActive ? (isConnected ? 1 : 0.25) : 1;
              const strokeColor = isSelected
                ? "var(--text-primary)"
                : isHovered
                ? "var(--status-ok)"
                : isConnected
                ? "var(--text-secondary)"
                : "var(--border)";

              const strokeWidth = isSelected ? 2 : isHovered ? 1.5 : 1;

              return (
                <g
                  key={node.id}
                  className="topo-node-group"
                  transform={`translate(${node.x}, ${node.y})`}
                  onClick={(e) => {
                    e.stopPropagation();
                    if (!shouldActivateNode(dragDistanceRef.current)) return; // Prevent drag-release from toggling node
                    setSelectedNodeId((cur) => (cur === node.id ? null : node.id));
                  }}
                  onMouseEnter={() => setHoveredNodeId(node.id)}
                  onMouseLeave={() => setHoveredNodeId(null)}
                  style={{ cursor: "pointer", transition: "opacity 0.15s ease-out" }}
                  opacity={opacity}
                >
                  {/* Card Background */}
                  <rect
                    width={NODE_W}
                    height={NODE_H}
                    rx={3}
                    ry={3}
                    fill="var(--bg-elevated)"
                    stroke={strokeColor}
                    strokeWidth={strokeWidth}
                  />

                  {/* Type Badge Tag */}
                  <rect
                    x={8}
                    y={8}
                    width={28}
                    height={16}
                    rx={2}
                    fill="rgba(255, 255, 255, 0.06)"
                    stroke="var(--border)"
                    strokeWidth={1}
                  />
                  <text
                    x={22}
                    y={19}
                    textAnchor="middle"
                    fill="var(--text-secondary)"
                    fontFamily="var(--font-mono)"
                    fontSize={8.5}
                    fontWeight={600}
                  >
                    {typeBadge(node.type)}
                  </text>

                  {/* Namespace text badge */}
                  <text
                    x={42}
                    y={19}
                    fill="var(--text-dim)"
                    fontFamily="var(--font-mono)"
                    fontSize={9}
                  >
                    {node.namespace ? `${node.namespace}` : "cluster"}
                  </text>

                  {/* Node Name */}
                  <text
                    x={8}
                    y={42}
                    fill="var(--text-primary)"
                    fontFamily="var(--font-mono)"
                    fontSize={11}
                    fontWeight={500}
                  >
                    {node.name.length > 22 ? `${node.name.slice(0, 20)}…` : node.name}
                  </text>

                  <title>{node.providerId}</title>
                </g>
              );
            })}
          </g>
        </svg>
      </div>

      {/* ── Fixed Visual Legend (Bottom Left) ───────────────────────────────── */}
      <div
        style={{
          position: "absolute",
          bottom: 12,
          left: 12,
          zIndex: 10,
          background: "rgba(17, 17, 17, 0.94)",
          border: "1px solid var(--border)",
          padding: "8px 14px",
          display: "flex",
          flexDirection: "column",
          gap: 6,
          backdropFilter: "blur(4px)",
          maxWidth: "460px",
        }}
      >
        <div
          style={{
            fontSize: 10,
            textTransform: "uppercase",
            letterSpacing: "0.08em",
            color: "var(--text-dim)",
            fontWeight: 600,
            marginBottom: 2,
          }}
        >
          Relationship Propagation Legend
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <svg width={26} height={12} style={{ flexShrink: 0 }}>
            <line x1={0} y1={6} x2={20} y2={6} stroke="var(--status-ok)" strokeWidth={2} />
            <polygon points="20,6 15,3 15,9" fill="var(--status-ok)" />
          </svg>
          <span style={{ fontSize: 11, color: "var(--text-secondary)" }}>
            <strong style={{ color: "var(--status-ok)" }}>Impact-Propagating</strong> (e.g. <code>DEPENDS_ON</code>) — failure in target propagates upstream.
          </span>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <svg width={26} height={12} style={{ flexShrink: 0 }}>
            <line
              x1={0}
              y1={6}
              x2={20}
              y2={6}
              stroke="var(--text-dim)"
              strokeWidth={1.5}
              strokeDasharray="4,4"
            />
            <polygon points="20,6 15,3 15,9" fill="var(--text-dim)" />
          </svg>
          <span style={{ fontSize: 11, color: "var(--text-dim)" }}>
            <strong style={{ color: "var(--text-secondary)" }}>Lifecycle Boundary</strong> (e.g. <code>OWNS</code>) — ownership only; does NOT propagate runtime failure.
          </span>
        </div>
      </div>

      {/* ── Slide-Over Node Details Drawer (Right Side) ───────────────────────── */}
      {activeSelectedNode && (
        <div
          style={{
            position: "absolute",
            top: 0,
            right: 0,
            width: "380px",
            height: "100%",
            background: "var(--bg-sidebar)",
            borderLeft: "1px solid var(--border)",
            zIndex: 20,
            display: "flex",
            flexDirection: "column",
            boxShadow: "-8px 0 24px rgba(0,0,0,0.6)",
          }}
        >
          {/* Header */}
          <div
            style={{
              padding: "14px 16px",
              borderBottom: "1px solid var(--border)",
              display: "flex",
              alignItems: "flex-start",
              justifyContent: "space-between",
            }}
          >
            <div>
              <div
                style={{
                  fontSize: 10,
                  textTransform: "uppercase",
                  color: "var(--text-dim)",
                  letterSpacing: "0.06em",
                  marginBottom: 2,
                }}
              >
                Selected Topology Node
              </div>
              <div
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: 13,
                  fontWeight: 600,
                  color: "var(--text-primary)",
                  wordBreak: "break-all",
                }}
              >
                {activeSelectedNode.name}
              </div>
            </div>
            <button
              onClick={() => setSelectedNodeId(null)}
              style={{
                background: "transparent",
                border: "none",
                color: "var(--text-dim)",
                cursor: "pointer",
                padding: 4,
              }}
              title="Close drawer"
            >
              <X size={16} />
            </button>
          </div>

          {/* Body */}
          <div style={{ flex: 1, overflowY: "auto", padding: "16px" }}>
            {/* Metadata Badges */}
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 14 }}>
              <span className="risk-badge low" style={{ fontSize: 10 }}>
                {activeSelectedNode.type}
              </span>
              <span
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: 10,
                  background: "var(--bg-elevated)",
                  padding: "2px 8px",
                  border: "1px solid var(--border)",
                  color: "var(--text-secondary)",
                }}
              >
                ns: {activeSelectedNode.namespace || "cluster"}
              </span>
              <span
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: 10,
                  background: "var(--bg-elevated)",
                  padding: "2px 8px",
                  border: "1px solid var(--border)",
                  color: "var(--text-dim)",
                }}
              >
                cluster: {activeSelectedNode.cluster || "default"}
              </span>
            </div>

            {/* Canonical Provider ID */}
            <div style={{ marginBottom: 16 }}>
              <div style={{ fontSize: 10, color: "var(--text-dim)", textTransform: "uppercase", marginBottom: 4 }}>
                Canonical Identity
              </div>
              <div
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: 11,
                  background: "var(--bg-elevated)",
                  padding: "8px 10px",
                  border: "1px solid var(--border)",
                  wordBreak: "break-all",
                  color: "var(--text-secondary)",
                }}
              >
                {activeSelectedNode.providerId}
              </div>
            </div>

            {/* Primary Action: Analyze Impact */}
            <div style={{ marginBottom: 20 }}>
              <button
                className="analyze-btn"
                onClick={() => {
                  onNavigateToInvestigate({
                    provider: activeSelectedNode.provider,
                    resource_type: activeSelectedNode.type,
                    provider_id: activeSelectedNode.providerId,
                    first_observed_at: new Date().toISOString(),
                    last_observed_at: new Date().toISOString(),
                  });
                }}
                style={{ width: "100%", justifyContent: "center", padding: "8px 12px" }}
              >
                <Play size={13} style={{ marginRight: 6 }} />
                Analyze Impact of Modifying This Node
              </button>
            </div>

            {/* Incoming Connections (Dependents & Owners) */}
            <div style={{ marginBottom: 20 }}>
              <div
                style={{
                  fontSize: 10,
                  color: "var(--text-dim)",
                  textTransform: "uppercase",
                  letterSpacing: "0.06em",
                  marginBottom: 8,
                  display: "flex",
                  alignItems: "center",
                  gap: 4,
                }}
              >
                <ArrowRight size={10} style={{ transform: "rotate(180deg)" }} />
                Incoming Connections ({selectedNodeRelationships.incoming.length})
              </div>
              {selectedNodeRelationships.incoming.length === 0 ? (
                <div style={{ fontSize: 11, color: "var(--text-dim)", fontStyle: "italic" }}>
                  No upstream controllers or callers targeting this resource.
                </div>
              ) : (
                <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                  {selectedNodeRelationships.incoming.map(({ edge, peer }) => (
                    <div
                      key={edge.id}
                      onClick={() => setSelectedNodeId(peer.id)}
                      style={{
                        background: "var(--bg-elevated)",
                        border: "1px solid var(--border)",
                        padding: "8px 10px",
                        cursor: "pointer",
                      }}
                    >
                      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 3 }}>
                        <span
                          style={{
                            fontFamily: "var(--font-mono)",
                            fontSize: 9,
                            padding: "1px 6px",
                            background: edge.isPropagating ? "var(--status-ok-bg)" : "rgba(255,255,255,0.06)",
                            color: edge.isPropagating ? "var(--status-ok)" : "var(--text-dim)",
                            border: `1px solid ${edge.isPropagating ? "var(--status-ok)" : "var(--border)"}`,
                          }}
                        >
                          {edge.kind}
                        </span>
                        <span style={{ fontSize: 9, color: "var(--text-dim)" }}>
                          {edge.isPropagating ? "Propagates failure" : "Boundary"}
                        </span>
                      </div>
                      <div style={{ fontFamily: "var(--font-mono)", fontSize: 11, fontWeight: 500, color: "var(--text-primary)" }}>
                        {peer.name}
                      </div>
                      <div style={{ fontSize: 10, color: "var(--text-dim)", marginTop: 2 }}>
                        {peer.type} · {peer.namespace || "cluster"}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* Outgoing Connections (Dependencies & Owned Units) */}
            <div>
              <div
                style={{
                  fontSize: 10,
                  color: "var(--text-dim)",
                  textTransform: "uppercase",
                  letterSpacing: "0.06em",
                  marginBottom: 8,
                  display: "flex",
                  alignItems: "center",
                  gap: 4,
                }}
              >
                <ArrowRight size={10} />
                Outgoing Connections ({selectedNodeRelationships.outgoing.length})
              </div>
              {selectedNodeRelationships.outgoing.length === 0 ? (
                <div style={{ fontSize: 11, color: "var(--text-dim)", fontStyle: "italic" }}>
                  No downstream dependencies or owned workloads recorded.
                </div>
              ) : (
                <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                  {selectedNodeRelationships.outgoing.map(({ edge, peer }) => (
                    <div
                      key={edge.id}
                      onClick={() => setSelectedNodeId(peer.id)}
                      style={{
                        background: "var(--bg-elevated)",
                        border: "1px solid var(--border)",
                        padding: "8px 10px",
                        cursor: "pointer",
                      }}
                    >
                      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 3 }}>
                        <span
                          style={{
                            fontFamily: "var(--font-mono)",
                            fontSize: 9,
                            padding: "1px 6px",
                            background: edge.isPropagating ? "var(--status-ok-bg)" : "rgba(255,255,255,0.06)",
                            color: edge.isPropagating ? "var(--status-ok)" : "var(--text-dim)",
                            border: `1px solid ${edge.isPropagating ? "var(--status-ok)" : "var(--border)"}`,
                          }}
                        >
                          {edge.kind}
                        </span>
                        <span style={{ fontSize: 9, color: "var(--text-dim)" }}>
                          {edge.isPropagating ? "Propagates failure" : "Boundary"}
                        </span>
                      </div>
                      <div style={{ fontFamily: "var(--font-mono)", fontSize: 11, fontWeight: 500, color: "var(--text-primary)" }}>
                        {peer.name}
                      </div>
                      <div style={{ fontSize: 10, color: "var(--text-dim)", marginTop: 2 }}>
                        {peer.type} · {peer.namespace || "cluster"}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
