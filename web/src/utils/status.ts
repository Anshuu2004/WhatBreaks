import type { ComponentHealth, ComponentDiagnosticResult } from "../types";

export type AuthoritativeStatus =
  | "HEALTHY"
  | "DEGRADED"
  | "FAILED"
  | "UNKNOWN"
  | "CHECKING";

/**
 * Normalizes any raw status string into the canonical AuthoritativeStatus union.
 */
export function normalizeStatus(rawStatus?: string | null): AuthoritativeStatus {
  if (!rawStatus) return "UNKNOWN";
  const s = rawStatus.toLowerCase().trim();

  switch (s) {
    case "checking":
    case "diagnosing":
    case "loading":
    case "pending":
      return "CHECKING";

    case "healthy":
    case "ok":
    case "passed":
    case "ready":
      return "HEALTHY";

    case "degraded":
    case "warn":
    case "warning":
    case "partial":
      return "DEGRADED";

    case "failed":
    case "unavailable":
    case "error":
    case "down":
    case "dead":
    case "unready":
      return "FAILED";

    case "not_configured":
    case "skipped":
    case "unconfigured":
    case "unknown":
      return "UNKNOWN";

    default:
      return "UNKNOWN";
  }
}

export interface ComponentStatusEvaluation {
  authoritativeStatus: AuthoritativeStatus;
  telemetryStatus: AuthoritativeStatus;
  diagnosticStatus: AuthoritativeStatus | null;
  hasDiscrepancy: boolean;
  discrepancyNote?: string;
  sourceOfTruth: "telemetry" | "diagnostic" | "combined" | "checking" | "error";
}

/**
 * Evaluates the authoritative operational status of a component by reconciling
 * passive background telemetry with active on-demand diagnostic probe results.
 *
 * Invariants enforced:
 * 1. While a probe check is in flight, the authoritative status is strictly CHECKING.
 * 2. On network or request failure, authoritative status is FAILED or UNKNOWN (never falsely HEALTHY).
 * 3. A successful diagnostic probe NEVER silently overwrites a separate genuine service failure in telemetry.
 * 4. A diagnostic probe detecting degradation/failure immediately promotes the component to DEGRADED/FAILED.
 * 5. Both telemetry and diagnostic checks are preserved and labeled transparently if they differ.
 */
export function evaluateComponentStatus(params: {
  telemetryHealth?: ComponentHealth | null;
  diagnosticResult?: ComponentDiagnosticResult | null;
  isChecking?: boolean;
  hasError?: boolean;
}): ComponentStatusEvaluation {
  const { telemetryHealth, diagnosticResult, isChecking, hasError } = params;

  const telNorm = normalizeStatus(telemetryHealth?.status);
  const diagNorm = diagnosticResult ? normalizeStatus(diagnosticResult.status) : null;

  if (isChecking) {
    return {
      authoritativeStatus: "CHECKING",
      telemetryStatus: telNorm,
      diagnosticStatus: diagNorm,
      hasDiscrepancy: false,
      sourceOfTruth: "checking",
    };
  }

  if (hasError && !diagnosticResult) {
    return {
      authoritativeStatus: "FAILED",
      telemetryStatus: telNorm,
      diagnosticStatus: null,
      hasDiscrepancy: false,
      sourceOfTruth: "error",
    };
  }

  // If no diagnostic result is available, rely purely on telemetry health
  if (!diagnosticResult || diagNorm === null) {
    return {
      authoritativeStatus: telNorm,
      telemetryStatus: telNorm,
      diagnosticStatus: null,
      hasDiscrepancy: false,
      sourceOfTruth: "telemetry",
    };
  }

  // Case 1: Passive telemetry reports genuine failure/degradation, but isolated diagnostic probe reports healthy.
  // Rule: A successful probe check must NOT mask background service failures.
  if ((telNorm === "FAILED" || telNorm === "DEGRADED") && diagNorm === "HEALTHY") {
    return {
      authoritativeStatus: telNorm,
      telemetryStatus: telNorm,
      diagnosticStatus: diagNorm,
      hasDiscrepancy: true,
      discrepancyNote: `Telemetry monitoring reports ${telNorm}, although on-demand diagnostic probe passed. Genuine background failure is preserved.`,
      sourceOfTruth: "telemetry",
    };
  }

  // Case 2: Diagnostic probe discovered a failure or warning that telemetry had not yet caught or classified.
  // Rule: Diagnostic probe failure takes immediate precedence over passive telemetry.
  if (diagNorm === "FAILED" || diagNorm === "DEGRADED") {
    const hasDiscrepancy = telNorm !== diagNorm && telNorm !== "UNKNOWN";
    return {
      authoritativeStatus: diagNorm,
      telemetryStatus: telNorm,
      diagnosticStatus: diagNorm,
      hasDiscrepancy,
      discrepancyNote: hasDiscrepancy
        ? `Live verification probe detected ${diagNorm} state, taking precedence over passive telemetry (${telNorm}).`
        : undefined,
      sourceOfTruth: "diagnostic",
    };
  }

  // Case 3: Both agree healthy
  if (telNorm === "HEALTHY" && diagNorm === "HEALTHY") {
    return {
      authoritativeStatus: "HEALTHY",
      telemetryStatus: telNorm,
      diagnosticStatus: diagNorm,
      hasDiscrepancy: false,
      sourceOfTruth: "combined",
    };
  }

  // Case 4: Default reconciliation based on severity
  const severityRank: Record<AuthoritativeStatus, number> = {
    FAILED: 4,
    DEGRADED: 3,
    UNKNOWN: 2,
    HEALTHY: 1,
    CHECKING: 0,
  };

  const chosen = (severityRank[diagNorm] ?? 0) >= (severityRank[telNorm] ?? 0) ? diagNorm : telNorm;
  return {
    authoritativeStatus: chosen,
    telemetryStatus: telNorm,
    diagnosticStatus: diagNorm,
    hasDiscrepancy: telNorm !== diagNorm && telNorm !== "UNKNOWN",
    sourceOfTruth: (severityRank[diagNorm] ?? 0) >= (severityRank[telNorm] ?? 0) ? "diagnostic" : "telemetry",
  };
}

/**
 * Returns consistent UI styling properties for any AuthoritativeStatus.
 */
export function getStatusStyle(status: AuthoritativeStatus): {
  color: string;
  background: string;
  borderColor: string;
  label: string;
} {
  switch (status) {
    case "HEALTHY":
      return {
        color: "var(--status-ok)",
        background: "rgba(34, 197, 94, 0.15)",
        borderColor: "var(--status-ok)",
        label: "HEALTHY",
      };
    case "DEGRADED":
      return {
        color: "var(--status-warn)",
        background: "rgba(234, 179, 8, 0.15)",
        borderColor: "var(--status-warn)",
        label: "DEGRADED",
      };
    case "FAILED":
      return {
        color: "var(--status-error)",
        background: "rgba(239, 68, 68, 0.15)",
        borderColor: "var(--status-error)",
        label: "FAILED",
      };
    case "CHECKING":
      return {
        color: "var(--status-info)",
        background: "rgba(59, 130, 246, 0.15)",
        borderColor: "var(--status-info)",
        label: "CHECKING...",
      };
    case "UNKNOWN":
    default:
      return {
        color: "var(--text-dim)",
        background: "rgba(100, 116, 139, 0.15)",
        borderColor: "var(--border)",
        label: "UNKNOWN",
      };
  }
}
