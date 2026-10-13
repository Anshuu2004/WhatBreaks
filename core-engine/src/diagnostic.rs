//! WhatBreaks Diagnostic and Analysis Completeness Model
//!
//! Provides structured, machine-readable diagnostics for unknown resources,
//! unresolved references, unmapped runtime connections, and collection scope integrity.
//!
//! # Architecture & Invariants
//!
//! - **Analysis Completeness**: `is_complete == (status == CompletenessStatus::Complete)`.
//! - **Relevance Partitioning**: Diagnostics distinguish `Target`, `BlastRadius`, and `Workspace` relevance.
//! - **Determinism**: Diagnostics and reasons are deterministically ordered.

use std::fmt;
use serde::{Deserialize, Serialize};

use crate::evidence::EvidenceId;
use crate::resource::ResourceIdentity;

// ---------------------------------------------------------------------------
// Diagnostic Code Constants
// ---------------------------------------------------------------------------

pub mod diagnostic_code {
    /// The target resource requested for analysis has not been directly observed.
    pub const UNOBSERVED_TARGET: &str = "UNOBSERVED_TARGET";

    /// A relationship points to an endpoint resource that has not been directly observed.
    pub const UNRESOLVED_REFERENCE: &str = "UNRESOLVED_REFERENCE";

    /// Runtime connection evidence exists but could not be mapped to any known target.
    pub const UNMAPPED_CONNECTION: &str = "UNMAPPED_CONNECTION";

    /// A required or optional collection scope failed or was incomplete.
    pub const INCOMPLETE_SCOPE: &str = "INCOMPLETE_SCOPE";

    /// Contradictory evidence observations prevented deterministic relationship derivation.
    pub const DISCOVERY_CONFLICT: &str = "DISCOVERY_CONFLICT";
}

// ---------------------------------------------------------------------------
// Diagnostic Severity
// ---------------------------------------------------------------------------

/// Severity level of a diagnostic finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DiagnosticSeverity {
    Info,
    Warning,
    Error,
}

impl DiagnosticSeverity {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Info => "INFO",
            Self::Warning => "WARNING",
            Self::Error => "ERROR",
        }
    }
}

impl fmt::Display for DiagnosticSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// Diagnostic Relevance
// ---------------------------------------------------------------------------

/// Scope of relevance for a diagnostic in relation to an analysis query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DiagnosticRelevance {
    /// Directly affects the resource being analyzed.
    Target,
    /// Affects a dependency or candidate impact path in the blast radius.
    BlastRadius,
    /// Describes workspace-wide collection conditions not necessarily impacting the query.
    Workspace,
}

impl DiagnosticRelevance {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Target => "TARGET",
            Self::BlastRadius => "BLAST_RADIUS",
            Self::Workspace => "WORKSPACE",
        }
    }
}

impl fmt::Display for DiagnosticRelevance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// Completeness Status
// ---------------------------------------------------------------------------

/// High-level completeness classification of an impact analysis result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CompletenessStatus {
    /// Analysis has complete observation coverage for the target and blast radius.
    Complete,
    /// Relevant observations or dependencies are unresolved, but partial blast radius is available.
    Partial,
    /// Critical observations or targets are missing; results are degraded.
    Degraded,
}

impl CompletenessStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Complete => "COMPLETE",
            Self::Partial => "PARTIAL",
            Self::Degraded => "DEGRADED",
        }
    }
}

impl fmt::Display for CompletenessStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// Analysis Completeness
// ---------------------------------------------------------------------------

/// Structured completeness metadata detailing whether the blast radius is authoritative.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnalysisCompleteness {
    /// Invariant: `is_complete == (status == CompletenessStatus::Complete)`.
    pub is_complete: bool,
    /// Completeness status classification.
    pub status: CompletenessStatus,
    /// Factual reasons explaining any partial or degraded completeness.
    pub reasons: Vec<String>,
}

impl AnalysisCompleteness {
    /// Construct a new `AnalysisCompleteness`, enforcing the invariant:
    /// `is_complete == (status == CompletenessStatus::Complete)`.
    pub fn new(status: CompletenessStatus, reasons: Vec<String>) -> Self {
        Self {
            is_complete: status == CompletenessStatus::Complete,
            status,
            reasons,
        }
    }

    /// Construct a complete status with no degradation reasons.
    pub fn complete() -> Self {
        Self::new(CompletenessStatus::Complete, Vec::new())
    }

    /// Construct a partial status with explanatory reasons.
    pub fn partial(reasons: Vec<String>) -> Self {
        Self::new(CompletenessStatus::Partial, reasons)
    }

    /// Construct a degraded status with explanatory reasons.
    pub fn degraded(reasons: Vec<String>) -> Self {
        Self::new(CompletenessStatus::Degraded, reasons)
    }
}

// ---------------------------------------------------------------------------
// Diagnostic Item
// ---------------------------------------------------------------------------

/// Machine-readable diagnostic item detailing dependency, connection, or scope findings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    /// Stable machine-readable code (e.g. `UNOBSERVED_TARGET`, `UNRESOLVED_REFERENCE`).
    pub code: String,
    /// Severity level (`INFO`, `WARNING`, `ERROR`).
    pub severity: DiagnosticSeverity,
    /// Human-readable explanation.
    pub message: String,
    /// The subject resource primarily associated with this diagnostic.
    pub target: Option<ResourceIdentity>,
    /// Referenced resource endpoint if this diagnostic concerns an unresolved reference.
    pub referenced_resource: Option<ResourceIdentity>,
    /// Unresolved network endpoint (e.g. `10.0.0.1:8080`) if unmapped connection.
    pub unresolved_endpoint: Option<String>,
    /// Collection scope identifier (e.g. `pods`, `ingresses`, `cluster/prod`).
    pub scope: Option<String>,
    /// IDs of supporting evidence observations supporting this diagnostic.
    pub supporting_evidence_ids: Vec<EvidenceId>,
    /// Relevance level to the query (`TARGET`, `BLAST_RADIUS`, `WORKSPACE`).
    pub relevance: DiagnosticRelevance,
    /// Completeness impact classification.
    pub completeness_impact: CompletenessStatus,
    /// Timestamp when first observed (RFC 3339 string).
    pub first_observed_at: Option<String>,
    /// Timestamp when last observed (RFC 3339 string).
    pub last_observed_at: Option<String>,
}

impl Diagnostic {
    /// Create a new Diagnostic.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        code: impl Into<String>,
        severity: DiagnosticSeverity,
        message: impl Into<String>,
        target: Option<ResourceIdentity>,
        referenced_resource: Option<ResourceIdentity>,
        unresolved_endpoint: Option<String>,
        scope: Option<String>,
        supporting_evidence_ids: Vec<EvidenceId>,
        relevance: DiagnosticRelevance,
        completeness_impact: CompletenessStatus,
        first_observed_at: Option<String>,
        last_observed_at: Option<String>,
    ) -> Self {
        Self {
            code: code.into(),
            severity,
            message: message.into(),
            target,
            referenced_resource,
            unresolved_endpoint,
            scope,
            supporting_evidence_ids,
            relevance,
            completeness_impact,
            first_observed_at,
            last_observed_at,
        }
    }

    /// Create an unobserved target diagnostic.
    pub fn unobserved_target(target: ResourceIdentity) -> Self {
        let msg = format!(
            "Target resource '{}:{}:{}' has not been directly observed in authoritative state",
            target.provider, target.resource_type, target.provider_id
        );
        Self {
            code: diagnostic_code::UNOBSERVED_TARGET.to_string(),
            severity: DiagnosticSeverity::Error,
            message: msg,
            target: Some(target),
            referenced_resource: None,
            unresolved_endpoint: None,
            scope: None,
            supporting_evidence_ids: Vec::new(),
            relevance: DiagnosticRelevance::Target,
            completeness_impact: CompletenessStatus::Degraded,
            first_observed_at: None,
            last_observed_at: None,
        }
    }

    /// Natural comparison key for deterministic ordering.
    pub fn sort_key(&self) -> (&str, &str, &str, &str) {
        let target_str = match &self.target {
            Some(t) => t.provider_id.as_str(),
            None => "",
        };
        let ref_str = match &self.referenced_resource {
            Some(r) => r.provider_id.as_str(),
            None => "",
        };
        (self.code.as_str(), self.message.as_str(), target_str, ref_str)
    }
}
