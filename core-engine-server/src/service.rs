//! Implementation of the generated DiscoveryService gRPC trait.

use std::sync::Arc;
use tonic::{Request, Response, Status};
use wb_core_engine::DiscoveryEngine;
use wb_core_proto::core_v1::discovery_service_server::DiscoveryService;
use wb_core_proto::core_v1::{RunDiscoveryRequest, RunDiscoveryResponse};

use crate::convert::{discovery_result_to_proto, proto_to_evidence};

/// Concrete implementation of `wb.core.v1.DiscoveryService`.
///
/// Owns an `Arc<DiscoveryEngine>` and delegates discovery logic to it.
/// Does not implement or duplicate discovery rules — acts solely as an adapter
/// between protobuf transport and the domain engine.
#[derive(Clone)]
pub struct DiscoveryServiceImpl {
    engine: Arc<DiscoveryEngine>,
    instance_id: String,
}

impl DiscoveryServiceImpl {
    /// Create a new `DiscoveryServiceImpl` wrapping the provided engine.
    pub fn new(engine: Arc<DiscoveryEngine>) -> Self {
        Self {
            engine,
            instance_id: uuid::Uuid::new_v4().to_string(),
        }
    }

    /// Create a new `DiscoveryServiceImpl` wrapping the provided engine and explicit server instance ID.
    pub fn with_instance_id(engine: Arc<DiscoveryEngine>, instance_id: String) -> Self {
        Self {
            engine,
            instance_id,
        }
    }

    /// Create a `DiscoveryServiceImpl` with the default rule set.
    pub fn with_default_engine() -> Self {
        Self::new(Arc::new(DiscoveryEngine::default_v2()))
    }

    /// Return a reference to the inner `DiscoveryEngine`.
    pub fn engine(&self) -> &DiscoveryEngine {
        &self.engine
    }

    /// Return the server instance ID.
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }
}

#[tonic::async_trait]
impl DiscoveryService for DiscoveryServiceImpl {
    async fn run_discovery(
        &self,
        request: Request<RunDiscoveryRequest>,
    ) -> Result<Response<RunDiscoveryResponse>, Status> {
        let req = request.into_inner();

        // 1. Convert and validate all evidence observations.
        // Fails fast with Status::invalid_argument on any malformed input.
        let evidence: Vec<wb_core_engine::Evidence> = req
            .evidence
            .into_iter()
            .map(proto_to_evidence)
            .collect::<Result<Vec<_>, Status>>()?;

        // 2. Delegate to the authoritative DiscoveryEngine.
        let results = self.engine.run(&evidence);

        // 3. Convert domain results to protobuf responses.
        let proto_results = results.into_iter().map(discovery_result_to_proto).collect();

        let mut resp = Response::new(RunDiscoveryResponse {
            results: proto_results,
        });
        if let Ok(val) = self.instance_id.parse() {
            resp.metadata_mut().insert("x-server-instance-id", val);
        }

        Ok(resp)
    }
}

// ---------------------------------------------------------------------------
// AnswerService Implementation
// ---------------------------------------------------------------------------

use std::collections::HashMap;
use std::sync::RwLock;
use wb_core_engine::answer::AnswerEngine;
use wb_core_engine::{Evidence, Graph, ProvenanceStore};
use wb_core_proto::core_v1::answer_service_server::AnswerService;
use wb_core_proto::core_v1::{
    AnalyzeImpactRequest, AnalyzeImpactResponse, InvalidateWorkspaceRequest,
    InvalidateWorkspaceResponse, LoadStateRequest, LoadStateResponse,
};

use crate::convert::{
    impact_answer_to_proto, proto_to_answer_request, proto_to_diagnostic, proto_to_relationship,
    proto_to_relationship_evidence, proto_to_resource_identity,
};

/// Tenant state encapsulating the authoritative Graph, ProvenanceStore, and Evidence.
#[derive(Clone, Default)]
pub struct TenantState {
    pub graph: Graph,
    pub provenance: ProvenanceStore,
    pub evidence: Vec<Evidence>,
}

/// Concrete implementation of `wb.core.v1.AnswerService`.
///
/// Holds in-memory Graph, ProvenanceStore, and Evidence state per workspace (tenant)
/// and delegates impact analysis to `AnswerEngine::analyze`.
#[derive(Clone)]
pub struct AnswerServiceImpl {
    instance_id: String,
    tenants: Arc<RwLock<HashMap<String, TenantState>>>,
}

impl Default for AnswerServiceImpl {
    fn default() -> Self {
        Self {
            instance_id: uuid::Uuid::new_v4().to_string(),
            tenants: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl AnswerServiceImpl {
    /// Create a new `AnswerServiceImpl` wrapping the provided tenant map.
    pub fn new(tenants: Arc<RwLock<HashMap<String, TenantState>>>) -> Self {
        Self {
            instance_id: uuid::Uuid::new_v4().to_string(),
            tenants,
        }
    }

    /// Create an `AnswerServiceImpl` with an explicit server instance ID.
    pub fn with_instance_id(instance_id: String) -> Self {
        Self {
            instance_id,
            tenants: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Return the server instance ID.
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Create an `AnswerServiceImpl` initialized with specific domain state in the default tenant ("").
    pub fn with_state(graph: Graph, provenance: ProvenanceStore, evidence: Vec<Evidence>) -> Self {
        let mut map = HashMap::new();
        map.insert(
            "".to_string(),
            TenantState {
                graph,
                provenance,
                evidence,
            },
        );
        Self {
            instance_id: uuid::Uuid::new_v4().to_string(),
            tenants: Arc::new(RwLock::new(map)),
        }
    }

    /// Update the default tenant state atomically.
    pub fn set_state(&self, graph: Graph, provenance: ProvenanceStore, evidence: Vec<Evidence>) {
        self.set_workspace_state("", graph, provenance, evidence);
    }

    /// Update a specific workspace tenant state atomically.
    pub fn set_workspace_state(
        &self,
        workspace_id: &str,
        graph: Graph,
        provenance: ProvenanceStore,
        evidence: Vec<Evidence>,
    ) {
        if let Ok(mut map) = self.tenants.write() {
            map.insert(
                workspace_id.to_string(),
                TenantState {
                    graph,
                    provenance,
                    evidence,
                },
            );
        }
    }
}

#[tonic::async_trait]
impl AnswerService for AnswerServiceImpl {
    async fn analyze_impact(
        &self,
        request: Request<AnalyzeImpactRequest>,
    ) -> Result<Response<AnalyzeImpactResponse>, Status> {
        let req = request.into_inner();
        let ws_id = req.workspace_id.clone();
        let domain_req = proto_to_answer_request(req)?;

        let tenants = self
            .tenants
            .read()
            .map_err(|_| Status::internal("tenants lock poisoned"))?;

        let tenant_state = match tenants.get(&ws_id) {
            Some(state) => state,
            None => {
                return Err(Status::failed_precondition(format!(
                    "workspace state not materialized for workspace_id: '{}'",
                    ws_id
                )));
            }
        };

        let answer = AnswerEngine::analyze(
            &tenant_state.graph,
            &tenant_state.provenance,
            &tenant_state.evidence,
            &domain_req,
        );
        let proto_resp = impact_answer_to_proto(answer);

        let mut resp = Response::new(proto_resp);
        if let Ok(val) = self.instance_id.parse() {
            resp.metadata_mut().insert("x-server-instance-id", val);
        }

        Ok(resp)
    }

    async fn load_state(
        &self,
        request: Request<LoadStateRequest>,
    ) -> Result<Response<LoadStateResponse>, Status> {
        let req = request.into_inner();
        let workspace_id = req.workspace_id;

        let mut graph = Graph::new();
        let mut provenance = ProvenanceStore::new();

        for res_proto in req.observed_resources {
            let res = proto_to_resource_identity(res_proto)?;
            graph.add_observed_resource(res);
        }

        for diag_proto in req.diagnostics {
            let diag = proto_to_diagnostic(diag_proto)?;
            graph.add_diagnostic(diag);
        }

        let mut rel_count = 0u32;
        for rel_proto in req.relationships {
            let rel = proto_to_relationship(rel_proto)?;
            graph.add_relationship(rel.clone());
            provenance.add_relationship(rel);
            rel_count += 1;
        }

        let mut prov_count = 0u32;
        for assoc in req.associations {
            let (rel, ev_ids) = proto_to_relationship_evidence(assoc)?;
            graph.add_relationship(rel.clone());
            provenance.add_relationship(rel.clone());
            for ev_id in ev_ids {
                provenance.add_evidence(&rel, ev_id);
                prov_count += 1;
            }
        }

        let mut ev_count = 0u32;
        let mut evidence = Vec::with_capacity(req.evidence.len());
        for ev_proto in req.evidence {
            let ev = proto_to_evidence(ev_proto)?;
            evidence.push(ev);
            ev_count += 1;
        }

        self.set_workspace_state(&workspace_id, graph, provenance, evidence);

        let mut resp = Response::new(LoadStateResponse {
            workspace_id,
            relationships_loaded: rel_count,
            evidence_loaded: ev_count,
            provenance_links_loaded: prov_count,
        });
        if let Ok(val) = self.instance_id.parse() {
            resp.metadata_mut().insert("x-server-instance-id", val);
        }

        Ok(resp)
    }

    async fn invalidate_workspace(
        &self,
        request: Request<InvalidateWorkspaceRequest>,
    ) -> Result<Response<InvalidateWorkspaceResponse>, Status> {
        let req = request.into_inner();
        let ws_id = req.workspace_id;

        let mut tenants = self
            .tenants
            .write()
            .map_err(|_| Status::internal("tenants lock poisoned"))?;

        let removed = tenants.remove(&ws_id).is_some();

        let mut resp = Response::new(InvalidateWorkspaceResponse {
            workspace_id: ws_id,
            invalidated: removed,
        });
        if let Ok(val) = self.instance_id.parse() {
            resp.metadata_mut().insert("x-server-instance-id", val);
        }

        Ok(resp)
    }
}
