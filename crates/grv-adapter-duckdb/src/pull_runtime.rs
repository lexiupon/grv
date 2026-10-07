//! Typed transactional process pull lifecycle and independent S3 reader setup.
use crate::{
    binding,
    extraction::error,
    pull::{self, PullError, WorkspaceBinding},
    pull_worker::PullWorker,
    worker::Interrupt,
};
use grv_adapter_api::{
    self as api, BindingState, ConnectionLocator, Handle, PullRecovery, Refresh, RequestRecord,
    ResolutionState, ResolvePhase,
};
use grv_adapter_sdk::{BoundConnection, StopToken};
use grv_types::{ErrorCode, Uuid};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io,
    path::PathBuf,
    sync::{Arc, Mutex, atomic::AtomicBool},
};
type Result<T> = grv_adapter_sdk::Result<T>;
#[allow(clippy::result_large_err)]
fn failure(failure: PullError) -> grv_types::PublicError {
    let (code, message) = match failure {
        PullError::Engine(error) => (
            if error.kind() == io::ErrorKind::WouldBlock {
                ErrorCode::EngineBusy
            } else if error.kind() == io::ErrorKind::InvalidInput {
                ErrorCode::InvalidDeclaration
            } else {
                ErrorCode::AdapterFailure
            },
            error.to_string(),
        ),
        PullError::OutcomeUnknown(message) => (ErrorCode::OutcomeUnknown, message),
        PullError::RequestMismatch => (
            ErrorCode::RequestMismatch,
            "pull request differs from recorded attempt".into(),
        ),
        PullError::StateConflict(message) => (ErrorCode::StateConflict, message),
    };
    error(code, message)
}
struct Bound {
    engine: PathBuf,
    requires_s3: AtomicBool,
    identity: String,
    binding: WorkspaceBinding,
    workspace_constraint: Option<Uuid>,
    worker: Arc<Mutex<PullWorker>>,
    interrupt: crate::native::NativeInterrupt,
}
#[derive(Default)]
pub struct Runtime {
    connections: BTreeMap<Handle, Bound>,
    next: u64,
    cancellation: Option<StopToken>,
}
#[allow(clippy::result_large_err)]
impl Runtime {
    pub fn configure_cancellation(&mut self, token: &StopToken) {
        self.cancellation = Some(token.clone());
    }
    pub fn bind(
        &mut self,
        locator: ConnectionLocator,
        root: Option<String>,
        expected: Option<String>,
        workspace: Option<Uuid>,
        resources: &api::Resources,
    ) -> Result<BoundConnection> {
        let root = root.ok_or_else(|| {
            error(
                ErrorCode::InvalidDeclaration,
                "pull requires canonical root",
            )
        })?;
        let relocated = binding::locate_pull(locator.canonical_connection.clone())
            .map_err(|e| failure(e.into()))?;
        if relocated != locator
            || expected
                .as_ref()
                .is_some_and(|value| Some(value) != locator.identity.as_ref())
        {
            return Err(error(
                ErrorCode::RequestMismatch,
                "pull connection locator changed",
            ));
        }
        let path = PathBuf::from(locator.engine_path.as_ref().unwrap());
        crate::extraction::outside_root(&path, &root).map_err(|e| failure(e.into()))?;
        let mut worker = PullWorker::open_with_resources(&path, false, resources.clone())
            .map_err(|e| failure(e.into()))?;
        let prior = worker
            .binding(root.clone(), &AtomicBool::new(false))
            .map_err(failure)?;
        if workspace.as_ref().is_some_and(|expected| {
            prior
                .as_ref()
                .is_some_and(|binding| &binding.workspace_id != expected)
        }) {
            return Err(error(
                ErrorCode::StateConflict,
                "workspace identity differs",
            ));
        }
        let state = if prior.is_some() {
            BindingState::Bound
        } else {
            BindingState::Uninitialized
        };
        let established_workspace = prior.as_ref().map(|binding| binding.workspace_id.clone());
        let workspace_constraint = established_workspace.clone().or(workspace);
        let binding = prior.unwrap_or_else(|| WorkspaceBinding {
            canonical_root: root,
            // No identity is established by binding. The fixed parent request
            // supplies its durably reserved candidate before the first write.
            workspace_id: workspace_constraint.clone().unwrap_or_else(Uuid::v4),
        });
        let interrupt = worker.interrupt_handle();
        self.next += 1;
        let handle = Handle::new(format!("duckdb-pull-{}", self.next))
            .map_err(|e| error(ErrorCode::AdapterFailure, e.to_string()))?;
        let identity = locator.identity.unwrap();
        self.connections.insert(
            handle.clone(),
            Bound {
                engine: path,
                requires_s3: AtomicBool::new(false),
                identity: identity.clone(),
                binding: binding.clone(),
                workspace_constraint,
                worker: Arc::new(Mutex::new(worker)),
                interrupt,
            },
        );
        Ok(BoundConnection {
            handle,
            identity: Some(identity),
            workspace_id: established_workspace.clone(),
            binding: state,
            details: json!({"database":locator.canonical_connection["database"],"workspace_id":established_workspace,"binding":state}),
        })
    }
    pub fn contains(&self, handle: &Handle) -> bool {
        self.connections.contains_key(handle)
    }
    fn bound(&self, handle: &Handle) -> Result<&Bound> {
        self.connections
            .get(handle)
            .ok_or_else(|| error(ErrorCode::ProtocolFailure, "unknown DuckDB pull handle"))
    }
    pub fn authenticate(&self, handle: &Handle, expected: Option<String>) -> Result<String> {
        let bound = self.bound(handle)?;
        if expected.is_some_and(|expected| expected != bound.identity) {
            return Err(error(
                ErrorCode::RequestMismatch,
                "connection identity changed",
            ));
        }
        if bound.requires_s3.load(std::sync::atomic::Ordering::Acquire) {
            let reader =
                crate::s3_config::load_reader(&bound.engine, &bound.binding.canonical_root)
                    .map_err(|_| {
                        error(
                            ErrorCode::AdapterFailure,
                            "independent DuckDB S3 reader configuration is unavailable or invalid",
                        )
                    })?;
            let mut worker = bound
                .worker
                .lock()
                .map_err(|_| error(ErrorCode::AdapterFailure, "pull owner poisoned"))?;
            if let Some(stop) = &self.cancellation {
                worker.configure_s3_reader(reader, stop)
            } else {
                worker.configure_s3_reader(reader, &AtomicBool::new(false))
            }
            .map_err(|_| {
                error(
                    ErrorCode::AdapterFailure,
                    "independent DuckDB S3 reader authentication failed",
                )
            })?;
        }
        Ok(bound.identity.clone())
    }
    fn fixed(&self, handle: &Handle, request: &RequestRecord) -> Result<pull::PullIdentity> {
        request
            .validate()
            .map_err(|e| error(ErrorCode::ProtocolFailure, e.to_string()))?;
        let bound = self.bound(handle)?;
        if request.connection_identity != bound.identity
            || bound
                .workspace_constraint
                .as_ref()
                .is_some_and(|workspace| &request.workspace_id != workspace)
            || request.root != bound.binding.canonical_root
            || request.adapter_identity.name.as_str() != "duckdb"
            || request.adapter_identity.package_version != env!("CARGO_PKG_VERSION")
        {
            return Err(error(
                ErrorCode::RequestMismatch,
                "fixed pull identity differs from bound destination",
            ));
        }
        let declaration = binding::validate_pull(request.effective_declaration.clone())
            .map_err(|e| failure(e.into()))?;
        if declaration != request.effective_declaration {
            return Err(error(
                ErrorCode::ProtocolFailure,
                "pull request was not purely normalized",
            ));
        }
        let write = match declaration
            .get("write")
            .and_then(Value::as_str)
            .unwrap_or("replace")
        {
            "replace" => pull::WriteMode::Replace,
            "append" => pull::WriteMode::Append,
            _ => return Err(error(ErrorCode::InvalidDeclaration, "invalid write mode")),
        };
        let tables = declaration["tables"]
            .as_array()
            .ok_or_else(|| error(ErrorCode::InvalidDeclaration, "missing tables"))?;
        if declaration["options"]["materialization"] == "s3-view"
            && (!request.root.starts_with("s3://")
                || write != pull::WriteMode::Replace
                || tables.iter().any(|table| {
                    table.get("partitions").is_some() || table.get("select").is_some()
                }))
        {
            return Err(error(
                ErrorCode::InvalidDeclaration,
                "S3 views require complete identity replacement against an S3 root",
            ));
        }
        Ok(pull::PullIdentity {
            binding: WorkspaceBinding {
                canonical_root: bound.binding.canonical_root.clone(),
                workspace_id: request.workspace_id.clone(),
            },
            attempt_id: request.attempt_id.clone(),
            request_sha256: request.request_sha256.clone(),
            adapter_identity: request.adapter_identity.clone(),
            dataset: request.dataset.clone(),
            target_schema: pull::RelationName::new(
                declaration["target"]["schema"].as_str().unwrap(),
            )
            .map_err(|e| failure(e.into()))?,
            write_mode: write,
            scope_mode: if tables.iter().any(|table| table.get("partitions").is_some()) {
                pull::ScopeMode::Selected
            } else {
                pull::ScopeMode::Complete
            },
            transform_mode: if tables.iter().any(|table| table.get("select").is_some()) {
                pull::TransformMode::Sql
            } else {
                pull::TransformMode::Identity
            },
            requested_revision: request.requested_revision.clone(),
        })
    }
    pub fn resolve(
        &mut self,
        handle: Handle,
        request: api::ResolvePullRequest,
        stop: &StopToken,
    ) -> Result<api::PullResolution> {
        request
            .validate()
            .map_err(|e| error(ErrorCode::ProtocolFailure, e.to_string()))?;
        let bound = self.bound(&handle)?;
        if request.root != bound.binding.canonical_root {
            return Err(error(ErrorCode::RequestMismatch, "resolution root differs"));
        }
        if request.phase == ResolvePhase::Compare {
            let fixed = request.request.as_ref().unwrap();
            self.fixed(&handle, fixed)?;
            bound.requires_s3.store(
                fixed.effective_declaration["options"]["materialization"] == "s3-view",
                std::sync::atomic::Ordering::Release,
            );
        }
        let mut worker = bound
            .worker
            .lock()
            .map_err(|_| error(ErrorCode::AdapterFailure, "pull owner poisoned"))?;
        let result = match request.phase {
            ResolvePhase::Lookup => worker.lookup(request.attempt_id, request.root, stop),
            ResolvePhase::Compare => worker.resolve(
                self.fixed(&handle, request.request.as_ref().unwrap())?,
                stop,
            ),
        }
        .map_err(failure)?;
        match result {
            pull::PullResolution::Committed(receipt) => {
                let receipt = receipt
                    .wire_receipt()
                    .map_err(|e| error(ErrorCode::ProtocolFailure, e.to_string()))?;
                Ok(api::PullResolution {
                    state: ResolutionState::Committed,
                    request: Some(receipt.request.clone()),
                    receipt: Some(receipt),
                    recovery: None,
                    prior_source_contracts: vec![],
                })
            }
            _ => {
                let prior_source_contracts = if request.phase == ResolvePhase::Compare {
                    let fixed = request.request.as_ref().unwrap();
                    let tables = fixed.effective_declaration["tables"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|table| serde_json::from_value(table["name"].clone()))
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .map_err(|e| error(ErrorCode::ProtocolFailure, e.to_string()))?;
                    worker
                        .prior_source_contracts(self.fixed(&handle, fixed)?, tables, stop)
                        .map_err(failure)?
                } else {
                    vec![]
                };
                Ok(api::PullResolution {
                    state: ResolutionState::NotCommitted,
                    request: request.request,
                    receipt: None,
                    recovery: None,
                    prior_source_contracts,
                })
            }
        }
    }
    fn native_plan(&self, handle: &Handle, plan: &api::PullPlan) -> Result<pull::PullPlan> {
        let identity = self.fixed(handle, &plan.request)?;
        let materialization_mode =
            if plan.request.effective_declaration["options"]["materialization"] == "s3-view" {
                pull::MaterializationMode::S3View
            } else {
                pull::MaterializationMode::Local
            };
        let declarations = plan.request.effective_declaration["tables"]
            .as_array()
            .unwrap();
        let mut tables = Vec::new();
        for table in &plan.tables {
            let declaration = declarations
                .iter()
                .find(|entry| entry["name"] == table.name.as_str())
                .ok_or_else(|| error(ErrorCode::RequestMismatch, "pull table not declared"))?;
            if table.target != declaration["target"]
                || table.select.as_ref() != declaration.get("select")
            {
                return Err(error(
                    ErrorCode::RequestMismatch,
                    "pull target or query differs from fixed declaration",
                ));
            }
            let sql = table
                .select
                .as_ref()
                .map(|value| {
                    value
                        .get("sql")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        .ok_or_else(|| {
                            error(ErrorCode::ProtocolFailure, "pull SQL file was not expanded")
                        })
                })
                .transpose()?;
            let not_null = plan
                .request
                .effective_declaration
                .get("checks")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter(|check| check["table"] == table.name.as_str())
                .flat_map(|check| {
                    check
                        .get("not_null")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                })
                .map(|column| {
                    column.as_str().map(str::to_owned).ok_or_else(|| {
                        error(ErrorCode::InvalidDeclaration, "invalid not-null check")
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let files = plan
                .files
                .iter()
                .filter(|file| file.table == table.name)
                .map(|file| {
                    let expected_access =
                        if materialization_mode == pull::MaterializationMode::S3View {
                            api::FileAccess::S3View
                        } else {
                            api::FileAccess::Local
                        };
                    if file.access != expected_access {
                        return Err(error(
                            ErrorCode::UnsupportedCapability,
                            "pull file access differs from its fixed materialization",
                        ));
                    }
                    let mut contract = table.source_contract.clone();
                    contract.columns = file.schema.columns.clone();
                    contract
                        .column_ext
                        .as_object_mut()
                        .unwrap()
                        .retain(|name, _| {
                            contract.columns.iter().any(|column| &column.name == name)
                        });
                    Ok(pull::SourceFile {
                        path: if file.access == api::FileAccess::Local {
                            file.location.clone().into()
                        } else {
                            PathBuf::new()
                        },
                        remote: if file.access == api::FileAccess::S3View {
                            Some(file.clone())
                        } else {
                            None
                        },
                        bytes: file.size,
                        sha256: file.sha256.clone(),
                        contract,
                        partition: file.partition.clone(),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            tables.push(pull::PullTable {
                name: table.name.clone(),
                target_table: pull::RelationName::new(
                    table.target["table"].as_str().ok_or_else(|| {
                        error(ErrorCode::InvalidDeclaration, "missing target table")
                    })?,
                )
                .map_err(|e| failure(e.into()))?,
                source_contract: table.source_contract.clone(),
                output_contract: table.output_contract.clone(),
                files,
                sql,
                not_null,
            });
        }
        let selected = plan
            .tables
            .iter()
            .flat_map(|table| {
                table
                    .partitions
                    .iter()
                    .map(|partition| json!({"table":table.name,"partition":partition}))
            })
            .collect::<Vec<_>>();
        let native = pull::PullPlan {
            materialization_mode,
            identity,
            committed_revision: plan.resolved_revision,
            generation_id: plan.plan_id.clone(),
            request_record: Some(plan.request.clone()),
            selected_partitions: Some(json!(selected)),
            tables,
        };
        native.validate().map_err(|e| failure(e.into()))?;
        Ok(native)
    }
    pub fn prepare(
        &mut self,
        handle: Handle,
        request: api::PreparePullRequest,
        stop: &StopToken,
    ) -> Result<api::PullPlan> {
        stop.check()?;
        request
            .validate()
            .map_err(|e| error(ErrorCode::ProtocolFailure, e.to_string()))?;
        if request.recovery.is_some() {
            return Err(error(
                ErrorCode::ProtocolFailure,
                "transactional pull recovery must be null",
            ));
        }
        let identity = self.fixed(&handle, &request.request)?;
        let targets =
            request
                .tables
                .iter()
                .map(|table| {
                    pull::RelationName::new(table.target["table"].as_str().ok_or_else(|| {
                        error(ErrorCode::InvalidDeclaration, "missing target table")
                    })?)
                    .map_err(|e| failure(e.into()))
                })
                .collect::<Result<Vec<_>>>()?;
        let preview = self
            .bound(&handle)?
            .worker
            .lock()
            .map_err(|_| error(ErrorCode::AdapterFailure, "pull owner poisoned"))?
            .preview(identity.clone(), targets, stop)
            .map_err(failure)?;
        let mappings = request
            .tables
            .iter()
            .map(|table| json!({"table":table.name,"target_table":table.target["table"]}))
            .collect::<Vec<_>>();
        let plan = api::PullPlan {
            plan_id: Uuid::v4(),
            request: request.request,
            resolved_revision: request.resolved_revision,
            tables: request.tables,
            files: request.files,
            refresh: Refresh::Full,
            recovery_contract: PullRecovery::Transactional,
            adapter_details: json!({"target_schema":identity.target_schema,"role":if identity.is_identity_materialization(){"identity"}else{"application"},"refresh":"full","mappings":mappings,"prior_revision":preview.prior_revision,"prior_generation_id":preview.prior_generation_id,"obsolete_targets":preview.obsolete_targets}),
        };
        self.native_plan(&handle, &plan)?; // Pure metadata only; no source file open.
        Ok(plan)
    }
    pub fn apply(
        &mut self,
        handle: Handle,
        plan: api::PullPlan,
        stop: &StopToken,
    ) -> Result<api::Receipt> {
        plan.validate()
            .map_err(|e| error(ErrorCode::ProtocolFailure, e.to_string()))?;
        let native = self.native_plan(&handle, &plan)?;
        let bound = self.bound(&handle)?;
        bound
            .worker
            .lock()
            .map_err(|_| error(ErrorCode::AdapterFailure, "pull owner poisoned"))?
            .apply(native, stop)
            .map_err(failure)?
            .wire_receipt()
            .map_err(|e| error(ErrorCode::ProtocolFailure, e.to_string()))
    }
    pub fn stop(&mut self) -> Result<()> {
        for bound in self.connections.values() {
            bound.interrupt.interrupt();
        }
        for (_, bound) in std::mem::take(&mut self.connections) {
            bound
                .worker
                .lock()
                .map_err(|_| error(ErrorCode::AdapterFailure, "pull owner poisoned"))?
                .stop()
                .map_err(|e| failure(e.into()))?;
        }
        Ok(())
    }
}
