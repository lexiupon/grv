//! Stale options-schema peer. Pure offline hooks; no engine/cloud access.
//!
//! Private per-process state: the connection coordinate may name a directory
//! holding controlled fixture state. `binding` is the explicit control marker
//! and must be a closed value; `identity`, `workspace`, `receipt.json`,
//! `unknown` and `audit` are owned fixture data. Present-but-unreadable or
//! invalid state fails closed as OutcomeUnknown; it is never reinterpreted
//! as Uninitialized or as absent history. A plain file coordinate keeps the
//! original behavior exactly.
use grv_adapter_api::*;
use grv_adapter_host::registry::CheckedRegistry;
use grv_adapter_sdk::{Adapter, BoundConnection, PreparedCommand, Registration, Result, StopToken};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
const STATE_FILES: [&str; 6] = [
    "binding",
    "identity",
    "workspace",
    "receipt.json",
    "unknown",
    "audit",
];
struct Stale {
    private: Option<PathBuf>,
}
fn private_dir(connection: &Value) -> Result<Option<PathBuf>> {
    let database = connection["database"]
        .as_str()
        .ok_or_else(|| private_state("database coordinate missing"))?;
    let path = Path::new(database);
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => {
            for name in STATE_FILES {
                match std::fs::symlink_metadata(path.join(name)) {
                    Ok(_) => return Ok(Some(path.to_path_buf())),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(_) => return Err(private_state("state inventory unreadable")),
                }
            }
            Ok(None)
        }
        Ok(metadata) if metadata.is_file() => Ok(None),
        Ok(_) => Err(private_state(
            "database coordinate is not a regular file or directory",
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(private_state("database coordinate unreadable")),
    }
}
// Only an actual missing directory entry is absence. Links (including dangling
// ones), directories and inventory/read failures are not clean missing history.
// This is owned-fixture validation, not an adversarial race-safe file opener.
fn regular_presence(path: &Path) -> Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(true),
        Ok(_) => Err(private_state("state entry is not a regular file")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(private_state("state inventory unreadable")),
    }
}
fn read_line(dir: &Path, name: &str) -> Result<Option<String>> {
    let path = dir.join(name);
    if !regular_presence(&path)? {
        return Ok(None);
    }
    let text = std::fs::read_to_string(path)
        .map_err(|_| private_state("state entry unreadable or invalid UTF-8"))?;
    Ok(Some(text.trim().to_owned()))
}
fn private_state(message: &str) -> grv_types::PublicError {
    grv_types::PublicError {
        code: ErrorCode::OutcomeUnknown,
        message: format!("fixture private state is incomplete or invalid: {message}"),
        retryable: false,
        object: None,
    }
}
fn closed_binding(dir: &Path) -> Result<BindingState> {
    let binding = read_line(dir, "binding")?
        .ok_or_else(|| private_state("binding marker missing or unreadable"))?;
    match binding.as_str() {
        "bound" => Ok(BindingState::Bound),
        "uninitialized" => Ok(BindingState::Uninitialized),
        "not_applicable" => Ok(BindingState::NotApplicable),
        _ => Err(private_state("binding marker is not a closed value")),
    }
}
#[allow(clippy::result_large_err)]
fn forbidden<T>() -> Result<T> {
    std::fs::write("forbidden-auth-or-effect", b"unexpected dispatch").unwrap();
    Err(grv_types::PublicError {
        code: ErrorCode::AdapterFailure,
        message: "fixture unexpected auth/effect".into(),
        retryable: false,
        object: None,
    })
}
impl Adapter for Stale {
    fn stop_and_wait(&mut self) -> Result<()> {
        Ok(())
    }
    fn registration(&self) -> Registration {
        let mut r = grv_adapter_duckdb::process::DuckDbAdapter::default().registration();
        // Identity/schema agree. Capability knows GCS, options schema does not.
        r.capabilities
            .pull_materializations
            .push(Materialization::GcsView);
        r.registry.schema_bundle["$defs"]["options"]["properties"]["materialization"]["enum"] =
            json!(["local", "s3-view"]);
        r
    }
    fn prepare_command(&self, _: &Name, _: &[String]) -> Result<PreparedCommand> {
        forbidden()
    }
    fn execute_command(&mut self, _: &CommandCall, _: &StopToken) -> Result<Value> {
        forbidden()
    }
    fn validate_binding(&self, declaration: Value, mode: Mode, schema: Req) -> Result<Value> {
        grv_adapter_duckdb::process::DuckDbAdapter::default().validate_binding(
            declaration,
            mode,
            schema,
        )
    }
    fn locate_connection(
        &self,
        connection: Value,
        _: Mode,
        _: Option<RunId>,
    ) -> Result<ConnectionLocator> {
        let database = connection["database"].as_str().unwrap().to_owned();
        let identity = match private_dir(&connection)? {
            Some(dir) => {
                // Present private state fails closed on the closed marker and
                // the owned identity; it never degrades to Uninitialized.
                closed_binding(&dir)?;
                read_line(&dir, "identity")?
                    .filter(|identity| !identity.is_empty())
                    .ok_or_else(|| private_state("identity missing or unreadable"))?
            }
            None => database,
        };
        Ok(ConnectionLocator {
            identity: Some(identity),
            canonical_connection: connection,
            engine_path: None,
            session_lock_path: None,
        })
    }
    fn bind_connection(
        &mut self,
        locator: ConnectionLocator,
        _: Option<String>,
        _: Option<String>,
        _: Option<Uuid>,
        _: Mode,
    ) -> Result<BoundConnection> {
        let (state, workspace) = match private_dir(&locator.canonical_connection)? {
            Some(dir) => {
                let state = closed_binding(&dir)?;
                let workspace = match state {
                    BindingState::Bound => {
                        let text = read_line(&dir, "workspace")?
                            .ok_or_else(|| private_state("bound state lacks a workspace file"))?;
                        let workspace = Uuid::new(&text).map_err(|_| {
                            private_state("bound state lacks a valid workspace uuid")
                        })?;
                        Some(workspace)
                    }
                    _ => None,
                };
                self.private = Some(dir);
                (state, workspace)
            }
            None => (BindingState::Uninitialized, None),
        };
        let label = match state {
            BindingState::Bound => "bound",
            BindingState::Uninitialized => "uninitialized",
            BindingState::NotApplicable => "not_applicable",
        };
        Ok(BoundConnection {
            handle: Handle::new("offline-stale-peer").unwrap(),
            identity: locator.identity,
            workspace_id: workspace.clone(),
            binding: state,
            details: json!({"database":locator.canonical_connection["database"],"workspace_id":workspace,"binding":label}),
        })
    }
    fn resolve_pull(
        &mut self,
        _: Handle,
        query: ResolvePullRequest,
        _: &StopToken,
    ) -> Result<PullResolution> {
        if let Some(dir) = &self.private {
            // Read-only probe evidence goes to a dedicated audit directory
            // outside every protected snapshot; a failed instrumentation
            // write fails the invocation instead of being swallowed.
            if let Some(audit) = read_line(dir, "audit")? {
                let marker = match query.phase {
                    ResolvePhase::Lookup => "probe-lookup",
                    ResolvePhase::Compare => "probe-compare",
                };
                std::fs::write(Path::new(&audit).join(marker), query.attempt_id.as_str())
                    .map_err(|e| private_state(&format!("audit write failed: {e}")))?;
            }
            if regular_presence(&dir.join("unknown"))? {
                return Ok(PullResolution {
                    state: ResolutionState::Unknown,
                    request: None,
                    receipt: None,
                    recovery: None,
                    prior_source_contracts: vec![],
                });
            }
            if regular_presence(&dir.join("receipt.json"))? {
                // Any present receipt that is unreadable or not a valid closed
                // typed record is unknown history, never absence.
                let raw = std::fs::read_to_string(dir.join("receipt.json"))
                    .map_err(|_| private_state("receipt unreadable"))?;
                let receipt = serde_json::from_str::<Receipt>(&raw)
                    .map_err(|_| private_state("receipt is not a closed typed record"))?;
                if receipt.validate().is_err() {
                    return Ok(PullResolution {
                        state: ResolutionState::Unknown,
                        request: None,
                        receipt: None,
                        recovery: None,
                        prior_source_contracts: vec![],
                    });
                }
                let registry = CheckedRegistry::recorded(receipt.request.registry.clone())
                    .map_err(|_| private_state("receipt registry is not closed"))?;
                if registry
                    .validate_point(
                        Mode::Pull,
                        "pull_result",
                        &receipt.adapter_result,
                        ErrorCode::ProtocolFailure,
                    )
                    .is_err()
                {
                    return Ok(PullResolution {
                        state: ResolutionState::Unknown,
                        request: None,
                        receipt: None,
                        recovery: None,
                        prior_source_contracts: vec![],
                    });
                }
                return Ok(PullResolution {
                    state: ResolutionState::Committed,
                    request: Some(receipt.request.clone()),
                    receipt: Some(receipt),
                    recovery: None,
                    prior_source_contracts: vec![],
                });
            }
        }
        Ok(PullResolution {
            state: ResolutionState::NotCommitted,
            request: query.request,
            receipt: None,
            recovery: None,
            prior_source_contracts: vec![],
        })
    }
    fn authenticate(&mut self, _: Handle, _: Option<String>) -> Result<String> {
        forbidden()
    }
    fn prepare_pull(
        &mut self,
        _: Handle,
        _: PreparePullRequest,
        _: &StopToken,
    ) -> Result<PullPlan> {
        forbidden()
    }
    fn apply_pull(&mut self, _: Handle, _: PullPlan, _: &StopToken) -> Result<Receipt> {
        forbidden()
    }
}
fn main() {
    if grv_adapter_sdk::run_fd3(Stale { private: None }).is_err() {
        std::process::exit(6);
    }
}
