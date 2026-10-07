//! Parent pull ordering: trusted destination resolution precedes source reads;
//! the exact verified plan becomes durable before one apply call.
use crate::{
    source,
    store::{Result, Store, public_error},
};
use grv_adapter_api::{
    FileAccess, Handle, Materialization, NamedContract, PreparePullRequest, PullPlan, Receipt,
    RequestRecord, ResolutionState, ResolvePhase, ResolvePullRequest, TableContract,
};
use grv_adapter_host::process::Session;
use grv_storage::Backend;
use grv_types::{ErrorCode, Name, Uuid};
use serde_json::Value;
use std::path::Path;

pub enum Comparison {
    Committed(Receipt),
    NotCommitted(AuthorizedPull),
}
/// Only successful fixed-request comparison constructs source authority.
pub struct AuthorizedPull {
    request: RequestRecord,
    recovery: Option<Value>,
    prior_source_contracts: Vec<NamedContract>,
}
impl AuthorizedPull {
    pub fn request(&self) -> &RequestRecord {
        &self.request
    }
}
/// Lookup deliberately takes no source backend or authentication callback.
pub fn lookup(
    session: &mut Session,
    handle: Handle,
    attempt_id: Uuid,
    root: String,
) -> Result<grv_adapter_api::PullResolution> {
    session
        .resolve_pull(
            handle,
            ResolvePullRequest {
                phase: ResolvePhase::Lookup,
                attempt_id,
                root,
                request: None,
            },
        )
        .map_err(|e| e.public())
}
/// Persist the exact fixed request, including a proposed uninitialized workspace
/// UUID, before compare. Authentication for execution follows this resolution.
pub fn compare(
    session: &mut Session,
    handle: Handle,
    request: RequestRecord,
    persist: impl FnOnce(&RequestRecord) -> Result<()>,
) -> Result<Comparison> {
    request
        .validate()
        .map_err(|e| public_error(ErrorCode::InvalidDeclaration, e.to_string()))?;
    persist(&request)?;
    let resolution = session
        .resolve_pull(
            handle,
            ResolvePullRequest {
                phase: ResolvePhase::Compare,
                attempt_id: request.attempt_id.clone(),
                root: request.root.clone(),
                request: Some(request.clone()),
            },
        )
        .map_err(|e| e.public())?;
    match resolution.state {
        ResolutionState::Committed => Ok(Comparison::Committed(
            resolution.receipt.expect("validated committed receipt"),
        )),
        ResolutionState::NotCommitted => Ok(Comparison::NotCommitted(AuthorizedPull {
            request,
            recovery: resolution.recovery,
            prior_source_contracts: resolution.prior_source_contracts,
        })),
        ResolutionState::Busy => Err(public_error(
            ErrorCode::EngineBusy,
            "former destination writer is still active",
        )),
        ResolutionState::Unknown => Err(public_error(
            ErrorCode::OutcomeUnknown,
            "destination outcome has no trustworthy resolution",
        )),
    }
}
pub struct TableRequest {
    pub selection: source::TableSelection,
    pub target: Value,
    pub select: Option<Value>,
    /// Required for transformed pulls; an exact assertion for identity pulls.
    pub output_contract: Option<TableContract>,
}
pub struct PreparedPull {
    plan: PullPlan,
    // Verified file lifetime ends only after apply, close, or cancellation stops
    // the adapter reader. Do not drop this before stopping a failed session.
    source: source::VerifiedSelection,
}
impl PreparedPull {
    pub fn plan(&self) -> &PullPlan {
        &self.plan
    }
    pub fn verified_files(&self) -> &[grv_adapter_api::VerifiedFile] {
        &self.source.files
    }
}
/// The source callback is invoked only after trustworthy not-committed evidence.
/// Callers authenticate the bound destination before calling this function.
pub fn prepare<B: Backend>(
    session: &mut Session,
    handle: Handle,
    authorization: AuthorizedPull,
    mut tables: Vec<TableRequest>,
    open_source: impl FnOnce() -> Result<Store<B>>,
    scratch: &Path,
    persist: impl FnOnce(&PullPlan) -> Result<()>,
) -> Result<PreparedPull> {
    if !session.capabilities.pull {
        return Err(public_error(
            ErrorCode::UnsupportedCapability,
            "adapter pull lifecycle is not advertised",
        ));
    }
    let (access, materialization) =
        if authorization.request.effective_declaration["options"]["materialization"] == "s3-view" {
            (FileAccess::S3View, Materialization::S3View)
        } else {
            (FileAccess::Local, Materialization::Local)
        };
    if !session
        .capabilities
        .pull_materializations
        .contains(&materialization)
    {
        return Err(public_error(
            ErrorCode::UnsupportedCapability,
            "adapter does not advertise the requested pull materialization",
        ));
    }
    let fixed_names: Vec<Name> = authorization.request.effective_declaration["tables"]
        .as_array()
        .ok_or_else(|| {
            public_error(
                ErrorCode::InvalidDeclaration,
                "fixed pull has no table declarations",
            )
        })?
        .iter()
        .map(|table| {
            table["name"]
                .as_str()
                .and_then(|name| Name::new(name).ok())
                .ok_or_else(|| {
                    public_error(ErrorCode::InvalidDeclaration, "invalid fixed pull table")
                })
        })
        .collect::<Result<_>>()?;
    if fixed_names
        != tables
            .iter()
            .map(|table| table.selection.table.clone())
            .collect::<Vec<_>>()
    {
        return Err(public_error(
            ErrorCode::RequestMismatch,
            "source selection changed the fixed declared table scope",
        ));
    }
    for (table, fixed) in tables.iter_mut().zip(
        authorization.request.effective_declaration["tables"]
            .as_array()
            .unwrap(),
    ) {
        let partitions = table
            .selection
            .partitions
            .as_ref()
            .map(|value| serde_json::to_value(value).expect("typed selectors"));
        if fixed.get("target") != Some(&table.target)
            || fixed.get("select") != table.select.as_ref()
            || fixed.get("partitions") != partitions.as_ref()
        {
            return Err(public_error(
                ErrorCode::RequestMismatch,
                "pull mapping or partition selection differs from the fixed declaration",
            ));
        }
        let expected_columns = fixed
            .get("expect")
            .and_then(|expect| expect.get("columns"))
            .map(|_| {
                crate::declaration::table_contract(&fixed["expect"], false)
                    .map(|contract| contract.columns)
            })
            .transpose()
            .map_err(|e| public_error(ErrorCode::InvalidDeclaration, e.to_string()))?;
        let expected_keys: Option<Vec<Name>> = fixed
            .get("expect")
            .and_then(|expect| expect.get("partition_keys"))
            .map(|keys| serde_json::from_value(keys.clone()))
            .transpose()
            .map_err(|_| {
                public_error(
                    ErrorCode::InvalidDeclaration,
                    "invalid fixed partition expectation",
                )
            })?;
        let expected_output = fixed
            .get("columns")
            .map(|_| crate::declaration::table_contract(fixed, true))
            .transpose()
            .map_err(|e| public_error(ErrorCode::InvalidDeclaration, e.to_string()))?;
        if table.selection.expect_columns != expected_columns
            || table.selection.expect_partition_keys != expected_keys
            || table.output_contract != expected_output
        {
            return Err(public_error(
                ErrorCode::RequestMismatch,
                "pull contracts differ from the fixed declaration",
            ));
        }
        // Prior evidence comes exclusively from the adapter's checked comparison,
        // never caller guesses or a newer durable table-wide schema baseline.
        table.selection.prior_source_contract = authorization
            .prior_source_contracts
            .iter()
            .find(|prior| prior.table == table.selection.table)
            .map(|prior| prior.contract.clone());
        if table.select.is_some() && table.output_contract.is_none() {
            return Err(public_error(
                ErrorCode::InvalidDeclaration,
                "transformed pull requires an output contract",
            ));
        }
    }
    let selections: Vec<_> = tables
        .iter_mut()
        .map(|table| source::TableSelection {
            table: table.selection.table.clone(),
            partitions: table.selection.partitions.take(),
            expect_columns: table.selection.expect_columns.take(),
            expect_partition_keys: table.selection.expect_partition_keys.take(),
            prior_source_contract: table.selection.prior_source_contract.take(),
        })
        .collect();
    let store = open_source()?;
    let selected = source::verify_with_access(
        &store,
        &authorization.request.dataset,
        &authorization.request.requested_revision,
        &selections,
        scratch,
        access,
    )?;
    let mut verified = Vec::new();
    for (table, selected_table) in tables.into_iter().zip(&selected.tables) {
        let output = match table.output_contract {
            Some(mut declared) if table.select.is_none() => {
                if declared.columns != selected_table.contract.columns {
                    return Err(public_error(
                        ErrorCode::InvalidDeclaration,
                        "identity pull output assertion differs from selected source",
                    ));
                }
                declared.partition_keys = selected_table.contract.partition_keys.clone();
                declared
            }
            Some(declared) => declared,
            None => selected_table.contract.clone(),
        };
        verified.push(grv_adapter_api::PullTable {
            name: selected_table.table.clone(),
            target: table.target,
            select: table.select,
            partitions: selected_table
                .partitions
                .iter()
                .map(|partition| serde_json::to_value(partition).expect("validated partition"))
                .collect(),
            source_contract: selected_table.contract.clone(),
            output_contract: output,
        });
    }
    let result = session.prepare_pull(
        handle,
        PreparePullRequest {
            request: authorization.request,
            resolved_revision: selected.revision,
            tables: verified,
            files: selected.files.clone(),
            recovery: authorization.recovery,
        },
    );
    let plan = match result {
        Ok(plan) => plan,
        Err(error) => {
            let _ = session.close();
            return Err(error.public());
        }
    };
    if let Err(error) = persist(&plan) {
        // Failed durable plan persistence must stop readers before staged paths
        // can disappear; ordinary apply is never sent.
        let _ = session.close();
        return Err(error);
    }
    Ok(PreparedPull {
        plan,
        source: selected,
    })
}
pub struct AppliedPull {
    pub receipt: Receipt,
    pub maintenance_error: Option<grv_types::PublicError>,
}
/// Consumes the persisted plan once. Close failure cannot erase a known receipt.
pub fn apply(session: &mut Session, handle: Handle, prepared: PreparedPull) -> Result<AppliedPull> {
    let result = session.apply_pull(handle, prepared.plan.clone());
    match result {
        Ok(receipt) => {
            let maintenance_error = session.close().err().map(|e| e.public());
            drop(prepared);
            Ok(AppliedPull {
                receipt,
                maintenance_error,
            })
        }
        Err(error) => {
            let _ = session.close();
            drop(prepared);
            Err(error.public())
        }
    }
}
