//! Destination receipts are authoritative, including when private state exists.
use super::{Failure, Success, backend::Root};
use grv_adapter_api::{BindingState, Mode, PullPlan, Receipt, RequestRecord, ResolutionState};
use grv_adapter_host::{
    discovery,
    process::{Deadlines, Session},
    registry::CheckedRegistry,
};
use grv_core::{
    declaration,
    journal::{Envelope, Evidence, Journal},
    pull::{self, Comparison, TableRequest},
    source::TableSelection,
    store::{Result, Store, public_error},
};
use grv_types::{
    AdapterIdentity, DeclarationIdentity, ErrorCode, LatestRevision, Name, PullRequestIdentity,
    RequestedRevision, Uuid,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, path::PathBuf};

fn invalid(message: &str) -> grv_types::PublicError {
    public_error(ErrorCode::InvalidArgument, message)
}
fn mismatch() -> grv_types::PublicError {
    public_error(
        ErrorCode::RequestMismatch,
        "pull attempt belongs to another fixed request",
    )
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixed {
    request: RequestRecord,
    canonical_connection: Value,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Progress {
    plan: Option<PullPlan>,
}
type Record = Envelope<Fixed, Value, Progress, Receipt>;
struct Options {
    root: String,
    decl: PathBuf,
    state: PathBuf,
    attempt: Uuid,
    revision: Option<RequestedRevision>,
}
fn options(args: &[String]) -> Result<Options> {
    let (mut root, mut decl, mut state, mut attempt, mut revision) = (None, None, None, None, None);
    let mut seen = BTreeSet::new();
    for pair in args.chunks(2) {
        if pair.len() != 2 || !seen.insert(pair[0].as_str()) {
            return Err(invalid("pull flags require unique names and values"));
        }
        match pair[0].as_str() {
            "--grv" => root = Some(pair[1].clone()),
            "--decl" => decl = Some(PathBuf::from(&pair[1])),
            "--state" => state = Some(PathBuf::from(&pair[1])),
            "--attempt" => {
                attempt = Some(
                    Uuid::new(&pair[1]).map_err(|_| invalid("attempt must be a canonical UUID"))?,
                )
            }
            "--revision" => {
                revision = Some(serde_json::from_value(json!(pair[1])).map_err(|_| {
                    invalid("revision must be latest or a canonical nonnegative integer")
                })?)
            }
            _ => return Err(invalid("unsupported pull flag")),
        }
    }
    let state = state.unwrap_or_else(|| {
        if let Some(path) = std::env::var_os("XDG_STATE_HOME") {
            PathBuf::from(path).join("grv")
        } else if cfg!(target_os = "macos") {
            PathBuf::from(std::env::var_os("HOME").unwrap_or_default())
                .join("Library/Application Support/grv/state")
        } else {
            PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join(".local/state/grv")
        }
    });
    Ok(Options {
        root: root.ok_or_else(|| invalid("pull requires --grv <root>"))?,
        decl: decl.ok_or_else(|| invalid("pull requires --decl <yaml>"))?,
        state,
        attempt: attempt.unwrap_or_else(Uuid::v4),
        revision,
    })
}
pub(super) fn pull(args: &[String]) -> std::result::Result<Success, Failure> {
    let options = options(args)?;
    let root = Root::parse(&options.root)?;
    let canonical = root.canonical.clone();
    run(options, root).map_err(|mut failure| {
        failure.root = Some(canonical);
        failure
    })
}
fn normalize(
    mut authored: Value,
    override_revision: Option<RequestedRevision>,
) -> Result<(Value, RequestedRevision)> {
    if declaration::mode(&authored).map_err(|_| invalid("invalid pull declaration"))? != Mode::Pull
    {
        return Err(invalid("pull requires a pull declaration"));
    }
    let selector = match override_revision {
        Some(selector) => selector,
        None => match authored.get("revision") {
            None => RequestedRevision::Latest(LatestRevision::Latest),
            Some(value) if value == "latest" => RequestedRevision::Latest(LatestRevision::Latest),
            Some(value) => serde_json::from_value(json!(
                value
                    .as_u64()
                    .ok_or_else(|| invalid("invalid revision selector"))?
                    .to_string()
            ))
            .map_err(|_| invalid("invalid revision selector"))?,
        },
    };
    authored["revision"] = serde_json::to_value(&selector).unwrap();
    if authored.get("write").is_none() {
        authored["write"] = json!("replace");
    }
    Ok((authored, selector))
}
/// Terminal comparison uses recorded whole-point and nested defaults. Explicit
/// caller values and every common field must still match exactly.
fn replay_declaration(
    authored: &Value,
    request: &RequestRecord,
    session: &mut Session,
) -> Result<Value> {
    let registry = CheckedRegistry::recorded(request.registry.clone()).map_err(|e| e.public())?;
    let mut original = authored.clone();
    declaration::apply_point_defaults(&mut original, &registry).map_err(|_| mismatch())?;
    if let Ok(effective) = declaration::replay_effective(
        &original,
        &request.validation_input,
        &request.effective_declaration,
        &registry,
    ) {
        return Ok(effective);
    }
    // Equivalent explicit/default authoring can require pure validation (for
    // example omitting an explicitly authored default). Only the exact recorded
    // validator identity and registry may provide that extra evidence. It still
    // must produce the exact recorded declaration; no authentication follows.
    let current = &session.descriptor;
    let recorded = &request.adapter_identity;
    if current.name != recorded.name
        || current.package_version != recorded.package_version
        || current.interface_version != recorded.interface_version
        || current.binding_schema_version != recorded.binding_schema_version
        || session.registry.registry != request.registry
    {
        return Err(mismatch());
    }
    let effective = session
        .validate_binding(original.clone(), Mode::Pull)
        .map_err(|_| mismatch())?;
    declaration::validate_effective(&original, &effective, &registry).map_err(|_| mismatch())?;
    if effective != request.effective_declaration {
        return Err(mismatch());
    }
    Ok(effective)
}
fn digest(
    effective: Value,
    adapter: AdapterIdentity,
    identity: String,
    coordinates: Value,
) -> Result<grv_types::Digest> {
    grv_types::declaration_digest(&DeclarationIdentity {
        effective_declaration: effective,
        adapter_identity: adapter,
        connection_identity: identity,
        canonical_connection: coordinates,
    })
    .map_err(|_| {
        public_error(
            ErrorCode::InvalidDeclaration,
            "pull declaration is not JCS interoperable",
        )
    })
}
fn run(options: Options, root: Root) -> std::result::Result<Success, Failure> {
    let authored = declaration::load(&options.decl).map_err(|_| {
        public_error(
            ErrorCode::InvalidDeclaration,
            "declaration validation failed",
        )
    })?;
    let (authored, selector) = normalize(authored, options.revision)?;
    let name = Name::new(authored["adapter"].as_str().unwrap())
        .map_err(|_| invalid("invalid adapter name"))?;
    let directory = options.state.join("pull").join(options.attempt.as_str());
    let mut journal = if directory
        .try_exists()
        .map_err(|_| public_error(ErrorCode::BackendFailure, "consumer state unavailable"))?
    {
        Some(Journal::open(&directory, root.exclusions())?)
    } else {
        None
    };
    let mut recorded: Option<Record> = journal.as_ref().map(Journal::read).transpose()?;
    if recorded.as_ref().is_some_and(|r| {
        r.evidence.intent.request.attempt_id != options.attempt
            || r.evidence.intent.request.root != root.canonical
    }) {
        return Err(mismatch().into());
    }
    let installations = discovery::discover(&discovery::search_roots(None)?)?;
    let installed = installations
        .iter()
        .find(|i| i.manifest.name == name)
        .ok_or_else(|| {
            public_error(
                ErrorCode::NotFound,
                "destination adapter installation unavailable",
            )
        })?;
    let path = std::fs::canonicalize(&options.decl)
        .map_err(|_| invalid("declaration path unavailable"))?;
    let mut session = Session::spawn_at(installed, Deadlines::default(), path.parent())?;
    // Locating and binding are offline. No validation against today's defaults
    // or destination authentication may precede durable receipt lookup.
    let locator = session.locate_connection(authored["connection"].clone(), Mode::Pull, None)?;
    let coordinates = locator.canonical_connection.clone();
    let bound = session.bind_connection(
        locator.clone(),
        Some(root.canonical.clone()),
        locator.identity.clone(),
        recorded
            .as_ref()
            .map(|r| r.evidence.intent.request.workspace_id.clone()),
        Mode::Pull,
    )?;
    let found = pull::lookup(
        &mut session,
        bound.handle.clone(),
        options.attempt.clone(),
        root.canonical.clone(),
    )?;
    if found.state == ResolutionState::Busy {
        return Err(public_error(
            ErrorCode::EngineBusy,
            "former destination writer is still active",
        )
        .into());
    }
    if found.state == ResolutionState::Unknown {
        return Err(public_error(
            ErrorCode::OutcomeUnknown,
            "destination history cannot establish the outcome",
        )
        .into());
    }
    if let Some(receipt) = found.receipt {
        let request = &receipt.request;
        let effective = replay_declaration(&authored, request, &mut session)?;
        if request.requested_revision != selector
            || bound.workspace_id.as_ref() != Some(&request.workspace_id)
            || bound.identity.as_ref() != Some(&request.connection_identity)
            || digest(
                effective,
                request.adapter_identity.clone(),
                request.connection_identity.clone(),
                coordinates.clone(),
            )? != request.declaration_sha256
            || recorded.as_ref().is_some_and(|r| {
                r.evidence.intent.request != *request
                    || r.evidence.intent.canonical_connection != coordinates
            })
        {
            return Err(mismatch().into());
        }
        let receipt =
            match pull::compare(&mut session, bound.handle.clone(), request.clone(), |_| {
                Ok(())
            })? {
                Comparison::Committed(receipt) => receipt,
                Comparison::NotCommitted(_) => {
                    return Err(public_error(
                        ErrorCode::ProtocolFailure,
                        "committed receipt disappeared during comparison",
                    )
                    .into());
                }
            };
        let maintenance = session.close().err().map(|e| e.public());
        return finish(
            receipt,
            true,
            maintenance,
            &root.canonical,
            journal.as_ref(),
            &mut recorded,
        );
    }
    if recorded
        .as_ref()
        .is_some_and(|r| r.evidence.terminal.is_some())
    {
        return Err(public_error(
            ErrorCode::OutcomeUnknown,
            "destination lost a previously committed receipt",
        )
        .into());
    }
    if !session.capabilities.pull {
        return Err(public_error(
            ErrorCode::UnsupportedCapability,
            "adapter does not advertise a complete pull lifecycle",
        )
        .into());
    }
    let mut original = authored;
    declaration::apply_point_defaults(&mut original, &session.registry).map_err(|_| {
        public_error(
            ErrorCode::InvalidDeclaration,
            "adapter defaults are invalid",
        )
    })?;
    let effective = session.validate_binding(original.clone(), Mode::Pull)?;
    declaration::validate_effective(&original, &effective, &session.registry).map_err(|_| {
        public_error(
            ErrorCode::InvalidDeclaration,
            "adapter declaration validation failed",
        )
    })?;
    let identity = match bound.identity.clone() {
        Some(identity) => identity,
        None => session.authenticate(bound.handle.clone(), None)?,
    };
    let workspace = match bound.binding {
        BindingState::Bound => bound.workspace_id.clone().ok_or_else(|| {
            public_error(
                ErrorCode::ProtocolFailure,
                "bound destination lacks workspace identity",
            )
        })?,
        BindingState::Uninitialized => recorded
            .as_ref()
            .map(|r| r.evidence.intent.request.workspace_id.clone())
            .unwrap_or_else(Uuid::v4),
        BindingState::NotApplicable => {
            return Err(public_error(
                ErrorCode::UnsupportedCapability,
                "pull destination has no durable workspace identity",
            )
            .into());
        }
    };
    let adapter = AdapterIdentity {
        name: session.descriptor.name.clone(),
        package_version: session.descriptor.package_version.clone(),
        interface_version: session.descriptor.interface_version,
        binding_schema_version: session.descriptor.binding_schema_version,
    };
    let declaration_sha256 = digest(
        effective.clone(),
        adapter.clone(),
        identity.clone(),
        coordinates.clone(),
    )?;
    let request_sha256 = grv_types::pull_request_digest(&PullRequestIdentity {
        root: root.canonical.clone(),
        workspace_id: workspace.clone(),
        declaration_sha256: declaration_sha256.clone(),
        requested_revision: selector.clone(),
    })
    .map_err(|_| invalid("invalid fixed pull identity"))?;
    let request = RequestRecord {
        attempt_id: options.attempt.clone(),
        root: root.canonical.clone(),
        dataset: Name::new(effective["dataset"].as_str().unwrap()).unwrap(),
        workspace_id: workspace,
        adapter_identity: adapter,
        connection_identity: identity.clone(),
        effective_declaration: effective.clone(),
        validation_input: original,
        declaration_sha256,
        request_sha256,
        registry: session.registry.registry.clone(),
        requested_revision: selector,
    };
    request
        .validate()
        .map_err(|_| invalid("invalid fixed pull request"))?;
    if found
        .request
        .as_ref()
        .is_some_and(|fixed| fixed != &request)
    {
        return Err(mismatch().into());
    }
    let fixed = Fixed {
        request: request.clone(),
        canonical_connection: coordinates,
    };
    if let Some(record) = &recorded {
        if record.evidence.intent.request != fixed.request
            || record.evidence.intent.canonical_connection != fixed.canonical_connection
        {
            return Err(mismatch().into());
        }
    } else {
        eprintln!("attempt {}", options.attempt);
        let created = Journal::create(&directory, root.exclusions())?;
        recorded = Some(created.create_evidence(Evidence {
            intent: fixed,
            capture: None,
            progress: Progress { plan: None },
            terminal: None,
        })?);
        journal = Some(created);
    }
    let authorization =
        match pull::compare(&mut session, bound.handle.clone(), request, |_| Ok(()))? {
            Comparison::Committed(receipt) => {
                let maintenance = session.close().err().map(|e| e.public());
                return finish(
                    receipt,
                    true,
                    maintenance,
                    &root.canonical,
                    journal.as_ref(),
                    &mut recorded,
                );
            }
            Comparison::NotCommitted(authorization) => authorization,
        };
    let materialization = if effective["options"]["materialization"] == "s3-view" {
        grv_adapter_api::Materialization::S3View
    } else {
        grv_adapter_api::Materialization::Local
    };
    if !session
        .capabilities
        .pull_materializations
        .contains(&materialization)
    {
        return Err(public_error(
            ErrorCode::UnsupportedCapability,
            "adapter does not advertise the requested pull materialization",
        )
        .into());
    }
    let authenticated = session.authenticate(bound.handle.clone(), Some(identity.clone()))?;
    if authenticated != identity {
        return Err(mismatch().into());
    }
    let tables = table_requests(&effective)?;
    let journal = journal.as_ref().unwrap();
    let prepared = pull::prepare(
        &mut session,
        bound.handle.clone(),
        authorization,
        tables,
        || Store::open(root.open(false)?),
        journal.directory(),
        |plan| {
            let record = recorded.as_mut().unwrap();
            let mut next = record.evidence.clone();
            next.progress.plan = Some(plan.clone());
            *record = journal.compare_and_swap(record.generation, next)?;
            Ok(())
        },
    )?;
    let applied = pull::apply(&mut session, bound.handle, prepared)?;
    finish(
        applied.receipt,
        false,
        applied.maintenance_error,
        &root.canonical,
        Some(journal),
        &mut recorded,
    )
}
fn table_requests(effective: &Value) -> Result<Vec<TableRequest>> {
    effective["tables"]
        .as_array()
        .unwrap()
        .iter()
        .map(|table| {
            let expect = table.get("expect");
            let expect_columns = expect
                .and_then(|v| v.get("columns"))
                .map(|_| declaration::table_contract(&table["expect"], false).map(|c| c.columns))
                .transpose()
                .map_err(|_| {
                    public_error(
                        ErrorCode::InvalidDeclaration,
                        "invalid expected source columns",
                    )
                })?;
            let expect_partition_keys = expect
                .and_then(|v| v.get("partition_keys"))
                .map(|v| serde_json::from_value(v.clone()))
                .transpose()
                .map_err(|_| {
                    public_error(
                        ErrorCode::InvalidDeclaration,
                        "invalid expected partition keys",
                    )
                })?;
            Ok(TableRequest {
                selection: TableSelection {
                    table: Name::new(table["name"].as_str().unwrap()).unwrap(),
                    partitions: table
                        .get("partitions")
                        .map(|p| serde_json::from_value(p.clone()))
                        .transpose()
                        .map_err(|_| {
                            public_error(
                                ErrorCode::InvalidDeclaration,
                                "invalid partition selection",
                            )
                        })?,
                    expect_columns,
                    expect_partition_keys,
                    prior_source_contract: None,
                },
                target: table["target"].clone(),
                select: table.get("select").cloned(),
                output_contract: table
                    .get("columns")
                    .map(|_| declaration::table_contract(table, true))
                    .transpose()
                    .map_err(|_| {
                        public_error(ErrorCode::InvalidDeclaration, "invalid output contract")
                    })?,
            })
        })
        .collect()
}
fn finish(
    receipt: Receipt,
    replayed: bool,
    maintenance: Option<grv_types::PublicError>,
    root: &str,
    journal: Option<&Journal>,
    recorded: &mut Option<Record>,
) -> std::result::Result<Success, Failure> {
    let request = &receipt.request;
    let result = json!({"adapter":request.adapter_identity.name,"attempt_id":request.attempt_id,"workspace_id":request.workspace_id,"dataset":request.dataset,"requested_revision":request.requested_revision,"committed_revision":receipt.committed_revision,"generation_id":receipt.generation_id,"pulled_at":receipt.pulled_at,"replayed":replayed,"adapter_result":receipt.adapter_result});
    let persistence = if let (Some(journal), Some(record)) = (journal, recorded.as_mut()) {
        let mut next = record.evidence.clone();
        next.terminal = Some(receipt);
        journal
            .compare_and_swap(record.generation, next)
            .map(|updated| {
                *record = updated;
            })
    } else {
        Ok(())
    };
    if let Some(error) = maintenance.or_else(|| persistence.err()) {
        return Err(Failure {
            error,
            result: Some(result),
            root: Some(root.into()),
        });
    }
    Ok(Success {
        result,
        root: Some(root.into()),
    })
}
