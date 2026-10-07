//! Backend facts are observed first. Optional declaration inspection adds
//! read-only adapter facts without creating consumer state or authenticating.
use super::{Failure, Success, backend::Root};
use grv_core::{
    clock::SystemClock,
    inspection::Inspector,
    store::{Result, Store, public_error},
};
use grv_storage::model::Counter;
use grv_types::{ErrorCode, Name, U64};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};
struct Options {
    root: String,
    dataset: Option<Name>,
    table: Option<Name>,
    revision: Option<Counter>,
    from: Option<Counter>,
    to: Option<Option<Counter>>,
    limit: usize,
    full: bool,
    retention: bool,
    declaration: Option<PathBuf>,
}
fn invalid(message: &str) -> grv_types::PublicError {
    public_error(ErrorCode::InvalidArgument, message)
}
fn counter(value: &str) -> Result<Counter> {
    let number = value
        .parse::<U64>()
        .map_err(|_| invalid("revision must be a canonical nonnegative int64 decimal"))?;
    Counter::new(number.get()).map_err(|_| invalid("revision exceeds int64"))
}
fn options(command: &str, args: &[String]) -> Result<Options> {
    let mut options = Options {
        root: String::new(),
        dataset: None,
        table: None,
        revision: None,
        from: None,
        to: None,
        limit: 20,
        full: false,
        retention: false,
        declaration: None,
    };
    let mut seen = BTreeSet::new();
    let mut index = 0;
    while index < args.len() {
        let flag = &args[index];
        if !flag.starts_with('-') {
            if options.dataset.is_some() {
                return Err(invalid("inspection accepts one dataset"));
            }
            options.dataset = Some(Name::new(flag).map_err(|_| invalid("invalid dataset"))?);
            index += 1;
            continue;
        }
        if !seen.insert(flag.as_str()) {
            return Err(invalid("inspection flags must be unique"));
        }
        match flag.as_str() {
            "--full" if command == "verify" => {
                options.full = true;
                index += 1;
                continue;
            }
            "--retention" if command == "show" => {
                options.retention = true;
                index += 1;
                continue;
            }
            _ => {}
        }
        let value = args
            .get(index + 1)
            .filter(|value| !value.starts_with("--"))
            .ok_or_else(|| invalid("inspection flag requires a value"))?;
        match flag.as_str() {
            "--grv" => options.root = value.clone(),
            "--table" if command == "ls" => {
                options.table = Some(Name::new(value).map_err(|_| invalid("invalid table"))?)
            }
            "--revision" if matches!(command, "ls" | "show" | "verify") => {
                options.revision = Some(counter(value)?)
            }
            "--from" if command == "diff" => options.from = Some(counter(value)?),
            "--to" if command == "diff" => {
                options.to = Some(if value == "latest" {
                    None
                } else {
                    Some(counter(value)?)
                })
            }
            "--limit" if command == "log" => {
                let parsed = value
                    .parse::<U64>()
                    .map_err(|_| invalid("limit must be a canonical positive integer"))?
                    .get();
                if parsed == 0 {
                    return Err(invalid("limit must be a canonical positive integer"));
                }
                options.limit = parsed as usize;
            }
            "--decl" if command == "status" => options.declaration = Some(PathBuf::from(value)),
            _ => return Err(invalid("unsupported inspection flag")),
        }
        index += 2;
    }
    if options.root.is_empty() {
        return Err(invalid("inspection requires --grv <root>"));
    }
    if options.dataset.is_none()
        && (command != "ls" || options.table.is_some() || options.revision.is_some())
    {
        return Err(invalid("inspection requires a dataset"));
    }
    if command == "diff" && (options.from.is_none() || options.to.is_none()) {
        return Err(invalid(
            "diff requires --from <revision> --to <revision|latest>",
        ));
    }
    Ok(options)
}
pub(super) fn run(command: &str, args: &[String]) -> std::result::Result<Success, Failure> {
    let options = options(command, args)?;
    let root = Root::parse(&options.root)?;
    let canonical = root.canonical.clone();
    execute(command, options, root).map_err(|mut failure| {
        failure.root = Some(canonical);
        failure
    })
}
fn execute(command: &str, options: Options, root: Root) -> std::result::Result<Success, Failure> {
    let store = Store::open(root.open(false)?)?;
    let staging = tempfile::tempdir()
        .map_err(|error| public_error(ErrorCode::BackendFailure, error.to_string()))?;
    let clock = SystemClock::default();
    let inspector = Inspector::new(&store, &clock, staging.path());
    let result = match command {
        "ls" => inspector.ls(
            options.dataset.as_ref(),
            options.revision,
            options.table.as_ref(),
        )?,
        "show" => inspector.show(
            options.dataset.as_ref().unwrap(),
            options.revision,
            options.retention,
        )?,
        "status" => {
            let mut result = inspector.status(options.dataset.as_ref().unwrap())?;
            if let Some(declaration) = options.declaration.as_ref() {
                match inspect_adapter(
                    declaration,
                    options.dataset.as_ref().unwrap(),
                    &root.canonical,
                    || inspector.status(options.dataset.as_ref().unwrap()),
                ) {
                    Ok((state, refreshed)) => {
                        result = refreshed;
                        result["adapter_state"] = state;
                    }
                    Err(error) => {
                        return Err(Failure {
                            error,
                            result: Some(result),
                            root: None,
                        });
                    }
                }
            }
            result
        }
        "log" => inspector.log(options.dataset.as_ref().unwrap(), options.limit)?,
        "diff" => inspector.diff(
            options.dataset.as_ref().unwrap(),
            options.from.unwrap(),
            options.to.unwrap(),
        )?,
        "verify" => {
            let report = inspector.verify(
                options.dataset.as_ref().unwrap(),
                options.revision,
                options.full,
            )?;
            if let Some(error) = report.error {
                return Err(Failure {
                    error,
                    result: Some(report.result),
                    root: None,
                });
            }
            report.result
        }
        _ => return Err(invalid("unknown inspection command").into()),
    };
    Ok(Success {
        result,
        root: Some(root.canonical),
    })
}

fn inspect_adapter(
    path: &Path,
    dataset: &Name,
    canonical: &str,
    observe: impl FnOnce() -> Result<serde_json::Value>,
) -> Result<(serde_json::Value, serde_json::Value)> {
    use grv_adapter_api::{InspectConnectionRequest, Mode};
    use grv_adapter_host::{
        discovery,
        process::{Deadlines, Session},
    };
    use grv_core::declaration;
    use serde_json::json;
    let path = std::fs::canonicalize(path).map_err(|_| invalid("declaration path unavailable"))?;
    let mut authored = declaration::load(&path).map_err(|_| {
        public_error(
            ErrorCode::InvalidDeclaration,
            "status declaration validation failed",
        )
    })?;
    if authored["dataset"] != dataset.as_str() {
        return Err(public_error(
            ErrorCode::InvalidDeclaration,
            "status declaration names another dataset",
        ));
    }
    let name = Name::new(authored["adapter"].as_str().unwrap())
        .map_err(|_| invalid("invalid adapter name"))?;
    let installations =
        discovery::discover(&discovery::search_roots(None).map_err(|e| e.public())?)
            .map_err(|e| e.public())?;
    let installed = installations
        .iter()
        .find(|i| i.manifest.name == name)
        .ok_or_else(|| {
            public_error(
                ErrorCode::NotFound,
                "inspection adapter installation unavailable",
            )
        })?;
    let mut process = Session::spawn_at(installed, Deadlines::default(), path.parent())
        .map_err(|e| e.public())?;
    if !process.capabilities.inspect_connection {
        return Err(public_error(
            ErrorCode::UnsupportedCapability,
            "adapter does not advertise read-only connection inspection",
        ));
    }
    let mode = declaration::mode(&authored)
        .map_err(|_| public_error(ErrorCode::InvalidDeclaration, "invalid declaration mode"))?;
    declaration::apply_point_defaults(&mut authored, &process.registry).map_err(|_| {
        public_error(
            ErrorCode::InvalidDeclaration,
            "inspection declaration defaults failed validation",
        )
    })?;
    let effective = process
        .validate_binding(authored.clone(), mode)
        .map_err(|e| e.public())?;
    declaration::validate_effective(&authored, &effective, &process.registry).map_err(|_| {
        public_error(
            ErrorCode::ProtocolFailure,
            "adapter changed explicit declaration values",
        )
    })?;
    let locator = process
        .locate_connection(effective["connection"].clone(), Mode::Inspect, None)
        .map_err(|e| e.public())?;
    let bound = process
        .bind_connection(
            locator.clone(),
            Some(canonical.into()),
            locator.identity,
            None,
            Mode::Inspect,
        )
        .map_err(|e| e.public())?;
    let mut details = process
        .inspect_connection(
            bound.handle,
            InspectConnectionRequest {
                root: Some(canonical.into()),
                declaration: Some(effective),
            },
        )
        .map_err(|e| e.public())?;
    process.close().map_err(|e| e.public())?;
    // Refresh backend observations after the fixed engine snapshot. A newer
    // concurrently published/pulled revision must not be misreported as corrupt
    // merely because the first backend read preceded that engine transaction.
    let observed = observe()?;
    // DuckDB deliberately omits GRV observations from engine summaries. The
    // parent projects only this dataset's already-observed backend facts.
    if name.as_str() == "duckdb" {
        if let Some(materializations) = details
            .get_mut("materializations")
            .and_then(serde_json::Value::as_array_mut)
        {
            for materialization in materializations {
                if materialization["dataset"] != dataset.as_str() {
                    return Err(public_error(
                        ErrorCode::ProtocolFailure,
                        "inspection escaped declared dataset scope",
                    ));
                }
                let committed = materialization["committed_revision"]
                    .as_str()
                    .ok_or_else(|| invalid("invalid observed materialization revision"))?
                    .parse::<U64>()
                    .map_err(|_| invalid("invalid observed materialization revision"))?;
                let latest = observed["current_revision"]
                    .as_str()
                    .unwrap()
                    .parse::<U64>()
                    .map_err(|_| invalid("invalid observed GRV revision"))?;
                if committed.get() > latest.get() {
                    return Err(public_error(
                        ErrorCode::IntegrityFailure,
                        "materialization revision exceeds observed GRV state",
                    ));
                }
                materialization["state"] = json!(if materialization["revision_mode"] == "fixed" {
                    "fixed"
                } else if committed == latest {
                    "current"
                } else {
                    "behind"
                });
            }
        }
        if let Some(sessions) = details
            .get_mut("sessions")
            .and_then(serde_json::Value::as_array_mut)
        {
            for session in sessions {
                if session["dataset"] != dataset.as_str() {
                    return Err(public_error(
                        ErrorCode::ProtocolFailure,
                        "inspection escaped declared dataset scope",
                    ));
                }
                session["run"] = observed["runs"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|run| run["run_id"] == session["run_id"])
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
            }
        }
        process
            .registry
            .validate_point(
                Mode::Inspect,
                "inspection_result",
                &details,
                ErrorCode::ProtocolFailure,
            )
            .map_err(|e| e.public())?;
    }
    Ok((json!({"adapter":name,"details":details}), observed))
}
