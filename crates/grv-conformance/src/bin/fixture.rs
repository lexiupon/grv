//! Test-only process adapter. It is never installed by the GRV product.
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, new_null_array};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use grv_adapter_api::*;
use grv_adapter_sdk::{
    Adapter, BoundConnection, Extraction, ExtractionEvent, PreparedCommand, Registration, Result,
    StopToken,
};
use grv_types::PublicError;
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    io::Write,
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    sync::Arc,
};
fn failure(code: ErrorCode, msg: &str) -> PublicError {
    PublicError {
        code,
        message: msg.into(),
        retryable: false,
        object: None,
    }
}
fn point(point: &str, schema: &str, default: bool) -> PointDescriptor {
    PointDescriptor {
        point: point.into(),
        mode: Mode::Extract,
        schema_pointer: format!("/$defs/{schema}"),
        default_value: if default { json!({}) } else { Value::Null },
        has_default: default,
    }
}
struct Fixture {
    hook_directory: Option<std::path::PathBuf>,
}
fn fixture_record(path: &std::path::Path, value: &impl serde::Serialize) -> Result<()> {
    let bytes = grv_types::canonical_json(value)
        .map_err(|_| failure(ErrorCode::AdapterFailure, "fixture record encoding failed"))?;
    if path.exists() {
        if std::fs::read(path).ok().as_deref() != Some(bytes.as_slice()) {
            return Err(failure(
                ErrorCode::RequestMismatch,
                "fixture fixed hook state differs",
            ));
        }
        return Ok(());
    }
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|_| failure(ErrorCode::AdapterFailure, "fixture record creation failed"))?;
    file.write_all(&bytes)
        .and_then(|_| file.sync_all())
        .map_err(|_| {
            failure(
                ErrorCode::AdapterFailure,
                "fixture record durability failed",
            )
        })?;
    std::fs::File::open(path.parent().unwrap())
        .and_then(|file| file.sync_all())
        .map_err(|_| {
            failure(
                ErrorCode::AdapterFailure,
                "fixture directory durability failed",
            )
        })
}
fn fixture_call(directory: &std::path::Path, name: &str) -> Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .mode(0o600)
        .open(directory.join(name))
        .map_err(|_| failure(ErrorCode::AdapterFailure, "fixture marker creation failed"))?;
    file.write_all(b"called\n")
        .and_then(|_| file.sync_all())
        .map_err(|_| {
            failure(
                ErrorCode::AdapterFailure,
                "fixture marker durability failed",
            )
        })
}
impl Adapter for Fixture {
    fn registration(&self) -> Registration {
        let closed = json!({"type":"object","properties":{},"additionalProperties":false});
        Registration {
            name: Name::new("fixture").unwrap(),
            package_version: "0.1.0".into(),
            interface_versions: vec![Req::new(1).unwrap()],
            binding_schema_version: Req::new(1).unwrap(),
            capabilities: Capabilities {
                push: true,
                source_consistency: WireConsistency::Snapshot,
                after_publish: self.hook_directory.is_some(),
                ..Capabilities::default()
            },
            registry: Registry {
                schema_bundle: json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$defs":{
                    "empty":closed,"selector":{"type":"string","minLength":1},
                    "source_identity":{"type":"object","properties":{"snapshot_id":{"type":"string","minLength":1}},"required":["snapshot_id"],"additionalProperties":false},
                    "job":{"type":"object","properties":{"kind":{"const":"fixture"}},"required":["kind"],"additionalProperties":false},
                    "push_result":{"type":"object","properties":{"rows":{"type":"string","pattern":"^(0|[1-9][0-9]*)$"}},"required":["rows"],"additionalProperties":false},
                    "echo_args":{"type":"object","properties":{"message":{"type":"string"}},"required":["message"],"additionalProperties":false},
                    "echo_result":{"type":"object","properties":{"message":{"type":"string"}},"required":["message"],"additionalProperties":false}
                }}),
                points: vec![
                    point("connection", "empty", false),
                    point("connection_details", "empty", false),
                    point("options", "empty", true),
                    point("table_source", "empty", false),
                    point("column_source", "selector", false),
                    point("source_identity", "source_identity", false),
                    point("source_job", "job", false),
                    point("push_result", "push_result", false),
                ],
            },
            commands: vec![CommandDescriptor {
                name: Name::new("echo").unwrap(),
                requires_connection: false,
                requires_authentication: false,
                args_schema_pointer: "/$defs/echo_args".into(),
                result_schema_pointer: "/$defs/echo_result".into(),
            }],
        }
    }
    fn prepare_command(&self, name: &Name, argv: &[String]) -> Result<PreparedCommand> {
        if name.as_str() != "echo" || argv.len() != 2 || argv[0] != "--message" {
            return Err(failure(
                ErrorCode::InvalidArgument,
                "echo requires --message <value>",
            ));
        }
        Ok(PreparedCommand {
            args: json!({"message":argv[1]}),
            connection: None,
        })
    }
    fn execute_command(&mut self, call: &CommandCall, stop: &StopToken) -> Result<Value> {
        stop.check()?;
        Ok(call.args.clone())
    }
    fn validate_binding(
        &self,
        declaration: Value,
        _mode: Mode,
        _schema_version: Req,
    ) -> Result<Value> {
        Ok(declaration)
    }
    fn locate_connection(
        &self,
        connection: Value,
        _mode: Mode,
        _run_id: Option<RunId>,
    ) -> Result<ConnectionLocator> {
        Ok(ConnectionLocator {
            canonical_connection: connection,
            identity: if self
                .hook_directory
                .as_ref()
                .is_some_and(|directory| directory.join("unknown_offline_identity").exists())
            {
                None
            } else {
                Some("fixture-source".into())
            },
            engine_path: None,
            session_lock_path: None,
        })
    }
    fn bind_connection(
        &mut self,
        locator: ConnectionLocator,
        _root: Option<String>,
        expected_identity: Option<String>,
        expected_workspace_id: Option<Uuid>,
        _mode: Mode,
    ) -> Result<BoundConnection> {
        if expected_identity
            .as_ref()
            .is_some_and(|id| locator.identity.as_ref().is_some_and(|actual| actual != id))
        {
            return Err(failure(
                ErrorCode::RequestMismatch,
                "fixture identity changed",
            ));
        }
        Ok(BoundConnection {
            handle: Handle::new("fixture-handle").unwrap(),
            identity: locator.identity,
            workspace_id: expected_workspace_id,
            binding: BindingState::NotApplicable,
            details: json!({}),
        })
    }
    fn authenticate(
        &mut self,
        handle: Handle,
        expected_identity: Option<String>,
    ) -> Result<String> {
        if handle.as_str() != "fixture-handle"
            || expected_identity
                .as_ref()
                .is_some_and(|id| id != "fixture-source")
        {
            return Err(failure(
                ErrorCode::RequestMismatch,
                "fixture authentication identity mismatch",
            ));
        }
        if let Some(directory) = &self.hook_directory {
            fixture_call(directory, "authentications")?;
        }
        Ok("fixture-source".into())
    }
    fn after_publish(
        &mut self,
        handle: Handle,
        request: AfterPublishRequest,
        stop: &StopToken,
    ) -> Result<()> {
        stop.check()?;
        if handle.as_str() != "fixture-handle" {
            return Err(failure(
                ErrorCode::ProtocolFailure,
                "unknown fixture hook handle",
            ));
        }
        let directory = self
            .hook_directory
            .as_ref()
            .ok_or_else(|| failure(ErrorCode::UnsupportedCapability, "fixture hook disabled"))?;
        let fixed = json!({"attempt_id":request.attempt_id,"declaration_sha256":request.declaration_sha256});
        let capture = directory.join(format!("{}.capture.json", request.attempt_id));
        if std::fs::read(&capture).ok().as_deref()
            != Some(grv_types::canonical_json(&fixed).unwrap().as_slice())
        {
            return Err(failure(
                ErrorCode::RequestMismatch,
                "fixture private attempt state differs or is missing",
            ));
        }
        fixture_record(
            &directory.join(format!("{}.pending.json", request.attempt_id)),
            &request,
        )?;
        fixture_call(directory, "hooks")?;
        if directory.join("require_auth").exists() {
            fixture_call(directory, "hook_authentications")?;
        }
        while directory.join("wait").exists() {
            stop.check()?;
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        if directory.join("fail").exists() {
            return Err(failure(
                ErrorCode::AdapterFailure,
                "fixture after-publish hook failed",
            ));
        }
        if directory.join("crash_once").exists() {
            std::fs::remove_file(directory.join("crash_once"))
                .map_err(|_| failure(ErrorCode::AdapterFailure, "fixture crash marker failed"))?;
            std::process::exit(71);
        }
        if matches!(
            request.outcome.kind,
            OutcomeKind::Published | OutcomeKind::NoOp
        ) {
            fixture_record(
                &directory.join(format!("{}.cursor.json", request.attempt_id)),
                &request.outcome,
            )?;
        }
        fixture_record(
            &directory.join(format!("{}.complete.json", request.attempt_id)),
            &request,
        )?;
        if directory.join("crash_after_completion_once").exists() {
            std::fs::remove_file(directory.join("crash_after_completion_once"))
                .map_err(|_| failure(ErrorCode::AdapterFailure, "fixture crash marker failed"))?;
            std::process::exit(72);
        }
        Ok(())
    }
    fn extract(
        &mut self,
        handle: Handle,
        request: ExtractRequest,
        stop: &StopToken,
    ) -> Result<Box<dyn Extraction>> {
        stop.check()?;
        if let Some(directory) = &self.hook_directory {
            fixture_call(directory, "extractions")?;
            fixture_record(
                &directory.join(format!("{}.capture.json", request.attempt_id)),
                &json!({"attempt_id":request.attempt_id,"declaration_sha256":request.declaration_sha256}),
            )?;
        }
        if handle.as_str() != "fixture-handle" {
            return Err(failure(
                ErrorCode::ProtocolFailure,
                "unknown fixture handle",
            ));
        }
        let timestamp = Timestamp::new("2026-10-06T00:00:00Z").unwrap();
        let checkpoint=Checkpoint{attempt_id:request.attempt_id.clone(),adapter_identity:request.adapter_identity.clone(),connection_identity:request.connection_identity.clone(),tables:request.tables.iter().map(|t|CheckpointTable{table:t.name.clone(),snapshot_id:format!("{}:{}",request.attempt_id,t.name),reopenable:false,source_identity:json!({"snapshot_id":format!("{}:{}",request.attempt_id,t.name)}),capture_start:timestamp.clone()}).collect(),job:json!({"kind":"fixture"})};
        // A durable nonreopenable acquisition marker prevents same-attempt replay.
        let dir = std::env::temp_dir().join(format!("grv-conformance-acquisitions-{}", unsafe {
            libc::geteuid()
        }));
        match std::fs::create_dir(&dir) {
            Ok(()) => std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
                .map_err(|_| {
                    failure(
                        ErrorCode::AdapterFailure,
                        "fixture journal protection failed",
                    )
                })?,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(_) => {
                return Err(failure(
                    ErrorCode::AdapterFailure,
                    "fixture journal creation failed",
                ));
            }
        }
        let metadata = std::fs::symlink_metadata(&dir)
            .map_err(|_| failure(ErrorCode::AdapterFailure, "fixture journal unavailable"))?;
        if !metadata.is_dir()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o077 != 0
        {
            return Err(failure(
                ErrorCode::AdapterFailure,
                "fixture journal is unprotected",
            ));
        }
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(dir.join(request.attempt_id.as_str()))
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::AlreadyExists {
                    failure(
                        ErrorCode::ExtractionIncomplete,
                        "fixture acquisition cannot be reopened",
                    )
                } else {
                    failure(ErrorCode::AdapterFailure, "fixture journal write failed")
                }
            })?;
        let bytes = grv_types::canonical_json(&checkpoint).map_err(|_| {
            failure(
                ErrorCode::AdapterFailure,
                "fixture checkpoint encoding failed",
            )
        })?;
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(|_| {
                failure(
                    ErrorCode::AdapterFailure,
                    "fixture checkpoint durability failed",
                )
            })?;
        std::fs::File::open(&dir)
            .and_then(|f| f.sync_all())
            .map_err(|_| {
                failure(
                    ErrorCode::AdapterFailure,
                    "fixture journal directory sync failed",
                )
            })?;
        Ok(Box::new(Events {
            checkpoint: Some(checkpoint),
            tables: request.tables.into(),
            pending: None,
            attempt: request.attempt_id,
            timestamp,
            total: 0,
            complete: false,
        }))
    }
    fn stop_and_wait(&mut self) -> Result<()> {
        Ok(())
    }
}
struct Events {
    checkpoint: Option<Checkpoint>,
    tables: VecDeque<ExtractTable>,
    pending: Option<ExtractionEvent>,
    attempt: Uuid,
    timestamp: Timestamp,
    total: u64,
    complete: bool,
}
impl Extraction for Events {
    fn next(&mut self, stop: &StopToken) -> Result<Option<ExtractionEvent>> {
        stop.check()?;
        if let Some(payload) = self.checkpoint.take() {
            return Ok(Some(ExtractionEvent::Checkpoint {
                checkpoint_id: Uuid::v4(),
                payload,
            }));
        }
        if let Some(event) = self.pending.take() {
            return Ok(Some(event));
        }
        if let Some(table) = self.tables.pop_front() {
            let rows = if table.name.as_str().contains("empty") {
                0
            } else {
                3
            };
            self.total += rows;
            let completion = ExtractionEvent::TableComplete {
                table: table.name.clone(),
                row_count: U64::new(rows).unwrap(),
                source_identity: json!({"snapshot_id":format!("{}:{}",self.attempt,table.name)}),
                capture: CaptureWindow {
                    start: self.timestamp.clone(),
                    end: self.timestamp.clone(),
                },
            };
            if rows == 0 {
                return Ok(Some(completion));
            }
            let mut fields = vec![];
            let mut arrays: Vec<ArrayRef> = vec![];
            for column in table.contract.columns {
                let datatype = datatype(&column.logical_type)?;
                fields.push(Field::new(&column.name, datatype.clone(), true));
                arrays.push(match datatype {
                    DataType::Int64 => Arc::new(Int64Array::from(vec![1, 2, 3])),
                    DataType::Utf8 => Arc::new(StringArray::from(vec!["a", "b", "c"])),
                    other => new_null_array(&other, 3),
                });
            }
            let batch =
                RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|_| {
                    failure(
                        ErrorCode::IntegrityFailure,
                        "fixture batch contract invalid",
                    )
                })?;
            let mut payload = vec![];
            {
                let mut writer =
                    StreamWriter::try_new(&mut payload, &batch.schema()).map_err(|_| {
                        failure(ErrorCode::AdapterFailure, "fixture Arrow writer failed")
                    })?;
                writer.write(&batch).map_err(|_| {
                    failure(ErrorCode::AdapterFailure, "fixture Arrow encoding failed")
                })?;
            }
            self.pending = Some(completion);
            return Ok(Some(ExtractionEvent::Batch {
                table: table.name,
                payload,
                rows: U64::new(rows).unwrap(),
            }));
        }
        if self.complete {
            return Ok(None);
        }
        self.complete = true;
        Ok(Some(ExtractionEvent::SourceComplete(SourceCompletion {
            job: json!({"kind":"fixture"}),
            capture_window: CaptureWindow {
                start: self.timestamp.clone(),
                end: self.timestamp.clone(),
            },
            adapter_result: json!({"rows":self.total.to_string()}),
        })))
    }
}
fn datatype(value: &Value) -> Result<DataType> {
    Ok(match value.as_str() {
        Some("int64") => DataType::Int64,
        Some("boolean") => DataType::Boolean,
        Some("float64") => DataType::Float64,
        Some("string") => DataType::Utf8,
        Some("binary") => DataType::Binary,
        Some("date") => DataType::Date32,
        _ => {
            if let Some(d) = value.get("decimal") {
                DataType::Decimal128(
                    d["precision"].as_u64().unwrap() as u8,
                    d["scale"].as_u64().unwrap() as i8,
                )
            } else if let Some(t) = value.get("timestamp") {
                DataType::Timestamp(
                    match t["unit"].as_str() {
                        Some("ms") => TimeUnit::Millisecond,
                        Some("us") => TimeUnit::Microsecond,
                        _ => TimeUnit::Nanosecond,
                    },
                    if t["utc"] == true {
                        Some("UTC".into())
                    } else {
                        None
                    },
                )
            } else {
                return Err(failure(
                    ErrorCode::InvalidDeclaration,
                    "fixture logical type unsupported",
                ));
            }
        }
    })
}
fn main() {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let hook_directory = match args.as_slice() {
        [] => None,
        [flag, directory] if flag == "--after-publish-dir" => Some(directory.into()),
        _ => {
            eprintln!("invalid fixture arguments");
            std::process::exit(2);
        }
    };
    if let Err(e) = grv_adapter_sdk::run_fd3(Fixture { hook_directory }) {
        eprintln!("fixture stopped: {e}");
        std::process::exit(1)
    }
}
