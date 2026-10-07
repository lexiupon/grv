#![cfg(feature = "native")]
use grv_adapter_api::*;
use grv_adapter_duckdb::build::{BuildError, BuildStore};
use serde_json::json;
use std::{fs, os::unix::fs::PermissionsExt, path::Path};
fn name(s: &str) -> Name {
    Name::new(s).unwrap()
}
fn contract() -> TableContract {
    TableContract {
        columns: vec![Column {
            name: "id".into(),
            logical_type: json!("int64"),
        }],
        partition_keys: vec![],
        extensions: json!({}),
        column_ext: json!({}),
    }
}
fn request(path: &Path) -> DiscoverBuildRequest {
    DiscoverBuildRequest {
        identity: BuildIdentity {
            attempt_id: Uuid::v4(),
            root: "file:///private/grv-build-test".into(),
            dataset: name("product"),
            run_id: RunId::new("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            workspace_id: Uuid::v4(),
            declaration_sha256: grv_types::sha256(b"build"),
            adapter_identity: AdapterIdentity {
                name: name("duckdb"),
                package_version: env!("CARGO_PKG_VERSION").into(),
                interface_version: Req::new(1).unwrap(),
                binding_schema_version: Req::new(1).unwrap(),
            },
            connection_identity: format!("duckdb:{}", path.display()),
        },
        options: json!({}),
        execution: BuildExecution::Managed,
        inputs: vec![],
        outputs: vec![
            BuildOutput {
                table: name("rows"),
                source: json!({"sql":"SELECT 7::BIGINT AS id UNION ALL SELECT 3::BIGINT AS id"}),
                columns: json!([{"name":"id","type":"int64","source":"id"}]),
                contract: contract(),
            },
            BuildOutput {
                table: name("empty"),
                source: json!({"sql":"SELECT 0::BIGINT AS id WHERE false"}),
                columns: json!([{"name":"id","type":"int64","source":"id"}]),
                contract: contract(),
            },
        ],
        selected_outputs: vec![name("rows"), name("empty")],
        self_input: false,
    }
}
fn preparation(discovery: BuildDiscovery) -> PrepareBuildRequest {
    PrepareBuildRequest {
        discovery,
        base_revision: U64::new(0).unwrap(),
        self_input: false,
        base_contracts: vec![],
        base_files: vec![],
        input_files: vec![],
        holds_confirmed: true,
    }
}
fn execute(session: &BuildSession) -> ExecuteBuildRequest {
    ExecuteBuildRequest {
        session: session.clone(),
        queries: session
            .selected_outputs
            .iter()
            .map(|table| {
                let output = session.outputs.iter().find(|o| &o.table == table).unwrap();
                BuildQuery {
                    table: table.clone(),
                    sql: output.source["sql"].as_str().unwrap().into(),
                }
            })
            .collect(),
    }
}
fn directory() -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix(".native-build-")
        .tempdir_in(env!("CARGO_MANIFEST_DIR"))
        .unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    dir
}
#[test]
fn managed_zero_rows_completion_acceptance_reopen_export_and_cleanup_preserve_evidence() {
    let dir = directory();
    let path = dir.path().join("build.duckdb");
    let request = request(&path);
    let resources = Resources::default();
    let mut store = BuildStore::open(
        &path,
        request.identity.root.clone(),
        Some(request.identity.workspace_id.clone()),
        &resources,
    )
    .unwrap();
    assert!(store.workspace_id().is_none());
    let discovery = store.discover(request.clone()).unwrap();
    let session = store.prepare(preparation(discovery)).unwrap();
    assert_eq!(
        store.workspace_id(),
        Some(request.identity.workspace_id.clone())
    );
    let result = store.execute(execute(&session)).unwrap();
    assert_eq!(result.status, BuildExecutionStatus::Succeeded);
    assert_eq!(
        result
            .row_counts
            .iter()
            .map(|c| c.rows.get())
            .collect::<Vec<_>>(),
        [2, 0]
    );
    let completion = result.completion.unwrap();
    let record = store.open_session(&request.identity).unwrap();
    assert_eq!(record.candidate, Some(completion.clone()));
    assert!(record.completion.is_none());
    assert!(matches!(
        store.execute(execute(&session)),
        Err(BuildError::Incomplete(_))
    ));
    assert!(
        store
            .start_export(&request.identity, &completion.digest().unwrap())
            .is_err()
    );
    let digest = store.accept(&request.identity, completion.clone()).unwrap();
    assert_eq!(
        store.accept(&request.identity, completion.clone()).unwrap(),
        digest
    );
    let mut changed = completion.clone();
    changed.completed_at = Timestamp::new("2026-10-06T00:00:00Z").unwrap();
    assert!(matches!(
        store.accept(&request.identity, changed),
        Err(BuildError::RequestMismatch)
    ));
    drop(store);
    let mut store = BuildStore::open(
        &path,
        request.identity.root.clone(),
        Some(request.identity.workspace_id.clone()),
        &resources,
    )
    .unwrap();
    assert_eq!(
        store
            .open_session(&request.identity)
            .unwrap()
            .completion_sha256,
        Some(digest.clone())
    );
    let counts = store.start_export(&request.identity, &digest).unwrap();
    assert_eq!(
        counts.iter().map(|c| c.rows.get()).collect::<Vec<_>>(),
        [2, 0]
    );
    let mut values = vec![];
    while let Some((bytes, rows)) = store.fetch_export(0, &resources).unwrap() {
        let batch = grv_adapter_wire::ipc::decode(&bytes, rows.get()).unwrap();
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        values.extend(column.values().iter().copied());
    }
    values.sort();
    assert_eq!(values, [3, 7]);
    assert!(store.fetch_export(1, &resources).unwrap().is_none());
    store.finish_export().unwrap();
    let outcome = BuildOutcome {
        kind: OutcomeKind::Published,
        revision: Some(U64::new(1).unwrap()),
        operation_id: Some(grv_types::RunId::new("01M3KQA080R6Y8C2D9F0G00099").unwrap()),
    };
    store
        .record_outcome(&request.identity, outcome.clone())
        .unwrap();
    let mut altered = outcome.clone();
    altered.operation_id = Some(request.identity.run_id.clone());
    assert!(store.record_outcome(&request.identity, altered).is_err());
    assert!(store.start_export(&request.identity, &digest).is_err());
    store.cleanup(&request.identity).unwrap();
    drop(store);
    let mut store = BuildStore::open(
        &path,
        request.identity.root.clone(),
        Some(request.identity.workspace_id.clone()),
        &resources,
    )
    .unwrap();
    let record = store.open_session(&request.identity).unwrap();
    assert_eq!(record.outcome, Some(outcome));
    assert_eq!(record.completion, Some(completion));
    assert_eq!(record.completion_sha256, Some(digest));
}
#[test]
fn second_query_failure_is_incomplete_without_candidate_or_reexecution() {
    let dir = directory();
    let path = dir.path().join("fail.duckdb");
    let mut request = request(&path);
    request.outputs[1].source = json!({"sql":"SELECT error('failed query')::BIGINT AS id"});
    let mut store = BuildStore::open(
        &path,
        request.identity.root.clone(),
        Some(request.identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    let discovery = store.discover(request.clone()).unwrap();
    let session = store.prepare(preparation(discovery)).unwrap();
    let result = store.execute(execute(&session)).unwrap();
    assert_eq!(result.status, BuildExecutionStatus::Failed);
    assert!(result.completion.is_none());
    assert!(result.row_counts.is_empty());
    drop(store);
    let mut store = BuildStore::open(
        &path,
        request.identity.root.clone(),
        Some(request.identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    let record = store.open_session(&request.identity).unwrap();
    assert_eq!(record.state, BuildState::Executing);
    assert!(record.candidate.is_none());
    assert!(record.completion.is_none());
    assert!(matches!(
        store.execute(execute(&session)),
        Err(BuildError::Incomplete(_))
    ));
    store.abort(&request.identity).unwrap();
}
#[test]
fn destroyed_discovery_transaction_never_selects_new_inputs_under_same_attempt() {
    let dir = directory();
    let path = dir.path().join("discovery.duckdb");
    let request = request(&path);
    let mut store = BuildStore::open(
        &path,
        request.identity.root.clone(),
        Some(request.identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    store.discover(request.clone()).unwrap();
    drop(store);
    let mut store = BuildStore::open(
        &path,
        request.identity.root.clone(),
        Some(request.identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    assert!(store.workspace_id().is_none());
    assert!(matches!(
        store.discover(request.clone()),
        Err(BuildError::Incomplete(_))
    ));
    assert!(store.open_session(&request.identity).is_err());
}

#[test]
fn managed_mappings_repeat_selectors_and_use_extraction_lossless_conversion_before_adoption() {
    let dir = directory();
    let path = dir.path().join("mapped.duckdb");
    let mut request = request(&path);
    request.outputs.truncate(1);
    request.selected_outputs.truncate(1);
    request.outputs[0].source = json!({"sql":"SELECT 7::INTEGER AS physical, -0.0::FLOAT AS value, 1.25::DECIMAL(3,2) AS dec; -- SQL file ends with a comment\n"});
    request.outputs[0].contract.columns = vec![
        Column {
            name: "id".into(),
            logical_type: json!("int64"),
        },
        Column {
            name: "copied".into(),
            logical_type: json!("int64"),
        },
        Column {
            name: "number".into(),
            logical_type: json!("float64"),
        },
        Column {
            name: "amount".into(),
            logical_type: json!({"decimal":{"precision":10,"scale":4}}),
        },
    ];
    request.outputs[0].columns = json!([
        {"name":"id","source":"physical","type":"int64"},
        {"name":"copied","source":"physical","type":"int64"},
        {"name":"number","source":"value","type":"float64"},
        {"name":"amount","source":"dec","type":{"decimal":{"precision":10,"scale":4}}},
    ]);
    let resources = Resources::default();
    let mut store = BuildStore::open(
        &path,
        request.identity.root.clone(),
        Some(request.identity.workspace_id.clone()),
        &resources,
    )
    .unwrap();
    let discovery = store.discover(request.clone()).unwrap();
    assert_eq!(discovery.outputs[0].columns, request.outputs[0].columns);
    let session = store.prepare(preparation(discovery)).unwrap();
    let result = store.execute(execute(&session)).unwrap();
    assert_eq!(result.status, BuildExecutionStatus::Succeeded);
    let digest = store
        .accept(&request.identity, result.completion.unwrap())
        .unwrap();
    assert_eq!(
        store.start_export(&request.identity, &digest).unwrap()[0]
            .rows
            .get(),
        1
    );
    let (bytes, rows) = store.fetch_export(0, &resources).unwrap().unwrap();
    assert_eq!(rows.get(), 1);
    let mut reader =
        arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0),
        7
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0),
        7
    );
    assert_eq!(
        batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap()
            .value(0),
        12500
    );
}

#[test]
fn managed_lossy_schema_and_infinite_temporal_values_never_create_completion() {
    for sql in ["SELECT '7' AS id", "SELECT CAST('infinity' AS DATE) AS id"] {
        let dir = directory();
        let path = dir.path().join("invalid.duckdb");
        let mut request = request(&path);
        request.outputs.truncate(1);
        request.selected_outputs.truncate(1);
        request.outputs[0].source = json!({"sql":sql});
        if sql.contains("DATE") {
            request.outputs[0].contract.columns[0].logical_type = json!("date");
            request.outputs[0].columns[0]["type"] = json!("date");
        }
        let mut store = BuildStore::open(
            &path,
            request.identity.root.clone(),
            Some(request.identity.workspace_id.clone()),
            &Resources::default(),
        )
        .unwrap();
        let discovery = store.discover(request.clone()).unwrap();
        let session = store.prepare(preparation(discovery)).unwrap();
        let result = store.execute(execute(&session)).unwrap();
        assert_eq!(result.status, BuildExecutionStatus::Failed);
        assert!(result.completion.is_none());
        assert!(
            store
                .open_session(&request.identity)
                .unwrap()
                .candidate
                .is_none()
        );
    }
}

fn parquet(dir: &Path, label: &str, rows: &[i64]) -> VerifiedFile {
    let path = dir.join(format!("{label}.parquet"));
    let schema = std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        true,
    )]));
    let batch = arrow_array::RecordBatch::try_new(
        schema.clone(),
        vec![std::sync::Arc::new(arrow_array::Int64Array::from(
            rows.to_vec(),
        ))],
    )
    .unwrap();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(fs::File::create(&path).unwrap(), schema, None)
            .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let data = fs::read(&path).unwrap();
    VerifiedFile {
        table: name("rows"),
        version: U64::new(1).unwrap(),
        schema: FileSchema {
            columns: contract().columns,
        },
        partition: json!({}),
        access: FileAccess::Local,
        location: path.to_str().unwrap().into(),
        size: U64::new(data.len() as u64).unwrap(),
        sha256: grv_types::sha256(&data),
        validator: "verified-test".into(),
    }
}
fn materialize(
    path: &Path,
    build: &DiscoverBuildRequest,
    schema: &str,
    dataset: &str,
    revision: u64,
    file: &VerifiedFile,
) {
    use grv_adapter_duckdb::pull as p;
    use grv_types::{
        DeclarationIdentity, LatestRevision, PullRequestIdentity, RequestedRevision,
        declaration_digest, pull_request_digest,
    };
    let declaration = grv_adapter_duckdb::binding::validate_pull(json!({"kind":"pull","adapter":"duckdb","dataset":dataset,"connection":{"database":path},"target":{"schema":schema},"tables":[{"name":"rows","target":{"table":"tracking"}}]})).unwrap();
    let declaration_sha256 = declaration_digest(&DeclarationIdentity {
        effective_declaration: declaration.clone(),
        adapter_identity: build.identity.adapter_identity.clone(),
        connection_identity: build.identity.connection_identity.clone(),
        canonical_connection: json!({"database":path}),
    })
    .unwrap();
    let requested_revision = RequestedRevision::Latest(LatestRevision::Latest);
    let request_sha256 = pull_request_digest(&PullRequestIdentity {
        root: build.identity.root.clone(),
        workspace_id: build.identity.workspace_id.clone(),
        declaration_sha256: declaration_sha256.clone(),
        requested_revision: requested_revision.clone(),
    })
    .unwrap();
    let request = RequestRecord {
        attempt_id: Uuid::v4(),
        root: build.identity.root.clone(),
        dataset: name(dataset),
        workspace_id: build.identity.workspace_id.clone(),
        adapter_identity: build.identity.adapter_identity.clone(),
        connection_identity: build.identity.connection_identity.clone(),
        validation_input: declaration.clone(),
        effective_declaration: declaration,
        declaration_sha256,
        request_sha256,
        registry: grv_adapter_duckdb::binding::registry(),
        requested_revision,
    };
    let plan = p::PullPlan {
        materialization_mode: p::MaterializationMode::Local,
        identity: p::PullIdentity {
            binding: p::WorkspaceBinding {
                canonical_root: request.root.clone(),
                workspace_id: request.workspace_id.clone(),
            },
            attempt_id: request.attempt_id.clone(),
            request_sha256: request.request_sha256.clone(),
            adapter_identity: request.adapter_identity.clone(),
            dataset: request.dataset.clone(),
            target_schema: p::RelationName::new(schema).unwrap(),
            write_mode: p::WriteMode::Replace,
            scope_mode: p::ScopeMode::Complete,
            transform_mode: p::TransformMode::Identity,
            requested_revision: request.requested_revision.clone(),
        },
        committed_revision: U64::new(revision).unwrap(),
        generation_id: Uuid::v4(),
        request_record: Some(request),
        selected_partitions: None,
        tables: vec![p::PullTable {
            name: name("rows"),
            target_table: p::RelationName::new("tracking").unwrap(),
            source_contract: contract(),
            output_contract: contract(),
            files: vec![p::SourceFile {
                remote: None,
                path: file.location.clone().into(),
                bytes: file.size,
                sha256: file.sha256.clone(),
                contract: contract(),
                partition: json!({}),
            }],
            sql: None,
            not_null: vec![],
        }],
    };
    p::PullStore::open(path)
        .unwrap()
        .apply_identity(&plan)
        .unwrap();
}

#[test]
fn fixed_held_alias_files_and_whole_self_base_survive_new_generation_and_source_removal() {
    let dir = directory();
    let path = dir.path().join("fixed.duckdb");
    let mut request = request(&path);
    let first = parquet(dir.path(), "first", &[5]);
    let second = parquet(dir.path(), "second", &[11]);
    let base = parquet(dir.path(), "base", &[20]);
    materialize(&path, &request, "one", "source_one", 1, &first);
    materialize(&path, &request, "two", "source_two", 1, &second);
    request.inputs = vec![
        BuildInput {
            alias: name("one"),
            relation: json!("one.tracking"),
        },
        BuildInput {
            alias: name("two"),
            relation: json!("two.tracking"),
        },
    ];
    request.self_input = true;
    request.outputs[0].source = json!({"sql":"SELECT id FROM grv_input.one UNION ALL SELECT id FROM grv_input.two UNION ALL SELECT id FROM grv_self.rows UNION ALL SELECT id FROM grv_self.unmapped_empty"});
    let resources = Resources::default();
    let mut store = BuildStore::open(
        &path,
        request.identity.root.clone(),
        Some(request.identity.workspace_id.clone()),
        &resources,
    )
    .unwrap();
    let discovery = store.discover(request.clone()).unwrap();
    assert_eq!(
        discovery
            .inputs
            .iter()
            .map(|i| i.dataset.as_str())
            .collect::<Vec<_>>(),
        vec!["source_one", "source_two"]
    );
    assert_eq!(discovery.inputs[0].table, discovery.inputs[1].table);
    let session = store
        .prepare(PrepareBuildRequest {
            discovery,
            base_revision: U64::new(1).unwrap(),
            self_input: true,
            base_contracts: vec![
                NamedContract {
                    table: name("rows"),
                    contract: contract(),
                },
                NamedContract {
                    table: name("unmapped_empty"),
                    contract: contract(),
                },
            ],
            base_files: vec![base.clone()],
            input_files: vec![
                BuildInputFiles {
                    alias: name("one"),
                    files: vec![first.clone()],
                },
                BuildInputFiles {
                    alias: name("two"),
                    files: vec![second.clone()],
                },
            ],
            holds_confirmed: true,
        })
        .unwrap();
    assert_eq!(session.adapter_details["database"], json!(path));
    assert_eq!(
        session.adapter_details["self_mappings"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["table"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["rows", "unmapped_empty"]
    );
    assert_eq!(
        session.adapter_details["input_mappings"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["alias"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["one", "two"]
    );
    for (index, mapping) in session.adapter_details["input_mappings"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        assert!(
            mapping["engine_table"]
                .as_str()
                .unwrap()
                .ends_with(&format!(".input_{index}"))
        );
    }
    drop(store);
    let newer = parquet(dir.path(), "newer", &[999]);
    materialize(&path, &request, "one", "source_one", 2, &newer);
    for file in [&first, &second, &base, &newer] {
        fs::remove_file(&file.location).unwrap();
    }
    let mut store = BuildStore::open(
        &path,
        request.identity.root.clone(),
        Some(request.identity.workspace_id.clone()),
        &resources,
    )
    .unwrap();
    assert_eq!(
        store
            .open_session(&request.identity)
            .unwrap()
            .session
            .inputs[0]
            .revision
            .get(),
        1
    );
    let result = store.execute(execute(&session)).unwrap();
    assert_eq!(result.status, BuildExecutionStatus::Succeeded);
    let digest = store
        .accept(&request.identity, result.completion.unwrap())
        .unwrap();
    store.start_export(&request.identity, &digest).unwrap();
    let (bytes, _) = store.fetch_export(0, &resources).unwrap().unwrap();
    let batch = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    let mut actual = values.values().to_vec();
    actual.sort();
    assert_eq!(actual, vec![5, 11, 20]);
    assert!(store.fetch_export(1, &resources).unwrap().is_none());
}

// Test-only external driver uses the pinned public C API after the adapter
// releases all invocation connections. This bypass never exists in production.
fn external_driver(path: &Path, sql: &str) {
    use std::ffi::{CString, c_char, c_void};
    unsafe extern "C" {
        fn duckdb_open(path: *const c_char, database: *mut *mut c_void) -> u32;
        fn duckdb_connect(database: *mut c_void, connection: *mut *mut c_void) -> u32;
        fn duckdb_query(connection: *mut c_void, query: *const c_char, result: *mut c_void) -> u32;
        fn duckdb_disconnect(connection: *mut *mut c_void);
        fn duckdb_close(database: *mut *mut c_void);
    }
    let path = CString::new(path.to_str().unwrap()).unwrap();
    let sql = CString::new(sql).unwrap();
    let mut database = std::ptr::null_mut();
    let mut connection = std::ptr::null_mut();
    unsafe {
        assert_eq!(duckdb_open(path.as_ptr(), &mut database), 0);
        assert_eq!(duckdb_connect(database, &mut connection), 0);
        let status = duckdb_query(connection, sql.as_ptr(), std::ptr::null_mut());
        duckdb_disconnect(&mut connection);
        duckdb_close(&mut database);
        assert_eq!(status, 0);
    }
}

#[test]
fn external_supervised_driver_probe() {
    let Ok(mode) = std::env::var("GRV_TEST_DRIVER_MODE") else {
        return;
    };
    if mode == "write" {
        external_driver(
            Path::new(&std::env::var("GRV_TEST_DRIVER_DATABASE").unwrap()),
            &std::env::var("GRV_TEST_DRIVER_SQL").unwrap(),
        );
    } else if mode == "background" || mode == "escaped" {
        use std::os::unix::process::CommandExt;
        // Deliberately adversarial: the supervisor must stop this orphan before
        // releasing inherited ownership and must refuse a success attestation.
        let mut command = std::process::Command::new("/bin/sh");
        command.args(["-c", "trap '' TERM; while :; do sleep 1; done"]);
        if mode == "escaped" {
            unsafe {
                command.pre_exec(|| {
                    if libc::setsid() < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
        }
        #[allow(clippy::zombie_processes)]
        let child = command.spawn().unwrap();
        fs::write(
            std::env::var("GRV_TEST_DRIVER_READY").unwrap(),
            child.id().to_string(),
        )
        .unwrap();
    } else {
        unsafe {
            libc::signal(libc::SIGTERM, libc::SIG_IGN);
        }
        fs::write(std::env::var("GRV_TEST_DRIVER_READY").unwrap(), "ready").unwrap();
        loop {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}
fn supervised_command(
    path: &Path,
    mode: &str,
    ready: &Path,
    session: &BuildSession,
) -> std::process::Command {
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "external_supervised_driver_probe", "--nocapture"])
        .env("GRV_TEST_DRIVER_MODE", mode)
        .env("GRV_TEST_DRIVER_DATABASE", path)
        .env("GRV_TEST_DRIVER_READY", ready)
        .env(
            "GRV_TEST_DRIVER_SQL",
            format!(
                "INSERT INTO {}(id) VALUES(42)",
                session.outputs[0].engine_table
            ),
        )
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    command
}
fn external_session(path: &Path) -> (BuildStore, BuildSession) {
    let mut request = request(path);
    request.execution = BuildExecution::External;
    for output in &mut request.outputs {
        output.source = json!({"table":output.table.as_str()});
    }
    let mut store = BuildStore::open(
        path,
        request.identity.root.clone(),
        Some(request.identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    let discovery = store.discover(request).unwrap();
    let session = store.prepare(preparation(discovery)).unwrap();
    (store, session)
}
fn completed_outputs(session: &BuildSession) -> Vec<CompletedOutput> {
    session
        .outputs
        .iter()
        .map(|o| CompletedOutput {
            table: o.table.clone(),
            engine_table: o.engine_table.clone(),
        })
        .collect()
}
fn wait_ready(path: &Path) {
    let start = std::time::Instant::now();
    while !path.exists() {
        assert!(start.elapsed() < std::time::Duration::from_secs(5));
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[test]
fn supervised_external_completion_is_durable_immutable_and_exportable_with_zero_output() {
    use grv_adapter_duckdb::{completion, driver::ExternalInvocation};
    let dir = directory();
    let path = dir.path().join("driver.duckdb");
    let (store, session) = external_session(&path);
    let ready = dir.path().join("ready");
    let command = supervised_command(&path, "write", &ready, &session);
    let invocation =
        ExternalInvocation::start(store, session.clone(), Resources::default(), command).unwrap();
    let file = dir.path().join("completion.json");
    let result = invocation
        .complete(
            CompletionKind::Direct,
            completed_outputs(&session),
            &file,
            || Ok(true),
        )
        .unwrap();
    assert_eq!(completion::read(&file, &session).unwrap(), result);
    completion::publish(&file, &session, &result).unwrap();
    let mut changed = result.clone();
    changed.invocation_id = "different".into();
    assert!(completion::publish(&file, &session, &changed).is_err());
    let mut store = BuildStore::open(
        &path,
        session.identity.root.clone(),
        Some(session.identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    assert_eq!(
        store.open_session(&session.identity).unwrap().state,
        BuildState::Executing
    );
    assert!(store.cleanup(&session.identity).is_err());
    assert!(store.accept(&session.identity, changed).is_err());
    let digest = store.accept(&session.identity, result).unwrap();
    assert_eq!(
        store
            .start_export(&session.identity, &digest)
            .unwrap()
            .iter()
            .map(|c| c.rows.get())
            .collect::<Vec<_>>(),
        [1, 0]
    );
}

#[test]
fn protected_completion_reader_rejects_duplicates_unknowns_links_sizes_and_permissions() {
    use grv_adapter_duckdb::{completion, driver::ExternalInvocation};
    use std::os::unix::fs::symlink;
    let dir = directory();
    let path = dir.path().join("driver.duckdb");
    let (store, session) = external_session(&path);
    let command = supervised_command(&path, "write", &dir.path().join("ready"), &session);
    let invocation =
        ExternalInvocation::start(store, session.clone(), Resources::default(), command).unwrap();
    let file = dir.path().join("completion.json");
    let record = invocation
        .complete(
            CompletionKind::Direct,
            completed_outputs(&session),
            &file,
            || Ok(true),
        )
        .unwrap();
    let canonical = fs::read(&file).unwrap();
    let string = String::from_utf8(canonical.clone()).unwrap();
    for bytes in [
        string
            .replacen('{', "{\"result_version\":1,", 1)
            .into_bytes(),
        string.replacen('{', "{\"unknown\":1,", 1).into_bytes(),
        vec![255],
        vec![b' '; completion::MAX_COMPLETION_BYTES as usize + 1],
    ] {
        fs::write(&file, bytes).unwrap();
        assert!(completion::read(&file, &session).is_err());
    }
    fs::write(&file, canonical).unwrap();
    fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(completion::read(&file, &session).is_err());
    fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).unwrap();
    let alias = dir.path().join("alias.json");
    fs::hard_link(&file, &alias).unwrap();
    assert!(completion::read(&file, &session).is_err());
    fs::remove_file(&alias).unwrap();
    symlink(&file, &alias).unwrap();
    assert!(completion::read(&alias, &session).is_err());
    assert_eq!(completion::read(&file, &session).unwrap(), record);
}

#[test]
fn abandoned_driver_children_are_stopped_but_cannot_attest_success() {
    use grv_adapter_duckdb::{driver::ExternalInvocation, lock::WorkspaceLock};
    let dir = directory();
    let path = dir.path().join("driver.duckdb");
    let (store, session) = external_session(&path);
    let ready = dir.path().join("ready");
    let command = supervised_command(&path, "background", &ready, &session);
    let invocation =
        ExternalInvocation::start(store, session.clone(), Resources::default(), command).unwrap();
    let file = dir.path().join("completion.json");
    assert!(
        invocation
            .complete(
                CompletionKind::Direct,
                completed_outputs(&session),
                &file,
                || Ok(true)
            )
            .is_err()
    );
    assert!(!file.exists());
    assert!(WorkspaceLock::acquire(&path).is_ok());
    let mut store = BuildStore::open(
        &path,
        session.identity.root.clone(),
        Some(session.identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    assert_eq!(
        store.open_session(&session.identity).unwrap().state,
        BuildState::Executing
    );
    assert!(store.cleanup(&session.identity).is_err());
}

#[test]
fn ownership_loss_abort_and_drop_stop_ignored_cancellation_before_unlocking() {
    use grv_adapter_duckdb::{driver::ExternalInvocation, lock::WorkspaceLock};
    use grv_adapter_host::session_lock::SessionMutationLock;
    for mode in ["ownership-loss", "abort", "drop"] {
        let dir = directory();
        let path = dir.path().join("driver.duckdb");
        let (store, session) = external_session(&path);
        let ready = dir.path().join("ready");
        let command = supervised_command(&path, "hang", &ready, &session);
        let invocation =
            ExternalInvocation::start(store, session.clone(), Resources::default(), command)
                .unwrap();
        wait_ready(&ready);
        assert_eq!(
            WorkspaceLock::acquire(&path).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        // Backend-only renewal may own the session mutex while driver owns only workspace.
        let mutation = SessionMutationLock::acquire(&path, &session.identity.run_id).unwrap();
        assert!(
            BuildStore::open(
                &path,
                session.identity.root.clone(),
                Some(session.identity.workspace_id.clone()),
                &Resources::default()
            )
            .is_err()
        );
        let file = dir.path().join("completion.json");
        match mode {
            "ownership-loss" => assert!(
                invocation
                    .complete(
                        CompletionKind::Direct,
                        completed_outputs(&session),
                        &file,
                        || Ok(false)
                    )
                    .is_err()
            ),
            "abort" => invocation.abort().unwrap(),
            _ => drop(invocation),
        }
        assert!(!file.exists());
        assert!(WorkspaceLock::acquire(&path).is_ok());
        let mut store = BuildStore::open(
            &path,
            session.identity.root.clone(),
            Some(session.identity.workspace_id.clone()),
            &Resources::default(),
        )
        .unwrap();
        let command = supervised_command(&path, "write", &ready, &session);
        assert!(
            ExternalInvocation::start(store, session.clone(), Resources::default(), command)
                .is_err()
        );
        store = BuildStore::open(
            &path,
            session.identity.root.clone(),
            Some(session.identity.workspace_id.clone()),
            &Resources::default(),
        )
        .unwrap();
        assert!(store.cleanup(&session.identity).is_err());
        store.abort(&session.identity).unwrap();
        store
            .record_outcome(
                &session.identity,
                BuildOutcome {
                    kind: OutcomeKind::Aborted,
                    revision: None,
                    operation_id: None,
                },
            )
            .unwrap();
        store.cleanup(&session.identity).unwrap();
        assert_eq!(
            store.open_session(&session.identity).unwrap().state,
            BuildState::Aborted
        );
        drop(mutation);
    }
}

#[test]
fn failed_external_spawn_preserves_intent_and_never_restarts_the_same_session() {
    use grv_adapter_duckdb::driver::ExternalInvocation;
    let dir = directory();
    let path = dir.path().join("driver.duckdb");
    let (store, session) = external_session(&path);
    let missing = std::process::Command::new(dir.path().join("missing-executable"));
    assert!(
        ExternalInvocation::start(store, session.clone(), Resources::default(), missing).is_err()
    );
    let mut store = BuildStore::open(
        &path,
        session.identity.root.clone(),
        Some(session.identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    assert_eq!(
        store.open_session(&session.identity).unwrap().state,
        BuildState::Executing
    );
    assert!(store.cleanup(&session.identity).is_err());
    let command = supervised_command(&path, "write", &dir.path().join("ready"), &session);
    assert!(ExternalInvocation::start(store, session, Resources::default(), command).is_err());
}

#[test]
fn external_omission_only_completion_has_no_inferred_outputs() {
    use grv_adapter_duckdb::driver::ExternalInvocation;
    let dir = directory();
    let path = dir.path().join("driver.duckdb");
    let mut request = request(&path);
    request.execution = BuildExecution::External;
    request.outputs.clear();
    request.selected_outputs.clear();
    let mut store = BuildStore::open(
        &path,
        request.identity.root.clone(),
        Some(request.identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    let discovery = store.discover(request).unwrap();
    let session = store.prepare(preparation(discovery)).unwrap();
    let invocation = ExternalInvocation::start(
        store,
        session.clone(),
        Resources::default(),
        std::process::Command::new("/usr/bin/true"),
    )
    .unwrap();
    let completion = invocation
        .complete(
            CompletionKind::OmissionOnly,
            vec![],
            &dir.path().join("completion.json"),
            || Ok(true),
        )
        .unwrap();
    assert!(completion.completed_outputs.is_empty());
    let mut store = BuildStore::open(
        &path,
        session.identity.root.clone(),
        Some(session.identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    let digest = store.accept(&session.identity, completion).unwrap();
    assert!(
        store
            .start_export(&session.identity, &digest)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn escaped_writer_keeps_workspace_busy_and_preserves_unresolved_evidence() {
    use grv_adapter_duckdb::{driver::ExternalInvocation, lock::WorkspaceLock};
    let dir = directory();
    let path = dir.path().join("driver.duckdb");
    let (store, session) = external_session(&path);
    let ready = dir.path().join("escaped-pid");
    let command = supervised_command(&path, "escaped", &ready, &session);
    let invocation =
        ExternalInvocation::start(store, session.clone(), Resources::default(), command).unwrap();
    wait_ready(&ready);
    struct EscapedFixture(i32);
    impl Drop for EscapedFixture {
        fn drop(&mut self) {
            unsafe {
                libc::kill(-self.0, libc::SIGKILL);
            }
        }
    }
    let escaped = EscapedFixture(fs::read_to_string(&ready).unwrap().parse().unwrap());
    let file = dir.path().join("completion.json");
    assert!(
        invocation
            .complete(
                CompletionKind::Direct,
                completed_outputs(&session),
                &file,
                || Ok(true),
            )
            .is_err()
    );
    assert!(!file.exists());
    assert_eq!(
        WorkspaceLock::acquire(&path).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    assert!(
        BuildStore::open(
            &path,
            session.identity.root.clone(),
            Some(session.identity.workspace_id.clone()),
            &Resources::default()
        )
        .is_err()
    );
    // Only the fixture owns this deliberately escaped, still-live process.
    // Real CLI abort must return ENGINE_BUSY rather than kill an unknown driver.
    drop(escaped);
    let start = std::time::Instant::now();
    while WorkspaceLock::acquire(&path).is_err() {
        assert!(start.elapsed() < std::time::Duration::from_secs(2));
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let mut store = BuildStore::open(
        &path,
        session.identity.root.clone(),
        Some(session.identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    assert_eq!(
        store.open_session(&session.identity).unwrap().state,
        BuildState::Executing
    );
    assert!(store.cleanup(&session.identity).is_err());
}
#[test]
fn external_fixed_mapping_accepts_only_stopped_exact_completion_and_exports_after_reopen() {
    let dir = directory();
    let path = dir.path().join("external.duckdb");
    let mut request = request(&path);
    request.execution = BuildExecution::External;
    for output in &mut request.outputs {
        output.source = json!({"table":output.table.as_str()});
        output.columns[0]["source"] = json!("physical_id");
    }
    request.outputs[0].contract.columns.push(Column {
        name: "copied".into(),
        logical_type: json!("int64"),
    });
    request.outputs[0]
        .columns
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"copied","source":"physical_id","type":"int64"}));
    let resources = Resources::default();
    let mut store = BuildStore::open(
        &path,
        request.identity.root.clone(),
        Some(request.identity.workspace_id.clone()),
        &resources,
    )
    .unwrap();
    let discovery = store.discover(request.clone()).unwrap();
    let session = store.prepare(preparation(discovery)).unwrap();
    drop(store);
    let output = &session.outputs[0].engine_table;
    external_driver(
        &path,
        &format!("INSERT INTO {output}(physical_id) VALUES(42)"),
    );
    // The native C API has disconnected and closed the driver before attestation.
    let completion = BuildCompletion {
        result_version: Req::new(1).unwrap(),
        run_id: request.identity.run_id.clone(),
        workspace_id: request.identity.workspace_id.clone(),
        declaration_sha256: request.identity.declaration_sha256.clone(),
        kind: CompletionKind::Direct,
        invocation_id: "closed-external-driver".into(),
        status: CompletionStatus::Succeeded,
        writers_stopped: true,
        completed_at: Timestamp::new("2026-10-06T00:00:00Z").unwrap(),
        completed_outputs: session
            .outputs
            .iter()
            .map(|o| CompletedOutput {
                table: o.table.clone(),
                engine_table: o.engine_table.clone(),
            })
            .collect(),
    };
    let mut store = BuildStore::open(
        &path,
        request.identity.root.clone(),
        Some(request.identity.workspace_id.clone()),
        &resources,
    )
    .unwrap();
    let mut unstopped = completion.clone();
    unstopped.writers_stopped = false;
    assert!(store.accept(&request.identity, unstopped).is_err());
    let mut changed = completion.clone();
    changed.completed_outputs[0].engine_table = "other.rows".into();
    assert!(store.accept(&request.identity, changed).is_err());
    let digest = store.accept(&request.identity, completion.clone()).unwrap();
    drop(store);
    let mut store = BuildStore::open(
        &path,
        request.identity.root.clone(),
        Some(request.identity.workspace_id.clone()),
        &resources,
    )
    .unwrap();
    assert_eq!(
        store.open_session(&request.identity).unwrap().completion,
        Some(completion)
    );
    let counts = store.start_export(&request.identity, &digest).unwrap();
    assert_eq!(
        counts.iter().map(|c| c.rows.get()).collect::<Vec<_>>(),
        vec![1, 0]
    );
    let (bytes, _) = store.fetch_export(0, &resources).unwrap().unwrap();
    let batch = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    for column in batch.columns() {
        assert_eq!(
            column
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap()
                .value(0),
            42
        );
    }
    assert!(store.fetch_export(1, &resources).unwrap().is_none());
}

#[test]
fn discovery_refuses_a_view_with_forged_old_materialization_metadata_without_binding_it() {
    let dir = directory();
    let path = dir.path().join("view.duckdb");
    let mut request = request(&path);
    let first = parquet(dir.path(), "first", &[5]);
    materialize(&path, &request, "one", "source_one", 1, &first);
    external_driver(
        &path,
        "DROP TABLE one.tracking; CREATE VIEW one.tracking AS SELECT error('must never bind view')::BIGINT AS id",
    );
    request.inputs = vec![BuildInput {
        alias: name("one"),
        relation: json!("one.tracking"),
    }];
    let mut store = BuildStore::open(
        &path,
        request.identity.root.clone(),
        Some(request.identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    let error = store.discover(request).unwrap_err();
    assert!(matches!(
        error,
        grv_adapter_duckdb::build::BuildError::Unknown(ref message)
            if message == "materialization kind differs from immutable receipt"
    ));
}

#[test]
fn managed_build_exports_every_exact_grv_native_type_with_nulls_and_typed_empty_output() {
    let dir = directory();
    let path = dir.path().join("types.duckdb");
    let mut request = request(&path);
    let columns = vec![
        ("b", json!("boolean"), "true"),
        ("i", json!("int64"), "-17::SMALLINT"),
        ("f", json!("float64"), "CAST('-0' AS FLOAT)"),
        ("d", json!("float64"), "2.5::DOUBLE"),
        ("s", json!("string"), "'🍕'"),
        ("blob", json!("binary"), "'\\x00\\xFF'::BLOB"),
        ("date", json!("date"), "DATE '2026-10-06'"),
        (
            "decimal",
            json!({"decimal":{"precision":30,"scale":2}}),
            "12345678901234567890.25::DECIMAL(30,2)",
        ),
        (
            "sec",
            json!({"timestamp":{"unit":"ms","utc":false}}),
            "TIMESTAMP_S '2026-10-06 12:34:56'",
        ),
        (
            "ms",
            json!({"timestamp":{"unit":"ms","utc":false}}),
            "TIMESTAMP_MS '2026-10-06 12:34:56.123'",
        ),
        (
            "us",
            json!({"timestamp":{"unit":"us","utc":false}}),
            "TIMESTAMP '2026-10-06 12:34:56.123456'",
        ),
        (
            "ns",
            json!({"timestamp":{"unit":"ns","utc":false}}),
            "TIMESTAMP_NS '2026-10-06 12:34:56.123456789'",
        ),
        (
            "utc",
            json!({"timestamp":{"unit":"us","utc":true}}),
            "TIMESTAMPTZ '2026-10-06 12:34:56.123456+00'",
        ),
    ];
    let query = format!(
        "SELECT {}",
        columns
            .iter()
            .map(|(n, _, sql)| format!("{sql} AS {n}"))
            .collect::<Vec<_>>()
            .join(",")
    );
    let output_contract = TableContract {
        columns: columns
            .iter()
            .map(|(n, t, _)| Column {
                name: (*n).into(),
                logical_type: t.clone(),
            })
            .collect(),
        ..contract()
    };
    let mappings = json!(
        columns
            .iter()
            .map(|(n, t, _)| json!({"name":n,"type":t,"source":n}))
            .collect::<Vec<_>>()
    );
    for output in &mut request.outputs {
        output.contract = output_contract.clone();
        output.columns = mappings.clone();
        output.source = json!({"sql":format!("{query} WHERE false")});
    }
    request.outputs[0].source = json!({"sql":format!("SELECT {} FROM (VALUES(false),(true)) flag(empty)", columns.iter().map(|(n,_,sql)|format!("CASE WHEN empty THEN NULL ELSE {sql} END AS {n}")).collect::<Vec<_>>().join(","))});
    let resources = Resources::default();
    let mut store = BuildStore::open(
        &path,
        request.identity.root.clone(),
        Some(request.identity.workspace_id.clone()),
        &resources,
    )
    .unwrap();
    let discovery = store.discover(request.clone()).unwrap();
    let session = store.prepare(preparation(discovery)).unwrap();
    let result = store.execute(execute(&session)).unwrap();
    assert_eq!(result.status, BuildExecutionStatus::Succeeded);
    let digest = store
        .accept(&request.identity, result.completion.unwrap())
        .unwrap();
    let counts = store.start_export(&request.identity, &digest).unwrap();
    assert_eq!(
        counts.iter().map(|c| c.rows.get()).collect::<Vec<_>>(),
        vec![2, 0]
    );
    let (bytes, _) = store.fetch_export(0, &resources).unwrap().unwrap();
    let batch = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(batch.num_columns(), 13);
    for column in batch.columns() {
        assert_eq!(column.null_count(), 1);
    }
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0),
        -17
    );
    assert_eq!(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap()
            .value(0)
            .to_bits(),
        (-0.0_f64).to_bits()
    );
    assert_eq!(
        batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap()
            .value(0),
        "🍕"
    );
    assert_eq!(
        batch
            .column(5)
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .unwrap()
            .value(0),
        [0, 255]
    );
    assert_eq!(
        batch
            .column(7)
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap()
            .value(0),
        1234567890123456789025
    );
    assert!(store.fetch_export(1, &resources).unwrap().is_none());
}

#[test]
fn managed_guard_rejects_mutable_private_and_external_dependencies_through_nested_macros() {
    for dependency in [
        "main.unrelated",
        "_grv.workspace_binding",
        "read_parquet('/tmp/forbidden-file.parquet')",
    ] {
        let dir = directory();
        let path = dir.path().join("dependencies.duckdb");
        let mut request = request(&path);
        external_driver(
            &path,
            "CREATE TABLE unrelated(id BIGINT); INSERT INTO unrelated VALUES(999)",
        );
        request.outputs.truncate(1);
        request.selected_outputs.truncate(1);
        request.outputs[0].source =
            json!({"sql":format!("SELECT CAST(count(*) AS BIGINT) AS id FROM {dependency}")});
        let mut store = BuildStore::open(
            &path,
            request.identity.root.clone(),
            Some(request.identity.workspace_id.clone()),
            &Resources::default(),
        )
        .unwrap();
        let discovery = store.discover(request.clone()).unwrap();
        let session = store.prepare(preparation(discovery)).unwrap();
        let result = store.execute(execute(&session)).unwrap();
        assert_eq!(result.status, BuildExecutionStatus::Failed);
        assert!(result.completion.is_none());
    }
    let dir = directory();
    let path = dir.path().join("macros.duckdb");
    let mut request = request(&path);
    let first = parquet(dir.path(), "macro_source", &[5]);
    materialize(&path, &request, "one", "source_one", 1, &first);
    external_driver(
        &path,
        "CREATE TABLE unrelated(id BIGINT); CREATE SCHEMA grv_input; CREATE TABLE grv_input.one(id BIGINT); CREATE MACRO bad_inner() AS TABLE SELECT id FROM unrelated; CREATE MACRO bad_outer() AS TABLE SELECT * FROM bad_inner(); CREATE MACRO good_inner() AS TABLE SELECT id FROM grv_input.one; CREATE MACRO good_outer() AS TABLE SELECT * FROM good_inner(); DROP TABLE grv_input.one; DROP SCHEMA grv_input",
    );
    request.inputs = vec![BuildInput {
        alias: name("one"),
        relation: json!("one.tracking"),
    }];
    request.outputs.truncate(1);
    request.selected_outputs.truncate(1);
    request.outputs[0].source = json!({"sql":"SELECT id FROM good_outer()"});
    let resources = Resources::default();
    let mut store = BuildStore::open(
        &path,
        request.identity.root.clone(),
        Some(request.identity.workspace_id.clone()),
        &resources,
    )
    .unwrap();
    let discovery = store.discover(request.clone()).unwrap();
    let session = store
        .prepare(PrepareBuildRequest {
            input_files: vec![BuildInputFiles {
                alias: name("one"),
                files: vec![first.clone()],
            }],
            ..preparation(discovery)
        })
        .unwrap();
    assert_eq!(
        store.execute(execute(&session)).unwrap().status,
        BuildExecutionStatus::Succeeded
    );
    drop(store);
    request.identity.attempt_id = Uuid::v4();
    request.identity.run_id = RunId::new("01ARZ3NDEKTSV4RRFFQ69G5FAX").unwrap();
    request.outputs[0].source = json!({"sql":"SELECT id FROM bad_outer()"});
    let mut store = BuildStore::open(
        &path,
        request.identity.root.clone(),
        Some(request.identity.workspace_id.clone()),
        &resources,
    )
    .unwrap();
    let discovery = store.discover(request).unwrap();
    let session = store
        .prepare(PrepareBuildRequest {
            input_files: vec![BuildInputFiles {
                alias: name("one"),
                files: vec![first],
            }],
            ..preparation(discovery)
        })
        .unwrap();
    let result = store.execute(execute(&session)).unwrap();
    assert_eq!(result.status, BuildExecutionStatus::Failed);
    assert!(result.completion.is_none());
}

#[test]
fn lost_or_inconsistent_session_history_and_substituted_output_views_refuse_recovery() {
    for corrupt in ["lost-history", "changed-session-id", "output-view"] {
        let dir = directory();
        let path = dir.path().join("history.duckdb");
        let request = request(&path);
        let identity = request.identity.clone();
        let resources = Resources::default();
        let mut store = BuildStore::open(
            &path,
            identity.root.clone(),
            Some(identity.workspace_id.clone()),
            &resources,
        )
        .unwrap();
        let discovery = store.discover(request).unwrap();
        let session = store.prepare(preparation(discovery)).unwrap();
        let result = store.execute(execute(&session)).unwrap();
        let digest = store.accept(&identity, result.completion.unwrap()).unwrap();
        drop(store);
        match corrupt {
            "lost-history" => external_driver(&path, "DROP TABLE _grv.build_sessions"),
            "changed-session-id" => external_driver(
                &path,
                &format!(
                    "UPDATE _grv.build_sessions SET session_id='{}'",
                    Uuid::v4().as_str()
                ),
            ),
            "output-view" => external_driver(
                &path,
                &format!(
                    "DROP TABLE {}; CREATE VIEW {} AS SELECT 999::BIGINT AS id",
                    session.outputs[0].engine_table, session.outputs[0].engine_table
                ),
            ),
            _ => unreachable!(),
        }
        let reopened = BuildStore::open(
            &path,
            identity.root.clone(),
            Some(identity.workspace_id.clone()),
            &resources,
        );
        if corrupt == "lost-history" {
            assert!(matches!(reopened, Err(BuildError::Unknown(_))));
            continue;
        }
        let mut store = reopened.unwrap();
        if corrupt == "output-view" {
            assert!(matches!(
                store.start_export(&identity, &digest),
                Err(BuildError::Unknown(_))
            ));
        } else {
            assert!(matches!(
                store.open_session(&identity),
                Err(BuildError::Unknown(_))
            ));
        }
    }
}
