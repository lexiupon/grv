use grv_adapter_api::*;
use serde_json::json;
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
fn session() -> BuildSession {
    BuildSession {
        options: json!({}),
        session_id: Uuid::v4(),
        identity: BuildIdentity {
            attempt_id: Uuid::v4(),
            root: "/root".into(),
            dataset: name("product"),
            run_id: RunId::new("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            workspace_id: Uuid::v4(),
            declaration_sha256: grv_types::sha256(b"fixed"),
            adapter_identity: AdapterIdentity {
                name: name("fixture"),
                package_version: "1".into(),
                interface_version: Req::new(1).unwrap(),
                binding_schema_version: Req::new(1).unwrap(),
            },
            connection_identity: "/engine".into(),
        },
        execution: BuildExecution::Managed,
        base_revision: U64::new(1).unwrap(),
        base_contracts: vec![],
        inputs: vec![InputBinding {
            alias: name("one"),
            relation: json!({"table":"raw.one"}),
            dataset: name("input"),
            table: name("shared"),
            revision: U64::new(1).unwrap(),
            generation_id: Uuid::v4(),
            contract: contract(),
            materialization: Materialization::Local,
        }],
        outputs: vec![OutputBinding {
            table: name("rows"),
            source: json!({"sql":"SELECT id FROM grv_input.one"}),
            columns: json!([{"name":"id","type":"int64","source":"id"}]),
            engine_table: "_private.rows".into(),
            contract: contract(),
        }],
        selected_outputs: vec![name("rows")],
        self_input: false,
        adapter_details: json!({}),
    }
}
fn completion(session: &BuildSession) -> BuildCompletion {
    BuildCompletion {
        result_version: Req::new(1).unwrap(),
        run_id: session.identity.run_id.clone(),
        workspace_id: session.identity.workspace_id.clone(),
        declaration_sha256: session.identity.declaration_sha256.clone(),
        kind: CompletionKind::Engine,
        invocation_id: "actual-invocation".into(),
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
    }
}
#[test]
fn completion_exact_schema_identity_selected_coverage_stopped_writers_and_digest() {
    let session = session();
    let c = completion(&session);
    c.validate_for(&session).unwrap();
    let mut value = serde_json::to_value(&c).unwrap();
    assert_eq!(value.as_object().unwrap().len(), 10);
    assert_eq!(
        value["completed_outputs"][0],
        json!({"table":"rows","engine_table":"_private.rows"})
    );
    assert_eq!(
        c.digest().unwrap(),
        grv_types::sha256(&grv_types::canonical_json(&value).unwrap())
    );
    value["row_counts"] = json!([]);
    assert!(serde_json::from_value::<BuildCompletion>(value).is_err());
    for field in [
        "run",
        "workspace",
        "digest",
        "mapping",
        "coverage",
        "duplicate",
        "writers",
        "version",
        "omission",
    ] {
        let mut bad = c.clone();
        match field {
            "run" => bad.run_id = RunId::new("01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap(),
            "workspace" => bad.workspace_id = Uuid::v4(),
            "digest" => bad.declaration_sha256 = grv_types::sha256(b"changed"),
            "mapping" => bad.completed_outputs[0].engine_table = "_private.other".into(),
            "coverage" => bad.completed_outputs.clear(),
            "duplicate" => bad.completed_outputs.push(bad.completed_outputs[0].clone()),
            "writers" => bad.writers_stopped = false,
            "version" => bad.result_version = Req::new(2).unwrap(),
            "omission" => bad.kind = CompletionKind::OmissionOnly,
            _ => unreachable!(),
        }
        assert!(bad.validate_for(&session).is_err(), "{field}");
    }
}
#[test]
fn alias_grouped_files_disambiguate_table_names_and_explicit_empty_inputs() {
    let session = session();
    let mut discovery = BuildDiscovery {
        discovery_id: Uuid::v4(),
        identity: session.identity.clone(),
        inputs: session.inputs.clone(),
        outputs: session.outputs.clone(),
    };
    let mut second = discovery.inputs[0].clone();
    second.alias = name("two");
    second.dataset = name("other");
    discovery.inputs.push(second);
    let file = VerifiedFile {
        table: name("shared"),
        partition: json!({}),
        version: U64::new(1).unwrap(),
        schema: FileSchema {
            columns: contract().columns,
        },
        access: FileAccess::Local,
        location: "/verified/file.parquet".into(),
        size: U64::new(100).unwrap(),
        sha256: grv_types::sha256(b"data"),
        validator: "v1".into(),
    };
    let prepare = PrepareBuildRequest {
        discovery,
        base_revision: U64::new(0).unwrap(),
        self_input: false,
        base_contracts: vec![],
        base_files: vec![],
        input_files: vec![
            BuildInputFiles {
                alias: name("one"),
                files: vec![file.clone()],
            },
            BuildInputFiles {
                alias: name("two"),
                files: vec![],
            },
        ],
        holds_confirmed: true,
    };
    prepare.validate().unwrap();
    let mut bad = prepare.clone();
    bad.input_files.pop();
    assert!(bad.validate().is_err());
    let mut bad = prepare.clone();
    bad.input_files[1].alias = name("one");
    assert!(bad.validate().is_err());
    let mut bad = prepare.clone();
    bad.holds_confirmed = false;
    assert!(bad.validate().is_err());
    let mut bad = prepare.clone();
    bad.input_files[0].files[0].schema.columns[0].logical_type = json!("utf8");
    assert!(bad.validate().is_err());
    let mut zero = prepare;
    zero.discovery.inputs[0].revision = U64::new(0).unwrap();
    assert!(zero.validate().is_err());
}
#[test]
fn self_input_keeps_whole_base_contracts_including_unmapped_zero_tables() {
    let session = session();
    let mut prepare = PrepareBuildRequest {
        discovery: BuildDiscovery {
            discovery_id: Uuid::v4(),
            identity: session.identity.clone(),
            inputs: vec![],
            outputs: session.outputs,
        },
        base_revision: U64::new(1).unwrap(),
        self_input: true,
        base_contracts: vec![NamedContract {
            table: name("not_an_output"),
            contract: contract(),
        }],
        base_files: vec![],
        input_files: vec![],
        holds_confirmed: true,
    };
    prepare.validate().unwrap();
    prepare.self_input = false;
    assert!(prepare.validate().is_err());
    prepare.self_input = true;
    prepare.base_revision = U64::new(0).unwrap();
    assert!(prepare.validate().is_err());
    prepare.base_contracts.clear();
    prepare.validate().unwrap();
}
#[test]
fn managed_execution_exact_queries_and_failed_result_never_claims_success() {
    let session = session();
    let request = ExecuteBuildRequest {
        queries: vec![BuildQuery {
            table: name("rows"),
            sql: "SELECT id FROM grv_input.one".into(),
        }],
        session: session.clone(),
    };
    request.validate().unwrap();
    let mut bad = request.clone();
    bad.queries[0].sql.push_str(" WHERE false");
    assert!(bad.validate().is_err());
    let mut bad = request;
    bad.session.execution = BuildExecution::External;
    assert!(bad.validate().is_err());
    let mut result = BuildExecutionResult {
        status: BuildExecutionStatus::Succeeded,
        completion: Some(completion(&session)),
        row_counts: vec![TableCount {
            table: name("rows"),
            rows: U64::new(0).unwrap(),
        }],
    };
    result.validate_for(&session).unwrap();
    result.status = BuildExecutionStatus::Failed;
    assert!(result.validate().is_err());
    result.completion = None;
    result.row_counts.clear();
    result.validate_for(&session).unwrap();
}
#[test]
fn reopen_preserves_completion_digest_candidate_and_terminal_outcomes() {
    let session = session();
    let completion = completion(&session);
    let mut record = BuildRecord {
        session,
        state: BuildState::Completed,
        completion_sha256: Some(completion.digest().unwrap()),
        completion: Some(completion.clone()),
        candidate: Some(completion),
        row_counts: vec![TableCount {
            table: name("rows"),
            rows: U64::new(0).unwrap(),
        }],
        outcome: Some(BuildOutcome {
            kind: OutcomeKind::Published,
            revision: Some(U64::new(1).unwrap()),
            operation_id: Some(RunId::new("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap()),
        }),
    };
    record.validate().unwrap();
    record.candidate.as_mut().unwrap().completed_at =
        Timestamp::new("2026-10-06T00:00:01Z").unwrap();
    assert!(record.validate().is_err());
    record.candidate = record.completion.clone();
    record.state = BuildState::Executing;
    assert!(record.validate().is_err());
    record.state = BuildState::Aborted;
    record.outcome = Some(BuildOutcome {
        kind: OutcomeKind::Aborted,
        revision: None,
        operation_id: None,
    });
    record.validate().unwrap();
    record.completion_sha256 = None;
    assert!(record.validate().is_err());
}

#[test]
fn terminal_outcomes_require_accepted_completion_or_recorded_abort() {
    let session = session();
    let completion = completion(&session);
    let mut record = BuildRecord {
        session,
        state: BuildState::Completed,
        completion_sha256: Some(completion.digest().unwrap()),
        completion: Some(completion),
        candidate: None,
        row_counts: vec![],
        outcome: Some(BuildOutcome {
            kind: OutcomeKind::NoOp,
            revision: Some(U64::new(0).unwrap()),
            operation_id: None,
        }),
    };
    record.validate().unwrap();
    let mut missing = record.clone();
    missing.completion = None;
    missing.completion_sha256 = None;
    assert!(missing.validate().is_err());
    record.state = BuildState::Aborted;
    assert!(record.validate().is_err());
    record.outcome = Some(BuildOutcome {
        kind: OutcomeKind::Aborted,
        revision: None,
        operation_id: None,
    });
    record.validate().unwrap();
    record.state = BuildState::Completed;
    assert!(record.validate().is_err());
}

#[test]
fn preparation_keeps_options_execution_selection_base_and_mappings_fixed() {
    let mut session = session();
    session.options = json!({"worker_limit":1});
    let request = DiscoverBuildRequest {
        identity: session.identity.clone(),
        options: session.options.clone(),
        execution: session.execution,
        inputs: session
            .inputs
            .iter()
            .map(|i| BuildInput {
                alias: i.alias.clone(),
                relation: i.relation.clone(),
            })
            .collect(),
        outputs: session
            .outputs
            .iter()
            .map(|o| BuildOutput {
                table: o.table.clone(),
                source: o.source.clone(),
                columns: o.columns.clone(),
                contract: o.contract.clone(),
            })
            .collect(),
        selected_outputs: session.selected_outputs.clone(),
        self_input: session.self_input,
    };
    let preparation = PrepareBuildRequest {
        discovery: BuildDiscovery {
            discovery_id: Uuid::v4(),
            identity: session.identity.clone(),
            inputs: session.inputs.clone(),
            outputs: session.outputs.clone(),
        },
        base_revision: session.base_revision,
        self_input: false,
        base_contracts: vec![],
        base_files: vec![],
        input_files: vec![BuildInputFiles {
            alias: name("one"),
            files: vec![],
        }],
        holds_confirmed: true,
    };
    session.validate_for(&request, &preparation).unwrap();
    for changed in ["options", "execution", "selected", "mapping", "base"] {
        let mut bad = session.clone();
        match changed {
            "options" => bad.options = json!({"worker_limit":2}),
            "execution" => bad.execution = BuildExecution::External,
            "selected" => bad.selected_outputs.clear(),
            "mapping" => bad.outputs[0].engine_table = "_private.changed".into(),
            "base" => bad.base_revision = U64::new(2).unwrap(),
            _ => unreachable!(),
        };
        assert!(
            bad.validate_for(&request, &preparation).is_err(),
            "{changed}"
        );
    }
    let mut bad = request;
    bad.options = json!([]);
    assert!(bad.validate().is_err());
}

#[test]
fn build_column_mappings_are_required_ordered_exact_and_immutable() {
    let original = session();
    original.validate().unwrap();
    let mut missing = serde_json::to_value(&original.outputs[0]).unwrap();
    missing.as_object_mut().unwrap().remove("columns");
    assert!(serde_json::from_value::<OutputBinding>(missing).is_err());
    for columns in [
        json!({"name":"id","type":"int64"}),
        json!([]),
        json!([{"name":"wrong","type":"int64"}]),
        json!([{"name":"id","type":"utf8"}]),
        json!([{"name":"id","type":"int64","derive":{"column":"other"}}]),
    ] {
        let mut changed = original.clone();
        changed.outputs[0].columns = columns;
        assert!(changed.validate().is_err());
    }
    let mut output = original.outputs[0].clone();
    output.columns = json!([{"name":"id","source":"physical_id","type":"int64"}]);
    output.validate().unwrap();
    let request = DiscoverBuildRequest {
        identity: original.identity.clone(),
        options: original.options.clone(),
        execution: original.execution,
        inputs: original
            .inputs
            .iter()
            .map(|i| BuildInput {
                alias: i.alias.clone(),
                relation: i.relation.clone(),
            })
            .collect(),
        outputs: vec![BuildOutput {
            table: output.table.clone(),
            source: output.source.clone(),
            columns: output.columns.clone(),
            contract: output.contract.clone(),
        }],
        selected_outputs: original.selected_outputs.clone(),
        self_input: false,
    };
    let mut discovery = BuildDiscovery {
        discovery_id: Uuid::v4(),
        identity: original.identity,
        inputs: original.inputs,
        outputs: vec![output],
    };
    discovery.validate_for(&request).unwrap();
    discovery.outputs[0].columns[0]["source"] = json!("id");
    assert!(discovery.validate_for(&request).is_err());
}

#[test]
fn build_authoring_type_aliases_are_preserved_while_contracts_stay_logical() {
    let mut session = session();
    for (authoring, logical) in [
        ("bool", json!("boolean")),
        ("double", json!("float64")),
        ("utf8", json!("string")),
        ("date32", json!("date")),
        (
            "decimal128(30,2)",
            json!({"decimal":{"precision":30,"scale":2}}),
        ),
        (
            "timestamp(us,UTC)",
            json!({"timestamp":{"unit":"us","utc":true}}),
        ),
    ] {
        session.outputs[0].contract.columns[0].logical_type = logical;
        session.outputs[0].columns[0]["type"] = json!(authoring);
        session.validate().unwrap();
        assert_eq!(
            serde_json::to_value(&session).unwrap()["outputs"][0]["columns"][0]["type"],
            authoring
        );
    }
}
