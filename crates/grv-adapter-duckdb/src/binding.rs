//! Pure fragments, offline connection location and exact output contracts.
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use grv_adapter_api::{
    ConnectionLocator, ExtractTable, Mode, PointDescriptor, Registry, TableContract,
};
use serde_json::{Value, json};
use std::{collections::HashMap, io, path::Path, sync::Arc};

pub fn registry() -> Registry {
    let source: Value =
        serde_json::from_str(include_str!("../../../spec/adapters/duckdb.schema.json")).unwrap();
    let mut defs = serde_json::Map::new();
    let mut points: Vec<PointDescriptor> = [
        ("connection", "connection", false),
        ("options", "push_options", true),
        ("table_source", "extract_source", false),
        ("column_source", "column_source", false),
        ("connection_details", "connection_details", false),
        ("source_identity", "source_identity", false),
        ("source_job", "source_job", false),
        ("push_result", "push_result", false),
    ]
    .into_iter()
    .map(|(point, definition, default)| {
        defs.insert(definition.into(), source["$defs"][definition].clone());
        PointDescriptor {
            point: point.into(),
            mode: Mode::Extract,
            schema_pointer: format!("/$defs/{definition}"),
            default_value: if default { json!({}) } else { Value::Null },
            has_default: default,
        }
    })
    .collect();
    points.extend(
        [
            ("connection", "connection", false),
            ("connection_details", "connection_details", false),
            ("options", "options", true),
            ("target", "pull_target", false),
            ("table_target", "pull_table_target", true),
            ("table_select", "select", false),
            ("pull_plan", "pull_plan", false),
            ("pull_result", "pull_result", false),
        ]
        .into_iter()
        .map(|(point, definition, default)| {
            defs.insert(definition.into(), source["$defs"][definition].clone());
            PointDescriptor {
                point: point.into(),
                mode: Mode::Pull,
                schema_pointer: format!("/$defs/{definition}"),
                default_value: if default { json!({}) } else { Value::Null },
                has_default: default,
            }
        }),
    );
    for (mode, source_definition) in [
        (Mode::ManagedBuild, "managed_build_source"),
        (Mode::ExternalBuild, "external_build_source"),
    ] {
        points.extend(
            [
                ("connection", "connection", false),
                ("connection_details", "connection_details", false),
                ("options", "push_options", true),
                ("table_source", source_definition, false),
                ("column_source", "column_source", false),
                ("build_input", "build_input", false),
                ("session_details", "session_details", false),
                ("push_result", "push_result", false),
            ]
            .into_iter()
            .map(|(point, definition, default)| {
                defs.insert(definition.into(), source["$defs"][definition].clone());
                PointDescriptor {
                    point: point.into(),
                    mode,
                    schema_pointer: format!("/$defs/{definition}"),
                    default_value: if default { json!({}) } else { Value::Null },
                    has_default: default,
                }
            }),
        );
    }
    // Inspection observations use local engine summaries. Parent-only GRV
    // projections retain the common public shapes in this offline bundle.
    let public: Value = serde_json::from_str(include_str!(
        "../../../spec/grv-client-v1-command-output.schema.json"
    ))
    .unwrap();
    for definition in [
        "engine_materialization",
        "engine_session",
        "inspection_result",
    ] {
        let mut value = source["$defs"][definition].clone();
        fn localize(value: &mut Value) {
            match value {
                Value::Object(object) => {
                    if let Some(Value::String(reference)) = object.get_mut("$ref")
                        && let Some(fragment) =
                            reference.strip_prefix("../grv-client-v1-command-output.schema.json#")
                    {
                        *reference = format!("#{fragment}");
                    }
                    object.values_mut().for_each(localize);
                }
                Value::Array(array) => array.iter_mut().for_each(localize),
                _ => {}
            }
        }
        localize(&mut value);
        defs.insert(definition.into(), value);
    }
    for definition in ["materialization", "application_import", "run_observation"] {
        defs.insert(definition.into(), public["$defs"][definition].clone());
    }
    for (point, definition) in [
        ("connection", "connection"),
        ("connection_details", "connection_details"),
        ("inspection_result", "inspection_result"),
    ] {
        points.push(PointDescriptor {
            point: point.into(),
            mode: Mode::Inspect,
            schema_pointer: format!("/$defs/{definition}"),
            default_value: Value::Null,
            has_default: false,
        });
    }
    Registry {
        schema_bundle: json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$defs":defs}),
        points,
    }
}
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
pub fn identifier(text: &str) -> bool {
    let mut bytes = text.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}
fn relation_part(text: &str) -> bool {
    let mut bytes = text.bytes();
    bytes
        .next()
        .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        && bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
}
pub fn relation(source: &Value) -> io::Result<(String, String, Option<String>)> {
    let object = source
        .as_object()
        .ok_or_else(|| invalid("source must be an object"))?;
    if object.keys().any(|key| key != "table" && key != "filter") {
        return Err(invalid("unknown extraction source member"));
    }
    let text = object
        .get("table")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("source.table is required"))?;
    let (schema, table) = text
        .split_once('.')
        .filter(|(schema, table)| relation_part(schema) && relation_part(table))
        .ok_or_else(|| invalid("source.table must be two identifiers"))?;
    if crate::pull::reserved_namespace(schema) {
        return Err(invalid("reserved extraction source namespace"));
    }
    let filter = object
        .get("filter")
        .map(|value| {
            value
                .as_str()
                .filter(|filter| !filter.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| invalid("filter must be a nonempty predicate"))
        })
        .transpose()?;
    Ok((schema.into(), table.into(), filter))
}
pub fn database(connection: &Value) -> io::Result<&str> {
    let object = connection
        .as_object()
        .ok_or_else(|| invalid("connection must be an object"))?;
    if object.len() != 1 {
        return Err(invalid("unknown connection member"));
    }
    object
        .get("database")
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty() && *text != ":memory:" && !text.contains("://"))
        .ok_or_else(|| invalid("database must be a local native path"))
}
pub fn validate(mut declaration: Value) -> io::Result<Value> {
    database(&declaration["connection"])?;
    if declaration
        .get("options")
        .is_some_and(|value| value != &json!({}))
    {
        return Err(invalid("extraction tuning options must be empty"));
    }
    let tables = declaration
        .get_mut("tables")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| invalid("tables must be an array"))?;
    for table in tables {
        relation(&table["source"])?;
        for column in table
            .get_mut("columns")
            .and_then(Value::as_array_mut)
            .ok_or_else(|| invalid("columns must be expanded"))?
        {
            if !column.is_object() {
                return Err(invalid("column must be an object"));
            }
            if column.get("derive").is_some() {
                continue;
            }
            if column.get("source").is_none() {
                column["source"] = column["name"].clone();
            }
            if !column["source"].as_str().is_some_and(identifier) {
                return Err(invalid("column source must be an identifier"));
            }
        }
    }
    Ok(declaration)
}
pub fn validate_build(mut declaration: Value, mode: Mode) -> io::Result<Value> {
    database(&declaration["connection"])?;
    if ![Mode::ManagedBuild, Mode::ExternalBuild].contains(&mode) {
        return Err(invalid("invalid build mode"));
    }
    if declaration.get("options").is_none() {
        declaration["options"] = json!({});
    }
    extraction_options(&declaration["options"])?;
    let build = declaration
        .get_mut("build")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| invalid("build configuration is required"))?;
    let execution = if mode == Mode::ManagedBuild {
        "managed"
    } else {
        "external"
    };
    if build.get("execution").is_some_and(|v| v != execution) {
        return Err(invalid("build execution differs from selected mode"));
    }
    build.entry("execution").or_insert(json!(execution));
    build.entry("inputs").or_insert(json!([]));
    build.entry("self_input").or_insert(json!(false));
    for input in build["inputs"]
        .as_array()
        .ok_or_else(|| invalid("build inputs must be an array"))?
    {
        let relation = input["table"]
            .as_str()
            .ok_or_else(|| invalid("build input relation missing"))?;
        let (schema, table) = relation
            .split_once('.')
            .ok_or_else(|| invalid("build input must be a qualified relation"))?;
        for part in [schema, table] {
            crate::pull::RelationName::new(part)?;
        }
        if crate::pull::reserved_namespace(schema) {
            return Err(invalid("reserved build input namespace"));
        }
    }
    let tables = declaration
        .get_mut("tables")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| invalid("tables must be an array"))?;
    for table in tables {
        let source = table["source"]
            .as_object()
            .filter(|o| o.len() == 1)
            .ok_or_else(|| invalid("build source must be exactly one mapping"))?;
        if mode == Mode::ManagedBuild {
            if !source
                .get("sql")
                .and_then(Value::as_str)
                .is_some_and(|sql| !sql.is_empty() && !sql.contains('\0'))
            {
                return Err(invalid("managed build requires fixed SQL"));
            }
        } else {
            crate::pull::RelationName::new(
                source
                    .get("table")
                    .and_then(Value::as_str)
                    .ok_or_else(|| invalid("external build requires table mapping"))?,
            )?;
        }
        for column in table
            .get_mut("columns")
            .and_then(Value::as_array_mut)
            .ok_or_else(|| invalid("columns must be expanded"))?
        {
            if column.get("derive").is_some() {
                continue;
            }
            if column.get("source").is_none() {
                column["source"] = column["name"].clone();
            }
            if !column["source"].as_str().is_some_and(identifier) {
                return Err(invalid("column source must be an identifier"));
            }
        }
    }
    Ok(declaration)
}
pub fn locate_build(
    connection: Value,
    run: Option<grv_types::RunId>,
) -> io::Result<ConnectionLocator> {
    let mut locator = locate_pull(connection)?;
    locator.session_lock_path = run.map(|run| {
        format!(
            "{}.grv-session-{}.lock",
            locator.engine_path.as_ref().unwrap(),
            run
        )
    });
    Ok(locator)
}
pub fn extraction_options(options: &Value) -> io::Result<()> {
    if options != &json!({}) {
        return Err(invalid("extraction tuning options must be empty"));
    }
    Ok(())
}
pub fn validate_pull(mut declaration: Value) -> io::Result<Value> {
    database(&declaration["connection"])?;
    let target = declaration["target"]
        .as_object()
        .filter(|target| target.len() == 1)
        .ok_or_else(|| invalid("target must name one destination schema"))?;
    let namespace = target
        .get("schema")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("target.schema is required"))?;
    crate::pull::RelationName::new(namespace)?;
    if crate::pull::reserved_namespace(namespace) {
        return Err(invalid("reserved destination namespace"));
    }
    if declaration.get("options").is_none() {
        declaration["options"] = json!({});
    }
    let options = declaration["options"]
        .as_object_mut()
        .ok_or_else(|| invalid("options must be an object"))?;
    if options
        .keys()
        .any(|key| key != "refresh" && key != "materialization")
    {
        return Err(invalid("unknown pull option"));
    }
    options.entry("materialization").or_insert(json!("local"));
    options.entry("refresh").or_insert(json!("auto"));
    if ![json!("local"), json!("s3-view")].contains(&options["materialization"])
        || ![json!("auto"), json!("full")].contains(&options["refresh"])
    {
        return Err(invalid("invalid pull option"));
    }
    let tables = declaration
        .get_mut("tables")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| invalid("tables must be an array"))?;
    for table in tables {
        let name = table["name"]
            .as_str()
            .ok_or_else(|| invalid("table.name is required"))?
            .to_owned();
        if table.get("target").is_none() {
            table["target"] = json!({});
        }
        let target = table["target"]
            .as_object_mut()
            .ok_or_else(|| invalid("table target must be an object"))?;
        if target.keys().any(|key| key != "table") {
            return Err(invalid("unknown table target member"));
        }
        target.entry("table").or_insert(json!(name));
        crate::pull::RelationName::new(
            target["table"]
                .as_str()
                .ok_or_else(|| invalid("target.table must be a string"))?,
        )?;
        if let Some(select) = table.get("select") {
            let object = select
                .as_object()
                .filter(|object| object.len() == 1)
                .ok_or_else(|| invalid("select must have exactly one SQL or file member"))?;
            if object.keys().any(|key| key != "sql" && key != "file")
                || object.values().any(|value| {
                    !value
                        .as_str()
                        .is_some_and(|text| !text.is_empty() && !text.contains('\0'))
                })
            {
                return Err(invalid("invalid SQL select"));
            }
        }
    }
    Ok(declaration)
}
pub fn locate_pull(connection: Value) -> io::Result<ConnectionLocator> {
    let path = crate::lock::canonical_engine_path(Path::new(database(&connection)?))?;
    let path = path
        .to_str()
        .ok_or_else(|| invalid("database path must be UTF-8"))?
        .to_owned();
    Ok(ConnectionLocator {
        canonical_connection: json!({"database":path}),
        identity: Some(format!("duckdb:{path}")),
        engine_path: Some(path),
        session_lock_path: None,
    })
}
pub fn locate(connection: Value) -> io::Result<ConnectionLocator> {
    let path = crate::lock::canonical_engine_path(Path::new(database(&connection)?))?;
    if !path.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "source database does not exist",
        ));
    }
    let path = path
        .to_str()
        .ok_or_else(|| invalid("database path must be UTF-8"))?
        .to_owned();
    Ok(ConnectionLocator {
        canonical_connection: json!({"database":path}),
        identity: Some(format!("duckdb:{path}")),
        engine_path: Some(path),
        session_lock_path: None,
    })
}
pub fn source_columns(table: &ExtractTable) -> io::Result<Vec<String>> {
    let columns = table
        .columns
        .as_array()
        .ok_or_else(|| invalid("column mappings must be an array"))?;
    table
        .contract
        .columns
        .iter()
        .map(|output| {
            let mapping = columns
                .iter()
                .find(|column| column.get("name").and_then(Value::as_str) == Some(&output.name))
                .ok_or_else(|| invalid("missing projected column mapping"))?;
            let source = mapping
                .get("source")
                .and_then(Value::as_str)
                .unwrap_or(&output.name);
            if !identifier(source) {
                return Err(invalid("column source must be an identifier"));
            }
            Ok(source.into())
        })
        .collect()
}
pub fn output_schema(contract: &TableContract) -> io::Result<Arc<Schema>> {
    contract.validate().map_err(io::Error::other)?;
    let fields = contract
        .columns
        .iter()
        .map(|column| {
            let value = &column.logical_type;
            let datatype = match value.as_str() {
                Some("boolean") => DataType::Boolean,
                Some("int64") => DataType::Int64,
                Some("float64") => DataType::Float64,
                Some("string") => DataType::Utf8,
                Some("binary") => DataType::Binary,
                Some("date") => DataType::Date32,
                _ if value.get("decimal").is_some() => DataType::Decimal128(
                    value["decimal"]["precision"].as_u64().unwrap() as u8,
                    value["decimal"]["scale"].as_u64().unwrap() as i8,
                ),
                _ => DataType::Timestamp(
                    match value["timestamp"]["unit"].as_str().unwrap() {
                        "ms" => TimeUnit::Millisecond,
                        "us" => TimeUnit::Microsecond,
                        "ns" => TimeUnit::Nanosecond,
                        _ => unreachable!(),
                    },
                    value["timestamp"]["utc"]
                        .as_bool()
                        .unwrap()
                        .then(|| Arc::from("UTC")),
                ),
            };
            let mut metadata = HashMap::new();
            if let Some(extension) = contract.column_ext.get(&column.name) {
                metadata.insert(
                    "grv.column.ext".into(),
                    String::from_utf8(
                        grv_types::canonical_json(extension).map_err(io::Error::other)?,
                    )
                    .unwrap(),
                );
            }
            Ok(Field::new(&column.name, datatype, true).with_metadata(metadata))
        })
        .collect::<io::Result<Vec<_>>>()?;
    let metadata = HashMap::from([(
        "grv.table.extensions".into(),
        String::from_utf8(
            grv_types::canonical_json(&contract.extensions).map_err(io::Error::other)?,
        )
        .unwrap(),
    )]);
    Ok(Arc::new(Schema::new_with_metadata(fields, metadata)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pure_validation_fills_only_missing_column_source_and_preserves_authoring() {
        let declaration = json!({"connection":{"database":"./authoring.duckdb"},"options":{},"tables":[{"name":"rows","source":{"table":"main.rows","filter":"selected"},"columns":[{"name":"id","type":"int64"},{"name":"label","source":"native_label","type":"string"}]}]});
        let normalized = validate(declaration.clone()).unwrap();
        assert_eq!(normalized["connection"], declaration["connection"]);
        assert_eq!(
            normalized["tables"][0]["source"],
            declaration["tables"][0]["source"]
        );
        assert_eq!(normalized["tables"][0]["columns"][0]["source"], "id");
        assert_eq!(
            normalized["tables"][0]["columns"][1]["source"],
            "native_label"
        );
        assert!(extraction_options(&json!({"threads":2})).is_err());
    }
    #[test]
    fn malformed_column_returns_error_without_panicking() {
        assert!(validate(json!({"connection":{"database":"./source.duckdb"},"tables":[{"source":{"table":"main.rows"},"columns":[null]}]})).is_err());
    }
    #[test]
    fn offline_location_canonicalizes_without_opening_engine_or_creating_metadata() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.duckdb");
        std::fs::write(&path, b"offline metadata lookup does not open DuckDB").unwrap();
        let before = std::fs::read(&path).unwrap();
        let located = locate(json!({"database":path})).unwrap();
        assert_eq!(
            located.engine_path,
            Some(
                std::fs::canonicalize(&path)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .into()
            )
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    }
}
