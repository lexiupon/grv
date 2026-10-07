//! Strict YAML authoring and parent-owned preparation of adapter fragments.
use crate::{Error, Result, contract};
use grv_adapter_api::{Column, Mode, TableContract};
use grv_adapter_host::registry::CheckedRegistry;
use grv_types::{ErrorCode, Name};
use serde_json::{Map, Number, Value, json};
use std::{collections::BTreeSet, fs::File, io::Read, path::Path, sync::OnceLock};
use yaml_rust2::{
    parser::{Event, Parser},
    scanner::TScalarStyle,
};

pub const DECLARATION_LIMIT: usize = 64 * 1024 * 1024;

pub fn parse_yaml(text: &str) -> Result<Value> {
    if text.len() > DECLARATION_LIMIT {
        return Err(Error("declaration exceeds 64MiB".into()));
    }
    let mut parser = Parser::new_from_str(text);
    fn next(parser: &mut Parser<std::str::Chars<'_>>) -> Result<Event> {
        parser
            .next_token()
            .map(|(event, _)| event)
            .map_err(|_| Error("invalid YAML declaration".into()))
    }
    fn node(parser: &mut Parser<std::str::Chars<'_>>, event: Event, depth: usize) -> Result<Value> {
        if depth > 64 {
            return Err(Error("declaration nesting exceeds supported limit".into()));
        }
        match event {
            Event::Scalar(value, style, _, tag) => {
                if tag.is_some() {
                    return Err(Error("explicit YAML tags are forbidden".into()));
                }
                if style != TScalarStyle::Plain {
                    return Ok(Value::String(value));
                }
                scalar(&value)
            }
            Event::SequenceStart(_, tag) => {
                if tag.is_some() {
                    return Err(Error("explicit YAML tags are forbidden".into()));
                }
                let mut values = Vec::new();
                loop {
                    let event = next(parser)?;
                    if event == Event::SequenceEnd {
                        break;
                    }
                    values.push(node(parser, event, depth + 1)?);
                }
                Ok(Value::Array(values))
            }
            Event::MappingStart(_, tag) => {
                if tag.is_some() {
                    return Err(Error("explicit YAML tags are forbidden".into()));
                }
                let mut values = Map::new();
                loop {
                    let event = next(parser)?;
                    if event == Event::MappingEnd {
                        break;
                    }
                    let key = node(parser, event, depth + 1)?;
                    let Value::String(key) = key else {
                        return Err(Error("YAML mapping keys must be strings".into()));
                    };
                    if values.contains_key(&key) || key == "<<" {
                        return Err(Error("duplicate or merge YAML mapping key".into()));
                    }
                    let event = next(parser)?;
                    values.insert(key, node(parser, event, depth + 1)?);
                }
                Ok(Value::Object(values))
            }
            Event::Alias(_) => Err(Error("YAML aliases are forbidden".into())),
            _ => Err(Error("unexpected YAML event".into())),
        }
    }
    if next(&mut parser)? != Event::StreamStart || next(&mut parser)? != Event::DocumentStart {
        return Err(Error("one YAML document required".into()));
    }
    let event = next(&mut parser)?;
    let result = node(&mut parser, event, 0)?;
    if next(&mut parser)? != Event::DocumentEnd || next(&mut parser)? != Event::StreamEnd {
        return Err(Error("one YAML document required".into()));
    }
    Ok(result)
}

// YAML1.2 Core scalars, independent of YAML1.1's yes/no/date coercions.
fn scalar(value: &str) -> Result<Value> {
    match value {
        "" | "~" | "null" | "Null" | "NULL" => return Ok(Value::Null),
        "true" | "True" | "TRUE" => return Ok(Value::Bool(true)),
        "false" | "False" | "FALSE" => return Ok(Value::Bool(false)),
        ".nan" | ".NaN" | ".NAN" | ".inf" | ".Inf" | ".INF" | "+.inf" | "+.Inf" | "+.INF"
        | "-.inf" | "-.Inf" | "-.INF" => {
            return Err(Error("nonfinite YAML values are forbidden".into()));
        }
        _ => {}
    }
    if let Some(digits) = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0o"))
    {
        let radix = if value.starts_with("0x") { 16 } else { 8 };
        if !digits.is_empty() && digits.chars().all(|c| c.is_digit(radix)) {
            return u64::from_str_radix(digits, radix)
                .map(|n| json!(n))
                .map_err(|_| Error("YAML integer exceeds supported range".into()));
        }
    }
    let digits = value.strip_prefix(['+', '-']).unwrap_or(value);
    if !digits.is_empty() && digits.bytes().all(|c| c.is_ascii_digit()) {
        return if value.starts_with('-') {
            value.parse::<i64>().map(|n| json!(n))
        } else {
            value
                .trim_start_matches('+')
                .parse::<u64>()
                .map(|n| json!(n))
        }
        .map_err(|_| Error("YAML integer exceeds supported range".into()));
    }
    if value.contains(['.', 'e', 'E']) && value.parse::<f64>().is_ok() {
        let number = value.parse::<f64>().unwrap();
        return Number::from_f64(number)
            .map(Value::Number)
            .ok_or_else(|| Error("nonfinite YAML number".into()));
    }
    Ok(Value::String(value.into()))
}

fn read_text(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    if file.metadata()?.len() > DECLARATION_LIMIT as u64 {
        return Err(Error("local reference exceeds64MiB".into()));
    }
    let mut bytes = Vec::new();
    (&mut file)
        .take(DECLARATION_LIMIT as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > DECLARATION_LIMIT {
        return Err(Error("local reference exceeds64MiB".into()));
    }
    String::from_utf8(bytes).map_err(|_| Error("local reference is not UTF-8".into()))
}
pub fn load(path: &Path) -> Result<Value> {
    let mut declaration = parse_yaml(&read_text(path)?)?;
    validate_common(&declaration)?;
    expand_references(&mut declaration, path.parent().unwrap_or(Path::new(".")))?;
    validate_common(&declaration)?;
    validate_cross_fields(&declaration)?;
    Ok(declaration)
}
pub fn validate_common(declaration: &Value) -> Result<()> {
    static VALIDATOR: OnceLock<jsonschema::Validator> = OnceLock::new();
    let validator = VALIDATOR.get_or_init(|| {
        let schema: Value = serde_json::from_str(include_str!(
            "../../../spec/grv-client-v1-declaration.schema.json"
        ))
        .unwrap();
        jsonschema::draft202012::new(&schema).expect("common declaration schema")
    });
    if !validator.is_valid(declaration) {
        return Err(Error("declaration does not satisfy common envelope".into()));
    }
    Ok(())
}
fn expand_columns(value: &mut Value, directory: &Path) -> Result<()> {
    if value.is_array() {
        return Ok(());
    }
    let object = value
        .as_object()
        .ok_or_else(|| Error("invalid column reference".into()))?;
    if object.len() != 1 {
        return Err(Error("invalid column reference".into()));
    }
    if let Some(path) = object.get("file").and_then(Value::as_str) {
        let columns = parse_yaml(&read_text(&directory.join(path))?)?;
        if !columns.is_array() {
            return Err(Error(
                "column files must contain an array, without recursive references".into(),
            ));
        }
        *value = columns;
        return Ok(());
    }
    if let Some(path) = object.get("ipc").and_then(Value::as_str) {
        let mut bytes = Vec::new();
        File::open(directory.join(path))?
            .take(DECLARATION_LIMIT as u64 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > DECLARATION_LIMIT {
            return Err(Error("IPC schema exceeds supported limit".into()));
        }
        *value = ipc_columns(&bytes)?;
        return Ok(());
    }
    Err(Error("unknown column reference".into()))
}
fn expand_references(declaration: &mut Value, directory: &Path) -> Result<()> {
    for table in declaration["tables"].as_array_mut().unwrap() {
        if let Some(columns) = table.get_mut("columns") {
            expand_columns(columns, directory)?;
        }
        if let Some(columns) = table
            .get_mut("expect")
            .and_then(|expect| expect.get_mut("columns"))
        {
            expand_columns(columns, directory)?;
        }
        if let Some(select) = table.get_mut("select")
            && let Some(path) = select.get("file").and_then(Value::as_str)
        {
            let sql = read_text(&directory.join(path))?;
            *select = json!({"sql":sql});
        }
    }
    Ok(())
}
fn ipc_columns(bytes: &[u8]) -> Result<Value> {
    if bytes.len() < 8 || bytes[..4] != [255; 4] {
        return Err(Error("modern IPC schema message required".into()));
    }
    let length = i32::from_le_bytes(bytes[4..8].try_into().unwrap());
    if length <= 0 || length % 8 != 0 || length as usize + 8 != bytes.len() {
        return Err(Error(
            "IPC reference must contain exactly one schema".into(),
        ));
    }
    let message = arrow_ipc::root_as_message(&bytes[8..])
        .map_err(|_| Error("invalid IPC schema metadata".into()))?;
    if message.version() != arrow_ipc::MetadataVersion::V5
        || message.bodyLength() != 0
        || message.header_type() != arrow_ipc::MessageHeader::Schema
    {
        return Err(Error("invalid IPC schema message".into()));
    }
    let wire_schema = message.header_as_schema().unwrap();
    if wire_schema.endianness() != arrow_ipc::Endianness::Little {
        return Err(Error("big-endian IPC reference is unsupported".into()));
    }
    if let Some(fields) = wire_schema.fields() {
        for field in fields {
            grv_adapter_wire::ipc::validate_field(field).map_err(|e| Error(e.to_string()))?;
        }
    }
    let schema = arrow_ipc::convert::fb_to_schema(wire_schema);
    let columns: Result<Vec<Value>> = schema
        .fields()
        .iter()
        .map(|field| {
            if field
                .metadata()
                .keys()
                .any(|key| key.starts_with("ARROW:extension:"))
            {
                return Err(Error("unsupported Arrow extension".into()));
            }
            let name = field.name();
            let value = match field.data_type() {
                arrow_schema::DataType::Boolean => "bool".into(),
                arrow_schema::DataType::Int64 => "int64".into(),
                arrow_schema::DataType::Float64 => "double".into(),
                arrow_schema::DataType::Utf8 => "utf8".into(),
                arrow_schema::DataType::Binary => "binary".into(),
                arrow_schema::DataType::Date32 => "date32".into(),
                arrow_schema::DataType::Decimal128(p, s) => format!("decimal128({p},{s})"),
                arrow_schema::DataType::Timestamp(unit, timezone) => {
                    let unit = match unit {
                        arrow_schema::TimeUnit::Millisecond => "ms",
                        arrow_schema::TimeUnit::Microsecond => "us",
                        arrow_schema::TimeUnit::Nanosecond => "ns",
                        _ => return Err(Error("unsupported IPC timestamp".into())),
                    };
                    let suffix = match timezone.as_deref() {
                        None => "",
                        Some("UTC") => ",UTC",
                        _ => return Err(Error("IPC timestamp must use UTC interchange".into())),
                    };
                    format!("timestamp({unit}{suffix})")
                }
                _ => return Err(Error(format!("unsupported IPC column {name}"))),
            };
            contract::authoring_type(&value)?;
            Ok(json!({"name":name,"type":value}))
        })
        .collect();
    Ok(Value::Array(columns?))
}

pub fn mode(declaration: &Value) -> Result<Mode> {
    match (declaration["kind"].as_str(), declaration.get("build")) {
        (Some("pull"), None) => Ok(Mode::Pull),
        (Some("push"), None) => Ok(Mode::Extract),
        (Some("push"), Some(build)) => match build["execution"].as_str() {
            None | Some("managed") => Ok(Mode::ManagedBuild),
            Some("external") => Ok(Mode::ExternalBuild),
            _ => Err(Error("unknown build execution mode".into())),
        },
        _ => Err(Error("invalid declaration direction".into())),
    }
}
pub fn apply_point_defaults(declaration: &mut Value, registry: &CheckedRegistry) -> Result<()> {
    let mode = mode(declaration)?;
    if matches!(mode, Mode::ManagedBuild | Mode::ExternalBuild) {
        let build = declaration["build"]
            .as_object_mut()
            .ok_or_else(|| Error("build object required".into()))?;
        build
            .entry("execution")
            .or_insert(json!(if mode == Mode::ManagedBuild {
                "managed"
            } else {
                "external"
            }));
        build.entry("inputs").or_insert(json!([]));
        build.entry("self_input").or_insert(json!(false));
    }
    for name in ["options", "target"] {
        if name == "target" && mode != Mode::Pull {
            continue;
        }
        if declaration.get(name).is_none() {
            let descriptor = registry
                .point(mode, name)
                .map_err(|e| Error(e.to_string()))?;
            if !descriptor.has_default {
                return Err(Error(format!("missing adapter point {name}")));
            }
            declaration
                .as_object_mut()
                .unwrap()
                .insert(name.into(), descriptor.default_value.clone());
        }
    }
    for table in declaration["tables"].as_array_mut().unwrap() {
        if mode == Mode::Pull && table.get("target").is_none() {
            let point = registry
                .point(mode, "table_target")
                .map_err(|e| Error(e.to_string()))?;
            if !point.has_default {
                return Err(Error("missing table target".into()));
            }
            table
                .as_object_mut()
                .unwrap()
                .insert("target".into(), point.default_value.clone());
        }
    }
    validate_points(declaration, registry)
}
pub fn validate_points(declaration: &Value, registry: &CheckedRegistry) -> Result<()> {
    let mode = mode(declaration)?;
    let validate = |point, value: &Value| {
        registry
            .validate_point(mode, point, value, ErrorCode::InvalidDeclaration)
            .map_err(|e| Error(e.to_string()))
    };
    validate("connection", &declaration["connection"])?;
    validate("options", &declaration["options"])?;
    if mode == Mode::Pull {
        validate("target", &declaration["target"])?;
    }
    for table in declaration["tables"].as_array().unwrap() {
        if mode == Mode::Pull {
            validate("table_target", &table["target"])?;
            if let Some(select) = table.get("select") {
                validate("table_select", select)?;
            }
        } else {
            validate("table_source", &table["source"])?;
            for column in table["columns"].as_array().unwrap() {
                if column.get("derive").is_none() {
                    validate(
                        "column_source",
                        column.get("source").unwrap_or(&column["name"]),
                    )?;
                }
            }
        }
    }
    Ok(())
}

/// Adapter defaults may add missing fragment fields, but every explicit value
/// and every parent-owned common field remains exact.
pub fn validate_effective(
    original: &Value,
    effective: &Value,
    registry: &CheckedRegistry,
) -> Result<()> {
    let mut common_original = original.clone();
    let mut common_effective = effective.clone();
    fn fragment(a: &mut Value, b: &mut Value, key: &str) -> Result<()> {
        let Some(value) = a.get(key) else {
            return Ok(());
        };
        let returned = b
            .get(key)
            .ok_or_else(|| Error("adapter removed explicit fragment".into()))?;
        fn extends(a: &Value, b: &Value) -> bool {
            match (a, b) {
                (Value::Object(a), Value::Object(b)) => a
                    .iter()
                    .all(|(key, value)| b.get(key).is_some_and(|other| extends(value, other))),
                _ => a == b,
            }
        }
        if !extends(value, returned) {
            return Err(Error("adapter changed explicit authoring value".into()));
        }
        a.as_object_mut().unwrap().remove(key);
        b.as_object_mut().unwrap().remove(key);
        Ok(())
    }
    for key in ["connection", "options", "target"] {
        fragment(&mut common_original, &mut common_effective, key)?;
    }
    let a = common_original["tables"]
        .as_array_mut()
        .ok_or_else(|| Error("invalid tables".into()))?;
    let b = common_effective["tables"]
        .as_array_mut()
        .ok_or_else(|| Error("invalid effective tables".into()))?;
    if a.len() != b.len() {
        return Err(Error("adapter changed table mappings".into()));
    }
    for (a, b) in a.iter_mut().zip(b) {
        for key in ["source", "target", "select"] {
            fragment(a, b, key)?;
        }
        if let (Some(a), Some(b)) = (
            a.get_mut("columns").and_then(Value::as_array_mut),
            b.get_mut("columns").and_then(Value::as_array_mut),
        ) {
            if a.len() != b.len() {
                return Err(Error("adapter changed output contract".into()));
            }
            for (a, b) in a.iter_mut().zip(b) {
                if a.get("source").is_none()
                    && a.get("derive").is_none()
                    && b.get("source") == a.get("name")
                {
                    b.as_object_mut().unwrap().remove("source");
                }
                fragment(a, b, "source")?;
            }
        }
    }
    if common_original != common_effective {
        return Err(Error(
            "adapter changed parent-normalized common fields".into(),
        ));
    }
    // Fixed pull identity uses canonical decimal strings, while the authoring
    // schema deliberately accepts integer revision selectors.
    let mut common = effective.clone();
    if common["kind"] == "pull"
        && let Some(selector) = common.get("revision").and_then(Value::as_str)
        && selector != "latest"
    {
        let number = selector
            .parse::<u64>()
            .map_err(|_| Error("invalid normalized revision".into()))?;
        if number.to_string() != selector {
            return Err(Error("noncanonical normalized revision".into()));
        }
        common["revision"] = json!(number);
    }
    validate_common(&common)?;
    validate_cross_fields(effective)?;
    validate_points(effective, registry)?;
    if serde_json::to_vec(effective)
        .map_err(|e| Error(e.to_string()))?
        .len()
        > DECLARATION_LIMIT
    {
        return Err(Error("expanded declaration exceeds64MiB".into()));
    }
    Ok(())
}

/// Reconstruct only defaults actually supplied by recorded pure validation.
/// Removing a former explicit mapping never inherits that mapping on replay.
pub fn replay_effective(
    original: &Value,
    validation_input: &Value,
    effective: &Value,
    registry: &CheckedRegistry,
) -> Result<Value> {
    validate_effective(validation_input, effective, registry)?;
    fn defaults(current: &mut Value, input: &Value, effective: &Value) {
        if let (Some(current), Some(input), Some(effective)) = (
            current.as_object_mut(),
            input.as_object(),
            effective.as_object(),
        ) {
            for (key, value) in effective {
                if let Some(before) = input.get(key) {
                    if let Some(current) = current.get_mut(key) {
                        defaults(current, before, value);
                    }
                } else {
                    current.entry(key.clone()).or_insert_with(|| value.clone());
                }
            }
        }
    }
    let mut result = original.clone();
    for key in ["connection", "options", "target"] {
        if let Some(current) = result.get_mut(key) {
            defaults(current, &validation_input[key], &effective[key]);
        }
    }
    let tables = result["tables"]
        .as_array_mut()
        .ok_or_else(|| Error("invalid replay tables".into()))?;
    let recorded = effective["tables"]
        .as_array()
        .ok_or_else(|| Error("invalid recorded tables".into()))?;
    let inputs = validation_input["tables"]
        .as_array()
        .ok_or_else(|| Error("invalid recorded validation input".into()))?;
    if tables.len() != recorded.len() || inputs.len() != recorded.len() {
        return Err(Error("changed replay table scope".into()));
    }
    for ((table, input), recorded) in tables.iter_mut().zip(inputs).zip(recorded) {
        if table["name"] != recorded["name"] {
            return Err(Error("changed replay table mapping".into()));
        }
        for key in ["target", "select"] {
            if let Some(current) = table.get_mut(key) {
                defaults(current, &input[key], &recorded[key]);
            }
        }
    }
    validate_effective(original, &result, registry)?;
    if result != *effective {
        return Err(Error(
            "caller differs from recorded effective request".into(),
        ));
    }
    Ok(result)
}

pub fn validate_cross_fields(declaration: &Value) -> Result<()> {
    let tables = declaration["tables"]
        .as_array()
        .ok_or_else(|| Error("table array required".into()))?;
    let mut names = BTreeSet::new();
    let mut destinations = BTreeSet::new();
    for table in tables {
        if !names.insert(
            table["name"]
                .as_str()
                .ok_or_else(|| Error("table name required".into()))?,
        ) {
            return Err(Error("duplicate table mapping".into()));
        }
        if let Some(target) = table.pointer("/target/table").and_then(Value::as_str)
            && !destinations.insert(target)
        {
            return Err(Error("duplicate physical destination".into()));
        }
        if let Some(columns) = table.get("columns").and_then(Value::as_array) {
            let mut fields = BTreeSet::new();
            for column in columns {
                if !fields.insert(
                    column["name"]
                        .as_str()
                        .ok_or_else(|| Error("column name required".into()))?,
                ) {
                    return Err(Error("duplicate output column".into()));
                }
                contract::authoring_type(
                    column["type"]
                        .as_str()
                        .ok_or_else(|| Error("column type required".into()))?,
                )?;
            }
            for key in table
                .get("partition_keys")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let name = format!("_{}_", key.as_str().unwrap());
                if !columns
                    .iter()
                    .any(|column| column["name"] == name && column["type"] == "utf8")
                {
                    return Err(Error("partition key requires its string column".into()));
                }
            }
            for column in columns {
                if let Some(derive) = column.get("derive") {
                    let from = derive["from"].as_str().unwrap();
                    let source = columns
                        .iter()
                        .find(|column| column["name"] == from)
                        .ok_or_else(|| Error("derived partition input does not exist".into()))?;
                    let ty = source["type"].as_str().unwrap();
                    if ty != "date32" && !ty.starts_with("timestamp(") {
                        return Err(Error(
                            "derived partition requires date or timestamp input".into(),
                        ));
                    }
                    if !table
                        .get("partition_keys")
                        .and_then(Value::as_array)
                        .is_some_and(|keys| {
                            keys.iter()
                                .any(|key| column["name"] == format!("_{}_", key.as_str().unwrap()))
                        })
                    {
                        return Err(Error("derive is allowed only for partition columns".into()));
                    }
                }
            }
        }
    }
    for check in declaration
        .get("checks")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let table = tables
            .iter()
            .find(|table| table["name"] == check["table"])
            .ok_or_else(|| Error("check references undeclared output table".into()))?;
        if let Some(columns) = table.get("columns").and_then(Value::as_array) {
            for name in check["not_null"].as_array().unwrap() {
                if !columns.iter().any(|column| column["name"] == *name) {
                    return Err(Error("check references undeclared output column".into()));
                }
            }
        }
    }
    Ok(())
}

pub fn table_contract(table: &Value, include_derived: bool) -> Result<TableContract> {
    let columns = table["columns"]
        .as_array()
        .ok_or_else(|| Error("resolved columns required".into()))?;
    let mut ext = table.get("column_ext").cloned().unwrap_or(json!({}));
    let columns: Result<Vec<Column>> = columns
        .iter()
        .filter(|column| include_derived || column.get("derive").is_none())
        .map(|column| {
            let name = column["name"].as_str().unwrap();
            if let Some(value) = column.get("ext") {
                let target = ext
                    .as_object_mut()
                    .unwrap()
                    .entry(name)
                    .or_insert(json!({}));
                for (key, value) in value.as_object().unwrap() {
                    if target.get(key).is_some_and(|existing| existing != value) {
                        return Err(Error("conflicting column extension".into()));
                    }
                    target
                        .as_object_mut()
                        .unwrap()
                        .insert(key.clone(), value.clone());
                }
            }
            Ok(Column {
                name: name.into(),
                logical_type: contract::authoring_type(column["type"].as_str().unwrap())?,
            })
        })
        .collect();
    let partition_keys = table
        .get("partition_keys")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|key| {
            include_derived
                || table["columns"].as_array().unwrap().iter().any(|column| {
                    column["name"] == format!("_{}_", key.as_str().unwrap())
                        && column.get("derive").is_none()
                })
        })
        .map(|key| Name::new(key.as_str().unwrap()).map_err(|e| Error(e.to_string())))
        .collect::<Result<_>>()?;
    let contract = TableContract {
        columns: columns?,
        partition_keys,
        extensions: table.get("extensions").cloned().unwrap_or(json!({})),
        column_ext: ext,
    };
    contract.validate().map_err(|e| Error(e.to_string()))?;
    Ok(contract)
}

#[cfg(test)]
mod tests {
    use super::*;
    use grv_adapter_api::{Capabilities, PointDescriptor};
    fn sample() -> Value {
        json!({"declaration_version":1,"kind":"push","dataset":"data","adapter":"fixture","connection":{"alias":"saved"},"tables":[{"name":"rows","source":{},"columns":[{"name":"value","type":"int64"}]}]})
    }
    fn registry() -> CheckedRegistry {
        let points = ["connection", "options", "table_source", "column_source"]
            .iter()
            .map(|name| PointDescriptor {
                point: (*name).into(),
                mode: Mode::Extract,
                schema_pointer: format!("/$defs/{name}"),
                has_default: *name == "options",
                default_value: if *name == "options" {
                    json!({})
                } else {
                    Value::Null
                },
            })
            .collect();
        CheckedRegistry::new(grv_adapter_api::Registry {
            schema_bundle: json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$defs":{
                "connection":{"type":"object","properties":{"alias":{"type":"string"},"nested":{"type":"boolean"}},"required":["alias"],"additionalProperties":false},
                "options":{"type":"object","properties":{"nested":{"type":"boolean","default":true}},"additionalProperties":false},
                "table_source":{"type":"object","additionalProperties":false},
                "column_source":{"type":"string"}
            }}), points,
        }, vec![], &Capabilities::default()).unwrap()
    }
    #[test]
    fn strict_yaml12_rejects_duplicates_aliases_tags_nonstring_keys_and_nonfinite() {
        assert_eq!(
            parse_yaml("yes: no\nflag: true\ndate: 2026-10-06\nnumber: 0o12\n").unwrap(),
            json!({"yes":"no","flag":true,"date":"2026-10-06","number":10})
        );
        for text in [
            "x: 1\nx: 2",
            "x: {a: 1, a: 2}",
            "x: &a yes\ny: *a",
            "x: !private foo",
            "x: !!str foo",
            "1: value",
            "x: .nan",
            "x: 1e999",
            "---\nx: 1\n---\ny: 2",
            "x: {<<: {a: 1}}",
        ] {
            assert!(parse_yaml(text).is_err(), "{text}");
        }
    }
    #[test]
    fn descriptor_defaults_and_parent_revalidation_preserve_explicit_values() {
        let registry = registry();
        let mut original = sample();
        apply_point_defaults(&mut original, &registry).unwrap();
        assert_eq!(original["options"], json!({})); // Schema default is an annotation.
        let mut effective = original.clone();
        effective["options"]["nested"] = json!(true);
        effective["connection"]["nested"] = json!(false);
        effective["tables"][0]["columns"][0]["source"] = json!("value");
        validate_effective(&original, &effective, &registry).unwrap();
        effective["dataset"] = json!("forged");
        assert!(validate_effective(&original, &effective, &registry).is_err());
        effective = original.clone();
        effective["connection"]["alias"] = json!("different");
        assert!(validate_effective(&original, &effective, &registry).is_err());
        effective = original.clone();
        effective["options"]["nested"] = json!("invalid");
        assert!(validate_effective(&original, &effective, &registry).is_err());
    }
    #[test]
    fn local_column_and_sql_references_expand_once_before_identity() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(
            temp.path().join("columns.yml"),
            "- {name: result, type: 'decimal128(38,6)'}\n",
        )
        .unwrap();
        std::fs::write(
            temp.path().join("query.sql"),
            "select amount as result from grv_source.rows\n",
        )
        .unwrap();
        let path = temp.path().join("decl.yml");
        std::fs::write(&path, "declaration_version: 1\nkind: pull\ndataset: data\nadapter: fixture\nconnection: {}\ntarget: {}\ntables:\n  - name: rows\n    columns: {file: columns.yml}\n    select: {file: query.sql}\n").unwrap();
        let loaded = load(&path).unwrap();
        assert_eq!(
            loaded["tables"][0]["columns"][0]["type"],
            "decimal128(38,6)"
        );
        assert!(
            loaded["tables"][0]["select"]["sql"]
                .as_str()
                .unwrap()
                .contains("grv_source")
        );
        std::fs::write(temp.path().join("columns.yml"), "{file: another.yml}").unwrap();
        assert!(load(&path).is_err());
    }
    #[test]
    fn duplicate_mappings_invalid_derive_and_checks_fail_in_core() {
        let mut value = sample();
        let table = value["tables"][0].clone();
        value["tables"].as_array_mut().unwrap().push(table);
        assert!(validate_cross_fields(&value).is_err());
        value = sample();
        value["checks"] = json!([{"table":"rows","not_null":["missing"]}]);
        assert!(validate_cross_fields(&value).is_err());
        value = sample();
        value["tables"][0]["columns"]
            .as_array_mut()
            .unwrap()
            .push(json!({"name":"_year_","type":"utf8","derive":{"from":"value","format":"year"}}));
        value["tables"][0]["partition_keys"] = json!(["year"]);
        assert!(validate_cross_fields(&value).is_err());
    }
    #[test]
    fn inline_and_ipc_schema_references_use_the_same_logical_mapping() {
        let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "value",
            arrow_schema::DataType::Decimal128(38, 6),
            false,
        )]);
        let generator = arrow_ipc::writer::IpcDataGenerator {};
        let mut bytes = Vec::new();
        let message = generator.schema_to_bytes_with_dictionary_tracker(
            &schema,
            &mut arrow_ipc::writer::DictionaryTracker::new(false),
            &arrow_ipc::writer::IpcWriteOptions::default(),
        );
        arrow_ipc::writer::write_message(
            &mut bytes,
            message,
            &arrow_ipc::writer::IpcWriteOptions::default(),
        )
        .unwrap();
        assert_eq!(
            ipc_columns(&bytes).unwrap(),
            json!([{"name":"value","type":"decimal128(38,6)"}])
        );
        bytes.extend_from_slice(&[255, 255, 255, 255, 0, 0, 0, 0]);
        assert!(ipc_columns(&bytes).is_err());
    }
}
