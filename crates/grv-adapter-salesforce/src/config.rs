use crate::{Result, invalid};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const DEFAULT_API_VERSION: &str = "v66.0";
pub const BINDING_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Connection {
    pub org: String,
    #[serde(default = "default_api_version")]
    pub api_version: String,
}
fn default_api_version() -> String {
    DEFAULT_API_VERSION.into()
}

impl Connection {
    pub fn validate(&self) -> Result<()> {
        if self.org.is_empty() || self.org.contains('\0') {
            return Err(invalid("org must be a nonempty alias or username"));
        }
        if !api_version_valid(&self.api_version) {
            return Err(invalid("api_version must have form v<major>.<minor>"));
        }
        Ok(())
    }
}

pub fn api_version_valid(value: &str) -> bool {
    let Some(rest) = value.strip_prefix('v') else {
        return false;
    };
    let Some((major, minor)) = rest.split_once('.') else {
        return false;
    };
    !major.is_empty()
        && !minor.is_empty()
        && major.bytes().all(|c| c.is_ascii_digit())
        && minor.bytes().all(|c| c.is_ascii_digit())
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Transport {
    #[default]
    Auto,
    Rest,
    Bulk,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Options {
    #[serde(default)]
    pub transport: Transport,
    #[serde(default)]
    pub all_rows: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub object: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
}

pub fn identifier(value: &str) -> bool {
    let mut chars = value.bytes();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == b'_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == b'_')
}
pub fn field_path(value: &str) -> bool {
    value.split('.').all(identifier)
}

impl Source {
    pub fn validate(&self) -> Result<()> {
        if !identifier(&self.object) {
            return Err(invalid("invalid object API name"));
        }
        if self.filter.as_ref().is_some_and(|f| f.trim().is_empty()) {
            return Err(invalid("filter must be nonempty"));
        }
        Ok(())
    }
}

/// Pure nested defaults. Common declaration fields and explicit alias/transport
/// authoring values are retained. Predicate parsing belongs to `PredicateCompiler`.
pub fn validate_binding(declaration: &Value) -> Result<Value> {
    let mut result = declaration.clone();
    let object = result
        .as_object_mut()
        .ok_or_else(|| invalid("declaration must be an object"))?;
    if object.get("adapter").and_then(Value::as_str) != Some("salesforce")
        || object.get("kind").and_then(Value::as_str) != Some("push")
        || object.contains_key("build")
    {
        return Err(crate::Error::new(
            "UNSUPPORTED_CAPABILITY",
            "Salesforce supports extraction bindings only",
        ));
    }
    let connection: Connection = serde_json::from_value(
        object
            .get("connection")
            .cloned()
            .ok_or_else(|| invalid("connection is required"))?,
    )
    .map_err(|_| invalid("invalid connection"))?;
    connection.validate()?;
    object.insert(
        "connection".into(),
        serde_json::to_value(connection).expect("serializable connection"),
    );
    let options: Options =
        serde_json::from_value(object.get("options").cloned().unwrap_or_else(|| json!({})))
            .map_err(|_| invalid("invalid options"))?;
    object.insert(
        "options".into(),
        serde_json::to_value(options).expect("serializable options"),
    );
    let tables = object
        .get_mut("tables")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| invalid("tables must be an array"))?;
    for table in tables {
        let source: Source = serde_json::from_value(
            table
                .get("source")
                .cloned()
                .ok_or_else(|| invalid("table source is required"))?,
        )
        .map_err(|_| invalid("invalid table source"))?;
        source.validate()?;
        if let Some(filter) = &source.filter {
            crate::predicate::SoqlPredicateCompiler.validate_row_predicate(filter)?;
        }
        let columns = table
            .get_mut("columns")
            .and_then(Value::as_array_mut)
            .ok_or_else(|| invalid("columns must be an array"))?;
        for column in columns {
            if column.get("derive").is_some() {
                continue;
            }
            if column.get("source").is_none() {
                column["source"] = column["name"].clone();
            }
            if !column
                .get("source")
                .and_then(Value::as_str)
                .is_some_and(field_path)
            {
                return Err(invalid("column source must be a field path"));
            }
        }
    }
    Ok(result)
}

/// Descriptor candidate for future extraction integration. This is not an
/// advertised registry; a handshake must expose only implemented lifecycles.
pub fn extraction_registry() -> grv_adapter_api::Registry {
    let bundle: Value = serde_json::from_str(include_str!(
        "../../../spec/adapters/salesforce.schema.json"
    ))
    .expect("checked-in Salesforce schema");
    let points: Vec<_> = [
        ("connection", "connection"),
        ("connection_details", "connection_details"),
        ("options", "options"),
        ("table_source", "extract_source"),
        ("column_source", "column_source"),
        ("source_identity", "source_identity"),
        ("source_job", "source_job"),
        ("push_result", "push_result"),
    ]
    .into_iter()
    .map(|(point, schema)| grv_adapter_api::PointDescriptor {
        point: point.into(),
        mode: grv_adapter_api::Mode::Extract,
        schema_pointer: format!("/$defs/{schema}"),
        default_value: if point == "options" {
            json!({})
        } else {
            Value::Null
        },
        has_default: point == "options",
    })
    .collect();
    grv_adapter_api::Registry {
        schema_bundle: bundle,
        points,
    }
}

/// A source-language parser must prove one row predicate and disallow relation
/// reads. No field or function denylist can substitute for that parser.
pub trait PredicateCompiler {
    fn validate_row_predicate(&mut self, expression: &str) -> Result<()>;
}

pub fn generate_query(
    source: &Source,
    fields: &[String],
    parser: &mut impl PredicateCompiler,
) -> Result<String> {
    source.validate()?;
    if fields.is_empty() || fields.iter().any(|f| !field_path(f)) {
        return Err(invalid("query needs valid field selectors"));
    }
    let mut query = format!("SELECT {} FROM {}", fields.join(", "), source.object);
    if let Some(filter) = &source.filter {
        parser.validate_row_predicate(filter)?;
        query.push_str(" WHERE ");
        query.push_str(filter);
    }
    Ok(query)
}

/// Eligibility must be proved by the decoder/type planner for the complete
/// contract, rather than selected through field-name or function-name bans.
#[derive(Debug, Clone, Copy)]
pub struct TransportEligibility {
    pub rest_lossless: bool,
    pub bulk_lossless: bool,
}
pub fn resolve_transport(
    preference: Transport,
    eligible: TransportEligibility,
) -> Result<Transport> {
    match preference {
        Transport::Auto | Transport::Rest if eligible.rest_lossless => Ok(Transport::Rest),
        Transport::Auto | Transport::Bulk if eligible.bulk_lossless => Ok(Transport::Bulk),
        _ => Err(crate::Error::new(
            "UNSUPPORTED_CAPABILITY",
            "no requested complete lossless transport",
        )),
    }
}
