//! CSV representation proofs from source descriptors. Formula fields use their
//! declared result type; nullable text is refused only because CSV cannot prove
//! whether an empty cell was null or an empty string. REST remains available.
use crate::{
    Result, acquisition::SourceHttp, auth::AuthenticatedSession, bulk::EmptyPolicy, integrity,
};
use grv_adapter_api::TableContract;
use serde_json::Value;
use std::collections::BTreeMap;

pub fn bulk_policies(
    http: &mut impl SourceHttp,
    session: &AuthenticatedSession,
    version: &str,
    object: &str,
    contract: &TableContract,
    selectors: &[String],
) -> Result<Vec<EmptyPolicy>> {
    projection(http, session, version, object, contract, selectors, true)
}

/// Prove source domains before querying, including empty/all-null captures.
/// REST preserves null versus empty text, unlike the Bulk CSV representation.
pub fn rest_projection(
    http: &mut impl SourceHttp,
    session: &AuthenticatedSession,
    version: &str,
    object: &str,
    contract: &TableContract,
    selectors: &[String],
) -> Result<()> {
    projection(http, session, version, object, contract, selectors, false).map(|_| ())
}

fn projection(
    http: &mut impl SourceHttp,
    session: &AuthenticatedSession,
    version: &str,
    object: &str,
    contract: &TableContract,
    selectors: &[String],
    bulk: bool,
) -> Result<Vec<EmptyPolicy>> {
    if selectors.len() != contract.columns.len() {
        return Err(integrity("metadata projection differs"));
    }
    let mut cache = BTreeMap::new();
    selectors
        .iter()
        .zip(&contract.columns)
        .map(|(selector, column)| {
            let parts: Vec<_> = selector.split('.').collect();
            if parts.len() > 32 {
                return Err(integrity(
                    "relationship projection exceeds metadata depth budget",
                ));
            }
            proof(
                http,
                session,
                version,
                object,
                &parts,
                &column.logical_type,
                false,
                &mut cache,
                bulk,
            )
            .map_err(|error| {
                crate::Error::new(
                    error.code,
                    format!("column {}: {}", column.name, error.message),
                )
            })
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn proof(
    http: &mut impl SourceHttp,
    session: &AuthenticatedSession,
    version: &str,
    object: &str,
    path: &[&str],
    logical: &Value,
    parent_nullable: bool,
    cache: &mut BTreeMap<String, Value>,
    bulk: bool,
) -> Result<EmptyPolicy> {
    let cache_key = object.to_ascii_lowercase();
    if !cache.contains_key(&cache_key) {
        if cache.len() >= 64 {
            return Err(integrity("metadata object cache exceeds resource budget"));
        }
        let description = http.describe(session, version, object)?;
        if !description
            .get("name")
            .and_then(Value::as_str)
            .is_some_and(|name| name.eq_ignore_ascii_case(object))
        {
            return Err(integrity("source descriptor object identity differs"));
        }
        let bytes = crate::source_json::heap_upper_bound(&description)?;
        let existing = cache.values().try_fold(0usize, |sum, value| {
            let size = crate::source_json::heap_upper_bound(value)?;
            sum.checked_add(size)
                .ok_or_else(|| integrity("metadata cache size overflow"))
        })?;
        if existing
            .checked_add(bytes)
            .is_none_or(|size| size > crate::acquisition::MAX_SOURCE_UNIT_BYTES)
        {
            return Err(integrity("metadata cache exceeds source budget"));
        }
        cache.insert(cache_key.clone(), description);
    }
    let fields = cache[&cache_key]
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| integrity("source descriptor lacks fields"))?;
    let mut matches = fields.iter().filter(|field| {
        field
            .get(if path.len() == 1 {
                "name"
            } else {
                "relationshipName"
            })
            .and_then(Value::as_str)
            .is_some_and(|name| name.eq_ignore_ascii_case(path[0]))
    });
    let Some(field) = matches.next() else {
        return Err(integrity("source field metadata is missing or ambiguous"));
    };
    if matches.next().is_some() {
        return Err(integrity("source field metadata is missing or ambiguous"));
    }
    let nullable = field
        .get("nillable")
        .and_then(Value::as_bool)
        .ok_or_else(|| integrity("source descriptor lacks nullability"))?
        || parent_nullable;
    let source_type = field
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| integrity("source descriptor lacks field type"))?;
    if path.len() > 1 {
        if source_type != "reference" {
            return Err(integrity("relationship descriptor is not a reference"));
        }
        let targets: Vec<String> = field
            .get("referenceTo")
            .and_then(Value::as_array)
            .filter(|targets| !targets.is_empty() && targets.len() <= 64)
            .ok_or_else(|| integrity("relationship descriptor lacks bounded target types"))?
            .iter()
            .map(|target| {
                target
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| integrity("invalid relationship target"))
            })
            .collect::<Result<_>>()?;
        let mut policies = Vec::new();
        for target in targets {
            if !crate::config::identifier(&target) {
                return Err(integrity("invalid relationship target"));
            }
            policies.push(proof(
                http,
                session,
                version,
                &target,
                &path[1..],
                logical,
                nullable,
                cache,
                bulk,
            )?);
        }
        if policies.iter().any(|policy| *policy != policies[0]) {
            return Err(integrity(
                "relationship types disagree on CSV representation",
            ));
        }
        return Ok(policies[0]);
    }
    let text = matches!(
        source_type,
        "string"
            | "textarea"
            | "picklist"
            | "multipicklist"
            | "combobox"
            | "email"
            | "phone"
            | "url"
            | "encryptedstring"
    );
    let identifier = matches!(source_type, "id" | "reference");
    let numeric_text =
        logical == "int64" || logical == "float64" || logical.get("decimal").is_some();
    let timestamp_text = logical.get("timestamp").is_some();
    let compatible = match source_type {
        "boolean" => logical == "boolean",
        "int" | "long" => logical == "int64",
        "double" if logical == "float64" => true,
        // Salesforce custom Number(p,0) is described as double, even though
        // its declared source domain contains only integral values. Up to
        // 18 decimal digits fit entirely within signed int64. Do not permit
        // fractional or unproved-width descriptors; row decoding still
        // rejects any value that is not an exact integer.
        "double" if logical == "int64" => {
            field.get("scale").and_then(Value::as_u64) == Some(0)
                && field
                    .get("precision")
                    .and_then(Value::as_u64)
                    .is_some_and(|p| (1..=18).contains(&p))
        }
        "double" | "currency" | "percent" => decimal_widening(field, logical)?,
        "date" => logical == "date",
        "datetime" => logical
            .get("timestamp")
            .is_some_and(|timestamp| timestamp.get("utc") == Some(&Value::Bool(true))),
        "base64" => logical == "binary",
        _ if text => logical == "string" || numeric_text || logical == "date" || timestamp_text,
        _ if identifier => logical == "string",
        _ => false,
    };
    if !compatible {
        return Err(crate::invalid(
            "source field type cannot widen exactly to the declared logical type",
        ));
    }
    if !bulk {
        return Ok(EmptyPolicy::Null);
    }
    if text || source_type == "base64" {
        if nullable {
            return Err(crate::Error::new(
                "UNSUPPORTED_CAPABILITY",
                "Bulk CSV cannot distinguish null and empty values for this source projection; select REST",
            ));
        }
        Ok(EmptyPolicy::String)
    } else if nullable {
        // Numeric/date/bool/ID domains have no valid empty lexical value.
        Ok(EmptyPolicy::Null)
    } else {
        Ok(EmptyPolicy::Reject)
    }
}

fn decimal_widening(field: &Value, logical: &Value) -> Result<bool> {
    let Some(target) = logical.get("decimal") else {
        return Ok(false);
    };
    grv_adapter_api::validate_logical_type(logical)
        .map_err(|_| crate::invalid("invalid declared decimal type"))?;
    let precision = field.get("precision").and_then(Value::as_u64);
    let scale = field.get("scale").and_then(Value::as_u64);
    let (Some(precision), Some(scale)) = (precision, scale) else {
        return Err(crate::Error::new(
            "UNSUPPORTED_CAPABILITY",
            "source descriptor lacks decimal precision/scale proof",
        ));
    };
    if precision == 0 || precision > 38 || scale > precision {
        return Err(crate::Error::new(
            "UNSUPPORTED_CAPABILITY",
            "source decimal width has no client-v1 representation proof",
        ));
    }
    let target_precision = target["precision"]
        .as_u64()
        .expect("validated logical decimal");
    let target_scale = target["scale"].as_u64().expect("validated logical decimal");
    Ok(target_scale >= scale && target_precision - target_scale >= precision - scale)
}
