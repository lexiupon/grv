use crate::{Error, Result};
use grv_adapter_api::{Capabilities, CommandDescriptor, Mode, Registry};
use grv_types::ErrorCode;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub struct CheckedRegistry {
    pub registry: Registry,
    pub commands: Vec<CommandDescriptor>,
    validators: BTreeMap<String, jsonschema::Validator>,
    bundle_bytes: usize,
    compiled_bytes: usize,
}
impl CheckedRegistry {
    /// Recorded schemas validate terminal evidence independently of today's
    /// executable capabilities; all offline metaschema and resource checks apply.
    pub fn recorded(registry: Registry) -> Result<Self> {
        Self::new(registry, vec![], &Capabilities::default())
    }
    pub fn new(
        registry: Registry,
        commands: Vec<CommandDescriptor>,
        caps: &Capabilities,
    ) -> Result<Self> {
        caps.validate()
            .map_err(|e| Error::new(ErrorCode::UnsupportedCapability, e.to_string()))?;
        if registry.schema_bundle["$schema"] != "https://json-schema.org/draft/2020-12/schema" {
            return Err(bad("registry must use JSON Schema 2020-12"));
        }
        check_schema(&registry.schema_bundle, 0, &mut 0)?;
        check_reference_budget(
            &registry.schema_bundle,
            &registry.schema_bundle,
            0,
            &mut 0,
            &mut BTreeSet::new(),
        )?;
        jsonschema::draft202012::meta::validate(&registry.schema_bundle)
            .map_err(|_| bad("invalid registry schema"))?;
        let bundle_bytes = serde_json::to_vec(&registry.schema_bundle)
            .map_err(|_| bad("registry encoding failed"))?
            .len();
        let mut out = Self {
            bundle_bytes,
            compiled_bytes: 0,
            registry,
            commands,
            validators: BTreeMap::new(),
        };
        let mut points = BTreeSet::new();
        let allowed = [
            "connection",
            "options",
            "table_source",
            "target",
            "table_target",
            "table_select",
            "column_source",
            "build_input",
            "connection_details",
            "pull_plan",
            "pull_result",
            "push_result",
            "inspection_result",
            "session_details",
            "source_identity",
            "source_job",
        ];
        let point_descriptors = out.registry.points.clone();
        for p in point_descriptors {
            if !allowed.contains(&p.point.as_str())
                || !points.insert((p.point.clone(), format!("{:?}", p.mode)))
                || (!p.has_default && !p.default_value.is_null())
            {
                return Err(bad("invalid point descriptor"));
            }
            out.compile(&p.schema_pointer)?;
            if p.has_default {
                out.validate(
                    &p.schema_pointer,
                    &p.default_value,
                    ErrorCode::AdapterFailure,
                )?;
            }
        }
        let mut names = BTreeSet::new();
        for c in out.commands.clone() {
            // Authentication implies a connection, so a login command that
            // needs neither is exactly one that does not need a connection.
            if !names.insert(c.name.clone())
                || (c.requires_authentication && !c.requires_connection)
                || (c.name.as_str() == "login" && c.requires_connection)
            {
                return Err(bad("invalid command descriptor"));
            }
            out.compile(&c.args_schema_pointer)?;
            out.compile(&c.result_schema_pointer)?;
            if c.requires_connection {
                out.require(Mode::Command, "connection")?;
                out.require(Mode::Command, "connection_details")?;
            }
        }
        for (enabled, mode, required) in [
            (
                caps.push,
                Mode::Extract,
                vec![
                    "options",
                    "table_source",
                    "column_source",
                    "source_identity",
                    "source_job",
                    "push_result",
                ],
            ),
            (
                caps.managed_build,
                Mode::ManagedBuild,
                vec![
                    "options",
                    "table_source",
                    "column_source",
                    "build_input",
                    "session_details",
                    "push_result",
                ],
            ),
            (
                caps.external_build,
                Mode::ExternalBuild,
                vec![
                    "options",
                    "table_source",
                    "column_source",
                    "build_input",
                    "session_details",
                    "push_result",
                ],
            ),
            (
                caps.pull,
                Mode::Pull,
                vec![
                    "options",
                    "target",
                    "table_target",
                    "table_select",
                    "pull_plan",
                    "pull_result",
                ],
            ),
            (
                caps.inspect_connection,
                Mode::Inspect,
                vec!["inspection_result"],
            ),
        ] {
            if enabled {
                out.require(mode, "connection")?;
                out.require(mode, "connection_details")?;
                for point in required {
                    out.require(mode, point)?;
                }
            }
        }
        Ok(out)
    }
    fn compile(&mut self, pointer: &str) -> Result<()> {
        if !pointer.starts_with('/') || self.registry.schema_bundle.pointer(pointer).is_none() {
            return Err(bad("unresolved schema pointer"));
        }
        if self.validators.contains_key(pointer) {
            return Ok(());
        }
        self.compiled_bytes = self
            .compiled_bytes
            .checked_add(self.bundle_bytes)
            .filter(|total| *total <= 64 * 1024 * 1024)
            .ok_or_else(|| bad("registry compilation budget exceeded"))?;
        let mut bundle = self.registry.schema_bundle.clone();
        fn rewrite(v: &mut Value) {
            if let Value::Object(object) = v {
                if let Some(Value::String(reference)) = object.get_mut("$ref") {
                    *reference =
                        format!("#/$defs/grv_bundle{}", reference.strip_prefix('#').unwrap());
                }
                for name in SCHEMA_SINGLE {
                    if let Some(child) = object.get_mut(*name) {
                        rewrite(child);
                    }
                }
                for name in SCHEMA_MAPS {
                    if let Some(map) = object.get_mut(*name).and_then(Value::as_object_mut) {
                        for child in map.values_mut() {
                            rewrite(child);
                        }
                    }
                }
                for name in SCHEMA_ARRAYS {
                    if let Some(array) = object.get_mut(*name).and_then(Value::as_array_mut) {
                        for child in array {
                            rewrite(child);
                        }
                    }
                }
            }
        }
        rewrite(&mut bundle);
        let selected = json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$defs":{"grv_bundle":bundle},"$ref":format!("#/$defs/grv_bundle{pointer}")});
        let validator = jsonschema::options()
            .with_draft(jsonschema::Draft::Draft202012)
            .should_validate_formats(true)
            .with_pattern_options(
                jsonschema::PatternOptions::fancy_regex()
                    .backtrack_limit(20_000)
                    .size_limit(1024 * 1024)
                    .dfa_size_limit(1024 * 1024),
            )
            .build(&selected)
            .map_err(|_| bad("registry schema compilation failed"))?;
        self.validators.insert(pointer.into(), validator);
        Ok(())
    }
    fn require(&self, mode: Mode, point: &str) -> Result<()> {
        if self
            .registry
            .points
            .iter()
            .any(|p| p.mode == mode && p.point == point)
        {
            Ok(())
        } else {
            Err(bad("missing implemented-mode point descriptor"))
        }
    }
    pub fn validate(&self, pointer: &str, v: &Value, code: ErrorCode) -> Result<()> {
        let validator = self
            .validators
            .get(pointer)
            .ok_or_else(|| bad("unknown schema pointer"))?;
        if validator.is_valid(v) {
            Ok(())
        } else {
            Err(Error::new(
                code,
                "adapter value does not satisfy registered schema",
            ))
        }
    }
    pub fn point(&self, mode: Mode, name: &str) -> Result<&grv_adapter_api::PointDescriptor> {
        self.registry
            .points
            .iter()
            .find(|p| p.mode == mode && p.point == name)
            .ok_or_else(|| bad("missing validation point"))
    }
    pub fn validate_point(&self, mode: Mode, name: &str, v: &Value, code: ErrorCode) -> Result<()> {
        self.validate(&self.point(mode, name)?.schema_pointer, v, code)
    }
}
fn bad(message: &str) -> Error {
    Error::new(ErrorCode::AdapterFailure, message)
}
fn check_schema(v: &Value, depth: usize, nodes: &mut usize) -> Result<()> {
    *nodes += 1;
    if depth > 64 || *nodes > 100_000 {
        return Err(bad("registry schema complexity limit exceeded"));
    }
    match v {
        Value::Object(o) => {
            for v in o.values() {
                check_schema(v, depth + 1, nodes)?;
            }
        }
        Value::Array(a) => {
            for v in a {
                check_schema(v, depth + 1, nodes)?
            }
        }
        _ => {}
    }
    Ok(())
}

const SCHEMA_SINGLE: &[&str] = &[
    "additionalProperties",
    "unevaluatedProperties",
    "propertyNames",
    "items",
    "contains",
    "unevaluatedItems",
    "not",
    "if",
    "then",
    "else",
    "contentSchema",
];
const SCHEMA_MAPS: &[&str] = &[
    "$defs",
    "definitions",
    "properties",
    "patternProperties",
    "dependentSchemas",
];
const SCHEMA_ARRAYS: &[&str] = &["prefixItems", "allOf", "anyOf", "oneOf"];
fn schema_children(value: &Value) -> Vec<&Value> {
    let mut children = Vec::new();
    for name in SCHEMA_SINGLE {
        if let Some(child) = value.get(*name) {
            children.push(child);
        }
    }
    for name in SCHEMA_MAPS {
        if let Some(map) = value.get(*name).and_then(Value::as_object) {
            children.extend(map.values());
        }
    }
    for name in SCHEMA_ARRAYS {
        if let Some(array) = value.get(*name).and_then(Value::as_array) {
            children.extend(array);
        }
    }
    children
}

// Bound work before the schema compiler follows references. Counting the
// expanded graph prevents a small bundle from causing exponential expansion.
fn check_reference_budget(
    bundle: &Value,
    value: &Value,
    depth: usize,
    nodes: &mut usize,
    active: &mut BTreeSet<String>,
) -> Result<()> {
    *nodes += 1;
    if depth > 64 || *nodes > 100_000 {
        return Err(bad("registry reference complexity limit exceeded"));
    }
    if let Value::Object(object) = value {
        if ["$dynamicRef", "$recursiveRef", "$id"]
            .iter()
            .any(|key| object.contains_key(*key))
        {
            return Err(bad("registry cannot change reference scope"));
        }
        if let Some(reference) = object.get("$ref") {
            let reference = reference
                .as_str()
                .filter(|r| *r == "#" || r.starts_with("#/"))
                .ok_or_else(|| bad("registry references must be local JSON pointers"))?;
            let pointer = reference.strip_prefix('#').unwrap();
            let target = bundle
                .pointer(pointer)
                .ok_or_else(|| bad("unresolved registry reference"))?;
            if !active.insert(reference.into()) {
                return Err(bad("registry reference complexity limit exceeded"));
            }
            check_reference_budget(bundle, target, depth + 1, nodes, active)?;
            active.remove(reference);
        }
        for child in schema_children(value) {
            check_reference_budget(bundle, child, depth + 1, nodes, active)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn schema_references_cannot_escape_or_expand_without_bound() {
        for schema in [
            json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$ref":"https://example.invalid/schema"}),
            json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$defs":{"loop":{"$ref":"#/$defs/loop"}}}),
            json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$ref":"#/$defs/missing"}),
        ] {
            assert!(
                CheckedRegistry::new(
                    Registry {
                        schema_bundle: schema,
                        points: vec![]
                    },
                    vec![],
                    &Capabilities::default()
                )
                .is_err()
            );
        }
    }
    #[test]
    fn business_property_names_and_default_annotations_are_not_schema_references() {
        let registry = Registry {
            schema_bundle: json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$defs":{
                "args":{"type":"object","properties":{"$ref":{"type":"string"},"$id":{"type":"string"}},"required":["$ref"],"additionalProperties":false,"default":{"$ref":"business-value"}},
                "result":{"type":"object","additionalProperties":false}
            }}),
            points: vec![],
        };
        let checked = CheckedRegistry::new(
            registry,
            vec![CommandDescriptor {
                name: grv_types::Name::new("test").unwrap(),
                requires_connection: false,
                requires_authentication: false,
                args_schema_pointer: "/$defs/args".into(),
                result_schema_pointer: "/$defs/result".into(),
            }],
            &Capabilities::default(),
        )
        .unwrap();
        checked
            .validate(
                "/$defs/args",
                &json!({"$ref":"business-value","$id":"coordinate"}),
                ErrorCode::InvalidArgument,
            )
            .unwrap();
        assert_eq!(
            checked.registry.schema_bundle["$defs"]["args"]["default"]["$ref"],
            "business-value"
        );
    }
}
