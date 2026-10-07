//! Reopens only the fixed binding for an idempotent acknowledgement. Publication
//! and source acquisition are never entered by this path.
use grv_adapter_api::{AdapterDescriptor, AfterPublishRequest, Mode, Registry};
use grv_adapter_host::{
    discovery,
    process::{Deadlines, Session},
};
use grv_core::store::{Result, public_error};
use grv_types::{Digest, ErrorCode, RunId, Uuid};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum State {
    Pending,
    Complete,
}
pub(super) fn required_state<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<State>, D::Error> {
    Option::deserialize(d)
}

pub(super) struct Fixed<'a> {
    pub descriptor: &'a AdapterDescriptor,
    pub registry: &'a Registry,
    pub connection: &'a Value,
    pub canonical_connection: &'a Value,
    pub identity: &'a str,
    pub root: &'a str,
    pub workspace: Option<&'a Uuid>,
    pub run: Option<&'a RunId>,
    pub mode: Mode,
    pub attempt: &'a Uuid,
    pub declaration_sha256: &'a Digest,
    pub declaration_path: &'a Path,
}
pub(super) fn validate_state(required: bool, state: Option<State>) -> Result<()> {
    if required != state.is_some() {
        return Err(public_error(
            ErrorCode::IntegrityFailure,
            "terminal acknowledgement evidence differs from the fixed adapter",
        ));
    }
    Ok(())
}
pub(super) fn call(fixed: Fixed<'_>, result: &Value) -> Result<()> {
    let request = AfterPublishRequest {
        attempt_id: fixed.attempt.clone(),
        declaration_sha256: fixed.declaration_sha256.clone(),
        outcome: serde_json::from_value(result["outcome"].clone()).map_err(|_| {
            public_error(
                ErrorCode::IntegrityFailure,
                "fixed publication outcome is invalid",
            )
        })?,
    };
    request.validate().map_err(|_| {
        public_error(
            ErrorCode::IntegrityFailure,
            "fixed acknowledgement request is invalid",
        )
    })?;
    let invoke = || -> grv_adapter_host::Result<()> {
        let installations = discovery::discover(&discovery::search_roots(None)?)?;
        let installation = installations
            .iter()
            .find(|i| i.manifest.name == fixed.descriptor.name)
            .ok_or_else(|| {
                grv_adapter_host::Error::new(
                    ErrorCode::NotFound,
                    "fixed acknowledgement adapter unavailable",
                )
            })?;
        if installation.manifest.version != fixed.descriptor.package_version
            || installation.manifest.binding_schema_version
                != fixed.descriptor.binding_schema_version
            || !installation
                .manifest
                .interface_versions
                .contains(&fixed.descriptor.interface_version)
        {
            return Err(grv_adapter_host::Error::new(
                ErrorCode::RequestMismatch,
                "fixed acknowledgement adapter identity changed",
            ));
        }
        let mut process = Session::spawn_at(
            installation,
            Deadlines::default(),
            fixed.declaration_path.parent(),
        )?;
        if process.descriptor != *fixed.descriptor
            || process.registry.registry != *fixed.registry
            || !process.capabilities.after_publish
        {
            return Err(grv_adapter_host::Error::new(
                ErrorCode::RequestMismatch,
                "fixed acknowledgement adapter descriptor changed",
            ));
        }
        let locator =
            process.locate_connection(fixed.connection.clone(), fixed.mode, fixed.run.cloned())?;
        if locator.canonical_connection != *fixed.canonical_connection
            || locator
                .identity
                .as_deref()
                .is_some_and(|identity| identity != fixed.identity)
        {
            return Err(grv_adapter_host::Error::new(
                ErrorCode::RequestMismatch,
                "fixed acknowledgement connection changed",
            ));
        }
        let bound = process.bind_connection(
            locator,
            Some(fixed.root.into()),
            Some(fixed.identity.into()),
            fixed.workspace.cloned(),
            fixed.mode,
        )?;
        // The hook authenticates lazily if its private acknowledgement needs it.
        process.after_publish(bound.handle, request)?;
        // The established response is authoritative even if clean close fails.
        let _ = process.close();
        Ok(())
    };
    invoke().map_err(|error| error.public())
}
