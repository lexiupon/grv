//! A committed publication and its pending acknowledgement survive hook death.
use grv_adapter_api::{AfterPublishRequest, Mode, Outcome, OutcomeKind};
use grv_adapter_host::validate_output;
use grv_adapter_host::{
    discovery,
    process::{Deadlines, Session},
};
use grv_types::Uuid;
use serde_json::Value;
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};

struct Harness {
    _temp: tempfile::TempDir,
    adapters: PathBuf,
    hooks: PathBuf,
    root: PathBuf,
    state: PathBuf,
    declaration: PathBuf,
}
fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}
impl Harness {
    fn new() -> Self {
        let temp = tempfile::tempdir_in(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .parent()
                .unwrap(),
        )
        .unwrap();
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let adapters = temp.path().join("adapters");
        let package = adapters.join("fixture");
        let hooks = temp.path().join("hooks");
        fs::create_dir_all(&package).unwrap();
        fs::create_dir(&hooks).unwrap();
        let executable = package.join("adapter");
        fs::write(
            &executable,
            format!(
                "#!/bin/sh\nexec {} --after-publish-dir {}\n",
                quote(env!("CARGO_BIN_EXE_fixture")),
                quote(hooks.to_str().unwrap())
            ),
        )
        .unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
        Self::manifest(&package, "0.1.0");
        let root = temp.path().join("grv");
        let state = temp.path().join("state");
        let declaration = temp.path().join("push.yml");
        fs::write(&declaration, "declaration_version: 1\nkind: push\ndataset: data\nadapter: fixture\nconnection: {}\ntables:\n  - name: rows\n    source: {}\n    columns:\n      - {name: value, source: value, type: int64}\n").unwrap();
        let result = Self {
            _temp: temp,
            adapters,
            hooks,
            root,
            state,
            declaration,
        };
        assert_eq!(
            result.cli(&["init", "--grv", result.root.to_str().unwrap()])["ok"],
            true
        );
        result
    }
    fn manifest(package: &Path, version: &str) {
        fs::write(package.join("adapter.toml"), format!("name = 'fixture'\nversion = {version:?}\ninterface_versions = [1]\nbinding_schema_version = 1\nentrypoint = 'adapter'\n")).unwrap();
    }
    fn cli(&self, args: &[&str]) -> Value {
        let output = Command::new(env!("CARGO_BIN_EXE_grv-conformance-cli"))
            .arg("--json")
            .args(args)
            .env("GRV_ADAPTERS_DIR", &self.adapters)
            .output()
            .unwrap();
        let value: Value = serde_json::from_slice(&output.stdout)
            .unwrap_or_else(|_| panic!("no CLI JSON; exit {:?}", output.status.code()));
        validate_output(&value).unwrap();
        assert_eq!(output.status.success(), value["ok"] == true);
        value
    }
    fn push(&self, attempt: &Uuid) -> Value {
        self.cli(&[
            "push",
            "--grv",
            self.root.to_str().unwrap(),
            "--decl",
            self.declaration.to_str().unwrap(),
            "--state",
            self.state.to_str().unwrap(),
            "--attempt",
            attempt.as_str(),
        ])
    }
    fn marker(&self, name: &str) -> usize {
        fs::read_to_string(self.hooks.join(name))
            .unwrap_or_default()
            .lines()
            .count()
    }
    fn journal(&self, attempt: &Uuid) -> Value {
        serde_json::from_slice(
            &fs::read(
                self.state
                    .join("push")
                    .join(attempt.as_str())
                    .join("journal.json"),
            )
            .unwrap(),
        )
        .unwrap()
    }
    fn hook_state(&self, attempt: &Uuid) -> Value {
        self.journal(attempt)["evidence"]["progress"]["after_publish"].clone()
    }
    fn remove_sources(&self) {
        fs::remove_dir_all(&self.root).unwrap();
        fs::remove_file(&self.declaration).unwrap();
    }
    fn session(&self) -> Session {
        let installation = discovery::discover(&[discovery::SearchRoot {
            path: self.adapters.clone(),
            kind: discovery::RootKind::User,
        }])
        .unwrap()
        .remove(0);
        Session::spawn(&installation, Deadlines::default()).unwrap()
    }
}

#[test]
fn offline_unknown_identity_defers_comparison_until_authentication() {
    let h = Harness::new();
    fs::write(h.hooks.join("unknown_offline_identity"), "").unwrap();
    for expected_auth in ["fixture-source", "wrong-source"] {
        let mut session = h.session();
        let locator = session
            .locate_connection(serde_json::json!({}), Mode::Extract, None)
            .unwrap();
        assert_eq!(locator.identity, None);
        let bound = session
            .bind_connection(
                locator,
                None,
                Some("fixture-source".into()),
                None,
                Mode::Extract,
            )
            .unwrap();
        assert_eq!(bound.identity, None);
        let authenticated = session.authenticate(bound.handle, Some(expected_auth.into()));
        if expected_auth == "fixture-source" {
            assert_eq!(authenticated.unwrap(), expected_auth);
        } else {
            assert_eq!(
                authenticated.unwrap_err().code,
                grv_types::ErrorCode::RequestMismatch
            );
        }
        session.close().unwrap();
    }
    fs::remove_file(h.hooks.join("unknown_offline_identity")).unwrap();
    let mut session = h.session();
    let locator = session
        .locate_connection(serde_json::json!({}), Mode::Extract, None)
        .unwrap();
    assert_eq!(
        session
            .bind_connection(
                locator,
                None,
                Some("wrong-source".into()),
                None,
                Mode::Extract
            )
            .err()
            .unwrap()
            .code,
        grv_types::ErrorCode::RequestMismatch
    );
    session.close().unwrap();
}

#[test]
fn pull_binding_cannot_defer_its_required_offline_identity() {
    let h = Harness::new();
    fs::write(h.hooks.join("unknown_offline_identity"), "").unwrap();
    let mut session = h.session();
    let locator = grv_adapter_api::ConnectionLocator {
        canonical_connection: serde_json::json!({}),
        identity: None,
        engine_path: None,
        session_lock_path: None,
    };
    assert_eq!(
        session
            .bind_connection(
                locator,
                None,
                Some("fixture-source".into()),
                None,
                Mode::Pull
            )
            .err()
            .unwrap()
            .code,
        grv_types::ErrorCode::ProtocolFailure
    );
    session.close().unwrap();
}

#[test]
fn aborted_hook_can_release_private_state_without_advancing_cursor() {
    let h = Harness::new();
    let attempt = Uuid::v4();
    let digest = grv_types::sha256(b"fixed");
    fs::write(
        h.hooks.join(format!("{attempt}.capture.json")),
        grv_types::canonical_json(
            &serde_json::json!({"attempt_id":attempt,"declaration_sha256":digest}),
        )
        .unwrap(),
    )
    .unwrap();
    let mut session = h.session();
    let locator = session
        .locate_connection(serde_json::json!({}), Mode::Extract, None)
        .unwrap();
    let bound = session
        .bind_connection(
            locator,
            None,
            Some("fixture-source".into()),
            None,
            Mode::Extract,
        )
        .unwrap();
    session
        .after_publish(
            bound.handle,
            AfterPublishRequest {
                attempt_id: attempt.clone(),
                declaration_sha256: digest,
                outcome: Outcome {
                    kind: OutcomeKind::Aborted,
                    revision: None,
                    operation_id: None,
                },
            },
        )
        .unwrap();
    session.close().unwrap();
    assert!(h.hooks.join(format!("{attempt}.complete.json")).exists());
    assert!(!h.hooks.join(format!("{attempt}.cursor.json")).exists());
    assert_eq!(h.marker("extractions"), 0);
}

#[test]
fn published_hook_failure_retries_only_fixed_acknowledgement_then_replays_offline() {
    let h = Harness::new();
    let attempt = Uuid::v4();
    fs::write(h.hooks.join("fail"), "").unwrap();
    let failed = h.push(&attempt);
    assert_eq!(failed["ok"], false, "{failed}");
    assert_eq!(failed["errors"][0]["code"], "ADAPTER_FAILURE", "{failed}");
    assert_eq!(failed["result"]["outcome"]["kind"], "published", "{failed}");
    assert_eq!(h.hook_state(&attempt), "pending");
    assert_eq!(h.marker("extractions"), 1);
    let authentications = h.marker("authentications");
    h.remove_sources();
    fs::remove_file(h.hooks.join("fail")).unwrap();
    fs::write(h.hooks.join("require_auth"), "").unwrap();
    let retried = h.push(&attempt);
    assert_eq!(retried["ok"], true, "{retried}");
    assert_eq!(retried["result"]["outcome"], failed["result"]["outcome"]);
    assert_eq!(h.hook_state(&attempt), "complete");
    assert_eq!(h.marker("extractions"), 1);
    assert_eq!(h.marker("authentications"), authentications);
    assert_eq!(h.marker("hooks"), 2);
    assert_eq!(h.marker("hook_authentications"), 1);
    fs::remove_dir_all(h.adapters.join("fixture")).unwrap();
    let offline = h.push(&attempt);
    assert_eq!(offline["ok"], true, "{offline}");
    assert_eq!(offline["result"]["outcome"], failed["result"]["outcome"]);
    assert_eq!(h.marker("hooks"), 2);
}

#[test]
fn hook_death_before_and_after_private_completion_keeps_publication_known() {
    for control in ["crash_once", "crash_after_completion_once"] {
        let h = Harness::new();
        let attempt = Uuid::v4();
        fs::write(h.hooks.join(control), "").unwrap();
        let failed = h.push(&attempt);
        assert_eq!(failed["ok"], false, "{control}: {failed}");
        assert_eq!(
            failed["errors"][0]["code"], "ADAPTER_FAILURE",
            "{control}: {failed}"
        );
        assert_eq!(failed["result"]["outcome"]["kind"], "published", "{failed}");
        assert_eq!(h.hook_state(&attempt), "pending");
        assert_eq!(
            h.hooks.join(format!("{attempt}.complete.json")).exists(),
            control == "crash_after_completion_once"
        );
        h.remove_sources();
        let retried = h.push(&attempt);
        assert_eq!(retried["ok"], true, "{retried}");
        assert_eq!(retried["result"]["outcome"], failed["result"]["outcome"]);
        assert_eq!(h.hook_state(&attempt), "complete");
        assert_eq!(h.marker("extractions"), 1);
    }
}

#[test]
fn no_op_hook_and_changed_adapter_refusal_preserve_fixed_pending_result() {
    let h = Harness::new();
    let first = Uuid::v4();
    assert_eq!(h.push(&first)["ok"], true);
    let attempt = Uuid::v4();
    fs::write(h.hooks.join("fail"), "").unwrap();
    let failed = h.push(&attempt);
    assert_eq!(failed["ok"], false, "{failed}");
    assert_eq!(failed["result"]["outcome"]["kind"], "no-op");
    assert_eq!(h.hook_state(&attempt), "pending");
    let package = h.adapters.join("fixture");
    Harness::manifest(&package, "0.2.0");
    let calls = h.marker("hooks");
    h.remove_sources();
    let changed = h.push(&attempt);
    assert_eq!(
        changed["errors"][0]["code"], "REQUEST_MISMATCH",
        "{changed}"
    );
    assert_eq!(changed["result"]["outcome"], failed["result"]["outcome"]);
    assert_eq!(h.hook_state(&attempt), "pending");
    assert_eq!(h.marker("hooks"), calls);
    Harness::manifest(&package, "0.1.0");
    fs::remove_file(h.hooks.join("fail")).unwrap();
    let retried = h.push(&attempt);
    assert_eq!(retried["ok"], true, "{retried}");
    assert_eq!(retried["result"]["outcome"], failed["result"]["outcome"]);
    assert!(h.hooks.join(format!("{attempt}.cursor.json")).exists());
    assert_eq!(h.marker("extractions"), 2);
}
