use grv_adapter_api::*;
use grv_adapter_host::{
    discovery::{self, Installation, RootKind, SearchRoot},
    install,
    process::{Deadlines, Session},
    validate_output,
};
use grv_types::{CommandOutput, ErrorCode};
use serde_json::json;
use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
fn protected() -> tempfile::TempDir {
    tempfile::tempdir_in(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap(),
    )
    .unwrap()
}
fn root(path: &Path) -> SearchRoot {
    SearchRoot {
        path: path.into(),
        kind: RootKind::User,
    }
}
fn package(dir: &Path, name: &str, executable: &Path, version: &str) {
    std::fs::create_dir_all(dir.join(name)).unwrap();
    std::fs::write(dir.join(name).join("adapter.toml"),format!("name = {name:?}\nversion = {version:?}\ninterface_versions = [1]\nbinding_schema_version = 1\nentrypoint = {:?}\n",executable.to_str().unwrap())).unwrap();
}
fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_fixture"))
}
fn cli(root: &Path, args: &[&str]) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_grv-conformance-cli"))
        .arg("--json")
        .args(args)
        .env("GRV_ADAPTERS_DIR", root)
        .env("AWS_ACCESS_KEY_ID", "credential-canary-environment")
        .env("AWS_SECRET_ACCESS_KEY", "credential-canary-environment")
        .env(
            "GOOGLE_APPLICATION_CREDENTIALS",
            "credential-canary-environment",
        )
        .env("GRV_ROOT", "credential-canary-environment")
        .env("GRV_STATE", "credential-canary-environment")
        .output()
        .unwrap()
}
fn cli_value(output: &std::process::Output) -> serde_json::Value {
    let value = serde_json::from_slice(&output.stdout).unwrap();
    validate_output(&value).unwrap();
    for bytes in [&output.stdout, &output.stderr] {
        assert!(!String::from_utf8_lossy(bytes).contains("credential-canary"));
    }
    value
}
#[test]
fn runnable_cli_list_capabilities_echo_slice_and_environment_isolation() {
    let temp = protected();
    package(temp.path(), "fixture", &fixture(), "0.1.0");
    for args in [
        vec!["adapter", "list"],
        vec!["adapter", "fixture", "capabilities"],
        vec!["adapter", "fixture", "echo", "--message", "hello"],
    ] {
        let output = cli(temp.path(), &args);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let value = cli_value(&output);
        assert!(value["root"].is_null());
        if args.last() == Some(&"hello") {
            assert_eq!(value["result"]["details"]["message"], "hello");
        }
    }
    let (temp, _) = adversary("env");
    let output = cli(
        temp.path(),
        &["adapter", "adversary", "echo", "--message", "probe"],
    );
    assert!(output.status.success());
    assert_eq!(cli_value(&output)["result"]["details"]["message"], "clean");
}
#[test]
fn cli_preserves_validated_result_when_close_fails() {
    let (temp, _) = adversary("abnormal");
    let output = cli(
        temp.path(),
        &["adapter", "adversary", "echo", "--message", "completed"],
    );
    assert!(!output.status.success());
    let value = cli_value(&output);
    assert_eq!(value["result"]["details"]["message"], "completed");
    assert_eq!(value["errors"][0]["code"], "ADAPTER_FAILURE");
}
#[test]
fn bounded_documents_support_large_command_metadata() {
    let temp = protected();
    package(temp.path(), "fixture", &fixture(), "0.1.0");
    let i = discovery::discover(&[root(temp.path())]).unwrap().remove(0);
    let mut s = Session::spawn(
        &i,
        Deadlines {
            response: Duration::from_secs(30),
            ..short()
        },
    )
    .unwrap();
    let message = "x".repeat(1024 * 1024 + 128);
    let result = s
        .command(
            Name::new("echo").unwrap(),
            vec!["--message".into(), message.clone()],
        )
        .unwrap();
    assert_eq!(result.details["message"], message);
    s.close().unwrap();
}
fn adversary(scenario: &str) -> (tempfile::TempDir, Installation) {
    let temp = protected();
    package(
        temp.path(),
        "adversary",
        Path::new(env!("CARGO_BIN_EXE_adversary")),
        "0.1.0",
    );
    std::fs::write(temp.path().join("adversary/scenario"), scenario).unwrap();
    let installation = discovery::discover(&[root(temp.path())]).unwrap().remove(0);
    (temp, installation)
}
fn short() -> Deadlines {
    Deadlines {
        bootstrap: Duration::from_secs(3),
        response: Duration::from_secs(3),
        grace: Duration::from_millis(150),
    }
}
#[test]
fn manifest_only_listing_no_child_and_deterministic_shadowing() {
    let temp = protected();
    let user = temp.path().join("user");
    let system = temp.path().join("system");
    let script = temp.path().join("must-not-run");
    std::fs::write(
        &script,
        format!("#!/bin/sh\ntouch {:?}\n", temp.path().join("spawned")),
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    package(&user, "fixture", &script, "user");
    package(&system, "fixture", &script, "system");
    let roots = [root(&user), root(&system)];
    let listed = discovery::list(&roots).unwrap();
    assert_eq!(listed.adapters.len(), 1);
    assert_eq!(listed.adapters[0].package_version, "user");
    assert_eq!(listed.search_roots.len(), 2);
    assert!(!temp.path().join("spawned").exists());
    validate_output(&serde_json::to_value(CommandOutput::success("adapter list", listed)).unwrap())
        .unwrap();
}
#[test]
fn manifest_and_handshake_identity_must_agree() {
    let temp = protected();
    package(temp.path(), "fixture", &fixture(), "wrong");
    let i = discovery::discover(&[root(temp.path())]).unwrap().remove(0);
    assert_eq!(
        Session::spawn(&i, short()).err().unwrap().code,
        ErrorCode::AdapterFailure
    );
}
#[test]
fn untrusted_manifest_executable_and_ancestors_are_rejected() {
    let temp = protected();
    package(temp.path(), "fixture", &fixture(), "0.1.0");
    let path = temp.path().join("fixture/adapter.toml");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o666)).unwrap();
    assert!(discovery::discover(&[root(temp.path())]).is_err());
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    std::fs::set_permissions(
        temp.path().join("fixture"),
        std::fs::Permissions::from_mode(0o777),
    )
    .unwrap();
    assert!(discovery::discover(&[root(temp.path())]).is_err());
}
#[test]
fn fixture_capabilities_and_pure_prepare_validate_execute_close() {
    let temp = protected();
    package(temp.path(), "fixture", &fixture(), "0.1.0");
    let i = discovery::discover(&[root(temp.path())]).unwrap().remove(0);
    let mut s = Session::spawn(&i, short()).unwrap();
    let capabilities = AdapterCapabilitiesResult {
        adapter: s.descriptor.clone(),
    };
    validate_output(
        &serde_json::to_value(CommandOutput::success("adapter capabilities", capabilities))
            .unwrap(),
    )
    .unwrap();
    s.close().unwrap();
    let mut s = Session::spawn(&i, short()).unwrap();
    let result = s
        .command(
            Name::new("echo").unwrap(),
            vec!["--message".into(), "hello".into()],
        )
        .unwrap();
    assert_eq!(result.details, json!({"message":"hello"}));
    validate_output(
        &serde_json::to_value(CommandOutput::success("adapter command", result)).unwrap(),
    )
    .unwrap();
    s.close().unwrap();
}
#[test]
fn directory_install_handshake_replace_and_failed_candidate_preserve_winner() {
    let temp = protected();
    let source = temp.path().join("source");
    let dest = temp.path().join("dest");
    package(&source, "fixture", &fixture(), "0.1.0");
    let installed =
        install::install(&source.join("fixture"), &root(&dest), false, short()).unwrap();
    assert!(!installed.replaced);
    assert_eq!(
        install::install(&source.join("fixture"), &root(&dest), false, short())
            .err()
            .unwrap()
            .code,
        ErrorCode::StateConflict
    );
    let replaced = install::install(&source.join("fixture"), &root(&dest), true, short()).unwrap();
    assert!(replaced.replaced);
    package(&source, "fixture", &fixture(), "invalid");
    assert!(install::install(&source.join("fixture"), &root(&dest), true, short()).is_err());
    assert_eq!(
        discovery::list(&[root(&dest)]).unwrap().adapters[0].package_version,
        "0.1.0"
    );
    validate_output(
        &serde_json::to_value(CommandOutput::success("adapter install", replaced)).unwrap(),
    )
    .unwrap();
}
#[test]
fn tarball_install_and_path_escape_rejection() {
    let temp = protected();
    let source = temp.path().join("source");
    package(&source, "fixture", &fixture(), "0.1.0");
    let archive = temp.path().join("fixture.tar");
    {
        let file = std::fs::File::create(&archive).unwrap();
        let mut tar = tar::Builder::new(file);
        tar.append_dir_all("fixture", source.join("fixture"))
            .unwrap();
        tar.finish().unwrap();
    }
    let result =
        install::install(&archive, &root(&temp.path().join("dest")), false, short()).unwrap();
    assert_eq!(result.adapter.name.as_str(), "fixture");
    let bad = temp.path().join("bad.tar");
    {
        let mut tar = tar::Builder::new(std::fs::File::create(&bad).unwrap());
        let mut header = tar::Header::new_gnu();
        header.set_size(1);
        header.set_mode(0o600);
        header.as_mut_bytes()[..12].copy_from_slice(b"../escaped\0\0");
        header.set_cksum();
        tar.append(&header, b"x".as_slice()).unwrap();
        tar.finish().unwrap();
    }
    assert!(install::install(&bad, &root(&temp.path().join("rejected")), false, short()).is_err());
    assert!(!temp.path().join("escaped").exists());
}
#[test]
fn stderr_flood_does_not_block_protocol_or_render_raw_credentials() {
    let (_t, i) = adversary("flood");
    let mut s = Session::spawn(&i, short()).unwrap();
    let flood_deadline = Instant::now() + Duration::from_secs(2);
    while !s
        .stderr_truncated
        .load(std::sync::atomic::Ordering::Acquire)
    {
        assert!(Instant::now() < flood_deadline);
        std::thread::sleep(Duration::from_millis(5));
    }
    let result = s
        .command(
            Name::new("echo").unwrap(),
            vec!["--message".into(), "hello".into()],
        )
        .unwrap();
    assert_eq!(result.details["message"], "hello");
    s.close().unwrap();
    assert!(
        s.stderr_truncated
            .load(std::sync::atomic::Ordering::Acquire)
    );
}
fn prepared_command(s: &mut Session) -> Req {
    let req = s.request_id().unwrap();
    s.send(
        &Frame::PrepareCommand {
            req,
            name: Name::new("echo").unwrap(),
            argv: Doc::inline(vec!["--message".into(), "hello".into()]),
        },
        &[],
    )
    .unwrap();
    let Frame::CommandPrepared {
        call: Doc::Inline(call),
        ..
    } = s.receive().unwrap().frame
    else {
        panic!()
    };
    let req = s.request_id().unwrap();
    s.send(
        &Frame::Command {
            req,
            command_id: call.inline.command_id,
            handle: None,
        },
        &[],
    )
    .unwrap();
    req
}
#[test]
fn cancellation_stops_descendant_before_acknowledging_and_clean_close() {
    let (t, i) = adversary("child");
    let mut s = Session::spawn(&i, short()).unwrap();
    let req = prepared_command(&mut s);
    let path = t.path().join("adversary/child.pid");
    let deadline = Instant::now() + Duration::from_secs(2);
    while !path.exists() {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(5));
    }
    let pid: i32 = std::fs::read_to_string(path).unwrap().parse().unwrap();
    let result = s.cancel(req).unwrap();
    assert_eq!(result.state, CancelState::Stopped);
    assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
    s.close().unwrap();
}
#[test]
fn ignored_cancel_escalates_with_bounded_process_group_shutdown() {
    let (_t, i) = adversary("ignore");
    let mut s = Session::spawn(&i, short()).unwrap();
    let req = prepared_command(&mut s);
    let start = Instant::now();
    assert!(s.cancel(req).is_err());
    drop(s);
    assert!(start.elapsed() < Duration::from_secs(2));
}
#[test]
fn established_command_result_survives_abnormal_shutdown() {
    let (_t, i) = adversary("abnormal");
    let mut s = Session::spawn(&i, short()).unwrap();
    let result = s
        .command(
            Name::new("echo").unwrap(),
            vec!["--message".into(), "known".into()],
        )
        .unwrap();
    assert!(s.close().is_err());
    assert_eq!(result.details["message"], "known");
}
#[test]
fn shutdown_acknowledgement_cannot_leave_supervised_descendants() {
    let (_t, i) = adversary("leak");
    let mut s = Session::spawn(&i, short()).unwrap();
    s.command(
        Name::new("echo").unwrap(),
        vec!["--message".into(), "hello".into()],
    )
    .unwrap();
    assert!(s.close().is_err());
}
#[test]
fn extraction_checkpoint_credited_rows_empty_completion_and_bounded_sink() {
    let temp = protected();
    package(temp.path(), "fixture", &fixture(), "0.1.0");
    let i = discovery::discover(&[root(temp.path())]).unwrap().remove(0);
    let mut s = Session::spawn(&i, short()).unwrap();
    let req = s.request_id().unwrap();
    s.send(
        &Frame::LocateConnection {
            req,
            connection: Doc::inline(json!({})),
            mode: Mode::Extract,
            run_id: None,
        },
        &[],
    )
    .unwrap();
    let Frame::ConnectionLocated {
        connection: Doc::Inline(locator),
        ..
    } = s.receive().unwrap().frame
    else {
        panic!()
    };
    let req = s.request_id().unwrap();
    s.send(
        &Frame::BindConnection {
            req,
            locator: Doc::inline(locator.inline),
            root: Some("/test".into()),
            expected_identity: Some("fixture-source".into()),
            expected_workspace_id: None,
            mode: Mode::Extract,
        },
        &[],
    )
    .unwrap();
    let Frame::BindResult { handle, .. } = s.receive().unwrap().frame else {
        panic!()
    };
    let attempt = Uuid::v4();
    let request = ExtractRequest {
        attempt_id: attempt.clone(),
        stream_id: Uuid::v4(),
        root: "/test".into(),
        dataset: Name::new("data").unwrap(),
        run_id: RunId::new("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
        declaration_sha256: Digest::new("a".repeat(64)).unwrap(),
        adapter_identity: grv_types::AdapterIdentity {
            name: s.descriptor.name.clone(),
            package_version: s.descriptor.package_version.clone(),
            interface_version: s.descriptor.interface_version,
            binding_schema_version: s.descriptor.binding_schema_version,
        },
        connection_identity: "fixture-source".into(),
        selection: ExtractSelection {
            policy: SelectionPolicy::All,
        },
        options: json!({}),
        tables: ["rows", "empty"]
            .into_iter()
            .map(|name| ExtractTable {
                name: Name::new(name).unwrap(),
                source: json!({}),
                columns: json!(["value"]),
                contract: TableContract {
                    columns: vec![Column {
                        name: "value".into(),
                        logical_type: json!("int64"),
                    }],
                    partition_keys: vec![],
                    extensions: json!({}),
                    column_ext: json!({}),
                },
            })
            .collect(),
        resume: None,
    };
    let req = s.request_id().unwrap();
    s.send(
        &Frame::Extract {
            req,
            handle,
            payload: Doc::inline(request),
        },
        &[],
    )
    .unwrap();
    let mut rows = 0;
    let mut tables = vec![];
    let mut acknowledged = false;
    loop {
        let packet = s.receive().unwrap();
        match packet.frame {
            Frame::ExtractStarted { .. } => {}
            Frame::Checkpoint {
                checkpoint_id,
                payload: Doc::Inline(checkpoint),
                ..
            } => {
                assert_eq!(checkpoint.inline.attempt_id, attempt);
                assert!(checkpoint.inline.tables.iter().all(|t| !t.reopenable));
                let receipt = temp.path().join("checkpoint.json");
                std::fs::write(
                    &receipt,
                    grv_types::canonical_json(&checkpoint.inline).unwrap(),
                )
                .unwrap();
                std::fs::File::open(receipt).unwrap().sync_all().unwrap();
                std::fs::File::open(temp.path())
                    .unwrap()
                    .sync_all()
                    .unwrap();
                s.send(&Frame::CheckpointAck { req, checkpoint_id }, &[])
                    .unwrap();
                acknowledged = true;
            }
            Frame::Batch {
                table,
                seq,
                slot,
                rows: count,
                ..
            } => {
                assert!(acknowledged);
                let batch = grv_adapter_wire::ipc::decode(&packet.payload, count.get()).unwrap();
                assert!(packet.payload.len() <= 8 * 1024 * 1024);
                assert_eq!(batch.num_rows(), 3);
                rows += batch.num_rows();
                drop(batch);
                drop(packet.payload);
                s.send(
                    &Frame::BatchAck {
                        req,
                        table,
                        seq,
                        slot,
                    },
                    &[],
                )
                .unwrap();
            }
            Frame::TableComplete {
                table, row_count, ..
            } => {
                assert!(acknowledged);
                if table.as_str() == "empty" {
                    assert_eq!(row_count.get(), 0);
                }
                tables.push(table);
            }
            Frame::SourceComplete {
                completion: Doc::Inline(completion),
                ..
            } => {
                assert_eq!(completion.inline.adapter_result["rows"], "3");
                break;
            }
            _ => panic!("unexpected frame"),
        }
    }
    assert_eq!(rows, 3);
    assert_eq!(tables.len(), 2);
    s.close().unwrap();
}

mod bundled_trust {
    use super::*;
    use grv_adapter_host::discovery::{Trust, trusted, trusted_with};
    use std::{
        fs,
        os::unix::fs::{MetadataExt, PermissionsExt},
        process::Command,
    };

    fn mode(path: &Path, mode: u32) {
        fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
    }
    /// `<temp>/prefix/lib/grv/adapters/demo/bin`, like a Homebrew prefix.
    fn layout() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let temp = protected();
        let root = temp.path().join("prefix/lib/grv/adapters");
        let bin = root.join("demo/bin");
        fs::create_dir_all(&bin).unwrap();
        fs::write(bin.join("tool"), "x").unwrap();
        (temp, root, bin.join("tool"))
    }
    /// Make `path` group-owned by macOS `admin` (gid 80); false if not allowed.
    fn admin_group(path: &Path) -> bool {
        cfg!(target_os = "macos")
            && Command::new("chgrp")
                .arg("admin")
                .arg(path)
                .status()
                .is_ok_and(|s| s.success())
    }
    fn me() -> u32 {
        unsafe { libc::geteuid() }
    }

    #[test]
    fn world_writable_is_refused_even_above_the_bundled_root() {
        let (temp, root, tool) = layout();
        let prefix = temp.path().join("prefix");
        mode(&prefix, 0o777);
        let trust = Trust::bundled_at(&root, me()).unwrap();
        assert!(trusted_with(&tool, None, &trust).is_err());
        mode(&prefix, 0o755);
        assert!(trusted_with(&tool, None, &trust).is_ok());
    }

    #[test]
    fn admin_group_write_is_allowed_only_above_the_bundled_root() {
        let (temp, root, tool) = layout();
        let lib = temp.path().join("prefix/lib");
        if !admin_group(&lib) || !admin_group(&root) || !admin_group(&root.join("demo")) {
            eprintln!("skipped: cannot assign the macOS admin group here");
            return;
        }
        mode(&lib, 0o775);
        let trust = Trust::bundled_at(&root, me()).unwrap();
        // Default policy refuses Homebrew-style admin-writable prefixes.
        assert!(trusted(&tool, None).is_err());
        assert!(trusted_with(&tool, None, &trust).is_ok());
        // The bundled root itself and anything inside it stay owner-only.
        mode(&root, 0o775);
        assert!(trusted_with(&tool, None, &trust).is_err());
        mode(&root, 0o755);
        mode(&root.join("demo"), 0o775);
        assert!(trusted_with(&tool, None, &trust).is_err());
        mode(&root.join("demo"), 0o755);
        // A policy for another root does not extend to this one.
        let other = Trust::bundled_at(temp.path(), me()).unwrap();
        assert!(trusted_with(&tool, None, &other).is_err());
    }

    #[test]
    fn group_write_by_a_non_admin_group_is_refused() {
        let (temp, root, tool) = layout();
        let lib = temp.path().join("prefix/lib");
        let gid = fs::metadata(&lib).unwrap().gid();
        if gid == 80 {
            return;
        }
        mode(&lib, 0o775);
        let trust = Trust::bundled_at(&root, me()).unwrap();
        assert!(trusted_with(&tool, None, &trust).is_err());
    }
}
