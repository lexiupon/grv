#![cfg(unix)]
use grv_adapter_duckdb::lock::WorkspaceLock;
use std::{fs, os::unix::fs::symlink};
// Fork temporarily inherits every open file description before CLOEXEC runs.
// Serialize these ownership fixtures so another test's child cannot briefly
// retain a lock that this test has just released.
static FIXTURE_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn symlink_aliases_share_nonblocking_persistent_lock() {
    let _serial = FIXTURE_SERIAL.lock().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let engine = directory.path().join("engine.duckdb");
    fs::write(&engine, []).unwrap();
    let alias = directory.path().join("alias.duckdb");
    symlink(&engine, &alias).unwrap();
    let owner = WorkspaceLock::acquire(&engine).unwrap();
    assert_eq!(owner.engine_path(), fs::canonicalize(&alias).unwrap());
    assert_eq!(
        WorkspaceLock::acquire(&alias).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    let helper = owner.helper_path().to_owned();
    let descendant = owner.descendant_reference().unwrap();
    drop(owner);
    assert!(WorkspaceLock::acquire(&alias).is_err());
    drop(descendant);
    assert!(WorkspaceLock::acquire(&engine).is_ok());
    assert!(helper.exists());
}

#[test]
fn hard_link_engine_aliases_are_rejected() {
    let _serial = FIXTURE_SERIAL.lock().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let engine = directory.path().join("engine.duckdb");
    fs::write(&engine, []).unwrap();
    fs::hard_link(&engine, directory.path().join("alias.duckdb")).unwrap();
    assert!(WorkspaceLock::acquire(&engine).is_err());
}

#[test]
fn replacement_and_symlink_helpers_are_rejected() {
    let _serial = FIXTURE_SERIAL.lock().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let engine = directory.path().join("engine.duckdb");
    fs::write(&engine, []).unwrap();
    let owner = WorkspaceLock::acquire(&engine).unwrap();
    let backup = directory.path().join("old-helper");
    fs::rename(owner.helper_path(), &backup).unwrap();
    fs::write(owner.helper_path(), []).unwrap();
    assert!(owner.recheck().is_err());
    drop(owner);
    fs::remove_file(engine.with_extension("duckdb.grv-lock")).unwrap();
    symlink(&backup, engine.with_extension("duckdb.grv-lock")).unwrap();
    assert!(WorkspaceLock::acquire(&engine).is_err());
}

#[test]
fn engine_inode_replacement_is_rejected_under_lock() {
    let _serial = FIXTURE_SERIAL.lock().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let engine = directory.path().join("engine.duckdb");
    fs::write(&engine, []).unwrap();
    let owner = WorkspaceLock::acquire(&engine).unwrap();
    fs::rename(&engine, directory.path().join("old.duckdb")).unwrap();
    fs::write(&engine, []).unwrap();
    assert!(owner.recheck().is_err());
}

#[test]
fn workspace_helper_permission_changes_are_refused_before_reuse() {
    use std::os::unix::fs::PermissionsExt;
    let _serial = FIXTURE_SERIAL.lock().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let engine = directory.path().join("engine.duckdb");
    let owner = WorkspaceLock::acquire(&engine).unwrap();
    fs::set_permissions(owner.helper_path(), fs::Permissions::from_mode(0o666)).unwrap();
    assert!(owner.recheck().is_err());
    fs::set_permissions(owner.helper_path(), fs::Permissions::from_mode(0o600)).unwrap();
    assert!(owner.recheck().is_ok());
}

#[test]
fn child_lock_reference_probe() {
    use std::io::{Read, Write};
    use std::os::fd::FromRawFd;
    let Ok(fd) = std::env::var("GRV_TEST_LOCK_REFERENCE_FD") else {
        return;
    };
    // The test parent explicitly passes this inherited descriptor.
    let file = unsafe { std::fs::File::from_raw_fd(fd.parse().unwrap()) };
    println!("grv-lock-ready");
    std::io::stdout().flush().unwrap();
    std::io::stdin().read_exact(&mut [0]).unwrap();
    drop(file);
}

#[test]
fn child_retains_shared_lock_after_parent_owner_closes() {
    let _serial = FIXTURE_SERIAL.lock().unwrap();
    use std::io::{BufRead, BufReader, Write};
    use std::os::{fd::AsRawFd, unix::process::CommandExt};
    use std::process::{Command, Stdio};
    let directory = tempfile::tempdir().unwrap();
    let engine = directory.path().join("engine.duckdb");
    fs::write(&engine, []).unwrap();
    let owner = WorkspaceLock::acquire(&engine).unwrap();
    let reference = owner.descendant_reference().unwrap();
    let fd = reference.as_raw_fd();
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "child_lock_reference_probe", "--nocapture"])
        .env("GRV_TEST_LOCK_REFERENCE_FD", fd.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped());
    unsafe {
        command.pre_exec(move || {
            if libc::fcntl(fd, libc::F_SETFD, 0) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command.spawn().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    loop {
        line.clear();
        assert_ne!(stdout.read_line(&mut line).unwrap(), 0);
        // Serial libtest prints its test-name prefix on this same line.
        if line.trim_end().ends_with("grv-lock-ready") {
            break;
        }
    }
    drop(reference);
    drop(owner);
    assert_eq!(
        WorkspaceLock::acquire(&engine).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    child.stdin.take().unwrap().write_all(&[1]).unwrap();
    assert!(child.wait().unwrap().success());
    assert!(WorkspaceLock::acquire(&engine).is_ok());
}
