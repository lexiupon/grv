use grv_adapter_host::session_lock::SessionMutationLock;
use grv_types::{ErrorCode, RunId};
use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
};

fn run(suffix: char) -> RunId {
    RunId::new(format!("01ARZ3NDEKTSV4RRFFQ69G5FA{suffix}")).unwrap()
}

#[test]
fn session_mutations_serialize_across_aliases_but_independent_runs_do_not() {
    let dir = tempfile::tempdir().unwrap();
    let engine = dir.path().join("engine.duckdb");
    fs::write(&engine, []).unwrap();
    let alias = dir.path().join("alias.duckdb");
    symlink(&engine, &alias).unwrap();
    let owner = SessionMutationLock::acquire(&engine, &run('V')).unwrap();
    assert_eq!(
        SessionMutationLock::acquire(&alias, &run('V'))
            .unwrap_err()
            .code,
        ErrorCode::EngineBusy
    );
    assert!(SessionMutationLock::acquire(&alias, &run('W')).is_ok());
    let helper = owner.helper_path().to_owned();
    drop(owner);
    assert!(helper.exists());
    assert!(SessionMutationLock::acquire(&alias, &run('V')).is_ok());
}

#[test]
fn session_helper_substitution_and_unprotected_paths_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let engine = dir.path().join("engine.duckdb");
    let owner = SessionMutationLock::acquire(&engine, &run('V')).unwrap();
    let helper = owner.helper_path().to_owned();
    fs::rename(&helper, dir.path().join("old-lock")).unwrap();
    fs::write(&helper, []).unwrap();
    assert!(owner.recheck().is_err());
    drop(owner);
    fs::set_permissions(&helper, fs::Permissions::from_mode(0o666)).unwrap();
    assert!(SessionMutationLock::acquire(&engine, &run('V')).is_err());
    fs::remove_file(&helper).unwrap();
    symlink(dir.path().join("old-lock"), &helper).unwrap();
    assert!(SessionMutationLock::acquire(&engine, &run('V')).is_err());
    fs::remove_file(&helper).unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o777)).unwrap();
    assert!(SessionMutationLock::acquire(&engine, &run('V')).is_err());
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
}

#[test]
fn hardlinks_and_engine_replacement_do_not_authorize_a_second_session_owner() {
    let dir = tempfile::tempdir().unwrap();
    let engine = dir.path().join("engine.duckdb");
    fs::write(&engine, []).unwrap();
    let owner = SessionMutationLock::acquire(&engine, &run('V')).unwrap();
    fs::rename(&engine, dir.path().join("original.duckdb")).unwrap();
    fs::write(&engine, []).unwrap();
    assert!(owner.recheck().is_err());
    drop(owner);
    fs::hard_link(&engine, dir.path().join("alias.duckdb")).unwrap();
    assert!(SessionMutationLock::acquire(&engine, &run('V')).is_err());
}
