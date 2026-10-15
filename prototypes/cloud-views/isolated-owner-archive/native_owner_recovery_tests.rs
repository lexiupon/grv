//! Isolated helper receipt-loss primitives, synthetic local-only state.
//! Receipt table tests qualify the native primitive, not full PullStore receipts.
use super::*;

#[test]
fn parent_crash_probe() {
    let Some(path) = std::env::var_os("GRV_TEST_OWNER_CRASH_PATH") else {
        return;
    };
    let mut engine = NativeEngine::open(Path::new(&path)).unwrap();
    engine.isolate_gcs_owner().unwrap();
    engine.metadata_query("BEGIN").unwrap();
    engine
        .metadata_query("CREATE TABLE _grv_crash_receipt (result BIGINT)")
        .unwrap();
    engine
        .metadata_query("INSERT INTO _grv_crash_receipt VALUES (42)")
        .unwrap();
    // Exit the entire parent process without destructors: only child-side
    // original-budget/parent-disconnection supervision can release its lock.
    unsafe { libc::_exit(72) };
}

#[test]
#[ignore = "requires explicit protected GCS helper; source-free parent-crash subprocess"]
fn parent_crash_self_fences_child_and_recovers_uncommitted_workspace() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("parent-crash.duckdb");
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "native::owner_process::recovery_tests::parent_crash_probe",
            "--test-threads=1",
        ])
        .env("GRV_TEST_OWNER_CRASH_PATH", &path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut parent = command.spawn().unwrap();
    let end = Instant::now() + Duration::from_secs(10);
    let code = loop {
        if let Some(status) = parent.try_wait().unwrap() {
            break status.code();
        }
        if Instant::now() >= end {
            parent.kill().unwrap();
            let _ = parent.wait();
            panic!("parent probe did not reach crash");
        }
        thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(code, Some(72));
    let mut recovered = loop {
        match NativeEngine::open(&path) {
            Ok(engine) => break engine,
            Err(_) if Instant::now() < end => thread::sleep(Duration::from_millis(10)),
            Err(_) => panic!("orphan child did not relinquish native and workspace locks"),
        }
    };
    assert!(
        !recovered
            .relation_exists("main", "_grv_crash_receipt")
            .unwrap()
    );
}

#[test]
#[ignore = "requires protected helper; invalid extension and synthetic token only, no cloud"]
fn failed_reader_load_sanitizes_errors_and_releases_private_owner_state() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("failed-reader.duckdb");
    let mut engine = NativeEngine::open(&path).unwrap();
    engine.isolate_gcs_owner().unwrap();
    let token = "synthetic-load-failure-canary-not-a-credential";
    let missing = dir.path().join("missing.duckdb_extension");
    let error = engine
        .isolated
        .as_mut()
        .unwrap()
        .configure(
            missing.to_str().unwrap(),
            "gs://test-bucket/data",
            token,
            "named-project",
            Instant::now() + Duration::from_secs(3),
        )
        .unwrap_err();
    let diagnostic = format!("{error:?} {error}");
    assert!(!diagnostic.contains(token));
    // Without the extension, native memory-secret inspection can itself be
    // unavailable. That now requires fencing, not a reusable-denial promise.
    // Source-including post-secret proof covers authoritative rollback path.
    engine.close_invocation().unwrap();
    engine.metadata_query("BEGIN").unwrap();
    engine
        .metadata_query("CREATE TABLE _grv_setup_rollback(id BIGINT)")
        .unwrap();
    engine.metadata_query("ROLLBACK").unwrap();
    assert!(
        !engine
            .relation_exists("main", "_grv_setup_rollback")
            .unwrap()
    );
    assert!(
        engine
            .metadata_query("SELECT * FROM duckdb_secrets()")
            .is_err()
    );
    drop(engine);
    let database = fs::read(&path).unwrap();
    assert!(
        !database
            .windows(token.len())
            .any(|bytes| bytes == token.as_bytes())
    );
    let mut reopened = NativeEngine::open(&path).unwrap();
    assert!(
        !reopened
            .relation_exists("main", "_grv_setup_rollback")
            .unwrap()
    );
}

#[test]
#[ignore = "requires protected helper; source-free invalid-extension setup, no cloud"]
fn later_configuration_cannot_renew_original_owner_budget() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("original-budget.duckdb");
    let path = crate::lock::canonical_engine_path(&path).unwrap();
    let lock = WorkspaceLock::acquire(&path).unwrap();
    let original = Instant::now() + Duration::from_secs(3);
    let mut owner =
        ProcessOwner::spawn_unconfigured(&path, &lock, original, Arc::new(AtomicBool::new(false)))
            .unwrap();
    let missing = dir.path().join("absent.duckdb_extension");
    assert!(
        owner
            .configure(
                missing.to_str().unwrap(),
                "gs://test-bucket/data",
                "synthetic-not-a-credential",
                "named-project",
                Instant::now() + Duration::from_secs(60)
            )
            .is_err()
    );
    assert_eq!(owner.deadline(), original);
    let shortened = Instant::now() + Duration::from_millis(500);
    assert!(
        owner
            .configure(
                missing.to_str().unwrap(),
                "gs://test-bucket/data",
                "synthetic-not-a-credential",
                "named-project",
                shortened
            )
            .is_err()
    );
    assert_eq!(owner.deadline(), shortened);
    assert!(
        owner
            .configure(
                missing.to_str().unwrap(),
                "gs://test-bucket/data",
                "synthetic-not-a-credential",
                "named-project",
                Instant::now() + Duration::from_secs(60)
            )
            .is_err()
    );
    assert_eq!(owner.deadline(), shortened);
    owner.close().unwrap();
    drop(owner);
    drop(lock);
    NativeEngine::open(&path).unwrap();
}

#[test]
fn unproved_setup_revocation_exits_whole_owner_instead_of_reusable_denial() {
    const CHILD: &str = "GRV_TEST_UNPROVED_GCS_SETUP";
    if std::env::var_os(CHILD).is_some() {
        let _ = setup_outcome(-2);
        panic!("unsafe setup returned");
    }
    let output=Command::new(std::env::current_exe().unwrap()).args(["--exact","native::owner_process::recovery_tests::unproved_setup_revocation_exits_whole_owner_instead_of_reusable_denial","--test-threads=1"]).env(CHILD,"1").output().unwrap();
    assert_eq!(output.status.code(), Some(125));
    assert!(setup_outcome(-1).is_err());
    assert!(setup_outcome(0).is_ok());
}

#[test]
fn original_budget_refuses_worst_case_escaped_frames_without_growth() {
    let query = "\u{0001}".repeat(ARG_LIMIT);
    let message = Message::Call(Request::Metadata {
        query,
        private: false,
    });
    let mut buffer = CappedBuffer(Zeroizing::new(Vec::with_capacity(FRAME_LIMIT)));
    let capacity = buffer.0.capacity();
    assert!(serde_json::to_writer(&mut buffer, &message).is_err());
    assert_eq!(buffer.0.capacity(), capacity);
    assert!(buffer.0.len() <= FRAME_LIMIT);
    // Logical2MiB commands are not automatically admissible if JSON expansion
    // exceeds bounded private IPC. Refuse, never allocate a larger frame.
}

#[test]
fn supervisor_fences_same_group_descendant_retaining_workspace_descriptor() {
    let dir = tempfile::tempdir().unwrap();
    let lock = tempfile::tempfile().unwrap();
    let fd = lock.as_raw_fd();
    let marker = dir.path().join("descendant.pid");
    let mut command = Command::new("/bin/sh");
    command
        .arg("-c")
        .arg("sleep 30 & echo $! > \"$1\"; wait")
        .arg("probe")
        .arg(&marker)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    unsafe {
        command.pre_exec(move || {
            if libc::setpgid(0, 0) != 0 {
                return Err(io::Error::last_os_error());
            }
            let flags = libc::fcntl(fd, libc::F_GETFD);
            if flags < 0 || libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = command.spawn().unwrap();
    let end = Instant::now() + Duration::from_secs(2);
    while !marker.exists() && Instant::now() < end {
        thread::sleep(Duration::from_millis(2));
    }
    assert!(marker.exists());
    let descendant: libc::pid_t = fs::read_to_string(&marker).unwrap().trim().parse().unwrap();
    assert_eq!(
        unsafe { libc::getpgid(descendant) },
        child.id() as libc::pid_t
    );
    let deadline = Instant::now() + Duration::from_millis(50);
    let supervisor = Arc::new(Supervisor {
        state: Mutex::new(State {
            stop: false,
            done: None,
            deadline: Some(deadline),
        }),
        wake: Condvar::new(),
        cancelled: Arc::new(AtomicBool::new(false)),
        lock: LockFence {
            file: ManuallyDrop::new(lock),
            proved: AtomicBool::new(false),
        },
    });
    watch(child, supervisor.clone(), deadline);
    // Zombies may delay group disappearance. That is an unresolved fence,
    // never permission to release ownership; both outcomes preserve safety.
    let done = supervisor.state.lock().unwrap().done.unwrap();
    if done {
        assert!(supervisor.lock.proved.load(Ordering::Acquire));
    } else {
        assert!(!supervisor.lock.proved.load(Ordering::Acquire));
        assert!(
            unsafe { libc::fcntl(fd, libc::F_GETFD) } >= 0,
            "unresolved descendant fence must retain ownership descriptor"
        );
    }
    assert!(supervisor.cancelled.load(Ordering::Acquire));
}

#[test]
#[ignore = "requires explicit protected GCS helper; no credentials/cloud"]
fn replaced_database_is_refused_without_mutating_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("replaced.duckdb");
    let replacement = dir.path().join("replacement.duckdb");
    let mut engine = NativeEngine::open(&path).unwrap();
    engine.isolate_gcs_owner().unwrap();
    {
        let mut other = NativeEngine::open(&replacement).unwrap();
        other
            .metadata_query("CREATE TABLE _grv_replacement AS SELECT 99::BIGINT AS id")
            .unwrap();
    }
    let before = fs::read(&replacement).unwrap();
    fs::rename(&path, dir.path().join("original.duckdb")).unwrap();
    fs::rename(&replacement, &path).unwrap();
    assert!(
        engine
            .metadata_query("CREATE TABLE _grv_must_not_exist AS SELECT 42::BIGINT AS id")
            .is_err()
    );
    assert_eq!(fs::read(&path).unwrap(), before);
    // Direct child RPC (bypassing parent's named inode recheck solely in test)
    // must independently refuse before executing or mutating replacement.
    assert!(
        engine
            .isolated
            .as_mut()
            .unwrap()
            .call(Request::Metadata {
                query: "CREATE TABLE _grv_must_not_exist AS SELECT 42::BIGINT AS id".into(),
                private: false
            })
            .is_err()
    );
    assert_eq!(fs::read(&path).unwrap(), before);
    drop(engine);
}

#[test]
#[ignore = "requires explicit protected GCS helper; no credentials/cloud"]
fn existing_interrupt_clone_cancels_isolated_compute_before_original_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cancel.duckdb");
    let mut engine = NativeEngine::open(&path).unwrap();
    let interrupt = engine.interrupt_handle();
    engine.isolate_gcs_owner().unwrap();
    let trigger = thread::spawn(move || {
        thread::sleep(Duration::from_millis(75));
        crate::worker::Interrupt::interrupt(&interrupt);
    });
    let start = Instant::now();
    assert!(
        engine
            .metadata_query("SELECT sum(i::DOUBLE) FROM range(1000000000000) t(i)")
            .is_err()
    );
    trigger.join().unwrap();
    engine.close_invocation().unwrap();
    assert!(start.elapsed() < Duration::from_secs(3));
    drop(engine);
    NativeEngine::open(&path).unwrap();
}

#[test]
#[ignore = "requires explicit protected GCS helper; no credentials/cloud"]
fn lost_commit_reply_is_fenced_and_resolved_without_reexecuting() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("commit.duckdb");
    let mut engine = NativeEngine::open(&path).unwrap();
    engine.isolate_gcs_owner().unwrap();
    engine.metadata_query("BEGIN").unwrap();
    engine
        .metadata_query("CREATE TABLE _grv_receipt (attempt VARCHAR PRIMARY KEY, result BIGINT)")
        .unwrap();
    engine
        .metadata_query("INSERT INTO _grv_receipt VALUES ('immutable-attempt',42)")
        .unwrap();
    let owner = engine.isolated.as_mut().unwrap();
    // Deliberately send COMMIT without receiving the reply. No second COMMIT
    // or INSERT is issued. Waiting for readable response proves the child
    // completed the call, but the application intentionally loses its content.
    send(
        &mut owner.stream,
        &Message::Call(Request::Metadata {
            query: "COMMIT".into(),
            private: false,
        }),
        owner.deadline,
        &owner.supervisor.cancelled,
    )
    .unwrap();
    ready(
        owner.stream.as_raw_fd(),
        libc::POLLIN,
        owner.deadline,
        &owner.supervisor.cancelled,
    )
    .unwrap();
    engine.close_invocation().unwrap();
    engine.authorize_relation("main", "_grv_receipt").unwrap();
    assert_eq!(
        engine
            .metadata_query("SELECT result FROM _grv_receipt WHERE attempt='immutable-attempt'")
            .unwrap(),
        vec![vec![Some("42".into())]]
    );
    assert_eq!(
        engine
            .metadata_query("SELECT count(*) FROM _grv_receipt")
            .unwrap(),
        vec![vec![Some("1".into())]]
    );
    drop(engine);
    let _recovered = NativeEngine::open(&path).unwrap();
}

#[test]
#[ignore = "requires explicit protected GCS helper; no credentials/cloud"]
fn lost_precommit_reply_leaves_no_committed_receipt_after_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rollback.duckdb");
    let mut engine = NativeEngine::open(&path).unwrap();
    engine.isolate_gcs_owner().unwrap();
    engine.metadata_query("BEGIN").unwrap();
    engine
        .metadata_query("CREATE TABLE _grv_receipt (attempt VARCHAR PRIMARY KEY, result BIGINT)")
        .unwrap();
    engine
        .metadata_query("INSERT INTO _grv_receipt VALUES ('immutable-attempt',42)")
        .unwrap();
    engine.close_invocation().unwrap();
    assert!(!engine.relation_exists("main", "_grv_receipt").unwrap());
    drop(engine);
    let mut reopened = NativeEngine::open(&path).unwrap();
    assert!(!reopened.relation_exists("main", "_grv_receipt").unwrap());
}
