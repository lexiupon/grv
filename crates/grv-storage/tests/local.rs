use grv_storage::{
    Backend, ErrorKind, FaultInjector, FaultPoint, ListEntry, ListMode, LocalBackend, ObjectKey,
    ObjectPrefix, WriteEffect,
    model::{Latest, encode_record},
};
use std::{
    io::{self, Read},
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    },
};
fn key(value: &str) -> ObjectKey {
    ObjectKey::new(value).unwrap()
}
struct FailOnce {
    point: FaultPoint,
    used: AtomicBool,
}
impl FaultInjector for FailOnce {
    fn check(&self, point: FaultPoint) -> io::Result<()> {
        if point == self.point && !self.used.swap(true, Ordering::SeqCst) {
            Err(io::Error::other("injected durability failure"))
        } else {
            Ok(())
        }
    }
}
fn fail(point: FaultPoint) -> Arc<FailOnce> {
    Arc::new(FailOnce {
        point,
        used: AtomicBool::new(false),
    })
}
#[test]
fn competing_conditional_creates_install_exactly_one_complete_object() {
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalBackend::open(temp.path()).unwrap());
    let start = Arc::new(Barrier::new(8));
    let threads: Vec<_> = (0u8..8)
        .map(|n| {
            let store = store.clone();
            let start = start.clone();
            std::thread::spawn(move || {
                start.wait();
                (
                    n,
                    store.create_bytes(&key("nested/object"), &vec![n; 200_000]),
                )
            })
        })
        .collect();
    let results: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    let winners: Vec<_> = results
        .iter()
        .filter(|(_, result)| result.is_ok())
        .collect();
    assert_eq!(winners.len(), 1);
    for (_, result) in results.iter().filter(|(_, result)| result.is_err()) {
        assert_eq!(
            result.as_ref().unwrap_err().kind,
            ErrorKind::PreconditionFailed
        );
    }
    let (bytes, meta) = store.read_bytes(&key("nested/object"), 200_000).unwrap();
    assert_eq!(bytes, vec![winners[0].0; 200_000]);
    assert_eq!(meta.size.get(), 200_000);
    assert_eq!(store.head(&key("nested/object")).unwrap(), meta);
}
#[test]
fn competing_cas_on_same_backend_has_one_winner_and_stale_mutation_never_returns() {
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalBackend::open(temp.path()).unwrap());
    let initial = encode_record(&Latest::empty()).unwrap();
    let validator = store.create_bytes(&key("LATEST"), &initial).unwrap();
    let start = Arc::new(Barrier::new(8));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let store = store.clone();
            let start = start.clone();
            let validator = validator.clone();
            std::thread::spawn(move || {
                let bytes = encode_record(&Latest::empty()).unwrap();
                start.wait();
                store.put_bytes(&key("LATEST"), &validator, &bytes)
            })
        })
        .collect();
    let results: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    assert_eq!(results.iter().filter(|v| v.is_ok()).count(), 1);
    assert!(
        results
            .iter()
            .filter_map(|v| v.as_ref().err())
            .all(|v| v.kind == ErrorKind::PreconditionFailed)
    );
    let current = store.head(&key("LATEST")).unwrap().validator;
    assert_ne!(validator, current);
    let unchanged_counters = encode_record(&Latest::empty()).unwrap();
    let next = store
        .put_bytes(&key("LATEST"), &current, &unchanged_counters)
        .unwrap();
    assert_ne!(next, validator);
    assert_eq!(
        store
            .put_bytes(&key("LATEST"), &validator, &initial)
            .unwrap_err()
            .kind,
        ErrorKind::PreconditionFailed
    );
}
#[test]
fn pre_install_failures_are_no_effect_and_leave_no_layout_object_or_temp() {
    for point in [FaultPoint::BeforeTempSync, FaultPoint::BeforeInstall] {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalBackend::open(temp.path())
            .unwrap()
            .with_faults(fail(point));
        let error = store
            .create_bytes(&key("deep/parent/object"), b"complete")
            .unwrap_err();
        assert_eq!(error.effect, WriteEffect::NoEffect);
        assert_eq!(
            store.head(&key("deep/parent/object")).unwrap_err().kind,
            ErrorKind::NotFound
        );
        assert!(
            std::fs::read_dir(temp.path().join("deep/parent"))
                .unwrap()
                .next()
                .is_none()
        );
    }
}
#[test]
fn lost_create_and_cas_acknowledgements_are_proved_by_durable_reread() {
    for point in [FaultPoint::AfterInstall, FaultPoint::BeforePathSync] {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalBackend::open(temp.path())
            .unwrap()
            .with_faults(fail(point));
        let error = store
            .create_bytes(&key("deep/parent/object"), b"created")
            .unwrap_err();
        assert_eq!(error.effect, WriteEffect::MaybeApplied);
        let (bytes, meta) = store.read_bytes(&key("deep/parent/object"), 64).unwrap();
        assert_eq!(bytes, b"created");
        let store = LocalBackend::open(temp.path())
            .unwrap()
            .with_faults(fail(point));
        let error = store
            .put_bytes(&key("deep/parent/object"), &meta.validator, b"replaced")
            .unwrap_err();
        assert_eq!(error.effect, WriteEffect::MaybeApplied);
        let (bytes, replacement) = store.read_bytes(&key("deep/parent/object"), 64).unwrap();
        assert_eq!(bytes, b"replaced");
        assert_ne!(replacement.validator, meta.validator);
    }
}
#[test]
fn durability_errors_are_failed_reads_and_never_absence() {
    for head in [true, false] {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalBackend::open(temp.path()).unwrap();
        store.create_bytes(&key("deep/object"), b"visible").unwrap();
        let store = store.with_faults(fail(FaultPoint::BeforeReadSync));
        let error = if head {
            store.head(&key("deep/object")).unwrap_err()
        } else {
            store.read_bytes(&key("deep/object"), 64).unwrap_err()
        };
        assert_eq!(error.kind, ErrorKind::Io);
        assert_eq!(store.head(&key("deep/object")).unwrap().size.get(), 7);
    }
}
#[test]
fn listings_are_sorted_delimited_and_ignore_private_temporary_names() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalBackend::open(temp.path()).unwrap();
    for name in [
        "table/version=10/manifest.json",
        "table/version=1/manifest.json",
        "table/.layout.json",
    ] {
        store.create_bytes(&key(name), b"{}").unwrap();
    }
    std::fs::write(temp.path().join("table/.tmp-orphan"), b"partial").unwrap();
    let child = store
        .list(&ObjectPrefix::new("table/").unwrap(), ListMode::Children)
        .unwrap();
    assert_eq!(
        child,
        vec![
            ListEntry::Object(key("table/.layout.json")),
            ListEntry::Prefix(ObjectPrefix::new("table/version=1/").unwrap()),
            ListEntry::Prefix(ObjectPrefix::new("table/version=10/").unwrap())
        ]
    );
    assert_eq!(
        store
            .list(
                &ObjectPrefix::new("table/version=1/").unwrap(),
                ListMode::Recursive
            )
            .unwrap(),
        vec![ListEntry::Object(key("table/version=1/manifest.json"))]
    );
    assert!(
        store
            .list(&ObjectPrefix::new("missing/").unwrap(), ListMode::Recursive)
            .unwrap()
            .is_empty()
    );
}
#[test]
fn missing_open_put_and_delete_preserve_absence() {
    let temp = tempfile::tempdir().unwrap();
    let missing = temp.path().join("missing");
    assert!(LocalBackend::open(&missing).is_err());
    assert!(!missing.exists());
    let store = LocalBackend::open(temp.path()).unwrap();
    let expected = grv_storage::Validator::new("opaque").unwrap();
    assert_eq!(
        store
            .put_bytes(&key("missing/object"), &expected, b"x")
            .unwrap_err()
            .kind,
        ErrorKind::PreconditionFailed
    );
    store.delete(&key("missing/object")).unwrap();
    assert!(!missing.exists());
    store.create_bytes(&key("object"), b"x").unwrap();
    store.delete(&key("object")).unwrap();
    store.delete(&key("object")).unwrap();
}
#[test]
fn traversal_symlinks_fifo_and_replaced_roots_are_refused_without_following() {
    use std::{ffi::CString, os::unix::ffi::OsStrExt, os::unix::fs::symlink};
    for path in [
        "../object",
        "a/../b",
        "/absolute",
        "a//b",
        "a/.tmp-forged",
        "a\\b",
    ] {
        assert!(ObjectKey::new(path).is_err());
    }
    let temp = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let store = LocalBackend::open(temp.path()).unwrap();
    symlink(outside.path(), temp.path().join("escape")).unwrap();
    assert_eq!(
        store
            .create_bytes(&key("escape/object"), b"x")
            .unwrap_err()
            .kind,
        ErrorKind::Integrity
    );
    assert!(!outside.path().join("object").exists());
    symlink("escape", temp.path().join("object")).unwrap();
    assert_eq!(
        store.head(&key("object")).unwrap_err().kind,
        ErrorKind::Integrity
    );
    let fifo = CString::new(temp.path().join("fifo").as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
    assert_eq!(
        store.head(&key("fifo")).unwrap_err().kind,
        ErrorKind::Integrity
    );
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    std::fs::create_dir(&root).unwrap();
    let store = LocalBackend::open(&root).unwrap();
    std::fs::rename(&root, base.path().join("old")).unwrap();
    std::fs::create_dir(&root).unwrap();
    assert_eq!(
        store.create_bytes(&key("object"), b"x").unwrap_err().kind,
        ErrorKind::Integrity
    );
}
#[test]
fn interrupted_source_never_installs_partial_content_and_reads_are_bounded() {
    struct Broken(bool);
    impl Read for Broken {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if self.0 {
                Err(io::Error::other("source interrupted"))
            } else {
                self.0 = true;
                buf[..4].copy_from_slice(b"part");
                Ok(4)
            }
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let store = LocalBackend::open(temp.path()).unwrap();
    let error = store
        .conditional_create(&key("object"), &mut Broken(false))
        .unwrap_err();
    assert_eq!(error.effect, WriteEffect::NoEffect);
    assert_eq!(
        store.head(&key("object")).unwrap_err().kind,
        ErrorKind::NotFound
    );
    store
        .create_bytes(&key("object"), &vec![4; 100_000])
        .unwrap();
    assert!(store.read_bytes(&key("object"), 99_999).is_err());
    assert_eq!(store.head(&key("object")).unwrap().size.get(), 100_000);
}

struct ExitAfterInstall;
impl FaultInjector for ExitAfterInstall {
    fn check(&self, point: FaultPoint) -> io::Result<()> {
        if point == FaultPoint::AfterInstall {
            std::process::exit(77);
        }
        Ok(())
    }
}
#[test]
#[ignore = "subprocess helper invoked by writer_process_crash_is_adopted_through_durable_get"]
fn crash_worker() {
    let Some(root) = std::env::var_os("GRV_TEST_CRASH_ROOT") else {
        return;
    };
    let store = LocalBackend::open(root)
        .unwrap()
        .with_faults(Arc::new(ExitAfterInstall));
    if let Ok(expected) = std::env::var("GRV_TEST_CRASH_VALIDATOR") {
        store
            .put_bytes(
                &key("deep/parent/object"),
                &grv_storage::Validator::new(expected).unwrap(),
                b"crashed replacement",
            )
            .unwrap();
    } else {
        store
            .create_bytes(&key("deep/parent/object"), b"crashed creation")
            .unwrap();
    }
    panic!("worker did not crash at install boundary");
}
#[test]
fn writer_process_crash_is_adopted_through_durable_get() {
    for put in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let store = LocalBackend::open(root.path()).unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap());
        process
            .args(["--exact", "crash_worker", "--ignored"])
            .env("GRV_TEST_CRASH_ROOT", root.path())
            .env_remove("GRV_TEST_CRASH_VALIDATOR");
        if put {
            let expected = store
                .create_bytes(&key("deep/parent/object"), b"original")
                .unwrap();
            process.env("GRV_TEST_CRASH_VALIDATOR", expected.as_str());
        }
        let output = process.output().unwrap();
        assert_eq!(output.status.code(), Some(77));
        let (bytes, meta) = store.read_bytes(&key("deep/parent/object"), 64).unwrap();
        assert_eq!(
            bytes,
            if put {
                b"crashed replacement".as_slice()
            } else {
                b"crashed creation".as_slice()
            }
        );
        assert_eq!(store.head(&key("deep/parent/object")).unwrap(), meta);
        assert_eq!(
            store
                .list(
                    &ObjectPrefix::new("deep/parent/").unwrap(),
                    ListMode::Children
                )
                .unwrap(),
            vec![ListEntry::Object(key("deep/parent/object"))]
        );
        store
            .put_bytes(
                &key("deep/parent/object"),
                &meta.validator,
                b"acknowledged successor",
            )
            .unwrap();
    }
}
