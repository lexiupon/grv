//! Live GCS publication through the conformance CLI and test-only fixture adapter.
//! This is NOT production DuckDB CLI coverage. No bucket, IAM, or xyz fixture changes.
use arrow_array::{Array, Int64Array};
use grv_adapter_host::validate_output;
use grv_core::{revision, store::Store};
use grv_storage::{
    Backend, Error, ErrorKind, ListEntry, ListMode, ObjectKey, ObjectMeta, ObjectPrefix, Validator,
    cloud::{CloudBackend, CloudOptions, CloudRoot, Scheme},
    model::{RunControl, SchemaBaseline, SealedRun, TableLayout, VersionManifest, decode_record},
};
use grv_types::{Name, Uuid};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fs::File,
    io::{Read, Write},
    os::unix::fs::PermissionsExt,
    path::Path,
    process::Command,
};

const AUTHORIZED_ROOT: &str = "gs://validation-gcs-bucket/validation";
const MAX_OBJECTS: usize = 256;
const MAX_STORED_BYTES: usize = 4 * 1024 * 1024;
const MAX_FILE_BYTES: usize = 512 * 1024;
const MAX_METADATA_BYTES: usize = 256 * 1024;

struct OwnedRoot {
    // The parent backend is used ONLY with the fresh UUID prefix, never a
    // parent-wide listing. This lets public Backend APIs inventory ALL objects,
    // including grv.json, without relying on a fixed list of expected paths.
    inventory_backend: CloudBackend,
    backend: CloudBackend,
    prefix: ObjectPrefix,
    root: String,
    account: String,
    project: String,
    cleaned: bool,
}
impl OwnedRoot {
    fn open() -> Self {
        let selected = CloudRoot::parse(
            &std::env::var("GRV_GCS_WRITE_TEST_ROOT").expect("explicit write root required"),
        )
        .unwrap();
        assert_eq!(selected.scheme, Scheme::Gcs);
        assert_eq!(
            selected.canonical(),
            AUTHORIZED_ROOT,
            "unauthorized write root"
        );
        let account = std::env::var("GRV_GCS_ACCOUNT").expect("explicit GCS account required");
        let project = std::env::var("GRV_GCS_PROJECT").expect("explicit GCS project required");
        assert!(!account.trim().is_empty() && !project.trim().is_empty());
        let options = CloudOptions {
            gcs_account: Some(account.clone()),
            gcs_project: Some(project.clone()),
            ..Default::default()
        };
        let uuid = Uuid::v4();
        let root = format!("{AUTHORIZED_ROOT}/{uuid}");
        eprintln!("Owned GCS fixture CLI test root: {root}");
        let mut owned = Self {
            inventory_backend: CloudBackend::open(AUTHORIZED_ROOT, options.clone()).unwrap(),
            backend: CloudBackend::open(&root, options).unwrap(),
            prefix: ObjectPrefix::new(format!("{uuid}/")).unwrap(),
            root,
            account,
            project,
            // Arm cleanup only after proving this UUID is fresh.
            cleaned: true,
        };
        assert!(
            owned.inventory().unwrap().is_empty(),
            "UUID root already exists"
        );
        owned.cleaned = false;
        owned
    }
    fn inventory(&self) -> grv_storage::Result<Vec<ObjectKey>> {
        self.inventory_backend
            .list(&self.prefix, ListMode::Recursive)?
            .into_iter()
            .map(|entry| match entry {
                ListEntry::Object(key) if key.as_str().starts_with(self.prefix.as_str()) => Ok(key),
                _ => Err(Error::new(
                    ErrorKind::Integrity,
                    "listing escaped owned UUID prefix",
                )),
            })
            .collect()
    }
    fn snapshot(&self) -> BTreeMap<String, Vec<u8>> {
        let keys = self.inventory().unwrap();
        assert!(keys.len() <= MAX_OBJECTS, "fixture object count exceeded");
        let mut total = 0;
        keys.into_iter()
            .map(|key| {
                let relative = key.as_str().strip_prefix(self.prefix.as_str()).unwrap();
                let key = ObjectKey::new(relative).unwrap();
                let head = self.backend.head(&key).unwrap();
                assert!(head.size.get() <= MAX_FILE_BYTES as u64);
                let (bytes, meta) = self.backend.read_bytes(&key, MAX_FILE_BYTES).unwrap();
                assert_eq!(meta, head);
                assert_eq!(bytes.len() as u64, head.size.get());
                total += bytes.len();
                assert!(total <= MAX_STORED_BYTES, "fixture byte budget exceeded");
                (relative.to_owned(), bytes)
            })
            .collect()
    }
    fn cleanup(&mut self) -> grv_storage::Result<()> {
        let mut failure = None;
        for key in self.inventory()? {
            // inventory() has already checked every key against the UUID slash
            // boundary. Delete is never called on the parent or a sibling.
            if let Err(error) = self.inventory_backend.delete(&key) {
                failure.get_or_insert(error);
                continue;
            }
            match self.inventory_backend.head(&key) {
                Err(error) if error.kind == ErrorKind::NotFound => {}
                Err(error) => {
                    failure.get_or_insert(error);
                }
                Ok(_) => {
                    failure
                        .get_or_insert(Error::new(ErrorKind::Integrity, "deleted object remains"));
                }
            }
        }
        let remaining = self.inventory()?;
        if let Some(error) = failure {
            return Err(error);
        }
        if !remaining.is_empty() {
            return Err(Error::new(
                ErrorKind::Integrity,
                "owned UUID prefix remains nonempty",
            ));
        }
        self.cleaned = true;
        eprintln!("Proved owned GCS UUID prefix absent: {}", self.root);
        Ok(())
    }
}
impl Drop for OwnedRoot {
    fn drop(&mut self) {
        if !self.cleaned
            && let Err(error) = self.cleanup()
        {
            if std::thread::panicking() {
                eprintln!("FAILED GCS panic cleanup at {}: {error}", self.root);
            } else {
                panic!("GCS cleanup failed at {}: {error}", self.root);
            }
        }
    }
}

// Independently verify persisted publication without allowing core verification
// to mutate storage. CLI-reported row counts alone are not evidence.
struct ReadOnly<'a>(&'a CloudBackend);
impl Backend for ReadOnly<'_> {
    fn get(&self, key: &ObjectKey, sink: &mut dyn Write) -> grv_storage::Result<ObjectMeta> {
        self.0.get(key, sink)
    }
    fn head(&self, key: &ObjectKey) -> grv_storage::Result<ObjectMeta> {
        self.0.head(key)
    }
    fn list(&self, prefix: &ObjectPrefix, mode: ListMode) -> grv_storage::Result<Vec<ListEntry>> {
        self.0.list(prefix, mode)
    }
    fn delete(&self, _: &ObjectKey) -> grv_storage::Result<()> {
        panic!("verification attempted delete")
    }
    fn conditional_create(
        &self,
        _: &ObjectKey,
        _: &mut dyn Read,
    ) -> grv_storage::Result<Validator> {
        panic!("verification attempted create")
    }
    fn conditional_put(
        &self,
        _: &ObjectKey,
        _: &Validator,
        _: &mut dyn Read,
    ) -> grv_storage::Result<Validator> {
        panic!("verification attempted put")
    }
}
fn record<T: grv_storage::model::Validate + for<'de> serde::Deserialize<'de>>(
    backend: &impl Backend,
    key: &str,
) -> T {
    decode_record(
        &backend
            .read_bytes(&ObjectKey::new(key).unwrap(), MAX_METADATA_BYTES)
            .unwrap()
            .0,
    )
    .unwrap()
}
fn verify_persisted(backend: &CloudBackend, scratch: &Path, published: &Value) {
    let store = Store::open(ReadOnly(backend)).unwrap();
    let dataset = Name::new("data").unwrap();
    let latest = revision::read_latest(&store, &dataset).unwrap().unwrap().0;
    assert_eq!(latest.revision.get(), 1);
    // A no-op may reserve a run/high-water number without committing a
    // second revision; revision and immutable version identities remain 1.
    assert!(latest.high_water.get() >= 1);
    assert!(latest.lease.is_none() && latest.pending.is_none());
    assert!(revision::committed(&store, &dataset, latest.revision, scratch).unwrap());
    let state = revision::read(&store, &dataset, latest.revision, scratch).unwrap();
    assert_eq!(state.state.len(), 2);
    let mut seen = Vec::new();
    for entry in state.state.values() {
        seen.push(entry.table.as_str().to_owned());
        assert_eq!(entry.version.get(), 1);
        assert_eq!(
            entry.run_id.as_str(),
            published["result"]["run_id"].as_str().unwrap()
        );
        let table = format!("datasets/data/{}", entry.table);
        let layout: TableLayout = record(&store.backend, &format!("{table}/.layout.json"));
        let baseline: SchemaBaseline = record(&store.backend, &format!("{table}/.schema.json"));
        assert_eq!(layout.table, entry.table);
        assert_eq!(baseline.table, entry.table);
        assert_eq!(baseline.columns.len(), 1);
        assert_eq!(baseline.columns[0].name, "value");
        let partition = layout.parse_partition(&entry.partition).unwrap();
        assert!(layout.partition_path(&partition).unwrap().is_empty());
        let version = format!("{table}/version={}", entry.version);
        let manifest: VersionManifest = record(&store.backend, &format!("{version}/manifest.json"));
        assert_eq!(manifest.table, entry.table);
        assert_eq!(manifest.partition, partition);
        assert_eq!(manifest.version, entry.version);
        assert_eq!(manifest.run_id, entry.run_id);
        let run: SealedRun = record(
            &store.backend,
            &format!("datasets/data/.runs/{}.json", entry.run_id),
        );
        let control: RunControl = record(
            &store.backend,
            &format!("datasets/data/.runs/{}.control.json", entry.run_id),
        );
        run.validate_control(&control).unwrap();
        run.validate_manifest(&manifest).unwrap();
        // The fixture emits one real schema-bearing Parquet file even for empty.
        assert_eq!(manifest.data_files.len(), 1);
        let mut rows = 0;
        let mut values = Vec::new();
        for file in &manifest.data_files {
            let key = ObjectKey::new(format!("{version}/{}", file.name)).unwrap();
            let head = store.backend.head(&key).unwrap();
            assert_eq!(head.size, file.size);
            assert!(head.size.get() <= MAX_FILE_BYTES as u64);
            let (bytes, meta) = store.backend.read_bytes(&key, MAX_FILE_BYTES).unwrap();
            assert_eq!(meta, head);
            assert_eq!(bytes.len() as u64, file.size.get());
            assert_eq!(grv_types::sha256(&bytes), file.sha256);
            let staged = tempfile::NamedTempFile::new_in(scratch).unwrap();
            std::fs::write(staged.path(), bytes).unwrap();
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(File::open(staged.path()).unwrap())
                    .unwrap();
            assert_eq!(builder.schema().fields().len(), 1);
            assert_eq!(builder.schema().field(0).name(), "value");
            assert_eq!(
                builder.schema().field(0).data_type(),
                &arrow_schema::DataType::Int64
            );
            assert_eq!(
                grv_core::contract::arrow_type(&baseline.columns[0].logical_type).unwrap(),
                arrow_schema::DataType::Int64
            );
            for batch in builder.with_batch_size(1024).build().unwrap() {
                let batch = batch.unwrap();
                rows += batch.num_rows() as u64;
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(column.null_count(), 0);
                values.extend(column.values().iter().copied());
            }
        }
        let expected = match entry.table.as_str() {
            "rows" => vec![1, 2, 3],
            "empty" => vec![],
            other => panic!("unexpected table {other}"),
        };
        assert_eq!(rows, manifest.row_count.get());
        assert_eq!(rows, expected.len() as u64);
        assert_eq!(values, expected);
    }
    seen.sort();
    assert_eq!(seen, ["empty", "rows"]);
}

fn cli(owned: &OwnedRoot, adapters: &Path, args: &[&str], offline: bool) -> Value {
    let mut command = Command::new(env!("CARGO_BIN_EXE_grv-conformance-cli"));
    command
        .arg("--json")
        .args(args)
        .env("GRV_ADAPTERS_DIR", adapters)
        .env("GRV_GCS_ACCOUNT", &owned.account)
        .env("GRV_GCS_PROJECT", &owned.project);
    if offline {
        // CloudBackend GCS credentials require gcloud. Removing PATH means even
        // an accidental attempt to open the remote store cannot authenticate.
        command
            .env("PATH", "")
            .env("GRV_GCS_ACCOUNT", "unavailable-replay-account")
            .env("GRV_GCS_PROJECT", "unavailable-replay-project");
    }
    let output = command.output().unwrap();
    let value: Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(&output.stderr)));
    validate_output(&value).unwrap();
    assert_eq!(output.status.success(), value["ok"] == true, "{value}");
    value
}

#[test]
#[ignore = "writes only to authorized GCS UUID child; requires GRV_GCS_WRITE_TEST_ROOT, GRV_GCS_ACCOUNT and GRV_GCS_PROJECT"]
fn live_cli_gcs_capture_publication_noop_and_source_free_terminal_replay() {
    let mut owned = OwnedRoot::open();
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let temp = tempfile::tempdir_in(workspace).unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let adapters = temp.path().join("adapters");
    let package = adapters.join("fixture");
    std::fs::create_dir_all(&package).unwrap();
    std::fs::write(package.join("adapter.toml"), format!(
        "name = 'fixture'\nversion = '0.1.0'\ninterface_versions = [1]\nbinding_schema_version = 1\nentrypoint = {:?}\n", env!("CARGO_BIN_EXE_fixture")
    )).unwrap();
    let decl = temp.path().join("push.yml");
    std::fs::write(&decl, "declaration_version: 1\nkind: push\ndataset: data\nadapter: fixture\nconnection: {}\ntables:\n  - name: rows\n    source: {}\n    columns:\n      - {name: value, source: value, type: int64}\n  - name: empty\n    source: {}\n    columns:\n      - {name: value, source: value, type: int64}\n").unwrap();
    let state = temp.path().join("state");
    let initialized = cli(&owned, &adapters, &["init", "--grv", &owned.root], false);
    assert_eq!(initialized["ok"], true, "{initialized}");
    let attempt = Uuid::v4();
    let second_attempt = Uuid::v4();
    let invoke = |owned: &OwnedRoot, id: &str, offline| {
        cli(
            owned,
            &adapters,
            &[
                "push",
                "--grv",
                &owned.root,
                "--decl",
                decl.to_str().unwrap(),
                "--state",
                state.to_str().unwrap(),
                "--attempt",
                id,
            ],
            offline,
        )
    };
    let published = invoke(&owned, attempt.as_str(), false);
    assert_eq!(published["ok"], true, "{published}");
    assert_eq!(published["result"]["outcome"]["kind"], "published");
    assert_eq!(published["result"]["outcome"]["revision"], "1");
    assert_eq!(published["result"]["replayed"], false);
    assert_eq!(published["result"]["adapter_result"]["rows"], "3");
    verify_persisted(&owned.backend, temp.path(), &published);
    let before = owned.snapshot();
    let noop = invoke(&owned, second_attempt.as_str(), false);
    assert_eq!(noop["ok"], true, "{noop}");
    assert_eq!(noop["result"]["outcome"]["kind"], "no-op");
    assert_eq!(noop["result"]["outcome"]["revision"], "1");
    assert!(noop["result"]["outcome"]["operation_id"].is_null());
    assert_eq!(noop["result"]["replayed"], false);
    assert_eq!(noop["result"]["adapter_result"]["rows"], "3");
    let after = owned.snapshot();
    // No-op may append run history and update the LATEST mutation validator,
    // but must preserve every immutable publication byte and allocate no version.
    for (key, bytes) in &before {
        if key != "datasets/data/.states/LATEST" {
            assert_eq!(after.get(key), Some(bytes), "no-op changed {key}");
        }
    }
    assert!(
        after
            .keys()
            .filter(|key| key.contains("/version="))
            .all(|key| before.contains_key(key))
    );
    verify_persisted(&owned.backend, temp.path(), &published);

    std::fs::remove_dir_all(&adapters).unwrap();
    owned.cleanup().unwrap();
    assert_eq!(
        owned
            .backend
            .head(&ObjectKey::new("grv.json").unwrap())
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
    assert!(owned.inventory().unwrap().is_empty());
    // Keep panic cleanup armed during replay too, so an unexpected recreation
    // cannot leak this owned prefix even if a later assertion fails.
    owned.cleaned = false;
    for (id, original) in [
        (attempt.as_str(), &published),
        (second_attempt.as_str(), &noop),
    ] {
        let replay = invoke(&owned, id, true);
        assert_eq!(replay["ok"], true, "{replay}");
        let mut expected = original["result"].clone();
        expected["replayed"] = true.into();
        assert_eq!(replay["result"], expected);
    }
    // Also reconstruct the window after durable core outcome, before the outer
    // CLI terminal record. Both the source package and all GRV objects are gone.
    let journal = state
        .join("push")
        .join(attempt.as_str())
        .join("journal.json");
    let mut recorded: Value = serde_json::from_slice(&std::fs::read(&journal).unwrap()).unwrap();
    assert!(!recorded["evidence"]["progress"]["push"]["terminal"].is_null());
    recorded["evidence"]["terminal"] = Value::Null;
    std::fs::write(&journal, serde_json::to_vec(&recorded).unwrap()).unwrap();
    File::open(&journal).unwrap().sync_all().unwrap();
    let replay = invoke(&owned, attempt.as_str(), true);
    assert_eq!(replay["ok"], true, "{replay}");
    let mut expected = published["result"].clone();
    expected["replayed"] = true.into();
    assert_eq!(replay["result"], expected);
    assert!(
        owned.inventory().unwrap().is_empty(),
        "replay recreated remote objects"
    );
    owned.cleanup().unwrap();
}
