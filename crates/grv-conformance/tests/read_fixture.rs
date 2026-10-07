//! This verification path has no backend write capability, including for live GCS.
use arrow_array::{Array, Int64Array};
use grv_core::{revision, store::Store};
use grv_storage::{
    Backend, ListEntry, ListMode, ObjectKey, ObjectMeta, ObjectPrefix, Validator,
    model::{RunControl, SchemaBaseline, SealedRun, TableLayout, VersionManifest, decode_record},
};
use grv_types::Name;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::{
    fs::File,
    io::{Read, Write},
    os::unix::fs::PermissionsExt,
    path::Path,
    process::Command,
};

struct ReadOnly<B>(B, Option<grv_storage::cloud::CloudRoot>);
impl<B: Backend> Backend for ReadOnly<B> {
    fn s3_data_uri(&self, key: &ObjectKey) -> grv_storage::Result<Option<String>> {
        match &self.1 {
            Some(root) => root.s3_data_uri(key),
            None => self.0.s3_data_uri(key),
        }
    }
    fn get(&self, key: &ObjectKey, sink: &mut dyn Write) -> grv_storage::Result<ObjectMeta> {
        self.0.get(key, sink)
    }
    fn head(&self, key: &ObjectKey) -> grv_storage::Result<ObjectMeta> {
        self.0.head(key)
    }
    fn list(&self, prefix: &ObjectPrefix, mode: ListMode) -> grv_storage::Result<Vec<ListEntry>> {
        self.0.list(prefix, mode)
    }
    fn conditional_create(
        &self,
        _: &ObjectKey,
        _: &mut dyn Read,
    ) -> grv_storage::Result<Validator> {
        panic!("read verification attempted create")
    }
    fn conditional_put(
        &self,
        _: &ObjectKey,
        _: &Validator,
        _: &mut dyn Read,
    ) -> grv_storage::Result<Validator> {
        panic!("read verification attempted put")
    }
    fn delete(&self, _: &ObjectKey) -> grv_storage::Result<()> {
        panic!("read verification attempted delete")
    }
}
fn record<T: grv_storage::model::Validate + for<'de> serde::Deserialize<'de>>(
    backend: &impl Backend,
    key: &str,
) -> T {
    decode_record(
        &backend
            .read_bytes(&ObjectKey::new(key).unwrap(), 1024 * 1024)
            .unwrap()
            .0,
    )
    .unwrap()
}
fn verify_fixture(backend: impl Backend, dataset: &str) {
    let temp = tempfile::tempdir().unwrap();
    let store = Store::open(ReadOnly(backend, None)).unwrap();
    let dataset = Name::new(dataset).unwrap();
    let latest_selector: grv_types::RequestedRevision =
        serde_json::from_value(serde_json::json!("latest")).unwrap();
    let selected = grv_core::source::verify(
        &store,
        &dataset,
        &latest_selector,
        &["rows", "empty"].map(|table| grv_core::source::TableSelection {
            table: Name::new(table).unwrap(),
            partitions: None,
            expect_columns: None,
            expect_partition_keys: None,
            prior_source_contract: None,
        }),
        temp.path(),
    )
    .unwrap();
    assert_eq!(selected.revision.get(), 1);
    assert_eq!(selected.files.len(), 2);
    assert!(
        selected
            .files
            .iter()
            .all(|file| Path::new(&file.location).is_file())
    );
    assert!(
        selected
            .tables
            .iter()
            .all(|table| table.contract.columns.len() == 1)
    );
    let latest = revision::read_latest(&store, &dataset).unwrap().unwrap().0;
    assert_eq!(latest.revision.get(), 1);
    assert!(revision::committed(&store, &dataset, latest.revision, temp.path()).unwrap());
    let state = revision::read(&store, &dataset, latest.revision, temp.path()).unwrap();
    assert_eq!(state.state.len(), 2);
    for entry in state.state.values() {
        assert!(matches!(entry.table.as_str(), "rows" | "empty"));
        let table = format!("datasets/{dataset}/{}", entry.table);
        let layout: TableLayout = record(&store.backend, &format!("{table}/.layout.json"));
        let baseline: SchemaBaseline = record(&store.backend, &format!("{table}/.schema.json"));
        assert_eq!(layout.table, entry.table);
        assert_eq!(baseline.table, entry.table);
        let partition = layout.parse_partition(&entry.partition).unwrap();
        let partition_path = layout.partition_path(&partition).unwrap();
        let group = if partition_path.is_empty() {
            table.clone()
        } else {
            format!("{table}/{partition_path}")
        };
        let version = format!("{group}/version={}", entry.version);
        let manifest: VersionManifest = record(&store.backend, &format!("{version}/manifest.json"));
        assert_eq!(manifest.table, entry.table);
        assert_eq!(manifest.partition, partition);
        assert_eq!(manifest.version, entry.version);
        assert_eq!(manifest.run_id, entry.run_id);
        let run: SealedRun = record(
            &store.backend,
            &format!("datasets/{dataset}/.runs/{}.json", entry.run_id),
        );
        let control: RunControl = record(
            &store.backend,
            &format!("datasets/{dataset}/.runs/{}.control.json", entry.run_id),
        );
        run.validate_control(&control).unwrap();
        run.validate_manifest(&manifest).unwrap();
        let mut rows = 0;
        let mut values = Vec::new();
        for file in &manifest.data_files {
            let key = ObjectKey::new(format!("{version}/{}", file.name)).unwrap();
            let head = store.backend.head(&key).unwrap();
            assert_eq!(head.size, file.size);
            assert!(
                head.size.get() <= 8 * 1024 * 1024,
                "fixture file unexpectedly large"
            );
            let mut staged = tempfile::NamedTempFile::new_in(temp.path()).unwrap();
            assert_eq!(store.backend.get(&key, staged.as_file_mut()).unwrap(), head);
            assert_eq!(
                std::fs::metadata(staged.path()).unwrap().len(),
                file.size.get()
            );
            assert_eq!(
                grv_types::sha256(&std::fs::read(staged.path()).unwrap()),
                file.sha256
            );
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(File::open(staged.path()).unwrap())
                    .unwrap();
            assert_eq!(builder.schema().fields().len(), baseline.columns.len());
            for (field, column) in builder.schema().fields().iter().zip(&baseline.columns) {
                assert_eq!(field.name(), &column.name);
                assert_eq!(
                    field.data_type(),
                    &grv_core::contract::arrow_type(&column.logical_type).unwrap()
                );
            }
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
        assert_eq!(rows, manifest.row_count.get());
        assert_eq!(
            values,
            if entry.table.as_str() == "rows" {
                vec![1, 2, 3]
            } else {
                vec![]
            }
        );
    }
}
#[test]
fn complete_published_fixture_is_verified_without_backend_mutations() {
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
    std::fs::write(package.join("adapter.toml"), format!("name = 'fixture'\nversion = '0.1.0'\ninterface_versions = [1]\nbinding_schema_version = 1\nentrypoint = {:?}\n", env!("CARGO_BIN_EXE_fixture"))).unwrap();
    let root = temp.path().join("store");
    let declaration = temp.path().join("push.yml");
    std::fs::write(&declaration, "declaration_version: 1\nkind: push\ndataset: grv_read_test\nadapter: fixture\nconnection: {}\ntables:\n  - name: rows\n    source: {}\n    columns:\n      - {name: value, source: value, type: int64}\n  - name: empty\n    source: {}\n    columns:\n      - {name: value, source: value, type: int64}\n").unwrap();
    let state = temp.path().join("state");
    for args in [
        vec!["init", "--grv", root.to_str().unwrap()],
        vec![
            "push",
            "--grv",
            root.to_str().unwrap(),
            "--decl",
            declaration.to_str().unwrap(),
            "--state",
            state.to_str().unwrap(),
        ],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_grv-conformance-cli"))
            .arg("--json")
            .args(args)
            .env("GRV_ADAPTERS_DIR", &adapters)
            .output()
            .unwrap();
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert!(output.status.success(), "{result}");
        grv_adapter_host::validate_output(&result).unwrap();
    }
    verify_fixture(
        grv_storage::LocalBackend::open(&root).unwrap(),
        "grv_read_test",
    );
    exercise_exact_remote_coordinates(&root, temp.path());
    exercise_selection_boundaries(&root, temp.path());
}
fn exercise_exact_remote_coordinates(root: &Path, scratch: &Path) {
    use grv_adapter_api::FileAccess;
    use grv_core::source::{TableSelection, verify_with_access};
    let dataset = Name::new("grv_read_test").unwrap();
    let selector = serde_json::from_value(serde_json::json!("latest")).unwrap();
    let selection = || {
        ["rows", "empty"].map(|table| TableSelection {
            table: Name::new(table).unwrap(),
            partitions: None,
            expect_columns: None,
            expect_partition_keys: None,
            prior_source_contract: None,
        })
    };
    let local = Store::open(ReadOnly(
        grv_storage::LocalBackend::open(root).unwrap(),
        None,
    ))
    .unwrap();
    assert_eq!(
        verify_with_access(
            &local,
            &dataset,
            &selector,
            &selection(),
            scratch,
            FileAccess::S3View
        )
        .err()
        .unwrap()
        .code,
        grv_types::ErrorCode::UnsupportedCapability
    );
    // Provider-free fixture: local immutable bytes stand in for S3, while the
    // real CloudRoot locator supplies exact remote coordinates. Every mutation
    // is forbidden; remote coordinates require the same full integrity checks.
    let mapped = Store::open(ReadOnly(
        grv_storage::LocalBackend::open(root).unwrap(),
        Some(grv_storage::cloud::CloudRoot::parse("s3://test-bucket/read%20only").unwrap()),
    ))
    .unwrap();
    let verified = verify_with_access(
        &mapped,
        &dataset,
        &selector,
        &selection(),
        scratch,
        FileAccess::S3View,
    )
    .unwrap();
    assert_eq!(verified.files.len(), 2);
    for file in &verified.files {
        assert_eq!(file.access, FileAccess::S3View);
        assert_eq!(
            file.location,
            format!(
                "s3://test-bucket/read%20only/datasets/grv_read_test/{}/version=1/data.parquet",
                file.table
            )
        );
        file.validate().unwrap();
    }
    let path = root.join("datasets/grv_read_test/rows/version=1/data.parquet");
    let original = std::fs::read(&path).unwrap();
    let mut damaged = original.clone();
    damaged[4] ^= 1;
    std::fs::write(&path, &damaged).unwrap();
    assert_eq!(
        verify_with_access(
            &mapped,
            &dataset,
            &selector,
            &selection(),
            scratch,
            FileAccess::S3View
        )
        .err()
        .unwrap()
        .code,
        grv_types::ErrorCode::IntegrityFailure
    );
    std::fs::write(path, original).unwrap();
}
fn exercise_selection_boundaries(root: &Path, scratch: &Path) {
    use grv_adapter_api::Column;
    use grv_core::source::{TableSelection, verify};
    use grv_storage::model::encode_record;
    use grv_types::ErrorCode;
    let backend = grv_storage::LocalBackend::open(root).unwrap();
    let schema_key = ObjectKey::new("datasets/grv_read_test/rows/.schema.json").unwrap();
    let (bytes, metadata) = backend.read_bytes(&schema_key, 1024 * 1024).unwrap();
    let mut baseline: SchemaBaseline = decode_record(&bytes).unwrap();
    baseline.columns.push(grv_storage::model::StorageColumn {
        name: "later".into(),
        logical_type: serde_json::json!("string"),
        ext: None,
    });
    baseline.mutation_id = grv_types::Uuid::v4();
    backend
        .put_bytes(
            &schema_key,
            &metadata.validator,
            &encode_record(&baseline).unwrap(),
        )
        .unwrap();
    let store = Store::open(ReadOnly(backend, None)).unwrap();
    let dataset = Name::new("grv_read_test").unwrap();
    let latest: grv_types::RequestedRevision =
        serde_json::from_value(serde_json::json!("latest")).unwrap();
    let selection = |table: &str, partitions, columns| TableSelection {
        table: Name::new(table).unwrap(),
        partitions,
        expect_columns: columns,
        expect_partition_keys: None,
        prior_source_contract: None,
    };
    let historical = verify(
        &store,
        &dataset,
        &latest,
        &[selection("rows", None, None)],
        scratch,
    )
    .unwrap();
    assert_eq!(
        historical.tables[0].contract.columns.len(),
        1,
        "baseline growth cannot fabricate a historical column"
    );
    let template: Vec<_> = baseline
        .columns
        .iter()
        .map(|column| Column {
            name: column.name.clone(),
            logical_type: column.logical_type.clone(),
        })
        .collect();
    assert_eq!(
        verify(
            &store,
            &dataset,
            &latest,
            &[selection("rows", None, Some(template.clone()))],
            scratch
        )
        .err()
        .unwrap()
        .code,
        ErrorCode::InvalidDeclaration
    );
    let empty = verify(
        &store,
        &dataset,
        &latest,
        &[selection("rows", Some(vec![]), Some(template))],
        scratch,
    )
    .unwrap();
    assert!(empty.files.is_empty());
    assert!(empty.tables[0].partitions.is_empty());
    assert_eq!(empty.tables[0].contract.columns.len(), 2);
    assert_eq!(
        verify(
            &store,
            &dataset,
            &latest,
            &[selection("rows", Some(vec![]), None)],
            scratch
        )
        .err()
        .unwrap()
        .code,
        ErrorCode::InvalidDeclaration
    );
    let prior = TableSelection {
        prior_source_contract: Some(historical.tables[0].contract.clone()),
        ..selection("rows", Some(vec![]), None)
    };
    assert_eq!(
        verify(&store, &dataset, &latest, &[prior], scratch)
            .unwrap()
            .tables[0]
            .contract
            .columns
            .len(),
        1
    );
    // An unselected tombstone must not make the selected empty table unavailable.
    let backend = grv_storage::LocalBackend::open(root).unwrap();
    backend
        .create_bytes(
            &ObjectKey::new("datasets/grv_read_test/rows/version=1/.pruned").unwrap(),
            b"tombstone",
        )
        .unwrap();
    let store = Store::open(ReadOnly(backend, None)).unwrap();
    assert_eq!(
        verify(
            &store,
            &dataset,
            &latest,
            &[selection("empty", None, None)],
            scratch
        )
        .unwrap()
        .files
        .len(),
        1
    );
    assert_eq!(
        verify(
            &store,
            &dataset,
            &latest,
            &[selection("rows", None, None)],
            scratch
        )
        .err()
        .unwrap()
        .code,
        ErrorCode::Unavailable
    );
    let zero = grv_types::RequestedRevision::Revision(grv_types::U64::new(0).unwrap());
    let template = historical.tables[0].contract.columns.clone();
    assert!(
        verify(
            &store,
            &dataset,
            &zero,
            &[selection("rows", None, Some(template))],
            scratch
        )
        .unwrap()
        .files
        .is_empty()
    );
    let uncommitted = grv_types::RequestedRevision::Revision(grv_types::U64::new(2).unwrap());
    assert_eq!(
        verify(
            &store,
            &dataset,
            &uncommitted,
            &[selection("empty", None, None)],
            scratch
        )
        .err()
        .unwrap()
        .code,
        ErrorCode::NotFound
    );
}

#[test]
#[ignore = "read-only: requires the uploaded complete GCS fixture"]
fn live_gcs_complete_published_fixture_read_only() {
    use grv_storage::cloud::{CloudBackend, CloudOptions, CloudRoot, Scheme};
    let root = std::env::var("GRV_GCS_TEST_ROOT").expect("explicit fixture prefix required");
    let parsed = CloudRoot::parse(&root).unwrap();
    assert_eq!(parsed.scheme, Scheme::Gcs);
    let backend = CloudBackend::open(
        &root,
        CloudOptions {
            gcs_account: Some(std::env::var("GRV_GCS_ACCOUNT").expect("explicit account required")),
            gcs_project: Some(std::env::var("GRV_GCS_PROJECT").expect("explicit project required")),
            ..Default::default()
        },
    )
    .unwrap();
    verify_fixture(backend, "grv_read_test");
}
