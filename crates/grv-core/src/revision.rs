//! Revision Parquet and committed-chain metadata. Historic versions may be
//! pruned; reading the chain never opens their data or manifests.
use crate::store::{Result, Store, backend_error, public_error};
use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use grv_storage::{
    Backend, ErrorKind, ListMode, ObjectKey, ObjectPrefix, Validator,
    model::{Counter, Latest, TableLayout, decode_record},
};
use grv_types::{ErrorCode, Name, RunId, Timestamp};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::{
        metadata::KeyValue,
        properties::{EnabledStatistics, WriterProperties},
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::Write,
    path::Path,
    sync::Arc,
};
const STATE_MEMORY_LIMIT: usize = 128 * 1024 * 1024;
const RECORD_LIMIT: usize = 64 * 1024 * 1024;
fn integrity(message: &str) -> grv_types::PublicError {
    public_error(ErrorCode::IntegrityFailure, message)
}
fn io(error: std::io::Error) -> grv_types::PublicError {
    public_error(ErrorCode::BackendFailure, error.to_string())
}
fn parquet_error(error: parquet::errors::ParquetError) -> grv_types::PublicError {
    public_error(ErrorCode::IntegrityFailure, error.to_string())
}
pub fn latest_key(dataset: &Name) -> ObjectKey {
    ObjectKey::new(format!("datasets/{dataset}/.states/LATEST")).unwrap()
}
pub fn revision_key(dataset: &Name, revision: Counter) -> ObjectKey {
    ObjectKey::new(format!(
        "datasets/{dataset}/.states/revisions/revision={revision}/data.parquet"
    ))
    .unwrap()
}
pub fn revision_prefix(dataset: &Name) -> ObjectPrefix {
    ObjectPrefix::new(format!("datasets/{dataset}/.states/revisions/")).unwrap()
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub table: Name,
    pub partition: String,
    pub version: Counter,
    pub run_id: RunId,
}
pub type State = BTreeMap<(Name, String), Entry>;
#[derive(Debug, Clone)]
pub struct Revision {
    pub revision: Counter,
    pub previous_revision: Counter,
    pub operation_id: RunId,
    pub created_at: Timestamp,
    pub state: State,
}
pub fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("table", DataType::Utf8, false),
        Field::new("partition", DataType::Utf8, false),
        Field::new("version", DataType::Int64, false),
        Field::new("run_id", DataType::Utf8, false),
    ]))
}
pub fn read_latest<B: Backend>(
    store: &Store<B>,
    dataset: &Name,
) -> Result<Option<(Latest, Validator)>> {
    match store.backend.read_bytes(&latest_key(dataset), RECORD_LIMIT) {
        Ok((bytes, meta)) => Ok(Some((
            decode_record(&bytes).map_err(backend_error)?,
            meta.validator,
        ))),
        Err(error) if error.kind == ErrorKind::NotFound => {
            if !store
                .backend
                .list(&revision_prefix(dataset), ListMode::Recursive)
                .map_err(backend_error)?
                .is_empty()
            {
                Err(public_error(
                    ErrorCode::ProtocolFailure,
                    "missing LATEST with existing revision objects",
                ))
            } else {
                Ok(None)
            }
        }
        Err(error) => Err(backend_error(error)),
    }
}

pub fn encode(revision: &Revision, parent: &Path) -> Result<tempfile::NamedTempFile> {
    if revision.revision.get() == 0 || revision.previous_revision >= revision.revision {
        return Err(integrity(
            "revision must be positive and follow a smaller predecessor",
        ));
    }
    if revision.state.iter().any(|(pair, entry)| {
        pair != &(entry.table.clone(), entry.partition.clone()) || entry.version.get() == 0
    }) {
        return Err(integrity("revision state key differs from entry identity"));
    }
    let mut output = tempfile::NamedTempFile::new_in(parent).map_err(io)?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_max_row_group_row_count(Some(8192))
        .set_max_row_group_bytes(Some(16 * 1024 * 1024))
        .set_created_by("grv/parquet-58.3.0/revision-v1".into())
        .set_key_value_metadata(Some(vec![
            KeyValue::new("grv.revision".into(), revision.revision.to_string()),
            KeyValue::new(
                "grv.previous_revision".into(),
                revision.previous_revision.to_string(),
            ),
            KeyValue::new(
                "grv.created_at".into(),
                revision.created_at.as_str().to_owned(),
            ),
            KeyValue::new(
                "grv.operation_id".into(),
                revision.operation_id.as_str().to_owned(),
            ),
        ]))
        .build();
    {
        let mut writer = ArrowWriter::try_new(output.as_file_mut(), schema(), Some(properties))
            .map_err(parquet_error)?;
        let entries: Vec<_> = revision.state.values().collect();
        for entries in entries.chunks(4096) {
            if entries.iter().any(|entry| {
                entry.version.get() == 0
                    || !revision
                        .state
                        .contains_key(&(entry.table.clone(), entry.partition.clone()))
            }) {
                return Err(integrity("revision entry has invalid identity"));
            }
            let batch = RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(StringArray::from(
                        entries.iter().map(|e| e.table.as_str()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        entries
                            .iter()
                            .map(|e| e.partition.as_str())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        entries
                            .iter()
                            .map(|e| e.version.get() as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        entries
                            .iter()
                            .map(|e| e.run_id.as_str())
                            .collect::<Vec<_>>(),
                    )),
                ],
            )
            .map_err(|e| integrity(&e.to_string()))?;
            writer.write(&batch).map_err(parquet_error)?;
        }
        writer.close().map_err(parquet_error)?;
    }
    output.flush().map_err(io)?;
    output.as_file().sync_all().map_err(io)?;
    Ok(output)
}

pub fn read<B: Backend>(
    store: &Store<B>,
    dataset: &Name,
    number: Counter,
    parent: &Path,
) -> Result<Revision> {
    if number.get() == 0 {
        return Err(integrity(
            "revision0 is an empty-state sentinel, not a Parquet object",
        ));
    }
    let mut staged = tempfile::NamedTempFile::new_in(parent).map_err(io)?;
    store
        .backend
        .get(&revision_key(dataset, number), staged.as_file_mut())
        .map_err(backend_error)?;
    staged.as_file().sync_all().map_err(io)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(staged.path()).map_err(io)?)
        .map_err(parquet_error)?;
    let expected = schema();
    if builder.schema().fields().len() != 4
        || builder
            .schema()
            .fields()
            .iter()
            .zip(expected.fields())
            .any(|(a, b)| a.name() != b.name() || a.data_type() != b.data_type())
    {
        return Err(integrity("revision Parquet has an invalid row schema"));
    }
    let metadata = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .ok_or_else(|| integrity("revision metadata missing"))?;
    let mut values = BTreeMap::new();
    for item in metadata {
        if item.key.starts_with("grv.")
            && values
                .insert(
                    item.key.as_str(),
                    item.value
                        .as_deref()
                        .ok_or_else(|| integrity("revision metadata has no value"))?,
                )
                .is_some()
        {
            return Err(integrity("duplicate revision metadata"));
        }
    }
    let get = |key| {
        values
            .get(key)
            .copied()
            .ok_or_else(|| integrity("required revision metadata missing"))
    };
    let count = |value: &str| -> Result<Counter> {
        let number = value
            .parse::<u64>()
            .ok()
            .filter(|number| number.to_string() == value)
            .ok_or_else(|| integrity("noncanonical revision number"))?;
        Counter::new(number).map_err(backend_error)
    };
    let recorded = count(get("grv.revision")?)?;
    let previous_revision = count(get("grv.previous_revision")?)?;
    if recorded != number || previous_revision >= recorded {
        return Err(integrity(
            "revision metadata differs from path or predecessor order",
        ));
    }
    let operation_id =
        RunId::new(get("grv.operation_id")?).map_err(|e| integrity(&e.to_string()))?;
    let created_at =
        Timestamp::new(get("grv.created_at")?).map_err(|e| integrity(&e.to_string()))?;
    let reader = builder
        .with_batch_size(4096)
        .build()
        .map_err(parquet_error)?;
    let mut state = State::new();
    let mut layouts: BTreeMap<Name, TableLayout> = BTreeMap::new();
    let mut memory = 0usize;
    for batch in reader {
        let batch = batch.map_err(|e| integrity(&e.to_string()))?;
        let tables = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let partitions = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let versions = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let runs = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for index in 0..batch.num_rows() {
            if batch.columns().iter().any(|array| array.is_null(index))
                || versions.value(index) <= 0
            {
                return Err(integrity(
                    "revision entries cannot have nulls or nonpositive versions",
                ));
            }
            let table = Name::new(tables.value(index)).map_err(|e| integrity(&e.to_string()))?;
            let partition = partitions.value(index).to_owned();
            if !layouts.contains_key(&table) {
                let key =
                    ObjectKey::new(format!("datasets/{dataset}/{table}/.layout.json")).unwrap();
                let (bytes, _) = store
                    .backend
                    .read_bytes(&key, RECORD_LIMIT)
                    .map_err(backend_error)?;
                let layout: TableLayout = decode_record(&bytes).map_err(backend_error)?;
                if layout.table != table {
                    return Err(integrity("layout table differs from path"));
                }
                layouts.insert(table.clone(), layout);
            }
            layouts[&table]
                .parse_partition(&partition)
                .map_err(backend_error)?;
            let entry = Entry {
                table: table.clone(),
                partition: partition.clone(),
                version: Counter::new(versions.value(index) as u64).map_err(backend_error)?,
                run_id: RunId::new(runs.value(index)).map_err(|e| integrity(&e.to_string()))?,
            };
            memory = memory.saturating_add(256 + table.as_str().len() * 2 + partition.len() * 2);
            if memory > STATE_MEMORY_LIMIT {
                return Err(public_error(
                    ErrorCode::UnsupportedCapability,
                    "revision state exceeds128MiB supported metadata workspace",
                ));
            }
            if state.insert((table, partition), entry).is_some() {
                return Err(integrity("duplicate revision table/partition"));
            }
        }
    }
    Ok(Revision {
        revision: number,
        previous_revision,
        operation_id,
        created_at,
        state,
    })
}

/// Walk only committed predecessor edges. A filename, reservation or orphan
/// revision never proves a publication committed.
pub fn committed<B: Backend>(
    store: &Store<B>,
    dataset: &Name,
    number: Counter,
    parent: &Path,
) -> Result<bool> {
    let latest =
        read_latest(store, dataset)?.map_or(Counter::from(0), |(record, _)| record.revision);
    if number.get() == 0 {
        return Ok(true);
    }
    let mut current = latest;
    let mut seen = BTreeSet::new();
    while current.get() != 0 {
        if current < number {
            return Ok(false);
        }
        if !seen.insert(current) {
            return Err(integrity("cyclic committed revision chain"));
        }
        let revision = read(store, dataset, current, parent).map_err(|error| {
            if error.code == ErrorCode::NotFound {
                public_error(
                    ErrorCode::Unavailable,
                    "committed revision object is missing",
                )
            } else {
                error
            }
        })?;
        if current == number {
            return Ok(true);
        }
        current = revision.previous_revision;
    }
    Ok(false)
}

/// Compare every transition, including changes that were later reversed.
pub fn check_run_conflicts<B: Backend>(
    store: &Store<B>,
    dataset: &Name,
    base: Counter,
    predecessor: Counter,
    pairs: &BTreeSet<(Name, String)>,
    parent: &Path,
) -> Result<()> {
    let mut current = predecessor;
    let mut transitions = Vec::new();
    let mut memory = 0usize;
    while current != base {
        if current.get() == 0 || current < base {
            return Err(public_error(
                ErrorCode::StateConflict,
                "run base is not on the committed predecessor chain",
            ));
        }
        let revision = read(store, dataset, current, parent)?;
        // Keep only contributed pairs and membership; never retain complete
        // historical snapshots or reopen historical version data.
        let selected: BTreeMap<_, _> = pairs
            .iter()
            .map(|pair| {
                (
                    pair.clone(),
                    revision.state.get(pair).map(|entry| entry.version),
                )
            })
            .collect();
        let tables: BTreeSet<_> = revision
            .state
            .keys()
            .map(|(table, _)| table.clone())
            .collect();
        memory += selected.len() * 256 + tables.len() * 96;
        if memory > STATE_MEMORY_LIMIT {
            return Err(public_error(
                ErrorCode::UnsupportedCapability,
                "conflict history exceeds supported metadata workspace",
            ));
        }
        transitions.push((selected, tables));
        current = revision.previous_revision;
    }
    let state = if base.get() == 0 {
        State::new()
    } else {
        read(store, dataset, base, parent)?.state
    };
    let mut previous: BTreeMap<_, _> = pairs
        .iter()
        .map(|pair| (pair.clone(), state.get(pair).map(|entry| entry.version)))
        .collect();
    let base_tables: BTreeSet<_> = state.keys().map(|(table, _)| table.clone()).collect();
    for (next, membership) in transitions.into_iter().rev() {
        if next != previous
            || pairs
                .iter()
                .any(|(table, _)| base_tables.contains(table) && !membership.contains(table))
        {
            return Err(public_error(
                ErrorCode::StateConflict,
                "run output changed in an intervening committed revision",
            ));
        }
        previous = next;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{clock::new_run_id, store::InitOptions};
    use grv_storage::{LocalBackend, model::encode_record};
    fn now() -> Timestamp {
        Timestamp::new("2026-10-06T00:00:00Z").unwrap()
    }
    fn dataset() -> Name {
        Name::new("data").unwrap()
    }
    fn table() -> Name {
        Name::new("rows").unwrap()
    }
    fn setup() -> (tempfile::TempDir, Store<LocalBackend>) {
        let temp = tempfile::tempdir().unwrap();
        let (store, _) = Store::initialize(
            LocalBackend::open(temp.path()).unwrap(),
            InitOptions::default(),
        )
        .unwrap();
        let layout = TableLayout {
            table: table(),
            partition_keys: vec![],
            extensions: None,
        };
        store
            .backend
            .create_bytes(
                &ObjectKey::new("datasets/data/rows/.layout.json").unwrap(),
                &encode_record(&layout).unwrap(),
            )
            .unwrap();
        (temp, store)
    }
    fn revision(
        store: &Store<LocalBackend>,
        parent: &Path,
        number: u32,
        previous: u32,
        selected: Option<u32>,
    ) {
        let mut state = State::new();
        if let Some(version) = selected {
            let entry = Entry {
                table: table(),
                partition: String::new(),
                version: version.into(),
                run_id: new_run_id(&now()).unwrap(),
            };
            state.insert((table(), String::new()), entry);
        }
        let revision = Revision {
            revision: number.into(),
            previous_revision: previous.into(),
            operation_id: new_run_id(&now()).unwrap(),
            created_at: now(),
            state,
        };
        let file = encode(&revision, parent).unwrap();
        store
            .backend
            .conditional_create(
                &revision_key(&dataset(), number.into()),
                &mut File::open(file.path()).unwrap(),
            )
            .unwrap();
    }
    fn latest(store: &Store<LocalBackend>, number: u32) {
        let mut latest = Latest::empty();
        latest.revision = number.into();
        latest.high_water = number.into();
        store
            .backend
            .create_bytes(&latest_key(&dataset()), &encode_record(&latest).unwrap())
            .unwrap();
    }
    #[test]
    fn empty_revision_keeps_all_metadata_and_exact_row_schema() {
        let (temp, store) = setup();
        revision(&store, temp.path(), 1, 0, None);
        let read = read(&store, &dataset(), 1.into(), temp.path()).unwrap();
        assert_eq!(read.revision.get(), 1);
        assert_eq!(read.previous_revision.get(), 0);
        assert_eq!(read.created_at, now());
        assert!(read.state.is_empty());
    }
    #[test]
    fn committed_chain_ignores_orphans_and_needs_no_version_files() {
        let (temp, store) = setup();
        revision(&store, temp.path(), 1, 0, Some(1));
        revision(&store, temp.path(), 2, 1, Some(2)); // Orphan.
        revision(&store, temp.path(), 3, 1, Some(3));
        latest(&store, 3);
        assert!(committed(&store, &dataset(), 1.into(), temp.path()).unwrap());
        assert!(!committed(&store, &dataset(), 2.into(), temp.path()).unwrap());
        assert!(committed(&store, &dataset(), 3.into(), temp.path()).unwrap());
    }
    #[test]
    fn missing_latest_is_a_violation_when_revision_objects_exist() {
        let (temp, store) = setup();
        assert!(read_latest(&store, &dataset()).unwrap().is_none());
        revision(&store, temp.path(), 1, 0, None);
        assert_eq!(
            read_latest(&store, &dataset()).err().unwrap().code,
            ErrorCode::ProtocolFailure
        );
        latest(&store, 2);
        assert_eq!(
            committed(&store, &dataset(), 2.into(), temp.path())
                .err()
                .unwrap()
                .code,
            ErrorCode::Unavailable
        );
    }
    #[test]
    fn aba_versions_absent_pairs_and_removed_tables_still_conflict() {
        let pairs = BTreeSet::from([(table(), String::new())]);
        for states in [
            [Some(1), Some(2), Some(1)],
            [None, Some(1), None],
            [Some(1), None, Some(1)],
        ] {
            let (temp, store) = setup();
            for (index, state) in states.into_iter().enumerate() {
                revision(&store, temp.path(), index as u32 + 1, index as u32, state);
            }
            assert_eq!(
                check_run_conflicts(&store, &dataset(), 1.into(), 3.into(), &pairs, temp.path())
                    .err()
                    .unwrap()
                    .code,
                ErrorCode::StateConflict
            );
        }
        let (temp, store) = setup();
        revision(&store, temp.path(), 1, 0, Some(1));
        revision(&store, temp.path(), 3, 1, Some(1));
        check_run_conflicts(&store, &dataset(), 1.into(), 3.into(), &pairs, temp.path()).unwrap();
        assert!(
            check_run_conflicts(&store, &dataset(), 2.into(), 3.into(), &pairs, temp.path())
                .is_err()
        );
    }
}
