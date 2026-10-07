//! Read-only committed-state inspection. Listings never confer commitment and
//! every default selector is fixed to one observed LATEST. No adapter, lease,
//! pending completion, receipt creation or repair occurs in this module.
use crate::{
    admin::Admin,
    clock::{Clock, parse},
    contract,
    holds::Holds,
    retention, revision,
    store::{Result, Store, backend_error, public_error},
};
use grv_adapter_api::{Column, TableContract};
use grv_storage::{Backend, ErrorKind, ListEntry, ListMode, ObjectKey, ObjectPrefix, model::*};
use grv_types::{ErrorCode, Name, PublicError};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};
const LIMIT: usize = 64 * 1024 * 1024;
const ITEMS: usize = 131_072;
fn error(code: ErrorCode, message: &str) -> PublicError {
    public_error(code, message)
}
fn integrity(message: &str) -> PublicError {
    error(ErrorCode::IntegrityFailure, message)
}
fn protocol(message: &str) -> PublicError {
    error(ErrorCode::ProtocolFailure, message)
}
fn object(dataset: &Name, suffix: &str) -> ObjectKey {
    ObjectKey::new(format!("datasets/{dataset}/{suffix}")).expect("canonical metadata path")
}
fn number(value: Counter) -> Value {
    json!(value.to_string())
}
/// Only descriptive extension metadata may escape to public output. Durable
/// ownership and credential fields are never exposed, including nested fields.
fn redact(mut value: Value) -> Value {
    fn visit(value: &mut Value) {
        match value {
            Value::Object(fields) => {
                fields.retain(|key, _| {
                    let key = key.to_ascii_lowercase();
                    ![
                        "token",
                        "password",
                        "secret",
                        "credential",
                        "authorization",
                        "access_key",
                        "private_key",
                    ]
                    .iter()
                    .any(|word| key.contains(word))
                });
                for child in fields.values_mut() {
                    visit(child);
                }
            }
            Value::Array(values) => {
                for child in values {
                    visit(child);
                }
            }
            _ => {}
        }
    }
    visit(&mut value);
    value
}
pub struct Inspector<'a, B: Backend> {
    store: &'a Store<B>,
    clock: &'a dyn Clock,
    scratch: &'a Path,
}
pub struct Verification {
    pub result: Value,
    pub error: Option<PublicError>,
}
struct Snapshot {
    latest: Latest,
    revision: Option<revision::Revision>,
    number: Counter,
}
impl<'a, B: Backend> Inspector<'a, B> {
    pub fn new(store: &'a Store<B>, clock: &'a dyn Clock, scratch: &'a Path) -> Self {
        Self {
            store,
            clock,
            scratch,
        }
    }
    fn optional<T: DeserializeOwned + Validate>(
        &self,
        path: &ObjectKey,
        coordination: bool,
    ) -> Result<Option<T>> {
        match self.store.backend.read_bytes(path, LIMIT) {
            Ok((bytes, _)) => decode_record(&bytes).map(Some).map_err(|failure| {
                if coordination {
                    protocol("malformed coordination record")
                } else {
                    backend_error(failure)
                }
            }),
            Err(failure) if failure.kind == ErrorKind::NotFound => Ok(None),
            Err(failure) => Err(backend_error(failure)),
        }
    }
    fn read<T: DeserializeOwned + Validate>(
        &self,
        path: &ObjectKey,
        coordination: bool,
    ) -> Result<T> {
        self.optional(path, coordination)?.ok_or_else(|| {
            error(
                ErrorCode::Unavailable,
                "referenced metadata object is missing",
            )
        })
    }
    fn latest(&self, dataset: &Name) -> Result<Latest> {
        self.optional(&revision::latest_key(dataset), true)?
            .ok_or_else(|| {
                error(
                    ErrorCode::NotFound,
                    "dataset coordination LATEST is missing",
                )
            })
    }
    fn read_revision(&self, dataset: &Name, number: Counter) -> Result<revision::Revision> {
        revision::read(self.store, dataset, number, self.scratch).map_err(|failure| {
            if failure.code == ErrorCode::NotFound {
                error(
                    ErrorCode::Unavailable,
                    "committed revision object is missing",
                )
            } else {
                failure
            }
        })
    }
    fn resolve_from(
        &self,
        dataset: &Name,
        latest: &Latest,
        number: Counter,
    ) -> Result<Option<revision::Revision>> {
        if number.get() == 0 {
            return Ok(None);
        }
        let mut cursor = latest.revision;
        let mut seen = BTreeSet::new();
        while cursor.get() != 0 {
            if cursor < number {
                break;
            }
            if !seen.insert(cursor) || seen.len() > ITEMS {
                return Err(protocol("invalid or excessive committed revision chain"));
            }
            let revision = self.read_revision(dataset, cursor)?;
            if cursor == number {
                return Ok(Some(revision));
            }
            cursor = revision.previous_revision;
        }
        Err(error(
            ErrorCode::NotFound,
            "requested revision was never committed",
        ))
    }
    fn snapshot(&self, dataset: &Name, requested: Option<Counter>) -> Result<Snapshot> {
        let latest = self.latest(dataset)?;
        let number = requested.unwrap_or(latest.revision);
        let revision = self.resolve_from(dataset, &latest, number)?;
        Ok(Snapshot {
            latest,
            revision,
            number,
        })
    }
    fn layout(&self, dataset: &Name, table: &Name) -> Result<TableLayout> {
        let layout: TableLayout =
            self.read(&object(dataset, &format!("{table}/.layout.json")), false)?;
        if layout.table != *table {
            return Err(integrity("layout table differs from its path"));
        }
        Ok(layout)
    }
    fn entry(&self, dataset: &Name, entry: &revision::Entry) -> Result<Value> {
        let layout = self.layout(dataset, &entry.table)?;
        let partition = layout
            .parse_partition(&entry.partition)
            .map_err(backend_error)?;
        Ok(
            json!({"table":entry.table,"partition":partition,"version":number(entry.version),"run_id":entry.run_id}),
        )
    }
    fn retired(&self, dataset: &Name) -> Result<bool> {
        Ok(self
            .optional::<RetiredMarker>(&object(dataset, ".retired"), true)?
            .is_some())
    }
    fn listing(&self, prefix: ObjectPrefix, mode: ListMode) -> Result<Vec<ListEntry>> {
        let entries = self
            .store
            .backend
            .list(&prefix, mode)
            .map_err(backend_error)?;
        if entries.len() > ITEMS {
            return Err(error(
                ErrorCode::UnsupportedCapability,
                "inspection metadata exceeds supported item budget",
            ));
        }
        Ok(entries)
    }
    pub fn ls(
        &self,
        dataset: Option<&Name>,
        requested: Option<Counter>,
        table: Option<&Name>,
    ) -> Result<Value> {
        let Some(dataset) = dataset else {
            let mut names = BTreeSet::new();
            for item in self.listing(ObjectPrefix::new("datasets/").unwrap(), ListMode::Children)? {
                if let ListEntry::Prefix(prefix) = item {
                    let text = prefix
                        .as_str()
                        .strip_prefix("datasets/")
                        .unwrap()
                        .trim_end_matches('/');
                    names.insert(
                        Name::new(text).map_err(|_| protocol("invalid dataset directory"))?,
                    );
                }
            }
            let mut items = vec![];
            for name in names {
                let latest: Option<Latest> = self.optional(&revision::latest_key(&name), true)?;
                items.push(json!({"object":{"dataset":name},"current_revision":latest.as_ref().map(|latest| number(latest.revision)),"retired":self.retired(&name)?,"coordination":if latest.is_some(){"present"}else{"missing"},"details":{}}));
            }
            return Ok(json!({"scope":"datasets","dataset":null,"revision":null,"items":items}));
        };
        let selected = self.snapshot(dataset, requested)?;
        let mut items = vec![];
        if let Some(revision) = selected.revision {
            let mut tables = BTreeSet::new();
            for entry in revision.state.values() {
                if let Some(table) = table {
                    if entry.table == *table {
                        let mut identity = self.entry(dataset, entry)?;
                        identity["dataset"] = json!(dataset);
                        items.push(json!({"object":identity,"current_revision":number(selected.latest.revision),"retired":self.retired(dataset)?,"coordination":"present","details":{}}));
                    }
                } else {
                    tables.insert(entry.table.clone());
                }
            }
            if table.is_none() {
                for table in tables {
                    items.push(json!({"object":{"dataset":dataset,"table":table},"current_revision":number(selected.latest.revision),"retired":self.retired(dataset)?,"coordination":"present","details":{}}));
                }
            }
        }
        Ok(
            json!({"scope":if table.is_some(){"partitions"}else{"tables"},"dataset":dataset,"revision":number(selected.number),"items":items}),
        )
    }
    fn folder(
        &self,
        dataset: &Name,
        entry: &revision::Entry,
    ) -> Result<(TableLayout, Partition, String)> {
        let layout = self.layout(dataset, &entry.table)?;
        let partition = layout
            .parse_partition(&entry.partition)
            .map_err(backend_error)?;
        let folder = format!(
            "datasets/{dataset}/{}{}version={}",
            entry.table,
            if entry.partition.is_empty() {
                "/".into()
            } else {
                format!("/{}/", entry.partition)
            },
            entry.version
        );
        Ok((layout, partition, folder))
    }
    fn tombstone(&self, folder: &str) -> Result<bool> {
        // Durable presence wins over residual data. Malformed coordination is
        // nevertheless a protocol error, rather than an invented GC outcome.
        let path = ObjectKey::new(format!("{folder}/.pruned")).unwrap();
        let marker: Option<PrunedMarker> = self.optional(&path, true)?;
        let Some(marker) = marker else {
            return Ok(false);
        };
        let segments: Vec<_> = folder.split('/').collect();
        let mut partition = Partition::new();
        for segment in &segments[3..segments.len() - 1] {
            let (key, value) = segment
                .split_once('=')
                .ok_or_else(|| protocol("invalid pruned version path"))?;
            partition.insert(
                Name::new(key).map_err(|_| protocol("invalid pruned partition key"))?,
                Name::new(value).map_err(|_| protocol("invalid pruned partition value"))?,
            );
        }
        if marker.table.as_str() != segments[2]
            || segments.last().unwrap() != &format!("version={}", marker.version)
            || marker.partition != partition
        {
            return Err(protocol("pruning tombstone identity differs from path"));
        }
        Ok(true)
    }
    fn manifest(
        &self,
        entry: &revision::Entry,
        partition: &Partition,
        folder: &str,
    ) -> Result<VersionManifest> {
        let manifest: VersionManifest = self.read(
            &ObjectKey::new(format!("{folder}/manifest.json")).unwrap(),
            false,
        )?;
        if manifest.table != entry.table
            || manifest.partition != *partition
            || manifest.version != entry.version
            || manifest.run_id != entry.run_id
        {
            return Err(integrity(
                "manifest identity differs from committed selection",
            ));
        }
        Ok(manifest)
    }
    /// Stream into a bounded disk sink; bound footer allocation before Parquet.
    fn file(
        &self,
        path: &ObjectKey,
        expected: &DataFile,
        hash: bool,
        failed_check: &mut &'static str,
    ) -> Result<File> {
        *failed_check = "file-size";
        let mut file = tempfile::tempfile_in(self.scratch)
            .map_err(|failure| public_error(ErrorCode::BackendFailure, failure.to_string()))?;
        struct Sink<'a> {
            file: &'a mut File,
            digest: Sha256,
            bytes: u64,
            limit: u64,
            exceeded: bool,
        }
        impl Write for Sink<'_> {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if bytes.len() as u64 > self.limit.saturating_sub(self.bytes) {
                    self.exceeded = true;
                    return Err(std::io::Error::other("file exceeds manifest size"));
                }
                self.file.write_all(bytes)?;
                self.digest.update(bytes);
                self.bytes += bytes.len() as u64;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                self.file.flush()
            }
        }
        let mut sink = Sink {
            file: &mut file,
            digest: Sha256::new(),
            bytes: 0,
            limit: expected.size.get(),
            exceeded: false,
        };
        let meta = self.store.backend.get(path, &mut sink).map_err(|failure| {
            if sink.exceeded {
                integrity("file exceeds manifest size")
            } else if failure.kind == ErrorKind::NotFound {
                error(ErrorCode::Unavailable, "selected data file is missing")
            } else {
                backend_error(failure)
            }
        })?;
        if meta.size != expected.size || sink.bytes != expected.size.get() {
            return Err(integrity("file size differs from manifest"));
        }
        *failed_check = "file-validator";
        let validator_matches = meta.validator == expected.validator;
        // A changed validator cannot establish integrity; accept it only after
        // recomputing and matching the full content hash.
        if hash || !validator_matches {
            *failed_check = "file-sha256";
            if format!("{:x}", sink.digest.finalize()) != expected.sha256.as_str() {
                return Err(integrity("file SHA-256 differs from manifest"));
            }
        }
        *failed_check = "schema";
        let size = meta.size.get();
        if size < 12 {
            return Err(integrity("invalid Parquet file"));
        }
        file.seek(SeekFrom::End(-8))
            .map_err(|failure| public_error(ErrorCode::BackendFailure, failure.to_string()))?;
        let mut footer = [0; 8];
        file.read_exact(&mut footer)
            .map_err(|failure| public_error(ErrorCode::BackendFailure, failure.to_string()))?;
        let length = u32::from_le_bytes(footer[..4].try_into().unwrap()) as u64;
        if &footer[4..] != b"PAR1" || length > size - 12 || length > 16 * 1024 * 1024 {
            return Err(integrity("invalid or excessive Parquet footer"));
        }
        Ok(file)
    }
    fn contract(
        &self,
        file: File,
        baseline: &SchemaBaseline,
        layout: &TableLayout,
    ) -> Result<(TableContract, File)> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(
            file.try_clone()
                .map_err(|failure| public_error(ErrorCode::BackendFailure, failure.to_string()))?,
        )
        .map_err(|_| integrity("invalid Parquet metadata"))?;
        let columns = baseline
            .columns
            .get(..builder.schema().fields().len())
            .ok_or_else(|| integrity("historic schema exceeds current baseline"))?
            .iter()
            .map(|column| Column {
                name: column.name.clone(),
                logical_type: column.logical_type.clone(),
            })
            .collect();
        let extensions = serde_json::to_value(&layout.extensions).unwrap();
        let column_ext = baseline
            .columns
            .iter()
            .take(builder.schema().fields().len())
            .filter_map(|column| {
                column
                    .ext
                    .as_ref()
                    .map(|ext| (column.name.clone(), serde_json::to_value(ext).unwrap()))
            })
            .collect::<serde_json::Map<_, _>>();
        let contract = TableContract {
            columns,
            partition_keys: layout.partition_keys.clone(),
            extensions: if extensions.is_null() {
                json!({})
            } else {
                extensions
            },
            column_ext: Value::Object(column_ext),
        };
        let schema = contract::arrow_schema(&contract).map_err(|failure| {
            public_error(ErrorCode::UnsupportedCapability, failure.to_string())
        })?;
        if schema
            .fields()
            .iter()
            .zip(builder.schema().fields())
            .any(|(a, b)| a.name() != b.name() || a.data_type() != b.data_type())
        {
            return Err(integrity("historic schema differs from baseline prefix"));
        }
        Ok((contract, file))
    }
    fn table_details(&self, dataset: &Name, state: &revision::State) -> Result<Vec<Value>> {
        let mut result = vec![];
        let tables: BTreeSet<_> = state.values().map(|entry| entry.table.clone()).collect();
        for table in tables {
            let layout = self.layout(dataset, &table)?;
            let baseline: Option<SchemaBaseline> =
                self.optional(&object(dataset, &format!("{table}/.schema.json")), false)?;
            if baseline
                .as_ref()
                .is_some_and(|baseline| baseline.table != table)
            {
                return Err(integrity("baseline table differs from path"));
            }
            let mut logical: Option<TableContract> = None;
            let mut available = true;
            let mut provenance = vec![];
            for entry in state.values().filter(|entry| entry.table == table) {
                let (_, partition, folder) = self.folder(dataset, entry)?;
                let retained_run: Option<SealedRun> = self.optional(
                    &object(dataset, &format!(".runs/{}.json", entry.run_id)),
                    true,
                )?;
                if retained_run
                    .as_ref()
                    .is_some_and(|run| run.run_id != entry.run_id)
                {
                    return Err(protocol("sealed run identity differs from path"));
                }
                provenance.push(json!({"version":number(entry.version),"partition":partition,"run_id":entry.run_id,"run":retained_run.map(|run|redact(json!(run)))}));
                if self.tombstone(&folder)? {
                    available = false;
                    continue;
                }
                let manifest = match self.manifest(entry, &partition, &folder) {
                    Ok(value) => value,
                    Err(failure) if failure.code == ErrorCode::Unavailable => {
                        available = false;
                        continue;
                    }
                    Err(failure) => return Err(failure),
                };
                provenance.push(json!({"version":number(entry.version),"partition":partition,"run_id":manifest.run_id,"created_at":manifest.created_at,"derived_from":manifest.derived_from,"metadata":redact(json!(manifest.metadata))}));
                let Some(baseline) = &baseline else {
                    available = false;
                    continue;
                };
                for expected in &manifest.data_files {
                    let path = ObjectKey::new(format!("{folder}/{}", expected.name)).unwrap();
                    let file = match self.file(&path, expected, false, &mut "schema") {
                        Ok(file) => file,
                        Err(failure) if failure.code == ErrorCode::Unavailable => {
                            available = false;
                            continue;
                        }
                        Err(failure) => return Err(failure),
                    };
                    let (contract, _) = self.contract(file, baseline, &layout)?;
                    if let Some(previous) = &logical {
                        let common = previous.columns.len().min(contract.columns.len());
                        if previous.columns[..common] != contract.columns[..common] {
                            return Err(integrity(
                                "revision version schemas are not prefix-compatible",
                            ));
                        }
                    }
                    if logical
                        .as_ref()
                        .is_none_or(|previous| previous.columns.len() < contract.columns.len())
                    {
                        logical = Some(contract);
                    }
                }
                if self.tombstone(&folder)? {
                    available = false;
                }
            }
            result.push(json!({"table":table,"layout":redact(json!(layout)),"baseline_schema":baseline.as_ref().map_or(json!({}),|baseline|redact(json!(baseline))),"revision_schema":if available{logical.map(|contract|redact(json!(contract)))}else{None},"availability":if available{"available"}else{"unavailable"},"provenance":provenance}));
        }
        Ok(result)
    }
    fn pins(&self, dataset: &Name, state: &revision::State, number: Counter) -> Result<Vec<Value>> {
        let mut found = BTreeMap::new();
        let mut scopes = vec![];
        if number.get() != 0 {
            scopes.push(PinScope::Revision(RevisionScope { revision: number }));
        }
        for entry in state.values() {
            let (layout, partition, _) = self.folder(dataset, entry)?;
            scopes.extend([
                PinScope::Table(TableScope {
                    table: layout.table.clone(),
                }),
                PinScope::Partition(PartitionScope {
                    table: layout.table.clone(),
                    partition: partition.clone(),
                }),
                PinScope::Version(VersionScope {
                    table: layout.table,
                    partition,
                    version: entry.version,
                }),
            ]);
        }
        let mut folders = BTreeSet::new();
        for scope in scopes {
            let sentinel = grv_types::Uuid::v4();
            let path = retention::pin_path(self.store, dataset, &scope, &sentinel, false)?;
            folders.insert(path.as_str().rsplit_once('/').unwrap().0.to_owned());
        }
        for folder in folders {
            for item in self.listing(
                ObjectPrefix::new(format!("{folder}/")).unwrap(),
                ListMode::Children,
            )? {
                let ListEntry::Object(path) = item else {
                    return Err(protocol("nested pin metadata"));
                };
                let text = path.as_str().strip_prefix(&format!("{folder}/")).unwrap();
                let id = text
                    .strip_suffix(".released.json")
                    .or_else(|| text.strip_suffix(".json"))
                    .ok_or_else(|| protocol("invalid pin filename"))?;
                let id = grv_types::Uuid::new(id).map_err(|_| protocol("invalid pin ID"))?;
                let path = ObjectKey::new(format!("{folder}/{id}.json")).unwrap();
                let pin: PinRecord = self.read(&path, true)?;
                if pin.pin_id != id
                    || retention::pin_path(self.store, dataset, &pin.scope, &id, false)? != path
                {
                    return Err(protocol("pin identity differs from path"));
                }
                let release: Option<PinReleaseMarker> = self.optional(
                    &ObjectKey::new(format!("{folder}/{id}.released.json")).unwrap(),
                    true,
                )?;
                if release.as_ref().is_some_and(|release| release.pin_id != id) {
                    return Err(protocol("pin release identity differs from path"));
                }
                found.insert(path.as_str().to_owned(),json!({"pin_id":id,"scope":pin.scope,"active":release.is_none(),"reason":pin.reason,"created_at":pin.created_at,"created_by":pin.created_by,"operation_id":pin.operation_id,"released_at":release.as_ref().map(|release|&release.released_at),"release_operation_id":release.as_ref().map(|release|&release.operation_id)}));
            }
        }
        Ok(found.into_values().collect())
    }
    pub fn show(
        &self,
        dataset: &Name,
        requested: Option<Counter>,
        retention: bool,
    ) -> Result<Value> {
        let selected = self.snapshot(dataset, requested)?;
        let publication = if let Some(revision) = &selected.revision {
            let operation: OperationRecord = self.read(
                &object(
                    dataset,
                    &format!(".states/operations/{}.json", revision.operation_id),
                ),
                true,
            )?;
            if operation.dataset != *dataset
                || operation.operation_id != revision.operation_id
                || !matches!(&operation.body,OperationPayload::Publish(payload) if payload.revision==revision.revision && payload.previous_revision==revision.previous_revision)
            {
                return Err(protocol("revision publication audit differs"));
            }
            Some(redact(json!(operation)))
        } else {
            None
        };
        let state = selected
            .revision
            .as_ref()
            .map(|revision| &revision.state)
            .cloned()
            .unwrap_or_default();
        let entries = state
            .values()
            .map(|entry| self.entry(dataset, entry))
            .collect::<Result<Vec<_>>>()?;
        let mut retention_report = None;
        if retention {
            let ttl = self.store.parameters.max_lease_ttl_seconds.get();
            let mut protections = if selected.number.get() == 0 {
                vec![]
            } else {
                Admin::new(self.store, self.clock, ttl)?.remaining_revision_protections(
                    dataset,
                    selected.number,
                    self.scratch,
                )?
            };
            // Preserve released dependency holds in the explanation as well.
            for item in self.listing(
                ObjectPrefix::new(format!("datasets/{dataset}/.holds/")).unwrap(),
                ListMode::Recursive,
            )? {
                let ListEntry::Object(path) = item else {
                    return Err(protocol("invalid hold listing"));
                };
                let hold: HoldRecord = self.read(&path, true)?;
                if crate::holds::hold_key(&hold) != path || hold.dataset != *dataset {
                    return Err(protocol("hold identity differs from path"));
                }
                if hold.revision != selected.number {
                    continue;
                }
                let release: Option<HoldReleaseMarker> =
                    self.optional(&crate::holds::release_key(&hold), true)?;
                if let Some(release) = release {
                    if release.retention_id != hold.retention_id {
                        return Err(protocol("hold release identity differs"));
                    }
                    let mut details = serde_json::Map::new();
                    details.insert("consumer_dataset".into(), json!(hold.target_dataset));
                    details.insert("retention_id".into(), json!(hold.retention_id));
                    details.insert("released_at".into(), json!(release.released_at));
                    details.insert("release_operation_id".into(), json!(release.operation_id));
                    protections.push(crate::admin::Protection {
                        kind: crate::admin::ProtectionKind::Hold,
                        object: crate::admin::ProtectionObject {
                            dataset: Some(dataset.clone()),
                            revision: Some(grv_types::U64::new(hold.revision.get()).unwrap()),
                            run_id: Some(hold.target_run_id),
                            path: Some(path.as_str().to_owned()),
                            ..Default::default()
                        },
                        active: false,
                        releasable: false,
                        until: None,
                        details,
                    });
                }
            }
            protections.sort_by_key(|protection| serde_json::to_string(protection).unwrap());
            retention_report = Some(
                json!({"pins":self.pins(dataset,&state,selected.number)?,"protections":protections}),
            );
        }
        let mut tables = self.table_details(dataset, &state)?;
        if let Some(publication) = publication {
            for table in &mut tables {
                table["provenance"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"publication":publication}));
            }
        }
        Ok(
            json!({"dataset":dataset,"revision":number(selected.number),"previous_revision":selected.revision.as_ref().map(|revision|number(revision.previous_revision)),"operation_id":selected.revision.as_ref().map(|revision|&revision.operation_id),"entries":entries,"tables":tables,"retention":retention_report}),
        )
    }
    pub fn status(&self, dataset: &Name) -> Result<Value> {
        let latest = self.latest(dataset)?;
        let pending = if let Some(path) = &latest.pending {
            let record: OperationRecord = self.read(&object(dataset, path), true)?;
            if record.dataset != *dataset
                || path != &format!(".states/operations/{}.json", record.operation_id)
            {
                return Err(protocol("pending operation identity differs from path"));
            }
            let body = json!(record.body);
            Some(json!({"operation_id":record.operation_id,"kind":body["kind"],"path":path}))
        } else {
            None
        };
        let mut runs = BTreeMap::new();
        for item in self.listing(
            ObjectPrefix::new(format!("datasets/{dataset}/.runs/")).unwrap(),
            ListMode::Children,
        )? {
            let ListEntry::Object(path) = item else {
                continue;
            };
            if !path.as_str().ends_with(".control.json") {
                continue;
            }
            let control: RunControl = self.read(&path, true)?;
            if path != object(dataset, &format!(".runs/{}.control.json", control.run_id)) {
                return Err(protocol("run control identity differs from path"));
            }
            runs.insert(control.run_id.clone(),json!({"run_id":control.run_id,"phase":control.phase,"base_revision":number(control.base_revision),"expires_at":control.expires_at,"expired":control.expires_at.as_ref().is_some_and(|expiry|parse(expiry)<=parse(&self.clock.now())),"holds_confirmed":control.holds_confirmed}));
        }
        let lease=latest.lease.as_ref().map(|lease|json!({"holder":lease.holder,"claimed_at":lease.claimed_at,"expires_at":lease.expires_at,"expired":parse(&lease.expires_at)<=parse(&self.clock.now())}));
        Ok(
            json!({"dataset":dataset,"current_revision":number(latest.revision),"retired":self.retired(dataset)?,"lease":lease,"pending":pending,"runs":runs.into_values().collect::<Vec<_>>(),"adapter_state":null}),
        )
    }
    pub fn log(&self, dataset: &Name, limit: usize) -> Result<Value> {
        let latest = self.latest(dataset)?;
        let mut cursor = latest.revision;
        let mut entries = vec![];
        let mut seen = BTreeSet::new();
        while cursor.get() != 0 && entries.len() < limit {
            if !seen.insert(cursor) {
                return Err(protocol("cyclic revision chain"));
            }
            if seen.len() > ITEMS {
                return Err(error(
                    ErrorCode::UnsupportedCapability,
                    "history exceeds supported metadata budget",
                ));
            }
            let current = self.read_revision(dataset, cursor)?;
            let previous = if current.previous_revision.get() == 0 {
                revision::State::new()
            } else {
                self.read_revision(dataset, current.previous_revision)?
                    .state
            };
            let changes = changes(&previous, &current.state);
            let operation: OperationRecord = self.read(
                &object(
                    dataset,
                    &format!(".states/operations/{}.json", current.operation_id),
                ),
                true,
            )?;
            let OperationPayload::Publish(payload) = &operation.body else {
                return Err(protocol("revision operation is not publication"));
            };
            if operation.dataset != *dataset
                || operation.operation_id != current.operation_id
                || payload.revision != current.revision
                || payload.previous_revision != current.previous_revision
            {
                return Err(protocol("revision publication audit differs"));
            }
            let mut metadata = serde_json::Map::new();
            if let Some(reason) = &payload.change_set.reason {
                metadata.insert("reason".into(), json!(reason));
            }
            let run_ids: BTreeSet<_> = payload
                .change_set
                .runs
                .iter()
                .cloned()
                .chain(
                    current
                        .state
                        .iter()
                        .filter(|(pair, entry)| previous.get(*pair) != Some(*entry))
                        .map(|(_, entry)| entry.run_id.clone()),
                )
                .collect();
            for run_id in current
                .state
                .values()
                .map(|entry| entry.run_id.clone())
                .collect::<BTreeSet<_>>()
            {
                if let Some(run) = self.optional::<SealedRun>(
                    &object(dataset, &format!(".runs/{run_id}.json")),
                    true,
                )? && let Some(details) = run.metadata
                {
                    metadata.insert(run_id.as_str().to_owned(), redact(json!(details)));
                }
            }
            entries.push(json!({"revision":number(current.revision),"previous_revision":number(current.previous_revision),"operation_id":current.operation_id,"created_at":current.created_at,"changes":changes,"run_ids":run_ids,"metadata":metadata}));
            cursor = current.previous_revision;
        }
        Ok(json!({"dataset":dataset,"limit":limit,"has_more":cursor.get()!=0,"entries":entries}))
    }
    pub fn diff(&self, dataset: &Name, from: Counter, to: Option<Counter>) -> Result<Value> {
        let latest = self.latest(dataset)?;
        let to = to.unwrap_or(latest.revision);
        let before = self
            .resolve_from(dataset, &latest, from)?
            .map(|revision| revision.state)
            .unwrap_or_default();
        let after = self
            .resolve_from(dataset, &latest, to)?
            .map(|revision| revision.state)
            .unwrap_or_default();
        let pairs: BTreeSet<_> = before.keys().chain(after.keys()).cloned().collect();
        let mut entries = vec![];
        for pair in pairs {
            let a = before.get(&pair);
            let b = after.get(&pair);
            if a == b {
                continue;
            }
            let sample = a.or(b).unwrap();
            let layout = self.layout(dataset, &sample.table)?;
            let partition = layout
                .parse_partition(&sample.partition)
                .map_err(backend_error)?;
            entries.push(json!({"table":sample.table,"partition":partition,"kind":if a.is_none(){"added"}else if b.is_none(){"removed"}else{"changed"},"before":a.map(|entry|json!({"version":number(entry.version),"run_id":entry.run_id})),"after":b.map(|entry|json!({"version":number(entry.version),"run_id":entry.run_id}))}));
        }
        let a: BTreeSet<_> = before.values().map(|entry| entry.table.clone()).collect();
        let b: BTreeSet<_> = after.values().map(|entry| entry.table.clone()).collect();
        let details_a = self.table_details(dataset, &before)?;
        let details_b = self.table_details(dataset, &after)?;
        let schemas=a.union(&b).map(|table|{let before=details_a.iter().find(|value|value["table"]==json!(table)).map(|value|value["revision_schema"].clone()).unwrap_or(Value::Null);let after=details_b.iter().find(|value|value["table"]==json!(table)).map(|value|value["revision_schema"].clone()).unwrap_or(Value::Null);json!({"table":table,"state":if before.is_null()||after.is_null(){"unknown"}else if before==after{"equal"}else{"changed"},"before":before,"after":after})}).collect::<Vec<_>>();
        Ok(
            json!({"dataset":dataset,"from_revision":number(from),"to_revision":number(to),"changed":!entries.is_empty(),"entries":entries,"tables_added":b.difference(&a).collect::<Vec<_>>(),"tables_removed":a.difference(&b).collect::<Vec<_>>(),"schemas":schemas}),
        )
    }
    pub fn verify(
        &self,
        dataset: &Name,
        requested: Option<Counter>,
        full: bool,
    ) -> Result<Verification> {
        let selected = self.snapshot(dataset, requested).map_err(|mut failure| {
            if failure.code == ErrorCode::NotFound
                && requested.is_some_and(|number| number.get() != 0)
            {
                failure.code = ErrorCode::ProtocolFailure;
            }
            failure
        })?;
        let identity = json!({"dataset":dataset,"revision":number(selected.number)});
        let mut checks = vec![
            json!({"object":identity,"check":"commitment","state":"passed"}),
            json!({"object":identity,"check":"revision","state":"passed"}),
        ];
        let mut unavailable = vec![];
        let mut failure = None;
        if let Some(revision) = selected.revision {
            let operation = self.read::<OperationRecord>(
                &object(
                    dataset,
                    &format!(".states/operations/{}.json", revision.operation_id),
                ),
                true,
            );
            match operation {
                Ok(operation) => match operation.body {
                    OperationPayload::Publish(payload)
                        if operation.dataset == *dataset
                            && operation.operation_id == revision.operation_id
                            && payload.revision == revision.revision
                            && payload.previous_revision == revision.previous_revision => {}
                    _ => {
                        failure = Some(protocol(
                            "revision operation does not match committed publication",
                        ));
                    }
                },
                Err(error) => failure = Some(error),
            }
            let mut seen = BTreeSet::new();
            for entry in revision.state.values() {
                if !seen.insert((entry.table.clone(), entry.partition.clone(), entry.version)) {
                    continue;
                }
                let identity = match self.entry(dataset, entry) {
                    Ok(mut value) => {
                        value["dataset"] = json!(dataset);
                        value
                    }
                    Err(error) => {
                        failure.get_or_insert(error);
                        continue;
                    }
                };
                let mut failed_check = "layout";
                let mut failed_object = identity.clone();
                match self.verify_entry(
                    dataset,
                    entry,
                    &identity,
                    &mut checks,
                    &mut failed_check,
                    &mut failed_object,
                ) {
                    Ok(()) => {}
                    Err(error) => {
                        let missing = error.code == ErrorCode::Unavailable;
                        checks.push(json!({"object":failed_object,"check":failed_check,"state":if missing{"unavailable"}else{"failed"}}));
                        if missing {
                            unavailable.push(failed_object);
                        }
                        if failure
                            .as_ref()
                            .is_none_or(|previous| previous.code == ErrorCode::Unavailable)
                        {
                            failure = Some(error);
                        }
                    }
                }
            }
        }
        Ok(Verification {
            result: json!({"dataset":dataset,"revision":number(selected.number),"full":full,"checks":checks,"unavailable":unavailable}),
            error: failure,
        })
    }
    fn verify_entry(
        &self,
        dataset: &Name,
        entry: &revision::Entry,
        identity: &Value,
        checks: &mut Vec<Value>,
        failed_check: &mut &'static str,
        failed_object: &mut Value,
    ) -> Result<()> {
        fn passed(checks: &mut Vec<Value>, identity: &Value, check: &str) {
            checks.push(json!({"object":identity,"check":check,"state":"passed"}));
        }
        let (layout, partition, folder) = self.folder(dataset, entry)?;
        passed(checks, identity, "layout");
        *failed_check = "tombstone";
        if self.tombstone(&folder)? {
            return Err(error(
                ErrorCode::Unavailable,
                "selected version has a pruning tombstone",
            ));
        }
        passed(checks, identity, "tombstone");
        *failed_check = "manifest";
        let manifest = self.manifest(entry, &partition, &folder)?;
        passed(checks, identity, "manifest");
        *failed_check = "run-fence";
        let control: RunControl = self
            .read(
                &object(dataset, &format!(".runs/{}.control.json", entry.run_id)),
                true,
            )
            .map_err(|failure| {
                if failure.code == ErrorCode::Unavailable {
                    protocol("committed producing run control is missing")
                } else {
                    failure
                }
            })?;
        let run: SealedRun = self
            .read(
                &object(dataset, &format!(".runs/{}.json", entry.run_id)),
                true,
            )
            .map_err(|failure| {
                if failure.code == ErrorCode::Unavailable {
                    protocol("committed producing sealed run is missing")
                } else {
                    failure
                }
            })?;
        run.validate_control(&control)
            .map_err(|_| protocol("sealed run and control disagree"))?;
        run.validate_manifest(&manifest)
            .map_err(|_| integrity("manifest is absent from confirmed sealed run"))?;
        passed(checks, identity, "run-fence");
        *failed_check = "hold";
        let holds = Holds::new(self.store, self.clock, self.scratch);
        for input in &run.inputs {
            let source_latest = self.latest(&input.dataset).map_err(|failure| {
                if failure.code == ErrorCode::NotFound {
                    protocol("dependency source coordination is missing")
                } else {
                    failure
                }
            })?;
            self.resolve_from(&input.dataset, &source_latest, input.revision)
                .map_err(|failure| {
                    if failure.code == ErrorCode::NotFound {
                        protocol("dependency references a never-committed revision")
                    } else {
                        failure
                    }
                })?;
            let path = object(
                &input.dataset,
                &format!(
                    ".holds/{dataset}/revision={}/{}.json",
                    input.revision, input.retention_id
                ),
            );
            let hold: HoldRecord = self.read(&path, true).map_err(|failure| {
                if failure.code == ErrorCode::Unavailable {
                    protocol("confirmed dependency hold is missing")
                } else {
                    failure
                }
            })?;
            if hold.dataset != input.dataset
                || hold.revision != input.revision
                || hold.retention_id != input.retention_id
                || hold.target_dataset != *dataset
                || hold.target_run_id != run.run_id
                || crate::holds::hold_key(&hold) != path
                || !holds.active(&hold)?
            {
                return Err(integrity(
                    "confirmed dependency hold is inactive or mismatched",
                ));
            }
        }
        holds
            .check_references(dataset, &run, &manifest)
            .map_err(|mut error| {
                if error.code == ErrorCode::NotFound {
                    error.code = ErrorCode::ProtocolFailure;
                }
                error
            })?;
        passed(checks, identity, "hold");
        *failed_check = "schema";
        let baseline: SchemaBaseline = self.read(
            &object(dataset, &format!("{}/.schema.json", entry.table)),
            false,
        )?;
        if baseline.table != entry.table {
            return Err(integrity("baseline identity differs from path"));
        }
        *failed_check = "manifest";
        let listed: BTreeSet<_> = self
            .listing(
                ObjectPrefix::new(format!("{folder}/")).unwrap(),
                ListMode::Children,
            )?
            .into_iter()
            .filter_map(|item| match item {
                ListEntry::Object(path) => path
                    .as_str()
                    .strip_prefix(&format!("{folder}/"))
                    .filter(|name| name.starts_with("data"))
                    .map(str::to_owned),
                ListEntry::Prefix(prefix)
                    if prefix
                        .as_str()
                        .strip_prefix(&format!("{folder}/"))
                        .is_some_and(|name| name.starts_with("data")) =>
                {
                    Some(prefix.as_str().to_owned())
                }
                _ => None,
            })
            .collect();
        let expected: BTreeSet<_> = manifest
            .data_files
            .iter()
            .map(|file| file.name.clone())
            .collect();
        if listed != expected {
            if !expected.is_subset(&listed) {
                *failed_check = "file-size";
                let missing = expected.difference(&listed).next().unwrap();
                *failed_object = json!({"dataset":dataset,"table":entry.table,"partition":partition,"version":number(entry.version),"path":format!("{folder}/{missing}")});
                return Err(error(
                    ErrorCode::Unavailable,
                    "manifest-listed data file is missing",
                ));
            }
            return Err(integrity("version data file set differs from manifest"));
        }
        let mut count = 0u64;
        let mut previous = None;
        for expected in &manifest.data_files {
            let path = ObjectKey::new(format!("{folder}/{}", expected.name)).unwrap();
            let file_identity = json!({"dataset":dataset,"table":entry.table,"partition":partition,"version":number(entry.version),"path":path.as_str()});
            *failed_object = file_identity.clone();
            let file = self.file(&path, expected, true, failed_check)?;
            passed(checks, &file_identity, "file-size");
            passed(checks, &file_identity, "file-validator");
            passed(checks, &file_identity, "file-sha256");
            *failed_check = "schema";
            let (contract, file) = self.contract(file, &baseline, &layout)?;
            if previous
                .as_ref()
                .is_some_and(|previous| previous != &contract)
            {
                return Err(integrity("version files have different logical schemas"));
            }
            previous = Some(contract);
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|_| integrity("invalid Parquet metadata"))?
                .with_batch_size(1024)
                .build()
                .map_err(|_| integrity("invalid Parquet reader"))?;
            for batch in reader {
                let batch = batch.map_err(|_| integrity("invalid Parquet data"))?;
                for key in &layout.partition_keys {
                    use arrow_array::Array;
                    let index = batch
                        .schema()
                        .index_of(&format!("_{key}_"))
                        .map_err(|_| integrity("partition duplicate column is absent"))?;
                    let values = batch
                        .column(index)
                        .as_any()
                        .downcast_ref::<arrow_array::StringArray>()
                        .ok_or_else(|| integrity("partition duplicate column is not string"))?;
                    if (0..values.len()).any(|row| {
                        values.is_null(row) || values.value(row) != partition[key].as_str()
                    }) {
                        return Err(integrity("partition duplicate values differ from path"));
                    }
                }
                count = count
                    .checked_add(batch.num_rows() as u64)
                    .ok_or_else(|| integrity("row count overflow"))?;
            }
        }
        *failed_object = identity.clone();
        *failed_check = "manifest";
        if count != manifest.row_count.get() {
            return Err(integrity("manifest row count differs from data"));
        }
        passed(checks, identity, "schema");
        *failed_check = "tombstone";
        if self.tombstone(&folder)? {
            return Err(error(
                ErrorCode::Unavailable,
                "selected version was pruned during verification",
            ));
        }
        Ok(())
    }
}
fn changes(before: &revision::State, after: &revision::State) -> Value {
    let mut added = 0;
    let mut removed = 0;
    let mut changed = 0;
    for pair in before.keys().chain(after.keys()).collect::<BTreeSet<_>>() {
        match (before.get(pair), after.get(pair)) {
            (None, Some(_)) => added += 1,
            (Some(_), None) => removed += 1,
            (Some(a), Some(b)) if a != b => changed += 1,
            _ => {}
        }
    }
    json!({"added":added,"removed":removed,"changed":changed})
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{clock::SystemClock, store::InitOptions};
    use grv_storage::{LocalBackend, ObjectMeta, Validator};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct ReadOnly {
        backend: LocalBackend,
        latest_reads: AtomicUsize,
    }
    impl Backend for ReadOnly {
        fn get(&self, key: &ObjectKey, sink: &mut dyn Write) -> grv_storage::Result<ObjectMeta> {
            if key.as_str().ends_with("/.states/LATEST") {
                self.latest_reads.fetch_add(1, Ordering::SeqCst);
            }
            self.backend.get(key, sink)
        }
        fn head(&self, key: &ObjectKey) -> grv_storage::Result<ObjectMeta> {
            self.backend.head(key)
        }
        fn list(
            &self,
            prefix: &ObjectPrefix,
            mode: ListMode,
        ) -> grv_storage::Result<Vec<ListEntry>> {
            self.backend.list(prefix, mode)
        }
        fn delete(&self, _: &ObjectKey) -> grv_storage::Result<()> {
            panic!("inspection attempted deletion")
        }
        fn conditional_create(
            &self,
            _: &ObjectKey,
            _: &mut dyn Read,
        ) -> grv_storage::Result<Validator> {
            panic!("inspection attempted creation")
        }
        fn conditional_put(
            &self,
            _: &ObjectKey,
            _: &Validator,
            _: &mut dyn Read,
        ) -> grv_storage::Result<Validator> {
            panic!("inspection attempted conditional mutation")
        }
    }
    #[test]
    fn default_and_latest_selectors_sample_once_and_never_invoke_mutating_backend_primitives() {
        let directory = tempfile::tempdir().unwrap();
        let dataset = Name::new("data").unwrap();
        let clock = SystemClock::default();
        let (store, _) = Store::initialize(
            LocalBackend::create(directory.path()).unwrap(),
            InitOptions::default(),
        )
        .unwrap();
        let operation = OperationRecord {
            operation_id: crate::clock::new_run_id(&clock.now()).unwrap(),
            dataset: dataset.clone(),
            created_at: clock.now(),
            created_by: "inspection-test".into(),
            body: OperationPayload::Publish(PublishPayload {
                revision: 1.into(),
                previous_revision: 0.into(),
                change_set: ChangeSet::default(),
            }),
        };
        let revision = revision::Revision {
            revision: 1.into(),
            previous_revision: 0.into(),
            operation_id: operation.operation_id.clone(),
            created_at: operation.created_at.clone(),
            state: revision::State::new(),
        };
        let file = revision::encode(&revision, directory.path()).unwrap();
        store
            .backend
            .conditional_create(
                &revision::revision_key(&dataset, 1.into()),
                &mut File::open(file.path()).unwrap(),
            )
            .unwrap();
        store
            .backend
            .create_bytes(
                &object(
                    &dataset,
                    &format!(".states/operations/{}.json", operation.operation_id),
                ),
                &encode_record(&operation).unwrap(),
            )
            .unwrap();
        let mut latest = Latest::empty();
        latest.revision = 1.into();
        latest.high_water = 1.into();
        store
            .backend
            .create_bytes(
                &revision::latest_key(&dataset),
                &encode_record(&latest).unwrap(),
            )
            .unwrap();
        let store = Store {
            backend: ReadOnly {
                backend: store.backend,
                latest_reads: AtomicUsize::new(0),
            },
            parameters: store.parameters,
        };
        let inspector = Inspector::new(&store, &clock, directory.path());
        let outputs = [
            inspector.ls(Some(&dataset), None, None).unwrap(),
            inspector.show(&dataset, None, false).unwrap(),
            inspector.status(&dataset).unwrap(),
            inspector.log(&dataset, 20).unwrap(),
            inspector.diff(&dataset, 0.into(), None).unwrap(),
            inspector.verify(&dataset, None, true).unwrap().result,
        ];
        assert_eq!(store.backend.latest_reads.load(Ordering::SeqCst), 6);
        for (command, result) in ["ls", "show", "status", "log", "diff", "verify"]
            .into_iter()
            .zip(outputs)
        {
            let mut envelope = grv_types::CommandOutput::success(command, result);
            envelope.root = Some(directory.path().to_str().unwrap().into());
            grv_adapter_host::validate_output(&json!(envelope)).unwrap();
        }
    }
}
