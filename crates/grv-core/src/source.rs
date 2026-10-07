//! Read-only, fixed source selection and verified local files for pull/build readers.
//! Byte hashes and logical schemas are parent facts. Adapters still enforce data
//! partition values, permitted trailing-null padding, and destination checks.
use crate::{
    contract, revision,
    store::{Result, Store, backend_error, public_error},
};
use base64::Engine as _;
use bytes::Bytes;
use grv_adapter_api::{Column, FileAccess, FileSchema, TableContract, VerifiedFile};
use grv_storage::{
    Backend, ErrorKind, ObjectKey, ObjectMeta,
    model::{
        Counter, DataFile, Partition, SchemaBaseline, TableLayout, Validate, VersionManifest,
        decode_record,
    },
};
use grv_types::{ErrorCode, Name, RequestedRevision, U64};
use parquet::{
    arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
    file::{
        metadata::{PageIndexPolicy, ParquetMetaDataReader},
        reader::{ChunkReader, Length},
    },
};
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeSet,
    fs::File,
    io::{Cursor, Read, Seek, SeekFrom, Write},
    path::Path,
};
const METADATA_LIMIT: usize = 64 * 1024 * 1024;

const RANGE_BYTES: usize = 64 * 1024;
const FOOTER_BYTES: usize = 16 * 1024 * 1024;

fn data_error(error: grv_storage::Error) -> grv_types::PublicError {
    if error.kind == ErrorKind::NotFound {
        unavailable("selected data file is missing")
    } else if error.kind == ErrorKind::PreconditionFailed {
        integrity("selected data file changed during metadata verification")
    } else {
        backend_error(error)
    }
}
fn footer_size(trailer: &[u8], size: u64) -> Result<usize> {
    if trailer.len() != 8 || size < 12 || &trailer[4..] != b"PAR1" {
        return Err(integrity("invalid selected Parquet file"));
    }
    let length = u32::from_le_bytes(trailer[..4].try_into().unwrap()) as usize;
    if length as u64 > size - 12 || length > FOOTER_BYTES {
        return Err(integrity(
            "selected Parquet footer exceeds supported metadata budget",
        ));
    }
    Ok(length)
}
/// Contains only serialized footer bytes and their trailer. Default Arrow
/// metadata loading skips page indexes. Any attempted body/index read fails.
struct FooterReader {
    size: u64,
    start: u64,
    tail: Bytes,
}
impl Length for FooterReader {
    fn len(&self) -> u64 {
        self.size
    }
}
impl ChunkReader for FooterReader {
    type T = Cursor<Bytes>;
    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        Ok(Cursor::new(self.get_bytes(
            start,
            usize::try_from(self.size.saturating_sub(start)).map_err(|_| {
                parquet::errors::ParquetError::General("footer range overflow".into())
            })?,
        )?))
    }
    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let invalid = || {
            parquet::errors::ParquetError::General(
                "metadata reader attempted a non-footer read".into(),
            )
        };
        let relative = start
            .checked_sub(self.start)
            .and_then(|v| usize::try_from(v).ok())
            .ok_or_else(invalid)?;
        let end = relative
            .checked_add(length)
            .filter(|v| *v <= self.tail.len())
            .ok_or_else(invalid)?;
        Ok(self.tail.slice(relative..end))
    }
}
fn decode_footer(size: u64, tail: Vec<u8>) -> Result<ArrowReaderMetadata> {
    let bytes = footer_size(&tail[tail.len().saturating_sub(8)..], size)?;
    if tail.len() != bytes + 8 {
        return Err(integrity("invalid selected Parquet footer length"));
    }
    footer_budget(&tail[..bytes])?;
    let metadata = ParquetMetaDataReader::new()
        .with_column_index_policy(PageIndexPolicy::Skip)
        .with_offset_index_policy(PageIndexPolicy::Skip)
        .parse_and_finish(&FooterReader {
            size,
            start: size - tail.len() as u64,
            tail: Bytes::from(tail),
        })
        .map_err(|_| integrity("invalid selected Parquet metadata"))?;
    // Match the pinned reader's last-valued-key resolution without copying the
    // metadata map. FlatBuffers can share one field table through many vector
    // entries, so serialized size alone is not an expanded schema bound.
    if let Some(encoded) = metadata
        .file_metadata()
        .key_value_metadata()
        .and_then(|values| {
            values.iter().rev().find_map(|value| {
                (value.key == parquet::arrow::ARROW_SCHEMA_META_KEY)
                    .then_some(value.value.as_deref())
                    .flatten()
            })
        })
    {
        arrow_schema_budget(encoded)?;
    }
    ArrowReaderMetadata::try_new(
        std::sync::Arc::new(metadata),
        ArrowReaderOptions::default().with_page_index_policy(PageIndexPolicy::Skip),
    )
    .map_err(|_| integrity("invalid selected Parquet metadata"))
}

fn arrow_schema_budget(encoded: &str) -> Result<()> {
    // Decode once under the serialized footer bound, and validate the expanded
    // FlatBuffer before the pinned Arrow conversion allocates schema objects.
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|_| integrity("invalid embedded selected Arrow schema"))?;
    let bytes = if decoded.len() > 8 && decoded[..4] == [255; 4] {
        &decoded[8..]
    } else {
        decoded.as_slice()
    };
    let options = {
        // Infer the pinned verifier type through the function argument. These
        // are resource bounds, independent of producer/writer identity.
        let mut options = Default::default();
        let _ = arrow_ipc::root_as_message_with_opts(&options, &[]);
        options.max_depth = 64;
        options.max_tables = 4096;
        options.max_apparent_size = 16 * 1024 * 1024;
        options
    };
    let message = arrow_ipc::root_as_message_with_opts(&options, bytes).map_err(|_| {
        integrity("embedded selected Arrow schema exceeds metadata budget or is malformed")
    })?;
    if message.header_as_schema().is_none() {
        return Err(integrity("invalid embedded selected Arrow schema"));
    }
    Ok(())
}

fn read_conditional(
    backend: &impl Backend,
    key: &ObjectKey,
    meta: &ObjectMeta,
    offset: u64,
    length: usize,
) -> grv_storage::Result<Vec<u8>> {
    // Reject oversize response writes before growing the allocation.
    struct Sink {
        bytes: Vec<u8>,
        limit: usize,
    }
    impl Write for Sink {
        fn write(&mut self, value: &[u8]) -> std::io::Result<usize> {
            if value.len() > self.limit - self.bytes.len() {
                return Err(std::io::Error::other("range exceeds requested length"));
            }
            self.bytes.extend_from_slice(value);
            Ok(value.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut sink = Sink {
        bytes: Vec::with_capacity(length),
        limit: length,
    };
    let actual = backend.read_range(key, &meta.validator, offset, length, &mut sink)?;
    if actual != *meta || sink.bytes.len() != length {
        return Err(grv_storage::Error::new(
            ErrorKind::Integrity,
            "conditional range metadata or length changed",
        ));
    }
    Ok(sink.bytes)
}
fn range_file_metadata(
    backend: &impl Backend,
    key: &ObjectKey,
    expected: &DataFile,
) -> Result<Option<(ArrowReaderMetadata, ObjectMeta)>> {
    let meta = backend.head(key).map_err(data_error)?;
    if meta.size != expected.size {
        return Err(integrity("selected file fails manifest size"));
    }
    if meta.validator != expected.validator {
        return Ok(None);
    }
    if meta.size.get() < 12 {
        return Err(integrity("invalid selected Parquet file"));
    }
    let header = match read_conditional(backend, key, &meta, 0, 4) {
        Ok(bytes) => bytes,
        Err(error) if error.kind == ErrorKind::Unsupported => return Ok(None),
        Err(error) => return Err(data_error(error)),
    };
    if header != b"PAR1" {
        return Err(integrity("invalid selected Parquet header"));
    }
    let trailer =
        read_conditional(backend, key, &meta, meta.size.get() - 8, 8).map_err(data_error)?;
    let length = footer_size(&trailer, meta.size.get())?;
    let start = meta.size.get() - 8 - length as u64;
    let mut tail = Vec::with_capacity(length + 8);
    while tail.len() < length {
        let count = RANGE_BYTES.min(length - tail.len());
        tail.extend_from_slice(
            &read_conditional(backend, key, &meta, start + tail.len() as u64, count)
                .map_err(data_error)?,
        );
    }
    tail.extend_from_slice(&trailer);
    Ok(Some((decode_footer(meta.size.get(), tail)?, meta)))
}
fn full_file_metadata(
    backend: &impl Backend,
    key: &ObjectKey,
    expected: &DataFile,
    parent: &Path,
) -> Result<(ArrowReaderMetadata, ObjectMeta, tempfile::NamedTempFile)> {
    let mut file = tempfile::NamedTempFile::new_in(parent)
        .map_err(|e| public_error(ErrorCode::BackendFailure, e.to_string()))?;
    struct Sink<'a> {
        file: &'a mut File,
        hash: Sha256,
        bytes: u64,
        limit: u64,
        exceeded: bool,
    }
    impl Write for Sink<'_> {
        fn write(&mut self, value: &[u8]) -> std::io::Result<usize> {
            if value.len() as u64 > self.limit.saturating_sub(self.bytes) {
                self.exceeded = true;
                return Err(std::io::Error::other("selected file exceeds manifest size"));
            }
            self.file.write_all(value)?;
            self.bytes += value.len() as u64;
            self.hash.update(value);
            Ok(value.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.file.flush()
        }
    }
    let mut sink = Sink {
        file: file.as_file_mut(),
        hash: Sha256::new(),
        bytes: 0,
        limit: expected.size.get(),
        exceeded: false,
    };
    let meta = backend.get(key, &mut sink).map_err(|error| {
        if sink.exceeded {
            integrity("selected file exceeds manifest size")
        } else {
            data_error(error)
        }
    })?;
    if meta.size != expected.size
        || sink.bytes != expected.size.get()
        || format!("{:x}", sink.hash.finalize()) != expected.sha256.as_str()
    {
        return Err(integrity("selected file fails manifest size or SHA-256"));
    }
    if meta.size.get() < 12 {
        return Err(integrity("invalid selected Parquet file"));
    }
    let staged = file.as_file_mut();
    let io = |e: std::io::Error| public_error(ErrorCode::BackendFailure, e.to_string());
    staged.seek(SeekFrom::Start(0)).map_err(io)?;
    let mut header = [0; 4];
    staged.read_exact(&mut header).map_err(io)?;
    if &header != b"PAR1" {
        return Err(integrity("invalid selected Parquet header"));
    }
    staged.seek(SeekFrom::End(-8)).map_err(io)?;
    let mut trailer = [0; 8];
    staged.read_exact(&mut trailer).map_err(io)?;
    let length = footer_size(&trailer, meta.size.get())?;
    staged
        .seek(SeekFrom::End(-(length as i64 + 8)))
        .map_err(io)?;
    let mut tail = vec![0; length + 8];
    staged.read_exact(&mut tail).map_err(io)?;
    Ok((decode_footer(meta.size.get(), tail)?, meta, file))
}
/// Compact-Thrift preflight runs without allocation. The pinned decoder can
/// reserve a vector from an untrusted list count before reading its elements.
/// Charge 4 KiB per value/container and four times each binary payload against
/// a 32-MiB decoded-metadata admission budget; the serialized tail is bounded
/// separately at 16 MiB. This conservatively bounds complexity before any
/// Arrow/Parquet allocations and makes malicious count headers fail early.
fn footer_budget(bytes: &[u8]) -> Result<()> {
    struct Check<'a> {
        bytes: &'a [u8],
        at: usize,
        budget: usize,
    }
    impl Check<'_> {
        fn charge(&mut self, count: usize) -> Option<()> {
            self.budget = self.budget.checked_sub(count)?;
            Some(())
        }
        fn byte(&mut self) -> Option<u8> {
            let b = *self.bytes.get(self.at)?;
            self.at += 1;
            Some(b)
        }
        fn skip(&mut self, count: usize) -> Option<()> {
            self.at = self
                .at
                .checked_add(count)
                .filter(|v| *v <= self.bytes.len())?;
            Some(())
        }
        fn varint(&mut self) -> Option<u64> {
            let mut value = 0;
            for shift in (0..70).step_by(7) {
                let b = self.byte()?;
                if shift == 63 && b > 1 {
                    return None;
                }
                value |= ((b & 127) as u64) << shift;
                if b & 128 == 0 {
                    return Some(value);
                }
            }
            None
        }
        fn value(&mut self, kind: u8, depth: usize, field: bool) -> Option<()> {
            if depth > 64 {
                return None;
            }
            self.charge(4096)?;
            match kind {
                1 | 2 => {
                    if !field && !matches!(self.byte()?, 1 | 2) {
                        return None;
                    }
                }
                3 => self.skip(1)?,
                4..=6 => {
                    self.varint()?;
                }
                7 => self.skip(8)?,
                8 => {
                    let n = usize::try_from(self.varint()?).ok()?;
                    self.charge(n.checked_mul(4)?)?;
                    self.skip(n)?;
                }
                9 | 10 => {
                    let header = self.byte()?;
                    let kind = header & 15;
                    let count = if header >> 4 == 15 {
                        usize::try_from(self.varint()?).ok()?
                    } else {
                        (header >> 4) as usize
                    };
                    if count > self.budget / 4096 {
                        return None;
                    }
                    for _ in 0..count {
                        self.value(kind, depth + 1, false)?;
                    }
                }
                11 => {
                    let count = usize::try_from(self.varint()?).ok()?;
                    if count > self.budget / 8192 {
                        return None;
                    }
                    if count > 0 {
                        let kinds = self.byte()?;
                        for _ in 0..count {
                            self.value(kinds >> 4, depth + 1, false)?;
                            self.value(kinds & 15, depth + 1, false)?;
                        }
                    }
                }
                12 => loop {
                    let header = self.byte()?;
                    if header == 0 {
                        break;
                    }
                    if header & 15 == 0 {
                        return None;
                    }
                    if header >> 4 == 0 {
                        self.varint()?;
                    }
                    self.value(header & 15, depth + 1, true)?;
                },
                _ => return None,
            }
            Some(())
        }
    }
    let mut check = Check {
        bytes,
        at: 0,
        budget: 32 * 1024 * 1024,
    };
    if check.value(12, 0, false).is_none() || check.at != bytes.len() {
        return Err(integrity(
            "selected Parquet metadata exceeds bounded decoder budget or is malformed",
        ));
    }
    Ok(())
}

pub struct TableSelection {
    pub table: Name,
    /// None selects all revision entries; Some([]) is an explicitly empty scope.
    pub partitions: Option<Vec<Partition>>,
    pub expect_columns: Option<Vec<Column>>,
    pub expect_partition_keys: Option<Vec<Name>>,
    /// Caller supplies only the trustworthy prior contract of this binding.
    pub prior_source_contract: Option<TableContract>,
}
pub struct SelectedTable {
    pub table: Name,
    pub partitions: Vec<Partition>,
    pub contract: TableContract,
}
pub struct VerifiedSelection {
    pub revision: U64,
    pub tables: Vec<SelectedTable>,
    pub files: Vec<VerifiedFile>,
    // Keep staging paths alive until the adapter finishes reading.
    _staging: tempfile::TempDir,
}
fn failure(code: ErrorCode, message: &str) -> grv_types::PublicError {
    public_error(code, message)
}
fn integrity(message: &str) -> grv_types::PublicError {
    failure(ErrorCode::IntegrityFailure, message)
}
fn unavailable(message: &str) -> grv_types::PublicError {
    failure(ErrorCode::Unavailable, message)
}
fn read<T: Validate + for<'de> serde::Deserialize<'de>>(
    backend: &impl Backend,
    path: &str,
) -> Result<T> {
    let bytes = backend
        .read_bytes(
            &ObjectKey::new(path).map_err(backend_error)?,
            METADATA_LIMIT,
        )
        .map_err(|error| {
            if error.kind == ErrorKind::NotFound {
                unavailable("selected source metadata is missing")
            } else {
                backend_error(error)
            }
        })?
        .0;
    decode_record(&bytes).map_err(backend_error)
}
fn absent(backend: &impl Backend, path: &str) -> Result<bool> {
    match backend.head(&ObjectKey::new(path).map_err(backend_error)?) {
        Ok(_) => Ok(false),
        Err(error) if error.kind == ErrorKind::NotFound => Ok(true),
        Err(error) => Err(backend_error(error)),
    }
}
fn logical_contract(columns: Vec<Column>, keys: Vec<Name>) -> Result<TableContract> {
    let contract = TableContract {
        columns,
        partition_keys: keys,
        extensions: serde_json::json!({}),
        column_ext: serde_json::json!({}),
    };
    contract.validate().map_err(|e| integrity(&e.to_string()))?;
    Ok(contract)
}
/// No source write, lease, or authentication occurs here. Receipt resolution
/// must precede this call. Fixed build inputs additionally require holds.
pub fn verify<B: Backend>(
    store: &Store<B>,
    dataset: &Name,
    selector: &RequestedRevision,
    tables: &[TableSelection],
    parent: &Path,
) -> Result<VerifiedSelection> {
    verify_with_access(store, dataset, selector, tables, parent, FileAccess::Local)
}
/// Select exact remote S3 coordinates after committed run/tombstone fences and
/// schema/footer verification. Matching manifest validators authorize bounded
/// conditional metadata reads; changed validators require full SHA-256 proof. Coordinates convey no
/// credentials; the adapter must independently authorize its read-only access.
pub fn verify_with_access<B: Backend>(
    store: &Store<B>,
    dataset: &Name,
    selector: &RequestedRevision,
    tables: &[TableSelection],
    parent: &Path,
    access: FileAccess,
) -> Result<VerifiedSelection> {
    if access == FileAccess::S3View
        && store
            .backend
            .s3_data_uri(&ObjectKey::new("grv.json").map_err(backend_error)?)
            .map_err(backend_error)?
            .is_none()
    {
        return Err(failure(
            ErrorCode::UnsupportedCapability,
            "S3 views require an S3 source backend",
        ));
    }
    let parent = std::fs::canonicalize(parent)
        .map_err(|_| failure(ErrorCode::InvalidArgument, "staging parent does not exist"))?;
    let staging = tempfile::tempdir_in(parent)
        .map_err(|e| public_error(ErrorCode::BackendFailure, e.to_string()))?;
    let number = match selector {
        RequestedRevision::Latest(_) => revision::read_latest(store, dataset)?
            .map_or(Counter::from(0), |(latest, _)| latest.revision),
        RequestedRevision::Revision(number) => Counter::new(number.get()).map_err(backend_error)?,
    };
    if !revision::committed(store, dataset, number, staging.path())? {
        return Err(failure(
            ErrorCode::NotFound,
            "requested revision was never committed",
        ));
    }
    let state = if number.get() == 0 {
        revision::State::new()
    } else {
        revision::read(store, dataset, number, staging.path())?.state
    };
    let mut result = VerifiedSelection {
        revision: U64::new(number.get()).unwrap(),
        tables: vec![],
        files: vec![],
        _staging: staging,
    };
    let mut names = BTreeSet::new();
    let mut metadata_bytes = 0usize;
    for selection in tables {
        if !names.insert(&selection.table) {
            return Err(failure(
                ErrorCode::InvalidDeclaration,
                "duplicate source table",
            ));
        }
        let base = format!("datasets/{dataset}/{}", selection.table);
        let layout = if absent(&store.backend, &format!("{base}/.layout.json"))? {
            if state.values().any(|entry| entry.table == selection.table) {
                return Err(integrity("selected table has no durable layout"));
            }
            TableLayout {
                table: selection.table.clone(),
                partition_keys: selection
                    .expect_partition_keys
                    .clone()
                    .or_else(|| {
                        selection
                            .prior_source_contract
                            .as_ref()
                            .map(|c| c.partition_keys.clone())
                    })
                    .unwrap_or_default(),
                extensions: None,
            }
        } else {
            read::<TableLayout>(&store.backend, &format!("{base}/.layout.json"))?
        };
        layout.validate().map_err(backend_error)?;
        if layout.table != selection.table {
            return Err(integrity("table layout differs from its path"));
        }
        if layout
            .extensions
            .as_ref()
            .is_some_and(|extensions| !extensions.is_empty())
        {
            return Err(failure(
                ErrorCode::UnsupportedCapability,
                "source table extensions are not implemented",
            ));
        }
        if selection
            .expect_partition_keys
            .as_ref()
            .is_some_and(|keys| keys != &layout.partition_keys)
        {
            return Err(failure(
                ErrorCode::InvalidDeclaration,
                "source partition keys differ from expect",
            ));
        }
        if let Some(tuples) = &selection.partitions {
            let mut selected = BTreeSet::new();
            for tuple in tuples {
                let path = layout.partition_path(tuple).map_err(|_| {
                    failure(
                        ErrorCode::InvalidDeclaration,
                        "invalid source partition selector",
                    )
                })?;
                if !selected.insert(path) {
                    return Err(failure(
                        ErrorCode::InvalidDeclaration,
                        "duplicate source partition selector",
                    ));
                }
            }
        }
        let mut source: Option<TableContract> = None;
        let mut partitions = Vec::new();
        for entry in state
            .values()
            .filter(|entry| entry.table == selection.table)
        {
            let partition = layout
                .parse_partition(&entry.partition)
                .map_err(backend_error)?;
            if selection
                .partitions
                .as_ref()
                .is_some_and(|tuples| !tuples.contains(&partition))
            {
                continue;
            }
            partitions.push(partition.clone());
            let folder = if entry.partition.is_empty() {
                base.clone()
            } else {
                format!("{base}/{}", entry.partition)
            };
            let version = format!("{folder}/version={}", entry.version);
            if !absent(&store.backend, &format!("{version}/.pruned"))? {
                return Err(unavailable("selected version is pruned"));
            }
            let manifest: VersionManifest =
                read(&store.backend, &format!("{version}/manifest.json"))?;
            if manifest.table != selection.table
                || manifest.partition != partition
                || manifest.version != entry.version
                || manifest.run_id != entry.run_id
            {
                return Err(integrity("selected manifest differs from revision entry"));
            }
            let baseline: SchemaBaseline = read(&store.backend, &format!("{base}/.schema.json"))?;
            if baseline.table != selection.table {
                return Err(integrity("schema baseline differs from table"));
            }
            if baseline
                .columns
                .iter()
                .any(|c| c.ext.as_ref().is_some_and(|ext| !ext.is_empty()))
            {
                return Err(failure(
                    ErrorCode::UnsupportedCapability,
                    "source column extensions are not implemented",
                ));
            }
            let mut version_schema = None;
            let mut rows = 0u64;
            for expected in &manifest.data_files {
                let key = ObjectKey::new(format!("{version}/{}", expected.name))
                    .map_err(backend_error)?;
                let fast = if access == FileAccess::S3View {
                    range_file_metadata(&store.backend, &key, expected)?
                } else {
                    None
                };
                let (builder, metadata, file) = match fast {
                    Some((builder, metadata)) => (builder, metadata, None),
                    None => {
                        let (builder, metadata, file) = full_file_metadata(
                            &store.backend,
                            &key,
                            expected,
                            result._staging.path(),
                        )?;
                        (builder, metadata, Some(file))
                    }
                };
                let columns = baseline
                    .columns
                    .get(..builder.schema().fields().len())
                    .ok_or_else(|| integrity("selected file schema exceeds baseline"))?
                    .iter()
                    .map(|c| Column {
                        name: c.name.clone(),
                        logical_type: c.logical_type.clone(),
                    })
                    .collect();
                let actual = logical_contract(columns, layout.partition_keys.clone())?;
                let arrow = contract::arrow_schema(&actual)
                    .map_err(|_| integrity("unsupported selected logical schema"))?;
                if arrow
                    .fields()
                    .iter()
                    .zip(builder.schema().fields())
                    .any(|(a, b)| a.name() != b.name() || a.data_type() != b.data_type())
                {
                    return Err(integrity(
                        "selected file schema differs from durable baseline prefix",
                    ));
                }
                if version_schema
                    .as_ref()
                    .is_some_and(|schema| schema != &actual)
                {
                    return Err(integrity("selected version files have different schemas"));
                }
                version_schema = Some(actual.clone());
                let count = u64::try_from(builder.metadata().file_metadata().num_rows())
                    .map_err(|_| integrity("negative selected row count"))?;
                rows = rows
                    .checked_add(count)
                    .ok_or_else(|| integrity("selected row count overflow"))?;
                if let Some(current) = &source {
                    let common = current.columns.len().min(actual.columns.len());
                    if current.columns[..common] != actual.columns[..common] {
                        return Err(integrity(
                            "selected version schemas are not prefix-compatible",
                        ));
                    }
                }
                if source
                    .as_ref()
                    .is_none_or(|current| current.columns.len() < actual.columns.len())
                {
                    source = Some(actual.clone());
                }
                let location = match access {
                    FileAccess::Local => {
                        let path = file
                            .ok_or_else(|| integrity("local source has no verified staging file"))?
                            .into_temp_path()
                            .keep()
                            .map_err(|_| {
                                failure(
                                    ErrorCode::BackendFailure,
                                    "cannot retain verified staging file",
                                )
                            })?;
                        path.to_str()
                            .ok_or_else(|| {
                                failure(ErrorCode::InvalidArgument, "staging path must be UTF-8")
                            })?
                            .into()
                    }
                    FileAccess::S3View => store
                        .backend
                        .s3_data_uri(&key)
                        .map_err(backend_error)?
                        .ok_or_else(|| {
                        integrity("S3 source lost its exact object coordinates")
                    })?,
                };
                let verified = VerifiedFile {
                    table: selection.table.clone(),
                    partition: serde_json::to_value(&partition).unwrap(),
                    version: U64::new(entry.version.get()).unwrap(),
                    schema: FileSchema {
                        columns: actual.columns,
                    },
                    access,
                    location,
                    size: U64::new(expected.size.get()).unwrap(),
                    sha256: expected.sha256.clone(),
                    validator: metadata.validator.as_str().into(),
                };
                verified
                    .validate()
                    .map_err(|_| integrity("invalid exact verified file coordinates"))?;
                result.files.push(verified);
                metadata_bytes = metadata_bytes
                    .checked_add(
                        serde_json::to_vec(result.files.last().unwrap())
                            .map_err(|_| integrity("file metadata encoding failed"))?
                            .len(),
                    )
                    .filter(|total| *total <= METADATA_LIMIT)
                    .ok_or_else(|| integrity("verified selection exceeds metadata budget"))?;
            }
            if rows != manifest.row_count.get() {
                return Err(integrity(
                    "selected Parquet row count differs from manifest",
                ));
            }
            if !absent(&store.backend, &format!("{version}/.pruned"))? {
                return Err(unavailable("selected version was pruned while verifying"));
            }
        }
        let source = match source {
            Some(contract) => contract,
            None => {
                if let Some(columns) = &selection.expect_columns {
                    logical_contract(columns.clone(), layout.partition_keys.clone())?
                } else if let Some(prior) = &selection.prior_source_contract {
                    prior.validate().map_err(|_| {
                        failure(
                            ErrorCode::ProtocolFailure,
                            "invalid recorded source contract",
                        )
                    })?;
                    if prior.partition_keys != layout.partition_keys {
                        return Err(failure(
                            ErrorCode::RequestMismatch,
                            "recorded source layout changed",
                        ));
                    }
                    prior.clone()
                } else {
                    return Err(failure(
                        ErrorCode::InvalidDeclaration,
                        "empty source requires expect.columns or recorded source contract",
                    ));
                }
            }
        };
        if selection
            .expect_columns
            .as_ref()
            .is_some_and(|columns| columns != &source.columns)
        {
            return Err(failure(
                ErrorCode::InvalidDeclaration,
                "selected source schema differs from expect.columns",
            ));
        }
        metadata_bytes = metadata_bytes
            .checked_add(
                serde_json::to_vec(&source)
                    .map_err(|_| integrity("source contract encoding failed"))?
                    .len(),
            )
            .and_then(|total| total.checked_add(serde_json::to_vec(&partitions).ok()?.len()))
            .filter(|total| *total <= METADATA_LIMIT)
            .ok_or_else(|| integrity("verified selection exceeds metadata budget"))?;
        result.tables.push(SelectedTable {
            table: selection.table.clone(),
            partitions,
            contract: source,
        });
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        canonical::Sorter,
        clock::{Clock, SystemClock},
        ownership::{Ownership, ReservationProgress},
        store::InitOptions,
    };
    use grv_storage::{
        ListEntry, ListMode, LocalBackend, ObjectPrefix, Validator,
        model::{ClaimOutcome, Latest, encode_record},
    };
    use grv_types::RunId;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Copy, Default)]
    enum Fault {
        #[default]
        None,
        Unsupported,
        Changed,
        Missing,
        WrongMeta,
        Short,
        Oversized,
        PrunedAfterReads,
    }
    #[derive(Default)]
    struct Traffic {
        ranges: Vec<(u64, usize)>,
        full: usize,
        writes: usize,
    }
    struct Remote {
        local: LocalBackend,
        data_key: ObjectKey,
        data: Vec<u8>,
        meta: ObjectMeta,
        fault: Fault,
        traffic: Mutex<Traffic>,
    }
    impl Backend for Remote {
        fn s3_data_uri(&self, key: &ObjectKey) -> grv_storage::Result<Option<String>> {
            Ok(Some(format!("s3://test-bucket/root/{}", key.as_str())))
        }
        fn head(&self, key: &ObjectKey) -> grv_storage::Result<ObjectMeta> {
            if key == &self.data_key
                || (key.as_str().ends_with("/.pruned")
                    && matches!(self.fault, Fault::PrunedAfterReads)
                    && !self.traffic.lock().unwrap().ranges.is_empty())
            {
                Ok(self.meta.clone())
            } else {
                self.local.head(key)
            }
        }
        fn get(&self, key: &ObjectKey, sink: &mut dyn Write) -> grv_storage::Result<ObjectMeta> {
            if key != &self.data_key {
                return self.local.get(key, sink);
            }
            self.traffic.lock().unwrap().full += 1;
            sink.write_all(&self.data)
                .map_err(|e| grv_storage::Error::new(ErrorKind::Io, e.to_string()))?;
            Ok(self.meta.clone())
        }
        fn read_range(
            &self,
            key: &ObjectKey,
            expected: &Validator,
            offset: u64,
            length: usize,
            sink: &mut dyn Write,
        ) -> grv_storage::Result<ObjectMeta> {
            assert_eq!(key, &self.data_key);
            assert!((1..=RANGE_BYTES).contains(&length));
            assert_eq!(expected, &self.meta.validator);
            let mut traffic = self.traffic.lock().unwrap();
            traffic.ranges.push((offset, length));
            if traffic.ranges.len() == 2 {
                let kind = match self.fault {
                    Fault::Changed => Some(ErrorKind::PreconditionFailed),
                    Fault::Missing => Some(ErrorKind::NotFound),
                    _ => None,
                };
                if let Some(kind) = kind {
                    return Err(grv_storage::Error::new(kind, "injected range fault"));
                }
            }
            if matches!(self.fault, Fault::Unsupported) {
                return Err(grv_storage::Error::new(
                    ErrorKind::Unsupported,
                    "no range support",
                ));
            }
            let end = offset as usize + length;
            let bytes = &self.data[offset as usize..end];
            if matches!(self.fault, Fault::Oversized) {
                sink.write_all(&vec![0; length + 1]).map_err(|_| {
                    grv_storage::Error::new(ErrorKind::Integrity, "oversized range")
                })?;
            } else {
                sink.write_all(if matches!(self.fault, Fault::Short) {
                    &bytes[..length - 1]
                } else {
                    bytes
                })
                .map_err(|e| grv_storage::Error::new(ErrorKind::Io, e.to_string()))?;
            }
            let mut meta = self.meta.clone();
            if matches!(self.fault, Fault::WrongMeta) {
                meta.validator = Validator::new("other-generation").unwrap();
            }
            Ok(meta)
        }
        fn list(
            &self,
            prefix: &ObjectPrefix,
            mode: ListMode,
        ) -> grv_storage::Result<Vec<ListEntry>> {
            self.local.list(prefix, mode)
        }
        fn delete(&self, key: &ObjectKey) -> grv_storage::Result<()> {
            self.traffic.lock().unwrap().writes += 1;
            self.local.delete(key)
        }
        fn conditional_create(
            &self,
            key: &ObjectKey,
            source: &mut dyn Read,
        ) -> grv_storage::Result<Validator> {
            self.traffic.lock().unwrap().writes += 1;
            self.local.conditional_create(key, source)
        }
        fn conditional_put(
            &self,
            key: &ObjectKey,
            expected: &Validator,
            source: &mut dyn Read,
        ) -> grv_storage::Result<Validator> {
            self.traffic.lock().unwrap().writes += 1;
            self.local.conditional_put(key, expected, source)
        }
    }
    struct Fixture {
        root: tempfile::TempDir,
        store: Store<Remote>,
        expected: DataFile,
    }
    fn name(value: &str) -> Name {
        Name::new(value).unwrap()
    }
    fn run(value: u32) -> RunId {
        RunId::new(format!("01ARZ3NDEKTSV4RRFFQ{value:07}")).unwrap()
    }
    impl Fixture {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let store = Store::initialize(
                LocalBackend::open(root.path()).unwrap(),
                InitOptions::default(),
            )
            .unwrap()
            .0;
            let clock = SystemClock::default();
            let own = Ownership::new(&store, &clock, 60).unwrap();
            let mut owner = own
                .prepare_run(name("upstream"), run(50), Counter::from(0), vec![], None)
                .unwrap();
            own.commit_run(&owner).unwrap();
            own.confirm_holds(&mut owner).unwrap();
            let contract = logical_contract(
                vec![Column {
                    name: "id".into(),
                    logical_type: serde_json::json!("int64"),
                }],
                vec![],
            )
            .unwrap();
            let mut sorter = Sorter::new(contract.clone(), root.path()).unwrap();
            sorter
                .append(
                    &arrow_array::RecordBatch::try_new(
                        contract::arrow_schema(&contract).unwrap(),
                        vec![Arc::new(arrow_array::Int64Array::from(
                            (0..1000).rev().collect::<Vec<i64>>(),
                        ))],
                    )
                    .unwrap(),
                )
                .unwrap();
            let staged = sorter.finish().unwrap();
            let intent = own
                .prepare_reservation(
                    &owner,
                    TableLayout {
                        table: name("rows"),
                        partition_keys: vec![],
                        extensions: None,
                    },
                    Partition::new(),
                    &contract,
                )
                .unwrap();
            let mut reservation = own
                .reserve_authorized(&owner, &intent, &mut ReservationProgress::Prepared, |_| {
                    Ok(())
                })
                .unwrap();
            own.write_group(&owner, &mut reservation, &contract, &staged, None)
                .unwrap();
            own.release(&mut reservation, ClaimOutcome::Finalized)
                .unwrap();
            own.seal(&mut owner).unwrap();
            let revision = revision::Revision {
                revision: Counter::from(1),
                previous_revision: Counter::from(0),
                operation_id: run(901),
                created_at: clock.now(),
                state: std::collections::BTreeMap::from([(
                    (name("rows"), String::new()),
                    revision::Entry {
                        table: name("rows"),
                        partition: String::new(),
                        version: Counter::from(1),
                        run_id: run(50),
                    },
                )]),
            };
            let file = revision::encode(&revision, root.path()).unwrap();
            store
                .backend
                .conditional_create(
                    &revision::revision_key(&name("upstream"), Counter::from(1)),
                    &mut File::open(file.path()).unwrap(),
                )
                .unwrap();
            store
                .backend
                .create_bytes(
                    &revision::latest_key(&name("upstream")),
                    &encode_record(&Latest {
                        revision: Counter::from(1),
                        high_water: Counter::from(1),
                        ..Latest::empty()
                    })
                    .unwrap(),
                )
                .unwrap();
            let manifest: VersionManifest = read(
                &store.backend,
                "datasets/upstream/rows/version=1/manifest.json",
            )
            .unwrap();
            assert_eq!(manifest.data_files.len(), 1);
            let expected = manifest.data_files[0].clone();
            let data_key = ObjectKey::new(format!(
                "datasets/upstream/rows/version=1/{}",
                expected.name
            ))
            .unwrap();
            let (data, meta) = store
                .backend
                .read_bytes(&data_key, 32 * 1024 * 1024)
                .unwrap();
            Self {
                root,
                store: Store {
                    parameters: store.parameters,
                    backend: Remote {
                        local: store.backend,
                        data_key,
                        data,
                        meta,
                        fault: Fault::None,
                        traffic: Mutex::new(Traffic::default()),
                    },
                },
                expected,
            }
        }
        fn verify(&self, access: FileAccess) -> Result<VerifiedSelection> {
            verify_with_access(
                &self.store,
                &name("upstream"),
                &RequestedRevision::Revision(U64::new(1).unwrap()),
                &[TableSelection {
                    table: name("rows"),
                    partitions: None,
                    expect_columns: None,
                    expect_partition_keys: None,
                    prior_source_contract: None,
                }],
                self.root.path(),
                access,
            )
        }
    }
    #[test]
    fn s3_matching_manifest_validator_reads_only_header_footer_and_never_stages_data_or_writes() {
        let f = Fixture::new();
        let selected = f.verify(FileAccess::S3View).unwrap();
        assert_eq!(selected.files.len(), 1);
        assert_eq!(selected.files[0].validator, f.expected.validator.as_str());
        let traffic = f.store.backend.traffic.lock().unwrap();
        assert_eq!(traffic.full, 0);
        assert_eq!(traffic.writes, 0);
        assert_eq!(traffic.ranges.len(), 3);
        assert_eq!(traffic.ranges[0], (0, 4));
        assert_eq!(traffic.ranges[1], (f.expected.size.get() - 8, 8));
        let footer = u32::from_le_bytes(
            f.store.backend.data[f.store.backend.data.len() - 8..f.store.backend.data.len() - 4]
                .try_into()
                .unwrap(),
        ) as usize;
        assert_eq!(
            traffic.ranges[2],
            (f.expected.size.get() - 8 - footer as u64, footer)
        );
        assert!(traffic.ranges.iter().map(|(_, n)| n).sum::<usize>() < f.store.backend.data.len());
        assert_eq!(
            std::fs::read_dir(selected._staging.path()).unwrap().count(),
            0
        );
    }
    #[test]
    fn s3_metadata_fastpath_preserves_commitment_manifest_schema_rows_and_pruning_fences() {
        for failure in [
            "uncommitted",
            "run",
            "rows",
            "schema",
            "pruned",
            "prune-race",
        ] {
            let mut f = Fixture::new();
            let local = &f.store.backend.local;
            let update = |path: &str, bytes: &[u8]| {
                let key = ObjectKey::new(path).unwrap();
                let meta = local.head(&key).unwrap();
                local.put_bytes(&key, &meta.validator, bytes).unwrap();
            };
            match failure {
                "uncommitted" => update(
                    "datasets/upstream/.states/LATEST",
                    &encode_record(&Latest {
                        revision: Counter::from(0),
                        high_water: Counter::from(1),
                        ..Latest::empty()
                    })
                    .unwrap(),
                ),
                "run" | "rows" => {
                    let mut manifest: VersionManifest =
                        read(local, "datasets/upstream/rows/version=1/manifest.json").unwrap();
                    if failure == "run" {
                        manifest.run_id = run(51);
                    } else {
                        manifest.row_count = Counter::from(999);
                    }
                    update(
                        "datasets/upstream/rows/version=1/manifest.json",
                        &encode_record(&manifest).unwrap(),
                    );
                }
                "schema" => {
                    let mut schema: SchemaBaseline =
                        read(local, "datasets/upstream/rows/.schema.json").unwrap();
                    schema.columns[0].logical_type = serde_json::json!("string");
                    update(
                        "datasets/upstream/rows/.schema.json",
                        &encode_record(&schema).unwrap(),
                    );
                }
                "pruned" => {
                    local
                        .create_bytes(
                            &ObjectKey::new("datasets/upstream/rows/version=1/.pruned").unwrap(),
                            b"opaque tombstone",
                        )
                        .unwrap();
                }
                "prune-race" => f.store.backend.fault = Fault::PrunedAfterReads,
                _ => unreachable!(),
            }
            let error = f.verify(FileAccess::S3View).err().unwrap();
            assert_eq!(
                error.code,
                match failure {
                    "uncommitted" => ErrorCode::NotFound,
                    "pruned" | "prune-race" => ErrorCode::Unavailable,
                    _ => ErrorCode::IntegrityFailure,
                },
                "{failure}"
            );
            let traffic = f.store.backend.traffic.lock().unwrap();
            assert_eq!(traffic.full, 0);
            assert_eq!(traffic.writes, 0);
            if matches!(failure, "uncommitted" | "run" | "pruned") {
                assert!(traffic.ranges.is_empty(), "{failure}");
            } else {
                assert!(!traffic.ranges.is_empty(), "{failure}");
            }
        }
    }
    #[test]
    fn s3_large_copied_footer_is_chunked_without_body_reads_and_keeps_noncanonical_writer_metadata()
    {
        use parquet::{
            arrow::ArrowWriter,
            file::{metadata::KeyValue, properties::WriterProperties},
        };
        let mut f = Fixture::new();
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            true,
        )]));
        let batch = arrow_array::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mut bytes = Vec::new();
        let properties = WriterProperties::builder()
            .set_created_by("valid copied v2 producer".into())
            .set_key_value_metadata(Some(vec![KeyValue::new(
                "opaque".into(),
                "x".repeat(140000),
            )]))
            .build();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        f.store.backend.data = bytes;
        f.store.backend.meta.size = Counter::new(f.store.backend.data.len() as u64).unwrap();
        f.expected.size = f.store.backend.meta.size;
        let (metadata, _) =
            range_file_metadata(&f.store.backend, &f.store.backend.data_key, &f.expected)
                .unwrap()
                .unwrap();
        assert_eq!(
            metadata.metadata().file_metadata().created_by(),
            Some("valid copied v2 producer")
        );
        assert_eq!(metadata.metadata().file_metadata().num_rows(), 3);
        let footer = u32::from_le_bytes(
            f.store.backend.data[f.store.backend.data.len() - 8..f.store.backend.data.len() - 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let traffic = f.store.backend.traffic.lock().unwrap();
        assert_eq!(traffic.full, 0);
        assert_eq!(traffic.writes, 0);
        assert_eq!(traffic.ranges.len(), 5);
        assert_eq!(traffic.ranges[2].1, RANGE_BYTES);
        assert_eq!(traffic.ranges[3].1, RANGE_BYTES);
        assert_eq!(
            traffic.ranges.iter().map(|(_, n)| n).sum::<usize>(),
            footer + 12
        );
    }
    #[test]
    fn s3_retagged_identical_bytes_require_full_sha_and_report_current_validator() {
        let mut f = Fixture::new();
        f.store.backend.meta.validator = Validator::new("retag").unwrap();
        let selected = f.verify(FileAccess::S3View).unwrap();
        assert_eq!(selected.files[0].validator, "retag");
        let traffic = f.store.backend.traffic.lock().unwrap();
        assert_eq!(traffic.full, 1);
        assert!(traffic.ranges.is_empty());
        assert_eq!(traffic.writes, 0);
    }
    #[test]
    fn s3_retagged_changed_same_size_body_fails_sha() {
        let mut f = Fixture::new();
        f.store.backend.meta.validator = Validator::new("retag").unwrap();
        f.store.backend.data[10] ^= 1;
        assert_eq!(
            f.verify(FileAccess::S3View).err().unwrap().code,
            ErrorCode::IntegrityFailure
        );
        assert_eq!(f.store.backend.traffic.lock().unwrap().full, 1);
    }
    #[test]
    fn s3_unsupported_ranges_conservatively_verify_complete_sha_and_local_always_does() {
        for access in [FileAccess::Local, FileAccess::S3View] {
            let mut f = Fixture::new();
            f.store.backend.fault = Fault::Unsupported;
            f.verify(access).unwrap();
            let traffic = f.store.backend.traffic.lock().unwrap();
            assert_eq!(traffic.full, 1);
            assert_eq!(traffic.writes, 0);
        }
    }
    #[test]
    fn s3_conditional_ranges_fail_closed_for_changed_missing_inconsistent_and_bad_lengths() {
        for (fault, code) in [
            (Fault::Changed, ErrorCode::IntegrityFailure),
            (Fault::Missing, ErrorCode::Unavailable),
            (Fault::WrongMeta, ErrorCode::IntegrityFailure),
            (Fault::Short, ErrorCode::IntegrityFailure),
            (Fault::Oversized, ErrorCode::IntegrityFailure),
        ] {
            let mut f = Fixture::new();
            f.store.backend.fault = fault;
            assert_eq!(f.verify(FileAccess::S3View).err().unwrap().code, code);
            let traffic = f.store.backend.traffic.lock().unwrap();
            assert_eq!(traffic.full, 0);
            assert_eq!(traffic.writes, 0);
        }
    }
    #[test]
    fn s3_matching_validator_still_rejects_malformed_header_footer_and_size() {
        for position in [0, usize::MAX] {
            let mut f = Fixture::new();
            let index = if position == 0 {
                0
            } else {
                f.store.backend.data.len() - 1
            };
            f.store.backend.data[index] ^= 1;
            assert_eq!(
                f.verify(FileAccess::S3View).err().unwrap().code,
                ErrorCode::IntegrityFailure
            );
            assert_eq!(f.store.backend.traffic.lock().unwrap().full, 0);
        }
        let mut f = Fixture::new();
        f.store.backend.meta.size = Counter::new(f.expected.size.get() + 1).unwrap();
        assert_eq!(
            f.verify(FileAccess::S3View).err().unwrap().code,
            ErrorCode::IntegrityFailure
        );
        assert!(f.store.backend.traffic.lock().unwrap().ranges.is_empty());
    }
    #[test]
    fn embedded_arrow_schema_preflight_limits_expansion_and_rejects_malformed_metadata() {
        let ordinary = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            true,
        )]);
        arrow_schema_budget(&parquet::arrow::encode_arrow_schema(&ordinary)).unwrap();
        let excessive = arrow_schema::Schema::new(
            (0..5000)
                .map(|n| {
                    arrow_schema::Field::new(
                        format!("field{n}"),
                        arrow_schema::DataType::Int64,
                        true,
                    )
                })
                .collect::<Vec<_>>(),
        );
        assert!(arrow_schema_budget(&parquet::arrow::encode_arrow_schema(&excessive)).is_err());
        for invalid in ["invalid base64", "AAAA", ""] {
            assert!(arrow_schema_budget(invalid).is_err());
        }
    }
    #[test]
    fn footer_reader_forbids_body_and_decoder_budget_rejects_huge_list_before_allocating() {
        let reader = FooterReader {
            size: 100,
            start: 90,
            tail: Bytes::from_static(b"0123456789"),
        };
        assert_eq!(reader.get_bytes(91, 2).unwrap(), Bytes::from_static(b"12"));
        assert!(reader.get_bytes(0, 4).is_err());
        assert!(reader.get_bytes(99, 2).is_err());
        // field1=list<struct> of u32::MAX elements, then STOP. Pinned decoder
        // must never see the declared capacity even with a tiny footer body.
        assert!(footer_budget(&[0x19, 0xfc, 0xff, 0xff, 0xff, 0xff, 0x0f, 0]).is_err());
        assert!(footer_budget(&[0x18, 0xff, 0xff, 0xff, 0xff, 0x0f, 0]).is_err());
        assert!(footer_budget(&[0, 0]).is_err());
    }
}
