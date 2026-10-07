//! Pinned native owner and bounded transfer seam. The legacy fixed-integer
//! encoding is a proof format; production extraction uses exact Arrow conversion.
use crate::{
    conversion::{Scalar, SourceType, TickUnit},
    lock::WorkspaceLock,
    worker::{Engine, Interrupt, Worker},
};
use std::{
    ffi::{CStr, CString, c_char, c_void},
    io,
    marker::PhantomData,
    path::Path,
    rc::Rc,
    sync::Arc,
};
pub(crate) const METADATA_BYTES: usize = 2 * 1024 * 1024;

unsafe extern "C" {
    fn grv_native_load_static_extensions(
        database: *mut c_void,
        error: *mut c_char,
        capacity: usize,
    ) -> i32;
    fn grv_native_s3_configure(
        owner: *mut c_void,
        httpfs: *const c_char,
        aws: *const c_char,
        scope: *const c_char,
        profile: *const c_char,
        region: *const c_char,
        error: *mut c_char,
        capacity: usize,
    ) -> i32;
    fn grv_native_s3_query(
        owner: *mut c_void,
        locations: *const *const c_char,
        sizes: *const u64,
        hashes: *const *const c_char,
        validators: *const *const c_char,
        count: usize,
        query: *const c_char,
        output: *mut u8,
        allowance: usize,
        written: *mut usize,
        error: *mut c_char,
        capacity: usize,
    ) -> i32;
    fn grv_native_stage_build_query(
        owner: *mut c_void,
        query: *const c_char,
        projection: *const c_char,
        stage: *const c_char,
        expected: *const c_char,
        schema_output: *mut u8,
        schema_allowance: usize,
        schema_written: *mut usize,
        error: *mut c_char,
        capacity: usize,
    ) -> i32;
    fn grv_native_close_invocation(
        owner: *mut c_void,
        path: *const c_char,
        error: *mut c_char,
        capacity: usize,
    ) -> i32;
    fn grv_native_stage_pull_query(
        owner: *mut c_void,
        query: *const c_char,
        stage: *const c_char,
        expected: *const c_char,
        error: *mut c_char,
        capacity: usize,
    ) -> i32;
    fn grv_native_private_command(
        owner: *mut c_void,
        query: *const c_char,
        output: *mut u8,
        allowance: usize,
        written: *mut usize,
        error: *mut c_char,
        capacity: usize,
    ) -> i32;
    fn grv_native_base_table(
        owner: *mut c_void,
        schema: *const c_char,
        table: *const c_char,
        native: *mut i32,
        error: *mut c_char,
        capacity: usize,
    ) -> i32;
    fn grv_native_is_view(
        owner: *mut c_void,
        schema: *const c_char,
        table: *const c_char,
        view: *mut i32,
        error: *mut c_char,
        capacity: usize,
    ) -> i32;
    fn grv_native_relation_schema(
        owner: *mut c_void,
        schema: *const c_char,
        table: *const c_char,
        output: *mut u8,
        allowance: usize,
        written: *mut usize,
        error: *mut c_char,
        capacity: usize,
    ) -> i32;
    fn grv_native_schema_exists(
        owner: *mut c_void,
        schema: *const c_char,
        exists: *mut i32,
        error: *mut c_char,
        capacity: usize,
    ) -> i32;
    fn grv_native_verified_file_query(
        owner: *mut c_void,
        descriptor: i32,
        query: *const c_char,
        output: *mut u8,
        allowance: usize,
        written: *mut usize,
        error: *mut c_char,
        capacity: usize,
    ) -> i32;
    fn grv_native_relation_exists(
        owner: *mut c_void,
        schema: *const c_char,
        table: *const c_char,
        exists: *mut i32,
        error: *mut c_char,
        capacity: usize,
    ) -> i32;
    fn grv_native_managed_initialized(
        owner: *mut c_void,
        initialized: *mut i32,
        error: *mut c_char,
        capacity: usize,
    ) -> i32;
    fn grv_native_metadata_query(
        owner: *mut c_void,
        query: *const c_char,
        output: *mut u8,
        allowance: usize,
        written: *mut usize,
        error: *mut c_char,
        capacity: usize,
    ) -> i32;
    fn grv_native_version() -> *const c_char;
    fn grv_native_guard_revision() -> u32;
    fn grv_native_open(path: *const c_char, error: *mut c_char, capacity: usize) -> *mut c_void;
    fn grv_native_open_readonly(
        path: *const c_char,
        error: *mut c_char,
        capacity: usize,
    ) -> *mut c_void;
    fn grv_native_close(owner: *mut c_void);
    fn grv_native_interrupt_handle(owner: *mut c_void) -> *mut c_void;
    fn grv_native_interrupt(handle: *mut c_void);
    fn grv_native_interrupt_close(handle: *mut c_void);
    fn grv_native_begin(
        owner: *mut c_void,
        query: *const c_char,
        error: *mut c_char,
        capacity: usize,
    ) -> i32;
    fn grv_native_fetch(
        owner: *mut c_void,
        output: *mut u8,
        allowance: usize,
        written: *mut usize,
        error: *mut c_char,
        capacity: usize,
    ) -> i32;
    fn grv_native_authorize_relation(
        owner: *mut c_void,
        schema: *const c_char,
        table: *const c_char,
        error: *mut c_char,
        capacity: usize,
    ) -> i32;
    fn grv_native_stage_index(
        owner: *mut c_void,
        table_id: u64,
        query: *const c_char,
        schema: *mut u8,
        schema_allowance: usize,
        written: *mut usize,
        rows: *mut u64,
        error: *mut c_char,
        capacity: usize,
    ) -> i32;
    fn grv_native_stage(
        owner: *mut c_void,
        query: *const c_char,
        schema: *mut u8,
        schema_allowance: usize,
        schema_written: *mut usize,
        rows: *mut u64,
        error: *mut c_char,
        capacity: usize,
    ) -> i32;
    fn grv_native_stage_fetch_index(
        owner: *mut c_void,
        table_id: u64,
        output: *mut u8,
        allowance: usize,
        source_allowance: usize,
        written: *mut usize,
        error: *mut c_char,
        capacity: usize,
    ) -> i32;
    fn grv_native_snapshot_begin(owner: *mut c_void, error: *mut c_char, capacity: usize) -> i32;
    fn grv_native_snapshot_seal(owner: *mut c_void, error: *mut c_char, capacity: usize) -> i32;
    fn grv_native_stage_source(
        owner: *mut c_void,
        table_id: u64,
        source_schema: *const c_char,
        source_table: *const c_char,
        columns: *const *const c_char,
        column_count: usize,
        filter: *const c_char,
        schema: *mut u8,
        schema_allowance: usize,
        schema_written: *mut usize,
        rows: *mut u64,
        error: *mut c_char,
        capacity: usize,
    ) -> i32;
}

struct InterruptPointer(*mut c_void);
// The only operation on this shared_ptr-backed handle is DuckDB's independently
// safe Interrupt(). A native mutex fences close against in-flight calls; after
// close the handle becomes inert and retains neither a connection nor a database.
unsafe impl Send for InterruptPointer {}
unsafe impl Sync for InterruptPointer {}
impl Drop for InterruptPointer {
    fn drop(&mut self) {
        unsafe { grv_native_interrupt_close(self.0) };
    }
}
#[derive(Clone)]
pub struct NativeInterrupt(Arc<InterruptPointer>);
impl Interrupt for NativeInterrupt {
    fn interrupt(&self) {
        unsafe { grv_native_interrupt(self.0.0) };
    }
}

pub struct NativeEngine {
    owner: *mut c_void,
    extensions: Option<crate::native_extensions::Extensions>,
    interrupt: NativeInterrupt,
    // Workspace ownership outlives the connection and all active interrupt calls.
    ownership: Arc<WorkspaceLock>,
    _thread_owner: PhantomData<Rc<()>>,
}
impl Drop for NativeEngine {
    fn drop(&mut self) {
        unsafe { grv_native_close(self.owner) };
    }
}

fn decode_metadata(output: &[u8]) -> io::Result<Vec<Vec<Option<String>>>> {
    let mut bytes = output;
    fn count(bytes: &mut &[u8]) -> io::Result<usize> {
        if bytes.len() < 8 {
            return Err(io::Error::other("truncated native metadata"));
        }
        let value = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        *bytes = &bytes[8..];
        usize::try_from(value).map_err(io::Error::other)
    }
    let rows = count(&mut bytes)?;
    let columns = count(&mut bytes)?;
    if columns > 16 || rows > bytes.len() / 9 / columns.max(1) {
        return Err(io::Error::other("invalid native metadata shape"));
    }
    let mut output = Vec::with_capacity(rows);
    for _ in 0..rows {
        let mut row = Vec::with_capacity(columns);
        for _ in 0..columns {
            let tag = *bytes
                .first()
                .ok_or_else(|| io::Error::other("missing native metadata tag"))?;
            bytes = &bytes[1..];
            let length = count(&mut bytes)?;
            if length > bytes.len() || tag > 1 || tag == 0 && length != 0 {
                return Err(io::Error::other("invalid native metadata field"));
            }
            row.push(if tag == 0 {
                None
            } else {
                Some(
                    std::str::from_utf8(&bytes[..length])
                        .map_err(io::Error::other)?
                        .to_owned(),
                )
            });
            bytes = &bytes[length..];
        }
        output.push(row);
    }
    if !bytes.is_empty() {
        return Err(io::Error::other("trailing native metadata bytes"));
    }
    Ok(output)
}
fn cstring(value: &[u8]) -> io::Result<CString> {
    CString::new(value)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL in native argument"))
}
fn failure(buffer: &[c_char]) -> io::Error {
    io::Error::other(
        unsafe { CStr::from_ptr(buffer.as_ptr()) }
            .to_string_lossy()
            .into_owned(),
    )
}

/// Registers the packaged static Parquet extension on a standalone C-API database.
/// This performs no SQL and does not alter connection guards or settings.
///
/// # Safety
/// `database` must be a live `duckdb_database` returned by this pinned library's
/// `duckdb_open` API. The caller must exclusively own it during initialization.
pub unsafe fn load_static_extensions(database: *mut c_void) -> io::Result<()> {
    let mut error = [0; 4096];
    if unsafe { grv_native_load_static_extensions(database, error.as_mut_ptr(), error.len()) } == 0
    {
        Ok(())
    } else {
        Err(failure(&error))
    }
}

impl NativeEngine {
    pub(crate) fn workspace_reference(&self) -> Arc<WorkspaceLock> {
        self.ownership.clone()
    }
    pub(crate) fn schema_exists(&mut self, schema: &str) -> io::Result<bool> {
        let schema = cstring(schema.as_bytes())?;
        let mut exists = 0;
        let mut error = [0; 4096];
        if unsafe {
            grv_native_schema_exists(
                self.owner,
                schema.as_ptr(),
                &mut exists,
                error.as_mut_ptr(),
                error.len(),
            )
        } != 0
        {
            return Err(failure(&error));
        }
        Ok(exists == 1)
    }
    pub(crate) fn relation_exists(&mut self, schema: &str, table: &str) -> io::Result<bool> {
        self.ownership.recheck()?;
        let schema = cstring(schema.as_bytes())?;
        let table = cstring(table.as_bytes())?;
        let mut exists = 0;
        let mut error = [0; 4096];
        if unsafe {
            grv_native_relation_exists(
                self.owner,
                schema.as_ptr(),
                table.as_ptr(),
                &mut exists,
                error.as_mut_ptr(),
                error.len(),
            )
        } != 0
        {
            return Err(failure(&error));
        }
        Ok(exists == 1)
    }
    pub(crate) fn verified_file_command(&mut self, descriptor: i32, query: &str) -> io::Result<()> {
        self.ownership.recheck()?;
        let query = cstring(query.as_bytes())?;
        let mut output = vec![0; 1024];
        let mut written = 0;
        let mut error = [0; 4096];
        if unsafe {
            grv_native_verified_file_query(
                self.owner,
                descriptor,
                query.as_ptr(),
                output.as_mut_ptr(),
                output.len(),
                &mut written,
                error.as_mut_ptr(),
                error.len(),
            )
        } != 0
        {
            return Err(failure(&error));
        }
        Ok(())
    }
    pub(crate) fn managed_initialized(&mut self) -> io::Result<bool> {
        self.ownership.recheck()?;
        let mut initialized = 0;
        let mut error = [0; 4096];
        if unsafe {
            grv_native_managed_initialized(
                self.owner,
                &mut initialized,
                error.as_mut_ptr(),
                error.len(),
            )
        } != 0
        {
            return Err(failure(&error));
        }
        Ok(initialized == 1)
    }
    pub(crate) fn metadata_query(&mut self, query: &str) -> io::Result<Vec<Vec<Option<String>>>> {
        self.internal_query(query, None)
    }
    pub(crate) fn managed_evidence_recorded(&self) -> io::Result<bool> {
        self.ownership.managed_evidence_recorded()
    }
    pub(crate) fn record_managed_evidence(&self, recorded: bool) -> io::Result<()> {
        self.ownership.record_managed_evidence(recorded)
    }
    pub(crate) fn managed_evidence_state(&self) -> io::Result<crate::lock::ManagedEvidence> {
        self.ownership.managed_evidence_state()
    }
    pub(crate) fn restore_managed_evidence(
        &self,
        state: crate::lock::ManagedEvidence,
    ) -> io::Result<()> {
        self.ownership.restore_managed_evidence(state)
    }
    pub(crate) fn build_evidence_recorded(&self) -> io::Result<bool> {
        self.ownership.build_evidence_recorded()
    }
    pub(crate) fn record_build_evidence(&self) -> io::Result<()> {
        self.ownership.record_build_evidence()
    }
    pub(crate) fn close_invocation(&mut self) -> io::Result<()> {
        use std::os::unix::ffi::OsStrExt;
        self.ownership.recheck()?;
        let path = cstring(self.ownership.engine_path().as_os_str().as_bytes())?;
        let mut error = [0; 4096];
        if unsafe {
            grv_native_close_invocation(self.owner, path.as_ptr(), error.as_mut_ptr(), error.len())
        } != 0
        {
            return Err(failure(&error));
        }
        Ok(())
    }
    pub(crate) fn stage_build_query(
        &mut self,
        query: &str,
        projection: &str,
        stage: &str,
        expected: &str,
        columns: usize,
    ) -> io::Result<Vec<SourceType>> {
        self.ownership.recheck()?;
        let query = cstring(query.as_bytes())?;
        let stage = cstring(stage.as_bytes())?;
        let projection = cstring(projection.as_bytes())?;
        let expected = cstring(expected.as_bytes())?;
        let allowance = columns
            .checked_mul(6)
            .and_then(|v| v.checked_add(4))
            .filter(|v| *v <= METADATA_BYTES)
            .ok_or_else(|| io::Error::other("build schema exceeds bounded metadata allowance"))?;
        let mut metadata = vec![0; allowance];
        let mut written = 0;
        let mut error = [0; 4096];
        if unsafe {
            grv_native_stage_build_query(
                self.owner,
                query.as_ptr(),
                projection.as_ptr(),
                stage.as_ptr(),
                expected.as_ptr(),
                metadata.as_mut_ptr(),
                metadata.len(),
                &mut written,
                error.as_mut_ptr(),
                error.len(),
            )
        } != 0
        {
            return Err(failure(&error));
        }
        metadata.truncate(written);
        Ok(decode_schema(&metadata, 0, 0)?.types)
    }
    pub(crate) fn stage_pull_query(
        &mut self,
        query: &str,
        stage: &str,
        expected: &str,
    ) -> io::Result<()> {
        self.ownership.recheck()?;
        let query = cstring(query.as_bytes())?;
        let stage = cstring(stage.as_bytes())?;
        let expected = cstring(expected.as_bytes())?;
        let mut error = [0; 4096];
        if unsafe {
            grv_native_stage_pull_query(
                self.owner,
                query.as_ptr(),
                stage.as_ptr(),
                expected.as_ptr(),
                error.as_mut_ptr(),
                error.len(),
            )
        } != 0
        {
            return Err(failure(&error));
        }
        Ok(())
    }
    pub(crate) fn private_query(&mut self, query: &str) -> io::Result<Vec<Vec<Option<String>>>> {
        self.internal_query(query, Some(-1))
    }
    pub(crate) fn verified_file_metadata(
        &mut self,
        descriptor: i32,
        query: &str,
    ) -> io::Result<Vec<Vec<Option<String>>>> {
        self.internal_query(query, Some(descriptor))
    }
    fn internal_query(
        &mut self,
        query: &str,
        descriptor: Option<i32>,
    ) -> io::Result<Vec<Vec<Option<String>>>> {
        self.ownership.recheck()?;
        let query = cstring(query.as_bytes())?;
        let mut output = vec![0; METADATA_BYTES];
        let mut written = 0;
        let mut error = [0; 4096];
        if unsafe {
            if descriptor == Some(-1) {
                grv_native_private_command(
                    self.owner,
                    query.as_ptr(),
                    output.as_mut_ptr(),
                    output.len(),
                    &mut written,
                    error.as_mut_ptr(),
                    error.len(),
                )
            } else if let Some(descriptor) = descriptor {
                grv_native_verified_file_query(
                    self.owner,
                    descriptor,
                    query.as_ptr(),
                    output.as_mut_ptr(),
                    output.len(),
                    &mut written,
                    error.as_mut_ptr(),
                    error.len(),
                )
            } else {
                grv_native_metadata_query(
                    self.owner,
                    query.as_ptr(),
                    output.as_mut_ptr(),
                    output.len(),
                    &mut written,
                    error.as_mut_ptr(),
                    error.len(),
                )
            }
        } != 0
        {
            return Err(failure(&error));
        }
        output.truncate(written);
        decode_metadata(&output)
    }
    pub fn acquire_sources(
        &mut self,
        sources: &[SourceSelection],
        journal: &crate::journal::AcquisitionJournal,
    ) -> io::Result<Vec<AcquiredSchema>> {
        journal.recheck()?;
        self.ownership.recheck()?;
        if sources.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "empty acquisition member selection",
            ));
        }
        for source in sources {
            self.authorize_relation(&source.schema, &source.table)?;
        }
        let mut error = [0; 4096];
        if unsafe { grv_native_snapshot_begin(self.owner, error.as_mut_ptr(), error.len()) } != 0 {
            return Err(failure(&error));
        }
        let mut acquired = Vec::with_capacity(sources.len());
        for (index, source) in sources.iter().enumerate() {
            let schema = cstring(source.schema.as_bytes())?;
            let table = cstring(source.table.as_bytes())?;
            let filter = cstring(source.filter.as_deref().unwrap_or("").as_bytes())?;
            let columns = source
                .columns
                .iter()
                .map(|column| cstring(column.as_bytes()))
                .collect::<io::Result<Vec<_>>>()?;
            let pointers = columns
                .iter()
                .map(|column| column.as_ptr())
                .collect::<Vec<_>>();
            let metadata_bytes = source
                .columns
                .len()
                .checked_mul(6)
                .and_then(|bytes| bytes.checked_add(4))
                .filter(|bytes| *bytes <= METADATA_BYTES)
                .ok_or_else(|| {
                    io::Error::other("source schema exceeds bounded metadata allowance")
                })?;
            let mut metadata = vec![0; metadata_bytes];
            let mut written = 0;
            let mut rows = 0;
            if unsafe {
                grv_native_stage_source(
                    self.owner,
                    index as u64,
                    schema.as_ptr(),
                    table.as_ptr(),
                    pointers.as_ptr(),
                    pointers.len(),
                    filter.as_ptr(),
                    metadata.as_mut_ptr(),
                    metadata.len(),
                    &mut written,
                    &mut rows,
                    error.as_mut_ptr(),
                    error.len(),
                )
            } != 0
            {
                return Err(failure(&error));
            }
            metadata.truncate(written);
            acquired.push(decode_schema(&metadata, rows, index as u64)?);
        }
        if unsafe { grv_native_snapshot_seal(self.owner, error.as_mut_ptr(), error.len()) } != 0 {
            return Err(failure(&error));
        }
        Ok(acquired)
    }
    pub(crate) fn base_table(&mut self, schema: &str, table: &str) -> io::Result<bool> {
        self.ownership.recheck()?;
        let schema = cstring(schema.as_bytes())?;
        let table = cstring(table.as_bytes())?;
        let mut native = 0;
        let mut error = [0; 4096];
        if unsafe {
            grv_native_base_table(
                self.owner,
                schema.as_ptr(),
                table.as_ptr(),
                &mut native,
                error.as_mut_ptr(),
                error.len(),
            )
        } != 0
        {
            return Err(failure(&error));
        }
        Ok(native == 1)
    }
    pub(crate) fn configure_s3_reader(
        &mut self,
        reader: &crate::s3_config::S3Reader,
    ) -> io::Result<()> {
        self.ownership.recheck()?;
        let extensions = crate::native_extensions::Extensions::stage(self.ownership.engine_path())?;
        let httpfs = cstring(extensions.paths[0].as_os_str().as_encoded_bytes())?;
        let aws = cstring(extensions.paths[1].as_os_str().as_encoded_bytes())?;
        let scope = cstring(reader.scope.as_bytes())?;
        let profile = cstring(reader.profile.as_bytes())?;
        let region = cstring(reader.region.as_bytes())?;
        let mut error = [0; 4096];
        if unsafe {
            grv_native_s3_configure(
                self.owner,
                httpfs.as_ptr(),
                aws.as_ptr(),
                scope.as_ptr(),
                profile.as_ptr(),
                region.as_ptr(),
                error.as_mut_ptr(),
                error.len(),
            )
        } != 0
        {
            return Err(failure(&error));
        }
        self.extensions = Some(extensions);
        Ok(())
    }
    pub(crate) fn verified_s3_metadata(
        &mut self,
        file: &grv_adapter_api::VerifiedFile,
        query: &str,
    ) -> io::Result<Vec<Vec<Option<String>>>> {
        self.s3_query(std::slice::from_ref(file), query)
    }
    pub(crate) fn verified_s3_command(
        &mut self,
        file: &grv_adapter_api::VerifiedFile,
        query: &str,
    ) -> io::Result<()> {
        self.verified_s3_files_command(std::slice::from_ref(file), query)
    }
    pub(crate) fn verified_s3_files_command(
        &mut self,
        files: &[grv_adapter_api::VerifiedFile],
        query: &str,
    ) -> io::Result<()> {
        self.s3_query(files, query).map(|_| ())
    }
    pub(crate) fn s3_query(
        &mut self,
        files: &[grv_adapter_api::VerifiedFile],
        query: &str,
    ) -> io::Result<Vec<Vec<Option<String>>>> {
        self.ownership.recheck()?;
        if self.extensions.is_none() {
            return Err(io::Error::other("independent S3 reader is not configured"));
        }
        if files.is_empty()
            || files.len() > 4096
            || files
                .iter()
                .any(|f| f.access != grv_adapter_api::FileAccess::S3View)
        {
            return Err(io::Error::other("invalid exact S3 file set"));
        }
        let locations = files
            .iter()
            .map(|f| cstring(f.location.as_bytes()))
            .collect::<io::Result<Vec<_>>>()?;
        let hashes = files
            .iter()
            .map(|f| cstring(f.sha256.as_str().as_bytes()))
            .collect::<io::Result<Vec<_>>>()?;
        let validators = files
            .iter()
            .map(|f| cstring(f.validator.as_bytes()))
            .collect::<io::Result<Vec<_>>>()?;
        let locations = locations.iter().map(|s| s.as_ptr()).collect::<Vec<_>>();
        let hashes = hashes.iter().map(|s| s.as_ptr()).collect::<Vec<_>>();
        let validators = validators.iter().map(|s| s.as_ptr()).collect::<Vec<_>>();
        let sizes = files.iter().map(|f| f.size.get()).collect::<Vec<_>>();
        let query = cstring(query.as_bytes())?;
        let mut output = vec![0; METADATA_BYTES];
        let mut written = 0;
        let mut error = [0; 4096];
        if unsafe {
            grv_native_s3_query(
                self.owner,
                locations.as_ptr(),
                sizes.as_ptr(),
                hashes.as_ptr(),
                validators.as_ptr(),
                files.len(),
                query.as_ptr(),
                output.as_mut_ptr(),
                output.len(),
                &mut written,
                error.as_mut_ptr(),
                error.len(),
            )
        } != 0
        {
            return Err(failure(&error));
        }
        output.truncate(written);
        decode_metadata(&output)
    }
    pub(crate) fn is_view(&mut self, schema: &str, table: &str) -> io::Result<bool> {
        self.ownership.recheck()?;
        let schema = cstring(schema.as_bytes())?;
        let table = cstring(table.as_bytes())?;
        let mut view = 0;
        let mut error = [0; 4096];
        if unsafe {
            grv_native_is_view(
                self.owner,
                schema.as_ptr(),
                table.as_ptr(),
                &mut view,
                error.as_mut_ptr(),
                error.len(),
            )
        } != 0
        {
            return Err(failure(&error));
        }
        Ok(view == 1)
    }
    pub(crate) fn relation_schema(
        &mut self,
        schema: &str,
        table: &str,
    ) -> io::Result<Vec<Vec<Option<String>>>> {
        self.ownership.recheck()?;
        let schema = cstring(schema.as_bytes())?;
        let table = cstring(table.as_bytes())?;
        let mut output = vec![0; METADATA_BYTES];
        let mut written = 0;
        let mut error = [0; 4096];
        if unsafe {
            grv_native_relation_schema(
                self.owner,
                schema.as_ptr(),
                table.as_ptr(),
                output.as_mut_ptr(),
                output.len(),
                &mut written,
                error.as_mut_ptr(),
                error.len(),
            )
        } != 0
        {
            return Err(failure(&error));
        }
        output.truncate(written);
        decode_metadata(&output)
    }
    pub(crate) fn acquire_build_outputs(
        &mut self,
        outputs: &[grv_adapter_api::OutputBinding],
        external: bool,
    ) -> io::Result<Vec<AcquiredSchema>> {
        self.ownership.recheck()?;
        if outputs.is_empty() {
            return Ok(vec![]);
        }
        for output in outputs {
            let (schema, table) = output
                .engine_table
                .split_once('.')
                .ok_or_else(|| io::Error::other("invalid build output mapping"))?;
            if !schema.starts_with("_grv_session_") {
                return Err(io::Error::other(
                    "build export requires private session outputs",
                ));
            }
            self.authorize_relation(schema, table)?;
        }
        let mut error = [0; 4096];
        if unsafe { grv_native_snapshot_begin(self.owner, error.as_mut_ptr(), error.len()) } != 0 {
            return Err(failure(&error));
        }
        let mut acquired = Vec::with_capacity(outputs.len());
        for (index, output) in outputs.iter().enumerate() {
            let (schema, table) = output.engine_table.split_once('.').unwrap();
            let mapped = if external {
                crate::binding::source_columns(&grv_adapter_api::ExtractTable {
                    name: output.table.clone(),
                    source: output.source.clone(),
                    columns: output.columns.clone(),
                    contract: output.contract.clone(),
                })?
            } else {
                output
                    .contract
                    .columns
                    .iter()
                    .map(|c| c.name.clone())
                    .collect()
            };
            let sql = format!(
                "SELECT {} FROM {}.{}",
                mapped
                    .iter()
                    .map(|c| crate::pull::quote_identifier(c))
                    .collect::<Vec<_>>()
                    .join(","),
                crate::pull::quote_identifier(schema),
                crate::pull::quote_identifier(table)
            );
            let query = cstring(sql.as_bytes())?;
            let bytes = output
                .contract
                .columns
                .len()
                .checked_mul(6)
                .and_then(|v| v.checked_add(4))
                .filter(|v| *v <= METADATA_BYTES)
                .ok_or_else(|| {
                    io::Error::other("build schema exceeds bounded metadata allowance")
                })?;
            let mut metadata = vec![0; bytes];
            let mut written = 0;
            let mut rows = 0;
            if unsafe {
                grv_native_stage_index(
                    self.owner,
                    index as u64,
                    query.as_ptr(),
                    metadata.as_mut_ptr(),
                    metadata.len(),
                    &mut written,
                    &mut rows,
                    error.as_mut_ptr(),
                    error.len(),
                )
            } != 0
            {
                return Err(failure(&error));
            }
            metadata.truncate(written);
            acquired.push(decode_schema(&metadata, rows, index as u64)?);
        }
        if unsafe { grv_native_snapshot_seal(self.owner, error.as_mut_ptr(), error.len()) } != 0 {
            return Err(failure(&error));
        }
        Ok(acquired)
    }
    pub fn acquire_recorded(
        &mut self,
        query: &str,
        journal: &crate::journal::AcquisitionJournal,
    ) -> io::Result<AcquiredSchema> {
        journal.recheck()?;
        self.acquire(query)
    }
    /// Authorize an exact local dependency; caller validation must restrict extraction
    /// sources to unmanaged base tables. Reserved/private relations are never accepted.
    pub fn authorize_relation(&mut self, schema: &str, table: &str) -> io::Result<()> {
        let schema = cstring(schema.as_bytes())?;
        let table = cstring(table.as_bytes())?;
        let mut error = [0; 4096];
        if unsafe {
            grv_native_authorize_relation(
                self.owner,
                schema.as_ptr(),
                table.as_ptr(),
                error.as_mut_ptr(),
                error.len(),
            )
        } == 0
        {
            Ok(())
        } else {
            Err(failure(&error))
        }
    }

    /// Evaluate one guarded source SELECT into independently bounded engine storage.
    /// This owner refuses a second acquisition even if the first failed. A durable
    /// external acquisition marker must be recorded BEFORE invoking this method.
    pub fn acquire(&mut self, query: &str) -> io::Result<AcquiredSchema> {
        self.ownership.recheck()?;
        let query = cstring(query.as_bytes())?;
        let mut metadata = vec![0; 4 + 256 * 6];
        let mut written = 0;
        let mut rows = 0;
        let mut error = [0; 4096];
        if unsafe {
            grv_native_stage(
                self.owner,
                query.as_ptr(),
                metadata.as_mut_ptr(),
                metadata.len(),
                &mut written,
                &mut rows,
                error.as_mut_ptr(),
                error.len(),
            )
        } != 0
        {
            return Err(failure(&error));
        }
        metadata.truncate(written);
        decode_schema(&metadata, rows, 0)
    }

    /// Length metadata fixes this immutable window before native variable bytes
    /// are materialized. No source SELECT is repeated while reading staging.
    pub fn fetch_acquired(
        &mut self,
        schema: &AcquiredSchema,
        allowance: usize,
        source_allowance: usize,
    ) -> io::Result<Option<NativeWindow>> {
        if allowance == 0
            || allowance > crate::MAX_BATCH_BYTES
            || source_allowance == 0
            || source_allowance > crate::SOURCE_BUDGET_BYTES
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid acquisition transfer allowance",
            ));
        }
        let mut bytes = vec![0; allowance];
        let mut written = 0;
        let mut error = [0; 4096];
        match unsafe {
            grv_native_stage_fetch_index(
                self.owner,
                schema.table_id,
                bytes.as_mut_ptr(),
                allowance,
                source_allowance,
                &mut written,
                error.as_mut_ptr(),
                error.len(),
            )
        } {
            0 => Ok(None),
            1 if written <= allowance => {
                bytes.truncate(written);
                NativeWindow::decode(bytes, schema.types.clone()).map(Some)
            }
            _ => Err(failure(&error)),
        }
    }
    pub fn open(path: &Path) -> io::Result<Self> {
        Self::open_mode(path, false)
    }
    pub fn open_readonly(path: &Path) -> io::Result<Self> {
        Self::open_mode(path, true)
    }
    fn open_mode(path: &Path, read_only: bool) -> io::Result<Self> {
        use std::os::unix::ffi::OsStrExt;
        if read_only && !path.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "extraction database does not exist",
            ));
        }
        let mut lock = WorkspaceLock::acquire(path)?;
        let path = cstring(lock.engine_path().as_os_str().as_bytes())?;
        let mut error = [0; 4096];
        let version = unsafe { CStr::from_ptr(grv_native_version()) }.to_bytes();
        if version != b"v1.5.6" || unsafe { grv_native_guard_revision() } != 1 {
            return Err(io::Error::other("native DuckDB version mismatch"));
        }
        let owner = unsafe {
            if read_only {
                grv_native_open_readonly(path.as_ptr(), error.as_mut_ptr(), error.len())
            } else {
                grv_native_open(path.as_ptr(), error.as_mut_ptr(), error.len())
            }
        };
        if owner.is_null() {
            let error = failure(&error);
            if error.to_string().contains("Could not set lock on file") {
                return Err(io::Error::new(io::ErrorKind::WouldBlock, error));
            }
            return Err(error);
        }
        if let Err(error) = lock.record_created_engine() {
            unsafe { grv_native_close(owner) };
            return Err(error);
        }
        let handle = unsafe { grv_native_interrupt_handle(owner) };
        Ok(Self {
            owner,
            extensions: None,
            interrupt: NativeInterrupt(Arc::new(InterruptPointer(handle))),
            ownership: Arc::new(lock),
            _thread_owner: PhantomData,
        })
    }
    pub fn spawn(path: &Path) -> io::Result<Worker<NativeInterrupt>> {
        let path = path.to_owned();
        Worker::spawn(move || Self::open(&path))
    }
}

fn decode_schema(metadata: &[u8], rows: u64, table_id: u64) -> io::Result<AcquiredSchema> {
    let mut input = metadata;
    let columns = take_u32(&mut input)? as usize;
    let mut types = Vec::with_capacity(columns);
    for _ in 0..columns {
        let code = take_u32(&mut input)?;
        let details = take(&mut input, 2)?;
        types.push(match code {
            1 => SourceType::Boolean,
            2..=5 => SourceType::SignedInteger {
                bits: [8, 16, 32, 64][(code - 2) as usize],
            },
            6 => SourceType::Float32,
            7 => SourceType::Float64,
            8 => SourceType::Utf8,
            9 => SourceType::Binary,
            10 => SourceType::Date32,
            11 => SourceType::Decimal128 {
                precision: details[0],
                scale: details[1],
            },
            12..=16 => SourceType::Timestamp {
                unit: match code {
                    12 => TickUnit::Second,
                    13 => TickUnit::Millisecond,
                    15 => TickUnit::Nanosecond,
                    _ => TickUnit::Microsecond,
                },
                utc: code == 16,
            },
            _ => return Err(io::Error::other("unknown pinned native source type")),
        });
    }
    if !input.is_empty() {
        return Err(io::Error::other("native schema trailing bytes"));
    }
    Ok(AcquiredSchema {
        types,
        rows,
        table_id,
    })
}

#[derive(Debug, Clone)]
pub struct SourceSelection {
    pub schema: String,
    pub table: String,
    pub columns: Vec<String>,
    pub filter: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AcquiredSchema {
    table_id: u64,
    pub types: Vec<SourceType>,
    pub rows: u64,
}
/// The only retained transfer unit. Scalars borrow variable bytes directly;
/// there is no decoded string/blob copy or hidden queue.
pub struct NativeWindow {
    bytes: Vec<u8>,
    types: Vec<SourceType>,
    rows: u64,
}
impl NativeWindow {
    pub fn source_types(&self) -> &[SourceType] {
        &self.types
    }
    fn decode(bytes: Vec<u8>, types: Vec<SourceType>) -> io::Result<Self> {
        let mut input = bytes.as_slice();
        let rows = take_u64(&mut input)?;
        if take_u64(&mut input)? as usize != types.len() {
            return Err(io::Error::other("native window schema mismatch"));
        }
        for _ in 0..rows {
            for ty in &types {
                scalar(&mut input, ty)?;
            }
        }
        if !input.is_empty() {
            return Err(io::Error::other("native window trailing bytes"));
        }
        Ok(Self { bytes, types, rows })
    }
    pub fn row_count(&self) -> u64 {
        self.rows
    }
    pub fn encoded_bytes(&self) -> usize {
        self.bytes.len()
    }
    pub fn visit<'a>(
        &'a self,
        mut visitor: impl FnMut(u64, usize, Scalar<'a>) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut input = &self.bytes[16..];
        for row in 0..self.rows {
            for (column, ty) in self.types.iter().enumerate() {
                visitor(row, column, scalar(&mut input, ty)?)?;
            }
        }
        Ok(())
    }
}
fn take<'a>(input: &mut &'a [u8], length: usize) -> io::Result<&'a [u8]> {
    if input.len() < length {
        return Err(io::Error::other("truncated native acquisition window"));
    }
    let (value, rest) = input.split_at(length);
    *input = rest;
    Ok(value)
}
fn take_u32(input: &mut &[u8]) -> io::Result<u32> {
    Ok(u32::from_le_bytes(take(input, 4)?.try_into().unwrap()))
}
fn take_u64(input: &mut &[u8]) -> io::Result<u64> {
    Ok(u64::from_le_bytes(take(input, 8)?.try_into().unwrap()))
}
fn scalar<'a>(input: &mut &'a [u8], ty: &SourceType) -> io::Result<Scalar<'a>> {
    let validity = take(input, 1)?[0];
    if validity > 1 {
        return Err(io::Error::other("invalid native validity byte"));
    }
    let value = match ty {
        SourceType::Boolean => Scalar::Boolean(take(input, 1)?[0] != 0),
        SourceType::SignedInteger { .. } => Scalar::Integer(take_u64(input)? as i64),
        SourceType::Float32 => Scalar::Float32(f32::from_bits(take_u32(input)?)),
        SourceType::Float64 => Scalar::Float64(f64::from_bits(take_u64(input)?)),
        SourceType::Date32 => Scalar::Date32(take_u32(input)? as i32),
        SourceType::Decimal128 { .. } => {
            Scalar::Decimal128(i128::from_le_bytes(take(input, 16)?.try_into().unwrap()))
        }
        SourceType::Timestamp { .. } => Scalar::Timestamp(take_u64(input)? as i64),
        SourceType::Utf8 | SourceType::Binary => {
            let length = usize::try_from(take_u64(input)?).map_err(io::Error::other)?;
            let bytes = take(input, length)?;
            if matches!(ty, SourceType::Utf8) {
                Scalar::Utf8(bytes)
            } else {
                Scalar::Binary(bytes)
            }
        }
        SourceType::Unsupported(_) => return Err(io::Error::other("unsupported native scalar")),
    };
    Ok(if validity == 0 { Scalar::Null } else { value })
}
impl Engine for NativeEngine {
    type Interrupt = NativeInterrupt;
    fn interrupt_handle(&self) -> NativeInterrupt {
        self.interrupt.clone()
    }
    fn begin(&mut self, query: &str) -> io::Result<()> {
        self.ownership.recheck()?;
        let query = cstring(query.as_bytes())?;
        let mut error = [0; 4096];
        if unsafe { grv_native_begin(self.owner, query.as_ptr(), error.as_mut_ptr(), error.len()) }
            == 0
        {
            Ok(())
        } else {
            Err(failure(&error))
        }
    }
    fn fetch(&mut self, allowance: usize) -> io::Result<Option<Vec<u8>>> {
        if allowance == 0 || allowance > crate::MAX_BATCH_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid native fetch allowance",
            ));
        }
        let mut bytes = vec![0; allowance];
        let mut written = 0;
        let mut error = [0; 4096];
        match unsafe {
            grv_native_fetch(
                self.owner,
                bytes.as_mut_ptr(),
                allowance,
                &mut written,
                error.as_mut_ptr(),
                error.len(),
            )
        } {
            0 => Ok(None),
            1 if written <= allowance => {
                bytes.truncate(written);
                Ok(Some(bytes))
            }
            _ => Err(failure(&error)),
        }
    }
}

#[cfg(test)]
mod descriptor_tests {
    #[test]
    fn standalone_c_api_static_parquet_initializer_is_idempotent() {
        #[repr(C)]
        struct NativeResult {
            column_count: u64,
            row_count: u64,
            rows_changed: u64,
            columns: *mut c_void,
            error: *mut c_char,
            internal: *mut c_void,
        }
        unsafe extern "C" {
            fn duckdb_open(path: *const c_char, database: *mut *mut c_void) -> u32;
            fn duckdb_connect(database: *mut c_void, connection: *mut *mut c_void) -> u32;
            fn duckdb_query(
                connection: *mut c_void,
                query: *const c_char,
                result: *mut NativeResult,
            ) -> u32;
            fn duckdb_value_int64(result: *mut NativeResult, column: u64, row: u64) -> i64;
            fn duckdb_destroy_result(result: *mut NativeResult);
            fn duckdb_disconnect(connection: *mut *mut c_void);
            fn duckdb_close(database: *mut *mut c_void);
        }
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("data.parquet");
        parquet(&path, 37);
        let query = cstring(
            format!(
                "SELECT value FROM read_parquet({})",
                crate::pull::quote_literal(path.to_str().unwrap())
            )
            .as_bytes(),
        )
        .unwrap();
        let mut database = std::ptr::null_mut();
        let mut connection = std::ptr::null_mut();
        unsafe {
            assert_eq!(duckdb_open(std::ptr::null(), &mut database), 0);
            assert_eq!(duckdb_connect(database, &mut connection), 0);
            // The pinned archive deliberately disables automatic static extension registration.
            let mut result: NativeResult = std::mem::zeroed();
            assert_ne!(duckdb_query(connection, query.as_ptr(), &mut result), 0);
            duckdb_destroy_result(&mut result);
            load_static_extensions(database).unwrap();
            load_static_extensions(database).unwrap();
            let status = duckdb_query(connection, query.as_ptr(), &mut result);
            let value = if status == 0 {
                duckdb_value_int64(&mut result, 0, 0)
            } else {
                0
            };
            duckdb_destroy_result(&mut result);
            duckdb_disconnect(&mut connection);
            duckdb_close(&mut database);
            assert_eq!(status, 0);
            assert_eq!(value, 37);
            assert!(load_static_extensions(std::ptr::null_mut()).is_err());
        }
    }
    #[test]
    fn s3_uri_roundtrip_preserves_exact_percent_keys_and_rejects_aliases() {
        unsafe extern "C" {
            fn grv_native_s3_uri_roundtrip(
                uri: *const c_char,
                decoded: *mut c_char,
                allowance: usize,
            ) -> i32;
        }
        for (uri, expected) in [
            (
                "s3://test-bucket/space%20key/version=1/owner@example/file.parquet",
                "/test-bucket/space%20key/version%3D1/owner%40example/file.parquet",
            ),
            (
                "s3://test-bucket/literal%25/%2A%5B%5D/file.parquet",
                "/test-bucket/literal%25/%2A%5B%5D/file.parquet",
            ),
        ] {
            let uri = cstring(uri.as_bytes()).unwrap();
            let mut output = [0; 8192];
            assert_eq!(
                unsafe {
                    grv_native_s3_uri_roundtrip(uri.as_ptr(), output.as_mut_ptr(), output.len())
                },
                0
            );
            assert_eq!(
                unsafe { CStr::from_ptr(output.as_ptr()) }.to_str().unwrap(),
                expected
            );
        }
        for uri in [
            "s3://user@test-bucket/a",
            "s3://test-bucket/a/../b",
            "s3://test-bucket/a/%2E%2E/b",
            "s3://test-bucket/a%2Fb",
            "s3://test-bucket/a?token=secret",
            "s3://test-bucket/raw*glob",
            "s3://test-bucket/%61lias",
            "s3://test-bucket/space%2akey",
        ] {
            let uri = cstring(uri.as_bytes()).unwrap();
            let mut output = [0; 8192];
            assert_eq!(
                unsafe {
                    grv_native_s3_uri_roundtrip(uri.as_ptr(), output.as_mut_ptr(), output.len())
                },
                -1
            );
        }
    }
    #[test]
    fn relation_schema_observes_persisted_views_without_binding_callbacks() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("catalog.duckdb");
        {
            let mut engine = NativeEngine::open(&path).unwrap();
            engine.metadata_query("CREATE TABLE main.rows(id BIGINT, stamp TIMESTAMP_MS, zoned TIMESTAMP WITH TIME ZONE)").unwrap();
            engine.authorize_relation("main", "rows").unwrap();
            engine
                .metadata_query("CREATE VIEW main.observed AS SELECT * FROM main.rows")
                .unwrap();
            assert!(engine.is_view("main", "observed").unwrap());
            assert!(!engine.is_view("main", "missing").unwrap());
        }
        let mut engine = NativeEngine::open(&path).unwrap();
        assert_eq!(
            engine.relation_schema("main", "observed").unwrap(),
            vec![
                vec![Some("id".into()), Some("BIGINT".into())],
                vec![Some("stamp".into()), Some("TIMESTAMP_MS".into())],
                vec![
                    Some("zoned".into()),
                    Some("TIMESTAMP WITH TIME ZONE".into())
                ]
            ]
        );
        assert_eq!(
            engine.relation_schema("main", "observed").unwrap(),
            engine.relation_schema("main", "rows").unwrap()
        );
        assert!(engine.relation_schema("main", "missing").is_err());
    }
    #[test]
    #[ignore = "requires protected pinned signed extensions and authorized named S3 profile"]
    fn independently_configured_signed_s3_reader_initializes_without_source_access() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("reader.duckdb");
        let mut engine = NativeEngine::open(&path).unwrap();
        engine
            .configure_s3_reader(&crate::s3_config::S3Reader {
                scope: "s3://private-scope-placeholder/grv-dev".into(),
                profile: "private-scope-placeholder".into(),
                region: "eu-west-1".into(),
            })
            .unwrap();
        assert!(engine.metadata_query("SELECT * FROM read_parquet('s3://private-scope-placeholder/grv-dev/foreign.parquet')").is_err());
    }

    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use std::{
        fs,
        os::unix::{fs::PermissionsExt, io::AsRawFd},
        sync::Arc,
    };
    fn parquet(path: &Path, value: i64) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![value]))],
        )
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(fs::File::create(path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    #[test]
    fn verified_descriptor_scans_unlinked_files_without_path_lookup_and_rejects_widening() {
        let dir = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR")).unwrap();
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let path = dir.path().join("source.parquet");
        parquet(&path, 37);
        let file = fs::File::open(&path).unwrap();
        let descriptor = file.as_raw_fd();
        let read = format!(
            "SELECT sum(value)::BIGINT FROM read_parquet('/dev/fd/{descriptor}',hive_partitioning=false,union_by_name=false)"
        );
        fs::remove_file(&path).unwrap();
        let mut engine = NativeEngine::open(&dir.path().join("engine.duckdb")).unwrap();
        assert_eq!(
            engine.verified_file_metadata(descriptor, &read).unwrap(),
            vec![vec![Some("37".into())]]
        );
        assert_eq!(
            engine.verified_file_metadata(descriptor, &read).unwrap(),
            vec![vec![Some("37".into())]]
        );
        let other = file.try_clone().unwrap();
        for query in [
            format!(
                "SELECT * FROM read_parquet('/dev/fd/{}')",
                other.as_raw_fd()
            ),
            format!("SELECT * FROM read_parquet('/dev/fd/{descriptor}/*.parquet')"),
            "SELECT * FROM read_parquet('grv-verified-fd://1')".into(),
            format!("COPY (SELECT 1) TO '/dev/fd/{descriptor}' (FORMAT PARQUET)"),
        ] {
            assert!(
                engine.verified_file_metadata(descriptor, &query).is_err(),
                "unauthorized query passed: {query}"
            );
        }
        let directory = fs::File::open(dir.path()).unwrap();
        assert!(
            engine
                .verified_file_metadata(directory.as_raw_fd(), "SELECT 1")
                .is_err()
        );
        parquet(&path, 99);
        let writable = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        assert!(
            engine
                .verified_file_metadata(writable.as_raw_fd(), "SELECT 1")
                .is_err()
        );
        let replacement = fs::File::open(&path).unwrap();
        let next = replacement.as_raw_fd();
        let query = format!(
            "SELECT sum(value)::BIGINT FROM read_parquet('/dev/fd/{next}',hive_partitioning=false,union_by_name=false)"
        );
        assert_eq!(
            engine.verified_file_metadata(next, &query).unwrap(),
            vec![vec![Some("99".into())]]
        );
        assert_eq!(
            engine.verified_file_metadata(descriptor, &read).unwrap(),
            vec![vec![Some("37".into())]]
        );
        // Trust expires with the scoped scan; ordinary metadata/user paths get
        // no authority from having previously scanned a pinned file.
        assert!(engine.metadata_query(&read).is_err());
    }
}
