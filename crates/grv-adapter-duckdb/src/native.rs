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
// Conservative admission reserve for GCS metadata/source scratch, not total
// engine memory or RSS. Local/S3 negotiation remains unchanged.
pub(crate) const GCS_MIN_SCRATCH_BYTES: u64 = 32 * 1024 * 1024;
const GCS_ARGUMENT_BYTES: usize = METADATA_BYTES;
fn bounded_gcs_arguments<'a>(values: impl IntoIterator<Item = &'a str>) -> io::Result<()> {
    values
        .into_iter()
        .try_fold(0usize, |sum, value| {
            if value.as_bytes().contains(&0) {
                return None;
            }
            sum.checked_add(value.len())
                .filter(|n| *n <= GCS_ARGUMENT_BYTES)
        })
        .map(|_| ())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "GCS argument budget exceeded"))
}

unsafe extern "C" {
    fn grv_native_load_static_extensions(
        database: *mut c_void,
        error: *mut c_char,
        capacity: usize,
    ) -> i32;
    fn grv_native_gcs_metadata_mode(owner: *mut c_void, error: *mut c_char, capacity: usize)
    -> i32;
    fn grv_native_gcs_configure(
        owner: *mut c_void,
        httpfs: *const c_char,
        scope: *const c_char,
        bearer: *const c_char,
        project: *const c_char,
        remaining_millis: u64,
        error: *mut c_char,
        capacity: usize,
    ) -> i32;
    fn grv_native_gcs_query(
        owner: *mut c_void,
        locations: *const *const c_char,
        count: usize,
        query: *const c_char,
        output: *mut u8,
        allowance: usize,
        written: *mut usize,
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
pub struct NativeInterrupt {
    pointer: Arc<std::sync::Mutex<InterruptPointer>>,
    cancelled: Arc<std::sync::atomic::AtomicBool>,
}
impl Interrupt for NativeInterrupt {
    fn interrupt(&self) {
        self.cancelled
            .store(true, std::sync::atomic::Ordering::Release);
        if let Ok(pointer) = self.pointer.lock() {
            unsafe { grv_native_interrupt(pointer.0) };
        }
    }
}

// Requests only DuckDB's supported cooperative interrupt. The RAII guard joins
// its watcher after synchronous execution returns; no detached native thread,
// no process kill, and no hard stop/join promise for stock HTTP requests.
struct CooperativeDeadline {
    stop: std::sync::mpsc::Sender<()>,
    watcher: Option<std::thread::JoinHandle<()>>,
}
impl Drop for CooperativeDeadline {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(watcher) = self.watcher.take() {
            let _ = watcher.join();
        }
    }
}

pub struct NativeEngine {
    owner: *mut c_void,
    extensions: Option<crate::native_extensions::Extensions>,
    gcs_deadline: Option<std::time::Instant>,
    gcs_mode: bool,
    owner_unusable: bool,
    interrupt: NativeInterrupt,
    // Workspace ownership outlives the connection and all active interrupt calls.
    ownership: Arc<WorkspaceLock>,
    _thread_owner: PhantomData<Rc<()>>,
    // Test-only: expire the original lease immediately after the next
    // synchronous native call returns, but before the final publication
    // deadline check. Never present in production builds.
    #[cfg(test)]
    test_expire_lease_after_ffi: bool,
}
impl Drop for NativeEngine {
    fn drop(&mut self) {
        if !self.owner.is_null() {
            unsafe { grv_native_close(self.owner) };
        }
    }
}

const DECODED_METADATA_LIMIT: usize = 8 * 1024 * 1024;
fn decode_metadata(output: &[u8]) -> io::Result<Vec<Vec<Option<String>>>> {
    decode_metadata_with_budget(output, usize::MAX)
}
fn decode_gcs_metadata(output: &[u8]) -> io::Result<Vec<Vec<Option<String>>>> {
    decode_metadata_with_budget(output, DECODED_METADATA_LIMIT)
}
fn decode_metadata_with_budget(
    output: &[u8],
    decoded_limit: usize,
) -> io::Result<Vec<Vec<Option<String>>>> {
    if output.len() > METADATA_BYTES {
        return Err(io::Error::other("native metadata exceeds raw budget"));
    }
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
    // Validate and account BEFORE allocating row/cell/string storage. Raw
    // bytes alone do not bound Vec<Vec<Option<String>>>: many empty cells
    // amplify per-row/per-cell layout. The encoded source remains borrowed.
    let mut storage = rows
        .checked_mul(std::mem::size_of::<Vec<Option<String>>>())
        .and_then(|n| {
            rows.checked_mul(columns)
                .and_then(|cells| cells.checked_mul(std::mem::size_of::<Option<String>>()))
                .and_then(|cells| n.checked_add(cells))
        })
        .filter(|n| *n <= decoded_limit)
        .ok_or_else(|| io::Error::other("decoded native metadata exceeds scratch budget"))?;
    let mut validated = bytes;
    for _ in 0..rows {
        for _ in 0..columns {
            let tag = *validated
                .first()
                .ok_or_else(|| io::Error::other("missing native metadata tag"))?;
            validated = &validated[1..];
            let length = count(&mut validated)?;
            if length > validated.len() || tag > 1 || tag == 0 && length != 0 {
                return Err(io::Error::other("invalid native metadata field"));
            }
            if tag == 1 {
                std::str::from_utf8(&validated[..length]).map_err(io::Error::other)?;
                storage = storage
                    .checked_add(length)
                    .filter(|n| *n <= decoded_limit)
                    .ok_or_else(|| {
                        io::Error::other("decoded native metadata exceeds scratch budget")
                    })?;
            }
            validated = &validated[length..];
        }
    }
    if !validated.is_empty() {
        return Err(io::Error::other("trailing native metadata bytes"));
    }
    let mut output = Vec::new();
    output.try_reserve_exact(rows).map_err(io::Error::other)?;
    for _ in 0..rows {
        let mut row = Vec::new();
        row.try_reserve_exact(columns).map_err(io::Error::other)?;
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
                let text = std::str::from_utf8(&bytes[..length]).map_err(io::Error::other)?;
                let mut owned = String::new();
                owned.try_reserve_exact(length).map_err(io::Error::other)?;
                owned.push_str(text);
                Some(owned)
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
    fn local_owner(&self) -> io::Result<()> {
        if self.owner.is_null() || self.owner_unusable {
            return Err(io::Error::other("native owner cleanup unresolved"));
        }
        Ok(())
    }
    fn gcs_arguments<'a>(&self, values: impl IntoIterator<Item = &'a str>) -> io::Result<()> {
        if self.gcs_mode {
            bounded_gcs_arguments(values)?;
        }
        Ok(())
    }
    fn cooperative_deadline(&self) -> io::Result<Option<CooperativeDeadline>> {
        let Some(deadline) = self.gcs_deadline else {
            return Ok(None);
        };
        let interrupt = self.interrupt.clone();
        let (stop, receiver) = std::sync::mpsc::channel();
        let watcher = std::thread::Builder::new()
            .name("grv-gcs-cooperative-deadline".into())
            .spawn(move || {
                if matches!(
                    receiver.recv_timeout(
                        deadline.saturating_duration_since(std::time::Instant::now())
                    ),
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout)
                ) {
                    // Query initialization may clear DuckDB's interrupt bit.
                    // Continue requesting cooperative interruption until this
                    // synchronous call returns; the original deadline never
                    // renews. This still cannot cancel blocked stock HTTP.
                    loop {
                        interrupt.interrupt();
                        match receiver.recv_timeout(std::time::Duration::from_millis(20)) {
                            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                            _ => break,
                        }
                    }
                }
            })?;
        Ok(Some(CooperativeDeadline {
            stop,
            watcher: Some(watcher),
        }))
    }
    pub(crate) fn workspace_reference(&self) -> Arc<WorkspaceLock> {
        self.ownership.clone()
    }
    pub(crate) fn schema_exists(&mut self, schema: &str) -> io::Result<bool> {
        self.ownership.recheck()?;
        self.local_owner()?;
        self.gcs_arguments([schema])?;
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
        self.local_owner()?;
        self.gcs_arguments([schema, table])?;
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
        if self.gcs_mode {
            return self.internal_query(query, Some(descriptor)).map(|_| ());
        }
        self.local_owner()?;
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
        self.local_owner()?;
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
    /// Closed durable VARCHAR projections in the GCS owner. Keep the original
    /// local/S3 path and preserve NULL, duplicates and absent rows. Oversized
    /// rows raise an error within the same statement/snapshot, never filter
    /// away evidence. This bounds result copying, NOT storage/source allocation.
    pub(crate) fn durable_metadata_query(
        &mut self,
        query: &str,
        fields: &[&str],
    ) -> io::Result<Vec<Vec<Option<String>>>> {
        if !self.gcs_mode {
            return self.metadata_query(query);
        }
        let guarded = bounded_durable_sql(query, fields)?;
        self.metadata_query(&guarded)
    }
    pub(crate) fn check_reader_deadline(&self) -> io::Result<()> {
        if self.owner_unusable {
            return Err(io::Error::other("GCS owner cleanup unresolved"));
        }
        if self.gcs_mode
            && self
                .interrupt
                .cancelled
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "GCS execution cancelled",
            ));
        }
        if self
            .gcs_deadline
            .is_some_and(|deadline| std::time::Instant::now() >= deadline)
        {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "GCS execution deadline exceeded",
            ));
        }
        Ok(())
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
        if self.owner.is_null() {
            return Err(io::Error::other("native owner unavailable"));
        }
        let path = cstring(self.ownership.engine_path().as_os_str().as_bytes())?;
        let mut error = [0; 4096];
        // Owner-thread close only AFTER synchronous execution returns; not a
        // concurrent cancellation/rollback attestation.
        let status = unsafe {
            grv_native_close_invocation(self.owner, path.as_ptr(), error.as_mut_ptr(), error.len())
        };
        if status != 0 {
            self.owner_unusable = true;
            return Err(failure(&error));
        }
        self.ownership.recheck()?;
        self.owner_unusable = false;
        self.gcs_mode = false;
        self.gcs_deadline = None;
        self.extensions = None;
        self.interrupt
            .cancelled
            .store(false, std::sync::atomic::Ordering::Release);
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
        self.local_owner()?;
        self.gcs_arguments([query, projection, stage, expected])?;
        self.check_reader_deadline()?;
        let _watchdog = self.cooperative_deadline()?;
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
        self.check_reader_deadline()?;
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
        self.local_owner()?;
        self.gcs_arguments([query, stage, expected])?;
        self.check_reader_deadline()?;
        let _watchdog = self.cooperative_deadline()?;
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
        self.check_reader_deadline()?;
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
        self.local_owner()?;
        self.gcs_arguments([query])?;
        // ROLLBACK is an explicit cleanup request, not new cloud execution.
        let rollback = query.trim().eq_ignore_ascii_case("ROLLBACK")
            || query.trim().eq_ignore_ascii_case("ROLLBACK TRANSACTION");
        if !rollback {
            self.check_reader_deadline()?;
        }
        let watchdog = if rollback {
            None
        } else {
            self.cooperative_deadline()?
        };
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
        drop(watchdog);
        self.ownership.recheck()?;
        #[cfg(test)]
        if self.test_expire_lease_after_ffi {
            // Test-only boundary placement: the synchronous native call has
            // already returned successfully, so expiring the lease here models a
            // result that is refused at the Rust publication boundary, not a
            // native failure. Production behavior is unchanged.
            self.test_expire_lease_after_ffi = false;
            self.gcs_deadline = Some(std::time::Instant::now() - std::time::Duration::from_secs(1));
        }
        if !rollback {
            self.check_reader_deadline()?;
        }
        if written > output.len() {
            self.owner_unusable = true;
            return Err(io::Error::other("invalid native metadata length"));
        }
        output.truncate(written);
        if self.gcs_mode {
            decode_gcs_metadata(&output)
        } else {
            decode_metadata(&output)
        }
    }
    pub fn acquire_sources(
        &mut self,
        sources: &[SourceSelection],
        journal: &crate::journal::AcquisitionJournal,
    ) -> io::Result<Vec<AcquiredSchema>> {
        self.local_owner()?;
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
        self.local_owner()?;
        self.gcs_arguments([schema, table])?;
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
    /// Source-free bounded metadata, no credential acquisition/lease start or
    /// cloud authority. Retain the same native connection and SQL transaction.
    pub(crate) fn admit_gcs_metadata(&mut self) -> io::Result<()> {
        self.local_owner()?;
        self.ownership.recheck()?;
        let mut error = [0; 4096];
        if unsafe { grv_native_gcs_metadata_mode(self.owner, error.as_mut_ptr(), error.len()) } != 0
        {
            return Err(failure(&error));
        }
        self.gcs_mode = true;
        Ok(())
    }
    /// Admit GCS without moving the connection. Discovery BEGIN and later
    /// configuration retain the same connection/snapshot and original budget.
    pub(crate) fn begin_gcs_execution(&mut self) -> io::Result<()> {
        self.local_owner()?;
        self.ownership.recheck()?;
        if self.extensions.is_some() {
            return Err(io::Error::other("reader configuration is fixed per owner"));
        }
        self.admit_gcs_metadata()?;
        let offered = std::time::Instant::now() + std::time::Duration::from_secs(300);
        self.gcs_deadline = Some(
            self.gcs_deadline
                .map_or(offered, |prior| prior.min(offered)),
        );
        Ok(())
    }
    /// Test-only bridge for cross-module tests: arm the existing
    /// publication-boundary lease-expiry probe so the first successful
    /// metadata FFI return is refused before any remote effect. Production
    /// builds never contain this method.
    #[cfg(test)]
    pub(crate) fn arm_lease_expiry_probe_for_test(&mut self) {
        self.test_expire_lease_after_ffi = true;
    }
    pub(crate) fn configure_gcs_reader(
        &mut self,
        reader: &crate::gcs_config::GcsReader,
        token: crate::gcs_token::AccessToken,
    ) -> io::Result<()> {
        self.begin_gcs_execution()?;
        let deadline = self.gcs_deadline.unwrap().min(token.deadline());
        self.gcs_deadline = Some(deadline);
        self.check_reader_deadline()?;
        let extensions = crate::native_extensions::Extensions::stage(self.ownership.engine_path())?;
        let httpfs = cstring(extensions.paths[0].as_os_str().as_encoded_bytes())?;
        let scope = cstring(crate::gcs_config::sql_filename(&reader.scope)?.as_bytes())?;
        let project = cstring(reader.project.as_bytes())?;
        // No bearer IPC/argv/env. Temporary C buffer zeroizes after configure;
        // stock secret/SQL copies are not claimed all wiped.
        let bearer = token.bearer()?;
        let mut private = zeroize::Zeroizing::new(Vec::with_capacity(bearer.len() + 1));
        private.extend_from_slice(bearer.as_bytes());
        private.push(0);
        let remaining = deadline
            .saturating_duration_since(std::time::Instant::now())
            .as_millis();
        if remaining == 0 {
            return Err(io::Error::other("GCS execution lease expired"));
        }
        let watchdog = self.cooperative_deadline()?;
        let mut error = [0; 4096];
        let status = unsafe {
            grv_native_gcs_configure(
                self.owner,
                httpfs.as_ptr(),
                scope.as_ptr(),
                private.as_ptr().cast(),
                project.as_ptr(),
                remaining as u64,
                error.as_mut_ptr(),
                error.len(),
            )
        };
        drop(watchdog);
        self.extensions = Some(extensions);
        self.gcs_setup_outcome(status)?;
        self.check_reader_deadline()?;
        Ok(())
    }
    fn gcs_setup_outcome(&mut self, status: i32) -> io::Result<()> {
        match status {
            0 => Ok(()),
            -1 => Err(io::Error::other("trusted GCS reader initialization failed")),
            _ => {
                // Setup returned synchronously but cleanup was not proved.
                // Quarantine; only owner-thread close/reopen may recover.
                self.owner_unusable = true;
                Err(io::Error::other(
                    "GCS setup cleanup unresolved; close and resolve durable outcome",
                ))
            }
        }
    }
    pub(crate) fn remote_metadata(
        &mut self,
        file: &grv_adapter_api::VerifiedFile,
        query: &str,
    ) -> io::Result<Vec<Vec<Option<String>>>> {
        self.remote_query(std::slice::from_ref(file), query)
    }
    pub(crate) fn remote_command(
        &mut self,
        file: &grv_adapter_api::VerifiedFile,
        query: &str,
    ) -> io::Result<()> {
        self.remote_metadata(file, query).map(|_| ())
    }
    pub(crate) fn remote_files_command(
        &mut self,
        files: &[grv_adapter_api::VerifiedFile],
        query: &str,
    ) -> io::Result<()> {
        self.remote_query(files, query).map(|_| ())
    }
    pub(crate) fn remote_query(
        &mut self,
        files: &[grv_adapter_api::VerifiedFile],
        query: &str,
    ) -> io::Result<Vec<Vec<Option<String>>>> {
        if files
            .first()
            .is_some_and(|f| f.access == grv_adapter_api::FileAccess::S3View)
        {
            return self.s3_query(files, query);
        }
        self.local_owner()?;
        self.ownership.recheck()?;
        self.check_reader_deadline()?;
        let deadline = self
            .gcs_deadline
            .ok_or_else(|| io::Error::other("GCS reader is not configured"))?;
        // Admit all borrowed source coordinates before canonicalization or
        // per-file validation can allocate their derivative strings.
        bounded_gcs_arguments(
            std::iter::once(query).chain(files.iter().map(|f| f.location.as_str())),
        )?;
        if std::time::Instant::now() >= deadline
            || files.is_empty()
            || files.len() > 4096
            || files
                .iter()
                .any(|f| f.access != grv_adapter_api::FileAccess::GcsView || f.validate().is_err())
        {
            return Err(io::Error::other("invalid GCS scanner lease or file set"));
        }
        let mut locations = Vec::with_capacity(files.len());
        let mut total = query.len();
        for file in files {
            let url = crate::gcs_config::sql_filename(&file.location)?;
            total = total
                .checked_add(url.len())
                .filter(|n| *n <= METADATA_BYTES)
                .ok_or_else(|| io::Error::other("GCS coordinate budget exceeded"))?;
            locations.push(url);
        }
        let locations = locations
            .into_iter()
            .map(|url| CString::new(url).map_err(io::Error::other))
            .collect::<io::Result<Vec<_>>>()?;
        let pointers = locations.iter().map(|url| url.as_ptr()).collect::<Vec<_>>();
        let query = cstring(query.as_bytes())?;
        self.check_reader_deadline()?;
        let watchdog = self.cooperative_deadline()?;
        let mut output = vec![0; METADATA_BYTES];
        let mut written = 0;
        let mut error = [0; 4096];
        let status = unsafe {
            grv_native_gcs_query(
                self.owner,
                pointers.as_ptr(),
                pointers.len(),
                query.as_ptr(),
                output.as_mut_ptr(),
                output.len(),
                &mut written,
                error.as_mut_ptr(),
                error.len(),
            )
        };
        drop(watchdog);
        self.ownership.recheck()?;
        self.check_reader_deadline()?;
        if status != 0 {
            return Err(io::Error::other(
                "guarded GCS query refused; resolve durable outcome",
            ));
        }
        if written > output.len() {
            self.owner_unusable = true;
            return Err(io::Error::other("invalid native GCS result length"));
        }
        output.truncate(written);
        decode_gcs_metadata(&output)
    }
    pub(crate) fn configure_s3_reader(
        &mut self,
        reader: &crate::s3_config::S3Reader,
    ) -> io::Result<()> {
        self.local_owner()?;
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
    pub(crate) fn s3_query(
        &mut self,
        files: &[grv_adapter_api::VerifiedFile],
        query: &str,
    ) -> io::Result<Vec<Vec<Option<String>>>> {
        self.local_owner()?;
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
        self.local_owner()?;
        self.gcs_arguments([schema, table])?;
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
        self.local_owner()?;
        self.gcs_arguments([schema, table])?;
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
        if written > output.len() {
            self.owner_unusable = true;
            return Err(io::Error::other("invalid native relation schema length"));
        }
        output.truncate(written);
        if self.gcs_mode {
            decode_gcs_metadata(&output)
        } else {
            decode_metadata(&output)
        }
    }
    pub(crate) fn acquire_build_outputs(
        &mut self,
        outputs: &[grv_adapter_api::OutputBinding],
        external: bool,
    ) -> io::Result<Vec<AcquiredSchema>> {
        self.local_owner()?;
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
        self.local_owner()?;
        self.gcs_arguments([schema, table])?;
        self.ownership.recheck()?;
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
        self.local_owner()?;
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
        self.local_owner()?;
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
        if version != b"v1.5.6" || unsafe { grv_native_guard_revision() } != 6 {
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
            gcs_deadline: None,
            gcs_mode: false,
            owner_unusable: false,
            interrupt: NativeInterrupt {
                pointer: Arc::new(std::sync::Mutex::new(InterruptPointer(handle))),
                cancelled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            },
            ownership: Arc::new(lock),
            _thread_owner: PhantomData,
            #[cfg(test)]
            test_expire_lease_after_ffi: false,
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
    if columns.checked_mul(6) != Some(input.len()) {
        return Err(io::Error::other("invalid native schema shape"));
    }
    let mut types = Vec::new();
    types.try_reserve_exact(columns).map_err(io::Error::other)?;
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
        self.local_owner()?;
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
        self.local_owner()?;
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

// Only explicit generated durable SELECTs enter this helper; no user SQL
// rewriting or public query capability. Fields are the complete ordered
// VARCHAR projection and inner queries already bound their row selection.
fn bounded_durable_sql(query: &str, fields: &[&str]) -> io::Result<String> {
    bounded_gcs_arguments(std::iter::once(query).chain(fields.iter().copied()))?;
    if fields.is_empty()
        || fields.len() > 16
        || fields.iter().enumerate().any(|(index, field)| {
            field.is_empty()
                || field.len() > 64
                || fields[..index].contains(field)
                || field.bytes().any(|byte| {
                    !byte.is_ascii_lowercase() && !byte.is_ascii_digit() && byte != b'_'
                })
        })
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid durable projection",
        ));
    }
    let allowance = METADATA_BYTES - 16 - 9 * fields.len();
    let lengths = fields
        .iter()
        .map(|field| format!("coalesce(bit_length(\"{field}\")//8,0)"))
        .collect::<Vec<_>>()
        .join("+");
    let projection = fields.iter().map(|field| format!("CASE WHEN {lengths}>{allowance} THEN error('durable metadata exceeds bounded record allowance') ELSE \"{field}\" END AS \"{field}\"")).collect::<Vec<_>>().join(",");
    let guarded = format!("SELECT {projection} FROM ({query}) AS _grv_bounded_metadata");
    bounded_gcs_arguments([guarded.as_str()])?;
    Ok(guarded)
}

#[cfg(test)]
mod descriptor_tests {
    #[test]
    fn gcs_relation_schema_keeps_bounded_decode_and_export_reopen_clears_invocation_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schema-lifetime.duckdb");
        let mut engine = super::NativeEngine::open(&path).unwrap();
        engine
            .metadata_query("CREATE TABLE _grv_schema_lifetime(id BIGINT, label VARCHAR)")
            .unwrap();
        let expected = engine
            .relation_schema("main", "_grv_schema_lifetime")
            .unwrap();
        engine.begin_gcs_execution().unwrap();
        assert!(engine.gcs_mode);
        assert_eq!(
            engine
                .relation_schema("main", "_grv_schema_lifetime")
                .unwrap(),
            expected
        );
        engine
            .metadata_query("CREATE TEMP TABLE _grv_invocation_only AS SELECT 42 AS id")
            .unwrap();
        engine.close_invocation().unwrap();
        assert!(!engine.gcs_mode);
        assert!(engine.gcs_deadline.is_none());
        assert!(
            engine
                .metadata_query("SELECT * FROM temp.main._grv_invocation_only")
                .is_err()
        );
        assert_eq!(
            engine
                .relation_schema("main", "_grv_schema_lifetime")
                .unwrap(),
            expected
        );
    }
    #[test]
    fn close_invocation_preserves_readonly_authority() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("readonly.duckdb");
        drop(super::NativeEngine::open(&path).unwrap());
        let mut engine = super::NativeEngine::open_readonly(&path).unwrap();
        assert!(
            engine
                .metadata_query("CREATE TABLE _grv_readonly(id BIGINT)")
                .is_err()
        );
        engine.close_invocation().unwrap();
        assert!(
            engine
                .metadata_query("CREATE TABLE _grv_readonly(id BIGINT)")
                .is_err()
        );
        assert_eq!(
            engine.metadata_query("SELECT 42").unwrap(),
            vec![vec![Some("42".into())]]
        );
    }
    #[test]
    fn configure_with_longer_token_deadline_never_renews_original_lease() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lease.duckdb");
        let mut engine = super::NativeEngine::open(&path).unwrap();
        let owner = engine.owner;
        let interrupt = engine.interrupt_handle();
        engine.begin_gcs_execution().unwrap();
        let initial = engine.gcs_deadline.unwrap();
        assert!(
            initial > std::time::Instant::now() + std::time::Duration::from_secs(290),
            "original lease is not the 300 second budget"
        );
        // A later, shorter lease only shortens.
        let shorter = std::time::Instant::now() + std::time::Duration::from_secs(30);
        engine.gcs_deadline = Some(shorter);
        engine.begin_gcs_execution().unwrap();
        assert_eq!(engine.gcs_deadline, Some(shorter));
        // A longer synthetic token deadline cannot renew the original lease;
        // the deadline remains exactly the shorter original budget regardless
        // of whether the signed extension setup succeeds in this environment.
        let reader = crate::gcs_config::GcsReader {
            scope: "gs://test-bucket/data".into(),
            provider: "gcloud".into(),
            account: "synthetic".into(),
            project: "named-project".into(),
        };
        let token = crate::gcs_token::AccessToken::synthetic_with_canary_for_test(
            std::time::Duration::from_secs(90),
            "synthetic-lease-canary-not-a-credential",
        );
        assert!(token.deadline() > shorter);
        let _ = engine.configure_gcs_reader(&reader, token);
        assert_eq!(engine.gcs_deadline, Some(shorter));
        assert_eq!(engine.owner, owner);
        // The original interrupt clone is preserved across configuration.
        interrupt.interrupt();
        assert!(engine.metadata_query("SELECT 42").is_err());
        // A fresh invocation resets the lease and the interrupt latch.
        engine.close_invocation().unwrap();
        assert!(engine.gcs_deadline.is_none());
        assert_eq!(
            engine.metadata_query("SELECT 42").unwrap(),
            vec![vec![Some("42".into())]]
        );
    }
    #[test]
    fn in_process_failed_setup_quarantines_without_exiting_and_closes_before_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("failed-setup.duckdb");
        let mut engine = super::NativeEngine::open(&path).unwrap();
        engine.begin_gcs_execution().unwrap();
        engine.metadata_query("BEGIN").unwrap();
        engine
            .metadata_query("CREATE TABLE _grv_setup_rollback(id BIGINT)")
            .unwrap();
        let scope = super::cstring(b"https://storage.googleapis.com/test-bucket/data").unwrap();
        let bearer = super::cstring(b"synthetic-not-a-credential").unwrap();
        let project = super::cstring(b"named-project").unwrap();
        let path_missing = super::cstring(b"/missing/signed-httpfs.duckdb_extension").unwrap();
        let mut error = [0; 4096];
        let status = unsafe {
            super::grv_native_gcs_configure(
                engine.owner,
                path_missing.as_ptr(),
                scope.as_ptr(),
                bearer.as_ptr(),
                project.as_ptr(),
                30000,
                error.as_mut_ptr(),
                error.len(),
            )
        };
        assert_ne!(status, 0);
        assert!(engine.gcs_setup_outcome(status).is_err());
        // Independently exercise uncertainty even if this engine can prove the
        // absent secret. Unexpected native statuses also fail closed.
        assert!(engine.gcs_setup_outcome(-2).is_err());
        assert!(engine.metadata_query("SELECT 42").is_err());
        assert!(engine.metadata_query("ROLLBACK").is_err());
        engine.close_invocation().unwrap();
        assert!(
            !engine
                .relation_exists("main", "_grv_setup_rollback")
                .unwrap()
        );
        assert_eq!(
            engine.metadata_query("SELECT 42").unwrap(),
            vec![vec![Some("42".into())]]
        );
    }
    #[test]
    fn in_process_proved_setup_failure_keeps_owner_usable_for_local_work() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proved-failure.duckdb");
        let mut engine = super::NativeEngine::open(&path).unwrap();
        engine.begin_gcs_execution().unwrap();
        engine.metadata_query("BEGIN").unwrap();
        engine
            .metadata_query("CREATE TABLE _grv_proved_probe(id BIGINT)")
            .unwrap();
        let scope = super::cstring(b"https://storage.googleapis.com/test-bucket/data").unwrap();
        let bearer = super::cstring(b"synthetic-not-a-credential").unwrap();
        let project = super::cstring(b"named-project").unwrap();
        let path_missing = super::cstring(b"/missing/signed-httpfs.duckdb_extension").unwrap();
        let mut error = [0; 4096];
        let status = unsafe {
            super::grv_native_gcs_configure(
                engine.owner,
                path_missing.as_ptr(),
                scope.as_ptr(),
                bearer.as_ptr(),
                project.as_ptr(),
                30000,
                error.as_mut_ptr(),
                error.len(),
            )
        };
        assert_ne!(status, 0);
        // Explicitly exercise the Rust -1 classification even where missing
        // LOAD produces -2. Native proved-absence evidence is in setup proof.
        assert!(engine.gcs_setup_outcome(-1).is_err());
        assert!(!engine.owner_unusable);
        if status == -1 {
            // Proved absence and restoration: the owner is NOT poisoned;
            // local work continues and only cloud reuse is denied by the
            // failed state.
            assert!(engine.gcs_setup_outcome(status).is_err());
            assert!(!engine.owner_unusable);
            assert_eq!(
                engine.metadata_query("SELECT 42").unwrap(),
                vec![vec![Some("42".into())]]
            );
            engine.metadata_query("ROLLBACK").unwrap();
            assert!(!engine.relation_exists("main", "_grv_proved_probe").unwrap());
        } else {
            // -2 or an unexpected status poisons operational reuse; only a
            // synchronous owner-thread close/reopen recovers.
            assert_ne!(status, -1);
            assert!(engine.gcs_setup_outcome(status).is_err());
            assert!(engine.metadata_query("SELECT 42").is_err());
            engine.close_invocation().unwrap();
            assert_eq!(
                engine.metadata_query("SELECT 42").unwrap(),
                vec![vec![Some("42".into())]]
            );
        }
    }
    #[test]
    fn in_process_original_lease_only_shortens_and_discovery_uses_same_connection() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = super::NativeEngine::open(&dir.path().join("snapshot.duckdb")).unwrap();
        let owner = engine.owner;
        let interrupt = engine.interrupt_handle();
        engine.begin_gcs_execution().unwrap();
        let initial = engine.gcs_deadline.unwrap();
        engine.metadata_query("BEGIN").unwrap();
        engine
            .metadata_query("CREATE TABLE _grv_snapshot(id BIGINT)")
            .unwrap();
        engine.begin_gcs_execution().unwrap();
        assert_eq!(engine.owner, owner);
        assert_eq!(engine.gcs_deadline, Some(initial));
        let shorter = std::time::Instant::now() + std::time::Duration::from_secs(30);
        engine.gcs_deadline = Some(shorter);
        engine.begin_gcs_execution().unwrap();
        assert_eq!(engine.gcs_deadline, Some(shorter));
        interrupt.interrupt();
        assert!(engine.metadata_query("SELECT 42").is_err());
        engine.metadata_query("ROLLBACK").unwrap();
        engine.close_invocation().unwrap();
        assert!(!engine.relation_exists("main", "_grv_snapshot").unwrap());
    }
    #[test]
    fn in_process_lease_expiry_refuses_at_publication_boundary_and_reopens_immutably() {
        use std::time::{Duration, Instant};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lease-expiry.duckdb");
        let mut engine = super::NativeEngine::open(&path).unwrap();
        engine.begin_gcs_execution().unwrap();
        // A normal read works under the fresh lease.
        assert_eq!(
            engine.metadata_query("SELECT 42").unwrap(),
            vec![vec![Some("42".into())]]
        );
        engine.metadata_query("BEGIN").unwrap();
        // Expire the original lease before dispatch: the call is refused before
        // any native effect, and the transaction is left exactly as found.
        engine.gcs_deadline = Some(Instant::now() - Duration::from_secs(1));
        assert!(
            engine
                .metadata_query("CREATE TABLE _grv_lease_refused(id BIGINT)")
                .is_err()
        );
        assert!(
            !engine
                .relation_exists("main", "_grv_lease_refused")
                .unwrap()
        );
        // An explicit ROLLBACK remains supported once control has returned.
        engine.metadata_query("ROLLBACK").unwrap();
        // A fresh invocation resets the lease (and the interrupt latch).
        engine.close_invocation().unwrap();
        assert!(engine.gcs_deadline.is_none());
        engine.begin_gcs_execution().unwrap();
        engine.metadata_query("BEGIN").unwrap();
        engine
            .metadata_query("CREATE TABLE _grv_lease_commit(id BIGINT)")
            .unwrap();
        engine
            .metadata_query("INSERT INTO _grv_lease_commit VALUES (42)")
            .unwrap();
        // Test-only: expire the original lease immediately after the synchronous
        // native COMMIT returns, but before the final publication check.
        engine.test_expire_lease_after_ffi = true;
        let error = engine.metadata_query("COMMIT").unwrap_err();
        // The native COMMIT succeeded (the FFI returned 0), but the result is
        // refused at the Rust publication boundary because the lease expired.
        // The caller sees a deadline error; the native outcome is uncertain to
        // it and is NOT proof of rollback.
        assert_eq!(error.kind(), std::io::ErrorKind::Interrupted);
        assert!(
            error.to_string().contains("deadline"),
            "{}",
            error.to_string()
        );
        // After the synchronous return, close_invocation retains the actual
        // workspace ownership; a fresh immutable read confirms the effect
        // exactly once, with no effects retry.
        engine.close_invocation().unwrap();
        engine
            .authorize_relation("main", "_grv_lease_commit")
            .unwrap();
        assert_eq!(
            engine
                .metadata_query("SELECT id FROM _grv_lease_commit")
                .unwrap(),
            vec![vec![Some("42".into())]]
        );
        assert_eq!(
            engine
                .metadata_query("SELECT count(*)::BIGINT FROM _grv_lease_commit")
                .unwrap(),
            vec![vec![Some("1".into())]]
        );
    }
    #[test]
    fn durable_projection_is_closed_and_counts_bytes_not_characters() {
        let sql = super::bounded_durable_sql(
            "SELECT record,session_id FROM _grv.build_sessions LIMIT 2",
            &["record", "session_id"],
        )
        .unwrap();
        assert!(
            super::bounded_durable_sql(
                "SELECT request_sha256 FROM _grv.pull_attempts LIMIT 2",
                &["request_sha256"]
            )
            .is_ok()
        );
        assert!(sql.contains("bit_length(\"record\")//8"));
        assert!(sql.contains("bit_length(\"session_id\")//8"));
        assert!(sql.contains("error('durable metadata exceeds bounded record allowance')"));
        assert!(sql.contains("ELSE \"record\" END AS \"record\""));
        assert!(!sql.contains("WHERE"));
        for fields in [
            vec![],
            vec!["record", "record"],
            vec!["record;"],
            vec!["Record"],
        ] {
            assert!(super::bounded_durable_sql("SELECT 1", &fields).is_err());
        }
        assert!(super::bounded_durable_sql("SELECT 1", &[&"r".repeat(65)]).is_err());
        assert!(super::bounded_durable_sql(&"x".repeat(3 * 1024 * 1024), &["receipt"]).is_err());
    }
    #[test]
    #[ignore = "requires protected signed extensions; synthetic token, no cloud"]
    fn in_process_durable_projection_preserves_rows_and_refuses_oversize_without_repair() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("durable-budget.duckdb");
        let mut engine = super::NativeEngine::open(&path).unwrap();
        let reader = crate::gcs_config::GcsReader {
            scope: "gs://test-bucket/data".into(),
            provider: "gcloud".into(),
            account: "synthetic".into(),
            project: "named-project".into(),
        };
        engine
            .configure_gcs_reader(
                &reader,
                crate::gcs_token::AccessToken::synthetic_for_test(std::time::Duration::from_secs(
                    30,
                )),
            )
            .unwrap();
        engine.metadata_query("CREATE SCHEMA _grv").unwrap();
        engine
            .metadata_query(
                "CREATE TABLE _grv.pull_attempts(receipt VARCHAR,adapter_identity VARCHAR)",
            )
            .unwrap();
        engine
            .metadata_query("INSERT INTO _grv.pull_attempts VALUES ('🍕',NULL),('🍕',NULL)")
            .unwrap();
        let sql = "SELECT receipt,adapter_identity FROM _grv.pull_attempts LIMIT 2";
        assert_eq!(
            engine
                .durable_metadata_query(sql, &["receipt", "adapter_identity"])
                .unwrap(),
            vec![vec![Some("🍕".into()), None]; 2]
        );
        assert!(
            engine
                .durable_metadata_query(
                    "SELECT receipt FROM _grv.pull_attempts LIMIT 0",
                    &["receipt"]
                )
                .unwrap()
                .is_empty()
        );
        assert!(
            engine
                .durable_metadata_query(
                    "SELECT receipt FROM _grv.pull_attempts WHERE receipt='absent' LIMIT 2",
                    &["receipt"]
                )
                .unwrap()
                .is_empty()
        );
        engine
            .metadata_query("DELETE FROM _grv.pull_attempts")
            .unwrap();
        engine
            .metadata_query("INSERT INTO _grv.pull_attempts VALUES ('',NULL),(NULL,NULL)")
            .unwrap();
        assert_eq!(
            engine
                .durable_metadata_query(sql, &["receipt", "adapter_identity"])
                .unwrap(),
            vec![vec![Some(String::new()), None], vec![None, None]]
        );
        engine
            .metadata_query("DELETE FROM _grv.pull_attempts")
            .unwrap();
        // Exact aggregate UTF-8 boundary, not Unicode code-point count. SQL is
        // small; engine constructs the large fixture only in this private test.
        let allowance = super::METADATA_BYTES - 16 - 18;
        let suffix = "z".repeat(allowance % 4);
        engine
            .metadata_query(&format!(
                "INSERT INTO _grv.pull_attempts VALUES (repeat('🍕',{})||'{}',NULL)",
                allowance / 4,
                suffix
            ))
            .unwrap();
        let boundary = engine
            .durable_metadata_query(sql, &["receipt", "adapter_identity"])
            .unwrap();
        assert_eq!(boundary[0][0].as_ref().unwrap().len(), allowance);
        drop(boundary);
        // Inner row selection excludes an oversized row beyond LIMIT1; CASE
        // must not inspect/reinterpret rows outside that selection.
        engine
            .metadata_query("INSERT INTO _grv.pull_attempts VALUES (repeat('x',2200000),NULL)")
            .unwrap();
        assert_eq!(engine.durable_metadata_query("SELECT receipt,adapter_identity FROM _grv.pull_attempts ORDER BY rowid LIMIT 1", &["receipt","adapter_identity"]).unwrap()[0][0].as_ref().unwrap().len(),allowance);
        engine
            .metadata_query("DELETE FROM _grv.pull_attempts")
            .unwrap();
        // Each value alone fits, but their aggregate encoding does not.
        engine
            .metadata_query(
                "INSERT INTO _grv.pull_attempts VALUES (repeat('x',1100000),repeat('y',1100000))",
            )
            .unwrap();
        assert!(
            engine
                .durable_metadata_query(sql, &["receipt", "adapter_identity"])
                .is_err()
        );
        // Error never filters the record into an empty/NotCommitted result.
        // Fence/reopen first, then verify no truncation or mutation/repair.
        engine.close_invocation().unwrap();
        assert_eq!(engine.metadata_query("SELECT bit_length(receipt)//8,bit_length(adapter_identity)//8 FROM _grv.pull_attempts").unwrap(),vec![vec![Some("1100000".into()),Some("1100000".into())]]);
    }
    #[test]
    fn native_schema_declared_count_refuses_before_allocation() {
        assert!(super::decode_schema(&u32::MAX.to_le_bytes(), 0, 0).is_err());
        assert!(super::decode_schema(&[1, 0, 0, 0], 0, 0).is_err());
        assert!(super::decode_schema(&[1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0], 0, 0).is_err());
        assert_eq!(
            super::decode_schema(&[1, 0, 0, 0, 1, 0, 0, 0, 0, 0], 0, 0)
                .unwrap()
                .types,
            vec![SourceType::Boolean]
        );
    }
    #[test]
    fn metadata_shape_amplification_is_refused_before_owned_allocation() {
        let rows = (super::METADATA_BYTES - 16) / 9;
        let mut raw = Vec::with_capacity(super::METADATA_BYTES);
        raw.extend_from_slice(&(rows as u64).to_le_bytes());
        raw.extend_from_slice(&1u64.to_le_bytes());
        for _ in 0..rows {
            raw.push(1);
            raw.extend_from_slice(&0u64.to_le_bytes());
        }
        assert!(super::decode_gcs_metadata(&raw).is_err());
        assert_eq!(super::decode_metadata(&raw).unwrap().len(), rows);
        let per_row =
            std::mem::size_of::<Vec<Option<String>>>() + std::mem::size_of::<Option<String>>();
        let admitted = super::DECODED_METADATA_LIMIT / per_row;
        let mut boundary = Vec::new();
        boundary.extend_from_slice(&(admitted as u64).to_le_bytes());
        boundary.extend_from_slice(&1u64.to_le_bytes());
        for _ in 0..admitted {
            boundary.push(0);
            boundary.extend_from_slice(&0u64.to_le_bytes());
        }
        let decoded = super::decode_gcs_metadata(&boundary).unwrap();
        let owned_bytes = decoded.capacity() * std::mem::size_of::<Vec<Option<String>>>()
            + decoded
                .iter()
                .map(|row| row.capacity() * std::mem::size_of::<Option<String>>())
                .sum::<usize>();
        assert!(owned_bytes <= super::DECODED_METADATA_LIMIT);
        drop(decoded);
        boundary[..8].copy_from_slice(&((admitted + 1) as u64).to_le_bytes());
        boundary.push(0);
        boundary.extend_from_slice(&0u64.to_le_bytes());
        assert!(super::decode_gcs_metadata(&boundary).is_err());
        let mut zero_columns = Vec::new();
        zero_columns.extend_from_slice(&100u64.to_le_bytes());
        zero_columns.extend_from_slice(&0u64.to_le_bytes());
        assert!(super::decode_metadata(&zero_columns).is_err());
        let mut valid = Vec::new();
        valid.extend_from_slice(&1u64.to_le_bytes());
        valid.extend_from_slice(&1u64.to_le_bytes());
        valid.push(1);
        valid.extend_from_slice(&2u64.to_le_bytes());
        valid.extend_from_slice(b"42");
        assert_eq!(
            super::decode_metadata(&valid).unwrap(),
            vec![vec![Some("42".into())]]
        );
        valid.push(0);
        assert!(super::decode_metadata(&valid).is_err());
    }
    #[test]
    #[ignore = "requires protected signed extensions; synthetic token, no cloud"]
    fn in_process_configure_preserves_discovery_transaction_and_original_interrupt_handle() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("configure-snapshot.duckdb");
        let mut engine = super::NativeEngine::open(&path).unwrap();
        let owner = engine.owner;
        let interrupt = engine.interrupt_handle();
        engine.begin_gcs_execution().unwrap();
        engine.metadata_query("BEGIN").unwrap();
        engine
            .metadata_query("CREATE TABLE _grv_discovery_snapshot(id BIGINT)")
            .unwrap();
        let reader = crate::gcs_config::GcsReader {
            scope: "gs://test-bucket/data".into(),
            provider: "gcloud".into(),
            account: "synthetic".into(),
            project: "named-project".into(),
        };
        engine
            .configure_gcs_reader(
                &reader,
                crate::gcs_token::AccessToken::synthetic_for_test(std::time::Duration::from_secs(
                    30,
                )),
            )
            .unwrap();
        assert_eq!(engine.owner, owner);
        interrupt.interrupt();
        assert!(engine.metadata_query("SELECT 42").is_err());
        engine.metadata_query("ROLLBACK").unwrap();
        engine.close_invocation().unwrap();
        assert!(
            !engine
                .relation_exists("main", "_grv_discovery_snapshot")
                .unwrap()
        );
    }
    #[test]
    #[ignore = "requires protected signed GRV_DUCKDB_EXTENSIONS_DIR; no cloud requests or credentials"]
    fn gcs_deadline_watchdog_interrupts_without_network_or_commit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("deadline.duckdb");
        let mut engine = super::NativeEngine::open(&path).unwrap();
        let reader = crate::gcs_config::GcsReader {
            scope: "gs://test-bucket/data".into(),
            provider: "gcloud".into(),
            account: "synthetic".into(),
            project: "named-project".into(),
        };
        let token =
            crate::gcs_token::AccessToken::synthetic_for_test(std::time::Duration::from_secs(4));
        engine.configure_gcs_reader(&reader, token).unwrap();
        engine.metadata_query("BEGIN").unwrap();
        engine
            .metadata_query("CREATE TABLE _grv_deadline_work (id BIGINT)")
            .unwrap();
        let file = grv_adapter_api::VerifiedFile {
            table: grv_types::Name::new("rows").unwrap(),
            partition: serde_json::json!({}),
            version: grv_types::U64::new(1).unwrap(),
            schema: grv_adapter_api::FileSchema {
                columns: vec![grv_types::Column {
                    name: "id".into(),
                    logical_type: serde_json::json!("int64"),
                }],
            },
            access: grv_adapter_api::FileAccess::GcsView,
            location: "gs://test-bucket/data/part.parquet".into(),
            size: grv_types::U64::new(1).unwrap(),
            sha256: grv_types::Digest::new("a".repeat(64)).unwrap(),
            validator: "1".into(),
        };
        let begin = std::time::Instant::now();
        assert!(
            engine
                .remote_metadata(
                    &file,
                    "SELECT SUM(i::DOUBLE) FROM range(1000000000000) t(i)"
                )
                .is_err()
        );
        assert!(begin.elapsed() < std::time::Duration::from_secs(8));
        assert!(engine.check_reader_deadline().is_err());
        // Cooperative compute interruption returned; explicit rollback remains
        // available after expiry. Reopen under ownership verifies durable state.
        engine.metadata_query("ROLLBACK").unwrap();
        engine.close_invocation().unwrap();
        assert!(
            engine
                .metadata_query("SELECT * FROM _grv_deadline_work")
                .is_err()
        );
        drop(engine);
        super::NativeEngine::open(&path).unwrap();
    }
    #[test]
    #[ignore = "requires protected signed extensions; synthetic token, no cloud"]
    fn gcs_owner_normal_failure_rolls_back_and_committed_state_reopens() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("owner-recovery.duckdb");
        let mut engine = super::NativeEngine::open(&path).unwrap();
        let reader = crate::gcs_config::GcsReader {
            scope: "gs://test-bucket/data".into(),
            provider: "gcloud".into(),
            account: "synthetic".into(),
            project: "named-project".into(),
        };
        engine
            .configure_gcs_reader(
                &reader,
                crate::gcs_token::AccessToken::synthetic_for_test(std::time::Duration::from_secs(
                    10,
                )),
            )
            .unwrap();
        engine.metadata_query("BEGIN").unwrap();
        engine
            .metadata_query("CREATE TABLE _grv_owner_rolled_back (id BIGINT)")
            .unwrap();
        assert!(
            engine
                .metadata_query("SELECT * FROM _grv_unlisted_relation")
                .is_err()
        );
        engine.metadata_query("ROLLBACK").unwrap();
        assert!(
            !engine
                .relation_exists("main", "_grv_owner_rolled_back")
                .unwrap()
        );
        engine
            .metadata_query("CREATE TABLE _grv_owner_committed AS SELECT 42::BIGINT AS id")
            .unwrap();
        engine.close_invocation().unwrap();
        engine
            .authorize_relation("main", "_grv_owner_committed")
            .unwrap();
        assert_eq!(
            engine
                .metadata_query("SELECT id FROM _grv_owner_committed")
                .unwrap(),
            vec![vec![Some("42".into())]]
        );
        // Local connection after fencing has no inherited credential/settings.
        assert!(
            engine
                .metadata_query("SELECT * FROM duckdb_secrets()")
                .is_err()
        );
        drop(engine);
        let mut reopened = super::NativeEngine::open(&path).unwrap();
        reopened
            .authorize_relation("main", "_grv_owner_committed")
            .unwrap();
        assert_eq!(
            reopened
                .metadata_query("SELECT id FROM _grv_owner_committed")
                .unwrap(),
            vec![vec![Some("42".into())]]
        );
    }
    #[test]
    #[ignore = "requires signed extensions; synthetic token and local SQL, no cloud"]
    fn gcs_streamed_metadata_refuses_large_results_and_closes_before_rollback() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("streamed-metadata.duckdb");
        let mut engine = super::NativeEngine::open(&path).unwrap();
        let reader = crate::gcs_config::GcsReader {
            scope: "gs://test-bucket/data".into(),
            provider: "gcloud".into(),
            account: "synthetic".into(),
            project: "named-project".into(),
        };
        engine
            .configure_gcs_reader(
                &reader,
                crate::gcs_token::AccessToken::synthetic_for_test(std::time::Duration::from_secs(
                    30,
                )),
            )
            .unwrap();
        assert_eq!(
            engine
                .metadata_query("SELECT sum(i)::BIGINT FROM range(1000000) r(i)")
                .unwrap(),
            vec![vec![Some("499999500000".into())]]
        );
        engine.metadata_query("BEGIN").unwrap();
        engine
            .metadata_query("CREATE TABLE _grv_stream_uncommitted(id BIGINT)")
            .unwrap();
        let refusal_started = std::time::Instant::now();
        assert!(
            engine
                .metadata_query("SELECT i FROM range(1000000000) r(i)")
                .is_err()
        );
        assert!(
            refusal_started.elapsed() < std::time::Duration::from_secs(5),
            "bounded prefix refusal must not drain/materialize a billion rows"
        );
        engine.metadata_query("ROLLBACK").unwrap();
        assert!(
            !engine
                .relation_exists("main", "_grv_stream_uncommitted")
                .unwrap()
        );
        assert!(engine.metadata_query("SELECT repeat('x',3000000)").is_err());
        assert!(engine.metadata_query("SELECT repeat('x',6000000)").is_err());
        assert_eq!(
            engine.metadata_query("SELECT 42::BIGINT").unwrap(),
            vec![vec![Some("42".into())]]
        );
        assert!(
            engine
                .metadata_query("SELECT (1/(i-i))::BIGINT FROM range(4097) r(i)")
                .is_err()
        );
        engine.metadata_query("BEGIN").unwrap();
        engine
            .metadata_query("CREATE TABLE _grv_after_stream_error(id BIGINT)")
            .unwrap();
        engine.metadata_query("ROLLBACK").unwrap();
        assert!(
            !engine
                .relation_exists("main", "_grv_after_stream_error")
                .unwrap()
        );
        assert_eq!(
            engine
                .metadata_query("SELECT 0.125::DOUBLE, 12.34::DECIMAL(8,2), NULL::VARCHAR")
                .unwrap(),
            vec![vec![Some("0.125".into()), Some("12.34".into()), None]]
        );
        // Nested renderings are refused rather than allocating ToString copies.
        assert!(engine.metadata_query("SELECT [1,2,3]").is_err());
        let multi = engine
            .metadata_query("SELECT i FROM range(4097) r(i)")
            .unwrap();
        assert_eq!(multi.len(), 4097);
        assert_eq!(multi.last().unwrap(), &vec![Some("4096".into())]);
        engine.close_invocation().unwrap();
        drop(engine);
        let mut reopened = super::NativeEngine::open(&path).unwrap();
        assert!(
            !reopened
                .relation_exists("main", "_grv_stream_uncommitted")
                .unwrap()
        );
    }
    #[test]
    fn in_process_argument_rejection_preserves_transaction_before_any_copy_or_effect() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("argument-budget.duckdb");
        let mut engine = super::NativeEngine::open(&path).unwrap();
        engine.begin_gcs_execution().unwrap();
        engine.metadata_query("BEGIN").unwrap();
        let oversize = "x".repeat(40 * 1024 * 1024);
        assert!(engine.metadata_query(&oversize).is_err());
        assert!(
            engine
                .stage_pull_query(&oversize, "_grv_stage", "{}")
                .is_err()
        );
        assert!(
            engine
                .stage_build_query(&oversize, "projection", "_grv_stage", "{}", 1)
                .is_err()
        );
        assert!(engine.schema_exists(&oversize).is_err());
        assert!(engine.authorize_relation("main", &oversize).is_err());
        assert!(engine.relation_schema("main", &oversize).is_err());
        // Input admission must not turn a known refusal into transport failure
        // or end the discovery snapshot. The same child rolls back normally.
        engine.metadata_query("ROLLBACK").unwrap();
        engine.close_invocation().unwrap();
        drop(engine);
        super::NativeEngine::open(&path).unwrap();
    }
    #[test]
    #[ignore = "requires protected signed extensions; distinct synthetic canaries, no cloud"]
    fn concurrent_gcs_owners_cancel_and_reconfigure_without_cross_owner_authority() {
        const CANARY_A: &[u8] = b"synthetic-canary-owner-a-not-a-credential";
        const CANARY_B: &[u8] = b"synthetic-canary-owner-b-not-a-credential";
        let dir = tempfile::tempdir().unwrap();
        let first_path = dir.path().join("first.duckdb");
        let second_path = dir.path().join("second.duckdb");
        let reader_a = crate::gcs_config::GcsReader {
            scope: "gs://test-bucket-a/data".into(),
            provider: "gcloud".into(),
            account: "synthetic".into(),
            project: "named-project-a".into(),
        };
        let reader_b = crate::gcs_config::GcsReader {
            scope: "gs://test-bucket-b/data".into(),
            provider: "gcloud".into(),
            account: "synthetic".into(),
            project: "named-project-b".into(),
        };
        let mut first = super::NativeEngine::open(&first_path).unwrap();
        let mut second = super::NativeEngine::open(&second_path).unwrap();
        first
            .configure_gcs_reader(
                &reader_a,
                crate::gcs_token::AccessToken::synthetic_with_canary_for_test(
                    std::time::Duration::from_secs(10),
                    "synthetic-canary-owner-a-not-a-credential",
                ),
            )
            .unwrap();
        second
            .configure_gcs_reader(
                &reader_b,
                crate::gcs_token::AccessToken::synthetic_with_canary_for_test(
                    std::time::Duration::from_secs(10),
                    "synthetic-canary-owner-b-not-a-credential",
                ),
            )
            .unwrap();
        for engine in [&mut first, &mut second] {
            assert!(
                engine
                    .metadata_query("SELECT * FROM duckdb_secrets()")
                    .is_err()
            );
            assert!(
                engine
                    .metadata_query("SELECT * FROM duckdb_settings()")
                    .is_err()
            );
            engine
                .metadata_query("CREATE TABLE _grv_owner_identity(id BIGINT)")
                .unwrap();
            engine
                .authorize_relation("main", "_grv_owner_identity")
                .unwrap();
        }
        first
            .metadata_query("INSERT INTO _grv_owner_identity VALUES (11)")
            .unwrap();
        second
            .metadata_query("INSERT INTO _grv_owner_identity VALUES (22)")
            .unwrap();
        // Cross-scope: owner A cannot read a coordinate in owner B's scope;
        // the refusal is canary-free.
        let cross = grv_adapter_api::VerifiedFile {
            table: grv_types::Name::new("rows").unwrap(),
            partition: serde_json::json!({}),
            version: grv_types::U64::new(1).unwrap(),
            schema: grv_adapter_api::FileSchema {
                columns: vec![grv_types::Column {
                    name: "id".into(),
                    logical_type: serde_json::json!("int64"),
                }],
            },
            access: grv_adapter_api::FileAccess::GcsView,
            location: "gs://test-bucket-b/data/cross.parquet".into(),
            size: grv_types::U64::new(1).unwrap(),
            sha256: grv_types::Digest::new("a".repeat(64)).unwrap(),
            validator: "1".into(),
        };
        let cross_error = first
            .remote_metadata(&cross, "SELECT 42")
            .unwrap_err()
            .to_string();
        assert!(!cross_error.contains("synthetic-canary-owner-a-not-a-credential"));
        assert!(!cross_error.contains("synthetic-canary-owner-b-not-a-credential"));
        first.interrupt_handle().interrupt();
        assert_eq!(
            first.remote_query(&[], "SELECT 42").unwrap_err().kind(),
            std::io::ErrorKind::Interrupted
        );
        assert!(
            first
                .metadata_query("SELECT id FROM _grv_owner_identity")
                .is_err()
        );
        assert_eq!(
            second
                .metadata_query("SELECT id FROM _grv_owner_identity")
                .unwrap(),
            vec![vec![Some("22".into())]]
        );
        first.close_invocation().unwrap();
        drop(first);
        let mut first = super::NativeEngine::open(&first_path).unwrap();
        first
            .configure_gcs_reader(
                &reader_a,
                crate::gcs_token::AccessToken::synthetic_with_canary_for_test(
                    std::time::Duration::from_secs(10),
                    "synthetic-canary-owner-a-not-a-credential",
                ),
            )
            .unwrap();
        first
            .authorize_relation("main", "_grv_owner_identity")
            .unwrap();
        assert_eq!(
            first
                .metadata_query("SELECT id FROM _grv_owner_identity")
                .unwrap(),
            vec![vec![Some("11".into())]]
        );
        second.close_invocation().unwrap();
        first.close_invocation().unwrap();
        drop(first);
        drop(second);
        // Every owned durable artifact in the workspace (DB, WAL, journal,
        // export, view, receipt) is canary-free. No heap-wipe claim is made.
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                let bytes = std::fs::read(entry.path()).unwrap();
                for canary in [CANARY_A, CANARY_B] {
                    assert!(
                        !bytes.windows(canary.len()).any(|window| window == canary),
                        "canary found in {:?}",
                        entry.path()
                    );
                }
            }
        }
    }
    #[test]
    #[ignore = "requires protected signed extensions; identical synthetic token values, no cloud"]
    fn synthetic_identical_tokens_configure_independent_owners() {
        const SHARED: &str = "synthetic-identical-token-value-not-a-credential";
        let dir = tempfile::tempdir().unwrap();
        let first_path = dir.path().join("identical-first.duckdb");
        let second_path = dir.path().join("identical-second.duckdb");
        let reader_a = crate::gcs_config::GcsReader {
            scope: "gs://identical-a-bucket/data".into(),
            provider: "gcloud".into(),
            account: "synthetic".into(),
            project: "named-project-a".into(),
        };
        let reader_b = crate::gcs_config::GcsReader {
            scope: "gs://identical-b-bucket/data".into(),
            provider: "gcloud".into(),
            account: "synthetic".into(),
            project: "named-project-b".into(),
        };
        let mut first = super::NativeEngine::open(&first_path).unwrap();
        let mut second = super::NativeEngine::open(&second_path).unwrap();
        // Two synthetic invocations carry the SAME token value. This exercises
        // configure/owner isolation, not provider acquisition (qualified separately).
        first
            .configure_gcs_reader(
                &reader_a,
                crate::gcs_token::AccessToken::synthetic_with_canary_for_test(
                    std::time::Duration::from_secs(10),
                    SHARED,
                ),
            )
            .unwrap();
        second
            .configure_gcs_reader(
                &reader_b,
                crate::gcs_token::AccessToken::synthetic_with_canary_for_test(
                    std::time::Duration::from_secs(10),
                    SHARED,
                ),
            )
            .unwrap();
        for engine in [&mut first, &mut second] {
            let error = engine
                .metadata_query("SELECT * FROM duckdb_secrets()")
                .unwrap_err()
                .to_string();
            assert!(
                !error.contains(SHARED),
                "refusal carried the shared token value"
            );
        }
        first.close_invocation().unwrap();
        second.close_invocation().unwrap();
        drop(first);
        drop(second);
        // A fresh owner configured with the same synthetic value works
        // independently; no credential provider is invoked here.
        let mut reopened = super::NativeEngine::open(&first_path).unwrap();
        reopened
            .configure_gcs_reader(
                &reader_a,
                crate::gcs_token::AccessToken::synthetic_with_canary_for_test(
                    std::time::Duration::from_secs(10),
                    SHARED,
                ),
            )
            .unwrap();
        reopened.close_invocation().unwrap();
        drop(reopened);
    }
    #[test]
    #[ignore = "real shipping-FFI GCS route (package 32 narrow gate): parent launches with GRV_GCS_ACCOUNT=jiacao@on.sinch.com, GRV_GCS_PROJECT=entit-finance-prod, two exclusively owned fixture URIs (GRV_TEST_GCS_URI_A/B, one BIGINT id row each: 111/222) and GRV_DUCKDB_EXTENSIONS_DIR at the signed extensions; real force-refresh provider only, token memory-only, no cloud mutation, no synthetic tokens; run only after parent review and artifact freeze"]
    fn real_shipping_gcs_route_repeated_reads_and_unadmitted_url_refusal() {
        // Explicit inputs are validated before any provider acquisition.
        let account = std::env::var("GRV_GCS_ACCOUNT").expect("explicit GRV_GCS_ACCOUNT required");
        let project = std::env::var("GRV_GCS_PROJECT").expect("explicit GRV_GCS_PROJECT required");
        assert_eq!(account, "jiacao@on.sinch.com");
        assert_eq!(project, "entit-finance-prod");
        let uri_a = std::env::var("GRV_TEST_GCS_URI_A").expect("explicit owned fixture A required");
        let uri_b = std::env::var("GRV_TEST_GCS_URI_B").expect("explicit owned fixture B required");
        assert_ne!(uri_a, uri_b);
        let filename_a = crate::gcs_config::sql_filename(&uri_a).unwrap();
        let filename_b = crate::gcs_config::sql_filename(&uri_b).unwrap();
        let coordinate = |uri: &str| {
            let (bucket, key) = uri.strip_prefix("gs://").unwrap().split_once('/').unwrap();
            let suffix = bucket.strip_prefix("grv-httpfs-oauth-").unwrap();
            assert_eq!(suffix.len(), 24);
            assert!(
                suffix
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            );
            assert!(matches!(
                key,
                "fixtures/rows-111.parquet" | "fixtures/rows-222.parquet"
            ));
            bucket.to_owned()
        };
        let bucket = coordinate(&uri_a);
        assert_eq!(coordinate(&uri_b), bucket);
        assert!(uri_a.ends_with("/rows-111.parquet") && uri_b.ends_with("/rows-222.parquet"));
        let scope = format!("gs://{bucket}/fixtures");

        let file_a = gcs_fixture_file(&uri_a, "A");
        let file_b = gcs_fixture_file(&uri_b, "B");
        file_a.validate().unwrap();
        file_b.validate().unwrap();
        let dir = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR")).unwrap();
        let path = dir.path().join("real-route.duckdb");
        let mut engine = super::NativeEngine::open(&path).unwrap();
        let owner = engine.owner;
        // The original lease starts BEFORE the provider acquisition; the fresh
        // token must not renew it.
        engine.begin_gcs_execution().unwrap();
        let original_deadline = engine.gcs_deadline.unwrap();
        let token =
            crate::gcs_token::acquire(&account, &project, std::time::Duration::from_secs(300))
                .unwrap();
        // Memory-only credential: no argv/env/log/receipt/hash below.
        let bearer = zeroize::Zeroizing::new(token.bearer().unwrap().to_owned());
        engine
            .configure_gcs_reader(
                &crate::gcs_config::GcsReader {
                    scope,
                    provider: "gcloud".into(),
                    account,
                    project,
                },
                token,
            )
            .unwrap();
        assert_eq!(engine.owner, owner);
        assert_eq!(engine.gcs_deadline, Some(original_deadline));

        let read_a = format!(
            "SELECT id::BIGINT FROM read_parquet({}, hive_partitioning=false, union_by_name=false)",
            crate::pull::quote_literal(&filename_a)
        );
        let read_b = format!(
            "SELECT id::BIGINT FROM read_parquet({}, hive_partitioning=false, union_by_name=false)",
            crate::pull::quote_literal(&filename_b)
        );
        // Repeated scope epochs through the shipping FFI and real signed
        // request parameters: A, B, A. No cache/request-count/pruning/rebind
        // or full-SDK claim is made from these reads.
        assert_eq!(
            engine
                .remote_query(std::slice::from_ref(&file_a), &read_a)
                .unwrap(),
            vec![vec![Some("111".into())]]
        );
        assert_eq!(
            engine
                .remote_query(std::slice::from_ref(&file_b), &read_b)
                .unwrap(),
            vec![vec![Some("222".into())]]
        );
        assert_eq!(
            engine
                .remote_query(std::slice::from_ref(&file_a), &read_a)
                .unwrap(),
            vec![vec![Some("111".into())]]
        );
        // Final: only URL-A is admitted for this scan; the query references
        // URL-B. The exact filesystem refuses the unadmitted open before any
        // HTTP dispatch. The failure is preserved: no retry, no reuse.
        let refusal = engine
            .remote_query(std::slice::from_ref(&file_a), &read_b)
            .unwrap_err();
        assert!(
            !refusal.to_string().contains(&*bearer),
            "refusal carried the credential"
        );
        // Close/join ownership before any inspection.
        engine.close_invocation().unwrap();
        drop(engine);
        assert!(WorkspaceLock::acquire(&path).is_ok());
        // Present closed workspace artifacts are scanned; this does not prove
        // arbitrary host memory, argv/environment history or other logs erased.
        let mut present = 0;
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let entry = entry.unwrap();
            let kind = std::fs::symlink_metadata(entry.path()).unwrap().file_type();
            assert!(kind.is_file(), "unexpected owned workspace entry");
            {
                present += 1;
                let bytes = std::fs::read(entry.path()).unwrap();
                assert!(
                    !bytes
                        .windows(bearer.len())
                        .any(|window| window == bearer.as_bytes()),
                    "credential found in {:?}",
                    entry.path()
                );
            }
        }
        assert!(present > 0);
        assert!(std::fs::symlink_metadata(&path).unwrap().is_file());
        drop(bearer);
    }
    fn gcs_fixture_file(location: &str, suffix: &str) -> grv_adapter_api::VerifiedFile {
        grv_adapter_api::VerifiedFile {
            table: grv_types::Name::new("rows").unwrap(),
            partition: serde_json::json!({}),
            version: grv_types::U64::new(1).unwrap(),
            schema: grv_adapter_api::FileSchema {
                columns: vec![grv_types::Column {
                    name: "id".into(),
                    logical_type: serde_json::json!("int64"),
                }],
            },
            access: grv_adapter_api::FileAccess::GcsView,
            location: location.into(),
            // Actual fixture metadata supplied by the protected owned driver.
            // This below-SDK test routes prepared descriptors, not a substitute
            // for full SHA/size/schema/row preparation.
            size: grv_types::U64::new(
                std::env::var(format!("GRV_TEST_GCS_SIZE_{suffix}"))
                    .unwrap()
                    .parse()
                    .unwrap(),
            )
            .unwrap(),
            sha256: grv_types::Digest::new(
                std::env::var(format!("GRV_TEST_GCS_SHA256_{suffix}")).unwrap(),
            )
            .unwrap(),
            validator: std::env::var(format!("GRV_TEST_GCS_GENERATION_{suffix}")).unwrap(),
        }
    }
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
                scope: "s3://validation-s3-profile/grv-dev".into(),
                profile: "validation-s3-profile".into(),
                region: "eu-west-1".into(),
            })
            .unwrap();
        assert!(engine.metadata_query("SELECT * FROM read_parquet('s3://validation-s3-profile/grv-dev/foreign.parquet')").is_err());
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
        verified_descriptor_case(false);
    }
    #[test]
    fn gcs_metadata_verified_descriptor_scans_unlinked_files_without_path_lookup_and_rejects_widening()
     {
        verified_descriptor_case(true);
    }
    fn verified_descriptor_case(isolated: bool) {
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
        if isolated {
            engine.begin_gcs_execution().unwrap();
        }
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
