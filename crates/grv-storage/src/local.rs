//! Durable local mapping of the six GRV backend operations.
use crate::{
    Backend, Error, ErrorKind, ListEntry, ListMode, ObjectKey, ObjectMeta, ObjectPrefix, Result,
    Validator, WriteEffect, model::Counter,
};
use sha2::{Digest, Sha256};
use std::{
    ffi::CString,
    fs::File,
    io::{Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultPoint {
    BeforeTempSync,
    BeforeInstall,
    AfterInstall,
    BeforePathSync,
    BeforeReadSync,
}
pub trait FaultInjector: Send + Sync {
    fn check(&self, point: FaultPoint) -> std::io::Result<()>;
}
struct NoFault;
impl FaultInjector for NoFault {
    fn check(&self, _: FaultPoint) -> std::io::Result<()> {
        Ok(())
    }
}
pub struct LocalBackend {
    root: PathBuf,
    root_fd: File,
    faults: Arc<dyn FaultInjector>,
}
impl LocalBackend {
    /// Opening a read/write backend never initializes a root or creates paths.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = std::fs::canonicalize(root).map_err(no_effect)?;
        let root_fd = open_directory(&root).map_err(no_effect)?;
        require_local_filesystem(&root_fd)?;
        let backend = Self {
            root,
            root_fd,
            faults: Arc::new(NoFault),
        };
        backend.sync_root_path()?;
        Ok(backend)
    }
    /// Explicit initialization may create the root; grv.json remains core-owned.
    pub fn create(root: impl AsRef<Path>) -> Result<Self> {
        std::fs::create_dir_all(root.as_ref()).map_err(no_effect)?;
        Self::open(root)
    }
    pub fn with_faults(mut self, faults: Arc<dyn FaultInjector>) -> Self {
        self.faults = faults;
        self
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    fn sync_root_path(&self) -> Result<()> {
        let current = std::fs::symlink_metadata(&self.root).map_err(no_effect)?;
        let opened = self.root_fd.metadata().map_err(no_effect)?;
        if !current.is_dir() || current.dev() != opened.dev() || current.ino() != opened.ino() {
            return Err(Error::new(
                ErrorKind::Integrity,
                "local root identity changed",
            ));
        }
        for path in self.root.ancestors() {
            open_directory(path)
                .and_then(|f| f.sync_all())
                .map_err(no_effect)?;
        }
        Ok(())
    }
    fn parent(&self, key: &ObjectKey, create: bool) -> Result<(Vec<File>, CString)> {
        self.sync_root_path()?;
        let mut chain = vec![self.root_fd.try_clone().map_err(no_effect)?];
        let mut parts = key.as_str().split('/').peekable();
        let mut leaf = None;
        while let Some(part) = parts.next() {
            let name = CString::new(part)
                .map_err(|_| Error::new(ErrorKind::InvalidKey, "NUL object key"))?;
            if parts.peek().is_none() {
                leaf = Some(name);
                break;
            }
            let fd = chain.last().unwrap().as_raw_fd();
            let mut opened = openat_directory(fd, &name);
            if create
                && opened
                    .as_ref()
                    .err()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound)
            {
                let result = unsafe { libc::mkdirat(fd, name.as_ptr(), 0o700) };
                if result < 0 {
                    let error = std::io::Error::last_os_error();
                    if error.kind() != std::io::ErrorKind::AlreadyExists {
                        return Err(no_effect(error));
                    }
                }
                opened = openat_directory(fd, &name);
            }
            chain.push(opened.map_err(path_error)?);
        }
        Ok((chain, leaf.unwrap()))
    }
    fn barrier(&self, file: &File, chain: &[File], read: bool) -> Result<()> {
        self.faults
            .check(if read {
                FaultPoint::BeforeReadSync
            } else {
                FaultPoint::BeforePathSync
            })
            .map_err(no_effect)?;
        file.sync_all().map_err(no_effect)?;
        for dir in chain.iter().rev() {
            dir.sync_all().map_err(no_effect)?;
        }
        self.sync_root_path()
    }
    fn read_open(&self, parent: &File, name: &CString) -> Result<File> {
        let fd = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            )
        };
        let file = owned_fd(fd).map_err(path_error)?;
        if !file.metadata().map_err(no_effect)?.is_file() {
            return Err(Error::new(
                ErrorKind::Integrity,
                "object is not a regular file",
            ));
        }
        Ok(file)
    }
    fn read(&self, key: &ObjectKey, mut sink: Option<&mut dyn Write>) -> Result<ObjectMeta> {
        let (chain, name) = self.parent(key, false)?;
        let mut file = self.read_open(chain.last().unwrap(), &name)?;
        let mut sha = Sha256::new();
        let mut size = 0u64;
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let n = read_retry(&mut file, &mut buffer).map_err(no_effect)?;
            if n == 0 {
                break;
            }
            size = size
                .checked_add(n as u64)
                .ok_or_else(|| Error::new(ErrorKind::Integrity, "object size overflows"))?;
            sha.update(&buffer[..n]);
            if let Some(output) = sink.as_mut() {
                output.write_all(&buffer[..n]).map_err(no_effect)?;
            }
        }
        self.barrier(&file, &chain, true)?;
        Ok(ObjectMeta {
            validator: Validator::new(format!("{:x}", sha.finalize()))?,
            size: Counter::new(size)?,
        })
    }
    fn write(
        &self,
        key: &ObjectKey,
        expected: Option<&Validator>,
        source: &mut dyn Read,
    ) -> Result<Validator> {
        let (chain, name) = self.parent(key, expected.is_none()).map_err(|mut e| {
            if expected.is_some() && e.kind == ErrorKind::NotFound {
                e.kind = ErrorKind::PreconditionFailed;
            }
            e
        })?;
        let parent = chain.last().unwrap();
        let lock = if expected.is_some() {
            Some(DirectoryLock::lock(parent)?)
        } else {
            None
        };
        if let Some(expected) = expected {
            let mut current = self.read_open(parent, &name).map_err(|mut e| {
                if e.kind == ErrorKind::NotFound {
                    e.kind = ErrorKind::PreconditionFailed;
                }
                e
            })?;
            let mut sha = Sha256::new();
            let mut buffer = [0u8; 64 * 1024];
            loop {
                let n = read_retry(&mut current, &mut buffer).map_err(no_effect)?;
                if n == 0 {
                    break;
                }
                sha.update(&buffer[..n]);
            }
            if format!("{:x}", sha.finalize()) != expected.as_str() {
                return Err(Error::new(
                    ErrorKind::PreconditionFailed,
                    "object validator changed",
                ));
            }
        }
        let temp_name = CString::new(format!(".tmp-{}", grv_types::Uuid::v4())).unwrap();
        let fd = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                temp_name.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0o600,
            )
        };
        let mut temp = TempFile {
            file: owned_fd(fd).map_err(no_effect)?,
            parent: parent.try_clone().map_err(no_effect)?,
            name: temp_name,
        };
        let mut sha = Sha256::new();
        let mut count = 0u64;
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let n = read_retry(source, &mut buffer).map_err(no_effect)?;
            if n == 0 {
                break;
            }
            count = count
                .checked_add(n as u64)
                .filter(|v| *v <= i64::MAX as u64)
                .ok_or_else(|| {
                    Error::new(ErrorKind::Integrity, "object exceeds int64 byte size")
                })?;
            temp.file.write_all(&buffer[..n]).map_err(no_effect)?;
            sha.update(&buffer[..n]);
        }
        self.faults
            .check(FaultPoint::BeforeTempSync)
            .map_err(no_effect)?;
        temp.file.sync_all().map_err(no_effect)?;
        self.faults
            .check(FaultPoint::BeforeInstall)
            .map_err(no_effect)?;
        let installed = unsafe {
            if expected.is_some() {
                libc::renameat(
                    parent.as_raw_fd(),
                    temp.name.as_ptr(),
                    parent.as_raw_fd(),
                    name.as_ptr(),
                )
            } else {
                libc::linkat(
                    parent.as_raw_fd(),
                    temp.name.as_ptr(),
                    parent.as_raw_fd(),
                    name.as_ptr(),
                    0,
                )
            }
        };
        if installed < 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                return Err(Error::new(
                    ErrorKind::PreconditionFailed,
                    "object already exists",
                ));
            }
            return Err(no_effect(e));
        }
        self.faults
            .check(FaultPoint::AfterInstall)
            .map_err(|e| no_effect(e).applied())?;
        if expected.is_none() {
            let result = unsafe { libc::unlinkat(parent.as_raw_fd(), temp.name.as_ptr(), 0) };
            if result < 0 {
                return Err(no_effect(std::io::Error::last_os_error()).applied());
            }
        }
        self.barrier(&temp.file, &chain, false)
            .map_err(Error::applied)?;
        drop(lock);
        Validator::new(format!("{:x}", sha.finalize()))
    }
    fn listing(&self, prefix: &ObjectPrefix, mode: ListMode) -> Result<Vec<ListEntry>> {
        // Traverse the same no-follow directory descriptors used by get/put.
        let artificial = ObjectKey::new(format!("{}__listing_leaf__", prefix.as_str()))?;
        let (chain, _) = match self.parent(&artificial, false) {
            Ok(v) => v,
            Err(e) if e.kind == ErrorKind::NotFound => return Ok(vec![]),
            Err(e) => return Err(e),
        };
        let mut entries = vec![];
        walk(chain.last().unwrap(), prefix.as_str(), mode, &mut entries)?;
        entries.sort_by(|a, b| entry_name(a).cmp(entry_name(b)));
        Ok(entries)
    }
}
impl Backend for LocalBackend {
    fn get(&self, key: &ObjectKey, sink: &mut dyn Write) -> Result<ObjectMeta> {
        self.read(key, Some(sink))
    }
    fn head(&self, key: &ObjectKey) -> Result<ObjectMeta> {
        self.read(key, None)
    }
    fn list(&self, prefix: &ObjectPrefix, mode: ListMode) -> Result<Vec<ListEntry>> {
        self.listing(prefix, mode)
    }
    fn delete(&self, key: &ObjectKey) -> Result<()> {
        let (chain, name) = match self.parent(key, false) {
            Ok(v) => v,
            Err(e) if e.kind == ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        let parent = chain.last().unwrap();
        let _lock = DirectoryLock::lock(parent)?;
        match self.read_open(parent, &name) {
            Ok(_) => {}
            Err(e) if e.kind == ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        }
        let result = unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), 0) };
        if result < 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(no_effect(e).applied());
            }
        }
        for dir in chain.iter().rev() {
            dir.sync_all().map_err(|e| no_effect(e).applied())?;
        }
        self.sync_root_path().map_err(Error::applied)
    }
    fn conditional_create(&self, key: &ObjectKey, source: &mut dyn Read) -> Result<Validator> {
        self.write(key, None, source)
    }
    fn conditional_put(
        &self,
        key: &ObjectKey,
        expected: &Validator,
        source: &mut dyn Read,
    ) -> Result<Validator> {
        self.write(key, Some(expected), source)
    }
}
fn no_effect(e: std::io::Error) -> Error {
    if matches!(
        e.raw_os_error(),
        Some(libc::ENOSYS) | Some(libc::EOPNOTSUPP)
    ) {
        return Error::new(
            ErrorKind::Unsupported,
            "filesystem lacks required local storage operations",
        );
    }
    Error::io(e, WriteEffect::NoEffect)
}
fn require_local_filesystem(root: &File) -> Result<()> {
    let mut info = std::mem::MaybeUninit::<libc::statfs>::uninit();
    if unsafe { libc::fstatfs(root.as_raw_fd(), info.as_mut_ptr()) } < 0 {
        return Err(no_effect(std::io::Error::last_os_error()));
    }
    let info = unsafe { info.assume_init() };
    #[cfg(target_os = "linux")]
    let network = matches!(
        info.f_type as u64,
        0x6969 | 0x517b | 0xff534d42 | 0xfe534d42 | 0x5346414f | 0x73757245
    );
    #[cfg(target_os = "macos")]
    let network = matches!(
        unsafe { std::ffi::CStr::from_ptr(info.f_fstypename.as_ptr()) }.to_bytes(),
        b"nfs" | b"smbfs" | b"webdav" | b"afpfs"
    );
    if network {
        return Err(Error::new(
            ErrorKind::Unsupported,
            "network filesystem is unsupported for local coordination",
        ));
    }
    Ok(())
}
fn path_error(e: std::io::Error) -> Error {
    if matches!(e.raw_os_error(), Some(libc::ELOOP) | Some(libc::ENOTDIR)) {
        Error::new(
            ErrorKind::Integrity,
            "object path contains a symlink or non-directory",
        )
    } else {
        no_effect(e)
    }
}
fn owned_fd(fd: i32) -> std::io::Result<File> {
    if fd < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}
fn open_directory(path: &Path) -> std::io::Result<File> {
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)
}
fn openat_directory(fd: i32, name: &CString) -> std::io::Result<File> {
    owned_fd(unsafe {
        libc::openat(
            fd,
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    })
}
fn read_retry(reader: &mut dyn Read, buf: &mut [u8]) -> std::io::Result<usize> {
    loop {
        match reader.read(buf) {
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            result => return result,
        }
    }
}
struct DirectoryLock {
    file: File,
}
impl DirectoryLock {
    fn lock(file: &File) -> Result<Self> {
        // flock locks belong to an open-file description. Cloning the root
        // descriptor would make two calls on the same Backend share one lock.
        let dot = CString::new(".").unwrap();
        let file = openat_directory(file.as_raw_fd(), &dot).map_err(no_effect)?;
        loop {
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } == 0 {
                break;
            }
            let e = std::io::Error::last_os_error();
            if e.kind() != std::io::ErrorKind::Interrupted {
                return Err(no_effect(e));
            }
        }
        Ok(Self { file })
    }
}
impl Drop for DirectoryLock {
    fn drop(&mut self) {
        unsafe {
            libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}
struct TempFile {
    file: File,
    parent: File,
    name: CString,
}
impl Drop for TempFile {
    fn drop(&mut self) {
        unsafe {
            libc::unlinkat(self.parent.as_raw_fd(), self.name.as_ptr(), 0);
        }
    }
}
fn entry_name(entry: &ListEntry) -> &str {
    match entry {
        ListEntry::Object(v) => v.as_str(),
        ListEntry::Prefix(v) => v.as_str(),
    }
}
fn walk(directory: &File, prefix: &str, mode: ListMode, out: &mut Vec<ListEntry>) -> Result<()> {
    // fdopendir consumes its descriptor; duplicate it so the caller retains its
    // directory and synchronization lifetime.
    let dup = unsafe { libc::fcntl(directory.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
    if dup < 0 {
        return Err(no_effect(std::io::Error::last_os_error()));
    }
    let raw = unsafe { libc::fdopendir(dup) };
    if raw.is_null() {
        unsafe {
            libc::close(dup);
        }
        return Err(no_effect(std::io::Error::last_os_error()));
    }
    struct Dir(*mut libc::DIR);
    impl Drop for Dir {
        fn drop(&mut self) {
            unsafe {
                libc::closedir(self.0);
            }
        }
    }
    let dir = Dir(raw);
    loop {
        set_errno(0);
        let entry = unsafe { libc::readdir(dir.0) };
        if entry.is_null() {
            let e = std::io::Error::last_os_error();
            if e.raw_os_error() != Some(0) {
                return Err(no_effect(e));
            }
            break;
        }
        let name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) }
            .to_str()
            .map_err(|_| Error::new(ErrorKind::Integrity, "non-UTF8 object name"))?;
        if name == "." || name == ".." || name.starts_with(".tmp-") {
            continue;
        }
        let component = CString::new(name).unwrap();
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        if unsafe {
            libc::fstatat(
                directory.as_raw_fd(),
                component.as_ptr(),
                stat.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } < 0
        {
            let e = std::io::Error::last_os_error();
            if e.kind() == std::io::ErrorKind::NotFound {
                continue;
            }
            return Err(no_effect(e));
        }
        let stat = unsafe { stat.assume_init() };
        let key = format!("{prefix}{name}");
        match stat.st_mode & libc::S_IFMT {
            libc::S_IFDIR => {
                let child_prefix = format!("{key}/");
                if mode == ListMode::Children {
                    out.push(ListEntry::Prefix(ObjectPrefix::new(child_prefix)?));
                } else {
                    let child =
                        openat_directory(directory.as_raw_fd(), &component).map_err(path_error)?;
                    walk(&child, &child_prefix, mode, out)?;
                }
            }
            libc::S_IFREG => out.push(ListEntry::Object(ObjectKey::new(key)?)),
            _ => {
                return Err(Error::new(
                    ErrorKind::Integrity,
                    "listing encountered non-regular object",
                ));
            }
        }
    }
    Ok(())
}
#[cfg(target_os = "linux")]
fn set_errno(value: i32) {
    unsafe {
        *libc::__errno_location() = value;
    }
}
#[cfg(target_os = "macos")]
fn set_errno(value: i32) {
    unsafe {
        *libc::__error() = value;
    }
}
