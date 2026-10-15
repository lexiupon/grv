//! Private, deadline-fenced GCS connection owner. No native call runs in a parent thread.
use super::*;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    mem::ManuallyDrop,
    os::unix::{
        fs::{FileExt, MetadataExt, OpenOptionsExt},
        io::{AsRawFd, FromRawFd, OwnedFd, RawFd},
        net::UnixStream,
        process::CommandExt,
    },
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{
        Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use zeroize::{Zeroize, Zeroizing};

// Commands use bounded JSON; ordinary2MiB SQL plus4096 location envelopes
// fit, but escape-expanded commands may be refused before any native effect.
// Responses have their own raw2MiB cap and never use this allocation.
const FRAME_LIMIT: usize = 2 * 1024 * 1024 + 64 * 1024;
// Responses carry native metadata bytes directly, with one additional tag byte.
const BYTE_LIMIT: usize = METADATA_BYTES;
const SLICE: Duration = Duration::from_millis(20);
const TERM_GRACE: Duration = Duration::from_millis(40);
const REAP_GRACE: Duration = Duration::from_millis(50);
const ARG_LIMIT: usize = 2 * 1024 * 1024;
const LOCATION_LIMIT: usize = 4096;

fn unresolved() -> io::Error {
    io::Error::other("GCS owner outcome unresolved")
}
fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "invalid GCS owner input")
}
fn expired() -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        "GCS owner deadline or cancellation",
    )
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub(super) enum Request {
    Metadata {
        query: String,
        private: bool,
    },
    Remote {
        #[serde(deserialize_with = "deserialize_locations")]
        locations: Vec<String>,
        query: String,
    },
    Relation {
        schema: String,
        table: String,
        kind: RelationKind,
    },
    SchemaExists {
        schema: String,
    },
    AuthorizeRelation {
        schema: String,
        table: String,
    },
    ManagedInitialized,
    StagePull {
        query: String,
        stage: String,
        expected: String,
    },
    StageBuild {
        query: String,
        projection: String,
        stage: String,
        expected: String,
        allowance: usize,
    },
    Close,
}
fn deserialize_locations<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<String>, D::Error> {
    struct Locations;
    impl<'de> serde::de::Visitor<'de> for Locations {
        type Value = Vec<String>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("bounded exact locations")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> Result<Self::Value, A::Error> {
            // Empty strings still carry Vec/String overhead. Enforce count
            // while decoding, not after unbounded Vec deserialization.
            let mut values = Vec::new();
            let mut total = 0usize;
            while let Some(value) = sequence.next_element::<String>()? {
                total = total
                    .checked_add(value.len())
                    .filter(|n| *n <= ARG_LIMIT)
                    .ok_or_else(|| serde::de::Error::custom("invalid GCS owner input"))?;
                if values.len() >= LOCATION_LIMIT {
                    return Err(serde::de::Error::custom("invalid GCS owner input"));
                }
                values.push(value);
            }
            Ok(values)
        }
    }
    deserializer.deserialize_seq(Locations)
}
pub(super) fn bounded_arguments<'a>(values: impl IntoIterator<Item = &'a str>) -> io::Result<()> {
    let total = values.into_iter().try_fold(0usize, |sum, value| {
        if value.as_bytes().contains(&0) {
            return None;
        }
        sum.checked_add(value.len()).filter(|n| *n <= ARG_LIMIT)
    });
    total.map(|_| ()).ok_or_else(invalid)
}
impl Request {
    fn validate_arguments(&self) -> io::Result<()> {
        match self {
            Self::Metadata { query, .. } => bounded_arguments([query.as_str()]),
            Self::Remote { locations, query } => {
                if locations.is_empty() || locations.len() > LOCATION_LIMIT {
                    return Err(invalid());
                }
                bounded_arguments(
                    std::iter::once(query.as_str()).chain(locations.iter().map(String::as_str)),
                )
            }
            Self::Relation { schema, table, .. } | Self::AuthorizeRelation { schema, table } => {
                bounded_arguments([schema.as_str(), table.as_str()])
            }
            Self::SchemaExists { schema } => bounded_arguments([schema.as_str()]),
            Self::StagePull {
                query,
                stage,
                expected,
            } => bounded_arguments([query.as_str(), stage.as_str(), expected.as_str()]),
            Self::StageBuild {
                query,
                projection,
                stage,
                expected,
                allowance,
            } => {
                if *allowance == 0 || *allowance > METADATA_BYTES {
                    return Err(invalid());
                }
                bounded_arguments([
                    query.as_str(),
                    projection.as_str(),
                    stage.as_str(),
                    expected.as_str(),
                ])
            }
            Self::ManagedInitialized | Self::Close => Ok(()),
        }
    }
}
#[derive(Serialize, Deserialize)]
pub(super) enum RelationKind {
    Exists,
    View,
    Base,
    Schema,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "value", deny_unknown_fields)]
pub(super) enum Reply {
    Bytes(Vec<u8>),
    Bool(bool),
    Done,
}

// Wrapping the token itself also wipes it on a partially decoded Configure error.
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
struct Secret(String);
impl Drop for Secret {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Open {
    // Closed shape discriminant, not negotiated or dual-decoded. An older
    // JSON-response helper refuses this field before opening the database.
    response_encoding: String,
    path: String,
    identity: Option<DatabaseIdentity>,
    deadline_nanos: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Configure {
    httpfs: String,
    scope: String,
    token: Secret,
    project: String,
    // CLOCK_MONOTONIC is common to the parent and the freshly exec'd child.
    deadline_nanos: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "value", deny_unknown_fields)]
enum Message {
    Open(Open),
    Configure(Configure),
    Call(Request),
    VerifiedDescriptor {
        query: String,
        parent_descriptor: i32,
    },
}
enum Response {
    Ok(Reply),
    Failed,
}

fn monotonic_nanos() -> io::Result<u64> {
    let mut ts = std::mem::MaybeUninit::<libc::timespec>::uninit();
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, ts.as_mut_ptr()) } != 0 {
        return Err(unresolved());
    }
    let ts = unsafe { ts.assume_init() };
    u64::try_from(ts.tv_sec)
        .ok()
        .and_then(|s| s.checked_mul(1_000_000_000))
        .and_then(|s| s.checked_add(ts.tv_nsec as u64))
        .ok_or_else(invalid)
}

struct CappedBuffer(Zeroizing<Vec<u8>>);
impl Write for CappedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > FRAME_LIMIT.saturating_sub(self.0.len()) {
            return Err(invalid());
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn ready(fd: RawFd, events: i16, deadline: Instant, cancelled: &AtomicBool) -> io::Result<()> {
    loop {
        if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
            return Err(expired());
        }
        let left = deadline
            .saturating_duration_since(Instant::now())
            .min(SLICE);
        let millis = left.as_millis().max(1) as i32;
        let mut pfd = libc::pollfd {
            fd,
            events,
            revents: 0,
        };
        let rc = unsafe { libc::poll(&mut pfd, 1, millis) };
        if rc < 0 {
            if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(unresolved());
        }
        if rc > 0 {
            if pfd.revents & (libc::POLLERR | libc::POLLNVAL) != 0 {
                return Err(unresolved());
            }
            // HUP still permits reading the last buffered response.
            if pfd.revents & (events | libc::POLLHUP) != 0 {
                return Ok(());
            }
        }
    }
}
fn transfer(
    stream: &mut UnixStream,
    mut bytes: &mut [u8],
    writing: bool,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> io::Result<()> {
    while !bytes.is_empty() {
        ready(
            stream.as_raw_fd(),
            if writing { libc::POLLOUT } else { libc::POLLIN },
            deadline,
            cancelled,
        )?;
        if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
            return Err(expired());
        }
        // std's UnixStream write uses MSG_NOSIGNAL on Linux; SO_NOSIGPIPE is set below on Darwin.
        let result = if writing {
            stream.write(bytes)
        } else {
            stream.read(bytes)
        };
        match result {
            Ok(0) => return Err(unresolved()),
            Ok(n) => bytes = &mut bytes[n..],
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
                ) => {}
            Err(_) => return Err(unresolved()),
        }
    }
    Ok(())
}
// Borrow response payloads: no JSON staging buffer or mutable byte copy.
fn write_response_bytes(
    stream: &mut UnixStream,
    mut bytes: &[u8],
    deadline: Instant,
    cancelled: &AtomicBool,
) -> io::Result<()> {
    while !bytes.is_empty() {
        ready(stream.as_raw_fd(), libc::POLLOUT, deadline, cancelled)?;
        if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
            return Err(expired());
        }
        // UnixStream suppresses SIGPIPE as in transfer / socket_setup.
        match stream.write(bytes) {
            Ok(0) => return Err(unresolved()),
            Ok(n) => bytes = &bytes[n..],
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
                ) => {}
            Err(_) => return Err(unresolved()),
        }
    }
    Ok(())
}
fn send_response(
    stream: &mut UnixStream,
    response: &Response,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> io::Result<()> {
    let (tag, payload): (u8, &[u8]) = match response {
        Response::Failed => (0, &[]),
        Response::Ok(Reply::Done) => (1, &[]),
        Response::Ok(Reply::Bool(false)) => (2, &[]),
        Response::Ok(Reply::Bool(true)) => (3, &[]),
        Response::Ok(Reply::Bytes(bytes)) => (4, bytes),
    };
    if payload.len() > BYTE_LIMIT {
        return Err(invalid());
    }
    let header = ((payload.len() + 1) as u32).to_be_bytes();
    write_response_bytes(stream, &header, deadline, cancelled)?;
    write_response_bytes(stream, &[tag], deadline, cancelled)?;
    write_response_bytes(stream, payload, deadline, cancelled)
}
fn receive_response(
    stream: &mut UnixStream,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> io::Result<Response> {
    let mut header = [0; 4];
    transfer(stream, &mut header, false, deadline, cancelled)?;
    let count = u32::from_be_bytes(header) as usize;
    if count == 0 || count > BYTE_LIMIT + 1 {
        return Err(invalid());
    }
    let mut tag = [0];
    transfer(stream, &mut tag, false, deadline, cancelled)?;
    match tag[0] {
        0 if count == 1 => Ok(Response::Failed),
        1 if count == 1 => Ok(Response::Ok(Reply::Done)),
        2 if count == 1 => Ok(Response::Ok(Reply::Bool(false))),
        3 if count == 1 => Ok(Response::Ok(Reply::Bool(true))),
        4 => {
            // One exact-size allocation becomes the returned payload directly.
            let mut bytes = vec![0; count - 1];
            transfer(stream, &mut bytes, false, deadline, cancelled)?;
            Ok(Response::Ok(Reply::Bytes(bytes)))
        }
        _ => Err(invalid()),
    }
}
fn socket_setup(stream: &UnixStream) -> io::Result<()> {
    stream.set_nonblocking(true).map_err(|_| unresolved())?;
    #[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
    {
        let one: libc::c_int = 1;
        if unsafe {
            libc::setsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_NOSIGPIPE,
                (&one as *const libc::c_int).cast(),
                std::mem::size_of_val(&one) as libc::socklen_t,
            )
        } != 0
        {
            return Err(unresolved());
        }
    }
    Ok(())
}
fn send<T: Serialize>(
    stream: &mut UnixStream,
    value: &T,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> io::Result<()> {
    // Fixed capacity avoids reallocations leaving unwiped token copies behind.
    let mut buffer = CappedBuffer(Zeroizing::new(Vec::with_capacity(FRAME_LIMIT)));
    serde_json::to_writer(&mut buffer, value).map_err(|_| invalid())?;
    let mut header = (buffer.0.len() as u32).to_be_bytes();
    transfer(stream, &mut header, true, deadline, cancelled)?;
    transfer(stream, &mut buffer.0, true, deadline, cancelled)
}
// Serde's internally/adjacently tagged enums may buffer Content BEFORE a
// field visitor enforces location count. Preflight with allocation-free
// structural visitors first, so tiny array elements cannot amplify a frame
// into an unbounded Content vector. Escaped-string scratch is frame-bounded.
fn preflight_json(bytes: &[u8]) -> io::Result<()> {
    use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
    struct Budget {
        nodes: usize,
        strings: usize,
    }
    struct Seed<'a> {
        budget: &'a mut Budget,
        depth: usize,
    }
    impl<'de> DeserializeSeed<'de> for Seed<'_> {
        type Value = ();
        fn deserialize<D: serde::Deserializer<'de>>(self, de: D) -> Result<(), D::Error> {
            self.budget.nodes += 1;
            if self.depth > 12 || self.budget.nodes > 16384 {
                return Err(serde::de::Error::custom("invalid GCS owner input"));
            }
            de.deserialize_any(self)
        }
    }
    impl<'de> Visitor<'de> for Seed<'_> {
        type Value = ();
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("bounded private JSON")
        }
        fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<(), E> {
            self.budget.strings = self
                .budget
                .strings
                .checked_add(s.len())
                .filter(|n| *n <= ARG_LIMIT + 64 * 1024)
                .ok_or_else(|| E::custom("invalid GCS owner input"))?;
            Ok(())
        }
        fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<(), E> {
            Ok(())
        }
        fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<(), E> {
            Ok(())
        }
        fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<(), E> {
            Ok(())
        }
        fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<(), E> {
            Ok(())
        }
        fn visit_unit<E: serde::de::Error>(self) -> Result<(), E> {
            Ok(())
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
            let mut count = 0;
            while seq
                .next_element_seed(Seed {
                    budget: self.budget,
                    depth: self.depth + 1,
                })?
                .is_some()
            {
                count += 1;
                if count > LOCATION_LIMIT {
                    return Err(serde::de::Error::custom("invalid GCS owner input"));
                }
            }
            Ok(())
        }
        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
            let mut count = 0;
            while map
                .next_key_seed(Seed {
                    budget: self.budget,
                    depth: self.depth + 1,
                })?
                .is_some()
            {
                count += 1;
                if count > 32 {
                    return Err(serde::de::Error::custom("invalid GCS owner input"));
                }
                map.next_value_seed(Seed {
                    budget: self.budget,
                    depth: self.depth + 1,
                })?;
            }
            Ok(())
        }
    }
    let mut de = serde_json::Deserializer::from_slice(bytes);
    Seed {
        budget: &mut Budget {
            nodes: 0,
            strings: 0,
        },
        depth: 0,
    }
    .deserialize(&mut de)
    .map_err(|_| invalid())?;
    de.end().map_err(|_| invalid())
}
fn receive<T: serde::de::DeserializeOwned>(
    stream: &mut UnixStream,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> io::Result<T> {
    let mut header = [0; 4];
    transfer(stream, &mut header, false, deadline, cancelled)?;
    let count = u32::from_be_bytes(header) as usize;
    if count == 0 || count > FRAME_LIMIT {
        return Err(invalid());
    }
    let mut buffer = Zeroizing::new(vec![0; count]);
    transfer(stream, &mut buffer, false, deadline, cancelled)?;
    preflight_json(&buffer)?;
    serde_json::from_slice(&buffer).map_err(|_| invalid())
}

// A separate one-byte sendmsg follows the complete typed frame. Frame reads
// deliberately remain ordinary reads and cannot consume this ancillary message.
const DESCRIPTOR_MARKER: u8 = 0xd7;
// Darwin's bsd/kern/uipc_usrreq.c bounds a rights message with
// UIPC_MAX_CMSG_FD = 512; Linux SCM_MAX_FD is 253. Darwin can install rights
// BEFORE userspace control truncation, so a tiny receive buffer leaks hidden
// descriptors. Reserve enough aligned storage even for 512 separate headers,
// adopt all delivered rights, and still accept exactly one. IPC frame/query
// budgets are unchanged. Never rely on MSG_CTRUNC closing unreported rights.
const CONTROL_WORDS: usize = 2048;
type DescriptorControl = [usize; CONTROL_WORDS];

fn send_descriptor(
    stream: &UnixStream,
    fd: RawFd,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> io::Result<()> {
    let mut marker = DESCRIPTOR_MARKER;
    let mut iov = libc::iovec {
        iov_base: (&mut marker as *mut u8).cast(),
        iov_len: 1,
    };
    let mut control: DescriptorControl = [0; CONTROL_WORDS];
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_iov = &mut iov;
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen = unsafe { libc::CMSG_SPACE(std::mem::size_of::<RawFd>() as _) } as _;
    unsafe {
        let header = libc::CMSG_FIRSTHDR(&message);
        (*header).cmsg_level = libc::SOL_SOCKET;
        (*header).cmsg_type = libc::SCM_RIGHTS;
        (*header).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<RawFd>() as _) as _;
        std::ptr::write_unaligned(libc::CMSG_DATA(header).cast::<RawFd>(), fd);
    }
    #[cfg(target_os = "linux")]
    let flags = libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL;
    #[cfg(not(target_os = "linux"))]
    let flags = libc::MSG_DONTWAIT; // socket_setup installs SO_NOSIGPIPE on Darwin.
    loop {
        ready(stream.as_raw_fd(), libc::POLLOUT, deadline, cancelled)?;
        if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
            return Err(expired());
        }
        let count = unsafe { libc::sendmsg(stream.as_raw_fd(), &message, flags) };
        if count == 1 {
            return Ok(());
        }
        if count < 0
            && matches!(
                io::Error::last_os_error().kind(),
                io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
            )
        {
            continue;
        }
        return Err(unresolved());
    }
}

// Do not return early on a malformed envelope: first take RAII ownership of
// every complete SCM_RIGHTS integer delivered in the bounded control buffer.
fn adopt_descriptor(message: &libc::msghdr, marker: u8) -> io::Result<OwnedFd> {
    let capacity = std::mem::size_of::<DescriptorControl>();
    let count = (message.msg_controllen as usize).min(capacity);
    let header_size = unsafe { libc::CMSG_LEN(0) } as usize;
    let mut offset = 0;
    let mut headers = 0;
    let mut rights = Vec::new();
    let mut valid = message.msg_controllen as usize <= capacity
        && message.msg_flags & (libc::MSG_CTRUNC | libc::MSG_TRUNC) == 0
        && marker == DESCRIPTOR_MARKER;
    while count.saturating_sub(offset) >= header_size {
        let header = unsafe {
            std::ptr::read_unaligned(
                message
                    .msg_control
                    .cast::<u8>()
                    .add(offset)
                    .cast::<libc::cmsghdr>(),
            )
        };
        headers += 1;
        let length = header.cmsg_len as usize;
        let available = count - offset;
        let is_rights =
            header.cmsg_level == libc::SOL_SOCKET && header.cmsg_type == libc::SCM_RIGHTS;
        valid &= is_rights && length >= header_size && length <= available;
        if is_rights && length >= header_size {
            let payload = length.min(available) - header_size;
            valid &= payload.is_multiple_of(std::mem::size_of::<RawFd>());
            for index in 0..payload / std::mem::size_of::<RawFd>() {
                let fd = unsafe {
                    std::ptr::read_unaligned(
                        message
                            .msg_control
                            .cast::<u8>()
                            .add(offset + header_size + index * std::mem::size_of::<RawFd>())
                            .cast::<RawFd>(),
                    )
                };
                if fd < 0 {
                    valid = false;
                } else {
                    // The kernel supplied these descriptors, never the JSON integer.
                    let owned = unsafe { OwnedFd::from_raw_fd(fd) };
                    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
                    valid &= flags >= 0
                        && unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } >= 0;
                    rights.push(owned);
                }
            }
        }
        if length < header_size || length > available {
            valid = false;
            break;
        }
        let step = unsafe { libc::CMSG_SPACE((length - header_size) as _) } as usize;
        if step == 0 || step > available {
            break; // A final cmsg need not include trailing alignment padding.
        }
        offset += step;
    }
    if !valid || headers != 1 || rights.len() != 1 {
        return Err(invalid());
    }
    Ok(rights.pop().unwrap())
}

fn receive_descriptor(
    stream: &UnixStream,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> io::Result<OwnedFd> {
    loop {
        ready(stream.as_raw_fd(), libc::POLLIN, deadline, cancelled)?;
        if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
            return Err(expired());
        }
        let mut marker = 0;
        let mut iov = libc::iovec {
            iov_base: (&mut marker as *mut u8).cast(),
            iov_len: 1,
        };
        let mut control: DescriptorControl = [0; CONTROL_WORDS];
        let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
        message.msg_iov = &mut iov;
        message.msg_iovlen = 1;
        message.msg_control = control.as_mut_ptr().cast();
        message.msg_controllen = std::mem::size_of_val(&control) as _;
        #[cfg(target_os = "linux")]
        let flags = libc::MSG_DONTWAIT | libc::MSG_CMSG_CLOEXEC;
        #[cfg(not(target_os = "linux"))]
        let flags = libc::MSG_DONTWAIT;
        let count = unsafe { libc::recvmsg(stream.as_raw_fd(), &mut message, flags) };
        if count < 0 {
            if matches!(
                io::Error::last_os_error().kind(),
                io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
            ) {
                continue;
            }
            return Err(unresolved());
        }
        // Even a bad marker/EOF must dispose of any installed ancillary rights.
        let descriptor = adopt_descriptor(&message, marker)?;
        if count != 1 {
            return Err(unresolved());
        }
        return Ok(descriptor);
    }
}

fn verify_file_descriptor(fd: RawFd) -> io::Result<()> {
    let stat = descriptor(fd)?; // Explicit CLOEXEC on both macOS and Linux.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if stat.st_mode & libc::S_IFMT != libc::S_IFREG
        || flags < 0
        || flags & libc::O_ACCMODE != libc::O_RDONLY
        || unsafe { libc::lseek(fd, 0, libc::SEEK_CUR) } < 0
    {
        return Err(invalid());
    }
    // No pathname lookup, ownership/link-count restriction, or reopen. An
    // unlinked verified file (nlink == 0) is deliberately still usable.
    Ok(())
}

// If any fence step fails, this fd intentionally never closes. This includes
// unwinding or a supervisor panic: releasing ownership must require positive proof.
struct LockFence {
    file: ManuallyDrop<File>,
    proved: AtomicBool,
}
impl Drop for LockFence {
    fn drop(&mut self) {
        if self.proved.load(Ordering::Acquire) {
            unsafe { ManuallyDrop::drop(&mut self.file) };
        }
    }
}
struct State {
    stop: bool,
    done: Option<bool>,
    deadline: Option<Instant>,
}
struct Supervisor {
    state: Mutex<State>,
    wake: Condvar,
    cancelled: Arc<AtomicBool>,
    lock: LockFence,
}
impl Supervisor {
    fn stop(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.state.lock().unwrap_or_else(|e| e.into_inner()).stop = true;
        self.wake.notify_all();
    }
}

/// Child remains unreaped (WNOWAIT) until every process-group signal has been
/// issued. In particular, the numeric PID cannot be recycled between TERM/KILL.
fn exited(pid: libc::pid_t) -> io::Result<bool> {
    let mut info = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
    let rc = unsafe {
        libc::waitid(
            libc::P_PID,
            pid as libc::id_t,
            info.as_mut_ptr(),
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    if rc != 0 {
        if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
            return Ok(false);
        }
        return Err(unresolved());
    }
    let actual = unsafe { info.assume_init().si_pid() };
    Ok(actual == pid)
}
// Darwin killpg returns EPERM, rather than ESRCH, for a zombie-only empty
// group. Use a kernel group-membership query, not EPERM itself, as evidence.
#[cfg(target_os = "macos")]
fn empty_group(pid: libc::pid_t) -> io::Result<bool> {
    // The public libproc API reports group PIDs without depending on private
    // kinfo_proc ABI offsets. The known unreaped leader may appear as a zombie;
    // exited() below proves it cannot execute. Any other member forbids proof.
    let mut members = vec![-1 as libc::pid_t; 16384];
    let capacity = std::mem::size_of_val(members.as_slice()) as libc::c_int;
    let count = unsafe { libc::proc_listpgrppids(pid, members.as_mut_ptr().cast(), capacity) };
    // proc_listpgrppids (unlike proc_listpids) returns a PID count, not bytes.
    if count < 0 || count as usize >= members.len() {
        return Err(unresolved());
    }
    // A zero return may also mean libproc could not enumerate. Do not use
    // ambiguous absence as proof; the known unreaped leader must be present.
    Ok(count > 0
        && members[..count as usize]
            .iter()
            .all(|&member| member == pid))
}
#[cfg(not(target_os = "macos"))]
fn empty_group(_: libc::pid_t) -> io::Result<bool> {
    Ok(false)
}
fn signal_group(pid: libc::pid_t, signal: i32) -> io::Result<()> {
    if pid <= 1 {
        return Err(unresolved());
    }
    let group = unsafe { libc::getpgid(pid) };
    // Darwin removes an exited leader from its group even while WNOWAIT
    // preserves its PID. Its descendants may still be group members.
    if group != pid {
        let missing = group == -1 && io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH);
        if !missing || !exited(pid)? {
            return Err(unresolved());
        }
    }
    if unsafe { libc::kill(-pid, signal) } != 0 {
        let errno = io::Error::last_os_error().raw_os_error();
        let missing =
            errno == Some(libc::ESRCH) || (errno == Some(libc::EPERM) && empty_group(pid)?);
        // An empty group is already fenced, but only while the unreaped child
        // proves the group number cannot refer to a recycled leader.
        if !missing || !exited(pid)? {
            return Err(unresolved());
        }
    }
    Ok(())
}
fn fence_child(child: &mut Child) -> io::Result<()> {
    let pid = libc::pid_t::try_from(child.id()).map_err(|_| unresolved())?;
    // Validate that this is still our unreaped child before signalling its group.
    exited(pid)?;
    signal_group(pid, libc::SIGTERM)?;
    // Do not reap even if TERM exits immediately: descendants may still be active.
    let end = Instant::now() + TERM_GRACE;
    while Instant::now() < end {
        thread::sleep(SLICE.min(end.saturating_duration_since(Instant::now())));
    }
    signal_group(pid, libc::SIGKILL)?;
    let end = Instant::now() + REAP_GRACE;
    loop {
        if exited(pid)? {
            break;
        }
        if Instant::now() >= end {
            return Err(unresolved());
        }
        thread::sleep(Duration::from_millis(2));
    }
    // WNOWAIT proved wait cannot block. No more signals may follow this reap.
    child.wait().map_err(|_| unresolved())?;
    let rc = unsafe { libc::kill(-pid, 0) };
    if rc == 0 || io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH) {
        return Err(unresolved());
    }
    Ok(())
}
fn watch(mut child: Child, supervisor: Arc<Supervisor>, deadline: Instant) {
    let pid = child.id() as libc::pid_t;
    // No lifecycle mutex is held across a syscall, native operation, or RPC.
    while let Ok(ending) = exited(pid) {
        let mut state = supervisor.state.lock().unwrap_or_else(|e| e.into_inner());
        let deadline = state.deadline.map_or(deadline, |d| d.min(deadline));
        if ending
            || state.stop
            || supervisor.cancelled.load(Ordering::Acquire)
            || Instant::now() >= deadline
        {
            break;
        }
        let duration = deadline
            .saturating_duration_since(Instant::now())
            .min(SLICE);
        let waited = supervisor
            .wake
            .wait_timeout(state, duration)
            .unwrap_or_else(|e| e.into_inner());
        state = waited.0;
        drop(state);
    }
    supervisor.cancelled.store(true, Ordering::Release);
    let proved = fence_child(&mut child).is_ok();
    if proved {
        supervisor.lock.proved.store(true, Ordering::Release);
    }
    // An unresolved child is deliberately not reaped or represented as stopped.
    let mut state = supervisor.state.lock().unwrap_or_else(|e| e.into_inner());
    state.done = Some(proved);
    supervisor.wake.notify_all();
}

fn helper_path() -> io::Result<PathBuf> {
    let current =
        fs::canonicalize(std::env::current_exe().map_err(|_| invalid())?).map_err(|_| invalid())?;
    let supplied = match std::env::var_os("GRV_DUCKDB_GCS_OWNER_BIN") {
        Some(path) => {
            let path = PathBuf::from(path);
            if !path.is_absolute() {
                return Err(invalid());
            }
            path
        }
        None => current
            .parent()
            .ok_or_else(invalid)?
            .join("grv-duckdb-gcs-owner"),
    };
    validate_helper(&supplied, &current)
}
fn validate_helper(supplied: &Path, current: &Path) -> io::Result<PathBuf> {
    crate::lock::check_ancestors(supplied).map_err(|_| invalid())?;
    let path = fs::canonicalize(supplied).map_err(|_| invalid())?;
    if path == current {
        return Err(invalid());
    }
    crate::lock::check_ancestors(&path).map_err(|_| invalid())?;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&path)
        .map_err(|_| invalid())?;
    let meta = file.metadata().map_err(|_| invalid())?;
    let uid = unsafe { libc::geteuid() };
    if !meta.is_file()
        || meta.nlink() != 1
        || (meta.uid() != uid && meta.uid() != 0)
        || meta.mode() & 0o022 != 0
        || meta.mode() & 0o111 == 0
    {
        return Err(invalid());
    }
    // An explicit override is a native helper, never an arbitrary shell command.
    let mut magic = [0; 4];
    if file.read_at(&mut magic, 0).map_err(|_| invalid())? != 4
        || !matches!(
            magic,
            [0x7f, b'E', b'L', b'F']
                | [0xcf, 0xfa, 0xed, 0xfe]
                | [0xfe, 0xed, 0xfa, 0xcf]
                | [0xca, 0xfe, 0xba, 0xbe]
                | [0xbe, 0xba, 0xfe, 0xca]
        )
    {
        return Err(invalid());
    }
    Ok(path)
}

pub(super) struct ProcessOwner {
    stream: UnixStream,
    supervisor: Arc<Supervisor>,
    watcher: Option<JoinHandle<()>>,
    deadline: Instant,
}
impl ProcessOwner {
    #[allow(clippy::too_many_arguments)] // Exact native setup plus shared fence authority.
    pub(super) fn spawn(
        path: &Path,
        lock: &WorkspaceLock,
        httpfs: &str,
        scope: &str,
        token: &str,
        project: &str,
        deadline: Instant,
        cancelled: Arc<AtomicBool>,
    ) -> io::Result<Self> {
        let mut owner = Self::spawn_unconfigured(path, lock, deadline, cancelled)?;
        if let Err(error) = owner.configure(httpfs, scope, token, project, deadline) {
            owner.supervisor.stop();
            if owner.close().is_err() {
                return Err(unresolved());
            }
            return Err(error);
        }
        Ok(owner)
    }
    pub(super) fn spawn_unconfigured(
        path: &Path,
        lock: &WorkspaceLock,
        deadline: Instant,
        cancelled: Arc<AtomicBool>,
    ) -> io::Result<Self> {
        if cancelled.load(Ordering::Acquire) || deadline <= Instant::now() {
            return Err(expired());
        }
        lock.recheck().map_err(|_| invalid())?;
        let identity = database_identity(path)?;
        if path != lock.engine_path() {
            return Err(invalid());
        }
        let helper = helper_path()?;
        let inherited_lock = lock.descendant_reference().map_err(|_| unresolved())?;
        let lock_fd = inherited_lock.as_raw_fd();
        let (stream, child_stream) = UnixStream::pair().map_err(|_| unresolved())?;
        socket_setup(&stream)?;
        let ipc_fd = child_stream.as_raw_fd();
        let mut command = Command::new(helper);
        command
            .args([
                "--ipc-fd",
                &ipc_fd.to_string(),
                "--lock-fd",
                &lock_fd.to_string(),
            ])
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        unsafe {
            command.pre_exec(move || {
                if libc::setpgid(0, 0) != 0 {
                    return Err(io::Error::last_os_error());
                }
                for fd in [ipc_fd, lock_fd] {
                    let flags = libc::fcntl(fd, libc::F_GETFD);
                    if flags < 0 || libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0 {
                        return Err(io::Error::last_os_error());
                    }
                }
                Ok(())
            });
        }
        let child = command.spawn().map_err(|_| unresolved())?;
        drop(child_stream);
        let supervisor = Arc::new(Supervisor {
            state: Mutex::new(State {
                stop: false,
                done: None,
                deadline: Some(deadline),
            }),
            wake: Condvar::new(),
            cancelled,
            lock: LockFence {
                file: ManuallyDrop::new(inherited_lock),
                proved: AtomicBool::new(false),
            },
        });
        // Keep recoverable ownership if spawning the watcher itself fails.
        let launch = Arc::new(Mutex::new(Some(child)));
        let resources = Arc::clone(&launch);
        let shared = Arc::clone(&supervisor);
        let watcher = match thread::Builder::new()
            .name("grv-gcs-owner-fence".into())
            .spawn(move || {
                let child = resources.lock().unwrap_or_else(|e| e.into_inner()).take();
                if let Some(child) = child {
                    watch(child, shared, deadline);
                }
            }) {
            Ok(watcher) => watcher,
            Err(_) => {
                supervisor.stop();
                if let Some(child) = launch.lock().unwrap_or_else(|e| e.into_inner()).take() {
                    watch(child, Arc::clone(&supervisor), deadline);
                }
                return Err(unresolved());
            }
        };
        let mut owner = Self {
            stream,
            supervisor,
            watcher: Some(watcher),
            deadline,
        };
        let result = (|| {
            let clock = monotonic_nanos()?;
            let remaining = deadline.saturating_duration_since(Instant::now());
            let deadline_nanos = clock
                .checked_add(u64::try_from(remaining.as_nanos()).map_err(|_| invalid())?)
                .ok_or_else(invalid)?;
            let init = Open {
                response_encoding: "raw-tagged-metadata".into(),
                path: path.to_str().ok_or_else(invalid)?.to_owned(),
                identity,
                deadline_nanos,
            };
            send(
                &mut owner.stream,
                &Message::Open(init),
                deadline,
                &owner.supervisor.cancelled,
            )?;
            match receive_response(&mut owner.stream, deadline, &owner.supervisor.cancelled)? {
                Response::Ok(Reply::Done) => Ok(()),
                _ => Err(unresolved()),
            }
        })();
        if let Err(error) = result {
            owner.supervisor.stop();
            if owner.close().is_err() {
                return Err(unresolved());
            }
            return Err(error);
        }
        Ok(owner)
    }
    pub(super) fn deadline(&self) -> Instant {
        self.deadline
    }
    pub(super) fn configure(
        &mut self,
        httpfs: &str,
        scope: &str,
        token: &str,
        project: &str,
        deadline: Instant,
    ) -> io::Result<()> {
        bounded_arguments([httpfs, scope, project, token])?;
        validate_token(token)?;
        self.deadline = self.deadline.min(deadline);
        {
            let mut state = self
                .supervisor
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            state.deadline = Some(
                state
                    .deadline
                    .map_or(self.deadline, |d| d.min(self.deadline)),
            );
        }
        self.supervisor.wake.notify_all();
        let result = (|| {
            let config = Configure {
                httpfs: httpfs.to_owned(),
                scope: scope.to_owned(),
                token: Secret(token.to_owned()),
                project: project.to_owned(),
                deadline_nanos: encode_deadline(self.deadline)?,
            };
            send(
                &mut self.stream,
                &Message::Configure(config),
                self.deadline,
                &self.supervisor.cancelled,
            )?;
            receive_response(&mut self.stream, self.deadline, &self.supervisor.cancelled)
        })();
        match result {
            Ok(Response::Ok(Reply::Done)) => Ok(()),
            Ok(Response::Failed) => Err(io::Error::other("GCS owner operation failed")),
            _ => {
                self.supervisor.stop();
                self.close()?;
                Err(unresolved())
            }
        }
    }
    #[allow(dead_code)] // Kept as part of the parent integration / test API.
    pub(super) fn cancelled(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.supervisor.cancelled)
    }
    pub(super) fn call(&mut self, request: Request) -> io::Result<Reply> {
        request.validate_arguments()?;
        let result = (|| {
            send(
                &mut self.stream,
                &Message::Call(request),
                self.deadline,
                &self.supervisor.cancelled,
            )?;
            receive_response(&mut self.stream, self.deadline, &self.supervisor.cancelled)
        })();
        match result {
            Ok(Response::Ok(reply)) => Ok(reply),
            // A complete denial is authoritative and preserves the connection:
            // callers may ROLLBACK or inspect receipts, but never retry effects.
            Ok(Response::Failed) => Err(io::Error::other("GCS owner operation failed")),
            Err(error) => {
                // A partial request/response is never reused. Fence before returning.
                self.supervisor.stop();
                if self.close().is_err() {
                    return Err(unresolved());
                }
                Err(error)
            }
        }
    }
    #[allow(dead_code)] // Parent integration calls this instead of local fail-closed dispatch.
    pub(super) fn verified_descriptor(
        &mut self,
        descriptor: RawFd,
        query: &str,
    ) -> io::Result<Reply> {
        // Validate bounded input before committing any part of the IPC exchange.
        argument(query)?;
        expected_descriptor_literal(descriptor, query)?;
        let result = (|| {
            send(
                &mut self.stream,
                &Message::VerifiedDescriptor {
                    query: query.to_owned(),
                    parent_descriptor: descriptor,
                },
                self.deadline,
                &self.supervisor.cancelled,
            )?;
            send_descriptor(
                &self.stream,
                descriptor,
                self.deadline,
                &self.supervisor.cancelled,
            )?;
            receive_response(&mut self.stream, self.deadline, &self.supervisor.cancelled)
        })();
        match result {
            Ok(Response::Ok(reply)) => Ok(reply),
            Ok(Response::Failed) => Err(io::Error::other("GCS owner operation failed")),
            Err(error) => {
                self.supervisor.stop();
                if self.close().is_err() {
                    return Err(unresolved());
                }
                Err(error)
            }
        }
    }
    pub(super) fn close(&mut self) -> io::Result<()> {
        self.supervisor.stop();
        let _ = self.stream.shutdown(std::net::Shutdown::Both);
        if let Some(watcher) = self.watcher.take()
            && watcher.join().is_err()
        {
            return Err(unresolved());
        }
        match self
            .supervisor
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .done
        {
            Some(true) => Ok(()),
            _ => Err(unresolved()),
        }
    }
}
impl Drop for ProcessOwner {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn validate_token(token: &str) -> io::Result<()> {
    if token.is_empty()
        || token.len() > 16 * 1024
        || !token
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-._~+/=".contains(&c))
    {
        return Err(invalid());
    }
    Ok(())
}
fn encode_deadline(deadline: Instant) -> io::Result<u64> {
    let clock = monotonic_nanos()?;
    clock
        .checked_add(
            u64::try_from(
                deadline
                    .saturating_duration_since(Instant::now())
                    .as_nanos(),
            )
            .map_err(|_| invalid())?,
        )
        .ok_or_else(invalid)
}
fn decode_deadline(nanos: u64) -> io::Result<Instant> {
    let anchor = Instant::now();
    let remaining = nanos.checked_sub(monotonic_nanos()?).ok_or_else(expired)?;
    anchor
        .checked_add(Duration::from_nanos(remaining))
        .ok_or_else(invalid)
}
#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct DatabaseIdentity {
    device: u64,
    inode: u64,
}
fn database_identity(path: &Path) -> io::Result<Option<DatabaseIdentity>> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            let uid = unsafe { libc::geteuid() };
            if !meta.is_file()
                || meta.nlink() != 1
                || (meta.uid() != uid && meta.uid() != 0)
                || meta.mode() & 0o022 != 0
            {
                return Err(invalid());
            }
            Ok(Some(DatabaseIdentity {
                device: meta.dev(),
                inode: meta.ino(),
            }))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(invalid()),
    }
}
fn verify_database(path: &Path, identity: Option<DatabaseIdentity>) -> io::Result<()> {
    if database_identity(path)? != identity {
        return Err(invalid());
    }
    Ok(())
}
fn argument(value: &str) -> io::Result<CString> {
    if value.len() > ARG_LIMIT {
        return Err(invalid());
    }
    cstring(value.as_bytes()).map_err(|_| invalid())
}
struct NativeOwner(*mut c_void);
impl Drop for NativeOwner {
    fn drop(&mut self) {
        unsafe { grv_native_close(self.0) };
    }
}

fn descriptor(fd: RawFd) -> io::Result<libc::stat> {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } != 0 {
        return Err(invalid());
    }
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        return Err(invalid());
    }
    Ok(unsafe { stat.assume_init() })
}
fn verify_lock(lock: &File, path: &Path) -> io::Result<()> {
    if crate::lock::canonical_engine_path(path).map_err(|_| invalid())? != path {
        return Err(invalid());
    }
    let mut helper = path.as_os_str().to_os_string();
    helper.push(".grv-lock");
    let helper = PathBuf::from(helper);
    crate::lock::check_ancestors(&helper).map_err(|_| invalid())?;
    let held = descriptor(lock.as_raw_fd())?;
    let named = fs::symlink_metadata(helper).map_err(|_| invalid())?;
    if held.st_mode & libc::S_IFMT != libc::S_IFREG
        || held.st_nlink != 1
        || held.st_uid != unsafe { libc::geteuid() }
        || held.st_mode & 0o077 != 0
        || !named.is_file()
        || named.nlink() != 1
        || named.uid() != held.st_uid
        || named.mode() & 0o077 != 0
        || named.dev() != held.st_dev as u64
        || named.ino() != held.st_ino as u64
    {
        return Err(invalid());
    }
    Ok(())
}

// A parent crash must not strand the token/lock inside a blocked native HTTP
// call. This watchdog belongs ONLY to the dedicated child and exits its entire
// process, never an individual native thread. It makes no native calls.
struct ChildWatchState {
    stop: bool,
    deadline: Instant,
}
struct ChildWatchdog {
    shared: Arc<(Mutex<ChildWatchState>, Condvar)>,
    watcher: Option<JoinHandle<()>>,
}
impl ChildWatchdog {
    fn start(stream: &UnixStream) -> io::Result<Self> {
        let peer = stream.try_clone().map_err(|_| unresolved())?;
        let parent = unsafe { libc::getppid() };
        if parent <= 1 {
            return Err(unresolved());
        }
        let shared = Arc::new((
            Mutex::new(ChildWatchState {
                stop: false,
                deadline: Instant::now() + Duration::from_secs(5),
            }),
            Condvar::new(),
        ));
        let state = Arc::clone(&shared);
        let watcher = thread::Builder::new()
            .name("grv-gcs-child-fence".into())
            .spawn(move || {
                loop {
                    let (mutex, wake) = &*state;
                    let guard = mutex.lock().unwrap_or_else(|e| e.into_inner());
                    if guard.stop {
                        return;
                    }
                    if Instant::now() >= guard.deadline || unsafe { libc::getppid() } != parent {
                        unsafe { libc::_exit(124) };
                    }
                    let duration = guard
                        .deadline
                        .saturating_duration_since(Instant::now())
                        .min(SLICE);
                    drop(guard);
                    let mut pfd = libc::pollfd {
                        fd: peer.as_raw_fd(),
                        events: libc::POLLIN,
                        revents: 0,
                    };
                    let rc = unsafe { libc::poll(&mut pfd, 1, 0) };
                    if rc > 0 && pfd.revents & (libc::POLLHUP | libc::POLLERR | libc::POLLNVAL) != 0
                    {
                        unsafe { libc::_exit(124) };
                    }
                    if rc > 0 && pfd.revents & libc::POLLIN != 0 {
                        let mut byte = 0u8;
                        let n = unsafe {
                            libc::recv(
                                peer.as_raw_fd(),
                                (&mut byte as *mut u8).cast(),
                                1,
                                libc::MSG_PEEK | libc::MSG_DONTWAIT,
                            )
                        };
                        if n == 0 {
                            unsafe { libc::_exit(124) };
                        }
                    }
                    let guard = mutex.lock().unwrap_or_else(|e| e.into_inner());
                    if guard.stop {
                        return;
                    }
                    drop(
                        wake.wait_timeout(guard, duration)
                            .unwrap_or_else(|e| e.into_inner()),
                    );
                }
            })
            .map_err(|_| unresolved())?;
        Ok(Self {
            shared,
            watcher: Some(watcher),
        })
    }
    fn configure(&self, deadline: Instant) {
        self.shared
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .deadline = deadline;
        self.shared.1.notify_all();
    }
}
impl Drop for ChildWatchdog {
    fn drop(&mut self) {
        self.shared.0.lock().unwrap_or_else(|e| e.into_inner()).stop = true;
        self.shared.1.notify_all();
        if let Some(watcher) = self.watcher.take() {
            let _ = watcher.join();
        }
    }
}

/// Called only by the dedicated binary through native::run_gcs_owner().
/// FDs are inherited from the supervisor, not reopened or newly flocked.
pub(super) fn serve() -> io::Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 4 || args[0] != "--ipc-fd" || args[2] != "--lock-fd" {
        return Err(invalid());
    }
    let ipc: RawFd = args[1].parse().map_err(|_| invalid())?;
    let lock_fd: RawFd = args[3].parse().map_err(|_| invalid())?;
    if ipc <= 2
        || lock_fd <= 2
        || ipc == lock_fd
        || unsafe { libc::getpgrp() } != unsafe { libc::getpid() }
    {
        return Err(invalid());
    }
    if descriptor(ipc)?.st_mode & libc::S_IFMT != libc::S_IFSOCK {
        return Err(invalid());
    }
    descriptor(lock_fd)?;
    let mut stream = unsafe { UnixStream::from_raw_fd(ipc) };
    // Close only at process exit, including after native close or unwinding.
    let lock = ManuallyDrop::new(unsafe { File::from_raw_fd(lock_fd) });
    socket_setup(&stream)?;
    // Declared before NativeOwner so unwinding keeps this fence active through
    // grv_native_close; its normal Drop stops and joins the bounded watcher.
    let self_fence = ChildWatchdog::start(&stream)?;
    let cancelled = AtomicBool::new(false);
    let init: Message = receive(
        &mut stream,
        Instant::now() + Duration::from_secs(5),
        &cancelled,
    )?;
    let Message::Open(init) = init else {
        return Err(invalid());
    };
    if init.response_encoding != "raw-tagged-metadata" {
        return Err(invalid());
    }
    let mut deadline = decode_deadline(init.deadline_nanos)?;
    if deadline <= Instant::now() {
        return Err(expired());
    }
    self_fence.configure(deadline);
    let path = Path::new(&init.path);
    verify_lock(&lock, path)?;
    verify_database(path, init.identity)?;
    let version = unsafe { grv_native_version() };
    if version.is_null()
        || unsafe { CStr::from_ptr(version).to_bytes() } != b"v1.5.6"
        || unsafe { grv_native_guard_revision() } != 5
    {
        return Err(unresolved());
    }
    let path_c = argument(&init.path)?;
    let mut error = Zeroizing::new([0 as c_char; 4096]);
    let pointer = unsafe { grv_native_open(path_c.as_ptr(), error.as_mut_ptr(), error.len()) };
    if pointer.is_null() {
        return Err(unresolved());
    }
    let owner = NativeOwner(pointer);
    let identity = match init.identity {
        Some(identity) => {
            verify_database(path, Some(identity))?;
            identity
        }
        None => database_identity(path)?.ok_or_else(invalid)?,
    };
    verify_lock(&lock, path)?;
    send_response(
        &mut stream,
        &Response::Ok(Reply::Done),
        deadline,
        &cancelled,
    )?;
    loop {
        let message: Message = receive(&mut stream, deadline, &cancelled)?;
        verify_lock(&lock, path)?;
        verify_database(path, Some(identity))?;
        let request = match message {
            Message::Call(request) => request,
            Message::Configure(mut config) => {
                deadline = deadline.min(decode_deadline(config.deadline_nanos)?);
                self_fence.configure(deadline);
                let response = match configure_native(owner.0, &mut config, deadline) {
                    Ok(()) => Response::Ok(Reply::Done),
                    Err(_) => Response::Failed,
                };
                verify_database(path, Some(identity))?;
                send_response(&mut stream, &response, deadline, &cancelled)?;
                continue;
            }
            Message::VerifiedDescriptor {
                query,
                parent_descriptor,
            } => {
                // Any descriptor transport/validation failure terminates this
                // stream. No native operation sees an unvalidated descriptor.
                let file = receive_descriptor(&stream, deadline, &cancelled)?;
                verify_file_descriptor(file.as_raw_fd())?;
                let response = match verified_native(owner.0, &file, parent_descriptor, &query) {
                    Ok(reply) => Response::Ok(reply),
                    Err(_) => Response::Failed,
                };
                // This descriptor authorizes only this synchronous materializing
                // call, never a stored lazy view or a later descriptor number.
                drop(file);
                verify_database(path, Some(identity))?;
                send_response(&mut stream, &response, deadline, &cancelled)?;
                continue;
            }
            Message::Open(_) => return Err(invalid()),
        };
        if matches!(request, Request::Close) {
            // Keep the inherited ownership reference through native close and response.
            drop(owner);
            send_response(
                &mut stream,
                &Response::Ok(Reply::Done),
                deadline,
                &cancelled,
            )?;
            return Ok(());
        }
        let response = match dispatch(owner.0, request) {
            Ok(reply) => Response::Ok(reply),
            Err(_) => Response::Failed,
        };
        verify_database(path, Some(identity))?;
        send_response(&mut stream, &response, deadline, &cancelled)?;
    }
}
fn configure_native(
    owner: *mut c_void,
    config: &mut Configure,
    deadline: Instant,
) -> io::Result<()> {
    bounded_arguments([
        config.httpfs.as_str(),
        config.scope.as_str(),
        config.project.as_str(),
        config.token.0.as_str(),
    ])?;
    validate_token(&config.token.0)?;
    let httpfs = argument(&config.httpfs)?;
    let scope = argument(&config.scope)?;
    let project = argument(&config.project)?;
    let token = Zeroizing::new(argument(&config.token.0)?.into_bytes_with_nul());
    config.token.0.zeroize();
    let millis = u64::try_from(
        deadline
            .saturating_duration_since(Instant::now())
            .as_millis(),
    )
    .map_err(|_| invalid())?;
    if millis == 0 {
        return Err(expired());
    }
    let mut error = Zeroizing::new([0 as c_char; 4096]);
    let status = unsafe {
        grv_native_gcs_configure(
            owner,
            httpfs.as_ptr(),
            scope.as_ptr(),
            token.as_ptr().cast(),
            project.as_ptr(),
            millis,
            error.as_mut_ptr(),
            error.len(),
        )
    };
    setup_outcome(status)
}
fn setup_outcome(status: i32) -> io::Result<()> {
    if status == -2 {
        // Native setup could not prove temporary credential revocation/guard
        // restoration. Exit the whole dedicated owner; parent EOF handling
        // must fence before reopening. This is not rollback evidence.
        unsafe { libc::_exit(125) };
    }
    if status != 0 {
        return Err(unresolved());
    }
    Ok(())
}

// This private endpoint executes trusted adapter-generated SQL, not user SQL.
// Native parsing/guards remain authoritative. Independently refuse keywords
// that can persist a lazy descriptor-backed definition: unlike a prefix check,
// this catches OR REPLACE/TEMP forms and leading or interleaved comments. Quoted
// strings/identifiers and nested block comments cannot introduce SQL keywords.
// Conservative refusals of unusual internal SQL are intentional.
fn has_lazy_definition(query: &str) -> bool {
    let bytes = query.as_bytes();
    let mut cursor = 0;
    while cursor < bytes.len() {
        match bytes[cursor] {
            b'\'' | b'"' => {
                let quote = bytes[cursor];
                cursor += 1;
                loop {
                    if cursor >= bytes.len() {
                        return true; // malformed quote: fail closed before native
                    }
                    if bytes[cursor] == quote {
                        cursor += 1;
                        if bytes.get(cursor) == Some(&quote) {
                            cursor += 1;
                        } else {
                            break;
                        }
                    } else {
                        cursor += 1;
                    }
                }
            }
            b'-' if bytes.get(cursor + 1) == Some(&b'-') => {
                cursor += 2;
                while cursor < bytes.len() && !matches!(bytes[cursor], b'\n' | b'\r') {
                    cursor += 1;
                }
            }
            b'/' if bytes.get(cursor + 1) == Some(&b'*') => {
                cursor += 2;
                let mut depth = 1usize;
                while depth != 0 {
                    if cursor + 1 >= bytes.len() {
                        return true;
                    }
                    if &bytes[cursor..cursor + 2] == b"/*" {
                        depth += 1;
                        cursor += 2;
                    } else if &bytes[cursor..cursor + 2] == b"*/" {
                        depth -= 1;
                        cursor += 2;
                    } else {
                        cursor += 1;
                    }
                }
            }
            c if c.is_ascii_alphabetic() || c == b'_' => {
                let start = cursor;
                cursor += 1;
                while bytes
                    .get(cursor)
                    .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_')
                {
                    cursor += 1;
                }
                let token = &bytes[start..cursor];
                if [b"VIEW".as_slice(), b"MACRO", b"PREPARE"]
                    .iter()
                    .any(|keyword| token.eq_ignore_ascii_case(keyword))
                {
                    return true;
                }
            }
            _ => cursor += 1,
        }
    }
    false
}

fn expected_descriptor_literal(parent_descriptor: i32, query: &str) -> io::Result<String> {
    if parent_descriptor < 0 || query.len() > ARG_LIMIT {
        return Err(invalid());
    }
    let literal = format!("'/dev/fd/{parent_descriptor}'");
    if !query.contains(&literal)
        || query.split(&literal).any(|part| part.contains("/dev/fd/"))
        || has_lazy_definition(query)
    {
        return Err(invalid());
    }
    Ok(literal)
}
fn descriptor_query(
    parent_descriptor: i32,
    child_descriptor: RawFd,
    query: &str,
) -> io::Result<CString> {
    let literal = expected_descriptor_literal(parent_descriptor, query)?;
    // Trusted internal SQL only. Replace the entire exact quoted literal, not
    // numbers, prefixes, other paths, or other descriptor literals.
    let replacement = format!("'/dev/fd/{child_descriptor}'");
    let occurrences = query.matches(&literal).count();
    let length = query
        .len()
        .checked_sub(occurrences.checked_mul(literal.len()).ok_or_else(invalid)?)
        .and_then(|n| n.checked_add(occurrences.checked_mul(replacement.len())?))
        .filter(|&n| n <= ARG_LIMIT)
        .ok_or_else(invalid)?;
    // Build once with terminator capacity; no growable replacement String
    // plus a second CString copy. Exact quoted literal remains sole authority.
    let mut remapped = Vec::new();
    remapped
        .try_reserve_exact(length.checked_add(1).ok_or_else(invalid)?)
        .map_err(|_| invalid())?;
    for (index, part) in query.split(&literal).enumerate() {
        if index > 0 {
            remapped.extend_from_slice(replacement.as_bytes());
        }
        remapped.extend_from_slice(part.as_bytes());
    }
    debug_assert_eq!(length, remapped.len());
    CString::new(remapped).map_err(|_| invalid())
}
fn verified_native(
    owner: *mut c_void,
    file: &OwnedFd,
    parent_descriptor: i32,
    query: &str,
) -> io::Result<Reply> {
    let query = descriptor_query(parent_descriptor, file.as_raw_fd(), query)?;
    let mut error = Zeroizing::new([0 as c_char; 4096]);
    let mut output = vec![0; METADATA_BYTES];
    let mut written = 0;
    let status = unsafe {
        grv_native_verified_file_query(
            owner,
            file.as_raw_fd(),
            query.as_ptr(),
            output.as_mut_ptr(),
            output.len(),
            &mut written,
            error.as_mut_ptr(),
            error.len(),
        )
    };
    if status != 0 || written > output.len() || written > BYTE_LIMIT {
        return Err(unresolved());
    }
    output.truncate(written);
    Ok(Reply::Bytes(output))
}

fn dispatch(owner: *mut c_void, request: Request) -> io::Result<Reply> {
    request.validate_arguments()?;
    let mut error = Zeroizing::new([0 as c_char; 4096]);
    let mut output = vec![0; BYTE_LIMIT.min(METADATA_BYTES)];
    let mut written = 0;
    let mut boolean = 0;
    let mut bytes = false;
    let mut is_bool = false;
    let status = match request {
        Request::Metadata { query, private } => {
            let query = argument(&query)?;
            bytes = true;
            unsafe {
                if private {
                    grv_native_private_command(
                        owner,
                        query.as_ptr(),
                        output.as_mut_ptr(),
                        output.len(),
                        &mut written,
                        error.as_mut_ptr(),
                        error.len(),
                    )
                } else {
                    grv_native_metadata_query(
                        owner,
                        query.as_ptr(),
                        output.as_mut_ptr(),
                        output.len(),
                        &mut written,
                        error.as_mut_ptr(),
                        error.len(),
                    )
                }
            }
        }
        Request::Remote { locations, query } => {
            if locations.is_empty()
                || locations.len() > LOCATION_LIMIT
                || locations
                    .iter()
                    .try_fold(0usize, |sum, s| sum.checked_add(s.len()))
                    .is_none_or(|sum| sum > ARG_LIMIT)
            {
                return Err(invalid());
            }
            let query = argument(&query)?;
            let locations = locations
                .iter()
                .map(|s| argument(s))
                .collect::<io::Result<Vec<_>>>()?;
            let pointers: Vec<_> = locations.iter().map(|s| s.as_ptr()).collect();
            bytes = true;
            unsafe {
                grv_native_gcs_query(
                    owner,
                    pointers.as_ptr(),
                    pointers.len(),
                    query.as_ptr(),
                    output.as_mut_ptr(),
                    output.len(),
                    &mut written,
                    error.as_mut_ptr(),
                    error.len(),
                )
            }
        }
        Request::Relation {
            schema,
            table,
            kind,
        } => {
            let schema = argument(&schema)?;
            let table = argument(&table)?;
            unsafe {
                match kind {
                    RelationKind::Exists => {
                        is_bool = true;
                        grv_native_relation_exists(
                            owner,
                            schema.as_ptr(),
                            table.as_ptr(),
                            &mut boolean,
                            error.as_mut_ptr(),
                            error.len(),
                        )
                    }
                    RelationKind::View => {
                        is_bool = true;
                        grv_native_is_view(
                            owner,
                            schema.as_ptr(),
                            table.as_ptr(),
                            &mut boolean,
                            error.as_mut_ptr(),
                            error.len(),
                        )
                    }
                    RelationKind::Base => {
                        is_bool = true;
                        grv_native_base_table(
                            owner,
                            schema.as_ptr(),
                            table.as_ptr(),
                            &mut boolean,
                            error.as_mut_ptr(),
                            error.len(),
                        )
                    }
                    RelationKind::Schema => {
                        bytes = true;
                        grv_native_relation_schema(
                            owner,
                            schema.as_ptr(),
                            table.as_ptr(),
                            output.as_mut_ptr(),
                            output.len(),
                            &mut written,
                            error.as_mut_ptr(),
                            error.len(),
                        )
                    }
                }
            }
        }
        Request::SchemaExists { schema } => {
            let schema = argument(&schema)?;
            is_bool = true;
            unsafe {
                grv_native_schema_exists(
                    owner,
                    schema.as_ptr(),
                    &mut boolean,
                    error.as_mut_ptr(),
                    error.len(),
                )
            }
        }
        Request::AuthorizeRelation { schema, table } => {
            let schema = argument(&schema)?;
            let table = argument(&table)?;
            unsafe {
                grv_native_authorize_relation(
                    owner,
                    schema.as_ptr(),
                    table.as_ptr(),
                    error.as_mut_ptr(),
                    error.len(),
                )
            }
        }
        Request::ManagedInitialized => {
            is_bool = true;
            unsafe {
                grv_native_managed_initialized(owner, &mut boolean, error.as_mut_ptr(), error.len())
            }
        }
        Request::StagePull {
            query,
            stage,
            expected,
        } => {
            let query = argument(&query)?;
            let stage = argument(&stage)?;
            let expected = argument(&expected)?;
            unsafe {
                grv_native_stage_pull_query(
                    owner,
                    query.as_ptr(),
                    stage.as_ptr(),
                    expected.as_ptr(),
                    error.as_mut_ptr(),
                    error.len(),
                )
            }
        }
        Request::StageBuild {
            query,
            projection,
            stage,
            expected,
            allowance,
        } => {
            if allowance == 0 {
                return Err(invalid());
            }
            let query = argument(&query)?;
            let projection = argument(&projection)?;
            let stage = argument(&stage)?;
            let expected = argument(&expected)?;
            output.truncate(allowance.min(BYTE_LIMIT).min(METADATA_BYTES));
            bytes = true;
            unsafe {
                grv_native_stage_build_query(
                    owner,
                    query.as_ptr(),
                    projection.as_ptr(),
                    stage.as_ptr(),
                    expected.as_ptr(),
                    output.as_mut_ptr(),
                    output.len(),
                    &mut written,
                    error.as_mut_ptr(),
                    error.len(),
                )
            }
        }
        Request::Close => return Err(invalid()),
    };
    if status != 0 || written > output.len() || is_bool && !matches!(boolean, 0 | 1) {
        return Err(unresolved());
    }
    if bytes {
        output.truncate(written);
        Ok(Reply::Bytes(output))
    } else if is_bool {
        Ok(Reply::Bool(boolean == 1))
    } else {
        Ok(Reply::Done)
    }
}

#[cfg(test)]
#[path = "native_owner_tests.rs"]
mod lifecycle_tests;

#[cfg(test)]
#[path = "native_owner_recovery_tests.rs"]
mod recovery_tests;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn command_aggregate_and_closed_encoding_are_checked_before_native_dispatch() {
        let large = "x".repeat(ARG_LIMIT);
        assert!(bounded_arguments([large.as_str()]).is_ok());
        assert!(bounded_arguments([large.as_str(), "x"]).is_err());
        let request = Request::StageBuild {
            query: large.clone(),
            projection: "x".into(),
            stage: String::new(),
            expected: String::new(),
            allowance: METADATA_BYTES,
        };
        assert!(request.validate_arguments().is_err());
        let remote = Request::Remote {
            query: large,
            locations: vec!["gs://synthetic/object".into()],
        };
        assert!(remote.validate_arguments().is_err());
        let old_open = serde_json::json!({"type":"Open","value":{"path":"/no/open","identity":null,"deadline_nanos":1}});
        assert!(serde_json::from_value::<Message>(old_open).is_err());
        let many_locations = serde_json::json!({"type":"Call","value":{"type":"Remote","query":"SELECT 1","locations":vec![""; LOCATION_LIMIT+1]}});
        assert!(serde_json::from_value::<Message>(many_locations).is_err());
    }
    #[test]
    fn maximum_location_count_and_aggregate_ascii_command_fit_closed_frame() {
        let width = ARG_LIMIT / LOCATION_LIMIT;
        let message = Message::Call(Request::Remote {
            query: String::new(),
            locations: vec!["x".repeat(width); LOCATION_LIMIT],
        });
        let mut buffer = CappedBuffer(Zeroizing::new(Vec::with_capacity(FRAME_LIMIT)));
        serde_json::to_writer(&mut buffer, &message).unwrap();
        assert_eq!(buffer.0.capacity(), FRAME_LIMIT);
        let Message::Call(request) = serde_json::from_slice::<Message>(&buffer.0).unwrap() else {
            panic!("wrong command")
        };
        request.validate_arguments().unwrap();
    }
    #[test]
    fn allocation_free_preflight_refuses_tagged_content_amplification() {
        let many = serde_json::to_vec(&serde_json::json!({"type":"Call","value":{"type":"Remote","query":"SELECT 1","locations":vec!["";100_000]}})).unwrap();
        assert!(many.len() < FRAME_LIMIT);
        assert!(preflight_json(&many).is_err());
        assert!(preflight_json(br#"{"unknown":{"nested":{"unrecognized":[1,2,3]}}}"#).is_ok());
        let max = serde_json::to_vec(&Message::Call(Request::Remote {
            query: String::new(),
            locations: vec!["x"; LOCATION_LIMIT]
                .into_iter()
                .map(str::to_owned)
                .collect(),
        }))
        .unwrap();
        preflight_json(&max).unwrap();
        let deep = format!("{}0{}", "[".repeat(14), "]".repeat(14));
        assert!(preflight_json(deep.as_bytes()).is_err());
    }
    fn descriptor_pair() -> (UnixStream, UnixStream) {
        let (reader, writer) = UnixStream::pair().unwrap();
        socket_setup(&reader).unwrap();
        socket_setup(&writer).unwrap();
        (reader, writer)
    }
    fn readonly_fixture() -> (tempfile::TempDir, File) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pinned");
        fs::write(&path, b"pinned offline bytes").unwrap();
        let file = File::open(&path).unwrap();
        (dir, file)
    }
    fn descriptor_deadline() -> Instant {
        Instant::now() + Duration::from_secs(1)
    }
    #[allow(clippy::unnecessary_cast)] // stat field widths differ across supported Unix targets.
    fn source_descriptor_count(file: &File) -> usize {
        // Count only this unique inode, not unrelated descriptors opened by
        // concurrent tests. All test-delivered descriptors must be in this range.
        assert!(file.as_raw_fd() < 4096);
        let identity = file.metadata().unwrap();
        (0..4096)
            .filter(|&fd| {
                let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
                (unsafe { libc::fstat(fd, stat.as_mut_ptr()) }) == 0 && {
                    let stat = unsafe { stat.assume_init() };
                    stat.st_dev as u64 == identity.dev() && stat.st_ino as u64 == identity.ino()
                }
            })
            .count()
    }
    fn send_test_rights(stream: &UnixStream, marker: u8, descriptors: &[RawFd]) -> io::Result<()> {
        // Offline adversarial sender, including more ancillary data than the
        // production receiver will accept. Production always sends exactly one.
        let mut marker = marker;
        let mut iov = libc::iovec {
            iov_base: (&mut marker as *mut u8).cast(),
            iov_len: 1,
        };
        let mut control: DescriptorControl = [0; CONTROL_WORDS];
        let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
        message.msg_iov = &mut iov;
        message.msg_iovlen = 1;
        if !descriptors.is_empty() {
            message.msg_control = control.as_mut_ptr().cast();
            message.msg_controllen =
                unsafe { libc::CMSG_SPACE(std::mem::size_of_val(descriptors) as _) } as _;
            assert!(message.msg_controllen as usize <= std::mem::size_of_val(&control));
            unsafe {
                let header = libc::CMSG_FIRSTHDR(&message);
                (*header).cmsg_level = libc::SOL_SOCKET;
                (*header).cmsg_type = libc::SCM_RIGHTS;
                (*header).cmsg_len = libc::CMSG_LEN(std::mem::size_of_val(descriptors) as _) as _;
                std::ptr::copy_nonoverlapping(
                    descriptors.as_ptr().cast::<u8>(),
                    libc::CMSG_DATA(header),
                    std::mem::size_of_val(descriptors),
                );
            }
        }
        #[cfg(target_os = "linux")]
        let flags = libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL;
        #[cfg(not(target_os = "linux"))]
        let flags = libc::MSG_DONTWAIT;
        match unsafe { libc::sendmsg(stream.as_raw_fd(), &message, flags) } {
            1 => Ok(()),
            -1 => Err(io::Error::last_os_error()),
            _ => Err(unresolved()),
        }
    }
    #[test]
    fn typed_descriptor_roundtrip_keeps_frames_separate_and_closes_unlinked_fd() {
        let (mut reader, mut writer) = descriptor_pair();
        let (dir, file) = readonly_fixture();
        fs::remove_file(dir.path().join("pinned")).unwrap();
        assert_eq!(file.metadata().unwrap().nlink(), 0);
        let parent = file.as_raw_fd();
        let query = format!("SELECT count(*) FROM read_parquet('/dev/fd/{parent}')");
        let cancelled = AtomicBool::new(false);
        let deadline = descriptor_deadline();
        send(
            &mut writer,
            &Message::VerifiedDescriptor {
                query: query.clone(),
                parent_descriptor: parent,
            },
            deadline,
            &cancelled,
        )
        .unwrap();
        send_descriptor(&writer, parent, deadline, &cancelled).unwrap();
        send_response(
            &mut writer,
            &Response::Ok(Reply::Done),
            deadline,
            &cancelled,
        )
        .unwrap();
        let message: Message = receive(&mut reader, deadline, &cancelled).unwrap();
        let Message::VerifiedDescriptor {
            query: received,
            parent_descriptor,
        } = message
        else {
            panic!("wrong typed frame")
        };
        assert_eq!(received, query);
        assert_eq!(parent_descriptor, parent);
        let owned = receive_descriptor(&reader, deadline, &cancelled).unwrap();
        assert_ne!(owned.as_raw_fd(), parent);
        assert!(owned.as_raw_fd() < 4096);
        verify_file_descriptor(owned.as_raw_fd()).unwrap();
        assert_ne!(
            unsafe { libc::fcntl(owned.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
            0
        );
        let remapped = descriptor_query(parent, owned.as_raw_fd(), &received).unwrap();
        assert!(
            remapped
                .to_str()
                .unwrap()
                .contains(&format!("'/dev/fd/{}'", owned.as_raw_fd()))
        );
        let held = File::from(owned);
        let mut bytes = [0; 20];
        assert_eq!(held.read_at(&mut bytes, 0).unwrap(), bytes.len());
        assert_eq!(&bytes, b"pinned offline bytes");
        assert_eq!(source_descriptor_count(&file), 2);
        drop(held);
        assert_eq!(source_descriptor_count(&file), 1);
        assert!(matches!(
            receive_response(&mut reader, deadline, &cancelled).unwrap(),
            Response::Ok(Reply::Done)
        ));
    }
    #[test]
    fn descriptor_transport_rejects_missing_extra_maximum_rights_and_wrong_marker_without_leaks() {
        let (_dir, file) = readonly_fixture();
        for (marker, count) in [
            (DESCRIPTOR_MARKER, 0),
            (DESCRIPTOR_MARKER, 2),
            (DESCRIPTOR_MARKER, 32),
            // Largest portable batch; additionally probe the platform's
            // supported maximum below without shrinking receive storage.
            (DESCRIPTOR_MARKER, 253),
            (0, 1),
        ] {
            let (reader, writer) = descriptor_pair();
            send_test_rights(&writer, marker, &vec![file.as_raw_fd(); count]).unwrap();
            assert!(
                receive_descriptor(&reader, descriptor_deadline(), &AtomicBool::new(false))
                    .is_err()
            );
            assert_eq!(source_descriptor_count(&file), 1, "ancillary count {count}");
        }
        #[cfg(target_os = "macos")]
        let maximum = 512;
        #[cfg(not(target_os = "macos"))]
        let maximum = 253;
        // Installed Darwin versions may reject the current XNU maximum at
        // sendmsg (e.g. socket control-size limits). Failed sends install no
        // receiver rights. The first accepted batch must all be closed.
        let (reader, writer) = descriptor_pair();
        let mut accepted = false;
        for count in (253..=maximum).rev() {
            match send_test_rights(&writer, DESCRIPTOR_MARKER, &vec![file.as_raw_fd(); count]) {
                Ok(()) => {
                    assert!(
                        receive_descriptor(&reader, descriptor_deadline(), &AtomicBool::new(false))
                            .is_err()
                    );
                    assert_eq!(
                        source_descriptor_count(&file),
                        1,
                        "maximum accepted batch {count}"
                    );
                    accepted = true;
                    break;
                }
                Err(error) => assert!(
                    matches!(error.raw_os_error(), Some(libc::EINVAL | libc::EMSGSIZE)),
                    "unexpected send limit error: {error}"
                ),
            }
        }
        assert!(accepted, "platform rejected even 253 rights");
    }
    #[test]
    fn malformed_control_adopts_and_closes_complete_rights_before_rejection() {
        let (_dir, file) = readonly_fixture();
        let duplicate = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) };
        assert!((0..4096).contains(&duplicate));
        let mut control: DescriptorControl = [0; CONTROL_WORDS];
        let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
        message.msg_control = control.as_mut_ptr().cast();
        message.msg_controllen = unsafe { libc::CMSG_LEN(std::mem::size_of::<RawFd>() as _) } as _;
        unsafe {
            let header = libc::CMSG_FIRSTHDR(&message);
            (*header).cmsg_level = libc::SOL_SOCKET;
            (*header).cmsg_type = libc::SCM_RIGHTS;
            (*header).cmsg_len = message.msg_controllen + 1; // malformed overrun
            std::ptr::write_unaligned(libc::CMSG_DATA(header).cast::<RawFd>(), duplicate);
        }
        assert!(adopt_descriptor(&message, DESCRIPTOR_MARKER).is_err());
        assert_eq!(source_descriptor_count(&file), 1);
    }
    #[test]
    fn truncated_control_flag_adopts_and_closes_visible_rights_safely() {
        // Synthetic truncation only: deliberately shrinking recvmsg storage
        // on Darwin would install unreported rights that a test cannot clean.
        let (_dir, file) = readonly_fixture();
        let duplicate = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) };
        assert!((0..4096).contains(&duplicate));
        let mut control: DescriptorControl = [0; CONTROL_WORDS];
        let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
        message.msg_control = control.as_mut_ptr().cast();
        message.msg_controllen = unsafe { libc::CMSG_LEN(std::mem::size_of::<RawFd>() as _) } as _;
        message.msg_flags = libc::MSG_CTRUNC;
        unsafe {
            let header = libc::CMSG_FIRSTHDR(&message);
            (*header).cmsg_level = libc::SOL_SOCKET;
            (*header).cmsg_type = libc::SCM_RIGHTS;
            (*header).cmsg_len = message.msg_controllen;
            std::ptr::write_unaligned(libc::CMSG_DATA(header).cast::<RawFd>(), duplicate);
        }
        assert!(adopt_descriptor(&message, DESCRIPTOR_MARKER).is_err());
        assert_eq!(source_descriptor_count(&file), 1);
    }
    #[test]
    fn descriptor_endpoint_refuses_lazy_definitions_not_eager_queries() {
        for sql in [
            "CREATE VIEW saved AS SELECT * FROM read_parquet('/dev/fd/12')",
            "/* leading */ CREATE OR REPLACE TEMP VIEW saved AS SELECT * FROM read_parquet('/dev/fd/12')",
            "CREATE /* nested /* comment */ ok */ MACRO saved() AS TABLE SELECT * FROM read_parquet('/dev/fd/12')",
            "-- leading\n prepare saved AS SELECT * FROM read_parquet('/dev/fd/12')",
            "SELECT '/dev/fd/12'; CREATE VIEW saved AS SELECT 1",
        ] {
            assert!(
                descriptor_query(12, 34, sql).is_err(),
                "lazy SQL accepted: {sql}"
            );
            // Even a malformed direct child message fails before FFI: a null
            // owner here would crash if verified_native reached native code.
            let (_dir, file) = readonly_fixture();
            let fd: OwnedFd = file.into();
            assert!(verified_native(std::ptr::null_mut(), &fd, 12, sql).is_err());
        }
        for sql in [
            "SELECT * FROM read_parquet('/dev/fd/12')",
            "DESCRIBE SELECT * FROM read_parquet('/dev/fd/12')",
            "INSERT INTO target SELECT * FROM read_parquet('/dev/fd/12')",
            "CREATE TABLE target AS SELECT * FROM read_parquet('/dev/fd/12')",
            "SELECT 'VIEW', 'escaped '' MACRO', \"view\" FROM read_parquet('/dev/fd/12') /* PREPARE */",
        ] {
            assert!(
                descriptor_query(12, 34, sql).is_ok(),
                "eager SQL refused: {sql}"
            );
        }
    }
    #[test]
    fn refused_lazy_definition_leaves_no_native_catalog_entry_or_transaction_effect() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = NativeEngine::open(&dir.path().join("lazy.duckdb")).unwrap();
        engine.metadata_query("BEGIN").unwrap();
        engine
            .metadata_query("CREATE TABLE eager (id BIGINT)")
            .unwrap();
        engine.authorize_relation("main", "eager").unwrap();
        let (_source, source) = readonly_fixture();
        let file: OwnedFd = source.into();
        verify_file_descriptor(file.as_raw_fd()).unwrap();
        for sql in [
            "CREATE VIEW stale AS SELECT * FROM read_parquet('/dev/fd/12')",
            "CREATE MACRO stale_macro() AS TABLE SELECT * FROM read_parquet('/dev/fd/12')",
            "PREPARE stale_prepared AS SELECT * FROM read_parquet('/dev/fd/12')",
        ] {
            assert!(verified_native(engine.owner, &file, 12, sql).is_err());
        }
        // The received integer can be recycled after drop, but no saved view
        // was ever installed that could authorize its next occupant.
        drop(file);
        assert!(!engine.relation_exists("main", "stale").unwrap());
        engine
            .metadata_query("INSERT INTO eager VALUES (42)")
            .unwrap();
        assert_eq!(
            engine.metadata_query("SELECT id FROM eager").unwrap(),
            vec![vec![Some("42".into())]]
        );
        engine.metadata_query("ROLLBACK").unwrap();
        assert!(!engine.relation_exists("main", "eager").unwrap());
    }
    #[test]
    fn received_readwrite_and_nonregular_descriptors_are_refused() {
        let (dir, file) = readonly_fixture();
        let writable = OpenOptions::new()
            .read(true)
            .write(true)
            .open(dir.path().join("pinned"))
            .unwrap();
        let (reader, writer) = descriptor_pair();
        for fd in [writable.as_raw_fd(), writer.as_raw_fd()] {
            send_descriptor(&writer, fd, descriptor_deadline(), &AtomicBool::new(false)).unwrap();
            let owned = receive_descriptor(&reader, descriptor_deadline(), &AtomicBool::new(false))
                .unwrap();
            assert!(verify_file_descriptor(owned.as_raw_fd()).is_err());
            drop(owned);
        }
        assert_eq!(source_descriptor_count(&file), 2);
        drop(writable);
        assert_eq!(source_descriptor_count(&file), 1);
    }
    #[test]
    fn descriptor_remap_is_exact_bounded_and_does_not_widen_authority() {
        let sql = "SELECT 12, '/dev/fd/12', '/dev/fd/12'";
        assert_eq!(
            descriptor_query(12, 34, sql).unwrap().to_str().unwrap(),
            "SELECT 12, '/dev/fd/34', '/dev/fd/34'"
        );
        for sql in [
            "SELECT 1",
            "SELECT '/dev/fd/123'",
            "SELECT '/dev/fd/12', '/dev/fd/34'",
            "SELECT '/dev/fd/12', '/dev/fd/12suffix'",
        ] {
            assert!(descriptor_query(12, 34, sql).is_err());
        }
        assert!(descriptor_query(-1, 34, "SELECT '/dev/fd/-1'").is_err());
        assert!(descriptor_query(12, 34, "SELECT '/dev/fd/12', '\0'").is_err());
        let mut maximal = "'/dev/fd/1'".to_owned();
        maximal.push_str(&" ".repeat(ARG_LIMIT - maximal.len()));
        assert!(descriptor_query(1, 1, &maximal).is_ok());
        assert!(descriptor_query(1, 123, &maximal).is_err());
        maximal.push(' ');
        assert!(descriptor_query(1, 1, &maximal).is_err());
    }
    #[test]
    fn descriptor_receive_and_send_obey_original_deadline_and_cancellation() {
        let (reader, writer) = descriptor_pair();
        let (_dir, file) = readonly_fixture();
        let start = Instant::now();
        let error = receive_descriptor(
            &reader,
            start + Duration::from_millis(35),
            &AtomicBool::new(false),
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(start.elapsed() < Duration::from_millis(150));
        assert!(
            send_descriptor(&writer, file.as_raw_fd(), start, &AtomicBool::new(false)).is_err()
        );
        assert!(
            send_descriptor(
                &writer,
                file.as_raw_fd(),
                descriptor_deadline(),
                &AtomicBool::new(true)
            )
            .is_err()
        );
        send_descriptor(
            &writer,
            file.as_raw_fd(),
            descriptor_deadline(),
            &AtomicBool::new(false),
        )
        .unwrap();
        assert!(
            receive_descriptor(&reader, descriptor_deadline(), &AtomicBool::new(true)).is_err()
        );
        let owned =
            receive_descriptor(&reader, descriptor_deadline(), &AtomicBool::new(false)).unwrap();
        drop(owned);
        assert_eq!(source_descriptor_count(&file), 1);
    }
    #[test]
    fn watchdog_plain_peek_does_not_steal_or_leak_queued_rights() {
        // The production watchdog may _exit on a missed deadline. Re-exec
        // only this test so a scheduling stall cannot kill the unit-test suite.
        const CHILD_FLAG: &str = "GRV_DESCRIPTOR_PEEK_TEST_CHILD";
        if std::env::var_os(CHILD_FLAG).is_none() {
            let mut child = Command::new(std::env::current_exe().unwrap())
                .arg("watchdog_plain_peek_does_not_steal_or_leak_queued_rights")
                .arg("--test-threads=1")
                .env(CHILD_FLAG, "1")
                .spawn()
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                if let Some(status) = child.try_wait().unwrap() {
                    assert!(
                        status.success(),
                        "watchdog peek subprocess failed: {status}"
                    );
                    return;
                }
                if Instant::now() >= deadline {
                    child.kill().unwrap();
                    let _ = child.wait();
                    panic!("watchdog peek subprocess exceeded deadline");
                }
                thread::sleep(SLICE);
            }
        }
        let (reader, writer) = descriptor_pair();
        let watchdog = ChildWatchdog::start(&reader).unwrap();
        watchdog.configure(descriptor_deadline());
        let (_dir, file) = readonly_fixture();
        send_descriptor(
            &writer,
            file.as_raw_fd(),
            descriptor_deadline(),
            &AtomicBool::new(false),
        )
        .unwrap();
        // Exercise the watchdog's exact syscall deterministically as well as
        // allowing its active thread to observe the queued marker repeatedly.
        for _ in 0..64 {
            let mut marker = 0u8;
            assert_eq!(
                unsafe {
                    libc::recv(
                        reader.as_raw_fd(),
                        (&mut marker as *mut u8).cast(),
                        1,
                        libc::MSG_PEEK | libc::MSG_DONTWAIT,
                    )
                },
                1
            );
            assert_eq!(marker, DESCRIPTOR_MARKER);
        }
        thread::sleep(SLICE * 2);
        assert_eq!(source_descriptor_count(&file), 1);
        let owned =
            receive_descriptor(&reader, descriptor_deadline(), &AtomicBool::new(false)).unwrap();
        verify_file_descriptor(owned.as_raw_fd()).unwrap();
        drop(owned);
        assert_eq!(source_descriptor_count(&file), 1);
        drop(watchdog);
    }
    #[test]
    fn raw_response_tags_and_empty_bytes_roundtrip() {
        for (tag, response) in [
            (0, Response::Failed),
            (1, Response::Ok(Reply::Done)),
            (2, Response::Ok(Reply::Bool(false))),
            (3, Response::Ok(Reply::Bool(true))),
            (4, Response::Ok(Reply::Bytes(Vec::new()))),
        ] {
            let (mut reader, mut writer) = descriptor_pair();
            let deadline = descriptor_deadline();
            let cancelled = AtomicBool::new(false);
            send_response(&mut writer, &response, deadline, &cancelled).unwrap();
            let mut wire = [0; 5];
            transfer(&mut reader, &mut wire, false, deadline, &cancelled).unwrap();
            assert_eq!(wire, [0, 0, 0, 1, tag]);
            write_response_bytes(&mut writer, &wire, deadline, &cancelled).unwrap();
            let decoded = receive_response(&mut reader, deadline, &cancelled).unwrap();
            let decoded_tag = match decoded {
                Response::Failed => 0,
                Response::Ok(Reply::Done) => 1,
                Response::Ok(Reply::Bool(false)) => 2,
                Response::Ok(Reply::Bool(true)) => 3,
                Response::Ok(Reply::Bytes(bytes)) => {
                    assert!(bytes.is_empty());
                    4
                }
            };
            assert_eq!(decoded_tag, tag);
        }
    }
    #[test]
    fn raw_bytes_exact_two_mib_use_one_receive_allocation() {
        assert_eq!(BYTE_LIMIT, 2 * 1024 * 1024);
        let (mut reader, mut writer) = descriptor_pair();
        let deadline = Instant::now() + Duration::from_secs(5);
        let (retained, overlap) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        let sender = thread::spawn(move || {
            let response = Response::Ok(Reply::Bytes(vec![255; BYTE_LIMIT]));
            send_response(&mut writer, &response, deadline, &AtomicBool::new(false)).unwrap();
            let Response::Ok(Reply::Bytes(payload)) = &response else {
                unreachable!()
            };
            retained.send(payload.capacity()).unwrap();
            released.recv_timeout(Duration::from_secs(3)).unwrap();
            drop(response);
        });
        let Response::Ok(Reply::Bytes(bytes)) =
            receive_response(&mut reader, deadline, &AtomicBool::new(false)).unwrap()
        else {
            panic!("wrong raw response")
        };
        assert_eq!(bytes.len(), BYTE_LIMIT);
        assert_eq!(bytes.capacity(), BYTE_LIMIT);
        assert!(bytes.iter().all(|&byte| byte == 255));
        assert_eq!(
            overlap.recv_timeout(Duration::from_secs(3)).unwrap() + bytes.capacity(),
            2 * BYTE_LIMIT
        );
        release.send(()).unwrap();
        sender.join().unwrap();
    }
    #[test]
    fn raw_response_rejects_missing_unknown_tags_bad_bools_and_oversize() {
        for (count, body) in [
            (0, &[][..]),
            (1, &[][..]), // missing tag / EOF
            (1, &[5][..]),
            (2, &[0][..]),    // Failed cannot carry a payload
            (2, &[1][..]),    // Done cannot carry a payload
            (2, &[2, 0][..]), // false cannot carry a boolean byte
            (2, &[3, 1][..]), // true cannot carry a boolean byte
            (BYTE_LIMIT + 2, &[4][..]),
        ] {
            let (mut reader, mut writer) = descriptor_pair();
            writer.write_all(&(count as u32).to_be_bytes()).unwrap();
            writer.write_all(body).unwrap();
            writer.shutdown(std::net::Shutdown::Write).unwrap();
            assert!(
                receive_response(&mut reader, descriptor_deadline(), &AtomicBool::new(false))
                    .is_err(),
                "accepted count {count}, body {body:?}"
            );
        }
        // Oversized lengths fail before reading even the tag, not on EOF.
        let (mut reader, mut writer) = descriptor_pair();
        writer
            .write_all(&((BYTE_LIMIT + 2) as u32).to_be_bytes())
            .unwrap();
        let error = receive_response(&mut reader, descriptor_deadline(), &AtomicBool::new(false))
            .err()
            .unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(
            send_response(
                &mut writer,
                &Response::Ok(Reply::Bytes(vec![0; BYTE_LIMIT + 1])),
                descriptor_deadline(),
                &AtomicBool::new(false)
            )
            .is_err()
        );
        let mut byte = [0];
        assert_eq!(
            reader.read(&mut byte).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    }
    #[test]
    fn message_json_retains_fixed_frame_cap() {
        let mut writer = CappedBuffer(Zeroizing::new(Vec::with_capacity(FRAME_LIMIT)));
        serde_json::to_writer(&mut writer, &Message::Call(Request::Close)).unwrap();
        assert_eq!(writer.0.capacity(), FRAME_LIMIT);
        assert!(writer.write(&vec![0; FRAME_LIMIT]).is_err());
        let (mut reader, mut peer) = descriptor_pair();
        peer.write_all(&((FRAME_LIMIT + 1) as u32).to_be_bytes())
            .unwrap();
        assert!(
            receive::<Message>(&mut reader, descriptor_deadline(), &AtomicBool::new(false))
                .is_err()
        );
    }
    #[test]
    fn fragmented_raw_response_preserves_frame_boundaries() {
        let (mut reader, mut writer) = descriptor_pair();
        let deadline = descriptor_deadline();
        let sender = thread::spawn(move || {
            for byte in [0, 0, 0, 4, 4, 0, 255, 128, 0, 0, 0, 1, 3] {
                write_response_bytes(&mut writer, &[byte], deadline, &AtomicBool::new(false))
                    .unwrap();
                thread::sleep(Duration::from_millis(1));
            }
        });
        assert!(matches!(
            receive_response(&mut reader, deadline, &AtomicBool::new(false)).unwrap(),
            Response::Ok(Reply::Bytes(bytes)) if bytes == [0, 255, 128]
        ));
        assert!(matches!(
            receive_response(&mut reader, deadline, &AtomicBool::new(false)).unwrap(),
            Response::Ok(Reply::Bool(true))
        ));
        sender.join().unwrap();
    }
    #[test]
    fn partial_raw_frame_obeys_absolute_deadline() {
        for prefix in [&[0, 0][..], &[0, 0, 0, 1][..], &[0, 0, 0, 4, 4, 7][..]] {
            let (mut reader, mut writer) = descriptor_pair();
            writer.write_all(prefix).unwrap();
            let start = Instant::now();
            let error = receive_response(
                &mut reader,
                start + Duration::from_millis(35),
                &AtomicBool::new(false),
            )
            .err()
            .unwrap();
            assert_eq!(error.kind(), io::ErrorKind::TimedOut);
            assert!(start.elapsed() < Duration::from_millis(150));
        }
    }
    #[test]
    fn raw_response_cancellation_interrupts_partial_payload_and_blocked_write() {
        for writing in [false, true] {
            let (mut reader, mut writer) = descriptor_pair();
            if !writing {
                writer.write_all(&[0, 0, 0, 4, 4, 7]).unwrap();
            }
            let cancelled = Arc::new(AtomicBool::new(false));
            let shared = Arc::clone(&cancelled);
            let cancel = thread::spawn(move || {
                thread::sleep(Duration::from_millis(35));
                shared.store(true, Ordering::Release);
            });
            let start = Instant::now();
            let deadline = start + Duration::from_secs(1);
            let error = if writing {
                send_response(
                    &mut writer,
                    &Response::Ok(Reply::Bytes(vec![0; BYTE_LIMIT])),
                    deadline,
                    &cancelled,
                )
                .err()
                .unwrap()
            } else {
                receive_response(&mut reader, deadline, &cancelled)
                    .err()
                    .unwrap()
            };
            assert_eq!(error.kind(), io::ErrorKind::TimedOut);
            assert!(start.elapsed() < Duration::from_millis(250));
            cancel.join().unwrap();
        }
    }
    #[test]
    fn raw_response_send_checks_expiry_and_cancellation_before_header() {
        for cancelled in [false, true] {
            let (mut reader, mut writer) = descriptor_pair();
            let deadline = if cancelled {
                descriptor_deadline()
            } else {
                Instant::now()
            };
            let error = send_response(
                &mut writer,
                &Response::Ok(Reply::Done),
                deadline,
                &AtomicBool::new(cancelled),
            )
            .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::TimedOut);
            let mut byte = [0];
            assert_eq!(
                reader.read(&mut byte).unwrap_err().kind(),
                io::ErrorKind::WouldBlock
            );
        }
    }
    #[test]
    fn raw_response_blocked_write_obeys_absolute_deadline() {
        let (_reader, mut writer) = descriptor_pair();
        let start = Instant::now();
        let error = send_response(
            &mut writer,
            &Response::Ok(Reply::Bytes(vec![0; BYTE_LIMIT])),
            start + Duration::from_millis(35),
            &AtomicBool::new(false),
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(start.elapsed() < Duration::from_millis(150));
    }
    #[test]
    fn untrusted_helper_is_rejected() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("helper");
        fs::write(&path, b"#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        assert!(validate_helper(&path, Path::new("/not-current")).is_err());
        fs::write(&path, b"\x7fELFplaceholder").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o777)).unwrap();
        assert!(validate_helper(&path, Path::new("/not-current")).is_err());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        assert!(validate_helper(&path, &fs::canonicalize(&path).unwrap()).is_err());
    }
    fn supervised_sleeper(deadline: Instant) -> ProcessOwner {
        let mut command = Command::new("/bin/sleep");
        command
            .arg("30")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) != 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let child = command.spawn().unwrap();
        let supervisor = Arc::new(Supervisor {
            state: Mutex::new(State {
                stop: false,
                done: None,
                deadline: Some(deadline),
            }),
            wake: Condvar::new(),
            cancelled: Arc::new(AtomicBool::new(false)),
            lock: LockFence {
                file: ManuallyDrop::new(tempfile::tempfile().unwrap()),
                proved: AtomicBool::new(false),
            },
        });
        let shared = Arc::clone(&supervisor);
        let watcher = thread::spawn(move || watch(child, shared, deadline));
        let (stream, _) = UnixStream::pair().unwrap();
        ProcessOwner {
            stream,
            supervisor,
            watcher: Some(watcher),
            deadline,
        }
    }
    #[test]
    fn watchdog_fences_idle_owner_at_original_deadline() {
        let start = Instant::now();
        let mut owner = supervised_sleeper(start + Duration::from_millis(45));
        owner.watcher.take().unwrap().join().unwrap();
        assert!(owner.cancelled().load(Ordering::Acquire));
        owner.close().unwrap();
        assert!(start.elapsed() < Duration::from_millis(300));
    }
    #[test]
    fn cancellation_fences_idle_owner() {
        let start = Instant::now();
        let mut owner = supervised_sleeper(start + Duration::from_secs(30));
        owner.cancelled().store(true, Ordering::Release);
        owner.watcher.take().unwrap().join().unwrap();
        owner.close().unwrap();
        assert!(start.elapsed() < Duration::from_millis(300));
    }
    #[test]
    fn cancellation_interrupts_partial_read() {
        let (mut reader, _writer) = UnixStream::pair().unwrap();
        socket_setup(&reader).unwrap();
        assert!(
            receive_response(
                &mut reader,
                Instant::now() + Duration::from_secs(1),
                &AtomicBool::new(true)
            )
            .is_err()
        );
    }
}
