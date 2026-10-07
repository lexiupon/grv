//! GRV Storage v2 backend primitives and coordination records.
//! Ownership tokens live here, and are not dependencies of adapter crates.
mod json;
mod local;
pub mod model;
pub use local::{FaultInjector, FaultPoint, LocalBackend};
#[cfg(feature = "cloud")]
pub mod cloud;
use serde::{Deserialize, Deserializer, Serialize};
use std::{
    fmt,
    io::{Read, Write},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    NotFound,
    PreconditionFailed,
    InvalidKey,
    InvalidRecord,
    Integrity,
    Unsupported,
    Io,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteEffect {
    NoEffect,
    MaybeApplied,
}
#[derive(Debug)]
pub struct Error {
    pub kind: ErrorKind,
    pub effect: WriteEffect,
    pub message: String,
    source: Option<std::io::Error>,
}
impl Error {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            effect: WriteEffect::NoEffect,
            message: message.into(),
            source: None,
        }
    }
    pub(crate) fn io(error: std::io::Error, effect: WriteEffect) -> Self {
        Self {
            kind: if error.kind() == std::io::ErrorKind::NotFound {
                ErrorKind::NotFound
            } else {
                ErrorKind::Io
            },
            effect,
            message: error.to_string(),
            source: Some(error),
        }
    }
    pub(crate) fn applied(mut self) -> Self {
        self.effect = WriteEffect::MaybeApplied;
        self
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|e| e as &(dyn std::error::Error + 'static))
    }
}
pub type Result<T> = std::result::Result<T, Error>;

fn valid_relative(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with('/')
        && !value.contains(['\0', '\\'])
        && value
            .split('/')
            .all(|s| !s.is_empty() && s != "." && s != ".." && !s.starts_with(".tmp-"))
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct ObjectKey(String);
impl ObjectKey {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if valid_relative(&value) {
            Ok(Self(value))
        } else {
            Err(Error::new(
                ErrorKind::InvalidKey,
                "object key must be a canonical relative path",
            ))
        }
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl<'de> Deserialize<'de> for ObjectKey {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        Self::new(String::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectPrefix(String);
impl ObjectPrefix {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.ends_with('/') && valid_relative(&value[..value.len() - 1]) {
            Ok(Self(value))
        } else {
            Err(Error::new(
                ErrorKind::InvalidKey,
                "object prefix must end in one slash",
            ))
        }
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct Validator(String);
impl Validator {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if !value.is_empty() {
            Ok(Self(value))
        } else {
            Err(Error::new(
                ErrorKind::InvalidRecord,
                "empty object validator",
            ))
        }
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl<'de> Deserialize<'de> for Validator {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        Self::new(String::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub validator: Validator,
    pub size: model::Counter,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListMode {
    Recursive,
    Children,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListEntry {
    Object(ObjectKey),
    Prefix(ObjectPrefix),
}

/// Object reads stream into consumer-owned sinks. Successful get/head include
/// durable read-back; listings are discovery only. Failed writes explicitly
/// distinguish pre-install failures from effects that require durable proof.
pub trait Backend: Send + Sync {
    /// Credential-free coordinates for an exact S3 object. This is a pure
    /// locator operation: it conveys no read authority, signed URL or secret.
    /// The core must verify the object before passing it to an adapter reader.
    fn s3_data_uri(&self, _key: &ObjectKey) -> Result<Option<String>> {
        Ok(None)
    }
    fn get(&self, key: &ObjectKey, sink: &mut dyn Write) -> Result<ObjectMeta>;
    /// Read exactly one conditional byte range (1..=64 KiB). The returned
    /// metadata describes the complete object, not the range. Success proves
    /// its current validator equals `expected`; failed/precondition reads do
    /// not authorize reuse or silently retry. Unsupported implementations may
    /// conservatively fall back to an ordinary full-content verification.
    fn read_range(
        &self,
        _key: &ObjectKey,
        _expected: &Validator,
        _offset: u64,
        _length: usize,
        _sink: &mut dyn Write,
    ) -> Result<ObjectMeta> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "conditional range reads are unsupported",
        ))
    }
    fn head(&self, key: &ObjectKey) -> Result<ObjectMeta>;
    fn list(&self, prefix: &ObjectPrefix, mode: ListMode) -> Result<Vec<ListEntry>>;
    fn delete(&self, key: &ObjectKey) -> Result<()>;
    fn conditional_create(&self, key: &ObjectKey, source: &mut dyn Read) -> Result<Validator>;
    fn conditional_put(
        &self,
        key: &ObjectKey,
        expected: &Validator,
        source: &mut dyn Read,
    ) -> Result<Validator>;
    fn create_bytes(&self, key: &ObjectKey, bytes: &[u8]) -> Result<Validator> {
        self.conditional_create(key, &mut std::io::Cursor::new(bytes))
    }
    fn put_bytes(&self, key: &ObjectKey, expected: &Validator, bytes: &[u8]) -> Result<Validator> {
        self.conditional_put(key, expected, &mut std::io::Cursor::new(bytes))
    }
    fn read_bytes(&self, key: &ObjectKey, limit: usize) -> Result<(Vec<u8>, ObjectMeta)> {
        struct Bounded {
            bytes: Vec<u8>,
            limit: usize,
        }
        impl Write for Bounded {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
                    return Err(std::io::Error::other(
                        "object exceeds consumer metadata budget",
                    ));
                }
                self.bytes.extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut sink = Bounded {
            bytes: vec![],
            limit,
        };
        let meta = self.get(key, &mut sink)?;
        Ok((sink.bytes, meta))
    }
}

impl<B: Backend + ?Sized> Backend for Box<B> {
    fn s3_data_uri(&self, key: &ObjectKey) -> Result<Option<String>> {
        (**self).s3_data_uri(key)
    }
    fn get(&self, key: &ObjectKey, sink: &mut dyn Write) -> Result<ObjectMeta> {
        (**self).get(key, sink)
    }
    fn read_range(
        &self,
        key: &ObjectKey,
        expected: &Validator,
        offset: u64,
        length: usize,
        sink: &mut dyn Write,
    ) -> Result<ObjectMeta> {
        (**self).read_range(key, expected, offset, length, sink)
    }
    fn head(&self, key: &ObjectKey) -> Result<ObjectMeta> {
        (**self).head(key)
    }
    fn list(&self, prefix: &ObjectPrefix, mode: ListMode) -> Result<Vec<ListEntry>> {
        (**self).list(prefix, mode)
    }
    fn delete(&self, key: &ObjectKey) -> Result<()> {
        (**self).delete(key)
    }
    fn conditional_create(&self, key: &ObjectKey, source: &mut dyn Read) -> Result<Validator> {
        (**self).conditional_create(key, source)
    }
    fn conditional_put(
        &self,
        key: &ObjectKey,
        expected: &Validator,
        source: &mut dyn Read,
    ) -> Result<Validator> {
        (**self).conditional_put(key, expected, source)
    }
}
