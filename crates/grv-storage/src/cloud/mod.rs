//! Conditional cloud backends. Provider errors and signed URLs are private;
//! only sanitized operation errors cross the storage interface.
mod credentials;
mod gcs;
#[cfg(test)]
mod mock;
mod s3;
use crate::{
    Backend, Error, ErrorKind, ListEntry, ListMode, ObjectKey, ObjectMeta, ObjectPrefix, Result,
    Validator, WriteEffect, model::Counter,
};
use futures_util::StreamExt;
use object_store::{
    ObjectStore, ObjectStoreExt, RetryConfig,
    aws::{AmazonS3, AmazonS3Builder, S3ConditionalPut},
    client::ClientOptions,
    path::Path as RemotePath,
    signer::Url,
};
use std::{
    fmt,
    io::{Read, Write},
    sync::Arc,
    time::Duration,
};
use tokio::runtime::Runtime;

const PART_BYTES: usize = 8 * 1024 * 1024;
const RESPONSE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    S3,
    Gcs,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudRoot {
    pub scheme: Scheme,
    pub bucket: String,
    prefix: String,
    canonical: String,
}
impl CloudRoot {
    pub fn parse(root: &str) -> Result<Self> {
        let url = Url::parse(root)
            .map_err(|_| Error::new(ErrorKind::InvalidKey, "invalid cloud root"))?;
        let scheme = match url.scheme() {
            "s3" => Scheme::S3,
            "gs" => Scheme::Gcs,
            _ => {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "unsupported cloud scheme",
                ));
            }
        };
        let bucket = url
            .host_str()
            .ok_or_else(|| Error::new(ErrorKind::InvalidKey, "cloud bucket required"))?;
        if !url.username().is_empty()
            || url.password().is_some()
            || url.port().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || !(3..=222).contains(&bucket.len())
            || bucket.starts_with(['-', '.'])
            || bucket.ends_with(['-', '.'])
            || !bucket
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
            || (scheme == Scheme::S3 && bucket.len() > 63)
        {
            return Err(Error::new(
                ErrorKind::InvalidKey,
                "invalid cloud bucket coordinates",
            ));
        }
        let raw_path = root
            .split_once("://")
            .and_then(|(_, rest)| rest.split_once('/'))
            .map_or("", |(_, path)| path);
        let decoded = RemotePath::from_url_path(raw_path)
            .map_err(|_| Error::new(ErrorKind::InvalidKey, "invalid cloud prefix"))?;
        let prefix = decoded.as_ref().to_string();
        if !prefix.is_empty() {
            ObjectKey::new(prefix.clone())?;
        }
        let mut canonical_url = url.clone();
        {
            let mut parts = canonical_url
                .path_segments_mut()
                .map_err(|_| Error::new(ErrorKind::InvalidKey, "invalid cloud root path"))?;
            parts.clear();
            if !prefix.is_empty() {
                for segment in prefix.split('/') {
                    parts.push(segment);
                }
            }
        }
        let canonical = canonical_url.to_string().trim_end_matches('/').to_owned();
        let canonical = if scheme == Scheme::S3 {
            canonical
                .replace('*', "%2A")
                .replace('[', "%5B")
                .replace(']', "%5D")
        } else {
            canonical
        };
        Ok(Self {
            scheme,
            bucket: bucket.into(),
            prefix,
            canonical,
        })
    }
    pub fn canonical(&self) -> &str {
        &self.canonical
    }
    /// Locate one object without opening credentials or contacting the provider.
    pub fn s3_data_uri(&self, key: &ObjectKey) -> Result<Option<String>> {
        if self.scheme != Scheme::S3 {
            return Ok(None);
        }
        let path = self.path(key.as_str())?;
        let mut uri = Url::parse(&format!("s3://{}", self.bucket))
            .map_err(|_| Error::new(ErrorKind::InvalidKey, "invalid S3 bucket"))?;
        {
            let mut segments = uri
                .path_segments_mut()
                .map_err(|_| Error::new(ErrorKind::InvalidKey, "invalid S3 object path"))?;
            segments.clear();
            for segment in path.as_ref().split('/') {
                segments.push(segment);
            }
        }
        // These bytes have glob meaning in readers even inside an otherwise
        // valid object key. Keep the URI exact by percent-encoding them.
        let uri = uri
            .to_string()
            .replace('*', "%2A")
            .replace('[', "%5B")
            .replace(']', "%5D");
        Ok(Some(uri))
    }
    fn path(&self, relative: &str) -> Result<RemotePath> {
        let path = if self.prefix.is_empty() {
            relative.to_owned()
        } else if relative.is_empty() {
            self.prefix.clone()
        } else {
            format!("{}/{relative}", self.prefix)
        };
        RemotePath::parse(path)
            .map_err(|_| Error::new(ErrorKind::InvalidKey, "invalid cloud object path"))
    }
    fn relative(&self, path: &RemotePath) -> Result<String> {
        let path = path.as_ref();
        if self.prefix.is_empty() {
            Ok(path.into())
        } else {
            path.strip_prefix(&format!("{}/", self.prefix))
                .map(String::from)
                .ok_or_else(|| Error::new(ErrorKind::Integrity, "cloud listing escaped its root"))
        }
    }
}

#[derive(Clone)]
pub struct CloudOptions {
    pub profile: Option<String>,
    pub region: Option<String>,
    pub gcs_account: Option<String>,
    pub gcs_project: Option<String>,
    pub timeout: Duration,
}
impl Default for CloudOptions {
    fn default() -> Self {
        Self {
            profile: std::env::var("AWS_PROFILE").ok(),
            region: std::env::var("AWS_REGION")
                .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
                .ok(),
            gcs_account: std::env::var("GRV_GCS_ACCOUNT").ok(),
            gcs_project: std::env::var("GRV_GCS_PROJECT").ok(),
            timeout: Duration::from_secs(120),
        }
    }
}
impl fmt::Debug for CloudOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CloudOptions")
            .field("profile", &"[private]")
            .field("gcs_account", &"[private]")
            .field("timeout", &self.timeout)
            .finish()
    }
}
trait ConditionalUpload: Send + Sync {
    fn upload(
        &self,
        key: &RemotePath,
        expected: Option<&Validator>,
        source: &mut dyn Read,
    ) -> Result<Validator>;
}

/// A single signed, conditional GET. Range/condition headers are signed and
/// redirects/retries are disabled: coherence failure is never retried as a new
/// object generation. No provider response body or signed URL is exposed.
struct S3RangeRead {
    store: AmazonS3,
    http: reqwest::Client,
    timeout: Duration,
}
fn range_end(offset: u64, length: usize) -> Result<u64> {
    if !(1..=64 * 1024).contains(&length) {
        return Err(Error::new(
            ErrorKind::InvalidRecord,
            "range length must be 1..=65536",
        ));
    }
    offset
        .checked_add(length as u64 - 1)
        .ok_or_else(|| Error::new(ErrorKind::InvalidRecord, "range offset overflow"))
}
fn range_metadata(
    headers: &reqwest::header::HeaderMap,
    expected: &Validator,
    offset: u64,
    length: usize,
) -> Result<ObjectMeta> {
    let invalid = || {
        Error::new(
            ErrorKind::Integrity,
            "S3 conditional range response is inconsistent",
        )
    };
    let one = |name: &str| -> Result<&str> {
        let mut values = headers.get_all(name).iter();
        let value = values.next().ok_or_else(invalid)?;
        if values.next().is_some() {
            return Err(invalid());
        }
        value.to_str().map_err(|_| invalid())
    };
    if one("etag")? != expected.as_str() || one("content-length")? != length.to_string() {
        return Err(invalid());
    }
    if let Some(encoding) = headers.get("content-encoding")
        && encoding != "identity"
    {
        return Err(invalid());
    }
    let (span, total) = one("content-range")?
        .strip_prefix("bytes ")
        .and_then(|v| v.split_once('/'))
        .ok_or_else(invalid)?;
    let total = total.parse::<u64>().map_err(|_| invalid())?;
    let end = range_end(offset, length)?;
    if span != format!("{offset}-{end}") || total <= end {
        return Err(invalid());
    }
    Ok(ObjectMeta {
        validator: expected.clone(),
        size: Counter::new(total)?,
    })
}
impl S3RangeRead {
    fn new(store: AmazonS3, timeout: Duration) -> Result<Self> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(timeout)
            .https_only(true)
            .build()
            .map_err(|_| Error::new(ErrorKind::Io, "cloud range client initialization failed"))?;
        Ok(Self {
            store,
            http,
            timeout,
        })
    }
    async fn read(
        &self,
        key: &RemotePath,
        expected: &Validator,
        offset: u64,
        length: usize,
        sink: &mut dyn Write,
    ) -> Result<ObjectMeta> {
        use object_store::signer::{HeaderName, HeaderValue, Method, SignedUrlOptions, Signer};
        let end = range_end(offset, length)?;
        let condition = HeaderValue::from_str(expected.as_str()).map_err(|_| {
            Error::new(ErrorKind::InvalidRecord, "invalid S3 conditional validator")
        })?;
        let range = HeaderValue::from_str(&format!("bytes={offset}-{end}"))
            .map_err(|_| Error::new(ErrorKind::InvalidRecord, "invalid range"))?;
        let options = SignedUrlOptions::new()
            .with_signed_header(HeaderName::from_static("if-match"), condition.clone())
            .with_signed_header(HeaderName::from_static("range"), range.clone());
        let url = self
            .store
            .signed_url_opts(
                Method::GET,
                key,
                self.timeout + Duration::from_secs(60),
                &options,
            )
            .await
            .map_err(|e| remote_error(e, false))?;
        let mut response = self
            .http
            .get(url)
            .header("if-match", condition)
            .header("range", range)
            .header("accept-encoding", "identity")
            .send()
            .await
            .map_err(|_| Error::new(ErrorKind::Io, "S3 conditional range request failed"))?;
        match response.status().as_u16() {
            206 => {}
            404 => {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    "S3 range object is missing",
                ));
            }
            412 => {
                return Err(Error::new(
                    ErrorKind::PreconditionFailed,
                    "S3 range object changed",
                ));
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::Io,
                    "S3 conditional range request was rejected",
                ));
            }
        }
        let meta = range_metadata(response.headers(), expected, offset, length)?;
        // Buffer at most one admitted 64-KiB unit before exposing any bytes to
        // the caller; an oversized/truncated response cannot contaminate it.
        let mut bytes = Vec::with_capacity(length);
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| Error::new(ErrorKind::Io, "S3 range response failed"))?
        {
            if chunk.len() > length - bytes.len() {
                return Err(Error::new(
                    ErrorKind::Integrity,
                    "S3 range response is oversized",
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() != length {
            return Err(Error::new(
                ErrorKind::Integrity,
                "S3 range response is truncated",
            ));
        }
        sink.write_all(&bytes).map_err(io_error)?;
        Ok(meta)
    }
}

pub struct CloudBackend {
    root: CloudRoot,
    store: Arc<dyn ObjectStore>,
    runtime: Arc<Runtime>,
    writer: Arc<dyn ConditionalUpload>,
    ranges: Option<S3RangeRead>,
}
impl fmt::Debug for CloudBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CloudBackend")
            .field("root", &self.root.canonical())
            .finish_non_exhaustive()
    }
}
impl CloudBackend {
    pub fn open(root: &str, options: CloudOptions) -> Result<Self> {
        let root = CloudRoot::parse(root)?;
        if options.timeout.is_zero() || options.timeout > Duration::from_secs(3600) {
            return Err(Error::new(
                ErrorKind::InvalidRecord,
                "cloud timeout must be positive and bounded",
            ));
        }
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|_| Error::new(ErrorKind::Io, "cloud runtime initialization failed"))?,
        );
        let retry = RetryConfig {
            max_retries: 0,
            ..Default::default()
        };
        let clients = ClientOptions::new()
            .with_allow_http(false)
            .with_timeout(options.timeout);
        match root.scheme {
            Scheme::S3 => {
                let mut builder = AmazonS3Builder::from_env()
                    .with_bucket_name(&root.bucket)
                    .with_retry(retry)
                    .with_client_options(clients.clone())
                    .with_conditional_put(S3ConditionalPut::ETagMatch);
                if let Some(profile) = options.profile {
                    let credentials = Arc::new(credentials::Profile::new(profile));
                    let region = match options.region {
                        Some(region) => Some(region),
                        None => credentials.region().map_err(|e| remote_error(e, false))?,
                    };
                    let region = match region {
                        Some(region) => region,
                        None => runtime
                            .block_on(object_store::aws::resolve_bucket_region(
                                &root.bucket,
                                &clients,
                            ))
                            .map_err(|e| remote_error(e, false))?,
                    };
                    builder = s3_endpoint(builder, &region)?;
                    builder = builder.with_credentials(credentials);
                } else {
                    let region = match options.region {
                        Some(region) => region,
                        None => runtime
                            .block_on(object_store::aws::resolve_bucket_region(
                                &root.bucket,
                                &clients,
                            ))
                            .map_err(|e| remote_error(e, false))?,
                    };
                    builder = s3_endpoint(builder, &region)?;
                }
                let store = builder.build().map_err(|e| remote_error(e, false))?;
                let writer = s3::S3Upload::new(store.clone(), runtime.clone(), options.timeout)?;
                let ranges = Some(S3RangeRead::new(store.clone(), options.timeout)?);
                Ok(Self {
                    root,
                    store: Arc::new(store),
                    runtime,
                    writer: Arc::new(writer),
                    ranges,
                })
            }
            Scheme::Gcs => {
                let mut headers = reqwest::header::HeaderMap::new();
                if let Some(project) = &options.gcs_project {
                    headers.insert(
                        "x-goog-user-project",
                        reqwest::header::HeaderValue::from_str(project).map_err(|_| {
                            Error::new(ErrorKind::InvalidRecord, "invalid GCS quota project")
                        })?,
                    );
                }
                let mut builder = object_store::gcp::GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(&root.bucket)
                    .with_base_url("https://storage.googleapis.com")
                    .with_skip_signature(false)
                    .with_retry(retry)
                    .with_client_options(clients.with_default_headers(headers));
                if let Some(account) = options.gcs_account {
                    if account.is_empty()
                        || account.len() > 320
                        || account.contains(['\0', '\r', '\n'])
                    {
                        return Err(Error::new(
                            ErrorKind::InvalidRecord,
                            "invalid GCloud account selector",
                        ));
                    }
                    builder = builder.with_credentials(Arc::new(credentials::Gcloud {
                        account,
                        project: options.gcs_project.clone(),
                    }));
                }
                let store = builder.build().map_err(|e| remote_error(e, false))?;
                let writer = gcs::GcsUpload::new(
                    &root.bucket,
                    store.credentials().clone(),
                    runtime.clone(),
                    options.timeout,
                    options.gcs_project.as_deref(),
                )?;
                Ok(Self {
                    root,
                    store: Arc::new(store),
                    runtime,
                    writer: Arc::new(writer),
                    ranges: None,
                })
            }
        }
    }
    pub fn root(&self) -> &CloudRoot {
        &self.root
    }
    fn metadata(&self, meta: object_store::ObjectMeta) -> Result<ObjectMeta> {
        let validator = match self.root.scheme {
            Scheme::S3 => meta.e_tag,
            Scheme::Gcs => meta.version,
        }
        .ok_or_else(|| {
            Error::new(
                ErrorKind::Integrity,
                "cloud object lacks its required validator",
            )
        })?;
        Ok(ObjectMeta {
            validator: Validator::new(validator)?,
            size: Counter::new(meta.size)?,
        })
    }
}
fn s3_endpoint(builder: AmazonS3Builder, region: &str) -> Result<AmazonS3Builder> {
    if region.is_empty()
        || region.len() > 128
        || !region
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Err(Error::new(ErrorKind::InvalidRecord, "invalid AWS region"));
    }
    let domain = if region.starts_with("cn-") {
        "amazonaws.com.cn"
    } else {
        "amazonaws.com"
    };
    Ok(builder
        .with_region(region)
        .with_endpoint(format!("https://s3.{region}.{domain}"))
        .with_virtual_hosted_style_request(false)
        .with_skip_signature(false))
}
pub(super) fn remote_error(error: object_store::Error, write: bool) -> Error {
    let kind = match error {
        object_store::Error::NotFound { .. } => ErrorKind::NotFound,
        object_store::Error::AlreadyExists { .. }
        | object_store::Error::Precondition { .. }
        | object_store::Error::NotModified { .. } => ErrorKind::PreconditionFailed,
        object_store::Error::NotSupported { .. } | object_store::Error::NotImplemented { .. } => {
            ErrorKind::Unsupported
        }
        object_store::Error::InvalidPath { .. } => ErrorKind::InvalidKey,
        _ => ErrorKind::Io,
    };
    let mut error = Error::new(kind, "cloud storage operation failed");
    if write && kind == ErrorKind::Io {
        error.effect = WriteEffect::MaybeApplied;
    }
    error
}
pub(super) fn io_error(_: std::io::Error) -> Error {
    Error::new(ErrorKind::Io, "cloud transfer stream failed")
}
pub(super) fn ambiguous() -> Error {
    Error::new(ErrorKind::Io, "cloud write outcome requires durable proof").applied()
}
pub(super) async fn response_bytes(mut response: reqwest::Response) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    while let Some(bytes) = response.chunk().await.map_err(|_| ambiguous())? {
        if output.len() + bytes.len() > RESPONSE_BYTES {
            return Err(ambiguous());
        }
        output.extend_from_slice(&bytes);
    }
    Ok(output)
}
impl Backend for CloudBackend {
    fn s3_data_uri(&self, key: &ObjectKey) -> Result<Option<String>> {
        self.root.s3_data_uri(key)
    }
    fn get(&self, key: &ObjectKey, sink: &mut dyn Write) -> Result<ObjectMeta> {
        self.runtime.block_on(async {
            let response = self
                .store
                .get(&self.root.path(key.as_str())?)
                .await
                .map_err(|e| remote_error(e, false))?;
            let meta = self.metadata(response.meta.clone())?;
            let mut stream = response.into_stream();
            let mut size = 0u64;
            while let Some(bytes) = stream.next().await {
                let bytes = bytes.map_err(|e| remote_error(e, false))?;
                size = size.checked_add(bytes.len() as u64).ok_or_else(|| {
                    Error::new(ErrorKind::Integrity, "cloud object byte count overflow")
                })?;
                if size > meta.size.get() {
                    return Err(Error::new(
                        ErrorKind::Integrity,
                        "cloud response exceeds object size",
                    ));
                }
                sink.write_all(&bytes).map_err(io_error)?;
            }
            if size != meta.size.get() {
                return Err(Error::new(
                    ErrorKind::Integrity,
                    "cloud response is truncated",
                ));
            }
            Ok(meta)
        })
    }
    fn read_range(
        &self,
        key: &ObjectKey,
        expected: &Validator,
        offset: u64,
        length: usize,
        sink: &mut dyn Write,
    ) -> Result<ObjectMeta> {
        let ranges = self.ranges.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::Unsupported,
                "conditional range reads are unsupported",
            )
        })?;
        self.runtime.block_on(ranges.read(
            &self.root.path(key.as_str())?,
            expected,
            offset,
            length,
            sink,
        ))
    }
    fn head(&self, key: &ObjectKey) -> Result<ObjectMeta> {
        self.runtime.block_on(async {
            self.metadata(
                self.store
                    .head(&self.root.path(key.as_str())?)
                    .await
                    .map_err(|e| remote_error(e, false))?,
            )
        })
    }
    fn list(&self, prefix: &ObjectPrefix, mode: ListMode) -> Result<Vec<ListEntry>> {
        self.list_bounded(prefix, mode, 64 * 1024 * 1024)
    }
    fn delete(&self, key: &ObjectKey) -> Result<()> {
        self.runtime.block_on(async {
            let result = self.store.delete(&self.root.path(key.as_str())?).await;
            match result {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                Err(e) => Err(remote_error(e, true)),
            }
        })
    }
    fn conditional_create(&self, key: &ObjectKey, source: &mut dyn Read) -> Result<Validator> {
        self.writer
            .upload(&self.root.path(key.as_str())?, None, source)
    }
    fn conditional_put(
        &self,
        key: &ObjectKey,
        expected: &Validator,
        source: &mut dyn Read,
    ) -> Result<Validator> {
        self.writer
            .upload(&self.root.path(key.as_str())?, Some(expected), source)
    }
}

impl CloudBackend {
    fn list_bounded(
        &self,
        prefix: &ObjectPrefix,
        mode: ListMode,
        limit: usize,
    ) -> Result<Vec<ListEntry>> {
        let path = self.root.path(prefix.as_str())?;
        self.runtime.block_on(async {
            let mut entries = Vec::new();
            let mut prefixes = std::collections::BTreeSet::new();
            let mut used = 0usize;
            // Stream pages even for child listings. list_with_delimiter first
            // aggregates every page and cannot enforce an allocation budget.
            let mut stream = self.store.list(Some(&path));
            while let Some(object) = stream.next().await {
                let object = object.map_err(|e| remote_error(e, false))?;
                let relative = self.root.relative(&object.location)?;
                used = used
                    .checked_add(relative.len().saturating_add(256))
                    .filter(|used| *used <= limit)
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::InvalidRecord,
                            "cloud listing exceeds metadata budget",
                        )
                    })?;
                let suffix = relative.strip_prefix(prefix.as_str()).ok_or_else(|| {
                    Error::new(
                        ErrorKind::Integrity,
                        "cloud listing escaped requested prefix",
                    )
                })?;
                if mode == ListMode::Children
                    && let Some((child, _)) = suffix.split_once('/')
                {
                    let child = format!("{}{child}/", prefix.as_str());
                    prefixes.insert(child);
                    continue;
                }
                entries.push(ListEntry::Object(ObjectKey::new(relative)?));
            }
            for child in prefixes {
                entries.push(ListEntry::Prefix(ObjectPrefix::new(child)?));
            }
            entries.sort_by(|a, b| {
                fn key(entry: &ListEntry) -> &str {
                    match entry {
                        ListEntry::Object(k) => k.as_str(),
                        ListEntry::Prefix(k) => k.as_str(),
                    }
                }
                key(a).cmp(key(b))
            });
            Ok(entries)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{PutMode, UpdateVersion, memory::InMemory};
    fn loopback_ranges(peer: &mock::Peer) -> S3RangeRead {
        let store = AmazonS3Builder::new()
            .with_bucket_name("bucket")
            .with_region("us-east-1")
            .with_endpoint(&peer.url)
            .with_allow_http(true)
            .with_access_key_id("synthetic-key")
            .with_secret_access_key("synthetic-secret")
            .with_retry(RetryConfig {
                max_retries: 0,
                ..Default::default()
            })
            .build()
            .unwrap();
        S3RangeRead {
            store,
            http: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap(),
            timeout: Duration::from_secs(2),
        }
    }
    fn range_runtime() -> Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
    }
    #[test]
    fn s3_range_get_signs_exact_condition_and_range_and_returns_only_admitted_bytes() {
        let peer = mock::Peer::start(|_| {
            vec![Some(
                mock::Response::new(206, "abcdefgh")
                    .header("etag", "\"expected\"")
                    .header("content-range", "bytes 4-11/100"),
            )]
        });
        let ranges = loopback_ranges(&peer);
        let mut sink = Vec::new();
        let meta = range_runtime()
            .block_on(ranges.read(
                &RemotePath::parse("exact key/part.parquet").unwrap(),
                &Validator::new("\"expected\"").unwrap(),
                4,
                8,
                &mut sink,
            ))
            .unwrap();
        assert_eq!(sink, b"abcdefgh");
        assert_eq!(meta.size.get(), 100);
        let requests = peer.finish();
        assert_eq!(requests.len(), 1);
        assert!(
            requests[0]
                .line
                .starts_with("GET /bucket/exact%20key/part.parquet?")
        );
        assert!(
            requests[0].line.contains("if-match") && requests[0].line.contains("range"),
            "both authority headers must be signed"
        );
        assert_eq!(requests[0].headers["if-match"], "\"expected\"");
        assert_eq!(requests[0].headers["range"], "bytes=4-11");
        assert_eq!(requests[0].headers["accept-encoding"], "identity");
        assert!(requests[0].body.is_empty());
    }
    #[test]
    fn s3_range_failures_do_not_retry_redirect_disclose_response_or_fill_sink() {
        for (response, kind) in [
            (None, ErrorKind::Io),
            (
                Some(
                    mock::Response::new(302, "private-provider-body")
                        .header("location", "http://127.0.0.1:1/leak"),
                ),
                ErrorKind::Io,
            ),
            (
                Some(mock::Response::new(404, "private-provider-body")),
                ErrorKind::NotFound,
            ),
            (
                Some(mock::Response::new(412, "private-provider-body")),
                ErrorKind::PreconditionFailed,
            ),
            (
                Some(
                    mock::Response::new(200, "abcdefgh")
                        .header("etag", "\"expected\"")
                        .header("content-range", "bytes 4-11/100"),
                ),
                ErrorKind::Io,
            ),
            (
                Some(
                    mock::Response::new(206, "abcdefgh")
                        .header("etag", "\"changed\"")
                        .header("content-range", "bytes 4-11/100"),
                ),
                ErrorKind::Integrity,
            ),
            (
                Some(
                    mock::Response::new(206, "too long!")
                        .header("etag", "\"expected\"")
                        .header("content-range", "bytes 4-11/100"),
                ),
                ErrorKind::Integrity,
            ),
        ] {
            let peer = mock::Peer::start(|_| vec![response]);
            let ranges = loopback_ranges(&peer);
            let mut sink = Vec::new();
            let error = range_runtime()
                .block_on(ranges.read(
                    &RemotePath::parse("key").unwrap(),
                    &Validator::new("\"expected\"").unwrap(),
                    4,
                    8,
                    &mut sink,
                ))
                .unwrap_err();
            assert_eq!(error.kind, kind);
            assert_eq!(error.effect, WriteEffect::NoEffect);
            assert!(sink.is_empty());
            assert!(
                !error.to_string().contains("private-provider-body")
                    && !error.to_string().contains("synthetic-secret")
            );
            assert_eq!(peer.finish().len(), 1);
        }
    }
    #[test]
    fn conditional_range_headers_require_exact_validator_span_length_and_full_size() {
        use reqwest::header::HeaderMap;
        let expected = Validator::new("\"etag\"").unwrap();
        let good = || {
            let mut h = HeaderMap::new();
            for (key, value) in [
                ("etag", "\"etag\""),
                ("content-range", "bytes 4-11/100"),
                ("content-length", "8"),
            ] {
                h.insert(
                    reqwest::header::HeaderName::from_bytes(key.as_bytes()).unwrap(),
                    value.parse().unwrap(),
                );
            }
            h
        };
        let meta = range_metadata(&good(), &expected, 4, 8).unwrap();
        assert_eq!(meta.size.get(), 100);
        assert_eq!(meta.validator, expected);
        for (key, value) in [
            ("etag", "other"),
            ("content-range", "bytes 4-11/*"),
            ("content-range", "bytes 4-12/100"),
            ("content-range", "bytes 4-11/11"),
            ("content-length", "9"),
            ("content-encoding", "gzip"),
        ] {
            let mut h = good();
            h.insert(
                reqwest::header::HeaderName::from_bytes(key.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
            assert_eq!(
                range_metadata(&h, &expected, 4, 8).unwrap_err().kind,
                ErrorKind::Integrity
            );
        }
        let mut h = good();
        h.append("etag", "other".parse().unwrap());
        assert!(range_metadata(&h, &expected, 4, 8).is_err());
        let mut h = good();
        h.remove("content-range");
        assert!(range_metadata(&h, &expected, 4, 8).is_err());
        for (offset, length) in [(0, 0), (0, 65537), (u64::MAX, 2)] {
            assert_eq!(
                range_end(offset, length).unwrap_err().kind,
                ErrorKind::InvalidRecord
            );
        }
    }
    #[test]
    fn unavailable_range_backend_is_explicit_and_box_delegation_preserves_it() {
        let backend: Box<dyn Backend> = Box::new(memory());
        let mut sink = Vec::new();
        assert_eq!(
            backend
                .read_range(
                    &ObjectKey::new("object").unwrap(),
                    &Validator::new("etag").unwrap(),
                    0,
                    4,
                    &mut sink
                )
                .unwrap_err()
                .kind,
            ErrorKind::Unsupported
        );
        assert!(sink.is_empty());
    }
    struct MemoryUpload {
        store: Arc<InMemory>,
        runtime: Arc<Runtime>,
    }
    impl ConditionalUpload for MemoryUpload {
        fn upload(
            &self,
            key: &RemotePath,
            expected: Option<&Validator>,
            source: &mut dyn Read,
        ) -> Result<Validator> {
            let mut bytes = Vec::new();
            source.read_to_end(&mut bytes).map_err(io_error)?;
            let mode = expected.map_or(PutMode::Create, |v| {
                PutMode::Update(UpdateVersion {
                    e_tag: Some(v.as_str().into()),
                    version: None,
                })
            });
            let result = self
                .runtime
                .block_on(self.store.put_opts(key, bytes.into(), mode.into()))
                .map_err(|e| remote_error(e, true))?;
            Validator::new(result.e_tag.unwrap())
        }
    }
    fn memory() -> CloudBackend {
        let root = CloudRoot::parse("s3://test-bucket/isolated/root/").unwrap();
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap(),
        );
        let store = Arc::new(InMemory::new());
        CloudBackend {
            root,
            store: store.clone(),
            runtime: runtime.clone(),
            writer: Arc::new(MemoryUpload { store, runtime }),
            ranges: None,
        }
    }
    #[test]
    fn cloud_listing_streams_with_a_budget_for_recursive_and_child_modes() {
        let backend = memory();
        for key in [
            "objects/a",
            "objects/nested/a",
            "objects/nested/b",
            "other/unselected",
        ] {
            backend
                .create_bytes(&ObjectKey::new(key).unwrap(), b"value")
                .unwrap();
        }
        let prefix = ObjectPrefix::new("objects/").unwrap();
        for mode in [ListMode::Recursive, ListMode::Children] {
            assert_eq!(
                backend.list_bounded(&prefix, mode, 300).unwrap_err().kind,
                ErrorKind::InvalidRecord
            );
            let result = backend.list_bounded(&prefix, mode, 4096).unwrap();
            assert_eq!(
                result.len(),
                if mode == ListMode::Recursive { 3 } else { 2 }
            );
        }
    }
    #[test]
    fn s3_object_locations_are_exact_pure_and_escape_prefixes_once() {
        let key = ObjectKey::new("datasets/data/rows/version=1/data.parquet").unwrap();
        for (root, expected) in [
            (
                "s3://test-bucket",
                "s3://test-bucket/datasets/data/rows/version=1/data.parquet",
            ),
            (
                "s3://test-bucket/a%2520b/c%20d",
                "s3://test-bucket/a%2520b/c%20d/datasets/data/rows/version=1/data.parquet",
            ),
            (
                "s3://test-bucket/literal%2A",
                "s3://test-bucket/literal%2A/datasets/data/rows/version=1/data.parquet",
            ),
        ] {
            let root = CloudRoot::parse(root).unwrap();
            let uri = root.s3_data_uri(&key).unwrap().unwrap();
            assert_eq!(uri, expected);
            assert!(uri.starts_with(&format!("{}/", root.canonical())));
            let parsed = CloudRoot::parse(&uri).unwrap();
            assert_eq!(parsed.prefix, root.path(key.as_str()).unwrap().as_ref());
            assert!(!uri.contains(['*', '[', ']', '?', '#', '@']));
        }
        assert_eq!(
            CloudRoot::parse("gs://test-bucket/prefix")
                .unwrap()
                .s3_data_uri(&key)
                .unwrap(),
            None
        );
        let wrapped: Box<dyn Backend> = Box::new(memory());
        let direct = wrapped.s3_data_uri(&key).unwrap();
        assert_eq!(
            direct,
            Some(format!("s3://test-bucket/isolated/root/{}", key.as_str()))
        );
    }
    #[test]
    fn cloud_root_is_pure_canonical_round_trippable_and_rejects_ambiguous_authorities() {
        for uri in [
            "s3://test-bucket",
            "s3://test-bucket/prefix/",
            "s3://test-bucket/a%2520b/c%20d",
            "s3://test-bucket/literal*glob[01]",
            "s3://test-bucket/owner@example",
            "gs://test-bucket/prefix",
        ] {
            let root = CloudRoot::parse(uri).unwrap();
            assert_eq!(CloudRoot::parse(root.canonical()).unwrap(), root);
            assert!(!root.canonical().contains(['*', '[', ']']));
        }
        for uri in [
            "https://test-bucket/prefix",
            "s3://user:secret@test-bucket/a",
            "s3://test-bucket:443/a",
            "s3://test-bucket/a?credential=canary",
            "s3://test-bucket/a#secret",
            "s3://test-bucket/a/../b",
            "s3://test-bucket/a/%2E%2E/b",
            "s3://test-bucket/a//b",
        ] {
            assert!(CloudRoot::parse(uri).is_err(), "{uri}");
        }
    }
    #[test]
    fn cloud_six_operations_preserve_conditions_validators_prefix_isolation_and_idempotent_delete()
    {
        let backend = memory();
        let key = ObjectKey::new("objects/control.json").unwrap();
        let first = backend.create_bytes(&key, b"first").unwrap();
        assert_eq!(
            backend.create_bytes(&key, b"second").unwrap_err().kind,
            ErrorKind::PreconditionFailed
        );
        let meta = backend.head(&key).unwrap();
        assert_eq!(meta.validator, first);
        assert_eq!(meta.size.get(), 5);
        let second = backend.put_bytes(&key, &first, b"second").unwrap();
        assert_eq!(
            backend.put_bytes(&key, &first, b"stale").unwrap_err().kind,
            ErrorKind::PreconditionFailed
        );
        assert_eq!(
            backend.read_bytes(&key, 64).unwrap(),
            (
                b"second".to_vec(),
                ObjectMeta {
                    validator: second,
                    size: 6.into()
                }
            )
        );
        backend
            .create_bytes(&ObjectKey::new("objects/nested/row").unwrap(), b"row")
            .unwrap();
        let children = backend
            .list(&ObjectPrefix::new("objects/").unwrap(), ListMode::Children)
            .unwrap();
        assert_eq!(
            children,
            vec![
                ListEntry::Object(key.clone()),
                ListEntry::Prefix(ObjectPrefix::new("objects/nested/").unwrap())
            ]
        );
        assert_eq!(
            backend
                .list(&ObjectPrefix::new("objects/").unwrap(), ListMode::Recursive)
                .unwrap()
                .len(),
            2
        );
        backend.delete(&key).unwrap();
        backend.delete(&key).unwrap();
        assert_eq!(backend.head(&key).unwrap_err().kind, ErrorKind::NotFound);
    }
    #[test]
    fn cloud_errors_cannot_expose_credentials_or_signed_urls_and_preserve_ambiguity() {
        let input = object_store::Error::Generic {
            store: "S3",
            source: "https://bucket/key?X-Amz-Security-Token=credential-canary".into(),
        };
        let error = remote_error(input, true);
        assert_eq!(error.effect, WriteEffect::MaybeApplied);
        assert!(!format!("{error:?} {error}").contains("credential-canary"));
        assert_eq!(
            remote_error(
                object_store::Error::Precondition {
                    path: "private".into(),
                    source: "credential-canary".into()
                },
                true
            )
            .effect,
            WriteEffect::NoEffect
        );
    }
    #[test]
    #[ignore = "requires an explicitly configured dedicated S3 root and profile"]
    fn live_s3_conditional_multipart_contract() {
        let root = std::env::var("GRV_S3_TEST_ROOT").expect("GRV_S3_TEST_ROOT required");
        let profile = std::env::var("AWS_PROFILE").expect("AWS_PROFILE required");
        let root = format!(
            "{}/backend-conformance-{}",
            root.trim_end_matches('/'),
            grv_types::Uuid::v4()
        );
        eprintln!("S3 conformance root: {root}");
        let backend = CloudBackend::open(
            &root,
            CloudOptions {
                profile: Some(profile),
                ..Default::default()
            },
        )
        .unwrap();
        struct Cleanup<'a>(&'a CloudBackend);
        impl Drop for Cleanup<'_> {
            fn drop(&mut self) {
                for name in ["objects/control.json", "objects/files/large.bin"] {
                    if self.0.delete(&ObjectKey::new(name).unwrap()).is_err() {
                        eprintln!("S3 conformance cleanup requires retry: {name}");
                    }
                }
            }
        }
        let _cleanup = Cleanup(&backend);
        let key = ObjectKey::new("objects/control.json").unwrap();
        let first = backend
            .create_bytes(&key, b"{\"mutation_id\":\"first\"}")
            .unwrap();
        assert_eq!(
            backend.create_bytes(&key, b"different").unwrap_err().kind,
            ErrorKind::PreconditionFailed
        );
        let second = backend
            .put_bytes(&key, &first, b"{\"mutation_id\":\"second\"}")
            .unwrap();
        assert_ne!(first, second);
        assert_eq!(
            backend.put_bytes(&key, &first, b"stale").unwrap_err().kind,
            ErrorKind::PreconditionFailed
        );
        assert_eq!(
            backend.read_bytes(&key, 1024).unwrap().0,
            b"{\"mutation_id\":\"second\"}"
        );
        struct Generated(u64);
        impl Read for Generated {
            fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
                let n = self.0.min(buffer.len() as u64) as usize;
                buffer[..n].fill(42);
                self.0 -= n as u64;
                Ok(n)
            }
        }
        struct Count(u64);
        impl Write for Count {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                assert!(bytes.iter().all(|b| *b == 42));
                self.0 += bytes.len() as u64;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let large = ObjectKey::new("objects/files/large.bin").unwrap();
        let size = PART_BYTES as u64 + 1024;
        backend
            .conditional_create(&large, &mut Generated(size))
            .unwrap();
        let mut count = Count(0);
        assert_eq!(backend.get(&large, &mut count).unwrap().size.get(), size);
        assert_eq!(count.0, size);
        assert_eq!(
            backend
                .list(&ObjectPrefix::new("objects/").unwrap(), ListMode::Recursive)
                .unwrap()
                .len(),
            2
        );
        backend.delete(&key).unwrap();
        backend.delete(&large).unwrap();
        backend.delete(&large).unwrap();
        assert!(
            backend
                .list(&ObjectPrefix::new("objects/").unwrap(), ListMode::Recursive)
                .unwrap()
                .is_empty()
        );
    }
    // This wrapper suppresses a *successful return*, not a wire response. The
    // real uploader has already validated completion before we return ambiguity.
    struct SuppressSuccessfulReturn {
        inner: Arc<dyn ConditionalUpload>,
        committed: std::sync::Mutex<Option<Validator>>,
    }
    impl ConditionalUpload for SuppressSuccessfulReturn {
        fn upload(
            &self,
            key: &RemotePath,
            expected: Option<&Validator>,
            source: &mut dyn Read,
        ) -> Result<Validator> {
            let validator = self.inner.upload(key, expected, source)?;
            *self.committed.lock().unwrap() = Some(validator);
            Err(Error::new(ErrorKind::Io, "test suppressed successful upload return").applied())
        }
    }
    fn assert_rejected(error: Error) {
        assert_eq!(error.kind, ErrorKind::PreconditionFailed);
        assert_eq!(error.effect, WriteEffect::NoEffect);
    }
    fn suppressed_return_contract(backend: &CloudBackend) {
        let wrapper = Arc::new(SuppressSuccessfulReturn {
            inner: backend.writer.clone(),
            committed: std::sync::Mutex::new(None),
        });
        let suppressed = CloudBackend {
            root: backend.root.clone(),
            store: backend.store.clone(),
            runtime: backend.runtime.clone(),
            writer: wrapper.clone(),
            ranges: None,
        };
        let key = ObjectKey::new("objects/response-loss").unwrap();
        let first_bytes = b"mutation=create-after-suppressed-return";
        let error = suppressed.create_bytes(&key, first_bytes).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Io);
        assert_eq!(error.effect, WriteEffect::MaybeApplied);
        let first = backend.head(&key).unwrap();
        assert_eq!(
            Some(first.validator.clone()),
            *wrapper.committed.lock().unwrap()
        );
        assert_eq!(
            backend.read_bytes(&key, 1024).unwrap(),
            (first_bytes.to_vec(), first.clone())
        );
        assert_rejected(
            backend
                .create_bytes(&key, b"replayed-create-must-not-land")
                .unwrap_err(),
        );
        assert_eq!(backend.head(&key).unwrap(), first);
        assert_eq!(
            backend.read_bytes(&key, 1024).unwrap(),
            (first_bytes.to_vec(), first.clone())
        );

        let second_bytes = b"mutation=cas-after-suppressed-return";
        let error = suppressed
            .put_bytes(&key, &first.validator, second_bytes)
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Io);
        assert_eq!(error.effect, WriteEffect::MaybeApplied);
        let second = backend.head(&key).unwrap();
        assert_ne!(first.validator, second.validator);
        assert_eq!(
            Some(second.validator.clone()),
            *wrapper.committed.lock().unwrap()
        );
        assert_eq!(
            backend.read_bytes(&key, 1024).unwrap(),
            (second_bytes.to_vec(), second.clone())
        );
        assert_rejected(
            backend
                .put_bytes(&key, &first.validator, b"stale-replay-must-not-land")
                .unwrap_err(),
        );
        assert_eq!(
            backend.read_bytes(&key, 1024).unwrap(),
            (second_bytes.to_vec(), second)
        );
        backend.delete(&key).unwrap();
    }

    // The start barrier admits two concurrent calls. First-read rendezvous makes
    // both GCS sessions initiate with the same condition before either can send
    // its final bytes. Only then is the selected winner allowed to finish; the
    // loser sends after that completion. This tests deterministic commit orders,
    // not an uncontrolled simultaneous transport race. Timeouts bound failures
    // before Read (e.g. failed session initiation) rather than hanging a barrier.
    fn ordered_conditional_race(
        backend: &CloudBackend,
        key: &ObjectKey,
        expected: Option<&Validator>,
        winner: usize,
        round: usize,
    ) {
        use std::sync::{Barrier, mpsc};
        struct RendezvousRead<'a> {
            bytes: std::io::Cursor<&'a [u8]>,
            ready: Option<mpsc::Sender<()>>,
            peer_ready: mpsc::Receiver<()>,
            winner_done: Option<mpsc::Receiver<()>>,
        }
        impl Read for RendezvousRead<'_> {
            fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
                if let Some(ready) = self.ready.take() {
                    let failed = || std::io::Error::other("test race rendezvous failed");
                    ready.send(()).map_err(|_| failed())?;
                    self.peer_ready
                        .recv_timeout(Duration::from_secs(150))
                        .map_err(|_| failed())?;
                    if let Some(done) = self.winner_done.take() {
                        done.recv_timeout(Duration::from_secs(150))
                            .map_err(|_| failed())?;
                    }
                }
                self.bytes.read(buffer)
            }
        }
        let values = [
            format!("round={round};contender=0;mutation=unique"),
            format!("round={round};contender=1;mutation=unique"),
        ];
        let barrier = Barrier::new(2);
        let (ready0_tx, ready0_rx) = mpsc::channel();
        let (ready1_tx, ready1_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let mut ready = [(ready0_tx, ready1_rx), (ready1_tx, ready0_rx)].into_iter();
        let mut done_tx = Some(done_tx);
        let mut done_rx = Some(done_rx);
        let results = std::thread::scope(|scope| {
            let mut threads = Vec::new();
            for (index, value) in values.iter().enumerate() {
                let (ready_tx, peer_ready) = ready.next().unwrap();
                let signal = if index == winner {
                    done_tx.take()
                } else {
                    None
                };
                let wait = if index != winner {
                    done_rx.take()
                } else {
                    None
                };
                let barrier = &barrier;
                threads.push(scope.spawn(move || {
                    let mut source = RendezvousRead {
                        bytes: std::io::Cursor::new(value.as_bytes()),
                        ready: Some(ready_tx),
                        peer_ready,
                        winner_done: wait,
                    };
                    barrier.wait();
                    let result = match expected {
                        None => backend.conditional_create(key, &mut source),
                        Some(v) => backend.conditional_put(key, v, &mut source),
                    };
                    if let Some(signal) = signal {
                        let _ = signal.send(());
                    }
                    result
                }));
            }
            // Join both before asserting, so cleanup never races a live writer.
            threads
                .into_iter()
                .map(|thread| thread.join())
                .collect::<Vec<_>>()
        });
        let mut results = results
            .into_iter()
            .map(|result| result.unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        let loser = 1 - winner;
        let rejection = results[loser].as_ref().unwrap_err();
        assert_eq!(rejection.kind, ErrorKind::PreconditionFailed);
        assert_eq!(rejection.effect, WriteEffect::NoEffect);
        let validator = results.swap_remove(winner).unwrap();
        if let Some(expected) = expected {
            assert_ne!(&validator, expected);
        }
        let (bytes, meta) = backend.read_bytes(key, 1024).unwrap();
        assert_eq!(bytes, values[winner].as_bytes());
        assert_eq!(meta.validator, validator);
        assert_eq!(meta.size.get(), bytes.len() as u64);
        assert_eq!(backend.head(key).unwrap(), meta);
    }

    struct OwnedGcsTestRoot {
        backend: CloudBackend,
        cleaned: bool,
    }
    impl OwnedGcsTestRoot {
        fn open() -> Self {
            let selected = CloudRoot::parse(
                &std::env::var("GRV_GCS_WRITE_TEST_ROOT").expect("separate write root required"),
            )
            .unwrap();
            // Authorization is deliberately hard-coded, not just a nonempty
            // prefix check: no environment value can authorize the xyz fixture.
            assert_eq!(selected.scheme, Scheme::Gcs);
            assert_eq!(selected.bucket, "validation-gcs-bucket");
            assert_eq!(selected.prefix, "grv-release-validation");
            assert!(!selected.prefix.is_empty());
            let account = std::env::var("GRV_GCS_ACCOUNT").expect("explicit account required");
            let project = std::env::var("GRV_GCS_PROJECT").expect("explicit project required");
            assert!(!account.trim().is_empty() && !project.trim().is_empty());
            let root = format!("{}/{}", selected.canonical(), grv_types::Uuid::v4());
            eprintln!("Owned GCS primitive test root: {root}");
            let mut owned = Self {
                backend: CloudBackend::open(
                    &root,
                    CloudOptions {
                        gcs_account: Some(account),
                        gcs_project: Some(project),
                        ..Default::default()
                    },
                )
                .unwrap(),
                // Do not arm deletion until fresh ownership has been proved.
                cleaned: true,
            };
            assert!(
                owned.inventory().unwrap().is_empty(),
                "fresh UUID root must be empty"
            );
            owned.cleaned = false;
            owned
        }
        fn inventory(&self) -> Result<Vec<ObjectKey>> {
            self.backend.runtime.block_on(async {
                let path = self.backend.root.path("")?;
                let mut stream = self.backend.store.list(Some(&path));
                let mut keys = Vec::new();
                while let Some(meta) = stream.next().await {
                    let meta = meta.map_err(|e| remote_error(e, false))?;
                    // Fail closed if a provider listing ever escapes ownership.
                    keys.push(ObjectKey::new(self.backend.root.relative(&meta.location)?)?);
                }
                Ok(keys)
            })
        }
        fn cleanup(&mut self) -> Result<()> {
            let mut failure = None;
            for key in self.inventory()? {
                if let Err(error) = self.backend.delete(&key) {
                    failure.get_or_insert(error);
                }
            }
            let remaining = self.inventory()?;
            if let Some(error) = failure {
                return Err(error);
            }
            if !remaining.is_empty() {
                return Err(Error::new(
                    ErrorKind::Integrity,
                    "owned GCS prefix is not empty after cleanup",
                ));
            }
            self.cleaned = true;
            Ok(())
        }
    }
    impl Drop for OwnedGcsTestRoot {
        fn drop(&mut self) {
            if !self.cleaned
                && let Err(error) = self.cleanup()
            {
                if std::thread::panicking() {
                    // The original panic already fails the test; never mask
                    // this additional cleanup failure with a double panic.
                    eprintln!(
                        "FAILED GCS panic cleanup at {}: {error}",
                        self.backend.root.canonical()
                    );
                } else {
                    panic!("GCS cleanup failed: {error}");
                }
            }
        }
    }

    #[test]
    fn suppressed_successful_return_and_ordered_races_have_durable_evidence() {
        let backend = memory();
        suppressed_return_contract(&backend);
        for winner in 0..2 {
            let key = ObjectKey::new(format!("objects/race-{winner}")).unwrap();
            ordered_conditional_race(&backend, &key, None, winner, 0);
            let first = backend.head(&key).unwrap().validator;
            ordered_conditional_race(&backend, &key, Some(&first), winner, 1);
            backend.delete(&key).unwrap();
        }
        assert!(
            backend
                .list(&ObjectPrefix::new("objects/").unwrap(), ListMode::Recursive)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    #[ignore = "writes only to authorized UUID roots; requires GRV_GCS_WRITE_TEST_ROOT, ACCOUNT and PROJECT"]
    fn live_gcs_conditional_streaming_response_loss_and_race_contract() {
        use sha2::{Digest as _, Sha256};
        let mut owned = OwnedGcsTestRoot::open();
        let backend = &owned.backend;
        let key = ObjectKey::new("objects/control").unwrap();
        let first = backend.create_bytes(&key, b"first-generation").unwrap();
        assert!(first.as_str().parse::<u64>().unwrap() > 0);
        let initial = backend.head(&key).unwrap();
        assert_eq!(initial.validator, first);
        assert_rejected(backend.create_bytes(&key, b"duplicate").unwrap_err());
        assert_eq!(
            backend.read_bytes(&key, 1024).unwrap(),
            (b"first-generation".to_vec(), initial)
        );
        let second = backend
            .put_bytes(&key, &first, b"second-generation")
            .unwrap();
        assert_ne!(first, second);
        assert!(second.as_str().parse::<u64>().unwrap() > 0);
        assert_rejected(backend.put_bytes(&key, &first, b"stale").unwrap_err());
        assert_eq!(
            backend.read_bytes(&key, 1024).unwrap(),
            (
                b"second-generation".to_vec(),
                ObjectMeta {
                    validator: second,
                    size: (b"second-generation".len() as u32).into(),
                }
            )
        );

        // A generated source and checking sink cross PART_BYTES without holding
        // the entire object. Every byte is checked, as well as count and SHA-256.
        fn byte_at(offset: u64) -> u8 {
            ((offset * 31 + offset / 251) % 256) as u8
        }
        struct Generated {
            offset: u64,
            size: u64,
            hash: Sha256,
        }
        impl Read for Generated {
            fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
                let n = (self.size - self.offset)
                    .min(buffer.len() as u64)
                    .min(65537) as usize;
                for (i, byte) in buffer[..n].iter_mut().enumerate() {
                    *byte = byte_at(self.offset + i as u64);
                }
                self.hash.update(&buffer[..n]);
                self.offset += n as u64;
                Ok(n)
            }
        }
        struct Checked {
            offset: u64,
            hash: Sha256,
        }
        impl Write for Checked {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                for (i, byte) in bytes.iter().enumerate() {
                    assert_eq!(*byte, byte_at(self.offset + i as u64));
                }
                self.offset += bytes.len() as u64;
                self.hash.update(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let large = ObjectKey::new("objects/files/large.bin").unwrap();
        let size = PART_BYTES as u64 + 65537;
        let mut source = Generated {
            offset: 0,
            size,
            hash: Sha256::new(),
        };
        let validator = backend.conditional_create(&large, &mut source).unwrap();
        assert_eq!(source.offset, size);
        let mut sink = Checked {
            offset: 0,
            hash: Sha256::new(),
        };
        let meta = backend.get(&large, &mut sink).unwrap();
        assert_eq!(meta.validator, validator);
        assert_eq!(meta.size.get(), size);
        assert_eq!(backend.head(&large).unwrap(), meta);
        assert_eq!(sink.offset, size);
        assert_eq!(sink.hash.finalize(), source.hash.finalize());
        let prefix = ObjectPrefix::new("objects/").unwrap();
        assert_eq!(
            backend.list(&prefix, ListMode::Recursive).unwrap(),
            vec![
                ListEntry::Object(key.clone()),
                ListEntry::Object(large.clone()),
            ]
        );
        assert_eq!(
            backend.list(&prefix, ListMode::Children).unwrap(),
            vec![
                ListEntry::Object(key.clone()),
                ListEntry::Prefix(ObjectPrefix::new("objects/files/").unwrap()),
            ]
        );
        suppressed_return_contract(backend);
        for winner in 0..2 {
            for round in 0..20 {
                let race = ObjectKey::new(format!("objects/race-{winner}-{round}")).unwrap();
                ordered_conditional_race(backend, &race, None, winner, round * 2);
                let expected = backend.head(&race).unwrap().validator;
                ordered_conditional_race(backend, &race, Some(&expected), winner, round * 2 + 1);
                backend.delete(&race).unwrap();
            }
        }
        backend.delete(&key).unwrap();
        backend.delete(&large).unwrap();
        backend.delete(&large).unwrap();
        assert_eq!(backend.head(&large).unwrap_err().kind, ErrorKind::NotFound);
        assert!(
            backend
                .list(&prefix, ListMode::Recursive)
                .unwrap()
                .is_empty()
        );
        owned
            .cleanup()
            .expect("cleanup must delete all objects and prove the whole UUID prefix empty");
    }

    fn source_read_failure_contract(backend: &CloudBackend, fail_offsets: &[usize]) {
        struct FailAfter {
            remaining: usize,
            delivered: usize,
            failed: bool,
        }
        impl Read for FailAfter {
            fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
                if self.remaining == 0 {
                    self.failed = true;
                    return Err(std::io::Error::other("injected test source failure"));
                }
                let n = self.remaining.min(buffer.len());
                buffer[..n].fill(73);
                self.remaining -= n;
                self.delivered += n;
                Ok(n)
            }
        }
        let existing = ObjectKey::new("objects/read-failure-existing").unwrap();
        let original_bytes = b"original-must-survive-aborted-cas";
        backend.create_bytes(&existing, original_bytes).unwrap();
        let original = backend.head(&existing).unwrap();
        for (index, &fail_after) in fail_offsets.iter().enumerate() {
            let key = ObjectKey::new(format!("objects/read-failure-{index}")).unwrap();
            let mut source = FailAfter {
                remaining: fail_after,
                delivered: 0,
                failed: false,
            };
            let error = backend.conditional_create(&key, &mut source).unwrap_err();
            assert_eq!(error.kind, ErrorKind::Io);
            assert_eq!(error.effect, WriteEffect::NoEffect);
            assert!(source.failed);
            assert_eq!(source.delivered, fail_after);
            assert_eq!(backend.head(&key).unwrap_err().kind, ErrorKind::NotFound);
            assert_eq!(
                backend
                    .list(&ObjectPrefix::new("objects/").unwrap(), ListMode::Recursive)
                    .unwrap(),
                vec![ListEntry::Object(existing.clone())]
            );
            backend.delete(&key).unwrap();
            backend.delete(&key).unwrap();
            let mut source = FailAfter {
                remaining: fail_after,
                delivered: 0,
                failed: false,
            };
            let error = backend
                .conditional_put(&existing, &original.validator, &mut source)
                .unwrap_err();
            assert_eq!(error.kind, ErrorKind::Io);
            assert_eq!(error.effect, WriteEffect::NoEffect);
            assert!(source.failed);
            assert_eq!(source.delivered, fail_after);
            assert_eq!(
                backend.read_bytes(&existing, 1024).unwrap(),
                (original_bytes.to_vec(), original.clone())
            );
            assert_eq!(backend.head(&existing).unwrap(), original);
        }
        backend.delete(&existing).unwrap();
        assert!(
            backend
                .list(&ObjectPrefix::new("objects/").unwrap(), ListMode::Recursive)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn source_read_failure_has_no_completed_object_or_replacement() {
        source_read_failure_contract(&memory(), &[0, 17]);
    }

    #[test]
    #[ignore = "writes only to authorized UUID roots; requires GRV_GCS_WRITE_TEST_ROOT, ACCOUNT and PROJECT"]
    fn live_gcs_streaming_read_failure_contract() {
        let mut owned = OwnedGcsTestRoot::open();
        source_read_failure_contract(&owned.backend, &[0, PART_BYTES + 17]);
        // gcs::GcsUpload attempts DELETE of the session after a Read error,
        // ignoring that cancellation result. The second case fails after a
        // nonfinal PART_BYTES chunk was acknowledged. HEAD/list prove no
        // completed object, NOT server-side resumable-session reclamation.
        owned
            .cleanup()
            .expect("cleanup must prove the whole UUID prefix empty");
    }

    #[test]
    #[ignore = "read-only; requires an explicitly configured GCS prefix and account"]
    fn live_gcs_read_only_contract() {
        use sha2::{Digest as _, Sha256};
        let selected = CloudRoot::parse(
            &std::env::var("GRV_GCS_TEST_ROOT").expect("read-only GCS root required"),
        )
        .unwrap();
        assert_eq!(selected.scheme, Scheme::Gcs);
        assert!(
            !selected.prefix.is_empty(),
            "dedicated read prefix required"
        );
        let backend = CloudBackend::open(
            &format!("gs://{}", selected.bucket),
            CloudOptions {
                gcs_account: Some(
                    std::env::var("GRV_GCS_ACCOUNT").expect("explicit account required"),
                ),
                gcs_project: Some(
                    std::env::var("GRV_GCS_PROJECT").expect("explicit project required"),
                ),
                ..Default::default()
            },
        )
        .unwrap();
        let prefix = ObjectPrefix::new(format!("{}/", selected.prefix)).unwrap();
        let entries = backend.list(&prefix, ListMode::Recursive).unwrap();
        backend.list(&prefix, ListMode::Children).unwrap();
        eprintln!("Read-only GCS prefix contains {} objects", entries.len());
        struct HashSink {
            bytes: u64,
            hash: Sha256,
        }
        impl Write for HashSink {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.bytes += bytes.len() as u64;
                self.hash.update(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut reads = 0;
        for entry in entries {
            let ListEntry::Object(key) = entry else {
                continue;
            };
            assert!(key.as_str().starts_with(prefix.as_str()));
            let head = backend.head(&key).unwrap();
            if head.size.get() > PART_BYTES as u64 {
                continue;
            }
            let mut sink = HashSink {
                bytes: 0,
                hash: Sha256::new(),
            };
            let actual = backend.get(&key, &mut sink).unwrap();
            assert_eq!(actual, head);
            assert_eq!(sink.bytes, actual.size.get());
            eprintln!(
                "Verified GCS read: {} bytes, SHA-256 {:x}",
                sink.bytes,
                sink.hash.finalize()
            );
            reads += 1;
            if reads == 3 {
                break;
            }
        }
        if reads == 0 {
            eprintln!("A small fixture is needed to verify GCS HEAD and GET");
        }
    }
}
