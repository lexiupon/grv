//! A generation condition is fixed when the upload session is created. Chunks
//! are sent once, and only a verified final response supplies a new validator.
//! Session URIs are private capabilities and never appear in diagnostics.
use super::*;
use object_store::gcp::GcpCredentialProvider;

pub(super) struct GcsUpload {
    bucket: String,
    credentials: GcpCredentialProvider,
    runtime: Arc<Runtime>,
    http: reqwest::Client,
    endpoint: Url,
    // True only when both named preparation selectors were supplied. Ordinary
    // GCS operations retain their existing credential behavior.
    view_selected: bool,
    timeout: Duration,
}
impl GcsUpload {
    pub fn new(
        bucket: &str,
        credentials: GcpCredentialProvider,
        runtime: Arc<Runtime>,
        timeout: Duration,
        project: Option<&str>,
        account: Option<&str>,
    ) -> Result<Self> {
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(project) = project {
            headers.insert(
                "x-goog-user-project",
                reqwest::header::HeaderValue::from_str(project).map_err(|_| {
                    Error::new(ErrorKind::InvalidRecord, "invalid GCS quota project")
                })?,
            );
        }
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(timeout)
            .https_only(true)
            .build()
            .map_err(|_| Error::new(ErrorKind::Io, "cloud HTTP client initialization failed"))?;
        Ok(Self {
            bucket: bucket.into(),
            credentials,
            runtime,
            http,
            endpoint: Url::parse("https://storage.googleapis.com").expect("constant URL"),
            view_selected: account.is_some_and(|v| named_selector(v, 320))
                && project.is_some_and(|v| named_selector(v, 128)),
            timeout,
        })
    }
    fn view_eligible(&self) -> Result<()> {
        // Refuse absent/invalid selectors before requesting any credential;
        // never silently use ambient ADC or an active gcloud account/project.
        if !self.view_selected {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "GCS views require explicit preparation account and project selectors",
            ));
        }
        self.runtime.block_on(async {
            tokio::time::timeout(self.timeout, self.bucket_admission())
                .await
                .map_err(|_| {
                    Error::new(ErrorKind::Io, "GCS bucket eligibility deadline exceeded")
                })?
        })
    }
    async fn bucket_admission(&self) -> Result<()> {
        let credentials = self
            .credentials
            .get_credential()
            .await
            .map_err(|e| remote_error(e, false))?;
        let mut url = self.endpoint.clone();
        url.path_segments_mut()
            .expect("absolute endpoint")
            .clear()
            .extend(["storage", "v1", "b", &self.bucket]);
        // A closed projection avoids interpreting unrelated bucket metadata.
        url.query_pairs_mut()
            .append_pair("fields", "name,billing(requesterPays)");
        let mut response = self
            .http
            .get(url)
            .bearer_auth(&credentials.bearer)
            .send()
            .await
            .map_err(|_| Error::new(ErrorKind::Io, "GCS bucket eligibility request failed"))?;
        if response.status().as_u16() != 200 {
            return Err(Error::new(
                ErrorKind::Io,
                "GCS bucket eligibility request was rejected",
            ));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| Error::new(ErrorKind::Io, "GCS bucket eligibility response failed"))?
        {
            if chunk.len() > RESPONSE_BYTES.saturating_sub(bytes.len()) {
                return Err(Error::new(
                    ErrorKind::Integrity,
                    "GCS bucket eligibility metadata exceeds budget",
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        bucket_metadata(&bytes, &self.bucket)
    }
    fn initiation(&self, key: &RemotePath, expected: Option<&Validator>) -> Result<Url> {
        let condition = expected.map_or("0", Validator::as_str);
        // GCS validators are object generations, not ETags or metagenerations.
        generation(condition, expected.is_none())?;
        let mut url = self.endpoint.clone();
        url.path_segments_mut()
            .expect("absolute endpoint")
            .clear()
            .extend(["upload", "storage", "v1", "b", &self.bucket, "o"]);
        url.query_pairs_mut()
            .append_pair("uploadType", "resumable")
            .append_pair("name", key.as_ref())
            .append_pair("ifGenerationMatch", condition);
        Ok(url)
    }
    fn session(&self, location: &str) -> Result<Url> {
        if location.len() > 16 * 1024 {
            return Err(ambiguous());
        }
        let url = Url::parse(location).map_err(|_| ambiguous())?;
        // An authenticated response cannot redirect credentials or the bytes to
        // another service. The URI is accepted only for this bucket's endpoint.
        if url.origin() != self.endpoint.origin()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || url.path() != format!("/upload/storage/v1/b/{}/o", self.bucket)
            || !url
                .query_pairs()
                .any(|(key, value)| key == "upload_id" && !value.is_empty())
        {
            return Err(ambiguous());
        }
        Ok(url)
    }
    async fn send(
        &self,
        key: &RemotePath,
        expected: Option<&Validator>,
        source: &mut dyn Read,
    ) -> Result<Validator> {
        let credentials = self
            .credentials
            .get_credential()
            .await
            .map_err(|e| remote_error(e, false))?;
        let response = self
            .http
            .post(self.initiation(key, expected)?)
            .bearer_auth(&credentials.bearer)
            .header("content-length", "0")
            .header("x-upload-content-type", "application/octet-stream")
            .send()
            .await
            .map_err(|_| ambiguous())?;
        if response.status().as_u16() == 412 {
            return Err(precondition());
        }
        if !response.status().is_success() {
            return Err(ambiguous());
        }
        let location = response
            .headers()
            .get("location")
            .and_then(|h| h.to_str().ok())
            .ok_or_else(ambiguous)?;
        let session = self.session(location)?;
        // Reading one byte ahead distinguishes a final full chunk from an
        // intermediate chunk without buffering the next entire chunk.
        let result = async {
            let mut offset = 0u64;
            let mut next = None;
            loop {
                let mut buffer = vec![0u8; PART_BYTES];
                let mut size = 0;
                if let Some(byte) = next.take() {
                    buffer[0] = byte;
                    size = 1;
                }
                while size < buffer.len() {
                    match source.read(&mut buffer[size..]) {
                        Ok(0) => break,
                        Ok(n) => size += n,
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                        Err(e) => return Err(io_error(e)),
                    }
                }
                let mut byte = [0];
                if size == PART_BYTES {
                    loop {
                        match source.read(&mut byte) {
                            Ok(0) => break,
                            Ok(_) => {
                                next = Some(byte[0]);
                                break;
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                            Err(e) => return Err(io_error(e)),
                        }
                    }
                }
                let final_chunk = next.is_none();
                let end = offset
                    .checked_add(size as u64)
                    .filter(|end| *end <= i64::MAX as u64)
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::InvalidRecord,
                            "cloud object byte count exceeds its bound",
                        )
                    })?;
                let range = if size == 0 {
                    format!("bytes */{end}")
                } else {
                    format!(
                        "bytes {offset}-{}/{}",
                        end - 1,
                        if final_chunk {
                            end.to_string()
                        } else {
                            "*".into()
                        }
                    )
                };
                buffer.truncate(size);
                let response = self
                    .http
                    .put(session.clone())
                    .header("content-type", "application/octet-stream")
                    .header("content-range", range)
                    .header("content-length", size)
                    .body(buffer)
                    .send()
                    .await
                    .map_err(|_| ambiguous())?;
                if response.status().as_u16() == 412 {
                    return Err(precondition());
                }
                if final_chunk {
                    if !matches!(response.status().as_u16(), 200 | 201) {
                        return Err(ambiguous());
                    }
                    return completion(
                        &response_bytes(response).await?,
                        &self.bucket,
                        key.as_ref(),
                        end,
                    );
                }
                if response.status().as_u16() != 308 {
                    return Err(ambiguous());
                }
                let acknowledged = response
                    .headers()
                    .get("range")
                    .and_then(|h| h.to_str().ok());
                if acknowledged != Some(format!("bytes=0-{}", end - 1).as_str()) {
                    return Err(ambiguous());
                }
                offset = end;
            }
        }
        .await;
        if result.is_err() {
            // Cancel this session only. A final write with a lost response may
            // already have committed; never delete the destination to undo it.
            let _ = self
                .http
                .delete(session)
                .header("content-length", "0")
                .send()
                .await;
        }
        result
    }
}
fn named_selector(value: &str, limit: usize) -> bool {
    !value.is_empty()
        && value.len() <= limit
        && !value.starts_with('-')
        && value.bytes().all(|b| b.is_ascii_graphic())
}
fn bucket_metadata(bytes: &[u8], bucket: &str) -> Result<()> {
    #[derive(Default, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Billing {
        // The authenticated JSON API omits default-false properties, including
        // the entire billing object, for ordinary buckets. Null/wrong types
        // remain errors; explicit true is never admitted.
        #[serde(default, rename = "requesterPays")]
        requester_pays: bool,
    }
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Metadata {
        name: String,
        #[serde(default)]
        billing: Billing,
    }
    let invalid = || {
        Error::new(
            ErrorKind::Integrity,
            "GCS bucket eligibility metadata is invalid or unknown",
        )
    };
    // Strict parser rejects duplicate/unknown keys and missing bucket identity.
    let value = crate::json::parse(bytes).map_err(|_| invalid())?;
    let metadata: Metadata = serde_json::from_value(value).map_err(|_| invalid())?;
    if metadata.name != bucket {
        return Err(invalid());
    }
    if metadata.billing.requester_pays {
        return Err(Error::new(
            ErrorKind::Unsupported,
            "GCS requester-pays buckets cannot back views",
        ));
    }
    Ok(())
}
fn precondition() -> Error {
    Error::new(
        ErrorKind::PreconditionFailed,
        "GCS conditional write was rejected",
    )
}
fn generation(value: &str, allow_zero: bool) -> Result<u64> {
    let parsed = value
        .parse::<u64>()
        .ok()
        .filter(|n| (*n > 0 || allow_zero) && n.to_string() == value)
        .ok_or_else(|| Error::new(ErrorKind::InvalidRecord, "invalid GCS generation validator"))?;
    Ok(parsed)
}
fn completion(bytes: &[u8], bucket: &str, key: &str, size: u64) -> Result<Validator> {
    let value = crate::json::parse(bytes).map_err(|_| ambiguous())?;
    if value.get("bucket").and_then(serde_json::Value::as_str) != Some(bucket)
        || value.get("name").and_then(serde_json::Value::as_str) != Some(key)
        || value.get("size").and_then(serde_json::Value::as_str) != Some(size.to_string().as_str())
    {
        return Err(ambiguous());
    }
    let validator = value
        .get("generation")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(ambiguous)?;
    generation(validator, false).map_err(|_| ambiguous())?;
    Validator::new(validator).map_err(|_| ambiguous())
}
impl ConditionalUpload for GcsUpload {
    fn gcs_view_eligible(&self) -> Result<()> {
        self.view_eligible()
    }
    fn upload(
        &self,
        key: &RemotePath,
        expected: Option<&Validator>,
        source: &mut dyn Read,
    ) -> Result<Validator> {
        self.runtime.block_on(self.send(key, expected, source))
    }
}

#[cfg(test)]
mod tests {
    use super::super::mock::{Peer, Response};
    use super::*;
    fn mock(peer: &Peer) -> GcsUpload {
        let credentials = Arc::new(object_store::client::StaticCredentialProvider::new(
            object_store::gcp::GcpCredential {
                bearer: "synthetic-test-credential".into(),
            },
        ));
        GcsUpload {
            bucket: "bucket".into(),
            credentials,
            runtime: Arc::new(
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .enable_all()
                    .build()
                    .unwrap(),
            ),
            http: reqwest::Client::builder()
                .default_headers({
                    let mut headers = reqwest::header::HeaderMap::new();
                    headers.insert("x-goog-user-project", "named-project".parse().unwrap());
                    headers
                })
                .retry(reqwest::retry::never())
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap(),
            endpoint: Url::parse(&peer.url).unwrap(),
            view_selected: true,
            timeout: Duration::from_secs(2),
        }
    }
    #[test]
    fn gcs_view_eligibility_uses_one_authenticated_bounded_bucket_get() {
        let peer = Peer::start(|_| {
            vec![Some(Response::new(
                200,
                r#"{"name":"bucket","billing":{"requesterPays":false}}"#,
            ))]
        });
        let upload = mock(&peer);
        let runtime = upload.runtime.clone();
        let backend: Box<dyn Backend> = Box::new(CloudBackend {
            root: CloudRoot::parse("gs://bucket").unwrap(),
            store: Arc::new(object_store::memory::InMemory::new()),
            runtime,
            writer: Arc::new(upload),
            ranges: None,
        });
        backend.gcs_view_eligible().unwrap();
        let requests = peer.finish();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].line.starts_with("GET /storage/v1/b/bucket?"));
        assert!(
            requests[0]
                .line
                .contains("fields=name%2Cbilling%28requesterPays%29")
        );
        assert_eq!(
            requests[0].headers["authorization"],
            "Bearer synthetic-test-credential"
        );
        assert_eq!(requests[0].headers["x-goog-user-project"], "named-project");
        assert!(requests[0].body.is_empty());
    }
    #[test]
    fn gcs_json_api_omitted_default_billing_is_not_requester_pays() {
        for metadata in [
            r#"{"name":"bucket"}"#,
            r#"{"name":"bucket","billing":{}}"#,
            r#"{"name":"bucket","billing":{"requesterPays":false}}"#,
        ] {
            bucket_metadata(metadata.as_bytes(), "bucket").unwrap();
        }
    }
    #[test]
    fn gcs_view_eligibility_fails_closed_without_retries_or_private_diagnostics() {
        for (response, kind) in [
            (
                Some(Response::new(
                    200,
                    r#"{"name":"bucket","billing":{"requesterPays":true}}"#,
                )),
                ErrorKind::Unsupported,
            ),
            (
                Some(Response::new(
                    200,
                    r#"{"name":"other","billing":{"requesterPays":false}}"#,
                )),
                ErrorKind::Integrity,
            ),
            (
                Some(Response::new(200, r#"{"billing":{}}"#)),
                ErrorKind::Integrity,
            ),
            (
                Some(Response::new(200, r#"{"name":"bucket","billing":null}"#)),
                ErrorKind::Integrity,
            ),
            (
                Some(Response::new(
                    200,
                    r#"{"name":"bucket","billing":{"requesterPays":"false"}}"#,
                )),
                ErrorKind::Integrity,
            ),
            (
                Some(Response::new(
                    200,
                    r#"{"name":"bucket","billing":{"requesterPays":false,"unknown":1}}"#,
                )),
                ErrorKind::Integrity,
            ),
            (
                Some(Response::new(
                    200,
                    r#"{"name":"bucket","billing":{"requesterPays":false},"name":"bucket"}"#,
                )),
                ErrorKind::Integrity,
            ),
            (
                Some(Response::new(200, "private-provider-body")),
                ErrorKind::Integrity,
            ),
            (
                Some(Response::new(200, "x".repeat(RESPONSE_BYTES + 1))),
                ErrorKind::Integrity,
            ),
            (
                Some(Response::new(403, "private-provider-body")),
                ErrorKind::Io,
            ),
            (
                Some(
                    Response::new(302, "private-provider-body")
                        .header("location", "http://127.0.0.1:1/leak"),
                ),
                ErrorKind::Io,
            ),
            (None, ErrorKind::Io),
        ] {
            let peer = Peer::start(|_| vec![response]);
            let error = mock(&peer).gcs_view_eligible().unwrap_err();
            assert_eq!(error.kind, kind);
            assert_eq!(error.effect, WriteEffect::NoEffect);
            assert!(!format!("{error:?}").contains("private-provider-body"));
            assert!(!format!("{error:?}").contains("synthetic-test-credential"));
            assert_eq!(peer.finish().len(), 1);
        }
    }
    #[test]
    fn gcs_view_missing_selectors_refuse_before_authentication() {
        #[derive(Debug)]
        struct Never;
        #[async_trait::async_trait]
        impl object_store::client::CredentialProvider for Never {
            type Credential = object_store::gcp::GcpCredential;
            async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
                panic!("missing selectors must refuse before authentication")
            }
        }
        let peer = Peer::start(|_| vec![]);
        let mut upload = mock(&peer);
        upload.credentials = Arc::new(Never);
        for (account, project) in [
            (None, None),
            (Some("named@example.invalid"), None),
            (None, Some("named-project")),
            (Some(""), Some("named-project")),
            (Some("named@example.invalid"), Some("")),
        ] {
            let unselected = GcsUpload::new(
                "bucket",
                upload.credentials.clone(),
                upload.runtime.clone(),
                Duration::from_secs(2),
                project,
                account,
            )
            .unwrap();
            assert_eq!(
                unselected.gcs_view_eligible().unwrap_err().kind,
                ErrorKind::Unsupported
            );
        }
        assert!(peer.finish().is_empty());
        for selector in ["", "bad selector", "-flag", "bad\nvalue"] {
            assert!(!named_selector(selector, 320));
        }
    }
    #[test]
    fn gcs_view_deadline_bounds_authentication_before_bucket_request() {
        #[derive(Debug)]
        struct Slow;
        #[async_trait::async_trait]
        impl object_store::client::CredentialProvider for Slow {
            type Credential = object_store::gcp::GcpCredential;
            async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
                std::future::pending().await
            }
        }
        let peer = Peer::start(|_| vec![]);
        let mut upload = mock(&peer);
        upload.timeout = Duration::from_millis(10);
        upload.credentials = Arc::new(Slow);
        let error = upload.gcs_view_eligible().unwrap_err();
        assert_eq!(error.kind, ErrorKind::Io);
        assert_eq!(error.effect, WriteEffect::NoEffect);
        assert!(peer.finish().is_empty());
    }
    #[test]
    fn gcs_commit_evidence_requires_exact_coordinates_count_and_generation() {
        let valid =
            br#"{"bucket":"bucket","name":"a/b","size":"3","generation":"123","etag":"ignored"}"#;
        assert_eq!(
            completion(valid, "bucket", "a/b", 3).unwrap().as_str(),
            "123"
        );
        for (bucket, key, count) in [
            ("other", "a/b", 3),
            ("bucket", "other", 3),
            ("bucket", "a/b", 4),
        ] {
            assert_eq!(
                completion(valid, bucket, key, count).unwrap_err().effect,
                WriteEffect::MaybeApplied
            );
        }
        for bytes in [
            br#"{"bucket":"bucket","name":"a/b","size":"3","generation":"0"}"#.as_slice(),
            br#"{"bucket":"bucket","name":"a/b","size":"3","generation":"0123"}"#,
            br#"{"bucket":"bucket","name":"a/b","size":"3","generation":"123","generation":"456"}"#,
        ] {
            assert_eq!(
                completion(bytes, "bucket", "a/b", 3).unwrap_err().effect,
                WriteEffect::MaybeApplied
            );
        }
    }
    #[test]
    fn gcs_streams_bounded_chunks_with_fixed_condition_and_exact_acknowledgements() {
        let size = PART_BYTES + 7;
        let peer = Peer::start(|url| {
            vec![
                Some(Response::new(200, "").header(
                    "location",
                    format!("{url}/upload/storage/v1/b/bucket/o?upload_id=private-session"),
                )),
                Some(Response::new(308, "").header("range", format!("bytes=0-{}", PART_BYTES - 1))),
                Some(Response::new(
                    200,
                    format!(
                        r#"{{"bucket":"bucket","name":"prefix/key","size":"{size}","generation":"456"}}"#
                    ),
                )),
            ]
        });
        let upload = mock(&peer);
        let result = upload
            .upload(
                &RemotePath::parse("prefix/key").unwrap(),
                Some(&Validator::new("123").unwrap()),
                &mut std::io::Cursor::new(vec![42; size]),
            )
            .unwrap();
        assert_eq!(result.as_str(), "456");
        let requests = peer.finish();
        assert!(requests[0].line.contains("ifGenerationMatch=123"));
        assert!(requests[0].line.contains("name=prefix%2Fkey"));
        assert_eq!(
            requests[0].headers["authorization"],
            "Bearer synthetic-test-credential"
        );
        assert_eq!(
            requests[1].headers["content-range"],
            format!("bytes 0-{}/*", PART_BYTES - 1)
        );
        assert_eq!(
            requests[2].headers["content-range"],
            format!("bytes {PART_BYTES}-{}/{size}", size - 1)
        );
        assert_eq!(requests[1].body.len(), PART_BYTES);
        assert_eq!(requests[2].body.len(), 7);
        assert!(!requests[1].headers.contains_key("authorization"));
    }
    #[test]
    fn gcs_final_full_and_empty_chunks_commit_without_an_extra_request() {
        for size in [0, PART_BYTES] {
            let peer = Peer::start(|url| {
                vec![
                    Some(Response::new(200, "").header(
                        "location",
                        format!("{url}/upload/storage/v1/b/bucket/o?upload_id=session"),
                    )),
                    Some(Response::new(
                        200,
                        format!(
                            r#"{{"bucket":"bucket","name":"key","size":"{size}","generation":"1"}}"#
                        ),
                    )),
                ]
            });
            mock(&peer)
                .upload(
                    &RemotePath::parse("key").unwrap(),
                    None,
                    &mut std::io::Cursor::new(vec![42; size]),
                )
                .unwrap();
            let requests = peer.finish();
            assert_eq!(requests.len(), 2);
            assert!(requests[0].line.contains("ifGenerationMatch=0"));
            assert_eq!(requests[1].body.len(), size);
        }
    }
    #[test]
    fn gcs_lost_final_response_stale_condition_and_bad_partial_ack_are_not_retried() {
        for status in [None, Some(412), Some(308)] {
            let peer = Peer::start(|url| {
                vec![
                    Some(Response::new(200, "").header(
                        "location",
                        format!("{url}/upload/storage/v1/b/bucket/o?upload_id=session"),
                    )),
                    status.map(|s| Response::new(s, "")),
                    Some(Response::new(499, "")),
                ]
            });
            let size = if status == Some(308) {
                PART_BYTES + 1
            } else {
                3
            };
            let error = mock(&peer)
                .upload(
                    &RemotePath::parse("key").unwrap(),
                    None,
                    &mut std::io::Cursor::new(vec![42; size]),
                )
                .unwrap_err();
            assert_eq!(
                error.effect,
                if status == Some(412) {
                    WriteEffect::NoEffect
                } else {
                    WriteEffect::MaybeApplied
                }
            );
            let requests = peer.finish();
            assert_eq!(requests.len(), 3);
            assert!(requests[2].line.starts_with("DELETE "));
        }
    }
    #[test]
    fn gcs_rejects_cross_origin_session_and_wrong_bucket_before_sending_bytes() {
        let peer = Peer::start(|url| {
            vec![Some(Response::new(200, "").header(
                "location",
                format!("{url}/upload/storage/v1/b/other/o?upload_id=private"),
            ))]
        });
        assert_eq!(
            mock(&peer)
                .upload(
                    &RemotePath::parse("key").unwrap(),
                    None,
                    &mut &b"secret"[..]
                )
                .unwrap_err()
                .effect,
            WriteEffect::MaybeApplied
        );
        assert_eq!(peer.finish().len(), 1);
        let peer = Peer::start(|_| vec![]);
        let upload = mock(&peer);
        for uri in [
            "https://attacker.invalid/upload/storage/v1/b/bucket/o?upload_id=private",
            "https://storage.googleapis.com@attacker.invalid/upload/storage/v1/b/bucket/o?upload_id=private",
        ] {
            assert!(upload.session(uri).is_err());
        }
        peer.finish();
    }
}
