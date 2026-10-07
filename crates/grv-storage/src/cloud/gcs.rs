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
}
impl GcsUpload {
    pub fn new(
        bucket: &str,
        credentials: GcpCredentialProvider,
        runtime: Arc<Runtime>,
        timeout: Duration,
        project: Option<&str>,
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
        })
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
                .retry(reqwest::retry::never())
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap(),
            endpoint: Url::parse(&peer.url).unwrap(),
        }
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
