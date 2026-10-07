//! Multipart completion is the default sole commit point. A successful part
//! upload or initiation does not mean the destination object exists.
//! Explicit single-PUT mode buffers up to its configured byte limit before any request and never
//! retries/falls back after a multipart failure or uncertain PUT outcome.
use super::*;
use bytes::Bytes;
use object_store::{
    aws::AmazonS3,
    multipart::{MultipartStore, PartId},
    signer::{HeaderName, HeaderValue, Method, SignedUrlOptions, Signer},
};
use serde::Deserialize;

const SINGLE_PUT_BYTES: usize = 1024 * 1024 * 1024;
const MAX_SINGLE_PUT_BYTES: u64 = 5_000_000_000;

pub(super) fn single_put_limit(value: Option<&str>) -> Result<usize> {
    let invalid = || {
        Error::new(
            ErrorKind::InvalidRecord,
            "S3 single PUT byte limit must be an ASCII decimal integer in 1..=5000000000 and fit memory address space",
        )
    };
    let Some(value) = value else {
        return Ok(SINGLE_PUT_BYTES);
    };
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid());
    }
    let bytes = value.parse::<u64>().map_err(|_| invalid())?;
    if bytes == 0 || bytes > MAX_SINGLE_PUT_BYTES {
        return Err(invalid());
    }
    let bytes = usize::try_from(bytes).map_err(|_| invalid())?;
    if bytes
        .checked_add(1)
        .is_none_or(|size| size > isize::MAX as usize)
    {
        return Err(invalid());
    }
    Ok(bytes)
}

pub(super) struct S3Upload {
    store: AmazonS3,
    runtime: Arc<Runtime>,
    http: reqwest::Client,
    timeout: Duration,
    single_put: bool,
    single_put_max_bytes: usize,
}
impl S3Upload {
    pub fn new(
        store: AmazonS3,
        runtime: Arc<Runtime>,
        timeout: Duration,
        single_put: bool,
        single_put_max_bytes: usize,
    ) -> Result<Self> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(timeout)
            .https_only(true)
            .build()
            .map_err(|_| Error::new(ErrorKind::Io, "cloud HTTP client initialization failed"))?;
        Ok(Self {
            store,
            runtime,
            http,
            timeout,
            single_put,
            single_put_max_bytes,
        })
    }
    async fn single_put(
        &self,
        key: &RemotePath,
        expected: Option<&Validator>,
        body: Vec<u8>,
    ) -> Result<Validator> {
        let (header, condition) = match expected {
            Some(validator) => ("if-match", validator.as_str()),
            None => ("if-none-match", "*"),
        };
        let condition = HeaderValue::from_str(condition).map_err(|_| {
            Error::new(ErrorKind::InvalidRecord, "invalid S3 conditional validator")
        })?;
        let options = SignedUrlOptions::new()
            .with_signed_header(HeaderName::from_static(header), condition.clone());
        let url = self
            .store
            .signed_url_opts(
                Method::PUT,
                key,
                self.timeout + Duration::from_secs(60),
                &options,
            )
            .await
            .map_err(|e| remote_error(e, false))?;
        let response = self
            .http
            .put(url)
            .header(header, condition)
            .body(body)
            .send()
            .await
            .map_err(|_| ambiguous())?;
        if response.status().as_u16() == 412 {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "S3 conditional write was rejected",
            ));
        }
        if !response.status().is_success() {
            return Err(ambiguous());
        }
        let mut etags = response.headers().get_all("etag").iter();
        let etag = etags.next().ok_or_else(ambiguous)?;
        if etags.next().is_some() {
            return Err(ambiguous());
        }
        let etag = etag.to_str().map_err(|_| ambiguous())?;
        if etag.is_empty() || etag.len() > 1024 {
            return Err(ambiguous());
        }
        Validator::new(etag.to_owned()).map_err(|_| ambiguous())
    }
    async fn complete(
        &self,
        key: &RemotePath,
        upload: &str,
        parts: &[PartId],
        expected: Option<&Validator>,
    ) -> Result<Validator> {
        let (header, condition) = match expected {
            Some(validator) => ("if-match", validator.as_str()),
            None => ("if-none-match", "*"),
        };
        let condition = HeaderValue::from_str(condition).map_err(|_| {
            Error::new(ErrorKind::InvalidRecord, "invalid S3 conditional validator")
        })?;
        let options = SignedUrlOptions::new()
            .with_query([("uploadId", upload)])
            .with_signed_header(HeaderName::from_static(header), condition.clone())
            .with_signed_header(
                HeaderName::from_static("content-type"),
                HeaderValue::from_static("application/xml"),
            );
        let url = self
            .store
            .signed_url_opts(
                Method::POST,
                key,
                self.timeout + Duration::from_secs(60),
                &options,
            )
            .await
            .map_err(|e| remote_error(e, false))?;
        let body = complete_body(parts)?;
        let response = self
            .http
            .post(url)
            .header(header, condition)
            .header("content-type", "application/xml")
            .body(body)
            .send()
            .await
            .map_err(|_| ambiguous())?;
        let status = response.status();
        if status.as_u16() == 412 {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "S3 conditional write was rejected",
            ));
        }
        if !status.is_success() {
            return Err(ambiguous());
        }
        let bytes = response_bytes(response).await?;
        decode_completion(&bytes)
    }
}
fn complete_body(parts: &[PartId]) -> Result<String> {
    if parts.is_empty() || parts.len() > 10_000 {
        return Err(Error::new(
            ErrorKind::Unsupported,
            "S3 multipart part count is out of range",
        ));
    }
    let mut body = String::from("<CompleteMultipartUpload>");
    for (index, part) in parts.iter().enumerate() {
        if part.content_id.len() > 1024 {
            return Err(Error::new(
                ErrorKind::Integrity,
                "S3 part validator exceeds its bound",
            ));
        }
        body.push_str(&format!(
            "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
            index + 1,
            quick_xml::escape::escape(&part.content_id)
        ));
    }
    body.push_str("</CompleteMultipartUpload>");
    Ok(body)
}
fn decode_completion(bytes: &[u8]) -> Result<Validator> {
    #[derive(Deserialize)]
    #[serde(rename = "CompleteMultipartUploadResult")]
    struct Complete {
        #[serde(rename = "ETag")]
        etag: String,
    }
    // A 200 status may contain an Error XML document. Check the root first;
    // serde's root-name annotation alone does not enforce it.
    let mut reader = quick_xml::Reader::from_reader(bytes);
    let mut depth = 0usize;
    let mut root = false;
    loop {
        match reader.read_event().map_err(|_| ambiguous())? {
            quick_xml::events::Event::Start(event) => {
                if depth == 0 {
                    if root || event.local_name().as_ref() != b"CompleteMultipartUploadResult" {
                        return Err(ambiguous());
                    }
                    root = true;
                }
                depth += 1;
                if depth > 64 {
                    return Err(ambiguous());
                }
            }
            quick_xml::events::Event::End(_) => {
                depth = depth.checked_sub(1).ok_or_else(ambiguous)?;
            }
            quick_xml::events::Event::Empty(_) if depth > 0 => {}
            quick_xml::events::Event::Decl(_) if !root => {}
            quick_xml::events::Event::Text(event)
                if depth > 0 || event.as_ref().iter().all(u8::is_ascii_whitespace) => {}
            quick_xml::events::Event::CData(_) if depth > 0 => {}
            quick_xml::events::Event::GeneralRef(_) if depth > 0 => {}
            quick_xml::events::Event::Comment(_) => {}
            quick_xml::events::Event::Eof if root && depth == 0 => break,
            _ => return Err(ambiguous()),
        }
    }
    let result: Complete = quick_xml::de::from_reader(bytes).map_err(|_| ambiguous())?;
    if result.etag.is_empty() || result.etag.len() > 1024 {
        return Err(ambiguous());
    }
    Validator::new(result.etag).map_err(|_| ambiguous())
}
impl ConditionalUpload for S3Upload {
    fn upload(
        &self,
        key: &RemotePath,
        expected: Option<&Validator>,
        source: &mut dyn Read,
    ) -> Result<Validator> {
        if self.single_put {
            // Read the complete admitted source before signing/contacting S3.
            // One extra byte distinguishes the exact limit from an overshoot.
            let bound = self.single_put_max_bytes + 1;
            let mut body = Vec::new();
            let mut buffer = [0u8; 64 * 1024];
            let mut source = source.take(bound as u64);
            loop {
                let size = source.read(&mut buffer).map_err(io_error)?;
                if size == 0 {
                    break;
                }
                let required = body.len() + size;
                if required > self.single_put_max_bytes {
                    return Err(Error::new(
                        ErrorKind::Unsupported,
                        "S3 single PUT source exceeds configured byte limit",
                    ));
                }
                if required > body.capacity() {
                    let capacity = required.max(body.capacity().saturating_mul(2)).min(bound);
                    body.try_reserve_exact(capacity - body.len()).map_err(|_| {
                        Error::new(
                            ErrorKind::Io,
                            "S3 single PUT buffer allocation failed before upload",
                        )
                    })?;
                }
                body.extend_from_slice(&buffer[..size]);
            }
            return self.runtime.block_on(self.single_put(key, expected, body));
        }
        self.runtime.block_on(async {
            let upload = self
                .store
                .create_multipart(key)
                .await
                .map_err(|e| remote_error(e, false))?;
            let result = async {
                let mut parts = Vec::new();
                loop {
                    let mut buffer = vec![0u8; PART_BYTES];
                    let mut size = 0usize;
                    while size < buffer.len() {
                        let n = source.read(&mut buffer[size..]).map_err(io_error)?;
                        if n == 0 {
                            break;
                        }
                        size += n;
                    }
                    if size == 0 && !parts.is_empty() {
                        break;
                    }
                    if parts.len() == 10_000 {
                        return Err(Error::new(
                            ErrorKind::Unsupported,
                            "S3 multipart upload exceeds its part limit",
                        ));
                    }
                    buffer.truncate(size);
                    parts.push(
                        self.store
                            .put_part(key, &upload, parts.len(), Bytes::from(buffer).into())
                            .await
                            .map_err(|e| remote_error(e, false))?,
                    );
                    if size < PART_BYTES {
                        break;
                    }
                }
                self.complete(key, &upload, &parts, expected).await
            }
            .await;
            if result.is_err() {
                let _ = self.store.abort_multipart(key, &upload).await;
            }
            result
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::mock::{Peer, Response};
    use super::*;
    fn mock(peer: &Peer) -> S3Upload {
        mock_at(&peer.url)
    }
    fn mock_at(url: &str) -> S3Upload {
        let store = AmazonS3Builder::new()
            .with_bucket_name("bucket")
            .with_region("us-east-1")
            .with_endpoint(url)
            .with_allow_http(true)
            .with_access_key_id("synthetic-key")
            .with_secret_access_key("synthetic-secret")
            .with_retry(RetryConfig {
                max_retries: 0,
                ..Default::default()
            })
            .with_client_options(
                ClientOptions::new()
                    .with_allow_http(true)
                    .with_timeout(Duration::from_secs(2)),
            )
            .build()
            .unwrap();
        S3Upload {
            store,
            runtime: Arc::new(
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .enable_all()
                    .build()
                    .unwrap(),
            ),
            http: reqwest::Client::builder()
                .no_proxy()
                .retry(reqwest::retry::never())
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap(),
            timeout: Duration::from_secs(2),
            single_put: false,
            // Keep wire tests small; validate the real default separately.
            single_put_max_bytes: 16 * 1024 * 1024,
        }
    }
    #[test]
    fn configurable_single_put_limits_validate_before_provider_access() {
        assert_eq!(single_put_limit(None).unwrap(), 1_073_741_824);
        assert_eq!(single_put_limit(Some("1048576")).unwrap(), 1024 * 1024);
        if usize::BITS >= 64 {
            assert_eq!(
                single_put_limit(Some("5000000000")).unwrap(),
                usize::try_from(MAX_SINGLE_PUT_BYTES).unwrap()
            );
        }
        for value in [
            "",
            "0",
            "-1",
            "+1",
            " 16",
            "16 ",
            "16MiB",
            "1.5",
            "５",
            "5000000001",
            "18446744073709551616",
        ] {
            let error = CloudBackend::open(
                "s3://synthetic-bucket",
                CloudOptions {
                    s3_upload_mode: Some("single-put".into()),
                    s3_single_put_max_bytes: Some(value.into()),
                    ..Default::default()
                },
            )
            .unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidRecord);
            assert_eq!(error.effect, WriteEffect::NoEffect);
        }
    }

    #[test]
    fn custom_single_put_limit_exact_boundary_oversize_and_above_previous_default() {
        for limit in [7, 16 * 1024 * 1024 + 1] {
            let peer =
                Peer::start(|_| vec![Some(Response::new(200, "").header("etag", "\"new\""))]);
            let mut upload = mock(&peer);
            upload.single_put = true;
            upload.single_put_max_bytes = limit;
            let body = vec![b'x'; limit];
            upload
                .upload(
                    &RemotePath::parse("key").unwrap(),
                    None,
                    &mut body.as_slice(),
                )
                .unwrap();
            let requests = peer.finish();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].body, body);
        }
        let peer = Peer::start(|_| vec![]);
        let mut upload = mock(&peer);
        upload.single_put = true;
        upload.single_put_max_bytes = 7;
        let error = upload
            .upload(
                &RemotePath::parse("key").unwrap(),
                None,
                &mut &b"12345678"[..],
            )
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Unsupported);
        assert_eq!(error.effect, WriteEffect::NoEffect);
        assert!(peer.finish().is_empty());
    }

    #[test]
    fn single_put_signs_conditions_and_sends_exact_body_in_one_request() {
        for expected in [None, Some(Validator::new("\"old\"").unwrap())] {
            for body in [b"data".as_slice(), b"".as_slice()] {
                let peer =
                    Peer::start(|_| vec![Some(Response::new(200, "").header("etag", "\"new\""))]);
                let mut upload = mock(&peer);
                upload.single_put = true;
                assert_eq!(
                    upload
                        .upload(
                            &RemotePath::parse("key").unwrap(),
                            expected.as_ref(),
                            &mut &body[..],
                        )
                        .unwrap()
                        .as_str(),
                    "\"new\""
                );
                let requests = peer.finish();
                assert_eq!(requests.len(), 1);
                assert!(requests[0].line.starts_with("PUT "));
                assert!(!requests[0].line.contains("uploadId="));
                assert!(!requests[0].line.contains("uploads"));
                assert_eq!(requests[0].body, body);
                let header = if expected.is_some() {
                    "if-match"
                } else {
                    "if-none-match"
                };
                assert_eq!(
                    requests[0].headers[header],
                    expected.as_ref().map_or("*", Validator::as_str)
                );
                assert!(
                    requests[0].line.contains(header),
                    "condition must be signed"
                );
            }
        }
    }
    #[test]
    fn single_put_admits_exact_limit() {
        let etag = "x".repeat(1024);
        let peer = Peer::start(|_| vec![Some(Response::new(200, "").header("etag", &etag))]);
        let mut upload = mock(&peer);
        upload.single_put = true;
        let limit = upload.single_put_max_bytes;
        let validator = upload
            .upload(
                &RemotePath::parse("key").unwrap(),
                None,
                &mut std::io::repeat(b'x').take(limit as u64),
            )
            .unwrap();
        assert_eq!(validator.as_str(), etag);
        let requests = peer.finish();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].body.len(), limit);
        assert!(requests[0].body.iter().all(|&b| b == b'x'));
    }
    #[test]
    fn single_put_overshoot_and_source_failure_make_no_request() {
        use std::net::TcpListener;
        struct BrokenSource(bool);
        impl Read for BrokenSource {
            fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
                if self.0 {
                    return Err(std::io::Error::other("synthetic source failure"));
                }
                self.0 = true;
                bytes[0] = b'x';
                Ok(1)
            }
        }
        // Keep a listener open so even an attempted request is observable.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let mut upload = mock_at(&format!("http://{}", listener.local_addr().unwrap()));
        upload.single_put = true;
        let mut oversized = std::io::repeat(b'x').take((upload.single_put_max_bytes + 100) as u64);
        let error = upload
            .upload(&RemotePath::parse("key").unwrap(), None, &mut oversized)
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Unsupported);
        assert_eq!(error.effect, WriteEffect::NoEffect);
        assert_eq!(oversized.limit(), 99, "must read only limit + 1 bytes");
        let error = upload
            .upload(
                &RemotePath::parse("key").unwrap(),
                None,
                &mut BrokenSource(false),
            )
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Io);
        assert_eq!(error.effect, WriteEffect::NoEffect);
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
    #[test]
    fn single_put_stale_condition_is_rejected_and_uncertain_outcomes_are_not_retried() {
        for response in [
            None,
            Some(Response::new(412, "")),
            Some(Response::new(500, "")),
            Some(Response::new(307, "").header("location", "http://127.0.0.1:1/redirect")),
            Some(Response::new(200, "")),
            Some(Response::new(200, "").header("etag", "")),
            Some(Response::new(200, "").header("etag", "x".repeat(1025))),
            Some(
                Response::new(200, "")
                    .header("etag", "a")
                    .header("etag", "b"),
            ),
        ] {
            let rejected = response.as_ref().is_some_and(|r| r.status == 412);
            let peer = Peer::start(|_| vec![response]);
            let mut upload = mock(&peer);
            upload.single_put = true;
            let error = upload
                .upload(
                    &RemotePath::parse("key").unwrap(),
                    Some(&Validator::new("\"stale\"").unwrap()),
                    &mut &b"data"[..],
                )
                .unwrap_err();
            assert_eq!(
                error.kind,
                if rejected {
                    ErrorKind::PreconditionFailed
                } else {
                    ErrorKind::Io
                }
            );
            assert_eq!(
                error.effect,
                if rejected {
                    WriteEffect::NoEffect
                } else {
                    WriteEffect::MaybeApplied
                }
            );
            let requests = peer.finish();
            assert_eq!(requests.len(), 1);
            assert!(requests[0].line.starts_with("PUT "));
            assert_eq!(requests[0].headers["if-match"], "\"stale\"");
        }
    }
    #[test]
    fn invalid_s3_upload_mode_is_rejected_without_provider_access() {
        let error = CloudBackend::open(
            "s3://bucket",
            CloudOptions {
                s3_upload_mode: Some("automatic-fallback".into()),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidRecord);
    }
    fn initiated() -> Option<Response> {
        Some(Response::new(
            200,
            "<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>upload</UploadId></InitiateMultipartUploadResult>",
        ))
    }
    fn part() -> Option<Response> {
        Some(Response::new(200, "").header("etag", "\"part\""))
    }
    #[test]
    fn s3_completion_requires_a_real_success_document_and_escapes_part_etags() {
        let body = complete_body(&[PartId {
            content_id: "\"a&b\"".into(),
        }])
        .unwrap();
        assert!(body.contains("&quot;a&amp;b&quot;"));
        let result = decode_completion(br#"<CompleteMultipartUploadResult><ETag>"abc"</ETag><Location>ignored</Location></CompleteMultipartUploadResult>"#).unwrap();
        assert_eq!(result.as_str(), "\"abc\"");
        for bytes in [b"<Error><Code>InternalError</Code></Error>".as_slice(), b"<CompleteMultipartUploadResult/>", b"garbage", b"<CompleteMultipartUploadResult><ETag></ETag></CompleteMultipartUploadResult>", b"<CompleteMultipartUploadResult><ETag>a</ETag></CompleteMultipartUploadResult><Error/>", b"<!DOCTYPE foo><CompleteMultipartUploadResult><ETag>a</ETag></CompleteMultipartUploadResult>"] {
            assert_eq!(decode_completion(bytes).unwrap_err().effect, WriteEffect::MaybeApplied);
        }
    }
    #[test]
    fn s3_conditional_commit_headers_are_signed_and_applied_at_completion() {
        for expected in [None, Some(Validator::new("\"old\"").unwrap())] {
            let peer = Peer::start(|_| {
                vec![
                    initiated(),
                    part(),
                    Some(Response::new(
                        200,
                        "<CompleteMultipartUploadResult><ETag>&quot;new&quot;</ETag></CompleteMultipartUploadResult>",
                    )),
                ]
            });
            assert_eq!(
                mock(&peer)
                    .upload(
                        &RemotePath::parse("key").unwrap(),
                        expected.as_ref(),
                        &mut &b"data"[..]
                    )
                    .unwrap()
                    .as_str(),
                "\"new\""
            );
            let requests = peer.finish();
            assert!(requests[0].line.contains("uploads"));
            assert!(requests[1].line.contains("partNumber=1"));
            assert_eq!(requests[1].body, b"data");
            assert!(requests[2].line.starts_with("POST "));
            assert!(requests[2].line.contains("uploadId=upload"));
            let header = if expected.is_some() {
                "if-match"
            } else {
                "if-none-match"
            };
            assert_eq!(
                requests[2].headers[header],
                expected.as_ref().map_or("*", Validator::as_str)
            );
            assert!(
                requests[2].line.contains(header),
                "condition must be in the signed header set"
            );
        }
    }
    #[test]
    fn s3_lost_completion_and_200_error_preserve_ambiguity_and_never_retry_commit() {
        for completion in [
            None,
            Some(Response::new(
                200,
                "<Error><Code>InternalError</Code></Error>",
            )),
            Some(Response::new(412, "")),
        ] {
            let rejected = completion.as_ref().is_some_and(|r| r.status == 412);
            let peer = Peer::start(|_| {
                vec![
                    initiated(),
                    part(),
                    completion,
                    Some(Response::new(204, "")),
                ]
            });
            let error = mock(&peer)
                .upload(&RemotePath::parse("key").unwrap(), None, &mut &b"data"[..])
                .unwrap_err();
            assert_eq!(
                error.effect,
                if rejected {
                    WriteEffect::NoEffect
                } else {
                    WriteEffect::MaybeApplied
                }
            );
            let requests = peer.finish();
            assert_eq!(requests.len(), 4);
            assert!(requests[3].line.starts_with("DELETE "));
            assert!(requests[3].line.contains("uploadId=upload"));
        }
    }
}
