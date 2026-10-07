//! Verified TLS HTTP via a supervised curl helper. Credentials and POST bodies
//! travel through private stdin; retries, redirects, curlrc and debug logging
//! are disabled. Results are paged and bounded, never whole-job downloads.
use crate::{
    Error, Result,
    acquisition::{BulkCreation, MAX_SOURCE_UNIT_BYTES, RestPage, SourceHttp},
    auth::{AuthenticatedSession, OrgId},
    config::api_version_valid,
    integrity,
    runtime::{self, Cancellation, ProcessSpec},
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, ffi::OsString, time::Duration};

const MAX_HEADERS: usize = 64 * 1024;
pub struct HttpResponse {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}
pub trait HttpExecutor: Send {
    fn configure_resources(&mut self, _: &grv_adapter_api::Resources) -> Result<()> {
        Ok(())
    }
    fn source_budget(&self) -> usize {
        MAX_SOURCE_UNIT_BYTES
    }
    fn request(
        &mut self,
        session: &AuthenticatedSession,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
    ) -> Result<HttpResponse>;
}

pub struct CurlHttp {
    pub program: OsString,
    pub supervisor: Option<OsString>,
    pub cancellation: Cancellation,
    pub timeout: Duration,
    pub resources: grv_adapter_api::Resources,
}
impl Default for CurlHttp {
    fn default() -> Self {
        Self {
            program: "curl".into(),
            supervisor: None,
            cancellation: Default::default(),
            timeout: Duration::from_secs(60),
            resources: Default::default(),
        }
    }
}
fn quote(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}
pub fn percent_encode(value: &str) -> String {
    let mut output = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            output.push(char::from(byte));
        } else {
            use std::fmt::Write;
            write!(&mut output, "%{byte:02X}").expect("string write");
        }
    }
    output
}

impl HttpExecutor for CurlHttp {
    fn configure_resources(&mut self, resources: &grv_adapter_api::Resources) -> Result<()> {
        resources
            .validate()
            .map_err(|_| integrity("invalid HTTP budgets"))?;
        self.resources = resources.clone();
        Ok(())
    }
    fn source_budget(&self) -> usize {
        self.resources.max_source_unit_bytes.get() as usize
    }
    fn request(
        &mut self,
        session: &AuthenticatedSession,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
    ) -> Result<HttpResponse> {
        self.cancellation.check()?;
        if !matches!(method, "GET" | "POST")
            || !path.starts_with("/services/")
            || !path.is_ascii()
            || path.contains(['\n', '\r', '\\', '"', '#'])
            || body.is_some_and(|b| b.len() > MAX_SOURCE_UNIT_BYTES)
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "invalid Salesforce HTTP request",
            ));
        }
        let config = session.with_credentials(|origin, token| -> Result<Vec<u8>> {
            let accept = if path.contains("/results?") { "text/csv" } else { "application/json" };
            let mut config = format!("url = \"{}{}\"\nrequest = \"{method}\"\nheader = \"Authorization: Bearer {}\"\nheader = \"Accept: {accept}\"\n", quote(origin.trim_end_matches('/')), quote(path), quote(token));
            if let Some(body) = body {
                let text = std::str::from_utf8(body).map_err(|_| integrity("HTTP JSON body is invalid UTF-8"))?;
                config.push_str(&format!("header = \"Content-Type: application/json\"\ndata = \"{}\"\n", quote(text)));
            }
            Ok(config.into_bytes())
        })?;
        let output = runtime::run(
            ProcessSpec {
                program: self.program.clone(),
                supervisor: self.supervisor.clone(),
                args: [
                    "--disable",
                    "--silent",
                    "--show-error",
                    "--retry",
                    "0",
                    "--max-redirs",
                    "0",
                    "--proto",
                    "=https",
                    "--proto-redir",
                    "=https",
                    "--connect-timeout",
                    "5",
                    "--dump-header",
                    "-",
                    "--config",
                    "-",
                ]
                .into_iter()
                .map(OsString::from)
                .collect(),
                env: vec![("CURL_HOME".into(), "/nonexistent-grv-curl-config".into())],
                stdin: config,
                stdout_limit: self.source_budget() + MAX_HEADERS,
                timeout: self.timeout,
            },
            &self.cancellation,
        )?;
        if !output.status.success() {
            return Err(Error::new(
                "ADAPTER_FAILURE",
                "Salesforce HTTP transport failed",
            ));
        }
        decode_response(output.stdout)
    }
}

pub fn decode_response(mut bytes: Vec<u8>) -> Result<HttpResponse> {
    let mut offset = 0;
    let mut final_headers = None;
    while bytes
        .get(offset..)
        .is_some_and(|rest| rest.starts_with(b"HTTP/"))
    {
        let remaining = &bytes[offset..];
        let end = remaining
            .windows(4)
            .position(|v| v == b"\r\n\r\n")
            .ok_or_else(|| integrity("HTTP header is incomplete"))?;
        if offset + end + 4 > MAX_HEADERS {
            return Err(integrity("HTTP headers exceed resource budget"));
        }
        let text = std::str::from_utf8(&remaining[..end])
            .map_err(|_| integrity("HTTP header is invalid"))?;
        let mut lines = text.split("\r\n");
        let status = lines
            .next()
            .and_then(|line| line.split_ascii_whitespace().nth(1))
            .and_then(|v| v.parse::<u16>().ok())
            .filter(|n| (100..=599).contains(n))
            .ok_or_else(|| integrity("HTTP status is invalid"))?;
        let mut headers = BTreeMap::new();
        for (index, line) in lines.enumerate() {
            if index >= 256 {
                return Err(integrity("HTTP header count exceeds resource budget"));
            }
            let (key, value) = line
                .split_once(':')
                .ok_or_else(|| integrity("HTTP header is invalid"))?;
            let key = key.to_ascii_lowercase();
            if headers.contains_key(&key)
                && matches!(
                    key.as_str(),
                    "sforce-locator" | "sforce-numberofrecords" | "content-length"
                )
            {
                return Err(integrity("HTTP source facts are duplicated"));
            }
            headers.insert(key, value.trim().into());
        }
        offset += end + 4;
        final_headers = Some((status, headers));
    }
    let (status, headers) =
        final_headers.ok_or_else(|| integrity("missing HTTP response headers"))?;
    if bytes.len() - offset > MAX_SOURCE_UNIT_BYTES {
        return Err(integrity("HTTP source unit exceeds resource budget"));
    }
    bytes.drain(..offset);
    Ok(HttpResponse {
        status,
        headers,
        body: bytes,
    })
}

pub struct SalesforceHttp<H = CurlHttp> {
    pub executor: H,
}
fn success(response: HttpResponse) -> Result<HttpResponse> {
    if !(200..=299).contains(&response.status) {
        return Err(Error::new(
            "ADAPTER_FAILURE",
            "Salesforce HTTP request was rejected",
        ));
    }
    Ok(response)
}
fn version(version: &str) -> Result<()> {
    if api_version_valid(version) {
        Ok(())
    } else {
        Err(integrity("invalid HTTP API version"))
    }
}
fn job(job: &str) -> Result<()> {
    if matches!(job.len(), 15 | 18) && job.bytes().all(|c| c.is_ascii_alphanumeric()) {
        Ok(())
    } else {
        Err(integrity("invalid Bulk job identity"))
    }
}
impl<H: HttpExecutor> SourceHttp for SalesforceHttp<H> {
    fn configure_resources(&mut self, resources: &grv_adapter_api::Resources) -> Result<()> {
        self.executor.configure_resources(resources)
    }
    fn source_budget(&self) -> usize {
        self.executor.source_budget()
    }
    fn describe(
        &mut self,
        session: &AuthenticatedSession,
        version: &str,
        object: &str,
    ) -> Result<Value> {
        SalesforceHttp::describe(self, session, version, object)
    }
    fn bulk_status(
        &mut self,
        session: &AuthenticatedSession,
        version: &str,
        job: &str,
    ) -> Result<Value> {
        SalesforceHttp::bulk_status(self, session, version, job)
    }
    fn bulk_page(
        &mut self,
        session: &AuthenticatedSession,
        version: &str,
        job: &str,
        locator: Option<&str>,
    ) -> Result<HttpResponse> {
        SalesforceHttp::bulk_page(self, session, version, job, locator)
    }
    fn rest_query(
        &mut self,
        session: &AuthenticatedSession,
        api_version: &str,
        query: &str,
        all_rows: bool,
    ) -> Result<RestPage> {
        version(api_version)?;
        let endpoint = if all_rows { "queryAll" } else { "query" };
        let response = success(self.executor.request(
            session,
            "GET",
            &format!(
                "/services/data/{api_version}/{endpoint}?q={}",
                percent_encode(query)
            ),
            None,
        )?)?;
        RestPage::parse_with_resources(
            &response.body,
            response.body.capacity(),
            self.executor.source_budget(),
        )
    }
    fn rest_next(&mut self, session: &AuthenticatedSession, locator: &str) -> Result<RestPage> {
        if !locator.starts_with("/services/data/") || locator.contains(['?', '#']) {
            return Err(integrity("invalid REST locator"));
        }
        let response = success(self.executor.request(session, "GET", locator, None)?)?;
        RestPage::parse_with_resources(
            &response.body,
            response.body.capacity(),
            self.executor.source_budget(),
        )
    }
    fn bulk_create(
        &mut self,
        session: &AuthenticatedSession,
        api_version: &str,
        query: &str,
        all_rows: bool,
    ) -> BulkCreation {
        if version(api_version).is_err() {
            return BulkCreation::Rejected;
        }
        let body = serde_json::to_vec(&json!({"operation":if all_rows {"queryAll"} else {"query"},"query":query,"contentType":"CSV","columnDelimiter":"COMMA","lineEnding":"LF"})).expect("serializable creation request");
        let response = match self.executor.request(
            session,
            "POST",
            &format!("/services/data/{api_version}/jobs/query"),
            Some(&body),
        ) {
            Ok(response) => response,
            Err(_) => return BulkCreation::Ambiguous,
        };
        if (400..=499).contains(&response.status) && response.status != 408 {
            return BulkCreation::Rejected;
        }
        if !(200..=299).contains(&response.status) {
            return BulkCreation::Ambiguous;
        }
        let value = match crate::source_json::parse(
            &response.body,
            response.body.capacity(),
            self.executor.source_budget(),
        ) {
            Ok(value) => value,
            Err(_) => return BulkCreation::Ambiguous,
        };
        match value
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| job(id).is_ok())
        {
            Some(id) => BulkCreation::Created { job_id: id.into() },
            None => BulkCreation::Ambiguous,
        }
    }
}

impl<H: HttpExecutor> SalesforceHttp<H> {
    pub fn verify_org(
        &mut self,
        session: &AuthenticatedSession,
        api_version: &str,
    ) -> Result<OrgId> {
        let page = self.rest_query(
            session,
            api_version,
            "SELECT Id FROM Organization LIMIT 1",
            false,
        )?;
        if !page.done
            || page.total_size != 1
            || page.records.len() != 1
            || page.next_records_url.is_some()
        {
            return Err(integrity("authenticated org verification is incomplete"));
        }
        OrgId::parse(
            page.records[0]
                .get("Id")
                .and_then(Value::as_str)
                .ok_or_else(|| integrity("authenticated org identity missing"))?,
        )
    }
    pub fn describe(
        &mut self,
        session: &AuthenticatedSession,
        api_version: &str,
        object: &str,
    ) -> Result<Value> {
        version(api_version)?;
        if !crate::config::identifier(object) {
            return Err(integrity("invalid object API name"));
        }
        let response = success(self.executor.request(
            session,
            "GET",
            &format!("/services/data/{api_version}/sobjects/{object}/describe"),
            None,
        )?)?;
        crate::source_json::parse(
            &response.body,
            response.body.capacity(),
            self.executor.source_budget(),
        )
        .map_err(|_| integrity("invalid object description"))
    }
    pub fn bulk_status(
        &mut self,
        session: &AuthenticatedSession,
        api_version: &str,
        job_id: &str,
    ) -> Result<Value> {
        version(api_version)?;
        job(job_id)?;
        let response = success(self.executor.request(
            session,
            "GET",
            &format!("/services/data/{api_version}/jobs/query/{job_id}"),
            None,
        )?)?;
        crate::source_json::parse(
            &response.body,
            response.body.capacity(),
            self.executor.source_budget(),
        )
        .map_err(|_| integrity("invalid Bulk job status"))
    }
    pub fn bulk_page(
        &mut self,
        session: &AuthenticatedSession,
        api_version: &str,
        job_id: &str,
        locator: Option<&str>,
    ) -> Result<HttpResponse> {
        version(api_version)?;
        job(job_id)?;
        let mut path =
            format!("/services/data/{api_version}/jobs/query/{job_id}/results?maxRecords=1000");
        if let Some(locator) = locator {
            path.push_str("&locator=");
            path.push_str(&percent_encode(locator));
        }
        success(self.executor.request(session, "GET", &path, None)?)
    }
}
