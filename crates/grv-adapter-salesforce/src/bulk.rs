//! Exact bounded Bulk query CSV. An empty cell has no universal null/string
//! meaning; a preflight representation proof must supply a per-field policy.
use crate::{Result, acquisition::MAX_SOURCE_UNIT_BYTES, http::HttpResponse, integrity};
use grv_adapter_api::TableContract;
use serde_json::{Map, Value};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmptyPolicy {
    /// Metadata/representation proof establishes empty means source null.
    Null,
    /// Metadata/representation proof establishes the source is never null.
    String,
    /// Representation cannot distinguish the two. Never invent a source value.
    Reject,
}

pub struct BulkPage {
    pub rows: Vec<Value>,
    pub next_locator: Option<String>,
}

fn csv(bytes: &[u8], columns: usize) -> Result<Vec<Vec<String>>> {
    if bytes.len() > MAX_SOURCE_UNIT_BYTES {
        return Err(integrity("Bulk CSV source unit exceeds resource budget"));
    }
    std::str::from_utf8(bytes).map_err(|_| integrity("Bulk CSV is not valid UTF-8"))?;
    let mut rows = Vec::new();
    let mut row = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        if rows.len() > 1000 {
            return Err(integrity("Bulk page exceeds requested row budget"));
        }
        if row.len() >= columns {
            return Err(integrity("Bulk CSV row exceeds projected width"));
        }
        let mut field = Vec::new();
        if bytes[at] == b'"' {
            at += 1;
            let mut closed = false;
            while at < bytes.len() {
                if bytes[at] == b'"' {
                    if bytes.get(at + 1) == Some(&b'"') {
                        field.push(b'"');
                        at += 2;
                    } else {
                        at += 1;
                        closed = true;
                        break;
                    }
                } else {
                    field.push(bytes[at]);
                    at += 1;
                }
            }
            if !closed {
                return Err(integrity("Bulk CSV quoted field is incomplete"));
            }
        } else {
            while at < bytes.len() && !matches!(bytes[at], b',' | b'\r' | b'\n') {
                if bytes[at] == b'"' {
                    return Err(integrity("Bulk CSV unquoted field contains a quote"));
                }
                field.push(bytes[at]);
                at += 1;
            }
        }
        row.push(
            String::from_utf8(field).map_err(|_| integrity("Bulk CSV field is invalid UTF-8"))?,
        );
        match bytes.get(at).copied() {
            Some(b',') => {
                at += 1;
                if at == bytes.len() {
                    row.push(String::new());
                    rows.push(row);
                    row = Vec::new();
                }
            }
            Some(b'\n') => {
                at += 1;
                rows.push(row);
                row = Vec::new();
            }
            Some(b'\r') => {
                if bytes.get(at + 1) != Some(&b'\n') {
                    return Err(integrity("Bulk CSV has an invalid line ending"));
                }
                at += 2;
                rows.push(row);
                row = Vec::new();
            }
            None => {
                rows.push(row);
                row = Vec::new();
            }
            _ => return Err(integrity("Bulk CSV has trailing text after a quoted field")),
        }
    }
    Ok(rows)
}

fn preflight_csv(bytes: &[u8], retained: usize, selectors: &[String], limit: usize) -> Result<()> {
    let max_key = selectors.iter().map(String::len).max().unwrap_or(0);
    let per_field = 1024usize
        .checked_add(max_key)
        .ok_or_else(|| integrity("Bulk source allocation overflow"))?;
    let mut charged = bytes
        .len()
        .checked_mul(3)
        .and_then(|size| size.checked_add(retained.max(bytes.len())))
        .and_then(|size| size.checked_add(256 * 1024))
        .ok_or_else(|| integrity("Bulk source allocation overflow"))?;
    let mut at = 0;
    let mut quoted = false;
    while at < bytes.len() {
        match bytes[at] {
            b'"' => {
                if quoted && bytes.get(at + 1) == Some(&b'"') {
                    at += 1;
                } else {
                    quoted = !quoted;
                }
            }
            b',' | b'\n' if !quoted => {
                charged = charged
                    .checked_add(per_field)
                    .ok_or_else(|| integrity("Bulk source allocation overflow"))?;
            }
            _ => (),
        }
        if charged > limit {
            return Err(integrity(
                "Bulk encoded and decoded page exceed aggregate source budget",
            ));
        }
        at += 1;
    }
    if charged
        .checked_add(per_field)
        .is_none_or(|size| size > limit)
    {
        return Err(integrity(
            "Bulk encoded and decoded page exceed aggregate source budget",
        ));
    }
    Ok(())
}

pub fn decode_page(
    response: HttpResponse,
    contract: &TableContract,
    sources: &[String],
    policies: &[EmptyPolicy],
) -> Result<BulkPage> {
    decode_page_bounded(response, contract, sources, policies, MAX_SOURCE_UNIT_BYTES)
}
pub fn decode_page_bounded(
    response: HttpResponse,
    contract: &TableContract,
    sources: &[String],
    policies: &[EmptyPolicy],
    source_limit: usize,
) -> Result<BulkPage> {
    contract
        .validate()
        .map_err(|_| integrity("invalid Bulk extraction contract"))?;
    if sources.len() != contract.columns.len() || policies.len() != sources.len() {
        return Err(integrity("Bulk projection differs from contract"));
    }
    if response.status != 200 {
        return Err(integrity("Bulk result response is not successful"));
    }
    preflight_csv(
        &response.body,
        response.body.capacity(),
        sources,
        source_limit,
    )?;
    let unique_sources: BTreeSet<_> = sources
        .iter()
        .map(|source| source.to_ascii_lowercase())
        .collect();
    let rows = csv(&response.body, unique_sources.len())?;
    let header = rows
        .first()
        .cloned()
        .ok_or_else(|| integrity("Bulk CSV lacks a header"))?;
    let unique: BTreeSet<_> = header
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect();
    let expected = unique_sources;
    if unique.len() != header.len() || unique != expected {
        return Err(integrity(
            "Bulk CSV headers differ from requested projection",
        ));
    }
    let reported = response
        .headers
        .get("sforce-numberofrecords")
        .and_then(|v| v.parse::<u64>().ok())
        .ok_or_else(|| integrity("Bulk CSV result count is missing or invalid"))?;
    if reported > 1000 || reported != rows.len().saturating_sub(1) as u64 {
        return Err(integrity(
            "Bulk CSV row count disagrees with response facts",
        ));
    }
    let locator = response
        .headers
        .get("sforce-locator")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| integrity("Bulk CSV pagination locator is missing"))?;
    let next_locator = if locator == "null" {
        None
    } else {
        Some(locator.clone())
    };
    let mut decoded = Vec::new();
    for row in rows.into_iter().skip(1) {
        if row.len() != header.len() {
            return Err(integrity("Bulk CSV row width differs from header"));
        }
        let mut object = Map::new();
        for (name, text) in header.iter().zip(row) {
            let index = sources
                .iter()
                .position(|source| source.eq_ignore_ascii_case(name))
                .expect("verified source header");
            let logical = &contract.columns[index].logical_type;
            let value = if text.is_empty() {
                match policies[index] {
                    EmptyPolicy::Null => Value::Null,
                    EmptyPolicy::String => Value::String(text),
                    EmptyPolicy::Reject => {
                        return Err(integrity(
                            "Bulk CSV empty cell lacks a lossless null/string representation proof",
                        ));
                    }
                }
            } else if logical == "boolean" {
                match text.as_str() {
                    "true" => Value::Bool(true),
                    "false" => Value::Bool(false),
                    _ => return Err(integrity("Bulk CSV boolean source is invalid")),
                }
            } else {
                Value::String(text)
            };
            object.insert(name.clone(), value);
        }
        decoded.push(Value::Object(object));
    }
    Ok(BulkPage {
        rows: decoded,
        next_locator,
    })
}

/// One nonresumable job. Each bounded result page is consumed before fetching
/// the next, and checkpoint acknowledgement precedes polling/result download.
pub struct BulkAcquisition {
    identity: crate::journal::AcquisitionIdentity,
    job_id: String,
    sources: Vec<String>,
    policies: Vec<EmptyPolicy>,
    contract: TableContract,
    expected: Option<u64>,
    captured: u64,
    pending: Option<(u64, Option<String>)>,
    locator: Option<String>,
}
impl BulkAcquisition {
    pub fn begin(
        journal: &crate::journal::Journal,
        identity: crate::journal::AcquisitionIdentity,
        query: &str,
        session: &crate::auth::AuthenticatedSession,
        http: &mut impl crate::acquisition::SourceHttp,
        contract: &TableContract,
        sources: &[String],
    ) -> Result<Self> {
        if identity.transport != crate::config::Transport::Bulk
            || identity.connection_identity != session.org_id().identity()
            || identity.query_sha256 != grv_types::sha256(query.as_bytes()).as_str()
        {
            return Err(crate::Error::new(
                "REQUEST_MISMATCH",
                "fixed Bulk query identity differs",
            ));
        }
        // Preparation evidence precedes metadata GETs. Only creation_intent
        // authorizes the one POST, and a reopened intent never authorizes it.
        journal.prepare(&identity)?;
        let policies = crate::metadata::bulk_policies(
            http,
            session,
            &identity.api_version,
            &identity.object,
            contract,
            sources,
        )?;
        journal.creation_intent(&identity)?;
        let record =
            crate::acquisition::create_bulk_after_intent(journal, &identity, query, session, http)?;
        Ok(Self {
            identity,
            job_id: record.job_ids()[0].clone(),
            sources: sources.to_vec(),
            policies,
            contract: contract.clone(),
            expected: None,
            captured: 0,
            pending: None,
            locator: None,
        })
    }
    pub fn take_rows(
        &mut self,
        journal: &crate::journal::Journal,
        session: &crate::auth::AuthenticatedSession,
        http: &mut impl crate::acquisition::SourceHttp,
        stop: &grv_adapter_sdk::StopToken,
    ) -> Result<Vec<Value>> {
        if !journal.required(&self.identity)?.checkpoint_acked
            || self.pending.is_some()
            || session.org_id().identity() != self.identity.connection_identity
        {
            return Err(integrity(
                "Bulk page requires acknowledged active acquisition",
            ));
        }
        if self.expected.is_none() {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(600);
            loop {
                if stop.is_cancelled() {
                    return Err(crate::Error::new(
                        "EXTRACTION_INCOMPLETE",
                        "Bulk acquisition stopped",
                    ));
                }
                if std::time::Instant::now() >= deadline {
                    return Err(crate::Error::new(
                        "EXTRACTION_INCOMPLETE",
                        "Bulk job polling deadline expired",
                    ));
                }
                let status = http.bulk_status(session, &self.identity.api_version, &self.job_id)?;
                if status.get("id").and_then(Value::as_str) != Some(self.job_id.as_str()) {
                    return Err(integrity("Bulk status job identity differs"));
                }
                match status.get("state").and_then(Value::as_str) {
                    Some("JobComplete") => {
                        let count = status
                            .get("numberRecordsProcessed")
                            .and_then(Value::as_u64)
                            .ok_or_else(|| {
                                integrity("completed Bulk job lacks exact record count")
                            })?;
                        journal.expected_rows(&self.identity, count)?;
                        self.expected = Some(count);
                        break;
                    }
                    Some("Failed" | "Aborted") => {
                        return Err(crate::Error::new(
                            "EXTRACTION_INCOMPLETE",
                            "Bulk job failed or was aborted",
                        ));
                    }
                    Some("UploadComplete" | "InProgress" | "Open") => {
                        std::thread::sleep(std::time::Duration::from_millis(100))
                    }
                    _ => return Err(integrity("Bulk status has unknown job state")),
                }
            }
        }
        if stop.is_cancelled() {
            return Err(crate::Error::new(
                "EXTRACTION_INCOMPLETE",
                "Bulk acquisition stopped",
            ));
        }
        let page = decode_page_bounded(
            http.bulk_page(
                session,
                &self.identity.api_version,
                &self.job_id,
                self.locator.as_deref(),
            )?,
            &self.contract,
            &self.sources,
            &self.policies,
            http.source_budget(),
        )?;
        let rows = page.rows.len() as u64;
        if self
            .captured
            .checked_add(rows)
            .is_none_or(|count| count > self.expected.expect("job count"))
        {
            return Err(integrity("Bulk result page exceeds completed job count"));
        }
        self.pending = Some((rows, page.next_locator));
        Ok(page.rows)
    }
    pub fn finish_rows(&mut self, journal: &crate::journal::Journal) -> Result<bool> {
        let (rows, next) = self
            .pending
            .take()
            .ok_or_else(|| integrity("no consumed Bulk page is pending"))?;
        self.captured = self
            .captured
            .checked_add(rows)
            .ok_or_else(|| integrity("Bulk row count overflow"))?;
        if let Some(locator) = next {
            journal.bulk_locator(&self.identity, &locator)?;
            self.locator = Some(locator);
            Ok(false)
        } else {
            journal.finish(&self.identity, self.captured)?;
            Ok(true)
        }
    }
}
