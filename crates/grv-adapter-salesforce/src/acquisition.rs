//! Mockable source acquisition. Source access always follows durable intent;
//! REST bootstrap rows stay private until checkpoint acknowledgement.
use crate::{
    Error, Result,
    auth::AuthenticatedSession,
    config::Transport,
    integrity,
    journal::{AcquisitionIdentity, AcquisitionRecord, AcquisitionState, Journal},
};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const MAX_SOURCE_UNIT_BYTES: usize = 32 * 1024 * 1024;

#[cfg(test)]
mod resource_tests {
    use super::*;
    #[test]
    fn decoded_mock_rows_honor_offered_source_budget_before_emission() {
        let row: serde_json::Map<String, Value> = (0..512)
            .map(|i| (format!("c{i}"), Value::String("x".into())))
            .collect();
        let page = RestPage {
            total_size: 1,
            done: true,
            records: vec![Value::Object(row)],
            next_records_url: None,
        };
        assert!(serde_json::to_vec(&page.records).unwrap().len() < 16 * 1024);
        assert!(page.validate("v66.0", 512 * 1024).is_err());
        assert!(page.validate("v66.0", MAX_SOURCE_UNIT_BYTES).is_ok());
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RestPage {
    pub total_size: u64,
    pub done: bool,
    pub records: Vec<Value>,
    pub next_records_url: Option<String>,
}

impl RestPage {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        Self::parse_with_resources(bytes, bytes.len(), MAX_SOURCE_UNIT_BYTES)
    }
    pub(crate) fn parse_with_resources(
        bytes: &[u8],
        retained: usize,
        limit: usize,
    ) -> Result<Self> {
        if bytes.len() > MAX_SOURCE_UNIT_BYTES {
            return Err(integrity("REST source unit exceeds budget"));
        }
        let value = crate::source_json::parse(bytes, retained, limit)?;
        let Value::Object(mut object) = value else {
            return Err(integrity("invalid REST query response"));
        };
        if object.keys().any(|key| {
            !matches!(
                key.as_str(),
                "totalSize" | "done" | "records" | "nextRecordsUrl"
            )
        }) {
            return Err(integrity("unknown REST query field"));
        }
        let total_size = object
            .remove("totalSize")
            .and_then(|value| value.as_u64())
            .ok_or_else(|| integrity("invalid REST count"))?;
        let done = object
            .remove("done")
            .and_then(|value| value.as_bool())
            .ok_or_else(|| integrity("invalid REST completeness"))?;
        let Some(Value::Array(records)) = object.remove("records") else {
            return Err(integrity("invalid REST records"));
        };
        let next_records_url = match object.remove("nextRecordsUrl") {
            None | Some(Value::Null) => None,
            Some(Value::String(locator)) => Some(locator),
            _ => return Err(integrity("invalid REST locator")),
        };
        Ok(Self {
            total_size,
            done,
            records,
            next_records_url,
        })
    }
    fn validate(&self, version: &str, limit: usize) -> Result<()> {
        if self.total_size > i64::MAX as u64
            || self.records.len() as u64 > self.total_size
            || self.records.iter().any(|v| !v.is_object())
            || self.done == self.next_records_url.is_some()
        {
            return Err(integrity("REST page completeness or count disagreement"));
        }
        if let Some(locator) = &self.next_records_url {
            let prefix = format!("/services/data/{version}/query/");
            if !locator.strip_prefix(&prefix).is_some_and(|tail| {
                !tail.is_empty()
                    && tail
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
            }) {
                return Err(integrity("REST locator is not a same-version query URI"));
            }
        }
        // Typed mocks share exactly the same bounded unit contract as decoding.
        let overhead = 256usize * 1024
            + self.records.capacity() * std::mem::size_of::<Value>()
            + self.next_records_url.as_ref().map_or(0, String::capacity);
        let size = self.records.iter().try_fold(overhead, |sum, row| {
            sum.checked_add(crate::source_json::heap_upper_bound(row)?)
                .ok_or_else(|| integrity("REST source unit size overflow"))
        })?;
        if size > limit.min(MAX_SOURCE_UNIT_BYTES) {
            return Err(integrity("REST source unit exceeds budget"));
        }
        Ok(())
    }
}

pub enum BulkCreation {
    Created {
        job_id: String,
    },
    /// A definitive server rejection establishes no successful creation.
    Rejected,
    /// Includes timeout/disconnect, malformed success, or unresolved response.
    Ambiguous,
}

/// HTTP implementations never automatically retry creation or initial REST
/// queries. Errors must be redacted, and credential transport stays private.
pub trait SourceHttp {
    fn configure_resources(&mut self, _: &grv_adapter_api::Resources) -> Result<()> {
        Ok(())
    }
    fn source_budget(&self) -> usize {
        MAX_SOURCE_UNIT_BYTES
    }
    fn describe(&mut self, _: &AuthenticatedSession, _: &str, _: &str) -> Result<Value> {
        Err(Error::new(
            "UNSUPPORTED_CAPABILITY",
            "source metadata lookup is unavailable",
        ))
    }
    fn bulk_status(&mut self, _: &AuthenticatedSession, _: &str, _: &str) -> Result<Value> {
        Err(Error::new(
            "UNSUPPORTED_CAPABILITY",
            "Bulk status lookup is unavailable",
        ))
    }
    fn bulk_page(
        &mut self,
        _: &AuthenticatedSession,
        _: &str,
        _: &str,
        _: Option<&str>,
    ) -> Result<crate::http::HttpResponse> {
        Err(Error::new(
            "UNSUPPORTED_CAPABILITY",
            "Bulk result lookup is unavailable",
        ))
    }
    fn rest_query(
        &mut self,
        session: &AuthenticatedSession,
        api_version: &str,
        query: &str,
        all_rows: bool,
    ) -> Result<RestPage>;
    fn rest_next(&mut self, session: &AuthenticatedSession, locator: &str) -> Result<RestPage>;
    fn bulk_create(
        &mut self,
        session: &AuthenticatedSession,
        api_version: &str,
        query: &str,
        all_rows: bool,
    ) -> BulkCreation;
}

fn verify_request(
    identity: &AcquisitionIdentity,
    query: &str,
    session: &AuthenticatedSession,
    transport: Transport,
) -> Result<()> {
    let digest = format!("{:x}", Sha256::digest(query.as_bytes()));
    if identity.transport != transport
        || identity.query_sha256 != digest
        || identity.connection_identity != session.org_id().identity()
    {
        return Err(Error::new(
            "REQUEST_MISMATCH",
            "query or authenticated org differs from fixed acquisition",
        ));
    }
    Ok(())
}

pub struct RestAcquisition {
    identity: AcquisitionIdentity,
    snapshot_id: uuid::Uuid,
    page: Option<RestPage>,
    seen_locators: std::collections::HashSet<String>,
    captured_rows: u64,
    pending: Option<(u64, bool, Option<String>)>,
}

impl RestAcquisition {
    pub fn begin(
        journal: &Journal,
        identity: AcquisitionIdentity,
        query: &str,
        session: &AuthenticatedSession,
        http: &mut impl SourceHttp,
    ) -> Result<Self> {
        verify_request(&identity, query, session, Transport::Rest)?;
        journal.begin(&identity)?; // durable, before the first source request
        Self::query_after_intent(journal, identity, query, session, http)
    }

    pub fn begin_projected(
        journal: &Journal,
        identity: AcquisitionIdentity,
        query: &str,
        session: &AuthenticatedSession,
        http: &mut impl SourceHttp,
        contract: &grv_adapter_api::TableContract,
        selectors: &[String],
    ) -> Result<Self> {
        verify_request(&identity, query, session, Transport::Rest)?;
        journal.prepare(&identity)?;
        crate::metadata::rest_projection(
            http,
            session,
            &identity.api_version,
            &identity.object,
            contract,
            selectors,
        )?;
        journal.creation_intent(&identity)?;
        Self::query_after_intent(journal, identity, query, session, http)
    }

    fn query_after_intent(
        journal: &Journal,
        identity: AcquisitionIdentity,
        query: &str,
        session: &AuthenticatedSession,
        http: &mut impl SourceHttp,
    ) -> Result<Self> {
        let page = http
            .rest_query(session, &identity.api_version, query, identity.all_rows)
            .map_err(|_| Error::new("ADAPTER_FAILURE", "Salesforce REST query failed"))?;
        page.validate(&identity.api_version, http.source_budget())?;
        let jobs = page.next_records_url.iter().cloned().collect();
        let record = journal.created(&identity, jobs, Some(page.total_size))?;
        Ok(Self {
            identity,
            snapshot_id: record.snapshot_id,
            page: Some(page),
            seen_locators: Default::default(),
            captured_rows: 0,
            pending: None,
        })
    }

    pub fn checkpoint(&self, journal: &Journal) -> Result<AcquisitionRecord> {
        journal.required(&self.identity)
    }

    /// The caller validates/persists parent checkpoint acknowledgement first.
    pub fn acknowledge(&self, journal: &Journal, snapshot_id: uuid::Uuid) -> Result<()> {
        journal.acknowledge(&self.identity, snapshot_id)
    }

    pub fn take_rows(&mut self, journal: &Journal) -> Result<Vec<Value>> {
        let record = journal.required(&self.identity)?;
        if !record.checkpoint_acked
            || record.snapshot_id != self.snapshot_id
            || self.pending.is_some()
            || !matches!(record.state, AcquisitionState::Active { .. })
        {
            return Err(Error::new(
                "PROTOCOL_FAILURE",
                "page consumption requires acknowledged active acquisition",
            ));
        }
        let page = self
            .page
            .take()
            .ok_or_else(|| Error::new("EXTRACTION_INCOMPLETE", "REST page is unavailable"))?;
        self.pending = Some((page.records.len() as u64, page.done, page.next_records_url));
        Ok(page.records)
    }

    /// Advance only after the private page's rows have entered the credited
    /// producer. A stopped stream never starts this page's next GET.
    pub fn finish_rows(
        &mut self,
        journal: &Journal,
        session: &AuthenticatedSession,
        http: &mut impl SourceHttp,
    ) -> Result<bool> {
        if session.org_id().identity() != self.identity.connection_identity {
            return Err(Error::new(
                "REQUEST_MISMATCH",
                "source org changed during acquisition",
            ));
        }
        let (rows, done, locator) = self
            .pending
            .take()
            .ok_or_else(|| Error::new("PROTOCOL_FAILURE", "no consumed REST page is pending"))?;
        let record = journal.required(&self.identity)?;
        let expected = match record.state {
            AcquisitionState::Active { expected_rows, .. } => expected_rows,
            _ => return Err(integrity("REST acquisition is not active")),
        };
        self.captured_rows = self
            .captured_rows
            .checked_add(rows)
            .ok_or_else(|| integrity("REST row count overflow"))?;
        if expected.is_some_and(|expected| self.captured_rows > expected) {
            return Err(integrity("REST captured more rows than reported"));
        }
        if done {
            journal.finish(&self.identity, self.captured_rows)?;
            return Ok(true);
        }
        let locator = locator.ok_or_else(|| integrity("incomplete REST page has no locator"))?;
        if !self.seen_locators.insert(locator.clone()) {
            return Err(integrity("REST query locator repeated"));
        }
        journal.record_locator(&self.identity, &locator)?;
        let next = http
            .rest_next(session, &locator)
            .map_err(|_| Error::new("ADAPTER_FAILURE", "Salesforce REST pagination failed"))?;
        next.validate(&self.identity.api_version, http.source_budget())?;
        if expected != Some(next.total_size) {
            return Err(integrity("REST totalSize changed during acquisition"));
        }
        self.page = Some(next);
        Ok(false)
    }

    /// One bounded page per call. The sink is the later credited SDK producer;
    /// it must not successfully return until that page is consumed/acknowledged.
    /// A sink failure makes this in-memory stream unusable; retry cannot requery.
    pub fn emit_page(
        &mut self,
        journal: &Journal,
        session: &AuthenticatedSession,
        http: &mut impl SourceHttp,
        sink: &mut impl FnMut(&[Value]) -> Result<()>,
    ) -> Result<bool> {
        let record = journal.required(&self.identity)?;
        if !record.checkpoint_acked || record.snapshot_id != self.snapshot_id {
            return Err(Error::new(
                "PROTOCOL_FAILURE",
                "rows require checkpoint acknowledgement",
            ));
        }
        if session.org_id().identity() != self.identity.connection_identity {
            return Err(Error::new(
                "REQUEST_MISMATCH",
                "source org changed during acquisition",
            ));
        }
        if !matches!(record.state, AcquisitionState::Active { .. }) {
            return Err(Error::new("PROTOCOL_FAILURE", "acquisition is not active"));
        }
        let page = self.page.take().ok_or_else(|| {
            Error::new("EXTRACTION_INCOMPLETE", "REST stream stopped or completed")
        })?;
        let expected_rows = match record.state {
            AcquisitionState::Active { expected_rows, .. } => expected_rows,
            _ => unreachable!(),
        };
        self.captured_rows = self
            .captured_rows
            .checked_add(page.records.len() as u64)
            .ok_or_else(|| integrity("REST row count overflow"))?;
        if expected_rows.is_some_and(|expected| expected < self.captured_rows) {
            return Err(integrity("REST captured more rows than reported"));
        }
        if !page.records.is_empty() {
            sink(&page.records)?;
        }
        if page.done {
            journal.finish(&self.identity, self.captured_rows)?;
            return Ok(true);
        }
        let locator = page.next_records_url.expect("validated incomplete page");
        if !self.seen_locators.insert(locator.clone()) {
            return Err(integrity("REST query locator repeated"));
        }
        journal.record_locator(&self.identity, &locator)?;
        let next = http
            .rest_next(session, &locator)
            .map_err(|_| Error::new("ADAPTER_FAILURE", "Salesforce REST pagination failed"))?;
        next.validate(&self.identity.api_version, http.source_budget())?;
        if expected_rows != Some(next.total_size) {
            return Err(integrity("REST totalSize changed during acquisition"));
        }
        self.page = Some(next);
        Ok(false)
    }
}

pub fn begin_bulk(
    journal: &Journal,
    identity: &AcquisitionIdentity,
    query: &str,
    session: &AuthenticatedSession,
    http: &mut impl SourceHttp,
) -> Result<AcquisitionRecord> {
    verify_request(identity, query, session, Transport::Bulk)?;
    journal.begin(identity)?; // crash after here is ambiguous: never repeat POST
    create_bulk_after_intent(journal, identity, query, session, http)
}

pub(crate) fn create_bulk_after_intent(
    journal: &Journal,
    identity: &AcquisitionIdentity,
    query: &str,
    session: &AuthenticatedSession,
    http: &mut impl SourceHttp,
) -> Result<AcquisitionRecord> {
    verify_request(identity, query, session, Transport::Bulk)?;
    if journal.required(identity)?.state != AcquisitionState::Intent {
        return Err(integrity("Bulk creation requires fresh intent"));
    }
    match http.bulk_create(session, &identity.api_version, query, identity.all_rows) {
        BulkCreation::Created { job_id } if crate::journal::valid_bulk_job(&job_id) => {
            journal.created(identity, vec![job_id], None)
        }
        BulkCreation::Rejected => {
            journal.creation_failed(identity, false)?;
            Err(Error::new(
                "ADAPTER_FAILURE",
                "Bulk creation was definitively rejected",
            ))
        }
        BulkCreation::Created { .. } | BulkCreation::Ambiguous => {
            journal.creation_failed(identity, true)?;
            Err(Error::new(
                "OUTCOME_UNKNOWN",
                "Bulk creation is unresolved; its POST must not be repeated",
            ))
        }
    }
}
