//! SDK-integrated REST/Bulk producer with source projection proofs, durable
//! checkpoint acknowledgements, credited batches and stopped completion.
use crate::{
    Error,
    acquisition::{RestAcquisition, SourceHttp},
    auth::AuthenticatedSession,
    config::{Options, Source, Transport, generate_query},
    journal::{AcquisitionIdentity, AcquisitionRecord, Journal},
    predicate::SoqlPredicateCompiler,
    row,
};
use grv_adapter_api::{
    Checkpoint, CheckpointTable, ExtractRequest, Resources, SourceCompletion, U64, Uuid,
};
use grv_adapter_sdk::{Extraction, ExtractionEvent, StopToken};
use grv_types::{ErrorCode, PublicError};
use serde_json::{Value, json};
use std::sync::Arc;

pub(crate) fn public(error: Error) -> PublicError {
    let code = match error.code {
        "INVALID_ARGUMENT" => ErrorCode::InvalidArgument,
        "INVALID_DECLARATION" => ErrorCode::InvalidDeclaration,
        "INTEGRITY_FAILURE" => ErrorCode::IntegrityFailure,
        "PROTOCOL_FAILURE" => ErrorCode::ProtocolFailure,
        "STATE_CONFLICT" => ErrorCode::StateConflict,
        "REQUEST_MISMATCH" => ErrorCode::RequestMismatch,
        "OUTCOME_UNKNOWN" => ErrorCode::OutcomeUnknown,
        "EXTRACTION_INCOMPLETE" => ErrorCode::ExtractionIncomplete,
        "UNSUPPORTED_CAPABILITY" => ErrorCode::UnsupportedCapability,
        _ => ErrorCode::AdapterFailure,
    };
    PublicError {
        code,
        message: error.message,
        retryable: false,
        object: None,
    }
}
fn count(rows: u64) -> crate::Result<U64> {
    U64::new(rows).map_err(|_| crate::integrity("source count exceeds wire counter"))
}

pub struct RestProducer<H: SourceHttp + Send> {
    journal: Journal,
    request: ExtractRequest,
    session: Arc<AuthenticatedSession>,
    http: H,
    options: Options,
    api_version: String,
    batch_limit: usize,
    scratch_limit: usize,
    table_index: usize,
    current: Option<Acquisition>,
    current_identity: Option<AcquisitionIdentity>,
    acknowledged: bool,
    rows: Vec<Value>,
    cursor: usize,
    page_consumed: bool,
    records: Vec<AcquisitionRecord>,
    completed: bool,
}
impl<H: SourceHttp + Send> RestProducer<H> {
    pub fn new(
        journal: Journal,
        request: ExtractRequest,
        session: Arc<AuthenticatedSession>,
        mut http: H,
        api_version: String,
        resources: &Resources,
    ) -> crate::Result<Self> {
        resources
            .validate()
            .map_err(|_| crate::integrity("invalid producer resources"))?;
        http.configure_resources(resources)?;
        if request.resume.is_some() {
            return Err(Error::new(
                "EXTRACTION_INCOMPLETE",
                "nonresumable Salesforce acquisition cannot reopen",
            ));
        }
        if request.connection_identity != session.org_id().identity()
            || request.adapter_identity.name.as_str() != "salesforce"
            || request.tables.is_empty()
        {
            return Err(Error::new(
                "REQUEST_MISMATCH",
                "Salesforce extraction identity differs",
            ));
        }
        let options: Options = serde_json::from_value(request.options.clone())
            .map_err(|_| crate::invalid("invalid Salesforce extraction options"))?;
        for table in &request.tables {
            table
                .contract
                .validate()
                .map_err(|_| crate::integrity("invalid extraction table contract"))?;
            let source: Source = serde_json::from_value(table.source.clone())
                .map_err(|_| crate::invalid("invalid Salesforce source"))?;
            let sources = row::selectors(table)?;
            generate_query(&source, &unique(&sources), &mut SoqlPredicateCompiler)?;
            let identity = identity(
                &request,
                table.name.as_str(),
                &source.object,
                "",
                &api_version,
                &options,
            )?;
            // Existing acquisition evidence is refused before any source access.
            // Its actual query hash is checked again when acquisition begins.
            let mut identity = identity;
            let query = generate_query(&source, &unique(&sources), &mut SoqlPredicateCompiler)?;
            identity.query_sha256 = grv_types::sha256(query.as_bytes()).as_str().into();
            if let Some(record) = journal.load(&identity)? {
                if options.transport == Transport::Bulk
                    && matches!(
                        record.state,
                        crate::journal::AcquisitionState::Intent
                            | crate::journal::AcquisitionState::OutcomeUnknown
                    )
                {
                    return Err(Error::new(
                        "OUTCOME_UNKNOWN",
                        "Bulk creation is unresolved; its POST must not be repeated",
                    ));
                }
                return Err(Error::new(
                    "EXTRACTION_INCOMPLETE",
                    "Salesforce acquisition already exists; reuse accepted capture or start another attempt",
                ));
            }
        }
        Ok(Self {
            journal,
            request,
            session,
            http,
            options,
            api_version,
            batch_limit: resources.max_batch_bytes.get() as usize,
            scratch_limit: resources.max_scratch_bytes.get() as usize,
            table_index: 0,
            current: None,
            current_identity: None,
            acknowledged: false,
            rows: vec![],
            cursor: 0,
            page_consumed: false,
            records: vec![],
            completed: false,
        })
    }
    fn job(&self) -> Value {
        let mut ids = Vec::new();
        for record in &self.records {
            for id in record.job_ids() {
                if !ids.contains(id) {
                    ids.push(id.clone());
                }
            }
        }
        if let Some(identity) = &self.current_identity
            && let Ok(record) = self.journal.required(identity)
        {
            for id in record.job_ids() {
                if !ids.contains(id) {
                    ids.push(id.clone());
                }
            }
        }
        json!({"transport":if self.options.transport == Transport::Bulk {"bulk"} else {"rest"},"api_version":self.api_version,"job_ids":ids})
    }
    fn checkpoint(&self) -> crate::Result<Checkpoint> {
        let mut records = self.records.clone();
        records.push(
            self.journal
                .required(self.current_identity.as_ref().expect("active identity"))?,
        );
        let tables = records
            .iter()
            .map(|record| {
                Ok(CheckpointTable {
                    table: grv_types::Name::new(&record.identity.table)
                        .map_err(|_| crate::integrity("invalid table name"))?,
                    snapshot_id: record.snapshot_id.to_string(),
                    reopenable: false,
                    source_identity: record.source_identity()?,
                    capture_start: record.capture_start.clone(),
                })
            })
            .collect::<crate::Result<Vec<_>>>()?;
        Ok(Checkpoint {
            attempt_id: self.request.attempt_id.clone(),
            adapter_identity: self.request.adapter_identity.clone(),
            connection_identity: self.request.connection_identity.clone(),
            tables,
            job: self.job(),
        })
    }
    fn next_inner(&mut self, stop: &StopToken) -> crate::Result<Option<ExtractionEvent>> {
        if stop.is_cancelled() {
            return Err(Error::new(
                "EXTRACTION_INCOMPLETE",
                "Salesforce extraction stopped",
            ));
        }
        if self.completed {
            return Ok(None);
        }
        loop {
            if self.table_index == self.request.tables.len() {
                self.completed = true;
                let start = self
                    .records
                    .first()
                    .expect("nonempty tables")
                    .capture_start
                    .clone();
                let end = self
                    .records
                    .last()
                    .and_then(AcquisitionRecord::capture_window)
                    .expect("completed tables")
                    .end;
                return Ok(Some(ExtractionEvent::SourceComplete(SourceCompletion {
                    job: self.job(),
                    capture_window: grv_adapter_api::CaptureWindow { start, end },
                    adapter_result: json!({}),
                })));
            }
            let table = &self.request.tables[self.table_index];
            if self.current.is_none() {
                let source: Source = serde_json::from_value(table.source.clone())
                    .map_err(|_| crate::invalid("invalid Salesforce source"))?;
                let sources = row::selectors(table)?;
                let query = generate_query(&source, &unique(&sources), &mut SoqlPredicateCompiler)?;
                let identity = identity(
                    &self.request,
                    table.name.as_str(),
                    &source.object,
                    &query,
                    &self.api_version,
                    &self.options,
                )?;
                let current = if self.options.transport == Transport::Bulk {
                    Acquisition::Bulk(crate::bulk::BulkAcquisition::begin(
                        &self.journal,
                        identity.clone(),
                        &query,
                        &self.session,
                        &mut self.http,
                        &table.contract,
                        &sources,
                    )?)
                } else {
                    Acquisition::Rest(RestAcquisition::begin_projected(
                        &self.journal,
                        identity.clone(),
                        &query,
                        &self.session,
                        &mut self.http,
                        &table.contract,
                        &sources,
                    )?)
                };
                self.current = Some(current);
                self.current_identity = Some(identity);
                self.acknowledged = false;
                return Ok(Some(ExtractionEvent::Checkpoint {
                    checkpoint_id: Uuid::v4(),
                    payload: self.checkpoint()?,
                }));
            }
            let current = self.current.as_mut().expect("active acquisition");
            if !self.acknowledged {
                // The SDK calls next only after the preceding checkpoint ACK.
                let identity = self.current_identity.as_ref().expect("active identity");
                let snapshot = self.journal.required(identity)?.snapshot_id;
                self.journal.acknowledge(identity, snapshot)?;
                self.acknowledged = true;
                self.rows =
                    current.take_rows(&self.journal, &self.session, &mut self.http, stop)?;
                self.cursor = 0;
                self.page_consumed = true;
            }
            if self.cursor < self.rows.len() {
                let sources = row::selectors(table)?;
                let mut take = 0;
                let mut estimate = 1024 + table.contract.columns.len() * 512;
                for row in self.rows[self.cursor..].iter().take(1024) {
                    let mut row_size = table.contract.columns.len() * 32;
                    for source in &sources {
                        let value = row::field(row, source)?;
                        row_size = row_size
                            .checked_add(row::source_text_bytes(value).max(32))
                            .ok_or_else(|| crate::integrity("projected row size overflow"))?;
                    }
                    if estimate
                        .checked_add(row_size)
                        .is_none_or(|size| size > self.batch_limit)
                    {
                        break;
                    }
                    estimate += row_size;
                    take += 1;
                }
                if take == 0 {
                    return Err(crate::integrity(
                        "projected source row exceeds batch budget",
                    ));
                }
                loop {
                    let scratch = row::scratch_upper_bound(
                        &table.contract,
                        &sources,
                        &self.rows[self.cursor..self.cursor + take],
                    )?;
                    if scratch > self.scratch_limit {
                        if take > 1 {
                            take = take.div_ceil(2);
                            continue;
                        }
                        return Err(crate::integrity(
                            "projected row exceeds negotiated scratch budget",
                        ));
                    }
                    let batch = row::decode_rows(
                        &table.contract,
                        &sources,
                        &self.rows[self.cursor..self.cursor + take],
                    )?;
                    match row::ipc(&batch, self.batch_limit) {
                        Ok(payload) => {
                            self.cursor += take;
                            return Ok(Some(ExtractionEvent::Batch {
                                table: table.name.clone(),
                                payload,
                                rows: count(take as u64)?,
                            }));
                        }
                        Err(_) if take > 1 => {
                            take = take.div_ceil(2);
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
            if self.page_consumed {
                self.rows.clear();
                self.cursor = 0;
                self.page_consumed = false;
                if current.finish_rows(&self.journal, &self.session, &mut self.http)? {
                    let record = self
                        .journal
                        .required(self.current_identity.as_ref().unwrap())?;
                    let grv_adapter_api::CaptureWindow { start, end } =
                        record.capture_window().expect("completed acquisition");
                    let rows = match record.state {
                        crate::journal::AcquisitionState::Complete { rows, .. } => rows,
                        _ => unreachable!(),
                    };
                    let event = ExtractionEvent::TableComplete {
                        table: table.name.clone(),
                        row_count: count(rows)?,
                        source_identity: record.source_identity()?,
                        capture: grv_adapter_api::CaptureWindow { start, end },
                    };
                    self.records.push(record);
                    self.current = None;
                    self.current_identity = None;
                    self.table_index += 1;
                    return Ok(Some(event));
                }
                self.rows =
                    current.take_rows(&self.journal, &self.session, &mut self.http, stop)?;
                self.cursor = 0;
                self.page_consumed = true;
            }
        }
    }
}
fn unique(sources: &[String]) -> Vec<String> {
    let mut fields = Vec::new();
    for source in sources {
        if !fields
            .iter()
            .any(|existing: &String| existing.eq_ignore_ascii_case(source))
        {
            fields.push(source.clone());
        }
    }
    fields
}
fn identity(
    request: &ExtractRequest,
    table: &str,
    object: &str,
    query: &str,
    api_version: &str,
    options: &Options,
) -> crate::Result<AcquisitionIdentity> {
    let mut fixed = serde_json::to_value(request)
        .map_err(|_| crate::integrity("invalid extraction identity"))?;
    let object_value = fixed
        .as_object_mut()
        .expect("typed extraction request is an object");
    object_value.remove("stream_id");
    object_value.remove("resume");
    Ok(AcquisitionIdentity {
        attempt_id: uuid::Uuid::parse_str(request.attempt_id.as_str())
            .expect("validated attempt UUID"),
        table: table.into(),
        object: object.into(),
        request_sha256: grv_types::sha256(
            &grv_types::canonical_json(&fixed)
                .map_err(|_| crate::integrity("extraction identity cannot be canonicalized"))?,
        )
        .as_str()
        .into(),
        connection_identity: request.connection_identity.clone(),
        query_sha256: grv_types::sha256(query.as_bytes()).as_str().into(),
        api_version: api_version.into(),
        transport: if options.transport == Transport::Bulk {
            Transport::Bulk
        } else {
            Transport::Rest
        },
        all_rows: options.all_rows,
    })
}
impl<H: SourceHttp + Send> Extraction for RestProducer<H> {
    fn next(&mut self, stop: &StopToken) -> grv_adapter_sdk::Result<Option<ExtractionEvent>> {
        self.next_inner(stop).map_err(public)
    }
}

enum Acquisition {
    Rest(RestAcquisition),
    Bulk(crate::bulk::BulkAcquisition),
}
impl Acquisition {
    fn take_rows(
        &mut self,
        journal: &Journal,
        session: &AuthenticatedSession,
        http: &mut impl SourceHttp,
        stop: &StopToken,
    ) -> crate::Result<Vec<Value>> {
        match self {
            Self::Rest(acquisition) => acquisition.take_rows(journal),
            Self::Bulk(acquisition) => acquisition.take_rows(journal, session, http, stop),
        }
    }
    fn finish_rows(
        &mut self,
        journal: &Journal,
        session: &AuthenticatedSession,
        http: &mut impl SourceHttp,
    ) -> crate::Result<bool> {
        match self {
            Self::Rest(acquisition) => acquisition.finish_rows(journal, session, http),
            Self::Bulk(acquisition) => acquisition.finish_rows(journal),
        }
    }
}
