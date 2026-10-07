//! The same state machine validates both ends of the channel.
use crate::{DOCUMENT_LIMIT, ProtocolError, Result, fail, number_string, value};
use base64::{Engine, engine::general_purpose::STANDARD};
use grv_adapter_api::Frame;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Parent,
    Adapter,
}
impl Role {
    fn other(self) -> Self {
        if self == Self::Parent {
            Self::Adapter
        } else {
            Self::Parent
        }
    }
    fn index(self) -> usize {
        if self == Self::Parent { 0 } else { 1 }
    }
}
#[derive(Default)]
struct Documents {
    upload: Option<Upload>,
    complete: Option<Document>,
}
struct Upload {
    id: String,
    size: usize,
    digest: String,
    next: u64,
    bytes: Vec<u8>,
}
struct Document {
    id: String,
    value: Value,
    acked: bool,
}
struct Operation {
    req: u64,
    kind: String,
    request: Value,
}
#[derive(Default)]
struct Stream {
    identity: Value,
    checkpoint_ids: BTreeSet<String>,
    expected: BTreeSet<String>,
    covered: BTreeSet<String>,
    completed: BTreeSet<String>,
    checkpoints: BTreeMap<String, Vec<String>>,
    snapshots: BTreeMap<String, Value>,
    slots: BTreeMap<u64, (String, u64)>,
    sequences: BTreeMap<String, u64>,
    counts: BTreeMap<String, u64>,
    current: Option<String>,
    started: bool,
    required_counts: BTreeMap<String, u64>,
}

pub struct State {
    role: Role,
    handshake: u8,
    resources: Value,
    last: u64,
    active: Option<Operation>,
    retired: Option<u64>,
    cancelled: Option<u64>,
    cancel_ack: Option<String>,
    poisoned: bool,
    closed: bool,
    documents: [Documents; 2],
    document_ids: BTreeSet<String>,
    stream: Stream,
    build_session: Option<grv_adapter_api::BuildSession>,
    build_counts: Vec<grv_adapter_api::TableCount>,
    accepted_completion: Option<grv_adapter_api::Digest>,
}
impl State {
    pub fn new(role: Role) -> Self {
        Self {
            role,
            handshake: 0,
            resources: Value::Null,
            last: 0,
            active: None,
            retired: None,
            cancelled: None,
            cancel_ack: None,
            poisoned: false,
            closed: false,
            documents: Default::default(),
            document_ids: Default::default(),
            stream: Default::default(),
            build_session: None,
            build_counts: vec![],
            accepted_completion: None,
        }
    }
    pub fn acknowledgements_frozen(&self) -> bool {
        self.cancelled.is_some() || self.poisoned
    }
    pub fn is_closed(&self) -> bool {
        self.closed
    }
    pub fn document(&self, sender: Role, id: &str) -> Result<Value> {
        let d = self.documents[sender.index()]
            .complete
            .as_ref()
            .ok_or_else(|| ProtocolError::Invalid("unknown document".into()))?;
        if d.id != id || !d.acked {
            return fail("unacknowledged document reference");
        }
        Ok(d.value.clone())
    }
    pub fn resolve(&self, sender: Role, doc: &Value) -> Result<Value> {
        if let Some(v) = doc.get("inline") {
            Ok(v.clone())
        } else if let Some(id) = doc.get("document_id").and_then(Value::as_str) {
            self.document(sender, id)
        } else {
            fail("invalid document reference")
        }
    }
    pub fn resolved_frame(&self, frame: &Frame, outbound: bool) -> Result<Frame> {
        fn expand(state: &State, sender: Role, v: &mut Value) -> Result<()> {
            if let Some(obj) = v.as_object_mut() {
                if obj.len() == 1 && obj.contains_key("document_id") {
                    let resolved = state.document(sender, obj["document_id"].as_str().unwrap())?;
                    *v = serde_json::json!({"inline":resolved});
                } else {
                    for child in obj.values_mut() {
                        expand(state, sender, child)?;
                    }
                }
            } else if let Some(a) = v.as_array_mut() {
                for child in a {
                    expand(state, sender, child)?;
                }
            }
            Ok(())
        }
        let mut v = value(frame);
        expand(
            self,
            if outbound {
                self.role
            } else {
                self.role.other()
            },
            &mut v,
        )?;
        Ok(serde_json::from_value(v)?)
    }
    pub fn observe(&mut self, frame: &Frame, outbound: bool) -> Result<()> {
        if matches!(frame, Frame::DocumentBegin { size, .. } if size.get() > DOCUMENT_LIMIT as u64)
        {
            return Err(ProtocolError::DocumentTooLarge);
        }
        frame
            .validate()
            .map_err(|e| ProtocolError::Invalid(e.to_string()))?;
        if self.closed {
            return fail("closed channel");
        }
        let sender = if outbound {
            self.role
        } else {
            self.role.other()
        };
        let mut v = value(frame);
        let msg = v["msg"].as_str().unwrap().to_owned();
        if msg == "error" {
            if let Some(req) = v["req"].as_u64() {
                if self.active.as_ref().map(|o| o.req) != Some(req) {
                    return fail("uncorrelated error");
                }
                self.retired = Some(req);
                self.active = None;
            }
            self.poisoned = true;
            self.documents = Default::default();
            self.stream.slots.clear();
            return Ok(());
        }
        if self.handshake < 3 {
            let (expected, role) = [
                ("hello", Role::Parent),
                ("identified", Role::Adapter),
                ("ready", Role::Parent),
            ][self.handshake as usize];
            if msg != expected || sender != role {
                return fail("illegal bootstrap frame");
            }
            if let Frame::Hello {
                resources,
                interface_versions,
                ..
            } = frame
            {
                resources
                    .validate()
                    .map_err(|e| ProtocolError::Invalid(e.to_string()))?;
                if interface_versions.is_empty()
                    || interface_versions
                        .iter()
                        .enumerate()
                        .any(|(i, x)| interface_versions[..i].contains(x))
                {
                    return fail("invalid interface offer");
                }
                self.resources = v["resources"].clone();
            }
            if let Frame::Identified {
                capabilities,
                registry,
                package_version,
                interface_versions,
                ..
            } = frame
            {
                capabilities
                    .validate()
                    .map_err(|e| ProtocolError::Invalid(e.to_string()))?;
                registry
                    .validate()
                    .map_err(|e| ProtocolError::Invalid(e.to_string()))?;
                if package_version.is_empty()
                    || interface_versions.is_empty()
                    || interface_versions
                        .iter()
                        .enumerate()
                        .any(|(i, x)| interface_versions[..i].contains(x))
                {
                    return fail("invalid adapter identity");
                }
            }
            if msg == "ready" && self.resources != v["resources"] {
                return fail("ready changed offered resources");
            }
            self.handshake += 1;
            return Ok(());
        }
        if msg.starts_with("document_") {
            return self.document_control(sender, &msg, &v);
        }
        if msg == "cancel" {
            let req = req(&v)?;
            if sender != Role::Parent
                || (self.active.as_ref().map(|o| o.req) != Some(req) && self.retired != Some(req))
            {
                return fail("uncorrelated cancel");
            }
            if self.cancelled.is_some_and(|r| r != req) {
                return fail("changed cancellation");
            }
            self.cancelled = Some(req);
            return Ok(());
        }
        if msg == "cancel_ack" {
            let r = req(&v)?;
            let s = v["state"].as_str().unwrap();
            if sender != Role::Adapter || self.cancelled != Some(r) {
                return fail("uncorrelated cancel ack");
            }
            if self.cancel_ack.as_deref() == Some(s) {
                return Ok(());
            }
            if self.cancel_ack.as_deref().is_some_and(|old| old != s) {
                return fail("changed cancel ack");
            }
            if s == "completed" && self.retired != Some(r) {
                return fail("completed ack before result");
            }
            if s == "stopped" && self.retired == Some(r) && self.cancel_ack.is_none() {
                return fail("stopped ack after completion");
            }
            self.cancel_ack = Some(s.into());
            self.active = None;
            self.retired = Some(r);
            self.stream.slots.clear();
            self.documents = Default::default();
            return Ok(());
        }
        let r = req(&v)?;
        if is_request(&msg) {
            if sender != Role::Parent
                || self.active.is_some()
                || !self.stream.slots.is_empty()
                || r <= self.last
            {
                return fail("illegal request or correlation");
            }
            if msg != "close" && (self.cancelled.is_some() || self.poisoned) {
                return fail("ordinary work after cancellation/error");
            }
            if self.cancelled.is_some() && self.cancel_ack.is_none() {
                return fail("close before cancel ack");
            }
            self.expand_documents(sender, &mut v)?;
            if msg == "extract" {
                self.stream = Stream::default();
                let payload = &v["payload"]["inline"];
                self.stream.identity = serde_json::json!({"attempt_id":payload["attempt_id"],"adapter_identity":payload["adapter_identity"],"connection_identity":payload["connection_identity"]});
                for t in v["payload"]["inline"]["tables"]
                    .as_array()
                    .ok_or_else(|| ProtocolError::Invalid("missing tables".into()))?
                {
                    let name = t["name"]
                        .as_str()
                        .ok_or_else(|| ProtocolError::Invalid("missing table".into()))?;
                    if !self.stream.expected.insert(name.into()) {
                        return fail("duplicate table");
                    }
                }
            }
            serde_json::from_value::<Frame>(v.clone())?
                .validate()
                .map_err(|e| ProtocolError::Invalid(e.to_string()))?;
            if msg == "export_build" {
                let session = self.build_session.as_ref().ok_or_else(|| {
                    ProtocolError::Invalid("export has no prepared or opened session".into())
                })?;
                if v["session_id"].as_str() != Some(session.session_id.as_str())
                    || v["completion_sha256"].as_str()
                        != self.accepted_completion.as_ref().map(|d| d.as_str())
                {
                    return fail("export requires matching accepted completion");
                }
                self.stream = Stream::default();
                self.stream.started = true;
                self.stream.expected.extend(
                    session
                        .selected_outputs
                        .iter()
                        .map(|n| n.as_str().to_owned()),
                );
                self.stream.covered = self.stream.expected.clone();
                self.stream.required_counts.extend(
                    self.build_counts
                        .iter()
                        .map(|c| (c.table.as_str().to_owned(), c.rows.get())),
                );
            }
            self.last = r;
            self.active = Some(Operation {
                req: r,
                kind: msg,
                request: v,
            });
            return Ok(());
        }
        let op = self
            .active
            .as_ref()
            .ok_or_else(|| ProtocolError::Invalid("no active request".into()))?;
        if op.req != r {
            return fail("uncorrelated response");
        }
        let kind = op.kind.clone();
        let request = op.request.clone();
        if ["batch_ack", "checkpoint_ack"].contains(&msg.as_str()) {
            if sender != Role::Parent || self.acknowledgements_frozen() {
                return fail("illegal acknowledgement");
            }
        } else if sender != Role::Adapter {
            return fail("wrong-role result");
        }
        if self.cancel_ack.is_some() && kind != "close" {
            return fail("event after cancellation barrier");
        }
        self.expand_documents(sender, &mut v)?;
        serde_json::from_value::<Frame>(v.clone())?
            .validate()
            .map_err(|e| ProtocolError::Invalid(e.to_string()))?;
        if (kind == "extract" || kind == "export_build") && self.stream_event(&msg, &v)? {
            return Ok(());
        }
        let terminal = match kind.as_str() {
            "validate_binding" => "validate_result",
            "locate_connection" => "connection_located",
            "bind_connection" => "bind_result",
            "authenticate" => "authenticate_result",
            "after_publish" => "after_publish_result",
            "inspect_connection" => "inspect_result",
            "prepare_command" => "command_prepared",
            "command" => "command_result",
            "extract" => "source_complete",
            "resolve_pull" => "resolve_result",
            "prepare_pull" => "prepare_result",
            "apply_pull" => "apply_result",
            "discover_build" => "build_discovered",
            "prepare_build" => "build_prepared",
            "execute_build" => "build_finished",
            "accept_build_completion" => "completion_accepted",
            "export_build" => "export_complete",
            "open_build" => "build_opened",
            "inspect_build" => "build_inspected",
            "abort_build" => "build_aborted",
            "record_build_outcome" => "build_outcome_recorded",
            "cleanup_build" => "build_cleaned",
            "close" => "close_result",
            _ => return fail("unsupported operation state"),
        };
        if msg != terminal {
            return fail("wrong result kind or unexpected event");
        }
        if (kind == "extract" || kind == "export_build")
            && (!self.stream.slots.is_empty()
                || self.stream.completed != self.stream.expected
                || !self.stream.started)
        {
            return fail("incomplete source");
        }
        match msg.as_str() {
            "build_prepared" => {
                self.build_session = Some(serde_json::from_value(v["session"]["inline"].clone())?);
                self.accepted_completion = None;
                self.build_counts.clear();
            }
            "build_opened" => {
                let record: grv_adapter_api::BuildRecord =
                    serde_json::from_value(v["record"]["inline"].clone())?;
                if serde_json::to_value(&record.session.identity)? != request["payload"]["inline"] {
                    return fail("reopened session identity differs");
                }
                self.accepted_completion = record.completion_sha256;
                self.build_counts = record.row_counts;
                self.build_session = Some(record.session);
            }
            "build_finished" => {
                let result: grv_adapter_api::BuildExecutionResult =
                    serde_json::from_value(v["result"]["inline"].clone())?;
                let session: grv_adapter_api::BuildSession =
                    serde_json::from_value(request["payload"]["inline"]["session"].clone())?;
                result
                    .validate_for(&session)
                    .map_err(|e| ProtocolError::Invalid(e.to_string()))?;
                self.build_counts = result.row_counts;
            }
            "completion_accepted" => {
                let completion: grv_adapter_api::BuildCompletion =
                    serde_json::from_value(request["completion"]["inline"].clone())?;
                let session = self
                    .build_session
                    .as_ref()
                    .ok_or_else(|| ProtocolError::Invalid("acceptance has no session".into()))?;
                completion
                    .validate_for(session)
                    .map_err(|e| ProtocolError::Invalid(e.to_string()))?;
                let digest = completion
                    .digest()
                    .map_err(|e| ProtocolError::Invalid(e.to_string()))?;
                if v["session_id"] != request["session_id"]
                    || v["session_id"].as_str() != Some(session.session_id.as_str())
                    || v["completion_sha256"].as_str() != Some(digest.as_str())
                {
                    return fail("acceptance changed session or completion digest");
                }
                if self
                    .accepted_completion
                    .as_ref()
                    .is_some_and(|old| old != &digest)
                {
                    return fail("changed accepted completion");
                }
                self.accepted_completion = Some(digest);
            }
            "export_complete" => {
                if v["session_id"] != request["session_id"]
                    || v["completion_sha256"] != request["completion_sha256"]
                {
                    return fail("export changed accepted identity");
                }
            }
            "build_aborted" | "build_outcome_recorded" | "build_cleaned"
                if v["session_id"] != request["session_id"] =>
            {
                return fail("build acknowledgement changed session");
            }
            _ => {}
        }
        self.active = None;
        self.retired = Some(r);
        if kind == "close" {
            self.closed = true;
        }
        Ok(())
    }
    fn document_control(&mut self, sender: Role, msg: &str, v: &Value) -> Result<()> {
        if self.cancel_ack.is_some() || self.poisoned {
            return fail("documents after barrier");
        }
        let id = v["document_id"].as_str().unwrap().to_owned();
        if msg == "document_ack" {
            let d = self.documents[sender.other().index()]
                .complete
                .as_mut()
                .ok_or_else(|| ProtocolError::Invalid("unknown document ack".into()))?;
            if d.id != id || d.acked {
                return fail("invalid document ack");
            }
            d.acked = true;
            return Ok(());
        }
        let docs = &mut self.documents[sender.index()];
        match msg {
            "document_begin" => {
                let size = number_string(v, "size")? as usize;
                if size > DOCUMENT_LIMIT {
                    return Err(ProtocolError::DocumentTooLarge);
                }
                if size == 0 || docs.upload.is_some() || !self.document_ids.insert(id.clone()) {
                    return fail("invalid document begin");
                }
                docs.upload = Some(Upload {
                    id,
                    size,
                    digest: v["sha256"].as_str().unwrap().into(),
                    next: 0,
                    bytes: Vec::new(),
                });
            }
            "document_chunk" => {
                let u = docs
                    .upload
                    .as_mut()
                    .ok_or_else(|| ProtocolError::Invalid("chunk without begin".into()))?;
                let s = v["data"].as_str().unwrap();
                let b = STANDARD
                    .decode(s)
                    .map_err(|_| ProtocolError::Invalid("invalid base64".into()))?;
                if u.id != id
                    || v["index"].as_u64() != Some(u.next)
                    || b.is_empty()
                    || b.len() > 512 * 1024
                    || STANDARD.encode(&b) != s
                    || u.bytes.len() + b.len() > u.size
                {
                    return fail("invalid document chunk");
                }
                u.next += 1;
                u.bytes.extend(b);
            }
            "document_end" => {
                let u = docs
                    .upload
                    .take()
                    .ok_or_else(|| ProtocolError::Invalid("end without begin".into()))?;
                if u.id != id
                    || docs.complete.is_some()
                    || u.bytes.len() != u.size
                    || format!("{:x}", Sha256::digest(&u.bytes)) != u.digest
                {
                    return fail("document length/hash mismatch");
                }
                let parsed = crate::json::parse(&u.bytes)?;
                if grv_types::canonical_json(&parsed)
                    .map_err(|e| ProtocolError::Invalid(e.to_string()))?
                    != u.bytes
                {
                    return fail("noncanonical document");
                }
                docs.complete = Some(Document {
                    id,
                    value: parsed,
                    acked: false,
                });
            }
            _ => return fail("unknown document control"),
        }
        Ok(())
    }
    fn expand_documents(&mut self, sender: Role, v: &mut Value) -> Result<()> {
        if let Some(obj) = v.as_object_mut() {
            if obj.len() == 1 && obj.contains_key("document_id") {
                let id = obj["document_id"].as_str().unwrap();
                let resolved = self.document(sender, id)?;
                self.documents[sender.index()].complete = None;
                *v = serde_json::json!({"inline":resolved});
            } else {
                for child in obj.values_mut() {
                    self.expand_documents(sender, child)?;
                }
            }
        } else if let Some(a) = v.as_array_mut() {
            for child in a {
                self.expand_documents(sender, child)?;
            }
        }
        Ok(())
    }
    fn stream_event(&mut self, msg: &str, v: &Value) -> Result<bool> {
        let is_export = self
            .active
            .as_ref()
            .is_some_and(|o| o.kind == "export_build");
        let s = &mut self.stream;
        if is_export
            && [
                "extract_started",
                "checkpoint",
                "checkpoint_ack",
                "table_complete",
            ]
            .contains(&msg)
        {
            return fail("extraction events forbidden during build export");
        }
        if !is_export && msg == "build_table_complete" {
            return fail("build completion forbidden during extraction");
        }
        if msg == "extract_started" {
            if s.started {
                return fail("duplicate start");
            }
            s.started = true;
            return Ok(true);
        }
        if msg == "checkpoint" {
            if !s.started {
                return fail("checkpoint before start");
            }
            let id = v["checkpoint_id"].as_str().unwrap();
            let mut tables = Vec::new();
            let payload = &v["payload"]["inline"];
            if serde_json::json!({"attempt_id":payload["attempt_id"],"adapter_identity":payload["adapter_identity"],"connection_identity":payload["connection_identity"]})
                != s.identity
                || !s.checkpoint_ids.insert(id.into())
            {
                return fail("checkpoint identity mismatch or ID reuse");
            }
            for t in v["payload"]["inline"]["tables"]
                .as_array()
                .ok_or_else(|| ProtocolError::Invalid("checkpoint tables missing".into()))?
            {
                let name = t["table"].as_str().unwrap();
                if !s.expected.contains(name) || tables.iter().any(|n| n == name) {
                    return fail("unexpected checkpoint table");
                }
                if let Some(old) = s.snapshots.get(name) {
                    if old != t {
                        return fail("changed acquisition evidence");
                    }
                } else {
                    s.snapshots.insert(name.into(), t.clone());
                }
                tables.push(name.into());
            }
            if s.checkpoints.insert(id.into(), tables).is_some() {
                return fail("repeated checkpoint ID");
            }
            return Ok(true);
        }
        if msg == "checkpoint_ack" {
            let id = v["checkpoint_id"].as_str().unwrap();
            let tables = s
                .checkpoints
                .remove(id)
                .ok_or_else(|| ProtocolError::Invalid("unknown checkpoint ack".into()))?;
            s.covered.extend(tables);
            return Ok(true);
        }
        if msg == "batch" {
            let table = v["table"].as_str().unwrap();
            let seq = v["seq"].as_u64().unwrap();
            let slot = v["slot"].as_u64().unwrap();
            let rows = number_string(v, "rows")?;
            let max = number_string(&self.resources, "max_batch_bytes")?;
            if !s.covered.contains(table)
                || s.completed.contains(table)
                || slot >= self.resources["slots"].as_u64().unwrap()
                || s.slots.contains_key(&slot)
                || seq != *s.sequences.get(table).unwrap_or(&0)
                || rows == 0
                || rows > i32::MAX as u64
                || number_string(v, "size")? > max
            {
                return fail("invalid batch credit/order");
            }
            if s.current.as_deref().is_some_and(|c| c != table) {
                return fail("interleaved tables");
            }
            s.current = Some(table.into());
            s.sequences.insert(
                table.into(),
                seq.checked_add(1)
                    .ok_or_else(|| ProtocolError::Invalid("sequence exhausted".into()))?,
            );
            let count = s.counts.entry(table.into()).or_default();
            *count = count
                .checked_add(rows)
                .filter(|c| *c <= i64::MAX as u64)
                .ok_or_else(|| ProtocolError::Invalid("count overflow".into()))?;
            s.slots.insert(slot, (table.into(), seq));
            return Ok(true);
        }
        if msg == "batch_ack" {
            let slot = v["slot"].as_u64().unwrap();
            let tuple = (
                v["table"].as_str().unwrap().into(),
                v["seq"].as_u64().unwrap(),
            );
            if s.slots.get(&slot) != Some(&tuple) {
                return fail("wrong credit tuple");
            }
            s.slots.remove(&slot);
            return Ok(true);
        }
        if msg == "table_complete" || msg == "build_table_complete" {
            let table = v["table"].as_str().unwrap();
            if !s.covered.contains(table)
                || !s.completed.insert(table.into())
                || s.slots.values().any(|(t, _)| t == table)
                || number_string(v, "row_count")? != *s.counts.get(table).unwrap_or(&0)
                || s.current.as_deref().is_some_and(|c| c != table)
                || s.required_counts
                    .get(table)
                    .is_some_and(|c| number_string(v, "row_count").ok().as_ref() != Some(c))
            {
                return fail("invalid table completion");
            }
            s.current = None;
            return Ok(true);
        }
        Ok(false)
    }
}
fn req(v: &Value) -> Result<u64> {
    v["req"]
        .as_u64()
        .ok_or_else(|| ProtocolError::Invalid("missing request ID".into()))
}
fn is_request(msg: &str) -> bool {
    [
        "validate_binding",
        "locate_connection",
        "bind_connection",
        "authenticate",
        "after_publish",
        "inspect_connection",
        "prepare_command",
        "command",
        "extract",
        "resolve_pull",
        "prepare_pull",
        "apply_pull",
        "discover_build",
        "prepare_build",
        "execute_build",
        "accept_build_completion",
        "export_build",
        "open_build",
        "inspect_build",
        "abort_build",
        "record_build_outcome",
        "cleanup_build",
        "close",
    ]
    .contains(&msg)
}
