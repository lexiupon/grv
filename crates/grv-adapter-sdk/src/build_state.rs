//! Channel-local authorization complements durable adapter session evidence.
//! Losing this state requires reopening the exact session, never rediscovery.
use super::*;

#[derive(Default)]
pub(crate) struct BuildLifecycle {
    discovery_request: Option<DiscoverBuildRequest>,
    discovery: Option<BuildDiscovery>,
    session: Option<BuildSession>,
    invocation_started: bool,
    candidate: Option<BuildCompletion>,
    accepted: Option<Digest>,
    aborted: bool,
    outcome: Option<BuildOutcome>,
}
impl BuildLifecycle {
    fn session(&self, id: &Uuid) -> Result<&BuildSession> {
        self.session
            .as_ref()
            .filter(|s| &s.session_id == id)
            .ok_or_else(|| protocol("unknown build session"))
    }
    pub(crate) fn authorize(&mut self, frame: &Frame) -> Result<()> {
        match frame {
            Frame::DiscoverBuild {
                payload: Doc::Inline(p),
                ..
            } => {
                if self.discovery_request.is_some() || self.session.is_some() {
                    return Err(protocol("build discovery cannot be repeated"));
                }
                self.discovery_request = Some(p.inline.clone());
            }
            Frame::PrepareBuild {
                payload: Doc::Inline(p),
                ..
            } => {
                let request = self
                    .discovery_request
                    .as_ref()
                    .ok_or_else(|| protocol("preparation has no retained discovery"))?;
                if self.session.is_some()
                    || self.discovery.as_ref() != Some(&p.inline.discovery)
                    || request.self_input != p.inline.self_input
                {
                    return Err(protocol(
                        "preparation changed retained discovery or self-input",
                    ));
                }
            }
            Frame::ExecuteBuild {
                payload: Doc::Inline(p),
                ..
            } => {
                if self.session.as_ref() != Some(&p.inline.session)
                    || self.invocation_started
                    || self.aborted
                    || self.outcome.is_some()
                {
                    return Err(protocol(
                        "unknown, changed, or already invoked build session",
                    ));
                }
                self.invocation_started = true;
            }
            Frame::AcceptBuildCompletion {
                session_id,
                completion: Doc::Inline(c),
                ..
            } => {
                let session = self.session(session_id)?;
                c.inline
                    .validate_for(session)
                    .map_err(|e| protocol(e.to_string()))?;
                if self.aborted
                    || self.outcome.is_some()
                    || (session.execution == BuildExecution::Managed
                        && self.candidate.as_ref() != Some(&c.inline))
                    || self
                        .accepted
                        .as_ref()
                        .is_some_and(|d| c.inline.digest().as_ref().ok() != Some(d))
                {
                    return Err(protocol(
                        "completion conflicts with durable candidate, acceptance, or terminal state",
                    ));
                }
            }
            Frame::ExportBuild {
                session_id,
                completion_sha256,
                ..
            } => {
                self.session(session_id)?;
                if self.accepted.as_ref() != Some(completion_sha256)
                    || self.aborted
                    || self.outcome.is_some()
                {
                    return Err(protocol(
                        "export requires immutable accepted completion on a live session",
                    ));
                }
            }
            Frame::OpenBuild {
                payload: Doc::Inline(identity),
                ..
            }
            | Frame::InspectBuild {
                payload: Doc::Inline(identity),
                ..
            } => {
                if self.discovery_request.is_some()
                    || self
                        .session
                        .as_ref()
                        .is_some_and(|s| s.identity != identity.inline)
                {
                    return Err(protocol(
                        "build reopen changed fixed identity or active discovery",
                    ));
                }
            }
            Frame::AbortBuild { session_id, .. } => {
                self.session(session_id)?;
                if self
                    .outcome
                    .as_ref()
                    .is_some_and(|o| o.kind != OutcomeKind::Aborted)
                {
                    return Err(protocol("abort cannot erase known build publication"));
                }
            }
            Frame::RecordBuildOutcome {
                session_id,
                outcome,
                ..
            } => {
                self.session(session_id)?;
                if self.outcome.as_ref().is_some_and(|o| o != outcome)
                    || (self.aborted && outcome.kind != OutcomeKind::Aborted)
                {
                    return Err(protocol("changed build outcome"));
                }
            }
            Frame::CleanupBuild { session_id, .. } => {
                self.session(session_id)?;
                if self.outcome.is_none() {
                    return Err(protocol("cleanup requires known terminal outcome"));
                }
            }
            _ => {}
        }
        Ok(())
    }
    pub(crate) fn observe_result(&mut self, request: &Frame, result: &Frame) -> Result<()> {
        match (request, result) {
            (
                Frame::DiscoverBuild {
                    payload: Doc::Inline(request),
                    ..
                },
                Frame::BuildDiscovered {
                    discovery: Doc::Inline(discovery),
                    ..
                },
            ) => {
                discovery
                    .inline
                    .validate_for(&request.inline)
                    .map_err(|e| protocol(e.to_string()))?;
                self.discovery = Some(discovery.inline.clone());
            }
            (
                Frame::PrepareBuild {
                    payload: Doc::Inline(preparation),
                    ..
                },
                Frame::BuildPrepared {
                    session: Doc::Inline(session),
                    ..
                },
            ) => {
                session
                    .inline
                    .validate_for(
                        self.discovery_request
                            .as_ref()
                            .ok_or_else(|| protocol("missing build discovery"))?,
                        &preparation.inline,
                    )
                    .map_err(|e| protocol(e.to_string()))?;
                self.session = Some(session.inline.clone());
                self.discovery = None;
                // The retained discovery request is no longer a live transaction.
                self.discovery_request = None;
            }
            (
                Frame::ExecuteBuild {
                    payload: Doc::Inline(request),
                    ..
                },
                Frame::BuildFinished {
                    result: Doc::Inline(result),
                    ..
                },
            ) => {
                result
                    .inline
                    .validate_for(&request.inline.session)
                    .map_err(|e| protocol(e.to_string()))?;
                self.candidate = result.inline.completion.clone();
            }
            (
                Frame::AcceptBuildCompletion {
                    session_id,
                    completion: Doc::Inline(completion),
                    ..
                },
                Frame::CompletionAccepted {
                    session_id: returned,
                    completion_sha256,
                    ..
                },
            ) => {
                if returned != session_id
                    || completion
                        .inline
                        .digest()
                        .map_err(|e| protocol(e.to_string()))?
                        != *completion_sha256
                {
                    return Err(protocol("acceptance changed session or completion digest"));
                }
                self.accepted = Some(completion_sha256.clone());
            }
            (
                Frame::ExportBuild {
                    session_id,
                    completion_sha256,
                    ..
                },
                Frame::ExportComplete {
                    session_id: returned,
                    completion_sha256: digest,
                    ..
                },
            ) => {
                if returned != session_id || digest != completion_sha256 {
                    return Err(protocol("export changed accepted completion identity"));
                }
            }
            (
                Frame::OpenBuild {
                    payload: Doc::Inline(identity),
                    ..
                },
                Frame::BuildOpened {
                    record: Doc::Inline(record),
                    ..
                },
            )
            | (
                Frame::InspectBuild {
                    payload: Doc::Inline(identity),
                    ..
                },
                Frame::BuildInspected {
                    record: Doc::Inline(record),
                    ..
                },
            ) => {
                record
                    .inline
                    .validate()
                    .map_err(|e| protocol(e.to_string()))?;
                if record.inline.session.identity != identity.inline
                    || self
                        .session
                        .as_ref()
                        .is_some_and(|s| s != &record.inline.session)
                {
                    return Err(protocol("reopened build changed fixed session"));
                }
                if self
                    .accepted
                    .as_ref()
                    .is_some_and(|digest| record.inline.completion_sha256.as_ref() != Some(digest))
                    || self.candidate.as_ref().is_some_and(|candidate| {
                        record.inline.candidate.as_ref() != Some(candidate)
                    })
                    || (self.invocation_started && record.inline.state == BuildState::Prepared)
                    || (self.aborted && record.inline.state != BuildState::Aborted)
                    || self
                        .outcome
                        .as_ref()
                        .is_some_and(|outcome| record.inline.outcome.as_ref() != Some(outcome))
                {
                    return Err(protocol(
                        "reopened build erased established lifecycle evidence",
                    ));
                }
                // Inspection alone grants no mutation or export authority.
                if matches!(request, Frame::OpenBuild { .. }) {
                    self.session = Some(record.inline.session.clone());
                    self.candidate = record.inline.candidate.clone();
                    self.accepted = record.inline.completion_sha256.clone();
                    self.invocation_started = record.inline.state != BuildState::Prepared;
                    self.aborted = record.inline.state == BuildState::Aborted;
                    self.outcome = record.inline.outcome.clone();
                }
            }
            (
                Frame::AbortBuild { session_id, .. },
                Frame::BuildAborted {
                    session_id: returned,
                    writers_stopped,
                    ..
                },
            ) => {
                if returned != session_id || !writers_stopped {
                    return Err(protocol("abort lacks matching stopped-writer evidence"));
                }
                self.aborted = true;
            }
            (
                Frame::RecordBuildOutcome {
                    session_id,
                    outcome,
                    ..
                },
                Frame::BuildOutcomeRecorded {
                    session_id: returned,
                    ..
                },
            ) => {
                if returned != session_id {
                    return Err(protocol("outcome acknowledgement changed session"));
                }
                self.outcome = Some(outcome.clone());
            }
            (
                Frame::CleanupBuild { session_id, .. },
                Frame::BuildCleaned {
                    session_id: returned,
                    ..
                },
            ) if returned != session_id => {
                return Err(protocol("cleanup acknowledgement changed session"));
            }
            _ => {}
        }
        Ok(())
    }
}
