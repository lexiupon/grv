//! Cancel a recorded acquisition without starting it or changing its token.
use super::*;

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum LeaseAbortProgress {
    Prepared,
    Release {
        proposed: Latest,
        expected: Validator,
    },
    Done,
}
fn save(
    progress: &mut LeaseAbortProgress,
    next: LeaseAbortProgress,
    persist: &mut impl FnMut(&LeaseAbortProgress) -> Result<()>,
) -> Result<()> {
    persist(&next)?;
    *progress = next;
    Ok(())
}
impl<B: Backend> Publisher<'_, B> {
    /// Never acquire on abort. Release only the recorded lease if it remains
    /// owned; another holder's lease is preserved. No publication CAS occurs.
    pub fn abandon_lease_authorized(
        &self,
        intent: &LeaseIntent,
        evidence: &LeaseProgress,
        progress: &mut LeaseAbortProgress,
        mut persist: impl FnMut(&LeaseAbortProgress) -> Result<()>,
    ) -> Result<()> {
        let key = revision::latest_key(&intent.dataset);
        loop {
            match progress.clone() {
                LeaseAbortProgress::Prepared => {
                    if matches!(evidence, LeaseProgress::Prepared) {
                        save(progress, LeaseAbortProgress::Done, &mut persist)?;
                        continue;
                    }
                    let (mut actual, validator): (Latest, _) = self.read(&key)?;
                    let owned = actual.lease.as_ref().is_some_and(|lease| {
                        lease.token == intent.token && lease.holder == intent.holder
                    });
                    match evidence {
                        LeaseProgress::Compare { latest, expected } => {
                            if actual != *latest && validator == *expected || !owned {
                                save(progress, LeaseAbortProgress::Done, &mut persist)?;
                                continue;
                            }
                            if actual != *latest {
                                return Err(error(
                                    ErrorCode::OutcomeUnknown,
                                    "publication lease acquisition changed without exact journal evidence",
                                ));
                            }
                        }
                        LeaseProgress::Owned { owner } => {
                            if owner.intent.dataset != intent.dataset
                                || owner.intent.token != intent.token
                                || owner.intent.holder != intent.holder
                            {
                                return Err(error(
                                    ErrorCode::IntegrityFailure,
                                    "recorded dataset lease differs from abort intent",
                                ));
                            }
                            if !owned {
                                save(progress, LeaseAbortProgress::Done, &mut persist)?;
                                continue;
                            }
                        }
                        LeaseProgress::Prepared => unreachable!(),
                    }
                    actual.lease = None;
                    actual.mutation_id = Uuid::v4();
                    save(
                        progress,
                        LeaseAbortProgress::Release {
                            proposed: actual,
                            expected: validator,
                        },
                        &mut persist,
                    )?;
                }
                LeaseAbortProgress::Release { proposed, expected } => {
                    let (actual, validator): (Latest, _) = self.read(&key)?;
                    if actual == proposed
                        || !actual.lease.as_ref().is_some_and(|lease| {
                            lease.token == intent.token && lease.holder == intent.holder
                        })
                    {
                        save(progress, LeaseAbortProgress::Done, &mut persist)?;
                        continue;
                    }
                    if validator != expected {
                        return Err(error(
                            ErrorCode::OutcomeUnknown,
                            "dataset lease release precondition changed",
                        ));
                    }
                    self.put(&intent.dataset, &expected, &proposed)?;
                    save(progress, LeaseAbortProgress::Done, &mut persist)?;
                }
                LeaseAbortProgress::Done => return Ok(()),
            }
        }
    }
}
