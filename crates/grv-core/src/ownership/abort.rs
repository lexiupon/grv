//! Stopped-producer cleanup. No acquisition or allocation is initiated here.
use super::*;

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AbortClaim {
    intent: ReservationIntent,
    evidence: ReservationProgress,
    sources: Option<Vec<SourceReference>>,
}
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum AbortClaimProgress {
    Prepared,
    Release {
        claim: ClaimRecord,
        expected: Validator,
    },
    Released {
        claim: ClaimRecord,
    },
    Resolve {
        allocation: AllocationRecord,
        expected: Validator,
    },
    Done {
        allocation: Option<AllocationRecord>,
    },
}
fn save(
    progress: &mut AbortClaimProgress,
    next: AbortClaimProgress,
    persist: &mut impl FnMut(&AbortClaimProgress) -> Result<()>,
) -> Result<()> {
    persist(&next)?;
    *progress = next;
    Ok(())
}
fn unknown(message: &str) -> grv_types::PublicError {
    public_error(ErrorCode::OutcomeUnknown, message)
}
impl AbortClaim {
    pub fn from_reserving(
        owner: &RunOwner,
        intent: &ReservationIntent,
        evidence: &ReservationProgress,
    ) -> Result<Self> {
        if owner.dataset != intent.dataset
            || owner.control.run_id != intent.run_id
            || owner.control.owner_token != intent.owner_token
        {
            return Err(integrity(
                "abort reservation belongs to another owner epoch",
            ));
        }
        Ok(Self {
            intent: intent.clone(),
            evidence: evidence.clone(),
            sources: None,
        })
    }
    pub fn from_derived_reserving(
        owner: &RunOwner,
        intent: &DerivedReservationIntent,
        evidence: &ReservationProgress,
    ) -> Result<Self> {
        let mut claim = Self::from_reserving(owner, &intent.reservation, evidence)?;
        claim.sources = Some(intent.sources.clone());
        Ok(claim)
    }
    pub fn from_reservation(
        owner: &RunOwner,
        reservation: &Reservation,
        contract: &TableContract,
    ) -> Result<Self> {
        let allocation = &reservation.allocation;
        if owner.dataset != reservation.dataset || owner.control.run_id != allocation.run_id {
            return Err(integrity("abort reservation belongs to another run"));
        }
        let intent = ReservationIntent {
            dataset: reservation.dataset.clone(),
            run_id: allocation.run_id.clone(),
            owner_token: owner.control.owner_token.clone(),
            layout: reservation.layout.clone(),
            partition: allocation.partition.clone(),
            contract: contract.clone(),
            claim_token: allocation.claim_token.clone(),
        };
        Self::from_reserving(
            owner,
            &intent,
            &ReservationProgress::Complete {
                reservation: reservation.clone(),
            },
        )
    }
    pub fn from_derived(owner: &RunOwner, reservation: &DerivedReservation) -> Result<Self> {
        let mut claim = Self::from_reserving(
            owner,
            &reservation.intent.reservation,
            &ReservationProgress::Complete {
                reservation: reservation.reservation.clone(),
            },
        )?;
        claim.sources = Some(reservation.intent.sources.clone());
        Ok(claim)
    }
    pub fn token(&self) -> &ClaimToken {
        &self.intent.claim_token
    }
}
impl<B: Backend> Ownership<'_, B> {
    /// Caller must retain proven stopped-writer/workspace authority throughout
    /// cleanup. A recovered owner token supplies no authority for this epoch.
    fn stopped_control(&self, owner: &RunOwner) -> Result<RunControl> {
        let (current, _): (RunControl, _) =
            self.read(&control_key(&owner.dataset, &owner.control.run_id))?;
        if current.owner_token != owner.control.owner_token
            || current.run_id != owner.control.run_id
            || current.inputs != owner.control.inputs
            || current.base_revision != owner.control.base_revision
            || current.created_at != owner.control.created_at
            || current.metadata != owner.control.metadata
            || current.phase == RunPhase::Recovering
        {
            return Err(public_error(
                ErrorCode::OwnershipLost,
                "stopped build owner epoch was recovered or changed",
            ));
        }
        Ok(current)
    }
    pub fn require_stopped_epoch(&self, owner: &RunOwner) -> Result<()> {
        self.stopped_control(owner).map(|_| ())
    }
    /// Only a durable sealing intent after stopped cleanup authorizes this
    /// caller. No engine/source access or new owner epoch is inferred here.
    pub fn adopt_stopped_seal(&self, owner: &mut RunOwner) -> Result<Option<SealedRun>> {
        let current = self.stopped_control(owner)?;
        if current.phase != RunPhase::Sealed {
            return Ok(None);
        }
        let sealed = current.sealed_run().map_err(backend_error)?;
        self.create(&run_key(&owner.dataset, &current.run_id), &sealed)?;
        owner.control = current;
        Ok(Some(sealed))
    }
    fn abort_allocation(
        &self,
        claim: &AbortClaim,
    ) -> Result<Option<(AllocationRecord, Validator)>> {
        let path = claim
            .intent
            .claim_token
            .allocation_key(&claim.intent.dataset, &claim.intent.run_id)
            .map_err(backend_error)?;
        match self.read::<AllocationRecord>(&path) {
            Ok((record, validator)) => {
                if record.run_id != claim.intent.run_id
                    || record.table != claim.intent.layout.table
                    || record.partition != claim.intent.partition
                    || record.claim_token != claim.intent.claim_token
                {
                    return Err(integrity(
                        "abort allocation identity differs from fixed reservation",
                    ));
                }
                Ok(Some((record, validator)))
            }
            Err(e) if e.code == ErrorCode::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
    fn abort_release_proof(
        &self,
        claim: &AbortClaim,
        actual: &ClaimRecord,
    ) -> Result<Option<ClaimOutcome>> {
        if let Some((allocation, _)) = self.abort_allocation(claim)? {
            return allocation.release_proof(actual).map_err(backend_error);
        }
        if actual.holder == claim.intent.run_id
            && actual.token == claim.intent.claim_token
            && actual.released_at.is_some()
        {
            return Ok(actual.outcome);
        }
        Ok(None)
    }
    /// Releases only an exact journaled acquisition and resolves only an
    /// already-existing allocation. Every mutation/validator is persisted first.
    pub fn abort_claim_authorized(
        &self,
        owner: &RunOwner,
        claim: &AbortClaim,
        progress: &mut AbortClaimProgress,
        mut persist: impl FnMut(&AbortClaimProgress) -> Result<()>,
        mut check_stopped: impl FnMut() -> Result<()>,
    ) -> Result<()> {
        if owner.dataset != claim.intent.dataset
            || owner.control.run_id != claim.intent.run_id
            || owner.control.owner_token != claim.intent.owner_token
        {
            return Err(integrity("abort claim belongs to another stopped owner"));
        }
        let path = key(format!(
            "{}/.claim",
            table_base(
                &claim.intent.dataset,
                &claim.intent.layout,
                &claim.intent.partition
            )?
        ));
        loop {
            if matches!(progress, AbortClaimProgress::Done { .. }) {
                return Ok(());
            }
            check_stopped()?;
            self.require_stopped_epoch(owner)?;
            match progress.clone() {
                AbortClaimProgress::Prepared => {
                    if matches!(claim.evidence, ReservationProgress::Prepared) {
                        save(
                            progress,
                            AbortClaimProgress::Done { allocation: None },
                            &mut persist,
                        )?;
                        continue;
                    }
                    let (actual, validator): (ClaimRecord, _) = match self.read(&path) {
                        Ok(record) => record,
                        Err(e)
                            if e.code == ErrorCode::NotFound
                                && matches!(
                                    claim.evidence,
                                    ReservationProgress::Acquire { expected: None, .. }
                                ) =>
                        {
                            save(
                                progress,
                                AbortClaimProgress::Done { allocation: None },
                                &mut persist,
                            )?;
                            continue;
                        }
                        Err(e) => return Err(e),
                    };
                    if let ReservationProgress::Acquire {
                        claim: proposed,
                        expected,
                    } = &claim.evidence
                    {
                        self.validate_progress_claim(&claim.intent, proposed, false)?;
                        if actual != *proposed && Some(&validator) == expected.as_ref() {
                            save(
                                progress,
                                AbortClaimProgress::Done { allocation: None },
                                &mut persist,
                            )?;
                            continue;
                        }
                        if actual != *proposed
                            && self.abort_release_proof(claim, &actual)?.is_none()
                        {
                            return Err(unknown(
                                "claim acquisition has no exact applied or unchanged-precondition proof",
                            ));
                        }
                    }
                    if let ReservationProgress::Acquired {
                        claim: proposed,
                        validator: expected,
                    } = &claim.evidence
                    {
                        self.validate_progress_claim(&claim.intent, proposed, false)?;
                        if (&actual != proposed || &validator != expected)
                            && self.abort_release_proof(claim, &actual)?.is_none()
                        {
                            return Err(unknown(
                                "acquired-unallocated claim changed before stopped cleanup",
                            ));
                        }
                    }
                    if let ReservationProgress::Allocate {
                        claim: proposed,
                        expected,
                    } = &claim.evidence
                    {
                        self.validate_progress_claim(&claim.intent, proposed, true)?;
                        if actual != *proposed
                            && (validator != *expected
                                || actual.holder != claim.intent.run_id
                                || actual.token != claim.intent.claim_token
                                || actual.version.is_some())
                            && self.abort_release_proof(claim, &actual)?.is_none()
                        {
                            return Err(unknown(
                                "allocation CAS has no exact applied or original-precondition proof",
                            ));
                        }
                    }
                    if let ReservationProgress::Allocated {
                        claim: proposed,
                        validator: expected,
                        allocation,
                    } = &claim.evidence
                    {
                        self.validate_progress_claim(&claim.intent, proposed, true)?;
                        self.validate_intent_allocation(&claim.intent, proposed, allocation)?;
                        if (actual != *proposed || validator != *expected)
                            && self.abort_release_proof(claim, &actual)?.is_none()
                        {
                            return Err(unknown(
                                "allocated claim changed before its allocation record was established",
                            ));
                        }
                    }
                    if let ReservationProgress::Complete { reservation } = &claim.evidence {
                        self.validate_intent_allocation(
                            &claim.intent,
                            &reservation.claim,
                            &reservation.allocation,
                        )?;
                        if actual.token == claim.intent.claim_token
                            && actual.version != Some(reservation.allocation.version)
                        {
                            return Err(integrity("completed reservation claim version changed"));
                        }
                    }
                    if self.abort_release_proof(claim, &actual)?.is_some() {
                        save(
                            progress,
                            AbortClaimProgress::Released { claim: actual },
                            &mut persist,
                        )?;
                        continue;
                    }
                    if actual.holder != claim.intent.run_id
                        || actual.token != claim.intent.claim_token
                        || actual.released_at.is_some()
                    {
                        return Err(unknown("claim is no longer the exact stopped acquisition"));
                    }
                    let allocation = self.abort_allocation(claim)?;
                    if let Some((record, _)) = &allocation {
                        if actual.version != Some(record.version) {
                            return Err(integrity("claim and existing allocation version differ"));
                        }
                    } else if matches!(claim.evidence, ReservationProgress::Complete { .. }) {
                        return Err(unknown("completed reservation lost its allocation history"));
                    }
                    let outcome = if let Some(version) = actual.version {
                        let manifest_key = key(format!(
                            "{}/version={version}/manifest.json",
                            table_base(
                                &claim.intent.dataset,
                                &claim.intent.layout,
                                &claim.intent.partition
                            )?
                        ));
                        let committed = !self.absent(&manifest_key)?;
                        if !committed
                            && allocation.as_ref().is_some_and(|(record, _)| {
                                record.state == AllocationState::Finalized
                            })
                        {
                            return Err(integrity(
                                "finalized allocation lost its committed manifest",
                            ));
                        }
                        if !committed {
                            ClaimOutcome::Abandoned
                        } else {
                            match self.verify_version(
                                &claim.intent.dataset,
                                &claim.intent.layout,
                                &claim.intent.partition,
                                version,
                            ) {
                                Ok(manifest) => {
                                    if manifest.run_id != claim.intent.run_id
                                        || manifest.claim_token != claim.intent.claim_token
                                        || manifest.derived_from != claim.sources
                                    {
                                        return Err(integrity(
                                            "surviving version differs from stopped reservation provenance",
                                        ));
                                    }
                                    if allocation.is_none() {
                                        return Err(unknown(
                                            "committed version has lost its allocation history",
                                        ));
                                    }
                                    ClaimOutcome::Finalized
                                }
                                Err(e) if e.code == ErrorCode::NotFound => {
                                    return Err(integrity(
                                        "committed manifest lost a referenced immutable object",
                                    ));
                                }
                                Err(e) => return Err(e),
                            }
                        }
                    } else {
                        if allocation.is_some() {
                            return Err(integrity(
                                "unallocated claim has unexpected allocation history",
                            ));
                        }
                        ClaimOutcome::Abandoned
                    };
                    let mut released = actual;
                    released.claimed_at = None;
                    released.expires_at = None;
                    released.released_at = Some(self.clock.now());
                    released.outcome = Some(outcome);
                    released.mutation_id = Uuid::v4();
                    save(
                        progress,
                        AbortClaimProgress::Release {
                            claim: released,
                            expected: validator,
                        },
                        &mut persist,
                    )?;
                }
                AbortClaimProgress::Release {
                    claim: released,
                    expected,
                } => {
                    let (actual, _): (ClaimRecord, _) = self.read(&path)?;
                    if self
                        .abort_release_proof(claim, &actual)?
                        .is_some_and(|outcome| Some(outcome) == released.outcome)
                    {
                        save(
                            progress,
                            AbortClaimProgress::Released { claim: actual },
                            &mut persist,
                        )?;
                        continue;
                    }
                    self.commit_claim_transition(&path, &released, Some(&expected))?;
                    save(
                        progress,
                        AbortClaimProgress::Released { claim: released },
                        &mut persist,
                    )?;
                }
                AbortClaimProgress::Released { claim: released } => {
                    let Some((mut allocation, expected)) = self.abort_allocation(claim)? else {
                        save(
                            progress,
                            AbortClaimProgress::Done { allocation: None },
                            &mut persist,
                        )?;
                        continue;
                    };
                    let outcome = allocation
                        .release_proof(&released)
                        .map_err(backend_error)?
                        .ok_or_else(|| {
                            unknown("released claim supplies no exact allocation outcome proof")
                        })?;
                    let target = if outcome == ClaimOutcome::Finalized {
                        AllocationState::Finalized
                    } else {
                        AllocationState::Abandoned
                    };
                    if allocation.state == target {
                        save(
                            progress,
                            AbortClaimProgress::Done {
                                allocation: Some(allocation),
                            },
                            &mut persist,
                        )?;
                        continue;
                    }
                    if allocation.state != AllocationState::Allocated {
                        return Err(unknown(
                            "existing allocation has a different terminal outcome",
                        ));
                    }
                    allocation.state = target;
                    allocation.mutation_id = Uuid::v4();
                    save(
                        progress,
                        AbortClaimProgress::Resolve {
                            allocation,
                            expected,
                        },
                        &mut persist,
                    )?;
                }
                AbortClaimProgress::Resolve {
                    allocation: proposed,
                    expected,
                } => {
                    let Some((actual, validator)) = self.abort_allocation(claim)? else {
                        return Err(unknown("allocation disappeared during stopped cleanup"));
                    };
                    if actual != proposed && validator != expected {
                        if actual.state == proposed.state {
                            save(
                                progress,
                                AbortClaimProgress::Done {
                                    allocation: Some(actual),
                                },
                                &mut persist,
                            )?;
                            continue;
                        }
                        return Err(unknown("allocation resolution precondition changed"));
                    }
                    if actual != proposed {
                        let key = proposed
                            .claim_token
                            .allocation_key(&claim.intent.dataset, &claim.intent.run_id)
                            .map_err(backend_error)?;
                        self.put(&key, &expected, &proposed)?;
                    }
                    save(
                        progress,
                        AbortClaimProgress::Done {
                            allocation: Some(proposed),
                        },
                        &mut persist,
                    )?;
                }
                AbortClaimProgress::Done { .. } => return Ok(()),
            }
        }
    }
}
