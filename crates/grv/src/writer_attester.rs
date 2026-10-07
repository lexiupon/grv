//! Local writer evidence comes from a protected attempt journal retained under
//! its exclusive process lock. Remote in-flight writes need separate proof.
use super::backend::Root;
use grv_core::{
    gc::{StoppedWriterEvidence, WriterAttester},
    journal::Journal,
    store::Result,
};
use grv_storage::model::RunControl;
use grv_types::{ErrorCode, Name, Uuid};
use std::{
    cell::RefCell,
    collections::BTreeMap,
    path::{Path, PathBuf},
};

pub(super) struct LocalWriterAttester {
    state: PathBuf,
    canonical: String,
    local: Option<PathBuf>,
    locks: RefCell<BTreeMap<PathBuf, Journal>>,
}
impl LocalWriterAttester {
    pub(super) fn new(state: &Path, root: &Root) -> Self {
        Self {
            state: state.into(),
            canonical: root.canonical.clone(),
            local: root.local.clone(),
            locks: RefCell::new(BTreeMap::new()),
        }
    }
}
impl WriterAttester for LocalWriterAttester {
    fn stopped(
        &self,
        dataset: &Name,
        control: &RunControl,
    ) -> Result<Option<StoppedWriterEvidence>> {
        let Some(local) = &self.local else {
            return Ok(None);
        };
        let Some(metadata) = control.metadata.as_ref().and_then(|m| m.get("grv_cli")) else {
            return Ok(None);
        };
        let Some(attempt) = metadata["attempt_id"]
            .as_str()
            .and_then(|s| Uuid::new(s).ok())
        else {
            return Ok(None);
        };
        let directory = self.state.join("push").join(attempt.as_str());
        let mut locks = self.locks.borrow_mut();
        if !locks.contains_key(&directory) {
            match Journal::open(&directory, std::slice::from_ref(local)) {
                Ok(journal) => {
                    locks.insert(directory.clone(), journal);
                }
                Err(error)
                    if matches!(
                        error.code,
                        ErrorCode::StateConflict
                            | ErrorCode::EngineBusy
                            | ErrorCode::NotFound
                            | ErrorCode::IntegrityFailure
                            | ErrorCode::BackendFailure
                    ) =>
                {
                    return Ok(None);
                }
                Err(error) => return Err(error),
            }
        }
        let journal = locks.get(&directory).unwrap();
        let proof = match metadata.get("mode").and_then(serde_json::Value::as_str) {
            Some("build") => {
                super::build::stopped_export_proof(journal, dataset, control, &self.canonical)
            }
            None | Some("extract") => {
                super::transfer::stopped_capture_proof(journal, dataset, control, &self.canonical)
            }
            _ => Ok(None),
        };
        let proof = match proof {
            Ok(proof) => proof,
            Err(error) if error.code == ErrorCode::RequestMismatch => None,
            Err(error) => return Err(error),
        };
        Ok(proof.map(|proof| StoppedWriterEvidence::verified(dataset.clone(), control, proof)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grv_core::{
        clock::{Clock, SystemClock, new_run_id},
        ownership::Ownership,
        store::{InitOptions, Store},
    };
    use grv_storage::{LocalBackend, model::Counter};
    use serde_json::json;
    fn control(temp: &Path, attempt: &Uuid) -> RunControl {
        let (store, _) = Store::initialize(
            LocalBackend::create(temp.join("grv")).unwrap(),
            InitOptions::default(),
        )
        .unwrap();
        let clock = SystemClock::default();
        Ownership::new(&store, &clock, 900)
            .unwrap()
            .prepare_run(
                Name::new("data").unwrap(),
                new_run_id(&clock.now()).unwrap(),
                Counter::from(0),
                vec![],
                Some(BTreeMap::from([(
                    "grv_cli".into(),
                    json!({"attempt_id":attempt,"mode":"build"}),
                )])),
            )
            .unwrap()
            .control()
            .clone()
    }
    #[test]
    fn live_journal_lock_never_attests_a_writer_or_fails_recovery() {
        let temp = tempfile::tempdir_in(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .parent()
                .unwrap(),
        )
        .unwrap();
        let attempt = Uuid::v4();
        let control = control(temp.path(), &attempt);
        let root = Root::parse(temp.path().join("grv").to_str().unwrap()).unwrap();
        let state = temp.path().join("state");
        let _writer =
            Journal::create(state.join("push").join(attempt.as_str()), root.exclusions()).unwrap();
        let attester = LocalWriterAttester::new(&state, &root);
        assert!(
            attester
                .stopped(&Name::new("data").unwrap(), &control)
                .unwrap()
                .is_none()
        );
        assert!(attester.locks.borrow().is_empty());
    }
    #[test]
    fn cloud_writer_evidence_never_comes_from_a_local_process_lock() {
        let temp = tempfile::tempdir().unwrap();
        let attempt = Uuid::v4();
        let control = control(temp.path(), &attempt);
        let root = Root {
            canonical: "s3://fixture/readonly/".into(),
            local: None,
        };
        let state = temp.path().join("does-not-exist");
        let attester = LocalWriterAttester::new(&state, &root);
        assert!(
            attester
                .stopped(&Name::new("data").unwrap(), &control)
                .unwrap()
                .is_none()
        );
        assert!(!state.exists());
    }
}
