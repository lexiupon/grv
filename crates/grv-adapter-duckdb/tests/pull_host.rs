#![cfg(feature = "native")]
use grv_adapter_api::*;
use grv_adapter_host::{
    discovery::{self, RootKind, SearchRoot},
    process::{Deadlines, Session},
};
use grv_types::{
    DeclarationIdentity, LatestRevision, PullRequestIdentity, RequestedRevision,
    declaration_digest, pull_request_digest,
};
use serde_json::json;
use std::{path::Path, time::Duration};

fn contract() -> TableContract {
    TableContract {
        columns: vec![Column {
            name: "id".into(),
            logical_type: json!("int64"),
        }],
        partition_keys: vec![],
        extensions: json!({}),
        column_ext: json!({}),
    }
}
#[test]
fn typed_host_advertises_gated_transactional_pull_and_replays_each_local_write_mode() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let directory = tempfile::tempdir_in(repository).unwrap();
    let package = directory.path().join("duckdb");
    std::fs::create_dir(&package).unwrap();
    std::fs::write(package.join("adapter.toml"),format!("name = \"duckdb\"\nversion = \"{}\"\ninterface_versions = [1]\nbinding_schema_version = 1\nentrypoint = {:?}\n",env!("CARGO_PKG_VERSION"),env!("CARGO_BIN_EXE_grv-adapter-duckdb"))).unwrap();
    let installation = discovery::discover(&[SearchRoot {
        path: directory.path().into(),
        kind: RootKind::User,
    }])
    .unwrap()
    .remove(0);
    let deadlines = Deadlines {
        bootstrap: Duration::from_secs(10),
        response: Duration::from_secs(10),
        grace: Duration::from_secs(5),
    };
    for write in ["replace", "append"] {
        let root = directory.path().join(format!("root-{write}"));
        std::fs::create_dir(&root).unwrap();
        let database = directory.path().join(format!("host-{write}.duckdb"));
        let mut host = Session::spawn(&installation, deadlines).unwrap();
        assert!(host.capabilities.pull);
        assert_eq!(
            host.capabilities.pull_recovery,
            Some(PullRecovery::Transactional)
        );
        assert_eq!(
            host.capabilities.pull_materializations,
            vec![Materialization::Local, Materialization::S3View]
        );
        assert!(host.capabilities.managed_build && host.capabilities.external_build);
        let declaration=host.validate_binding(json!({"kind":"pull","dataset":"data","adapter":"duckdb","write":write,"connection":{"database":database},"target":{"schema":"app"},"tables":[{"name":"rows","columns":[{"name":"id","type":"int64"}]}]}),Mode::Pull).unwrap();
        let locator = host
            .locate_connection(json!({"database":database}), Mode::Pull, None)
            .unwrap();
        let bound = host
            .bind_connection(
                locator.clone(),
                Some(root.to_str().unwrap().into()),
                locator.identity.clone(),
                None,
                Mode::Pull,
            )
            .unwrap();
        assert_eq!(bound.binding, BindingState::Uninitialized);
        assert_eq!(bound.workspace_id, None);
        let attempt = Uuid::v4();
        let workspace = Uuid::v4();
        let lookup = ResolvePullRequest {
            phase: ResolvePhase::Lookup,
            attempt_id: attempt.clone(),
            root: root.to_str().unwrap().into(),
            request: None,
        };
        assert_eq!(
            host.resolve_pull(bound.handle.clone(), lookup.clone())
                .unwrap()
                .state,
            ResolutionState::NotCommitted
        );
        let identity = host
            .authenticate(bound.handle.clone(), bound.identity.clone())
            .unwrap();
        let adapter = AdapterIdentity {
            name: host.descriptor.name.clone(),
            package_version: host.descriptor.package_version.clone(),
            binding_schema_version: host.descriptor.binding_schema_version,
            interface_version: host.descriptor.interface_version,
        };
        let declaration_sha256 = declaration_digest(&DeclarationIdentity {
            effective_declaration: declaration.clone(),
            adapter_identity: adapter.clone(),
            connection_identity: identity.clone(),
            canonical_connection: locator.canonical_connection,
        })
        .unwrap();
        let requested_revision = RequestedRevision::Latest(LatestRevision::Latest);
        let request_sha256 = pull_request_digest(&PullRequestIdentity {
            root: root.to_str().unwrap().into(),
            workspace_id: workspace.clone(),
            declaration_sha256: declaration_sha256.clone(),
            requested_revision: requested_revision.clone(),
        })
        .unwrap();
        let request = RequestRecord {
            attempt_id: attempt,
            root: root.to_str().unwrap().into(),
            dataset: Name::new("data").unwrap(),
            workspace_id: workspace.clone(),
            adapter_identity: adapter,
            connection_identity: identity,
            validation_input: declaration.clone(),
            effective_declaration: declaration,
            declaration_sha256,
            request_sha256,
            registry: host.registry.registry.clone(),
            requested_revision,
        };
        let compare = ResolvePullRequest {
            phase: ResolvePhase::Compare,
            request: Some(request.clone()),
            ..lookup.clone()
        };
        assert_eq!(
            host.resolve_pull(bound.handle.clone(), compare.clone())
                .unwrap()
                .state,
            ResolutionState::NotCommitted
        );
        let plan = host
            .prepare_pull(
                bound.handle.clone(),
                PreparePullRequest {
                    request,
                    resolved_revision: U64::new(0).unwrap(),
                    tables: vec![PullTable {
                        name: Name::new("rows").unwrap(),
                        target: json!({"table":"rows"}),
                        select: None,
                        partitions: vec![],
                        source_contract: contract(),
                        output_contract: contract(),
                    }],
                    files: vec![],
                    recovery: None,
                },
            )
            .unwrap();
        assert_eq!(plan.refresh, Refresh::Full);
        let receipt = host.apply_pull(bound.handle, plan).unwrap();
        assert_eq!(receipt.row_counts[0].rows.get(), 0);
        host.close().unwrap();
        let mut replay = Session::spawn(&installation, deadlines).unwrap();
        let locator = replay
            .locate_connection(json!({"database":database}), Mode::Pull, None)
            .unwrap();
        let bound = replay
            .bind_connection(
                locator.clone(),
                Some(root.to_str().unwrap().into()),
                locator.identity,
                Some(workspace),
                Mode::Pull,
            )
            .unwrap();
        assert_eq!(bound.binding, BindingState::Bound);
        assert_eq!(
            replay
                .resolve_pull(bound.handle.clone(), lookup)
                .unwrap()
                .receipt,
            Some(receipt.clone())
        );
        assert_eq!(
            replay.resolve_pull(bound.handle, compare).unwrap().receipt,
            Some(receipt)
        );
        replay.close().unwrap();
    }
}
