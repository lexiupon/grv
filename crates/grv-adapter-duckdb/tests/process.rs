#![cfg(unix)]
use grv_adapter_api::{CoreIdentity, Frame, Req, Resources, Uuid};
use grv_adapter_wire::{Channel, state::Role};
use std::{
    os::{
        fd::AsRawFd,
        unix::{net::UnixStream, process::CommandExt},
    },
    process::{Command, Stdio},
    time::Duration,
};

#[test]
fn duckdb_executable_handshakes_without_claiming_ungated_capabilities_and_closes() {
    let (parent, child_socket) = UnixStream::pair().unwrap();
    parent
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    parent
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let child_fd = child_socket.as_raw_fd();
    let mut command = Command::new(env!("CARGO_BIN_EXE_grv-adapter-duckdb"));
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    unsafe {
        command.pre_exec(move || {
            if libc::dup2(child_fd, 3) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            // dup2 does not clear CLOEXEC when the descriptors are identical.
            if libc::fcntl(3, libc::F_SETFD, 0) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = command.spawn().unwrap();
    drop(child_socket);
    let resources = Resources::default();
    let mut channel = Channel::new(
        parent,
        Role::Parent,
        resources.max_batch_bytes.get() as usize,
    );
    channel
        .send(
            &Frame::Hello {
                interface_versions: vec![Req::new(1).unwrap()],
                core: CoreIdentity {
                    name: "grv".into(),
                    version: "0.1.0".into(),
                },
                attempt: Uuid::v4(),
                resources: resources.clone(),
            },
            &[],
        )
        .unwrap();
    let identified = channel.receive().unwrap().frame;
    let Frame::Identified {
        name,
        capabilities,
        commands,
        registry,
        ..
    } = identified
    else {
        panic!("expected identified");
    };
    assert_eq!(name.as_str(), "duckdb");
    assert_eq!(capabilities.inspect_connection, cfg!(feature = "native"));
    assert_eq!(capabilities.push, cfg!(feature = "native"));
    assert_eq!(capabilities.pull, cfg!(feature = "native"));
    if cfg!(feature = "native") {
        assert_eq!(
            capabilities.source_consistency,
            grv_adapter_api::WireConsistency::Snapshot
        );
        assert_eq!(
            capabilities.pull_write_modes,
            vec![
                grv_adapter_api::WriteMode::Replace,
                grv_adapter_api::WriteMode::Append
            ]
        );
        assert_eq!(
            capabilities.pull_materializations,
            vec![
                grv_adapter_api::Materialization::Local,
                grv_adapter_api::Materialization::S3View,
            ]
        );
        assert_eq!(
            capabilities.pull_recovery,
            Some(grv_adapter_api::PullRecovery::Transactional)
        );
    }
    assert_eq!(capabilities.managed_build, cfg!(feature = "native"));
    assert_eq!(capabilities.inspect_connection, cfg!(feature = "native"));
    assert_eq!(capabilities.external_build, cfg!(feature = "native"));
    assert!(!capabilities.resumable_extract);
    assert!(commands.is_empty());
    registry.validate().unwrap();
    assert!(
        registry
            .points
            .iter()
            .any(|point| point.point == "connection")
    );
    channel
        .send(
            &Frame::Ready {
                interface_version: Req::new(1).unwrap(),
                binding_schema_version: Req::new(1).unwrap(),
                resources,
            },
            &[],
        )
        .unwrap();
    channel
        .send(
            &Frame::Close {
                req: Req::new(1).unwrap(),
            },
            &[],
        )
        .unwrap();
    assert!(matches!(
        channel.receive().unwrap().frame,
        Frame::CloseResult { .. }
    ));
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    assert!(output.stdout.is_empty());
    assert!(output.stderr.is_empty());
}
