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
fn salesforce_executable_advertises_only_conformant_extraction_and_closes() {
    let (parent, child_socket) = UnixStream::pair().unwrap();
    parent
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    parent
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let child_fd = child_socket.as_raw_fd();
    let mut command = Command::new(env!("CARGO_BIN_EXE_grv-adapter-salesforce"));
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
    assert_eq!(name.as_str(), "salesforce");
    assert!(capabilities.push && !capabilities.pull && !capabilities.inspect_connection);
    assert_eq!(
        capabilities.source_consistency,
        grv_adapter_api::WireConsistency::CaptureWindow
    );
    assert!(
        !capabilities.managed_build
            && !capabilities.external_build
            && !capabilities.resumable_extract
    );
    assert!(commands.is_empty());
    assert_eq!(registry.points.len(), 8);
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
