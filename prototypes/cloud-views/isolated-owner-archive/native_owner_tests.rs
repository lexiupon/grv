//! Supplemental signed-stock adversary under the same process supervisor.
//! This is not full DNS/TLS/chunk/trailer or durable receipt qualification.
use super::*;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::AtomicUsize;

#[test]
#[ignore = "requires explicit GRV_GCS_STOCK_PROOF_BIN and signed GRV_DUCKDB_EXTENSIONS_DIR; synthetic loopback only"]
fn signed_stock_header_trickle_is_process_fenced_before_workspace_reopen() {
    signed_stock_stall_case("header_trickle");
}
#[test]
#[ignore = "explicit signed proof/extensions; synthetic status/body/chunk stalls, not production helper TLS/DNS qualification"]
fn signed_stock_status_body_and_chunk_stalls_are_supervised_without_budget_renewal() {
    for phase in ["status", "body", "chunk", "trailer"] {
        signed_stock_stall_case(phase);
    }
}
fn signed_stock_stall_case(phase: &str) {
    let phase = phase.to_owned();
    let proof =
        PathBuf::from(std::env::var_os("GRV_GCS_STOCK_PROOF_BIN").expect("explicit proof binary"));
    let current = std::env::current_exe().unwrap();
    let proof = validate_helper(&proof, &current).unwrap();
    let extensions = PathBuf::from(
        std::env::var_os("GRV_DUCKDB_EXTENSIONS_DIR").expect("explicit signed extensions"),
    );
    let httpfs = extensions.join("httpfs.duckdb_extension");
    let fixture = tempfile::tempdir().unwrap();
    let engine = fixture.path().join("fenced.duckdb");
    let lock = WorkspaceLock::acquire(&engine).unwrap();
    let inherited = lock.descendant_reference().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(AtomicUsize::new(0));
    let observed = requests.clone();
    let closed = Arc::new(AtomicBool::new(false));
    let stopped = closed.clone();
    let expect_get = matches!(phase.as_str(), "body" | "chunk" | "trailer");
    let server = thread::spawn(move || {
        let end = Instant::now() + Duration::from_secs(6);
        let mut stream: TcpStream = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock && Instant::now() < end => {
                    thread::sleep(Duration::from_millis(2))
                }
                Err(_) => return,
            }
        };
        stream
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let mut header = [0; 8192];
        let n = stream.read(&mut header).unwrap();
        assert!(header[..n].starts_with(b"HEAD "));
        observed.fetch_add(1, Ordering::AcqRel);
        let prefix: &[u8] = match phase.as_str() {
            "header_trickle" => b"HTTP/1.1 200 OK\r\nX-Trickle: ",
            "status" => b"HTTP/1.1 200 ",
            _ => b"HTTP/1.1 200 OK\r\nContent-Length: 4096\r\nAccept-Ranges: bytes\r\n\r\n",
        };
        if stream.write_all(prefix).is_err() {
            stopped.store(true, Ordering::Release);
            return;
        }
        if matches!(phase.as_str(), "body" | "chunk" | "trailer") {
            // Signed stock may create a fresh client/connection after HEAD.
            // This fixture explicitly closes HEAD so GET cannot be mistaken
            // for a persistent-connection read timeout.
            drop(stream);
            stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock && Instant::now() < end => {
                        thread::sleep(Duration::from_millis(2))
                    }
                    Err(_) => {
                        stopped.store(true, Ordering::Release);
                        return;
                    }
                }
            };
            stream
                .set_read_timeout(Some(Duration::from_millis(500)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_millis(100)))
                .unwrap();
            let mut request = [0; 8192];
            let mut used = 0;
            loop {
                match stream.read(&mut request[used..]) {
                    Ok(0) | Err(_) => {
                        stopped.store(true, Ordering::Release);
                        return;
                    }
                    Ok(n) => {
                        used += n;
                        if request[..used].windows(4).any(|b| b == b"\r\n\r\n") {
                            break;
                        }
                    }
                }
                if used == request.len() {
                    panic!("GET header too large");
                }
            }
            assert!(request[..used].starts_with(b"GET "));
            observed.fetch_add(1, Ordering::AcqRel);
            let response: &[u8] = if phase == "body" {
                b"HTTP/1.1 206 Partial Content\r\nContent-Length: 4096\r\nContent-Range: bytes 0-4095/4096\r\n\r\nPAR1"
            } else {
                b"HTTP/1.1 206 Partial Content\r\nTransfer-Encoding: chunked\r\nContent-Range: bytes 0-4095/4096\r\n\r\n"
            };
            if stream.write_all(response).is_err() {
                stopped.store(true, Ordering::Release);
                return;
            }
            if phase == "trailer" {
                let mut complete_chunk = b"1000\r\n".to_vec();
                complete_chunk.extend_from_slice(&[0; 4096]);
                complete_chunk.extend_from_slice(b"\r\n0\r\nX-Trailer: ");
                if stream.write_all(&complete_chunk).is_err() {
                    stopped.store(true, Ordering::Release);
                    return;
                }
            }
        }
        while Instant::now() < end {
            let fragment: &[u8] = if phase == "chunk" {
                b"1\r\nx\r\n"
            } else {
                b"x"
            };
            if stream.write_all(fragment).is_err() {
                stopped.store(true, Ordering::Release);
                return;
            }
            thread::sleep(Duration::from_millis(15));
        }
    });
    let mut command = Command::new(proof);
    command
        .args([
            httpfs.to_str().unwrap(),
            &format!("http://{address}/fixture.parquet"),
            "header_trickle",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    unsafe {
        command.pre_exec(|| {
            if libc::setpgid(0, 0) != 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = command.spawn().unwrap();
    // Allow signed extension setup first; fixture dispatch starts its own250ms
    // native deadline. The operation supervisor budget still never resets.
    let start = Instant::now();
    let deadline = start + Duration::from_millis(1200);
    let supervisor = Arc::new(Supervisor {
        state: Mutex::new(State {
            stop: false,
            done: None,
            deadline: Some(deadline),
        }),
        wake: Condvar::new(),
        cancelled: Arc::new(AtomicBool::new(false)),
        lock: LockFence {
            file: ManuallyDrop::new(inherited),
            proved: AtomicBool::new(false),
        },
    });
    let shared = supervisor.clone();
    let watcher = thread::spawn(move || watch(child, shared, deadline));
    let (stream, _) = UnixStream::pair().unwrap();
    let mut owner = ProcessOwner {
        stream,
        supervisor,
        watcher: Some(watcher),
        deadline,
    };
    owner.watcher.take().unwrap().join().unwrap();
    owner.close().unwrap();
    assert!(start.elapsed() < Duration::from_secs(3));
    assert!(owner.cancelled().load(Ordering::Acquire));
    server.join().unwrap();
    assert_eq!(
        requests.load(Ordering::Acquire),
        if expect_get { 2 } else { 1 },
        "must reach selected signed HEAD/GET adversary"
    );
    assert!(
        closed.load(Ordering::Acquire),
        "connection must close on process fence"
    );
    drop(owner);
    drop(lock);
    let _recovered = WorkspaceLock::acquire(&engine).unwrap();
}
