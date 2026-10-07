//! Loopback-only scripted peers. All credentials used here are synthetic.
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::TcpListener,
    thread::JoinHandle,
    time::{Duration, Instant},
};
pub struct Request {
    pub line: String,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}
pub struct Response {
    pub status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}
impl Response {
    pub fn new(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: body.into(),
        }
    }
    pub fn header(mut self, key: &str, value: impl Into<String>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }
}
pub struct Peer {
    pub url: String,
    worker: JoinHandle<Vec<Request>>,
}
impl Peer {
    pub fn start(responses: impl FnOnce(&str) -> Vec<Option<Response>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let responses = responses(&url);
        let worker = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for response in responses {
                let deadline = Instant::now() + Duration::from_secs(5);
                let mut socket = loop {
                    match listener.accept() {
                        Ok((socket, _)) => break socket,
                        Err(e)
                            if e.kind() == std::io::ErrorKind::WouldBlock
                                && Instant::now() < deadline =>
                        {
                            std::thread::sleep(Duration::from_millis(1))
                        }
                        Err(e) => panic!("mock accept failed: {e}"),
                    }
                };
                socket.set_nonblocking(false).unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut bytes = Vec::new();
                let header_end = loop {
                    let mut buffer = [0; 4096];
                    let n = socket.read(&mut buffer).unwrap();
                    assert!(n > 0 && bytes.len() + n < 32 * 1024 * 1024);
                    bytes.extend_from_slice(&buffer[..n]);
                    if let Some(index) = bytes.windows(4).position(|p| p == b"\r\n\r\n") {
                        break index + 4;
                    }
                };
                let header = std::str::from_utf8(&bytes[..header_end]).unwrap();
                let mut lines = header.split("\r\n");
                let line = lines.next().unwrap().to_owned();
                let headers: BTreeMap<_, _> = lines
                    .filter_map(|l| l.split_once(':'))
                    .map(|(k, v)| (k.to_ascii_lowercase(), v.trim().to_owned()))
                    .collect();
                assert!(
                    !headers.contains_key("transfer-encoding"),
                    "mock expects bounded content length"
                );
                let length: usize = headers
                    .get("content-length")
                    .map_or(0, |s| s.parse().unwrap());
                // Bound synthetic peer allocations while admitting configurable PUT
                // tests that deliberately cross the default 16 MiB threshold.
                assert!(length <= 32 * 1024 * 1024);
                while bytes.len() - header_end < length {
                    let mut buffer = [0; 8192];
                    let n = socket.read(&mut buffer).unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buffer[..n]);
                }
                requests.push(Request {
                    line,
                    headers,
                    body: bytes[header_end..].to_vec(),
                });
                if let Some(response) = response {
                    write!(
                        socket,
                        "HTTP/1.1 {} mock\r\nContent-Length: {}\r\nConnection: close\r\n",
                        response.status,
                        response.body.len()
                    )
                    .unwrap();
                    for (key, value) in response.headers {
                        write!(socket, "{key}: {value}\r\n").unwrap();
                    }
                    socket.write_all(b"\r\n").unwrap();
                    socket.write_all(&response.body).unwrap();
                }
            }
            requests
        });
        Self { url, worker }
    }
    pub fn finish(self) -> Vec<Request> {
        self.worker.join().unwrap()
    }
}
