//! Bounded, fragment-independent framing. A single Channel owns its writer.
pub mod ipc;
pub mod json;
pub mod state;
use grv_adapter_api::Frame;
use serde_json::Value;
use std::io::{self, Read, Write};

pub const FRAME_LIMIT: usize = 1024 * 1024;
pub const IDENTIFIED_LIMIT: usize = 16 * 1024 * 1024;
pub const DOCUMENT_LIMIT: usize = 64 * 1024 * 1024;

#[derive(Debug)]
pub enum ProtocolError {
    Invalid(String),
    Transport(String),
    DocumentTooLarge,
}
impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Invalid(message) | Self::Transport(message) => f.write_str(message),
            Self::DocumentTooLarge => f.write_str("metadata document exceeds 64MiB"),
        }
    }
}
impl std::error::Error for ProtocolError {}
impl From<io::Error> for ProtocolError {
    fn from(e: io::Error) -> Self {
        Self::Transport(e.to_string())
    }
}
impl From<serde_json::Error> for ProtocolError {
    fn from(e: serde_json::Error) -> Self {
        Self::Invalid(e.to_string())
    }
}
pub type Result<T> = std::result::Result<T, ProtocolError>;
pub(crate) fn fail<T>(s: &str) -> Result<T> {
    Err(ProtocolError::Invalid(s.into()))
}
pub fn value(f: &Frame) -> Value {
    serde_json::to_value(f).expect("frame serializes")
}

#[derive(Debug)]
pub struct Packet {
    pub frame: Frame,
    pub payload: Vec<u8>,
}

pub struct Codec<T> {
    io: T,
    max_batch: usize,
    bootstrap: bool,
}
impl<T: Read + Write> Codec<T> {
    pub fn new(io: T, max_batch: usize) -> Self {
        Self {
            io,
            max_batch,
            bootstrap: true,
        }
    }
    pub fn into_inner(self) -> T {
        self.io
    }
    pub fn io_mut(&mut self) -> &mut T {
        &mut self.io
    }
    pub fn read(&mut self) -> Result<Packet> {
        // Never buffer beyond LF: payload and next frame remain untouched.
        let mut bytes = Vec::new();
        loop {
            let mut b = [0];
            match self.io.read(&mut b) {
                Ok(0) => {
                    return if bytes.is_empty() {
                        Err(ProtocolError::Transport("channel EOF".into()))
                    } else {
                        fail("truncated frame")
                    };
                }
                Ok(_) => {
                    bytes.push(b[0]);
                    if b[0] == b'\n' {
                        break;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            }
            let limit = if self.bootstrap {
                IDENTIFIED_LIMIT
            } else {
                FRAME_LIMIT
            };
            if bytes.len() >= limit {
                return fail("frame size exceeded");
            }
        }
        let v = json::parse(&bytes[..bytes.len() - 1])?;
        let msg = v
            .get("msg")
            .and_then(Value::as_str)
            .ok_or_else(|| ProtocolError::Invalid("missing msg".into()))?;
        if bytes.len()
            > if msg == "identified" && self.bootstrap {
                IDENTIFIED_LIMIT
            } else {
                FRAME_LIMIT
            }
        {
            return fail("frame size exceeded");
        }
        let frame: Frame = serde_json::from_value(v.clone())?;
        if msg == "ready" {
            self.bootstrap = false;
        }
        let mut payload = Vec::new();
        if msg == "batch" {
            let size = number_string(&v, "size")? as usize;
            if size == 0 || !size.is_multiple_of(8) || size > self.max_batch {
                return fail("invalid batch size");
            }
            payload.resize(size, 0);
            if let Err(error) = self.io.read_exact(&mut payload) {
                return if error.kind() == io::ErrorKind::UnexpectedEof {
                    fail("truncated binary payload")
                } else {
                    Err(error.into())
                };
            }
        }
        Ok(Packet { frame, payload })
    }
    pub fn write(&mut self, frame: &Frame, payload: &[u8]) -> Result<()> {
        let v = value(frame);
        let msg = v["msg"].as_str().unwrap();
        let bytes = serde_json::to_vec(frame)?;
        if bytes.len() + 1
            > if msg == "identified" && self.bootstrap {
                IDENTIFIED_LIMIT
            } else {
                FRAME_LIMIT
            }
        {
            return fail("frame size exceeded");
        }
        if msg == "batch" {
            let size = number_string(&v, "size")? as usize;
            if size == 0
                || !size.is_multiple_of(8)
                || size > self.max_batch
                || size != payload.len()
            {
                return fail("invalid batch payload length");
            }
        } else if !payload.is_empty() {
            return fail("unexpected binary payload");
        }
        self.io.write_all(&bytes)?;
        self.io.write_all(b"\n")?;
        self.io.write_all(payload)?;
        self.io.flush()?;
        if msg == "ready" {
            self.bootstrap = false;
        }
        Ok(())
    }
}
pub(crate) fn number_string(v: &Value, key: &str) -> Result<u64> {
    v[key]
        .as_str()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| ProtocolError::Invalid(format!("invalid {key}")))
}

pub struct Channel<T> {
    pub codec: Codec<T>,
    pub state: state::State,
}
impl<T: Read + Write> Channel<T> {
    pub fn new(io: T, role: state::Role, max_batch: usize) -> Self {
        Self {
            codec: Codec::new(io, max_batch),
            state: state::State::new(role),
        }
    }
    pub fn send(&mut self, f: &Frame, payload: &[u8]) -> Result<()> {
        self.state.observe(f, true)?;
        self.codec.write(f, payload)
    }
    pub fn receive(&mut self) -> Result<Packet> {
        let mut p = self.codec.read()?;
        if matches!(&p.frame, Frame::DocumentBegin { size, .. } if size.get() > DOCUMENT_LIMIT as u64)
        {
            return Err(ProtocolError::DocumentTooLarge);
        }
        let resolved = self.state.resolved_frame(&p.frame, false)?;
        resolved
            .validate()
            .map_err(|e| ProtocolError::Invalid(e.to_string()))?;
        self.state.observe(&p.frame, false)?;
        p.frame = resolved;
        Ok(p)
    }
}
