//! Optional adapter-selected metadata admission. Not a wire version or allocator.
use crate::{ProtocolError, Result};
use std::io::{self, Write};

pub const GCS_DOCUMENT_BYTES: usize = 8 * 1024 * 1024;
pub const GCS_LOGICAL_BYTES: usize = 8 * 1024 * 1024;
pub const GCS_DOCUMENT_IDS: usize = 1024;
pub const GCS_DEPTH: usize = 64;

/// Streaming lexical reserve for standard serde JSON and borrowed incoming JSON.
/// String bytes cost one; non-string bytes reserve256 for nodes/containers.
/// This is conservative logical admission, not physical capacity/RSS proof.
#[derive(Default)]
pub struct Admission {
    charged: usize,
    in_string: bool,
    escaped: bool,
    depth: usize,
}
impl Admission {
    pub fn include(&mut self, value: &impl serde::Serialize) -> Result<()> {
        serde_json::to_writer(&mut *self, value)?;
        if self.in_string || self.depth != 0 {
            return Err(ProtocolError::Invalid("incomplete bounded metadata".into()));
        }
        Ok(())
    }
    pub fn raw(bytes: &[u8]) -> Result<()> {
        let mut counter = Self::default();
        counter.write_all(bytes)?;
        if counter.in_string || counter.depth != 0 {
            return Err(ProtocolError::Invalid("incomplete bounded metadata".into()));
        }
        Ok(())
    }
}
impl Write for Admission {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        for &byte in bytes {
            self.charged = self
                .charged
                .checked_add(if self.in_string { 1 } else { 256 })
                .filter(|n| *n <= GCS_LOGICAL_BYTES)
                .ok_or_else(|| io::Error::other("GCS metadata logical allowance exceeded"))?;
            if self.in_string {
                if self.escaped {
                    self.escaped = false;
                } else if byte == b'\\' {
                    self.escaped = true;
                } else if byte == b'"' {
                    self.in_string = false;
                }
            } else {
                match byte {
                    b'"' => self.in_string = true,
                    b'{' | b'[' => {
                        self.depth += 1;
                        if self.depth > GCS_DEPTH {
                            return Err(io::Error::other(
                                "GCS metadata nesting allowance exceeded",
                            ));
                        }
                    }
                    b'}' | b']' => {
                        self.depth = self
                            .depth
                            .checked_sub(1)
                            .ok_or_else(|| io::Error::other("invalid bounded metadata nesting"))?
                    }
                    _ => {}
                }
            }
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub fn admit(value: &impl serde::Serialize) -> Result<()> {
    Admission::default().include(value)
}

#[cfg(test)]
mod tests {
    #[test]
    fn raw_admission_refuses_nodes_before_parse_and_preserves_escapes() {
        let bytes = serde_json::to_vec(&vec![""; 40000]).unwrap();
        assert!(super::Admission::raw(&bytes).is_err());
        let value = serde_json::json!({"quoted":"\\\"{[🍕","nested":[null,{}]});
        super::admit(&value).unwrap();
        super::Admission::raw(&serde_json::to_vec(&value).unwrap()).unwrap();
        let mut deep = String::new();
        deep.push_str(&"[".repeat(65));
        deep.push('0');
        deep.push_str(&"]".repeat(65));
        assert!(super::Admission::raw(deep.as_bytes()).is_err());
    }
}
