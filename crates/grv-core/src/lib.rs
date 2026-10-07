//! Consumer orchestration and canonical staging. No adapter imports this crate.
pub mod admin;
pub mod build;
pub mod build_abort;
pub mod build_export;
pub mod build_publication;
pub mod canonical;
pub mod capture;
pub mod clock;
pub mod contract;
pub mod declaration;
pub mod gc;
pub mod holds;
pub mod inspection;
pub mod journal;
pub mod normalize;
pub mod ownership;
pub mod publication;
pub mod pull;
pub mod push;
pub mod recovery;
mod renewal;
pub mod retention;
pub mod revision;
pub mod source;
pub mod store;

#[derive(Debug)]
pub struct Error(pub String);
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for Error {}
impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self(value.to_string())
    }
}
pub type Result<T> = std::result::Result<T, Error>;
