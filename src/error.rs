use std::io;

#[derive(Debug)]
pub enum KvsError {
    /// IO error.
    IO(io::Error),
    Serde(serde_json::Error),
    UnexpectedCommandType,
    KeyNotFound,
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::IO(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> KvsError {
        KvsError::Serde(err)
    }
}

pub type Result<T> = std::result::Result<T, KvsError>;
