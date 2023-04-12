use crate::core::Type;
use std::fmt::{Debug, Display, Formatter};
use std::{error, result};

pub type Result<T> = result::Result<T, Box<dyn error::Error>>;

#[derive(Debug, Clone)]
pub enum Error {
    KeyNotFound,
    HandlerNotFound { key: Type },
    ExpectedMessage { found: Type, expected: Type },
    NotInitializedYet,
    AlreadyInitialized,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let error = match self {
            Error::KeyNotFound => "Key not found.".to_owned(),
            Error::HandlerNotFound { key } => {
                format!(r#"Couldn't find a handler for key "{:?}"."#, key)
            }
            Error::ExpectedMessage { found, expected } => format!(
                r#"Expected "{:?}" message but found "{:?}"."#,
                expected, found
            ),
            Error::NotInitializedYet => "Node is not initialized yet.".to_owned(),
            Error::AlreadyInitialized => "Node is already initialized.".to_owned(),
        };
        write!(f, "{error}")
    }
}

impl error::Error for Error {}
