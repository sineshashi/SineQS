use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum MessageIOError {
    SegmentOverFlow(String),
    IOError(std::io::Error),
    CustomError(String),
    PartitionOverFlow(String),
}

impl Error for MessageIOError {}

impl fmt::Display for MessageIOError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::SegmentOverFlow(msg) => write!(f, "SegmentOverFlow: {}", msg),
            Self::IOError(msg) => write!(f, "IOError: {:?}", msg),
            Self::CustomError(msg) => writeln!(f, "Custom Error {:?}", msg),
            Self::PartitionOverFlow(msg) => writeln!(f, "Partition Over Flow Error {:?}", msg),
        }
    }
}

impl From<std::io::Error> for MessageIOError {
    fn from(error: std::io::Error) -> Self {
        MessageIOError::IOError(error)
    }
}
