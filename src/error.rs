use std::fmt;

#[derive(Debug)]
pub enum QueueError {
    Sqlite(rusqlite::Error),
    LockPoisoned,
    NotFound(String),
    AlreadyExists(String),
    InvalidStatus(String),
    JobFailed(String),
}

impl fmt::Display for QueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueueError::Sqlite(e) => write!(f, "database error: {e}"),
            QueueError::LockPoisoned => write!(f, "lock poisoned"),
            QueueError::NotFound(id) => write!(f, "job not found: {id}"),
            QueueError::AlreadyExists(id) => write!(f, "job already exists: {id}"),
            QueueError::InvalidStatus(s) => write!(f, "invalid status: {s}"),
            QueueError::JobFailed(msg) => write!(f, "job failed: {msg}"),
        }
    }
}

impl std::error::Error for QueueError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            QueueError::Sqlite(e) => Some(e),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for QueueError {
    fn from(e: rusqlite::Error) -> Self {
        QueueError::Sqlite(e)
    }
}

impl<T> From<std::sync::PoisonError<T>> for QueueError {
    fn from(_: std::sync::PoisonError<T>) -> Self {
        QueueError::LockPoisoned
    }
}
