use std::path::PathBuf;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Finding {
    Crash { actor: String, note: String },
    Hang { actor: String, last_status: String },
    Protocol { actor: String, token: String },
    Mismatch { actor: String, path: String, detail: String },
    Escaped { path: String },
}

impl Finding {
    pub(crate) fn fingerprint(&self) -> Fingerprint {
        match self {
            Finding::Crash { .. } => Fingerprint::Crash,
            Finding::Hang { .. } => Fingerprint::Hang,
            Finding::Protocol { .. } => Fingerprint::Protocol,
            Finding::Mismatch { path, .. } => Fingerprint::Mismatch { path: path.clone() },
            Finding::Escaped { .. } => Fingerprint::Escaped,
        }
    }

    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Finding::Crash { .. } => "crash",
            Finding::Hang { .. } => "hang",
            Finding::Protocol { .. } => "protocol",
            Finding::Mismatch { .. } => "mismatch",
            Finding::Escaped { .. } => "escaped",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Fingerprint {
    Crash,
    Hang,
    Protocol,
    Mismatch { path: String },
    Escaped,
}

impl std::fmt::Display for Finding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Finding::Crash { actor, note } => write!(f, "crash actor={actor} {note}"),
            Finding::Hang { actor, last_status } => {
                write!(f, "hang actor={actor} status={last_status}")
            }
            Finding::Protocol { actor, token } => write!(f, "protocol actor={actor} {token}"),
            Finding::Mismatch { actor, path, detail } => {
                write!(f, "mismatch actor={actor} path={path} {detail}")
            }
            Finding::Escaped { path } => write!(f, "escaped path={path}"),
        }
    }
}

/// [`Verdict::Clean`] writes nothing. [`Verdict::Finding`] names the artifact.
pub enum Verdict {
    Clean,
    Finding { found: Finding, artifact: PathBuf },
}

#[derive(Debug, thiserror::Error)]
pub enum HarnessError {
    #[error("bin: {0}")]
    Bin(String),
    #[error("limits: {0}")]
    Limits(String),
    #[error("artifact: {0}")]
    Artifact(String),
    #[error("sandbox: {0}")]
    Sandbox(String),
    #[error("io: {0}")]
    Io(String),
}
