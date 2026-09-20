use std::fmt;
use std::num::{NonZeroU16, NonZeroUsize};

use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize};

use crate::config::ConfigError;

pub const WORKERS_MIN: u16 = 1;
pub const WORKERS_MAX: u16 = 256;
pub const INFLIGHT_MIN: u16 = 1;
pub const INFLIGHT_MAX: u16 = 64;
pub const INFLIGHT_DEFAULT: u16 = 4;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TuneSpec {
    #[serde(default)]
    pub hashing: Option<HashingSpec>,
    #[serde(default)]
    pub fulfill_parked: Option<FulfillParkedSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HashingSpec {
    #[serde(default = "default_workers_spec")]
    pub workers: WorkerCountSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FulfillParkedSpec {
    #[serde(default = "default_inflight")]
    pub inflight: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerCountSpec {
    Nproc,
    Count(NonZeroU16),
}

fn default_workers_spec() -> WorkerCountSpec {
    WorkerCountSpec::Nproc
}

fn default_inflight() -> u16 {
    INFLIGHT_DEFAULT
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkerCount(NonZeroUsize);

impl WorkerCount {
    pub fn get(self) -> usize {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FulfillCap(NonZeroUsize);

impl FulfillCap {
    pub fn get(self) -> usize {
        self.0.get()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tune {
    hashing_workers: WorkerCount,
    hashing_workers_spec: WorkerCountSpec,
    fulfill_cap: Option<FulfillCap>,
}

impl Tune {
    pub fn hashing_workers(&self) -> WorkerCount {
        self.hashing_workers
    }

    pub fn hashing_workers_spec(&self) -> &WorkerCountSpec {
        &self.hashing_workers_spec
    }

    pub fn fulfill_cap(&self) -> Option<FulfillCap> {
        self.fulfill_cap
    }

    pub fn fulfill_admission(&self) -> Option<FulfillAdmission> {
        self.fulfill_cap.map(FulfillAdmission::new)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FulfillAdmission {
    limit: FulfillCap,
}

impl FulfillAdmission {
    pub const LARGE_BYTES: u64 = 16 * 1024 * 1024;

    pub fn new(limit: FulfillCap) -> Self {
        Self { limit }
    }

    pub fn admits(self, sending: usize, any_large: bool, next_size: Option<u64>) -> bool {
        sending < self.limit.get()
            && !(next_size.is_some_and(|size| size >= Self::LARGE_BYTES) && any_large)
    }

    pub fn is_large(size: u64) -> bool {
        size >= Self::LARGE_BYTES
    }

    pub fn limit(self) -> FulfillCap {
        self.limit
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TuneRole {
    Master,
    Slave,
}

pub fn project_tune(spec: TuneSpec, role: TuneRole) -> Result<Tune, ConfigError> {
    if spec.fulfill_parked.is_some() && role == TuneRole::Master {
        return Err(ConfigError::TuneNotOnRole {
            field: "tune.fulfill_parked".into(),
            role: "master",
        });
    }

    let hashing_workers_spec = spec
        .hashing
        .map(|h| h.workers)
        .unwrap_or(WorkerCountSpec::Nproc);
    let hashing_workers = resolve_nproc(&hashing_workers_spec)?;

    let fulfill_cap = match (role, spec.fulfill_parked) {
        (TuneRole::Master, _) => None,
        (TuneRole::Slave, parked) => {
            let value = parked.map(|p| p.inflight).unwrap_or(INFLIGHT_DEFAULT);
            if !(INFLIGHT_MIN..=INFLIGHT_MAX).contains(&value) {
                return Err(ConfigError::InflightOutOfRange { value });
            }
            Some(FulfillCap(
                NonZeroUsize::new(usize::from(value)).expect("inflight min is 1"),
            ))
        }
    };

    Ok(Tune {
        hashing_workers,
        hashing_workers_spec,
        fulfill_cap,
    })
}

pub fn resolve_nproc(spec: &WorkerCountSpec) -> Result<WorkerCount, ConfigError> {
    let value = match spec {
        WorkerCountSpec::Nproc => {
            let n = match std::thread::available_parallelism() {
                Ok(n) => n.get(),
                Err(err) => {
                    log::warn!("available_parallelism failed ({err}); using 1");
                    1
                }
            };
            n.min(usize::from(WORKERS_MAX))
                .max(usize::from(WORKERS_MIN))
        }
        WorkerCountSpec::Count(n) => {
            let value = n.get();
            if value > WORKERS_MAX {
                return Err(ConfigError::WorkersOutOfRange { value });
            }
            usize::from(value)
        }
    };
    Ok(WorkerCount(
        NonZeroUsize::new(value).expect("worker count min is 1"),
    ))
}

impl fmt::Display for WorkerCountSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Nproc => write!(f, "nproc"),
            Self::Count(n) => write!(f, "{n}"),
        }
    }
}

impl Serialize for WorkerCountSpec {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Nproc => serializer.serialize_str("nproc"),
            Self::Count(n) => serializer.serialize_u16(n.get()),
        }
    }
}

impl<'de> Deserialize<'de> for WorkerCountSpec {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct WorkerCountVisitor;

        impl<'de> Visitor<'de> for WorkerCountVisitor {
            type Value = WorkerCountSpec;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "\"nproc\" or an integer")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                if value == "nproc" {
                    Ok(WorkerCountSpec::Nproc)
                } else {
                    Err(de::Error::custom(format!(
                        "tune.hashing.workers {value} is not \"nproc\" or an integer"
                    )))
                }
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
                let n = u16::try_from(value).map_err(|_| {
                    de::Error::custom(format!("tune.hashing.workers {value} exceeds u16"))
                })?;
                NonZeroU16::new(n)
                    .map(WorkerCountSpec::Count)
                    .ok_or_else(|| de::Error::custom("tune.hashing.workers 0 is not in 1..=256"))
            }

            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
                let n = u16::try_from(value).map_err(|_| {
                    de::Error::custom(format!("tune.hashing.workers {value} is not a u16"))
                })?;
                NonZeroU16::new(n)
                    .map(WorkerCountSpec::Count)
                    .ok_or_else(|| de::Error::custom("tune.hashing.workers 0 is not in 1..=256"))
            }
        }

        deserializer.deserialize_any(WorkerCountVisitor)
    }
}
