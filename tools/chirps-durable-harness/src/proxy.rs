//! Ordering wrapper that makes a synced oracle intent precede one append call.

use crate::oracle::{OracleError, OracleIntent, OracleObservation, OracleStore};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

/// Fault-harness-only wrapper around the independent append-only oracle.
#[derive(Debug)]
pub struct FaultProxy {
    oracle: OracleStore,
}

impl FaultProxy {
    #[must_use]
    pub const fn new(oracle: OracleStore) -> Self {
        Self { oracle }
    }

    /// Syncs intent first and calls the `FnOnce` append boundary exactly once.
    /// If oracle storage fails, this method returns without constructing or
    /// invoking the wrapped append operation.
    pub fn append_after_intent<F, R>(
        &mut self,
        intent: &OracleIntent,
        append: F,
    ) -> Result<R, ProxyError>
    where
        F: FnOnce() -> R,
    {
        self.oracle.persist_intent(intent)?;
        Ok(append())
    }

    /// Appends and syncs one correlated observation without mutating old records.
    pub fn observe(&mut self, observation: &OracleObservation) -> Result<(), ProxyError> {
        self.oracle.append_observation(observation)?;
        Ok(())
    }

    #[must_use]
    pub const fn oracle(&self) -> &OracleStore {
        &self.oracle
    }
}

/// Oracle persistence rejected work before the wrapped append boundary.
#[derive(Debug)]
pub enum ProxyError {
    Oracle(OracleError),
}

impl Display for ProxyError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Oracle(error) => write!(formatter, "fault proxy oracle failed: {error}"),
        }
    }
}

impl Error for ProxyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Oracle(error) => Some(error),
        }
    }
}

impl From<OracleError> for ProxyError {
    fn from(value: OracleError) -> Self {
        Self::Oracle(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oracle::{OracleObservation, OracleRecord, WireObservation, WireStage, test_intent};
    use std::cell::Cell;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_directory(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "chirps-oracle-{name}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn v07_task_6_2_synced_intent_is_readable_before_wrapped_append_runs_once() {
        let directory = unique_directory("ordered");
        fs::create_dir(&directory).unwrap();
        let path = directory.join("oracle.log");
        let intent = test_intent();
        let calls = Cell::new(0);
        let mut proxy = FaultProxy::new(OracleStore::new(&path));

        let result = proxy
            .append_after_intent(&intent, || {
                calls.set(calls.get() + 1);
                let records = OracleStore::new(&path).load().unwrap();
                assert_eq!(records, vec![OracleRecord::Intent(intent.clone())]);
                17
            })
            .unwrap();

        assert_eq!(result, 17);
        assert_eq!(calls.get(), 1);

        let observation = OracleObservation::wire(
            intent.attempt_id(),
            WireObservation::new(
                crate::oracle::APPEND_ONE_SYNCED_CODE,
                WireStage::AppendInvocation,
                1,
                1,
                1,
                1,
            ),
        );
        proxy.observe(&observation).unwrap();
        assert_eq!(
            OracleStore::new(&path).load().unwrap(),
            vec![
                OracleRecord::Intent(intent),
                OracleRecord::Observation(observation),
            ]
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn v07_task_6_2_storage_failure_prevents_wrapped_append() {
        let directory = unique_directory("failure");
        fs::create_dir(&directory).unwrap();
        let intent = test_intent();
        let calls = Cell::new(0);
        let mut proxy = FaultProxy::new(OracleStore::new(&directory));

        let result = proxy.append_after_intent(&intent, || {
            calls.set(calls.get() + 1);
        });

        assert!(matches!(result, Err(ProxyError::Oracle(_))));
        assert_eq!(calls.get(), 0);
        fs::remove_dir_all(directory).unwrap();
    }
}
