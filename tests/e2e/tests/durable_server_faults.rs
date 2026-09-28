use anyhow::{Result, ensure};
use chirps_e2e::v07::AppendObservationStage;
use chirps_fault_oracle::WireStage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExpectedTerminal {
    OsSyncedAccepted,
    StartupRejected,
    Indeterminate,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct AppendScenario {
    pub(crate) name: &'static str,
    pub(crate) failpoint: Option<&'static str>,
    pub(crate) expected: ExpectedTerminal,
    pub(crate) observed_before_failure: &'static [AppendObservationStage],
}

const PRODUCTION: &[AppendScenario] = &[AppendScenario {
    name: "clean-control",
    failpoint: None,
    expected: ExpectedTerminal::OsSyncedAccepted,
    observed_before_failure: &[],
}];

const FAULTS: &[AppendScenario] = &[
    AppendScenario {
        name: "message-open-sync",
        failpoint: Some("message-open-sync"),
        expected: ExpectedTerminal::StartupRejected,
        observed_before_failure: &[],
    },
    AppendScenario {
        name: "index-open-sync",
        failpoint: Some("index-open-sync"),
        expected: ExpectedTerminal::StartupRejected,
        observed_before_failure: &[],
    },
    AppendScenario {
        name: "append",
        failpoint: Some("append"),
        expected: ExpectedTerminal::Indeterminate,
        observed_before_failure: &[
            AppendObservationStage::WireWrite,
            AppendObservationStage::JournalFlush,
        ],
    },
    AppendScenario {
        name: "journal-flush",
        failpoint: Some("journal-flush"),
        expected: ExpectedTerminal::Indeterminate,
        observed_before_failure: &[AppendObservationStage::WireWrite],
    },
    AppendScenario {
        name: "message-sync",
        failpoint: Some("message-sync"),
        expected: ExpectedTerminal::Indeterminate,
        observed_before_failure: &[
            AppendObservationStage::WireWrite,
            AppendObservationStage::JournalFlush,
        ],
    },
    AppendScenario {
        name: "index-sync",
        failpoint: Some("index-sync"),
        expected: ExpectedTerminal::Indeterminate,
        observed_before_failure: &[
            AppendObservationStage::WireWrite,
            AppendObservationStage::JournalFlush,
            AppendObservationStage::MessageSync,
        ],
    },
    AppendScenario {
        name: "response",
        failpoint: Some("response"),
        expected: ExpectedTerminal::Indeterminate,
        observed_before_failure: &[
            AppendObservationStage::WireWrite,
            AppendObservationStage::JournalFlush,
            AppendObservationStage::MessageSync,
            AppendObservationStage::IndexSync,
        ],
    },
];

pub(crate) fn scenarios(lane: &str) -> Result<&'static [AppendScenario]> {
    let scenarios = match lane {
        "production" => PRODUCTION,
        "fault" => FAULTS,
        _ => anyhow::bail!("durable_send received an unknown runner lane"),
    };
    ensure!(
        scenarios
            .iter()
            .all(|scenario| scenario.failpoint.is_none() == (lane == "production")),
        "durable_send mixed production and fault scenarios"
    );
    Ok(scenarios)
}

/// The stages that Task 6.2 can represent after an external observer reports
/// them. Open/bootstrap sync remains a startup observation rather than a
/// fabricated `WireStage`.
pub(crate) const ORACLE_RUNTIME_STAGES: [WireStage; 6] = [
    WireStage::AppendInvocation,
    WireStage::WireWrite,
    WireStage::JournalFlush,
    WireStage::MessageSync,
    WireStage::IndexSync,
    WireStage::Response,
];
