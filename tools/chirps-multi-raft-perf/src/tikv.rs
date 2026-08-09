//! TiKV/YCSB-compatible workload contract used for cross-project comparisons.

/// Measured on the local Docker TiKV v8.5.0 control with 500us netem.
/// This is an environment calibration point, not an official TiKV SLO.
pub const LOCAL_DOCKER_TIKV_SHAPED_TOTAL_OPS: f64 = 580.6;
/// UPDATE OPS are the closest write-side proxy for committed proposals/s.
pub const LOCAL_DOCKER_TIKV_SHAPED_UPDATE_OPS: f64 = 291.4;
pub const LOCAL_DOCKER_TIKV_CONTROL_SOURCE: &str =
    "tikv-control-2026-08-08/v8.5.0-workload-a-shaped-500us";

use anyhow::{Result, ensure};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TikvWorkloadA {
    pub nodes: u64,
    pub replication_factor: u64,
    pub record_count: u64,
    pub operation_count: u64,
    pub value_bytes: u64,
    pub read_percent: u8,
    pub update_percent: u8,
}

impl TikvWorkloadA {
    pub const fn reference() -> Self {
        Self {
            nodes: 3,
            replication_factor: 3,
            record_count: 10_000_000,
            operation_count: 30_000_000,
            value_bytes: 1_024,
            read_percent: 50,
            update_percent: 50,
        }
    }

    pub fn validate(self) -> Result<()> {
        ensure!(self.nodes == 3, "TiKV comparison requires three nodes");
        ensure!(
            self.replication_factor == 3,
            "TiKV comparison requires three replicas"
        );
        ensure!(self.record_count > 0, "record_count must be positive");
        ensure!(
            self.operation_count >= self.record_count,
            "operation_count must cover the dataset"
        );
        ensure!(self.value_bytes == 1_024, "comparison value must be 1KiB");
        ensure!(
            self.read_percent + self.update_percent == 100,
            "READ and UPDATE percentages must sum to 100"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_matches_tikv_workload_a_contract() {
        let workload = TikvWorkloadA::reference();
        workload.validate().unwrap();
        assert_eq!(workload.read_percent, 50);
        assert_eq!(workload.update_percent, 50);
        assert_eq!(workload.value_bytes, 1024);
    }

    #[test]
    fn invalid_mix_is_rejected() {
        let workload = TikvWorkloadA {
            read_percent: 60,
            update_percent: 60,
            ..TikvWorkloadA::reference()
        };
        assert!(workload.validate().is_err());
    }

    #[test]
    fn local_control_baseline_is_write_metric() {
        assert_eq!(LOCAL_DOCKER_TIKV_SHAPED_TOTAL_OPS, 580.6);
        assert_eq!(LOCAL_DOCKER_TIKV_SHAPED_UPDATE_OPS, 291.4);
        assert_eq!(
            LOCAL_DOCKER_TIKV_CONTROL_SOURCE,
            "tikv-control-2026-08-08/v8.5.0-workload-a-shaped-500us"
        );
    }
}
