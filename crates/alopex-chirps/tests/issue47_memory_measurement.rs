use alopex_chirps::memory::{MemoryMeasurement, MemoryStabilityReport};

#[test]
fn stability_report_exposes_peak_final_growth_and_budget_result() {
    let samples = vec![
        MemoryMeasurement::from_values(Some(70), Some(60), Some(100)),
        MemoryMeasurement::from_values(Some(82), Some(75), Some(100)),
        MemoryMeasurement::from_values(Some(80), Some(72), Some(100)),
    ];

    let report = MemoryStabilityReport::from_samples(&samples, 90, 15).unwrap();
    assert_eq!(report.peak_bytes, 82);
    assert_eq!(report.final_bytes, 80);
    assert_eq!(report.growth_bytes, 10);
    assert!(report.within_budget);
    assert!(report.stable);
}
