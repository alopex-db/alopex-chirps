use serde_json::Value;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn required<'a>(value: &'a Value, path: &[&str]) -> &'a Value {
    path.iter().fold(value, |current, key| {
        current
            .get(key)
            .unwrap_or_else(|| panic!("missing contract field {}", path.join(".")))
    })
}

#[test]
fn layered_performance_contract_is_complete_and_wired() {
    let root = workspace_root();
    let contract_path = root.join("formal/file-transfer/performance-contract.json");
    let contract: Value = serde_json::from_slice(
        &std::fs::read(&contract_path).expect("read FileTransfer performance contract"),
    )
    .expect("parse FileTransfer performance contract");

    assert_eq!(contract["schema"], "alopex-performance-contract/v1");
    assert_eq!(contract["requirement_id"], "FT-THROUGHPUT-100MBPS");
    let workload = required(&contract, &["workload"]);
    let file_bytes = workload["file_bytes"].as_u64().expect("file_bytes");
    let chunk_bytes = workload["chunk_bytes"].as_u64().expect("chunk_bytes");
    let chunk_count = workload["chunk_count"].as_u64().expect("chunk_count");
    assert_eq!(file_bytes, chunk_bytes * chunk_count);
    assert_eq!(workload["concurrency"], 4);
    assert_eq!(workload["compression"], "none");
    assert_eq!(workload["resumable"], true);

    let layers = required(&contract, &["layers"]);
    assert_eq!(
        layers
            .as_object()
            .expect("layers object")
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["binary", "function", "module", "service"]),
    );

    let function = &layers["function"];
    let function_harness = root.join(function["harness"].as_str().expect("function harness"));
    let function_source = std::fs::read_to_string(&function_harness).expect("function harness");
    for operation in function["operations"]
        .as_array()
        .expect("function operations")
    {
        let operation = operation.as_str().expect("operation name");
        let leaf = operation.rsplit('/').next().expect("operation leaf");
        assert!(
            function_source.contains(leaf),
            "function benchmark {operation} is not wired in {}",
            function_harness.display()
        );
    }
    assert_eq!(
        function["comparison"]["method"],
        "criterion_same_host_baseline"
    );
    assert_eq!(function["comparison"]["maximum_regression_ratio"], 0.1);

    for invariant in layers["module"]["invariants"]
        .as_array()
        .expect("module invariants")
    {
        let test = invariant["test"].as_str().expect("module test");
        let (path, name) = test.rsplit_once("::").expect("path::test_name");
        let source = std::fs::read_to_string(root.join(path)).expect("module test source");
        assert!(
            source.contains(&format!("fn {name}")),
            "module invariant {} is not wired to {test}",
            invariant["id"]
        );
    }

    let service = &layers["service"];
    assert!(root.join(service["harness"].as_str().unwrap()).is_file());
    assert_eq!(service["control_plane"], "chirps-quic");
    assert_eq!(service["data_plane"], "quic-chunk-stream");
    assert!(service["phases"]["sender"].as_object().unwrap().len() >= 4);
    assert!(service["phases"]["receiver"].as_object().unwrap().len() >= 4);

    let binary = &layers["binary"];
    assert!(root.join(binary["harness"].as_str().unwrap()).is_file());
    assert_eq!(binary["profile_id"], "ft-1g-v1");
    assert_eq!(binary["sample_count"], 5);
    assert_eq!(binary["minimum_end_to_end_bytes_per_second"], 100_000_000);
    assert_eq!(binary["aggregation"], "all_samples");
    assert_eq!(binary["requires_native_linux_profile"], true);
    assert_eq!(binary["requires_integrity_and_identity"], true);
}
