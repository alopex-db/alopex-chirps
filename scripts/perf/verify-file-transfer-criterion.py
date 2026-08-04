#!/usr/bin/env python3
"""Validate Criterion output against the FileTransfer performance contract."""

import argparse
import json
import pathlib
import sys


def arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--current", required=True, type=pathlib.Path)
    parser.add_argument("--baseline", type=pathlib.Path)
    parser.add_argument("--output", required=True, type=pathlib.Path)
    parser.add_argument(
        "--contract",
        type=pathlib.Path,
        default=pathlib.Path("formal/file-transfer/performance-contract.json"),
    )
    return parser.parse_args()


def estimate(root, operation):
    path = root / "file_transfer_components" / operation / "new" / "estimates.json"
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
        point = float(document["mean"]["point_estimate"])
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError(f"missing/invalid Criterion estimate for {operation}: {path}: {error}") from error
    if point <= 0:
        raise ValueError(f"non-positive Criterion estimate for {operation}: {point}")
    return point, path


def main():
    args = arguments()
    contract = json.loads(args.contract.read_text(encoding="utf-8"))
    function = contract["layers"]["function"]
    maximum = float(function["comparison"]["maximum_regression_ratio"])
    absolute_minimum = function["absolute_minimum_bytes_per_second"]
    workload = contract["workload"]
    operation_bytes = {
        operation: (
            workload["chunk_bytes"]
            if "1mib" in operation and "128x1mib" not in operation
            else workload["file_bytes"]
        )
        for operation in function["operations"]
    }
    results = []
    passed = True
    for operation in function["operations"]:
        current, current_path = estimate(args.current, operation)
        item = {
            "operation": operation,
            "current_mean_nanoseconds": current,
            "current_estimate": str(current_path),
        }
        current_bytes_per_second = operation_bytes[operation] / (current / 1_000_000_000)
        absolute_floor = float(absolute_minimum[operation])
        absolute_passed = current_bytes_per_second >= absolute_floor
        item.update(
            current_bytes_per_second=current_bytes_per_second,
            absolute_minimum_bytes_per_second=absolute_floor,
            absolute_passed=absolute_passed,
        )
        passed = passed and absolute_passed
        if args.baseline is not None:
            baseline, baseline_path = estimate(args.baseline, operation)
            ratio = current / baseline - 1.0
            operation_passed = ratio <= maximum
            item.update(
                baseline_mean_nanoseconds=baseline,
                baseline_estimate=str(baseline_path),
                regression_ratio=ratio,
                passed=operation_passed,
            )
            passed = passed and operation_passed
        results.append(item)

    report = {
        "schema": "chirps-file-transfer-function-calibration/v1",
        "requirement_id": contract["requirement_id"],
        "comparison_method": function["comparison"]["method"],
        "maximum_regression_ratio": maximum,
        "baseline_supplied": args.baseline is not None,
        "absolute_limits_supplied": True,
        "operations": results,
        "passed": passed,
        "release_evidence": False,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, sort_keys=True))
    return 0 if passed else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        print(error, file=sys.stderr)
        sys.exit(2)
