#!/usr/bin/env bash
set -euo pipefail

readonly EXPECTED_OUTPUT="/home/roomtv/works/alopex-db/release-artifacts/chirps-v0.7.0/local-state-corpora"
readonly BUDGET_GATE="/home/roomtv/works/alopex-db/scripts/check-rust-cache-budget.sh"
readonly CLEANUP_GATE="/home/roomtv/works/alopex-db/scripts/cleanup-generated-artifacts.sh"
readonly TARGET_DIR="/tmp/chirps-v07-task-4_5_1-target"

usage() {
    echo "usage: $0 --output $EXPECTED_OUTPUT --freeze --verify" >&2
}

output=""
freeze=0
verify=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --output)
            output="${2:?missing output directory}"
            shift 2
            ;;
        --freeze)
            freeze=1
            shift
            ;;
        --verify)
            verify=1
            shift
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            usage
            exit 64
            ;;
    esac
done

if [[ "$output" != "$EXPECTED_OUTPUT" || $freeze -ne 1 || $verify -ne 1 ]]; then
    usage
    exit 64
fi
if [[ -L "$output" || ( -e "$output" && ! -d "$output" ) ]]; then
    echo "corpus: output must be a non-symlink directory or absent: $output" >&2
    exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repo_parent="$(dirname "$repo_root")"
requirements="$repo_root/../../.spec-workflow/specs/chirps-v0-7-durable-backend/requirements.md"
design="$repo_root/../../.spec-workflow/specs/chirps-v0-7-durable-backend/design.md"
shadow="$(rtk mktemp -d "$repo_parent/.chirps-v07-corpus.XXXXXX")"
staging=""
repeat_staging=""
published_output=0
cleaned=0

cleanup() {
    local status=$?
    if [[ $cleaned -eq 0 && -d "$TARGET_DIR" && ! -L "$TARGET_DIR" ]]; then
        (cd "$repo_root" && rtk cargo clean --target-dir "$TARGET_DIR") || status=1
    fi
    if [[ -d "$shadow" && ! -L "$shadow" ]]; then
        rtk rm -rf -- "$shadow" || status=1
    fi
    if [[ -n "$staging" && -d "$staging" && ! -L "$staging" ]]; then
        rtk rm -rf -- "$staging" || status=1
    fi
    if [[ -n "$repeat_staging" && -d "$repeat_staging" && ! -L "$repeat_staging" ]]; then
        rtk rm -rf -- "$repeat_staging" || status=1
    fi
    if [[ $status -ne 0 && $published_output -eq 1 && -d "$output" && ! -L "$output" ]]; then
        rtk rm -rf -- "$output" || status=1
        rtk sync -f "$(dirname "$output")" || status=1
    fi
    exit "$status"
}
trap cleanup EXIT

rtk bash "$BUDGET_GATE" --check
if [[ -e "$TARGET_DIR" ]]; then
    echo "corpus: task target already exists: $TARGET_DIR" >&2
    exit 2
fi
rtk rsync -a \
    --exclude '/.git/' \
    --exclude '/target/' \
    --exclude '/.hook-target/' \
    "$repo_root/" "$shadow/"
rtk tee -a "$shadow/crates/chirps-backend-iggy/src/lib.rs" >/dev/null <<'RUST'

#[cfg(test)]
mod final_corpus_runner;
RUST
rtk tee -a "$shadow/crates/chirps-backend-iggy/src/state/creation.rs" >/dev/null <<'RUST'

#[cfg(test)]
fn final_corpus_checkpoint_directory_id(
    directory: &Path,
) -> Result<CheckpointDirectoryId, CreationStoreError> {
    let physical_root = PathBuf::from(
        std::env::var("CHIRPS_FINAL_CORPUS_PHYSICAL_ROOT")
            .map_err(|_| CreationStoreError::CorruptState)?,
    );
    let canonical_root = PathBuf::from(
        std::env::var("CHIRPS_FINAL_CORPUS_CANONICAL_ROOT")
            .map_err(|_| CreationStoreError::CorruptState)?,
    );
    let Ok(relative) = directory.strip_prefix(&physical_root) else {
        return checkpoint_directory_id(directory);
    };
    let bound = canonical_root.join(relative);
    let encoded = bound.as_os_str().as_encoded_bytes();
    Ok(CheckpointDirectoryId::from_bytes(digest_parts(&[
        DIRECTORY_ID_DOMAIN,
        encoded,
    ])))
}

#[cfg(test)]
fn final_creation_corpus_manifest(
    case_bytes: &BTreeMap<&'static str, Vec<u8>>,
    inputs: &CorpusInputs<'_>,
) -> Result<String, CreationStoreError> {
    let template = case_bytes
        .get("directory-sync-new")
        .ok_or(CreationStoreError::CorruptState)?;
    let materialization = format!(
        concat!(
            "  ],\n",
            "  \"materialization\": {{\n",
            "    \"template\": \"directory-sync-new/creation.unit\",\n",
            "    \"template_sha256\": \"{}\",\n",
            "    \"dynamic_fields\": [\n",
            "      \"checkpoint_directory_id\",\n",
            "      \"subscription_id\",\n",
            "      \"target\",\n",
            "      \"generation\",\n",
            "      \"partition\",\n",
            "      \"lifecycle_generation\",\n",
            "      \"namespace_digest\",\n",
            "      \"initial_position\",\n",
            "      \"resolved_initial_offset\",\n",
            "      \"captured_end_exclusive\",\n",
            "      \"captured_oldest_available\",\n",
            "      \"resource_id\",\n",
            "      \"resource_epoch\",\n",
            "      \"genesis_owner_id\"\n",
            "    ],\n",
            "    \"canonical_journal\": \"checkpoint.journal\"\n",
            "  }},\n",
            "  \"cases\": [\n",
        ),
        hex(&digest(template)),
    );
    let manifest = corpus_manifest(case_bytes, inputs)?
        .replacen("  \"schema_version\": 1,", "  \"schema_version\": 2,", 1)
        .replacen("  ],\n  \"cases\": [\n", &materialization, 1);
    if !manifest.contains("  \"schema_version\": 2,")
        || !manifest.contains("  \"materialization\": {")
    {
        return Err(CreationStoreError::CorruptState);
    }
    Ok(manifest)
}
RUST
rtk sed -i \
    -e 's/match checkpoint_directory_id(directory) {/match final_corpus_checkpoint_directory_id(directory) {/' \
    -e 's/checkpoint_directory_id(directory)?/final_corpus_checkpoint_directory_id(directory)?/g' \
    -e 's/corpus_manifest(&case_bytes, inputs)/final_creation_corpus_manifest(\&case_bytes, inputs)/g' \
    "$shadow/crates/chirps-backend-iggy/src/state/creation.rs"
if [[ "$(rtk grep -c 'final_corpus_checkpoint_directory_id(directory)' \
    "$shadow/crates/chirps-backend-iggy/src/state/creation.rs")" -ne 4 ]]; then
    echo "corpus: failed to install the shadow-only final-path binding" >&2
    exit 2
fi
if [[ "$(rtk grep -c 'final_creation_corpus_manifest(&case_bytes, inputs)' \
    "$shadow/crates/chirps-backend-iggy/src/state/creation.rs")" -ne 2 ]]; then
    echo "corpus: failed to install the schema-v2 creation manifest" >&2
    exit 2
fi
rtk tee "$shadow/crates/chirps-backend-iggy/src/final_corpus_runner.rs" >/dev/null <<'RUST'
use crate::state::compaction::{
    CompactionCorpusInputs, generate_compaction_corpus, verify_compaction_corpus,
};
use crate::state::creation::{
    ActiveSubscription, CorpusInputs, CreationNamespace, CreationRequest, CreationStore,
    CreationStoreResult, generate_creation_corpus, verify_creation_corpus,
};
use crate::state::journal::{
    JournalCorpusInputs, generate_journal_corpus, verify_journal_corpus,
};
use alopex_chirps_core::durable::{
    InitialPosition, PollObservation, ResourceEpoch, ResourceId, SubscriptionId,
};
use alopex_chirps_wire::node_id::NodeId;
use std::fs;
use std::path::{Path, PathBuf};

const REQUIREMENTS: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../../.spec-workflow/specs/chirps-v0-7-durable-backend/requirements.md"
));
const DESIGN: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../../.spec-workflow/specs/chirps-v0-7-durable-backend/design.md"
));
const SOURCES: &[(&str, &[u8])] = &[
    ("codec.rs", include_bytes!(concat!(env!("CHIRPS_SOURCE_ROOT"), "/crates/chirps-backend-iggy/src/codec.rs"))),
    ("delivery.rs", include_bytes!(concat!(env!("CHIRPS_SOURCE_ROOT"), "/crates/chirps-backend-iggy/src/delivery.rs"))),
    ("lib.rs", include_bytes!(concat!(env!("CHIRPS_SOURCE_ROOT"), "/crates/chirps-backend-iggy/src/lib.rs"))),
    ("lifecycle.rs", include_bytes!(concat!(env!("CHIRPS_SOURCE_ROOT"), "/crates/chirps-backend-iggy/src/lifecycle.rs"))),
    ("message_id.rs", include_bytes!(concat!(env!("CHIRPS_SOURCE_ROOT"), "/crates/chirps-backend-iggy/src/message_id.rs"))),
    ("observability.rs", include_bytes!(concat!(env!("CHIRPS_SOURCE_ROOT"), "/crates/chirps-backend-iggy/src/observability.rs"))),
    ("offset_mirror.rs", include_bytes!(concat!(env!("CHIRPS_SOURCE_ROOT"), "/crates/chirps-backend-iggy/src/offset_mirror.rs"))),
    ("poll.rs", include_bytes!(concat!(env!("CHIRPS_SOURCE_ROOT"), "/crates/chirps-backend-iggy/src/poll.rs"))),
    ("producer.rs", include_bytes!(concat!(env!("CHIRPS_SOURCE_ROOT"), "/crates/chirps-backend-iggy/src/producer.rs"))),
    ("protocol.rs", include_bytes!(concat!(env!("CHIRPS_SOURCE_ROOT"), "/crates/chirps-backend-iggy/src/protocol.rs"))),
    ("routing.rs", include_bytes!(concat!(env!("CHIRPS_SOURCE_ROOT"), "/crates/chirps-backend-iggy/src/routing.rs"))),
    ("runtime.rs", include_bytes!(concat!(env!("CHIRPS_SOURCE_ROOT"), "/crates/chirps-backend-iggy/src/runtime.rs"))),
    ("session.rs", include_bytes!(concat!(env!("CHIRPS_SOURCE_ROOT"), "/crates/chirps-backend-iggy/src/session.rs"))),
    ("state/capacity.rs", include_bytes!(concat!(env!("CHIRPS_SOURCE_ROOT"), "/crates/chirps-backend-iggy/src/state/capacity.rs"))),
    ("state/compaction.rs", include_bytes!(concat!(env!("CHIRPS_SOURCE_ROOT"), "/crates/chirps-backend-iggy/src/state/compaction.rs"))),
    ("state/creation.rs", include_bytes!(concat!(env!("CHIRPS_SOURCE_ROOT"), "/crates/chirps-backend-iggy/src/state/creation.rs"))),
    ("state/identity.rs", include_bytes!(concat!(env!("CHIRPS_SOURCE_ROOT"), "/crates/chirps-backend-iggy/src/state/identity.rs"))),
    ("state/journal.rs", include_bytes!(concat!(env!("CHIRPS_SOURCE_ROOT"), "/crates/chirps-backend-iggy/src/state/journal.rs"))),
    ("state/mod.rs", include_bytes!(concat!(env!("CHIRPS_SOURCE_ROOT"), "/crates/chirps-backend-iggy/src/state/mod.rs"))),
    ("state/owner.rs", include_bytes!(concat!(env!("CHIRPS_SOURCE_ROOT"), "/crates/chirps-backend-iggy/src/state/owner.rs"))),
    ("subscriber.rs", include_bytes!(concat!(env!("CHIRPS_SOURCE_ROOT"), "/crates/chirps-backend-iggy/src/subscriber.rs"))),
    ("transport.rs", include_bytes!(concat!(env!("CHIRPS_SOURCE_ROOT"), "/crates/chirps-backend-iggy/src/transport.rs"))),
];

fn namespace() -> CreationNamespace {
    let mut target = [0x21; 16];
    target[6] = 0x41;
    target[8] = 0x81;
    CreationNamespace::new(
        SubscriptionId::from_bytes([0x31; 16]),
        NodeId::from(target),
        7,
        3,
        11,
        [0x41; 32],
    )
}

fn resource_epoch() -> ResourceEpoch {
    let mut resource = [0x51; 16];
    resource[6] = 0x41;
    resource[8] = 0x91;
    ResourceEpoch::new(ResourceId::from_bytes(resource), 9)
}

fn creation_template(work: &Path) -> Box<ActiveSubscription> {
    if work.exists() {
        fs::remove_dir_all(work).unwrap();
    }
    fs::create_dir_all(work).unwrap();
    let observation = PollObservation::try_new(resource_epoch(), 10, 4, None).unwrap();
    let result = CreationStore
        .create(
            &work.join("source"),
            CreationRequest::new(namespace(), InitialPosition::Exact(6)),
            &observation,
        )
        .unwrap();
    let CreationStoreResult::Created(active) = result else {
        panic!("expected active creation template")
    };
    active
}

fn generate_all(root: &Path, template_work: &Path) {
    fs::create_dir_all(root).unwrap();
    let active = creation_template(template_work);
    let creation_inputs = CorpusInputs {
        requirements: REQUIREMENTS,
        design: DESIGN,
        sources: SOURCES,
    };
    let journal_inputs = JournalCorpusInputs {
        requirements: REQUIREMENTS,
        design: DESIGN,
        sources: SOURCES,
    };
    let compaction_inputs = CompactionCorpusInputs {
        requirements: REQUIREMENTS,
        design: DESIGN,
        sources: SOURCES,
    };
    generate_creation_corpus(&root.join("task-4_1"), active.creation(), &creation_inputs).unwrap();
    generate_journal_corpus(&root.join("task-4_2"), &journal_inputs).unwrap();
    generate_compaction_corpus(&root.join("task-4_3"), &compaction_inputs).unwrap();
}

fn verify_all_with_sources(root: &Path, template_work: &Path, sources: &[(&str, &[u8])]) {
    let active = creation_template(template_work);
    verify_creation_corpus(
        &root.join("task-4_1"),
        active.creation(),
        &CorpusInputs {
            requirements: REQUIREMENTS,
            design: DESIGN,
            sources,
        },
    )
    .unwrap();
    verify_journal_corpus(
        &root.join("task-4_2"),
        &JournalCorpusInputs {
            requirements: REQUIREMENTS,
            design: DESIGN,
            sources,
        },
    )
    .unwrap();
    verify_compaction_corpus(
        &root.join("task-4_3"),
        &CompactionCorpusInputs {
            requirements: REQUIREMENTS,
            design: DESIGN,
            sources,
        },
    )
    .unwrap();
}

fn verify_all(root: &Path, template_work: &Path) {
    verify_all_with_sources(root, template_work, SOURCES);
}

#[test]
fn v07_task_4_5_1_final_corpora_are_single_projection_atomic_and_fail_closed() {
    let output = PathBuf::from(std::env::var("CHIRPS_FINAL_CORPUS_OUTPUT").unwrap());
    let target = PathBuf::from(std::env::var("CARGO_TARGET_DIR").unwrap());
    let action = std::env::var("CHIRPS_FINAL_CORPUS_ACTION").unwrap();
    let template_work = target.join(format!("task-4_5_1-template-{action}"));

    match action.as_str() {
        "red" => {
            if output.exists() {
                fs::remove_dir_all(&output).unwrap();
            }
            generate_all(&output, &template_work);
            verify_all(&output, &template_work);

            let mut stale_sources = SOURCES.to_vec();
            stale_sources[0] = ("codec.rs", b"stale-source-projection");
            let stale = std::panic::catch_unwind(|| {
                verify_all_with_sources(&output, &template_work, &stale_sources);
            });
            assert!(stale.is_err(), "stale source projection was accepted");

            let journal_manifest = output.join("task-4_2/manifest.json");
            let compaction_manifest = output.join("task-4_3/manifest.json");
            let journal_bytes = fs::read(&journal_manifest).unwrap();
            let compaction_bytes = fs::read(&compaction_manifest).unwrap();
            fs::write(&journal_manifest, &compaction_bytes).unwrap();
            assert!(
                verify_journal_corpus(
                    &output.join("task-4_2"),
                    &JournalCorpusInputs {
                        requirements: REQUIREMENTS,
                        design: DESIGN,
                        sources: SOURCES,
                    },
                )
                .is_err(),
                "mixed task manifest was accepted"
            );
            fs::write(&journal_manifest, &journal_bytes).unwrap();

            fs::remove_dir_all(output.join("task-4_3/base-write-old")).unwrap();
            assert!(
                verify_compaction_corpus(
                    &output.join("task-4_3"),
                    &CompactionCorpusInputs {
                        requirements: REQUIREMENTS,
                        design: DESIGN,
                        sources: SOURCES,
                    },
                )
                .is_err(),
                "partial corpus directory was accepted"
            );
            fs::remove_dir_all(&output).unwrap();
            generate_all(&output, &template_work);
            verify_all(&output, &template_work);
        }
        "generate" => {
            generate_all(&output, &template_work);
            verify_all(&output, &template_work);
        }
        "verify" => verify_all(&output, &template_work),
        other => panic!("unsupported corpus action: {other}"),
    }
}
RUST

run_harness() {
    local action="$1"
    local destination="$2"
    (cd "$shadow" && rtk env \
        CARGO_TARGET_DIR="$TARGET_DIR" \
        CHIRPS_SOURCE_ROOT="$repo_root" \
        CHIRPS_FINAL_CORPUS_PHYSICAL_ROOT="$destination" \
        CHIRPS_FINAL_CORPUS_CANONICAL_ROOT="$output" \
        CHIRPS_FINAL_CORPUS_ACTION="$action" \
        CHIRPS_FINAL_CORPUS_OUTPUT="$destination" \
        cargo test --locked -p alopex-chirps-backend-iggy v07_task_4_5_1 -- --test-threads=1)
}

if [[ -e "$output" ]]; then
    if [[ ! -f "$output/freeze.json" ]]; then
        echo "corpus: existing root is partial or was not frozen: $output" >&2
        exit 2
    fi
    root_entries="$(rtk proxy find "$output" -mindepth 1 -maxdepth 1 -printf . | rtk wc -c)"
    if [[ "$root_entries" -ne 4 \
        || -L "$output/freeze.json" \
        || ! -f "$output/freeze.json" \
        || -L "$output/task-4_1" \
        || ! -d "$output/task-4_1" \
        || -L "$output/task-4_2" \
        || ! -d "$output/task-4_2" \
        || -L "$output/task-4_3" \
        || ! -d "$output/task-4_3" ]]; then
        echo "corpus: frozen root inventory is invalid or tampered" >&2
        exit 2
    fi
    run_harness verify "$output"
    corpus_root="$output"
else
    output_parent="$(dirname "$output")"
    rtk mkdir -p -- "$output_parent"
    staging="$(rtk mktemp -d "$output_parent/.local-state-corpora.pending.XXXXXX")"
    run_harness red "$staging"
    corpus_root="$staging"
fi

output_parent="$(dirname "$output")"
repeat_staging="$(rtk mktemp -d "$output_parent/.local-state-corpora.repeat.XXXXXX")"
run_harness generate "$repeat_staging"
for task in task-4_1 task-4_2 task-4_3; do
    if ! rtk proxy diff --no-dereference --recursive --brief \
        "$corpus_root/$task" "$repeat_staging/$task"; then
        echo "corpus: repeated generation was not byte-identical for $task" >&2
        exit 2
    fi
done
rtk rm -rf -- "$repeat_staging"
repeat_staging=""

requirements_sha="$(rtk sha256sum "$requirements" | rtk awk '{print $1}')"
design_sha="$(rtk sha256sum "$design" | rtk awk '{print $1}')"
source_catalog=""
source_input_4_1=""
source_input_4_2=""
source_input_4_3=""
for task in 4_1 4_2 4_3; do
    manifest="$corpus_root/task-$task/manifest.json"
    producer="${task/_/.}"
    schema_version=1
    if [[ "$task" == "4_1" ]]; then
        schema_version=2
    fi
    rtk proxy jq -e \
        --arg producer "$producer" \
        --arg requirements "$requirements_sha" \
        --arg design "$design_sha" \
        --argjson schema_version "$schema_version" \
        '.schema_version == $schema_version
         and .producer_task == $producer
         and .requirements_sha256 == $requirements
         and .design_sha256 == $design
         and (.sources | length == 22)
         and (.cases | length > 0)' \
        "$manifest" >/dev/null
    current_catalog="$(rtk proxy jq -cS '.sources' "$manifest")"
    if [[ -z "$source_catalog" ]]; then
        source_catalog="$current_catalog"
    elif [[ "$source_catalog" != "$current_catalog" ]]; then
        echo "corpus: generators did not use one exact source catalog" >&2
        exit 2
    fi
    current_source="$(rtk proxy jq -r '.source_input_sha256' "$manifest")"
    case "$task" in
        4_1) source_input_4_1="$current_source" ;;
        4_2) source_input_4_2="$current_source" ;;
        4_3) source_input_4_3="$current_source" ;;
    esac
done
source_projection_sha="$(rtk proxy jq -cS '.sources' "$corpus_root/task-4_1/manifest.json" \
    | rtk sha256sum | rtk awk '{print $1}')"

manifest_4_1_sha="$(rtk sha256sum "$corpus_root/task-4_1/manifest.json" | rtk awk '{print $1}')"
manifest_4_2_sha="$(rtk sha256sum "$corpus_root/task-4_2/manifest.json" | rtk awk '{print $1}')"
manifest_4_3_sha="$(rtk sha256sum "$corpus_root/task-4_3/manifest.json" | rtk awk '{print $1}')"
freeze_pending="$TARGET_DIR/freeze.json.pending"
rtk proxy jq -n \
    --arg requirements_sha256 "$requirements_sha" \
    --arg design_sha256 "$design_sha" \
    --arg source_projection_sha256 "$source_projection_sha" \
    --arg source_input_4_1_sha256 "$source_input_4_1" \
    --arg source_input_4_2_sha256 "$source_input_4_2" \
    --arg source_input_4_3_sha256 "$source_input_4_3" \
    --arg task_4_1_manifest_sha256 "$manifest_4_1_sha" \
    --arg task_4_2_manifest_sha256 "$manifest_4_2_sha" \
    --arg task_4_3_manifest_sha256 "$manifest_4_3_sha" \
    '{schema_version: 1,
      producer_task: "4.5.1",
      requirements_sha256: $requirements_sha256,
      design_sha256: $design_sha256,
      source_projection_sha256: $source_projection_sha256,
      corpora: [
        {task: "4.1", path: "task-4_1", source_input_sha256: $source_input_4_1_sha256, manifest_sha256: $task_4_1_manifest_sha256},
        {task: "4.2", path: "task-4_2", source_input_sha256: $source_input_4_2_sha256, manifest_sha256: $task_4_2_manifest_sha256},
        {task: "4.3", path: "task-4_3", source_input_sha256: $source_input_4_3_sha256, manifest_sha256: $task_4_3_manifest_sha256}
      ]}' >"$freeze_pending"
rtk sync -f "$freeze_pending"

if [[ "$corpus_root" == "$output" ]]; then
    if ! rtk cmp -s -- "$freeze_pending" "$output/freeze.json"; then
        echo "corpus: frozen aggregate manifest is stale or tampered" >&2
        exit 2
    fi
    rtk rm -- "$freeze_pending"
else
    rtk mv -- "$freeze_pending" "$corpus_root/freeze.json"
    rtk sync -f "$corpus_root"
    rtk mv -T -- "$corpus_root" "$output"
    staging=""
    published_output=1
    rtk sync -f "$(dirname "$output")"
fi

rtk cargo clean --target-dir "$TARGET_DIR"
cleaned=1
rtk rm -rf -- "$shadow"
shadow=""
rtk bash "$CLEANUP_GATE" --force
rtk bash "$BUDGET_GATE" --check
echo "corpus: verified source projection $source_projection_sha"
