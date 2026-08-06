---------------------- MODULE model ----------------------
EXTENDS Naturals

CONSTANT
    \* @type: Str;
    UnsafeMode

Versions == {"0.5.2", "0.6.0"}
Contracts == {"docs/release/v0.5.2.md", "docs/release/v0.6.0.md"}
Commits == {"target", "stale"}
Checks == {"unknown", "pass", "fail"}
EvidenceStates == {"missing", "complete"}
Phases == {"idle", "selected", "contract_loaded", "manifest_loaded",
           "gate_checked", "verified", "rejected", "published"}
UnsafeModes == {"none", "digest"}

ContractFor(v) ==
    IF v = "0.6.0"
    THEN "docs/release/v0.6.0.md"
    ELSE "docs/release/v0.5.2.md"

VARIABLES
    \* @type: Str;
    phase,
    \* @type: Str;
    targetVersion,
    \* @type: Str;
    contract,
    \* @type: Str;
    manifestVersion,
    \* @type: Str;
    manifestCommit,
    \* @type: Str;
    digestCheck,
    \* @type: Str;
    requiredEvidence,
    \* @type: Str;
    targetGate,
    \* @type: Bool;
    published

vars == << phase, targetVersion, contract, manifestVersion, manifestCommit,
           digestCheck, requiredEvidence, targetGate, published >>

Init ==
    /\ phase = "idle"
    /\ targetVersion = "0.5.2"
    /\ contract = ContractFor("0.5.2")
    /\ manifestVersion = "0.5.2"
    /\ manifestCommit = "stale"
    /\ digestCheck = "unknown"
    /\ requiredEvidence = "missing"
    /\ targetGate = "unknown"
    /\ published = FALSE

SelectVersion(v) ==
    /\ phase = "idle"
    /\ phase' = "selected"
    /\ targetVersion' = v
    /\ UNCHANGED << contract, manifestVersion, manifestCommit, digestCheck,
                    requiredEvidence, targetGate, published >>

LoadContract(c) ==
    /\ phase = "selected"
    /\ phase' = "contract_loaded"
    /\ contract' = c
    /\ UNCHANGED << targetVersion, manifestVersion, manifestCommit,
                    digestCheck, requiredEvidence, targetGate, published >>

LoadManifest(v, commit, digest, evidence) ==
    /\ phase = "contract_loaded"
    /\ phase' = "manifest_loaded"
    /\ manifestVersion' = v
    /\ manifestCommit' = commit
    /\ digestCheck' = digest
    /\ requiredEvidence' = evidence
    /\ UNCHANGED << targetVersion, contract, targetGate, published >>

RunTargetGate(result) ==
    /\ phase = "manifest_loaded"
    /\ phase' = "gate_checked"
    /\ targetGate' = result
    /\ UNCHANGED << targetVersion, contract, manifestVersion, manifestCommit,
                    digestCheck, requiredEvidence, published >>

ValidationAccepts ==
    /\ contract = ContractFor(targetVersion)
    /\ manifestVersion = targetVersion
    /\ manifestCommit = "target"
    /\ requiredEvidence = "complete"
    /\ targetGate = "pass"
    /\ (digestCheck = "pass" \/ UnsafeMode = "digest")

Validate ==
    /\ phase = "gate_checked"
    /\ phase' = IF ValidationAccepts THEN "verified" ELSE "rejected"
    /\ UNCHANGED << targetVersion, contract, manifestVersion, manifestCommit,
                    digestCheck, requiredEvidence, targetGate, published >>

Publish ==
    /\ phase = "verified"
    /\ phase' = "published"
    /\ published' = TRUE
    /\ UNCHANGED << targetVersion, contract, manifestVersion, manifestCommit,
                    digestCheck, requiredEvidence, targetGate >>

Stutter == UNCHANGED vars

Next ==
    (\E v \in Versions: SelectVersion(v)) \/
    (\E c \in Contracts: LoadContract(c)) \/
    (\E v \in Versions, commit \in Commits, digest \in Checks,
        evidence \in EvidenceStates:
        LoadManifest(v, commit, digest, evidence)) \/
    (\E result \in Checks: RunTargetGate(result)) \/
    Validate \/ Publish \/ Stutter

TypeOK ==
    /\ UnsafeMode \in UnsafeModes
    /\ phase \in Phases
    /\ targetVersion \in Versions
    /\ contract \in Contracts
    /\ manifestVersion \in Versions
    /\ manifestCommit \in Commits
    /\ digestCheck \in Checks
    /\ requiredEvidence \in EvidenceStates
    /\ targetGate \in Checks
    /\ published \in BOOLEAN

VerifiedUsesTargetContract ==
    phase \in {"verified", "published"} => contract = ContractFor(targetVersion)

VerifiedUsesTargetVersionEvidence ==
    phase \in {"verified", "published"} => manifestVersion = targetVersion

VerifiedUsesTargetCommit ==
    phase \in {"verified", "published"} => manifestCommit = "target"

VerifiedUsesValidDigests ==
    phase \in {"verified", "published"} => digestCheck = "pass"

VerifiedHasAllRequiredEvidence ==
    phase \in {"verified", "published"} => requiredEvidence = "complete"

VerifiedPassedTargetGate ==
    phase \in {"verified", "published"} => targetGate = "pass"

PublishOnlyAfterVerification ==
    published => phase = "published"

RejectedNeverPublishes ==
    phase = "rejected" => ~published

==========================================================
