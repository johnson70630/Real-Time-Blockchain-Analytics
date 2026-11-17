# Milestone 11.6C — First runtime window aborted before ingestion

Follow-up: [the approved repeat reached the endpoint-format gate and was stopped; whole-payload verification/correction is now under review](aws_runtime_format_gate_result.md). The unknown generic failure below preserves the first window's history.

**The approved instance was started, the host/image checks passed, and the
private chain preflight failed. The application was never started. EC2 was
normally stopped and independently verified. The underlying failure is unknown.**

This is a failed readiness gate, not a successful streaming demonstration.
Milestone 11.6C remains open; 11.5D remains paused. The completed start permission
does not authorize another start or a retry.

## A. What was implemented and observed

On 2026-11-17, the owner approved the exact [supervised proposal](aws_runtime_demo_readiness.md)
and started only `i-0ca1671a59751acc5` in Oregon. Source checkpoint was
`19f1d2052486de135337ff0a3339ab1e9f61525e`. The independent watcher observed
launch at **22:42:54 UTC** and confirmed stopped at **22:47:36 UTC**.

Live successes:

- Fresh four-hour MFA session and expected deployer identity; exact encrypted
  disks, no ingress, standard credits, shutdown-to-stop and normal-stop DryRun.
- Unchanged workload trust/profile, boundary v2, exact secret grant, ECR
  manifest/config identities and complete scan finding inventories.
- Kafka XFS mount survived reboot and matched the exact volume/UUID. Docker,
  Compose version/hash, free space and initial empty container inventory passed.
- Installed the nine verified frozen host files, exact release and legacy
  operator check without overwrites. Installed three reviewed systemd definitions
  and manually started the emergency timer; no automatic pilot enablement.
- Pilot S3 prefix was empty at preflight. All three accepted images were pulled
  and their architecture/config/digest identities checked before the chain phase.

The private chain-check command then returned nonzero. The nested helpers
reported only `RuntimeError / COMMAND_FAILED`. They did not retain a safe
underlying failure category. Secret retrieval success, WSS handshake and actual
chain ID are **not established**. There was no automatic retry.

The service remained inactive, no Spark status file existed, and no Kafka,
Spark or producer service containers were found. The status action enumerated
those three services, not the complete transient-validator inventory. Earlier
conversational shorthand saying no containers remained was broader than this
observation. Verified EC2 stop ended any remaining guest process.

## B. Why this behavior matters

Failing before application startup prevents unverified source data from reaching
Bronze or creating a checkpoint. The bounded abort limits paid idle time and
preserves storage for investigation. A safe failure code is also necessary for
fault isolation: IAM denial, secret format, Docker startup, TLS, timeout and
RPC errors require different repairs. Do not assume this was an Alchemy key,
network or IAM problem without evidence.

## C. Files changed and preserved artifacts

- [Operator check](../scripts/check_runtime_chain.py): allowlisted stage/error
  diagnostics and a complete-line parser for outer operators.
- [Tests](../tests/test_runtime_chain_preflight.py): redaction and failure-stage
  coverage, including malicious/extra provider output and partial timeout data.
- This report, [sanitized evidence](evidence/aws_runtime_demo_abort_2026-11-17.json),
  and current-status links in the pilot/proposal.

Original approval, metadata checks, transfer/install results, generic failure,
watcher and stopped readback remain private. Their hashes are in the evidence;
the original generic failure was not rewritten as a known cause. Host bundle
SHA remains `669a806f2370cc33c43296b769b9e8eddb39d939eaeb624591f637782242a627`;
release SHA remains `7f948e94cec7f7aebaf2aa4dab4ddd8e92a60f083f17bb9cefcff4db89cd200f`.
Frozen application images and all nine host files are unchanged. Preserve the
installed legacy check; the updated check has not been deployed.

## D. Tests and validation

Before the window, **28 offline operator checks** passed for stop scope,
artifact/path guards and dummy Parquet validation. The revised tracked source
passes **96 focused tests and 28 negative subtests**, including 25 chain-preflight
tests. Ruff and whitespace pass. One existing dependency deprecation warning
remains. Run the secret scan, evidence/link/hash review and new CI before checkpoint.

New tests verify that known AWS error codes survive without provider text,
client diagnostics survive the Docker boundary, unknown/extra output is
rejected, HTTP/TLS/timeouts remain private, and wrong-chain/RPC/format failures
stay distinct. They do not establish live secret or RPC access.

An offline local-container check was unavailable because the accepted producer
digest/config was absent from the local image store; no image pull was attempted.
Prospective dispatch code compiles, binds the exact updated check, preserves the
legacy installation, parses only the fixed failure protocol, and keeps terminal
input lines below 4,096 bytes. None of that code was executed on EC2.

## E. Security and correctness review

No URL, key, credential, raw provider error or secret value was returned to the
assistant. The attempted validator used stdin, non-root execution, no Docker
logging, normal TLS verification, read-only storage and bounded resources.
Its secret retrieval result is unknown. Operator registry-auth cleanup ran in
the abort path; OS stop cleared tmpfs/process memory.

New diagnostics emit only hardcoded stages and allowlisted codes. They never
forward exception messages, response bodies, headers, endpoints or partial
timeout output. Unknown values become fixed generic categories. The outer
operator must parse the complete line rather than discard it or echo raw output.
The fix improves observability; it does **not** repair or prove the live cause.
All existing instance/release/image/region/secret guards and success criteria
remain enforced. No additional privileges, retries or TLS bypass were added.

No application controller invocation, Kafka storage initialization, pipeline
container startup, Bronze/checkpoint write, Silver/Gold promotion or live
recovery test occurred. The original conditional image-risk acceptance remains
limited to one supervised streaming run before **2026-11-24 00:00 UTC**; zero
streaming runs have occurred. It is not permission for another paid boot.

## F. Git and CI

Checkpoint these reviewed source/tests/report files on develop and observe all
six automatic CI jobs. Report the resulting SHA/run in conversation. Execution
source `19f1d20` passed all six jobs in
[CI 44185994807](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/44185994807).
README, Terraform, frozen runtime source, dependency/image locks and main are
unchanged. Preserve the 67 unrelated untracked duplicate files.

## G. Cost implications

The conservative observed interval was **282.26 seconds**. At the same-day
verified combined EC2/IPv4 rate of USD 0.1714/hour, this is approximately
**USD 0.01344**, before requests, other usage and taxes; not an invoice or cap.
No new AWS resource was created. Retained EBS, secret, ECR, alarms and other
existing resources continue billing. See the public rate evidence in the
[previous readiness record](evidence/aws_runtime_network_readiness_2026-11-17.json).

A separately approved repeat of the bounded window would add up to approximately
USD 0.43 EC2/IPv4 for 150 minutes, plus usage/retention/taxes. Finish early after
smoke acceptance or a failed gate; do not keep EC2 running to fill that window.

## H. Remaining blockers

Determine the actual failing stage with the revised safe diagnostic. Live
chain verification, ingestion/S3 smoke, drain/checkpoint evidence and Kafka/Spark
recovery acceptance remain incomplete. Do not guess a repair, edit the secret,
expand IAM, reformat storage or restart production from a passing offline test.

## I. Exact proposed next attempt and rollback

**Not approved:** one new start of the same instance, same account/region and
existing disks, with the same one-controller/3,600-second/150-minute limits and
independent stop controls from the reviewed proposal. Set absolute UTC times
and require at least three hours of valid MFA credentials before arming.

Reverify current IAM/image scans and existing installation. Preserve all frozen
host files, the release and `/opt/blockchain-runtime/check_runtime_chain.py`.
Do not rerun installation or the legacy `prepare` action. After an approved boot,
transfer only the revised check to `/run/blockchain-runtime/check_runtime_chain-v2.py`,
verify its external SHA-256
`8c64cd485ca7ab2dce78c343fe4699fd27d7a9f47d59446589211f801622e114`,
and use the prepared dispatcher that preserves its allowlisted diagnostic.
The dispatcher checks the existing mount/software, empty pilot prefix and
cached image identities, starts/verifies the unchanged emergency timer and
invokes the check at most once. It never changes the secret or IAM.

On a failed check, retain its safe stage/code, close SSM and normally stop this
exact instance without retry. Only if all gates pass may the same new approval
cover the first application invocation and isolated S3/log writes, followed by
the already reviewed smoke/drain/normal-stop procedure. Prefer early shutdown
after smoke evidence. Failure-injection recovery remains outside that scope.

The [evidence](evidence/aws_runtime_demo_abort_2026-11-17.json) binds the prepared
dispatch/relay hashes. No Terraform initialization, plan/apply/import, new cloud
resources, image publication, IAM expansion, secret edit, filesystem format,
destructive cleanup, Snowflake change or main merge is proposed. Preserve both
disks, Kafka identity and checkpoints. Existing installed software can remain
on the stopped node; any removal or rollback is a separately reviewed action.

## J. Whether production infrastructure changed

**Yes, within the approved failed window:** one existing EC2 start/normal stop,
guest installation, manual timer activation and reviewed image downloads to its
existing root disk. Final readback at **22:48:44 UTC** confirmed stopped, no public
IPv4, both encrypted gp3 disks attached with the same IDs and Kafka
delete-on-termination false. No new AWS resource, Terraform state/backend
operation, IAM/secret edit, EBS format, platform/Snowflake change or main merge.
No fresh Terraform inventory recount was performed. Milestone 11.5D stays paused.
