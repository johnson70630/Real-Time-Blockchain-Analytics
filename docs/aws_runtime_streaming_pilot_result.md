# Milestone 11.6C — First AWS streaming pilot accepted and stopped

**Alchemy → producer → Kafka → Spark → isolated S3 Bronze/checkpoints passed the
first controlled AWS pilot.** The existing secret was unchanged. EC2 is stopped;
the single-demonstration security exception is consumed. No further launch is
approved. Times below are UTC on November 19, 2026 (November 18 in Los Angeles).

## A. What was implemented and observed

Executed the explicitly approved replacement window from develop
`575ca232d98da158a70b292aa48a3ab09e825577`, using operator archive
`62cd07b38cddd94f8c213b33cb773dacecad33be8ad512697b10a222e8821e27`.
There was one manual EC2 start, one short chain-validation container and one
streaming controller attempt. The corrected terminal reader received the actual
controls response successfully. The guest payload matched the reviewed prior
package; its source-commit manifest changed.

| Event | UTC |
| --- | --- |
| Actual EC2 launch | 01:17:03 |
| Graceful application shutdown | After sample acceptance; before the +20-minute deadline |
| Relay normal-stop request returned `stopping` | 01:23:20.934175 |
| Independent watcher completed `stopped` | 01:23:50.766950 |
| Independent stopped/disks/public-IPv4 readback | 01:25:28.861434 |
| CloudWatch acceptance readback | 01:25:32.701851 |

Controls, original-file integrity, exact host/current boot, encrypted Kafka mount,
no swap, tmpfs, registry/image identities and empty pilot namespace passed.
The nine-file candidate was installed in a separate versioned directory. The
existing production installation and its original systemd units were preserved.
The private provider check authenticated and returned **Arbitrum chain ID 42161**.
Kafka and Spark became healthy; the producer ran and Spark emitted fresh progress.

A bounded sample verified **45 rows in two Parquet objects (81,314 bytes)**,
committed batch 0, Kafka partition 0 offsets **0–44** (exclusive end 45). Its
checkpoint commit existed; every sampled offset was present once, all chain labels
were Arbitrum, and no duplicate transaction/log identity appeared in this sample.
The pipeline continued beyond this sample and drained with verified checkpoint
**batch 13**. The 45 rows are a validated sample, not the entire run's row count.

Producer and Spark exited 0; Kafka exited 143 during controlled termination. No
container was OOM-killed. The controller reported successful stop, the journal
contained its graceful-stop marker and CloudWatch contained `SPARK_DRAIN_VERIFIED`.
All four Compose roles—Kafka, topic-init, producer and Spark—had current-window
CloudWatch events. Event observations were bounded, not total log counts.

## B. Why it matters

This proves the first ingestion path runs on AWS independently of the Mac. EBS
retains broker data, S3 retains Bronze data/checkpoints, and explicit offset and
commit checks tie a readable output sample to Kafka progress. Graceful draining
establishes a recoverable stopping point before releasing compute.

This does not establish complete historical blockchain capture, end-to-end exactly
once semantics or uninterrupted production operation. A WebSocket feed can miss
events during downtime; durable offsets protect records already accepted by Kafka.

## C. Files changed and exact AWS scope

Added [sanitized execution evidence](evidence/aws_runtime_streaming_pilot_result_2026-11-19.json),
[security consumption ledger](evidence/aws_runtime_security_consumption_2026-11-19.json),
[this report](aws_runtime_streaming_pilot_result.md) and an
[offline consumption gate](../runtime/pilot_security.py). The package builder,
cloud-readiness command and streaming relay now reject reuse of the consumed
exception before packaging output or AWS client creation. Missing, malformed,
ambiguous, oversized or mismatched consumption history fails closed. Added focused
regression tests and current-result links. Historical approval records remain intact.

| Existing resource | Actual action / retained state |
| --- | --- |
| EC2 `i-0ca1671a59751acc5` | One t3.xlarge start in us-west-2a; normal stop; public IPv4 released |
| Root `vol-03bd5d26947719f98` | Encrypted 30 GiB gp3 retained; separate versioned candidate added |
| Kafka `vol-02c9fb14a9c568fb5` | Encrypted 100 GiB gp3 retained, DeleteOnTermination=false; broker logs persisted |
| Guest service | Manual `/run/systemd/system/blockchain-pilot-reader-candidate.service`; no enable-on-boot |
| S3 | New objects only under the reviewed pilot Bronze/checkpoint paths |
| CloudWatch | New container streams/events under existing `/blockchain/runtime/containers` |
| ECR | Existing digest-pinned images pulled; no publication/repository changes |
| Secrets Manager / IAM | Existing secret read privately using the existing workload role; no edits or privilege expansion |
| Terraform / platform | No backend initialization, authenticated plan, apply, import or state changes |

Candidate directory:
`/opt/blockchain-runtime-candidates/ae678174072b2484e2c095593f3363adf822904e`.
Frozen host archive:
`568d0fc889ca7139aefa651d6d5dccea90ac2bc2106130d8d6451e736b3c9143`.
Release metadata:
`7f948e94cec7f7aebaf2aa4dab4ddd8e92a60f083f17bb9cefcff4db89cd200f`.
Rendered service SHA-256:
`ed2a64e4edf3782f967adb84f81a16392fcad84ad86d24ab6539234ff7f59976`.

Both Terraform states remain separate: `production/runtime/terraform.tfstate` and
`production/platform/terraform.tfstate`. Exact Terraform changes and plan/apply
scope for this execution: **none**. No new cloud infrastructure was provisioned.
README, local development behavior, dependencies/images, original host files,
Snowflake/dbt/Streamlit and unrelated duplicate files were preserved.

## D. Tests and validation

Executed-source validation: **749 runtime tests plus 36 negative subtests**, Ruff,
secret scanning and Python/shell compatibility checks; all six jobs passed in
[CI 44433049046](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/44433049046).
Live evidence establishes boot/controls/install, unchanged-secret authentication,
chain identity, healthy streaming, bounded Parquet/offset correctness, final drain,
CloudWatch delivery and independent stopped/storage readbacks.

The result checkpoint passes **777 runtime tests and 36 negative subtests**
(one existing websockets deprecation warning). Added cases cover consumed, missing,
symlinked, oversized, duplicate-key, malformed, FIFO/directory and
hash/scope/count-mismatched ledgers, including boolean values passed as counters. Tests prove packaging creates no output after rejection
and readiness/relay rejection precedes SDK or command execution. Ruff, tracked
and new-file secret scanning, whitespace checks, Python 3.9 syntax, local links
and public-to-private receipt/hash comparisons pass. Observe all six hosted CI
jobs after push. Exact final checkpoint and
CI status are reported in the conversation and private completion receipt.

Live crash/restart/recovery experiments were **not performed**. Existing offline
Kafka/Spark checkpoint recovery integration remains relevant, but is not evidence
of a second successful cloud boot or live recovery from this new checkpoint.

## E. Security review, IAM, secrets and image identity

The existing Alchemy secret ARN was used unchanged:
`arn:aws:secretsmanager:us-west-2:209479274979:secret:blockchain-runtime/11-6c/alchemy-websocket-url-d44flr`.
The JSON-object reader resolved it privately, and tmpfs secret injection supplied
only the URL to the original producer image. No URL/key, credential, secret payload,
raw row, raw terminal transcript or raw log message was disclosed or persisted in
these evidence files. This run used the current secret value; it did not separately
attest a version ID. No further secret correction is needed.

Access used the MFA-backed `blockchain-runtime-deployer` role, Session Manager
plugin and reviewed private `blockchain-runtime-demo-shell` document. No SSH or
security-group ingress was introduced. The workload role/profile remained
`blockchain-runtime-ec2`, with its existing exact-secret read permission and
reviewed permissions boundary. Containers still share the instance-role trust
boundary; this pilot does not provide per-container IAM isolation.

Registry prefix: `209479274979.dkr.ecr.us-west-2.amazonaws.com/`.

| Role / repository | Exact executed manifest digest |
| --- | --- |
| Producer / `blockchain-runtime-producer` | `sha256:63d9d0f24a1cb7eb30b080e58f8a40ddd49292b89020328409e6f45319597afd` |
| Spark / `blockchain-runtime-spark` | `sha256:bb03f7d9917f2d8dc7d2fc1fc6a1df83fe876f5298e98c4163c689a1b890a134` |
| Kafka / `blockchain-runtime-producer` | `sha256:1113a10e8a93fc12b2d310ef6f53b8f5fc95fb806a8c6edcfa6b67d17959378b` |

The exact 41-advisory supervised demonstration exception was exercised and is
**consumed**, despite time remaining before its former expiration. It does not
patch vulnerabilities or authorize another run. The original acceptance JSON is
an immutable historical snapshot; the hash-bound consumption ledger records its
current exhausted status. Another live run requires remediation or a new reviewed
security decision, plus explicit paid/deployment approval. Existing archives and
approval history are retained; no replacement executable pilot package is issued.

The Kafka filesystem was initialized under a prior, separate approval. Its XFS UUID
is `da1454d8-1bea-41a3-89d7-87bd570a47bd`, mounted at
`/var/lib/blockchain-runtime/kafka`. **No mkfs/wipefs or initialization occurred in
this window. Never repeat initialization on this volume.** Any different,
uninitialized volume must fail closed with `EBS_INITIALIZATION_REQUIRED` and
requires its own exact-volume approval before first filesystem creation.

## F. Git commit and CI status

Executed develop checkpoint: `575ca232d98da158a70b292aa48a3ab09e825577`;
[CI 44433049046](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/44433049046)
passed all six jobs. The result/consumption checkpoint follows safe local review,
commit, push and automatic CI observation. It does not deploy or rebuild images.
Main remains unchanged; no merge is authorized. No historical artifact is replaced.

## G. Cost implications

Launch to independent stopped observation was **505.861 seconds (8 minutes
26 seconds)**. At the previously recorded combined EC2/IPv4 estimate of
USD 0.1714/hour, this conservative interval estimates **USD 0.024085**, excluding
retained storage, requests, logs, provider usage, tax and other charges. Actual
billing was not queried; this is a historical-rate estimate, not an invoice or cap.
The account also incurred other separately reported preparation windows.

EC2 compute is stopped and its public IPv4 is released. The retained **130 GiB EBS**,
ECR images, S3 objects/checkpoints, CloudWatch logs and Secrets Manager secret can
continue costing money while compute is stopped. The provisional 40-hour/month,
approximately USD 24 planning scenario remains an estimate and is not permission
to schedule starts. This offline result work creates no billable AWS resource.

## H. Remaining blockers

The first controlled streaming pilot is accepted. Continuous operation, scheduling,
a later live restart/recovery test and additional starts remain outside this
completed authorization. The vulnerable-image exception is exhausted. Before a
future cloud run, review patched images or a new exact risk decision, current scans,
IAM/source-bound artifacts, retained Kafka identity and the existing checkpoint.
Do not require an empty pilot prefix or replay the first-run preparation blindly.
No additional secret edit, AWS resource change or paid start is approved.

## I. Next step, shutdown, rollback and cleanup

Next safe work is offline image/security remediation and a cost-aware recovery /
orchestration proposal. Keep milestone **11.5D paused**. A future live test must
have a concrete scope and new approval; do not reuse this archive's approval.

This window armed absolute application stop at launch +20 minutes, external normal
EC2 stop at +30, guest poweroff at +35, target stopped at +40 and outer supervision
at +45. Acceptance triggered earlier shutdown. Producer stopped, Spark drained to
its final checkpoint, Kafka stopped, the service entered its configured 45-second post-stop observation phase,
and the relay requested normal EC2 stop, which was independently confirmed. No timer extended the launch time.

Completed rollback/cost containment was normal stop, preserving originals and all
new evidence/data. Keep the versioned candidate, both encrypted disks, Kafka
identity/logs and S3 prefixes:

- `s3://johnson-blockchain-analytics-prod/blockchain-analytics/pilot/runtime/11-6c-v1/bronze/swaps/`
- `s3://johnson-blockchain-analytics-prod/blockchain-analytics/pilot/runtime/11-6c-v1/checkpoints/swaps_bronze/`

No checkpoint/log reset, volume formatting, data deletion or resource cleanup is
authorized. Any later cleanup needs an exact retention/cost review and approval.
Stopped/IPv4/storage readbacks confirm the immediate compute shutdown; they do not
assert zero ongoing AWS cost. The public CloudWatch summary retains only booleans
and bounded counts, never raw messages.

## J. Whether production infrastructure changed

**Yes: the explicitly approved existing EC2 runtime was started and normally
stopped; a separate candidate was installed; Kafka logs, pilot S3 data/checkpoints
and CloudWatch streams/events were written.** No new infrastructure, Terraform
state/configuration, IAM privilege, secret value, original installation or existing
Snowflake/dbt/Streamlit component changed. Both original encrypted disks remain
attached and the public IPv4 is released. Milestone **11.6C's first AWS streaming
pilot is accepted**; broader unattended runtime remains future work.
