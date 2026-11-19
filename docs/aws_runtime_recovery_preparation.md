# 11.6C follow-up — Offline recovery preparation and security recheck

Current follow-up: [metadata collector and offline recovery operator](aws_runtime_recovery_operator_review.md). Production execution remains blocked; the earlier preparation and approval snapshots below are preserved.

**The first cloud streaming pilot remains complete. This follow-up prepares
recovery checks offline; it does not authorize or perform another AWS run.**
The one-run exception remains consumed and milestone 11.5D remains paused.

## A. What was implemented

Added [a pure recovery consistency checker](../runtime/recovery.py), focused
[negative and positive tests](../tests/test_runtime_recovery.py), and expanded
[synthetic container integration](../tests/integration/streaming_pilot.py).
The checker performs no file/network/cloud operation, executes no command and
never grants a start. It compares trusted baseline metadata against an observed
snapshot, returning only fixed status, a pending-record count and explicit
`cloud_start_authorized=false` / `live_checks_required=true` flags.

The offline integration now records Kafka topic/cluster identity and Spark query,
commit, sink and offset metadata after the original abrupt-process-exit test. It
checks the same state after broker restart, processes two additional records,
records a new stopped baseline, and repeats without duplicating prior records.
A new adversarial phase recreates the synthetic topic with the **same numeric
end offset** and different events. Its changed topic UUID must be rejected before
Spark starts. The original lower-offset data-loss rejection remains a separate test.

This test deletes only synthetic topics in the isolated test broker. Its local
Compose resources are disposable; it never touches the production Kafka topic,
EBS, S3 or credentials.

## B. Why

The first-start operator requires an empty pilot namespace. That was appropriate
for the initial demonstration, but the namespace now contains real Bronze data and
checkpoint commits. Bypassing the empty-prefix check or resetting the checkpoint
would undermine recovery. A dedicated resume workflow needs different evidence.

Kafka offsets are positions, not identities. A deleted/recreated topic can acquire
the same offsets with different contents. We therefore check **topic UUID**, cluster
identity, retained storage identity, output/checkpoint paths and Spark query ID,
as well as offsets and matching checkpoint/sink commits. Retention may advance
past already committed data, but it must not remove unprocessed offsets. An active
or unknown writer, changed progress, missing metadata or partition changes block
this one-partition contract. The source must remain stopped during inspection.

The first local test exposed a representation error in the new check: librdkafka's
UUID text can use standard Base64 `+/`, while Kafka metadata can use URL-safe
`-_`. A local broker probe reproduced the actual format. The checker now compares
canonical UUID bytes, rejects zero/noncanonical IDs, and preserves input snapshots.
Regression cases retain this evidence; no production behavior was weakened.

## C. Files changed and preserved scope

Changed only the recovery module/tests, synthetic integration and its shell
sequence, this report/evidence, and current-status links in the pilot/security/
publication/result reports. The [sanitized evidence](evidence/aws_runtime_recovery_preparation_2026-11-19.json)
records upstream response hashes, frozen decision bindings and actual validation.

README, Terraform/state/backend, IAM, Secrets Manager, production Compose,
controller/operator/relay, dependency locks, image pins, existing ECR artifacts,
Kafka/S3 data and the consumed-exception gate are preserved. Develop remains the
working branch; unrelated duplicate files remain untracked and untouched.

## D. Tests and validation

**47 focused recovery cases, 824 runtime tests and 36 negative subtests pass**
(one existing websockets deprecation warning). Ruff, secret scanning, Python 3.9
syntax, shell syntax, link/hash and whitespace checks pass. The local Docker
integration uses the existing reviewed Linux/AMD64 Spark/Kafka images with the new
recovery module mounted read-only. It uses an internal network, no AWS credentials,
no provider, no image pull/build/publication and a uniquely named disposable project.
The initial failed test and format probe remain preserved privately. The corrected complete local run passed: initial 3 rows, resumed 5 rows, repeated
5 rows without duplication, equal-offset topic replacement rejected before Spark,
and the original offset-loss test rejected missing data. Owned-resource cleanup
returned 0, and an independent check confirmed zero owned test containers, volumes
or networks remained; exact results are in the linked evidence. Hosted CI additionally builds committed-source images
and runs the same expanded integration without the temporary development mount.

This validates synthetic recovery and the checker. It does **not** establish a
live AWS restart, current production topic identity, a fresh vulnerability scan,
or compatibility of a different Hadoop/SDK/image cohort.

## E. Security recheck and remediation decision

The original [41-advisory decision](evidence/aws_runtime_security_acceptance_2026-11-17.json)
remains bound to its exact historical images; its [consumption ledger](evidence/aws_runtime_security_consumption_2026-11-19.json)
is unchanged. No finding is waived, downgraded or marked fixed by this work.
There was no new image scan or publication. Recorded scanner counts are historical.

Public metadata was fetched again on **2026-11-19 UTC** and preserved privately
with response hashes in the evidence. The limited compatibility/fix check found:

| Input | Observed result | Implementation decision |
| --- | --- | --- |
| Maven Hadoop release metadata | Release 3.5.0 | No automatic Hadoop upgrade |
| Hadoop 3.5.0 project POM | AWS SDK V2 2.35.4 | Same embedded SDK cohort remains selected |
| Maven AWS SDK bundle metadata | Release 2.59.4 | Candidate metadata only; not a tested S3A replacement |
| Debian CVE-2026-19499 | Trixie glibc 2.41-12+deb13u4 vulnerable | No Trixie fix established by this record |
| Debian CVE-2026-5435 | Trixie glibc 2.41-12+deb13u4 vulnerable | No Trixie fix established by this record |

[Apache's S3A guidance](https://hadoop.apache.org/docs/r3.4.3/hadoop-aws/tools/hadoop-aws/troubleshooting_s3a.html#Classpath_Setup)
requires the matching Hadoop components and SDK; arbitrary SDK substitution can
break linkage. The [3.5.0 project POM](https://raw.githubusercontent.com/apache/hadoop/rel/release-3.5.0/hadoop-project/pom.xml)
still selects 2.35.4. See the [Hadoop release metadata](https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/maven-metadata.xml)
and [SDK release metadata](https://repo.maven.apache.org/maven2/software/amazon/awssdk/bundle/maven-metadata.xml).
The [Debian monetary-formatting record](https://security-tracker.debian.org/tracker/CVE-2026-19499)
and [DNS-printing record](https://security-tracker.debian.org/tracker/CVE-2026-5435)
report fixes in other suites; those do not patch the selected Trixie image.

This limited check establishes no simple supported pin change for these blockers.
It does not prove every possible remediation unavailable or evaluate all 41 IDs
again. A private Hadoop rebuild, distribution migration or replacing S3A with a
new upload architecture would need its own compatibility and maintenance review.
For this demo, keep the validated cohort frozen while evaluating that concrete
tradeoff. Do not mix unstable OS packages or silently broaden a risk exception.

## F. Git and CI

Starting develop checkpoint: `6ab949ecc0d7d67650cfd86f6050f3daa42bd7ef`;
[CI 44438101563](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/44438101563)
passed all six jobs. Remote develop/main were verified before edits; main remained
`21be18e45791a4199830754b80620d7d6599587e`. Commit this follow-up after successful
local validation, push develop and observe all six automatic CI jobs. Report its
exact new SHA/run in the conversation and private result receipt, avoiding a
self-referencing commit in this document. No main merge or deployment is included.

## G. Cost implications

This work uses public metadata, local Docker and the existing CI workflow. It
makes **zero AWS API calls**, creates no AWS resources and adds no EC2 hours.
The latest cloud state is the prior stopped readback; this offline work does not
claim a fresh AWS observation or zero ongoing storage cost. Existing retained EBS,
S3, ECR, logs and the secret remain in place. No spending limit, scheduler or paid
runner is introduced.

## H. Remaining blockers and missing dependencies

1. A fresh security decision is needed before any additional live execution of
   affected images; the consumed exception cannot be renewed by this checker.
2. A trustworthy **production recovery baseline is incomplete**: final checkpoint
   batch 13 is verified, but the public result does not contain the original topic
   UUID, Spark query ID or exact final offset map. Never invent those values from
   the 45-row sample, whose exclusive end offset 45 belongs to batch 0.
3. A separately approved inspection must reconcile retained Kafka metadata,
   checkpoint/sink records and prior evidence before establishing that baseline.
   If original topic identity cannot be established, hold application startup and
   review reconciliation rather than treating a new observation as historical proof.
4. The checker is used by **offline tests only**. A dedicated current-boot,
   source-bound recovery operator and bounded metadata collector still need
   implementation/review before it becomes a live startup gate. Current first-run
   packaging/readiness/relay remain blocked by the consumed security record.
5. Kafka's bounded retention and WebSocket downtime can create source gaps. A
   Kafka/checkpoint resume does not backfill missed blockchain events; that needs
   a separate bounded historical reconciliation design and acceptance test.

No new Python dependency is needed for the pure checker. Synthetic inspection
uses the already pinned confluent-kafka Admin/Consumer APIs and existing Spark.
There is no new credential, IAM or cloud service dependency in this follow-up.

## I. Recovery plan and next approval boundary

The next implementation scope is a dedicated offline metadata collector/operator
that preserves the existing namespace and cannot invoke topic creation, filesystem
initialization, checkpoint reset or secret edits. Keep the checker distinct from
permission checks and enforce it before any producer/Spark start.

A future live proposal must identify the exact image/security decision, operator
archive, existing instance/disks, reviewed checkpoint, single writer, fresh boot
and absolute shutdown deadlines. It should first verify identity and retained
metadata with the source stopped, then conditionally resume the existing query,
compare new committed offset ranges and drain/stop normally. A failed identity,
retention or commit check must preserve all data and stop the attempt. Do not
reuse this document as deployment approval or issue a runnable pilot package yet.

Keep the prior +20-minute application-stop, +30 external normal-stop and +35 guest
shutdown pattern unless a new concrete proposal justifies different deadlines.
New inspection/recovery starts, publication, cloud mutations, credential scope or
paid runtime require explicit approval after their actual artifacts are ready.
No rollback action is needed for production because this follow-up changes none.
Local rollback is the previous Git checkpoint; preserve the executed release,
all prior evidence and retained cloud data. No production cleanup is authorized.

## J. Whether production infrastructure changed

**No.** No AWS API, EC2 start, image publication/scan request, SSM command,
production Terraform operation, secret read/edit, IAM change, EBS operation,
S3 write, Snowflake/dbt/Streamlit change or main merge occurred. Milestone 11.5D
remains paused. The first AWS pilot remains accepted; live recovery is unverified.
