# Milestone 11.6C: one-time Kafka filesystem initialized

Completed 2026-11-16 UTC (2026-11-15 Pacific). **Kafka volume initialized once,
UUID-based mount verified, EC2 stopped.** The exact-volume formatting approval
is consumed. Never repeat initialization on this volume; future mount failures
are recovery cases. Milestone 11.5D remains paused.

## A. What was implemented

Initialized only encrypted 100 GiB gp3 volume `vol-02c9fb14a9c568fb5` as XFS
using one `mkfs.xfs` invocation without force. Verified its generated UUID,
added exactly one UUID-based fstab entry, and mounted only the Kafka target.

| Item | Verified value |
|---|---|
| Instance / Region | i-0ca1671a59751acc5 / us-west-2 |
| Kafka volume / serial | vol-02c9fb14a9c568fb5 / vol02c9fb14a9c568fb5 |
| Observed device this boot | /dev/nvme0n1, selected through its stable EBS by-id path |
| Filesystem UUID | **da1454d8-1bea-41a3-89d7-87bd570a47bd** |
| Mount target | /var/lib/blockchain-runtime/kafka |
| Filesystem / ownership / mode | xfs / UID 0, GID 0 / 0750 |
| Available at verification | 106524286976 bytes, above the 10 GiB requirement |
| fstab change | One `UUID=... /var/lib/blockchain-runtime/kafka xfs defaults,nofail 0 2` entry |
| Containers | None |
| End state | Stopped at 04:50:40 UTC; final readback 04:53:13 UTC, no public IPv4, both encrypted disks attached |

## B. Why

Kafka now has a real persistent filesystem for later broker logs. Cloud
attachment alone did not provide one. Its UUID can now replace the placeholder
in a future reviewed release manifest, while application/image/secret gates
remain separate. No example release was made deployable by this step.

NVMe numbering changed from the earlier boot: Kafka was previously nvme1n1 and
became nvme0n1. EBS IDs, serials and live root ancestry, rather than an index,
identified the authorized disk and excluded the 30 GiB root volume.

## C. Files and artifacts

- This result and [sanitized initialization evidence](evidence/aws_runtime_ebs_initialization_2026-11-16.json).
- A consumed-approval notice in the [historical proposal](aws_runtime_ebs_initialization_review.md)
  and current readiness/status links.
- Separately prepared [ECR publisher proposal](aws_runtime_ecr_publisher_review.md),
  JSON supplement, policy contract tests and read-only ECR evidence.
- No normal-startup, Terraform, application, README or workflow changes.

Private operator programs, original/corrected probe source and receipts are
preserved locally. Root-only guest receipts remain in
`/root/blockchain-ebs-init-vvfr2sgo`, including original/proposed fstab and
formatter output. A durable exclusive attempt marker is at
`/root/blockchain-ebs-init-vol-02c9fb14a9c568fb5.attempt.json`.
No raw fstab, credential cache, session token or broad guest log is committed.

Executed initialization-program SHA-256:
`4da7d5a69f9ea6a3fca160302a1e2ad31eda7858bfc30370315218b676fc4dba`.
The helper is a private one-time operator action, not boot or recovery code.

## D. Tests and validation

Fresh AWS metadata verified exact identities, creation time, no snapshot
origin, encrypted size/AZ/attachments, stopped state, no ingress, standard
credits, the unchanged private document and normal-stop DryRun authorization.

The first scheduled window expired while EC2 was off. The operator later
started it; normal shutdown closed that late boot without SSM or formatting.
After the operator requested a replacement window, privately renewed MFA.
Only that role's unexpired CLI cache entries were preserved under held names
to force renewal; no credential values or other identities were changed.

The replacement watcher waits for start before beginning the runtime clock.
Four simulated lifecycle tests cover no-start expiry, immediate start and
delayed starts. It conservatively measures runtime from the last verified
stopped observation before the start, requests normal stop at minute 10 and
targets stopped by minute 15. It is Mac/credential-dependent, not an AWS-side
hard deadline. The Console remains the stop fallback.

The private initialization guards passed 32 adversarial snapshot checks plus
fstab, approval/deadline and exclusive-attempt tests. On the guest, the first
audit blocked before formatting because `file` inspected the by-id symlink
itself. Corrected only the probe to inspect the already resolved block device;
the required result remained `data`. No existing signature was erased and no
format attempt had occurred. The corrected full audit passed:

- Exact host, block-device serial/byte size, writable whole disk and excluded
  root identity/ancestry.
- No partitions, filesystem, UUID, partition table, signatures, mounts, holders
  or swap use; consistent blkid, resolved-device file and read-only wipefs.
- Safe empty/unmounted target, valid unchanged fstab, no prior attempt marker,
  persistent Docker/Compose configuration and no containers.

Rechecked all guards immediately before formatting, preserved/fsynced private
backup/audit/attempt receipts, then formatted once. Verified unique new XFS UUID
distinct from root; validated proposed fstab before atomic replacement and
mounted only this target. Independent readback proved the current fstab is
exactly the original plus the one expected entry, root UUID unchanged, correct
mount source/serial/UUID/permissions, no containers and exit 0. SSM exited 0.
No subsequent-boot mount test or Kafka/Spark recovery test occurred here.

All **180 policy/pilot/startup-safety/image/bundle tests pass**, as do Ruff,
JSON/local-link checks, secret scanning and whitespace validation. Hosted CI
is observed separately. The new publisher contracts are offline scope tests,
not an AWS IAM simulator.

## E. Security and data correctness

Only the specifically approved Kafka disk was formatted. Root was not
formatted; its configuration received the approved fstab entry, backup and
private audit/attempt receipts. No force flag, destructive wipe, partition
creation, automatic format retry, full bootstrap rerun or cloud-init reset.
The normal startup template still contains no mkfs/wipefs and must remain so.

Formatting approval is now consumed. Existing/damaged/ambiguous storage must
always block future initialization. Reuse the recorded UUID; never generate a
fresh filesystem to solve a mount, Kafka or checkpoint error. No application
data, Kafka cluster metadata, image, secret or S3 checkpoint was deployed.

## F. Git and CI

Checkpoint the validated result and offline publisher proposal on develop and
observe that commit's six CI jobs. Source image rebuilds/publication plans
must reference the new committed source, not older mutable local tags.
Main remains `21be18e45791a4199830754b80620d7d6599587e`; README is unchanged.

## G. Cost implications

Successful-window LaunchTime was 04:46:04 UTC; stopped was observed at
04:50:40 UTC. At the reviewed USD 0.1714/hour compute/IPv4 rate, the conservative
276.34-second interval is approximately **USD 0.0132** before other usage/taxes.
The late boot's 04:34:33–04:37:03 interval adds an upper estimate of USD 0.0072.
These are interval estimates, not invoices or caps; both include observation
latency. EC2 is stopped. Retained disks/alarms remain approximately USD
10.60/month before other usage/taxes. Formatting did not resize storage.

## H. Remaining blockers

- Verify UUID-based automatic mount and Docker persistence at the next approved
  runtime start; no extra restart was added to this window.
- Approve ECR publisher access, exact reviewed publication and BASIC scan
  review; actual ECR digests remain unavailable before upload.
- Create the Alchemy secret and review exact-ARN workload IAM/boundary changes.
- Approve a concrete streaming release/window and verify S3 writes, Kafka/Spark
  recovery, drain and cloud-independent stop behavior.

## I. Next step

Review the prepared [publisher scope](aws_runtime_ecr_publisher_review.md).
Building/reviewing local committed images is safe offline work; live IAM
changes and image publication require separate approval. Keep EC2 stopped.

## J. Production infrastructure changes

The approved node was started and stopped. The exact Kafka disk received XFS;
the approved guest mount/fstab/receipt changes were made. No AWS resource
creation/deletion, IAM change, Terraform/backend/state operation, Snowflake
change or main merge occurred. Subsequent ECR preflight was read-only. The last
verified runtime inventory remains 28 resources; no state recount was done.
