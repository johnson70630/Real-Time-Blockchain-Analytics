# Milestone 11.6C: Compose and Docker host recovery

Completed 2026-11-16 UTC (2026-11-15 Pacific). **The approved host-software
recovery passed and EC2 is stopped.** The separate 15-minute window is closed;
it is not continuing approval for another start, storage initialization or
application deployment. Milestone 11.5D remains paused.
The later separately approved [one-time Kafka initialization](aws_runtime_ebs_initialization_result.md)
is complete and ended with EC2 stopped. Findings below describe this earlier
software-recovery window; its storage proposal is now historical/consumed.

## A. What was implemented

On only `i-0ca1671a59751acc5`, installed the reviewed Compose 5.1.3 binary at
`/usr/local/lib/docker/cli-plugins/docker-compose`. Checked its SHA-256 before
installation/execution and again afterward. Created the final file without
overwriting an existing path; verified root ownership and mode 0755. Enabled
and started the existing `docker.service` and verified it is enabled/active.

Verified zero containers, unchanged unformatted/unmounted Kafka disk, clean
SSM exit and early normal EC2 stop. Stopped was observed at **01:44:02 UTC**;
final readback at 01:45:38 UTC confirmed no public IPv4 and both encrypted disks
still attached. No full user-data/cloud-init rerun was used.

## B. Why

The previous live access test found missing Compose and a disabled Docker
service. A narrow guest repair resolves those observed dependencies without
replacing infrastructure or attempting to replay a consumed Terraform plan.
It does not establish complete streaming readiness: persistent Kafka storage,
images, secrets and an approved application window remain separate steps.

## C. Files and artifacts

- This A-J report and [sanitized recovery evidence](evidence/aws_runtime_host_recovery_2026-11-16.json).
- The [exact-volume initialization proposal](aws_runtime_ebs_initialization_review.md).
- Current status links in the pilot, SSM and access-test reports.
- No application, Terraform, IAM policy, workflow, dependency or README changes.

Private approval/preflight/watcher/stop receipts and reviewed repair commands
are preserved in mode 0700 temporary directories with mode 0600 files. Repair
script SHA-256 is
`7c49c8513ef9fb07c65b75e8974a36a7570bb16b6b86d6f6a5b5c19478f1a134`.
An integrity-checked copy ran over the existing approved SSM channel. No session
token, credentials, broad guest log or raw Terraform artifact is committed.

## D. Tests and validation

Before start, verified expected STS principal, stopped node, ownership tags,
instance profile/AZ, both encrypted disks, no ingress, standard CPU credits,
shutdown-to-stop, the unchanged private document and normal-stop DryRun access.
The fresh session had enough time for the window plus a shutdown margin.

Verified the reviewed binary hash against Docker's official
[v5.1.3 checksum manifest](https://github.com/docker/compose/releases/download/v5.1.3/checksums.txt).
The private repair script passed Bash syntax and offline safety review:
checksum precedes execution, final-path creation refuses overwrite, failed
container queries fail closed, and no format/mount/cloud-init/credential/image
operations are included. The bounded driver retains exact-node, state/tag/AZ
and normal-stop flag guards with a 10-minute stop / 15-minute end limit.

| Guest verification | Result |
|---|---|
| Compose version | 5.1.3 |
| Installed binary SHA-256 | `a0298760c9772d2c06888fc8703a487c94c3c3b0134adeef830742a2fc7647b4` |
| Owner / mode | UID 0, GID 0, 0755 |
| Docker | Server 25.0.18, service enabled and active |
| Containers | None, including stopped containers |
| Kafka device identity | Stable by-id resolves to /dev/nvme1n1; exact approved serial matches |
| Kafka filesystem / mount | No TYPE/UUID, blkid exit 2; no mount, findmnt exit 1 |
| Repair and verification | Exit 0; SSM shell exit 0 |
| Final AWS state | Stopped, no public IPv4, both disks attached/encrypted |

Systemd enablement is verified; a subsequent-boot persistence test has not yet
been run. Missing Compose/Docker enablement is resolved, but the original
bootstrap interruption's cause remains unconfirmed. Filesystem/partition/
signature/root-exclusion checks for safe initialization still need a fresh
guest audit. The earlier absence of a filesystem alone is not that audit.

All **117 policy/pilot/startup-safety tests pass**, as do focused Ruff, secret
scanning, JSON/local-link validation and whitespace checks. Observe the new
six-job CI independently of earlier successful runs.

## E. Security and correctness

Used the existing MFA deployer and exact private session document. The operator
started the node through the Console; no StartInstances IAM grant was added.
Only the approved plugin file/directory, private temporary files and Docker
service enablement changed in the guest. Temporary download/staging files were
cleaned; no existing plugin was overwritten. No OS package upgrade occurred.

Kafka EBS was not formatted, partitioned, mounted or written by application
code. No fstab edit, secret access, image pull/push, container creation,
Terraform/backend/state operation, Snowflake operation or main merge occurred.
The normal startup template is unchanged and still contains no mkfs/wipefs.
Full SSM transcript logging remains unconfigured. Effective policy JSON and
document sharing permissions remain operator-attested where previously noted.

## F. Git and CI

Checkpoint the validated report/proposal on develop and observe its hosted CI;
record the resulting commit/run in the conversation. Main remains
`21be18e45791a4199830754b80620d7d6599587e`; README remains unchanged.

## G. Cost implications

LaunchTime was 01:41:22 UTC; stopped was observed at 01:44:02 UTC, a conservative
160.36-second interval. At the previously reviewed combined compute/IPv4 rate
of USD 0.1714/hour, estimated cost is **USD 0.0076** before other usage/taxes.
This includes observation latency and is not an invoice or spending cap.
Retained disks and alarms remain approximately **USD 10.60/month**, excluding
other usage/taxes. No resource was deleted or resized.

## H. Remaining blockers

- Exact-volume one-time XFS initialization and persistent mount approval.
- ECR publication permissions, immutable digests, scan review and release inputs.
- Alchemy secret provisioning and exact-ARN workload IAM/boundary review.
- Approved streaming deployment, real S3 smoke/recovery tests, graceful drain
  and final retained-cost verification.

## I. Next step

Review the [one-time EBS initialization proposal](aws_runtime_ebs_initialization_review.md).
It names only Kafka volume `vol-02c9fb14a9c568fb5` and requires new explicit
formatting/mount and paid-window approval. Any existing or ambiguous storage
metadata must block formatting. No initialization has been approved here.

## J. Production infrastructure changes

The approved existing instance was started, its guest Compose/Docker setup
repaired, and normally stopped. No AWS resource creation/deletion, IAM change,
Terraform state access or platform/Snowflake change occurred. The last verified
runtime inventory remains 28 managed resources; this step did not recount state.
