# Milestone 11.6C: exact Kafka EBS initialization proposal

**Historical proposal: subsequently approved and completed.** See the
[initialization result](aws_runtime_ebs_initialization_result.md) and recorded
UUID. The exact-volume approval is consumed; the example formatting command
below must not be replayed. EC2 is stopped. Normal startup must never initialize
storage. Milestone 11.5D remains paused. The original review scope is preserved.

## A. Proposed action

Initialize only the existing, unused Kafka volume as XFS once, record its real
UUID, add its UUID-based persistent mount, and verify the mount without starting
any application. Use a new supervised 15-minute EC2/SSM/normal-stop window.
Request normal stop by minute 10 and confirm stopped by minute 15.

| Identity | Exact proposed scope / last verified metadata |
|---|---|
| Account / Region | 209479274979 / us-west-2 |
| Instance / AZ | i-0ca1671a59751acc5 / us-west-2a |
| Kafka volume | **vol-02c9fb14a9c568fb5** |
| Storage | Encrypted gp3, 100 GiB (107374182400 bytes) |
| Creation | 2026-11-13 22:28:03.293 UTC; no source snapshot |
| AWS attachment | /dev/sdf, attached to the exact instance only |
| Stable guest path | /dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_vol02c9fb14a9c568fb5 |
| Last guest observation | /dev/nvme1n1, correct serial, no TYPE/UUID, not mounted |
| Mount target | /var/lib/blockchain-runtime/kafka |
| Excluded root volume | **vol-03bd5d26947719f98**, 30 GiB, AMI/snapshot-backed, /dev/xvda |

The NVMe index is an observation, not a formatter target. Device enumeration
can change; see [AWS device naming](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/device_naming.html).

## B. Why

Kafka needs persistent filesystem-backed logs across container/instance stops.
The volume already exists and is attached, but currently cannot be mounted as
XFS. Terraform adoption/attachment is separate from this one-time destructive
operation. It is not part of producer/Kafka deployment or routine recovery.

## C. Inputs and files

- This approval scope, the unchanged
  [storage operating contract](../infra/terraform/environments/runtime/OPERATIONS.md#one-time-kafka-volume-initialization),
  and [sanitized cloud/guest evidence](evidence/aws_runtime_host_recovery_2026-11-16.json).
- No Terraform/configuration/backend/state changes or new AWS resources.
- No modification of the normal startup script; it remains fail-closed with
  `EBS_INITIALIZATION_REQUIRED` on an uninitialized volume.

## D. Mandatory fresh preflight and conditional execution

**Approval would authorize execution only if every storage check passes.**
The prior TYPE/UUID probe and lack of a snapshot are not by themselves proof
that formatting is safe. The full guest audit below has not yet been run.

1. Recheck fresh expected MFA identity, exact stopped instance, attachments,
   volume size/encryption/creation/snapshot metadata, no ingress, reviewed
   document, normal-stop access and Console fallback. Arm the local stop
   watcher before the operator starts the exact node. No start IAM expansion.
2. In the explicitly targeted SSM shell, verify host identity and Docker/
   Compose persistence; require zero containers. Resolve the stable EBS by-id
   path, verify its serial and exact byte size, and require a writable whole
   disk. Compare against the live root disk's identity/ancestors and the
   excluded root volume; reject a root device or any root partition.
3. Inspect `lsblk` device/partition/filesystem/UUID/partition-table/mount data,
   kernel holders, swap and mount records. Require no children/partitions,
   holders, filesystem, UUID, partition table, swap use or mounts anywhere.
   Probe with `blkid -p -o export`, `file -s`, and **`wipefs --no-act`**.
   Require consistent empty/new-device results and no signatures. Missing
   tools, probe errors, existing/damaged/unrecognized signatures, nonempty
   metadata or any ambiguity block formatting. Never erase a signature to
   make a check pass.
4. Check the exact mount target is absent or empty/unmounted and has no
   conflicting fstab entry. Preserve a private, uniquely named root-only
   fstab backup and hash. Recheck the candidate identity immediately before
   the single formatting command. If approval or any check is missing, stop.
5. The only proposed formatter is the following **without any force option**:

   ```bash
   sudo mkfs.xfs /dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_vol02c9fb14a9c568fb5
   ```

   It must never be issued merely because the example is present in this
   document. Execute once only after the explicit exact-volume approval and
   passing fresh guards. No partition creation, destructive wipefs, force
   formatting, automatic retry or fallback device is included.
6. Read the generated XFS UUID; verify it resolves uniquely to the exact serial
   and differs from root. Create the approved mount directory if absent, with
   root ownership/mode 0750. Preserve existing fstab entries and add only:

   ```text
   UUID=<verified-new-Kafka-UUID> /var/lib/blockchain-runtime/kafka xfs defaults,nofail 0 2
   ```

   The UUID is an execution result, not a fabricated release input. Validate
   the proposed fstab before replacement and keep its private backup. Mount
   **only this target**, not all fstab filesystems. Verify target/source/UUID/
   XFS/serial, mode and at least 10 GiB available. Do not rerun user data or
   reset cloud-init. No test payload, Kafka metadata, container or secret is
   written to the new filesystem in this window.
7. Exit SSM and normally stop EC2 with force/hibernate/skip-OS-shutdown false.
   Verify stopped, no public IPv4 and the retained exact disk attachment.
   Record the UUID/mount evidence for the later release manifest. A separate
   future boot will verify automatic mount persistence; do not add another
   restart inside this window without approval.

This follows the filesystem and UUID-mount approach in
[AWS EBS guidance](https://docs.aws.amazon.com/ebs/latest/userguide/ebs-using-volumes.html),
with the project's stricter identity/signature gates. Raw fstab/host logs stay
private; only selected metadata and hashes belong in Git.

## E. Security and irreversibility

Formatting writes filesystem metadata and can destroy existing data. This is
why the exact-volume approval and fresh rejection guards are mandatory. Do not
claim the operation can be rolled back to the original raw volume afterward.
The root disk is excluded from formatting; its only proposed configuration
change is the narrowly scoped fstab entry/backup and mount directory.

No IAM, security group, credentials, secret, ECR, S3/checkpoint, Snowflake,
Terraform, main-merge, resource creation/deletion or OS-package action is in
scope. A passing test or prior host-repair approval does not authorize this.

## F. Git and CI

Keep this offline proposal with the validated host-recovery checkpoint on
develop. Main and README remain unchanged. CI validates offline/static
contracts; it must never format a real device or connect to the AWS pilot.

## G. Cost and duration

No new disk or snapshot is proposed. At the previously reviewed combined
compute/IPv4 rate of USD 0.1714/hour, 15 minutes is approximately **USD 0.04285**
before other usage/taxes. Retained disk/alarm charges remain about USD
10.60/month; formatting does not resize or remove them. These are estimates,
not a spending cap. The watcher depends on this Mac/credentials; keep the
Console stop fallback active. No AWS-side stop schedule is added.

## H. Failure, rollback and cleanup

If any pre-format check fails, perform no format/mount and stop EC2 normally.
If formatting was attempted or partially succeeded, preserve the evidence and
**never retry mkfs automatically**. An existing XFS volume becomes a recovery
case, not permission to initialize again. No force option or wipe is a remedy.

If only the new mount/fstab configuration fails, preserve the filesystem.
Undo only this change's verified mount/entry when safe, using the private
backup only if no unrelated edits would be overwritten. Otherwise pause that
repair and preserve the partial result. Do not touch root mounts, delete disks,
reformat, detach storage or add snapshots automatically. Confirm EC2 stopped
and retained charges afterward; resource cleanup requires separate approval.

## I. Exact next approval

Request explicit approval for **one-time XFS initialization of only
vol-02c9fb14a9c568fb5**, conditional on all fresh rejection checks, plus its
UUID/fstab/mount setup and one supervised 15-minute start/SSM/normal-stop window
on i-0ca1671a59751acc5. This is a destructive storage approval, not a general
deployment approval. Secrets, images, Kafka/Spark startup and later reformatting
remain excluded. If the volume is not demonstrably unused and safe, do not
format even with this conditional approval.

## J. Production infrastructure changed by this proposal

None. Existing disk metadata was read after the separately approved host repair;
no initialization, mount or fstab change has occurred. Both Terraform states
and the existing platform resources remain outside scope.
