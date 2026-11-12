# AWS runtime operating contract

This root owns only the future EC2 runtime foundation. It does not own the
existing analytical-data bucket, Terraform-state bucket, Snowflake resources,
or Streamlit deployment. Its backend key is
`production/runtime/terraform.tfstate`, separate from
`production/platform/terraform.tfstate`.

Applying this root is not authorized by Milestone 11.6B. A future apply will
create an EC2 instance in the running state because AWS must boot a newly
created instance. The bootstrap installs Docker, verifies a pinned Compose
binary, and mounts the persistent Kafka volume, but it deliberately starts no
application containers and reads no secrets.

For infrastructure-only planning, `approved_secret_arns` defaults to `[]`.
The entire secret-read IAM statement is omitted in that mode. This does not
create a secret or make the streaming application ready to start. Before a
later pilot, separately approve secure secret creation and a policy update
allowing only its exact ARN in account 209479274979, Region us-west-2.
Wildcards and other accounts/Regions are rejected. Never use a fabricated ARN
to unblock a real plan. Offline provider-mocked tests cover both modes.

## Runtime contract

- No inbound security-group rules, SSH key, Elastic IP, NAT Gateway, or load
  balancer exists.
- Session Manager is the only operator access path.
- The automatically assigned public IPv4 address provides outbound HTTPS via
  the Internet Gateway and is released when the instance stops.
- Kafka stays on the private Docker network and binds no host/public listener
  in the future cloud Compose file.
- Container images must use ECR digest references. Tags alone are not an
  immutable deployment identity.
- The producer may start only after Kafka is healthy. Spark may start only
  after Kafka and the target S3 checkpoint are verified.
- Exactly one Spark writer may own a checkpoint path at a time.
- Kafka retains events for 72 hours. Spark checkpoints and pilot Parquet data
  live under `blockchain-analytics/pilot/runtime/`; production Bronze paths are
  forbidden during the pilot.
- CloudWatch container logs retain data for 14 days by default.

## Shared container IAM boundary

IMDSv2 is required. Its response hop limit is two so AWS SDKs inside Docker
containers can obtain the EC2 instance-role credentials needed for S3 and
other approved AWS calls. This makes the EC2 role a shared trust boundary:
Kafka, the producer, and Spark do not have independent IAM roles and may all be
able to obtain the same instance credentials.

Only reviewed, digest-pinned images and trusted application code may run on
this host. Do not expose the Docker socket to application containers, run
untrusted images, or permit arbitrary code execution. The shared role remains
limited to the pilot S3 prefix, approved secrets, approved ECR repositories,
runtime logging, and SSM; it has an explicit denial for the Terraform state
bucket. Secrets must remain individually allowlisted and must not be passed to
containers that do not require them. Stronger per-container IAM isolation
would require a later design such as ECS task roles.

## One-time Kafka volume initialization

Normal user data never formats a block device. A blank volume causes the safe
status `EBS_INITIALIZATION_REQUIRED`; a device with unrecognized signatures
causes `EBS_FILESYSTEM_UNRECOGNIZED_OR_DAMAGED`. Both stop bootstrap before
any application container can start while leaving SSM available for diagnosis.

Formatting a new Kafka volume is a separate production mutation requiring its
own reviewed milestone and explicit operator approval. Before any future
`mkfs.xfs` command, the operator must:

1. Confirm the exact AWS Region, EBS volume ID, expected EC2 instance ID, and
   attachment using independent AWS metadata.
2. Confirm the volume was newly allocated and was not created or restored from
   a snapshot containing existing data.
3. Map the expected EBS volume ID to its stable NVMe by-id device and verify its
   serial; reject any mismatch or missing device.
4. Prove the candidate is not the root device or a partition of the root
   device and is not mounted anywhere.
5. Inspect partition, filesystem, and signature metadata using non-mutating
   tools such as `lsblk`, `blkid`, and `wipefs --no-act`.
6. Reject existing, damaged, unrecognized, or ambiguous metadata. Absence of a
   recognized filesystem alone is not proof that a volume is safe to format.
7. Obtain explicit approval naming that exact blank volume before formatting
   it once as XFS.
8. Verify the new XFS UUID, add the UUID-based persistent mount, mount it at
   `/var/lib/blockchain-runtime/kafka`, and rerun the non-destructive bootstrap
   verification.

The initialization procedure must never be invoked automatically, used for
recovery, or repeated during a normal boot, restart, replacement, or mount
failure.

## Start procedure

1. Confirm the approved change window, expected instance ID, stopped state,
   attached Kafka volume, and no other checkpoint owner.
2. Start the instance through an approved operator action. There is no
   automatic start schedule.
3. Require the safe `EBS_MOUNT_VERIFIED` result, then verify Docker, available
   disk space, SSM, and CloudWatch delivery. Any initialization-required or
   mount error blocks application startup.
4. Retrieve approved secrets at runtime without writing them to the image,
   user data, Compose file, shell history, or logs.
5. Pull images by reviewed ECR digest.
6. Start Kafka and require a healthy broker with its log directory on EBS.
7. Start the producer, then the single Spark Bronze writer using the existing
   S3 checkpoint.
8. Verify Kafka lag, checkpoint progress, pilot-object writes, and logs before
   declaring the runtime healthy.

## Stop procedure

1. Stop producer ingress gracefully.
2. Let the single Spark writer consume the remaining Kafka lag and complete a
   checkpoint commit.
3. Stop Spark gracefully and verify the checkpoint advanced.
4. Stop Kafka last and verify its data directory is on the persistent volume.
5. Stop, rather than terminate, the EC2 instance.
6. Confirm the stopped-state notification and that the EBS volume remains
   attached. EC2 compute billing stops, while EBS, snapshots, ECR, S3, logs,
   secrets, and any retained public IPv4 still incur their applicable charges.

An optional EventBridge Scheduler resource can only stop the instance. It is
disabled by default and has no `StartInstances` permission.

## Recovery boundaries

- **Normal stop/start:** reuse the EBS Kafka log and exact S3 checkpoint.
- **Instance loss:** create a replacement only from reviewed Terraform and
  attach the preserved Kafka volume. Never format a volume that already has a
  filesystem.
- **Kafka volume loss:** restore the latest reviewed snapshot when useful, but
  assume events after that snapshot may be absent. Reconcile the chain range
  and use the existing bounded recovery paths.
- **Checkpoint retained, Kafka retained:** restart the one writer; Spark resumes
  from checkpointed offsets.
- **Checkpoint lost:** `startingOffsets=latest` means a new stream will skip
  earlier retained Kafka events. Do not create a fresh checkpoint and call it
  recovery. Restore the checkpoint or run bounded chain recovery/backfill into
  isolated paths before promotion.
- **Shutdown longer than Kafka retention:** reconcile source positions and run
  bounded recovery even when the checkpoint exists.
- Preserve deterministic event identities, one-writer checkpoint ownership,
  Silver deduplication, Snowflake COPY history/MERGE behavior, completed-range
  ledgers, watermarks, and leases to prevent duplicates.

## Snapshots and cost controls

Snapshot creation and retention are intentionally not automated in 11.6B.
Before 24/7 use, approve a policy with a short retention window and test one
restore. Snapshots are recovery aids, not substitutes for S3 checkpoints or
chain recovery.

Use AWS Budgets and Cost Anomaly Detection in a separately approved billing
change. Suggested notification thresholds are USD 25, 50, and 100, but budget
notifications are delayed alerts rather than hard spending caps. Required
controls are project tags, standard T3 CPU credits, a disabled-by-default stop
schedule, 14-day logs, ECR expiry for untagged images, 72-hour Kafka retention,
and monthly review of EBS, snapshots, public IPv4, ECR, logs, secrets, and S3.

Planning estimate for `us-west-2`, excluding Snowflake and taxes:

The following original 11.6B estimates are preserved as historical planning
figures. Use the refreshed [launch cost review](../../../../docs/aws_demo_launch_approval.md)
and its verified base/usage assumptions for a new approval decision; none of
these figures is an authorized spend cap or a guaranteed bill.

| Schedule | Approximate monthly AWS cost |
|---|---:|
| 40-hour demonstration | USD 24 |
| Four hours per day | USD 43 |
| Continuous 730-hour runtime | USD 168 |

The instance uses standard rather than Unlimited T3 credits to prevent surplus
credit charges. Sustained Spark load may therefore throttle; benchmark before
continuous use and approve a non-burstable instance if required.
