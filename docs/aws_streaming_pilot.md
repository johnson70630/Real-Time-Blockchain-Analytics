# Milestone 11.6C — first streaming pilot readiness

Status: offline implementation complete. The first infrastructure-only paid
apply and normal stop were separately approved and executed. **The apply
partially completed: 23 managed resources exist; EC2 is stopped and tainted.**
Five resources remain uncreated. No streaming application has been deployed.
See the [actual apply/stop and recovery review](aws_runtime_first_apply_review.md).
The historical fresh plan proposed 28 creates with zero updates/deletes/imports;
it is now a consumed, partially executed artifact and must not be reapplied.
See the
[completed review](aws_runtime_boundary_plan_review.md) and retained
[approval proposal](aws_runtime_boundary_plan_approval.md).
Milestone 11.5D remains paused. Recovery state changes/planning, further applies,
IAM changes, EC2 start/replacement, EBS initialization, Snowflake operations and
main merge remain separate gates. README is unchanged. See the
[completed runtime plan review](aws_runtime_plan_review.md) and subsequent
[cost, provisioning-permission and launch review](aws_demo_launch_approval.md).

The subsequent [dedicated deployer setup review](aws_demo_deployer_setup.md)
records MFA-protected temporary deployment credentials and an EC2 role boundary.
The operator completed that IAM setup and confirmed the role settings; the
boundary policy readback independently matches the repository. The older saved
infrastructure plan omits that boundary and must not be used for this design.
Its replacement includes the exact boundary. SSM operator permissions and
secret access remain later gates.

Current milestone: **11.6C, first AWS streaming demo preparation**. Offline
application/deployment components are implemented and tested. The planning-user
managed grant and MFA enrollment are verified; the role and temporary profiles
resolve to the expected principal in account 209479274979. Read-only preflight
found no runtime name collisions, a matching AMI and sufficient inventory-based
vCPU headroom. The approved apply persisted partial runtime state and released
its lock. Normal EC2 stop succeeded; no streaming containers were deployed.

The [first-launch window review](aws_runtime_first_launch_window.md) contains
fresh pricing, reviewed stop scope and the remaining administrator checks.
Its supervised infrastructure-only window was approved and closed after the
partial apply and verified stop.
IAM Console validation is now operator-confirmed with no unresolved findings;
the Organizations introduction screen indicates a standalone account. The
state bucket has no bucket policy. The next proposed gate is an exact-instance
untaint of runtime state plus a fresh saved recovery plan, with no apply/start.
Remaining sequence:

1. IAM role/policy setup and temporary-profile authentication are complete.
2. The approved fresh runtime-only init/plan and boundary review are complete.
3. Obtain specific untaint-and-plan approval for the stopped instance; review
   the remaining five creates before any further apply. No automatic retry,
   replacement or IAM expansion is authorized.
4. Approve initialization of the exact Kafka EBS volume, image publication,
   secret/read permissions and application deployment before those actions.
5. Smoke-test the isolated streaming path, test Kafka/Spark recovery, drain and
   stop gracefully, and verify retained-resource costs and cleanup scope.

The initial target remains a scheduled demo around 40 runtime hours/month;
the cost estimate is not a spending authorization or guaranteed bill.
Milestone 11.5D remains paused. The
[MFA outcome report](aws_demo_mfa_api_enrollment.md) records the successful
attachment and the helper's separate local reporting problem.

## Implementation plan and decisions

1. Verify develop, origin, and hosted CI; inspect the 11.6B foundation.
2. Add opt-in cloud images, release contracts, Compose, secret injection, and
   bounded operating procedures without changing local Compose defaults.
3. Confirm Kafka delivery, reuse the canonical Bronze transformation, enforce
   one checkpoint owner, and fail when source offsets or checkpoint data vanish.
4. Build images locally, test synthetic Kafka/Spark recovery, run regression
   and secret checks, review security/data correctness, then checkpoint develop.
5. Observe hosted CI; stop at the production approval boundary.

The application is Alchemy Arbitrum Uniswap V3 logs → one producer → one Kafka
broker → one Java 17 / PySpark 4.0.1 `local[2]` writer → isolated S3 Parquet and
checkpoints. The producer uses the existing normalization/dispatch code with an
optional Kafka factory; the default local producer and root Compose are intact.
No warehouse, dbt, dashboard, or existing analytical paths are involved.

Kafka has one partition, replication factor one, 72-hour retention, and a
20-GiB per-partition retention bound. This is durable single-node storage, not
high availability. T3 standard credits bound surplus charges but can throttle.
Spark is capped at 5,000 offsets per microbatch, a 3-GiB driver heap, and a
7-GiB container limit. Kafka is capped at 3 GiB and the producer at 1 GiB,
leaving host memory on the provisional 16-GiB instance. Spark temporary storage
is a 2-GiB tmpfs; benchmark spill pressure before increasing workloads. Spark's
tmpfs permits native-library execution because Snappy extracts its bundled
library there; the application filesystem remains read-only.

## Proposed resources and exact Terraform scope

The initial offline checkpoint changed no Terraform source. The subsequent
infrastructure-only planning change makes `approved_secret_arns` optional,
omits its IAM statement when empty, and rejects wildcard/wrong-account/Region
ARNs. It adds no resource definitions. Use only
`infra/terraform/environments/runtime/`. The completed authenticated plan
proposes **28 creates, 0 updates, 0 deletes**, plus the EC2-managed encrypted
30-GiB gp3 root volume. Nothing has been applied:

| Category | Exact resource addresses / configuration |
|---|---|
| Compute | `aws_instance.runtime`: one t3.xlarge, AL2023 x86_64, us-west-2a, IMDSv2, standard credits, stop on guest shutdown, termination protection; `aws_volume_attachment.kafka` |
| Persistent Kafka storage | `aws_ebs_volume.kafka`: 100-GiB encrypted gp3, 3,000 IOPS, 125 MiB/s, same AZ, prevent_destroy |
| Network | `aws_vpc.runtime`, `aws_internet_gateway.runtime`, `aws_subnet.public`, `aws_route_table.public`, `aws_route_table_association.public`, `aws_security_group.runtime`, and egress rules `https`, `dns_udp`, `dns_tcp`, `time_sync` |
| ECR | `aws_ecr_repository.producer`, `aws_ecr_repository.spark`, and their two `aws_ecr_lifecycle_policy` resources; immutable tags, scan on push |
| Runtime identity | `aws_iam_role.runtime`, `aws_iam_instance_profile.runtime`, `aws_iam_role_policy_attachment.ssm`, `aws_iam_role_policy.runtime` |
| Logs and alerts | `aws_cloudwatch_log_group.runtime`, `aws_sns_topic.runtime_alerts`, `aws_sns_topic_policy.runtime_alerts`, alarms `instance_status` and `high_cpu`, `aws_cloudwatch_event_rule.runtime_state`, `aws_cloudwatch_event_target.runtime_state` |

VPC `10.46.0.0/24`; subnet `10.46.0.0/26`; no ingress, SSH key, Elastic IP, NAT
Gateway, ALB, or public Kafka port. Ephemeral public IPv4 provides outbound TLS.
SNS has no subscriber in this foundation: delivery to an operator is a separate
readiness item, not a working notification claim.

Keep `enable_scheduled_shutdown=false`: the three optional Scheduler/IAM
resources are excluded from the 28. The host's manually started systemd timer
provides a separate emergency stop without expanding the instance IAM role.

The approved preflight found no runtime state object and verified the backend
key `production/runtime/terraform.tfstate` with native S3 locking. Following
separate user-approved public-AMI and lock-object permission additions, the
authenticated plan completed as `blockchain-de-local` (`AWS_PROFILE=blockchain-de`).
The public AMI is `ami-0dac6cd09676c9fb1`; the plan was reviewed successfully.
The runtime state and lock objects were absent after the plan, and full logs
remain restricted. Never use or migrate
`production/platform/terraform.tfstate`. Stop on any existing-platform change,
replacement, unexpected destroy, different region/AZ/type, privilege widening,
or unreviewed charge. Preserve all 43 platform resources and 15 adopted grants.
Do not print raw state, full plans, approved secret ARNs, or external IDs.

Applying an empty runtime root launches EC2 and starts billing immediately.
An apply approval must therefore explicitly include initial instance launch.
The foundation's SSM AMI lookup resolves at plan time; capture/review the exact
AMI, Compose binary release and checksum before approving apply.

## Image identities and supply chain

Build locally with `bash scripts/build_runtime_images.sh`. It does not push or
log in to AWS. Build-time access to public image/package/Maven registries is
required; the integration tests themselves use synthetic data on a private
Docker network. The Spark classpath smoke runs with `--network none`.

| Artifact | Pin / publication status |
|---|---|
| Python base, linux/amd64 | `python:3.12-slim-bookworm@sha256:0f086103b026048cc50cb5da25ec435c023c5241a83fc44daea593377abc61c4` |
| Official Kafka 3.9.1 multi-platform source | `apache/kafka@sha256:4ceccc577f03f51f6af8dbfda55194d0d892f4fa7913ffbded567ce3895622ed` |
| Producer ECR deployment digest | **Unavailable: no ECR publication was authorized or performed** |
| Spark ECR deployment digest | **Unavailable: no ECR publication was authorized or performed** |
| Mirrored Kafka ECR deployment digest | **Unavailable: no ECR publication was authorized or performed** |

Mirror the reviewed **amd64** Kafka image into the existing
`blockchain-runtime-producer` repository under an immutable `kafka-3.9.1-...`
tag. This avoids adding a third repository or IAM scope. Keep producer tags
`producer-<commit>` and Spark tags `spark-<commit>`. Tags aid inspection;
deployment must use the actual ECR-returned `repository@sha256:...` digest.
Do not substitute local image IDs or invent ECR digests. Record source SHA,
platform, build inputs, scan results and ECR digest in the release review.
The controller rejects mutable, wrong-account, wrong-repository and non-amd64
images. ECR publication and any new repositories require explicit approval.

Python requirements are frozen, hash-bearing subsets exported from uv.lock.
Spark includes Hadoop 3.4.1, hadoop-aws 3.4.1, AWS SDK v2 bundle 2.24.6, Kafka
connector 4.0.1/Scala 2.13, and transitive dependencies. `jars.lock.json` records
SHA-512 hashes; the build verifies them and rejects conflicting bundled JARs.
No Maven resolution occurs at application startup. Java 17 OS packages are
installed at build time; rebuilt image contents can change with Debian security
updates, so the final image digest is the deployed identity. Scan the complete
images before publication; a successful build is not a vulnerability approval.

See [Spark 4.0.1 compatibility](https://spark.apache.org/docs/4.0.1/) and the
[Kafka connector contract](https://spark.apache.org/docs/4.0.1/streaming/structured-streaming-kafka-integration.html).

## IAM, secrets, and host prerequisites

The existing instance-role policy already covers pilot-only S3 reads/writes,
two ECR repositories, the log group, and SSM. Secret-read access is now omitted
by default (`approved_secret_arns=[]`); no secret exists yet. Enabling an exact
secret ARN later requires separate permission-change approval. No IAM
change was applied. The state bucket has an explicit deny. No Snowflake
credentials or privileges are needed. All containers share the EC2 trust
boundary through IMDSv2 with response hop limit two; this is not per-container
IAM isolation. Never mount the Docker socket or run unreviewed code/images.

Before deployment, identify an existing approved Secrets Manager ARN or obtain
separate approval to create one containing **only the WSS endpoint string**.
Do not place the value in Terraform, tfvars, environment files, CLI arguments,
shell history, user data, images, Git, or logs. Use a protected provisioning
channel. The existing policy supplies no custom-KMS decrypt permission; use a
compatible approved secret or separately review a precisely scoped KMS change.

The root host controller retrieves SecretString with the instance role and
captures output without echoing it. It writes a 0400 file owned by UID 10001
below `/run/blockchain-runtime/secrets/`, verifies `/run` is tmpfs, and rejects
active swap. Only the producer receives the read-only Compose secret mount.
The application reads the URL directly from that file, not its environment.
ECR login uses stdin and a root-only Docker config on the same tmpfs; both files
are removed on shutdown. Root/SSM administrators remain trusted. Disable core
dumps. Do not enable shell tracing or dump process memory/configuration.

[Compose secrets](https://docs.docker.com/compose/how-tos/use-secrets/) are bind
mounts on this standalone runtime; they are not an encrypted secret store.
The host controller sets ownership/mode because Compose cannot remap those
attributes on a bind-mounted file.

Required host dependencies: healthy SSM Agent, working outbound DNS/TLS and
time sync, Docker with awslogs support, checksum-pinned Compose plugin, Python
3.9+ standard library, AWS CLI v2, findmnt/lsblk/blkid, XFS utilities, systemd,
adequate root-disk space for images, and at least 10 GiB free on the Kafka EBS
mount. The foundation installs Docker/XFS/Compose; verify the remaining AL2023
packages rather than assuming bootstrap success. No application starts in user
data. The emergency timer must be active before the controller passes preflight.

Operators need AWS CLI/Session Manager plugin or the console, scoped
`ssm:StartSession`/session-management permissions for the exact instance,
Describe permissions to verify EC2/EBS, and separate approved EC2 start/stop
permissions. The instance profile needs AmazonSSMManagedInstanceCore. No SSH
access is required. The publisher needs separately approved ECR push permissions;
the runtime instance remains pull-only. A Terraform operator must use a separate
identity with backend and approved provisioning permissions.

## EBS first initialization — separate exact-volume approval

The EC2 user-data script remains unchanged and never calls mkfs/wipefs. A blank
volume must still produce `EBS_INITIALIZATION_REQUIRED`. No initialization
script was added to normal startup.

After an approved apply, capture the exact account, region, instance ID,
availability zone, EBS volume ID, attachment, creation time, snapshot origin and
NVMe by-id path. Use independent AWS metadata and read-only `lsblk`, `findmnt`,
`blkid -p`, and `wipefs --no-act` checks. Prove it is a newly allocated blank
data volume, not the root device, not a root partition, not mounted anywhere,
and has no filesystem/partition/signature ambiguity. A missing TYPE is not
proof of blank storage. Obtain approval naming **that exact volume ID** before
one `mkfs.xfs` operation; never use `-f`, wipefs mutation, or a guessed device.

The approval review must supply the concrete command and verified stable device
path. None can be supplied safely before the actual volume exists. After the
approved operation, capture the XFS UUID, add the UUID-based mount, verify serial
and UUID again, and rerun the existing non-destructive mount verification.
Populate `kafka_volume_id` and `kafka_filesystem_uuid` in the release manifest.
The application controller refuses root-filesystem fallback and makes only a
Kafka-owned `data` directory below the already verified EBS mount. Kafka's own
KRaft metadata initialization is distinct from EBS filesystem formatting.

## Isolated S3 contract and recovery semantics

Exact pilot locations:

```text
s3://johnson-blockchain-analytics-prod/blockchain-analytics/pilot/runtime/11-6c-v1/bronze/swaps/
s3://johnson-blockchain-analytics-prod/blockchain-analytics/pilot/runtime/11-6c-v1/checkpoints/swaps_bronze/
```

Spark uses the equivalent `s3a://` URIs. The release and application validate the
exact bucket/prefix/topic/chain. Retain protocol, chain, event_type and
event_date partitions, original JSON, producer/schema versions, Kafka offsets
and processing metadata. Raw Bronze may contain malformed source envelopes;
it preserves evidence for later validation rather than claiming Silver quality.

The cloud stream starts at `earliest` only on a truly new checkpoint; subsequent
starts resume checkpointed offsets. The local stream still uses
`startingOffsets=latest`. Never delete a checkpoint to fix a failure. Either
half of a missing Bronze/checkpoint pair blocks restart. `failOnDataLoss=true`
rejects missing broker offsets. Source gaps after WebSocket reconnects, volume
loss, retention expiry (time **or size**), or long shutdowns require bounded
chain reconciliation/backfill before promotion. Reconnection does not replay
missed Alchemy subscription messages.

The cloud producer resubscribes with exponential backoff and jitter capped at
61 seconds. Each Kafka send uses idempotence/acks=all and confirms delivery in a
bounded flush. An unconfirmed send is logged as recovery-required; source events
can still be lost or repeated across a source reconnect or process crash. This
is not end-to-end exactly-once blockchain ingestion. Kafka offsets and source
event identity make those boundaries inspectable; existing later-stage
deduplication/recovery remains necessary.

A nonblocking flock prevents duplicate writers sharing this host's checkpoint
mount; a second host is prohibited by the release operating contract. This is
not a distributed S3 lease. The original platform warehouse leases are not
repurposed, since the pilot must not touch Snowflake.

## Approved deployment/start procedure (not executed)

1. Runtime-only authenticated planning completed (see the plan review).
   Reconfirm configuration/state/AMI freshness before an apply. Obtain separate apply/launch/spending
   approval, including the first boot; keep a stop deadline during provisioning.
2. Resolve secret provisioning, Compose version/checksum, image publication,
   ECR scans/digests, and exact AMI. Approve any mutations individually. Stop
   the newly created instance promptly when not actively configuring it.
3. Complete the separately approved exact-volume initialization if required;
   verify the EBS mount on every boot. Preserve a generated 22-character Kafka
   cluster ID for every subsequent start with that volume.
4. Transfer only reviewed runtime files/images and a concrete release manifest
   to root-owned `/opt/blockchain-runtime/`. Do not copy credentials or a local
   .env. The example manifest deliberately fails validation. Run
   `python3 -m runtime.host validate /opt/blockchain-runtime/release.json`.
5. Install the three reviewed systemd unit files and reload systemd after
   deployment approval. They have no Install section and must not be enabled
   on boot. Confirm guest shutdown means EC2 **stop**, not termination.
6. Through SSM, start `blockchain-pilot-deadline.timer` first, then
   `blockchain-pilot.service`. A normal release uses 3,600 seconds including
   preparation/startup; permitted manifests range from 300 to 7,200 seconds.
   The independent emergency timer powers off at 150 minutes if normal control
   fails. Bootstrap and time before the timer starts are outside this deadline;
   an operator must monitor them. This is not a hard AWS spending cap.
7. Controller verifies host ID, EBS serial/XFS UUID, Kafka cluster ID, disk,
   tmpfs/swap and timer; injects secrets; pulls approved digests; verifies amd64;
   starts healthy Kafka; creates the explicit topic; starts producer and Spark.
   A stale secret file requires inspection instead of being overwritten.
8. Verify the smoke/acceptance checklist below. Do not declare success merely
   because `compose up` returned. Operator-initiated stop is
   `systemctl stop blockchain-pilot.service`, invoking the same drain path.

## Smoke, recovery, and acceptance checks

Offline gates: Ruff; secret scan; shell syntax; release/identity/secret/lock/
lifecycle contracts; locked image builds; Java 17/Spark 4.0.1/Hadoop 3.4.1 and
S3A/Kafka class loading without network; synthetic Kafka → canonical Bronze
with first-start retained records, abrupt writer-process exit after a committed
batch, broker restart, checkpoint resume, repeat run without duplicate offsets,
and topic deletion/recreation with old offsets rejected. Test volumes and
network belong only to `blockchain-pilot-offline` and are deleted afterward.
CI runs these inside the existing Spark job; Python contracts run in quality.

Additional live acceptance, after approval:

- SSM access works; no SG ingress or published Kafka/Spark ports; the exact EBS
  mount and amd64 image digests match the release; only one checkpoint owner.
- CloudWatch receives Kafka, producer and Spark logs in
  `/blockchain/runtime/containers`; retention is 14 days. Docker's
  [awslogs driver](https://docs.docker.com/engine/logging/drivers/awslogs/) uses
  the instance profile. Nonblocking buffers cap memory and can drop logs under
  sustained delivery failure; logs are not the offset ledger. Verify delivery
  externally; this controller does not add CloudWatch read permissions.
- Observe at least two non-empty committed microbatches and 10 minutes of
  stable processing. Inspect Parquet schema, producer metadata, original JSON,
  partition values and unique `(kafka_partition, kafka_offset)` coverage.
  Compare raw source block/event identities for continuity and reorg effects.
- Check Kafka topic has one partition, RF=1, and intended effective retention
  settings (create --if-not-exists does not rewrite an existing topic).
- Stop producer, drain, compare broker high-water marks to Spark end offsets,
  and inspect S3 checkpoint commits and file-sink metadata. No production object
  prefix or warehouse table may be changed.
- Exercise bounded producer reconnect, Kafka restart with retained EBS logs,
  and single Spark restart with the same checkpoint. Exercise an abrupt Spark
  interruption during a microbatch in the isolated pilot namespace, then verify
  no missing/duplicated Kafka offset identities. Offline tests cover the
  committed-batch crash boundary; in-flight/S3 crash behavior remains a live test.
- Simulate missing checkpoint or expired Kafka positions only in a separately
  approved disposable pilot namespace; never delete production data for a test.
  Verify fail-closed recovery and reject a second concurrent writer.
- Record CPU credit balance, CPU/memory, root/EBS free space, processing latency,
  source gap/reconnect logs, S3 growth/requests, and actual duration. Require no
  OOM, sustained growing backlog, disk threshold breach, or unexplained gaps.

## Graceful stop, emergency stop, rollback, and cleanup

Normal duration expiry, operator SIGTERM, or service-health failure runs the
same sequence: stop producer (75-second grace); require successful flush/exit;
let Spark process all available data; compare its final offsets with Kafka;
require a matching durable checkpoint commit; stop Spark (180-second bound);
stop Kafka last (90-second bound). Failed drain preserves the broker for
inspection rather than reporting success. Release-owned secret/login files are
removed. `ExecStopPost` requests guest poweroff even on failure, stopping EC2
under the foundation's shutdown policy; an interrupted drain then requires
recovery. The independent timer is the fallback if the controller hangs.

Copy nonsecret acceptance evidence to the milestone record before shutdown;
`/run` status disappears on poweroff. Broker logs/KRaft data stay on EBS, while
offsets/commits/file-sink metadata and Parquet remain in S3. Verify AWS reports
`stopped`, not merely that SSM disconnected. An emergency stop is not a graceful
acceptance result. Preserve evidence and reconcile before restarting.

Rollback means stop the pilot, preserve EBS/S3 and checkpoint identity, and use
the last reviewed compatible image digests. Do not roll back across an
incompatible Spark checkpoint/schema/source change. If no prior deployment
exists, rollback is simply a stopped runtime with retained evidence. No
Terraform destroy, force detach, reformat, checkpoint reset or warehouse change
is part of rollback. Any deletion requires separate approval.

Afterward verify EC2 stopped; no detached/orphan EBS, Elastic IP, NAT/ALB or
unexpected runtime exists; EBS retains its volume ID; no start schedule/unit is
enabled; no unapproved containers restart at boot; temporary secrets are gone;
remaining S3/ECR/log/secret/snapshot storage and versioning costs are accounted
for. Review Cost Explorer/project tags after billing data arrives. ECR's existing
policy expires only untagged images; tagged images remain billable until an
approved cleanup. No S3 lifecycle or snapshot policy was introduced.

## Cost and approval boundaries

The inherited 11.6A/11.6B planning estimate is approximately **USD 24/month for
40 runtime hours**, not a spend cap or guarantee. The refreshed
[cost review](aws_demo_launch_approval.md#g-verified-cost-model) verifies a
USD 17.46 base and a USD 19.60 future-streaming example with explicit assumptions
and exclusions. The proposed first
release is one hour including image preparation, plus bounded drain and host
boot/provisioning time; do not authorize a continuous deployment from that
estimate. Storage (130 GiB of gp3), ECR, retained logs, secrets and S3 continue
to accrue costs while EC2 is stopped; public IPv4 applies while allocated.
Snapshots, data transfer, S3 requests/versioned checkpoint churn, monitoring,
and taxes can add charges. Existing Snowflake/Streamlit costs are excluded.

The 2026-11-12 [price snapshot](evidence/aws_runtime_demo_costs_2026-11-12.json)
covers region-specific EC2, gp3, IPv4, ECR, Secrets Manager, S3 and CloudWatch
rates. Recheck it if launch is delayed and review the usage assumptions before
spending approval. AWS publishes separate
[EBS storage pricing](https://aws.amazon.com/ebs/pricing/) and
[IPv4 pricing](https://aws.amazon.com/vpc/pricing/); stopping compute alone does
not eliminate retained-storage charges. No new AWS charges were incurred by
offline implementation; public downloads/local Docker builds consume local
resources and CI image builds add Actions minutes.

## Remaining deployment blockers

- Explicit apply/initial-launch/spending approval and an approved run window;
  no such deployment approval has been given.
- An approved provisioning identity with the required write permissions; the
  successful plan does not test those permissions.
- Review of the refreshed cost assumptions and a plan freshness check. The
  verified AMI and Compose v5.1.3/checksum are recorded in the plan review.
- Exact EC2/EBS IDs, proven blank volume, separate formatting approval and UUID.
- Existing approved secret ARN or separately approved secure provisioning.
- Published/scanned amd64 ECR digests for all three container artifacts.
- Host dependency/timer/SSM checks, optional SNS subscriber approval, and live
  CloudWatch/S3/IMDS validation. Offline tests cannot establish these.
- Live acceptance and in-flight S3 checkpoint/recovery tests after deployment.

Next gated action: review the completed proposal, provisioning-identity scope
and cost assumptions before separately approving an apply/launch window.
Formatting remains a separate exact-volume decision afterward. This is still
a scheduled demo, with no automatic start or continuous-runtime authorization.
