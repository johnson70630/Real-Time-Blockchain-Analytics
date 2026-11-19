# 11.6C follow-up — Retained metadata inspection readiness

**Offline preparation only. No paid start, AWS account access or deployment is
authorized by this report.** The first streaming pilot succeeded and was stopped;
its single-run image security exception remains consumed. Milestone 11.5D remains
paused. This inspection does not execute those images or grant recovery approval.

## A. What was implemented

Added a committed-source, inspection-only guest package, a restricted host AWS CLI
bridge, a separate local approval/window guard and an SSM relay. The guest uses
Python's standard library and the already installed AWS CLI; it installs no SDK,
package or image. It reads two Kafka files while the broker is stopped:
`data/meta.properties` and `data/<pilot-topic>-0/partition.metadata` beneath the
retained mount. Kafka segments are never read or modified.

The S3 bridge permits only fixed pilot metadata directories and metadata keys:
ListObjectsV2, HeadObject and conditional/ranged GetObject. STS verifies the exact
instance-role session. The guest reads the query ID, commit 13, offset log 13 and
sink metadata 13 (or its compacted form), then repeats reads/listings and disk
identity checks. Changed bytes, versions, listings or storage identities fail
closed. The 45-row batch-0 sample is not used as the final offset map.

The result includes hashes, retained identities and checkpoint end offsets.
**Kafka broker watermarks are null; historical identity confirmation, baseline
approval and resume authorization are false.** This is a candidate for review,
not a complete recovery baseline or permission to start a writer.

## B. Why

Recovery needs the retained topic UUID, Spark query ID and last committed offset
map. The accepted pilot did not publish all three. Reading existing metadata
before starting another stream reduces ambiguity without reusing an expired
image exception or exposing the Alchemy secret. A disk identity and a checkpoint
offset are useful evidence, but they do not prove the current broker's retained
offset range or establish that an identity is the original historical identity.

The Kafka parser follows the exact 4.3.2 source formats:
[MetaProperties.java](https://raw.githubusercontent.com/apache/kafka/4.3.2/metadata/src/main/java/org/apache/kafka/metadata/properties/MetaProperties.java),
[PartitionMetadata.java](https://raw.githubusercontent.com/apache/kafka/4.3.2/storage/src/main/java/org/apache/kafka/storage/internals/checkpoint/PartitionMetadata.java).
The integration also compares disk identity with a real broker-observed UUID.

## C. Files changed

- `runtime/inspection.py`: approval, package, boot, storage, timer and inactive-writer guards; bounded candidate collection.
- `runtime/inspection_transport.py`: host AWS CLI bridge and read-only Kafka disk parser.
- `scripts/prepare_runtime_inspection.py`: deterministic committed-source archive and pending command templates.
- `scripts/runtime_inspection_window.py`: separate scope, dependency checks, cloud preflight and external normal-stop supervision.
- `scripts/runtime_inspection_relay.py`: boot-bound SSM staging, controls, one inspection and normal stop on success/failure.
- `tests/test_runtime_inspection.py`, `tests/integration/inspection_disk.py`, test-only Compose and integration driver.
- This report, [sanitized evidence](evidence/aws_runtime_retained_inspection_2026-11-19.json),
  [pending scope](proposals/runtime-retained-metadata-inspection.proposed.json) and a status link.

README, main, production Compose/services, frozen host source, dependency/image
pins, security acceptance/consumption records, Terraform, IAM, Snowflake/dbt and
Streamlit are unchanged. Unrelated duplicate files are preserved.

## D. Tests and validation

**61 focused cases, 927 runtime tests and 36 negative subtests pass** (one existing
websockets deprecation warning).

Focused tests exercise exact approvals, immutable package/source hashes,
reproducible archives, Python 3.9 and shell syntax, private files, duplicate JSON,
metadata limits, changed versions/bytes, disk symlinks, active applications,
wrong volumes/filesystems, fixed timer deadlines, single-use intents, late starts,
reboots, expiry and mocked SSM failure cleanup. Fake AWS calls verify exact reads
and a normal-stop DryRun; tests never contact an AWS account.

The local integration uses existing images, a unique disposable project, an
internal network, no credentials/provider and read-only module mounts. It stops
Kafka before running a network-disabled disk reader. That reader compares the
retained on-disk topic UUID with the prior real broker observation. Kafka then
restarts for the existing resume/repeat, same-offset topic replacement and
missing-offset tests. Cleanup is restricted to that synthetic project.

On this Mac, two initial broker-readiness attempts failed before Spark started.
The diagnostic attempt showed Kafka STARTED while health probes timed out. A
private 30-second probe override subsequently passed with a real topic-list
response after about 26 seconds; the committed 10-second health setting and all
production settings remain unchanged. Exact final outcomes/counts and any further
local limitations are recorded in the linked evidence and final checkpoint.
Hosted CI runs the committed test configuration on standard Ubuntu runners.

The full local attempt subsequently exceeded its 300-second outer timeout in
the initial Spark phase; it did not reach the new disk phase. Compose cleanup
returned 0 but briefly reported a one-off run's volume/network in use. A separate
owned-project cleanup/readback verified zero remaining containers, volumes and
networks. A targeted real-broker/stopped-disk test validates the new parser
separately; hosted CI must validate the complete committed recovery sequence.

**The targeted test passed:** an internal Python Kafka client observed a real
cluster/topic UUID; after broker stop, the network-disabled read-only reader
matched the on-disk UUID. No Spark engine was started. That private test disabled
the repeated emulated Java probe and required a bounded real Kafka protocol
response instead. The committed configuration was unchanged. A prior host-port
assumption was rejected before a protocol probe; the corrected test ran inside
the internal network. All attempt directories are preserved. The successful
project's cleanup independently verified zero containers, volumes and networks.

## E. Security and correctness review

The archive is built from committed Git blobs, not arbitrary working-tree files.
It contains six Python files plus a manifest. Pending templates carry no active
approval. A future owner record must bind the exact source, archive, manifest,
local operator sources and all five package artifacts. Old streaming approval,
missing approval or changed sources fail before SDK/credential access.

The guest requires root, exact IMDSv2 instance identity/current boot, no swap,
tmpfs scratch, the original release hash, exact XFS UUID and EBS NVMe serial,
inactive application units and no running containers. It shares the existing
writer lock, reserves exclusive intents and never starts containers. Known
stopped containers are allowed; Compose restart policies remain `no`, and the
accepted candidate unit was manual/transient rather than enabled on boot.

The AWS CLI subprocess has a clean environment, no inherited profile/static
credentials/custom endpoints/proxies, private bounded output files, timeouts,
file-size/core limits and one attempt. It obtains the existing instance role
through IMDS. Only selected metadata and version hashes leave the guest; provider
errors, credentials, secret payloads and shell transcripts are suppressed.
The role itself retains its existing broader application permissions; this code
restricts the operations it invokes. This is not a new IAM boundary or a sandbox
against arbitrary root code. No permission expansion is proposed.

SSM and transient shutdown controls are privileged guest changes and require the
separate approval below. Passing offline tests does not validate a live boot,
tool availability, IAM permission, network reachability or actual shutdown timer.
An unknown guest CLI/version/layout or missing permission stops inspection;
there is no installation, policy edit, secret edit or storage repair fallback.

## F. Git and CI

Starting develop: `c895ab1d0c127c9d9dfa28136bc0a86bb4ecf854`;
[CI 44468826322](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/44468826322)
passed all six jobs. Commit only after local security/correctness review, then
push develop and observe the new six-job CI. Build the actual pending archive
from that clean committed source twice and verify reproducibility. The final
commit, CI URL and exact archive hash belong in the completion receipt and
conversation, avoiding a circular self-referencing package hash in this file.
Main remains `21be18e45791a4199830754b80620d7d6599587e`.

## G. Cost and exact proposed resources

Offline work adds no AWS resources or runtime hours. Public pricing/source
downloads are unauthenticated; no account API is called. Retained resources may
continue charging while compute is stopped.

One proposed inspection uses the **existing** resources:

| Resource | Exact target |
| --- | --- |
| Account / region / AZ | `209479274979` / `us-west-2` / `us-west-2a` |
| EC2 | `i-0ca1671a59751acc5`, t3.xlarge, standard CPU credits |
| Root EBS | `vol-03bd5d26947719f98`, encrypted 30 GiB gp3 |
| Kafka EBS | `vol-02c9fb14a9c568fb5`, encrypted 100 GiB gp3, `/dev/sdf`, retained on termination |
| Kafka mount / XFS UUID | `/var/lib/blockchain-runtime/kafka` / `da1454d8-1bea-41a3-89d7-87bd570a47bd` |
| Kafka cluster ID | `vLEW3m8cQNyTdif0Nm5U2g` |
| Instance role/profile | `blockchain-runtime-ec2` |
| SSM document | `blockchain-runtime-demo-shell`, private version 1 |
| S3 bucket | `johnson-blockchain-analytics-prod` |
| Pilot root | `blockchain-analytics/pilot/runtime/11-6c-v1` |

The public Oregon Linux/shared on-demand t3.xlarge price refreshed November 19,
2026 is **USD 0.1664/hour**, from the
[regional AWS price catalog](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/us-west-2/index.csv),
SKU `3ZD39ECB23FRWNNP`. Add
[USD 0.005/hour for an in-use public IPv4 address](https://aws.amazon.com/vpc/pricing/).
Fifteen minutes at USD 0.1714/hour is **USD 0.04285**, approximately 4.3 cents,
excluding retained EBS, S3 requests, other services, tax and other charges. This is
an estimate, not a spending cap or guaranteed bill. Normal stop is due earlier,
at ten minutes. Stop controls can fail; manual Console supervision remains required.

**Terraform changes: none. Plan/apply/import/backend scope: none. New AWS
resources: none.** Runtime and platform states remain separate and untouched.
ECR image digests are not inputs to this inspection: no pull, login, image build
or container execution occurs. Existing published digests and their consumed
exception remain historical in the accepted pilot report.

## H. Dependencies and remaining blockers

Production approval is still absent. After approval, require a fresh private MFA
login to `blockchain-demo-deploy`, exact deployer identity and enough remaining
credential time. Local preflight requires AWS CLI v2 and the reviewed SSM plugin
1.2.896.0. No MFA code or credential is sent to chat.

Existing IAM must permit EC2 configuration/volume/security-group/credit reads,
normal StopInstances on this instance, private SSM document reads/StartSession
and the session channel. Stop permission is checked with DryRun while stopped.
The existing instance role must permit STS identity, ListBucket on the pilot
metadata prefixes and GetObject on those metadata keys. No new policy is added.
The Alchemy secret ARN/value is not required or accessed.

The host AWS CLI, expected Kafka metadata layout and retained batch 13 still
need live confirmation. The last known stopped observation is historical; this
offline checkpoint provides no fresh AWS-state readback. Original historical
topic/query identity and broker offset availability remain recovery blockers
even if inspection succeeds. A subsequent stream requires separate security,
baseline and deployment approval; the consumed exception cannot be reused.

## I. Proposed startup, smoke checks, shutdown and rollback

1. Bind one owner approval to the final clean source and package. Keep the
   existing execution markers untouched; use the separate inspection marker.
2. Verify identity, credentials, exact stopped instance/disks, zero ingress,
   standard credits, stop-on-guest-shutdown and unchanged SSM document. Verify
   stop permission with DryRun. Arm the independent external watcher while stopped.
3. Permit one **manual** Console start within five minutes of arming. A late or
   changed boot is refused and normally stopped when running. There is no Start
   API in these tools. Revalidate configuration and require SSM availability.
4. Via the reviewed SSM document, stage only the hash-bound package under its
   unique `/run/blockchain-retained-inspection-<archive-prefix>` directory.
   Do not change `/opt`, install software or overwrite existing artifacts.
5. Verify boot/storage/idle-writer guards and arm a single absolute transient
   poweroff timer for launch +12 minutes. Confirm the timer's actual deadline.
   Controls must be confirmed by +5 minutes or the external watcher stops EC2.
6. Run at most one metadata inspection. Smoke acceptance requires stable reads,
   expected batch 13, exact disk/role/boot identities, zero running containers,
   zero secret reads/writes and explicitly unapproved candidate flags. Preserve
   hashes and metadata-only receipts locally. No Kafka/Spark recovery is executed.
7. The relay requests normal StopInstances on success or failure. The independent
   watcher requests normal stop by +10 minutes regardless of inspection progress;
   guest poweroff is a backup at +12. At +15, require stopped confirmation or
   immediate supervised Console normal stop/escalation. These deadlines do not
   guarantee an upper billing bound if AWS/network/shutdown controls fail.
8. Confirm EC2 stopped, public IPv4 released and both encrypted disks retained;
   consume the one-window authorization. Review private refusal receipts if any.

Rollback means stop inspection/session and normally stop EC2. No state, data,
checkpoint, log volume, production unit or original source is rolled back or
deleted. The staged package/scratch/transient timer disappear with `/run` at
shutdown. The shared host lock may remain as an empty lock file; no log data is
changed. EBS **was already initialized**: there is no one-time initialization
step in this scope and absolutely no `mkfs`, `wipefs`, reformat or repair.

CloudWatch receives no new application container logs because containers stay
stopped. Existing SSM/account telemetry may continue under current settings.
No logging retention or destination is modified. After the window, inspect
billing/cost records when available; retained EBS, ECR, Secrets Manager and log
retention charges may continue. No resource deletion or broader cleanup is
authorized. Any such proposal requires its own exact approval.

## J. Whether production infrastructure changed

**No.** This checkpoint is source, tests, public-source/pricing verification and
local synthetic validation. It performs zero authenticated AWS account calls,
paid starts, SSM sessions, Terraform actions, IAM changes, secret reads/edits or
production S3 writes. The pending package is not deployed. The next approval is
only the concrete one-window inspection described above, not another stream.
