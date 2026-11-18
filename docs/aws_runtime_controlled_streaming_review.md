# Milestone 11.6C — Controlled Arbitrum streaming window, offline deployment review

**Offline preparation. This is a new live scope, not the completed one-read
approval. No EC2 start or deployment occurred during this work.** Keep the
existing secret unchanged. [The format gate already passed](aws_runtime_reader_probe_result.md).

## A. What was implemented

Added a [guest operator](../runtime/pilot_operator.py),
[committed-source packager](../scripts/prepare_runtime_pilot_package.py),
[scope-bound SSM relay](../scripts/runtime_pilot_relay.py),
[bounded public sample reader](../runtime/pilot_sample.py) and
[local Parquet acceptance checks](../runtime/pilot_acceptance.py).
Their imports are inert. Generated commands are proposed assets, not executed
by packaging. The relay rejects the consumed format-only approval before AWS
client creation. A new explicit streaming/install/write approval is required.

The operator has a fixed sequence: controls, install, prepare, launch, status,
sample, journal and stop. It verifies committed package hashes, the frozen
candidate, unchanged release, original host files/units, exact EC2 identity,
current Linux boot, retained Kafka mount, tmpfs, swap disabled and active timers.
Installation writes a separate versioned candidate and stages its manual service;
it never overwrites the legacy installation or starts/enables a service.

Prepare requires no existing containers, an empty pilot prefix, an unexpired
accepted image scope and exact amd64 manifest/config identities. It retrieves
temporary registry authentication in memory, removes its tmpfs configuration in
finally, and runs the corrected standalone chain helper. Only verified Arbitrum
chain ID **42161** permits a prepared receipt. No prepared receipt means no launch.
Exclusive prepare/controller intent files prevent automatic retries. The service
independently takes the existing single-writer checkpoint lock.

An asynchronous systemctl start is only a request. The relay marks the controller
started only after the current-boot journal marker, all three running containers,
Kafka/Spark health, active service and fresh Spark heartbeat agree. Failure of a
gate or relay exit requests normal EC2 stop, with independent watcher/timer
fallbacks. No exception/provider text or terminal transcript is forwarded.

## B. Why and shutdown timing decision

The original candidate service powered off immediately on exit, racing collection
of the final drain evidence. Its uninstalled unit template now allows **480
seconds** for stop and adds a **45-second post-stop evidence window**, followed
by normal guest poweroff. The frozen nine-file application candidate is unchanged;
this unit is rendered by the separately reviewed installer sidecar.

A second absolute transient timer requests application stop at launch +20
minutes. Producer stop, Spark checkpoint drain and Kafka shutdown have bounded
CLI timeouts totaling 390 seconds (90 + 195 + 105), followed by the 45-second
observation window. The 480-second systemd stop timeout controls service-stop
handling; it is not a whole-window or billing cap. The independent timers remain
the operating limits. An application stop at +20 leaves margin before external
normal stop at +30. Neither timer installation nor later inspection extends the
original launch deadlines. These stop/post-stop semantics follow the
[systemd service documentation](https://www.freedesktop.org/software/systemd/man/latest/systemd.service.html#TimeoutStopSec=).
The completed format probe did not install this service.

| Deadline from actual launch | Action |
|---|---|
| Before +15 minutes | Finish setup and confirm a real controller start; otherwise normal-stop abort |
| No later than +20 | Absolute guest timer requests graceful application stop; stop earlier after acceptance |
| During stop | Verify Spark drained/final checkpoint, Kafka/producer/Spark exit states and journal marker |
| After main process exit | 45-second evidence window, then guest requests normal poweroff |
| +30 | External watcher requests normal EC2 stop |
| +35 | Independent absolute guest poweroff timer |
| +40 / +45 | Target stopped / outer supervision bound, not a billing guarantee |

The original manual +150-minute timer is started as a last fallback because the
frozen host preflight requires it. The earlier timers/watch govern this window.
No timers or application services are enabled on boot.

## C. Files and exact proposed deployment scope

[Sanitized evidence](evidence/aws_runtime_controlled_streaming_review_2026-11-18.json)
records source hashes, timing and the unapproved proposal.

New runtime modules, packager/relay and four focused test modules are listed
above. Updated only the candidate unit renderer and its tests; added this review,
sanitized evidence and pilot/result status links. README, Terraform, dependency
locks, producer/Spark/container source, Compose, published images and all original
installed files are preserved.

| Existing resource / guest asset | Proposed action after new approval |
|---|---|
| EC2 `i-0ca1671a59751acc5` | One manual start, t3.xlarge in us-west-2a; standard CPU credits; normal stop |
| Root `vol-03bd5d26947719f98` | Retain encrypted 30 GiB gp3; add only versioned candidate application files |
| Kafka `vol-02c9fb14a9c568fb5` | Retain encrypted 100 GiB gp3; persistent broker logs under the existing mount |
| Guest staging | New root-only `/run/blockchain-pilot-operator-<archive-digest>` tmpfs directory |
| Candidate directory | `/opt/blockchain-runtime-candidates/ae678174072b2484e2c095593f3363adf822904e` |
| Manual service | `/run/systemd/system/blockchain-pilot-reader-candidate.service`; daemon-reload, one start, graceful stop |
| Timers | Existing manual fallback plus two new transient absolute application-stop/poweroff timers |
| Docker | One short, removed chain-validation container; then one controller attempt with Kafka, topic-init, producer and Spark |
| S3 | Pipeline objects only in the isolated Bronze/checkpoint namespace below |
| CloudWatch | Container streams/events under existing `/blockchain/runtime/containers`; tag `pilot/<name>/<id>` |
| Secret | Existing unchanged ARN read by the chain helper and controller; no edit/rotation |
| New infrastructure | No EC2/volume, IAM, VPC, ECR repository, secret or log group creation |
| Terraform | **No changes, backend initialization, authenticated plan, apply or import** |

Workload role/profile remains `blockchain-runtime-ec2`; operator access remains the
existing MFA-backed `blockchain-runtime-deployer` role and private SSM document
`blockchain-runtime-demo-shell` version 1. No ingress, SSH, NAT Gateway or ALB.
Fresh private MFA must have at least 90 minutes remaining. Arm the reviewed
watcher before showing a new five-minute manual start cutoff. Late launches
cannot enter SSM/application tooling and receive normal stop.

The [one-time EBS initialization](aws_runtime_ebs_initialization_result.md) is
already complete. Verify the exact retained volume/serial, XFS UUID
`da1454d8-1bea-41a3-89d7-87bd570a47bd` and mount
`/var/lib/blockchain-runtime/kafka`. Never run mkfs/wipefs or reset it. An
uninitialized replacement must fail closed with `EBS_INITIALIZATION_REQUIRED`
and needs a separate exact-volume procedure/approval. Kafka KRaft log metadata
initialization inside its existing filesystem is distinct from formatting EBS;
preserve the release's persistent cluster/node identity.

Runtime/platform states remain `production/runtime/terraform.tfstate` and
`production/platform/terraform.tfstate`, separate and untouched.

## D. Tests and data acceptance

736 runtime tests and 36 negative subtests pass locally, with the existing
websockets deprecation warning. Ruff, Python 3.9 syntax, shell syntax, secret
scanning and source/evidence binding checks apply before checkpointing. Tests
exercise failed authentication, wrong image configuration, nonempty pilot paths,
duplicate attempts, stale boot receipts, immutable timers, legacy-file/units
preservation, requested-versus-confirmed startup and final drain/exit evidence.

Synthetic real Parquet tests verify the entire bounded S3-sample/local-validation
flow, compacted sink metadata, retained checkpoint logs, hash tampering, offset
gaps/duplicates, wrong chains/partitions, malformed identities, stale timestamps
and oversized objects. The sample contains a committed positive batch/range,
public transaction/log identities and offset provenance. Metadata/Parquet reads
are bounded to 64 MiB total, 32 files, 16 MiB per part, one million rows and
256 MiB uncompressed; no source rows are printed or stored by the relay.

Live smoke acceptance requires the chain gate, healthy broker/Spark, running
producer, fresh Spark heartbeat and a positive committed Bronze sample with a
complete Kafka offset range, correct Arbitrum/Uniswap labels and readable Parquet.
Bronze transaction/log duplicates are counted, not silently removed: append-only
Kafka-offset correctness does not promise blockchain-event uniqueness or complete
WebSocket history. Reconnects remain gap candidates requiring reconciliation.

Final acceptance requires `PILOT_GRACEFUL_STOP_VERIFIED`, Spark drained plus a
verified checkpoint batch, producer/Spark exit 0, Kafka exit 0 or 143, no OOM,
then independently stopped EC2/no IPv4/original disks retained. Collect CloudWatch
stream metadata and safe markers through existing operator read permissions;
do not dump container logs/provider payloads.

Kafka persistence/Spark checkpoint restart correctness stays covered by offline
integration CI. This one controller attempt does not authorize a live crash,
restart, checkpoint reset or broker recovery experiment. Failed acceptance must
be reported honestly with data/logs/checkpoints retained for later review.

## E. Security and dependencies

Host operator dependencies are existing Linux Python 3.9 stdlib, AWS CLI, Docker,
Compose and systemd. Local acceptance uses the existing locked PyArrow dependency.
No new dependency, credential, image build/publication, IAM expansion or Snowflake
change is needed. Live preflight must reverify workload trust/boundary/secret policy,
SSM document, image manifests/configurations/full finding inventories and stop access.
Read-only log checks must fail safely if denied; never expand IAM automatically.

Secret ARN remains
`arn:aws:secretsmanager:us-west-2:209479274979:secret:blockchain-runtime/11-6c/alchemy-websocket-url-d44flr`.
The stored JSON representation is decoded by the shared reviewed reader. The
validation client receives the raw URL only through stdin; the producer receives
its existing UID 10001/mode 0400 tmpfs secret file. Values, value hashes, keys,
credentials and provider responses never enter Git, argv, environment or receipts.
An authentication failure does not authorize a blind secret edit.

Use the existing conditional one-run image exception, expiring
**2026-11-24 00:00:00 UTC**. It retains 41 advisory IDs and uncertainty; this work
does not patch them. Reserve its single demonstration attempt before invoking
image validation; once quarantined images run, no automatic second attempt is
allowed even if streaming never starts. The completed format-only window ran no
containers and consumed none of this exception.

Frozen host source `ae678174072b2484e2c095593f3363adf822904e`, bundle SHA-256
`568d0fc889ca7139aefa651d6d5dccea90ac2bc2106130d8d6451e736b3c9143` and release
SHA-256 `7f948e94cec7f7aebaf2aa4dab4ddd8e92a60f083f17bb9cefcff4db89cd200f` stay fixed.
Only the sidecar operator/installer and manual service timing differ. Exact images:

- Producer: `209479274979.dkr.ecr.us-west-2.amazonaws.com/blockchain-runtime-producer@sha256:63d9d0f24a1cb7eb30b080e58f8a40ddd49292b89020328409e6f45319597afd`
- Spark: `209479274979.dkr.ecr.us-west-2.amazonaws.com/blockchain-runtime-spark@sha256:bb03f7d9917f2d8dc7d2fc1fc6a1df83fe876f5298e98c4163c689a1b890a134`
- Kafka: `209479274979.dkr.ecr.us-west-2.amazonaws.com/blockchain-runtime-producer@sha256:1113a10e8a93fc12b2d310ef6f53b8f5fc95fb806a8c6edcfa6b67d17959378b`

S3 paths remain:
`s3://johnson-blockchain-analytics-prod/blockchain-analytics/pilot/runtime/11-6c-v1/bronze/swaps/`
and the same root plus `checkpoints/swaps_bronze/`. Prepare refuses any existing
object under the pilot root. Spark may create/update/rename/remove its own
temporary, sink-log and checkpoint metadata there as normal commit/retention
work; there is no operator deletion, prefix reset or production-lake cleanup.

## F. Git and CI

Started on develop at `8a30e4dabcb97ed4bc3ac77260dd4809d9de3b98`;
[CI 44418764927](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/44418764927)
was rechecked: all six jobs passed. Commit only reviewed source/tests/reports,
then generate the 20-member operator archive from immutable committed blobs and
the verified candidate/legacy archives/release. Record its exact digest, private
command-file digests and resulting CI metadata in the final checkpoint receipt.
Generated proposals remain owner_approved false. Observe new develop CI; never
merge main. Unrelated duplicate files and previous artifacts remain preserved.

## G. Cost implications

No AWS API, authentication refresh, start or deployment was performed during this
offline work. Existing retained resources continue billing. At the previously
recorded USD 0.1714/hour EC2/IPv4 rate, 30 minutes is about USD 0.0857 and the
45-minute supervision interval USD 0.12855. These historical-rate estimates are
not spending caps or a guaranteed bill. Add retained EBS/ECR, S3 requests/storage,
CloudWatch ingestion/storage, transfer, provider quota/charges, tax and any other
usage. No new monthly budget or continuous-runtime authorization is implied.

Stop earlier after the first valid sample and drain evidence. Normal-stop requests
and timers cannot guarantee an exact stopped time; independently verify stopped,
IPv4 released and retained disks. Do not extend the window to chase data or debug
a failed gate. No wallet signing, token transfer, trading or customer data.

## H. Remaining blockers

New explicit live approval is required for the exact package installation,
validation container/RPC, one controller attempt, scoped S3/log writes and normal
shutdown. The owner's `approve` after the result authorized continuing preparation;
no concrete new package/digest or full write scope existed at that point. This
review makes those actions concrete before the production approval gate.

Actual provider authentication, chain identity, logging and streaming/data/drain
success remain unverified. Fresh stopped/running cloud checks and MFA margin are
required only after approval. No secret editing or new IAM policy is a prerequisite.

## I. Next step and rollback

After the checkpoint/package/CI pass, request one new conditional pilot approval.
Create a new private execution directory with source/package hashes and explicit
stream/install/write flags; do not reactivate the consumed one-read authorization.
Refresh private MFA, revalidate stopped resources, detach the watcher and show a
new actual five-minute cutoff before the owner's single manual start.

Recheck the accepted running boot and cloud controls before SSM. Upload the exact
digest-verified package, establish both immutable timers, install/verify the
versioned candidate, then run prepare. On a chain/image/prefix failure, stop
normally and preserve the result. On success, launch once; confirm actual startup,
collect a bounded positive committed sample, request graceful stop early, verify
drain during the post-stop window, and independently verify EC2 shutdown/disks/cost.
The +20 timer is a fallback, not a reason to keep an accepted demo running.

Rollback means stop, preserve EBS/Kafka/S3/checkpoints/evidence and leave the
original installation untouched. Do not automatically restart the legacy service,
delete containers/volumes with `down -v`, reset offsets, format storage or remove
pilot objects. The additional candidate files can stay for inspection. Any cloud
resource deletion or later recovery/restart requires a separate reviewed approval.
Retained disks/images/logs continue their usual charges after stopping.

## J. Whether production infrastructure changed

No changes in this offline checkpoint. No AWS API calls, EC2 starts, credentials
refreshes, guest commands, image publication, Terraform or Snowflake actions.
The last independent readback after the format-only window confirmed stopped,
IPv4 released and both original disks attached; this work does not claim a new
cloud readback. Milestone 11.6C is open; 11.5D remains paused.
