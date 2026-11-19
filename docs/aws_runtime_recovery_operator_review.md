# 11.6C follow-up — Metadata collector and offline recovery operator

**Implemented and tested offline. No AWS call, paid start or deployment is included.**
The first AWS pilot remains accepted; its single-run exception remains consumed.
Milestone 11.5D stays paused.

## A. What was implemented

Added [the bounded metadata collector](../runtime/recovery_metadata.py) and
[recovery preview/operator](../runtime/recovery_operator.py). The collector reads
only Spark query metadata, commit/offset logs and sink metadata. It never reads
Parquet rows, secrets or Terraform state. File and injected-client S3 adapters
share an exact metadata-key allowlist. S3 construction creates no SDK client.
Its only methods are ListObjectsV2, HeadObject and conditional/ranged GetObject.

An explicitly invoked Kafka probe describes the existing cluster/topic and reads
partition-0 watermarks. Its consumer disables topic auto-creation and auto-commit;
it never subscribes, publishes, creates a topic or changes consumer offsets.
Collection checks writer inactivity, resource identity and broker offsets before
and after the read, rereads selected bytes, compares listings and returns hashes
plus a strictly typed snapshot. Limits are 1 MiB per object, 8 MiB aggregate,
1,000 directory entries per listing and a 30-second observation-age check.
Truncated, partial, ambiguous, changed or malformed metadata fails closed.

Preview binds a caller-supplied baseline hash, checks snapshot and metadata hashes,
requires fresh observed evidence, and calls the existing recovery contract. It
returns symbolic steps with `cloud_start_authorized=false`. A hash proves binding
to supplied bytes; it does not approve a baseline or establish historical truth.

The execution entrypoint accepts only a local metadata source and the synthetic
topic/storage/profile. There is **no production execution adapter or live CLI**.
It takes a nonblocking writer lock, reserves an exclusive run intent, recollects
metadata and validates before starting a reader. It then starts/stops the bounded
synthetic source, drains/stops the reader, collects final metadata, verifies
monotonic identity/offset/commit progress and requires output verification.
Success is recorded only after those checks. Reusing a run UUID is refused.
Failures preserve the intent and attempt cleanup, including partial starts and
expired budgets. Application callback failures record fixed phase/reason codes.

## B. Why

Recovery needs proof that the retained Kafka topic and Spark query are the same
ones, that the next offsets still exist, and that sink/checkpoint commits agree.
A fresh observation cannot silently become an approved historical baseline.
Separating collection, preview and execution makes each responsibility testable
without supplying production credentials or weakening the consumed-run gate.

The new integration uses this operator for real synthetic resume/repeat phases,
including actual Kafka metadata and durable Spark file logs. It starts the reader
before sending additional records, preserving the existing checkpoint. It checks
all synthetic output offsets, not just a process-start return code.

The first local operator run resumed correctly but failed its no-new-data repeat.
The old generic backend error did not retain its phase/cause, so timing was not
proven to be the sole cause. Review identified a timing-sensitive condition: an
idle Spark query can
return from processAllAvailable before publishing usable progress. The synthetic
backend now waits up to 45 seconds for actual progress matching broker end offsets
before asking the existing drain routine to verify its final commit. It never
converts missing progress into success. Failed attempts and cleanup evidence are preserved, and the corrected complete
integration passed before checkpointing. Fixed phase/reason receipts now improve
diagnosis without forwarding provider text.

## C. Files changed

- `runtime/recovery_metadata.py`: metadata adapters, Kafka probe, bounded collector.
- `runtime/recovery_operator.py`: envelope/hash checks, preview, lock, intent and offline sequencing.
- `tests/test_runtime_recovery_collection.py`: correctness, bounds, provenance and cleanup tests.
- `tests/integration/streaming_pilot.py`: shared collector and real synthetic execution backend.
- This report, [sanitized evidence](evidence/aws_runtime_recovery_operator_2026-11-19.json),
  [review-only future scope](proposals/runtime-recovery-inspection.proposed.json),
  and status links in the pilot/preparation reports.

README, existing production Compose/controller/operator/relay, immutable host
bundle, dependency locks, image pins, security acceptance/consumption records,
Terraform/state/backend, IAM and all retained production data remain unchanged.
Develop is the working branch; unrelated duplicate files remain untouched.

## D. Tests and validation

**42 focused cases, 866 runtime tests and 36 negative subtests pass**
(one existing websockets deprecation warning). Ruff, tracked/new-file secret
scanning, Python 3.9 and shell syntax, local links, historical/source hashes
and whitespace checks apply before committing. The local Docker test uses existing Linux/AMD64 images,
read-only module mounts, an internal network, no credentials/provider, no image
pull/build/publication and a uniquely named disposable project. Its outer process bounds each phase. The complete corrected run passed: 3 initial
rows, 5 after operator resume, still 5 after operator repeat, equal-offset topic
replacement rejected before Spark and missing-offset loss rejected. Cleanup
returned 0, and an independent check confirmed no owned containers, volumes or
networks remained.

Cases include duplicate/nonfinite JSON, unsupported logs, missing/partial commits,
compaction, empty append metadata, symlinks, path escapes, list/download bounds,
changed ETags/bytes, broker changes, active writers, stale observations, changed
baseline hashes, topic auto-creation disabled, changed metadata with identical
semantics, repeated attempts, competing locks, false drain reports, output-check
failure, cleanup failure and expired budgets. SDK/provider text is suppressed.
Exact final counts and integration results are in the linked evidence.

Hosted CI builds committed-source images and runs the same integration without
the temporary development mounts. Observe all six jobs after push. These checks
do not establish a live EC2 restart or fresh vulnerability assessment.

## E. Security and correctness review

No AWS SDK/client is constructed on import, and this work invokes no AWS API.
The S3 adapter requires an explicitly supplied client from a separately authorized
caller and reads only the existing pilot metadata namespace. It cannot read a
Parquet file or arbitrary object through its API. Empty/oversized objects,
truncated listings and changed conditional reads are refused; response bodies close
on success/failure. Collected evidence excludes metadata payloads and file paths
inside sink records, forwarding only identities, counters and SHA-256 hashes.

Synthetic execution rejects S3 sources, production profiles and the actual pilot
topic before backend callbacks. It provides no EC2/SSM/Docker/cloud control path.
Callback code remains trusted Python code; profile checks are not a sandbox for
an arbitrary malicious callback. The real integration enforces isolation through
its dedicated internal Docker network and explicit test-only mounts/resources.

Clock checks run between backend operations; they cannot interrupt an arbitrarily
blocking callback. Each future production backend and independent supervisor must
supply its own bounded operations and absolute shutdown controls. No live safety
claim or production authorization follows from passing an offline preview.

The original 41-advisory exception is consumed. It is not extended or replaced.
No advisory is waived, image rebuilt/published for deployment, secret read/edited,
credential scope changed, filesystem formatted, Kafka identity reset, checkpoint
removed or existing production resource altered. The [prior limited compatibility
review](aws_runtime_recovery_preparation.md#e-security-recheck-and-remediation-decision)
remains historical; this checkpoint performs no new vulnerability scan/research.

## F. Git and CI

Starting checkpoint: develop `ec8872d625c95d2c493356f99921c9d9c066b57d`;
[CI 44441229712](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/44441229712)
passed all six jobs. Remote develop/main were verified before editing. After local
review/testing, commit and push the exact intended files and observe the new CI.
The final SHA/run are reported in the conversation and private completion receipt.
Main remains `21be18e45791a4199830754b80620d7d6599587e`; no merge is included.

## G. Cost implications

This work creates no AWS resources, makes zero AWS calls and adds zero EC2 hours.
Existing retained cloud resources may continue billing. Local Docker and the
existing CI workflow perform the tests; no paid runner/service is added.

For future planning only, the earlier 45-minute supervision pattern would estimate
USD 0.12855 for EC2 plus IPv4 at the historical USD 0.1714/hour rate, excluding
storage, requests, logs, provider usage, tax and other charges. This is not a fresh
quote, bill, spending cap or authorization. Fresh pricing and a bounded exact
runtime scope must accompany any eventual paid approval request.

## H. Remaining blockers

The core collector, preview and synthetic execution path are implemented. The
following production work remains explicitly separate:

1. Reconcile the original topic UUID, Spark query ID and final batch-13 offset map
   from retained production evidence. The 45-row batch-0 sample is not that map.
   No approved production baseline is generated in this checkpoint.
2. Review a new exact security decision/current artifacts before affected images
   execute again; the previous one-run exception cannot be reused.
3. Implement/review a source-bound guest/SSM transport with actual instance, boot,
   mount/volume UUID, image, IAM, writer, timer and credential checks. The physical
   storage identity callback must verify the live resource, not trust a supplied
   string. No executable production package or start command is issued here.
4. Collect under the existing EC2 role after separate approval, or verify existing
   deployer read permissions before proposing any direct metadata inspection.
   Missing permissions must stop that path; no IAM expansion is implied.
5. Validate a live restart separately. Kafka/checkpoint recovery does not backfill
   blockchain events missed while the WebSocket was disconnected.

No new dependency is required for this offline checkpoint. The synthetic backend
uses the already pinned Kafka/Spark dependencies. Production placement remains a
review item: the bare EC2 Python must not be assumed to have confluent-kafka, and
the minimal application images must not be assumed to contain a compatible S3 SDK
client. A future driver must choose a reviewed probe/client location or bridge,
validate its exact build and avoid installing packages implicitly on startup.

## I. Exact proposed scope, shutdown and rollback

The linked JSON proposal identifies the existing EC2, encrypted disks, reviewed
SSM document, digest-pinned images, secret ARN and isolated S3 prefixes. Terraform
changes and proposed plan/apply scope are **none**. Every cloud authorization is
false. Missing baseline, new security decision and live package fields remain
null/blocked; this is a review document, not an executable deployment manifest.

Any future live window must be separately approved against completed artifacts.
First verify physical identity and metadata with source/writer stopped, reconcile
and bind the baseline, then conditionally resume the same query/topic/checkpoint.
Require fresh Spark progress, bounded output/offset checks, source stop, verified
Spark drain/final commit, broker stop and independent normal EC2 stop. Retain
+20-minute application stop, +30 external stop and +35 guest shutdown unless a
new concrete proposal justifies different deadlines. These are operating controls,
not a guaranteed billing cap. Do not restart while the proposal is blocked.

Rollback is to stop the attempt and preserve the disks, Kafka identity/logs,
Bronze objects, checkpoint, original installation and all old/new evidence. Never
format the already initialized volume, reset offsets, recreate a production topic,
delete checkpoints/data or revert a checkpoint by overwriting metadata. Production
cleanup requires a separate exact retention/cost review and approval.

## J. Whether production infrastructure changed

**No.** No AWS API call, EC2 start, ECR publication/scan request, SSM command,
production Terraform operation, secret access, IAM change, EBS action, production
S3 write, Snowflake/dbt/Streamlit change or main merge occurred. The latest cloud
state remains the prior stopped observation; no fresh cloud readback is claimed.
Milestone 11.5D stays paused, and the first streaming pilot remains accepted.
