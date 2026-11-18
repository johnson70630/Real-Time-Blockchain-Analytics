# Milestone 11.6C — Revised input diagnostic and conditional pilot review

**Prepared for a new approval; EC2 is stopped.** The last paid start was consumed
and stopped before streaming. This review proposes one new supervised 45-minute
window using the revised host-versus-stdin diagnostic, with conditional streaming
only if every safety and chain gate passes. No new start is authorized yet.

## A. What was implemented

Prepared a new private SSM dispatcher and guest command template bound to the
[reviewed operator](../scripts/check_runtime_chain.py) from `db45609`. The operator
SHA-256 is `e4dd9034677e4f507d097dfc79b19b0c551b5110a31570648930a537f6bf9bb2`.
All earlier operator artifacts, receipts and failed-window evidence are preserved.

The template embeds those exact bytes in a fresh tmpfs path and preserves the
installed release/legacy check. It retains a separate pre-chain receipt containing
verified boot, installation, shutdown deadline, empty pilot prefix and cached-image
checks, including when the probe fails. The dispatcher rechecks develop/HEAD,
tracked cleanliness, approved source hashes and all five private artifact hashes
before SSM and each action. Install and legacy prepare remain unavailable.

## B. Why

The last live failure could not distinguish the retrieved value from its Docker
stdin transport. The revised checker first validates the retrieved value on the
host using the unchanged client rules. Host rejection reports `SECRET_FORMAT`;
a later client format rejection reports `INPUT_TRANSPORT`. RPC errors remain
separate. Neither a stored-value repair nor a transport repair is assumed.

A successful diagnostic may continue to the original demo within the same boot,
avoiding an additional paid restart solely to launch streaming. A failed gate
stops immediately, preserves only sanitized evidence and does not retry.

## C. Exact proposed resources and changes

Account `209479274979`, Oregon `us-west-2`; all AWS resources already exist:

| Resource | Exact scope |
| --- | --- |
| EC2 | One manual Console start and normal stop of `i-0ca1671a59751acc5`, t3.xlarge, standard CPU credits |
| EBS | Preserve root `vol-03bd5d26947719f98` and Kafka `vol-02c9fb14a9c568fb5`; no filesystem initialization |
| Kafka mount | Existing `/var/lib/blockchain-runtime/kafka`, UUID `da1454d8-1bea-41a3-89d7-87bd570a47bd`; no metadata reset |
| SSM | Existing private `blockchain-runtime-demo-shell`; no SSH or ingress |
| Guest changes | Temporary operator files under `/run/blockchain-runtime`; absolute transient shutdown timer only |
| Secret | One probe retrieval of the existing exact ARN ending `-d44flr`; the existing controller retrieves it again only after successful gates; no edits |
| S3 | `johnson-blockchain-analytics-prod/blockchain-analytics/pilot/runtime/11-6c-v1/` only |
| Logs | Existing `/blockchain/runtime/containers` group and scoped runtime streams |

**Terraform changes and expected plan/apply/import/backend scope: none.**
**New AWS resources, IAM expansion and deployment with new credentials: none.**
The existing workload role and exact secret grant remain the only credential
path. Read-only IAM/image checks must pass again before the paid start.

The original release candidate SHA remains
`7f948e94cec7f7aebaf2aa4dab4ddd8e92a60f083f17bb9cefcff4db89cd200f`.
Its exact accepted ECR images remain:

- Producer: `209479274979.dkr.ecr.us-west-2.amazonaws.com/blockchain-runtime-producer@sha256:63d9d0f24a1cb7eb30b080e58f8a40ddd49292b89020328409e6f45319597afd`
- Spark: `209479274979.dkr.ecr.us-west-2.amazonaws.com/blockchain-runtime-spark@sha256:bb03f7d9917f2d8dc7d2fc1fc6a1df83fe876f5298e98c4163c689a1b890a134`
- Kafka: `209479274979.dkr.ecr.us-west-2.amazonaws.com/blockchain-runtime-producer@sha256:1113a10e8a93fc12b2d310ef6f53b8f5fc95fb806a8c6edcfa6b67d17959378b`

New tracked files are this review and its [sanitized evidence](evidence/aws_runtime_transport_review_2026-11-18.json),
plus status links in the pilot and failed-window report. No application source,
image, Compose, frozen host file, dependency lock or README change is included.

## D. Tests and validation

The revised operator already passed **548 runtime tests and 36 negative subtests**
and all six jobs in [CI 44213308369](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/44213308369).
The new private dispatch/template pass **14 additional offline contract cases**:
success, host-format failure, stdin failure, RPC denial, unrecognized output,
timeout, and refusal of changed approvals, HEAD, branch, worktree or hashes.
Tests mock the guest/cloud interface and use no credentials/network. They verify
that only success creates the application preflight gate and that failed probes
retain the earlier timer controls. Static compilation and terminal line limits pass.

All five private artifact hashes and test-source hash are bound in the evidence.
Review secret scan, links, whitespace, frozen-file equivalence and evidence hashes;
checkpoint on develop and observe its CI. The exact accepted producer image is
not currently available through the local mutable offline tag; no local retest
is claimed and no pull/rebuild is included. Guest image verification remains
mandatory before the real secret check.

## E. Security and data correctness

Expected human profile: `blockchain-demo-deploy`, assumed role
`blockchain-runtime-deployer`; at least 90 minutes of session validity before
arming. MFA codes remain private. Check the exact instance/boot, disk identities,
IMDSv2, no ingress/swap, free space, cached digests and safe Kafka metadata.
No secret value, endpoint, payload hash, provider text or terminal transcript
may enter chat, Git or logs. The probe uses memory/stdin, bounded resources,
non-root execution, no container logging and normal TLS. No trimming, unwrapping,
secret rewrite, permission bypass or automatic retry is permitted.

The existing exact-image risk acceptance expires **2026-11-24 00:00 UTC** and is
still unused for streaming. Changed findings/artifacts or unmet controls block
the run; the shared EC2 workload-role trust risk remains as previously reviewed.
No new exception or additional streaming run is proposed.

Require verified Arbitrum chain ID **42161** before invoking the existing service
once. Kafka has no public endpoint and retains EBS logs. Spark writes Bronze to
`bronze/swaps/` and its S3 checkpoint to `checkpoints/swaps_bronze/` under the
pilot prefix, with exactly one writer. Existing Snowflake/dbt/Streamlit remain
outside scope.

## F. Git and CI

Preparation began on clean tracked develop at `db45609`; all six jobs passed.
Commit this review and observe its new automatic CI; record the resulting SHA
and run in conversation. Main and unrelated untracked duplicates stay untouched.

## G. Cost and duration

The same public AWS rates recorded on 2026-11-18 00:30 UTC imply approximately
**USD 0.12855 (USD 0.13) for 45 minutes of EC2/IPv4**. This is an estimate,
not a hard bill cap; requests/logs/transfer, retained resources, taxes and provider
charges are additional. No provider plan upgrade or new AWS resource is included.
A failed probe requests normal stop immediately; a successful smoke test also
stops early rather than consuming the full window.

## H. Acceptance and remaining blockers

All deadlines bind to this new boot's actual EC2 launch time:

| Deadline | Required action |
| --- | --- |
| 15 minutes | Abort if application setup/start is incomplete |
| 30 minutes | Normal-stop request if still running |
| 35 minutes | Independently verified guest timer initiates OS poweroff |
| 40 minutes | Target verified stopped |
| 45 minutes | End of supervision; Console normal-stop fallback if needed |

Use the detached watcher, fresh heartbeat and owner Console fallback. Preserve
the original 150-minute secondary timer and unchanged 3,600-second controller
backup cap; the shorter external controls govern. Create/verify the new timer
before secret retrieval, retain its proof and never extend it on a repeated check.

On success require Kafka/topic health, producer delivery, positive committed
Spark batch/checkpoint progress, scoped CloudWatch delivery and a readable public
Parquet sample with expected schema, Arbitrum labels, transaction/log identifiers
and complete observed Kafka partition-offset range without gaps/duplicates.
Do not claim overall chain completeness from a single observed batch.
The live format mismatch, chain access, ingestion and drain are still unverified.
Live Kafka/Spark failure-injection recovery remains a later, separately approved
acceptance task; offline synthetic recovery is already covered by CI.

## I. Stop, rollback, cleanup and approval

Prefer service stop after smoke evidence: flush producer, drain Spark to Kafka
end offsets, verify its final checkpoint, stop Kafka, clear tmpfs credentials and
power off. Independently confirm EC2 stopped, IPv4 released and both disks
preserved; record elapsed runtime and any unverified drain/cleanup. On diagnostic
failure, normally stop immediately. Preserve logs/checkpoints/EBS and receipts.
No force stop, terminate, blind restart, offsets reset or destructive cleanup.

EBS one-time initialization was completed previously for the exact Kafka volume;
**no mkfs/wipefs action or new initialization is proposed**. Stop does not remove
retained EBS/ECR/secret charges. Resource deletion or secret rollback needs its
own exact approval.

**Requested approval:** one new supervised 45-minute window for the exact node,
temporary shutdown timer/operator files, scoped SSM checks and exact secret read,
revised chain diagnostic, conditional single streaming invocation with isolated
S3/log writes, graceful shutdown and normal stop. Every failed gate stops the
attempt. No additional secret edit, IAM/Terraform change, new resource, formatting,
image publication, downstream change or main merge is included.

## J. Whether production infrastructure changed

No. Preparation was offline plus read-only identity/instance/SSM metadata.
EC2 remains verified stopped, and no watcher is armed for a new attempt.
No new start, secret retrieval/edit, guest change, Terraform/IAM/storage change
or application invocation occurred in this preparation. Milestone 11.6C remains
open; 11.5D stays paused.
