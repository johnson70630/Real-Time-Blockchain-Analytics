# Milestone 11.6C: offline image publication review

**Current status:** GitHub billing/CI access is restored; the foundation, SSM
access, host software and one-time Kafka filesystem setup are complete. EC2 is
stopped. See the [new publisher permission/publication proposal](aws_runtime_ecr_publisher_review.md).
Its IAM expansion and live uploads are unapproved. The existing image builder
and planner described below remain offline-only; older private reviews are
preserved and must not substitute for a fresh frozen-source artifact plan.

## A. What was implemented

The cloud-only build script now builds the producer and Spark images from a
committed Git snapshot, labels each with its full source revision and role,
and selects `linux/amd64` on a local Unix-socket Docker engine. It retains a
private build-context archive and SHA-256 receipt. The existing local image
tags and Java/Spark/S3A/Kafka/Parquet smoke test remain in use.

`python -m runtime.images plan` inspects the three local pilot images and
writes an offline publication proposal. It checks architecture, source labels,
non-root application users and the pinned upstream Kafka reference. It never
logs in, tags, pushes, scans or calls AWS.

## B. Why

An image built from a dirty working directory can differ from its claimed
commit. A committed snapshot excludes untracked secrets and uncommitted edits
from the build input. Explicit platform inspection also prevents the Mac's
ARM64 Kafka cache from being mistaken for an EC2-compatible AMD64 image.

Local image IDs are local evidence, not ECR deployment digests. The proposal
keeps ECR digests unset until a separately approved publication returns them.

## C. Files changed and usage

- `runtime/images.py`: committed build-context generation and local review.
- `scripts/build_runtime_images.sh`: source snapshot, labels and local builder.
- `tests/test_runtime_images.py`: source, platform and publication contracts.
- This report and the link in `aws_streaming_pilot.md`.

Review, test and commit the source **before** running:

```sh
bash scripts/build_runtime_images.sh
python3 -m runtime.images plan --output /tmp/blockchain-image-review.json
```

Use a fresh output path; existing files are never overwritten. The build script
prints the source commit and its private receipt location. Build-time public
registry/package access is needed. The planner only reads local Docker metadata.
It requires a Docker CLI/engine that supports platform-specific image inspection;
unsupported tooling fails closed. Its output names these exact destinations:

| Role | Existing ECR repository | Proposed immutable tag |
|---|---|---|
| Producer | `209479274979.dkr.ecr.us-west-2.amazonaws.com/blockchain-runtime-producer` | `producer-<full-source-commit>` |
| Spark | `209479274979.dkr.ecr.us-west-2.amazonaws.com/blockchain-runtime-spark` | `spark-<full-source-commit>` |
| Kafka | `209479274979.dkr.ecr.us-west-2.amazonaws.com/blockchain-runtime-producer` | `kafka-3.9.1-<full-source-commit>` |

Kafka's source is the digest pinned in the pilot readiness report. Its upstream
source is independent of the application commit; the proposed tag associates
the selected Kafka artifact with this release review. No third repository is
proposed. A later publisher must use the reviewed image IDs, verify that the
immutable tags do not already identify different content, review scan findings,
and record the ECR-returned digests before deployment.

## D. Tests and validation

The new suite passes **21 offline tests**; combined with host-bundle and runtime
pilot tests, **68 tests pass**. Cases cover committed snapshot repeatability,
dirty/untracked-file exclusion, overwrite refusal, wrong architecture/OS,
incorrect labels/users, duplicate IDs, missing roles, unpinned Kafka, exact
repository targets, absent ECR digests, remote-engine rejection and suppression
of raw Docker errors. Ruff, Bash syntax and the repository secret scan pass.

After committing, run the actual image builds, network-disabled Spark classpath
smoke and planner against local Docker. Record the source revision, results
and artifact location in the conversation. A failure remains a blocker; unit
tests alone do not establish that actual images are ready for publication.

## E. Security review

Only selected committed application/build paths enter the context; the existing
`.dockerignore` still excludes sensitive file patterns. Review the committed
source and run secret scanning first: this is not a secret detector. The
planner projects seven metadata fields and never prints container environment
variables. It removes inherited AWS and Docker endpoint overrides and rejects
non-Unix Docker endpoints. Raw command output is suppressed on failure.

Source labels are an operator review aid, not a signature or tamper-proof build
attestation. Retain the context receipt and use a trusted local engine. Hashes
make the source context reproducible; OS package updates mean rebuilt image
bytes can still differ. Complete-image vulnerability review remains pending.
No credentials are embedded or requested, and no publisher IAM changes are made.

## F. Git and CI

Create a develop checkpoint after offline validation, push and observe its
new hosted CI. GitHub access is restored. Build/review from that exact committed
source; report the revision and artifacts before requesting publication.
Do not substitute older successful CI or mutable local tags. Main remains untouched.

## G. Cost implications

This work uses local Docker and initiates no new AWS charges. Existing retained
EBS disks continue costing approximately USD 10.40/month. Later approved image
publication adds ECR storage; tagged images require explicit retention/cleanup
review because the existing lifecycle policy only expires untagged images.

## H. Remaining blockers

Actual ECR digests and scan reviews remain unavailable. Publisher IAM and image
uploads/scans, secret provisioning/workload IAM, next-boot mount checks and
application deployment retain their separate gates. The foundation, access,
host recovery and exact-volume initialization are complete; their approvals do
not authorize publication. CI is operational and EC2 is stopped.

## I. Next step

Review the new publisher proposal and fresh committed local builds. Make exact
image IDs, destination tags, source/context hashes, permission scope and storage
cost reviewable before requesting publication. No Terraform apply is needed;
all earlier recovery plans are consumed and must not be replayed.

## J. Production infrastructure changes

None. No AWS API, Terraform backend/plan/apply, ECR push, EC2 action, production
storage operation, secret access or Snowflake change is part of this work.
The approved Terraform configuration and five-resource plan remain separate
and unchanged. Milestone 11.5D remains paused; README and local Compose behavior
are unchanged.
