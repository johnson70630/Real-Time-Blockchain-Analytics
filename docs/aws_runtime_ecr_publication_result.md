# Milestone 11.6C: image publication completed; release blocked

The three approved Linux/AMD64 images were published to the two existing ECR
repositories on **2026-11-16**, with completion at **06:07:02 UTC**. Every stored
manifest hash matches the reviewed local manifest. All BASIC scans completed,
but unresolved HIGH/CRITICAL findings **block deployment of this release**.
EC2 was independently verified stopped before and after publication. No
streaming application, secret or S3 pilot data has been deployed.

## A. What was implemented

The operator reported attaching the exact reviewed
`blockchain-runtime-ecr-publisher` managed policy to the human MFA role
`blockchain-runtime-deployer`. Effective ECR access was verified. The assistant
did not modify IAM or use another administrator identity. Live IAM document
readback is not available to this role, so the attachment/document identity
remains operator-attested, not independently IAM-readback verified.

Published only producer, Spark and pinned upstream Kafka to the reviewed
immutable tags. The producer and Kafka share the existing producer repository;
Spark uses the existing Spark repository. No third repository was created.
The selected platform was explicitly `linux/amd64`, using Docker's
[single-platform push](https://docs.docker.com/reference/cli/docker/image/push/).

## B. Why

ECR provides persistent cloud image identities so the future EC2 runtime can
pull exact reviewed bytes without depending on the Mac. Immutable tags aid
inspection; deployment must use the returned digest references. A successful
upload establishes artifact integrity, not vulnerability acceptance.

## C. Files and exact artifacts

The image source is frozen at
`c917f2ac7a0ecd34d5ddd82997e5166fa07ea006`. The committed-source context SHA-256 is
`17ae82d039a7c0343b4f459453a5d17a2e2ae0f85a40802014bef8c3762a6126`.
Later documentation checkpoints do not change this published source revision.

Registry: `209479274979.dkr.ecr.us-west-2.amazonaws.com`.
Each tag below appends `-c917f2ac7a0ecd34d5ddd82997e5166fa07ea006`.

| Role | Existing repository | Tag prefix | Returned ECR digest |
|---|---|---|---|
| Producer | blockchain-runtime-producer | producer | `sha256:daa2cab53f201ac58435a557a2e1d2d006a4a442dc0a0c7c22fb9a81167f88e0` |
| Spark | blockchain-runtime-spark | spark | `sha256:85c0b8ebf842847215698fe24efe05db04d0b6525d587955eb58209897843c38` |
| Kafka | blockchain-runtime-producer | kafka-3.9.1 | `sha256:5862db4a63a6dd7d46fd14771b10a1b39e069c2c47f17d8e4640f960720a0ead` |

The [sanitized evidence](evidence/aws_runtime_ecr_publication_2026-11-16.json)
contains exact tags/digest references, manifest/config hashes, measured sizes,
scan timestamps/counts and all 221 findings projected to nonsecret fields.
Private operator receipts and original scans remain preserved outside Git.
Updated readiness/image/publisher reports link to this completed result.
No application code, Terraform source, README or local Compose behavior changed.

## D. Tests and validation

The frozen source passed **212 focused contracts**, Ruff, secret scanning,
Bash syntax and whitespace checks. Actual local image builds and the
network-disabled Java 17 / Spark 4.0.1 / S3A / Kafka / Parquet smoke passed.
Its [CI run 43811882656](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/43811882656)
passed all six jobs, including Kafka persistence and checkpoint recovery.

Eight offline adversarial operator checks passed before execution: changed
source, wrong identity, running EC2, mutable repository, enhanced scanning,
changed local manifest and existing target tags all prevent upload; a simulated
push failure performs no automatic push retry and removes temporary auth.
The private helper SHA-256 is
`4e36406e6400cf972f5051f0c1d45df561a5c72f4bd96cef45b32dd0c4ed4cbe`.

Live preflight verified the expected MFA role, stopped instance, exact two
IMMUTABLE/AES256 repositories, BASIC registry settings and absence of all three
destination tags. After each push, DescribeImages and BatchGetImage verified
the returned digest and independently hashed the stored single-platform
manifest. No unknown-result push was retried or tag overwritten.

## E. Security review and vulnerability gate

| Image | Scan | CRITICAL | HIGH | MEDIUM | LOW | UNDEFINED |
|---|---|---:|---:|---:|---:|---:|
| Producer | COMPLETE | 0 | 16 | 17 | 5 | 1 |
| Spark | COMPLETE | 0 | 16 | 33 | 9 | 1 |
| Kafka | COMPLETE | 10 | 57 | 46 | 10 | 0 |

No findings pages are missing. Producer/Spark scans were already complete when
checked; no duplicate scans were requested. Kafka initially had no scan, so
exactly one StartImageScan request was made for its new digest. Registry
configuration stayed BASIC with empty rules; no Inspector/enhanced scan was
enabled. BASIC is an OS-package check, not a full Python/JVM dependency audit.
See [scan results API](https://docs.aws.amazon.com/AmazonECR/latest/APIReference/API_DescribeImageScanFindings.html)
and [scan frequency limits](https://docs.aws.amazon.com/AmazonECR/latest/APIReference/API_StartImageScan.html).

The base distributions were verified from `/etc/os-release` in read-only,
network-disabled local containers: Debian 12 for producer/Spark, Alpine 3.21.3
for Kafka. These probes ran only `/bin/cat`, not applications.

Producer and Spark share the same 16 HIGH findings across six source packages:
util-linux (3), sqlite3 (3), glibc (1), pam (1), krb5 (6) and zlib (2).
Kafka's CRITICAL findings affect brotli (1), zlib (1), OpenSSL (6) and GnuTLS (2).
All package versions/CVE IDs are preserved in the evidence.

Initial vendor triage established that an unreviewed upgrade is insufficient:

- [Alpine's lifecycle](https://alpinelinux.org/releases/) lists 3.21 support
  ending 2026-11-01. The Kafka image requires a supported base/image review;
  merely mirroring its old pinned digest cannot fix its packages.
- Debian lists the published Bookworm PAM version as vulnerable for
  [CVE-2025-6020](https://security-tracker.debian.org/tracker/CVE-2025-6020).
  Newer release fixes must not be assumed to have been backported.
- [util-linux CVE-2026-53615](https://security-tracker.debian.org/tracker/CVE-2026-53615)
  and [zlib CVE-2026-85091](https://security-tracker.debian.org/tracker/CVE-2026-85091)
  have fixes in newer Debian releases while the scanned Bookworm versions
  remain vulnerable in the vendor records.
- Debian still lists vulnerable stable-release versions for
  [glibc CVE-2026-84243](https://security-tracker.debian.org/tracker/CVE-2026-84243),
  [SQLite CVE-2026-119374](https://security-tracker.debian.org/tracker/CVE-2026-119374)
  and [Kerberos CVE-2026-112320](https://security-tracker.debian.org/tracker/CVE-2026-112320).
  A base-tag refresh alone does not prove these findings resolved.

These are initial vendor checks, not a complete reachability assessment of
every CVE. Runtime hardening and absence of public Kafka do not automatically
waive findings. No severity was suppressed, exception accepted or release
approved. This candidate is **blocked and must not be deployed**.

Registry login used the existing temporary MFA role, password through stdin,
and a dedicated mode-0700 Docker config with its auth file mode 0600. Only the
temporary authentication directory was removed after upload; cleanup was
verified. No token, credential file, environment dump or secret value entered
Git, build context, image, user data or logs. Single-platform pushes do not
preserve index-level attestations; hashes/labels here are not signatures.

## F. Git and CI

Published source remains `c917f2a`, with all six source CI jobs successful.
Checkpoint these documentation/evidence updates on develop after secret and
consistency validation, then observe that checkpoint's own CI. Main is untouched.
The documentation checkpoint is not an additional image publication approval.

## G. Cost implications

ECR reports **1,330,397,364 bytes** summed across the three image records.
At the [AWS example storage rate](https://aws.amazon.com/ecr/pricing/) of
USD 0.10/GB-month, this is about **USD 0.13/month** before shared-layer
deduplication, other registry usage and taxes. This is a reference estimate,
not an invoice, hard limit or free-tier assumption. Tagged images remain
retained; the existing untagged-only lifecycle does not remove them.

The existing retained disks/alarms remain approximately USD 10.60/month before
other usage/taxes. This operation started no EC2 runtime. BASIC scan execution
adds no enhanced Inspector scanning component. Do not delete evidence images
or expand retention/cleanup scope without approval.

## H. Remaining blockers

Unresolved image vulnerabilities block this release. Remediation must first
identify supported package/base candidates and assess exact affected code
paths against vendor records, then validate compatibility and new hashes.
Do not simply suppress findings or assume current packages are fixed.

The Alchemy secret does not yet have a reviewed ARN; its creation and exact
workload boundary/read grants are separate gates. Mount persistence on the
next boot, a timed cloud smoke/recovery run and application deployment remain
unapproved. Existing EBS formatting approval is consumed and never repeatable.

## I. Next step and rollback

Continue offline vulnerability remediation planning and compatibility review,
starting with the unsupported Kafka base. Preserve this frozen publication as
an auditable rejected release. Any replacement images require a fresh source,
local review, tests/CI and separate approval for publication/scans. No approval
for new image bytes can be inferred from this completed three-image operation.

Rollback is to leave the application undeployed and EC2 stopped; there is no
running release to revert. Do not delete the images, detach policies, alter
repositories, reformat Kafka EBS or replay consumed Terraform plans.

## J. Production infrastructure changes

Yes: operator-reported attachment of the approved publisher policy, three new
image tags/manifests in existing ECR repositories, and one Kafka BASIC scan.
The helper made no IAM changes. No EC2 launch/start, new repository, Terraform
init/plan/apply/import, S3 write, secret access, workload IAM change, Snowflake
operation, resource deletion or main merge occurred. Existing platform resources
were untouched. Milestone 11.5D remains paused.
