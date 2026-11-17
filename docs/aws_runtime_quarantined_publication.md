# Milestone 11.6C — Quarantined replacement image publication

**All uploads verified; deployment remains blocked.** Milestone 11.5D remains paused.
Completed: 2026-11-17T02:04:50.544045+00:00.

## A. What was implemented

Published the three explicitly approved Linux/AMD64 images to scan-only immutable
tags in the two existing private ECR repositories. Independently hashed each
stored single-platform manifest and matched its config digest to the frozen
review. All three match exactly. No existing image/tag was overwritten or deleted.

## B. Why

Cloud artifact verification lets the future runtime pull reviewed bytes without
depending on the Mac. Independent ECR BASIC scanning adds an OS assessment.
Publication establishes artifact integrity; it does not accept vulnerabilities.

## C. Files and exact artifacts

Image source: `ddc1861cef958d5bc718100fcb502aac8318011b`.
Registry: `209479274979.dkr.ecr.us-west-2.amazonaws.com`.

| Role | Existing repository | Verified ECR manifest |
| --- | --- | --- |
| Producer | `blockchain-runtime-producer` | `sha256:63d9d0f24a1cb7eb30b080e58f8a40ddd49292b89020328409e6f45319597afd` |
| Spark | `blockchain-runtime-spark` | `sha256:bb03f7d9917f2d8dc7d2fc1fc6a1df83fe876f5298e98c4163c689a1b890a134` |
| Kafka | `blockchain-runtime-producer` | `sha256:1113a10e8a93fc12b2d310ef6f53b8f5fc95fb806a8c6edcfa6b67d17959378b` |

Tags are `scanonly-producer-<full-source>`, `scanonly-spark-<full-source>` and
`scanonly-kafka-4.3.2-<full-source>`. Kafka shares the producer repository.
The [sanitized evidence](evidence/aws_runtime_quarantined_publication_2026-11-17.json)
records exact tags, digest references, scans, source identity and helper hashes.
This report and links in the pilot/security reports are documentation only.
README, runtime code, dependency locks, Terraform and previous artifacts are preserved.

## D. Tests and validation

Eight adversarial preflight cases and three valid control cases passed offline.
Eight publisher cases passed: success, an already-identical tag, missing scan,
changed proposal, conflicting tag, altered manifest readback, failed upload and
inconsistent scan counts. Failed/unknown uploads are never retried automatically.
Dedicated registry authentication cleanup was checked on success and failure.

The frozen runtime has 460 focused contract tests, all three exact image smokes,
two setid export audits and validated Kafka/checkpoint recovery. The documentation
source changes only the image revision label; root filesystem layers and runtime
configuration match the validated recovery baseline. Source
[CI 43992698947](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/43992698947)
passed all six jobs. This new documentation checkpoint receives its own CI.

Live preflight verified the expected assumed role/account, stopped instance,
two IMMUTABLE/AES256 repositories, legacy scan-on-push flags, BASIC registry mode,
untagged-only expiry policies, exact local AMD64 manifests and unused target tags.
Final checks confirmed EC2 remained stopped and scanning configuration unchanged.

## E. Security review and scan results

| Image | ECR scan status | CRITICAL | HIGH | MEDIUM | LOW | UNDEFINED |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Producer | COMPLETE | 0 | 2 | 10 | 3 | 1 |
| Spark | COMPLETE | 0 | 2 | 10 | 3 | 1 |
| Kafka | COMPLETE | 0 | 0 | 0 | 0 | 0 |

All completed scan pages are retained privately, hash-identified and projected to
nonsecret CVE/package/version/severity fields in Git. Counts must match all pages.

Registry configuration remains `BASIC` with empty scan rules. Legacy repository
scan-on-push flags do not establish that every new image will be scanned.
AWS explains that repositories without a matching filter use
[manual scan frequency](https://docs.aws.amazon.com/AmazonECR/latest/userguide/image-scanning-basic.html).
No scanning settings, Inspector enrollment or manual scan calls were changed/made.

ECR BASIC checks OS packages. It does not clear the local Spark JVM result of
**1 critical / 30 high**, or the producer/Spark OS results of **47 high each**.
The [consolidated security review](aws_runtime_security_decision.md) retains all
39 advisory IDs and 125 local high/critical occurrences. Different scanner scopes,
feed timestamps and package mappings must be reconciled rather than selecting the
lowest count. **No security exception or application deployment is approved.**

The cloud review adds two IDs to the high/critical decision inventory:
**CVE-2026-19499** and **CVE-2026-5435**, both in glibc 2.41-12+deb13u4.
They were already recorded as MEDIUM against libc-bin/libc6 in the full local
scans. ECR reports HIGH against the glibc source package; neither original
severity is overwritten. The combined high/critical advisory inventory is now
**41 IDs**, rather than the earlier 39-ID local-only decision scope. ECR source
package counts and local binary-package counts must not be added as distinct flaws.

[Debian's monetary-formatting advisory](https://security-tracker.debian.org/tracker/CVE-2026-19499)
concerns output-buffer bounds during right-justified formatting.
[Debian's DNS-printing advisory](https://security-tracker.debian.org/tracker/CVE-2026-5435)
also lists the installed Trixie release as vulnerable. The
[glibc maintainer announcement](https://sourceware.org/pipermail/libc-announce/2026/000057.html)
describes the deprecated DNS debug-printing APIs separately from normal resolver
operation. These prerequisites help assess exposure but do not prove all native
or transitive callers safe. No exploit is executed and no not-affected claim is
issued. Vendor priorities or fixes in another distribution do not patch Trixie.
The evidence preserves successful source hashes and the failed direct upstream
monetary-advisory fetch; its Debian record was retrieved successfully.

Authentication used the existing MFA role, token through stdin and a dedicated
mode-0700 Docker config with mode-0600 auth file. The temporary authentication
directory was removed and verified. No token/secret entered Git, logs or images.
The upload receipt remains private; sanitized evidence contains no credentials.

## F. Git commit and CI

The uploaded images remain frozen at `ddc1861`; their source CI passed all six
jobs. Commit this documentation/evidence checkpoint to develop after validation,
then observe its automatically triggered CI. The checkpoint SHA and result are
reported in the conversation. Main is unchanged; no images are rebuilt or
republished because of this documentation checkpoint.

## G. Cost implications

ECR reports **1,615,714,633 bytes** summed across the
three records. At the [AWS example rate](https://aws.amazon.com/ecr/pricing/) of
USD 0.10/GB-month, this is approximately **USD 0.16/month**
before layer deduplication, other retained images and taxes. This estimate is not
a guaranteed bill or spending cap. Tagged storage persists while EC2 is stopped.
[BASIC scanning has no additional scanning charge](https://aws.amazon.com/about-aws/whats-new/2024/08/new-version-amazon-ecr-basic-scanning/).
No EC2 runtime hours, new repository or enhanced Inspector scanning were added.
Existing retained disks/monitoring and old artifacts can continue their charges.

## H. Remaining blockers

All three scan results are complete. Residual security risk still requires an
owner decision; no automatic waiver is applied. Alchemy's
reviewed secret ARN, exact workload secret permissions, authenticated Terraform
scope and a separately bounded paid runtime approval remain later gates.
Never repeat the consumed Kafka EBS initialization approval; verify its existing
UUID-based mount on the next approved boot.

## I. Next step, rollback and cleanup

The ECR findings have been reconciled with all severities in the local scans.
The next owner decision is whether to accept the documented residual risk for
one supervised public-data demonstration, or keep deployment blocked while
broader remediation is investigated. The exact pending proposal is
`security_exception_proposal` in the linked JSON evidence: all 41 advisory IDs,
the three verified ECR digests and report hashes, a maximum 3,600-second stream,
one run, and expiration at **2026-11-24 00:00:00 UTC**. Acceptance remains false.
The baseline per-package/path inventory and additional glibc occurrences are
hash-bound to that proposal. No vulnerability severity is reduced.

For this isolated portfolio demo, a conditional one-run exception is a possible
owner choice instead of maintaining a private vendor-bundle rebuild. It carries
unproven native/transitive reachability and the retained Spark critical finding.
All listed hardening controls and separate credential/IAM/paid-deployment approvals
must be satisfied before any live run. Security acceptance alone authorizes no
AWS write, EC2 start or application deployment. If the owner declines, the images
stay quarantined; no broader private rebuild is silently adopted.

Rollback is to keep the application undeployed and leave EC2 stopped. Preserve
new and old images, data volumes, checkpoints and scan artifacts. No image
cleanup, policy mutation, resource deletion, Terraform action, EC2 start,
Secrets Manager entry, Snowflake change or main merge is authorized by this
publication. Confirm retained-cost scope before any later cleanup proposal.

## J. Whether production infrastructure changed

**Yes, production registry artifacts only:** three new exact scan-only image
tags/manifests/layers in two existing ECR repositories. No new AWS resources,
EC2 start, Terraform initialization/plan/apply/import, IAM/secret/S3 data mutation,
Snowflake change or resource deletion occurred. Existing platform resources are
preserved. Milestone 11.5D remains paused.
