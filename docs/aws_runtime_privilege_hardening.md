# Milestone 11.6C — Application image privilege hardening

Review started 2026-11-16 UTC; exact evidence timestamps are recorded separately.
**Deployment remains blocked.** This offline checkpoint accepts no exceptions,
publishes no images and authorizes no paid runtime. Milestone 11.5D stays paused.

## A. What was implemented

The producer and Spark image builds clear setuid and setgid bits from all
regular files on the image root filesystem after installation and application
copying. Each baseline contained 11 such executables, including `mount`, `su`,
`passwd`, `newgrp` and `unix_chkpwd`. Each candidate now has zero. Their file
contents, owners and sizes remain unchanged. Package files and metadata remain
present. Setgid directory inheritance is outside this executable-mode change.

The build script audits both final image filesystems through flattened Docker
exports. The owned inspection containers are **never started**, have no mounts,
no added capabilities, a read-only root and no network. The Docker engine exports
metadata for otherwise inaccessible private directories. The Python checker
reads tar headers without extracting files, following links or executing code.
It rejects privileged regular files/hardlinks, malformed or duplicate paths,
empty/corrupt input and excessive work. Inspection containers are cleaned on
success or failure. Ordinary application smokes still run as UID/GID 10001.

The checker is taken from the exact committed build-context tar, alongside the
Dockerfiles, rather than from an uncommitted worktree. The preceding producer
and Spark manifests are quarantined in publication planning and release checks.

## B. Why it was implemented

These workers ingest events and process data; they do not administer accounts,
enter host namespaces or mount EBS filesystems. Host administration remains a
separate, explicitly gated SSM operation. Removing executable privilege bits
reduces available escalation paths and backs up the existing non-root,
all-capabilities-dropped and no-new-privileges controls.

Debian still lists the four util-linux advisories
[76642](https://security-tracker.debian.org/tracker/CVE-2026-76642),
[78408](https://security-tracker.debian.org/tracker/CVE-2026-78408),
[78409](https://security-tracker.debian.org/tracker/CVE-2026-78409) and
[78410](https://security-tracker.debian.org/tracker/CVE-2026-78410)
as vulnerable for Trixie. Their documented conditions involve privileged mount
or namespace operations. Clearing setuid bits constrains how the installed
helpers can acquire privilege. **This is a mitigation, not a package fix or a
finding disposition.** It does not prove every library call path unreachable.

All 11 remaining OS CVE vendor pages were rechecked and hash-identified. None
provides a fixed Trixie package in this review. Unstable-package substitutions,
forced removal of essential OS packages and private native rebuilds are not
introduced. The existing Hadoop, Spark and SDK bundled-library findings remain.

## C. Files changed

- `deploy/runtime/Dockerfile.producer`, `Dockerfile.spark`: final permission-bit
  removal; existing users, entrypoints, versions and package metadata retained.
- `deploy/runtime/check_image_privileges.py`: bounded metadata-only export audit.
- `scripts/build_runtime_images.sh`: exact-context audit, mount rejection and
  cleanup of only its own never-started inspection containers.
- `runtime/contracts.py` and image/pilot tests: reject both preceding manifests.
- `tests/test_runtime_image_privileges.py`: archive and permission-bit contracts.
- This report, [sanitized evidence](evidence/aws_runtime_privilege_bits_2026-11-16.json)
  and status links in the Parquet and pilot-readiness reports.

README, local development behavior, dependency locks, Kafka configuration,
Terraform, Snowflake/dbt/Streamlit and production artifacts are preserved.

## D. Tests and validation

The focused suite passes **460 tests**. The 17 new archive cases include both
privilege bits on regular files and hardlinks, unusual filenames, duplicate and
unsafe paths, truncated/empty input and resource limits. Symbolic links are
never followed, and no archive files are extracted.

The final export audit rejects both exact baseline images and accepts both
hardened prototypes. It examines private directories without changing their
permissions. An earlier live-filesystem inspection needed extra read/search
privilege; security review replaced that approach before commit. No extra
capability or root inspection process is part of the final export audit.

The ordinary non-root producer/Spark smokes pass, including native Kafka ABI,
Java 17/Spark 4.0.1, all 51 class origins, CA/native TLS/crypto checks, typed and
legacy Parquet reads, compression and deliberate classpath-shadow rejection.
Separate non-root checks confirm all 11 affected executable contents, owners
and sizes are preserved in each prototype.

Synthetic tests cover ingestion, broker restart, checkpoint resume, repeat
without duplicate rows and fail-closed offset loss. Only owned disposable
volumes are cleaned. Ruff, all-files secret scanning, shell syntax and diff
checks must pass before commit. Exact committed builds/audits, identity-bound
scans and recovery are verified after commit, followed by all six CI jobs.

## E. Security review

| Scope | Before | Candidate |
| --- | --- | --- |
| Setuid/setgid regular executables, per app image | 11 | 0 |
| Producer library CRITICAL / HIGH | 0 / 0 | 0 / 0 |
| Spark library CRITICAL / HIGH | 1 / 30 | 1 / 30 |
| OS HIGH, per app image | 47 | 47 |

The unsuppressed same-database scans have no introduced or removed
HIGH/CRITICAL package-version/CVE pairs. Counts correctly remain unchanged:
permission hardening does not patch vulnerable code. The 47 OS HIGH rows
represent 11 distinct CVEs across multiple binary packages. Scanner input,
artifact and vendor-page hashes are retained; cached coverage is not a
claim of exhaustive coverage of all newly disclosed vulnerabilities.

The SDK critical issue and remaining library/OS HIGH findings stay open. No
severity is lowered, no package metadata is hidden, and no applicability
exception is accepted. Prior reachability candidates in the
[applicability review](aws_runtime_security_applicability.md) remain unapproved.
The metadata audit does not attest runtime volumes, host packages, Linux file
capabilities/xattrs or every possible application path. Existing application
capability/no-new-privileges controls remain required.

## F. Git commit and CI status

Starting develop SHA: `bb66dd6f3cd8fbbbd966c748679999a62fce410f`.
[CI 43979029114](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/43979029114)
passed all six jobs. The reviewed change is committed/pushed only after local
validation. The exact new SHA, frozen-image verification and new six-job CI
result are recorded in the final conversation result. No main merge.

## G. Cost implications

This uses local builds, read-only image exports and existing standard
public-repository CI. No AWS resources, ECR storage or runtime hours are created
or authorized. Retained cloud resources can continue existing charges; this
batch performs no live billing or resource-state query.

## H. Remaining blockers

The critical SDK issue, remaining library/OS HIGH findings and final release
review remain unresolved. No approved replacement ECR deployment digests exist.
Reviewed Alchemy secret/IAM scope, next-boot EBS mount validation, live S3 and
cloud recovery tests, and a concrete bounded pilot approval remain separate.

## I. Next step

Prepare a consolidated, digest-specific remediation/applicability decision for
the remaining bundled libraries and OS findings. Keep the requested Spark/Java
versions and the tested Hadoop/SDK pairing unless a separately reviewed upgrade
is selected. Do not treat these permission changes or CI success as a waiver.
Refresh the deployment-readiness proposal before any production approval.

## J. Whether production infrastructure changed

**No.** No AWS API, production Terraform initialization/plan/apply/import,
instance start, EBS operation, IAM/secret/Snowflake change or ECR publication
occurs. Both production state locations remain separate and untouched.
Milestone 11.5D remains paused.
