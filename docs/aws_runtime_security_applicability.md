# Milestone 11.6C — library security and applicability review

Reviewed 2026-11-16 UTC. **The AWS streaming release remains blocked.**
This is an offline implementation checkpoint, not an exception approval or
deployment authorization. Milestone 11.5D remains paused.

## A. What was implemented

- Scanned the exact `8805eddb25a20f5abc188d359bf7412d2998c1c4` Linux/AMD64
  producer, Spark and Kafka image archives for Python/JVM vulnerabilities.
- Added a separate cloud Python project and lock under `deploy/runtime/`.
  Container requirements are generated from its producer/Spark dependency groups.
- Patched producer `aiohttp` 3.14.1 → 3.14.5, `certifi` 2025.1.31 → 2026.10.23,
  and `urllib3` 2.7.0 → 2.8.0. The resolver also updated required transitive
  `yarl` 1.24.2 → 1.25.1. Other retained package versions stayed unchanged.
- Added CI lock/export checks and a regression test that rejects a stale
  requirement pin. Exports keep distribution hashes; Docker still requires them.
- Added an actual installed producer CA-bundle check alongside native Kafka ABI
  and canonical event-decoding smoke checks.
- Quarantined the two rejected `8805edd` application manifests in both release
  validation and image-publication planning, including attempts to relabel them.
- Recorded component evidence for all 11 remaining high OS CVEs and exact JAR
  paths for Spark's high/critical library findings. No CVE suppressions were added.

## B. Why it was implemented

OS package scanning misses Python wheels and dependencies inside Java bundles.
The earlier OS/JRE remediation therefore did not establish full-image readiness.
This review adds those layers and distinguishes installed vulnerable packages
from evidence about how a specific application can reach them.

The Python CA issue affects outbound HTTPS trust. We use the upstream patched
bundle and inspect its installed roots, rather than disabling certificate
verification. See the [certifi advisory](https://github.com/advisories/GHSA-6q48-6c7w-9gw2).
The selected [aiohttp maintenance release](https://github.com/aio-libs/aiohttp/releases/tag/v3.14.5)
also avoids an inconsistent affected-version boundary in the preceding advisory;
the rebuilt image is rescanned. The [urllib3 advisory](https://github.com/advisories/GHSA-8988-9cw3-xx77)
identifies 2.8.0 as the fix for the reported proxy TLS issue.

A separate lock allows these cloud-only patches without changing the existing
root dependency graph used by local development, Snowflake, dbt and Streamlit.
The smaller Python 3.12 constraint also removes irrelevant distribution hashes
from the generated exports; Spark's installed Python package versions are unchanged.
The root environment still contains its older pins; a separate shared-environment
security update is needed to address those versions outside the cloud producer.

## C. Files changed

- `deploy/runtime/pyproject.toml`, `deploy/runtime/uv.lock`: independent cloud graph.
- `deploy/runtime/{producer,spark}-requirements.txt`: hash-checked group exports.
- `deploy/runtime/smoke_producer.py`: installed versions and real trust-store checks.
- `scripts/check_runtime_dependencies.py`: offline byte-for-byte export verification.
- `.github/workflows/ci.yaml`: cloud lock freshness and export validation in the
  existing quality job; no new job or credentials.
- `runtime/contracts.py`: additional known rejected manifest identities.
- `tests/test_runtime_dependencies.py`, `tests/test_runtime_pilot.py`,
  `tests/test_runtime_images.py`: export and rejected-release regression coverage.
- This report and
  [sanitized evidence](evidence/aws_runtime_library_review_2026-11-16.json).
- Status links in the existing pilot and image-remediation reports.

## D. Tests and validation

The patched local producer prototype built from hash-checked public artifacts and
passed native Kafka ABI, synthetic Uniswap event normalization, installed version,
and CA-root presence/absence checks in a read-only, network-disabled container.
It ran as UID 10001 with all capabilities dropped and no-new-privileges enabled.
The prototype is deliberately labeled `UNCOMMITTED_PROTOTYPE` and is not publishable.

Trivy 0.76.2 was verified against the previously reviewed binary SHA-256.
The vulnerability database was updated at `2026-11-16T01:11:02Z`; the separately
downloaded Java identification database at `2026-11-16T00:57:00Z`. Image scans
used local tar inputs, all severities, unfixed findings enabled, and no ignore
file. Raw reports remain private; public projections retain their SHA-256 hashes.
See [Trivy's Java coverage](https://trivy.dev/docs/latest/coverage/language/java/).

| Exact baseline image at `8805edd` | Library critical | Library high | Medium | Low |
| --- | ---: | ---: | ---: | ---: |
| Producer | 1 | 6 | 13 | 2 |
| Spark | 9 | 102 | 122 | 9 |
| Kafka | 0 | 0 | 0 | 0 |

These are scanner occurrences, not counts of distinct exploitable flaws.
Spark has 92 unique high/critical package-version/advisory pairs across 111
occurrences; a library can appear both independently and inside shaded bundles.

The patched producer prototype has **0 critical / 0 high library findings**,
with **6 medium / 1 low** still reported. Its OS scan remains **0 critical /
47 high / 92 medium / 65 low / 5 unknown**. Those OS findings are not waived.
Baseline Spark has the same OS totals. Kafka's baseline OS and library scans
report no findings; its JRE trust-store validation remains a separate requirement.
These local Trivy results are separate from the historical ECR BASIC scans.

The focused local suite passed **235 tests**. Ruff, secret scanning, cloud lock
freshness and offline export checks passed. Frozen-build smoke/recovery
validation is also required for the develop checkpoint. Its exact source SHA and
hosted six-job CI outcome are reported after the commit is pushed; a successful
CI run does not clear this report's security gate.

## E. Security review

### OS applicability candidates — no exceptions accepted

All 11 CVEs below remain marked vulnerable by the Debian tracker for Trixie.
The evidence contains checked timestamps, vendor-page hashes and affected binary
versions. Classifications below are review candidates, not a signed VEX statement.
They apply only to the inspected application manifests and reviewed configuration.

| CVE | Observed component / required condition | Review candidate |
| --- | --- | --- |
| [2025-69720](https://security-tracker.debian.org/tracker/CVE-2025-69720) | `infocmp` is installed; reviewed app entrypoints do not invoke it. | Present; call path not established. |
| [2026-119373](https://security-tracker.debian.org/tracker/CVE-2026-119373) | SQLite FTS5 is enabled; no application FTS queries found. | Present; unused-feature candidate. |
| [2026-119374](https://security-tracker.debian.org/tracker/CVE-2026-119374) | FTS5 exists; this pipeline does not accept SQLite database files. | Present; transitive reachability unproven. |
| [2026-119379](https://security-tracker.debian.org/tracker/CVE-2026-119379) | SQLite session extension exists; no app changeset application found. | Present; unused-feature candidate. |
| [2026-16742](https://security-tracker.debian.org/tracker/CVE-2026-16742) | `systemd-homed` and `homectl` executables are absent; shared libraries remain. | Affected component absent candidate. |
| [2026-54369](https://security-tracker.debian.org/tracker/CVE-2026-54369) | `libacl` exists; exploit requires a privileged pathname operation. | Privilege restrictions mitigate; unpatched. |
| [2026-76642](https://security-tracker.debian.org/tracker/CVE-2026-76642) | `mount` exists and retains a setuid bit on disk. | no-new-privileges/capability restrictions mitigate; unpatched. |
| [2026-78408](https://security-tracker.debian.org/tracker/CVE-2026-78408) | `nsenter` exists without setuid; no privileged namespace entry in the app. | Privileged-caller condition not established. |
| [2026-78409](https://security-tracker.debian.org/tracker/CVE-2026-78409) | Mount-related code exists; reviewed containers cannot gain mount privileges. | Privilege restrictions mitigate; unpatched. |
| [2026-78410](https://security-tracker.debian.org/tracker/CVE-2026-78410) | Setuid `mount` remains; no-new-privileges prevents setuid elevation. | Privilege restrictions mitigate; unpatched. |
| [2026-9538](https://security-tracker.debian.org/tracker/CVE-2026-9538) | Perl exists; `Archive::Tar` fails to load specifically because its module is missing. | Affected component absent candidate. |

Both inspected images report UID 10001, `CapEff=0`, `CapBnd=0` and
`NoNewPrivs=1` under the reviewed flags. The cloud Compose configuration uses a
read-only root filesystem and drops all capabilities. No privileged container,
host Docker socket or public Kafka endpoint is configured. Host-side root disk
administration is outside this container applicability assessment.

Static searches do not prove that a dependency can never invoke vulnerable code.
Changes to entrypoints, packages, mounts, privileges, exposed services or data
formats invalidate these candidates. No package metadata was hidden or removed
to improve scanner counts; no exploit payloads were run.

### Spark critical findings — unresolved compatibility work

| Advisory / installed component | Location / pilot observation | Required next review |
| --- | --- | --- |
| CVE-2026-75595 / Netty 4.1.100 and 4.1.118 | AWS SDK bundle, standalone Netty and shaded Spark Connect client; relevant class entries exist. Pilot does not configure per-SNI mTLS server routing. | Verify all three copies and TLS call paths; no waiver. |
| CVE-2024-47561 / Avro 1.9.2 | Shaded inside `hadoop-client-runtime-3.4.1.jar`; schema class exists. Pilot reads JSON and writes Parquet, without an Avro input/schema service. | Review Hadoop's shaded implementation, including indirect Parquet paths. |
| CVE-2022-46337 / Derby 10.16.1.1 | LDAP authentication class exists. Pilot does not configure an LDAP-authenticated Derby server or Hive metastore. | Verify effective catalog/Derby configuration; Java 17 rules out a blind move to a Java 21-only Derby release. |
| CVE-2025-14813 / Bouncy Castle 1.80 | GOST CTR cipher class exists; no application GOST configuration found. | Review provider selection and upstream patch compatibility. |
| CVE-2026-8763 / Bouncy Castle 1.80 | X.509 name-constraint classes exist. Outbound S3 TLS is required. | Establish actual TLS provider/certificate-validation path; do not dismiss an outbound TLS risk. |
| CVE-2019-10202 / Jackson 1.9.13 | Legacy `ObjectMapper` class exists. This job uses Spark's JSON reader rather than explicit Codehaus default typing. | Review transitive uses; scanner has no fixed version for this legacy coordinate. |
| CVE-2025-12383 / Jersey client 3.0.16 | HTTP connector class exists. No explicit application Jersey client is configured. | Trace indirect clients and review the upstream concurrency/TLS fix. |

The [Netty advisory](https://github.com/netty/netty/security/advisories/GHSA-c4c3-7fpv-j4q5)
describes a particular server TLS routing condition; Trivy's critical rating is
preserved even though the upstream page labels it high. Package severity and
application exposure are separate facts. Bouncy Castle's vendor pages cover
[GOST CTR](https://github.com/bcgit/bc-java/wiki/CVE%E2%80%902025%E2%80%9014813)
and [certificate name constraints](https://github.com/bcgit/bc-java/wiki/CVE%E2%80%902026%E2%80%908763).
Jersey's [upstream fix](https://github.com/eclipse-ee4j/jersey/commit/b2c7ba6d388cb9722f39073d7e82aa818fec49d5)
changes concurrent HTTPS connector initialization.

The other 102 high occurrences also remain open, including compression, JSON,
HTTP/DNS, ZooKeeper, Thrift and shaded dependencies. The public evidence lists
every high/critical package path, installed version and scanner-reported fix.
Removing a top-level JAR alone cannot patch a second copy inside Hadoop, the AWS
SDK bundle, Parquet or Spark Connect.

Keep PySpark 4.0.1 / Java 17 for this checkpoint. The next offline task is to
compare a supported Spark maintenance distribution and its matching Kafka/Hadoop
connectors against a narrowly patched 4.0.1 distribution. Any proposal must retain
checkpoint/recovery behavior and the requested Java version. Do not replace
individual Hadoop/AWS JARs opportunistically: the
[Hadoop 3.4.1 S3A guidance](https://hadoop.apache.org/docs/r3.4.1/hadoop-aws/tools/hadoop-aws/troubleshooting_s3a.html)
requires compatible Hadoop components and warns that arbitrary SDK substitutions
can introduce runtime linkage errors.

## F. Git commit and CI status

The verified starting checkpoint is develop `8805eddb25a20f5abc188d359bf7412d2998c1c4`;
[CI 43821949830](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/43821949830)
passed all six jobs. This change is committed to develop only after its local
security/correctness checks pass. Its new commit and automatically triggered CI
are observed and reported in the conversation. No main merge is authorized.

## G. Cost implications

This review uses local Docker, public package downloads and the existing CI jobs.
It creates no billable AWS resources and publishes no replacement ECR images.
Previously retained AWS storage/monitoring resources continue to incur their
existing charges. The earlier roughly $24/month scheduled-demo estimate is not
a spending authorization. No paid EC2 start is part of this checkpoint.

## H. Remaining blockers

- Spark's unresolved JVM library findings and both app images' high OS findings.
- No accepted, digest-specific exception record; applicability candidates alone
  do not clear the gate. Medium/low findings also remain visible for review.
- Replacement images are not approved for ECR publication or cloud deployment.
- Alchemy secret ARN, exact workload-role/boundary access, live S3 writes,
  next-boot EBS mount and cloud recovery/acceptance tests remain outstanding.

## I. Next step

Prepare the Spark/Hadoop compatibility proposal offline, then rebuild and repeat
library/OS/JRE scans plus synthetic recovery tests for the selected distribution.
Present any remaining exceptions individually with exact digest, evidence,
compensating controls, expiration and re-review trigger; never blanket-accept them.
After all readiness gates are resolved, request approval for exact publication,
credential/IAM scope and the bounded AWS pilot. Historical approvals are consumed.

## J. Whether production infrastructure changed

**No.** No AWS API calls, production Terraform commands, IAM changes, ECR pushes, EC2 starts,
S3 writes, Secrets Manager operations, Snowflake actions or EBS formatting occurred
in this checkpoint. The existing CI still performs credential-free Terraform
static validation without a production backend. The prior EC2 observation was stopped at
`2026-11-16T06:12:25Z`; this offline review does not claim a new live observation.
The initialized Kafka EBS volume and all existing cloud artifacts are preserved.
README and the root platform/local dependency files are unchanged.
