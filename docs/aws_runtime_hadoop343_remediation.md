# Milestone 11.6C — matched Hadoop 3.4.3 runtime

Latest profile review: [minimal cloud streaming distribution](aws_runtime_streaming_profile.md)
removes unused server/catalog modules and reduces Spark library findings to
1 critical / 60 high. The release remains blocked; no exceptions, publication
or paid pilot are approved. Counts below preserve the earlier checkpoint.

Reviewed 2026-11-16 UTC. **The release remains blocked.** This is an offline
application-image checkpoint. It accepts no vulnerability exceptions and does
not authorize publication or an AWS pilot. Milestone 11.5D remains paused.

## A. What was implemented

The cloud image now uses a coordinated Hadoop/S3 dependency set:

| Component | Previous | Selected |
| --- | --- | --- |
| Hadoop client API/runtime | 3.4.1 | 3.4.3 |
| Hadoop S3A connector | 3.4.1 | 3.4.3 |
| AWS SDK v2 bundle | 2.24.6 | 2.35.4 |
| WildFly OpenSSL | 1.1.3.Final | 2.2.5.Final |
| S3 Analytics Accelerator | Not installed | 1.3.1, declared compile dependency |

Java 17, PySpark 4.0.1, Kafka client 3.9.2 and broker 4.3.2 stay unchanged.
The existing Netty, native TLS, Bouncy Castle and Jersey patches are retained.

Build manifest schema 2 binds the selected component versions together.
Before replacing any bundled client, the installer rejects a missing, duplicate,
old or mismatched connector/SDK artifact. Both original Hadoop client hashes
must match the reviewed PySpark distribution before replacement.

The complete download lock contains 45 SHA-512-pinned artifacts, including the
new compile dependency discovered in the Hadoop S3A POM. The clients occupy only
the standard Spark JAR directory; the existing 34-family precedence directory
remains for Netty/Bouncy Castle/Jersey. This avoids duplicate Hadoop binaries.

Startup checks the actual class origins of 16 representative classes, including
both Hadoop clients, S3A, SDK, WildFly and Analytics Accelerator. It requires the
matched Hadoop version, Java 17, Spark 4.0.1, an in-memory catalog and the classic
S3A input stream. The accelerator dependency is available, but its reader and
the prefetch reader are not enabled for the pilot.

## B. Why it was implemented

Hadoop 3.4.1 embedded Avro 1.9.2, including a reported critical schema-parsing
vulnerability. Updating only a top-level Avro JAR would leave that relocated
copy unchanged. Replacing Hadoop's matched client API/runtime updates the actual
shaded implementation while keeping its S3 connector and SDK compatible.

The selected versions come from the published Hadoop 3.4.3 project and S3A POMs;
their hashes and dependency scope are in the evidence. Apache lists
[Hadoop 3.4.3](https://hadoop.apache.org/releases.html) as a released version.
Its [S3A troubleshooting guidance](https://hadoop.apache.org/docs/r3.4.3/hadoop-aws/tools/hadoop-aws/troubleshooting_s3a.html)
requires compatible Hadoop components and the SDK against which they were built.
The implementation does not substitute an arbitrary newer SDK.

The first candidate copied the matched Hadoop clients into both the standard
and override directories. Its raw high count increased because the same shaded
libraries appeared twice. The final candidate retains one complete, verified
client pair and checks the selected class files there. No distinct library or
vulnerability metadata was stripped to improve counts.

## C. Files changed

- `deploy/runtime/jars.lock.json`, `deploy/runtime/spark-jvm-patches.lock.json`:
  coordinated artifact/hash pins and schema 2.
- `deploy/runtime/patch_jars.py`, `deploy/runtime/Dockerfile.spark`: matched-cohort
  validation, exact client replacement and deduplicated precedence directory.
- `runtime/jvm.py`: expanded class-origin/version checks and classic-reader guard.
- `runtime/stream.py`, `deploy/runtime/smoke_spark.py`: explicit classic S3A mode
  and updated Hadoop validation.
- `runtime/contracts.py`: quarantine of the rejected `f91dc41` Spark manifest.
- `tests/test_runtime_jvm.py`, `tests/test_runtime_jvm_patches.py`,
  `tests/test_runtime_pilot.py`, `tests/test_runtime_images.py`: mixed/missing
  dependency, stale class source, input-mode and rejected-manifest contracts.
- This report, [sanitized evidence](evidence/aws_runtime_hadoop343_2026-11-16.json)
  and status links in the previous readiness reports.

README, root Python project/lock, local development Spark entrypoints,
Terraform, Snowflake/dbt/Streamlit and existing cloud artifacts are preserved.

## D. Tests and validation

The unpublished Linux/AMD64 candidate passed the existing image checks under
UID 10001, read-only filesystem, dropped capabilities, no-new-privileges and
network isolation. Checks cover engine versions, actual class sources, epoll and
native TLS, Bouncy Castle AES-GCM, Jersey, the Java CA store and Parquet I/O.

The smoke continues to construct SDK sync/async S3 clients and an S3A filesystem
with anonymous credentials and bucket probing disabled. No request methods are
called; the container has no external network route. The classpath-shadow
negative test still rejects an incorrectly launched JVM without starting Spark RPC.

Synthetic integration covers ingestion, Kafka restart, checkpoint resume,
repeat without duplicate rows and offset-loss rejection. Its internal network
and owned disposable volumes are separate from production storage. Live S3
authentication, checkpoint writes and EC2 recovery remain separate pilot tests.

The focused local suite passed **304 tests**. Ruff, secret scanning and shell
checks passed before the develop checkpoint. Exact committed builds, scans, recovery and all
six hosted CI jobs are observed afterwards and reported in the conversation.

## E. Security review

The same verified Trivy 0.76.2 binary and database snapshot scan both image
filesystems. OS and library findings include all severities and unfixed issues;
there is no ignore file. Raw reports stay private; sanitized projections retain
their SHA-256 hashes and exact local image identities.

| Spark findings | Baseline `f91dc41` | Final prototype |
| --- | ---: | ---: |
| Library critical | 5 | 4 |
| Library high | 82 | 78 |
| Library medium | 97 | 91 |
| Library low | 7 | 6 |
| OS critical | 0 | 0 |
| OS high | 47 | 47 |
| Unique high/critical package-advisory pairs | 49 | 45 |

No new high/critical package-advisory pair is introduced. The unique removed
pairs are Avro CVE-2024-47561 / CVE-2023-39410, BeanUtils CVE-2025-48734 and
Netty handler CVE-2025-24970. Some other copies of affected components remain,
so raw occurrence changes and distinct package/advisory changes differ.

The initial duplicate-client prototype reported 4 critical / 90 high library
occurrences, but the same 45 unique high/critical pairs. The evidence preserves
that comparison. Deduplication removed only redundant copies, leaving all
remaining distinct findings visible.

Four critical occurrences still remain:

- Netty 4.1.126 in AWS SDK 2.35.4: CVE-2026-75595.
- Netty 4.1.118 in the Spark Connect client: CVE-2026-75595.
- Derby 10.16.1.1: CVE-2022-46337.
- Legacy Jackson mapper 1.9.13: CVE-2019-10202.

All 78 high JVM occurrences and 47 high OS occurrences also remain open. No
exception is accepted, including for inactive server/catalog features. The
classic-reader guard keeps the new accelerator available as a dependency without
silently opting this demonstration into a new reader behavior.

New input checksum provenance is explicit: AWS SDK and Analytics Accelerator
published SHA-512; the three Hadoop artifacts and WildFly published SHA-1,
checked over HTTPS before computing their committed SHA-512 pins. This does not
claim independent publisher-signature verification. CI verifies the committed
SHA-512 values regardless of the upstream checksum format.

The verified cache is **771,743,831 bytes / 45 artifacts**, below the unchanged
1 GiB limit. No runtime dependency download, credential embedding, TLS bypass,
IAM expansion, client-side encryption setup or MapReduce job submission is added.

## F. Git commit and CI status

Verified starting develop SHA: `f91dc411146911f3c259fe0b0b67facba07a175d`.
[CI 43830176995](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/43830176995)
passed all six jobs. The reviewed change is committed/pushed on develop after
local validation; its exact SHA and automatically triggered CI result are
reported after observation. Main is not merged.

## G. Cost implications

No paid compute starts, AWS resources change or images are published. Local
build/cache storage grows, and the existing CI jobs process the larger SDK.
The cache limit, runner class, job count, quota and billing settings stay unchanged.
Existing retained AWS storage/monitoring fees continue. A later larger ECR
publication and a bounded paid pilot both require separate approval.

## H. Remaining blockers

- Four critical / 78 high JVM occurrences and the existing high OS findings.
- No accepted digest-specific exceptions or approved replacement ECR digests.
- Reviewed Alchemy secret ARN and exact runtime-role/boundary permissions.
- Live S3 writes/checkpoint recovery, next-boot EBS mount and pilot acceptance.

No older publication or EBS-initialization approval is reused. No volume is
formatted or cloud state initialized as part of these checks.

## I. Next step

Review the remaining Connect, Derby and legacy-mapper features and their actual
use before selecting supported replacement artifacts or a minimal cloud
distribution. Review the SDK's retained shaded Netty copy separately; a patched
standalone Netty family does not update relocated classes. Any proposed exception
must identify the exact digest, CVE, conditions, controls and expiration.
The actual AWS pilot remains gated on security review and explicit approval.

## J. Whether production infrastructure changed

**No.** No AWS API calls, authenticated production Terraform commands, IAM
actions, ECR pushes, EC2 starts, S3 writes, Secrets Manager changes, Snowflake
actions or EBS operations occurred. There is no new live EC2-state observation.
The platform/runtime Terraform states remain separate and untouched.
