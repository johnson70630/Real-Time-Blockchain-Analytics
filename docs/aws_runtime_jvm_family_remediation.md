# Milestone 11.6C — coordinated JVM family remediation

Reviewed 2026-11-16 UTC. **The release remains blocked.** This checkpoint fixes
compatible standalone dependencies and an observed classpath problem. It does
not approve remaining findings, publication or an AWS pilot. Milestone 11.5D is paused.

## A. What was implemented

The cloud Spark image replaces 34 exact bundled JARs as coordinated families:

| Family | Before | Selected version | Replacements |
| --- | --- | --- | ---: |
| Netty Java/transport | 4.1.118.Final | 4.1.140.Final | 21 |
| Netty native TLS | 2.0.70.Final | 2.0.85.Final | 6 |
| Bouncy Castle provider | 1.80 | 1.87 | 1 |
| Jersey | 3.0.16 | 3.0.18 | 6 |

The Netty native version matches the selected Netty parent's dependency property.
All platform classifiers already present in the distribution are retained.
Java 17, PySpark 4.0.1, Hadoop 3.4.1, AWS SDK 2.24.6 and Kafka client 3.9.2 are unchanged.

`patch_jars.py` validates the complete expected family inventory and SHA-512
hashes of every old and new file before replacing installed JARs. Unknown files,
mixed inventories, corrupt caches, traversal and symlinks fail closed. An exact
repeat verifies the already installed files. This runs only while building an
image; it is not an EC2 boot or application-startup operation.

The reviewed replacement families also occupy `/opt/pilot/jvm-overrides`.
Explicit driver/executor classpaths give them precedence. Before the cloud stream
constructs its S3 filesystem, `runtime.jvm` verifies the actual origins of ten
representative classes, engine versions and the in-memory catalog. Shadowed
classes or a Hive catalog cause startup to fail.

## B. Why it was implemented

The first candidate installed patched Netty JARs and reported a patched version,
but its actual `SslClientHelloHandler` still loaded from `bundle-2.24.6.jar`.
Version metadata alone therefore gave a misleading picture of the executed code.
The corrected candidate checks the classloader's selected file as well as disk
inventory. A negative container test disables precedence and proves rejection.

Spark documents driver and executor extra classpaths in its
[configuration reference](https://spark.apache.org/docs/4.0.1/configuration.html).
Selected upstream maintenance releases are
[Netty 4.1.140](https://github.com/netty/netty/releases/tag/netty-4.1.140.Final),
[native TLS 2.0.85](https://github.com/netty/netty-tcnative/releases/tag/netty-tcnative-parent-2.0.85.Final),
[Bouncy Castle 1.87](https://www.bouncycastle.org/download/bouncy-castle-java/)
and [Jersey 3.0.18](https://github.com/eclipse-ee4j/jersey/releases/tag/3.0.18).

Hadoop and AWS SDK binary versions stay paired. The
[S3A troubleshooting guidance](https://hadoop.apache.org/docs/r3.4.1/hadoop-aws/tools/hadoop-aws/troubleshooting_s3a.html)
requires compatible Hadoop components and the SDK version against which they
were built. A newer arbitrary SDK bundle is not a safe substitute.

## C. Files changed

- `deploy/runtime/spark-jvm-patches.lock.json`: exact old hashes and replacement coordinates.
- `deploy/runtime/jars.lock.json`: the original eight connectors plus 34 new SHA-512 pins.
- `deploy/runtime/patch_jars.py`: validated build-time family replacement and override staging.
- `deploy/runtime/Dockerfile.spark`: offline installation and explicit classpath precedence.
- `runtime/jvm.py`, `runtime/stream.py`: class-origin/version/catalog guards before S3 access.
- `deploy/runtime/smoke_spark.py`: installed dependency, native, cryptographic and SDK checks.
- `deploy/runtime/smoke_jvm_shadow.py`: rejection when override precedence is disabled.
- `scripts/build_runtime_images.sh`: committed negative smoke and capability restrictions.
- `runtime/contracts.py`: quarantine of the prior Spark image and rejected shadowed prototype.
- `tests/test_runtime_jvm.py`, `tests/test_runtime_jvm_patches.py`,
  `tests/test_runtime_images.py`, `tests/test_runtime_pilot.py`: integrity and rejection coverage.
- This report, [sanitized evidence](evidence/aws_runtime_jvm_families_2026-11-16.json)
  and status links in the prior readiness reports.

README, root Python lock/project, local development entrypoints, production
Terraform, Snowflake/dbt/Streamlit and all existing cloud artifacts are preserved.

## D. Tests and validation

The complete unpublished prototype passed the following under Linux/AMD64,
UID 10001, read-only root filesystem, dropped capabilities, no-new-privileges
and `--network none`:

- Java 17 / Spark 4.0.1 / Hadoop 3.4.1 / Kafka client 3.9.2 checks.
- Actual class origins for Netty common/buffer/transport/codec/HTTP/TLS/native,
  Bouncy Castle and Jersey.
- Netty epoll and native TLS loading; Bouncy Castle AES-GCM known-answer vector;
  Jersey client construction; default SunJSSE TLS provider.
- AWS SDK sync and Netty async S3 client construction using an anonymous provider.
  S3A filesystem construction used bucket probing disabled. No request methods
  were called, and the container had no external network route or AWS credentials.
- The existing Java trust-store hash and positive/negative CA-anchor checks.
- Parquet write/read correctness.
- Intentional driver-classpath misconfiguration rejected before application S3 work.

Synthetic Kafka integration also passed initial ingestion, broker restart,
checkpoint resume, repeated processing without duplicate output rows, and
fail-closed offset-loss handling. These tests use an isolated internal Docker
network and owned disposable volumes, which were removed afterwards. Their
root-owned volume fixture is separate from the non-root cloud image smoke.

The focused local suite passed **273 tests**. Ruff, secret scanning and shell
syntax checks passed before commit. Exact committed builds, recovery and hosted CI are
observed after the develop checkpoint; results are reported in the conversation.

## E. Security review

Trivy 0.76.2 scanned local exported image archives with cached vulnerability and
Java databases, all severities, unfixed findings retained and no ignore file.
The same database snapshot supports the comparison; this is not an ECR scan.

| Spark final-filesystem findings | Baseline `43d3b4b` | Corrected prototype |
| --- | ---: | ---: |
| Library critical | 9 | 5 |
| Library high | 102 | 82 |
| Library medium | 122 | 97 |
| Library low | 9 | 7 |
| OS critical | 0 | 0 |
| OS high | 47 | 47 |

Four critical and twenty high library occurrences are removed. Counts are
scanner occurrences, not a claim that every reported CVE is exploitable.
No finding is suppressed. Shaded bundles and their metadata are not rewritten.
Explicit precedence fixes unshaded class selection; it does not update separate
relocated copies inside a bundle.

Five critical occurrences remain:

| Advisory | Component / location | Status |
| --- | --- | --- |
| CVE-2026-75595 | Netty 4.1.100 inside AWS SDK bundle | Unchanged bundled/shaded code remains; no exception. |
| CVE-2026-75595 | Netty 4.1.118 inside Spark Connect client | Relocated copy remains; this job does not start Connect. No exception. |
| CVE-2024-47561 | Avro 1.9.2 inside Hadoop client runtime | Requires coordinated Hadoop review; no exception. |
| CVE-2022-46337 | Derby 10.16.1.1 | In-memory catalog is enforced; vulnerable code remains installed. No exception. |
| CVE-2019-10202 | Legacy Jackson mapper 1.9.13 | No fixed artifact reported for this coordinate; no exception. |

The 82 high occurrences and previous OS applicability candidates remain open.
Live S3 authentication, Alchemy ingestion and cloud recovery are not established
by constructor or local recovery tests. New Netty behavior against the unchanged
SDK must also pass the eventual separately approved S3 pilot.

All new Git lock pins use SHA-512. Maven publishes SHA-512 for 28 new artifacts;
for six Jersey artifacts it publishes SHA-1, which was checked over HTTPS before
computing their SHA-512 pins. The evidence records this distinction. These checks
do not claim independent publisher-signature verification. CI rechecks the
committed SHA-512 pins and downloads only from the existing fixed Maven origin.

The verified cache contains 42 artifacts / 590,379,819 bytes, below the existing
1 GiB ceiling. No runtime dependency resolver, credential embedding, TLS
verification disablement, IAM expansion or arbitrary dynamic JAR loading is added.

## F. Git commit and CI status

Starting develop SHA: `43d3b4b40166483ea7412bda7d10a6f153dfddaf`.
[CI 43824642262](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/43824642262)
was independently verified successful in all six jobs. This checkpoint is
committed/pushed on develop only after local review; its source SHA and triggered
CI result are reported after observation. No main merge is authorized.

## G. Cost implications

No AWS resource is created, started, modified or deleted. No image is published.
Local image/cache storage grows, and the existing six CI jobs perform additional
offline checks. No runner class, billing budget, quota or CI permission changes.
Existing retained AWS storage/monitoring charges continue unchanged. Future
publication would store larger replacement images and requires separate approval.

## H. Remaining blockers

The five critical / 82 high JVM occurrences and 47 high OS occurrences keep the
Spark security gate blocked. There are no accepted exceptions or approved
replacement ECR deployment digests. The secret ARN, exact workload IAM scope,
next-boot EBS mount verification and paid pilot acceptance remain outstanding.
Historical publication and filesystem initialization approvals are consumed.

## I. Next step and dependency proposal

Continue offline with the coordinated Hadoop candidate, preserving Spark 4.0.1
and Java 17 until compatibility tests justify any scope change:

| Candidate | Required matched set | What metadata establishes |
| --- | --- | --- |
| Hadoop 3.4.3 | client API/runtime and `hadoop-aws` 3.4.3, SDK 2.35.4, WildFly OpenSSL 2.2.5.Final | Declares Avro 1.11.4, but still older Jackson/Guava/Netty dependencies. Not yet a tested or security-cleared binary set. |
| Hadoop 3.5.0 | client API/runtime and `hadoop-aws` 3.5.0, SDK 2.35.4, WildFly OpenSSL 2.2.5.Final | A larger change; also retains dependencies requiring review. Not selected. |

The public [Hadoop releases](https://hadoop.apache.org/releases.html) and matching
Maven project POM hashes are recorded in the evidence. Spark 4.0.5's declared
POM still has Hadoop 3.4.1, Derby 10.16.1.1, Bouncy Castle 1.80 and Netty
4.1.118; changing only the Spark maintenance version is not sufficient.

Derby 10.16.1.2 is absent from the checked Maven release metadata; the published
[10.17.1.0 release](https://db.apache.org/derby/releases/release-10_17_1_0.cgi)
requires Java 21. Do not silently upgrade Java or substitute an unverified backport.
Review unused Connect/Hive/legacy-mapper features and their actual call paths
before choosing supported replacement artifacts or proposing individual,
digest-specific, time-limited exceptions. This checkpoint accepts none.

## J. Whether production infrastructure changed

**No.** No AWS API calls, production Terraform commands, IAM actions, ECR pushes,
EC2 starts, S3 writes, Secrets Manager changes, Snowflake actions or EBS operations
were performed. The prior stopped EC2 observation remains historical; this
offline checkpoint does not claim a new live-state check.
