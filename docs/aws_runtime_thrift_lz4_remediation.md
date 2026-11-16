# Milestone 11.6C — LZ4 maintenance and unused Thrift removal

Reviewed 2026-11-16 UTC. **Deployment remains blocked.** This is an offline
security checkpoint, with no accepted exceptions, ECR publication or paid
runtime authorization. Milestone 11.5D remains paused.

## A. What was implemented

The cloud Spark image replaces `org.lz4:lz4-java:1.8.0` with the maintained
`at.yawk.lz4:lz4-java:1.14.1`. The `net.jpountz` Java package remains compatible;
the Maven coordinate change is explicit in the SHA512 download/replacement
locks and installer. The old coordinate is rejected as a replacement source.
Only one LZ4 JAR is installed, in Spark's normal JAR directory.

The in-memory streaming profile also removes the exact original
`libthrift-0.16.0.jar` and `libfb303-0.9.3.jar`. Their sizes and SHA512 hashes
must match before any removal. Startup rejects restored filenames and
standalone Thrift/Facebook service class resources, including renamed JARs.
Parquet's separate, relocated Thrift implementation and `hive-storage-api`
remain intact. The profile now removes 63 JARs; the build validates 50 exact
replacements and 59 downloaded artifacts. Startup checks 40 class origins,
plus a separate embedded-Parquet-Thrift origin check in the image smoke.

Java 17, Spark 4.0.1, the matched Hadoop 3.4.3/SDK 2.35.4 pairing, existing
launch configuration and the existing 34 precedence copies are preserved.

## B. Why it was implemented

The archived [original LZ4 project](https://github.com/lz4/lz4-java) directs
users to the [maintained fork](https://github.com/yawkat/lz4-java). The latter
keeps the Java package names and Java 7+ compatibility. It documents that the
fast decompressor now uses Java safe code; the smoke therefore explicitly
selects the native **safe** decompressor and verifies its JNI class, rather
than mistaking a Java fallback for native coverage.

The [native extraction advisory](https://github.com/yawkat/lz4-java/security/advisories/GHSA-mcr4-qmvw-px4g)
identifies the shared-temporary-file issue, with the maintained fix beginning
at 1.11.4. The selected 1.14.1 also exceeds the maintained fixes for the
[output-buffer disclosure](https://github.com/yawkat/lz4-java/security/advisories/GHSA-cmp6-m4wj-q63q).
Its published Maven SHA512 matches the downloaded artifact. No additional
runtime dependencies are declared by its POM.

Earlier profile documentation treated standalone `libthrift` as a shared
Parquet dependency. Bytecode inspection disproved that assumption for this
profile: Parquet's generated structures reference
`shaded.parquet.org.apache.thrift`, embedded in its format-structures JAR.
The remaining standalone references belonged to Facebook's unused monitoring
RPC service. Removing both unused RPC modules avoids introducing a new Thrift
HTTP client dependency chain. No Commons Lang or HTTP5 upgrade is included.

A scan of JVM UTF8 constants in all 264 retained root JARs, covering 523,556
class files, found no direct class-name, descriptor or literal fully qualified
references to the removed packages. This does not certify dynamically built
reflection names or future Hive/server profiles; runtime tests cover the
selected streaming workload.

## C. Files changed

- `deploy/runtime/jars.lock.json`, `spark-jvm-patches.lock.json` and
  `patch_jars.py`: explicit maintained LZ4 coordinate and exact hash pin.
- `deploy/runtime/spark-features.lock.json`, `prune_features.py` and
  `runtime/features.py`: two bounded RPC removals and startup resource guards.
- `runtime/jvm.py`, `deploy/runtime/smoke_spark.py`: LZ4/XXHash/native class
  origins, JNI/Java checks, legacy block streams/Parquet and a shuffle check.
- Runtime feature, patch, origin, publication and release contract tests.
- This report, [sanitized evidence](evidence/aws_runtime_thrift_lz4_2026-11-16.json)
  and status links in the client-library and pilot-readiness reports.

README, local development behavior, root Python dependencies, CI workflows,
Terraform, Snowflake/dbt/Streamlit and existing cloud artifacts are preserved.

## D. Tests and validation

The focused suite passes **403 tests**, including old-coordinate rejection,
old/duplicate managed-download rejection, normal-directory installation,
original/replacement integrity and restored/renamed RPC resource rejection.
Ruff, the all-files secret scan, shell syntax and diff checks must pass before
commit. Exact committed-image and hosted CI results are observed after commit.

The Linux/AMD64 prototype passes with no network, read-only root, nonroot UID
10001, dropped capabilities and no-new-privileges. Tests exercise Java and JNI
compressors/decompressors, native versus Java XXHash, old raw LZ4 blocks and
lz4-java block streams captured from the previous image. These block streams
are Spark's format, not the standard LZ4 frame format. A bounded malformed
input must raise the expected exception in the Java safe implementation.

The smoke reads a tiny synthetic Parquet file produced by the previous image
and compares actual decoded IDs. It verifies the embedded Thrift class origin,
forces a multi-partition Spark shuffle and repeats new Parquet write/read.
Existing native TLS/crypto, CA, JSON, SDK/S3A construction and intentional
classpath-shadow rejection checks remain. Anonymous SDK construction makes no
requests and provides no evidence of live S3 authorization.

Synthetic streaming tests cover initial ingestion, Kafka restart, checkpoint
resume, repeat without duplicate rows and fail-closed offset loss. The harness
cleans only its isolated test-owned volumes. No production checkpoint or
volume is accessed. Frozen builds, identity-bound scans and recovery are
rechecked against the exact develop checkpoint; the prototype evidence is
explicitly identified as unpublished.

## E. Security review

| Spark finding scope | Previous | Candidate |
| --- | --- | --- |
| Library CRITICAL | 1 | 1 |
| Library HIGH | 43 | 38 |
| Library MEDIUM | 60 | 56 |
| Library LOW | 4 | 3 |
| Distinct HIGH/CRITICAL package/CVE pairs | 36 | 31 |
| OS HIGH | 47 | 47 |

The unsuppressed, same-database comparison removes two standalone Thrift and
three original-coordinate LZ4 HIGH findings, with **no introduced
HIGH/CRITICAL pairs**. Scanner inventory positively identifies the maintained
LZ4 coordinate/version and contains neither removed standalone RPC package.
The scan includes unfixed findings and all severities. Cached database times,
artifact hashes and scope limits are recorded in the evidence; this is not a
claim of exhaustive coverage of every newly disclosed vulnerability.

The existing SDK critical finding, shaded Hadoop/Parquet/Spark findings and OS
findings remain blockers. The previous Spark manifest is quarantined in both
publication planning and runtime release validation. No scanner suppression,
exception or deployment approval is added.

## F. Git commit and CI status

Work is based on `47815e9e4234bb574ef0b2331c0d5de204cebedd`, whose hosted run
`43973318321` passed all six jobs. The next develop checkpoint is created only
after local review and validation, followed by exact-source image/recovery
checks, identity-bound scans and observation of all six newly triggered jobs.
The final conversation result records that checkpoint and run. No main merge.

## G. Cost implications

This batch uses local Docker and standard public-repository CI. It creates no
billable AWS resources, publishes no ECR layers and authorizes no runtime
hours. Previously retained AWS resources can continue their existing charges;
no current billing or cloud-state query is performed in this batch.

## H. Remaining blockers

The SDK critical issue, remaining bundled library/OS HIGH findings and final
release review remain unresolved. Fresh scan coverage is required before a
later deployment decision. Live ECR digests for this replacement do not exist.
The separately gated Alchemy secret/IAM work, next-boot EBS mount verification,
live S3 smoke/recovery and bounded paid pilot remain outside this checkpoint.

## I. Next step

Continue offline review of the remaining shaded Hadoop, Parquet, Spark and SDK
dependencies and OS findings. Preserve the tested Hadoop/S3A/SDK pairing until
a compatible replacement is demonstrated. Then refresh the readiness report
with actual reviewed image digests and a concrete bounded pilot proposal.

## J. Whether production infrastructure changed

**No.** No AWS APIs, Terraform backend initialization, plan/apply/import,
resource changes, instance starts, EBS formatting, IAM/secret changes,
Snowflake changes or ECR publication occurred. Both production state locations
remain separate and untouched. Milestone 11.5D remains paused.
