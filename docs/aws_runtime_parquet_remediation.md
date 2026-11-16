# Milestone 11.6C — Complete Parquet cohort maintenance

Reviewed 2026-11-16 UTC. **Deployment remains blocked.** This is an offline
checkpoint, with no accepted security exceptions, ECR publication or paid
runtime authorization. Milestone 11.5D remains paused.

## A. What was implemented

The cloud Spark image updates all six Parquet modules from 1.15.2 to 1.18.2:
column, common, encoding, format-structures, Hadoop and Jackson. The new
Parquet-Jackson artifact contains its own relocated Jackson core/databind
2.22.4 and annotations 2.22. Replacing standalone Jackson cannot fix that
isolated namespace; this checkpoint replaces the whole published vendor JARs.

The complete Parquet runtime dependency review also requires:

| Artifact | Previous | Selected |
| --- | --- | --- |
| zstd-jni | 1.5.6-9 | 1.5.7-24 |
| commons-pool, original API line | 1.5.4 | 1.6 |
| jts-core | absent | 1.20.0 |
| javax.annotation-api | absent | 1.3.2 |

Existing Snappy 1.1.10.12 and Aircompressor 2.0.3 already match the selected
Parquet requirements. SLF4J 2.0.16 remains Spark's existing compatible API.
Commons Pool 2 and Jakarta annotations are separate APIs and remain intact.
Test/provided dependencies and optional Brotli support are not added. The
Parquet modules embed their own relocated fastutil/Thrift dependencies.

The installer requires a complete matched Parquet replacement cohort and
checks the added runtime dependencies in the download lock. Old, duplicate,
missing and mixed inputs fail validation. All ten selected artifacts are
pinned by SHA512. The build now validates 58 exact replacements and 69 downloads,
using 801,572,009 verified Maven bytes, below its 1 GiB cap. The 34 precedence
copies and 63 optional whole-JAR removals remain unchanged. Startup verifies
51 actual JVM class origins, including every Parquet module and its additions.

## B. Why it was implemented

The [Parquet 1.18.2 release](https://github.com/apache/parquet-java/releases/tag/apache-parquet-1.18.2)
includes dependency maintenance and reader/writer fixes. Its published
[POM](https://repo.maven.apache.org/maven2/org/apache/parquet/parquet/1.18.2/parquet-1.18.2.pom),
artifact checksums and actual embedded metadata establish the selected versions.
The binary's Jackson metadata reports 2.22.4, which is recorded rather than
assuming that every release-note entry gives the final artifact version.
Java 11 bytecode in the core modules is compatible with the pilot's Java 17.

The SDK review was refreshed first. Current public metadata still identifies
Hadoop 3.5.0 as latest; its POM still selects SDK 2.35.4. The latest SDK POM,
2.59.1, declares newer Netty but does not establish a tested Hadoop replacement.
[Hadoop's S3A guidance](https://hadoop.apache.org/docs/r3.4.3/hadoop-aws/tools/hadoop-aws/troubleshooting_s3a.html)
warns against arbitrary SDK substitutions. Therefore this batch preserves
Hadoop 3.4.3/SDK 2.35.4 and leaves the critical SDK finding open. No private
Hadoop/SDK rebuild or security applicability exception is introduced.

## C. Files changed

- `deploy/runtime/jars.lock.json`, `spark-jvm-patches.lock.json`, `patch_jars.py`:
  whole-cohort pins, required additions and version/inventory guards.
- `runtime/jvm.py`: eleven more class-origin checks, including relocated
  Jackson/Thrift, native Zstandard, Pool, JTS and Javax annotations.
- `deploy/runtime/smoke_spark.py`: typed Parquet and native/dependency checks;
  existing legacy-file, launch, TLS, JSON and S3A checks are preserved.
- `runtime/contracts.py` and runtime tests: quarantine of the previous image,
  cohort/dependency integrity and old-publication/release rejection.
- This report, [sanitized evidence](evidence/aws_runtime_parquet_2026-11-16.json)
  and status links in the prior report and pilot-readiness report.

README, root Python dependencies, local development behavior, CI workflows,
Terraform, Snowflake/dbt/Streamlit and existing cloud artifacts are preserved.

## D. Tests and validation

The focused suite passes **439 tests**. It covers every required Parquet module,
mixed module versions, missing/old/duplicate runtime additions, old codec
artifacts, original/replacement hashes, native-version filenames and actual
class-origin requirements. Ruff, the all-files secret scan, shell syntax and
diff checks must pass before commit. Exact committed builds, recovery and
identity-bound scans are observed after commit, followed by all six CI jobs.

The Linux/AMD64 prototype runs with no network, nonroot UID 10001, a read-only
root filesystem, dropped capabilities and no-new-privileges. It passes all 51
class-origin checks and the intentional classpath-shadow rejection. The smoke
constructs anonymous S3A/SDK clients without making requests.

Data checks read the previous image's fixed Parquet file and compare decoded
values. New writes round-trip high-precision decimal(38,18), microsecond
timestamps, binary bytes, arrays, maps, a nested struct, nulls and empty
collections through both Snappy and Zstandard. Both vectorized and scalar
readers must return the same values, including a filtered read. The smoke
also exercises the relocated Jackson parser, native Zstandard, JTS geometry
construction and Pool lifecycle. It does not certify optional geospatial
Spark extensions, every Parquet encoding or all third-party readers.

The cross-version recovery harness ingests its initial rows with the previous
committed Parquet 1.15.2 image, then resumes with the 1.18.2 prototype against
the same synthetic Kafka logs, Bronze files and Spark checkpoint. It checks
broker restart, continued progress, mixed-file reads, repeat without duplicate
rows and fail-closed offset loss. Only isolated test-owned volumes are cleaned.
A second recovery pass uses the exact committed new images. No production
volume, checkpoint or S3 path is accessed.

## E. Security review

| Spark scope | Previous | Candidate |
| --- | --- | --- |
| Library CRITICAL occurrences | 1 | 1 |
| Library HIGH occurrences | 38 | 30 |
| Library MEDIUM occurrences | 56 | 47 |
| Library LOW occurrences | 3 | 3 |
| Distinct HIGH/CRITICAL package/CVE pairs | 31 | 29 |
| OS HIGH occurrences | 47 | 47 |

The same-database, unsuppressed comparison introduces **no new HIGH/CRITICAL
package/CVE pair**. All eight HIGH occurrences in the old Parquet-Jackson JAR
are absent from its replacement. Only two distinct package/CVE pairs disappear
image-wide: six affected pairs still exist in other bundled copies. Those
remaining copies are not silently treated as fixed.

Trivy inventory identifies the six new Parquet modules and all required runtime
dependencies. Scans include unfixed findings and all severities. Exact tool,
database and public-input hashes/timestamps are retained in the evidence; cached
coverage is not a guarantee of every newly disclosed vulnerability. The prior
Spark manifest remains rejected by publication planning and release validation.

The SDK critical issue, 30 remaining library HIGH occurrences and 47 OS HIGH
occurrences remain blockers. There are no scanner suppressions, metadata
removals, accepted exceptions or new deployment permissions.

## F. Git commit and CI status

Starting develop SHA: `3505f95f962eaa0dbc7e297f80134aa1c4ad0e79`.
[CI 43976809375](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/43976809375)
passed all six jobs. The next reviewed develop checkpoint is committed/pushed
only after local validation. Exact-source image/scan/recovery results and the
newly triggered six-job CI result are observed afterwards and recorded in the
final conversation result. Main remains unmerged.

## G. Cost implications

No billable cloud resources, ECR layers or runtime hours are created or
authorized. This batch uses public source reads, local Docker and existing
standard public-repository CI. Retained AWS resources can continue existing
charges; this batch makes no current billing or cloud-state queries.

## H. Remaining blockers

The unresolved SDK critical issue, remaining Hadoop/Spark/SDK library findings,
OS findings and final release review remain open. Fresh scans are required
before a deployment decision. No approved replacement ECR deployment digests
exist. The separately gated Alchemy secret/IAM work, next-boot EBS mount check,
live S3/checkpoint tests and bounded paid pilot are not performed here.

## I. Next step

Continue offline remediation or explicit applicability review of the remaining
bundled Hadoop/Spark/SDK and OS findings. Preserve compatible component sets.
Then refresh the deployment-readiness proposal with actual reviewed image
digests and exact duration/cost bounds before requesting pilot approval.

## J. Whether production infrastructure changed

**No.** No AWS API, Terraform backend initialization, plan/apply/import,
instance start, EBS operation, IAM/secret/Snowflake change or ECR publication
occurs. The two Terraform states remain separate and untouched. The initialized
Kafka volume is never reformatted. Milestone 11.5D remains paused.
