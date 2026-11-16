# Milestone 11.6C — standalone JSON and compression maintenance

Latest maintenance: [Guava, JLine and ZooKeeper updates](aws_runtime_client_library_remediation.md)
reduces Spark library findings to 1 critical / 43 high, with no new high/critical
pairs. The release remains blocked with no accepted exceptions.

Reviewed 2026-11-16 UTC. **The release remains blocked.** This offline checkpoint
updates compatible standalone libraries. It accepts no vulnerability exceptions,
publishes no images and authorizes no paid pilot. Milestone 11.5D remains paused.

## A. What was implemented

The cloud Spark build replaces eight more exact, hash-reviewed JARs:

| Component | Previous | Selected | JARs |
| --- | --- | --- | ---: |
| Jackson annotations/core/databind/YAML/Java time/Scala | 2.18.2 | 2.18.12 | 6 |
| Aircompressor | 2.0.2 | 2.0.3 | 1 |
| Snappy Java | 1.1.10.7 | 1.1.10.12 | 1 |

The existing installer validates every original and replacement hash before
changing any installed file. Its complete inventory now covers 44 replacements.
Legacy Codehaus JSON remains under the separate 61-JAR feature-removal policy;
it cannot be treated as modern Jackson. Unknown or mixed modern JARs fail closed.

These eight libraries occupy only the normal Spark JAR directory, alongside the
matched Hadoop clients. The 34-family precedence copies remain for the existing
Netty/Bouncy Castle/Jersey patches. This avoids redundant new library copies.
Startup now checks 27 actual class origins, including all six Jackson artifacts,
Aircompressor and Snappy, before application S3 work.

The SHA-512 download lock grows from 45 to 53 artifacts / 777,496,932 bytes,
below the unchanged 1 GiB verified-input limit. Hadoop 3.4.3, SDK 2.35.4,
Java 17, Spark 4.0.1, Kafka and the cloud launch settings are preserved.

## B. Why it was implemented

Standalone dependencies can be patched without replacing code embedded inside
other vendors' bundles. Jackson stays within Spark's existing 2.18 line, with
all six modules aligned to its published BOM. The
[2.18.11](https://github.com/FasterXML/jackson/wiki/Jackson-Release-2.18.11) and
[2.18.12 release notes](https://github.com/FasterXML/jackson/wiki/Jackson-Release-2.18.12)
describe security fixes in that maintenance line.

The [Aircompressor advisory](https://github.com/airlift/aircompressor/security/advisories/GHSA-vx9q-rhv9-3jvg)
identifies 2.0.3 as a fixed release for reused-output-buffer disclosure. Selecting
that backport preserves the existing API line and Java requirements. Snappy
1.1.10.12 includes the earlier security fixes and a
[Hadoop block-header compatibility correction](https://github.com/xerial/snappy-java/releases/tag/v1.1.10.12).
Actual Java/native and streaming tests remain necessary despite patch-version
compatibility expectations.

POM review found no missing runtime dependencies for this batch. The existing
SnakeYAML 2.3 and Paranamer 2.8 satisfy the declared versions; Scala 2.13.16 is in
the same binary line as the Scala module's declared 2.13.15. Aircompressor's
Hadoop adapter and Snappy's OSGi API are provided dependencies; test dependencies
are not added to the image. The selected application paths pass the container
checks below, without claiming every optional adapter is exercised.

## C. Files changed

- `deploy/runtime/jars.lock.json`, `deploy/runtime/spark-jvm-patches.lock.json`:
  eight new artifact/hash pins and three additional family versions.
- `deploy/runtime/patch_jars.py`: modern JSON/compression inventory validation,
  exact Jackson Maven groups and normal-directory staging without duplicate copies.
- `runtime/jvm.py`: eight additional class-origin checks.
- `deploy/runtime/smoke_spark.py`: JSON/YAML/time/Scala and parser-limit checks;
  native Snappy and Aircompressor Snappy/LZ4 round trips and legacy-frame reads.
- `runtime/contracts.py`: quarantine of the earlier standalone-dependency manifest.
- `tests/test_runtime_jvm.py`, `tests/test_runtime_jvm_patches.py`,
  `tests/test_runtime_images.py`, `tests/test_runtime_pilot.py`: shadowed sources,
  nonduplicated patches, separate legacy handling and rejected-manifest contracts.
- This report, [sanitized evidence](evidence/aws_runtime_standalone_2026-11-16.json)
  and status links in the SDK and pilot-readiness reports.

README, root Python project/lock, local development entrypoints, Terraform,
Snowflake/dbt/Streamlit and existing cloud artifacts are preserved.

## D. Tests and validation

The focused local suite passed **355 tests**. Ruff, all-files secret scanning,
shell syntax and diff checks passed before commit. Exact committed-image checks
and the hosted CI result are reported after observation.

The Linux/AMD64 prototype passes all existing engine, classpath, feature-profile,
SDK/S3A anonymous construction, epoll/native TLS, cryptographic, CA-store and
Parquet checks. It runs as UID 10001 with read-only root, dropped capabilities,
no-new-privileges and no network. The wrong-classpath gateway must still fail
with its specific guard error.

New checks exercise JSON parsing, YAML parsing, Java time serialization and a
Scala map through the aligned Jackson modules. A bounded nesting-limit test
requires the exact Jackson constraint exception. Native Snappy and Aircompressor
Snappy/LZ4 each perform byte-for-byte round trips. They also decode fixed
synthetic frames generated by the baseline image's Snappy 1.1.10.7 and
Aircompressor 2.0.2. The Parquet check also compares decoded ID values `[0, 1, 2]`.
The smoke keeps those small frames inline; capture provenance
and hashes are recorded in the evidence. This is compressed-frame compatibility,
not a claim that every historical file or optional codec format was tested.

Synthetic Kafka integration checks ingestion, broker restart, checkpoint resume,
repeat without duplicate output and offset-loss rejection. Only owned disposable
volumes are cleaned. Exact committed builds, scans, recovery and all six hosted
CI jobs are checked afterwards. No live AWS/S3 access is established here.

## E. Security review

The same verified Trivy 0.76.2 binary and cached database snapshot scan the final
filesystem, OS and language packages, all severities and unfixed findings, with
no ignore file. No embedded bundle or vulnerability metadata is rewritten.

| Spark findings | Baseline `f209652` | Prototype |
| --- | ---: | ---: |
| Library critical | 1 | 1 |
| Library high | 60 | 49 |
| Library medium | 73 | 63 |
| Library low | 4 | 4 |
| OS critical | 0 | 0 |
| OS high | 47 | 47 |
| Unique high/critical package-advisory pairs | 41 | 38 |

No new high/critical package-advisory pair appears. Aircompressor
CVE-2025-67721 and Snappy CVE-2026-108106 / CVE-2026-93451 disappear entirely.
The standalone Jackson occurrences disappear, but affected copies remain inside
Hadoop and Parquet. Counts of occurrences and distinct package/advisory pairs
therefore change by different amounts.

The remaining library findings are routed explicitly:

| Location/group | Critical | High | Next treatment |
| --- | ---: | ---: | --- |
| SDK bundle 2.35.4 | 1 | 15 | Preserve matched Hadoop dependency; complete specific applicability/remediation review. |
| Hadoop client runtime 3.4.3 | 0 | 12 | Relocated dependencies require a matched vendor change or reviewed rebuild. |
| Parquet Jackson 1.15.2 | 0 | 8 | Review the coordinated Parquet dependency set; standalone Jackson does not patch it. |
| Spark core/network bundles | 0 | 3 | Embedded Jetty/Guava need separate review. |
| Other standalone libraries | 0 | 11 | Guava, JLine, Thrift, LZ4 and ZooKeeper remain open. |

The SDK Critical finding and all remaining high library/OS findings keep the
release blocked. The earlier SDK applicability candidate is still unaccepted.
This batch does not accept exceptions for inactive features or suppress reports.

All eight artifacts are pinned by SHA-512. Six Jackson artifacts publish
SHA-512; Aircompressor and Snappy publish SHA-1, checked over HTTPS before their
committed SHA-512 pins were computed. The evidence records this distinction;
it does not claim independent publisher-signature verification. No runtime
resolver, credential embedding, IAM expansion or TLS-verification bypass is added.

## F. Git commit and CI status

Verified starting develop SHA: `f2096521e142958083d50337690ea8205c6dc636`.
[CI 43963742487](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/43963742487)
passed all six jobs. The reviewed change is committed/pushed on develop only
after local validation. Its exact SHA, committed-image checks and automatically
triggered CI result are reported after observation. Main remains unmerged.

## G. Cost implications

No AWS resource, paid runtime or ECR publication occurs. The local verified
build inputs grow by 5,753,101 bytes; image/cache storage and the existing six
standard Ubuntu CI jobs are used. No runner, quota, billing or input-limit
change is made. Existing retained cloud charges continue. A later publication
and bounded paid pilot still need separate approval.

## H. Remaining blockers

- One Critical / 49 high Spark library occurrences and 47 high OS occurrences.
- No accepted exceptions or approved replacement ECR deployment digests.
- Reviewed Alchemy secret ARN and exact workload-role/boundary permissions.
- Next-boot EBS mount, live S3 writes/checkpoint recovery and pilot acceptance.

Earlier publication and filesystem-initialization approvals are consumed. The
initialized Kafka volume must never be formatted again. No production Terraform
backend is initialized or authenticated production plan run.

## I. Next step

Review the remaining standalone libraries for compatible fixed releases or
unneeded whole-feature modules. Handle Parquet/Hadoop/Spark embedded copies as
separate dependency sets. Keep scanner results and actual code locations visible
while preparing any bounded, digest-specific security decision. Resolve the
remaining gates before requesting publication or paid-pilot approval.

## J. Whether production infrastructure changed

**No.** No AWS APIs, authenticated production Terraform commands, IAM/Snowflake
actions, ECR pushes, EC2 starts, S3 writes, secret changes or EBS operations occur.
No new live EC2-state observation is claimed. The runtime/platform Terraform
states remain separate and untouched; 11.5D stays paused.
