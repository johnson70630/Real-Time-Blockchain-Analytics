# Milestone 11.6C — Guava, JLine and ZooKeeper maintenance

Reviewed 2026-11-16 UTC. **The release remains blocked.** This offline checkpoint
patches five standalone libraries, accepts no exceptions, publishes no images
and authorizes no paid pilot. Milestone 11.5D remains paused.

## A. What was implemented

| Component | Previous | Selected |
| --- | --- | --- |
| Guava, JRE variant | 33.4.0-jre | 33.7.2-jre |
| Guava failureaccess companion | 1.0.2 | 1.0.3 |
| JLine 3, jdk8 classifier | 3.27.1-jdk8 | 3.30.16-jdk8 |
| ZooKeeper client and Jute | 3.9.3 | 3.9.7 |

The installer now validates 49 exact replacements and 58 downloaded artifacts.
All five additions occupy only the normal Spark JAR directory. The existing 34
precedence copies remain unchanged. Startup verifies 37 actual class origins,
including the companion library, JLine parser/builtins/Telnet classes and both
ZooKeeper artifacts.

Guava's `-jre` variant and JLine's `-jdk8` classifier are retained explicitly.
JLine 2 remains a separate API line and is not silently replaced with JLine 3.
Download validation rejects older or duplicate managed JARs before installation,
in addition to the existing original-file and replacement checksum checks.

Java 17, Spark 4.0.1, the matched Hadoop 3.4.3/SDK 2.35.4 set, the existing
Jackson/compression fixes and application launch configuration are preserved.

## B. Why it was implemented

The [Guava 33.7.2 release](https://github.com/google/guava/releases/tag/v33.7.2)
fixes excessive allocation during collection deserialization and identifies
failureaccess 1.0.3 as its runtime companion. The empty listenablefuture artifact
is for dependency conflict avoidance, not additional runtime code. This classpath
pilot retains its existing annotation libraries and introduces no annotation
processor or JPMS configuration; the tests do not certify optional annotation
reflection paths against every version named in Guava's POM.

[JLine 3.30.16](https://github.com/jline/jline3/releases/tag/jline-3.30.16)
includes maintenance/security hardening while retaining the JLine 3 API line.
Its optional SSH/native-terminal dependencies are not enabled by the pilot.

The [ZooKeeper security page](https://zookeeper.apache.org/security/) recommends
3.9.7 for fixes beyond the scanner's minimum 3.9.5 recommendations. That is why
the matching 3.9.7 client/Jute pair was selected. Existing Netty 4.1.140,
tcnative 2.0.85, SLF4J 2.0.16, Commons IO 2.18.0 and audience annotations 0.12.0
provide the relevant client dependencies. Optional administration-server,
logging-backend and test dependencies are not added.

No ZooKeeper service, JLine Telnet/SSH listener or additional application service
is launched. Kafka continues to use its existing KRaft configuration. These are
libraries shipped with Spark, not new cloud architecture components.

## C. Files changed

- `deploy/runtime/jars.lock.json`, `deploy/runtime/spark-jvm-patches.lock.json`:
  five new artifact/hash pins and four family versions.
- `deploy/runtime/patch_jars.py`: JRE/classifier and separate-API handling,
  matching ZooKeeper artifact scope and complete managed-download inventory.
- `runtime/jvm.py`: ten additional class-origin checks and JLine classifier mapping.
- `deploy/runtime/smoke_spark.py`: Guava collection/cache/future checks, JLine
  parsing, Jute serialization and unstarted Curator client construction.
- `runtime/contracts.py`: quarantine of the earlier client-library image manifest.
- `tests/test_runtime_jvm.py`, `tests/test_runtime_jvm_patches.py`,
  `tests/test_runtime_images.py`, `tests/test_runtime_pilot.py`: filename/variant,
  duplicate/old download, API separation and rejected-manifest contracts.
- This report, [sanitized evidence](evidence/aws_runtime_client_libraries_2026-11-16.json)
  and status links in the standalone-library and pilot-readiness reports.

README, root Python project/lock, local development entrypoints, Terraform,
Snowflake/dbt/Streamlit and existing cloud artifacts are preserved.

## D. Tests and validation

The focused suite passed **386 tests**. Ruff, the all-files secret scan, shell
syntax and diff checks passed before the develop checkpoint. Exact committed
image checks and the hosted result are reported after observation.

The initial prototype failed safely because the new guard expected an
unclassified JLine filename. The corrected guard requires the reviewed `-jdk8`
file. A cross-manifest contract now compares runtime filenames with the actual
download pins, preventing this configuration regression from recurring.

The corrected Linux/AMD64 image passes all 37 class origins and the existing
version, profile, parser, compression, decoded-Parquet, native TLS, crypto,
CA-store and anonymous SDK/S3A construction checks. The smoke runs as UID 10001,
with read-only root, dropped capabilities, no-new-privileges and no network.
Intentional classpath shadowing must still produce its specific rejection error.

New tests exercise immutable collections, bounded cache eviction and the
Guava/failureaccess future API. They parse JLine input and round-trip a synthetic
ZooKeeper identity through Jute. Curator construction must remain `LATENT`;
`start()` is never called. This establishes local linkage, not remote ZooKeeper
session, optional terminal/SSH or server compatibility.

Synthetic streaming checks ingestion, Kafka restart, checkpoint resume, repeat
without duplicate rows and offset-loss rejection. Owned disposable volumes are
cleaned. Exact committed builds, scans, recovery and all six CI jobs are observed
after commit. Live AWS/S3 access remains separately gated.

## E. Security review

The same verified Trivy 0.76.2 binary and cached database snapshot scan the full
final filesystem, all severities and unfixed issues, OS and language packages,
without an ignore file. No bundle or vulnerability metadata is rewritten.

| Spark findings | Baseline `9285013` | Corrected prototype |
| --- | ---: | ---: |
| Library critical | 1 | 1 |
| Library high | 49 | 43 |
| Library medium | 63 | 60 |
| Library low | 4 | 4 |
| OS critical | 0 | 0 |
| OS high | 47 | 47 |
| Unique high/critical package-advisory pairs | 38 | 36 |

No new high/critical package-advisory pair appears. ZooKeeper CVE-2026-24281 and
CVE-2026-24308 disappear entirely. Standalone Guava and JLine occurrences are
removed, while affected copies remain inside Hadoop/Spark bundles. Vendor
security review also informed the 3.9.7 selection; the frozen scanner database
is not presented as an exhaustive list of current vendor advisories.

Remaining high library occurrences are SDK bundle 15, Hadoop client runtime 12,
Parquet Jackson 8, Spark core/network bundles 3, and standalone Thrift/LZ4 5.
The SDK's relocated Netty Critical finding also remains. All findings stay open
with no accepted exceptions; the release is not security-cleared.

All five new files have SHA-512 pins. Guava and JLine publish SHA-512;
failureaccess and both ZooKeeper artifacts publish SHA-1, checked over HTTPS before
computing committed SHA-512 pins. The evidence distinguishes those sources and
does not claim independent publisher-signature verification.

The verified cache is 783,667,723 bytes, below the unchanged 1 GiB limit. There
are no runtime dependency downloads, credential additions, IAM expansions,
listener changes or TLS-verification bypasses.

## F. Git commit and CI status

Verified starting develop SHA: `9285013eaf3c605bcccab4df7fab13d004eb0d22`.
[CI 43968456998](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/43968456998)
passed all six jobs. The new change is committed/pushed on develop after local
review; its exact SHA, committed-image checks and automatically triggered CI
result are reported after observation. Main remains unmerged.

## G. Cost implications

No AWS resource, paid compute or ECR publication occurs. Local verified build
inputs grow by 6,170,791 bytes. The existing six standard Ubuntu CI jobs, runner
class, input cap, quotas and billing settings are unchanged. Existing retained
cloud costs continue; publication and a bounded paid pilot need separate approval.

## H. Remaining blockers

- One Critical / 43 high Spark library occurrences and 47 high OS occurrences.
- No accepted exceptions or approved replacement ECR deployment digests.
- Reviewed Alchemy secret ARN and exact workload-role/boundary permissions.
- Next-boot EBS mount, live S3/checkpoint recovery and pilot acceptance.

Old publication and one-time filesystem-initialization approvals are consumed.
The initialized Kafka volume must never be formatted again. No production backend
initialization or authenticated production Terraform plan is performed.

## I. Next step

Review the remaining Thrift and LZ4 compatibility paths separately, then handle
embedded Parquet/Hadoop/Spark/SDK dependencies as coordinated sets. Refresh
security evidence before any eventual deployment review. Keep exact digests,
remaining findings and bounded decision requirements visible; a passing local
or hosted test does not grant deployment approval.

## J. Whether production infrastructure changed

**No.** No AWS APIs, authenticated production Terraform commands, IAM/Snowflake
changes, ECR pushes, EC2 starts, S3 writes, secret changes or EBS operations occur.
No new live EC2-state observation is claimed. The platform/runtime Terraform
states remain separate and untouched; 11.5D remains paused.
