# Milestone 11.6C — minimal cloud streaming distribution

Latest review: [SDK applicability and cloud launch correction](aws_runtime_sdk_review.md)
fixes Compose dropping the reviewed classpaths and verifies the SDK namespace
separately. Findings remain open; no exception or deployment is approved.

Reviewed 2026-11-16 UTC. **The release remains blocked.** This offline checkpoint
removes unused executable modules from the cloud image and accepts no security
exceptions. It authorizes neither image publication nor an AWS pilot. Milestone
11.5D remains paused.

## A. What was implemented

The cloud Spark image has a fixed `local_streaming_pilot` profile:

| Excluded feature | Whole JARs removed |
| --- | ---: |
| Spark Connect Java server/client and remote REPL | 42 |
| Hive catalog/metastore modules | 9 |
| Hive JDBC/Thrift server modules | 5 |
| Embedded Derby | 3 |
| Legacy Codehaus Jackson core/mapper | 2 |
| Total | 61 |

The manifest records the original path, size and SHA-512 of every removal.
The build checks the complete selected inventory and every hash before deleting
any listed file. Unknown versions, partial inventories, corrupt files, symlinks,
traversal and a conflicting receipt fail closed. An exact repeat requires the
matching prior receipt. No recursive or wildcard deletion is used.

The image retains Spark SQL, Kafka, Parquet, `hive-storage-api`, shared Thrift
serialization and the coordinated Hadoop 3.4.3/S3A/SDK dependencies. Java 17,
PySpark 4.0.1 and the earlier security-family patches are preserved. The profile
is deliberately restricted to the existing local Spark streaming application;
it is not a general-purpose Connect or Hive distribution. Python Connect API
files are not separately removed.

Before application S3 work, startup validates the installed profile, rejects
restored excluded modules and checks that representative excluded class resources
cannot be resolved by the actual Java context classloader. This complements the
existing 16 positive class-origin checks, in-memory catalog and classic S3A guard.
The full earlier Spark manifest is quarantined from release/publication planning.

## B. Why it was implemented

The pilot runs one local Spark driver/executor, consumes Kafka and writes Parquet.
It does not need a remote Spark Connect server, Hive metastore, SQL Thrift server,
embedded database or legacy JSON mapper. Removing those complete code modules
reduces installed attack surface without treating an inactive feature as a
vulnerability exception.

Spark documents optional Hive/server build profiles in
[Building Spark](https://spark.apache.org/docs/4.0.1/building-spark.html).
[Hive tables](https://spark.apache.org/docs/4.0.1/sql-data-sources-hive-tables.html)
describes enabling Hive support and its metastore dependencies, while
[Spark Connect](https://spark.apache.org/docs/4.0.1/spark-connect-overview.html)
describes a separate client/server architecture. These sources establish feature
boundaries; the container and recovery tests establish compatibility with this
specific demonstration workload.

## C. Files changed

- `deploy/runtime/spark-features.lock.json`: 61 exact original JAR hashes/sizes.
- `deploy/runtime/prune_features.py`: build-only inventory/checksum validation
  and exact removal with a repeatable receipt.
- `runtime/features.py`: shared feature policy, path/inventory validation and
  runtime class-resource guard.
- `deploy/runtime/Dockerfile.spark`: cloud-only profile application after the
  existing matched dependency installation.
- `runtime/jvm.py`, `deploy/runtime/smoke_spark.py`: startup verification and a
  real Java classloader test for a renamed excluded class resource.
- `runtime/contracts.py`: quarantine of the prior full-feature Spark manifest.
- `tests/test_runtime_features.py`, `tests/test_runtime_jvm.py`,
  `tests/test_runtime_images.py`, `tests/test_runtime_pilot.py`: integrity,
  partial-removal, restored-class and rejected-manifest coverage.
- This report, [sanitized evidence](evidence/aws_runtime_streaming_profile_2026-11-16.json)
  and links in the Hadoop and pilot-readiness reports.

README, root Python project/lock, local development entrypoints, Terraform,
Snowflake/dbt/Streamlit and existing cloud artifacts are preserved.

## D. Tests and validation

The focused suite covers the profile and existing image, bundle, pipeline,
deployer-policy, Terraform-foundation and CI contracts. Ruff, secret scanning,
shell syntax and diff checks are required before the develop checkpoint.
The focused local suite passed **330 tests**; Ruff, secret scanning, shell syntax
and diff checks passed. The hosted result is reported after observation.

The Linux/AMD64 prototype passes Java/Spark/Hadoop/Kafka version checks, actual
class origins, excluded-class absence, native epoll/TLS, Bouncy Castle AES-GCM,
Jersey, Java CA-store verification and Parquet round-trip I/O. The application
smoke runs as UID 10001 with read-only root, dropped capabilities,
no-new-privileges and no network.

A synthetic resource in a renamed JAR is presented through a real child Java
classloader. The profile must reject it with its specific guard error. The
resource is never executed, and the driver's own classpath remains unchanged.
The existing negative gateway test still rejects wrong classpath precedence
before Spark RPC initialization.

SDK sync/async S3 and S3A construction uses anonymous credentials, disabled
bucket probing and no request methods. This validates linkage without asserting
live AWS access. Synthetic streaming validates initial ingestion, broker restart,
checkpoint resume, repeat without duplicate output and offset-loss rejection.
Only owned disposable test volumes are cleaned. Exact committed builds, scans,
recovery and all six CI jobs are checked after the develop commit.

## E. Security review

The verified Trivy 0.76.2 binary and the same cached database snapshot scan the
complete final filesystem, OS and language packages, all severities and unfixed
issues. No ignore file, metadata stripping, relocated-bundle rewriting or
vulnerability exception is used. Raw logs/reports remain private; the evidence
records their hashes and local image identities.

| Spark findings | Baseline `1b3fb7d` | Streaming profile prototype |
| --- | ---: | ---: |
| Library critical | 4 | 1 |
| Library high | 78 | 60 |
| Library medium | 91 | 73 |
| Library low | 6 | 4 |
| OS critical | 0 | 0 |
| OS high | 47 | 47 |
| Unique high/critical package-advisory pairs | 45 | 41 |

No new high/critical package-advisory pair appears. Four distinct pairs disappear:
Derby CVE-2022-46337, legacy Jackson CVE-2019-10172 / CVE-2019-10202 and Netty HTTP/2
CVE-2025-55163. Raw occurrence reductions also include duplicate copies that were
inside the removed modules. Other copies of several affected components remain.

**The AWS SDK bundle still contains Netty 4.1.126 with critical
CVE-2026-75595.** Patched standalone Netty does not update its relocated copy.
The 60 high library occurrences and 47 high OS occurrences remain unresolved.
The application is not security-cleared or approved for publication/deployment.

The profile removes 92,035,605 bytes from the final installed filesystem.
Earlier image layers can retain deleted bytes, so this is not a claim of reduced
compressed image size or ECR storage cost. The 45-artifact download lock and
1 GiB verified build-input limit stay unchanged. No new dependencies, runtime
downloads, secrets, credential stores or IAM permissions are introduced.

## F. Git commit and CI status

Verified starting develop SHA: `1b3fb7d2b2cbe5d07f951b5bb9d8f12c89dc0807`.
[CI 43834995798](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/43834995798)
passed all six jobs. The reviewed change is committed/pushed on develop only
after local checks pass. Its exact SHA, committed image identities and triggered
CI result are reported after observation. No main merge is authorized.

## G. Cost implications

No AWS resource, paid runtime or image publication occurs. Local build/cache
storage and existing standard Ubuntu CI jobs are used. There are no billing,
runner, quota, cache-limit or job-count changes. Existing retained cloud costs
continue. The earlier approximately $24/month, 40-hour demonstration estimate
remains provisional and is not spending authorization.

## H. Remaining blockers

- One critical / 60 high Spark library occurrences and high OS findings.
- No accepted exceptions or replacement ECR deployment digests.
- Reviewed Alchemy secret ARN and exact workload-role/boundary permissions.
- Next-boot EBS mount, live S3 writes/checkpoint recovery and pilot acceptance.

Historical ECR publication and one-time filesystem-initialization approvals are
consumed. The initialized Kafka volume must never be formatted again. This
offline checkpoint performs no production Terraform initialization or planning.

## I. Next step

Review the retained SDK's relocated Netty implementation and supported matched
dependency options. Shared libraries still needed by Spark/Parquet must not be
removed solely to lower scanner counts. Keep digest-specific findings visible
until fixed or explicitly reviewed with bounded controls and expiration. Resolve
the remaining gates before requesting a concrete publication or paid-pilot approval.

## J. Whether production infrastructure changed

**No.** No AWS APIs, authenticated production Terraform commands, IAM/Snowflake
actions, ECR pushes, EC2 starts, S3 writes, secret changes or EBS operations occur.
The historical stopped-instance observation is not a new live-state check. The
platform and runtime Terraform states remain separate and untouched.
