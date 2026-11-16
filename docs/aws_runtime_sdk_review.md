# Milestone 11.6C — SDK review and cloud launch correction

Latest maintenance: [standalone JSON and compression remediation](aws_runtime_standalone_remediation.md)
updates aligned Jackson modules and compression libraries, reducing Spark library
findings to 1 critical / 49 high. The release remains blocked with no exceptions.

Reviewed 2026-11-16 UTC. **The release remains blocked.** This checkpoint fixes
a cloud startup configuration error and records the SDK finding's compatibility
and applicability evidence. It accepts no exception, publishes no image and
authorizes no paid pilot. Milestone 11.5D remains paused.

## A. What was implemented

The cloud Compose file previously set `PYSPARK_SUBMIT_ARGS` to only a 3 GiB
driver-memory option. That replaced the image's driver/executor classpath
settings. The image could pass its isolated smoke yet fail its class-origin guard
when launched by cloud Compose, before S3 work.

The image now owns the complete canonical launch configuration: 3 GiB driver
heap and both reviewed classpaths. Compose inherits it without overriding the
environment variable. A deployment contract rejects reintroducing that override;
the real JVM smoke checks the configured memory and actual maximum heap.

Startup now verifies 19 class origins. The three additions are the SDK async
HTTP client, relocated Netty `SslClientHelloHandler` and relocated Apache SSL
socket factory. These must come from the exact matched SDK bundle, separately
from the patched standalone Netty family. The smoke also constructs an SDK
namespace TLS client context without making requests. The earlier full image
manifest remains quarantined; an applicability review cannot clear it for use.

## B. Why it was implemented

The SDK 2.35.4 bundle contains 2,490 relocated Netty classes and no unshaded
`io/netty` class files. Its Netty handler metadata reports 4.1.126.Final.
Standalone Netty 4.1.140 therefore cannot update that separate implementation.
A stale Dockerfile comment suggesting otherwise has been corrected.

There is no fixed drop-in vendor cohort among the releases inspected:

| Published input | SDK dependency | Relevant Netty version | Decision |
| --- | --- | --- | --- |
| Hadoop 3.4.3 project POM | 2.35.4 | SDK POM: 4.1.126.Final | Retained matched set; finding stays open. |
| Hadoop 3.5.0 project POM | 2.35.4 | SDK POM: 4.1.126.Final | Does not fix this SDK finding. Other upgrades need separate tests. |
| SDK 2.59.1 project POM | 2.59.1 | 4.1.138.Final | Newer dependency declaration, not a tested Hadoop replacement. |

The third row is POM evidence only; that bundle was not downloaded, installed or
scanned. Hadoop's [S3A troubleshooting guidance](https://hadoop.apache.org/docs/r3.4.3/hadoop-aws/tools/hadoop-aws/troubleshooting_s3a.html)
requires its matching SDK and warns that a safe version change requires
recompilation. Hadoop's actual `ConfigureShadedAWSSocketFactory` bytecode/source
also references SDK-relocated Apache socket classes. Merely replacing the bundle
with ordinary modular SDK JARs would leave those required names unavailable.
AWS's [Maven setup guidance](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/setup-project-maven.html)
requires consistent SDK module versions; a BOM does not supply Hadoop's shaded
class names or establish binary compatibility.

## C. Files changed

- `deploy/runtime/Dockerfile.spark`, `deploy/runtime/compose.yaml`: one inherited
  cloud launch configuration, preserving the planned heap and classpath guards.
- `runtime/jvm.py`, `deploy/runtime/smoke_spark.py`: SDK-specific class origins,
  real JVM heap check and relocated TLS client-context construction.
- `runtime/contracts.py`: rejection of the reviewed unresolved baseline manifest.
- `tests/test_runtime_jvm.py` existing parameterized coverage automatically tests
  the new class origins; `tests/test_runtime_pilot.py` and
  `tests/test_runtime_images.py` add launch and unaccepted-review contracts.
- This report, [sanitized evidence](evidence/aws_runtime_sdk_review_2026-11-16.json)
  and current-status links in the streaming-profile and pilot-readiness reports.

No README, root Python project/lock, local development entrypoint, dependency
lock, Terraform, Snowflake/dbt/Streamlit or production artifact is changed.

## D. Tests and validation

The focused local suite passed **336 tests**. Ruff, the all-files secret scan,
shell syntax and diff checks passed before the develop checkpoint. The exact
committed builds and hosted CI result are reported after observation.

The Linux/AMD64 candidate passes version checks, all 19 actual class origins,
profile exclusions, renamed-resource rejection, native epoll/TLS, Bouncy Castle
AES-GCM, Jersey, SDK/S3A anonymous construction, CA-store checks and Parquet I/O.
The positive smoke verifies a real 3 GiB Java heap and a client-mode JDK TLS context
inside the SDK namespace. No request methods are called; network is disabled.
The wrong-classpath gateway test still requires its specific guard error.

Synthetic Kafka tests cover initial ingestion, broker restart, checkpoint resume,
repeat without duplicate rows and offset-loss rejection. Only owned disposable
volumes are cleaned. Exact committed builds, scans, recovery and all six hosted
CI jobs are observed afterwards. Live S3 access remains untested.

## E. Security review

The original [Netty advisory](https://github.com/netty/netty/security/advisories/GHSA-c4c3-7fpv-j4q5)
describes an inbound TLS server failure: a fragmented ClientHello causes SNI
fallback. An authentication bypass requires per-SNI mutual TLS to be the sole
gate, a permissive fallback context and no secondary peer-certificate check.
Patched Netty versions are 4.1.137.Final and 4.2.17.Final.

The proposed pilot uses outbound S3 TLS clients, local Spark execution, no Spark
UI and internal plaintext Kafka. Its committed Compose has no published ports,
and Terraform declares no security-group ingress. The SDK source builds its TLS
context with `SslContextBuilder.forClient()`. **The inference is that this
specific exploit's server conditions are not configured in the proposed pilot.**
That is a candidate applicability disposition, not proof that every SDK path is
unreachable or that the installed vulnerable class is absent.

The upstream repository page currently labels the advisory High, while the
cached Trivy database/GitHub Advisory Database classify it Critical. This review
retains the scanner's Critical result; it does not silently change severity.
The complete final-filesystem scan keeps unfixed findings and all severities,
with no ignore file, metadata stripping or accepted exception.

Baseline Spark findings remain **1 critical / 60 high library occurrences** and
**47 high OS occurrences**, with 41 distinct high/critical library package-advisory
pairs. Candidate and exact-commit scans check for regressions; fresh package
installation is not assumed to produce identical bytes. The same cached scanner
database supports comparisons; its timestamp is recorded, not presented as a
guarantee of all current advisories.

Any future applicability approval must name the exact reviewed image/ECR digest,
this CVE, retained library hash, controls, owner and expiration. A possible review
scope is one demonstration of at most one hour, expiring within seven days,
with no inbound TLS/SNI server and no classpath, listener or image changes.
Those bounds are proposals only, do not alter the existing two-hour maximum
validator and do not authorize a run. A changed digest requires a fresh review.
Other high findings require their own dispositions; accepting this one would
not clear the whole image.

## F. Git commit and CI status

Verified starting develop SHA: `3dfef1328239124fb1a0c863790ad183b140bbe6`.
[CI 43839583156](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/43839583156)
passed all six jobs. The new change is committed/pushed on develop after local
review. Its exact SHA, committed-image checks and automatically triggered CI
are reported after observation. Main remains unmerged.

## G. Cost implications

No AWS API, resource change, paid runtime or ECR publication occurs. Public POM
and source-archive reads, local Docker builds and the existing six standard
Ubuntu CI jobs are used. The driver memory is the already planned cloud value,
not an EC2 instance-size change. Existing retained cloud costs continue.

## H. Remaining blockers

- SDK Critical finding and unresolved high library/OS findings; no accepted exceptions.
- No approved replacement ECR deployment digests.
- Reviewed Alchemy secret ARN and exact workload-role/boundary permissions.
- Next-boot EBS mount, live S3 writes/checkpoint recovery and pilot acceptance.

No prior publication, EC2 start or formatting approval is reused. The initialized
Kafka volume must never be formatted again. No production backend is initialized.

## I. Next step

Finish the remaining findings' applicability/remediation review, keeping exact
digests and raw scanner results visible. For this scheduled demonstration,
reviewing bounded dispositions is preferable to casually taking ownership of a
private Hadoop/SDK rebuild; that is an engineering recommendation, not a security
approval. A vendor-matched fixed release or a reproducible custom rebuild with
linkage, SDK behavior, S3 and recovery validation are alternative remediation
paths. Neither is selected by this checkpoint. Prepare a concrete approval
package only after the remaining security and deployment gates are resolved.

## J. Whether production infrastructure changed

**No.** No AWS APIs, authenticated production Terraform commands, IAM/Snowflake
changes, ECR pushes, EC2 starts, S3 writes, secret changes or EBS operations occur.
No new live EC2-state observation is claimed. The runtime/platform Terraform
states remain separate and untouched; 11.5D remains paused.
