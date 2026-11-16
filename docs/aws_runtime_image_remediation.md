# Milestone 11.6C: offline image remediation

Status: replacement prototypes build and pass functional/recovery tests.
Kafka's local OS scan is clear. Producer/Spark still have unfixed HIGH findings,
so **the replacement release is blocked**. No replacement image was published,
no AWS API was called and EC2 was not started during this work. Milestone 11.5D
remains paused. The earlier ECR publication remains a rejected frozen release.

## A. What changed

The cloud Kafka candidate is now a source-built image based on digest-pinned
Apache Kafka **4.3.2**, with `apk upgrade --no-cache` applying available Alpine
security updates. Apache lists 4.3.2 among its
[supported releases](https://kafka.apache.org/community/downloads/).
The selected upstream image uses Alpine 3.24, replacing the expired 3.21 base.
Its UID/GID remains 1000:1000; EBS mapping, single-broker topology and retention
limits are unchanged. No production Kafka data or metadata was migrated.

The first Kafka OS-patched candidate still bundled Java 21.0.12.1 with a
compromised CA root. It is not a publication candidate. The final derivative
also replaces that JRE with a separately pinned Temurin **21.0.13+9** Alpine
runtime. Its 118-anchor store was independently inspected: the three roots
checked for Spark are absent and Amazon/ISRG positive controls are present.
Its exact store hash is
`628b436e6698c364c5498a32c5bff91806bea7a9b501f75e2289ddc60b4ca7ef`.
The upstream launch script's two old application CDS references are removed
with an exact-count guard; only the two stale image-build `.jsa` files are
removed. The replacement JRE's normal default CDS remains available. This
changes no Kafka logs, EBS filesystem or cluster metadata. Actual broker
restart/checkpoint recovery and a dedicated Java/CA/CDS smoke are required.

Producer/Spark use digest-pinned Python **3.12.16 / Debian 13** and apply
available OS updates. Spark retains **PySpark 4.0.1, Hadoop 3.4.1 and Java 17**.
Its self-contained Temurin 17 JRE is copied from a separately pinned official
image; Debian runtime libraries remain from Debian's own repositories.
The actual Java/classpath/Parquet smoke passed on this combination. This is
a dedicated headless pipeline runtime: unused font/XML dependencies are omitted.
The JRE has its upstream CA trust store; no custom certificate was injected.

The Kafka Java client is patched from 3.9.1 to **3.9.2**, with a new official
Maven SHA-512 pin. Apache documents a producer buffer race that can corrupt or
misroute messages and identifies 3.9.2 as a fixed version; see
[CVE-2026-35554](https://kafka.apache.org/cve-list.html).
The remaining locked connectors/Python dependencies are preserved.
CI may restore the prior Maven-cache prefix after a lock change; each named
artifact is still checked against the new committed SHA-512 lock. This reuses
the seven unchanged JARs while obtaining only the new client. Unlisted old JARs
are excluded from the build context; the cache key is never an integrity proof.

Cloud application images and Compose fix `LANG`/`LC_ALL` to `C.UTF-8` and clear
`LANGUAGE`; startup rejects changed/missing values. This restricts the malicious
gettext catalog path described by
[CVE-2026-84243](https://security-tracker.debian.org/tracker/CVE-2026-84243).
It is a mitigation, not a package fix or risk-acceptance decision.

The release validator and local publication planner now reject all three exact
manifests from the failed first ECR release. Changing labels cannot make those
digests acceptable. They also reject the unpublished 83d4363 Kafka candidate
whose OS patches left the old Java trust store. Unknown/new digests still
require independent review.

## B. Why

Digest pinning makes artifacts identifiable; it does not keep old packages
patched. Supported upstream versions, OS package updates, explicit runtime
compatibility checks and visible unresolved findings are separate controls.
The Kafka client patch also protects data correctness, not only availability.

An Alpine application alternative was examined but not selected: the current
uv.lock has no musllinux wheel for confluent-kafka 2.14.2. Maintaining a new
native build chain and validating additional Spark/native-code ABI behavior
would be a separate design effort. No native build or wheel/hash relaxation was
used to force that alternative. Debian's tested glibc runtime is retained.

## C. Files

- Cloud producer/Spark Dockerfiles and new `Dockerfile.kafka`.
- Existing CI Maven-cache restore prefix; no new jobs, credentials or publishing.
- `deploy/runtime/images.lock.json`: upstream tags/index/platform/config digests,
  selected version metadata and transfer sizes from public registry readback.
- Kafka client JAR lock, cloud/offline Compose, and committed-source build script.
- `deploy/runtime/smoke_producer.py`: synthetic native-client and Uniswap
  ABI/envelope check supplied to the producer container over stdin.
- `deploy/runtime/smoke_spark.py`: Java/classpath/Parquet and actual CA-store
  checks, using the trust-store hash from the exact committed input lock.
- `deploy/runtime/smoke_kafka.sh`: Java 21 version, pinned CA-store and stale-CDS
  checks under the non-root broker image user, without network or broker startup.
- Runtime locale/quarantine/publication contracts and their adversarial tests.
- [Sanitized scan/input evidence](evidence/aws_runtime_image_remediation_2026-11-16.json)
  and current-status links in the existing readiness reports.

The normal local development Compose, root Dockerfiles, uv.lock, README,
Terraform source and production settings are unchanged. Kafka still uses the
existing producer ECR repository if separately approved for publication.

## D. Tests and validation

All **230 focused contracts pass**, plus Ruff, secret scanning, Bash syntax and
whitespace checks. New cases reject historical failed digests in both the host
release validator and publication planner, forged Kafka provenance/user labels,
and unsafe/missing locale settings. Pin consistency and Java 17 are checked.

Actual local prototype validation passed:

- Non-root producer imports, native librdkafka load, synthetic Uniswap ABI
  decode and canonical event envelope, with networking disabled.
- Java 17 / Spark 4.0.1 / Kafka client 3.9.2 / Hadoop 3.4.1 / S3A class loading
  and a Parquet round trip, with networking disabled.
- Synthetic Kafka persistence across broker restart; checkpoint resume;
  repeated-run duplicate prevention; missing-offset/checkpoint failure paths.
  The test network is internal. Only its uniquely named disposable volumes
  were removed after the test; no production EBS volume was involved.

The private prototype staging harness initially produced mode-0700 source
directories after safe tar extraction. Files were present but unreadable by the
non-root application user. Staging was corrected to readable public-source
modes, and the producer check passed. The new permanent smoke runs inside the
real image using only the exact committed tar's smoke script, so repository
imports cannot conceal a future image packaging error.

Prototype images are labeled `UNCOMMITTED_PROTOTYPE` and are never publication
candidates. After checkpointing, rebuild/review from that exact committed source
and observe all six CI jobs. ECR deployment digests remain unavailable for the
replacement images until a separately approved publication.

## E. Security review

Trivy **0.76.2** was downloaded into a private temporary directory. Its archive
SHA-256 matched both official GitHub release-asset metadata and the release's
checksum file. It was not globally installed. Local image tar files were scanned
with one cached database snapshot updated 2026-11-16 01:11:02 UTC. No image was
uploaded to a scanning service. No custom ignore file, severity filter or
unfixed-finding suppression was used. See
[local image-file scanning](https://trivy.dev/docs/latest/target/container_image/)
and [severity/vendor handling](https://trivy.dev/docs/latest/scanner/vulnerability/).

Same-tool, same-database OS-package comparison:

| Role | Original CRITICAL / HIGH | Replacement prototype CRITICAL / HIGH |
|---|---:|---:|
| Producer | 2 / 100 | 0 / 47 |
| Spark | 5 / 113 | 0 / 47 |
| Kafka | 6 / 69 | 0 / 0 |

These are Trivy package/CVE findings, **not the earlier ECR counts**. The
producer/Spark 47 HIGH rows each represent 11 distinct CVEs across multiple
binary packages. No fix is listed for their remaining findings in this database.
The evidence lists every remaining HIGH CVE and affected binary version.
An absent Trivy match does not prove an earlier ECR finding resolved.

The remaining HIGH CVEs concern util-linux (four), SQLite (three), and one each
in acl, ncurses, systemd and Perl. Scanner status alone does not establish
application reachability. In particular, Debian still marks stable SQLite
versions vulnerable for
[CVE-2026-119374](https://security-tracker.debian.org/tracker/CVE-2026-119374).
No source-package/binary-package false positive, exception or unreachable-code
claim has been automatically accepted.

OS scans also do not establish Python/JVM/JAR/JRE security. The copied Temurin
JRE is not a Debian dpkg-managed Java package; its separate upstream pin/version
must remain visible in reviews. The original Spark scan's Java CRITICAL entries
CVE-2026-91403 and CVE-2026-114820 were therefore carried into a separate review,
not silently dropped from consideration. Oracle identifies the former as an
[eMudhra trust issue](https://www.oracle.com/security-alerts/alert-cve-2026-91403.html).
Adoptium documents that its October 17.0.21+9 build uses updated Mozilla CA data
and [contains the fix](https://adoptium.net/news/2026/10/eclipse-temurin-17021-21013-2505-2701-available).
Its [vendor notice](https://adoptium.net/news/2026/10/temurin-october-2026-emudhra)
identifies the removed roots and explains its differing treatment of G1.

In the actual pinned container, the Java KeyStore API verified 118 anchors,
absence of `emSign ECC Root CA - G3`, `AAA Certificate Services` and G2, and
presence of Amazon Root CA 1 and ISRG Root X1. The exact cacerts SHA-256 is
`c8b4cbbd1a9d122f61946cef518fcae97b3ead4b2902da3605aeedf8a478eb37`.
G1 remains present per the vendor's Mozilla trust policy; no ad-hoc trust-store
edit or scanner suppression was used. The source-pinned smoke now checks these
conditions and the store hash. This establishes the documented CA-removal fix
conditions; it is not a live hostile-chain or signed-code test, or a claim that
the whole JRE is vulnerability-free. Full language/dependency review and actual ECR
scans of any future published replacements are still required. No release
approval follows from zero local Kafka OS findings or from green functional CI.

Builds remove inherited AWS credential/profile overrides and use only public
package/image sources. No registry token, secret value, Terraform state or
private credential material enters the context, evidence or images. Package
databases remain intact; findings were not hidden by deleting package metadata.

## F. Git and CI

Work on develop; checkpoint only after the above review/checks, then rebuild
the three images from that exact source and observe its own hosted CI. Published
source c917f2a and its rejected ECR images remain unchanged. A later commit and
artifact receipt are reported in the conversation. Main is unchanged.

## G. Cost

Only local CPU/disk and public downloads were used. No AWS resource, paid
compute, ECR upload or scanner service was added. The existing rejected images
remain retained at the previously estimated storage cost; no deletion was
authorized. GitHub uses the existing standard public-repository runners/cache
settings. No budget, cache quota, runner class or workflow permission increased.

## H. Remaining blockers

The 11 distinct unfixed HIGH CVEs shared by producer/Spark require a targeted
vendor/component/applicability review or a validated alternative before release.
Do not blanket-waive them, remove essential package metadata or mix unstable
distribution packages to manufacture a passing count. Language/JRE review,
future ECR results, an exact Alchemy secret ARN/workload permissions, mount-on-boot
validation and a separately approved timed application pilot remain outstanding.

## I. Next step

Preserve the improved, tested offline checkpoint. Review each remaining CVE's
affected binary/code path against the actual pipeline and vendor advisories;
record evidence for any non-applicability or mitigation. Keep unresolved items
blocking release. Replacement publication, any risk exception, secret/IAM changes
and EC2 start remain separate gates. The previous three-image upload approval
is consumed; it cannot authorize new image bytes.

## J. Infrastructure changes

None. No AWS API, ECR login/upload/scan start, Terraform backend/plan/apply/import,
EC2 action, EBS initialization, secret access, Snowflake operation or main merge
occurred. The production EBS formatting approval is consumed and must never be
reused. Milestone 11.5D remains paused.
