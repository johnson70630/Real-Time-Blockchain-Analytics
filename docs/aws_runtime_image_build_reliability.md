# Milestone 11.6C: cache verified build dependencies

The Spark image step in develop CI run
[43807733166](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/43807733166)
failed with Maven Central HTTP 429. Spark-backed tests passed; the failure was
downloading a pinned connector during a clean image build. EC2 was already
stopped and no image was published. The failed checkpoint is retained; use the
new validated source/build review rather than its earlier local tags.

## A. What changed

The cloud-only builder first obtains the eight SHA-512-pinned JARs in a bounded
host cache. A pinned official actions/cache v4 commit retains that directory
under a key derived only from jars.lock.json. On a warm cache, no Maven request
is needed. Cache entries contain public dependencies, never credentials.

The build-context generator reads the committed lock, independently verifies
each listed file, and snapshots its bytes while hashing. It adds only those
verified dependencies to the committed-source tar; unrelated cache files and
dirty worktree inputs are excluded. Snapshot files avoid a cache mutation
between hash verification and archive creation. The resulting receipt hashes
the complete context and records artifact count/bytes. The raw-archive API
without a cache remains available for source-only inspection.

Spark installs from a read-only BuildKit bind mount of that verified context
directory with `--offline`. It checks hashes again and atomically promotes
readable JAR files. The cache is not copied into an extra image layer, and no
Maven request is made inside that Docker build step or at runtime.

Cold downloads use the same Maven Central host and pins, at most three attempts
for transient failures. Short numeric Retry-After is respected; long/unknown
cooldowns, permanent HTTP errors and checksum failures stop. Partial files are
removed and never installed. No host switching, proxy rotation, credential,
hash relaxation or unpinned fallback is introduced. This follows
[Sonatype's caching guidance](https://central.sonatype.org/faq/429-error/);
the cache is the main reduction in repeated traffic, not aggressive retries.

The local publication plan also records each fixed local reference next to its
platform-specific identity. On the inspected Docker engine, the identity
returned by platform-specific inspection was not directly addressable as a
standalone CLI reference. A publisher must revalidate the reviewed reference,
platform and identity before local retag/push; never substitute an unchecked
tag or equate a local identity with an ECR deployment digest.

## B. Why

Fresh hosted runners previously re-downloaded the same large dependencies on
every documentation/code checkpoint. Reusing independently verified bytes
reduces public repository load and improves build reliability without weakening
artifact integrity or adding AWS infrastructure. A cold cache still depends on
Maven availability and cannot bypass a provider block.

## C. Files

- `deploy/runtime/fetch_jars.py`: cached/offline installation, integrity and
  atomic-file handling, bounded transient download handling.
- `runtime/images.py`: committed-lock cache validation and deterministic context;
  fixed local artifact references in review output.
- `scripts/build_runtime_images.sh`, cloud Spark Dockerfile and CI cache step.
- Download/cache/context tests and updated image/publisher review links.

## D. Validation

All **211 focused contracts pass**, with full Ruff, secret scanning, Bash syntax
and whitespace checks. Offline cases cover warm-cache no-network behavior, corrupt/missing/symlink/
oversized cache rejection, committed-versus-dirty lock files, unlisted private
file exclusion, reproducibility, partial-read recovery, bounded retries,
Retry-After, permanent/integrity failures, readable modes and bundled conflicts.
Run the focused contracts, full Ruff, secret scan, Bash syntax and whitespace
checks before committing. Build actual Java 17 images and run the existing
network-disabled Spark/classpath/Parquet smoke from that new committed source.
Observe all six hosted jobs; a passing local unit test is not a replacement.

The local cache can reuse the eight previously downloaded JARs from a stopped,
network-disabled disposable Spark container after verifying every locked hash.
Measured JAR bytes: **569234559**. No Maven request is needed to reuse them.
The disposable container is never started and only that container is removed.

## E. Security

Restored caches are untrusted input; their names/keys are not integrity proofs.
The context generator and image installer verify bytes against the committed
SHA-512 lock before executing them. Only the eight named public files enter the
context. The accepted total is limited to 1 GiB. A mismatch fails rather than
silently replacing a bundled/cached dependency. Java 17, Spark 4.0.1, pins,
runtime users, local development Compose and application behavior are unchanged.

## F. Git and CI

Commit the validated fix on develop and observe its new CI. Freeze and rebuild
the publication candidate from that exact commit; do not publish the earlier
e7d5f9b candidate or claim its failed CI passed. Main/README remain unchanged.

## G. Cost

Read-only GitHub metadata showed 3 caches totaling **1467634585 bytes** before
this change. This lock adds one cache entry limited to 1 GiB, within the
observed headroom of GitHub's separate 10 GiB repository cache allowance.
The action is pinned to official commit
`0057852bfaa89a56745cba8c7296529d2fc39830`; no cache quota, billing budget, runner
class or workflow permission is increased. No artifact/package upload or AWS
resource is added. See [GitHub cache billing](https://docs.github.com/en/billing/concepts/product-billing/github-actions).

## H. Limits

A cold runner needs one successful public download to seed its cache. An
upstream cooldown that exceeds the bounded retry budget remains a build blocker.
Cache integrity never grants permission to publish or deploy images. Actual
ECR digests/scans and production secret/IAM changes remain unapproved gates.

## I. Next step

Validate CI and new local artifacts, then request only the previously prepared
publisher IAM/publication scope. Keep EC2 stopped and never repeat formatting.

## J. Infrastructure

No AWS change. This is offline build/CI work after the completed exact-volume
operation. Neither Terraform state, Snowflake nor Milestone 11.5D is touched.
