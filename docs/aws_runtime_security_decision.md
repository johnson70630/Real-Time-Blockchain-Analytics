# Milestone 11.6C — Consolidated security decision

Current status: [conditional one-run security acceptance and next secret proposal](aws_runtime_pilot_security_acceptance.md). The owner accepted the exact 41-advisory scope; secret creation, IAM changes, EC2 start and deployment remain separately gated.

The content below is preserved checkpoint history. Approval flags and status statements reflect the time of each original snapshot.

Latest scan-only publication: [verified ECR artifacts and scan status](aws_runtime_quarantined_publication.md). Images remain quarantined; no security exception or deployment is approved.

**Status: owner decision required; no exception or deployment is approved.**
Review snapshot: 2026-11-17 UTC. Milestone 11.5D remains paused.

The offline pilot is implemented and its hardening checkpoint passes 460 focused
contracts, exact-source image/audit/recovery checks and all six CI jobs. A refreshed
scan and a review of all remaining maintainer advisories now identify the decision
that ordinary offline coding cannot settle: whether to accept residual risk for
one tightly bounded public-data demo, or pursue a broader dependency rebuild.

## A. What was implemented

This documentation checkpoint consolidates the remaining findings into one
39-advisory inventory, with exact package versions, affected image paths,
maintainer references, exploit prerequisites, observed controls and uncertainty.
It also records fresh benign inspection of the installed client/OS configuration.
No application code, dependency pin, Terraform file or security gate is changed.

The baseline source is `272df839ad69e0579c6181eb95f5c48e56c2ee69`.
Its image archives have been rescanned after refreshing the vulnerability and
Java-identification databases into a separate cache. Every image manifest,
config and compressed-layer digest was reverified. Old snapshots are preserved.

| Exact baseline artifact | Local Linux/AMD64 manifest |
| --- | --- |
| Producer | `sha256:510368dd54d5cf8d0bfcc619d01b745fb38634b12e9d72224d17b474608a533a` |
| Spark | `sha256:52d956774b5133217bb41ed8b6577599f8813df635969296502572dcb121128b` |
| Kafka | `sha256:162573bb80e9548b98c3e12b87e1d0829efde2e9b73d92689a9139f3c4b65957` |

These are baseline local manifest identities, **not approved ECR deployment
digests**. The documentation commit changes source labels when images are rebuilt.
The final conversation attaches the resulting exact approval-scope snapshot after
CI and identity checks; that snapshot, rather than this baseline table, defines
any requested exception. A changed digest invalidates the proposal.

## B. Why a decision is needed

The remaining vulnerable code is relocated inside vendor bundles. Standalone
Netty/Jackson/Guava updates cannot replace those copies. Public metadata now lists
SDK 2.59.2; its POM declares Netty 4.1.138. That is POM evidence only, not a tested
Hadoop replacement. The latest Hadoop project POM inspected, 3.5.0, still selects
SDK 2.35.4. The pilot retains its tested Hadoop 3.4.3/SDK 2.35.4 pairing.
[Hadoop's S3A guidance](https://hadoop.apache.org/docs/r3.4.3/hadoop-aws/tools/hadoop-aws/troubleshooting_s3a.html)
warns against arbitrary SDK substitution without rebuilding compatible components.

Debian's recorded Trixie advisories and refreshed scanner snapshot still list the
11 OS CVEs without a Trixie fix in those records. Forced removal of essential
packages, unstable-distribution mixing and private vendor rebuilds are not
silently introduced to make counts disappear. Those are wider maintenance choices.

My recommendation for this portfolio demo is to **consider a digest-specific,
one-run exception**, rather than take ownership of a private Hadoop/SDK build.
That is a recommendation to the owner, not acceptance. Some transitive paths
remain unproven; the owner can decline and keep deployment blocked.

A supported Debian 12 alternative was also probed before requesting acceptance.
The official digest-pinned Python 3.12.16 Bookworm image was updated with public
OS packages and required native libraries, then scanned with the same refreshed
database. Its OS result was **2 critical / 99 high**, versus **0 critical / 47 high**
in the selected app base. It was rejected before application adoption.
[Bookworm has AMD64 LTS support through June 2028](https://www.debian.org/releases/bookworm/),
but lifecycle support alone does not establish a lower scanner finding count.
These raw results are not proof that every reported component is reachable.
This was a minimal OS probe, not a full application compatibility benchmark.
No Bookworm runtime pin was committed.

## C. Files changed

- This report: decision, limitations, remaining gates and per-advisory appendix.
- [Sanitized evidence](evidence/aws_runtime_security_decision_2026-11-17.json):
  exact baseline identities, refreshed scan hashes, all affected occurrences,
  vendor/source hashes, observed settings and an unaccepted proposal.
- `docs/aws_streaming_pilot.md`: a link to this consolidated decision.

README, application/build/controller files, dependency locks, Terraform,
Snowflake/dbt/Streamlit and existing production artifacts are preserved.

## D. Tests and validation

The baseline 460-test result and recovery/cleanup evidence remain applicable to
unchanged runtime inputs. This checkpoint validates inventory arithmetic,
manifest/config/layer bindings, report completeness and pending-approval flags.
No redundant unit tests are added for documentation. Existing six-job CI runs
automatically after the reviewed develop checkpoint; its result is observed.

Fresh installed-image checks run non-root, with no capabilities, no-new-privileges,
read-only roots and no network. They confirm:

- Spark uses `local[2]`, an in-memory catalog, classic S3A input and no UI URL;
  all 51 guarded class origins match the reviewed JARs.
- The installed Hadoop async-builder defaults use HTTP/1.1 and assume that
  protocol, have no custom TLS trust-manager provider, and do not trust all certs.
- An explicit JDK diagnostic context is client-mode; the actual SDK handler
  applies HTTPS endpoint identification. Default JDK trust managers are extended
  managers. No request or handshake is performed, and no claim about every TLS
  provider or live S3 authorization follows from this check.
- Both app images have effective/bounding capabilities zero and no-new-privileges
  set. Selected mount/account helpers have mode 0755, without setid bits.
- systemd-homed/homectl and Perl Archive::Tar are absent in the inspected images.
  SQLite FTS5/session support and infocmp are present. Their presence is not waived.

All 28 library maintainer advisories were retrieved from public repository
advisory APIs and hash-identified. Public SDK/Hadoop sources were checksum-verified.
The inspected SDK HTTP-client source constructs HttpClientCodec; source searches
found no direct construction of the reviewed LZ4/bzip2/SPDY/decompressor handlers.
Such searches do not establish universal transitive unreachability.

## E. Security review and limits

| Current exact baseline | CRITICAL | HIGH | MEDIUM | LOW | UNKNOWN |
| --- | ---: | ---: | ---: | ---: | ---: |
| Producer libraries | 0 | 0 | 6 | 1 | 0 |
| Spark libraries | 1 | 30 | 47 | 3 | 0 |
| OS, each app image | 0 | 47 | 92 | 65 | 5 |
| Kafka OS/libraries | 0 | 0 | 0 | 0 | 0 |

The 125 high/critical occurrences across both app images represent 39 distinct
advisory IDs: 28 library advisories and 11 OS CVEs. There are 76 distinct
package/advisory pairs, or 78 when different installed versions are counted
separately. A single advisory can affect multiple binary packages or copies;
125 is not a count of distinct exploitable flaws. Medium/low/unknown findings
remain visible in the full preserved scans.

The refreshed vulnerability DB was published at 2026-11-16 18:50:18 UTC, later
than the previous 01:11 snapshot. The latest downloaded Java DB reports its
2026-11-16 00:57 publication time. Coverage is identified, not represented as
an exhaustive real-time guarantee. No ignore file, unfixed exclusion, severity
filter, metadata removal or exception is applied.

The critical Netty advisory concerns an inbound per-SNI mutual-TLS fallback;
the inspected SDK builds outbound TLS clients. That supports a condition-not-
configured candidate, not a patched-class or accepted-exception claim. The
[hostname-verification advisory](https://github.com/netty/netty/security/advisories/GHSA-c653-97m9-rcg9)
requires a supplied plain trust manager; the observed default has no custom
provider. The [response-desynchronization advisory](https://github.com/netty/netty/security/advisories/GHSA-57rv-r2g8-2cj3)
is client-side, so outbound-only operation does not dismiss it.

Likewise, the Jetty Digest advisory includes HTTP client authentication;
disabling Spark's UI alone is insufficient. The selected workload uses S3
signing, not Jetty Digest. Jackson polymorphic/object-identity and cache paths
are not enabled by direct pilot code, but every transitive Hadoop mapper is not
proven safe. Those uncertainties remain explicit in the appendix.

The scanner's CVE-2026-68494 reference links to an upstream advisory whose API
CVE field is null. The original scanner ID/severity is retained; the reference
is not treated as proof of a replacement alias or justification for suppression.
No formal VEX not-affected decision is issued here.

### Proposed exception scope — pending, not active

Owner: **johnson70630**. Maximum: **one supervised streaming run, configured for
at most 3,600 seconds**. Expiration: **2026-11-24 00:00:00 UTC**. All 39 listed
advisory IDs and their exact affected package/version/path occurrences must be
reviewed as the scope; a generic future-image exception is not proposed.

The final attached snapshot must bind the exact single-platform local manifests
and report hashes after the documentation checkpoint. ECR publication requires
its own approval and manifest readback. If its resulting digest differs, this
scope is invalid until reviewed again. Approval cannot cover new findings.

Required controls: one host and checkpoint writer; unchanged producer/Kafka/
Spark workload; isolated pilot S3 prefixes; non-root/read-only/capability-free
apps; no-new-privileges; no public Kafka, ingress, UI, Telnet or TLS server;
no custom trust manager, trust-all certificates or disabled hostname checks;
reviewed least-privilege workload IAM; controlled secret injection; supervised
startup, drain/recovery evidence and an independent shutdown deadline.

The run handles public blockchain analytics only, with no wallet signing keys,
transactions, private customer data or broader warehouse/dashboard workload.
Changed images, classpaths, listeners, protocols, credentials handling, data
formats, privileges or expiration require a fresh decision. A configured runtime
limit is not a guaranteed billing cap. The owner must explicitly accept the
residual uncertainty; **accepted exceptions remain empty** until then.

## F. Git commit and CI status

The verified baseline is develop `272df839ad69e0579c6181eb95f5c48e56c2ee69`;
[CI 43984277427](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/43984277427)
passed all six jobs. This report/evidence checkpoint changes no runtime inputs.
Its exact new commit and automatically triggered CI are recorded after observation.
Rebuilt image identities are separately rebound for the final owner decision.
Main remains unmerged.

## G. Cost implications

The review uses public metadata, local image inspection and existing standard
public-repository CI. No AWS resources, image publication or runtime hours are
created or authorized. Existing retained resources can continue their charges.
The earlier roughly $24/month at 40 hours estimate is historical planning,
not a current quote, spending limit or approval. Fresh pricing and explicit
runtime/cost scope remain necessary before a paid pilot.

## H. Remaining blockers and deployment readiness

| Gate | Required next evidence or approval |
| --- | --- |
| Security | Owner accepts the exact pending snapshot or selects broader remediation. No automatic waiver. |
| ECR | Separate approval for the exact three replacement images in the existing repositories; readback must match reviewed manifests. Existing rejected images remain quarantined. |
| Alchemy secret | Reviewed Secrets Manager ARN and owner-controlled secret entry; never send secret values to chat. It has not been supplied for this release. |
| IAM / Terraform | Review exact secret-read/boundary scope after the ARN is known; obtain separate authenticated-plan and modification approvals. No production backend initialization here. |
| Paid EC2 window | Verify identity/resources/cost and obtain a separately bounded start/pilot approval. No new live EC2-state observation is claimed. |
| Kafka EBS | Check the existing UUID-based mount on the next approved boot. Never reformat the initialized volume or reuse consumed formatting approval. |
| Live S3 | Verify pilot-prefix isolation, one checkpoint writer, writes and Kafka/Spark recovery during the approved window. Offline tests do not prove live authorization. |
| Shutdown / rollback | Stop producer, drain against Kafka end offsets, verify the final checkpoint commit, then stop containers/EC2. On failure, preserve files/logs/checkpoints; do not roll back to rejected images or delete/reformat data. |
| Cleanup / cost | Confirm stop and retained resources; no automatic cloud deletion. Storage/monitoring charges can remain while stopped. |

The existing recorded infrastructure and runbooks are in
[the pilot-readiness report](aws_streaming_pilot.md). Its resource history is
not a fresh cloud-state observation. This checkpoint proposes no Terraform or
AWS resource change and preserves both runtime/platform states.

## I. Next step

The next necessary owner decision is whether to accept the exact bounded demo
exception or keep deployment blocked while broader runtime changes/private
rebuilds are investigated. Neither choice automatically authorizes ECR push,
Secrets Manager/IAM work, a production plan, EC2 start, Snowflake changes or a
main merge. Ordinary authorized offline work continues without repeated prompts.

## J. Whether production infrastructure changed

**No.** No AWS API, production Terraform initialization/plan/apply/import,
resource mutation, EC2 start, secret/IAM/Snowflake change or ECR publication
occurred. README and main remain unchanged. Milestone 11.5D remains paused.

## Per-advisory decision appendix

Every row is a pending applicability/exception candidate, with no accepted
waiver. Exact per-image occurrences and versions are in the JSON evidence.

| Advisory / maintainer source | Severity | Installed component(s) | Required condition and current assessment |
| --- | --- | --- | --- |
| [CVE-2026-75595](https://github.com/netty/netty/security/advisories/GHSA-c4c3-7fpv-j4q5) | CRITICAL | io.netty:netty-handler 4.1.126.Final | Inbound SNI-specific mutual TLS falls back to a permissive default context. The SDK source constructs client TLS; no per-SNI mutual-TLS server is configured. Scanner Critical severity is retained. |
| [CVE-2025-52999](https://github.com/FasterXML/jackson-core/security/advisories/GHSA-h46c-h94j-95f3) | HIGH | com.fasterxml.jackson.core:jackson-core 2.12.7 | Deeply nested JSON reaches the old shaded parser. Transitive Hadoop JSON paths remain incompletely proven; the direct Spark input parser is separately patched. |
| [CVE-2025-69720](https://security-tracker.debian.org/tracker/CVE-2025-69720) | HIGH | libncursesw6 6.5+20250216-2; libtinfo6 6.5+20250216-2; ncurses-base 6.5+20250216-2; ncurses-bin 6.5+20250216-2 | The infocmp command processes malicious terminal-description input. No terminal-description input or infocmp command is part of the reviewed pipeline. |
| [CVE-2026-10050](https://github.com/jetty/jetty.project/security/advisories/GHSA-2fvj-hgj9-j2gr) | HIGH | org.eclipse.jetty:jetty-security 11.0.24 | HTTP Digest authentication hashes collide for non-Latin credentials. This advisory includes client authentication; disabling the Spark UI alone is insufficient. The pilot selects S3 signing rather than Jetty HTTP Digest authentication. |
| [CVE-2026-102554](https://github.com/google/guava/security/advisories/ghsa-xxph-c9ww-hj94) | HIGH | com.google.guava:guava 33.4.0-jre; com.google.guava:guava 33.4.8-jre | Untrusted Java object streams deserialize Guava collections. Selected Kafka input is JSON, output is Parquet, and Spark runs locally; arbitrary external Java object input is not configured. |
| [CVE-2026-119373](https://security-tracker.debian.org/tracker/CVE-2026-119373) | HIGH | libsqlite3-0 3.46.1-7+deb13u2 | SQLite FTS5 processes adversarial tables and prefix queries. No SQLite input or FTS service is selected; the library remains installed and no package fix is claimed. |
| [CVE-2026-119374](https://security-tracker.debian.org/tracker/CVE-2026-119374) | HIGH | libsqlite3-0 3.46.1-7+deb13u2 | SQLite deletes from a malicious FTS5 index. The demo accepts blockchain events rather than SQLite database files; no exhaustive dependency call-path claim is made. |
| [CVE-2026-119379](https://security-tracker.debian.org/tracker/CVE-2026-119379) | HIGH | libsqlite3-0 3.46.1-7+deb13u2 | SQLite applies a crafted session changeset. No SQLite changeset ingestion is configured; affected library code remains installed. |
| [CVE-2026-16742](https://security-tracker.debian.org/tracker/CVE-2026-16742) | HIGH | libsystemd0 257.13-1~deb13u1; libudev1 257.13-1~deb13u1 | A privileged systemd-homed service changes managed-user group membership. The exact installed images have neither systemd-homed nor homectl; shared source-package libraries remain reported. |
| [CVE-2026-2332](https://github.com/jetty/jetty.project/security/advisories/GHSA-355h-qmc2-wpwf) | HIGH | org.eclipse.jetty:jetty-http 11.0.24; org.eclipse.jetty:jetty-http 9.4.57.v20241219 | An HTTP parser accepts ambiguous quoted chunk extensions. No Jetty HTTP service is launched by the selected workload; this is a role/configuration candidate, not removal of its parser. |
| [CVE-2026-33870](https://github.com/netty/netty/security/advisories/GHSA-pwqr-wmgm-9rr8) | HIGH | io.netty:netty-codec-http 4.1.126.Final | HTTP request parsers disagree about quoted chunk extensions. The selected SDK role is an outbound client; no SDK inbound HTTP request service is configured. |
| [CVE-2026-33871](https://github.com/netty/netty/security/advisories/GHSA-w9fj-cfpg-grvv) | HIGH | io.netty:netty-codec-http2 4.1.126.Final | An HTTP/2 endpoint processes an unbounded continuation-frame sequence. Observed Hadoop HTTP-client defaults use HTTP/1.1; no HTTP/2 server is launched. Future protocol changes invalidate the assessment. |
| [CVE-2026-42583](https://github.com/netty/netty/security/advisories/GHSA-mj4r-2hfc-f8p6) | HIGH | io.netty:netty-codec 4.1.126.Final | A Netty LZ4 frame decoder trusts peer-controlled allocation lengths. No LZ4 decoder construction was found in the inspected SDK HTTP-client source; installed codec code remains. |
| [CVE-2026-42584](https://github.com/netty/netty/security/advisories/GHSA-57rv-r2g8-2cj3) | HIGH | io.netty:netty-codec-http 4.1.126.Final | HTTP/1.1 pipelining combines HEAD, informational responses and unsafe reuse. This is a client-side advisory; outbound-only operation does not dismiss it. Pipelining prerequisites were not separately tested. |
| [CVE-2026-42587](https://github.com/netty/netty/security/advisories/GHSA-f6hv-jmp6-3vwv) | HIGH | io.netty:netty-codec-http 4.1.126.Final; io.netty:netty-codec-http2 4.1.126.Final | Compressed HTTP content bypasses advertised allocation limits. No decompressor construction was found in inspected SDK client source, but complete transitive response-path absence is not proven. |
| [CVE-2026-44249](https://github.com/netty/netty/security/advisories/GHSA-3qp7-7mw8-wx86) | HIGH | io.netty:netty-handler 4.1.126.Final | An application relies on Netty IPv6 subnet filtering for authorization. No Netty subnet-filter policy is configured; required AWS ingress and container controls remain separate prerequisites. |
| [CVE-2026-45416](https://github.com/netty/netty/security/advisories/GHSA-x4gw-5cx5-pgmh) | HIGH | io.netty:netty-handler 4.1.126.Final | An inbound TLS ClientHello handler eagerly allocates peer-declared buffers. The inspected SDK TLS role is client-only, not an inbound SNI service. |
| [CVE-2026-50010](https://github.com/netty/netty/security/advisories/GHSA-c653-97m9-rcg9) | HIGH | io.netty:netty-handler 4.1.126.Final | A supplied plain trust manager loses endpoint-identification context. The installed Hadoop async-builder has no custom trust provider or trust-all flag; an explicit JDK handler has HTTPS endpoint identification. No handshake or every-provider proof is claimed. |
| [CVE-2026-54369](https://security-tracker.debian.org/tracker/CVE-2026-54369) | HIGH | libacl1 2.3.2-2+b1 | A privileged caller performs pathname-based ACL operations on mutable paths. The applications run non-root without capabilities, with read-only roots and no-new-privileges; host ACL administration is outside scope. |
| [CVE-2026-54512](https://github.com/FasterXML/jackson-databind/security/advisories/GHSA-j3rv-43j4-c7qm) | HIGH | com.fasterxml.jackson.core:jackson-databind 2.12.7.1 | Polymorphic deserialization accepts attacker-chosen generic type identifiers. Direct pilot code does not enable polymorphic typing; absence from every transitive Hadoop mapper is not proven. |
| [CVE-2026-54513](https://github.com/FasterXML/jackson-databind/security/advisories/GHSA-rmj7-2vxq-3g9f) | HIGH | com.fasterxml.jackson.core:jackson-databind 2.12.7.1 | Array subtype allowlisting permits unapproved element classes. No such validator configuration is selected by pilot code; transitive configuration remains uncertain. |
| [CVE-2026-55831](https://github.com/netty/netty/security/advisories/GHSA-6jqx-86gh-f27w) | HIGH | io.netty:netty-codec-http 4.1.126.Final | A configured SPDY decoder accepts excessive SETTINGS entries. Observed HTTP-client defaults are HTTP/1.1; no SPDY handler construction was found in the inspected client source. |
| [CVE-2026-55833](https://github.com/netty/netty/security/advisories/GHSA-mvh2-crg5-v77c) | HIGH | io.netty:netty-codec-http 4.1.126.Final | SPDY header inflation continues after size rejection. SPDY is not selected by the observed defaults; vulnerable codec classes remain installed. |
| [CVE-2026-56740](https://github.com/jline/jline3/security/advisories/GHSA-47qp-hqvx-6r3f) | HIGH | org.jline:jline-remote-telnet 3.9.0 | A running JLine Telnet service accepts excessive environment variables. No Telnet service is launched; the old relocated classes remain inside Hadoop. |
| [CVE-2026-56741](https://github.com/jline/jline3/security/advisories/GHSA-2r2c-cx56-8933) | HIGH | org.jline:jline-remote-telnet 3.9.0 | A running JLine Telnet service renders excessive terminal dimensions. No Telnet or interactive terminal service is launched by the selected pipeline. |
| [CVE-2026-56745](https://github.com/netty/netty/security/advisories/GHSA-jppx-w49h-x2qq) | HIGH | io.netty:netty-codec-http 4.1.126.Final | SPDY-to-HTTP state handling leaks buffers on stream errors. No SPDY-to-HTTP service or handler is selected by the pilot configuration. |
| [CVE-2026-56819](https://github.com/netty/netty/security/advisories/GHSA-93wv-jw9v-4972) | HIGH | io.netty:netty-codec-http2 4.1.126.Final | HTTP/2 decompression error paths leak direct buffers. Observed defaults use HTTP/1.1; no HTTP/2 decompressor construction was found in the inspected client source. |
| [CVE-2026-59901](https://github.com/netty/netty/security/advisories/GHSA-558v-64gr-wgg4) | HIGH | io.netty:netty-codec 4.1.126.Final | A malformed bzip2 stream reaches a Netty compression decoder. No bzip2 decoder construction was found in inspected client source; dynamic or future pipelines are not certified. |
| [CVE-2026-68494](https://github.com/FasterXML/jackson-core/security/advisories/GHSA-r7wm-3cxj-wff9) | HIGH | com.fasterxml.jackson.core:jackson-core 2.12.7 | An asynchronous numeric parser accumulates incomplete streamed numbers. Specific old-parser path is not exercised by the pilot tests; scanner-to-upstream advisory mapping retained without inventing a CVE alias. |
| [CVE-2026-76642](https://security-tracker.debian.org/tracker/CVE-2026-76642) | HIGH | bsdutils 1:2.41.5-0+deb13u1; libblkid1 2.41.5-0+deb13u1; liblastlog2-2 2.41.5-0+deb13u1; libmount1 2.41.5-0+deb13u1; libsmartcols1 2.41.5-0+deb13u1; libuuid1 2.41.5-0+deb13u1; login 1:4.16.0-2+really2.41.5-0+deb13u1; mount 2.41.5-0+deb13u1; util-linux 2.41.5-0+deb13u1 | Privileged mount helpers run post-mount operations after helper failure. Setuid bits are cleared, capabilities dropped and privilege elevation blocked. Vulnerable package code is retained. |
| [CVE-2026-77422](https://github.com/jline/jline3/security/advisories/GHSA-r2xf-8xr9-62gw) | HIGH | org.jline:jline-builtins 3.9.0 | A JLine shell evaluates attacker-controlled grep patterns. The pipeline is not a remote interactive JLine shell and exposes no grep command service. |
| [CVE-2026-78408](https://security-tracker.debian.org/tracker/CVE-2026-78408) | HIGH | bsdutils 1:2.41.5-0+deb13u1; libblkid1 2.41.5-0+deb13u1; liblastlog2-2 2.41.5-0+deb13u1; libmount1 2.41.5-0+deb13u1; libsmartcols1 2.41.5-0+deb13u1; libuuid1 2.41.5-0+deb13u1; login 1:4.16.0-2+really2.41.5-0+deb13u1; mount 2.41.5-0+deb13u1; util-linux 2.41.5-0+deb13u1 | A privileged operator enters a hostile cgroup namespace and inherits descriptors. No privileged nsenter operation is selected; containers have no namespace-administration capabilities. |
| [CVE-2026-78409](https://security-tracker.debian.org/tracker/CVE-2026-78409) | HIGH | bsdutils 1:2.41.5-0+deb13u1; libblkid1 2.41.5-0+deb13u1; liblastlog2-2 2.41.5-0+deb13u1; libmount1 2.41.5-0+deb13u1; libsmartcols1 2.41.5-0+deb13u1; libuuid1 2.41.5-0+deb13u1; login 1:4.16.0-2+really2.41.5-0+deb13u1; mount 2.41.5-0+deb13u1; util-linux 2.41.5-0+deb13u1 | A privileged subdirectory mount resolves an attacker-controlled path. No container mount administration is authorized; executable privilege bits are cleared. Vendor vulnerable status is retained. |
| [CVE-2026-78410](https://security-tracker.debian.org/tracker/CVE-2026-78410) | HIGH | bsdutils 1:2.41.5-0+deb13u1; libblkid1 2.41.5-0+deb13u1; liblastlog2-2 2.41.5-0+deb13u1; libmount1 2.41.5-0+deb13u1; libsmartcols1 2.41.5-0+deb13u1; libuuid1 2.41.5-0+deb13u1; login 1:4.16.0-2+really2.41.5-0+deb13u1; mount 2.41.5-0+deb13u1; util-linux 2.41.5-0+deb13u1 | A privileged bind-mount operation follows mutable source paths. No mount privileges or setuid helpers remain in the app images. Host EBS operations remain a separate gated activity. |
| [CVE-2026-89425](https://github.com/FasterXML/jackson-core/security/advisories/GHSA-7hhh-6rmp-j9qf) | HIGH | com.fasterxml.jackson.core:jackson-core 2.12.7 | Malformed tokens reach a DataInput-based JSON parser. No adversarial parser test was run against vulnerable code; transitive path uncertainty remains. |
| [CVE-2026-91776](https://github.com/FasterXML/jackson-databind/security/advisories/GHSA-wv8q-qhhj-9h54) | HIGH | com.fasterxml.jackson.core:jackson-databind 2.12.7.1 | Unknown named type identifiers fill a fallback-deserializer cache. No direct annotated fallback model is selected; retained shaded code and indirect model use remain visible. |
| [CVE-2026-91777](https://github.com/FasterXML/jackson-databind/security/advisories/GHSA-cxp5-3px4-pw24) | HIGH | com.fasterxml.jackson.core:jackson-databind 2.12.7.1 | Object-identity collections resolve adversarial references in reverse order. No direct object-identity model is selected; all transitive JSON paths are not certified. |
| [CVE-2026-9538](https://security-tracker.debian.org/tracker/CVE-2026-9538) | HIGH | perl-base 5.40.1-6+deb13u1 | Perl Archive::Tar processes an adversarial archive entry length. The exact installed images cannot load Archive::Tar because its module is absent; raw source-package scanner status is preserved. |
| [GHSA-443w-3rq3-5m5h](https://github.com/aws/aws-sdk-java-v2/security/advisories/GHSA-443w-3rq3-5m5h) | HIGH | software.amazon.awssdk:cloudfront 2.35.4 | Unvalidated input enters CloudFront signed-policy utilities. The workload uses S3 and does not invoke CloudFront signing utilities. The broad SDK bundle remains installed. |
