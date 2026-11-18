# Milestone 11.6C — Existing secret accepted; one-read window stopped

Next review: [controlled chain-ID and streaming deployment prepared offline](aws_runtime_controlled_streaming_review.md).
The completed one-read approval is not reused for deployment or streaming.

**CONTRACT_PASS. The existing secret is a JSON object; the corrected reader
accepts it without any edit. EC2 is independently confirmed stopped.**
No Alchemy RPC, application deployment, controller or streaming container ran.

## A. What was executed

At reviewed source `c42a240afa0dbf1da394ab239aa0aa23c61e07c9`, the owner replied
`next` to the exact one-read proposal and refreshed MFA privately. New approval
receipts were created for this scope; earlier approvals were not reused.
Stopped and running preflights verified the exact instance/disks, no ingress,
IMDSv2, standard CPU credits, stop-on-shutdown behavior, normal-stop authorization,
private SSM document, workload IAM and accepted image identity/finding inventories.
The read-only receipts matched the current launch and a healthy detached watcher.

| Event | Observed UTC |
|---|---|
| EC2 launch | 2026-11-18 23:04:55 |
| Advertised cutoff | 2026-11-18 23:05:20.426355 |
| Normal stop returned stopping | 2026-11-18 23:08:04.624735 |
| Independent stopped / IPv4-released readback | 2026-11-18 23:08:34.736584 |
| Detached watcher completed stopped | 2026-11-18 23:08:38.313477 |

Through the guarded SSM session, temporary controls verified eleven original
host/release/helper files, the original systemd files, current Linux boot, existing
Kafka XFS mount/serial, swap disabled, tmpfs staging and no containers present.
The absolute transient shutdown timer was verified at launch + 35 minutes
(23:39:55 UTC). Four digest-verified helper sources were staged in a new root-only
tmpfs directory. Both local and guest exclusive intent files prevented a retry.

The inspector made one GetSecretValue request for the exact existing ARN,
version `12d38103-a68c-4226-b20a-3095cbb3de78` and AWSCURRENT together. Its
sanitized result passed identity, representation decoding and endpoint format.
SSM exited successfully. Normal stop was requested immediately afterward;
the independent readback verified both original encrypted gp3 disks still attached
and Kafka delete-on-termination false. The start/read authorization is consumed.

## B. Why and what the result means

The previous bare-URL assumption was the problem. The saved representation is
`JSON_OBJECT`, supported by the corrected shared decoder. The secret was not
rewritten, repaired, rotated or displayed. The decoded endpoint passes the strict
Arbitrum WSS host/path/URL rules.

The old raw-string check still reports JSON/quotes and related URL mismatch codes
because it sees the JSON wrapper. That observation is expected; the effective
result after decoding is **CONTRACT_PASS / FORMAT_PASS**, with no format codes.
This proves payload compatibility, not provider authentication or chain ID.

## C. Files changed

Added this report and
[sanitized evidence](evidence/aws_runtime_reader_probe_result_2026-11-18.json).
Updated status links in the pilot, reader-fix and rollout-review reports. No
application source, container digest, dependency lock, README, Terraform,
Snowflake/dbt/Streamlit configuration or original installed host file changed.

Temporary SSM adapters and receipts remain preserved privately. Their digests,
the four-file probe archive digest and committed source bindings are recorded
in evidence. The candidate service/installer was not uploaded or installed.

## D. Tests and validation

Source checkpoint `c42a240` had **701 runtime tests, 36 negative subtests, secret
scanning and all six hosted CI jobs passing**. Temporary command adapters passed
shell syntax and Python 3.9 parsing checks before execution; source/probe hashes
were rechecked before each live action. This documentation follow-up changes no
runtime code; validate evidence, source/hash bindings, links and secret safety.

Live result: response identity matches, no identity codes, JSON_OBJECT decoded,
FORMAT_PASS with no effective format codes, CONTRACT_PASS. Observed host versions:
AWS CLI 2.38.0 and Python 3.9.25. The current launch was before the enforced cutoff.
The timer, old installation and EBS mount were verified before the read.

The final state is independently verified stopped, public IPv4 released, both
original encrypted gp3 volumes attached. No authentication, chain-ID, Bronze
data, checkpoint, CloudWatch container log or live recovery success is claimed.

## E. Security review

Exactly one read attempt; zero secret writes, provider RPC calls, container
starts, controller invocations or pipeline S3 writes. No value, value hash,
provider response, key, URL or credential entered Git, terminal output or receipts.
The inspector captured its provider response in memory and emitted only fixed
format/identity metadata. The original secret remains unchanged.

The exact existing role/document and scoped instance access were reused. No IAM
expansion, resource provisioning, storage initialization, force/hibernate/skip-OS
shutdown, termination, Terraform or Snowflake operation occurred. Only temporary
tmpfs helper/control files and a transient shutdown timer were added during the
approved session. The one-run streaming image exception remains unused.

## F. Git and CI

Executed source: `c42a240afa0dbf1da394ab239aa0aa23c61e07c9` on develop.
[CI 44402339848](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/44402339848)
passed all six jobs. This result-only checkpoint will be committed/pushed after
validation and its automatic CI observed. Final checkpoint/CI metadata is retained
in the conversation and private result receipt. Main and unrelated duplicates
remain preserved; no main merge is authorized.

## G. Cost implications

Launch to independent stopped observation was **219.737 seconds**, under four
minutes. At the previously recorded USD 0.1714/hour combined EC2/IPv4 rate, that
conservative interval is approximately **USD 0.01046**, before retained storage,
API requests, tax and other charges. Actual billing was not queried; this is an
estimate using historical rates, not a spending cap. Retained EBS, images and
logs continue their usual costs while EC2 is stopped. No new resource was created.

## H. Remaining blockers

The payload-format blocker is resolved. Provider authentication and Arbitrum
chain ID 42161 have not yet been checked. The new versioned host candidate has
not been deployed, and Kafka/Spark have never run against the pilot paths.

A later conditional streaming window needs a complete reviewed operator package,
current image/security checks and new explicit approval for the exact candidate
installation, validation container/RPC, application start, scoped S3/log writes
and normal shutdown. This completed approval authorizes none of those actions.
No secret correction or additional secret edit is needed.

## I. Next step

Prepare the versioned candidate deployment and controlled chain-ID/streaming
commands offline, preserving the existing installation. Keep the same digest-pinned
images, Kafka identity, retained EBS and isolated Bronze/checkpoint prefixes.
Review conditional startup, one-writer controls, data acceptance, graceful drain,
rollback and cost verification before requesting the actual streaming window.

Do not restart EC2, repeat this probe or use an old dispatcher/approval. Keep
the secret unchanged. Failure of a later authentication/RPC gate must stop that
action and trigger normal shutdown; it must not trigger a blind secret edit.

## J. Whether production infrastructure changed

**Yes, within the approved runtime window:** the existing EC2 instance was started
and normally stopped, and temporary SSM files/timer were used. No new persistent
resource, Terraform state/configuration, IAM policy, original host installation,
secret, existing production platform or Snowflake privilege was changed. The
original disks are retained; shutdown clears the tmpfs helpers/transient timer.
Milestone 11.6C remains open for actual streaming; 11.5D remains paused.
