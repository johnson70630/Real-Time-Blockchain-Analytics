# Milestone 11.6C — Arbitrum confirmation and proposed streaming window

Execution outcome: [the owner approved this window; the private chain gate failed and EC2 was normally stopped before ingestion](aws_runtime_demo_abort_result.md). This start scope is consumed. The proposal below preserves its original pending status; it is not permission to retry.

**Offline preparation only. The owner confirmed Arbitrum after privately checking
the existing secret. No secret correction is needed based on that attestation.
Paid start, deployment, runtime secret retrieval and ingestion remain unapproved.**

## A. What was implemented

Recorded the owner confirmation separately from the historical pending result.
Prepared a concrete [release candidate](evidence/aws_runtime_release_candidate_2026-11-17.json)
and a host-only [chain preflight](../scripts/check_runtime_chain.py). The
[readiness evidence](evidence/aws_runtime_network_readiness_2026-11-17.json)
binds their exact hashes and fresh public prices. This is a proposal, not a
record of a live chain check or permission to execute one.

The preflight requires the exact release bytes, root on the exact EC2 instance,
disabled swap and the reviewed local amd64 producer image. It retrieves only
the exact secret through the instance role, captures it in memory and passes
it through stdin to a transient, non-root container using that existing image.
The client requires the Arbitrum hostname and a successful `eth_chainId` reply
of `0xa4b1` (42161). It prints only fixed success/failure messages.

## B. Why

The configured chain labels must match the source before any data is written.
Owner inspection resolves the earlier Ethereum guidance error; a separate live
RPC check will verify connectivity and chain identity. A hostname attestation,
offline tests and an IAM policy readback do not establish live retrieval or
RPC success. The additional operator check does not rebuild images or alter
the frozen application/host bundle.

## C. Files and exact proposed scope

New files: this report, the two evidence JSON files linked above,
`scripts/check_runtime_chain.py` and `tests/test_runtime_chain_preflight.py`.
Only the current-status links change in the pilot and previous apply report.
README, Terraform, existing runtime code, Compose, systemd units, dependency
locks, all frozen images and unrelated untracked files are unchanged.

The proposed demo uses only these existing resources in account `209479274979`,
Oregon `us-west-2`:

| Item | Exact scope |
| --- | --- |
| EC2 | One start of `i-0ca1671a59751acc5`, t3.xlarge, standard CPU credits; normal stop of this instance |
| Kafka EBS | Existing encrypted 100 GiB gp3 `vol-02c9fb14a9c568fb5`; XFS UUID `da1454d8-1bea-41a3-89d7-87bd570a47bd` |
| Kafka mount | `/var/lib/blockchain-runtime/kafka`; application logs in its `data` directory |
| Workload IAM | Existing `blockchain-runtime-ec2` role, boundary v2 and exact secret grant verified in the completed apply |
| Secret | Existing `blockchain-runtime/11-6c/alchemy-websocket-url`; exact ARN ending `-d44flr` in the candidate; GetSecretValue through the host role only |
| Images | Exactly the three accepted digest references in the candidate; ECR pull only |
| Logs | Existing `/blockchain/runtime/containers` group; new runtime log streams and events |
| S3 | Existing `johnson-blockchain-analytics-prod` bucket; only `blockchain-analytics/pilot/runtime/11-6c-v1/` |
| Host install | Verified nine-file bundle under `/opt/blockchain-runtime`; exact candidate and separately verified operator check; manual systemd installation/start |

**Terraform changes: none. Expected plan/apply scope: none.** No backend
initialization, plan, apply, import, resource creation/replacement, IAM expansion,
new image publication, secret edit or Snowflake action is included. New S3
objects, CloudWatch log streams/events and guest/container files are proposed
runtime writes and can incur charges.

Frozen host bundle SHA-256:
`669a806f2370cc33c43296b769b9e8eddb39d939eaeb624591f637782242a627`.
Release SHA-256:
`7f948e94cec7f7aebaf2aa4dab4ddd8e92a60f083f17bb9cefcff4db89cd200f`.
The candidate's cluster ID was generated once offline. Before installation,
require the Kafka data directory to be absent/empty or its existing
`meta.properties` to match this ID and node 1. A mismatch blocks deployment;
never overwrite metadata, generate a replacement ID or reset storage.

## D. Tests and validation

The 12 new operator tests exercise valid RPC round trips, wrong networks and
responses, timeout/error redaction, exact release/host guards, architecture
failure before secret retrieval, clean credential-free environment and secret
transport over stdin. Combined operator/bundle/pilot tests pass **83 tests and
22 negative subtests**, with one existing dependency deprecation warning.
Ruff passes. Validate manifest/acceptance/bundle bindings, secret scan, local
links and whitespace before committing; observe new CI after push.

The existing controller's `validate` action is offline. Its `run` action and
the new check's `--execute-on-approved-host` flag are execution paths requiring
the separate approval below. A CLI flag is not authorization.

## E. Security review

No credential value was obtained by this checkpoint. The proposed live check
uses no URL/key in arguments, environment, files or returned output. Docker
logging is disabled for the transient check; core dumps are disabled. Normal
TLS certificate/hostname verification remains enabled. The container has no
capabilities, no-new-privileges, a read-only filesystem and bounded memory/CPU/
processes. It neither starts the producer nor writes Kafka/S3.

The RPC check and controller independently read the secret; they are not atomic.
Keep rotation off and make no secret changes during the supervised window.
If either endpoint/metadata changes, stop and re-review. A driver timeout may
leave the transient container running; fail the gate, inspect it and normally
stop the instance. Never launch the streaming service after a failed check.

The existing shared EC2-role exposure and accepted software vulnerabilities
remain. The [conditional acceptance](aws_runtime_pilot_security_acceptance.md)
is bound to the exact images, one supervised public-data run, at most 3,600
streaming seconds, and expiry **2026-11-24 00:00 UTC**. Additional findings,
changed artifacts or unmet controls block the run. This check is a separate
operator preflight, not a modification of the frozen workload images.

## F. Git and CI

Checkpoint the reviewed files on develop after validation and observe the
automatically triggered six-job CI. The previous `e746a09` checkpoint passed
all six jobs in [CI 44180823131](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/44180823131).
Report this checkpoint's SHA/run separately. Preserve the 67 unrelated untracked
duplicates; tracked cleanliness does not imply an entirely clean working tree.
Main remains unchanged.

## G. Cost and duration proposal

Fresh unauthenticated AWS Price List reads on 2026-11-17 verified Oregon
Linux t3.xlarge at USD 0.1664/hour and public IPv4 at USD 0.005/hour. Exact
SKU/rate/source details are in the readiness JSON. Combined **USD 0.1714/hour**;
150 minutes is approximately **USD 0.43**, before logs, requests, transfer,
existing retained resources and taxes. This is an estimate, not a spending cap.
See [EC2 pricing](https://aws.amazon.com/ec2/pricing/on-demand/) and
[public IPv4 pricing](https://aws.amazon.com/vpc/pricing/).

Propose one supervised window of up to 150 minutes from EC2 start. Allow at
most 45 minutes for boot/install/preflight; otherwise abort without ingestion.
Invoke the controller at most once, for 3,600 seconds including its pull/startup
work, then allow bounded drain/stop verification. Target confirmed stopped by
minute 120. Record absolute UTC start/target/end and a valid session with at
least three hours remaining immediately before execution. No automatic retry.

Start and verify the unchanged guest emergency timer before sensitive runtime
work. Its 150 minutes run from activation after boot, not from EC2 start. It is
a backup, not enforcement of the total window. Maintain an independent operator
watcher from EC2 start and an administrator Console stop fallback through the
window. Neither mechanism guarantees a hard bill ceiling. Existing EBS, secret,
ECR, alarms, logs and S3 storage continue charging after EC2 stops.

## H. Live prerequisites and acceptance

After explicit approval, first verify fresh operator identity/account/region,
exact instance/tags/profile, empty ingress, standard credits, shutdown-to-stop,
unchanged boundary/secret grant, disk IDs/attachment and current image/scan
bindings. A changed/new finding blocks the existing security exception. Keep
the administrator stop fallback available; do not broaden IAM if access fails.

Use the installed SSM plugin and existing private approved session document;
no SSH or ingress change. The deployer has no StartInstances grant; after this
window is approved, the owner starts only the exact instance in the Console.
No IAM expansion is proposed. Verify automatic mount persistence after boot, UUID,
volume serial, XFS type, available disk space, Docker/Compose and zero unexpected
containers. The filesystem initialization is already complete. **No mkfs,
wipefs, disk formatting, metadata reset or full bootstrap replay is permitted.**

Transfer only the exact reviewed bundle, candidate and separate check over the
approved SSM channel. Verify external hashes before executing/extracting; refuse
overwrites/unsafe paths and stop on any unexpected installation. Install only
the reviewed units; do not enable the pilot or timer on boot. Authenticate ECR
through the workload role using a private tmpfs Docker config, pull exact
digests and verify amd64 identity. Never display registry tokens or broad logs.

Run the operator chain check before any producer/Kafka/Spark startup. On its
fixed success only, invoke the reviewed service once. Require Kafka health and
topic creation, confirmed producer acknowledgements and Spark committed batch
progress. Inspect only sanitized metrics and public pilot records. Verify:

- Bronze objects under `.../11-6c-v1/bronze/swaps/` and checkpoint objects under
  `.../11-6c-v1/checkpoints/swaps_bronze/`, with exactly one checkpoint writer.
- Kafka partition/offset metadata and the frozen Bronze schema; matching
  Arbitrum chain labels, transaction/log identity and sensible timestamps.
- No missing Kafka offsets in the observed committed range, and documented
  source reconnects/possible chain gaps. WebSocket delivery is not a complete
  chain-history guarantee; keep Bronze raw and do not claim business-level
  exactly-once delivery or promote to Silver/Gold without reconciliation.
- Sanitized CloudWatch logs and final offset-based drain evidence.

Live Kafka/Spark restart recovery is still unverified. The frozen controller
stops on service failure; intentional failure injection is outside this first
run and would require a separately reviewed recovery procedure/approval. Passing
this initial demo does not close the whole 11.6C recovery acceptance milestone.

## I. Shutdown, rollback and approval request

For normal shutdown, stop the producer, drain Spark to confirmed Kafka end
offsets, verify final checkpoint/clean exit, stop Kafka and remove only this
run's tmpfs secret/auth files. The controller/service implements bounded stop
and host poweroff; independently verify EC2 reaches `stopped`, not just
`stopping`. Do not disconnect and assume the node stopped.

On a failed gate or runtime error, preserve Kafka EBS/checkpoints and private
evidence, clear only this run's transient credentials when safely possible,
and normally stop this exact instance. An unverified drain means recovery is
required; do not delete offsets, rerun a stale Terraform plan or restart blindly.
No force stop, termination, resource deletion, checkpoint deletion or rollback
to a rejected image is included. Any destructive cleanup needs separate approval.

Record actual running interval, final stopped/public-IP state, attached disks,
retained billable inventory and any failed cleanup. Review next-day billing
when metering arrives. Stop is not deletion or zero recurring cost.

**Pending explicit approval:** one paid start of `i-0ca1671a59751acc5`, the exact
SSM transfer/guest installation, ECR pulls and transient chain check, workload
retrieval of the one secret without exposing it, one bounded streaming demo
using the exact candidate/images and isolated S3/log writes, graceful shutdown
and normal stop under the window above. A failed prerequisite stops that action;
it does not authorize privilege expansion, a different artifact or a retry.

## J. Production infrastructure changes

None in this checkpoint. Only offline files/tests and public price downloads
were performed. The last verified instance state is stopped; no fresh
authenticated AWS call was made here. Historical approved IAM/resource changes
remain documented separately. No secret value/edit, EC2 start, deployment,
Terraform operation, EBS mutation, Snowflake change or main merge occurred.
Milestone 11.5D remains paused.
