# Milestone 11.6C — Reader rollout tooling and one-read compatibility proposal

Result: [one-read compatibility passed; existing secret unchanged, EC2 stopped](aws_runtime_reader_probe_result.md).
The approved start/read scope is consumed. Proposal statements below preserve history.

**Offline checkpoint. Keep the existing AWS secret unchanged. No live action is
authorized by this report.** The next proposed AWS action is one format check,
followed by normal stop. A successful format check does not authorize streaming.

## A. What was implemented

Added a [versioned installer](../runtime/deployment.py), a
[tracked window guard](../scripts/runtime_pilot_window.py),
[read-only cloud checks](../scripts/check_runtime_cloud_readiness.py), and an
[offline probe packager](../scripts/prepare_runtime_reader_probe.py).

The installer verifies the frozen nine-file reader candidate and unchanged
release, then writes exclusively under
`/opt/blockchain-runtime-candidates/ae678174072b2484e2c095593f3363adf822904e`.
It preserves `/opt/blockchain-runtime` and its units. It refuses changed,
partial, redirected or unexpectedly writable candidates. The temporary candidate
unit is `/run/systemd/system/blockchain-pilot-reader-candidate.service`; it has
no enable-on-boot section, uses the new working directory, receives SIGTERM,
allows 420 seconds for drain, and requests OS shutdown afterward. Installation
never starts/enables it. Loaded unit overrides and shadowed units are rejected.
This installer was exercised only in temporary directories on the Mac.

The watcher admits launches strictly before arm + 300 seconds. Thirty seconds
of observation grace accommodates a timely launch appearing late; it does not
extend start permission. A late pending boot remains supervised and receives
normal stop when running. Late/stale/different boots cannot enter the guarded
SSM path. Deadlines remain relative to the actual accepted launch. Fresh running
preflight and IAM/image receipts must match that same boot before SSM entry.
This cannot prevent an owner manually starting EC2 after the watcher expires;
do not start after the displayed cutoff.

The probe packager reads four allowlisted files from committed Git blobs, checks
their internal dependency digests, and produces a private deterministic archive,
public-code upload command and one inspector command. Proposed commands are not
executed during generation. Each upload line is short enough for a terminal;
the target is a new root-only tmpfs directory, with exclusive file creation and
an external archive digest check. No credentials or secret values are packaged.

## B. Why

The [reader fix](aws_runtime_secret_reader_compatibility.md) accepts supported
JSON wrappers while preserving the decoded URL. Its compatibility with the
actual saved value remains unproven. Verify that once, without another secret
edit or an application deployment. This separates payload decoding from the
later questions of provider authentication, chain identity and streaming.

The prior watcher allowed ten minutes while the owner handoff said five. The
tracked guard now enforces the advertised cutoff. Versioned installation also
makes it possible to preserve and compare the old installation during a later
approved deployment.

## C. Files changed and proposed resource scope

New source: runtime/deployment.py, scripts/runtime_pilot_window.py,
scripts/check_runtime_cloud_readiness.py and scripts/prepare_runtime_reader_probe.py.
Updated the pure window policy; added installer, watcher and package tests,
updated policy tests, this report and its sanitized evidence. Added status links
to existing pilot/reader reports. README, application source, dependency locks,
Terraform, published images and the frozen nine-file candidate are unchanged.

[Sanitized validation evidence](evidence/aws_runtime_reader_rollout_review_2026-11-18.json)
records the tested source digests and the unapproved live proposal.

| Existing item | Exact proposed use, only after new approval |
|---|---|
| EC2 | One manual start of `i-0ca1671a59751acc5`, t3.xlarge, us-west-2a; then normal stop |
| Root EBS | Retain `vol-03bd5d26947719f98`, encrypted 30 GiB gp3 |
| Kafka EBS | Retain `vol-02c9fb14a9c568fb5`, encrypted 100 GiB gp3; no Kafka writes/formatting |
| Workload role/profile | Existing `blockchain-runtime-ec2`; no IAM changes |
| SSM document | Existing `blockchain-runtime-demo-shell`, version 1, reviewed canonical digest |
| Secret | One GetSecretValue for the exact ARN below, version and AWSCURRENT together |
| Guest files | Four helpers + public manifest in a new `/run/blockchain-reader-probe-<digest>` directory |
| Guest shutdown | One transient absolute shutdown timer bound to the observed boot; not enabled on boot |
| Terraform | No changes, backend initialization, authenticated plan, apply or import |
| New cloud resources | None; no ECR publication, secret creation, IAM expansion or storage allocation |

Secret ARN:
`arn:aws:secretsmanager:us-west-2:209479274979:secret:blockchain-runtime/11-6c/alchemy-websocket-url-d44flr`.
Pinned version: `12d38103-a68c-4226-b20a-3095cbb3de78`, also requiring AWSCURRENT.
No operator secret-read privilege is added; the existing instance role performs
the read. The assistant never requests the URL, key, MFA codes or secret contents.

The Kafka filesystem was [already initialized once](aws_runtime_ebs_initialization_result.md).
Verify XFS UUID `da1454d8-1bea-41a3-89d7-87bd570a47bd`, EBS serial and mount
`/var/lib/blockchain-runtime/kafka`; never initialize it again. A missing/uninitialized
replacement volume is a fail-closed recovery case requiring separate exact-volume
approval. Runtime and platform Terraform states remain separate and untouched.

## D. Tests and validation

701 runtime tests and 36 negative subtests pass locally, with one existing
websockets deprecation warning. Ruff passes for the changed Python files.
Tests cover input replacement after verification, exclusive install/reuse,
tampered/missing/extra files, directories, symlinks, interrupted installs,
permissions, inherited unit overrides, cutoff boundaries, late pending/running
boots, normal-stop flags, stale/changed-boot SSM checks and future-dated receipts.
Package tests verify deterministic committed inputs, private modes, dependency
hashes, dirty-source refusal, Python 3.9 bootstrap parsing and shell syntax only.

The actual frozen candidate archive and unchanged release were also installed
and reverified in a private local temporary directory. No guest unit, systemctl
operation, Docker operation, credential refresh or AWS API call was performed.
CloudWatch, S3, live recovery and actual-secret compatibility are not claimed.

Frozen candidate source: `ae678174072b2484e2c095593f3363adf822904e`.
Archive SHA-256: `568d0fc889ca7139aefa651d6d5dccea90ac2bc2106130d8d6451e736b3c9143`.
Release SHA-256: `7f948e94cec7f7aebaf2aa4dab4ddd8e92a60f083f17bb9cefcff4db89cd200f`.
The original archive remains `669a806f2370cc33c43296b769b9e8eddb39d939eaeb624591f637782242a627`.

## E. Security and correctness review

The first proposed probe calls only the pinned inspector. It reports fixed
representation names, identity checks and format codes; no value/value hash,
stdout provider response, key, URL or credential is retained. Exactly one secret
read, zero secret writes, zero provider RPC calls, zero containers/controllers
and zero pipeline S3 writes are in this proposal. Do not call the chain-check
helper's CLI: it is packaged only as a verified inspector dependency.

Stop after the result, including failure. Unknown shapes require a reader-side
contract investigation; do not ask the owner to rewrite the same secret. A future
streaming run still needs authenticated Arbitrum chain ID 42161, exact image and
finding checks, empty isolated prefixes, one checkpoint writer, and fresh approved
deployment commands. The one-run image exception is not consumed by this probe.

## F. Git and CI

Started on develop at `227c63982430b0b7cf8966341895b9e3ba48f003`. Rechecked
[CI 44390224777](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/44390224777):
all six jobs successful. Commit only the reviewed files, generate the probe from
that committed checkpoint, push develop, and observe its automatically triggered
CI. The resulting checkpoint/package/CI metadata belongs in the follow-up evidence.
Unrelated untracked duplicates and main remain preserved.

## G. Cost implications

This work used no AWS APIs or paid runtime. Existing retained disks, images and
logs continue their usual charges. At the previously recorded combined EC2/IPv4
rate of USD 0.1714/hour, five minutes is about USD 0.0143, fifteen about USD 0.04285,
and the full 45-minute supervision interval USD 0.12855. These are historical-rate
estimates, not fresh quotations or spending caps; API, retained storage, tax and
other charges are additional. Normal-stop requests do not guarantee an exact
stopped time.

Aim to stop immediately after the one-read result. With no application confirmed,
the watcher requests normal stop at launch + 15 minutes. Normal stop is otherwise
due at +30, guest shutdown at +35, target stopped at +40 and outer supervision
at +45. Never start without the detached watcher and fresh MFA session with at
least 90 minutes remaining. A Console start must occur before the displayed
five-minute cutoff; do not reuse an expired window or old start approval.

## H. Remaining blockers and future streaming acceptance

New approval is required for the paid compatibility window, temporary guest files,
one pinned read, transient timer and normal stop. Existing credentials have not
been refreshed for this offline work. The last independently recorded EC2 state
was stopped with IPv4 released; this checkpoint performs no fresh cloud readback.

The format probe does not install the candidate service or prove streaming. Later
deployment must separately package/review the installer sidecar and verify old
host/unit preservation. Existing digest references remain:

- Producer: `209479274979.dkr.ecr.us-west-2.amazonaws.com/blockchain-runtime-producer@sha256:63d9d0f24a1cb7eb30b080e58f8a40ddd49292b89020328409e6f45319597afd`
- Spark: `209479274979.dkr.ecr.us-west-2.amazonaws.com/blockchain-runtime-spark@sha256:bb03f7d9917f2d8dc7d2fc1fc6a1df83fe876f5298e98c4163c689a1b890a134`
- Kafka: `209479274979.dkr.ecr.us-west-2.amazonaws.com/blockchain-runtime-producer@sha256:1113a10e8a93fc12b2d310ef6f53b8f5fc95fb806a8c6edcfa6b67d17959378b`

Future Bronze/checkpoint paths remain under
`s3://johnson-blockchain-analytics-prod/blockchain-analytics/pilot/runtime/11-6c-v1/`,
specifically `bronze/swaps/` and `checkpoints/swaps_bronze/`. Logs use existing
`/blockchain/runtime/containers`. The format-only proposal writes none of these.
Future smoke tests need producer events, Kafka health, Spark committed positive
batches, correct Arbitrum records, readable Bronze Parquet and matching offsets.
Kafka/Spark restart and checkpoint recovery remain offline-tested; live recovery
would require another explicitly reviewed controller/start scope. Do not spend
the one-run image exception on an unreviewed restart.

## I. Next step, shutdown and rollback

After CI, request approval only for the one-read compatibility scope. Then record
the exact new owner approval and source/file hashes in a new private execution
directory and new marker. Never copy an old approval as active permission.

1. Fresh private MFA login; verify the expected deployer account/role and margin.
2. Run stopped preflight and read-only IAM/image checks using the new tracked tools.
3. Detach the watcher, verify its heartbeat, and show the actual cutoff. The owner
   starts only the exact EC2 instance before it expires.
4. Revalidate current running boot, IAM/images and SSM document; require fresh
   matching receipts. Enter through the guarded SSM path.
5. Install/verify the boot-relative transient shutdown timer first. Upload only
   the new hash-verified probe package to tmpfs; do not run the old dispatcher.
6. Invoke the pinned inspector once. Retain only its sanitized result. Normal
   stop immediately; no automatic retry, chain check, deployment or streaming.
7. Observe stopped, independently verify no public IPv4 and both original disks
   attached, close SSM and record timestamps/cost estimate. No forced stop,
   termination, deletion, volume reset or sensitive output.

For this probe, rollback means stop normally and preserve the result. The original
secret, installed application and disk contents were never replaced. Tmpfs helpers
and timer disappear on shutdown/reboot. Existing retained resources continue to
bill; deletion needs separate approval. If an operator check fails, preserve
evidence and stop that action; use normal Console stop if the watcher fails.
For a later candidate deployment, an unsuccessful run must be stopped and reviewed;
do not automatically restart the legacy service against a changed checkpoint.

## J. Whether production infrastructure changed

No. No credentials refreshed, AWS resources started/modified, secret read/edited,
unit installed, Docker/container run, Terraform operation or Snowflake action
occurred. Milestone 11.6C remains open and 11.5D remains paused.
