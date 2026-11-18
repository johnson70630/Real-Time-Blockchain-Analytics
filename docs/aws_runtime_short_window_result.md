# Milestone 11.6C — Short pilot stopped at the format gate

Current follow-up: [revised input diagnostic and conditional 45-minute pilot prepared for a new approval](aws_runtime_transport_review.md). EC2 remains stopped; no new paid start is authorized. Earlier statements below preserve checkpoint history.

**EC2 is stopped, with no public IPv4. The private validator returned
`SECRET_FORMAT / INVALID_ENDPOINT` before RPC or application startup.**
The owner-reported whole-payload PASS remains an attestation; this attempt did
not isolate whether the retrieved value or its container stdin handoff differs.
No automatic retry or additional paid start is authorized.

## A. What was implemented

Executed the approved 45-minute scope at source `4b13bb1`. The first handoff
expired without a start; its watcher was cancelled after confirming stopped.
After fresh MFA, the same unconsumed approval was rearmed in a new private
directory. The owner started the exact instance at **2026-11-18 01:02:39 UTC**.

Read-only checks verified the instance/disks, no ingress, standard CPU credits,
normal-stop access, private SSM document, workload IAM and unchanged accepted
image/finding inventories. The detached watcher remained healthy. Through SSM,
the verified operator capsule was transferred to tmpfs; the existing installation
was preserved. Sequential checks verified the current boot/mount, existing
installation, absolute temporary shutdown timer at **01:37:39 UTC**, original
fallback timer, initially empty S3 prefix and cached images before the chain step.
The failed reply retains only the safe diagnostic; there is no standalone saved
receipt for the successful timer checks.

Secret retrieval and client import succeeded, then endpoint validation failed.
Normal stop was requested at **01:03:46 UTC**. The watcher observed stopped at
**01:04:12 UTC**; independent readback at **01:04:21 UTC** confirmed stopped,
public IPv4 absent and both original disks attached. SSM was closed. No controller
intent, Kafka/producer/Spark service startup or pipeline S3 write occurred.

## B. Why

The stop limited paid idle time while preserving EBS and existing artifacts.
The executed checker validated formatting only inside the container, so this
failure cannot establish that the stored value is wrong. Avoid another blind
secret edit or repeated paid boot based on the same ambiguous diagnostic.

The offline operator now validates the retrieved CLI JSON value on the host
using the exact unchanged client rules before Docker. A rejection there reports
`SECRET_FORMAT`. If host validation passes but the client rejects stdin, the
outer checker reports `INPUT_TRANSPORT`. RPC/authentication failures retain
their original stages. No normalization, JSON unwrapping or retry was added.

## C. Files changed

- [Operator check](../scripts/check_runtime_chain.py) and [regression tests](../tests/test_runtime_chain_preflight.py).
- This report and [sanitized evidence](evidence/aws_runtime_short_window_result_2026-11-18.json).
- Current-status links in the pilot and earlier short-window review.

Original receipts are preserved privately and bound by hashes in the evidence.
The nine frozen host files, original release/check, images, Docker Compose,
systemd definitions, Terraform, dependencies and README are unchanged.

## D. Tests and validation

**All 548 runtime tests and 36 negative subtests pass**, including the 64
focused format/chain/window tests. Ruff passes. One existing websockets
deprecation warning remains. The nine frozen host files match the original
verified bundle byte for byte. New cases verify
that wrapped/quoted/whitespace/wrong-network values cannot reach Docker, valid
values retain their exact bytes as strings, host-pass/client-reject failures
become transport errors, RPC failures retain their meaning, and diagnostic
parsing/exception handling does not expose input. No real secret was used.

The local mutable offline producer tag currently resolves to a different digest
than the accepted release. An exact-image container retest was therefore not
run; no image pull, retag or rebuild occurred. Earlier test receipts remain
historical evidence, not current local-image verification. Review hashes, links,
secret scan and whitespace; checkpoint on develop and observe all six CI jobs.
The offline change has not been deployed and does not fix or identify the live cause.

## E. Security review

No real URL, key, MFA code, credential or provider body reached chat/Git.
Secret read remained limited to the approved exact ARN through the workload
role. The check used stdin, non-root execution, no container logging, normal
TLS, read-only storage and bounded resources. Stop cleared guest tmpfs/process
memory. No secret edit, IAM expansion, new image or destructive storage action
occurred. The new check preserves the exact release/host/image/secret guards.

## F. Git and CI

Executed source: `4b13bb1d5b189a4eff9675b106501c7f771595d4`, with all six jobs
successful in [CI 44209467686](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/44209467686).
Commit this reviewed follow-up on develop and record its new CI in conversation.
Main and the 67 unrelated untracked duplicates remain untouched.

## G. Cost implications

The launch-to-observed-stop interval was **93.7991 seconds**, an upper bound on
actual running time from polling. At the previously recorded compute/IPv4 rate
of USD 0.1714/hour, that interval is approximately **USD 0.0045**, before requests,
logs, transfer, retained storage and taxes. This is an estimate, not an invoice.
Retained EBS, ECR and the existing secret continue to incur their usual charges.
No new AWS resource was created.

## H. Remaining blockers

The live input-format mismatch is unresolved. Alchemy connectivity/chain ID,
first committed Bronze/checkpoint data, CloudWatch application delivery and
Kafka/Spark live recovery remain unverified. The one-run image-risk acceptance
is still unused for streaming; it does not authorize another paid boot.

## I. Next step and rollback

Keep EC2 stopped and preserve the secret, disks, cluster ID and prefixes.
The revised operator is ready for review but requires a fresh exact scope and
approval before another live retrieval/start. Do not automatically repair or
rotate the secret, reinstall the host, reset offsets or replay Terraform.
No application rollback is required because streaming never started. A later
successful run must still prove data correctness, graceful drain and cleanup;
failure-injection recovery requires its own approval.

## J. Whether production infrastructure changed

The owner started the existing EC2 instance once under approval; the assistant
transferred temporary operator files, created the approved transient shutdown
control, performed the scoped secret read, and normally stopped the instance.
Persistent application configuration was preserved. No new AWS resource,
Terraform/IAM/Snowflake/secret-value change, EBS formatting/deletion or main merge
occurred. Milestone 11.6C remains open; 11.5D remains paused.
