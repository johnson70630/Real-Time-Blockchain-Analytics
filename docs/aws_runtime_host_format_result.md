# Milestone 11.6C — Retrieved payload rejected on the host; EC2 stopped

**The revised diagnostic isolated the failure before the validation container
or any Alchemy RPC request. EC2 is stopped, with both disks preserved.**
This paid start is consumed. No retry, additional start or secret edit is authorized.

## A. What was executed

The owner approved the [reviewed scope](aws_runtime_transport_review.md) at source
`5e1d779`, refreshed MFA privately, and manually started the exact instance at
**2026-11-18 05:43:12 UTC**. Read-only checks verified instance/disks, no ingress,
standard CPU credits, normal-stop authorization, SSM document, existing workload
IAM and unchanged accepted image manifests/finding inventories. The detached
watcher was healthy. SSM transferred only the verified temporary operator files.

The new retained pre-chain receipt confirms the current boot, existing
installation, initially empty pilot prefix, three cached image identities and
absolute guest shutdown timer at **06:18:12 UTC**. The revised operator then
retrieved the scoped Secrets Manager CLI response successfully and rejected it
at host validation with `SECRET_FORMAT / INVALID_ENDPOINT`.

Normal stop was requested at **05:44:27 UTC**. The watcher confirmed stopped at
**05:44:53 UTC**; independent readback at **05:44:54 UTC** confirmed no public
IPv4 and both original disks attached. SSM closed. No validation container,
Alchemy RPC, controller, producer/Kafka/Spark service or pipeline S3 write followed.
Docker image inspection did run earlier; this is not a claim that Docker was unused.

## B. What the result means

The host validates the CLI JSON response and its decoded SecretString using the
same endpoint rules as the client. This rejection happens before stdin is handed
to a validation container, isolating this attempt from Docker stdin transport,
WebSocket connectivity, TLS and Alchemy authentication.

The generic safe code does not identify which format rule failed, nor distinguish
JSON decoding from endpoint validation. Do not claim the API key is bad, guess
that a particular wrapper/character exists, or rotate/edit the secret blindly.
The prior owner-reported whole-payload PASS remains preserved as an attestation;
it does not match the result against the value retrieved on this host.

## C. Files and evidence

Added this report and [sanitized evidence](evidence/aws_runtime_host_format_result_2026-11-18.json),
plus current-status links in the pilot and proposal. Private approvals, metadata,
dispatch, safe failure, timer proof, watcher and stop receipts are hash-bound in
the evidence and preserved outside Git. No application, operator source, image,
Compose, frozen host file, dependency, Terraform or README change is included.

## D. Tests and validation

**64 focused tests and 36 negative subtests pass again.** The unchanged operator
previously passed all 548 runtime tests and six CI jobs; its exact SHA remains
`e4dd9034677e4f507d097dfc79b19b0c551b5110a31570648930a537f6bf9bb2`.
The prepared adapters previously passed 14 offline contract cases. The live failed
reply now retains the pre-chain controls, proving that receipt path works on failure.

Five additional loopback-only cases used the real local AWS CLI 2.35.2 with
unsigned requests, credential/config files disabled and dummy data. The exact
`--query SecretString --output json` path preserved a bare URL and rejected JSON
object/quoted wrappers, a trailing newline and a binary-only response. No production
AWS call or real secret was used. This does not reproduce the live cause or prove
that the guest CLI behaves identically; its exact version was not retained.

Review source/private evidence hashes, frozen-file equivalence, links, secret scan
and whitespace; create the develop checkpoint and observe its CI. Tests make no
real secret/clipboard reads. No live Kafka/Spark recovery was attempted.

## E. Security review

Only the approved workload role retrieved the exact secret ARN ending `-d44flr`.
The actual URL/value, key, value hash, raw CLI response and provider text never
entered chat or Git. The safe diagnostic discloses only constant codes and control
booleans/timestamps. There was no second probe, additional permission, secret edit
or validation bypass. Stop clears guest tmpfs/process memory. The risk acceptance
for the exact images remains unused for streaming and expires 2026-11-24 00:00 UTC.

## F. Git and CI

Executed source `5e1d7796a7cf946ff88edd701569db39291c45c5` passed all six jobs in
[CI 44214730587](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/44214730587).
The initial result checkpoint `0f2c7df` passed all six jobs in
[CI 44246610578](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/44246610578).
Checkpoint the owner-attestation/loopback follow-up on develop and observe its CI.
Main and unrelated untracked duplicates remain untouched.

## G. Cost implications

Launch to observed stop was **101.428955 seconds**, an upper bound from polling.
At the previously recorded USD 0.1714/hour for compute/IPv4, that interval is
approximately **USD 0.00483**, before API/log/transfer usage, retained resources
and taxes. This is not an invoice or hard cap. No new resource was created.
Retained EBS/ECR/secret charges continue while the instance is stopped.

## H. Remaining blocker

The specific retrieved-value format issue is still unknown. The owner completed
the requested private check and returned `{"codes": [], "status": "FORMAT_PASS"}`.
This is recorded as an attestation; the value, exact selected ARN/version and
clipboard contents were not independently read. It does not explain the host-side
rejection, so no secret edit, rotation or additional paid boot is justified yet.
Live chain ID, ingestion, checkpoint/data correctness, application logging, drain
and recovery remain open.

## I. Next step, rollback and cleanup

The copied-value format check is complete; do not ask the owner to repeat it.
The next missing information is the version ID, creation time and stage labels
for `AWSCURRENT` on the exact existing secret in Oregon, plus whether it is the
version the owner saved. Request only that metadata, never a value or screenshot.
Do not move a staging label or edit anything. This needs no EC2 runtime or IAM
expansion. The current-version metadata result is pending.

When no version is specified, GetSecretValue uses `AWSCURRENT`; SecretString and
SecretBinary are separate response fields. These API semantics identify a useful
comparison, not the cause of this incident. See the primary
[AWS GetSecretValue reference](https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html).
The executed query discarded version/field metadata, so the host's returned
version cannot be reconstructed from the retained safe result. A future revised
probe would need one scoped retrieval that verifies response identity/version
and emits only metadata and specific constant format codes; it is not deployed
or approved here. Do not authorize another boot just to repeat the generic check.

Keep EC2 stopped and preserve EBS, Kafka identity, checkpoints and earlier evidence.
No application rollback is needed because streaming did not start. Any proposed
secret correction needs its exact review and approval. No force stop, restart,
offset reset, deletion or Terraform replay is included. This stop did not delete
retained, billable resources.

## J. Whether production infrastructure changed

Yes, within the approved scope: the existing EC2 instance started/stopped once,
temporary operator files/timer were created, and the exact secret was read once.
No new AWS resource, persistent application configuration, IAM/Terraform/secret-value
change, EBS formatting/deletion, Snowflake change or main merge occurred.
Milestone 11.6C remains open; 11.5D stays paused.
