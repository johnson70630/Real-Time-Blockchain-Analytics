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
Checkpoint this documentation-only result on develop and record the new CI in
conversation. Main and unrelated untracked duplicates remain untouched.

## G. Cost implications

Launch to observed stop was **101.428955 seconds**, an upper bound from polling.
At the previously recorded USD 0.1714/hour for compute/IPv4, that interval is
approximately **USD 0.00483**, before API/log/transfer usage, retained resources
and taxes. This is not an invoice or hard cap. No new resource was created.
Retained EBS/ECR/secret charges continue while the instance is stopped.

## H. Remaining blocker

The specific retrieved-value format issue is still unknown. The owner must
privately compare the complete current saved payload from the exact secret in
Oregon against the existing Mac checker. The assistant requested only the
checker's JSON status/codes, with EC2 stopped and no secret edit. That result is
pending; no assistant clipboard read is permitted. Live chain ID, ingestion,
checkpoint/data correctness, application logging, drain and recovery remain open.

## I. Next step, rollback and cleanup

Use the owner Console access to select `blockchain-runtime/11-6c/alchemy-websocket-url`
with ARN ending `-d44flr`, Retrieve secret value → Plaintext, and copy the entire
value while the owner-run checker waits. Do not copy only a field from Key/value,
paste the value into chat/terminal, or edit/save anything yet. Return only status
and codes. This check needs no EC2 runtime or additional AWS permission.

Keep EC2 stopped and preserve EBS, Kafka identity, checkpoints and earlier evidence.
No application rollback is needed because streaming did not start. Any proposed
secret correction needs its exact review and approval; another paid boot is not
the next default action. No force stop, restart, offset reset, deletion or Terraform
replay is included. This stop did not delete retained, billable resources.

## J. Whether production infrastructure changed

Yes, within the approved scope: the existing EC2 instance started/stopped once,
temporary operator files/timer were created, and the exact secret was read once.
No new AWS resource, persistent application configuration, IAM/Terraform/secret-value
change, EBS formatting/deletion, Snowflake change or main merge occurred.
Milestone 11.6C remains open; 11.5D stays paused.
