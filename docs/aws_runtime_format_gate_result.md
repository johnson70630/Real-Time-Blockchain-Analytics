# Milestone 11.6C — Endpoint-format gate isolated; owner-only correction proposed

Follow-up: [owner reports saving the approved correction and whole-payload FORMAT_PASS; shorter pilot scope prepared](aws_runtime_short_window_review.md). The pending correction statements below preserve the original review history.

**The second approved window ended with EC2 stopped and no application startup.
The diagnostic reached `SECRET_FORMAT / INVALID_ENDPOINT` before an RPC request.
The owner validated a copied URL, then clarified that only the URL was copied.
That PASS does not establish the format of the complete stored SecretString.**

## A. What was implemented and observed

The owner approved one repeat window at source `c00a509`. Cloud metadata checks
again verified the expected identity, stopped instance/disks, SSM document,
normal-stop access, workload IAM and unchanged image/scan inventories.
The instance launched at **2026-11-17 23:24:38 UTC**. The revised check retrieved
the secret successfully, imported the client, then rejected the endpoint input
at its format gate. No Alchemy RPC request or application invocation followed.
There was no automatic retry.

Normal stop was requested at **23:29:15 UTC**; the detached watcher confirmed
stopped at **23:29:41 UTC**. Readback at **23:37:11 UTC** confirmed stopped,
no public IPv4 and the same encrypted gp3 root/Kafka disks attached. Kafka
delete-on-termination remains false. No Bronze/checkpoint write occurred.

Added an [owner-run local format checker](../scripts/check_runtime_endpoint_format.py).
It waits for confirmation before reading the Mac clipboard once, prints only
constant format codes, uses the actual cloud validator, disables core dumps,
and makes no AWS/network request. It writes no credential file and does not
modify the clipboard. The assistant did not execute its clipboard action.

## B. Why

The copied URL passed local validation, but the owner subsequently said
`only url actually` when asked whether the complete Plaintext payload was copied.
A valid URL field can exist inside a payload that the bare-string contract
rejects. The runtime expects the entire SecretString to be one WSS URL, with
no JSON wrapper, quotes or whitespace. Do not label the stored payload invalid
solely from the copied field's PASS or assume the API key is bad.

This separates a payload contract issue from IAM and provider connectivity.
There is no reason to expand IAM, rotate a key, bypass TLS or keep compute
running while the owner checks the full payload privately.

## C. Files changed and evidence

- The local checker and [its tests](../tests/test_runtime_endpoint_format.py).
- This A–J report and [sanitized result/proposal](evidence/aws_runtime_format_gate_2026-11-17.json).
- Current-status links in the pilot and prior abort report.

Private approval, exact diagnostic, watcher recovery, stop, disk and offline
image receipts remain outside Git; their hashes are bound in the evidence.
No README, Terraform, application/runtime source, Docker image, Compose,
systemd definition or dependency-lock change was made in this checkpoint.
All existing installed host files and earlier artifacts are preserved.

## D. Tests and validation

The new checker has **18 tests**. Combined with the cloud checker, **43 tests
and 28 negative subtests passed**, with Ruff passing. Cases cover JSON/quotes,
hidden whitespace, scheme/host/path/port issues, malformed URLs, consent before
clipboard access, no clipboard access from noninteractive execution, and
suppression of clipboard/exception contents. No real clipboard was read by tests.

Two additional offline tests ran in the **exact accepted producer manifest**
using dummy input and `--network=none`: valid stdin URL transport/success and
HTTP-denial classification both passed. No real credential or image pull was
used. This supports the transport path on the local engine; it does not prove
that the EC2 path received identical input.

The earlier local-image lookup was incomplete: the accepted image was found
through its archived local tag. The modern local engine's image ID equals the
manifest digest, rather than the config digest. The accepted manifest and
amd64 architecture were verified before executing either offline case.

## E. Security review and watcher limitation

No URL/key was returned to the assistant. The owner shares only format codes;
the stored value stays in the Console and owner-run local process. The helper
never prints input, exception details, actual hostname/path/key or clipboard
contents. It has no secret-edit or cloud-execution capability.

The original interactive watch process was absent and its heartbeat stale at
handoff; its exit cause is unknown. No secret or streaming action was taken
until a detached watcher was established with a private log/failure receipt.
It retained the same approved boot and original absolute shutdown deadlines.
This was recovery of a stop control, not another start or an extension.

The owner started 36.73 seconds after the requested five-minute handoff target,
within the original ten-minute arm interval. The session at the actual launch
still met the three-hour minimum. Future handoffs must use a detached process
with fresh heartbeat checks, plus the guest timer before sensitive runtime work
and the Console stop fallback. Do not rely on an interactive tool session alone.

## F. Git and CI

Checkpoint the reviewed helper/tests/report on develop after secret, link,
receipt-hash and whitespace review. Observe its new six-job CI and report the
SHA/status in conversation. Execution source `c00a509` passed all six jobs in
[CI 44196089997](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/44196089997).
Keep main and the 67 unrelated untracked duplicates unchanged.

## G. Cost implications

The conservative observed second interval was **303.25 seconds**, approximately
**USD 0.01444** at the same-day verified EC2/IPv4 rate. The two failed windows
together are approximately USD 0.02788 for compute/IPv4, excluding requests,
other usage, retention and taxes. These are estimates, not invoices or caps.
Retained EBS, Secrets Manager, ECR and monitoring charges continue after stop.

The proposed correction creates no new secret/resource and starts no compute.
Existing secret storage fees and ordinary API request charges continue.

## H. Remaining blockers and private check

Confirm the complete stored value matches the bare-URL contract. The owner may
run, without starting EC2:

```sh
python3 scripts/check_runtime_endpoint_format.py --clipboard
```

Start the checker first. At its prompt, privately copy the **entire stored value
from the secret's Plaintext tab**, then return to Terminal and press Enter.
Do not paste the secret into Terminal/chat or send a screenshot. Return only
the result/code line; copying that line replaces the sensitive clipboard text.
Copying only a URL field is insufficient to test the complete payload.

Live chain identity, first ingestion/S3 smoke, drain/checkpoint evidence and
Kafka/Spark recovery remain unverified. Milestone 11.6C is incomplete.

## I. Exact proposed secret correction and rollback

**Not approved yet:** owner-only, conditional format correction of exactly:

`arn:aws:secretsmanager:us-west-2:209479274979:secret:blockchain-runtime/11-6c/alchemy-websocket-url-d44flr`

In account `209479274979`, Oregon, use the existing validated URL/API key as the
entire SecretString. Remove only a wrapper, quotation marks or whitespace if
needed. Preserve the endpoint/key, ARN/name, encryption, tags, rotation,
replication, resource policy and all IAM settings. If the complete stored value
already equals that exact bare URL, do not save an unnecessary version; report
that fact so the input path can be investigated instead.

The owner performs any approved edit privately in the Console using existing
access; no new operator secret-read/edit permission is proposed. If access is
denied, stop without attaching policies. Never send the value to the assistant
or put it in Git, shell arguments/history, `.env`, images, user data or logs.

An update creates a new `AWSCURRENT` version; AWS retains the preceding version
as `AWSPREVIOUS`. See [AWS value-update guidance](https://docs.aws.amazon.com/secretsmanager/latest/userguide/manage_update-secret-value.html).
Preserve the prior version and metadata; any rollback is a separately reviewed
version-stage/value action, not deletion or restoration of Terraform state.
After an approved save, recheck the **whole** Plaintext value privately and
return only saved status plus `FORMAT_PASS`.

This proposal includes **no EC2 start**, deployment, Terraform operation, image
publication, IAM expansion, key rotation, EBS format, resource deletion,
Snowflake change or main merge. Prepare a fresh bounded pilot scope only after
payload readiness is confirmed; passing a format check is not deployment approval.

## J. Whether production infrastructure changed

Yes, in the second approved window: one existing EC2 start/normal stop, temporary
operator files, a private secret retrieval and manual guest-timer activation.
No application, S3 data write, new AWS resource, IAM/secret edit, EBS format,
Terraform state/backend action, platform/Snowflake change or main merge occurred.
The local helper and report make no production change. Secret correction and
any further paid start remain unapproved. Milestone 11.5D remains paused.
