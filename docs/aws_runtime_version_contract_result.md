# Milestone 11.6C — Exact stored-version format mismatch confirmed; EC2 stopped

**AWS returned the exact requested current version. Its stored SecretString
begins with JSON or a quote and fails the bare WSS URL contract. EC2 is stopped.**
No value was disclosed. No pipeline or Alchemy request ran. The next proposed
action is an owner-only correction of one existing secret, pending new approval.

## A. What was executed

The owner approved one diagnostic-only boot at source `725e823`. The exact instance
`i-0ca1671a59751acc5` launched at **2026-11-18 07:12:38 UTC**. This was 258 seconds
after the five-minute Console cutoff, still inside the watcher's ten-minute arm
interval. Before secret retrieval, the assistant refreshed the running instance,
disks/profile/IMDSv2/credits/ingress/shutdown controls, SSM document, workload IAM
and full accepted ECR identity/finding checks. Fresh read-only results explicitly
say running; the original stopped snapshots and late-start record are preserved.
The metadata refresh used a separately hashed, read-only running-state adaptation.
No approved deployment script or boot-relative shutdown deadline was changed.

The watcher remained healthy. The hash-verified temporary scripts and timer ran
through SSM. The retained receipt verifies the current boot, existing installation
and absolute guest shutdown timer at **07:47:38 UTC**. The diagnostic made one
GetSecretValue request with both the selected version ID and `AWSCURRENT`.

Normal stop was requested at **07:15:35 UTC**, immediately after the result.
The watcher observed stopped at **07:16:03 UTC**; independent metadata at
**07:17:17 UTC** confirmed stopped, no public IPv4 and both original encrypted
gp3 disks attached, with Kafka delete-on-termination false. SSM closed. There
was no retry, validation container, RPC, controller, producer/Kafka/Spark service,
S3 pipeline write or secret edit.

## B. What was learned

The exact ARN, name, version `12d38103-a68c-4226-b20a-3095cbb3de78` and current-stage
checks all passed. The field is SecretString, with no binary field. Safe codes:

- `JSON_OR_QUOTES`
- `SCHEME_NOT_WSS`
- `HOSTNAME_MISMATCH`
- `PATH_MISMATCH`

This proves the complete stored value for that current version is not the bare
endpoint required by the frozen runtime. It does not reveal the exact wrapper,
field name, embedded URL/key or the earlier Console/clipboard steps. A copied URL
can pass while the complete saved value fails. Do not claim the owner knowingly
copied a field or that the API key is bad. Alchemy authentication was not attempted.
The guest reported AWS CLI **2.38.0**, Python **3.9.25**. No IAM expansion, Docker
transport repair, key rotation or image rebuild is indicated by this result.

## C. Files and evidence

Added this report, [sanitized result/proposal](evidence/aws_runtime_version_contract_result_2026-11-18.json)
and current-status links in the pilot and diagnostic review. Private approvals,
late-start review, refreshed metadata, exact safe result, watcher and stop receipts
are preserved and hash-bound. No source, helper, frozen host file, image, Compose,
dependency lock, Terraform or README change is included in this result checkpoint.

## D. Tests and validation

**104 focused tests and 36 negative subtests pass again.** The prior complete
suite passed 588 runtime tests, 15 private operator cases and seven unsigned
loopback cases with the real local CLI. The executed inspector source and helpers
matched their reviewed hashes. Identity/version and specific-code reporting now
worked live without exposing the value. No live Kafka/Spark recovery was attempted.

Review secret scan, evidence/source hashes, frozen-file equivalence, document links
and whitespace; checkpoint on develop and observe the new automatic CI. These
checks do not authorize the proposed secret update or another paid boot.

## E. Security review and approval boundary

Only the exact approved workload role/secret/version were used. The full response
stayed in process memory; only fixed identity checks, known version, constant format
codes and sanitized tool versions reached the receipt. No value, value hash, raw
provider text, clipboard content, credential or key entered chat/Git. Core dumps
were disabled, swap was absent and SDK attempts were capped at one.

The early handoff cutoff and ten-minute watcher timeout are different controls;
the late start is disclosed rather than treated as on time. Fresh controls and
an unchanged, actual-boot-based stop window preceded the read. Before a future
boot, reconcile the advertised handoff deadline with arming behavior; the proposed
secret correction requires no boot and no change to that watcher.

## F. Git and CI

Executed source `725e823e6ae82f7d2fe7005ce47cbd0f80eeb89c` passed all six jobs in
[CI 44254298690](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/44254298690).
Checkpoint this documentation-only result on develop and report the new SHA/CI
in conversation. Main and unrelated untracked duplicates remain untouched.

## G. Cost implications

Launch to observed stop was **205.670452 seconds**, an upper bound from polling.
At the recorded USD 0.1714/hour compute/IPv4 rate, that interval is approximately
**USD 0.00979** before requests/log/transfer usage, retained resources and taxes.
This is an estimate, not an invoice or cap. No new resource was created. Existing
EBS/ECR/secret charges continue while stopped. The proposed owner correction adds
no EC2 runtime or new secret resource; normal existing-secret/API charges remain.

## H. Remaining acceptance

The stored configuration must satisfy the reviewed bare-endpoint contract before
another streaming proposal. The URL/key's actual provider authentication, chain ID,
first committed Bronze/checkpoint data, application logging, drain and live recovery
remain unverified. The one streaming-run image acceptance is still unused; this
completed diagnostic approval does not authorize another start or a value update.

## I. Concrete owner-only correction proposal — NOT APPROVED

Target only the existing secret:
`arn:aws:secretsmanager:us-west-2:209479274979:secret:blockchain-runtime/11-6c/alchemy-websocket-url-d44flr`.
Use the owner's existing Console access, not an expanded deployer/workload role.
Preserve the already valid Arbitrum URL and API key privately. Replace the complete
SecretString with that same bare URL; do not retain a JSON object, field name,
quotes or whitespace. The displayed shape is only a placeholder, not a value to
paste: `wss://arb-mainnet.g.alchemy.com/v2/<EXISTING_API_KEY>`.

After separate approval, the exact manual procedure is:

1. In Oregon, open the existing named secret and confirm its ARN ends `-d44flr`.
2. Choose Retrieve secret value, then **Edit**. In the edit dialog choose **Plaintext**.
3. Click inside the value editor, select its entire contents and replace them with
   the existing raw WSS URL privately. Do not use the Key/value editor or add braces,
   a key name or quotes. Do not send the URL, key, screenshot or clipboard to the assistant.
4. Click **Save once**. Preserve ARN/name, encryption key, tags, rotation, replicas
   and resource policy. Do not create another secret or regenerate the Alchemy key.
5. Refresh Versions. The new version labeled `AWSCURRENT` must have a different ID
   from `12d38103-a68c-4226-b20a-3095cbb3de78`. Report only that new ID, creation time
   and confirmation that the complete value was saved as one bare URL.
6. If the version ID is unchanged or saving fails, stop and report safe status;
   do not repeatedly save, move labels manually, edit permissions or start EC2.

Saving a changed secret creates a new version and ordinarily moves `AWSCURRENT`
to it, retaining the former current version as `AWSPREVIOUS`. These expected
automatic stage movements are included in the proposal; no manual stage operation
or old-version deletion is proposed. See the primary
[AWS PutSecretValue reference](https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_PutSecretValue.html).
The old diagnostic is pinned to the old version and must not be blindly rerun.
After save, prepare/validate a new exact version scope offline before requesting
any live read or paid streaming pilot. No implicit restart is included.

**Requested approval:** one owner-only value-format correction/save on this exact
existing secret, preserving its URL/key and settings, including the resulting new
version and automatic current/previous staging. No assistant secret access, IAM,
Terraform, EC2 start, new resource, key rotation or downstream change is included.
Rollback or old-version restoration requires its own exact approval; nothing is
automatically reverted. Keep EC2 stopped and preserve EBS/data/all prior receipts.

## J. Whether production infrastructure changed

Within the approved scope, the existing EC2 instance started/stopped once,
temporary operator files/timers were created and one exact secret version was read.
No new AWS resource, Terraform/IAM/secret-value change, EBS formatting/deletion,
Snowflake change, pipeline invocation or main merge occurred. Milestone 11.6C
remains open; 11.5D remains paused.
