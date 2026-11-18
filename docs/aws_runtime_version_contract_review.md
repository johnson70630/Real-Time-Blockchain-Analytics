# Milestone 11.6C — Version-pinned diagnostic prepared offline

Current result: [exact current version has a stored-value format mismatch; diagnostic stopped and owner-only correction proposed](aws_runtime_version_contract_result.md). EC2 is stopped. No new start or secret edit is approved; earlier statements below preserve checkpoint history.

**The owner supplied current-version metadata. EC2 is stopped.** A new diagnostic
is implemented and tested offline; no live secret read, edit or paid start occurred.
This is a diagnostic-only proposal. It cannot authorize or start streaming.

## A. What was implemented

Recorded owner-supplied version ID `12d38103-a68c-4226-b20a-3095cbb3de78`, supplied
in response to the `AWSCURRENT` request, and the displayed creation time
`November 17, 2026 at 03:02:41`. The timezone was not provided, so no UTC conversion
or temporal comparison is claimed. The owner did not separately confirm that it
is the version previously saved; current-stage association remains an attestation.
The old host probe discarded version metadata and cannot be compared retrospectively.

Added a [version-pinned inspector](../scripts/check_runtime_secret_contract.py)
and [40 regression tests](../tests/test_runtime_secret_contract.py). It uses one
full GetSecretValue response, requests both this version ID and `AWSCURRENT`, and
checks the exact ARN/name/version/stage, string/binary fields and endpoint format.
It reuses the existing owner/chain validators by exact source hashes. Those two
helpers, the application, release and nine frozen host files are unchanged.

Prepared a private temporary command template and SSM dispatcher. The dispatcher
requires `purpose=secret-contract-only`, unchanged source/private hashes, an active
watcher and an unused one-attempt intent. It allows only upload, inspect, diagnose
and exit. It creates no application preflight gate or controller intent. All
operator artifacts, their test source and sanitized evidence are hash-bound in
[the review evidence](evidence/aws_runtime_version_contract_review_2026-11-18.json).

## B. Why

The owner's local FORMAT_PASS and the live host rejection remain inconsistent.
The generic prior diagnostic cannot distinguish wrong/missing fields, a wrapper,
whitespace or another URL rule. Repeating that probe would add little evidence.
This inspector checks the exact selected version and gives constant, specific
format codes without disclosing the value, a value hash or arbitrary provider text.

When VersionId and VersionStage are both supplied, AWS requires them to refer to
the same version. This is a read constraint, not a label change. SecretString and
SecretBinary are separate fields. See the primary
[AWS GetSecretValue API reference](https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html).
A CONTRACT_PASS proves identity and format only; it does not prove Alchemy access,
chain ID, successful ingestion or data completeness.

## C. Files and exact proposed resources

Changed/new tracked files: inspector, tests, this report/evidence and current-status
links in the pilot and prior result. No README, Docker image, Compose, dependency,
frozen host file, Terraform or application-runtime change is included.

| Existing resource | Proposed diagnostic-only scope |
| --- | --- |
| Account/region | `209479274979`, `us-west-2` (Oregon) |
| EC2 | One manual Console start and normal stop of `i-0ca1671a59751acc5`, t3.xlarge, standard credits |
| Root/Kafka EBS | Preserve `vol-03bd5d26947719f98` / `vol-02c9fb14a9c568fb5` |
| Kafka filesystem | Preserve `/var/lib/blockchain-runtime/kafka`, XFS UUID `da1454d8-1bea-41a3-89d7-87bd570a47bd`; no initialization/reset |
| SSM | Existing private `blockchain-runtime-demo-shell`; no SSH/ingress change |
| Guest files | Three hash-verified scripts under a fresh `/run/blockchain-runtime/secret-contract-v1`; nonpersistent timer/intent/control receipts only |
| Secrets Manager | One GetSecretValue for the exact ARN ending `-d44flr`, selected VersionId and `AWSCURRENT`; no edit/stage movement |
| IAM | Existing workload role `blockchain-runtime-ec2` and its exact-secret GetSecretValue grant; no expansion |

**New AWS resources: zero. Terraform backend/plan/apply/import scope: none.**
No new credentials, ECR push/build/deployment, container execution, Alchemy RPC,
Kafka/Spark/producer invocation, application log stream or S3 write is proposed.
Existing image digests remain recorded in the [unchanged release candidate](evidence/aws_runtime_release_candidate_2026-11-17.json);
none is executed by this diagnostic. Existing Bronze/checkpoint prefixes under
`blockchain-analytics/pilot/runtime/11-6c-v1/` remain untouched. Snowflake, dbt and
Streamlit are outside scope. Milestone 11.5D remains paused.

## D. Tests and validation

**588 runtime tests and 36 negative subtests pass**, including 40 new inspector
tests. Ruff passes; one existing websockets deprecation warning remains.
**Seven unsigned loopback cases with the real local AWS CLI** verify pinned request
parameters and full-response parsing with dummy data. They cover matching values,
wrappers, wrong versions/stages/ARNs and binary fields. No credential or production
request was used. They do not identify the live cause or reproduce the guest CLI.

**15 private operator contract cases pass**, including success/failure/timeout,
retained timer proof, source/hash guards and rejection of a streaming-purpose
approval. These tests mock the guest interface and root-owned temporary directory;
no live command executed. Static compilation and terminal line bounds pass.
Review secret scanning, hashes, links, whitespace and frozen-file equivalence;
checkpoint on develop and observe its CI.

## E. Security and correctness review

Import and pure assessment are inert. Execution needs an explicit flag, root on
the exact instance, the exact original release path/hash and no swap. Source
helpers are verified before the read. The CLI uses the workload role, clears
inherited credentials/config, disables core dumps, bounds timeouts and sets one
SDK attempt. There is no automatic retry, normalization, key rotation or fallback
to another version. API failure codes are allowlisted; bodies remain suppressed.

The full response exists only in process memory. Output contains the known
expected version, match booleans, fixed format codes and sanitized CLI/Python
versions. Unexpected ARN/name/version/stage text, values and value hashes are
never echoed or written to a response file. SSM transcripts remain suppressed.
A successful diagnostic still stops; it never creates permission to ingest.

## F. Git and CI

Work began on develop at `ebbab76`, whose six jobs passed in
[CI 44247393021](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/44247393021).
Create the validated checkpoint and record its SHA/new CI in conversation. Keep
main and the unrelated untracked duplicate files unchanged.

## G. Cost and duration

Preparation caused no compute start or secret read. A live check is expected to
take roughly 2–5 minutes, then normally stop immediately. At the previously
recorded USD 0.1714/hour compute/IPv4 rate, five minutes is approximately USD 0.0143;
15 minutes is USD 0.04285 and the full 45-minute supervision interval USD 0.12855.
These are estimates, not spending caps or invoices; retained EBS/ECR/secret charges,
API requests, transfer/log usage and taxes are additional.

## H. Admission and stop controls

Before any start, recheck expected identity, at least 90 minutes of session validity,
exact stopped instance/disks, no ingress, standard credits, stop authorization,
SSM document and unchanged workload policy/boundary. The preserved cloud-readiness
adapter also checks accepted ECR identities/findings without executing images.
Use the detached watcher, a fresh heartbeat and owner Console normal-stop fallback.

Bind every deadline to the actual new boot. Request stop immediately after the
single diagnostic, regardless of result. The existing tested watcher requests
normal stop at 15 minutes because this scope never confirms an application start.
The independently verified guest timer powers off at 35 minutes if needed; target
verified stopped by 40, supervision ends at 45. The unchanged 150-minute timer
is only a secondary fallback required by frozen host preflight. No timer extension
or retry is included. The 30-minute application stop path is never reached in a
healthy diagnostic-only watch because no application intent can be created.

## I. Shutdown, rollback, cleanup and approval

After the safe result or any failure, request normal stop, close SSM and independently
confirm stopped, IPv4 released and original encrypted disks attached. Preserve all
existing state/data and receipts. Temporary files disappear on OS shutdown; no
application rollback is needed. EBS was initialized previously and must not be
formatted again. Stop does not remove retained-resource charges. No force stop,
termination, deletion, secret rollback, label movement or Terraform replay is included.

**Requested approval:** one new supervised diagnostic-only boot of the exact
instance, temporary shutdown timer/files, scoped SSM inspection, one version-pinned
secret read, immediate normal stop and verification, within the 45-minute supervision
scope above. No streaming is included even if the result passes. Any eventual
secret correction or streaming run needs its own concrete review and approval.

## J. Whether production infrastructure changed

No. This preparation used offline tests and read-only stopped-instance metadata.
EC2 remains stopped. No new watcher, secret read/edit, temporary guest file/timer,
Terraform/IAM/storage change, container or pipeline invocation occurred. The live
format mismatch is still unresolved; milestone 11.6C remains open.
