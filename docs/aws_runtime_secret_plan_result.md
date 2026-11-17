# Milestone 11.6C — Saved secret-access plan; observations need acceptance

**One runtime-only plan is saved. The strict drift gate stopped as designed.
No apply is approved or performed.** Milestone 11.5D remains paused.

## A. What was implemented

Under explicit owner approval, initialized only the isolated runtime S3 backend
and ran one normal, refreshed Terraform plan. Native locking operated normally.
The plan exited 2 and was saved privately. Its actual managed-resource actions
are **0 create / 1 update / 0 destroy**, solely `aws_iam_role_policy.runtime`.

The gate stopped on four refresh observations. No automatic retry, apply,
configuration workaround, state persistence or manual unlock followed. The
unchanged saved plan received a supplemental offline review under an explicitly
proposed observation-acceptance scope; the original stop remains preserved.

## B. Why

The intended change is one regional GetSecretValue grant on the exact reviewed
Alchemy secret. Both the default boundary ceiling and identity policy must allow
it. Boundary v2 is already verified; the identity policy still has no such grant.
An actual saved plan makes the resource mutation reviewable before approval.

Refresh observations must be distinguished from planned AWS mutations. The strict
gate catches both; it does not silently accept extra state observations because
the intended policy delta happens to be correct. The owner must decide whether
to accept the exact observations below together with the one saved-plan apply.

## C. Files and immutable artifacts

- This report and [sanitized plan evidence](evidence/aws_runtime_secret_plan_result_2026-11-17.json).
- Status links in the pilot and preceding boundary report.
- Private saved binary plan, raw JSON, logs, readbacks, operator tests and exact
  unapproved apply proposal retained outside Git with restrictive permissions.

Saved plan SHA-256:
`dc5f4ec7d9d3f425cb0308729850cbbd8ef693214f710d910c399e21f4717c7a`.
Plan source remains the frozen runtime tree at `4ab0f2c`; approved inputs,
backend settings and source archive hashes are recorded in the evidence.
The source came only from tracked Git content. The 67 unrelated untracked
duplicate files remain untouched and excluded from source, staging and execution.

## D. Tests and validation

The private plan reviewer passed **15 offline adversarial checks**, including
extra permissions, replacement, unknown policy values, unexpected state and
environment injection. The observation assessor passed **13 checks**, rejecting
nonempty monitoring changes, other volumes, encryption/deletion changes, root-disk
changes, wildcard grants, incomplete plans and additional observations.

The actual plan is complete/applyable, all its Terraform checks passed, and all
policy values are known. Semantic comparison verifies the exact new statement,
preserved nonsecret statements and unchanged role/policy identity fields. There
are no output changes. These results do not erase the strict drift gate failure
or authorize apply. No test invokes GetSecretValue.

The frozen implementation already passed 482 focused contracts, 16 mock plans
and all six jobs of [CI 44172838539](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/44172838539).
Validate the new evidence bindings, secret safety and whitespace before this
documentation checkpoint and observe its automatic CI.

## E. Security and exact refresh observations

| Refreshed resource | Exact observation | Planned resource action |
| --- | --- | --- |
| EventBridge runtime-state rule | `tags`: null to empty map; no nonempty tags changed | No-op |
| High-CPU alarm | `tags`, `ok_actions`, `insufficient_data_actions`: null to empty collections | No-op |
| Instance-status alarm | Same null-to-empty fields | No-op |
| Runtime EC2 instance | EBS block list now reports the existing separately managed Kafka attachment | No-op |

The instance was stopped both before and after refresh. Direct read-only EC2
verification identifies the existing `vol-02c9fb14a9c568fb5`: encrypted 100 GiB gp3,
attached to the reviewed instance as `/dev/sdf`, delete-on-termination false.
The observed block matches both existing Terraform volume and attachment IDs.
Root disk fields are unchanged. No disk is added, detached, formatted or deleted
by the saved plan. No alarm action or tag is added or removed by its apply graph.

The supplemental assessor accepts no other observation: it checks exact addresses,
changed fields, null-to-empty directions and existing volume properties before
checking the remaining policy/action graph. It analyzes a memory copy; the
binary plan, raw JSON, original stop receipt and configuration stay unchanged.
No -target, -refresh=false, ignore_changes or plan editing is used.

Postchecks confirm runtime state metadata/version unchanged, native lock absent,
EC2 stopped and the workload inline policy unchanged. Raw state/plan content and
secret values were not printed. This is a one-time review of the approved runtime
plan, not resumption of paused drift-monitoring automation.

## F. Git and CI

Checkpoint only these reviewed documentation/evidence changes on develop, leaving
untracked duplicates alone. Observe and report all six CI jobs. Runnable source
and saved plan remain frozen; later documentation commits change neither.
README/main and accepted ECR image identities remain unchanged.

## G. Cost implications

No EC2 runtime or new service was provisioned. The plan used normal S3 state/lock
and AWS read requests; retained storage/monitoring and the approved secret continue
their existing charges. The proposed IAM update adds no new billable runtime
component. State version storage and normal API requests may still incur charges.
No automatic cloud cleanup or spending-cap guarantee is introduced.

## H. Exact next approval and remaining gates

Request fresh explicit approval to **accept only these four classified refresh
observations for state persistence and apply exactly the saved plan above**.
The only managed-resource mutation is the existing workload inline-policy update.
Normal Terraform persistence is restricted to `production/runtime/terraform.tfstate`
and ownership-safe locking at its `.tflock`. Platform state is excluded.

Before any approved apply, reverify artifact hashes, source/inputs/backend,
expected MFA role/account/Region, boundary v2, exact role-policy before document,
expected policy attachments, stopped EC2 and intact Kafka volume. State metadata
must still match the pre-plan version; reject a stale plan. Reinspect the binary
plan with the same resource/policy and exact-observation criteria.

One apply command maximum; stop on failure or ambiguous result without an
automatic retry or force-unlock. No EC2 start, streaming deployment, secret-value
read, secret mutation, boundary/trust/attachment change, operator IAM expansion,
resource create/destroy, EBS operation, Snowflake action or main merge is included.

## I. Next step and rollback

Obtain the observation-and-saved-plan apply approval above. After any separately
approved apply, verify only the intended policy and runtime state changed and
EC2 remains stopped. Application startup is still a later paid-runtime gate.

If the owner declines, keep the saved artifacts and leave the grant unapplied.
If prechecks differ, do not use the saved plan. Do not delete checkpoints or
restore raw state manually. Any IAM rollback requires a reviewed policy change;
preserve boundary v1/v2 and the previous identity policy as evidence.

## J. Whether production infrastructure changed

**Only approved temporary native-lock operations occurred during planning.**
The runtime state object was unchanged at postcheck. No IAM resource update,
EC2 start, application/S3 data write, secret-value access, disk change,
Snowflake action, deletion, platform-state access or main merge occurred.
The earlier owner-approved boundary v2 stays in place. 11.5D remains paused.
