# Milestone 11.6C — Boundary v2 verified; runtime plan requested

**The approved administrator boundary update is verified. Production Terraform
initialization/plan/apply and EC2 startup remain unapproved.** 11.5D stays paused.

## A. What was implemented

After explicit approval, the owner updated the existing
`blockchain-runtime-ec2-boundary` managed policy in the Console. Read-only
verification at **2026-11-17 20:41:24 UTC** confirms default **v2** exactly matches
the reviewed candidate, and the previous **v1** document remains preserved.
The same workload role is its sole boundary user; ordinary policy attachments
remain zero. The workload inline policy is unchanged and still grants no secret
read. EC2 `i-0ca1671a59751acc5` remained stopped at verification.

Prepared a private, unexecuted runtime-plan package from a tracked Git archive,
with reviewed inputs, native-lock backend settings and the verified boundary
receipt. No production backend or Terraform plan was initialized or executed.

## B. Why

The boundary is a permission ceiling. The existing identity policy still needs
the matching exact-secret GetSecretValue grant. That update belongs to the
Terraform-managed inline policy, so a new saved plan must be reviewed before
apply. Manual role-policy replacement would create avoidable configuration drift.

## C. Files and frozen scope

- This result and [sanitized evidence](evidence/aws_runtime_secret_boundary_result_2026-11-17.json).
- Current-status links in the pilot and prior secret-access review.
- Private runtime archive, input/backend files and plan-approval proposal retained
  outside Git. No credential values are present; secret ARN metadata is marked
  sensitive by the existing Terraform variable.

Plan source: `4ab0f2c948e170549692bd36f0476fe606f51929`, runtime root only.
Boundary canonical SHA-256:
`1ae18de848a97a218592fda0f1fb392002cd3ede44beb04e4ce0d3eedccb7e2a`.
The prior [exact-secret access review](aws_runtime_secret_access_review.md)
contains the policy comparison and guard design. Neither the original v1 policy
artifact nor the approved candidate is rewritten after the Console change.

## D. Tests and validation

Verified the candidate against its committed file and approved hash, compared
the live default document semantically and by canonical hash, checked retained
v1, boundary usage, unchanged role attachment/inline policy and stopped EC2.
No GetSecretValue call occurred. Secret metadata remains owner-attested because
the deployer lacks DescribeSecret; its permissions were not expanded.

The frozen source passed **482 focused Python contracts**, **16 Terraform mock
plans** and [all six CI jobs](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/44008796513).
For this documentation checkpoint, verify receipt/archive/input/backend bindings,
secret safety and whitespace. No redundant application test run is required.
Observe the new checkpoint's automatic CI and record its result in conversation.

## E. Security and isolated execution design

The Mac workspace has unrelated untracked duplicate files, including Terraform
files. They are preserved and never staged or interpreted by this proposal.
The plan source is an isolated **tracked Git archive**, not a workspace copy.
Only the runtime root is extracted; platform Terraform is absent.

The future operator environment must use the expected MFA-protected role,
account `209479274979`, Region `us-west-2`, and workspace `default`. Reverify the
live boundary, unchanged inline policy, stopped instance, input hashes and source
before any approved initialization. Reject inherited endpoint overrides, alternate
workspaces and Terraform CLI argument injections. Use the locked provider and
private files/logs. Never display raw state, plan JSON, credentials or secret values.

The prepared inputs preserve the committed historical reviewed baseline, adding
only the exact owner-created secret ARN and verified v2 receipt. Historical
inputs are not proof of current state equivalence. A normal refreshed plan must
detect unexpected differences and stop; targeting, refresh suppression and
ignore_changes are not permitted workarounds.

## F. Git and CI

Checkpoint this documentation/evidence on develop after review. Report the
checkpoint and its automatic CI result in conversation. The runnable plan module
remains frozen at the tested `4ab0f2c` archive; documentation commits do not alter
its bytes. Existing ECR images remain the accepted exact `ddc1861` manifests.
README/main and all unrelated untracked files are preserved. Tracked changes
can be clean while the workspace still has those untracked duplicates.

## G. Cost implications

The boundary update creates no new billable runtime service. The separately
approved secret and existing storage/monitoring can continue charging. The next
proposed operation starts no EC2 instance or application and provisions no
resource. Normal AWS API and S3 state/lock request charges may still apply.
Neither a plan nor the earlier one-hour exception is a guaranteed billing cap.

## H. Exact next approval and expected plan

Request explicit approval for **only runtime-backend initialization and one
authenticated saved Terraform plan**, including normal native lock handling:

| Item | Exact proposed scope |
| --- | --- |
| State bucket | `blockchain-analytics-terraform-state-209479274979-usw2` |
| State key | `production/runtime/terraform.tfstate` |
| Plan-owned lock | `production/runtime/terraform.tfstate.tflock` |
| Workspace | `default` |
| Expected managed actions | **0 create / 1 update / 0 destroy** |
| Only expected update | `aws_iam_role_policy.runtime` |
| Existing role/policy | `blockchain-runtime-ec2` / `blockchain-runtime-least-privilege` |
| Intended difference | Regional GetSecretValue on the one reviewed Alchemy ARN |

Initialization is in the isolated directory, with no migration. The backend may
read only the runtime state and create/delete only its own native lock during
normal locking. Platform state `production/platform/terraform.tfstate` is excluded.
Do not delete a conflicting lock or force-unlock. Do not reuse an earlier plan.

Stop for any other resource update, create, destroy, replacement, AMI/user-data/
network/storage/profile change, unexpected state address, boundary/input mismatch
or plan failure. Sensitive diagnostics remain private and are summarized safely.
No apply, import, destroy, state manipulation, IAM mutation during planning,
EC2 start, secret-value access, application/S3 data write, Snowflake operation,
cloud cleanup, main merge or 11.5D work is included.

## I. Next step and rollback

Obtain the exact plan-only approval above. Then inspect the saved plan privately
and report its sanitized actions and policy delta. Request apply approval only
after that concrete result passes review. EC2 start/deployment remain later gates.

If the plan differs or fails, leave the instance stopped and preserve artifacts;
do not apply or bypass safeguards. Boundary rollback, if separately needed,
selects the preserved v1 only after reviewing intervening changes. Do not detach
the boundary, delete policy versions, reformat EBS or discard checkpoints.

## J. Whether production infrastructure changed

**Yes: the owner performed the approved boundary default-version update to v2.**
The assistant verified it using read-only AWS calls. No workload grant, Terraform
backend/state/plan/apply, EC2 start, secret-value access, application deployment,
Snowflake change or deletion occurred. The existing platform resources and both
Terraform states are preserved. Milestone 11.5D remains paused.
