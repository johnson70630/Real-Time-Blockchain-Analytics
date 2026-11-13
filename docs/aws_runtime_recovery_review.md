# AWS demo: state recovery and stopped-instance plan correction

**Completed follow-up:** the operator explicitly approved the revised-config
plan and renewed MFA privately. The saved plan completed with five creates,
23 no-ops and no updates/deletions/replacements. See the
[successful plan result](aws_runtime_recovery_plan_result.md). No apply or
EC2 start followed; the prior rejection is resolved for this planning scope.

Date: 2026-11-13 UTC. Milestone 11.6C. **Approved untaint completed. The
unchanged-config plan was blocked safely; the corrected-config plan awaits
fresh approval. EC2 remains stopped.** Milestone 11.5D stays paused.

## A. What was implemented

The operator approved clearing only the taint for
`aws_instance.runtime` / `i-0ca1671a59751acc5` and generating a saved recovery
plan. Exact identity, stopped instance/safety attributes, source, inputs,
backend and remote state hash were rechecked. Private state backups were made.

The native-locked untaint succeeded at 22:45:17 UTC. State advanced from serial
2 to 3. Semantic comparison confirmed that only the taint and serial changed;
Terraform also reordered equivalent cached check results. All resource
attributes, outputs and lineage were unchanged. No manual state editing was
used. The instance stayed stopped and the lock was released.

The approved plan with the unchanged configuration exited 1 at 22:46:48 UTC:
`prevent_destroy` blocked a proposed EC2 replacement triggered solely by
`associate_public_ip_address`. No apply ran.

## B. Why a local correction is needed

Stopping EC2 releases its automatically assigned public IPv4 address. Provider
6.69.1 reads association presence into the instance's optional/computed,
replacement-triggering public-IP field. An explicit `true` therefore conflicts
with the legitimate stopped-state observation of `false`.

The local correction removes that duplicate instance-level argument. The
existing public subnet still enforces `map_public_ip_on_launch=true`; AWS
network interfaces inherit that launch setting. `prevent_destroy` remains
enabled, and no `ignore_changes` is introduced. Later start acceptance must
verify the actual public address, outbound connectivity and SSM readiness.

Sources: [AWS stop/start address behavior](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/how-ec2-instance-stop-start-works.html),
[AWS subnet/ENI address inheritance](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-eni.html),
[locked provider schema/read behavior](https://github.com/hashicorp/terraform-provider-aws/blob/v6.69.1/internal/service/ec2/ec2_instance.go#L3163).

## C. Files changed and retained evidence

- `infra/terraform/environments/runtime/compute.tf`: remove the instance public
  IP pin, with a comment explaining stopped-state behavior; formatting updated.
- `infra/terraform/environments/runtime/OPERATIONS.md`: subnet inheritance and
  post-start address checks.
- `tests/test_terraform_runtime_foundation.py`: protect subnet launch intent,
  absence of the duplicate instance pin, no ignored attributes, and destruction
  protection; make one existing assertion independent of spacing.
- This review, readiness links and
  [sanitized recovery evidence](evidence/aws_runtime_recovery_2026-11-13.json).

Private backups, logs and the failed plan remain under
`/tmp/blockchain-runtime-recovery-proposal-nh1hf6gu`. Full state/plan output and
credentials remain outside Git. Neither the first partially applied plan nor
the failed replacement plan may be used as a deployment artifact.

## D. Tests and validation

The focused Python suites passed **37 tests**. Terraform format, isolated
backend-disabled validation and all **nine credential-free provider mocks**
passed. The sandbox initially prevented provider-plugin negotiation; the same
isolated validation/mocks succeeded outside it with AWS credentials/config
disabled and without a production backend. Secret/lint/JSON/link checks are
run before committing. Hosted CI is observed after push.

The failed authenticated plan demonstrated the replacement trigger and the
working destruction guard. The expected fixed result—five remaining creates
and no EC2 action—is **not yet verified by an authenticated plan**.

## E. Security and correctness

The existing 23 runtime resources and exact stopped instance are preserved.
No apply, start, replacement, deletion, formatting, secret access or IAM change
occurred during this recovery. The inactive guest-shutdown permission proposal
remains undeployed. No AWS write probe or broader permission grant was used.

Remote state after untaint remained unchanged through the failed plan. State
serial is 3; its SHA-256 is
`16b7a99fa7dccc4cd445d5816456982ac029309022451d5b2800ae9805997d13`.
The runtime lock is absent. Platform state was not accessed. No protection or
network ingress control was relaxed; the local correction changes planning
intent only until a separately approved apply.

## F. Git and CI

Work remains on develop; README/main and the 43 platform resources are
unchanged. The checkpoint SHA and hosted CI result are recorded in the
conversation. Runtime source plus lockfile SHA-256 for the revised plan is
`6dd1bb8ebb3de1d89600a4159701548af3c918a570f15629b05d264b9abe9712`.

## G. Cost implications

No new EC2 running time or additional runtime resource creation occurred.
The retained 130 GiB of encrypted gp3 remains approximately USD 10.40/month;
state requests/versions and other usage may add charges. The proposed plan
does not launch resources or stop retained-storage charges. No spending cap or
measured invoice is claimed.

## H. Automatic approval-review blocker

Automatic approval review rejected the attempted authenticated plan command
before it executed, stating that the modified configuration expanded beyond
the explicitly approved state-only untaint-and-plan scope and required fresh
approval. No alternate/indirect execution was attempted. Offline validation and
read-only instance/state checks continued; the revised plan has not run.

Request fresh approval for **only an authenticated saved recovery plan using
the reviewed public-IP configuration correction**. Expected scope is the five
remaining creates, no EC2 update/replacement/deletion, with the instance kept
stopped. Any different actions must be reviewed before proceeding.

Use profile `blockchain-demo-deploy`, default workspace and the existing private
`TF_DATA_DIR=/tmp/blockchain-runtime-boundary-plan-qo31l9uu/terraform-data`;
verify its S3 key is only `production/runtime/terraform.tfstate`, native locking
is enabled, and runtime state/source/input hashes and session lifetime match.
The proposed command is:

```sh
/tmp/terraform-1.17.0/terraform -chdir=infra/terraform/environments/runtime plan \
  -input=false -no-color -lock-timeout=30s -detailed-exitcode \
  -var-file=/tmp/blockchain-runtime-boundary-plan-qo31l9uu/runtime.tfvars.json \
  -out=/tmp/blockchain-runtime-recovery-proposal-nh1hf6gu/revised-config/runtime-recovery.tfplan
```

Capture full output privately and report only allowlisted actions/security
results and artifact hashes. Do not repeat untaint. No apply, start, IAM change,
replacement, deletion, formatting or application deployment is included.

## I. Next step

Obtain the specific changed-config plan approval, generate/review it, and only
then prepare any separate remaining-resource apply request. Preserve the
stopped instance and inactive permission proposal while planning is gated.

## J. Production infrastructure changes

The approved operation changed **runtime Terraform state metadata only**,
removing the taint and advancing serial 2 → 3. No EC2/EBS/IAM/network resource
was changed by this recovery. The original 23-resource partial runtime remains;
EC2 is stopped. Platform state/resources and Snowflake were untouched.
