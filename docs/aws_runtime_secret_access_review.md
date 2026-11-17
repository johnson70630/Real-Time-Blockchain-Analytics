# Milestone 11.6C — Exact Alchemy secret access review

Current status: [boundary v2 verified and plan-only approval scope](aws_runtime_secret_boundary_result.md). The workload inline policy is unchanged. Production Terraform initialization, plan/apply and EC2 startup remain unapproved.

The earlier review statements below are preserved as checkpoint history.

**Offline proposal complete; IAM modification and production Terraform actions
are not approved.** Milestone 11.5D remains paused. The prior conditional security
exception for the three frozen `ddc1861` images remains unchanged.

## A. What was implemented

Recorded the owner's ARN for the separately approved Console-created secret.
The owner confirmed AWS-managed `aws/secretsmanager` encryption, rotation off,
no replicas and no resource policy. The assistant received no secret value.

Prepared an exact replacement boundary document and a full comparison artifact
for the future Terraform-managed inline policy. Added a fail-closed Terraform
receipt guard and credential-free adversarial tests. Defaults remain no-secret.
No live boundary, role policy, trust, attachment or permission was changed.

## B. Why

The live EC2 boundary explicitly denies all Secrets Manager actions. An added
role Allow alone cannot override that Deny. Both the boundary ceiling and the
identity policy must permit this exact read. A boundary itself grants nothing;
see [AWS boundary evaluation](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_boundaries.html).
The new ceiling allows only GetSecretValue for one ARN and denies all other
Secrets Manager resources and administration. IAM and role delegation remain
denied, and all existing nonsecret statements are preserved.

## C. Files changed and exact scope

- [Proposed boundary](../infra/terraform/environments/runtime/policies/runtime-secret-boundary.proposed.json).
- [Proposed role-policy comparison](policies/runtime-deployer/runtime-role-secret-read.proposed.json).
- Runtime `iam.tf`, `variables.tf`, `OPERATIONS.md` and new mock tests.
- `tests/test_runtime_deployer_policy.py`: negative action/resource matching and
  28 new policy cases, including explicit-Deny dominance over overbroad grants.
- This report, [sanitized evidence](evidence/aws_runtime_secret_access_review_2026-11-17.json),
  and current-status links in the acceptance and pilot reports.

Boundary ARN: `arn:aws:iam::209479274979:policy/blockchain-runtime-ec2-boundary`.
Workload role: `blockchain-runtime-ec2`.
Existing inline policy: `blockchain-runtime-least-privilege`.
The proposed JSON contains only the owner's exact secret ARN; no wildcard secret
name or additional read/write secret capability is proposed.

Canonical candidate boundary SHA-256:
`1ae18de848a97a218592fda0f1fb392002cd3ede44beb04e4ce0d3eedccb7e2a`.
This hashes parsed JSON with sorted keys and compact separators. Terraform uses
`sha256(jsonencode(jsondecode(file(...))))`; tests verify the same digest.

## D. Tests and validation

**482 focused Python contracts pass**, including all 124 deployer-policy cases.
Terraform 1.17.0 validation and **16 mock plans pass**, including seven new
receipt/secret-scope cases. Correct ARN/hash permits the proposed configuration;
missing/wrong hash, another/additional secret, empty secret input with a receipt,
or a receipt without the selected boundary fail closed. Existing foundation and
no-secret behavior pass unchanged. These tests are not an AWS IAM simulator.

All Terraform work used a temporary copy with `backend.tf` excluded, credentials
disabled and mocked providers. A clean `init -backend=false -lockfile=readonly`
verified the signed public AWS 6.69.1 provider against the unchanged lock file.
An older cached package failed checksum validation and was not trusted. An
initial assertion type mismatch also exposed a Terraform diagnostic-formatting
panic; the assertion was corrected, artifacts preserved, and all final checks pass.
No production backend, plan, state or credentials were used by these mock plans.
Run Ruff, secret scanning and whitespace checks before checkpointing and observe CI.

## E. Security and live read-only findings

The expected MFA-protected operator role was verified. IAM readback confirms:

- Boundary default **v1** matches the preserved `runtime-boundary.json` exactly.
- The reviewed workload role carries that boundary and has no secret-read grant.
- Boundary usage is **one**, with **zero** ordinary policy attachments.
- EC2 `i-0ca1671a59751acc5` was **stopped** at 2026-11-17 03:10:50 UTC.

DescribeSecret was denied to the existing deployer. Metadata is owner-attested,
not independently verified; no operator permission was expanded to bypass this.
No GetSecretValue call occurred. ARN metadata is not the credential value.

The candidate retains state-bucket denial, pilot-only S3, ECR pulls, logging,
SSM limits, and IAM/AssumeRole denial. Only regional GetSecretValue on the exact
ARN is allowed. `Deny` with `NotResource` blocks other secrets; a resource-scoped
`Deny` with `NotAction` blocks administration of this secret. Neither negative
element appears in an Allow. See AWS guidance for
[NotResource](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_notresource.html)
and [NotAction](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_notaction.html).
The full comparison role policy preserves the live nonsecret statements and adds
only the matching regional GetSecretValue statement. No custom-KMS grant is added.

All containers still share the EC2 role trust boundary. After future permissions
are active, any compromised container able to obtain that role's credentials
could request this secret; a producer-only file mount does not provide separate
IAM isolation. The prior risk acceptance and hardening controls remain required.

## F. Git and CI

Checkpoint reviewed source/policies/evidence on develop after local validation.
Observe all six CI jobs and report the resulting SHA/status in the conversation.
Application images stay frozen at `ddc1861`; no rebuild or publication occurs.
README and main remain unchanged.

## G. Cost implications

This review creates no billable resource or runtime hours. The owner-created
secret is the only resource reported created during this step; its previously
approved estimate is about USD 0.40/month plus API usage. IAM changes proposed
here add no EC2 runtime or secret replica/rotation/custom-key component. Existing
storage/monitoring charges continue. No deletion or automatic cleanup is proposed.

## H. Approval boundaries and remaining blockers

1. Obtain explicit administrator approval to update only the existing boundary's
   default document to the candidate. Use an existing authorized Console identity;
   do not expand the human deployer. Preserve v1 and the same boundary attachment.
   If policy version capacity is exhausted, stop; deleting old versions needs its
   own review. Recheck usage and exact old document immediately before changing it.
2. Read back the new live default document and require its canonical hash to
   match the candidate. A supplied receipt is an operator attestation, not an
   automatic IAM check; calculating the local hash alone is insufficient.
3. Separately approve a fresh authenticated runtime-only Terraform plan. Supply
   this exact secret ARN and the verified receipt while preserving existing
   reviewed runtime inputs. Expected managed-resource scope: **0 create / 1 update
   / 0 destroy**, only `aws_iam_role_policy.runtime`. Data-policy recalculation may
   appear. Stop for any other delta, instance replacement, AMI/boot/storage change,
   or platform-state access. No authenticated plan is performed here.
4. Review the actual saved plan before separate apply approval. Never manually
   paste the comparison JSON over the Terraform-managed inline policy.
5. EC2 start, deployment, mount/S3 smoke and recovery still need their own bounded
   approval. Secret creation or risk acceptance did not grant those permissions.

Runtime state remains `production/runtime/terraform.tfstate`; platform state
`production/platform/terraform.tfstate` is excluded. No init/import/apply against
either production state is authorized by this offline code checkpoint.

## I. Next step and rollback

Request approval for **only the boundary default-document update** described
above. Provide the exact JSON and Console steps after approval. Leave the
existing workload inline policy and all human-user/deployer permissions unchanged
at this stage; effective secret read is still absent from the identity policy.
After owner update, verify live readback and prepare the separate plan request.

Rollback of a separately approved boundary change is to select the preserved
previous default version after reviewing any intervening changes. Keep EC2
stopped/application undeployed. Do not detach/remove the boundary, delete policy
versions, delete the secret, reformat EBS or reuse an earlier saved plan.

## J. Whether production infrastructure changed

The **owner reports creation of the previously approved secret** and its metadata
settings. Assistant actions were read-only IAM/EC2/DescribeSecret calls and offline
implementation. No IAM mutation, secret-value read, production Terraform action,
EC2 start, application/S3 deployment, Snowflake change, deletion or main merge
occurred. Existing 43 platform resources and initialized Kafka EBS are preserved.
Milestone 11.5D remains paused.
