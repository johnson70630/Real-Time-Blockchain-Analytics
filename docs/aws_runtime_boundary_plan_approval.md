# AWS demo: verified access and fresh runtime-plan approval

Date: 2026-11-13 UTC. Milestone 11.6C. **Read-only preflight complete;
fresh backend initialization and authenticated plan are proposed, not executed.**
No apply, EC2 launch/start, EBS initialization, image push, secret setup,
Snowflake operation or main merge is authorized by this report.

## A. What was completed

The operator reported creation of the four reviewed managed policies and a
matching `blockchain-runtime-deployer` role. The operator confirmed its trust
requires the exact planning user with MFA, maximum session is one hour, the
three deployment policies are attached, and its boundary is unset. The EC2
boundary policy remains reserved for the future runtime role.

The two local CLI profiles were added without changing existing config bytes
or profile values; config and private backup files are mode 0600. The AWS
credentials file was not modified. The operator authenticated interactively,
then both `blockchain-demo-assume` and `blockchain-demo-deploy` returned the
expected assumed-role identity in account `209479274979`. A separate Boto3
session through the process profile also verified the same role.

Live read-only checks found:

- EC2 boundary policy default version v1 matches the repository document.
- Runtime state and lock objects are absent; no state object bodies were read.
- No matching runtime VPC/subnet/gateway/routes/SG, EC2 instance, Kafka volume,
  runtime EC2 IAM role/profile, ECR repositories, log group, alarms, SNS topic
  or EventBridge rule were found by the scoped checks.
- The pinned public AMI remains `ami-0dac6cd09676c9fb1`; the SSM parameter is
  version 201. The reviewed image is available and Amazon-owned.
- `t3.xlarge` has four vCPUs/16,384 MiB and is offered in `us-west-2a`.
  Standard on-demand quota is 16 vCPUs. Inventory found zero active standard
  on-demand instances in the Region; this is an inventory estimate, not a
  capacity or launch guarantee.
- EBS account-wide default encryption is **off**; the default key is
  `alias/aws/ebs`. Terraform explicitly encrypts both proposed volumes. No
  account encryption setting was modified.
- Registry scanning is BASIC. No images were pushed or scanned by this check.

## B. Why a new plan is needed

The earlier 28-create saved plan predates the EC2 role boundary. It must not be
applied for the dedicated-deployer design. A fresh plan must use the approved
role/profile, exact runtime backend, empty secret list and boundary ARN, while
preserving the older plan artifacts and evidence.

Passing preflight does not authorize resource creation or prove every write API
will succeed. It supplies account-specific evidence before a plan approval.

## C. Files and exact proposed scope

- [Sanitized live preflight evidence](evidence/aws_runtime_preflight_2026-11-13.json).
- [Complete noncredential plan inputs](evidence/aws_runtime_boundary_plan_inputs_2026-11-13.json).
- This report and status cross-references in the pilot/IAM review.
- Private backend config, identical input copy and command proposal under
  `/tmp/blockchain-runtime-boundary-plan-qo31l9uu`, with files mode 0600 and
  directory mode 0700. No Terraform backend metadata exists there yet.

| Item | Proposed value |
|---|---|
| Terraform root | `infra/terraform/environments/runtime/` |
| Terraform / AWS provider | Existing verified Terraform 1.17.0; locked AWS 6.69.1 |
| Authentication profile | `blockchain-demo-deploy` via the MFA session |
| Backend bucket | `blockchain-analytics-terraform-state-209479274979-usw2` |
| Backend key | `production/runtime/terraform.tfstate` |
| Workspace / locking | `default` / native S3 `use_lockfile=true` |
| Region / AZ | `us-west-2` / `us-west-2a` |
| EC2 | One proposed `t3.xlarge`, standard credits, IMDSv2, no ingress |
| EBS | Encrypted 30-GiB root and encrypted persistent 100-GiB Kafka gp3 |
| Boundary | `arn:aws:iam::209479274979:policy/blockchain-runtime-ec2-boundary` |
| Secret ARNs | `[]`; no secret resources or secret-read statement |
| Pilot prefix | `blockchain-analytics/pilot/runtime/11-6c-v1` |
| Compose | v5.1.3; previously reviewed amd64 download/SHA-256 unchanged |
| Schedule | Disabled; no new AWS-side hard stop schedule |
| Expected plan | **28 managed creates, 0 updates, 0 deletes** |

The 28 expected resources remain the isolated network/egress rules, EC2/EBS
and attachment, runtime EC2 role/profile/policies, two ECR repositories with
lifecycle policies, log group, two alarms, SNS topic/policy and EventBridge
rule/target. The already-created administrator-managed deployment policies/role
are outside both Terraform states. No import or adoption is proposed.

Reviewed runtime config SHA-256 (root `.tf` files and templates):

```text
910d53461d49d46ac83dc03037a8e5827a9cf0610a0f1f65112407de770e60a6
```

## D. Validation

The live checks used read APIs only, without write probes, DryRun launch/stop,
Terraform commands or bucket object bodies. Account, assumed-role ARN and
session expiry were checked; credential-process output was consumed privately
by the SDK and no credentials were printed or committed.

The prior code checkpoint passed 597 Python contracts and all six CI jobs,
including nine credential-free Terraform mocks. This evidence/report change
does not modify Terraform, runtime application code or CI configuration.
Validation for this change checks JSON, local links, diff whitespace and the
repository secret scanner. Hosted CI is observed for the new checkpoint.

## E. Security and correctness limits

- The platform backend `production/platform/terraform.tfstate` was not accessed.
  Default workspace only; no state migration, workspace creation, force unlock,
  import, targeted apply, refresh-only apply or destroy is included.
- The boundary readback is independently verified. The deployer trust, attached
  policy names and maximum duration were Console-confirmed by the operator;
  full self-role/policy readback is not granted to the planning identity.
- Inventory absence is scoped to the reviewed names/tags and exact service
  identifiers at the evidence timestamp. It does not prove no unrelated
  resources exist or that names/capacity will remain available.
- Read calls do not prove effective RunInstances, PassRole, state writes or
  StopInstances authorization through every SCP/resource policy. Do not use
  resource creation to discover missing permissions. Before a paid apply,
  complete the administrator's effective-permission/stop-path review.
- The refreshed plan must show the exact EC2 boundary, encrypted disks, standard
  credits, IMDSv2, empty ingress, no secret access and the pinned image. Stop
  if its managed actions differ from the 28-create expectation; retain private
  evidence and do not apply or delete anything to repair the scope automatically.
- Full plan text/JSON and any backend metadata stay in the private work area.
  Only an allowlisted action/security summary and hashes may be reported.
- The initial deployer still lacks image push, Secrets Manager and SSM execution
  permissions. Those remain later, separately reviewed gates. Neither this
  preflight nor a successful future plan authorizes EBS formatting.

## F. Git and CI

Working branch is develop. README/main, production Terraform root, both remote
states, and the 43 platform resources remain unchanged by this review. The
checkpoint SHA and hosted CI result are recorded in the conversation.
Milestone 11.5D stays paused.

## G. Cost implications

Read-only preflight created no runtime resources. A future approved init/plan
may make native state-lock S3 requests and retain lock-object versions in the
existing bucket, with small request/storage charges. It will not create EC2,
EBS, ECR, network or monitoring resources. Runtime cost/duration remains the
[dated November 12 model](aws_demo_launch_approval.md#g-verified-cost-model),
initially around 40 hours/month; it is neither a spending limit nor a bill
guarantee. A paid launch needs its own current cost/scope approval.

## H. Required approval

Approve **only** fresh initialization of the exact runtime backend in a new
private data directory and an authenticated, saved Terraform plan with the
inputs above, using native locking. This includes reading the runtime state
and acquiring/releasing its lock, never the platform state. It does not include
apply/import, cloud resource creation/start/modification, EBS initialization,
secrets, deployment, Snowflake operations, monitoring automation or main merge.

Before execution, recheck config/input hashes, clean develop, exact role/account,
backend identity and session lifetime. If authentication expires, the operator
renews the same one-hour session privately; no longer session, new permissions
or spending approval is implied.

## I. Concrete commands proposed, not executed

Environment: remove inherited AWS credential/config/endpoint overrides and
Terraform variable/argument/logging overrides; set `AWS_PROFILE=blockchain-demo-deploy`,
Region `us-west-2`, `TF_WORKSPACE=default`, and
`TF_DATA_DIR=/tmp/blockchain-runtime-boundary-plan-qo31l9uu/terraform-data`.
Use the verified local binary/provider cache and capture all output privately:

```sh
/tmp/terraform-1.17.0/terraform -chdir=infra/terraform/environments/runtime init \
  -input=false -no-color -lockfile=readonly \
  -backend-config=/tmp/blockchain-runtime-boundary-plan-qo31l9uu/backend.hcl \
  -plugin-dir=/tmp/blockchain-pilot-tf-_uol3u43/.terraform/providers

/tmp/terraform-1.17.0/terraform -chdir=infra/terraform/environments/runtime plan \
  -input=false -no-color -lock-timeout=30s -detailed-exitcode \
  -var-file=/tmp/blockchain-runtime-boundary-plan-qo31l9uu/runtime.tfvars.json \
  -out=/tmp/blockchain-runtime-boundary-plan-qo31l9uu/runtime-boundary.tfplan
```

After an approved execution, inspect the saved plan privately, verify action and
security scope, and check the runtime lock was released without changing or
reading platform state. Do not run any `apply` command. A failed or unexpected
plan is retained for review; it must not become a deployment artifact.

## J. Production infrastructure changes

None in this preflight. The operator's prior IAM/MFA setup is in use; the role
session and SDK handoff work. No new backend init or authenticated plan was run,
no lock was written, and no paid runtime was created. The next boundary is the
specific init/plan approval above, followed later by a separate deployment gate.
