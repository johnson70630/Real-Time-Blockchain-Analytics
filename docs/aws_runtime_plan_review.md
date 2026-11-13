# AWS demo infrastructure-only plan review

Date: 2026-11-12. **Status: authenticated plan complete; security review passed;
nothing deployed.** This remains a scheduled portfolio demo, initially around
40 runtime hours per month. Milestone 11.5D stays paused. README and main are
unchanged.

**Historical plan snapshot:** the subsequent
[dedicated deployer design](aws_demo_deployer_setup.md) adds an optional
administrator-owned EC2 role boundary. The saved plan reviewed here omits that
boundary and must not be applied for the new design. Preserve this evidence;
obtain a fresh scoped plan with the approved boundary input before deployment.

## A. What was implemented

The runtime supports infrastructure-only planning with
`approved_secret_arns=[]`; the entire Secrets Manager read statement is omitted.
No secret resource is defined. Later exact-ARN opt-in rejects wildcards and
other accounts/Regions. The application still requires a real approved secret
before producer startup.

The authenticated plan completed using `AWS_PROFILE=blockchain-de`, IAM user
`blockchain-de-local`, account 209479274979. Its verified result is **28 creates,
0 updates, 0 deletes**. This is a proposal, not an apply or deployment approval.

## B. Why

An infrastructure preview should not require premature secret provisioning or
a fabricated ARN. Explicitly selecting the local operator profile also avoids
using the pipeline identity for Terraform work. Native locking protects the
separate runtime state against concurrent operations.

## C. Files changed

The planning implementation is already committed at `94d193f`: optional secret
inputs, a conditional IAM statement, five offline provider-mocked tests,
Python contracts, CI integration, and operating documentation. This follow-up
changes only documentation:

- This completed A-J plan review.
- `docs/aws_streaming_pilot.md`: current readiness and remaining approval gates.
- `docs/policies/runtime-plan-state-lock.json`: the separately approved runtime
  lock-object policy, administered outside Terraform.

`docs/policies/runtime-plan-public-ami-read.json` records the other approved
operator permission. Neither operator policy is adopted into runtime Terraform
state. No Terraform configuration or application code changed in this follow-up.

## D. Tests and validation

The preceding implementation passed 560 Python tests, five isolated
credential-free/backend-free AWS-mock cases, Terraform formatting/validation,
lint, the secret scan, and all six hosted CI jobs. The mock cases cover empty
secret access, exact-ARN opt-in, and rejection of wildcards/wrong accounts/Regions.

The completed authenticated plan used only
`infra/terraform/environments/runtime/`, default workspace, us-west-2/us-west-2a,
and backend key `production/runtime/terraform.tfstate`. The binary and JSON were
retained privately. Private JSON checks confirmed a complete, non-errored plan,
28 managed creates, no updates/deletes, and no existing resources claimed.

Native S3 locking stayed enabled. Both the runtime state object and temporary
`.tflock` object were confirmed absent after planning. The platform backend was
not initialized, migrated or planned.

## E. Security review

The user separately approved and reported adding public-AMI read and exact
runtime-lock permissions to `blockchain-de-local` through an administrator.
Public AMI access and successful lock acquisition/release were verified. The
available profiles could not manage IAM, and the assistant's write attempts
were denied. Live policy readback was unavailable, so this report does not claim
an independent audit of every permission attached to the operator.

The runtime plan review confirmed:

- No ingress, SSH key, NAT Gateway, ALB, Elastic IP, Secrets Manager secret or
  Scheduler resources.
- IMDSv2 required with hop limit two; standard T3 credits; termination
  protection; guest shutdown stops the instance.
- Encrypted 30-GiB root and encrypted 100-GiB Kafka gp3 volumes.
- Pilot object access limited to
  `blockchain-analytics/pilot/runtime/11-6c-v1/*`, with explicit state-bucket
  denial for the future runtime role.
- Empty secret allowlist and no Secrets Manager read statement.
- Fourteen-day logs, immutable ECR tags and scan on push.
- Only new runtime resources; no changes to the existing 43 platform resources,
  Snowflake or Streamlit.

Some resource ARNs and rendered user data remain computed until IDs exist at
apply time. Their committed source/template was reviewed. Offline contracts
still require fail-closed EBS mounting, no filesystem formatting and no automatic
application startup. No secret values were retrieved.

Full plans/logs/local backend metadata remain outside Git in a restricted
temporary directory. No raw state, full plan, credential, endpoint, secret value
or external ID is included here. Earlier failed artifacts are retained privately
and marked `INCOMPLETE`; they must not be used. Terraform considers the new
successful plan technically applicable; **this does not authorize applying it**.

## F. Git and CI

The exact planned configuration is
`94d193f93998b9d6334ceca07189e1f6e4f96a11`. Its six jobs passed in
[CI run 43304620356](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/43304620356).
This documentation checkpoint does not change that configuration. Its final
commit and automatically triggered CI result are recorded in the conversation.
`main` remains at `21be18e45791a4199830754b80620d7d6599587e`.

Runtime configuration SHA-256 (Terraform files, lockfile, bootstrap template):
`e177b93d39fb97db6f5633b25f05fac9e5d88d73a86bcfd645279ee25534f37e`.
Saved successful plan SHA-256:
`36d446d33d7b3adafa94999fa5af6f874242f8388efef14e1a3f7f4dc74f6a9f`.
The binary stays private, outside Git. Refresh and re-review before a separately
approved apply if configuration, state or AMI selection has changed; do not
assume this snapshot remains current indefinitely.

## G. Cost and runtime

No EC2, EBS, ECR, runtime IAM role, log-group or secret resources were deployed.
The only approved IAM changes reported by the user are the local operator's
public-AMI and runtime-lock policies, administered outside Terraform. Planning
used normal request/locking operations on the existing S3 backend; existing API
and versioning charges can apply. There is no new runtime compute charge.

The subsequent [launch approval package](aws_demo_launch_approval.md) refreshes
the Oregon price inputs from official AWS files: **USD 17.46 base at 40 runtime
hours** with 130 GiB retained for the full month and two alarms. Its stated
future streaming example is **USD 19.60** before excluded usage/taxes. The
inherited USD 24 estimate remains a planning allowance, not an approved cap or
guaranteed bill. The first streaming release proposes one hour plus bounded
shutdown/boot time; initial provisioning has a separate supervised-window
proposal. An apply launches EC2 immediately, even without a secret or containers.

The retained 130-GiB EBS storage, ECR images, logs and later secret/S3 storage
continue to cost money when compute is stopped. Public IPv4 is separately
billed while allocated ([AWS IPv4 pricing](https://aws.amazon.com/vpc/pricing/));
EBS charges remain while volumes are retained
([AWS EBS pricing](https://aws.amazon.com/ebs/pricing/)). The
[cost evidence snapshot](evidence/aws_runtime_demo_costs_2026-11-12.json) records
exact rates, quantities and exclusions; refresh it if launch is delayed.

## H. Verified plan and remaining deployment prerequisites

| Input | Verified proposal |
|---|---|
| Account / Region / AZ | 209479274979 / us-west-2 / us-west-2a |
| Operator | profile `blockchain-de`, IAM user `blockchain-de-local` |
| Instance | t3.xlarge, standard CPU credits |
| AL2023 x86_64 AMI | `ami-0dac6cd09676c9fb1` |
| Storage | 30-GiB encrypted gp3 root; 100-GiB encrypted gp3 Kafka |
| Pilot prefix | `blockchain-analytics/pilot/runtime/11-6c-v1` |
| Secret allowlist | empty; secret-read statement omitted |
| Scheduler resources | disabled; no automatic start schedule |
| Compose | official v5.1.3 Linux x86_64 binary |
| Compose SHA-256 | `a0298760c9772d2c06888fc8703a487c94c3c3b0134adeef830742a2fc7647b4` |
| Managed resource actions | 28 creates, 0 updates, 0 deletes |

The Compose binary was downloaded locally and its SHA-256 matched both official
release metadata and the published checksum. This version matches the local
offline validation environment. Nothing was installed on AWS. Source:
[official Compose v5.1.3 release](https://github.com/docker/compose/releases/tag/v5.1.3).
The exact 28 resource addresses are listed by category in the
[pilot readiness report](aws_streaming_pilot.md).

Deployment still requires separately approved apply/initial-launch/spending,
review of the refreshed cost assumptions, an approved run window, image publication/scans and
actual ECR digests, approved secret provisioning and its exact ARN, SSM/operator
checks, and exact-volume EBS initialization approval. The pilot procedures cover
acceptance, recovery, graceful shutdown, rollback and cleanup.

A successful plan does not prove the operator can provision all 28 resources:
EC2/EBS, networking, ECR, IAM and monitoring writes were not exercised. Review a
separately approved provisioning identity and its exact scope before apply. Do
not widen pipeline-user privileges or grant blanket administrator access as a
shortcut. There is no remaining blocker to this completed planning step.

## I. Next step

Review this completed proposal and the [launch approval package](aws_demo_launch_approval.md).
Keep the project at the demo-planning stage until separately approving
provisioning-identity scope, the verified cost assumptions, the exact
apply/initial EC2 launch window and remaining deployment actions.
Filesystem initialization still requires approval naming the actual EBS volume
after it exists. No apply or deployment command is part of this handoff.

## J. Production infrastructure changes

Only the user-approved operator IAM permissions were added manually by the
user/administrator. The assistant verified their functional effect but made no
successful IAM writes. Native locking created and released its temporary runtime
lock object; no runtime state object remains. No Terraform apply ran. Existing
platform resources, Snowflake roles/grants/warehouse, Streamlit and the platform
state remain untouched by this work.
