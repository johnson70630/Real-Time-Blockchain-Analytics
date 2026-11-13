# AWS demo launch approval package

Date: 2026-11-12. **Review only: no deployment or IAM changes authorized or
performed by this package.** This is the scheduled demonstration scenario,
initially 40 running hours per month. Milestone 11.5D remains paused.

The completed infrastructure plan proposes **28 creates, 0 updates, 0 deletes**.
Current Oregon prices give a **USD 17.46 monthly base at 40 running hours**,
before variable services. A stated future streaming example is **USD 19.60**
before excluded usage and taxes. Those prices are the dated November 12
snapshot. The deployment identity has since been verified; see the
[current access/preflight and plan proposal](aws_runtime_boundary_plan_approval.md).
This historical launch review is not an apply approval.

## A. What was prepared

- A current, source-backed Oregon cost snapshot and inspectable calculations.
- A service-by-service provisioning-permission review for the existing plan.
- A supervised first-provisioning window and separate streaming-demo window.
- A record of authorization checks and remaining approval gates.

No Terraform, application, Docker, warehouse or CI configuration changes are
part of this review. The exact resource scope remains the
[verified runtime plan](aws_runtime_plan_review.md).

## B. Why

Planning access does not establish permission to create EC2, EBS, networking,
ECR, IAM and monitoring resources. An incomplete permission grant could leave a
partially provisioned, billable environment. The identity and stop path must be
reviewed together before launch, rather than discovered through resource writes.

Storage and alarms also remain chargeable when compute is stopped. Costs must
separate running hours, retained capacity, and workload assumptions.

## C. Files and evidence

- This approval package.
- [Cost evidence JSON](evidence/aws_runtime_demo_costs_2026-11-12.json): selected
  official price rows, source URLs/versions, units, assumptions and calculations.
- Cross-references in the plan review, pilot readiness and runtime operations
  documentation. No raw plan/state, credentials or secret values are included.

## D. Validation and current permissions

The repository was clean on develop at `34cf9e0`; its six CI jobs passed.
The completed plan configuration remains `94d193f`, and no new production
Terraform plan was run during this review.

AWS identity was verified as `blockchain-de-local` using profile `blockchain-de`
in account 209479274979. Its public AMI lookup and native runtime locking were
already verified. A read-only `iam:SimulatePrincipalPolicy` request for
CreateRole/PutRolePolicy/AttachRolePolicy/PassRole on the exact planned runtime
role returned **AccessDenied**. This means simulation is unavailable; it does
not prove those four provisioning actions themselves are denied or allowed.
No resource-write API calls or permission grants were used as tests.

The simulator is not an authorization guarantee even when available:
[AWS policy simulator guidance](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_testing-policies.html).
An administrator must review the actual identity policies, permissions boundary,
SCP/session policies, relevant resource policies and request conditions.

Price rows were filtered to Oregon, USD, on-demand/shared Linux t3.xlarge with
no preinstalled software, ordinary gp3 storage, and standard-resolution alarms.
Calculations use decimal arithmetic; expected subtotals were checked. No Free
Tier, credits, taxes, reservations or Savings Plans are assumed.

## E. Provisioning-permission and security review

### Identity recommendation

The operator subsequently confirmed a root Console login and reported no
deployment-role match. The proposed dedicated-role setup and exact offline
policy documents are in [the IAM setup review](aws_demo_deployer_setup.md).
Nothing has been provisioned. Its boundary design changes Terraform inputs;
the earlier saved plan must not be applied for that design. A fresh scoped
plan and separate IAM/deployment approvals are required.

Keep `blockchain-de-local` as the planning identity and keep the pipeline user
out of provisioning. Prefer an **existing, explicitly approved provisioning role
with a temporary federated/SSO session** for the one-off apply. Record the chosen
profile, principal ARN, account, session expiry and effective scope before use;
do not send or store credentials in the project. No such profile has been
selected or verified yet. This follows
[AWS temporary-credential guidance](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html).

If no suitable role exists, prepare a dedicated role for separate administrator
approval. Do not add AdministratorAccess to either local or pipeline users.
Delegating CreateRole plus PutRolePolicy and PassRole can permit privilege
escalation even when the role name is restricted. A dedicated provisioner design
must therefore bound the future runtime role's permissions, for example with an
administrator-controlled permissions boundary, or reserve IAM creation for a
reviewed administrator. The current Terraform runtime role has no boundary
input; choosing that design would require a reviewed configuration change and
an updated plan before apply. A boundary grants no permissions by itself:
[AWS permissions-boundary guidance](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_boundaries.html).

### Permission matrix for administrator review

The operations below are a review inventory, **not an attachable or exhaustive
IAM policy**. The locked AWS provider may make additional read, tagging, waiter
and post-create calls. Resource support/conditions differ by API; validate the
final policy against the selected identity and provider version. Do not replace
this work with `ec2:*`, `iam:*` or `s3:*` grants.

| Area | Representative operations to review | Scope and boundary |
|---|---|---|
| Backend | S3 ListBucket/GetObject, **PutObject on runtime state**, Get/Put/Delete on runtime lock | Exact `production/runtime/terraform.tfstate` and `.tflock` in the existing state bucket. Never DeleteObject on the state file; no platform-state access. Planning did not verify state-file writes. |
| Public AMI | `ssm:GetParameter` | Only the approved AL2023 public parameter; no general Parameter Store or Secrets Manager access. |
| VPC/subnet/routes/IGW | CreateVpc, ModifyVpcAttribute, CreateSubnet, ModifySubnetAttribute, CreateInternetGateway, AttachInternetGateway, CreateRouteTable, CreateRoute, AssociateRouteTable, tagging and reads | New runtime VPC/subnet/resources only, account 209479274979, us-west-2; use the project/workload tags where supported. No default/shared production VPC modifications. |
| Security group | CreateSecurityGroup, RevokeSecurityGroupEgress, AuthorizeSecurityGroupEgress, tagging and reads | New `blockchain-runtime` SG only. Terraform must remove the automatically created broad default egress rule. No ingress authorization. |
| EC2 | RunInstances, needed attribute/credit updates, CreateTags and Describe/read calls | One t3.xlarge, reviewed AL2023 AMI, us-west-2a, new runtime subnet/SG, standard credits, IMDSv2, exact runtime instance profile. Review instance/volume/ENI/image resource dependencies. |
| EBS | CreateVolume, AttachVolume, tags and Describe/read calls | Encrypted 100-GiB gp3 Kafka volume plus 30-GiB root volume created with EC2, same AZ. Account-default KMS behavior must be checked; a customer-managed key may need separately reviewed KMS permissions. No snapshots, formatting, detach or delete in the initial window. |
| Runtime IAM | CreateRole, GetRole, TagRole, CreateInstanceProfile, TagInstanceProfile, AddRoleToInstanceProfile, PutRolePolicy, AttachRolePolicy, and required reads | Only role and instance profile `blockchain-runtime-ec2`, inline policy `blockchain-runtime-least-privilege`, and the reviewed SSM managed-policy attachment. Address delegation/privilege-escalation controls before granting these. IAM is global; a blanket us-west-2 condition must not accidentally block global IAM calls. |
| PassRole | `iam:PassRole` | Only `arn:aws:iam::209479274979:role/blockchain-runtime-ec2`, conditioned on `iam:PassedToService=ec2.amazonaws.com`. No arbitrary roles or services. |
| ECR | CreateRepository, PutLifecyclePolicy, tagging and required Describe/Get reads | Only `blockchain-runtime-producer` and `blockchain-runtime-spark`. Inspect registry scanning settings; enhanced scanning can add charges. Image push/publication is a later approval, not part of this apply scope. |
| Logs/alarms | CreateLogGroup, PutRetentionPolicy, log tagging/reads, PutMetricAlarm and alarm reads/tags | Only `/blockchain/runtime/containers` and the two `blockchain-runtime-*` alarms, 14-day logs. No arbitrary log deletion or unrelated alarms. |
| SNS | CreateTopic, SetTopicAttributes, TagResource and topic reads | Only `blockchain-runtime-alerts` and its reviewed resource policy. No subscription or outbound-recipient setup in the current plan. |
| EventBridge | PutRule, PutTargets, tagging and required rule/target reads | Only `blockchain-runtime-state-change`, targeting the exact runtime SNS topic. No Scheduler or StartInstances grants in this plan. |
| Supervised stop/access | StopInstances, DescribeInstances/Status; approved SSM session access and required reads | Only the newly created runtime instance, identified by its apply result and tags. Establish the stop path before RunInstances; application readiness must not be required to stop compute. |

PassRole restrictions are documented by
[AWS](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_passrole.html).
Tag filters are not a substitute for the exact role ARN and PassedToService
condition. Runtime-container IAM permissions and provisioning permissions are
different; neither should be copied wholesale onto the other identity.

### Required read-only preflight with the selected provisioner

Before apply, verify the backend write/lock authorization through policy review,
the target role/profile identity and expiry, name collisions/unmanaged runtime
objects, EC2/vCPU/EBS quotas, t3.xlarge offering in the chosen AZ, AMI freshness,
default EBS encryption/KMS settings, registry scanning mode, and operator stop/
SSM access. These account-specific checks remain unverified where the current
planning identity cannot inspect them. No launch/write call should be used to
discover missing permissions. A clean Terraform plan does not discover every
unmanaged object with a conflicting physical name or guarantee capacity.

No apply credentials or new role are authorized by this document. No destroy,
termination, force-stop, detach, import or state migration is requested. The
default-egress removal on the newly created SG is part of its reviewed creation,
not permission to edit an existing platform security group.

## F. Git and CI

This is a documentation/evidence checkpoint on develop. Its commit and CI
result are recorded in the conversation. The runtime configuration, reviewed
plan hashes and resource scope remain those in the plan review. README/main
are unchanged; the existing platform state and 43 resources are not part of
this work.

## G. Verified cost model

The [public AWS regional price files](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/using-the-aws-price-list-bulk-api-fetching-price-list-files-manually.html)
were retrieved on 2026-11-12. The evidence JSON records exact SKU/rate codes,
effective dates, source URLs and offer versions; selected offers were published
2026-11-04 through 2026-11-11. These are current public list rates, not a locked
commercial quote or a spending authorization.

| Base component | Quantity at 40 hours/month | Public USD rate | Monthly amount |
|---|---:|---:|---:|
| Linux t3.xlarge, on demand | 40 hours | 0.1664/hour | 6.6560 |
| gp3 root + Kafka EBS | 130 GiB retained all month | 0.08/GiB-month | 10.4000 |
| Automatically assigned public IPv4 | 40 allocated hours | 0.005/hour | 0.2000 |
| Standard metric alarms | 2 retained all month | 0.10/alarm-month | 0.2000 |
| **Base subtotal** | | | **17.4560 (17.46 rounded)** |

This includes baseline gp3 performance only (3,000 IOPS/125 MiB/s). T3 standard
credits avoid Unlimited surplus-credit billing but can throttle under load.
The 40 hours include provisioning, boot, troubleshooting and streaming time.
An instance stop does not remove the volumes or alarms.

For the future streaming-demo example only, add these **unmeasured assumptions**:

| Additional usage | Rate | Monthly amount |
|---|---:|---:|
| 3 GB-month private ECR images | 0.10/GB-month | 0.3000 |
| 1 GB standard log ingestion | 0.50/GB | 0.5000 |
| 0.5 GB average retained logs | 0.03/GB-month | 0.0150 |
| 10 GB-month S3 Standard, first-tier storage | 0.023/GB-month | 0.2300 |
| 100,000 S3 PUT/COPY/POST/LIST requests | 0.005/1,000 | 0.5000 |
| 500,000 S3 GET/other tier-two requests | 0.0004/1,000 | 0.2000 |
| One later-approved secret | 0.40/secret-month | 0.4000 |
| 100 secret API requests | 0.05/10,000 | 0.0005 |
| **Example total including base** | | **19.6015 (19.60 rounded)** |

The infrastructure-only plan still has no secret or images. Those example
charges require separate later actions and are not measured workload usage.
Checkpoint versions and old object/image versions must be included in actual
stored-byte measurements; retention does not automatically delete them here.

The USD 24 planning allowance leaves about USD 4.40 above this example. It is
**not a hard cap or a guarantee**. SNS delivery, outbound/cross-AZ/cross-region
transfer, snapshots, enhanced scanning, KMS charges, excess storage/requests/logs, taxes and
any changed resource scope are excluded from the example. Existing Snowflake,
Streamlit and other platform costs are excluded. No Free Tier/credits are netted
against the rates. Review the snapshot again if launch is delayed.

With one runtime hour but volumes/alarms retained all month, the base is still
**USD 10.77**, not merely the USD 0.1714 compute-plus-IPv4 charge. Accidentally
running 730 hours gives a **USD 135.72 base before variable services**; continuous
runtime is not authorized. A fresh quote does not make the old USD 24 estimate
an approved budget. No new resources or running-instance charges were incurred
by this review.

## H. Remaining decisions and blockers

- Choose and review an existing temporary provisioning identity, or separately
  approve a dedicated deployment-role design. Current write authorization is
  unverified; denied simulation is not permission to proceed.
- Confirm the read-only preflight findings, especially state-file persistence,
  a working StopInstances fallback and name/KMS/quota checks.
- Review a fresh plan within the previously approved runtime scope, its source
  hash and current cost assumptions. A changed IAM boundary/Scheduler/resource
  design requires a new scoped plan review before spending approval.
- Explicitly approve the 28-resource apply, initial paid EC2 launch, stop action
  and supervised time window. Those approvals are not implied by this review.
- Keep EBS first initialization, image publication, secret provisioning and
  later secret-read IAM changes as separate gates. EBS approval must name the
  real volume. Actual ECR digests do not exist yet.

## I. Proposed launch and shutdown sequence

### Window 1: supervised infrastructure provisioning (future approval)

Propose a **60-minute initial provisioning window**, with one active operator
and an administrator/console stop fallback. No schedule/date has been selected.
The target is EC2 stopped by the window end, not one hour of streaming.

1. Verify account/profile/session, reviewed plan hash, native backend locking,
   state persistence and stop authority. Establish an absolute UTC end time and
   keep the AWS console/CLI stop path available before applying anything.
2. Apply only the reviewed saved runtime plan after explicit approval. It
   creates the 28 resources and launches EC2 immediately. Record the instance
   and volume IDs from restricted outputs; identify only resources from this
   apply if an operation fails partway through.
3. Observe SSM/bootstrap. A blank Kafka volume should report
   `EBS_INITIALIZATION_REQUIRED`. Record that expected boundary; do not format,
   start containers, inject secrets or widen network ingress.
4. Close provisioning early when possible. Request a normal stop by minute 45
   to leave time to verify `stopped` by minute 60. Verify the exact instance ID
   and retained encrypted EBS volume. Do not rely on losing SSM connectivity as
   proof compute stopped. No forced stop or termination is preapproved.
5. If apply fails, preserve its state/error artifacts privately and stop only
   the new runtime instance through the approved fallback. No blind re-apply,
   destroy, import, force-unlock or state migration. If state persistence fails,
   protect any `errored.tfstate` outside Git and obtain a reviewed recovery plan.

**The current 28-resource plan has no AWS-side automatic stop schedule.** Its
guest/application timer is installed in a later step, so it does not bound
initial provisioning. The first window needs active supervision and stop
authority. Optional cloud Scheduler controls would change the reviewed resource
scope and require separate approval and a new plan. The one-hour target is
therefore an operating procedure, not an automatic cost ceiling.

### Window 2: first streaming demo (separate future approval)

After exact-volume initialization and image/secret/deployment approvals, use a
reviewed release with `max_runtime_seconds=3600`, then graceful drain/stop time.
Start the existing independent 150-minute emergency timer before the pilot
service. It is a fallback, not permission to run for 150 minutes. No service is
enabled on boot and no automatic start schedule exists. Stop producer ingress,
verify Spark drains/commits, stop Kafka last, and verify EC2 stopped. Recovery,
rollback and live acceptance remain in the [pilot procedures](aws_streaming_pilot.md).

The application timer permits later configurations up to two hours, but this
first-demo proposal is only the one-hour manifest. All billed running time and
retained storage must be reconciled with the approved window afterward.

## J. Production infrastructure changes

None during this review. Only read-only AWS identity/simulation checks, public
price downloads and local documentation/calculations ran. No IAM grant, new
credential, resource creation/modification, Terraform plan/apply, instance start,
secret retrieval, EBS initialization or Snowflake operation was performed.
The earlier user-approved operator IAM policies remain externally administered.
Milestone 11.5D remains paused.
