# AWS demo: first infrastructure launch window

Date: 2026-11-13 UTC. Milestone 11.6C. **Preparation only. No apply, launch,
stop, IAM modification or production credential deployment was performed.**
The operator has confirmed IAM Console review with no unresolved findings and
reported the Organizations screen showing **Create an organization**. This
indicates a standalone account; no organization was created. A separate
read-only check found no state-bucket policy. The proposal is ready for an
explicit paid-window decision, subject to the execution checks below.
Milestone 11.5D stays paused.

## A. What was prepared

Refreshed the public Oregon prices, reviewed launch/stop policy conditions and
prepared the first supervised infrastructure-only window. This phase creates
the runtime foundation; it does not run the blockchain application.

The [completed saved plan](aws_runtime_boundary_plan_review.md) proposes exactly
28 creates, zero updates/deletes/imports. It includes the isolated VPC, public
subnet/routes/gateway, no-ingress SG and four egress rules; EC2, encrypted root
and Kafka disks/attachment; bounded runtime IAM role/profile/policies; two ECR
repositories/lifecycle policies; log group, two alarms, SNS topic/policy and
EventBridge rule/target. The existing platform resources are outside this root.

## B. Why

A successful plan proves planning and native-lock access. It does not prove
effective creation, runtime-state persistence or normal-stop permission.
Creation can partially succeed before a later API fails. The stop path and
state-recovery procedure must therefore be available before the paid instance
starts. See [AWS policy evaluation](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_evaluation-logic.html).

## C. Files and exact inputs

- This launch/stop runbook and status links in the current readiness reports.
- [Fresh cost evidence](evidence/aws_runtime_demo_costs_2026-11-13.json).
- [Public authorization-metadata review](evidence/aws_runtime_launch_permissions_2026-11-13.json).
- [Pending account-specific gate receipt](evidence/aws_runtime_launch_gate_2026-11-13.json).
- [Unchanged noncredential Terraform inputs](evidence/aws_runtime_boundary_plan_inputs_2026-11-13.json).

Account `209479274979`, Region `us-west-2`, AZ `us-west-2a`, profile
`blockchain-demo-deploy`. One `t3.xlarge`; standard CPU credits; encrypted gp3
root 30 GiB and Kafka 100 GiB. Exact EC2 boundary:
`arn:aws:iam::209479274979:policy/blockchain-runtime-ec2-boundary`.
Secret allowlist remains empty; scheduled shutdown remains disabled.

No Terraform change is proposed. Backend is only
`production/runtime/terraform.tfstate`; native locking and default workspace
remain mandatory. Never initialize, read or migrate the platform backend.

## D. Validation

Fresh unauthenticated downloads rechecked all 12 previously reviewed SKU/rate
identities across six official regional price files. No rate changed.

Current public AWS service-reference JSON recognizes all 107 explicit
deployment-policy action entries (excluding the intentional `s3:*` deny).
The EC2/IAM review checked 137 action/resource/condition combinations. Nine
redundant request-tag/VPC dependency branches were identified: the same VPC
dependencies are separately allowed by `NetworkCreationVpcDependency` using
the three existing ownership tags. No required dependency relies on the
redundant branch, and no IAM edit is proposed. These reviewed branches remain
visible in the evidence rather than being treated as proof of live access.
The 19 existing offline policy security tests passed.

Metadata coverage is not an IAM simulator, API dependency guarantee or readback
of the installed deployment policies. The review used no authenticated AWS
calls, write probes or DryRun calls. Source:
[AWS machine-readable authorization reference](https://docs.aws.amazon.com/service-authorization/latest/reference/service-reference.html).

## E. Account-specific permission and stop review

The operator confirmed review of the three deployment policies' IAM Console
validation findings, including runtime state writes and normal EC2 stop access,
with no unresolved findings. This is operator-attested review, not independent
API readback of every installed deployment-policy document.

The operator subsequently reported **Create an organization** in the
Organizations Console. Treat the account as standalone based on that Console
observation; organization SCPs/RCPs do not apply. No Organizations mutation or
privilege expansion is needed. Source:
[AWS organization creation flow](https://docs.aws.amazon.com/organizations/latest/userguide/orgs_manage_org_create.html).

At 2026-11-13T22:12:23Z, `s3:GetBucketPolicy` through the existing
`blockchain-de` planning user returned `NoSuchBucketPolicy` for the exact state
bucket with expected owner `209479274979`. No state object bodies were read.
The reviewed deployer identity policy grants `GetObject/PutObject` on only the
runtime key; the prior successful plan verified native lock acquisition/release.
These checks resolve the resource-policy review without a state-write probe.
Actual state persistence and future instance stop have not been exercised.

The CLI role/profile identity and EC2 boundary readback are already verified.
Do not repeat IAM setup, MFA enrollment or broaden permissions for inspection.
The current role cannot read back its own full deployment-policy set. The
following checklist preserves the review scope and remaining execution checks:

1. IAM → Policies: inspect the **current default-version JSON and validation
   findings** for `blockchain-runtime-deploy-control`,
   `blockchain-runtime-deploy-ec2` and
   `blockchain-runtime-deploy-discovery-tags`. Compare with their repository
   JSON. Resolve errors; record and understand warnings. Viewing is sufficient;
   do not save a new policy version or edit a grant without separate approval.
2. Review effective permissions for the dedicated deployer: create-only scope,
   exact bounded role/PassRole delegation, runtime state `GetObject/PutObject`,
   exact lock-object `GetObject/PutObject/DeleteObject`, and normal
   `ec2:StopInstances` on the new instance carrying the three ownership tags.
   Include resource-policy denies, applicable SCPs/RCPs, session restrictions
   and any boundary. If the account is standalone, record that; otherwise
   inspect applicable organization controls through the administrator.
3. Confirm the existing state bucket permits persistence to the runtime key;
   the observed lock cycle is not evidence of state-file PutObject. Do not
   test by overwriting state or modifying the bucket policy.
4. Keep an authenticated administrator Console available in account
   `209479274979`, Region `us-west-2`, with EC2 stop access as a fallback.
   Do not create root keys, grant AdministratorAccess or use root CLI.

The reviewed `RuntimeCreditsAndStop` statement permits stop only in us-west-2
on instance ARNs with `Project=real-time-blockchain-analytics`,
`Environment=production-runtime`, `Workload=blockchain-runtime`.
Start, termination, detach, volume deletion and ingress authorization are not
granted by these deployment policies. IAM permits more stop flags than this
procedure authorizes: **normal OS shutdown only**, with force, hibernate and
skip-OS-shutdown all false. Source:
[AWS StopInstances API](https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_StopInstances.html).

No instance exists yet, so no exact-instance stop test has been performed.
Once an instance is created under separate approval, use its actual ID from
the private apply result, cross-check account/AZ/profile/tags, and verify its
final state using EC2 metadata. Never stop an instance selected by tag alone.

## F. Git and CI

Develop remains the working branch. This checkpoint changes documentation and
sanitized evidence only; no Terraform, application, CI or README changes.
JSON, calculations, local links, whitespace, secret scan and policy tests are
validated. The commit and automatically triggered CI result are recorded in
the conversation. Main remains at `21be18e45791a4199830754b80620d7d6599587e`.

## G. Fresh cost and duration proposal

The exact official price-file URLs, offer versions, SKU/rate codes and retrieval
time are in the fresh evidence. USD list rates remain:

| Component | Rate | Proposed usage |
|---|---:|---|
| Linux t3.xlarge | 0.1664/hour | Up to 60 minutes for this first window |
| Assigned public IPv4 | 0.005/hour | While allocated during runtime |
| gp3 | 0.08/GiB-month | 130 GiB retained after stop |
| Standard alarms | 0.10/alarm-month | Two retained alarms |

Compute plus IPv4 for one running hour is **USD 0.1714**, before other usage.
Retained disks and alarms cost **USD 10.60/month**, approximately USD 0.35/day
using a 30-day month. One hour with those resources retained a full month is
**USD 10.77 base**. At 40 running hours/month the base is **USD 17.46**; the
unchanged future streaming example is **USD 19.60** with its assumed image,
log, S3 and secret usage. These are estimates, not a budget cap or spending
approval. Taxes, existing platform costs and excluded/extra usage are additional.

The proposed first window is infrastructure-only: request normal stop by
minute 45 and target confirmed `stopped` by minute 60. This is a supervised
target, not an automatic deadline. No AWS-side hard stop exists, and credential
expiry does not stop EC2. Storage/alarms continue billing until separately
approved cleanup; stopping is not cleanup. Review retained costs the next day
and agree whether to preserve the runtime or request a cleanup plan.

## H. Remaining blockers and approval scope

IAM Console validation and standalone-account checks are operator-confirmed;
state-bucket policy absence is independently verified. No paid apply is
approved. Request approval covering **the exact 28-create saved plan, immediate
paid EC2 launch, runtime-state persistence and normal stop of only its new
instance**, with active operator supervision and the administrator Console
stop fallback. Full installed-policy readback remains a disclosed limitation;
the readiness decision uses the operator's review and the unchanged repository
policy documents, not a claim that live writes are guaranteed to succeed.
Set an absolute UTC start/end immediately before execution; use a fresh MFA
session with approximately 55 minutes remaining and keep the Console fallback
available through the end. If the session needs renewal, the operator enters
MFA privately; never send codes to the assistant.

The future apply command, for review only, is:

```sh
/tmp/terraform-1.17.0/terraform -chdir=infra/terraform/environments/runtime apply \
  -input=false -no-color \
  /tmp/blockchain-runtime-boundary-plan-qo31l9uu/runtime-boundary.tfplan
```

It requires the already-reviewed clean environment, exact MFA profile and
private `TF_DATA_DIR`; all output must be captured privately. Before execution,
verify saved-plan SHA-256
`b9406bba089870186ff7f5d7e71274a3a878f82c6aee96e2858906dc72a6512e`,
source/input/lockfile hashes and runtime backend/state freshness. If state,
configuration, AMI or assumptions changed, stop and review a replacement plan
within the approved planning scope before any apply.

## I. Execution, shutdown and rollback after separate approval

1. Record the absolute window, identity/expiry, fallback and private artifact
   paths. Recheck relevant name/quota/AMI findings. Require active supervision.
2. Apply only the verified saved plan. Monitor progress; preserve logs/state
   privately. Do not stream full Terraform output or state into chat/CI.
3. Extract only `runtime_instance_id` and `kafka_volume_id` from private outputs.
   Independently verify their new runtime ownership, encrypted storage and
   attachment. Check effective SG, IMDS, credits, boundary and runtime policies.
4. Observe EC2/SSM registration through allowed read calls. Operator SSM session
   access is a later gate; no StartSession/SendCommand is included. A blank disk
   should fail bootstrap closed with `EBS_INITIALIZATION_REQUIRED`. No format,
   app images, secret injection, containers or blockchain traffic are included.
5. Finish early when possible. Request a normal stop by minute 45 using the
   exact new instance ID and the verified role, or the reviewed Console fallback.
   In SDK terms use `stop_instances(InstanceIds=[exact_id], Force=False,
   Hibernate=False, SkipOsShutdown=False)`. Verify EC2 reports `stopped`;
   disconnecting SSM or observing `stopping` is not sufficient.
6. If apply stalls, obtain a graceful Terraform interruption before abandoning
   it; do not force-kill a state write. Stop only the verified new runtime
   instance through the approved fallback when needed. No force stop,
   termination, blind re-apply, destroy, import or force unlock follows.
7. If state persistence fails, preserve any `errored.tfstate` privately and
   obtain a scoped recovery review. If no reliable new instance ID is available,
   use the administrator to reconcile creation time, ownership and outputs;
   never operate on a guessed ID or unrelated tagged resource.
8. Record final EC2/volume state, runtime state persistence and released lock,
   actual runtime duration and retained billable inventory. Review next-day
   billing and prepare separately approved cleanup if retention is not needed.

Streaming remains a later phase: SSM operator access, image push/scans/digests,
Alchemy secret and exact-ARN IAM/boundary changes, exact-volume initialization,
app startup, smoke tests and Kafka/Spark recovery checks. The existing
[pilot runbook](aws_streaming_pilot.md) covers that phase's graceful drain,
rollback and acceptance. None is authorized by this launch-window proposal.

## J. Production infrastructure changes

None. The initial review downloaded public price/authorization files and ran
local tests and calculations. The follow-up verified the planning identity and
read only state-bucket policy metadata, receiving `NoSuchBucketPolicy`. No state
object bodies, new Terraform plan/apply, IAM changes, instance launch/stop,
EBS initialization, secret access, image push or Snowflake operation occurred.
Existing platform resources are unchanged.
