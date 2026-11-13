# AWS demo: completed boundary-based runtime plan

Date: 2026-11-13 UTC. Milestone 11.6C. **Approved initialization and planning
completed; no apply or AWS runtime deployment performed.** Milestone 11.5D
remains paused.

## A. What was implemented

Initialized the runtime S3 backend in a new private local data directory, then
generated and reviewed a saved plan through the MFA-protected
`blockchain-demo-deploy` profile. The result is **28 managed creates, zero
updates, zero deletes and zero imports**, matching the approved proposal.
Terraform exited 2, its normal detailed-exitcode result for proposed changes.

Only `production/runtime/terraform.tfstate` in the existing
`blockchain-analytics-terraform-state-209479274979-usw2` bucket was selected.
Native locking remained enabled in the default workspace. The lock was
released; the runtime state object remained absent after planning.

## B. Why

The older plan omitted the runtime EC2 role boundary. This replacement uses
the dedicated deployment role and exact administrator-managed boundary while
keeping secret access disabled. It provides a reviewable infrastructure
proposal without creating the paid EC2 runtime.

## C. Files changed and retained artifacts

- This completed review and status links in the pilot/access/launch documents.
- [Sanitized plan evidence](evidence/aws_runtime_boundary_plan_2026-11-13.json),
  containing action addresses, checked controls and artifact hashes.
- The [approved noncredential inputs](evidence/aws_runtime_boundary_plan_inputs_2026-11-13.json)
  are unchanged. No Terraform, application, CI or README edits were needed.
- Full plan binary/JSON, logs and local backend metadata remain outside Git in
  `/tmp/blockchain-runtime-boundary-plan-qo31l9uu`, whose directory is mode 0700.
  The saved plan and captured output files are mode 0600. Earlier artifacts
  remain preserved. Temporary files are not a durable deployment release.

## D. Tests and validation

Before execution, checked clean develop, reviewed source/input hashes, exact
role/account, more than 15 minutes of session lifetime, boundary-policy
readback and absence of runtime state/lock objects. No inherited credential,
endpoint, Terraform variable, CLI-argument or debug-log overrides were used.

Terraform 1.17.0 initialized successfully using locked AWS provider 6.69.1
from the existing local provider cache, with the lockfile read-only. The saved
plan is complete and non-errored; all 14 plan checks passed. Private JSON
validation confirmed the exact 28-address allowlist, AWS provider only,
create-only actions, no imports and no prior managed resources.

The planned source checkpoint is `b695f7f29518791e0059a9c5fcab168c06fe561c`,
whose [six CI jobs passed](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/43512686939).
This documentation checkpoint runs JSON/link/whitespace checks, lint and secret
scanning; its automatically triggered CI result is recorded in the conversation.

## E. Security and correctness review

Concrete plan values confirm:

- Exact EC2 boundary `arn:aws:iam::209479274979:policy/blockchain-runtime-ec2-boundary`
  and EC2-service-only role trust; only the reviewed SSM managed attachment.
- One `t3.xlarge` in `us-west-2a`, AMI `ami-0dac6cd09676c9fb1`, standard CPU
  credits, IMDSv2 required, hop limit two, termination protection and guest
  shutdown behavior `stop`; no SSH key or detailed monitoring.
- Encrypted gp3 root 30 GiB and Kafka volume 100 GiB/3,000 IOPS/125 MiB/s.
- Empty secret allowlist, exact isolated pilot prefix, 14-day log retention,
  immutable ECR tags and scan on push. No NAT Gateway, ALB, Elastic IP,
  Secrets Manager or Scheduler resources appear in the create allowlist.
- Four expected egress rules: TLS 443, VPC DNS TCP/UDP 53 and time-sync UDP 123.

Some values remain computed until creation: effective SG ingress/egress,
rendered runtime IAM/SNS policies, user data and new resource IDs. These were
reviewed through their unchanged source and templates, not claimed as known
plan values. There are no ingress declarations/resources; the provider must
remove the new SG's default broad egress during creation. The conditional
secret statement is omitted with the empty input. Bootstrap still fails closed
with `EBS_INITIALIZATION_REQUIRED`, has no formatting command and does not
retrieve secrets or start application containers. A separately approved
deployment must verify the resulting settings before application startup.

Successful planning proves backend access and native-lock acquisition/release,
not permission to create every resource, write the actual state file or stop a
future instance. Administrator effective-permission review remains required
before paid apply. No permission expansion or write probes were performed.

## F. Git and CI

Develop remains the working branch; main is unchanged at
`21be18e45791a4199830754b80620d7d6599587e`. The report checkpoint and hosted CI
result are recorded in the conversation. No main merge is included.

Reviewed source SHA-256 (root Terraform files and templates):
`910d53461d49d46ac83dc03037a8e5827a9cf0610a0f1f65112407de770e60a6`.

Source SHA-256 including the provider lockfile:
`78b6606027470aa8e06ce1306308797ade322fecabc9e65672f08a40dc200015`.

Saved binary plan SHA-256:
`b9406bba089870186ff7f5d7e71274a3a878f82c6aee96e2858906dc72a6512e`.

These hashes identify this snapshot; they are not apply approval. Recheck source,
inputs, state, AMI, artifact integrity and credentials before any separately
approved deployment. Do not substitute the historical boundary-free plan.

## G. Cost implications

No runtime compute/storage/monitoring resources were created. Initialization and
planning used the existing S3 backend; request charges and retained lock-object
versions can incur small existing-bucket charges. No measured bill is claimed.

The [November 12 cost model](aws_demo_launch_approval.md#g-verified-cost-model)
remains a dated planning estimate: approximately USD 17.46 monthly base at
40 running hours, or USD 19.60 under its stated variable-usage example, before
exclusions and tax. The original approximately USD 24 allowance is not a cap.
Refresh costs and the agreed duration before requesting a paid launch.

## H. Remaining blockers

No blocker remains for this completed planning step. Before a paid apply,
review effective creation/state-write/stop permissions and the supervised stop
window. Applying this plan would launch EC2 immediately. There is no AWS-side
hard-stop schedule, and expiring credentials do not stop compute.

Before an actual streaming demonstration, complete separately reviewed SSM
operator access, image publication/scans and ECR digests, secret provisioning
and exact-ARN access, exact-volume EBS initialization, and application deployment.
The initial boundary currently excludes Secrets Manager. None of those actions
is authorized by the completed init/plan approval.

## I. Next step

The [first-launch window review](aws_runtime_first_launch_window.md) now records
fresh public pricing, launch/stop policy metadata and the pending administrator
checks. It does not authorize paid deployment.

Complete the remaining launch-permission and supervised-stop review, refresh
the paid-window proposal, and request explicit approval for its concrete scope.
Then use separate exact-resource approvals for storage initialization and app
deployment. The [pilot runbook](aws_streaming_pilot.md) retains smoke, recovery,
graceful-shutdown, rollback and cleanup checks.

## J. Production infrastructure changes

No runtime resources or IAM grants were created or changed; no EBS formatting,
secret access, image push, Snowflake operation or deployment occurred. Planning
temporarily acquired/released the approved runtime lock in the existing S3
backend. The platform state and 43 production resources were not accessed or
changed. Milestone 11.5D remains paused.
