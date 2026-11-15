# Milestone 11.6C: runtime foundation completion

Completed 2026-11-15 at 23:03:07 UTC. The operator explicitly approved only
the reviewed five-resource saved plan, with EC2 kept stopped. The apply and
post-apply checks succeeded. This completes the runtime infrastructure
foundation; the streaming demo has not run. Milestone 11.5D remains paused.

## A. What was implemented

Created exactly five Terraform resources:

| Address | Verified result |
|---|---|
| `aws_volume_attachment.kafka` | Existing encrypted volume `vol-02c9fb14a9c568fb5` attached to stopped instance `i-0ca1671a59751acc5` at `/dev/sdf` |
| `aws_cloudwatch_metric_alarm.high_cpu` | CPU alarm for that exact instance, targeting the existing runtime SNS topic |
| `aws_cloudwatch_metric_alarm.instance_status` | Status-check alarm for that exact instance, targeting the same topic |
| `aws_cloudwatch_event_rule.runtime_state` | Rule restricted to that instance's stopped/terminated events |
| `aws_cloudwatch_event_target.runtime_state` | Rule target is the existing runtime SNS topic |

Terraform reported **5 added, 0 changed, 0 destroyed**. All 23 existing resource
IDs are preserved. The runtime now contains **28 managed resource instances**.

## B. Why

The earlier partial apply left Kafka storage unattached and the final monitoring
resources absent. Completing these five resources prepares persistent storage
and runtime visibility while keeping compute stopped. Attachment does not
initialize a filesystem: the exact-volume formatting approval is still required.

## C. Files and artifacts

- This A-J report and
  [sanitized verification evidence](evidence/aws_runtime_remaining_apply_2026-11-15.json).
- Current status/sequence in `aws_streaming_pilot.md` and a consumed-plan notice
  in `aws_runtime_recovery_plan_result.md`.
- No application, Terraform configuration, IAM policy, dependency, workflow or
  README changes in this checkpoint.

Private execution artifacts are under
`/var/folders/vl/rg4x65p973s6bsvn_m85950m0000gn/T/blockchain-five-resource-apply-nz_qmu40`.
The directory is mode 0700 and state backups/logs are mode 0600. Raw state,
decoded plans, credentials and full Terraform output are not committed.

The saved plan SHA-256 is
`4a859fa4ff87bd7e0098727de829679eecd95ca6106bb3f45153668ed64e7382`.
It is now **consumed and must not be reapplied**. The local apply-attempt receipt
blocks accidental repeat execution. Earlier failed/partially used plans remain
invalid for deployment and are preserved as historical artifacts.

## D. Tests and validation

The exact application/source checkpoint `1534ed927dbe1fb2a0a4e80fed396938a2028dd3`
passed all six [hosted CI jobs](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/43770951907)
after the GitHub billing lock cleared. This includes the new bundle/image
contracts, actual Java 17 image builds and synthetic Kafka/checkpoint recovery.

Before applying, verified the clean develop checkout, unchanged Terraform and
input hashes, exact saved-plan hash, runtime backend/default workspace, fresh
MFA session, expected account/role, state serial/hash and absent native lock.
Read-only AWS checks verified stopped EC2, instance type/AZ/AMI/profile, IMDSv2,
standard credits, termination protection, guest shutdown-to-stop, no ingress,
exact egress, encrypted disks, runtime boundary and unchanged role policies.
The two alarm names and event rule were absent before creation.

The local preflight parser initially rejected the disabled zero-instance
scheduled-shutdown role, then used an empty computed root-volume ID retained
from the partial apply. Those read-only guards were corrected before any apply.
The reviewed plan already contained the correct refreshed metadata. Exactly
**one Terraform apply invocation** occurred; no failed apply was retried.

After applying, independently verified the exact disk attachment, both alarms,
event rule/target, stopped EC2, 28 managed instances, unchanged resource IDs,
persisted state and released lock. Existing-resource attributes match the
reviewed plan's refreshed values. Computed metadata was refreshed in state;
this is not an API modification of the existing AWS resources.

For this documentation/evidence checkpoint, validate JSON and links, run Ruff,
secret scanning and whitespace checks, then observe the triggered hosted CI.
No new mirror tests are added for report-only changes.

## E. Security and data correctness

Used only the MFA-backed `blockchain-demo-deploy` profile, account 209479274979,
Region us-west-2 and runtime backend `production/runtime/terraform.tfstate`.
The platform backend `production/platform/terraform.tfstate` was not accessed.
The existing 43 platform resources and adopted Snowflake grants were outside
the execution scope. No IAM expansion, Snowflake operation, secret access,
SSM command, EC2 start, filesystem formatting, image push or app deployment
occurred. Normal startup remains fail-closed on uninitialized EBS.

State serial advanced from **3 to 5**. Final state SHA-256:
`e88e8e62e9aa870696715af93b27c6f6a569c4ee182537841d0e2c7a2e8d522e`.
The lineage is unchanged. No state import, untaint, migration, forced unlock,
resource replacement, deletion or automatic apply retry was performed.

## F. Git and CI

Develop remains the working branch. Main remains
`21be18e45791a4199830754b80620d7d6599587e`. GitHub Actions is operational again.
After local report validation, checkpoint these files on develop and observe
the new CI run. The resulting commit/run is recorded in the conversation;
do not describe the prior source run as the new checkpoint's CI result.

## G. Cost implications

EC2 was not started during this step. Estimated retained gp3 storage remains
USD 10.40/month. The two standard alarms add approximately USD 0.20/month
before credits/free allowances, for an estimated disks-plus-alarms baseline of
**USD 10.60/month**, excluding requests, notifications, other usage and taxes.
Alarm charges are prorated. EventBridge/SNS activity can add usage charges.
These are estimates, not a spending cap or invoice. See the retained
[regional pricing evidence](evidence/aws_runtime_demo_costs_2026-11-13.json) and
[CloudWatch pricing](https://aws.amazon.com/cloudwatch/pricing/).

Future EC2 runtime, ECR images, secrets, logs and pilot data have separate
cost implications and approval gates. No billable streaming session began.

## H. Remaining blockers

- SSM operator access and the exact operator policy/session scope.
- Approval to initialize only the verified Kafka volume; real filesystem UUID
  and host mount checks remain unavailable while the guest is stopped.
- ECR publication permissions, exact image publication, vulnerability review
  and deployment digests for producer, Spark and mirrored AMD64 Kafka.
- Alchemy secret provisioning and exact-ARN read permissions. The current EC2
  permissions boundary still excludes Secrets Manager, so secret access needs
  an explicit IAM design/review; it cannot be enabled implicitly.
- A concrete release manifest, reviewed host installation and a separately
  approved, bounded EC2/application startup window.
- Verify an intended SNS delivery subscription before relying on human alert
  delivery; this step verified the topic targets, not subscriber delivery.

## I. Next step

Prepare the least-privilege SSM operator and image/secret permission proposals
offline, then obtain the corresponding explicit approvals. Plan any EC2 start
as a supervised timed window with verified stop capability. EBS initialization,
secret handling and application deployment remain distinct gates. The eventual
pilot writes only the isolated `11-6c-v1` S3 Bronze/checkpoint prefixes.
Do not start EC2 or format storage based on this apply result.

## J. Production infrastructure changes

**Yes: exactly the five approved runtime resources were created.** Existing
resource IDs are preserved; EC2 stayed stopped and Kafka EBS is attached but
uninitialized. No changes were made to the platform stack, Snowflake, IAM,
README, main, application containers or S3 pilot data. The native runtime lock
was released and all retained artifacts are preserved.
