# AWS demo: successful recovery plan and remaining-resource proposal

Date: 2026-11-13 UTC. Milestone 11.6C. **Approved revised-config planning
completed. No apply or EC2 start performed.** Milestone 11.5D remains paused.

## A. What was completed

After explicit approval for the public-IP correction and private MFA renewal,
generated and reviewed the saved recovery plan at 23:14:52 UTC. The result is
**5 creates, 23 no-ops, zero updates/deletes/replacements/imports**. All 14
checks passed. EC2 stayed stopped; runtime state remained byte-for-byte
unchanged at serial 3, and the native lock was released.

The prior automatic-review rejection was resolved by the operator's fresh
approval. Neither untaint nor the earlier failed apply was repeated.

## B. Why

The revised plan confirms that removing the duplicate instance public-IP pin
avoids replacing the stopped instance. Subnet launch auto-assignment and
`prevent_destroy` remain configured. Existing guest shutdown and termination
protection values already match the configuration, so EC2 and IAM are no-ops;
the inactive shutdown permission proposal is unnecessary for this plan.

## C. Files and artifacts

- This A-J result and [sanitized evidence](evidence/aws_runtime_recovery_plan_2026-11-13.json).
- Updated recovery/readiness links and gate receipt; no further Terraform,
  application, test or CI configuration change in this checkpoint.
- Saved binary, JSON, logs, authorization receipt and state snapshots remain
  private under `/tmp/blockchain-runtime-recovery-proposal-nh1hf6gu/revised-config`.
  Full plan/state and credentials are not committed or printed. Earlier failed
  artifacts remain preserved and invalid for deployment.

## D. Tests and validation

The exact planned source is `c5333ef2e5d467cedc61f7b76e212ae2b39eeb8d`, whose
[six CI jobs passed](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/43522856650).
Its local checks passed 37 focused tests, nine credential-free Terraform mocks,
format/validate, Ruff and secret scanning.

Private plan validation confirmed the exact five-create allowlist and 23
existing no-op resources, AWS provider only, no imports, no EC2/IAM mutation,
and all 14 checks passing. This documentation/evidence checkpoint validates
JSON, links, hashes, whitespace, lint and secrets, then observes hosted CI.

## E. Security and data correctness

The Kafka attachment targets only instance `i-0ca1671a59751acc5` and existing
encrypted volume `vol-02c9fb14a9c568fb5` at `/dev/sdf`. It does not format or
mount a filesystem. The two alarms monitor only that instance and publish to
the existing runtime SNS topic. The EventBridge rule filters that exact
instance's stopped/terminated notifications and targets the same SNS topic.

The locked provider's attachment-create path only attaches and waits; its
instance-ready waiter explicitly accepts `stopped`. This operation does not
require starting EC2. Sources:
[attachment create implementation](https://github.com/hashicorp/terraform-provider-aws/blob/v6.69.1/internal/service/ec2/ebs_volume_attachment.go#L78),
[ready waiter](https://github.com/hashicorp/terraform-provider-aws/blob/v6.69.1/internal/service/ec2/wait.go#L2783).

The existing instance, disks, network, runtime role/boundary and policy are
no-ops. Plan checks retain IMDSv2, standard credits, termination protection,
encrypted disks, no ingress, empty secret allowlist and isolated S3 prefix.
There is no Scheduler/start action, SSM execution, image push or app operation.
No data pipeline or Snowflake resource was changed.

## F. Git and CI

Develop is the working branch; main remains
`21be18e45791a4199830754b80620d7d6599587e`. README and the 43 platform resources
are unchanged. The follow-up checkpoint and hosted CI result are recorded in
the conversation.

Configuration plus lockfile SHA-256:
`6dd1bb8ebb3de1d89600a4159701548af3c918a570f15629b05d264b9abe9712`.
Saved plan SHA-256:
`4a859fa4ff87bd7e0098727de829679eecd95ca6106bb3f45153668ed64e7382`.
Only this successfully reviewed plan may be considered for the next approval.

## G. Cost implications

Planning created no resources or compute runtime. Existing gp3 retention remains
approximately USD 10.40/month. A future approved apply adds two standard alarms
at approximately **USD 0.20/month**, making disks plus alarms approximately
**USD 10.60/month**, before service requests, other usage and taxes. The
attachment reuses the existing disk. EventBridge/SNS usage can add charges.
Rates and exclusions are in the [fresh cost evidence](evidence/aws_runtime_demo_costs_2026-11-13.json).
These estimates are not an invoice, spending cap or apply authorization.

## H. Exact remaining-resource apply approval requested

Request approval for **only applying this reviewed five-create saved plan**:

| Terraform address | Proposed action |
|---|---|
| `aws_volume_attachment.kafka` | Attach the existing Kafka volume to the exact stopped instance |
| `aws_cloudwatch_metric_alarm.high_cpu` | Create runtime CPU alarm |
| `aws_cloudwatch_metric_alarm.instance_status` | Create runtime status alarm |
| `aws_cloudwatch_event_rule.runtime_state` | Create exact-instance state notification rule |
| `aws_cloudwatch_event_target.runtime_state` | Point that rule to the existing runtime SNS topic |

EC2 must remain stopped. No start, replacement, deletion, IAM change, EBS
formatting, secret operation, image publication or application deployment is
included. Before execution, verify the fresh identity/session, clean reviewed
source, exact plan/config/input hashes, unchanged runtime state and absent lock.
Retain a private state backup and capture all output privately. Environment is
profile `blockchain-demo-deploy`, Region us-west-2, default workspace, existing
private `TF_DATA_DIR=/tmp/blockchain-runtime-boundary-plan-qo31l9uu/terraform-data`;
backend key must remain `production/runtime/terraform.tfstate`.

Proposed command, **not executed**:

```sh
/tmp/terraform-1.17.0/terraform -chdir=infra/terraform/environments/runtime apply \
  -input=false -no-color \
  /tmp/blockchain-runtime-recovery-proposal-nh1hf6gu/revised-config/runtime-recovery.tfplan
```

On failure, preserve state/evidence and review rather than retrying, replacing,
deleting, force-unlocking or expanding permissions automatically. After a
separately approved apply, verify the exact volume attachment, two alarms,
rule/target, runtime state persistence, released lock and stopped EC2. A success
does not authorize a later start or filesystem initialization.

## I. Next step

Obtain the five-resource apply approval above. After the foundation completes,
separate gates still cover SSM operator access, exact-volume initialization,
image publication/scans/digests, Alchemy secret and exact-ARN access, EC2 start
and application deployment. The actual streaming demo has not run.

## J. Production infrastructure changes

None from this successful planning step. Runtime state stayed at serial 3 with
the same content hash; only the native planning lock was acquired/released.
The original 23-resource partial runtime remains, with EC2 stopped and Kafka
unattached. Platform state was not accessed; no AWS resource was created or
modified, and 11.5D remains paused.
