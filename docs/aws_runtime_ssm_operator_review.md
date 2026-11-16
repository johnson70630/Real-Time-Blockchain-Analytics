# Milestone 11.6C: SSM operator access proposal

**Status update, 2026-11-16 UTC:** the operator reports the approved policy
attached and its document references retargeted. The separately approved private
document now exists in us-west-2; read-only verification confirms its owner,
version and exact reviewed content with EC2 stopped. See the
[private-document result and historical proposal](aws_runtime_ssm_document_review.md).
Live IAM policy JSON remains independently unread. Subsequent live SSM shell
and data-channel access passed; the approved window closed with EC2 stopped.
See the [access result and host-recovery proposal](aws_runtime_access_test_result.md).
The later separately approved [Compose/Docker recovery](aws_runtime_host_recovery_result.md)
is also complete with EC2 stopped. The later approved
[one-time Kafka initialization](aws_runtime_ebs_initialization_result.md) is
complete; reuse its recorded UUID and never reformat on recovery.
The operator reports a four-hour maximum role duration; the local assume profile
now requests four hours and a fresh MFA session was checked before access.
The original offline proposal below is retained as historical review evidence;
its six-action policy file remains unchanged. Milestone 11.5D remains paused.

## A. What was implemented

Prepared [ssm-operator.proposed.json](policies/runtime-deployer/ssm-operator.proposed.json)
as a separately approved supplement to the existing MFA deployer role:

| Item | Exact proposed scope |
|---|---|
| Account / Region | `209479274979` / `us-west-2` |
| Managed policy name | `blockchain-runtime-ssm-operator` |
| Policy ARN | `arn:aws:iam::209479274979:policy/blockchain-runtime-ssm-operator` |
| Attachment target | `arn:aws:iam::209479274979:role/blockchain-runtime-deployer` |
| Node | `i-0ca1671a59751acc5` only |
| Session document | Account-owned `SSM-SessionManagerRunShell` in us-west-2 only |
| Session management | Only sessions tagged by AWS with the caller's user ID and that exact node |

Six explicit API actions: `ssm:StartSession`, `ssm:GetDocument`,
`ssm:DescribeDocument`, `ssm:ResumeSession`, `ssm:TerminateSession`, and
`ssmmessages:OpenDataChannel`. Every statement checks the requested Region.
The session ARN wildcard covers generated session IDs inside this one account
and Region; both owner and target conditions are also required.

The file SHA-256 is
`ff1a510ce202d04f2df272c43adef615923f5a2a63053bbb2e145fedf307ad00`.
Its canonical JSON SHA-256 (sorted keys, compact separators) is
`a9709816a91377bffe625185b9e92848f73ac6816d1b075542052fb6aefaa49a`.
This supersedes the earlier private five-action draft by adding document
metadata inspection. Neither draft has been attached.

## B. Why

The EC2 workload role already has SSM Agent permissions. The human operator
still needs client-side authorization to administer this specific machine.
SSM provides an administration path without SSH or security-group ingress.

For this single-person demo, propose adding a separately reviewed capability
to the existing temporary MFA role, retaining its trust and one-hour role
session configuration. This combines provisioning and host administration in
one human identity. Existing provisioning powers remain; the supplement is
not a boundary that limits the whole role to six actions. A separate operator
role remains the stronger isolation option for a multi-user deployment.
The workload instance profile remains a distinct identity.

AWS documents owner/target session tags for IAM users and assumed roles. Using
those tags avoids treating a role-session user ID as a session-name prefix.
See [AWS session access examples](https://docs.aws.amazon.com/systems-manager/latest/userguide/getting-started-restrict-access-examples.html).

## C. Files changed

- The proposed JSON policy; initial provisioner/trust/boundary files remain intact.
- `tests/test_runtime_deployer_policy.py`: caller-variable handling in the small
  offline evaluator and 40 additional adversarial policy cases.
- This A-J proposal and its pilot-readiness link.

No Terraform, runtime application, root Compose, dependency, README or workflow
changes. No production backend initialization, plan, apply or state change.

## D. Tests and validation

**61 policy tests pass**, including 40 new cases. They exercise the exact node
and document, wrong node/account/Region, unauthorized SSH/port-forwarding/custom
documents, wrong or missing session owner/target tags, role and user IDs,
document reads, and exclusion of unrelated API actions from the supplement.
Existing MFA trust, provisioning, boundary and state-isolation cases still pass.
Ruff, JSON, link, secret and whitespace checks are required before checkpointing.

These tests model only the operators used by these files. They are **not** an
AWS IAM simulator, Access Analyzer validation, a live authorization test or a
command-level sandbox. Account controls and live policy validation remain gates.
Hosted CI for this exact checkpoint is recorded in the conversation.

## E. Security review

Start authorization is restricted to the exact node and default shell document.
The document's name must be allowed independently when AWS authorizes the
session. Other SSH, forwarding and interactive-command documents are absent.
See [AWS document authorization](https://docs.aws.amazon.com/systems-manager/latest/userguide/getting-started-specify-session-document.html).

The role's existing trust requires `blockchain-de-local` and MFA; this proposal
does not modify trust, source-user permissions, maximum role duration, runtime
role or its permissions boundary. Ownership uses `${aws:userid}` plus AWS's
target-node tag. Fixed role-session names identify the same role session owner
across credential renewals; this is not per-MFA-login isolation. The trust must
remain restricted to the reviewed user.

**An SSM shell can administer the guest**, including reading workload-accessible
data, altering services, or modifying disks through guest privileges. This IAM
policy does not prevent those commands. Approval to attach the policy does not
authorize EBS formatting, secret handling or app changes. Those operational
gates remain explicit. No direct EC2 start, SendCommand, IAM modification,
secret, ECR push or S3 action is added by this supplement.

The IAM permissions of all attached policies combine. No broad console discovery
permissions are proposed; this is a CLI-focused operator path. Do not attach it
to the EC2 workload role, source IAM user, or an unrelated role. Do not grant
administrator access or enable SSH to work around a denied request.

## F. Git and CI

Checkpoint on develop after local security checks and observe hosted CI. Main
remains `21be18e45791a4199830754b80620d7d6599587e`. The new policy is a versioned
proposal, not Terraform-managed IAM and not automatically attached by CI.
The earlier [runtime-completion CI](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/43772856294)
passed all six jobs; it does not validate this later checkpoint.

## G. Cost implications

Offline work starts no cloud resources. IAM policy preparation and attachment
do not start compute. Standard Session Manager access to EC2 has no additional
Session Manager charge; EC2 runtime, optional session logs/S3/KMS and other
usage can still be billed. See [Systems Manager pricing](https://aws.amazon.com/systems-manager/pricing/).
The retained disks-plus-alarms estimate remains USD 10.60/month before other
usage/taxes. No future compute spending is approved by this policy proposal.

## H. Remaining blockers

1. Explicit approval for creating this exact managed policy and attaching it
   only to the named deployer role. Existing deployment credentials cannot
   self-expand their role; use an already authorized IAM administrator Console
   session, without giving the assistant new permanent credentials.
2. IAM Console/Access Analyzer validation and administrator readback are pending.
   Review unresolved errors/warnings before creation; do not infer AWS approval
   from passing offline tests. If that policy name already exists, stop and
   compare rather than overwriting or creating a new default version.
3. AWS CLI v2 is available locally. The Session Manager plugin is absent from
   PATH and the three usual installation locations checked on 2026-11-15.
   Installation is a separate local system change. Use the official signed
   Apple-silicon installer, verify its signature/version, and never send a Mac
   password to chat. See [macOS installation](https://docs.aws.amazon.com/systems-manager/latest/userguide/install-plugin-macos-overview.html).
   AWS currently requires plugin 1.2.764.0 or later; recheck before installing
   rather than treating this minimum as a version pin.
   See [plugin requirements](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html).
4. The default document's existence, owner, type, version, Run As behavior,
   idle/max duration, KMS settings and log destinations need read-only inspection
   after permission approval, while EC2 remains stopped. Capture content only
   in a private file and report selected non-secret fields. Session preferences
   can add permissions or costs; do not silently broaden this policy.

AWS creates the default document when Session Manager preferences are configured.
Its presence has not been verified here. If missing or incompatible, stop and
prepare a separately approved document/preferences proposal.
See [default document behavior](https://docs.aws.amazon.com/systems-manager/latest/userguide/getting-started-default-session-document.html).

## I. Exact next approval and procedure

Request approval for **only creating `blockchain-runtime-ssm-operator` from the
reviewed JSON and attaching it to `blockchain-runtime-deployer`**, with the
existing trust, duration and other policies unchanged. This expands IAM
privileges and therefore requires the user's standing production approval gate.
It does not approve plugin installation, EC2 startup, opening a session,
formatting, secrets, image publication, app installation or a Terraform action.

After approval, the authorized administrator can use IAM → Policies → Create
policy → JSON, review validation findings, create with the exact name, then
attach only that customer-managed policy under the named role's permissions.
Verify the account, policy ARN/document and attachment target. Preserve existing
policies. Do not use root access keys, the default pipeline profile, or new
administrator credentials for this task.

After administrator confirmation, use the existing MFA profile for identity and
read-only default-document inspection. Do not call StartSession as a test while
the instance is stopped. Plugin setup and a supervised, separately approved
startup window follow after prerequisites are satisfied. Closing or terminating
SSM does not stop EC2; credential expiry is also not an instance shutdown timer.
Retain the independent stop path and runtime deadline requirements.

Rollback after an approved attachment is to remove only this new attachment
under an explicitly approved IAM rollback scope. Detachment is not a substitute
for terminating an already open session. No rollback or policy deletion is
performed now.

## J. Production infrastructure changes

**None.** No authenticated AWS operation, IAM policy attachment, document
creation, instance action, session, storage operation, secret access or
Snowflake change occurred. The runtime remains last verified at 28 managed
resources with stopped EC2 and attached, uninitialized Kafka EBS. README,
platform resources, main and Milestone 11.5D are unchanged.
