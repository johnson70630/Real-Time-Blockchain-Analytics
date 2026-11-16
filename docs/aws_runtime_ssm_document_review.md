# Milestone 11.6C: private SSM session document proposal

## Current result: approved bootstrap document verified

**2026-11-16 00:54:57 UTC:** the operator approved document creation and the
two-ARN managed-policy revision, reported updating the policy, then confirmed
document creation. Read-only verification now passes. EC2 stayed stopped.
See [sanitized verification evidence](evidence/aws_runtime_ssm_document_2026-11-16.json).
The original proposal below is retained as historical review evidence.

**Subsequent access result:** the approved local client is installed and the
live SSM shell/data channel passed. The supervised window closed with EC2
stopped at 2026-11-16 01:25:02 UTC. The separately approved later
[host recovery](aws_runtime_host_recovery_result.md) fixed Compose/Docker
enablement and ended with verified stop at 01:44:02 UTC. See the
[earlier access result](aws_runtime_access_test_result.md) for historical findings.
The separately approved [one-time Kafka initialization](aws_runtime_ebs_initialization_result.md)
is also complete with EC2 stopped; its formatting approval is consumed.

| Report item | Current result |
|---|---|
| A. Implemented | Operator-created `blockchain-runtime-demo-shell` in us-west-2; operator reports the existing SSM policy retargeted to it. |
| B. Purpose | Explicit, inspectable project shell settings for the exact demo node, without SSH ingress or changes to account defaults. |
| C. Files | This result, sanitized evidence, and current-status links in the operator and pilot reviews. Reviewed JSON files remain unchanged. |
| D. Validation | Expected STS identity; document owner/account, Active status, Session type, EC2 target and default/latest version 1 verified. Parsed live JSON exactly matches the reviewed artifact. Instance stopped before and after. All 71 offline policy/document tests, focused Ruff, repository secret scan and whitespace checks pass. |
| E. Security | No startup commands, forwarding or added IAM actions. Live policy JSON and sharing permissions were not independently read; policy update is operator-attested. Subsequent live shell/data-channel access passed; explicit resume/terminate API tests remain unperformed. |
| F. Git / CI | Documentation/evidence checkpoint on develop; hosted CI status is reported separately in the conversation. Main and README remain unchanged. |
| G. Cost | Document creation/readback started no compute. The later access window has separate costs in its result report. Retained disks and alarms remain approximately USD 10.60/month before other usage and taxes; not a spending limit. |
| H. Blockers | Client/access/Compose/Docker and one-time XFS initialization are complete. Mount-on-boot validation, image publication, secret-access design and streaming deployment remain outstanding. |
| I. Next step | Review the offline ECR publisher proposal linked from the initialization result. IAM expansion and image publication require separate approval. |
| J. Infrastructure | Document verification used only read calls. Later approved EC2/SSM, guest repair and one-time Kafka formatting are recorded in their separate results. No Terraform/state, secret, image publication or Snowflake operation occurred. |

The verified document uses a 15-minute idle timeout and 60-minute shell limit.
These limits do not stop EC2. The operator also reports increasing the human
deployer role's maximum credential duration to four hours; that IAM setting was
not independently read. The local `blockchain-demo-assume` profile requested
3600 seconds at the document inspection. The operator subsequently changed it
to 14400 seconds and renewed MFA; both the profile and sufficient cached-session
duration were checked before the access test.
Neither setting changes the EC2 workload role or the reviewed shell document.

Milestone 11.5D remains paused. The runtime/platform Terraform states were not
accessed. Do not repeat either consumed recovery apply or overwrite the document
under the earlier creation-only approval.

## Historical proposal (superseded status; artifacts retained)

Status: **offline proposal; document creation and policy revision not approved**.
The operator reported the original SSM policy attached. Read-only inspection at
2026-11-15 23:43:29 UTC verified the expected deployer identity and stopped EC2,
but `DescribeDocument` returned `InvalidDocument` for the default session
document in us-west-2. No session was opened. Milestone 11.5D remains paused.

## A. What was implemented

Prepared two versioned artifacts:

- [Private session document](../deploy/runtime/ssm-session.proposed.json):
  proposed AWS name `blockchain-runtime-demo-shell`, type `Session`, JSON format,
  target type `/AWS::EC2::Instance`, owner account 209479274979, Region us-west-2.
- [Operator policy revision](policies/runtime-deployer/ssm-operator-runtime-document.proposed.json):
  replaces only the two default-document ARN entries in the existing policy
  with `arn:aws:ssm:us-west-2:209479274979:document/blockchain-runtime-demo-shell`.

The shell uses schema 1.0 / `Standard_Stream`, a 15-minute idle timeout and
60-minute maximum session duration. No command parameters or startup commands
are included; Run As overrides are off. Required booleans follow the
[AWS session schema](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-schema.html).

## B. Why

The default document could not be inspected using the approved role. This was
**not a StartSession test** and does not prove every default-shell invocation
would fail. AWS explains when the account's saved
[default document](https://docs.aws.amazon.com/systems-manager/latest/userguide/getting-started-default-session-document.html)
is created. We propose explicit project settings rather than relying on
uninspected service fallback behavior or changing account-wide preferences.

The private document keeps the demo's configuration separate from other nodes.
The existing policy's six actions, exact instance, Region, caller ownership and
target-node checks remain unchanged. This is a replacement of document access,
not authorization for additional nodes, SSH or forwarding documents.

## C. Files and evidence

- The document and policy revision above.
- `tests/test_runtime_deployer_policy.py`: ten further security/contract cases.
- [Sanitized inspection evidence](evidence/aws_runtime_ssm_inspection_2026-11-15.json),
  this report, and current status links in the operator/pilot reviews.
- The original `ssm-operator.proposed.json` remains unchanged as the
  operator-attested attached-policy baseline; do not silently replace it.

Document file SHA-256:
`1054f84a3953852a88558e17248379635df4622910ef04356c7dce7a42da9f8e`.
Policy revision file SHA-256:
`22ae62eb4318ca4c4f1807f2565c6e05e462bb22d83a04dd086567697bc178b6`.

## D. Tests and validation

**71 offline policy/document tests pass.** Ten new cases verify that only the
two document ARNs change, the new document is allowed and the default/other
documents are excluded, caller/target session guards persist, durations have
the documented types/ranges, startup commands/parameters are absent and no
unreviewed logging/KMS destinations are introduced. Earlier 61 cases still pass.
Run Ruff, JSON/hash/link, whitespace and secret checks before checkpointing.
Observe hosted CI for the resulting develop commit.

These are offline contracts, not AWS schema validation, IAM simulation or a live
session test. AWS must validate any future CreateDocument request and the IAM
Console must validate the revised policy before it is saved.

## E. Security review and limits

The proposed document is private. No account default document/preferences,
runtime IAM role, permissions boundary, network rules or provisioning grant is
changed. The deployer receives no document creation, update or sharing
permission; the authorized administrator handles this bootstrap configuration.
The operator cannot alter the document through the proposed permission set.
Verify its owner, default version and content against the reviewed file before
any later approved session. Stop if the administrator changed its contents.

Shell access can administer the guest. An empty startup profile is not a
command sandbox: formatting, service changes and secret handling keep their
separate operational approval gates. Session timeouts do not stop EC2 or end
background work; an independent instance-stop deadline is still required.

For initial bootstrap, shell transcript destinations are deliberately empty and
no customer KMS key is selected. The encryption flags do not enable logging
without destinations, nor do they represent a configured KMS key. Session
transport uses the service's protected channel. This proposal does not disable
existing logging on another document. Full shell transcript auditing is a
remaining limitation; container CloudWatch logging is a separate runtime path.
Do not print secrets or enable CLI/plugin debug output. Do not return the raw
StartSession API response, which includes a session token, to chat or CI.

The proposed document is an administrator-managed operator bootstrap artifact,
like the separately managed deployer policies. **It is not added to Terraform
state by this proposal.** If approved, maintain its content/version receipt in
Git and operator evidence. A later Terraform adoption would require its own
review/import approval; no adoption or drift-monitoring automation is resumed.

## F. Git and CI

Work remains on develop; main is unchanged. The earlier SSM proposal commit
`2e50344cecb06ac8b439a17eee3196a40f7aadcd` passed all six
[CI jobs](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/43773977765).
Checkpoint this later proposal after local validation and record its separate
hosted result in the conversation. README, Terraform, application code and
workflows remain unchanged.

## G. Cost implications

Read-only inspection and offline preparation started no compute and created no
resource. The proposal adds no EC2 runtime, log group, KMS key, secret, NAT or
load balancer. Standard EC2 Session Manager access has no additional Session
Manager charge; future compute, logs, KMS and other usage can still be billed.
See [AWS pricing](https://aws.amazon.com/systems-manager/pricing/).
The prior disks-plus-alarms estimate remains USD 10.60/month before other usage
and taxes. These estimates are not an invoice or a spending limit.

## H. Actual state and remaining blockers

The operator's report confirms attachment of `blockchain-runtime-ssm-operator`
to `blockchain-runtime-deployer`; live IAM contents/Console findings were not
independently read back after that manual action. STS and EC2 read checks passed.
Default-document inspection returned InvalidDocument, so GetDocument and live
StartSession/channel authorization were not tested. The runtime was stopped.

The signed Apple-silicon plugin package was prepared separately (metadata
version 1.2.896.0) but is not installed. The new document/policy revision,
plugin installation, paid startup window, session opening, exact-volume
initialization, image publication, secrets and streaming deployment remain gates.
SSM Agent compatibility and a real session must be verified in a later approved
window; this proposal alone does not establish operational connectivity.

## I. Exact next approval requested

Request approval for only these two administrator actions in account
209479274979, Region us-west-2:

1. Create the **private** Session document `blockchain-runtime-demo-shell`
   from the reviewed JSON, with target type `/AWS::EC2::Instance`. First check
   whether that name exists. If it does, stop for comparison; do not overwrite,
   share it, or create another version under this creation-only approval.
2. Validate and save a new default version of managed policy
   `arn:aws:iam::209479274979:policy/blockchain-runtime-ssm-operator` using the
   reviewed revision. First compare the current policy to the original reviewed
   JSON. Retain old versions and keep the same deployer-role attachment. If the
   version limit requires deletion, stop; no version deletion is included.

The instance ARN, six actions, session ownership/target checks, trust, one-hour
role duration and all other attached policies stay unchanged. Do not attach a
second competing operator policy. If either action fails, preserve the partial
result and review it; do not delete resources, revert defaults or broaden access
automatically. No CreateDocument permission is granted to the deployer itself.

After administrator confirmation, inspect the custom document read-only and
compare owner/default version/content, with EC2 still stopped. Future session
commands must explicitly name `blockchain-runtime-demo-shell`; the revised
policy no longer allows the account-default document. Session opening and paid
startup are not part of these two changes.

## J. Production infrastructure changes

The operator reports the previously approved IAM policy creation/attachment
completed. **No further AWS resource or permission changes were performed by
the assistant during this inspection/proposal.** No custom document exists as
a result of this work. EC2 was verified stopped; no session, formatting,
Terraform backend/plan/apply/import, secret access, image push, Snowflake change
or plugin installation occurred. The 28-resource runtime state and the separate
platform stack were not accessed by these inspection calls.
