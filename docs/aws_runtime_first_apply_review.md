# AWS demo: partial first apply and verified normal stop

**Recovery follow-up:** the operator subsequently approved the exact-instance
untaint and recovery plan. Untaint completed; EC2 remains stopped. The original
configuration's plan was blocked by `prevent_destroy`; a local correction is
validated and its authenticated plan awaits fresh approval. See the
[current recovery review](aws_runtime_recovery_review.md). This report preserves
the original apply outcome and tainted-state snapshot.

Date: 2026-11-13 UTC. Milestone 11.6C. **First paid infrastructure attempt
partially completed; EC2 is stopped. Application deployment has not begun.**
No retry, replacement, untaint, permission update or further apply is included
in the completed window. Milestone 11.5D remains paused.

## A. What was executed

The operator explicitly approved the reviewed 28-create saved plan, immediate
paid EC2 launch and normal stop, with the administrator Console available.
After private MFA renewal, read-only preflight verified the role/session,
source/input/plan hashes, boundary, AMI, names, quota and empty runtime state.

Terraform apply started at **22:27:58 UTC** and exited 1 at **22:28:43 UTC**.
It created **23 managed resource instances**. EC2 creation reached a running
instance, then AWS denied `ec2:ModifyInstanceAttribute` while the provider was
setting `InstanceInitiatedShutdownBehavior`. There was no automatic retry.

The instance was identified from the persisted runtime state and independently
checked against EC2 ownership, tags, creation time, shape, AMI, AZ and profile.
A normal stop was requested at **22:29:23 UTC** and **stopped** was verified at
**22:30:10 UTC**, well before the approved 23:20:59 UTC deadline. Force,
hibernate and skip-OS-shutdown were false. The API stop path worked without
requiring SSM access or the administrator fallback.

## B. Why the apply stopped

The deployed policy's `InitialInstanceSafetyAttributes` statement conditions
`ModifyInstanceAttribute` on the primitive `ec2:Attribute` request parameter.
The locked provider instead sends a structured
`InstanceInitiatedShutdownBehavior={Value: stop}` request without that primitive
parameter. AWS documents value-specific keys for these structured parameters.
This request-shape mismatch is the evidence-supported explanation for the
implicit denial. The encoded authorization message was not decoded, and the
assistant cannot independently read back every installed deployment policy.

Sources: [AWS attribute condition keys](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-policies-for-amazon-ec2.html#amazon-ec2-keys),
[AWS provider 6.69.1 request construction](https://github.com/hashicorp/terraform-provider-aws/blob/v6.69.1/internal/service/ec2/ec2_instance.go#L1637).
The earlier metadata/local evaluator review missed this request-shape gap.
New offline tests reproduce it and check the narrowly scoped candidate below.

## C. Files and preserved artifacts

- This A-J review and current-status links in the pilot and launch documents.
- [Sanitized apply/stop evidence](evidence/aws_runtime_first_apply_2026-11-13.json).
- [Optional exact-instance permission proposal](policies/runtime-deployer/recovery-guest-shutdown-stop.json),
  **not deployed**. It permits only the existing instance's structured guest
  shutdown value `stop`, with the three ownership tags and Region restriction.
  It grants no start, stop, termination, user-data or protection-change action.
- Two regression/security cases in `tests/test_runtime_deployer_policy.py`.
- Updated launch-gate receipt records the completed approval and partial result.

Private artifacts remain under `/tmp/blockchain-approved-launch-i8bey7yb`:
apply logs, runtime state snapshots, stop receipts and inventory. The directory
and captured files are private; no raw state, full plan, encoded authorization
message, credentials or console output is committed or printed. The earlier
plan/approval artifacts are preserved. The original saved plan is now a
historical, partially executed artifact and **must not be reapplied**.

## D. Validation and actual inventory

Read-only AWS checks verified all 16 recorded controls, including:

- `i-0ca1671a59751acc5`: **stopped**, public IPv4 released, IMDSv2/hop limit two,
  standard credits, termination protection enabled, stop protection disabled
  and guest shutdown behavior already `stop`.
- Root `vol-03bd5d26947719f98`: encrypted 30-GiB gp3, retained with the instance.
- Kafka `vol-02c9fb14a9c568fb5`: encrypted 100-GiB gp3, **available/unattached**.
- Actual SG has no ingress and exactly the four reviewed egress rules.
- EC2 boundary is exact, only SSM managed policy is attached, and runtime inline
  policy readback matches the persisted Terraform configuration.
- Runtime state persisted at serial 2 in the runtime key; native lock released.
  Platform state was never read or operated on.

The five not-created resources are the Kafka volume attachment, two CloudWatch
alarms, EventBridge state rule and its target. The zero-count Scheduler entry
in serialized state has no instance and is not a created resource.

No filesystem/bootstrap acceptance marker was observed. Because the Kafka
attachment was blocked by the incomplete EC2 resource, this is **not** a
successful EBS initialization-required smoke test. No containers were deployed.

The offline policy suite passes **21 tests**, including SDK serialization
without credentials/network, reproduction of the old denial, and candidate
rejection of `terminate`, unrelated instances, missing ownership and unrelated
attribute/action requests. JSON/link/hash checks, Ruff and secret scanning are
run for this checkpoint; hosted CI is observed after push.

## E. Security and data correctness

AWS resource changes were limited to the approved new runtime foundation.
No platform resource, Snowflake privilege/warehouse, local development flow or
README was changed. No EBS formatting, secret retrieval, image push, streaming
writer or analytical-data write occurred. The Kafka disk remains unattached.

Terraform marked `aws_instance.runtime` **tainted** because creation failed
after allocation. A retry could request replacement; `prevent_destroy` remains
enabled. Do not remove that guard, replace/terminate the instance, edit raw
state, or reuse the consumed plan. The normal-stop evidence and current
attributes support reviewing reuse of this exact stopped instance.

The candidate policy is an offline proposal only. Do not attach it, create a
policy version or edit the deployed policies without a separate IAM approval.
It is not required automatically: the current guest shutdown value is already
`stop`, so a refreshed recovery plan may need no attribute modification.

## F. Git and CI

The applied configuration was the unchanged runtime source reviewed at
`b695f7f`, with the approval checkpoint `76cc193` clean on develop. Its
[six jobs passed](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/43517503862).
This follow-up changes documentation, an inactive policy proposal and offline
tests only. It makes no Terraform configuration change. Its commit and CI
result are recorded in the conversation. Main remains
`21be18e45791a4199830754b80620d7d6599587e`.

## G. Actual cost implications

The instance launched at 22:28:18 UTC and was confirmed stopped at 22:30:10 UTC.
Compute plus IPv4 for this brief attempt is estimated at **less than USD 0.01**,
excluding storage/other requests and taxes; this is not a measured invoice.

The **130 GiB of retained gp3 disks costs approximately USD 10.40/month**, or
USD 0.35/day using a 30-day month. The two alarms were not created, so their
previous USD 0.20/month proposal is not included in this partial inventory.
Empty repository definitions do not imply image-storage usage. S3 state
requests/versions, any logs or other service usage can add charges. Sources and
current rates remain in the [fresh cost evidence](evidence/aws_runtime_demo_costs_2026-11-13.json).
Stopping EC2 is verified; retaining disks still costs money. Cleanup requires
separate approval and was not performed.

## H. Remaining blocker and proposed recovery approval

The first apply did not complete. A separate recovery authorization is needed
for a **runtime-state-only untaint of this exact stopped instance and a fresh
authenticated saved plan**. That approval must not include apply, start,
replacement, deletion, EBS formatting or an IAM change.

Before untaint, recheck the exact instance ID against runtime state, stopped
state, ownership, attributes, source and backend. Preserve another private
state snapshot and its serial/hash; use native locking. Then propose only:

```sh
/tmp/terraform-1.17.0/terraform -chdir=infra/terraform/environments/runtime untaint \
  -lock-timeout=30s aws_instance.runtime
```

Use the reviewed MFA profile and exact existing private `TF_DATA_DIR`; the
backend key must remain `production/runtime/terraform.tfstate`. Do not manually
edit or push state. After the separately approved untaint, create a new saved
runtime-only plan with the unchanged inputs. Expect the five remaining creates
and no EC2 replacement/deletion; verify actual actions rather than assuming
that result. Retain the stopped state and inspect any proposed EC2 update
before requesting a further apply.

This staged route first checks whether recovery can finish without an IAM
expansion. If a structured shutdown write is still required, validate/review
the inactive exact-instance candidate and request its own administrator
approval. No permission change is bundled into this proposed recovery scope.

## I. Next step

Request the specific untaint-and-plan approval above. Review its result before
requesting any remaining-resource apply. A later EC2 start, SSM operator
access, exact-volume initialization, image publication, secret setup and app
deployment remain separate gates. No automatic retry or restart is scheduled.

If the demo will not continue soon, prepare a separately approved cleanup plan
for the partial runtime to end retained-storage costs. Preserve all state and
evidence until cleanup/recovery is reviewed.

## J. Production infrastructure changes

**Yes: 23 new runtime managed resource instances were created**, including EC2
and encrypted disks. EC2 was normally stopped and verified. Runtime state was
persisted under its isolated key; native locking was released. No existing
platform resource or platform state was changed. No deletion, formatting,
secret access, image push or application deployment occurred. 11.5D stays paused.
