# Dedicated AWS demo deployment role: offline review

## A. What was prepared

The operator confirmed that the AWS Console login is the **root user** in
account `209479274979`, displayed as `JohnsonChang`, and reported no existing
deployment role found by searching IAM Roles for deploy/terraform. This is not
an inventory of every role in the account. The local planning identity cannot
list roles: AWS returned `AccessDenied` for its read-only `iam:ListRoles` call.
The operator also confirmed that `blockchain-de-local` has no assigned MFA device.

This package proposes a separate, one-hour, MFA-protected deployment role.
**No IAM changes, MFA enrollment, credential configuration, authenticated
Terraform plan, apply, instance launch, secret creation, or EBS initialization
are authorized by this document.** Root credentials will not be used by
Terraform or placed in the local AWS profiles.

| Proposed administrator-managed item | Exact name / attachment |
|---|---|
| Runtime EC2 permissions-boundary managed policy | `blockchain-runtime-ec2-boundary` |
| Deployment control-plane managed policy | `blockchain-runtime-deploy-control` |
| Deployment EC2 managed policy | `blockchain-runtime-deploy-ec2` |
| Deployment discovery and creation-tag managed policy | `blockchain-runtime-deploy-discovery-tags` |
| Deployment role, maximum session 3,600 seconds | `blockchain-runtime-deployer`, attaching only the three deployment policies above |
| Customer-managed policy attached only to existing `blockchain-de-local` | `DeFiRuntimeAssumeDemoDeployer`: MFA-protected assumption of the exact role, own-device listing, and read-only virtual-MFA inventory |
| Proposed new virtual MFA device, assigned only to `blockchain-de-local` | `blockchain-de-local-demo`, after name/inventory checks and explicit MFA approval |

The boundary policy is attached to the **runtime EC2 role**, not to the
deployment role. The deployer can create the exact runtime role only with that
boundary and cannot modify the boundary policy, detach it, change role trust,
grant itself permissions, or manage other users. Root remains the administrator
for this narrowly reviewed setup; it is not the operational deployment identity.

## B. Why

The planning user should not gain permanent provisioning privileges. An assumed
role gives the operator a separate identity, expiring credentials and an audit
trail. MFA must belong to `blockchain-de-local`; root's MFA does not satisfy
the role's trust requirement. This follows
[AWS human-access guidance](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html)
and [MFA-protected AssumeRole guidance](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_mfa_configure-api-require.html).

Restricting an IAM role's name is insufficient when the deployer can write its
permissions and pass it to EC2. The administrator-owned boundary limits the
effective EC2 role permissions even if its inline policy is mistakenly expanded.
A boundary limits grants; it grants no access itself. See
[AWS permissions-boundary semantics](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_boundaries.html).

## C. Files and exact policy documents

| File | Intended administrator action after approval |
|---|---|
| [runtime-boundary.json](policies/runtime-deployer/runtime-boundary.json) | Create the boundary managed policy above. |
| [deploy-control.json](policies/runtime-deployer/deploy-control.json) | Create `blockchain-runtime-deploy-control`. |
| [deploy-ec2.json](policies/runtime-deployer/deploy-ec2.json) | Create `blockchain-runtime-deploy-ec2`. |
| [deploy-discovery-tags.json](policies/runtime-deployer/deploy-discovery-tags.json) | Create `blockchain-runtime-deploy-discovery-tags`. |
| [trust.json](policies/runtime-deployer/trust.json) | Use as the deployment role's custom trust policy. |
| [assume-role.json](policies/runtime-deployer/assume-role.json) | Create `DeFiRuntimeAssumeDemoDeployer` as a customer-managed policy and attach only to `blockchain-de-local`. |

All JSON files are review artifacts. None is automatically attached or executed.
They contain resource identifiers and permission definitions, not credentials.
Each managed policy is below the 6,144-character non-whitespace policy limit;
the role trust document is below 2,048 characters. The user grant uses a
customer-managed policy because the existing user inline policies leave
insufficient room under IAM's aggregate 2,048-character user limit. Its JSON
permissions are unchanged; no existing inline policies are removed.

The runtime Terraform root adds optional input
`runtime_permissions_boundary_arn` and attaches it to `aws_iam_role.runtime`.
Its default is null, preserving the existing unbounded foundation behavior.
Only this exact non-null ARN is accepted:

```text
arn:aws:iam::209479274979:policy/blockchain-runtime-ec2-boundary
```

The dedicated deployer's IAM policy will reject role creation without that
boundary even if the Terraform input is accidentally omitted. A Terraform
precondition also rejects secret opt-in while this initial boundary is selected.
The administrator-owned policies and deployment role are outside both Terraform
states in this design; record their identifiers and review receipt after setup.
Do not import them into either state without a separately approved adoption plan.

## D. Validation and limits of the evidence

Local validation passed: **578 Python contracts**, **nine Terraform mocks**,
Terraform validate/format, Ruff and the repository secret scan. Public AWS
metadata checks matched all **153 explicit action entries** and their used
service/tag condition keys. Hosted CI status is recorded with the checkpoint.

Offline Python security cases check allowed runtime state/lock operations and
reject platform-state access, arbitrary role delegation, boundary removal,
administrator-policy attachment, retagging existing EC2 resources, unreviewed
AMI/instance shape, unencrypted/oversized root disks, image push, secrets,
ingress, start, termination and volume deletion. The test evaluator deliberately
covers only the policy constructs in these documents; it is **not an AWS IAM
simulator** and does not model account-level policies or API dependencies.

Credential-free Terraform mocks cover null/default behavior, exact boundary
attachment, rejection of another policy ARN, and rejection of secret opt-in
under the initial boundary. The existing five secret-scope mocks remain intact.

Action names and supported condition/resource metadata are reviewed against
AWS's public [service authorization reference](https://docs.aws.amazon.com/service-authorization/latest/reference/)
and its [machine-readable service files](https://docs.aws.amazon.com/service-authorization/latest/reference/service-reference.html).
No write API is used to test
permissions. The planning user's read-only `access-analyzer:ValidatePolicy`
attempt was denied by AWS; no additional permissions were granted. Before
setup, the administrator must inspect IAM Console policy
validation findings and review effective SCPs, boundaries, resource policies,
name collisions and existing user permissions. Local tests cannot prove the
proposed role's effective permissions or successful Terraform application.

## E. Security scope and residual risks

- Trust delegates only to the exact `blockchain-de-local` principal with MFA.
  No root CLI access, public/external principal, session-tag grant, or
  AdministratorAccess attachment is proposed.
- Deployment policies allow the isolated runtime root's initial infrastructure
  operations in `us-west-2`; IAM operations use exact global ARNs. EC2 mutations
  require the three runtime ownership tags where supported. Initial creation
  tagging is allowed only in AWS's create-action context, so the deployer cannot
  obtain access to unrelated objects simply by adding those tags afterward.
- Launch authorization pins `t3.xlarge`, `us-west-2a`, reviewed AMI
  `ami-0dac6cd09676c9fb1`, the exact instance profile, IMDSv2/hop limit 2,
  default tenancy, encrypted 30-GiB gp3 root, and encrypted 100-GiB gp3 Kafka
  disk. A changed AMI, size, AZ or boundary requires a new policy/plan review.
- The policies do not enforce a count of one instance, a monthly budget, volume
  lifetime, or an automatic stop deadline. Runtime tags do not establish
  Terraform ownership and are not a universal isolation boundary. The
  administrator must check that no unrelated existing objects share the exact
  ownership tags. EC2 multi-resource launch/network authorization must also be
  reviewed; newly allocated IDs cannot yet be pinned. `StopInstances` is limited
  to the tagged runtime. Its IAM permission does not distinguish a normal stop
  from force-stop request flags; only a normal stop is authorized by the proposed
  procedure. Start, termination, volume deletion/detach and cleanup permissions
  are outside this initial role. Credit-mode changes are also a residual
  capability of the required EC2 credit API; the reviewed Terraform uses
  standard credits, and switching to Unlimited is not approved.
- Only the runtime state key can be read/written. Only its `.tflock` can be
  deleted. Platform-state object access is explicitly denied. Bucket listing is
  limited to the default-workspace runtime key/lock prefixes; no workspace or
  backend migration is approved. Existing bucket/KMS policies can still deny
  these operations and require review.
- The runtime boundary restricts S3 objects to
  `blockchain-analytics/pilot/runtime/11-6c-v1/*`, ECR pulls to the two runtime
  repositories, and log writes to the runtime log group. Bucket-level multipart
  listing still exposes upload metadata within the analytical bucket, matching
  the existing runtime policy. Secrets Manager, IAM and role delegation are
  explicitly denied. SSM Parameter Store reads are limited to the public AMI
  parameter, rather than inheriting the broad parameter reads in
  [AmazonSSMManagedInstanceCore](https://docs.aws.amazon.com/aws-managed-policy/latest/reference/AmazonSSMManagedInstanceCore.html).
  Agent compatibility must be checked during the separately approved pilot.
- Infrastructure provisioning permissions and application-operation permissions
  are separate. This initial deployer has **no SSM StartSession/SendCommand,
  ECR image push, Secrets Manager creation/read, or S3 data-plane access**.
  The first infrastructure window can inspect the EC2 console output and stop
  the instance. Before an interactive diagnostic or streaming window, separately
  review a target-instance-specific SSM operator policy and image-publisher
  permissions. Do not grant SSM execution simply to test this role.
- Fixed-name SNS, alarm and EventBridge writes still permit changing their
  configurations. EC2 creation and the S3 lock also have effects beyond what a
  policy test can model. Approval is for the reviewed procedure and plan only;
  IAM capability is not authorization to call every permitted API.

## F. Git and CI

The validated develop checkpoint and six-job hosted CI result are recorded in
the conversation. README, main, local Docker development and application code
are unchanged. Milestone 11.5D remains paused.

## G. Cost implications

Preparing these files and running offline tests creates no AWS resources.
The proposed IAM role/policy setup itself has
[no IAM usage charge](https://aws.amazon.com/iam/faqs/); it would
grant the ability to create billable resources, so it requires explicit approval.
No access-key creation or paid identity-provider service is part of this design.
The later 40-hour demo estimate remains the
[reviewed cost model](aws_demo_launch_approval.md#g-verified-cost-model), not a
spending limit. The policy neither launches EC2 nor imposes a hard cost ceiling.

## H. Remaining blockers and approval boundaries

**Live setup update:** the operator approved the IAM/MFA setup and reported
`DeFiRuntimeAssumeDemoDeployer` attached as a customer-managed policy after the
inline attempt exceeded the existing aggregate quota. Read-only calls verified
the expected planning-user identity and both MFA inventory permissions. The
operator separately approved the temporary MFA update and completed enrollment.
Independent reads verify the exact demo device was enabled at
`2026-11-13T01:18:06Z`; the existing Authapp remains unassigned and preserved.
See the [API enrollment and verified outcome](aws_demo_mfa_api_enrollment.md).
The temporary addition still expires at the original stated deadline. The current identity cannot
read back the managed policy or inventory every role; functional verification
does not prove the complete attached-policy contents.

The operator subsequently reported all four runtime/deployer managed policies
created and an existing deployment role with matching settings. Both CLI
profiles and a Boto3 process-profile session now authenticate as that role.
The EC2 boundary's default document independently matches the repository.
The [read-only preflight report](aws_runtime_boundary_plan_approval.md) records
resource-name, quota, AMI, encryption and registry checks. IAM access setup is
complete; a fresh runtime init/plan is the next proposed approval boundary.


1. MFA enrollment for `blockchain-de-local` is complete and verified. Retain the
   authenticator entry and do not repeat enrollment. The local QR was removed
   after verification; no cloud MFA device was deleted.
2. Complete administrator policy validation/effective-permission review and
   check the proposed names are unused. Do not overwrite existing policies or
   roles. IAM setup is a separate explicit approval, covering the exact items
   in section A; it does not authorize runtime resource creation. The two MFA
   inventory reads grant no enrollment, device deletion or user-management API.
3. After approved IAM setup, configure the temporary profile and verify only
   the returned principal/account and expiry, never credential values. Source
   profile `blockchain-de` remains the planning user's existing credentials.
4. Obtain a fresh runtime-only authenticated plan with the boundary input,
   after confirming scope/approval. The older saved 28-create plan omits the
   boundary and is **not valid for this deployer design**. Keep its evidence;
   do not apply it or edit a saved plan. Source hashes have changed.
5. Repeat the existing quota/KMS/registry/AMI/stop-path preflight. Then request
   separate approval for the exact apply, immediate paid launch and supervised
   stop window. The current design still has no AWS-side hard stop timer.
6. SSM access, ECR publication/digests, secret setup/read, exact-volume
   initialization, app deployment and cleanup remain separate later gates.

## I. Setup procedure after explicit IAM approval

These steps are a reviewable runbook, not instructions to execute before approval.
The live user-policy attachment and MFA enrollment are complete and verified.
**Do not repeat Console, API or recovery enrollment below.** The
[API outcome report](aws_demo_mfa_api_enrollment.md) preserves the troubleshooting
evidence. The operator has confirmed the four runtime/deployer policies and
role settings; both temporary profiles authenticate successfully. These
completed setup steps are retained for reference and must not be repeated.

1. In the administrator Console, confirm the account and existing planning-user
   ARN. Inspect its MFA devices and the proposed names without making changes.
2. In IAM Policies, create the managed policies listed in section A using
   their exact JSON documents. Resolve validation findings before saving. Do
   not substitute a broader AWS managed policy or overwrite an existing name.
3. In IAM Roles, create `blockchain-runtime-deployer` with the custom trust JSON
   and only the three deployment managed policies. Set maximum session duration
   to one hour. The fourth policy is the future EC2 boundary and is not attached
   as a permissions policy to this deployer.
4. On `blockchain-de-local`, attach the customer-managed
   `DeFiRuntimeAssumeDemoDeployer` policy containing the exact assume-role JSON.
   Existing grants are preserved. The local profile can then
   inspect its MFA assignment and the account's virtual-MFA inventory without
   receiving any device-management permission. Before opening the enrollment
   wizard, check for the proposed name and any unassigned virtual devices.
   AWS documents that Console enrollment can remove unassigned devices. If
   any exist, stop enrollment and preserve them; their deletion is not approved.
   Prepare a separately reviewed API enrollment procedure instead. See
   [AWS enrollment behavior](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_mfa_enable_virtual.html).
5. After explicit enrollment approval and clear inventory checks, the user
   opens IAM → Users → `blockchain-de-local` → Security credentials → Assign
   MFA device. Use device name `blockchain-de-local-demo` and Authenticator app.
   Scan the QR with the user's authenticator app and enter two successive codes
   directly into AWS, then choose Add MFA. Do not save the QR/seed/codes in this
   repository or send them to the assistant. This profile's direct
   MFA-protected AssumeRole flow uses TOTP; see
   [AWS API MFA compatibility](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_mfa_fido_supported_configurations.html).
   Record only the assigned device ARN and user name. Do not modify root MFA.
6. Record policy ARNs, default versions, trust, role session limit and attach
   inventory in a nonsecret setup receipt. Review no unintended grants.
7. Add this profile manually to the user's AWS CLI config outside Git only
   after the role and MFA device are verified. Replace the placeholder with
   the existing **IAM user's MFA device ARN**, not a key or TOTP code:

   ```ini
   [profile blockchain-demo-assume]
   role_arn = arn:aws:iam::209479274979:role/blockchain-runtime-deployer
   source_profile = blockchain-de
   mfa_serial = <verified IAM-user MFA device ARN>
   duration_seconds = 3600
   role_session_name = blockchain-runtime-deploy
   region = us-west-2

   [profile blockchain-demo-deploy]
   credential_process = /opt/homebrew/bin/aws configure export-credentials --profile blockchain-demo-assume --format process
   region = us-west-2
   ```

   `/opt/homebrew/bin/aws` is the verified executable on the current Mac. Verify
   the absolute path again if the installation changes. Keep the two profile
   names distinct to avoid credential-process recursion. This follows
   [AWS's credential-process export](https://docs.aws.amazon.com/cli/latest/reference/configure/export-credentials.html)
   and the [locked AWS provider's external-process support](https://github.com/hashicorp/terraform-provider-aws/blob/v6.69.1/website/docs/index.html.markdown#using-an-external-credentials-process).
   The CLI handles the MFA role session and local CLI cache; the SDK receives
   process JSON privately. Do not run the export command directly in a terminal,
   print its response, enable credential debug logging, or save it in `.env`.

8. First run `sts get-caller-identity` with `--profile blockchain-demo-assume`
   interactively. The user enters MFA locally; only identity metadata is
   returned. Then verify the same expected assumed-role/account identity with
   `--profile blockchain-demo-deploy`, which exercises the SDK handoff. Record
   session expiry through a filtered metadata check without displaying any
   credential JSON. Use `AWS_PROFILE=blockchain-demo-deploy` for both Terraform's
   backend and provider; remove inherited access-key/session environment
   overrides and do not fall back to default/root profiles. Do not assume
   Terraform can prompt for MFA or read the CLI role cache by itself.
   Before each approved operation, check remaining session lifetime and reserve
   time to stop the instance. If renewal is needed, return to the interactive
   CLI identity check first; never remove MFA or broaden policies to make a
   noninteractive process succeed. The two profiles are now configured, and
   both identity checks succeeded; no credential values were printed or stored
   in the repository. Session credentials remain managed by the AWS CLI/SDK.
9. Stop here. No apply, launch, image push, secrets, SSM execution or formatting
   follows automatically. Expiring credentials do not stop running resources.

Rollback of IAM setup, if needed, is a separately approved administrator action:
detach the exact user managed-policy grant and role/policy attachments, and revoke any
active deployer sessions where necessary before deleting the new role/policies.
Detach the EC2 boundary only through a reviewed replacement/removal procedure;
it cannot be deleted while attached. Removing assume permission alone does not
invalidate already issued sessions. No Terraform destroy or production-state
operation is part of this IAM rollback.

## J. Production infrastructure changes

None. All new policies and role configuration are local review artifacts.
No new authenticated plan or backend initialization was run for this change.
The existing 43 platform resources, both remote state keys, Snowflake and
Streamlit were not modified. The sole Terraform source change affects an
optional EC2 role boundary, and is verified with credential-free mocks only.
