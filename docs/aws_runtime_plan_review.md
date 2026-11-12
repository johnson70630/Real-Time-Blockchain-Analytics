# AWS demo infrastructure-only plan review

Date: 2026-11-12. **Status: authenticated plan blocked; nothing deployed.**
This remains a scheduled portfolio demo, initially around 40 runtime hours per
month. Milestone 11.5D stays paused. No README or main-branch changes.

## A. What was implemented

The runtime can now be planned with no Alchemy secret provisioned:
`approved_secret_arns` defaults to an empty list and Terraform omits the entire
Secrets Manager read statement. Exact ARNs can be opted into later, but
wildcards, other accounts and other Regions fail validation. No Secrets Manager
resource was added. The application release still requires a real approved
secret before producer startup.

## B. Why

Secret provisioning should not block an infrastructure preview or require a
fabricated ARN. Empty configuration grants no secret permissions. Later secret
creation and the corresponding IAM update remain explicit approval gates.

## C. Files changed

- Runtime `variables.tf`: empty default and exact-ARN validation.
- Runtime `iam.tf`: conditional secret-read statement.
- Runtime `tests/secret_access.tftest.hcl`: five offline AWS-mock cases.
- `Makefile`: run the mock tests in the Terraform CI job.
- Python Terraform contracts and runtime operations/readiness documentation.
- `docs/policies/runtime-plan-public-ami-read.json`: **review-only**, not attached
  or managed as a Terraform resource.

## D. Validation

Terraform formatting and isolated validation passed. Five credential-free,
backend-free mock-provider tests cover no secret access by default, exact-secret
opt-in, and rejection of wildcards, wrong accounts and wrong Regions. Python
contracts, lint and the secret scanner passed. Hosted CI is reported with the
final Git checkpoint in the conversation.

The authenticated attempt used only the runtime root, default workspace,
us-west-2/us-west-2a, and the separate
`production/runtime/terraform.tfstate` backend. Preflight confirmed AWS account
209479274979 and found the runtime state object absent. Backend initialization
succeeded using isolated local metadata. The platform backend was not initialized,
migrated or planned.

## E. Security review and blocker

Both existing AWS profiles lack `ssm:GetParameter` for this AWS-managed public
AMI parameter:

```text
/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64
```

The plan therefore failed during AMI discovery. It is **not a complete plan or
an apply-ready artifact**. Terraform saved an errored partial plan, which is
retained privately with an `INCOMPLETE` filename. Post-checks confirmed no
runtime state object or stale native lock remains. No IAM permission was expanded to bypass the error.
The read-only policy prepared for review allows exactly one action against
exactly that parameter in us-west-2. It belongs to the **planning identity**,
not the future EC2 instance role. Its attachment requires separate IAM approval
and an authorized IAM administrator. An already authorized planning identity
is another option. No secret-value retrieval was attempted.

Complete plan logs and local backend metadata remain outside Git in a
restricted temporary directory. No raw state, full plan, credential, endpoint,
secret value or external ID is included here.

## F. Git and CI

The preceding offline checkpoint is `95e5d8b83b4534ff7a5ddf7abc855a25cdd03a78`;
its six hosted CI jobs passed. This follow-up checkpoint contains only the
planning-mode change, its tests, and documentation. The conversation records
its final commit and CI result.

## G. Proposed cost and runtime

No EC2, EBS, ECR, IAM, logging or secret resources were created or modified.
Planning used the existing S3 backend's normal request/locking operations;
there is no new runtime compute charge. Existing S3 API/versioning charges can
still apply.

The inherited estimate remains approximately USD 24/month for 40 runtime hours,
not an approved spending cap or a current quote. The first release manifest
proposes one hour plus bounded shutdown/boot time. Before any apply, refresh
the region-specific rates and explicitly approve retained storage costs as
well as running compute. An apply would launch EC2 immediately, even with no
Alchemy secret and no containers started.

## H. Remaining blockers and proposed inputs

The AMI read permission is the current planning blocker. Once resolved, retry
the already approved plan with these inputs:

| Input | Proposed value |
|---|---|
| Account / Region / AZ | 209479274979 / us-west-2 / us-west-2a |
| Instance | t3.xlarge, standard CPU credits |
| Storage | 30-GiB encrypted gp3 root; 100-GiB encrypted gp3 Kafka volume |
| Pilot prefix | `blockchain-analytics/pilot/runtime/11-6c-v1` |
| Secret allowlist | empty; secret-read statement omitted |
| Scheduled stop resources | disabled; no automatic start schedule |
| Compose binary | `https://github.com/docker/compose/releases/download/v5.1.3/docker-compose-linux-x86_64` |
| Compose SHA-256 | `a0298760c9772d2c06888fc8703a487c94c3c3b0134adeef830742a2fc7647b4` |
| Expected resource count | 28 creates, configuration expectation only; authenticated plan incomplete |
| Expected updates/deletes | zero; reject unexpected changes or any platform resources |

The Compose binary was downloaded locally and its SHA-256 matched both official
release metadata and the published checksum. This version matches the local
offline validation environment. Nothing was installed on AWS. Source:
[official Compose v5.1.3 release](https://github.com/docker/compose/releases/tag/v5.1.3).

After a complete plan, deployment still requires apply/launch/spending approval,
image publication/scans and actual ECR digests, approved secret provisioning and
its exact ARN, SSM/operator access, and separate exact-volume EBS initialization
approval. Existing [pilot procedures](aws_streaming_pilot.md) cover acceptance,
recovery, shutdown, rollback, and cleanup.

## I. Next step

Review the single-parameter read policy and separately authorize its attachment
to the planning identity, or select an existing identity already permitted to
read that parameter and the runtime backend. Then retry the scoped plan and
review the complete result before requesting any apply/launch approval.

## J. Production infrastructure changes

None. Backend initialization and an unsuccessful runtime-only plan do not
deploy the proposed resources. Existing platform resources, Snowflake roles,
grants, warehouse, Streamlit, and the platform state remain unchanged.
