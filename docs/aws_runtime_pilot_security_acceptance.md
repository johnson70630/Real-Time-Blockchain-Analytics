# Milestone 11.6C — Conditional security acceptance and secret proposal

Current next step: [exact Alchemy secret-access review](aws_runtime_secret_access_review.md). The owner reported the approved secret created and confirmed its metadata settings. IAM changes, production Terraform actions and deployment remain unapproved.

The prior acceptance/proposal notes below are preserved as checkpoint history.

**The owner accepted the exact one-run security exception. Deployment is still
blocked on credential/IAM readiness and separate paid-runtime approval.**
Milestone 11.5D remains paused.

## A. What was implemented

Recorded the owner's explicit `ok approve` decision against scope SHA-256
`5654f4f255730e9ab7838b8219d8148102c5417861e2c8db428a88d41e7c665e`.
The [acceptance record](evidence/aws_runtime_security_acceptance_2026-11-17.json)
binds all 41 advisory IDs, affected occurrence inventories, scan hashes and the
three verified ECR manifests. Earlier pending proposals remain preserved as
historical snapshots; their false acceptance flags are not rewritten.

Maximum: one supervised public-blockchain demonstration, a streaming duration
of at most 3,600 seconds, expiring **2026-11-24 00:00:00 UTC**. No run has occurred.
New findings, changed artifacts/scope or unmet controls invalidate this exception.
The exception does not patch software or authorize AWS resource changes.

## B. Why

This makes the risk decision explicit and reviewable before cloud execution.
Vulnerable code, if reached under its exploit prerequisites, could interrupt the
pipeline or compromise a process. A compromised process could expose credentials
available to it and use its permitted S3 access to alter pilot data/checkpoints.
All containers share the EC2 role trust boundary; per-container IAM isolation is
not provided. API/request/storage misuse could increase costs. The one-hour
duration limits exposure but is not a guaranteed billing cap.

The scanner's critical Netty finding concerns a TLS-server configuration absent
from the inspected client-only pilot. Other findings include client-side paths
and native libraries, and every transitive path has not been proven unreachable.
The [full review](aws_runtime_security_decision.md) and
[ECR reconciliation](aws_runtime_quarantined_publication.md) retain original
severities, prerequisites and uncertainty. No not-affected claim is issued.

## C. Files changed

- This acceptance report and concrete next secret proposal.
- The sanitized acceptance JSON linked above.
- Status links in the pilot, consolidated security and publication reports.

No runtime source, Docker image, dependency lock, Terraform file or README changes.

## D. Tests and validation

Verify the exact approved scope hash, publication-evidence hash, 41-ID inventory,
three manifest/config/scan bindings, unexpired one-run limits and preserved
required controls. Reject any accidental execution/secret/IAM authorization.
Run Ruff, repository secret scanning and whitespace checks before checkpointing.
The frozen image source remains `ddc1861`; its 460 focused contracts, image
smokes, privilege audits and recovery evidence remain applicable. Publication
checkpoint `3235aee` passed all six jobs in
[CI 44000558469](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/44000558469).
Observe this documentation checkpoint's own automatic CI after push.

## E. Security review and next credential design

The next proposed resource is exactly one **owner-entered Secrets Manager secret**:

| Setting | Proposed value |
| --- | --- |
| Account / Region | `209479274979` / `us-west-2` |
| Name | `blockchain-runtime/11-6c/alchemy-websocket-url` |
| Type / payload | Other type, Plaintext tab; one complete Alchemy WSS URL string |
| Format | Starts with `wss://`; no JSON wrapper, quotes, key/value mapping or newline |
| Encryption | AWS-managed `aws/secretsmanager`; no customer-managed KMS key |
| Rotation / replication | Both off for this initial demo |
| Resource policy | None added |
| Tags | `Project=Real-Time-Blockchain-Analytics`, `Milestone=11.6C` |

**Creation is not approved by the security acceptance.** Obtain separate approval
before the owner stores it in the Console. Use an existing identity authorized to
create the secret; do not expand the deployer role or request broad administrator
policies. If the name already exists, stop and review its metadata/ARN rather
than overwriting, replacing or reading its value. If permission is missing, stop
that action and review the exact creator permissions separately.

The owner enters the value directly in the AWS Console, with assistant screen
access/sharing/recording inactive, and returns **only the ARN**. The assistant
never receives the URL/key or uses GetSecretValue during provisioning review.
Do not enter the value in Git, Terraform, a local .env, CLI arguments/history,
logs, tags, screenshots or chat. The Console's Plaintext tab is an input format;
Secrets Manager encrypts the stored value. See
[AWS creation guidance](https://docs.aws.amazon.com/secretsmanager/latest/userguide/create_secret.html).

AWS can automatically create the AWS-managed `aws/secretsmanager` key on first
use if absent; that key has no additional key charge. No custom KMS key or key
policy change is proposed. See
[encryption guidance](https://docs.aws.amazon.com/secretsmanager/latest/userguide/security-encryption.html).

After the exact ARN is available, prepare a separate workload policy/boundary
review for `blockchain-runtime-ec2`, allowing only GetSecretValue on that ARN.
The current boundary explicitly denies Secrets Manager and Terraform intentionally
rejects nonempty secret input while that boundary is selected. Merely adding an
Allow statement cannot override an explicit Deny. Preserve the state-bucket deny,
other privilege limits and both Terraform states. No IAM edit, backend init,
authenticated production plan, apply or import is authorized here.

The secret is initially a Console-provisioned resource outside existing Terraform
state. Any later metadata adoption is a separately reviewed import/change; its
secret value must never be placed in Terraform state.

## F. Git commit and CI

Checkpoint this documentation/evidence on develop after validation and observe
its CI. Report the resulting SHA/status in the conversation. Uploaded image
identities remain frozen; this checkpoint does not rebuild or republish them.
Main remains unchanged.

## G. Cost implications

Recording acceptance creates no cloud usage or additional resource. If separately
approved, one Secrets Manager secret is approximately **USD 0.40/month**, plus
**USD 0.05 per 10,000 API calls**, using
[AWS pricing examples](https://aws.amazon.com/secrets-manager/pricing/).
Actual charges depend on retention, usage, credits and taxes; this is not a cap.
No rotation Lambda, replica, customer-managed key or EC2 runtime is proposed.
Retained EBS, ECR, monitoring and other existing resources can continue charging.

## H. Remaining blockers

Secret-creation approval and owner entry, its reviewed ARN, exact workload
permission/boundary review, separately approved production Terraform scope,
fresh live preflight, bounded EC2 start/deployment approval and cloud smoke/
recovery/stop evidence. Never format the existing initialized Kafka volume.

## I. Next step

Request approval for the single secret described above, including its recurring
cost and secure owner-controlled entry. Provide Console steps after approval.
Do not ask for the credential value. Once its ARN is known, prepare exact IAM
changes offline before requesting their separate approval.

## J. Whether production infrastructure changed

**No changes in this checkpoint.** No AWS API call, secret creation, IAM edit,
Terraform operation, EC2 start, deployment, S3 write or Snowflake change occurred.
The prior three-image ECR publication is preserved. No main merge. 11.5D stays paused.
