# Milestone 11.6C — Exact secret-access apply verified

**The approved saved plan was applied and verified: 0 create / 1 update /
0 destroy. EC2 is stopped; application deployment is not approved.**
Milestone 11.5D remains paused.

## A. What was implemented

The owner explicitly accepted the exact four classified refresh observations and
approved one apply of saved plan SHA-256
`dc5f4ec7d9d3f425cb0308729850cbbd8ef693214f710d910c399e21f4717c7a`.
The single apply completed at **2026-11-17 21:24:41 UTC** with exit 0.
Only `aws_iam_role_policy.runtime` changed: the existing
`blockchain-runtime-least-privilege` policy on `blockchain-runtime-ec2` now allows
regional GetSecretValue for the one exact reviewed Alchemy secret ARN.

Normal runtime-state persistence includes the four accepted observations.
The original strict drift stop and later explicit owner acceptance are preserved
as separate historical records. No original failure was relabelled or erased.

## B. Why

The verified boundary v2 is a ceiling; the matching identity grant is also needed
for future runtime secret injection. This apply enables that narrowly scoped
configuration without operator secret-read permission, static credentials or a
broader workload policy. It does not establish that live secret retrieval works
through all applicable service/key/account policies: no value was requested.

## C. Files and exact artifacts

- This report and [sanitized result](evidence/aws_runtime_secret_apply_result_2026-11-17.json).
- Current-status links in the pilot and saved-plan report.
- Private single-use approval, preflight, apply log, readbacks and state snapshot
  remain outside Git; none is printed or copied wholesale into the report.
- Two deterministic offline host bundles, their external digest and verification
  receipt. No transfer, installation or startup occurred.

Terraform source remains `4ab0f2c`; the saved binary plan is unchanged. Application
images remain the exact accepted `ddc1861` ECR artifacts. README, frozen runtime
code/configuration, dependency pins and untracked duplicate files are unchanged.

## D. Tests and validation

**14 offline apply-guard checks** passed, including 11 negative cases for changed
plan/state/checkpoint, unexpected permissions, another role, disk or boundary.
Preflight reverified the actual plan binary/JSON/source/input/backend hashes,
MFA operator/account/Region, boundary v2 and sole use, role trust and policy
inventory, exact before-policy, unchanged state version, stopped instance and
the existing encrypted Kafka volume/attachment. The binary plan was reinspected
privately immediately before the one apply command.

Terraform reported **0 added / 1 changed / 0 destroyed**. Postchecks verified:

- Live inline policy equals the exact saved after-policy semantically; all
  nonsecret statements and policy identity fields are preserved.
- All **28 managed runtime resource addresses, identities and non-target values**
  are preserved in the persisted state. The four accepted observations remain.
- Boundary v2, trust, policy attachments, EC2 image/type/profile and both disk IDs
  are unchanged. Kafka remains encrypted 100 GiB gp3, attached, and not configured
  for delete-on-termination. The instance is stopped.
- Runtime state persisted normally and its native lock is absent. No manual
  unlock, automatic apply retry, extra plan or state manipulation was performed.

The frozen source's 482 focused contracts, 16 mock plans and previous six-job CI
remain valid. Validate this documentation's integrity/secret safety and observe
its new automatic CI. Tests do not imply deployment approval.

## E. Security review and network correction

The workload grant is GetSecretValue only, on the exact ARN and in us-west-2.
It does not add DescribeSecret, secret administration, operator privileges,
boundary/trust/attachment changes or any other secret. The boundary continues to
deny other secrets, administration, IAM delegation and Terraform-state access.
Shared EC2-role exposure and the previously accepted software risks remain;
one producer-only secret-file mount is not per-container IAM isolation.

**A source-network issue needs owner confirmation before launch.** The assistant
earlier instructed Ethereum Mainnet in the secret-entry guidance. That was wrong:
the frozen `runtime/contracts.py` and cloud Compose require **Arbitrum Mainnet /
Arbitrum One**, with `CHAIN=arbitrum`. The secret value has not been read, so its
actual endpoint network is unknown. No ingestion has run and no pilot data has
been written with a wrong chain label.

The owner has been asked to confirm the network only, never the URL/API key.
If the endpoint is Ethereum, request separate approval to update only this
existing secret's value privately in the Console; preserve its ARN and settings.
Do not silently change the frozen images or chain configuration. A future
approved live preflight must check the endpoint's chain identity before data writes.
No secret edit or value read is authorized by the completed IAM apply.

## F. Git and CI

Checkpoint only this sanitized documentation/evidence on develop and observe its
six-job CI. The resulting SHA/run status is reported in conversation. The prior
[CI 44176811624](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/44176811624)
passed all six jobs. Main remains unchanged; untracked duplicates stay untouched.

## G. Cost implications

This apply started no compute and created no billable runtime service. Existing
secret, EBS, ECR, monitoring and state-version storage charges can continue, as
can normal AWS/S3 request charges. The offline bundle adds no cloud usage. Fresh
cost/window review is still required before a paid instance start or demo.

## H. Remaining blockers and host artifact

Confirm/correct the secret's source network, then finish the exact release inputs
and separately approved live prerequisites: persistent mount after boot, Kafka
cluster identity, secret retrieval, image pull/identity, actual source chain,
pilot-only S3/checkpoint smoke, recovery and shutdown. EBS formatting approval is
consumed and must never be repeated.

Offline host bundle SHA-256:
`669a806f2370cc33c43296b769b9e8eddb39d939eaeb624591f637782242a627`.
Two builds were byte-identical and externally verified without extraction. All
nine host files are identical to frozen `ddc1861` source; its manifest records
the local `1625df4` checkpoint. It contains only allowlisted tracked host files
and the placeholder release example, no secret value or executable deployment
authorization. A complete concrete release manifest and paid-window proposal
must still be reviewed before transfer/startup.

## I. Next step and rollback

Await the owner's network confirmation while retaining all useful offline
artifacts. If correction is needed, request approval naming the one existing
secret before giving mutation steps. After network readiness is resolved,
prepare/review the exact bounded paid-pilot scope; do not start EC2 or deploy yet.

Any IAM rollback is a separately reviewed change using the preserved before-policy,
not a replay of the now-consumed/stale saved plan. Preserve v1/v2 boundary
versions, volumes, checkpoints, plan and state artifacts. No deletion, disk
formatting, raw state restore or main merge is included.

## J. Whether production infrastructure changed

**Yes, within the exact approved scope:** one existing workload inline policy,
normal persistence of runtime state and the four accepted observations, and
native lock creation/release. All 28 runtime identities and existing 43 platform
resources are preserved. No EC2 start, data pipeline, disk mutation, secret-value
access/edit, Snowflake action, platform-state access or main merge occurred.
Milestone 11.5D remains paused.
