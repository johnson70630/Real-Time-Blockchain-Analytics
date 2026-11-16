# Milestone 11.6C: bounded image publisher proposal

**Offline proposal; IAM expansion and image publication are not approved.**
EC2 remains stopped after the completed one-time filesystem initialization.
Milestone 11.5D remains paused.

## A. What was prepared

Prepared [ecr-publisher.proposed.json](policies/runtime-deployer/ecr-publisher.proposed.json)
as a managed-policy supplement named `blockchain-runtime-ecr-publisher`, to be
attached only to `arn:aws:iam::209479274979:role/blockchain-runtime-deployer`.
No new user/role, trust change or EC2 workload privilege is proposed.

Ten explicit actions: regional GetAuthorizationToken on `*`, and these nine
on only the two existing private repository ARNs:

- BatchCheckLayerAvailability, InitiateLayerUpload, UploadLayerPart,
  CompleteLayerUpload, PutImage.
- BatchGetImage, DescribeImages, DescribeImageScanFindings, StartImageScan.

Every statement requires `aws:RequestedRegion=us-west-2`. Repository ARNs are
`arn:aws:ecr:us-west-2:209479274979:repository/blockchain-runtime-producer` and
`arn:aws:ecr:us-west-2:209479274979:repository/blockchain-runtime-spark`.
Existing deploy-control permissions already provide exact-repository
DescribeRepositories and regional GetRegistryScanningConfiguration.

Proposed policy file SHA-256:
`605689b0836195b09e79215b234fae5e264adb9554f8e9cbcf869e9d0a9d7ac8`.
Its compact JSON is 754 characters, within the managed-policy size limit.

## B. Why

The current deployer grant provisions repositories but does not publish images.
The future runtime must pull immutable ECR digests rather than depend on the
Mac's local image cache or mutable tags. The official
[push-permission example](https://docs.aws.amazon.com/AmazonECR/latest/userguide/image-push-iam.html)
requires registry authentication separately from repository-scoped uploads.

GetAuthorizationToken lacks repository-level resource scoping. Its wildcard
does not grant upload permissions to other repositories. This supplement is
not a boundary on the entire role: existing provisioning and SSM powers remain.
IAM permits publication of other new tags in these two repositories; the
operational approval below is limited to the three reviewed artifacts.

## C. Files and source inputs

- Proposed JSON, 25 new adversarial policy cases, this report, and
  [read-only ECR metadata](evidence/aws_runtime_ecr_preflight_2026-11-16.json).
- Existing committed-source builder/planner in `runtime/images.py` and
  `scripts/build_runtime_images.sh`; no publisher execution code added here.
- Build producer/Spark from the final checkpoint's Git archive, verify AMD64,
  revision/role/non-root labels and actual Java 17/Spark 4.0.1 classpath smoke.
  Inspect the digest-pinned upstream Kafka AMD64 image separately.

Before requesting actual publication, the local review must name exactly:

| Role | Existing repository | Immutable proposed tag |
|---|---|---|
| Producer | blockchain-runtime-producer | producer-<full-reviewed-source-commit> |
| Spark | blockchain-runtime-spark | spark-<full-reviewed-source-commit> |
| Kafka | blockchain-runtime-producer | kafka-3.9.1-<full-reviewed-source-commit> |

Freeze that source revision, local image IDs and context hash in the private
review plan. Record the exact values in the conversation before approval.
No `latest`, third repository, untracked workspace context or remote builder.
Local image IDs are not ECR deployment digests.

## D. Validation and scanning

Read-only preflight at 2026-11-16 04:54:47 UTC verified both exact repositories:
IMMUTABLE, AES256 and legacy scan-on-push true. Registry scan type is BASIC,
but its scan rules are empty. Do not assume the legacy repository field proves
an automatic scan will run. Current BASIC scanning uses registry filters and
defaults unmatched repositories to manual scans; see
[AWS scanning guidance](https://docs.aws.amazon.com/AmazonECR/latest/userguide/image-scanning-basic.html).

After a separately approved upload, query DescribeImageScanFindings. If no scan
exists, StartImageScan is limited operationally to one BASIC scan of each of the
three newly published digests. Never change registry scanning configuration or
enable Inspector. If scan status is already pending/complete, do not start a
duplicate. Respect the once-per-24-hour image limit. Poll with bounded time;
missing, failed, unsupported or unresolved critical/high findings block release
approval. BASIC scans cover OS vulnerabilities, not a full application-library
security audit. Use DescribeImageScanFindings, not legacy summary fields on
DescribeImages; see [current scan API guidance](https://docs.aws.amazon.com/AmazonECR/latest/userguide/image-scanning.html).

All 25 new publisher cases pass; combined focused suites pass 180 tests.
The policy tests cover exact account/Region/repositories, missing/wrong Region,
foreign/unrelated repositories, only one wildcard action, deletion and policy
mutation exclusion, and no new unrelated service access. These are offline
contracts, not IAM simulation or validation of live installed JSON. Validate
in the IAM Console before saving; resolve errors and review warnings.

## E. Security and execution controls

The workload role/boundary stays pull-only. No Secrets Manager, EC2 start,
image/repository deletion, GetDownloadUrlForLayer, IAM administration, tag
mutability, registry/repository-policy, lifecycle or scanning-configuration
mutation is added by this supplement. Existing lifecycle expires only untagged
images; it does not clean these proposed tagged artifacts automatically.

Before publication, reverify STS/account, exact repo settings, BASIC scan mode,
source/artifact IDs and absence of conflicting immutable destination tags.
If any differs, stop. Push only the reviewed Linux/AMD64 image content and
compare the returned manifest/config digest against that reviewed image.
Capture ECR's returned digest for each role and use only digest references in
the later release. Retain successful partial uploads on failure; do not delete
images, overwrite tags or retry an unknown-result PutImage blindly.

Request ECR login only after approval using the existing MFA role. Send its
token through stdin, never command arguments/logs, and use an isolated mode
0700 Docker config with sensitive files mode 0600. Remove only that temporary
authentication material in a finally/exit cleanup; retain nonsecret receipts.
Never inherit the Mac's unrelated registry config or print raw Docker inspect,
AWS token responses, environments or debug logs. No credentials enter images.

## F. Git and CI

Keep develop, review/tests/secret scan before checkpoint, observe its hosted CI,
then build/review local images from that frozen source. The final commit and
exact private plan are reported in the conversation. Main and README unchanged.

## G. Cost implications

Offline planning and these read APIs started no compute or image storage.
Policy creation has no EC2 runtime component. Later upload incurs retained ECR
storage and possible transfer charges. AWS's example rate is USD 0.10/GB-month;
use actual compressed bytes after upload, not uncompressed local sizes, for
the resulting estimate. Tagged images persist until separately approved cleanup.
See [ECR pricing](https://aws.amazon.com/ecr/pricing/).

BASIC scanning is offered without additional scanning charges; enhanced
Inspector scanning is different and excluded. See
[AWS BASIC scanning announcement](https://aws.amazon.com/about-aws/whats-new/2024/08/new-version-amazon-ecr-basic-scanning/).
Do not assume account credits/free-tier eligibility. EC2 stays stopped during
publication; retained disks/alarms remain approximately USD 10.60/month before
other usage/taxes. Publication scope/estimates are not an unlimited budget.

## H. Remaining blockers

Administrator creation/attachment and Console validation of the proposed
policy; fresh local builds and exact artifact review; explicit permission for
upload/basic scans; actual ECR digest/scan results. Secret/workload-IAM design,
mount-on-boot validation and application deployment remain later gates.

## I. Exact next approval

Once the exact local plan is available, request approval to (1) create and
attach only the reviewed publisher managed policy to the existing human MFA
role, and (2) publish only the three reviewed image IDs/tags to these existing
repositories and perform/read their BASIC scans as above. Check for an existing
policy name first; do not overwrite an existing policy without comparison and
review. Keep all other attachments/trust/boundaries unchanged. This is neither
EC2 start nor application deployment approval. No Terraform plan/apply/import.

## J. Production infrastructure changes

None by this proposal. Only pre-existing ECR read permissions were used; no
authorization token, policy change, upload or scan start occurred. The completed
filesystem action is recorded separately and does not authorize publication.
