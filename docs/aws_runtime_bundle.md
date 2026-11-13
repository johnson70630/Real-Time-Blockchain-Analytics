# Milestone 11.6C: offline host deployment bundle

## A. What was implemented

Added `python -m runtime.bundle build` and `verify`. The builder reads nine
allowlisted host/controller, Compose, systemd and example-release files from
the local Git HEAD snapshot. It creates a deterministic uncompressed tar with
the source commit, per-file SHA-256 hashes and sizes. It never extracts,
installs, starts services, builds/pushes images or calls AWS.

## B. Why

The release validator already checks the eventual manifest and pinned ECR
references. The host files also need a reviewable transfer artifact. A fixed
committed snapshot avoids copying a working directory containing `.env`, local
state, logs or uncommitted edits. Normalized archive metadata makes repeat
builds of the same source byte-for-byte identical.

## C. Files changed and usage

- `runtime/bundle.py`: standard-library-only builder and verifier.
- `tests/test_runtime_bundle.py`: reproducibility and adversarial archive cases.
- This operating/report document and the pilot-readiness link.
- README, Terraform, images, dependencies and existing local entry points are
  unchanged by this work.

After reviewing and committing the source, build locally to a new path:

```sh
python3 -m runtime.bundle build --output /tmp/blockchain-host.tar
```

Record the returned commit and archive SHA-256 in the release review. Verify
from a trusted checkout using that reviewed digest:

```sh
python3 -m runtime.bundle verify /tmp/blockchain-host.tar --sha256 REVIEWED_SHA256
```

`REVIEWED_SHA256` must be the actual hash returned and independently retained
at review, not a placeholder or an untrusted replacement archive's own claim.
Verification performs no external commands and no extraction. A trusted copy
of the verifier is required; do not execute code extracted from an unverified
archive. After a separately approved transfer, compare the host archive's
SHA-256 with the reviewed value before any separately approved installation.

## D. Tests and validation

The bundle suite passes **17 tests**. Combined with the existing runtime pilot
contracts, **47 tests passed**, with one existing dependency deprecation warning.
Cases cover work-tree/untracked-file exclusion, inherited Git override rejection,
source symlinks, overwrite refusal, external digest enforcement, tampering,
extra/missing/duplicate files, unsafe links/paths, duplicate JSON keys, hidden
trailing bytes and size limits. Ruff and secret scanning passed.

After the local Git checkpoint, build two actual source bundles, compare their
hashes, and verify one using the CLI. Record artifact location/hash in the
conversation; the archive stays outside Git. Hosted CI will discover the new
tests through the existing `ci-python` target after service access is restored.

## E. Security review

Only regular allowlisted committed blobs are packaged; no repository-wide
copying, Git metadata, credentials, Terraform files/state, data or logs are
included. Run the repository secret scan and review the source before packaging:
an allowlist/checksum is not a secret detector or a vulnerability scan.
The builder disables Git transports/lazy fetch and strips inherited Git/AWS
overrides. Missing local objects fail closed. Existing artifacts are never
overwritten, and new archives are mode 0600.

The verifier requires an external SHA-256, bounds archive/member sizes, rejects
unexpected members and JSON keys, checks every file, and enforces canonical
archive bytes. SHA-256 establishes integrity against a trusted reviewed hash;
it is not code signing or an authorization to deploy. The example release
retains placeholders; real ECR digests, secret access and filesystem identity
are still required before application startup.

## F. Git and CI

Create a local develop checkpoint after validation. Do not push or repeatedly
rerun hosted CI while the known account verification/billing lock is pending.
The local commit and smoke results are recorded in the conversation; push and
hosted validation remain deferred until access returns. Main remains untouched.
This defers the blocked hosted operation, not the local implementation/tests.

The approved five-resource Terraform plan remains a separate artifact; no
runtime Terraform source or input was changed by this bundle work.

## G. Cost implications

No new AWS or GitHub paid usage was initiated. Local tests/builds use the Mac;
the future runtime still executes on EC2 after separate deployment approval.
Existing retained AWS disks continue costing approximately USD 10.40/month.

## H. Remaining blockers

GitHub account verification and hosted CI remain pending. The five-resource
AWS apply, EC2 start, SSM operator access, exact-volume initialization, image
publication/digests, secret provisioning/access and app deployment retain their
existing approval gates. A host bundle does not complete those prerequisites.

## I. Next step

When verification completes, push the reviewed local checkpoint and observe
one new CI run rather than repeatedly retrying blocked runs. Then return to
the [five-resource apply proposal](aws_runtime_recovery_plan_result.md).
Use the bundle only after a later explicit file-transfer/deployment approval.

## J. Production infrastructure changes

None. No authenticated AWS call, Terraform operation, image push, EC2 action,
filesystem initialization, secret access or Snowflake operation occurred.
Milestone 11.5D remains paused.
