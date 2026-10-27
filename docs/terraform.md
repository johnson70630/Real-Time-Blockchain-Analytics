# Terraform foundation

## Scope and ownership

Terraform uses two deliberately separate root modules:

- `infra/terraform/bootstrap/state` manages only the dedicated Terraform state bucket. Its state remains local, access-restricted, and separately backed up so the backend does not depend on itself.
- `infra/terraform/environments/prod` manages adopted production infrastructure through the S3 remote backend.

The project has one real environment, represented by the `prod` root and Terraform's default workspace. Additional workspaces are not used as environments.

## Existing infrastructure inventory

| Category | Current infrastructure | Classification | Adoption boundary |
|---|---|---|---|
| AWS | Production analytical-data S3 bucket | A — Terraform managed | Five bucket resources were imported without recreation and have a verified zero-change plan. |
| AWS | Snowflake S3 integration IAM role and policy | A — Terraform managed | Trust, external-ID condition, least-privilege policy, and attachment were preserved during import. |
| AWS | Local named-profile credential resolution | B — externally managed | Remains in standard AWS shared configuration, outside Terraform source and state. |
| Snowflake | Database, schemas, warehouse, roles, service user, grants, CONTROL tables, storage integration, stage, and file format | A — Terraform adoption candidates | Inspect and import selectively in 11.5C. Account bootstrap and credentials remain external. |
| Hosted app | Streamlit Community Cloud application | B — managed externally | Continues tracking `main`; no provider is introduced in this milestone. |
| Local runtime | Kafka, Spark, Airflow, and the Airflow PostgreSQL metadata database | C — local development only | Docker Compose and local process management remain authoritative. |
| Runtime platform | Future continuously hosted producers, Spark, Kafka, and Airflow | D — deferred until 11.6 | Deployment architecture must be approved before any Terraform resources are modeled. |

Classification does not imply that every resource will be adopted. Each later milestone must confirm ownership, lifecycle, and safe import behavior independently.

## Versions and authentication

Both roots require Terraform `>= 1.10.0, < 2.0.0`. Terraform 1.10 is the minimum because it introduced native S3 lockfiles. CI uses an explicit supported Terraform 1.x release.

Provider constraints are deliberately bounded to compatible minor lines:

- `hashicorp/aws ~> 6.69.0`
- `snowflakedb/snowflake ~> 2.22.0`

AWS authentication uses the standard provider credential chain. Local operators select an approved shared profile with `AWS_PROFILE`; CI validation needs no AWS credentials. The existing Snowflake storage-integration external ID is retrieved through an authorized IAM read and supplied only through the sensitive `TF_VAR_snowflake_storage_external_id` process environment. Credentials, external IDs, private keys, and passwords must never be stored in Terraform source, backend configuration, plans, logs, or committed variable files.

## Remote state design

Terraform state does not share the production analytical-data bucket. Production state uses:

```text
bucket: blockchain-analytics-terraform-state-209479274979-usw2
key:    production/platform/terraform.tfstate
region: us-west-2
```

The dedicated bucket is provisioned with versioning, SSE-S3 `AES256` encryption, all four public-access protections, bucket-owner-enforced ownership, retained noncurrent state history, and Terraform `prevent_destroy`. Native S3 locking uses `<state-key>.tflock`; no DynamoDB table is involved.

The least-privilege backend policy requires:

- `s3:ListBucket` restricted to the exact state and lockfile keys.
- `s3:GetObject` and `s3:PutObject` on the state object.
- `s3:GetObject`, `s3:PutObject`, and `s3:DeleteObject` on the lock object.

The operator does not require `s3:DeleteObject` on the state object or backend-policy access to analytical-data objects. Backend credentials come from the normal AWS credential chain. The version-controlled `backend.hcl.example` contains only non-sensitive settings; the real `backend.hcl` is local and ignored.

## Bootstrap and adoption status

The state bucket was created from the isolated bootstrap root. Its Terraform state remains local and separately backed up; it must not be moved into the production state that depends on the bucket.

The production root was initialized against S3 with native locking. Eight existing resources were then imported without recreation:

1. Production S3 bucket
2. S3 versioning configuration
3. S3 server-side encryption configuration
4. S3 public-access block
5. S3 ownership controls
6. Snowflake S3 IAM role
7. Snowflake S3 managed policy
8. IAM role-policy attachment

The import preserved the bucket, IAM trust principal, external-ID condition, managed-policy permissions, and attachment. No production resource mutation was required. A full refresh-enabled plan on 2026-10-26 returned exit code `0` with `0 add, 0 change, 0 destroy` and no detected drift.

## Import-first production adoption

Existing production infrastructure is never recreated merely to place it under Terraform management. For each AWS or Snowflake object in 11.5B or 11.5C:

1. Inspect and record the live configuration.
2. Write a matching Terraform resource definition.
3. Import the existing object into the appropriate state.
4. Run a refresh-only review and a normal plan.
5. Reconcile configuration until the plan is zero-change.
6. Only then consider Terraform authoritative for that object.

No unmanaged or speculative apply is permitted. Imports and applies require an explicitly approved milestone and operator review. The AWS resources above completed this workflow and are now authoritative in the production state.

## Safe workflow

### Validation

Credential-free validation is available through:

```bash
make terraform-fmt
make terraform-init
make terraform-validate
```

Run backend-disabled validation in an isolated Terraform data directory when a working checkout is already initialized against production. CI runs the combined `make ci-terraform` target and never runs authenticated plans, imports, or applies.

### Drift detection

An authorized drift review should:

1. Confirm the `develop` branch and a clean working tree.
2. Confirm the expected AWS identity, default workspace, state key, and absence of a stale lockfile.
3. Supply sensitive variables through the approved process environment.
4. Run one full refresh-enabled plan with state locking and `-detailed-exitcode`.
5. Treat exit code `0` as no change, `1` as an error, and `2` as proposed change.
6. Review every proposed address and attribute before authorizing a mutation.

Do not use `-target`, `-refresh=false`, `-lock=false`, or broad `ignore_changes` to conceal drift.

### Future infrastructure changes

For an intentional change:

1. Update the smallest relevant Terraform configuration on a reviewed branch.
2. Run formatting, validation, contract tests, Ruff, pytest, and secret scanning.
3. Run a full authenticated plan and preserve only a protected, reviewed plan when an apply is authorized.
4. Obtain explicit approval before applying exactly that plan once.
5. Re-run a full plan and require zero remaining changes.
6. Confirm state versioning, lock cleanup, infrastructure health, and repository status.

Existing resources must continue to use import-first adoption: inspect, model, import, reconcile to zero change, and only then consider Terraform authoritative.

## State recovery

Keep the local bootstrap state and its verified backup access-restricted. Production recovery uses S3 object versions:

1. Stop all Terraform writers and confirm no active lock.
2. Record the current state-object version.
3. Select a known-good earlier version through an approved recovery process.
4. Restore it without displaying state contents.
5. Run `terraform state list`, followed by a refresh-only review and a normal full plan.
6. Require a zero-change or fully explained plan before resuming infrastructure changes.

Never delete state history, manually edit state JSON, expose raw state, or restore a version while another Terraform operation is active.
