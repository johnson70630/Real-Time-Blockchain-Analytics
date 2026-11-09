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
| Snowflake | Warehouse, database, five schemas, five account roles, five role-hierarchy grants, S3 storage integration, external stage, and Parquet file format | A — Terraform managed | Twenty existing infrastructure resources were imported without recreation and verified with zero-change plans. |
| Snowflake | Selected warehouse, database, and schema privileges | A — Terraform managed | Fifteen existing privilege-grant resources were imported. Grouped dbt grants represent multiple privileges in one Terraform resource. |
| Snowflake | Operational tables, dbt relations, service user, user-role assignments, and unadopted grants | B — externally managed | These remain governed by provisioning SQL, dbt, application operations, or Snowflake administration and are explicitly outside the 43-resource scope. |
| Hosted app | Streamlit Community Cloud application | B — managed externally | Continues tracking `main`; no provider is introduced in this milestone. |
| Local runtime | Kafka, Spark, Airflow, and the Airflow PostgreSQL metadata database | C — local development only | Docker Compose and local process management remain authoritative. |
| Runtime platform | Future continuously hosted producers, Spark, Kafka, and Airflow | D — deferred until 11.6 | Deployment architecture must be approved before any Terraform resources are modeled. |

Classification does not imply that every resource will be adopted. Each later milestone must confirm ownership, lifecycle, and safe import behavior independently.

## Versions and authentication

Both roots require Terraform `>= 1.10.0, < 2.0.0`. Terraform 1.10 is the minimum because it introduced native S3 lockfiles. The completed production-adoption verification used Terraform `1.17.0`; CI uses an explicit supported Terraform 1.x release.

Provider constraints are deliberately bounded to compatible minor lines:

- `hashicorp/aws ~> 6.69.0`
- `snowflakedb/snowflake ~> 2.22.0`

The verified production state used AWS provider `6.69.1` and Snowflake provider `2.22.0`.

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

The production root uses the default workspace and was initialized against S3 with native locking and state versioning. Eight existing AWS resources were then imported without recreation:

1. Production S3 bucket
2. S3 versioning configuration
3. S3 server-side encryption configuration
4. S3 public-access block
5. S3 ownership controls
6. Snowflake S3 IAM role
7. Snowflake S3 managed policy
8. IAM role-policy attachment

The import preserved the bucket, IAM trust principal, external-ID condition, managed-policy permissions, and attachment. No production resource mutation was required. A full refresh-enabled plan on 2026-10-26 returned exit code `0` with `0 add, 0 change, 0 destroy` and no detected drift.

Milestone 11.5C subsequently adopted twenty existing Snowflake infrastructure resources:

1. One warehouse
2. One database
3. Five schemas
4. Five account roles
5. Five role-hierarchy grant resources
6. One S3 storage integration
7. One external S3 stage
8. One Parquet file format

The eight AWS resources and twenty Snowflake infrastructure resources form the original 28-resource infrastructure subtotal. The separately bootstrapped state bucket is not stored in, or counted as part of, this production state.

### Adopted privilege grants

Fifteen existing Snowflake privilege-grant resources were also imported:

| Scope | Roles and privileges | Terraform resources |
|---|---|---:|
| Warehouse | `USAGE` for DE, Pipeline, dbt, and Analyst | 4 |
| Database | `USAGE` for DE, Pipeline, dbt, and Analyst | 4 |
| RAW schema | `USAGE` for DE, Pipeline, and dbt | 3 |
| CONTROL schema | `USAGE` for Pipeline | 1 |
| STAGING schema | Grouped dbt `CREATE TABLE`, `CREATE VIEW`, and `USAGE` | 1 |
| CORE schema | Grouped dbt `CREATE TABLE`, `CREATE VIEW`, and `USAGE` | 1 |
| MARTS schema | Grouped dbt `CREATE TABLE`, `CREATE VIEW`, and `USAGE` | 1 |
| **Total** |  | **15** |

A grouped dbt grant represents three existing Snowflake privileges in one Terraform resource. These fifteen resources do not imply that Terraform manages every privilege held by the affected roles or schemas. All adopted privileges existed before import; adoption required no new `GRANT` or `REVOKE`.

The production state therefore contains **43 managed resources**: 28 infrastructure resources plus 15 privilege-grant resources. Only resources explicitly imported into this state are Terraform-managed.

### Intentionally deferred and excluded

The agreed 43-resource scope intentionally does not manage:

- The live, direct MARTS schema `USAGE` grant to `BLOCKCHAIN_ANALYST_ROLE`. It remains valid, has no grant option, and remains deliberately unmodeled and unimported.
- Four Analyst `SELECT` grants on tutorial MART tables.
- Pipeline `USAGE` grants on the storage integration, external stage, and Parquet file format.
- DE `USAGE` grants on the external stage and Parquet file format.
- One future RAW-table `SELECT` grant.
- Ninety-three scoped RAW/CONTROL table grants.
- RAW and CONTROL operational table definitions.
- dbt-managed STAGING, CORE, and MARTS tables and views, including dbt object ownership grants.
- `BLOCKCHAIN_APP_USER`, user-to-role assignments, and personal users.
- Credentials, private keys, and storage-integration external IDs.

These objects and privileges remain live and externally managed; they were not removed, revoked, disabled, or adopted implicitly. Terraform does not own dbt-managed relations or their permissions.

### Final verification evidence

Adoption proceeded sequentially: inspect the live object, model it, import it, and require a zero-change plan before continuing. Each batch completed that workflow. The final verification covered all 43 production resources with Terraform `1.17.0`, AWS provider `6.69.1`, and Snowflake provider `2.22.0` against the default workspace and versioned S3 backend with native locking.

The latest authorized full refresh-enabled plan returned exit code `0` with `0 add`, `0 change`, `0 destroy`, no output-only changes, and no unexpected provider diagnostics. The verified production state version was `Bd_4sizRbSKTAnSx_e4RrrrGumPC8E8v`.

This evidence records the completed adoption; it does not authorize a future Terraform apply. This documentation update did not rerun the production plan.

## Import-first production adoption

Existing production infrastructure is never recreated merely to place it under Terraform management. For each AWS or Snowflake object in 11.5B or 11.5C:

1. Inspect and record the live configuration.
2. Write a matching Terraform resource definition.
3. Import the existing object into the appropriate state.
4. Run a refresh-only review and a normal plan.
5. Reconcile configuration until the plan is zero-change.
6. Only then consider Terraform authoritative for that object.

No unmanaged or speculative apply is permitted. Imports and applies require an explicitly approved milestone and operator review. The 43 explicitly imported AWS and Snowflake resources above completed this workflow and are now authoritative in the production state. Existing resources outside that list remain externally managed.

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
