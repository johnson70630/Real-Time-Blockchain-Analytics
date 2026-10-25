# Terraform foundation

## Scope and ownership

Terraform is introduced through two deliberately separate root modules:

- `infra/terraform/bootstrap/state` defines only the future remote-state bucket. It uses local state during the one-time, separately approved bootstrap.
- `infra/terraform/environments/prod` is the explicit production root. It contains matching definitions for the discovered production data bucket and Snowflake S3 integration role. These definitions do not become authoritative until a separately approved import produces a zero-change plan.

Terraform workspaces are not used to represent environments. The project currently has one real environment, represented by the `prod` root.

## Existing infrastructure inventory

| Category | Current infrastructure | Classification | Adoption boundary |
|---|---|---|---|
| AWS | Production analytical-data S3 bucket | A — Terraform adoption candidate | Inspect and import in 11.5B; never recreate it. |
| AWS | Snowflake S3 integration IAM role and policies | A — Terraform adoption candidate | Inspect and import in 11.5B. |
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

AWS authentication uses the standard provider credential chain. Local operators may select an existing shared profile with `AWS_PROFILE`; CI validation needs no AWS credentials. Snowflake credentials, private keys, and passwords are never stored in Terraform source. Future authenticated Snowflake operations must use an approved external credential path.

## Remote state design

Terraform state must not share the production analytical-data bucket. The dedicated bucket name follows:

```text
blockchain-analytics-terraform-state-<unique-suffix>
```

The production state key is:

```text
production/platform/terraform.tfstate
```

The bucket is in `us-west-2` and is modeled with versioning, S3-managed encryption, blocked public access, bucket-owner-enforced ownership, retained noncurrent state history, and destroy protection. Native S3 locking uses `<state-key>.tflock`; no DynamoDB table is involved.

The future least-privilege backend policy requires:

- `s3:ListBucket` restricted to the production state path.
- `s3:GetObject` and `s3:PutObject` on the state object.
- `s3:GetObject`, `s3:PutObject`, and `s3:DeleteObject` on the lock object.

Backend credentials must come from the normal AWS credential chain. The version-controlled `backend.hcl.example` contains only non-sensitive bucket, key, region, and encryption settings. A real `backend.hcl` is local and ignored.

## Bootstrap and migration flow

The bootstrap root is intentionally isolated from application infrastructure:

1. Review the bootstrap configuration and choose a globally unique, non-sensitive bucket suffix.
2. Initialize the bootstrap root with local state.
3. In a separately approved change, apply only the bootstrap root once.
4. Copy `backend.hcl.example` to ignored `backend.hcl` and replace the bucket placeholder with the created bucket name.
5. Initialize the production root with `-backend-config=backend.hcl` and migrate its state to S3.
6. Verify remote state and native lockfile behavior before adopting any production resource.

This milestone stops before step 3. The bucket has not been created, and no state migration has occurred.

## Import-first production adoption

Existing production infrastructure is never recreated merely to place it under Terraform management. For each AWS or Snowflake object in 11.5B or 11.5C:

1. Inspect and record the live configuration.
2. Write a matching Terraform resource definition.
3. Import the existing object into the appropriate state.
4. Run a refresh-only review and a normal plan.
5. Reconcile configuration until the plan is zero-change.
6. Only then consider Terraform authoritative for that object.

No unmanaged or speculative apply is permitted. Imports and applies require an explicitly approved milestone and operator review.

## Safe validation

Credential-free validation is available through:

```bash
make terraform-fmt
make terraform-init
make terraform-validate
```

Initialization always uses `-backend=false`, so validation neither contacts the future backend nor creates remote state. CI runs the combined `make ci-terraform` target. CI does not run plan, import, or apply.
