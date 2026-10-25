# Release and Continuous Deployment Contract

## Current deployment inventory

The DeFi Data Lab is the project's only externally hosted component.
Streamlit Community Cloud deploys `streamlit_app.py` by tracking `main`.
The public application authenticates as `BLOCKCHAIN_APP_USER` with
`BLOCKCHAIN_ANALYST_ROLE`; hosted navigation excludes Operations and does not
grant access to `CONTROL`.

Kafka, Spark, producers, recovery jobs, Airflow, and warehouse commands remain
local or operator-run workloads. They are not deployable by this release
workflow. AWS runtime deployment is intentionally deferred to Milestone 11.6.

## CI and release lifecycle

CI answers whether a commit is safe to merge. It validates pushes and pull
requests without production credentials or mutations. A release answers which
validated `main` commit Streamlit Community Cloud is expected to run and
whether its public endpoint became healthy.

The `Release` workflow runs only after a successful push-triggered `CI` run on
`main`. Its immutable identity is the CI run's commit SHA. Release metadata
records that SHA, `main`, the UTC verification time, the `production`
environment, and the `defi-data-lab-streamlit` component in the workflow
summary. Development remains the validation environment; production releases
originate only from `main`.

The existing public application URL is non-secret deployment configuration in
`config/release.json`. No Snowflake, AWS, RPC, or Streamlit credentials are
supplied to the workflow.

## Smoke and failure behavior

The release workflow allows Streamlit Community Cloud time to redeploy, then
uses headless Chromium through Playwright to establish a normal browser session
and validate the rendered application shell. Raw HTTP clients can receive a
Streamlit authentication/session redirect even when a guest browser can use the
public application, so direct root or `/_stcore/health` responses are not the
production release gate. The browser must remain on the application, render all
five public lesson labels, avoid authentication UI, and exclude Operations.

The retry policy is version-controlled in `config/release.json`: 15 seconds
between attempts, at most 20 attempts, and a 300-second overall deadline. Each
attempt uses a fresh browser session. Failures are reported with a stable,
non-sensitive category for browser reachability, deployment startup,
authentication blocking, missing shell content, or browser runtime failure. A
failure remains visible in GitHub Actions and retains the target commit SHA in
the workflow record. It never triggers a data-platform mutation.

The smoke contract intentionally does not query private `CONTROL` data or use
application credentials. Playwright and Chromium are installed only in the
release smoke job. Streamlit's hosted-mode contracts and CI tests verify the
five public pages and exclusion of Operations; live release acceptance confirms
those behaviors after deployment.

## Production safety boundary

Release verification must not run recovery or forward processing, advance
watermarks, write completed ranges, change alerts, load Snowflake RAW, execute
production dbt models, write S3, start Airflow, or deploy Kafka/Spark runtimes.

## Rollback

Rollback means restoring `main` to application behavior from a known-good
commit through a normal revert commit, or applying a forward corrective commit.
After CI succeeds, Streamlit Community Cloud redeploys that new `main` commit
and the release smoke check runs again. Never force-push or rewrite `main`
history for rollback.
