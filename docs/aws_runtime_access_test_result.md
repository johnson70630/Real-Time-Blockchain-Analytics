# Milestone 11.6C: first live SSM access test

Completed 2026-11-16 UTC (2026-11-15 Pacific). **Access passed; application
readiness is blocked by incomplete guest bootstrap. EC2 is stopped.**
The operator approved one supervised 30-minute access window, normal shutdown
by minute 20, and read-only guest checks. This window is closed; it does not
authorize another start or any repair. Milestone 11.5D remains paused.
The separately approved later [host recovery](aws_runtime_host_recovery_result.md)
resolved Compose/Docker enablement and also ended with verified stop. The
findings below describe this earlier access window.

## A. What was implemented and executed

Verified the operator-installed macOS Session Manager plugin, version
`1.2.896.0`, package receipt and symlink. Its reviewed package SHA-256 remains
`b95139a76cb473654d92185c200ac65205244b20f96aeb240b53837ba4973084`.
Apple signing/notarization was verified before the approved installation.

With fresh MFA-backed credentials, verified the exact instance, disks,
no-ingress security group, standard CPU credits, shutdown-to-stop attribute
and reviewed session document. A non-mutating normal-stop DryRun returned
`DryRunOperation`. Armed a private local watcher before the operator started
only `i-0ca1671a59751acc5` through the Oregon administrator Console.

Opened an actual SSM shell using `blockchain-runtime-demo-shell`, inspected
the host, exited the shell cleanly and requested normal stop early. AWS
confirmed `stopped` at **01:25:02 UTC**; a further read check at 01:26:24 UTC
confirmed no public IPv4 and both encrypted disks still attached.

## B. Why

Provisioning success and offline CI do not prove guest software installation
or an operational administration channel. This test established real SSM shell
access and found a dependency gap before any application or secret deployment.
The Kafka volume remained untouched while its cloud and guest identities were
cross-checked. The next recovery can therefore be limited to host software.

## C. Files and evidence

- This report and [sanitized access evidence](evidence/aws_runtime_access_test_2026-11-16.json).
- Current status in the pilot, SSM operator and document reviews.
- No Terraform, application, IAM policy, dependency, workflow or README edits.

Private authorization, preflight, fixed guest-command list, client-install
receipt, watcher completion and final-state metadata are retained in mode 0700
temporary directories with mode 0600 files. Raw credentials, session tokens,
user-data contents and broad guest logs are not committed. Temporary operator
drivers are local control tools, not deployed runtime or AWS shutdown services.

## D. Tests and validation

| Check | Observed result |
|---|---|
| Account / Region / host | 209479274979 / us-west-2 / reviewed t3.xlarge in us-west-2a |
| Local client | Version 1.2.896.0, symlink, package receipt and package hash verified |
| Credentials | Expected deployer ARN; refreshed session had over 45 minutes; local assume profile requests 14400 seconds |
| Session document | Account owner, Active, Session/EC2 target, default/latest version 1; exact canonical JSON hash verified |
| Live session | StartSession and OpenDataChannel worked; `ssm-user`; shell exited with code 0 |
| SSM Agent | Active/running |
| Docker | Server 25.0.18; no running containers |
| Docker service / socket | Service initially inactive/disabled; socket enabled and active. Service became active after the version query, consistent with socket activation |
| Docker Compose | Unavailable; standard plugin paths absent |
| Tools and capacity | Python, AWS CLI, findmnt, lsblk, blkid and XFS tools present; root has 28G available; memory 15787 MiB; swap 0 |
| Kafka disk | Serial `vol02c9fb14a9c568fb5` resolves to /dev/nvme1n1; 100 GiB; no TYPE/UUID from blkid (exit 2); not mounted |
| Guest bootstrap script | SHA-256 matches the current template rendered offline with reviewed inputs; blank-disk guard present; no formatting command |
| Bootstrap completion | Not proven: Compose absent, Docker service disabled, and selected logs did not show the disk-guard marker |
| Shutdown | Force=false, Hibernate=false, SkipOsShutdown=false; stopped and no public IPv4 verified |

Guest script SHA-256:
`aea4e3fc8df1b982fab4d49f49374392e90949c1486c7592eca8be48734da464`.
The local render was ordinary text substitution; no Terraform or backend was run.
The reason bootstrap did not finish is **unconfirmed**. A successful
cloud-final unit status on this boot is insufficient evidence of completion of
all earlier per-instance script steps. Do not assume the missing disk-guard
marker means its runtime behavior was tested.

Nine offline operator-driver safety checks covered the exact stop target,
normal-shutdown flags, stopped/stopping/pending states, ownership/AZ guards and
absence of start/destructive APIs. All **101 repository policy/document/pilot
tests pass**, as do secret scanning, JSON/local links, focused Ruff and
whitespace checks. The resulting hosted CI is reported separately.

## E. Security and data correctness

The administrator used the Console to start the node because the reviewed
deployer policy excludes StartInstances. No IAM expansion was needed or made.
Human access used the existing temporary MFA role, not EC2 workload credentials.
Live policy JSON and document sharing permissions remain independently unread;
actual shell and data-channel access are now demonstrated. Explicit
ResumeSession and TerminateSession API tests were not performed; the shell
exited normally. Full shell transcript logging remains unconfigured.

Guest commands were read-only inspections. The Docker version query appears to
have activated the daemon through its existing socket; this is a disclosed
service side effect, not a container deployment. No service enable/start command,
package install, bootstrap rerun, cloud-init reset, filesystem initialization,
mount, secret retrieval, image pull/push or application start was issued.
No S3 pilot data, checkpoints or Snowflake objects were changed.

The local watcher was an additional stop mechanism dependent on this Mac and
its credentials; it was not an AWS-side hard deadline. Its deadline stop was
not needed because the approved early normal stop completed first. Credential
expiry, SSM exit and the document's timeout do not stop EC2.

## F. Git and CI

Checkpoint on develop after local validation and observe the automatically
triggered six-job CI. The commit/run are recorded in the conversation. Main
remains `21be18e45791a4199830754b80620d7d6599587e`; README is unchanged.

## G. Cost implications

EC2 LaunchTime was 01:20:19 UTC; stopped was observed at 01:25:02 UTC, a
conservative 283.33-second interval. At the previously reviewed Oregon rates
of USD 0.1664/hour for t3.xlarge and USD 0.005/hour for public IPv4, the
interval estimate is **USD 0.0135** before other usage and taxes. This is an
estimate, not a billing measurement or cap; it includes observation latency.
See [retained official price evidence](evidence/aws_runtime_demo_costs_2026-11-13.json).

Stopped compute no longer accrues running-instance charges. Retained disks
and alarms still have an estimated **USD 10.60/month** baseline before other
usage/taxes. No ECR publication, secret, streaming or cleanup occurred.

## H. Remaining blockers

1. Checksum-pinned Compose installation and explicit Docker service enablement
   need a separate, narrow guest-recovery approval. Do not replace the node or
   reapply Terraform to repair missing guest software.
2. Kafka EBS still needs exact-volume initialization approval and a verified XFS
   UUID/mount. The observed disk probe is not approval to format it.
3. ECR publication/digests/scans and an actual release manifest remain pending.
4. Alchemy secret creation and exact-ARN runtime IAM/boundary changes remain
   pending. The current workload boundary excludes Secrets Manager.
5. Application deployment, real S3 smoke/recovery tests and full stop/drain
   acceptance still need their own approved streaming window.

## I. Historical next-step proposal: host software recovery only

Propose a **new, supervised 15-minute** start/SSM/normal-stop window for the same
instance. Request normal stop by minute 10 and confirm stopped by minute 15.
Use the same identity/document/disk/ingress preflight and Console fallback.
Estimate compute plus IPv4 at USD 0.04285 for 15 minutes, excluding other usage
and retained storage. No AWS-side stop schedule is added.

After separately approving the window and these guest mutations:

1. Confirm x86_64, existing Docker version and the missing Compose path. Stop
   for review if a plugin already exists or any identity/configuration differs.
2. Download only the reviewed official binary to a private temporary file:
   `https://github.com/docker/compose/releases/download/v5.1.3/docker-compose-linux-x86_64`.
   Require HTTPS redirects and successful download; never execute it first.
3. Verify SHA-256 exactly
   `a0298760c9772d2c06888fc8703a487c94c3c3b0134adeef830742a2fc7647b4`.
   On mismatch, stop. Do not substitute latest or change the checksum.
4. Create `/usr/local/lib/docker/cli-plugins` if absent, install that verified
   file as `docker-compose` with mode 0755, and check `docker compose version`.
5. Enable/start the existing `docker.service`; verify service enabled/active,
   socket status, and zero running containers. Run no Compose application.
6. Reconfirm the Kafka disk is still unformatted/unmounted, exit SSM, normally
   stop EC2, and verify stopped/no public IPv4 and retained disk attachments.

This does not rerun the full user-data script, reset cloud-init, install/update
OS packages, format/mount storage, pull images or deploy applications/secrets.
No AWS resource creation, IAM edit, Terraform plan/apply/import/state action,
Snowflake operation or main merge is proposed. Any additional finding pauses
that repair rather than broadening its scope. This proposal was subsequently
approved and completed; see the separate host-recovery result linked above.

## J. Production infrastructure changes

The approved existing instance was started by the operator, accessed through
SSM, and normally stopped by the assistant. Docker socket activation was
observed as above. No resources were created/deleted and neither Terraform
state was accessed. The last verified runtime inventory remains 28 managed
resources; it was not recounted from state during this test. Existing platform
resources/grants were outside scope. The next recovery requires new approval.
