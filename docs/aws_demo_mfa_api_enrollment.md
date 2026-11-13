# AWS demo MFA: API-only enrollment review

## A. What was prepared

The approved MFA inventory reads succeeded on 2026-11-13 UTC using profile
`blockchain-de`, account `209479274979`, principal
`arn:aws:iam::209479274979:user/blockchain-de-local`.
No device is assigned to that user. The account has this unassigned device:

```text
arn:aws:iam::209479274979:mfa/Authapp
```

**Preserve Authapp.** The Console enrollment wizard is stopped because AWS
documents that it can remove unassigned virtual devices. Unassigned does not
establish that a device is disposable. No cloud deletion is approved. See
[AWS Console enrollment behavior](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_mfa_enable_virtual.html).

Prepared locally, not executed:

- [Interactive enrollment helper](../scripts/enroll_runtime_mfa.py).
- [Complete proposed managed-policy update](policies/runtime-deployer/assume-role-enrollment-window.json),
  preserving every original statement from
  [assume-role.json](policies/runtime-deployer/assume-role.json).
- Synthetic security/privacy tests, with no real devices, seeds or MFA codes.

## B. Why

The existing user grant permits assumption and MFA inventory only. API
enrollment requires two additional IAM actions. These were not part of that
JSON and need explicit approval under the project's IAM expansion gate.
Using the APIs directly avoids the Console's unassigned-device cleanup.
[AWS's API enrollment procedure](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_mfa_enable_cliapi.html)
separates device creation from synchronization/assignment.

## C. Exact permission proposal

Update only the customer-managed policy
`arn:aws:iam::209479274979:policy/DeFiRuntimeAssumeDemoDeployer`, attached to
`blockchain-de-local`. Do not create another user grant, change any inline
policy, remove existing statements, or change the deployment-role trust.

The complete review JSON adds only:

| Action | Allowed resource |
|---|---|
| `iam:CreateVirtualMFADevice` | `arn:aws:iam::209479274979:mfa/blockchain-de-local-demo` |
| `iam:EnableMFADevice` | `arn:aws:iam::209479274979:user/blockchain-de-local` |

Both new statements require the exact planning-user principal ARN and
`aws:CurrentTime` **before `2026-11-13T02:45:00Z`** (November 12, 18:45 PST).
At that time the additional permissions expire; the original assumption and
inventory statements remain unchanged. If the window expires before setup,
stop and obtain approval for a fresh window rather than silently extending it.

IAM scopes EnableMFADevice to the user, not the serial number. The policy
therefore cannot enforce the exact serial on that API. The helper pins the
serial, checks the user has no MFA, and refuses collisions or changed
assignment. During the window, possession of the source user's credentials
plus suitable MFA codes could allow enrollment on that user. Keep the window
short and perform this on the trusted local machine. No deletion, deactivation,
resynchronization, root-MFA management or other-user enrollment is granted.

## D. Validation

Local validation passed: 589 Python contracts, Ruff, repository secret scan,
JSON/policy-size checks and local documentation links. The two IAM action names
match the official service reference. The current CLI history setting is unset.
Hosted CI status is recorded with the develop checkpoint in the conversation.

Offline cases cover preserved orphan devices, exact principal/name, refusal
of existing MFA or target-name collisions, failure without cloud cleanup,
hidden-input requirements, disabled CLI history, credential override removal,
and redaction of AWS errors. Codes are supplied through subprocess stdin rather
than process arguments, shell history or a credential file.

Policy cases preserve the original grant byte-for-byte at statement level,
reject other users and device names, and deny the new actions at/after expiry.
These are offline contracts, not AWS IAM simulation. Before the administrator
saves the update, resolve the Console's policy validation findings and confirm
the current three statements match the base document. If anything differs,
preserve it and stop for review. The CLI identity cannot read back this managed
policy, so the user's Console review is still needed.

## E. Secret handling and recovery

Run the helper **manually in the user's own Terminal**, after approval and the
policy update. An agent/CI must not run live enrollment or collect the codes.
The helper does not open Preview or inspect/display the QR image to the agent.
The user opens the image themselves and enters two consecutive TOTP codes into
hidden local prompts. No QR, seed, code, root credential or temporary credential
response belongs in chat, Git, `.env`, shell arguments or CI logs.

The new QR is held in a newly generated temporary directory with mode 0700 and
file mode 0600. AWS CLI stdout/stderr are captured; only allowlisted error codes
are shown. CLI command history must be disabled or unset before any enrollment
API is called; the helper stops if history or hidden input cannot be verified.
Do not run under terminal recording or screen sharing.

After confirmed assignment and unchanged existing inventory, the helper removes
**only its newly generated local QR file** and writes a nonsecret receipt.
Unlinking is not a guarantee of secure erasure from SSDs, backups or application
caches. Close Preview without saving/exporting and retain the authenticator
entry securely. AWS describes QR/seed data as secret access information:
[CreateVirtualMFADevice](https://docs.aws.amazon.com/cli/latest/reference/iam/create-virtual-mfa-device.html).

If interrupted or an API fails, the helper preserves the new pending cloud
device and private directory, prints no raw exception/response, and stops.
Do not rerun creation, delete a device, resync, disable MFA or change grants to
recover. Review the exact state first. If hidden input fails, no codes are sent;
a pending new device may already exist and must be preserved for recovery.
No automatic rollback, device replacement or cloud deletion occurs.

## F. Git and CI

The develop checkpoint and hosted CI result are recorded in the conversation.
The application, Terraform resources, README and main are unchanged by this
enrollment preparation. Milestone 11.5D stays paused.

## G. Cost

No billable resource is created by this preparation. IAM policy/MFA operations
have [no additional IAM service charge](https://aws.amazon.com/iam/faqs/).
No paid authenticator purchase or subscription is proposed. This does not
authorize any paid EC2 start/launch or extend the demo's runtime allowance.

## H. Remaining approval

The earlier IAM/MFA setup approval still applies. The newly identified API
permission expansion is separate: explicitly approve the two actions, exact
resources, expiry and helper procedure above before installing/running them.
No backend initialization, authenticated Terraform plan, apply, EBS formatting,
SSM execution, secret creation or application deployment is included.

## I. Procedure after that approval

1. Administrator: open IAM Policies → `DeFiRuntimeAssumeDemoDeployer`. Review
   the current JSON against the base assume-role document, default version and
   attached-user scope. Preserve any unexpected content. Do not overwrite an
   unrelated or already changed policy.
2. Edit that policy's JSON to the complete proposed enrollment-window JSON.
   Resolve validation findings, confirm the UTC expiry is still in the future,
   and save as a new default version. Keep the old version. If the version quota
   is reached, stop; do not delete versions to make room.
3. User: in their own unrecorded Terminal, run from the repository:

   ```sh
   python3 scripts/enroll_runtime_mfa.py
   ```

   The script fixes profile `blockchain-de` and verifies the exact IAM principal.
   It refuses a pipe/noninteractive run. Do not run it through an agent tool.
4. Review the displayed target and type ENROLL locally. Open the printed private
   `bootstrap.png` path yourself in Preview, scan it into the authenticator app,
   and enter SCANNED locally. Enter two consecutive codes at the hidden prompts.
5. Confirm the helper reports verified assignment and preserved existing
   inventory. Share only **"MFA enrolled"** and, if needed, the new device ARN.
   The receipt contains identifiers/status only; do not share the QR/private
   directory contents. The assistant can independently verify assignment with
   the already approved metadata reads.
6. Stop enrollment. Automatic expiry removes the temporary action grants at
   the stated time. Later cleanup of the extra policy statements is a separate
   approved policy update; do not remove the original grants or orphan devices.
   The remainder of the approved deployment-role setup may then continue.

## J. Infrastructure status

The operator reports the base customer-managed user policy attached, and its
two MFA inventory permissions work. No other new IAM setup is verified yet.
No MFA create/enable action has been called by the assistant, and no temporary
enrollment expansion is installed. Authapp remains unassigned and preserved at
the inventory check. EC2, EBS, both Terraform states, the 43 platform resources,
Snowflake and Streamlit were not changed by this preparation.
