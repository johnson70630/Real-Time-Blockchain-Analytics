# Milestone 11.6C — Controls reply unverified; EC2 stopped; terminal parser fixed

Follow-up: [replacement window passed streaming and graceful drain; EC2 stopped](aws_runtime_streaming_pilot_result.md).
The aborted window below remains historical evidence. Its replacement completed
one separately approved start and consumed the single-demonstration exception.

**The first streaming window stopped before installation or image validation.**
My terminal parser rejected a normal nested-terminal line ending. The guest
controls reply was not verified, so no later gate was entered. The parser is now
fixed offline; the existing secret remains unchanged.

## A. What happened and what was fixed

The owner approved the exact `0230858` controlled pilot and refreshed MFA privately.
The source/package and stopped/running cloud checks passed. Exact workload IAM,
SSM document, image/config/finding inventories, disks, ingress and stop controls
matched the reviewed setup; the existing log group was readable with zero streams.

| Event | UTC, 2026-11-19 |
|---|---|
| EC2 launch | 00:36:20 |
| Start cutoff | 00:40:34.128332 |
| Relay normal stop returned stopping | 00:43:37.367857 |
| Independent normal-stop check | 00:43:47.187800, stopping |
| Watcher completed stopped | 00:44:02.357800 |
| Independent stopped/disks/IPv4 readback | 00:44:10.934689 |

The accepted boot entered the reviewed SSM relay. The 20-member archive upload
reported success. The controls command was sent, but no parsed reply arrived.
The relay exited with a RuntimeError and requested normal stop; independent stop
and final resource readbacks followed. No installation, prepare, chain-validation,
controller, sample or other streaming action was sent. The single manual-start
approval is consumed and inactive; it cannot authorize another start.

The reply parser required LF or one CR before LF. A real local pseudo-terminal
reproduces a second CR when both terminal layers apply ONLCR, producing CRCRLF.
The original regex rejects that framing. The corrected parser accepts CR/LF
sequences while retaining output limits, JSON parsing and a strict boolean `ok`.
It forwards fixed transport failure codes, never raw terminal bytes or exceptions.

## B. Why

This was an avoidable local transport assumption. The old tests did not exercise
the real terminal translation used by Session Manager. The change fixes reception,
not the secret or application. A successful upload marker did not prove the
subsequent JSON controls reply was received.

The terminal transcript was not retained or disclosed. We therefore cannot claim
the guest controls succeeded, or prove framing was the only guest-side issue.
The fixed parser must still receive and validate the actual result on a future
approved run. There is no evidence of a new secret-format problem; the prior
exact-version JSON-object format check remains successful.

## C. Files changed

Changed [the relay](../scripts/runtime_pilot_relay.py) and
[relay tests](../tests/test_runtime_pilot_relay.py). Added this report,
[sanitized evidence](evidence/aws_runtime_controlled_streaming_abort_2026-11-19.json)
and links in the pilot/readiness report. Guest operator, installer, service timing,
frozen host candidate, release, Compose, images and dependencies are unchanged.
README, Terraform, IAM, Snowflake/dbt/Streamlit and original host files are preserved.

The failed archive remains
`54c7ca95cc90f96dc67952f2548712ce6e28639cacf790686d69f66f44b4095f`.
Rebuild the proposed archive from the new checkpoint because its source-commit
manifest changes; verify the guest payload file hashes remain identical. Preserve
both archives and the failed execution receipts. Do not update or reuse its approval.

## D. Tests and validation

**749 runtime tests and 36 negative subtests pass**, with the existing websockets
deprecation warning. Thirteen new regression cases cover actual PTYs with ONLCR,
success/error replies, LF/CRLF/CRCRLF endings, split writes, invalid JSON, missing
or non-boolean `ok`, output limits and no transcript leakage. The original regex
fails the reproduced CRCRLF fixture; the corrected receiver succeeds.

The actual staged operator entrypoint was additionally invoked under local system
Python **3.9.6**, as a non-root process. It loaded and emitted the expected safe
JSON host-refusal result before any cloud access. This is a compatibility check,
not a substitute for the unverified live guest controls. Shell/Python syntax,
source/evidence hashes, links, secret scanning and Ruff apply before checkpointing.

Independent readback confirmed stopped, public IPv4 released and both original
encrypted gp3 disks attached. The watcher reached completed_stopped. There was
no secret read/edit, provider RPC, quarantined-image execution, candidate service
installation, controller attempt or pipeline S3 write in this window.

## E. Security review

The parser change alters only local response framing/diagnostics. It does not
weaken source/package hashes, approval checks, current-boot constraints, timing,
image gates, one-attempt intent files, metadata-only output or stop-on-failure.
Unknown JSON or non-boolean success flags fail closed. Error codes are allowlisted;
terminal contents and provider text remain suppressed. No command allowlist or
remote application entrypoint is expanded.

Only temporary upload/control actions were attempted on the exact approved host.
Some or all guest timers may have been established, but their reply was unverified.
The VM is stopped and its tmpfs staging/control state is cleared by shutdown.
No filesystem formatting, volume operation, forced/hibernated/skipped-OS stop,
resource provisioning, IAM change, production Terraform action or Snowflake action
occurred. No original disk, Kafka identity or checkpoint was reset or deleted.

## F. Git and CI

Executed checkpoint: `02308589c62ce538f33583f7d6633271a6e4f6b2` on develop.
[CI 44425204117](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/44425204117)
passed all six jobs before the run. Commit the validated fix/result, build and
verify its source-bound replacement artifact, push develop and observe its CI.
The final checkpoint/CI/artifact metadata belongs in the conversation and private
result receipt. Main and unrelated duplicates remain unchanged.

## G. Cost implications

Launch to independent stopped observation was **470.935 seconds**, under eight
minutes. At the previously recorded USD 0.1714/hour combined EC2/IPv4 rate, this
conservative interval is about **USD 0.02242**, plus retained storage, requests,
tax and other charges. Actual billing was not queried. The estimate is not a
spending cap; retained disks/images/logs continue billing while EC2 is stopped.

The offline fix/tests use no AWS APIs or paid runtime. Do not restart simply
because time or estimated spend remains within the old 45-minute interval.
The approved single start has already been used.

## H. Remaining blockers

Live guest-controls success, versioned installation, Alchemy authentication,
Arbitrum chain ID, streaming, Bronze data and graceful-drain acceptance remain
unverified. No new secret correction is needed. No quarantined image ran, so the
single streaming security exception remains unused; its expiry and all finding/
scope controls must still pass before any replacement paid start.

A replacement start requires new explicit approval for the rebuilt source-bound
package, with the same reviewed conditional streaming/write/shutdown scope.
Do not silently extend the previous approval or skip the controls gate.

## I. Next step and rollback

Finish the replacement artifact/CI verification offline. Then request one new
controlled window. Preserve the existing secret, original installation, EBS,
Kafka identity and isolated S3 prefixes. A new execution must use fresh receipts,
a healthy detached watcher, an actual five-minute cutoff and both absolute timers.
Proceed only after the now-readable controls reply passes, then install, prepare,
chain-check and launch once conditionally.

Rollback for this window was normal stop and preservation of evidence. The
candidate application was never installed, and no pipeline data requires rollback.
No automatic retry, secret edit, checkpoint reset, resource deletion or live
recovery experiment is authorized. Keep all earlier artifacts/receipts intact.

## J. Whether production infrastructure changed

**Yes, only the approved existing runtime was started and normally stopped**, with
temporary SSM staging/control actions attempted. No new persistent cloud resource,
Terraform state/configuration, IAM privilege, secret, original host installation,
existing production platform or Snowflake privilege was changed. Final readback
confirmed stopped/IPv4 released/original disks retained. Milestone 11.6C stays
open for real streaming; 11.5D stays paused.
