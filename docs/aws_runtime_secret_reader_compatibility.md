# Milestone 11.6C — Keep the secret unchanged; fix the reader offline

Follow-up: [versioned rollout tooling and one-read compatibility proposal](aws_runtime_reader_rollout_review.md).

**The secret-edit path is on hold. The reader is fixed and tested offline.**
No cloud secret read/write, EC2 start or deployment occurred during this work.
The owner objected to repeated edits. The old approval is preserved as history,
not active permission to proceed. Keep the existing URL/key/value.

## A. What was implemented

Added a pure decoder in [runtime/contracts.py](../runtime/contracts.py), used by
[host injection](../runtime/host.py) and the [chain preflight](../scripts/check_runtime_chain.py).
It accepts a bare endpoint, a JSON string containing it, or a JSON object with
exactly `ALCHEMY_WEBSOCKET_URL` as its sole field. It preserves the decoded URL/key.
The producer receives the same raw URL through its existing protected tmpfs file;
no URL is passed in environment variables or argv, and nothing is rewritten in AWS.

The [version inspector](../scripts/check_runtime_secret_contract.py) separates raw
format observations from the resolved endpoint contract. A supported JSON wrapper
can report raw JSON/quotes and still pass the reader contract. Output contains
only fixed representation names, codes and identity checks. The resolved secret's
repr omits its URL. The clipboard checker remains a raw URL checker; no new owner
clipboard access or repetition was requested.

## B. Why

The previous live check proved a mismatch with our bare-string assumption, not a
bad key. Repeatedly asking for the same secret to be rewritten added work without
fixing compatibility. The reader now decodes declared forms before validating the
endpoint. Host and preflight share that implementation so their contracts agree.

The actual stored shape/key was never disclosed or retained. Compatibility with
that exact value is **not claimed live**. Unknown forms remain a reader-side
investigation; do not resume the secret-edit loop or silently guess another key.

## C. Files and artifact scope

Changed contracts.py, host.py, chain/version inspectors and their regression tests.
Added [parser tests](../tests/test_runtime_secret_payload.py), this report and
[sanitized evidence](evidence/aws_runtime_secret_reader_compatibility_2026-11-18.json).
Updated status links in the pilot and previous result. README, Terraform, dependency
locks, producer/Spark application source, Compose, systemd definitions, published
image digests and the release candidate are unchanged.

Two of the nine host-source files change offline: contracts.py and host.py. The
original installed files, original archive and release are preserved. The bundle
allowlist/schema is unchanged. Build a separately named candidate from committed
Git blobs, verify without extraction, and record its source commit/digest. Do not
overwrite `/opt/blockchain-runtime` or reuse an old deployment approval.

## D. Tests and validation

**664 runtime tests and 36 negative subtests pass**, with one existing websockets
deprecation warning. Focused parser/host/preflight/inspector/producer/bundle tests
pass. Injection tests verify all three forms produce the exact same file and mode
0400; ambiguous inputs stop before registry/container actions. Tests cover duplicate
or extra fields, arrays, nesting, malformed JSON, wrong hosts/ports, userinfo,
whitespace and controls. Errors omit input. The container CLIENT source is unchanged.

Seven unsigned loopback cases with the real local CLI exercise raw/object/string
acceptance and identity/version/stage/binary rejection. Credential files are disabled;
fixtures are dummy data and production calls are zero. Ruff and Python 3.9 syntax
checks pass. Review secret scan, source/evidence hashes, original bundle preservation
and links; checkpoint, build/verify its offline candidate and observe hosted CI.
No test reads the owner's clipboard or actual secret.

## E. Security review

This is explicit schema decoding, not recursive unwrapping. Objects must contain
exactly the declared field. Duplicates, extra fields, arrays, nested values and
single-quote pseudo-JSON fail closed. Structural JSON whitespace is allowed; URL
whitespace is never stripped or repaired. The resolved endpoint must satisfy the
intended Arbitrum WSS host/path/userinfo/port restrictions; controls are rejected.
Authentication and chain ID still require separate approved live gates.

The parser performs no I/O. Preflight verifies its exact source digest before a
secret read, including an explicitly staged standalone copy. Values stay in memory
or the existing tmpfs credential file with unchanged permissions/cleanup. Values
and value hashes never enter logs, receipts, Git or images. No privilege, source
secret selection, network destination or S3/checkpoint scope is broadened.

## F. Git and CI

Work began on develop at `50ef275`, whose six jobs passed in
[CI 44258299317](https://github.com/johnson70630/Real-Time-Blockchain-Analytics/actions/runs/44258299317).
Source checkpoint: `ae678174072b2484e2c095593f3363adf822904e`. Its separate
nine-file candidate was built from immutable Git blobs and verified without
extraction/installation. Candidate SHA-256:
`568d0fc889ca7139aefa651d6d5dccea90ac2bc2106130d8d6451e736b3c9143`.
The old archive is preserved; exactly two host files differ and seven match.
This documentation follow-up records the artifact without changing its source.
Push develop and observe CI. Main and unrelated duplicates stay unchanged.

## G. Cost implications

No paid runtime or secret API operation was used. Local unsigned loopback testing
has no production AWS charges. Existing retained resources keep their usual costs.
No new resource, image publication, subscription, IAM change or Terraform action
is part of this fix.

## H. Remaining blockers

This code has not read the actual saved payload. Only the earlier safe JSON/quote
indicator is known. The new host/parser artifact differs from the accepted legacy
host and needs a fresh security/deployment review. Existing container image bytes
can stay the same because they still receive the same raw URL file; any future
live scope must recheck image risk acceptance and controls.

Do not run the old dispatcher, which pins old reader bytes. Before a future paid
start, prepare a versioned install/service path preserving the old installation,
and reconcile the five-minute handoff with the old ten-minute arming behavior.
No new cloud start is approved here.

## I. Next step

Preserve the existing secret. Finish isolated artifact review, then prepare updated
hash-bound deployment commands before requesting live action. A future approved
check should report only representation and resolved-format status. Unknown shapes
need an explicit reader contract, not a blind edit. Format success does not prove
RPC/data correctness or authorize streaming. Preserve disks, Kafka identity, S3
checkpoints/data and earlier evidence. No rollback or secret restoration occurred.

## J. Whether production infrastructure changed

No. Work was offline, and no authentication refresh was requested. The last
independent EC2 readback confirmed stopped and IPv4 released; this work did not
start it or read/change a secret. The installed host and cloud resources remain
untouched by these changes. Milestone 11.6C stays open; 11.5D stays paused.
Secret-edit instructions are superseded by this reader fix.
