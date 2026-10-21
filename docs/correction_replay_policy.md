# Reorg, Late-Arrival, and Replay Policy

This policy separates source progress from bounded verification. A replay is an
operator-requested audit of an inclusive interval; it is not evidence that the
normal forward cursor should move.

## Correction-risk inventory

| Layer/source | Late arrival | Existing record can change/disappear | Deterministic replay | Update support | Delete support |
|---|---|---|---|---|---|
| Aave Pool logs | Yes, when capture was unavailable or incomplete | Yes, after a chain reorganization | Yes: chain + transaction hash + log index | Bronze/Silver replacement and Snowflake MERGE can update/replay identities | No safe canonical tombstone is currently persisted |
| Uniswap pool logs | Yes, across WebSocket gaps | Yes, after a chain reorganization | Yes: chain + transaction hash + log index | Recovery Bronze, Silver, and Snowflake MERGE support additions; warehouse analytics currently model Swap only | No safe canonical tombstone is currently persisted |
| Chainlink proxy rounds | Yes, recovered rounds can arrive after events | A round can be corrected by a replayed proxy response; omission can be filled | Yes: chain + feed proxy + round ID | Snowflake MERGE and the bounded PIT price-window logic support corrected/late rounds | Hard deletion is not supported or appropriate for an unproven omission |
| Silver | Yes, when Bronze/recovery inputs appear | Replacement builds can change a retained row | Yes at the event/observation natural key | Yes | No source-canonicality marker today |
| Snowflake RAW | Yes through repeatable COPY/MERGE | A newer source row can update the natural key | Yes | Yes | Loader intentionally has no delete clause |
| dbt incremental models | Yes through propagated `loaded_at` timestamps | Late Chainlink input can revise historical PIT rows | Yes through MERGE keys | Yes | A full refresh cannot infer a reorg deletion without an upstream tombstone |
| Completed-range ledger | Not a data store | Evidence must not be rewritten by replay | Exact retries are deterministic | Append-only evidence only | Not applicable |
| Production watermarks | Advance only after successful forward work | Must never regress automatically | Optimistic commit contract | Forward-only | No replay mutation permitted |

Terms are intentionally distinct:

- **Late arrival:** a genuine canonical identity was absent and later appears.
- **Duplicate replay:** the same identity and source content is observed again.
- **True correction:** the same identity is returned with different source content.
- **Reorganization:** a previously retained blockchain identity is no longer in
  the canonical bounded source response, or canonical content changes.

## Operational safety lag

Aave, Chainlink, and Uniswap currently subtract 1,200 blocks from the observed
head before forward work. This is an operational confirmation/safety window. It
reduces exposure to very recent source instability and gives providers time to
serve consistent data. It does not prove protocol finality, guarantee that a
deeper reorganization cannot occur, or detect a provider omission.

The same value remains configured for all three sources because no repository
measurement currently justifies a change. The semantic protection differs:
Aave and Uniswap defer event logs, while Chainlink pins proxy reads to one safe
block. Source-specific values remain separately configurable.

## Processing modes and state rules

1. **Normal forward:** the existing source-specific entrypoint processes
   `watermark + 1` through a frozen safe head, performs downstream checks, and
   commits the watermark last.
2. **Historical replay:** an operator supplies a bounded interval. It is
   add-only; a missing or changed retained identity is rejected and escalated
   to correction review.
3. **Correction replay:** the same bounded fetch classifies unchanged, added,
   changed, and missing-from-source identities. It never advances or regresses
   forward watermarks.

The replay coordinator deliberately has no watermark commit or completed-range
store dependency. It snapshots all production cursors before and after work and
fails if any changed. Existing completed-range rows continue to mean only that
an interval was checked successfully under its source contract. Replay activity
is recorded in the Airflow/run logs; it does not create duplicate coverage
evidence or pretend that source output is immutable.

## Canonicality and deletion policy

Hard deletion is rejected. It could destroy audit evidence, erase a record
outside the intended interval, or propagate a provider's partial response as a
chain reorganization.

The selected long-term removal mechanism is an auditable soft
canonicality/tombstone override keyed by the deterministic source identity. dbt
staging would exclude an approved non-canonical identity while RAW and Bronze
remain intact. This milestone's coordinator exposes an explicit apply callback
for that mechanism but does not fabricate or auto-provision the durable
tombstone store. Until that owner-approved control object exists, live replay is
detect-first: additions/changes/removals are reported and no destructive
mutation occurs. An incomplete RPC response is rejected before comparison.

## Lookback policy

Normal forward processing remains `watermark + 1 -> safe head` for all sources.
Automatic overlap is not enabled:

- Aave and Uniswap overlap would conflict with immutable completed-range
  evidence and independently committed range objects without yet providing an
  approved tombstone path.
- Chainlink already uses per-feed round cursors and phase-aware recovery; a
  block overlap is not the correct unit for missing-round reconciliation.

Explicit bounded replay is safer and auditable at the current scale. A measured
rate of shallow corrections could justify a later source-specific overlap.

## Downstream behavior

Late or corrected Chainlink observations use the existing dbt interval logic:
the newly changed observation identifies the preceding and following effective
timestamps, and only event-token rows in that bounded window are merged. The
no-future-price rule and 300-second stale policy remain unchanged.

Added or corrected Uniswap Swap records can flow through the existing Silver,
RAW, incremental fact, valued fact, PIT, and tutorial-model MERGEs. A confirmed
removal must first receive the approved tombstone representation; full refresh
alone is not a substitute for source canonicality evidence.

## Concurrency and failure safety

Uniswap replay acquires the same durable source lease used by normal gap
recovery, so those paths cannot query/apply the source contract concurrently.
Aave and Chainlink audit runs are read-only and use the existing exclusive
`warehouse_writes` Airflow pool. Their current forward paths do not have
pre-provisioned source leases; mutation through replay therefore remains
disabled rather than introducing a second locking framework.

RPC incompleteness, reconciliation conflicts, downstream failures, or a stale
Uniswap lease leave production watermarks unchanged. Retrying an unchanged
audit produces the same classification and adds neither business rows nor
completed-range evidence.
