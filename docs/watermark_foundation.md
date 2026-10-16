# Watermark and Incremental-State Foundation

This design separates source observation, active processing, and durable
completion:

- **Source high-water mark**: the highest position currently observable from
  a source, such as the confirmed Arbitrum block number.
- **Processing position**: the finite range a job is currently attempting.
- **Committed watermark**: the highest source position whose required data,
  persistence, warehouse work, and validation have all succeeded.

A Kafka offset, Spark checkpoint, Airflow logical date, task status, XCom,
Snowflake COPY history, or dbt state is not the committed application
watermark. Those mechanisms retain their existing responsibilities.

## Current progress inventory

| Pipeline | Natural position | Current durable progress | Explicit forward watermark |
|---|---|---|---|
| Uniswap/Aave WebSocket producer | block number plus transaction/log identity | none at the source; Kafka retains published messages | No for the current live service. A future gap-recovery batch would need its own block cursor. |
| Chainlink poller | pinned block and per-feed `round_id` | published rounds are in memory only; Kafka retains published observations | Yes for a future scheduled forward capture, using block number at the pipeline boundary. |
| Blockchain Spark Bronze | Kafka partition/offset | Spark checkpoint | No; the checkpoint is authoritative for stream consumption. |
| Market-data Spark Bronze | Kafka partition/offset | Spark checkpoint | No; the checkpoint is authoritative for stream consumption. |
| Uniswap Silver | Bronze object identity | local processed-file manifest; cloud mode deterministically rebuilds | No while current rebuild semantics remain. |
| Aave Silver | complete Bronze inputs | deterministic full rebuild and event-key deduplication | No. |
| Chainlink Silver | complete live and historical Bronze inputs | deterministic full rebuild and observation-key deduplication | No. |
| Price enrichment | complete Silver/reference inputs | deterministic full rebuild | No. |
| Snowflake RAW | staged object and natural key | Snowflake COPY history, landing `merged_at`, deterministic MERGE | No separate loader watermark. It is a required boundary before a forward source watermark commits. |
| dbt STAGING/CORE/MARTS | complete RAW relations | views and deterministic table rebuilds | No. |
| Bounded Aave backfill | descending block range | S3 `next_end_block` checkpoint | No forward watermark; this historical state remains separate. |
| Bounded Chainlink backfill | per-feed proxy phase and round | S3 per-phase `next_round_id` checkpoint | No forward watermark; this historical state remains separate. |

## Durable control state

The authoritative store is
`BLOCKCHAIN_ANALYTICS.CONTROL.PIPELINE_WATERMARKS`, separate from analytical
RAW, STAGING, CORE, and MARTS objects. It stores one current row per identity:

- `pipeline_name`
- `source_name`
- `chain_id` as `NUMBER(38, 0)`
- `watermark_type`
- `committed_value` as `NUMBER(38, 0)`
- `successful_run_id`
- `updated_at`

Initial forward identities are `aave_forward` / `aave_v3_pool` and
`chainlink_forward` / `chainlink_proxy_rpc`, both for Arbitrum chain ID 42161
and a `block_number` cursor. There is intentionally no global blockchain
watermark.

A read-only warehouse inspection on 2026-10-13 found a maximum represented
block of 512,036,686 for both current Aave and Chainlink canonical rows. That
maximum is evidence about stored records, not proof that every earlier source
block was scanned. The safe no-skip initialization value is therefore **0**
for both pipelines until an authoritative Aave deployment block and each
Chainlink proxy's effective start boundary are verified as complete bootstrap
boundaries. Production watermarks must not be initialized from the observed
maximum alone.

The infrastructure owner provisions the object with
`warehouse/provision_watermarks.sql`. Runtime access uses
`BLOCKCHAIN_PIPELINE_ROLE`, key-pair authentication, secondary roles disabled,
and only `SELECT`, `INSERT`, and `UPDATE` on the exact control table.

## Initialization and commit contract

Missing state is an error for forward processing. Initialization is an
explicit operation and must distinguish a verified bootstrap position, a new
pipeline start, or an isolated test identity. It must never infer chain head,
latest S3 data, or current time.

For committed value 100 and attempted range 101–200, any failed required step
leaves 100 unchanged. Only after source processing, persistence, warehouse/dbt
work, and reconciliation succeed may the caller commit 200. Equal commits are
idempotent no-ops; lower commits are rejected. Optimistic comparison prevents
a stale writer from replacing concurrently advanced state.

The existing bounded backfill is historical replay and is explicitly barred
from committing a forward watermark.

## Aave project collection boundary

The supported Aave analytics boundary is a project decision, not an Aave
deployment claim: `2026-10-01T00:00:00Z`. A deterministic Arbitrum timestamp
binary search resolved the first block at or after that boundary as
`510496864`. Its timestamp is exactly `2026-10-01T00:00:00Z`; the immediately
preceding block, `510496863`, is timestamped `2026-09-30T23:59:59Z`.

Forward bootstrap uses an operational lag of 1,200 blocks. This is a
configurable guard against processing the newest L2 blocks and is not a claim
about protocol finality. At the 2026-10-13 measurement, head `514554979`
produced safe end `514553779`; the lag represented about 359 seconds. The
inclusive bootstrap interval therefore contained 4,056,916 blocks. With the
current Alchemy-compatible ten-block request limit, it requires 405,692
source chunks, so execution must be bounded and resumable rather than one
unbounded run.

Initial Alchemy range calibration tested known-event intervals of 1, 10, 20, 50,
100, 500, 1,000, 5,000, and 10,000 blocks. One- and ten-block requests
succeeded, including decoding a real Borrow event without quarantine. Every
request above ten blocks returned HTTP 400 because that Alchemy project
limited `eth_getLogs` to ten blocks. This correctly blocked execution rather
than issuing more than 400,000 requests.

Bounded Aave bootstrap and recovery now use a separately configured Ankr RPC;
Alchemy remains the live WebSocket and streaming provider. Known-event Ankr
calibration succeeded for 100, 1,000, 2,000, and 3,000 blocks in one request
each, with complete Borrow/Repay decoding and no decode failures. A single
5,000-block probe returned provider error `-32062` (block range too large).
The calibrated policy is therefore a 3,000-block preferred/maximum parent,
100-block minimum fallback, and a finite 64-attempt per-parent budget.
Adaptive bisection preserves contiguous child boundaries and never records a
failed parent as complete.

A read-only Ankr head estimate on 2026-10-14 observed block `514894541`,
yielding estimated safe end `514893341` under the same 1,200-block lag. That
snapshot spans 4,396,478 blocks, or 1,466 logical 3,000-block parents. It is an
estimate, not a committed bootstrap boundary; the safe end must be refreshed
again immediately before an approved execution.

Successful source intervals are recorded in the append-only
`BLOCKCHAIN_ANALYTICS.CONTROL.PIPELINE_COMPLETED_RANGES` ledger. The runtime
role receives only `SELECT` and `INSERT`. Each interval is recorded only after
its source output has persisted successfully. Planning always resumes at the
first uncovered block, merges adjacent or overlapping evidence when computing
coverage, and never jumps across a gap. Reprocessing an exact completed range
is a safe no-op in the ledger.

The infrastructure owner must provision the ledger using
`warehouse/provision_completed_ranges.sql`. Until that object exists and the
entire project boundary through a measured safe end is proven gap-free, the
The accepted one-time Aave bootstrap range evidence remains immutable under
the legacy `alchemy_rpc` source identity. Production watermark state and all
new completed forward ranges use the provider-neutral `aave_v3_pool` source
identity; coverage checks combine both evidence sets without rewriting history.
Until explicit initialization, the `aave_forward` watermark remains
uninitialized. Historical bounded backfills
remain isolated and cannot write either this forward coverage evidence or the
production forward watermark.

## Replay and lookback

- **Aave:** do not choose an overlap count until a confirmed/safe-head and
  reorganization policy is established. If replay is introduced, it should be
  block-based and rely on the existing transaction/log event identity for
  deterministic deduplication.
- **Chainlink:** keep the committed pipeline boundary block-based. Extraction
  may use its existing source-specific per-feed/per-phase round overlap to
  recover retryable or boundary observations; a global round ID is invalid
  across feeds. The existing bounded path already supports explicit
  `overlap_rounds`.

Full replay policy belongs to Milestone 11.2D.

## Future Airflow flow

```text
read_watermark
      ↓
discover_source_high_water_mark
      ↓
calculate_processing_range
      ↓
process_increment
      ↓
validate_output
      ↓
warehouse / dbt processing
      ↓
commit_watermark
```

The commit task must remain downstream of every success condition represented
by the watermark. This milestone does not change any existing DAG or data job
to incremental behavior.
