# Incremental Warehouse Strategy

Milestone 11.2E adopts a mixed dbt materialization strategy. The measured
full-model baseline was 407.65 seconds. Views below report definition-creation
time; their evaluation cost is paid by downstream consumers.

Decision classes:

- A: keep the existing full/view behavior for correctness and simplicity.
- B: strong native incremental candidate.
- C: incremental with bounded historical recomputation.
- D: measured cost is too small to justify incremental state.

| Model | Before | Rows | Baseline seconds | Decision |
|---|---:|---:|---:|---|
| stg_aave_borrows | view | 12,438 | 0.47 | A |
| stg_aave_liquidations | view | 149 | 0.51 | A |
| stg_aave_repays | view | 8,939 | 0.47 | A |
| stg_chainlink_prices | view | 29,709 | 1.01 | A |
| stg_tokens | view | 39 | 0.49 | A |
| stg_uniswap_swaps | view | 1,932,224 | 0.44 | A |
| stg_uniswap_v3_pools | view | 68 | 0.39 | A |
| dim_chain | table | 1 | 2.67 | D |
| dim_date | table | 1,118 | 3.54 | D |
| dim_pool | table | 68 | 1.47 | D |
| dim_protocol | table | 3 | 2.62 | D |
| dim_token | table | 39 | 1.35 | D |
| fact_aave_borrow | table | 12,438 | 1.66 | D |
| fact_aave_liquidation | table | 149 | 1.63 | D |
| fact_aave_repay | table | 8,939 | 1.66 | D |
| fact_market_price | table | 29,709 | 1.62 | D |
| int_uniswap_swap_classification | view | 1,932,224 | 0.76 | A |
| int_uniswap_swap_exclusions | view | 102 | 0.47 | A |
| fact_uniswap_swap | table | 1,932,122 | 4.53 | B |
| int_defi_event_token_amounts | view | 3,885,919 | 0.47 | A |
| int_defi_event_token_prices | view | 3,885,919 | 0.60 | C |
| fact_aave_borrow_valued | table | 12,438 | 5.62 | D |
| fact_aave_liquidation_valued | table | 149 | 2.84 | D |
| fact_aave_repay_valued | table | 8,939 | 4.96 | D |
| fact_uniswap_swap_valued | table | 1,932,122 | 213.96 | C |
| mart_aave_lending_tutorial | table | 21,377 | 1.53 | D |
| mart_chainlink_oracle_tutorial | table | 29,709 | 1.46 | D |
| mart_uniswap_swap_tutorial | table | 1,932,122 | 7.31 | B |
| mart_point_in_time_pricing_tutorial | table | 3,885,919 | 388.75 | C |

## Incremental contracts

`fact_uniswap_swap` merges on `event_id`. It selects source records whose RAW
`loaded_at` is newer than the greatest load already represented in the target.
This handles both new events and deterministic replay updates. Reference-data or
classification contract changes require `--full-refresh` so rows can also leave
the canonical population.

`int_defi_event_token_prices` merges on
`event_domain + event_id + token_side`. New event inputs are selected by their
source load timestamp. Newly loaded Chainlink observations define a bounded
window from the preceding distinct effective price timestamp through the next
timestamp. This covers both the selected-price interval and the preceding
interval whose educational next-round value may change. Rows previously matched
to a corrected observation are also selected explicitly.

`fact_uniswap_swap_valued`, `mart_uniswap_swap_tutorial`, and
`mart_point_in_time_pricing_tutorial` merge on their established grains and
consume propagated change timestamps. This lets late prices update existing
events without rebuilding unaffected history.

Every incremental model supports `dbt run --full-refresh`. Full refresh is the
required recovery path for model-logic changes, broad historical corrections,
reference/classification changes, or source-contract changes. Normal Airflow
commands remain unchanged and therefore use dbt's incremental behavior.

Dimensions, reference models, Aave facts/marts, and Chainlink facts/marts stay
full rebuild because they contain at most 29,709 rows and measured between 1.35
and 5.62 seconds. Their simpler full-rebuild semantics are worth more than the
negligible runtime savings available at current scale.
