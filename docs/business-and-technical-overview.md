# Vehicle Parts Market Potential and Share

## Executive summary

This project demonstrates how Databricks can process automotive fleet, vehicle-documentation, part applicability, replacement-rate, claims, and sales data to produce part-level market potential and captured-share measures. The central engineering problem is resolving a large many-to-many applicability domain: which parts apply to which in-operation vehicles, for which market and fitment position, under effective-dated BM/BR/model and option rules.

The current workspace run uses generated synthetic data and illustrative rules. It proves that the workflow can execute in Databricks and registers queryable Delta tables. It does not establish that any rule, replacement rate, claim definition, market-share formula, or performance result represents Daimler Truck production behavior.

## Business problem

An OEM or parts organization needs to estimate the serviceable opportunity for each part in each market. The answer depends on more than catalog membership: vehicle configuration and model identifiers, active fleet population, fitment position, option codes, rule effective dates, and market restrictions all affect the addressable base. Claims can describe warranty replacements, while commercial sales capture parts sold; confusing those datasets produces misleading share calculations.

The Databricks pipeline addresses the data-processing shape by applying explicit rules through distributed joins, aggregating eligible vehicle populations, and keeping claims and commercial sales as separate measures. With approved business rules and representative source data, this can support repeatable market sizing, parts planning, warranty analysis, and part/market demand forecasting.

## What was achieved in the Databricks workspace

- Deployed and ran the workflow on Databricks serverless compute.
- Implemented separate Spark join lanes for exact BM+BR, BM-only, BR-only, and model-type-level fitment rules.
- Applied market, effective-date, and required-option filters without constructing the full fleet-by-catalog result as the main algorithm.
- Compared the optimized path with a deliberately bounded cross-join reference on a synthetic sample and checked exact result parity.
- Built Gold aggregation for unique eligible VIN/part/market population, annual replacement potential, approved synthetic warranty claims, synthetic sales, and sales-based share.
- Prepared a separate Many Model Forecasting workflow. It remains unrun because it requires an approved historical part-market demand series.

## Registered Unity Catalog objects

The write-enabled workflow targets `workspace.supply_chain_forecasting` in the connected Databricks workspace. The latest successful run read each table back through Spark and returned row counts. Its nine synthetic Delta tables are:

| Layer | Table | Contents |
|---|---|---|
| Bronze | `workspace.supply_chain_forecasting.bronze_parts_master` | Generated part number and active flag |
| Bronze | `workspace.supply_chain_forecasting.bronze_vedoc_vehicles` | Generated VIN, market, model type, BM/BR and installed options |
| Bronze | `workspace.supply_chain_forecasting.bronze_vpm_fleet` | Generated active fleet rows, effective dates and vehicle age |
| Silver | `workspace.supply_chain_forecasting.silver_fitment_rules` | Generated effective-dated rules and fitment positions |
| Silver | `workspace.supply_chain_forecasting.silver_parts_applicability` | Rule-backed VIN/part/market/position matches |
| Silver | `workspace.supply_chain_forecasting.silver_replacement_rates` | Generated market/part/age-band annual replacement assumptions |
| Silver | `workspace.supply_chain_forecasting.silver_claims` | Generated approved claim-line quantities |
| Silver | `workspace.supply_chain_forecasting.silver_sales` | Generated commercial sale quantities, independent of claims |
| Gold | `workspace.supply_chain_forecasting.gold_market_potential_share` | Annual potential, eligible fleet, claims, sales and illustrative captured-share measures |

Verified table row counts from the latest successful write-enabled run:

| Table | Rows |
|---|---:|
| `bronze_parts_master` | 500 |
| `bronze_vedoc_vehicles` | 10,000 |
| `bronze_vpm_fleet` | 10,000 |
| `silver_fitment_rules` | 500 |
| `silver_parts_applicability` | 485,848 |
| `silver_replacement_rates` | 4,000 |
| `silver_claims` | 13,013 |
| `silver_sales` | 21,032 |
| `gold_market_potential_share` | 500 |

The tables are visible in Catalog Explorer under catalog `workspace` and schema `supply_chain_forecasting`. They are managed Delta tables and are overwritten on each demo run.

Tables are written in overwrite mode for repeatable demo runs. They are synthetic and belong in a demo namespace, not a production data product.

## Technical processing

1. Generate deterministic source-shaped Spark DataFrames for parts, vehicles, rules, VPM population, and replacement rates.
2. Filter active parts and in-force rules; split the rules by match specificity.
3. Join by model/BM/BR keys, then apply market and all-required-option conditions.
4. Preserve matched rule ID/version and fitment position for traceability.
5. Deduplicate to the VIN/part/market grain before applying annual replacement-rate assumptions.
6. Aggregate warranty claims and commercial sales independently. Only synthetic commercial sales feed the sample share calculation.
7. Write Bronze/Silver/Gold outputs to managed Delta tables in Unity Catalog.
8. Run a limited correctness/performance comparison and return its measurements from the Databricks job.
9. Read the registered tables back through Spark and return row counts in the job result.

## Benchmark evidence and interpretation

On the latest write-enabled run, the 5,000-vehicle by 400-part benchmark considered 2,000,000 candidate VIN/part pairs. The rule-based output returned 194,173 matches, with zero set differences from the baseline and a 90.29% reduction in candidate VIN/part combinations. The latest execution measured 1.216 seconds for the join path and 1.162 seconds for the forced cross-join baseline.

Therefore the evidence demonstrates candidate-space reduction and correctness parity for the generated fixture, but **does not demonstrate a meaningful runtime improvement**. The join path was slightly slower in this run; timings vary between runs, and the Spark baseline is not a faithful benchmark of the original production implementation. The workflow contains no claim of “one week to minutes” or any other unmeasured improvement.

For a management performance claim, run repeatable comparisons on representative source volumes and rule distributions with the same cluster/runtime. Capture end-to-end elapsed time, DBU/cost, input/output bytes, shuffle/spill, skew, task distribution, output parity, and the existing process baseline. Use medians and variability across repeated trials.

## Assumptions requiring business validation

- `F`, `AF`, BM, BR, engine/front/rear fitment, and option semantics in this project are fabricated examples.
- Null BM/BR in a rule is treated as a wildcard; null rule market applies to all markets.
- All required option codes must be present on the vehicle.
- Rule effective dates are inclusive, and null end date is open-ended.
- Replacement rates are illustrative and not derived from Aqua or approved actuarial/aftermarket history.
- Claims are not sales. Market share is calculated only when a commercial sales dataset is supplied.
- The sample share formula divides commercial sold quantity by annual expected replacement potential; the enterprise numerator, denominator, period, and treatment of competitor sales must be agreed.
- Multiple matching rules/positions are retained as applicability detail; potential counts each VIN/part/market once.

## Next steps to make this business-grade

1. Map the real source systems and agree source ownership, grains, keys, refresh cadence, effective dating, and data-quality checks.
2. Obtain an approved fitment-rule catalog and validate precedence, BM/BR hierarchy, wildcard handling, options, markets, positions, supersession, and exclusions with domain SMEs.
3. Load representative, access-controlled VeDoc, VPM, parts, Aqua, and commercial sales extracts into governed Bronze tables.
4. Agree eligible-fleet, replacement-rate, claim-eligibility, and captured-market-share definitions with business owners.
5. Reconcile sample VIN/part decisions against trusted MPMS outputs, investigate mismatches, and establish an auditable rule version.
6. Benchmark at production scale and tune partitions, data layout, statistics, and skew handling based on query plans and Spark UI evidence.
7. Build monthly part-market history from an approved demand measure, then run backtesting and model comparison through Many Model Forecasting.
8. Add data-quality expectations, lineage, access controls, job alerts, incremental processing, and controlled releases before production use.