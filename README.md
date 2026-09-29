# Supply Chain Demand Forecasting

Databricks implementation scaffold for automotive parts applicability, market potential/share, and part-market demand forecasting. This project is separate from the retail database migration workspace.

The processing code is structured for Spark and Delta Lake. Source-shaped synthetic data is used only to exercise the pipeline and validate the join logic; it is not production data and does not represent Daimler Truck internal MPMS rules.

## Workflows

- `notebooks/01_market_potential_share.py`: creates synthetic parts, VeDoc-like vehicles, applicability rules, and VPM rows; computes applicable VIN/part/position matches; calculates annual potential, claims, sales, and market share; measures the optimized join path against a bounded cross-join reference.
- `notebooks/02_part_market_forecast.py`: optional 12-month part-market forecasting with Databricks Many Model Forecasting, MLflow tracking, and backtest metrics. It requires an approved monthly demand history table.
- `src/supply_chain_forecasting/`: reusable Spark transformations and synthetic source generator.
- `resources/jobs.yml`: Databricks Workflow resource for the market-potential notebook.
- `docs/architecture.md`: processing flow and component responsibilities.
- `docs/data-contract.md`: source and output grains, fields, and business assumptions.
- `docs/performance.md`: benchmark method and the currently observed workspace result.

## Run in Databricks

1. Use the Databricks CLI with an authenticated profile, then validate the bundle: `databricks bundle validate -t dev`.
2. Deploy and run the workflow: `databricks bundle deploy -t dev`, then `databricks bundle run supply_chain_demand_forecasting -t dev`.
3. For interactive exploration, open `notebooks/01_market_potential_share.py` in the workspace and run it on approved compute. Set catalog/schema widgets to a namespace where you have permission. Delta writes default off.
4. To forecast, create `gold_monthly_part_market_demand` with `series_id STRING`, `ds TIMESTAMP` aligned to month end, and `y_demand DOUBLE`, then run the second notebook on a runtime supported by the current MMF project.

No credentials, tokens, endpoints, or real customer data belong in this repository. The Asset Bundle uses the active Databricks CLI authentication.

## Applicability assumptions in the synthetic source generator

- `F` and `AF` are illustrative model-type values, not decoded Daimler codes.
- BM/BR identifiers are generated values. The source generator creates exact BM+BR rules; the resolver supports exact, BM-only, BR-only, and model-type-only rules by treating null BM/BR rule fields as wildcards.
- A null rule market means all markets; otherwise it must equal the vehicle market.
- All options listed in `required_options` must exist in `installed_options`.
- Rule dates are inclusive; a null `valid_to` means no end date.
- Fitment position is supplied by a rule; it is never inferred from part text.
- Annual replacement rates and generated claims/sales are fabricated inputs for demonstrating transformations only.

Before using real data, replace the generator and approve rule precedence, BM/BR hierarchy, options semantics, fitment position, market restrictions, effective dating, part supersession, active VPM population, claim eligibility, demand source, and the business definition of market share.

## Performance measurement

The notebook times both strategies on the same bounded sample (default up to 5,000 vehicles × 400 parts), forces result materialization, and compares exact output sets. The reference path estimates vehicle × part candidates and refuses to run past its two-million-pair limit. It must not be run on production-sized inputs. Report observed elapsed times, candidate reduction, parity, and Spark UI/query-plan evidence; do not present an unmeasured speedup as fact. Runtime depends on compute, data distribution, cache state, table layout, and workload concurrency.

## Reference implementations

- [Parts Demand Forecasting](https://github.com/databricks-industry-solutions/parts-demand-forecasting)
- [Many Model Forecasting](https://github.com/databricks-industry-solutions/many-model-forecasting)
- [Supply Chain Stress Test](https://github.com/databricks-industry-solutions/supply-chain-stress-test)
- [Automotive Lakehouse Data Models](https://github.com/databricks-industry-solutions/lakehouse-industry-data-models/tree/main/data-models/automotive)