# Architecture

## Processing flow

```text
Parts master + fitment rules ─┐
VeDoc vehicle configurations ─┼─> Spark applicability joins ─> Silver VIN/part/position matches
VPM active fleet ─────────────┘                                  │
                                                                  ├─> Gold market potential/share
Aqua-like warranty claims ────────────────────────────────────────┤
Commercial part sales ────────────────────────────────────────────┘

Approved monthly part/market demand ─> Many Model Forecasting ─> Gold forecasts + MLflow metrics
```

The applicability resolver separates exact BM+BR, BM-only, BR-only, and model-type-only rule lanes. It filters active parts, rule effectiveness, market, and required options in Spark. It emits matched rules and fitment position for traceability rather than generating every false vehicle/part pair.

The source generator exists to validate schemas and run the workflow without production data. Replace it with governed Delta source tables and approved rule tables before using business results. The forecasting workflow is separate because applicability and fleet potential do not constitute a time series; it requires an approved historical demand source.

## Components

- `src/supply_chain_forecasting/applicability.py`: scalable rule matching.
- `src/supply_chain_forecasting/gold_metrics.py`: fleet potential, claim quantities, sales, and share aggregation.
- `src/supply_chain_forecasting/benchmark.py`: bounded cross-join reference, exact result comparison, and timing capture.
- `src/supply_chain_forecasting/synthetic_data.py`: deterministic Spark-generated source-shaped datasets.
- `notebooks/01_market_potential_share.py`: end-to-end applicability and market metrics workflow.
- `notebooks/02_part_market_forecast.py`: optional MLflow-tracked part/market forecasting workflow using Databricks MMF.
- `resources/jobs.yml`: serverless Databricks Workflow definition.

## Operational controls

Delta output is disabled by default. When enabled, the workflow validates catalog and schema identifiers and writes to named tables in the selected Unity Catalog schema. The cross-join baseline is limited to two million vehicle/part candidates. No secrets or production data are stored in this project.