# Databricks notebook source
# MAGIC %md
# MAGIC # Part/Market Demand Forecast
# MAGIC Uses the Databricks Many Model Forecasting project. Provide a validated monthly demand history; do not derive commercial demand from warranty claims alone.

# COMMAND ----------

# MAGIC %pip install "mmf_sa[local] @ git+https://github.com/databricks-industry-solutions/many-model-forecasting.git"

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog")
dbutils.widgets.text("schema", "supply_chain_forecasting", "Schema")
dbutils.widgets.text("experiment_path", "/Shared/supply_chain_demand_forecasting", "MLflow experiment")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
experiment_path = dbutils.widgets.get("experiment_path")

from mmf_sa import run_forecast
import mlflow

mlflow.set_experiment(experiment_path)

# Expected input grain: one monthly observation per series_id and month-end ds.
training_table = f"{catalog}.{schema}.gold_monthly_part_market_demand"
forecast_table = f"{catalog}.{schema}.gold_part_market_forecast"
evaluation_table = f"{catalog}.{schema}.forecast_evaluation"

if not spark.catalog.tableExists(training_table):
    raise ValueError(
        f"Missing {training_table}. Create it from an approved demand source with "
        "series_id STRING, ds TIMESTAMP (month-end), and y_demand DOUBLE."
    )

run_forecast(
    spark=spark,
    train_data=training_table,
    scoring_data=training_table,
    scoring_output=forecast_table,
    evaluation_output=evaluation_table,
    group_id="series_id",
    date_col="ds",
    target="y_demand",
    freq="M",
    prediction_length=12,
    backtest_length=12,
    stride=3,
    metric="smape",
    train_predict_ratio=2,
    data_quality_check=True,
    resample=True,
    active_models=["StatsForecastBaselineSeasonalNaive", "StatsForecastAutoETS", "StatsForecastAutoArima"],
    experiment_path=experiment_path,
    use_case_name="part_market_supply_chain",
)

# COMMAND ----------

display(spark.table(forecast_table).orderBy("series_id", "ds"))
display(spark.table(evaluation_table))