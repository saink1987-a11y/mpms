# Databricks notebook source
# MAGIC %md
# MAGIC # Vehicle Parts Market Potential and Share
# MAGIC This workflow uses synthetic source-shaped data to demonstrate the processing pattern. Replace the generator with governed source tables and approved applicability rules before using business results.

# COMMAND ----------

from datetime import date
from pathlib import Path
import json
import sys

from pyspark.sql import functions as F

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workspace_notebook = (
    Path(notebook_path)
    if notebook_path.startswith("/Workspace/")
    else Path("/Workspace") / notebook_path.lstrip("/")
)
root_candidates = [workspace_notebook.parent.parent, Path.cwd()]
repo_root = next(
    (
        candidate
        for candidate in root_candidates
        if (candidate / "src" / "supply_chain_forecasting").is_dir()
    ),
    None,
)
if repo_root is None:
    raise FileNotFoundError(
        f"Could not locate src/supply_chain_forecasting; checked {root_candidates}"
    )
sys.path.insert(0, str(repo_root / "src"))

from supply_chain_forecasting.applicability import resolve_applicability
from supply_chain_forecasting.benchmark import run_benchmark
from supply_chain_forecasting.gold_metrics import calculate_market_metrics
from supply_chain_forecasting.synthetic_data import build_synthetic_sources

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog")
dbutils.widgets.text("schema", "supply_chain_forecasting", "Schema")
dbutils.widgets.text("vehicle_count", "10000", "Synthetic vehicles")
dbutils.widgets.text("part_count", "500", "Synthetic parts")
dbutils.widgets.text("write_delta", "false", "Write Delta tables (true/false)")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
vehicle_count = int(dbutils.widgets.get("vehicle_count"))
part_count = int(dbutils.widgets.get("part_count"))
write_delta = dbutils.widgets.get("write_delta").lower() == "true"
reporting_year = 2025
as_of_date = date(reporting_year, 12, 31)

if write_delta:
    import re

    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog) or not re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_]*", schema
    ):
        raise ValueError("Catalog and schema must be simple Unity Catalog identifiers")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

# COMMAND ----------

sources = build_synthetic_sources(spark, vehicle_count, part_count)
parts = sources["parts"]
vehicles = sources["vehicles"]
fitment_rules = sources["fitment_rules"]
vpm = sources["vpm"]
replacement_rates = sources["replacement_rates"]

display(parts.limit(10))
display(vehicles.limit(10))
display(fitment_rules.limit(10))

# COMMAND ----------

applicable = resolve_applicability(parts, vehicles, fitment_rules, as_of_date)
applicable_count = applicable.count()
print(f"Rule-backed VIN/part/fitment-position matches: {applicable_count:,}")
display(applicable.limit(25))

if write_delta:
    applicable.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog}.{schema}.silver_parts_applicability"
    )

# COMMAND ----------

# Run a bounded legacy-style cross-join comparison, not on the full data.
benchmark_vehicles = vehicles.orderBy("vin").limit(5000)
benchmark_parts = parts.orderBy("part_number").limit(400)
benchmark_rules = fitment_rules.join(
    benchmark_parts.select("part_number"), "part_number", "inner"
)
benchmark = run_benchmark(
    benchmark_parts,
    benchmark_vehicles,
    benchmark_rules,
    as_of_date,
    max_candidate_pairs=2_000_000,
)
if benchmark["baseline_only_mismatch_count"] or benchmark["optimized_only_mismatch_count"]:
    raise AssertionError(f"Applicability outputs differ: {benchmark}")
print("Measured timings on this cluster and bounded input; not a general speedup claim:")
for name, value in benchmark.items():
    print(f"{name}: {value}")

# COMMAND ----------

# Synthetic Aqua-like approved claims and separate commercial-sales records.
claims = (
    applicable.filter(F.pmod(F.xxhash64("vin", "part_number"), F.lit(37)) == 0)
    .select("vin", "market_code", "part_number")
    .dropDuplicates(["vin", "market_code", "part_number"])
    .withColumn("claim_id", F.concat(F.lit("CLM-"), "vin", F.lit("-"), "part_number"))
    .withColumn("claim_date", F.lit(date(reporting_year, 6, 15)))
    .withColumn("claim_status", F.lit("APPROVED"))
    .withColumn("replacement_qty", F.lit(1.0))
)
sales = (
    applicable.filter(F.pmod(F.xxhash64("vin", "part_number"), F.lit(23)) == 0)
    .select("market_code", "part_number", "vin")
    .dropDuplicates(["market_code", "part_number", "vin"])
    .withColumn("sale_id", F.concat(F.lit("SALE-"), "vin", F.lit("-"), "part_number"))
    .withColumn("sale_date", F.lit(date(reporting_year, 7, 15)))
    .withColumn("sold_qty", F.lit(1.0))
    .drop("vin")
)

gold_metrics = calculate_market_metrics(
    applicable,
    vpm,
    replacement_rates,
    claims,
    sales,
    reporting_year,
)
display(gold_metrics.orderBy(F.desc("total_market_potential_qty")).limit(50))

if write_delta:
    gold_metrics.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog}.{schema}.gold_market_potential_share"
    )
    parts.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog}.{schema}.bronze_parts_master"
    )
    vehicles.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog}.{schema}.bronze_vedoc_vehicles"
    )
    vpm.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog}.{schema}.bronze_vpm_fleet"
    )
    fitment_rules.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog}.{schema}.silver_fitment_rules"
    )
    replacement_rates.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog}.{schema}.silver_replacement_rates"
    )
    claims.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog}.{schema}.silver_claims"
    )
    sales.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog}.{schema}.silver_sales"
    )

# COMMAND ----------

registered_table_names = [
    "bronze_parts_master",
    "bronze_vedoc_vehicles",
    "bronze_vpm_fleet",
    "silver_fitment_rules",
    "silver_parts_applicability",
    "silver_replacement_rates",
    "silver_claims",
    "silver_sales",
    "gold_market_potential_share",
]
registered_row_counts = {}
if write_delta:
    for table_name in registered_table_names:
        qualified_name = f"{catalog}.{schema}.{table_name}"
        registered_row_counts[table_name] = spark.table(qualified_name).count()
        print(f"{qualified_name}: {registered_row_counts[table_name]:,} rows")

applicable.explain(mode="formatted")
print("Workflow complete. Timings are specific to the current run and cluster.")
dbutils.notebook.exit(
    json.dumps(
        {
            "status": "success",
            "benchmark": benchmark,
            "delta_tables_written": write_delta,
            "target_schema": f"{catalog}.{schema}",
            "registered_tables": registered_table_names if write_delta else [],
            "registered_row_counts": registered_row_counts,
        }
    )
)