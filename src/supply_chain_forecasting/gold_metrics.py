from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def calculate_market_metrics(
    applicable: DataFrame,
    vpm: DataFrame,
    replacement_rates: DataFrame,
    claims: DataFrame,
    sales: DataFrame | None,
    year: int,
) -> DataFrame:
    """Calculate annual fleet potential, warranty claims, sales, and share."""
    year_start = date(year, 1, 1)
    year_end = date(year, 12, 31)
    eligible = (
        applicable.filter(F.col("is_applicable"))
        .select("vin", "part_number", "market_code")
        .dropDuplicates(["vin", "part_number", "market_code"])
        .join(
            vpm.filter(
                (F.col("is_active") == F.lit(True))
                & (F.col("valid_from") <= F.lit(year_end))
                & (F.col("valid_to").isNull() | (F.col("valid_to") >= F.lit(year_start)))
            ).select("vin", "market_code", "vehicle_age_years"),
            ["vin", "market_code"],
            "inner",
        )
        .alias("e")
    )

    rate_rows = replacement_rates.filter(F.col("is_active")).alias("r")
    rated = eligible.join(
        rate_rows,
        on=(
            (F.col("e.part_number") == F.col("r.part_number"))
            & (F.col("e.market_code") == F.col("r.market_code"))
            & (F.col("e.vehicle_age_years") >= F.col("r.age_min_years"))
            & (F.col("e.vehicle_age_years") <= F.col("r.age_max_years"))
        ),
        how="left",
    ).select(
        F.col("e.vin").alias("vin"),
        F.col("e.part_number").alias("part_number"),
        F.col("e.market_code").alias("market_code"),
        F.col("r.annual_replacement_rate").alias("annual_replacement_rate"),
    )

    potential = rated.groupBy("market_code", "part_number").agg(
        F.countDistinct("vin").alias("eligible_vehicle_count"),
        F.sum(F.coalesce(F.col("annual_replacement_rate"), F.lit(0.0))).alias(
            "total_market_potential_qty"
        ),
    )

    claim_totals = (
        claims.filter(
            (F.col("claim_status") == "APPROVED")
            & (F.year("claim_date") == year)
        )
        .groupBy("market_code", "part_number")
        .agg(F.sum("replacement_qty").alias("warranty_claim_qty"))
    )

    result = potential.join(claim_totals, ["market_code", "part_number"], "left")
    if sales is not None:
        sales_totals = (
            sales.filter(F.year("sale_date") == year)
            .groupBy("market_code", "part_number")
            .agg(F.sum("sold_qty").alias("commercial_sales_qty"))
        )
        result = result.join(sales_totals, ["market_code", "part_number"], "left")
    else:
        result = result.withColumn("commercial_sales_qty", F.lit(None).cast("double"))

    return (
        result.withColumn("reporting_year", F.lit(year))
        .withColumn(
            "captured_market_share_pct",
            F.when(
                F.col("commercial_sales_qty").isNotNull()
                & (F.col("total_market_potential_qty") > 0),
                100.0 * F.col("commercial_sales_qty") / F.col("total_market_potential_qty"),
            ),
        )
        .withColumn("warranty_claim_qty", F.coalesce("warranty_claim_qty", F.lit(0.0)))
        .select(
            "market_code",
            "part_number",
            "reporting_year",
            "eligible_vehicle_count",
            "total_market_potential_qty",
            "warranty_claim_qty",
            "commercial_sales_qty",
            "captured_market_share_pct",
        )
    )