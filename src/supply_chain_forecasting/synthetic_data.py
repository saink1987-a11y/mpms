from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_synthetic_sources(
    spark: SparkSession,
    vehicle_count: int,
    part_count: int,
) -> dict[str, DataFrame]:
    """Generate deterministic, distributed source-shaped data for validation."""
    if vehicle_count < 1 or part_count < 1:
        raise ValueError("vehicle_count and part_count must be positive")

    markets = ["US", "DE", "FR", "UK"]
    market_array = F.array(*[F.lit(market) for market in markets])
    vehicles = (
        spark.range(vehicle_count)
        .select(
            F.concat(F.lit("VIN"), F.lpad(F.col("id").cast("string"), 10, "0")).alias("vin"),
            "id",
        )
        .withColumn(
            "market_code",
            F.element_at(market_array, (F.col("id") % 4 + 1).cast("int")),
        )
        .withColumn("model_type", F.when(F.col("id") % 2 == 0, "F").otherwise("AF"))
        .withColumn(
            "bm_type",
            F.concat(F.lit("BM-"), F.lpad((F.col("id") % 8 + 1).cast("string"), 3, "0")),
        )
        .withColumn(
            "br_type",
            F.concat(F.lit("BR-"), F.lpad((F.col("id") % 4 + 1).cast("string"), 2, "0")),
        )
        .withColumn(
            "installed_options",
            F.when(
                F.col("id") % 3 == 0,
                F.array(F.lit("OPT-A"), F.lit("OPT-C")),
            ).otherwise(F.array(F.lit("OPT-B"))),
        )
        .drop("id")
    )

    parts = (
        spark.range(part_count)
        .select(
            F.concat(F.lit("PART-"), F.lpad(F.col("id").cast("string"), 6, "0")).alias(
                "part_number"
            ),
            "id",
        )
        .withColumn("is_active", F.lit(True))
        .drop("id")
    )

    part_index = F.regexp_extract("part_number", r"(\d+)$", 1).cast("int")
    rules = (
        parts.withColumn("part_index", part_index)
        .withColumn("rule_id", F.concat(F.lit("RULE-"), F.col("part_number")))
        .withColumn("rule_version", F.lit("synthetic-v1"))
        .withColumn("model_type", F.when(F.col("part_index") % 2 == 0, "F").otherwise("AF"))
        .withColumn(
            "bm_type",
            F.concat(F.lit("BM-"), F.lpad((F.col("part_index") % 8 + 1).cast("string"), 3, "0")),
        )
        .withColumn(
            "br_type",
            F.concat(F.lit("BR-"), F.lpad((F.col("part_index") % 4 + 1).cast("string"), 2, "0")),
        )
        .withColumn(
            "market_code",
            F.when(F.col("part_index") % 5 == 0, F.lit(None).cast("string"))
            .otherwise(F.element_at(market_array, (F.col("part_index") % 4 + 1).cast("int"))),
        )
        .withColumn(
            "fitment_position",
            F.element_at(
                F.array(F.lit("ENGINE"), F.lit("FRONT"), F.lit("REAR")),
                (F.col("part_index") % 3 + 1).cast("int"),
            ),
        )
        .withColumn(
            "required_options",
            F.when(F.col("part_index") % 3 == 0, F.array(F.lit("OPT-A")))
            .otherwise(F.array().cast("array<string>")),
        )
        .withColumn("valid_from", F.lit(date(2020, 1, 1)))
        .withColumn("valid_to", F.lit(None).cast("date"))
        .withColumn("is_active", F.lit(True))
        .drop("part_index")
    )

    vpm = (
        vehicles.withColumn(
            "vehicle_age_years", F.pmod(F.xxhash64("vin"), F.lit(21)).cast("int")
        )
        .withColumn("valid_from", F.lit(date(2020, 1, 1)))
        .withColumn("valid_to", F.lit(None).cast("date"))
        .withColumn("is_active", F.lit(True))
    )

    market_df = spark.createDataFrame([(market,) for market in markets], ["market_code"])
    age_rates = spark.createDataFrame(
        [(0, 9, 0.035), (10, 20, 0.075)],
        ["age_min_years", "age_max_years", "annual_replacement_rate"],
    )
    replacement_rates = (
        parts.select("part_number")
        .crossJoin(market_df)
        .crossJoin(age_rates)
        .withColumn("is_active", F.lit(True))
    )

    return {
        "parts": parts,
        "vehicles": vehicles,
        "fitment_rules": rules,
        "vpm": vpm,
        "replacement_rates": replacement_rates,
    }