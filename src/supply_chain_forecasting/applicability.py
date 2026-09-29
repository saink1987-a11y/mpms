from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def resolve_applicability(
    parts: DataFrame,
    vehicles: DataFrame,
    fitment_rules: DataFrame,
    as_of_date: date,
    broadcast_rules: bool = False,
) -> DataFrame:
    """Return rule-backed applicable VIN/part/position matches.

    Expected rule columns: rule_id, rule_version, part_number, model_type,
    bm_type, br_type, market_code, fitment_position, required_options,
    valid_from, valid_to, is_active.
    Expected vehicle columns: vin, market_code, model_type, bm_type,
    br_type, installed_options.

    Each rule must specify model_type. BM and BR may be null to mean a
    model-type-level rule. A rule's required_options are ALL required.
    """
    active_parts = parts.filter(F.col("is_active") == F.lit(True)).select(
        "part_number"
    ).dropDuplicates()

    rules = fitment_rules.filter(
        (F.col("is_active") == F.lit(True))
        & (F.col("valid_from") <= F.lit(as_of_date))
        & (F.col("valid_to").isNull() | (F.col("valid_to") >= F.lit(as_of_date)))
    ).join(active_parts, on="part_number", how="inner")

    if broadcast_rules:
        rules = F.broadcast(rules)

    rule_rows = rules.select(
        F.col("rule_id").alias("matched_rule_id"),
        F.col("rule_version").alias("matched_rule_version"),
        F.col("part_number"),
        F.col("model_type").alias("rule_model_type"),
        F.col("bm_type").alias("rule_bm_type"),
        F.col("br_type").alias("rule_br_type"),
        F.col("market_code").alias("rule_market_code"),
        F.col("fitment_position"),
        F.coalesce(F.col("required_options"), F.array().cast("array<string>")).alias(
            "required_options"
        ),
    )

    vehicle_rows = vehicles.select(
        "vin",
        F.col("market_code").alias("vehicle_market_code"),
        F.col("model_type").alias("vehicle_model_type"),
        F.col("bm_type").alias("vehicle_bm_type"),
        F.col("br_type").alias("vehicle_br_type"),
        F.coalesce(F.col("installed_options"), F.array().cast("array<string>")).alias(
            "installed_options"
        ),
    )

    def match_lane(
        lane_rules: DataFrame,
        join_condition: F.Column,
    ) -> DataFrame:
        candidates = lane_rules.join(vehicle_rows, on=join_condition, how="inner")
        return (
            candidates.filter(
                F.col("rule_market_code").isNull()
                | (F.col("rule_market_code") == F.col("vehicle_market_code"))
            )
            .filter(
                F.size(
                    F.array_except(
                        F.col("required_options"), F.col("installed_options")
                    )
                )
                == 0
            )
            .select(
                "vin",
                "part_number",
                F.col("vehicle_market_code").alias("market_code"),
                "fitment_position",
                F.lit(True).alias("is_applicable"),
                "matched_rule_id",
                "matched_rule_version",
            )
        )

    exact_rules = rule_rows.filter(
        F.col("rule_bm_type").isNotNull() & F.col("rule_br_type").isNotNull()
    )
    bm_rules = rule_rows.filter(
        F.col("rule_bm_type").isNotNull() & F.col("rule_br_type").isNull()
    )
    br_rules = rule_rows.filter(
        F.col("rule_bm_type").isNull() & F.col("rule_br_type").isNotNull()
    )
    model_rules = rule_rows.filter(
        F.col("rule_bm_type").isNull() & F.col("rule_br_type").isNull()
    )

    exact_matches = match_lane(
        exact_rules,
        (F.col("rule_model_type") == F.col("vehicle_model_type"))
        & (F.col("rule_bm_type") == F.col("vehicle_bm_type"))
        & (F.col("rule_br_type") == F.col("vehicle_br_type")),
    )
    bm_matches = match_lane(
        bm_rules,
        (F.col("rule_model_type") == F.col("vehicle_model_type"))
        & (F.col("rule_bm_type") == F.col("vehicle_bm_type")),
    )
    br_matches = match_lane(
        br_rules,
        (F.col("rule_model_type") == F.col("vehicle_model_type"))
        & (F.col("rule_br_type") == F.col("vehicle_br_type")),
    )
    model_matches = match_lane(
        model_rules,
        F.col("rule_model_type") == F.col("vehicle_model_type"),
    )

    return (
        exact_matches.unionByName(bm_matches)
        .unionByName(br_matches)
        .unionByName(model_matches)
        .dropDuplicates(
            [
                "vin",
                "part_number",
                "market_code",
                "fitment_position",
                "matched_rule_id",
                "matched_rule_version",
            ]
        )
    )