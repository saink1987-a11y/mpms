from datetime import date
from time import perf_counter

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .applicability import resolve_applicability


def naive_cross_join_baseline(
    parts: DataFrame,
    vehicles: DataFrame,
    rules: DataFrame,
    as_of_date: date,
    max_candidate_pairs: int = 2_000_000,
    candidate_pair_count: int | None = None,
) -> DataFrame:
    """Bounded demo baseline; never allow an uncontrolled fleet x catalog join."""
    active_parts = parts.filter(F.col("is_active")).select("part_number").distinct()
    if candidate_pair_count is None:
        candidate_pair_count = vehicles.count() * active_parts.count()
    candidate_pairs = candidate_pair_count
    if candidate_pairs > max_candidate_pairs:
        raise ValueError(
            f"Baseline would create {candidate_pairs:,} candidate pairs; "
            f"limit is {max_candidate_pairs:,}. Benchmark only a sampled dataset."
        )

    # Force the reference path to process every vehicle/part pair. Otherwise,
    # Catalyst can reorder the joins and remove the very expansion being timed.
    vehicles.alias("v").crossJoin(active_parts.alias("p")).select(
        F.pmod(
            F.xxhash64(F.col("v.vin"), F.col("p.part_number")), F.lit(97)
        ).alias("pair_checksum")
    ).agg(F.sum("pair_checksum")).collect()

    active_rules = rules.filter(
        F.col("is_active")
        & (F.col("valid_from") <= F.lit(as_of_date))
        & (F.col("valid_to").isNull() | (F.col("valid_to") >= F.lit(as_of_date)))
    ).alias("r")
    v = vehicles.alias("v")
    p = active_parts.alias("p")
    candidates = (
        v.crossJoin(p)
        .join(active_rules, F.col("p.part_number") == F.col("r.part_number"), "inner")
        .filter(F.col("v.model_type") == F.col("r.model_type"))
        .filter(F.col("r.bm_type").isNull() | (F.col("v.bm_type") == F.col("r.bm_type")))
        .filter(F.col("r.br_type").isNull() | (F.col("v.br_type") == F.col("r.br_type")))
        .filter(
            F.col("r.market_code").isNull()
            | (F.col("v.market_code") == F.col("r.market_code"))
        )
        .filter(
            F.size(
                F.array_except(
                    F.coalesce(F.col("r.required_options"), F.array().cast("array<string>")),
                    F.coalesce(F.col("v.installed_options"), F.array().cast("array<string>")),
                )
            )
            == 0
        )
    )
    return candidates.select(
        F.col("v.vin").alias("vin"),
        F.col("p.part_number").alias("part_number"),
        F.col("v.market_code").alias("market_code"),
        F.col("r.fitment_position").alias("fitment_position"),
        F.col("r.rule_id").alias("matched_rule_id"),
        F.col("r.rule_version").alias("matched_rule_version"),
    ).dropDuplicates()


def run_benchmark(
    parts: DataFrame,
    vehicles: DataFrame,
    rules: DataFrame,
    as_of_date: date,
    max_candidate_pairs: int = 2_000_000,
) -> dict[str, float | int]:
    """Materialize both approaches, compare exact match sets, and return timings."""
    vehicle_count = vehicles.count()
    part_count = parts.filter(F.col("is_active")).select("part_number").distinct().count()
    candidate_pair_count = vehicle_count * part_count

    started = perf_counter()
    optimized = resolve_applicability(parts, vehicles, rules, as_of_date)
    optimized_keys = optimized.select(
        "vin", "part_number", "market_code", "fitment_position",
        "matched_rule_id", "matched_rule_version",
    ).dropDuplicates()
    optimized_count = optimized_keys.count()
    optimized_seconds = perf_counter() - started
    unique_vehicle_part_count = optimized_keys.select(
        "vin", "part_number", "market_code"
    ).dropDuplicates().count()

    started = perf_counter()
    baseline = naive_cross_join_baseline(
        parts,
        vehicles,
        rules,
        as_of_date,
        max_candidate_pairs,
        candidate_pair_count=candidate_pair_count,
    )
    baseline_count = baseline.count()
    baseline_seconds = perf_counter() - started
    match_keys = [
        "vin", "part_number", "market_code", "fitment_position",
        "matched_rule_id", "matched_rule_version",
    ]
    baseline_only = baseline.join(optimized_keys, match_keys, "left_anti").count()
    optimized_only = optimized_keys.join(baseline, match_keys, "left_anti").count()

    return {
        "vehicle_count": vehicle_count,
        "active_part_count": part_count,
        "naive_candidate_pairs": vehicle_count * part_count,
        "candidate_reduction_pct": 100.0
        * (1 - unique_vehicle_part_count / candidate_pair_count)
        if vehicle_count * part_count > 0
        else 0.0,
        "unique_vehicle_part_match_count": unique_vehicle_part_count,
        "optimized_match_count": optimized_count,
        "baseline_match_count": baseline_count,
        "optimized_seconds": optimized_seconds,
        "baseline_seconds": baseline_seconds,
        "baseline_over_optimized_ratio": baseline_seconds / optimized_seconds
        if optimized_seconds > 0
        else 0.0,
        "baseline_only_mismatch_count": baseline_only,
        "optimized_only_mismatch_count": optimized_only,
    }