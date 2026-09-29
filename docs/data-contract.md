# Data Contract

The following is a proposed contract for the synthetic implementation. Production source columns and business definitions must be mapped and approved with data owners.

## Sources

| Source | Grain / key | Required columns |
|---|---|---|
| Parts | One row per part number | `part_number`, `is_active` |
| Fitment rules | One row per versioned rule and fitment position | `rule_id`, `rule_version`, `part_number`, `model_type`, `bm_type`, `br_type`, `market_code`, `fitment_position`, `required_options`, `valid_from`, `valid_to`, `is_active` |
| VeDoc vehicles | One row per vehicle snapshot | `vin`, `market_code`, `model_type`, `bm_type`, `br_type`, `installed_options` |
| VPM fleet | One row per VIN/market/effective interval | `vin`, `market_code`, `vehicle_age_years`, `valid_from`, `valid_to`, `is_active` |
| Replacement rates | One row per part/market/age interval/version | `part_number`, `market_code`, `age_min_years`, `age_max_years`, `annual_replacement_rate`, `is_active` |
| Warranty claims | One row per claim line | `claim_id`, `vin`, `market_code`, `part_number`, `claim_date`, `claim_status`, `replacement_qty` |
| Commercial sales | One row per sold part line | `sale_id`, `market_code`, `part_number`, `sale_date`, `sold_qty` |
| Forecast history | One row per part-market-month | `series_id`, `ds` (month-end), `y_demand` |

## Outputs

- `silver_parts_applicability`: one unique record per VIN, part, market, fitment position, matched rule ID, and rule version.
- `gold_market_potential_share`: one record per market, part, and reporting year, with eligible unique VIN count, annual expected replacement quantity, warranty claim quantity, commercial sales quantity, and sales-based captured share.
- `gold_part_market_forecast`: output written by MMF for the configured part-market series and 12-month horizon; exact MMF output fields follow the installed accelerator version.

## Synthetic semantics

- A null BM or BR on a rule is a wildcard at that level; null on the rule market means all markets.
- All codes in `required_options` must be present on the vehicle.
- Rule effective dates are inclusive; null `valid_to` is open-ended.
- Applicability can return multiple fitment positions or matching rules. Potential deduplicates to unique VIN/part/market before applying rates.
- Replacement-rate rows must have non-overlapping age intervals for each part/market. Production ingestion should quarantine overlaps and uncovered ages rather than treating them as zero silently.
- Claims and commercial sales are separate measures. Claims must not be used as the market-share numerator unless the business explicitly defines that measure.
- The synthetic share formula is sales quantity divided by annual expected replacement potential. Confirm the enterprise definition before reusing it.