# Performance Validation

## Method

The workflow compares the rule-based Spark joins with a bounded legacy-style candidate expansion on the same synthetic sample. The baseline forces enumeration of every sampled vehicle/part pair before filtering, then both result sets are materialized and compared exactly. A two-million candidate-pair guard prevents accidental large cross joins. Times include Spark action execution, not full job startup and provisioning.

The optimization metric is candidate reduction: one candidate pair is one vehicle × active part. The matched VIN/part/market count is distinct across fitment positions and rules. Runtime is measured separately and depends on cluster/runtime, data distribution, cache state, and concurrent workload.

## Current observation

Databricks serverless run, synthetic input, 2026-09-29:

| Measure | Observed |
|---|---:|
| Vehicles | 5,000 |
| Active parts | 400 |
| Candidate vehicle/part pairs | 2,000,000 |
| Rule-based exact match rows | 194,173 |
| Unique vehicle/part matches | 194,173 |
| Candidate reduction | 90.29% |
| Result mismatches, either direction | 0 |
| Rule-based path elapsed | 1.146s |
| Forced cross-join baseline elapsed | 1.043s |
| Delta tables written | No |

This run demonstrates a substantial reduction in candidate combinations and exact result parity, but it does **not** demonstrate a wall-clock speedup. On this sample, the rule-based path was about 10% slower than the bounded baseline. Do not claim a runtime improvement from this result. Further tuning and representative benchmarks are required before making a latency or cost claim.

## Before presenting production performance

- Benchmark realistic, approved source snapshots at multiple fleet and catalog scales.
- Run repeated trials with comparable compute, warm-up, and concurrency; report medians and variability.
- Include end-to-end duration, input/output bytes, shuffle, spill, skew, task distribution, and cost.
- Verify output parity and business-rule correctness for each run.
- Inspect final query plans and Spark UI metrics; do not extrapolate from the synthetic sample.