# Deep Spike Benchmark Summary

This summary is generated from measured deep-run artifacts, not manual judgment.

## Scope

- Deep path only: standalone spike runs in `runs/spikes/deep/...`

## Detection Quality

- Deep anomaly recall: 20.00%
- Deep precision proxy: 100.00%
- Deep evidence sufficiency: 100.00%
- Deep root-cause depth: 100.00%
- Deep run count: 1

## Consistency

- Recall stddev: 0.000000
- Precision stddev: 0.000000

## Trace Observability

- Deep avg steps: 8.0
- Deep avg tool calls: 8.0
- Deep avg tool errors: 1.0

## Deep Failure Cases

- A-001: COGS spike in CC-300 (2024-03/2024-04)
- A-002: Sales revenue zeroed in 2024-06
- A-003: EUR FX rate anomaly in 2024-07
- A-004: Finance budget misalignment in Q3 2024

## Recommendation Inputs

- Tune prompt/tool limits if anomaly recall is low or if tool error rate is high.
- Track recall and precision proxy across repeated runs before changing defaults.
- Investigate unmatched anomalies in `evaluation.json` and `trace.json`.
