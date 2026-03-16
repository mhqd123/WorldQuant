# Alpha workflow rebuild

## What changed

- `discover_and_submit.py` now records richer simulation metadata:
  - expr
  - expr_hash
  - family tag
  - alpha_id
  - sharpe / fitness / turnover / drawdown
  - qualified flag
  - stage
- Added append-only ledger: `alpha_ledger.jsonl`
- Added summary rollup: `alpha_summary.json`
- Added submission reconciler: `monitor/reconcile_submissions.py`
- `monitor/run_alpha_batch.py` now tracks family counts and qualified totals

## Why

The old loop optimized for simulation score only. The rebuilt flow separates:
1. simulated
2. attempted submit
3. final platform state

This makes it possible to measure actual submission conversion instead of raw Sharpe alone.
