# Alpha Candidates Notes

Assumption: these candidates are primarily aimed at Delay 1, with controlled turnover and lower drawdown as first-class goals.

1. `trade_when(volume > ts_mean(volume, 20), rank(-(close / vwap - 1)), -1)`
   - Thesis: trade mean reversion only when liquidity is elevated.
   - Why it may help: event gating can reduce unnecessary churn versus trading every day.

2. `rank(-(close / ts_mean(close, 15) - 1) / (1 + ts_std_dev(returns, 20)))`
   - Thesis: short-term reversion scaled by recent volatility.
   - Why it may help: volatility scaling can reduce drawdown spikes.

3. `rank(ts_delta(close, 20) / (1 + ts_std_dev(returns, 20)))`
   - Thesis: medium-horizon momentum penalized by realized volatility.
   - Why it may help: avoids pure noisy momentum chasing.

4. `rank(-(ts_delta(close, 5)) * (volume / ts_mean(volume, 20)))`
   - Thesis: reversal after short-term move, strengthened by abnormal volume.
   - Why it may help: focuses on crowded short-term dislocations.

5. `rank((vwap / ts_mean(vwap, 10) - 1) - (close / ts_mean(close, 10) - 1))`
   - Thesis: exploit divergence between transaction-weighted price behavior and closing-price behavior.
   - Why it may help: captures intraday pressure not fully reflected in smoothed close.

6. `rank((cashflow_op / cap) / (1 + ts_std_dev(returns, 60)))`
   - Thesis: favor firms with stronger operating yield, but penalize high realized volatility.
   - Why it may help: slower-moving fundamental signal may help lower turnover.

These are research candidates, not proven submission-ready alphas. Real qualification still requires platform backtests against turnover, drawdown, Sharpe, and uniqueness constraints.
