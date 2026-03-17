# WorldQuant BRAIN 专家速查

## 1. 常用函数

```
ts_mean(x, n)  ts_sum(x, n)  ts_std(x, n)  ts_corr(x, y, n)
ts_rank(x, n)  ts_delta(x, n)  ts_delay(x, n)
rank(x)  scale(x)  signed_power(x, p)
close  volume  returns  open  high  low
decay  trade_when  truncation
```

## 2. 策略模板（可直接用）

| 策略 | 表达式 |
|------|--------|
| 动量 | `rank(ts_mean(returns, 5))` |
| 动量 | `ts_mean(returns, 5)` |
| 动量 | `rank(ts_sum(returns, 10))` |
| 均值回归 | `close / ts_mean(close, 20) - 1` |
| 量价 | `-ts_corr(close, volume, 20)` |
| 波动率 | `ts_std(returns, 10)` |
| 组合 | `rank(ts_mean(returns,5)) + rank(-ts_corr(close,volume,20))` |

## 3. 质量标准

| 指标 | 合格 | 不合格 |
|------|------|--------|
| Sharpe | > 2.5 | < 2.0 |
| Fitness | > 1.0 | < 0.8 |
| Turnover | < 40% | > 50% |
| IC Mean | > 0.02 | < 0.01 |

## 4. 执行命令

```bash
cd ~/.openclaw/skills/worldquant-brain
python scripts/discover_and_submit.py ~/.openclaw/workspace
# 或指定文件
python scripts/discover_and_submit.py --file ~/.openclaw/workspace/alphas.txt
```

## 5. 凭证

- 路径：`~/.brain_credentials` 或 `~/.openclaw/.brain_credentials`
- 格式：`["email","password"]`（JSON 数组）

## 6. 优化建议

- Sharpe 低：增加 lookback 或换收益来源
- Turnover 高：用 decay、rank、trade_when 控制
- 权重集中：truncation 调 0.05~0.1
- TOP1000 换手高：优先用 TOP3000

## 7. 表达式来源

| 来源 | 格式 |
|------|------|
| alphas.txt | 每行一个表达式，`#` 开头为注释 |
| expressions.txt | 同上 |
| alphas.json | `{"expressions": ["expr1", "expr2"]}` |

## 8. 默认仿真设置（autobrain-sim）

```
instrumentType: EQUITY, region: USA, universe: TOP3000
delay: 1, decay: 15, neutralization: SUBINDUSTRY
truncation: 0.08, maxTrade: ON, pasteurization: ON
testPeriod: P1Y6M
```
