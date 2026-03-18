# WorldQuant BRAIN 专家速查

## 1. 可用数据字段

### 价量数据（高度拥挤，谨慎使用）
```
close  open  high  low  volume  vwap  returns
```

### 基本面数据（低拥挤度，优先使用）
```
sales  operating_income  net_income  cashflow
assets  debt  book_value  cap  sharesout
```

## 2. 常用函数
```
时间序列: ts_mean(x,n)  ts_sum(x,n)  ts_std_dev(x,n)  ts_corr(x,y,n)
         ts_rank(x,n)  ts_delta(x,n)  ts_delay(x,n)
截面:     rank(x)  scale(x)  signed_power(x,p)
分组:     group_rank(group, x)  group_mean(group, x)  group_neutralize(group, x)
条件:     trade_when(signal, condition)
```

## 3. 高通过率策略（按优先级排序）

### 策略一：纯基本面（预计通过率 3-8%）
| 信号类型 | 表达式 |
|---------|--------|
| 价值 | `rank(sales / cap)` |
| 价值 | `rank(operating_income / cap)` |
| 价值 | `rank(cashflow / cap)` |
| 质量 | `rank(cashflow / net_income)` |
| 质量 | `rank(operating_income / sales)` |
| 杠杆 | `-rank(debt / assets)` |
| 效率变化 | `rank(ts_delta(sales / assets, 60))` |
| 盈利改善 | `rank(ts_delta(operating_income / sales, 60))` |

### 策略二：基本面 × 价格交叉（预计通过率 2-5%）
| 信号类型 | 表达式 |
|---------|--------|
| 价值+反转 | `rank(sales / cap) * rank(-ts_delta(close, 5))` |
| 价值+低波 | `rank(operating_income / cap) * rank(-ts_std_dev(returns, 20))` |
| 质量+反转 | `rank(cashflow / net_income) * rank(-ts_delta(close, 5) / (1 + ts_std_dev(returns, 20)))` |

### 策略三：条件交易 trade_when（预计通过率 5-10%）
```
trade_when(rank(-ts_delta(close, 5) / (1 + ts_std_dev(returns, 20))), volume > ts_mean(volume, 20))
trade_when(rank(sales / cap) * rank(-ts_delta(close, 5)), ts_std_dev(returns, 5) > ts_std_dev(returns, 20))
```

### 策略四：行业内相对排名 group_rank（预计通过率 2-4%）
```
group_rank(subindustry, cashflow / cap)
group_rank(subindustry, operating_income / cap) * rank(-ts_delta(close, 5))
group_rank(industry, -debt / assets) * rank(-ts_std_dev(returns, 20))
```

### 策略五：长窗口信号（预计通过率 1-3%）
```
rank(ts_mean(operating_income, 120) / ts_mean(operating_income, 240))
rank(ts_mean(sales, 60) / ts_mean(sales, 120)) * rank(-(close / ts_mean(close, 60) - 1))
```

### 策略六：三维度组合 价值×质量×动量
```
rank(sales / cap) * rank(cashflow / net_income) * rank(-ts_delta(close, 5))
rank(operating_income / cap) * rank(-debt / assets) * rank(-ts_std_dev(returns, 20))
```

## 4. 质量标准

| 指标 | 合格 | 高质量 | 淘汰 |
|------|------|--------|------|
| Sharpe | > 1.25 | > 2.0 | < 1.0 |
| Fitness | > 1.0 | > 1.5 | < 0.8 |
| Turnover | < 70% | < 40% | > 80% |
| Drawdown | < 10% | < 5% | > 15% |
| Self-Correlation | < 0.7 | < 0.5 | > 0.7 |

## 5. 执行命令

### 自动闭环（推荐，7x24）
```bash
python ~/.openclaw/skills/worldquant-brain/scripts/alpha_loop.py \
  --workspace ~/.openclaw/workspace \
  --batch-size 8 \
  --interval 120
```

### 单次提交
```bash
python ~/.openclaw/skills/worldquant-brain/scripts/discover_and_submit.py \
  --file ~/.openclaw/workspace/alphas.txt
```

## 6. 凭证
- 路径：`~/.brain_credentials` 或 `~/.openclaw/.brain_credentials`
- 格式：`["email","password"]`（JSON 数组）

## 7. 核心原则

1. **数据多样化**：优先使用基本面数据（sales, cashflow, debt 等），避免纯价量信号
2. **降低自相关**：用 trade_when、group_rank、基本面×价格交叉来创造独特信号指纹
3. **窗口多样化**：混合短期（3-10天）和长期（60-120天）窗口
4. **质量优先**：宁可少量高质量，不要大量低质量
5. **自动去重**：系统自动跳过已提交和结构相似的表达式
6. **Family 学习**：系统自动跟踪每个 family 的通过率，淘汰表现差的方向
