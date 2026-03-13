# Alpha Standards

用于筛选和生成更接近 WorldQuant BRAIN 提交门槛的 alpha 候选。

## 1. 基本门槛

- Delay 0：Sharpe > 2.0，Health > 1.3
- Delay 1：Sharpe > 1.25，Health > 1.0

## 2. Turnover 约束

- 必须满足：1% < Turnover < 70%
- 过低：信号过钝
- 过高：噪音和不稳定性偏大

## 3. 集中度约束

- 单只股票最大权重 < 10%
- 不能依赖少数股票支撑表现

## 4. 稳健性约束

- 必须通过 Sub-universe test
- 在子股票池中也应保持可接受表现

## 5. 去重约束

- Self-correlation < 0.7
- 若相关性较高，新 alpha 的 Sharpe 需比已有高相关 alpha 至少高 10%

## 6. 生成约束

默认只生成少量、严选、可解释的候选：

- 每个候选必须体现不同 hypothesis
- 不接受只改窗口长度的伪多样性
- 不接受不可解释的操作符堆叠
- 优先方向：
  - 量价反转
  - 波动约束动量
  - 偏离均值 / VWAP 回归
  - 流动性 / 拥挤效应
  - 有解释力的基本面效率类

## 7. 淘汰规则

出现以下任一情形，默认视为低质量候选：

- 模板拼接
- 同一逻辑仅做参数微调
- 明显高换手躁动
- 明显低换手失活
- 难以解释的黑箱式结构
- 大概率卡在 weight / sub-universe / self-correlation

## 8. 交付规则

- 少给，给精
- 未经回测，只称为 submission-quality candidate
- 经测试后再标记为：接近合格 / 合格 / 该淘汰
