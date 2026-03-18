#!/usr/bin/env python3
"""
智能 Alpha 表达式生成器。

策略层次：
  1. 基本面（低拥挤度）
  2. 基本面 × 价格交叉
  3. 条件交易 trade_when
  4. 行业相对 group_rank
  5. 长窗口信号
  6. 多维度组合

每个策略提供多组参数排列，自动去重，保证与已提交表达式不重复。
"""
import hashlib
import itertools
import random
from typing import List, Set


# ── 信号骨干库 ─────────────────────────────────────────────
# 每个骨干函数返回 (expression, family_tag, settings_override) 的列表

def _fundamental_signals() -> list:
    """纯基本面信号 — 被挖掘最少，自相关最低"""
    templates = []

    value_ratios = [
        ("rank(sales / cap)", "fundamental_value", {}),
        ("rank(operating_income / cap)", "fundamental_value", {}),
        ("rank(cashflow / cap)", "fundamental_value", {}),
        ("rank(book_value / cap)", "fundamental_value", {}),
        ("-rank(cap / sales)", "fundamental_value", {}),
    ]

    quality_ratios = [
        ("rank(cashflow / net_income)", "fundamental_quality", {}),
        ("rank(operating_income / sales)", "fundamental_quality", {}),
        ("rank(cashflow / assets)", "fundamental_quality", {}),
        ("-rank(debt / assets)", "fundamental_quality", {}),
        ("rank((sales - debt) / assets)", "fundamental_quality", {}),
    ]

    efficiency_changes = [
        ("rank(ts_delta(sales / assets, 60))", "fundamental_efficiency", {"decay": 8}),
        ("rank(ts_delta(operating_income / sales, 60))", "fundamental_efficiency", {"decay": 8}),
        ("rank(ts_delta(cashflow / assets, 40))", "fundamental_efficiency", {"decay": 8}),
        ("-rank(ts_delta(debt / assets, 60))", "fundamental_leverage", {"decay": 8}),
        ("rank(ts_delta(sales, 60) / sales)", "fundamental_growth", {"decay": 8}),
    ]

    templates.extend(value_ratios)
    templates.extend(quality_ratios)
    templates.extend(efficiency_changes)
    return templates


def _fundamental_price_cross() -> list:
    """基本面 × 价格交叉信号 — 两个不相关维度组合"""
    templates = []

    value_signals = [
        "rank(sales / cap)",
        "rank(operating_income / cap)",
        "rank(cashflow / cap)",
        "rank(book_value / cap)",
    ]

    momentum_signals = [
        "rank(-ts_delta(close, 5))",
        "rank(-ts_delta(close, 5) / (1 + ts_std_dev(returns, 20)))",
        "rank(-(close / ts_mean(close, 10) - 1))",
        "rank(-ts_mean(returns, 3))",
    ]

    for val, mom in itertools.product(value_signals, momentum_signals):
        expr = f"{val} * {mom}"
        templates.append((expr, "fundamental_x_momentum", {"decay": 6}))

    quality_signals = [
        "rank(cashflow / net_income)",
        "rank(operating_income / sales)",
        "-rank(debt / assets)",
    ]

    vol_signals = [
        "rank(-ts_std_dev(returns, 20))",
        "rank(-ts_std_dev(returns, 60))",
        "rank(1 / (1 + ts_std_dev(returns, 10)))",
    ]

    for qual, vol in itertools.product(quality_signals, vol_signals):
        expr = f"{qual} * {vol}"
        templates.append((expr, "quality_x_lowvol", {"decay": 8}))

    return templates


def _trade_when_signals() -> list:
    """条件交易信号 — 用 trade_when 大幅降低自相关"""
    templates = []

    base_signals = [
        "rank(-ts_delta(close, 5) / (1 + ts_std_dev(returns, 20)))",
        "rank(sales / cap) * rank(-ts_delta(close, 5))",
        "rank(-(close / ts_mean(close, 10) - 1))",
        "rank(cashflow / cap) * rank(-ts_mean(returns, 3))",
        "rank(operating_income / cap) * rank(-ts_std_dev(returns, 20))",
    ]

    conditions = [
        "volume > ts_mean(volume, 20)",
        "volume > 1.5 * ts_mean(volume, 20)",
        "ts_std_dev(returns, 5) > ts_std_dev(returns, 20)",
        "abs(close - open) > ts_mean(abs(close - open), 10)",
    ]

    for sig, cond in itertools.product(base_signals, conditions):
        expr = f"trade_when({sig}, {cond})"
        templates.append((expr, "conditional_trade", {"decay": 5}))

    return templates


def _group_rank_signals() -> list:
    """行业内相对排名信号 — 天然低自相关"""
    templates = []

    groupings = ["subindustry", "industry", "sector"]

    inner_signals = [
        ("cashflow / cap", "group_fundamental_value"),
        ("operating_income / cap", "group_fundamental_value"),
        ("sales / cap", "group_fundamental_value"),
        ("-ts_delta(close, 5)", "group_reversal"),
        ("-(close / ts_mean(close, 10) - 1)", "group_mean_reversion"),
        ("cashflow / net_income", "group_quality"),
        ("-debt / assets", "group_leverage"),
        ("ts_delta(sales / assets, 40)", "group_efficiency"),
    ]

    for group in groupings:
        for inner, family in inner_signals:
            expr = f"group_rank({group}, {inner})"
            templates.append((expr, f"{family}_{group}", {"decay": 6, "neutralization": group.upper()}))

    momentum_modifiers = [
        "rank(-ts_delta(close, 5))",
        "rank(-ts_std_dev(returns, 20))",
    ]
    for group in ["subindustry", "industry"]:
        for inner, family in inner_signals[:4]:
            for mod in momentum_modifiers:
                expr = f"group_rank({group}, {inner}) * {mod}"
                templates.append((expr, f"{family}_{group}_cross", {"decay": 6}))

    return templates


def _long_window_signals() -> list:
    """长窗口信号 — 中低频，竞争较少"""
    templates = []

    long_fund = [
        ("rank(ts_mean(operating_income, 120) / ts_mean(operating_income, 240))", "long_earnings_trend", {"decay": 10}),
        ("rank(ts_mean(sales, 60) / ts_mean(sales, 120))", "long_revenue_trend", {"decay": 10}),
        ("rank(ts_mean(cashflow, 60) / ts_mean(cashflow, 120))", "long_cashflow_trend", {"decay": 10}),
        ("-rank(ts_mean(debt, 60) / ts_mean(debt, 120))", "long_deleveraging", {"decay": 10}),
    ]

    long_price = [
        ("rank(close / ts_mean(close, 120) - 1)", "long_momentum", {"decay": 8}),
        ("rank(-(close / ts_mean(close, 60) - 1))", "long_reversion", {"decay": 8}),
        ("rank(-ts_std_dev(returns, 60))", "long_low_vol", {"decay": 8}),
    ]

    templates.extend(long_fund)
    templates.extend(long_price)

    for fund_expr, fund_fam, fund_s in long_fund:
        for price_expr, price_fam, price_s in long_price:
            expr = f"{fund_expr} * {price_expr}"
            merged_s = {**fund_s, **price_s, "decay": max(fund_s.get("decay", 6), price_s.get("decay", 6))}
            templates.append((expr, f"{fund_fam}_x_{price_fam}", merged_s))

    return templates


def _multi_dimension_signals() -> list:
    """三维度组合信号 — 价值 × 质量 × 动量"""
    templates = []

    value_dim = [
        "rank(sales / cap)",
        "rank(operating_income / cap)",
        "rank(book_value / cap)",
    ]

    quality_dim = [
        "rank(cashflow / net_income)",
        "rank(operating_income / sales)",
        "rank(-debt / assets)",
    ]

    momentum_dim = [
        "rank(-ts_delta(close, 5))",
        "rank(-ts_std_dev(returns, 20))",
        "rank(-(close / ts_mean(close, 10) - 1))",
    ]

    for v, q, m in itertools.product(value_dim, quality_dim, momentum_dim):
        expr = f"{v} * {q} * {m}"
        templates.append((expr, "triple_factor", {"decay": 6}))

    return templates


# ── 相似度检测 ──────────────────────────────────────────────

def _normalize_expr(expr: str) -> str:
    return " ".join(expr.strip().split()).lower()


def _expr_hash(expr: str) -> str:
    return hashlib.sha256(_normalize_expr(expr).encode()).hexdigest()[:16]


def _structural_signature(expr: str) -> str:
    """提取表达式的结构签名（去掉具体数字），用于相似度检测"""
    import re
    sig = _normalize_expr(expr)
    sig = re.sub(r'\d+\.?\d*', 'N', sig)
    sig = re.sub(r'\s+', '', sig)
    return sig


def deduplicate(candidates: list, submitted_exprs: Set[str]) -> list:
    """去重：排除已提交的和结构相同的"""
    submitted_normalized = {_normalize_expr(e) for e in submitted_exprs}
    seen_hashes = set()
    seen_signatures = set()
    result = []

    for expr, family, settings in candidates:
        norm = _normalize_expr(expr)
        if norm in submitted_normalized:
            continue
        h = _expr_hash(expr)
        if h in seen_hashes:
            continue
        sig = _structural_signature(expr)
        if sig in seen_signatures:
            continue
        seen_hashes.add(h)
        seen_signatures.add(sig)
        result.append((expr, family, settings))

    return result


# ── 策略编排 ──────────────────────────────────────────────

ALL_STRATEGIES = {
    "fundamental": _fundamental_signals,
    "fundamental_x_price": _fundamental_price_cross,
    "trade_when": _trade_when_signals,
    "group_rank": _group_rank_signals,
    "long_window": _long_window_signals,
    "multi_dimension": _multi_dimension_signals,
}

STRATEGY_PRIORITY = [
    ("fundamental", 0.25),
    ("fundamental_x_price", 0.20),
    ("trade_when", 0.15),
    ("group_rank", 0.15),
    ("long_window", 0.10),
    ("multi_dimension", 0.15),
]


def generate_batch(
    batch_size: int = 10,
    submitted_exprs: Set[str] = None,
    strategy_weights: dict = None,
    exclude_families: Set[str] = None,
) -> List[tuple]:
    """
    生成一批 Alpha 表达式。
    返回 [(expression, family_tag, settings_override), ...]
    """
    submitted_exprs = submitted_exprs or set()
    exclude_families = exclude_families or set()

    all_candidates = []
    for name, gen_fn in ALL_STRATEGIES.items():
        candidates = gen_fn()
        if exclude_families:
            candidates = [(e, f, s) for e, f, s in candidates if f not in exclude_families]
        all_candidates.extend(candidates)

    unique = deduplicate(all_candidates, submitted_exprs)

    if not unique:
        return []

    weights = dict(STRATEGY_PRIORITY)
    if strategy_weights:
        weights.update(strategy_weights)

    family_to_strategy = {}
    for name, gen_fn in ALL_STRATEGIES.items():
        for _, fam, _ in gen_fn():
            family_to_strategy[fam] = name

    weighted = []
    for item in unique:
        strat = family_to_strategy.get(item[1], "fundamental")
        w = weights.get(strat, 0.1)
        weighted.append((w, item))

    weighted.sort(key=lambda x: x[0], reverse=True)

    # 从高权重策略中优先选取，同时保证多样性
    selected = []
    families_used = set()
    for w, (expr, fam, settings) in weighted:
        base_fam = fam.split("_x_")[0] if "_x_" in fam else fam
        if base_fam in families_used and len(selected) < batch_size * 0.7:
            continue
        selected.append((expr, fam, settings))
        families_used.add(base_fam)
        if len(selected) >= batch_size:
            break

    if len(selected) < batch_size:
        remaining = [(e, f, s) for _, (e, f, s) in weighted if (e, f, s) not in selected]
        random.shuffle(remaining)
        selected.extend(remaining[:batch_size - len(selected)])

    return selected[:batch_size]


def get_strategy_stats() -> dict:
    """返回每个策略可生成的表达式数量"""
    stats = {}
    for name, gen_fn in ALL_STRATEGIES.items():
        candidates = gen_fn()
        stats[name] = len(candidates)
    stats["total"] = sum(stats.values())
    return stats


if __name__ == "__main__":
    stats = get_strategy_stats()
    print("策略库统计:")
    for name, count in stats.items():
        print(f"  {name}: {count}")
    print(f"\n示例 batch (10 个):")
    batch = generate_batch(10)
    for expr, fam, settings in batch:
        print(f"  [{fam}] {expr[:80]}...")
