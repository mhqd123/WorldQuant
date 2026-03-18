#!/usr/bin/env python3
"""
表达式结构指纹 — 在仿真前预估自相关风险。

原理：提取表达式的结构特征（使用的数据字段、操作符、时间窗口），
与已提交的 4000+ 表达式做相似度比较。相似度 > 阈值的直接跳过不仿真。
"""
import re
import json
from pathlib import Path


def extract_fingerprint(expr: str) -> dict:
    """提取表达式的结构指纹"""
    e = expr.lower().strip()

    data_fields = set()
    for field in ["close", "open", "high", "low", "volume", "vwap", "returns", "cap",
                   "sales", "cashflow", "cashflow_op", "operating_income", "net_income",
                   "debt", "assets", "book_value", "sharesout", "adv20", "adv60",
                   "short_interest", "analyst_rating", "dividend_yield"]:
        if field in e:
            data_fields.add(field)

    operators = set()
    for op in ["ts_mean", "ts_sum", "ts_std_dev", "ts_corr", "ts_rank", "ts_delta",
               "ts_delay", "rank", "scale", "signed_power", "group_rank", "group_mean",
               "trade_when", "abs", "log", "sign"]:
        if op in e:
            operators.add(op)

    windows = set()
    for m in re.finditer(r'[\(,]\s*(\d+)\s*\)', e):
        try:
            w = int(m.group(1))
            if 1 <= w <= 500:
                windows.add(w)
        except ValueError:
            pass

    window_class = set()
    for w in windows:
        if w <= 5: window_class.add("ultra_short")
        elif w <= 15: window_class.add("short")
        elif w <= 40: window_class.add("medium")
        elif w <= 120: window_class.add("long")
        else: window_class.add("very_long")

    has_conditional = "trade_when" in e
    has_grouping = "group_rank" in e or "group_mean" in e
    has_fundamental = bool(data_fields & {"sales", "cashflow", "cashflow_op", "operating_income",
                                           "net_income", "debt", "assets", "book_value", "cap", "sharesout"})
    has_price_only = data_fields <= {"close", "open", "high", "low", "vwap", "returns", "volume"}

    return {
        "data_fields": sorted(data_fields),
        "operators": sorted(operators),
        "windows": sorted(windows),
        "window_class": sorted(window_class),
        "has_conditional": has_conditional,
        "has_grouping": has_grouping,
        "has_fundamental": has_fundamental,
        "has_price_only": has_price_only,
        "complexity": len(operators) + len(data_fields),
    }


def similarity(fp1: dict, fp2: dict) -> float:
    """计算两个指纹的相似度 [0, 1]"""
    f1 = set(fp1["data_fields"])
    f2 = set(fp2["data_fields"])
    field_sim = len(f1 & f2) / max(len(f1 | f2), 1)

    o1 = set(fp1["operators"])
    o2 = set(fp2["operators"])
    op_sim = len(o1 & o2) / max(len(o1 | o2), 1)

    w1 = set(fp1["window_class"])
    w2 = set(fp2["window_class"])
    win_sim = len(w1 & w2) / max(len(w1 | w2), 1)

    cond_same = 1.0 if fp1["has_conditional"] == fp2["has_conditional"] else 0.0
    group_same = 1.0 if fp1["has_grouping"] == fp2["has_grouping"] else 0.0
    fund_same = 1.0 if fp1["has_fundamental"] == fp2["has_fundamental"] else 0.0

    return (field_sim * 0.35 + op_sim * 0.25 + win_sim * 0.15 +
            cond_same * 0.05 + group_same * 0.05 + fund_same * 0.15)


class FingerprintDB:
    """已提交表达式的指纹数据库"""

    def __init__(self, workspace: Path):
        self.ws = workspace
        self.fingerprints = []
        self._load()

    def _load(self):
        sub_file = self.ws / "alphas_submitted.json"
        if not sub_file.exists():
            return
        try:
            data = json.loads(sub_file.read_text(encoding="utf-8"))
            for r in data.get("submitted", []):
                expr = r.get("expr", "")
                if expr:
                    self.fingerprints.append(extract_fingerprint(expr))
        except Exception:
            pass

    def max_similarity(self, expr: str) -> float:
        """计算表达式与已有库的最大相似度"""
        if not self.fingerprints:
            return 0.0
        fp = extract_fingerprint(expr)
        return max(similarity(fp, existing) for existing in self.fingerprints)

    def should_skip(self, expr: str, threshold: float = 0.75) -> bool:
        """是否应该跳过（与已有表达式过于相似）"""
        return self.max_similarity(expr) >= threshold

    def novelty_score(self, expr: str) -> float:
        """新颖度评分 [0, 1]，越高越新颖"""
        return 1.0 - self.max_similarity(expr)

    def add(self, expr: str):
        self.fingerprints.append(extract_fingerprint(expr))


if __name__ == "__main__":
    import sys
    test_exprs = [
        "rank(-ts_delta(close, 5) / (1 + ts_std_dev(returns, 20)))",
        "rank(sales / cap) * rank(cashflow / net_income)",
        "trade_when(rank(operating_income / cap), volume > ts_mean(volume, 20))",
        "group_rank(subindustry, ts_delta(sales / assets, 60))",
    ]
    for e in test_exprs:
        fp = extract_fingerprint(e)
        print(f"\n{e[:60]}...")
        print(f"  字段: {fp['data_fields']}")
        print(f"  算子: {fp['operators']}")
        print(f"  窗口: {fp['window_class']}")
        print(f"  基本面: {fp['has_fundamental']}  条件: {fp['has_conditional']}  分组: {fp['has_grouping']}")

    print("\n相似度矩阵:")
    fps = [extract_fingerprint(e) for e in test_exprs]
    for i, e1 in enumerate(test_exprs):
        for j, e2 in enumerate(test_exprs):
            if j > i:
                sim = similarity(fps[i], fps[j])
                print(f"  [{i}] vs [{j}]: {sim:.2f}")
