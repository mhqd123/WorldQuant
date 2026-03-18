#!/usr/bin/env python3
"""
Alpha 7x24 自动闭环 - 母因子迭代策略。

核心逻辑：
  1. 启动时扫描平台 alpha 池，找出母因子候选
  2. 对母因子做结构性变异（换骨干、改分组、加条件）
  3. 逐个仿真，跟踪自相关变化
  4. 自相关 < 0.7 且指标合格 → 提交
  5. 提交成功 → 冻结该 family，转向下一个母因子
  6. 模板冷启动：无母因子时用策略模板探索

用法:
  python alpha_loop.py [--workspace /path] [--batch-size 5] [--interval 90]
"""
import argparse
import json
import os
import sys
import time
import traceback
import hashlib
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from brain_api import BrainAPI, utc_now_iso, _safe_float
from alpha_strategies import generate_batch, get_strategy_stats

# ── 常量 ───────────────────────────────────────────────────

SHARPE_SUBMIT = 1.25
FITNESS_SUBMIT = 1.0
TURNOVER_MAX = 0.70
DRAWDOWN_MAX = 0.10
SELF_CORR_MAX = 0.70

SIM_INTERVAL = 10
CYCLE_INTERVAL = 90
MAX_CONSECUTIVE_ERRORS = 5


# ── 变异器 ────────────────────────────────────────────────

BACKBONE_SWAPS = [
    ("ts_delta(close, 2)", "(close - ts_mean(close, 6))"),
    ("ts_delta(close, 5)", "(close - ts_mean(close, 10))"),
    ("(close - open)", "ts_delta(close, 3)"),
    ("(2 * close - high - low)", "(close - ts_mean(close, 5))"),
    ("(close / vwap - 1)", "(close / ts_mean(close, 8) - 1)"),
    ("(close / ts_mean(close, 10) - 1)", "(close / ts_mean(close, 20) - 1)"),
    ("(close / ts_mean(close, 15) - 1)", "ts_delta(close, 10) / close"),
]

GROUPING_SWAPS = [
    ("subindustry", "industry"),
    ("industry", "sector"),
    ("sector", "subindustry"),
]

VOLUME_SWAPS = [
    ("volume / ts_mean(volume, 20)", "ts_mean(volume, 5) / ts_mean(volume, 40)"),
    ("volume / ts_mean(volume, 41)", "volume / ts_mean(volume, 20)"),
    ("ts_mean(volume, 5) / ts_mean(volume, 40)", "volume / ts_mean(volume, 60)"),
]

DECAY_OPTIONS = [3, 4, 5, 6, 8, 10, 12]
NEUTRALIZATION_OPTIONS = ["SUBINDUSTRY", "INDUSTRY", "SECTOR"]


def mutate_expression(expr: str, mutation_type: str) -> list:
    """对表达式做结构性变异，返回变异列表 [(expr, mutation_desc, settings_override)]"""
    results = []

    if mutation_type == "swap_backbone":
        for old, new in BACKBONE_SWAPS:
            if old in expr:
                mutated = expr.replace(old, new, 1)
                if mutated != expr:
                    results.append((mutated, f"backbone:{old}->{new}", {}))

    elif mutation_type == "swap_grouping":
        for old, new in GROUPING_SWAPS:
            if old in expr.lower():
                mutated = expr.replace(old, new).replace(old.capitalize(), new.capitalize())
                if mutated != expr:
                    results.append((mutated, f"grouping:{old}->{new}", {"neutralization": new.upper()}))

    elif mutation_type == "swap_volume":
        for old, new in VOLUME_SWAPS:
            if old in expr:
                mutated = expr.replace(old, new, 1)
                if mutated != expr:
                    results.append((mutated, f"volume:{old[:20]}->{new[:20]}", {}))

    elif mutation_type == "add_trade_when":
        conditions = [
            "volume > ts_mean(volume, 20)",
            "volume > 1.5 * ts_mean(volume, 20)",
            "ts_std_dev(returns, 5) > ts_std_dev(returns, 20)",
        ]
        for cond in conditions:
            if "trade_when" not in expr:
                mutated = f"trade_when({expr}, {cond})"
                results.append((mutated, f"trade_when:{cond[:30]}", {}))

    elif mutation_type == "change_decay":
        for d in DECAY_OPTIONS:
            results.append((expr, f"decay:{d}", {"decay": d}))

    elif mutation_type == "change_neutralization":
        for n in NEUTRALIZATION_OPTIONS:
            results.append((expr, f"neutralization:{n}", {"neutralization": n}))

    elif mutation_type == "add_group_rank":
        for group in ["subindustry", "industry"]:
            if "group_rank" not in expr:
                mutated = f"group_rank({group}, {expr})"
                results.append((mutated, f"group_rank:{group}", {"neutralization": group.upper()}))

    return results


MUTATION_PRIORITY = [
    "swap_backbone",
    "add_trade_when",
    "swap_grouping",
    "add_group_rank",
    "swap_volume",
    "change_decay",
    "change_neutralization",
]


def generate_mutations(expr: str, tried_mutations: set, max_count: int = 5) -> list:
    """为母因子生成所有可能的变异，去重后按优先级返回"""
    all_mutations = []
    for mt in MUTATION_PRIORITY:
        for mutated_expr, desc, settings in mutate_expression(expr, mt):
            key = hashlib.sha256(mutated_expr.strip().lower().encode()).hexdigest()[:16]
            if key not in tried_mutations:
                all_mutations.append((mutated_expr, desc, settings, mt))
                if len(all_mutations) >= max_count * 3:
                    break
    return all_mutations[:max_count]


# ── 数据管理 ──────────────────────────────────────────────

class Ledger:
    def __init__(self, workspace: Path):
        self.ws = workspace
        self.ws.mkdir(parents=True, exist_ok=True)

    def _path(self, name): return self.ws / name

    def _load_json(self, name, default=None):
        p = self._path(name)
        if not p.exists(): return default if default is not None else {}
        try: return json.loads(p.read_text(encoding="utf-8"))
        except Exception: return default if default is not None else {}

    def _save_json(self, name, data):
        self._path(name).write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    def _append_jsonl(self, name, record):
        with self._path(name).open("a", encoding="utf-8") as f:
            f.write(json.dumps({**record, "ts": utc_now_iso()}, ensure_ascii=False) + "\n")

    def load_submitted_exprs(self) -> set:
        data = self._load_json("alphas_submitted.json", {"submitted": []})
        return {r.get("expr", "").strip() for r in data.get("submitted", []) if r.get("expr")}

    def load_tried_hashes(self) -> set:
        data = self._load_json("alphas_submitted.json", {"submitted": []})
        hashes = set()
        for r in data.get("submitted", []):
            e = r.get("expr", "").strip().lower()
            if e:
                hashes.add(hashlib.sha256(e.encode()).hexdigest()[:16])
        return hashes

    def save_result(self, record: dict):
        data = self._load_json("alphas_submitted.json", {"submitted": []})
        data.setdefault("submitted", []).append(record)
        self._save_json("alphas_submitted.json", data)

    def save_qualified(self, record: dict):
        data = self._load_json("alphas_qualified.json", {"qualified": []})
        data.setdefault("qualified", []).append(record)
        self._save_json("alphas_qualified.json", data)

    def log(self, event: dict):
        self._append_jsonl("alpha_loop_log.jsonl", event)

    def save_stats(self, stats: dict):
        existing = self._load_json("alpha_loop_stats.json", {})
        existing.update(stats)
        existing["updatedAt"] = utc_now_iso()
        self._save_json("alpha_loop_stats.json", existing)

    def save_pool_analysis(self, analysis: dict):
        self._save_json("pool_analysis.json", {**analysis, "ts": utc_now_iso()})

    def load_pool_analysis(self) -> dict:
        return self._load_json("pool_analysis.json", {})

    def save_data_fields(self, fields: list):
        self._save_json("brain_data_fields.json", {"fields": fields, "ts": utc_now_iso()})


# ── 诊断 ──────────────────────────────────────────────────

def diagnose(m: dict) -> tuple:
    labels = []
    if _safe_float(m.get("sharpe")) < SHARPE_SUBMIT: labels.append("low_sharpe")
    if _safe_float(m.get("fitness")) < FITNESS_SUBMIT: labels.append("low_fitness")
    if _safe_float(m.get("turnover")) > TURNOVER_MAX: labels.append("high_turnover")
    if _safe_float(m.get("drawdown")) > DRAWDOWN_MAX: labels.append("high_drawdown")
    if _safe_float(m.get("self_correlation")) > SELF_CORR_MAX: labels.append("too_correlated")
    if not labels: return "submit", ["pass"]
    has_signal = _safe_float(m.get("sharpe")) >= 1.0 and _safe_float(m.get("fitness")) >= 0.8
    if has_signal and "too_correlated" in labels and len(labels) <= 2:
        return "mutate_decorrelate", labels
    if has_signal and "too_correlated" not in labels:
        return "mutate_improve", labels
    return "reject", labels


# ── 主循环阶段 ────────────────────────────────────────────

def phase_scan(api: BrainAPI, ledger: Ledger) -> dict:
    """阶段 0：扫描平台，分析 alpha 池"""
    print("\n[扫描] 分析平台 alpha 池...")
    try:
        analysis = api.scan_alpha_pool(limit=500)
        ledger.save_pool_analysis(analysis)
        print(f"  总计: {analysis['total']} 个 alpha")
        print(f"  状态分布: {analysis['by_status']}")
        print(f"  ACTIVE: {len(analysis.get('active_alphas', []))} 个")
        print(f"  高分被自相关挡住: {len(analysis.get('high_sharpe_blocked', []))} 个")
        print(f"  接近合格: {len(analysis.get('near_pass', []))} 个")
    except Exception as e:
        print(f"  扫描失败: {e}，使用缓存")
        analysis = ledger.load_pool_analysis()

    try:
        fields = api.get_data_fields()
        if fields:
            ledger.save_data_fields(fields)
            field_names = [f.get("name") or f for f in fields[:10]] if fields else []
            print(f"  数据字段示例: {field_names}")
    except Exception:
        pass

    return analysis


def phase_mother_iterate(api: BrainAPI, ledger: Ledger, mother: dict, max_mutations: int = 5) -> dict:
    """阶段 1：母因子迭代 — 对一个母因子做结构性变异"""
    expr = mother.get("expression", "")
    alpha_id = mother.get("alpha_id", "")
    reason = mother.get("reason", "")
    sharpe = mother.get("sharpe", 0)
    self_corr = mother.get("self_correlation", 0)

    print(f"\n[母因子迭代] {alpha_id}")
    print(f"  Sharpe: {sharpe:.2f}  自相关: {self_corr:.4f}  原因: {reason}")
    print(f"  表达式: {expr[:80]}...")

    stats = {"mother_id": alpha_id, "mutations_tried": 0, "improved": 0, "submitted": 0}
    tried = ledger.load_tried_hashes()
    mutations = generate_mutations(expr, tried, max_count=max_mutations)

    if not mutations:
        print("  无可用变异，跳过")
        return stats

    print(f"  生成 {len(mutations)} 个变异")

    for i, (mutated_expr, desc, settings, mt) in enumerate(mutations, 1):
        print(f"\n  [{i}/{len(mutations)}] {desc}")
        print(f"    {mutated_expr[:70]}...")

        try:
            alpha_data = api.simulate_and_wait(mutated_expr, settings=settings, timeout=300)
            metrics = BrainAPI.extract_metrics(alpha_data)
            stats["mutations_tried"] += 1

            record = {
                "expr": mutated_expr, "mother_id": alpha_id, "mutation": desc,
                "mutation_type": mt, "alpha_id": metrics["alpha_id"],
                "sharpe": metrics["sharpe"], "fitness": metrics["fitness"],
                "turnover": metrics["turnover"], "drawdown": metrics["drawdown"],
                "self_correlation": metrics["self_correlation"],
                "settings": settings,
            }

            decision, labels = diagnose(metrics)
            record["decision"] = decision
            record["labels"] = labels
            ledger.save_result(record)
            ledger.log({"event": "mutation_sim", **record})

            corr_delta = metrics["self_correlation"] - self_corr
            sharpe_delta = metrics["sharpe"] - sharpe

            if decision == "submit":
                stats["submitted"] += 1
                print(f"    [合格!] Sharpe:{metrics['sharpe']:.2f} Fitness:{metrics['fitness']:.2f} 自相关:{metrics['self_correlation']:.4f}")
                ledger.save_qualified(record)
                try:
                    api.submit_alpha(metrics["alpha_id"])
                    print(f"    [已提交] {metrics['alpha_id']}")
                    ledger.log({"event": "submitted", "alpha_id": metrics["alpha_id"]})
                    time.sleep(3)
                    status = api.check_submission_status(metrics["alpha_id"])
                    print(f"    [提交状态] {status['status']} 自相关:{status['self_corr_result']}")
                    ledger.log({"event": "submission_status", **status})
                except Exception as e:
                    print(f"    [提交失败] {e}")

            elif decision == "mutate_decorrelate":
                print(f"    [有信号但自相关高] Sharpe:{metrics['sharpe']:.2f} 自相关:{metrics['self_correlation']:.4f} (Δ{corr_delta:+.4f})")
                if corr_delta < -0.05:
                    stats["improved"] += 1
                    print(f"    [自相关下降!] 继续以此为基础变异")

            elif decision == "mutate_improve":
                print(f"    [有信号] Sharpe:{metrics['sharpe']:.2f} (Δ{sharpe_delta:+.2f}) 标签:{labels}")
                if sharpe_delta > 0.05:
                    stats["improved"] += 1

            else:
                print(f"    [淘汰] Sharpe:{metrics['sharpe']:.2f} Fitness:{metrics['fitness']:.2f} 标签:{labels}")

        except Exception as e:
            print(f"    [错误] {e}")
            ledger.log({"event": "error", "mutation": desc, "error": str(e)})
            if "429" in str(e):
                time.sleep(30)
            elif "401" in str(e):
                try: api.authenticate()
                except Exception: pass

        time.sleep(SIM_INTERVAL)

    return stats


def phase_explore(api: BrainAPI, ledger: Ledger, batch_size: int = 5) -> dict:
    """阶段 2：探索新方向 — 用策略模板冷启动"""
    print("\n[探索] 用策略模板寻找新母因子...")
    submitted_exprs = ledger.load_submitted_exprs()
    batch = generate_batch(batch_size=batch_size, submitted_exprs=submitted_exprs)
    stats = {"explored": 0, "promising": 0}

    if not batch:
        print("  模板池已耗尽")
        return stats

    for i, (expr, family, settings) in enumerate(batch, 1):
        print(f"\n  [{i}/{len(batch)}] [{family}] {expr[:60]}...")
        try:
            alpha_data = api.simulate_and_wait(expr, settings=settings, timeout=300)
            metrics = BrainAPI.extract_metrics(alpha_data)
            stats["explored"] += 1

            record = {
                "expr": expr, "family": family, "alpha_id": metrics["alpha_id"],
                "sharpe": metrics["sharpe"], "fitness": metrics["fitness"],
                "turnover": metrics["turnover"], "self_correlation": metrics["self_correlation"],
                "settings": settings,
            }
            decision, labels = diagnose(metrics)
            record["decision"] = decision
            record["labels"] = labels
            ledger.save_result(record)
            ledger.log({"event": "explore_sim", **record})

            if decision in ("submit", "mutate_decorrelate", "mutate_improve"):
                stats["promising"] += 1
                tag = "合格" if decision == "submit" else "有潜力"
                print(f"    [{tag}] Sharpe:{metrics['sharpe']:.2f} Fitness:{metrics['fitness']:.2f} 自相关:{metrics['self_correlation']:.4f}")
                if decision == "submit":
                    ledger.save_qualified(record)
                    try:
                        api.submit_alpha(metrics["alpha_id"])
                        print(f"    [已提交] {metrics['alpha_id']}")
                    except Exception as e:
                        print(f"    [提交失败] {e}")
            else:
                print(f"    [淘汰] Sharpe:{metrics['sharpe']:.2f} 标签:{labels}")

        except Exception as e:
            print(f"    [错误] {e}")
            if "429" in str(e): time.sleep(30)

        time.sleep(SIM_INTERVAL)

    return stats


# ── 主循环 ────────────────────────────────────────────────

def run_cycle(api: BrainAPI, ledger: Ledger, batch_size: int = 5, cycle_num: int = 0) -> dict:
    cycle_stats = {"cycle": cycle_num, "start": utc_now_iso()}

    # 每 5 轮或第一轮扫描平台
    if cycle_num % 5 == 0:
        pool = phase_scan(api, ledger)
    else:
        pool = ledger.load_pool_analysis()

    # 找母因子
    mothers = api.find_mother_candidates(pool, top_n=3) if pool.get("total") else []

    if mothers:
        print(f"\n找到 {len(mothers)} 个母因子候选")
        for m in mothers[:2]:
            stats = phase_mother_iterate(api, ledger, m, max_mutations=batch_size)
            cycle_stats[f"mother_{m.get('alpha_id', '')[:8]}"] = stats
    else:
        print("\n无母因子候选，进入探索模式")

    # 探索阶段（找新方向）
    explore_stats = phase_explore(api, ledger, batch_size=max(3, batch_size - 2))
    cycle_stats["explore"] = explore_stats
    cycle_stats["end"] = utc_now_iso()

    ledger.save_stats(cycle_stats)
    ledger.log({"event": "cycle_done", **cycle_stats})
    return cycle_stats


def main():
    parser = argparse.ArgumentParser(description="Alpha 7x24 母因子迭代闭环")
    parser.add_argument("--workspace", default=os.path.expanduser("~/.openclaw/workspace"))
    parser.add_argument("--batch-size", type=int, default=5)
    parser.add_argument("--interval", type=int, default=CYCLE_INTERVAL)
    parser.add_argument("--max-cycles", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    workspace = Path(args.workspace)
    ledger = Ledger(workspace)

    print("=" * 60)
    print("Alpha 7x24 母因子迭代闭环")
    print("=" * 60)
    print(f"工作区: {workspace}")
    print(f"策略: 母因子迭代 + 探索冷启动")
    print(f"批大小: {args.batch_size}  间隔: {args.interval}s")

    if args.dry_run:
        print("\n[DRY RUN]")
        stats = get_strategy_stats()
        for k, v in stats.items(): print(f"  {k}: {v}")
        return

    print("\n认证 BRAIN API...")
    api = BrainAPI()
    api.authenticate()
    print("认证成功")

    cycle = 0
    errors = 0

    while True:
        cycle += 1
        if args.max_cycles > 0 and cycle > args.max_cycles:
            break

        try:
            stats = run_cycle(api, ledger, batch_size=args.batch_size, cycle_num=cycle)
            errors = 0
            print(f"\n[第 {cycle} 轮完成] 等待 {args.interval}s...")
        except KeyboardInterrupt:
            print("\n用户中断")
            break
        except Exception as e:
            errors += 1
            print(f"\n[错误] {e}")
            traceback.print_exc()
            if errors >= MAX_CONSECUTIVE_ERRORS:
                backoff = min(300, 60 * errors)
                print(f"连续 {errors} 次错误，等待 {backoff}s...")
                time.sleep(backoff)
                try: api.authenticate()
                except Exception: pass

        time.sleep(args.interval)

    print(f"\n停止。共 {cycle} 轮。")


if __name__ == "__main__":
    main()
