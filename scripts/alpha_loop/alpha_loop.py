#!/usr/bin/env python3
"""
Alpha 7x24 自动闭环主循环。

流程：
  1. 认证 BRAIN API
  2. 获取账号现有 alpha 和已提交记录
  3. 智能生成新表达式（自动去重、多策略）
  4. 逐个仿真
  5. 诊断结果 → 合格提交，有潜力的变异，差的丢弃
  6. 记录所有结果
  7. 等待间隔 → 回到 2

用法:
  python alpha_loop.py [--workspace /path] [--batch-size 8] [--interval 120] [--max-cycles 0] [--dry-run]
"""
import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from brain_api import BrainAPI, utc_now_iso, _safe_float
from alpha_strategies import generate_batch, get_strategy_stats, ALL_STRATEGIES, deduplicate

# ── 常量 ───────────────────────────────────────────────────

SHARPE_SUBMIT = 1.25
FITNESS_SUBMIT = 1.0
TURNOVER_MAX = 0.70
DRAWDOWN_MAX = 0.10

SHARPE_QUALIFIED = 2.0
FITNESS_QUALIFIED = 1.0

NEAR_PASS_MARGIN = 0.15

SIM_INTERVAL_SEC = 8
CYCLE_INTERVAL_SEC = 120
MAX_CONSECUTIVE_ERRORS = 5


# ── 数据管理 ──────────────────────────────────────────────

class AlphaLedger:
    """管理所有 Alpha 记录"""

    def __init__(self, workspace: Path):
        self.workspace = workspace
        self.workspace.mkdir(parents=True, exist_ok=True)
        self.submitted_file = workspace / "alphas_submitted.json"
        self.qualified_file = workspace / "alphas_qualified.json"
        self.log_file = workspace / "alpha_loop_log.jsonl"
        self.stats_file = workspace / "alpha_loop_stats.json"
        self.family_stats_file = workspace / "family_performance.json"

    def load_submitted_exprs(self) -> set:
        if not self.submitted_file.exists():
            return set()
        try:
            data = json.loads(self.submitted_file.read_text(encoding="utf-8"))
            return {r.get("expr", "").strip() for r in data.get("submitted", []) if r.get("expr")}
        except Exception:
            return set()

    def load_family_stats(self) -> dict:
        if not self.family_stats_file.exists():
            return {}
        try:
            return json.loads(self.family_stats_file.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def save_result(self, record: dict):
        data = {"submitted": []}
        if self.submitted_file.exists():
            try:
                data = json.loads(self.submitted_file.read_text(encoding="utf-8"))
            except Exception:
                pass
        data.setdefault("submitted", []).append(record)
        self.submitted_file.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    def save_qualified(self, record: dict):
        data = {"qualified": []}
        if self.qualified_file.exists():
            try:
                data = json.loads(self.qualified_file.read_text(encoding="utf-8"))
            except Exception:
                pass
        data.setdefault("qualified", []).append(record)
        self.qualified_file.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    def append_log(self, event: dict):
        with self.log_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps({**event, "ts": utc_now_iso()}, ensure_ascii=False) + "\n")

    def update_stats(self, stats: dict):
        existing = {}
        if self.stats_file.exists():
            try:
                existing = json.loads(self.stats_file.read_text(encoding="utf-8"))
            except Exception:
                pass
        existing.update(stats)
        existing["updatedAt"] = utc_now_iso()
        self.stats_file.write_text(json.dumps(existing, indent=2, ensure_ascii=False), encoding="utf-8")

    def update_family_performance(self, family: str, result: dict):
        stats = self.load_family_stats()
        fam = stats.setdefault(family, {
            "attempts": 0, "qualified": 0, "submitted": 0,
            "avg_sharpe": 0.0, "avg_fitness": 0.0,
            "best_sharpe": 0.0, "best_alpha_id": None,
            "last_attempt": None,
        })
        fam["attempts"] += 1
        fam["last_attempt"] = utc_now_iso()

        sharpe = _safe_float(result.get("sharpe"))
        fitness = _safe_float(result.get("fitness"))

        n = fam["attempts"]
        fam["avg_sharpe"] = fam["avg_sharpe"] * (n - 1) / n + sharpe / n
        fam["avg_fitness"] = fam["avg_fitness"] * (n - 1) / n + fitness / n

        if sharpe > fam.get("best_sharpe", 0):
            fam["best_sharpe"] = sharpe
            fam["best_alpha_id"] = result.get("alpha_id")

        if sharpe >= SHARPE_QUALIFIED and fitness >= FITNESS_QUALIFIED:
            fam["qualified"] += 1

        stats[family] = fam
        self.family_stats_file.write_text(json.dumps(stats, indent=2, ensure_ascii=False), encoding="utf-8")


# ── 诊断 ──────────────────────────────────────────────────

def diagnose(metrics: dict) -> tuple:
    """诊断仿真结果，返回 (decision, labels)"""
    labels = []
    sharpe = _safe_float(metrics.get("sharpe"))
    fitness = _safe_float(metrics.get("fitness"))
    turnover = _safe_float(metrics.get("turnover"))
    drawdown = _safe_float(metrics.get("drawdown"))
    self_corr = _safe_float(metrics.get("self_correlation"))

    if sharpe < SHARPE_SUBMIT:
        labels.append("low_sharpe")
    if fitness < FITNESS_SUBMIT:
        labels.append("low_fitness")
    if turnover > TURNOVER_MAX:
        labels.append("high_turnover")
    if drawdown > DRAWDOWN_MAX:
        labels.append("high_drawdown")
    if self_corr > 0.7:
        labels.append("too_correlated")

    if not labels:
        return "submit", ["pass"]

    near_pass = (
        sharpe >= SHARPE_SUBMIT - NEAR_PASS_MARGIN
        and fitness >= FITNESS_SUBMIT - NEAR_PASS_MARGIN
        and turnover <= TURNOVER_MAX
        and drawdown <= DRAWDOWN_MAX
    )
    if near_pass:
        labels.append("near_pass")
        return "improve", labels

    return "reject", labels


# ── 主循环 ────────────────────────────────────────────────

def run_cycle(api: BrainAPI, ledger: AlphaLedger, batch_size: int = 8, dry_run: bool = False) -> dict:
    """执行一个完整的研究循环"""
    cycle_start = time.time()
    cycle_stats = {
        "cycle_start": utc_now_iso(),
        "generated": 0,
        "simulated": 0,
        "submit_pool": 0,
        "improve_pool": 0,
        "reject_pool": 0,
        "qualified": 0,
        "errors": 0,
    }

    # 1. 加载已提交记录用于去重
    submitted_exprs = ledger.load_submitted_exprs()
    print(f"\n{'='*60}")
    print(f"[循环开始] {utc_now_iso()}")
    print(f"已有提交记录: {len(submitted_exprs)} 条")

    # 2. 分析 family 表现，排除表现差的 family
    family_stats = ledger.load_family_stats()
    exclude_families = set()
    for fam, stats in family_stats.items():
        attempts = stats.get("attempts", 0)
        qualified = stats.get("qualified", 0)
        if attempts >= 20 and qualified == 0 and stats.get("avg_sharpe", 0) < 0.8:
            exclude_families.add(fam)

    if exclude_families:
        print(f"排除表现差的 family: {len(exclude_families)} 个")

    # 3. 生成表达式
    batch = generate_batch(
        batch_size=batch_size,
        submitted_exprs=submitted_exprs,
        exclude_families=exclude_families,
    )
    cycle_stats["generated"] = len(batch)
    print(f"生成候选: {len(batch)} 个")

    if not batch:
        print("无可用候选表达式，跳过此轮")
        ledger.append_log({"event": "cycle_skip", "reason": "no_candidates"})
        return cycle_stats

    if dry_run:
        for expr, fam, _ in batch:
            print(f"  [DRY] [{fam}] {expr[:70]}")
        return cycle_stats

    # 4. 逐个仿真
    for idx, (expr, family, settings) in enumerate(batch, 1):
        print(f"\n[{idx}/{len(batch)}] [{family}] {expr[:60]}...")

        try:
            alpha_data = api.simulate_and_wait(expr, settings=settings, timeout=300)
            metrics = BrainAPI.extract_metrics(alpha_data)
            cycle_stats["simulated"] += 1

            record = {
                "expr": expr,
                "family": family,
                "alpha_id": metrics["alpha_id"],
                "sharpe": metrics["sharpe"],
                "fitness": metrics["fitness"],
                "turnover": metrics["turnover"],
                "drawdown": metrics["drawdown"],
                "self_correlation": metrics["self_correlation"],
                "settings": settings,
                "ts": utc_now_iso(),
            }

            decision, labels = diagnose(metrics)
            record["decision"] = decision
            record["labels"] = labels

            ledger.save_result(record)
            ledger.update_family_performance(family, metrics)
            ledger.append_log({"event": "simulation", "decision": decision, **record})

            if decision == "submit":
                cycle_stats["submit_pool"] += 1
                print(f"  [合格] Alpha: {metrics['alpha_id']}  Sharpe: {metrics['sharpe']:.2f}  Fitness: {metrics['fitness']:.2f}")

                if metrics["sharpe"] >= SHARPE_QUALIFIED and metrics["fitness"] >= FITNESS_QUALIFIED:
                    cycle_stats["qualified"] += 1
                    ledger.save_qualified(record)
                    print(f"  [高质量] 尝试提交到平台...")
                    try:
                        api.submit_alpha(metrics["alpha_id"])
                        print(f"  [已提交] {metrics['alpha_id']}")
                        ledger.append_log({"event": "submitted", "alpha_id": metrics["alpha_id"]})
                    except Exception as e:
                        print(f"  [提交失败] {e}")
                        ledger.append_log({"event": "submit_error", "alpha_id": metrics["alpha_id"], "error": str(e)})

            elif decision == "improve":
                cycle_stats["improve_pool"] += 1
                print(f"  [有潜力] Alpha: {metrics['alpha_id']}  Sharpe: {metrics['sharpe']:.2f}  Fitness: {metrics['fitness']:.2f}  标签: {labels}")
            else:
                cycle_stats["reject_pool"] += 1
                print(f"  [淘汰] Sharpe: {metrics['sharpe']:.2f}  Fitness: {metrics['fitness']:.2f}  标签: {labels}")

        except Exception as e:
            cycle_stats["errors"] += 1
            error_msg = str(e)
            print(f"  [错误] {error_msg}")
            ledger.append_log({"event": "error", "expr": expr[:50], "error": error_msg})

            if "429" in error_msg or "rate" in error_msg.lower():
                print("  触发限流，等待 30 秒...")
                time.sleep(30)
            elif "401" in error_msg:
                print("  认证失效，重新认证...")
                try:
                    api.authenticate()
                except Exception:
                    pass

        time.sleep(SIM_INTERVAL_SEC)

    # 5. 循环总结
    elapsed = time.time() - cycle_start
    cycle_stats["elapsed_sec"] = round(elapsed, 1)
    cycle_stats["cycle_end"] = utc_now_iso()

    print(f"\n{'─'*60}")
    print(f"循环完成 ({elapsed:.0f}s)")
    print(f"  仿真: {cycle_stats['simulated']}/{cycle_stats['generated']}")
    print(f"  合格: {cycle_stats['submit_pool']}  有潜力: {cycle_stats['improve_pool']}  淘汰: {cycle_stats['reject_pool']}")
    print(f"  高质量提交: {cycle_stats['qualified']}  错误: {cycle_stats['errors']}")

    ledger.update_stats(cycle_stats)
    ledger.append_log({"event": "cycle_done", **cycle_stats})

    return cycle_stats


def main():
    parser = argparse.ArgumentParser(description="Alpha 7x24 自动闭环")
    parser.add_argument("--workspace", default=os.path.expanduser("~/.openclaw/workspace"),
                        help="工作区路径")
    parser.add_argument("--batch-size", type=int, default=8,
                        help="每轮生成的候选数量")
    parser.add_argument("--interval", type=int, default=CYCLE_INTERVAL_SEC,
                        help="循环间隔（秒）")
    parser.add_argument("--max-cycles", type=int, default=0,
                        help="最大循环次数（0=无限）")
    parser.add_argument("--dry-run", action="store_true",
                        help="仅生成表达式，不实际仿真")
    args = parser.parse_args()

    workspace = Path(args.workspace)
    workspace.mkdir(parents=True, exist_ok=True)
    ledger = AlphaLedger(workspace)

    print("="*60)
    print("Alpha 7x24 自动闭环系统")
    print("="*60)
    print(f"工作区: {workspace}")
    print(f"批大小: {args.batch_size}")
    print(f"间隔: {args.interval}s")
    print(f"最大循环: {'无限' if args.max_cycles == 0 else args.max_cycles}")

    strategy_stats = get_strategy_stats()
    print(f"\n策略库容量:")
    for name, count in strategy_stats.items():
        print(f"  {name}: {count}")

    if args.dry_run:
        print("\n[DRY RUN 模式] 不会实际调用 API")
        api = None
    else:
        print("\n正在认证 BRAIN API...")
        api = BrainAPI()
        api.authenticate()
        print("认证成功")

    cycle_count = 0
    consecutive_errors = 0
    total_stats = {"total_simulated": 0, "total_qualified": 0, "total_submitted": 0, "total_errors": 0}

    while True:
        cycle_count += 1
        if args.max_cycles > 0 and cycle_count > args.max_cycles:
            print(f"\n达到最大循环次数 ({args.max_cycles})，退出")
            break

        try:
            stats = run_cycle(api, ledger, batch_size=args.batch_size, dry_run=args.dry_run)
            total_stats["total_simulated"] += stats.get("simulated", 0)
            total_stats["total_qualified"] += stats.get("qualified", 0)
            total_stats["total_submitted"] += stats.get("submit_pool", 0)
            total_stats["total_errors"] += stats.get("errors", 0)

            if stats.get("errors", 0) == 0:
                consecutive_errors = 0
            else:
                consecutive_errors += 1

        except KeyboardInterrupt:
            print("\n用户中断，退出")
            break
        except Exception as e:
            consecutive_errors += 1
            print(f"\n[致命错误] {e}")
            traceback.print_exc()
            ledger.append_log({"event": "fatal_error", "error": str(e)})

        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
            backoff = min(300, 30 * consecutive_errors)
            print(f"\n连续错误 {consecutive_errors} 次，等待 {backoff}s 后重试...")
            time.sleep(backoff)

            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS * 2:
                print("错误过多，尝试重新认证...")
                try:
                    api.authenticate()
                    consecutive_errors = 0
                except Exception as e:
                    print(f"重新认证失败: {e}")

        if not args.dry_run:
            print(f"\n累计: 仿真 {total_stats['total_simulated']} | 合格 {total_stats['total_submitted']} | 高质量 {total_stats['total_qualified']} | 错误 {total_stats['total_errors']}")

        generated = stats.get("generated", 0) if stats else 0
        if generated == 0:
            print(f"候选池已耗尽，等待 {args.interval * 3}s 后重试...")
            time.sleep(args.interval * 3)
        else:
            print(f"等待 {args.interval}s 后进入下一循环...")
            time.sleep(args.interval)

    ledger.update_stats({**total_stats, "total_cycles": cycle_count, "status": "stopped"})
    print(f"\n{'='*60}")
    print(f"系统停止。总计 {cycle_count} 轮循环")
    print(json.dumps(total_stats, indent=2))


if __name__ == "__main__":
    main()
