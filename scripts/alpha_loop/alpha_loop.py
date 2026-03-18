#!/usr/bin/env python3
"""
Alpha 7x24 自动闭环 v3 — 母因子迭代 + LLM 生成 + 网格搜索 + 多区域。

执行策略（按优先级）：
  Phase 0: 扫描平台 → 缓存数据字段 → 分析 alpha 池
  Phase 1: 母因子迭代（结构性变异降自相关）
  Phase 2: Settings 网格搜索（对有潜力的跑不同 decay/neutralization）
  Phase 3: LLM 生成（用 GPT 推理新方向）
  Phase 4: 模板探索（冷启动 fallback）
  Phase 5: 多区域尝试（把有潜力的表达式放到其他区域跑）
  提交后轮询状态直到 PENDING 消失。

用法:
  python alpha_loop.py [--workspace /path] [--batch-size 5] [--interval 90]
"""
import argparse, json, os, sys, time, traceback, hashlib, subprocess
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from brain_api import BrainAPI, utc_now_iso, _safe_float
from alpha_strategies import generate_batch
from fingerprint import FingerprintDB

SHARPE_SUBMIT = 1.25
FITNESS_SUBMIT = 1.0
SELF_CORR_MAX = 0.70
SIM_INTERVAL = 10

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
GROUPING_SWAPS = [("subindustry", "industry"), ("industry", "sector"), ("sector", "subindustry")]
VOLUME_SWAPS = [
    ("volume / ts_mean(volume, 20)", "ts_mean(volume, 5) / ts_mean(volume, 40)"),
    ("volume / ts_mean(volume, 41)", "volume / ts_mean(volume, 20)"),
]
MUTATION_TYPES = ["swap_backbone", "add_trade_when", "swap_grouping", "add_group_rank", "swap_volume", "change_decay"]

SETTINGS_GRID = [
    {"decay": 4, "neutralization": "SUBINDUSTRY", "truncation": 0.06},
    {"decay": 6, "neutralization": "SUBINDUSTRY", "truncation": 0.08},
    {"decay": 8, "neutralization": "INDUSTRY", "truncation": 0.08},
    {"decay": 10, "neutralization": "SECTOR", "truncation": 0.10},
    {"decay": 12, "neutralization": "SUBINDUSTRY", "truncation": 0.05},
]

REGIONS = [
    {"region": "USA", "universe": "TOP3000"},
    {"region": "CHN", "universe": "TOP2000"},
    {"region": "EUR", "universe": "TOP1200"},
    {"region": "ASI", "universe": "TOP1200"},
]


def _expr_hash(expr: str) -> str:
    return hashlib.sha256(expr.strip().lower().encode()).hexdigest()[:16]


def mutate_expression(expr, tried):
    results = []
    for old, new in BACKBONE_SWAPS:
        if old in expr:
            m = expr.replace(old, new, 1)
            if m != expr and _expr_hash(m) not in tried:
                results.append((m, f"backbone:{old[:20]}->{new[:20]}", {}))
    for old, new in GROUPING_SWAPS:
        if old in expr.lower():
            m = expr.replace(old, new).replace(old.capitalize(), new.capitalize())
            if m != expr and _expr_hash(m) not in tried:
                results.append((m, f"group:{old}->{new}", {"neutralization": new.upper()}))
    for old, new in VOLUME_SWAPS:
        if old in expr:
            m = expr.replace(old, new, 1)
            if m != expr and _expr_hash(m) not in tried:
                results.append((m, f"vol:{old[:15]}->{new[:15]}", {}))
    if "trade_when" not in expr:
        for cond in ["volume > ts_mean(volume, 20)", "volume > 1.5 * ts_mean(volume, 20)",
                      "ts_std_dev(returns, 5) > ts_std_dev(returns, 20)"]:
            m = f"trade_when({expr}, {cond})"
            if _expr_hash(m) not in tried:
                results.append((m, f"tw:{cond[:25]}", {}))
    if "group_rank" not in expr:
        for g in ["subindustry", "industry"]:
            m = f"group_rank({g}, {expr})"
            if _expr_hash(m) not in tried:
                results.append((m, f"gr:{g}", {"neutralization": g.upper()}))
    return results


# ── LLM 生成 ──────────────────────────────────────────────

LLM_PROMPT_TEMPLATE = """你是 WorldQuant BRAIN 平台的量化因子专家。

## 平台可用数据字段
{data_fields}

## 可用函数
ts_mean(x,n) ts_sum(x,n) ts_std_dev(x,n) ts_corr(x,y,n) ts_rank(x,n) ts_delta(x,n) ts_delay(x,n)
rank(x) scale(x) signed_power(x,p) group_rank(group,x) trade_when(signal,condition)

## 当前 ACTIVE 的 alpha
{active_alphas}

## 最近 {n_failures} 个失败的主要原因
{failure_summary}

## 已试过的信号骨干（不要再用）
{tried_backbones}

## 任务
生成 {count} 个全新的 Alpha 表达式。要求：
1. 每个表达式必须有明确的投资假设（一句话说明）
2. 不要使用上面列出的已试过的信号骨干
3. 优先使用低拥挤度的数据字段（基本面、非常规价量组合）
4. 确保表达式语法正确，可直接在 BRAIN 平台运行
5. 每个表达式结构上要有明显差异

## 输出格式
严格按以下 JSON 格式输出，不要有其他内容：
[
  {{"expr": "表达式", "hypothesis": "投资假设", "family": "family标签"}},
  ...
]"""


def build_llm_prompt(ledger, data_fields, pool_analysis, count=5):
    active = pool_analysis.get("active_alphas", [])
    active_str = "\n".join(f"- {a.get('expression','')[:80]}" for a in active[:3]) or "无"

    submitted = ledger._load_json("alphas_submitted.json", {"submitted": []}).get("submitted", [])
    recent_failures = [r for r in submitted[-50:] if r.get("decision") in ("reject", "mutate_decorrelate", "mutate_improve")]
    label_counts = {}
    for r in recent_failures:
        for l in r.get("labels", []):
            label_counts[l] = label_counts.get(l, 0) + 1
    failure_str = ", ".join(f"{k}:{v}" for k, v in sorted(label_counts.items(), key=lambda x: -x[1])[:6]) or "无数据"

    backbones = set()
    for r in submitted[-100:]:
        e = r.get("expr", "").lower()
        for sig in ["ts_delta(close", "(close - open)", "(close / vwap", "(2 * close - high - low)",
                     "close / ts_mean(close", "ts_corr(close, volume", "ts_mean(returns"]:
            if sig in e:
                backbones.add(sig)
    backbones_str = ", ".join(sorted(backbones)) or "无"

    field_names = []
    if isinstance(data_fields, list):
        for f in data_fields[:50]:
            name = f.get("name") if isinstance(f, dict) else str(f)
            if name:
                field_names.append(name)
    fields_str = ", ".join(field_names) if field_names else "close, open, high, low, volume, vwap, returns, cap, sales, cashflow_op, operating_income, net_income, debt, assets, book_value, sharesout"

    return LLM_PROMPT_TEMPLATE.format(
        data_fields=fields_str, active_alphas=active_str,
        n_failures=len(recent_failures), failure_summary=failure_str,
        tried_backbones=backbones_str, count=count,
    )


def call_llm(prompt: str) -> list:
    """通过 openclaw agent 或直接 API 调用 LLM"""
    try:
        r = subprocess.run(
            ["openclaw", "agent", "--agent", "main", "-m", prompt, "--timeout", "120"],
            capture_output=True, text=True, timeout=180,
            cwd=os.path.expanduser("~/.openclaw/workspace"),
        )
        text = r.stdout or ""
    except Exception:
        return []

    start = text.find("[")
    end = text.rfind("]")
    if start >= 0 and end > start:
        try:
            items = json.loads(text[start:end + 1])
            return [(it["expr"], it.get("family", "llm_generated"), {}) for it in items if it.get("expr")]
        except Exception:
            pass
    return []


# ── 数据管理 ──────────────────────────────────────────────

class Ledger:
    def __init__(self, ws: Path):
        self.ws = ws
        ws.mkdir(parents=True, exist_ok=True)

    def _load_json(self, name, default=None):
        p = self.ws / name
        if not p.exists(): return default if default is not None else {}
        try: return json.loads(p.read_text(encoding="utf-8"))
        except Exception: return default if default is not None else {}

    def _save_json(self, name, data):
        (self.ws / name).write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    def _append_jsonl(self, name, rec):
        with (self.ws / name).open("a", encoding="utf-8") as f:
            f.write(json.dumps({**rec, "ts": utc_now_iso()}, ensure_ascii=False) + "\n")

    def tried_hashes(self) -> set:
        data = self._load_json("alphas_submitted.json", {"submitted": []})
        return {_expr_hash(r["expr"]) for r in data.get("submitted", []) if r.get("expr")}

    def submitted_exprs(self) -> set:
        data = self._load_json("alphas_submitted.json", {"submitted": []})
        return {r["expr"].strip() for r in data.get("submitted", []) if r.get("expr")}

    def save_result(self, rec):
        data = self._load_json("alphas_submitted.json", {"submitted": []})
        data.setdefault("submitted", []).append(rec)
        self._save_json("alphas_submitted.json", data)

    def save_qualified(self, rec):
        data = self._load_json("alphas_qualified.json", {"qualified": []})
        data.setdefault("qualified", []).append(rec)
        self._save_json("alphas_qualified.json", data)

    def log(self, ev): self._append_jsonl("alpha_loop_log.jsonl", ev)
    def save_stats(self, s): self._save_json("alpha_loop_stats.json", {**s, "updatedAt": utc_now_iso()})
    def save_pool(self, a): self._save_json("pool_analysis.json", {**a, "ts": utc_now_iso()})
    def load_pool(self): return self._load_json("pool_analysis.json", {})
    def save_fields(self, f): self._save_json("brain_data_fields.json", {"fields": f, "ts": utc_now_iso()})
    def load_fields(self): return self._load_json("brain_data_fields.json", {}).get("fields", [])


# ── 仿真 + 诊断 ──────────────────────────────────────────

def diagnose(m):
    labels = []
    s, f = _safe_float(m.get("sharpe")), _safe_float(m.get("fitness"))
    sc = _safe_float(m.get("self_correlation"))
    if s < SHARPE_SUBMIT: labels.append("low_sharpe")
    if f < FITNESS_SUBMIT: labels.append("low_fitness")
    if _safe_float(m.get("turnover")) > 0.70: labels.append("high_turnover")
    if _safe_float(m.get("drawdown")) > 0.10: labels.append("high_drawdown")
    if sc > SELF_CORR_MAX: labels.append("too_correlated")
    if not labels: return "submit", ["pass"]
    if s >= 1.0 and f >= 0.8 and "too_correlated" in labels: return "grid_search", labels
    if s >= 1.0 and f >= 0.8: return "mutate", labels
    return "reject", labels


def sim_one(api, expr, settings, ledger, tag="", extra=None, fpdb=None):
    """仿真一个表达式，记录结果，返回 (metrics, decision, labels)"""
    if fpdb and fpdb.should_skip(expr, threshold=0.80):
        nov = fpdb.novelty_score(expr)
        print(f"    [跳过] 结构相似度过高 (新颖度:{nov:.2f})")
        ledger.log({"event": "skip_similar", "expr": expr[:50], "novelty": nov, "tag": tag})
        raise ValueError("too_similar")
    try:
        alpha_data = api.simulate_and_wait(expr, settings=settings, timeout=300)
        metrics = BrainAPI.extract_metrics(alpha_data)
        decision, labels = diagnose(metrics)
        rec = {"expr": expr, "alpha_id": metrics["alpha_id"],
               "sharpe": metrics["sharpe"], "fitness": metrics["fitness"],
               "turnover": metrics["turnover"], "drawdown": metrics["drawdown"],
               "self_correlation": metrics["self_correlation"],
               "decision": decision, "labels": labels, "settings": settings, "tag": tag}
        if extra: rec.update(extra)
        ledger.save_result(rec)
        ledger.log({"event": "sim", **rec})
        return metrics, decision, labels
    except Exception as e:
        ledger.log({"event": "sim_error", "expr": expr[:50], "error": str(e), "tag": tag})
        if "429" in str(e): time.sleep(30)
        elif "401" in str(e):
            try: api.authenticate()
            except Exception: pass
        raise


def try_submit(api, metrics, rec, ledger):
    """尝试提交并轮询状态"""
    alpha_id = metrics["alpha_id"]
    print(f"    [提交] {alpha_id}...")
    ledger.save_qualified(rec)
    try:
        status = api.submit_and_track(alpha_id)
        print(f"    [状态] {status['status']} 自相关结果:{status['self_corr_result']} 值:{status['self_corr_value']:.4f}")
        ledger.log({"event": "submit_tracked", **status})
        return status
    except Exception as e:
        print(f"    [提交失败] {e}")
        ledger.log({"event": "submit_error", "alpha_id": alpha_id, "error": str(e)})
        return None


# ── 各阶段 ────────────────────────────────────────────────

def phase_scan(api, ledger):
    print("\n[Phase 0] 扫描平台...")
    try:
        fields = api.get_data_fields()
        if fields:
            ledger.save_fields(fields)
            names = [f.get("name") if isinstance(f, dict) else str(f) for f in fields[:15]]
            print(f"  数据字段: {names}")
    except Exception as e:
        print(f"  字段查询失败: {e}")
        fields = ledger.load_fields()

    try:
        pool = api.scan_alpha_pool(limit=300)
        ledger.save_pool(pool)
        print(f"  Alpha 池: {pool['total']} 个, 状态: {pool['by_status']}")
        print(f"  ACTIVE: {len(pool.get('active_alphas', []))}  被自相关挡: {len(pool.get('high_sharpe_blocked', []))}  接近合格: {len(pool.get('near_pass', []))}")
    except Exception as e:
        print(f"  池扫描失败: {e}")
        pool = ledger.load_pool()

    return pool, fields


def phase_mother(api, ledger, mothers, max_per_mother=4, fpdb=None):
    """Phase 1: 母因子迭代"""
    stats = {"tried": 0, "submitted": 0, "skipped": 0}
    tried = ledger.tried_hashes()

    for mother in mothers[:2]:
        expr = mother.get("expression", "")
        if not expr: continue
        print(f"\n[Phase 1] 母因子 {mother.get('alpha_id','')[:8]} Sharpe:{mother.get('sharpe',0):.2f} 自相关:{mother.get('self_correlation',0):.4f}")
        print(f"  {expr[:70]}...")

        mutations = mutate_expression(expr, tried)[:max_per_mother]
        if not mutations:
            print("  无可用变异")
            continue

        for m_expr, desc, m_settings in mutations:
            print(f"  [{desc}] {m_expr[:60]}...")
            try:
                metrics, decision, labels = sim_one(api, m_expr, m_settings, ledger, tag=f"mother:{desc}",
                                                      extra={"mother_id": mother.get("alpha_id")}, fpdb=fpdb)
                stats["tried"] += 1
                s, sc = metrics["sharpe"], metrics["self_correlation"]
                if decision == "submit":
                    print(f"    [合格!] Sharpe:{s:.2f} 自相关:{sc:.4f}")
                    try_submit(api, metrics, {"expr": m_expr, **metrics}, ledger)
                    stats["submitted"] += 1
                else:
                    print(f"    Sharpe:{s:.2f} 自相关:{sc:.4f} → {decision} {labels}")
            except ValueError:
                stats["skipped"] += 1
            except Exception:
                pass
            time.sleep(SIM_INTERVAL)

    return stats


def phase_grid_search(api, ledger, max_candidates=2):
    """Phase 2: 对有潜力但自相关高的表达式做 settings 网格"""
    data = ledger._load_json("alphas_submitted.json", {"submitted": []})
    candidates = [r for r in data.get("submitted", [])
                  if r.get("decision") in ("grid_search", "mutate")
                  and _safe_float(r.get("sharpe")) >= 1.0
                  and not r.get("grid_done")]
    candidates.sort(key=lambda x: _safe_float(x.get("sharpe")), reverse=True)

    if not candidates:
        return {"grid_tried": 0}

    stats = {"grid_tried": 0, "grid_submitted": 0}
    print(f"\n[Phase 2] Settings 网格搜索 ({len(candidates[:max_candidates])} 个候选)")

    for cand in candidates[:max_candidates]:
        expr = cand["expr"]
        print(f"  表达式: {expr[:60]}... (原 Sharpe:{cand.get('sharpe',0):.2f})")

        best_sharpe = _safe_float(cand.get("sharpe"))
        for gs in SETTINGS_GRID:
            if gs == cand.get("settings"): continue
            print(f"    decay:{gs['decay']} neut:{gs['neutralization']} trunc:{gs['truncation']}")
            try:
                metrics, decision, labels = sim_one(api, expr, gs, ledger, tag="grid",
                                                      extra={"grid_settings": gs})
                stats["grid_tried"] += 1
                if decision == "submit":
                    print(f"      [合格!] Sharpe:{metrics['sharpe']:.2f} 自相关:{metrics['self_correlation']:.4f}")
                    try_submit(api, metrics, {"expr": expr, **metrics}, ledger)
                    stats["grid_submitted"] += 1
                    break
                else:
                    print(f"      Sharpe:{metrics['sharpe']:.2f} 自相关:{metrics['self_correlation']:.4f} → {decision}")
            except Exception:
                pass
            time.sleep(SIM_INTERVAL)

    return stats


def phase_llm(api, ledger, pool, fields, count=5, fpdb=None):
    """Phase 3: LLM 驱动生成"""
    print(f"\n[Phase 3] LLM 生成 {count} 个新表达式...")
    prompt = build_llm_prompt(ledger, fields, pool, count=count)
    expressions = call_llm(prompt)

    if not expressions:
        print("  LLM 未返回有效表达式，跳过")
        return {"llm_tried": 0}

    stats = {"llm_tried": 0, "llm_submitted": 0}
    tried = ledger.tried_hashes()

    for expr, family, settings in expressions:
        if _expr_hash(expr) in tried:
            continue
        print(f"  [{family}] {expr[:60]}...")
        try:
            metrics, decision, labels = sim_one(api, expr, settings, ledger, tag=f"llm:{family}", fpdb=fpdb)
            stats["llm_tried"] += 1
            if decision == "submit":
                print(f"    [合格!] Sharpe:{metrics['sharpe']:.2f}")
                try_submit(api, metrics, {"expr": expr, **metrics}, ledger)
                stats["llm_submitted"] += 1
            else:
                print(f"    Sharpe:{metrics['sharpe']:.2f} → {decision} {labels}")
        except Exception:
            pass
        time.sleep(SIM_INTERVAL)

    return stats


def phase_explore(api, ledger, batch_size=3, fpdb=None):
    """Phase 4: 模板探索 fallback"""
    print(f"\n[Phase 4] 模板探索...")
    batch = generate_batch(batch_size=batch_size, submitted_exprs=ledger.submitted_exprs())
    stats = {"explored": 0, "promising": 0}

    for expr, family, settings in batch:
        print(f"  [{family}] {expr[:60]}...")
        try:
            metrics, decision, labels = sim_one(api, expr, settings, ledger, tag=f"explore:{family}", fpdb=fpdb)
            stats["explored"] += 1
            if decision == "submit":
                print(f"    [合格!]")
                try_submit(api, metrics, {"expr": expr, **metrics}, ledger)
                stats["promising"] += 1
            elif decision in ("grid_search", "mutate"):
                stats["promising"] += 1
                print(f"    [有潜力] Sharpe:{metrics['sharpe']:.2f} → 下轮网格搜索")
            else:
                print(f"    Sharpe:{metrics['sharpe']:.2f} → {decision}")
        except Exception:
            pass
        time.sleep(SIM_INTERVAL)

    return stats


def phase_multiregion(api, ledger, max_candidates=2):
    """Phase 5: 多区域尝试"""
    data = ledger._load_json("alphas_submitted.json", {"submitted": []})
    good = [r for r in data.get("submitted", [])
            if _safe_float(r.get("sharpe")) >= 1.0 and _safe_float(r.get("fitness")) >= 0.8
            and r.get("settings", {}).get("region", "USA") == "USA"
            and not r.get("multiregion_done")]
    good.sort(key=lambda x: _safe_float(x.get("sharpe")), reverse=True)

    if not good:
        return {"multiregion_tried": 0}

    stats = {"multiregion_tried": 0, "multiregion_submitted": 0}
    print(f"\n[Phase 5] 多区域 ({len(good[:max_candidates])} 个候选)")

    for cand in good[:max_candidates]:
        expr = cand["expr"]
        for reg in REGIONS[1:]:
            print(f"  [{reg['region']}] {expr[:50]}...")
            try:
                metrics, decision, labels = sim_one(api, expr, reg, ledger, tag=f"region:{reg['region']}")
                stats["multiregion_tried"] += 1
                if decision == "submit":
                    print(f"    [合格!] {reg['region']} Sharpe:{metrics['sharpe']:.2f}")
                    try_submit(api, metrics, {"expr": expr, **metrics}, ledger)
                    stats["multiregion_submitted"] += 1
                else:
                    print(f"    Sharpe:{metrics['sharpe']:.2f} → {decision}")
            except Exception:
                pass
            time.sleep(SIM_INTERVAL)

    return stats


# ── 主循环 ────────────────────────────────────────────────

def run_cycle(api, ledger, batch_size, cycle_num):
    cs = {"cycle": cycle_num, "start": utc_now_iso()}
    fpdb = FingerprintDB(ledger.ws)

    if cycle_num % 5 == 1:
        pool, fields = phase_scan(api, ledger)
    else:
        pool = ledger.load_pool()
        fields = ledger.load_fields()

    mothers = api.find_mother_candidates(pool, top_n=3) if pool.get("total") else []
    if mothers:
        cs["mother"] = phase_mother(api, ledger, mothers, max_per_mother=batch_size, fpdb=fpdb)

    cs["grid"] = phase_grid_search(api, ledger, max_candidates=2)

    if cycle_num % 3 == 0:
        cs["llm"] = phase_llm(api, ledger, pool, fields, count=5, fpdb=fpdb)

    cs["explore"] = phase_explore(api, ledger, batch_size=max(2, batch_size - 2), fpdb=fpdb)

    if cycle_num % 4 == 0:
        cs["multiregion"] = phase_multiregion(api, ledger, max_candidates=1)

    cs["end"] = utc_now_iso()
    ledger.save_stats(cs)
    ledger.log({"event": "cycle_done", **cs})

    print(f"\n[第 {cycle_num} 轮完成]")
    return cs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workspace", default=os.path.expanduser("~/.openclaw/workspace"))
    ap.add_argument("--batch-size", type=int, default=5)
    ap.add_argument("--interval", type=int, default=90)
    ap.add_argument("--max-cycles", type=int, default=0)
    args = ap.parse_args()

    ws = Path(args.workspace)
    ledger = Ledger(ws)

    print("=" * 60)
    print("Alpha 7x24 v3 — 母因子迭代 + LLM + 网格 + 多区域")
    print("=" * 60)

    api = BrainAPI()
    api.authenticate()
    print("认证成功\n")

    cycle, errors = 0, 0
    while True:
        cycle += 1
        if args.max_cycles > 0 and cycle > args.max_cycles: break
        try:
            run_cycle(api, ledger, args.batch_size, cycle)
            errors = 0
        except KeyboardInterrupt:
            break
        except Exception as e:
            errors += 1
            print(f"\n[错误] {e}")
            traceback.print_exc()
            if errors >= 5:
                time.sleep(min(300, 60 * errors))
                try: api.authenticate()
                except Exception: pass
        time.sleep(args.interval)

    print(f"\n停止。共 {cycle} 轮。")


if __name__ == "__main__":
    main()
