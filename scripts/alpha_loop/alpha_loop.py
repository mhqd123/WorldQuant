#!/usr/bin/env python3
"""
Alpha 7x24 自动闭环 v4 — 统一架构。

合并了 alpha_loop (API集成/生成/去重) 与 brain_research (诊断/变异/调度) 的全部能力。

执行策略（按 explore/exploit/improve/retry 四桶动态分配）：
  Phase 0: 扫描平台 → 缓存数据字段 → 分析 alpha 池 → 更新 family 统计
  Phase 1: improve 桶 — 对有潜力的 alpha 做智能变异（来自 improvement_service 的变异器）
  Phase 2: exploit 桶 — Settings 网格搜索 + 母因子迭代
  Phase 3: explore 桶 — 模板探索 + LLM 生成
  Phase 4: retry 桶 — 对失败但 near_pass 的重试
  Phase 5: 多区域尝试
  提交后轮询状态直到 PENDING 消失。

用法:
  python alpha_loop.py [--workspace /path] [--batch-size 8] [--interval 120]
"""
import argparse, json, os, sys, time, traceback, hashlib, subprocess, fcntl
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from brain_api import BrainAPI, utc_now_iso, _safe_float
from alpha_strategies import generate_batch
from fingerprint import FingerprintDB

SHARPE_SUBMIT = 1.25
FITNESS_SUBMIT = 1.0
TURNOVER_MAX = 0.70
DRAWDOWN_MAX = 0.10
SELF_CORR_MAX = 0.70
NEAR_PASS_MARGIN = 0.10
SIM_INTERVAL = 10
MAX_GENERATION = 2

BUCKET_DEFAULT = {"explore": 2, "exploit": 3, "improve": 2, "retry": 1}
BUCKET_IMPROVE_HEAVY = {"explore": 1, "exploit": 2, "improve": 3, "retry": 1}
BUCKET_EXPLORE_HEAVY = {"explore": 3, "exploit": 2, "improve": 1, "retry": 1}

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


# ── 统一诊断器 (合并 diagnoser.py) ────────────────────────

def diagnose(m):
    """诊断仿真结果，返回 (decision, labels)。
    decision: submit / improve / grid_search / reject
    labels 提供变异器使用的问题标签。
    """
    labels = []
    s = _safe_float(m.get("sharpe"))
    f = _safe_float(m.get("fitness"))
    sc = _safe_float(m.get("self_correlation"))
    to = _safe_float(m.get("turnover"))
    dd = _safe_float(m.get("drawdown"))

    if s < SHARPE_SUBMIT:
        labels.append("low_sharpe")
    if f < FITNESS_SUBMIT:
        labels.append("low_fitness")
    if to > TURNOVER_MAX:
        labels.append("high_turnover")
    if dd > DRAWDOWN_MAX:
        labels.append("high_drawdown")
    if sc > SELF_CORR_MAX:
        labels.append("too_correlated")

    if not labels:
        return "submit", ["pass"]

    near_pass = (
        s >= SHARPE_SUBMIT - NEAR_PASS_MARGIN
        and f >= FITNESS_SUBMIT - NEAR_PASS_MARGIN
        and to <= TURNOVER_MAX
        and dd <= DRAWDOWN_MAX
    )

    if near_pass:
        labels.append("near_pass")
        if "too_correlated" in labels:
            return "grid_search", labels
        return "improve", labels

    if s >= 1.0 and f >= 0.8 and "too_correlated" in labels:
        return "grid_search", labels
    if s >= 1.0 and f >= 0.8:
        return "improve", labels

    return "reject", labels


# ── 统一变异器 (合并 mutator + improvement_service) ───────

MUTATION_PLAYBOOK = {
    "low_sharpe": ["add_smoothing", "swap_backbone", "rotate_neutralization"],
    "low_fitness": ["increase_decay", "remove_smoothing", "toggle_volume"],
    "high_turnover": ["increase_decay", "shift_horizon_longer", "add_smoothing"],
    "high_drawdown": ["tighten_truncation", "increase_decay"],
    "too_correlated": ["swap_backbone", "rotate_neutralization", "toggle_volume",
                       "toggle_grouping", "add_trade_when", "add_group_rank"],
}


def propose_mutations(labels):
    """根据诊断标签提出变异动作列表"""
    actions = []
    for label in labels:
        actions.extend(MUTATION_PLAYBOOK.get(label, []))
    seen = set()
    return [a for a in actions if not (a in seen or seen.add(a))][:6]


def identify_backbone(expr):
    e = expr.lower()
    if "(2 * close - high - low)" in e:
        return "intraday_position"
    if "(close - open)" in e:
        return "intraday_body"
    if "ts_delta(close" in e:
        return "delta"
    if "ts_mean(close" in e and "ts_delta" not in e:
        return "reversion"
    if any(f in e for f in ["sales", "cashflow", "operating_income", "net_income", "debt", "book_value"]):
        return "fundamental"
    return "generic"


def mutate_expression(expr, action, settings=None):
    """对表达式施加一个变异动作，返回 (new_expr, new_settings, expected_effect)"""
    new_expr = expr
    new_settings = dict(settings or {})
    expected = "unknown"

    if action == "swap_backbone":
        bb = identify_backbone(expr)
        swaps = {
            "intraday_position": [("(2 * close - high - low)", "ts_delta(close, 2)")],
            "intraday_body": [("(close - open)", "ts_delta(close, 3)")],
            "delta": [
                ("ts_delta(close, 2)", "(close - ts_mean(close, 6))"),
                ("ts_delta(close, 5)", "(close - ts_mean(close, 10))"),
                ("ts_delta(close, 3)", "(close / ts_mean(close, 8) - 1)"),
            ],
            "reversion": [
                ("(close / ts_mean(close, 10) - 1)", "(close / ts_mean(close, 20) - 1)"),
                ("(close / ts_mean(close, 15) - 1)", "ts_delta(close, 10) / close"),
            ],
        }
        for old, new in swaps.get(bb, []):
            if old in new_expr:
                new_expr = new_expr.replace(old, new, 1)
                expected = "de-correlation"
                break

    elif action == "rotate_neutralization":
        order = ["SUBINDUSTRY", "INDUSTRY", "SECTOR"]
        current = new_settings.get("neutralization", "SUBINDUSTRY")
        if current in order:
            new_settings["neutralization"] = order[(order.index(current) + 1) % len(order)]
        else:
            new_settings["neutralization"] = "SUBINDUSTRY"
        expected = "de-correlation"

    elif action == "toggle_grouping":
        if "subindustry" in new_expr:
            new_expr = new_expr.replace("subindustry", "industry")
        elif "industry" in new_expr.lower() and "subindustry" not in new_expr:
            new_expr = new_expr.replace("industry", "sector").replace("Industry", "Sector")
        elif "sector" in new_expr.lower():
            new_expr = new_expr.replace("sector", "subindustry").replace("Sector", "Subindustry")
        expected = "de-correlation"

    elif action == "toggle_volume":
        if "rank(volume / ts_mean(volume," in new_expr:
            new_expr = new_expr.replace("rank(volume / ts_mean(volume,",
                                        "rank(ts_mean(volume, 5) / ts_mean(volume,")
        elif "rank(ts_mean(volume, 5) / ts_mean(volume," in new_expr:
            new_expr = new_expr.replace("rank(ts_mean(volume, 5) / ts_mean(volume,",
                                        "rank(volume / ts_mean(volume,")
        elif "volume / ts_mean(volume, 20)" in new_expr:
            new_expr = new_expr.replace("volume / ts_mean(volume, 20)",
                                        "ts_mean(volume, 5) / ts_mean(volume, 40)")
        expected = "de-correlation"

    elif action == "increase_decay":
        new_settings["decay"] = min(15, int(new_settings.get("decay", 6)) + 2)
        expected = "turnover_down"

    elif action == "decrease_decay":
        new_settings["decay"] = max(1, int(new_settings.get("decay", 6)) - 2)
        expected = "sharpe_up"

    elif action == "shift_horizon_longer":
        new_settings["decay"] = max(int(new_settings.get("decay", 6)), 8)
        expected = "turnover_down"

    elif action == "tighten_truncation":
        new_settings["truncation"] = round(max(0.04, float(new_settings.get("truncation", 0.08)) - 0.02), 4)
        expected = "drawdown_down"

    elif action == "relax_truncation":
        new_settings["truncation"] = round(min(0.12, float(new_settings.get("truncation", 0.08)) + 0.02), 4)
        expected = "sharpe_up"

    elif action == "add_smoothing":
        if "(1 + high - low)" in new_expr:
            new_expr = new_expr.replace("(1 + high - low)", "(1 + ts_mean(high - low, 3))")
        elif "ts_mean(high - low, 3)" in new_expr:
            new_expr = new_expr.replace("ts_mean(high - low, 3)", "ts_mean(high - low, 5)")
        expected = "turnover_down"

    elif action == "remove_smoothing":
        new_expr = new_expr.replace("(1 + ts_mean(high - low, 5))", "(1 + high - low)")
        new_expr = new_expr.replace("(1 + ts_mean(high - low, 3))", "(1 + high - low)")
        expected = "sharpe_up"

    elif action == "add_trade_when":
        if "trade_when" not in new_expr:
            for cond in ["volume > ts_mean(volume, 20)",
                         "ts_std_dev(returns, 5) > ts_std_dev(returns, 20)"]:
                candidate = f"trade_when({new_expr}, {cond})"
                new_expr = candidate
                expected = "de-correlation"
                break

    elif action == "add_group_rank":
        if "group_rank" not in new_expr:
            for g in ["subindustry", "industry"]:
                candidate = f"group_rank({g}, {new_expr})"
                new_expr = candidate
                new_settings["neutralization"] = g.upper()
                expected = "de-correlation"
                break

    return new_expr, new_settings, expected


def estimate_mutation_gain(action, labels):
    """估算变异的预期收益"""
    base = 0.25
    if "too_correlated" in labels and action in {"swap_backbone", "toggle_grouping",
                                                   "toggle_volume", "add_trade_when", "add_group_rank"}:
        base += 0.35
    if "low_fitness" in labels and action in {"remove_smoothing", "decrease_decay"}:
        base += 0.15
    if "high_turnover" in labels and action in {"increase_decay", "add_smoothing", "shift_horizon_longer"}:
        base += 0.15
    if "high_drawdown" in labels and action in {"tighten_truncation", "increase_decay"}:
        base += 0.10
    return round(min(base, 0.95), 4)


def generate_improve_candidates(candidate, labels, tried_hashes, max_children=3):
    """为一个有潜力的 alpha 生成变异候选列表"""
    generation = int(candidate.get("generation", 0))
    if generation >= MAX_GENERATION:
        return []
    if int(candidate.get("same_diagnosis_count", 0)) >= 2:
        return []

    actions = propose_mutations(labels)
    out = []
    settings = dict(candidate.get("settings", {}))
    expr = candidate.get("expr", candidate.get("expression", ""))

    for action in actions:
        new_expr, new_settings, expected = mutate_expression(expr, action, settings)
        if new_expr == expr and new_settings == settings:
            continue
        h = _expr_hash(new_expr)
        if h in tried_hashes:
            continue
        gain = estimate_mutation_gain(action, labels)
        if gain < 0.2:
            continue
        out.append({
            "expr": new_expr,
            "settings": new_settings,
            "parent_id": candidate.get("alpha_id", ""),
            "family": candidate.get("family", candidate.get("tag", "unknown")),
            "generation": generation + 1,
            "mutation": action,
            "expected_effect": expected,
            "expected_gain": gain,
            "same_diagnosis_count": candidate.get("same_diagnosis_count", 0) + 1,
            "source_bucket": "improve",
        })
        if len(out) >= max_children:
            break
    return out


# ── Family 统计与冻结 (合并 scheduler) ────────────────────

def build_family_stats(submitted_records):
    """从提交记录中构建 family 级别的统计"""
    stats = defaultdict(lambda: {
        "attempts": 0, "near_pass": 0, "submitted": 0,
        "consec_no_near": 0, "consec_no_submit": 0,
        "gain_sum": 0.0, "gain_count": 0,
    })

    sorted_recs = sorted(submitted_records, key=lambda r: r.get("ts", ""))
    for rec in sorted_recs:
        family = rec.get("family", rec.get("tag", "unknown"))
        if family and ":" in family:
            family = family.split(":")[0]
        s = stats[family]
        s["attempts"] += 1

        labels = rec.get("labels", [])
        if "near_pass" in labels or "pass" in labels:
            s["near_pass"] += 1
            s["consec_no_near"] = 0
        else:
            s["consec_no_near"] += 1

        decision = rec.get("decision", "")
        if decision == "submit":
            s["submitted"] += 1
            s["consec_no_submit"] = 0
        else:
            s["consec_no_submit"] += 1

        sharpe = _safe_float(rec.get("sharpe"))
        if sharpe > 0:
            s["gain_sum"] += sharpe
            s["gain_count"] += 1

    result = {}
    for family, s in stats.items():
        attempts = max(1, s["attempts"])
        result[family] = {
            **s,
            "success_rate": s["submitted"] / attempts,
            "near_pass_rate": s["near_pass"] / attempts,
            "avg_sharpe": s["gain_sum"] / max(1, s["gain_count"]),
        }
    return result


def apply_family_freeze(family_stats):
    """对表现差的 family 施加冻结策略"""
    now = datetime.now(timezone.utc)
    for family, s in family_stats.items():
        if s.get("consec_no_submit", 0) >= 20:
            s["frozen_until"] = (now + timedelta(hours=72)).isoformat()
            s["freeze_reason"] = "20_consecutive_no_submit"
        elif s.get("consec_no_near", 0) >= 10:
            s["frozen_until"] = (now + timedelta(hours=24)).isoformat()
            s["freeze_reason"] = "10_consecutive_no_near_pass"
    return family_stats


def is_frozen(family, family_stats):
    s = family_stats.get(family, {})
    frozen_until = s.get("frozen_until")
    if not frozen_until:
        return False
    try:
        deadline = datetime.fromisoformat(frozen_until.replace("Z", "+00:00"))
        return datetime.now(timezone.utc) < deadline
    except Exception:
        return False


def choose_bucket_plan(improve_queue_size, family_stats):
    """动态选择桶分配方案"""
    if improve_queue_size >= 8:
        return BUCKET_IMPROVE_HEAVY
    high_value_families = sum(1 for s in family_stats.values()
                              if s.get("success_rate", 0) >= 0.02 or s.get("near_pass_rate", 0) >= 0.1)
    if high_value_families >= 3:
        return BUCKET_EXPLORE_HEAVY
    return BUCKET_DEFAULT


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
1. 每个表达式必须有明确的投资假设
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
    active_str = "\n".join(f"- {a.get('expression', '')[:80]}" for a in active[:5]) or "无"

    submitted = ledger.load_submitted()
    recent_failures = [r for r in submitted[-50:]
                       if r.get("decision") in ("reject", "improve", "grid_search")]
    label_counts = {}
    for r in recent_failures:
        for l in r.get("labels", []):
            label_counts[l] = label_counts.get(l, 0) + 1
    failure_str = ", ".join(f"{k}:{v}" for k, v in
                            sorted(label_counts.items(), key=lambda x: -x[1])[:6]) or "无数据"

    backbones = set()
    for r in submitted[-100:]:
        e = r.get("expr", "").lower()
        for sig in ["ts_delta(close", "(close - open)", "(close / vwap",
                     "(2 * close - high - low)", "close / ts_mean(close",
                     "ts_corr(close, volume", "ts_mean(returns"]:
            if sig in e:
                backbones.add(sig)
    backbones_str = ", ".join(sorted(backbones)) or "无"

    field_names = []
    if isinstance(data_fields, list):
        for f in data_fields[:50]:
            name = f.get("name") if isinstance(f, dict) else str(f)
            if name:
                field_names.append(name)
    fields_str = ", ".join(field_names) if field_names else \
        "close, open, high, low, volume, vwap, returns, cap, sales, cashflow_op, operating_income, net_income, debt, assets, book_value, sharesout"

    return LLM_PROMPT_TEMPLATE.format(
        data_fields=fields_str, active_alphas=active_str,
        n_failures=len(recent_failures), failure_summary=failure_str,
        tried_backbones=backbones_str, count=count,
    )


def call_llm(prompt):
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
            return [(it["expr"], it.get("family", "llm_generated"), {})
                    for it in items if it.get("expr")]
        except Exception:
            pass
    return []


# ── 数据管理（带文件锁） ──────────────────────────────────

class Ledger:
    def __init__(self, ws: Path):
        self.ws = ws
        ws.mkdir(parents=True, exist_ok=True)

    def _load_json(self, name, default=None):
        p = self.ws / name
        if not p.exists():
            return default if default is not None else {}
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return default if default is not None else {}

    def _save_json_locked(self, name, data):
        p = self.ws / name
        lock_path = self.ws / f".{name}.lock"
        try:
            with open(lock_path, "w") as lf:
                fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
                p.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
                fcntl.flock(lf.fileno(), fcntl.LOCK_UN)
        except (ImportError, OSError):
            p.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    def _append_jsonl(self, name, rec):
        with (self.ws / name).open("a", encoding="utf-8") as f:
            f.write(json.dumps({**rec, "ts": utc_now_iso()}, ensure_ascii=False) + "\n")

    def tried_hashes(self):
        data = self._load_json("alphas_submitted.json", {"submitted": []})
        return {_expr_hash(r["expr"]) for r in data.get("submitted", []) if r.get("expr")}

    def submitted_exprs(self):
        data = self._load_json("alphas_submitted.json", {"submitted": []})
        return {r["expr"].strip() for r in data.get("submitted", []) if r.get("expr")}

    def load_submitted(self):
        return self._load_json("alphas_submitted.json", {"submitted": []}).get("submitted", [])

    def save_result(self, rec):
        data = self._load_json("alphas_submitted.json", {"submitted": []})
        data.setdefault("submitted", []).append(rec)
        self._save_json_locked("alphas_submitted.json", data)

    def save_qualified(self, rec):
        data = self._load_json("alphas_qualified.json", {"qualified": []})
        data.setdefault("qualified", []).append(rec)
        self._save_json_locked("alphas_qualified.json", data)

    def load_improve_queue(self):
        return self._load_json("improve_queue.json", {"queue": []}).get("queue", [])

    def save_improve_queue(self, queue):
        self._save_json_locked("improve_queue.json", {"queue": queue, "ts": utc_now_iso()})

    def log(self, ev):
        self._append_jsonl("alpha_loop_log.jsonl", ev)

    def save_stats(self, s):
        self._save_json_locked("alpha_loop_stats.json", {**s, "updatedAt": utc_now_iso()})

    def save_pool(self, a):
        self._save_json_locked("pool_analysis.json", {**a, "ts": utc_now_iso()})

    def load_pool(self):
        return self._load_json("pool_analysis.json", {})

    def save_fields(self, f):
        self._save_json_locked("brain_data_fields.json", {"fields": f, "ts": utc_now_iso()})

    def load_fields(self):
        return self._load_json("brain_data_fields.json", {}).get("fields", [])

    def save_family_stats(self, s):
        self._save_json_locked("family_stats.json", {"families": s, "ts": utc_now_iso()})

    def load_family_stats(self):
        return self._load_json("family_stats.json", {}).get("families", {})

    def load_field_scan(self):
        return self._load_json("field_scan_report.json", {})


# ── 仿真核心 ─────────────────────────────────────────────

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
        rec = {
            "expr": expr, "alpha_id": metrics["alpha_id"],
            "sharpe": metrics["sharpe"], "fitness": metrics["fitness"],
            "turnover": metrics["turnover"], "drawdown": metrics["drawdown"],
            "self_correlation": metrics["self_correlation"],
            "decision": decision, "labels": labels, "settings": settings, "tag": tag,
        }
        if extra:
            rec.update(extra)
        ledger.save_result(rec)
        ledger.log({"event": "sim", **rec})
        return metrics, decision, labels
    except ValueError:
        raise
    except Exception as e:
        ledger.log({"event": "sim_error", "expr": expr[:50], "error": str(e), "tag": tag})
        if "429" in str(e):
            time.sleep(30)
        elif "401" in str(e):
            try:
                api.authenticate()
            except Exception:
                pass
        raise


def try_submit(api, metrics, rec, ledger):
    alpha_id = metrics["alpha_id"]
    print(f"    [提交] {alpha_id}...")
    ledger.save_qualified(rec)
    try:
        status = api.submit_and_track(alpha_id)
        print(f"    [状态] {status['status']} SC结果:{status['self_corr_result']} 值:{status['self_corr_value']:.4f}")
        ledger.log({"event": "submit_tracked", **status})
        return status
    except Exception as e:
        print(f"    [提交失败] {e}")
        ledger.log({"event": "submit_error", "alpha_id": alpha_id, "error": str(e)})
        return None


# ── Phase 0: 扫描平台 ────────────────────────────────────

def phase_scan(api, ledger):
    print("\n[Phase 0] 扫描平台...")
    fields = []
    try:
        fields = api.get_data_fields()
        if fields:
            ledger.save_fields(fields)
            names = [f.get("name") if isinstance(f, dict) else str(f) for f in fields[:15]]
            print(f"  数据字段: {names}")
    except Exception as e:
        print(f"  字段查询失败: {e}")
        fields = ledger.load_fields()

    pool = {}
    try:
        pool = api.scan_alpha_pool(limit=300)
        ledger.save_pool(pool)
        print(f"  Alpha 池: {pool['total']} 个, 状态: {pool['by_status']}")
        print(f"  ACTIVE: {len(pool.get('active_alphas', []))}  "
              f"被SC挡: {len(pool.get('high_sharpe_blocked', []))}  "
              f"Near-pass: {len(pool.get('near_pass', []))}")
    except Exception as e:
        print(f"  池扫描失败: {e}")
        pool = ledger.load_pool()

    submitted = ledger.load_submitted()
    fstats = build_family_stats(submitted)
    fstats = apply_family_freeze(fstats)
    ledger.save_family_stats(fstats)

    frozen_list = [f for f, s in fstats.items() if is_frozen(f, fstats)]
    if frozen_list:
        print(f"  冻结 families: {frozen_list[:5]}")

    return pool, fields, fstats


# ── Phase 1: improve 桶 ──────────────────────────────────

def phase_improve(api, ledger, fstats, slots, fpdb=None):
    """对 improve 队列里的候选做仿真"""
    queue = ledger.load_improve_queue()
    if not queue:
        return {"improve_tried": 0}

    queue = [c for c in queue if not is_frozen(c.get("family", ""), fstats)]
    queue.sort(key=lambda x: x.get("expected_gain", 0), reverse=True)

    stats = {"improve_tried": 0, "improve_submitted": 0, "improve_skipped": 0}
    tried = ledger.tried_hashes()
    remaining_queue = list(queue)

    print(f"\n[Phase 1] Improve 桶 ({len(queue)} 个候选, 分配 {slots} 槽)")

    for cand in queue[:slots]:
        expr = cand.get("expr", "")
        h = _expr_hash(expr)
        if h in tried:
            remaining_queue.remove(cand)
            continue

        print(f"  [{cand.get('mutation', '?')}] {expr[:55]}... (预期:{cand.get('expected_effect', '?')})")
        try:
            metrics, decision, labels = sim_one(
                api, expr, cand.get("settings", {}), ledger,
                tag=f"improve:{cand.get('mutation', '')}", fpdb=fpdb,
                extra={"parent_id": cand.get("parent_id"), "generation": cand.get("generation")},
            )
            stats["improve_tried"] += 1
            remaining_queue.remove(cand)

            if decision == "submit":
                print(f"    [合格!] Sharpe:{metrics['sharpe']:.2f} SC:{metrics['self_correlation']:.4f}")
                try_submit(api, metrics, {"expr": expr, **metrics}, ledger)
                stats["improve_submitted"] += 1
            elif decision == "improve" and cand.get("generation", 0) < MAX_GENERATION:
                new_mutations = generate_improve_candidates(
                    {**cand, "alpha_id": metrics["alpha_id"], "sharpe": metrics["sharpe"]},
                    labels, tried, max_children=2)
                remaining_queue.extend(new_mutations)
                print(f"    Sharpe:{metrics['sharpe']:.2f} → 继续变异 ({len(new_mutations)} 个子代)")
            else:
                print(f"    Sharpe:{metrics['sharpe']:.2f} → {decision}")
        except ValueError:
            stats["improve_skipped"] += 1
            remaining_queue.remove(cand)
        except Exception:
            pass
        time.sleep(SIM_INTERVAL)

    ledger.save_improve_queue(remaining_queue)
    return stats


# ── Phase 2: exploit 桶 ──────────────────────────────────

def phase_exploit(api, ledger, mothers, fstats, slots, fpdb=None):
    """母因子迭代 + Settings 网格搜索"""
    stats = {"exploit_tried": 0, "exploit_submitted": 0}
    tried = ledger.tried_hashes()

    half = max(1, slots // 2)

    # 2a: 母因子变异
    for mother in mothers[:2]:
        expr = mother.get("expression", "")
        if not expr:
            continue
        family = identify_backbone(expr)
        if is_frozen(family, fstats):
            print(f"  [跳过] {family} 已冻结")
            continue

        print(f"\n[Phase 2a] 母因子 {mother.get('alpha_id', '')[:8]} Sharpe:{mother.get('sharpe', 0):.2f}")

        labels_for_mutation = []
        if mother.get("self_correlation", 0) > SELF_CORR_MAX:
            labels_for_mutation.append("too_correlated")
        if mother.get("sharpe", 0) < SHARPE_SUBMIT:
            labels_for_mutation.append("low_sharpe")
        if not labels_for_mutation:
            labels_for_mutation = ["too_correlated"]

        mutations = generate_improve_candidates(
            {"expr": expr, "alpha_id": mother.get("alpha_id"), "settings": {},
             "family": family, "generation": 0},
            labels_for_mutation, tried, max_children=half,
        )

        for cand in mutations:
            m_expr = cand["expr"]
            print(f"  [{cand['mutation']}] {m_expr[:55]}...")
            try:
                metrics, decision, labels = sim_one(
                    api, m_expr, cand.get("settings", {}), ledger,
                    tag=f"exploit:{cand['mutation']}", fpdb=fpdb,
                    extra={"parent_id": mother.get("alpha_id")},
                )
                stats["exploit_tried"] += 1
                if decision == "submit":
                    print(f"    [合格!] Sharpe:{metrics['sharpe']:.2f}")
                    try_submit(api, metrics, {"expr": m_expr, **metrics}, ledger)
                    stats["exploit_submitted"] += 1
                elif decision in ("improve", "grid_search"):
                    improve_queue = ledger.load_improve_queue()
                    new_cands = generate_improve_candidates(
                        {**cand, "alpha_id": metrics["alpha_id"], "sharpe": metrics["sharpe"]},
                        labels, tried, max_children=2)
                    improve_queue.extend(new_cands)
                    ledger.save_improve_queue(improve_queue)
                    print(f"    → {decision}, {len(new_cands)} 个变异加入 improve 队列")
                else:
                    print(f"    Sharpe:{metrics['sharpe']:.2f} → {decision}")
            except ValueError:
                pass
            except Exception:
                pass
            time.sleep(SIM_INTERVAL)

    # 2b: Settings 网格搜索
    submitted = ledger.load_submitted()
    grid_candidates = [r for r in submitted
                       if r.get("decision") in ("grid_search", "improve")
                       and _safe_float(r.get("sharpe")) >= 1.0
                       and not r.get("grid_done")]
    grid_candidates.sort(key=lambda x: _safe_float(x.get("sharpe")), reverse=True)

    for cand in grid_candidates[:1]:
        expr = cand["expr"]
        print(f"\n[Phase 2b] 网格搜索: {expr[:50]}... (原Sharpe:{cand.get('sharpe', 0):.2f})")
        for gs in SETTINGS_GRID:
            if gs == cand.get("settings"):
                continue
            print(f"    decay:{gs['decay']} neut:{gs['neutralization']}")
            try:
                metrics, decision, labels = sim_one(api, expr, gs, ledger, tag="grid")
                stats["exploit_tried"] += 1
                if decision == "submit":
                    print(f"      [合格!] Sharpe:{metrics['sharpe']:.2f} SC:{metrics['self_correlation']:.4f}")
                    try_submit(api, metrics, {"expr": expr, **metrics}, ledger)
                    stats["exploit_submitted"] += 1
                    break
                else:
                    print(f"      Sharpe:{metrics['sharpe']:.2f} SC:{metrics['self_correlation']:.4f} → {decision}")
            except Exception:
                pass
            time.sleep(SIM_INTERVAL)

    return stats


# ── Phase 3: explore 桶 ──────────────────────────────────

def phase_explore(api, ledger, pool, fields, fstats, slots, fpdb=None):
    """模板探索 + LLM 生成"""
    stats = {"explore_tried": 0, "explore_submitted": 0}
    tried = ledger.tried_hashes()

    # 3a: 从 field_scanner 结果动态生成
    field_scan = ledger.load_field_scan()
    good_fields = [r for r in field_scan.get("results", [])
                   if _safe_float(r.get("sharpe")) >= 0.8]

    dynamic_exprs = []
    for r in good_fields[:5]:
        field = r["field"]
        for combo in ["rank({f})", "rank(ts_delta({f}, 60))", "rank({f}) * rank(-ts_delta(close, 5))",
                       "group_rank(subindustry, {f})", "trade_when(rank({f}), volume > ts_mean(volume, 20))"]:
            e = combo.format(f=field)
            if _expr_hash(e) not in tried:
                dynamic_exprs.append((e, f"field_scan_{field}", {}))

    # 3b: 模板库
    template_batch = generate_batch(batch_size=max(2, slots - len(dynamic_exprs[:3])),
                                     submitted_exprs=ledger.submitted_exprs())

    all_explore = dynamic_exprs[:3] + template_batch

    # 3c: 排除冻结 family
    all_explore = [(e, f, s) for e, f, s in all_explore if not is_frozen(f.split("_")[0], fstats)]

    print(f"\n[Phase 3] Explore 桶 ({len(all_explore)} 个表达式, 分配 {slots} 槽)")

    for expr, family, settings in all_explore[:slots]:
        print(f"  [{family}] {expr[:55]}...")
        try:
            metrics, decision, labels = sim_one(api, expr, settings, ledger,
                                                 tag=f"explore:{family}", fpdb=fpdb)
            stats["explore_tried"] += 1
            if decision == "submit":
                print(f"    [合格!]")
                try_submit(api, metrics, {"expr": expr, **metrics}, ledger)
                stats["explore_submitted"] += 1
            elif decision in ("improve", "grid_search"):
                improve_queue = ledger.load_improve_queue()
                new_cands = generate_improve_candidates(
                    {"expr": expr, "alpha_id": metrics["alpha_id"], "settings": settings,
                     "family": family, "sharpe": metrics["sharpe"]},
                    labels, tried, max_children=2)
                improve_queue.extend(new_cands)
                ledger.save_improve_queue(improve_queue)
                print(f"    [有潜力] Sharpe:{metrics['sharpe']:.2f} → {len(new_cands)} 变异入队")
                stats["explore_submitted"] += 0
            else:
                print(f"    Sharpe:{metrics['sharpe']:.2f} → {decision}")
        except ValueError:
            pass
        except Exception:
            pass
        time.sleep(SIM_INTERVAL)

    return stats


# ── Phase 4: LLM 生成 ────────────────────────────────────

def phase_llm(api, ledger, pool, fields, fpdb=None, count=5):
    print(f"\n[Phase LLM] 生成 {count} 个新表达式...")
    prompt = build_llm_prompt(ledger, fields, pool, count=count)
    expressions = call_llm(prompt)

    if not expressions:
        print("  LLM 未返回有效表达式")
        return {"llm_tried": 0}

    stats = {"llm_tried": 0, "llm_submitted": 0}
    tried = ledger.tried_hashes()

    for expr, family, settings in expressions:
        if _expr_hash(expr) in tried:
            continue
        print(f"  [{family}] {expr[:55]}...")
        try:
            metrics, decision, labels = sim_one(api, expr, settings, ledger,
                                                 tag=f"llm:{family}", fpdb=fpdb)
            stats["llm_tried"] += 1
            if decision == "submit":
                print(f"    [合格!] Sharpe:{metrics['sharpe']:.2f}")
                try_submit(api, metrics, {"expr": expr, **metrics}, ledger)
                stats["llm_submitted"] += 1
            elif decision in ("improve", "grid_search"):
                improve_queue = ledger.load_improve_queue()
                new_cands = generate_improve_candidates(
                    {"expr": expr, "alpha_id": metrics["alpha_id"], "settings": settings,
                     "family": family, "sharpe": metrics["sharpe"]},
                    labels, tried, max_children=2)
                improve_queue.extend(new_cands)
                ledger.save_improve_queue(improve_queue)
            else:
                print(f"    Sharpe:{metrics['sharpe']:.2f} → {decision}")
        except Exception:
            pass
        time.sleep(SIM_INTERVAL)

    return stats


# ── Phase 5: 多区域 ──────────────────────────────────────

def phase_multiregion(api, ledger, max_candidates=1):
    submitted = ledger.load_submitted()
    good = [r for r in submitted
            if _safe_float(r.get("sharpe")) >= 1.0
            and _safe_float(r.get("fitness")) >= 0.8
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
                metrics, decision, labels = sim_one(api, expr, reg, ledger,
                                                     tag=f"region:{reg['region']}")
                stats["multiregion_tried"] += 1
                if decision == "submit":
                    print(f"    [合格!] {reg['region']} Sharpe:{metrics['sharpe']:.2f}")
                    try_submit(api, metrics, {"expr": expr, **metrics}, ledger)
                    stats["multiregion_submitted"] += 1
            except Exception:
                pass
            time.sleep(SIM_INTERVAL)

    return stats


# ── 主循环 ────────────────────────────────────────────────

def run_cycle(api, ledger, batch_size, cycle_num):
    cs = {"cycle": cycle_num, "start": utc_now_iso()}
    fpdb = FingerprintDB(ledger.ws)

    if cycle_num % 5 == 1:
        pool, fields, fstats = phase_scan(api, ledger)
    else:
        pool = ledger.load_pool()
        fields = ledger.load_fields()
        fstats = ledger.load_family_stats()

    improve_queue = ledger.load_improve_queue()
    plan = choose_bucket_plan(len(improve_queue), fstats)
    total_slots = batch_size
    print(f"\n[调度] 桶分配: {plan}  总槽位: {total_slots}")

    slot_improve = max(1, int(total_slots * plan["improve"] / sum(plan.values())))
    slot_exploit = max(1, int(total_slots * plan["exploit"] / sum(plan.values())))
    slot_explore = max(1, int(total_slots * plan["explore"] / sum(plan.values())))

    mothers = api.find_mother_candidates(pool, top_n=3) if pool.get("total") else []

    cs["improve"] = phase_improve(api, ledger, fstats, slot_improve, fpdb=fpdb)
    cs["exploit"] = phase_exploit(api, ledger, mothers, fstats, slot_exploit, fpdb=fpdb)
    cs["explore"] = phase_explore(api, ledger, pool, fields, fstats, slot_explore, fpdb=fpdb)

    if cycle_num % 3 == 0:
        cs["llm"] = phase_llm(api, ledger, pool, fields, fpdb=fpdb, count=5)

    if cycle_num % 4 == 0:
        cs["multiregion"] = phase_multiregion(api, ledger, max_candidates=1)

    cs["end"] = utc_now_iso()
    cs["plan"] = plan
    ledger.save_stats(cs)
    ledger.log({"event": "cycle_done", **cs})

    submitted = ledger.load_submitted()
    total = len(submitted)
    qualified = len(ledger._load_json("alphas_qualified.json", {"qualified": []}).get("qualified", []))
    rate = qualified / max(1, total) * 100
    print(f"\n[第 {cycle_num} 轮完成] 总仿真:{total} 合格:{qualified} 通过率:{rate:.2f}%")
    return cs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workspace", default=os.path.expanduser("~/.openclaw/workspace"))
    ap.add_argument("--batch-size", type=int, default=8)
    ap.add_argument("--interval", type=int, default=120)
    ap.add_argument("--max-cycles", type=int, default=0)
    args = ap.parse_args()

    ws = Path(args.workspace)
    ledger = Ledger(ws)

    print("=" * 60)
    print("Alpha 7x24 v4 — 统一架构")
    print("  improve/exploit/explore/retry 四桶调度")
    print("  智能变异 + family 冻结 + 指纹去重")
    print("=" * 60)

    api = BrainAPI()
    api.authenticate()
    print("认证成功\n")

    cycle, errors = 0, 0
    while True:
        cycle += 1
        if 0 < args.max_cycles < cycle:
            break
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
                try:
                    api.authenticate()
                except Exception:
                    pass
        time.sleep(args.interval)

    print(f"\n停止。共 {cycle} 轮。")


if __name__ == "__main__":
    main()
