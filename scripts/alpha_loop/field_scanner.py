#!/usr/bin/env python3
"""
数据字段扫描器 — 系统性发现高价值数据字段。

流程：
  1. 调 API 获取所有可用字段
  2. 对每个字段测 rank(field)
  3. 对 Sharpe > 0.8 的字段两两组合
  4. 输出排名报告

用法:
  python field_scanner.py [--workspace /path] [--top 30] [--skip-pairs]
"""
import argparse, json, os, sys, time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from brain_api import BrainAPI, utc_now_iso, _safe_float

SKIP_FIELDS = {"date", "time", "ticker", "id", "cusip", "sedol", "isin", "currency", "exchange", "name", "siccode"}
SIM_WAIT = 8


def scan_fields(api, workspace):
    ws = Path(workspace)
    ws.mkdir(parents=True, exist_ok=True)
    report_file = ws / "field_scan_report.json"

    print("[1/3] 获取数据字段...")
    raw = api.get_data_fields()
    fields = []
    for f in raw:
        name = f.get("name") if isinstance(f, dict) else str(f)
        if name and name.lower() not in SKIP_FIELDS:
            fields.append(name)
    print(f"  共 {len(fields)} 个可用字段")

    (ws / "brain_data_fields.json").write_text(
        json.dumps({"fields": raw, "names": fields, "ts": utc_now_iso()}, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"\n[2/3] 逐字段测试 rank(field)...")
    results = []
    for i, name in enumerate(fields, 1):
        expr = f"rank({name})"
        print(f"  [{i}/{len(fields)}] {expr}", end="", flush=True)
        try:
            alpha = api.simulate_and_wait(expr, timeout=180)
            m = BrainAPI.extract_metrics(alpha)
            results.append({"field": name, "expr": expr, "sharpe": m["sharpe"], "fitness": m["fitness"],
                            "turnover": m["turnover"], "self_correlation": m["self_correlation"],
                            "alpha_id": m["alpha_id"]})
            print(f"  → Sharpe:{m['sharpe']:.2f} Fitness:{m['fitness']:.2f} SC:{m['self_correlation']:.2f}")
        except Exception as e:
            print(f"  → 失败: {str(e)[:40]}")
            results.append({"field": name, "expr": expr, "error": str(e)[:80]})
            if "429" in str(e): time.sleep(30)
        time.sleep(SIM_WAIT)

    results.sort(key=lambda x: _safe_float(x.get("sharpe")), reverse=True)
    report = {"phase": "single_field", "ts": utc_now_iso(), "total_fields": len(fields),
              "tested": len([r for r in results if "sharpe" in r]), "results": results}
    report_file.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    good = [r for r in results if _safe_float(r.get("sharpe")) >= 0.8]
    print(f"\n  Sharpe >= 0.8 的字段: {len(good)} 个")
    for r in good[:10]:
        print(f"    {r['field']}: Sharpe={r['sharpe']:.2f} Fitness={r['fitness']:.2f} SC={r['self_correlation']:.2f}")

    return results, good


def scan_pairs(api, workspace, good_fields, max_pairs=50):
    ws = Path(workspace)
    pair_file = ws / "field_pair_report.json"

    print(f"\n[3/3] 两两组合测试 (top {len(good_fields)} 字段)...")
    pairs = []
    names = [r["field"] for r in good_fields]
    tested = 0

    for i, a in enumerate(names):
        for b in names[i+1:]:
            if tested >= max_pairs:
                break
            expr = f"rank({a}) * rank({b})"
            print(f"  [{tested+1}] {expr}", end="", flush=True)
            try:
                alpha = api.simulate_and_wait(expr, timeout=180)
                m = BrainAPI.extract_metrics(alpha)
                pairs.append({"fields": [a, b], "expr": expr, "sharpe": m["sharpe"],
                              "fitness": m["fitness"], "self_correlation": m["self_correlation"],
                              "alpha_id": m["alpha_id"]})
                print(f"  → Sharpe:{m['sharpe']:.2f} SC:{m['self_correlation']:.2f}")
            except Exception as e:
                print(f"  → 失败: {str(e)[:40]}")
                if "429" in str(e): time.sleep(30)
            tested += 1
            time.sleep(SIM_WAIT)
        if tested >= max_pairs:
            break

    pairs.sort(key=lambda x: _safe_float(x.get("sharpe")), reverse=True)
    report = {"phase": "field_pairs", "ts": utc_now_iso(), "tested": tested, "results": pairs}
    pair_file.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"\n  Top 组合:")
    for r in pairs[:10]:
        print(f"    {r['expr'][:50]}: Sharpe={r['sharpe']:.2f} SC={r['self_correlation']:.2f}")

    return pairs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workspace", default=os.path.expanduser("~/.openclaw/workspace"))
    ap.add_argument("--top", type=int, default=30, help="两两组合时取 top N 字段")
    ap.add_argument("--skip-pairs", action="store_true")
    ap.add_argument("--max-pairs", type=int, default=50)
    args = ap.parse_args()

    api = BrainAPI()
    api.authenticate()
    print("认证成功\n")

    results, good = scan_fields(api, args.workspace)

    if not args.skip_pairs and good:
        scan_pairs(api, args.workspace, good[:args.top], max_pairs=args.max_pairs)

    print("\n扫描完成。报告已保存到:")
    print(f"  {args.workspace}/field_scan_report.json")
    print(f"  {args.workspace}/field_pair_report.json")


if __name__ == "__main__":
    main()
