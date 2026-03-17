#!/usr/bin/env python3
import argparse
import json
import hashlib
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

WORKSPACE = Path('/root/.openclaw/workspace')
DATA = WORKSPACE / 'brain_research_data'
OUT = DATA / 'improve_funnel_report.json'


def utc_now():
    return datetime.now(timezone.utc)


def parse_ts(value):
    if not value:
        return None
    text = str(value).strip()
    for candidate in [text.replace('Z', '+00:00'), text]:
        try:
            dt = datetime.fromisoformat(candidate)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            pass
    for fmt in ['%Y-%m-%d %H:%M:%S UTC', '%Y-%m-%d %H:%M UTC']:
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            pass
    return None


def load_jsonl(path):
    if not path.exists():
        return []
    rows = []
    with path.open('r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    return rows


def load_json(path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return default


def in_window(row, cutoff):
    ts = parse_ts(row.get('ts') or row.get('scheduled_at') or row.get('scheduledAt') or row.get('timestamp'))
    if ts is None:
        return False
    return ts >= cutoff


def safe_ratio(a, b):
    return round(a / b, 4) if b else 0.0


def normalize_expression(expr):
    if not expr:
        return None
    return ' '.join(str(expr).strip().split())


def expression_hash(expr):
    norm = normalize_expression(expr)
    if not norm:
        return None
    return hashlib.sha1(norm.encode('utf-8')).hexdigest()


def build_match_keys(row, expr_lookup=None):
    keys = []
    alpha_id = row.get('alpha_id')
    if alpha_id:
        keys.append(('alpha_id', alpha_id))
    parent_alpha_id = row.get('parent_alpha_id')
    generation = row.get('generation')
    if parent_alpha_id and generation is not None:
        keys.append(('parent_generation', f'{parent_alpha_id}:{generation}'))
    expr = row.get('expression') or row.get('expr')
    if not expr and expr_lookup and alpha_id:
        expr = expr_lookup.get(alpha_id)
    expr_h = row.get('expr_hash') or row.get('expression_hash') or expression_hash(expr)
    if expr_h:
        keys.append(('expression_hash', str(expr_h)))
    norm = normalize_expression(expr)
    if norm:
        keys.append(('expression', norm))
    return keys


def get_metrics(row):
    if 'metrics' in row and isinstance(row['metrics'], dict):
        m = row['metrics']
        return float(m.get('sharpe') or 0), float(m.get('fitness') or 0), float(m.get('turnover') or 0)
    return float(row.get('sharpe') or 0), float(row.get('fitness') or 0), float(row.get('turnover') or 0)


def classify_delta(parent, child):
    p_sh, p_fit, p_to = parent
    c_sh, c_fit, c_to = child
    d_sh = c_sh - p_sh
    d_fit = c_fit - p_fit
    d_to = c_to - p_to
    if (d_fit >= 0.05) or (d_sh >= 0.05 and d_to <= 0.03):
        label = 'improved'
    elif abs(d_sh) <= 0.03 and abs(d_fit) <= 0.03 and abs(d_to) <= 0.03:
        label = 'flat'
    else:
        label = 'worse'
    return label, d_sh, d_fit, d_to


def assess(funnel, mutation_stats, improved_count, flat_count, worse_count):
    if funnel['improve_decision_count'] > 0 and funnel['mutation_generated_count'] == 0:
        return 'decision_generation'
    if funnel['mutation_generated_count'] > 0 and funnel['queued_improve_count'] == 0:
        return 'queue_transition'
    if funnel['pending_sim_from_improve_count'] > 0 and funnel['scheduled_from_improve_count'] == 0:
        return 'scheduler_transition'
    if funnel['returned_from_improve_count'] > 0 and worse_count >= improved_count:
        return 'return_quality'
    if sum(funnel.values()) == 0:
        return 'insufficient_data'
    return 'mixed'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--hours', type=int, default=48)
    args = ap.parse_args()
    cutoff = utc_now() - timedelta(hours=args.hours)

    paths = {
        'candidates.jsonl': DATA / 'candidates.jsonl',
        'results.jsonl': DATA / 'results.jsonl',
        'improve_queue.jsonl': DATA / 'improve_queue.jsonl',
        'result_writeback_trace.jsonl': DATA / 'result_writeback_trace.jsonl',
        'scheduled_jobs.jsonl': DATA / 'scheduled_jobs.jsonl',
        'family_stats.json': DATA / 'family_stats.json',
    }

    candidates = load_jsonl(paths['candidates.jsonl'])
    results = load_jsonl(paths['results.jsonl'])
    improve_queue = load_jsonl(paths['improve_queue.jsonl'])
    trace_rows = load_jsonl(paths['result_writeback_trace.jsonl'])
    scheduled_rows = load_jsonl(paths['scheduled_jobs.jsonl'])
    family_stats = load_json(paths['family_stats.json'], {'families': {}})

    candidates_48 = [r for r in candidates if in_window(r, cutoff)]
    results_48 = [r for r in results if in_window(r, cutoff)]
    improve_48 = [r for r in improve_queue if in_window(r, cutoff)]
    trace_48 = [r for r in trace_rows if in_window(r, cutoff)]
    scheduled_48 = [r for r in scheduled_rows if in_window(r, cutoff)]

    data_sources = {name: path.exists() for name, path in paths.items()}

    improve_decisions = [r for r in results_48 if r.get('decision') == 'improve_pool' or 'near_pass' in (r.get('diagnosis_labels') or []) or 'promising_but_needs_refinement' in (r.get('diagnosis_labels') or [])]
    mutation_generated = [r for r in improve_48 if r.get('mutation_type')]
    queued_improve = [r for r in improve_48 if r.get('status') in {'queued_improve', 'consumed'}]
    pending_improve = [r for r in candidates_48 if r.get('source_bucket') == 'improve' and r.get('status') == 'pending_sim']
    scheduled_improve = [r for r in candidates_48 if r.get('source_bucket') == 'improve' and r.get('status') == 'scheduled']

    candidate_expr_by_alpha = {}
    for row in candidates:
        if row.get('alpha_id') and row.get('expression'):
            candidate_expr_by_alpha[row['alpha_id']] = row['expression']

    scheduled_improve_keys = set()
    for row in scheduled_improve:
        for key in build_match_keys(row):
            scheduled_improve_keys.add(key)
    returned_improve = []
    for row in results_48:
        if row.get('source_bucket') == 'improve' or any(k in scheduled_improve_keys for k in build_match_keys(row, candidate_expr_by_alpha)):
            returned_improve.append(row)

    funnel = {
        'improve_decision_count': len(improve_decisions),
        'mutation_generated_count': len(mutation_generated),
        'queued_improve_count': len(queued_improve),
        'pending_sim_from_improve_count': len(pending_improve),
        'scheduled_from_improve_count': len(scheduled_improve),
        'returned_from_improve_count': len(returned_improve),
        'decision_to_mutation_rate': safe_ratio(len(mutation_generated), len(improve_decisions)),
        'mutation_to_queue_rate': safe_ratio(len(queued_improve), len(mutation_generated)),
        'queue_to_pending_rate': safe_ratio(len(pending_improve), len(queued_improve)),
        'pending_to_scheduled_rate': safe_ratio(len(scheduled_improve), len(pending_improve)),
        'scheduled_to_return_rate': safe_ratio(len(returned_improve), len(scheduled_improve)),
    }

    parent_result_by_alpha = {r.get('alpha_id'): r for r in results if r.get('alpha_id')}
    parent_child_rows = []
    improved_count = flat_count = worse_count = 0
    mutation_stats = defaultdict(lambda: {'generated': 0, 'scheduled': 0, 'returned': 0, 'improved': 0, 'worse': 0})

    for row in mutation_generated:
        mutation_stats[row.get('mutation_type', 'unknown')]['generated'] += 1
    for row in scheduled_improve:
        mutation_stats[row.get('mutation_type', 'unknown')]['scheduled'] += 1

    for child in returned_improve:
        child_alpha = child.get('alpha_id')
        matching_candidate = None
        for c in candidates:
            if c.get('alpha_id') == child_alpha:
                matching_candidate = c
                break
        parent_alpha = (matching_candidate or {}).get('parent_alpha_id')
        parent = parent_result_by_alpha.get(parent_alpha) if parent_alpha else None
        child_metrics = get_metrics(child)
        parent_metrics = get_metrics(parent) if parent else (0.0, 0.0, 0.0)
        label, d_sh, d_fit, d_to = classify_delta(parent_metrics, child_metrics)
        if label == 'improved':
            improved_count += 1
        elif label == 'flat':
            flat_count += 1
        else:
            worse_count += 1
        mutation_type = (matching_candidate or {}).get('mutation_type', 'unknown')
        mutation_stats[mutation_type]['returned'] += 1
        if label == 'improved':
            mutation_stats[mutation_type]['improved'] += 1
        if label == 'worse':
            mutation_stats[mutation_type]['worse'] += 1
        parent_child_rows.append({
            'parent_alpha_id': parent_alpha,
            'child_alpha_id': child_alpha,
            'mutation_type': mutation_type,
            'parent_sharpe': parent_metrics[0],
            'parent_fitness': parent_metrics[1],
            'parent_turnover': parent_metrics[2],
            'child_sharpe': child_metrics[0],
            'child_fitness': child_metrics[1],
            'child_turnover': child_metrics[2],
            'delta_sharpe': round(d_sh, 4),
            'delta_fitness': round(d_fit, 4),
            'delta_turnover': round(d_to, 4),
            'assessment': label,
        })

    bottleneck = assess(funnel, mutation_stats, improved_count, flat_count, worse_count)

    notes = []
    for name, exists in data_sources.items():
        if not exists:
            notes.append(f'missing data source: {name}')

    report = {
        'window_hours': args.hours,
        'generated_at': utc_now().isoformat(),
        'data_sources': data_sources,
        'improve_funnel': funnel,
        'parent_child_comparison': {
            'improved_count': improved_count,
            'flat_count': flat_count,
            'worse_count': worse_count,
            'rows': parent_child_rows,
        },
        'mutation_type_distribution': dict(mutation_stats),
        'funnel_breakpoint_assessment': bottleneck,
        'notes': notes,
    }

    OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')

    print('=== brain_research improve funnel report ===')
    print(f'window_hours: {args.hours}')
    print(f'generated_at: {report["generated_at"]}')
    print('improve_funnel:')
    for k, v in funnel.items():
        print(f'  - {k}: {v}')
    print('parent_child_comparison:')
    print(f'  - improved_count: {improved_count}')
    print(f'  - flat_count: {flat_count}')
    print(f'  - worse_count: {worse_count}')
    print('mutation_type_distribution:')
    for mt, stats in mutation_stats.items():
        print(f"  - {mt}: generated={stats['generated']} scheduled={stats['scheduled']} returned={stats['returned']} improved={stats['improved']} worse={stats['worse']}")
    print(f'funnel_breakpoint_assessment: {bottleneck}')
    if notes:
        print('notes:')
        for note in notes:
            print(f'  - {note}')


if __name__ == '__main__':
    main()
