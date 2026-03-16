#!/usr/bin/env python3
import json, os, re, subprocess, sys, datetime
from pathlib import Path

WORKSPACE = Path('/root/.openclaw/workspace')
STATUS = WORKSPACE / 'monitor' / 'alpha-status.json'
ALPHAS = WORKSPACE / 'alphas.txt'
SCRIPT = Path('/root/.openclaw/skills/worldquant-brain/scripts/discover_and_submit.py')

ALPHA_RE = re.compile(r'Alpha ID: ([A-Za-z0-9]+)\s+Sharpe: ([^\s]+)\s+Fitness: ([^\s]+)')
EXPR_RE = re.compile(r'^\[(\d+)/(\d+)\]\s+(.*)\.\.\.$')


def now():
    return datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')


def load_status():
    if STATUS.exists():
        try:
            return json.loads(STATUS.read_text())
        except Exception:
            pass
    return {
        'status': 'idle',
        'updatedAt': None,
        'currentBatch': [],
        'best': None,
        'totals': {'tested': 0, 'batches': 0, 'qualified': 0},
        'recent': [],
        'notes': '',
        'families': {}
    }


def save_status(data):
    STATUS.parent.mkdir(parents=True, exist_ok=True)
    STATUS.write_text(json.dumps(data, ensure_ascii=False, indent=2))


def parse_float(x):
    try:
        return float(x)
    except Exception:
        return None


def main():
    exprs = [line.strip() for line in ALPHAS.read_text().splitlines() if line.strip() and not line.strip().startswith('#')]
    status = load_status()
    status['status'] = 'running'
    status['updatedAt'] = now()
    status['currentBatch'] = exprs
    status['totals']['batches'] = int(status.get('totals', {}).get('batches', 0)) + 1
    status['notes'] = 'Submitting current batch to BRAIN platform.'
    save_status(status)

    proc = subprocess.Popen(
        ['python3', str(SCRIPT), '--file', str(ALPHAS)],
        cwd=str(WORKSPACE), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1
    )

    current_expr = None
    for raw in proc.stdout:
        line = raw.rstrip('\n')
        m = EXPR_RE.match(line)
        if m:
            current_expr = m.group(3)
            status['updatedAt'] = now()
            status['notes'] = f'Testing expression {m.group(1)}/{m.group(2)}'
            save_status(status)
            print(line, flush=True)
            continue

        m = ALPHA_RE.search(line)
        if m:
            alpha_id, sharpe_s, fitness_s = m.groups()
            sharpe = parse_float(sharpe_s)
            fitness = parse_float(fitness_s)
            expr = current_expr or ''
            family = 'uncategorized'
            if 'ts_delta(close' in expr:
                family = 'delta-close'
            elif 'ts_corr(' in expr and 'volume' in expr:
                family = 'price-volume-corr'
            elif 'close - open' in expr:
                family = 'intraday'
            elif 'ts_mean(close' in expr:
                family = 'mean-close'
            item = {
                'alphaId': alpha_id,
                'sharpe': sharpe if sharpe is not None else sharpe_s,
                'fitness': fitness if fitness is not None else fitness_s,
                'expr': expr,
                'family': family
            }
            recent = status.get('recent', [])
            recent.insert(0, item)
            status['recent'] = recent[:20]
            status['totals']['tested'] = int(status.get('totals', {}).get('tested', 0)) + 1
            fams = status.setdefault('families', {})
            fams[item['family']] = int(fams.get(item['family'], 0)) + 1
            if sharpe is not None and fitness is not None and sharpe >= 1.25 and fitness >= 1.0:
                status['totals']['qualified'] = int(status.get('totals', {}).get('qualified', 0)) + 1
            best = status.get('best')
            score = (-1e9 if sharpe is None else sharpe) + (-1e9 if fitness is None else fitness)
            best_score = -1e18
            if isinstance(best, dict):
                bs = parse_float(best.get('sharpe'))
                bf = parse_float(best.get('fitness'))
                best_score = (-1e9 if bs is None else bs) + (-1e9 if bf is None else bf)
            if score > best_score:
                status['best'] = item
            status['updatedAt'] = now()
            status['notes'] = f'Latest result: {alpha_id} | Sharpe {item["sharpe"]} | Fitness {item["fitness"]}'
            save_status(status)
        print(line, flush=True)

    code = proc.wait()
    status['updatedAt'] = now()
    status['status'] = 'done' if code == 0 else 'failed'
    if code == 0:
        status['notes'] = 'Batch finished.'
    else:
        status['notes'] = f'Batch failed with code {code}.'
    save_status(status)
    sys.exit(code)


if __name__ == '__main__':
    main()
