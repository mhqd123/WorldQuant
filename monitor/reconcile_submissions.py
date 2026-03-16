#!/usr/bin/env python3
import json, os, base64, urllib.request, urllib.error, http.cookiejar
from pathlib import Path
from datetime import datetime

BASE = 'https://api.worldquantbrain.com'
WORKSPACE = Path('/root/.openclaw/workspace')
SUBMITTED = WORKSPACE / 'alphas_submitted.json'
LEDGER = WORKSPACE / 'alpha_ledger.jsonl'
STATUS_OUT = WORKSPACE / 'alpha_submission_status.json'


def utc_now_iso():
    return datetime.utcnow().isoformat() + 'Z'


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return default


def append_jsonl(path: Path, record: dict):
    with open(path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(record, ensure_ascii=False) + '\n')


def auth_opener():
    creds_file = os.path.expanduser('~/.brain_credentials')
    if not os.path.exists(creds_file):
        creds_file = os.path.expanduser('~/.openclaw/.brain_credentials')
    with open(creds_file, 'r', encoding='utf-8') as f:
        email, password = json.load(f)
    jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    cred = base64.b64encode(f'{email}:{password}'.encode()).decode()
    opener.open(urllib.request.Request(
        f'{BASE}/authentication', method='POST',
        headers={'Authorization': f'Basic {cred}', 'Content-Type': 'application/json'}, data=b'{}'
    )).read()
    return opener


def main():
    data = load_json(SUBMITTED, {'submitted': []})
    rows = [r for r in data.get('submitted', []) if isinstance(r, dict) and r.get('alpha_id')]
    latest = {}
    for r in rows:
        latest[r['alpha_id']] = r
    opener = auth_opener()
    status_rows = []
    for alpha_id, rec in latest.items():
        row = {
            'ts': utc_now_iso(),
            'alpha_id': alpha_id,
            'expr': rec.get('expr'),
            'family': rec.get('family'),
            'sim_sharpe': rec.get('sharpe'),
            'sim_fitness': rec.get('fitness'),
            'sim_turnover': rec.get('turnover'),
            'sim_drawdown': rec.get('drawdown'),
        }
        try:
            with opener.open(urllib.request.Request(f'{BASE}/alphas/{alpha_id}')) as r:
                payload = json.loads(r.read().decode())
            row['status'] = payload.get('status')
            row['dateSubmitted'] = payload.get('dateSubmitted')
            metrics = payload.get('is', {}) if isinstance(payload.get('is', {}), dict) else {}
            checks = metrics.get('checks', []) if isinstance(metrics.get('checks', []), list) else []
            for c in checks:
                if c.get('name') == 'SELF_CORRELATION':
                    row['selfCorrelationResult'] = c.get('result')
                    row['selfCorrelationValue'] = c.get('value')
                    break
        except urllib.error.HTTPError as e:
            row['error'] = f'HTTP {e.code}: {e.read().decode(errors="ignore")}'
        except Exception as e:
            row['error'] = str(e)
        status_rows.append(row)
        append_jsonl(LEDGER, {'kind': 'submission_status', **row})
    STATUS_OUT.write_text(json.dumps({'updatedAt': utc_now_iso(), 'rows': status_rows}, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({'updatedAt': utc_now_iso(), 'count': len(status_rows)}, ensure_ascii=False))


if __name__ == '__main__':
    main()
