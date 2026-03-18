#!/usr/bin/env python3
import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone

WORKSPACE = Path('/root/.openclaw/workspace')
if str(WORKSPACE) not in sys.path:
    sys.path.insert(0, str(WORKSPACE))

from brain_research.orchestrators.improve_consumer import ImproveConsumer
from brain_research.orchestrators.submission_status_writer import SubmissionStatusWriter
from brain_research.storage import JsonlStore, JsonStore
from brain_research.scheduler import schedule_next_jobs, apply_family_freeze_policy

DATA = WORKSPACE / 'brain_research_data'
FAMILY_STATS_FILE = DATA / 'family_stats.json'
MAIN_QUEUE_FILE = WORKSPACE / 'alphas.txt'
SCHEDULED_QUEUE_FILE = DATA / 'scheduled_jobs.jsonl'


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def sync_submission_status():
    writer = SubmissionStatusWriter(
        status_json_path=str(WORKSPACE / 'alpha_submission_status.json'),
        out_store_path=str(DATA / 'submission_status.jsonl'),
    )
    return writer.sync()


def drain_improve_queue_into_main_pipeline(limit: int = 2):
    improve = ImproveConsumer(
        queue_store_path=str(DATA / 'improve_queue.jsonl'),
        candidate_store_path=str(DATA / 'candidates.jsonl'),
    )
    return improve.consume_once(limit=limit)


def build_family_stats(candidates, results, submission_rows):
    stats = defaultdict(lambda: {
        'attempts': 0,
        'near_pass_count': 0,
        'submit_count': 0,
        'platform_active_count': 0,
        'mutation_gain_sum': 0.0,
        'mutation_gain_count': 0,
        'recent_success_rate': 0.0,
        'near_pass_density': 0.0,
        'novelty_budget_need': 0.2,
        'consecutive_no_near_pass': 0,
        'consecutive_no_submit_candidate': 0,
        'consecutive_no_gain_mutations': 0,
    })

    sorted_results = sorted(results, key=lambda r: r.get('ts') or r.get('sim_id') or '', reverse=False)
    for row in sorted_results:
        family_id = row.get('family_id') or 'unknown'
        s = stats[family_id]
        s['attempts'] += 1
        labels = row.get('diagnosis_labels', [])
        if 'near_pass' in labels or 'promising_but_needs_refinement' in labels:
            s['near_pass_count'] += 1
            s['consecutive_no_near_pass'] = 0
        else:
            s['consecutive_no_near_pass'] += 1
        if row.get('decision') == 'submit_pool':
            s['submit_count'] += 1
            s['consecutive_no_submit_candidate'] = 0
        else:
            s['consecutive_no_submit_candidate'] += 1
        gain = float(row.get('priority_score', 0.0) or 0.0)
        s['mutation_gain_sum'] += gain
        s['mutation_gain_count'] += 1

    for row in submission_rows:
        family_id = row.get('family_id') or row.get('family') or 'unknown'
        s = stats[family_id]
        if row.get('status') == 'ACTIVE' or row.get('dateSubmitted'):
            s['platform_active_count'] += 1

    for family_id, s in stats.items():
        attempts = max(1, s['attempts'])
        s['near_pass_density'] = s['near_pass_count'] / attempts
        total_submit = s['submit_count'] + s['platform_active_count']
        s['recent_success_rate'] = total_submit / attempts
        s['avg_mutation_gain'] = s['mutation_gain_sum'] / max(1, s['mutation_gain_count'])
        if s['consecutive_no_gain_mutations'] >= 2:
            s['branch_stop_reason'] = '2_mutations_without_gain'
    return dict(stats)


def persist_family_stats(family_stats):
    frozen = apply_family_freeze_policy(family_stats)
    JsonStore(str(FAMILY_STATS_FILE)).save({'updatedAt': utc_now_iso(), 'families': frozen})
    return frozen


def load_family_stats():
    payload = JsonStore(str(FAMILY_STATS_FILE)).load({'families': {}})
    return payload.get('families', {})


def load_active_jobs(candidates):
    return [c for c in candidates if c.get('status') in {'scheduled', 'running'}]


def load_queue_state(candidates):
    return {
        'improve': [c for c in candidates if c.get('status') == 'pending_sim' and c.get('source_bucket') == 'improve'],
        'exploit': [c for c in candidates if c.get('status') == 'pending_sim' and c.get('source_bucket') == 'exploit'],
        'explore': [c for c in candidates if c.get('status') == 'pending_sim' and c.get('source_bucket') not in {'improve', 'exploit', 'retry'}],
        'retry': [c for c in candidates if c.get('status') == 'pending_retry'],
    }


def write_main_pending_queue(scheduled_jobs):
    MAIN_QUEUE_FILE.write_text('\n'.join(job['expression'] for job in scheduled_jobs if job.get('expression')) + ('\n' if scheduled_jobs else ''), encoding='utf-8')
    scheduled_store = JsonlStore(str(SCHEDULED_QUEUE_FILE))
    for job in scheduled_jobs:
        scheduled_store.append({**job, 'scheduledAt': utc_now_iso()})


def run_bridge_once(max_concurrency: int = 5):
    candidate_store = JsonlStore(str(DATA / 'candidates.jsonl'))
    result_store = JsonlStore(str(DATA / 'results.jsonl'))
    submission_store = JsonlStore(str(DATA / 'submission_status.jsonl'))

    candidates = candidate_store.load_all()
    results = result_store.load_all()
    submission_rows = submission_store.load_all()

    family_stats = build_family_stats(candidates, results, submission_rows)
    family_stats = persist_family_stats(family_stats)

    improve_produced = drain_improve_queue_into_main_pipeline(limit=2)
    candidates = candidate_store.load_all()

    queue_state = load_queue_state(candidates)
    active_jobs = load_active_jobs(candidates)
    scheduled = schedule_next_jobs(queue_state=queue_state, active_jobs=active_jobs, family_stats=family_stats, max_concurrency=max_concurrency)

    scheduled_keys = {(x.get('parent_alpha_id'), x.get('expression'), x.get('source_bucket')) for x in scheduled}
    candidate_store.update_rows(
        lambda row: (row.get('parent_alpha_id'), row.get('expression'), row.get('source_bucket')) in scheduled_keys and row.get('status') == 'pending_sim',
        lambda row: {**row, 'status': 'scheduled', 'scheduled_by_bridge': True, 'scheduled_at': utc_now_iso()}
    )

    write_main_pending_queue(scheduled)
    synced = sync_submission_status()
    return {
        'scheduled_jobs': len(scheduled),
        'improve_produced': len(improve_produced),
        'submission_status_synced': len(synced),
        'main_queue_file': str(MAIN_QUEUE_FILE),
    }


def main():
    print(run_bridge_once())


if __name__ == '__main__':
    main()
