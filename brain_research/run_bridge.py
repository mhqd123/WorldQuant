#!/usr/bin/env python3
import sys
from pathlib import Path

WORKSPACE = Path('/root/.openclaw/workspace')
if str(WORKSPACE) not in sys.path:
    sys.path.insert(0, str(WORKSPACE))

from brain_research.orchestrators.improve_consumer import ImproveConsumer
from brain_research.orchestrators.submission_status_writer import SubmissionStatusWriter
from brain_research.storage import JsonlStore
from brain_research.scheduler import schedule_next_jobs

DATA = WORKSPACE / 'brain_research_data'


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


def run_bridge_once(max_concurrency: int = 5):
    candidate_store = JsonlStore(str(DATA / 'candidates.jsonl'))
    candidates = candidate_store.load_all()
    queue_state = {
        'improve': [c for c in candidates if c.get('status') == 'pending_sim' and c.get('source_bucket') == 'improve'],
        'exploit': [c for c in candidates if c.get('status') == 'pending_sim' and c.get('source_bucket') == 'exploit'],
        'explore': [c for c in candidates if c.get('status') == 'pending_sim' and c.get('source_bucket') not in {'improve', 'exploit', 'retry'}],
        'retry': [c for c in candidates if c.get('status') == 'pending_retry'],
    }
    scheduled = schedule_next_jobs(queue_state=queue_state, active_jobs=[], family_stats={}, max_concurrency=max_concurrency)
    scheduled_keys = {(x.get('parent_alpha_id'), x.get('expression'), x.get('source_bucket')) for x in scheduled}
    candidate_store.update_rows(
        lambda row: (row.get('parent_alpha_id'), row.get('expression'), row.get('source_bucket')) in scheduled_keys and row.get('status') == 'pending_sim',
        lambda row: {**row, 'status': 'scheduled', 'scheduled_by_bridge': True}
    )
    improve_produced = drain_improve_queue_into_main_pipeline(limit=2)
    synced = sync_submission_status()
    return {
        'scheduled_jobs': len(scheduled),
        'improve_produced': len(improve_produced),
        'submission_status_synced': len(synced),
    }


def main():
    print(run_bridge_once())


if __name__ == '__main__':
    main()
