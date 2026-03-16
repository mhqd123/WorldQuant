#!/usr/bin/env python3
import sys
from pathlib import Path

WORKSPACE = Path('/root/.openclaw/workspace')
if str(WORKSPACE) not in sys.path:
    sys.path.insert(0, str(WORKSPACE))

from brain_research.orchestrators.improve_consumer import ImproveConsumer
from brain_research.orchestrators.submission_status_writer import SubmissionStatusWriter

WORKSPACE = Path('/root/.openclaw/workspace')
DATA = WORKSPACE / 'brain_research_data'


def main():
    improve = ImproveConsumer(
        queue_store_path=str(DATA / 'improve_queue.jsonl'),
        candidate_store_path=str(DATA / 'candidates.jsonl'),
    )
    produced = improve.consume_once(limit=2)

    writer = SubmissionStatusWriter(
        status_json_path=str(WORKSPACE / 'alpha_submission_status.json'),
        out_store_path=str(DATA / 'submission_status.jsonl'),
    )
    synced = writer.sync()
    print({
        'improve_produced': len(produced),
        'submission_status_synced': len(synced),
    })


if __name__ == '__main__':
    main()
