import json
from pathlib import Path

from brain_research.storage import JsonlStore


class SubmissionStatusWriter:
    def __init__(self, status_json_path: str, out_store_path: str):
        self.status_json = Path(status_json_path)
        self.out_store = JsonlStore(out_store_path)

    def sync(self):
        if not self.status_json.exists():
            return []
        payload = json.loads(self.status_json.read_text(encoding='utf-8'))
        rows = payload.get('rows', [])
        written = []
        for row in rows:
            record = {
                'kind': 'submission_status',
                'alpha_id': row.get('alpha_id'),
                'status': row.get('status'),
                'dateSubmitted': row.get('dateSubmitted'),
                'selfCorrelationResult': row.get('selfCorrelationResult'),
                'selfCorrelationValue': row.get('selfCorrelationValue'),
                'sim_sharpe': row.get('sim_sharpe'),
                'sim_fitness': row.get('sim_fitness'),
                'family': row.get('family'),
                'expr': row.get('expr'),
                'ts': row.get('ts'),
            }
            self.out_store.append(record)
            written.append(record)
        return written
