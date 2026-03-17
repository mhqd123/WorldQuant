from brain_research.storage import JsonlStore
from brain_research.services.improvement_service import ImprovementService


class ImproveConsumer:
    def __init__(self, queue_store_path: str, candidate_store_path: str):
        self.queue_store = JsonlStore(queue_store_path)
        self.candidate_store = JsonlStore(candidate_store_path)
        self.improvement_service = ImprovementService()

    def consume_once(self, limit: int = 2):
        items = self.queue_store.load_all()
        queued = [x for x in items if x.get('status') == 'queued_improve']
        produced = []
        for item in queued[:limit]:
            generated = self.improvement_service.generate_mutation_candidates(item, [item.get('reason', '')], max_children=1)
            for out in generated:
                out.update({
                    'status': 'pending_sim',
                    'source': 'improve_consumer',
                    'source_bucket': 'improve',
                })
                self.candidate_store.append(out)
                produced.append(out)
        consumed_keys = {(x.get('parent_alpha_id'), x.get('mutation_type'), x.get('expression')) for x in queued[:limit]}
        self.queue_store.update_rows(
            lambda row: (row.get('parent_alpha_id'), row.get('mutation_type'), row.get('expression')) in consumed_keys,
            lambda row: {**row, 'status': 'consumed'}
        )
        return produced
