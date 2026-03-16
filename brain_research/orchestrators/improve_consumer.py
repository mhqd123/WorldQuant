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
            new_expr = self.improvement_service.mutate_expression(item['expression'], item['mutation_type'])
            out = {
                'parent_alpha_id': item.get('parent_alpha_id'),
                'family_id': item.get('family_id'),
                'generation': item.get('generation', 1),
                'mutation_type': item.get('mutation_type'),
                'expression': new_expr,
                'status': 'pending_sim',
                'source': 'improve_consumer',
            }
            self.candidate_store.append(out)
            produced.append(out)
            item['status'] = 'consumed'
        return produced
