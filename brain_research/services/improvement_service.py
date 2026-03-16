from brain_research.mutator import propose_mutations


class ImprovementService:
    def enqueue_mutations_for_improve(self, candidate, diagnosis_labels, candidate_store):
        actions = propose_mutations(diagnosis_labels)
        queue_items = []
        for i, action in enumerate(actions[:2], start=1):
            queue_items.append({
                'parent_alpha_id': candidate.alpha_id,
                'family_id': candidate.family_id,
                'generation': candidate.generation + 1,
                'mutation_type': action,
                'expression': candidate.expression,
                'status': 'queued_improve',
                'rank': i,
            })
        for item in queue_items:
            candidate_store.append(item)
        return queue_items
