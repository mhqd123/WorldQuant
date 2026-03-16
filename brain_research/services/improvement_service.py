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

    def mutate_expression(self, expression: str, action: str) -> str:
        expr = expression
        if action == 'increase_decay':
            expr = expr.replace('ts_mean(volume, 45)', 'ts_mean(volume, 55)')
            expr = expr.replace('ts_mean(high - low, 3)', 'ts_mean(high - low, 4)')
        elif action == 'change_neutralization':
            expr = expr.replace('subindustry', 'industry') if 'subindustry' in expr else expr.replace('industry', 'subindustry')
        elif action == 'change_signal_backbone':
            expr = expr.replace('(2 * close - high - low)', '(close - open)')
            expr = expr.replace('(1 + high - low)', '(1 + ts_mean(high - low, 3))')
        elif action == 'change_volume_confirm':
            expr = expr.replace('rank(volume / ts_mean(volume, 45))', 'rank(volume / ts_mean(volume, 55))')
            expr = expr.replace('rank(volume / ts_mean(volume, 40))', 'rank(volume / ts_mean(volume, 50))')
        elif action == 'add_smoothing':
            expr = expr.replace('(1 + high - low)', '(1 + ts_mean(high - low, 3))')
            expr = expr.replace('ts_mean(high - low, 3)', 'ts_mean(high - low, 4)')
        elif action == 'remove_complexity':
            expr = expr.replace(' / (1 + ts_std_dev(returns, 10))', '')
            expr = expr.replace(' / (1 + ts_std_dev(returns, 12))', '')
        elif action == 'change_horizon':
            expr = expr.replace('ts_mean(volume, 45)', 'ts_mean(volume, 60)')
            expr = expr.replace('ts_mean(high - low, 3)', 'ts_mean(high - low, 5)')
        return expr
