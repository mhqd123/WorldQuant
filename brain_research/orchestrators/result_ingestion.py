from brain_research.adapters.discover_submit_adapter import build_candidate, build_metrics
from brain_research.diagnoser import diagnose_result
from brain_research.models import SimulationResult
from brain_research.services.lineage_service import LineageService
from brain_research.services.improvement_service import ImprovementService


class ResultIngestionOrchestrator:
    def __init__(self, candidate_store, result_store, lineage_store, pool_store, mutation_queue_store):
        self.candidate_store = candidate_store
        self.result_store = result_store
        self.lineage_store = lineage_store
        self.pool_store = pool_store
        self.mutation_queue_store = mutation_queue_store
        self.lineage_service = LineageService()
        self.improvement_service = ImprovementService()

    def ingest_simulation_result(self, raw_candidate: dict, raw_result: dict, source: str = 'discover_and_submit'):
        expression = raw_candidate['expression']
        candidate = build_candidate(
            expression=expression,
            raw_result=raw_result,
            parent_alpha_id=raw_candidate.get('parent_alpha_id'),
            mutation_type=raw_candidate.get('mutation_type'),
        )
        metrics = build_metrics(raw_result)
        labels, decision = diagnose_result(
            metrics=metrics,
            complexity_score=candidate.operator_profile.complexity_score,
            corr_to_pool=raw_result.get('corr_to_pool', 0.0) or 0.0,
        )
        result = SimulationResult(
            sim_id=f'sim_{candidate.alpha_id}',
            alpha_id=candidate.alpha_id,
            metrics=metrics,
            passed_internal_threshold=(decision == 'submit_pool'),
            stable_enough=True,
            too_correlated=('too_correlated' in labels),
            diagnosis_labels=labels,
            decision=decision,
            notes=[f'source:{source}'],
            source_bucket=candidate.source_bucket,
            priority_score=candidate.priority_score,
        )
        self.candidate_store.append(candidate.to_dict())
        self.result_store.append(result.to_dict())
        lineage = self.lineage_service.build_lineage(candidate.alpha_id, candidate.parent_alpha_id, candidate.mutation_type)
        self.lineage_store.append(lineage.to_dict())
        self.pool_store.append({
            'alpha_id': candidate.alpha_id,
            'decision': decision,
            'diagnosis_labels': labels,
            'family_id': candidate.family_id,
            'theme': candidate.theme,
            'subtheme': candidate.subtheme,
            'source_bucket': candidate.source_bucket,
            'priority_score': candidate.priority_score,
            'ts': raw_result.get('ts'),
        })
        improve_items = []
        if decision == 'improve_pool':
            improve_items = self.improvement_service.enqueue_mutations_for_improve(candidate, labels, self.mutation_queue_store)
        return {
            'candidate': candidate.to_dict(),
            'result': result.to_dict(),
            'decision': decision,
            'diagnosis_labels': labels,
            'improve_items': improve_items,
        }
