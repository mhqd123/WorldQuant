from .models import SimulationResult, AlphaLineage
from .diagnoser import diagnose_result
from .mutator import propose_mutations
from .scheduler import schedule_next_jobs, priority_score


def cheap_filter_score(candidate):
    score = 0.0
    score += 0.25
    complexity = candidate.operator_profile.complexity_score
    score += max(0, 0.20 * (1 - complexity))
    score += 0.20 * candidate.novelty_score
    score += 0.15
    if complexity < 0.5:
        score += 0.10
    score -= 0.10 * candidate.redundancy_score
    if candidate.settings.decay <= 2:
        score -= 0.10
    return round(score, 4)


def research_cycle(
    hypotheses,
    candidate_store,
    result_store,
    lineage_store,
    total_budget,
    expand_hypothesis_to_family,
    run_simulations,
    mutate_candidate,
    family_stats=None,
):
    family_stats = family_stats or {}
    candidates = []
    for hyp in hypotheses:
        family_candidates = expand_hypothesis_to_family(hyp)
        for candidate in family_candidates:
            candidate.prior_score = cheap_filter_score(candidate)
            candidate.priority_score = priority_score(candidate.to_dict(), family_stats)
            candidate.source_bucket = 'explore'
        candidates.extend(family_candidates)

    candidates = [c for c in candidates if c.prior_score >= 0.35]
    queue_state = {
        'explore': [c.to_dict() for c in candidates],
        'exploit': [],
        'improve': [],
        'retry': [],
    }
    scheduled = schedule_next_jobs(queue_state=queue_state, active_jobs=[], family_stats=family_stats, max_concurrency=min(5, total_budget))
    chosen_ids = {x.get('alpha_id') for x in scheduled}
    sim_queue = [c for c in candidates if c.alpha_id in chosen_ids]
    results = run_simulations(sim_queue)

    for candidate, metrics in results:
        labels, decision = diagnose_result(
            metrics=metrics,
            complexity_score=candidate.operator_profile.complexity_score,
            corr_to_pool=0.0,
        )

        result = SimulationResult(
            sim_id=f"sim_{candidate.alpha_id}",
            alpha_id=candidate.alpha_id,
            metrics=metrics,
            passed_internal_threshold=(decision == "submit_pool"),
            stable_enough=True,
            too_correlated=("too_correlated" in labels),
            diagnosis_labels=labels,
            decision=decision,
            source_bucket=candidate.source_bucket,
            priority_score=candidate.priority_score,
        )
        result_store.append(result.to_dict())

        if candidate.parent_alpha_id:
            lineage = AlphaLineage(
                lineage_id=f"lin_{candidate.alpha_id}",
                alpha_id=candidate.alpha_id,
                ancestor_chain=[candidate.parent_alpha_id, candidate.alpha_id],
                mutations=[candidate.mutation_type] if candidate.mutation_type else [],
            )
            lineage_store.append(lineage.to_dict())

        if decision == "improve_pool":
            actions = propose_mutations(labels)
            mutated = mutate_candidate(candidate, actions)
            for m in mutated[:3]:
                candidate_store.append(m.to_dict())
