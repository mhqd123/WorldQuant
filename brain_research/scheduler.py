def family_priority_score(stats):
    return (
        0.35 * stats.get("recent_success_rate", 0.0)
        + 0.25 * stats.get("near_pass_density", 0.0)
        + 0.20 * stats.get("avg_mutation_gain", 0.0)
        + 0.20 * stats.get("novelty_budget_need", 0.0)
    )


def allocate_budget(total_budget, families):
    ranked = sorted(families, key=family_priority_score, reverse=True)
    allocation = {}
    if not ranked:
        return allocation

    total_score = sum(max(0.01, family_priority_score(f)) for f in ranked)
    for fam in ranked:
        score = max(0.01, family_priority_score(fam))
        allocation[fam["family_id"]] = int(total_budget * score / total_score)

    return allocation
