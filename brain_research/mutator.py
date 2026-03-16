PLAYBOOK = {
    "low_sharpe": [
        "add_smoothing",
        "change_signal_backbone",
        "change_neutralization",
    ],
    "low_fitness": [
        "increase_decay",
        "remove_complexity",
        "change_volume_confirm",
    ],
    "high_turnover": [
        "increase_decay",
        "change_horizon",
        "add_smoothing",
    ],
    "too_correlated": [
        "change_signal_backbone",
        "change_neutralization",
        "change_volume_confirm",
    ],
    "overcomplex": [
        "remove_complexity",
    ],
}


def propose_mutations(labels):
    actions = []
    for label in labels:
        actions.extend(PLAYBOOK.get(label, []))
    seen = set()
    uniq = []
    for action in actions:
        if action not in seen:
            seen.add(action)
            uniq.append(action)
    return uniq[:5]
