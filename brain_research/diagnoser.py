from .enums import FailureLabel, Decision


SHARPE_MIN = 1.25
FITNESS_MIN = 1.0
TURNOVER_MAX = 0.70
DRAWDOWN_MAX = 0.10
NEAR_PASS_MARGIN = 0.10


def diagnose_result(metrics, complexity_score=0.0, corr_to_pool=0.0):
    labels = []

    if metrics.sharpe < SHARPE_MIN:
        labels.append(FailureLabel.LOW_SHARPE.value)

    if metrics.fitness < FITNESS_MIN:
        labels.append(FailureLabel.LOW_FITNESS.value)

    if metrics.turnover > TURNOVER_MAX:
        labels.append(FailureLabel.HIGH_TURNOVER.value)

    if metrics.drawdown > DRAWDOWN_MAX:
        labels.append(FailureLabel.HIGH_DRAWDOWN.value)

    if complexity_score > 0.75:
        labels.append(FailureLabel.OVERCOMPLEX.value)

    if corr_to_pool > 0.70:
        labels.append(FailureLabel.TOO_CORRELATED.value)

    if not labels:
        labels.append(FailureLabel.PASS.value)

    near_pass = (
        metrics.sharpe >= SHARPE_MIN - NEAR_PASS_MARGIN
        and metrics.fitness >= FITNESS_MIN - NEAR_PASS_MARGIN
        and metrics.turnover <= TURNOVER_MAX
        and metrics.drawdown <= DRAWDOWN_MAX
    )

    if FailureLabel.PASS.value in labels:
        decision = Decision.SUBMIT_POOL.value
    elif near_pass:
        labels.append(FailureLabel.NEAR_PASS.value)
        labels.append(FailureLabel.PROMISING_BUT_NEEDS_REFINEMENT.value)
        decision = Decision.IMPROVE_POOL.value
    else:
        decision = Decision.REJECT_POOL.value

    return labels, decision
