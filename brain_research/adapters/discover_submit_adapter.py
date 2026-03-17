import hashlib
from typing import Optional, Dict, Any

from brain_research.models import (
    AlphaCandidate,
    CandidateSettings,
    OperatorProfile,
    SourceProfile,
    SimulationMetrics,
)


def _id(prefix: str, text: str) -> str:
    return f"{prefix}_{hashlib.sha1(text.encode('utf-8')).hexdigest()[:12]}"


def infer_family(expression: str) -> Dict[str, str]:
    e = expression.lower()
    if '(2 * close - high - low)' in e:
        return {'family_key': 'intraday_reversal_close_position', 'theme': 'intraday_reversal', 'subtheme': 'close_position'}
    if '(close - open)' in e and 'volume / ts_mean(volume' in e:
        return {'family_key': 'intraday_body_pressure_volume_confirmed', 'theme': 'intraday_reversal', 'subtheme': 'body_pressure'}
    if 'ts_delta(close' in e:
        return {'family_key': 'short_term_delta_reversal', 'theme': 'short_term_delta_mean_reversion', 'subtheme': 'delta_reversal'}
    if 'ts_mean(close' in e and 'close - ts_mean(close' in e:
        return {'family_key': 'short_term_mean_reversion', 'theme': 'short_term_delta_mean_reversion', 'subtheme': 'mean_reversion'}
    if 'high - low' in e or 'ts_mean(high - low' in e:
        if 'close - open' in e or 'open - low' in e:
            return {'family_key': 'intraday_range_volatility', 'theme': 'range_volatility', 'subtheme': 'intraday_range_pressure'}
        return {'family_key': 'range_volatility_generic', 'theme': 'range_volatility', 'subtheme': 'range_normalized_signal'}
    if 'volume / ts_mean(volume' in e or 'ts_mean(volume, 5) / ts_mean(volume' in e:
        return {'family_key': 'volume_confirmed_reversal', 'theme': 'volume_confirmed_reversal', 'subtheme': 'slow_volume_confirmation'}
    if 'group_rank' in e and ('subindustry' in e or 'industry' in e or 'sector' in e):
        return {'family_key': 'group_relative_signal', 'theme': 'group_relative', 'subtheme': 'grouped_variant'}
    if 'ts_corr(' in e and 'volume' in e:
        return {'family_key': 'price_volume_interaction_corr', 'theme': 'price_volume_interaction', 'subtheme': 'correlation_driven_signal'}
    return {'family_key': 'generic_alpha', 'theme': 'generic_alpha', 'subtheme': 'unclassified'}


def infer_family_id(expression: str) -> str:
    fam = infer_family(expression)
    return _id('fam', fam['family_key'])


def infer_hypothesis(expression: str) -> Dict[str, str]:
    fam = infer_family(expression)
    mapping = {
        'intraday_reversal_close_position': 'Use close location in daily range to capture intraday reversal pressure.',
        'intraday_body_pressure_volume_confirmed': 'Use candle body pressure with slow volume confirmation.',
        'short_term_delta_reversal': 'Exploit short horizon delta reversal under grouped ranking.',
        'short_term_mean_reversion': 'Exploit deviation from short moving average with grouped normalization.',
        'range_volatility_generic': 'Normalize signal by range-volatility structure to reduce noise.',
        'intraday_range_volatility': 'Use intraday structure under range-volatility normalization.',
        'volume_confirmed_reversal': 'Use slower volume confirmation as a reversal quality filter.',
        'group_relative_signal': 'Compare relative signal strength within grouped cross sections.',
        'price_volume_interaction_corr': 'Exploit breakdown in price-volume interaction.',
        'generic_alpha': 'Heuristically classified alpha candidate.',
    }
    return {
        'theme': fam['theme'],
        'subtheme': fam['subtheme'],
        'description': mapping.get(fam['family_key'], 'Heuristically classified alpha candidate.'),
    }


def infer_settings(expression: str, family: Dict[str, str]) -> CandidateSettings:
    e = expression.lower()
    delay = 1
    decay = 4
    truncation = 0.08
    neutralization = 'subindustry' if 'subindustry' in e else ('industry' if 'industry' in e else ('sector' if 'sector' in e else 'subindustry'))
    theme = family['theme']
    subtheme = family['subtheme']
    if theme == 'short_term_delta_mean_reversion':
        decay = 3
        delay = 1
        neutralization = 'subindustry' if neutralization == 'sector' else neutralization
    elif theme == 'volume_confirmed_reversal':
        decay = 6
        truncation = 0.06
    elif theme == 'group_relative':
        decay = 5
        neutralization = 'subindustry' if neutralization == 'none' else neutralization
    elif theme == 'price_volume_interaction':
        decay = 5
        truncation = 0.07
    elif theme == 'range_volatility':
        decay = 5
        truncation = 0.07
    elif theme == 'intraday_reversal' and subtheme == 'body_pressure':
        decay = 4
        truncation = 0.08
    if 'ts_mean(volume, 5) / ts_mean(volume' in e:
        decay = max(decay, 5)
    if 'ts_corr(' in e:
        delay = 1
        decay = max(decay, 6)
    return CandidateSettings(
        region='USA',
        universe='TOP3000',
        delay=delay,
        decay=decay,
        neutralization=neutralization,
        truncation=truncation,
    )


def build_candidate(expression: str, raw_result: Dict[str, Any], parent_alpha_id: Optional[str] = None, mutation_type: Optional[str] = None):
    alpha_id = raw_result.get('alpha_id') or _id('alp', expression)
    family = infer_family(expression)
    hypo = infer_hypothesis(expression)
    family_id = infer_family_id(expression)
    complexity = min(0.95, 0.08 * expression.count('ts_') + 0.06 * expression.count('rank(') + 0.05 * expression.count('group_'))
    settings = infer_settings(expression, family)
    op = OperatorProfile(
        ts_ops=expression.count('ts_'),
        cross_section_ops=expression.count('rank('),
        group_ops=expression.count('group_'),
        conditional_ops=expression.count('trade_when') + expression.count('?'),
        complexity_score=round(complexity, 4),
    )
    src = SourceProfile(
        uses_price=any(tok in expression.lower() for tok in ['close', 'open', 'high', 'low', 'returns']),
        uses_volume='volume' in expression.lower(),
        uses_fundamental=any(tok in expression.lower() for tok in ['cashflow', 'operating_income', 'cap', 'debt']),
    )
    return AlphaCandidate(
        alpha_id=alpha_id,
        hypothesis_id=_id('hyp', hypo['theme'] + '::' + hypo['subtheme']),
        family_id=family_id,
        theme=hypo['theme'],
        subtheme=hypo['subtheme'],
        parent_alpha_id=parent_alpha_id,
        generation=0 if not parent_alpha_id else int(raw_result.get('generation', 0) or 0) + 1,
        expression=expression,
        settings=settings,
        operator_profile=op,
        source_profile=src,
        mutation_type=mutation_type,
        novelty_score=float(raw_result.get('novelty_score', 0.5) or 0.5),
        redundancy_score=float(raw_result.get('redundancy_score', 0.0) or 0.0),
        prior_score=float(raw_result.get('prior_score', 0.0) or 0.0),
        status=raw_result.get('status', 'pending_sim'),
        tags=[hypo['theme'], hypo['subtheme'], family['family_key']],
        source_bucket=raw_result.get('source_bucket', 'explore'),
        priority_score=float(raw_result.get('priority_score', 0.0) or 0.0),
        mutation_expected_gain=float(raw_result.get('mutation_expected_gain', 0.0) or 0.0),
    )


def build_metrics(raw_result: Dict[str, Any]) -> SimulationMetrics:
    def f(key: str) -> float:
        val = raw_result.get(key)
        try:
            return float(val)
        except Exception:
            return 0.0
    return SimulationMetrics(
        sharpe=f('sharpe'),
        turnover=f('turnover'),
        fitness=f('fitness'),
        returns=f('returns'),
        drawdown=f('drawdown'),
        margin=f('margin'),
    )
