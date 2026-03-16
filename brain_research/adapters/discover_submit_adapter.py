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


def infer_family_id(expression: str) -> str:
    e = expression.lower()
    tags = []
    if '2 * close - high - low' in e:
        tags.append('intraday_position')
    if 'ts_delta(close' in e:
        tags.append('delta_close')
    if 'ts_mean(close' in e:
        tags.append('mean_close')
    if 'ts_corr(' in e and 'volume' in e:
        tags.append('price_volume_corr')
    if 'volume / ts_mean(volume' in e:
        tags.append('volume_confirm')
    if 'subindustry' in e:
        tags.append('subindustry')
    elif 'industry' in e:
        tags.append('industry')
    family = '__'.join(tags) if tags else 'generic'
    return _id('fam', family)


def infer_hypothesis(expression: str) -> Dict[str, str]:
    e = expression.lower()
    if '2 * close - high - low' in e:
        return {
            'theme': 'intraday_reversal',
            'subtheme': 'close_position_with_volume_confirmation',
            'description': 'Use close location inside daily range with relative volume confirmation.',
        }
    if 'ts_corr(' in e and 'volume' in e:
        return {
            'theme': 'price_volume_interaction',
            'subtheme': 'price_volume_correlation_break',
            'description': 'Exploit instability of price-volume relationship.',
        }
    if 'ts_delta(close' in e or 'ts_mean(close' in e:
        return {
            'theme': 'short_term_reversal',
            'subtheme': 'smoothed_price_reversal',
            'description': 'Short-term price reversal with stabilization and confirmation.',
        }
    return {
        'theme': 'generic_alpha',
        'subtheme': 'unclassified',
        'description': 'Heuristically classified alpha candidate.',
    }


def build_candidate(expression: str, raw_result: Dict[str, Any], parent_alpha_id: Optional[str] = None, mutation_type: Optional[str] = None):
    alpha_id = raw_result.get('alpha_id') or _id('alp', expression)
    hypo = infer_hypothesis(expression)
    family_id = infer_family_id(expression)
    complexity = min(0.95, 0.08 * expression.count('ts_') + 0.06 * expression.count('rank(') + 0.05 * expression.count('group_'))
    settings = CandidateSettings(
        region='USA', universe='TOP3000', delay=1, decay=4,
        neutralization='subindustry' if 'subindustry' in expression.lower() else ('industry' if 'industry' in expression.lower() else 'none'),
        truncation=0.08,
    )
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
        parent_alpha_id=parent_alpha_id,
        generation=0 if not parent_alpha_id else 1,
        expression=expression,
        settings=settings,
        operator_profile=op,
        source_profile=src,
        mutation_type=mutation_type,
        tags=[hypo['theme'], hypo['subtheme']],
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
