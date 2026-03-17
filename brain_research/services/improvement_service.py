from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from brain_research.mutator import propose_mutations

TRACE_FILE = Path('/root/.openclaw/workspace/brain_research_data/improve_generation_trace.jsonl')


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def trace(stage: str, alpha_id: str | None, diagnosis_labels: list[str] | None, status: str, actions=None, notes: str = ''):
    TRACE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with TRACE_FILE.open('a', encoding='utf-8') as f:
        f.write(json.dumps({
            'timestamp': utc_now_iso(),
            'alpha_id': alpha_id,
            'stage': stage,
            'status': status,
            'diagnosis_labels': diagnosis_labels or [],
            'actions': actions or [],
            'notes': notes,
        }, ensure_ascii=False) + '\n')


class ImprovementService:
    def enqueue_mutations_for_improve(self, candidate, diagnosis_labels, candidate_store):
        alpha_id = getattr(candidate, 'alpha_id', None) if hasattr(candidate, 'alpha_id') else candidate.get('alpha_id')
        trace('improve_decision_seen', alpha_id, diagnosis_labels, 'start')
        actions = propose_mutations(diagnosis_labels)
        trace('actions_proposed', alpha_id, diagnosis_labels, 'ok', actions=actions)
        generated = self.generate_mutation_candidates(candidate.to_dict() if hasattr(candidate, 'to_dict') else candidate, diagnosis_labels, max_children=3)
        trace('enqueue_called', alpha_id, diagnosis_labels, 'ok', actions=[g.get('mutation_type') for g in generated])
        queue_items = []
        for i, item in enumerate(generated[:3], start=1):
            item.update({
                'status': 'queued_improve',
                'rank': i,
                'source_bucket': 'improve',
                'ts': utc_now_iso(),
            })
            queue_items.append(item)
        for item in queue_items:
            candidate_store.append(item)
            trace('queued_improve_persisted', item.get('parent_alpha_id'), diagnosis_labels, 'ok', actions=[item.get('mutation_type')])
        if queue_items:
            trace('mutation_generated', alpha_id, diagnosis_labels, 'ok', actions=[q.get('mutation_type') for q in queue_items])
            return queue_items
        trace('mutation_generated', alpha_id, diagnosis_labels, 'empty', actions=actions, notes='no mutation candidates generated')
        return [{'parent_alpha_id': alpha_id, 'actions': actions, 'status': 'queued_improve', 'ts': utc_now_iso()}]

    def identify_backbone(self, expression: str) -> str:
        e = expression.lower()
        if '(2 * close - high - low)' in e:
            return 'intraday_position'
        if '(close - open)' in e:
            return 'intraday_body'
        if 'ts_delta(close' in e:
            return 'delta'
        if 'ts_mean(close' in e and 'ts_delta' not in e:
            return 'reversion'
        return 'generic'

    def mutate_settings(self, settings: dict, action: str) -> tuple[dict, str]:
        new_settings = dict(settings)
        expected = 'unknown'
        if action == 'increase_decay':
            new_settings['decay'] = min(15, int(new_settings.get('decay', 4)) + 2)
            expected = 'turnover down'
        elif action == 'decrease_decay':
            new_settings['decay'] = max(1, int(new_settings.get('decay', 4)) - 1)
            expected = 'sharpe up'
        elif action == 'rotate_neutralization':
            order = ['subindustry', 'industry', 'sector']
            current = new_settings.get('neutralization', 'subindustry')
            if current in order:
                new_settings['neutralization'] = order[(order.index(current) + 1) % len(order)]
            else:
                new_settings['neutralization'] = 'subindustry'
            expected = 'de-correlation'
        elif action == 'relax_truncation':
            new_settings['truncation'] = round(min(0.12, float(new_settings.get('truncation', 0.08)) + 0.02), 4)
            expected = 'sharpe up'
        elif action == 'tighten_truncation':
            new_settings['truncation'] = round(max(0.04, float(new_settings.get('truncation', 0.08)) - 0.02), 4)
            expected = 'drawdown down'
        elif action == 'shift_horizon_short_to_mid':
            new_settings['decay'] = max(int(new_settings.get('decay', 4)), 6)
            expected = 'turnover down'
        elif action == 'shift_horizon_mid_to_short':
            new_settings['decay'] = min(int(new_settings.get('decay', 4)), 3)
            expected = 'sharpe up'
        return new_settings, expected

    def mutate_expression(self, expression: str, action: str) -> tuple[str, str]:
        expr = expression
        expected = 'unknown'
        backbone = self.identify_backbone(expr)
        if action == 'swap_backbone_intraday_to_delta' and backbone in {'intraday_position', 'intraday_body'}:
            expr = expr.replace('(2 * close - high - low)', 'ts_delta(close, 2)')
            expr = expr.replace('(close - open)', 'ts_delta(close, 2)')
            expected = 'de-correlation'
        elif action == 'swap_backbone_delta_to_reversion' and backbone == 'delta':
            expr = expr.replace('ts_delta(close, 2)', '(close - ts_mean(close, 6))')
            expected = 'signal_strengthening'
        elif action == 'toggle_volume_confirmation':
            if 'rank(volume / ts_mean(volume,' in expr:
                expr = expr.replace('rank(volume / ts_mean(volume,', 'rank(ts_mean(volume, 5) / ts_mean(volume,')
            elif 'rank(ts_mean(volume, 5) / ts_mean(volume,' in expr:
                expr = expr.replace('rank(ts_mean(volume, 5) / ts_mean(volume,', 'rank(volume / ts_mean(volume,')
            expected = 'de-correlation'
        elif action == 'add_smoothing':
            expr = expr.replace('(1 + high - low)', '(1 + ts_mean(high - low, 3))')
            expr = expr.replace('ts_mean(high - low, 3)', 'ts_mean(high - low, 5)')
            expected = 'turnover down'
        elif action == 'remove_smoothing':
            expr = expr.replace('(1 + ts_mean(high - low, 5))', '(1 + high - low)')
            expr = expr.replace('(1 + ts_mean(high - low, 3))', '(1 + high - low)')
            expected = 'sharpe up'
        elif action == 'toggle_grouping':
            expr = expr.replace('subindustry', 'industry') if 'subindustry' in expr else expr.replace('industry', 'subindustry')
            expected = 'de-correlation'
        elif action == 'toggle_volatility_normalization':
            if 'ts_std_dev(returns' in expr:
                expr = expr.replace(' / (1 + ts_std_dev(returns, 10))', '')
                expr = expr.replace(' / (1 + ts_std_dev(returns, 12))', '')
            else:
                expr = expr.replace('rank(-', 'rank(-(')
                expr = expr.replace(')), subindustry)', ') / (1 + ts_std_dev(returns, 10))), subindustry)')
                expr = expr.replace(')), industry)', ') / (1 + ts_std_dev(returns, 10))), industry)')
            expected = 'simplification'
        elif action == 'swap_range_vol_signal_variant':
            expr = expr.replace('(1 + high - low)', '(1 + ts_mean(high - low, 4))')
            expr = expr.replace('(1 + ts_mean(high - low, 3))', '(1 + ts_mean(high - low, 6))')
            expected = 'de-correlation'
        return expr, expected

    def estimate_mutation_gain(self, action: str, diagnosis_labels: list[str]) -> float:
        base = 0.25
        if 'too_correlated' in diagnosis_labels and action in {'toggle_grouping', 'toggle_volume_confirmation', 'swap_backbone_intraday_to_delta', 'swap_range_vol_signal_variant'}:
            base += 0.35
        if 'low_fitness' in diagnosis_labels and action in {'remove_smoothing', 'decrease_decay'}:
            base += 0.15
        if 'high_turnover' in diagnosis_labels and action in {'increase_decay', 'add_smoothing', 'shift_horizon_short_to_mid'}:
            base += 0.15
        return round(min(base, 0.95), 4)

    def generate_mutation_candidates(self, candidate: dict, diagnosis_labels: list[str], max_children: int = 3) -> list[dict]:
        generation = int(candidate.get('generation', 0))
        if generation > 2:
            return []
        if int(candidate.get('same_diagnosis_count', 0)) >= 2:
            return []
        actions = propose_mutations(diagnosis_labels)
        expanded = []
        for action in actions:
            if action == 'increase_decay':
                expanded.extend(['increase_decay', 'shift_horizon_short_to_mid'])
            elif action == 'change_neutralization':
                expanded.extend(['rotate_neutralization', 'toggle_grouping'])
            elif action == 'change_signal_backbone':
                expanded.extend(['swap_backbone_intraday_to_delta', 'swap_backbone_delta_to_reversion'])
            elif action == 'change_volume_confirm':
                expanded.extend(['toggle_volume_confirmation'])
            elif action == 'add_smoothing':
                expanded.extend(['add_smoothing', 'swap_range_vol_signal_variant'])
            elif action == 'remove_complexity':
                expanded.extend(['remove_smoothing', 'toggle_volatility_normalization'])
            elif action == 'change_horizon':
                expanded.extend(['shift_horizon_short_to_mid', 'shift_horizon_mid_to_short'])
        if not expanded:
            expanded = ['toggle_grouping', 'toggle_volume_confirmation']
        seen = set()
        uniq = []
        for action in expanded:
            if action not in seen:
                seen.add(action)
                uniq.append(action)
        out = []
        settings = dict(candidate.get('settings', {}))
        for action in uniq:
            expr, expr_effect = self.mutate_expression(candidate['expression'], action)
            new_settings, settings_effect = self.mutate_settings(settings, action)
            expected_gain = self.estimate_mutation_gain(action, diagnosis_labels)
            if expected_gain < 0.2:
                continue
            out.append({
                'parent_alpha_id': candidate.get('alpha_id') or candidate.get('parent_alpha_id'),
                'family_id': candidate.get('family_id'),
                'generation': generation + 1,
                'mutation_type': action,
                'reason': ','.join(diagnosis_labels),
                'expected_effect': expr_effect if expr_effect != 'unknown' else settings_effect,
                'mutation_expected_gain': expected_gain,
                'expression': expr,
                'settings': new_settings,
                'source_bucket': 'improve',
                'priority_score': min(0.99, float(candidate.get('priority_score', 0.5)) + expected_gain / 2),
                'same_diagnosis_count': candidate.get('same_diagnosis_count', 0) + 1,
                'status': 'pending_sim',
            })
            if len(out) >= max_children:
                break
        return out
