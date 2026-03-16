from brain_research.models import AlphaLineage, MutationRecord


class LineageService:
    def build_lineage(self, alpha_id: str, parent_alpha_id: str | None, mutation_type: str | None):
        if not parent_alpha_id:
            return AlphaLineage(
                lineage_id=f'lin_{alpha_id}',
                alpha_id=alpha_id,
                ancestor_chain=[alpha_id],
                mutations=[],
            )
        return AlphaLineage(
            lineage_id=f'lin_{alpha_id}',
            alpha_id=alpha_id,
            ancestor_chain=[parent_alpha_id, alpha_id],
            mutations=[mutation_type] if mutation_type else [],
        )

    def build_mutation_record(self, from_alpha_id: str, to_alpha_id: str, reason: str, actions: list[str]):
        return MutationRecord(
            mutation_id=f'mut_{to_alpha_id}',
            from_alpha_id=from_alpha_id,
            to_alpha_id=to_alpha_id,
            reason=reason,
            actions=actions,
            expected_effect={},
        )
