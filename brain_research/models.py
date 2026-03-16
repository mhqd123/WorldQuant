from dataclasses import dataclass, field, asdict
from typing import List, Optional, Dict, Any

from .enums import CandidateStatus, Decision


@dataclass
class Hypothesis:
    hypothesis_id: str
    theme: str
    subtheme: str
    description: str
    data_family: List[str]
    expected_horizon: str
    expected_turnover: str
    risk_notes: List[str] = field(default_factory=list)
    priority: float = 0.5
    status: str = "active"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class CandidateSettings:
    region: str = "USA"
    universe: str = "TOP3000"
    delay: int = 1
    decay: int = 4
    neutralization: str = "subindustry"
    truncation: float = 0.08

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class OperatorProfile:
    ts_ops: int
    cross_section_ops: int
    group_ops: int
    conditional_ops: int
    complexity_score: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class SourceProfile:
    uses_price: bool = False
    uses_volume: bool = False
    uses_fundamental: bool = False
    uses_sentiment: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class AlphaCandidate:
    alpha_id: str
    hypothesis_id: str
    family_id: str
    parent_alpha_id: Optional[str]
    generation: int
    expression: str
    settings: CandidateSettings
    operator_profile: OperatorProfile
    source_profile: SourceProfile
    prior_score: float = 0.0
    novelty_score: float = 0.0
    redundancy_score: float = 0.0
    mutation_type: Optional[str] = None
    lineage_depth: int = 0
    status: str = CandidateStatus.PENDING_SIM.value
    tags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["settings"] = self.settings.to_dict()
        d["operator_profile"] = self.operator_profile.to_dict()
        d["source_profile"] = self.source_profile.to_dict()
        return d


@dataclass
class SimulationMetrics:
    sharpe: float
    turnover: float
    fitness: float
    returns: float
    drawdown: float
    margin: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class SimulationResult:
    sim_id: str
    alpha_id: str
    metrics: SimulationMetrics
    passed_internal_threshold: bool
    stable_enough: bool
    too_correlated: bool
    diagnosis_labels: List[str]
    decision: str = Decision.REJECT_POOL.value
    notes: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["metrics"] = self.metrics.to_dict()
        return d


@dataclass
class AlphaLineage:
    lineage_id: str
    alpha_id: str
    ancestor_chain: List[str]
    mutations: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class FamilyStats:
    family_id: str
    attempts: int = 0
    near_pass_count: int = 0
    submit_count: int = 0
    avg_mutation_gain: float = 0.0
    recent_success_rate: float = 0.0
    budget_weight: float = 1.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class MutationRecord:
    mutation_id: str
    from_alpha_id: str
    to_alpha_id: str
    reason: str
    actions: List[str]
    expected_effect: Dict[str, str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
