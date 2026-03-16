from enum import Enum


class CandidateStatus(str, Enum):
    PENDING_SIM = "pending_sim"
    SIMULATED = "simulated"
    IN_IMPROVE_POOL = "in_improve_pool"
    IN_SUBMIT_POOL = "in_submit_pool"
    REJECTED = "rejected"
    ARCHIVED = "archived"


class Decision(str, Enum):
    REJECT_POOL = "reject_pool"
    IMPROVE_POOL = "improve_pool"
    SUBMIT_POOL = "submit_pool"
    ARCHIVE_POOL = "archive_pool"


class FailureLabel(str, Enum):
    LOW_SHARPE = "low_sharpe"
    LOW_FITNESS = "low_fitness"
    WEAK_RETURNS = "weak_returns"
    HIGH_TURNOVER = "high_turnover"
    HIGH_DRAWDOWN = "high_drawdown"
    TOO_CORRELATED = "too_correlated"
    OVERCOMPLEX = "overcomplex"
    SETTING_MISMATCH = "setting_mismatch"
    SIGNAL_TOO_NOISY = "signal_too_noisy"
    SIGNAL_TOO_WEAK = "signal_too_weak"
    GOOD_SIGNAL_BAD_SHELL = "good_signal_bad_shell"
    GOOD_SHELL_BAD_SETTING = "good_shell_bad_setting"
    REGIME_SENSITIVE = "regime_sensitive"
    PROMISING_BUT_NEEDS_REFINEMENT = "promising_but_needs_refinement"
    PASS = "pass"
    NEAR_PASS = "near_pass"


class MutationType(str, Enum):
    INCREASE_DECAY = "increase_decay"
    DECREASE_DECAY = "decrease_decay"
    CHANGE_NEUTRALIZATION = "change_neutralization"
    CHANGE_LOOKBACK = "change_lookback"
    CHANGE_SIGNAL_BACKBONE = "change_signal_backbone"
    CHANGE_VOLUME_CONFIRM = "change_volume_confirm"
    ADD_SMOOTHING = "add_smoothing"
    REMOVE_COMPLEXITY = "remove_complexity"
    ADD_CONDITIONAL = "add_conditional"
    CHANGE_HORIZON = "change_horizon"
