from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any

DEFAULT_BUCKET_PLAN = {"explore": 1, "exploit": 3, "improve": 1, "retry": 0}
IMPROVE_HEAVY_PLAN = {"explore": 1, "exploit": 2, "improve": 2, "retry": 0}
EXPLORE_HEAVY_PLAN = {"explore": 2, "exploit": 2, "improve": 1, "retry": 0}
MAX_CONCURRENCY = 5


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def family_priority_score(stats: Dict[str, Any]) -> float:
    return (
        0.35 * float(stats.get("recent_success_rate", 0.0))
        + 0.25 * float(stats.get("near_pass_density", 0.0))
        + 0.20 * float(stats.get("avg_mutation_gain", 0.0))
        + 0.20 * float(stats.get("novelty_budget_need", 0.0))
    )


def priority_score(job: Dict[str, Any], family_stats: Dict[str, Dict[str, Any]] | None = None) -> float:
    stats = (family_stats or {}).get(job.get("family_id"), {})
    return (
        0.30 * float(job.get("prior_score", 0.0))
        + 0.20 * float(stats.get("recent_success_rate", 0.0))
        + 0.20 * float(job.get("mutation_expected_gain", 0.0))
        + 0.15 * float(job.get("novelty_score", 0.0))
        + 0.15 * float(job.get("improve_urgency", 0.0))
        - 0.20 * float(job.get("redundancy_penalty", job.get("redundancy_score", 0.0)))
    )


def is_family_frozen(family_id: str, family_stats: Dict[str, Dict[str, Any]], now: datetime | None = None) -> bool:
    now = now or utc_now()
    stats = family_stats.get(family_id, {})
    freeze_until = parse_dt(stats.get("family_freeze_until"))
    return bool(freeze_until and freeze_until > now)


def apply_family_freeze_policy(family_stats: Dict[str, Dict[str, Any]], now: datetime | None = None) -> Dict[str, Dict[str, Any]]:
    now = now or utc_now()
    updated = dict(family_stats)
    for family_id, stats in updated.items():
        no_near = int(stats.get("consecutive_no_near_pass", 0))
        no_qual = int(stats.get("consecutive_no_submit_candidate", 0))
        branch_no_gain = int(stats.get("consecutive_no_gain_mutations", 0))
        if no_qual >= 20:
            stats["family_freeze_until"] = (now + timedelta(hours=72)).isoformat()
            stats["freeze_reason"] = "20_no_submit_candidate"
        elif no_near >= 10:
            stats["family_freeze_until"] = (now + timedelta(hours=24)).isoformat()
            stats["freeze_reason"] = "10_no_near_pass"
        if branch_no_gain >= 2:
            stats["branch_stop_reason"] = "2_mutations_without_gain"
    return updated


def choose_bucket_plan(queue_state: Dict[str, List[Dict[str, Any]]], family_stats: Dict[str, Dict[str, Any]] | None = None) -> Dict[str, int]:
    improve_count = len(queue_state.get("improve", []))
    explore_items = queue_state.get("explore", [])
    stats = family_stats or {}
    high_explore = 0
    for item in explore_items:
        family_id = item.get("family_id")
        fam = stats.get(family_id, {})
        if family_priority_score(fam) >= 0.45:
            high_explore += 1
    if improve_count >= 8:
        return IMPROVE_HEAVY_PLAN.copy()
    if high_explore >= 3:
        return EXPLORE_HEAVY_PLAN.copy()
    return DEFAULT_BUCKET_PLAN.copy()


def pick_with_bucket_constraints(
    jobs: List[Dict[str, Any]],
    slots: int,
    active_jobs: List[Dict[str, Any]],
    family_stats: Dict[str, Dict[str, Any]] | None = None,
    now: datetime | None = None,
) -> List[Dict[str, Any]]:
    now = now or utc_now()
    family_stats = apply_family_freeze_policy(family_stats or {}, now=now)
    active_by_family = defaultdict(int)
    active_by_theme = defaultdict(int)
    total_if_full = max(MAX_CONCURRENCY, len(active_jobs) + slots)
    theme_cap = max(1, int(total_if_full * 0.4))
    for job in active_jobs:
        active_by_family[job.get("family_id")] += 1
        active_by_theme[job.get("theme", "generic")] += 1

    ordered = sorted(jobs, key=lambda x: priority_score(x, family_stats), reverse=True)
    picked = []
    for job in ordered:
        if len(picked) >= slots:
            break
        family_id = job.get("family_id")
        theme = job.get("theme", "generic")
        if is_family_frozen(family_id, family_stats, now=now):
            continue
        family_limit = 2 if priority_score(job, family_stats) >= 0.85 else 1
        if active_by_family[family_id] >= family_limit:
            continue
        if active_by_theme[theme] >= theme_cap:
            continue
        active_by_family[family_id] += 1
        active_by_theme[theme] += 1
        picked.append(job)
    return picked


def schedule_next_jobs(
    queue_state: Dict[str, List[Dict[str, Any]]],
    active_jobs: List[Dict[str, Any]] | None = None,
    family_stats: Dict[str, Dict[str, Any]] | None = None,
    max_concurrency: int = MAX_CONCURRENCY,
) -> List[Dict[str, Any]]:
    active_jobs = active_jobs or []
    family_stats = family_stats or {}
    free_slots = max(0, max_concurrency - len(active_jobs))
    if free_slots <= 0:
        return []
    plan = choose_bucket_plan(queue_state, family_stats)
    picked = []
    remaining = free_slots
    local_active = list(active_jobs)
    for bucket in ["retry", "improve", "exploit", "explore"]:
        if remaining <= 0:
            break
        requested = min(plan.get(bucket, 0), remaining)
        if requested <= 0:
            continue
        bucket_jobs = [dict(job, source_bucket=bucket) for job in queue_state.get(bucket, [])]
        selected = pick_with_bucket_constraints(bucket_jobs, requested, local_active + picked, family_stats)
        picked.extend(selected)
        remaining = free_slots - len(picked)
    if remaining > 0:
        leftovers = []
        for bucket, jobs in queue_state.items():
            if bucket not in {"retry", "improve", "exploit", "explore"}:
                continue
            leftovers.extend([dict(job, source_bucket=bucket) for job in jobs])
        seen = {(j.get("candidate_id"), j.get("source_bucket")) for j in picked}
        leftovers = [j for j in leftovers if (j.get("candidate_id"), j.get("source_bucket")) not in seen]
        picked.extend(pick_with_bucket_constraints(leftovers, remaining, local_active + picked, family_stats))
    return picked
