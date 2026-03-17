# brain_research

First-pass skeleton for the WorldQuant BRAIN automatic research system.

Included modules:
- enums.py
- models.py
- storage.py
- diagnoser.py
- mutator.py
- pool_manager.py
- scheduler.py
- pipeline.py
- adapters/discover_submit_adapter.py
- orchestrators/result_ingestion.py
- orchestrators/improve_consumer.py
- orchestrators/submission_status_writer.py
- services/lineage_service.py
- services/improvement_service.py
- run_bridge.py

Current live wiring:
- `discover_and_submit.py` writes real simulation outputs into `brain_research_data/`
- improve decisions enqueue mutation jobs with `source_bucket`, `priority_score`, `mutation_expected_gain`, lineage fields, and branch controls
- `run_bridge.py` now acts as a bridge into the main path: it consumes improve jobs, schedules jobs under real bucket constraints, marks scheduled jobs, and syncs submission status into `brain_research_data/submission_status.jsonl`
- `scheduler.py` is now a real job scheduler with explore / exploit / improve / retry buckets, family caps, theme caps, and freeze logic
- adapter settings are inferred from family / hypothesis rather than one hardcoded template

Current goal:
- establish lineage-aware research memory
- classify failures
- route candidates into pools
- consume improve queue into the main simulation path
- write back submission status
- prioritize scarce 5-slot simulation capacity using scheduler decisions
