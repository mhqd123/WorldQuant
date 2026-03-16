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
- `discover_and_submit.py` now writes real simulation outputs into `brain_research_data/`
- improve decisions enqueue lightweight mutation tasks into `brain_research_data/improve_queue.jsonl`
- `run_bridge.py` can consume improve tasks and sync submission status into `brain_research_data/submission_status.jsonl`

Current goal:
- establish lineage-aware research memory
- classify failures
- route candidates into pools
- consume improve queue
- write back submission status
