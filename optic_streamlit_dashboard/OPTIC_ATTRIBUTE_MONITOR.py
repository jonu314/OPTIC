"""Independent Attribute Health CLI / MLRun handler. Importing this module does no live work."""
from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

try:
    from .attribute_health.config import Settings, Window
    from .attribute_health.contracts import canonical_json
except ImportError:
    from attribute_health.config import Settings, Window
    from attribute_health.contracts import canonical_json


def _run(options, context=None):
    if __package__:
        from .attribute_health.ai import FixtureClient, OdinClient
        from .attribute_health.repository import FixtureRepository, ResearchRepository, SnowflakeRepository
        from .attribute_health.store import Store
        from .attribute_health.workflow import Worker
    else:
        from attribute_health.ai import FixtureClient, OdinClient
        from attribute_health.repository import FixtureRepository, ResearchRepository, SnowflakeRepository
        from attribute_health.store import Store
        from attribute_health.workflow import Worker
    fixture = options.get("fixture", False) is True
    research = options.get("research_fixture")
    fake_ai = options.get("fixture_ai", False) is True
    enable_ai = options.get("enable_ai", False) is True
    if fake_ai and not fixture:
        raise ValueError("--fixture-ai requires explicit --fixture.")
    if fixture and research:
        raise ValueError("Synthetic fixtures and recorded research are separate source modes.")
    if (fixture or research) and enable_ai:
        raise ValueError("Real Odin inference is prohibited on fixture/research inputs.")
    if research and fake_ai:
        raise ValueError("Recorded aggregates have no transcripts; synthetic data is not a fallback.")
    overrides = {"ai_enabled": enable_ai}
    if options.get("state_dir"):
        overrides["state_dir"] = Path(options["state_dir"])
    if options.get("all_active_jobs"):
        overrides["all_active_jobs"] = True
    settings = Settings.from_env(**overrides)
    store = Store(settings)
    repository = (FixtureRepository(settings=settings) if fixture else
                  ResearchRepository(research, settings=settings) if research else
                  SnowflakeRepository(settings=settings, context=context))
    client = FixtureClient(fixture=True, store=store, settings=settings) if fake_ai else (
        OdinClient(settings, store, context=context) if enable_ai else None)
    worker = Worker(repository, settings, store, client=client, fixture=fixture,
                    dry_run=options.get("dry_run", False), plan_only=options.get("plan_only", False),
                    read_only=options.get("read_only", False), render=not options.get("no_render", False))
    end = options.get("end")
    if research and not end:
        records = []
        for group in ("acquisition_daily", "boolean_daily"):
            records.extend(row["period"][:10] for row in repository.data.get(group, {}).get("distribution", []))
        if not records:
            raise ValueError("Research fixture has no covered daily dates.")
        end = max(records)
    stage = options.get("stage", "detect")
    if stage == "replay":
        return worker.replay(options.get("replay_end") or ([end] if end else [Window.ending().end.isoformat()]))
    result = worker.execute(stage, window=Window.ending(end=end) if end else None,
                            resume=options.get("resume"), expand=options.get("expand", False),
                            reevaluate=options.get("reevaluate", False))
    if options.get("fixture_disposition"):
        if not fixture:
            raise ValueError("Demo dispositions require explicit fixture mode.")
        completed = [x for x in store.list("incidents") if x.get("fixture") and (x.get("summary") or {}).get("complete")]
        if not completed:
            raise ValueError("Complete a fixture audit before saving a demo disposition.")
        for incident in completed:
            store.record_disposition(incident["id"], {
                "author": "Explicit fixture reviewer", "outcome": "Inconclusive", "action": "Monitoring",
                "note": "Synthetic demonstration only; not a production or human-calibration decision."
            }, "fixture-disposition:" + result["id"])
    return result


def main(context=None, **kwargs):
    return _run(kwargs, context=context)


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("stage", nargs="?", default="detect",
                        choices=("detect", "review", "sample", "audit", "summarize", "run", "replay"))
    sources = result.add_mutually_exclusive_group()
    sources.add_argument("--fixture", action="store_true", help="Explicit synthetic source; visibly demo data")
    sources.add_argument("--research-fixture", help="Recorded aggregate snapshot; never fake transcripts")
    result.add_argument("--fixture-ai", action="store_true", help="Explicit synthetic inference, never Odin")
    result.add_argument("--fixture-disposition", action="store_true", help="Save a clearly labeled demo disposition")
    result.add_argument("--enable-ai", action="store_true", help="Explicitly authorize this worker's real Odin calls")
    result.add_argument("--dry-run", action="store_true", help="No inference or production writes; local artifacts allowed")
    result.add_argument("--plan-only", action="store_true", help="No inference or image rendering")
    result.add_argument("--read-only", action="store_true", help="Source/aggregate detection only; local snapshots allowed")
    result.add_argument("--no-render", action="store_true", help="Keep saved chart spec but explicitly skip PNG export")
    result.add_argument("--all-active-jobs", action="store_true")
    result.add_argument("--state-dir", help="Durable local/shared state directory outside the checkout")
    result.add_argument("--end", help="Last complete Central date; source readiness can move it earlier")
    result.add_argument("--resume", help="Frozen run ID; only missing/changed/failed stages are performed")
    result.add_argument("--expand", action="store_true", help="Incrementally expand promising/uncertain pilot toward 120")
    result.add_argument("--reevaluate", action="store_true",
                        help="Explicitly regrade under latest approved rubric/prompt; preserves first judgment history")
    result.add_argument("--replay-end", action="append", help="Chronological replay end date; may be repeated")
    return result


if __name__ == "__main__":
    args = vars(parser().parse_args())
    outcome = main(**args)
    print(canonical_json({key: outcome[key] for key in ("id", "status", "window", "stages", "runs", "budget", "metrics")
                          if key in outcome}))
