import copy
import inspect
from dataclasses import replace
from datetime import date, timedelta
from unittest.mock import Mock, patch

import OPTIC_ATTRIBUTE_MONITOR as entrypoint
from attribute_health.ai import AIError, FixtureClient
from attribute_health.config import Window
from attribute_health.contracts import Contract, digest
from attribute_health.repository import FixtureRepository
from attribute_health.workflow import Worker
from test_health_audit import detection
from test_health_store import MemoryStoreCase
from test_health_readiness import readiness_evidence
from attribute_health.readiness import evaluate_job_readiness


class WorkflowTests(MemoryStoreCase):
    def setUp(self):
        super().setUp()
        self.window = Window.ending(end="2026-10-12")
        self.repository = FixtureRepository(settings=self.settings)
        self.client = FixtureClient(fixture=True, store=self.store, settings=self.settings)
        self.detect_calls = 0
        original = self.repository.transcript
        identifiers = {}
        def distinct_transcript(item, window):
            result = original(item, window)
            identifiers.setdefault(item["production_result_id"], len(identifiers))
            result["transcript"] += "\nAgent: Unique synthetic conversation " + str(identifiers[item["production_result_id"]]) + "."
            result["transcript_hash"] = digest(result["transcript"])
            return result
        self.repository.transcript = distinct_transcript

    def detector(self, contracts, daily, window, settings):
        self.detect_calls += 1
        return [detection(contract, window) if index == 0 else dict(
            contract_key=contract.key, status="Insufficient data", signals=[], mix={},
            statistics={"scan_adjusted_p": 1., "corrected_q": 1.})
            for index, contract in enumerate(contracts)]

    def worker(self, **overrides):
        args = dict(repository=self.repository, settings=self.settings, store=self.store,
                    client=self.client, fixture=True, render=False, detector=self.detector)
        args.update(overrides)
        return Worker(**args)

    def candidate(self):
        return next(x for x in self.store.list("incidents") if x.get("signals"))

    def test_source_heartbeat_keeps_long_discovery_leased_and_is_unbound(self):
        callbacks = []
        self.repository.set_heartbeat = lambda callback: callbacks.append(callback)
        original = self.repository.registry
        def slow_registry():
            for _ in range(3):
                self.clock[0] += timedelta(seconds=self.settings.lease_seconds * .6)
                callbacks[-1]()
            return original()
        self.repository.registry = slow_registry
        run = self.worker().execute("detect", window=self.window)
        self.assertEqual(run["status"], "complete")
        self.assertTrue(callable(callbacks[0]))
        self.assertIsNone(callbacks[-1])

    def test_source_heartbeat_is_unbound_after_discovery_failure(self):
        callbacks = []
        self.repository.set_heartbeat = lambda callback: callbacks.append(callback)
        self.repository.registry = Mock(side_effect=RuntimeError("source failed"))
        with self.assertRaisesRegex(RuntimeError, "source failed"):
            self.worker().execute("detect", window=self.window)
        self.assertIsNone(callbacks[-1])

    def test_full_offline_e2e_persists_all_stages_and_disposition(self):
        run = self.worker().execute("run", window=self.window)
        incident = self.candidate()
        self.assertEqual(run["status"], "complete")
        self.assertEqual(incident["status"], "Needs human review")
        self.assertEqual(len(incident["sample_manifest"]["items"]), 40)
        self.assertEqual(len(self.store.list("judgments")), 40)
        self.assertEqual(len(self.store.list("judgment_versions")), 40)
        self.assertEqual(len(self.store.list("evidence")), 1)
        self.assertEqual(len(self.store.list("charts")), 1)
        self.assertTrue(incident["summary"]["complete"])
        self.assertEqual(len(incident["summary"]["human_review_items"]), 20)
        self.assertTrue(run["demo"])
        saved = self.store.record_disposition(incident["id"], {
            "author": "Fixture human", "outcome": "Inconclusive", "action": "Monitoring", "note": "Demo only"
        }, "human")
        self.assertEqual(self.store.get("incidents", incident["id"])["disposition"], saved)
        self.assertFalse(any("transcript" in x for x in self.store.list("judgments")))

    def test_successful_resume_never_repeats_successful_ai(self):
        run = self.worker().execute("run", window=self.window)
        calls = len(self.client.calls)
        self.worker().execute("run", resume=run["id"])
        self.assertEqual(len(self.client.calls), calls)
        self.assertEqual(self.detect_calls, 1)

    def test_safe_modes_override_enabled_client_no_ai(self):
        for mode in ("dry_run", "plan_only", "read_only"):
            worker = self.worker(**{mode: True})
            worker.execute("run", window=self.window)
        self.assertEqual(self.client.calls, [])
        self.assertEqual(self.store.budget_summary()["transcript_attempts"], 0)

    def test_resume_after_dry_run_invalidates_missing_downstream_stages(self):
        run = self.worker(dry_run=True).execute("run", window=self.window)
        self.assertEqual(self.client.calls, [])
        resumed = self.worker().execute("run", resume=run["id"])
        self.assertEqual(resumed["status"], "complete")
        self.assertEqual(len(self.store.list("judgments")), 40)

    def test_expansion_incremental_preserves_pilot_judgments(self):
        run = self.worker().execute("run", window=self.window)
        old = {x["id"]: x for x in self.store.list("judgments")}
        self.worker().execute("run", resume=run["id"], expand=True)
        self.assertEqual(len(self.store.list("judgments")), 120)
        self.assertTrue(all(self.store.get("judgments", key) == value for key, value in old.items()))
        self.assertEqual(len(self.client.calls), 121)

    def test_two_workers_same_run_fenced(self):
        run = self.worker().execute("detect", window=self.window)
        claim = self.store.claim("run:" + run["id"], "other")
        result = self.worker().execute("audit", resume=run["id"])
        self.assertEqual(result["status"], "leased")
        self.assertEqual(self.client.calls, [])
        self.store.release(claim)

    def test_item_lease_prevents_duplicate_audit(self):
        worker = self.worker()
        run = worker.execute("run", window=self.window)
        incident = self.candidate()
        item = incident["sample_manifest"]["items"][0]
        with self.store._transaction() as db:
            db.execute("DELETE FROM objects WHERE kind='judgments' AND id=?",
                       (incident["id"] + ":" + item["item_id"],))
            db.execute("DELETE FROM objects WHERE kind='checkpoints' AND id=?", (run["id"] + ":audit",))
        claim = self.store.claim("item:" + incident["id"] + ":" + item["item_id"], "other")
        calls = len(self.client.calls)
        worker.execute("audit", resume=run["id"])
        self.assertEqual(len(self.client.calls), calls)
        self.assertEqual(len(self.store.list("judgments")), 39)
        self.store.release(claim)

    def test_failed_item_resume_keeps_successes_safe_errors(self):
        original = self.client.call
        failed = [False]
        def call(*args, **kwargs):
            if kwargs["category"] == "judge" and not failed[0]:
                failed[0] = True
                raise AIError("synthetic_transport_failure", retryable=True)
            return original(*args, **kwargs)
        with patch.object(self.client, "call", side_effect=call):
            run = self.worker().execute("run", window=self.window)
        self.assertEqual(len(self.store.list("judgments")), 39)
        self.assertEqual(self.store.list("errors")[0]["code"], "synthetic_transport_failure")
        calls = len(self.client.calls)
        self.worker().execute("run", resume=run["id"])
        self.assertEqual(len(self.store.list("judgments")), 40)
        self.assertEqual(len(self.client.calls), calls + 1)

    def test_out_of_budget_partial_resume_and_counts(self):
        settings = replace(self.settings, transcript_attempts_day=20, reserve_fraction=0)
        store = self.new_store(transcript_attempts_day=20, reserve_fraction=0)
        client = FixtureClient(fixture=True, store=store, settings=settings)
        run = self.worker(settings=settings, store=store, client=client).execute("run", window=self.window)
        self.assertEqual(len(store.list("judgments")), 20)
        self.assertEqual(store.budget_summary()["transcript_attempts"], 20)
        self.clock[0] += timedelta(days=1)
        self.worker(settings=settings, store=store, client=client).execute("run", resume=run["id"])
        self.assertEqual(len(store.list("judgments")), 40)

    def test_severe_persistent_cannot_be_dismissed(self):
        original = self.client.call
        def dismiss(*args, **kwargs):
            response = original(*args, **kwargs)
            if kwargs["category"] == "review":
                import json
                value = json.loads(response["content"])
                value["decision"] = "Monitor"
                response["content"] = json.dumps(value)
            return response
        with patch.object(self.client, "call", side_effect=dismiss):
            self.worker().execute("run", window=self.window)
        self.assertTrue(self.candidate()["review"]["deterministic_override"])
        self.assertEqual(len(self.store.list("judgments")), 40)

    def test_measured_mix_can_defer_nonsevere_persistent_candidate(self):
        def detector(*args):
            rows = self.detector(*args)
            rows[0]["mix"] = {"brand": {"mix_supported": True}}
            return rows
        original = self.client.call
        def explained(*args, **kwargs):
            import json
            result = original(*args, **kwargs)
            if kwargs["category"] == "review":
                value = json.loads(result["content"])
                value["decision"] = "Population-mix supported"
                result["content"] = json.dumps(value)
            return result
        with patch.object(self.client, "call", side_effect=explained):
            run = self.worker(detector=detector).execute("detect", window=self.window)
            self.worker().execute("review", resume=run["id"])
        incident = self.candidate()
        self.assertEqual(incident["status"], "Monitoring")
        self.assertFalse(incident["review"]["deterministic_override"])
        self.assertTrue(incident["dismissed_audit_eligible"])
        self.assertFalse(self.store.list("judgments"))
        self.assertEqual(incident["review_recheck_after"], "2026-10-19")

    def test_mix_explanation_does_not_hide_other_unexplained_label(self):
        def detector(*args):
            rows = self.detector(*args)
            rows[0]["mix"] = {"brand": {"mix_supported": True}}
            rows[0]["mix_by_label"] = {"true": {"brand": {"mix_supported": True}},
                                       "false": {"brand": {"mix_supported": False}}}
            rows[0]["signals"].append(dict(rows[0]["signals"][0], label="false", direction="decrease"))
            return rows
        original = self.client.call
        def explained(*args, **kwargs):
            import json
            result = original(*args, **kwargs)
            if kwargs["category"] == "review":
                value = json.loads(result["content"])
                value["decision"] = "Population-mix supported"
                result["content"] = json.dumps(value)
            return result
        with patch.object(self.client, "call", side_effect=explained):
            run = self.worker(detector=detector).execute("detect", window=self.window)
            self.worker().execute("review", resume=run["id"])
        self.assertEqual(self.candidate()["status"], "Audit queued")
        self.assertTrue(self.candidate()["review"]["deterministic_override"])

    def test_large_catalog_only_materializes_candidate_evidence(self):
        self.many_contracts(250)
        def detector(*args):
            rows = self.detector(*args)
            family = [c.key for c in args[0]]
            for row in rows:
                row["statistics"]["declared_family"] = family
            return rows
        with patch("attribute_health.evidence.render_png") as render:
            self.worker(detector=detector, render=True).execute("detect", window=self.window)
        self.assertEqual(len(self.store.list("incidents")), 250)
        self.assertEqual(len(self.store.list("evidence")), 1)
        self.assertEqual(len(self.store.list("charts")), 1)
        self.assertEqual(len(self.store.list("detector_families")), 1)
        render.assert_not_called()
        from attribute_health.contracts import canonical_json
        for incident in self.store.list("incidents"):
            self.assertNotIn("declared_family", incident["statistics"])
            self.assertNotIn("packet", incident["evidence"])
            self.assertNotIn("chart_spec", incident["evidence"])
            self.assertLess(len(canonical_json(incident)), 8000)

    def test_image_exports_respect_review_slots_and_do_not_run_during_detection(self):
        from attribute_health import evidence
        detector = self.many_contracts(12)
        settings = replace(self.settings, image_verified=True)
        worker = self.worker(detector=detector, render=True, settings=settings)
        original = evidence.save_evidence
        def fake_export(*args, **kwargs):
            return original(*args, render=False)
        with patch.object(evidence, "save_evidence", side_effect=fake_export) as export, \
                patch.object(worker, "_image", return_value=None):
            run = worker.execute("detect", window=self.window)
            self.assertFalse(any(x.kwargs.get("render") for x in export.call_args_list))
            worker.execute("review", resume=run["id"])
            self.assertEqual(sum(x.kwargs.get("render", False) for x in export.call_args_list), 10)
            worker.execute("review", resume=run["id"])
            self.assertEqual(sum(x.kwargs.get("render", False) for x in export.call_args_list), 10)

    def test_storage_failure_does_not_mask_original_failure_during_cleanup(self):
        import sqlite3
        worker = self.worker()
        original_put = self.store.put
        def failing_put(kind, *args, **kwargs):
            if kind == "errors":
                raise sqlite3.OperationalError("Synthetic unavailable disk")
            return original_put(kind, *args, **kwargs)
        with patch.object(self.repository, "registry", side_effect=ValueError("Original source failure")), \
                patch.object(self.store, "put", side_effect=failing_put), \
                patch.object(self.store, "release", side_effect=sqlite3.OperationalError("Synthetic cleanup failure")), \
                self.assertLogs("attribute_health.workflow", level="WARNING"):
            with self.assertRaisesRegex(ValueError, "Original source failure"):
                worker.execute("detect", window=self.window)

    def test_watch_promotes_when_statistical_confirmation_arrives(self):
        def watch(*args):
            rows = self.detector(*args)
            rows[0]["status"] = "Watch"
            return rows
        first = self.worker(detector=watch).execute("detect", window=self.window)
        old = self.candidate()
        self.assertEqual(old["status"], "Watch")
        self.assertEqual(old["evidence"]["daily_run_id"], first["id"])
        second = self.worker().execute("detect", window=self.window)
        incident = self.candidate()
        self.assertEqual(incident["id"], old["id"])
        self.assertEqual(incident["status"], "AI review pending")
        self.assertIn(second["id"], incident["run_ids"])
        self.assertIn("packet_hash", incident["evidence"])

    def test_coverage_status_updates_without_creating_another_incident(self):
        def clear(*args):
            return [dict(contract_key=c.key, status="No detected shift", signals=[], mix={},
                         statistics={"scan_adjusted_p": 1., "corrected_q": 1.}) for c in args[0]]
        self.worker().execute("detect", window=self.window)
        key = self.repository.registry()[1].key
        old = next(x for x in self.store.list("incidents") if x["contract_key"] == key)
        self.assertEqual(old["status"], "Insufficient data")
        self.worker(detector=clear).execute("detect", window=self.window)
        changed = [x for x in self.store.list("incidents") if x["contract_key"] == key]
        self.assertEqual(len(changed), 1)
        self.assertEqual(changed[0]["status"], "No detected shift")

    def test_invalid_review_never_clears_flag(self):
        self.client.call = Mock(return_value={"content": '{"decision":"Monitor"}'})
        self.worker().execute("run", window=self.window)
        self.assertEqual(self.candidate()["status"], "Technical error")
        self.assertFalse(self.store.list("judgments"))

    def test_unresolved_transcripts_preserved_without_inference(self):
        with patch.object(self.repository, "transcript", return_value={"status": "multi-content unresolved"}):
            self.worker().execute("run", window=self.window)
        self.assertEqual(len(self.client.calls), 1)
        self.assertTrue(all(x["comparison"] == "unresolved" for x in self.store.list("judgments")))
        self.assertGreater(self.candidate()["summary"]["periods"]["pre"]["unresolved_n"], 0)

    def test_changed_rubric_creates_new_extractions_preserving_first_judgments(self):
        run = self.worker().execute("run", window=self.window)
        incident = self.candidate()
        calls = len(self.client.calls)
        self.store.save_rubric(incident["contract_key"], {
            "author": "Human", "text": "Require an explicit offer in agent speech.", "approved": True
        }, "approved")
        self.worker().execute("run", resume=run["id"], reevaluate=True)
        self.assertEqual(len(self.client.calls), calls + 40)
        self.assertEqual(len(self.store.list("judgment_versions")), 80)
        self.assertEqual(len(self.store.list("judgments")), 40)

    def test_source_lag_recomputes_90_dates_and_persists_diagnostics(self):
        self.repository.readiness = Mock(return_value={
            "ready_through": "2026-10-09", "watermark": "lagged", "excluded_identity_conflicts": 3})
        run = self.worker().execute("detect", window=self.window)
        self.assertEqual(run["window"]["end"], "2026-10-09")
        self.assertEqual(run["window"]["days"], 90)
        self.assertEqual(run["readiness"]["excluded_identity_conflicts"], 3)

    def test_baseline_ageing_preserves_old_evidence_no_old_audit_in_new_run(self):
        run = self.worker(dry_run=True).execute("detect", window=self.window)
        incident = self.candidate()
        original_hash = incident["evidence"]["packet_hash"]
        future = Window.ending(end="2026-10-12")
        changed = copy.deepcopy(incident)
        changed["signals"][0]["baseline_start"] = "2026-01-01"
        self.store.put("incidents", incident["id"], changed)
        self.worker(dry_run=True).execute("run", window=future)
        saved = self.store.get("incidents", incident["id"])
        self.assertIn("no longer available", saved["baseline_ageing"])
        self.assertEqual(saved["evidence"]["packet_hash"], original_hash)
        self.assertIsNone(saved["sample_manifest"])

    def test_snapshot_window_cannot_change_on_resume(self):
        run = self.worker().execute("detect", window=self.window)
        with self.assertRaises(ValueError):
            self.worker().execute("run", resume=run["id"], window=Window.ending(end="2026-10-11"))

    def test_fake_adapter_cannot_enter_real_source_mode(self):
        with self.assertRaises(ValueError):
            self.worker(fixture=False)

    def test_malformed_daily_counts_and_calendar_rejected(self):
        original = self.repository.aggregate
        def malformed(contracts, window):
            rows = original(contracts, window)
            rows[contracts[0].key][0]["valid"] += 1
            return rows
        with patch.object(self.repository, "aggregate", side_effect=malformed):
            with self.assertRaises(ValueError):
                self.worker().execute("detect", window=self.window)
        self.assertTrue(self.store.list("errors"))

    def test_later_stage_requires_resume(self):
        with self.assertRaises(ValueError):
            self.worker().execute("audit", window=self.window)

    def test_handler_unannotated_and_fixture_flags_explicit(self):
        self.assertEqual(inspect.signature(entrypoint.main).return_annotation, inspect.Signature.empty)
        self.assertTrue(all(x.annotation is inspect.Signature.empty for x in inspect.signature(entrypoint.main).parameters.values()))
        with self.assertRaises(ValueError):
            entrypoint.main(fixture_ai=True)
        with self.assertRaises(ValueError):
            entrypoint.main(fixture=True, enable_ai=True)

    def test_cli_parser_safe_default_and_no_ai_after_environment_enable(self):
        args = vars(entrypoint.parser().parse_args([]))
        self.assertEqual(args["stage"], "detect")
        self.assertFalse(args["enable_ai"])
        with patch.dict("os.environ", {"OPTIC_HEALTH_AI_ENABLED": "1"}), patch(
                "attribute_health.repository.SnowflakeRepository") as repository, patch(
                "attribute_health.workflow.Worker") as worker:
            worker.return_value.execute.return_value = {"id": "safe"}
            entrypoint.main(stage="detect", read_only=True)
            self.assertFalse(worker.call_args.args[1].ai_enabled)
            self.assertIsNone(worker.call_args.kwargs["client"])

    def test_replay_uses_core_and_persists_without_inference(self):
        with patch("attribute_health.replay.replay", return_value={"metrics": {"judge_human_agreement": None}}) as core:
            report = self.worker(dry_run=True).replay(["2026-10-01", "2026-10-12"])
        core.assert_called_once()
        self.assertEqual(core.call_args.kwargs["evaluation_dates"], [date(2026, 10, 1), date(2026, 10, 12)])
        self.assertEqual(self.store.get("replays", report["id"]), report)
        self.assertEqual(self.client.calls, [])

    def test_duplicate_transcript_cache_counts_forty_calls_not_two_requests(self):
        self.repository = FixtureRepository(settings=self.settings)
        run = self.worker().execute("run", window=self.window)
        self.assertEqual(len(self.client.calls), 3)
        self.assertEqual(run["audit_counts"]["call_attribute_items"], 40)
        self.assertEqual(run["audit_counts"]["unique_production_results"], 40)
        self.assertEqual(run["audit_counts"]["unique_content_extractions"], 2)
        self.assertEqual(run["audit_counts"]["cache_hits"], 38)
        self.assertEqual(self.store.budget_summary()["unique_transcripts"], 40)
        self.assertEqual(self.candidate()["summary"]["call_attribute_items"], 40)

    def test_unflagged_sentinel_has_saved_evidence_and_combined_reserve(self):
        def stable(contracts, daily, window, settings):
            return [dict(contract_key=x.key, status="No detected shift", signals=[], mix={},
                         statistics={"scan_adjusted_p": 1., "corrected_q": 1.}) for x in contracts]
        run = self.worker(detector=stable).execute("run", window=self.window)
        sentinel = next(x for x in self.store.list("incidents") if x.get("audit_pool") == "sentinel")
        self.assertTrue(sentinel["evidence"]["packet_hash"])
        self.assertEqual(sentinel["summary"]["call_attribute_items"], 40)
        self.assertEqual(self.store.budget_summary()["reserve_attempts"], 40)
        self.worker(detector=stable).execute("run", resume=run["id"])
        self.assertEqual(self.store.budget_summary()["reserve_attempts"], 40)

    def test_dismissed_candidate_uses_same_reserve_not_second_pool(self):
        def ordinary(contracts, daily, window, settings):
            results = self.detector(contracts, daily, window, settings)
            results[0]["signals"][0]["persistent"] = False
            return results
        original = self.client.call
        def monitor(*args, **kwargs):
            response = original(*args, **kwargs)
            if kwargs["category"] == "review":
                import json
                value = json.loads(response["content"])
                value["decision"] = "Monitor"
                response["content"] = json.dumps(value)
            return response
        with patch.object(self.client, "call", side_effect=monitor):
            self.worker(detector=ordinary).execute("run", window=self.window)
        incident = self.candidate()
        self.assertEqual(incident["audit_pool"], "dismissed")
        self.assertEqual(self.store.budget_summary()["reserve_attempts"], 40)
        self.assertEqual(len(self.store.list("judgments")), 40)

    def test_explicit_queued_retry_recovers_unresolved_identity_only(self):
        with patch.object(self.repository, "transcript", return_value={"status": "missing"}):
            run = self.worker().execute("run", window=self.window)
        incident = self.candidate()
        self.store.queue_retry(incident["id"], {"author": "Local reviewer", "stage": "audit"}, "identity-retry")
        self.worker().execute("run", resume=run["id"])
        self.assertTrue(all(x["identity"]["status"] == "resolved" for x in self.store.list("judgments")))
        self.assertEqual(self.store.list("retries")[0]["status"], "completed")

    def test_changed_judge_prompt_invalidates_saved_extractions(self):
        run = self.worker().execute("run", window=self.window)
        count = len(self.client.calls)
        with patch("attribute_health.ai.JUDGE_PROMPT_VERSION", "health-blind-next"):
            self.worker().execute("run", resume=run["id"], reevaluate=True)
        self.assertEqual(len(self.client.calls), count + 40)
        self.assertEqual(len(self.store.list("judgment_versions")), 80)

    def test_monitor_rechecked_after_seven_new_dates_with_frozen_baseline(self):
        def ordinary(contracts, daily, window, settings):
            results = self.detector(contracts, daily, window, settings)
            results[0]["signals"][0]["persistent"] = False
            return results
        original = self.client.call
        def monitor(*args, **kwargs):
            response = original(*args, **kwargs)
            import json
            value = json.loads(response["content"])
            value["decision"] = "Monitor"
            response["content"] = json.dumps(value)
            return response
        with patch.object(self.client, "call", side_effect=monitor):
            run = self.worker(detector=ordinary).execute("detect", window=Window.ending(end="2026-10-01"))
            self.worker(detector=ordinary).execute("review", resume=run["id"])
            initial = self.candidate()
            first_baseline = initial["signals"][0]["baseline_start"]
            first_packet = initial["evidence"]["packet_hash"]
            self.assertEqual(initial["review_recheck_after"], "2026-10-08")
            later = self.worker(detector=ordinary).execute("detect", window=Window.ending(end="2026-10-08"))
            rechecked = self.candidate()
            self.assertEqual(rechecked["signals"][0]["baseline_start"], first_baseline)
            self.assertNotEqual(rechecked["evidence"]["packet_hash"], first_packet)
            self.assertEqual(rechecked["status"], "AI review pending")
            self.worker(detector=ordinary).execute("review", resume=later["id"])
        self.assertEqual(len(self.client.calls), 2)

    def test_draft_rubric_and_resolved_disposition_never_rejudge_completed_resume(self):
        run = self.worker().execute("run", window=self.window)
        incident = self.candidate()
        first = copy.deepcopy(self.store.list("judgments"))
        count = len(self.client.calls)
        self.store.save_rubric(incident["contract_key"], {
            "author": "Offline UI reviewer", "text": "Unapproved fixture draft", "approved": False
        }, "draft")
        self.store.record_disposition(incident["id"], {
            "author": "Offline UI reviewer", "outcome": "Inconclusive", "action": "Resolved"
        }, "resolved")
        self.worker().execute("run", resume=run["id"])
        self.assertEqual(len(self.client.calls), count)
        self.assertEqual(self.store.list("judgments"), first)
        self.assertEqual(len(self.store.list("judgment_versions")), 40)
        self.assertEqual(self.store.get("incidents", incident["id"])["status"], "Resolved")
        self.assertEqual(self.store.get("incidents", incident["id"])["next_action"], "Resolved")

    def test_legacy_resolved_disposition_restores_status_without_rejudging(self):
        run = self.worker().execute("run", window=self.window)
        incident = self.candidate()
        calls = len(self.client.calls)
        self.store.record_disposition(incident["id"], {
            "author": "Offline UI reviewer", "outcome": "Inconclusive", "action": "Resolved"
        }, "resolved")
        changed = self.store.get("incidents", incident["id"])
        changed.update(status="Needs human review", next_action="Review examples")
        self.store.put("incidents", incident["id"], changed)
        self.worker().execute("run", resume=run["id"])
        fixed = self.store.get("incidents", incident["id"])
        self.assertEqual((fixed["status"], fixed["next_action"]), ("Resolved", "Resolved"))
        self.assertEqual(fixed["disposition"], changed["disposition"])
        self.assertEqual(len(self.client.calls), calls)

    def test_approved_rubric_and_prompt_changes_require_explicit_reevaluation(self):
        run = self.worker().execute("run", window=self.window)
        incident = self.candidate()
        first = copy.deepcopy(self.store.list("judgments"))
        count = len(self.client.calls)
        self.store.save_rubric(incident["contract_key"], {
            "author": "Human approver", "text": "Approved new taxonomy", "approved": True
        }, "approval")
        with patch("attribute_health.ai.JUDGE_PROMPT_VERSION", "health-blind-future"):
            self.worker().execute("run", resume=run["id"])
        self.assertEqual(len(self.client.calls), count)
        self.assertEqual(self.store.list("judgments"), first)

    def test_deferred_enabled_job_never_claims_healthy_or_dispatches_ai(self):
        contracts = self.repository.registry()
        self.repository.readiness = Mock(return_value={
            "ready_through": "2026-10-12", "watermark": None, "deferred_jobs": ["ACQUISITION"],
            "jobs": {"ACQUISITION": {"status": "unknown", "reasons": ["ID/watermark evidence missing."]}}})
        run = self.worker().execute("run", window=self.window)
        incident = next(x for x in self.store.list("incidents") if x["contract_key"] == contracts[0].key)
        self.assertEqual(incident["status"], "Not yet evaluated")
        self.assertEqual(incident["statistics"]["scan_adjusted_p"], 1.)
        self.assertIn("Source readiness deferred", incident["next_action"])
        self.assertEqual(incident["signals"], [])
        self.assertEqual(self.client.calls, [])
        self.assertEqual(run["readiness"]["deferred_jobs"], ["ACQUISITION"])

    def test_deferred_job_remains_in_full_detector_family_with_zero_input(self):
        self.repository.readiness = Mock(return_value={
            "ready_through": "2026-10-12", "deferred_jobs": ["ACQUISITION"], "jobs": {}})
        captured = []
        def detector(contracts, daily, window, settings):
            captured.append((contracts, daily))
            results = self.detector(contracts, daily, window, settings)
            for result in results:
                result["statistics"]["family_size"] = len(contracts)
            return results
        self.worker(detector=detector).execute("detect", window=self.window)
        contracts, daily = captured[0]
        self.assertEqual(len(contracts), 4)
        self.assertEqual(sum(x["eligible"] for x in daily[contracts[0].key]), 0)
        incident = next(x for x in self.store.list("incidents") if x["contract_key"] == contracts[0].key)
        self.assertEqual(incident["statistics"]["family_size"], 4)
        self.assertEqual(incident["statistics"]["corrected_q"], 1.)

    def test_job_gate_rolls_back_and_worker_recomputes_exact_ninety_dates(self):
        contracts = self.repository.registry()
        evidence = readiness_evidence(self.window, contracts)
        for row in evidence["days"]:
            if row["job"] == "ACQUISITION" and row["day"] >= "2026-10-10":
                row.update(n=100, nonnull_ids=100)
        for row in evidence["attributes"]:
            if row["job"] == "ACQUISITION" and row["day"] >= "2026-10-10":
                row.update(n=100, present=100)
        with patch("attribute_health.readiness._today", return_value=date(2026, 10, 13)):
            ready = evaluate_job_readiness(self.window, self.settings, ["ACQUISITION", "SERVICE"], evidence, contracts)
        self.repository.readiness = Mock(return_value=ready)
        run = self.worker().execute("detect", window=self.window)
        self.assertEqual(run["window"]["end"], "2026-10-09")
        self.assertEqual(run["window"]["start"], "2026-07-12")
        self.assertEqual(run["window"]["days"], 90)
        self.assertEqual(run["readiness"]["jobs"]["ACQUISITION"]["proposed_end_status"], "incomplete")

    def test_complete_resume_uses_frozen_job_readiness_and_no_source_reprobe(self):
        self.repository.apply_readiness = Mock()
        run = self.worker().execute("run", window=self.window)
        frozen = copy.deepcopy(run["readiness"])
        calls = len(self.client.calls)
        self.repository.readiness = Mock(side_effect=AssertionError("Resume must not probe fresh source."))
        self.worker().execute("run", resume=run["id"])
        self.repository.readiness.assert_not_called()
        self.assertEqual(self.repository.apply_readiness.call_args.args, (frozen, Window.from_dict(run["window"])))
        self.assertEqual(len(self.client.calls), calls)

    def test_quality_signal_routes_processing_before_semantic_audit(self):
        def processing(contracts, daily, window, settings):
            results = self.detector(contracts, daily, window, settings)
            quality = dict(results[0]["signals"][0], label="missing", processing_route=True)
            results[0]["signals"].append(quality)
            return results
        self.worker(detector=processing).execute("run", window=self.window)
        incident = self.candidate()
        self.assertIsNone(incident["sample_manifest"])
        self.assertIn("Data/processing investigation first", incident["next_action"])
        self.assertEqual(len(self.client.calls), 1)
        self.assertEqual(self.store.list("judgments"), [])

    def regime_detector(self, include_third=False, status="Candidate", jitter=False, new_category=False):
        def detect(contracts, daily, window, settings):
            results = self.detector(contracts, daily, window, settings)
            initial = results[0]["signals"][0]
            first = dict(initial, onset="2026-08-12", baseline_start="2026-07-15", baseline_end="2026-08-11",
                         recent_start="2026-08-12", recent_end="2026-08-25",
                         baseline_stable=True, support_sufficient=True)
            second = dict(initial, onset="2026-09-07" if jitter else "2026-09-05",
                          baseline_start="2026-08-08", baseline_end="2026-09-04",
                          recent_start="2026-09-05", recent_end="2026-09-18",
                          baseline_stable=True, support_sufficient=True)
            results[0]["signals"] = [first, second]
            if include_third:
                results[0]["signals"].append(dict(initial, onset="2026-09-26",
                    baseline_start="2026-08-29", baseline_end="2026-09-25",
                    recent_start="2026-09-26", recent_end="2026-10-09",
                    baseline_stable=True, support_sufficient=True))
            if new_category:
                results[0]["signals"].append(dict(second, label="false", direction="decrease"))
            results[0]["status"] = status
            return results
        return detect

    def test_third_material_step_in_new_run_preserves_resolved_old_two_step_incident(self):
        old_run = self.worker(detector=self.regime_detector()).execute("run", window=self.window)
        old = self.candidate()
        self.store.record_disposition(old["id"], {"author": "Human", "outcome": "Inconclusive", "action": "Resolved"}, "done")
        original = copy.deepcopy(self.store.get("incidents", old["id"]))
        old_judgments = [x for x in self.store.list("judgments") if x["incident_id"] == old["id"]]
        new_run = self.worker(detector=self.regime_detector(include_third=True)).execute("run", window=self.window)
        changed = [x for x in self.store.list("incidents") if x["contract_key"] == old["contract_key"] and x["id"] != old["id"]]
        self.assertEqual(len(changed), 1)
        new = changed[0]
        self.assertEqual(new["previous_incident_id"], old["id"])
        self.assertEqual([x["onset"] for x in new["signals"]], ["2026-09-26"])
        self.assertNotEqual(new["evidence"]["packet_hash"], old["evidence"]["packet_hash"])
        self.assertEqual(len(new["sample_manifest"]["items"]), 40)
        self.assertEqual(new["status"], "Needs human review")
        self.assertEqual(self.store.get("incidents", old["id"]), original)
        self.assertEqual([x for x in self.store.list("judgments") if x["incident_id"] == old["id"]], old_judgments)
        self.assertNotEqual(new_run["id"], old_run["id"])
        count = len(self.client.calls)
        self.store.save_rubric(old["contract_key"], {"author": "Human", "text": "Draft only", "approved": False}, "draft")
        self.worker().execute("run", resume=old_run["id"])
        self.assertEqual(len(self.client.calls), count)
        self.assertEqual(self.store.get("incidents", old["id"])["status"], "Resolved")

    def test_open_incident_gets_separate_new_regime_snapshot_without_packet_rewrite(self):
        self.worker(detector=self.regime_detector()).execute("detect", window=self.window)
        old = copy.deepcopy(self.candidate())
        self.worker(detector=self.regime_detector(include_third=True)).execute("detect", window=self.window)
        new = next(x for x in self.store.list("incidents") if x.get("previous_incident_id") == old["id"])
        self.assertEqual(new["status"], "AI review pending")
        self.assertEqual(self.store.get("incidents", old["id"]), old)

    def test_onset_jitter_and_new_category_at_old_step_do_not_duplicate_regime(self):
        self.worker(detector=self.regime_detector()).execute("detect", window=self.window)
        old = self.candidate()
        self.worker(detector=self.regime_detector(jitter=True, new_category=True)).execute("detect", window=self.window)
        same = [x for x in self.store.list("incidents") if x["contract_key"] == old["contract_key"]]
        self.assertEqual(len(same), 1)
        self.assertEqual(same[0]["evidence"]["packet_hash"], old["evidence"]["packet_hash"])

    def test_new_step_without_calibrated_candidate_gate_does_not_reopen_resolved(self):
        self.worker(detector=self.regime_detector()).execute("detect", window=self.window)
        old = self.candidate()
        self.store.record_disposition(old["id"], {"author": "Human", "outcome": "Inconclusive", "action": "Resolved"}, "done")
        self.worker(detector=self.regime_detector(include_third=True, status="Watch")).execute("detect", window=self.window)
        same = [x for x in self.store.list("incidents") if x["contract_key"] == old["contract_key"]]
        self.assertEqual(len(same), 1)
        self.assertEqual(same[0]["status"], "Resolved")

    def many_contracts(self, count):
        base = self.repository.registry()[0]
        contracts = [replace(base, attribute_id="demo-many-" + str(i), name="offer_" + str(i)) for i in range(count)]
        original_aggregate, original_candidates = self.repository.aggregate, self.repository.candidates
        self.repository.registry = Mock(return_value=contracts)
        self.repository.aggregate = lambda selected, window: {
            x.key: copy.deepcopy(original_aggregate([base], window)[base.key]) for x in selected}
        def candidates(contract, window, **kwargs):
            result = original_candidates(base, window, **kwargs)
            result["rows"] = [dict(x, production_result_id=contract.attribute_id + ":" + x["production_result_id"],
                                  contract_hash=contract.contract_hash, attribute_id=contract.attribute_id,
                                  attribute_name=contract.name) for x in result["rows"]]
            return result
        self.repository.candidates = candidates
        def detector(contracts, daily, window, settings):
            return [detection(x, window) for x in contracts]
        return detector

    def test_more_than_ten_reviews_remain_queued_across_repeated_resume(self):
        detector = self.many_contracts(14)
        run = self.worker(detector=detector).execute("detect", window=self.window)
        self.worker(detector=detector).execute("review", resume=run["id"])
        self.assertEqual(len(self.client.calls), 10)
        for _ in range(3):
            self.worker(detector=detector).execute("review", resume=run["id"])
        self.assertEqual(len(self.client.calls), 10)
        self.assertEqual(self.store.run_envelope_summary(run["id"])["review"], {"used": 10, "limit": 10})
        pending = [x for x in self.store.list("incidents") if x["status"] == "AI review pending"]
        self.assertEqual(len(pending), 4)
        self.assertTrue(all("NEW run" in x["next_action"] for x in pending))
        changed = replace(self.settings, max_reviews_run=100)
        self.worker(settings=changed, detector=detector).execute("review", resume=run["id"])
        self.assertEqual(len(self.client.calls), 10)
        next_run = self.worker(detector=detector).execute("detect", window=self.window)
        self.worker(detector=detector).execute("review", resume=next_run["id"])
        self.assertEqual(len(self.client.calls), 14)

    def test_more_than_five_pilots_remain_queued_across_resume_and_setting_change(self):
        detector = self.many_contracts(8)
        run = self.worker(detector=detector).execute("detect", window=self.window)
        self.worker(detector=detector).execute("review", resume=run["id"])
        self.worker(detector=detector).execute("sample", resume=run["id"])
        self.assertEqual(len(self.store.list("manifests")), 5)
        for _ in range(3):
            self.worker(detector=detector).execute("sample", resume=run["id"])
        self.worker(settings=replace(self.settings, max_pilots_run=100), detector=detector).execute("sample", resume=run["id"])
        self.assertEqual(len(self.store.list("manifests")), 5)
        self.assertEqual(self.store.run_envelope_summary(run["id"])["pilot"], {"used": 5, "limit": 5})
        pending = [x for x in self.store.list("incidents") if not x.get("sample_manifest")]
        self.assertEqual(len(pending), 3)
        self.assertTrue(all("NEW run" in x["next_action"] for x in pending))
        next_run = self.worker(detector=detector).execute("detect", window=self.window)
        self.worker(detector=detector).execute("sample", resume=next_run["id"])
        self.assertEqual(len(self.store.list("manifests")), 8)

    def test_review_retry_reuses_run_slot_and_successful_work_stays_skipped(self):
        original = self.client.call
        failed = [False]
        def once(*args, **kwargs):
            if kwargs["category"] == "review" and not failed[0]:
                failed[0] = True
                raise AIError("synthetic_review_failure", retryable=True)
            return original(*args, **kwargs)
        with patch.object(self.client, "call", side_effect=once):
            run = self.worker().execute("run", window=self.window)
        self.assertEqual(self.store.run_envelope_summary(run["id"])["review"]["used"], 1)
        self.worker().execute("run", resume=run["id"])
        self.assertEqual(self.store.run_envelope_summary(run["id"])["review"]["used"], 1)
        self.assertEqual(self.store.run_envelope_summary(run["id"])["pilot"]["used"], 1)
        calls = len(self.client.calls)
        self.worker().execute("run", resume=run["id"], expand=True)
        self.assertEqual(self.store.run_envelope_summary(run["id"])["pilot"]["used"], 1)
        self.assertGreater(len(self.client.calls), calls)

    def test_second_worker_cannot_reset_or_double_run_envelope(self):
        detector = self.many_contracts(12)
        run = self.worker(detector=detector).execute("detect", window=self.window)
        self.worker(detector=detector).execute("review", resume=run["id"])
        held = self.store.claim("run:" + run["id"], "first-worker")
        result = self.worker(store=self.new_store(), detector=detector).execute("review", resume=run["id"])
        self.assertEqual(result["status"], "leased")
        self.store.release(held)
        self.worker(store=self.new_store(), detector=detector).execute("review", resume=run["id"])
        self.assertEqual(len(self.client.calls), 10)
        self.assertEqual(self.store.run_envelope_summary(run["id"])["review"]["used"], 10)
