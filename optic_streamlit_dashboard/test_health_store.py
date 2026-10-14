import sqlite3
import threading
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from attribute_health.config import Settings
from attribute_health.contracts import Contract
from attribute_health.store import BudgetExceeded, ConflictError, LeaseLost, RunEnvelopeExceeded, Store


class MemoryStoreCase(unittest.TestCase):
    """Real SQLite transactions in memory; no scratch directories or production state touched."""
    def setUp(self):
        self.db = sqlite3.connect(":memory:", isolation_level=None, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        db, lock = self.db, self.lock
        @contextmanager
        def connection(store):
            with lock:
                yield db
        self.connection_patch = patch.object(Store, "_connection", connection)
        self.connection_patch.start()
        self.addCleanup(self.connection_patch.stop)
        self.addCleanup(self.db.close)
        self.clock = [datetime(2026, 10, 12, 18, tzinfo=timezone.utc)]
        self.settings = Settings(state_dir=Path.home() / ".optic" / ("validation-" + uuid.uuid4().hex))
        self.store = self.new_store()

    def new_store(self, **overrides):
        with patch.object(Path, "mkdir"):
            return Store(replace(self.settings, **overrides), clock=lambda: self.clock[0])

    def incident(self, status="Technical error"):
        contract = Contract("1", "flag", "ACQUISITION", "BOOL", prompt="Return true when the agent offers.")
        self.store.put("registry", contract.key, contract.as_dict())
        self.store.put("incidents", "incident", {"contract_key": contract.key, "status": status})
        return contract


class StoreTests(MemoryStoreCase):
    def test_large_snapshots_are_compressed_and_preserve_immutable_semantics(self):
        payload = {"by_key": {str(i): {"values": [0] * 500} for i in range(100)}}
        self.store.put("daily", "large", payload, immutable=True)
        saved = self.store.get("daily", "large")
        self.assertEqual(saved["by_key"], payload["by_key"])
        self.assertEqual(self.store.list("daily"), [saved])
        raw = self.db.execute("SELECT payload FROM objects WHERE id='large'").fetchone()[0]
        self.assertLess(len(raw), 10000)
        self.store.put("daily", "large", payload, immutable=True)
        with self.assertRaises(ConflictError):
            self.store.put("daily", "large", {"changed": True})

    def test_legacy_large_json_snapshot_remains_readable_and_idempotent(self):
        from attribute_health.contracts import canonical_json
        payload = {"id": "legacy", "rows": tuple([0] * 20000)}
        self.db.execute("INSERT INTO objects(kind,id,payload,immutable,created,updated) VALUES(?,?,?,?,?,?)",
                        ("daily", "legacy", canonical_json(payload), 1, 0, 0))
        self.store.put("daily", "legacy", payload, immutable=True)
        self.assertEqual(len(self.store.get("daily", "legacy")["rows"]), 20000)

    def test_immutable_snapshot_and_identical_replay(self):
        self.store.put("runs", "r", {"window": {}})
        self.store.put("runs", "r", {"window": {}})
        with self.assertRaises(ConflictError):
            self.store.put("runs", "r", {"window": {"changed": True}})

    def test_run_progress_overlay_does_not_mutate_snapshot(self):
        self.store.put("runs", "r", {"fixture": True})
        self.store.put("run_progress", "r", {"status": "complete"})
        self.assertEqual(self.store.get("runs", "r")["status"], "complete")
        self.assertEqual(self.store.list("runs")[0]["status"], "complete")

    def test_compare_and_set_rejects_concurrent_content(self):
        self.store.put("incidents", "i", {"status": "Watch"})
        version = self.store.version("incidents", "i")
        self.store.put("incidents", "i", {"status": "Candidate"}, expected_version=version)
        with self.assertRaises(ConflictError):
            self.store.put("incidents", "i", {"status": "Resolved"}, expected_version=version)

    def test_invalid_kind_and_object(self):
        with self.assertRaises(ValueError):
            self.store.put("../file", "id", {})
        with self.assertRaises(ValueError):
            self.store.put("incidents", "id", [])

    def test_checkout_state_rejected(self):
        root = Path(__file__).resolve().parent.parent
        with self.assertRaises(ValueError):
            Store(root / "forbidden-state")

    def test_settings_or_path_constructor_and_root_alias(self):
        with patch.object(Path, "mkdir"):
            other = Store(self.settings.state_dir)
        self.assertEqual(other.root, self.store.root)

    def test_live_lease_blocks_second_worker(self):
        first = self.store.claim("item", "a")
        self.assertIsNone(self.store.claim("item", "b"))
        self.store.release(first)
        next_lease = self.store.claim("item", "b")
        self.assertGreater(next_lease["fence"], first["fence"])

    def test_stale_fencing_blocks_put_release_and_reservation(self):
        first = self.store.claim("item", "a", seconds=2)
        self.clock[0] += timedelta(seconds=3)
        second = self.store.claim("item", "b")
        for operation in (
            lambda: self.store.put("incidents", "i", {}, lease=first),
            lambda: self.store.release(first),
            lambda: self.store.reserve("judge", 1, 1, lease=first),
        ):
            with self.assertRaises(LeaseLost):
                operation()
        self.store.assert_lease(second)

    def test_lease_renew_and_invalid_duration(self):
        lease = self.store.claim("item", "a", seconds=2)
        renewed = self.store.renew(lease, seconds=4)
        self.assertGreater(renewed["expires"], lease["expires"])
        with self.assertRaises(ValueError):
            self.store.renew(renewed, seconds=-1)

    def test_disposition_idempotency_and_history(self):
        self.incident()
        payload = {"author": "Local reviewer", "outcome": "Inconclusive", "action": "Monitoring", "note": "reviewed"}
        first = self.store.record_disposition("incident", payload, "key")
        self.assertEqual(first, self.store.record_disposition("incident", payload, "key"))
        self.assertEqual(len(self.store.list("dispositions")), 1)
        self.assertEqual(self.store.get("incidents", "incident")["status"], "Monitoring")
        with self.assertRaises(ConflictError):
            self.store.record_disposition("incident", dict(payload, note="different"), "key")

    def test_disposition_validation_and_unknown_incident(self):
        with self.assertRaises(ValueError):
            self.store.record_disposition("missing", {"author": "x", "outcome": "healthy", "action": "Resolved"}, "k")
        with self.assertRaises(ValueError):
            self.store.record_disposition("missing", {"author": "", "outcome": "Inconclusive", "action": "Resolved"}, "k")

    def test_rubric_version_and_approval_are_immutable(self):
        contract = self.incident()
        draft = self.store.save_rubric(contract.key, {"author": "x", "text": "Draft rules", "approved": False}, "1")
        approved = self.store.save_rubric(contract.key, {"author": "x", "text": "Approved rules", "approved": True}, "2")
        self.assertEqual((draft["version"], approved["version"]), (1, 2))
        self.assertNotEqual(draft["rubric_hash"], approved["rubric_hash"])
        self.assertEqual(self.store.latest_rubric(contract.key), approved)
        with self.assertRaises(ConflictError):
            self.store.put("rubrics", draft["id"], dict(draft, approved=True))

    def test_rubric_rejects_truthy_string_approval(self):
        contract = self.incident()
        with self.assertRaises(ValueError):
            self.store.save_rubric(contract.key, {"author": "x", "text": "rules", "approved": "false"}, "k")

    def test_human_item_review_requires_existing_incident_item(self):
        self.incident()
        self.store.put("judgments", "incident:item", {"incident_id": "incident", "item_id": "item"})
        saved = self.store.record_item_review("incident", "item", {"author": "x", "decision": "uncertain"}, "k")
        self.assertEqual(saved["decision"], "uncertain")
        with self.assertRaises(ValueError):
            self.store.record_item_review("incident", "other", {"author": "x", "decision": "accept"}, "k")

    def test_retry_is_only_local_and_resolved_rejected(self):
        self.incident()
        row = self.store.queue_retry("incident", {"author": "x", "stage": "audit"}, "k")
        self.assertFalse(row["ai_dispatch_enabled"])
        self.assertEqual(row["dispatch"], "local only")
        self.store.put("incidents", "incident", {"status": "Resolved"})
        with self.assertRaises(ConflictError):
            self.store.queue_retry("incident", {"author": "x"}, "next")

    def test_shared_concurrent_attempt_cap(self):
        stores = [self.new_store(transcript_attempts_day=8, reserve_fraction=0) for _ in range(2)]
        def reserve(index):
            try:
                token = stores[index % 2].reserve("judge", 10, 10, item_ref="item" + str(index))
                stores[index % 2].settle(token, input_tokens=10, output_tokens=10, successful=True)
                return True
            except BudgetExceeded:
                return False
        with ThreadPoolExecutor(max_workers=4) as workers:
            result = list(workers.map(reserve, range(20)))
        self.assertEqual(sum(result), 8)
        self.assertEqual(stores[0].budget_summary()["transcript_attempts"], 8)

    def test_daily_tokens_and_per_request_limits(self):
        store = self.new_store(input_tokens_day=15, output_tokens_day=15,
                               input_tokens_request=10, output_tokens_request=10)
        with self.assertRaises(BudgetExceeded):
            store.reserve("judge", 11, 1)
        first = store.reserve("judge", 10, 10)
        store.settle(first, input_tokens=10, output_tokens=10)
        with self.assertRaises(BudgetExceeded):
            store.reserve("judge", 6, 1)

    def test_combined_sentinel_dismissed_reserve(self):
        store = self.new_store(transcript_attempts_day=20, reserve_fraction=.1)
        for pool in ("sentinel", "dismissed"):
            token = store.reserve("judge", 1, 1, pool=pool)
            store.settle(token, input_tokens=1, output_tokens=1)
        with self.assertRaises(BudgetExceeded):
            store.reserve("judge", 1, 1, pool="sentinel")
        self.assertEqual(store.budget_summary()["reserve_attempts"], 2)

    def test_main_cannot_consume_reserve(self):
        store = self.new_store(transcript_attempts_day=10, reserve_fraction=.1)
        for _ in range(9):
            token = store.reserve("judge", 1, 1)
            store.settle(token, input_tokens=1, output_tokens=1)
        with self.assertRaises(BudgetExceeded):
            store.reserve("judge", 1, 1)
        store.reserve("judge", 1, 1, pool="dismissed")

    def test_timeout_expiry_conservatively_charged_and_get_readonly(self):
        first = self.store.reserve("judge", 100, 200)
        self.clock[0] += timedelta(seconds=self.settings.lease_seconds + 1)
        before = self.db.total_changes
        self.assertEqual(self.store.budget_summary()["input_tokens"], 100)
        self.assertEqual(self.db.total_changes, before)
        self.store.reserve("review", 1, 1)
        summary = self.store.budget_summary()
        self.assertEqual(summary["uncertain_usage_requests"], 1)
        self.store.settle(first, input_tokens=0, output_tokens=0)
        self.assertEqual(self.store.budget_summary()["input_tokens"], 101)

    def test_actual_settlement_releases_estimate_not_attempt(self):
        reservation = self.store.reserve("judge", 100, 200, item_ref="a", transcript_hash="hash")
        self.store.settle(reservation, input_tokens=5, output_tokens=7, successful=True)
        self.store.settle(reservation, input_tokens=999, output_tokens=999)
        summary = self.store.budget_summary()
        self.assertEqual((summary["input_tokens"], summary["output_tokens"]), (5, 7))
        self.assertEqual((summary["successful_requests"], summary["unique_transcripts"]), (1, 1))

    def test_retry_attempts_separate_from_items_and_transcripts(self):
        for _ in range(2):
            reservation = self.store.reserve("judge", 1, 1, item_ref="same", transcript_hash="same")
            self.store.settle(reservation, uncertain=True)
        summary = self.store.budget_summary()
        self.assertEqual((summary["transcript_attempts"], summary["retries"]), (2, 1))
        self.assertEqual((summary["call_attribute_items"], summary["unique_transcripts"]), (1, 1))

    def test_central_day_reset_not_utc_midnight_and_dst(self):
        self.clock[0] = datetime(2026, 11, 1, 4, 59, tzinfo=timezone.utc)
        reservation = self.store.reserve("judge", 1, 1)
        self.store.settle(reservation, input_tokens=1, output_tokens=1)
        self.clock[0] = datetime(2026, 11, 1, 5, tzinfo=timezone.utc)
        self.assertEqual(self.store.budget_summary()["transcript_attempts"], 0)
        self.assertEqual(self.store.budget_summary()["day"], "2026-11-01")

    def test_persisted_rotation_across_store_instances(self):
        self.assertEqual(self.store.rotate("sentinel", ["b", "a"]), ["a"])
        other = self.new_store()
        self.assertEqual(other.rotate("sentinel", ["a", "b"]), ["b"])

    def test_concurrent_retry_keys_only_one_pending_operation(self):
        self.incident()
        stores = [self.store, self.new_store()]
        def queue(index):
            try:
                return stores[index].queue_retry("incident", {"author": "x"}, str(index))
            except ConflictError:
                return None
        with ThreadPoolExecutor(max_workers=2) as workers:
            results = list(workers.map(queue, range(2)))
        self.assertEqual(sum(x is not None for x in results), 1)
        self.assertEqual(len(self.store.list("retries")), 1)

    def test_retry_budget_checked_inside_transaction(self):
        self.incident()
        store = self.new_store(review_attempts_day=1)
        token = store.reserve("review", 1, 1)
        store.settle(token, input_tokens=1, output_tokens=1)
        with self.assertRaises(ConflictError):
            store.queue_retry("incident", {"author": "x", "stage": "review"}, "blocked")

    def test_item_ledger_counts_cached_calls_separately_from_requests(self):
        for index in range(40):
            self.store.record_audit_item("item:" + str(index), "call:" + str(index), "shared-content")
        token = self.store.reserve("judge", 1, 1, transcript_hash="shared-content", item_ref="extract:shared")
        self.store.settle(token, input_tokens=1, output_tokens=1, successful=True)
        summary = self.store.budget_summary()
        self.assertEqual((summary["call_attribute_items"], summary["unique_transcripts"]), (40, 40))
        self.assertEqual((summary["transcript_attempts"], summary["unique_transcript_hashes"]), (1, 1))

    def envelope_run(self, run_id="r"):
        self.store.put("runs", run_id, {"settings": {"max_reviews_run": 10, "max_pilots_run": 5}})
        return self.store.claim("run:" + run_id, "coordinator")

    def test_two_worker_atomic_run_review_and_pilot_envelopes(self):
        run_lease = self.envelope_run()
        stores = [self.store, self.new_store()]
        phases = [self.store.claim("phase:" + str(i), "worker" + str(i)) for i in range(2)]
        def reserve(pair):
            kind, index = pair
            try:
                return stores[index % 2].reserve_run_envelope("r", kind, "incident" + str(index),
                    lease=run_lease, phase_lease=phases[index % 2])["new"]
            except RunEnvelopeExceeded:
                return False
        for kind, expected in (("review", 10), ("pilot", 5)):
            with ThreadPoolExecutor(max_workers=4) as workers:
                results = list(workers.map(reserve, [(kind, i) for i in range(25)]))
            self.assertEqual(sum(results), expected)
            self.assertEqual(self.store.run_envelope_summary("r")[kind]["used"], expected)

    def test_run_envelope_retry_reuses_distinct_incident_slot(self):
        lease = self.envelope_run()
        first = self.store.reserve_run_envelope("r", "review", "incident", lease=lease)
        second = self.store.reserve_run_envelope("r", "review", "incident", lease=lease)
        self.assertTrue(first["new"])
        self.assertFalse(second["new"])
        self.assertEqual(second["count"], 1)

    def test_run_envelope_limit_frozen_despite_resume_process_setting_change(self):
        lease = self.envelope_run()
        for i in range(10):
            self.store.reserve_run_envelope("r", "review", str(i), lease=lease)
        changed = self.new_store(max_reviews_run=100)
        with self.assertRaises(RunEnvelopeExceeded):
            changed.reserve_run_envelope("r", "review", "extra", lease=lease)
        self.assertEqual(changed.run_envelope_summary("r")["review"]["limit"], 10)

    def test_new_immutable_run_gets_separate_envelope(self):
        lease = self.envelope_run()
        for i in range(5):
            self.store.reserve_run_envelope("r", "pilot", str(i), lease=lease)
        new_lease = self.envelope_run("r2")
        self.assertTrue(self.store.reserve_run_envelope("r2", "pilot", "new", lease=new_lease)["new"])
        self.assertEqual(self.store.run_envelope_summary("r")["pilot"]["used"], 5)
        self.assertEqual(self.store.run_envelope_summary("r2")["pilot"]["used"], 1)

    def test_run_and_phase_fencing_checked_before_envelope_write(self):
        lease = self.envelope_run()
        phase = self.store.claim("phase", "worker", seconds=1)
        self.clock[0] += timedelta(seconds=2)
        self.store.claim("phase", "replacement")
        with self.assertRaises(LeaseLost):
            self.store.reserve_run_envelope("r", "review", "i", lease=lease, phase_lease=phase)
        self.assertEqual(self.store.run_envelope_summary("r")["review"]["used"], 0)
        self.store.release(lease)
        self.store.claim("run:r", "replacement")
        with self.assertRaises(LeaseLost):
            self.store.reserve_run_envelope("r", "review", "i", lease=lease)

    def test_run_envelope_requires_matching_run_resource(self):
        lease = self.envelope_run()
        with self.assertRaises(ValueError):
            self.store.reserve_run_envelope("other", "review", "i", lease=lease)

    def test_legacy_partial_run_seeds_prior_work_before_new_reservation(self):
        lease = self.envelope_run()
        for index in range(10):
            self.store.put("incidents", str(index), {"run_id": "r", "review": {"decision": "Audit"}})
        before = self.db.total_changes
        self.assertEqual(self.store.run_envelope_summary("r")["review"]["used"], 10)
        self.assertEqual(self.db.total_changes, before)
        with self.assertRaises(RunEnvelopeExceeded):
            self.store.reserve_run_envelope("r", "review", "new", lease=lease)
        self.assertFalse(self.store.reserve_run_envelope("r", "review", "0", lease=lease)["new"])
