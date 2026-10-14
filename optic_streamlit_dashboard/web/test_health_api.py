"""Offline API/security tests; the only server binds an ephemeral loopback port."""
from __future__ import annotations

import copy
import hashlib
import http.client
import json
import shutil
import subprocess
import threading
import unittest
import uuid
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from unittest.mock import Mock, patch

from health_service import HealthError, HealthService, MAX_BODY, OUTCOMES, PREFIX, STATUSES, local_request, strict_json
from attribute_health.config import CENTRAL, Settings, Window
from attribute_health.contracts import Contract, digest


class FakeStore:
    def __init__(self):
        self.objects = {}
        self.mutations = []
        self.idempotency = {}
        self.budget = {"day": "2026-10-13", "transcript_attempts": 0, "review_attempts": 0,
                       "input_tokens": 0, "output_tokens": 0,
                       "limits": {"transcript_attempts": 500, "review_attempts": 20,
                                  "input_tokens": 2000000, "output_tokens": 500000}}

    def put(self, kind, identifier, payload, immutable=False):
        value = copy.deepcopy(payload)
        value.setdefault("id", identifier)
        self.objects.setdefault(kind, {})[identifier] = value

    def get(self, kind, identifier):
        return copy.deepcopy(self.objects.get(kind, {}).get(identifier))

    def list(self, kind):
        return copy.deepcopy(list(self.objects.get(kind, {}).values()))

    def budget_summary(self):
        return copy.deepcopy(self.budget)

    def save(self, kind, scope, payload, key):
        identity = (scope, key)
        if identity in self.idempotency:
            old_payload, result = self.idempotency[identity]
            if old_payload != payload:
                raise ValueError("Idempotency conflict")
            return copy.deepcopy(result)
        result = dict(payload, id=str(len(self.mutations) + 1), created_at="2026-10-13T12:00:00-05:00")
        self.mutations.append((kind, scope, copy.deepcopy(payload)))
        self.put(kind, result["id"], result)
        self.idempotency[identity] = (copy.deepcopy(payload), result)
        return copy.deepcopy(result)

    def record_disposition(self, incident_id, payload, idempotency_key):
        return self.save("dispositions", "disposition:" + incident_id,
                         dict(payload, incident_id=incident_id), idempotency_key)

    def save_rubric(self, contract_key, payload, idempotency_key):
        return self.save("rubrics", "rubric:" + contract_key,
                         dict(payload, contract_key=contract_key), idempotency_key)

    def record_item_review(self, incident_id, item_id, payload, idempotency_key):
        return self.save("item_reviews", incident_id + ":" + item_id,
                         dict(payload, incident_id=incident_id, item_id=item_id), idempotency_key)

    def queue_retry(self, incident_id, payload, idempotency_key):
        return self.save("retries", "retry:" + incident_id,
                         dict(payload, incident_id=incident_id, status="queued", ai_dispatch_enabled=False),
                         idempotency_key)


def seeded():
    store = FakeStore()
    window = Window.from_dict({"start": "2026-07-15", "end": "2026-10-12", "watermark": "2026-10-13"})
    contracts = [
        Contract("demo-1", "same_name", "ACQUISITION", "BOOL", prompt="Offer only if present."),
        Contract("demo-2", "same_name", "RETENTIONCHATTX", "BOOL", channel="chat"),
        Contract("demo-3", "measurement", "SERVICE", "NUMERIC"),
    ]
    for contract in contracts:
        store.put("registry", contract.key, contract.as_dict())
    packet = {"window": window.as_dict(), "daily": [{"day": "2026-07-15", "eligible": 20}],
              "contract": contracts[0].as_dict(), "demo": True}
    packet["hash"] = digest(packet)
    spec = {"tooltip": {"trigger": "axis", "renderMode": "richText"},
            "xAxis": {"data": ["2026-07-15"]}, "series": [{"name": "true", "type": "line", "data": [20]}]}
    chart_hash = digest(spec)
    store.put("evidence", packet["hash"], packet)
    store.put("charts", chart_hash, {"hash": chart_hash, "packet_hash": packet["hash"], "spec": spec})
    incident = {"id": "incident-1", "contract_key": contracts[0].key, "contract": contracts[0].as_dict(),
                "status": "Audit complete", "severity": "high", "window": window.as_dict(),
                "signals": [], "statistics": {}, "mix": {"brands": {"TXUE": {"raw": .2}}},
                "review": {"decision": "Audit", "rationale": "Sustained supported movement."},
                "summary": {"periods": {"pre": {"disagreement_rate": .1}, "post": {"disagreement_rate": .2}}},
                "mode": "fixture", "sample_manifest": "manifest-1",
                "evidence": {"packet_hash": packet["hash"], "chart_hash": chart_hash,
                             "chart_spec_hash": chart_hash},
                "transcript": "NEVER LIST FULL TRANSCRIPT", "api_token": "NEVER LIST TOKEN"}
    store.put("incidents", incident["id"], incident)
    second = dict(incident, id="incident-2", contract_key=contracts[1].key,
                  contract=contracts[1].as_dict(), status="Technical error")
    store.put("incidents", second["id"], second)
    identity = {"item_id": "item-1", "production_result_id": "result-1", "connection_id": "conn-1",
                "job": "ACQUISITION", "interaction_date": "2026-07-20T12:00:00", "stored_output": True}
    manifest = {"id": "manifest-1", "incident_id": incident["id"], "window": window.as_dict(),
                "items": [identity], "fixture": True, "full_transcript": "NEVER LIST FULL TRANSCRIPT"}
    store.put("manifests", "manifest-1", manifest)
    store.put("judgments", "incident-1:item-1",
              {"incident_id": "incident-1", "item_id": "item-1", "stored_output": True,
               "extracted_value": False, "status": "Supported", "comparison": "disagreement",
               "quotes": [{"speaker": "Agent", "text": "I will explain your bill."}],
               "transcript_text": "NEVER LIST FULL TRANSCRIPT"})
    store.put("runs", "run-1", {"id": "run-1", "mode": "fixture", "status": "complete",
                              "window": window.as_dict(), "completed_at": "2026-10-13T11:00:00-05:00"})
    return store


def decoded(response):
    return json.loads(response.body)


class HealthServiceTests(unittest.TestCase):
    def setUp(self):
        self.store = seeded()
        self.repository = Mock()
        item = self.store.get("manifests", "manifest-1")["items"][0]
        self.repository.transcript.return_value = dict(item, status="resolved",
            transcript="Agent: I will explain your bill.\nCustomer: Thank you.", transcript_analysis_id="analysis-1",
            analysis_content_id="content-1", completeness="full")
        self.factory = Mock(return_value=self.repository)
        self.service = HealthService(Settings(), self.store, self.factory)

    def get(self, endpoint, query=None):
        return self.service.handle_get(PREFIX + endpoint, query or {})

    def post(self, endpoint, payload, key="test-key"):
        return self.service.handle_post(PREFIX + endpoint, payload, key)

    def test_overview_all_contracts_duplicate_names_and_capabilities(self):
        result = decoded(self.get("/overview"))
        self.assertEqual(result["all_contracts"], 3)
        self.assertEqual(len([x for x in result["contracts"] if x["name"] == "same_name"]), 2)
        self.assertEqual(set(result["counts"]), set(STATUSES))
        self.assertEqual(sum(result["counts"].values()), 3)
        numeric = next(x for x in result["contracts"] if x["result_type"] == "NUMERIC")
        self.assertEqual(numeric["status"], "Not yet evaluated")
        self.assertEqual(numeric["monitoring_capability"]["output_shift"], "deferred")
        chat = next(x for x in result["contracts"] if x["channel"] == "chat")
        self.assertFalse(chat["runtime_enabled"])
        self.assertTrue(result["demo"])
        self.assertFalse(result["safety"]["inference_enabled"])
        self.assertEqual(result["window"]["days"], 90)

    def test_monitoring_disposition_is_not_mislabelled_as_watch(self):
        self.store.objects["incidents"]["incident-1"]["status"] = "Monitoring"
        result = decoded(self.get("/overview", {"status": ["Monitoring"]}))
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["contracts"][0]["status"], "Monitoring")
        self.assertEqual(result["counts"]["Monitoring"], 1)

    def test_latest_discovery_hides_retired_contracts_without_deleting_history(self):
        current = self.store.list("registry")[0]
        self.store.put("runs", "current-run", {"created_at": "2026-10-14T10:00:00Z",
            "contracts": [current], "window": Window.ending("2026-10-12").as_dict()})
        result = decoded(self.get("/registry"))
        self.assertEqual(result["all_contracts"], 1)
        self.assertEqual(result["contracts"][0]["contract_key"], current["key"])
        self.assertEqual(self.get("/incidents/incident-2").status, 200)
        self.assertEqual(len(self.store.list("registry")), 3)

    def test_gets_are_read_only_no_repository_calls_and_no_transcript_leaks(self):
        before = copy.deepcopy(self.store.objects)
        for endpoint in ("/overview", "/registry", "/runs", "/incidents", "/incidents/incident-1",
                         "/incidents/incident-1/evidence", "/incidents/incident-1/chart",
                         "/incidents/incident-1/audits"):
            with self.subTest(endpoint=endpoint):
                response = self.get(endpoint)
                self.assertEqual(response.status, 200)
                self.assertNotIn(b"NEVER LIST", response.body)
        self.assertEqual(before, self.store.objects)
        self.assertEqual(self.store.mutations, [])
        self.factory.assert_not_called()

    def test_sample_counts_distinct_from_content_cache_and_dispatch_ledger(self):
        items = []
        for index in range(40):
            item = {"item_id": "sample-" + str(index), "production_result_id": "source-" + str(index),
                    "connection_id": "connection-" + str(index), "job": "ACQUISITION",
                    "interaction_date": "2026-07-20"}
            items.append(item)
            self.store.put("judgments", "incident-1:" + item["item_id"],
                           {"incident_id": "incident-1", "item_id": item["item_id"], "cache_hit": index >= 2})
        self.store.objects["judgments"].pop("incident-1:item-1")
        incident = self.store.objects["incidents"]["incident-1"]
        incident["run_id"] = "run-1"
        manifest = {"incident_id": "incident-1", "contract_key": incident["contract_key"], "items": items}
        self.store.put("manifests", "manifest-1", manifest)
        self.store.put("manifests", "manifest-previous-version", dict(manifest, items=items[:20]))
        incident["sample_manifest"] = manifest
        self.store.budget.update(unique_transcripts=40, call_attribute_items=40, unique_transcript_hashes=2,
                                 transcript_attempts=2, successful_requests=2)
        result = decoded(self.get("/overview"))
        summary = result["sample_summary"]
        self.assertEqual(summary["sampled_unique_source_calls"], 40)
        self.assertEqual(summary["sampled_call_attribute_items"], 40)
        self.assertEqual(summary["saved_judgment_items"], 40)
        self.assertEqual(summary["cache_hit_judgment_items"], 38)
        self.assertEqual(result["budgets"]["unique_transcript_hashes"], 2)
        self.assertEqual(result["budgets"]["transcript_attempts"], 2)
        self.assertEqual(summary["scope"], "Latest saved run")

    def test_sample_summary_scopes_latest_run_and_preserves_multiple_attribute_items(self):
        incident = self.store.objects["incidents"]["incident-1"]
        incident["run_ids"] = ["run-1"]
        second = self.store.objects["incidents"]["incident-2"]
        second["run_id"] = "older-run"
        result = decoded(self.get("/overview"))["sample_summary"]
        self.assertEqual(result["sampled_unique_source_calls"], 1)
        self.assertEqual(result["saved_judgment_items"], 1)
        item = self.store.objects["manifests"]["manifest-1"]["items"][0]
        second["run_ids"] = ["older-run", "run-1"]
        self.store.put("manifests", "manifest-2", {"incident_id": "incident-2",
            "contract_key": second["contract_key"], "items": [dict(item, item_id="item-other-attribute")]})
        result = decoded(self.get("/overview"))["sample_summary"]
        self.assertEqual(result["sampled_unique_source_calls"], 1)
        self.assertEqual(result["sampled_call_attribute_items"], 2)

    def test_exact_ninety_days_accepted_ninety_one_rejected(self):
        good = self.get("/overview", {"start": ["2026-07-15"], "end": ["2026-10-12"]})
        bad = self.get("/overview", {"start": ["2026-07-14"], "end": ["2026-10-12"]})
        self.assertEqual(good.status, 200)
        self.assertEqual(bad.status, 400)

    def test_leap_and_dst_date_filters(self):
        for start, end in (("2024-02-01", "2024-04-30"), ("2025-10-05", "2026-01-02"),
                           ("2026-02-01", "2026-05-01")):
            with self.subTest(start=start):
                self.assertEqual(self.get("/overview", {"start": [start], "end": [end]}).status, 200)

    def test_invalid_incomplete_reversed_and_missing_dates(self):
        today = datetime.now(CENTRAL).date().isoformat()
        invalid = [
            {"start": ["2026-10-12"]}, {"end": ["2026-10-12"]},
            {"start": ["bad"], "end": ["2026-10-12"]},
            {"start": ["2026-10-12"], "end": ["2026-07-15"]},
            {"start": [today], "end": [today]},
        ]
        for query in invalid:
            with self.subTest(query=query):
                self.assertEqual(self.get("/overview", query).status, 400)

    def test_all_stage_detail_filters_cannot_override_saved_window(self):
        for suffix in ("", "/evidence", "/chart", "/audits"):
            with self.subTest(suffix=suffix):
                self.assertEqual(self.get("/incidents/incident-1" + suffix,
                                         {"start": ["2026-08-01"], "end": ["2026-08-02"]}).status, 400)

    def test_filters_job_channel_type_status_search_brand_and_decision(self):
        tests = [
            ({"job": ["ACQUISITION"]}, 1), ({"channel": ["chat"]}, 1), ({"type": ["NUMERIC"]}, 1),
            ({"status": ["Technical error"]}, 1), ({"search": ["same_name"]}, 2),
            ({"brand": ["TXUE"]}, 2), ({"decision": ["Audit"]}, 2), ({"attribute": ["measurement"]}, 1),
            ({"severity": ["high"]}, 2),
        ]
        for query, count in tests:
            with self.subTest(query=query):
                self.assertEqual(decoded(self.get("/overview", query))["total"], count)

    def test_filters_and_pagination_bounded(self):
        for query in ({"path": ["x"]}, {"url": ["https://example.com"]}, {"limit": ["0"]},
                      {"limit": ["2001"]}, {"offset": ["-1"]}, {"job": ["x", "y"]},
                      {"search": ["x" * 241]}, {"limit": ["nan"]}):
            with self.subTest(query=query):
                self.assertEqual(self.get("/overview", query).status, 400)
        result = decoded(self.get("/overview", {"limit": ["1"], "offset": ["1"]}))
        self.assertEqual(len(result["contracts"]), 1)
        self.assertEqual(result["total"], 3)

    def test_brand_filter_exact_supported_segment_not_substring(self):
        self.assertEqual(decoded(self.get("/overview", {"brand": ["TX"]}))["total"], 0)
        self.store.objects["incidents"]["incident-1"]["mix"] = {
            "brand": {"segments": [{"name": "TXUE", "baseline_rate": .1, "recent_rate": .2}]}}
        self.assertIn("TXUE", decoded(self.get("/overview"))["contracts"][0]["brands"])

    def test_detail_missing_or_malformed_ids(self):
        for endpoint in ("/incidents/missing", "/incidents/../secret", "/incidents/incident-1/transcript",
                         "/incidents/incident-1/retry", "/files/anything"):
            with self.subTest(endpoint=endpoint):
                self.assertEqual(self.get(endpoint).status, 404)

    def test_bad_saved_window_rejected(self):
        self.store.objects["incidents"]["incident-1"]["window"]["start"] = "2026-07-14"
        self.assertEqual(self.get("/incidents/incident-1").status, 409)

    def test_chart_is_exact_saved_spec_and_hash_bound(self):
        response = decoded(self.get("/incidents/incident-1/chart"))
        self.assertEqual(response["chart_spec"], self.store.get("charts", response["chart_hash"])["spec"])
        self.assertEqual(digest(response["chart_spec"]), response["chart_hash"])
        self.store.objects["charts"][response["chart_hash"]]["packet_hash"] = "a" * 64
        self.assertEqual(self.get("/incidents/incident-1/chart").status, 409)

    def test_chart_hash_corruption_and_unsafe_references_rejected(self):
        chart_id = self.store.objects["incidents"]["incident-1"]["evidence"]["chart_hash"]
        self.store.objects["charts"][chart_id]["spec"]["series"][0]["data"] = [40]
        self.assertEqual(self.get("/incidents/incident-1/chart").status, 409)
        self.store.objects["incidents"]["incident-1"]["evidence"]["chart_hash"] = "../../secret"
        self.assertEqual(self.get("/incidents/incident-1/chart").status, 409)

    def test_packet_hash_and_out_of_window_data_rejected(self):
        packet_id = self.store.objects["incidents"]["incident-1"]["evidence"]["packet_hash"]
        packet = self.store.objects["evidence"][packet_id]
        packet["daily"][0]["day"] = "2026-07-14"
        self.assertEqual(self.get("/incidents/incident-1/evidence").status, 409)

    def test_packet_window_and_incomplete_states(self):
        evidence = self.store.objects["incidents"]["incident-1"]["evidence"]
        evidence["packet_hash"] = "a" * 64
        self.assertEqual(self.get("/incidents/incident-1/evidence").status, 409)
        self.store.objects["incidents"]["incident-1"]["evidence"] = None
        self.assertEqual(self.get("/incidents/incident-1/chart").status, 409)

    def test_packet_contract_and_reference_integrity(self):
        ref = self.store.objects["incidents"]["incident-1"]["evidence"]
        packet = self.store.objects["evidence"][ref["packet_hash"]]
        packet["contract"] = self.store.list("registry")[1]
        packet["hash"] = digest({k: v for k, v in packet.items() if k not in {"hash", "id"}})
        new_hash = packet["hash"]
        self.store.put("evidence", new_hash, packet)
        ref["packet_hash"] = new_hash
        self.assertEqual(self.get("/incidents/incident-1/evidence").status, 409)

    def test_core_evidence_save_adapter_matches_api_contract(self):
        from attribute_health.evidence import build_packet, save_evidence
        incident = self.store.objects["incidents"]["incident-1"]
        contract = Contract.from_dict(incident["contract"])
        window = Window.from_dict(incident["window"])
        daily = [{"day": day.isoformat(), "eligible": 20, "valid": 19, "allowed_empty": 0,
                  "missing": 1, "invalid": 0, "categories": {"true": 5, "false": 14}}
                 for day in window.days()]
        packet = build_packet(contract, daily, {"status": "Watch", "signals": [],
                              "statistics": {"scan_adjusted_p": .5, "corrected_q": 1.}}, window)
        incident["evidence"] = save_evidence(self.store, incident["id"], packet, render=False)
        saved = self.get("/incidents/incident-1/evidence")
        chart = self.get("/incidents/incident-1/chart")
        self.assertEqual(saved.status, 200)
        self.assertEqual(chart.status, 200)
        self.assertEqual(decoded(saved)["hash"], decoded(chart)["packet_hash"])
        self.assertEqual(len(decoded(chart)["chart_spec"]["xAxis"][0]["data"]), 90)

    def test_lightweight_coverage_chart_is_readonly_frozen_drilldown(self):
        incident = self.store.objects["incidents"]["incident-1"]
        window = Window.from_dict(incident["window"])
        rows = [{"day": day.isoformat(), "eligible": 20, "valid": 20, "allowed_empty": 0,
                 "missing": 0, "invalid": 0, "categories": {"true": 5, "false": 15}}
                for day in window.days()]
        by_key = {incident["contract_key"]: rows}
        incident.update(status="Insufficient data", signals=[],
                        statistics={"scan_adjusted_p": 1., "corrected_q": 1.},
                        evidence={"daily_run_id": "coverage-run", "render_status": "on demand"})
        self.store.put("runs", "coverage-run", {"window": incident["window"], "contracts": [incident["contract"]]})
        self.store.put("daily", "coverage-run", {"window": incident["window"], "by_key": by_key, "hash": digest(by_key)})
        before = copy.deepcopy(self.store.objects)
        packet = self.get("/incidents/incident-1/evidence")
        chart = self.get("/incidents/incident-1/chart")
        self.assertEqual(packet.status, 200)
        self.assertEqual(chart.status, 200)
        self.assertEqual(decoded(packet)["hash"], decoded(chart)["packet_hash"])
        self.assertFalse(decoded(chart)["artifacts"]["available"])
        self.assertEqual(self.store.objects, before)
        self.factory.assert_not_called()

    def test_lightweight_coverage_rejects_modified_snapshot(self):
        incident = self.store.objects["incidents"]["incident-1"]
        incident["evidence"] = {"daily_run_id": "bad-run", "render_status": "on demand"}
        self.store.put("runs", "bad-run", {"window": incident["window"], "contracts": [incident["contract"]]})
        self.store.put("daily", "bad-run", {"window": incident["window"], "by_key": {}, "hash": "0" * 64})
        self.assertEqual(self.get("/incidents/incident-1/evidence").status, 409)

    def test_external_and_executable_chart_assets_rejected(self):
        for unsafe in ({"image": "file:///secret"}, {"formatter": "function(){return 1}"},
                       {"symbol": "image://https://example.com/x"}):
            with self.subTest(unsafe=unsafe):
                store = seeded()
                ref = store.objects["incidents"]["incident-1"]["evidence"]
                old = store.objects["charts"][ref["chart_hash"]]
                spec = dict(old["spec"], unsafe=unsafe)
                chart_id = digest(spec)
                store.put("charts", chart_id, {"hash": chart_id, "packet_hash": ref["packet_hash"], "spec": spec})
                ref["chart_hash"] = chart_id
                self.assertEqual(HealthService(Settings(), store).handle_get(
                    PREFIX + "/incidents/incident-1/chart", {}).status, 409)

    def test_disposition_persisted_idempotent_and_conflict_safe(self):
        payload = {"author": "Local reviewer", "outcome": OUTCOMES[0],
                   "action": "Needs human review", "note": "Observed disagreement, not causal proof."}
        first = self.post("/incidents/incident-1/dispositions", payload)
        again = self.post("/incidents/incident-1/dispositions", payload)
        self.assertEqual(first.status, 201)
        self.assertEqual(first.body, again.body)
        self.assertEqual(len(self.store.mutations), 1)
        self.assertEqual(self.post("/incidents/incident-1/dispositions",
                                  dict(payload, action="Resolved")).status, 409)
        detail = decoded(self.get("/incidents/incident-1"))
        self.assertEqual(detail["history"][0]["author"], "Local reviewer")

    def test_every_mutation_requires_idempotency_key(self):
        for key in (None, "", " ", "a" * 129, "../bad", "\x00"):
            with self.subTest(key=key):
                self.assertEqual(self.post("/incidents/incident-1/retry",
                    {"author": "A", "note": ""}, key).status, 400)
        self.assertFalse(self.store.mutations)

    def test_disposition_author_enum_note_and_unknown_fields_validation(self):
        base = {"author": "A", "outcome": OUTCOMES[0], "action": "Resolved", "note": ""}
        cases = [dict(base, author=""), dict(base, author=["A"]), dict(base, author="a" * 129),
                 dict(base, outcome="Healthy"), dict(base, action="Deploy"), dict(base, note=1),
                 dict(base, note="a" * 4001), dict(base, production_write=True)]
        for payload in cases:
            with self.subTest(payload=str(payload)[:100]):
                self.assertEqual(self.post("/incidents/incident-1/dispositions", payload).status, 400)
        self.assertFalse(self.store.mutations)

    def test_rubric_approval_version_action_and_contract_binding(self):
        contract_key = self.store.list("registry")[0]["key"]
        payload = {"contract_key": contract_key, "author": "A", "text": "Approved decision boundaries.",
                   "approved": False}
        self.assertEqual(self.post("/rubrics", payload).status, 201)
        self.assertEqual(self.post("/rubrics", payload).status, 201)
        self.assertEqual(len(self.store.mutations), 1)
        self.assertEqual(self.post("/rubrics", dict(payload, approved=True), "approval").status, 201)
        self.assertEqual(self.post("/rubrics", dict(payload, approved="true"), "invalid").status, 400)
        self.assertEqual(self.post("/rubrics", dict(payload, contract_key="../../anything"), "missing").status, 404)

    def test_item_review_accept_reject_uncertain_and_unknown_item(self):
        for decision in ("accept", "reject", "uncertain"):
            with self.subTest(decision=decision):
                response = self.post("/incidents/incident-1/items/item-1/review",
                    {"author": "A", "decision": decision, "note": "Human review"}, decision)
                self.assertEqual(response.status, 201)
        self.assertEqual(self.post("/incidents/incident-1/items/missing/review",
            {"author": "A", "decision": "accept", "note": ""}).status, 404)
        self.assertEqual(self.post("/incidents/incident-1/items/item-1/review",
            {"author": "A", "decision": "correct", "note": ""}).status, 400)

    def test_retry_eligibility_budgets_and_idempotent_queue_only(self):
        payload = {"author": "A", "note": "Resume failed work", "stage": "audit"}
        self.assertEqual(self.post("/incidents/incident-1/retry", payload).status, 409)
        first = self.post("/incidents/incident-2/retry", payload)
        self.assertEqual(first.status, 202)
        self.assertFalse(decoded(first)["dispatch_enabled"])
        self.assertEqual(first.body, self.post("/incidents/incident-2/retry", payload).body)
        self.assertEqual(self.post("/incidents/incident-2/retry", payload, "another").status, 409)
        self.factory.assert_not_called()

    def test_exhausted_budget_and_resolved_incident_cannot_queue(self):
        for field in ("transcript_attempts", "input_tokens", "output_tokens"):
            with self.subTest(field=field):
                self.store.budget[field] = self.store.budget["limits"][field]
                self.assertEqual(self.post("/incidents/incident-2/retry", {"author": "A", "note": ""}).status, 409)
                self.store.budget[field] = 0
        self.store.objects["incidents"]["incident-2"]["status"] = "Resolved"
        self.assertEqual(self.post("/incidents/incident-2/retry", {"author": "A", "note": ""}).status, 409)

    def test_retry_checks_relevant_stage_budget_not_unrelated_review_cap(self):
        self.store.budget["review_attempts"] = 20
        self.assertTrue(decoded(self.get("/incidents/incident-2"))["retry"]["eligible"])
        self.store.objects["incidents"]["incident-2"]["status"] = "AI review pending"
        self.assertFalse(decoded(self.get("/incidents/incident-2"))["retry"]["eligible"])

    def test_transcript_intentional_selected_bounded_read_not_persisted(self):
        before = copy.deepcopy(self.store.objects)
        response = self.post("/incidents/incident-1/items/item-1/transcript", {"author": "A", "confirm": True})
        self.assertEqual(response.status, 200)
        self.assertIn("Agent:", decoded(response)["transcript"])
        selected, window = self.repository.transcript.call_args.args
        self.assertEqual(selected["production_result_id"], "result-1")
        self.assertEqual(window.start.isoformat(), "2026-07-15")
        self.assertEqual(self.store.objects, before)
        self.assertFalse(self.store.mutations)

    def test_transcript_opt_in_author_and_payload_identity_restrictions(self):
        for payload in ({"author": "A", "confirm": False}, {"author": "A", "confirm": "yes"},
                        {"author": "", "confirm": True}, {"author": "A", "confirm": True, "id": "arbitrary"}):
            with self.subTest(payload=payload):
                self.assertEqual(self.post("/incidents/incident-1/items/item-1/transcript", payload).status, 400)
        self.assertEqual(self.post("/incidents/incident-1/items/missing/transcript",
                                  {"author": "A", "confirm": True}).status, 404)
        self.factory.assert_not_called()

    def test_transcript_saved_outside_window_and_mismatched_job_rejected_before_query(self):
        item = self.store.objects["manifests"]["manifest-1"]["items"][0]
        item["interaction_date"] = "2026-07-14"
        self.assertEqual(self.post("/incidents/incident-1/items/item-1/transcript",
                                  {"author": "A", "confirm": True}).status, 400)
        item["interaction_date"] = "2026-07-20"
        item["job"] = "SERVICE"
        self.assertEqual(self.post("/incidents/incident-1/items/item-1/transcript",
                                  {"author": "A", "confirm": True}).status, 409)
        self.factory.assert_not_called()

    def test_transcript_identity_mismatch_ambiguity_missing_and_truncated_rejected(self):
        original = self.repository.transcript.return_value
        cases = [dict(original, connection_id="other"), dict(original, production_result_id="wrong"),
                 dict(original, job="SERVICE"), dict(original, status="ambiguous"), dict(original, complete=False),
                 dict(original, transcript=""), dict(original, interaction_date="2026-07-21"),
                 dict(original, interaction_date="2026-07-14"), dict(original, status=None),
                 dict(original, completeness="truncated text")]
        for result in cases:
            with self.subTest(result=str(result)[:100]):
                self.repository.transcript.return_value = result
                self.assertIn(self.post("/incidents/incident-1/items/item-1/transcript",
                                       {"author": "A", "confirm": True}).status, {400, 409})

    def test_manifest_window_contract_and_hash_bound(self):
        manifest = self.store.objects["manifests"]["manifest-1"]
        manifest["window"]["start"] = "2026-07-16"
        self.assertEqual(self.get("/incidents/incident-1/audits").status, 409)
        manifest["window"]["start"] = "2026-07-15"
        manifest["contract_key"] = "not-this-contract"
        self.assertEqual(self.post("/incidents/incident-1/items/item-1/transcript",
                                  {"author": "A", "confirm": True}).status, 409)
        manifest.pop("contract_key")
        manifest["hash"] = "0" * 64
        self.assertEqual(self.get("/incidents/incident-1/audits").status, 409)
        self.factory.assert_not_called()

    def test_transcript_real_adapter_constructed_only_for_selected_read(self):
        self.store.objects["incidents"]["incident-1"]["mode"] = "real"
        self.store.objects["incidents"]["incident-1"]["contract"]["attribute_id"] = "real-1"
        self.store.objects["manifests"]["manifest-1"]["fixture"] = False
        service = HealthService(Settings(), self.store)
        with patch("attribute_health.repository.SnowflakeRepository", return_value=self.repository) as adapter:
            self.assertEqual(service.handle_get(PREFIX + "/incidents/incident-1/audits", {}).status, 200)
            adapter.assert_not_called()
            response = service.handle_post(PREFIX + "/incidents/incident-1/items/item-1/transcript",
                                           {"author": "A", "confirm": True}, "real-selected")
            self.assertEqual(response.status, 200)
            adapter.assert_called_once()

    def test_repository_errors_do_not_leak_gateway_or_credentials(self):
        self.repository.transcript.side_effect = RuntimeError("token=SECRET_GATEWAY_URL")
        response = self.post("/incidents/incident-1/items/item-1/transcript", {"author": "A", "confirm": True})
        self.assertEqual(response.status, 503)
        self.assertNotIn(b"SECRET", response.body)

    def test_transcript_default_fixture_adapter_is_not_live(self):
        item = self.store.objects["manifests"]["manifest-1"]["items"][0]
        item["production_result_id"] = "synthetic-5-1"
        service = HealthService(Settings(), self.store)
        with patch("attribute_health.repository.SnowflakeRepository") as live:
            response = service.handle_post(PREFIX + "/incidents/incident-1/items/item-1/transcript",
                                           {"author": "A", "confirm": True}, "fixture-read")
        self.assertEqual(response.status, 200)
        live.assert_not_called()

    def test_png_artifact_exact_incident_hash_only(self):
        root = Path(".ha-" + uuid.uuid4().hex[:8])
        self.store.root = root
        ref = self.store.objects["incidents"]["incident-1"]["evidence"]
        chart_hash = ref["chart_hash"]
        body = b"\x89PNG\r\n\x1a\nsynthetic fixture bytes"
        try:
            (root / "charts").mkdir(parents=True)
            (root / "charts" / (chart_hash + ".png")).write_bytes(body)
            self.store.put("chart_images", chart_hash, {"artifact": chart_hash + ".png",
                "packet_hash": ref["packet_hash"], "png_hash": hashlib.sha256(body).hexdigest()})
            response = self.get("/incidents/incident-1/chart-image/" + chart_hash + ".png")
            self.assertEqual(response.status, 200)
            self.assertEqual(response.body, body)
            self.assertEqual(response.content_type, "image/png")
            self.assertEqual(self.get("/incidents/incident-1/chart-image/" + "a" * 64 + ".png").status, 404)
            image = self.store.objects["chart_images"][chart_hash]
            original_hash = image["png_hash"]
            image["png_hash"] = "f" * 64
            self.assertEqual(self.get("/incidents/incident-1/chart-image/" + chart_hash + ".png").status, 409)
            image["png_hash"] = original_hash
            self.store.objects["chart_images"][chart_hash]["artifact"] = "../../outside.png"
            self.assertEqual(self.get("/incidents/incident-1/chart-image/" + chart_hash + ".png").status, 409)
        finally:
            shutil.rmtree(root)

    def test_png_corruption_and_query_paths_rejected(self):
        chart_hash = self.store.objects["incidents"]["incident-1"]["evidence"]["chart_hash"]
        response = self.get("/incidents/incident-1/chart-image/" + chart_hash + ".png", {"path": ["anything"]})
        self.assertEqual(response.status, 400)
        self.assertEqual(self.get("/incidents/incident-1/chart-image/" + chart_hash + ".png").status, 404)

    def test_non_object_and_oversized_action_payloads(self):
        self.assertEqual(self.post("/rubrics", ["bad"]).status, 400)
        self.assertEqual(self.post("/rubrics", {"text": "a" * MAX_BODY}).status, 413)


class SecurityHelpersTests(unittest.TestCase):
    def test_local_host_origin_and_peer_matrix(self):
        for peer, host in (("127.0.0.1", "127.0.0.1:9000"), ("127.0.0.1", "localhost:9000"),
                           ("::1", "[::1]:9000")):
            with self.subTest(host=host):
                local_request(peer, host, 9000, "http://" + host, True, "same-origin")
        cases = [
            ("192.168.1.2", "localhost:9000", "http://localhost:9000"),
            ("127.0.0.1", "evil.test:9000", "http://evil.test:9000"),
            ("127.0.0.1", "localhost.evil.test:9000", "http://localhost.evil.test:9000"),
            ("127.0.0.1", "localhost:9001", "http://localhost:9001"),
            ("127.0.0.1", "localhost:9000", None),
            ("127.0.0.1", "localhost:9000", "null"),
            ("127.0.0.1", "localhost:9000", "http://evil.test:9000"),
            ("127.0.0.1", "localhost:9000", "http://localhost:9001"),
            ("127.0.0.1", "localhost:9000", "https://localhost:9000"),
            ("127.0.0.1", "user@localhost:9000", "http://localhost:9000"),
            ("127.0.0.1", "localhost:9000/path", "http://localhost:9000"),
        ]
        for peer, host, origin in cases:
            with self.subTest(host=host, origin=origin):
                with self.assertRaises(HealthError):
                    local_request(peer, host, 9000, origin, True)
        with self.assertRaises(HealthError):
            local_request("127.0.0.1", "localhost:9000", 9000, "http://localhost:9000", True, "cross-site")

    def test_strict_json_rejects_duplicate_nonfinite_array_and_encoding(self):
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":Infinity}', b'[]', b'null', b'\xff', b'{'):
            with self.subTest(raw=raw):
                with self.assertRaises(HealthError):
                    strict_json(raw)
        self.assertEqual(strict_json(b'{"note":"safe"}'), {"note": "safe"})

class HealthTransactionalStoreTests(unittest.TestCase):
    """Exercise real Store transactions/schema in memory, with no state-directory writes."""

    def setUp(self):
        from attribute_health.store import Store
        self.db = sqlite3.connect(":memory:", isolation_level=None)
        self.db.row_factory = sqlite3.Row

        @contextmanager
        def connection(_store):
            yield self.db

        self.connection_patch = patch.object(Store, "_connection", connection)
        self.connection_patch.start()
        with patch.object(Path, "mkdir"):
            self.store = Store(Settings())
        seed = seeded()
        seed.objects["manifests"]["manifest-1"]["items"][0]["production_result_id"] = "synthetic-5-1"
        for kind, records in seed.objects.items():
            for identifier, payload in records.items():
                self.store.put(kind, identifier, payload)
        self.service = HealthService(Settings(), self.store)

    def tearDown(self):
        self.connection_patch.stop()
        self.db.close()

    def post(self, endpoint, payload, key="actual-store-test"):
        return self.service.handle_post(PREFIX + endpoint, payload, key)

    def test_disposition_transaction_idempotency_and_persisted_history(self):
        payload = {"author": "Synthetic local reviewer", "outcome": "Inconclusive",
                   "action": "Resolved", "note": "Fixture-only UI review."}
        response = self.post("/incidents/incident-1/dispositions", payload)
        self.assertEqual(response.status, 201)
        self.assertEqual(decoded(response), decoded(self.post("/incidents/incident-1/dispositions", payload)))
        self.assertEqual(len(self.store.list("dispositions")), 1)
        detail = decoded(self.service.handle_get(PREFIX + "/incidents/incident-1", {}))
        self.assertEqual(detail["status"], "Resolved")
        self.assertEqual(detail["history"][0]["author"], "Synthetic local reviewer")
        self.assertEqual(self.post("/incidents/incident-1/dispositions",
                                  dict(payload, action="Monitoring")).status, 409)

    def test_draft_and_mock_approval_rubric_versions_preserve_original_judgments(self):
        key = self.store.list("registry")[0]["key"]
        payload = {"author": "Synthetic test reviewer", "contract_key": key,
                   "text": "Synthetic boundaries, not a business approval.", "approved": False}
        before = self.store.list("judgments")
        draft = self.post("/rubrics", payload)
        self.assertEqual(draft.status, 201)
        self.assertEqual(decoded(draft)["version"], 1)
        approved = self.post("/rubrics", dict(payload, approved=True), "mock-test-approval")
        self.assertEqual(approved.status, 201)
        self.assertEqual(decoded(approved)["version"], 2)
        self.assertTrue(decoded(approved)["approved"])
        self.assertEqual(self.store.list("judgments"), before)
        self.assertEqual(len(self.store.list("rubrics")), 2)

    def test_item_review_real_colon_key_and_idempotent_history(self):
        payload = {"author": "Synthetic test reviewer", "decision": "uncertain", "note": "Fixture item."}
        response = self.post("/incidents/incident-1/items/item-1/review", payload)
        self.assertEqual(response.status, 201)
        self.assertEqual(decoded(response), decoded(self.post("/incidents/incident-1/items/item-1/review", payload)))
        audits = decoded(self.service.handle_get(PREFIX + "/incidents/incident-1/audits", {}))
        self.assertEqual(audits["item_reviews"][0]["decision"], "uncertain")
        self.assertEqual(len(audits["item_reviews"]), 1)

    def test_intentional_fixture_transcript_real_saved_manifest_no_dispatch_or_persistence(self):
        before = self.db.total_changes
        response = self.post("/incidents/incident-1/items/item-1/transcript",
                             {"author": "Synthetic test reviewer", "confirm": True})
        self.assertEqual(response.status, 200)
        payload = decoded(response)
        self.assertIn("Agent:", payload["transcript"])
        self.assertEqual(payload["identity"]["production_result_id"], "synthetic-5-1")
        self.assertEqual(self.db.total_changes, before)
        self.assertEqual(self.store.budget_summary()["transcript_attempts"], 0)

    def test_queue_retry_real_store_atomic_and_get_read_only(self):
        payload = {"author": "Synthetic test reviewer", "note": "", "stage": "audit"}
        response = self.post("/incidents/incident-2/retry", payload)
        self.assertEqual(response.status, 202)
        self.assertFalse(decoded(response)["dispatch_enabled"])
        self.assertEqual(decoded(response), decoded(self.post("/incidents/incident-2/retry", payload)))
        self.assertEqual(self.post("/incidents/incident-2/retry", payload, "other-key").status, 409)
        before = self.db.total_changes
        for endpoint in ("/overview", "/incidents/incident-1/chart", "/incidents/incident-1/audits"):
            self.assertEqual(self.service.handle_get(PREFIX + endpoint, {}).status, 200)
        self.assertEqual(self.db.total_changes, before)


class HealthHTTPTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import optic_console_server
        cls.module = optic_console_server
        cls.server = optic_console_server.ThreadingHTTPServer(("127.0.0.1", 0), optic_console_server.Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.authority = "127.0.0.1:" + str(cls.server.server_port)
        cls.origin = "http://" + cls.authority

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=3)

    def setUp(self):
        self.service = HealthService(Settings(), seeded(), Mock())
        self.service_patch = patch.object(self.module, "_health_service", self.service)
        self.service_patch.start()

    def tearDown(self):
        self.service_patch.stop()

    def request(self, path, method="GET", payload=None, headers=None, raw=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=5)
        body = raw if raw is not None else (json.dumps(payload).encode() if payload is not None else None)
        supplied = {"Content-Type": "application/json", "Origin": self.origin, "Idempotency-Key": "http-test"}
        if headers is not None:
            supplied.update(headers)
        supplied = {key: value for key, value in supplied.items() if value is not None}
        try:
            connection.request(method, path, body=body, headers=supplied)
            response = connection.getresponse()
            return response.status, response.read(), dict(response.getheaders())
        finally:
            connection.close()

    def test_existing_static_routes_and_health_navigation_preserved(self):
        for path, marker in (("/", b"OPTIC Console"), ("/model-lab", b"OPTIC Model Lab"),
                             ("/attribute-health", b"Attribute Health"),
                             ("/attribute-health/", b"Attribute Health")):
            with self.subTest(path=path):
                status, body, _ = self.request(path)
                self.assertEqual(status, 200)
                self.assertIn(marker, body)
        status, body, _ = self.request("/")
        self.assertIn(b'href="/attribute-health"', body)
        for route in ("/api/live", "/api/drift", "/api/value-spikes", "/api/expected-drift",
                      "/api/metadata", "/api/explorer", "/api/top-values"):
            self.assertIn(route, self.module.ROUTES)

    def test_get_overview_does_not_construct_or_call_model_lab_or_repository(self):
        with patch.object(self.module.MODEL_LAB, "handle_get", side_effect=AssertionError("Wrong route")):
            status, body, _ = self.request(PREFIX + "/overview")
        self.assertEqual(status, 200)
        self.assertFalse(json.loads(body)["safety"]["inference_enabled"])
        self.service.repository_factory.assert_not_called()

    def test_get_bad_host_and_remote_dns_rebinding_rejected(self):
        for host in ("evil.test:" + str(self.server.server_port), "localhost:9999"):
            with self.subTest(host=host):
                self.assertEqual(self.request(PREFIX + "/overview", headers={"Host": host})[0], 403)

    def test_post_same_origin_missing_cross_site_and_invalid_content_type(self):
        path = PREFIX + "/incidents/incident-2/retry"
        payload = {"author": "A", "note": ""}
        for headers, status in (({"Origin": None}, 403), ({"Origin": "http://evil.test"}, 403),
                                ({"Sec-Fetch-Site": "cross-site"}, 403), ({"Content-Type": "text/plain"}, 415),
                                ({"Idempotency-Key": None}, 400)):
            with self.subTest(headers=headers):
                self.assertEqual(self.request(path, "POST", payload, headers)[0], status)
        self.assertFalse(self.service.store.mutations)

    def test_post_duplicate_content_length_and_transfer_encoding_rejected(self):
        path = PREFIX + "/incidents/incident-2/retry"
        self.assertEqual(self.request(path, "POST", {"author": "A", "note": ""},
                                      {"Transfer-Encoding": "chunked"})[0], 400)
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=5)
        try:
            connection.putrequest("POST", path)
            for name, value in (("Origin", self.origin), ("Content-Type", "application/json"),
                                ("Idempotency-Key", "duplicate"), ("Content-Length", "2"),
                                ("Content-Length", "2")):
                connection.putheader(name, value)
            connection.endheaders(b"{}")
            response = connection.getresponse()
            self.assertEqual(response.status, 400)
            response.read()
        finally:
            connection.close()

    def test_json_limits_encoding_and_duplicate_fields(self):
        path = PREFIX + "/incidents/incident-2/retry"
        for raw, status in ((b'[]', 400), (b'{"author":"A","author":"B"}', 400),
                            (b'{"author":NaN}', 400), (b'\xff', 400), (b'a' * (MAX_BODY + 1), 413)):
            with self.subTest(raw=raw[:30]):
                self.assertEqual(self.request(path, "POST", raw=raw)[0], status)

    def test_duplicate_host_and_origin_headers_rejected(self):
        for field, value in (("Host", self.authority), ("Origin", self.origin)):
            with self.subTest(field=field):
                connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=5)
                try:
                    connection.putrequest("POST", PREFIX + "/incidents/incident-2/retry")
                    connection.putheader("Origin", self.origin)
                    connection.putheader(field, value)
                    if field == "Origin":
                        connection.putheader("Origin", value)
                    connection.putheader("Content-Length", "2")
                    connection.putheader("Content-Type", "application/json")
                    connection.putheader("Idempotency-Key", "duplicate")
                    connection.endheaders(b"{}")
                    response = connection.getresponse()
                    self.assertEqual(response.status, 403)
                    response.read()
                finally:
                    connection.close()

    def test_post_valid_review_and_readback(self):
        path = PREFIX + "/incidents/incident-1/dispositions"
        payload = {"author": "A", "outcome": "Inconclusive", "action": "Monitoring", "note": "Human review."}
        self.assertEqual(self.request(path, "POST", payload)[0], 201)
        status, body, headers = self.request(PREFIX + "/incidents/incident-1")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["history"][0]["author"], "A")
        self.assertEqual(headers["Cache-Control"], "no-store")

    def test_get_action_no_mutation_and_post_url_paths_rejected(self):
        self.assertEqual(self.request(PREFIX + "/incidents/incident-2/retry")[0], 404)
        self.assertEqual(self.request(PREFIX + "/incidents/incident-2/retry?path=secret",
                                     "POST", {"author": "A", "note": ""})[0], 400)
        self.assertFalse(self.service.store.mutations)

    def test_service_initialization_is_lazy_and_fail_closed(self):
        with patch.object(self.module, "_health_service", None), patch.object(
                self.module, "HealthService", side_effect=RuntimeError("SECRET")) as constructor:
            self.assertEqual(self.request("/")[0], 200)
            constructor.assert_not_called()
            status, body, _ = self.request(PREFIX + "/overview")
            self.assertEqual(status, 503)
            self.assertNotIn(b"SECRET", body)

    def test_html_no_dynamic_html_injection_or_disconnected_mock(self):
        source = Path(__file__).with_name("OPTIC_Attribute_Health.html").read_text(encoding="utf-8")
        self.assertNotIn("innerHTML", source)
        self.assertNotIn("insertAdjacentHTML", source)
        self.assertIn("textContent", source)
        self.assertIn("Idempotency-Key", source)
        self.assertIn("Refresh saved state", source)
        self.assertIn("DEMO · SYNTHETIC DATA", source)
        self.assertIn("/vendor/echarts.min.js", source)
        self.assertIn('rel="icon" href="data:image/svg+xml,', source)
        self.assertIn("unique sampled source calls", source)
        self.assertIn("distinct uncached transcript-content hashes", source)
        self.assertNotIn("+' unique transcripts", source)
        self.assertIn("Weighted disagreement by period", source)
        self.assertIn("Disagreement intervals by period", source)
        self.assertIn("Clicking Read full transcript explicitly opts in", source)
        self.assertNotIn("fetchButton.disabled=true;check.onchange", source)
        self.assertIn("compactLayout.addEventListener('change'", source)
        self.assertIn("HTMLFormElement.prototype.reset.call", source)
        self.assertNotIn('id="reset"', source)
        self.assertIn("1-days", source)
        for endpoint in ("/overview", "/registry", "/runs", "/chart", "/evidence", "/audits",
                         "/dispositions", "/rubrics", "/review", "/retry", "/transcript"):
            self.assertIn(endpoint, source)

    def test_html_javascript_syntax(self):
        try:
            from playwright._impl._driver import compute_driver_executable
        except ImportError:
            self.skipTest("Optional headless UI runtime not installed.")
        source = Path(__file__).with_name("OPTIC_Attribute_Health.html").read_text(encoding="utf-8")
        script = source.split("<script>", 1)[1].split("</script>", 1)[0]
        result = subprocess.run([str(compute_driver_executable()[0]), "--check", "-"],
                                input=script, text=True, encoding="utf-8", capture_output=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_browser_filter_reset_restores_rows_without_masked_form_method(self):
        try:
            from playwright.sync_api import sync_playwright, expect
        except ImportError:
            self.skipTest("Optional headless UI runtime not installed.")
        web_dir = Path(__file__).resolve().parent
        source = (web_dir / "OPTIC_Attribute_Health.html").read_bytes()
        vendor = (web_dir / "vendor" / "echarts.min.js").read_bytes()
        errors, paths = [], []

        def serve(route):
            parsed = urlparse(route.request.url)
            paths.append(parsed.path)
            if parsed.path == "/attribute-health":
                route.fulfill(status=200, body=source, content_type="text/html; charset=utf-8")
            elif parsed.path == "/vendor/echarts.min.js":
                route.fulfill(status=200, body=vendor, content_type="application/javascript")
            elif parsed.path.startswith(PREFIX + "/"):
                response = self.service.handle_get(parsed.path, parse_qs(parsed.query))
                route.fulfill(status=response.status, body=response.body, content_type=response.content_type)
            else:
                route.fulfill(status=404, body="No external request permitted.")

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True, channel="msedge")
            try:
                page = browser.new_page(viewport={"width": 1280, "height": 900})
                page.route("**/*", serve)
                page.on("pageerror", lambda error: errors.append(str(error)))
                page.goto("http://health.test/attribute-health")
                expect(page.locator("#rows tr")).to_have_count(3)
                page.fill("#author", "Synthetic browser reviewer")
                page.fill("#search", "does-not-exist")
                page.click("#filters button[type=submit]")
                expect(page.locator("#rows tr")).to_have_count(0)
                self.assertIn("No saved contracts match", page.locator("#empty").inner_text())
                page.click("#resetFilters")
                expect(page.locator("#rows tr")).to_have_count(3)
                self.assertEqual(page.locator("#search").input_value(), "")
                self.assertEqual(page.locator("#author").input_value(), "Synthetic browser reviewer")
                self.assertEqual(page.locator("#status").input_value(), "")
                self.assertFalse(errors, errors)
                self.assertNotIn("/favicon.ico", paths)
            finally:
                browser.close()

    def test_browser_mobile_populated_evidence_has_no_root_overflow_and_reachable_tabs(self):
        from playwright.sync_api import sync_playwright, expect
        from attribute_health.evidence import build_packet, save_evidence

        store = self.service.store
        incident = store.objects["incidents"]["incident-1"]
        window = Window.from_dict(incident["window"])
        signal = {"label": "true", "baseline_start": "2026-07-15", "baseline_end": "2026-08-11",
                  "recent_start": "2026-09-29", "recent_end": "2026-10-12", "onset": "2026-08-24",
                  "baseline_rate": .05, "recent_rate": .2, "change_pp": 15., "relative_change": 3.,
                  "baseline_n": 10000, "recent_n": 5000, "persistent": True, "direction": "increase"}
        statistics = {"scan_adjusted_p": .005, "corrected_q": .01}
        daily = [{"day": day.isoformat(), "eligible": 200, "valid": 198, "allowed_empty": 0,
                  "missing": 1, "invalid": 1, "categories": {"true": 40, "false": 158}}
                 for day in window.days()]
        incident.update(signals=[signal], statistics=statistics, mix={"brand": {
            "status": "common-support comparison", "mix_only_rate": .06,
            "baseline_standardized_rate": .05, "recent_standardized_rate": .2,
            "observed_common_support_rate": .2, "unsupported_recent_share": .02,
            "segments": [{"name": "TXUE", "baseline_n": 10000, "recent_n": 5000,
                          "baseline_rate": .05, "recent_rate": .2}]}},
            review={"decision": "Audit", "observed_pattern": "Sustained step change across full weeks.",
                    "rationale": "The supported movement merits an independent blinded audit.",
                    "supported_explanation": "Population mix does not fully explain the measured shift.",
                    "mode": "structured", "evidence": [{"metric_id": "signal:0:change_pp",
                                                       "date": "2026-10-12", "value": 15.}]})
        packet = build_packet(Contract.from_dict(incident["contract"]), daily, incident, window)
        incident["evidence"] = save_evidence(store, incident["id"], packet, render=False)
        web_dir = Path(__file__).resolve().parent
        source = (web_dir / "OPTIC_Attribute_Health.html").read_bytes()
        vendor = (web_dir / "vendor" / "echarts.min.js").read_bytes()
        errors = []

        def serve(route):
            parsed = urlparse(route.request.url)
            if parsed.path == "/attribute-health":
                route.fulfill(status=200, body=source, content_type="text/html; charset=utf-8")
            elif parsed.path == "/vendor/echarts.min.js":
                route.fulfill(status=200, body=vendor, content_type="application/javascript")
            elif parsed.path.startswith(PREFIX + "/"):
                response = self.service.handle_get(parsed.path, parse_qs(parsed.query))
                route.fulfill(status=response.status, body=response.body, content_type=response.content_type)
            else:
                route.fulfill(status=404, body="No external request permitted.")

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page(viewport={"width": 390, "height": 844})
                page.route("**/*", serve)
                page.on("pageerror", lambda error: errors.append(str(error)))
                page.goto("http://health.test/attribute-health")
                expect(page.locator("#rows tr")).to_have_count(3)
                page.locator("#rows .contract-name").filter(has_text="same_name").first.click()
                expect(page.locator("#chart canvas")).to_have_count(1)
                expect(page.locator("#review")).to_contain_text("Sustained step")
                expect(page.locator("#mix")).to_contain_text("TXUE")
                page.locator("#auditItems > *").first.wait_for(state="attached")
                page.locator("#chartMeta").evaluate("""node => node.append(
                    document.createTextNode(' Artifact reference: ' + '8'.repeat(64)))""")
                page.mouse.move(0, 0)
                for tab in ("tab-trend", "tab-audit", "tab-human"):
                    page.locator("#" + tab).click()
                    dimensions = page.evaluate("""() => ({
                        width:innerWidth, html:document.documentElement.scrollWidth,
                        body:document.body.scrollWidth,
                        tabs:[...document.querySelectorAll('[role=tab]')].map(n=>{
                            const r=n.getBoundingClientRect();return {id:n.id,left:r.left,right:r.right};
                        })
                    })""")
                    self.assertLessEqual(dimensions["html"], 390, dimensions)
                    self.assertLessEqual(dimensions["body"], 390, dimensions)
                    for bounds in dimensions["tabs"]:
                        self.assertGreaterEqual(bounds["left"], 0, dimensions)
                        self.assertLessEqual(bounds["right"], 390, dimensions)
                    controls = page.evaluate("""tab => [...document.querySelectorAll(
                        '#panel-'+tab.replace('tab-','')+' button, #panel-'+tab.replace('tab-','')+
                        ' select, #panel-'+tab.replace('tab-','')+' textarea')].filter(n=>
                        n.getClientRects().length).map(n=>{
                            const r=n.getBoundingClientRect();return {
                                name:n.id||n.textContent,left:r.left,right:r.right
                            };
                        })""", tab)
                    for bounds in controls:
                        self.assertGreaterEqual(bounds["left"], 0, bounds)
                        self.assertLessEqual(bounds["right"], 390, bounds)
                self.assertTrue(page.locator("#saveDisposition").is_visible())
                self.assertTrue(page.locator("#saveRubric").is_visible())
                self.assertFalse(errors, errors)
            finally:
                browser.close()


if __name__ == "__main__":
    unittest.main()
