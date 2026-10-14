import copy
import hashlib
import json
import unittest
from dataclasses import replace
from datetime import date, timedelta
from unittest.mock import Mock, patch

import requests

from attribute_health.ai import (
    AIError, FixtureClient, OdinClient, blinded_payload, compare_output, extraction_key,
    judge_extract, metric_ids, strict_json, validate_extraction, validate_identity, validate_review,
)
from attribute_health.config import Settings, Window
from attribute_health.contracts import Contract, digest
from attribute_health.repository import FixtureRepository
from attribute_health.sampling import build_manifest, expansion_needed, human_selection, summarize
from test_health_store import MemoryStoreCase


def detection(contract, window, persistent=True):
    return {"contract_key": contract.key, "status": "Candidate", "mix": {},
            "statistics": {"scan_adjusted_p": .001, "corrected_q": .004},
            "signals": [{"label": "true", "baseline_start": str(window.end - timedelta(days=48)),
                         "baseline_end": str(window.end - timedelta(days=21)),
                         "recent_start": str(window.end - timedelta(days=13)), "recent_end": str(window.end),
                         "onset": str(window.start + timedelta(days=56)), "direction": "increase",
                         "persistent": persistent, "severe": False, "baseline_rate": .12, "recent_rate": .43,
                         "change_pp": 31., "relative_change": .43 / .12 - 1,
                         "baseline_n": 20000, "recent_n": 10000, "methods": ["fixture"]}]}


class AuditValidationTests(unittest.TestCase):
    def setUp(self):
        self.contract = Contract("1", "flag", "ACQUISITION", "BOOL", prompt="True only for explicit offer.")
        self.transcript = "Agent: I offer the product.\nCustomer: Yes, please explain it."
        self.window = Window.ending(end="2026-10-12")
        self.extraction = {"extracted_value": True, "status": "Supported", "rationale": "Explicit offer.",
                           "quotes": [{"speaker": "Agent", "text": "I offer the product."}]}

    def test_blinded_payload_hides_all_production_and_period_context(self):
        payload = blinded_payload(self.contract, self.transcript)
        self.assertEqual(set(payload), {"transcript", "system_instructions", "attribute"})
        text = json.dumps(payload)
        for forbidden in ("stored_output", "production_result_id", "pre_flagged", "onset", "suspected", "candidate_model"):
            self.assertNotIn(forbidden, text)

    def test_unapproved_rubric_never_sent(self):
        draft = {"approved": False, "text": "Secret draft taxonomy"}
        self.assertNotIn("approved_rubric", blinded_payload(self.contract, self.transcript, draft))
        self.assertEqual(blinded_payload(self.contract, self.transcript, dict(draft, approved=True))[
            "approved_rubric"], "Secret draft taxonomy")

    def test_exact_speaker_quote_valid(self):
        self.assertEqual(validate_extraction(self.extraction, self.contract, self.transcript)["extracted_value"], True)

    def test_wrong_speaker_or_approximate_quote_rejected(self):
        for speaker, text in (("Customer", "I offer the product."), ("Agent", "I offered the product.")):
            value = dict(self.extraction, quotes=[{"speaker": speaker, "text": text}])
            with self.assertRaises(AIError):
                validate_extraction(value, self.contract, self.transcript)

    def test_absent_extracted_field_and_nonfinite_confidence_rejected(self):
        value = dict(self.extraction)
        value.pop("extracted_value")
        with self.assertRaises(AIError):
            validate_extraction(value, self.contract, self.transcript)
        with self.assertRaises(AIError):
            validate_extraction(dict(self.extraction, confidence=float("nan")), self.contract, self.transcript)

    def test_invalid_label_and_unsupported_without_quote(self):
        with self.assertRaises(AIError):
            validate_extraction(dict(self.extraction, extracted_value="maybe"), self.contract, self.transcript)
        with self.assertRaises(AIError):
            validate_extraction(dict(self.extraction, quotes=[]), self.contract, self.transcript)

    def test_allowed_empty_distinct_from_missing_and_false(self):
        contract = replace(self.contract, result_type="BOOLEMPTY", allow_empty=True)
        extraction = dict(self.extraction, extracted_value="")
        self.assertEqual(compare_output(contract, "", extraction)["comparison"], "agreement")
        self.assertEqual(compare_output(contract, False, extraction)["comparison"], "disagreement")
        self.assertEqual(compare_output(contract, "", extraction, present=False)["comparison"], "production_missing")

    def test_uncertain_neither_correct_nor_incorrect(self):
        extraction = dict(self.extraction, status="Insufficient evidence", extracted_value=None, quotes=[])
        self.assertEqual(compare_output(self.contract, True, extraction)["comparison"], "uncertain")

    def test_multi_label_normalized_independently(self):
        contract = replace(self.contract, result_type="LIST", options=("Billing", "Move"))
        extraction = dict(self.extraction, extracted_value=["Move", "Billing"])
        self.assertEqual(compare_output(contract, '["Billing","Move"]', extraction)["comparison"], "agreement")

    def test_cache_changes_contract_rubric_transcript_and_prompt(self):
        key = extraction_key(self.contract, "hash")
        self.assertNotEqual(key, extraction_key(self.contract, "other"))
        self.assertNotEqual(key, extraction_key(replace(self.contract, prompt="changed"), "hash"))
        self.assertNotEqual(key, extraction_key(self.contract, "hash", {"rubric_hash": "approved"}))
        with patch("attribute_health.ai.JUDGE_PROMPT_VERSION", "next"):
            self.assertNotEqual(key, extraction_key(self.contract, "hash"))
        with self.assertRaises(ValueError):
            extraction_key(self.contract, "hash", model="gpt-5.5")

    def test_ambiguous_contract_does_not_call_inference(self):
        client = Mock()
        result = judge_extract(client, replace(self.contract, contract_status="rubric approval required"),
                               self.transcript, digest(self.transcript), Settings(), item_ref="item")
        self.assertEqual(result["status"], "Ambiguous contract")
        client.call.assert_not_called()

    def test_identity_statuses_are_not_graded(self):
        item = {"interaction_date": "2026-10-01", "production_result_id": "1",
                "connection_id": "c", "job": "ACQUISITION"}
        for status in ("missing", "ambiguous", "inconsistent", "multi-content unresolved", "truncated"):
            self.assertEqual(validate_identity(item, {"status": status}, self.window), status)

    def test_identity_collision_hash_and_outside_date_rejected(self):
        item = {"interaction_date": "2026-10-01", "production_result_id": "1", "connection_id": "c", "job": "ACQUISITION"}
        resolved = dict(item, status="resolved", transcript=self.transcript, transcript_hash=digest(self.transcript),
                        transcript_analysis_id="a", analysis_content_id="content", match_method="exact ID")
        self.assertEqual(validate_identity(item, resolved, self.window), "resolved")
        self.assertEqual(validate_identity(item, dict(resolved, connection_id="different"), self.window), "inconsistent")
        with self.assertRaises(AIError):
            validate_identity(item, dict(resolved, transcript_hash="wrong"), self.window)
        with self.assertRaises(ValueError):
            validate_identity(dict(item, interaction_date="2026-01-01"), resolved, self.window)

    def test_reviewer_exact_schema_metric_date_hash(self):
        packet = {"hash": "a" * 64, "metrics": {"measured": .1}}
        review = {"decision": "Audit", "packet_hash": packet["hash"], "observed_pattern": "Measured movement",
                  "evidence": [{"metric_id": "measured", "date": "2026-10-01"}],
                  "supported_explanation": "Not explained", "unresolved_questions": [],
                  "recommended_strata": [], "rationale": "Audit"}
        self.assertEqual(validate_review(review, packet, self.window), review)
        for changed in (
                dict(review, packet_hash="b" * 64), dict(review, evidence=[{"metric_id": "invented", "date": "2026-10-01"}]),
                dict(review, evidence=[{"metric_id": "measured", "date": "2026-01-01"}]),
                dict(review, invented="fact")):
            with self.assertRaises(AIError):
                validate_review(changed, packet, self.window)

    def test_strict_json_no_markdown_extra_trailing_or_nan(self):
        for invalid in ("```json\n{}\n```", "{} trailing", "[]", '{"value": NaN}'):
            with self.assertRaises(AIError):
                strict_json(invalid)

    def test_fixture_client_requires_explicit_flag(self):
        with self.assertRaises(ValueError):
            FixtureClient()

    def test_metric_embedded_date_must_match_review_date(self):
        packet = {"hash": "a" * 64, "metrics": {"daily:2026-10-01:eligible": 200}}
        review = {"decision": "Audit", "packet_hash": packet["hash"], "observed_pattern": "Measured movement",
                  "evidence": [{"metric_id": "daily:2026-10-01:eligible", "date": "2026-10-12"}],
                  "supported_explanation": "Unexplained", "unresolved_questions": [],
                  "recommended_strata": [], "rationale": "Audit"}
        with self.assertRaises(AIError) as caught:
            validate_review(review, packet, self.window)
        self.assertEqual(caught.exception.code, "review_metric_date_mismatch")

    def test_packet_object_id_is_not_a_metric(self):
        self.assertEqual(metric_ids({"id": "packet-hash", "contract": {"id": "contract-id"},
                                     "metrics": {"measured": .1}}), {"measured"})

    def test_timestamped_and_structured_speaker_turns(self):
        transcript = "[00:04] Agent: I offer the product.\n00:05 Customer: Yes, please explain it."
        validate_extraction(self.extraction, self.contract, transcript)
        transcript = json.dumps([{"speaker": "Agent", "text": "I offer the product."}])
        validate_extraction(self.extraction, self.contract, transcript)


class OdinTransportTests(MemoryStoreCase):
    def setUp(self):
        super().setUp()
        self.settings = replace(self.settings, ai_enabled=True, max_attempts=3)
        self.store = self.new_store(ai_enabled=True, max_attempts=3)
        self.session = Mock()
        self.client = OdinClient(self.settings, self.store, session=self.session, sleep=Mock())
        self.token_patch = patch.dict("os.environ", {"ODIN_API_TOKEN": "Bearer mock-token"})
        self.token_patch.start()
        self.addCleanup(self.token_patch.stop)

    def response(self, status=200, usage=True):
        payload = {"choices": [{"message": {"content": "{}"}}]}
        if usage:
            payload["usage"] = {"prompt_tokens": 5, "completion_tokens": 2}
        return Mock(status_code=status, headers={}, json=Mock(return_value=payload))

    def call(self, **kwargs):
        return self.client.call("gpt-6.1-sol", "system", "user", category="judge", item_ref="item", **kwargs)

    def test_request_shape_token_prefix_and_no_temperature(self):
        self.session.post.return_value = self.response()
        result = self.call()
        kwargs = self.session.post.call_args.kwargs
        self.assertNotIn("temperature", kwargs["json"])
        self.assertEqual(kwargs["json"]["reasoning_effort"], "low")
        self.assertEqual(kwargs["headers"]["Authorization"], "Bearer mock-token")
        self.assertFalse(kwargs["allow_redirects"])
        self.assertIn("/gpt-6.1-sol/chat/completions?api-version=2025-04-01-preview",
                      self.session.post.call_args.args[0])
        self.assertEqual(result["mode"], "structured")

    def test_401_and_403_fail_without_retry_or_raw_error(self):
        for status in (401, 403):
            self.session.post.reset_mock()
            self.session.post.return_value = self.response(status)
            with self.assertRaises(AIError) as caught:
                self.call()
            self.assertEqual(self.session.post.call_count, 1)
            self.assertEqual(str(caught.exception), "odin_http_" + str(status))

    def test_429_then_success_reserves_each_attempt(self):
        self.session.post.side_effect = [self.response(429), self.response()]
        self.assertEqual(self.call()["retries"], 1)
        summary = self.store.budget_summary()
        self.assertEqual((summary["transcript_attempts"], summary["retries"]), (2, 1))
        self.assertEqual(summary["uncertain_usage_requests"], 1)

    def test_timeout_retries_conservative_usage(self):
        self.session.post.side_effect = [requests.Timeout(), self.response()]
        self.call()
        self.assertEqual(self.store.budget_summary()["uncertain_usage_requests"], 1)

    def test_missing_usage_charges_reservation(self):
        self.session.post.return_value = self.response(usage=False)
        self.call()
        self.assertEqual(self.store.budget_summary()["uncertain_usage_requests"], 1)

    def test_disabled_live_never_posts(self):
        self.client.settings = replace(self.settings, ai_enabled=False)
        with self.assertRaises(AIError):
            self.call()
        self.session.post.assert_not_called()

    def test_image_mismatch_and_unverified_capability(self):
        image = {"packet_hash": "hash", "bytes": b"\x89PNG\r\n\x1a\nmock"}
        with self.assertRaises(AIError):
            self.call(image=image, packet_hash="hash")
        self.client.settings = replace(self.settings, image_verified=True)
        with self.assertRaises(AIError):
            self.call(image=image, packet_hash="different")
        self.session.post.assert_not_called()

    def test_verified_image_adapter(self):
        self.client.settings = replace(self.settings, image_verified=True)
        self.session.post.return_value = self.response()
        result = self.call(image={"packet_hash": "hash", "bytes": b"\x89PNG\r\n\x1a\nmock"}, packet_hash="hash")
        self.assertEqual(result["mode"], "image+structured")
        self.assertEqual(self.session.post.call_args.kwargs["json"]["messages"][1]["content"][1]["type"], "image_url")


class SamplingTests(unittest.TestCase):
    def setUp(self):
        self.window = Window.ending(end="2026-10-12")
        self.repository = FixtureRepository()
        self.contract = self.repository.registry()[0]
        self.daily = self.repository.aggregate([self.contract], self.window)[self.contract.key]
        self.detection = detection(self.contract, self.window)

    def manifest(self, **kwargs):
        return build_manifest(self.repository, self.contract, self.detection, self.window, daily=self.daily, seed=7, **kwargs)

    def test_pilot_15_15_5_5_deterministic_unique_metadata_only(self):
        first = self.manifest()
        self.assertEqual(first, self.manifest())
        self.assertEqual([x["selected"] for x in first["strata"]], [15, 15, 5, 5])
        self.assertEqual(len({x["production_result_id"] for x in first["items"]}), 40)
        self.assertTrue(first["fixture"])
        self.assertTrue(all("transcript" not in x for x in first["items"]))
        self.assertTrue(all(x["selection_probability"] > 0 for x in first["items"]))

    def test_expansion_preserves_pilot_and_saved_probabilities(self):
        pilot = self.manifest()
        expanded = self.manifest(previous=pilot, total=120)
        self.assertEqual([x["selected"] for x in expanded["strata"]], [45, 45, 15, 15])
        self.assertTrue({x["item_id"] for x in pilot["items"]} <= {x["item_id"] for x in expanded["items"]})
        self.assertEqual(pilot["target"], 40)
        self.assertEqual(len(pilot["items"]), 40)

    def test_shortfalls_not_outside_window_replacements(self):
        with patch.object(self.repository, "candidates", return_value={
                "rows": [], "population_count": 0, "truncated": False, "selection_method": "seeded uniform"}):
            manifest = self.manifest()
        self.assertEqual(sum(x["shortfall"] for x in manifest["strata"]), 40)
        self.assertEqual(manifest["items"], [])

    def test_reject_outside_window_and_nonuniform_pool(self):
        changed = copy.deepcopy(self.detection)
        changed["signals"][0]["baseline_start"] = "2026-01-01"
        with self.assertRaises(ValueError):
            build_manifest(self.repository, self.contract, changed, self.window)
        with patch.object(self.repository, "candidates", return_value={
                "rows": [], "population_count": 0, "selection_method": "latest convenient records"}):
            with self.assertRaises(ValueError):
                self.manifest()

    def test_population_change_cannot_replace_saved_pilot(self):
        pilot = self.manifest()
        original = self.repository.candidates
        def changed(*args, **kwargs):
            result = original(*args, **kwargs)
            result["population_count"] += 1
            return result
        with patch.object(self.repository, "candidates", side_effect=changed):
            with self.assertRaises(ValueError):
                self.manifest(previous=pilot, total=120)

    def test_stratified_weighted_not_oversampled_accuracy(self):
        manifest = self.manifest()
        for item in manifest["items"]:
            item["weight"] = 1 if item["stratum"].endswith("flagged") else 100
        judgments = [dict(incident_id="i", item_id=x["item_id"], status="Supported", extracted_value=False,
                          comparison="disagreement" if x["stratum"].endswith("flagged") else "agreement")
                     for x in manifest["items"]]
        summary = summarize(manifest, judgments, seed=1, simulations=100)
        self.assertLess(summary["periods"]["pre"]["disagreement_rate"], .05)
        self.assertNotEqual(summary["periods"]["pre"]["disagreement_rate"], .75)
        self.assertFalse(summary["human_review"]["ground_truth_calibrated"])
        self.assertEqual(len(summary["human_review_items"]), 20)

    def test_uncertainty_unresolved_and_boundary_intervals(self):
        manifest = self.manifest()
        judgments = [dict(item_id=x["item_id"], status="Supported", comparison="agreement", extracted_value=x["stored_output"])
                     for x in manifest["items"]]
        summary = summarize(manifest, judgments, simulations=100)
        self.assertGreater(summary["periods"]["pre"]["disagreement_interval"][1], 0)
        judgments[0].update(status="Ambiguous contract", comparison="uncertain")
        judgments.pop()
        summary = summarize(manifest, judgments, simulations=100)
        self.assertEqual(summary["periods"]["pre"]["ambiguity_n"], 1)
        self.assertEqual(summary["periods"]["post"]["unresolved_n"], 1)
        self.assertTrue(expansion_needed(summary))
        self.assertFalse(summary["complete"])

    def test_human_selection_balances_outcomes_and_periods(self):
        rows = [dict(item_id=str(i), period="pre" if i % 2 else "post",
                     comparison=("agreement", "disagreement", "uncertain")[i % 3]) for i in range(40)]
        first = human_selection(rows, count=20, seed=3)
        self.assertEqual(first, human_selection(rows, count=20, seed=3))
        self.assertEqual(len(set(first)), 20)
