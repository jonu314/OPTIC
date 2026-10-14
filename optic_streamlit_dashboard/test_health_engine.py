from __future__ import annotations

import json
import math
import tempfile
import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import numpy as np

from attribute_health.config import CENTRAL, Settings, Window
from attribute_health.contracts import Contract, aggregate_values, digest, normalize, resolve_contract
from attribute_health.detection import baseline_ageing, cluster_incidents, detect, explain_mix
from attribute_health.evidence import build_packet, chart_spec, render_png
from attribute_health.repository import FixtureRepository, ResearchRepository, SnowflakeRepository, identifier, resolve_identity
from attribute_health.replay import replay


END = date(2025, 10, 12)
WINDOW = Window(END - timedelta(days=89), END, "fixture watermark")


def boolean_series(p=.12, jump=None, post=.40, missing=False, volume=False, seed=4):
    rng = np.random.default_rng(seed)
    rows = []
    for i, day in enumerate(WINDOW.days()):
        n = (1200 if day.weekday() < 5 else 500) if not volume else (50 + i * 45)
        rate = p if jump is None or i < jump else post
        k = int(round(n * rate))
        rows += [{"day": str(day), "value": None if missing else True, "count": k, "brand": "X"},
                 {"day": str(day), "value": False, "count": n - k, "brand": "X"}]
    return rows


class WindowTests(unittest.TestCase):
    def test_exact_ninety_and_bounds(self):
        w = Window.ending("2026-10-12", now=datetime(2026, 10, 13, 12, tzinfo=CENTRAL))
        self.assertEqual(str(w.start), "2026-07-15")
        self.assertEqual(len(w.days()), 90)
        self.assertEqual(w.bounds()[1].date(), date(2026, 10, 13))

    def test_oversized_and_incomplete_rejected(self):
        with self.assertRaises(ValueError):
            Window(END - timedelta(days=90), END)
        with self.assertRaises(ValueError):
            Window.ending("2026-10-13", now=datetime(2026, 10, 13, 12, tzinfo=CENTRAL))

    def test_leap_arithmetic(self):
        w = Window.ending("2024-03-01", now=datetime(2024, 3, 2, tzinfo=CENTRAL))
        self.assertIn(date(2024, 2, 29), w.days())
        self.assertEqual(len(w.days()), 90)

    def test_central_utc_clock_boundary(self):
        w = Window.ending(now=datetime(2025, 10, 13, 2, tzinfo=timezone.utc))
        self.assertEqual(w.end, date(2025, 10, 11))

    def test_spring_dst_half_open_hours(self):
        w = Window(date(2025, 3, 9), date(2025, 3, 9))
        a, b = w.bounds(utc=True)
        self.assertEqual((b - a).total_seconds(), 23 * 3600)

    def test_fall_dst_half_open_hours(self):
        w = Window(date(2025, 11, 2), date(2025, 11, 2))
        a, b = w.bounds(utc=True)
        self.assertEqual((b - a).total_seconds(), 25 * 3600)

    def test_lag_recomputes_start(self):
        w = Window.ending("2026-10-12", now=datetime(2026, 10, 13, tzinfo=CENTRAL),
                          ready_through="2026-10-10", watermark="source")
        self.assertEqual(w.end, date(2026, 10, 10))
        self.assertEqual(w.start, date(2026, 7, 13))

    def test_naive_clock_and_out_of_window_rejected(self):
        with self.assertRaises(ValueError):
            Window.ending(now=datetime(2025, 10, 13))
        with self.assertRaises(ValueError):
            WINDOW.require(END + timedelta(days=1))

    def test_state_outside_repo(self):
        with self.assertRaises(ValueError):
            Settings(state_dir=Path(__file__).parent / "state")

    def test_settings_bounds(self):
        for kwargs in ({"workers": 5}, {"reserve_fraction": .2}, {"expansion_limit": 121},
                       {"simulations_min": 99, "simulations_max": 98}):
            with self.assertRaises(ValueError):
                Settings(**kwargs)


class ContractTests(unittest.TestCase):
    def setUp(self):
        self.bool = Contract("1", "flag", "ACQUISITION", "BOOL")

    def test_allowed_empty_not_false(self):
        c = replace(self.bool, result_type="BOOLEMPTY", allow_empty=True)
        self.assertEqual(normalize(c, "")["status"], "allowed_empty")
        self.assertEqual(normalize(self.bool, "")["status"], "missing")
        self.assertIs(normalize(c, "0")["value"], False)

    def test_absent_required_key_not_allowed_empty(self):
        self.assertEqual(normalize(replace(self.bool, allow_empty=True), "", present=False)["status"], "missing")

    def test_legitimate_other_separate_invalid(self):
        c = replace(self.bool, result_type="STRINGPICKONE", options=("Other", "Generic"))
        self.assertEqual(normalize(c, "Other")["status"], "valid")
        self.assertEqual(normalize(c, "unexpected")["status"], "invalid")

    def test_multilabel_prevalence_and_counts(self):
        c = replace(self.bool, result_type="LIST", options=("A", "B"))
        rows = aggregate_values(c, [{"day": str(END), "value": '["A","B"]', "count": 20}], WINDOW)
        self.assertEqual(rows[-1]["categories"], {"A": 20, "B": 20})
        self.assertEqual(rows[-1]["labels_per_call_sum"], 40)
        self.assertEqual(normalize(c, ["A", "bad"])["status"], "invalid")

    def test_numeric_finite_and_deferred(self):
        c = replace(self.bool, result_type="NUMERIC")
        self.assertEqual(normalize(c, "3.2")["status"], "valid")
        self.assertEqual(normalize(c, "nan")["status"], "invalid")
        self.assertEqual(c.as_dict()["monitoring_capability"]["numeric_distribution"], "deferred")
        self.assertEqual(replace(c, result_type="INTEGER").as_dict()["monitoring_capability"]["numeric_distribution"], "deferred")

    def test_unbounded_list_validity_without_invented_categories(self):
        c = replace(self.bool, result_type="LIST")
        self.assertEqual(normalize(c, '["arbitrary", "phrases"]')["status"], "valid")
        self.assertEqual(normalize(c, ["arbitrary"])["labels"], [])
        self.assertFalse(c.finite)

    def test_three_word_presence_not_semantics(self):
        c = replace(self.bool, result_type="STRING", allow_empty=True, value_mode="TEXT_PRESENCE_3_WORDS")
        rows = aggregate_values(c, [{"day": str(END), "value": ""},
                                   {"day": str(END), "value": "two words"},
                                   {"day": str(END), "value": "three distinct words"}], WINDOW)
        self.assertEqual(rows[-1]["categories"], {"text_short": 2, "text_present": 1})
        self.assertEqual(c.as_dict()["monitoring_capability"]["text_semantics"], "deferred")

    def test_prompt_empty_deterministically_resolved(self):
        c = resolve_contract({"attributeid": 9, "attributename": "home_warranty_pitched",
                              "jobname": "ACQUISITION", "resultvaluetype": "STRINGPICKONE",
                              "resultvalueoptions": ["Warranty"], "prompt": 'If no product pitched, return "".'})
        self.assertTrue(c.allow_empty)
        self.assertEqual(c.contract_status, "resolved")

    def test_conflicting_empty_explicit(self):
        c = resolve_contract({"attributeid": 9, "attributename": "x", "jobname": "SERVICE",
                              "resultvaluetype": "BOOL", "prompt": 'Return ""; never return blank.'})
        self.assertEqual(c.contract_status, "contract conflict")

    def test_taxonomy_not_hidden_ground_truth(self):
        c = resolve_contract({"attributeid": 9, "attributename": "call_type_acq", "jobname": "ACQUISITION",
                              "resultvaluetype": "STRINGPICKONE", "options": ["Generic", "Other"]})
        self.assertEqual(c.contract_status, "rubric approval required")

    def test_entity_key_identity_job_definition(self):
        self.assertNotEqual(self.bool.key, replace(self.bool, attribute_id="2").key)
        self.assertNotEqual(self.bool.key, replace(self.bool, job="SERVICE").key)
        self.assertNotEqual(self.bool.key, replace(self.bool, prompt="new contract").key)
        self.assertEqual(replace(self.bool, provenance={"updated": "now"}).key, self.bool.key)

    def test_calendar_gaps_undefined(self):
        rows = aggregate_values(self.bool, [{"day": str(END), "value": True}], WINDOW)
        self.assertEqual(len(rows), 90)
        self.assertEqual(rows[0]["eligible"], 0)
        packet = build_packet(self.bool, rows, {"status": "Insufficient data", "signals": [],
                              "statistics": {"scan_adjusted_p": 1., "corrected_q": 1.}}, WINDOW)
        spec = chart_spec(packet)
        self.assertIsNone(spec["series"][0]["data"][0])

    def test_input_window_boundary(self):
        with self.assertRaises(ValueError):
            aggregate_values(self.bool, [{"day": str(WINDOW.start - timedelta(days=1)), "value": 1}], WINDOW)


class IdentityTests(unittest.TestCase):
    def setUp(self):
        self.item = {"production_result_id": "42", "connection_id": "C", "job": "ACQUISITION",
                     "interaction_date": str(END) + "T12:00:00", "agent": "R"}
        self.row = {"production_result_id": "42", "analysis_result_id": "42", "raw_connection": "C",
                    "analysis_connection": "C", "raw_job": "ACQUISITION", "analysis_job": "ACQUISITION",
                    "raw_time": self.item["interaction_date"], "analysis_time": self.item["interaction_date"],
                    "analysis_agent": "r", "transcript_analysis_id": "9001", "analysis_content_id": "c1",
                    "content": "Agent: Full evidence."}

    def test_correct_two_distinct_id_relationships(self):
        out = resolve_identity(self.item, [self.row], WINDOW)
        self.assertEqual(out["status"], "resolved")
        self.assertEqual(out["transcript_analysis_id"], "9001")
        self.assertEqual(out["production_result_id"], "42")

    def test_wrong_id_collision_different_connection(self):
        out = resolve_identity(self.item, [{**self.row, "analysis_connection": "WRONG"}], WINDOW)
        self.assertEqual(out["status"], "inconsistent")

    def test_wrong_result_bridge(self):
        out = resolve_identity(self.item, [{**self.row, "analysis_result_id": "9001"}], WINDOW)
        self.assertEqual(out["status"], "inconsistent")

    def test_multicontent_different_text_unresolved(self):
        out = resolve_identity(self.item, [self.row, {**self.row, "analysis_content_id": "c2", "content": "chunk2"}], WINDOW)
        self.assertEqual(out["status"], "multi-content unresolved")

    def test_identical_duplicate_content_safe(self):
        out = resolve_identity(self.item, [self.row, {**self.row, "analysis_content_id": "c2"}], WINDOW)
        self.assertEqual(out["status"], "resolved")
        self.assertEqual(out["content_rows"], 2)

    def test_unique_consistent_fallback(self):
        out = resolve_identity(self.item, [], WINDOW, fallback_rows=[{**self.row, "analysis_result_id": "77"}])
        self.assertEqual(out["status"], "resolved")
        self.assertIn("unique", out["match_method"])

    def test_ambiguous_fallback_never_latest(self):
        out = resolve_identity(self.item, [], WINDOW, fallback_rows=[
            self.row, {**self.row, "transcript_analysis_id": "9002", "analysis_result_id": "77"}])
        self.assertEqual(out["status"], "ambiguous")

    def test_missing_and_truncated_not_supported(self):
        self.assertEqual(resolve_identity(self.item, [], WINDOW)["status"], "missing")
        self.assertEqual(resolve_identity(self.item, [{**self.row, "truncated": True}], WINDOW)["status"], "truncated")

    def test_missing_connection_not_a_verified_empty_match(self):
        item = {**self.item, "connection_id": None}
        row = {**self.row, "raw_connection": None, "analysis_connection": None}
        self.assertEqual(resolve_identity(item, [row], WINDOW)["status"], "missing")

    def test_time_and_job_consistency(self):
        for changes in ({"analysis_job": "SERVICE"}, {"analysis_time": str(END) + "T14:00:00"}):
            self.assertEqual(resolve_identity(self.item, [{**self.row, **changes}], WINDOW)["status"], "inconsistent")

    def test_selected_transcript_date_bound(self):
        with self.assertRaises(ValueError):
            resolve_identity({**self.item, "interaction_date": str(WINDOW.start - timedelta(days=1))},
                             [self.row], WINDOW)


class RepositoryTests(unittest.TestCase):
    def source_repo(self):
        repo = SnowflakeRepository(connection_factory=lambda: None)
        repo.apply_readiness({"ready_through": str(END), "deferred_jobs": [],
                              "jobs": {job: {"status": "ready"} for job in repo.settings.jobs}}, WINDOW)
        return repo

    def test_validated_fully_qualified_identifiers(self):
        self.assertEqual(identifier("DB.S.T"), '"DB"."S"."T"')
        for value in ("T", "D.S.T;DROP", "D.S.*"):
            with self.assertRaises(ValueError):
                identifier(value)

    def test_registry_active_ids_job_specific_no_max(self):
        repo = self.source_repo()
        sqls = []
        repo.query = lambda sql, params=(): sqls.append(sql) or [
            {"attributeid": 1, "member_attribute_id": 1, "attributename": "same", "jobname": "SERVICE", "resultvaluetype": "BOOL"},
            {"attributeid": 2, "member_attribute_id": 2, "attributename": "same", "jobname": "ACQUISITION", "resultvaluetype": "BOOL"}]
        registry = repo.registry()
        self.assertEqual(len(registry), 2)
        self.assertIn("COALESCE(p.ENABLED", sqls[0])
        self.assertIn("COALESCE(a.DISABLED", sqls[0])
        self.assertNotIn("MAX(d.PROMPT)", sqls[0])
        self.assertTrue(all(c.historical_scope == "current-contract retrospective" for c in registry))

    def test_duplicate_ids_same_name_job_mapping_review(self):
        repo = self.source_repo()
        repo.query = lambda sql, params=(): [
            {"attributeid": n, "member_attribute_id": n, "attributename": "same", "jobname": "SERVICE", "resultvaluetype": "BOOL"}
            for n in (1, 2)]
        self.assertTrue(all("ambiguous" in c.contract_status for c in repo.registry()))

    def test_aggregate_only_active_jobs_no_inactive_nulls(self):
        repo = self.source_repo()
        repo._columns = {"FLAG"}
        c = Contract("1", "flag", "ACQUISITION", "BOOL")
        inactive = replace(c, job="ASM")
        calls = []
        def query(sql, params=()):
            calls.append((sql, params))
            return [{"contract_key": c.key, "day": str(END), "value": "1", "count": 20, "brand": "X"}]
        repo.query = query
        values = repo.aggregate([c, inactive], WINDOW)
        self.assertEqual(values[c.key][-1]["eligible"], 20)
        self.assertEqual(sum(r["eligible"] for r in values[inactive.key]), 0)
        self.assertNotIn("ASM", calls[0][1])
        self.assertIn("PARTITION BY i.ID", calls[0][0])
        self.assertNotIn("PARTITION BY i.CONNECTION_ID", calls[0][0])

    def test_sample_bound_and_correct_reference_query(self):
        repo = self.source_repo()
        repo._columns = {"FLAG"}
        captured = []
        repo.query = lambda sql, params=(): captured.append((sql, params)) or []
        c = Contract("1", "flag", "ACQUISITION", "BOOL")
        repo.candidates(c, WINDOW, WINDOW.start, END, "true", False, limit=40)
        self.assertIn("LIMIT 40", captured[0][0])
        self.assertIn("COUNT(*) OVER", captured[0][0])
        self.assertNotIn("CONTENT", captured[0][0])
        with self.assertRaises(ValueError):
            repo.candidates(c, WINDOW, WINDOW.start - timedelta(days=1), END, "true", False)

    def test_write_rejected_before_connect(self):
        repo = SnowflakeRepository(connection_factory=lambda: self.fail("Connected"))
        with self.assertRaises(ValueError):
            repo.query("UPDATE table SET x=1")

    def test_fixture_no_dates_hardcoded(self):
        repo = FixtureRepository()
        contracts = repo.registry()
        daily = repo.aggregate(contracts, WINDOW)
        self.assertEqual(daily[contracts[0].key][0]["day"], str(WINDOW.start))

    def test_fixture_sample_population_matches_count_series(self):
        repo = FixtureRepository()
        contract = repo.registry()[0]
        daily = repo.aggregate([contract], WINDOW)[contract.key]
        sample = repo.candidates(contract, WINDOW, WINDOW.start, END, "true", False, 40)
        self.assertEqual(sample["population_count"], sum(r["categories"].get("true", 0) for r in daily))
        self.assertEqual(len({r["production_result_id"] for r in sample["rows"]}), 40)

    def test_parameterized_json_output_query_placeholder_shape(self):
        repo = self.source_repo()
        repo._columns = {"ODINDATA"}
        calls = []
        repo.query = lambda sql, params=(): calls.append((sql, params)) or []
        contract = Contract("1", "flag", "ACQUISITION", "BOOL")
        repo.aggregate([contract], WINDOW)
        repo.candidates(contract, WINDOW, WINDOW.start, END, "true", True)
        for sql, params in calls:
            self.assertEqual(sql.count("%s"), len(params))

    def test_real_dimension_join_is_exact_and_tied_conflicts_not_arbitrary(self):
        repo = self.source_repo()
        repo._columns = {"FLAG"}
        captured = []
        repo.query = lambda sql, params=(): captured.append(sql) or []
        repo.aggregate([Contract("1", "flag", "ACQUISITION", "BOOL")], WINDOW)
        sql = captured[0]
        for expected in ("b.ID=r.ID", "b.CONNECTION_ID", "b.JOBNAME", "DATEDIFF", "COUNT(DISTINCT LANGUAGECD)"):
            self.assertIn(expected, sql)


class DetectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.c = Contract("1", "flag", "ACQUISITION", "BOOL")
        cls.s = Settings(simulations_min=199, simulations_max=999)

    def run_series(self, source, **overrides):
        settings = replace(self.s, **overrides)
        daily = aggregate_values(self.c, source, WINDOW)
        return detect([self.c], {self.c.key: daily}, WINDOW, settings)[0]

    def test_stable_no_candidate(self):
        out = self.run_series(boolean_series())
        self.assertNotEqual(out["status"], "Candidate")
        self.assertEqual(out["statistics"]["scan_adjusted_p"], 1.)

    def test_sustained_common_jump(self):
        out = self.run_series(boolean_series(jump=56))
        self.assertEqual(out["status"], "Candidate")
        dates = [s["onset"] for s in out["signals"] if s["persistent"]]
        target = str(WINDOW.start + timedelta(days=56))
        self.assertIn(target, dates)

    def test_sustained_common_drop(self):
        out = self.run_series(boolean_series(p=.40, jump=56, post=.10))
        self.assertEqual(out["status"], "Candidate")
        self.assertIn("decrease", {s["direction"] for s in out["signals"]})

    def test_rare_disappearance(self):
        out = self.run_series(boolean_series(p=.006, jump=56, post=0))
        self.assertTrue(any(s["label"] == "true" and s["direction"] == "decrease" for s in out["signals"]))
        self.assertEqual(out["status"], "Candidate")

    def test_zero_baseline_no_infinite_change(self):
        out = self.run_series(boolean_series(p=0, jump=56, post=.02))
        signals = [s for s in out["signals"] if s["label"] == "true"]
        self.assertTrue(signals)
        self.assertTrue(any(s["relative_change"] is None for s in signals))
        self.assertTrue(all(s["relative_change"] is None or math.isfinite(s["relative_change"]) for s in signals))
        json.dumps(out, allow_nan=False)

    def test_changing_volume_uses_counts(self):
        out = self.run_series(boolean_series(jump=56, volume=True))
        self.assertEqual(out["status"], "Candidate")

    def test_one_day_spike_not_candidate(self):
        source = boolean_series()
        source[120]["count"] = 900
        source[121]["count"] = 300
        out = self.run_series(source)
        self.assertNotEqual(out["status"], "Candidate")

    def test_two_steps_data_driven(self):
        rows = []
        for i, day in enumerate(WINDOW.days()):
            n = 1500
            p = .05 if i < 35 else (.15 if i < 63 else .30)
            rows.extend([{"day": str(day), "value": True, "count": round(n * p)},
                         {"day": str(day), "value": False, "count": n - round(n * p)}])
        out = self.run_series(rows)
        found = {s["onset"] for s in out["signals"] if s["label"] == "true"}
        self.assertIn(str(WINDOW.start + timedelta(days=35)), found)
        self.assertIn(str(WINDOW.start + timedelta(days=63)), found)

    def test_gradual_drift_watch_or_candidate(self):
        rows = []
        for i, day in enumerate(WINDOW.days()):
            p = .10 + max(0, i - 30) * .002
            rows.extend([{"day": str(day), "value": True, "count": round(1500 * p)},
                         {"day": str(day), "value": False, "count": 1500 - round(1500 * p)}])
        out = self.run_series(rows)
        self.assertIn(out["status"], {"Watch", "Candidate"})
        self.assertEqual(out["statistics"]["diagnostics"]["ewma_lambda"], .2)

    def test_missing_invalid_surge_processing_route(self):
        out = self.run_series(boolean_series(p=0, jump=56, post=.06, missing=True))
        signals = [s for s in out["signals"] if s["label"] == "missing"]
        self.assertTrue(signals)
        self.assertTrue(all(s["processing_route"] for s in signals))

    def test_partial_blocks_not_double_confirmed(self):
        out = self.run_series(boolean_series(jump=85, post=.20))
        self.assertFalse(any(s["persistent"] for s in out["signals"]))
        self.assertNotEqual(out["status"], "Candidate")

    def test_severe_seven_day_expedite_still_statistically_calibrated(self):
        out = self.run_series(boolean_series(jump=83, post=.42))
        self.assertEqual(out["status"], "Candidate")
        self.assertTrue(any(s["severe"] for s in out["signals"]))
        self.assertGreater(out["statistics"]["simulations"], 0)

    def test_whole_categorical_vector_without_individual_practical_label(self):
        options = tuple(f"L{i}" for i in range(10))
        contract = Contract("vector", "category", "ACQUISITION", "STRINGPICKONE", options)
        rows = []
        for i, day in enumerate(WINDOW.days()):
            for j, label in enumerate(options):
                rate = .10 if i < 56 else (.112 if j < 5 else .088)
                rows.append({"day": str(day), "value": label, "count": round(2000 * rate)})
        daily = aggregate_values(contract, rows, WINDOW)
        out = detect([contract], {contract.key: daily}, WINDOW, self.s)[0]
        self.assertEqual(out["status"], "Candidate")
        self.assertTrue(any(s.get("distribution_tvd", 0) >= .05 for s in out["signals"]))

    def test_multilabel_prevalence_shift_not_share_summing_one(self):
        contract = Contract("multi", "labels", "ACQUISITION", "LIST", ("A", "B"))
        rows = []
        for i, day in enumerate(WINDOW.days()):
            count = 100 if i < 56 else 500
            rows.extend([{"day": str(day), "value": ["A", "B"], "count": count},
                         {"day": str(day), "value": ["B"], "count": 1000 - count}])
        daily = aggregate_values(contract, rows, WINDOW)
        out = detect([contract], {contract.key: daily}, WINDOW, self.s)[0]
        self.assertEqual(out["status"], "Candidate")
        self.assertTrue(any(s["label"] == "A" for s in out["signals"]))

    def test_invalid_surge_not_semantic_classification_error(self):
        rows = []
        for i, day in enumerate(WINDOW.days()):
            bad = 0 if i < 56 else 60
            rows.extend([{"day": str(day), "value": "UNEXPECTED", "count": bad},
                         {"day": str(day), "value": False, "count": 1000 - bad}])
        out = self.run_series(rows)
        self.assertTrue(any(s["label"] == "invalid" and s["processing_route"] for s in out["signals"]))

    def test_seed_reproducible_no_zero_p(self):
        a = self.run_series(boolean_series(jump=56))
        b = self.run_series(boolean_series(jump=56))
        self.assertEqual(a["statistics"], b["statistics"])
        self.assertGreater(a["statistics"]["scan_adjusted_p"], 0)

    def test_family_preserved_nonmoving_p_one(self):
        contracts = [replace(self.c, attribute_id=str(i)) for i in range(8)]
        moving = aggregate_values(contracts[0], boolean_series(jump=56), WINDOW)
        stable = aggregate_values(self.c, boolean_series(), WINDOW)
        data = {c.key: moving if i == 0 else stable for i, c in enumerate(contracts)}
        out = detect(contracts, data, WINDOW, self.s)
        self.assertTrue(all(r["statistics"]["family_size"] == 8 for r in out))
        self.assertTrue(all(r["statistics"]["scan_adjusted_p"] == 1 for r in out[1:]))
        self.assertEqual(out[0]["statistics"]["corrected_q"], min(1, 8 * out[0]["statistics"]["scan_adjusted_p"]))

    def test_353_family_minimum_7059_simulations(self):
        contracts = [replace(self.c, attribute_id=str(i)) for i in range(353)]
        out = detect(contracts, {}, WINDOW, replace(self.s, simulations_max=7058))
        self.assertFalse(out[0]["statistics"]["precision_sufficient"])
        out = detect(contracts, {}, WINDOW, replace(self.s, simulations_max=7059))
        self.assertTrue(out[0]["statistics"]["precision_sufficient"])

    def test_1059_family_cap_insufficient_precision(self):
        contracts = [replace(self.c, attribute_id=str(i)) for i in range(1059)]
        data = {contracts[0].key: aggregate_values(contracts[0], boolean_series(jump=56), WINDOW)}
        out = detect(contracts, data, WINDOW, replace(self.s, simulations_max=199))
        self.assertEqual(out[0]["status"], "Watch")
        self.assertFalse(out[0]["statistics"]["precision_sufficient"])
        self.assertIn("precision", out[0]["next_action"])

    def test_baseline_ageing_preserves_decision(self):
        incident = {"signals": [{"baseline_start": str(WINDOW.start - timedelta(days=1))}]}
        state = baseline_ageing(incident, WINDOW)
        self.assertFalse(state["compare_old_baseline"])
        self.assertTrue(state["preserve_prior_decision"])
        self.assertIn("no longer available", state["message"])

    def test_mix_only_expected_and_fixed_weight_comparison(self):
        rows = []
        for i, day in enumerate(WINDOW.days()):
            n_a, n_b = (900, 100) if i < 56 else (100, 900)
            for brand, n, p in (("A", n_a, .05), ("B", n_b, .35)):
                rows.extend([{"day": str(day), "brand": brand, "value": True, "count": round(n * p)},
                             {"day": str(day), "brand": brand, "value": False, "count": n - round(n * p)}])
        daily = aggregate_values(self.c, rows, WINDOW)
        out = detect([self.c], {self.c.key: daily}, WINDOW, self.s)[0]
        self.assertTrue(out["mix"]["brand"]["mix_supported"])
        self.assertAlmostEqual(out["mix"]["brand"]["baseline_standardized_rate"],
                               out["mix"]["brand"]["recent_standardized_rate"])
        self.assertFalse(out["mix"]["language"]["mix_supported"])

    def test_related_attributes_parent_incident(self):
        incidents = [{"id": str(i), "contract": {"job": "SERVICE"},
                      "signals": [{"onset": str(END - timedelta(days=i))}]} for i in range(2)]
        parents = cluster_incidents(incidents)
        self.assertEqual(len(parents), 1)
        self.assertEqual(incidents[0]["parent_id"], incidents[1]["parent_id"])

    def test_chronological_replay_uses_no_future_or_old_baseline(self):
        daily = aggregate_values(self.c, boolean_series(jump=56), WINDOW)
        early_end = WINDOW.start + timedelta(days=48)
        result = replay([self.c], {self.c.key: daily}, WINDOW, self.s, [early_end, END])
        self.assertEqual(result["evaluations"][0]["window"]["end"], str(early_end))
        for evaluation in result["evaluations"]:
            for detection in evaluation["detections"]:
                for signal in detection["signals"]:
                    self.assertLessEqual(signal["recent_end"], evaluation["window"]["end"])
                    self.assertGreaterEqual(signal["baseline_start"], evaluation["window"]["start"])
        self.assertIsNone(result["metrics"]["judge_human_agreement"])

    def test_noisy_weekday_stable_null_burden(self):
        false = 0
        for seed in range(10):
            rng = np.random.default_rng(seed)
            source = []
            for day in WINDOW.days():
                n = 1200 if day.weekday() < 5 else 600
                p = (.12 if day.weekday() < 5 else .17) + rng.normal(0, .008)
                k = int(rng.binomial(n, np.clip(p, .01, .99)))
                source.extend([{"day": str(day), "value": True, "count": k},
                               {"day": str(day), "value": False, "count": n - k}])
            false += self.run_series(source)["status"] == "Candidate"
        self.assertLessEqual(false, 1)


class EvidenceTests(unittest.TestCase):
    def test_packet_chart_and_hash_bound(self):
        c = Contract("1", "flag", "ACQUISITION", "BOOL")
        rows = aggregate_values(c, boolean_series(jump=56), WINDOW)
        detection = detect([c], {c.key: rows}, WINDOW, Settings(simulations_min=199, simulations_max=199))[0]
        packet = build_packet(c, rows, detection, WINDOW)
        self.assertEqual(packet["hash"], digest({k: v for k, v in packet.items() if k != "hash"}))
        spec = chart_spec(packet)
        self.assertEqual(spec, chart_spec(packet))
        self.assertEqual(spec["xAxis"][0]["data"][0], str(WINDOW.start))
        self.assertIn("Eligible calls", [s["name"] for s in spec["series"]])
        self.assertIn("7-day volume-weighted smoother", [s["name"] for s in spec["series"]])

    def test_packet_out_of_window_rejected(self):
        c = Contract("1", "flag", "ACQUISITION", "BOOL")
        with self.assertRaises(ValueError):
            build_packet(c, [{"day": str(END + timedelta(days=1))}], {}, WINDOW)


if __name__ == "__main__":
    unittest.main()
