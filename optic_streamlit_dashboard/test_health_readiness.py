import copy
import unittest
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from attribute_health.config import CENTRAL, Settings, Window
from attribute_health.contracts import Contract
from attribute_health.readiness import READINESS_DEFAULTS, _settings, evaluate_job_readiness


def readiness_evidence(window, contracts, volume=1000):
    jobs = {x.job for x in contracts}
    return {"days": [{"job": job, "day": day.isoformat(), "n": volume, "nonnull_ids": volume,
                      "identity_conflicts": 0, "watermark": day.isoformat() + "T23:59:59"}
                     for job in jobs for day in window.days()],
            "attributes": [{"job": contract.job, "contract_key": contract.key,
                            "day": day.isoformat(), "n": volume, "present": volume}
                           for contract in contracts for day in window.days()],
            "unmapped_contracts": []}


class ReadinessTests(unittest.TestCase):
    def setUp(self):
        self.window = Window.ending(end="2026-10-12")
        self.settings = SimpleNamespace(**READINESS_DEFAULTS)
        self.contracts = [Contract("a", "offer", "ACQUISITION", "BOOL", prompt="Explicit product offer."),
                          Contract("s", "issue", "SERVICE", "BOOL", prompt="Explicit unresolved issue.")]
        self.evidence = readiness_evidence(self.window, self.contracts)
        self.clock = patch("attribute_health.readiness._today", return_value=date(2026, 10, 13))
        self.clock.start()
        self.addCleanup(self.clock.stop)

    def evaluate(self, jobs=("ACQUISITION", "SERVICE")):
        return evaluate_job_readiness(self.window, self.settings, jobs, self.evidence, self.contracts)

    def current(self, job):
        return next(x for x in self.evidence["days"] if x["job"] == job and x["day"] == "2026-10-12")

    def attribute(self, key, day="2026-10-12"):
        return next(x for x in self.evidence["attributes"] if x["contract_key"] == key and x["day"] == day)

    def test_ready_jobs_use_independent_supported_evidence(self):
        result = self.evaluate()
        self.assertEqual(result["ready_through"], "2026-10-12")
        self.assertEqual(result["deferred_jobs"], [])
        self.assertFalse(result["automatic_completeness_claim"])
        self.assertEqual(result["probe_window"], self.window.as_dict())
        self.assertEqual(result["jobs"]["ACQUISITION"]["checks"]["volume"]["baseline_median"], 1000)

    def test_partial_acquisition_not_hidden_by_ready_service_or_chat(self):
        self.current("ACQUISITION").update(n=100, nonnull_ids=100)
        self.attribute(self.contracts[0].key).update(n=100, present=100)
        self.evidence["days"].append(dict(self.current("SERVICE"), job="SERVICECHATTX", n=10000, nonnull_ids=10000))
        result = self.evaluate()
        self.assertEqual(result["ready_through"], "2026-10-11")
        self.assertEqual(result["jobs"]["ACQUISITION"]["proposed_end_status"], "incomplete")
        self.assertEqual(result["jobs"]["SERVICE"]["proposed_end_status"], "ready")
        self.assertEqual(result["deferred_jobs"], [])

    def test_lagging_job_rolls_back_three_complete_days(self):
        for row in self.evidence["days"]:
            if row["job"] == "ACQUISITION" and row["day"] >= "2026-10-10":
                row.update(n=100, nonnull_ids=100)
        for row in self.evidence["attributes"]:
            if row["job"] == "ACQUISITION" and row["day"] >= "2026-10-10":
                row.update(n=100, present=100)
        result = self.evaluate()
        self.assertEqual(result["ready_through"], "2026-10-09")
        effective = Window.ending(end=result["ready_through"])
        self.assertEqual(effective.start, date(2026, 7, 12))
        self.assertEqual(len(effective.days()), 90)

    def test_configured_lag_is_a_central_calendar_bound(self):
        self.settings.source_lag_days = 3
        result = self.evaluate()
        self.assertEqual(result["proposed_end"], "2026-10-10")
        self.assertEqual(result["ready_through"], "2026-10-10")

    def test_missing_enabled_job_explicitly_deferred(self):
        self.evidence["days"] = [x for x in self.evidence["days"] if x["job"] == "SERVICE"]
        result = self.evaluate()
        self.assertEqual(result["deferred_jobs"], ["ACQUISITION"])
        self.assertEqual(result["jobs"]["ACQUISITION"]["proposed_end_status"], "unknown")
        self.assertEqual(result["ready_through"], "2026-10-12")

    def test_missing_all_evidence_never_claims_complete(self):
        self.evidence = {}
        result = self.evaluate()
        self.assertEqual(result["deferred_jobs"], ["ACQUISITION", "SERVICE"])
        self.assertIsNone(result["watermark"])
        self.assertEqual(result["ready_through"], "2026-10-12")

    def test_missing_id_fraction_is_unknown_even_with_volume(self):
        self.current("ACQUISITION").pop("nonnull_ids")
        result = self.evaluate()
        self.assertEqual(result["jobs"]["ACQUISITION"]["checks"]["identity"]["status"], "unknown")
        self.assertIn("ACQUISITION", result["deferred_jobs"])

    def test_missing_conflict_evidence_is_unknown(self):
        self.current("ACQUISITION").pop("identity_conflicts")
        self.assertIn("ACQUISITION", self.evaluate()["deferred_jobs"])

    def test_missing_watermark_is_unknown_not_configured_lag_complete(self):
        self.current("ACQUISITION")["watermark"] = None
        result = self.evaluate()
        self.assertEqual(result["jobs"]["ACQUISITION"]["checks"]["watermark"]["status"], "unknown")
        self.assertIn("ACQUISITION", result["deferred_jobs"])

    def test_invalid_or_old_watermark_is_unknown(self):
        for value in ("not a timestamp", "2026-10-11T23:59:59"):
            self.current("ACQUISITION")["watermark"] = value
            self.assertIn("ACQUISITION", self.evaluate()["deferred_jobs"])

    def test_identity_loss_or_conflicts_roll_back(self):
        self.current("ACQUISITION")["nonnull_ids"] = 989
        self.assertEqual(self.evaluate()["ready_through"], "2026-10-11")
        self.current("ACQUISITION").update(nonnull_ids=1000, identity_conflicts=11)
        self.assertEqual(self.evaluate()["ready_through"], "2026-10-11")

    def test_identity_threshold_boundary_is_supported(self):
        self.current("ACQUISITION").update(nonnull_ids=990, identity_conflicts=10)
        self.assertNotIn("ACQUISITION", self.evaluate()["deferred_jobs"])
        self.assertEqual(self.evaluate()["ready_through"], "2026-10-12")

    def test_invalid_count_does_not_become_complete(self):
        self.current("ACQUISITION")["nonnull_ids"] = 1001
        self.assertIn("ACQUISITION", self.evaluate()["deferred_jobs"])

    def test_allowed_empty_contracts_exempt_required_presence_only(self):
        self.contracts = [Contract("a", "optional", "ACQUISITION", "BOOLEMPTY", allow_empty=True)]
        self.evidence = readiness_evidence(self.window, self.contracts)
        self.evidence["attributes"] = []
        result = self.evaluate(jobs=("ACQUISITION",))
        self.assertEqual(result["deferred_jobs"], [])
        self.assertEqual(result["jobs"]["ACQUISITION"]["checks"]["required_attributes"]["status"], "not applicable")
        self.current("ACQUISITION")["watermark"] = None
        self.assertIn("ACQUISITION", self.evaluate(jobs=("ACQUISITION",))["deferred_jobs"])

    def test_ambiguous_empty_contract_not_silently_exempted(self):
        self.contracts = [Contract("a", "optional", "ACQUISITION", "BOOLEMPTY",
                                   allow_empty=True, contract_status="contract conflict")]
        self.evidence = readiness_evidence(self.window, self.contracts)
        result = self.evaluate(jobs=("ACQUISITION",))
        self.assertIn("ACQUISITION", result["deferred_jobs"])

    def test_inactive_job_membership_never_enters_denominator(self):
        inactive = Contract("inactive", "same name", "OTHERCHAT", "BOOL")
        self.contracts.append(inactive)
        result = self.evaluate(jobs=("ACQUISITION",))
        check = result["jobs"]["ACQUISITION"]["checks"]["required_attributes"]
        self.assertEqual(check["required_contracts"], 1)
        self.assertNotIn(inactive.key, [x["contract_key"] for x in check["contracts"]])

    def test_single_required_missing_surge_does_not_hide_quality_detector(self):
        self.contracts = [Contract(str(i), "flag" + str(i), "ACQUISITION", "BOOL") for i in range(10)]
        self.evidence = readiness_evidence(self.window, self.contracts)
        self.attribute(self.contracts[0].key)["present"] = 0
        result = self.evaluate(jobs=("ACQUISITION",))
        check = result["jobs"]["ACQUISITION"]["checks"]["required_attributes"]
        self.assertEqual(check["ready_fraction"], .9)
        self.assertEqual(check["incomplete_supported_contracts"], 1)
        self.assertEqual(result["ready_through"], "2026-10-12")
        self.assertEqual(result["deferred_jobs"], [])

    def test_widespread_required_partial_fills_roll_back(self):
        self.contracts = [Contract(str(i), "flag" + str(i), "ACQUISITION", "BOOL") for i in range(10)]
        self.evidence = readiness_evidence(self.window, self.contracts)
        for contract in self.contracts[:5]:
            self.attribute(contract.key)["present"] = 100
        result = self.evaluate(jobs=("ACQUISITION",))
        self.assertEqual(result["jobs"]["ACQUISITION"]["checks"]["required_attributes"]["status"], "incomplete")
        self.assertEqual(result["ready_through"], "2026-10-11")

    def test_invalid_nonblank_outputs_are_presence_not_correctness(self):
        self.attribute(self.contracts[0].key).update(present=1000, invalid=1000)
        self.assertEqual(self.evaluate()["jobs"]["ACQUISITION"]["proposed_end_status"], "ready")

    def test_absent_historical_applicability_does_not_infer_activation(self):
        for row in self.evidence["attributes"]:
            if row["job"] == "ACQUISITION" and row["day"] != "2026-10-12":
                row["present"] = 0
        result = self.evaluate()
        check = result["jobs"]["ACQUISITION"]["checks"]["required_attributes"]
        self.assertEqual(check["supported_contracts"], 0)
        self.assertIn("ACQUISITION", result["deferred_jobs"])

    def test_unstable_historical_presence_marked_unknown(self):
        for row in self.evidence["attributes"]:
            if row["job"] == "ACQUISITION" and date.fromisoformat(row["day"]).day % 2:
                row["present"] = 0
        check = self.evaluate()["jobs"]["ACQUISITION"]["checks"]["required_attributes"]
        self.assertEqual(check["status"], "unknown")

    def test_unmapped_required_contract_is_unknown(self):
        self.evidence["unmapped_contracts"] = [self.contracts[0].key]
        result = self.evaluate()
        self.assertIn("ACQUISITION", result["deferred_jobs"])
        self.assertEqual(result["jobs"]["ACQUISITION"]["checks"]["required_attributes"]["unknown_contracts"],
                         [self.contracts[0].key])

    def test_missing_current_attribute_evidence_is_unknown(self):
        self.evidence["attributes"] = [x for x in self.evidence["attributes"] if not (
            x["job"] == "ACQUISITION" and x["day"] == "2026-10-12")]
        self.assertIn("ACQUISITION", self.evaluate()["deferred_jobs"])

    def test_missing_attribute_population_not_reweighted_as_ready(self):
        self.attribute(self.contracts[0].key).update(n=100, present=100)
        self.assertIn("ACQUISITION", self.evaluate()["deferred_jobs"])

    def test_no_supported_same_weekday_baseline_is_unknown(self):
        keep = {"2026-10-12", "2026-10-05", "2026-09-28"}
        self.evidence["days"] = [x for x in self.evidence["days"] if x["job"] != "ACQUISITION" or x["day"] in keep]
        self.assertIn("ACQUISITION", self.evaluate()["deferred_jobs"])

    def test_scan_is_bounded_and_no_ready_day_means_deferred(self):
        self.settings.readiness_lookback_days = 2
        for row in self.evidence["days"]:
            if row["job"] == "ACQUISITION" and row["day"] >= "2026-10-11":
                row.update(n=10, nonnull_ids=10)
        result = self.evaluate()
        self.assertEqual(len(result["jobs"]["ACQUISITION"]["scanned_dates"]), 2)
        self.assertIn("ACQUISITION", result["deferred_jobs"])

    def test_out_of_window_evidence_rejected(self):
        self.evidence["days"].append(dict(self.current("ACQUISITION"), day="2026-07-14"))
        with self.assertRaises(ValueError):
            self.evaluate()

    def test_duplicate_readiness_rows_are_unknown(self):
        self.evidence["days"].append(copy.deepcopy(self.current("ACQUISITION")))
        self.assertIn("ACQUISITION", self.evaluate()["deferred_jobs"])

    def test_effective_shared_day_is_checked_for_each_job(self):
        for row in self.evidence["days"]:
            if row["job"] == "ACQUISITION" and row["day"] >= "2026-10-11":
                row.update(n=100, nonnull_ids=100)
        for row in self.evidence["attributes"]:
            if row["job"] == "ACQUISITION" and row["day"] >= "2026-10-11":
                row.update(n=100, present=100)
        self.evidence["days"] = [x for x in self.evidence["days"] if not (
            x["job"] == "SERVICE" and x["day"] == "2026-10-10")]
        result = self.evaluate()
        self.assertEqual(result["ready_through"], "2026-10-10")
        self.assertIn("SERVICE", result["deferred_jobs"])

    def test_unsafe_policy_parameters_rejected(self):
        for name, value in (("readiness_volume_fraction", 1.1), ("source_lag_days", 0),
                            ("readiness_baseline_weeks", 13), ("readiness_lookback_days", 31)):
            prior = getattr(self.settings, name)
            setattr(self.settings, name, value)
            with self.assertRaises(ValueError):
                self.evaluate()
            setattr(self.settings, name, prior)

    def test_validated_settings_support_complete_readiness_boundary_ranges(self):
        for weeks in range(1, 13):
            settings = Settings(readiness_baseline_weeks=weeks, readiness_min_baseline_days=min(3, weeks),
                                readiness_lookback_days=30)
            with self.subTest(weeks=weeks):
                policy = _settings(settings)
                self.assertEqual(policy["readiness_baseline_weeks"], weeks)
                result = evaluate_job_readiness(self.window, settings, ("ACQUISITION", "SERVICE"),
                                                self.evidence, self.contracts)
                self.assertEqual(result["deferred_jobs"], [])
        minimum = Settings(source_lag_days=1, readiness_lookback_days=1, readiness_baseline_weeks=1,
                           readiness_min_baseline_days=1, readiness_min_baseline_volume=1,
                           readiness_attribute_baseline_days=1)
        maximum = Settings(source_lag_days=89, readiness_lookback_days=30, readiness_baseline_weeks=12,
                           readiness_min_baseline_days=12, readiness_min_baseline_volume=10**12,
                           readiness_attribute_baseline_days=89)
        self.assertEqual(_settings(minimum)["readiness_lookback_days"], 1)
        self.assertEqual(_settings(maximum)["readiness_min_baseline_days"], 12)

    def test_settings_and_policy_reject_identical_out_of_range_values(self):
        for name, value in (("readiness_lookback_days", 31), ("readiness_baseline_weeks", 13),
                            ("readiness_min_baseline_days", 13), ("readiness_min_baseline_volume", 10**12 + 1),
                            ("readiness_attribute_baseline_days", 90), ("readiness_volume_fraction", True),
                            ("readiness_lookback_days", 1.5)):
            with self.subTest(name=name, value=value):
                with self.assertRaises(ValueError):
                    Settings(**{name: value})
                with self.assertRaises(ValueError):
                    _settings(SimpleNamespace(**{**READINESS_DEFAULTS, name: value}))

    def test_minimum_support_cannot_exceed_either_baseline_in_settings_or_policy(self):
        for values in ({"readiness_baseline_weeks": 2, "readiness_min_baseline_days": 3},
                       {"readiness_attribute_baseline_days": 2, "readiness_min_baseline_days": 3}):
            with self.subTest(values=values):
                with self.assertRaisesRegex(ValueError, "bounded baseline"):
                    Settings(**values)
                with self.assertRaisesRegex(ValueError, "bounded baseline"):
                    _settings(SimpleNamespace(**{**READINESS_DEFAULTS, **values}))

    def test_clock_uses_central_not_utc_date(self):
        self.clock.stop()
        with patch("attribute_health.readiness.datetime", wraps=datetime) as clock:
            clock.now.return_value = datetime(2026, 10, 13, 4, 59, tzinfo=timezone.utc).astimezone(CENTRAL)
            self.assertEqual(self.evaluate()["proposed_end"], "2026-10-11")
