import json
import unittest
from dataclasses import replace

from attribute_health.config import Settings
from attribute_health.contracts import Contract, normalize
from attribute_health.repository import OBJECTS, SnowflakeRepository, identifier
from test_health_engine import END, WINDOW


def ready_repo():
    repo = SnowflakeRepository(Settings(jobs=("ACQUISITION",)), connection_factory=lambda: None)
    repo.apply_readiness({"ready_through": str(END), "deferred_jobs": [],
                          "jobs": {"ACQUISITION": {"status": "ready"}}}, WINDOW)
    return repo


class ActivePromptMembershipTests(unittest.TestCase):
    def repository(self, prompts, attributes):
        repo = ready_repo()

        def query(sql, params=()):
            self.assertIn("TRY_PARSE_JSON(p.ATTRIBUTELIST)", sql)
            self.assertIn("prm.value:ResultAttributeList", sql)
            self.assertIn("a.ATTRIBUTEID = ra.value:AttributeId::INT", sql)
            self.assertIn("COALESCE(p.ENABLED, FALSE) = TRUE", sql)
            self.assertIn("p.HISTORYENDDATE >= CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ", sql)
            self.assertEqual(sql.count("CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ"), 4)
            self.assertIn("prm.value:Enabled", sql)
            self.assertIn("ra.value:Enabled", sql)
            self.assertNotIn(identifier(OBJECTS["details"]), sql)
            rows = []
            for prompt in prompts:
                if not prompt.get("enabled", True) or prompt.get("expired"):
                    continue
                for group in json.loads(prompt["attributelist"]):
                    if group.get("Enabled", True) is not True:
                        continue
                    for member in group["ResultAttributeList"]:
                        if member.get("Enabled", True) is not True:
                            continue
                        attribute = attributes.get(member["AttributeId"])
                        if attribute is None or attribute.get("disabled", False):
                            continue
                        rows.append({**attribute, "jobname": prompt["jobname"],
                                     "promptid": prompt["promptid"], "system_prompt": prompt["instructions"],
                                     "member_attribute_id": member["AttributeId"],
                                     "member_attribute_name": member["AttributeName"],
                                     "prompt_group_name": group["PromptName"],
                                     "prompt_group_enabled": True})
            return rows

        repo.query = query
        return repo

    @staticmethod
    def prompt(prompt_id, ids, **flags):
        return {"promptid": prompt_id, "jobname": "ACQUISITION", "instructions": "Exact active system",
                "attributelist": json.dumps([{"Order": 1, "PromptName": "High Priority Attributes",
                                             "Enabled": True, "ResultAttributeList": [
                                                 {"Order": i, "AttributeId": x, "AttributeName": f"a{x}"}
                                                 for i, x in enumerate(ids)]}]), **flags}

    @staticmethod
    def attributes():
        return {i: {"attributeid": i, "attributename": f"a{i}", "resultvaluetype": "BOOL",
                    "prompt": f"Definition for attribute {i}", "historystartdate": "2026-10-01"}
                for i in range(1, 5)}

    def test_disabled_and_expired_prompt_cannot_resurrect_retired_members(self):
        prompts = [self.prompt(67, [1, 2]), self.prompt(68, [1, 3], enabled=False),
                   self.prompt(69, [1, 4], enabled=True, expired=True)]
        result = self.repository(prompts, self.attributes()).registry()
        self.assertEqual({c.attribute_id for c in result}, {"1", "2"})
        self.assertTrue(all(c.provenance["prompt_id"] == 67 for c in result))
        self.assertTrue(all(c.historical_scope == "current-contract retrospective" for c in result))
        self.assertTrue(all(c.provenance["metadata_history_start"] == "2026-10-01" for c in result))

    def test_shared_active_attribute_keeps_coherent_prompt_bindings(self):
        prompts = [self.prompt(67, [1]), self.prompt(70, [1])]
        result = self.repository(prompts, self.attributes()).registry()
        self.assertEqual(len(result), 1)
        self.assertEqual({b["prompt_id"] for b in result[0].provenance["active_prompt_bindings"]}, {67, 70})
        self.assertEqual(result[0].system_prompt, "Exact active system")

    def test_conflicting_active_system_instructions_remain_separate(self):
        prompts = [self.prompt(67, [1]), {**self.prompt(70, [1]), "instructions": "Different active definition"}]
        result = self.repository(prompts, self.attributes()).registry()
        self.assertEqual(len(result), 2)
        self.assertEqual({c.system_prompt for c in result},
                         {"Exact active system", "Different active definition"})
        self.assertTrue(all(c.contract_status == "conflicting active job definitions" for c in result))

    def test_disabled_group_member_and_attribute_excluded(self):
        prompt = self.prompt(67, [1, 2])
        groups = json.loads(prompt["attributelist"])
        groups[0]["ResultAttributeList"][1]["Enabled"] = False
        groups.append({"PromptName": "Disabled group", "Enabled": False,
                       "ResultAttributeList": [{"AttributeId": 3, "AttributeName": "a3"}]})
        prompt["attributelist"] = json.dumps(groups)
        result = self.repository([prompt], self.attributes()).registry()
        self.assertEqual([c.attribute_id for c in result], ["1"])

    def test_ids_not_duplicate_names_define_membership(self):
        attributes = self.attributes()
        attributes[3] = {**attributes[3], "attributename": "a1"}
        result = self.repository([self.prompt(67, [1]), self.prompt(68, [3], enabled=False)], attributes).registry()
        self.assertEqual([c.attribute_id for c in result], ["1"])
        self.assertEqual(result[0].contract_status, "resolved")

    def test_unverified_member_id_rejected(self):
        repo = ready_repo()
        repo.query = lambda *args: [{"attributeid": 1, "member_attribute_id": 3,
                                     "attributename": "a1", "jobname": "ACQUISITION"}]
        with self.assertRaisesRegex(ValueError, "verified active-prompt"):
            repo.registry()


class ListSamplePopulationTests(unittest.TestCase):
    contract = Contract("list-1", "labels", "ACQUISITION", "LIST", ("Offer", "Other"))
    values = ['["offer"]', '["Offer","Other"]', "Offer,Other", '["Other"]',
              "Other", "[]", '["Offer","Invalid"]', '["Invalid"]', '["Offer",1]',
              '["Offer",null]', "", None, '"Offer"', "Offer,,Other"]

    def sample(self, complement):
        repo = ready_repo()
        repo._columns = {"LABELS"}
        calls = []

        def query(sql, params=()):
            calls.append((sql, params))
            self.assertIn("BOOLAND_AGG", sql)
            self.assertIn("BOOLOR_AGG", sql)
            self.assertIn("TYPEOF(f.VALUE)='VARCHAR'", sql)
            self.assertIn("SPLIT(s.HEALTH_VALUE_TEXT,',')", sql)
            self.assertIn("LOWER(TRIM(f.VALUE::VARCHAR))", sql)
            self.assertIn("ARRAY_SIZE(s.HEALTH_LIST)=0", sql)
            self.assertIn("FROM valid_list_values i", sql)
            self.assertLess(sql.index("m.HEALTH_VALID_LIST"), sql.index("COUNT(*) OVER"))
            self.assertNotIn("ARRAY_CONTAINS", sql)
            self.assertEqual(sql.count("%s"), len(params))
            self.assertIn("Offer", params)
            self.assertIn("Other", params)
            eligible = []
            for i, raw in enumerate(self.values):
                value = normalize(self.contract, raw)
                if value["status"] == "valid" and (("Offer" in value["labels"]) != complement):
                    eligible.append({"production_result_id": i + 1, "connection_id": f"C{i}",
                                     "job": "ACQUISITION", "interaction_date": str(END) + "T12:00:00",
                                     "stored_output": raw})
            for row in eligible:
                row["population_count"] = len(eligible)
            return eligible

        repo.query = query
        return repo.candidates(self.contract, WINDOW, WINDOW.start, END, "Offer", complement, limit=120), calls

    def test_lowercase_and_json_or_comma_values_are_flagged(self):
        result, _ = self.sample(False)
        self.assertEqual([r["stored_output"] for r in result["rows"]],
                         ['["offer"]', '["Offer","Other"]', "Offer,Other"])
        self.assertEqual(result["population_count"], 3)

    def test_complement_has_valid_absence_including_zero_label_array(self):
        result, calls = self.sample(True)
        self.assertEqual([r["stored_output"] for r in result["rows"]], ['["Other"]', "Other", "[]"])
        self.assertEqual(result["population_count"], 3)
        self.assertIn("m.HEALTH_HAS_LABEL=FALSE", calls[0][0])

    def test_invalid_arrays_blank_and_json_scalars_never_representative(self):
        for complement in (False, True):
            result, _ = self.sample(complement)
            self.assertTrue(all(normalize(self.contract, r["stored_output"])["status"] == "valid"
                                for r in result["rows"]))
            self.assertNotIn('["Invalid"]', [r["stored_output"] for r in result["rows"]])
            self.assertNotIn("", [r["stored_output"] for r in result["rows"]])

    def test_unknown_or_processing_label_cannot_default_to_boolean_false(self):
        repo = ready_repo()
        repo._columns = {"FLAG"}
        repo.query = lambda *args: self.fail("Unknown finite label must fail before SELECT.")
        with self.assertRaisesRegex(ValueError, "declared finite label"):
            repo.candidates(Contract("1", "flag", "ACQUISITION", "BOOL"), WINDOW, WINDOW.start, END,
                            "missing", False)

    def test_deferred_job_cannot_sample_or_aggregate(self):
        repo = ready_repo()
        repo._columns = {"LABELS"}
        repo.apply_readiness({"ready_through": str(END), "deferred_jobs": ["ACQUISITION"],
                              "jobs": {"ACQUISITION": {"status": "unknown"}}}, WINDOW)
        repo.query = lambda *args: self.fail("Deferred job cannot read an audit/measurement population.")
        with self.assertRaisesRegex(ValueError, "explicitly deferred"):
            repo.candidates(self.contract, WINDOW, WINDOW.start, END, "Offer", False)
        result = repo.aggregate([self.contract], WINDOW)
        self.assertEqual(sum(r["eligible"] for r in result[self.contract.key]), 0)


if __name__ == "__main__":
    unittest.main()
