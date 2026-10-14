import unittest
from model_lab import SnowflakeRepository, normalize_sample_row


class BaselineIdentityTests(unittest.TestCase):
    def test_sampler_uses_result_bridge_not_content_bridge(self):
        repo = SnowflakeRepository(lambda: None)
        queries = []
        repo._query = lambda sql, params=(): queries.append((sql, params)) or []
        repo.sample("ACQUISITION", "2026-07-15", "2026-10-12", 40, 13)
        sql = " ".join(queries[0][0].upper().split())
        self.assertIn("A.ID AS PRODUCTION_RESULT_ID", sql)
        self.assertIn("S.PRODUCTION_RESULT_ID = R.ID", sql)
        self.assertNotIn("S.ANALYSISID = R.ID", sql)
        self.assertIn("C.ANALYSISID = S.ANALYSISID", sql)
        self.assertIn("R.CONNID", sql)
        self.assertIn("R.JOBNAME", sql)
        self.assertIn("DATEDIFF(SECOND", sql)
        self.assertIn("R.CREATEDDATE DESC", sql)

    def test_content_and_result_id_remain_distinct(self):
        row = normalize_sample_row({"analysisid": 9001, "production_result_id": 42,
                                    "connid": "C", "content": "Agent: hello"})
        self.assertEqual(row["analysis_id"], "9001")
        self.assertEqual(row["transcript_analysis_id"], "9001")
        self.assertEqual(row["production_result_id"], "42")

    def test_legacy_saved_row_still_normalizes(self):
        row = normalize_sample_row({"analysis_id": "old", "content": "old transcript"})
        self.assertEqual(row["analysis_id"], "old")
        self.assertEqual(row["production_result_id"], "")


if __name__ == "__main__":
    unittest.main()
