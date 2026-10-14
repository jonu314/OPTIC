import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from model_lab import ModelLabService, DemoRepository, OdinClient, candidate_call
from test_model_lab import FakeClient, base_payload


class RecoveryTests(unittest.TestCase):
    def test_default_judge_is_verified_sol_61(self):
        with tempfile.TemporaryDirectory() as directory:
            service = ModelLabService(Path(directory), repository_factory=lambda _: DemoRepository())
            payload = base_payload(judge={"enabled": True})
            self.assertEqual(service.validate_config(payload)["judge"]["model"], "gpt-6.1-sol")
            self.assertEqual(service.bootstrap("demo")["defaults"]["judge_model"], "gpt-6.1-sol")

    def test_gpt6_omits_temperature_and_uses_supported_reasoning(self):
        class Response:
            status_code = 200
            headers = {}
            def json(self):
                return {"choices": [{"message": {"content": '{}'}}], "model": "gpt-6.1-sol"}
        class Session:
            def post(self, url, **kwargs):
                self.body = kwargs["json"]
                return Response()
        with patch.dict(os.environ, {"ODIN_API_TOKEN": "test-only", "OPTIC_MODEL_LAB_REASONING_EFFORT": "low"}):
            for model in ["gpt-6.1-sol", "gpt-6-luna", "gpt-5.6-luna"]:
                session = Session()
                OdinClient(session=session).call(model, "system", "user")
                self.assertNotIn("temperature", session.body)
                self.assertEqual(session.body["reasoning_effort"], "low")

    def test_resume_retains_candidates_judgments_and_saved_sample(self):
        class CountingClient(FakeClient):
            def __init__(self):
                self.requests = []
            def call(self, model, system, user):
                self.requests.append((model, "BLINDED CANDIDATE OUTPUTS" in user))
                return super().call(model, system, user)
        with tempfile.TemporaryDirectory() as directory:
            repository = DemoRepository()
            client = CountingClient()
            service = ModelLabService(Path(directory), repository_factory=lambda _: repository, client_factory=lambda _: client)
            config = service.validate_config(base_payload(sample_size=1, judge={"enabled": True, "scope": "all"}))
            samples = repository.sample(config["job_name"], config["start_date"], config["end_date"], 1, config["seed"])
            with patch('threading.Thread.start'):
                run, _ = service.manager.start(config, samples)
            event = service.manager.cancel_events.pop(run["id"])
            service.manager.execution_locks.pop(run["id"]).release()
            calls = [candidate_call(client, service.validator, model, config["system_prompt"], samples[0], config["attribute_specs"], 0) for model in config["models"]]
            run["calls"] = calls
            run["progress"]["candidate_completed"] = 2
            for model in config["models"]:
                run["progress"]["per_model"][model]["completed"] = 1
            run["progress"]["per_batch"]["1"]["completed"] = 2
            run["status"] = "failed"
            service.store.save(run)
            client.requests.clear()
            with patch.object(repository, 'sample', side_effect=AssertionError('Must reuse saved transcript')):
                service.manager.resume(run["id"])
                deadline = time.monotonic() + 5
                while service.store.load(run["id"])["status"] != 'completed' and time.monotonic() < deadline:
                    time.sleep(.02)
            done = service.store.load(run["id"])
            self.assertEqual(done["status"], 'completed')
            self.assertEqual(done["calls"], calls)
            self.assertEqual(client.requests, [("gpt-6.1-sol", True)])
            done["status"] = 'failed'
            service.store.save(done)
            client.requests.clear()
            service.manager.resume(run["id"])
            deadline = time.monotonic() + 5
            while service.store.load(run["id"])["status"] != 'completed' and time.monotonic() < deadline:
                time.sleep(.02)
            self.assertEqual(client.requests, [])
            self.assertEqual(len(service.store.load(run["id"])["judgments"]), 1)


if __name__ == '__main__':
    unittest.main()
