from __future__ import annotations

import csv
import io
import json
import os
import tempfile
import threading
import time
import unittest
import urllib.request
from pathlib import Path
from unittest.mock import patch

from model_lab import (
    ANTHROPIC_CANDIDATE_MODELS,
    BLANK,
    DEFAULT_MODELS,
    DEMO_ATTRIBUTES,
    GPT_CANDIDATE_MODELS,
    ApiResponse,
    DemoRepository,
    IntegrationError,
    MAX_SAMPLE_SIZE,
    ModelLabError,
    ModelLabService,
    OdinClient,
    ResultValidationAdapter,
    RunStore,
    SnowflakeRepository,
    VERIFIED_GPT_MODELS,
    build_batch_prompt,
    build_judge_prompt,
    chunk_attributes,
    compute_report,
    configured_judge_attribute_names,
    configured_models,
    default_judge_attribute_names,
    extract_attribute_layout,
    model_catalog,
    normalize_judge_output,
    parse_json_object,
    judge_eligible_attributes,
    run_summary,
    select_judge_attributes,
)


class FakeClient:
    def call(self, deployment, system_prompt, user_prompt):
        if "BLINDED CANDIDATE OUTPUTS" in user_prompt:
            return {
                "content": json.dumps({"attributes": {}}),
                "latency_seconds": 0.001,
                "retries": 0,
                "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
                "returned_model": deployment,
                "estimated_cost_usd": None,
            }
        keys = json.loads(user_prompt.split("in this order:\n", 1)[1].split("\n", 1)[0])
        values = {}
        for key in keys:
            spec = next(item for item in DEMO_ATTRIBUTES if item["name"] == key)
            if spec["result_type"] == "BOOL":
                values[key] = "1" if deployment.endswith("sol") else "0"
            elif spec["result_type"] == "INTEGER":
                values[key] = 2
            elif spec["result_type"] == "LIST":
                values[key] = []
            elif spec["options"]:
                values[key] = spec["options"][0]
            else:
                values[key] = "Summary"
        return {
            "content": json.dumps(values),
            "latency_seconds": 0.001,
            "retries": 0,
            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
            "returned_model": deployment,
            "estimated_cost_usd": None,
        }


def base_payload(**overrides):
    payload = {
        "mode": "demo",
        "job_id": "service",
        "start_date": "2026-01-01",
        "end_date": "2026-07-30",
        "sample_size": 2,
        "seed": 42,
        "models": ["gpt-5.6-sol", "gpt-5.6-luna"],
        "attributes": ["issue_unresolved", "call_type_svc"],
        "judge": {"enabled": False, "scope": "disagreements", "model": "gpt-5.6-sol"},
    }
    payload.update(overrides)
    return payload


class BatchingAndPromptTests(unittest.TestCase):
    def test_stable_batching_boundaries(self):
        cases = {
            0: [],
            1: [1],
            20: [20],
            21: [20, 1],
            41: [20, 20, 1],
        }
        for count, expected_sizes in cases.items():
            with self.subTest(count=count):
                values = list(range(count))
                batches = chunk_attributes(values)
                self.assertEqual([len(batch) for batch in batches], expected_sizes)
                self.assertEqual([item for batch in batches for item in batch], values)

    def test_model_catalog_defaults_and_environment_override(self):
        verified = [
            "gpt-6.1-sol",
            "gpt-6-luna",
            "gpt-5.6-sol",
            "gpt-5.6-luna",
            "gpt-5.6-terra",
            "gpt-5.5",
            "gpt-5.4",
            "gpt-5.4-mini",
        ]
        anthropic = [
            "claude-fable-5",
            "claude-opus-5",
            "claude-sonnet-5",
            "claude-haiku-4-5",
            "claude-opus-4-8",
            "claude-opus-4-7",
            "claude-opus-4-6",
            "claude-sonnet-4-6",
            "claude-sonnet-4-5",
            "claude-opus-4-5",
        ]
        gpt_candidates = []
        with patch.dict(os.environ, {"OPTIC_MODEL_LAB_MODELS": ""}, clear=False):
            self.assertEqual(VERIFIED_GPT_MODELS, verified)
            self.assertEqual(GPT_CANDIDATE_MODELS, gpt_candidates)
            self.assertEqual(ANTHROPIC_CANDIDATE_MODELS, anthropic)
            self.assertEqual(DEFAULT_MODELS, verified + gpt_candidates + anthropic)
            self.assertEqual(configured_models(), verified + gpt_candidates + anthropic)
            catalog = model_catalog(configured_models())
            self.assertEqual(
                [item["slug"] for item in catalog], verified + gpt_candidates + anthropic
            )
            self.assertTrue(all(item["status"] == "verified" for item in catalog[:len(verified)]))
            self.assertEqual(catalog[len(verified) - 1]["slug"], "gpt-5.4-mini")
            self.assertEqual(catalog[len(verified) - 1]["provider"], "openai")
            self.assertEqual(catalog[len(verified) - 1]["group"], "Verified GPT")
            self.assertTrue(all(item["status"] == "candidate" for item in catalog[len(verified):]))
            self.assertEqual({item["provider"] for item in catalog[len(verified):]}, {"anthropic"})
        with patch.dict(
            os.environ,
            {
                "OPTIC_MODEL_LAB_MODELS": (
                    "private-model-1, gpt-5.4-mini, private-model-1, INVALID_SLUG!"
                )
            },
            clear=False,
        ):
            self.assertEqual(configured_models(), ["private-model-1", "gpt-5.4-mini"])

    def test_prompt_requires_exact_keys_and_schema_details(self):
        specs = DEMO_ATTRIBUTES[:2]
        prompt = build_batch_prompt(specs, "Customer: hello")
        self.assertIn('["issue_unresolved", "call_type_svc"]', prompt)
        self.assertIn('Exact JSON key: "issue_unresolved"', prompt)
        self.assertIn("Result type: BOOL", prompt)
        self.assertIn("Allowed options:", prompt)
        self.assertIn("Blank allowed: no", prompt)
        self.assertIn("exactly these keys", prompt)
        self.assertIn("<transcript>\nCustomer: hello\n</transcript>", prompt)

    def test_prompt_rejects_more_than_twenty_attributes(self):
        with self.assertRaises(ValueError):
            build_batch_prompt([dict(DEMO_ATTRIBUTES[0], name=f"a{index}") for index in range(21)], "x")

    def test_attribute_list_order_and_groups_are_preserved(self):
        value = {
            "PromptGroups": [
                {
                    "PromptName": "High priority",
                    "ResultAttributeList": [
                        {"AttributeName": "issue_unresolved"},
                        {"AttributeName": "call_summary"},
                    ],
                },
                {
                    "PromptName": "Basics",
                    "ResultAttributeList": ["call_type_svc"],
                },
            ]
        }
        order, groups = extract_attribute_layout(value)
        self.assertEqual(order, ["issue_unresolved", "call_summary", "call_type_svc"])
        self.assertEqual(groups["issue_unresolved"], "High priority")
        self.assertEqual(groups["call_type_svc"], "Basics")


class JsonAndValidationTests(unittest.TestCase):
    def setUp(self):
        self.validator = ResultValidationAdapter()

    def test_json_parser_plain_fenced_noisy_and_invalid(self):
        self.assertEqual(parse_json_object('{"a":1}'), {"a": 1})
        self.assertEqual(parse_json_object({"a": 1}), {"a": 1})
        self.assertEqual(parse_json_object('```json\n{"a":1}\n```'), {"a": 1})
        self.assertEqual(parse_json_object('Result follows: {"a":1} done'), {"a": 1})
        with self.assertRaises(ValueError):
            parse_json_object("not json")
        with self.assertRaises(ValueError):
            parse_json_object("[1,2]")

    def test_boolean_and_blank_validation(self):
        bool_spec = {"name": "flag", "result_type": "BOOL", "options": ["1", "0"], "allow_blank": False}
        self.assertEqual(self.validator.validate(bool_spec, True)["canonical"], "1")
        self.assertTrue(self.validator.validate(bool_spec, "yes")["valid"])
        self.assertFalse(self.validator.validate(bool_spec, "maybe")["valid"])
        self.assertFalse(self.validator.validate(bool_spec, "")["valid"])
        empty_spec = dict(bool_spec, result_type="BOOLEMPTY", allow_blank=True)
        result = self.validator.validate(empty_spec, "")
        self.assertTrue(result["valid"])
        self.assertEqual(result["canonical"], BLANK)

    def test_blank_free_text_is_valid_but_constrained_blanks_remain_invalid(self):
        for result_type in ("STRING", "TEXT", "VARCHAR", "FREEFORM"):
            for value in ("", "   \t"):
                with self.subTest(result_type=result_type, value=repr(value)):
                    spec = {
                        "name": "PUC_risk_reasoning",
                        "result_type": result_type,
                        "options": [],
                        "allow_blank": False,
                    }
                    result = self.validator.validate(spec, value)
                    self.assertTrue(result["present"])
                    self.assertTrue(result["valid"])
                    self.assertEqual(result["canonical"], BLANK)
                    self.assertEqual(result["reason"], "blank_allowed")
        constrained = [
            {"name": "enum", "result_type": "STRINGPICKONE", "options": ["Yes", "No"]},
            {"name": "flag", "result_type": "BOOL", "options": ["1", "0"]},
            {"name": "amount", "result_type": "NUMBER", "options": []},
            {"name": "count", "result_type": "INTEGER", "options": []},
            {"name": "date", "result_type": "DATE", "options": []},
            {"name": "items", "result_type": "LIST", "options": []},
        ]
        for spec in constrained:
            with self.subTest(result_type=spec["result_type"]):
                spec["allow_blank"] = False
                result = self.validator.validate(spec, " ")
                self.assertFalse(result["valid"])
                self.assertEqual(result["reason"], "blank_not_allowed")

    def test_free_text_prompt_contract_marks_blank_as_allowed(self):
        spec = {
            "name": "do_not_contact_verbatim",
            "result_type": "FREEFORM",
            "options": [],
            "allow_blank": False,
            "prompt": "Return the exact quote, or blank when absent.",
        }
        prompt = build_batch_prompt([spec], "Customer: hello")
        self.assertIn("Result type: STRING", prompt)
        self.assertIn("Blank allowed: yes", prompt)

    def test_default_judge_eligibility_excludes_free_text(self):
        specs = [
            {"name": "reasoning", "result_type": "STRING", "options": []},
            {"name": "pick", "result_type": "STRINGPICKONE", "options": ["A", "B"]},
            {"name": "flag", "result_type": "BOOL", "options": ["1", "0"]},
            {"name": "count", "result_type": "INTEGER", "options": []},
            {"name": "amount", "result_type": "NUMBER", "options": []},
            {"name": "items", "result_type": "LIST", "options": []},
        ]
        self.assertEqual(
            default_judge_attribute_names(specs),
            ["pick", "flag", "count", "amount", "items"],
        )
        self.assertEqual(
            [spec["name"] for spec in judge_eligible_attributes(specs, True)],
            ["reasoning", "pick", "flag", "count", "amount", "items"],
        )

    def test_pick_list_number_and_integer_validation(self):
        pick = {
            "name": "type",
            "result_type": "STRINGPICKONE",
            "options": ["High Bill", "Other"],
            "allow_blank": False,
        }
        self.assertEqual(self.validator.validate(pick, "high-bill")["canonical"], "High Bill")
        self.assertFalse(self.validator.validate(pick, "Outage")["valid"])
        listed = {
            "name": "competitors",
            "result_type": "LIST",
            "options": ["Reliant", "Gexa"],
            "allow_blank": True,
        }
        result = self.validator.validate(listed, ["gexa", "Reliant"])
        self.assertTrue(result["valid"])
        self.assertEqual(result["canonical"], ["Gexa", "Reliant"])
        self.assertFalse(self.validator.validate(listed, ["Unknown"])["valid"])
        number = {"name": "amount", "result_type": "NUMBER", "options": [], "allow_blank": False}
        integer = dict(number, name="count", result_type="INTEGER")
        self.assertTrue(self.validator.validate(number, "12.50")["valid"])
        self.assertTrue(self.validator.validate(integer, "12.0")["valid"])
        self.assertFalse(self.validator.validate(integer, "12.5")["valid"])

    def test_missing_key_is_distinct_from_blank(self):
        result = self.validator.validate(DEMO_ATTRIBUTES[0])
        self.assertFalse(result["present"])
        self.assertEqual(result["reason"], "missing_key")


class OdinClientTests(unittest.TestCase):
    def test_authorization_header_uses_configured_token(self):
        class Response:
            status_code = 200
            headers = {}

            @staticmethod
            def json():
                return {
                    "choices": [{"message": {"content": '{"answer":"ok"}'}}],
                    "usage": {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5},
                    "model": "gpt-5.6-sol",
                }

        class Session:
            def __init__(self):
                self.request = None

            def post(self, url, **kwargs):
                self.request = {"url": url, **kwargs}
                return Response()

        session = Session()
        with patch.dict(os.environ, {"ODIN_API_TOKEN": "Bearer unit-test-token"}, clear=False):
            result = OdinClient(session=session).call("gpt-5.6-sol", "system", "user")
        self.assertEqual(session.request["headers"]["Authorization"], "Bearer unit-test-token")
        self.assertNotIn("unit-test-token", json.dumps(result))
        self.assertIn("/openai/deployments/gpt-5.6-sol/chat/completions", session.request["url"])
        self.assertNotIn("anthropic-version", session.request["headers"])
        self.assertEqual(
            session.request["json"]["response_format"], {"type": "json_object"}
        )
        self.assertIn("max_completion_tokens", session.request["json"])
        self.assertNotIn("max_tokens", session.request["json"])

    def test_gpt_5_4_mini_uses_openai_chat_completions_protocol(self):
        class Response:
            status_code = 200
            headers = {}

            @staticmethod
            def json():
                return {
                    "choices": [{"message": {"content": '{"answer":"mini"}'}}],
                    "usage": {"prompt_tokens": 4, "completion_tokens": 2},
                    "model": "gpt-5.4-mini",
                }

        class Session:
            def __init__(self):
                self.request = None

            def post(self, url, **kwargs):
                self.request = {"url": url, **kwargs}
                return Response()

        session = Session()
        with patch.dict(os.environ, {"ODIN_API_TOKEN": "mini-unit-token"}, clear=False):
            result = OdinClient(session=session).call("gpt-5.4-mini", "system", "user")
        self.assertIn(
            "/openai/deployments/gpt-5.4-mini/chat/completions",
            session.request["url"],
        )
        self.assertNotIn("/anthropic/", session.request["url"])
        self.assertNotIn("anthropic-version", session.request["headers"])
        self.assertEqual(
            session.request["json"]["response_format"], {"type": "json_object"}
        )
        self.assertEqual(session.request["json"]["temperature"], 0)
        self.assertNotIn("model", session.request["json"])
        self.assertEqual(result["returned_model"], "gpt-5.4-mini")
        self.assertNotIn("mini-unit-token", json.dumps(result))

    def test_anthropic_messages_route_headers_body_envelope_and_usage(self):
        class Response:
            status_code = 200
            headers = {}

            @staticmethod
            def json():
                return {
                    "id": "msg_test",
                    "model": "claude-sonnet-5",
                    "content": [
                        {"type": "text", "text": '{"answer":'},
                        {"type": "text", "text": '"ok"}'},
                    ],
                    "usage": {"input_tokens": 7, "output_tokens": 4},
                }

        class Session:
            def __init__(self):
                self.request = None

            def post(self, url, **kwargs):
                self.request = {"url": url, **kwargs}
                return Response()

        session = Session()
        env = {
            "ODIN_API_TOKEN": "anthropic-unit-token",
            "ODIN_GATEWAY_BASE_URL": "https://odin.example/gateway",
            "ODIN_ANTHROPIC_MESSAGES_PATH": "/custom/anthropic/messages",
            "ODIN_ANTHROPIC_VERSION": "2023-06-01",
            "OPTIC_MODEL_LAB_MAX_COMPLETION_TOKENS": "4000",
        }
        with patch.dict(os.environ, env, clear=False):
            result = OdinClient(session=session).call("claude-sonnet-5", "system", "user")

        request = session.request
        self.assertEqual(request["url"], "https://odin.example/gateway/custom/anthropic/messages")
        self.assertEqual(request["headers"]["Authorization"], "Bearer anthropic-unit-token")
        self.assertEqual(request["headers"]["anthropic-version"], "2023-06-01")
        self.assertEqual(request["headers"]["Content-Type"], "application/json")
        self.assertEqual(
            request["json"],
            {
                "model": "claude-sonnet-5",
                "system": "system",
                "messages": [{"role": "user", "content": "user"}],
                "max_tokens": 4000,
            },
        )
        self.assertEqual(result["content"], '{"answer":"ok"}')
        self.assertEqual(
            result["usage"],
            {"prompt_tokens": 7, "completion_tokens": 4, "total_tokens": 11},
        )
        self.assertEqual(result["returned_model"], "claude-sonnet-5")
        self.assertNotIn("anthropic-unit-token", json.dumps(result))

    def test_anthropic_uses_shared_retry_after_handling(self):
        class Response:
            def __init__(self, status_code, payload, headers=None):
                self.status_code = status_code
                self.payload = payload
                self.headers = headers or {}

            def json(self):
                return self.payload

        class Session:
            def __init__(self):
                self.requests = []
                self.responses = [
                    Response(429, {}, {"Retry-After": "0"}),
                    Response(
                        200,
                        {
                            "model": "claude-haiku-4-5",
                            "content": [{"type": "text", "text": '{"ok":true}'}],
                            "usage": {"input_tokens": 2, "output_tokens": 1},
                        },
                    ),
                ]

            def post(self, url, **kwargs):
                self.requests.append({"url": url, **kwargs})
                return self.responses.pop(0)

        session = Session()
        sleeps = []
        with patch.dict(
            os.environ,
            {
                "ODIN_API_TOKEN": "retry-unit-token",
                "ODIN_MAX_RETRIES": "2",
                "ODIN_ANTHROPIC_MESSAGES_PATH": "/anthropic/v1/messages",
            },
            clear=False,
        ):
            result = OdinClient(session=session, sleep=sleeps.append).call(
                "claude-haiku-4-5", "system", "user"
            )
        self.assertEqual(len(session.requests), 2)
        self.assertEqual(sleeps, [0.0])
        self.assertEqual(result["retries"], 1)
        self.assertNotIn("retry-unit-token", json.dumps(result))

    def test_anthropic_rejects_unsafe_route_without_affecting_gpt(self):
        class Response:
            status_code = 200
            headers = {}

            @staticmethod
            def json():
                return {
                    "choices": [{"message": {"content": '{"ok":true}'}}],
                    "usage": {},
                }

        class Session:
            def post(self, url, **kwargs):
                return Response()

        with patch.dict(
            os.environ,
            {
                "ODIN_API_TOKEN": "safe-path-token",
                "ODIN_ANTHROPIC_MESSAGES_PATH": "https://untrusted.example/messages",
            },
            clear=False,
        ):
            client = OdinClient(session=Session())
            self.assertEqual(
                client.call("gpt-5.6-sol", "system", "user")["content"], '{"ok":true}'
            )
            with self.assertRaises(IntegrationError):
                client.call("claude-opus-5", "system", "user")
        with patch.dict(
            os.environ,
            {
                "ODIN_API_TOKEN": "safe-path-token",
                "ODIN_ANTHROPIC_MESSAGES_PATH": "/anthropic/../../outside/messages",
            },
            clear=False,
        ):
            with self.assertRaises(IntegrationError):
                OdinClient(session=Session()).call(
                    "claude-opus-5", "system", "user"
                )


class SnowflakeRepositoryTests(unittest.TestCase):
    def test_active_job_history_uses_utc_ntz_current_time(self):
        repository = SnowflakeRepository(lambda: None)
        queries = []
        repository._query = lambda sql, params=(): queries.append(sql) or []

        self.assertEqual(repository.list_jobs(), [])

        normalized = " ".join(queries[0].split()).upper()
        utc_now = "CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ"
        self.assertEqual(normalized.count(utc_now), 2)
        self.assertIn(f"HISTORYSTARTDATE <= {utc_now}", normalized)
        self.assertIn(f"HISTORYENDDATE >= {utc_now}", normalized)
        self.assertNotIn("HISTORYSTARTDATE <= CURRENT_TIMESTAMP()", normalized)
        self.assertNotIn("HISTORYENDDATE >= CURRENT_TIMESTAMP()", normalized)


class EvaluationTests(unittest.TestCase):
    def setUp(self):
        self.validator = ResultValidationAdapter()
        self.spec = {
            "name": "flag",
            "result_type": "BOOL",
            "options": ["1", "0"],
            "allow_blank": False,
        }

    def _call(self, model, value):
        validation = self.validator.validate(self.spec, value)
        return {
            "analysis_id": "A1",
            "source_id": "S1",
            "model": model,
            "batch_index": 0,
            "request_success": True,
            "parse_success": True,
            "extra_keys": [],
            "validations": [validation],
            "values": {"flag": value},
            "latency_seconds": 1.0,
            "retries": 0,
            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
            "estimated_cost_usd": None,
            "error": None,
        }

    def test_operational_baseline_and_pairwise_metrics(self):
        run = {
            "config": {"models": ["model-a", "model-b"]},
            "attributes": [self.spec],
            "transcripts": [
                {
                    "analysis_id": "A1",
                    "source_id": "S1",
                    "baseline": {"result_json": '{"flag":"1"}'},
                }
            ],
            "calls": [self._call("model-a", "1"), self._call("model-b", "0")],
            "judgments": [],
            "manual_labels": {},
        }
        report = compute_report(run, self.validator)
        by_model = {row["model"]: row for row in report["leaderboard"]}
        self.assertEqual(by_model["model-a"]["components"]["baseline_agreement"], 1.0)
        self.assertEqual(by_model["model-b"]["components"]["baseline_agreement"], 0.0)
        self.assertEqual(report["pairwise"][0]["disagreement_rate"], 1.0)
        self.assertEqual(len(report["disagreements"]), 1)
        self.assertEqual(report["recommendation"]["label"], "Provisional leader")
        self.assertEqual(report["recommendation"]["model"], "model-a")
        self.assertIn("baseline_agreement", report["weights"])

    def test_missing_expected_key_fails_schema_compliance(self):
        call = self._call("model-a", "1")
        call["values"] = {}
        call["validations"] = [self.validator.validate(self.spec)]
        run = {
            "config": {"models": ["model-a"]},
            "attributes": [self.spec],
            "transcripts": [{"analysis_id": "A1", "source_id": "S1", "baseline": {}}],
            "calls": [call],
            "judgments": [],
            "manual_labels": {},
        }
        report = compute_report(run, self.validator)
        self.assertEqual(report["leaderboard"][0]["components"]["schema_compliance"], 0.0)

    def test_blank_free_text_does_not_create_violation_or_reduce_validity(self):
        spec = {
            "name": "auditory_verbatim",
            "result_type": "STRING",
            "options": [],
            "allow_blank": False,
        }
        validation = self.validator.validate(spec, "   ")
        call = {
            "analysis_id": "A1",
            "source_id": "S1",
            "model": "model-a",
            "batch_index": 0,
            "request_success": True,
            "parse_success": True,
            "extra_keys": [],
            "validations": [validation],
            "values": {"auditory_verbatim": "   "},
            "latency_seconds": 1,
            "retries": 0,
            "usage": {},
            "estimated_cost_usd": None,
            "error": None,
        }
        run = {
            "status": "completed",
            "config": {"models": ["model-a"]},
            "attributes": [spec],
            "transcripts": [{"analysis_id": "A1", "source_id": "S1", "baseline": {}}],
            "calls": [call],
            "judgments": [],
            "manual_labels": {},
            "progress": {"candidate_total": 1, "candidate_completed": 1},
        }
        report = compute_report(run, self.validator)
        self.assertEqual(report["violations"], [])
        self.assertEqual(
            report["leaderboard"][0]["components"]["domain_validity"], 1.0
        )
        self.assertEqual(report["recommendation"]["model"], "model-a")

    def test_judge_selection_blinding_and_parsing(self):
        calls = [self._call("gpt-5.6-sol", "1"), self._call("gpt-5.6-terra", "0")]
        selected = select_judge_attributes(
            calls, [self.spec], ["gpt-5.6-sol", "gpt-5.6-terra"], "disagreements"
        )
        self.assertEqual([item["name"] for item in selected], ["flag"])
        prompt, mapping = build_judge_prompt(
            "Customer: test",
            [self.spec],
            {"gpt-5.6-sol": {"flag": "1"}, "gpt-5.6-terra": {"flag": "0"}},
            "seed",
        )
        self.assertNotIn("gpt-5.6-sol", prompt)
        self.assertEqual(set(mapping.values()), {"gpt-5.6-sol", "gpt-5.6-terra"})
        raw = {
            "attributes": {
                "flag": {
                    "candidate_correctness": {label: True for label in mapping},
                    "winner": "tie",
                    "confidence": 0.8,
                    "rationale": "both are supported",
                    "evidence": "test",
                }
            }
        }
        normalized, missing, extra_count = normalize_judge_output(raw, mapping, ["flag"])
        self.assertEqual(normalized["flag"]["winner"], "tie")
        self.assertEqual(set(normalized["flag"]["candidate_correctness"]), set(mapping))
        self.assertEqual(missing, [])
        self.assertEqual(extra_count, 0)

    def test_judge_normalization_enforces_requested_attribute_scope(self):
        mapping = {"candidate_a": "model-a", "candidate_b": "model-b"}
        raw = {
            "attributes": {
                "flag": {
                    "candidate_correctness": {"candidate_a": True, "candidate_b": False},
                    "winner": "candidate_a",
                },
                "prompt_injected_key": {
                    "candidate_correctness": {"candidate_a": False, "candidate_b": True},
                    "winner": "candidate_b",
                },
            }
        }
        normalized, missing, extra_count = normalize_judge_output(
            raw, mapping, ["flag", "missing_flag"]
        )
        self.assertEqual(set(normalized), {"flag"})
        self.assertEqual(missing, ["missing_flag"])
        self.assertEqual(extra_count, 1)

    def test_judge_selection_skips_identical_valid_outputs(self):
        calls = [self._call("a", "1"), self._call("b", True)]
        self.assertEqual(select_judge_attributes(calls, [self.spec], ["a", "b"], "disagreements"), [])
        self.assertEqual(select_judge_attributes(calls, [self.spec], ["a", "b"], "all"), [self.spec])

    def test_recommendation_requires_complete_run_evidence_and_unique_metric(self):
        calls = [self._call("model-a", "1"), self._call("model-b", "0")]
        base = {
            "status": "completed",
            "config": {"models": ["model-a", "model-b"]},
            "attributes": [self.spec],
            "transcripts": [{"analysis_id": "A1", "source_id": "S1", "baseline": {}}],
            "calls": calls,
            "judgments": [],
            "manual_labels": {},
            "progress": {"candidate_total": 2, "candidate_completed": 2},
        }
        tied = compute_report(base, self.validator)
        self.assertEqual(tied["recommendation"]["label"], "Tie")
        self.assertIsNone(tied["recommendation"]["model"])
        self.assertIsNone(run_summary(dict(base, report=tied)).get("leader"))

        cancelled = dict(base, status="cancelled")
        cancelled_report = compute_report(cancelled, self.validator)
        self.assertEqual(cancelled_report["recommendation"]["label"], "Incomplete run")
        self.assertIsNone(cancelled_report["recommendation"]["model"])
        self.assertIn("No leader is declared", cancelled_report["recommendation"]["statement"])

        failed = dict(base, status="failed")
        failed_report = compute_report(failed, self.validator)
        self.assertEqual(failed_report["recommendation"]["label"], "Incomplete run")
        self.assertIsNone(failed_report["recommendation"]["model"])

        partial = dict(base, calls=calls[:1], progress={"candidate_total": 2, "candidate_completed": 1})
        partial_report = compute_report(partial, self.validator)
        self.assertEqual(partial_report["recommendation"]["label"], "Incomplete run")
        self.assertIsNone(partial_report["recommendation"]["model"])

        failed_calls = []
        for call in calls:
            failed = dict(call, request_success=False, parse_success=False, values={})
            failed["validations"] = [self.validator.validate(self.spec)]
            failed_calls.append(failed)
        no_evidence = compute_report(dict(base, calls=failed_calls), self.validator)
        self.assertEqual(no_evidence["recommendation"]["label"], "Insufficient evidence")
        self.assertIsNone(no_evidence["recommendation"]["model"])

    def test_unrequested_judge_results_do_not_affect_accuracy(self):
        calls = [self._call("model-a", "1"), self._call("model-b", "0")]
        run = {
            "status": "completed",
            "config": {"models": ["model-a", "model-b"]},
            "attributes": [self.spec],
            "transcripts": [{"analysis_id": "A1", "source_id": "S1", "baseline": {}}],
            "calls": calls,
            "manual_labels": {},
            "progress": {"candidate_total": 2, "candidate_completed": 2},
            "judgments": [
                {
                    "attributes_requested": ["flag"],
                    "mapping": {"candidate_a": "model-a", "candidate_b": "model-b"},
                    "results": {
                        "flag": {
                            "candidate_correctness": {"candidate_a": True, "candidate_b": False}
                        },
                        "judge_only": {
                            "candidate_correctness": {"candidate_a": False, "candidate_b": True}
                        },
                    },
                    "missing_attributes": [],
                    "extra_attribute_count": 1,
                }
            ],
        }
        report = compute_report(run, self.validator)
        rows = {row["model"]: row for row in report["leaderboard"]}
        self.assertEqual(rows["model-a"]["ai_judged_items"], 1)
        self.assertEqual(rows["model-a"]["ai_judged_accuracy"], 1.0)
        self.assertEqual(rows["model-b"]["ai_judged_items"], 1)
        self.assertEqual(rows["model-b"]["ai_judged_accuracy"], 0.0)
        self.assertEqual(report["judge_scope"]["extra_attributes"], 1)


class PersistenceAndAsyncTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.service = ModelLabService(
            self.root,
            repository_factory=lambda mode: DemoRepository(),
            client_factory=lambda mode: FakeClient(),
        )

    def tearDown(self):
        self.temp.cleanup()

    def wait_for_run(self, run_id):
        deadline = time.time() + 5
        while time.time() < deadline:
            run = self.service.store.load(run_id)
            if run["status"] in {"completed", "failed", "cancelled"}:
                return run
            time.sleep(0.02)
        self.fail("Async run did not finish.")

    def test_model_cap_accepts_full_catalog_and_environment_override(self):
        with patch.dict(os.environ, {"OPTIC_MODEL_LAB_MODEL_CAP": ""}, clear=False):
            service = ModelLabService(
                self.root / "catalog-cap",
                repository_factory=lambda mode: DemoRepository(),
                client_factory=lambda mode: FakeClient(),
            )
            self.assertEqual(service.model_cap, len(DEFAULT_MODELS))
            config = service.validate_config(base_payload(models=list(DEFAULT_MODELS)))
            self.assertEqual(config["models"], DEFAULT_MODELS)
        with patch.dict(os.environ, {"OPTIC_MODEL_LAB_MODEL_CAP": "24"}, clear=False):
            service = ModelLabService(
                self.root / "catalog-cap-override",
                repository_factory=lambda mode: DemoRepository(),
                client_factory=lambda mode: FakeClient(),
            )
            self.assertEqual(service.model_cap, 24)

    def test_sample_cap_accepts_10000_and_rejects_larger_or_noninteger(self):
        self.assertEqual(MAX_SAMPLE_SIZE, 10_000)
        config = self.service.validate_config(base_payload(sample_size=10_000))
        self.assertEqual(config["sample_size"], 10_000)
        self.assertEqual(self.service.sample_cap, 10_000)
        with self.assertRaisesRegex(ModelLabError, "between 1 and 10000"):
            self.service.validate_config(base_payload(sample_size=10_001))
        for invalid in (1.5, "1.5", True, None, ""):
            with self.subTest(invalid=invalid):
                with self.assertRaisesRegex(ModelLabError, "sample_size must be an integer"):
                    self.service.validate_config(base_payload(sample_size=invalid))

    def test_10000_sample_preflight_estimates_large_call_scale(self):
        attributes = [
            {
                "attribute_id": index,
                "name": f"flag_{index:02d}",
                "description": f"Flag {index}",
                "result_type": "BOOL",
                "options": ["1", "0"],
                "prompt": f"Return flag {index}.",
                "allow_blank": False,
                "group": "Scale",
                "rownum": index,
            }
            for index in range(41)
        ]

        class ScaleRepository(DemoRepository):
            def list_attributes(self, job):
                return attributes

            def count_samples(self, job_name, start_date, end_date):
                return 10_000

        service = ModelLabService(
            self.root / "sample-scale",
            repository_factory=lambda mode: ScaleRepository(),
            client_factory=lambda mode: FakeClient(),
        )
        payload = base_payload(
            sample_size=10_000,
            models=["gpt-5.6-sol", "gpt-5.6-luna", "gpt-5.6-terra"],
            attributes=[attribute["name"] for attribute in attributes],
            judge={
                "enabled": True,
                "scope": "all",
                "model": "gpt-5.6-sol",
                "include_free_text": False,
                "attributes": [attribute["name"] for attribute in attributes],
            },
        )
        config = service.validate_config(payload)
        self.assertEqual(config["batch_count"], 3)
        self.assertEqual(config["estimated_candidate_calls"], 90_000)
        self.assertEqual(config["max_judge_calls"], 30_000)
        self.assertEqual(config["max_judge_items"], 410_000)
        self.assertEqual(config["max_judge_output_tokens"], 120_000_000)

        preflight = service.preflight(payload)
        self.assertEqual(preflight["sample_will_use"], 10_000)
        self.assertEqual(preflight["candidate_calls"], 90_000)
        self.assertEqual(preflight["max_judge_calls"], 30_000)
        self.assertEqual(preflight["max_judge_items"], 410_000)
        self.assertGreater(preflight["estimated_judge_input_tokens"], 0)
        self.assertGreater(
            preflight["estimated_max_judge_tokens"], 120_000_000
        )
        self.assertIn("Large run: 90,000 candidate calls", preflight["warning"])
        self.assertIn("Execution starts only after Test is clicked", preflight["warning"])

    def test_judge_scope_validation_persistence_and_estimates(self):
        payload = base_payload(
            attributes=["issue_unresolved", "call_summary"],
            judge={
                "enabled": True,
                "scope": "all",
                "model": "gpt-5.6-sol",
                "include_free_text": False,
                "attributes": ["issue_unresolved"],
            },
        )
        config = self.service.validate_config(payload)
        self.assertEqual(config["attributes"], ["issue_unresolved", "call_summary"])
        self.assertEqual(config["judge"]["attributes"], ["issue_unresolved"])
        self.assertFalse(config["judge"]["include_free_text"])
        self.assertEqual(config["max_judge_items"], 2)
        self.assertEqual(config["max_judge_calls"], 2)
        preflight = self.service.preflight(payload)
        self.assertEqual(preflight["max_judge_items"], 2)
        self.assertGreater(preflight["estimated_judge_input_tokens"], 0)
        self.assertGreater(
            preflight["estimated_max_judge_tokens"],
            preflight["estimated_judge_input_tokens"],
        )

        with self.assertRaisesRegex(ModelLabError, "at least one judge attribute"):
            self.service.validate_config(
                base_payload(
                    judge={
                        "enabled": True,
                        "scope": "all",
                        "model": "gpt-5.6-sol",
                        "attributes": [],
                    }
                )
            )
        with self.assertRaisesRegex(ModelLabError, "Enable free-text judging"):
            self.service.validate_config(
                base_payload(
                    attributes=["issue_unresolved", "call_summary"],
                    judge={
                        "enabled": True,
                        "scope": "all",
                        "model": "gpt-5.6-sol",
                        "include_free_text": False,
                        "attributes": ["call_summary"],
                    },
                )
            )
        include_text = self.service.validate_config(
            base_payload(
                attributes=["issue_unresolved", "call_summary"],
                judge={
                    "enabled": True,
                    "scope": "all",
                    "model": "gpt-5.6-sol",
                    "include_free_text": True,
                    "attributes": ["call_summary"],
                },
            )
        )
        self.assertEqual(include_text["judge"]["attributes"], ["call_summary"])
        with self.assertRaisesRegex(ModelLabError, "candidate-test attributes"):
            self.service.validate_config(
                base_payload(
                    attributes=["issue_unresolved"],
                    judge={
                        "enabled": True,
                        "scope": "all",
                        "model": "gpt-5.6-sol",
                        "attributes": ["call_type_svc"],
                    },
                )
            )

    def test_exact_judge_subset_reaches_prompt_and_accuracy(self):
        judge_prompts = []

        class CapturingClient(FakeClient):
            def call(self, deployment, system_prompt, user_prompt):
                if "BLINDED CANDIDATE OUTPUTS" not in user_prompt:
                    return super().call(deployment, system_prompt, user_prompt)
                judge_prompts.append(user_prompt)
                candidates = json.loads(
                    user_prompt.split("BLINDED CANDIDATE OUTPUTS\n", 1)[1].split(
                        "\n\nTRANSCRIPT", 1
                    )[0]
                )
                correctness = {label: True for label in candidates}
                return {
                    "content": json.dumps(
                        {
                            "attributes": {
                                "issue_unresolved": {
                                    "candidate_correctness": correctness,
                                    "winner": "tie",
                                    "confidence": 0.9,
                                },
                                "call_summary": {
                                    "candidate_correctness": correctness,
                                    "winner": "tie",
                                    "confidence": 0.9,
                                },
                            }
                        }
                    ),
                    "latency_seconds": 0.001,
                    "retries": 0,
                    "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
                    "returned_model": deployment,
                    "estimated_cost_usd": None,
                }

        service = ModelLabService(
            self.root / "judge-subset",
            repository_factory=lambda mode: DemoRepository(),
            client_factory=lambda mode: CapturingClient(),
        )
        config = service.validate_config(
            base_payload(
                sample_size=1,
                attributes=["issue_unresolved", "call_summary"],
                judge={
                    "enabled": True,
                    "scope": "all",
                    "model": "gpt-5.6-sol",
                    "include_free_text": False,
                    "attributes": ["issue_unresolved"],
                },
            )
        )
        started, _ = service.manager.start(config)
        deadline = time.time() + 5
        while time.time() < deadline:
            final = service.store.load(started["id"])
            if final["status"] in {"completed", "failed", "cancelled"}:
                break
            time.sleep(0.02)
        self.assertEqual(final["status"], "completed")
        self.assertEqual(final["config"]["judge"]["attributes"], ["issue_unresolved"])
        self.assertEqual(len(judge_prompts), 1)
        self.assertIn('"key":"issue_unresolved"', judge_prompts[0])
        self.assertNotIn("call_summary", judge_prompts[0])
        judgment = final["judgments"][0]
        self.assertEqual(judgment["attributes_requested"], ["issue_unresolved"])
        self.assertEqual(set(judgment["results"]), {"issue_unresolved"})
        self.assertEqual(judgment["extra_attribute_count"], 1)
        for row in final["report"]["leaderboard"]:
            self.assertEqual(row["ai_judged_items"], 1)
            self.assertEqual(row["ai_judged_accuracy"], 1.0)

    def test_old_artifact_judge_scope_falls_back_to_all_tested_attributes(self):
        legacy_config = {
            "attributes": ["issue_unresolved", "call_summary"],
            "judge": {"enabled": True, "scope": "all", "model": "gpt-5.6-sol"},
        }
        self.assertEqual(
            configured_judge_attribute_names(legacy_config),
            ["issue_unresolved", "call_summary"],
        )
        summary = run_summary(
            {
                "id": "d" * 32,
                "status": "completed",
                "config": legacy_config,
                "report": {"recommendation": {"model": None}},
            }
        )
        self.assertEqual(summary["judge_attribute_count"], 2)

    def test_payload_validation_and_async_state(self):
        with self.assertRaises(ModelLabError):
            self.service.validate_config(base_payload(models=["gpt-5.6-sol"]))
        with self.assertRaises(ModelLabError):
            self.service.validate_config(base_payload(attributes=["not_active"]))
        config = self.service.validate_config(base_payload())
        run, duplicate = self.service.manager.start(config)
        self.assertFalse(duplicate)
        final = self.wait_for_run(run["id"])
        self.assertEqual(final["status"], "completed")
        self.assertEqual(final["progress"]["candidate_completed"], 4)
        self.assertEqual(len(final["calls"]), 4)
        self.assertTrue(final["report"]["leaderboard"])

    def test_concurrent_duplicate_is_reused(self):
        class SlowClient(FakeClient):
            def call(self, *args):
                time.sleep(0.08)
                return super().call(*args)

        service = ModelLabService(
            self.root / "slow",
            repository_factory=lambda mode: DemoRepository(),
            client_factory=lambda mode: SlowClient(),
        )
        config = service.validate_config(base_payload())
        first, duplicate_one = service.manager.start(config)
        second, duplicate_two = service.manager.start(config)
        self.assertFalse(duplicate_one)
        self.assertTrue(duplicate_two)
        self.assertEqual(first["id"], second["id"])
        deadline = time.time() + 5
        while time.time() < deadline:
            if service.store.load(first["id"])["status"] in {"completed", "failed", "cancelled"}:
                break
            time.sleep(0.02)
        else:
            self.fail("Duplicate run did not finish.")

    def test_cancel_after_sampling_submits_no_candidate_calls(self):
        sampling_started = threading.Event()
        allow_sampling_to_finish = threading.Event()
        client_calls = []

        class BlockingRepository(DemoRepository):
            def sample(self, *args, **kwargs):
                sampling_started.set()
                allow_sampling_to_finish.wait(timeout=2)
                return super().sample(*args, **kwargs)

        class CountingClient(FakeClient):
            def call(self, *args):
                client_calls.append(args)
                return super().call(*args)

        service = ModelLabService(
            self.root / "cancel",
            repository_factory=lambda mode: BlockingRepository(),
            client_factory=lambda mode: CountingClient(),
        )
        config = service.validate_config(base_payload())
        started, _ = service.manager.start(config)
        self.assertTrue(sampling_started.wait(timeout=2))
        service.manager.cancel(started["id"])
        allow_sampling_to_finish.set()
        deadline = time.time() + 5
        while time.time() < deadline:
            run = service.store.load(started["id"])
            if run["status"] in {"completed", "failed", "cancelled"}:
                break
            time.sleep(0.02)
        self.assertEqual(run["status"], "cancelled")
        self.assertEqual(client_calls, [])
        self.assertEqual(run["report"]["recommendation"]["label"], "Incomplete run")
        self.assertIsNone(run["report"]["recommendation"]["model"])

    def test_cancel_records_already_running_candidate_calls(self):
        calls_started = threading.Event()
        release_calls = threading.Event()
        call_count = 0
        call_guard = threading.Lock()

        class BlockingClient(FakeClient):
            def call(self, *args):
                nonlocal call_count
                with call_guard:
                    call_count += 1
                    if call_count == 2:
                        calls_started.set()
                release_calls.wait(timeout=2)
                return super().call(*args)

        service = ModelLabService(
            self.root / "cancel-running",
            repository_factory=lambda mode: DemoRepository(),
            client_factory=lambda mode: BlockingClient(),
        )
        config = service.validate_config(base_payload(sample_size=1))
        started, _ = service.manager.start(config)
        self.assertTrue(calls_started.wait(timeout=2))
        service.manager.cancel(started["id"])
        release_calls.set()
        deadline = time.time() + 5
        while time.time() < deadline:
            run = service.store.load(started["id"])
            if run["status"] in {"completed", "failed", "cancelled"}:
                break
            time.sleep(0.02)
        self.assertEqual(run["status"], "cancelled")
        self.assertEqual(run["progress"]["candidate_completed"], 2)
        self.assertEqual(len(run["calls"]), 2)

    def test_active_run_is_not_marked_interrupted_by_second_store(self):
        root = self.root / "shared-store"
        store = RunStore(root)
        run_id = "b" * 32
        execution_lock = store.acquire_execution_lock(run_id)
        self.assertIsNotNone(execution_lock)
        try:
            store.save(
                {
                    "id": run_id,
                    "status": "running",
                    "config": {},
                    "progress": {},
                    "report": {},
                }
            )
            second_store = RunStore(root)
            self.assertEqual(second_store.load(run_id)["status"], "running")
        finally:
            execution_lock.release()
        third_store = RunStore(root)
        interrupted = third_store.load(run_id)
        self.assertEqual(interrupted["status"], "failed")
        self.assertTrue(interrupted["recoverable"])

    def test_interrupted_recovery_reloads_status_after_acquiring_lock(self):
        root = self.root / "recovery-race"
        store = RunStore(root)
        run_id = "c" * 32
        store.save(
            {
                "id": run_id,
                "status": "running",
                "config": {},
                "progress": {},
                "report": {},
            }
        )
        acquire = store.acquire_execution_lock

        def finish_before_returning_lock(target_run_id):
            execution_lock = acquire(target_run_id)
            completed = store.load(target_run_id)
            completed["status"] = "completed"
            store.save(completed)
            return execution_lock

        store.acquire_execution_lock = finish_before_returning_lock
        store._mark_interrupted()
        self.assertEqual(store.load(run_id)["status"], "completed")

    def test_manual_label_persists_and_recomputes_reviewed_accuracy(self):
        validator = self.service.validator
        spec = DEMO_ATTRIBUTES[0]
        run_id = "a" * 32
        run = {
            "id": run_id,
            "status": "completed",
            "created_at": "2026-07-30T00:00:00+00:00",
            "updated_at": "2026-07-30T00:00:00+00:00",
            "config": {
                "models": ["gpt-5.6-sol", "gpt-5.6-luna"],
                "job_name": "Service",
                "sample_size": 1,
                "attributes": [spec["name"]],
                "mode": "demo",
            },
            "attributes": [spec],
            "transcripts": [{"analysis_id": "A1", "source_id": "S1", "baseline": {}}],
            "calls": [
                {
                    "analysis_id": "A1", "source_id": "S1", "model": "gpt-5.6-sol", "batch_index": 0,
                    "request_success": True, "parse_success": True, "extra_keys": [],
                    "validations": [validator.validate(spec, "1")], "values": {spec["name"]: "1"},
                    "latency_seconds": 1, "retries": 0,
                    "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
                    "estimated_cost_usd": None, "error": None,
                },
                {
                    "analysis_id": "A1", "source_id": "S1", "model": "gpt-5.6-luna", "batch_index": 0,
                    "request_success": True, "parse_success": True, "extra_keys": [],
                    "validations": [validator.validate(spec, "0")], "values": {spec["name"]: "0"},
                    "latency_seconds": 1, "retries": 0,
                    "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
                    "estimated_cost_usd": None, "error": None,
                },
            ],
            "judgments": [],
            "manual_labels": {},
            "progress": {},
            "report": {},
        }
        run["report"] = compute_report(run, validator)
        self.service.store.save(run)
        result = self.service.add_label(
            run_id,
            {"analysis_id": "A1", "attribute": spec["name"], "decision": "expected", "expected_value": "1"},
        )
        rows = {row["model"]: row for row in result["report"]["leaderboard"]}
        self.assertEqual(rows["gpt-5.6-sol"]["reviewed_accuracy"], 1.0)
        self.assertEqual(rows["gpt-5.6-luna"]["reviewed_accuracy"], 0.0)
        self.assertEqual(result["report"]["recommendation"]["label"], "Reviewed leader")
        reloaded = self.service.store.load(run_id)
        self.assertIn("A1|issue_unresolved", reloaded["manual_labels"])

    def test_export_shapes(self):
        config = self.service.validate_config(base_payload(sample_size=1))
        started, _ = self.service.manager.start(config)
        final = self.wait_for_run(started["id"])
        csv_response = self.service.export(final["id"], "csv")
        json_response = self.service.export(final["id"], "json")
        self.assertIsInstance(csv_response, ApiResponse)
        csv_rows = list(
            csv.DictReader(io.StringIO(csv_response.body.decode("utf-8")))
        )
        self.assertTrue(csv_rows)
        self.assertTrue(all(row["record_type"] == "candidate" for row in csv_rows))
        self.assertIn("analysis_id", csv_rows[0])
        self.assertIn("source_id", csv_rows[0])
        self.assertIn("model", csv_rows[0])
        self.assertIn("judge_outcome", csv_rows[0])
        exported = json.loads(json_response.body)
        self.assertEqual(exported["id"], final["id"])
        self.assertTrue(exported["sanitized"])
        serialized = json_response.body.decode("utf-8")
        self.assertNotIn(final["transcripts"][0]["transcript"], serialized)
        self.assertNotIn(final["config"]["system_prompt"], serialized)
        self.assertNotIn("result_json", serialized)
        self.assertNotIn("raw_output", serialized)

    def test_csv_export_flattens_judgment_audit_and_sanitizes_all_strings(self):
        validator = self.service.validator
        flag = {
            "name": "flag",
            "description": "Flag description",
            "prompt": "=DANGEROUS_INSTRUCTION()",
            "result_type": "BOOL",
            "options": ["1", "0"],
            "allow_blank": False,
        }
        text = {
            "name": "summary",
            "description": "Summary description",
            "prompt": "Return a summary.",
            "result_type": "STRING",
            "options": [],
            "allow_blank": True,
        }
        models = ["model-a", "model-b"]

        def candidate(analysis_id, source_id, model, spec, value):
            validation = validator.validate(spec, value)
            return {
                "analysis_id": analysis_id,
                "source_id": source_id,
                "model": model,
                "batch_index": 0,
                "request_success": True,
                "parse_success": True,
                "validations": [validation],
                "values": {spec["name"]: value},
                "latency_seconds": 0.2,
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 3,
                    "total_tokens": 13,
                },
                "returned_model": model + "-returned",
            }

        calls = [
            candidate("A1", "=SOURCE()", "model-a", flag, "=RAW_FORMULA()"),
            candidate("A1", "=SOURCE()", "model-b", flag, "0"),
        ]
        run_id = "e" * 32
        run = {
            "id": run_id,
            "status": "cancelled",
            "created_at": "2026-08-10T00:00:00+00:00",
            "updated_at": "2026-08-10T00:01:00+00:00",
            "config": {
                "job_name": "Service",
                "models": models,
                "attributes": ["flag", "summary"],
                "judge": {
                    "enabled": True,
                    "model": "gpt-5.6-sol",
                    "scope": "all",
                    "attributes": ["flag", "summary"],
                },
            },
            "attributes": [flag, text],
            "transcripts": [
                {
                    "analysis_id": "A1",
                    "source_id": "=SOURCE()",
                    "transcript": "SENSITIVE_TRANSCRIPT_SENTINEL",
                },
                {"analysis_id": "A2", "source_id": "S2", "transcript": "private"},
                {"analysis_id": "A3", "source_id": "S3", "transcript": "private"},
                {"analysis_id": "A4", "source_id": "S4", "transcript": "private"},
                {"analysis_id": "A5", "source_id": "S5", "transcript": "private"},
            ],
            "calls": calls,
            "judgments": [
                {
                    "analysis_id": "A1",
                    "batch_index": 0,
                    "attributes_requested": ["flag"],
                    "mapping": {"Candidate A": "model-a", "Candidate B": "model-b"},
                    "results": {
                        "flag": {
                            "candidate_correctness": {
                                "Candidate A": True,
                                "Candidate B": False,
                            },
                            "winner": "Candidate A",
                            "confidence": 0.91,
                            "rationale": "=HYPERLINK(\"bad\")",
                            "evidence": "+malicious evidence",
                        }
                    },
                    "latency_seconds": 0.7,
                    "usage": {
                        "prompt_tokens": 20,
                        "completion_tokens": 8,
                        "total_tokens": 28,
                    },
                    "retries": 1,
                    "missing_attributes": [],
                    "extra_attribute_count": 2,
                    "raw_output": "RAW_JUDGE_ENVELOPE_SENTINEL",
                },
                {
                    "analysis_id": "A2",
                    "batch_index": 0,
                    "attributes_requested": ["summary"],
                    "mapping": {"Candidate A": "model-a", "Candidate B": "model-b"},
                    "results": {
                        "summary": {
                            "candidate_correctness": {
                                "Candidate A": True,
                                "Candidate B": True,
                            },
                            "winner": "tie",
                            "confidence": 0.5,
                            "rationale": "Both supported",
                            "evidence": "",
                        }
                    },
                },
                {
                    "analysis_id": "A3",
                    "batch_index": 0,
                    "attributes_requested": ["flag"],
                    "mapping": {"Candidate A": "model-a", "Candidate B": "model-b"},
                    "results": {
                        "flag": {
                            "candidate_correctness": {
                                "Candidate A": None,
                                "Candidate B": None,
                            },
                            "winner": "uncertain",
                            "confidence": None,
                        }
                    },
                },
                {
                    "analysis_id": "A4",
                    "batch_index": 0,
                    "attributes_requested": ["flag"],
                    "mapping": {},
                    "results": {},
                },
                {
                    "analysis_id": "A5",
                    "batch_index": 0,
                    "mapping": {},
                    "results": {},
                    "error": "@judge failed safely",
                },
            ],
        }
        self.service.store.save(run)
        response = self.service.export(run_id, "csv")
        text_value = response.body.decode("utf-8")
        rows = list(csv.DictReader(io.StringIO(text_value)))

        candidate_rows = [row for row in rows if row["record_type"] == "candidate"]
        judgment_rows = [row for row in rows if row["record_type"] == "judgment"]
        self.assertEqual(len(candidate_rows), 2)
        self.assertEqual(
            {
                key: candidate_rows[0][key]
                for key in (
                    "analysis_id",
                    "source_id",
                    "model",
                    "batch_index",
                    "attribute",
                    "valid",
                    "reason",
                    "request_success",
                    "parse_success",
                )
            },
            {
                "analysis_id": "A1",
                "source_id": "'=SOURCE()",
                "model": "model-a",
                "batch_index": "0",
                "attribute": "flag",
                "valid": "False",
                "reason": "invalid_boolean",
                "request_success": "True",
                "parse_success": "True",
            },
        )
        winner = next(
            row
            for row in judgment_rows
            if row["analysis_id"] == "A1" and row["model"] == "model-a"
        )
        self.assertEqual(winner["anonymous_judge_label"], "Candidate A")
        self.assertEqual(winner["judge_candidate_correctness"], "true")
        self.assertEqual(winner["judge_winner_model"], "model-a")
        self.assertEqual(winner["judge_winner_label"], "Candidate A")
        self.assertEqual(winner["judge_outcome"], "winner")
        self.assertEqual(winner["judge_confidence"], "0.91")
        self.assertEqual(winner["judge_rationale"], '\'=HYPERLINK("bad")')
        self.assertEqual(winner["judge_evidence"], "'+malicious evidence")
        self.assertEqual(winner["attribute_instruction"], "'=DANGEROUS_INSTRUCTION()")
        self.assertEqual(winner["source_id"], "'=SOURCE()")
        self.assertEqual(winner["judge_extra_attribute_count"], "2")
        self.assertEqual(winner["judge_prompt_tokens"], "20")
        self.assertEqual(winner["judge_completion_tokens"], "8")
        self.assertEqual(winner["judge_total_tokens"], "28")
        self.assertEqual(winner["judge_retries"], "1")

        self.assertTrue(
            all(
                row["judge_outcome"] == "tie"
                and row["attribute_instruction"] == "Return a summary."
                and row["blank_allowed"] == "True"
                for row in judgment_rows
                if row["analysis_id"] == "A2"
            )
        )
        self.assertTrue(
            all(
                row["judge_outcome"] == "uncertain"
                and row["judge_candidate_correctness"] == "null"
                for row in judgment_rows
                if row["analysis_id"] == "A3"
            )
        )
        missing_rows = [
            row for row in judgment_rows if row["analysis_id"] == "A4"
        ]
        self.assertTrue(missing_rows)
        self.assertTrue(
            all(
                row["judge_outcome"] == "missing"
                and row["judge_missing_attribute"] == "True"
                and row["judge_missing_attributes"] == '["flag"]'
                for row in missing_rows
            )
        )
        error_rows = [row for row in judgment_rows if row["analysis_id"] == "A5"]
        self.assertTrue(error_rows)
        self.assertTrue(
            all(
                row["judge_outcome"] == "error"
                and row["judge_error"] == "'@judge failed safely"
                and row["attribute"] == ""
                for row in error_rows
            )
        )
        self.assertNotIn("SENSITIVE_TRANSCRIPT_SENTINEL", text_value)
        self.assertNotIn("RAW_JUDGE_ENVELOPE_SENTINEL", text_value)
        self.assertIn('\'=HYPERLINK(""bad"")', text_value)
        self.assertIn("'+malicious evidence", text_value)
        self.assertNotIn("system_prompt", text_value)

    def test_sanitized_export_does_not_copy_untrusted_output_keys(self):
        config = self.service.validate_config(base_payload(sample_size=1))
        started, _ = self.service.manager.start(config)
        final = self.wait_for_run(started["id"])
        sentinel = "SENSITIVE_TRANSCRIPT_SENTINEL"
        final["calls"][0]["extra_keys"] = [sentinel]
        final["judgments"] = [
            {
                "analysis_id": final["transcripts"][0]["analysis_id"],
                "batch_index": 0,
                "attributes_requested": ["issue_unresolved"],
                "mapping": {"candidate_a": "gpt-5.6-sol"},
                "results": {
                    sentinel: {"winner": "candidate_a", "confidence": 1},
                    "issue_unresolved": {"winner": "candidate_a", "confidence": 1},
                },
            }
        ]
        self.service.store.save(final)
        exported = self.service.export(final["id"], "json").body.decode("utf-8")
        self.assertNotIn(sentinel, exported)
        self.assertIn('"extra_key_count": 1', exported)


class ServerCompatibilityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            import optic_console_server
        except ImportError as exc:  # pragma: no cover - optional local dependency gate
            raise unittest.SkipTest(str(exc))
        cls.module = optic_console_server
        cls.server = optic_console_server.ThreadingHTTPServer(("127.0.0.1", 0), optic_console_server.Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def get(self, path):
        with urllib.request.urlopen(self.base + path, timeout=5) as response:
            return response.status, response.read(), response.headers.get("Content-Type")

    def test_existing_console_and_model_lab_static_routes(self):
        status, body, _ = self.get("/")
        self.assertEqual(status, 200)
        self.assertIn(b"OPTIC Console", body)
        status, body, _ = self.get("/model-lab")
        self.assertEqual(status, 200)
        self.assertIn(b"OPTIC Model Lab", body)
        self.assertIn(b"AI judging", body)
        self.assertIn(b"tab-judging", body)

    def test_demo_bootstrap_api_without_external_calls(self):
        with patch.dict(os.environ, {"OPTIC_MODEL_LAB_MODELS": ""}, clear=False):
            status, body, content_type = self.get("/api/model-lab/bootstrap?mode=demo")
        self.assertEqual(status, 200)
        self.assertIn("application/json", content_type)
        payload = json.loads(body)
        self.assertEqual(payload["mode"], "demo")
        self.assertTrue(payload["jobs"])
        self.assertTrue(payload["attributes"])
        self.assertEqual(payload["models"], DEFAULT_MODELS)
        self.assertEqual(
            [item["slug"] for item in payload["model_catalog"]], DEFAULT_MODELS
        )
        self.assertEqual(
            [item["group"] for item in payload["model_catalog"][:6]],
            ["Verified GPT"] * 6,
        )
        self.assertEqual(payload["model_catalog"][5]["provider"], "openai")
        self.assertEqual(
                [item["group"] for item in payload["model_catalog"][len(VERIFIED_GPT_MODELS):]],
            ["Anthropic candidates"] * 10,
        )
        self.assertEqual(payload["defaults"]["model_cap"], len(DEFAULT_MODELS))
        self.assertEqual(payload["defaults"]["sample_cap"], 10_000)

    def test_model_lab_source_guards_poll_races_and_aggregate_filters(self):
        source = Path(__file__).with_name("OPTIC_Model_Lab.html").read_text(encoding="utf-8")
        self.assertIn("pollGeneration", source)
        self.assertIn("if(!isActiveRun(id,generation))return", source)
        self.assertIn("clearTimeout(S.pollTimer)", source)
        self.assertIn("if(generation!==S.pollGeneration)return", source)
        self.assertIn('class:"filters two"},model,attr', source)
        self.assertNotIn('"All statuses"', source)
        self.assertIn('"All review statuses"', source)
        self.assertIn('"Anthropic candidates"', source)
        self.assertIn("Anthropic candidates depend on Odin gateway access", source)
        self.assertIn("candidates depend on Odin gateway access", source)
        self.assertIn("S.models.slice(0,2)", source)
        self.assertIn("Include free-text/string attributes in judge scope", source)
        self.assertIn("attributes:[...S.selectedJudgeAttrs]", source)
        self.assertIn(
            "if(!eligibleNames.has(name))S.selectedJudgeAttrs.delete(name)", source
        )
        self.assertIn(
            "if(!S.judgeEligibleSeen.has(attr.name)&&!isFreeTextAttribute(attr))",
            source,
        )
        self.assertIn("Select at least one judge attribute", source)
        self.assertIn("attributes will be judged", source)
        self.assertIn('id="sampleSize" type="number" min="1" max="10000" step="1"', source)
        self.assertIn("Transcript sample (1–10,000)", source)
        self.assertIn("d.sample_cap||10000", source)
        self.assertIn("Export audit CSV", source)
        self.assertIn("Candidate results plus full AI-judgment audit rows", source)
        self.assertIn("May include transcript-derived judge evidence", source)
        self.assertIn("full transcripts and raw judge envelopes are excluded", source)

    def test_ai_judging_audit_source_contract_and_privacy(self):
        source = Path(__file__).with_name("OPTIC_Model_Lab.html").read_text(encoding="utf-8")
        audit = source[
            source.index("function storedBlankRule"):
            source.index("function reportFilters")
        ]
        self.assertIn('["judging","AI judging"]', source)
        self.assertIn('id="tab-judging"', source)
        self.assertIn("function buildJudgeAuditItems(run)", audit)
        self.assertIn("run.judgments||[]", audit)
        self.assertIn("Array.isArray(judge.attributes)?judge.attributes", audit)
        self.assertIn("function judgeRequestedAttributes(judgment)", audit)
        self.assertIn("function judgeMissingAttributes(judgment)", audit)
        self.assertIn("function judgeExtraAttributeCount(judgment)", audit)
        self.assertIn("judgmentIncludesAttribute(judgment,attribute)", audit)
        self.assertIn("judgeRequestedAttributes(judgment).filter(attribute=>!Object.hasOwn(results,attribute))", audit)
        self.assertIn("Object.entries(mapping||{})", audit)
        self.assertIn("result.candidate_correctness", audit)
        self.assertIn("Blinded as ${candidate.label}", audit)
        self.assertIn("reversed for display only after judgment", audit)
        self.assertIn("AI proxy only—not human ground truth", audit)
        self.assertIn("Completed judge calls", audit)
        self.assertIn("Missing results", audit)
        self.assertIn("Error calls", audit)
        self.assertIn("Average confidence", audit)
        self.assertIn('["winner","Winner"]', audit)
        self.assertIn('["tie","Tie"]', audit)
        self.assertIn('["uncertain","Uncertain"]', audit)
        self.assertIn('["error","Error"]', audit)
        self.assertIn('["unjudged","Unjudged"]', audit)
        self.assertIn("Missing requested attrs", audit)
        self.assertIn("Extra judge attrs", audit)
        self.assertIn("Prompt tokens", audit)
        self.assertIn("Completion tokens", audit)
        self.assertIn("Transcript unavailable in this historical or partial run", audit)
        self.assertIn("if(!judge.enabled&&!judgments.length)", audit)
        self.assertIn("judgments.filter(item=>!item.error).length", audit)
        self.assertIn("Not allowed in stored metadata", audit)
        self.assertIn("without applying newer derived-policy rules", audit)
        self.assertIn("callIndex=new Map()", audit)
        self.assertIn("callIndex.get(`${analysisId}|${Number(batchIndex||0)}|${model}`)", audit)
        self.assertIn("const pageSize=100", audit)
        self.assertIn("Showing ${pageStart+1}-${pageStart+pageItems.length} of ${filtered.length}", audit)
        self.assertLess(
            audit.index("if(!judge.enabled&&!judgments.length)"),
            audit.index("const items=buildJudgeAuditItems(run)"),
        )
        candidate_builder = audit[
            audit.index("function buildJudgeCandidates"):
            audit.index("function buildJudgeAuditItems")
        ]
        self.assertNotIn("(run.calls||[]).filter", candidate_builder)
        self.assertNotIn("raw_output", audit)
        self.assertNotIn("innerHTML", audit)


if __name__ == "__main__":
    unittest.main()
