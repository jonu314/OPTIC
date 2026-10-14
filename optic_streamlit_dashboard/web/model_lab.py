"""Local OPTIC Model Lab engine.

The module is intentionally headless. Real Snowflake and Odin access is created only
for an explicit preflight or run, while deterministic demo implementations support
tests and local UI review without credentials or network calls.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import os
import random
import re
import threading
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import quote

try:
    import requests
except ImportError:  # pragma: no cover - surfaced only when a real call is requested
    requests = None


MAX_ATTRIBUTES_PER_BATCH = 20
MAX_SAMPLE_SIZE = 10_000
VERIFIED_GPT_MODELS = [
    "gpt-6.1-sol",
    "gpt-6-luna",
    "gpt-5.6-sol",
    "gpt-5.6-luna",
    "gpt-5.6-terra",
    "gpt-5.5",
    "gpt-5.4",
    "gpt-5.4-mini",
]
GPT_CANDIDATE_MODELS: list[str] = []
ANTHROPIC_CANDIDATE_MODELS = [
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
DEFAULT_MODELS = VERIFIED_GPT_MODELS + GPT_CANDIDATE_MODELS + ANTHROPIC_CANDIDATE_MODELS
MODEL_SLUG_RE = re.compile(r"^[a-z0-9](?:[a-z0-9.-]{0,62}[a-z0-9])?$")
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
RUN_ID_RE = re.compile(r"^[0-9a-f]{32}$")
BLANK = "<BLANK>"
MISSING = object()
FREE_TEXT_RESULT_TYPES = {"STRING", "TEXT", "VARCHAR", "FREEFORM"}

SERVICE_FALLBACK = (
    "Return call transcript analysis as the following JSON dictionary object. The current company "
    "will always be one of the following: TXU Energy, Ambit Energy, TriEagle Energy, Ambit, Txu, "
    "TriEagleTx, Dynegy, BetterBuy, EnergyHarbor, Crius, USGE, Veridian, Brighten, Homefield, "
    "PublicPower, TeepaNj, Universal4CHG, Residential4CHG, UniversalEXP, ResidentialEXP, "
    "UniversalVET, ResidentialVET - anything to the contrary is a transcription error and should "
    "be corrected into the one that best fits. TXU Energy, Ambit Energy, and TriEagle Energy will "
    "appear most frequently in transcripts, so choose whichever of these fits the context when "
    "ambiguous. Agents may refer to current company as a separate entity, so that is not exclusion "
    "criterion."
)
RETENTION_FALLBACK = (
    SERVICE_FALLBACK
    + " Finally, if it seems like customer and agent are mislabeled, flip them for the purposes "
    "of all your analyses."
)
ACQUISITION_FALLBACK = (
    "Return call transcript analysis as the following JSON dictionary object. The current company "
    "will always be one of the following: TXU Energy, Ambit Energy, TriEagle Energy, Ambit, Txu, "
    "TriEagleTx, Dynegy, BetterBuy, EnergyHarbor, Crius, USGE, Veridian, Brighten, Homefield, "
    "PublicPower, TeepaNj, Universal4CHG, Residential4CHG, UniversalEXP, ResidentialEXP, "
    "UniversalVET, ResidentialVET. Correct transcription errors to the best fit. When ambiguous, "
    "prefer TXU Energy, Ambit Energy, or TriEagle Energy based on context."
)

# Baseline agreement is deliberately secondary. Accuracy is never inferred from these weights.
OPERATIONAL_SCORE_WEIGHTS = {
    "request_success": 0.20,
    "parse_success": 0.20,
    "completeness": 0.20,
    "domain_validity": 0.25,
    "schema_compliance": 0.10,
    "baseline_agreement": 0.05,
}

DEMO_ATTRIBUTES = [
    {
        "attribute_id": 1,
        "name": "issue_unresolved",
        "description": "Whether the call ended without resolution.",
        "result_type": "BOOL",
        "options": ["1", "0"],
        "prompt": "Return '1' if the request remained unresolved; otherwise return '0'.",
        "allow_blank": False,
        "group": "Call outcome",
        "rownum": 1,
    },
    {
        "attribute_id": 2,
        "name": "call_type_svc",
        "description": "Primary service call category.",
        "result_type": "STRINGPICKONE",
        "options": ["Payment Arrangement", "High Bill", "Power Outage", "Other"],
        "prompt": "Choose exactly one supported service call category.",
        "allow_blank": False,
        "group": "Call basics",
        "rownum": 2,
    },
    {
        "attribute_id": 3,
        "name": "customer_sentiment",
        "description": "Customer sentiment at call conclusion.",
        "result_type": "STRINGPICKONE",
        "options": ["Positive", "Neutral", "Negative", "Mixed"],
        "prompt": "Classify the customer's sentiment at the conclusion of the call.",
        "allow_blank": False,
        "group": "Experience",
        "rownum": 3,
    },
    {
        "attribute_id": 4,
        "name": "agent_grammar_mistakes",
        "description": "Count of egregious agent grammar mistakes.",
        "result_type": "INTEGER",
        "options": [],
        "prompt": "Return the count of egregious agent grammar mistakes.",
        "allow_blank": False,
        "group": "Agent quality",
        "rownum": 4,
    },
    {
        "attribute_id": 5,
        "name": "competitors_mentioned",
        "description": "Competitor companies explicitly mentioned.",
        "result_type": "LIST",
        "options": ["Reliant", "Gexa", "Champion", "Direct Energy"],
        "prompt": "Return a JSON list of explicitly mentioned competitor companies.",
        "allow_blank": True,
        "group": "Market signals",
        "rownum": 5,
    },
    {
        "attribute_id": 6,
        "name": "call_summary",
        "description": "One-sentence call summary.",
        "result_type": "STRING",
        "options": [],
        "prompt": "Return a concise one-sentence summary of the call.",
        "allow_blank": False,
        "group": "Call outcome",
        "rownum": 6,
    },
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ModelLabError(Exception):
    """A safe error that may be returned to the local browser."""

    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


class IntegrationError(Exception):
    """An integration failure whose details should remain local and sanitized."""

    def __init__(self, message: str, retries: int = 0):
        super().__init__(message)
        self.retries = retries


@dataclass
class ApiResponse:
    status: int
    body: bytes
    content_type: str = "application/json; charset=utf-8"
    filename: str | None = None

    @classmethod
    def json(cls, payload: Any, status: int = 200) -> "ApiResponse":
        return cls(status, json.dumps(payload, default=json_default, allow_nan=False).encode("utf-8"))


def json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        value = default
    return max(minimum, min(maximum, value))


def gateway_path(name: str, default: str) -> str:
    value = os.getenv(name, default).strip()
    segments = value.split("/")[1:] if value.startswith("/") and not value.startswith("//") else []
    if (
        not segments
        or any(
            not segment
            or segment in {".", ".."}
            or not re.fullmatch(r"[A-Za-z0-9._~-]+", segment)
            for segment in segments
        )
    ):
        raise IntegrationError(f"{name} must be a safe absolute gateway path.")
    return value


def configured_models() -> list[str]:
    raw = os.getenv("OPTIC_MODEL_LAB_MODELS", "").strip()
    values = [part.strip().lower() for part in raw.split(",") if part.strip()] if raw else DEFAULT_MODELS
    valid = [value for value in values if MODEL_SLUG_RE.fullmatch(value)]
    return list(dict.fromkeys(valid)) or list(DEFAULT_MODELS)


def model_catalog(models: Iterable[str]) -> list[dict[str, Any]]:
    catalog = []
    for model in models:
        if model in VERIFIED_GPT_MODELS:
            catalog.append(
                {
                    "slug": model,
                    "provider": "openai",
                    "group": "Verified GPT",
                    "status": "verified",
                    "availability": "Verified HTTP 200 through the Odin gateway.",
                }
            )
        elif model in GPT_CANDIDATE_MODELS:
            catalog.append(
                {
                    "slug": model,
                    "provider": "openai",
                    "group": "GPT candidates",
                    "status": "candidate",
                    "availability": "Availability depends on Odin gateway access.",
                }
            )
        elif model.startswith("claude-"):
            catalog.append(
                {
                    "slug": model,
                    "provider": "anthropic",
                    "group": "Anthropic candidates",
                    "status": "candidate",
                    "availability": "Availability depends on Odin gateway access.",
                }
            )
        else:
            catalog.append(
                {
                    "slug": model,
                    "provider": "configured",
                    "group": "Environment configured",
                    "status": "configured",
                    "availability": "Configured explicitly; availability is not pre-verified.",
                }
            )
    return catalog


def pricing_config() -> dict[str, dict[str, float]]:
    raw = os.getenv("ODIN_MODEL_PRICING_JSON", "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    result: dict[str, dict[str, float]] = {}
    if not isinstance(parsed, dict):
        return result
    for model, values in parsed.items():
        if not isinstance(values, dict) or not MODEL_SLUG_RE.fullmatch(str(model)):
            continue
        try:
            input_rate = float(values.get("input_per_million"))
            output_rate = float(values.get("output_per_million"))
        except (TypeError, ValueError):
            continue
        if input_rate >= 0 and output_rate >= 0:
            result[str(model)] = {
                "input_per_million": input_rate,
                "output_per_million": output_rate,
            }
    return result


def normalize_result_type(value: Any) -> str:
    result_type = str(value or "").strip().upper()
    return {
        "BOOLEAN": "BOOL",
        "BOOL_EMPTY": "BOOLEMPTY",
        "STRING_PICK_ONE": "STRINGPICKONE",
        "PICKONE": "STRINGPICKONE",
        "STRINGMULTI": "LIST",
        "INT": "INTEGER",
        "FLOAT": "NUMBER",
        "DECIMAL": "NUMBER",
        "DOUBLE": "NUMBER",
        "TEXT": "STRING",
        "VARCHAR": "STRING",
        "FREEFORM": "STRING",
        "FREE_FORM": "STRING",
    }.get(result_type, result_type or "STRING")


def is_free_text_result_type(result_type: str, options: Iterable[Any] = ()) -> bool:
    return str(result_type or "").strip().upper() in FREE_TEXT_RESULT_TYPES and not list(options)


def prompt_allows_blank(spec: dict[str, Any]) -> bool:
    result_type = normalize_result_type(spec.get("result_type"))
    return (
        bool(spec.get("allow_blank"))
        or result_type == "BOOLEMPTY"
        or is_free_text_result_type(result_type, spec.get("options") or [])
    )


def judge_eligible_attributes(
    attributes: Iterable[dict[str, Any]], include_free_text: bool = False
) -> list[dict[str, Any]]:
    return [
        spec
        for spec in attributes
        if include_free_text
        or not is_free_text_result_type(
            normalize_result_type(spec.get("result_type")), spec.get("options") or []
        )
    ]


def default_judge_attribute_names(attributes: Iterable[dict[str, Any]]) -> list[str]:
    return [
        str(spec["name"])
        for spec in judge_eligible_attributes(attributes, include_free_text=False)
    ]


def configured_judge_attribute_names(config: dict[str, Any]) -> list[str]:
    judge = config.get("judge") or {}
    explicit = judge.get("attributes")
    if isinstance(explicit, list):
        return [str(name) for name in explicit]
    return [str(name) for name in config.get("attributes") or []]


def normalize_match_key(value: Any) -> str:
    if value is None:
        return BLANK
    text = str(value).strip().strip("'\"")
    if not text or text.upper() in {"NONE", "NULL", "N/A", "NOT_FOUND", BLANK}:
        return BLANK
    text = text.rstrip(".").replace("&", " AND ")
    text = re.sub(r"[_-]+", " ", text)
    text = re.sub(r"[^A-Za-z0-9/ .+-]+", "", text)
    return re.sub(r"\s+", " ", text).strip().upper() or BLANK


def parse_result_options(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        candidates = value
    elif isinstance(value, dict):
        candidates = list(value.values())
    else:
        raw = str(value).strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            candidates = parsed
        elif isinstance(parsed, dict):
            candidates = list(parsed.values())
        else:
            candidates = re.split(r"[\n;|,]+", raw)
    cleaned = [str(item).strip().strip("'\"") for item in candidates if str(item).strip()]
    return list(dict.fromkeys(item for item in cleaned if normalize_match_key(item) != BLANK))


class ResultValidationAdapter:
    """Uses Dashboard_Final's mature canonicalization in production when available."""

    def __init__(self, dashboard_module: Any | None = None):
        self.dashboard = dashboard_module

    def result_type(self, value: Any) -> str:
        if self.dashboard is not None:
            return self.dashboard.normalize_result_type_label(str(value or ""))
        return normalize_result_type(value)

    def options(self, value: Any) -> list[str]:
        if self.dashboard is not None:
            return self.dashboard.parse_result_value_options(value)
        return parse_result_options(value)

    def canonicalize(self, spec: dict[str, Any], value: Any) -> Any:
        result_type = self.result_type(spec.get("result_type"))
        options = list(spec.get("options") or [])
        allow_blank = bool(spec.get("allow_blank"))
        if value is None or (isinstance(value, str) and normalize_match_key(value) == BLANK):
            return BLANK
        if result_type in {"BOOL", "BOOLEMPTY"}:
            if isinstance(value, bool):
                return "1" if value else "0"
            key = normalize_match_key(value)
            if key in {"1", "TRUE", "YES", "Y"}:
                return "1"
            if key in {"0", "FALSE", "NO", "N"}:
                return "0"
            return str(value).strip()
        if result_type in {"NUMBER", "INTEGER"}:
            try:
                number = Decimal(str(value).strip().replace(",", ""))
            except (InvalidOperation, ValueError):
                return str(value).strip()
            if result_type == "INTEGER" and number == number.to_integral_value():
                return str(int(number))
            normalized = format(number.normalize(), "f")
            return normalized.rstrip("0").rstrip(".") if "." in normalized else normalized
        if result_type == "LIST":
            if isinstance(value, list):
                parts = value
            else:
                text = str(value).strip()
                if not text:
                    return [] if allow_blank else BLANK
                try:
                    parsed = json.loads(text)
                except json.JSONDecodeError:
                    parsed = None
                parts = parsed if isinstance(parsed, list) else re.split(r"[,;|]+", text)
            canonical_parts = [self._canonical_scalar(spec, part) for part in parts if normalize_match_key(part) != BLANK]
            return sorted(dict.fromkeys(canonical_parts), key=normalize_match_key)
        return self._canonical_scalar(spec, value)

    def _canonical_scalar(self, spec: dict[str, Any], value: Any) -> str:
        options = list(spec.get("options") or [])
        if self.dashboard is not None:
            return self.dashboard.canonicalize_output(str(spec.get("name") or ""), value, options)
        key = normalize_match_key(value)
        option_map = {normalize_match_key(option): option for option in options}
        return option_map.get(key, BLANK if key == BLANK else str(value).strip())

    def validate(self, spec: dict[str, Any], value: Any = MISSING) -> dict[str, Any]:
        name = str(spec["name"])
        result_type = self.result_type(spec.get("result_type"))
        options = list(spec.get("options") or [])
        allow_blank = bool(spec.get("allow_blank")) or result_type == "BOOLEMPTY"
        if value is MISSING:
            return {
                "attribute": name,
                "present": False,
                "valid": False,
                "canonical": None,
                "raw": None,
                "reason": "missing_key",
            }
        canonical = self.canonicalize(spec, value)
        blank = canonical == BLANK or canonical == []
        if blank:
            blank_free_text = (
                is_free_text_result_type(result_type, options)
                and isinstance(value, str)
                and not value.strip()
            )
            valid = allow_blank or blank_free_text
            reason = "blank_allowed" if valid else "blank_not_allowed"
        elif result_type in {"BOOL", "BOOLEMPTY"}:
            valid = canonical in {"1", "0"}
            reason = "valid" if valid else "invalid_boolean"
        elif result_type == "INTEGER":
            try:
                number = Decimal(str(value).strip().replace(",", ""))
                valid = number.is_finite() and number == number.to_integral_value()
            except (InvalidOperation, ValueError):
                valid = False
            reason = "valid" if valid else "invalid_integer"
        elif result_type == "NUMBER":
            try:
                valid = Decimal(str(value).strip().replace(",", "")).is_finite()
            except (InvalidOperation, ValueError):
                valid = False
            reason = "valid" if valid else "invalid_number"
        elif result_type == "STRINGPICKONE":
            allowed = {normalize_match_key(item) for item in options}
            valid = bool(allowed) and normalize_match_key(canonical) in allowed
            reason = "valid" if valid else "outside_allowed_options"
        elif result_type == "LIST":
            valid = isinstance(canonical, list)
            if valid and options:
                allowed = {normalize_match_key(item) for item in options}
                valid = all(normalize_match_key(item) in allowed for item in canonical)
            reason = "valid" if valid else "invalid_list_or_option"
        else:
            valid = isinstance(canonical, str) and bool(canonical.strip())
            reason = "valid" if valid else "invalid_string"
        return {
            "attribute": name,
            "present": True,
            "valid": bool(valid),
            "canonical": canonical,
            "raw": value,
            "reason": reason,
        }


def chunk_attributes(attributes: list[Any], size: int = MAX_ATTRIBUTES_PER_BATCH) -> list[list[Any]]:
    if size < 1 or size > MAX_ATTRIBUTES_PER_BATCH:
        raise ValueError(f"Batch size must be between 1 and {MAX_ATTRIBUTES_PER_BATCH}.")
    return [attributes[index:index + size] for index in range(0, len(attributes), size)]


def parse_json_object(text: Any) -> dict[str, Any]:
    if isinstance(text, dict):
        return dict(text)
    if not isinstance(text, str) or not text.strip():
        raise ValueError("Response did not contain JSON text.")
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError:
        start, end = cleaned.find("{"), cleaned.rfind("}")
        if start < 0 or end <= start:
            raise ValueError("Response did not contain a JSON object.")
        try:
            parsed = json.loads(cleaned[start:end + 1])
        except json.JSONDecodeError as exc:
            raise ValueError("Response contained invalid JSON.") from exc
    if not isinstance(parsed, dict):
        raise ValueError("Response JSON must be an object.")
    return parsed


def extract_attribute_layout(value: Any) -> tuple[list[str], dict[str, str]]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return [], {}
    ordered: list[str] = []
    groups: dict[str, str] = {}

    def add_item(item: Any, group: str) -> None:
        if isinstance(item, str) and item.strip():
            name = item.strip()
            ordered.append(name)
            groups.setdefault(name.lower(), group)
        elif isinstance(item, dict):
            lowered = {str(key).lower(): val for key, val in item.items()}
            for key in ("attributename", "attribute", "name", "resultattribute"):
                if isinstance(lowered.get(key), str) and lowered[key].strip():
                    name = lowered[key].strip()
                    ordered.append(name)
                    groups.setdefault(name.lower(), group)
                    return
            walk(item, group)

    def walk(node: Any, inherited_group: str = "Configured attributes") -> None:
        if isinstance(node, dict):
            lowered = {str(key).lower(): val for key, val in node.items()}
            group = inherited_group
            if "resultattributelist" in lowered:
                for key in ("promptname", "promptgroupname", "groupname", "title", "name"):
                    candidate = lowered.get(key)
                    if isinstance(candidate, str) and candidate.strip():
                        group = candidate.strip()
                        break
            for key, child in node.items():
                if str(key).lower() == "resultattributelist" and isinstance(child, list):
                    for item in child:
                        add_item(item, group)
                else:
                    walk(child, group)
        elif isinstance(node, list):
            for child in node:
                walk(child, inherited_group)

    walk(value)
    return list(dict.fromkeys(name.lower() for name in ordered)), groups


def extract_attribute_order(value: Any) -> list[str]:
    return extract_attribute_layout(value)[0]


def fallback_system_prompt(job_name: str) -> str:
    normalized = re.sub(r"[^A-Z]", "", str(job_name or "").upper())
    if "RETENTION" in normalized:
        return RETENTION_FALLBACK
    if "ACQUISITION" in normalized:
        return ACQUISITION_FALLBACK
    return SERVICE_FALLBACK


def build_batch_prompt(attributes: list[dict[str, Any]], transcript: str) -> str:
    if not attributes or len(attributes) > MAX_ATTRIBUTES_PER_BATCH:
        raise ValueError("A prompt batch must contain between 1 and 20 attributes.")
    keys = [str(spec["name"]) for spec in attributes]
    lines = [
        "Analyze the transcript using the exact attribute contracts below.",
        "Return one JSON dictionary and nothing else.",
        "The dictionary must contain exactly these keys, once each, in this order:",
        json.dumps(keys, ensure_ascii=True),
        "Do not add keys, commentary, markdown, or explanations.",
        "",
        "ATTRIBUTE CONTRACTS",
    ]
    for index, spec in enumerate(attributes, 1):
        options = list(spec.get("options") or [])
        lines.extend(
            [
                f"{index}. Exact JSON key: {json.dumps(str(spec['name']))}",
                f"   Instruction: {str(spec.get('prompt') or spec.get('description') or '').strip()}",
                f"   Result type: {normalize_result_type(spec.get('result_type'))}",
                f"   Allowed options: {json.dumps(options, ensure_ascii=True) if options else 'No finite option set'}",
                f"   Blank allowed: {'yes' if prompt_allows_blank(spec) else 'no'}",
            ]
        )
    lines.extend(["", "TRANSCRIPT", "<transcript>", transcript, "</transcript>"])
    return "\n".join(lines)


def build_judge_prompt(
    transcript: str,
    attributes: list[dict[str, Any]],
    outputs_by_model: dict[str, dict[str, Any]],
    seed_material: str,
) -> tuple[str, dict[str, str]]:
    models = sorted(outputs_by_model)
    rng = random.Random(int(hashlib.sha256(seed_material.encode("utf-8")).hexdigest()[:16], 16))
    rng.shuffle(models)
    labels = [f"Candidate {chr(65 + index)}" for index in range(len(models))]
    mapping = dict(zip(labels, models))
    blinded = {
        label: {str(spec["name"]): outputs_by_model[model].get(str(spec["name"])) for spec in attributes}
        for label, model in mapping.items()
    }
    specs = [
        {
            "key": spec["name"],
            "instruction": spec.get("prompt") or spec.get("description") or "",
            "result_type": normalize_result_type(spec.get("result_type")),
            "options": list(spec.get("options") or []),
            "blank_allowed": prompt_allows_blank(spec),
        }
        for spec in attributes
    ]
    prompt = "\n".join(
        [
            "Act as a strict, independent extraction evaluator.",
            "Deployment identities are hidden. Evaluate only transcript evidence and each attribute contract.",
            "Return one JSON object with an 'attributes' dictionary keyed by exact attribute key.",
            "For every attribute return: candidate_correctness (candidate label -> true/false/null), "
            "winner (candidate label, tie, or uncertain), confidence (0 through 1), rationale (max 20 words), "
            "and evidence (max 16 transcript words or empty).",
            "Use null and winner='uncertain' when evidence is insufficient. Do not infer human ground truth.",
            "",
            "ATTRIBUTE SPECS",
            json.dumps(specs, separators=(",", ":"), ensure_ascii=True),
            "",
            "BLINDED CANDIDATE OUTPUTS",
            json.dumps(blinded, separators=(",", ":"), ensure_ascii=True),
            "",
            "TRANSCRIPT",
            "<transcript>",
            transcript,
            "</transcript>",
        ]
    )
    return prompt, mapping


def estimate_judge_input_tokens(
    config: dict[str, Any],
    samples: list[dict[str, Any]],
    sample_will_use: int,
) -> int:
    if not (config.get("judge") or {}).get("enabled") or not samples or sample_will_use < 1:
        return 0
    judge_names = set(configured_judge_attribute_names(config))
    scoped_batches = [
        [spec for spec in batch if str(spec["name"]) in judge_names]
        for batch in chunk_attributes(config.get("attribute_specs") or [])
    ]
    scoped_batches = [batch for batch in scoped_batches if batch]
    if not scoped_batches:
        return 0
    preview_tokens = 0
    for sample_index, sample in enumerate(samples):
        transcript, _, _ = truncate_transcript(
            str(sample.get("transcript") or ""), int(config["transcript_char_limit"])
        )
        for batch_index, batch in enumerate(scoped_batches):
            placeholder_outputs = {
                model: {str(spec["name"]): None for spec in batch}
                for model in config["models"]
            }
            prompt, _ = build_judge_prompt(
                transcript,
                batch,
                placeholder_outputs,
                f"estimate:{sample_index}:{batch_index}",
            )
            preview_tokens += math.ceil(len(prompt) / 4)
    return math.ceil(preview_tokens / len(samples) * sample_will_use)


def validate_identifier_path(value: str) -> str:
    parts = value.split(".")
    if len(parts) != 3 or any(not IDENTIFIER_RE.fullmatch(part) for part in parts):
        raise ValueError(f"Invalid configured Snowflake identifier: {value!r}")
    return ".".join(f'"{part.upper()}"' for part in parts)


class SnowflakeRepository:
    PROMPT_TABLE = os.getenv("OPTIC_MODEL_LAB_PROMPT_TABLE", "RETAIL_PRD.LANDPOWER_ODS.ODIN_OPTIC_PROMPT")
    DETAIL_VIEW = os.getenv(
        "OPTIC_MODEL_LAB_DETAIL_VIEW", "RETAIL_PRD.LANDPOWER_ODS.VW_ODIN_OPTIC_PROMPTDETAILS"
    )
    ANALYSIS_TABLE = os.getenv(
        "OPTIC_MODEL_LAB_ANALYSIS_TABLE", "RETAIL_PRD.LANDPOWER_ODS.ODIN_OPTIC_ANALYSIS"
    )
    CONTENT_TABLE = os.getenv(
        "OPTIC_MODEL_LAB_CONTENT_TABLE", "RETAIL_PRD.LANDPOWER_ODS.ODIN_OPTIC_ANALYSISCONTENT"
    )
    JOB_TABLE = os.getenv(
        "OPTIC_MODEL_LAB_JOB_TABLE", "RETAIL_PRD.LANDPOWER_ODS.ODIN_OPTIC_LOOKJOBNAME"
    )
    RESULT_TABLE = os.getenv(
        "OPTIC_MODEL_LAB_RESULT_TABLE", "RETAIL_PRD.LANDPOWER_ODS.ODIN_DBO_OPTICRESULTS"
    )

    def __init__(self, connection_factory: Callable[..., Any]):
        self.connection_factory = connection_factory
        self.tables = {
            name: validate_identifier_path(value)
            for name, value in {
                "prompt": self.PROMPT_TABLE,
                "detail": self.DETAIL_VIEW,
                "analysis": self.ANALYSIS_TABLE,
                "content": self.CONTENT_TABLE,
                "job": self.JOB_TABLE,
                "result": self.RESULT_TABLE,
            }.items()
        }

    def _query(self, sql: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        conn = self.connection_factory()
        try:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, tuple(params))
                columns = [str(item[0]).lower() for item in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
            finally:
                cursor.close()
        finally:
            conn.close()

    def list_jobs(self) -> list[dict[str, Any]]:
        sql = f"""
        SELECT PROMPTID, JOBTYPE, JOBNAME, DESCRIPTION, ATTRIBUTEPROMPT, ATTRIBUTELIST,
               MODEL, TEMPERATURE, HISTORYSTARTDATE, HISTORYENDDATE
        FROM {self.tables['prompt']}
        WHERE COALESCE(ENABLED, FALSE) = TRUE
          AND UPPER(TRIM(JOBTYPE)) = 'AGENTINTERACTION'
          AND (
              HISTORYSTARTDATE IS NULL
              OR HISTORYSTARTDATE <= CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ
          )
          AND (
              HISTORYENDDATE IS NULL
              OR HISTORYENDDATE >= CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ
          )
        ORDER BY UPPER(JOBNAME), HISTORYSTARTDATE DESC NULLS LAST, PROMPTID DESC
        """
        jobs = []
        for row in self._query(sql):
            job_name = str(row.get("jobname") or "").strip()
            if not job_name:
                continue
            attribute_order, attribute_groups = extract_attribute_layout(row.get("attributelist"))
            jobs.append(
                {
                    "id": str(row.get("promptid")),
                    "prompt_id": row.get("promptid"),
                    "job_name": job_name,
                    "job_type": str(row.get("jobtype") or ""),
                    "description": str(row.get("description") or ""),
                    "system_prompt": str(row.get("attributeprompt") or "").strip() or fallback_system_prompt(job_name),
                    "attribute_order": attribute_order,
                    "attribute_groups": attribute_groups,
                    "configured_model": str(row.get("model") or ""),
                    "temperature": row.get("temperature"),
                    "history_start": row.get("historystartdate"),
                    "history_end": row.get("historyenddate"),
                }
            )
        return jobs

    def get_job(self, job_id: str) -> dict[str, Any]:
        for job in self.list_jobs():
            if job["id"] == str(job_id):
                return job
        raise ModelLabError("The selected active prompt no longer exists.", 404)

    def list_attributes(self, job: dict[str, Any]) -> list[dict[str, Any]]:
        sql = f"""
        SELECT JOBNAME, ROWNUM, ATTRIBUTEID, ATTRIBUTENAME, DESCRIPTION,
               RESULTVALUETYPE, RESULTVALUEOPTIONS, PROMPT
        FROM {self.tables['detail']}
        WHERE UPPER(TRIM(JOBNAME)) = UPPER(TRIM(%s))
        ORDER BY ROWNUM, ATTRIBUTEID
        """
        rows = self._query(sql, [job["job_name"]])
        attributes = []
        for row in rows:
            name = str(row.get("attributename") or "").strip()
            if not name:
                continue
            result_type = normalize_result_type(row.get("resultvaluetype"))
            raw_options = row.get("resultvalueoptions")
            options = parse_result_options(raw_options)
            raw_text = str(raw_options or "")
            allow_blank = (
                result_type == "BOOLEMPTY"
                or '""' in raw_text
                or "''" in raw_text
                or "<BLANK>" in raw_text.upper()
            )
            attributes.append(
                {
                    "attribute_id": row.get("attributeid"),
                    "name": name,
                    "description": str(row.get("description") or ""),
                    "result_type": result_type,
                    "options": options,
                    "prompt": str(row.get("prompt") or "").strip(),
                    "allow_blank": allow_blank,
                    "group": (job.get("attribute_groups") or {}).get(name.lower(), "Configured attributes"),
                    "rownum": row.get("rownum"),
                }
            )
        order = {name.lower(): index for index, name in enumerate(job.get("attribute_order") or [])}
        attributes.sort(
            key=lambda spec: (
                order.get(spec["name"].lower(), len(order) + int(spec.get("rownum") or 1_000_000)),
                int(spec.get("rownum") or 1_000_000),
                str(spec["name"]).lower(),
            )
        )
        return attributes

    def count_samples(self, job_name: str, start_date: str, end_date: str) -> int:
        sql = f"""
        SELECT COUNT(DISTINCT a.ANALYSISID) AS AVAILABLE
        FROM {self.tables['analysis']} a
        JOIN {self.tables['job']} j ON j.JOBNAMEID = a.JOBNAMEID
        WHERE a.INTERACTIONDATE >= %s::DATE
          AND a.INTERACTIONDATE < DATEADD(day, 1, %s::DATE)
          AND UPPER(TRIM(j.NAME)) = UPPER(TRIM(%s))
          AND EXISTS (
              SELECT 1 FROM {self.tables['content']} c
              WHERE c.ANALYSISID = a.ANALYSISID
                AND NULLIF(TRIM(c.CONTENT), '') IS NOT NULL
          )
        """
        rows = self._query(sql, [start_date, end_date, job_name])
        return int(rows[0]["available"]) if rows else 0

    def sample(
        self, job_name: str, start_date: str, end_date: str, sample_size: int, seed: int
    ) -> list[dict[str, Any]]:
        sql = f"""
        WITH analysis_sample AS (
            SELECT a.ID AS PRODUCTION_RESULT_ID, a.ANALYSISID, a.CONNID,
                   a.INTERACTIONDATE, j.NAME AS JOBNAME
            FROM {self.tables['analysis']} a
            JOIN {self.tables['job']} j ON j.JOBNAMEID = a.JOBNAMEID
            WHERE a.INTERACTIONDATE >= %s::DATE
              AND a.INTERACTIONDATE < DATEADD(day, 1, %s::DATE)
              AND UPPER(TRIM(j.NAME)) = UPPER(TRIM(%s))
              AND EXISTS (
                  SELECT 1 FROM {self.tables['content']} cx
                  WHERE cx.ANALYSISID = a.ANALYSISID
                    AND NULLIF(TRIM(cx.CONTENT), '') IS NOT NULL
              )
            ORDER BY HASH(TO_VARCHAR(a.ANALYSISID) || ':' || TO_VARCHAR(%s))
            LIMIT {int(sample_size)}
        ),
        chosen_content AS (
            SELECT s.ANALYSISID, c.ANALYSISCONTENTID, c.CONTENT
            FROM analysis_sample s
            JOIN {self.tables['content']} c ON c.ANALYSISID = s.ANALYSISID
            WHERE NULLIF(TRIM(c.CONTENT), '') IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY s.ANALYSISID
                ORDER BY LENGTH(TRIM(c.CONTENT)) DESC, c.ANALYSISCONTENTID DESC
            ) = 1
        ),
        baseline AS (
            SELECT r.ID AS PRODUCTION_RESULT_ID, r.RESULTJSON, r.MODEL, r.PROMPTVERSION,
                   r.PROMPTTOKENS, r.COMPLETIONTOKENS
            FROM {self.tables['result']} r
            JOIN analysis_sample s ON s.PRODUCTION_RESULT_ID = r.ID
                AND TRIM(TO_VARCHAR(r.CONNID)) = TRIM(TO_VARCHAR(s.CONNID))
                AND UPPER(TRIM(r.JOBNAME)) = UPPER(TRIM(s.JOBNAME))
                AND ABS(DATEDIFF(second, r.INTERACTIONDATE, s.INTERACTIONDATE)) <= 5
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY r.ID
                ORDER BY r.CREATEDDATE DESC NULLS LAST
            ) = 1
        )
        SELECT s.PRODUCTION_RESULT_ID, s.ANALYSISID, s.CONNID, s.INTERACTIONDATE, s.JOBNAME,
               c.ANALYSISCONTENTID, c.CONTENT, b.RESULTJSON AS BASELINE_RESULTJSON,
               b.MODEL AS BASELINE_MODEL, b.PROMPTVERSION AS BASELINE_PROMPTVERSION,
               b.PROMPTTOKENS AS BASELINE_PROMPTTOKENS,
               b.COMPLETIONTOKENS AS BASELINE_COMPLETIONTOKENS
        FROM analysis_sample s
        JOIN chosen_content c ON c.ANALYSISID = s.ANALYSISID
        LEFT JOIN baseline b ON b.PRODUCTION_RESULT_ID = s.PRODUCTION_RESULT_ID
        ORDER BY HASH(TO_VARCHAR(s.ANALYSISID) || ':' || TO_VARCHAR(%s))
        """
        rows = self._query(sql, [start_date, end_date, job_name, seed, seed])
        return [normalize_sample_row(row) for row in rows]


def normalize_sample_row(row: dict[str, Any]) -> dict[str, Any]:
    analysis_id = str(row.get("analysisid") or row.get("analysis_id") or "")
    return {
        "analysis_id": analysis_id,
        "transcript_analysis_id": analysis_id,
        "production_result_id": str(row.get("production_result_id") or ""),
        "baseline_match_method": "analysis.ID -> production result ID, connection/job/time checked",
        "source_id": str(row.get("connid") or analysis_id),
        "interaction_date": json_default(row.get("interactiondate")) if row.get("interactiondate") else None,
        "job_name": str(row.get("jobname") or ""),
        "analysis_content_id": row.get("analysiscontentid"),
        "transcript": str(row.get("content") or ""),
        "baseline": {
            "result_json": row.get("baseline_resultjson"),
            "model": row.get("baseline_model"),
            "prompt_version": row.get("baseline_promptversion"),
            "prompt_tokens": row.get("baseline_prompttokens"),
            "completion_tokens": row.get("baseline_completiontokens"),
        },
    }


class DemoRepository:
    def list_jobs(self) -> list[dict[str, Any]]:
        return [
            {
                "id": family.lower(),
                "prompt_id": family.lower(),
                "job_name": family.title(),
                "job_type": "AgentInteraction",
                "description": f"Deterministic local {family.title()} demonstration.",
                "system_prompt": fallback_system_prompt(family),
                "attribute_order": [spec["name"] for spec in DEMO_ATTRIBUTES],
                "configured_model": "demo",
                "temperature": None,
                "history_start": "2026-01-01",
                "history_end": None,
            }
            for family in ("service", "retention", "acquisition")
        ]

    def get_job(self, job_id: str) -> dict[str, Any]:
        for job in self.list_jobs():
            if job["id"] == str(job_id):
                return job
        raise ModelLabError("Unknown demo job.", 404)

    def list_attributes(self, job: dict[str, Any]) -> list[dict[str, Any]]:
        return [dict(spec) for spec in DEMO_ATTRIBUTES]

    def count_samples(self, job_name: str, start_date: str, end_date: str) -> int:
        return 250

    def sample(
        self, job_name: str, start_date: str, end_date: str, sample_size: int, seed: int
    ) -> list[dict[str, Any]]:
        templates = [
            "Customer: My bill is much higher than expected. Agent: I reviewed usage and explained the charges. "
            "Customer: That answers my question, thank you.",
            "Customer: My power is out and I need help. Agent: I confirmed an area outage and provided an estimate. "
            "Customer: I understand.",
            "Customer: I cannot pay the full balance today. Agent: We created a payment arrangement. "
            "Customer: The new schedule works for me.",
            "Customer: I am still confused about the final bill. Agent: I need to transfer you to another team. "
            "Customer: This has not been resolved.",
            "Customer: Reliant offered me a lower rate. Agent: I reviewed the current plan. "
            "Customer: I will think about the options.",
        ]
        rng = random.Random(seed)
        indexes = list(range(250))
        rng.shuffle(indexes)
        rows = []
        for number in indexes[:sample_size]:
            transcript = templates[number % len(templates)]
            analysis_id = f"DEMO-{number + 1:04d}"
            baseline = demo_values(analysis_id, "gpt-5.4", DEMO_ATTRIBUTES)
            rows.append(
                {
                    "analysis_id": analysis_id,
                    "source_id": f"CALL-{number + 1:04d}",
                    "interaction_date": start_date,
                    "job_name": job_name,
                    "analysis_content_id": number + 1,
                    "transcript": transcript,
                    "baseline": {
                        "result_json": json.dumps(baseline),
                        "model": "historical-demo",
                        "prompt_version": "demo-v1",
                        "prompt_tokens": 420,
                        "completion_tokens": 80,
                    },
                }
            )
        return rows


def demo_values(source: str, model: str, attributes: list[dict[str, Any]]) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for spec in attributes:
        name = str(spec["name"])
        digest = int(hashlib.sha256(f"{source}|{name}".encode("utf-8")).hexdigest()[:8], 16)
        model_shift = {"gpt-5.6-sol": 0, "gpt-5.6-luna": 0, "gpt-5.6-terra": 1, "gpt-5.5": 1}.get(model, 2)
        result_type = normalize_result_type(spec.get("result_type"))
        options = list(spec.get("options") or [])
        if result_type in {"BOOL", "BOOLEMPTY"}:
            value: Any = str((digest + model_shift) % 2)
        elif result_type == "STRINGPICKONE":
            value = options[(digest + model_shift) % len(options)] if options else "Other"
        elif result_type == "INTEGER":
            value = (digest + model_shift) % 4
        elif result_type == "NUMBER":
            value = round(((digest % 500) + model_shift) / 10, 1)
        elif result_type == "LIST":
            value = [] if digest % 3 else [options[(digest + model_shift) % len(options)]] if options else []
        else:
            value = f"Deterministic summary for {source.lower()}."
        values[name] = value
    return values


class OdinClient:
    def __init__(
        self,
        session: Any | None = None,
        sleep: Callable[[float], None] = time.sleep,
        rng: random.Random | None = None,
    ):
        if requests is None and session is None:
            raise IntegrationError("The requests package is required for Odin calls.")
        self.session = session or requests.Session()
        self.sleep = sleep
        self.rng = rng or random.Random()
        self.base_url = (
            os.getenv("ODIN_GATEWAY_BASE_URL")
            or os.getenv("ODIN_API_BASE_URL")
            or "https://prod.landpower.net/api/odin/gateway"
        ).rstrip("/")
        self.api_version = os.getenv("ODIN_API_VERSION", "2025-04-01-preview")
        self.timeout = env_int("ODIN_TIMEOUT_SECONDS", 180, 10, 900)
        self.max_retries = env_int("ODIN_MAX_RETRIES", 3, 0, 8)
        self.max_tokens = env_int("OPTIC_MODEL_LAB_MAX_COMPLETION_TOKENS", 4000, 256, 16000)
        self.reasoning_effort = os.getenv("OPTIC_MODEL_LAB_REASONING_EFFORT", "low")
        self.pricing = pricing_config()

    @staticmethod
    def token() -> str:
        for name in ("ODIN_API_TOKEN", "ODIN_BEARER_TOKEN", "ODIN_API_KEY"):
            raw = os.getenv(name, "").strip()
            if raw:
                return re.sub(r"^Bearer\s+", "", raw, flags=re.IGNORECASE).strip()
        raise IntegrationError("Odin credentials are not configured.")

    def call(self, deployment: str, system_prompt: str, user_prompt: str) -> dict[str, Any]:
        if not MODEL_SLUG_RE.fullmatch(deployment):
            raise IntegrationError("The Odin deployment slug is invalid.")
        token = self.token()
        is_anthropic = deployment.startswith("claude-")
        if is_anthropic:
            messages_path = gateway_path(
                "ODIN_ANTHROPIC_MESSAGES_PATH", "/anthropic/v1/messages"
            )
            anthropic_version = os.getenv(
                "ODIN_ANTHROPIC_VERSION", "2023-06-01"
            ).strip()
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", anthropic_version):
                raise IntegrationError("ODIN_ANTHROPIC_VERSION must use YYYY-MM-DD format.")
            url = f"{self.base_url}{messages_path}"
            body: dict[str, Any] = {
                "model": deployment,
                "system": system_prompt,
                "messages": [{"role": "user", "content": user_prompt}],
                "max_tokens": self.max_tokens,
            }
        else:
            url = (
                f"{self.base_url}/openai/deployments/{quote(deployment, safe='.-')}/chat/completions"
                f"?api-version={quote(self.api_version, safe='.-')}"
            )
            body = {
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "max_completion_tokens": self.max_tokens,
                "response_format": {"type": "json_object"},
            }
            if not re.match(r"^gpt-(?:5\.(?:5|6)|6(?:\.\d+)?)(?:[.-]|$)", deployment):
                body["temperature"] = 0
            if re.match(r"^gpt-(?:5\.(?:5|6)|6(?:\.\d+)?)(?:[.-]|$)", deployment):
                body["reasoning_effort"] = self.reasoning_effort
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        if is_anthropic:
            headers["anthropic-version"] = anthropic_version
        started = time.perf_counter()
        retries = 0
        last_error = "Odin request failed."
        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.post(url, headers=headers, json=body, timeout=self.timeout)
            except Exception as exc:
                last_error = f"Odin transport failure ({type(exc).__name__})."
                retryable = True
                response = None
            else:
                retryable = response.status_code == 429 or response.status_code >= 500
                if response.status_code < 400:
                    try:
                        payload = response.json()
                        if is_anthropic:
                            blocks = payload["content"]
                            if not isinstance(blocks, list):
                                raise ValueError("Anthropic content must be a list.")
                            text_blocks = [
                                block["text"]
                                for block in blocks
                                if isinstance(block, dict)
                                and block.get("type") == "text"
                                and isinstance(block.get("text"), str)
                            ]
                            if not text_blocks:
                                raise ValueError("Anthropic response has no text content.")
                            content = "".join(text_blocks)
                        else:
                            content = payload["choices"][0]["message"]["content"]
                    except (ValueError, KeyError, IndexError, TypeError) as exc:
                        raise IntegrationError("Odin returned an invalid response envelope.", retries) from exc
                    usage = payload.get("usage") or {}
                    prompt_tokens = int(
                        usage.get("input_tokens" if is_anthropic else "prompt_tokens") or 0
                    )
                    completion_tokens = int(
                        usage.get("output_tokens" if is_anthropic else "completion_tokens") or 0
                    )
                    returned_model = str(payload.get("model") or deployment)
                    cost = self._estimate_cost(deployment, prompt_tokens, completion_tokens)
                    return {
                        "content": content,
                        "latency_seconds": round(time.perf_counter() - started, 4),
                        "retries": retries,
                        "usage": {
                            "prompt_tokens": prompt_tokens,
                            "completion_tokens": completion_tokens,
                            "total_tokens": int(usage.get("total_tokens") or prompt_tokens + completion_tokens),
                        },
                        "requested_model": deployment,
                        "returned_model": returned_model,
                        "estimated_cost_usd": cost,
                    }
                if response.status_code in (401, 403):
                    last_error = "Odin authorization failed."
                else:
                    last_error = f"Odin returned HTTP {response.status_code}."
                if not retryable:
                    raise IntegrationError(last_error, retries)
            if attempt >= self.max_retries or not retryable:
                break
            retries += 1
            delay = self._retry_delay(response, attempt)
            self.sleep(delay)
        raise IntegrationError(last_error, retries)

    def _retry_delay(self, response: Any | None, attempt: int) -> float:
        if response is not None:
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    return min(60.0, max(0.0, float(retry_after)))
                except ValueError:
                    try:
                        parsed = parsedate_to_datetime(retry_after)
                        return min(60.0, max(0.0, parsed.timestamp() - time.time()))
                    except (TypeError, ValueError):
                        pass
        return min(30.0, (2 ** attempt) + self.rng.uniform(0, 0.75))

    def _estimate_cost(self, model: str, prompt_tokens: int, completion_tokens: int) -> float | None:
        rates = self.pricing.get(model)
        if not rates:
            return None
        return round(
            prompt_tokens / 1_000_000 * rates["input_per_million"]
            + completion_tokens / 1_000_000 * rates["output_per_million"],
            8,
        )


class DemoOdinClient:
    def __init__(self, validator: ResultValidationAdapter):
        self.validator = validator

    def call(self, deployment: str, system_prompt: str, user_prompt: str) -> dict[str, Any]:
        transcript_match = re.search(r"<transcript>\n(.*?)\n</transcript>", user_prompt, re.DOTALL)
        transcript = transcript_match.group(1) if transcript_match else ""
        if "BLINDED CANDIDATE OUTPUTS" in user_prompt:
            parsed = self._judge(user_prompt)
        else:
            keys_match = re.search(r"in this order:\n(\[[^\n]+\])", user_prompt)
            keys = json.loads(keys_match.group(1)) if keys_match else []
            specs = [spec for spec in DEMO_ATTRIBUTES if spec["name"] in keys]
            parsed = demo_values(hashlib.sha1(transcript.encode("utf-8")).hexdigest()[:10], deployment, specs)
            noise = int(hashlib.sha1(f"{deployment}|{transcript}".encode("utf-8")).hexdigest()[:4], 16)
            if deployment.endswith("terra") and noise % 7 == 0 and keys:
                parsed[keys[0]] = "invalid"
            if deployment == "gpt-5.4" and noise % 5 == 0 and keys:
                parsed.pop(keys[-1], None)
        prompt_tokens = max(1, len(user_prompt) // 4)
        completion_tokens = max(1, len(json.dumps(parsed)) // 4)
        return {
            "content": json.dumps(parsed),
            "latency_seconds": round(0.035 + (len(deployment) % 5) * 0.006, 4),
            "retries": 0,
            "usage": {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": prompt_tokens + completion_tokens,
            },
            "requested_model": deployment,
            "returned_model": f"{deployment}-demo",
            "estimated_cost_usd": None,
        }

    @staticmethod
    def _judge(prompt: str) -> dict[str, Any]:
        spec_text = prompt.split("ATTRIBUTE SPECS\n", 1)[1].split("\n\nBLINDED", 1)[0]
        output_text = prompt.split("BLINDED CANDIDATE OUTPUTS\n", 1)[1].split("\n\nTRANSCRIPT", 1)[0]
        specs = json.loads(spec_text)
        candidates = json.loads(output_text)
        result = {}
        for spec in specs:
            key = spec["key"]
            labels = list(candidates)
            values = [json.dumps(candidates[label].get(key), sort_keys=True) for label in labels]
            winner = "tie" if len(set(values)) == 1 else labels[0]
            result[key] = {
                "candidate_correctness": {label: (label == winner or winner == "tie") for label in labels},
                "winner": winner,
                "confidence": 0.72 if winner != "tie" else 0.9,
                "rationale": "Best supported by the transcript evidence.",
                "evidence": "",
            }
        return {"attributes": result}


_HELD_RUN_LOCKS: set[str] = set()
_HELD_RUN_LOCKS_GUARD = threading.Lock()


class RunExecutionLock:
    def __init__(self, path: Path):
        self.path = path
        self.handle: Any | None = None
        self.key = str(path.resolve())

    def acquire(self) -> bool:
        with _HELD_RUN_LOCKS_GUARD:
            if self.key in _HELD_RUN_LOCKS:
                return False
            _HELD_RUN_LOCKS.add(self.key)
        try:
            self.handle = self.path.open("a+b")
            self.handle.seek(0, os.SEEK_END)
            if self.handle.tell() == 0:
                self.handle.write(b"\0")
                self.handle.flush()
            self.handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except (OSError, ImportError):
            if self.handle is not None:
                self.handle.close()
                self.handle = None
            with _HELD_RUN_LOCKS_GUARD:
                _HELD_RUN_LOCKS.discard(self.key)
            return False

    def release(self) -> None:
        if self.handle is None:
            return
        try:
            self.handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.handle.close()
            self.handle = None
            with _HELD_RUN_LOCKS_GUARD:
                _HELD_RUN_LOCKS.discard(self.key)


class RunStore:
    def __init__(self, root: Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self._mark_interrupted()

    def _path(self, run_id: str) -> Path:
        if not RUN_ID_RE.fullmatch(run_id):
            raise ModelLabError("Invalid run id.", 404)
        return self.root / f"{run_id}.json"

    def acquire_execution_lock(self, run_id: str) -> RunExecutionLock | None:
        self._path(run_id)
        execution_lock = RunExecutionLock(self.root / f"{run_id}.lock")
        return execution_lock if execution_lock.acquire() else None

    def save(self, run: dict[str, Any]) -> None:
        run["updated_at"] = utc_now()
        with self.lock:
            self._save_unlocked(run)

    def _save_unlocked(self, run: dict[str, Any]) -> None:
        target = self._path(str(run["id"]))
        temp = target.with_suffix(f".{uuid.uuid4().hex}.tmp")
        payload = json.dumps(run, default=json_default, ensure_ascii=True, allow_nan=False)
        try:
            temp.write_text(payload, encoding="utf-8")
            for attempt in range(6):
                try:
                    os.replace(temp, target)
                    break
                except PermissionError:
                    if attempt == 5:
                        raise
                    time.sleep(0.01 * (2 ** attempt))
        finally:
            if temp.exists():
                temp.unlink()

    def update(
        self, run_id: str, updater: Callable[[dict[str, Any]], dict[str, Any] | None]
    ) -> dict[str, Any]:
        path = self._path(run_id)
        with self.lock:
            if not path.exists():
                raise ModelLabError("Run not found.", 404)
            run = json.loads(path.read_text(encoding="utf-8"))
            updated = updater(run) or run
            updated["updated_at"] = utc_now()
            self._save_unlocked(updated)
            return updated

    def load(self, run_id: str) -> dict[str, Any]:
        path = self._path(run_id)
        if not path.exists():
            raise ModelLabError("Run not found.", 404)
        with self.lock:
            return json.loads(path.read_text(encoding="utf-8"))

    def list(self) -> list[dict[str, Any]]:
        summaries = []
        with self.lock:
            paths = sorted(self.root.glob("*.json"), key=lambda path: path.stat().st_mtime, reverse=True)
            for path in paths[:200]:
                try:
                    run = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    continue
                summaries.append(run_summary(run))
        return summaries

    def _mark_interrupted(self) -> None:
        for path in self.root.glob("*.json"):
            try:
                run = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if run.get("status") in {"queued", "running", "cancelling"}:
                execution_lock = self.acquire_execution_lock(str(run.get("id") or ""))
                if execution_lock is None:
                    continue
                try:
                    try:
                        current = json.loads(path.read_text(encoding="utf-8"))
                    except (OSError, json.JSONDecodeError):
                        continue
                    if current.get("status") in {"queued", "running", "cancelling"}:
                        current["status"] = "failed"
                        current["recoverable"] = True
                        current["error"] = "The local server stopped before this run completed."
                        current["updated_at"] = utc_now()
                        self._save_unlocked(current)
                finally:
                    execution_lock.release()


def run_summary(run: dict[str, Any]) -> dict[str, Any]:
    config = run.get("config") or {}
    report = run.get("report") or {}
    judge = config.get("judge") or {}
    return {
        "id": run.get("id"),
        "status": run.get("status"),
        "created_at": run.get("created_at"),
        "updated_at": run.get("updated_at"),
        "job_name": config.get("job_name"),
        "models": config.get("models") or [],
        "sample_size": config.get("sample_size"),
        "attribute_count": len(config.get("attributes") or []),
        "judge_attribute_count": (
            len(configured_judge_attribute_names(config)) if judge.get("enabled") else 0
        ),
        "mode": config.get("mode"),
        "progress": run.get("progress") or {},
        "leader": (report.get("recommendation") or {}).get("model"),
        "recoverable": bool(run.get("recoverable")),
        "error": run.get("error"),
    }


def truncate_transcript(text: str, limit: int) -> tuple[str, bool, int]:
    original_length = len(text)
    if original_length <= limit:
        return text, False, original_length
    return text[:limit], True, original_length


def candidate_call(
    client: Any,
    validator: ResultValidationAdapter,
    model: str,
    system_prompt: str,
    transcript: dict[str, Any],
    batch: list[dict[str, Any]],
    batch_index: int,
) -> dict[str, Any]:
    user_prompt = build_batch_prompt(batch, transcript["transcript"])
    record = {
        "analysis_id": transcript["analysis_id"],
        "source_id": transcript["source_id"],
        "model": model,
        "batch_index": batch_index,
        "attributes": [spec["name"] for spec in batch],
        "request_success": False,
        "parse_success": False,
        "extra_keys": [],
        "values": {},
        "validations": [],
        "error": None,
        "latency_seconds": None,
        "retries": 0,
        "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
        "estimated_cost_usd": None,
        "returned_model": None,
    }
    try:
        response = client.call(model, system_prompt, user_prompt)
        record.update(
            {
                "request_success": True,
                "latency_seconds": response.get("latency_seconds"),
                "retries": int(response.get("retries") or 0),
                "usage": response.get("usage") or record["usage"],
                "estimated_cost_usd": response.get("estimated_cost_usd"),
                "returned_model": response.get("returned_model"),
                "raw_output": response.get("content"),
            }
        )
        parsed = parse_json_object(response.get("content"))
        record["parse_success"] = True
        record["values"] = parsed
        expected = {str(spec["name"]) for spec in batch}
        record["extra_keys"] = sorted(str(key) for key in parsed if str(key) not in expected)
        record["validations"] = [validator.validate(spec, parsed.get(spec["name"], MISSING)) for spec in batch]
    except IntegrationError as exc:
        record["error"] = str(exc)
        record["retries"] = exc.retries
        record["validations"] = [validator.validate(spec, MISSING) for spec in batch]
    except ValueError as exc:
        record["error"] = str(exc)
        record["validations"] = [validator.validate(spec, MISSING) for spec in batch]
    return record


def select_judge_attributes(
    calls: list[dict[str, Any]],
    attributes: list[dict[str, Any]],
    models: list[str],
    scope: str,
) -> list[dict[str, Any]]:
    if scope == "all":
        return list(attributes)
    selected = []
    by_model = {call["model"]: call for call in calls}
    for spec in attributes:
        name = str(spec["name"])
        values = []
        invalid = False
        for model in models:
            call = by_model.get(model)
            validation = next(
                (item for item in (call or {}).get("validations", []) if item["attribute"] == name),
                None,
            )
            if not call or not call.get("request_success") or not call.get("parse_success") or not validation:
                invalid = True
                values.append(("missing", model))
            elif not validation.get("valid"):
                invalid = True
                values.append(("invalid", json.dumps(validation.get("canonical"), sort_keys=True)))
            else:
                values.append(("valid", json.dumps(validation.get("canonical"), sort_keys=True)))
        if invalid or len(set(values)) > 1:
            selected.append(spec)
    return selected


def normalize_judge_output(
    payload: dict[str, Any],
    mapping: dict[str, str],
    attributes_requested: Iterable[str],
) -> tuple[dict[str, Any], list[str], int]:
    raw_attrs = payload.get("attributes")
    if not isinstance(raw_attrs, dict):
        raise ValueError("Judge JSON must contain an attributes object.")
    requested = list(dict.fromkeys(str(attribute) for attribute in attributes_requested))
    requested_set = set(requested)
    raw_by_name = {str(attribute): result for attribute, result in raw_attrs.items()}
    normalized = {}
    missing = []
    for attribute in requested:
        result = raw_by_name.get(attribute)
        if not isinstance(result, dict):
            missing.append(attribute)
            continue
        correctness = result.get("candidate_correctness") or {}
        normalized_correctness = {}
        for label, model in mapping.items():
            value = correctness.get(label) if isinstance(correctness, dict) else None
            normalized_correctness[label] = value if isinstance(value, bool) else None
        winner = str(result.get("winner") or "uncertain")
        if winner not in mapping and winner.lower() not in {"tie", "uncertain"}:
            winner = "uncertain"
        try:
            confidence = max(0.0, min(1.0, float(result.get("confidence") or 0)))
        except (TypeError, ValueError):
            confidence = 0.0
        normalized[attribute] = {
            "candidate_correctness": normalized_correctness,
            "winner": winner,
            "confidence": confidence,
            "rationale": " ".join(str(result.get("rationale") or "").split()[:20]),
            "evidence": " ".join(str(result.get("evidence") or "").split()[:16]),
        }
    extra_count = sum(attribute not in requested_set for attribute in raw_by_name)
    return normalized, missing, extra_count


class RunManager:
    def __init__(
        self,
        store: RunStore,
        repository_factory: Callable[[str], Any],
        client_factory: Callable[[str], Any],
        validator: ResultValidationAdapter,
    ):
        self.store = store
        self.repository_factory = repository_factory
        self.client_factory = client_factory
        self.validator = validator
        self.lock = threading.RLock()
        self.cancel_events: dict[str, threading.Event] = {}
        self.signatures: dict[str, str] = {}
        self.execution_locks: dict[str, RunExecutionLock] = {}

    def start(self, config: dict[str, Any], transcripts: list[dict[str, Any]] | None = None) -> tuple[dict[str, Any], bool]:
        signature = hashlib.sha256(
            json.dumps(config, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        with self.lock:
            existing_id = self.signatures.get(signature)
            if existing_id:
                existing = self.store.load(existing_id)
                if existing.get("status") in {"queued", "running", "cancelling"}:
                    return existing, True
            run_id = uuid.uuid4().hex
            run = {
                "id": run_id,
                "signature": signature,
                "status": "queued",
                "created_at": utc_now(),
                "updated_at": utc_now(),
                "config": config,
                "progress": {
                    "phase": "queued",
                    "candidate_completed": 0,
                    "candidate_total": config["estimated_candidate_calls"],
                    "judge_completed": 0,
                    "judge_total_max": config["max_judge_calls"] if config["judge"]["enabled"] else 0,
                    "errors": 0,
                    "retries": 0,
                    "per_model": {
                        model: {"completed": 0, "total": config["sample_size"] * config["batch_count"]}
                        for model in config["models"]
                    },
                    "per_batch": {
                        str(index + 1): {
                            "completed": 0,
                            "total": config["sample_size"] * len(config["models"]),
                        }
                        for index in range(config["batch_count"])
                    },
                },
                "transcripts": transcripts or [],
                "attributes": config["attribute_specs"],
                "calls": [],
                "judgments": [],
                "manual_labels": {},
                "report": {},
                "error": None,
                "recoverable": False,
            }
            execution_lock = self.store.acquire_execution_lock(run_id)
            if execution_lock is None:
                raise ModelLabError("The run is already active in another local server.", 409)
            try:
                self.store.save(run)
            except Exception:
                execution_lock.release()
                raise
            event = threading.Event()
            self.cancel_events[run_id] = event
            self.signatures[signature] = run_id
            self.execution_locks[run_id] = execution_lock
            thread = threading.Thread(target=self._execute, args=(run_id, event), daemon=True)
            try:
                thread.start()
            except Exception:
                self.execution_locks.pop(run_id, None)
                execution_lock.release()
                raise
            return run, False

    def resume(self, run_id: str) -> dict[str, Any]:
        """Continue a saved run under its original id, prompts and sampled calls."""
        with self.lock:
            run = self.store.load(run_id)
            if run.get("status") == "completed":
                return run
            if run_id in self.cancel_events:
                raise ModelLabError("The run is already active.", 409)
            execution_lock = self.store.acquire_execution_lock(run_id)
            if execution_lock is None:
                raise ModelLabError("The run is already active in another local server.", 409)
            event = threading.Event()
            try:
                run["status"] = "queued"
                run["error"] = None
                run["recoverable"] = False
                self.store.save(run)
                self.cancel_events[run_id] = event
                self.signatures[run["signature"]] = run_id
                self.execution_locks[run_id] = execution_lock
                thread = threading.Thread(target=self._execute, args=(run_id, event), daemon=True)
                thread.start()
            except Exception:
                self.cancel_events.pop(run_id, None)
                self.execution_locks.pop(run_id, None)
                if self.signatures.get(run["signature"]) == run_id:
                    self.signatures.pop(run["signature"], None)
                execution_lock.release()
                raise
            return run

    def cancel(self, run_id: str) -> dict[str, Any]:
        with self.lock:
            event = self.cancel_events.get(run_id)
            run = self.store.load(run_id)
            if run.get("status") not in {"queued", "running", "cancelling"}:
                return run
            run["status"] = "cancelling"
            run["progress"]["phase"] = "cancelling"
            self.store.save(run)
            if event:
                event.set()
            return run

    def _execute(self, run_id: str, cancel_event: threading.Event) -> None:
        run = self.store.load(run_id)
        config = run["config"]
        try:
            run["status"] = "running"
            run["progress"]["phase"] = "sampling"
            self.store.save(run)
            repository = self.repository_factory(config["mode"])
            client = self.client_factory(config["mode"])
            if hasattr(client, "max_tokens") and config.get("max_completion_tokens"):
                client.max_tokens = config["max_completion_tokens"]
            if hasattr(client, "reasoning_effort") and config.get("reasoning_effort"):
                client.reasoning_effort = config["reasoning_effort"]
            samples = run.get("transcripts") or repository.sample(
                config["job_name"],
                config["start_date"],
                config["end_date"],
                config["sample_size"],
                config["seed"],
            )
            if not samples:
                raise IntegrationError("No transcripts matched the selected sample.")
            transcript_limit = config["transcript_char_limit"]
            for sample in samples:
                capped, truncated, original_length = truncate_transcript(sample["transcript"], transcript_limit)
                sample["transcript"] = capped
                sample["truncated"] = truncated or bool(sample.get("truncated"))
                sample["original_characters"] = max(original_length, int(sample.get("original_characters") or 0))
            run["transcripts"] = samples
            if cancel_event.is_set():
                run["status"] = "cancelled"
                run["progress"]["phase"] = "cancelled"
                run["report"] = compute_report(run, self.validator)
                self.store.save(run)
                return
            batches = chunk_attributes(config["attribute_specs"])
            completed_candidates = {
                (call["model"], call["analysis_id"], call["batch_index"])
                for call in run["calls"]
            }
            run["progress"]["phase"] = "candidates"
            run["progress"]["candidate_total"] = len(samples) * len(config["models"]) * len(batches)
            self.store.save(run)

            executor = ThreadPoolExecutor(max_workers=config["concurrency"], thread_name_prefix="optic-lab")
            task_iterator = (
                (model, transcript, batch, batch_index)
                for transcript in samples
                for batch_index, batch in enumerate(batches)
                for model in config["models"]
                if (model, transcript["analysis_id"], batch_index) not in completed_candidates
            )
            futures: dict[Any, str] = {}

            def fill_candidate_slots() -> None:
                while len(futures) < config["concurrency"] and not cancel_event.is_set():
                    try:
                        model, transcript, batch, batch_index = next(task_iterator)
                    except StopIteration:
                        return
                    future = executor.submit(
                        candidate_call,
                        client,
                        self.validator,
                        model,
                        config["system_prompt"],
                        transcript,
                        batch,
                        batch_index,
                    )
                    futures[future] = model

            def record_candidate(future: Any) -> None:
                call = future.result()
                run["calls"].append(call)
                progress = run["progress"]
                progress["candidate_completed"] += 1
                progress["errors"] += 1 if call.get("error") else 0
                progress["retries"] += int(call.get("retries") or 0)
                progress["per_model"][call["model"]]["completed"] += 1
                progress["per_batch"][str(call["batch_index"] + 1)]["completed"] += 1

            fill_candidate_slots()
            checkpoint_every = 1  # persist every returned request before scheduling more work
            last_checkpoint = time.monotonic()
            try:
                while futures:
                    completed, _ = wait(futures, return_when=FIRST_COMPLETED)
                    for future in completed:
                        futures.pop(future, None)
                        record_candidate(future)
                        progress = run["progress"]
                        now = time.monotonic()
                        if (
                            progress["candidate_completed"] % checkpoint_every == 0
                            or now - last_checkpoint >= 1.0
                        ):
                            self.store.save(run)
                            last_checkpoint = now
                    if cancel_event.is_set():
                        for pending in futures:
                            pending.cancel()
                        break
                    fill_candidate_slots()
            finally:
                executor.shutdown(wait=True, cancel_futures=True)
            for future in list(futures):
                if not future.cancelled():
                    record_candidate(future)
            self.store.save(run)
            if cancel_event.is_set():
                run["status"] = "cancelled"
                run["progress"]["phase"] = "cancelled"
                run["report"] = compute_report(run, self.validator)
                self.store.save(run)
                return

            if config["judge"]["enabled"]:
                run["progress"]["phase"] = "judge"
                judge_attribute_names = set(configured_judge_attribute_names(config))
                calls_by_key: dict[tuple[str, int], list[dict[str, Any]]] = {}
                for call in run["calls"]:
                    calls_by_key.setdefault((call["analysis_id"], call["batch_index"]), []).append(call)
                transcripts = {item["analysis_id"]: item for item in run["transcripts"]}
                judge_model = config["judge"]["model"]
                completed_judgments = {
                    (item["analysis_id"], item["batch_index"]) for item in run["judgments"]
                }
                judge_checkpoint_every = 1
                last_judge_checkpoint = time.monotonic()
                for (analysis_id, batch_index), calls in calls_by_key.items():
                    if (analysis_id, batch_index) in completed_judgments:
                        continue
                    if cancel_event.is_set():
                        break
                    batch = batches[batch_index]
                    scoped_batch = [
                        spec for spec in batch if str(spec["name"]) in judge_attribute_names
                    ]
                    if not scoped_batch:
                        continue
                    selected = select_judge_attributes(
                        calls, scoped_batch, config["models"], config["judge"]["scope"]
                    )
                    if not selected:
                        continue
                    outputs_by_model = {call["model"]: call.get("values") or {} for call in calls}
                    prompt, mapping = build_judge_prompt(
                        transcripts[analysis_id]["transcript"],
                        selected,
                        outputs_by_model,
                        f"{config['seed']}:{run['signature']}:{analysis_id}:{batch_index}",
                    )
                    judgment = {
                        "analysis_id": analysis_id,
                        "batch_index": batch_index,
                        "attributes_requested": [spec["name"] for spec in selected],
                        "mapping": mapping,
                        "results": {},
                        "missing_attributes": [spec["name"] for spec in selected],
                        "extra_attribute_count": 0,
                        "error": None,
                    }
                    try:
                        response = client.call(
                            judge_model,
                            "You are a strict JSON extraction evaluator. Never reveal or infer model identities.",
                            prompt,
                        )
                        judgment["latency_seconds"] = response.get("latency_seconds")
                        judgment["usage"] = response.get("usage")
                        judgment["estimated_cost_usd"] = response.get("estimated_cost_usd")
                        judgment["retries"] = int(response.get("retries") or 0)
                        judgment["raw_output"] = response.get("content")
                        (
                            judgment["results"],
                            judgment["missing_attributes"],
                            judgment["extra_attribute_count"],
                        ) = normalize_judge_output(
                            parse_json_object(response.get("content")),
                            mapping,
                            judgment["attributes_requested"],
                        )
                        run["progress"]["retries"] += judgment["retries"]
                    except (IntegrationError, ValueError) as exc:
                        judgment["error"] = str(exc)
                        judgment["retries"] = getattr(exc, "retries", 0)
                        run["progress"]["errors"] += 1
                    run["judgments"].append(judgment)
                    run["progress"]["judge_completed"] += 1
                    now = time.monotonic()
                    if (
                        run["progress"]["judge_completed"] % judge_checkpoint_every == 0
                        or now - last_judge_checkpoint >= 1.0
                    ):
                        self.store.save(run)
                        last_judge_checkpoint = now
                self.store.save(run)
            if cancel_event.is_set():
                run["status"] = "cancelled"
                run["progress"]["phase"] = "cancelled"
            else:
                run["status"] = "completed"
                run["progress"]["phase"] = "completed"
            run["report"] = compute_report(run, self.validator)
            self.store.save(run)
        except Exception as exc:  # keep the async worker honest without leaking secrets
            run["status"] = "failed"
            run["progress"]["phase"] = "failed"
            run["error"] = str(exc) if isinstance(exc, (IntegrationError, ModelLabError)) else "Run execution failed."
            run["report"] = compute_report(run, self.validator)
            self.store.save(run)
        finally:
            with self.lock:
                self.cancel_events.pop(run_id, None)
                if self.signatures.get(run.get("signature")) == run_id:
                    self.signatures.pop(run["signature"], None)
                execution_lock = self.execution_locks.pop(run_id, None)
                if execution_lock is not None:
                    execution_lock.release()


def ratio(numerator: float, denominator: float) -> float | None:
    return round(numerator / denominator, 6) if denominator else None


def values_equal(left: Any, right: Any) -> bool:
    return json.dumps(left, sort_keys=True, default=json_default) == json.dumps(
        right, sort_keys=True, default=json_default
    )


def candidate_coverage(
    run: dict[str, Any],
    models: list[str],
    specs: list[dict[str, Any]],
    transcripts: list[dict[str, Any]],
    calls: list[dict[str, Any]],
) -> dict[str, Any]:
    status = str(run.get("status") or "completed")
    batch_count = math.ceil(len(specs) / MAX_ATTRIBUTES_PER_BATCH) if specs else 0
    analysis_ids = {
        str(transcript.get("analysis_id")) for transcript in transcripts
    }
    model_set = set(models)
    expected_count = len(analysis_ids) * batch_count * len(model_set)
    recorded_keys = {
        (str(call.get("analysis_id")), call.get("batch_index"), str(call.get("model")))
        for call in calls
    }
    progress = run.get("progress") or {}
    declared_total = int(progress.get("candidate_total") or 0)
    declared_completed = int(progress.get("candidate_completed") or len(calls))
    complete = (
        status == "completed"
        and expected_count > 0
        and len(recorded_keys) == expected_count
        and all(
            analysis_id in analysis_ids
            and isinstance(batch_index, int)
            and 0 <= batch_index < batch_count
            and model in model_set
            for analysis_id, batch_index, model in recorded_keys
        )
        and (not declared_total or declared_completed >= declared_total)
        and (not declared_total or len(calls) >= declared_total)
    )
    successful_parsed_calls = sum(
        bool(call.get("request_success")) and bool(call.get("parse_success")) for call in calls
    )
    valid_outputs = sum(
        bool(validation.get("present")) and bool(validation.get("valid"))
        for call in calls
        for validation in call.get("validations", [])
    )
    return {
        "status": status,
        "expected_candidate_calls": max(expected_count, declared_total),
        "recorded_candidate_calls": len(recorded_keys),
        "complete": complete,
        "successful_parsed_calls": successful_parsed_calls,
        "valid_outputs": valid_outputs,
        "has_evidence": successful_parsed_calls > 0 and valid_outputs > 0,
    }


def compute_report(run: dict[str, Any], validator: ResultValidationAdapter) -> dict[str, Any]:
    models = list((run.get("config") or {}).get("models") or [])
    specs = list(run.get("attributes") or [])
    spec_by_name = {str(spec["name"]): spec for spec in specs}
    calls = list(run.get("calls") or [])
    transcripts = list(run.get("transcripts") or [])
    baseline_by_analysis: dict[str, dict[str, Any]] = {}
    for transcript in transcripts:
        raw = (transcript.get("baseline") or {}).get("result_json")
        if raw:
            try:
                baseline_by_analysis[transcript["analysis_id"]] = parse_json_object(raw)
            except ValueError:
                pass

    leaderboard = []
    per_attribute: list[dict[str, Any]] = []
    violations = []
    values_by_item: dict[tuple[str, str], dict[str, Any]] = {}
    for model in models:
        model_calls = [call for call in calls if call.get("model") == model]
        request_successes = sum(bool(call.get("request_success")) for call in model_calls)
        parse_successes = sum(bool(call.get("parse_success")) for call in model_calls)
        validations = [item for call in model_calls for item in call.get("validations", [])]
        present = sum(bool(item.get("present")) for item in validations)
        valid = sum(bool(item.get("valid")) for item in validations)
        schema_ok = sum(
            bool(call.get("parse_success"))
            and not call.get("extra_keys")
            and bool(call.get("validations"))
            and all(item.get("present") for item in call.get("validations", []))
            for call in model_calls
        )
        baseline_matches = baseline_total = 0
        for call in model_calls:
            baseline = baseline_by_analysis.get(call["analysis_id"]) or {}
            for item in call.get("validations", []):
                name = item["attribute"]
                values_by_item.setdefault((call["analysis_id"], name), {})[model] = item
                if item.get("present") and name in baseline and name in spec_by_name:
                    baseline_total += 1
                    baseline_value = validator.canonicalize(spec_by_name[name], baseline[name])
                    baseline_matches += values_equal(item.get("canonical"), baseline_value)
                if not item.get("valid"):
                    violations.append(
                        {
                            "analysis_id": call["analysis_id"],
                            "source_id": call.get("source_id"),
                            "model": model,
                            "attribute": name,
                            "reason": item.get("reason"),
                            "raw": item.get("raw"),
                        }
                    )
        request_rate = ratio(request_successes, len(model_calls)) or 0.0
        parse_rate = ratio(parse_successes, len(model_calls)) or 0.0
        completeness = ratio(present, len(validations)) or 0.0
        validity = ratio(valid, len(validations)) or 0.0
        schema = ratio(schema_ok, len(model_calls)) or 0.0
        baseline_agreement = ratio(baseline_matches, baseline_total)
        components = {
            "request_success": request_rate,
            "parse_success": parse_rate,
            "completeness": completeness,
            "domain_validity": validity,
            "schema_compliance": schema,
            "baseline_agreement": baseline_agreement,
        }
        operational_score = sum(
            (components[key] if components[key] is not None else 0.0) * weight
            for key, weight in OPERATIONAL_SCORE_WEIGHTS.items()
        )
        latencies = [float(call["latency_seconds"]) for call in model_calls if call.get("latency_seconds") is not None]
        total_cost_values = [call.get("estimated_cost_usd") for call in model_calls]
        cost_available = all(value is not None for value in total_cost_values) and bool(total_cost_values)
        leaderboard.append(
            {
                "model": model,
                "operational_quality": round(operational_score, 6),
                "components": components,
                "requests": len(model_calls),
                "errors": sum(bool(call.get("error")) for call in model_calls),
                "retries": sum(int(call.get("retries") or 0) for call in model_calls),
                "avg_latency_seconds": round(sum(latencies) / len(latencies), 4) if latencies else None,
                "p95_latency_seconds": percentile(latencies, 0.95),
                "prompt_tokens": sum(int((call.get("usage") or {}).get("prompt_tokens") or 0) for call in model_calls),
                "completion_tokens": sum(
                    int((call.get("usage") or {}).get("completion_tokens") or 0) for call in model_calls
                ),
                "total_tokens": sum(int((call.get("usage") or {}).get("total_tokens") or 0) for call in model_calls),
                "estimated_cost_usd": round(sum(total_cost_values), 8) if cost_available else None,
                "baseline_compared": baseline_total,
                "reviewed_accuracy": None,
                "reviewed_items": 0,
                "ai_judged_accuracy": None,
                "ai_judged_items": 0,
            }
        )
        for spec in specs:
            name = str(spec["name"])
            items = [
                item
                for call in model_calls
                for item in call.get("validations", [])
                if item.get("attribute") == name
            ]
            per_attribute.append(
                {
                    "model": model,
                    "attribute": name,
                    "completeness": ratio(sum(bool(item.get("present")) for item in items), len(items)),
                    "domain_validity": ratio(sum(bool(item.get("valid")) for item in items), len(items)),
                    "count": len(items),
                }
            )

    disagreements = []
    pair_counts: dict[tuple[str, str], list[int]] = {}
    for (analysis_id, attribute), model_values in values_by_item.items():
        normalized = {
            model: (
                json.dumps(item.get("canonical"), sort_keys=True, default=json_default)
                if item.get("valid")
                else f"<{item.get('reason')}>"
            )
            for model, item in model_values.items()
        }
        if len(set(normalized.values())) > 1:
            source = next((t.get("source_id") for t in transcripts if t["analysis_id"] == analysis_id), analysis_id)
            disagreements.append(
                {
                    "analysis_id": analysis_id,
                    "source_id": source,
                    "attribute": attribute,
                    "outputs": {
                        model: {
                            "raw": item.get("raw"),
                            "canonical": item.get("canonical"),
                            "valid": item.get("valid"),
                            "reason": item.get("reason"),
                        }
                        for model, item in model_values.items()
                    },
                }
            )
        for left_index, left in enumerate(models):
            for right in models[left_index + 1:]:
                if left not in normalized or right not in normalized:
                    continue
                counts = pair_counts.setdefault((left, right), [0, 0])
                counts[1] += 1
                counts[0] += normalized[left] == normalized[right]
    pairwise = [
        {
            "model_a": pair[0],
            "model_b": pair[1],
            "agreement_rate": ratio(counts[0], counts[1]),
            "disagreement_rate": ratio(counts[1] - counts[0], counts[1]),
            "compared": counts[1],
        }
        for pair, counts in pair_counts.items()
    ]

    leaderboard_by_model = {item["model"]: item for item in leaderboard}
    judge_correct: dict[str, list[bool]] = {model: [] for model in models}
    for judgment in run.get("judgments") or []:
        mapping = judgment.get("mapping") or {}
        requested = set(str(attribute) for attribute in judgment.get("attributes_requested") or [])
        for attribute, result in (judgment.get("results") or {}).items():
            if str(attribute) not in requested:
                continue
            correctness = result.get("candidate_correctness") or {}
            for label, value in correctness.items():
                model = mapping.get(label)
                if model in judge_correct and isinstance(value, bool):
                    judge_correct[model].append(value)
    for model, values in judge_correct.items():
        leaderboard_by_model[model]["ai_judged_items"] = len(values)
        leaderboard_by_model[model]["ai_judged_accuracy"] = ratio(sum(values), len(values))

    reviewed_correct: dict[str, list[bool]] = {model: [] for model in models}
    for key, label in (run.get("manual_labels") or {}).items():
        analysis_id, attribute = key.split("|", 1)
        model_values = values_by_item.get((analysis_id, attribute), {})
        if label.get("decision") == "uncertain":
            continue
        accepted = set(label.get("accepted_models") or [])
        expected_set = "expected_value" in label
        expected = (
            validator.canonicalize(spec_by_name[attribute], label.get("expected_value"))
            if expected_set and attribute in spec_by_name
            else None
        )
        for model in models:
            item = model_values.get(model)
            if not item:
                reviewed_correct[model].append(False)
            elif accepted:
                reviewed_correct[model].append(model in accepted)
            elif expected_set:
                reviewed_correct[model].append(
                    bool(item.get("valid")) and values_equal(item.get("canonical"), expected)
                )
    for model, values in reviewed_correct.items():
        leaderboard_by_model[model]["reviewed_items"] = len(values)
        leaderboard_by_model[model]["reviewed_accuracy"] = ratio(sum(values), len(values))

    has_reviewed = bool(leaderboard) and all(item["reviewed_items"] for item in leaderboard)
    has_judged = bool(leaderboard) and all(item["ai_judged_items"] for item in leaderboard)
    if has_reviewed:
        basis = "Reviewed/human accuracy"
        leader_label = "Reviewed leader"
        primary_metric = lambda item: item["reviewed_accuracy"]
        sort_key = lambda item: (
            -(item["reviewed_accuracy"] if item["reviewed_accuracy"] is not None else -1),
            -(item["ai_judged_accuracy"] if item["ai_judged_accuracy"] is not None else -1),
            -item["operational_quality"],
        )
    elif has_judged:
        basis = "AI-judged accuracy (proxy)"
        leader_label = "AI-judged leader"
        primary_metric = lambda item: item["ai_judged_accuracy"]
        sort_key = lambda item: (
            -(item["ai_judged_accuracy"] if item["ai_judged_accuracy"] is not None else -1),
            -item["operational_quality"],
        )
    else:
        basis = "Operational quality"
        leader_label = "Provisional leader"
        primary_metric = lambda item: item["operational_quality"]
        sort_key = lambda item: (-item["operational_quality"],)
    leaderboard.sort(key=sort_key)
    top_models: list[str] = []
    top_value = primary_metric(leaderboard[0]) if leaderboard else None
    if top_value is not None:
        top_models = [
            item["model"]
            for item in leaderboard
            if primary_metric(item) is not None
            and math.isclose(float(primary_metric(item)), float(top_value), abs_tol=1e-9)
        ]
    rank = 0
    previous_value: float | None = None
    for index, item in enumerate(leaderboard, 1):
        value = primary_metric(item)
        if previous_value is None or value is None or not math.isclose(
            float(value), previous_value, abs_tol=1e-9
        ):
            rank = index
        item["rank"] = rank
        previous_value = float(value) if value is not None else None

    coverage = candidate_coverage(run, models, specs, transcripts, calls)
    if coverage["status"] != "completed" or not coverage["complete"]:
        recommendation = {
            "model": None,
            "label": "Incomplete run",
            "basis": "Candidate execution coverage",
            "statement": (
                f"No leader is declared: {coverage['recorded_candidate_calls']} of "
                f"{coverage['expected_candidate_calls']} candidate calls were recorded "
                f"and the run status is {coverage['status']}."
            ),
        }
    elif not coverage["has_evidence"] or not leaderboard:
        recommendation = {
            "model": None,
            "label": "Insufficient evidence",
            "basis": "Evidence quality",
            "statement": "No leader is declared because no valid parsed candidate output is available.",
        }
    elif len(top_models) != 1:
        recommendation = {
            "model": None,
            "label": "Tie",
            "basis": basis,
            "statement": f"No unique leader: {', '.join(top_models)} tie on {basis.lower()}.",
        }
    else:
        recommendation = {
            "model": top_models[0],
            "label": leader_label,
            "basis": basis,
            "statement": f"{top_models[0]} leads on {basis.lower()}.",
        }
    judge_scope = {
        "requested_items": sum(
            len(judgment.get("attributes_requested") or [])
            for judgment in run.get("judgments") or []
        ),
        "judged_items": sum(
            len(judgment.get("results") or {}) for judgment in run.get("judgments") or []
        ),
        "missing_attributes": sum(
            len(judgment.get("missing_attributes") or []) for judgment in run.get("judgments") or []
        ),
        "extra_attributes": sum(
            int(judgment.get("extra_attribute_count") or 0) for judgment in run.get("judgments") or []
        ),
    }
    return {
        "recommendation": recommendation,
        "leaderboard": leaderboard,
        "per_attribute": per_attribute,
        "violations": violations,
        "disagreements": disagreements,
        "pairwise": pairwise,
        "weights": OPERATIONAL_SCORE_WEIGHTS,
        "coverage": coverage,
        "judge_scope": judge_scope,
        "methodology": {
            "operational_quality": (
                "Weighted request, parse, completeness, domain, schema, and historical-baseline "
                "agreement metrics. It is not accuracy."
            ),
            "baseline": "Historical production output agreement is a comparison baseline, not truth.",
            "judge": (
                "AI-judged accuracy is a blinded model proxy calculated only on the exact "
                "configured judge-attribute scope; it is not human ground truth."
            ),
            "reviewed": "Reviewed accuracy uses locally persisted manual labels and is the strongest ranking signal.",
        },
    }


def percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(quantile * len(ordered)) - 1))
    return round(ordered[index], 4)


def spreadsheet_safe(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    return "'" + value if value.lstrip().startswith(("=", "+", "-", "@")) else value


CSV_EXPORT_FIELDS = [
    "record_type",
    "run_id",
    "run_status",
    "run_created_at",
    "run_updated_at",
    "job_name",
    "configured_judge_model",
    "judge_scope",
    "judge_scope_attributes",
    "analysis_id",
    "source_id",
    "model",
    "batch_index",
    "attribute",
    "attribute_instruction",
    "attribute_description",
    "result_type",
    "allowed_options",
    "blank_allowed",
    "raw_value",
    "canonical_value",
    "present",
    "valid",
    "reason",
    "request_success",
    "parse_success",
    "latency_seconds",
    "prompt_tokens",
    "completion_tokens",
    "total_tokens",
    "returned_model",
    "anonymous_judge_label",
    "judge_candidate_correctness",
    "judge_winner_model",
    "judge_winner_label",
    "judge_outcome",
    "judge_confidence",
    "judge_rationale",
    "judge_evidence",
    "judge_latency_seconds",
    "judge_prompt_tokens",
    "judge_completion_tokens",
    "judge_total_tokens",
    "judge_retries",
    "judge_error",
    "judge_missing_attribute",
    "judge_missing_attributes",
    "judge_extra_attribute_count",
]


def csv_json_value(value: Any) -> str:
    return json.dumps(value, default=json_default, ensure_ascii=True, allow_nan=False)


def judgment_requested_attributes(judgment: dict[str, Any]) -> list[str]:
    requested = judgment.get("attributes_requested")
    if isinstance(requested, list):
        return list(dict.fromkeys(str(attribute) for attribute in requested))
    results = judgment.get("results") if isinstance(judgment.get("results"), dict) else {}
    missing = (
        judgment.get("missing_attributes")
        if isinstance(judgment.get("missing_attributes"), list)
        else []
    )
    return list(dict.fromkeys([*(str(attribute) for attribute in results), *(str(attribute) for attribute in missing)]))


def judgment_missing_attributes(judgment: dict[str, Any]) -> list[str]:
    results = judgment.get("results") if isinstance(judgment.get("results"), dict) else {}
    stored = (
        [str(attribute) for attribute in judgment.get("missing_attributes")]
        if isinstance(judgment.get("missing_attributes"), list)
        else []
    )
    inferred = [
        attribute
        for attribute in judgment_requested_attributes(judgment)
        if attribute not in results
    ]
    return list(dict.fromkeys([*stored, *inferred]))


def judge_outcome(
    result: dict[str, Any] | None,
    mapping: dict[str, str],
    error: Any,
    missing: bool,
) -> tuple[str, str | None, str | None]:
    if error:
        return "error", None, None
    if missing:
        return "missing", None, None
    if not isinstance(result, dict):
        return "unjudged", None, None
    winner_label = str(result.get("winner")) if result.get("winner") is not None else None
    winner_model = mapping.get(winner_label) if winner_label else None
    if winner_model:
        return "winner", winner_model, winner_label
    if winner_label and winner_label.lower() == "tie":
        return "tie", None, winner_label
    if winner_label and winner_label.lower() == "uncertain":
        return "uncertain", None, winner_label
    return "uncertain", None, winner_label


def csv_export_rows(run: dict[str, Any]) -> list[dict[str, Any]]:
    config = run.get("config") or {}
    judge_config = config.get("judge") or {}
    specs = {
        str(spec.get("name")): spec
        for spec in run.get("attributes") or []
        if spec.get("name") is not None
    }
    common = {
        "run_id": run.get("id"),
        "run_status": run.get("status"),
        "run_created_at": run.get("created_at"),
        "run_updated_at": run.get("updated_at"),
        "job_name": config.get("job_name"),
        "configured_judge_model": judge_config.get("model"),
        "judge_scope": judge_config.get("scope"),
        "judge_scope_attributes": csv_json_value(
            configured_judge_attribute_names(config)
            if judge_config.get("enabled")
            else []
        ),
    }
    calls = list(run.get("calls") or [])
    rows: list[dict[str, Any]] = []
    for call in calls:
        usage = call.get("usage") or {}
        for validation in call.get("validations") or []:
            rows.append(
                {
                    **common,
                    "record_type": "candidate",
                    "analysis_id": call.get("analysis_id"),
                    "source_id": call.get("source_id"),
                    "model": call.get("model"),
                    "batch_index": call.get("batch_index"),
                    "attribute": validation.get("attribute"),
                    "raw_value": csv_json_value(validation.get("raw")),
                    "canonical_value": csv_json_value(validation.get("canonical")),
                    "present": validation.get("present"),
                    "valid": validation.get("valid"),
                    "reason": validation.get("reason"),
                    "request_success": call.get("request_success"),
                    "parse_success": call.get("parse_success"),
                    "latency_seconds": call.get("latency_seconds"),
                    "prompt_tokens": usage.get("prompt_tokens"),
                    "completion_tokens": usage.get("completion_tokens"),
                    "total_tokens": usage.get("total_tokens"),
                    "returned_model": call.get("returned_model"),
                }
            )

    calls_by_key = {
        (
            str(call.get("analysis_id")),
            int(call.get("batch_index") or 0),
            str(call.get("model")),
        ): call
        for call in calls
    }
    source_by_analysis = {
        str(transcript.get("analysis_id")): transcript.get("source_id")
        for transcript in run.get("transcripts") or []
    }
    configured_models = [str(model) for model in config.get("models") or []]
    for judgment in run.get("judgments") or []:
        analysis_id = str(judgment.get("analysis_id") or "")
        batch_index = int(judgment.get("batch_index") or 0)
        mapping = (
            {str(label): str(model) for label, model in judgment.get("mapping").items()}
            if isinstance(judgment.get("mapping"), dict)
            else {}
        )
        results = judgment.get("results") if isinstance(judgment.get("results"), dict) else {}
        requested = judgment_requested_attributes(judgment)
        missing_attributes = judgment_missing_attributes(judgment)
        attributes = requested or ([""] if judgment.get("error") else [])
        models = list(dict.fromkeys([*configured_models, *mapping.values()]))
        judge_usage = judgment.get("usage") or {}
        for attribute in attributes:
            result = results.get(attribute) if isinstance(results.get(attribute), dict) else None
            missing = attribute in missing_attributes
            outcome, winner_model, winner_label = judge_outcome(
                result, mapping, judgment.get("error"), missing
            )
            spec = specs.get(attribute) or {}
            for model in models or [""]:
                call = calls_by_key.get((analysis_id, batch_index, model))
                validations = call.get("validations") if call else []
                validation = next(
                    (
                        item
                        for item in validations or []
                        if str(item.get("attribute")) == attribute
                    ),
                    None,
                )
                values = call.get("values") if call and isinstance(call.get("values"), dict) else {}
                raw_value = (
                    validation.get("raw")
                    if validation and "raw" in validation
                    else values.get(attribute, MISSING)
                )
                canonical_value = (
                    validation.get("canonical")
                    if validation and "canonical" in validation
                    else MISSING
                )
                anonymous_label = next(
                    (label for label, mapped_model in mapping.items() if mapped_model == model),
                    None,
                )
                correctness = MISSING
                correctness_map = (
                    result.get("candidate_correctness")
                    if result and isinstance(result.get("candidate_correctness"), dict)
                    else {}
                )
                if anonymous_label and anonymous_label in correctness_map:
                    correctness = correctness_map.get(anonymous_label)
                call_usage = call.get("usage") if call else {}
                rows.append(
                    {
                        **common,
                        "record_type": "judgment",
                        "analysis_id": analysis_id or None,
                        "source_id": source_by_analysis.get(analysis_id)
                        or (call.get("source_id") if call else None),
                        "model": model or None,
                        "batch_index": batch_index,
                        "attribute": attribute or None,
                        "attribute_instruction": spec.get("prompt")
                        or spec.get("description"),
                        "attribute_description": spec.get("description"),
                        "result_type": spec.get("result_type"),
                        "allowed_options": csv_json_value(spec.get("options") or []),
                        "blank_allowed": prompt_allows_blank(spec) if spec else None,
                        "raw_value": (
                            "" if raw_value is MISSING else csv_json_value(raw_value)
                        ),
                        "canonical_value": (
                            ""
                            if canonical_value is MISSING
                            else csv_json_value(canonical_value)
                        ),
                        "present": validation.get("present") if validation else None,
                        "valid": validation.get("valid") if validation else None,
                        "reason": validation.get("reason") if validation else None,
                        "request_success": call.get("request_success") if call else None,
                        "parse_success": call.get("parse_success") if call else None,
                        "latency_seconds": call.get("latency_seconds") if call else None,
                        "prompt_tokens": (call_usage or {}).get("prompt_tokens"),
                        "completion_tokens": (call_usage or {}).get("completion_tokens"),
                        "total_tokens": (call_usage or {}).get("total_tokens"),
                        "returned_model": call.get("returned_model") if call else None,
                        "anonymous_judge_label": anonymous_label,
                        "judge_candidate_correctness": (
                            ""
                            if correctness is MISSING
                            else csv_json_value(correctness)
                        ),
                        "judge_winner_model": winner_model,
                        "judge_winner_label": winner_label,
                        "judge_outcome": outcome,
                        "judge_confidence": result.get("confidence") if result else None,
                        "judge_rationale": result.get("rationale") if result else None,
                        "judge_evidence": result.get("evidence") if result else None,
                        "judge_latency_seconds": judgment.get("latency_seconds"),
                        "judge_prompt_tokens": judge_usage.get("prompt_tokens"),
                        "judge_completion_tokens": judge_usage.get("completion_tokens"),
                        "judge_total_tokens": judge_usage.get("total_tokens"),
                        "judge_retries": judgment.get("retries"),
                        "judge_error": judgment.get("error"),
                        "judge_missing_attribute": missing,
                        "judge_missing_attributes": csv_json_value(missing_attributes),
                        "judge_extra_attribute_count": judgment.get(
                            "extra_attribute_count"
                        ),
                    }
                )
    return rows


def sanitized_run_export(run: dict[str, Any]) -> dict[str, Any]:
    config = run.get("config") or {}
    report = run.get("report") or {}
    safe_report = {
        key: report.get(key)
        for key in ("recommendation", "leaderboard", "per_attribute", "pairwise", "weights", "methodology")
    }
    safe_report["violations"] = [
        {key: item.get(key) for key in ("analysis_id", "source_id", "model", "attribute", "reason")}
        for item in report.get("violations") or []
    ]
    safe_report["disagreements"] = [
        {
            "analysis_id": item.get("analysis_id"),
            "source_id": item.get("source_id"),
            "attribute": item.get("attribute"),
            "outputs": {
                model: {
                    "valid": output.get("valid"),
                    "reason": output.get("reason"),
                }
                for model, output in (item.get("outputs") or {}).items()
            },
        }
        for item in report.get("disagreements") or []
    ]
    return {
        "schema_version": 1,
        "sanitized": True,
        "exported_at": utc_now(),
        "id": run.get("id"),
        "status": run.get("status"),
        "created_at": run.get("created_at"),
        "updated_at": run.get("updated_at"),
        "config": {
            key: config.get(key)
            for key in (
                "mode", "job_id", "job_name", "start_date", "end_date", "sample_size", "seed",
                "models", "attributes", "judge", "concurrency", "transcript_char_limit",
                "batch_count", "estimated_candidate_calls", "max_judge_calls",
                "max_judge_items", "max_judge_output_tokens",
            )
        },
        "attributes": [
            {
                key: spec.get(key)
                for key in ("attribute_id", "name", "result_type", "options", "allow_blank", "group", "rownum")
            }
            for spec in run.get("attributes") or []
        ],
        "transcripts": [
            {
                "analysis_id": item.get("analysis_id"),
                "source_id": item.get("source_id"),
                "interaction_date": item.get("interaction_date"),
                "truncated": bool(item.get("truncated")),
                "original_characters": item.get("original_characters"),
                "baseline": {
                    key: (item.get("baseline") or {}).get(key)
                    for key in ("model", "prompt_version", "prompt_tokens", "completion_tokens")
                },
            }
            for item in run.get("transcripts") or []
        ],
        "calls": [
            {
                "analysis_id": call.get("analysis_id"),
                "source_id": call.get("source_id"),
                "model": call.get("model"),
                "batch_index": call.get("batch_index"),
                "request_success": call.get("request_success"),
                "parse_success": call.get("parse_success"),
                "extra_key_count": len(call.get("extra_keys") or []),
                "validations": [
                    {
                        key: validation.get(key)
                        for key in ("attribute", "present", "valid", "reason")
                    }
                    for validation in call.get("validations") or []
                ],
                "latency_seconds": call.get("latency_seconds"),
                "retries": call.get("retries"),
                "usage": call.get("usage") or {},
                "returned_model": call.get("returned_model"),
                "estimated_cost_usd": call.get("estimated_cost_usd"),
                "error": call.get("error"),
            }
            for call in run.get("calls") or []
        ],
        "judgments": [
            {
                "analysis_id": judgment.get("analysis_id"),
                "batch_index": judgment.get("batch_index"),
                "attributes_requested": judgment.get("attributes_requested") or [],
                "missing_attributes": judgment.get("missing_attributes") or [],
                "extra_attribute_count": int(judgment.get("extra_attribute_count") or 0),
                "mapping": judgment.get("mapping") or {},
                "results": {
                    attribute: {
                        key: result.get(key)
                        for key in ("candidate_correctness", "winner", "confidence")
                    }
                    for attribute, result in (judgment.get("results") or {}).items()
                    if attribute in set(judgment.get("attributes_requested") or [])
                },
                "latency_seconds": judgment.get("latency_seconds"),
                "usage": judgment.get("usage") or {},
                "estimated_cost_usd": judgment.get("estimated_cost_usd"),
                "retries": judgment.get("retries"),
                "error": judgment.get("error"),
            }
            for judgment in run.get("judgments") or []
        ],
        "manual_labels": {
            key: {
                field: label.get(field)
                for field in ("analysis_id", "attribute", "decision", "accepted_models", "updated_at")
                if field in label
            }
            for key, label in (run.get("manual_labels") or {}).items()
        },
        "progress": run.get("progress") or {},
        "report": safe_report,
        "omitted_sensitive_fields": [
            "transcript text", "system and attribute prompts", "baseline result JSON",
            "candidate raw/canonical outputs", "judge rationale/evidence", "manual expected values",
        ],
    }


class ModelLabService:
    def __init__(
        self,
        root: Path,
        dashboard_module: Any | None = None,
        repository_factory: Callable[[str], Any] | None = None,
        client_factory: Callable[[str], Any] | None = None,
    ):
        self.root = Path(root)
        self.validator = ResultValidationAdapter(dashboard_module)
        self.store = RunStore(
            Path(os.getenv("OPTIC_MODEL_LAB_RUNS_DIR", self.root / "model_lab_runs"))
        )
        connection_factory = getattr(dashboard_module, "create_snowflake_connection", None)

        def default_repository(mode: str) -> Any:
            if mode == "demo":
                return DemoRepository()
            if connection_factory is None:
                raise IntegrationError("Snowflake support is unavailable.")
            return SnowflakeRepository(connection_factory)

        def default_client(mode: str) -> Any:
            return DemoOdinClient(self.validator) if mode == "demo" else OdinClient()

        self.repository_factory = repository_factory or default_repository
        self.client_factory = client_factory or default_client
        self.manager = RunManager(
            self.store, self.repository_factory, self.client_factory, self.validator
        )
        self.sample_cap = MAX_SAMPLE_SIZE
        self.model_cap = env_int(
            "OPTIC_MODEL_LAB_MODEL_CAP", len(DEFAULT_MODELS), 2, 24
        )
        self.attribute_cap = env_int("OPTIC_MODEL_LAB_ATTRIBUTE_CAP", 319, 1, 1000)
        self.concurrency = env_int("OPTIC_MODEL_LAB_CONCURRENCY", 4, 1, 12)
        self.transcript_limit = env_int(
            "OPTIC_MODEL_LAB_TRANSCRIPT_CHAR_LIMIT", 60000, 4000, 200000
        )

    def bootstrap(self, mode: str, job_id: str | None = None) -> dict[str, Any]:
        mode = validate_mode(mode)
        repository = self.repository_factory(mode)
        jobs = repository.list_jobs()
        selected = None
        if job_id:
            selected = repository.get_job(job_id)
        elif jobs:
            selected = jobs[0]
        attributes = repository.list_attributes(selected) if selected else []
        models = configured_models()
        return {
            "mode": mode,
            "jobs": [
                {
                    key: value
                    for key, value in job.items()
                    if key not in {"system_prompt", "attribute_order", "attribute_groups"}
                }
                for job in jobs
            ],
            "selected_job_id": selected["id"] if selected else None,
            "attributes": attributes,
            "models": models,
            "model_catalog": model_catalog(models),
            "defaults": {
                "start_date": f"{date.today().year}-01-01",
                "end_date": date.today().isoformat(),
                "sample_size": min(10, self.sample_cap),
                "seed": 42,
                "sample_cap": self.sample_cap,
                "model_cap": self.model_cap,
                "attribute_cap": self.attribute_cap,
                "max_attributes_per_request": MAX_ATTRIBUTES_PER_BATCH,
                "concurrency": self.concurrency,
                "transcript_char_limit": self.transcript_limit,
                "judge_model": "gpt-6.1-sol",
                "judge_scope": "disagreements",
            },
            "pricing_configured_models": sorted(pricing_config()),
        }

    def validate_config(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ModelLabError("Request body must be a JSON object.")
        mode = validate_mode(payload.get("mode"))
        repository = self.repository_factory(mode)
        job_id = str(payload.get("job_id") or "").strip()
        if not job_id:
            raise ModelLabError("Select an active job prompt.")
        job = repository.get_job(job_id)
        start_date = validate_date(payload.get("start_date"), "start_date")
        end_date = validate_date(payload.get("end_date"), "end_date")
        if start_date > end_date:
            raise ModelLabError("Start date must be on or before end date.")
        sample_size = validate_int(payload.get("sample_size"), "sample_size", 1, self.sample_cap)
        seed = validate_int(payload.get("seed", 42), "seed", -2_147_483_648, 2_147_483_647)
        models = payload.get("models")
        if not isinstance(models, list):
            raise ModelLabError("Models must be an array.")
        models = list(dict.fromkeys(str(model).strip().lower() for model in models))
        if not 2 <= len(models) <= self.model_cap:
            raise ModelLabError(f"Select between 2 and {self.model_cap} models.")
        if any(not MODEL_SLUG_RE.fullmatch(model) for model in models):
            raise ModelLabError("One or more model deployment slugs are invalid.")
        available = repository.list_attributes(job)
        available_by_name = {str(spec["name"]): spec for spec in available}
        selected = payload.get("attributes")
        if not isinstance(selected, list):
            raise ModelLabError("Attributes must be an array.")
        selected_names = list(dict.fromkeys(str(name).strip() for name in selected if str(name).strip()))
        if not 1 <= len(selected_names) <= self.attribute_cap:
            raise ModelLabError(f"Select between 1 and {self.attribute_cap} attributes.")
        unknown = [name for name in selected_names if name not in available_by_name]
        if unknown:
            raise ModelLabError("One or more selected attributes are not active for this job.")
        selected_set = set(selected_names)
        attribute_specs = [spec for spec in available if spec["name"] in selected_set]
        judge_raw = payload.get("judge") or {}
        if not isinstance(judge_raw, dict):
            raise ModelLabError("Judge configuration must be an object.")
        judge_enabled = bool(judge_raw.get("enabled"))
        judge_scope = str(judge_raw.get("scope") or "disagreements")
        if judge_scope not in {"disagreements", "all"}:
            raise ModelLabError("Judge scope must be disagreements or all.")
        judge_model = str(judge_raw.get("model") or "gpt-6.1-sol").strip().lower()
        if not MODEL_SLUG_RE.fullmatch(judge_model):
            raise ModelLabError("Judge deployment slug is invalid.")
        include_free_text = bool(judge_raw.get("include_free_text"))
        eligible_judge_names = {
            str(spec["name"])
            for spec in judge_eligible_attributes(attribute_specs, include_free_text)
        }
        raw_judge_attributes = judge_raw.get("attributes", MISSING)
        if raw_judge_attributes is MISSING:
            judge_attributes = default_judge_attribute_names(attribute_specs)
        elif not isinstance(raw_judge_attributes, list):
            raise ModelLabError("Judge attributes must be an array.")
        else:
            judge_attributes = list(
                dict.fromkeys(
                    str(name).strip()
                    for name in raw_judge_attributes
                    if str(name).strip()
                )
            )
        outside_candidate_scope = [
            name for name in judge_attributes if name not in selected_set
        ]
        if outside_candidate_scope:
            raise ModelLabError("Judge attributes must be selected candidate-test attributes.")
        ineligible_judge_attributes = [
            name for name in judge_attributes if name not in eligible_judge_names
        ]
        if ineligible_judge_attributes:
            raise ModelLabError(
                "Enable free-text judging before selecting STRING judge attributes."
            )
        if judge_enabled and not judge_attributes:
            raise ModelLabError(
                "Select at least one judge attribute when the AI judge is enabled."
            )
        batch_count = len(chunk_attributes(attribute_specs))
        candidate_calls = sample_size * len(models) * batch_count
        judge_attribute_set = set(judge_attributes)
        judge_batch_count = sum(
            any(str(spec["name"]) in judge_attribute_set for spec in batch)
            for batch in chunk_attributes(attribute_specs)
        )
        max_judge_calls = sample_size * judge_batch_count
        max_judge_items = sample_size * len(judge_attributes)
        max_judge_output_tokens = (
            max_judge_calls
            * env_int("OPTIC_MODEL_LAB_MAX_COMPLETION_TOKENS", 4000, 256, 16000)
        )
        return {
            "mode": mode,
            "job_id": job["id"],
            "job_name": job["job_name"],
            "job_description": job.get("description"),
            "system_prompt": job["system_prompt"],
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "sample_size": sample_size,
            "seed": seed,
            "models": models,
            "attributes": [spec["name"] for spec in attribute_specs],
            "attribute_specs": attribute_specs,
            "batch_count": batch_count,
            "estimated_candidate_calls": candidate_calls,
            "max_judge_calls": max_judge_calls,
            "max_judge_items": max_judge_items,
            "max_judge_output_tokens": max_judge_output_tokens,
            "judge": {
                "enabled": judge_enabled,
                "scope": judge_scope,
                "model": judge_model,
                "include_free_text": include_free_text,
                "attributes": judge_attributes,
                "label": "AI-judged accuracy (proxy)",
            },
            "concurrency": self.concurrency,
            "transcript_char_limit": self.transcript_limit,
        }

    def preflight(self, payload: dict[str, Any]) -> dict[str, Any]:
        config = self.validate_config(payload)
        repository = self.repository_factory(config["mode"])
        available = repository.count_samples(
            config["job_name"], config["start_date"], config["end_date"]
        )
        preview_count = min(config["sample_size"], available, 5)
        preview = (
            repository.sample(
                config["job_name"],
                config["start_date"],
                config["end_date"],
                preview_count,
                config["seed"],
            )
            if preview_count
            else []
        )
        sample_will_use = min(config["sample_size"], available)
        estimated_judge_input_tokens = estimate_judge_input_tokens(
            config, preview, sample_will_use
        )
        warnings = []
        if config["estimated_candidate_calls"] >= 10_000:
            warnings.append(
                f"Large run: {config['estimated_candidate_calls']:,} candidate calls "
                f"for up to {sample_will_use:,} sampled transcripts. Execution starts "
                "only after Test is clicked and remains concurrency-bounded."
            )
        if config["judge"]["enabled"] and config["judge"]["scope"] == "all":
            warnings.append("All-output judging can materially increase token use.")
        return {
            "config": {
                key: value
                for key, value in config.items()
                if key not in {"system_prompt", "attribute_specs"}
            },
            "available_transcripts": available,
            "sample_will_use": sample_will_use,
            "candidate_calls": config["estimated_candidate_calls"],
            "max_judge_calls": config["max_judge_calls"] if config["judge"]["enabled"] else 0,
            "max_judge_items": config["max_judge_items"] if config["judge"]["enabled"] else 0,
            "estimated_judge_input_tokens": estimated_judge_input_tokens,
            "max_judge_output_tokens": (
                config["max_judge_output_tokens"] if config["judge"]["enabled"] else 0
            ),
            "estimated_max_judge_tokens": (
                estimated_judge_input_tokens + config["max_judge_output_tokens"]
                if config["judge"]["enabled"]
                else 0
            ),
            "batch_count": config["batch_count"],
            "preview": [
                {
                    "analysis_id": row["analysis_id"],
                    "source_id": row["source_id"],
                    "interaction_date": row.get("interaction_date"),
                    "characters": len(row.get("transcript") or ""),
                    "will_truncate": len(row.get("transcript") or "") > self.transcript_limit,
                    "baseline_available": bool((row.get("baseline") or {}).get("result_json")),
                }
                for row in preview
            ],
            "warning": " ".join(warnings) or None,
        }

    def add_label(self, run_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        analysis_id = str(payload.get("analysis_id") or "").strip()
        attribute = str(payload.get("attribute") or "").strip()

        def apply_label(run: dict[str, Any]) -> dict[str, Any]:
            if run.get("status") not in {"completed", "cancelled"}:
                raise ModelLabError("Manual labels are available after candidate execution finishes.", 409)
            valid_analysis = {item["analysis_id"] for item in run.get("transcripts") or []}
            valid_attributes = {item["name"] for item in run.get("attributes") or []}
            if analysis_id not in valid_analysis or attribute not in valid_attributes:
                raise ModelLabError("The manual label target is not part of this run.")
            key = f"{analysis_id}|{attribute}"
            if payload.get("delete"):
                run.setdefault("manual_labels", {}).pop(key, None)
            else:
                decision = str(payload.get("decision") or "expected")
                if decision not in {"expected", "accepted", "tie", "uncertain"}:
                    raise ModelLabError("Invalid manual-label decision.")
                label: dict[str, Any] = {
                    "analysis_id": analysis_id,
                    "attribute": attribute,
                    "decision": decision,
                    "updated_at": utc_now(),
                }
                if decision == "expected":
                    if "expected_value" not in payload:
                        raise ModelLabError("An expected value is required.")
                    label["expected_value"] = payload.get("expected_value")
                elif decision in {"accepted", "tie"}:
                    accepted = payload.get("accepted_models") or []
                    allowed = set(run["config"]["models"])
                    if (
                        not isinstance(accepted, list)
                        or not accepted
                        or any(model not in allowed for model in accepted)
                    ):
                        raise ModelLabError("Select at least one candidate model from this run.")
                    label["accepted_models"] = list(dict.fromkeys(accepted))
                run.setdefault("manual_labels", {})[key] = label
            run["report"] = compute_report(run, self.validator)
            return run

        run = self.store.update(run_id, apply_label)
        return {"run": run_summary(run), "report": run["report"], "manual_labels": run["manual_labels"]}

    def export(self, run_id: str, export_format: str) -> ApiResponse:
        run = self.store.load(run_id)
        if export_format == "json":
            body = json.dumps(
                sanitized_run_export(run), default=json_default, ensure_ascii=True, indent=2
            ).encode("utf-8")
            return ApiResponse(
                200, body, "application/json; charset=utf-8", f"optic-model-lab-{run_id}.json"
            )
        if export_format != "csv":
            raise ModelLabError("Export format must be json or csv.")
        output = io.StringIO(newline="")
        writer = csv.DictWriter(output, fieldnames=CSV_EXPORT_FIELDS)
        writer.writeheader()
        for row in csv_export_rows(run):
            writer.writerow(
                {
                    key: spreadsheet_safe(row.get(key))
                    for key in CSV_EXPORT_FIELDS
                }
            )
        return ApiResponse(
            200, output.getvalue().encode("utf-8"), "text/csv; charset=utf-8",
            f"optic-model-lab-{run_id}.csv",
        )

    def handle_get(self, path: str, query: dict[str, list[str]]) -> ApiResponse:
        try:
            if path in {"/api/model-lab/bootstrap", "/api/model-lab/config"}:
                mode = first_query(query, "mode", "real")
                job_id = first_query(query, "job", "") or None
                return ApiResponse.json(self.bootstrap(mode, job_id))
            if path in {"/api/model-lab/runs", "/api/model-lab/history"}:
                return ApiResponse.json({"runs": self.store.list()})
            match = re.fullmatch(r"/api/model-lab/runs/([0-9a-f]{32})", path)
            if match:
                run = self.store.load(match.group(1))
                if first_query(query, "view", "") == "status":
                    summary = run_summary(run)
                    summary.update(
                        {
                            "config": {
                                "job_name": (run.get("config") or {}).get("job_name"),
                                "models": (run.get("config") or {}).get("models") or [],
                                "judge": (run.get("config") or {}).get("judge") or {},
                            },
                            "progress": run.get("progress") or {},
                        }
                    )
                    return ApiResponse.json(summary)
                return ApiResponse.json(run)
            match = re.fullmatch(r"/api/model-lab/runs/([0-9a-f]{32})/export", path)
            if match:
                return self.export(match.group(1), first_query(query, "format", "json"))
            raise ModelLabError("Model Lab endpoint not found.", 404)
        except ModelLabError as exc:
            return ApiResponse.json({"error": str(exc)}, exc.status)
        except IntegrationError as exc:
            return ApiResponse.json({"error": str(exc)}, 503)
        except Exception:
            return ApiResponse.json({"error": "Model Lab request failed."}, 500)

    def handle_post(self, path: str, payload: dict[str, Any]) -> ApiResponse:
        try:
            if path == "/api/model-lab/preflight":
                return ApiResponse.json(self.preflight(payload))
            if path in {"/api/model-lab/runs", "/api/model-lab/start"}:
                config = self.validate_config(payload)
                run, duplicate = self.manager.start(config)
                return ApiResponse.json(
                    {"run": run_summary(run), "duplicate": duplicate}, 200 if duplicate else 202
                )
            match = re.fullmatch(r"/api/model-lab/runs/([0-9a-f]{32})/cancel", path)
            if match:
                return ApiResponse.json({"run": run_summary(self.manager.cancel(match.group(1)))})
            match = re.fullmatch(r"/api/model-lab/runs/([0-9a-f]{32})/resume", path)
            if match:
                return ApiResponse.json({"run": run_summary(self.manager.resume(match.group(1)))}, 202)
            match = re.fullmatch(r"/api/model-lab/runs/([0-9a-f]{32})/labels", path)
            if match:
                return ApiResponse.json(self.add_label(match.group(1), payload))
            raise ModelLabError("Model Lab endpoint not found.", 404)
        except ModelLabError as exc:
            return ApiResponse.json({"error": str(exc)}, exc.status)
        except IntegrationError as exc:
            return ApiResponse.json({"error": str(exc)}, 503)
        except Exception:
            return ApiResponse.json({"error": "Model Lab request failed."}, 500)


def validate_mode(value: Any) -> str:
    mode = str(value or "real").strip().lower()
    if mode not in {"real", "demo"}:
        raise ModelLabError("Mode must be real or demo.")
    return mode


def validate_date(value: Any, name: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise ModelLabError(f"{name} must use YYYY-MM-DD.") from exc


def validate_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not (
        isinstance(value, int)
        or (isinstance(value, str) and re.fullmatch(r"[+-]?\d+", value.strip()))
    ):
        raise ModelLabError(f"{name} must be an integer.")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ModelLabError(f"{name} must be an integer.") from exc
    if parsed < minimum or parsed > maximum:
        raise ModelLabError(f"{name} must be between {minimum} and {maximum}.")
    return parsed


def first_query(query: dict[str, list[str]], name: str, default: str) -> str:
    values = query.get(name) or []
    return values[0] if values else default
