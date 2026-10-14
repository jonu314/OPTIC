from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass, field

NUMERIC_TYPES = {"NUMBER", "NUMERIC", "INT", "INTEGER", "FLOAT", "DECIMAL"}


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False,
                      default=lambda x: x.isoformat() if hasattr(x, "isoformat") else str(x))


def digest(value):
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def options(value):
    if isinstance(value, dict):
        return options(list(value.values())) if all(isinstance(x, (str, int, float)) for x in value.values()) else ()
    if isinstance(value, list):
        return tuple(str(x).strip() for x in value if str(x).strip())
    text = str(value or "").strip()
    if not text:
        return ()
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        parsed = None
    if isinstance(parsed, (list, dict)):
        return options(parsed)
    return tuple(x.strip(" \t\r\n'\"") for x in re.split(r"[,;|\n]", text)
                 if x.strip(" \t\r\n'\"") and x.strip().upper() not in {"<BLANK>", '""', "''"})


@dataclass(frozen=True)
class Contract:
    attribute_id: str
    name: str
    job: str
    result_type: str
    options: tuple[str, ...] = ()
    prompt: str = ""
    system_prompt: str = ""
    allow_empty: bool = False
    channel: str = "voice"
    contract_status: str = "resolved"
    historical_scope: str = "current-contract retrospective"
    provenance: dict = field(default_factory=dict)
    value_mode: str = ""

    @property
    def contract_hash(self):
        return digest({k: v for k, v in asdict(self).items() if k != "provenance"})

    @property
    def key(self):
        return f"{self.attribute_id}:{self.job}:{self.channel}:{self.contract_hash}"

    @property
    def finite(self):
        return self.result_type in {"BOOL", "BOOLEMPTY"} or (
            self.result_type in {"STRINGPICKONE", "LIST"} and bool(self.options))

    @property
    def labels(self):
        return ("true", "false") if self.result_type in {"BOOL", "BOOLEMPTY"} else self.options

    def as_dict(self):
        result = asdict(self)
        result.update(key=self.key, contract_hash=self.contract_hash, monitoring_capability={
            "completeness_validity": "enabled",
            "output_shift": "enabled" if self.finite else "deferred",
            "text_presence": "enabled" if self.value_mode == "TEXT_PRESENCE_3_WORDS" else "not applicable",
            "numeric_distribution": "deferred" if self.result_type in NUMERIC_TYPES
                                    else "not applicable",
            "text_semantics": "deferred" if not self.finite else "not applicable"},
            audit_capability="rubric approval required" if self.contract_status != "resolved"
                             else ("defined contract" if self.prompt else "definition unavailable"))
        return result

    @classmethod
    def from_dict(cls, row):
        data = {k: v for k, v in row.items() if k in cls.__dataclass_fields__}
        data["options"] = tuple(data.get("options") or ())
        return cls(**data)


def resolve_contract(row, system_prompt="", provenance=None):
    kind = str(row.get("resultvaluetype") or row.get("result_type") or "STRING").upper()
    kind = re.sub(r"[\s_-]", "", kind)
    name = str(row.get("attributename") or row.get("name") or row.get("attribute") or "")
    prompt = str(row.get("prompt") or row.get("description") or "")
    raw_options = row.get("resultvalueoptions", row.get("options", ""))
    allowed = kind == "BOOLEMPTY" or bool(row.get("allow_blank")) or '<BLANK>' in str(raw_options).upper()
    explicit_empty = bool(re.search(
        r"(?:return|output|respond|provide)\s+(?:an?\s+)?(?:empty\s+string|blank|[\"']{2})", prompt, re.I))
    forbidden_empty = bool(re.search(r"(?:never|must not|do not)\s+(?:return|output)\s+(?:blank|empty)", prompt, re.I))
    allowed = allowed or explicit_empty
    status = "contract conflict" if allowed and forbidden_empty else "resolved"
    # Four labels alone do not establish business decision boundaries.
    if name.lower() == "call_type_acq":
        status = "rubric approval required"
    value_mode = str(row.get("value_mode") or "")
    if kind in {"STRING", "TEXT"} and (allowed or re.search(r"reasoning|verbatim", name, re.I)):
        value_mode = "TEXT_PRESENCE_3_WORDS"
    job = str(row.get("jobname") or row.get("job") or "").upper()
    return Contract(str(row.get("attributeid") or row.get("attribute_id") or ""), name, job,
                    kind, options(raw_options), prompt, system_prompt, allowed,
                    "chat" if "CHAT" in job else "voice", status, provenance=provenance or {},
                    value_mode=value_mode)


def normalize(contract, raw, present=True):
    if not present:
        return {"status": "missing", "value": None, "labels": [], "reason": "required field absent"}
    if raw is None or (isinstance(raw, str) and not raw.strip()) or raw == "<BLANK>":
        return {"status": "allowed_empty" if contract.allow_empty else "missing", "value": "",
                "labels": [], "reason": "contract empty rule" if contract.allow_empty else "required output blank"}
    kind = contract.result_type
    if kind in {"BOOL", "BOOLEMPTY"}:
        token = str(raw).strip().lower()
        if token in {"1", "true", "0", "false"}:
            value = token in {"1", "true"}
            return {"status": "valid", "value": value, "labels": ["true" if value else "false"],
                    "reason": "boolean normalization"}
    elif kind == "STRINGPICKONE" and contract.options:
        found = next((label for label in contract.options if str(raw).strip().casefold() == label.casefold()), None)
        if found is not None:
            return {"status": "valid", "value": found, "labels": [found], "reason": "declared option"}
    elif kind == "LIST" and contract.options:
        values = raw
        if isinstance(raw, str):
            try:
                values = json.loads(raw)
            except json.JSONDecodeError:
                values = [x.strip() for x in raw.split(",")]
        if isinstance(values, list):
            mapping = {x.casefold(): x for x in contract.options}
            if all(isinstance(x, str) and x.strip().casefold() in mapping for x in values):
                labels = sorted(set(mapping[x.strip().casefold()] for x in values))
                return {"status": "valid", "value": labels, "labels": labels, "reason": "finite multi-label"}
    elif kind in NUMERIC_TYPES:
        try:
            value = float(raw)
        except (ValueError, TypeError):
            value = math.nan
        if math.isfinite(value) and not isinstance(raw, bool):
            return {"status": "valid", "value": value, "labels": [], "reason": "finite numeric"}
    elif kind == "LIST":
        values = raw
        if isinstance(raw, str):
            try:
                values = json.loads(raw)
            except json.JSONDecodeError:
                values = None
        if isinstance(values, list):
            return {"status": "valid", "value": values, "labels": [], "reason": "unbounded list; semantic drift deferred"}
    elif kind in {"STRING", "TEXT"}:
        if isinstance(raw, str):
            presence = len(raw.split()) >= 3
            return {"status": "valid", "value": raw, "labels": ["text_present" if presence else "text_short"]
                    if contract.value_mode == "TEXT_PRESENCE_3_WORDS" else [], "reason": "text, not semantics"}
    return {"status": "invalid", "value": raw, "labels": [], "reason": "outside declared output contract"}


def aggregate_values(contract, rows, window):
    days = {day.isoformat(): {"day": day.isoformat(), "eligible": 0, "valid": 0,
                              "allowed_empty": 0, "missing": 0, "invalid": 0, "categories": {},
                              "labels_per_call_sum": 0, "raw_values": {}, "segments": {}}
            for day in window.days()}
    for row in rows:
        window.require(row["day"])
        day = days[str(row["day"])[:10]]
        count = int(row.get("count", 1))
        if count < 0:
            raise ValueError("Negative daily counts.")
        value = normalize(contract, row.get("value"), row.get("present", True))
        if contract.value_mode == "TEXT_PRESENCE_3_WORDS" and value["status"] in {"allowed_empty", "missing"}:
            value["labels"] = ["text_short"]
        day["eligible"] += count
        day[value["status"]] += count
        day["raw_values"][canonical_json(row.get("value"))] = count + day["raw_values"].get(
            canonical_json(row.get("value")), 0)
        day["labels_per_call_sum"] += len(value["labels"]) * count
        for label in value["labels"]:
            day["categories"][label] = day["categories"].get(label, 0) + count
        for dimension in ("brand", "language", "site"):
            segment = row.get(dimension)
            if segment is not None:
                part = day["segments"].setdefault(dimension, {}).setdefault(str(segment), {"n": 0, "counts": {}})
                part["n"] += count
                for label in value["labels"]:
                    part["counts"][label] = part["counts"].get(label, 0) + count
    return list(days.values())
