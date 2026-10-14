"""Saved Attribute Health presentation and explicit local-review actions.

GETs never query the warehouse or launch work. Transcript drilldown is the only
warehouse read, scoped to an already selected immutable manifest item.
"""
from __future__ import annotations

import copy
import hashlib
import ipaddress
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit

PKG_ROOT = Path(__file__).resolve().parent.parent
if str(PKG_ROOT) not in sys.path:
    sys.path.insert(0, str(PKG_ROOT))

from attribute_health.config import Settings, Window
from attribute_health.contracts import digest
from model_lab import ApiResponse

PREFIX = "/api/attribute-health"
MAX_BODY = 32_768
SAFE_ID = r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}"
STATUSES = (
    "Not yet evaluated", "Insufficient data", "No detected shift", "Watch", "Candidate",
    "AI review pending", "Audit queued", "Audit complete", "Needs human review", "Resolved",
    "Technical error", "Monitoring",
)
OUTCOMES = (
    "Likely classification degradation", "Real population change supported", "Ambiguous contract",
    "Data/processing issue", "Inconclusive",
)
FILTERS = {"search", "attribute", "job", "channel", "brand", "type", "severity", "status",
           "decision", "start", "end", "limit", "offset"}
PRIVATE_KEYS = {"transcript", "full_transcript", "transcript_text", "content", "contenttranslated",
                "content_translated", "analysiscontent", "analysis_content", "api_key", "api_token",
                "authorization", "private_key", "private_key_pem", "bearer_token", "gateway_error",
                "raw_response", "file_path", "state_dir", "full_text", "source_transcript", "token",
                "password", "secret", "credentials", "client_secret", "snowflake_password",
                "odin_api_token", "odin_bearer_token", "odin_api_key", "snowflake_private_key_pem"}
IDENTITY_FIELDS = {"production_result_id", "transcript_analysis_id", "analysis_content_id", "connection_id",
                   "job", "interaction_date", "day", "match_method", "status", "completeness",
                   "transcript_hash", "content_rows", "output_consistency"}


class HealthError(ValueError):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


def public(value):
    """Do not leak full transcripts, credentials or storage paths through saved APIs."""
    if isinstance(value, dict):
        return {str(k): public({name: part for name, part in v.items() if name in IDENTITY_FIELDS})
                if str(k) == "identity" and isinstance(v, dict) else public(v) for k, v in value.items()
                if str(k).lower() not in PRIVATE_KEYS}
    if isinstance(value, (list, tuple)):
        return [public(v) for v in value]
    return value


def strict_json(raw):
    def pairs(values):
        result = {}
        for key, value in values:
            if key in result:
                raise HealthError("Duplicate JSON fields are not allowed.")
            result[key] = value
        return result

    try:
        result = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs,
                            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise HealthError("Request body must be a finite JSON object.") from exc
    if not isinstance(result, dict):
        raise HealthError("Request body must be a JSON object.")
    return result


def local_request(peer, host, port, origin=None, write=False, fetch_site=None):
    """Reject remote clients, DNS rebinding and cross-origin writes; no enterprise auth claim."""
    try:
        if not ipaddress.ip_address(peer).is_loopback:
            raise ValueError()
        parsed = urlsplit("http://" + str(host))
        if (parsed.hostname not in {"localhost", "127.0.0.1", "::1"}
                or parsed.username or parsed.password or parsed.path or parsed.query or parsed.fragment
                or (parsed.port or 80) != port):
            raise ValueError()
        if write:
            source = urlsplit(str(origin or ""))
            if (source.scheme != "http" or source.netloc.lower() != parsed.netloc.lower()
                    or source.username or source.password or source.path or source.query or source.fragment
                    or fetch_site not in {None, "same-origin", "none"}):
                raise ValueError()
    except (ValueError, TypeError):
        raise HealthError("Attribute Health requires loopback access and same-origin local writes.", 403)


def fields(payload, required, optional=()):
    if not isinstance(payload, dict) or set(payload) - set(required) - set(optional):
        raise HealthError("Unknown or invalid request fields.")
    if set(required) - set(payload):
        raise HealthError("Required request fields are missing.")


def text(value, label, maximum=4000, blank=False):
    if (not isinstance(value, str) or len(value) > maximum
            or (not blank and not value.strip()) or "\x00" in value):
        raise HealthError(f"{label} must be text of at most {maximum} characters.")
    return value.strip()


class HealthService:
    def __init__(self, settings=None, store=None, repository_factory=None):
        self.settings = settings or Settings.from_env()
        if store is None:
            from attribute_health.store import Store
            store = Store(self.settings)
        self.store = store
        self.repository_factory = repository_factory

    def _query(self, query):
        if set(query) - FILTERS:
            raise HealthError("Unsupported filter; file paths and artifact URLs are not accepted.")
        result = {}
        for key, values in query.items():
            if not isinstance(values, list) or len(values) != 1:
                raise HealthError("Each filter must occur exactly once.")
            result[key] = text(values[0], "Filter", 240, blank=True)
        if bool(result.get("start")) != bool(result.get("end")):
            raise HealthError("Supply both start and end dates.")
        if result.get("start"):
            try:
                window = Window.from_dict({"start": result["start"], "end": result["end"]})
            except (ValueError, TypeError) as exc:
                raise HealthError("Dates must be 1–90 inclusive complete America/Chicago dates.") from exc
            result["_window"] = window
        for key, default, maximum in (("limit", 500, 2000), ("offset", 0, 1_000_000)):
            try:
                number = int(result.get(key, default))
            except ValueError as exc:
                raise HealthError("Invalid pagination.") from exc
            if number < (1 if key == "limit" else 0) or number > maximum:
                raise HealthError("Pagination is outside the supported limits.")
            result[key] = number
        return result

    def _list(self, kind):
        return copy.deepcopy(self.store.list(kind))

    def _incident(self, incident_id):
        row = self.store.get("incidents", incident_id)
        if not row:
            raise HealthError("Incident not found.", 404)
        try:
            Window.from_dict(row["window"])
        except (KeyError, ValueError, TypeError) as exc:
            raise HealthError("Saved incident has an invalid analytical window.", 409) from exc
        return copy.deepcopy(row)

    @staticmethod
    def _brands(row):
        contract = row.get("contract") or row
        brands = set(row.get("brands") or contract.get("brands") or [])
        mix = row.get("mix") or {}
        brands.update((mix.get("brands") or {}).keys())
        brand_mix = mix.get("brand") or {}
        brands.update(part.get("name") for part in brand_mix.get("segments", [])
                      if isinstance(part, dict) and part.get("name"))
        brands.update(brand_mix.get("unknown_new_segments") or [])
        return brands

    @staticmethod
    def _match(row, filters):
        contract = row.get("contract") or row
        joined = dict(contract, **row)
        query = filters.get("search", "").casefold()
        if query and query not in " ".join(str(joined.get(k, "")) for k in
                                         ("name", "attribute_id", "job", "channel", "contract_key")).casefold():
            return False
        for field, source in (("attribute", "name"), ("type", "result_type"), ("job", "job"),
                              ("channel", "channel"), ("severity", "severity"), ("status", "status")):
            if filters.get(field) and str(joined.get(source, "")).casefold() != filters[field].casefold():
                return False
        if filters.get("decision"):
            review = row.get("review") or {}
            decision = review.get("decision") or review.get("recommendation", "")
            if str(decision).casefold() != filters["decision"].casefold():
                return False
        if filters.get("brand"):
            if filters["brand"] not in HealthService._brands(row):
                return False
        window = filters.get("_window")
        if window and row.get("window"):
            saved = Window.from_dict(row["window"])
            if saved.end < window.start or saved.start > window.end:
                return False
        return True

    def _overview(self, filters):
        registry = self._list("registry")
        saved_runs = self._list("runs")
        discovered = sorted(saved_runs, key=lambda x: str(x.get("created_at") or x.get("completed_at") or ""),
                            reverse=True)
        current_registry = next((r["contracts"] for r in discovered if isinstance(r.get("contracts"), list)), None)
        if current_registry is not None:
            # Registry storage also retains old contract hashes for historical
            # incidents. Only the latest discovery describes currently active jobs.
            registry = current_registry
        incidents = sorted(reversed(self._list("incidents")),
                           key=lambda x: str(x.get("updated_at") or x.get("created_at") or
                                             (x.get("latest_run_window") or x.get("window") or {}).get("end", "")),
                           reverse=True)
        latest = {}
        for incident in incidents:
            latest.setdefault(incident.get("contract_key"), incident)
        rows = []
        rubrics = self._list("rubrics")
        for contract in registry:
            incident = latest.get(contract.get("key")) or {}
            status = incident.get("status") or "Not yet evaluated"
            status = {"Audit partial": "Needs human review",
                      "Retry queued": "Audit queued", "Out of budget": "Audit queued",
                      "Trend review": "AI review pending"}.get(status, status)
            if status not in STATUSES:
                status = "Technical error"
            disposition = incident.get("disposition") or {}
            if disposition.get("action") == "Resolved":
                status = "Resolved"
            elif disposition.get("action") == "Needs human review":
                status = "Needs human review"
            row = dict(contract, contract=contract, contract_key=contract.get("key"),
                       incident_id=incident.get("id"), status=status)
            for key in ("severity", "window", "signals", "statistics", "mix", "review", "summary",
                        "disposition", "next_action", "baseline_ageing", "brands"):
                row[key] = incident.get(key)
            row["brands"] = sorted(self._brands(row))
            row["rubric"] = max((r for r in rubrics if r.get("contract_key") == contract.get("key")),
                                key=lambda r: r.get("version", 0), default=None)
            row["runtime_enabled"] = self.settings.all_active_jobs or (
                contract.get("job") in self.settings.jobs and contract.get("channel", "voice") == "voice")
            rows.append(row)
        counts = {status: sum(row["status"] == status for row in rows) for status in STATUSES}
        filtered = [row for row in rows if self._match(row, filters)]
        runs = sorted(saved_runs, key=lambda x: str(x.get("completed_at") or x.get("updated_at") or
                       x.get("created_at") or x.get("id", "")), reverse=True)
        last = next((row for row in runs if row.get("status") in {"complete", "completed", "success",
                   "successful", "summarized", "Audit complete"}), None)
        effective = (runs[0].get("window") if runs else None) or Window.ending().as_dict()
        demo = any(row.get("mode") in {"fixture", "demo"}
                   or row.get("fixture") or row.get("demo") for row in runs)
        demo = demo or any(row.get("attribute_id", "").startswith("demo-") for row in registry)
        return public({
            "contracts": filtered[filters["offset"]:filters["offset"] + filters["limit"]],
            "total": len(filtered), "all_contracts": len(rows), "counts": counts,
            "window": effective, "last_successful_run": last, "latest_run": runs[0] if runs else None,
            "source_watermark": effective.get("watermark"), "demo": demo,
            "sample_summary": self._sample_summary(runs[0] if runs else None, incidents),
            "backlog": sum(row.get("status") in {"Candidate", "AI review pending", "Audit queued",
                                                "Technical error"} for row in incidents),
            "budgets": self.store.budget_summary(), "statuses": STATUSES, "outcomes": OUTCOMES,
            "safety": {"reviewer_identity": "Explicit local reviewer; not enterprise authentication",
                       "inference_enabled": False, "configured_worker_ai": self.settings.ai_enabled,
                       "retry": "Local queue only; no dispatch", "refresh": "Saved state only"},
            "coverage": {"jobs": self.settings.jobs, "all_active_jobs": self.settings.all_active_jobs},
            "date_policy": "Filters select saved incidents; never rewrite immutable evidence windows.",
        })

    def _sample_summary(self, latest_run, incidents):
        run_id = latest_run.get("id") if latest_run else None
        selected = [row for row in incidents if not run_id or
                    run_id in row.get("run_ids", [row.get("run_id")])]
        by_incident = {row["id"]: row for row in selected}
        manifests = [row for row in self._list("manifests")
                     if row.get("incident_id") in by_incident]
        manifests.extend(row["sample_manifest"] for row in selected
                         if isinstance(row.get("sample_manifest"), dict))
        calls, items = set(), set()
        for manifest in manifests:
            incident = by_incident.get(manifest.get("incident_id")) or {}
            contract_key = manifest.get("contract_key") or incident.get("contract_key")
            for item in manifest.get("items") or []:
                identity = item.get("identity") or item
                result_id = identity.get("production_result_id")
                item_id = item.get("item_id") or item.get("id")
                if result_id is not None:
                    calls.add((str(result_id), str(identity.get("job") or ""),
                               str(identity.get("connection_id") or "")))
                if item_id is not None:
                    items.add((str(contract_key or ""), str(item_id)))
        saved = [row for row in self._list("judgments")
                 if row.get("incident_id") in by_incident and row.get("item_id")]
        judgments = {(row.get("incident_id"), row.get("item_id")) for row in saved}
        cache_hits = {(row.get("incident_id"), row.get("item_id")) for row in saved if row.get("cache_hit")}
        return {"scope": "Latest saved run" if run_id else "All saved incidents", "run_id": run_id,
                "sampled_unique_source_calls": len(calls), "sampled_call_attribute_items": len(items),
                "saved_judgment_items": len(judgments), "cache_hit_judgment_items": len(cache_hits),
                "notice": "Saved source identities and call-attribute items, deduplicated across "
                          "manifest versions. Distinct request-content hashes in the daily dispatch "
                          "ledger are not unique source calls; cache reuse can reduce requests."}

    def _evidence(self, incident):
        evidence = incident.get("evidence")
        expected_hash = None
        if isinstance(evidence, dict) and evidence.get("daily_run_id"):
            from attribute_health.evidence import coverage_packet
            try:
                evidence = coverage_packet(self.store, incident)
            except (KeyError, ValueError, TypeError) as exc:
                raise HealthError("Frozen coverage evidence is unavailable or mismatched.", 409) from exc
        if isinstance(evidence, str):
            evidence = self.store.get("evidence", evidence)
        if not isinstance(evidence, dict):
            raise HealthError("Immutable evidence is not yet available.", 409)
        if evidence.get("packet_hash"):
            reference = evidence["packet_hash"]
            expected_hash = reference
            if not re.fullmatch(r"[0-9a-f]{64}", str(reference)):
                raise HealthError("Invalid immutable packet reference.", 409)
            stored = self.store.get("evidence", reference)
            if stored:
                evidence = stored
            elif not evidence.get("packet"):
                raise HealthError("Immutable evidence packet is unavailable.", 409)
        result = copy.deepcopy(evidence)
        packet = result.get("packet") or result
        if not packet.get("hash") or (expected_hash and packet["hash"] != expected_hash):
            raise HealthError("Immutable evidence packet reference does not match.", 409)
        if digest({k: v for k, v in packet.items() if k not in {"hash", "id"}}) != packet["hash"]:
            raise HealthError("Immutable evidence packet hash does not match its data.", 409)
        if (packet.get("contract") or {}).get("key") != incident.get("contract_key"):
            raise HealthError("Evidence contract does not match the selected incident.", 409)
        if packet.get("window") and (
                Window.from_dict(packet["window"]).as_dict()["start"] != incident["window"]["start"]
                or packet["window"]["end"] != incident["window"]["end"]):
            raise HealthError("Evidence and incident windows do not match.", 409)
        window = Window.from_dict(incident["window"])
        for row in packet.get("daily", packet.get("days", [])):
            if not isinstance(row, dict) or not (row.get("day") or row.get("date")):
                raise HealthError("Invalid saved daily evidence.", 409)
            try:
                window.require(row.get("day") or row.get("date"))
            except ValueError as exc:
                raise HealthError("Saved evidence falls outside the incident window.", 409) from exc
        return result

    def _chart(self, incident):
        evidence = self._evidence(incident)
        reference = incident.get("evidence") or {}
        reference = reference if isinstance(reference, dict) else {}
        chart_id = reference.get("chart_hash") or reference.get("chart_spec_hash")
        if chart_id and not re.fullmatch(r"[0-9a-f]{64}", str(chart_id)):
            raise HealthError("Invalid immutable chart reference.", 409)
        chart = self.store.get("charts", chart_id) if chart_id else None
        chart = chart or evidence.get("chart") or {}
        spec = chart.get("spec") or chart.get("option") or evidence.get("chart_spec")
        if not spec and reference.get("daily_run_id"):
            from attribute_health.evidence import chart_spec
            spec = chart_spec(evidence)
        if not isinstance(spec, dict):
            raise HealthError("Deterministic chart is not yet available.", 409)
        packet = evidence.get("packet") or evidence
        data_hash = packet.get("hash") or evidence.get("packet_hash") or evidence.get("data_hash")
        chart_hash = chart.get("packet_hash") or chart.get("data_hash") or data_hash
        if not data_hash or not re.fullmatch(r"[0-9a-f]{64}", str(data_hash)) or chart_hash != data_hash:
            raise HealthError("Chart/evidence hash is missing or mismatched.", 409)
        if chart_id and (chart.get("hash") != chart_id or digest(spec) != chart_id):
            raise HealthError("Immutable chart hash does not match its spec.", 409)
        if reference.get("packet_hash") and reference["packet_hash"] != data_hash:
            raise HealthError("Incident and packet hashes do not match.", 409)

        def check(value, depth=0):
            if depth > 30:
                raise HealthError("Chart nesting exceeds the safe limit.", 409)
            if isinstance(value, dict):
                for key, part in value.items():
                    if key.lower() in {"url", "href", "src", "path", "image", "link"}:
                        raise HealthError("External chart assets are not allowed.", 409)
                    if key == "formatter" and isinstance(part, str) and re.search(r"<|function|=>", part):
                        raise HealthError("Executable/HTML chart formatters are not allowed.", 409)
                    check(part, depth + 1)
            elif isinstance(value, list):
                for part in value:
                    check(part, depth + 1)
            elif isinstance(value, str) and re.search(r"(?:https?://|file:|image://|javascript:)", value, re.I):
                raise HealthError("External chart assets are not allowed.", 409)
        check(spec)
        return {"incident_id": incident["id"], "window": incident["window"], "packet_hash": data_hash,
                "chart_hash": chart_id or digest(spec), "chart_spec": copy.deepcopy(spec),
                "artifacts": {"png_hash": reference.get("png_hash"), "render_status": reference.get("render_status"),
                              "available": reference.get("render_status") == "exported"}}

    def _chart_image(self, incident, chart_id):
        chart = self._chart(incident)
        if chart["chart_hash"] != chart_id:
            raise HealthError("Chart artifact does not belong to the selected incident.", 404)
        image = self.store.get("chart_images", chart_id)
        if not image or image.get("packet_hash") != chart["packet_hash"]:
            raise HealthError("Saved chart image is unavailable.", 404)
        if image.get("artifact") != chart_id + ".png":
            raise HealthError("Invalid saved chart artifact reference.", 409)
        root = Path(self.store.root).resolve()
        path = root / "charts" / (chart_id + ".png")
        if (path.resolve().parent != (root / "charts").resolve() or path.is_symlink()
                or (root / "charts").is_symlink() or (root / "charts").resolve().parent != root):
            raise HealthError("Invalid saved chart artifact location.", 409)
        if not path.is_file():
            raise HealthError("Saved chart image is unavailable.", 404)
        if path.stat().st_size > 10_000_000:
            raise HealthError("Saved image exceeds the bounded artifact limit.", 413)
        body = path.read_bytes()
        if not body.startswith(b"\x89PNG\r\n\x1a\n") or hashlib.sha256(body).hexdigest() != image.get("png_hash"):
            raise HealthError("Saved chart image failed immutable hash validation.", 409)
        return ApiResponse(200, body, "image/png")

    def _manifest(self, incident):
        manifest = incident.get("sample_manifest")
        if isinstance(manifest, str):
            manifest = self.store.get("manifests", manifest)
        if not manifest:
            manifest = next((row for row in self._list("manifests")
                             if row.get("incident_id") == incident["id"]), None)
        if not isinstance(manifest, dict):
            return {"items": [], "status": "pending"}
        if manifest.get("window") and (
                Window.from_dict(manifest["window"]).start.isoformat() != incident["window"]["start"]
                or manifest["window"]["end"] != incident["window"]["end"]):
            raise HealthError("Saved manifest and incident windows do not match.", 409)
        if manifest.get("contract_key") and manifest["contract_key"] != incident.get("contract_key"):
            raise HealthError("Manifest contract does not match the incident.", 409)
        if manifest.get("hash") and digest({k: v for k, v in manifest.items()
                                          if k not in {"hash", "id"}}) != manifest["hash"]:
            raise HealthError("Saved manifest hash does not match its selected items.", 409)
        return manifest

    def _audits(self, incident):
        judgments = [row for row in self._list("judgments") if row.get("incident_id") == incident["id"]]
        reviews = [row for row in self._list("item_reviews") if row.get("incident_id") == incident["id"]]
        return public({"incident_id": incident["id"], "summary": incident.get("summary"),
                       "manifest": self._manifest(incident), "judgments": judgments, "item_reviews": reviews,
                       "notice": "Stratified samples are not unweighted population accuracy. "
                                 "Uncertain judgments are neither correct nor incorrect."})

    def _detail(self, incident):
        result = public(incident)
        result["history"] = public([row for row in self._list("dispositions")
                                   if row.get("incident_id") == incident["id"]])
        result["rubrics"] = public([row for row in self._list("rubrics")
                                   if row.get("contract_key") == incident.get("contract_key")])
        result["retry"] = self._retry_eligibility(incident)
        return result

    def _retry_eligibility(self, incident):
        allowed = incident.get("status") in {"Technical error", "AI review pending", "Audit queued",
                                            "Audit partial", "Out of budget", "Retry queued"}
        pending = any(row.get("incident_id") == incident["id"] and row.get("status") in
                      {"queued", "pending"} for row in self._list("retries"))
        budgets = self.store.budget_summary()
        exhausted = budgets.get("exhausted", False) or budgets.get("remaining") == 0
        stage = "review" if incident.get("status") == "AI review pending" or (
            incident.get("error") or {}).get("stage") == "review" else "audit"
        for key, limit in (budgets.get("limits") or {}).items():
            applicable = {"input_tokens", "output_tokens",
                          "review_attempts" if stage == "review" else "transcript_attempts"}
            if key in applicable:
                if budgets.get(key, 0) >= limit:
                    exhausted = True
        for key in ("transcript", "review", "input_tokens", "output_tokens"):
            entry = budgets.get(key)
            if isinstance(entry, dict) and entry.get("remaining") == 0:
                exhausted = True
        reason = ("A local retry is already queued." if pending else
                  "Daily budget is exhausted." if exhausted else
                  "Only pending or technically failed work can be queued." if not allowed else
                  "Queues local work only; this Console never dispatches inference.")
        return {"eligible": allowed and not pending and not exhausted, "reason": reason,
                "dispatch_enabled": False, "stage": stage}

    def handle_get(self, path, query):
        try:
            filters = self._query(query)
            image = re.fullmatch(PREFIX + rf"/incidents/({SAFE_ID})/chart-image/([0-9a-f]{{64}})\.png", path)
            if image:
                if query:
                    raise HealthError("Saved artifact endpoints do not accept paths or filters.")
                return self._chart_image(self._incident(image[1]), image[2])
            if path == PREFIX + "/overview":
                return ApiResponse.json(self._overview(filters))
            if path == PREFIX + "/registry":
                result = self._overview(filters)
                return ApiResponse.json({k: result[k] for k in ("contracts", "total", "all_contracts", "coverage")})
            if path in {PREFIX + "/runs", PREFIX + "/incidents"}:
                kind = path.rsplit("/", 1)[-1]
                rows = self._list(kind)
                rows = [row for row in rows if self._match(row, filters)]
                return ApiResponse.json(public({kind: rows[filters["offset"]:filters["offset"] + filters["limit"]],
                                                "total": len(rows)}))
            match = re.fullmatch(PREFIX + rf"/incidents/({SAFE_ID})(?:/(evidence|chart|audits))?", path)
            if match:
                if query:
                    raise HealthError("Saved incident evidence cannot be overridden by filters or paths.")
                incident = self._incident(match[1])
                kind = match[2]
                result = (self._evidence(incident) if kind == "evidence" else
                          self._chart(incident) if kind == "chart" else
                          self._audits(incident) if kind == "audits" else self._detail(incident))
                return ApiResponse.json(public(result))
            raise HealthError("Attribute Health endpoint not found.", 404)
        except Exception as exc:
            return self._error(exc)

    def _transcript(self, incident, item_id, payload):
        fields(payload, ("author", "confirm"))
        text(payload["author"], "Local reviewer", 128)
        if payload["confirm"] is not True:
            raise HealthError("Explicit transcript drilldown consent is required.")
        manifest = self._manifest(incident)
        if manifest.get("incident_id") not in {None, incident["id"]}:
            raise HealthError("Manifest does not belong to this incident.", 409)
        item = next((row for row in manifest.get("items", [])
                     if str(row.get("item_id") or row.get("id")) == item_id), None)
        if not item:
            raise HealthError("Selected manifest item not found.", 404)
        window = Window.from_dict(incident["window"])
        identity = item.get("identity") or item
        day = identity.get("day") or identity.get("interaction_date") or item.get("day")
        if not day:
            raise HealthError("Selected item has no bounded interaction date.", 409)
        window.require(day)
        for field in ("production_result_id", "connection_id", "job"):
            if not identity.get(field):
                raise HealthError("Selected item identity is incomplete.", 409)
        contract_job = (incident.get("contract") or {}).get("job")
        if contract_job and identity["job"] != contract_job:
            raise HealthError("Selected identity job does not match its incident.", 409)
        if self.repository_factory:
            repository = self.repository_factory(incident)
        else:
            from attribute_health.repository import FixtureRepository, SnowflakeRepository
            fixture = incident.get("mode") in {"fixture", "demo"} or manifest.get("fixture")
            if not fixture and incident.get("run_id"):
                run = self.store.get("runs", incident["run_id"]) or {}
                fixture = run.get("mode") in {"fixture", "demo"} or run.get("demo")
            fixture = fixture or (incident.get("contract") or {}).get("attribute_id", "").startswith("demo-")
            repository = FixtureRepository() if fixture else SnowflakeRepository(self.settings)
        selected = copy.deepcopy(item)
        selected.update(identity)
        resolved = repository.transcript(selected, window)
        if not isinstance(resolved, dict):
            raise HealthError("Transcript identity could not be resolved.", 409)
        actual = resolved.get("identity") or resolved
        status = resolved.get("status") or actual.get("status")
        if (status not in {"resolved", "complete", "ok"} or resolved.get("complete") is False
                or resolved.get("truncated") or "truncat" in str(resolved.get("completeness", "")).lower()):
            raise HealthError("Transcript is unresolved, ambiguous or incomplete.", 409)
        for field in ("production_result_id", "connection_id", "job"):
            if str(actual.get(field)) != str(identity[field]):
                raise HealthError("Resolved transcript does not match the saved selected identity.", 409)
        resolved_day = actual.get("day") or actual.get("interaction_date")
        if not resolved_day:
            raise HealthError("Resolved transcript date is unavailable.", 409)
        window.require(resolved_day)
        if str(resolved_day)[:10] != str(day)[:10]:
            raise HealthError("Resolved transcript date differs from the selected identity.", 409)
        body = resolved.get("text") or resolved.get("transcript") or resolved.get("content")
        if not isinstance(body, str) or not body.strip():
            raise HealthError("No complete transcript is available.", 409)
        if len(body) > 2_000_000:
            raise HealthError("Transcript exceeds the bounded drilldown limit.", 413)
        return {"incident_id": incident["id"], "item_id": item_id,
                "identity": {key: public(value) for key, value in actual.items() if key in IDENTITY_FIELDS},
                "transcript": body, "notice": "Intentional bounded read; text is not saved by this Console."}

    def handle_post(self, path, payload, idempotency_key):
        try:
            key = text(idempotency_key, "Idempotency-Key", 128)
            if not re.fullmatch(r"[A-Za-z0-9_.:-]+", key):
                raise HealthError("Invalid Idempotency-Key.")
            if len(json.dumps(payload, allow_nan=False).encode()) > MAX_BODY:
                raise HealthError("JSON body is too large.", 413)
            if path == PREFIX + "/rubrics":
                fields(payload, ("contract_key", "author", "text", "approved"))
                contract_key = text(payload["contract_key"], "Contract key", 240)
                if not any(row.get("key") == contract_key for row in self._list("registry")):
                    raise HealthError("Contract not found.", 404)
                text(payload["author"], "Local reviewer", 128)
                text(payload["text"], "Rubric", 20_000)
                if type(payload["approved"]) is not bool:
                    raise HealthError("Rubric approval must be boolean.")
                return ApiResponse.json(public(self.store.save_rubric(contract_key, payload, key)), 201)
            match = re.fullmatch(PREFIX + rf"/incidents/({SAFE_ID})/(dispositions|retry|items/({SAFE_ID})/(review|transcript))", path)
            if not match:
                raise HealthError("Attribute Health endpoint not found.", 404)
            incident = self._incident(match[1])
            action = match[2]
            if match[4] == "transcript":
                return ApiResponse.json(self._transcript(incident, match[3], payload))
            if action == "dispositions":
                fields(payload, ("author", "outcome", "action", "note"))
                text(payload["author"], "Local reviewer", 128)
                text(payload["note"], "Note", blank=True)
                if payload["outcome"] not in OUTCOMES or payload["action"] not in {
                        "Monitoring", "Resolved", "Needs human review"}:
                    raise HealthError("Unsupported disposition outcome or action.")
                return ApiResponse.json(public(self.store.record_disposition(incident["id"], payload, key)), 201)
            if match[4] == "review":
                fields(payload, ("author", "decision", "note"))
                text(payload["author"], "Local reviewer", 128)
                text(payload["note"], "Note", blank=True)
                if payload["decision"] not in {"accept", "reject", "uncertain"}:
                    raise HealthError("Decision must be accept, reject or uncertain.")
                if not any(row.get("item_id") == match[3] and row.get("incident_id") == incident["id"]
                           for row in self._list("judgments")):
                    raise HealthError("Saved judgment item not found.", 404)
                return ApiResponse.json(public(self.store.record_item_review(incident["id"], match[3], payload, key)), 201)
            fields(payload, ("author", "note"), ("stage",))
            text(payload["author"], "Local reviewer", 128)
            text(payload["note"], "Note", blank=True)
            if payload.get("stage", "audit") not in {"review", "sample", "audit", "summarize"}:
                raise HealthError("Unsupported retry stage.")
            # Permit a replay of the same accepted request; Store enforces payload identity.
            previous = next((row for row in self._list("retries") if row.get("incident_id") == incident["id"]
                             and row.get("idempotency_key") == key), None)
            eligibility = self._retry_eligibility(incident)
            if not previous and not eligibility["eligible"]:
                raise HealthError(eligibility["reason"], 409)
            result = self.store.queue_retry(incident["id"], dict(payload, idempotency_key=key), key)
            return ApiResponse.json({"retry": public(result), "dispatch_enabled": False,
                                     "notice": "Local queue only. No inference was launched."}, 202)
        except Exception as exc:
            return self._error(exc)

    @staticmethod
    def _error(exc):
        if isinstance(exc, HealthError):
            return ApiResponse.json({"error": str(exc)}, exc.status)
        if isinstance(exc, (ValueError, KeyError, TypeError)):
            conflict = "idempot" in str(exc).lower() or "conflict" in str(exc).lower()
            return ApiResponse.json({"error": "Saved state conflict." if conflict else
                                    "Request or saved state failed validation."}, 409 if conflict else 400)
        return ApiResponse.json({"error": "Attribute Health is unavailable. Check local configuration; "
                                "no inference was launched."}, 503)
