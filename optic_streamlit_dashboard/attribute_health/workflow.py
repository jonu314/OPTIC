"""Independent resumable worker; no dashboard refresh or local retry launches inference."""
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import sys
import uuid
from dataclasses import asdict
from datetime import date, timedelta

from .ai import (AIError, JUDGE_MODEL, REVIEW_PROMPT_VERSION, compare_output, extraction_key,
                 judge_extract, packet_hash, review_trend, validate_identity)
from .config import Settings, Window
from .contracts import Contract, aggregate_values, canonical_json, digest
from .sampling import build_manifest, expansion_needed, summarize
from .store import BudgetExceeded, ConflictError, LeaseLost, RunEnvelopeExceeded, Store

STAGES = ("detect", "review", "sample", "audit", "summarize")


def _safe_error(exc):
    if isinstance(exc, AIError):
        return {"code": exc.code, "retryable": exc.retryable}
    if isinstance(exc, BudgetExceeded):
        return {"code": "shared_budget_exhausted", "retryable": True}
    return {"code": type(exc).__name__, "retryable": not isinstance(exc, (LeaseLost, ConflictError))}


class Worker:
    def __init__(self, repository, settings=None, store=None, *, client=None, fixture=False,
                 dry_run=False, plan_only=False, read_only=False, render=True, detector=None,
                 evidence_module=None, owner=None):
        self.settings = settings or Settings.from_env()
        self.store = store or Store(self.settings)
        self.repository, self.client = repository, client
        self.fixture, self.dry_run, self.plan_only, self.read_only = fixture, dry_run, plan_only, read_only
        self.render = render and not plan_only
        self.detector, self.evidence_module = detector, evidence_module
        self.owner = owner or uuid.uuid4().hex
        self.lease = None
        self.readiness = {}
        if getattr(client, "fixture", False) and (not fixture or not getattr(repository, "demo", False)):
            raise ValueError("Fake AI is only allowed for an explicitly selected synthetic fixture repository.")
        if self.settings.judge_model != JUDGE_MODEL:
            raise ValueError("Independent transcript judge is fixed to gpt-6.1-sol.")

    @property
    def inference_allowed(self):
        return self.client is not None and not (self.dry_run or self.plan_only or self.read_only) and (
            self.settings.ai_enabled or (self.fixture and getattr(self.client, "fixture", False)))

    def _modules(self):
        if self.detector is None:
            from .detection import detect
            self.detector = detect
        if self.evidence_module is None:
            from . import evidence
            self.evidence_module = evidence

    def _renew(self):
        self.lease = self.store.renew(self.lease)

    def execute(self, stage="run", *, window=None, resume=None, expand=False, reevaluate=False):
        if stage not in STAGES + ("run",):
            raise ValueError("Unknown worker stage.")
        if stage not in {"detect", "run"} and not resume:
            raise ValueError("Independent later stages require an explicit frozen --resume run ID.")
        if reevaluate and (not resume or not self.inference_allowed):
            raise ValueError("Reevaluation requires explicit resume and inference authorization.")
        self._modules()
        run_id = resume or uuid.uuid4().hex
        self.lease = self.store.claim("run:" + run_id, self.owner)
        if not self.lease:
            return {"id": run_id, "status": "leased", "next_action": "Another worker owns this run."}
        bind_heartbeat = getattr(self.repository, "set_heartbeat", None)
        try:
            if callable(bind_heartbeat):
                bind_heartbeat(self._renew)
            if resume:
                run = self.store.get("runs", run_id)
                if not run:
                    raise ValueError("Unknown run to resume.")
                if bool(run["fixture"]) != self.fixture:
                    raise ValueError("Resume cannot change real/fixture data mode.")
                if window is not None and window.as_dict() != run["window"]:
                    raise ValueError("Resume cannot change the frozen run window.")
                run_window = Window.from_dict(run["window"])
                contracts = [Contract.from_dict(x) for x in run["contracts"]]
                daily = self.store.get("daily", run_id)["by_key"]
                readiness = run.get("readiness") or {}
            else:
                proposed = window or Window.ending()
                readiness = self.repository.readiness(proposed)
                if not isinstance(readiness, dict) or not readiness.get("ready_through"):
                    raise ValueError("Repository readiness must return ready_through.")
                ready = date.fromisoformat(str(readiness["ready_through"])[:10])
                run_window = Window.ending(end=min(proposed.end, ready), watermark=readiness.get("watermark"))
                contracts = self.repository.registry()
                if not isinstance(contracts, list) or not all(isinstance(x, Contract) for x in contracts):
                    raise ValueError("Repository registry must return Contract objects.")
                if len({x.key for x in contracts}) != len(contracts):
                    raise ValueError("Registry contains duplicate contract identities.")
                if callable(getattr(self.repository, "apply_readiness", None)):
                    self.repository.apply_readiness(readiness, run_window)
                daily = self.repository.aggregate(contracts, run_window)
                self._validate_daily(contracts, daily, run_window)
                settings_snapshot = {k: v for k, v in asdict(self.settings).items() if k != "state_dir"}
                run = {"id": run_id, "window": run_window.as_dict(),
                       "contracts": [x.as_dict() for x in contracts], "readiness": readiness,
                       "settings": settings_snapshot, "settings_hash": digest(settings_snapshot),
                       "fixture": self.fixture, "mode": "fixture" if self.fixture else "real",
                       "demo": self.fixture, "source_mode": "fixture" if self.fixture else "read-only source",
                       "created_at": self.store._now().isoformat(),
                       "initial_mode": {"dry_run": self.dry_run, "plan_only": self.plan_only,
                                        "read_only": self.read_only}}
                self.store.put("runs", run_id, run, immutable=True, lease=self.lease)
                self.store.put("daily", run_id, {"by_key": daily, "hash": digest(daily),
                                                "window": run_window.as_dict()}, immutable=True, lease=self.lease)
                for contract in contracts:
                    self.store.put("registry", contract.key, contract.as_dict(), lease=self.lease)
            self.readiness = readiness
            if resume and callable(getattr(self.repository, "apply_readiness", None)):
                self.repository.apply_readiness(readiness, run_window)
            self._validate_daily(contracts, daily, run_window)
            for incident in self._incidents(run_id):
                action = (incident.get("disposition") or {}).get("action")
                if action == "Resolved" and (incident.get("status") != action or incident.get("next_action") != action):
                    incident.update(status=action, next_action=action)
                    self.store.put("incidents", incident["id"], incident, lease=self.lease)
            stages = STAGES if stage == "run" else (stage,)
            for current in stages:
                checkpoint = self.store.get("checkpoints", f"{run_id}:{current}")
                input_hash = self._stage_hash(run_id, current, expand, reevaluate)
                incident_ids = {x["id"] for x in self._incidents(run_id)}
                queued = [x for x in self.store.list("retries") if x.get("incident_id") in incident_ids
                          and x.get("stage", "audit") == current and x.get("status") in {"queued", "pending"}]
                queued = queued if current not in {"review", "audit"} or self.inference_allowed else []
                if checkpoint and checkpoint["status"] == "done" and checkpoint.get("input_hash") == input_hash and not queued and not (
                        (expand and current in {"sample", "audit", "summarize"}) or
                        (reevaluate and current in {"audit", "summarize"})):
                    continue
                self._renew()
                function = getattr(self, "_" + current)
                result = function(run_id, contracts, daily, run_window, expand=expand, reevaluate=reevaluate)
                done = result.pop("_done", True)
                self.store.put("checkpoints", f"{run_id}:{current}", {
                    "run_id": run_id, "stage": current, "status": "done" if done else "pending",
                    "result": result, "input_hash": input_hash,
                    "completed_at": self.store._now().isoformat()}, lease=self.lease)
                for retry in queued:
                    retry.update(status="completed" if done else "pending",
                                 processed_by_run=run_id, updated_at=self.store._now().isoformat())
                    self.store.put("retries", retry["id"], retry, lease=self.lease)
            checkpoints = [x for x in self.store.list("checkpoints") if x["run_id"] == run_id]
            progress = {"id": run_id, "status": "complete" if all(
                x["status"] == "done" for x in checkpoints) else "partial",
                "stages": {x["stage"]: x["status"] for x in checkpoints},
                "updated_at": self.store._now().isoformat(), "budget": self.store.budget_summary()}
            progress["run_envelopes"] = self.store.run_envelope_summary(run_id)
            run_incidents = self._incidents(run_id)
            run_incident_ids = {x["id"] for x in run_incidents}
            manifested = [item for incident in run_incidents
                          if isinstance(incident.get("sample_manifest"), dict)
                          for item in incident["sample_manifest"].get("items", [])]
            run_judgments = [x for x in self.store.list("judgments") if x["incident_id"] in run_incident_ids]
            progress["audit_counts"] = {
                "call_attribute_items": len({(x["contract_key"], x["item_id"]) for x in manifested}),
                "unique_production_results": len({x["production_result_id"] for x in manifested}),
                "unique_source_calls": len({(x["production_result_id"], x["job"], x["connection_id"]) for x in manifested}),
                "saved_judgments": len(run_judgments),
                "cache_hits": sum(bool(x.get("cache_hit")) for x in run_judgments),
                "unique_content_extractions": len({x.get("identity", {}).get("transcript_hash")
                    for x in run_judgments if x.get("identity", {}).get("transcript_hash")})}
            self.store.put("run_progress", run_id, progress, lease=self.lease)
            return self.store.get("runs", run_id)
        except Exception as exc:
            # Boundary handler persists a sanitized failure and then re-raises, never a silent fallback.
            error = {"run_id": run_id, "stage": stage, **_safe_error(exc),
                     "created_at": self.store._now().isoformat()}
            if not isinstance(exc, LeaseLost):
                try:
                    self.store.put("errors", uuid.uuid4().hex, error, lease=self.lease)
                    self.store.put("run_progress", run_id, {"status": "Technical error", "error": error},
                                   lease=self.lease)
                except (sqlite3.Error, OSError) as storage_error:
                    logging.getLogger(__name__).warning("Health failure could not be persisted: %s",
                                                       type(storage_error).__name__)
            raise
        finally:
            if callable(bind_heartbeat):
                bind_heartbeat(None)
            if self.lease:
                active_error = sys.exception()
                try:
                    self.store.release(self.lease)
                except LeaseLost:
                    # A fenced worker must not release its replacement's claim.
                    pass
                except (sqlite3.Error, OSError) as storage_error:
                    if active_error is None:
                        raise
                    logging.getLogger(__name__).warning("Health failure lease cleanup deferred: %s",
                                                       type(storage_error).__name__)
                self.lease = None

    @staticmethod
    def _validate_daily(contracts, daily, window):
        if not isinstance(daily, dict) or set(daily) != {x.key for x in contracts}:
            raise ValueError("Aggregate output must include every registry contract exactly once.")
        for rows in daily.values():
            seen = set()
            for row in rows:
                if not isinstance(row, dict) or "day" not in row:
                    raise ValueError("Invalid daily aggregate row.")
                window.require(row["day"])
                day = str(row["day"])[:10]
                if day in seen:
                    raise ValueError("Duplicate daily aggregate date.")
                seen.add(day)
                for field in ("eligible", "valid", "allowed_empty", "missing", "invalid"):
                    if type(row.get(field)) is not int or row[field] < 0:
                        raise ValueError("Invalid aggregate counts.")
                if sum(row[x] for x in ("valid", "allowed_empty", "missing", "invalid")) != row["eligible"]:
                    raise ValueError("Daily normalization counts do not reconcile.")
                if not isinstance(row.get("categories"), dict) or any(
                        type(x) is not int or not 0 <= x <= row["eligible"] for x in row["categories"].values()):
                    raise ValueError("Invalid category numerators.")
            if seen != {x.isoformat() for x in window.days()}:
                raise ValueError("Aggregate must preserve the full calendar, including zero-volume dates.")

    def _incidents(self, run_id):
        return [x for x in self.store.list("incidents") if run_id in x.get("run_ids", [x.get("run_id")])]

    def _job_deferred(self, job):
        return job.upper() in {str(x).upper() for x in self.readiness.get("deferred_jobs", [])}

    def _readiness_detections(self, detections, contracts):
        by_key = {x.key: x for x in contracts}
        for detection in detections:
            contract = by_key[detection["contract_key"]]
            if self._job_deferred(contract.job):
                diagnostic = self.readiness.get("jobs", {}).get(contract.job, {})
                reasons = "; ".join(diagnostic.get("reasons") or ["Readiness evidence is unknown or incomplete."])
                detection.update(status="Not yet evaluated", signals=[], mix={},
                                 next_action="Source readiness deferred for " + contract.job + ": " + reasons,
                                 source_readiness_deferred=True, readiness=diagnostic)
                detection.setdefault("statistics", {}).update(scan_adjusted_p=1., corrected_q=1.)
        return detections

    def _stage_hash(self, run_id, stage, expand, reevaluate=False):
        incidents = self._incidents(run_id)
        if stage == "detect":
            return digest({"run": run_id, "daily": self.store.get("daily", run_id)["hash"]})
        if stage == "review":
            return digest({"evidence": [(x["id"], x.get("evidence", {}).get("packet_hash")) for x in incidents],
                           "model": self.settings.reviewer_model, "prompt": REVIEW_PROMPT_VERSION})
        if stage == "sample":
            return digest({"review": [(x["id"], x.get("review"), x.get("audit_pool")) for x in incidents],
                           "expand": expand})
        if stage == "audit":
            return digest({"manifests": [(x["id"], (x.get("sample_manifest") or {}).get("hash")
                           if isinstance(x.get("sample_manifest"), dict) else x.get("sample_manifest"),
                           self._audit_context(run_id, x, reevaluate=reevaluate)) for x in incidents],
                           "model": self.settings.judge_model})
        return digest({"judgments": [x for x in self.store.list("judgments") if x["incident_id"] in {
                       i["id"] for i in incidents}], "human_reviews": self.store.list("item_reviews"),
                       "manifests": [x.get("sample_manifest") for x in incidents]})

    def _audit_context(self, run_id, incident, *, create=False, reevaluate=False):
        from .ai import JUDGE_PROMPT_VERSION, JUDGE_SYSTEM
        binding_id = run_id + ":" + incident["id"]
        binding = self.store.get("audit_bindings", binding_id)
        if binding and not reevaluate:
            context = self.store.get("audit_contexts", binding["context_id"])
            return {k: v for k, v in context.items() if k != "id"}
        context = {"contract_hash": incident["contract"]["contract_hash"],
                   "rubric": self.store.approved_rubric(incident["contract_key"]),
                   "model": JUDGE_MODEL, "prompt_version": JUDGE_PROMPT_VERSION, "system_prompt": JUDGE_SYSTEM}
        if not binding and not reevaluate:
            first = next((x for x in self.store.list("judgment_versions")
                          if x.get("incident_id") == incident["id"] and x.get("prompt_version")), None)
            if first:
                original_rubric = next((x for x in self.store.list("rubrics")
                    if x.get("rubric_hash") == first.get("rubric_hash") and x.get("approved") is True), None)
                context.update(rubric=original_rubric, prompt_version=first["prompt_version"])
                if first["prompt_version"] != JUDGE_PROMPT_VERSION:
                    context["system_prompt"] = None
        if create:
            identifier = digest(context)
            self.store.put("audit_contexts", identifier, context, immutable=True, lease=self.lease)
            self.store.put("audit_bindings", binding_id, {"context_id": identifier}, lease=self.lease)
        return context

    def _packet(self, incident):
        evidence = incident.get("evidence") or {}
        packet = evidence.get("packet") or self.store.get("evidence", evidence.get("packet_hash", "_"))
        if not packet:
            raise ValueError("Incident has no immutable evidence packet.")
        window = Window.from_dict(incident["window"])
        if packet.get("window") != window.as_dict() or packet.get("contract", {}).get("key") != incident["contract_key"]:
            raise ValueError("Incident evidence identity/window mismatch.")
        value = {k: v for k, v in packet.items() if k not in {"hash", "id"}}
        if digest(value) != packet_hash(packet):
            raise ValueError("Incident evidence integrity check failed.")
        return packet, window

    def _compact_detection(self, detection):
        """Keep the declared test family once, without shrinking the test family."""
        detection = dict(detection, statistics=dict(detection.get("statistics") or {}))
        family = detection["statistics"].pop("declared_family", None)
        if family is not None:
            family_hash = digest(family)
            self.store.put("detector_families", family_hash, {"keys": family},
                           immutable=True, lease=self.lease)
            detection["statistics"].update(declared_family_hash=family_hash,
                                           family_reference=family_hash)
        return detection

    def _save_candidate_evidence(self, incident_id, contract, rows, detection, window):
        packet = self.evidence_module.build_packet(contract, rows, detection, window)
        # A browser export is only needed for an explicitly enabled image reviewer.
        # The Console renders the saved spec directly, without generating a PNG.
        return self.evidence_module.save_evidence(
            self.store, incident_id, packet, render=False)

    def _detect(self, run_id, contracts, daily, window, **kwargs):
        inputs = {contract.key: aggregate_values(contract, [], window) if self._job_deferred(contract.job)
                  else daily[contract.key] for contract in contracts}
        detections = self.detector(contracts, inputs, window, self.settings)
        if not isinstance(detections, list) or {x.get("contract_key") for x in detections} != {x.key for x in contracts}:
            raise ValueError("Detector must return one result per declared contract.")
        detections = [self._compact_detection(x) for x in
                      self._readiness_detections(detections, contracts)]
        previous = self.store.list("incidents")
        for old in previous:
            if (old.get("disposition") or {}).get("action") == "Resolved":
                old.update(status="Resolved", next_action="Resolved")
                self.store.put("incidents", old["id"], old, lease=self.lease)
            aged = any(not window.contains(x["baseline_start"]) for x in old.get("signals", []))
            if aged and old.get("status") != "Resolved":
                old.update(baseline_ageing="pre-shift baseline no longer available in the current window",
                           next_action="Preserve prior decision; no fresh healthy baseline is inferred.")
                self.store.put("incidents", old["id"], old, lease=self.lease)
        made = []
        contract_map = {x.key: x for x in contracts}
        for detection in detections:
            contract = contract_map[detection["contract_key"]]
            signals = detection.get("signals", [])
            for signal in signals:
                for field in ("baseline_start", "baseline_end", "recent_start", "recent_end"):
                    window.require(signal[field])
                if signal.get("onset"):
                    window.require(signal["onset"])
            related = [x for x in previous if x["contract_key"] == contract.key and x.get("signals")]
            known_onsets = [date.fromisoformat(s["onset"]) for x in related for s in x["signals"] if s.get("onset")]
            material = [s for s in signals if detection["status"] == "Candidate" and s.get("onset") and
                        s.get("persistent") and s.get("support_sufficient") and s.get("baseline_stable") and
                        known_onsets and all(abs((date.fromisoformat(s["onset"]) - old).days) >= 14
                                             for old in known_onsets)]
            predecessor = max(related, key=lambda x: max(
                (s.get("onset") or "" for s in x["signals"]), default="")) if material else None
            if material:
                detection = dict(detection, signals=material)
                signals = material
            direction = sorted(set(x["direction"] for x in signals))
            onset = min((x["onset"] for x in signals if x.get("onset")), default=None)
            matched = [x for x in previous if x["contract_key"] == contract.key and (
                (not signals and not x.get("signals")) or
                (signals and x.get("signals") and all(any(s.get("onset") and current.get("onset") and abs(
                    (date.fromisoformat(s["onset"]) - date.fromisoformat(current["onset"])).days) <= 7
                    for current in signals) for s in x["signals"])))]
            old = max(matched, key=lambda x: max((s.get("onset") or "" for s in x.get("signals", [])),
                                                 default=""), default=None) if not material else None
            if old:
                old["run_ids"] = sorted(set(old.get("run_ids", [old.get("run_id")]) + [run_id]))
                old["latest_detection"] = detection
                old["latest_run_window"] = window.as_dict()
                if not detection.get("source_readiness_deferred"):
                    old.pop("source_readiness_deferred", None)
                    old.pop("readiness", None)
                if detection.get("source_readiness_deferred"):
                    old.update(source_readiness_deferred=True, readiness=detection["readiness"])
                    if (old.get("disposition") or {}).get("action") not in {"Resolved", "Monitoring"}:
                        old.update(status="Not yet evaluated", next_action=detection["next_action"])
                elif detection["status"] == "Candidate" and old.get("status") == "Watch" and not old.get("baseline_ageing"):
                    # A Watch must be able to qualify on later evidence; it has no
                    # completed decision/audit to overwrite.
                    artifacts = self._save_candidate_evidence(
                        old["id"], contract, daily[contract.key], detection, window)
                    old.update(signals=signals, statistics=detection["statistics"], mix=detection.get("mix", {}),
                               mix_by_label=detection.get("mix_by_label", {}),
                               window=window.as_dict(), evidence=artifacts, status="AI review pending",
                               next_action="Watch now meets calibrated candidate gates")
                elif not signals and not old.get("signals") and (old.get("disposition") or {}).get("action") not in {
                        "Resolved", "Monitoring", "Needs human review"}:
                    # Coverage records advance with each frozen run; stale
                    # Insufficient data must not mask a newly evaluated series.
                    old.update(status=detection["status"], statistics=detection["statistics"],
                               window=window.as_dict(), next_action=detection.get("next_action"),
                               evidence={"daily_run_id": run_id, "render_status": "on demand"})
                due = old.get("review_recheck_after")
                if old.get("status") == "Monitoring" and due and window.end >= date.fromisoformat(due) and (
                        not old.get("baseline_ageing")):
                    frozen = self._frozen_detection(old, detection, daily[contract.key], window)
                    artifacts = self._save_candidate_evidence(old["id"], contract, daily[contract.key], frozen, window)
                    old.update(signals=frozen["signals"], statistics=frozen["statistics"], window=window.as_dict(),
                               evidence=artifacts, status="AI review pending", next_action="Seven-new-date recheck due")
                self.store.put("incidents", old["id"], old, lease=self.lease)
                made.append(old["id"])
                continue
            identifier = digest({"contract": contract.key, "direction": direction, "onset": onset,
                                 "regime": "tracking" if not signals else onset})
            self._renew()
            artifacts = (self._save_candidate_evidence(identifier, contract, daily[contract.key], detection, window)
                         if detection["status"] == "Candidate" else
                         {"daily_run_id": run_id, "render_status": "on demand"})
            parent = next((x["id"] for x in previous if x.get("onset") and onset and
                           x["contract"]["job"] == contract.job and
                           abs((date.fromisoformat(x["onset"]) - date.fromisoformat(onset)).days) <= 7 and
                           x["contract_key"] != contract.key), None)
            incident = {"id": identifier, "run_id": run_id, "run_ids": [run_id],
                        "contract_key": contract.key, "contract": contract.as_dict(),
                        "window": window.as_dict(), "onset": onset, **detection,
                        "severity": "severe" if any(x.get("severe") for x in signals) else "ordinary",
                        "evidence": artifacts, "review": None, "sample_manifest": None, "summary": None,
                        "disposition": None, "parent_id": parent, "baseline_ageing": None,
                        "fixture": self.fixture, "mode": "fixture" if self.fixture else "real",
                        "created_at": self.store._now().isoformat()}
            if predecessor:
                incident.update(previous_incident_id=predecessor["id"],
                                related_incident_ids=[predecessor["id"]],
                                parent_id=parent or predecessor["id"])
            if detection["status"] == "Candidate":
                incident.update(status="AI review pending", next_action="Explicit trend-review worker stage")
            self.store.put("incidents", identifier, incident, lease=self.lease)
            previous.append(incident)
            made.append(identifier)
        return {"incident_ids": made}

    @staticmethod
    def _frozen_detection(incident, detection, daily, window):
        result = json.loads(canonical_json(detection))
        for signal in result["signals"]:
            original = next((x for x in incident["signals"] if x["label"] == signal["label"] and
                             x["direction"] == signal["direction"]), None)
            if not original:
                continue
            for field in ("baseline_start", "baseline_end"):
                window.require(original[field])
                signal[field] = original[field]
            rows = [x for x in daily if signal["baseline_start"] <= str(x["day"]) <= signal["baseline_end"]]
            n = sum(x["eligible"] for x in rows)
            k = sum(x.get(signal["label"], 0) if signal["label"] in {"missing", "invalid", "allowed_empty"}
                    else x["categories"].get(signal["label"], 0) for x in rows)
            rate = k / n if n else None
            signal.update(baseline_n=n, baseline_rate=rate,
                          change_pp=(signal["recent_rate"] - rate) * 100 if rate is not None else None,
                          relative_change=(signal["recent_rate"] - rate) / rate if rate else None,
                          onset=original.get("onset"), baseline_frozen=True)
        return result

    def _review(self, run_id, contracts, daily, window, **kwargs):
        candidates = [x for x in self._incidents(run_id) if x["status"] in {
            "AI review pending", "Candidate", "Technical error"} and x.get("signals") and not x.get("baseline_ageing")
            and not self._job_deferred(x["contract"]["job"])]
        candidates.sort(key=lambda x: (-x.get("priority", 0), x["id"]))
        done = True
        reviewed = 0
        for incident in candidates:
            packet, incident_window = self._packet(incident)
            cache_key = digest({"packet": packet_hash(packet), "model": self.settings.reviewer_model,
                                "prompt": REVIEW_PROMPT_VERSION})
            saved = self.store.get("reviews", cache_key)
            if saved:
                if not self._run_slot(run_id, "review", incident):
                    done = False
                    continue
                review = saved["review"]
            elif not self.inference_allowed:
                done = False
                incident["next_action"] = "Trend review pending; no inference in this safe mode"
                self.store.put("incidents", incident["id"], incident, lease=self.lease)
                continue
            else:
                review_lease = self.store.claim("review:" + cache_key, self.owner)
                if not review_lease:
                    done = False
                    continue
                try:
                    self._renew()
                    if not self._run_slot(run_id, "review", incident, phase_lease=review_lease):
                        done = False
                        continue
                    if self.settings.image_verified and self.render and not incident["evidence"].get("png_artifact"):
                        refs = self.evidence_module.save_evidence(self.store, incident["id"], packet, render=True)
                        incident["evidence"] = refs
                        self.store.put("incidents", incident["id"], incident, lease=self.lease)
                    image = self._image(incident) if self.settings.image_verified else None
                    review = review_trend(self.client, packet, incident_window, self.settings,
                                          item_ref="review:" + cache_key, lease=review_lease, image=image)
                    self.store.put("reviews", cache_key, {"review": review, "packet_hash": packet_hash(packet)},
                                   immutable=True, lease=review_lease)
                except (AIError, ValueError, BudgetExceeded) as exc:
                    self._incident_error(incident, "review", exc)
                    done = False
                    continue
                finally:
                    try:
                        self.store.release(review_lease)
                    except LeaseLost:
                        pass
            reviewed += 1
            # Mix support must come from the measured packet AND the validated
            # reviewer. One model assertion or missing dimensions cannot clear it.
            labels = {s.get("label") for s in incident["signals"]}
            def explained(signal):
                measurements = packet.get("mix_by_label", {}).get(signal.get("label"))
                # Older packets only identify one dominant label. Do not let an
                # explanation for it dismiss other unmeasured category changes.
                if measurements is None and len(labels) == 1:
                    measurements = packet.get("mix", {})
                return review["decision"] == "Population-mix supported" and any(
                    isinstance(x, dict) and x.get("mix_supported") is True
                    for x in (measurements or {}).values())
            override = any(x.get("severe") or (x.get("persistent") and not explained(x))
                           for x in incident["signals"])
            audit = review["decision"] == "Audit" or override
            incident.update(review=dict(review, deterministic_override=override and review["decision"] != "Audit"),
                            review_run_id=run_id,
                            status="Audit queued" if audit else "Monitoring",
                            next_action="Create representative pilot manifest" if audit else
                            "Recheck after seven new complete dates")
            if not audit:
                incident.update(review_recheck_after=(incident_window.end + timedelta(days=7)).isoformat(),
                                dismissed_audit_eligible=True)
            self.store.put("incidents", incident["id"], incident, lease=self.lease)
        return {"reviewed": reviewed, "_done": done}

    def _run_slot(self, run_id, kind, incident, phase_lease=None):
        try:
            self.store.reserve_run_envelope(run_id, kind, incident["id"], lease=self.lease,
                                            phase_lease=phase_lease)
        except RunEnvelopeExceeded:
            incident.update(status="AI review pending" if kind == "review" else "Audit queued",
                            next_action="Frozen run " + kind + " envelope exhausted; queued for a NEW run",
                            run_envelope_exhausted={"run_id": run_id, "kind": kind})
            self.store.put("incidents", incident["id"], incident, lease=self.lease)
            return False
        return True

    def _image(self, incident):
        refs = incident["evidence"]
        artifact = refs.get("png_artifact")
        if not artifact:
            return None
        if artifact != refs["chart_hash"] + ".png":
            raise ValueError("Invalid saved chart artifact identity.")
        data = (self.store.root / "charts" / artifact).read_bytes()
        if hashlib.sha256(data).hexdigest() != refs.get("png_hash"):
            raise AIError("image_packet_mismatch")
        return {"bytes": data, "packet_hash": refs["packet_hash"]}

    def _sample(self, run_id, contracts, daily, window, *, expand=False, **kwargs):
        eligible = [x for x in self._incidents(run_id) if x["status"] in {
            "Audit queued", "Audit partial", "Out of budget", "Needs human review", "Technical error"}
            and (x.get("review") or x.get("audit_pool") in {"sentinel", "dismissed"}) and not x.get("baseline_ageing")
            and not self._job_deferred(x["contract"]["job"])]
        if not (self.dry_run or self.plan_only or self.read_only):
            eligible += self._reserve_incident(run_id, contracts, daily, window)
        eligible = list({x["id"]: x for x in eligible}.values())
        pilots = made = 0
        done = True
        for incident in sorted(eligible, key=lambda x: (-x.get("priority", 0), x["id"])):
            processing = [x for x in incident.get("signals", []) if x.get("processing_route") or
                          x.get("label") in {"missing", "invalid", "allowed_empty"}]
            if processing:
                incident.update(status="Needs human review",
                                next_action="Data/processing investigation first; no classification audit dispatched",
                                processing_investigation={"status": "pending human investigation", "signals": processing})
                self.store.put("incidents", incident["id"], incident, lease=self.lease)
                continue
            semantic_signals = [x for x in incident.get("signals", []) if x.get("label") in
                                Contract.from_dict(incident["contract"]).labels]
            if not semantic_signals:
                incident.update(status="Needs human review",
                                next_action="Data/processing investigation first; no supported semantic-label sampler")
                self.store.put("incidents", incident["id"], incident, lease=self.lease)
                continue
            previous = incident.get("sample_manifest")
            if isinstance(previous, str):
                previous = self.store.get("manifests", previous)
            total = self.settings.expansion_limit if expand else 40
            if previous and (previous["target"] >= total or not expand):
                continue
            if expand and incident.get("summary") and not expansion_needed(incident["summary"]):
                continue
            if self.read_only:
                done = False
                continue
            if not previous and not self._run_slot(run_id, "pilot", incident):
                done = False
                continue
            try:
                self._renew()
                manifest = build_manifest(self.repository, Contract.from_dict(incident["contract"]),
                    dict(incident, signals=semantic_signals),
                    Window.from_dict(incident["window"]), daily=daily[incident["contract_key"]],
                    seed=self.settings.seed, total=total, previous=previous,
                    incident_id=incident["id"], pool=incident.get("audit_pool", "main"))
                self.store.put("manifests", manifest["id"], manifest, immutable=True, lease=self.lease)
                incident.update(sample_manifest=manifest, status="Audit queued",
                                next_action="Blinded independent extraction; explicit inference permission")
                if not previous:
                    incident["sample_run_id"] = run_id
                self.store.put("incidents", incident["id"], incident, lease=self.lease)
                made += 1
                pilots += int(previous is None)
            except (ValueError, BudgetExceeded) as exc:
                self._incident_error(incident, "sample", exc)
                done = False
        return {"manifests_saved": made, "_done": done}

    def _reserve_incident(self, run_id, contracts, daily, window):
        if not self.inference_allowed:
            return []
        selected_before = self.store.get("checkpoints", f"{run_id}:reserve-selection")
        if selected_before:
            value = self.store.get("incidents", selected_before["incident_id"])
            return [value] if value else []
        budget = self.store.budget_summary()
        if budget["limits"]["combined_reserve"] - budget["reserve_attempts"] < 40:
            return []
        dismissed = [x for x in self._incidents(run_id) if x.get("dismissed_audit_eligible") and
                     not x.get("sample_manifest")]
        kinds = self.store.rotate("combined-reserve-kind", ["sentinel", "dismissed"])
        pool = kinds[0]
        if pool == "dismissed" and dismissed:
            incident = sorted(dismissed, key=lambda x: x["id"])[0]
            incident.update(audit_pool="dismissed", status="Audit queued")
            self.store.put("incidents", incident["id"], incident, lease=self.lease)
            self.store.put("checkpoints", f"{run_id}:reserve-selection",
                           {"run_id": run_id, "stage": "reserve", "status": "done",
                            "incident_id": incident["id"]}, lease=self.lease)
            return [incident]
        active = {x["contract_key"] for x in self.store.list("incidents") if x.get("signals") and
                  x["status"] not in {"Monitoring", "Resolved"}}
        available = [x for x in contracts if x.finite and x.contract_status == "resolved" and
                     not self._job_deferred(x.job) and
                     (self.settings.all_active_jobs or x.job in self.settings.jobs) and x.key not in active and
                     sum(r["eligible"] for r in daily[x.key]) >= self.settings.baseline_n + self.settings.recent_n]
        selected = self.store.rotate("sentinel-registry", [x.key for x in available])
        if not selected:
            if dismissed:
                incident = dismissed[0]
                incident.update(audit_pool="dismissed", status="Audit queued")
                self.store.put("incidents", incident["id"], incident, lease=self.lease)
                self.store.put("checkpoints", f"{run_id}:reserve-selection",
                               {"run_id": run_id, "stage": "reserve", "status": "done",
                                "incident_id": incident["id"]}, lease=self.lease)
                return [incident]
            return []
        contract = next(x for x in available if x.key == selected[0])
        identifier = digest({"sentinel": contract.key, "end": window.end})
        old = self.store.get("incidents", identifier)
        if old:
            return [old]
        signal = {"baseline_start": str(window.end - timedelta(days=48)),
                  "baseline_end": str(window.end - timedelta(days=21)),
                  "recent_start": str(window.end - timedelta(days=13)), "recent_end": str(window.end),
                  "onset": None, "direction": "sentinel", "label": contract.labels[0],
                  "persistent": False, "severe": False}
        rows = daily[contract.key]
        def rate(start, stop):
            selected_rows = [x for x in rows if start <= str(x["day"]) <= stop]
            n = sum(x["eligible"] for x in selected_rows)
            k = sum(x["categories"].get(signal["label"], 0) for x in selected_rows)
            return n, k / n if n else None
        baseline_n, baseline_rate = rate(signal["baseline_start"], signal["baseline_end"])
        recent_n, recent_rate = rate(signal["recent_start"], signal["recent_end"])
        change = recent_rate - baseline_rate if recent_rate is not None and baseline_rate is not None else None
        signal.update(baseline_n=baseline_n, recent_n=recent_n, baseline_rate=baseline_rate,
                      recent_rate=recent_rate, change_pp=change * 100 if change is not None else None,
                      relative_change=change / baseline_rate if baseline_rate and change is not None else None,
                      methods=["rotating sentinel; no statistical flag"])
        statistics = {"scan_adjusted_p": 1., "corrected_q": 1., "interpretation": "Unflagged sentinel, not a significance claim"}
        detection = {"status": "No detected shift", "signals": [signal], "statistics": statistics, "mix": {}}
        evidence = self._save_candidate_evidence(identifier, contract, rows, detection, window)
        incident = {"id": identifier, "contract_key": contract.key, "contract": contract.as_dict(),
                    "run_id": run_id, "run_ids": [run_id], "window": window.as_dict(), "signals": [signal],
                    "status": "Audit queued", "severity": "sentinel", "statistics": statistics, "mix": {},
                    "evidence": evidence, "review": None, "sample_manifest": None, "summary": None,
                    "disposition": None, "next_action": "Rotating sentinel accuracy audit; no shift claim",
                    "parent_id": None, "baseline_ageing": None, "audit_pool": "sentinel",
                    "fixture": self.fixture, "mode": "fixture" if self.fixture else "real"}
        self.store.put("incidents", identifier, incident, lease=self.lease)
        self.store.put("checkpoints", f"{run_id}:reserve-selection",
                       {"run_id": run_id, "stage": "reserve", "status": "done",
                        "incident_id": identifier}, lease=self.lease)
        return [incident]

    def _audit(self, run_id, contracts, daily, window, *, reevaluate=False, **kwargs):
        done = True
        saved = skipped = 0
        for incident in self._incidents(run_id):
            if self._job_deferred(incident["contract"]["job"]):
                continue
            action = (incident.get("disposition") or {}).get("action")
            if incident.get("baseline_ageing") or incident.get("status") == "Resolved" or action == "Resolved" or (
                    action == "Monitoring" and not reevaluate and not kwargs.get("expand")):
                continue
            manifest = incident.get("sample_manifest")
            if not manifest:
                continue
            if isinstance(manifest, str):
                manifest = self.store.get("manifests", manifest)
            contract = Contract.from_dict(incident["contract"])
            context = self._audit_context(run_id, incident, create=True, reevaluate=reevaluate)
            rubric = context["rubric"]
            incident_window = Window.from_dict(manifest["window"])
            if manifest["contract_hash"] != contract.contract_hash:
                raise ValueError("Manifest contract changed after sampling.")
            for item in manifest["items"]:
                identifier = incident["id"] + ":" + item["item_id"]
                existing = self.store.get("judgments", identifier)
                rubric_hash = rubric.get("rubric_hash") if rubric else None
                if existing and existing.get("rubric_hash") == rubric_hash and (
                        existing.get("contract_hash") == contract.contract_hash and
                        existing.get("prompt_version") == context["prompt_version"] and
                        existing.get("identity", {}).get("status") == "resolved"):
                    skipped += 1
                    continue
                if not self.inference_allowed:
                    done = False
                    continue
                self._renew()
                item_lease = self.store.claim("item:" + identifier, self.owner)
                if not item_lease:
                    done = False
                    continue
                extraction_lease = None
                try:
                    resolved = self.repository.transcript(item, incident_window)
                    identity_status = validate_identity(item, resolved, incident_window)
                    self.store.record_audit_item(identifier, item["production_result_id"],
                        resolved.get("transcript_hash") if identity_status == "resolved" else None, lease=item_lease)
                    if identity_status != "resolved":
                        judgment = {"extracted_value": None, "status": "Insufficient evidence",
                                    "comparison": "unresolved", "quotes": [],
                                    "rationale": "Transcript identity/completeness unresolved: " + identity_status}
                        identity = {key: resolved.get(key) for key in (
                            "transcript_analysis_id", "analysis_content_id", "match_method")}
                        identity["status"] = identity_status
                    else:
                        key = extraction_key(contract, resolved["transcript_hash"], rubric,
                                             prompt_version=context["prompt_version"])
                        extraction = self.store.get("extractions", key)
                        cache_hit = extraction is not None
                        if extraction is None:
                            if context["system_prompt"] is None:
                                raise AIError("historical_judge_prompt_unrecoverable")
                            extraction_lease = self.store.claim("extraction:" + key, self.owner)
                            if extraction_lease is None:
                                done = False
                                continue
                            extraction = judge_extract(self.client, contract, resolved["transcript"],
                                resolved["transcript_hash"], self.settings, item_ref="judge:" + key,
                                rubric=rubric, lease=extraction_lease, pool=item.get("pool", "main"),
                                prompt_version=context["prompt_version"], system_prompt=context["system_prompt"])
                            self.store.put("extractions", key, dict(extraction, transcript_hash=resolved["transcript_hash"],
                                contract_hash=contract.contract_hash, rubric_hash=rubric.get("rubric_hash") if rubric else None),
                                immutable=True, lease=extraction_lease)
                        judgment = {**extraction, **compare_output(contract, item["stored_output"], extraction,
                            present=item.get("stored_output_present", True))}
                        judgment.update(cache_hit=cache_hit, extraction_cache_key=key)
                        judgment.pop("id", None)
                        identity = {key: resolved.get(key) for key in (
                            "transcript_analysis_id", "analysis_content_id", "match_method", "transcript_hash")}
                        identity["status"] = "resolved"
                    judgment.update(incident_id=incident["id"], item_id=item["item_id"],
                        stored_output=item["stored_output"], contract_hash=contract.contract_hash,
                        rubric_hash=rubric.get("rubric_hash") if rubric else None, historical_scope=contract.historical_scope,
                        identity={**identity, **{key: item[key] for key in (
                            "production_result_id", "connection_id", "job", "interaction_date")}},
                        stratum=item["stratum"], period=item["period"], representative=True)
                    self.store.assert_lease(self.lease)
                    self.store.put("judgment_versions", digest(judgment), judgment,
                                   immutable=True, lease=item_lease)
                    self.store.put("judgments", identifier, judgment, lease=item_lease)
                    self.store.put("checkpoints", "judgment:" + identifier,
                                   {"run_id": run_id, "stage": "item", "status": "done"}, lease=item_lease)
                    saved += 1
                except LeaseLost:
                    raise
                except (AIError, ValueError, BudgetExceeded) as exc:
                    self._incident_error(incident, "audit", exc, item_id=item["item_id"])
                    done = False
                    if isinstance(exc, BudgetExceeded):
                        break
                finally:
                    for lease in (extraction_lease, item_lease):
                        if lease:
                            try:
                                self.store.release(lease)
                            except LeaseLost:
                                pass
            judgments = [x for x in self.store.list("judgments") if x["incident_id"] == incident["id"]]
            incident.update(status="Audit complete" if len(judgments) == len(manifest["items"]) else "Audit partial",
                            next_action="Summarize saved independent judgments" if len(judgments) == len(
                                manifest["items"]) else "Resume missing/failed items within shared budgets")
            self.store.put("incidents", incident["id"], incident, lease=self.lease)
        return {"saved": saved, "successful_items_skipped": skipped, "_done": done}

    def _summarize(self, run_id, contracts, daily, window, **kwargs):
        count = 0
        judgments = self.store.list("judgments")
        human_reviews = self.store.list("item_reviews")
        for incident in self._incidents(run_id):
            if self._job_deferred(incident["contract"]["job"]):
                continue
            action = (incident.get("disposition") or {}).get("action")
            if incident.get("status") == "Resolved" or action == "Resolved":
                if action == "Resolved":
                    incident.update(status="Resolved", next_action="Resolved")
                    self.store.put("incidents", incident["id"], incident, lease=self.lease)
                continue
            manifest = incident.get("sample_manifest")
            if not manifest:
                continue
            if isinstance(manifest, str):
                manifest = self.store.get("manifests", manifest)
            values = [x for x in judgments if x["incident_id"] == incident["id"]]
            result = summarize(manifest, values, seed=self.settings.seed,
                               human_reviews=[x for x in human_reviews if x["incident_id"] == incident["id"]])
            human_action = (incident.get("disposition") or {}).get("action")
            incident.update(summary=result, status=human_action if human_action in {"Monitoring", "Resolved"} else
                            ("Needs human review" if result["complete"] else "Audit partial"),
                            next_action="Review 20 stratified examples and record a human disposition"
                            if result["complete"] else "Partial audit; resume missing items")
            if human_action in {"Monitoring", "Resolved"}:
                incident["next_action"] = human_action
            self.store.put("incidents", incident["id"], incident, lease=self.lease)
            self.store.put("incident_versions", digest({"incident": incident["id"], "summary": result}),
                           {"incident_id": incident["id"], "summary": result}, immutable=True, lease=self.lease)
            count += 1
        return {"summarized": count}

    def _incident_error(self, incident, stage, exc, item_id=None):
        error = {"incident_id": incident["id"], "stage": stage, "item_id": item_id,
                 **_safe_error(exc), "created_at": self.store._now().isoformat()}
        self.store.put("errors", uuid.uuid4().hex, error, lease=self.lease)
        incident.update(error=error, status="Out of budget" if isinstance(exc, BudgetExceeded) else "Technical error",
                        next_action="Explicit local retry/resume; successful checkpoints are preserved")
        self.store.put("incidents", incident["id"], incident, lease=self.lease)

    def replay(self, end_dates):
        """Persist the core's chronological recorded replay, without duplicating its calibration logic."""
        if not self.dry_run and self.inference_allowed:
            raise ValueError("Replay does not perform inference.")
        from .replay import replay
        ordered = sorted(set(date.fromisoformat(str(x)) for x in end_dates))
        if not ordered:
            raise ValueError("Replay requires at least one evaluation date.")
        proposed = Window.ending(end=ordered[-1])
        readiness = self.repository.readiness(proposed)
        self.readiness = readiness
        window = Window.ending(end=proposed.end, ready_through=readiness["ready_through"],
                               watermark=readiness.get("watermark"))
        for end in ordered:
            window.require(end)
        contracts = self.repository.registry()
        daily = self.repository.aggregate(contracts, window)
        self._validate_daily(contracts, daily, window)
        inputs = {x.key: aggregate_values(x, [], window) if self._job_deferred(x.job) else daily[x.key]
                  for x in contracts}
        result = replay(contracts, inputs, window, self.settings, evaluation_dates=ordered)
        for evaluation in result.get("evaluations", []):
            self._readiness_detections(evaluation["detections"], contracts)
        result["readiness"] = readiness
        result.update(window=window.as_dict(), fixture=self.fixture, mode="fixture" if self.fixture else "recorded",
                      id=uuid.uuid4().hex)
        self.store.put("replays", result["id"], result, immutable=True)
        return result
