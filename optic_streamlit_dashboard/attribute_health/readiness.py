"""Bounded, fail-safe source-arrival policy independent of output correctness."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from statistics import median

from .config import CENTRAL, validate_readiness_settings

READINESS_DEFAULTS = {
    "source_lag_days": 1,
    "readiness_lookback_days": 7,
    "readiness_baseline_weeks": 4,
    "readiness_min_baseline_days": 3,
    "readiness_min_baseline_volume": 100,
    "readiness_volume_fraction": .8,
    "readiness_id_fraction": .99,
    "readiness_required_present_fraction": .8,
    "readiness_attribute_ready_fraction": .9,
    "readiness_attribute_baseline_days": 14,
}


def _today():
    return datetime.now(CENTRAL).date()


def _settings(settings):
    values = {name: getattr(settings, name, default) for name, default in READINESS_DEFAULTS.items()}
    return validate_readiness_settings(values)


def _index(rows, window, *, attributes=False):
    indexed = {}
    if not isinstance(rows, list):
        return indexed
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("job"), str) or "day" not in row:
            continue
        window.require(row["day"])
        day = date.fromisoformat(str(row["day"])[:10])
        job = row["job"].strip().upper()
        key = (job, str(row.get("contract_key", "")), day) if attributes else (job, day)
        indexed[key] = None if key in indexed else row
    return indexed


def _count(row, name, denominator=None):
    value = row.get(name) if isinstance(row, dict) else None
    return value if type(value) is int and value >= 0 and (
        denominator is None or value <= denominator) else None


def _watermark(row, day):
    raw = row.get("watermark") if isinstance(row, dict) else None
    if not isinstance(raw, (str, datetime)) or not str(raw).strip():
        return {"status": "unknown", "value": None, "reason": "Source watermark is missing."}
    try:
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return {"status": "unknown", "value": str(raw), "reason": "Source watermark is not a verified timestamp."}
    local_day = parsed.astimezone(CENTRAL).date() if parsed.tzinfo else parsed.date()
    if local_day < day:
        return {"status": "unknown", "value": str(raw), "reason": "Watermark predates the source interaction day."}
    return {"status": "ready", "value": str(raw)}


def _identity(row, settings):
    n = _count(row, "n")
    ids = _count(row, "nonnull_ids", n)
    conflicts = _count(row, "identity_conflicts", n)
    if not n or ids is None or conflicts is None:
        return {"status": "unknown", "n": n, "nonnull_ids": ids, "identity_conflicts": conflicts,
                "reason": "Usable source volume and explicit ID/conflict evidence are required."}
    id_rate, conflict_rate = ids / n, conflicts / n
    ready = id_rate >= settings["readiness_id_fraction"] and (
        conflict_rate <= 1 - settings["readiness_id_fraction"] + 1e-12)
    return {"status": "ready" if ready else "incomplete", "n": n, "nonnull_ids": ids,
            "identity_conflicts": conflicts, "nonnull_id_fraction": id_rate,
            "conflict_fraction": conflict_rate, "minimum_id_fraction": settings["readiness_id_fraction"],
            **({} if ready else {"reason": "Source identity coverage/conflicts exceed the configured tolerance."})}


def _volume(job, day, source, window, settings):
    row = source.get((job, day))
    n = _count(row, "n")
    baseline = []
    for week in range(1, settings["readiness_baseline_weeks"] + 1):
        previous = day - timedelta(days=7 * week)
        if not window.contains(previous):
            continue
        prior = source.get((job, previous))
        count = _count(prior, "n")
        if count and _identity(prior, settings)["status"] == "ready" and (
                _watermark(prior, previous)["status"] == "ready"):
            baseline.append({"day": previous.isoformat(), "n": count})
    total = sum(x["n"] for x in baseline)
    supported = len(baseline) >= settings["readiness_min_baseline_days"] and (
        total >= settings["readiness_min_baseline_volume"])
    baseline_median = median(x["n"] for x in baseline) if supported else None
    result = {"n": n, "baseline_days": baseline, "baseline_volume": total,
              "baseline_median": baseline_median, "minimum_fraction": settings["readiness_volume_fraction"]}
    if n is None or not supported:
        return dict(result, status="unknown", reason="Missing/ambiguous volume or unsupported prior same-weekday baseline.")
    ratio = n / baseline_median
    return dict(result, status="ready" if ratio >= settings["readiness_volume_fraction"] else "incomplete",
                fraction=ratio, **({} if ratio >= settings["readiness_volume_fraction"] else {
                    "reason": "Job volume is thin relative to its own supported same-weekday baseline."}))


def _attributes(job, day, row, attributes, contracts, unmapped, window, settings):
    applicable = [x for x in contracts if x.job.upper() == job]
    exempt = [x.key for x in applicable if x.allow_empty and x.contract_status == "resolved"]
    required = [x for x in applicable if x.key not in exempt]
    if not applicable:
        return {"status": "unknown", "required_contracts": 0, "supported_contracts": 0,
                "exempt_allowed_empty": [], "contracts": [], "reason": "Active job membership evidence is unavailable."}
    if not required:
        return {"status": "not applicable", "required_contracts": 0, "supported_contracts": 0,
                "exempt_allowed_empty": exempt, "contracts": [],
                "reason": "Every active contract has a resolved legitimate allowed-empty rule."}
    details = []
    for contract in required:
        item = {"contract_key": contract.key, "status": "unknown", "supported": False}
        if contract.key in unmapped or contract.contract_status != "resolved":
            item["reason"] = "Unmapped or ambiguous current contract; presence readiness is unknown."
            details.append(item)
            continue
        prior = []
        for offset in range(1, settings["readiness_attribute_baseline_days"] + 1):
            previous = day - timedelta(days=offset)
            if not window.contains(previous):
                continue
            observation = attributes.get((job, contract.key, previous))
            n = _count(observation, "n")
            present = _count(observation, "present", n)
            if n and present is not None:
                prior.append({"day": previous.isoformat(), "n": n, "present": present,
                              "rate": present / n})
        total = sum(x["n"] for x in prior)
        present = sum(x["present"] for x in prior)
        high_days = sum(x["rate"] >= settings["readiness_required_present_fraction"] for x in prior)
        stable_fraction = high_days / len(prior) if prior else None
        supported = len(prior) >= settings["readiness_min_baseline_days"] and (
            total >= settings["readiness_min_baseline_volume"]) and (
            present / total >= settings["readiness_required_present_fraction"] if total else False) and (
            stable_fraction >= settings["readiness_attribute_ready_fraction"] if prior else False)
        item.update(baseline_days=len(prior), baseline_volume=total,
                    baseline_present_fraction=present / total if total else None,
                    baseline_high_day_fraction=stable_fraction, supported=supported)
        if not supported:
            item["reason"] = "Prior in-window presence is absent, unsupported or unstable; retrospective activation is not inferred."
            details.append(item)
            continue
        current = attributes.get((job, contract.key, day))
        n = _count(current, "n")
        count = _count(current, "present", n)
        if not n or count is None or n != _count(row, "n"):
            item["reason"] = "Current attribute presence/denominator evidence is missing or inconsistent."
            details.append(item)
            continue
        rate = count / n
        item.update(status="ready" if rate >= settings["readiness_required_present_fraction"] else "incomplete",
                    n=n, present=count, present_fraction=rate)
        if item["status"] == "incomplete":
            item["reason"] = "Supported required attribute is still partly filled."
        details.append(item)
    supported = [x for x in details if x["supported"]]
    ready = sum(x["status"] == "ready" for x in supported)
    incomplete = sum(x["status"] == "incomplete" for x in supported)
    unknown = sum(x["status"] == "unknown" for x in supported)
    result = {"required_contracts": len(required), "supported_contracts": len(supported),
              "ready_supported_contracts": ready, "incomplete_supported_contracts": incomplete,
              "unknown_supported_contracts": unknown, "exempt_allowed_empty": exempt,
              "contracts": details, "minimum_ready_fraction": settings["readiness_attribute_ready_fraction"],
              "ready_fraction": ready / len(supported) if supported else None,
              "unknown_contracts": [x["contract_key"] for x in details if x["status"] == "unknown"]}
    if not supported or unknown:
        return dict(result, status="unknown", reason="Usable readiness evidence is missing for required attributes.")
    if ready / len(supported) < settings["readiness_attribute_ready_fraction"]:
        return dict(result, status="incomplete", reason="Widespread required-attribute partial fills exceed the configured tolerance.")
    return dict(result, status="ready")


def evaluate_job_readiness(window, settings, jobs, evidence, contracts):
    """Assess source arrival by enabled job; absence of evidence is never completeness."""
    values = _settings(settings)
    evidence = evidence if isinstance(evidence, dict) else {}
    source = _index(evidence.get("days"), window)
    attributes = _index(evidence.get("attributes"), window, attributes=True)
    unmapped = set(evidence.get("unmapped_contracts") or [])
    enabled = sorted({str(x).strip().upper() for x in jobs if str(x).strip()})
    end = min(window.end, _today() - timedelta(days=values["source_lag_days"]))
    if not window.contains(end):
        raise ValueError("Configured source lag leaves no proposed in-window readiness date.")
    dates = [end - timedelta(days=i) for i in range(values["readiness_lookback_days"])
             if window.contains(end - timedelta(days=i))]

    def check(job, day):
        row = source.get((job, day))
        checks = {"volume": _volume(job, day, source, window, values),
                  "identity": _identity(row, values), "watermark": _watermark(row, day),
                  "required_attributes": _attributes(job, day, row, attributes, contracts,
                                                      unmapped, window, values)}
        states = [x["status"] for x in checks.values()]
        state = "incomplete" if "incomplete" in states else "unknown" if "unknown" in states else "ready"
        return {"day": day.isoformat(), "status": state, "checks": checks,
                "reasons": [x["reason"] for x in checks.values() if x.get("reason") and
                            x["status"] not in {"ready", "not applicable"}]}

    diagnostics = {}
    for job in enabled:
        scanned = [check(job, day) for day in dates]
        proposed = scanned[0]
        latest = next((x for x in scanned if x["status"] == "ready"), None)
        # Unknown proposed-end evidence is deferred, not silently replaced by an arrival assumption.
        deferred = proposed["status"] == "unknown" or latest is None
        state = proposed["status"] if deferred else "ready" if proposed["status"] == "ready" else "rolled back"
        diagnostics[job] = {
            "status": state, "state": state, "proposed_end": end.isoformat(),
            "proposed_end_status": proposed["status"], "latest_ready_through": latest["day"] if latest else None,
            "checks": proposed["checks"], "reasons": proposed["reasons"], "scanned_dates": scanned,
            "deferred": deferred,
        }
    retained = [x for x in diagnostics.values() if not x["deferred"]]
    ready = min(x["latest_ready_through"] for x in retained) if retained else end.isoformat()
    # A shared end must itself be proven for every retained job, not only its later latest-ready day.
    for job, item in diagnostics.items():
        if not item["deferred"]:
            aligned = check(job, date.fromisoformat(ready))
            item["effective_end_checks"] = aligned
            if aligned["status"] != "ready":
                item.update(deferred=True, status=aligned["status"], state=aligned["status"])
                item["reasons"] = item["reasons"] + ["The shared effective end lacks proven job readiness."]
    retained = [x for x in diagnostics.values() if not x["deferred"]]
    watermark_values = [x["effective_end_checks"]["checks"]["watermark"]["value"] for x in retained]
    return {"ready_through": ready, "watermark": min(watermark_values) if watermark_values else None,
            "jobs": diagnostics, "deferred_jobs": [job for job, x in diagnostics.items() if x["deferred"]],
            "readiness_method": "bounded per-job weekday volume, identities, watermark and supported required-presence gate",
            "automatic_completeness_claim": False, "probe_window": window.as_dict(),
            "settings": values, "proposed_end": end.isoformat()}
