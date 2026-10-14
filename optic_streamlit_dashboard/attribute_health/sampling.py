"""Deterministic saved stratified samples and uncertainty-aware independent comparisons."""
from __future__ import annotations

import math
import random
from collections import Counter, defaultdict
from datetime import date

from .config import Window
from .contracts import canonical_json, digest, normalize

PILOT = (15, 15, 5, 5)
STRATA = ("pre_flagged", "post_flagged", "pre_complement", "post_complement")


def _periods(detection, window):
    signals = detection.get("signals", [])
    if not signals:
        raise ValueError("Sampling needs a saved detection regime.")
    signal = signals[0]
    periods = {}
    for period, prefix in (("pre", "baseline"), ("post", "recent")):
        start, end = (date.fromisoformat(signal[prefix + "_start"]),
                      date.fromisoformat(signal[prefix + "_end"]))
        if start > end:
            raise ValueError("Invalid sample interval.")
        window.require(start)
        window.require(end)
        periods[period] = (start, end)
    if periods["pre"][1] >= periods["post"][0]:
        raise ValueError("Pre/post sample periods must not overlap.")
    return signal, periods


def _target_sizes(total):
    if not 40 <= total <= 120:
        raise ValueError("Representative audit is bounded to a 40-item pilot through 120 items.")
    targets = [int(total * weight / 40) for weight in PILOT]
    for index in range(total - sum(targets)):
        targets[index % 4] += 1
    return targets


def _brand_population(daily, start, end, label, complement):
    result = Counter()
    for row in daily:
        day = date.fromisoformat(str(row["day"])[:10])
        if start <= day <= end:
            for name, segment in row.get("segments", {}).get("brand", {}).items():
                n = int(segment.get("n", 0))
                flagged = int(segment.get("counts", {}).get(label, 0))
                result[name] += n - flagged if complement else flagged
    return {k: v for k, v in result.items() if v > 0}


def _allocation(populations, count):
    total = sum(populations.values())
    if not total:
        return {}
    exact = {key: count * n / total for key, n in populations.items()}
    assigned = {key: min(populations[key], int(value)) for key, value in exact.items()}
    remaining = min(count, total) - sum(assigned.values())
    ordered = sorted(populations, key=lambda key: (-(exact[key] - int(exact[key])), key))
    while remaining > 0:
        progressed = False
        for key in ordered:
            if assigned[key] < populations[key] and remaining:
                assigned[key] += 1
                remaining -= 1
                progressed = True
        if not progressed:
            break
    return assigned


def build_manifest(repository, contract, detection, window, *, daily=(), seed=1, total=40,
                   previous=None, incident_id=None, pool="main"):
    signal, periods = _periods(detection, window)
    label = signal.get("label")
    if not isinstance(label, str) or not label:
        raise ValueError("A concrete flagged category is required for representative sampling.")
    targets = _target_sizes(total)
    if pool not in {"main", "sentinel", "dismissed"}:
        raise ValueError("Unknown sample budget pool.")
    items = [dict(x) for x in (previous or {}).get("items", [])]
    if previous:
        if previous.get("contract_hash") != contract.contract_hash or previous.get("window") != window.as_dict():
            raise ValueError("Incremental sampling cannot change its frozen contract/window.")
        if previous.get("label") != label or previous.get("seed") != seed:
            raise ValueError("Incremental sampling cannot change label/seed.")
    selected = {x["production_result_id"] for x in items}
    manifest_strata = []
    for index, (name, target) in enumerate(zip(STRATA, targets)):
        period = "pre" if name.startswith("pre") else "post"
        complement = name.endswith("complement")
        start, end = periods[period]
        stratum_seed = int(digest({"seed": seed, "contract": contract.key, "stratum": name})[:8], 16)
        existing = [x for x in items if x["stratum"] == name]
        previous_stratum = next((x for x in (previous or {}).get("strata", []) if x["name"] == name), None)
        if len(existing) >= target and previous_stratum:
            manifest_strata.append(dict(previous_stratum, target=target, selected=len(existing)))
            continue
        response = repository.candidates(contract, window, start=start, end=end, label=label,
                                          complement=complement, limit=120, seed=stratum_seed)
        if not isinstance(response, dict) or not isinstance(response.get("rows"), list) or (
                type(response.get("population_count")) is not int or response["population_count"] < 0):
            raise ValueError("Invalid candidate repository response.")
        if not str(response.get("selection_method", "")).startswith("seeded uniform"):
            raise ValueError("Representative audits require verified seeded-uniform candidate selection.")
        population = response["population_count"]
        if previous_stratum and previous_stratum["population_count"] != population:
            raise ValueError("Candidate population changed; preserve pilot and create a new evidence regime.")
        candidates = {}
        for raw in response["rows"]:
            required = {"production_result_id", "connection_id", "job", "interaction_date", "stored_output"}
            if not isinstance(raw, dict) or not required <= set(raw):
                raise ValueError("Candidate identity/output fields missing.")
            window.require(raw["interaction_date"])
            day = date.fromisoformat(str(raw["interaction_date"])[:10])
            if not start <= day <= end or raw["job"].upper() != contract.job.upper():
                raise ValueError("Candidate is outside its stratum/job.")
            if "transcript" in raw or "content" in raw:
                raise ValueError("Candidate queries must be metadata-only.")
            normalized = normalize(contract, raw["stored_output"], raw.get("stored_output_present", True))
            has_label = label in normalized["labels"]
            if has_label == complement:
                raise ValueError("Candidate does not match its saved label stratum.")
            identifier = str(raw["production_result_id"])
            if identifier in candidates:
                if canonical_json(candidates[identifier]) != canonical_json(raw):
                    raise ValueError("Conflicting candidate production-result identity.")
                continue
            candidates[identifier] = raw
        if len(candidates) > population or len(candidates) > 120:
            raise ValueError("Candidate count exceeds its bounded population.")
        ordered = sorted(candidates.values(), key=lambda x: digest(
            {"seed": stratum_seed, "id": x["production_result_id"]}))
        populations = _brand_population(daily, start, end, label, complement)
        segment_weighted = bool(populations) and sum(populations.values()) == population
        if segment_weighted:
            allocation = _allocation(populations, target)
            for brand in sorted(allocation):
                needed = max(0, allocation[brand] - sum(x.get("brand") == brand for x in existing))
                chosen = [x for x in ordered if x.get("brand") == brand and
                          str(x["production_result_id"]) not in selected][:needed]
                for raw in chosen:
                    item = _item(raw, contract, name, period, populations[brand], brand, pool)
                    items.append(item)
                    selected.add(str(raw["production_result_id"]))
        else:
            needed = max(0, target - len(existing))
            for raw in [x for x in ordered if str(x["production_result_id"]) not in selected][:needed]:
                items.append(_item(raw, contract, name, period, population, None, pool))
                selected.add(str(raw["production_result_id"]))
        current = [x for x in items if x["stratum"] == name]
        groups = Counter(x["sampling_cell"] for x in current)
        for item in current:
            n = groups[item["sampling_cell"]]
            item.update(selection_probability=n / item["population_count"] if item["population_count"] else None,
                        weight=item["population_count"] / n if n else None,
                        sampling_n=n)
        manifest_strata.append({
            "name": name, "period": period, "start": start.isoformat(), "end": end.isoformat(),
            "label": label, "complement": complement, "population_count": population,
            "target": target, "selected": len(current), "shortfall": max(0, target - len(current)),
            "selection_method": "seeded uniform within brand" if segment_weighted else "seeded uniform",
            "segment_populations": populations if segment_weighted else {},
            "candidate_pool_truncated": bool(response.get("truncated")),
            "brand_weighting": "verified daily population" if segment_weighted else "unknown; stratum weights only",
        })
    manifest = {"incident_id": incident_id, "contract_key": contract.key,
                "contract_hash": contract.contract_hash, "window": window.as_dict(), "label": label,
                "seed": seed, "target": total, "items": items, "strata": manifest_strata,
                "pool": pool, "fixture": bool(getattr(repository, "demo", False)),
                "previous_manifest_id": previous.get("id") if previous else None,
                "representative": True, "diagnostic_items": []}
    manifest["hash"] = digest(manifest)
    manifest["id"] = manifest["hash"]
    return manifest


def _item(raw, contract, stratum, period, population, brand, pool):
    identifier = str(raw["production_result_id"])
    result = {key: raw.get(key) for key in (
        "production_result_id", "connection_id", "job", "interaction_date", "stored_output",
        "brand", "language", "site", "stored_output_present", "agent",
        "attribute_name", "attribute_id", "contract_hash") if key in raw}
    result.update(production_result_id=identifier, contract_key=contract.key, stratum=stratum,
                  period=period, population_count=population, pool=pool,
                  sampling_cell=stratum + (":" + brand if brand is not None else ""),
                  item_id=digest({"production_result_id": identifier, "contract_key": contract.key}),
                  identity_status="pending", representative=True)
    return result


def _wilson(successes, n):
    if n <= 0:
        return [None, None]
    z = 1.959963984540054
    p = successes / n
    denominator = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denominator
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denominator
    return [max(0, center - half), min(1, center + half)]


def summarize(manifest, judgments, *, seed=1, simulations=1000, human_reviews=()):
    """Post-stratified estimates with whole-call resampling and conservative boundary intervals."""
    by_item = {x["item_id"]: x for x in judgments}
    cells = defaultdict(list)
    periods = defaultdict(list)
    results = []
    for item in manifest["items"]:
        judgment = by_item.get(item["item_id"])
        comparison = judgment.get("comparison", "unresolved") if judgment else "unresolved"
        if isinstance(comparison, dict):
            comparison = comparison.get("comparison", "uncertain")
        row = dict(item, comparison=comparison, status=judgment.get("status") if judgment else "pending",
                   extracted_value=judgment.get("extracted_value") if judgment else None)
        cells[item["sampling_cell"]].append(row)
        periods[item["period"]].append(row)
        results.append(row)
    scored_outcomes = {"agreement", "disagreement", "production_missing", "production_invalid"}
    disagrees = {"disagreement", "production_missing", "production_invalid"}
    overall = {}
    cell_details = []
    for name, rows in sorted(cells.items()):
        n = len(rows)
        scored = [x for x in rows if x["comparison"] in scored_outcomes]
        k = sum(x["comparison"] in disagrees for x in scored)
        cell_details.append({"name": name, "sample_n": n, "population_n": rows[0]["population_count"],
                             "scored_n": len(scored), "disagreements": k,
                             "uncertain_n": sum(x["comparison"] == "uncertain" for x in rows),
                             "unresolved_n": sum(x["comparison"] == "unresolved" for x in rows),
                             "rate": k / len(scored) if scored else None,
                             "wilson_interval": _wilson(k, len(scored))})
    rng = random.Random(seed)
    for period, rows in sorted(periods.items()):
        relevant = [x for x in cell_details if x["name"].startswith(period + "_")]
        represented_population = sum(x["population_n"] for x in relevant)
        declared_population = sum(x["population_count"] for x in manifest["strata"] if x["period"] == period)
        weighted_total = sum(x.get("weight", 0) for x in rows)
        scored_rows = [x for x in rows if x["comparison"] in scored_outcomes]
        weighted_scored = sum(x.get("weight", 0) for x in scored_rows)
        weighted_disagreement = sum(x.get("weight", 0) for x in scored_rows if x["comparison"] in disagrees)
        point = weighted_disagreement / weighted_scored if weighted_scored else None
        lower = sum(x.get("weight", 0) for x in rows if x["comparison"] in disagrees) / weighted_total if weighted_total else None
        unknown_weight = sum(x.get("weight", 0) for x in rows if x["comparison"] not in scored_outcomes)
        upper = (weighted_disagreement + unknown_weight) / weighted_total if weighted_total else None
        confusion = Counter()
        for row in scored_rows:
            confusion[canonical_json([row["stored_output"], row["extracted_value"]])] += row.get("weight", 0)
        draws = []
        period_cells = {key: value for key, value in cells.items() if key.startswith(period + "_")}
        if point is not None:
            for _ in range(simulations):
                denominator = numerator = 0.
                for cell_rows in period_cells.values():
                    # Call is the cluster. Each manifest admits only one item per production call.
                    for _ in range(len(cell_rows)):
                        row = rng.choice(cell_rows)
                        if row["comparison"] in scored_outcomes:
                            denominator += row["weight"]
                            numerator += row["weight"] * (row["comparison"] in disagrees)
                if denominator:
                    draws.append(numerator / denominator)
        draws.sort()
        bootstrap = [draws[int(.025 * (len(draws) - 1))], draws[int(.975 * (len(draws) - 1))]] if draws else [None, None]
        weighted_wilson_low = weighted_wilson_high = 0.
        if weighted_scored:
            for cell in relevant:
                if cell["scored_n"]:
                    scored_weight = cell["population_n"] * cell["scored_n"] / cell["sample_n"]
                    weighted_wilson_low += scored_weight * cell["wilson_interval"][0] / weighted_scored
                    weighted_wilson_high += scored_weight * cell["wilson_interval"][1] / weighted_scored
        interval = [min(bootstrap[0], weighted_wilson_low), max(bootstrap[1], weighted_wilson_high)] if draws else [None, None]
        overall[period] = {
            "sample_n": len(rows), "scored_n": len(scored_rows), "weighted_scored": weighted_scored,
            "disagreement_rate": point, "disagreement_interval": interval,
            "uncertainty_bounds_all_selected": [lower, upper],
            "ambiguity_n": sum(x["status"] == "Ambiguous contract" for x in rows),
            "uncertain_n": sum(x["comparison"] == "uncertain" for x in rows),
            "unresolved_n": sum(x["comparison"] == "unresolved" for x in rows),
            "confusion_destinations": [{"stored": json_pair[0], "extracted": json_pair[1],
                                       "weighted_count": count}
                                      for pair, count in sorted(confusion.items())
                                      for json_pair in [__import__("json").loads(pair)]],
            "represented_population": represented_population, "declared_population": declared_population,
            "population_coverage": represented_population / declared_population if declared_population else None,
        }
    latest_human = {}
    for review in human_reviews:
        latest_human[review["item_id"]] = review
    judged_human = [x for x in latest_human.values() if x["decision"] != "uncertain"]
    return {"manifest_id": manifest["id"], "periods": overall, "category_specific": cell_details,
            "call_attribute_items": len(manifest["items"]),
            "unique_production_results": len({x["production_result_id"] for x in manifest["items"]}),
            "unique_source_calls": len({(x["production_result_id"], x["job"], x["connection_id"])
                                       for x in manifest["items"]}),
            "saved_judgment_items": len({x["item_id"] for x in judgments}),
            "cache_hits": sum(bool(x.get("cache_hit")) for x in judgments),
            "unique_content_extractions": len({x.get("identity", {}).get("transcript_hash")
                                               for x in judgments if x.get("identity", {}).get("transcript_hash")}),
            "method": "post-stratified whole-call bootstrap; boundary envelope of weighted cell Wilson intervals",
            "simulations": simulations, "seed": seed,
            "limitations": "Conditional on resolved/scorable items; unresolved bounds and coverage are separate. "
                            "AI disagreement is not human-confirmed error; no causal model regression claim.",
            "human_review": {"reviewed_n": len(latest_human), "scored_n": len(judged_human),
                             "accept_n": sum(x["decision"] == "accept" for x in judged_human),
                             "ground_truth_calibrated": False},
            "human_review_items": human_selection(results, seed=seed),
            "complete": len(judgments) >= len(manifest["items"]),
            "shortfalls": sum(x["shortfall"] for x in manifest["strata"])}


def human_selection(rows, *, count=20, seed=1):
    groups = defaultdict(list)
    for row in rows:
        outcome = row["comparison"]
        group = "agreement" if outcome == "agreement" else (
            "disagreement" if outcome in {"disagreement", "production_missing", "production_invalid"} else "uncertain")
        groups[(row["period"], group)].append(row)
    for group in groups.values():
        group.sort(key=lambda row: digest({"seed": seed, "human": row["item_id"]}))
    selected = []
    while len(selected) < min(count, len(rows)):
        progressed = False
        for key in sorted(groups):
            if groups[key] and len(selected) < count:
                selected.append(groups[key].pop(0)["item_id"])
                progressed = True
        if not progressed:
            break
    return selected


def expansion_needed(summary):
    for period in summary.get("periods", {}).values():
        interval = period["disagreement_interval"]
        if period["uncertain_n"] or period["unresolved_n"]:
            return True
        if period["disagreement_rate"] is not None and period["disagreement_rate"] >= .05:
            return True
        if interval[0] is not None and interval[0] < .05 < interval[1]:
            return True
    return False
