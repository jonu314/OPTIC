from __future__ import annotations

import math
from dataclasses import replace
from datetime import date, timedelta

import numpy as np

from .config import Settings
from .contracts import digest


def _totals(rows, label):
    n = sum(r["eligible"] for r in rows)
    k = sum(r.get(label, 0) if label in {"missing", "invalid", "allowed_empty"} else
            r["categories"].get(label, 0) for r in rows)
    return n, k, k / n if n else None


def practical(p0, p1, n1, label, settings):
    if p0 is None or p1 is None:
        return False
    delta = p1 - p0
    pp = 100 * abs(delta)
    if label in {"missing", "invalid"}:
        return delta >= settings.missing_pp / 100
    if p0 < .01:
        relative = abs(delta) / p0 if p0 else None
        return (relative is None or relative >= settings.rare_relative) and (
            pp >= max(settings.rare_min_pp, 50 * p0)) and abs(n1 * delta) >= settings.rare_excess
    return ((abs(delta) / p0 >= settings.common_relative and pp >= settings.common_pp)
            or pp >= settings.common_absolute_pp)


def _fit(rows, labels):
    n = np.asarray([r["eligible"] for r in rows], dtype=float)
    x = np.asarray([[r.get(label, 0) if label in {"missing", "invalid", "allowed_empty"} else
                     r["categories"].get(label, 0) for label in labels] for r in rows], dtype=float)
    if (x < 0).any() or (x > n[:, None]).any():
        raise ValueError("Daily category counts must be within eligible counts.")
    usable = n > 0
    rates = np.divide(x, n[:, None], out=np.full_like(x, np.nan), where=n[:, None] > 0)
    # Fit from the first in-window 28 dates; no pre-window observations or contaminated full-period average.
    first = np.arange(min(28, len(n)))
    support = first[n[first] > 0]
    p = (x[support].sum(0) + .5) / (n[support].sum() + 1)
    weekday = np.array([date.fromisoformat(r["day"]).weekday() for r in rows])
    centers = np.tile(p, (len(rows), 1))
    supported_weekdays = []
    for wd in range(7):
        indices = support[weekday[support] == wd]
        if len(indices) >= 3:
            centers[weekday == wd] = (x[indices].sum(0) + .5) / (n[indices].sum() + 1)
            supported_weekdays.append(wd)
    residual = rates[support] - centers[support]
    sampling_variance = centers[support] * (1 - centers[support]) / n[support, None]
    excess_variance = np.maximum(0, np.nanmean(residual ** 2 - sampling_variance, axis=0))
    rho = np.clip(excess_variance / np.maximum(p * (1 - p), 1e-7), 1e-6, .20)
    # Positive residual dependence makes contrast variance wider, never narrower.
    autocorrelation = []
    for j in range(len(labels)):
        values = residual[:, j]
        correlation = []
        for lag in range(1, min(8, len(values) // 2)):
            a, b = values[:-lag], values[lag:]
            correlation.append(float(np.corrcoef(a, b)[0, 1]) if a.std() > 1e-8 and b.std() > 1e-8 else 0.)
        autocorrelation.append(max(1., 1 + 2 * sum(max(0., v) for v in correlation if math.isfinite(v))))
    return n, x, centers, rho, np.array(autocorrelation), {
        "null_fit": "in-window initial 28-day stable segment",
        "weekday_effects": supported_weekdays, "rho": rho.tolist(),
        "variance_design_effect": autocorrelation, "usable_days": int(usable.sum()),
        "fit_days": len(support), "null_family": "weekday seven-day latent-vector block resampling + count draws",
        "parameters_estimated": True}


def _scan(n, x, rho, dependence, weekday_offsets=None):
    """Exact same date/category/recent/EWMA search for observed and simulated counts."""
    if x.ndim == 2:
        x = x[None, :, :]
    count = len(n)
    cumulative = np.concatenate([np.zeros((x.shape[0], 1, x.shape[2])), x.cumsum(1)], axis=1)
    cn = np.r_[0., n.cumsum()]
    scores = []
    descriptors = []
    for split in range(21, count - 13):
        # Local segments avoid averaging two previous regimes into a synthetic baseline.
        left = max(0, split - 28)
        right = min(count, split + 28)
        if split - left < 21 or right - split < 14:
            continue
        descriptors.append(("historical_step", left, split, split, right))
    if count >= 49:
        descriptors.append(("sustained_recent", count - 49, count - 21, count - 14, count))
    for _, left, pre_end, post_start, right in descriptors:
        nb, nr = cn[pre_end] - cn[left], cn[right] - cn[post_start]
        if nb == 0 or nr == 0:
            scores.append(np.zeros((x.shape[0], x.shape[2])))
            continue
        p0 = (cumulative[:, pre_end] - cumulative[:, left]) / nb
        p1 = (cumulative[:, right] - cumulative[:, post_start]) / nr
        pooled = np.clip((p0 * nb + p1 * nr + .5) / (nb + nr + 1), 1e-6, 1 - 1e-6)
        vb = ((1 - rho) * nb + rho * np.sum(n[left:pre_end] ** 2)) / nb ** 2
        vr = ((1 - rho) * nr + rho * np.sum(n[post_start:right] ** 2)) / nr ** 2
        variance = pooled * (1 - pooled) * (vb + vr) * dependence
        scores.append(np.abs(p1 - p0) / np.sqrt(np.maximum(variance, 1e-12)))
    score_matrix = np.stack(scores, axis=1) if scores else np.zeros((x.shape[0], 0, x.shape[2]))
    maxima = score_matrix.max((1, 2)) if len(scores) else np.zeros(x.shape[0])
    # Whole vector movement is searched jointly, not tested as unrelated winning options.
    if len(scores):
        joint = np.sqrt(np.sum(score_matrix ** 2, axis=2) / max(1, x.shape[2] - 1))
        maxima = np.maximum(maxima, joint.max(1))
    base_n = cn[min(28, count)]
    if base_n:
        center = (cumulative[:, min(28, count)] + .5) / (base_n + 1)
        expected = np.clip(center[:, None, :] + (weekday_offsets if weekday_offsets is not None else 0),
                           1e-7, 1 - 1e-7)
        z = np.divide(x - n[None, :, None] * expected,
                      np.sqrt(np.maximum(n[None, :, None] * expected * (1 - expected) *
                                         (1 + (n[None, :, None] - 1) * rho) * dependence, 1.)),
                      out=np.zeros_like(x), where=n[None, :, None] > 0)
        ewma = np.zeros((x.shape[0], x.shape[2]))
        ewma_max = np.zeros(x.shape[0])
        for i in range(min(28, count), count):
            ewma = .2 * z[:, i] + .8 * ewma
            ewma_max = np.maximum(ewma_max, np.abs(ewma).max(1) / math.sqrt(.2 / 1.8))
        maxima = np.maximum(maxima, ewma_max)
    return maxima, score_matrix, descriptors


def _null_counts(n, observed, centers, rho, settings, seed, simulations, multi_label):
    rng = np.random.default_rng(seed)
    usable = np.flatnonzero(n[:28] > 0)
    rates = np.divide(observed, n[:, None], out=np.zeros_like(observed), where=n[:, None] > 0)
    residuals = rates[:28] - centers[:28]
    residuals -= residuals[usable].mean(0) if len(usable) else 0
    # Circular seven-calendar-day vectors preserve shared category movement, weekday and weekly dependence.
    block_starts = np.arange(0, min(28, len(n)), 7)
    latents = np.empty((simulations, len(n), observed.shape[1]))
    for start in range(0, len(n), 7):
        draws = rng.choice(block_starts, size=simulations)
        for offset in range(min(7, len(n) - start)):
            indices = (draws + offset) % min(28, len(n))
            latents[:, start + offset] = centers[start + offset] + residuals[indices]
    latents = np.clip(latents, 1e-7, 1 - 1e-7)
    output = np.empty_like(latents)
    if multi_label:
        # A common block latent captures observed dependence; beta-binomial marginal residual dispersion.
        concentration = np.maximum(2., 1 / rho - 1)
        prob = rng.beta(latents * concentration, (1 - latents) * concentration)
        output[:] = rng.binomial(n.astype(int)[None, :, None], prob)
    else:
        latents /= latents.sum(2, keepdims=True)
        concentration = float(max(2., 1 / max(float(rho.max()), 1e-6) - 1))
        prob = rng.gamma(np.maximum(latents * concentration, 1e-6), 1)
        prob /= prob.sum(2, keepdims=True)
        remaining = np.broadcast_to(n.astype(int), (simulations, len(n))).copy()
        mass = np.ones((simulations, len(n)))
        for j in range(observed.shape[1] - 1):
            p = np.clip(prob[:, :, j] / np.maximum(mass, 1e-12), 0, 1)
            draws = rng.binomial(remaining, p)
            output[:, :, j] = draws
            remaining -= draws
            mass -= prob[:, :, j]
        output[:, :, -1] = remaining
    return output


def _stable(rows, label):
    """Four non-overlapping weekday-balanced seven-day blocks; allow supported day noise."""
    blocks = [rows[i:i + 7] for i in range(0, len(rows) - 6, 7)]
    stats = [_totals(block, label) for block in blocks]
    values = [(n, p) for n, _, p in stats if n > 0 and p is not None]
    if len(values) < 3:
        return False
    center = sum(n * p for n, p in values) / sum(n for n, _ in values)
    deviations = [abs(p - center) for _, p in values]
    return max(deviations) <= max(.02, 4 * math.sqrt(max(center * (1 - center), .001) /
                                                   max(1, min(n for n, _ in values))))


def _segments(n, counts, rho, dependence):
    """Penalized binomial count likelihood, with overdispersion-adjusted effective exposure."""
    weight = 1 / ((1 + np.maximum(0, n - 1) * rho) * dependence)
    exposure = n * weight
    events = counts * weight
    cn, ck = np.r_[0., exposure.cumsum()], np.r_[0., events.cumsum()]
    size = len(n)
    penalty = 2 * math.log(max(2, size))
    best = np.full(size + 1, np.inf)
    paths = [[] for _ in range(size + 1)]
    best[0] = -penalty
    for end in range(21, size + 1):
        for start in [0, *range(21, end - 13)]:
            if end - start < (21 if start == 0 else 14) or not math.isfinite(best[start]):
                continue
            total, positive = cn[end] - cn[start], ck[end] - ck[start]
            if total <= 0:
                continue
            rate = np.clip(positive / total, 1e-10, 1 - 1e-10)
            cost = -2 * (positive * math.log(rate) + (total - positive) * math.log1p(-rate))
            value = best[start] + cost + penalty
            if value < best[end]:
                best[end] = value
                paths[end] = paths[start] + ([start] if start else [])
    return paths[size]


def _signal(rows, label, descriptor, settings):
    method, left, pre_end, post_start, right = descriptor
    pre, post = rows[left:pre_end], rows[post_start:right]
    nb, _, p0 = _totals(pre, label)
    nr, _, p1 = _totals(post, label)
    if p0 is None or p1 is None:
        return None
    supported = nb >= settings.baseline_n and nr >= settings.recent_n and (
        sum(r["eligible"] > 0 for r in pre) >= settings.baseline_days and
        sum(r["eligible"] > 0 for r in post) >= settings.recent_days)
    blocks = [post[i:i + 7] for i in (0, 7)]
    block_rates = [_totals(block, label)[2] if len(block) == 7 else None for block in blocks]
    direction = 1 if p1 > p0 else -1
    persistence = all(p is not None and direction * (p - p0) > 0 for p in block_rates)
    meaningful = practical(p0, p1, nr, label, settings)
    latest = rows[-7:]
    nl, _, pl = _totals(latest, label)
    severe = nl >= settings.recent_n and pl is not None and (
        (label in {"missing", "invalid"} and pl - p0 >= settings.severe_missing_pp / 100)
        or abs(pl - p0) >= settings.severe_pp / 100)
    severe = severe and (method == "sustained_recent" or persistence)
    if not meaningful and not severe:
        return None
    stable = _stable(pre, label)
    recovered = pl is not None and not practical(p0, pl, nl, label, settings)
    return {"label": label, "methods": [method], "baseline_start": pre[0]["day"], "baseline_end": pre[-1]["day"],
            "recent_start": post[0]["day"], "recent_end": post[-1]["day"], "onset": post[0]["day"],
            "baseline_rate": p0, "recent_rate": p1, "baseline_n": nb, "recent_n": nr,
            "change_pp": 100 * (p1 - p0), "relative_change": (p1 - p0) / p0 if p0 else None,
            "direction": "increase" if direction > 0 else "decrease", "persistent": persistence,
            "confirming_blocks": [{"start": b[0]["day"], "end": b[-1]["day"], "rate": p}
                                  for b, p in zip(blocks, block_rates) if len(b) == 7],
            "baseline_stable": stable, "support_sufficient": supported, "severe": severe,
            "episode": "recovered" if recovered else "active", "estimated_excess_labels": nr * abs(p1 - p0),
            "processing_route": label in {"missing", "invalid"}}


def explain_mix(rows, signal, dimension="brand"):
    pre = [r for r in rows if signal["baseline_start"] <= r["day"] <= signal["baseline_end"]]
    post = [r for r in rows if signal["recent_start"] <= r["day"] <= signal["recent_end"]]
    def grouped(period):
        values = {}
        for row in period:
            for name, part in row.get("segments", {}).get(dimension, {}).items():
                item = values.setdefault(name, [0, 0])
                item[0] += part["n"]
                item[1] += part["counts"].get(signal["label"], 0)
        return values
    a, b = grouped(pre), grouped(post)
    na, nb = sum(v[0] for v in a.values()), sum(v[0] for v in b.values())
    common = {k for k in a.keys() & b.keys() if a[k][0] >= 100 and b[k][0] >= 50}
    covered_a, covered_b = sum(a[k][0] for k in common), sum(b[k][0] for k in common)
    if not common or not na or not nb:
        return {"dimension": dimension, "status": "unavailable / insufficient common support",
                "unsupported_recent_share": 1., "mix_supported": False}
    mix = sum(b[k][0] / covered_b * a[k][1] / a[k][0] for k in common)
    standardized_a = sum(a[k][0] / covered_a * a[k][1] / a[k][0] for k in common)
    standardized_b = sum(a[k][0] / covered_a * b[k][1] / b[k][0] for k in common)
    observed = sum(b[k][1] for k in common) / covered_b
    unsupported = 1 - covered_b / max(signal["recent_n"], 1)
    stable = all(abs(b[k][1] / b[k][0] - a[k][1] / a[k][0]) < .02 for k in common)
    return {"dimension": dimension, "status": "common-support comparison", "mix_only_rate": mix,
            "baseline_standardized_rate": standardized_a, "recent_standardized_rate": standardized_b,
            "observed_common_support_rate": observed, "unsupported_recent_share": unsupported,
            "unknown_new_segments": sorted(b.keys() - a.keys()),
            "mix_supported": stable and unsupported <= .10 and abs(observed - mix) <= .01,
            "segments": [{"name": k, "baseline_n": a[k][0], "recent_n": b[k][0],
                          "baseline_rate": a[k][1] / a[k][0], "recent_rate": b[k][1] / b[k][0]}
                         for k in sorted(common)],
            "limitation": "Within-segment call intent remains unmeasured; missing dimensions are not stability."}


def detect(contracts, daily_by_key, window, settings=None):
    settings = settings or Settings()
    # Include every declared enabled finite/quality test, not only practical pre-screen survivors.
    family = [c for c in contracts if settings.all_active_jobs or c.job in settings.jobs]
    family_size = len(family)
    family_keys = [c.key for c in family]
    required = max(settings.simulations_min, math.ceil(family_size / settings.q_gate) - 1,
                   math.ceil(1 / settings.p_gate) - 1)
    simulations = min(required, settings.simulations_max)
    resolution = 1 / (simulations + 1)
    precision = resolution <= min(settings.p_gate, settings.q_gate / max(1, family_size))
    results = []
    for contract in contracts:
        rows = daily_by_key.get(contract.key, [])
        for row in rows:
            window.require(row["day"])
        state = {"contract_key": contract.key, "status": "Not yet evaluated", "signals": [], "mix": {},
                 "statistics": {"family_size": family_size, "correction": "Bonferroni; arbitrary dependence",
                                "scan_adjusted_p": 1., "corrected_q": 1., "simulations": 0,
                                "minimum_attainable_p": resolution, "precision_sufficient": precision,
                                "declared_family": family_keys, "seed": settings.seed}}
        if contract not in family:
            state["next_action"] = "Outside default job allowlist; completeness capability available when enabled."
            results.append(state)
            continue
        if sum(r["eligible"] for r in rows) < settings.baseline_n + settings.recent_n or len(rows) < 49:
            state["status"] = "Insufficient data"
            state["next_action"] = "Accumulate eligible in-window volume; no health/accuracy claim."
            results.append(state)
            continue
        labels = list(contract.labels) if contract.finite else []
        labels += ["missing", "invalid", "allowed_empty"]
        if contract.value_mode == "TEXT_PRESENCE_3_WORDS":
            labels += ["text_present", "text_short"]
        n, x, centers, rho, dependence, diagnostics = _fit(rows, labels)
        fit_n = n[:28].sum()
        weekday_offsets = centers - ((centers[:28] * n[:28, None]).sum(0) / max(1., fit_n))
        observed, matrix, descriptors = _scan(n, x, rho, dependence, weekday_offsets)
        signals = []
        segment_changes = [_segments(n, x[:, j], rho[j], dependence[j]) for j in range(len(labels))]
        for j, label in enumerate(labels):
            candidates = []
            changes = segment_changes[j]
            for k, descriptor in enumerate(descriptors):
                if descriptor[0] == "historical_step" and descriptor[2] not in changes:
                    continue
                signal = _signal(rows, label, descriptor, settings)
                if signal:
                    signal["scan_score"] = float(matrix[0, k, j])
                    candidates.append(signal)
            # Count-cost penalized local segmentation; select separated supported step regimes.
            candidates.sort(key=lambda s: s["scan_score"], reverse=True)
            confirmed = [s for s in candidates if s["baseline_stable"] and s["support_sufficient"] and
                         (s["persistent"] or s["severe"])]
            candidates = confirmed or candidates[:1]
            accepted = []
            for signal in candidates:
                nearby = next((v for v in accepted if abs((date.fromisoformat(v["onset"]) -
                                                           date.fromisoformat(signal["onset"])).days) < 14), None)
                if nearby:
                    nearby["methods"] = sorted(set(nearby["methods"] + signal["methods"]))
                elif signal["scan_score"] ** 2 > 2 * math.log(max(2, len(rows))):
                    accepted.append(signal)
            signals.extend(accepted)
        # Distribution movement can qualify when no single option passes its practical gate.
        if contract.result_type == "STRINGPICKONE" and contract.options:
            for descriptor in descriptors:
                if descriptor[0] == "historical_step" and not any(
                    descriptor[2] in segment_changes[j]
                    for j in range(len(contract.options))):
                    continue
                method, left, pre_end, post_start, right = descriptor
                pre, post = rows[left:pre_end], rows[post_start:right]
                tvd = sum(abs((_totals(post, label)[2] or 0) - (_totals(pre, label)[2] or 0))
                          for label in contract.options) / 2
                if tvd < settings.distribution_tvd:
                    continue
                relaxed = replace(settings, common_pp=0., common_relative=0., rare_excess=0, rare_min_pp=0.)
                label = max(contract.options, key=lambda v: abs((_totals(post, v)[2] or 0) -
                                                               (_totals(pre, v)[2] or 0)))
                signal = _signal(rows, label, descriptor, relaxed)
                block_tvd = [
                    sum(abs((_totals(block, option)[2] or 0) - (_totals(pre, option)[2] or 0))
                        for option in contract.options) / 2
                    for block in (post[:7], post[7:14]) if len(block) == 7]
                if signal and signal["baseline_stable"] and signal["persistent"] and not any(
                    s["onset"] == signal["onset"] and s["label"] == label for s in signals) and (
                    len(block_tvd) == 2 and min(block_tvd) >= settings.distribution_tvd):
                    signal.update(distribution_tvd=tvd, distribution_block_tvd=block_tvd,
                                  methods=[method, "joint_category_vector"])
                    signals.append(signal)
        # Gradual movement has a separate Watch route; infer no onset from a left-boundary level alone.
        if observed[0] >= 3 and not signals:
            state["status"] = "Watch"
            state["next_action"] = "EWMA residual movement; await supported practical persistence."
        else:
            state["status"] = "No detected shift"
        diagnostics["complexity_penalty"] = "2 log(calendar days) binomial effective-count cost; minimum segment 14 days"
        diagnostics["selected_split_calibration"] = "complete category/date/recent/EWMA superset scan, conservative for segmentation"
        diagnostics["ewma_lambda"] = .2
        state["statistics"]["diagnostics"] = diagnostics
        if signals:
            seed = int(digest([contract.key, window.as_dict(), settings.seed])[:16], 16)
            exceedances = 0
            done = 0
            while done < simulations:
                batch = min(256, simulations - done)
                null = _null_counts(n, x, centers, rho, settings, seed + done, batch,
                                    contract.result_type == "LIST" or not contract.finite)
                maxima, _, _ = _scan(n, null, rho, dependence, weekday_offsets)
                exceedances += int(np.sum(maxima >= observed[0]))
                done += batch
            p = (exceedances + 1) / (done + 1)
            q = min(1., p * family_size)
            state["statistics"].update(scan_adjusted_p=p, corrected_q=q, simulations=done,
                                       exceedances=exceedances, seed=seed,
                                       observed_complete_scan_max=float(observed[0]))
            qualified = [s for s in signals if s["support_sufficient"] and s["baseline_stable"] and
                         (s["persistent"] or s["severe"])]
            state["signals"] = sorted(signals, key=lambda s: (s["onset"], s["label"]))
            if qualified and p <= settings.p_gate and q <= settings.q_gate and precision:
                state["status"] = "Candidate"
            else:
                state["status"] = "Watch"
            state["next_action"] = ("Trend review then bounded independent transcript audit."
                                    if state["status"] == "Candidate" else
                                    ("Insufficient statistical precision for the full declared family."
                                     if not precision else "Support/stability/statistical confirmation pending."))
            strongest = max(signals, key=lambda s: s["estimated_excess_labels"])
            state["mix"] = {d: explain_mix(rows, strongest, d) for d in ("brand", "language", "site")}
            state["mix_by_label"] = {s["label"]: {d: explain_mix(rows, s, d)
                                      for d in ("brand", "language", "site")}
                                     for s in signals}
            state["priority"] = strongest["estimated_excess_labels"] * (
                2 if strongest["persistent"] else 1) * (2 if strongest["severe"] else 1)
        results.append(state)
    return results


def baseline_ageing(incident, window):
    signals = incident.get("signals") or []
    if any(not window.contains(s["baseline_start"]) for s in signals):
        return {"status": "unavailable", "message": "pre-shift baseline no longer available in the current window",
                "preserve_prior_decision": True, "compare_old_baseline": False}
    return {"status": "available", "compare_old_baseline": True}


def cluster_incidents(incidents):
    parents = []
    for incident in sorted(incidents, key=lambda i: i["id"]):
        signal = (incident.get("signals") or [{}])[0]
        onset = signal.get("onset")
        job = incident.get("contract", {}).get("job")
        parent = next((p for p in parents if onset and p["onset"] and p["job"] == job and
                       abs((date.fromisoformat(onset) - date.fromisoformat(p["onset"])).days) <= 3), None)
        if parent is None:
            parent = {"id": digest([job, onset, incident["id"]])[:24], "job": job, "onset": onset, "children": []}
            parents.append(parent)
        parent["children"].append(incident["id"])
        incident["parent_id"] = parent["id"]
    return parents
