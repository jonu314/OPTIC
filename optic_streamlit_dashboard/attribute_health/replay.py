from __future__ import annotations

from datetime import timedelta

from .config import Window
from .detection import detect


def replay(contracts, daily_by_key, window, settings, evaluation_dates=None, known_onsets=None):
    """Chronological bounded replay; no future rows, inference or human-label fabrication."""
    evaluation_dates = evaluation_dates or [
        window.start + timedelta(days=i) for i in range(48, len(window.days()), 7)] + [window.end]
    results = []
    for end in sorted(set(evaluation_dates)):
        window.require(end)
        current = Window(max(window.start, end - timedelta(days=89)), end, window.watermark)
        inputs = {key: [row for row in rows if current.contains(row["day"])] for key, rows in daily_by_key.items()}
        detections = detect(contracts, inputs, current, settings)
        results.append({"window": current.as_dict(),
                        "detections": [{"contract_key": d["contract_key"], "status": d["status"],
                                        "signals": d["signals"], "statistics": d["statistics"]} for d in detections]})
    first = {}
    unique = set()
    for evaluation in results:
        for detection in evaluation["detections"]:
            if detection["status"] == "Candidate":
                first.setdefault(detection["contract_key"], evaluation["window"]["end"])
                for signal in detection["signals"]:
                    unique.add((detection["contract_key"], signal["direction"], signal["onset"]))
    delays = {}
    if known_onsets:
        from datetime import date
        for key, onset in known_onsets.items():
            delays[key] = ((date.fromisoformat(first[key]) - date.fromisoformat(str(onset))).days
                           if key in first else None)
    return {"replay_version": "chronological-bounded-v1", "evaluations": results,
            "calibration_split": "earlier evaluations", "held_out_split": "last two evaluations",
            "threshold_tuning_performed": False, "unique_candidate_regimes": len(unique),
            "first_candidate_dates": first, "detection_delay_days": delays,
            "metrics": {"false_incidents_per_stable_attribute_month": None,
                        "candidate_audit_yield": None, "suppression_misses": None,
                        "judge_human_agreement": None, "cost_per_useful_incident": None},
            "limitations": ["Unlabelled recorded movements are not confirmed defects or a stable-null benchmark.",
                            "Null burden requires declared stable fixture truth.",
                            "Yield, suppression and human calibration require independently reviewed outcomes."]}
