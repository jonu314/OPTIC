from __future__ import annotations

import base64
import hashlib
import os
from pathlib import Path

from .contracts import digest

PACKET_VERSION = "health-evidence-v1"


def coverage_packet(store, incident):
    """Reconstruct a coverage drilldown from frozen snapshots without writing state."""
    from .config import Window
    from .contracts import Contract

    run_id = (incident.get("evidence") or {}).get("daily_run_id")
    run = store.get("runs", run_id) if run_id else None
    daily = store.get("daily", run_id) if run_id else None
    if not run or not daily or run.get("window") != incident.get("window"):
        raise ValueError("Coverage snapshot window is unavailable or mismatched.")
    contract = Contract.from_dict(incident["contract"])
    if not any(x.get("key") == contract.key for x in run.get("contracts", [])):
        raise ValueError("Coverage contract is not in the referenced frozen run.")
    if daily.get("window") != run["window"] or digest(daily.get("by_key")) != daily.get("hash"):
        raise ValueError("Coverage aggregate snapshot hash/window mismatch.")
    rows = daily["by_key"].get(contract.key)
    if rows is None:
        raise ValueError("Coverage aggregate is missing the selected contract.")
    return build_packet(contract, rows, incident, Window.from_dict(run["window"]))


def build_packet(contract, daily, detection, window):
    for row in daily:
        window.require(row["day"])
    metrics = {}
    rows = []
    for row in daily:
        n, valid = row["eligible"], row["valid"]
        categories = {label: {"count": value, "eligible_rate": value / n if n else None,
                              "valid_rate": value / valid if valid else None}
                      for label, value in row["categories"].items()}
        copy = {key: row[key] for key in ("day", "eligible", "valid", "allowed_empty", "missing", "invalid")}
        copy["categories"] = categories
        copy["labels_per_call"] = row.get("labels_per_call_sum", 0) / n if n else None
        rows.append(copy)
        metrics[f"daily:{row['day']}:eligible"] = n
        for label, measure in categories.items():
            metrics[f"daily:{row['day']}:{label}:rate"] = measure["eligible_rate"]
    for index, signal in enumerate(detection.get("signals") or []):
        for key in ("baseline_rate", "recent_rate", "change_pp", "relative_change", "baseline_n", "recent_n"):
            metrics[f"signal:{index}:{key}"] = signal[key]
    statistics = dict(detection["statistics"])
    if "declared_family" in statistics:
        statistics["declared_family_hash"] = digest(statistics.pop("declared_family"))
        statistics["family_reference"] = "full declared keys retained in the immutable run detector snapshot"
    metrics["statistics:scan_adjusted_p"] = statistics["scan_adjusted_p"]
    metrics["statistics:corrected_q"] = statistics["corrected_q"]
    packet = {"version": PACKET_VERSION, "window": window.as_dict(), "contract": contract.as_dict(),
              "daily": rows, "signals": detection.get("signals") or [], "statistics": statistics,
              "mix": detection.get("mix") or {}, "mix_by_label": detection.get("mix_by_label") or {},
              "metrics": metrics, "triggering_rules": {
                  "confirmation": "two non-overlapping complete seven-day blocks",
                  "rates": "[0,1]; change_pp uses percentage points; relative_change is a ratio",
                  "stage": detection["status"]}, "known_operational_changes": [],
              "unknowns": ["Historical runtime contract not reconstructed", "Model/prompt version may be unknown",
                           "Language/site absence does not establish unchanged mix",
                           "Distribution change is not classification correctness"],
              "scope": contract.historical_scope, "demo": contract.attribute_id.startswith("demo-")}
    packet["hash"] = digest(packet)
    return packet


def chart_spec(packet):
    signals = packet["signals"]
    labels = sorted({label for row in packet["daily"] for label in row["categories"]})
    label = max(signals, key=lambda s: abs(s["change_pp"]))["label"] if signals else (labels[0] if labels else "missing")
    days = [r["day"] for r in packet["daily"]]
    counts = [r["eligible"] for r in packet["daily"]]
    def count(row, key):
        return row.get(key, 0) if key in {"missing", "invalid", "allowed_empty"} else row["categories"].get(
            key, {}).get("count", 0)
    numerator = [count(row, label) for row in packet["daily"]]
    rates = [100 * k / n if n else None for k, n in zip(numerator, counts)]
    smoother = []
    for i in range(len(days)):
        n = sum(counts[max(0, i - 6):i + 1])
        k = sum(numerator[max(0, i - 6):i + 1])
        smoother.append(100 * k / n if n and i >= 6 else None)
    areas, lines = [], []
    for signal in signals:
        areas.extend([[{"name": "Baseline", "xAxis": signal["baseline_start"],
                        "itemStyle": {"color": "rgba(77,159,255,.08)"}},
                       {"xAxis": signal["baseline_end"]}],
                      [{"name": "Recent", "xAxis": signal["recent_start"],
                        "itemStyle": {"color": "rgba(251,191,36,.06)"}},
                       {"xAxis": signal["recent_end"]}]])
        if signal.get("onset"):
            lines.append({"name": "Proposed shift", "xAxis": signal["onset"]})
    series = [
        {"name": label + " / eligible", "type": "line", "data": rates, "connectNulls": False,
         "symbolSize": 4, "lineStyle": {"width": 2}, "itemStyle": {"color": "#54a8ff"},
         "markArea": {"silent": True, "data": areas},
         "markLine": {"symbol": "none", "label": {"color": "#a9b8ca"}, "data": lines}},
        {"name": "7-day volume-weighted smoother", "type": "line", "data": smoother,
         "showSymbol": False, "connectNulls": False, "lineStyle": {"width": 1, "opacity": .35,
         "type": "dashed"}, "itemStyle": {"color": "#a7c9ff"}},
        {"name": "Eligible calls", "type": "bar", "xAxisIndex": 1, "yAxisIndex": 1,
         "data": counts, "itemStyle": {"color": "#334a64"}},
    ]
    palette = ["#54a8ff", "#45d5b0", "#fbbf24", "#c697fa", "#ed8585", "#86b9cc"]
    for i, key in enumerate(labels + ["allowed_empty", "missing", "invalid"]):
        values = [100 * count(r, key) / r["eligible"] if r["eligible"] else None for r in packet["daily"]]
        series.append({"name": key, "type": "line", "xAxisIndex": 2, "yAxisIndex": 2,
                       "data": values, "showSymbol": False, "connectNulls": False,
                       "lineStyle": {"width": 1}, "itemStyle": {"color": palette[i % len(palette)]}})
    spec = {"animation": False, "backgroundColor": "#10161e",
            "title": {"text": packet["contract"]["name"] + " / " + packet["contract"]["job"],
                      "subtext": f"{packet['window']['start']} to {packet['window']['end']} | {label}",
                      "left": 18, "top": 10, "textStyle": {"color": "#e6eef9", "fontSize": 16},
                      "subtextStyle": {"color": "#a3b1c3", "fontSize": 11}},
            "tooltip": {"trigger": "axis"}, "legend": {"type": "scroll", "bottom": 1,
                "textStyle": {"color": "#abb8cb"}, "data": [s["name"] for s in series]},
            "grid": [{"left": 62, "right": 24, "top": 80, "height": "36%"},
                     {"left": 62, "right": 24, "top": "53%", "height": "12%"},
                     {"left": 62, "right": 24, "top": "72%", "height": "18%"}],
            "xAxis": [{"type": "category", "gridIndex": i, "data": days,
                       "axisLabel": {"color": "#8d9eb3", "hideOverlap": True, "show": i == 2},
                       "axisLine": {"lineStyle": {"color": "#334053"}}} for i in range(3)],
            "yAxis": [{"type": "value", "gridIndex": i, "name": name,
                       "nameTextStyle": {"color": "#8d9eb3"}, "axisLabel": {"color": "#8d9eb3"},
                       "splitLine": {"lineStyle": {"color": "#243142"}}}
                      for i, name in enumerate(["Rate (%)", "Calls", "Category (%)"])],
            "series": series}
    return spec


def render_png(spec, output):
    """Render the immutable spec using local ECharts, without a user's desktop browser."""
    from playwright.sync_api import sync_playwright

    vendor = Path(__file__).resolve().parents[1] / "web" / "vendor" / "echarts.min.js"
    if not vendor.is_file():
        raise ValueError("Local ECharts asset is missing.")
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, channel=os.getenv("OPTIC_HEALTH_BROWSER_CHANNEL") or None)
        try:
            page = browser.new_page(viewport={"width": 1200, "height": 760}, device_scale_factor=1)
            page.set_content('<html><body style="margin:0"><div id="chart" style="width:1200px;height:760px"></div></body></html>')
            page.add_script_tag(path=str(vendor))
            encoded = page.evaluate("""spec => {
                const c = echarts.init(document.getElementById('chart'), null, {renderer:'canvas'});
                c.setOption(spec);
                return c.getDataURL({type:'png',pixelRatio:1,backgroundColor:'#10161e'}).split(',')[1];
            }""", spec)
            data = base64.b64decode(encoded, validate=True)
            if not data.startswith(b"\x89PNG\r\n\x1a\n"):
                raise ValueError("Headless evidence export did not produce PNG.")
            output = Path(output)
            output.parent.mkdir(parents=True, exist_ok=True)
            tmp = output.with_suffix(".tmp")
            tmp.write_bytes(data)
            tmp.replace(output)
            return hashlib.sha256(data).hexdigest()
        finally:
            browser.close()


def save_evidence(store, incident_id, packet, render=True):
    # Store.get adds its row ID; that field is not part of the signed packet.
    packet = {k: v for k, v in packet.items() if k != "id"}
    if digest({k: v for k, v in packet.items() if k != "hash"}) != packet["hash"]:
        raise ValueError("Evidence packet hash mismatch.")
    store.put("evidence", packet["hash"], packet, immutable=True)
    spec = chart_spec(packet)
    spec_hash = digest(spec)
    store.put("charts", spec_hash, {"hash": spec_hash, "packet_hash": packet["hash"], "spec": spec}, immutable=True)
    result = {"packet_hash": packet["hash"], "chart_hash": spec_hash, "chart_spec_hash": spec_hash,
              "incident_id": incident_id, "render_status": "not requested"}
    if render:
        root = Path(store.root)
        output = root / "charts" / (spec_hash + ".png")
        image_hash = render_png(spec, output)
        result.update(png_hash=image_hash, png_artifact=spec_hash + ".png", render_status="exported",
                      rendering_version="local ECharts/Chromium; immutable spec and PNG separately hashed")
        store.put("chart_images", spec_hash, {"chart_hash": spec_hash, "packet_hash": packet["hash"],
                                             "png_hash": image_hash, "artifact": spec_hash + ".png"}, immutable=True)
    return result
