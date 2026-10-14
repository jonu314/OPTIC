"""OPTIC Console — thin JSON API + static host for the OPTIC_Console.html dashboard.

This server does NOT modify Dashboard_Final.py. It imports that module headlessly
(neutralising Streamlit's caching decorators) and reuses its Snowflake connection,
query, and metadata logic to serve a modern single-page HTML dashboard.

A browser cannot speak to Snowflake directly (driver + key-pair auth live in Python),
so this lightweight local server is the bridge: the HTML calls /api/* endpoints here,
which run the exact same queries the Streamlit app uses.

Run:
    python optic_console_server.py            # serves http://localhost:8520
    python optic_console_server.py --port 9000 --no-browser
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import threading
import time
import traceback
import webbrowser
from datetime import date, datetime, timedelta
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd

# ---------------------------------------------------------------------------
# Import Dashboard_Final headlessly: turn Streamlit's caching decorators into
# no-ops so the module's data functions can run outside a Streamlit script run.
# ---------------------------------------------------------------------------
WEB_DIR = Path(__file__).resolve().parent
PKG_ROOT = WEB_DIR.parent
DASH_DIR = PKG_ROOT / "dashboards"
VENDOR_DIR = WEB_DIR / "vendor"

ACTIVE_PROMPT_FILENAMES = [
    "Service_attributes_prompt_20260630.json",
    "Acquisition_attributes_prompt_20260630.json",
    "Retention_attributes_prompt_20260630.json",
    "ASM_attributes_prompt_20260630.json",
    "ASM_MWNE_attributes_prompt_20260630.json",
    "Escalation_attributes_prompt_20260630.json",
    "MWNE_Acquisition_attributes_prompt_20260630.json",
    "MWNE_Retention_attributes_prompt_20260630.json",
    "MWNE_Service_attributes_prompt_20260630.json",
    "MWNE_OBTM_attributes_prompt_20260630.json",
]

import os as _os
_os.environ.setdefault("OPTIC_DASHBOARD_CONFIG_DIR", str(PKG_ROOT / "config"))
_os.environ.setdefault("OPTIC_DASHBOARD_OUTPUT_ROOT", str(PKG_ROOT / "output"))

sys.path.insert(0, str(DASH_DIR))

import streamlit as st  # noqa: E402


def _identity_decorator(*args, **kwargs):
    if args and callable(args[0]) and not kwargs:
        return args[0]

    def wrap(fn):
        return fn

    return wrap


st.cache_data = _identity_decorator  # type: ignore[assignment]
st.cache_resource = _identity_decorator  # type: ignore[assignment]

import importlib  # noqa: E402

dfm = importlib.import_module("Dashboard_Final")
from model_lab import ApiResponse, ModelLabService  # noqa: E402
from health_service import HealthError, HealthService, MAX_BODY, local_request, strict_json  # noqa: E402


# ---------------------------------------------------------------------------
# Small in-process TTL cache wrapped around the module's run_query so repeated
# UI interactions don't re-hit Snowflake. (Streamlit's own cache was disabled.)
# ---------------------------------------------------------------------------
_QUERY_TTL = 300.0
_query_cache: dict[str, tuple[float, pd.DataFrame]] = {}
_query_lock = threading.Lock()
_original_run_query = dfm.run_query


def _cached_run_query(sql: str) -> pd.DataFrame:
    now = time.time()
    with _query_lock:
        hit = _query_cache.get(sql)
        if hit and (now - hit[0]) < _QUERY_TTL:
            return hit[1]
    df = _original_run_query(sql)
    with _query_lock:
        _query_cache[sql] = (now, df)
    return df


dfm.run_query = _cached_run_query  # functions resolve run_query as a module global
MODEL_LAB = ModelLabService(PKG_ROOT, dfm)
MAX_JSON_BODY_BYTES = 1_000_000
_health_service = None
_health_lock = threading.Lock()


def get_health_service():
    global _health_service
    with _health_lock:
        if _health_service is None:
            _health_service = HealthService()
        return _health_service


def clear_caches() -> None:
    global _active_prompt_cache
    with _query_lock:
        _query_cache.clear()
    _active_prompt_cache = None


# ---------------------------------------------------------------------------
# Source-table context (columns, detected dimension columns, attribute metadata).
# Cached per source table so we only probe information_schema once per process.
# ---------------------------------------------------------------------------
_ctx_lock = threading.Lock()
_ctx_cache: dict[str, dict] = {}
_active_prompt_cache: dict | None = None


def normalize_job_name(value: str) -> str:
    return str(value or "").strip().upper().replace(" ", "_").replace("-", "_")


def normalize_attr_name(value: str) -> str:
    return str(value or "").strip().lower()


def active_prompt_paths() -> list[Path]:
    explicit = _os.getenv("OPTIC_CONSOLE_ACTIVE_PROMPT_FILES", "").strip()
    if explicit:
        return [Path(part.strip()) for part in explicit.split(";") if part.strip()]

    paths: list[Path] = []
    # Prefer the freshly downloaded working files, with bundled config as a fallback
    # if these are later copied into the dashboard package.
    for base in [Path.home() / "Downloads", PKG_ROOT / "config"]:
        for filename in ACTIVE_PROMPT_FILENAMES:
            path = base / filename
            if path.exists() and path not in paths:
                paths.append(path)
    return paths


def load_active_prompt_job_map() -> dict:
    global _active_prompt_cache
    if _active_prompt_cache is not None:
        return _active_prompt_cache

    attr_jobs: dict[str, set[str]] = {}
    loaded: list[str] = []
    errors: list[str] = []
    for path in active_prompt_paths():
        try:
            data = json.loads(path.read_text(encoding="utf-8-sig"))
            job = normalize_job_name(data.get("jobName") or path.stem.replace("_attributes_prompt_20260630", ""))
            if not job:
                continue
            attr_count = 0
            for subprompt in data.get("subPrompts", []):
                for attr in (subprompt.get("attributes") or {}).keys():
                    attr_l = normalize_attr_name(attr)
                    if not attr_l:
                        continue
                    attr_jobs.setdefault(attr_l, set()).add(job)
                    attr_count += 1
            loaded.append(f"{path.name}:{job}:{attr_count}")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{path.name}: {exc}")

    _active_prompt_cache = {
        "attr_jobs": {attr: sorted(jobs) for attr, jobs in attr_jobs.items()},
        "loaded": loaded,
        "errors": errors,
    }
    return _active_prompt_cache


def apply_active_prompt_jobs(metadata_df: pd.DataFrame) -> pd.DataFrame:
    if metadata_df is None or metadata_df.empty:
        return metadata_df

    active = load_active_prompt_job_map()
    active_jobs_by_attr = active.get("attr_jobs", {})
    if not active_jobs_by_attr:
        metadata_df.attrs["metadata_warning"] = " ".join(
            filter(None, [metadata_df.attrs.get("metadata_warning"), "No active 20260630 prompt JSONs were loaded."])
        )
        return metadata_df

    meta = metadata_df.copy()
    attrs = [normalize_attr_name(value) for value in meta.get("attribute", [])]
    active_jobs = [list(active_jobs_by_attr.get(attr, [])) for attr in attrs]
    meta["jobs"] = active_jobs
    meta["jobs_display"] = [", ".join(jobs) for jobs in active_jobs]
    inactive_mask = meta["jobs"].map(lambda jobs: len(jobs) == 0)
    for col in ["validation_enabled", "drift_eligible"]:
        if col in meta.columns:
            meta.loc[inactive_mask, col] = False

    meta.attrs = metadata_df.attrs.copy()
    source = meta.attrs.get("metadata_source", "")
    active_source = f"active prompt jobs from {len(active.get('loaded', []))} 20260630 JSONs"
    meta.attrs["metadata_source"] = f"{source} + {active_source}" if source else active_source
    if active.get("errors"):
        meta.attrs["metadata_warning"] = " ".join(
            filter(None, [meta.attrs.get("metadata_warning"), "Active prompt JSON load errors: " + "; ".join(active["errors"])])
        )
    return meta


def get_context(source_table: str) -> dict:
    with _ctx_lock:
        cached = _ctx_cache.get(source_table)
    if cached:
        return cached

    ctx: dict = {"source_table": source_table, "live_ok": False, "error": None}
    try:
        columns_df = dfm.get_table_columns(source_table)
        records = dfm.column_records(columns_df)
        ctx["columns"] = records
        ctx["attr_types"] = dfm.column_type_lookup(records)
        ctx["live_ok"] = bool(records)
        ctx["date_col"] = dfm.find_first_column(records, ["INTERACTIONDATE", "RUNDATE", "CREATEDDATE"])
        ctx["job_col"] = dfm.find_first_column(records, ["JOBNAME", "JOB_NAME"])
        ctx["brand_col"] = dfm.find_first_column(records, ["BRAND", "BRANDNAME"])
        ctx["connection_col"] = dfm.find_first_column(records, ["CONNECTION_ID", "CONNID", "SOURCEID"])
        ctx["duration_col"] = dfm.find_first_column(records, ["DURATION", "CONTENTLENGTH"])
    except Exception as exc:  # noqa: BLE001
        ctx["error"] = str(exc)
        ctx["columns"] = []
        ctx["attr_types"] = {}
        ctx["date_col"] = None

    # Attribute metadata (ODIN source-of-truth with JSON/Excel fallback).
    try:
        meta = dfm.load_attribute_metadata(
            dfm.DEFAULT_ATTRIBUTE_TABLE,
            str(dfm.DEFAULT_PROMPT_PATHS["ACQUISITION"]),
            str(dfm.DEFAULT_PROMPT_PATHS["RETENTION"]),
            str(dfm.DEFAULT_PROMPT_PATHS["SERVICE"]),
            str(dfm.DEFAULT_ATTRIBUTES_XLSX),
        )
        # The ODIN prompt-details view already carries the authoritative
        # JOBNAME -> attribute mapping, so the legacy active-prompt JSON override is
        # skipped for it. It stays as a fallback for the ODIN-table / JSON paths.
        if meta.attrs.get("jobs_source") != "promptdetails_view":
            meta = apply_active_prompt_jobs(meta)
        ctx["metadata_df"] = meta
        ctx["metadata_warning"] = meta.attrs.get("metadata_warning")
        ctx["metadata_source"] = meta.attrs.get("metadata_source", "")
    except Exception as exc:  # noqa: BLE001
        ctx["metadata_df"] = pd.DataFrame()
        ctx["metadata_warning"] = f"metadata load failed: {exc}"
        ctx["metadata_source"] = ""

    if ctx.get("live_ok"):
        with _ctx_lock:
            _ctx_cache[source_table] = ctx
    return ctx


# ---------------------------------------------------------------------------
# JSON helpers
# ---------------------------------------------------------------------------
def _json_default(value):
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return None if pd.isna(value) else value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (pd.Int64Dtype, )):
        return int(value)
    try:
        import numpy as np

        if isinstance(value, np.integer):
            return int(value)
        if isinstance(value, np.floating):
            return None if np.isnan(value) else float(value)
        if isinstance(value, np.bool_):
            return bool(value)
    except Exception:  # noqa: BLE001
        pass
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    return str(value)


def df_records(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []
    return df.astype(object).where(pd.notna(df), None).to_dict("records")


def sanitize(obj):
    """Recursively replace NaN/Inf floats (invalid JSON) with None."""
    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [sanitize(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj


def parse_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [part for part in (p.strip() for p in value.split(",")) if part]


def parse_dates(qs: dict) -> tuple[date, date]:
    end_s = (qs.get("end") or [""])[0]
    start_s = (qs.get("start") or [""])[0]
    end_dt = date.fromisoformat(end_s) if end_s else date.today()
    start_dt = date.fromisoformat(start_s) if start_s else end_dt - timedelta(days=30)
    return start_dt, end_dt


def metadata_subset(metadata_df: pd.DataFrame, columns: set[str]) -> pd.DataFrame:
    if metadata_df is None or metadata_df.empty:
        return pd.DataFrame()
    return metadata_df[metadata_df["column"].isin(columns)].copy()


def attribute_jobs_by_column(metadata_df: pd.DataFrame) -> dict[str, list[str]]:
    if metadata_df is None or metadata_df.empty:
        return {}
    mapping: dict[str, list[str]] = {}
    for _, row in metadata_df.iterrows():
        column = str(row.get("column") or "").upper()
        if not column:
            continue
        mapping[column] = [str(job).upper() for job in (row.get("jobs") or []) if str(job).strip()]
    return mapping


def effective_jobs_for_attribute(
    attr: str,
    metadata_df: pd.DataFrame,
    selected_jobs: list[str],
) -> list[str] | None:
    selected = {str(job).upper() for job in selected_jobs if str(job).strip()}
    attr_u = str(attr or "").upper()
    attr_jobs = attribute_jobs_by_column(metadata_df).get(attr_u, [])
    active = {str(job).upper() for job in attr_jobs if str(job).strip()}
    if not active:
        return None
    if selected:
        effective = active & selected
        return sorted(effective) if effective else None
    return sorted(active)


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------
def api_bootstrap(qs: dict) -> dict:
    source_table = (qs.get("source") or [dfm.DEFAULT_OPTIC_TABLE])[0]
    start_dt, end_dt = parse_dates(qs)
    ctx = get_context(source_table)

    payload = {
        "source_table": source_table,
        "live_ok": ctx.get("live_ok", False),
        "error": ctx.get("error"),
        "date_col": ctx.get("date_col"),
        "job_col": ctx.get("job_col"),
        "brand_col": ctx.get("brand_col"),
        "connection_col": ctx.get("connection_col"),
        "duration_col": ctx.get("duration_col"),
        "columns": ctx.get("columns", []),
        "metadata_warning": ctx.get("metadata_warning"),
        "metadata_source": ctx.get("metadata_source", ""),
        "jobs": [],
        "brands": [],
        "metadata": [],
    }
    if not ctx.get("live_ok"):
        return payload

    column_names = {c["name"] for c in ctx["columns"]}
    options = dfm.fetch_filter_options(
        source_table, ctx["date_col"], start_dt, end_dt, ctx["job_col"], ctx["brand_col"]
    )
    payload["jobs"] = options["jobs"]["VALUE"].tolist() if not options["jobs"].empty else []
    payload["brands"] = options["brands"]["VALUE"].tolist() if not options["brands"].empty else []

    scoped = metadata_subset(ctx["metadata_df"], column_names)
    meta_records = []
    if not scoped.empty:
        for _, row in scoped.iterrows():
            meta_records.append(
                {
                    "column": str(row.get("column") or ""),
                    "attribute": str(row.get("attribute") or ""),
                    "result_type": str(row.get("result_type") or ""),
                    "jobs": list(row.get("jobs") or []),
                    "prompt_groups": list(row.get("prompt_groups") or []),
                    "validation_enabled": bool(row.get("validation_enabled")),
                    "drift_eligible": bool(row.get("drift_eligible")),
                    "allow_blank": bool(row.get("allow_blank")),
                    "value_mode": str(row.get("value_mode") or ""),
                    "expected_display": str(row.get("expected_display") or ""),
                    "data_type": ctx["attr_types"].get(str(row.get("column") or ""), "TEXT"),
                }
            )
    payload["metadata"] = meta_records
    payload["default_jobs"] = [j for j in ["SERVICE", "RETENTION", "ACQUISITION"] if j in payload["jobs"]]
    return payload


def _resolve_request(qs: dict):
    source_table = (qs.get("source") or [dfm.DEFAULT_OPTIC_TABLE])[0]
    ctx = get_context(source_table)
    start_dt, end_dt = parse_dates(qs)
    grain = (qs.get("grain") or ["day"])[0]
    jobs = parse_list((qs.get("jobs") or [""])[0])
    brands = parse_list((qs.get("brands") or [""])[0])
    attrs = parse_list((qs.get("attrs") or [""])[0])
    return source_table, ctx, start_dt, end_dt, grain, jobs, brands, attrs


def api_live(qs: dict) -> dict:
    src, ctx, start_dt, end_dt, grain, jobs, brands, attrs = _resolve_request(qs)
    if not ctx.get("live_ok"):
        return {"error": ctx.get("error") or "Snowflake unavailable"}
    date_col = ctx["date_col"]
    job_col, brand_col = ctx["job_col"], ctx["brand_col"]
    conn_col, dur_col = ctx["connection_col"], ctx["duration_col"]
    attr_types = ctx["attr_types"]
    attr_jobs = attribute_jobs_by_column(ctx.get("metadata_df"))

    summary = dfm.fetch_live_summary(
        src, date_col, start_dt, end_dt, job_col, jobs, brand_col, brands, conn_col, dur_col
    )
    kpis: dict = {}
    deltas: dict = {}
    if not summary.empty:
        row = summary.iloc[0]
        window_days = max((end_dt - start_dt).days, 0)
        prev_end = start_dt - timedelta(days=1)
        prev_start = prev_end - timedelta(days=window_days)
        prev = dfm.fetch_live_summary(
            src, date_col, prev_start, prev_end, job_col, jobs, brand_col, brands, conn_col, dur_col
        )
        prow = prev.iloc[0] if not prev.empty else None

        def num(v):
            return None if pd.isna(v) else float(v)

        kpis = {
            "rows": int(row["TOTAL_ROWS"]),
            "connections": None if pd.isna(row["DISTINCT_CONNECTIONS"]) else int(row["DISTINCT_CONNECTIONS"]),
            "jobs": None if pd.isna(row["JOB_COUNT"]) else int(row["JOB_COUNT"]),
            "brands": None if pd.isna(row["BRAND_COUNT"]) else int(row["BRAND_COUNT"]),
            "avg_duration": num(row["AVG_DURATION"]),
            "first_interaction": None if pd.isna(row["FIRST_INTERACTION"]) else pd.to_datetime(row["FIRST_INTERACTION"]).isoformat(),
            "latest_interaction": None if pd.isna(row["LATEST_INTERACTION"]) else pd.to_datetime(row["LATEST_INTERACTION"]).isoformat(),
        }
        # staleness
        days_stale = None
        try:
            latest_dt = pd.to_datetime(row["LATEST_INTERACTION"])
            if pd.notna(latest_dt):
                if getattr(latest_dt, "tzinfo", None) is not None:
                    latest_dt = latest_dt.tz_localize(None)
                days_stale = max((pd.Timestamp(end_dt) - latest_dt.normalize()).days, 0)
        except Exception:  # noqa: BLE001
            days_stale = None
        kpis["days_stale"] = days_stale

        def pct(curr, key):
            if prow is None:
                return None
            try:
                p = prow.get(key)
            except Exception:  # noqa: BLE001
                p = None
            if p is None or pd.isna(p) or float(p) == 0 or curr is None:
                return None
            return (float(curr) - float(p)) / float(p) * 100.0

        deltas = {
            "rows": pct(kpis["rows"], "TOTAL_ROWS"),
            "connections": pct(kpis["connections"], "DISTINCT_CONNECTIONS"),
        }

    volume = dfm.fetch_volume_trend(src, date_col, start_dt, end_dt, grain, job_col, jobs, brand_col, brands)
    by_job = dfm.fetch_dimension_counts(src, date_col, start_dt, end_dt, job_col, job_col, jobs, brand_col, brands, 12) if job_col else pd.DataFrame()
    by_brand = dfm.fetch_dimension_counts(src, date_col, start_dt, end_dt, brand_col, job_col, jobs, brand_col, brands, 12) if brand_col else pd.DataFrame()

    quality = dfm.fetch_attribute_quality(
        src, date_col, start_dt, end_dt, job_col, jobs, brand_col, brands, attrs, attr_types, attr_jobs
    )
    coverage_score = float(quality["populated_rate"].mean()) if not quality.empty and "populated_rate" in quality else None
    recency_score = None if kpis.get("days_stale") is None else max(0.0, 1.0 - max(kpis["days_stale"] - 1, 0) / 7.0)
    if coverage_score is not None and recency_score is not None:
        health = 0.6 * coverage_score + 0.4 * recency_score
    elif coverage_score is not None:
        health = coverage_score
    else:
        health = recency_score

    return {
        "kpis": kpis,
        "deltas": deltas,
        "health": {"score": health, "coverage": coverage_score, "recency": recency_score},
        "volume": df_records(volume),
        "by_job": df_records(by_job),
        "by_brand": df_records(by_brand),
        "quality": df_records(quality),
    }


def api_drift(qs: dict) -> dict:
    src, ctx, start_dt, end_dt, grain, jobs, brands, attrs = _resolve_request(qs)
    if not ctx.get("live_ok"):
        return {"error": ctx.get("error") or "Snowflake unavailable"}
    ts = dfm.fetch_attribute_time_series(
        src, ctx["date_col"], start_dt, end_dt, grain,
        ctx["job_col"], jobs, ctx["brand_col"], brands, attrs, ctx["attr_types"],
        attribute_jobs_by_column(ctx.get("metadata_df")),
    )
    return {"series": df_records(ts)}


def api_value_spikes(qs: dict) -> dict:
    src, ctx, start_dt, end_dt, grain, jobs, brands, attrs = _resolve_request(qs)
    if not ctx.get("live_ok"):
        return {"error": ctx.get("error") or "Snowflake unavailable"}
    metadata_df = ctx["metadata_df"]
    attr_types = ctx["attr_types"]

    spec_json = dfm.value_distribution_specs(attrs, attr_types, metadata_df, jobs)
    dist = dfm.fetch_expected_value_distribution(
        src, ctx["date_col"], start_dt, end_dt, grain,
        ctx["job_col"], jobs, ctx["brand_col"], brands, spec_json,
    )
    expected_lookup = dfm.value_spike_expected_lookup(metadata_df)
    dist_records = []
    if not dist.empty:
        for _, r in dist.iterrows():
            attr = str(r["attribute"]).upper()
            bucket = dfm.bucket_value_for_spikes(attr, r["raw_value"], expected_lookup)
            dist_records.append({
                "period": pd.to_datetime(r["period"]).date().isoformat() if pd.notna(r["period"]) else None,
                "attribute": attr,
                "raw_value": str(r["raw_value"]),
                "bucket": bucket,
                "row_count": int(r["row_count"]),
            })
    result_types = dfm.infer_value_result_types(dist, attr_types)

    positive = dfm.fetch_positive_flag_rates(
        src, ctx["date_col"], start_dt, end_dt, grain,
        ctx["job_col"], jobs, ctx["brand_col"], brands, spec_json,
    )
    pos_records = []
    if not positive.empty:
        for _, r in positive.iterrows():
            pos_records.append({
                "period": pd.to_datetime(r["period"]).date().isoformat() if pd.notna(r["period"]) else None,
                "attribute": str(r["attribute"]).upper(),
                "total_calls": int(r["total_calls"]),
                "positive_rows": int(r["positive_rows"]),
                "positive_rate": float(r["positive_rate"]),
            })
    return {"distribution": dist_records, "positive": pos_records, "result_types": result_types}


def api_expected_drift(qs: dict) -> dict:
    src, ctx, start_dt, end_dt, grain, jobs, brands, attrs = _resolve_request(qs)
    if not ctx.get("live_ok"):
        return {"error": ctx.get("error") or "Snowflake unavailable"}
    metadata_df = ctx["metadata_df"]
    attr_types = ctx["attr_types"]
    spec_json = dfm.expected_spec_records(metadata_df, attrs, attr_types)
    specs = json.loads(spec_json)
    if not specs:
        return {"summary": [], "note": "No drift-eligible attributes in the current selection."}
    value_df = dfm.fetch_expected_value_distribution(
        src, ctx["date_col"], start_dt, end_dt, grain,
        ctx["job_col"], jobs, ctx["brand_col"], brands, spec_json,
    )
    scored, summary = dfm.score_expected_outputs(value_df, metadata_df)

    # Per-attribute breakdown of the actual unexpected output values + counts.
    def unexpected_box(grp: pd.DataFrame) -> dict:
        total_rows = int(grp["row_count"].sum())
        bad = grp[~grp["is_expected"]]
        unexpected_rows = int(bad["row_count"].sum())
        values = []
        if not bad.empty:
            agg = (
                bad.groupby("raw_value", dropna=False)["row_count"].sum()
                .sort_values(ascending=False)
            )
            for raw_value, cnt in agg.items():
                rv = str(raw_value)
                values.append({
                    "value": rv,
                    "count": int(cnt),
                    "share": (int(cnt) / total_rows) if total_rows else 0.0,
                    "is_blank": dfm.normalize_match_key(raw_value) == dfm.BLANK_VALUE,
                })
        return {
            "total_rows": total_rows,
            "unexpected_rows": unexpected_rows,
            "distinct_unexpected": len(values),
            "values": values,
        }

    def period_key(value) -> str:
        return pd.to_datetime(value).date().isoformat() if pd.notna(value) else ""

    unexpected: dict[str, dict] = {}
    unexpected_by_period: dict[str, dict[str, dict]] = {}
    if scored is not None and not scored.empty:
        for attr, grp in scored.groupby("attribute", dropna=False):
            attr_l = str(attr)
            unexpected[attr_l] = unexpected_box(grp)
        for (attr, period), grp in scored.groupby(["attribute", "period"], dropna=False):
            attr_l = str(attr)
            unexpected_by_period.setdefault(attr_l, {})[period_key(period)] = unexpected_box(grp)

    records = []
    if not summary.empty:
        for _, r in summary.iterrows():
            records.append({
                "period": pd.to_datetime(r["period"]).date().isoformat() if pd.notna(r["period"]) else None,
                "attribute": str(r["attribute"]),
                "total_rows": int(r["total_rows"]),
                "expected_rows": int(r["expected_rows"]),
                "unexpected_rows": int(r["unexpected_rows"]),
                "blank_rows": int(r["blank_rows"]),
                "expected_rate": float(r["expected_rate"]),
                "unexpected_rate": float(r["unexpected_rate"]),
                "blank_rate": float(r["blank_rate"]),
                "severity": str(r["severity"]),
                "expected_outputs": str(r.get("expected_outputs") or ""),
                "jobs": str(r.get("jobs") or ""),
                "result_type": str(r.get("result_type") or ""),
            })
    return {
        "summary": records,
        "attr_count": len(specs),
        "unexpected": unexpected,
        "unexpected_by_period": unexpected_by_period,
    }


def api_metadata(qs: dict) -> dict:
    source_table = (qs.get("source") or [dfm.DEFAULT_OPTIC_TABLE])[0]
    ctx = get_context(source_table)
    meta = ctx.get("metadata_df")
    if meta is None or meta.empty:
        return {"rows": [], "source": ctx.get("metadata_source", "")}
    column_names = {c["name"] for c in ctx.get("columns", [])}
    rows = []
    for _, row in meta.iterrows():
        rows.append({
            "attribute": str(row.get("attribute") or ""),
            "column": str(row.get("column") or ""),
            "result_type": str(row.get("result_type") or ""),
            "jobs": ", ".join(row.get("jobs") or []),
            "prompt_groups": ", ".join(row.get("prompt_groups") or []),
            "expected_display": str(row.get("expected_display") or ""),
            "validation_enabled": bool(row.get("validation_enabled")),
            "allow_blank": bool(row.get("allow_blank")),
            "value_mode": str(row.get("value_mode") or ""),
            "in_table": str(row.get("column") or "") in column_names,
            "description": str(row.get("description") or ""),
            "prompt": str(row.get("prompt") or ""),
        })
    return {"rows": rows, "source": ctx.get("metadata_source", ""), "table_columns": sorted(column_names)}


def api_explorer(qs: dict) -> dict:
    src, ctx, start_dt, end_dt, grain, jobs, brands, attrs = _resolve_request(qs)
    if not ctx.get("live_ok"):
        return {"error": ctx.get("error") or "Snowflake unavailable"}
    limit = int((qs.get("limit") or ["100"])[0])
    date_col = ctx["date_col"]
    base_cols = [c for c in [date_col, ctx["job_col"], ctx["brand_col"], ctx["connection_col"]] if c]
    cols = list(dict.fromkeys(base_cols + attrs))
    if not cols:
        cols = [date_col]
    df = dfm.fetch_sample_rows(
        src, date_col, start_dt, end_dt, ctx["job_col"], jobs, ctx["brand_col"], brands, cols, limit
    )
    out = df.where(pd.notna(df), None)
    return {
        "columns": list(out.columns),
        "rows": [[(_json_default(v) if isinstance(v, (datetime, date, pd.Timestamp, Decimal)) else v) for v in rec]
                 for rec in out.itertuples(index=False, name=None)],
    }


def api_top_values(qs: dict) -> dict:
    src, ctx, start_dt, end_dt, grain, jobs, brands, attrs = _resolve_request(qs)
    if not ctx.get("live_ok"):
        return {"error": ctx.get("error") or "Snowflake unavailable"}
    attr = (qs.get("attr") or [""])[0].upper()
    if not attr:
        return {"values": []}
    effective_jobs = effective_jobs_for_attribute(attr, ctx.get("metadata_df"), jobs)
    if effective_jobs is None:
        return {"values": [], "attr": attr, "data_type": ctx["attr_types"].get(attr, "TEXT")}
    dtype = ctx["attr_types"].get(attr, "TEXT")
    limit = int((qs.get("limit") or ["25"])[0])
    df = dfm.fetch_top_values(
        src, ctx["date_col"], start_dt, end_dt, ctx["job_col"], effective_jobs, ctx["brand_col"], brands, attr, dtype, limit
    )
    return {"values": df_records(df), "attr": attr, "data_type": dtype}


ROUTES = {
    "/api/bootstrap": api_bootstrap,
    "/api/live": api_live,
    "/api/drift": api_drift,
    "/api/value-spikes": api_value_spikes,
    "/api/expected-drift": api_expected_drift,
    "/api/metadata": api_metadata,
    "/api/explorer": api_explorer,
    "/api/top-values": api_top_values,
}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # quieter logs
        sys.stderr.write("[api] " + (fmt % args) + "\n")

    def _send(
        self,
        code: int,
        body: bytes,
        content_type: str,
        filename: str | None = None,
    ):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        if filename:
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.end_headers()
        self.wfile.write(body)

    def _send_model_lab(self, response: ApiResponse):
        return self._send(
            response.status,
            response.body,
            response.content_type,
            response.filename,
        )

    def _health_guard(self, write=False):
        try:
            if len(self.headers.get_all("Host", [])) != 1:
                raise HealthError("Exactly one local Host header is required.", 403)
            if write and len(self.headers.get_all("Origin", [])) != 1:
                raise HealthError("Exactly one same-origin Origin header is required.", 403)
            local_request(self.client_address[0], self.headers.get("Host"), self.server.server_port,
                          self.headers.get("Origin"), write, self.headers.get("Sec-Fetch-Site"))
            return True
        except HealthError as exc:
            self.close_connection = True
            self._send_model_lab(ApiResponse.json({"error": str(exc)}, exc.status))
            return False

    def _health_response(self, callback):
        try:
            return self._send_model_lab(callback(get_health_service()))
        except Exception:
            return self._send_model_lab(ApiResponse.json(
                {"error": "Attribute Health local state is unavailable. No inference was launched."}, 503))

    def do_GET(self):  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if path in ("/", "/index.html"):
            html = (WEB_DIR / "OPTIC_Console.html").read_bytes()
            return self._send(200, html, "text/html; charset=utf-8")

        if path in ("/model-lab", "/model-lab/", "/model-lab/index.html"):
            html = (WEB_DIR / "OPTIC_Model_Lab.html").read_bytes()
            return self._send(200, html, "text/html; charset=utf-8")

        if path in ("/attribute-health", "/attribute-health/", "/attribute-health/index.html"):
            if not self._health_guard():
                return
            html = (WEB_DIR / "OPTIC_Attribute_Health.html").read_bytes()
            return self._send(200, html, "text/html; charset=utf-8")

        if path.startswith("/api/attribute-health/"):
            if not self._health_guard():
                return
            return self._health_response(lambda service: service.handle_get(path, qs))

        if path == "/vendor/echarts.min.js":
            f = VENDOR_DIR / "echarts.min.js"
            if f.exists():
                return self._send(200, f.read_bytes(), "application/javascript; charset=utf-8")
            return self._send(404, b"missing", "text/plain")

        if path.startswith("/api/model-lab/"):
            return self._send_model_lab(MODEL_LAB.handle_get(path, qs))

        if path == "/api/clear-cache":
            clear_caches()
            with _ctx_lock:
                _ctx_cache.clear()
            return self._send(200, b'{"ok":true}', "application/json")

        if path in ROUTES:
            try:
                result = sanitize(ROUTES[path](qs))
                body = json.dumps(result, default=_json_default, allow_nan=False).encode("utf-8")
                return self._send(200, body, "application/json; charset=utf-8")
            except Exception as exc:  # noqa: BLE001
                traceback.print_exc()
                body = json.dumps({"error": str(exc)}).encode("utf-8")
                return self._send(500, body, "application/json; charset=utf-8")

        return self._send(404, b"Not found", "text/plain")

    def do_POST(self):  # noqa: N802
        path = urlparse(self.path).path
        if path.startswith("/api/attribute-health/"):
            if not self._health_guard(write=True):
                return
            try:
                if urlparse(self.path).query:
                    raise HealthError("Write endpoints do not accept URL parameters.")
                if self.headers.get("Transfer-Encoding"):
                    raise HealthError("Transfer encoding is not supported.")
                if len(self.headers.get_all("Content-Length", [])) != 1:
                    raise HealthError("Exactly one Content-Length is required.")
                length = int(self.headers.get("Content-Length", "0"))
                if length < 1 or length > MAX_BODY:
                    raise HealthError("JSON body is missing or too large.", 413)
                if self.headers.get("Content-Type", "").split(";", 1)[0].strip().lower() != "application/json":
                    raise HealthError("Content-Type must be application/json.", 415)
                if len(self.headers.get_all("Idempotency-Key", [])) != 1:
                    raise HealthError("Exactly one Idempotency-Key is required.")
                payload = strict_json(self.rfile.read(length))
            except (HealthError, ValueError) as exc:
                self.close_connection = True
                return self._send_model_lab(ApiResponse.json(
                    {"error": str(exc) if isinstance(exc, HealthError) else "Invalid Content-Length."},
                    exc.status if isinstance(exc, HealthError) else 400))
            return self._health_response(lambda service: service.handle_post(
                path, payload, self.headers.get("Idempotency-Key")))
        if not path.startswith("/api/model-lab/"):
            return self._send(404, b"Not found", "text/plain")
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            return self._send(
                400,
                b'{"error":"Invalid Content-Length."}',
                "application/json; charset=utf-8",
            )
        if length < 1 or length > MAX_JSON_BODY_BYTES:
            return self._send(
                413,
                b'{"error":"JSON body is missing or too large."}',
                "application/json; charset=utf-8",
            )
        try:
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return self._send(
                400,
                b'{"error":"Request body must be valid JSON."}',
                "application/json; charset=utf-8",
            )
        return self._send_model_lab(MODEL_LAB.handle_post(path, payload))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8520)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    url = f"http://{args.host}:{args.port}"
    print(f"OPTIC Console serving at {url}")
    print("Reusing Dashboard_Final.py logic against Snowflake (key-pair auth).")
    if not args.no_browser:
        threading.Thread(target=lambda: (time.sleep(1.0), webbrowser.open(url)), daemon=True).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.shutdown()


if __name__ == "__main__":
    main()
