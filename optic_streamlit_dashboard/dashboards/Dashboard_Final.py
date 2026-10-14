from __future__ import annotations

import base64
import glob
import hmac
import ast
import json
import os
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

try:
    import plotly.express as px
    import plotly.graph_objects as go
    import plotly.io as pio

    PLOTLY_AVAILABLE = True

    _OPTIC_PLOT_FONT = "Inter, -apple-system, Segoe UI, sans-serif"
    _OPTIC_PALETTE = [
        "#6366f1", "#06b6d4", "#a855f7", "#ec4899",
        "#10b981", "#f59e0b", "#ef4444", "#3b82f6",
        "#14b8a6", "#f472b6",
    ]
    pio.templates["optic_dark"] = go.layout.Template(
        layout=go.Layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(15,23,42,0.35)",
            font=dict(family=_OPTIC_PLOT_FONT, color="#e2e8f0", size=12),
            title=dict(font=dict(family=_OPTIC_PLOT_FONT, color="#f8fafc", size=15), x=0.01),
            colorway=_OPTIC_PALETTE,
            margin=dict(l=14, r=14, t=44, b=14),
            xaxis=dict(
                gridcolor="rgba(148,163,184,0.10)",
                zerolinecolor="rgba(148,163,184,0.18)",
                linecolor="rgba(148,163,184,0.25)",
                tickfont=dict(color="#94a3b8"),
                title_font=dict(color="#cbd5e1"),
            ),
            yaxis=dict(
                gridcolor="rgba(148,163,184,0.10)",
                zerolinecolor="rgba(148,163,184,0.18)",
                linecolor="rgba(148,163,184,0.25)",
                tickfont=dict(color="#94a3b8"),
                title_font=dict(color="#cbd5e1"),
            ),
            legend=dict(
                bgcolor="rgba(15,23,42,0.5)",
                bordercolor="rgba(148,163,184,0.18)",
                borderwidth=1,
                font=dict(color="#e2e8f0"),
            ),
            hoverlabel=dict(
                bgcolor="rgba(15,23,42,0.95)",
                bordercolor="rgba(99,102,241,0.6)",
                font=dict(family=_OPTIC_PLOT_FONT, color="#f8fafc"),
            ),
        )
    )
    pio.templates.default = "plotly_dark+optic_dark"
    px.defaults.template = "plotly_dark+optic_dark"
    px.defaults.color_discrete_sequence = _OPTIC_PALETTE
except ImportError:  # pragma: no cover - fallback is exercised in local smoke tests
    px = None
    go = None
    pio = None
    PLOTLY_AVAILABLE = False

try:
    import snowflake.connector
except ImportError:  # pragma: no cover - handled in the Streamlit UI
    snowflake = None

try:
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization
except ImportError:  # pragma: no cover - handled in the Streamlit UI
    default_backend = None
    serialization = None


APP_DIR = Path(__file__).resolve().parent
APP_ROOT = APP_DIR.parent
DEFAULT_OUTPUT_ROOT = Path(os.getenv("OPTIC_DASHBOARD_OUTPUT_ROOT", APP_ROOT / "output"))
CONFIG_DIR = Path(os.getenv("OPTIC_DASHBOARD_CONFIG_DIR", APP_ROOT / "config"))

DEFAULT_OPTIC_TABLE = os.getenv(
    "OPTIC_DASHBOARD_SOURCE_TABLE",
    "RETAIL_REVENUE_PRD.INTEGRATED.VERTEX_OPTIC_AGENTINTERACTION_DATA",
)
DEFAULT_RAW_TABLE = os.getenv(
    "OPTIC_DASHBOARD_RAW_TABLE",
    "RETAIL_PRD.LANDPOWER_ODS.ODIN_DBO_OPTICRESULTS",
)
DEFAULT_ATTRIBUTE_TABLE = os.getenv(
    "OPTIC_DASHBOARD_ATTRIBUTE_TABLE",
    "RETAIL_PRD.LANDPOWER_ODS.ODIN_OPTIC_ATTRIBUTE",
)
# Consolidated ODIN view pairing every job with its configured attribute prompt
# details (attribute, description, result type/options, prompt). This is the live
# source of truth for the OPTIC Console: a single query supplies both the attribute
# definitions and the authoritative JOBNAME -> attribute mapping.
DEFAULT_PROMPTDETAILS_VIEW = os.getenv(
    "OPTIC_DASHBOARD_PROMPTDETAILS_VIEW",
    "RETAIL_PRD.LANDPOWER_ODS.VW_ODIN_OPTIC_PROMPTDETAILS",
)
DOWNLOADS_DIR = Path.home() / "Downloads"


def default_config_file(filename: str) -> Path:
    bundled = CONFIG_DIR / filename
    return bundled if bundled.exists() else DOWNLOADS_DIR / filename


DEFAULT_PROMPT_PATHS = {
    "ACQUISITION": Path(
        os.getenv(
            "OPTIC_DASHBOARD_ACQUISITION_PROMPT",
            default_config_file("Acquisition_attributes_prompt_20260602.json"),
        )
    ),
    "RETENTION": Path(
        os.getenv(
            "OPTIC_DASHBOARD_RETENTION_PROMPT",
            default_config_file("Retention_attributes_prompt_20260602.json"),
        )
    ),
    "SERVICE": Path(
        os.getenv(
            "OPTIC_DASHBOARD_SERVICE_PROMPT",
            default_config_file("Service_attributes_prompt_20260602.json"),
        )
    ),
}
DEFAULT_ATTRIBUTES_XLSX = Path(
    os.getenv(
        "OPTIC_DASHBOARD_ATTRIBUTES_XLSX",
        default_config_file("Attributes_06022026_1425.xlsx"),
    )
)

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "SVC_MARTECH_PRD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "vistra.us-east-1.privatelink")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "RTL_PRD_WHS")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "RETAIL_PRD")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "USERDB_MKT")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_PRIVATE_KEY_SECRET = os.getenv(
    "SNOWFLAKE_PRIVATE_KEY_SECRET",
    "SNOWFLAKE_PRIVATE_KEY_PEM",
)
SNOWFLAKE_PRIVATE_KEY_PATH = os.getenv(
    "SNOWFLAKE_PRIVATE_KEY_PATH",
    str(
        Path.home()
        / "OneDrive - Vistra Corp"
        / "Desktop"
        / "SVC_MARTECH_PRD_PEM_Private_Key.pem"
    ),
)
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")

EMPTY_TEXT_VALUES = ("", "NOT_FOUND", "NONE", "NULL", "N/A", "[]", "UNKNOWN")
BLANK_VALUE = "<BLANK>"
REVIEW_FILENAME = "step1_review_sheet_with_evidence.json"
AUTOSAVE_FILENAME = "reviewed_values.json"
DONE_FILENAME = "reviewed_done.json"
REVIEW_STATE_BACKEND = os.getenv("OPTIC_DASHBOARD_REVIEW_STATE_BACKEND", "file").strip().lower()
REVIEW_STATE_TABLE = os.getenv("OPTIC_DASHBOARD_REVIEW_TABLE", "").strip()

METRIC_COLS = [
    "overall_accuracy",
    "boolean_accuracy",
    "categorical_semantic",
    "string_semantic",
    "list_jaccard",
    "strict_blank_rate",
    "false_fill_rate",
    "cost_per_transcript",
    "e2e_latency_avg",
]
METRIC_LABELS = {
    "overall_accuracy": "Overall Accuracy",
    "boolean_accuracy": "Boolean Accuracy",
    "categorical_semantic": "Categorical Semantic",
    "string_semantic": "String Semantic",
    "list_jaccard": "List Jaccard",
    "strict_blank_rate": "Strict Blank Rate",
    "false_fill_rate": "False Fill Rate",
    "cost_per_transcript": "Cost / Transcript",
    "e2e_latency_avg": "E2E Latency",
}

MODEL_COLORS = {
    "sonnet-4-5": "#1f77b4",
    "nova-lite2": "#d62728",
    "gpt-oss-20b": "#2ca02c",
    "haiku-4-5": "#9467bd",
}

DIMENSION_COLUMNS = {
    "ID",
    "RUNDATE",
    "SOURCEID",
    "CONNID",
    "CONNECTION_ID",
    "JOBTYPE",
    "JOBNAME",
    "BRAND",
    "ACCOUNT",
    "CUSTOMER_ID",
    "CUSTOMERPHONE",
    "DNIS",
    "INTERACTIONDATE",
    "INTERACTIONDATEONLY",
    "CREATEDDATE",
    "INSTRUCTIONDATE",
    "START_TS",
    "END_TS",
    "CALL_START_TS",
    "ODINCALLSUMMARY",
    "SUMMARY",
    "ODINDATA",
    "RESULTJSON",
    "CALLDATA",
    "OTHERDATA",
    "CONTENTLENGTH",
    "DATASETNAME",
    "PROMPTTOKENS",
    "COMPLETIONTOKENS",
    "MODEL",
    "PROMPTVERSION",
    "AGENTREFERENCEID",
    "AGENTNAME",
    "AGENTONCALL",
    "AGENTID",
    "SITE",
    "LOB",
    "FULL_NAME",
    "CHTV_FULL_NAME",
    "CHTV_LOB",
    "CHTV_SITE",
    "CHTV_HIRE_DT",
    "DURATION",
    "EXTERNALTALKPERCENT",
    "INTERNALTALKPERCENT",
    "SILENCEPERCENT",
    "MEDIATYPE",
    "CALLERBI",
    "CALLERNEED",
}

PREFERRED_ATTRIBUTES = [
    "ODIN_LIKELY_PUC_COMPLAINT",
    "RAGE_FACTOR",
    "ISSUE_UNRESOLVED",
    "SALE_CLOSED",
    "TRUE_OPPORTUNITY",
    "CUSTOMER_SWITCH_INTENT_BO",
    "TRANSFER_OUT",
    "POWER_TO_CHOOSE",
    "PRICE_SENSITIVITY",
    "PLAN_AWARENESS",
    "PLANS_OFFERED",
    "INTENT_LIST",
    "AGENT_SENTIMENT",
    "CALL_TYPE",
    "SALE_OBJECTION_REASONING",
    "PUC_RISK_REASONING",
]

VALUE_SPIKE_DEFAULT_ATTRIBUTES = [
    "PUC_RISK_REASONING",
    "PUC_RISK_VERBATIM",
    "DO_NOT_CONTACT_VERBATIM",
    "DO_NOT_CONTACT",
    "ISSUE_UNRESOLVED",
    "COMPETITOR_TRUE_OFFER",
    "TABOO_TOPICS",
    "TABOO_REASONING",
    "UNAUTHORIZED_FORCE_OUT_BO",
    "UNAUTHORIZED_FORCE_OUT_ACTION_BO",
    "CUSTOMER_INSULT_VERBATIM",
    "AUDITORY_VERBATIM",
]

EXPECTED_OUTPUT_JOB_OVERRIDES = {
    "retention_effort": ["RETENTION"],
}

EXPECTED_OUTPUT_OVERRIDES = {
    "act_now_offer": ["0", "50", "100"],
    "agent_sentiment": ["NEGATIVE", "NEUTRAL", "POSITIVE"],
    "awareness": [
        "Store",
        "Online",
        "Friends/family",
        "Mail",
        "Email",
        "Advertisement",
        "Door-to-door",
        "Social Media",
        "Solar installer",
        "Other",
    ],
    "call_type": [
        "Renewal Opportunity",
        "Non-Renewal Plan Evaluation",
        "High Bill",
        "Stop/Move Service",
        "Competitor Offer",
        "Shopping",
        "Service Reconnection",
        "Payment Arrangement",
        "Other",
    ],
    "call_type_acq": ["Move-In Opportunity", "Switch-In Opportunity", "Generic Opportunity", "Other"],
    "call_type_svc": [
        "Payment Arrangement",
        "Account Maintenance",
        "High Bill",
        "Reconnect after Disconnect for Non-Pay",
        "Service Reconnection",
        "Power Outage",
        "Other",
    ],
    "callback_risk": ["Very Unlikely", "Unlikely", "Neutral", "Likely", "Very Likely"],
    "caretaker": ["Parent", "Child", "Friend", "Other Relative"],
    "confidence_level": ["High", "Medium", "Low"],
    "customer_confusion": ["Low", "Moderate", "High"],
    "customer_satisfaction": ["Satisfied", "Dissatisfied", "Neutral"],
    "customer_sentiment_end": ["Positive", "Negative", "Neutral"],
    "customer_sentiment_start": ["Positive", "Negative", "Neutral"],
    "defection_risk": ["Very High", "High", "Moderate", "Low", "Very Low"],
    "green_pitched": ["Solar Club", "GreenUp", "Solar Buyback Plans", "Connected Conservation"],
    "home_warranty_pitched": [
        "TXU Surge Protect Max",
        "TXU Tech Protect",
        "TXU AC & Heat Protect",
        "TXU Plumbing Protect Ultra",
        "TXU MyHome Protect Ultra",
        "TXU MyHome Appliances Protect",
        "TXU MyHome Systems Protect",
    ],
    "igl_event_type": [
        "INADVERTENT_GAIN",
        "INADVERTENT_LOSS",
        "FORCE_OUT",
        "RESCISSION_GAIN",
        "RESCISSION_LOSS",
        "LOIE_NO_POWER",
        "NO_IGL_RELATED",
        "UNCLEAR",
    ],
    "igl_expected_action": [
        "IAG_CASE",
        "IAL_CASE",
        "FORCE_OUT_CASE",
        "RESCISSION_GAIN_CASE",
        "CONTACT_OTHER_REP_FOR_RESCISSION",
        "LOIE",
        "CUSTOMER_SUCCESS_ENROLLMENT",
        "NO_IGL_ACTION",
        "UNCLEAR",
    ],
    "move_reconsideration": ["Likely", "Unlikely", "Neutral"],
    "plan_awareness": [
        "In-Store",
        "Online",
        "Friends/family",
        "Mail",
        "Email",
        "Advertisement",
        "Door-to-door",
        "Social Media",
        "Other",
    ],
    "plan_usage_connection": ["Good", "Poor"],
    "preferred_term": ["Longer", "Shorter", "Neither"],
    "premise_type": ["Single-Family", "Multi-Family", "Other", "Undisclosed"],
    "price_sensitivity": ["High", "Moderate", "Low"],
    "retention_effort": ["Awful", "Poor", "Average", "Good", "Excellent"],
}

EXPECTED_OUTPUT_ALIASES = {
    "agent_sentiment": {
        "POSITIVE": "POSITIVE",
        "NEUTRAL": "NEUTRAL",
        "NEGATIVE": "NEGATIVE",
    },
    "customer_sentiment_start": {
        "POSITIVE": "Positive",
        "NEUTRAL": "Neutral",
        "NEGATIVE": "Negative",
    },
    "customer_sentiment_end": {
        "POSITIVE": "Positive",
        "NEUTRAL": "Neutral",
        "NEGATIVE": "Negative",
        "DISSATISFIED": "Negative",
        "SATISFIED": "Positive",
    },
    "customer_satisfaction": {
        "SATISFIED": "Satisfied",
        "DISSATISFIED": "Dissatisfied",
        "NEUTRAL": "Neutral",
    },
    "home_warranty_pitched": {
        "TXU TECHPROTECT": "TXU Tech Protect",
        "TECH PROTECT": "TXU Tech Protect",
        "AC AND HEAT PROTECT": "TXU AC & Heat Protect",
        "TXU AC AND HEAT PROTECT": "TXU AC & Heat Protect",
        "A C HEAT PROTECT": "TXU AC & Heat Protect",
        "TXU MYHOME APPLIANCE PROTECT": "TXU MyHome Appliances Protect",
    },
    "igl_event_type": {
        "NOT_RELATED": "NO_IGL_RELATED",
        "NOT RELATED": "NO_IGL_RELATED",
        "NO IGL RELATED": "NO_IGL_RELATED",
        "NO_IGL_RELATED": "NO_IGL_RELATED",
        "LOIE": "LOIE_NO_POWER",
        "LOIE NO POWER": "LOIE_NO_POWER",
        "LOIE/NO POWER": "LOIE_NO_POWER",
        "INADVERTENT GAIN": "INADVERTENT_GAIN",
        "INADVERTENT_GAIN": "INADVERTENT_GAIN",
        "INADVERTENT LOSS": "INADVERTENT_LOSS",
        "INADVERTENT_LOSS": "INADVERTENT_LOSS",
        "FORCE OUT": "FORCE_OUT",
        "FORCE_OUT": "FORCE_OUT",
        "RESCISSION GAIN": "RESCISSION_GAIN",
        "RESCISSION_GAIN": "RESCISSION_GAIN",
        "RESCISSION LOSS": "RESCISSION_LOSS",
        "RESCISSION_LOSS": "RESCISSION_LOSS",
        "UNCLEAR": "UNCLEAR",
    },
}

DRIFT_ELIGIBLE_RESULT_TYPES = {"BOOL", "BOOLEMPTY", "BOOLEAN", "STRINGPICKONE", "STRINGMULTI"}
PROMPT_FINITE_RESULT_TYPES = DRIFT_ELIGIBLE_RESULT_TYPES | {"LIST"}
FREEFORM_RESULT_TYPES = {"STRING", "TEXT", "FREEFORM"}
TEXT_PRESENCE_VALUE_MODE = "TEXT_PRESENCE_3_WORDS"


st.set_page_config(
    page_title="OPTIC Production Dashboard",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)


class NativeChart:
    """Minimal chart object used when Plotly is unavailable."""

    def __init__(
        self,
        kind: str,
        data_frame: pd.DataFrame,
        x: str,
        y: str,
        color: str | None = None,
        orientation: str | None = None,
        **_: Any,
    ):
        self.kind = kind
        self.data_frame = data_frame.copy()
        self.x = x
        self.y = y
        self.color = color
        self.orientation = orientation

    def update_layout(self, *_: Any, **__: Any) -> "NativeChart":
        return self


class NativePlotlyExpress:
    @staticmethod
    def line(data_frame: pd.DataFrame, x: str, y: str, color: str | None = None, **kwargs: Any) -> NativeChart:
        return NativeChart("line", data_frame, x, y, color=color, **kwargs)

    @staticmethod
    def bar(
        data_frame: pd.DataFrame,
        x: str,
        y: str,
        color: str | None = None,
        orientation: str | None = None,
        **kwargs: Any,
    ) -> NativeChart:
        return NativeChart("bar", data_frame, x, y, color=color, orientation=orientation, **kwargs)


if not PLOTLY_AVAILABLE:
    px = NativePlotlyExpress()
    _streamlit_plotly_chart = st.plotly_chart

    def _plotly_chart_fallback(fig: Any, *args: Any, **kwargs: Any) -> None:
        if not isinstance(fig, NativeChart):
            _streamlit_plotly_chart(fig, *args, **kwargs)
            return

        chart_df = fig.data_frame.copy()
        if chart_df.empty or fig.x not in chart_df or fig.y not in chart_df:
            st.info("No chart data available.")
            return

        if fig.kind == "line":
            if fig.color and fig.color in chart_df:
                native_df = chart_df.pivot_table(
                    index=fig.x,
                    columns=fig.color,
                    values=fig.y,
                    aggfunc="sum",
                ).sort_index()
            else:
                native_df = chart_df.set_index(fig.x)[[fig.y]].sort_index()
            st.line_chart(native_df)
            return

        if fig.kind == "bar":
            if fig.orientation == "h":
                native_df = chart_df.set_index(fig.y)[[fig.x]]
            else:
                native_df = chart_df.set_index(fig.x)[[fig.y]]
            st.bar_chart(native_df)
            return

        st.dataframe(chart_df, use_container_width=True, hide_index=True)

    st.plotly_chart = _plotly_chart_fallback


def require_dashboard_auth() -> None:
    """Use Amazon's auth hook when present, otherwise support a simple env secret."""
    try:
        from dashboards.auth import require_auth

        require_auth()
        return
    except Exception:
        pass

    configured_password = os.getenv("DASHBOARD_PASSWORD") or _secret("DASHBOARD_PASSWORD")
    if not configured_password:
        st.sidebar.warning("Dashboard auth is not configured.")
        return

    if st.session_state.get("_optic_dashboard_auth_ok"):
        return

    with st.sidebar:
        st.subheader("Sign In")
        submitted_password = st.text_input("Dashboard password", type="password")
        if st.button("Sign in", use_container_width=True):
            if hmac.compare_digest(submitted_password or "", configured_password):
                st.session_state["_optic_dashboard_auth_ok"] = True
                st.rerun()
            st.error("Invalid password")
    st.stop()


def _secret(name: str, default: str | None = None) -> str | None:
    try:
        value = st.secrets.get(name, default)
        return str(value) if value is not None else default
    except Exception:
        return default


def css() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap');

        :root {
            --bg-0: #070912;
            --bg-1: #0c1020;
            --bg-2: #111728;
            --panel: rgba(23, 27, 42, 0.72);
            --panel-strong: rgba(23, 27, 42, 0.92);
            --border: rgba(148, 163, 184, 0.12);
            --border-strong: rgba(148, 163, 184, 0.25);
            --text-0: #f8fafc;
            --text-1: #cbd5e1;
            --text-2: #94a3b8;
            --muted: #64748b;
            --accent-1: #6366f1;
            --accent-2: #06b6d4;
            --accent-3: #a855f7;
            --success: #10b981;
            --warning: #f59e0b;
            --danger: #ef4444;
            --grad-primary: linear-gradient(135deg, #6366f1 0%, #06b6d4 100%);
            --grad-warm: linear-gradient(135deg, #a855f7 0%, #ec4899 100%);
            --grad-success: linear-gradient(135deg, #10b981 0%, #06b6d4 100%);
            --shadow-1: 0 8px 30px rgba(0, 0, 0, 0.35);
            --shadow-glow: 0 0 32px rgba(99, 102, 241, 0.22);
        }

        html, body, [class*="css"], .stApp, .stMarkdown, .stText, p, span, div, label, input, textarea, button {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif !important;
            -webkit-font-smoothing: antialiased;
        }
        code, pre, .stCode, [data-testid="stCodeBlock"] * {
            font-family: 'JetBrains Mono', 'SF Mono', Consolas, monospace !important;
        }
        /* Restore Streamlit's Material Symbols icon font (sidebar toggle, etc.) */
        [class*="material-symbols"],
        [class*="material-icons"],
        span[data-testid="stIconMaterial"],
        .material-symbols-outlined,
        .material-symbols-rounded,
        .material-symbols-sharp,
        .material-icons {
            font-family: 'Material Symbols Outlined', 'Material Symbols Rounded', 'Material Icons' !important;
            font-feature-settings: 'liga';
            font-variation-settings: 'FILL' 0, 'wght' 400, 'GRAD' 0, 'opsz' 24;
            -webkit-font-feature-settings: 'liga';
            text-transform: none !important;
            letter-spacing: normal !important;
            word-wrap: normal !important;
            white-space: nowrap !important;
            direction: ltr !important;
        }

        .stApp {
            background:
                radial-gradient(1100px 600px at 8% -10%, rgba(99,102,241,0.16) 0%, transparent 55%),
                radial-gradient(1000px 500px at 100% 0%, rgba(6,182,212,0.12) 0%, transparent 55%),
                radial-gradient(900px 700px at 50% 110%, rgba(168,85,247,0.10) 0%, transparent 60%),
                linear-gradient(180deg, var(--bg-0) 0%, var(--bg-1) 100%) !important;
            color: var(--text-0);
        }

        .block-container {
            padding-top: 1.2rem !important;
            padding-bottom: 4rem !important;
            max-width: 1500px;
            animation: fadeUp 0.55s cubic-bezier(0.22, 1, 0.36, 1);
        }

        @keyframes fadeUp {
            from { opacity: 0; transform: translateY(14px); }
            to   { opacity: 1; transform: translateY(0); }
        }
        @keyframes fadeIn {
            from { opacity: 0; }
            to   { opacity: 1; }
        }
        @keyframes shimmer {
            0%   { background-position: -200% 0; }
            100% { background-position: 200% 0; }
        }
        @keyframes pulseDot {
            0%, 100% { box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.55); }
            50%      { box-shadow: 0 0 0 8px rgba(16, 185, 129, 0); }
        }
        @keyframes pulseDotRed {
            0%, 100% { box-shadow: 0 0 0 0 rgba(239, 68, 68, 0.55); }
            50%      { box-shadow: 0 0 0 8px rgba(239, 68, 68, 0); }
        }
        @keyframes floaty {
            0%, 100% { transform: translateY(0); }
            50%      { transform: translateY(-3px); }
        }
        @keyframes gradientShift {
            0%   { background-position: 0% 50%; }
            50%  { background-position: 100% 50%; }
            100% { background-position: 0% 50%; }
        }

        /* ============ Hero header ============ */
        .optic-hero {
            position: relative;
            margin: 0 0 1.5rem 0;
            padding: 28px 32px;
            border-radius: 18px;
            background: linear-gradient(135deg, rgba(99,102,241,0.18) 0%, rgba(6,182,212,0.12) 50%, rgba(168,85,247,0.16) 100%);
            background-size: 200% 200%;
            animation: gradientShift 14s ease infinite, fadeUp 0.7s cubic-bezier(0.22, 1, 0.36, 1);
            border: 1px solid var(--border-strong);
            box-shadow: var(--shadow-1), var(--shadow-glow);
            overflow: hidden;
        }
        .optic-hero::before {
            content: "";
            position: absolute; inset: 0;
            background:
                radial-gradient(600px 200px at 0% 0%, rgba(99,102,241,0.25), transparent 60%),
                radial-gradient(500px 180px at 100% 100%, rgba(6,182,212,0.20), transparent 60%);
            pointer-events: none;
        }
        .optic-hero-row {
            position: relative; z-index: 1;
            display: flex; align-items: center; justify-content: space-between;
            gap: 18px; flex-wrap: wrap;
        }
        .optic-hero-title {
            display: flex; align-items: center; gap: 16px;
        }
        .optic-hero-logo {
            width: 54px; height: 54px;
            border-radius: 14px;
            display: grid; place-items: center;
            background: var(--grad-primary);
            box-shadow: 0 12px 30px rgba(99,102,241,0.45);
            animation: floaty 5s ease-in-out infinite;
            font-weight: 800; font-size: 1.2rem; color: white;
            letter-spacing: 0.5px;
        }
        .optic-hero h1 {
            font-size: 1.85rem !important;
            font-weight: 800 !important;
            letter-spacing: -0.02em;
            margin: 0 !important;
            background: linear-gradient(90deg, #ffffff 0%, #cbd5e1 100%);
            -webkit-background-clip: text; background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        .optic-hero-sub {
            color: var(--text-2);
            font-size: 0.95rem;
            margin-top: 4px;
        }
        .optic-hero-meta {
            display: flex; align-items: center; gap: 10px; flex-wrap: wrap;
        }

        /* Hide the default st.title that we replace with the hero */
        h1.optic-fallback-title { display: none; }

        /* ============ Status pill ============ */
        .status-pill {
            display: inline-flex; align-items: center; gap: 8px;
            border: 1px solid var(--border-strong);
            border-radius: 999px;
            padding: 6px 14px;
            color: var(--text-1);
            background: rgba(15, 23, 42, 0.55);
            backdrop-filter: blur(8px);
            font-size: 0.82rem; font-weight: 500;
            transition: all 0.25s ease;
        }
        .status-pill:hover { transform: translateY(-1px); border-color: rgba(99,102,241,0.5); }
        .status-pill .dot {
            width: 8px; height: 8px; border-radius: 50%;
            background: var(--success);
            animation: pulseDot 2s infinite;
        }
        .status-pill.offline .dot {
            background: var(--danger);
            animation: pulseDotRed 2s infinite;
        }
        .status-pill code {
            background: rgba(0,0,0,0.35);
            padding: 2px 6px; border-radius: 6px;
            color: #cbd5e1; font-size: 0.78rem;
        }

        .small-note {
            color: var(--text-2);
            font-size: 0.88rem; line-height: 1.45;
        }

        /* ============ Metrics ============ */
        div[data-testid="stMetric"] {
            position: relative;
            border: 1px solid var(--border);
            border-radius: 14px;
            padding: 16px 18px;
            background: linear-gradient(180deg, rgba(23,27,42,0.85) 0%, rgba(15,18,30,0.85) 100%);
            backdrop-filter: blur(12px);
            box-shadow: 0 4px 18px rgba(0,0,0,0.25);
            transition: transform 0.25s ease, box-shadow 0.25s ease, border-color 0.25s ease;
            overflow: hidden;
        }
        div[data-testid="stMetric"]::before {
            content: ""; position: absolute; left: 0; top: 0; bottom: 0;
            width: 3px;
            background: var(--grad-primary);
            opacity: 0.85;
        }
        div[data-testid="stMetric"]::after {
            content: ""; position: absolute; inset: 0;
            background: radial-gradient(400px 80px at 0% 0%, rgba(99,102,241,0.10), transparent 60%);
            pointer-events: none; opacity: 0; transition: opacity 0.3s ease;
        }
        div[data-testid="stMetric"]:hover {
            transform: translateY(-3px);
            border-color: rgba(99,102,241,0.45);
            box-shadow: 0 14px 36px rgba(0,0,0,0.4), 0 0 0 1px rgba(99,102,241,0.25);
        }
        div[data-testid="stMetric"]:hover::after { opacity: 1; }

        div[data-testid="stMetric"] label,
        div[data-testid="stMetricLabel"],
        div[data-testid="stMetricLabel"] p {
            color: var(--text-2) !important;
            font-size: 0.78rem !important;
            font-weight: 500 !important;
            text-transform: uppercase;
            letter-spacing: 0.06em;
        }
        div[data-testid="stMetricValue"],
        div[data-testid="stMetricValue"] div {
            color: var(--text-0) !important;
            font-size: 1.7rem !important;
            font-weight: 700 !important;
            letter-spacing: -0.01em;
        }
        div[data-testid="stMetricDelta"],
        div[data-testid="stMetricDelta"] div {
            color: var(--success) !important;
            font-weight: 600 !important;
        }

        /* ============ Sidebar ============ */
        section[data-testid="stSidebar"] {
            background: linear-gradient(180deg, rgba(8,11,22,0.92) 0%, rgba(12,16,32,0.92) 100%) !important;
            border-right: 1px solid var(--border);
            backdrop-filter: blur(12px);
        }
        section[data-testid="stSidebar"] > div { padding-top: 0.6rem; }
        section[data-testid="stSidebar"] h1,
        section[data-testid="stSidebar"] h2,
        section[data-testid="stSidebar"] h3 {
            color: var(--text-0) !important;
            font-weight: 700 !important;
            letter-spacing: -0.01em;
        }
        section[data-testid="stSidebar"] .stMarkdown p { color: var(--text-1); }
        section[data-testid="stSidebar"] hr {
            border: none;
            height: 1px;
            background: linear-gradient(90deg, transparent, var(--border-strong), transparent);
            margin: 1rem 0;
        }

        /* ============ Tabs ============ */
        div[data-testid="stTabs"] {
            background: rgba(15, 23, 42, 0.35);
            border: 1px solid var(--border);
            border-radius: 14px;
            padding: 6px;
            backdrop-filter: blur(10px);
            margin-bottom: 1rem;
        }
        div[data-testid="stTabs"] [role="tablist"] {
            gap: 4px;
            border-bottom: none !important;
        }
        div[data-testid="stTabs"] [role="tab"] {
            background: transparent !important;
            color: var(--text-2) !important;
            border: 1px solid transparent !important;
            border-radius: 10px !important;
            padding: 10px 18px !important;
            font-weight: 500 !important;
            font-size: 0.9rem !important;
            transition: all 0.22s ease !important;
            position: relative;
        }
        div[data-testid="stTabs"] [role="tab"]:hover {
            color: var(--text-0) !important;
            background: rgba(99,102,241,0.10) !important;
            border-color: var(--border) !important;
        }
        div[data-testid="stTabs"] [role="tab"][aria-selected="true"] {
            color: white !important;
            background: var(--grad-primary) !important;
            border-color: transparent !important;
            box-shadow: 0 6px 20px rgba(99,102,241,0.35);
        }
        div[data-testid="stTabs"] [role="tabpanel"] {
            animation: fadeIn 0.4s ease;
            padding-top: 1rem;
        }

        /* ============ Buttons ============ */
        .stButton > button,
        .stDownloadButton > button,
        .stFormSubmitButton > button {
            background: var(--grad-primary) !important;
            color: white !important;
            border: none !important;
            border-radius: 10px !important;
            padding: 0.5rem 1.1rem !important;
            font-weight: 600 !important;
            letter-spacing: 0.01em;
            box-shadow: 0 6px 18px rgba(99,102,241,0.28);
            transition: transform 0.18s ease, box-shadow 0.22s ease, filter 0.22s ease !important;
        }
        .stButton > button:hover,
        .stDownloadButton > button:hover,
        .stFormSubmitButton > button:hover {
            transform: translateY(-2px);
            filter: brightness(1.08);
            box-shadow: 0 12px 26px rgba(99,102,241,0.45);
        }
        .stButton > button:active { transform: translateY(0); }

        /* Secondary button look for sidebar refresh, etc. */
        section[data-testid="stSidebar"] .stButton > button {
            background: rgba(99,102,241,0.12) !important;
            color: var(--text-0) !important;
            border: 1px solid rgba(99,102,241,0.35) !important;
            box-shadow: none;
        }
        section[data-testid="stSidebar"] .stButton > button:hover {
            background: rgba(99,102,241,0.22) !important;
            border-color: rgba(99,102,241,0.6) !important;
        }

        /* ============ Inputs ============ */
        .stTextInput input,
        .stNumberInput input,
        .stTextArea textarea,
        .stDateInput input,
        div[data-baseweb="select"] > div {
            background: rgba(15, 23, 42, 0.6) !important;
            border: 1px solid var(--border-strong) !important;
            color: var(--text-0) !important;
            border-radius: 10px !important;
            transition: border-color 0.2s ease, box-shadow 0.2s ease, background 0.2s ease !important;
        }
        .stTextInput input:focus,
        .stNumberInput input:focus,
        .stTextArea textarea:focus,
        .stDateInput input:focus {
            border-color: var(--accent-1) !important;
            box-shadow: 0 0 0 3px rgba(99,102,241,0.18) !important;
            outline: none !important;
        }
        .stSlider > div > div > div { background: var(--grad-primary) !important; }
        .stSlider [role="slider"] {
            box-shadow: 0 0 0 4px rgba(99,102,241,0.25) !important;
        }

        /* ============ Expanders ============ */
        details[data-testid="stExpander"], div[data-testid="stExpander"] {
            border: 1px solid var(--border) !important;
            border-radius: 12px !important;
            background: rgba(15, 23, 42, 0.45) !important;
            backdrop-filter: blur(8px);
            overflow: hidden;
            transition: border-color 0.22s ease, box-shadow 0.22s ease;
        }
        details[data-testid="stExpander"]:hover,
        div[data-testid="stExpander"]:hover {
            border-color: rgba(99,102,241,0.35) !important;
        }
        details[data-testid="stExpander"] summary { padding: 0.6rem 0.9rem !important; }

        /* ============ DataFrames ============ */
        div[data-testid="stDataFrame"], div[data-testid="stTable"] {
            border: 1px solid var(--border);
            border-radius: 12px;
            overflow: hidden;
            background: rgba(15, 23, 42, 0.35);
            backdrop-filter: blur(6px);
            transition: border-color 0.22s ease;
        }
        div[data-testid="stDataFrame"]:hover { border-color: rgba(99,102,241,0.3); }

        /* ============ Alerts / info / warning / error ============ */
        div[data-testid="stAlert"] {
            border-radius: 12px !important;
            border: 1px solid var(--border-strong) !important;
            backdrop-filter: blur(8px);
            animation: fadeUp 0.4s ease;
        }

        /* ============ Headings ============ */
        h1, h2, h3, h4 {
            color: var(--text-0) !important;
            letter-spacing: -0.015em;
        }
        h2 { font-weight: 700 !important; }
        h3 { font-weight: 600 !important; }
        .stMarkdown h2,
        .stMarkdown h3,
        .stMarkdown h4 {
            position: relative;
            padding-left: 14px;
        }
        .stMarkdown h2::before,
        .stMarkdown h3::before {
            content: ""; position: absolute; left: 0; top: 8px; bottom: 8px;
            width: 4px; border-radius: 3px;
            background: var(--grad-primary);
        }

        /* ============ Captions and small text ============ */
        [data-testid="stCaptionContainer"], .stCaption {
            color: var(--text-2) !important;
        }

        /* ============ Spinner ============ */
        div[data-testid="stSpinner"] > div > i {
            border-top-color: var(--accent-1) !important;
            border-right-color: var(--accent-2) !important;
        }

        /* ============ Scrollbars ============ */
        ::-webkit-scrollbar { width: 10px; height: 10px; }
        ::-webkit-scrollbar-track { background: rgba(15,23,42,0.4); }
        ::-webkit-scrollbar-thumb {
            background: linear-gradient(180deg, rgba(99,102,241,0.5), rgba(6,182,212,0.5));
            border-radius: 10px;
            border: 2px solid transparent;
            background-clip: padding-box;
        }
        ::-webkit-scrollbar-thumb:hover {
            background: linear-gradient(180deg, rgba(99,102,241,0.8), rgba(6,182,212,0.8));
            background-clip: padding-box;
        }

        /* Reduce Streamlit's top toolbar visual weight */
        header[data-testid="stHeader"] {
            background: transparent !important;
            backdrop-filter: blur(8px);
        }
        #MainMenu, footer { visibility: hidden; }

        /* Code blocks polished */
        pre, .stCodeBlock, [data-testid="stCodeBlock"] {
            background: rgba(8, 11, 22, 0.7) !important;
            border: 1px solid var(--border) !important;
            border-radius: 10px !important;
        }

        /* Multiselect chips */
        span[data-baseweb="tag"] {
            background: rgba(99,102,241,0.18) !important;
            border: 1px solid rgba(99,102,241,0.4) !important;
            color: var(--text-0) !important;
            border-radius: 999px !important;
        }

        /* Radio + checkboxes */
        .stRadio label, .stCheckbox label { color: var(--text-1) !important; }

        /* Section divider helper */
        .optic-divider {
            height: 1px;
            background: linear-gradient(90deg, transparent, var(--border-strong), transparent);
            margin: 1.25rem 0;
            border: none;
        }

        /* ============ Section header ============ */
        .optic-section { margin: 0.5rem 0 0.95rem 0; animation: fadeUp 0.45s ease; }
        .optic-section .eyebrow {
            display: inline-flex; align-items: center; gap: 9px;
            text-transform: uppercase; letter-spacing: 0.16em;
            font-size: 0.68rem; font-weight: 700; color: var(--accent-2);
        }
        .optic-section .eyebrow::before {
            content: ""; width: 20px; height: 2px; border-radius: 2px;
            background: var(--grad-primary);
        }
        .optic-section h3.optic-section-title {
            margin: 7px 0 3px 0 !important; padding-left: 0 !important;
            font-size: 1.18rem !important; font-weight: 700 !important;
            color: var(--text-0) !important; letter-spacing: -0.015em;
        }
        .optic-section h3.optic-section-title::before { display: none !important; }
        .optic-section .optic-section-sub {
            color: var(--text-2); font-size: 0.86rem; line-height: 1.5; max-width: 78ch;
        }

        /* ============ Panel card ============ */
        .optic-card {
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 18px 20px;
            background: linear-gradient(180deg, rgba(23,27,42,0.7) 0%, rgba(15,18,30,0.7) 100%);
            backdrop-filter: blur(10px);
            box-shadow: 0 4px 18px rgba(0,0,0,0.22);
        }

        /* ============ Severity / status badges ============ */
        .badge-row { display: flex; gap: 10px; flex-wrap: wrap; margin: 2px 0 14px 0; }
        .sev-badge {
            display: inline-flex; align-items: center; gap: 7px;
            padding: 5px 13px; border-radius: 999px;
            font-size: 0.78rem; font-weight: 600; border: 1px solid transparent;
            transition: transform 0.18s ease;
        }
        .sev-badge:hover { transform: translateY(-1px); }
        .sev-badge .bdot { width: 7px; height: 7px; border-radius: 50%; }
        .sev-badge.high   { background: rgba(239,68,68,0.16);  color: #fca5a5; border-color: rgba(239,68,68,0.42); }
        .sev-badge.high .bdot   { background: #ef4444; }
        .sev-badge.medium { background: rgba(245,158,11,0.16); color: #fcd34d; border-color: rgba(245,158,11,0.42); }
        .sev-badge.medium .bdot { background: #f59e0b; }
        .sev-badge.low    { background: rgba(99,102,241,0.16); color: #a5b4fc; border-color: rgba(99,102,241,0.42); }
        .sev-badge.low .bdot    { background: #6366f1; }
        .sev-badge.ok     { background: rgba(16,185,129,0.16); color: #6ee7b7; border-color: rgba(16,185,129,0.42); }
        .sev-badge.ok .bdot     { background: #10b981; }

        /* ============ Empty state ============ */
        .optic-empty {
            border: 1px dashed var(--border-strong);
            border-radius: 16px;
            padding: 36px 22px; text-align: center;
            background: rgba(15,23,42,0.32);
            color: var(--text-2); animation: fadeIn 0.4s ease;
        }
        .optic-empty .optic-empty-icon {
            font-size: 1.7rem; margin-bottom: 10px; opacity: 0.85;
            background: var(--grad-primary);
            -webkit-background-clip: text; background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        .optic-empty .optic-empty-title { color: var(--text-1); font-weight: 600; font-size: 1.02rem; margin-bottom: 5px; }
        .optic-empty .optic-empty-msg { font-size: 0.86rem; max-width: 52ch; margin: 0 auto; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def decode_private_key_secret(raw_secret: str) -> bytes:
    candidate = raw_secret.strip().strip("'\"").replace("\\n", "\n")
    if "-----BEGIN" in candidate:
        return candidate.encode("utf-8")

    compact = "".join(candidate.split())
    try:
        return base64.b64decode(compact, validate=True)
    except Exception:
        return candidate.encode("utf-8")


def read_private_key_material() -> bytes | None:
    secret_value = os.getenv(SNOWFLAKE_PRIVATE_KEY_SECRET) or _secret(SNOWFLAKE_PRIVATE_KEY_SECRET)
    if secret_value:
        return decode_private_key_secret(secret_value)

    key_path = Path(SNOWFLAKE_PRIVATE_KEY_PATH)
    if key_path.exists():
        return key_path.read_bytes()
    return None


def load_private_key_der() -> bytes:
    if serialization is None:
        raise RuntimeError("cryptography is required for Snowflake key-pair auth.")

    key_material = read_private_key_material()
    if not key_material:
        raise RuntimeError(
            f"No Snowflake private key found. Set {SNOWFLAKE_PRIVATE_KEY_SECRET} "
            "or SNOWFLAKE_PRIVATE_KEY_PATH."
        )

    password = (
        SNOWFLAKE_PRIVATE_KEY_PASSPHRASE.encode("utf-8")
        if SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
        else None
    )
    load_kwargs: dict[str, Any] = {"password": password}
    if default_backend is not None:
        load_kwargs["backend"] = default_backend()

    errors: list[str] = []
    for loader in (serialization.load_pem_private_key, serialization.load_der_private_key):
        try:
            key_obj = loader(key_material, **load_kwargs)
            return key_obj.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        except Exception as exc:
            errors.append(str(exc))
    raise RuntimeError("Failed to parse Snowflake private key. " + " | ".join(errors))


def create_snowflake_connection(**overrides: Any):
    if snowflake is None:
        raise RuntimeError("snowflake-connector-python is not installed.")

    password = os.getenv("SNOWFLAKE_PASSWORD") or _secret("SNOWFLAKE_PASSWORD")
    connect_kwargs: dict[str, Any] = {
        "user": os.getenv("SNOWFLAKE_USER", SNOWFLAKE_USER),
        "account": os.getenv("SNOWFLAKE_ACCOUNT", SNOWFLAKE_ACCOUNT),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", SNOWFLAKE_WAREHOUSE),
        "database": os.getenv("SNOWFLAKE_DATABASE", SNOWFLAKE_DATABASE),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", SNOWFLAKE_SCHEMA),
    }
    role = os.getenv("SNOWFLAKE_ROLE", SNOWFLAKE_ROLE or "")
    if role:
        connect_kwargs["role"] = role

    key_material = read_private_key_material()
    if key_material:
        connect_kwargs["private_key"] = load_private_key_der()
    elif password:
        connect_kwargs["password"] = password
    else:
        raise RuntimeError(
            f"Set {SNOWFLAKE_PRIVATE_KEY_SECRET}, SNOWFLAKE_PRIVATE_KEY_PATH, "
            "or SNOWFLAKE_PASSWORD for Snowflake access."
        )

    connect_kwargs.update({k: v for k, v in overrides.items() if v is not None})
    return snowflake.connector.connect(**connect_kwargs)


@st.cache_data(ttl=300, show_spinner=False)
def run_query(sql: str) -> pd.DataFrame:
    conn = create_snowflake_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            columns = [desc[0].upper() for desc in cur.description]
            return pd.DataFrame(rows, columns=columns)
        finally:
            cur.close()
    finally:
        conn.close()


def qident(identifier: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", identifier or ""):
        raise ValueError(f"Unsafe Snowflake identifier: {identifier!r}")
    return f'"{identifier.upper()}"'


def fq_table_name(table_name: str) -> str:
    parts = [part.strip().strip('"') for part in table_name.split(".") if part.strip()]
    if len(parts) != 3:
        raise ValueError("Use a fully qualified table name: DATABASE.SCHEMA.TABLE")
    return ".".join(qident(part) for part in parts)


def split_table_name(table_name: str) -> tuple[str, str, str]:
    parts = [part.strip().strip('"') for part in table_name.split(".") if part.strip()]
    if len(parts) != 3:
        raise ValueError("Use a fully qualified table name: DATABASE.SCHEMA.TABLE")
    return parts[0].upper(), parts[1].upper(), parts[2].upper()


def sibling_table_name(table_name: str, sibling: str) -> str:
    database, schema, _ = split_table_name(table_name)
    return ".".join(qident(part) for part in (database, schema, sibling))


def sql_quote(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def sql_in(values: list[str]) -> str:
    return ", ".join(sql_quote(v) for v in values)


def where_clause(
    date_col: str,
    start_date: date,
    end_date: date,
    job_col: str | None,
    jobs: list[str],
    brand_col: str | None,
    brands: list[str],
) -> str:
    clauses = [
        f"{qident(date_col)} >= {sql_quote(start_date)}::DATE",
        f"{qident(date_col)} < DATEADD(day, 1, {sql_quote(end_date)}::DATE)",
    ]
    if job_col and jobs:
        clauses.append(f"UPPER({qident(job_col)}) IN ({sql_in([j.upper() for j in jobs])})")
    if brand_col and brands:
        clauses.append(f"UPPER({qident(brand_col)}) IN ({sql_in([b.upper() for b in brands])})")
    return " AND ".join(clauses)


def column_records(df: pd.DataFrame) -> list[dict[str, str]]:
    if df.empty:
        return []
    return [
        {"name": str(row["COLUMN_NAME"]).upper(), "type": str(row["DATA_TYPE"]).upper()}
        for _, row in df.iterrows()
    ]


@st.cache_data(ttl=900, show_spinner=False)
def get_table_columns(table_name: str) -> pd.DataFrame:
    database, schema, table = split_table_name(table_name)
    sql = f"""
    SELECT column_name, data_type, ordinal_position
    FROM {qident(database)}.information_schema.columns
    WHERE table_schema = {sql_quote(schema)}
      AND table_name = {sql_quote(table)}
    ORDER BY ordinal_position
    """
    return run_query(sql)


def find_first_column(columns: list[dict[str, str]], candidates: list[str]) -> str | None:
    names = {col["name"] for col in columns}
    for candidate in candidates:
        if candidate.upper() in names:
            return candidate.upper()
    return None


def candidate_attribute_columns(columns: list[dict[str, str]]) -> list[str]:
    allowed_types = {"BOOLEAN", "TEXT", "VARCHAR", "STRING", "ARRAY", "NUMBER", "FLOAT", "DOUBLE", "DECIMAL"}
    attrs: list[str] = []
    for col in columns:
        name = col["name"]
        dtype = col["type"]
        if name in DIMENSION_COLUMNS or name.startswith("CHTV_"):
            continue
        if dtype in allowed_types:
            attrs.append(name)
    return attrs


def default_attribute_selection(available: list[str]) -> list[str]:
    selected = [attr for attr in PREFERRED_ATTRIBUTES if attr in available]
    for attr in available:
        if len(selected) >= 8:
            break
        if attr not in selected:
            selected.append(attr)
    return selected[:8]


def column_type_lookup(columns: list[dict[str, str]]) -> dict[str, str]:
    return {col["name"]: col["type"] for col in columns}


def clean_prompt_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).replace("\r\n", "\n").strip()


def normalize_match_key(value: Any) -> str:
    if value is None or pd.isna(value):
        return BLANK_VALUE
    text = str(value).strip().strip("'\"")
    if not text:
        return BLANK_VALUE
    if text.upper() in {BLANK_VALUE, "<NULL>", "<NULL_OR_EMPTY>"}:
        return BLANK_VALUE
    text = re.sub(r"\s+", " ", text)
    text = text.rstrip(".")
    text = text.replace("&", " AND ")
    text = re.sub(r"[_-]+", " ", text)
    text = re.sub(r"[^A-Za-z0-9/ ]+", "", text)
    return re.sub(r"\s+", " ", text).strip().upper() or BLANK_VALUE


def normalize_prompt_quotes(prompt: str) -> str:
    return (
        str(prompt or "")
        .replace("\u201c", '"')
        .replace("\u201d", '"')
        .replace("\u2018", "'")
        .replace("\u2019", "'")
    )


def extract_quoted_prompt_values(prompt: str, include_blank: bool = False) -> list[str]:
    prompt_n = normalize_prompt_quotes(prompt)
    values: list[str] = []
    patterns = [
        r"\"([^\"]{0,120})\"",
        r"(?<![A-Za-z0-9])'([^']{0,120})'(?![A-Za-z0-9])",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, prompt_n):
            value = match.group(1).strip()
            if value or include_blank:
                values.append(value)
    return values


def prompt_allows_blank(prompt: str, result_type: str = "") -> bool:
    prompt_n = normalize_prompt_quotes(prompt)
    prompt_l = prompt_n.lower()
    if prompt_n.strip():
        negated_blank_patterns = [
            r"\bnever\s+return\s+(?:''|\"\"|blank|a blank|an empty string|empty string|nothing|null|n/a|none|unknown)",
            r"\bdo\s+not\s+return\s+(?:''|\"\"|blank|a blank|an empty string|empty string|nothing|null|n/a|none|unknown)",
            r"\bdon't\s+return\s+(?:''|\"\"|blank|a blank|an empty string|empty string|nothing|null|n/a|none|unknown)",
            r"\bmust\s+not\s+return\s+(?:''|\"\"|blank|a blank|an empty string|empty string|nothing|null|n/a|none|unknown)",
        ]
        if any(re.search(pattern, prompt_l) for pattern in negated_blank_patterns):
            return False
        return bool(
            re.search(r"\breturn\s+(?:''|\"\"|blank|a blank|an empty string|empty string|nothing)(?:\b|[^\w])", prompt_l)
            or re.search(r"\boutput\s+(?:exactly\s+)?(?:''|\"\"|blank|a blank|an empty string|empty string|nothing)(?:\b|[^\w])", prompt_l)
            or re.search(r"\b(?:or|as|outputs?:)\s*(?:''|\"\")", prompt_l)
            or "return a blank value" in prompt_l
            or "leave blank" in prompt_l
            or "completely blank" in prompt_l
            or "must be blank" in prompt_l
            or "if none" in prompt_l and "blank" in prompt_l
            or "if not applicable" in prompt_l and ("''" in prompt_l or '""' in prompt_l)
        )
    return "BOOLEMPTY" in (result_type or "").upper()


def prompt_requires_boolean(prompt: str, result_type: str = "") -> bool:
    prompt_n = normalize_prompt_quotes(prompt)
    prompt_l = prompt_n.lower()
    if prompt_n.strip():
        quoted = {value.strip() for value in extract_quoted_prompt_values(prompt_n, include_blank=True)}
        nonblank = {value for value in quoted if value}
        explicit_bool_list = nonblank == {"1", "0"} and bool(
            re.search(r"\b(?:allowed outputs?|valid outputs?|return|indicate|classify)\b", prompt_l)
        )
        has_bool_values = {"1", "0"}.issubset(nonblank)
        has_bool_list_context = has_bool_values and bool(
            re.search(r"\b(?:return exactly one of these|only allowed outputs?|allowed outputs?|valid outputs?)\b", prompt_l)
            or re.search(r"\bas\s+['\"]?1['\"]?\s*,?\s*['\"]?0['\"]?", prompt_l)
        )
        has_indicate_bool_pair = bool(
            re.search(
                r"\b(?:indicate|return|output|use|classify)\b[^.\n]{0,160}"
                r"['\"]?1['\"]?\s*(?:or|/|and)\s*['\"]?0['\"]?",
                prompt_l,
            )
        )
        has_bool_return_sentences = bool(
            re.search(r"\breturn\s+(?:'1'|\"1\"|1)(?:\b|[^\w])", prompt_l)
            and re.search(r"\breturn\s+(?:'0'|\"0\"|0)(?:\b|[^\w])", prompt_l)
        )
        return bool(
            explicit_bool_list
            or has_bool_list_context
            or has_indicate_bool_pair
            or has_bool_return_sentences
            or re.search(
                r"\b(?:indicate|return|output|use|classify)\b[^.\n]{0,160}"
                r"(?:'1'|\"1\"|1)\s*(?:or|/|and)\s*(?:'0'|\"0\"|0)\b",
                prompt_l,
            )
            or re.search(
                r"\breturn\b[^.\n]{0,220}\b1\b[^.\n]{0,220}\botherwise\b[^.\n]{0,120}\b0\b",
                prompt_l,
            )
            or re.search(
                r"\breturn\b[^.\n]{0,120}\b0\b[^.\n]{0,220}\botherwise\b[^.\n]{0,120}\b1\b",
                prompt_l,
            )
        )
    return (result_type or "").upper() in {"BOOL", "BOOLEMPTY", "BOOLEAN"}


def prompt_has_finite_output_context(prompt: str) -> bool:
    prompt_l = normalize_prompt_quotes(prompt).lower()
    return bool(
        re.search(
            r"\b(?:valid outputs?|valid values?|allowed outputs?|allowed values?|only allowed outputs?)\b",
            prompt_l,
        )
        or re.search(r"\b(?:return exactly one of|exactly one of these|one of these|one of the following)\b", prompt_l)
        or re.search(r"\b(?:from this list|must be one of|return one of|previously defined)\b", prompt_l)
        or re.search(r"\b(?:categorize|classify|indicate|evaluate)[^.\n]{0,260}\bas\b", prompt_l)
    )


def prompt_allows_multiple_outputs(prompt: str) -> bool:
    prompt_l = normalize_prompt_quotes(prompt).lower()
    return bool(
        "comma-separated" in prompt_l
        or "comma separated" in prompt_l
        or "separated by commas" in prompt_l
        or "if multiple" in prompt_l
        or "multiple values" in prompt_l
        or "all that apply" in prompt_l
        or re.search(r"\breturn\s+(?:a\s+)?(?:list|array)\b", prompt_l)
    )


def infer_unquoted_expected_outputs(prompt: str) -> list[str]:
    prompt_n = normalize_prompt_quotes(prompt)
    segments: list[str] = []
    patterns = [
        r"\bvalid [^.]{0,80} include ([^.]+)",
        r"\bfollowing [^:\n]{0,100}:\s*([^.]+)",
        r"\bfrom this list:\s*([^.]+)",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, prompt_n, flags=re.IGNORECASE):
            segments.append(match.group(1))

    outputs: list[str] = []
    for segment in segments:
        cleaned = re.sub(r"\([^)]*\)", "", segment)
        cleaned = re.sub(r"\b(?:and|or)\b", ",", cleaned, flags=re.IGNORECASE)
        for part in cleaned.split(","):
            value = part.strip().strip(" .:;\"'")
            value = re.sub(r"\s+", " ", value)
            if not value or value.lower() in {"blank", "a blank value", "none", "not applicable"}:
                continue
            if len(value.split()) <= 8 and len(value) <= 80:
                outputs.append(value)
    return list(dict.fromkeys(outputs))


def infer_quoted_expected_outputs(prompt: str) -> list[str]:
    if not prompt or not prompt_has_finite_output_context(prompt):
        return []

    quoted = [
        q.strip()
        for q in extract_quoted_prompt_values(prompt, include_blank=False)
        if q.strip() and len(q.split()) <= 12
    ]
    outputs = quoted or infer_unquoted_expected_outputs(prompt)
    outputs = [value for value in outputs if value and normalize_match_key(value) != BLANK_VALUE]
    deduped = list(dict.fromkeys(outputs))
    if 2 <= len(deduped) <= 60:
        return deduped
    return []


def prompt_requests_number(prompt: str, attr: str = "") -> bool:
    prompt_l = normalize_prompt_quotes(prompt).lower()
    attr_l = (attr or "").lower()
    if attr_l.endswith("_amt") or any(token in attr_l for token in ("amount", "kwh", "rate", "interval")):
        return True
    return bool(
        re.search(r"\b(?:count how many|how many times)\b", prompt_l)
        or re.search(
            r"\b(?:return|provide|give|extract|identify)\b[^.\n]{0,120}"
            r"\b(?:number|count|amount|dollar amount|kwh|rate|date|day interval|percentage|percent|score)\b",
            prompt_l,
        )
    )


def normalize_result_type_label(result_type: str) -> str:
    result_type_u = (result_type or "").strip().upper()
    aliases = {
        "BOOLEAN": "BOOL",
        "BOOL_EMPTY": "BOOLEMPTY",
        "STRING_PICK_ONE": "STRINGPICKONE",
        "PICKONE": "STRINGPICKONE",
        "INT": "INTEGER",
        "FLOAT": "NUMBER",
        "DECIMAL": "NUMBER",
        "DOUBLE": "NUMBER",
        "TEXT": "STRING",
    }
    return aliases.get(result_type_u, result_type_u)


def expected_outputs_for_attribute(attr: str, result_type: str, prompt: str) -> tuple[list[str], bool]:
    attr_l = attr.lower()
    allow_blank = prompt_allows_blank(prompt, result_type)

    if attr_l in EXPECTED_OUTPUT_OVERRIDES:
        outputs = list(EXPECTED_OUTPUT_OVERRIDES[attr_l])
    elif prompt_requires_boolean(prompt, ""):
        outputs = ["1", "0"]
    else:
        outputs = infer_quoted_expected_outputs(prompt)
        if not outputs and normalize_result_type_label(result_type) == "STRINGPICKONE":
            outputs = infer_quoted_expected_outputs(prompt)

    outputs = [str(v).strip() for v in outputs if str(v).strip()]
    outputs = list(dict.fromkeys(outputs))
    return outputs, allow_blank


def infer_result_type_from_prompt(attr: str, excel_result_type: str, prompt: str, expected_outputs: list[str], allow_blank: bool) -> str:
    excel_type = normalize_result_type_label(excel_result_type)
    if prompt_requires_boolean(prompt, ""):
        return "BOOLEMPTY" if allow_blank else "BOOL"
    if expected_outputs and prompt_allows_multiple_outputs(prompt):
        return "STRINGMULTI"
    if expected_outputs:
        return "STRINGPICKONE"
    if prompt_allows_multiple_outputs(prompt):
        return "LIST"
    if prompt_requests_number(prompt, attr):
        return "INTEGER" if excel_type == "INTEGER" else "NUMBER"
    if not prompt.strip() and excel_type:
        if excel_type == "BOOLEAN":
            return "BOOL"
        return excel_type
    if excel_type in {"BOOL", "BOOLEMPTY"} and not prompt.strip():
        return excel_type
    if excel_type in {"INTEGER", "NUMBER"} and not prompt.strip():
        return excel_type
    return "STRING"


def result_type_source_label(excel_result_type: str, inferred_result_type: str, prompt: str) -> str:
    excel_type = normalize_result_type_label(excel_result_type)
    inferred = normalize_result_type_label(inferred_result_type)
    if not prompt.strip():
        return "excel fallback" if excel_type else "unknown"
    if not excel_type:
        return "prompt"
    if excel_type != inferred:
        return "prompt override"
    return "prompt confirmed"


def prompt_indicates_text_presence_flag(attr: str, prompt: str, result_type: str, expected_outputs: list[str]) -> bool:
    result_type_u = normalize_result_type_label(result_type)
    if result_type_u not in FREEFORM_RESULT_TYPES:
        return False
    if [v for v in expected_outputs if normalize_match_key(v) != BLANK_VALUE]:
        return False

    prompt_l = normalize_prompt_quotes(prompt).lower()
    attr_l = str(attr or "").lower()
    if not prompt_l.strip():
        return False
    is_reasoning_or_verbatim_attr = any(
        token in attr_l
        for token in ("reasoning", "verbatim", "rationale", "evidence", "explanation")
    )
    implicit_blank = bool(
        is_reasoning_or_verbatim_attr
        and re.search(r"\bif\b[^.\n]{0,220}\breturn\b", prompt_l)
    )
    if not prompt_allows_blank(prompt, result_type) and not implicit_blank:
        return False

    has_text_target = (
        is_reasoning_or_verbatim_attr
        or re.search(r"\b(?:reasoning|reason|rationale|verbatim|quote|excerpt|evidence|explanation|details?|summary|note)\b", prompt_l)
        or re.search(r"\breturn\s+(?:a\s+)?(?:string|text|sentence|phrase|description)\b", prompt_l)
    )
    has_conditional_context = bool(
        re.search(r"\b(?:if|when|where|whether|only if|in cases? where|in the event)\b", prompt_l)
        or re.search(r"\b(?:identify|determine|extract|capture|provide|return)\b", prompt_l)
    )
    has_freeform_return = bool(
        re.search(r"\b(?:return|provide|extract|capture|include|give)\b[^.\n]{0,160}\b(?:reason|reasoning|verbatim|quote|evidence|details?|summary|string|text|phrase|description)\b", prompt_l)
        or re.search(r"\b(?:reason|reasoning|verbatim|quote|evidence|details?|summary|string|text|phrase|description)\b[^.\n]{0,160}\b(?:return|provide|extract|capture|include|give)\b", prompt_l)
    )
    return bool(has_text_target and has_conditional_context and has_freeform_return)


def is_expected_drift_eligible(result_type: str, expected_outputs: list[str], value_mode: str = "") -> bool:
    if (value_mode or "").upper() == TEXT_PRESENCE_VALUE_MODE:
        return True
    result_type_u = normalize_result_type_label(result_type)
    nonblank_expected = [v for v in expected_outputs if normalize_match_key(v) != BLANK_VALUE]
    return result_type_u in PROMPT_FINITE_RESULT_TYPES and bool(nonblank_expected)


def canonicalize_igl_expected_action(value: Any) -> str:
    key = normalize_match_key(value)
    if key == BLANK_VALUE:
        return BLANK_VALUE
    if "UNCLEAR" in key:
        return "UNCLEAR"
    if "NO IGL" in key or "NO ACTION" in key or "NOT RELATED" in key or "NORMAL BILLING" in key:
        return "NO_IGL_ACTION"
    if "LOIE" in key or "NO POWER" in key:
        return "LOIE"
    if "FORCE OUT" in key or "BILLING FORCE OUT" in key:
        return "FORCE_OUT_CASE"
    if "IAG" in key or "INADVERTENT GAIN" in key:
        return "IAG_CASE"
    if "IAL" in key or "INADVERTENT LOSS" in key:
        return "IAL_CASE"
    if "RESCISSION GAIN" in key:
        return "RESCISSION_GAIN_CASE"
    if "OTHER REP" in key and "RESCISSION" in key:
        return "CONTACT_OTHER_REP_FOR_RESCISSION"
    if "CUSTOMER SUCCESS" in key or "ENROLLMENT" in key:
        return "CUSTOMER_SUCCESS_ENROLLMENT"
    return str(value).strip()


def canonicalize_output(attr: str, value: Any, expected_outputs: list[str]) -> str:
    attr_l = attr.lower()
    if value is None or pd.isna(value):
        return BLANK_VALUE
    if isinstance(value, bool):
        return "1" if value else "0"

    text = str(value).strip()
    if not text or text.upper() in EMPTY_TEXT_VALUES or text.upper() in {BLANK_VALUE, "<NULL>", "<NULL_OR_EMPTY>"}:
        return BLANK_VALUE
    if text.lower() in {"true", "yes"}:
        return "1"
    if text.lower() in {"false", "no"}:
        return "0"
    if re.fullmatch(r"\d+\.0+", text):
        text = text.split(".", 1)[0]

    if attr_l == "igl_expected_action":
        return canonicalize_igl_expected_action(text)

    alias = EXPECTED_OUTPUT_ALIASES.get(attr_l, {}).get(normalize_match_key(text))
    if alias:
        return alias

    expected_map = {normalize_match_key(v): v for v in expected_outputs}
    return expected_map.get(normalize_match_key(text), text)


def clean_result_option(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if not text:
        return ""
    text = text.rstrip(",").strip()
    text = text.strip("'\"").strip()
    text = text.rstrip(",").strip()
    text = text.strip("'\"").strip()
    return text


def parse_result_value_options(value: Any) -> list[str]:
    if value is None:
        return []
    try:
        if pd.isna(value):
            return []
    except (TypeError, ValueError):
        pass

    raw = str(value).strip()
    if not raw or raw.upper() in EMPTY_TEXT_VALUES:
        return []

    parsed: Any
    try:
        parsed = json.loads(raw)
    except Exception:
        try:
            parsed = ast.literal_eval(raw)
        except Exception:
            parsed = None

    if isinstance(parsed, list):
        candidates = parsed
    elif isinstance(parsed, dict):
        candidates = parsed.values()
    elif parsed is not None:
        candidates = [parsed]
    else:
        candidates = re.split(r"[\n;|,]+", raw)

    outputs = [clean_result_option(candidate) for candidate in candidates]
    outputs = [output for output in outputs if output and normalize_match_key(output) != BLANK_VALUE]
    return list(dict.fromkeys(outputs))


def result_value_options_allow_blank(value: Any) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass

    raw = str(value).strip()
    if not raw:
        return False

    try:
        parsed = json.loads(raw)
    except Exception:
        try:
            parsed = ast.literal_eval(raw)
        except Exception:
            parsed = None

    if isinstance(parsed, list):
        candidates = parsed
    elif isinstance(parsed, dict):
        candidates = parsed.values()
    elif parsed is not None:
        candidates = [parsed]
    else:
        candidates = re.split(r"[\n;|,]+", raw)

    return any(normalize_match_key(clean_result_option(candidate)) == BLANK_VALUE for candidate in candidates)


@st.cache_data(ttl=900, show_spinner=False)
def fetch_odin_attribute_metadata(attribute_table_name: str) -> pd.DataFrame:
    attribute_table = fq_table_name(attribute_table_name)
    usage_table = sibling_table_name(attribute_table_name, "ODIN_OPTIC_ATTRIBUTEUSAGE")
    job_table = sibling_table_name(attribute_table_name, "ODIN_OPTIC_LOOKJOBNAME")
    sql = f"""
    WITH active_attribute AS (
        SELECT *
        FROM {attribute_table}
        WHERE ATTRIBUTENAME IS NOT NULL
          AND COALESCE(DISABLED, FALSE) = FALSE
          AND (HISTORYENDDATE IS NULL OR HISTORYENDDATE >= CURRENT_TIMESTAMP())
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY LOWER(TRIM(ATTRIBUTENAME))
            ORDER BY
                COALESCE(HISTORYENDDATE, '9999-12-31'::TIMESTAMP_NTZ) DESC,
                COALESCE(HISTORYSTARTDATE, CREATEDDATE, '1900-01-01'::TIMESTAMP_NTZ) DESC,
                ATTRIBUTEID DESC
        ) = 1
    ),
    usage_by_job AS (
        SELECT
            ATTRIBUTEID,
            JOBNAMEID,
            MAX("COUNT") AS USAGE_COUNT,
            MAX(LASTOCCURENCEDATE) AS LAST_USAGE_DATE
        FROM {usage_table}
        GROUP BY 1, 2
    ),
    job_rollup AS (
        SELECT
            u.ATTRIBUTEID,
            LISTAGG(DISTINCT UPPER(j.NAME), ',') WITHIN GROUP (ORDER BY UPPER(j.NAME)) AS JOBS,
            SUM(u.USAGE_COUNT) AS USAGE_COUNT,
            MAX(u.LAST_USAGE_DATE) AS LAST_USAGE_DATE
        FROM usage_by_job u
        LEFT JOIN {job_table} j
            ON u.JOBNAMEID = j.JOBNAMEID
        WHERE j.NAME IS NOT NULL
        GROUP BY 1
    )
    SELECT
        a.ATTRIBUTEID,
        a.ATTRIBUTENAME,
        a.DESCRIPTION,
        a.RESULTVALUETYPE,
        a.RESULTVALUEOPTIONS,
        a.PROMPT,
        a.MODELID,
        a.USERID,
        a.CREATEDDATE,
        a.SHOWINREPORT,
        a.DISABLED,
        a.PURGERECORDS,
        a.AILASTUPDATEDDATE,
        a.SECURITYGROUPID,
        a.HISTORYSTARTDATE,
        a.HISTORYENDDATE,
        a.NOTES,
        a.GROUPNAME,
        a.SUMMARYPROMPT,
        a.INCLUDEAGENTINSUMMARY,
        COALESCE(j.JOBS, '') AS JOBS,
        j.USAGE_COUNT,
        j.LAST_USAGE_DATE
    FROM active_attribute a
    LEFT JOIN job_rollup j
        ON a.ATTRIBUTEID = j.ATTRIBUTEID
    ORDER BY LOWER(a.ATTRIBUTENAME)
    """
    return run_query(sql)


def fetch_optic_promptdetails_metadata(view_name: str) -> pd.DataFrame:
    """Read the consolidated ODIN prompt-details view as the source of truth.

    The view holds one row per (JOBNAME, attribute). Result type, options and prompt
    are consistent across jobs for a given attribute, so we collapse to one row per
    attribute and aggregate the authoritative JOBNAME set. The returned frame matches
    the column shape consumed by ``transform_odin_attribute_metadata``.
    """
    view = fq_table_name(view_name)
    sql = f"""
    SELECT
        ATTRIBUTEID,
        MAX(ATTRIBUTENAME) AS ATTRIBUTENAME,
        MAX(DESCRIPTION) AS DESCRIPTION,
        MAX(RESULTVALUETYPE) AS RESULTVALUETYPE,
        MAX(RESULTVALUEOPTIONS) AS RESULTVALUEOPTIONS,
        MAX(PROMPT) AS PROMPT,
        LISTAGG(DISTINCT UPPER(TRIM(JOBNAME)), ',')
            WITHIN GROUP (ORDER BY UPPER(TRIM(JOBNAME))) AS JOBS
    FROM {view}
    WHERE ATTRIBUTENAME IS NOT NULL
    GROUP BY ATTRIBUTEID
    ORDER BY ATTRIBUTEID
    """
    return run_query(sql)


def split_metadata_jobs(value: Any) -> list[str]:
    if value is None:
        return []
    return [
        job.strip().upper()
        for job in str(value).split(",")
        if job.strip()
    ]


def transform_odin_attribute_metadata(
    raw_df: pd.DataFrame,
    attribute_table_name: str,
    apply_job_overrides: bool = True,
) -> pd.DataFrame:
    if raw_df.empty:
        return pd.DataFrame()

    df = raw_df.copy()
    df.columns = [str(col).lower() for col in df.columns]
    records: list[dict[str, Any]] = []

    for _, row in df.iterrows():
        attr_l = clean_prompt_text(row.get("attributename")).lower()
        if not attr_l:
            continue

        prompt_text = clean_prompt_text(row.get("prompt"))
        source_result_type = clean_prompt_text(row.get("resultvaluetype")).upper()
        result_type = normalize_result_type_label(source_result_type)
        option_outputs = parse_result_value_options(row.get("resultvalueoptions"))
        options_allow_blank = result_value_options_allow_blank(row.get("resultvalueoptions"))
        if prompt_requires_boolean(prompt_text, ""):
            prompt_outputs = ["1", "0"]
        elif result_type in {"STRINGPICKONE", "STRINGMULTI", "LIST"}:
            prompt_outputs = infer_quoted_expected_outputs(prompt_text)
        else:
            prompt_outputs = []

        if option_outputs:
            expected_outputs = option_outputs
        elif result_type in {"BOOL", "BOOLEMPTY"}:
            expected_outputs = ["1", "0"]
        else:
            expected_outputs = prompt_outputs

        value_mode = ""
        can_use_prompt_text_presence = (
            not option_outputs
            and result_type in FREEFORM_RESULT_TYPES
        )
        if can_use_prompt_text_presence and prompt_indicates_text_presence_flag(attr_l, prompt_text, result_type, expected_outputs):
            expected_outputs = ["1", "0"]
            value_mode = TEXT_PRESENCE_VALUE_MODE

        allow_blank = (
            result_type == "BOOLEMPTY"
            or options_allow_blank
            or (not source_result_type and prompt_allows_blank(prompt_text, result_type))
            or any(normalize_match_key(value) == BLANK_VALUE for value in expected_outputs)
        )
        canonical_expected = [
            canonicalize_output(attr_l, value, expected_outputs)
            for value in expected_outputs
            if normalize_match_key(value) != BLANK_VALUE
        ]
        canonical_expected = list(dict.fromkeys(canonical_expected))
        if not result_type:
            result_type = infer_result_type_from_prompt(attr_l, source_result_type, prompt_text, canonical_expected, allow_blank)
        if allow_blank and canonical_expected and value_mode != TEXT_PRESENCE_VALUE_MODE:
            canonical_expected.append(BLANK_VALUE)
            canonical_expected = list(dict.fromkeys(canonical_expected))

        jobs = split_metadata_jobs(row.get("jobs"))
        if apply_job_overrides:
            jobs = EXPECTED_OUTPUT_JOB_OVERRIDES.get(attr_l, jobs)
        drift_eligible = is_expected_drift_eligible(result_type, canonical_expected, value_mode)
        validation_method = "Text presence >=3 words" if value_mode == TEXT_PRESENCE_VALUE_MODE else "Expected output set"
        group_name = clean_prompt_text(row.get("groupname")) or "ODIN Attribute"

        records.append(
            {
                "attribute": attr_l,
                "column": attr_l.upper(),
                "attribute_id": row.get("attributeid"),
                "description": clean_prompt_text(row.get("description")),
                "jobs": jobs,
                "jobs_display": ", ".join(jobs),
                "prompt_groups": [group_name],
                "prompt_groups_display": group_name,
                "result_type": result_type or "UNKNOWN",
                "excel_result_type": source_result_type or "UNKNOWN",
                "source_result_type": source_result_type or "UNKNOWN",
                "result_type_source": "ODIN attribute table",
                "excel_model": clean_prompt_text(row.get("modelid")),
                "model_id": row.get("modelid"),
                "usage": pd.to_numeric(row.get("usage_count"), errors="coerce"),
                "last_usage_date": clean_prompt_text(row.get("last_usage_date")),
                "created_date": clean_prompt_text(row.get("createddate")),
                "history_start_date": clean_prompt_text(row.get("historystartdate")),
                "history_end_date": clean_prompt_text(row.get("historyenddate")),
                "show_in_report": row.get("showinreport"),
                "security_group_id": row.get("securitygroupid"),
                "notes": clean_prompt_text(row.get("notes")),
                "summary_prompt": clean_prompt_text(row.get("summaryprompt")),
                "result_value_options": clean_prompt_text(row.get("resultvalueoptions")),
                "value_mode": value_mode,
                "validation_method": validation_method if drift_eligible else "",
                "prompt": prompt_text,
                "prompt_source": attribute_table_name,
                "metadata_source": "ODIN source of truth",
                "expected_outputs": canonical_expected,
                "expected_display": ", ".join(canonical_expected) if canonical_expected else "",
                "validation_enabled": drift_eligible,
                "allow_blank": allow_blank,
                "drift_eligible": drift_eligible,
            }
        )

    return pd.DataFrame(records).sort_values("attribute").reset_index(drop=True)


@st.cache_data(ttl=900, show_spinner=False)
def load_prompt_file_metadata(
    acquisition_prompt_path: str,
    retention_prompt_path: str,
    service_prompt_path: str,
    attributes_xlsx_path: str,
) -> pd.DataFrame:
    prompt_paths = {
        "ACQUISITION": Path(acquisition_prompt_path),
        "RETENTION": Path(retention_prompt_path),
        "SERVICE": Path(service_prompt_path),
    }
    rows_by_attr: dict[str, dict[str, Any]] = {}

    for job_name, path in prompt_paths.items():
        if not path.exists():
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        json_job_name = str(data.get("jobName") or job_name).upper()
        for subprompt in data.get("subPrompts", []):
            prompt_group = str(subprompt.get("promptName") or "Ungrouped")
            for attr, prompt in subprompt.get("attributes", {}).items():
                attr_l = str(attr).strip().lower()
                if not attr_l:
                    continue
                row = rows_by_attr.setdefault(
                    attr_l,
                    {
                        "attribute": attr_l,
                        "column": attr_l.upper(),
                        "jobs": set(),
                        "prompt_groups": set(),
                        "json_prompts": {},
                    },
                )
                row["jobs"].add(json_job_name)
                row["prompt_groups"].add(prompt_group)
                row["json_prompts"][json_job_name] = clean_prompt_text(prompt)

    if not rows_by_attr:
        return pd.DataFrame()

    excel_lookup: dict[str, dict[str, Any]] = {}
    xlsx_path = Path(attributes_xlsx_path)
    if xlsx_path.exists():
        excel_df = pd.read_excel(xlsx_path, sheet_name="Attributes")
        excel_df["_attr_lower"] = excel_df["Attribute Name"].astype(str).str.strip().str.lower()
        excel_df = excel_df.drop_duplicates("_attr_lower", keep="first")
        excel_lookup = {
            str(row["_attr_lower"]): row.to_dict()
            for _, row in excel_df.iterrows()
            if str(row.get("_attr_lower", "")).strip()
        }

    records: list[dict[str, Any]] = []
    for attr_l, row in rows_by_attr.items():
        excel = excel_lookup.get(attr_l, {})
        excel_result_type = clean_prompt_text(excel.get("Result Type")).upper()
        excel_prompt = clean_prompt_text(excel.get("Prompt"))
        json_prompt_text = "\n\n".join(row["json_prompts"].values())
        prompt_text = json_prompt_text or excel_prompt
        prompt_source = "production JSON" if json_prompt_text else "Excel fallback"
        expected_outputs, allow_blank = expected_outputs_for_attribute(attr_l, excel_result_type, prompt_text)
        canonical_expected = [
            canonicalize_output(attr_l, value, expected_outputs)
            for value in expected_outputs
        ]
        canonical_expected = list(dict.fromkeys(canonical_expected))
        result_type = infer_result_type_from_prompt(attr_l, excel_result_type, prompt_text, canonical_expected, allow_blank)
        if allow_blank and canonical_expected:
            canonical_expected.append(BLANK_VALUE)
            canonical_expected = list(dict.fromkeys(canonical_expected))
        drift_eligible = is_expected_drift_eligible(result_type, canonical_expected)
        result_type_source = result_type_source_label(excel_result_type, result_type, prompt_text)
        jobs = EXPECTED_OUTPUT_JOB_OVERRIDES.get(attr_l, sorted(row["jobs"]))

        records.append(
            {
                "attribute": attr_l,
                "column": attr_l.upper(),
                "jobs": jobs,
                "jobs_display": ", ".join(jobs),
                "prompt_groups": sorted(row["prompt_groups"]),
                "prompt_groups_display": ", ".join(sorted(row["prompt_groups"])),
                "result_type": result_type or "UNKNOWN",
                "excel_result_type": excel_result_type or "UNKNOWN",
                "result_type_source": result_type_source,
                "excel_model": clean_prompt_text(excel.get("Model")),
                "usage": pd.to_numeric(excel.get("Usage"), errors="coerce"),
                "last_usage_date": clean_prompt_text(excel.get("Last Usage Date")),
                "prompt": prompt_text,
                "prompt_source": prompt_source,
                "expected_outputs": canonical_expected,
                "expected_display": ", ".join(canonical_expected) if canonical_expected else "",
                "validation_enabled": drift_eligible,
                "allow_blank": allow_blank,
                "drift_eligible": drift_eligible,
            }
        )

    return pd.DataFrame(records).sort_values("attribute").reset_index(drop=True)


def load_attribute_metadata(
    attribute_table_name: str,
    acquisition_prompt_path: str,
    retention_prompt_path: str,
    service_prompt_path: str,
    attributes_xlsx_path: str,
    promptdetails_view_name: str = DEFAULT_PROMPTDETAILS_VIEW,
) -> pd.DataFrame:
    warnings: list[str] = []
    # Primary source of truth: the consolidated ODIN prompt-details view. It carries
    # authoritative attribute definitions AND the JOBNAME -> attribute mapping, so the
    # legacy per-attribute job overrides are intentionally not applied on this path.
    if promptdetails_view_name and promptdetails_view_name.strip():
        try:
            view_raw = fetch_optic_promptdetails_metadata(promptdetails_view_name.strip())
            view_df = transform_odin_attribute_metadata(
                view_raw, promptdetails_view_name.strip(), apply_job_overrides=False
            )
            if not view_df.empty:
                view_df.attrs["metadata_source"] = promptdetails_view_name.strip()
                view_df.attrs["jobs_source"] = "promptdetails_view"
                return view_df
            warnings.append(f"{promptdetails_view_name.strip()} returned no attributes.")
        except Exception as exc:
            warnings.append(f"OPTIC prompt-details view load failed: {exc}")

    if attribute_table_name.strip():
        try:
            odin_raw = fetch_odin_attribute_metadata(attribute_table_name.strip())
            odin_df = transform_odin_attribute_metadata(odin_raw, attribute_table_name.strip())
            if not odin_df.empty:
                odin_df.attrs["metadata_source"] = attribute_table_name.strip()
                if warnings:
                    odin_df.attrs["metadata_warning"] = " ".join(warnings)
                return odin_df
            warnings.append(f"{attribute_table_name.strip()} returned no active attributes.")
        except Exception as exc:
            warnings.append(f"ODIN attribute metadata load failed: {exc}")

    fallback_df = load_prompt_file_metadata(
        acquisition_prompt_path,
        retention_prompt_path,
        service_prompt_path,
        attributes_xlsx_path,
    )
    fallback_df.attrs["metadata_source"] = "JSON/Excel fallback"
    if warnings:
        fallback_df.attrs["metadata_warning"] = " ".join(warnings)
    return fallback_df


def blank_expr(col_name: str, dtype: str) -> str:
    col = qident(col_name)
    dtype = dtype.upper()
    if dtype == "BOOLEAN":
        return f"{col} IS NULL"
    if dtype == "ARRAY":
        return f"{col} IS NULL OR ARRAY_SIZE({col}) = 0"
    return (
        f"{col} IS NULL OR UPPER(TRIM(TO_VARCHAR({col}))) "
        f"IN ({sql_in(list(EMPTY_TEXT_VALUES))})"
    )


def populated_expr(col_name: str, dtype: str) -> str:
    return f"NOT ({blank_expr(col_name, dtype)})"


def attribute_quality_select(col_name: str, dtype: str, source_sql: str = "base", where_sql: str | None = None) -> str:
    col = qident(col_name)
    blank = blank_expr(col_name, dtype)
    populated = populated_expr(col_name, dtype)
    attr_literal = sql_quote(col_name)
    dtype_literal = sql_quote(dtype)
    from_clause = f"FROM {source_sql}"
    if where_sql:
        from_clause += f"\n        WHERE {where_sql}"

    if dtype == "BOOLEAN":
        return f"""
        SELECT
            {attr_literal} AS attribute,
            {dtype_literal} AS data_type,
            COUNT(*) AS total_rows,
            COUNT_IF({blank}) AS blank_rows,
            COUNT_IF({populated}) AS populated_rows,
            COUNT_IF({col} = TRUE) AS true_rows,
            COUNT_IF({col} = FALSE) AS false_rows,
            NULL::FLOAT AS avg_length,
            NULL::NUMBER AS distinct_values
        {from_clause}
        """
    if dtype == "ARRAY":
        return f"""
        SELECT
            {attr_literal} AS attribute,
            {dtype_literal} AS data_type,
            COUNT(*) AS total_rows,
            COUNT_IF({blank}) AS blank_rows,
            COUNT_IF({populated}) AS populated_rows,
            NULL::NUMBER AS true_rows,
            NULL::NUMBER AS false_rows,
            AVG(IFF({populated}, ARRAY_SIZE({col}), NULL)) AS avg_length,
            APPROX_COUNT_DISTINCT(IFF({populated}, TO_VARCHAR({col}), NULL)) AS distinct_values
        {from_clause}
        """
    return f"""
    SELECT
        {attr_literal} AS attribute,
        {dtype_literal} AS data_type,
        COUNT(*) AS total_rows,
        COUNT_IF({blank}) AS blank_rows,
        COUNT_IF({populated}) AS populated_rows,
        NULL::NUMBER AS true_rows,
        NULL::NUMBER AS false_rows,
        AVG(IFF({populated}, LENGTH(TO_VARCHAR({col})), NULL)) AS avg_length,
        APPROX_COUNT_DISTINCT(IFF({populated}, TO_VARCHAR({col}), NULL)) AS distinct_values
    {from_clause}
    """


def effective_attribute_jobs(
    attr: str,
    attribute_jobs: dict[str, list[str]] | None,
    selected_jobs_upper: set[str],
) -> list[str] | None:
    if attribute_jobs is None:
        return sorted(selected_jobs_upper)

    active_jobs = {
        str(job).upper()
        for job in attribute_jobs.get(str(attr).upper(), [])
        if str(job).strip()
    }
    if not active_jobs:
        return None
    if selected_jobs_upper:
        effective_jobs = active_jobs & selected_jobs_upper
        return sorted(effective_jobs) if effective_jobs else None
    return sorted(active_jobs)


def attribute_where_clause(
    date_col: str,
    start_date: date,
    end_date: date,
    job_col: str | None,
    effective_jobs: list[str] | None,
    brand_col: str | None,
    brands: list[str],
) -> str:
    jobs = effective_jobs or []
    return where_clause(date_col, start_date, end_date, job_col, jobs, brand_col, brands)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_filter_options(
    table_name: str,
    date_col: str,
    start_date: date,
    end_date: date,
    job_col: str | None,
    brand_col: str | None,
) -> dict[str, pd.DataFrame]:
    table = fq_table_name(table_name)
    base_where = where_clause(date_col, start_date, end_date, None, [], None, [])
    result: dict[str, pd.DataFrame] = {"jobs": pd.DataFrame(), "brands": pd.DataFrame()}

    if job_col:
        result["jobs"] = run_query(
            f"""
            SELECT UPPER(COALESCE({qident(job_col)}, 'UNKNOWN')) AS VALUE, COUNT(*) AS ROW_COUNT
            FROM {table}
            WHERE {base_where}
            GROUP BY 1
            ORDER BY ROW_COUNT DESC, VALUE
            LIMIT 100
            """
        )
    if brand_col:
        result["brands"] = run_query(
            f"""
            SELECT UPPER(COALESCE({qident(brand_col)}, 'UNKNOWN')) AS VALUE, COUNT(*) AS ROW_COUNT
            FROM {table}
            WHERE {base_where}
            GROUP BY 1
            ORDER BY ROW_COUNT DESC, VALUE
            LIMIT 100
            """
        )
    return result


@st.cache_data(ttl=300, show_spinner=False)
def fetch_live_summary(
    table_name: str,
    date_col: str,
    start_date: date,
    end_date: date,
    job_col: str | None,
    jobs: list[str],
    brand_col: str | None,
    brands: list[str],
    connection_col: str | None,
    duration_col: str | None,
) -> pd.DataFrame:
    table = fq_table_name(table_name)
    filters = where_clause(date_col, start_date, end_date, job_col, jobs, brand_col, brands)
    connection_metric = (
        f"COUNT(DISTINCT {qident(connection_col)}) AS distinct_connections"
        if connection_col
        else "NULL::NUMBER AS distinct_connections"
    )
    job_metric = (
        f"COUNT(DISTINCT {qident(job_col)}) AS job_count"
        if job_col
        else "NULL::NUMBER AS job_count"
    )
    brand_metric = (
        f"COUNT(DISTINCT {qident(brand_col)}) AS brand_count"
        if brand_col
        else "NULL::NUMBER AS brand_count"
    )
    duration_metric = (
        f"AVG({qident(duration_col)}) AS avg_duration"
        if duration_col
        else "NULL::FLOAT AS avg_duration"
    )
    sql = f"""
    SELECT
        COUNT(*) AS total_rows,
        {connection_metric},
        {job_metric},
        {brand_metric},
        MIN({qident(date_col)}) AS first_interaction,
        MAX({qident(date_col)}) AS latest_interaction,
        {duration_metric}
    FROM {table}
    WHERE {filters}
    """
    return run_query(sql)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_volume_trend(
    table_name: str,
    date_col: str,
    start_date: date,
    end_date: date,
    grain: str,
    job_col: str | None,
    jobs: list[str],
    brand_col: str | None,
    brands: list[str],
) -> pd.DataFrame:
    if grain not in {"day", "week", "month"}:
        grain = "day"
    table = fq_table_name(table_name)
    filters = where_clause(date_col, start_date, end_date, job_col, jobs, brand_col, brands)
    sql = f"""
    SELECT
        DATE_TRUNC({sql_quote(grain)}, {qident(date_col)})::DATE AS period,
        COUNT(*) AS row_count
    FROM {table}
    WHERE {filters}
    GROUP BY 1
    ORDER BY 1
    """
    return run_query(sql)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_dimension_counts(
    table_name: str,
    date_col: str,
    start_date: date,
    end_date: date,
    dimension_col: str,
    job_col: str | None,
    jobs: list[str],
    brand_col: str | None,
    brands: list[str],
    limit: int = 20,
) -> pd.DataFrame:
    table = fq_table_name(table_name)
    filters = where_clause(date_col, start_date, end_date, job_col, jobs, brand_col, brands)
    sql = f"""
    SELECT UPPER(COALESCE({qident(dimension_col)}, 'UNKNOWN')) AS value, COUNT(*) AS row_count
    FROM {table}
    WHERE {filters}
    GROUP BY 1
    ORDER BY row_count DESC, value
    LIMIT {int(limit)}
    """
    return run_query(sql)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_attribute_quality(
    table_name: str,
    date_col: str,
    start_date: date,
    end_date: date,
    job_col: str | None,
    jobs: list[str],
    brand_col: str | None,
    brands: list[str],
    selected_attrs: list[str],
    attr_types: dict[str, str],
    attribute_jobs: dict[str, list[str]] | None = None,
) -> pd.DataFrame:
    if not selected_attrs:
        return pd.DataFrame()

    table = fq_table_name(table_name)
    selected_jobs_upper = {str(job).upper() for job in jobs if str(job).strip()}
    selects: list[str] = []
    for attr in selected_attrs:
        effective_jobs = effective_attribute_jobs(attr, attribute_jobs, selected_jobs_upper)
        if effective_jobs is None:
            continue
        filters = attribute_where_clause(date_col, start_date, end_date, job_col, effective_jobs, brand_col, brands)
        selects.append(attribute_quality_select(attr, attr_types.get(attr, "TEXT"), table, filters))
    if not selects:
        return pd.DataFrame()

    sql = " UNION ALL ".join(selects)
    df = run_query(sql)
    if df.empty:
        return df
    df.columns = [c.lower() for c in df.columns]
    df["blank_rate"] = df["blank_rows"] / df["total_rows"].clip(lower=1)
    df["populated_rate"] = df["populated_rows"] / df["total_rows"].clip(lower=1)
    df["true_rate"] = df["true_rows"] / df["total_rows"].clip(lower=1)
    df["false_rate"] = df["false_rows"] / df["total_rows"].clip(lower=1)
    return df


@st.cache_data(ttl=300, show_spinner=False)
def fetch_attribute_time_series(
    table_name: str,
    date_col: str,
    start_date: date,
    end_date: date,
    grain: str,
    job_col: str | None,
    jobs: list[str],
    brand_col: str | None,
    brands: list[str],
    selected_attrs: list[str],
    attr_types: dict[str, str],
    attribute_jobs: dict[str, list[str]] | None = None,
) -> pd.DataFrame:
    if not selected_attrs:
        return pd.DataFrame()
    if grain not in {"day", "week", "month"}:
        grain = "week"

    table = fq_table_name(table_name)
    selected_jobs_upper = {str(job).upper() for job in jobs if str(job).strip()}
    selects: list[str] = []
    for attr in selected_attrs:
        effective_jobs = effective_attribute_jobs(attr, attribute_jobs, selected_jobs_upper)
        if effective_jobs is None:
            continue
        filters = attribute_where_clause(date_col, start_date, end_date, job_col, effective_jobs, brand_col, brands)
        dtype = attr_types.get(attr, "TEXT")
        blank = blank_expr(attr, dtype)
        populated = populated_expr(attr, dtype)
        true_rows = f"COUNT_IF({qident(attr)} = TRUE)" if dtype == "BOOLEAN" else "NULL::NUMBER"
        false_rows = f"COUNT_IF({qident(attr)} = FALSE)" if dtype == "BOOLEAN" else "NULL::NUMBER"
        selects.append(
            f"""
            SELECT
                DATE_TRUNC({sql_quote(grain)}, {qident(date_col)})::DATE AS period,
                {sql_quote(attr)} AS attribute,
                {sql_quote(dtype)} AS data_type,
                COUNT(*) AS total_rows,
                COUNT_IF({blank}) AS blank_rows,
                COUNT_IF({populated}) AS populated_rows,
                {true_rows} AS true_rows,
                {false_rows} AS false_rows
            FROM {table}
            WHERE {filters}
            GROUP BY 1, 2, 3
            """
        )
    if not selects:
        return pd.DataFrame()

    sql = f"""
    {" UNION ALL ".join(selects)}
    ORDER BY 1, 2
    """
    df = run_query(sql)
    if df.empty:
        return df
    df.columns = [c.lower() for c in df.columns]
    df["blank_rate"] = df["blank_rows"] / df["total_rows"].clip(lower=1)
    df["populated_rate"] = df["populated_rows"] / df["total_rows"].clip(lower=1)
    df["true_rate"] = df["true_rows"] / df["total_rows"].clip(lower=1)
    df["false_rate"] = df["false_rows"] / df["total_rows"].clip(lower=1)
    return df


def attribute_value_expr(col_name: str, dtype: str, value_mode: str = "") -> str:
    col = qident(col_name)
    dtype_u = (dtype or "").upper()
    value_mode_u = (value_mode or "").upper()
    if value_mode_u == "TEXT_PRESENCE_3_WORDS":
        text_value = f"NULLIF(TRIM(TO_VARCHAR({col})), '')"
        word_pattern = sql_quote(r"[^[:space:]]+")
        return (
            f"IFF({text_value} IS NULL, '0', "
            f"IFF(REGEXP_COUNT({text_value}, {word_pattern}) >= 3, '1', '0'))"
        )
    if dtype_u == "BOOLEAN":
        return f"IFF({col} IS NULL, {sql_quote(BLANK_VALUE)}, IFF({col} = TRUE, '1', '0'))"
    if dtype_u == "ARRAY":
        return f"IFF({col} IS NULL OR ARRAY_SIZE({col}) = 0, {sql_quote(BLANK_VALUE)}, TO_VARCHAR({col}))"
    return f"COALESCE(NULLIF(TRIM(TO_VARCHAR({col})), ''), {sql_quote(BLANK_VALUE)})"


def expected_spec_records(metadata_df: pd.DataFrame, selected_attrs: list[str], attr_types: dict[str, str]) -> str:
    selected_lower = {attr.lower() for attr in selected_attrs}
    rows = []
    for _, row in metadata_df.iterrows():
        column = str(row["column"]).upper()
        if column.lower() not in selected_lower and str(row["attribute"]).lower() not in selected_lower:
            continue
        if not row.get("validation_enabled"):
            continue
        rows.append(
            {
                "attribute": str(row["attribute"]).lower(),
                "column": column,
                "data_type": attr_types.get(column, "TEXT"),
                "jobs": list(row.get("jobs") or []),
                "expected_outputs": list(row.get("expected_outputs") or []),
                "allow_blank": bool(row.get("allow_blank")),
                "result_type": str(row.get("result_type") or ""),
                "value_mode": str(row.get("value_mode") or ""),
            }
        )
    return json.dumps(rows, default=str, sort_keys=True)


def value_distribution_specs(
    attrs: list[str],
    attr_types: dict[str, str],
    metadata_df: pd.DataFrame,
    fallback_jobs: list[str],
) -> str:
    """Build fetch_expected_value_distribution specs for *any* attribute (not just drift-eligible ones).

    Reuses the existing per-period value-distribution query so the Value Spikes view can track the
    volume mix of every selected attribute over time. Value Spikes intentionally does not require
    finite expected outputs; it uses source-table columns and the sidebar job filter only.
    """
    fallback = [str(j).upper() for j in (fallback_jobs or [])]
    metadata_by_column: dict[str, dict[str, Any]] = {}
    if metadata_df is not None and not metadata_df.empty:
        metadata_by_column = {
            str(row.get("column") or "").upper(): row.to_dict()
            for _, row in metadata_df.iterrows()
            if str(row.get("column") or "").strip()
        }

    specs: list[dict[str, Any]] = []
    for col in attrs:
        column = str(col).upper()
        dtype = (attr_types.get(column, "TEXT") or "TEXT").upper()
        meta = metadata_by_column.get(column, {})
        meta_has_scope = bool(meta)
        active_jobs = [str(job).upper() for job in (meta.get("jobs") or []) if str(job).strip()]
        if meta_has_scope and not active_jobs:
            continue
        result_type = normalize_result_type_label(str(meta.get("result_type") or ""))
        expected_outputs = [
            str(value).strip()
            for value in (meta.get("expected_outputs") or [])
            if str(value).strip() and normalize_match_key(value) != BLANK_VALUE
        ]
        value_mode = str(meta.get("value_mode") or "").upper()
        if (
            not value_mode
            and dtype in {"TEXT", "VARCHAR", "STRING"}
            and prompt_indicates_text_presence_flag(
                str(meta.get("attribute") or column).lower(),
                str(meta.get("prompt") or ""),
                result_type,
                expected_outputs,
            )
        ):
            value_mode = TEXT_PRESENCE_VALUE_MODE
        specs.append(
            {
                "attribute": column,
                "column": column,
                "data_type": dtype,
                "jobs": active_jobs if meta_has_scope else fallback,
                "expected_outputs": [],
                "allow_blank": False,
                "result_type": result_type,
                "value_mode": value_mode,
            }
        )
    return json.dumps(specs, default=str, sort_keys=True)


def infer_value_result_types(dist: pd.DataFrame, attr_types: dict[str, str]) -> dict[str, str]:
    """Infer useful result categories from observed source-table values."""
    inferred: dict[str, str] = {}
    if dist.empty:
        return inferred

    bool_values = {"0", "1", "TRUE", "FALSE", "YES", "NO", "Y", "N", BLANK_VALUE}
    numeric_types = {"NUMBER", "FLOAT", "DOUBLE", "DECIMAL", "INTEGER", "INT", "BIGINT"}

    for attr, group in dist.groupby("attribute", dropna=False):
        attr_key = str(attr).upper()
        dtype = str(attr_types.get(attr_key) or group["data_type"].dropna().astype(str).head(1).squeeze() or "").upper()
        values = {str(v).strip().upper() for v in group["raw_value"].dropna().unique()}
        nonblank_values = {v for v in values if v != BLANK_VALUE}
        distinct_count = len(nonblank_values)
        total_rows = float(group["row_count"].sum())
        top_share = float(group["row_count"].max() / total_rows) if total_rows else 0.0

        if dtype == "BOOLEAN" or (nonblank_values and nonblank_values.issubset(bool_values)):
            result_type = "Boolean-like"
        elif dtype in numeric_types:
            result_type = "Numeric"
        elif dtype == "ARRAY":
            result_type = "List/array"
        elif distinct_count <= 25:
            result_type = "Categorical"
        elif distinct_count <= 100 or top_share >= 0.10:
            result_type = "High-cardinality categorical"
        else:
            result_type = "Freeform/high-cardinality"

        inferred[attr_key] = result_type
    return inferred


def value_spike_expected_lookup(metadata_df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    """Expected-output specs keyed by source-table column for Value Spikes bucketing."""
    lookup: dict[str, dict[str, Any]] = {}
    if metadata_df is None or metadata_df.empty:
        return lookup

    for _, row in metadata_df.iterrows():
        column = str(row.get("column") or "").upper()
        expected_outputs = [str(v).strip() for v in (row.get("expected_outputs") or []) if str(v).strip()]
        nonblank_expected = [value for value in expected_outputs if normalize_match_key(value) != BLANK_VALUE]
        if not column or not nonblank_expected:
            continue
        lookup[column] = {
            "attribute": str(row.get("attribute") or column).lower(),
            "expected_outputs": expected_outputs,
            "allow_blank": bool(row.get("allow_blank")),
            "result_type": str(row.get("result_type") or ""),
        }
    return lookup


def split_multi_output_values(value: Any) -> list[str]:
    text = str(value or "").strip()
    if not text or normalize_match_key(text) == BLANK_VALUE:
        return []
    text = text.strip("[](){}")
    if not re.search(r"[,;|]", text):
        return [text]
    return [part.strip().strip("'\"") for part in re.split(r"[,;|]", text) if part.strip().strip("'\"")]


def is_expected_prompt_value(
    attr_name: str,
    raw_value: Any,
    expected_outputs: list[str],
    allow_blank: bool = False,
    result_type: str = "",
) -> bool:
    expected_map = {normalize_match_key(value): str(value).strip() for value in expected_outputs}
    canonical_value = canonicalize_output(attr_name, raw_value, expected_outputs)
    canonical_key = normalize_match_key(canonical_value)
    if canonical_key == BLANK_VALUE:
        return allow_blank or BLANK_VALUE in expected_map
    if canonical_key in expected_map:
        return True

    if normalize_result_type_label(result_type) in {"STRINGMULTI", "LIST"}:
        parts = split_multi_output_values(raw_value)
        if not parts:
            return False
        for part in parts:
            part_key = normalize_match_key(canonicalize_output(attr_name, part, expected_outputs))
            if part_key not in expected_map:
                return False
        return True
    return False


def canonical_expected_display_value(attr_name: str, raw_value: Any, expected_outputs: list[str], result_type: str = "") -> str:
    expected_map = {normalize_match_key(value): str(value).strip() for value in expected_outputs}
    canonical_value = canonicalize_output(attr_name, raw_value, expected_outputs)
    canonical_key = normalize_match_key(canonical_value)
    if canonical_key in expected_map:
        return expected_map[canonical_key]
    if normalize_result_type_label(result_type) in {"STRINGMULTI", "LIST"}:
        parts = split_multi_output_values(raw_value)
        display_parts: list[str] = []
        for part in parts:
            part_key = normalize_match_key(canonicalize_output(attr_name, part, expected_outputs))
            if part_key not in expected_map:
                return str(raw_value)
            display_parts.append(expected_map[part_key])
        if display_parts:
            return ", ".join(display_parts)
    return str(raw_value)


def bucket_value_for_spikes(attr: str, raw_value: Any, expected_lookup: dict[str, dict[str, Any]]) -> str:
    """Keep expected values separate and collapse unexpected values into Other."""
    attr_key = str(attr).upper()
    spec = expected_lookup.get(attr_key)
    raw_text = str(raw_value)
    if not spec:
        return raw_text

    attr_name = str(spec.get("attribute") or attr_key).lower()
    expected_outputs = list(spec.get("expected_outputs") or [])
    allow_blank = bool(spec.get("allow_blank"))
    result_type = str(spec.get("result_type") or "")
    if normalize_match_key(raw_value) == BLANK_VALUE and allow_blank:
        return BLANK_VALUE

    if is_expected_prompt_value(attr_name, raw_value, expected_outputs, allow_blank, result_type):
        return canonical_expected_display_value(attr_name, raw_value, expected_outputs, result_type)
    return "Other"


@st.cache_data(ttl=300, show_spinner=False)
def fetch_expected_value_distribution(
    table_name: str,
    date_col: str,
    start_date: date,
    end_date: date,
    grain: str,
    job_col: str | None,
    jobs: list[str],
    brand_col: str | None,
    brands: list[str],
    spec_records_json: str,
) -> pd.DataFrame:
    specs = json.loads(spec_records_json)
    if not specs:
        return pd.DataFrame()
    if grain not in {"day", "week", "month"}:
        grain = "day"

    table = fq_table_name(table_name)
    selected_jobs_upper = {str(job).upper() for job in jobs if str(job).strip()}
    selected_brands_upper = {str(brand).upper() for brand in brands if str(brand).strip()}

    selects: list[str] = []
    for spec in specs:
        attr_jobs = {str(job).upper() for job in spec.get("jobs", []) if str(job).strip()}
        if attr_jobs:
            effective_jobs = sorted(attr_jobs & selected_jobs_upper) if selected_jobs_upper else sorted(attr_jobs)
            if selected_jobs_upper and not effective_jobs:
                continue
        else:
            effective_jobs = sorted(selected_jobs_upper)

        clauses = [
            f"{qident(date_col)} >= {sql_quote(start_date)}::DATE",
            f"{qident(date_col)} < DATEADD(day, 1, {sql_quote(end_date)}::DATE)",
        ]
        if job_col and effective_jobs:
            clauses.append(f"UPPER({qident(job_col)}) IN ({sql_in(effective_jobs)})")
        if brand_col and selected_brands_upper:
            clauses.append(f"UPPER({qident(brand_col)}) IN ({sql_in(sorted(selected_brands_upper))})")

        col_name = str(spec["column"]).upper()
        dtype = str(spec.get("data_type") or "TEXT").upper()
        value_mode = str(spec.get("value_mode") or "")
        selects.append(
            f"""
            SELECT
                DATE_TRUNC({sql_quote(grain)}, {qident(date_col)})::DATE AS period,
                {sql_quote(str(spec["attribute"]).lower())} AS attribute,
                {sql_quote(dtype)} AS data_type,
                {attribute_value_expr(col_name, dtype, value_mode)} AS raw_value,
                COUNT(*) AS row_count
            FROM {table}
            WHERE {" AND ".join(clauses)}
            GROUP BY 1, 2, 3, 4
            """
        )

    if not selects:
        return pd.DataFrame()

    sql = f"""
    {" UNION ALL ".join(selects)}
    ORDER BY 1, 2, 5 DESC
    """
    df = run_query(sql)
    if df.empty:
        return df
    df.columns = [c.lower() for c in df.columns]
    return df


@st.cache_data(ttl=300, show_spinner=False)
def fetch_positive_flag_rates(
    table_name: str,
    date_col: str,
    start_date: date,
    end_date: date,
    grain: str,
    job_col: str | None,
    jobs: list[str],
    brand_col: str | None,
    brands: list[str],
    spec_records_json: str,
) -> pd.DataFrame:
    """Fetch positive counts using each attribute's active job scope."""
    specs = json.loads(spec_records_json)
    if not specs:
        return pd.DataFrame()
    if grain not in {"day", "week", "month"}:
        grain = "day"

    table = fq_table_name(table_name)
    selected_jobs_upper = {str(job).upper() for job in jobs if str(job).strip()}
    selected_brands_upper = sorted({str(brand).upper() for brand in brands if str(brand).strip()})

    selects: list[str] = []
    for spec in specs:
        attribute = str(spec.get("attribute") or "").lower()
        column = str(spec.get("column") or "").upper()
        if not attribute or not column:
            continue
        attr_jobs = {str(job).upper() for job in spec.get("jobs", []) if str(job).strip()}
        if attr_jobs:
            effective_jobs = sorted(attr_jobs & selected_jobs_upper) if selected_jobs_upper else sorted(attr_jobs)
            if selected_jobs_upper and not effective_jobs:
                continue
        else:
            effective_jobs = sorted(selected_jobs_upper)

        clauses = [
            f"{qident(date_col)} >= {sql_quote(start_date)}::DATE",
            f"{qident(date_col)} < DATEADD(day, 1, {sql_quote(end_date)}::DATE)",
        ]
        if job_col and effective_jobs:
            clauses.append(f"UPPER({qident(job_col)}) IN ({sql_in(effective_jobs)})")
        if brand_col and selected_brands_upper:
            clauses.append(f"UPPER({qident(brand_col)}) IN ({sql_in(selected_brands_upper)})")

        dtype = str(spec.get("data_type") or "TEXT").upper()
        value_mode = str(spec.get("value_mode") or "")
        value_expr = attribute_value_expr(column, dtype, value_mode)
        selects.append(
            f"""
            SELECT
                DATE_TRUNC({sql_quote(grain)}, {qident(date_col)})::DATE AS period,
                {sql_quote(attribute)} AS attribute,
                COUNT(*) AS total_calls,
                COUNT_IF({value_expr} = '1') AS positive_rows
            FROM {table}
            WHERE {" AND ".join(clauses)}
            GROUP BY 1, 2
            """
        )

    if not selects:
        return pd.DataFrame()

    sql = f"""
    {" UNION ALL ".join(selects)}
    ORDER BY 1, 2
    """
    df = run_query(sql)
    if df.empty:
        return df

    df.columns = [str(column).lower() for column in df.columns]
    df["positive_rows"] = pd.to_numeric(df["positive_rows"], errors="coerce").fillna(0).astype(int)
    df["total_calls"] = pd.to_numeric(df["total_calls"], errors="coerce").fillna(0).astype(int)
    df["positive_rate"] = df["positive_rows"] / df["total_calls"].clip(lower=1)
    return df


def score_expected_outputs(value_df: pd.DataFrame, metadata_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if value_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    spec_lookup = {
        str(row["attribute"]).lower(): {
            "expected_outputs": list(row.get("expected_outputs") or []),
            "expected_display": row.get("expected_display", ""),
            "jobs_display": row.get("jobs_display", ""),
            "prompt_groups_display": row.get("prompt_groups_display", ""),
            "result_type": row.get("result_type", ""),
            "allow_blank": bool(row.get("allow_blank")),
        }
        for _, row in metadata_df.iterrows()
    }

    scored = value_df.copy()
    scored["canonical_value"] = scored.apply(
        lambda row: canonicalize_output(
            str(row["attribute"]),
            row["raw_value"],
            spec_lookup.get(str(row["attribute"]).lower(), {}).get("expected_outputs", []),
        ),
        axis=1,
    )
    scored["expected_outputs"] = scored["attribute"].map(
        lambda attr: spec_lookup.get(str(attr).lower(), {}).get("expected_outputs", [])
    )
    scored["is_blank"] = scored["canonical_value"].eq(BLANK_VALUE)
    scored["is_expected"] = scored.apply(
        lambda row: is_expected_prompt_value(
            str(row["attribute"]),
            row["raw_value"],
            spec_lookup.get(str(row["attribute"]).lower(), {}).get("expected_outputs", []),
            spec_lookup.get(str(row["attribute"]).lower(), {}).get("allow_blank", False),
            spec_lookup.get(str(row["attribute"]).lower(), {}).get("result_type", ""),
        ),
        axis=1,
    )
    scored["status"] = scored.apply(
        lambda row: "Expected" if row["is_expected"] else "Unexpected",
        axis=1,
    )

    grouped_rows: list[dict[str, Any]] = []
    for (period, attr), group in scored.groupby(["period", "attribute"], dropna=False):
        total_rows = int(group["row_count"].sum())
        expected_rows = int(group.loc[group["is_expected"], "row_count"].sum())
        unexpected_rows = total_rows - expected_rows
        blank_rows = int(group.loc[group["is_blank"], "row_count"].sum())
        spec = spec_lookup.get(str(attr).lower(), {})
        unexpected_rate = unexpected_rows / max(total_rows, 1)
        blank_rate = blank_rows / max(total_rows, 1)
        if unexpected_rate >= 0.05:
            severity = "High"
        elif unexpected_rate >= 0.01:
            severity = "Medium"
        elif unexpected_rows > 0:
            severity = "Low"
        else:
            severity = "OK"
        grouped_rows.append(
            {
                "period": period,
                "attribute": attr,
                "total_rows": total_rows,
                "expected_rows": expected_rows,
                "unexpected_rows": unexpected_rows,
                "blank_rows": blank_rows,
                "expected_rate": expected_rows / max(total_rows, 1),
                "unexpected_rate": unexpected_rate,
                "blank_rate": blank_rate,
                "severity": severity,
                "expected_outputs": spec.get("expected_display", ""),
                "jobs": spec.get("jobs_display", ""),
                "prompt_groups": spec.get("prompt_groups_display", ""),
                "result_type": spec.get("result_type", ""),
            }
        )

    summary = pd.DataFrame(grouped_rows)
    if not summary.empty:
        summary = summary.sort_values(["period", "unexpected_rate", "unexpected_rows"], ascending=[True, False, False])
    return scored, summary


@st.cache_data(ttl=300, show_spinner=False)
def fetch_top_values(
    table_name: str,
    date_col: str,
    start_date: date,
    end_date: date,
    job_col: str | None,
    jobs: list[str],
    brand_col: str | None,
    brands: list[str],
    attr: str,
    dtype: str,
    limit: int,
) -> pd.DataFrame:
    table = fq_table_name(table_name)
    filters = where_clause(date_col, start_date, end_date, job_col, jobs, brand_col, brands)

    if dtype == "ARRAY":
        value_expr = "COALESCE(NULLIF(TRIM(f.value::string), ''), '<NULL_OR_EMPTY>')"
        sql = f"""
        WITH base AS (
            SELECT {qident(attr)}
            FROM {table}
            WHERE {filters}
        )
        SELECT {value_expr} AS value, COUNT(*) AS row_count
        FROM base, LATERAL FLATTEN(input => {qident(attr)}, OUTER => TRUE) f
        GROUP BY 1
        ORDER BY row_count DESC, value
        LIMIT {int(limit)}
        """
    elif dtype == "BOOLEAN":
        value_expr = f"COALESCE(TO_VARCHAR({qident(attr)}), '<NULL>')"
        sql = f"""
        SELECT {value_expr} AS value, COUNT(*) AS row_count
        FROM {table}
        WHERE {filters}
        GROUP BY 1
        ORDER BY row_count DESC, value
        LIMIT {int(limit)}
        """
    else:
        value_expr = (
            f"COALESCE(NULLIF(TRIM(TO_VARCHAR({qident(attr)})), ''), '<NULL_OR_EMPTY>')"
        )
        sql = f"""
        SELECT {value_expr} AS value, COUNT(*) AS row_count
        FROM {table}
        WHERE {filters}
        GROUP BY 1
        ORDER BY row_count DESC, value
        LIMIT {int(limit)}
        """
    df = run_query(sql)
    if df.empty:
        return df
    df.columns = [c.lower() for c in df.columns]
    total = df["row_count"].sum()
    df["share"] = df["row_count"] / max(total, 1)
    return df


@st.cache_data(ttl=300, show_spinner=False)
def fetch_sample_rows(
    table_name: str,
    date_col: str,
    start_date: date,
    end_date: date,
    job_col: str | None,
    jobs: list[str],
    brand_col: str | None,
    brands: list[str],
    columns: list[str],
    limit: int,
) -> pd.DataFrame:
    table = fq_table_name(table_name)
    filters = where_clause(date_col, start_date, end_date, job_col, jobs, brand_col, brands)
    select_cols = ", ".join(qident(col) for col in columns)
    sql = f"""
    SELECT {select_cols}
    FROM {table}
    WHERE {filters}
    ORDER BY {qident(date_col)} DESC
    LIMIT {int(limit)}
    """
    return run_query(sql)


def output_roots() -> list[Path]:
    roots = [DEFAULT_OUTPUT_ROOT, APP_ROOT / "output", Path.cwd() / "output"]
    unique: list[Path] = []
    for root in roots:
        resolved = root.resolve() if root.exists() else root
        if resolved not in unique:
            unique.append(resolved)
    return unique


def discover_output_jobs() -> dict[str, Path]:
    jobs: dict[str, Path] = {}
    for root in output_roots():
        if not root.exists():
            continue
        candidates = [root] + [p for p in root.iterdir() if p.is_dir()]
        for candidate in candidates:
            has_data = (
                list(candidate.glob("results_batch_*.json"))
                or (candidate / "drift_history.json").exists()
                or (candidate / REVIEW_FILENAME).exists()
            )
            if has_data:
                label = candidate.name if candidate != root else root.name
                jobs[label] = candidate
    return dict(sorted(jobs.items()))


@st.cache_data(show_spinner=False)
def load_json_file(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def normalize_experiment_row(row: dict, approach: str, run_at: str) -> dict:
    return {
        "approach": approach,
        "model": row.get("config", row.get("model", "unknown")),
        "batch_size": row.get("batch_size", row.get("batch_cap", 0)),
        "overall_accuracy": row.get("overall_accuracy"),
        "boolean_accuracy": row.get("metric_boolean_accuracy", row.get("boolean_accuracy")),
        "categorical_semantic": row.get("metric_categorical_semantic", row.get("categorical_semantic")),
        "string_semantic": row.get("metric_string_semantic", row.get("string_semantic")),
        "list_jaccard": row.get("metric_list_jaccard", row.get("list_jaccard")),
        "strict_blank_rate": row.get("strict_completeness", {}).get("blank_rate", row.get("strict_blank_rate")),
        "false_fill_rate": row.get("false_fill", {}).get("false_fill_rate", row.get("false_fill_rate")),
        "cost_per_transcript": row.get("avg_cost_per_transcript", row.get("cost_per_transcript")),
        "e2e_latency_avg": row.get("e2e_latency_avg"),
        "run_at": run_at,
    }


def load_experiment_data(output_dir: Path) -> pd.DataFrame:
    patterns = {
        "flat": "results_batch_*.json",
        "flat-linkage": "results_flat_linkage_*.json",
        "semantic": "results_semantic_cap_*.json",
    }
    rows: list[dict] = []
    for approach, pattern in patterns.items():
        for path in sorted(output_dir.glob(pattern)):
            data = load_json_file(str(path))
            run_at = data.get("run_at", path.stem)
            for entry in data.get("comparison_table", []):
                rows.append(normalize_experiment_row(entry, approach, run_at))
    df = pd.DataFrame(rows)
    if not df.empty:
        df["batch_size"] = pd.to_numeric(df["batch_size"], errors="coerce").fillna(0).astype(int)
    return df


def load_per_attribute_data(output_dir: Path) -> pd.DataFrame:
    records: list[dict] = []
    for path in sorted(output_dir.glob("results_batch_*.json")):
        data = load_json_file(str(path))
        batch_by_model = {
            row.get("config", "unknown"): row.get("batch_size")
            for row in data.get("comparison_table", [])
        }
        for full in data.get("full_results", []):
            model = full.get("config_name", "unknown")
            per_attr = full.get("accuracy", {}).get("per_attribute", {})
            for attr_name, attr_data in per_attr.items():
                records.append(
                    {
                        "model": model,
                        "batch_size": batch_by_model.get(model),
                        "attribute": attr_name,
                        "type": attr_data.get("type", "unknown"),
                        "accuracy": extract_accuracy(attr_data),
                    }
                )
    return pd.DataFrame(records)


def extract_accuracy(attr_data: dict) -> float | None:
    for key in ("accuracy", "semantic_match_rate", "jaccard_similarity"):
        if key in attr_data:
            return attr_data[key]
    return None


def load_drift_history(output_dir: Path) -> list[dict]:
    path = output_dir / "drift_history.json"
    if not path.exists():
        return []
    data = load_json_file(str(path))
    return data if isinstance(data, list) else []


def build_drift_df(snaps: list[dict]) -> pd.DataFrame:
    rows: list[dict] = []
    for snap in snaps:
        overall = snap.get("overall", {})
        snap_date = snap.get("snapshot_date", str(snap.get("timestamp", ""))[:10])
        rows.append(
            {
                "snapshot_date": snap_date,
                "snapshot": snap.get("run_label", snap_date),
                "timestamp": snap.get("timestamp", ""),
                "model": snap.get("model", "unknown"),
                "transcript_count": snap.get("transcript_count"),
                "attribute_count": snap.get("attribute_count"),
                "overall_accuracy": overall.get("overall_accuracy"),
                "boolean_accuracy": overall.get("boolean_accuracy"),
                "categorical_semantic": overall.get("categorical_semantic"),
                "string_semantic": overall.get("string_semantic"),
                "list_jaccard": overall.get("list_jaccard"),
                "strict_blank_rate": overall.get("strict_blank_rate"),
                "false_fill_rate": overall.get("false_fill_rate"),
                "cost_per_transcript": overall.get("avg_cost_per_transcript"),
            }
        )
    return pd.DataFrame(rows)


def find_review_files() -> dict[str, Path]:
    found: dict[str, Path] = {}
    for root in output_roots():
        if not root.exists():
            continue
        candidates = [root] + [p for p in root.iterdir() if p.is_dir()]
        for candidate in candidates:
            review_path = candidate / REVIEW_FILENAME
            if review_path.exists():
                found[candidate.name] = review_path
    return dict(sorted(found.items()))


def review_state_uses_snowflake() -> bool:
    return REVIEW_STATE_BACKEND == "snowflake" and bool(REVIEW_STATE_TABLE)


def review_scope(output_dir: Path) -> str:
    configured = os.getenv("OPTIC_DASHBOARD_REVIEW_SCOPE", "").strip()
    return configured or output_dir.name


def run_uncached_query(sql: str) -> pd.DataFrame:
    conn = create_snowflake_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            columns = [desc[0].upper() for desc in cur.description]
            return pd.DataFrame(rows, columns=columns)
        finally:
            cur.close()
    finally:
        conn.close()


def execute_snowflake_statement(sql: str) -> None:
    conn = create_snowflake_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
        finally:
            cur.close()
    finally:
        conn.close()


def ensure_review_state_table() -> str:
    table = fq_table_name(REVIEW_STATE_TABLE)
    execute_snowflake_statement(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            REVIEW_SCOPE STRING,
            SOURCE_ID STRING,
            ATTRIBUTE STRING,
            CORRECT_VALUE STRING,
            IS_DONE BOOLEAN DEFAULT FALSE,
            UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            UPDATED_BY STRING DEFAULT CURRENT_USER(),
            PRIMARY KEY (REVIEW_SCOPE, SOURCE_ID, ATTRIBUTE)
        )
        """
    )
    return table


def split_review_key(key: str) -> tuple[str, str]:
    source_id, _, attribute = key.partition("_")
    return source_id, attribute


def load_saved_reviews(output_dir: Path) -> dict[str, str]:
    if review_state_uses_snowflake():
        try:
            table = ensure_review_state_table()
            scope = sql_quote(review_scope(output_dir))
            df = run_uncached_query(
                f"""
                SELECT SOURCE_ID, ATTRIBUTE, CORRECT_VALUE
                FROM {table}
                WHERE REVIEW_SCOPE = {scope}
                  AND CORRECT_VALUE IS NOT NULL
                """
            )
            return {
                f"{row['SOURCE_ID']}_{row['ATTRIBUTE']}": row.get("CORRECT_VALUE", "")
                for _, row in df.iterrows()
            }
        except Exception as exc:
            st.warning(f"Snowflake review-state read failed; falling back to local file state: {exc}")

    path = output_dir / AUTOSAVE_FILENAME
    if not path.exists():
        return {}
    try:
        rows = load_json_file(str(path))
        return {f"{r['source_id']}_{r['attribute']}": r.get("correct_value", "") for r in rows}
    except Exception:
        return {}


def load_done_keys(output_dir: Path) -> set[str]:
    if review_state_uses_snowflake():
        try:
            table = ensure_review_state_table()
            scope = sql_quote(review_scope(output_dir))
            df = run_uncached_query(
                f"""
                SELECT SOURCE_ID, ATTRIBUTE
                FROM {table}
                WHERE REVIEW_SCOPE = {scope}
                  AND IS_DONE = TRUE
                """
            )
            return {f"{row['SOURCE_ID']}_{row['ATTRIBUTE']}" for _, row in df.iterrows()}
        except Exception as exc:
            st.warning(f"Snowflake review-state read failed; falling back to local file state: {exc}")

    path = output_dir / DONE_FILENAME
    if not path.exists():
        return set()
    try:
        return set(load_json_file(str(path)))
    except Exception:
        return set()


def save_done_keys(output_dir: Path, done_keys: set[str]) -> None:
    if review_state_uses_snowflake():
        try:
            table = ensure_review_state_table()
            scope_value = review_scope(output_dir)
            scope = sql_quote(scope_value)
            execute_snowflake_statement(
                f"""
                UPDATE {table}
                SET IS_DONE = FALSE, UPDATED_AT = CURRENT_TIMESTAMP(), UPDATED_BY = CURRENT_USER()
                WHERE REVIEW_SCOPE = {scope}
                """
            )
            for key in sorted(done_keys):
                source_id, attribute = split_review_key(key)
                execute_snowflake_statement(
                    f"""
                    MERGE INTO {table} AS target
                    USING (
                        SELECT
                            {scope} AS REVIEW_SCOPE,
                            {sql_quote(source_id)} AS SOURCE_ID,
                            {sql_quote(attribute)} AS ATTRIBUTE
                    ) AS source
                    ON target.REVIEW_SCOPE = source.REVIEW_SCOPE
                       AND target.SOURCE_ID = source.SOURCE_ID
                       AND target.ATTRIBUTE = source.ATTRIBUTE
                    WHEN MATCHED THEN UPDATE SET
                        IS_DONE = TRUE,
                        UPDATED_AT = CURRENT_TIMESTAMP(),
                        UPDATED_BY = CURRENT_USER()
                    WHEN NOT MATCHED THEN INSERT (
                        REVIEW_SCOPE, SOURCE_ID, ATTRIBUTE, IS_DONE, UPDATED_AT, UPDATED_BY
                    ) VALUES (
                        source.REVIEW_SCOPE, source.SOURCE_ID, source.ATTRIBUTE,
                        TRUE, CURRENT_TIMESTAMP(), CURRENT_USER()
                    )
                    """
                )
            return
        except Exception as exc:
            st.warning(f"Snowflake review-state write failed; falling back to local file state: {exc}")

    path = output_dir / DONE_FILENAME
    path.write_text(json.dumps(sorted(done_keys), indent=2), encoding="utf-8")


def autosave_reviews(output_dir: Path, reviewed: dict[str, str]) -> None:
    if review_state_uses_snowflake():
        try:
            table = ensure_review_state_table()
            scope = sql_quote(review_scope(output_dir))
            for key, value in reviewed.items():
                source_id, attribute = split_review_key(key)
                execute_snowflake_statement(
                    f"""
                    MERGE INTO {table} AS target
                    USING (
                        SELECT
                            {scope} AS REVIEW_SCOPE,
                            {sql_quote(source_id)} AS SOURCE_ID,
                            {sql_quote(attribute)} AS ATTRIBUTE,
                            {sql_quote(value)} AS CORRECT_VALUE
                    ) AS source
                    ON target.REVIEW_SCOPE = source.REVIEW_SCOPE
                       AND target.SOURCE_ID = source.SOURCE_ID
                       AND target.ATTRIBUTE = source.ATTRIBUTE
                    WHEN MATCHED THEN UPDATE SET
                        CORRECT_VALUE = source.CORRECT_VALUE,
                        UPDATED_AT = CURRENT_TIMESTAMP(),
                        UPDATED_BY = CURRENT_USER()
                    WHEN NOT MATCHED THEN INSERT (
                        REVIEW_SCOPE, SOURCE_ID, ATTRIBUTE, CORRECT_VALUE, IS_DONE, UPDATED_AT, UPDATED_BY
                    ) VALUES (
                        source.REVIEW_SCOPE, source.SOURCE_ID, source.ATTRIBUTE,
                        source.CORRECT_VALUE, FALSE, CURRENT_TIMESTAMP(), CURRENT_USER()
                    )
                    """
                )
            return
        except Exception as exc:
            st.warning(f"Snowflake review-state write failed; falling back to local file state: {exc}")

    rows = []
    for key, value in reviewed.items():
        source_id, attribute = split_review_key(key)
        rows.append(
            {
                "source_id": source_id,
                "attribute": attribute,
                "correct_value": value,
            }
        )
    path = output_dir / AUTOSAVE_FILENAME
    path.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")


def metric_format(metric: str) -> str:
    if "cost" in metric:
        return "${:.4f}"
    if "latency" in metric:
        return "{:.1f}"
    return "{:.2%}"


def format_ts(value: Any) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return str(value)[:19]


def pct_col(df: pd.DataFrame, cols: list[str]) -> pd.io.formats.style.Styler:
    fmt = {col: "{:.2%}" for col in cols if col in df.columns}
    return df.style.format(fmt, na_rep="")


def section_title(eyebrow: str, title: str, subtitle: str = "") -> None:
    """Render a consistent, branded section header used across every tab."""
    sub_html = f'<div class="optic-section-sub">{subtitle}</div>' if subtitle else ""
    st.markdown(
        f"""
        <div class="optic-section">
          <div class="eyebrow">{eyebrow}</div>
          <h3 class="optic-section-title">{title}</h3>
          {sub_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_empty(title: str, message: str = "", icon: str = "◇") -> None:
    """Styled empty-state card that reads better than a bare st.info()."""
    msg_html = f'<div class="optic-empty-msg">{message}</div>' if message else ""
    st.markdown(
        f"""
        <div class="optic-empty">
          <div class="optic-empty-icon">{icon}</div>
          <div class="optic-empty-title">{title}</div>
          {msg_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def severity_badges(counts: dict[str, int]) -> None:
    """Render a row of severity pills (High / Medium / Low / OK) with counts."""
    order = [("High", "high"), ("Medium", "medium"), ("Low", "low"), ("OK", "ok")]
    chips = []
    for label, cls in order:
        value = int(counts.get(label, 0))
        chips.append(
            f'<span class="sev-badge {cls}"><span class="bdot"></span>{value:,} {label}</span>'
        )
    st.markdown(f'<div class="badge-row">{"".join(chips)}</div>', unsafe_allow_html=True)


def download_df_button(df: pd.DataFrame, label: str, filename: str, key: str) -> None:
    """Reusable CSV export so every table is takeaway-ready."""
    if df is None or df.empty:
        return
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(label, data=csv, file_name=filename, mime="text/csv", key=key)


def pct_delta_label(curr: float | None, prev: float | None, suffix: str = " vs prev") -> str | None:
    """Period-over-period percent change string for st.metric deltas."""
    if curr is None or prev is None or pd.isna(curr) or pd.isna(prev) or not prev:
        return None
    return f"{(curr - prev) / prev:+.1%}{suffix}"


def build_positive_rate_anomaly_watchlist(
    positive_rate: pd.DataFrame,
    target_date: date,
    baseline_periods: int = 7,
    min_move_pp: float = 0.5,
    relative_threshold: float = 0.5,
    anomaly_score_threshold: float = 3.0,
    large_move_pp: float = 3.0,
) -> pd.DataFrame:
    """Compare each attribute's target rate with its own recent baseline."""
    if positive_rate.empty:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for attribute, group in positive_rate.groupby("attribute", dropna=False):
        history = group.sort_values("date")
        current = history[history["date"] == target_date]
        prior = history[history["date"] < target_date].tail(max(int(baseline_periods), 1))
        if current.empty or len(prior) < 3:
            continue

        target = current.iloc[-1]
        target_rate = float(target["positive_rate"])
        baseline_rate = float(prior["positive_rate"].median())
        delta_rate = target_rate - baseline_rate
        delta_pp = delta_rate * 100.0
        relative_change = (
            delta_rate / baseline_rate
            if baseline_rate > 0
            else (float("inf") if target_rate > 0 else 0.0)
        )

        absolute_deviations = (prior["positive_rate"] - baseline_rate).abs()
        variability = float(absolute_deviations.median()) * 1.4826
        if variability <= 0:
            variability = float(prior["positive_rate"].std(ddof=1))
        anomaly_score = (
            delta_rate / variability
            if variability > 0
            else (float("inf") if delta_rate > 0 else (float("-inf") if delta_rate < 0 else 0.0))
        )

        abs_move_pp = abs(delta_pp)
        unusual_relative_move = abs(relative_change) >= float(relative_threshold)
        unusual_standardized_move = abs(anomaly_score) >= float(anomaly_score_threshold)
        is_large_move = abs_move_pp >= float(large_move_pp)
        is_low_rate_anomaly = (
            abs_move_pp >= float(min_move_pp)
            and (unusual_relative_move or unusual_standardized_move)
        )
        if not (is_large_move or is_low_rate_anomaly):
            continue

        if abs_move_pp >= 10 or abs(anomaly_score) >= 10:
            severity = "High"
        elif abs_move_pp >= 3 or abs(anomaly_score) >= 5:
            severity = "Medium"
        else:
            severity = "Low"

        rows.append(
            {
                "severity": severity,
                "direction": "Jump" if delta_pp > 0 else "Drop",
                "attribute": str(attribute),
                "target_rate": target_rate,
                "usual_rate": baseline_rate,
                "change_pp": delta_pp,
                "relative_change": relative_change,
                "anomaly_score": anomaly_score,
                "positive_rows": int(target["positive_rows"]),
                "total_calls": int(target["total_calls"]),
                "baseline_periods": int(len(prior)),
            }
        )

    if not rows:
        return pd.DataFrame()

    watchlist = pd.DataFrame(rows)
    severity_rank = {"High": 0, "Medium": 1, "Low": 2}
    watchlist["_severity_rank"] = watchlist["severity"].map(severity_rank).fillna(3)
    watchlist["_abs_move"] = watchlist["change_pp"].abs()
    return (
        watchlist.sort_values(
            ["_severity_rank", "_abs_move", "attribute"],
            ascending=[True, False, True],
        )
        .drop(columns=["_severity_rank", "_abs_move"])
        .reset_index(drop=True)
    )


def health_gauge(score: float):
    """Plotly indicator gauge for the live data-health score (None if Plotly missing)."""
    if not PLOTLY_AVAILABLE:
        return None
    color = "#10b981" if score >= 85 else "#f59e0b" if score >= 70 else "#ef4444"
    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=round(score, 1),
            number={"suffix": "", "font": {"size": 42, "color": "#f8fafc"}},
            gauge={
                "axis": {"range": [0, 100], "tickcolor": "#64748b", "tickwidth": 1, "tickfont": {"color": "#64748b", "size": 10}},
                "bar": {"color": color, "thickness": 0.3},
                "bgcolor": "rgba(0,0,0,0)",
                "borderwidth": 0,
                "steps": [
                    {"range": [0, 70], "color": "rgba(239,68,68,0.14)"},
                    {"range": [70, 85], "color": "rgba(245,158,11,0.14)"},
                    {"range": [85, 100], "color": "rgba(16,185,129,0.14)"},
                ],
                "threshold": {"line": {"color": color, "width": 3}, "thickness": 0.78, "value": round(score, 1)},
            },
        )
    )
    fig.update_layout(height=232, margin=dict(l=22, r=22, t=10, b=6))
    return fig


def render_header(source_table: str, live_ok: bool, start_dt: date | None = None, end_dt: date | None = None) -> None:
    status_label = "Snowflake Connected" if live_ok else "Snowflake Unavailable"
    status_class = "" if live_ok else "offline"
    now_str = datetime.now().strftime("%a %b %d, %Y %H:%M")
    window_pill = ""
    if start_dt and end_dt:
        window_pill = (
            '<span class="status-pill"><span class="dot" style="background:#a855f7;animation:none;">'
            f'</span>{start_dt:%b %d} &ndash; {end_dt:%b %d, %Y}</span>'
        )
    st.markdown(
        f"""
        <div class="optic-hero">
          <div class="optic-hero-row">
            <div class="optic-hero-title">
              <div class="optic-hero-logo">OP</div>
              <div>
                <h1>OPTIC Production Dashboard</h1>
                <div class="optic-hero-sub">
                  Live OPTIC monitoring from Snowflake — paired with model evaluation, drift detection, and reviewer workflows.
                </div>
              </div>
            </div>
            <div class="optic-hero-meta">
              <span class="status-pill {status_class}"><span class="dot"></span>{status_label}</span>
              {window_pill}
              <span class="status-pill"><span class="dot" style="background:#06b6d4;animation:none;"></span>{now_str}</span>
              <span class="status-pill">Source <code>{source_table}</code></span>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_live_overview(
    source_table: str,
    columns: list[dict[str, str]],
    date_col: str,
    job_col: str | None,
    brand_col: str | None,
    connection_col: str | None,
    duration_col: str | None,
    start_dt: date,
    end_dt: date,
    selected_jobs: list[str],
    selected_brands: list[str],
    selected_attrs: list[str],
    attr_types: dict[str, str],
    trend_grain: str,
) -> None:
    section_title(
        "Live Monitoring",
        "OPTIC Production Snapshot",
        "Coverage, recency, and volume computed directly from the production OPTIC attribute table. "
        "Labeled accuracy lives in the evaluation tabs.",
    )

    with st.spinner("Loading live OPTIC summary..."):
        summary = fetch_live_summary(
            source_table,
            date_col,
            start_dt,
            end_dt,
            job_col,
            selected_jobs,
            brand_col,
            selected_brands,
            connection_col,
            duration_col,
        )
    if summary.empty:
        render_empty(
            "No rows matched the current filters",
            "Widen the date range or clear the job / brand filters in the sidebar.",
        )
        return

    row = summary.iloc[0]

    # Previous equal-length window powers the period-over-period deltas.
    prev_row = None
    try:
        window_days = max((end_dt - start_dt).days, 0)
        prev_end = start_dt - timedelta(days=1)
        prev_start = prev_end - timedelta(days=window_days)
        prev_summary = fetch_live_summary(
            source_table,
            date_col,
            prev_start,
            prev_end,
            job_col,
            selected_jobs,
            brand_col,
            selected_brands,
            connection_col,
            duration_col,
        )
        if not prev_summary.empty:
            prev_row = prev_summary.iloc[0]
    except Exception:
        prev_row = None

    def _prev(colname: str) -> float | None:
        if prev_row is None:
            return None
        value = prev_row.get(colname)
        return None if pd.isna(value) else value

    rows_now = int(row["TOTAL_ROWS"])
    conn_now = None if pd.isna(row["DISTINCT_CONNECTIONS"]) else int(row["DISTINCT_CONNECTIONS"])

    days_stale = None
    freshness_delta = None
    try:
        latest_dt = pd.to_datetime(row["LATEST_INTERACTION"])
        if pd.notna(latest_dt):
            if getattr(latest_dt, "tzinfo", None) is not None:
                latest_dt = latest_dt.tz_localize(None)
            days_stale = max((pd.Timestamp(end_dt) - latest_dt.normalize()).days, 0)
            freshness_delta = "current" if days_stale <= 0 else f"{days_stale}d behind"
    except Exception:
        days_stale = None

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Rows", f"{rows_now:,}", pct_delta_label(rows_now, _prev("TOTAL_ROWS")))
    c2.metric(
        "Connections",
        "N/A" if conn_now is None else f"{conn_now:,}",
        pct_delta_label(conn_now, _prev("DISTINCT_CONNECTIONS")) if conn_now is not None else None,
    )
    c3.metric("Jobs", "N/A" if pd.isna(row["JOB_COUNT"]) else f"{int(row['JOB_COUNT']):,}")
    c4.metric("Brands", "N/A" if pd.isna(row["BRAND_COUNT"]) else f"{int(row['BRAND_COUNT']):,}")
    c5.metric("Latest Interaction", format_ts(row["LATEST_INTERACTION"]), freshness_delta, delta_color="off")

    # Coverage feeds both the health score and the table below; the cached read is free the second time.
    quality = fetch_attribute_quality(
        source_table,
        date_col,
        start_dt,
        end_dt,
        job_col,
        selected_jobs,
        brand_col,
        selected_brands,
        selected_attrs,
        attr_types,
    )
    coverage_score = (
        float(quality["populated_rate"].mean())
        if not quality.empty and "populated_rate" in quality
        else None
    )
    recency_score = None if days_stale is None else max(0.0, 1.0 - max(days_stale - 1, 0) / 7.0)
    if coverage_score is not None and recency_score is not None:
        health = 100.0 * (0.6 * coverage_score + 0.4 * recency_score)
    elif coverage_score is not None:
        health = 100.0 * coverage_score
    elif recency_score is not None:
        health = 100.0 * recency_score
    else:
        health = None

    st.markdown('<div style="height:4px"></div>', unsafe_allow_html=True)
    gauge_col, trend_col = st.columns([1, 2], gap="large")

    with gauge_col:
        section_title("Health", "Data Health Score")
        if health is None:
            render_empty("Not enough signal", "Select attributes to score coverage health.")
        else:
            fig_gauge = health_gauge(health)
            if fig_gauge is not None:
                st.plotly_chart(fig_gauge, use_container_width=True)
            else:
                st.metric("Data Health", f"{health:.0f} / 100")
            cov_txt = f"{coverage_score:.0%}" if coverage_score is not None else "—"
            rec_txt = f"{recency_score:.0%}" if recency_score is not None else "—"
            verdict = "Healthy" if health >= 85 else "Watch" if health >= 70 else "Degraded"
            st.caption(f"{verdict} · Coverage {cov_txt} · Recency {rec_txt}")

    with trend_col:
        section_title("Throughput", "Volume Trend")
        trend = fetch_volume_trend(
            source_table,
            date_col,
            start_dt,
            end_dt,
            trend_grain,
            job_col,
            selected_jobs,
            brand_col,
            selected_brands,
        )
        if trend.empty:
            render_empty("No volume in range")
        else:
            trend.columns = [c.lower() for c in trend.columns]
            fig = px.line(
                trend,
                x="period",
                y="row_count",
                markers=True,
                labels={"period": "Period", "row_count": "Rows"},
            )
            fig.update_traces(
                line=dict(width=2.6, color="#818cf8"),
                marker=dict(size=6),
                fill="tozeroy",
                fillcolor="rgba(99,102,241,0.16)",
            )
            fig.update_layout(height=292, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig, use_container_width=True)

    section_title("Distribution", "Top Jobs & Brands")
    left, right = st.columns(2, gap="large")
    if job_col:
        job_counts = fetch_dimension_counts(
            source_table,
            date_col,
            start_dt,
            end_dt,
            job_col,
            job_col,
            selected_jobs,
            brand_col,
            selected_brands,
        )
        if not job_counts.empty:
            job_counts.columns = [c.lower() for c in job_counts.columns]
            job_counts = job_counts.sort_values("row_count").tail(12)
            fig_jobs = px.bar(
                job_counts,
                x="row_count",
                y="value",
                orientation="h",
                title="Top Jobs",
                labels={"row_count": "Rows", "value": ""},
            )
            fig_jobs.update_traces(marker_color="#6366f1")
            fig_jobs.update_layout(height=360, showlegend=False, margin=dict(l=10, r=10, t=40, b=10))
            left.plotly_chart(fig_jobs, use_container_width=True)
    if brand_col:
        brand_counts = fetch_dimension_counts(
            source_table,
            date_col,
            start_dt,
            end_dt,
            brand_col,
            job_col,
            selected_jobs,
            brand_col,
            selected_brands,
        )
        if not brand_counts.empty:
            brand_counts.columns = [c.lower() for c in brand_counts.columns]
            brand_counts = brand_counts.sort_values("row_count").tail(12)
            fig_brands = px.bar(
                brand_counts,
                x="row_count",
                y="value",
                orientation="h",
                title="Top Brands",
                labels={"row_count": "Rows", "value": ""},
            )
            fig_brands.update_traces(marker_color="#06b6d4")
            fig_brands.update_layout(height=360, showlegend=False, margin=dict(l=10, r=10, t=40, b=10))
            right.plotly_chart(fig_brands, use_container_width=True)

    section_title(
        "Completeness",
        "Attribute Coverage",
        "Populated vs blank rates for the attributes selected in the sidebar.",
    )
    if quality.empty:
        render_empty(
            "Select at least one attribute",
            "Use the Attributes control in the sidebar to choose which fields to monitor.",
        )
    else:
        display_cols = [
            "attribute",
            "data_type",
            "total_rows",
            "populated_rate",
            "blank_rate",
            "true_rate",
            "distinct_values",
            "avg_length",
        ]
        display = quality[[c for c in display_cols if c in quality.columns]].copy()
        display["total_rows"] = display["total_rows"].astype(int)
        st.dataframe(
            pct_col(display, ["populated_rate", "blank_rate", "true_rate"]).format(
                {"avg_length": "{:.1f}", "distinct_values": "{:.0f}", "total_rows": "{:,}"},
                na_rep="",
            ),
            use_container_width=True,
            hide_index=True,
        )
        download_df_button(display, "Download coverage CSV", "optic_attribute_coverage.csv", "dl_coverage")

        fig_cov = px.bar(
            quality.sort_values("populated_rate", ascending=True),
            x="populated_rate",
            y="attribute",
            color="data_type",
            orientation="h",
            labels={"populated_rate": "Populated Rate", "attribute": "Attribute"},
        )
        fig_cov.update_layout(height=max(320, 28 * len(quality)), xaxis_tickformat=".0%")
        st.plotly_chart(fig_cov, use_container_width=True)


def render_attribute_drift(
    source_table: str,
    date_col: str,
    job_col: str | None,
    brand_col: str | None,
    start_dt: date,
    end_dt: date,
    selected_jobs: list[str],
    selected_brands: list[str],
    selected_attrs: list[str],
    attr_types: dict[str, str],
    trend_grain: str,
) -> None:
    section_title(
        "Trends",
        "Attribute Drift",
        "Spot changes in blank rates, boolean true rates, and coverage for the selected attributes over time.",
    )

    ts = fetch_attribute_time_series(
        source_table,
        date_col,
        start_dt,
        end_dt,
        trend_grain,
        job_col,
        selected_jobs,
        brand_col,
        selected_brands,
        selected_attrs,
        attr_types,
    )
    if ts.empty:
        render_empty(
            "No drift data for the current selection",
            "Pick attributes that have live rows in the chosen date range from the sidebar.",
        )
        return

    metric = st.selectbox(
        "Drift metric",
        ["populated_rate", "blank_rate", "true_rate", "row_count"],
        format_func=lambda x: x.replace("_", " ").title(),
    )
    y_col = "total_rows" if metric == "row_count" else metric
    plot_df = ts.copy()
    fig = px.line(
        plot_df,
        x="period",
        y=y_col,
        color="attribute",
        markers=True,
        labels={"period": "Period", y_col: metric.replace("_", " ").title()},
    )
    if metric != "row_count":
        fig.update_layout(yaxis_tickformat=".0%")
    fig.update_layout(height=450)
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("#### Latest Period Watchlist")
    latest_period = plot_df["period"].max()
    latest = plot_df[plot_df["period"] == latest_period].copy()
    latest["health_flag"] = "OK"
    latest.loc[latest["blank_rate"] >= 0.35, "health_flag"] = "High blank rate"
    latest.loc[(latest["data_type"] == "BOOLEAN") & latest["true_rate"].isna(), "health_flag"] = "No true-rate data"
    latest_display = latest[
        [
            "attribute",
            "data_type",
            "total_rows",
            "populated_rate",
            "blank_rate",
            "true_rate",
            "health_flag",
        ]
    ].sort_values(["health_flag", "blank_rate"], ascending=[True, False])
    st.dataframe(
        pct_col(latest_display, ["populated_rate", "blank_rate", "true_rate"]).format(
            {"total_rows": "{:,}"},
            na_rep="",
        ),
        use_container_width=True,
        hide_index=True,
    )
    download_df_button(latest_display, "Download watchlist CSV", "optic_attribute_watchlist.csv", "dl_watchlist")

    st.markdown("#### Value Mix")
    attr = st.selectbox("Attribute values", selected_attrs)
    top_n = st.slider("Top values", 5, 50, 15)
    value_df = fetch_top_values(
        source_table,
        date_col,
        start_dt,
        end_dt,
        job_col,
        selected_jobs,
        brand_col,
        selected_brands,
        attr,
        attr_types.get(attr, "TEXT"),
        top_n,
    )
    if not value_df.empty:
        st.plotly_chart(
            px.bar(
                value_df.sort_values("row_count", ascending=True),
                x="row_count",
                y="value",
                orientation="h",
                labels={"row_count": "Rows", "value": "Value"},
            ),
            use_container_width=True,
        )
        st.dataframe(
            value_df.style.format({"row_count": "{:,}", "share": "{:.2%}"}),
            use_container_width=True,
            hide_index=True,
        )


def render_volume_drift(
    source_table: str,
    columns: list[dict[str, str]],
    date_col: str,
    job_col: str | None,
    brand_col: str | None,
    start_dt: date,
    end_dt: date,
    selected_jobs: list[str],
    selected_brands: list[str],
    selected_attrs: list[str],
    attr_types: dict[str, str],
    metadata_df: pd.DataFrame,
    trend_grain: str,
) -> None:
    section_title(
        "Anomaly",
        "Value Distribution & Spikes",
        "Track the volume mix of each attribute's values over time and catch sudden spikes — values "
        "whose share of an attribute jumps versus their recent baseline. Works best on categorical fields.",
    )

    source_attrs = candidate_attribute_columns(columns)
    if not source_attrs:
        render_empty(
            "No source attributes available",
            "The source table did not expose any supported non-dimension attribute columns to scan.",
        )
        return
    source_attr_set = set(source_attrs)

    json_attrs: list[str] = []
    json_attr_total = 0
    if metadata_df is not None and not metadata_df.empty:
        json_columns = [
            str(column).upper()
            for column in metadata_df["column"].dropna().tolist()
            if str(column).strip()
        ]
        json_columns = list(dict.fromkeys(json_columns))
        json_attr_total = len(json_columns)
        json_attrs = [column for column in json_columns if column in source_attr_set]

    value_attrs = json_attrs or source_attrs
    missing_json_attrs = max(json_attr_total - len(json_attrs), 0)

    attr_label_lookup: dict[str, str] = {}
    if metadata_df is not None and not metadata_df.empty:
        attr_label_lookup = {
            str(r["column"]).upper(): f"{r['attribute']} ({r['result_type']})"
            for _, r in metadata_df.iterrows()
        }
    for attr in value_attrs:
        attr_u = str(attr).upper()
        source_label = f"{attr_u} ({attr_types.get(attr_u, 'UNKNOWN')})"
        if attr_u in attr_label_lookup:
            attr_label_lookup[attr_u] = f"{attr_u} - {attr_label_lookup[attr_u]} / {attr_types.get(attr_u, 'UNKNOWN')}"
        else:
            attr_label_lookup[attr_u] = source_label

    attr_search = st.text_input(
        "Find catalog attribute",
        "",
        key="value_spike_attr_search",
        help="Type part of an attribute name, such as do_not_contact, to narrow the ODIN attribute catalog.",
    )
    visible_attrs = value_attrs
    if attr_search.strip():
        needle = attr_search.strip().lower()
        visible_attrs = [
            attr
            for attr in value_attrs
            if needle in attr.lower() or needle in attr_label_lookup.get(str(attr).upper(), "").lower()
        ]
        if not visible_attrs:
            st.warning(f"No catalog attributes matched `{attr_search.strip()}`.")
            return

    if attr_search.strip():
        default_scan_attrs = visible_attrs.copy()
    else:
        default_scan_attrs = [attr for attr in VALUE_SPIKE_DEFAULT_ATTRIBUTES if attr in visible_attrs]
        if not default_scan_attrs:
            default_scan_attrs = default_attribute_selection(visible_attrs)

    grain_options = ["day", "week", "month"]
    ctrl = st.columns([1.5, 1, 1, 1])
    scan_attrs = ctrl[0].multiselect(
        "Attributes to scan",
        visible_attrs,
        default=default_scan_attrs,
        key=f"value_spike_attrs_v2_{normalize_match_key(attr_search)}",
        format_func=lambda c: attr_label_lookup.get(str(c).upper(), c),
        help=(
            "Uses attributes from the ODIN metadata catalog. "
            "Only catalog attributes that exist as source-table columns can be queried."
        ),
    )
    if json_attrs:
        st.caption(
            f"Scanning {len(json_attrs):,} catalog attributes found in the source table"
            + (f"; {missing_json_attrs:,} catalog attributes were not present as columns." if missing_json_attrs else ".")
        )
    else:
        st.caption("No catalog attributes matched source columns, so the tab is falling back to source columns.")
    grain = ctrl[1].selectbox(
        "Grain",
        grain_options,
        index=grain_options.index(trend_grain) if trend_grain in grain_options else 0,
    )
    window_days = max((end_dt - start_dt).days + 1, 2)
    lookback = ctrl[2].slider("Lookback days", 2, 90, min(14, window_days))
    top_n = ctrl[3].slider("Top values", 4, 20, 8)

    if not scan_attrs:
        st.warning("Select at least one attribute to scan.")
        return

    query_start = end_dt - timedelta(days=int(lookback) - 1)
    spec_json = value_distribution_specs(scan_attrs, attr_types, metadata_df, selected_jobs)
    all_spec_json = value_distribution_specs(value_attrs, attr_types, metadata_df, selected_jobs)

    try:
        with st.spinner(f"Scanning value distributions for {len(scan_attrs)} attributes from Snowflake..."):
            dist = fetch_expected_value_distribution(
                source_table,
                date_col,
                query_start,
                end_dt,
                grain,
                job_col,
                selected_jobs,
                brand_col,
                selected_brands,
                spec_json,
            )
    except Exception as exc:
        st.error("Value distribution scan failed before results were returned.")
        st.exception(exc)
        return

    if dist.empty:
        render_empty("No values returned", "Widen the lookback window or relax the sidebar filters.")
        return

    dist = dist.copy()
    dist["date"] = pd.to_datetime(dist["period"], errors="coerce").dt.date
    dist = dist.dropna(subset=["date"])
    if dist.empty:
        render_empty("No dated values returned")
        return

    expected_lookup = value_spike_expected_lookup(metadata_df)
    dist["raw_value"] = dist.apply(
        lambda row: bucket_value_for_spikes(str(row["attribute"]), row["raw_value"], expected_lookup),
        axis=1,
    )
    dist = (
        dist.groupby(["period", "date", "attribute", "data_type", "raw_value"], as_index=False)["row_count"]
        .sum()
        .sort_values(["period", "attribute", "row_count"], ascending=[True, True, False])
    )

    periods = sorted(dist["date"].unique())
    inferred_types = infer_value_result_types(dist, attr_types)
    dist["inferred_result_type"] = dist["attribute"].map(lambda attr: inferred_types.get(str(attr).upper(), "Unknown"))
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Attributes Scanned", f"{dist['attribute'].nunique():,}")
    m2.metric("Distinct Values", f"{dist[['attribute', 'raw_value']].drop_duplicates().shape[0]:,}")
    m3.metric("Periods", f"{len(periods):,}")
    m4.metric("Window", f"{query_start:%b %d} – {end_dt:%b %d}")

    specs = json.loads(spec_json)
    selected_flag_attrs = {
        str(spec.get("attribute") or "").lower()
        for spec in specs
        if str(spec.get("value_mode") or "").upper() == "TEXT_PRESENCE_3_WORDS"
        or normalize_result_type_label(str(spec.get("result_type") or "")) in {"BOOL", "BOOLEMPTY", "BOOLEAN"}
        or str(spec.get("data_type") or "").upper() == "BOOLEAN"
    }
    selected_flag_attrs = {attr for attr in selected_flag_attrs if attr}
    all_specs = json.loads(all_spec_json)
    flag_specs = [
        spec
        for spec in all_specs
        if str(spec.get("value_mode") or "").upper() == "TEXT_PRESENCE_3_WORDS"
        or normalize_result_type_label(str(spec.get("result_type") or "")) in {"BOOL", "BOOLEMPTY", "BOOLEAN"}
        or str(spec.get("data_type") or "").upper() == "BOOLEAN"
    ]

    st.markdown("#### Positive Flag Rate")
    st.caption(
        "Percent of positive flags by period: value `1` divided by total calls. "
        "This uses BOOL, BOOLEMPTY, source BOOLEAN, and 3-word text-presence fields only. "
        "The jump/drop watchlist scans every matching catalog attribute, not only the chart selection."
    )
    try:
        with st.spinner(f"Calculating positive rates for {len(flag_specs):,} flag-style attributes..."):
            positive_rate = fetch_positive_flag_rates(
                source_table,
                date_col,
                query_start,
                end_dt,
                grain,
                job_col,
                selected_jobs,
                brand_col,
                selected_brands,
                json.dumps(flag_specs, default=str, sort_keys=True),
            )
    except Exception as exc:
        st.error("Positive flag-rate scan failed before results were returned.")
        st.exception(exc)
        positive_rate = pd.DataFrame()

    if positive_rate.empty:
        render_empty(
            "No flag-style attributes in this scan",
            "No BOOL, BOOLEMPTY, source BOOLEAN, or 3-word text-presence fields returned data.",
        )
    else:
        positive_rate = positive_rate.copy()
        positive_rate["date"] = pd.to_datetime(positive_rate["period"], errors="coerce").dt.date
        positive_rate = positive_rate.dropna(subset=["date"])
        flag_periods = sorted(positive_rate["date"].unique())

        control_cols = st.columns([1.4, 1, 1, 1])
        target_date = control_cols[0].selectbox(
            "Flag date",
            options=list(reversed(flag_periods)),
            index=0,
            format_func=lambda value: value.strftime("%b %d, %Y"),
            help="Compare this period with the prior baseline periods.",
        )
        max_baseline_periods = max(min(30, len([period for period in flag_periods if period < target_date])), 3)
        baseline_periods = control_cols[1].slider(
            "Baseline periods",
            3,
            max_baseline_periods,
            min(7, max_baseline_periods),
            help="The usual rate is the median of these prior available periods.",
        )
        min_move_pp = control_cols[2].number_input(
            "Minimum move (pp)",
            min_value=0.05,
            max_value=20.0,
            value=0.5,
            step=0.05,
            help="Minimum absolute percentage-point movement for low-rate anomalies.",
        )
        relative_threshold_pct = control_cols[3].number_input(
            "Minimum relative move (%)",
            min_value=10,
            max_value=500,
            value=50,
            step=10,
            format="%d",
            help="Relative jump/drop required unless the move is at least 3 percentage points.",
        )

        target_positive = positive_rate[positive_rate["date"] == target_date].copy()
        target_positive = target_positive.sort_values(
            ["positive_rate", "positive_rows", "attribute"],
            ascending=[False, False, True],
        )
        chart_flag_attrs = [attribute for attribute in selected_flag_attrs if attribute in set(target_positive["attribute"])]
        if not chart_flag_attrs:
            chart_flag_attrs = target_positive.head(20)["attribute"].tolist()
        positive_plot = positive_rate[positive_rate["attribute"].isin(chart_flag_attrs)].copy()
        positive_plot["attribute_label"] = positive_plot["attribute"].map(
            lambda attr: attr_label_lookup.get(str(attr).upper(), str(attr).upper())
        )
        fig_positive = px.line(
            positive_plot,
            x="date",
            y="positive_rate",
            color="attribute_label",
            markers=True,
            hover_data={
                "positive_rows": ":,",
                "total_calls": ":,",
                "positive_rate": ":.4%",
                "attribute_label": True,
            },
            labels={
                "date": "Date",
                "positive_rate": "Positive flags / total calls",
                "attribute_label": "Attribute",
            },
        )
        fig_positive.update_layout(height=430, yaxis_tickformat=".3%", margin=dict(l=10, r=10, t=10, b=10))
        flag_date_marker = pd.Timestamp(target_date).to_pydatetime()
        fig_positive.add_vline(
            x=flag_date_marker,
            line_dash="dot",
            line_color="#f59e0b",
        )
        fig_positive.add_annotation(
            x=flag_date_marker,
            y=1,
            xref="x",
            yref="paper",
            text=f"Flag date: {target_date:%b %d}",
            showarrow=False,
            xanchor="left",
            yanchor="bottom",
            font=dict(color="#f59e0b"),
        )
        st.plotly_chart(fig_positive, use_container_width=True)

        st.markdown("##### Flag-Rate Jump / Drop Watchlist")
        st.caption(
            "Flags a move of at least the selected minimum when it is also at least the selected relative change "
            "or 3 robust deviations from the attribute's usual rate. Any move of 3 percentage points or more is flagged."
        )
        watchlist = build_positive_rate_anomaly_watchlist(
            positive_rate,
            target_date,
            baseline_periods=baseline_periods,
            min_move_pp=float(min_move_pp),
            relative_threshold=float(relative_threshold_pct) / 100.0,
        )
        if watchlist.empty:
            render_empty(
                "No unusual flag-rate moves",
                f"No attributes crossed the jump/drop thresholds on {target_date:%b %d, %Y}.",
            )
        else:
            watchlist_display = watchlist.copy()
            watchlist_display["attribute"] = watchlist_display["attribute"].map(
                lambda attr: attr_label_lookup.get(str(attr).upper(), str(attr).upper())
            )
            st.dataframe(
                watchlist_display.style.format(
                    {
                        "target_rate": "{:.4%}",
                        "usual_rate": "{:.4%}",
                        "change_pp": "{:+.4f}",
                        "relative_change": "{:+.1%}",
                        "anomaly_score": "{:+.1f}",
                        "positive_rows": "{:,}",
                        "total_calls": "{:,}",
                        "baseline_periods": "{:,}",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )
            download_df_button(
                watchlist,
                "Download jump/drop watchlist CSV",
                f"optic_positive_flag_watchlist_{target_date:%Y-%m-%d}.csv",
                f"dl_positive_flag_watchlist_{target_date:%Y%m%d}",
            )

        target_display = target_positive[["attribute", "positive_rows", "total_calls", "positive_rate"]].head(40).copy()
        target_display["attribute"] = target_display["attribute"].map(
            lambda attr: attr_label_lookup.get(str(attr).upper(), str(attr).upper())
        )
        st.dataframe(
            target_display.style.format(
                {
                    "positive_rows": "{:,}",
                    "total_calls": "{:,}",
                    "positive_rate": "{:.4%}",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
        download_df_button(
            positive_rate,
            "Download positive flag rates CSV",
            "optic_positive_flag_rates.csv",
            "dl_positive_flag_rates",
        )

    st.markdown("#### Spike Radar")
    st.caption(
        "Each value's share in the latest period vs. its baseline (all earlier periods in the window). "
        "Sorted by the biggest share movement."
    )
    if len(periods) < 2:
        st.info("Spike detection needs at least 2 periods — increase the lookback window or use day grain.")
    else:
        min_count = st.slider(
            "Min latest-period count",
            1,
            50,
            5,
            help="Hide tiny categories so the radar focuses on meaningful spikes.",
        )

        latest = periods[-1]
        latest_df = dist[dist["date"] == latest]
        prior_df = dist[dist["date"] != latest]

        lat = (
            latest_df.groupby(["attribute", "raw_value"], as_index=False)["row_count"]
            .sum()
            .rename(columns={"row_count": "latest_count"})
        )
        pri = (
            prior_df.groupby(["attribute", "raw_value"], as_index=False)["row_count"]
            .sum()
            .rename(columns={"row_count": "prior_count"})
        )
        merged = lat.merge(pri, on=["attribute", "raw_value"], how="outer")
        merged[["latest_count", "prior_count"]] = merged[["latest_count", "prior_count"]].fillna(0.0)
        lat_tot = latest_df.groupby("attribute")["row_count"].sum().rename("latest_total")
        pri_tot = prior_df.groupby("attribute")["row_count"].sum().rename("prior_total")
        merged = merged.merge(lat_tot, on="attribute", how="left").merge(pri_tot, on="attribute", how="left")
        merged[["latest_total", "prior_total"]] = merged[["latest_total", "prior_total"]].fillna(0.0)
        merged["latest_share"] = merged["latest_count"] / merged["latest_total"].clip(lower=1)
        merged["baseline_share"] = merged["prior_count"] / merged["prior_total"].clip(lower=1)
        merged["delta_pp"] = (merged["latest_share"] - merged["baseline_share"]) * 100.0
        merged["inferred_result_type"] = merged["attribute"].map(
            lambda attr: inferred_types.get(str(attr).upper(), "Unknown")
        )
        merged["growth"] = [
            (ls / ps) if ps > 0 else (float("inf") if ls > 0 else 0.0)
            for ls, ps in zip(merged["latest_share"], merged["baseline_share"])
        ]
        merged = merged[merged["latest_count"] >= min_count].copy()

        if merged.empty:
            render_empty(
                "No values above the minimum count",
                "Lower the minimum latest-period count, or widen the lookback window.",
            )
        else:
            def _severity(delta_pp: float, growth: float, latest_share: float) -> str:
                magnitude = abs(delta_pp)
                if magnitude >= 15 or (growth >= 3 and latest_share >= 0.05):
                    return "High"
                if magnitude >= 7 or growth >= 2:
                    return "Medium"
                if magnitude >= 3:
                    return "Low"
                return "OK"

            merged["severity"] = [
                _severity(d, g, s)
                for d, g, s in zip(merged["delta_pp"], merged["growth"], merged["latest_share"])
            ]
            merged["direction"] = merged["delta_pp"].map(
                lambda x: "▲ spike" if x > 0 else ("▼ drop" if x < 0 else "—")
            )
            merged["abs_move"] = merged["delta_pp"].abs()
            merged = merged.sort_values("abs_move", ascending=False)

            movers = merged[merged["severity"] != "OK"]
            counts = movers["severity"].value_counts().to_dict() if not movers.empty else {}
            severity_badges({str(k): int(v) for k, v in counts.items()})

            if movers.empty:
                render_empty(
                    "No notable spikes",
                    "No value moved enough versus its baseline to flag. Try a longer lookback or a lower minimum count.",
                )
            else:
                radar = movers.head(40).copy()
                radar["latest_count"] = radar["latest_count"].astype(int)
                display = radar[
                    [
                        "severity",
                        "direction",
                        "attribute",
                        "inferred_result_type",
                        "raw_value",
                        "latest_share",
                        "baseline_share",
                        "delta_pp",
                        "growth",
                        "latest_count",
                    ]
                ].rename(columns={"raw_value": "value"})
                display["growth"] = display["growth"].replace([float("inf")], pd.NA)
                st.dataframe(
                    display.style.format(
                        {
                            "latest_share": "{:.1%}",
                            "baseline_share": "{:.1%}",
                            "delta_pp": "{:+.1f} pp",
                            "growth": "{:.1f}×",
                            "latest_count": "{:,}",
                        },
                        na_rep="new",
                    ),
                    use_container_width=True,
                    hide_index=True,
                )
                download_df_button(display, "Download spike radar CSV", "optic_spike_radar.csv", "dl_spike_radar")

                bar = movers.head(15).copy()
                bar["label"] = bar["attribute"].astype(str) + " = " + bar["raw_value"].astype(str)
                fig_bar = px.bar(
                    bar.sort_values("delta_pp"),
                    x="delta_pp",
                    y="label",
                    color="severity",
                    orientation="h",
                    labels={"delta_pp": "Share change (pp)", "label": ""},
                )
                fig_bar.update_layout(height=max(320, len(bar) * 26), margin=dict(l=10, r=10, t=10, b=10))
                st.plotly_chart(fig_bar, use_container_width=True)

    st.markdown("#### Attribute Deep Dive")
    focus_options = sorted(dist["attribute"].unique())
    focus = st.selectbox(
        "Attribute",
        focus_options,
        format_func=lambda a: f"{attr_label_lookup.get(str(a).upper(), a)} / {inferred_types.get(str(a).upper(), 'Unknown')}",
        key="volume_focus_attr",
    )
    focus_df = dist[dist["attribute"] == focus].copy()
    top_values = (
        focus_df.groupby("raw_value")["row_count"].sum().sort_values(ascending=False).head(int(top_n)).index.tolist()
    )
    focus_df["value_grp"] = focus_df["raw_value"].where(focus_df["raw_value"].isin(top_values), other="Other")
    grouped = focus_df.groupby(["date", "value_grp"], as_index=False)["row_count"].sum()
    period_totals = grouped.groupby("date")["row_count"].sum().rename("period_total")
    grouped = grouped.merge(period_totals, on="date")
    grouped["share"] = grouped["row_count"] / grouped["period_total"].clip(lower=1)
    grouped = grouped.sort_values(["value_grp", "date"])
    color_map = {"Other": "#64748b", BLANK_VALUE: "#94a3b8"}

    share_tab, volume_tab = st.tabs(["Composition (100%)", "Volume"])
    with share_tab:
        fig_area = px.area(
            grouped,
            x="date",
            y="share",
            color="value_grp",
            color_discrete_map=color_map,
            labels={"date": "Date", "share": "Share", "value_grp": "Value"},
        )
        fig_area.update_layout(height=420, yaxis_tickformat=".0%")
        st.plotly_chart(fig_area, use_container_width=True)
    with volume_tab:
        fig_vol = px.area(
            grouped,
            x="date",
            y="row_count",
            color="value_grp",
            color_discrete_map=color_map,
            labels={"date": "Date", "row_count": "Rows", "value_grp": "Value"},
        )
        fig_vol.update_layout(height=420, margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig_vol, use_container_width=True)

    fig_lines = px.line(
        grouped,
        x="date",
        y="share",
        color="value_grp",
        color_discrete_map=color_map,
        markers=True,
        labels={"date": "Date", "share": "Share", "value_grp": "Value"},
    )
    fig_lines.update_layout(height=380, yaxis_tickformat=".0%")
    st.plotly_chart(fig_lines, use_container_width=True)

    pivot = grouped.pivot_table(index="value_grp", columns="date", values="share", aggfunc="sum").fillna(0.0)
    if not pivot.empty:
        first_col, last_col = pivot.columns.min(), pivot.columns.max()
        summary = pd.DataFrame(
            {
                "value": pivot.index,
                "start_share": pivot[first_col].to_numpy(),
                "latest_share": pivot[last_col].to_numpy(),
            }
        )
        summary["change_pp"] = (summary["latest_share"] - summary["start_share"]) * 100.0
        summary = summary.sort_values("latest_share", ascending=False)
        st.dataframe(
            summary.style.format(
                {"start_share": "{:.1%}", "latest_share": "{:.1%}", "change_pp": "{:+.1f} pp"}
            ),
            use_container_width=True,
            hide_index=True,
        )
        download_df_button(summary, "Download value mix CSV", f"optic_value_mix_{focus}.csv", "dl_value_mix")


def render_expected_output_drift(
    source_table: str,
    date_col: str,
    job_col: str | None,
    brand_col: str | None,
    start_dt: date,
    end_dt: date,
    selected_jobs: list[str],
    selected_brands: list[str],
    selected_attrs: list[str],
    attr_types: dict[str, str],
    metadata_df: pd.DataFrame,
    trend_grain: str,
) -> None:
    section_title(
        "Compliance",
        "Expected Output Drift",
        "Drift means the model returned values outside finite ODIN options, or changed a conditional text field's "
        "blank-vs-populated behavior. Freeform STRING fields are included only when the prompt supports a "
        "3-word text-presence flag. Compared day by day.",
    )
    if start_dt > end_dt:
        st.error("Start date must be on or before end date.")
        return

    validation_meta = metadata_df[
        metadata_df["validation_enabled"]
        & metadata_df["column"].isin([attr.upper() for attr in selected_attrs])
    ].copy()
    if validation_meta.empty:
        st.info("No selected attributes have finite result options or conditional text-presence checks eligible for expected-output drift.")
        return

    validation_meta["usage_sort"] = pd.to_numeric(validation_meta.get("usage"), errors="coerce").fillna(0)
    validation_meta["result_priority"] = validation_meta["result_type"].map(
        lambda result_type: 0 if str(result_type).upper() in {"STRINGPICKONE", "STRINGMULTI"} else 1
    )
    validation_meta = validation_meta.sort_values(
        ["result_priority", "usage_sort", "attribute"],
        ascending=[True, False, True],
    )

    st.markdown("#### Calculation Scope")
    total_validated_attrs = int(validation_meta["attribute"].nunique())
    max_drift_lookback_days = 365
    default_lookback = min(7, max_drift_lookback_days)

    scope_cols = st.columns([1.35, 1, 1, 1])
    preset = scope_cols[0].selectbox(
        "Preset",
        [
            "Finite categorical + high usage",
            "Finite categorical only",
            "All selected expected-output attrs",
            "Custom",
        ],
        help="Expected-output drift is calculated from finite ODIN result options and prompt-derived text-presence checks.",
    )
    drift_lookback_days = scope_cols[1].slider(
        "Drift lookback days",
        min_value=1,
        max_value=max_drift_lookback_days,
        value=default_lookback,
        help="Uses the sidebar end date and can look back up to one year for this Snowflake query.",
    )
    max_attrs = scope_cols[2].number_input(
        "Max attrs",
        min_value=1,
        max_value=max(total_validated_attrs, 1),
        value=min(35, max(total_validated_attrs, 1)),
        step=5,
        help="Caps the default query size. Increase this for broader validation.",
    )
    allow_full_query = scope_cols[3].checkbox(
        "Run full set",
        value=False,
        help="Allows querying every selected expected-output attribute, even when it is a large Snowflake query.",
    )

    finite_pick_meta = validation_meta[validation_meta["result_type"].isin(["STRINGPICKONE", "STRINGMULTI"])]
    non_finite_pick_meta = validation_meta[~validation_meta["attribute"].isin(finite_pick_meta["attribute"])]
    quick_meta = pd.concat([finite_pick_meta, non_finite_pick_meta], ignore_index=True).drop_duplicates("attribute")

    if preset == "Finite categorical only":
        default_meta = finite_pick_meta.copy()
    elif preset == "All selected expected-output attrs":
        default_meta = validation_meta.copy()
    else:
        default_meta = quick_meta.copy()

    if not allow_full_query:
        default_meta = default_meta.head(int(max_attrs))

    attr_options = validation_meta["attribute"].tolist()
    default_attr_names = [attr for attr in default_meta["attribute"].tolist() if attr in attr_options]
    attr_label_lookup = {
        row["attribute"]: f"{row['attribute']} ({row['result_type']}; {row['jobs_display']})"
        for _, row in validation_meta.iterrows()
    }
    selected_drift_attrs = st.multiselect(
        "Attributes to calculate",
        attr_options,
        default=default_attr_names,
        format_func=lambda attr: attr_label_lookup.get(attr, attr),
        help="These values are compared against expected outputs from ODIN result options, with prompt parsing only as fallback.",
    )
    if not selected_drift_attrs:
        st.warning("Select at least one expected-output attribute to calculate drift.")
        return

    calculation_meta = validation_meta[validation_meta["attribute"].isin(selected_drift_attrs)].copy()
    calculation_meta = calculation_meta.sort_values(
        ["result_priority", "usage_sort", "attribute"],
        ascending=[True, False, True],
    )
    if len(calculation_meta) > int(max_attrs) and not allow_full_query:
        st.warning(
            f"The selected scope has {len(calculation_meta):,} attributes. "
            f"Querying the first {int(max_attrs):,}; enable Run full set to include all selected attributes."
        )
        calculation_meta = calculation_meta.head(int(max_attrs))

    calculation_columns = calculation_meta["column"].tolist()
    query_start_dt = end_dt - timedelta(days=int(drift_lookback_days) - 1)

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Drift-Eligible Attrs", f"{total_validated_attrs:,}")
    finite_pick_count = int(validation_meta["result_type"].isin(["STRINGPICKONE", "STRINGMULTI"]).sum())
    m2.metric("Finite Pick/List", f"{finite_pick_count:,}")
    m3.metric("Query Attrs", f"{len(calculation_meta):,}")
    m4.metric("Query Window", f"{query_start_dt:%Y-%m-%d} to {end_dt:%Y-%m-%d}")

    st.dataframe(
        calculation_meta[
            [
                "attribute",
                "result_type",
                "jobs_display",
                "prompt_groups_display",
                "expected_display",
                "usage",
            ]
        ].head(100).style.format({"usage": "{:,.0f}"}, na_rep=""),
        use_container_width=True,
        hide_index=True,
    )

    st.caption(
        "Expected-output drift recalculates automatically when the date range, jobs, brands, preset, "
        "lookback, or selected attributes change. Cached results are reused for repeated views."
    )

    spec_json = expected_spec_records(calculation_meta, calculation_columns, attr_types)
    if spec_json == "[]":
        st.warning("No expected-output specs were available for the selected attributes.")
        return

    try:
        with st.spinner(
            f"Calculating expected-output drift for {len(calculation_meta):,} attributes from Snowflake..."
        ):
            value_df = fetch_expected_value_distribution(
                source_table,
                date_col,
                query_start_dt,
                end_dt,
                "day",
                job_col,
                selected_jobs,
                brand_col,
                selected_brands,
                spec_json,
            )
    except Exception as exc:
        st.error("Expected-output drift query failed before results were returned.")
        st.exception(exc)
        return

    scored, summary = score_expected_outputs(value_df, calculation_meta)
    if summary.empty:
        st.warning("No rows matched the selected expected-output attributes and filters.")
        return

    summary["date"] = pd.to_datetime(summary["period"], errors="coerce").dt.date
    latest_day = summary["date"].max()
    latest = summary[summary["date"] == latest_day].copy()
    total_rows = int(latest["total_rows"].sum())
    unexpected_rows = int(latest["unexpected_rows"].sum())
    attrs_with_drift = int((latest["unexpected_rows"] > 0).sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Drift Attributes", f"{latest['attribute'].nunique():,}")
    c2.metric("Latest Day Rows", f"{total_rows:,}")
    c3.metric("Unexpected Outputs", f"{unexpected_rows:,}")
    c4.metric("Unexpected Rate", f"{unexpected_rows / max(total_rows, 1):.2%}", f"{attrs_with_drift} attrs")

    watch_cols = [
        "severity",
        "attribute",
        "result_type",
        "jobs",
        "prompt_groups",
        "total_rows",
        "expected_rate",
        "unexpected_rate",
        "unexpected_rows",
        "blank_rate",
        "expected_outputs",
    ]
    latest_display = latest[watch_cols].sort_values(
        ["unexpected_rate", "unexpected_rows", "attribute"],
        ascending=[False, False, True],
    )
    st.markdown("#### Latest Day Compliance")
    severity_badges({str(k): int(v) for k, v in latest["severity"].value_counts().to_dict().items()})
    st.dataframe(
        latest_display.style.format(
            {
                "total_rows": "{:,}",
                "unexpected_rows": "{:,}",
                "expected_rate": "{:.2%}",
                "unexpected_rate": "{:.2%}",
                "blank_rate": "{:.2%}",
            },
            na_rep="",
        ),
        use_container_width=True,
        hide_index=True,
    )
    download_df_button(
        latest_display, "Download latest-day compliance CSV", "optic_expected_output_latest.csv", "dl_eod_latest"
    )

    daily_cols = [
        "date",
        "severity",
        "attribute",
        "result_type",
        "total_rows",
        "expected_rate",
        "unexpected_rate",
        "unexpected_rows",
        "blank_rate",
        "expected_outputs",
    ]
    daily_display = summary[daily_cols].sort_values(
        ["date", "unexpected_rate", "unexpected_rows", "attribute"],
        ascending=[False, False, False, True],
    )
    st.markdown("#### Day-by-Day Compliance")
    st.dataframe(
        daily_display.style.format(
            {
                "total_rows": "{:,}",
                "unexpected_rows": "{:,}",
                "expected_rate": "{:.2%}",
                "unexpected_rate": "{:.2%}",
                "blank_rate": "{:.2%}",
            },
            na_rep="",
        ),
        use_container_width=True,
        hide_index=True,
    )

    top_attrs = latest_display.head(25)["attribute"].tolist()
    bar_df = latest_display.head(30).sort_values("unexpected_rate", ascending=True)
    if not bar_df.empty:
        fig = px.bar(
            bar_df,
            x="unexpected_rate",
            y="attribute",
            color="severity",
            orientation="h",
            labels={"unexpected_rate": "Unexpected Output Rate", "attribute": "Attribute"},
        )
        fig.update_layout(height=max(360, len(bar_df) * 22), xaxis_tickformat=".1%")
        st.plotly_chart(fig, use_container_width=True)

    trend_df = summary[summary["attribute"].isin(top_attrs[:12])].copy()
    if not trend_df.empty:
        st.markdown("#### Daily Unexpected Output Trend")
        fig_trend = px.line(
            trend_df.sort_values(["attribute", "date"]),
            x="date",
            y="unexpected_rate",
            color="attribute",
            markers=True,
            labels={"date": "Date", "unexpected_rate": "Unexpected Output Rate"},
        )
        fig_trend.update_layout(height=420, yaxis_tickformat=".1%")
        st.plotly_chart(fig_trend, use_container_width=True)

    st.markdown("#### Raw Value Review")
    st.caption("Review one attribute's raw model outputs day by day, up to one year back from the sidebar end date.")
    attr_options = latest_display["attribute"].tolist()
    selected_attr = st.selectbox("Attribute", attr_options, key="expected_value_attr")
    selected_attr_meta = calculation_meta[calculation_meta["attribute"] == selected_attr].copy()
    if selected_attr_meta.empty:
        st.warning("No attribute metadata was available for the selected raw value attribute.")
        return

    raw_min_day = end_dt - timedelta(days=max_drift_lookback_days - 1)
    raw_default_start = max(query_start_dt, raw_min_day)
    selected_range = st.date_input(
        "Raw value review date range",
        value=(raw_default_start, end_dt),
        min_value=raw_min_day,
        max_value=end_dt,
        key=f"expected_value_range_{selected_attr}",
        help="Runs a focused one-attribute Snowflake query for the selected dates, up to one year back.",
    )
    if isinstance(selected_range, tuple):
        if len(selected_range) < 2:
            st.info("Select both a start and end date for raw value review.")
            return
        review_start, review_end = selected_range[0], selected_range[1]
    else:
        review_start = selected_range
        review_end = selected_range
    if review_start > review_end:
        st.error("Raw value review start date must be on or before the end date.")
        return

    raw_review_spec_json = expected_spec_records(
        selected_attr_meta,
        selected_attr_meta["column"].tolist(),
        attr_types,
    )
    if raw_review_spec_json == "[]":
        st.warning("No expected-output spec was available for the selected raw value attribute.")
        return

    try:
        with st.spinner(f"Loading raw values for {selected_attr} from Snowflake..."):
            raw_review_df = fetch_expected_value_distribution(
                source_table,
                date_col,
                review_start,
                review_end,
                "day",
                job_col,
                selected_jobs,
                brand_col,
                selected_brands,
                raw_review_spec_json,
            )
    except Exception as exc:
        st.error("Raw value review query failed before results were returned.")
        st.exception(exc)
        return

    review_scored, review_summary = score_expected_outputs(raw_review_df, selected_attr_meta)
    if review_scored.empty:
        st.warning("No raw values matched the selected review date range.")
        return

    review_scored["date"] = pd.to_datetime(review_scored["period"], errors="coerce").dt.date
    review_scored = review_scored.dropna(subset=["date"]).copy()
    if review_scored.empty:
        st.warning("No dated raw values are available for this attribute.")
        return

    if not review_summary.empty:
        review_summary["date"] = pd.to_datetime(review_summary["period"], errors="coerce").dt.date
        review_summary = review_summary.dropna(subset=["date"]).sort_values("date")
        if not review_summary.empty:
            st.markdown("#### Selected Attribute Daily Drift")
            fig_raw_history = px.line(
                review_summary,
                x="date",
                y="unexpected_rate",
                markers=True,
                labels={"date": "Date", "unexpected_rate": "Unexpected Output Rate"},
                hover_data={
                    "total_rows": ":,",
                    "unexpected_rows": ":,",
                    "blank_rate": ":.2%",
                    "expected_rate": ":.2%",
                },
            )
            fig_raw_history.update_layout(height=360, yaxis_tickformat=".1%")
            st.plotly_chart(fig_raw_history, use_container_width=True)
            with st.expander("Daily drift table", expanded=False):
                st.dataframe(
                    review_summary[
                        [
                            "date",
                            "severity",
                            "total_rows",
                            "expected_rate",
                            "unexpected_rate",
                            "unexpected_rows",
                            "blank_rate",
                        ]
                    ].style.format(
                        {
                            "total_rows": "{:,}",
                            "unexpected_rows": "{:,}",
                            "expected_rate": "{:.2%}",
                            "unexpected_rate": "{:.2%}",
                            "blank_rate": "{:.2%}",
                        },
                        na_rep="",
                    ),
                    use_container_width=True,
                    hide_index=True,
                )

    value_detail = (
        review_scored.groupby(
            ["status", "raw_value", "canonical_value", "is_expected"],
            dropna=False,
        )
        .agg(
            row_count=("row_count", "sum"),
            first_date=("date", "min"),
            last_date=("date", "max"),
            days_seen=("date", "nunique"),
        )
        .reset_index()
    )
    value_detail["share"] = value_detail["row_count"] / max(value_detail["row_count"].sum(), 1)
    value_detail = value_detail.sort_values(["is_expected", "row_count"], ascending=[True, False])
    st.dataframe(
        value_detail[
            [
                "status",
                "raw_value",
                "canonical_value",
                "row_count",
                "share",
                "first_date",
                "last_date",
                "days_seen",
            ]
        ].style.format({"row_count": "{:,}", "share": "{:.2%}", "days_seen": "{:,}"}),
        use_container_width=True,
        hide_index=True,
    )


def render_attribute_metadata(metadata_df: pd.DataFrame, table_columns: list[dict[str, str]]) -> None:
    section_title(
        "Catalog",
        "Attribute Metadata",
        "Attributes come from the ODIN source-of-truth table. Expected-output drift is limited to fields with finite result options; "
        "freeform STRING and LIST attributes are retained for metadata and quality review only.",
    )
    if metadata_df.empty:
        render_empty("No attribute metadata was loaded", "Check the ODIN metadata table or fallback file paths in the sidebar.")
        return

    column_names = {col["name"] for col in table_columns}
    display = metadata_df.copy()
    display["in_snowflake"] = display["column"].isin(column_names)
    display["usage"] = pd.to_numeric(display["usage"], errors="coerce")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("ODIN Attributes", f"{len(display):,}")
    c2.metric("In Snowflake", f"{int(display['in_snowflake'].sum()):,}")
    c3.metric("Drift-Eligible", f"{int(display['validation_enabled'].sum()):,}")
    show_count = int(display.get("show_in_report", pd.Series(dtype=object)).fillna(False).astype(bool).sum())
    c4.metric("Show In Report", f"{show_count:,}")

    filter_cols = st.columns(4)
    job_filter = filter_cols[0].multiselect("Job", ["ACQUISITION", "RETENTION", "SERVICE"], default=[])
    result_filter = filter_cols[1].multiselect("Result Type", sorted(display["result_type"].dropna().unique()), default=[])
    validation_filter = filter_cols[2].selectbox("Drift Eligibility", ["All", "Drift eligible only", "Freeform/non-drift only"])
    search = filter_cols[3].text_input("Search", "")

    filtered = display.copy()
    if job_filter:
        job_set = set(job_filter)
        filtered = filtered[filtered["jobs"].map(lambda jobs: bool(job_set & set(jobs)))]
    if result_filter:
        filtered = filtered[filtered["result_type"].isin(result_filter)]
    if validation_filter == "Drift eligible only":
        filtered = filtered[filtered["validation_enabled"]]
    elif validation_filter == "Freeform/non-drift only":
        filtered = filtered[~filtered["validation_enabled"]]
    if search.strip():
        needle = search.strip().lower()
        filtered = filtered[
            filtered["attribute"].str.contains(needle, case=False, na=False)
            | filtered["prompt"].str.contains(needle, case=False, na=False)
            | filtered.get("description", pd.Series("", index=filtered.index)).str.contains(needle, case=False, na=False)
        ]

    display_columns = [
        "attribute_id",
        "attribute",
        "description",
        "column",
        "in_snowflake",
        "jobs_display",
        "prompt_groups_display",
        "result_type",
        "source_result_type",
        "result_type_source",
        "drift_eligible",
        "validation_method",
        "value_mode",
        "expected_display",
        "result_value_options",
        "prompt_source",
        "usage",
        "last_usage_date",
        "created_date",
    ]
    display_columns = [column for column in display_columns if column in filtered.columns]
    st.dataframe(
        filtered[display_columns].style.format({"usage": "{:,.0f}"}, na_rep=""),
        use_container_width=True,
        hide_index=True,
    )

    if filtered.empty:
        render_empty("No attributes match the current filters", "Loosen the job, result-type, or search filters above.")
        return

    download_df_button(
        filtered.drop(columns=[c for c in ("jobs", "prompt_groups", "json_prompts") if c in filtered.columns]),
        "Download metadata CSV",
        "optic_attribute_metadata.csv",
        "dl_metadata",
    )

    attr = st.selectbox("Prompt preview", filtered["attribute"].tolist())
    if attr:
        row = filtered[filtered["attribute"] == attr].iloc[0]
        st.markdown(f"**Expected outputs:** `{row['expected_display'] or 'Freeform / no finite expected set'}`")
        description = str(row.get("description") or "").strip()
        if description:
            st.caption(description)
        st.text_area("Prompt", value=row["prompt"], height=220, disabled=True)


def render_experiment_tab(jobs: dict[str, Path]) -> None:
    section_title(
        "Benchmarks",
        "Model Evaluation",
        "Reads evaluation output files when present: results_batch, flat-linkage, and semantic-cap JSON.",
    )
    if not jobs:
        render_empty(
            "No output folders found",
            "Set OPTIC_DASHBOARD_OUTPUT_ROOT or run the evaluation pipeline to populate this view.",
        )
        return

    job_label = st.selectbox("Output job", list(jobs.keys()), key="exp_job")
    output_dir = jobs[job_label]
    exp_df = load_experiment_data(output_dir)
    attr_df = load_per_attribute_data(output_dir)

    if exp_df.empty:
        st.warning(f"No experiment result files found in {output_dir}.")
        return

    c1, c2, c3 = st.columns(3)
    best_acc = exp_df.dropna(subset=["overall_accuracy"])
    best_cost = exp_df.dropna(subset=["cost_per_transcript"])
    best_speed = exp_df.dropna(subset=["e2e_latency_avg"])
    if not best_acc.empty:
        row = best_acc.loc[best_acc["overall_accuracy"].idxmax()]
        c1.metric("Highest Accuracy", f"{row['overall_accuracy']:.1%}", f"{row['model']} / {row['approach']}")
    if not best_cost.empty:
        row = best_cost.loc[best_cost["cost_per_transcript"].idxmin()]
        c2.metric("Lowest Cost", f"${row['cost_per_transcript']:.4f}", f"{row['model']} / {row['approach']}")
    if not best_speed.empty:
        row = best_speed.loc[best_speed["e2e_latency_avg"].idxmin()]
        c3.metric("Fastest", f"{row['e2e_latency_avg']:.1f}s", f"{row['model']} / {row['approach']}")

    metric_options = [m for m in METRIC_COLS if m in exp_df.columns and exp_df[m].notna().any()]
    metric = st.selectbox(
        "Metric",
        metric_options,
        format_func=lambda x: METRIC_LABELS.get(x, x),
        key="exp_metric",
    )
    selected_models = st.multiselect(
        "Models",
        sorted(exp_df["model"].dropna().unique()),
        default=sorted(exp_df["model"].dropna().unique()),
        key="exp_models",
    )
    plot_df = exp_df[exp_df["model"].isin(selected_models)].copy() if selected_models else exp_df.copy()
    plot_df["batch_size_str"] = plot_df["batch_size"].astype(str)
    plot_df["series"] = plot_df["approach"] + " / " + plot_df["model"]

    fig = px.line(
        plot_df.sort_values(["batch_size", "series"]),
        x="batch_size_str",
        y=metric,
        color="series",
        markers=True,
        labels={"batch_size_str": "Batch Size", metric: METRIC_LABELS.get(metric, metric)},
    )
    if "cost" not in metric and "latency" not in metric:
        fig.update_layout(yaxis_tickformat=".0%")
    fig.update_layout(height=430)
    st.plotly_chart(fig, use_container_width=True)

    display_cols = ["approach", "model", "batch_size"] + [m for m in METRIC_COLS if m in exp_df.columns]
    fmt = {col: metric_format(col) for col in display_cols if col in METRIC_COLS}
    st.dataframe(
        exp_df[display_cols].sort_values(["approach", "model", "batch_size"]).style.format(fmt, na_rep=""),
        use_container_width=True,
        hide_index=True,
    )

    if not attr_df.empty:
        st.markdown("#### Per-Attribute Accuracy")
        model = st.selectbox("Model", sorted(attr_df["model"].dropna().unique()), key="exp_attr_model")
        batch_values = sorted(attr_df[attr_df["model"] == model]["batch_size"].dropna().unique())
        batch = st.selectbox("Batch size", batch_values, key="exp_attr_batch") if batch_values else None
        filtered = attr_df[(attr_df["model"] == model) & (attr_df["batch_size"] == batch)].dropna(subset=["accuracy"])
        if not filtered.empty:
            filtered = filtered.sort_values("accuracy", ascending=True)
            fig_attr = px.bar(
                filtered,
                x="accuracy",
                y="attribute",
                color="type",
                orientation="h",
                labels={"accuracy": "Accuracy", "attribute": "Attribute"},
            )
            fig_attr.update_layout(height=max(420, len(filtered) * 18), xaxis_tickformat=".0%")
            st.plotly_chart(fig_attr, use_container_width=True)


def render_production_runs_tab(jobs: dict[str, Path]) -> None:
    section_title(
        "History",
        "Production Run Quality",
        "Reads drift_history.json snapshots written by the production workflow.",
    )
    if not jobs:
        render_empty("No output folders found", "Point OPTIC_DASHBOARD_OUTPUT_ROOT at a folder with drift snapshots.")
        return

    job_label = st.selectbox("Output job", list(jobs.keys()), key="drift_job")
    output_dir = jobs[job_label]
    snaps = load_drift_history(output_dir)
    if not snaps:
        st.warning(f"No drift_history.json found in {output_dir}.")
        return

    df = build_drift_df(snaps)
    if df.empty:
        st.warning("Drift history did not contain usable rows.")
        return

    c1, c2, c3 = st.columns(3)
    c1.metric("Snapshots", f"{len(df):,}")
    c2.metric("Models", f"{df['model'].nunique():,}")
    c3.metric("Latest Snapshot", str(df["snapshot_date"].max()))

    selected_models = st.multiselect(
        "Models",
        sorted(df["model"].dropna().unique()),
        default=sorted(df["model"].dropna().unique()),
        key="drift_models",
    )
    metric_options = [m for m in METRIC_COLS if m in df.columns and df[m].notna().any()]
    metric = st.selectbox(
        "Metric",
        metric_options,
        format_func=lambda x: METRIC_LABELS.get(x, x),
        key="drift_metric",
    )
    fdf = df[df["model"].isin(selected_models)].copy() if selected_models else df.copy()
    fig = px.line(
        fdf.sort_values("timestamp"),
        x="snapshot_date",
        y=metric,
        color="model",
        markers=True,
        color_discrete_map=MODEL_COLORS,
        labels={"snapshot_date": "Snapshot", metric: METRIC_LABELS.get(metric, metric)},
    )
    if "cost" not in metric and "latency" not in metric:
        fig.update_layout(yaxis_tickformat=".0%")
    fig.update_layout(height=430)
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("#### Snapshot Table")
    display_cols = [
        "snapshot_date",
        "snapshot",
        "model",
        "transcript_count",
        "attribute_count",
    ] + [m for m in METRIC_COLS if m in df.columns]
    fmt = {col: metric_format(col) for col in display_cols if col in METRIC_COLS}
    st.dataframe(
        df[display_cols].sort_values(["snapshot_date", "model"]).style.format(fmt, na_rep=""),
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("#### Per-Attribute Drift")
    model = st.selectbox("Model", sorted(df["model"].dropna().unique()), key="drift_attr_model")
    model_snaps = [snap for snap in snaps if snap.get("model") == model]
    all_attrs = sorted({attr for snap in model_snaps for attr in snap.get("per_attribute", {}).keys()})
    if not all_attrs:
        st.info("No per-attribute metrics were found in the drift snapshots.")
        return
    attr_metric = st.selectbox(
        "Attribute metric",
        ["accuracy", "blank_rate", "positive_rate", "avg_length"],
        key="drift_attr_metric",
    )
    selected_attrs = st.multiselect("Attributes", all_attrs, default=all_attrs[:5], key="drift_attrs")
    rows: list[dict] = []
    for snap in model_snaps:
        snap_date = snap.get("snapshot_date", str(snap.get("timestamp", ""))[:10])
        for attr in selected_attrs:
            value = snap.get("per_attribute", {}).get(attr, {}).get(attr_metric)
            if value is not None:
                rows.append({"snapshot_date": snap_date, "attribute": attr, attr_metric: value})
    attr_metric_df = pd.DataFrame(rows)
    if not attr_metric_df.empty:
        fig_attr = px.line(
            attr_metric_df,
            x="snapshot_date",
            y=attr_metric,
            color="attribute",
            markers=True,
        )
        if attr_metric != "avg_length":
            fig_attr.update_layout(yaxis_tickformat=".0%")
        fig_attr.update_layout(height=430)
        st.plotly_chart(fig_attr, use_container_width=True)


def render_review_tab() -> None:
    section_title(
        "Ground Truth",
        "Review Queue",
        "Loads step1_review_sheet_with_evidence.json and persists reviewed values beside that file.",
    )
    available = find_review_files()
    if not available:
        render_empty(
            "No review sheet files found",
            "Run post_step1_add_evidence.py first, or set OPTIC_DASHBOARD_OUTPUT_ROOT.",
        )
        return

    selected = st.selectbox("Review file", list(available.keys()), key="review_file")
    review_path = available[selected]
    output_dir = review_path.parent
    rows = load_json_file(str(review_path))
    df = pd.DataFrame(rows)
    if df.empty:
        st.warning("Review file is empty.")
        return

    state_key = f"reviewed_values_{selected}"
    done_key = f"done_keys_{selected}"
    if state_key not in st.session_state:
        st.session_state[state_key] = load_saved_reviews(output_dir)
    if done_key not in st.session_state:
        st.session_state[done_key] = load_done_keys(output_dir)

    with st.sidebar:
        st.markdown("---")
        st.subheader("Review Filters")
        type_values = sorted(df.get("disagreement_type", pd.Series(dtype=str)).dropna().unique())
        selected_types = st.multiselect("Disagreement Type", type_values, default=type_values)
        attr_types = sorted(df.get("attribute_type", pd.Series(dtype=str)).dropna().unique())
        default_attr_types = ["BOOL"] if "BOOL" in attr_types else attr_types
        selected_attr_types = st.multiselect("Attribute Type", attr_types, default=default_attr_types)
        verdicts = sorted(df.get("judge_verdict", pd.Series(dtype=str)).dropna().unique())
        default_verdicts = ["different"] if "different" in verdicts else verdicts
        selected_verdicts = st.multiselect("Judge Verdict", verdicts, default=default_verdicts)
        status = st.radio("Review Status", ["All", "Needs Review", "Already Reviewed"], horizontal=False)

    filtered = df.copy()
    if selected_types and "disagreement_type" in filtered:
        filtered = filtered[filtered["disagreement_type"].isin(selected_types)]
    if selected_attr_types and "attribute_type" in filtered:
        filtered = filtered[filtered["attribute_type"].isin(selected_attr_types)]
    if selected_verdicts and "judge_verdict" in filtered:
        filtered = filtered[filtered["judge_verdict"].isin(selected_verdicts)]

    done = st.session_state[done_key]
    row_keys = filtered.apply(lambda r: f"{r.get('source_id', '')}_{r.get('attribute', '')}", axis=1)
    if status == "Needs Review":
        filtered = filtered[~row_keys.isin(done)]
    elif status == "Already Reviewed":
        filtered = filtered[row_keys.isin(done)]

    total = len(df)
    done_count = len(done)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total", f"{total:,}")
    c2.metric("Done", f"{done_count:,}")
    c3.metric("Remaining", f"{max(total - done_count, 0):,}")
    c4.metric("Progress", f"{done_count / max(total, 1):.0%}")

    st.markdown("#### Review Rows")
    max_rows = st.slider("Rows to display", 10, 250, 50, step=10)
    for _, row in filtered.head(max_rows).iterrows():
        source_id = str(row.get("source_id", ""))
        attr = str(row.get("attribute", ""))
        key = f"{source_id}_{attr}"
        is_done = key in done
        title = f"{'[Done]' if is_done else '[Open]'} {attr} - {source_id}"
        with st.expander(title, expanded=not is_done):
            info, evidence = st.columns(2)
            with info:
                st.write("Production Value")
                st.code(str(row.get("production_value", ""))[:1000], language=None)
                st.write("Frontier Value")
                st.code(str(row.get("frontier_value", ""))[:1000], language=None)
            with evidence:
                quote = str(row.get("evidence_quote", "") or "")
                context = str(row.get("transcript_context", "") or "")
                reasoning = str(row.get("evidence_reasoning", "") or "")
                if quote:
                    st.write("Evidence Quote")
                    st.info(quote)
                if reasoning:
                    st.write("Reasoning")
                    st.caption(reasoning)
                if context:
                    st.write("Transcript Context")
                    st.code(context[:1500], language=None)

            existing = st.session_state[state_key].get(key, "")
            correct = st.text_input(
                "Correct Value",
                value=existing,
                key=f"review_input_{selected}_{key}",
                placeholder="Enter the reviewed value, or leave blank if no correction is needed",
            )
            btn1, btn2 = st.columns([1, 5])
            with btn1:
                if is_done:
                    if st.button("Undo", key=f"review_undo_{selected}_{key}"):
                        done.discard(key)
                        save_done_keys(output_dir, done)
                        st.rerun()
                else:
                    if st.button("Done", key=f"review_done_{selected}_{key}"):
                        if correct:
                            st.session_state[state_key][key] = correct
                            autosave_reviews(output_dir, st.session_state[state_key])
                        done.add(key)
                        save_done_keys(output_dir, done)
                        st.rerun()
            if correct and correct != existing:
                st.session_state[state_key][key] = correct
                autosave_reviews(output_dir, st.session_state[state_key])

    st.markdown("#### Export")
    reviewed = st.session_state[state_key]
    review_rows = [
        {
            "source_id": key.partition("_")[0],
            "attribute": key.partition("_")[2],
            "correct_value": value,
        }
        for key, value in reviewed.items()
    ]
    if review_rows:
        review_df = pd.DataFrame(review_rows)
        st.dataframe(review_df, use_container_width=True, hide_index=True)
        st.download_button(
            "Download reviewed values JSON",
            data=json.dumps(review_rows, indent=2, ensure_ascii=False),
            file_name=f"reviewed_values_{selected}_{datetime.now().strftime('%Y%m%d')}.json",
            mime="application/json",
        )


def render_data_explorer(
    source_table: str,
    columns: list[dict[str, str]],
    date_col: str,
    job_col: str | None,
    brand_col: str | None,
    start_dt: date,
    end_dt: date,
    selected_jobs: list[str],
    selected_brands: list[str],
    selected_attrs: list[str],
) -> None:
    section_title(
        "Samples",
        "Data Explorer",
        "Samples production rows from Snowflake for quick spot checks.",
    )
    base_cols = [c for c in [date_col, job_col, brand_col, "CONNECTION_ID", "CONNID", "AGENTREFERENCEID"] if c]
    available_names = {col["name"] for col in columns}
    base_cols = [col for col in dict.fromkeys(base_cols) if col in available_names]
    sample_cols = list(dict.fromkeys(base_cols + selected_attrs[:8]))
    user_cols = st.multiselect(
        "Columns",
        [col["name"] for col in columns],
        default=sample_cols,
        key="sample_cols",
    )
    limit = st.slider("Sample rows", 25, 500, 100, step=25)
    if not user_cols:
        st.info("Select at least one column.")
        return
    sample = fetch_sample_rows(
        source_table,
        date_col,
        start_dt,
        end_dt,
        job_col,
        selected_jobs,
        brand_col,
        selected_brands,
        user_cols,
        limit,
    )
    if sample.empty:
        render_empty("No rows matched", "Adjust the date range or filters in the sidebar.")
        return
    st.caption(f"Showing {len(sample):,} sampled rows.")
    st.dataframe(sample, use_container_width=True, hide_index=True)
    download_df_button(sample, "Download sample CSV", "optic_sample_rows.csv", "dl_sample")


def main() -> None:
    require_dashboard_auth()
    css()

    with st.sidebar:
        st.markdown(
            """
            <div style="
              display:flex;align-items:center;gap:12px;
              padding:14px 8px 18px 8px;margin-bottom:6px;
              border-bottom:1px solid rgba(148,163,184,0.12);">
              <div style="
                width:38px;height:38px;border-radius:10px;
                display:grid;place-items:center;color:white;font-weight:800;
                background:linear-gradient(135deg,#6366f1 0%,#06b6d4 100%);
                box-shadow:0 8px 20px rgba(99,102,241,0.35);">OP</div>
              <div>
                <div style="font-weight:700;font-size:1rem;color:#f8fafc;letter-spacing:-0.01em;">OPTIC Console</div>
                <div style="font-size:0.75rem;color:#94a3b8;">Production Monitoring</div>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.header("Controls")
        source_table = st.text_input("Live source table", DEFAULT_OPTIC_TABLE)
        raw_table = st.text_input("Raw OPTIC table", DEFAULT_RAW_TABLE)
        attribute_table = st.text_input("Attribute metadata table", DEFAULT_ATTRIBUTE_TABLE)
        with st.expander("Metadata fallback files", expanded=False):
            acquisition_prompt_path = st.text_input(
                "Acquisition JSON",
                str(DEFAULT_PROMPT_PATHS["ACQUISITION"]),
            )
            retention_prompt_path = st.text_input(
                "Retention JSON",
                str(DEFAULT_PROMPT_PATHS["RETENTION"]),
            )
            service_prompt_path = st.text_input(
                "Service JSON",
                str(DEFAULT_PROMPT_PATHS["SERVICE"]),
            )
            attributes_xlsx_path = st.text_input(
                "Attributes Excel",
                str(DEFAULT_ATTRIBUTES_XLSX),
            )
        st.markdown("**Date range**")
        preset_days = {"7 days": 7, "14 days": 14, "30 days": 30, "90 days": 90}
        range_options = list(preset_days.keys()) + ["Custom"]
        if hasattr(st, "segmented_control"):
            preset = (
                st.segmented_control(
                    "Date range", range_options, default="30 days", label_visibility="collapsed"
                )
                or "30 days"
            )
        else:
            preset = st.radio(
                "Date range", range_options, index=2, horizontal=True, label_visibility="collapsed"
            )
        end_dt = date.today()
        if preset == "Custom":
            date_cols = st.columns(2)
            start_dt = date_cols[0].date_input("Start date", value=date.today() - timedelta(days=30))
            end_dt = date_cols[1].date_input("End date", value=date.today())
        else:
            start_dt = end_dt - timedelta(days=preset_days[preset])
            st.caption(f"{start_dt:%b %d} → {end_dt:%b %d, %Y}  ·  {preset_days[preset]} days")
        trend_grain = st.selectbox("Trend grain", ["day", "week", "month"], index=0)
        if st.button("Refresh cached data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        st.caption("Cached queries auto-expire every 5 minutes.")

    live_ok = False
    columns_df = pd.DataFrame()
    columns: list[dict[str, str]] = []
    live_error: str | None = None
    try:
        columns_df = get_table_columns(source_table)
        columns = column_records(columns_df)
        live_ok = bool(columns)
    except Exception as exc:
        live_error = str(exc)

    render_header(source_table, live_ok, start_dt, end_dt)
    if live_error:
        st.warning(f"Live Snowflake data is unavailable: {live_error}")

    output_jobs = discover_output_jobs()
    metadata_df = load_attribute_metadata(
        attribute_table,
        acquisition_prompt_path,
        retention_prompt_path,
        service_prompt_path,
        attributes_xlsx_path,
    )
    metadata_warning = metadata_df.attrs.get("metadata_warning")
    if metadata_warning:
        st.warning(f"{metadata_warning} Using JSON/Excel fallback metadata.")
    if metadata_df.empty:
        st.warning("No attribute metadata was loaded. Check the ODIN metadata table or fallback file paths in the sidebar.")

    if live_ok:
        date_col = find_first_column(columns, ["INTERACTIONDATE", "RUNDATE", "CREATEDDATE"])
        job_col = find_first_column(columns, ["JOBNAME", "JOB_NAME"])
        brand_col = find_first_column(columns, ["BRAND", "BRANDNAME"])
        connection_col = find_first_column(columns, ["CONNECTION_ID", "CONNID", "SOURCEID"])
        duration_col = find_first_column(columns, ["DURATION", "CONTENTLENGTH"])

        if not date_col:
            st.error("The live source table does not have a supported date column.")
            live_ok = False
    if live_ok:
        with st.sidebar:
            st.markdown("---")
            st.subheader("Live Filters")
            options = fetch_filter_options(source_table, date_col, start_dt, end_dt, job_col, brand_col)
            job_values = options["jobs"]["VALUE"].tolist() if not options["jobs"].empty else []
            brand_values = options["brands"]["VALUE"].tolist() if not options["brands"].empty else []
            default_jobs = [j for j in ["SERVICE", "RETENTION", "ACQUISITION"] if j in job_values]
            selected_jobs = st.multiselect("Jobs", job_values, default=default_jobs)
            selected_brands = st.multiselect("Brands", brand_values, default=[])
            column_names = {col["name"] for col in columns}
            scoped_metadata = metadata_df[metadata_df["column"].isin(column_names)].copy()
            if selected_jobs:
                selected_job_set = {job.upper() for job in selected_jobs}
                scoped_metadata = scoped_metadata[
                    scoped_metadata["jobs"].map(lambda jobs: bool(selected_job_set & set(jobs)))
                ]

            prompt_groups = sorted({group for groups in scoped_metadata["prompt_groups"] for group in groups})
            selected_prompt_groups = st.multiselect("Prompt Groups", prompt_groups, default=[])
            if selected_prompt_groups:
                prompt_group_set = set(selected_prompt_groups)
                scoped_metadata = scoped_metadata[
                    scoped_metadata["prompt_groups"].map(lambda groups: bool(prompt_group_set & set(groups)))
                ]

            result_types = sorted(scoped_metadata["result_type"].dropna().unique())
            selected_result_types = st.multiselect("Result Types", result_types, default=[])
            if selected_result_types:
                scoped_metadata = scoped_metadata[scoped_metadata["result_type"].isin(selected_result_types)]

            validation_mode = st.selectbox(
                "Expected Output Filter",
                ["Drift-eligible attrs", "All catalog attrs", "Freeform/non-drift attrs"],
                index=0,
            )
            if validation_mode == "Drift-eligible attrs":
                scoped_metadata = scoped_metadata[scoped_metadata["validation_enabled"]]
            elif validation_mode == "Freeform/non-drift attrs":
                scoped_metadata = scoped_metadata[~scoped_metadata["validation_enabled"]]

            attr_search = st.text_input("Attribute Search", "")
            if attr_search.strip():
                needle = attr_search.strip().lower()
                scoped_metadata = scoped_metadata[
                    scoped_metadata["attribute"].str.contains(needle, case=False, na=False)
                    | scoped_metadata["prompt"].str.contains(needle, case=False, na=False)
                ]

            attrs = scoped_metadata["column"].tolist()
            if not attrs:
                attrs = candidate_attribute_columns(columns)
            default_attrs = scoped_metadata[scoped_metadata["validation_enabled"]]["column"].tolist()
            if not default_attrs:
                default_attrs = default_attribute_selection(attrs)
            attr_label_lookup = {
                row["column"]: f"{row['attribute']} ({row['result_type']})"
                for _, row in scoped_metadata.iterrows()
            }
            selected_attrs = st.multiselect(
                "Attributes",
                attrs,
                default=default_attrs,
                format_func=lambda col: attr_label_lookup.get(col, col),
                help="Attributes from the ODIN metadata source of truth. Drift-eligible attrs validate against finite result options only.",
            )
        attr_types = column_type_lookup(columns)
    else:
        date_col = ""
        job_col = None
        brand_col = None
        connection_col = None
        duration_col = None
        selected_jobs = []
        selected_brands = []
        selected_attrs = []
        attr_types = {}

    (
        live_tab,
        drift_tab,
        value_tab,
        expected_tab,
        metadata_tab,
        exp_tab,
        prod_tab,
        review_tab,
        sample_tab,
    ) = st.tabs(
        [
            "Live OPTIC",
            "Attribute Drift",
            "Value Spikes",
            "Expected Output Drift",
            "Attribute Metadata",
            "Model Evaluation",
            "Production Runs",
            "Review Queue",
            "Data Explorer",
        ]
    )

    with live_tab:
        if live_ok:
            render_live_overview(
                source_table,
                columns,
                date_col,
                job_col,
                brand_col,
                connection_col,
                duration_col,
                start_dt,
                end_dt,
                selected_jobs,
                selected_brands,
                selected_attrs,
                attr_types,
                trend_grain,
            )
        else:
            st.info("Configure Snowflake access to enable live OPTIC monitoring.")

    with drift_tab:
        if live_ok:
            render_attribute_drift(
                source_table,
                date_col,
                job_col,
                brand_col,
                start_dt,
                end_dt,
                selected_jobs,
                selected_brands,
                selected_attrs,
                attr_types,
                trend_grain,
            )
        else:
            st.info("Configure Snowflake access to enable attribute drift monitoring.")

    with value_tab:
        if live_ok:
            render_volume_drift(
                source_table,
                columns,
                date_col,
                job_col,
                brand_col,
                start_dt,
                end_dt,
                selected_jobs,
                selected_brands,
                selected_attrs,
                attr_types,
                metadata_df,
                trend_grain,
            )
        else:
            st.info("Configure Snowflake access to enable value distribution monitoring.")

    with expected_tab:
        if live_ok:
            render_expected_output_drift(
                source_table,
                date_col,
                job_col,
                brand_col,
                start_dt,
                end_dt,
                selected_jobs,
                selected_brands,
                selected_attrs,
                attr_types,
                metadata_df,
                trend_grain,
            )
        else:
            st.info("Configure Snowflake access to enable expected-output drift monitoring.")

    with metadata_tab:
        render_attribute_metadata(metadata_df, columns if live_ok else [])

    with exp_tab:
        render_experiment_tab(output_jobs)

    with prod_tab:
        render_production_runs_tab(output_jobs)

    with review_tab:
        render_review_tab()

    with sample_tab:
        if live_ok:
            render_data_explorer(
                source_table,
                columns,
                date_col,
                job_col,
                brand_col,
                start_dt,
                end_dt,
                selected_jobs,
                selected_brands,
                selected_attrs,
            )
        else:
            st.info("Configure Snowflake access to sample production rows.")

    st.markdown(
        """
        <hr class="optic-divider" />
        <div style="
          display:flex;align-items:center;justify-content:space-between;
          gap:12px;flex-wrap:wrap;
          font-size:0.78rem;color:#64748b;padding:8px 4px 0 4px;">
          <div>OPTIC Production Dashboard &middot; built for Vistra Analytics</div>
          <div style="display:flex;gap:14px;align-items:center;">
            <span><span style="display:inline-block;width:6px;height:6px;border-radius:50%;background:#10b981;margin-right:6px;vertical-align:middle;"></span>Live</span>
            <span>Powered by Snowflake &middot; Plotly &middot; Streamlit</span>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
