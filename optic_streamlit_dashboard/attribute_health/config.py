from __future__ import annotations

import os
from dataclasses import dataclass, fields
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

CENTRAL = ZoneInfo("America/Chicago")
READINESS_INTEGER_BOUNDS = {
    "source_lag_days": (1, 89),
    "readiness_lookback_days": (1, 30),
    "readiness_baseline_weeks": (1, 12),
    "readiness_min_baseline_days": (1, 12),
    "readiness_min_baseline_volume": (1, 10**12),
    "readiness_attribute_baseline_days": (1, 89),
}
READINESS_FRACTIONS = (
    "readiness_volume_fraction", "readiness_id_fraction",
    "readiness_required_present_fraction", "readiness_attribute_ready_fraction",
)


def validate_readiness_settings(values):
    for name, (low, high) in READINESS_INTEGER_BOUNDS.items():
        if type(values[name]) is not int or not low <= values[name] <= high:
            raise ValueError("Invalid source readiness setting: " + name)
    if values["readiness_min_baseline_days"] > min(
            values["readiness_baseline_weeks"], values["readiness_attribute_baseline_days"]):
        raise ValueError("Readiness support exceeds its bounded baseline lookback.")
    for name in READINESS_FRACTIONS:
        if type(values[name]) not in {int, float} or not 0 < values[name] <= 1:
            raise ValueError("Invalid source readiness fraction: " + name)
    return values


@dataclass(frozen=True)
class Window:
    start: date
    end: date
    watermark: str | None = None

    def __post_init__(self):
        days = (self.end - self.start).days + 1
        if not 1 <= days <= 90:
            raise ValueError("Attribute Health requires 1 to 90 inclusive complete Central dates.")
        if self.end >= datetime.now(CENTRAL).date():
            raise ValueError("Current/future Central days are not complete.")

    @classmethod
    def ending(cls, end=None, now=None, ready_through=None, watermark=None):
        now = now or datetime.now(CENTRAL)
        if now.tzinfo is None:
            raise ValueError("An aware clock is required.")
        last = now.astimezone(CENTRAL).date() - timedelta(days=1)
        end = date.fromisoformat(str(end)) if end else last
        if end > last:
            raise ValueError("The requested end includes an incomplete Central day.")
        if ready_through:
            end = min(end, date.fromisoformat(str(ready_through)))
        return cls(end - timedelta(days=89), end, watermark)

    @classmethod
    def from_dict(cls, value):
        return cls(date.fromisoformat(value["start"]), date.fromisoformat(value["end"]),
                   value.get("watermark"))

    def contains(self, value):
        if isinstance(value, datetime):
            value = value.astimezone(CENTRAL).date() if value.tzinfo else value.date()
        elif not isinstance(value, date):
            value = date.fromisoformat(str(value)[:10])
        return self.start <= value <= self.end

    def require(self, value):
        if not self.contains(value):
            raise ValueError("Evidence date is outside its immutable analytical window.")

    def bounds(self, utc=False):
        start = datetime.combine(self.start, time.min, CENTRAL)
        stop = datetime.combine(self.end + timedelta(days=1), time.min, CENTRAL)
        return (start.astimezone(timezone.utc), stop.astimezone(timezone.utc)) if utc else (start, stop)

    def days(self):
        return [self.start + timedelta(days=i) for i in range((self.end - self.start).days + 1)]

    def as_dict(self):
        return {"start": self.start.isoformat(), "end": self.end.isoformat(),
                "days": len(self.days()), "timezone": "America/Chicago", "watermark": self.watermark}


@dataclass(frozen=True)
class Settings:
    state_dir: Path = Path.home() / ".optic" / "attribute-health"
    jobs: tuple[str, ...] = ("ACQUISITION", "RETENTION", "SERVICE")
    all_active_jobs: bool = False
    ai_enabled: bool = False
    image_verified: bool = False
    reviewer_model: str = "gpt-6.1-sol"
    judge_model: str = "gpt-6.1-sol"
    max_reviews_run: int = 10
    max_pilots_run: int = 5
    transcript_attempts_day: int = 500
    review_attempts_day: int = 20
    workers: int = 4
    expansion_limit: int = 120
    reserve_fraction: float = 0.10
    input_tokens_day: int = 2_000_000
    output_tokens_day: int = 500_000
    input_tokens_request: int = 24_000
    output_tokens_request: int = 2_000
    max_attempts: int = 3
    lease_seconds: int = 600
    simulations_min: int = 1999
    simulations_max: int = 19999
    seed: int = 61301
    baseline_n: int = 1000
    recent_n: int = 300
    baseline_days: int = 21
    recent_days: int = 10
    common_relative: float = .20
    common_pp: float = 2.0
    common_absolute_pp: float = 5.0
    rare_relative: float = .50
    rare_min_pp: float = .02
    rare_excess: int = 20
    distribution_tvd: float = .05
    missing_pp: float = 1.0
    severe_missing_pp: float = 5.0
    severe_pp: float = 10.0
    p_gate: float = .01
    q_gate: float = .05
    source_timezone: str = "America/Chicago"
    raw_dimensions: bool = True
    source_lag_days: int = 1
    readiness_lookback_days: int = 7
    readiness_baseline_weeks: int = 4
    readiness_min_baseline_days: int = 3
    readiness_min_baseline_volume: int = 100
    readiness_volume_fraction: float = .80
    readiness_id_fraction: float = .99
    readiness_required_present_fraction: float = .80
    readiness_attribute_ready_fraction: float = .90
    readiness_attribute_baseline_days: int = 14

    def __post_init__(self):
        if not 1 <= self.workers <= 4 or not 40 <= self.expansion_limit <= 120:
            raise ValueError("Default operating envelope is <=4 workers and 40..120 items.")
        if not 0 <= self.reserve_fraction <= .10:
            raise ValueError("Combined sentinel/dismissed reserve must be <=10%.")
        if self.simulations_min < 99 or self.simulations_max < self.simulations_min:
            raise ValueError("Invalid bounded simulation envelope.")
        if min(self.input_tokens_day, self.output_tokens_day, self.input_tokens_request,
               self.output_tokens_request, self.transcript_attempts_day, self.review_attempts_day) < 1:
            raise ValueError("Budgets must be positive.")
        validate_readiness_settings({
            name: getattr(self, name) for name in (*READINESS_INTEGER_BOUNDS, *READINESS_FRACTIONS)})
        root = Path(__file__).resolve().parents[2]
        resolved = self.state_dir.resolve()
        if resolved == root or root in resolved.parents:
            raise ValueError("Attribute Health state must be outside the source checkout.")

    @classmethod
    def from_env(cls, **overrides):
        defaults = cls()
        values = {}
        for field in fields(cls):
            raw = os.getenv("OPTIC_HEALTH_" + field.name.upper())
            default = getattr(defaults, field.name)
            if raw is not None:
                if isinstance(default, bool):
                    if raw.lower() not in {"0", "1", "true", "false"}:
                        raise ValueError(f"Invalid boolean setting {field.name}.")
                    values[field.name] = raw.lower() in {"1", "true"}
                elif isinstance(default, Path):
                    values[field.name] = Path(raw).expanduser()
                elif isinstance(default, tuple):
                    values[field.name] = tuple(x.strip().upper() for x in raw.split(",") if x.strip())
                else:
                    values[field.name] = type(default)(raw)
        values.update(overrides)
        return cls(**values)
