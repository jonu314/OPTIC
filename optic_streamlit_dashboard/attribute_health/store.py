"""Transactional local state, fenced claims, and shared Central-day budgets."""
from __future__ import annotations

import json
import base64
import zlib
import math
import sqlite3
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from .config import CENTRAL, Settings
from .contracts import canonical_json, digest


class ConflictError(ValueError):
    status = 409


class LeaseLost(ConflictError):
    pass


class BudgetExceeded(RuntimeError):
    pass


class RunEnvelopeExceeded(BudgetExceeded):
    pass


OUTCOMES = {
    "Likely classification degradation", "Real population change supported",
    "Ambiguous contract", "Data/processing issue", "Inconclusive",
}
IMMUTABLE = {"runs", "replays", "audit_contexts", "evidence", "charts", "chart_images", "manifests", "judgment_versions", "extractions", "detector_families",
             "dispositions", "rubrics", "item_reviews", "errors", "incident_versions"}
KINDS = IMMUTABLE | {"registry", "incidents", "judgments", "audit_bindings", "retries", "checkpoints", "run_progress",
                     "rotation", "daily", "reviews"}


class Store:
    def __init__(self, settings=None, *, clock=None):
        self.settings = settings if isinstance(settings, Settings) else Settings(
            state_dir=Path(settings) if settings is not None else Settings().state_dir)
        self.state_dir = self.settings.state_dir.expanduser().resolve()
        self.root = self.state_dir
        root = Path(__file__).resolve().parents[2]
        if self.state_dir == root or root in self.state_dir.parents:
            raise ValueError("Health state must be outside the source checkout.")
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.path = self.state_dir / "health.sqlite3"
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        with self._connection() as db:
            db.executescript("""
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS objects (
                    kind TEXT NOT NULL, id TEXT NOT NULL, payload TEXT NOT NULL,
                    immutable INTEGER NOT NULL, version INTEGER NOT NULL DEFAULT 1,
                    created REAL NOT NULL, updated REAL NOT NULL, PRIMARY KEY(kind,id));
                CREATE TABLE IF NOT EXISTS leases (
                    resource TEXT PRIMARY KEY, owner TEXT NOT NULL, fence INTEGER NOT NULL,
                    expires REAL NOT NULL);
                CREATE INDEX IF NOT EXISTS object_listing ON objects(kind,created,id);
                CREATE TABLE IF NOT EXISTS idempotency (
                    scope TEXT NOT NULL, key TEXT NOT NULL, request_hash TEXT NOT NULL,
                    result TEXT NOT NULL, PRIMARY KEY(scope,key));
                CREATE TABLE IF NOT EXISTS reservations (
                    id TEXT PRIMARY KEY, day TEXT NOT NULL, category TEXT NOT NULL,
                    pool TEXT NOT NULL, input_tokens INTEGER NOT NULL,
                    output_tokens INTEGER NOT NULL, actual_input INTEGER,
                    actual_output INTEGER, state TEXT NOT NULL, successful INTEGER NOT NULL DEFAULT 0,
                    uncertain INTEGER NOT NULL DEFAULT 0, item_ref TEXT, transcript_hash TEXT,
                    created REAL NOT NULL, expires REAL NOT NULL);
                CREATE INDEX IF NOT EXISTS reservation_day ON reservations(day,state);
                CREATE TABLE IF NOT EXISTS audit_items (
                    day TEXT NOT NULL, item_ref TEXT NOT NULL, production_result_id TEXT NOT NULL,
                    transcript_hash TEXT, PRIMARY KEY(day,item_ref));
                CREATE TABLE IF NOT EXISTS run_envelopes (
                    run_id TEXT NOT NULL, kind TEXT NOT NULL, incident_id TEXT NOT NULL,
                    created REAL NOT NULL, PRIMARY KEY(run_id,kind,incident_id));
                CREATE TABLE IF NOT EXISTS run_envelope_initializations (
                    run_id TEXT NOT NULL, kind TEXT NOT NULL, PRIMARY KEY(run_id,kind));
            """)

    def _now(self):
        value = self.clock()
        if value.tzinfo is None:
            raise ValueError("Store clock must be timezone-aware.")
        return value

    @contextmanager
    def _connection(self):
        db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=30000")
        try:
            yield db
        finally:
            db.close()

    @contextmanager
    def _transaction(self):
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
            except BaseException:
                db.rollback()
                raise
            else:
                db.commit()

    @staticmethod
    def _encode(payload):
        encoded = canonical_json(payload)
        if len(encoded) >= 16_384:
            compressed = "zlib1:" + base64.b64encode(zlib.compress(encoded.encode("utf-8"))).decode("ascii")
            if len(compressed) < len(encoded) * .8:
                return compressed
        return encoded

    @staticmethod
    def _decode(encoded):
        if encoded.startswith("zlib1:"):
            encoded = zlib.decompress(base64.b64decode(encoded[6:], validate=True)).decode("utf-8")
        return json.loads(encoded)

    @staticmethod
    def _validate(kind, identifier, payload=None):
        if kind not in KINDS or not isinstance(identifier, str) or not identifier or len(identifier) > 512:
            raise ValueError("Invalid health object kind or identifier.")
        if payload is not None and not isinstance(payload, dict):
            raise ValueError("Health payloads must be objects.")

    def _put(self, db, kind, identifier, payload, immutable=False, expected_version=None):
        self._validate(kind, identifier, payload)
        payload = dict(payload)
        payload.setdefault("id", identifier)
        encoded = self._encode(payload)
        old = db.execute("SELECT * FROM objects WHERE kind=? AND id=?", (kind, identifier)).fetchone()
        if old and (old["payload"] == encoded or canonical_json(self._decode(old["payload"])) == canonical_json(payload)):
            return payload
        if old and old["immutable"]:
            raise ConflictError("An immutable snapshot cannot be replaced.")
        if expected_version is not None and (old["version"] if old else 0) != expected_version:
            raise ConflictError("Health object changed concurrently.")
        now = self._now().timestamp()
        db.execute("""
            INSERT INTO objects(kind,id,payload,immutable,version,created,updated)
            VALUES(?,?,?,?,1,?,?)
            ON CONFLICT(kind,id) DO UPDATE SET payload=excluded.payload,
                immutable=excluded.immutable, version=objects.version+1, updated=excluded.updated
        """, (kind, identifier, encoded, int(immutable or kind in IMMUTABLE), now, now))
        return payload

    def put(self, kind, id, payload, immutable=False, *, lease=None, expected_version=None):
        with self._transaction() as db:
            if lease:
                self._assert_lease(db, lease)
            return self._put(db, kind, id, payload, immutable, expected_version)

    @staticmethod
    def _get(db, kind, identifier):
        row = db.execute("SELECT payload FROM objects WHERE kind=? AND id=?", (kind, identifier)).fetchone()
        return Store._decode(row["payload"]) if row else None

    def get(self, kind, id):
        self._validate(kind, id)
        with self._connection() as db:
            result = self._get(db, kind, id)
            if result and kind == "runs":
                result.update(self._get(db, "run_progress", id) or {})
            return result

    def list(self, kind):
        self._validate(kind, "_")
        with self._connection() as db:
            rows = db.execute("SELECT id,payload FROM objects WHERE kind=? ORDER BY created,id", (kind,))
            result = [self._decode(row["payload"]) for row in rows]
            if kind == "runs":
                for value in result:
                    value.update(self._get(db, "run_progress", value["id"]) or {})
            return result

    def version(self, kind, identifier):
        with self._connection() as db:
            row = db.execute("SELECT version FROM objects WHERE kind=? AND id=?", (kind, identifier)).fetchone()
            return row["version"] if row else 0

    def claim(self, resource, owner, seconds=None):
        if not resource or not owner:
            raise ValueError("Lease resource and owner are required.")
        duration = seconds if seconds is not None else self.settings.lease_seconds
        if duration <= 0:
            raise ValueError("Lease duration must be positive.")
        now = self._now().timestamp()
        with self._transaction() as db:
            row = db.execute("SELECT * FROM leases WHERE resource=?", (resource,)).fetchone()
            if row and row["expires"] > now:
                return None
            fence = row["fence"] + 1 if row else 1
            db.execute("""
                INSERT INTO leases(resource,owner,fence,expires) VALUES(?,?,?,?)
                ON CONFLICT(resource) DO UPDATE SET owner=excluded.owner,
                    fence=excluded.fence,expires=excluded.expires
            """, (resource, owner, fence, now + duration))
            return {"resource": resource, "owner": owner, "fence": fence, "expires": now + duration}

    def _assert_lease(self, db, lease):
        row = db.execute("SELECT * FROM leases WHERE resource=?", (lease["resource"],)).fetchone()
        if not row or row["owner"] != lease["owner"] or row["fence"] != lease["fence"] or (
                row["expires"] <= self._now().timestamp()):
            raise LeaseLost("Worker lease expired or was fenced by a newer worker.")

    def assert_lease(self, lease):
        with self._connection() as db:
            self._assert_lease(db, lease)

    def renew(self, lease, seconds=None):
        if seconds is not None and seconds <= 0:
            raise ValueError("Lease duration must be positive.")
        with self._transaction() as db:
            self._assert_lease(db, lease)
            expires = self._now().timestamp() + (seconds or self.settings.lease_seconds)
            db.execute("UPDATE leases SET expires=? WHERE resource=?", (expires, lease["resource"]))
            return dict(lease, expires=expires)

    def release(self, lease):
        with self._transaction() as db:
            self._assert_lease(db, lease)
            db.execute("UPDATE leases SET expires=0 WHERE resource=?", (lease["resource"],))

    @staticmethod
    def _author(payload):
        if not isinstance(payload.get("author"), str) or not payload["author"].strip():
            raise ValueError("Explicit local reviewer identification is required.")
        for field in ("note", "text"):
            if field in payload and (not isinstance(payload[field], str) or len(payload[field]) > 20000):
                raise ValueError("Invalid review text.")

    def _mutation(self, scope, payload, key, operation):
        if not isinstance(key, str) or not key.strip() or len(key) > 200:
            raise ValueError("An idempotency key is required.")
        request_hash = digest(payload)
        with self._transaction() as db:
            old = db.execute("SELECT * FROM idempotency WHERE scope=? AND key=?", (scope, key)).fetchone()
            if old:
                if old["request_hash"] != request_hash:
                    raise ConflictError("Idempotency key was already used for different content.")
                return json.loads(old["result"])
            result = operation(db, uuid.uuid4().hex, self._now().isoformat())
            db.execute("INSERT INTO idempotency VALUES(?,?,?,?)",
                       (scope, key, request_hash, canonical_json(result)))
            return result

    def record_disposition(self, incident_id, payload, idempotency_key):
        self._author(payload)
        if payload.get("outcome") not in OUTCOMES or payload.get("action") not in {
                "Monitoring", "Resolved", "Needs human review"}:
            raise ValueError("Invalid disposition outcome or action.")
        def save(db, identifier, timestamp):
            incident = self._get(db, "incidents", incident_id)
            if not incident:
                raise ValueError("Unknown incident.")
            result = self._put(db, "dispositions", identifier,
                               dict(payload, incident_id=incident_id, created_at=timestamp))
            incident.update(disposition=result, status=payload["action"], next_action=payload["action"])
            self._put(db, "incidents", incident_id, incident)
            return result
        return self._mutation("disposition:" + incident_id, payload, idempotency_key, save)

    def save_rubric(self, contract_key, payload, idempotency_key):
        self._author(payload)
        if not isinstance(payload.get("text"), str) or not payload["text"].strip() or (
                type(payload.get("approved")) is not bool):
            raise ValueError("Rubric text and an explicit boolean approval state are required.")
        def save(db, identifier, timestamp):
            if not self._get(db, "registry", contract_key):
                raise ValueError("Unknown attribute/job contract.")
            existing = [self._decode(row["payload"]) for row in db.execute(
                "SELECT payload FROM objects WHERE kind='rubrics'")]
            version = 1 + max((x["version"] for x in existing if x["contract_key"] == contract_key), default=0)
            result = dict(payload, contract_key=contract_key, version=version,
                          created_at=timestamp, approval_state="approved" if payload["approved"] else "draft")
            result["rubric_hash"] = digest({"text": payload["text"], "approved": payload["approved"],
                                           "version": version, "contract_key": contract_key})
            return self._put(db, "rubrics", identifier, result)
        return self._mutation("rubric:" + contract_key, payload, idempotency_key, save)

    def latest_rubric(self, contract_key):
        values = [x for x in self.list("rubrics") if x["contract_key"] == contract_key]
        return max(values, key=lambda x: x["version"]) if values else None

    def approved_rubric(self, contract_key):
        values = [x for x in self.list("rubrics") if x["contract_key"] == contract_key and x.get("approved") is True]
        return max(values, key=lambda x: x["version"]) if values else None

    def record_item_review(self, incident_id, item_id, payload, idempotency_key):
        self._author(payload)
        if payload.get("decision") not in {"accept", "reject", "uncertain"}:
            raise ValueError("Invalid human review decision.")
        def save(db, identifier, timestamp):
            judgment = self._get(db, "judgments", f"{incident_id}:{item_id}")
            if not judgment or judgment.get("incident_id") != incident_id:
                raise ValueError("Unknown incident judgment.")
            return self._put(db, "item_reviews", identifier, dict(
                payload, incident_id=incident_id, item_id=item_id, created_at=timestamp))
        return self._mutation(f"item:{incident_id}:{item_id}", payload, idempotency_key, save)

    def queue_retry(self, incident_id, payload, idempotency_key):
        self._author(payload)
        if payload.get("stage", "audit") not in {"review", "sample", "audit", "summarize"}:
            raise ValueError("Invalid retry stage.")
        def save(db, identifier, timestamp):
            incident = self._get(db, "incidents", incident_id)
            if not incident:
                raise ValueError("Unknown incident.")
            if incident.get("status") == "Resolved":
                raise ConflictError("Resolved incidents cannot be retried without reopening.")
            if incident.get("status") not in {"Technical error", "Audit queued", "AI review pending",
                    "Needs human review", "Audit partial", "Retry queued", "Out of budget"}:
                raise ConflictError("This incident has no retry-eligible stage.")
            pending = [self._decode(row["payload"]) for row in db.execute(
                "SELECT payload FROM objects WHERE kind='retries'")]
            if any(x.get("incident_id") == incident_id and x.get("status") in {"queued", "pending", "running"}
                   for x in pending):
                raise ConflictError("A local retry is already pending for this incident.")
            stage = payload.get("stage", "audit")
            if stage in {"review", "audit"}:
                budget = self._budget(db, self._now().astimezone(CENTRAL).date().isoformat())
                category = "review_attempts" if stage == "review" else "transcript_attempts"
                if budget[category] >= budget["limits"][category] or (
                        budget["input_tokens"] >= self.settings.input_tokens_day or
                        budget["output_tokens"] >= self.settings.output_tokens_day):
                    raise ConflictError("Current shared budget does not permit this retry stage.")
            result = self._put(db, "retries", identifier, dict(
                payload, incident_id=incident_id, status="queued", dispatch="local only",
                created_at=timestamp, ai_dispatch_enabled=False))
            incident["next_action"] = "Local retry queued; explicit worker authorization required"
            self._put(db, "incidents", incident_id, incident)
            return result
        return self._mutation("retry:" + incident_id, payload, idempotency_key, save)

    def _expire_reservations(self, db):
        db.execute("""
            UPDATE reservations SET state='settled', actual_input=input_tokens,
                actual_output=output_tokens, uncertain=1
            WHERE state='reserved' AND expires<=?
        """, (self._now().timestamp(),))

    def _budget(self, db, day):
        rows = db.execute("SELECT * FROM reservations WHERE day=?", (day,)).fetchall()
        items = db.execute("SELECT * FROM audit_items WHERE day=?", (day,)).fetchall()
        by_category = {name: sum(x["category"] == name for x in rows) for name in ("judge", "review")}
        reserve = sum(x["pool"] in {"sentinel", "dismissed"} for x in rows if x["category"] == "judge")
        successful = sum(x["successful"] for x in rows)
        used_input = sum(x["actual_input"] if x["actual_input"] is not None else x["input_tokens"] for x in rows)
        used_output = sum(x["actual_output"] if x["actual_output"] is not None else x["output_tokens"] for x in rows)
        return {
            "day": day, "timezone": "America/Chicago", "attempts": by_category,
            "transcript_attempts": by_category["judge"], "review_attempts": by_category["review"],
            "successful_requests": successful, "retries": sum(x["item_ref"] is not None for x in rows)
                - len({x["item_ref"] for x in rows if x["item_ref"] is not None}),
            "call_attribute_items": len(items) if items else len({
                x["item_ref"] for x in rows if x["category"] == "judge" and x["item_ref"]}),
            "unique_transcripts": len({x["production_result_id"] for x in items}) if items else len({
                x["transcript_hash"] for x in rows if x["transcript_hash"]}),
            "unique_transcript_hashes": len({x["transcript_hash"] for x in rows if x["transcript_hash"]}),
            "input_tokens": used_input, "output_tokens": used_output,
            "reserved_requests": sum(x["state"] == "reserved" for x in rows),
            "uncertain_usage_requests": sum(x["uncertain"] for x in rows),
            "reserve_attempts": reserve,
            "limits": {"transcript_attempts": self.settings.transcript_attempts_day,
                       "review_attempts": self.settings.review_attempts_day,
                       "input_tokens": self.settings.input_tokens_day,
                       "output_tokens": self.settings.output_tokens_day,
                       "combined_reserve": math.floor(self.settings.transcript_attempts_day *
                                                     self.settings.reserve_fraction)},
        }

    def budget_summary(self):
        with self._connection() as db:
            return self._budget(db, self._now().astimezone(CENTRAL).date().isoformat())

    def record_audit_item(self, item_ref, production_result_id, transcript_hash=None, *, lease=None):
        with self._transaction() as db:
            if lease:
                self._assert_lease(db, lease)
            db.execute("""INSERT INTO audit_items(day,item_ref,production_result_id,transcript_hash)
                VALUES(?,?,?,?) ON CONFLICT(day,item_ref) DO UPDATE SET transcript_hash=excluded.transcript_hash""",
                       (self._now().astimezone(CENTRAL).date().isoformat(), item_ref,
                        str(production_result_id), transcript_hash))

    def reserve_run_envelope(self, run_id, kind, incident_id, *, lease, phase_lease=None):
        """Count distinct incident work under the immutable run's original limits."""
        if kind not in {"review", "pilot"} or not isinstance(incident_id, str) or not incident_id:
            raise ValueError("Invalid per-run envelope reservation.")
        if not lease or lease.get("resource") != "run:" + run_id:
            raise ValueError("A matching run lease is required for envelope reservations.")
        with self._transaction() as db:
            self._assert_lease(db, lease)
            if phase_lease:
                self._assert_lease(db, phase_lease)
            run = self._get(db, "runs", run_id)
            if not run:
                raise ValueError("Unknown immutable run.")
            field = "max_reviews_run" if kind == "review" else "max_pilots_run"
            limit = run.get("settings", {}).get(field, 10 if kind == "review" else 5)
            if type(limit) is not int or limit < 1:
                raise ValueError("Invalid frozen per-run envelope.")
            initialized = db.execute("SELECT 1 FROM run_envelope_initializations WHERE run_id=? AND kind=?",
                                     (run_id, kind)).fetchone()
            if not initialized:
                for historic in self._historic_run_envelope(db, run_id, kind):
                    db.execute("INSERT OR IGNORE INTO run_envelopes VALUES(?,?,?,?)",
                               (run_id, kind, historic, self._now().timestamp()))
                db.execute("INSERT INTO run_envelope_initializations VALUES(?,?)", (run_id, kind))
            count = db.execute("SELECT COUNT(*) FROM run_envelopes WHERE run_id=? AND kind=?",
                               (run_id, kind)).fetchone()[0]
            old = db.execute("SELECT 1 FROM run_envelopes WHERE run_id=? AND kind=? AND incident_id=?",
                             (run_id, kind, incident_id)).fetchone()
            if old:
                return {"run_id": run_id, "kind": kind, "incident_id": incident_id,
                        "count": count, "limit": limit, "new": False}
            if count >= limit:
                raise RunEnvelopeExceeded("Frozen per-run " + kind + " envelope exhausted.")
            db.execute("INSERT INTO run_envelopes VALUES(?,?,?,?)",
                       (run_id, kind, incident_id, self._now().timestamp()))
            return {"run_id": run_id, "kind": kind, "incident_id": incident_id,
                    "count": count + 1, "limit": limit, "new": True}

    @staticmethod
    def _historic_run_envelope(db, run_id, kind):
        result = []
        for row in db.execute("SELECT id,payload FROM objects WHERE kind='incidents'"):
            incident = Store._decode(row["payload"])
            field = "review_run_id" if kind == "review" else "sample_run_id"
            origin = incident.get(field, incident.get("run_id"))
            completed = incident.get("review") if kind == "review" else incident.get("sample_manifest")
            attempted = kind == "review" and (incident.get("error") or {}).get("stage") == "review"
            if origin == run_id and (completed or attempted):
                result.append(row["id"])
        return result

    def run_envelope_summary(self, run_id):
        with self._connection() as db:
            run = self._get(db, "runs", run_id)
            if not run:
                raise ValueError("Unknown immutable run.")
            result = {}
            for kind, field, default in (("review", "max_reviews_run", 10), ("pilot", "max_pilots_run", 5)):
                rows = {x["incident_id"] for x in db.execute(
                    "SELECT incident_id FROM run_envelopes WHERE run_id=? AND kind=?", (run_id, kind))}
                if not db.execute("SELECT 1 FROM run_envelope_initializations WHERE run_id=? AND kind=?",
                                  (run_id, kind)).fetchone():
                    rows.update(self._historic_run_envelope(db, run_id, kind))
                result[kind] = {"used": len(rows), "limit": run.get("settings", {}).get(field, default)}
            return result

    def reserve(self, category, input_tokens, output_tokens, *, pool="main", item_ref=None,
                transcript_hash=None, lease=None):
        if category not in {"judge", "review"} or pool not in {"main", "sentinel", "dismissed"}:
            raise ValueError("Invalid request category or budget pool.")
        if category == "review" and pool != "main":
            raise ValueError("Trend reviews do not consume the transcript reserve.")
        if any(type(x) is not int or x < 1 for x in (input_tokens, output_tokens)):
            raise ValueError("Positive conservative token reservations are required.")
        if input_tokens > self.settings.input_tokens_request or output_tokens > self.settings.output_tokens_request:
            raise BudgetExceeded("Per-request token envelope exceeded; transcript will not be truncated.")
        now = self._now()
        day = now.astimezone(CENTRAL).date().isoformat()
        with self._transaction() as db:
            if lease:
                self._assert_lease(db, lease)
            self._expire_reservations(db)
            current = self._budget(db, day)
            inflight = db.execute("SELECT COUNT(*) FROM reservations WHERE state='reserved'").fetchone()[0]
            cap = self.settings.transcript_attempts_day if category == "judge" else self.settings.review_attempts_day
            if current["attempts"][category] >= cap or inflight >= self.settings.workers:
                raise BudgetExceeded("Shared request/concurrency budget exhausted.")
            if category == "judge":
                reserve_cap = current["limits"]["combined_reserve"]
                if pool != "main" and current["reserve_attempts"] >= reserve_cap:
                    raise BudgetExceeded("Combined sentinel/dismissed reserve exhausted.")
                if pool == "main" and current["transcript_attempts"] - current["reserve_attempts"] >= cap - reserve_cap:
                    raise BudgetExceeded("Main budget exhausted; remaining capacity is reserved.")
            if current["input_tokens"] + input_tokens > self.settings.input_tokens_day or (
                    current["output_tokens"] + output_tokens > self.settings.output_tokens_day):
                raise BudgetExceeded("Shared daily token budget exhausted.")
            identifier = uuid.uuid4().hex
            db.execute("""INSERT INTO reservations
                (id,day,category,pool,input_tokens,output_tokens,state,item_ref,transcript_hash,created,expires)
                VALUES(?,?,?,?,?,?,'reserved',?,?,?,?)""", (
                    identifier, day, category, pool, input_tokens, output_tokens, item_ref,
                    transcript_hash, now.timestamp(), now.timestamp() + self.settings.lease_seconds))
            return identifier

    def settle(self, reservation, *, input_tokens=None, output_tokens=None, successful=False, uncertain=False):
        with self._transaction() as db:
            row = db.execute("SELECT * FROM reservations WHERE id=?", (reservation,)).fetchone()
            if not row:
                raise ValueError("Unknown reservation.")
            if row["state"] == "settled":
                return
            unknown = uncertain or input_tokens is None or output_tokens is None
            if unknown:
                input_tokens, output_tokens = row["input_tokens"], row["output_tokens"]
            if any(type(x) is not int or x < 0 for x in (input_tokens, output_tokens)):
                raise ValueError("Invalid actual usage.")
            db.execute("""UPDATE reservations SET state='settled', actual_input=?,actual_output=?,
                successful=?,uncertain=? WHERE id=?""",
                       (input_tokens, output_tokens, int(successful), int(unknown), reservation))

    def rotate(self, queue, identifiers, count=1):
        """Persist a fair queue cursor shared by every worker and dashboard restart."""
        ordered = sorted(set(identifiers))
        if not ordered or count <= 0:
            return []
        with self._transaction() as db:
            old = self._get(db, "rotation", queue) or {"cursor": 0}
            cursor = old["cursor"] % len(ordered)
            result = [ordered[(cursor + i) % len(ordered)] for i in range(min(count, len(ordered)))]
            self._put(db, "rotation", queue, {"cursor": (cursor + len(result)) % len(ordered)})
            return result
