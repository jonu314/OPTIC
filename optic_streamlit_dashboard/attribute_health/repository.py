from __future__ import annotations

import json
import os
import re
from copy import deepcopy
from collections import Counter, defaultdict
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from .config import CENTRAL, Settings, Window
from .contracts import Contract, aggregate_values, digest, normalize, resolve_contract

OBJECTS = {
    "integrated": "RETAIL_REVENUE_PRD.INTEGRATED.VERTEX_OPTIC_AGENTINTERACTION_DATA",
    "details": "RETAIL_PRD.LANDPOWER_ODS.VW_ODIN_OPTIC_PROMPTDETAILS",
    "prompts": "RETAIL_PRD.LANDPOWER_ODS.ODIN_OPTIC_PROMPT",
    "attributes": "RETAIL_PRD.LANDPOWER_ODS.ODIN_OPTIC_ATTRIBUTE",
    "results": "RETAIL_PRD.LANDPOWER_ODS.ODIN_DBO_OPTICRESULTS",
    "analysis": "RETAIL_PRD.LANDPOWER_ODS.ODIN_OPTIC_ANALYSIS",
    "content": "RETAIL_PRD.LANDPOWER_ODS.ODIN_OPTIC_ANALYSISCONTENT",
    "jobs": "RETAIL_PRD.LANDPOWER_ODS.ODIN_OPTIC_LOOKJOBNAME",
}


def identifier(value, qualified=True):
    parts = value.split(".")
    if len(parts) != (3 if qualified else 1) or any(
        not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", part) for part in parts):
        raise ValueError("Invalid Snowflake identifier.")
    return ".".join('"' + part.upper() + '"' for part in parts)


def secret(context, name):
    return (context.get_secret(name) if context else None) or os.getenv(name)


def connect(context=None):
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization

    pem = secret(context, "SNOWFLAKE_PRIVATE_KEY_PEM")
    path = secret(context, "SNOWFLAKE_PRIVATE_KEY_PATH")
    if not pem and path:
        pem = Path(path).read_text(encoding="utf-8")
    if not pem:
        raise ValueError("Snowflake key-pair secret is required.")
    key = serialization.load_pem_private_key(pem.strip().replace("\\n", "\n").encode(), password=None)
    der = key.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8,
                            serialization.NoEncryption())
    kwargs = {key.lower(): secret(context, "SNOWFLAKE_" + key)
              for key in ("USER", "ACCOUNT", "WAREHOUSE", "DATABASE", "SCHEMA", "ROLE")}
    if not kwargs["user"] or not kwargs["account"]:
        raise ValueError("SNOWFLAKE_USER and SNOWFLAKE_ACCOUNT are required.")
    return snowflake.connector.connect(**{k: v for k, v in kwargs.items() if v},
                                       private_key=der, login_timeout=20, network_timeout=120,
                                       session_parameters={"QUERY_TAG": "OPTIC_ATTRIBUTE_HEALTH_READ_ONLY",
                                                           "STATEMENT_TIMEOUT_IN_SECONDS": 180})


class SnowflakeRepository:
    """SELECT-only, bounded source adapter. No persistent warehouse state writes."""

    demo = False

    def __init__(self, settings=None, connection_factory=None, context=None):
        self.settings = settings or Settings.from_env()
        self.connection_factory = connection_factory or (lambda: connect(context))
        self.tables = {k: identifier(os.getenv("OPTIC_HEALTH_" + k.upper() + "_TABLE", v))
                       for k, v in OBJECTS.items()}
        ZoneInfo(self.settings.source_timezone)
        self.date_expr = "CONVERT_TIMEZONE(%s, 'America/Chicago', i.INTERACTIONDATE::TIMESTAMP_NTZ)"
        self._columns = None
        self._registry = None
        self._readiness_result = None
        self._heartbeat = None

    def set_heartbeat(self, callback):
        self._heartbeat = callback

    def query(self, sql, params=()):
        if not re.match(r"\s*(SELECT|WITH)\b", sql, re.I):
            raise ValueError("Attribute Health source adapter is SELECT-only.")
        if self._heartbeat is not None:
            self._heartbeat()
        conn = self.connection_factory()
        try:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, tuple(params))
                keys = [str(x[0]).lower() for x in cursor.description]
                return [dict(zip(keys, row)) for row in cursor.fetchall()]
            finally:
                cursor.close()
        finally:
            conn.close()

    def registry(self):
        rows = self.query(f"""
            SELECT p.JOBNAME, ROW_NUMBER() OVER (
                       PARTITION BY p.JOBNAME ORDER BY a.ATTRIBUTEID) AS ROWNUM,
                   a.ATTRIBUTEID, a.ATTRIBUTENAME,
                   a.DESCRIPTION, a.RESULTVALUETYPE, a.RESULTVALUEOPTIONS, a.PROMPT,
                   p.PROMPTID, p.ATTRIBUTEPROMPT AS SYSTEM_PROMPT,
                   p.HISTORYSTARTDATE AS PROMPT_HISTORY_START,
                   p.HISTORYENDDATE AS PROMPT_HISTORY_END,
                   a.HISTORYSTARTDATE, a.HISTORYENDDATE, a.AILASTUPDATEDDATE,
                   prm.value:PromptName::VARCHAR AS PROMPT_GROUP_NAME,
                   prm.value:Order::INT AS PROMPT_GROUP_ORDER,
                   prm.value:Enabled::BOOLEAN AS PROMPT_GROUP_ENABLED,
                   ra.value:AttributeId::INT AS MEMBER_ATTRIBUTE_ID,
                   ra.value:AttributeName::VARCHAR AS MEMBER_ATTRIBUTE_NAME,
                   ra.value:Order::INT AS MEMBER_ORDER,
                   ra.value:Enabled::BOOLEAN AS MEMBER_ENABLED
            FROM {self.tables['prompts']} p,
                 LATERAL FLATTEN(input => TRY_PARSE_JSON(p.ATTRIBUTELIST)) prm,
                 LATERAL FLATTEN(input => prm.value:ResultAttributeList) ra
            JOIN {self.tables['attributes']} a ON a.ATTRIBUTEID = ra.value:AttributeId::INT
            WHERE COALESCE(a.DISABLED, FALSE) = FALSE AND COALESCE(p.ENABLED, FALSE) = TRUE
              AND (a.HISTORYENDDATE IS NULL OR a.HISTORYENDDATE >= CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ)
              AND (p.HISTORYENDDATE IS NULL OR p.HISTORYENDDATE >= CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ)
              AND (p.HISTORYSTARTDATE IS NULL OR p.HISTORYSTARTDATE <= CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ)
              AND (a.HISTORYSTARTDATE IS NULL OR a.HISTORYSTARTDATE <= CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ)
              AND (prm.value:Enabled IS NULL OR TRY_TO_BOOLEAN(prm.value:Enabled::VARCHAR) = TRUE)
              AND (ra.value:Enabled IS NULL OR TRY_TO_BOOLEAN(ra.value:Enabled::VARCHAR) = TRUE)
            ORDER BY p.JOBNAME, a.ATTRIBUTEID, p.PROMPTID, prm.INDEX, ra.INDEX
        """)
        contracts = {}
        for row in rows:
            if row.get("member_attribute_id") is None or str(row["member_attribute_id"]) != str(row.get("attributeid")):
                raise ValueError("Registry row lacks verified active-prompt attribute-ID membership.")
            binding = {"prompt_id": row.get("promptid"), "group_name": row.get("prompt_group_name"),
                       "group_order": row.get("prompt_group_order"),
                       "group_enabled": row.get("prompt_group_enabled"),
                       "member_attribute_id": row["member_attribute_id"],
                       "member_attribute_name": row.get("member_attribute_name"),
                       "member_order": row.get("member_order"), "member_enabled": row.get("member_enabled")}
            contract = resolve_contract(row, str(row.get("system_prompt") or ""),
                                        {"source": "active prompt-group AttributeId -> active attribute",
                                         "prompt_table": self.tables["prompts"], "attribute_table": self.tables["attributes"],
                                         "prompt_id": row.get("promptid"), "active_prompt_bindings": [binding],
                                         "metadata_history_start": str(row.get("historystartdate") or ""),
                                         "runtime_history": "unverified; not an activation date",
                                         "historical_membership": "current active membership retrospective"})
            if not contract.attribute_id or not contract.name:
                raise ValueError("Registry row lacks its attribute ID or output name.")
            member_name = str(row.get("member_attribute_name") or "").strip()
            if member_name and member_name.casefold() != contract.name.casefold():
                contract = replace(contract, contract_status="active member/output-name mismatch")
            if contract.key in contracts:
                prior = contracts[contract.key]
                bindings = prior.provenance["active_prompt_bindings"] + [binding]
                state = contract.contract_status if contract.contract_status != "resolved" else prior.contract_status
                contract = replace(prior, contract_status=state,
                                   provenance={**prior.provenance, "active_prompt_bindings": bindings})
            contracts[contract.key] = contract
        groups = defaultdict(list)
        names = defaultdict(set)
        for contract in contracts.values():
            groups[(contract.attribute_id, contract.job)].append(contract)
            names[(contract.name.lower(), contract.job)].add(contract.attribute_id)
        result = []
        for contract in contracts.values():
            state = contract.contract_status
            if len(groups[(contract.attribute_id, contract.job)]) > 1:
                state = "conflicting active job definitions"
            if len(names[(contract.name.lower(), contract.job)]) > 1:
                state = "ambiguous output-name mapping across attribute IDs"
            result.append(replace(contract, contract_status=state))
        self._registry = result
        return result

    def readiness(self, window, contracts=None):
        from .readiness import evaluate_job_readiness

        contracts = contracts if contracts is not None else (self._registry if self._registry is not None else self.registry())
        selected = [c for c in contracts if self.settings.all_active_jobs or c.job in self.settings.jobs]
        jobs = sorted({c.job for c in selected} | (
            set() if self.settings.all_active_jobs else set(self.settings.jobs)))
        evidence = {"days": [], "attributes": [], "unmapped_contracts": []}
        if jobs:
            job_params = ",".join(["%s"] * len(jobs))
            evidence["days"] = self.query(f"""
                WITH probe AS (
                    SELECT i.ID, UPPER(TRIM(i.JOBNAME)) AS JOB, i.CONNECTION_ID,
                           i.INTERACTIONDATE, {self.date_expr}::DATE AS DAY, i.UPDATE_DT_TM
                    FROM {self.tables['integrated']} i
                    WHERE {self.date_expr} >= %s::TIMESTAMP_NTZ
                      AND {self.date_expr} < %s::TIMESTAMP_NTZ
                      AND UPPER(TRIM(i.JOBNAME)) IN ({job_params})
                ), identities AS (
                    SELECT ID,JOB,COUNT(DISTINCT COALESCE(TO_VARCHAR(CONNECTION_ID),'<NULL>') || ':' ||
                           COALESCE(TO_VARCHAR(INTERACTIONDATE),'<NULL>')) AS VARIANTS
                    FROM probe WHERE ID IS NOT NULL GROUP BY ID,JOB
                )
                SELECT b.JOB,b.DAY,COUNT(DISTINCT b.ID)+COALESCE(COUNT_IF(b.ID IS NULL),0) AS N,
                       COUNT(DISTINCT b.ID) AS NONNULL_IDS,
                       COUNT(DISTINCT CASE WHEN d.VARIANTS>1 THEN b.ID END) AS IDENTITY_CONFLICTS,
                       MAX(b.UPDATE_DT_TM) AS WATERMARK
                FROM probe b LEFT JOIN identities d ON b.ID=d.ID AND b.JOB=d.JOB
                GROUP BY b.JOB,b.DAY ORDER BY b.JOB,b.DAY
            """, [self.settings.source_timezone, self.settings.source_timezone, window.start.isoformat(),
                  self.settings.source_timezone, (window.end + timedelta(days=1)).isoformat(), *jobs])
        required = [c for c in selected if not c.allow_empty and c.contract_status == "resolved"]
        for offset in range(0, len(required), 24):
            batch = required[offset:offset + 24]
            batch_jobs = sorted({c.job for c in batch})
            branches, parameters = [], []
            for contract in batch:
                expr = self.value_expr(contract)
                if expr is None:
                    evidence["unmapped_contracts"].append(contract.key)
                    continue
                branches.append(f"""
                    SELECT %s AS CONTRACT_KEY, UPPER(TRIM(i.JOBNAME)) AS JOB,
                           {self.date_expr}::DATE AS DAY, COUNT(*) AS N,
                           COALESCE(COUNT_IF(NULLIF(TRIM(TO_VARCHAR({expr})),'') IS NOT NULL
                               AND TO_VARCHAR({expr})<>'<BLANK>'),0) AS PRESENT
                    FROM bounded i WHERE UPPER(TRIM(i.JOBNAME))=%s GROUP BY 1,2,3
                """)
                parameters += [contract.key, self.settings.source_timezone]
                if "%s" in expr:
                    parameters += [contract.name, contract.name]
                parameters.append(contract.job)
            if branches:
                bounds = [self.settings.source_timezone, window.start.isoformat(), self.settings.source_timezone,
                          (window.end + timedelta(days=1)).isoformat(), *batch_jobs]
                evidence["attributes"] += self.query(
                    self._base().format(jobs=",".join(["%s"] * len(batch_jobs))) +
                    " UNION ALL ".join(branches), bounds + parameters)
        for row in evidence["days"] + evidence["attributes"]:
            row["day"] = str(row["day"])[:10]
            window.require(row["day"])
        result = evaluate_job_readiness(window, self.settings, jobs, evidence, contracts)
        result["rerun_policy"] = "latest UPDATE_DT_TM/RUNDATE only when ID/job connection/time identity is unique"
        self.apply_readiness(result, window)
        return result

    def apply_readiness(self, result, window):
        ready = date.fromisoformat(result["ready_through"])
        if ready > window.end:
            raise ValueError("Source readiness cannot extend the analytical end.")
        copied = deepcopy(result)
        if not isinstance(result.get("jobs"), dict):
            copied["deferred_jobs"] = sorted(set(result.get("deferred_jobs") or []) | set(self.settings.jobs))
            copied["diagnostic"] = "Frozen source snapshot lacks per-job readiness evidence; evaluation deferred."
        elif not self.settings.all_active_jobs:
            copied["deferred_jobs"] = sorted(set(result.get("deferred_jobs") or []) |
                                            (set(self.settings.jobs) - set(result["jobs"])))
        self._readiness_result = copied

    def _require_readiness(self, window, job=None):
        if self._readiness_result is None:
            self.readiness(window)
        if window.end > date.fromisoformat(self._readiness_result["ready_through"]):
            raise ValueError("Source end is incomplete; recompute the <=90-day window from ready_through.")
        if job and (job not in self._readiness_result.get("jobs", {}) or
                    job in self._readiness_result.get("deferred_jobs", [])):
            raise ValueError("Selected job has unknown/incomplete source readiness and is explicitly deferred.")

    def columns(self):
        if self._columns is None:
            name = os.getenv("OPTIC_HEALTH_INTEGRATED_TABLE", OBJECTS["integrated"])
            database, schema, table = name.split(".")
            self._columns = {str(r["column_name"]).upper() for r in self.query(
                f"SELECT COLUMN_NAME FROM {identifier(database, False)}.INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s", (schema.upper(), table.upper()))}
        return self._columns

    def value_expr(self, contract):
        column = contract.name.upper()
        if column in self.columns():
            return "i." + identifier(column, False)
        if "ODINDATA" in self.columns():
            # Names remain SQL values, not unvalidated identifier fragments.
            return "GET_IGNORE_CASE(TRY_PARSE_JSON(TO_VARCHAR(i.ODINDATA)), %s)"
        return None

    def _base(self):
        return f"""
            WITH bounded AS (
                SELECT i.* FROM {self.tables['integrated']} i
                WHERE {self.date_expr} >= %s::TIMESTAMP_NTZ
                  AND {self.date_expr} < %s::TIMESTAMP_NTZ
                  AND UPPER(TRIM(i.JOBNAME)) IN ({{jobs}})
                QUALIFY i.ID IS NULL OR (
                    COUNT(DISTINCT COALESCE(TO_VARCHAR(i.CONNECTION_ID), '<NULL>') || ':' ||
                          COALESCE(TO_VARCHAR(i.INTERACTIONDATE), '<NULL>')) OVER (
                          PARTITION BY i.ID, UPPER(TRIM(i.JOBNAME)))=1
                    AND ROW_NUMBER() OVER (
                    PARTITION BY i.ID, UPPER(TRIM(i.JOBNAME))
                    ORDER BY i.UPDATE_DT_TM DESC NULLS LAST, i.RUNDATE DESC NULLS LAST) = 1)
            )
        """

    def aggregate(self, contracts, window):
        self._require_readiness(window)
        deferred = set(self._readiness_result.get("deferred_jobs", []))
        selected = [c for c in contracts if (self.settings.all_active_jobs or c.job in self.settings.jobs)
                    and c.job not in deferred]
        result = {c.key: aggregate_values(c, [], window) for c in contracts}
        for offset in range(0, len(selected), 24):
            batch = selected[offset:offset + 24]
            jobs = sorted({c.job for c in batch})
            bounds = [self.settings.source_timezone, window.start.isoformat(), self.settings.source_timezone,
                      (window.end + timedelta(days=1)).isoformat(), *jobs]
            branches = []
            params = []
            for contract in batch:
                expr = self.value_expr(contract)
                if expr is None:
                    continue
                dimensions = ", i.HEALTH_LANGUAGE AS LANGUAGE, i.HEALTH_SITE AS SITE" if self.settings.raw_dimensions else ""
                branches.append(f"""
                    SELECT %s AS CONTRACT_KEY, {self.date_expr}::DATE AS DAY,
                           TO_VARCHAR({expr}) AS VALUE, i.BRAND {dimensions}, COUNT(*) AS COUNT
                    FROM {'enriched' if self.settings.raw_dimensions else 'bounded'} i
                    WHERE UPPER(TRIM(i.JOBNAME))=%s
                    GROUP BY 1,2,3,4 {',5,6' if self.settings.raw_dimensions else ''}
                """)
                params.extend([contract.key, self.settings.source_timezone])
                if "%s" in expr:
                    params.append(contract.name)
                params.append(contract.job)
            if not branches:
                continue
            dimension_sql = f"""
                , raw_latest AS (
                    SELECT r.ID, r.LANGUAGECD, r.SITE
                    FROM {self.tables['results']} r JOIN bounded b ON b.ID=r.ID
                      AND TRIM(TO_VARCHAR(b.CONNECTION_ID))=TRIM(TO_VARCHAR(r.CONNID))
                      AND UPPER(TRIM(b.JOBNAME))=UPPER(TRIM(r.JOBNAME))
                      AND ABS(DATEDIFF(second,b.INTERACTIONDATE,r.INTERACTIONDATE))<=5
                    QUALIFY DENSE_RANK() OVER (PARTITION BY r.ID ORDER BY r.CREATEDDATE DESC NULLS LAST)=1
                ), dimensions AS (
                    SELECT ID, CASE WHEN COUNT(DISTINCT LANGUAGECD)=1 THEN MIN(LANGUAGECD) END AS HEALTH_LANGUAGE,
                           CASE WHEN COUNT(DISTINCT SITE)=1 THEN MIN(SITE) END AS HEALTH_SITE
                    FROM raw_latest GROUP BY ID
                ), enriched AS (
                    SELECT b.*, d.HEALTH_LANGUAGE, d.HEALTH_SITE
                    FROM bounded b LEFT JOIN dimensions d ON b.ID=d.ID
                )
            """ if self.settings.raw_dimensions else ""
            rows = self.query(self._base().format(jobs=",".join(["%s"] * len(jobs))) +
                              dimension_sql + " UNION ALL ".join(branches), bounds + params)
            by_key = defaultdict(list)
            for row in rows:
                by_key[row["contract_key"]].append(row)
            for contract in batch:
                result[contract.key] = aggregate_values(contract, by_key[contract.key], window)
        return result

    def candidates(self, contract, window, start, end, label, complement, limit=120, seed=1):
        window.require(start)
        window.require(end)
        if start > end or not 1 <= limit <= 120:
            raise ValueError("Invalid bounded sample request.")
        self._require_readiness(window, contract.job)
        if label not in contract.labels or not contract.finite:
            raise ValueError("Audit sample strata require a declared finite label; processing/presence signals need investigation.")
        expr = self.value_expr(contract)
        if expr is None:
            return {"rows": [], "population_count": 0, "truncated": False,
                    "selection_method": "output column unavailable"}
        value_sql = f", sample_values AS (SELECT i.*, TO_VARCHAR({expr}) AS HEALTH_VALUE_TEXT FROM bounded i)"
        value_params = [contract.name] if "%s" in expr else []
        source = "sample_values"
        pred_params = []
        if contract.result_type in {"BOOL", "BOOLEMPTY"}:
            positive = (label == "true") != complement
            pred = "LOWER(TRIM(i.HEALTH_VALUE_TEXT)) IN " + (
                "('1','true')" if positive else "('0','false')")
        elif contract.result_type == "LIST":
            allowed = ",".join(["LOWER(%s)"] * len(contract.options))
            value_sql += f"""
                , parsed_lists AS (
                    SELECT s.*, TRY_PARSE_JSON(s.HEALTH_VALUE_TEXT) AS HEALTH_PARSED_LIST
                    FROM sample_values s
                ), normalized_lists AS (
                    SELECT s.*, CASE
                        WHEN TYPEOF(s.HEALTH_PARSED_LIST)='ARRAY' THEN s.HEALTH_PARSED_LIST
                        WHEN s.HEALTH_PARSED_LIST IS NULL
                             AND NULLIF(TRIM(s.HEALTH_VALUE_TEXT),'') IS NOT NULL
                             THEN TO_VARIANT(SPLIT(s.HEALTH_VALUE_TEXT,','))
                        ELSE NULL END AS HEALTH_LIST
                    FROM parsed_lists s
                ), list_membership AS (
                    SELECT s.ID, s.JOBNAME,
                           BOOLAND_AGG(CASE WHEN ARRAY_SIZE(s.HEALTH_LIST)=0 THEN TRUE
                               WHEN TYPEOF(f.VALUE)='VARCHAR'
                                AND LOWER(TRIM(f.VALUE::VARCHAR)) IN ({allowed})
                               THEN TRUE ELSE FALSE END) AS HEALTH_VALID_LIST,
                           BOOLOR_AGG(CASE WHEN TYPEOF(f.VALUE)='VARCHAR'
                                AND LOWER(TRIM(f.VALUE::VARCHAR))=LOWER(%s)
                               THEN TRUE ELSE FALSE END) AS HEALTH_HAS_LABEL
                    FROM normalized_lists s,
                         LATERAL FLATTEN(input=>s.HEALTH_LIST, OUTER=>TRUE) f
                    WHERE TYPEOF(s.HEALTH_LIST)='ARRAY'
                    GROUP BY s.ID,s.JOBNAME
                ), valid_list_values AS (
                    SELECT s.* FROM normalized_lists s
                    JOIN list_membership m ON m.ID=s.ID AND m.JOBNAME=s.JOBNAME
                    WHERE m.HEALTH_VALID_LIST AND m.HEALTH_HAS_LABEL={'FALSE' if complement else 'TRUE'}
                )
            """
            value_params += list(contract.options) + [label]
            source = "valid_list_values"
            pred = "TRUE"
        else:
            pred = "LOWER(TRIM(i.HEALTH_VALUE_TEXT))=LOWER(%s)"
            pred_params = [label]
            if complement:
                pred = f"NOT ({pred}) AND LOWER(TRIM(i.HEALTH_VALUE_TEXT)) IN (" + (
                    ",".join(["LOWER(%s)"] * len(contract.options))) + ")"
                pred_params += list(contract.options)
        sql = self._base().format(jobs="%s") + value_sql + f"""
            , population AS (
                SELECT i.ID AS PRODUCTION_RESULT_ID, i.CONNECTION_ID,
                       i.JOBNAME AS JOB, i.INTERACTIONDATE AS INTERACTION_DATE,
                       i.BRAND, i.AGENTREFERENCEID AS AGENT, i.HEALTH_VALUE_TEXT AS STORED_OUTPUT,
                       COUNT(*) OVER () AS POPULATION_COUNT
                FROM {source} i
                WHERE {self.date_expr} >= %s::TIMESTAMP_NTZ AND {self.date_expr} < %s::TIMESTAMP_NTZ
                  AND i.ID IS NOT NULL AND ({pred})
            )
            SELECT * FROM population
            ORDER BY HASH(TO_VARCHAR(PRODUCTION_RESULT_ID) || ':' || TO_VARCHAR(%s))
            LIMIT {int(limit)}
        """
        params = [self.settings.source_timezone, window.start.isoformat(), self.settings.source_timezone,
                  (window.end + timedelta(days=1)).isoformat(), contract.job]
        params += value_params
        params += [self.settings.source_timezone, str(start), self.settings.source_timezone,
                   str(end + timedelta(days=1)), *pred_params, seed]
        rows = self.query(sql, params)
        population = int(rows[0]["population_count"]) if rows else 0
        for row in rows:
            row["production_result_id"] = str(row["production_result_id"])
            row["interaction_date"] = str(row["interaction_date"])
            row["connection_id"] = str(row.get("connection_id") or "")
            row["attribute_name"] = contract.name
            row["attribute_id"] = contract.attribute_id
            row["contract_hash"] = contract.contract_hash
        return {"rows": rows, "population_count": population, "truncated": False,
                "selection_method": "seeded uniform; Snowflake HASH ordered probability sample"}

    def transcript(self, item, window):
        window.require(item["interaction_date"])
        rows = self.query(f"""
            WITH raw AS (
                SELECT * FROM {self.tables['results']} WHERE ID=%s
                QUALIFY DENSE_RANK() OVER (PARTITION BY ID ORDER BY CREATEDDATE DESC NULLS LAST)=1
            )
            SELECT r.ID AS PRODUCTION_RESULT_ID, r.CONNID AS RAW_CONNECTION,
                   r.JOBNAME AS RAW_JOB, r.INTERACTIONDATE AS RAW_TIME,
                   r.RESULTJSON AS RAW_RESULT_JSON, r.MODEL AS PRODUCTION_MODEL,
                   r.PROMPTVERSION AS PRODUCTION_PROMPT_VERSION,
                   a.ID AS ANALYSIS_RESULT_ID, a.ANALYSISID AS TRANSCRIPT_ANALYSIS_ID,
                   a.CONNID AS ANALYSIS_CONNECTION, a.INTERACTIONDATE AS ANALYSIS_TIME,
                   a.AGENTREFERENCEID AS ANALYSIS_AGENT, j.NAME AS ANALYSIS_JOB,
                   c.ANALYSISCONTENTID AS ANALYSIS_CONTENT_ID, c.CONTENT,
                   c.CONTENTTRANSLATED
            FROM raw r
            LEFT JOIN {self.tables['analysis']} a ON a.ID=r.ID
            LEFT JOIN {self.tables['jobs']} j ON j.JOBNAMEID=a.JOBNAMEID
            LEFT JOIN {self.tables['content']} c ON c.ANALYSISID=a.ANALYSISID
            WHERE r.ID=%s
        """, (item["production_result_id"], item["production_result_id"]))
        if any(r.get("transcript_analysis_id") is not None for r in rows):
            return resolve_identity(item, rows, window)
        if not item.get("connection_id") or not item.get("agent"):
            return {"status": "missing", "production_result_id": item["production_result_id"],
                    "reason": "Fallback requires connection and agent identities; no latest-CONNID lookup."}
        # A unique report-compatible fallback is explicit, candidate-key bounded and never latest-CONNID.
        fallback = self.query(f"""
            SELECT r.ID AS PRODUCTION_RESULT_ID, r.CONNID AS RAW_CONNECTION,
                   r.JOBNAME AS RAW_JOB, r.INTERACTIONDATE AS RAW_TIME,
                   a.ID AS ANALYSIS_RESULT_ID, a.ANALYSISID AS TRANSCRIPT_ANALYSIS_ID,
                   a.CONNID AS ANALYSIS_CONNECTION, a.INTERACTIONDATE AS ANALYSIS_TIME,
                   a.AGENTREFERENCEID AS ANALYSIS_AGENT, j.NAME AS ANALYSIS_JOB,
                   c.ANALYSISCONTENTID AS ANALYSIS_CONTENT_ID, c.CONTENT
            FROM {self.tables['results']} r
            JOIN {self.tables['analysis']} a ON TRIM(TO_VARCHAR(a.CONNID))=TRIM(TO_VARCHAR(r.CONNID))
                AND UPPER(TRIM(a.AGENTREFERENCEID))=UPPER(TRIM(%s))
                AND (ABS(DATEDIFF(second, a.INTERACTIONDATE, r.INTERACTIONDATE))<=5
                    OR ABS(DATEDIFF(second, a.INTERACTIONDATE,
                       CONVERT_TIMEZONE('UTC','America/Chicago',r.INTERACTIONDATE::TIMESTAMP_NTZ)))<=5)
            JOIN {self.tables['jobs']} j ON j.JOBNAMEID=a.JOBNAMEID
                AND UPPER(TRIM(j.NAME))=UPPER(TRIM(r.JOBNAME))
            JOIN {self.tables['content']} c ON c.ANALYSISID=a.ANALYSISID
            WHERE r.ID=%s AND r.CONNID=%s AND UPPER(TRIM(r.JOBNAME))=UPPER(TRIM(%s))
        """, (item.get("agent") or "", item["production_result_id"], item["connection_id"], item["job"]))
        return resolve_identity(item, [], window, fallback_rows=fallback)


def _time(value):
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _consistent_time(left, right):
    a, b = _time(left), _time(right)
    if a.tzinfo and b.tzinfo:
        return abs((a - b).total_seconds()) <= 5
    if a.tzinfo:
        a = a.astimezone(CENTRAL).replace(tzinfo=None)
    if b.tzinfo:
        b = b.astimezone(CENTRAL).replace(tzinfo=None)
    if abs((a - b).total_seconds()) <= 5:
        return True
    # Established report lookup permits a unique raw UTC-to-Central alternative.
    return abs((a.replace(tzinfo=timezone.utc).astimezone(CENTRAL).replace(tzinfo=None) - b).total_seconds()) <= 5


def resolve_identity(item, rows, window, fallback_rows=None):
    window.require(item["interaction_date"])
    out = {key: item.get(key) for key in ("production_result_id", "connection_id", "job", "interaction_date")}
    out.update(status="missing", match_method="exact production ID -> analysis.ID -> content.ANALYSISID")
    if not item.get("connection_id") or not str(item.get("job") or "").strip():
        return dict(out, reason="Connection/job consistency cannot be verified.")
    if not rows and fallback_rows:
        rows = fallback_rows
        out["match_method"] = "unique connection+job+agent+timezone-aware time"
    if not rows:
        return out
    for row in rows:
        if out["match_method"].startswith("exact") and str(row.get("production_result_id")) != str(item["production_result_id"]):
            return dict(out, status="inconsistent")
        if row.get("transcript_analysis_id") is None:
            return out
        for field in ("raw_connection", "analysis_connection"):
            if str(row.get(field) or "") != str(item.get("connection_id") or ""):
                return dict(out, status="inconsistent")
        for field in ("raw_job", "analysis_job"):
            if str(row.get(field) or "").strip().upper() != str(item["job"]).strip().upper():
                return dict(out, status="inconsistent")
        for field in ("raw_time", "analysis_time"):
            if row.get(field) is None or not _consistent_time(item["interaction_date"], row[field]):
                return dict(out, status="inconsistent")
            window.require(row[field])
        if item.get("agent") and str(row.get("analysis_agent") or "").upper() != str(item["agent"]).upper():
            return dict(out, status="inconsistent")
        if out["match_method"].startswith("exact") and str(row.get("analysis_result_id")) != str(item["production_result_id"]):
            return dict(out, status="inconsistent")
    identities = {str(row.get("transcript_analysis_id")) for row in rows if row.get("transcript_analysis_id") is not None}
    if len(identities) != 1:
        return dict(out, status="ambiguous" if identities else "missing")
    raw_outputs = {str(row.get("raw_result_json")) for row in rows if row.get("raw_result_json") is not None}
    if len(raw_outputs) > 1:
        return dict(out, status="ambiguous", reason="tied latest production-result snapshots disagree")
    out["production_model"] = rows[0].get("production_model")
    out["production_prompt_version"] = rows[0].get("production_prompt_version")
    out["output_consistency"] = "raw output unavailable / unverified"
    if raw_outputs and item.get("attribute_name"):
        raw_value = rows[0]["raw_result_json"]
        if isinstance(raw_value, str):
            try:
                raw_value = json.loads(raw_value)
            except json.JSONDecodeError:
                return dict(out, status="inconsistent", reason="invalid production result JSON")
        if not isinstance(raw_value, dict):
            return dict(out, status="inconsistent", reason="production result JSON is not an object")
        match = next((value for key, value in raw_value.items() if key.casefold() == item["attribute_name"].casefold()), None)
        out["raw_stored_output"] = match
        out["output_consistency"] = ("matching normalized source representation" if
                                    str(match).strip().casefold() == str(item.get("stored_output")).strip().casefold()
                                    else "source output representations require contract normalization")
    content = {str(r.get("analysis_content_id")): r for r in rows if str(r.get("content") or "").strip()}
    if not content:
        return out
    # Duplicate identical content is safe; different chunks have unverified assembly semantics.
    texts = {str(r["content"]) for r in content.values()}
    if len(texts) > 1:
        return dict(out, status="multi-content unresolved", content_rows=len(content))
    row = sorted(content.values(), key=lambda r: str(r.get("analysis_content_id")))[0]
    text = str(row["content"])
    if row.get("truncated") or item.get("truncated"):
        return dict(out, status="truncated")
    out.update(status="resolved", transcript_analysis_id=str(row["transcript_analysis_id"]),
               analysis_content_id=str(row["analysis_content_id"]), transcript=text,
               transcript_hash=digest(text), content_rows=len(content), completeness="full selected source content")
    return out


class FixtureRepository:
    demo = True

    def __init__(self, settings=None, fixture=None):
        self.settings = settings or Settings()
        self.fixture = (json.loads(Path(fixture).read_text(encoding="utf-8-sig"))
                        if isinstance(fixture, (str, Path)) else fixture)
        if self.fixture is not None and not isinstance(self.fixture, dict):
            raise ValueError("A fixture must be a JSON object.")
        self._window = None
        self._contract = Contract("demo-1", "product_offer", "ACQUISITION", "BOOL", prompt=(
            "Return 1 only if the agent explicitly offers a product to the customer; otherwise return 0."),
            system_prompt="Analyze customer-agent interaction using exact transcript evidence.",
            provenance={"source": "synthetic fixture, not production"})

    def registry(self):
        if self.fixture:
            return [Contract.from_dict(c) for c in self.fixture["contracts"]]
        return [self._contract,
                Contract("demo-2", "notes", "SERVICE", "STRING", value_mode="TEXT_PRESENCE_3_WORDS",
                         prompt="Provide supporting text or an empty string.", allow_empty=True),
                Contract("demo-3", "multi_label", "RETENTIONCHATTX", "LIST", ("Billing", "Move"), channel="chat"),
                Contract("demo-4", "measurement", "RETENTION", "NUMERIC")]

    def readiness(self, window):
        return {"ready_through": window.end.isoformat(), "watermark": str(window.end) + "T23:59:59",
                "readiness_method": "explicit synthetic fixture"}

    def aggregate(self, contracts, window):
        self._window = window
        if self.fixture:
            result = {}
            for contract in contracts:
                supplied = self.fixture.get("daily", {}).get(contract.key)
                if supplied is None:
                    source = [row for row in self.fixture.get("values", [])
                              if row["contract_key"] == contract.key and window.contains(row["day"])]
                    result[contract.key] = aggregate_values(contract, source, window)
                    continue
                by_day = {}
                for row in supplied:
                    window.require(row["day"])
                    if row["day"] in by_day:
                        raise ValueError("Fixture daily counts must already be at unique contract/day grain.")
                    by_day[row["day"]] = row
                complete = aggregate_values(contract, [], window)
                result[contract.key] = [by_day.get(row["day"], row) for row in complete]
            return result
        result = {}
        for contract in contracts:
            rows = []
            if contract.key == self._contract.key:
                for i, day in enumerate(window.days()):
                    n = 1200 if day.weekday() < 5 else 500
                    positive = round(n * (.12 if i < 56 else .43))
                    for label, count in [(True, positive), (False, n - positive)]:
                        rows.append({"day": str(day), "value": label, "count": count, "brand": "DEMO"})
            result[contract.key] = aggregate_values(contract, rows, window)
        return result

    def candidates(self, contract, window, start, end, label, complement, limit=120, seed=1):
        window.require(start)
        window.require(end)
        if start > end or not 1 <= limit <= 120:
            raise ValueError("Invalid fixture sample request.")
        rows = []
        import random
        rng = random.Random(seed)
        if self.fixture:
            eligible = []
            for item in self.fixture.get("candidates", []):
                window.require(item["interaction_date"])
                if item.get("contract_key") != contract.key or not str(start) <= item["interaction_date"][:10] <= str(end):
                    continue
                value = normalize(contract, item.get("stored_output"))
                has = label in value["labels"]
                if value["status"] == "valid" and has != complement:
                    eligible.append(item)
            unique = {str(i["production_result_id"]): i for i in eligible}
            eligible = sorted(unique.values(), key=lambda i: str(i["production_result_id"]))
            selected = rng.sample(eligible, min(limit, len(eligible)))
            return {"rows": selected, "population_count": len(eligible), "truncated": False,
                    "selection_method": "seeded uniform"}
        if contract.key != self._contract.key:
            return {"rows": [], "population_count": 0, "truncated": False,
                    "selection_method": "no eligible synthetic source rows for this contract"}
        true_label = label == "true"
        stored = (not true_label) if complement else true_label
        day_populations = []
        cursor = 0
        for i in range((end - start).days + 1):
            day = start + timedelta(days=i)
            n = 1200 if day.weekday() < 5 else 500
            positive = round(n * (.12 if (day - window.start).days < 56 else .43))
            count = positive if stored else n - positive
            day_populations.append((day, cursor, cursor + count))
            cursor += count
        population = cursor
        values = rng.sample(range(population), min(limit, population))
        for index in values:
            day, first, _ = next(p for p in day_populations if p[1] <= index < p[2])
            key = f"demo-{contract.attribute_id}-{day}-{index - first}-{int(stored)}"
            rows.append({"production_result_id": key, "connection_id": key + "-conn", "job": contract.job,
                         "interaction_date": str(day) + "T12:00:00", "stored_output": stored, "brand": "DEMO",
                         "agent": "DEMO", "attribute_name": contract.name, "attribute_id": contract.attribute_id,
                         "contract_hash": contract.contract_hash})
        return {"rows": rows, "population_count": population, "truncated": False,
                "selection_method": "seeded uniform"}

    def transcript(self, item, window):
        window.require(item["interaction_date"])
        if self.fixture:
            match = self.fixture.get("transcripts", {}).get(str(item["production_result_id"]))
            if match is None:
                return {**item, "status": "missing"}
            if match.get("status", "resolved") != "resolved":
                return {**item, **match}
            text = str(match.get("transcript") or "")
            if not text:
                return {**item, "status": "missing"}
            if match.get("truncated"):
                return {**item, "status": "truncated"}
            return {**item, **match, "status": "resolved", "transcript": text,
                    "transcript_hash": digest(text), "match_method": "explicit fixture identity",
                    "completeness": "full supplied fixture text"}
        stored = normalize(self._contract, item.get("stored_output"))["value"]
        last_index = int(str(item["production_result_id"]).rsplit("-", 2)[-2])
        offered = stored if last_index % 4 else not stored
        text = ("Agent: I can offer you our electricity product today.\nCustomer: Please explain the offer."
                if offered else "Agent: I will explain your existing bill.\nCustomer: Thank you for explaining it.")
        return {**item, "status": "resolved", "transcript": text, "transcript_hash": digest(text),
                "transcript_analysis_id": item["production_result_id"] + "-analysis",
                "analysis_content_id": item["production_result_id"] + "-content",
                "match_method": "explicit synthetic identity", "completeness": "full synthetic transcript"}


class ResearchRepository(FixtureRepository):
    """Aggregate-only recorded replay. No production transcript claims or downloads."""

    demo = False

    def __init__(self, path, settings=None):
        super().__init__(settings)
        self.data = json.loads(Path(path).read_text(encoding="utf-8-sig"))
        self._contracts = []
        records = self.data["metadata"]["rows"]
        for row in records:
            jobs = row.get("jobs", [])
            jobs = [x.strip() for x in jobs.split(",")] if isinstance(jobs, str) else jobs
            for job in jobs:
                contract = resolve_contract({**row, "job": job,
                                             "options": row.get("expected_display"),
                                             "attribute_id": row.get("attribute_id") or "unverified:" + row["attribute"]},
                                            provenance={"source": "recorded aggregate research",
                                                        "attribute_id": "unverified in aggregate-only snapshot"})
                self._contracts.append(contract)

    def registry(self):
        return self._contracts

    def aggregate(self, contracts, window):
        series = defaultdict(list)
        for group in ("acquisition_daily", "boolean_daily"):
            for row in self.data[group]["distribution"]:
                # The recorded acquisition-only queries are not evidence for other jobs.
                if window.contains(row["period"]):
                    series[row["attribute"].lower()].append(
                        {"day": row["period"], "value": row["raw_value"], "count": row["row_count"]})
        return {c.key: aggregate_values(c, series[c.name.lower()] if c.job == "ACQUISITION" else [], window)
                for c in contracts}

    def candidates(self, *args, **kwargs):
        return {"rows": [], "population_count": 0, "truncated": False,
                "selection_method": "aggregate-only snapshot has no call identities"}

    def transcript(self, item, window):
        window.require(item["interaction_date"])
        return {"status": "missing", "reason": "aggregate-only replay cannot resolve transcripts"}
