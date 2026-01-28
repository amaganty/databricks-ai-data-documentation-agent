# Databricks notebook source
# DBTITLE 1,Cell 1
# Databricks notebook source
# ==========================================
# AI Data Documentation Agent — Orchestrator
# ==========================================
# Runs as a Databricks Workflow task (background).
# Discovers tables -> extracts metadata -> calls Model Serving -> writes docs -> run_log.
# Idempotent by day unless force=true.

import re
import json
import uuid
import math
import decimal
import datetime
import hashlib
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import functions as F
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

# ---------- Widgets / parameters ----------
dbutils.widgets.text("catalog", "winfo_dbx")
dbutils.widgets.text("schema", "bronze")
dbutils.widgets.text("include_tables", "")   # regex
dbutils.widgets.text("exclude_tables", "")   # regex
dbutils.widgets.dropdown("force", "false", ["false", "true"])
dbutils.widgets.text("max_tables", "50")
dbutils.widgets.text("sample_rows_n", "10")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA  = dbutils.widgets.get("schema").strip()
INCLUDE = dbutils.widgets.get("include_tables").strip()
EXCLUDE = dbutils.widgets.get("exclude_tables").strip()
FORCE  = dbutils.widgets.get("force").strip().lower() == "true"
MAX_TB = int(dbutils.widgets.get("max_tables"))
SAMPLE_N = int(dbutils.widgets.get("sample_rows_n"))

DOC_OUTPUT_SCHEMA = f"{CATALOG}.ai_docs"

PRIMARY_ENDPOINT  = "databricks-claude-sonnet-4-5"
FALLBACK_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

w = WorkspaceClient()

# ---------- Helpers ----------
def _ok(data: Any) -> Dict[str, Any]:
    return {"ok": True, "data": data, "error": None}

def _err(code: str, message: str, detail: Optional[str] = None) -> Dict[str, Any]:
    return {"ok": False, "data": None, "error": {"code": code, "message": message, "detail": detail}}

def _sql_escape(s: str) -> str:
    return (s or "").replace("'", "''")

def _table_hash(columns: List[Dict[str, str]]) -> str:
    payload = json.dumps(columns, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def _to_jsonable(x):
    if x is None or isinstance(x, (str, int, float, bool)):
        if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
            return None
        return x
    if isinstance(x, (datetime.datetime, datetime.date, datetime.time)):
        return x.isoformat()
    try:
        import pandas as pd
        if isinstance(x, pd.Timestamp):
            return x.isoformat()
    except Exception:
        pass
    if isinstance(x, decimal.Decimal):
        return float(x)
    if isinstance(x, dict):
        return {str(k): _to_jsonable(v) for k, v in x.items()}
    if isinstance(x, (list, tuple, set)):
        return [_to_jsonable(v) for v in x]
    try:
        from pyspark.sql import Row
        if isinstance(x, Row):
            return _to_jsonable(x.asDict(recursive=True))
    except Exception:
        pass
    return str(x)

# ---------- Tooling (read-only) ----------
def list_tables(catalog: str, schema: str) -> Dict[str, Any]:
    try:
        rows = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
        out = [(f"{catalog}.{schema}.{r['tableName']}", "UNKNOWN") for r in rows]
        return _ok(out)
    except Exception as e:
        return _err("LIST_TABLES_FAILED", str(e))

def get_table_schema(table_full_name: str) -> Dict[str, Any]:
    try:
        df = spark.table(table_full_name)
        cols = [{"name": f.name, "type": f.dataType.simpleString(), "nullable": bool(f.nullable)} for f in df.schema.fields]
        return _ok({"columns": cols, "schema_hash": _table_hash([{"name": c["name"], "type": c["type"]} for c in cols])})
    except Exception as e:
        return _err("GET_SCHEMA_FAILED", str(e))

def get_table_stats(table_full_name: str) -> Dict[str, Any]:
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {table_full_name}").toPandas().to_dict(orient="records")
        return _ok(detail[0] if detail else {})
    except Exception as e:
        return _err("GET_STATS_FAILED", str(e))

def sample_rows(table_full_name: str, n: int = 20) -> Dict[str, Any]:
    try:
        df = spark.table(table_full_name).limit(n)
        rows = [r.asDict(recursive=True) for r in df.collect()]
        return _ok(rows)
    except Exception as e:
        return _err("SAMPLE_ROWS_FAILED", str(e))

def profile_nulls(table_full_name: str) -> Dict[str, Any]:
    try:
        df = spark.table(table_full_name)
        total = df.count()
        if total == 0:
            nulls = [{"column": f.name, "null_pct": 0.0} for f in df.schema.fields]
            return _ok({"total_rows": 0, "null_profile": nulls})

        exprs = [F.avg(F.when(F.col(c).isNull(), F.lit(1)).otherwise(F.lit(0))).alias(c) for c in df.columns]
        null_rates_row = df.select(*exprs).collect()[0].asDict()
        nulls = [{"column": c, "null_pct": float(null_rates_row[c])} for c in df.columns]
        return _ok({"total_rows": int(total), "null_profile": nulls})
    except Exception as e:
        return _err("PROFILE_NULLS_FAILED", str(e))

# ---------- LLM (Model Serving) ----------
_JSON_ONLY_SYSTEM = """You are a data documentation generator.
Return JSON only. No markdown. No commentary.

Hard constraints:
- Do NOT invent columns not present in schema.columns[].name
- column_descriptions keys MUST be a subset of schema.columns[].name
- confidence must be a number between 0 and 1
- If sample_rows is empty or total_rows is 0, mention insufficient sample data in data_quality_notes
"""

def _extract_json(text: str) -> dict:
    text = text.strip()
    if text.startswith("{") and text.endswith("}"):
        return json.loads(text)
    m = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not m:
        raise ValueError("Model did not return a JSON object.")
    return json.loads(m.group(0))

def _call_chat_endpoint(endpoint: str, messages: list[dict]) -> str:
    role_map = {"system": ChatMessageRole.SYSTEM, "user": ChatMessageRole.USER, "assistant": ChatMessageRole.ASSISTANT}
    chat_messages = [ChatMessage(role=role_map[m["role"]], content=m["content"]) for m in messages]
    resp = w.serving_endpoints.query(name=endpoint, messages=chat_messages)
    return resp.choices[0].message.content

def generate_docs_once(endpoint: str, table_name: str, schema_json: dict, stats_json: dict, sample_rows_json: list, null_profile_json: dict) -> dict:
    payload = _to_jsonable({
        "table_name": table_name,
        "schema": schema_json,
        "stats": stats_json,
        "sample_rows": sample_rows_json,
        "null_profile": null_profile_json,
    })

    user_msg = f"""
Generate documentation for the provided table.

Return STRICT JSON with EXACTLY these top-level keys:
{{
  "table_description": "...",
  "column_descriptions": {{ "col": "desc", ... }},
  "tags": ["..."],
  "data_quality_notes": ["..."],
  "confidence": 0.0
}}

Rules:
- column_descriptions keys MUST be a subset of schema.columns[].name
- Do not include extra keys
- Do not hallucinate columns

INPUT JSON:
{json.dumps(payload)}
"""

    content = _call_chat_endpoint(endpoint, [{"role": "system", "content": _JSON_ONLY_SYSTEM},
                                            {"role": "user", "content": user_msg}])
    out = _extract_json(content)

    required = {"table_description", "column_descriptions", "tags", "data_quality_notes", "confidence"}
    if set(out.keys()) != required:
        raise ValueError(f"Invalid keys: got={sorted(out.keys())} required={sorted(required)}")

    cols = {c["name"] for c in schema_json.get("columns", [])}
    bad_cols = [k for k in out["column_descriptions"].keys() if k not in cols]
    if bad_cols:
        raise ValueError(f"Hallucinated columns: {bad_cols}")

    conf = out["confidence"]
    if not isinstance(conf, (int, float)) or conf < 0 or conf > 1:
        raise ValueError(f"confidence must be 0..1, got: {conf}")

    return out

def generate_docs_with_fallback(table_name: str, schema_json: dict, stats_json: dict, sample_rows_json: list, null_profile_json: dict) -> Tuple[dict, str]:
    try:
        return generate_docs_once(PRIMARY_ENDPOINT, table_name, schema_json, stats_json, sample_rows_json, null_profile_json), PRIMARY_ENDPOINT
    except Exception as e1:
        out = generate_docs_once(FALLBACK_ENDPOINT, table_name, schema_json, stats_json, sample_rows_json, null_profile_json)
        return out, FALLBACK_ENDPOINT

# ---------- Write paths ----------
def try_write_uc_comments(table_full_name: str, doc_json: dict, schema_json: dict) -> Dict[str, Any]:
    try:
        spark.sql(f"COMMENT ON TABLE {table_full_name} IS '{_sql_escape(doc_json.get('table_description',''))}'")        
        cols = [c["name"] for c in schema_json.get("columns", [])]
        col_descs = doc_json.get("column_descriptions") or {}
        for col in cols:
            if col in col_descs and col_descs[col]:
                spark.sql(f"ALTER TABLE {table_full_name} ALTER COLUMN `{col}` COMMENT '{_sql_escape(col_descs[col])}'")
        return _ok({"path": "UC_COMMENTS"})
    except Exception as e:
        return _err("UC_WRITE_FAILED", str(e))

def write_delta_docs(output_schema: str, run_id: str, table_full_name: str, doc_json: dict, schema_json: dict, nulls_json: dict) -> Dict[str, Any]:
    try:
        now_ts = F.current_timestamp()

        trow = spark.createDataFrame([{
            "run_id": run_id,
            "table_full_name": table_full_name,
            "table_description": doc_json.get("table_description"),
            "tags": doc_json.get("tags"),
            "data_quality_notes": doc_json.get("data_quality_notes"),
            "confidence": float(doc_json.get("confidence")) if doc_json.get("confidence") is not None else None,
            "schema_hash": schema_json.get("schema_hash"),
        }]).withColumn("generated_ts", now_ts)

        trow.write.mode("append").saveAsTable(f"{output_schema}.table_docs")

        null_map = {x["column"]: x["null_pct"] for x in (nulls_json.get("null_profile") or [])}
        cols = schema_json.get("columns") or []
        col_descs = doc_json.get("column_descriptions") or {}

        rows = []
        for c in cols:
            name = c["name"]
            rows.append({
                "run_id": run_id,
                "table_full_name": table_full_name,
                "column_name": name,
                "data_type": c.get("type"),
                "column_description": col_descs.get(name),
                "null_pct": float(null_map.get(name)) if name in null_map else None,
            })

        cdf = spark.createDataFrame(rows).withColumn("generated_ts", now_ts)
        cdf.write.mode("append").saveAsTable(f"{output_schema}.column_docs")

        return _ok({"path": "DELTA_DOCS", "columns_written": len(rows)})
    except Exception as e:
        return _err("DELTA_WRITE_FAILED", str(e))

def already_done_today(output_schema: str, table_full_name: str) -> bool:
    # idempotency: skip if we've generated docs for this table today (UTC day)
    q = f"""
    SELECT 1
    FROM {output_schema}.table_docs
    WHERE table_full_name = '{table_full_name}'
      AND to_date(generated_ts) = current_date()
    LIMIT 1
    """
    return spark.sql(q).count() > 0

def write_run_log(output_schema: str, run_row: dict) -> None:
    spark.createDataFrame([run_row]).write.mode("append").saveAsTable(f"{output_schema}.run_log")

# ---------- Main ----------
run_id = str(uuid.uuid4())
started_ts = datetime.datetime.now(datetime.timezone.utc)

tables_res = list_tables(CATALOG, SCHEMA)
if not tables_res["ok"]:
    raise Exception(tables_res)

tables = [t for (t, _) in tables_res["data"]]

# include/exclude filtering
if INCLUDE:
    rx = re.compile(INCLUDE)
    tables = [t for t in tables if rx.search(t)]
if EXCLUDE:
    rx = re.compile(EXCLUDE)
    tables = [t for t in tables if not rx.search(t)]

tables = tables[:MAX_TB]

processed = 0
errors = 0
error_sample = ""
endpoint_used_any = PRIMARY_ENDPOINT

for tname in tables:
    if (not FORCE) and already_done_today(DOC_OUTPUT_SCHEMA, tname):
        continue

    schema_res = get_table_schema(tname)
    stats_res  = get_table_stats(tname)
    sample_res = sample_rows(tname, n=SAMPLE_N)
    nulls_res  = profile_nulls(tname)

    if not (schema_res["ok"] and stats_res["ok"] and sample_res["ok"] and nulls_res["ok"]):
        errors += 1
        if error_sample == "":
            error_sample = json.dumps(_to_jsonable({
                "table": tname,
                "schema_res": schema_res,
                "stats_res": stats_res,
                "sample_res": sample_res,
                "nulls_res": nulls_res,
            }))[:1000]
        continue

    try:
        doc_json, endpoint_used = generate_docs_with_fallback(
            tname, schema_res["data"], stats_res["data"], sample_res["data"], nulls_res["data"]
        )
        endpoint_used_any = endpoint_used

        uc_res = try_write_uc_comments(tname, doc_json, schema_res["data"])
        delta_res = write_delta_docs(DOC_OUTPUT_SCHEMA, run_id, tname, doc_json, schema_res["data"], nulls_res["data"])

        if not delta_res["ok"]:
            raise Exception(delta_res)

        processed += 1
    except Exception as e:
        errors += 1
        if error_sample == "":
            error_sample = str(e)[:1000]

finished_ts = datetime.datetime.now(datetime.timezone.utc)
status = "SUCCESS" if errors == 0 else ("PARTIAL_SUCCESS" if processed > 0 else "FAILED")

write_run_log(DOC_OUTPUT_SCHEMA, {
    "run_id": run_id,
    "started_ts": started_ts,
    "finished_ts": finished_ts,
    "status": status,
    "scope_catalog": CATALOG,
    "scope_schema": SCHEMA,
    "tables_processed": int(processed),
    "errors_count": int(errors),
    "model_endpoint": endpoint_used_any,
    "force": bool(FORCE),
    "params_json": json.dumps({
        "catalog": CATALOG,
        "schema": SCHEMA,
        "include_tables": INCLUDE,
        "exclude_tables": EXCLUDE,
        "max_tables": MAX_TB,
        "sample_rows_n": SAMPLE_N,
    }),
    "error_sample": error_sample
})

print("DONE")
print("run_id:", run_id)
print("status:", status)
print("processed:", processed, "errors:", errors)

display(spark.sql(f"SELECT * FROM {DOC_OUTPUT_SCHEMA}.run_log ORDER BY started_ts DESC LIMIT 5"))
