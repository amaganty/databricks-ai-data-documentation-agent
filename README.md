# AI Data Documentation Agent (Databricks Workflows + Mosaic AI)

Automated data documentation for Unity Catalog tables using deterministic profiling + Mosaic AI (Model Serving).
This job scans a target `catalog.schema`, collects metadata, generates documentation, and writes it back to:
- **Unity Catalog comments** (table + column comments) when permitted, and
- **Delta “source of truth” documentation tables** in `winfo_dbx.ai_docs` for history, auditability, and reporting.

---

## What this solves

Manual data documentation is slow and becomes stale quickly.
This agent automates documentation in a **repeatable, auditable, idempotent** way:

1. Discover tables in a selected scope (`catalog.schema`)
2. Extract metadata:
   - schema + types
   - table stats (`DESCRIBE DETAIL`)
   - sample rows
   - null profiling
3. Call a chat LLM via Databricks Model Serving (Mosaic AI)
4. Generate STRICT JSON:
   - table description
   - column descriptions
   - tags
   - data quality notes
   - confidence score (0..1)
5. Write documentation back:
   - **Path A:** Unity Catalog comments (preferred)
   - **Path B:** Delta docs tables (always)
6. Log every run for monitoring + debugging

---

## Architecture overview

**Workflow / Job Task**
- Databricks Workflow runs the orchestrator notebook on a schedule or on-demand.
- All execution is inside Databricks (Spark + Unity Catalog + Model Serving).

**Deterministic metadata tools**
- `list_tables(catalog, schema)`
- `get_table_schema(table_full_name)`
- `get_table_stats(table_full_name)`
- `sample_rows(table_full_name, n)`
- `profile_nulls(table_full_name)`

**LLM generation**
- Calls a Model Serving chat endpoint.
- Validates output strictly:
  - JSON only (no markdown)
  - no hallucinated columns
  - required keys enforced
  - confidence in range

**Write paths**
- **UC comments**: `COMMENT ON TABLE`, `ALTER TABLE ... ALTER COLUMN ... COMMENT`
- **Delta docs tables**: always append records for lineage/history

---

## Output tables (Unity Catalog)

All stored in `winfo_dbx.ai_docs`:

### `run_log`
Audit record per job run:
- run_id, timestamps, status
- scope catalog/schema
- tables_processed, errors_count
- model_endpoint, force flag
- params_json, error_sample

### `table_docs`
One row per table per run:
- table_description, tags, data_quality_notes, confidence
- schema_hash, generated_ts

### `column_docs`
One row per column per run:
- column_description, null_pct, data_type, generated_ts

---

## How to run (on-demand)

### Option 1: Run the Databricks Workflow
Open the job **AI Data Docs Agent** and click **Run now**.

### Parameters
- `catalog` (string): catalog name (default `winfo_dbx`)
- `schema` (string): schema name (default `bronze`)
- `include_tables` (regex, optional): only include matching tables
- `exclude_tables` (regex, optional): exclude matching tables
- `force` (true/false): re-generate docs even if already generated today
- `max_tables` (int): cap number of tables per run
- `sample_rows_n` (int): number of sample rows for LLM context

---

## Idempotency behavior

If `force=false`, tables are skipped when docs were generated **today** (UTC) in `ai_docs.table_docs`.

This prevents unnecessary cost and repeated documentation churn while still allowing manual refresh when needed.

---

## Monitoring

### Latest runs
```sql
SELECT started_ts, finished_ts, status, tables_processed, errors_count, model_endpoint
FROM winfo_dbx.ai_docs.run_log
ORDER BY started_ts DESC
LIMIT 20;
