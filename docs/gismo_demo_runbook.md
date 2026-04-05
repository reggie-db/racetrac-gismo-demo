# GISMO Demo Runbook

This runbook walks through the full Databricks demo flow for RaceTrac modernization:

- synthetic source generation
- streaming medallion consolidation
- AI forecasting and explainability outputs
- strict Lakebase gold serving sync
- Genie semantic exploration

## Prerequisites

- Databricks CLI authenticated on profile `DEFAULT`.
- Permission to run jobs, pipelines, and SQL warehouse statements.
- Optional but recommended: Lakebase DSN ready for serving sync.

## 1) Build

```bash
uv build --wheel
```

## 2) Validate and Deploy Bundle

If Lakebase sync is enabled, set the DSN before execution:

```bash
export LAKEBASE_DSN="postgresql://<user>:<pass>@<host>:5432/<db>?sslmode=require"
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

If you omit `LAKEBASE_DSN`, post-pipeline sync will fail fast by design.

## 3) Generate Dummy Operational Source Data

```bash
databricks bundle run gismo_source_simulation -t dev
```

The source simulation job emits append-style batches with job-level controls:
- `row_count`
- `hour_window`
- `batch_tag`

## 4) Run Lakeflow Pipeline

```bash
databricks bundle run gismo_lakeflow_pipeline -t dev
```

The bronze table reads the source table as a stream and builds silver and gold outputs for inventory, planner explainability, demand forecast, dispatch risk, and AI decisioning.

## 5) Run Post-Pipeline Operations

```bash
databricks bundle run gismo_post_pipeline_ops -t dev
```

This job:
1. Applies table and column comments for semantic usability
2. Syncs gold tables to Lakebase
3. Validates source vs Lakebase row and key consistency

## 6) Verify Source and Medallion Layers

```sql
SELECT source_system, COUNT(*) AS row_count
FROM reggie_pierce.racetrac_gismo.source_operational_signals
GROUP BY source_system
ORDER BY source_system;
```

```sql
SELECT source_system, COUNT(*) AS row_count
FROM reggie_pierce.racetrac_gismo.bronze_operational_signals
GROUP BY source_system
ORDER BY source_system;
```

```sql
SELECT market_key, product_id, ai_forecast_units, forecast_confidence
FROM reggie_pierce.racetrac_gismo.gold_demand_forecast
ORDER BY ai_forecast_units DESC
LIMIT 20;
```

```sql
SELECT ai_query_action, ai_query_reason_code, COUNT(*) AS decision_count
FROM reggie_pierce.racetrac_gismo.gold_ai_query_decisions
GROUP BY ai_query_action, ai_query_reason_code
ORDER BY decision_count DESC;
```

## 7) Verify Lakebase Gold Serving (If Configured)

```sql
SELECT COUNT(*) AS inventory_rows
FROM racetrac_gismo_serving.gold_inventory_visibility;
```

```sql
SELECT COUNT(*) AS decision_rows
FROM racetrac_gismo_serving.gold_ai_query_decisions;
```

## 8) Deploy Genie

```bash
pixi run deploy-genie
```

## 9) Deploy Dashboard

```bash
databricks bundle run gismo_gold_dashboard -t dev
```

## 10) Genie Prompt Pack

- How much inventory do we have right now by market and terminal?
- Why was a planner decision made for a given market and product?
- Which markets have the highest 7-day forecasted demand?
- Which routes have the highest dispatch exposure risk?
- What decisions did ai_query make and why?
