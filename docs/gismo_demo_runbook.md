# GISMO Demo Runbook

## Prerequisites

- Databricks CLI authenticated with profile `DEFAULT`.
- SQL warehouse access for the active identity.
- Lakebase DSN available as `LAKEBASE_DSN`.

## 1) Build Artifact

```bash
uv build --wheel
```

## 2) Validate and Deploy Bundle

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

## 3) Generate Simulated Source Signals

```bash
databricks bundle run gismo_source_simulation -t dev
```

## 4) Execute Pipeline

```bash
databricks bundle run gismo_lakeflow_pipeline -t dev
```

## 5) Execute Post Pipeline Operations

The job applies metadata comments and then syncs gold tables to Lakebase:

```bash
databricks bundle run gismo_post_pipeline_ops -t dev
```

## 6) Verify Unity Catalog Tables

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

## 7) Verify Lakebase Serving Tables

```sql
SELECT COUNT(*) AS inventory_rows
FROM racetrac_gismo_serving.gold_inventory_visibility;
```

```sql
SELECT COUNT(*) AS decision_rows
FROM racetrac_gismo_serving.gold_ai_query_decisions;
```

The sync process already performs row-count and distinct-key checks. Querying confirms data is available for downstream apps.

## 8) Deploy Genie Space

```bash
pixi run deploy-genie
```

## 9) Deploy Gold Dashboard

```bash
pixi run deploy-gold-dashboard
```

## 10) Suggested Genie Prompts

- How much inventory do we have right now by market and terminal?
- Why was a planner decision made for a given market and product?
- Which markets have the highest 7-day forecasted demand?
- Which routes have the highest dispatch exposure risk?
- What decisions did ai_query make and why?
