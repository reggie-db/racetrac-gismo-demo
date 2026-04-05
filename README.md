# RaceTrac GISMO Databricks Demo

This repo demonstrates a Databricks-first path to modernize RaceTrac fuel operations without a risky monolith rewrite. It focuses on a fast, incremental foundation:

- Synthetic operational signals modeled after Salesforce, RightAngle, FuelOpt outputs, DTN, PC Miler, terminals, weather, payroll, and related systems.
- A serverless Lakeflow Declarative Pipeline with streaming bronze ingest and medallion transforms.
- AI-style forecasting and explainability outputs for planner and dispatch decisions.
- Gold serving sync into Lakebase for low-latency operational access.
- Semantic metadata and Genie space provisioning for natural language exploration.

## Why This Demo Exists

RaceTrac has inventory visibility gaps, low planner explainability, fragmented reporting, and tightly coupled logic across many systems. This demo shows how Databricks can become the data and intelligence abstraction layer first, then support phased app and agent rollout later.

## Architecture

1. **Source simulation job** writes append-only dummy events to `source_operational_signals`.
2. **Lakeflow pipeline** reads that source as a stream and builds bronze, silver, and gold layers.
3. **AI and explainability logic** creates forecast outputs and transparent decision signals.
4. **Post-pipeline job** applies semantic metadata and syncs gold to Lakebase.
5. **Genie deployment** exposes curated gold tables for conversational analytics.

## Project Layout

- `packages/common`: Shared GISMO constants and SQL helpers.
- `packages/jobs`: Synthetic source generation job.
- `packages/pipeline`: Lakeflow pipeline, metadata application, and Lakebase sync.
- `packages/dashboard`: Lakeview dashboard assets over gold tables.
- `resources/`: Databricks Asset Bundle resources (jobs, pipeline, dashboard).
- `scripts/deploy_genie.py`: Genie provisioning script.

## Data Model

### Bronze

- `bronze_operational_signals`: Streaming ingestion of synthetic events across upstream source systems.

### Silver

- `silver_inventory_state`
- `silver_demand_drivers`
- `silver_dispatch_context`

### Gold

- `gold_inventory_visibility`
- `gold_planner_explainability`
- `gold_demand_forecast`
- `gold_dispatch_exposure`
- `gold_sales_context`
- `gold_ai_query_decisions`

## Quick Start

1) Build artifacts:

```bash
uv build --wheel
```

2) Validate and deploy bundle:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

3) Generate synthetic source events:

```bash
databricks bundle run gismo_source_simulation -t dev
```

4) Run Lakeflow pipeline:

```bash
databricks bundle run gismo_lakeflow_pipeline -t dev
```

5) Run post-pipeline operations:

```bash
databricks bundle run gismo_post_pipeline_ops -t dev
```

6) Deploy Genie:

```bash
pixi run deploy-genie
```

7) Deploy dashboard:

```bash
databricks bundle run gismo_gold_dashboard -t dev
```

## Source Simulation Controls

`gismo_source_simulation` supports configurable demo volume and cadence:

- `row_count` (default `960`)
- `hour_window` (default `72`)
- `batch_tag` (default `demo`)

These are configured as Databricks job parameters in the bundle resource.

## Lakebase Sync Requirements

Lakebase sync is intentionally strict-fail for safe operations. You must provide a DSN:

- Environment variable for the post-pipeline task: `LAKEBASE_DSN`
- Example:

```bash
export LAKEBASE_DSN="postgresql://<user>:<pass>@<host>:5432/<db>?sslmode=require"
databricks bundle deploy -t dev
```

Gold sync validates:

- Source vs Lakebase row counts
- Distinct business-key counts per serving table

## Validation Queries

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

## Roadmap Fit

This repo is intentionally structured so a Databricks App can be added later as the GISMO UI layer, while keeping ingestion, forecasting, explainability, and semantic serving independent and modular.
