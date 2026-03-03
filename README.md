# RaceTrac GISMO Databricks Demo

This repository demonstrates how RaceTrac GISMO can be implemented as a Databricks-first abstraction layer:

- Mocked operational source signals for all adjacent systems in the target architecture.
- A serverless Lakeflow Declarative Pipeline that builds bronze, silver, and gold layers.
- AI forecast and criteria-driven AI query decision outputs.
- Gold table synchronization into Lakebase serving tables.
- Rich table and column metadata for Genie.
- Local Genie provisioning with `pixi run deploy-genie`.

## Repo Structure

- `packages/common`: existing shared package.
- `packages/pipeline`: GISMO pipeline package with Lakeflow, metadata, and Lakebase sync logic.
- `packages/jobs`: GISMO job package for synthetic source simulation.
- `packages/dashboard`: dashboard package with reusable gold-layer SQL query definitions and Lakeview asset.
- `resources/`: Databricks Asset Bundle pipeline and job resources.
- `scripts/deploy_genie.py`: local script to create or update a Genie space.
- `databricks.yml`: bundle root configuration.

## Data Layers

### Bronze

- `bronze_operational_signals`
  - Mocked events from Salesforce, PDI/RightAngle, MarketView, Gurobi, terminals, MDM, PC Miler, Veeder Root/Sticks, payroll/workday, DTN/ADS, weather feeds, and Samsara.

### Silver

- `silver_inventory_state`
- `silver_demand_drivers`
- `silver_dispatch_context`

### Gold

- `gold_inventory_visibility`
- `gold_planner_explainability`
- `gold_demand_forecast` (AI forecast demo)
- `gold_dispatch_exposure`
- `gold_sales_context`
- `gold_ai_query_decisions` (AI query criteria and reason codes)

## Bundle Deployment

1. Build and validate:

   ```bash
   uv build --wheel
   databricks bundle validate
   ```

2. Deploy:

   ```bash
   databricks bundle deploy -t dev
   ```

3. Run the Lakeflow pipeline:

   ```bash
   databricks bundle run gismo_source_simulation -t dev
   databricks bundle run gismo_lakeflow_pipeline -t dev
   ```

4. Run post-pipeline metadata and Lakebase sync job:

   ```bash
   databricks bundle run gismo_post_pipeline_ops -t dev
   ```

5. Deploy dashboard:

   ```bash
   databricks bundle run gismo_gold_dashboard -t dev
   ```

## Lakebase Requirements

The Lakebase sync job expects:

- `LAKEBASE_DSN` available in job environment or task context.
- Databricks SQL connection values:
  - `DATABRICKS_SERVER_HOSTNAME`
  - `DATABRICKS_HTTP_PATH`
  - `DATABRICKS_TOKEN`

Gold tables are copied into Lakebase schema `racetrac_gismo_serving` by default and validated with:

- Source vs target row counts.
- Distinct key integrity checks per serving table.

## Metadata and Genie

Table and column comments are managed by `pipeline.metadata` and include operator-friendly descriptions for semantic querying.

Create or update the Genie space locally with:

```bash
pixi run deploy-genie
```

Generate synthetic source-system rows locally with:

```bash
pixi run generate-source-data -- --warehouse-id <warehouse-id>
```

Deploy the gold-layer dashboard with:

```bash
pixi run deploy-gold-dashboard
```

The script targets `reggie_pierce.racetrac_gismo` by default and registers all gold tables plus sample questions.
