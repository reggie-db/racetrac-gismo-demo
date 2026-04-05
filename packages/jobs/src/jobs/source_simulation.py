import argparse
import os
from dataclasses import dataclass

from common.gismo import (
    DEFAULT_GISMO_CATALOG,
    DEFAULT_GISMO_SCHEMA,
    source_system_case_sql,
)
from databricks.sdk import WorkspaceClient
from dbx_tools import clients
from lfp_logging import logs

# Creates synthetic source-system events used by the GISMO pipeline.

LOG = logs.logger()


@dataclass(frozen=True)
class SimulationConfig:
    catalog: str
    schema: str
    row_count: int
    hour_window: int
    batch_tag: str


def _parse_args() -> SimulationConfig:
    parser = argparse.ArgumentParser(
        description="Generate synthetic source data for GISMO pipeline ingestion."
    )
    parser.add_argument(
        "--catalog", default=os.getenv("GISMO_CATALOG", DEFAULT_GISMO_CATALOG)
    )
    parser.add_argument(
        "--schema", default=os.getenv("GISMO_SCHEMA", DEFAULT_GISMO_SCHEMA)
    )
    parser.add_argument("--row-count", type=int, default=960)
    parser.add_argument("--hour-window", type=int, default=72)
    parser.add_argument("--batch-tag", default=os.getenv("GISMO_BATCH_TAG", "demo"))
    args = parser.parse_args()
    if args.row_count <= 0:
        raise ValueError("--row-count must be greater than zero.")
    if args.hour_window <= 0:
        raise ValueError("--hour-window must be greater than zero.")
    if not str(args.batch_tag).strip():
        raise ValueError("--batch-tag must not be empty.")
    return SimulationConfig(
        catalog=args.catalog,
        schema=args.schema,
        row_count=args.row_count,
        hour_window=args.hour_window,
        batch_tag=str(args.batch_tag).strip(),
    )


def _statement_ok(
    workspace_client: WorkspaceClient, warehouse_id: str, statement: str
) -> None:
    response = workspace_client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s",
    )
    state = (
        response.status.state.value
        if response.status and response.status.state
        else "UNKNOWN"
    )
    if state != "SUCCEEDED":
        error_message = (
            response.status.error.message
            if response.status and response.status.error
            else "Unknown SQL error."
        )
        raise RuntimeError(
            f"Source simulation SQL failed with state {state}: {error_message}"
        )


def _create_source_table_sql(catalog: str, schema: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.`source_operational_signals` (
  id BIGINT,
  source_system STRING,
  signal_timestamp TIMESTAMP,
  market_key STRING,
  terminal_id STRING,
  product_id STRING,
  route_id STRING,
  retail_location_id STRING,
  inventory_units DOUBLE,
  demand_units DOUBLE,
  wholesale_cost DOUBLE,
  retail_price DOUBLE,
  weather_index DOUBLE,
  freight_cost DOUBLE,
  telematics_risk DOUBLE,
  planner_override_flag BOOLEAN,
  source_record_id STRING,
  ingest_batch_id STRING,
  quality_flag STRING,
  record_created_ts TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
)
"""


def _escape_sql_literal(raw_value: str) -> str:
    return raw_value.replace("'", "''")


def _insert_source_rows_sql(
    catalog: str, schema: str, row_count: int, hour_window: int, batch_tag: str
) -> str:
    source_system_case = source_system_case_sql(index_expression="raw_id % 12")
    escaped_batch_tag = _escape_sql_literal(batch_tag)
    return f"""
WITH source_rows AS (
  SELECT id AS raw_id
  FROM range(0, {row_count})
)
INSERT INTO `{catalog}`.`{schema}`.`source_operational_signals`
SELECT
  CAST((unix_millis(current_timestamp()) * 100000) + raw_id AS BIGINT) AS id,
  {source_system_case} AS source_system,
  timestampadd(HOUR, -CAST(raw_id % {hour_window} AS INT), current_timestamp()) AS signal_timestamp,
  concat('MKT-', lpad(cast(raw_id % 24 AS STRING), 2, '0')) AS market_key,
  concat('TRM-', lpad(cast(raw_id % 18 AS STRING), 3, '0')) AS terminal_id,
  concat('PRD-', lpad(cast(raw_id % 8 AS STRING), 2, '0')) AS product_id,
  concat('RTE-', lpad(cast(raw_id % 14 AS STRING), 3, '0')) AS route_id,
  concat('LOC-', lpad(cast(raw_id % 60 AS STRING), 4, '0')) AS retail_location_id,
  CAST(raw_id % 500 + 450 AS DOUBLE) AS inventory_units,
  CAST(raw_id % 160 + 120 AS DOUBLE) AS demand_units,
  CAST(raw_id % 45 + 150 AS DOUBLE) AS wholesale_cost,
  round((raw_id % 40 + 300) / 100.0, 3) AS retail_price,
  round((raw_id % 80) / 100.0, 3) AS weather_index,
  CAST(raw_id % 70 + 25 AS DOUBLE) AS freight_cost,
  round((raw_id % 100) / 100.0, 3) AS telematics_risk,
  (raw_id % 9 = 0) AS planner_override_flag,
  concat('SRC-', '{escaped_batch_tag}', '-', cast(raw_id AS STRING)) AS source_record_id,
  concat('BATCH-', '{escaped_batch_tag}', '-', date_format(current_timestamp(), 'yyyyMMddHHmmss')) AS ingest_batch_id,
  CASE WHEN raw_id % 31 = 0 THEN 'warning' ELSE 'ok' END AS quality_flag,
  current_timestamp() AS record_created_ts
FROM source_rows
"""


def main() -> None:
    config = _parse_args()
    workspace_client = WorkspaceClient()
    warehouse_id = str(clients.warehouse(workspace_client).id)
    _statement_ok(
        workspace_client=workspace_client,
        warehouse_id=warehouse_id,
        statement=f"CREATE SCHEMA IF NOT EXISTS `{config.catalog}`.`{config.schema}`",
    )
    _statement_ok(
        workspace_client=workspace_client,
        warehouse_id=warehouse_id,
        statement=_create_source_table_sql(config.catalog, config.schema),
    )
    _statement_ok(
        workspace_client=workspace_client,
        warehouse_id=warehouse_id,
        statement=_insert_source_rows_sql(
            config.catalog,
            config.schema,
            config.row_count,
            config.hour_window,
            config.batch_tag,
        ),
    )
    LOG.info("Synthetic source data generation completed for GISMO pipeline.")


if __name__ == "__main__":
    main()
