import argparse
import os
from collections.abc import Iterable, Sequence

import psycopg
from common.gismo import (
    DEFAULT_GISMO_CATALOG,
    DEFAULT_GISMO_SCHEMA,
    DEFAULT_LAKEBASE_SCHEMA,
)
from databricks.sdk import WorkspaceClient
from dbx_tools import clients
from lfp_logging import logs
from psycopg import sql as psql

from pipeline.constants import GOLD_TABLES

# Synchronizes Unity Catalog gold tables into a Lakebase Postgres serving schema.

LOG = logs.logger()

TABLE_KEY_COLUMNS: dict[str, tuple[str, ...]] = {
    "gold_inventory_visibility": ("market_key", "terminal_id", "product_id"),
    "gold_planner_explainability": ("market_key", "terminal_id", "product_id"),
    "gold_demand_forecast": ("market_key", "product_id"),
    "gold_dispatch_exposure": ("route_id", "market_key", "terminal_id"),
    "gold_sales_context": ("market_key", "product_id"),
    "gold_ai_query_decisions": ("market_key", "terminal_id", "product_id"),
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync GISMO gold tables into Lakebase serving schema."
    )
    parser.add_argument(
        "--catalog", default=os.getenv("GISMO_CATALOG", DEFAULT_GISMO_CATALOG)
    )
    parser.add_argument(
        "--schema", default=os.getenv("GISMO_SCHEMA", DEFAULT_GISMO_SCHEMA)
    )
    parser.add_argument(
        "--lakebase-schema",
        default=os.getenv("LAKEBASE_SCHEMA", DEFAULT_LAKEBASE_SCHEMA),
    )
    parser.add_argument("--lakebase-dsn", default=os.getenv("LAKEBASE_DSN"))
    return parser.parse_args()


def _map_type(type_code: object) -> str:
    type_name = str(type_code).upper()
    if "INT" in type_name:
        return "BIGINT"
    if "DOUBLE" in type_name or "FLOAT" in type_name or "DECIMAL" in type_name:
        return "DOUBLE PRECISION"
    if "BOOL" in type_name:
        return "BOOLEAN"
    if "TIMESTAMP" in type_name:
        return "TIMESTAMPTZ"
    if "DATE" in type_name:
        return "DATE"
    return "TEXT"


def _statement_ok(workspace_client: WorkspaceClient, warehouse_id: str, statement: str):
    result = workspace_client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s",
    )
    state = (
        result.status.state.value
        if result.status and result.status.state
        else "UNKNOWN"
    )
    if state != "SUCCEEDED":
        error_message = (
            result.status.error.message
            if result.status and result.status.error
            else "Unknown SQL error."
        )
        raise RuntimeError(
            f"Lakebase sync SQL failed with state {state}: {error_message}"
        )
    return result


def _table_schema(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    table_name: str,
) -> tuple[list[str], list[str]]:
    describe_result = _statement_ok(
        workspace_client=workspace_client,
        warehouse_id=warehouse_id,
        statement=f"DESCRIBE TABLE `{catalog}`.`{schema}`.`{table_name}`",
    )
    rows = describe_result.result.data_array if describe_result.result else []
    column_names: list[str] = []
    column_types: list[str] = []
    for row in rows or []:
        if not row:
            continue
        raw_column_name = str(row[0]).strip()
        if not raw_column_name or raw_column_name.startswith("#"):
            continue
        column_names.append(raw_column_name)
        column_types.append(str(row[1]))
    return column_names, column_types


def _table_rows(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    table_name: str,
) -> list[tuple[object, ...]]:
    query_result = _statement_ok(
        workspace_client=workspace_client,
        warehouse_id=warehouse_id,
        statement=f"SELECT * FROM `{catalog}`.`{schema}`.`{table_name}`",
    )
    rows = query_result.result.data_array if query_result.result else []
    return [tuple(row) for row in (rows or [])]


def _warehouse_scalar_count(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    statement: str,
) -> int:
    result = _statement_ok(
        workspace_client=workspace_client,
        warehouse_id=warehouse_id,
        statement=statement,
    )
    rows = result.result.data_array if result.result else []
    if not rows or not rows[0]:
        return 0
    return int(rows[0][0])


def _create_target_table(
    pg_conn: psycopg.Connection,
    lakebase_schema: str,
    table_name: str,
    column_names: Sequence[str],
    column_types: Sequence[str],
) -> None:
    identifier_list = [psql.Identifier(lakebase_schema), psql.Identifier(table_name)]
    column_parts: list[psql.Composable] = []
    for column_name, source_type in zip(column_names, column_types, strict=True):
        column_type = _map_type(source_type)
        column_parts.append(
            psql.SQL("{} {}").format(
                psql.Identifier(column_name), psql.SQL(column_type)
            )
        )

    create_sql = psql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
        identifier_list[0],
        identifier_list[1],
        psql.SQL(", ").join(column_parts),
    )
    with pg_conn.cursor() as pg_cursor:
        pg_cursor.execute(
            psql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                psql.Identifier(lakebase_schema)
            )
        )
        pg_cursor.execute(create_sql)
        pg_cursor.execute(
            psql.SQL("TRUNCATE TABLE {}.{}").format(
                identifier_list[0], identifier_list[1]
            ),
        )


def _insert_rows(
    pg_conn: psycopg.Connection,
    lakebase_schema: str,
    table_name: str,
    column_names: Sequence[str],
    rows: Iterable[Sequence[object]],
) -> None:
    column_identifiers = [psql.Identifier(name) for name in column_names]
    placeholders = psql.SQL(", ").join(psql.Placeholder() for _ in column_names)
    insert_sql = psql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
        psql.Identifier(lakebase_schema),
        psql.Identifier(table_name),
        psql.SQL(", ").join(column_identifiers),
        placeholders,
    )
    with pg_conn.cursor() as pg_cursor:
        pg_cursor.executemany(insert_sql, rows)


def _warehouse_row_count(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    table_name: str,
) -> int:
    fully_qualified_name = f"`{catalog}`.`{schema}`.`{table_name}`"
    return _warehouse_scalar_count(
        workspace_client=workspace_client,
        warehouse_id=warehouse_id,
        statement=f"SELECT COUNT(*) AS row_count FROM {fully_qualified_name}",
    )


def _lakebase_row_count(
    pg_conn: psycopg.Connection, lakebase_schema: str, table_name: str
) -> int:
    with pg_conn.cursor() as pg_cursor:
        pg_cursor.execute(
            psql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                psql.Identifier(lakebase_schema),
                psql.Identifier(table_name),
            ),
        )
        row = pg_cursor.fetchone()
    return int(row[0]) if row else 0


def _warehouse_distinct_key_count(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    table_name: str,
    key_columns: Sequence[str],
) -> int:
    fully_qualified_name = f"`{catalog}`.`{schema}`.`{table_name}`"
    distinct_columns = ", ".join(f"`{column}`" for column in key_columns)
    return _warehouse_scalar_count(
        workspace_client=workspace_client,
        warehouse_id=warehouse_id,
        statement=f"SELECT COUNT(*) FROM (SELECT DISTINCT {distinct_columns} FROM {fully_qualified_name}) key_rows",
    )


def _lakebase_distinct_key_count(
    pg_conn: psycopg.Connection,
    lakebase_schema: str,
    table_name: str,
    key_columns: Sequence[str],
) -> int:
    distinct_columns = psql.SQL(", ").join(
        psql.Identifier(column) for column in key_columns
    )
    query = psql.SQL(
        "SELECT COUNT(*) FROM (SELECT DISTINCT {} FROM {}.{}) key_rows"
    ).format(
        distinct_columns,
        psql.Identifier(lakebase_schema),
        psql.Identifier(table_name),
    )
    with pg_conn.cursor() as pg_cursor:
        pg_cursor.execute(query)
        row = pg_cursor.fetchone()
    return int(row[0]) if row else 0


def _sync_table(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    pg_conn: psycopg.Connection,
    catalog: str,
    schema: str,
    lakebase_schema: str,
    table_name: str,
) -> None:
    column_names, column_types = _table_schema(
        workspace_client=workspace_client,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        table_name=table_name,
    )
    rows = _table_rows(
        workspace_client=workspace_client,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        table_name=table_name,
    )
    if not column_names:
        LOG.warning("Skipping Lakebase sync for table with no description metadata.")
        return

    _create_target_table(
        pg_conn=pg_conn,
        lakebase_schema=lakebase_schema,
        table_name=table_name,
        column_names=column_names,
        column_types=column_types,
    )
    _insert_rows(
        pg_conn=pg_conn,
        lakebase_schema=lakebase_schema,
        table_name=table_name,
        column_names=column_names,
        rows=rows,
    )
    pg_conn.commit()
    warehouse_count = _warehouse_row_count(
        workspace_client=workspace_client,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        table_name=table_name,
    )
    lakebase_count = _lakebase_row_count(
        pg_conn=pg_conn,
        lakebase_schema=lakebase_schema,
        table_name=table_name,
    )
    if warehouse_count != lakebase_count:
        raise RuntimeError(
            f"Lakebase validation failed for {table_name}. "
            f"Warehouse count: {warehouse_count}, Lakebase count: {lakebase_count}."
        )
    key_columns = TABLE_KEY_COLUMNS.get(table_name)
    if key_columns:
        warehouse_distinct_count = _warehouse_distinct_key_count(
            workspace_client=workspace_client,
            warehouse_id=warehouse_id,
            catalog=catalog,
            schema=schema,
            table_name=table_name,
            key_columns=key_columns,
        )
        lakebase_distinct_count = _lakebase_distinct_key_count(
            pg_conn=pg_conn,
            lakebase_schema=lakebase_schema,
            table_name=table_name,
            key_columns=key_columns,
        )
        if warehouse_distinct_count != lakebase_distinct_count:
            raise RuntimeError(
                f"Lakebase key validation failed for {table_name}. "
                f"Warehouse distinct keys: {warehouse_distinct_count}, "
                f"Lakebase distinct keys: {lakebase_distinct_count}."
            )
    LOG.info("Synced Lakebase serving table.")


def main() -> None:
    args = _parse_args()
    if not args.lakebase_dsn:
        raise ValueError(
            "Missing required arguments or environment variables: lakebase-dsn"
        )
    workspace_client = WorkspaceClient()
    warehouse_id = str(clients.warehouse(workspace_client).id)
    with psycopg.connect(args.lakebase_dsn) as pg_conn:
        for table_name in GOLD_TABLES:
            _sync_table(
                workspace_client=workspace_client,
                warehouse_id=warehouse_id,
                pg_conn=pg_conn,
                catalog=args.catalog,
                schema=args.schema,
                lakebase_schema=args.lakebase_schema,
                table_name=table_name,
            )
    LOG.info("Completed Lakebase synchronization for all gold tables.")


if __name__ == "__main__":
    main()
