import argparse
import os
from collections.abc import Iterable, Sequence
from contextlib import closing

import psycopg
from common.gismo import (
    DEFAULT_GISMO_CATALOG,
    DEFAULT_GISMO_SCHEMA,
    DEFAULT_LAKEBASE_SCHEMA,
)
from databricks import sql as dbsql
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
    parser.add_argument(
        "--warehouse-host", default=os.getenv("DATABRICKS_SERVER_HOSTNAME")
    )
    parser.add_argument(
        "--warehouse-http-path", default=os.getenv("DATABRICKS_HTTP_PATH")
    )
    parser.add_argument("--warehouse-token", default=os.getenv("DATABRICKS_TOKEN"))
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


def _create_target_table(
    pg_conn: psycopg.Connection,
    lakebase_schema: str,
    table_name: str,
    cursor_description: Sequence[tuple[object, ...]],
) -> None:
    identifier_list = [psql.Identifier(lakebase_schema), psql.Identifier(table_name)]
    column_parts: list[psql.Composable] = []
    for column in cursor_description:
        column_name = str(column[0])
        column_type = _map_type(column[1])
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
    warehouse_cursor: dbsql.client.Cursor, catalog: str, schema: str, table_name: str
) -> int:
    fully_qualified_name = f"`{catalog}`.`{schema}`.`{table_name}`"
    warehouse_cursor.execute(
        f"SELECT COUNT(*) AS row_count FROM {fully_qualified_name}"
    )
    row = warehouse_cursor.fetchone()
    return int(row[0]) if row else 0


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
    warehouse_cursor: dbsql.client.Cursor,
    catalog: str,
    schema: str,
    table_name: str,
    key_columns: Sequence[str],
) -> int:
    fully_qualified_name = f"`{catalog}`.`{schema}`.`{table_name}`"
    distinct_columns = ", ".join(f"`{column}`" for column in key_columns)
    warehouse_cursor.execute(
        f"SELECT COUNT(*) FROM (SELECT DISTINCT {distinct_columns} FROM {fully_qualified_name}) key_rows",
    )
    row = warehouse_cursor.fetchone()
    return int(row[0]) if row else 0


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
    warehouse_cursor: dbsql.client.Cursor,
    pg_conn: psycopg.Connection,
    catalog: str,
    schema: str,
    lakebase_schema: str,
    table_name: str,
) -> None:
    fully_qualified_name = f"`{catalog}`.`{schema}`.`{table_name}`"
    warehouse_cursor.execute(f"SELECT * FROM {fully_qualified_name}")
    rows = warehouse_cursor.fetchall()
    description = warehouse_cursor.description or []
    if not description:
        LOG.warning("Skipping Lakebase sync for table with no description metadata.")
        return

    _create_target_table(
        pg_conn=pg_conn,
        lakebase_schema=lakebase_schema,
        table_name=table_name,
        cursor_description=description,
    )
    column_names = [str(column[0]) for column in description]
    _insert_rows(
        pg_conn=pg_conn,
        lakebase_schema=lakebase_schema,
        table_name=table_name,
        column_names=column_names,
        rows=rows,
    )
    pg_conn.commit()
    warehouse_count = _warehouse_row_count(
        warehouse_cursor=warehouse_cursor,
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
            warehouse_cursor=warehouse_cursor,
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
    missing = [
        name
        for name, value in (
            ("lakebase-dsn", args.lakebase_dsn),
            ("warehouse-host", args.warehouse_host),
            ("warehouse-http-path", args.warehouse_http_path),
            ("warehouse-token", args.warehouse_token),
        )
        if not value
    ]
    if missing:
        raise ValueError(
            f"Missing required arguments or environment variables: {', '.join(missing)}"
        )

    with closing(
        dbsql.connect(
            server_hostname=args.warehouse_host,
            http_path=args.warehouse_http_path,
            access_token=args.warehouse_token,
        )
    ) as warehouse_conn:
        with closing(warehouse_conn.cursor()) as warehouse_cursor:
            with psycopg.connect(args.lakebase_dsn) as pg_conn:
                for table_name in GOLD_TABLES:
                    _sync_table(
                        warehouse_cursor=warehouse_cursor,
                        pg_conn=pg_conn,
                        catalog=args.catalog,
                        schema=args.schema,
                        lakebase_schema=args.lakebase_schema,
                        table_name=table_name,
                    )
    LOG.info("Completed Lakebase synchronization for all gold tables.")


if __name__ == "__main__":
    main()
