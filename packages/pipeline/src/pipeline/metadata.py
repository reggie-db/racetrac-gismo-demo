import argparse
import os
from collections.abc import Iterable, Sequence

from common.gismo import DEFAULT_GISMO_CATALOG, DEFAULT_GISMO_SCHEMA
from databricks.sdk import WorkspaceClient
from dbx_tools import clients
from lfp_logging import logs

from pipeline.constants import GOLD_TABLES, SILVER_TABLES

# Metadata tooling for adding table and column descriptions for Genie readiness.

LOG = logs.logger()

TABLE_DESCRIPTIONS: dict[str, str] = {
    "bronze_operational_signals": (
        "Bronze mock operational signals from adjacent systems in the GISMO architecture "
        "including Salesforce, PDI/RightAngle, terminal feeds, weather, and logistics data."
    ),
    "silver_inventory_state": "Silver harmonized inventory posture by market, terminal, and product.",
    "silver_demand_drivers": "Silver demand driver features for forecasting and planner explainability.",
    "silver_dispatch_context": "Silver route and freight context for dispatch exposure and risk analytics.",
    "gold_inventory_visibility": "Gold current inventory transparency table for GISMO UI and operators.",
    "gold_planner_explainability": "Gold explainability facts for planner decisions and blend rationale.",
    "gold_demand_forecast": "Gold ai_forecast output with confidence intervals for next 7-day planning.",
    "gold_dispatch_exposure": "Gold freight and dispatch risk exposure table for route decisioning.",
    "gold_sales_context": "Gold sales and pricing context table for commercial planning workflows.",
    "gold_ai_query_decisions": (
        "Gold ai_query decision outcomes with criteria flags, actions, and reason codes for "
        "transparent operational automation."
    ),
}

COLUMN_DESCRIPTIONS: dict[str, dict[str, str]] = {
    "bronze_operational_signals": {
        "id": "Synthetic event sequence identifier for deterministic mock data generation.",
        "source_system": "Source system name from the left-column operational ecosystem.",
        "signal_timestamp": "Event timestamp of the incoming operational signal.",
        "market_key": "Market identifier used to group regional planning decisions.",
        "terminal_id": "Terminal identifier representing supply origin or storage location.",
        "product_id": "Fuel product identifier.",
        "route_id": "Dispatch route identifier.",
        "retail_location_id": "Retail location receiving inventory.",
        "inventory_units": "Observed inventory units for the event context.",
        "demand_units": "Observed demand units associated with the event.",
        "wholesale_cost": "Wholesale supply cost signal in cents-equivalent units.",
        "retail_price": "Retail market price signal in currency units.",
        "weather_index": "Normalized weather pressure index affecting demand.",
        "freight_cost": "Freight cost signal for route movement.",
        "telematics_risk": "Normalized telematics and route risk indicator.",
        "planner_override_flag": "Flag indicating planner override behavior in source systems.",
        "source_record_id": "Unique mocked source record identifier.",
        "ingest_batch_id": "Ingestion batch identifier for traceability.",
        "quality_flag": "Data quality indicator for ingestion diagnostics.",
        "record_created_ts": "Pipeline record creation timestamp.",
    },
    "silver_inventory_state": {
        "market_key": "Market identifier used for demand and inventory rollups.",
        "terminal_id": "Terminal identifier used in inventory visibility.",
        "product_id": "Fuel product identifier.",
        "as_of_ts": "Latest timestamp represented by the inventory snapshot.",
        "inventory_units_avg": "Average observed inventory units in the aggregation window.",
        "inventory_units_peak": "Peak observed inventory units in the aggregation window.",
        "warning_signal_count": "Count of source warnings included in the aggregation window.",
        "demand_units_avg": "Average demand units in the same aggregation window.",
        "retail_price_avg": "Average retail price in the aggregation window.",
        "wholesale_cost_avg": "Average wholesale cost in the aggregation window.",
        "weather_index_avg": "Average weather pressure index in the aggregation window.",
        "inventory_gap_units": "Difference between peak and average inventory units.",
    },
    "silver_demand_drivers": {
        "market_key": "Market identifier used in forecast feature generation.",
        "product_id": "Fuel product identifier.",
        "baseline_demand_units": "Average demand baseline computed from source events.",
        "weather_index_avg": "Average weather signal for the forecast feature window.",
        "retail_price_avg": "Average retail price for forecast feature engineering.",
        "wholesale_cost_avg": "Average wholesale cost for spread feature engineering.",
        "planner_override_count": "Count of planner overrides as forecast uncertainty signal.",
        "price_spread": "Retail minus wholesale price spread feature.",
    },
    "silver_dispatch_context": {
        "route_id": "Route identifier used for dispatch planning.",
        "market_key": "Market identifier for dispatch workload grouping.",
        "terminal_id": "Terminal identifier serving the route.",
        "freight_cost_avg": "Average freight cost on the route.",
        "freight_cost_peak": "Peak freight cost on the route.",
        "telematics_risk_avg": "Average telematics-derived risk score for the route.",
        "served_location_count": "Number of retail locations served by the route.",
        "route_cost_exposure": "Computed route-level freight cost exposure.",
    },
    "gold_inventory_visibility": {
        "market_key": "Market identifier used for inventory visibility drill-down.",
        "terminal_id": "Terminal identifier used for supply-side operational visibility.",
        "product_id": "Fuel product identifier.",
        "as_of_ts": "Latest timestamp represented by the inventory state.",
        "inventory_units_avg": "Average observed inventory units in the aggregation window.",
        "inventory_units_peak": "Peak observed inventory units in the aggregation window.",
        "warning_signal_count": "Count of warning signals affecting inventory confidence.",
        "demand_units_avg": "Average demand units observed for the same context.",
        "retail_price_avg": "Average retail price for the market and product.",
        "wholesale_cost_avg": "Average wholesale cost for the market and product.",
        "weather_index_avg": "Average weather index affecting the market and product.",
        "inventory_gap_units": "Difference between peak and average inventory units.",
        "inventory_health": "Inventory health status used by operator and planner workflows.",
        "inventory_utilization_ratio": "Ratio of demand to average inventory levels.",
    },
    "gold_planner_explainability": {
        "market_key": "Market identifier used for planner explainability analysis.",
        "product_id": "Fuel product identifier.",
        "terminal_id": "Terminal identifier tied to planner context.",
        "as_of_ts": "Latest signal time used for planner explainability.",
        "inventory_units_avg": "Average inventory level used in planner context.",
        "inventory_units_peak": "Peak inventory level used in planner context.",
        "warning_signal_count": "Warning signal count influencing confidence in planner context.",
        "demand_units_avg": "Average demand units observed in planner context.",
        "retail_price_avg": "Average retail price associated with planner context.",
        "wholesale_cost_avg": "Average wholesale cost associated with planner context.",
        "weather_index_avg": "Average weather signal in planner context.",
        "inventory_gap_units": "Difference between peak and average inventory values.",
        "baseline_demand_units": "Baseline demand feature contributing to explainability context.",
        "planner_override_count": "Override count used as a model uncertainty input.",
        "price_spread": "Price spread feature used in planner rationale.",
        "inventory_pressure_score": "Relative inventory pressure used in planner rationale.",
        "weather_pressure_score": "Weather-driven pressure component for planning decisions.",
        "planner_reason_code": "Explainability reason code attached to planner guidance.",
    },
    "gold_demand_forecast": {
        "market_key": "Market identifier used in the forecast output.",
        "product_id": "Fuel product identifier used in the forecast output.",
        "baseline_demand_units": "Baseline demand feature before AI uplift factors are applied.",
        "weather_index_avg": "Average weather feature used for the forecast.",
        "retail_price_avg": "Average retail price feature used for the forecast.",
        "wholesale_cost_avg": "Average wholesale cost feature used for the forecast.",
        "planner_override_count": "Override count used as a forecast uncertainty indicator.",
        "price_spread": "Retail minus wholesale spread used in forecast scaling.",
        "ai_forecast_units": "AI generated demand forecast for next planning horizon.",
        "forecast_confidence": "Forecast confidence score after override and volatility penalties.",
        "forecast_lower_bound": "Lower confidence bound for forecast units.",
        "forecast_upper_bound": "Upper confidence bound for forecast units.",
        "forecast_horizon_days": "Horizon length represented by forecast output.",
    },
    "gold_dispatch_exposure": {
        "route_id": "Dispatch route identifier used for route-level exposure analysis.",
        "market_key": "Market identifier used for dispatch grouping.",
        "terminal_id": "Terminal identifier serving the route.",
        "freight_cost_avg": "Average freight cost on the route.",
        "freight_cost_peak": "Peak freight cost observed on the route.",
        "telematics_risk_avg": "Average telematics risk indicator for the route.",
        "served_location_count": "Count of retail locations served by the route.",
        "route_cost_exposure": "Route-level exposure combining cost intensity and served stores.",
        "dispatch_risk_level": "Dispatch risk class based on route cost and telematics exposure.",
    },
    "gold_sales_context": {
        "market_key": "Market identifier for commercial planning context.",
        "product_id": "Fuel product identifier for commercial planning context.",
        "ai_forecast_units": "Forecast demand units used for sales prioritization.",
        "forecast_confidence": "Confidence score used to weight commercial recommendations.",
        "forecast_horizon_days": "Forecast horizon used for commercial context.",
        "price_spread": "Retail and wholesale spread used for margin context.",
        "weather_index_avg": "Weather indicator used for demand and sales context.",
        "sales_priority_band": "Sales prioritization classification for demand opportunity.",
    },
    "gold_ai_query_decisions": {
        "market_key": "Market identifier where ai_query decision criteria are evaluated.",
        "product_id": "Product identifier where ai_query decision criteria are evaluated.",
        "terminal_id": "Terminal identifier associated with ai_query planner context.",
        "as_of_ts": "Planner context timestamp used at ai_query decision time.",
        "inventory_units_avg": "Average inventory level used for ai_query criteria.",
        "inventory_units_peak": "Peak inventory level used for ai_query criteria.",
        "warning_signal_count": "Warning count used for ai_query confidence checks.",
        "demand_units_avg": "Average demand level used in ai_query criteria.",
        "retail_price_avg": "Retail price feature used in ai_query logic.",
        "wholesale_cost_avg": "Wholesale cost feature used in ai_query logic.",
        "weather_index_avg": "Weather feature used in ai_query logic.",
        "inventory_gap_units": "Inventory gap feature used in ai_query logic.",
        "baseline_demand_units": "Baseline demand used in ai_query context.",
        "planner_override_count": "Override count carried into ai_query context.",
        "price_spread": "Price spread used in ai_query criteria.",
        "inventory_pressure_score": "Inventory pressure feature used in decision rules.",
        "weather_pressure_score": "Weather pressure feature used in decision rules.",
        "planner_reason_code": "Planner reason code propagated for explainability.",
        "ai_forecast_units": "Forecast units used in ai_query decision thresholds.",
        "forecast_confidence": "Forecast confidence used in ai_query decision checks.",
        "route_cost_exposure": "Route exposure feature used in ai_query decision thresholds.",
        "dispatch_risk_level": "Dispatch risk class used in ai_query decision rules.",
        "inventory_risk_flag": "Criterion indicating elevated inventory pressure.",
        "demand_surge_flag": "Criterion indicating a material demand increase signal.",
        "freight_risk_flag": "Criterion indicating freight cost exposure beyond threshold.",
        "ai_query_action": "Recommended decision action selected by ai_query criteria.",
        "ai_query_reason_code": "Explainable reason code corresponding to decision action.",
        "decision_timestamp": "Timestamp when the decision record was generated.",
    },
}


def _all_table_names() -> Sequence[str]:
    # Streaming tables do not support COMMENT ON operations in SQL warehouses.
    # We apply metadata to silver and gold tables that power semantic querying.
    return (*SILVER_TABLES, *GOLD_TABLES)


def _escape_sql_comment(comment_text: str) -> str:
    return comment_text.replace("'", "''")


def _build_comment_sql(catalog: str, schema: str) -> list[str]:
    statements: list[str] = []
    for table_name in _all_table_names():
        table_description = TABLE_DESCRIPTIONS[table_name]
        full_name = f"`{catalog}`.`{schema}`.`{table_name}`"
        statements.append(
            f"COMMENT ON TABLE {full_name} IS '{_escape_sql_comment(table_description)}'"
        )
        for column_name, column_description in COLUMN_DESCRIPTIONS.get(
            table_name, {}
        ).items():
            statements.append(
                "COMMENT ON COLUMN "
                f"{full_name}.`{column_name}` IS '{_escape_sql_comment(column_description)}'"
            )
    return statements


def _run_sql_statements(
    workspace_client: WorkspaceClient, warehouse_id: str, statements: Iterable[str]
) -> None:
    for statement in statements:
        LOG.info("Executing metadata SQL statement.")
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
                f"Metadata SQL failed with state {state}: {error_message}"
            )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply GISMO table and column metadata comments."
    )
    parser.add_argument(
        "--catalog", default=os.getenv("GISMO_CATALOG", DEFAULT_GISMO_CATALOG)
    )
    parser.add_argument(
        "--schema", default=os.getenv("GISMO_SCHEMA", DEFAULT_GISMO_SCHEMA)
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    workspace_client = WorkspaceClient()
    warehouse_id = str(clients.warehouse(workspace_client).id)
    statements = _build_comment_sql(catalog=args.catalog, schema=args.schema)
    _run_sql_statements(
        workspace_client=workspace_client,
        warehouse_id=warehouse_id,
        statements=statements,
    )
    LOG.info("Applied metadata comments for GISMO tables.")


if __name__ == "__main__":
    main()
