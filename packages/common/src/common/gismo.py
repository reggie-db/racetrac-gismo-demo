from collections.abc import Sequence

# Shared GISMO constants and SQL helpers used across packages.

DEFAULT_GISMO_CATALOG = "reggie_pierce"
DEFAULT_GISMO_SCHEMA = "racetrac_gismo"
DEFAULT_LAKEBASE_SCHEMA = "racetrac_gismo_serving"

SOURCE_SYSTEMS: Sequence[str] = (
    "salesforce",
    "pdi_rightangle",
    "marketview",
    "gurobi",
    "terminals",
    "master_data_mgmt",
    "pc_miler",
    "veeder_root_sticks",
    "payroll_workday",
    "dtn_ads",
    "weather_other",
    "samsara",
)

BRONZE_TABLES: Sequence[str] = ("bronze_operational_signals",)

SILVER_TABLES: Sequence[str] = (
    "silver_inventory_state",
    "silver_demand_drivers",
    "silver_dispatch_context",
)

GOLD_TABLES: Sequence[str] = (
    "gold_inventory_visibility",
    "gold_planner_explainability",
    "gold_demand_forecast",
    "gold_dispatch_exposure",
    "gold_sales_context",
    "gold_ai_query_decisions",
)


def source_system_case_sql(index_expression: str = "id % 12") -> str:
    case_lines = [f"WHEN {idx} THEN '{source}'" for idx, source in enumerate(SOURCE_SYSTEMS[:-1])]
    return "\n    ".join(
        [
            f"CASE {index_expression}",
            *case_lines,
            f"    ELSE '{SOURCE_SYSTEMS[-1]}'",
            "  END",
        ]
    )
