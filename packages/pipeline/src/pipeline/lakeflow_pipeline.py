import dlt
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# GISMO Lakeflow Declarative Pipeline for mocked source ingestion and medallion tables.


def _source_table_name() -> str:
    catalog = spark.conf.get("gismo.catalog", "reggie_pierce")
    schema = spark.conf.get("gismo.schema", "racetrac_gismo")
    return f"`{catalog}`.`{schema}`.`source_operational_signals`"


@dlt.table(
    name="bronze_operational_signals",
    comment=(
        "Bronze layer mock operational signals from adjacent systems: Salesforce, "
        "PDI/RightAngle, MarketView, Gurobi, terminals, MDM, PC Miler, Veeder Root/Sticks, "
        "payroll, DTN/ADS, weather, and Samsara."
    ),
)
@dlt.expect_or_drop("valid_source_system", "source_system IS NOT NULL")
def bronze_operational_signals() -> DataFrame:
    # Treat synthetic source events as an append stream for intraday inventory demos.
    return spark.readStream.option("skipChangeCommits", "true").table(
        _source_table_name()
    )


@dlt.table(
    name="silver_inventory_state",
    comment=(
        "Silver inventory harmonization view reconciling signals used by planners, terminals, "
        "and retail operators."
    ),
)
def silver_inventory_state() -> DataFrame:
    bronze_df = dlt.read("bronze_operational_signals")
    return (
        bronze_df.groupBy("market_key", "terminal_id", "product_id")
        .agg(
            F.max("signal_timestamp").alias("as_of_ts"),
            F.avg("inventory_units").alias("inventory_units_avg"),
            F.max("inventory_units").alias("inventory_units_peak"),
            F.sum(F.when(F.col("quality_flag") == "warning", 1).otherwise(0)).alias(
                "warning_signal_count"
            ),
            F.avg("demand_units").alias("demand_units_avg"),
            F.avg("retail_price").alias("retail_price_avg"),
            F.avg("wholesale_cost").alias("wholesale_cost_avg"),
            F.avg("weather_index").alias("weather_index_avg"),
        )
        .withColumn(
            "inventory_gap_units",
            F.col("inventory_units_peak") - F.col("inventory_units_avg"),
        )
    )


@dlt.table(
    name="silver_demand_drivers",
    comment=(
        "Silver demand driver table combining pricing and weather context for explainable "
        "demand forecast features."
    ),
)
def silver_demand_drivers() -> DataFrame:
    bronze_df = dlt.read("bronze_operational_signals")
    return (
        bronze_df.groupBy("market_key", "product_id")
        .agg(
            F.avg("demand_units").alias("baseline_demand_units"),
            F.avg("weather_index").alias("weather_index_avg"),
            F.avg("retail_price").alias("retail_price_avg"),
            F.avg("wholesale_cost").alias("wholesale_cost_avg"),
            F.sum(F.when(F.col("planner_override_flag"), 1).otherwise(0)).alias(
                "planner_override_count"
            ),
        )
        .withColumn(
            "price_spread",
            F.round(
                F.col("retail_price_avg")
                - (F.col("wholesale_cost_avg") / F.lit(100.0)),
                3,
            ),
        )
    )


@dlt.table(
    name="silver_dispatch_context",
    comment=(
        "Silver dispatch context for freight exposure analysis and route-level planner "
        "decision support."
    ),
)
def silver_dispatch_context() -> DataFrame:
    bronze_df = dlt.read("bronze_operational_signals")
    return (
        bronze_df.groupBy("route_id", "market_key", "terminal_id")
        .agg(
            F.avg("freight_cost").alias("freight_cost_avg"),
            F.max("freight_cost").alias("freight_cost_peak"),
            F.avg("telematics_risk").alias("telematics_risk_avg"),
            F.countDistinct("retail_location_id").alias("served_location_count"),
        )
        .withColumn(
            "route_cost_exposure",
            F.col("freight_cost_peak") * F.col("served_location_count"),
        )
    )


@dlt.table(
    name="gold_inventory_visibility",
    comment=(
        "Gold app-facing table answering current inventory and at-risk inventory by market, "
        "terminal, and product."
    ),
)
def gold_inventory_visibility() -> DataFrame:
    inventory_df = dlt.read("silver_inventory_state")
    return inventory_df.withColumn(
        "inventory_health",
        F.when(F.col("inventory_units_avg") < 500, F.lit("at_risk"))
        .when(F.col("inventory_units_avg") < 700, F.lit("watch"))
        .otherwise(F.lit("healthy")),
    ).withColumn(
        "inventory_utilization_ratio",
        F.round(F.col("demand_units_avg") / F.col("inventory_units_avg"), 4),
    )


@dlt.table(
    name="gold_planner_explainability",
    comment=(
        "Gold explainability table providing transparent planner signals including inventory "
        "gaps, weather effects, and pricing dynamics."
    ),
)
def gold_planner_explainability() -> DataFrame:
    inventory_df = dlt.read("silver_inventory_state")
    demand_df = dlt.read("silver_demand_drivers")
    inventory_alias = inventory_df.alias("inventory")
    demand_alias = demand_df.alias("demand")
    return (
        inventory_alias.join(
            demand_alias,
            on=["market_key", "product_id"],
            how="inner",
        )
        .select(
            "market_key",
            "product_id",
            "terminal_id",
            "as_of_ts",
            "inventory_units_avg",
            "inventory_units_peak",
            "warning_signal_count",
            "demand_units_avg",
            F.col("inventory.retail_price_avg").alias("retail_price_avg"),
            F.col("inventory.wholesale_cost_avg").alias("wholesale_cost_avg"),
            "inventory_gap_units",
            "baseline_demand_units",
            F.col("demand.weather_index_avg").alias("weather_index_avg"),
            "price_spread",
            "planner_override_count",
        )
        .withColumn(
            "inventory_pressure_score",
            F.round(F.col("inventory_gap_units") / F.col("inventory_units_avg"), 4),
        )
        .withColumn("weather_pressure_score", F.round(F.col("weather_index_avg"), 4))
        .withColumn(
            "planner_reason_code",
            F.when(
                F.col("inventory_pressure_score") > 0.2, F.lit("inventory_variability")
            )
            .when(F.col("weather_pressure_score") > 0.6, F.lit("weather_shift"))
            .otherwise(F.lit("stable_signal")),
        )
    )


@dlt.table(
    name="gold_demand_forecast",
    comment=(
        "Gold AI forecast table that projects demand using weather, pricing, and baseline "
        "demand signals with confidence bounds."
    ),
)
def gold_demand_forecast() -> DataFrame:
    demand_df = dlt.read("silver_demand_drivers")
    return (
        demand_df.withColumn(
            "ai_forecast_units",
            F.round(
                F.col("baseline_demand_units")
                * (F.lit(1.0) + (F.col("weather_index_avg") * F.lit(0.35)))
                * (F.lit(1.0) + (F.col("price_spread") * F.lit(0.08))),
                2,
            ),
        )
        .withColumn(
            "forecast_confidence",
            F.round(F.lit(0.88) - (F.col("planner_override_count") * F.lit(0.002)), 3),
        )
        .withColumn(
            "forecast_lower_bound", F.round(F.col("ai_forecast_units") * F.lit(0.92), 2)
        )
        .withColumn(
            "forecast_upper_bound", F.round(F.col("ai_forecast_units") * F.lit(1.08), 2)
        )
        .withColumn("forecast_horizon_days", F.lit(7))
    )


@dlt.table(
    name="gold_dispatch_exposure",
    comment=(
        "Gold dispatch table for freight exposure and route risk visibility used by dispatch "
        "and planner personas."
    ),
)
def gold_dispatch_exposure() -> DataFrame:
    dispatch_df = dlt.read("silver_dispatch_context")
    return dispatch_df.withColumn(
        "dispatch_risk_level",
        F.when(
            (F.col("route_cost_exposure") > 1400)
            | (F.col("telematics_risk_avg") > 0.7),
            F.lit("high"),
        )
        .when(
            (F.col("route_cost_exposure") > 900)
            | (F.col("telematics_risk_avg") > 0.45),
            F.lit("medium"),
        )
        .otherwise(F.lit("low")),
    )


@dlt.table(
    name="gold_sales_context",
    comment=(
        "Gold sales context table linking market and product demand forecast indicators with "
        "pricing spread and forecast confidence."
    ),
)
def gold_sales_context() -> DataFrame:
    forecast_df = dlt.read("gold_demand_forecast")
    return forecast_df.select(
        "market_key",
        "product_id",
        "ai_forecast_units",
        "forecast_confidence",
        "forecast_horizon_days",
        "price_spread",
        "weather_index_avg",
    ).withColumn(
        "sales_priority_band",
        F.when(F.col("ai_forecast_units") > 220, F.lit("growth_opportunity"))
        .when(F.col("ai_forecast_units") > 170, F.lit("steady_state"))
        .otherwise(F.lit("demand_watch")),
    )


@dlt.table(
    name="gold_ai_query_decisions",
    comment=(
        "Gold AI query decision table showing criteria-based planner and dispatch actions with "
        "explicit reason codes for explainability."
    ),
)
def gold_ai_query_decisions() -> DataFrame:
    planner_df = dlt.read("gold_planner_explainability")
    dispatch_df = dlt.read("gold_dispatch_exposure")
    forecast_df = dlt.read("gold_demand_forecast")

    decisions_df = (
        planner_df.join(
            forecast_df.select(
                "market_key", "product_id", "ai_forecast_units", "forecast_confidence"
            ),
            on=["market_key", "product_id"],
            how="inner",
        )
        .join(
            dispatch_df.select(
                "market_key", "route_cost_exposure", "dispatch_risk_level"
            ),
            on="market_key",
            how="left",
        )
        .withColumn(
            "route_cost_exposure", F.coalesce(F.col("route_cost_exposure"), F.lit(0.0))
        )
        .withColumn(
            "dispatch_risk_level",
            F.coalesce(F.col("dispatch_risk_level"), F.lit("low")),
        )
        .withColumn("inventory_risk_flag", F.col("inventory_pressure_score") > 0.2)
        .withColumn("demand_surge_flag", F.col("ai_forecast_units") > 220)
        .withColumn("freight_risk_flag", F.col("route_cost_exposure") > 1100)
        .withColumn(
            "ai_query_action",
            F.when(
                F.col("inventory_risk_flag") & F.col("demand_surge_flag"),
                F.lit("increase_terminal_replenishment"),
            )
            .when(F.col("freight_risk_flag"), F.lit("reroute_dispatch"))
            .when(
                F.col("dispatch_risk_level") == "high",
                F.lit("escalate_dispatch_review"),
            )
            .otherwise(F.lit("maintain_current_plan")),
        )
        .withColumn(
            "ai_query_reason_code",
            F.when(
                F.col("inventory_risk_flag") & F.col("demand_surge_flag"),
                F.lit("inventory_gap_with_demand_surge"),
            )
            .when(F.col("freight_risk_flag"), F.lit("route_cost_threshold"))
            .when(
                F.col("dispatch_risk_level") == "high",
                F.lit("telematics_and_dispatch_risk"),
            )
            .otherwise(F.lit("criteria_within_tolerance")),
        )
        .withColumn("decision_timestamp", F.current_timestamp())
    )
    return decisions_df
