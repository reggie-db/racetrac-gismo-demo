from common.gismo import DEFAULT_GISMO_CATALOG, DEFAULT_GISMO_SCHEMA

# SQL query definitions used by the GISMO Lakeview dashboard.

INVENTORY_HEALTH_QUERY = f"""
SELECT
  market_key,
  terminal_id,
  product_id,
  inventory_health,
  inventory_units_avg,
  demand_units_avg,
  inventory_utilization_ratio
FROM {DEFAULT_GISMO_CATALOG}.{DEFAULT_GISMO_SCHEMA}.gold_inventory_visibility
ORDER BY inventory_utilization_ratio DESC
LIMIT 200
"""

FORECAST_TREND_QUERY = f"""
SELECT
  market_key,
  product_id,
  ai_forecast_units,
  forecast_confidence,
  forecast_lower_bound,
  forecast_upper_bound
FROM {DEFAULT_GISMO_CATALOG}.{DEFAULT_GISMO_SCHEMA}.gold_demand_forecast
ORDER BY ai_forecast_units DESC
LIMIT 200
"""

AI_DECISIONS_QUERY = f"""
SELECT
  ai_query_action,
  ai_query_reason_code,
  COUNT(*) AS decision_count
FROM {DEFAULT_GISMO_CATALOG}.{DEFAULT_GISMO_SCHEMA}.gold_ai_query_decisions
GROUP BY ai_query_action, ai_query_reason_code
ORDER BY decision_count DESC
"""

DISPATCH_RISK_QUERY = f"""
SELECT
  market_key,
  route_id,
  dispatch_risk_level,
  route_cost_exposure,
  freight_cost_avg,
  telematics_risk_avg
FROM {DEFAULT_GISMO_CATALOG}.{DEFAULT_GISMO_SCHEMA}.gold_dispatch_exposure
ORDER BY route_cost_exposure DESC
LIMIT 200
"""
