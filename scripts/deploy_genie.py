import argparse
import json
import os
from collections.abc import Sequence

from common.gismo import DEFAULT_GISMO_CATALOG, DEFAULT_GISMO_SCHEMA, GOLD_TABLES
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.platform import PermissionDenied
from dbx_tools import clients
from lfp_logging import logs

# Creates or updates a GISMO Genie space for curated gold tables.

LOG = logs.logger()

GENIE_DISPLAY_NAME = "GISMO Fuel Operations"
GENIE_DESCRIPTION = (
    "Explore RaceTrac GISMO operational intelligence tables, including inventory visibility, "
    "planner explainability, demand forecast outputs, dispatch exposure, and AI decisions."
)
GENIE_SAMPLE_QUESTIONS: Sequence[str] = (
    "How much inventory do we have right now by market and terminal?",
    "Why was a planner decision made for a given market and product?",
    "Which markets have the highest 7-day forecasted demand?",
    "Which routes have the highest dispatch exposure risk?",
    "What decisions did ai_query make and why?",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create or update the GISMO Genie space."
    )
    parser.add_argument(
        "--catalog", default=os.getenv("GISMO_CATALOG", DEFAULT_GISMO_CATALOG)
    )
    parser.add_argument(
        "--schema", default=os.getenv("GISMO_SCHEMA", DEFAULT_GISMO_SCHEMA)
    )
    return parser.parse_args()


def _table_identifiers(catalog: str, schema: str) -> list[str]:
    return [f"{catalog}.{schema}.{table_name}" for table_name in GOLD_TABLES]


def _find_existing_space_id(
    workspace_client: WorkspaceClient, display_name: str
) -> str | None:
    response = workspace_client.api_client.do("GET", "/api/2.0/genie/spaces")
    for space in response.get("spaces", []):
        if (
            space.get("display_name") == display_name
            or space.get("title") == display_name
        ):
            return str(space.get("id"))
    return None


def _payload(args: argparse.Namespace) -> dict[str, object]:
    payload: dict[str, object] = {
        "display_name": GENIE_DISPLAY_NAME,
        "description": GENIE_DESCRIPTION,
        "table_identifiers": _table_identifiers(
            catalog=args.catalog, schema=args.schema
        ),
        "sample_questions": list(GENIE_SAMPLE_QUESTIONS),
    }
    return payload


def _create_payload(args: argparse.Namespace, warehouse_id: str) -> dict[str, object]:
    payload = _payload(args)
    payload["warehouse_id"] = warehouse_id
    # Genie create now requires serialized_space; version 2 is the minimum valid payload.
    payload["serialized_space"] = json.dumps({"version": 2})
    return payload


def main() -> None:
    args = _parse_args()
    workspace_client = WorkspaceClient()
    warehouse_id = str(clients.warehouse(workspace_client).id)
    existing_space_id = _find_existing_space_id(
        workspace_client=workspace_client, display_name=GENIE_DISPLAY_NAME
    )
    if existing_space_id:
        payload = _payload(args)
        payload["warehouse_id"] = warehouse_id
        try:
            workspace_client.api_client.do(
                "PATCH",
                f"/api/2.0/genie/spaces/{existing_space_id}",
                body=payload,
            )
            LOG.info("Updated existing GISMO Genie space.")
        except PermissionDenied:
            # If the existing shared space is not editable, create a user-owned demo clone.
            clone_payload = _create_payload(args, warehouse_id)
            clone_payload["display_name"] = f"{GENIE_DISPLAY_NAME} Demo"
            workspace_client.api_client.do(
                "POST",
                "/api/2.0/genie/spaces",
                body=clone_payload,
            )
            LOG.info("Created user-owned GISMO Genie demo space.")
    else:
        workspace_client.api_client.do(
            "POST",
            "/api/2.0/genie/spaces",
            body=_create_payload(args, warehouse_id),
        )
        LOG.info("Created new GISMO Genie space.")


if __name__ == "__main__":
    main()
