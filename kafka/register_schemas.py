"""
register_schemas.py – Register JSON Schemas with Azure Event Hubs Schema Registry
==================================================================================
Each Kafka topic (Event Hub) has a corresponding Pydantic payload model in
schemas.py.  This script derives a JSON Schema from every model and registers
it in the Event Hubs Schema Registry under the configured schema group.

The Schema Registry endpoint is the same FQDN as the Event Hubs namespace
(aaaorgehns.servicebus.windows.net).  Authentication is identical to the
producer: UAMI → ManagedIdentityCredential on Azure, AzureCliCredential locally.

Pre-requisites
--------------
1. A Schema Group must already exist in the namespace:
     Azure Portal → Namespace → Schema Registry → + Schema Group
     Name:    custcomplaints    (or whatever SCHEMA_GROUP is set to in .env)
     Format:  JSON
2. The identity used to run this script must hold the
   "Schema Registry Contributor" role on the namespace (or schema group).
   Grant it in the Portal:
     Namespace → Access control (IAM) → + Add role assignment
     Role: Schema Registry Contributor
     Assign to: aaaorguamgdidentity (UAMI) or your CLI principal for local dev.

Registration is idempotent: re-running this script when a schema already exists
at the same version will return the existing schema ID without error.
To evolve a schema, increment `schema_version` in the model docstring comment
and add the new fields – the registry will assign a new ID for the new version.

Usage
-----
  python -m kafka.register_schemas            # register all four schemas
  python -m kafka.register_schemas --list     # list IDs of already-registered schemas
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Type

from pydantic import BaseModel

from azure.schemaregistry import SchemaRegistryClient, SchemaFormat
from azure.core.exceptions import HttpResponseError

from .config import EH_NAMESPACE, SCHEMA_GROUP, _credential
from .schemas import (
    PassengerPayload,
    FlightPayload,
    CasePayload,
    ComplaintPayload,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
logger = logging.getLogger(__name__)

# ── Schema registry client ────────────────────────────────────────────────────

# fully_qualified_namespace is the bare FQDN (no https://)
_registry = SchemaRegistryClient(
    fully_qualified_namespace=f"{EH_NAMESPACE}.servicebus.windows.net",
    credential=_credential,
)

# ── Topic → payload model mapping ────────────────────────────────────────────

SCHEMA_MAP: dict[str, Type[BaseModel]] = {
    "custcomplaints.passengers": PassengerPayload,
    "custcomplaints.flights":    FlightPayload,
    "custcomplaints.cases":      CasePayload,
    "custcomplaints.complaints": ComplaintPayload,
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _json_schema(model: Type[BaseModel]) -> str:
    """Return the JSON Schema string for a Pydantic model (pretty-printed)."""
    schema = model.model_json_schema()
    # Embed a $schema declaration so consumers know the dialect used.
    schema.setdefault("$schema", "https://json-schema.org/draft/2020-12/schema")
    return json.dumps(schema, indent=2, ensure_ascii=False)


def register_all() -> dict[str, str]:
    """
    Register all four payload schemas in the Event Hubs Schema Registry.

    Returns a dict mapping schema name → schema ID (GUID assigned by the registry).
    Registration is idempotent: if the exact definition already exists the
    registry returns the existing ID without raising an error.
    """
    results: dict[str, str] = {}

    for schema_name, model in SCHEMA_MAP.items():
        definition = _json_schema(model)
        logger.info("Registering schema: %s (group=%s)", schema_name, SCHEMA_GROUP)
        try:
            props = _registry.register_schema(
                group_name=SCHEMA_GROUP,
                name=schema_name,
                definition=definition,
                format=SchemaFormat.JSON,
            )
            results[schema_name] = props.id
            logger.info("  ✓ registered  id=%s", props.id)
        except HttpResponseError as exc:
            logger.error("  ✗ FAILED  schema=%s  error=%s", schema_name, exc)
            raise

    return results


def list_registered() -> dict[str, str]:
    """
    Retrieve the schema IDs for all four schemas that are already registered.
    Schemas that do not exist yet are reported as 'NOT FOUND'.
    """
    results: dict[str, str] = {}

    for schema_name, model in SCHEMA_MAP.items():
        definition = _json_schema(model)
        try:
            props = _registry.get_schema_properties(
                group_name=SCHEMA_GROUP,
                name=schema_name,
                definition=definition,
                format=SchemaFormat.JSON,
            )
            results[schema_name] = props.id
        except HttpResponseError as exc:
            if exc.status_code == 404:
                results[schema_name] = "NOT FOUND"
            else:
                logger.error("Error querying %s: %s", schema_name, exc)
                results[schema_name] = f"ERROR: {exc.reason}"

    return results


def lookup_schema_ids() -> dict[str, str]:
    """
    Resolve the current schema ID for every topic in SCHEMA_MAP.

    Intended for use by the producer at startup: call once, cache the result,
    then stamp each Kafka message with the correct "schema-id" header.

    Returns
    -------
    dict mapping topic name → schema ID (32-char hex UUID string).

    Raises
    ------
    RuntimeError  if any schema is not yet registered (run register_all() first).
    HttpResponseError  on any unexpected registry error.
    """
    ids: dict[str, str] = {}
    missing: list[str] = []

    for schema_name, model in SCHEMA_MAP.items():
        definition = _json_schema(model)
        try:
            props = _registry.get_schema_properties(
                group_name=SCHEMA_GROUP,
                name=schema_name,
                definition=definition,
                format=SchemaFormat.JSON,
            )
            ids[schema_name] = props.id
        except HttpResponseError as exc:
            if exc.status_code == 404:
                missing.append(schema_name)
            else:
                raise

    if missing:
        raise RuntimeError(
            f"Schemas not registered in group '{SCHEMA_GROUP}': {missing}. "
            "Run `python -m kafka.register_schemas` to register them first."
        )

    return ids


def _print_table(results: dict[str, str]) -> None:
    """Pretty-print name → schema ID as a table."""
    col = max(len(k) for k in results) + 2
    print(f"\n{'SCHEMA NAME':<{col}}  SCHEMA ID")
    print("-" * (col + 40))
    for name, sid in results.items():
        print(f"{name:<{col}}  {sid}")
    print()


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Register or list Event Hubs Schema Registry entries for CustComplaints topics."
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List IDs of already-registered schemas instead of registering.",
    )
    args = parser.parse_args()

    try:
        if args.list:
            logger.info("Querying existing schemas in group '%s'…", SCHEMA_GROUP)
            results = list_registered()
        else:
            logger.info("Registering schemas in group '%s'…", SCHEMA_GROUP)
            results = register_all()

        _print_table(results)

    except HttpResponseError as exc:
        logger.error("Schema Registry HTTP error: %s", exc)
        sys.exit(1)
    except Exception as exc:
        logger.error("Unexpected error: %s", exc)
        sys.exit(1)
