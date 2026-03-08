"""
register_schemas.py – Register Avro schemas in Fabric Schema Registry
======================================================================
Creates (or locates) a Fabric EventSchemaSet item in the configured workspace,
then uploads each of the four Avro schemas for the customer-complaints streams.

How it works
------------
Fabric Schema Registry is accessed via the standard Fabric REST Items API:
    • POST /v1/workspaces/{wid}/items             – create the schema set
    • GET  /v1/workspaces/{wid}/items?type=...    – find an existing schema set
    • POST /v1/workspaces/{wid}/items/{id}/       – push schema content via
              updateDefinition                       the item-definition API

Each .avsc file is base64-encoded and uploaded as an "inline definition part".
The path format Fabric uses inside an EventSchemaSet definition is:
    EventSchemas/<SchemaName>/schema.avsc

Re-running this script is safe:
    • If the schema set already exists it is reused (no duplicate created).
    • If a schema part already exists at the same path the definition is
      refreshed (Fabric treats this as a new version).

Authentication
--------------
Uses DefaultAzureCredential (UAMI on Azure, az-login locally).
Scope: https://api.fabric.microsoft.com/.default

Usage
-----
    # Register all four schemas (creates schema set if needed):
    python -m fabricstreams.register_schemas

    # List schemas currently in the schema set:
    python -m fabricstreams.register_schemas --list

    # Validate .avsc files without touching Fabric:
    python -m fabricstreams.register_schemas --validate
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import sys
import time
from pathlib import Path

import requests

from .config import (
    FABRIC_API_BASE,
    FABRIC_API_SCOPE,
    FABRIC_SCHEMA_SET_ID,
    FABRIC_SCHEMA_SET_NAME,
    FABRIC_WORKSPACE_ID,
    STREAM_SCHEMA_MAP,
    credential,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
logger = logging.getLogger(__name__)


# ── Token helper ──────────────────────────────────────────────────────────────

def _get_token() -> str:
    """Return a short-lived Bearer token for the Fabric REST API."""
    token = credential.get_token(FABRIC_API_SCOPE)
    return token.token


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {_get_token()}",
        "Content-Type": "application/json",
    }


# ── Schema set CRUD ───────────────────────────────────────────────────────────

def find_schema_set() -> str | None:
    """
    Return the item ID of an existing EventSchemaSet whose display name matches
    FABRIC_SCHEMA_SET_NAME, or None if not found.
    """
    url = f"{FABRIC_API_BASE}/workspaces/{FABRIC_WORKSPACE_ID}/items"
    params = {"type": "EventSchemaSet"}
    resp = requests.get(url, headers=_headers(), params=params, timeout=30)
    resp.raise_for_status()

    for item in resp.json().get("value", []):
        if item.get("displayName") == FABRIC_SCHEMA_SET_NAME:
            logger.info("Found existing schema set '%s'  id=%s",
                        FABRIC_SCHEMA_SET_NAME, item["id"])
            return item["id"]
    return None


def create_schema_set() -> str:
    """
    Create a new EventSchemaSet item and return its item ID.
    Fabric may take a moment to provision; the function waits and retries
    the item lookup up to 5 times before giving up.
    """
    url = f"{FABRIC_API_BASE}/workspaces/{FABRIC_WORKSPACE_ID}/items"
    body = {
        "displayName": FABRIC_SCHEMA_SET_NAME,
        "type": "EventSchemaSet",
        "description": (
            "Avro schemas for the customer-complaints real-time streams: "
            "passengers, flights, cases, complaints."
        ),
    }
    logger.info("Creating schema set '%s' …", FABRIC_SCHEMA_SET_NAME)
    resp = requests.post(url, headers=_headers(), json=body, timeout=30)

    # 201 Created or 202 Accepted (async provisioning)
    if resp.status_code not in (200, 201, 202):
        logger.error("Create schema set failed  status=%s  body=%s",
                     resp.status_code, resp.text)
        resp.raise_for_status()

    # For async 202, Fabric returns a Location header; poll for completion.
    if resp.status_code == 202:
        item_id = _poll_long_running(resp)
    else:
        item_id = resp.json()["id"]

    logger.info("Schema set created  id=%s", item_id)
    return item_id


def _poll_long_running(accepted_resp: requests.Response) -> str:
    """
    Poll a Fabric long-running operation (202 Accepted) until it finishes,
    then return the created item ID.
    """
    location = accepted_resp.headers.get("Location") or accepted_resp.headers.get("location")
    if not location:
        raise RuntimeError(
            "Fabric returned 202 but no Location header for polling.  "
            f"Body: {accepted_resp.text}"
        )

    for attempt in range(12):  # up to ~60 s
        time.sleep(5)
        poll_resp = requests.get(location, headers=_headers(), timeout=30)
        poll_resp.raise_for_status()
        data = poll_resp.json()
        status = data.get("status", "").lower()
        if status == "succeeded":
            return data["createdItemId"]
        if status in ("failed", "cancelled"):
            raise RuntimeError(f"Schema set provisioning {status}: {data}")
        logger.debug("Provisioning … status=%s (attempt %d)", status, attempt + 1)

    raise TimeoutError("Schema set provisioning timed out after 60 s.")


# ── Schema definition upload ──────────────────────────────────────────────────

def _fabric_safe_avro(schema_dict: dict) -> dict:
    """
    Return a copy of an Avro schema dict safe for Fabric's preview Avro parser.

    Fabric's Avro validator throws a NullReferenceException on union types such
    as ["null", "string"].  Collapse nullable unions to their non-null member and
    drop the accompanying "default": null – keeping it would be invalid Avro once
    "null" is no longer part of the type union.  The source .avsc files are left
    untouched.
    """
    import copy

    def _walk(node):
        if isinstance(node, dict):
            new_dict = {k: _walk(v) for k, v in node.items()}

            # Collapse ["null", "T"] → "T" at the field-dict level so we can
            # simultaneously remove the "default": null that becomes invalid.
            raw_type = node.get("type")
            if isinstance(raw_type, list) and "null" in raw_type:
                non_null = [x for x in raw_type if x != "null"]
                new_dict["type"] = non_null[0] if len(non_null) == 1 else non_null
                if "default" in new_dict and new_dict["default"] is None:
                    del new_dict["default"]

            return new_dict
        if isinstance(node, list):
            return [_walk(i) for i in node]
        return node

    return _walk(copy.deepcopy(schema_dict))


def _build_definition(schema_entries: list[dict], platform_payload: str) -> dict:
    """
    Build the Fabric item-definition payload for all four schemas.

    Structure reverse-engineered from a manually-created testschema in the
    Fabric UI (confirmed via getDefinition):

      eventTypes[]:
        - format      = "CloudEvents/1.0"
        - schemaUrl   = "#/schemas/<id>"   (NOT inline schema)
        - schemaFormat = "Avro/1.12.0"
        - envelopeMetadata with id/type/source/specversion

      schemas[]:
        - id      = schema name
        - format  = "Avro/1.12.0"
        - versions[]:
            - id       = "v1"
            - format   = "Avro/1.12.0"
            - schema   = escaped JSON string
            - ancestor = "v1"

    Avro union ["null","T"] must be collapsed to "T" and the associated
    "default": null removed (Fabric preview parser limitation).
    """
    event_types = []
    schemas = []

    for entry in schema_entries:
        avsc_path: Path = entry["avsc_path"]
        raw_dict = json.loads(avsc_path.read_text(encoding="utf-8"))
        schema_id = entry["schema_name"]

        # Collapse nullable unions and strip the now-invalid "default": null.
        fabric_dict = _fabric_safe_avro(raw_dict)
        # Fabric's Avro parser throws NullReferenceException on "namespace" and
        # top-level "doc" on the record.  Strip both and reorder keys to match
        # the format Fabric itself stores when schemas are uploaded via the UI:
        # { "fields": [...], "type": "record", "name": "<name>" }
        fabric_dict.pop("namespace", None)
        fabric_dict.pop("doc", None)
        fabric_dict = {
            "fields": fabric_dict["fields"],
            "type":   fabric_dict["type"],
            "name":   fabric_dict["name"],
        }
        schema_str = json.dumps(fabric_dict, indent=1)

        event_types.append({
            "id": schema_id,
            "description": entry.get("doc", ""),
            "format": "CloudEvents/1.0",
            "envelopeMetadata": {
                "id":          {"type": "string", "required": True},
                "type":        {"type": "string", "required": True, "value": schema_id},
                "source":      {"type": "string", "required": True},
                "specversion": {"type": "string", "required": True},
            },
            "schemaUrl": f"#/schemas/{schema_id}",
            "schemaFormat": "Avro/1.12.0",
        })

        schemas.append({
            "id": schema_id,
            "format": "Avro/1.12.0",
            "versions": [
                {
                    "id": "v1",
                    "format": "Avro/1.12.0",
                    "schema": schema_str,
                    "ancestor": "v1",
                }
            ],
        })

    definition_json = {"eventTypes": event_types, "schemas": schemas}

    parts = [
        {
            "path": "EventSchemaSetDefinition.json",
            "payload": base64.b64encode(
                json.dumps(definition_json, indent=2).encode("utf-8")
            ).decode("ascii"),
            "payloadType": "InlineBase64",
        },
        {
            "path": ".platform",
            "payload": platform_payload,
            "payloadType": "InlineBase64",
        },
    ]
    return {"definition": {"parts": parts}}


def _get_platform_payload(schema_set_id: str) -> str:
    """Fetch and return the raw base64 .platform payload from the existing schema set."""
    url = (
        f"{FABRIC_API_BASE}/workspaces/{FABRIC_WORKSPACE_ID}"
        f"/items/{schema_set_id}/getDefinition"
    )
    resp = requests.post(url, headers=_headers(), timeout=30)
    resp.raise_for_status()
    parts = resp.json().get("definition", {}).get("parts", [])
    for part in parts:
        if part["path"] == ".platform":
            return part["payload"]
    raise RuntimeError(".platform part not found in existing schema set definition")


def upload_schemas(schema_set_id: str) -> None:
    """
    Push all four Avro schemas into the Fabric EventSchemaSet via the
    updateDefinition API endpoint.
    """
    url = (
        f"{FABRIC_API_BASE}/workspaces/{FABRIC_WORKSPACE_ID}"
        f"/items/{schema_set_id}/updateDefinition"
    )
    platform_payload = _get_platform_payload(schema_set_id)
    definition_payload = _build_definition(STREAM_SCHEMA_MAP, platform_payload)
    logger.info(
        "Uploading %d schema(s) to schema set %s …",
        len(STREAM_SCHEMA_MAP), schema_set_id,
    )
    resp = requests.post(url, headers=_headers(), json=definition_payload, timeout=60)

    if resp.status_code not in (200, 201, 202):
        logger.error(
            "updateDefinition failed  status=%s  body=%s",
            resp.status_code, resp.text,
        )
        resp.raise_for_status()

    logger.info("All schemas uploaded successfully.")


# ── List helper ───────────────────────────────────────────────────────────────

def list_schemas(schema_set_id: str) -> None:
    """Print the event types and schema versions registered in the schema set."""
    url = (
        f"{FABRIC_API_BASE}/workspaces/{FABRIC_WORKSPACE_ID}"
        f"/items/{schema_set_id}/getDefinition"
    )
    resp = requests.post(url, headers=_headers(), timeout=30)
    resp.raise_for_status()
    parts = resp.json().get("definition", {}).get("parts", [])
    defn_part = next((p for p in parts if p["path"] == "EventSchemaSetDefinition.json"), None)
    if not defn_part:
        logger.info("Schema set is empty (no EventSchemaSetDefinition.json found).")
        return
    defn = json.loads(base64.b64decode(defn_part["payload"]).decode("utf-8"))
    schemas = defn.get("schemas", [])
    event_types = {et["id"]: et for et in defn.get("eventTypes", [])}
    if not schemas and not event_types:
        logger.info("Schema set '%s' has no schemas registered.", FABRIC_SCHEMA_SET_NAME)
        return
    logger.info("Schemas in schema set '%s':", FABRIC_SCHEMA_SET_NAME)
    for s in schemas:
        et = event_types.get(s["id"], {})
        versions = s.get("versions", [])
        ver_ids = [v.get("id") for v in versions]
        logger.info("  • %-20s  schemaFormat=%-14s  versions=%s  schemaUrl=%s",
                    s["id"], s.get("format", ""), ver_ids, et.get("schemaUrl", ""))
    # Also surface any event types that use inline schema (legacy uploads)
    for et_id, et in event_types.items():
        if "schema" in et and et_id not in {s["id"] for s in schemas}:
            logger.info("  • %-20s  (inline schema, no schemas[] entry)", et_id)


# ── Validate helper ───────────────────────────────────────────────────────────

def validate_schemas() -> bool:
    """
    Parse each .avsc file to ensure it is valid JSON with required Avro fields.
    Returns True if all are valid, False otherwise.
    """
    ok = True
    for entry in STREAM_SCHEMA_MAP:
        path: Path = entry["avsc_path"]
        try:
            schema = json.loads(path.read_text(encoding="utf-8"))
            assert schema.get("type") == "record", "top-level 'type' must be 'record'"
            assert "name" in schema, "missing 'name' field"
            assert "fields" in schema, "missing 'fields' array"
            logger.info("✓ %-20s  name=%-12s  fields=%d",
                        path.name, schema["name"], len(schema["fields"]))
        except (json.JSONDecodeError, AssertionError, FileNotFoundError) as exc:
            logger.error("✗ %-20s  %s", path.name, exc)
            ok = False
    return ok


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Register Avro schemas in Fabric Schema Registry."
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List schemas currently registered in the schema set.",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate .avsc files locally without touching Fabric.",
    )
    args = parser.parse_args()

    if args.validate:
        success = validate_schemas()
        sys.exit(0 if success else 1)

    # Ensure all .avsc files are valid before making any API calls.
    if not validate_schemas():
        logger.error("Schema validation failed – aborting.")
        sys.exit(1)

    # Resolve schema set ID – use the pinned value if provided.
    if FABRIC_SCHEMA_SET_ID:
        logger.info("Using pinned schema set id=%s", FABRIC_SCHEMA_SET_ID)
        schema_set_id = FABRIC_SCHEMA_SET_ID
    else:
        schema_set_id = find_schema_set()
        if not schema_set_id:
            schema_set_id = create_schema_set()

    if args.list:
        list_schemas(schema_set_id)
    else:
        upload_schemas(schema_set_id)


if __name__ == "__main__":
    main()
