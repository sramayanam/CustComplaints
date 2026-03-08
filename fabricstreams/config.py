"""
config.py – Fabric Schema Registry configuration
==================================================
Reads Fabric workspace and schema set settings from environment variables
(or the project .env file).

Authentication
--------------
A service principal (ClientSecretCredential) is used when all three
FABRIC_SP_* variables are present – the right choice when the Fabric
workspace lives in a different Entra tenant from the rest of the project.

Falls back to DefaultAzureCredential (UAMI → az-login) when the SP vars
are absent.

Required .env values
--------------------
    FABRIC_SP_TENANT_ID       – Entra tenant that owns the service principal.
    FABRIC_SP_CLIENT_ID       – Application (client) ID of the SP.
    FABRIC_SP_CLIENT_SECRET   – Client secret value (not the secret ID).

    FABRIC_WORKSPACE_ID       – GUID of the target Fabric workspace.
                                https://app.fabric.microsoft.com/groups/<id>

Optional .env values
--------------------
    FABRIC_SCHEMA_SET_ID      – Item GUID of an already-created EventSchemaSet.
                                When set the script skips create/lookup and
                                uploads directly.  Leave blank to
                                auto-create / auto-discover by name.
    FABRIC_SCHEMA_SET_NAME    – Display name used when creating a new schema
                                set (default: custcomplaints).
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from azure.identity import ClientSecretCredential, DefaultAzureCredential

# Load .env from the project root (two levels up from this file).
load_dotenv(Path(__file__).parents[1] / ".env")

# ── Authentication ─────────────────────────────────────────────────────────────

FABRIC_SP_TENANT_ID: str     = os.getenv("FABRIC_SP_TENANT_ID", "")
FABRIC_SP_CLIENT_ID: str     = os.getenv("FABRIC_SP_CLIENT_ID", "")
FABRIC_SP_CLIENT_SECRET: str = os.getenv("FABRIC_SP_CLIENT_SECRET", "")

if FABRIC_SP_TENANT_ID and FABRIC_SP_CLIENT_ID and FABRIC_SP_CLIENT_SECRET:
    # Service principal in a different tenant
    credential = ClientSecretCredential(
        tenant_id=FABRIC_SP_TENANT_ID,
        client_id=FABRIC_SP_CLIENT_ID,
        client_secret=FABRIC_SP_CLIENT_SECRET,
    )
elif FABRIC_SP_TENANT_ID:
    # User principal: run  az login --tenant <FABRIC_SP_TENANT_ID>  first
    from azure.identity import AzureCliCredential
    credential = AzureCliCredential(tenant_id=FABRIC_SP_TENANT_ID)
else:
    UAMI_CLIENT_ID: str = os.getenv(
        "UAMI_CLIENT_ID",
        "7f12934d-08b8-402b-8c1d-8529efd4f8c1",
    )
    credential = DefaultAzureCredential(managed_identity_client_id=UAMI_CLIENT_ID)

# Scope for the Fabric REST API.
FABRIC_API_SCOPE = "https://api.fabric.microsoft.com/.default"
FABRIC_API_BASE  = "https://api.fabric.microsoft.com/v1"

# ── Workspace ──────────────────────────────────────────────────────────────────

FABRIC_WORKSPACE_ID: str = os.getenv("FABRIC_WORKSPACE_ID", "")
if not FABRIC_WORKSPACE_ID:
    raise EnvironmentError(
        "FABRIC_WORKSPACE_ID is not set.  "
        "Add it to your .env file (find it in the Fabric workspace URL)."
    )

# ── Schema set ─────────────────────────────────────────────────────────────────

# If the EventSchemaSet item already exists, pin its ID here to skip
# the find/create step entirely.
FABRIC_SCHEMA_SET_ID: str   = os.getenv("FABRIC_SCHEMA_SET_ID", "")
FABRIC_SCHEMA_SET_NAME: str = os.getenv("FABRIC_SCHEMA_SET_NAME", "custcomplaints")

# ── Stream → schema file mapping ──────────────────────────────────────────────
# Maps each Fabric Eventstream topic name to its Avro schema file.
# The 'schema_name' key must be unique within the schema set and becomes the
# event schema's display name in the Fabric UI.

SCHEMAS_DIR = Path(__file__).parent / "schemas"

STREAM_SCHEMA_MAP: list[dict] = [
    {
        "stream":      "passengers",
        "schema_name": "Passenger",
        "avsc_path":   SCHEMAS_DIR / "passengers.avsc",
        "doc":         "Passenger master data – no FK dependencies",
    },
    {
        "stream":      "flights",
        "schema_name": "Flight",
        "avsc_path":   SCHEMAS_DIR / "flights.avsc",
        "doc":         "Flight records – no FK dependencies",
    },
    {
        "stream":      "cases",
        "schema_name": "Case",
        "avsc_path":   SCHEMAS_DIR / "cases.avsc",
        "doc":         "Support cases – depends on passengers and flights",
    },
    {
        "stream":      "complaints",
        "schema_name": "Complaint",
        "avsc_path":   SCHEMAS_DIR / "complaints.avsc",
        "doc":         "Complaints – depends on cases, passengers, and flights",
    },
]
