# Fabric Schema Registry – `fabricstreams`

Avro schema definitions and a registration script for the four
customer-complaints real-time streams, managed in the
[Fabric Schema Registry](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/schema-sets/schema-registry-overview)
(preview).

---

## Folder layout

```
fabricstreams/
├── __init__.py
├── config.py              # Workspace & auth settings (reads from .env)
├── register_schemas.py    # Programmatic registration via Fabric REST API
└── schemas/
    ├── passengers.avsc    # Avro schema – Passenger record
    ├── flights.avsc       # Avro schema – Flight record
    ├── cases.avsc         # Avro schema – Case record
    └── complaints.avsc    # Avro schema – Complaint record
```

---

## Why Avro?

Fabric Schema Registry currently supports **Avro only**.  
Avro gives us:

- Binary-efficient serialisation (smaller payloads through the eventstream)
- Schema-enforced validation at the eventstream boundary – bad events are
  dropped before they reach Eventhouse or Lakehouse
- First-class nullable union types (`["null", "string"]`) that round-trip
  cleanly to KQL `dynamic` / Delta `STRING`

---

## Schema-to-stream mapping

| Avro schema | Fabric Eventstream topic | FK dependencies |
|---|---|---|
| `Passenger` | `custcomplaints.passengers` | none |
| `Flight` | `custcomplaints.flights` | none |
| `Case` | `custcomplaints.cases` | passengers, flights |
| `Complaint` | `custcomplaints.complaints` | cases, passengers, flights |

Load / register in the order shown – Fabric validates FK integrity at query
time, not ingest time, but maintaining order avoids confusion.

---

## Prerequisites

### 1 – .env additions

Add the following to the project `.env` file:

```env
# ── Fabric Schema Registry ─────────────────────────────────────────────────
# GUID from the workspace URL:  https://app.fabric.microsoft.com/groups/<id>
FABRIC_WORKSPACE_ID=<your-fabric-workspace-id>

# Display name for the EventSchemaSet item that will be created in Fabric.
FABRIC_SCHEMA_SET_NAME=custcomplaints
```

### 2 – Permissions

The identity running the script needs:
- **Fabric workspace Member** (or higher) to create items in the workspace.
- The same `UAMI_CLIENT_ID` / `az login` identity already used by the kafka
  producer is sufficient; no extra role assignments are needed.

### 3 – Python dependencies

The registration script only uses packages already in `requirements.txt`:

```
azure-identity
requests
python-dotenv
```

---

## Registering schemas

### Validate locally (no Fabric calls)

```bash
python -m fabricstreams.register_schemas --validate
```

Expected output:
```
✓ passengers.avsc     name=Passenger     fields=9
✓ flights.avsc        name=Flight        fields=13
✓ cases.avsc          name=Case          fields=9
✓ complaints.avsc     name=Complaint     fields=17
```

### Register all four schemas

```bash
python -m fabricstreams.register_schemas
```

The script will:
1. Look for an existing `EventSchemaSet` named `custcomplaints` in the workspace.
2. Create one if it does not exist.
3. Upload all four `.avsc` schemas via the Fabric item-definition API.

Registration is **idempotent** – re-running will refresh the schemas in-place
(Fabric records this as a new version).

### List registered schemas

```bash
python -m fabricstreams.register_schemas --list
```

---

## Uploading schemas manually (UI fallback)

If the Fabric Item Definition API is not yet available for `EventSchemaSet`
in your tenant (feature is in preview), upload schemas through the UI:

1. Open [Microsoft Fabric](https://app.fabric.microsoft.com).
2. Navigate to your workspace → **+ New item** → search **Event Schema Set**.
3. Name the set **custcomplaints** → **Create**.
4. Click **+ New event schema** → **Upload** → select `passengers.avsc`.
5. Repeat for `flights.avsc`, `cases.avsc`, `complaints.avsc` in that order.

---

## Attaching a schema to an Eventstream

> **Important**: Schema validation must be enabled at *eventstream creation*
> time. It cannot be added to an existing eventstream.

1. Create a new Eventstream in your workspace.
2. Add a **Custom app / endpoint** source.
3. In the source configuration, toggle **Enable schema validation** → ON.
4. Select schema set **custcomplaints** and the matching schema
   (e.g., `Passenger` for the passengers stream).
5. Connect the destination (Eventhouse, Lakehouse, or derived stream).

Events that do not conform to the registered Avro schema are dropped and
logged under the Fabric error monitoring surface.

---

## Schema versioning

Fabric does not enforce compatibility checks between schema versions.
Follow these conventions to avoid breaking downstream pipelines:

| Change type | Safe approach |
|---|---|
| Add optional field | Add `["null", "T"]` union with `"default": null` – backwards compatible |
| Rename a field | Add new field, deprecate old one in `"doc"`, remove in a later version |
| Change a field type | Create a new schema version; update eventstream before retiring old |
| Remove a required field | Breaking – coordinate with all consumers before deploying |

---

## Avro type mapping

| PostgreSQL | Python (Pydantic) | Avro | Fabric / KQL |
|---|---|---|---|
| `BIGINT` | `int` | `"long"` | `long` |
| `VARCHAR(n)` | `str` | `"string"` | `string` |
| `TIMESTAMPTZ` | `str` (ISO-8601) | `"string"` | `datetime` |
| `DECIMAL(2,1)` | `float` | `"float"` | `real` |
| `BOOLEAN` | `bool` | `"boolean"` | `bool` |
| Nullable `T` | `Optional[T]` | `["null", "T"]` | nullable |
