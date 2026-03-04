# CustComplaints – Event Hub Kafka Producer

Kafka producer for the **Lunar Air / ZavaAir** customer complaints pipeline.
Produces four related tables to Azure Event Hubs in FK-safe order, with every
message stamped with a `schema-id` header resolved from the Event Hubs Schema
Registry.

---

## Architecture

```
┌─────────────────────────────────────────────────┐
│  Local dev / Azure compute                       │
│                                                  │
│  python -m kafka.producer                        │
│        │                                         │
│        ├─ DefaultAzureCredential (UAMI / CLI)    │
│        │         │                               │
│        │   ┌─────▼──────────────────────────┐   │
│        │   │  Event Hubs Schema Registry    │   │
│        │   │  aaaorgehns.servicebus.windows │   │
│        │   │  Group: custcomplaints (JSON)  │   │
│        │   └───────────────┬────────────────┘   │
│        │         schema-id │                     │
│        ▼                   ▼                     │
│  confluent-kafka Producer  +  schema-id header   │
│        │                                         │
└────────┼────────────────────────────────────────-┘
         │ SASL OAUTHBEARER (JWT from UAMI)
         ▼
┌─────────────────────────────────────────────────────────┐
│  Azure Event Hubs namespace: aaaorgehns (Standard tier) │
│                                                          │
│  custcomplaints.passengers   (partition key: passenger_id)
│  custcomplaints.flights      (partition key: flight_id)  │
│  custcomplaints.cases        (partition key: case_id)    │
│  custcomplaints.complaints   (partition key: complaint_id)
└─────────────────────────────────────────────────────────┘
```

**Produce order** (FK dependency): `passengers → flights → cases → complaints`

---

## Repository Layout

```
CustComplaints/
├── kafka/
│   ├── __init__.py
│   ├── config.py            # Event Hubs connection config + auth
│   ├── schemas.py           # Pydantic payload models + EventEnvelope
│   ├── producer.py          # Seed data + produce_dataset(); CLI entry point
│   └── register_schemas.py  # Schema Registry registration + lookup; CLI
├── sql/
│   ├── 01_schema.sql        # CREATE SCHEMA
│   ├── 02_tables.sql        # CREATE TABLE (all four tables + FK constraints)
│   └── 03_inserts.sql       # Reference seed data (mirrors producer seed)
├── .env.example             # Template – copy to .env and fill values
├── requirements.txt
└── README.md
```

---

## Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.11+ |
| Azure CLI (`az`) | any recent |
| Azure Event Hubs namespace | Standard tier or above |
| UAMI with roles below | – |

### Required IAM roles on the Event Hubs namespace

| Role | Purpose |
|---|---|
| **Azure Event Hubs Data Sender** | Produce messages to all four topics |
| **Schema Registry Contributor** | Register and read schemas |

Assign to both your UAMI (for Azure compute) and your personal Entra ID principal (for local dev via `az login`).

---

## Setup

### 1 – Clone and create a virtual environment

```bash
git clone <repo-url>
cd CustComplaints
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2 – Configure environment variables

```bash
cp .env.example .env
# Edit .env — minimum required changes are described in the file
```

Key variables:

| Variable | Description |
|---|---|
| `EH_NAMESPACE` | Event Hubs namespace name (no `.servicebus.windows.net`) |
| `AZURE_TENANT_ID` | Entra ID tenant ID – required so Event Hubs can validate the JWT issuer |
| `UAMI_CLIENT_ID` | Client ID of the User-Assigned Managed Identity |
| `USE_MANAGED_IDENTITY` | `false` for local dev (uses `az login`), `true` on Azure compute |
| `SCHEMA_GROUP` | Schema Registry group name (default: `custcomplaints`) |

### 3 – Create the four Event Hubs (topics)

Create these Event Hubs in your namespace (Azure Portal or CLI):

```
custcomplaints.passengers
custcomplaints.flights
custcomplaints.cases
custcomplaints.complaints
```

### 4 – Create the Schema Registry group

The Azure CLI only supports Avro schema groups. Use the ARM REST API to create a JSON group:

```bash
SUBSCRIPTION=<your-subscription-id>
RG=<resource-group>
NS=<eh-namespace>
GROUP=custcomplaints

az rest --method put \
  --url "https://management.azure.com/subscriptions/${SUBSCRIPTION}/resourceGroups/${RG}/providers/Microsoft.EventHub/namespaces/${NS}/schemagroups/${GROUP}?api-version=2022-10-01-preview" \
  --body '{"properties": {"schemaType": "Json"}}'
```

### 5 – Register the schemas

```bash
python -m kafka.register_schemas
```

Registers the JSON Schema derived from each Pydantic model. This is **idempotent** – safe to re-run; it returns existing IDs when schemas are already registered.

To verify:

```bash
python -m kafka.register_schemas --list
```

---

## Running the Producer

```bash
python -m kafka.producer
```

Produces the full seed dataset in FK-safe order:

| Topic | Records |
|---|---|
| `custcomplaints.passengers` | 20 |
| `custcomplaints.flights` | 13 |
| `custcomplaints.cases` | 20 |
| `custcomplaints.complaints` | 28 |

Each message carries a `schema-id` Kafka header (32-char hex UUID) that consumers can use to retrieve the schema from the registry without inspecting the topic name.

To produce your own data, import `produce_dataset` directly:

```python
from kafka.producer import produce_dataset
produce_dataset(passengers=[...], flights=[...], cases=[...], complaints=[...])
```

---

## Message Format

Every Kafka message value is a JSON-encoded `EventEnvelope`:

```json
{
  "event_id":       "550e8400-e29b-41d4-a716-446655440000",
  "table":          "custcomplaints.complaints",
  "schema_version": "1.0",
  "produced_at":    "2026-03-04T18:00:00+00:00",
  "payload": {
    "complaint_id": 7,
    "case_id": 5,
    ...
  }
}
```

`event_id` is a UUIDv4 deduplication key suitable for idempotent consumer upserts.

---

## SQL Reference Schema

The `sql/` directory contains the PostgreSQL DDL that mirrors the Kafka topics:

| File | Contents |
|---|---|
| `01_schema.sql` | `CREATE SCHEMA custcomplaints` |
| `02_tables.sql` | All four tables with PK / FK constraints |
| `03_inserts.sql` | Seed rows matching the producer seed data |

---

## Authentication Detail

Authentication uses **SASL OAUTHBEARER** — no connection strings or SAS keys.

`DefaultAzureCredential` is used on both paths:

- **Azure compute**: resolves to `ManagedIdentityCredential` when the UAMI is
  assigned to the VM / Container App / Function
- **Local dev**: resolves to `AzureCliCredential` after `az login`

The OAuth token scope **must** be namespace-specific:

```
https://<namespace>.servicebus.windows.net/.default
```

Using the generic `https://eventhubs.azure.net/.default` causes Event Hubs to
parse `eventhubs` as the tenant name and reject the token with
`SASL authentication error: Invalid tenant name 'eventhubs'`.

---

## Known Limitations (Standard Tier)

| Feature | Status |
|---|---|
| `enable.idempotence` | **Disabled** – requires `InitProducerId` API, not implemented on Standard tier |
| `compression.type` (gzip/snappy/lz4/zstd) | **Disabled** – compressed record batches rejected on Standard tier |

Both are supported on **Premium** and **Dedicated** tiers. The settings are removed from `PRODUCER_CONFIG` in `config.py`.

---

## Evolving Schemas

1. Update the relevant Pydantic model in `kafka/schemas.py`
2. Re-run `python -m kafka.register_schemas` — the registry assigns a new version/ID for the changed schema
3. Update `schema_version` in `EventEnvelope` if the change is breaking

---

## Next Steps

- **Consumer**: read `schema-id` header → `SchemaRegistryClient.get_schema(id)` → validate payload → upsert to PostgreSQL or Microsoft Fabric
- **Production IAM**: assign the UAMI both `Azure Event Hubs Data Sender` and `Schema Registry Contributor` on the namespace
- **Schema evolution**: bump fields in Pydantic models and re-register to get a new schema ID
