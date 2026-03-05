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
source .venv/bin/activate

python -m kafka.producer                   # seed data only (default)
python -m kafka.producer --dataset seed    # 20 passengers / 13 flights / 20 cases / 28 complaints
python -m kafka.producer --dataset test    # 3 / 2 / 4 / 5 new test records (passengers 21-23)
python -m kafka.producer --dataset all     # seed + test combined (23 / 15 / 24 / 33)
```

Records are sent in FK-safe order (`passengers → flights → cases → complaints`) with a
`flush()` after each table. Each message carries a `schema-id` Kafka header.

**Note:** always run as a module (`python -m kafka.producer`), not directly
(`python kafka/producer.py`) — the package uses relative imports.

### Test batch relationship map

```
Passenger 21 (Zara Thompson, AU)   → Case 21 → ZA1414 SIN→SYD → Complaints 29, 30
Passenger 22 (Hiroshi Yamamoto, JP) → Case 22 → ZA1414 SIN→SYD → Complaint 31
Passenger 23 (Lucia Ferreira, PT)   → Case 23 → ZA1515 SYD→LAX → Complaint 32
Passenger 21 (again)                → Case 24 → ZA1515 SYD→LAX → Complaint 33
```

---

## Forwarding to Fabric (Event Hubs → Fabric Eventstream)

Run one bridge process per topic. Use `--max-idle-secs 30` to exit automatically once
the queue drains (useful after a one-off produce run):

```bash
source .venv/bin/activate

python -m kafka.fabric_consumer --topic custcomplaints.passengers  --max-idle-secs 30 &
python -m kafka.fabric_consumer --topic custcomplaints.flights     --max-idle-secs 30 &
python -m kafka.fabric_consumer --topic custcomplaints.cases       --max-idle-secs 30 &
python -m kafka.fabric_consumer --topic custcomplaints.complaints  --max-idle-secs 30 &
wait
```

All four bridges run in parallel. Source offsets are committed **after** Fabric flush —
at-least-once delivery, with `event_id` handling duplicates at the Fabric side.

---

## Fabric Eventhouse KQL Setup

### 1 — Add `event_id` column to each table

```kql
.alter-merge table passengers (event_id: string)
.alter-merge table flights    (event_id: string)
.alter-merge table cases      (event_id: string)
.alter-merge table complaints (event_id: string)
```

### 2 — Create ingestion mappings

`event_id` is at the envelope root (`$.event_id`); all data columns are under `$.payload.*`.
Example for `passengers` — repeat for the other three tables with their own column lists:

```kql
.create-or-alter table passengers ingestion json mapping "PassengersMapping"
'[
  {"column":"event_id",            "path":"$.event_id"},
  {"column":"passenger_id",        "path":"$.payload.passenger_id"},
  {"column":"first_name",          "path":"$.payload.first_name"},
  {"column":"last_name",           "path":"$.payload.last_name"},
  {"column":"email",               "path":"$.payload.email"},
  {"column":"phone",               "path":"$.payload.phone"},
  {"column":"country",             "path":"$.payload.country"},
  {"column":"frequent_flyer_tier", "path":"$.payload.frequent_flyer_tier"},
  {"column":"total_flights",       "path":"$.payload.total_flights"},
  {"column":"member_since",        "path":"$.payload.member_since"}
]'
```

Set the mapping name in the Fabric Eventstream destination config, then re-run the
bridge with `--dataset all` to backfill `event_id` on rows ingested before this change.

### 3 — Verification KQL queries

**Row counts**
```kql
passengers | count
flights    | count
cases      | count
complaints | count
```

**Duplicate check — should return 0 rows**
```kql
union
  (passengers | summarize n=count() by event_id | where n > 1 | extend table="passengers"),
  (flights    | summarize n=count() by event_id | where n > 1 | extend table="flights"),
  (cases      | summarize n=count() by event_id | where n > 1 | extend table="cases"),
  (complaints | summarize n=count() by event_id | where n > 1 | extend table="complaints")
| project table, event_id, n
```

**Relationship integrity — complaints joined to passenger + flight (test batch)**
```kql
complaints
| where complaint_id between (29 .. 33)
| join kind=inner (cases      | project case_id, case_status, pnr) on case_id
| join kind=inner (passengers | project passenger_id, first_name, last_name) on passenger_id
| join kind=inner (flights    | project flight_id, flight_number, origin_code, destination_code) on flight_id
| project complaint_id, first_name, last_name, flight_number, origin_code, destination_code,
          category, subcategory, severity, case_status
| order by complaint_id asc
```

**Multi-case passenger check (Passenger 21 — expects 3 complaints across 2 flights)**
```kql
complaints
| where passenger_id == 21
| join kind=inner (cases | project case_id, flight_id, pnr, case_status) on case_id
| project complaint_id, case_id, flight_id, pnr, category, subcategory, severity, status
| order by case_id asc, complaint_id asc
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

`event_id` is a **UUID v5** derived deterministically from `"{table}:{pk}"` — the same
record always produces the same `event_id` across re-runs, enabling idempotent upserts
in Fabric Eventhouse without double-counting.

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

## Fabric IQ Ontology Agent

An ontology agent built on Fabric IQ sits on top of the four KQL tables and exposes
the complaint graph for natural-language queries and GQL traversal.

### Entity types and labels

| Entity type | Label in GQL | Primary key |
| --- | --- | --- |
| Passenger | `` `passenger` `` (lowercase) | `passenger_id` |
| Flight | `` `Flight` `` | `flight_id` |
| Case | `` `Case` `` | `case_id` |
| Complaint | `` `Complaint` `` | `complaint_id` |

### Relationships

| GQL relationship | From | To | Meaning |
| --- | --- | --- | --- |
| `` `creates` `` | `passenger` | `Case` | Passenger opened a case |
| `` `generates` `` | `Case` | `Complaint` | Case produced a complaint item |
| `` `takes` `` | `passenger` | `Flight` | Passenger flew on a flight associated with the case |

### GQL syntax rules (confirmed working)

- All labels and relationship types must be **backtick-quoted**
- `passenger` label is **lowercase** — all others are Title Case
- Multiple patterns in a single `MATCH` are **comma-separated**
- `GROUP BY` accepts **aliases only** — not `node.property` directly
- `FILTER` after `GROUP BY` is **not supported** — use `ORDER BY DESC` as a workaround
- Use `TO_JSON_STRING(node)` to return full node or edge as JSON
- **Relationship direction is strict** — never reverse these:
  - `(passenger)-[:creates]->(Case)` ✓
  - `(Case)-[:generates]->(Complaint)` ✓
  - `(passenger)-[:takes]->(Flight)` ✓

### GQL query skeleton

```gql
MATCH (node_Case:`Case`)-[edge1_generates:`generates`]->(node_Complaint:`Complaint`),
      (node_passenger:`passenger`)-[edge2_creates:`creates`]->(node_Case:`Case`),
      (node_passenger:`passenger`)-[edge3_takes:`takes`]->(node_Flight:`Flight`)
RETURN TO_JSON_STRING(node_Case) AS `Case`,
       TO_JSON_STRING(node_passenger) AS `passenger`,
       TO_JSON_STRING(node_Complaint) AS `Complaint`,
       TO_JSON_STRING(node_Flight) AS `Flight`
LIMIT 1000
```

### Agent instructions (datasource guide)

Point the agent at `datasource_guide.md` in the repo root. It contains:
- Table-by-table column reference with types and nullability
- Key join patterns and FK-safe query order
- KQL cookbook for cross-table queries
- Deduplication contract (`event_id` UUID v5)

### Sample agent questions

| Question | What it tests |
|---|---|
| Which passengers have more than one case? | Multi-hop relationship traversal |
| Show all Platinum passengers with unresolved complaints | Tier + status filter |
| What happened on flight ZA1818? | Flight-scoped complaint cluster |
| Which complaint category appears most often? | Aggregation |
| Has Chen Wei had any complaints resolved? | Named entity + status filter |
| Which routes have the most Critical complaints? | Route + severity join |

### Full GQL query library

See `GQLQueries.md` in the repo root for all confirmed working queries with
descriptions, covering: full traversal, top complainers, escalation analysis,
cancelled/diverted flight clusters, severity breakdowns, and tier-based filtering.

---

## Next Steps

- **Consumer**: read `schema-id` header → `SchemaRegistryClient.get_schema(id)` → validate payload → upsert to PostgreSQL or Microsoft Fabric
- **Production IAM**: assign the UAMI both `Azure Event Hubs Data Sender` and `Schema Registry Contributor` on the namespace
- **Schema evolution**: bump fields in Pydantic models and re-register to get a new schema ID
