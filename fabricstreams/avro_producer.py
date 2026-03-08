"""
fabricstreams/avro_producer.py  –  Fabric Eventstream Kafka producer (Avro binary)
====================================================================================
Same endpoint, same CloudEvents headers, same _TYPE_CONFIG as producer.py —
only difference is the wire format: Avro binary instead of JSON.

  ce_datacontenttype: application/avro

Fabric reads the Avro schema from the EventSchemaSet (matched via ce_type) and
uses it to deserialize the binary payload directly into the KQL table columns.

Usage
-----
  python -m fabricstreams.avro_producer                        # seed – all 4 types
  python -m fabricstreams.avro_producer --dataset test
  python -m fabricstreams.avro_producer --dataset all
  python -m fabricstreams.avro_producer --types Passenger Flights
"""

from __future__ import annotations

import io
import json
import logging
import os
import sys
import uuid
from pathlib import Path

import fastavro
from confluent_kafka import KafkaException, Producer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
logger = logging.getLogger(__name__)

# ── Schema loading ─────────────────────────────────────────────────────────────

_SCHEMA_DIR = Path(__file__).parent / "schemas"

def _load_schema(avsc_file: str) -> dict:
    """Parse an .avsc file into a fastavro schema object."""
    with open(_SCHEMA_DIR / avsc_file) as f:
        return fastavro.parse_schema(json.load(f))


# ── Connection ─────────────────────────────────────────────────────────────────

def _parse_connection_string(conn_str: str) -> tuple[str, str]:
    parts = dict(seg.split("=", 1) for seg in conn_str.split(";") if "=" in seg)
    host = parts["Endpoint"].removeprefix("sb://").rstrip("/")
    return f"{host}:9093", parts["EntityPath"]


_SCHEMA_SET_ID = os.environ.get("FABRIC_SCHEMA_SET_ID", "998f81b5-ceda-4f80-8b17-409388e5d508")
_SCHEMA_REGISTRY_BASE = (
    "https://rthprodbn73280331.eastus2.messagingcatalog.azure.net"
    f"/schemagroups/{_SCHEMA_SET_ID}/schemas"
)

# ce_type key → pk field, schema name in registry, local .avsc file
_TYPE_CONFIG: dict[str, dict] = {
    "complaints": {"pk": "complaint_id", "schema": "complaints", "avsc": "complaints.avsc"},
    "Flights":    {"pk": "flight_id",    "schema": "Flights",    "avsc": "flights.avsc"},
    "Passenger":  {"pk": "passenger_id", "schema": "Passenger",  "avsc": "passengers.avsc"},
    "case":       {"pk": "case_id",      "schema": "case",       "avsc": "cases.avsc"},
}

# Load and parse all schemas at import time
_AVRO_SCHEMAS: dict[str, dict] = {
    et: _load_schema(cfg["avsc"]) for et, cfg in _TYPE_CONFIG.items()
}

_CONN_STR = os.environ["FABRIC_CONNECTION_STRING"]
BOOTSTRAP_SERVER, TOPIC = _parse_connection_string(_CONN_STR)

_PRODUCER_CONFIG: dict = {
    "bootstrap.servers": BOOTSTRAP_SERVER,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism":    "PLAIN",
    "sasl.username":     "$ConnectionString",
    "sasl.password":     _CONN_STR,
    "acks":              "all",
    "retries":           3,
    "retry.backoff.ms":  1000,
}


# ── Avro serialisation ─────────────────────────────────────────────────────────

def _to_avro_bytes(schema: dict, record: dict) -> bytes:
    """Serialize a record dict to schemaless Avro binary (no container header)."""
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, record)
    return buf.getvalue()


# ── Delivery callback ──────────────────────────────────────────────────────────

def _on_delivery(err, msg) -> None:
    if err:
        logger.error(
            "Delivery FAILED | topic=%s partition=%s key=%s error=%s",
            msg.topic(), msg.partition(), msg.key(), err,
        )
    else:
        logger.debug(
            "Delivered | topic=%-60s partition=%d offset=%d key=%s",
            msg.topic(), msg.partition(), msg.offset(), msg.key(),
        )


# ── Core send helper ───────────────────────────────────────────────────────────

def _send(producer: Producer, topic: str, entity_type: str, record: dict,
          pk_field: str, schema_name: str, avro_schema: dict) -> None:
    key   = str(record[pk_field]).encode("utf-8")
    value = _to_avro_bytes(avro_schema, record)
    dataschema = f"{_SCHEMA_REGISTRY_BASE}/{schema_name}/versions/v1"

    headers = [
        ("ce_specversion",     b"1.0"),
        ("ce_id",              str(uuid.uuid4()).encode()),
        ("ce_type",            entity_type.encode()),
        ("ce_source",          b"fabricstreams/avro_producer"),
        ("ce_datacontenttype", b"application/avro"),          # ← Avro binary
        ("ce_dataschema",      dataschema.encode()),
    ]

    while True:
        try:
            producer.produce(
                topic=topic,
                key=key,
                value=value,
                headers=headers,
                on_delivery=_on_delivery,
            )
            break
        except BufferError:
            logger.warning("Producer queue full, polling to drain…")
            producer.poll(0.5)


# ── Public API ─────────────────────────────────────────────────────────────────

def produce_records(entity_type: str, records: list[dict]) -> None:
    """Produce Avro-serialized records for the given entity type to Fabric Eventstream."""
    cfg    = _TYPE_CONFIG[entity_type]
    schema = _AVRO_SCHEMAS[entity_type]
    producer = Producer(_PRODUCER_CONFIG)

    logger.info(
        "Avro producer ready | type=%-12s topic=%s records=%d",
        entity_type, TOPIC, len(records),
    )
    for row in records:
        _send(producer, TOPIC, entity_type, row, cfg["pk"], cfg["schema"], schema)
    producer.flush()
    logger.info("%s flushed ✓  (%d Avro records)", entity_type, len(records))


# ── Inline sample data ─────────────────────────────────────────────────────────
# A small standalone dataset that exercises nullable fields, all enum values,
# and the FK chain (Passenger → Flights → case → complaints).
# All timestamps are ISO-8601 strings to match the schema field type "string".

SAMPLE_PASSENGERS = [
    {
        "passenger_id": 101, "first_name": "Amara",   "last_name": "Osei",
        "email": "amara.osei@example.com", "phone": None,
        "country": "Ghana", "frequent_flyer_tier": "Gold", "total_flights": 42,
        "member_since": "2021-06-15T00:00:00+00:00",
    },
    {
        "passenger_id": 102, "first_name": "Lena",    "last_name": "Fischer",
        "email": "lena.fischer@example.de", "phone": "+49-30-9999",
        "country": "Germany", "frequent_flyer_tier": "Silver", "total_flights": 17,
        "member_since": "2023-01-10T00:00:00+00:00",
    },
    {
        "passenger_id": 103, "first_name": "Kenji",   "last_name": "Tanaka",
        "email": None, "phone": "+81-3-1234-5678",
        "country": "Japan", "frequent_flyer_tier": "Platinum", "total_flights": 130,
        "member_since": "2018-09-01T00:00:00+00:00",
    },
]

SAMPLE_FLIGHTS = [
    {
        "flight_id": 201, "flight_number": "ZA2001",
        "origin_code": "ACC", "origin_city": "Accra",
        "destination_code": "LHR", "destination_city": "London",
        "scheduled_departure": "2026-03-10T06:00:00+00:00",
        "actual_departure":    "2026-03-10T06:15:00+00:00",
        "scheduled_arrival":   "2026-03-10T16:30:00+00:00",
        "actual_arrival":      "2026-03-10T16:45:00+00:00",
        "aircraft_type": "Boeing 787", "flight_status": "On Time", "delay_minutes": 15,
    },
    {
        "flight_id": 202, "flight_number": "ZA2002",
        "origin_code": "FRA", "origin_city": "Frankfurt",
        "destination_code": "SIN", "destination_city": "Singapore",
        "scheduled_departure": "2026-03-11T10:00:00+00:00",
        "actual_departure":    None,
        "scheduled_arrival":   "2026-03-12T06:00:00+00:00",
        "actual_arrival":      None,
        "aircraft_type": "Airbus A380", "flight_status": "Cancelled", "delay_minutes": 0,
    },
    {
        "flight_id": 203, "flight_number": "ZA2003",
        "origin_code": "NRT", "origin_city": "Tokyo",
        "destination_code": "SYD", "destination_city": "Sydney",
        "scheduled_departure": "2026-03-12T22:00:00+00:00",
        "actual_departure":    "2026-03-12T22:50:00+00:00",
        "scheduled_arrival":   "2026-03-13T09:00:00+00:00",
        "actual_arrival":      "2026-03-13T09:55:00+00:00",
        "aircraft_type": "Boeing 777", "flight_status": "Delayed", "delay_minutes": 55,
    },
]

SAMPLE_CASES = [
    {
        "case_id": 301, "passenger_id": 101, "flight_id": 201,
        "flight_number": "ZA2001", "pnr": "AVROAA",
        "case_status": "Open",
        "opened_at":      "2026-03-10T18:00:00+00:00",
        "last_updated_at": "2026-03-10T18:00:00+00:00",
        "closed_at": None,
    },
    {
        "case_id": 302, "passenger_id": 102, "flight_id": 202,
        "flight_number": "ZA2002", "pnr": "AVROBB",
        "case_status": "Escalated",
        "opened_at":      "2026-03-11T12:00:00+00:00",
        "last_updated_at": "2026-03-12T09:00:00+00:00",
        "closed_at": None,
    },
    {
        "case_id": 303, "passenger_id": 103, "flight_id": 203,
        "flight_number": "ZA2003", "pnr": "AVROCC",
        "case_status": "Resolved",
        "opened_at":      "2026-03-13T11:00:00+00:00",
        "last_updated_at": "2026-03-14T10:00:00+00:00",
        "closed_at":       "2026-03-14T10:00:00+00:00",
    },
]

SAMPLE_COMPLAINTS = [
    {
        "complaint_id": 401, "case_id": 301, "passenger_id": 101,
        "flight_id": 201, "flight_number": "ZA2001", "pnr": "AVROAA",
        "complaint_date": "2026-03-10T19:00:00+00:00",
        "category": "Flight Delay", "subcategory": "Departure Delay",
        "description": "Flight departed 15 minutes late causing me to miss my connecting train.",
        "severity": "Low", "status": "Open",
        "assigned_agent": None, "resolution_notes": None,
        "resolution_date": None, "satisfaction_score": None,
    },
    {
        "complaint_id": 402, "case_id": 302, "passenger_id": 102,
        "flight_id": 202, "flight_number": "ZA2002", "pnr": "AVROBB",
        "complaint_date": "2026-03-11T13:00:00+00:00",
        "category": "Flight Cancellation", "subcategory": "Compensation Not Provided",
        "description": "Flight ZA2002 was cancelled without any compensation offer or rebooking assistance.",
        "severity": "Critical", "status": "Escalated",
        "assigned_agent": "Maria Santos", "resolution_notes": None,
        "resolution_date": None, "satisfaction_score": None,
    },
    {
        "complaint_id": 403, "case_id": 303, "passenger_id": 103,
        "flight_id": 203, "flight_number": "ZA2003", "pnr": "AVROCC",
        "complaint_date": "2026-03-13T12:00:00+00:00",
        "category": "Flight Delay", "subcategory": "Arrival Delay",
        "description": "Arrived 55 minutes late. Missed hotel shuttle. ZavaAir provided a meal voucher.",
        "severity": "Medium", "status": "Resolved",
        "assigned_agent": "Tom Nguyen",
        "resolution_notes": "Meal voucher issued, apology letter sent.",
        "resolution_date": "2026-03-14T10:00:00+00:00",
        "satisfaction_score": 3.5,
    },
]


# ── CLI entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    # Also support importing the main seed/test data from kafka.producer
    try:
        from kafka.producer import (
            SEED_PASSENGERS, TEST_PASSENGERS, TEST2_PASSENGERS, TEST3_PASSENGERS,
            SEED_FLIGHTS,    TEST_FLIGHTS,    TEST2_FLIGHTS,    TEST3_FLIGHTS,
            SEED_CASES,      TEST_CASES,      TEST2_CASES,      TEST3_CASES,
            SEED_COMPLAINTS, TEST_COMPLAINTS, TEST2_COMPLAINTS, TEST3_COMPLAINTS,
        )
        _kafka_data_available = True
    except ImportError:
        _kafka_data_available = False

    ALL_DATASETS: dict[str, dict[str, list[dict]]] = {
        "sample": {
            "Passenger":  SAMPLE_PASSENGERS,
            "Flights":    SAMPLE_FLIGHTS,
            "case":       SAMPLE_CASES,
            "complaints": SAMPLE_COMPLAINTS,
        },
    }

    if _kafka_data_available:
        ALL_DATASETS.update({
            "seed": {
                "Passenger":  SEED_PASSENGERS,
                "Flights":    SEED_FLIGHTS,
                "case":       SEED_CASES,
                "complaints": SEED_COMPLAINTS,
            },
            "all": {
                "Passenger":  SEED_PASSENGERS + TEST_PASSENGERS + TEST2_PASSENGERS + TEST3_PASSENGERS,
                "Flights":    SEED_FLIGHTS    + TEST_FLIGHTS    + TEST2_FLIGHTS    + TEST3_FLIGHTS,
                "case":       SEED_CASES      + TEST_CASES      + TEST2_CASES      + TEST3_CASES,
                "complaints": SEED_COMPLAINTS + TEST_COMPLAINTS + TEST2_COMPLAINTS + TEST3_COMPLAINTS,
            },
        })

    parser = argparse.ArgumentParser(
        description="Produce ZavaAir data as Avro binary to Fabric Eventstream"
    )
    parser.add_argument(
        "--dataset",
        choices=list(ALL_DATASETS),
        default="sample",
        help="Which batch to send (default: sample – 3 records per type)",
    )
    parser.add_argument(
        "--types",
        nargs="+",
        choices=list(_TYPE_CONFIG),
        default=list(_TYPE_CONFIG),
        metavar="TYPE",
        help="Entity types to produce (default: all four)",
    )
    args = parser.parse_args()

    dataset = ALL_DATASETS[args.dataset]
    logger.info("Dataset: %s | format: Avro binary | types: %s",
                args.dataset, ", ".join(args.types))

    try:
        # FK-safe order: Passenger → Flights → case → complaints
        for entity_type in ["Passenger", "Flights", "case", "complaints"]:
            if entity_type in args.types:
                produce_records(entity_type, dataset[entity_type])
    except KafkaException as exc:
        logger.error("Kafka error: %s", exc)
        sys.exit(1)
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)
        sys.exit(1)
