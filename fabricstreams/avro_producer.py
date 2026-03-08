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
from utils import mask_key as _mask_key

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


_SCHEMA_REGISTRY_ENDPOINT = os.environ.get("FABRIC_SCHEMA_REGISTRY_ENDPOINT", "")
_SCHEMA_SET_ID = os.environ.get("FABRIC_SCHEMA_SET_ID", "")
_SCHEMA_REGISTRY_BASE = (
    f"{_SCHEMA_REGISTRY_ENDPOINT}/schemagroups/{_SCHEMA_SET_ID}/schemas"
    if _SCHEMA_REGISTRY_ENDPOINT and _SCHEMA_SET_ID else ""
)

# ce_type key → pk field, schema name in registry, local .avsc file
# aeh_schema_id: Azure Schema Registry GUID (32-char hex, no dashes) uploaded to
#   /subscriptions/.../namespaces/dmtcehns/schemagroups/complaints
_TYPE_CONFIG: dict[str, dict] = {
    "complaints": {"pk": "complaint_id", "schema": "complaints", "avsc": "complaints.avsc", "aeh_schema_id": "d9398b18215f48499c7879dad9348837"},
    "Flights":    {"pk": "flight_id",    "schema": "Flights",    "avsc": "flights.avsc",    "aeh_schema_id": "76c8e1475ffd453e9568e5d7101df22e"},
    "Passenger":  {"pk": "passenger_id", "schema": "Passenger",  "avsc": "passengers.avsc", "aeh_schema_id": "5025d146a7de49ea84829d9485819aee"},
    "case":       {"pk": "case_id",      "schema": "case",       "avsc": "cases.avsc",      "aeh_schema_id": "5dc623ffd5854a0f9978838dda225f9b"},
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

# ── Second connection (FABRIC_CONNECTION_STRING_2) ─────────────────────────────

_CONN_STR_2 = os.environ.get("FABRIC_CONNECTION_STRING_2", "")
if _CONN_STR_2:
    BOOTSTRAP_SERVER_2, TOPIC_2 = _parse_connection_string(_CONN_STR_2)
    _PRODUCER_CONFIG_2: dict = {
        "bootstrap.servers": BOOTSTRAP_SERVER_2,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism":    "PLAIN",
        "sasl.username":     "$ConnectionString",
        "sasl.password":     _CONN_STR_2,
        "acks":              "all",
        "retries":           3,
        "retry.backoff.ms":  1000,
    }
else:
    BOOTSTRAP_SERVER_2 = TOPIC_2 = ""
    _PRODUCER_CONFIG_2 = {}

# ── Third connection (Azure Event Hub – dmtcehns/custcomplaints) ────────────────

_AEH_CONN_STR = os.environ.get("AEH_CONNECTION_STRING", "")
if _AEH_CONN_STR:
    BOOTSTRAP_SERVER_AEH, TOPIC_AEH = _parse_connection_string(_AEH_CONN_STR)
    _PRODUCER_CONFIG_AEH: dict = {
        "bootstrap.servers": BOOTSTRAP_SERVER_AEH,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism":    "PLAIN",
        "sasl.username":     "$ConnectionString",
        "sasl.password":     _AEH_CONN_STR,
        "acks":              "all",
        "retries":           3,
        "retry.backoff.ms":  1000,
    }
else:
    BOOTSTRAP_SERVER_AEH = TOPIC_AEH = ""
    _PRODUCER_CONFIG_AEH = {}

# Azure Schema Registry wire format preamble (azure-schemaregistry-avroencoder)
_AEH_SR_PREAMBLE = b"\x00\x00\x00\x00"   # 4-byte format indicator for Avro

# ── Avro serialisation ─────────────────────────────────────────────────────────

def _to_avro_bytes(schema: dict, record: dict, schema_id: str = "") -> bytes:
    """Schemaless Avro binary (no container header). Used for Fabric Eventstream conn 1."""
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, record)
    return buf.getvalue()


def _to_avro_ocf_bytes(schema: dict, record: dict, schema_id: str = "") -> bytes:
    """Avro OCF with embedded schema. Fabric can identify format without external registry."""
    buf = io.BytesIO()
    fastavro.writer(buf, schema, [record], codec="null")
    return buf.getvalue()


def _to_aeh_avro_bytes(schema: dict, record: dict, schema_id: str = "") -> bytes:
    """Azure Schema Registry wire format: 4-byte preamble + 32-char schema GUID + schemaless Avro.

    Compatible with azure-schemaregistry-avroencoder and the AEH Fabric Eventstream source.
    schema_id must be the 32-char hex GUID (no dashes) from the AEH Schema Registry.
    """
    buf = io.BytesIO()
    buf.write(_AEH_SR_PREAMBLE)                    # \x00\x00\x00\x00
    buf.write(schema_id.encode("ascii"))            # 32 bytes
    fastavro.schemaless_writer(buf, schema, record)
    return buf.getvalue()


_CONNECTIONS: dict[str, dict] = {
    "1": {
        "config":     _PRODUCER_CONFIG,
        "topic":      TOPIC,
        "serializer": _to_avro_bytes,
        "fmt":        "Avro schemaless",
        "use_aeh_sr": False,
    },
    "2": {
        "config":     _PRODUCER_CONFIG_2,
        "topic":      TOPIC_2,
        "serializer": _to_avro_ocf_bytes,
        "fmt":        "Avro OCF",
        "use_aeh_sr": False,
    },
    "3": {
        "config":     _PRODUCER_CONFIG_AEH,
        "topic":      TOPIC_AEH,
        "serializer": _to_avro_bytes,
        "fmt":        "Avro schemaless",
        "use_aeh_sr": False,
    },
}


# ── Key masking ────────────────────────────────────────────────────────────────

def _mask_key(key) -> str:
    """Return a masked representation of a Kafka message key for safe logging."""
    if key is None:
        return "<null>"
    if isinstance(key, (bytes, bytearray)):
        key = key.decode("utf-8", errors="replace")
    s = str(key)
    n = min(4, max(0, len(s) // 2))
    return (s[:n] + "****") if n > 0 else "****"


# ── Delivery callback ──────────────────────────────────────────────────────────

def _on_delivery(err, msg) -> None:
    if err:
        logger.error(
            "Delivery FAILED | topic=%s partition=%s key=%s error=%s",
            msg.topic(), msg.partition(), _mask_key(msg.key()), err,
        )
    else:
        logger.debug(
            "Delivered | topic=%-60s partition=%d offset=%d key=%s",
            msg.topic(), msg.partition(), msg.offset(), _mask_key(msg.key()),
        )


# ── Core send helper ───────────────────────────────────────────────────────────

def _send(producer: Producer, topic: str, entity_type: str, record: dict,
          pk_field: str, schema_name: str, avro_schema: dict,
          serializer=_to_avro_bytes, schema_id: str = "") -> None:
    key   = str(record[pk_field]).encode("utf-8")
    value = serializer(avro_schema, record, schema_id)
    dataschema = f"{_SCHEMA_REGISTRY_BASE}/{schema_name}/versions/v1"

    headers = [
        ("ce_specversion",     b"1.0"),
        ("ce_id",              str(uuid.uuid4()).encode()),
        ("ce_type",            entity_type.encode()),
        ("ce_source",          b"fabricstreams/avro_producer"),
        ("ce_datacontenttype", b"application/avro"),
        ("ce_dataschema",      dataschema.encode()),
        # Plain header for easy Fabric Eventstream routing rules
        ("entity_type",        entity_type.encode()),
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

def produce_records(entity_type: str, records: list[dict], connection: str = "1") -> None:
    """Produce Avro-serialized records for the given entity type to Fabric Eventstream.

    Args:
        entity_type: One of the keys in _TYPE_CONFIG.
        records:     List of record dicts matching the Avro schema.
        connection:  Which endpoint to use – "1", "2", or "both".
    """
    cfg    = _TYPE_CONFIG[entity_type]
    schema = _AVRO_SCHEMAS[entity_type]

    conn_keys = {"both": ["1", "2"], "all": ["1", "2", "3"]}.get(connection, [connection])

    for conn_key in conn_keys:
        conn = _CONNECTIONS[conn_key]
        if not conn["config"]:
            raise EnvironmentError(
                f"Connection {conn_key} is not configured – check the relevant *_CONNECTION_STRING env var."
            )
        producer   = Producer(conn["config"])
        topic      = conn["topic"]
        serializer = conn["serializer"]
        fmt        = conn["fmt"]
        schema_id  = cfg["aeh_schema_id"] if conn["use_aeh_sr"] else ""
        logger.info(
            "Producer ready | conn=%s type=%-12s format=%-14s topic=%s records=%d",
            conn_key, entity_type, fmt, topic, len(records),
        )
        for row in records:
            _send(producer, topic, entity_type, row, cfg["pk"], cfg["schema"], schema,
                  serializer, schema_id)
        producer.flush()
        logger.info("conn=%s %s flushed ✓  (%d %s records)", conn_key, entity_type, len(records), fmt)


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
    {
        "passenger_id": 104, "first_name": "Sofia",   "last_name": "Petrov",
        "email": "sofia.petrov@example.ru", "phone": "+7-495-000-1234",
        "country": "Russia", "frequent_flyer_tier": "Bronze", "total_flights": 5,
        "member_since": "2025-11-20T00:00:00+00:00",
    },
    {
        "passenger_id": 105, "first_name": "Mohammed", "last_name": "Al-Rashid",
        "email": "m.alrashid@example.ae", "phone": "+971-50-999-8888",
        "country": "UAE", "frequent_flyer_tier": "Platinum", "total_flights": 212,
        "member_since": "2016-03-01T00:00:00+00:00",
    },
    {
        "passenger_id": 106, "first_name": "Priya",   "last_name": "Sharma",
        "email": "priya.sharma@example.in", "phone": "+91-98765-43210",
        "country": "India", "frequent_flyer_tier": "Gold", "total_flights": 65,
        "member_since": "2020-07-22T00:00:00+00:00",
    },
    {
        "passenger_id": 107, "first_name": "Carlos",  "last_name": "Mendez",
        "email": "carlos.mendez@example.br", "phone": None,
        "country": "Brazil", "frequent_flyer_tier": "Silver", "total_flights": 28,
        "member_since": "2022-04-05T00:00:00+00:00",
    },
    {
        "passenger_id": 108, "first_name": "Yuki",    "last_name": "Watanabe",
        "email": "yuki.watanabe@example.jp", "phone": "+81-90-2222-3333",
        "country": "Japan", "frequent_flyer_tier": "Bronze", "total_flights": 3,
        "member_since": "2026-01-15T00:00:00+00:00",
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
    {
        "flight_id": 204, "flight_number": "ZA2004",
        "origin_code": "DXB", "origin_city": "Dubai",
        "destination_code": "JFK", "destination_city": "New York",
        "scheduled_departure": "2026-03-15T02:00:00+00:00",
        "actual_departure":    "2026-03-15T02:05:00+00:00",
        "scheduled_arrival":   "2026-03-15T10:30:00+00:00",
        "actual_arrival":      "2026-03-15T10:35:00+00:00",
        "aircraft_type": "Boeing 777", "flight_status": "On Time", "delay_minutes": 5,
    },
    {
        "flight_id": 205, "flight_number": "ZA2005",
        "origin_code": "BOM", "origin_city": "Mumbai",
        "destination_code": "LHR", "destination_city": "London",
        "scheduled_departure": "2026-03-16T01:30:00+00:00",
        "actual_departure":    "2026-03-16T03:00:00+00:00",
        "scheduled_arrival":   "2026-03-16T07:00:00+00:00",
        "actual_arrival":      "2026-03-16T08:30:00+00:00",
        "aircraft_type": "Airbus A350", "flight_status": "Delayed", "delay_minutes": 90,
    },
    {
        "flight_id": 206, "flight_number": "ZA2006",
        "origin_code": "GRU", "origin_city": "São Paulo",
        "destination_code": "LIS", "destination_city": "Lisbon",
        "scheduled_departure": "2026-03-17T21:00:00+00:00",
        "actual_departure":    None,
        "scheduled_arrival":   "2026-03-18T11:00:00+00:00",
        "actual_arrival":      None,
        "aircraft_type": "Boeing 787", "flight_status": "Cancelled", "delay_minutes": 0,
    },
    {
        "flight_id": 207, "flight_number": "ZA2007",
        "origin_code": "DEL", "origin_city": "Delhi",
        "destination_code": "SIN", "destination_city": "Singapore",
        "scheduled_departure": "2026-03-18T05:45:00+00:00",
        "actual_departure":    "2026-03-18T05:50:00+00:00",
        "scheduled_arrival":   "2026-03-18T14:15:00+00:00",
        "actual_arrival":      "2026-03-18T14:20:00+00:00",
        "aircraft_type": "Airbus A380", "flight_status": "On Time", "delay_minutes": 5,
    },
    {
        "flight_id": 208, "flight_number": "ZA2008",
        "origin_code": "NRT", "origin_city": "Tokyo",
        "destination_code": "LAX", "destination_city": "Los Angeles",
        "scheduled_departure": "2026-03-19T11:00:00+00:00",
        "actual_departure":    "2026-03-19T11:25:00+00:00",
        "scheduled_arrival":   "2026-03-19T06:30:00+00:00",
        "actual_arrival":      "2026-03-19T06:55:00+00:00",
        "aircraft_type": "Boeing 777", "flight_status": "Delayed", "delay_minutes": 25,
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
    {
        "case_id": 304, "passenger_id": 104, "flight_id": 204,
        "flight_number": "ZA2004", "pnr": "AVRODD",
        "case_status": "Open",
        "opened_at":      "2026-03-15T14:00:00+00:00",
        "last_updated_at": "2026-03-15T14:00:00+00:00",
        "closed_at": None,
    },
    {
        "case_id": 305, "passenger_id": 105, "flight_id": 204,
        "flight_number": "ZA2004", "pnr": "AVROEE",
        "case_status": "Escalated",
        "opened_at":      "2026-03-15T15:30:00+00:00",
        "last_updated_at": "2026-03-16T08:00:00+00:00",
        "closed_at": None,
    },
    {
        "case_id": 306, "passenger_id": 106, "flight_id": 205,
        "flight_number": "ZA2005", "pnr": "AVROFF",
        "case_status": "Open",
        "opened_at":      "2026-03-16T10:00:00+00:00",
        "last_updated_at": "2026-03-16T10:00:00+00:00",
        "closed_at": None,
    },
    {
        "case_id": 307, "passenger_id": 107, "flight_id": 206,
        "flight_number": "ZA2006", "pnr": "AVROGG",
        "case_status": "Escalated",
        "opened_at":      "2026-03-17T22:00:00+00:00",
        "last_updated_at": "2026-03-18T11:00:00+00:00",
        "closed_at": None,
    },
    {
        "case_id": 308, "passenger_id": 108, "flight_id": 208,
        "flight_number": "ZA2008", "pnr": "AVROHH",
        "case_status": "Resolved",
        "opened_at":      "2026-03-19T09:00:00+00:00",
        "last_updated_at": "2026-03-20T12:00:00+00:00",
        "closed_at":       "2026-03-20T12:00:00+00:00",
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
    {
        "complaint_id": 404, "case_id": 304, "passenger_id": 104,
        "flight_id": 204, "flight_number": "ZA2004", "pnr": "AVRODD",
        "complaint_date": "2026-03-15T16:00:00+00:00",
        "category": "Baggage", "subcategory": "Lost Baggage",
        "description": "My checked bag did not arrive on ZA2004 DXB-JFK. It has been 24 hours with no update from the airline.",
        "severity": "High", "status": "Open",
        "assigned_agent": None, "resolution_notes": None,
        "resolution_date": None, "satisfaction_score": None,
    },
    {
        "complaint_id": 405, "case_id": 305, "passenger_id": 105,
        "flight_id": 204, "flight_number": "ZA2004", "pnr": "AVROEE",
        "complaint_date": "2026-03-15T17:00:00+00:00",
        "category": "Customer Service", "subcategory": "Upgrade Denied",
        "description": "As a Platinum member I was denied an upgrade despite seats being available. Ground staff were dismissive.",
        "severity": "High", "status": "Escalated",
        "assigned_agent": "Sara Kim", "resolution_notes": None,
        "resolution_date": None, "satisfaction_score": None,
    },
    {
        "complaint_id": 406, "case_id": 306, "passenger_id": 106,
        "flight_id": 205, "flight_number": "ZA2005", "pnr": "AVROFF",
        "complaint_date": "2026-03-16T11:00:00+00:00",
        "category": "Flight Delay", "subcategory": "EU261 Compensation",
        "description": "ZA2005 was delayed 90 minutes on a BOM-LHR route. I am entitled to EU261 compensation which has not been offered.",
        "severity": "High", "status": "Open",
        "assigned_agent": None, "resolution_notes": None,
        "resolution_date": None, "satisfaction_score": None,
    },
    {
        "complaint_id": 407, "case_id": 307, "passenger_id": 107,
        "flight_id": 206, "flight_number": "ZA2006", "pnr": "AVROGG",
        "complaint_date": "2026-03-17T23:00:00+00:00",
        "category": "Flight Cancellation", "subcategory": "Rebooking Not Offered",
        "description": "ZA2006 GRU-LIS was cancelled with less than 2 hours notice. No rebooking or hotel accommodation was offered.",
        "severity": "Critical", "status": "Escalated",
        "assigned_agent": "Diego Ferreira", "resolution_notes": None,
        "resolution_date": None, "satisfaction_score": None,
    },
    {
        "complaint_id": 408, "case_id": 308, "passenger_id": 108,
        "flight_id": 208, "flight_number": "ZA2008", "pnr": "AVROHH",
        "complaint_date": "2026-03-19T10:00:00+00:00",
        "category": "Flight Delay", "subcategory": "Arrival Delay",
        "description": "First time flying ZavaAir – ZA2008 NRT-LAX arrived 25 minutes late. Very disappointed.",
        "severity": "Low", "status": "Resolved",
        "assigned_agent": "Tom Nguyen",
        "resolution_notes": "Apology issued and 500 bonus miles credited to new account.",
        "resolution_date": "2026-03-20T12:00:00+00:00",
        "satisfaction_score": 2.0,
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
    parser.add_argument(
        "--connection",
        choices=["1", "2", "3", "both", "all"],
        default="1",
        help=(
            "Target endpoint(s): "
            "1=Fabric ES conn1 (schemaless Avro), "
            "2=Fabric ES conn2 (Avro OCF), "
            "3=Azure Event Hub dmtcehns (Avro+AzSR), "
            "both=1+2, all=1+2+3  (default: 1)"
        ),
    )
    args = parser.parse_args()

    dataset = ALL_DATASETS[args.dataset]
    logger.info("Dataset: %s | format: Avro binary | types: %s | connection: %s",
                args.dataset, ", ".join(args.types), args.connection)

    try:
        # FK-safe order: Passenger → Flights → case → complaints
        for entity_type in ["Passenger", "Flights", "case", "complaints"]:
            if entity_type in args.types:
                produce_records(entity_type, dataset[entity_type], connection=args.connection)
    except KafkaException as exc:
        logger.error("Kafka error: %s", exc)
        sys.exit(1)
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)
        sys.exit(1)
