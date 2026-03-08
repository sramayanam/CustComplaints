"""
producer.py – Event Hub Kafka producer for Lunar Air / ZavaAir complaint data
=============================================================================
Authentication  : SASL OAUTHBEARER via User-Assigned Managed Identity
                  (aaaorguamgdidentity – key-based auth disabled on namespace)
Topics produced : custcomplaints.passengers  (partition key: passenger_id)
                  custcomplaints.flights     (partition key: flight_id)
                  custcomplaints.cases       (partition key: case_id)
                  custcomplaints.complaints  (partition key: complaint_id)

Delivery guarantee
------------------
- acks=all (all in-sync replicas must acknowledge before a message is committed).
- enable.idempotence and compression are intentionally disabled: the Event Hubs
  Standard tier Kafka endpoint does not support the InitProducerId API (needed
  for idempotence) or compressed record batches; both cause UNSUPPORTED_FOR_
  MESSAGE_FORMAT (error 43) at the broker.
- Records are sent in FK dependency order:
    passengers → flights → cases → complaints
  This guarantees a consumer writing straight to PostgreSQL or Fabric will
  never encounter a FK violation when committing in offset order.

Partitioning
------------
The record's primary key is used as the Kafka message key (UTF-8).
This ensures all records for the same entity always land in the same partition,
which preserves ordering and makes idempotent upserts deterministic.

Message format
--------------
Each message is a JSON-encoded EventEnvelope (see schemas.py).
{
  "event_id":       "<uuidv4>",
  "table":          "custcomplaints.complaints",
  "schema_version": "1.0",
  "produced_at":    "2026-03-04T18:00:00+00:00",
  "payload": { ... all columns ... }
}

Usage
-----
  # Produce sample seed data (15 passengers, 10 flights, 15 cases, 20 complaints)
  python kafka/producer.py

  # Or import and call directly:
  from kafka.producer import produce_dataset
  produce_dataset(passengers, flights, cases, complaints)
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any

from confluent_kafka import Producer, KafkaException
from utils import mask_key as _mask_key

from .config import (
    PRODUCER_CONFIG,
    TOPIC_PASSENGERS,
    TOPIC_FLIGHTS,
    TOPIC_CASES,
    TOPIC_COMPLAINTS,
)
from .register_schemas import lookup_schema_ids
from .schemas import (
    PassengerPayload,
    FlightPayload,
    CasePayload,
    ComplaintPayload,
    make_envelope,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
logger = logging.getLogger(__name__)


# ── Key masking ────────────────────────────────────────────────────────────────

def _mask_key(key: Any) -> str:
    """Return a masked representation of a Kafka message key for safe logging."""
    if key is None:
        return "<null>"
    if isinstance(key, (bytes, bytearray)):
        key = key.decode("utf-8", errors="replace")
    s = str(key)
    n = min(4, max(0, len(s) // 2))
    return (s[:n] + "****") if n > 0 else "****"


# ── Delivery callback ──────────────────────────────────────────────────────────

def _on_delivery(err: Any, msg: Any) -> None:
    """Called by librdkafka on broker ACK or terminal error."""
    if err:
        logger.error(
            "Delivery FAILED | topic=%s partition=%s key=%s error=%s",
            msg.topic(), msg.partition(), _mask_key(msg.key()), err,
        )
    else:
        logger.debug(
            "Delivered | topic=%-40s partition=%d offset=%d key=%s",
            msg.topic(), msg.partition(), msg.offset(), _mask_key(msg.key()),
        )


# ── Core send helper ──────────────────────────────────────────────────────────

def _send(
    producer: Producer,
    topic: str,
    key: str,
    payload_model: Any,
    schema_id: str | None = None,
) -> None:
    """
    Validate, serialise and produce one record.

    Parameters
    ----------
    producer      : confluent_kafka Producer instance
    topic         : destination Event Hub name
    key           : Kafka message key (primary-key value as string)
    payload_model : a Pydantic BaseModel instance (PassengerPayload etc.)
    schema_id     : schema ID from the Event Hubs Schema Registry.  When
                    provided, attached as a "schema-id" Kafka message header
                    so consumers can resolve the schema without inspecting
                    the topic name.  Format: 32-char lowercase hex UUID.
    """
    table_name = topic  # table == topic by convention
    envelope = make_envelope(table_name, payload_model, pk=key)
    value_bytes = json.dumps(envelope.model_dump(), ensure_ascii=False).encode("utf-8")
    key_bytes = key.encode("utf-8")
    # Kafka headers: list of (name, value) byte-tuples.
    # "schema-id" is the convention used by the Azure Schema Registry SDK
    # and Event Hubs documentation for JSON-format schemas.
    headers = [("schema-id", schema_id.encode("utf-8"))] if schema_id else []

    while True:
        try:
            producer.produce(
                topic=topic,
                key=key_bytes,
                value=value_bytes,
                headers=headers,
                on_delivery=_on_delivery,
            )
            break
        except BufferError:
            # Local queue full – poll to drain then retry
            logger.warning("Producer queue full, polling to drain…")
            producer.poll(0.5)


# ── Dataset producer ──────────────────────────────────────────────────────────

def produce_dataset(
    passengers: list[dict],
    flights:    list[dict],
    cases:      list[dict],
    complaints: list[dict],
) -> None:
    """
    Produce all four tables to Event Hubs in FK-safe order.

    Records are validated against the Pydantic schemas before serialisation –
    any schema violation raises immediately so bad data never reaches the bus.

    Parameters
    ----------
    passengers  : list of dicts matching custcomplaints.passengers columns
    flights     : list of dicts matching custcomplaints.flights columns
    cases       : list of dicts matching custcomplaints.cases columns
    complaints  : list of dicts matching custcomplaints.complaints columns
    """
    producer = Producer(PRODUCER_CONFIG)
    logger.info("Producer created. Bootstrap: %s", PRODUCER_CONFIG["bootstrap.servers"])

    # Resolve schema IDs from Event Hubs Schema Registry once at startup.
    # Fails fast if any schema is missing – run `python -m kafka.register_schemas`
    # first if you haven't registered them yet.
    logger.info("Resolving schema IDs from Event Hubs Schema Registry…")
    schema_ids = lookup_schema_ids()
    logger.info(
        "Schema IDs resolved | passengers=%s flights=%s cases=%s complaints=%s",
        schema_ids[TOPIC_PASSENGERS], schema_ids[TOPIC_FLIGHTS],
        schema_ids[TOPIC_CASES],      schema_ids[TOPIC_COMPLAINTS],
    )

    # ── 1. passengers (no FK) ────────────────────────────────────────────────
    logger.info("Producing %d passenger records → %s", len(passengers), TOPIC_PASSENGERS)
    for row in passengers:
        model = PassengerPayload(**row)
        _send(producer, TOPIC_PASSENGERS, str(model.passenger_id), model,
              schema_id=schema_ids[TOPIC_PASSENGERS])
    producer.flush()
    logger.info("passengers flushed ✓")

    # ── 2. flights (no FK) ──────────────────────────────────────────────────
    logger.info("Producing %d flight records → %s", len(flights), TOPIC_FLIGHTS)
    for row in flights:
        model = FlightPayload(**row)
        _send(producer, TOPIC_FLIGHTS, str(model.flight_id), model,
              schema_id=schema_ids[TOPIC_FLIGHTS])
    producer.flush()
    logger.info("flights flushed ✓")

    # ── 3. cases (FK → passengers, flights) ─────────────────────────────────
    logger.info("Producing %d case records → %s", len(cases), TOPIC_CASES)
    for row in cases:
        model = CasePayload(**row)
        _send(producer, TOPIC_CASES, str(model.case_id), model,
              schema_id=schema_ids[TOPIC_CASES])
    producer.flush()
    logger.info("cases flushed ✓")

    # ── 4. complaints (FK → cases, passengers, flights) ──────────────────────
    logger.info("Producing %d complaint records → %s", len(complaints), TOPIC_COMPLAINTS)
    for row in complaints:
        model = ComplaintPayload(**row)
        _send(producer, TOPIC_COMPLAINTS, str(model.complaint_id), model,
              schema_id=schema_ids[TOPIC_COMPLAINTS])
    producer.flush()
    logger.info("complaints flushed ✓")

    logger.info("All records delivered.")


# ── Seed data (mirrors 03_inserts.sql) ────────────────────────────────────────
# SYNTHETIC DATA NOTICE: All records below — including names, email addresses,
# phone numbers, flight details, and complaint descriptions — are entirely
# fictitious and were generated by a Large Language Model (LLM) for
# demonstration purposes only. They do not represent real individuals, airlines,
# flights, or customer complaints. Any resemblance to real people or events is
# purely coincidental.
# Timestamps are ISO-8601 strings with UTC offset – roundtrip-safe for
# PostgreSQL TIMESTAMPTZ and Microsoft Fabric TIMESTAMP columns.

SEED_PASSENGERS: list[dict] = [
    {"passenger_id": 1,  "first_name": "Ava",     "last_name": "Carter",   "email": "ava.carter@example.com",     "phone": "+1-555-0101", "country": "United States",  "frequent_flyer_tier": "Silver",   "total_flights": 42,  "member_since": "2019-01-10T00:00:00+00:00"},
    {"passenger_id": 2,  "first_name": "Noah",    "last_name": "Patel",    "email": "noah.patel@example.com",     "phone": "+1-555-0102", "country": "Canada",          "frequent_flyer_tier": "Gold",     "total_flights": 87,  "member_since": "2017-06-14T00:00:00+00:00"},
    {"passenger_id": 3,  "first_name": "Mia",     "last_name": "Lopez",    "email": "mia.lopez@example.com",      "phone": "+1-555-0103", "country": "Mexico",          "frequent_flyer_tier": "Bronze",   "total_flights": 19,  "member_since": "2022-03-03T00:00:00+00:00"},
    {"passenger_id": 4,  "first_name": "Liam",    "last_name": "Kim",      "email": "liam.kim@example.com",       "phone": "+1-555-0104", "country": "United States",  "frequent_flyer_tier": "None",     "total_flights": 8,   "member_since": "2024-02-01T00:00:00+00:00"},
    {"passenger_id": 5,  "first_name": "Emma",    "last_name": "Brown",    "email": "emma.brown@example.com",     "phone": "+1-555-0105", "country": "United Kingdom", "frequent_flyer_tier": "Platinum", "total_flights": 120, "member_since": "2015-09-22T00:00:00+00:00"},
    {"passenger_id": 6,  "first_name": "Amara",   "last_name": "Adeyemi",  "email": "amara.adeyemi@example.com",  "phone": "+1-555-0106", "country": "Nigeria",         "frequent_flyer_tier": "Gold",     "total_flights": 63,  "member_since": "2018-11-05T00:00:00+00:00"},
    {"passenger_id": 7,  "first_name": "Gabriel", "last_name": "Flores",   "email": "gabriel.flores@example.com", "phone": "+1-555-0107", "country": "Brazil",          "frequent_flyer_tier": "Silver",   "total_flights": 31,  "member_since": "2021-07-19T00:00:00+00:00"},
    {"passenger_id": 8,  "first_name": "Priya",   "last_name": "Hassan",   "email": "priya.hassan@example.com",   "phone": "+1-555-0108", "country": "India",           "frequent_flyer_tier": "Bronze",   "total_flights": 12,  "member_since": "2023-04-28T00:00:00+00:00"},
    {"passenger_id": 9,  "first_name": "Kenji",   "last_name": "Iwata",    "email": "kenji.iwata@example.com",    "phone": "+1-555-0109", "country": "Japan",           "frequent_flyer_tier": "Platinum", "total_flights": 205, "member_since": "2012-08-31T00:00:00+00:00"},
    {"passenger_id": 10, "first_name": "Marcus",  "last_name": "Jensen",   "email": "marcus.jensen@example.com",  "phone": "+1-555-0110", "country": "Denmark",         "frequent_flyer_tier": "Gold",     "total_flights": 74,  "member_since": "2016-02-17T00:00:00+00:00"},
    {"passenger_id": 11, "first_name": "Chloe",   "last_name": "Kim",      "email": "chloe.kim@example.com",      "phone": "+1-555-0111", "country": "South Korea",     "frequent_flyer_tier": "Silver",   "total_flights": 38,  "member_since": "2020-05-09T00:00:00+00:00"},
    {"passenger_id": 12, "first_name": "Oliver",  "last_name": "Leclerc",  "email": "oliver.leclerc@example.com", "phone": "+1-555-0112", "country": "France",          "frequent_flyer_tier": "Bronze",   "total_flights": 7,   "member_since": "2024-09-14T00:00:00+00:00"},
    {"passenger_id": 13, "first_name": "Fatima",  "last_name": "Müller",   "email": "fatima.muller@example.com",  "phone": "+1-555-0113", "country": "Germany",         "frequent_flyer_tier": "None",     "total_flights": 3,   "member_since": "2025-01-22T00:00:00+00:00"},
    {"passenger_id": 14, "first_name": "Diego",   "last_name": "Nakamura", "email": "diego.nakamura@example.com", "phone": "+1-555-0114", "country": "Argentina",       "frequent_flyer_tier": "Silver",   "total_flights": 28,  "member_since": "2021-03-11T00:00:00+00:00"},
    {"passenger_id": 15, "first_name": "Elif",    "last_name": "Okafor",   "email": "elif.okafor@example.com",    "phone": "+1-555-0115", "country": "Turkey",          "frequent_flyer_tier": "Gold",     "total_flights": 55,  "member_since": "2018-06-25T00:00:00+00:00"},
    {"passenger_id": 16, "first_name": "Aiko",    "last_name": "Suzuki",   "email": "aiko.suzuki@example.com",    "phone": "+1-555-0116", "country": "Japan",           "frequent_flyer_tier": "Bronze",   "total_flights": 14,  "member_since": "2023-08-11T00:00:00+00:00"},
    {"passenger_id": 17, "first_name": "Carlos",  "last_name": "Rivera",   "email": "carlos.rivera@example.com",  "phone": "+1-555-0117", "country": "Colombia",        "frequent_flyer_tier": "None",     "total_flights": 2,   "member_since": "2025-11-01T00:00:00+00:00"},
    {"passenger_id": 18, "first_name": "Ingrid",  "last_name": "Hansen",   "email": "ingrid.hansen@example.com",  "phone": "+1-555-0118", "country": "Norway",          "frequent_flyer_tier": "Silver",   "total_flights": 33,  "member_since": "2020-07-04T00:00:00+00:00"},
    {"passenger_id": 19, "first_name": "Tariq",   "last_name": "Ahmed",    "email": "tariq.ahmed@example.com",    "phone": "+1-555-0119", "country": "Pakistan",        "frequent_flyer_tier": "Gold",     "total_flights": 71,  "member_since": "2016-12-09T00:00:00+00:00"},
    {"passenger_id": 20, "first_name": "Sofia",   "last_name": "Esposito", "email": "sofia.esposito@example.com", "phone": "+1-555-0120", "country": "Italy",           "frequent_flyer_tier": "Platinum", "total_flights": 156, "member_since": "2013-05-17T00:00:00+00:00"},
]

SEED_FLIGHTS: list[dict] = [
    {"flight_id": 1,  "flight_number": "ZA101",  "origin_code": "JFK", "origin_city": "New York",    "destination_code": "LAX", "destination_city": "Los Angeles", "scheduled_departure": "2026-03-01T12:00:00+00:00", "actual_departure": "2026-03-01T12:20:00+00:00", "scheduled_arrival": "2026-03-01T18:00:00+00:00", "actual_arrival": "2026-03-01T18:20:00+00:00", "aircraft_type": "Crescent 787-9",   "flight_status": "Delayed",   "delay_minutes": 20},
    {"flight_id": 2,  "flight_number": "ZA202",  "origin_code": "LAX", "origin_city": "Los Angeles", "destination_code": "SEA", "destination_city": "Seattle",     "scheduled_departure": "2026-03-02T09:00:00+00:00", "actual_departure": "2026-03-02T09:00:00+00:00", "scheduled_arrival": "2026-03-02T11:30:00+00:00", "actual_arrival": "2026-03-02T11:30:00+00:00", "aircraft_type": "Selene A320",      "flight_status": "On Time",   "delay_minutes": 0},
    {"flight_id": 3,  "flight_number": "ZA303",  "origin_code": "SEA", "origin_city": "Seattle",     "destination_code": "ORD", "destination_city": "Chicago",     "scheduled_departure": "2026-03-02T14:00:00+00:00", "actual_departure": None,                        "scheduled_arrival": "2026-03-02T18:00:00+00:00", "actual_arrival": None,                        "aircraft_type": "Zava Cruiser 737", "flight_status": "Scheduled", "delay_minutes": 0},
    {"flight_id": 4,  "flight_number": "ZA404",  "origin_code": "ORD", "origin_city": "Chicago",     "destination_code": "MIA", "destination_city": "Miami",       "scheduled_departure": "2026-03-02T16:15:00+00:00", "actual_departure": None,                        "scheduled_arrival": "2026-03-02T21:15:00+00:00", "actual_arrival": None,                        "aircraft_type": "Tycho 777X",       "flight_status": "Cancelled", "delay_minutes": 0},
    {"flight_id": 5,  "flight_number": "ZA505",  "origin_code": "MIA", "origin_city": "Miami",       "destination_code": "JFK", "destination_city": "New York",    "scheduled_departure": "2026-03-03T08:00:00+00:00", "actual_departure": "2026-03-03T08:00:00+00:00", "scheduled_arrival": "2026-03-03T11:15:00+00:00", "actual_arrival": "2026-03-03T11:15:00+00:00", "aircraft_type": "Selene A320",      "flight_status": "On Time",   "delay_minutes": 0},
    {"flight_id": 6,  "flight_number": "ZA606",  "origin_code": "DEN", "origin_city": "Denver",      "destination_code": "LAX", "destination_city": "Los Angeles", "scheduled_departure": "2026-03-03T11:30:00+00:00", "actual_departure": "2026-03-03T13:05:00+00:00", "scheduled_arrival": "2026-03-03T13:00:00+00:00", "actual_arrival": "2026-03-03T14:35:00+00:00", "aircraft_type": "Zava Cruiser 737", "flight_status": "Delayed",   "delay_minutes": 95},
    {"flight_id": 7,  "flight_number": "ZA707",  "origin_code": "JFK", "origin_city": "New York",    "destination_code": "LHR", "destination_city": "London",      "scheduled_departure": "2026-03-04T18:00:00+00:00", "actual_departure": "2026-03-04T18:00:00+00:00", "scheduled_arrival": "2026-03-05T06:00:00+00:00", "actual_arrival": "2026-03-05T06:00:00+00:00", "aircraft_type": "Moonbeam A350",    "flight_status": "On Time",   "delay_minutes": 0},
    {"flight_id": 8,  "flight_number": "ZA808",  "origin_code": "LAX", "origin_city": "Los Angeles", "destination_code": "NRT", "destination_city": "Tokyo",       "scheduled_departure": "2026-03-05T22:00:00+00:00", "actual_departure": "2026-03-05T22:45:00+00:00", "scheduled_arrival": "2026-03-07T04:00:00+00:00", "actual_arrival": "2026-03-07T04:45:00+00:00", "aircraft_type": "Moonbeam A350",    "flight_status": "Delayed",   "delay_minutes": 45},
    {"flight_id": 9,  "flight_number": "ZA909",  "origin_code": "ORD", "origin_city": "Chicago",     "destination_code": "DEN", "destination_city": "Denver",      "scheduled_departure": "2026-03-06T13:00:00+00:00", "actual_departure": "2026-03-06T13:00:00+00:00", "scheduled_arrival": "2026-03-06T15:30:00+00:00", "actual_arrival": "2026-03-06T15:30:00+00:00", "aircraft_type": "Crescent 787-9",   "flight_status": "On Time",   "delay_minutes": 0},
    {"flight_id": 10, "flight_number": "ZA1010", "origin_code": "SEA", "origin_city": "Seattle",     "destination_code": "MIA", "destination_city": "Miami",       "scheduled_departure": "2026-03-07T07:45:00+00:00", "actual_departure": "2026-03-07T08:10:00+00:00", "scheduled_arrival": "2026-03-07T16:30:00+00:00", "actual_arrival": "2026-03-07T19:05:00+00:00", "aircraft_type": "Tycho 777X",       "flight_status": "Diverted",  "delay_minutes": 155},
    {"flight_id": 11, "flight_number": "ZA1111", "origin_code": "LHR", "origin_city": "London",       "destination_code": "DXB", "destination_city": "Dubai",        "scheduled_departure": "2026-03-08T10:00:00+00:00", "actual_departure": "2026-03-08T10:00:00+00:00", "scheduled_arrival": "2026-03-08T19:00:00+00:00", "actual_arrival": "2026-03-08T19:00:00+00:00", "aircraft_type": "Moonbeam A350",    "flight_status": "On Time",   "delay_minutes": 0},
    {"flight_id": 12, "flight_number": "ZA1212", "origin_code": "DXB", "origin_city": "Dubai",         "destination_code": "SIN", "destination_city": "Singapore",    "scheduled_departure": "2026-03-09T01:00:00+00:00", "actual_departure": "2026-03-09T01:30:00+00:00", "scheduled_arrival": "2026-03-09T13:00:00+00:00", "actual_arrival": "2026-03-09T13:30:00+00:00", "aircraft_type": "Crescent 787-9",   "flight_status": "Delayed",   "delay_minutes": 30},
    {"flight_id": 13, "flight_number": "ZA1313", "origin_code": "JFK", "origin_city": "New York",     "destination_code": "CDG", "destination_city": "Paris",         "scheduled_departure": "2026-03-09T21:00:00+00:00", "actual_departure": None,                        "scheduled_arrival": "2026-03-10T09:00:00+00:00", "actual_arrival": None,                        "aircraft_type": "Moonbeam A350",    "flight_status": "Scheduled", "delay_minutes": 0},
]

SEED_CASES: list[dict] = [
    {"case_id": 1,  "passenger_id": 1,  "flight_id": 1,  "flight_number": "ZA101",  "pnr": "PNR1001", "case_status": "Open",         "opened_at": "2026-03-01T13:00:00+00:00", "last_updated_at": "2026-03-01T14:00:00+00:00", "closed_at": None},
    {"case_id": 2,  "passenger_id": 2,  "flight_id": 2,  "flight_number": "ZA202",  "pnr": "PNR2002", "case_status": "Under Review",  "opened_at": "2026-03-02T09:15:00+00:00", "last_updated_at": "2026-03-02T10:20:00+00:00", "closed_at": None},
    {"case_id": 3,  "passenger_id": 3,  "flight_id": 4,  "flight_number": "ZA404",  "pnr": "PNR3003", "case_status": "Resolved",      "opened_at": "2026-02-27T11:00:00+00:00", "last_updated_at": "2026-02-28T12:00:00+00:00", "closed_at": "2026-02-28T12:00:00+00:00"},
    {"case_id": 4,  "passenger_id": 4,  "flight_id": 3,  "flight_number": "ZA303",  "pnr": "PNR4004", "case_status": "Open",         "opened_at": "2026-03-02T15:00:00+00:00", "last_updated_at": "2026-03-02T15:00:00+00:00", "closed_at": None},
    {"case_id": 5,  "passenger_id": 5,  "flight_id": 7,  "flight_number": "ZA707",  "pnr": "PNR5005", "case_status": "Escalated",    "opened_at": "2026-03-04T19:30:00+00:00", "last_updated_at": "2026-03-05T08:00:00+00:00", "closed_at": None},
    {"case_id": 6,  "passenger_id": 1,  "flight_id": 5,  "flight_number": "ZA505",  "pnr": "PNR6006", "case_status": "Under Review",  "opened_at": "2026-03-03T12:00:00+00:00", "last_updated_at": "2026-03-03T14:00:00+00:00", "closed_at": None},
    {"case_id": 7,  "passenger_id": 6,  "flight_id": 6,  "flight_number": "ZA606",  "pnr": "PNR7007", "case_status": "Open",         "opened_at": "2026-03-03T14:00:00+00:00", "last_updated_at": "2026-03-03T14:00:00+00:00", "closed_at": None},
    {"case_id": 8,  "passenger_id": 7,  "flight_id": 8,  "flight_number": "ZA808",  "pnr": "PNR8008", "case_status": "Under Review",  "opened_at": "2026-03-06T05:00:00+00:00", "last_updated_at": "2026-03-06T06:30:00+00:00", "closed_at": None},
    {"case_id": 9,  "passenger_id": 8,  "flight_id": 1,  "flight_number": "ZA101",  "pnr": "PNR9009", "case_status": "Resolved",      "opened_at": "2026-03-01T19:00:00+00:00", "last_updated_at": "2026-03-02T10:00:00+00:00", "closed_at": "2026-03-02T10:00:00+00:00"},
    {"case_id": 10, "passenger_id": 9,  "flight_id": 9,  "flight_number": "ZA909",  "pnr": "PNR1010", "case_status": "Open",         "opened_at": "2026-03-06T16:00:00+00:00", "last_updated_at": "2026-03-06T16:00:00+00:00", "closed_at": None},
    {"case_id": 11, "passenger_id": 10, "flight_id": 10, "flight_number": "ZA1010", "pnr": "PNR1011", "case_status": "Escalated",    "opened_at": "2026-03-07T20:00:00+00:00", "last_updated_at": "2026-03-08T09:00:00+00:00", "closed_at": None},
    {"case_id": 12, "passenger_id": 11, "flight_id": 2,  "flight_number": "ZA202",  "pnr": "PNR1012", "case_status": "Open",         "opened_at": "2026-03-02T12:00:00+00:00", "last_updated_at": "2026-03-02T12:00:00+00:00", "closed_at": None},
    {"case_id": 13, "passenger_id": 12, "flight_id": 3,  "flight_number": "ZA303",  "pnr": "PNR1013", "case_status": "Under Review",  "opened_at": "2026-03-02T16:30:00+00:00", "last_updated_at": "2026-03-02T17:00:00+00:00", "closed_at": None},
    {"case_id": 14, "passenger_id": 14, "flight_id": 5,  "flight_number": "ZA505",  "pnr": "PNR1014", "case_status": "Open",         "opened_at": "2026-03-03T11:45:00+00:00", "last_updated_at": "2026-03-03T11:45:00+00:00", "closed_at": None},
    {"case_id": 15, "passenger_id": 15, "flight_id": 7,  "flight_number": "ZA707",  "pnr": "PNR1015", "case_status": "Escalated",     "opened_at": "2026-03-04T20:00:00+00:00", "last_updated_at": "2026-03-05T10:00:00+00:00", "closed_at": None},
    {"case_id": 16, "passenger_id": 16, "flight_id": 11, "flight_number": "ZA1111", "pnr": "PNR1016", "case_status": "Open",          "opened_at": "2026-03-08T20:00:00+00:00", "last_updated_at": "2026-03-08T20:00:00+00:00", "closed_at": None},
    {"case_id": 17, "passenger_id": 17, "flight_id": 12, "flight_number": "ZA1212", "pnr": "PNR1017", "case_status": "Open",          "opened_at": "2026-03-09T14:00:00+00:00", "last_updated_at": "2026-03-09T14:00:00+00:00", "closed_at": None},
    {"case_id": 18, "passenger_id": 18, "flight_id": 9,  "flight_number": "ZA909",  "pnr": "PNR1018", "case_status": "Resolved",      "opened_at": "2026-03-06T17:00:00+00:00", "last_updated_at": "2026-03-07T09:00:00+00:00", "closed_at": "2026-03-07T09:00:00+00:00"},
    {"case_id": 19, "passenger_id": 19, "flight_id": 8,  "flight_number": "ZA808",  "pnr": "PNR1019", "case_status": "Escalated",     "opened_at": "2026-03-07T05:00:00+00:00", "last_updated_at": "2026-03-08T10:00:00+00:00", "closed_at": None},
    {"case_id": 20, "passenger_id": 20, "flight_id": 7,  "flight_number": "ZA707",  "pnr": "PNR1020", "case_status": "Under Review",  "opened_at": "2026-03-05T12:00:00+00:00", "last_updated_at": "2026-03-05T15:00:00+00:00", "closed_at": None},
]

SEED_COMPLAINTS: list[dict] = [
    {"complaint_id": 1,  "case_id": 1,  "passenger_id": 1,  "flight_id": 1,  "flight_number": "ZA101",  "pnr": "PNR1001", "complaint_date": "2026-03-01T13:05:00+00:00", "category": "Baggage",            "subcategory": "Delayed Baggage",      "description": "My checked bag did not appear on the carousel for over 90 minutes after landing. No staff could provide an update.",                                       "severity": "High",     "status": "Open",         "assigned_agent": "Selene Park",  "resolution_notes": None,                                                                             "resolution_date": None,                     "satisfaction_score": None},
    {"complaint_id": 2,  "case_id": 1,  "passenger_id": 1,  "flight_id": 1,  "flight_number": "ZA101",  "pnr": "PNR1001", "complaint_date": "2026-03-01T13:50:00+00:00", "category": "Customer Service",   "subcategory": "Staff Attitude",       "description": "Ground staff at carousel desk was dismissive and refused to file a formal delayed baggage report.",                                                   "severity": "Medium",   "status": "Under Review", "assigned_agent": "Selene Park",  "resolution_notes": None,                                                                             "resolution_date": None,                     "satisfaction_score": None},
    {"complaint_id": 3,  "case_id": 2,  "passenger_id": 2,  "flight_id": 2,  "flight_number": "ZA202",  "pnr": "PNR2002", "complaint_date": "2026-03-02T09:20:00+00:00", "category": "In-Flight Service",  "subcategory": "Food Quality",         "description": "Meal served cold in the business cabin. The pasta appeared to have been left out far too long.",                                                       "severity": "Medium",   "status": "Under Review", "assigned_agent": "Orion Bailey", "resolution_notes": None,                                                                             "resolution_date": None,                     "satisfaction_score": None},
    {"complaint_id": 4,  "case_id": 2,  "passenger_id": 2,  "flight_id": 2,  "flight_number": "ZA202",  "pnr": "PNR2002", "complaint_date": "2026-03-02T10:00:00+00:00", "category": "Seating",            "subcategory": "Seat Malfunction",     "description": "Seat 3A did not recline for the entire flight. Crew acknowledged the issue but could not fix it.",                                                    "severity": "High",     "status": "Escalated",    "assigned_agent": "Orion Bailey", "resolution_notes": None,                                                                             "resolution_date": None,                     "satisfaction_score": None},
    {"complaint_id": 5,  "case_id": 3,  "passenger_id": 3,  "flight_id": 4,  "flight_number": "ZA404",  "pnr": "PNR3003", "complaint_date": "2026-02-27T11:10:00+00:00", "category": "Flight Operations",  "subcategory": "Flight Cancellation",  "description": "Cancellation notice was sent only 45 minutes before departure, leaving insufficient time to arrange alternative travel.",                              "severity": "High",     "status": "Resolved",     "assigned_agent": "Cosmo Lee",    "resolution_notes": "ZavaAir issued full compensation and confirmed passenger on next available flight.", "resolution_date": "2026-02-28T12:00:00+00:00", "satisfaction_score": 4.0},
    {"complaint_id": 6,  "case_id": 4,  "passenger_id": 4,  "flight_id": 3,  "flight_number": "ZA303",  "pnr": "PNR4004", "complaint_date": "2026-03-02T15:10:00+00:00", "category": "Seating",            "subcategory": "Upgrade Not Applied",  "description": "Requested upgrade to premium economy was confirmed at check-in but seat was given to another passenger when boarding.",                               "severity": "Medium",   "status": "Open",         "assigned_agent": "Selene Park",  "resolution_notes": None,                                                                             "resolution_date": None,                     "satisfaction_score": None},
    {"complaint_id": 7,  "case_id": 5,  "passenger_id": 5,  "flight_id": 7,  "flight_number": "ZA707",  "pnr": "PNR5005", "complaint_date": "2026-03-04T19:40:00+00:00", "category": "In-Flight Service",  "subcategory": "Crew Behaviour",       "description": "Crew member was visibly rude to a passenger in 8B and refused to bring water after multiple polite requests during a 7-hour transatlantic flight.",    "severity": "Critical", "status": "Escalated",    "assigned_agent": "Orion Bailey", "resolution_notes": None,                                                                             "resolution_date": None,                     "satisfaction_score": None},
    {"complaint_id": 8,  "case_id": 5,  "passenger_id": 5,  "flight_id": 7,  "flight_number": "ZA707",  "pnr": "PNR5005", "complaint_date": "2026-03-05T08:30:00+00:00", "category": "Customer Service",   "subcategory": "Complaint Not Logged", "description": "Called ZavaAir support after landing and was told no incident record existed. Representative refused to open a case.",                               "severity": "High",     "status": "Escalated",    "assigned_agent": "Orion Bailey", "resolution_notes": None,                                                                             "resolution_date": None,                     "satisfaction_score": None},
    {"complaint_id": 9,  "case_id": 6,  "passenger_id": 1,  "flight_id": 5,  "flight_number": "ZA505",  "pnr": "PNR6006", "complaint_date": "2026-03-03T12:10:00+00:00", "category": "Booking",            "subcategory": "Refund Delay",         "description": "Requested refund for cancelled ancillary service 11 days ago. No response received and refund has not appeared on card statement.",                    "severity": "Medium",   "status": "Under Review", "assigned_agent": "Cosmo Lee",    "resolution_notes": None,                                                                             "resolution_date": None,                     "satisfaction_score": None},
    {"complaint_id": 10, "case_id": 7,  "passenger_id": 6,  "flight_id": 6,  "flight_number": "ZA606",  "pnr": "PNR7007", "complaint_date": "2026-03-03T14:15:00+00:00", "category": "Flight Operations",  "subcategory": "Excessive Delay",      "description": "Flight ZA606 was delayed by 95 minutes with no communication from the gate. Gate agent left without making any announcements.",                        "severity": "High",     "status": "Open",         "assigned_agent": "Selene Park",  "resolution_notes": None,                                                                             "resolution_date": None,                     "satisfaction_score": None},
    {"complaint_id": 11, "case_id": 7,  "passenger_id": 6,  "flight_id": 6,  "flight_number": "ZA606",  "pnr": "PNR7007", "complaint_date": "2026-03-03T15:00:00+00:00", "category": "Baggage",            "subcategory": "Damaged Baggage",      "description": "Hard-shell suitcase arrived with a broken wheel and cracked zipper housing after the delayed ZA606 flight.",                                           "severity": "Medium",   "status": "Open",         "assigned_agent": "Selene Park",  "resolution_notes": None,                                                                             "resolution_date": None,                     "satisfaction_score": None},
    {"complaint_id": 12, "case_id": 8,  "passenger_id": 7,  "flight_id": 8,  "flight_number": "ZA808",  "pnr": "PNR8008", "complaint_date": "2026-03-06T05:30:00+00:00", "category": "Booking",            "subcategory": "Seat Change",          "description": "Booked window seat 22A six months in advance. At check-in was moved to middle seat 34E with no explanation.",                                         "severity": "Medium",   "status": "Under Review", "assigned_agent": "Orion Bailey", "resolution_notes": None,                                                                             "resolution_date": None,                     "satisfaction_score": None},
    {"complaint_id": 13, "case_id": 9,  "passenger_id": 8,  "flight_id": 1,  "flight_number": "ZA101",  "pnr": "PNR9009", "complaint_date": "2026-03-01T19:10:00+00:00", "category": "Special Assistance", "subcategory": "Wheelchair Not Ready", "description": "Requested wheelchair assistance at check-in. No wheelchair was available at the gate causing significant distress.",                                   "severity": "Critical", "status": "Resolved",     "assigned_agent": "Cosmo Lee",    "resolution_notes": "ZavaAir confirmed procedural failure and issued formal apology and travel voucher.",  "resolution_date": "2026-03-02T10:00:00+00:00", "satisfaction_score": 3.5},
    {"complaint_id": 14, "case_id": 10, "passenger_id": 9,  "flight_id": 9,  "flight_number": "ZA909",  "pnr": "PNR1010", "complaint_date": "2026-03-06T16:10:00+00:00", "category": "In-Flight Service",  "subcategory": "Entertainment System", "description": "Seatback screen was completely non-functional for the 2.5 hour flight. Crew were unable to reset the unit.",                                           "severity": "Low",      "status": "Open",         "assigned_agent": "Selene Park",  "resolution_notes": None,                                                                             "resolution_date": None,                     "satisfaction_score": None},
    {"complaint_id": 15, "case_id": 11, "passenger_id": 10, "flight_id": 10, "flight_number": "ZA1010", "pnr": "PNR1011", "complaint_date": "2026-03-07T20:30:00+00:00", "category": "Flight Operations",  "subcategory": "Diversion",            "description": "Flight ZA1010 was diverted to ATL with 2.5 hour delay to Miami. No hotel or meal vouchers provided to passengers stuck overnight.",                   "severity": "Critical", "status": "Escalated",    "assigned_agent": "Orion Bailey", "resolution_notes": None,                                                                             "resolution_date": None,                     "satisfaction_score": None},
    {"complaint_id": 16, "case_id": 11, "passenger_id": 10, "flight_id": 10, "flight_number": "ZA1010", "pnr": "PNR1011", "complaint_date": "2026-03-08T07:00:00+00:00", "category": "Customer Service",   "subcategory": "No Compensation Info", "description": "ZavaAir staff at ATL could not explain compensation entitlements for diversion. Phone support wait time exceeded 3 hours.",                           "severity": "High",     "status": "Escalated",    "assigned_agent": "Orion Bailey", "resolution_notes": None,                                                                             "resolution_date": None,                     "satisfaction_score": None},
    {"complaint_id": 17, "case_id": 12, "passenger_id": 11, "flight_id": 2,  "flight_number": "ZA202",  "pnr": "PNR1012", "complaint_date": "2026-03-02T12:15:00+00:00", "category": "Baggage",            "subcategory": "Lost Baggage",         "description": "Checked two bags at LAX for ZA202. Only one arrived in Seattle. Baggage claim could not locate the second bag in their system.",                      "severity": "High",     "status": "Open",         "assigned_agent": "Cosmo Lee",    "resolution_notes": None,                                                                             "resolution_date": None,                     "satisfaction_score": None},
    {"complaint_id": 18, "case_id": 13, "passenger_id": 12, "flight_id": 3,  "flight_number": "ZA303",  "pnr": "PNR1013", "complaint_date": "2026-03-02T16:40:00+00:00", "category": "Booking",            "subcategory": "Check-in Issue",       "description": "Online check-in failed repeatedly 24 hours before departure. Was forced to pay airport fee to check in at counter.",                                   "severity": "Medium",   "status": "Under Review", "assigned_agent": "Selene Park",  "resolution_notes": None,                                                                             "resolution_date": None,                     "satisfaction_score": None},
    {"complaint_id": 19, "case_id": 14, "passenger_id": 14, "flight_id": 5,  "flight_number": "ZA505",  "pnr": "PNR1014", "complaint_date": "2026-03-03T11:50:00+00:00", "category": "In-Flight Service",  "subcategory": "Beverage Service",     "description": "No beverage service offered in economy cabin for the entire JFK-MIA flight. Crew stated supply issue but no water was provided.",                     "severity": "Medium",   "status": "Open",         "assigned_agent": "Cosmo Lee",    "resolution_notes": None,                                                                             "resolution_date": None,                     "satisfaction_score": None},
    {"complaint_id": 20, "case_id": 15, "passenger_id": 15, "flight_id": 7,  "flight_number": "ZA707",  "pnr": "PNR1015", "complaint_date": "2026-03-04T20:15:00+00:00", "category": "Special Assistance", "subcategory": "Medical Request",      "description": "Requested a specific meal for severe nut allergy confirmed at booking. A nut-containing meal was served causing a medical incident mid-flight.",                           "severity": "Critical", "status": "Escalated",    "assigned_agent": "Orion Bailey", "resolution_notes": None,                                                                                                                          "resolution_date": None,                     "satisfaction_score": None},
    {"complaint_id": 21, "case_id": 16, "passenger_id": 16, "flight_id": 11, "flight_number": "ZA1111", "pnr": "PNR1016", "complaint_date": "2026-03-08T20:15:00+00:00", "category": "Baggage",            "subcategory": "Delayed Baggage",      "description": "Baggage from ZA1111 LHR-DXB took over 2 hours to arrive at carousel. No staff available at the belt to provide updates.",                               "severity": "High",     "status": "Open",        "assigned_agent": "Selene Park",  "resolution_notes": None,                                                                                                                          "resolution_date": None,                     "satisfaction_score": None},
    {"complaint_id": 22, "case_id": 17, "passenger_id": 17, "flight_id": 12, "flight_number": "ZA1212", "pnr": "PNR1017", "complaint_date": "2026-03-09T14:10:00+00:00", "category": "Flight Operations",  "subcategory": "Excessive Delay",      "description": "ZA1212 DXB-SIN was delayed 30 minutes with zero communication from gate staff. First-time flyer was confused about connection options.",               "severity": "Low",      "status": "Open",        "assigned_agent": "Cosmo Lee",    "resolution_notes": None,                                                                                                                          "resolution_date": None,                     "satisfaction_score": None},
    {"complaint_id": 23, "case_id": 18, "passenger_id": 18, "flight_id": 9,  "flight_number": "ZA909",  "pnr": "PNR1018", "complaint_date": "2026-03-06T17:20:00+00:00", "category": "In-Flight Service",  "subcategory": "Food Quality",         "description": "Pre-ordered vegetarian meal unavailable on ZA909. Crew offered no alternative and passenger went without food on a 2.5-hour flight.",                "severity": "Medium",   "status": "Resolved",    "assigned_agent": "Cosmo Lee",    "resolution_notes": "ZavaAir apologised and issued a meal voucher redeemable on next booking.",                                                                   "resolution_date": "2026-03-07T09:00:00+00:00", "satisfaction_score": 4.5},
    {"complaint_id": 24, "case_id": 19, "passenger_id": 19, "flight_id": 8,  "flight_number": "ZA808",  "pnr": "PNR1019", "complaint_date": "2026-03-07T05:30:00+00:00", "category": "Seating",            "subcategory": "Seat Malfunction",     "description": "Business class fully-flat bed mechanism on seat 2K failed on ZA808 LAX-NRT 12-hour overnight. Used as a broken recliner the entire flight.",         "severity": "Critical", "status": "Escalated",    "assigned_agent": "Orion Bailey", "resolution_notes": None,                                                                                                                          "resolution_date": None,                     "satisfaction_score": None},
    {"complaint_id": 25, "case_id": 19, "passenger_id": 19, "flight_id": 8,  "flight_number": "ZA808",  "pnr": "PNR1019", "complaint_date": "2026-03-07T06:00:00+00:00", "category": "Customer Service",   "subcategory": "Staff Attitude",       "description": "When reporting broken seat 2K, cabin crew said nothing could be done and walked away. No apology or seat-change offer made.",                        "severity": "High",     "status": "Escalated",    "assigned_agent": "Orion Bailey", "resolution_notes": None,                                                                                                                          "resolution_date": None,                     "satisfaction_score": None},
    {"complaint_id": 26, "case_id": 20, "passenger_id": 20, "flight_id": 7,  "flight_number": "ZA707",  "pnr": "PNR1020", "complaint_date": "2026-03-05T12:20:00+00:00", "category": "Booking",            "subcategory": "Miles Not Credited",   "description": "Platinum tier miles for ZA707 JFK-LHR transatlantic not credited after 6 days. Loyalty portal returns a generic error on manual claim.",             "severity": "Medium",   "status": "Under Review", "assigned_agent": "Selene Park",  "resolution_notes": None,                                                                                                                          "resolution_date": None,                     "satisfaction_score": None},
    {"complaint_id": 27, "case_id": 20, "passenger_id": 20, "flight_id": 7,  "flight_number": "ZA707",  "pnr": "PNR1020", "complaint_date": "2026-03-05T13:00:00+00:00", "category": "In-Flight Service",  "subcategory": "Wi-Fi Service",        "description": "Paid Wi-Fi package (£29.99) failed to connect for the entire 8-hour ZA707 transatlantic. Crew acknowledged but refund request was ignored.",          "severity": "Low",      "status": "Under Review", "assigned_agent": "Selene Park",  "resolution_notes": None,                                                                                                                          "resolution_date": None,                     "satisfaction_score": None},
    {"complaint_id": 28, "case_id": 15, "passenger_id": 15, "flight_id": 7,  "flight_number": "ZA707",  "pnr": "PNR1015", "complaint_date": "2026-03-05T11:00:00+00:00", "category": "Special Assistance", "subcategory": "Medical Followup",    "description": "48 hours after nut-allergy medical incident on ZA707, no incident report filed and next of kin unable to reach ZavaAir medical team.",               "severity": "Critical", "status": "Escalated",    "assigned_agent": "Orion Bailey", "resolution_notes": None,                                                                                                                          "resolution_date": None,                     "satisfaction_score": None},
]


# ── Test batch (new records, distinct PKs, clean FK chain) ────────────────────
# Relationship map:
#   Passenger 21 → Case 21 (ZA1414) → Complaints 29, 30   (two complaints, one case)
#   Passenger 22 → Case 22 (ZA1414) → Complaint 31        (same flight, different passenger)
#   Passenger 23 → Case 23 (ZA1515) → Complaint 32
#   Passenger 21 → Case 24 (ZA1515) → Complaint 33        (same passenger, second flight)

TEST_PASSENGERS: list[dict] = [
    {"passenger_id": 21, "first_name": "Zara",   "last_name": "Thompson", "email": "zara.thompson@example.com",  "phone": "+61-2-5550-0121", "country": "Australia",  "frequent_flyer_tier": "Gold",   "total_flights": 89,  "member_since": "2017-04-12T00:00:00+00:00"},
    {"passenger_id": 22, "first_name": "Hiroshi", "last_name": "Yamamoto", "email": "hiroshi.yamamoto@example.com", "phone": "+81-3-5550-0122", "country": "Japan",      "frequent_flyer_tier": "Silver", "total_flights": 47,  "member_since": "2019-09-03T00:00:00+00:00"},
    {"passenger_id": 23, "first_name": "Lucia",   "last_name": "Ferreira", "email": "lucia.ferreira@example.com",  "phone": "+351-21-550-0123","country": "Portugal",   "frequent_flyer_tier": "Bronze", "total_flights": 15,  "member_since": "2023-02-18T00:00:00+00:00"},
]

TEST_FLIGHTS: list[dict] = [
    {"flight_id": 14, "flight_number": "ZA1414", "origin_code": "SIN", "origin_city": "Singapore", "destination_code": "SYD", "destination_city": "Sydney",          "scheduled_departure": "2026-03-10T08:00:00+00:00", "actual_departure": "2026-03-10T08:00:00+00:00", "scheduled_arrival": "2026-03-10T19:30:00+00:00", "actual_arrival": "2026-03-10T19:30:00+00:00", "aircraft_type": "Crescent 787-9",   "flight_status": "On Time", "delay_minutes": 0},
    {"flight_id": 15, "flight_number": "ZA1515", "origin_code": "SYD", "origin_city": "Sydney",    "destination_code": "LAX", "destination_city": "Los Angeles",     "scheduled_departure": "2026-03-11T22:00:00+00:00", "actual_departure": "2026-03-12T23:05:00+00:00", "scheduled_arrival": "2026-03-12T14:00:00+00:00", "actual_arrival": "2026-03-12T15:05:00+00:00", "aircraft_type": "Moonbeam A350",    "flight_status": "Delayed", "delay_minutes": 65},
]

TEST_CASES: list[dict] = [
    {"case_id": 21, "passenger_id": 21, "flight_id": 14, "flight_number": "ZA1414", "pnr": "PNR2101", "case_status": "Escalated",    "opened_at": "2026-03-10T20:00:00+00:00", "last_updated_at": "2026-03-11T09:00:00+00:00", "closed_at": None},
    {"case_id": 22, "passenger_id": 22, "flight_id": 14, "flight_number": "ZA1414", "pnr": "PNR2202", "case_status": "Open",         "opened_at": "2026-03-10T20:30:00+00:00", "last_updated_at": "2026-03-10T20:30:00+00:00", "closed_at": None},
    {"case_id": 23, "passenger_id": 23, "flight_id": 15, "flight_number": "ZA1515", "pnr": "PNR2303", "case_status": "Under Review", "opened_at": "2026-03-12T16:00:00+00:00", "last_updated_at": "2026-03-12T17:30:00+00:00", "closed_at": None},
    {"case_id": 24, "passenger_id": 21, "flight_id": 15, "flight_number": "ZA1515", "pnr": "PNR2401", "case_status": "Open",         "opened_at": "2026-03-12T15:30:00+00:00", "last_updated_at": "2026-03-12T15:30:00+00:00", "closed_at": None},
]

TEST_COMPLAINTS: list[dict] = [
    {"complaint_id": 29, "case_id": 21, "passenger_id": 21, "flight_id": 14, "flight_number": "ZA1414", "pnr": "PNR2101", "complaint_date": "2026-03-10T20:05:00+00:00", "category": "Seating",           "subcategory": "Seat Malfunction",     "description": "Seat 14A power socket failed shortly after take-off on ZA1414 SIN-SYD. Crew were unable to reset the row and offered no alternative seating.", "severity": "High",     "status": "Escalated",    "assigned_agent": "Cosmo Lee",    "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
    {"complaint_id": 30, "case_id": 21, "passenger_id": 21, "flight_id": 14, "flight_number": "ZA1414", "pnr": "PNR2101", "complaint_date": "2026-03-10T20:45:00+00:00", "category": "Customer Service",  "subcategory": "Staff Attitude",       "description": "After reporting the faulty power socket, a crew member told the passenger to 'just use the battery' and did not log the incident.",              "severity": "Medium",   "status": "Escalated",    "assigned_agent": "Cosmo Lee",    "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
    {"complaint_id": 31, "case_id": 22, "passenger_id": 22, "flight_id": 14, "flight_number": "ZA1414", "pnr": "PNR2202", "complaint_date": "2026-03-10T20:40:00+00:00", "category": "Baggage",           "subcategory": "Lost Baggage",         "description": "One checked bag did not arrive at SYD carousel after ZA1414. Baggage desk could not locate it in the tracking system and filed a PIR report.", "severity": "High",     "status": "Open",         "assigned_agent": "Selene Park",  "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
    {"complaint_id": 32, "case_id": 23, "passenger_id": 23, "flight_id": 15, "flight_number": "ZA1515", "pnr": "PNR2303", "complaint_date": "2026-03-12T16:10:00+00:00", "category": "Flight Operations", "subcategory": "Excessive Delay",      "description": "ZA1515 SYD-LAX departed 65 minutes late with no gate announcement. Connection to domestic LAX flight was missed as a result.",                   "severity": "Medium",   "status": "Under Review", "assigned_agent": "Orion Bailey", "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
    {"complaint_id": 33, "case_id": 24, "passenger_id": 21, "flight_id": 15, "flight_number": "ZA1515", "pnr": "PNR2401", "complaint_date": "2026-03-12T15:40:00+00:00", "category": "Booking",           "subcategory": "Refund Delay",         "description": "Upgrade fee charged at SYD check-in for ZA1515 was not reflected in the booking system. Refund requested at the gate; no confirmation received.", "severity": "Medium",   "status": "Open",         "assigned_agent": "Cosmo Lee",    "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
]


# ── Test batch 2 (passengers 24-27, flights 16-18, cases 25-30, complaints 34-43) ─────
# Scenario highlights:
#   - Passenger 24 (Kwame, Platinum)  → cancelled ZA1818 LHR→SYD → Platinum not prioritised
#   - Passenger 25 (Sofia, no tier)   → delayed ZA1717 CDG→JFK → first-time flyer, confused
#   - Passenger 26 (Ravi, Gold)       → delayed ZA1717 + on-time ZA1616 (multi-flight, multi-case)
#   - Passenger 27 (Emma, Silver)     → cancelled ZA1818 → hotel/rebooking failures
#   - Kwame reappears on ZA1818 (second case on cancelled flight, Platinum tier escalation)

TEST2_PASSENGERS: list[dict] = [
    {"passenger_id": 24, "first_name": "Kwame",  "last_name": "Asante",   "email": "kwame.asante@example.com",   "phone": "+233-30-550-0124", "country": "Ghana",   "frequent_flyer_tier": "Platinum", "total_flights": 156, "member_since": "2015-07-22T00:00:00+00:00"},
    {"passenger_id": 25, "first_name": "Sofia",  "last_name": "Andersen", "email": "sofia.andersen@example.com", "phone": "+45-32-550-0125",  "country": "Denmark", "frequent_flyer_tier": "None",     "total_flights": 3,   "member_since": "2025-11-01T00:00:00+00:00"},
    {"passenger_id": 26, "first_name": "Ravi",   "last_name": "Nair",     "email": "ravi.nair@example.com",      "phone": "+91-22-5550-0126", "country": "India",   "frequent_flyer_tier": "Gold",     "total_flights": 72,  "member_since": "2018-03-14T00:00:00+00:00"},
    {"passenger_id": 27, "first_name": "Emma",   "last_name": "Johansson","email": "emma.johansson@example.com", "phone": "+46-8-5550-0127",  "country": "Sweden",  "frequent_flyer_tier": "Silver",   "total_flights": 34,  "member_since": "2021-06-30T00:00:00+00:00"},
]

TEST2_FLIGHTS: list[dict] = [
    {"flight_id": 16, "flight_number": "ZA1616", "origin_code": "DXB", "origin_city": "Dubai",  "destination_code": "BOM", "destination_city": "Mumbai",       "scheduled_departure": "2026-03-13T06:00:00+00:00", "actual_departure": "2026-03-13T06:00:00+00:00", "scheduled_arrival": "2026-03-13T09:30:00+00:00", "actual_arrival": "2026-03-13T09:30:00+00:00", "aircraft_type": "Boeing 737-800",  "flight_status": "On Time",  "delay_minutes": 0},
    {"flight_id": 17, "flight_number": "ZA1717", "origin_code": "CDG", "origin_city": "Paris",  "destination_code": "JFK", "destination_city": "New York",     "scheduled_departure": "2026-03-14T11:00:00+00:00", "actual_departure": "2026-03-14T13:00:00+00:00", "scheduled_arrival": "2026-03-14T14:30:00+00:00", "actual_arrival": "2026-03-14T16:30:00+00:00", "aircraft_type": "Crescent 787-9", "flight_status": "Delayed",  "delay_minutes": 120},
    {"flight_id": 18, "flight_number": "ZA1818", "origin_code": "LHR", "origin_city": "London", "destination_code": "SYD", "destination_city": "Sydney",       "scheduled_departure": "2026-03-15T21:00:00+00:00", "actual_departure": None,                        "scheduled_arrival": "2026-03-17T06:00:00+00:00", "actual_arrival": None,                        "aircraft_type": "Moonbeam A350",  "flight_status": "Cancelled","delay_minutes": 0},
]

TEST2_CASES: list[dict] = [
    {"case_id": 25, "passenger_id": 24, "flight_id": 16, "flight_number": "ZA1616", "pnr": "PNR2401", "case_status": "Resolved",     "opened_at": "2026-03-13T10:00:00+00:00", "last_updated_at": "2026-03-14T09:00:00+00:00", "closed_at": "2026-03-14T09:00:00+00:00"},
    {"case_id": 26, "passenger_id": 25, "flight_id": 17, "flight_number": "ZA1717", "pnr": "PNR2501", "case_status": "Open",         "opened_at": "2026-03-14T13:10:00+00:00", "last_updated_at": "2026-03-14T13:10:00+00:00", "closed_at": None},
    {"case_id": 27, "passenger_id": 26, "flight_id": 17, "flight_number": "ZA1717", "pnr": "PNR2601", "case_status": "Escalated",    "opened_at": "2026-03-14T13:30:00+00:00", "last_updated_at": "2026-03-15T08:00:00+00:00", "closed_at": None},
    {"case_id": 28, "passenger_id": 27, "flight_id": 18, "flight_number": "ZA1818", "pnr": "PNR2701", "case_status": "Escalated",    "opened_at": "2026-03-15T19:00:00+00:00", "last_updated_at": "2026-03-16T10:00:00+00:00", "closed_at": None},
    {"case_id": 29, "passenger_id": 24, "flight_id": 18, "flight_number": "ZA1818", "pnr": "PNR2402", "case_status": "Escalated",    "opened_at": "2026-03-15T19:30:00+00:00", "last_updated_at": "2026-03-16T11:00:00+00:00", "closed_at": None},
    {"case_id": 30, "passenger_id": 26, "flight_id": 16, "flight_number": "ZA1616", "pnr": "PNR2602", "case_status": "Under Review", "opened_at": "2026-03-13T10:30:00+00:00", "last_updated_at": "2026-03-13T15:00:00+00:00", "closed_at": None},
]

TEST2_COMPLAINTS: list[dict] = [
    # case 25 — Kwame / ZA1616 DXB→BOM — resolved upgrade issue
    {"complaint_id": 34, "case_id": 25, "passenger_id": 24, "flight_id": 16, "flight_number": "ZA1616", "pnr": "PNR2401", "complaint_date": "2026-03-13T10:05:00+00:00", "category": "Seating",            "subcategory": "Upgrade Not Applied",  "description": "Platinum status upgrade to business class confirmed at online check-in but seat was reallocated at the gate to another passenger with no explanation.", "severity": "Medium",   "status": "Resolved",     "assigned_agent": "Cosmo Lee",    "resolution_notes": "ZavaAir confirmed seat reallocation error and issued a business class upgrade voucher valid 12 months.", "resolution_date": "2026-03-14T09:00:00+00:00", "satisfaction_score": 3.5},
    # case 26 — Sofia / ZA1717 CDG→JFK — first-time flyer, 120-min delay
    {"complaint_id": 35, "case_id": 26, "passenger_id": 25, "flight_id": 17, "flight_number": "ZA1717", "pnr": "PNR2501", "complaint_date": "2026-03-14T13:15:00+00:00", "category": "Flight Operations",  "subcategory": "Excessive Delay",      "description": "ZA1717 CDG-JFK departed 2 hours late. No gate announcement made. As a first-time international traveller I had no idea if my connection in JFK was at risk.", "severity": "Medium",   "status": "Open",         "assigned_agent": "Selene Park",  "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
    {"complaint_id": 36, "case_id": 26, "passenger_id": 25, "flight_id": 17, "flight_number": "ZA1717", "pnr": "PNR2501", "complaint_date": "2026-03-14T13:45:00+00:00", "category": "Customer Service",   "subcategory": "No Communication",     "description": "Gate staff at CDG gave no delay updates for over 90 minutes. When asked, they said 'check the board'. No ZavaAir rep was present at the gate.",        "severity": "High",     "status": "Open",         "assigned_agent": "Selene Park",  "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
    # case 27 — Ravi / ZA1717 CDG→JFK — baggage + staff escalation
    {"complaint_id": 37, "case_id": 27, "passenger_id": 26, "flight_id": 17, "flight_number": "ZA1717", "pnr": "PNR2601", "complaint_date": "2026-03-14T13:35:00+00:00", "category": "Baggage",            "subcategory": "Delayed Baggage",      "description": "Checked bag did not arrive at JFK after the already-delayed ZA1717. Baggage desk confirmed bag was still in CDG and estimated 48-hour delivery.",          "severity": "High",     "status": "Escalated",    "assigned_agent": "Orion Bailey", "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
    {"complaint_id": 38, "case_id": 27, "passenger_id": 26, "flight_id": 17, "flight_number": "ZA1717", "pnr": "PNR2601", "complaint_date": "2026-03-14T17:00:00+00:00", "category": "Customer Service",   "subcategory": "Staff Attitude",       "description": "When escalating the baggage issue the JFK ZavaAir supervisor raised their voice and told me I should have checked in earlier. Deeply unprofessional.",     "severity": "Critical", "status": "Escalated",    "assigned_agent": "Orion Bailey", "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
    # case 28 — Emma / ZA1818 LHR→SYD — cancelled flight, hotel/rebooking failures
    {"complaint_id": 39, "case_id": 28, "passenger_id": 27, "flight_id": 18, "flight_number": "ZA1818", "pnr": "PNR2701", "complaint_date": "2026-03-15T19:05:00+00:00", "category": "Flight Operations",  "subcategory": "Flight Cancellation",  "description": "ZA1818 LHR-SYD cancelled with less than 2 hours notice. No alternative flight offered within 24 hours. Stranded at LHR with no onward travel options.",  "severity": "Critical", "status": "Escalated",    "assigned_agent": "Cosmo Lee",    "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
    {"complaint_id": 40, "case_id": 28, "passenger_id": 27, "flight_id": 18, "flight_number": "ZA1818", "pnr": "PNR2701", "complaint_date": "2026-03-15T20:00:00+00:00", "category": "Customer Service",   "subcategory": "No Hotel Voucher",     "description": "ZavaAir staff at LHR refused to issue hotel accommodation voucher citing 'extraordinary circumstances' despite cancellation being due to crew shortage.", "severity": "High",     "status": "Escalated",    "assigned_agent": "Cosmo Lee",    "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
    {"complaint_id": 41, "case_id": 28, "passenger_id": 27, "flight_id": 18, "flight_number": "ZA1818", "pnr": "PNR2701", "complaint_date": "2026-03-15T21:00:00+00:00", "category": "Booking",            "subcategory": "Rebooking Refused",    "description": "ZavaAir app showed no available flights for 4 days. Phone queue wait exceeded 2 hours. Eventually hung up without reaching an agent.",                    "severity": "High",     "status": "Escalated",    "assigned_agent": "Cosmo Lee",    "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
    # case 29 — Kwame (Platinum) / ZA1818 — Platinum tier not prioritised during cancellation
    {"complaint_id": 42, "case_id": 29, "passenger_id": 24, "flight_id": 18, "flight_number": "ZA1818", "pnr": "PNR2402", "complaint_date": "2026-03-15T19:35:00+00:00", "category": "Customer Service",   "subcategory": "Tier Benefits Ignored","description": "As a Platinum member I was queued with all passengers during ZA1818 cancellation. No priority rebooking lane or dedicated agent. Platinum benefits were completely disregarded.", "severity": "Critical", "status": "Escalated",    "assigned_agent": "Orion Bailey", "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
    # case 30 — Ravi / ZA1616 DXB→BOM — IFE failure on a different flight
    {"complaint_id": 43, "case_id": 30, "passenger_id": 26, "flight_id": 16, "flight_number": "ZA1616", "pnr": "PNR2602", "complaint_date": "2026-03-13T10:35:00+00:00", "category": "In-Flight Service",  "subcategory": "Entertainment System", "description": "Seatback screen on ZA1616 DXB-BOM was frozen from boarding and never reset. Crew said the system was 'known to be faulty' but no compensation offered.",  "severity": "Low",      "status": "Under Review", "assigned_agent": "Selene Park",  "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
]

# ── Test batch 3 (passengers 28-32, flights 19-22, cases 31-38, complaints 44-57) ─────
# Scenario highlights:
#   - Passenger 29 (Chen Wei, Platinum, 243 flights) → long-haul NRT→LAX broken flat bed + lounge denied
#   - Passenger 29 again on diverted ZA2121 → Platinum not rebooked first (3rd case, repeat VIP)
#   - Passenger 30 (Yusuf, no tier) → first-time long-haul, lost connection info
#   - Passenger 31 (Priya, Silver) → ZA2121 diverted BOM→LHR re-routed via CDG, stranded, no transport
#   - Passenger 32 (Marco, Bronze) → ZA2222 JFK→MIA cancelled (weather), refund refused
#   - Passenger 28 (Amelia, Gold) → one resolved with 5.0 sat score (positive signal), one under review

TEST3_PASSENGERS: list[dict] = [
    {"passenger_id": 28, "first_name": "Amelia",  "last_name": "Walsh",         "email": "amelia.walsh@example.com",         "phone": "+353-1-550-0128",  "country": "Ireland",      "frequent_flyer_tier": "Gold",     "total_flights": 58,  "member_since": "2018-09-14T00:00:00+00:00"},
    {"passenger_id": 29, "first_name": "Chen",    "last_name": "Wei",           "email": "chen.wei@example.com",             "phone": "+86-10-5550-0129", "country": "China",        "frequent_flyer_tier": "Platinum", "total_flights": 243, "member_since": "2011-03-07T00:00:00+00:00"},
    {"passenger_id": 30, "first_name": "Yusuf",   "last_name": "Al-Rashidi",    "email": "yusuf.alrashidi@example.com",      "phone": "+971-4-550-0130",  "country": "UAE",          "frequent_flyer_tier": "None",     "total_flights": 5,   "member_since": "2025-08-20T00:00:00+00:00"},
    {"passenger_id": 31, "first_name": "Priya",   "last_name": "Krishnamurthy", "email": "priya.krishnamurthy@example.com",  "phone": "+91-80-5550-0131", "country": "India",        "frequent_flyer_tier": "Silver",   "total_flights": 29,  "member_since": "2021-11-02T00:00:00+00:00"},
    {"passenger_id": 32, "first_name": "Marco",   "last_name": "Rossi",         "email": "marco.rossi@example.com",          "phone": "+39-06-5550-0132", "country": "Italy",        "frequent_flyer_tier": "Bronze",   "total_flights": 11,  "member_since": "2023-05-19T00:00:00+00:00"},
]

TEST3_FLIGHTS: list[dict] = [
    {"flight_id": 19, "flight_number": "ZA1919", "origin_code": "SYD", "origin_city": "Sydney",      "destination_code": "DXB", "destination_city": "Dubai",     "scheduled_departure": "2026-03-16T23:00:00+00:00", "actual_departure": "2026-03-16T23:00:00+00:00", "scheduled_arrival": "2026-03-17T05:30:00+00:00", "actual_arrival": "2026-03-17T05:30:00+00:00", "aircraft_type": "Moonbeam A350",    "flight_status": "On Time",  "delay_minutes": 0},
    {"flight_id": 20, "flight_number": "ZA2020", "origin_code": "NRT", "origin_city": "Tokyo",       "destination_code": "LAX", "destination_city": "Los Angeles","scheduled_departure": "2026-03-17T11:00:00+00:00", "actual_departure": "2026-03-17T14:00:00+00:00", "scheduled_arrival": "2026-03-17T05:30:00+00:00", "actual_arrival": "2026-03-17T08:30:00+00:00", "aircraft_type": "Tycho 777X",       "flight_status": "Delayed",  "delay_minutes": 180},
    {"flight_id": 21, "flight_number": "ZA2121", "origin_code": "BOM", "origin_city": "Mumbai",      "destination_code": "LHR", "destination_city": "London",    "scheduled_departure": "2026-03-18T02:00:00+00:00", "actual_departure": "2026-03-18T02:15:00+00:00", "scheduled_arrival": "2026-03-18T07:30:00+00:00", "actual_arrival": "2026-03-18T10:45:00+00:00", "aircraft_type": "Crescent 787-9",   "flight_status": "Diverted", "delay_minutes": 195},
    {"flight_id": 22, "flight_number": "ZA2222", "origin_code": "JFK", "origin_city": "New York",    "destination_code": "MIA", "destination_city": "Miami",     "scheduled_departure": "2026-03-19T07:00:00+00:00", "actual_departure": None,                        "scheduled_arrival": "2026-03-19T10:15:00+00:00", "actual_arrival": None,                        "aircraft_type": "Selene A320",      "flight_status": "Cancelled","delay_minutes": 0},
]

TEST3_CASES: list[dict] = [
    {"case_id": 31, "passenger_id": 29, "flight_id": 20, "flight_number": "ZA2020", "pnr": "PNR2901", "case_status": "Escalated",    "opened_at": "2026-03-17T09:00:00+00:00", "last_updated_at": "2026-03-18T10:00:00+00:00", "closed_at": None},
    {"case_id": 32, "passenger_id": 28, "flight_id": 19, "flight_number": "ZA1919", "pnr": "PNR2801", "case_status": "Resolved",     "opened_at": "2026-03-17T06:00:00+00:00", "last_updated_at": "2026-03-18T14:00:00+00:00", "closed_at": "2026-03-18T14:00:00+00:00"},
    {"case_id": 33, "passenger_id": 30, "flight_id": 19, "flight_number": "ZA1919", "pnr": "PNR3001", "case_status": "Open",         "opened_at": "2026-03-17T06:30:00+00:00", "last_updated_at": "2026-03-17T06:30:00+00:00", "closed_at": None},
    {"case_id": 34, "passenger_id": 31, "flight_id": 21, "flight_number": "ZA2121", "pnr": "PNR3101", "case_status": "Escalated",    "opened_at": "2026-03-18T11:00:00+00:00", "last_updated_at": "2026-03-19T09:00:00+00:00", "closed_at": None},
    {"case_id": 35, "passenger_id": 32, "flight_id": 22, "flight_number": "ZA2222", "pnr": "PNR3201", "case_status": "Escalated",    "opened_at": "2026-03-19T08:00:00+00:00", "last_updated_at": "2026-03-19T15:00:00+00:00", "closed_at": None},
    {"case_id": 36, "passenger_id": 29, "flight_id": 21, "flight_number": "ZA2121", "pnr": "PNR2902", "case_status": "Escalated",    "opened_at": "2026-03-18T11:30:00+00:00", "last_updated_at": "2026-03-19T12:00:00+00:00", "closed_at": None},
    {"case_id": 37, "passenger_id": 28, "flight_id": 22, "flight_number": "ZA2222", "pnr": "PNR2802", "case_status": "Under Review", "opened_at": "2026-03-19T08:30:00+00:00", "last_updated_at": "2026-03-19T14:00:00+00:00", "closed_at": None},
    {"case_id": 38, "passenger_id": 31, "flight_id": 20, "flight_number": "ZA2020", "pnr": "PNR3102", "case_status": "Open",         "opened_at": "2026-03-17T09:30:00+00:00", "last_updated_at": "2026-03-17T09:30:00+00:00", "closed_at": None},
]

TEST3_COMPLAINTS: list[dict] = [
    # case 31 — Chen Wei (Platinum) / ZA2020 NRT→LAX — 3-hour delay, broken flat bed, lounge denied
    {"complaint_id": 44, "case_id": 31, "passenger_id": 29, "flight_id": 20, "flight_number": "ZA2020", "pnr": "PNR2901", "complaint_date": "2026-03-17T09:05:00+00:00", "category": "Customer Service",   "subcategory": "Lounge Access Denied",  "description": "Platinum tier lounge access denied at NRT T1 despite valid credential. Staff said system showed 'unverified tier' and refused manual override.",              "severity": "High",     "status": "Escalated",    "assigned_agent": "Orion Bailey", "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
    {"complaint_id": 45, "case_id": 31, "passenger_id": 29, "flight_id": 20, "flight_number": "ZA2020", "pnr": "PNR2901", "complaint_date": "2026-03-17T14:20:00+00:00", "category": "In-Flight Service",  "subcategory": "Food Quality",          "description": "Business class meal on ZA2020 was visibly reheated economy food relabelled. No amuse-bouche, no choice of main. 11-hour flight with sub-standard catering.",  "severity": "Medium",   "status": "Escalated",    "assigned_agent": "Orion Bailey", "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
    {"complaint_id": 46, "case_id": 31, "passenger_id": 29, "flight_id": 20, "flight_number": "ZA2020", "pnr": "PNR2901", "complaint_date": "2026-03-17T15:00:00+00:00", "category": "Seating",            "subcategory": "Seat Malfunction",      "description": "Business class fully-flat seat 1A on ZA2020 NRT-LAX collapsed to 45-degree angle 2 hours into an 11-hour flight. Crew could not fix. Slept upright entire night.", "severity": "Critical", "status": "Escalated",    "assigned_agent": "Orion Bailey", "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
    # case 32 — Amelia (Gold) / ZA1919 SYD→DXB — resolved, high satisfaction
    {"complaint_id": 47, "case_id": 32, "passenger_id": 28, "flight_id": 19, "flight_number": "ZA1919", "pnr": "PNR2801", "complaint_date": "2026-03-17T06:10:00+00:00", "category": "Baggage",            "subcategory": "Delayed Baggage",       "description": "Bag took 55 minutes at DXB carousel after ZA1919. Minor delay but wanted to flag given tight connection window to onward flight.",                             "severity": "Low",      "status": "Resolved",     "assigned_agent": "Cosmo Lee",    "resolution_notes": "ZavaAir confirmed bag was last off aircraft due to loading position. Issued a priority tag voucher for next 3 flights and apologised.", "resolution_date": "2026-03-18T14:00:00+00:00", "satisfaction_score": 5.0},
    # case 33 — Yusuf (no tier) / ZA1919 SYD→DXB — first-timer, missed connection info
    {"complaint_id": 48, "case_id": 33, "passenger_id": 30, "flight_id": 19, "flight_number": "ZA1919", "pnr": "PNR3001", "complaint_date": "2026-03-17T06:40:00+00:00", "category": "Customer Service",   "subcategory": "No Communication",      "description": "First long-haul flight. No one explained connection process at DXB. Missed onward connection and had to rebook at own expense. No ZavaAir desk was staffed at the gate.", "severity": "Medium",   "status": "Open",         "assigned_agent": "Selene Park",  "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
    # case 34 — Priya (Silver) / ZA2121 BOM→LHR diverted to CDG — stranded cascade
    {"complaint_id": 49, "case_id": 34, "passenger_id": 31, "flight_id": 21, "flight_number": "ZA2121", "pnr": "PNR3101", "complaint_date": "2026-03-18T11:05:00+00:00", "category": "Flight Operations",  "subcategory": "Diversion",             "description": "ZA2121 diverted to CDG from LHR with no PA announcement until 10 minutes before landing. Passengers had no time to prepare. Destination changed without consent.",  "severity": "Critical", "status": "Escalated",    "assigned_agent": "Cosmo Lee",    "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
    {"complaint_id": 50, "case_id": 34, "passenger_id": 31, "flight_id": 21, "flight_number": "ZA2121", "pnr": "PNR3101", "complaint_date": "2026-03-18T11:45:00+00:00", "category": "Customer Service",   "subcategory": "No Ground Transport",   "description": "After diversion to CDG no buses, trains or taxis were arranged by ZavaAir. 200 passengers left in arrivals with no guidance for 3+ hours.",                        "severity": "High",     "status": "Escalated",    "assigned_agent": "Cosmo Lee",    "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
    {"complaint_id": 51, "case_id": 34, "passenger_id": 31, "flight_id": 21, "flight_number": "ZA2121", "pnr": "PNR3101", "complaint_date": "2026-03-18T13:00:00+00:00", "category": "Booking",            "subcategory": "Missed Connection",     "description": "Missed pre-booked Eurostar from London St Pancras as a direct result of ZA2121 diversion. ZavaAir refused to reimburse the £180 non-refundable ticket.",        "severity": "High",     "status": "Escalated",    "assigned_agent": "Cosmo Lee",    "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
    # case 35 — Marco (Bronze) / ZA2222 JFK→MIA cancelled — refund refused + staff rude
    {"complaint_id": 52, "case_id": 35, "passenger_id": 32, "flight_id": 22, "flight_number": "ZA2222", "pnr": "PNR3201", "complaint_date": "2026-03-19T08:10:00+00:00", "category": "Flight Operations",  "subcategory": "Flight Cancellation",   "description": "ZA2222 JFK-MIA cancelled due to 'weather' despite clear skies and other airlines operating the same route. Full refund requested but denied at check-in counter.",  "severity": "Critical", "status": "Escalated",    "assigned_agent": "Selene Park",  "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
    {"complaint_id": 53, "case_id": 35, "passenger_id": 32, "flight_id": 22, "flight_number": "ZA2222", "pnr": "PNR3201", "complaint_date": "2026-03-19T09:00:00+00:00", "category": "Customer Service",   "subcategory": "Staff Attitude",        "description": "Check-in supervisor at JFK was sarcastic and told passengers to 'Google the weather' when challenged on the cancellation reason. Behaviour was witnessed by 12+ passengers.", "severity": "High",     "status": "Escalated",    "assigned_agent": "Selene Park",  "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
    # case 36 — Chen Wei (Platinum, 3rd case) / ZA2121 diverted — Platinum ignored again
    {"complaint_id": 54, "case_id": 36, "passenger_id": 29, "flight_id": 21, "flight_number": "ZA2121", "pnr": "PNR2902", "complaint_date": "2026-03-18T11:35:00+00:00", "category": "Customer Service",   "subcategory": "Tier Benefits Ignored", "description": "Third incident this month where Platinum status was disregarded. During ZA2121 diversion chaos at CDG no priority queue or dedicated agent was available for tier members.", "severity": "Critical", "status": "Escalated",    "assigned_agent": "Orion Bailey", "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
    {"complaint_id": 55, "case_id": 36, "passenger_id": 29, "flight_id": 21, "flight_number": "ZA2121", "pnr": "PNR2902", "complaint_date": "2026-03-18T14:00:00+00:00", "category": "Baggage",            "subcategory": "Lost Baggage",          "description": "Checked business class luggage not transferred during ZA2121 CDG diversion. Bag missing for 72+ hours. Contains medical equipment required daily.",                  "severity": "Critical", "status": "Escalated",    "assigned_agent": "Orion Bailey", "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
    # case 37 — Amelia (Gold) / ZA2222 cancelled — compensation question under review
    {"complaint_id": 56, "case_id": 37, "passenger_id": 28, "flight_id": 22, "flight_number": "ZA2222", "pnr": "PNR2802", "complaint_date": "2026-03-19T08:35:00+00:00", "category": "Booking",            "subcategory": "Compensation Unclear",  "description": "Weather cancellation on ZA2222. ZavaAir website states weather cancellations are non-compensable but EU261 may apply. Agent gave conflicting information twice.",     "severity": "Medium",   "status": "Under Review", "assigned_agent": "Cosmo Lee",    "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
    # case 38 — Priya (Silver) / ZA2020 NRT→LAX — IFE dead, second flight this batch
    {"complaint_id": 57, "case_id": 38, "passenger_id": 31, "flight_id": 20, "flight_number": "ZA2020", "pnr": "PNR3102", "complaint_date": "2026-03-17T09:35:00+00:00", "category": "In-Flight Service",  "subcategory": "Entertainment System",  "description": "Seatback IFE on ZA2020 row 28 was completely dead for the 10-hour NRT-LAX flight. No movies, no music, no flight map. Crew acknowledged but offered no solution.", "severity": "Low",      "status": "Open",         "assigned_agent": "Selene Park",  "resolution_notes": None, "resolution_date": None, "satisfaction_score": None},
]

# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Produce ZavaAir complaint data to Event Hubs")
    parser.add_argument(
        "--dataset", choices=["seed", "test", "test2", "test3", "all"], default="seed",
        help="seed=original 20/13/20/28; test=batch1 3/2/4/5; test2=batch2 4/3/6/10; test3=batch3 5/4/8/14; all=everything",
    )
    args = parser.parse_args()

    if args.dataset == "seed":
        p, f, c, co = SEED_PASSENGERS, SEED_FLIGHTS, SEED_CASES, SEED_COMPLAINTS
    elif args.dataset == "test":
        p, f, c, co = TEST_PASSENGERS, TEST_FLIGHTS, TEST_CASES, TEST_COMPLAINTS
    elif args.dataset == "test2":
        p, f, c, co = TEST2_PASSENGERS, TEST2_FLIGHTS, TEST2_CASES, TEST2_COMPLAINTS
    elif args.dataset == "test3":
        p, f, c, co = TEST3_PASSENGERS, TEST3_FLIGHTS, TEST3_CASES, TEST3_COMPLAINTS
    else:  # all
        p  = SEED_PASSENGERS + TEST_PASSENGERS  + TEST2_PASSENGERS + TEST3_PASSENGERS
        f  = SEED_FLIGHTS    + TEST_FLIGHTS     + TEST2_FLIGHTS    + TEST3_FLIGHTS
        c  = SEED_CASES      + TEST_CASES       + TEST2_CASES      + TEST3_CASES
        co = SEED_COMPLAINTS + TEST_COMPLAINTS  + TEST2_COMPLAINTS + TEST3_COMPLAINTS

    logger.info("Dataset: %s | passengers=%d flights=%d cases=%d complaints=%d",
                args.dataset, len(p), len(f), len(c), len(co))

    try:
        produce_dataset(passengers=p, flights=f, cases=c, complaints=co)
    except KafkaException as exc:
        logger.error("Kafka error: %s", exc)
        sys.exit(1)
    except Exception as exc:
        logger.error("Unexpected error: %s", exc)
        sys.exit(1)
