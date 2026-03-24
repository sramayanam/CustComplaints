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
from pathlib import Path
from typing import Any

from confluent_kafka import Producer, KafkaException

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


# ── Delivery callback ──────────────────────────────────────────────────────────

def _on_delivery(err: Any, msg: Any) -> None:
    """Called by librdkafka on broker ACK or terminal error."""
    if err:
        logger.error(
            "Delivery FAILED | topic=%s partition=%s key=%s error=%s",
            msg.topic(), msg.partition(), msg.key(), err,
        )
    else:
        logger.debug(
            "Delivered | topic=%-40s partition=%d offset=%d key=%s",
            msg.topic(), msg.partition(), msg.offset(), msg.key(),
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


# ── Data loading ──────────────────────────────────────────────────────────────

_DATA_DIR = Path(__file__).parent / "data"


def _load(name: str) -> dict[str, list[dict]]:
    """Load a single dataset from kafka/data/<name>.json."""
    path = _DATA_DIR / f"{name}.json"
    if not path.exists():
        raise FileNotFoundError(
            f"Dataset '{name}' not found. Expected: {path}\n"
            f"Available: {', '.join(_available())}"
        )
    return json.loads(path.read_text(encoding="utf-8"))


def _available() -> list[str]:
    """Return dataset names (JSON stems) sorted alphabetically."""
    return sorted(p.stem for p in _DATA_DIR.glob("*.json"))


def _load_all() -> tuple[list, list, list, list]:
    """Merge every dataset in kafka/data/ in sorted order."""
    passengers, flights, cases, complaints = [], [], [], []
    for name in _available():
        d = _load(name)
        passengers.extend(d.get("passengers", []))
        flights.extend(d.get("flights", []))
        cases.extend(d.get("cases", []))
        complaints.extend(d.get("complaints", []))
    return passengers, flights, cases, complaints



# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    available = _available()
    parser = argparse.ArgumentParser(
        description="Produce ZavaAir complaint data to Event Hubs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"Available datasets (from kafka/data/): {', '.join(available)}\nUse --dataset all to produce every dataset in sorted order.",
    )
    parser.add_argument(
        "--dataset", default="seed",
        help="Name of a JSON file in kafka/data/ (without .json), or 'all' for everything. Default: seed",
    )
    args = parser.parse_args()

    if args.dataset == "all":
        p, f, c, co = _load_all()
    else:
        d = _load(args.dataset)
        p  = d.get("passengers", [])
        f  = d.get("flights", [])
        c  = d.get("cases", [])
        co = d.get("complaints", [])

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
