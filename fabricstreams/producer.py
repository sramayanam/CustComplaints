"""
fabricstreams/producer.py  –  Fabric Eventstream Kafka producer (all 4 entity types)
======================================================================================
Protocol  : Apache Kafka with SASL PLAIN (SAS key)
Endpoint  : Fabric Eventstream custom endpoint
            All 4 types share the same namespace/topic, differentiated by ce_type header.
Format    : JSON  –  raw dict, no envelope wrapper

Usage
-----
  python -m fabricstreams.producer                        # seed  – all 4 types
  python -m fabricstreams.producer --dataset test         # batch 1
  python -m fabricstreams.producer --dataset test2        # batch 2
  python -m fabricstreams.producer --dataset test3        # batch 3
  python -m fabricstreams.producer --dataset all          # everything
  python -m fabricstreams.producer --types complaints     # complaints only
  python -m fabricstreams.producer --types passengers flights cases complaints
"""

from __future__ import annotations

import json
import logging
import os
import sys
import uuid

from confluent_kafka import KafkaException, Producer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
logger = logging.getLogger(__name__)


# ── Connection ─────────────────────────────────────────────────────────────────

def _parse_connection_string(conn_str: str) -> tuple[str, str]:
    """Return (bootstrap_server, topic) from a Fabric/Event Hubs connection string.

    Connection string format:
      Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=...;
      SharedAccessKey=...;EntityPath=<topic>
    """
    parts = dict(seg.split("=", 1) for seg in conn_str.split(";") if "=" in seg)
    host = parts["Endpoint"].removeprefix("sb://").rstrip("/")
    return f"{host}:9093", parts["EntityPath"]


_SCHEMA_SET_ID = os.environ.get("FABRIC_SCHEMA_SET_ID", "998f81b5-ceda-4f80-8b17-409388e5d508")
_SCHEMA_REGISTRY_BASE = (
    f"https://rthprodbn73280331.eastus2.messagingcatalog.azure.net"
    f"/schemagroups/{_SCHEMA_SET_ID}/schemas"
)

# Per-type config: key = ce_type header value (must match schema name in Fabric EventSchemaSet)
# Confirmed from Fabric-generated sample code: ce_type == schema name (case-sensitive)
_TYPE_CONFIG: dict[str, dict] = {
    "complaints": {"pk": "complaint_id", "schema": "complaints"},
    "Flights":    {"pk": "flight_id",    "schema": "Flights"},
    "Passenger":  {"pk": "passenger_id", "schema": "Passenger"},
    "case":       {"pk": "case_id",      "schema": "case"},
}

# Single connection string shared by all 4 types
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

def _send(producer: Producer, topic: str, entity_type: str, record: dict, pk_field: str, schema_name: str) -> None:
    key = str(record[pk_field]).encode("utf-8")
    value = json.dumps(record, default=str).encode("utf-8")
    dataschema = f"{_SCHEMA_REGISTRY_BASE}/{schema_name}/versions/v1"

    # CloudEvents 1.0 Kafka Protocol Binding headers (ce_ prefix, per spec).
    # ce_type must match the eventType name associated with the schema in Fabric.
    headers = [
        ("ce_specversion",     b"1.0"),
        ("ce_id",              str(uuid.uuid4()).encode()),
        ("ce_type",            entity_type.encode()),
        ("ce_source",          b"fabricstreams/producer"),
        ("ce_datacontenttype", b"application/json"),
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
    """Produce a list of records for the given entity type to Fabric Eventstream."""
    cfg = _TYPE_CONFIG[entity_type]
    producer = Producer(_PRODUCER_CONFIG)

    logger.info(
        "Producer ready | type=%-12s topic=%s records=%d",
        entity_type, TOPIC, len(records),
    )
    for row in records:
        _send(producer, TOPIC, entity_type, row, cfg["pk"], cfg["schema"])
    producer.flush()
    logger.info("%s flushed ✓  (%d records)", entity_type, len(records))


# ── CLI entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    from kafka.producer import (
        SEED_PASSENGERS,   TEST_PASSENGERS,   TEST2_PASSENGERS,   TEST3_PASSENGERS,
        SEED_FLIGHTS,      TEST_FLIGHTS,      TEST2_FLIGHTS,      TEST3_FLIGHTS,
        SEED_CASES,        TEST_CASES,        TEST2_CASES,        TEST3_CASES,
        SEED_COMPLAINTS,   TEST_COMPLAINTS,   TEST2_COMPLAINTS,   TEST3_COMPLAINTS,
    )

    ALL_DATASETS: dict[str, dict[str, list[dict]]] = {
        "seed": {
            "Passenger": SEED_PASSENGERS,
            "Flights":   SEED_FLIGHTS,
            "case":      SEED_CASES,
            "complaints": SEED_COMPLAINTS,
        },
        "test": {
            "Passenger": TEST_PASSENGERS,
            "Flights":   TEST_FLIGHTS,
            "case":      TEST_CASES,
            "complaints": TEST_COMPLAINTS,
        },
        "test2": {
            "Passenger": TEST2_PASSENGERS,
            "Flights":   TEST2_FLIGHTS,
            "case":      TEST2_CASES,
            "complaints": TEST2_COMPLAINTS,
        },
        "test3": {
            "Passenger": TEST3_PASSENGERS,
            "Flights":   TEST3_FLIGHTS,
            "case":      TEST3_CASES,
            "complaints": TEST3_COMPLAINTS,
        },
        "all": {
            "Passenger":  SEED_PASSENGERS + TEST_PASSENGERS + TEST2_PASSENGERS + TEST3_PASSENGERS,
            "Flights":    SEED_FLIGHTS    + TEST_FLIGHTS    + TEST2_FLIGHTS    + TEST3_FLIGHTS,
            "case":       SEED_CASES      + TEST_CASES      + TEST2_CASES      + TEST3_CASES,
            "complaints": SEED_COMPLAINTS + TEST_COMPLAINTS + TEST2_COMPLAINTS + TEST3_COMPLAINTS,
        },
    }

    parser = argparse.ArgumentParser(
        description="Produce ZavaAir data to Fabric Eventstream"
    )
    parser.add_argument(
        "--dataset",
        choices=list(ALL_DATASETS),
        default="seed",
        help="Which batch to send (default: seed)",
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
    logger.info("Dataset: %s | types: %s", args.dataset, ", ".join(args.types))

    try:
        # FK-safe order: Passenger → Flights → case → complaints
        for entity_type in ["Passenger", "Flights", "case", "complaints"]:
            if entity_type in args.types:
                produce_records(entity_type, dataset[entity_type])
    except KafkaException as exc:
        logger.error("Kafka error: %s", exc)
        sys.exit(1)
    except Exception as exc:
        logger.error("Unexpected error: %s", exc)
        sys.exit(1)
