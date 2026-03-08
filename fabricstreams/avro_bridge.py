"""
fabricstreams/avro_bridge.py – Avro→JSON bridge: Azure Event Hub → Fabric Eventstream
=======================================================================================
Consumes Avro-encoded messages from an Azure Event Hub (SASL PLAIN / SAS key),
deserializes each message using the schema identified by the ``entity_type`` Kafka
header, and re-produces the payload as JSON to a Fabric Eventstream Custom Endpoint
(also SASL PLAIN / SAS key).

This bridge exists because Fabric Eventstream Custom Endpoints only accept JSON —
there is no UI option to disable the JSON deserializer.  Customers who already have
Avro data flowing through Azure Event Hubs need this bridge to land records in Fabric
without changing the upstream producer.

Architecture
------------

  ┌────────────────────────────────────┐
  │  Azure Event Hub (dmtcehns)        │  ← source (AEH_CONNECTION_STRING)
  │  topic: custcomplaints             │    Avro binary, entity_type header
  └─────────────────┬──────────────────┘
                    │ SASL PLAIN (SAS key)
                    ▼
  ┌────────────────────────────────────────────────────────────┐
  │  avro_bridge.py                                            │
  │  1. Read entity_type header → select local .avsc schema    │
  │  2. Strip Azure SR wire preamble (4+32 bytes) if present   │
  │  3. fastavro.schemaless_reader → Python dict               │
  │  4. json.dumps → UTF-8 bytes                               │
  │  5. Forward with original headers (ce_* + entity_type)     │
  └─────────────────┬──────────────────────────────────────────┘
                    │ SASL PLAIN (SAS key)
                    ▼
  ┌────────────────────────────────────────────────────────────┐
  │  Fabric Eventstream Custom Endpoint (connection 1)         │  ← FABRIC_CONNECTION_STRING
  │  "Dynamic schema via headers" on entity_type               │
  │  → Eventhouse KQL tables                                   │
  └────────────────────────────────────────────────────────────┘

Delivery semantics
------------------
At-least-once.  Source offsets are committed in batches AFTER the Fabric producer
has flushed successfully.  On crash/restart the same messages are re-read and
re-forwarded; Eventhouse handles deduplication via event_id (UUID v5 from entity
PK) if that column is populated.

Wire-format detection
---------------------
The bridge auto-detects the Azure Schema Registry wire format:
  ``\\x00\\x00\\x00\\x00`` (4-byte preamble) + 32-byte ASCII hex schema GUID
followed by schemaless Avro.  When detected the preamble+GUID are stripped before
passing the remainder to fastavro.  Plain schemaless Avro is also accepted.

Usage
-----
  python -m fabricstreams.avro_bridge                          # run forever
  python -m fabricstreams.avro_bridge --max-idle-secs 60       # exit after 60s idle
  python -m fabricstreams.avro_bridge --group my-bridge-group  # custom consumer group
  python -m fabricstreams.avro_bridge --batch-size 50
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import signal
import sys
import time
from pathlib import Path
from typing import Any

import fastavro
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer
from dotenv import load_dotenv
import os
from utils import mask_key as _mask_key

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
logger = logging.getLogger(__name__)

# ── Schema loading ─────────────────────────────────────────────────────────────

_SCHEMA_DIR = Path(__file__).parent / "schemas"

# entity_type header value → local .avsc filename
_ENTITY_AVSC: dict[str, str] = {
    "Passenger":  "passengers.avsc",
    "Flights":    "flights.avsc",
    "case":       "cases.avsc",
    "complaints": "complaints.avsc",
}

_AVRO_SCHEMAS: dict[str, Any] = {}

def _load_schemas() -> None:
    for entity_type, avsc_file in _ENTITY_AVSC.items():
        path = _SCHEMA_DIR / avsc_file
        with open(path) as f:
            _AVRO_SCHEMAS[entity_type] = fastavro.parse_schema(json.load(f))
    logger.info("Loaded %d Avro schemas from %s", len(_AVRO_SCHEMAS), _SCHEMA_DIR)


# ── Connection string parsing ──────────────────────────────────────────────────

def _parse_connection_string(conn_str: str) -> tuple[str, str]:
    """Return (bootstrap_server, entity_path) from an AEH connection string."""
    parts = dict(seg.split("=", 1) for seg in conn_str.split(";") if "=" in seg)
    host = parts["Endpoint"].removeprefix("sb://").rstrip("/")
    return f"{host}:9093", parts["EntityPath"]


def _sasl_plain_config(conn_str: str, extra: dict | None = None) -> dict:
    bootstrap, _ = _parse_connection_string(conn_str)
    cfg = {
        "bootstrap.servers": bootstrap,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism":    "PLAIN",
        "sasl.username":     "$ConnectionString",
        "sasl.password":     conn_str,
    }
    if extra:
        cfg.update(extra)
    return cfg


# ── Avro → JSON deserialization ────────────────────────────────────────────────

_AEH_SR_PREAMBLE = b"\x00\x00\x00\x00"  # Azure Schema Registry wire format marker
_AEH_SR_HEADER_LEN = 4 + 32              # preamble (4) + schema GUID ASCII (32)


def _decode_avro(value: bytes, entity_type: str) -> bytes:
    """
    Deserialize Avro bytes to JSON.

    Accepts both:
      - plain schemaless Avro
      - Azure Schema Registry wire format (preamble + GUID stripped automatically)

    Returns UTF-8 encoded JSON bytes.
    """
    schema = _AVRO_SCHEMAS.get(entity_type)
    if schema is None:
        raise ValueError(f"Unknown entity_type '{entity_type}' — no schema loaded")

    # Strip Azure Schema Registry wire preamble if present
    if value[:4] == _AEH_SR_PREAMBLE and len(value) > _AEH_SR_HEADER_LEN:
        value = value[_AEH_SR_HEADER_LEN:]

    record = fastavro.schemaless_reader(io.BytesIO(value), schema)
    return json.dumps(record, default=str).encode("utf-8")


# ── Graceful shutdown ──────────────────────────────────────────────────────────

_shutdown = False


def _handle_signal(signum: int, frame: Any) -> None:
    global _shutdown
    logger.info("Signal %s — draining and shutting down…", signum)
    _shutdown = True


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


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

_delivery_errors: list[str] = []


def _on_delivery(err: Any, msg: Any) -> None:
    if err:
        _delivery_errors.append(
            f"topic={msg.topic()} key={_mask_key(msg.key())} error={err}"
        )
        logger.error("Fabric delivery FAILED | topic=%s key=%s error=%s",
                     msg.topic(), _mask_key(msg.key()), err)
    else:
        logger.debug("Fabric delivered | topic=%s partition=%d offset=%d",
                     msg.topic(), msg.partition(), msg.offset())


# ── Bridge ─────────────────────────────────────────────────────────────────────

def run_bridge(
    consumer_group: str = "avro-bridge",
    batch_size: int = 100,
    max_idle_secs: int = 0,
) -> None:
    """
    Consume Avro messages from AEH, transcode to JSON, forward to Fabric.

    Parameters
    ----------
    consumer_group : Kafka consumer group ID
    batch_size     : flush Fabric producer + commit source offsets every N messages
    max_idle_secs  : exit after N consecutive idle seconds (0 = run forever)
    """
    aeh_conn_str = os.environ["AEH_CONNECTION_STRING"]
    fabric_conn_str = os.environ["FABRIC_CONNECTION_STRING"]

    _, source_topic = _parse_connection_string(aeh_conn_str)
    _, fabric_topic = _parse_connection_string(fabric_conn_str)

    # ── Source consumer ────────────────────────────────────────────────────────
    consumer = Consumer(_sasl_plain_config(aeh_conn_str, {
        "group.id":            consumer_group,
        "auto.offset.reset":   "earliest",
        "enable.auto.commit":  False,
        "socket.timeout.ms":   60_000,
        "session.timeout.ms":  30_000,
        "max.poll.interval.ms": 300_000,
    }))
    consumer.subscribe([source_topic])
    logger.info("Consumer subscribed | source=%s group=%s", source_topic, consumer_group)

    # ── Fabric producer ────────────────────────────────────────────────────────
    fabric_producer = Producer(_sasl_plain_config(fabric_conn_str, {
        "acks":              "all",
        "retries":           5,
        "retry.backoff.ms":  1_000,
        "socket.timeout.ms": 60_000,
        "linger.ms":         20,
        "message.max.bytes": 1_000_000,
    }))
    logger.info("Fabric producer ready | topic=%s", fabric_topic)

    total_forwarded = 0
    total_skipped   = 0
    batch: list[Message] = []
    idle_since: float | None = None

    def _flush_and_commit() -> None:
        nonlocal total_forwarded, batch
        if not batch:
            return
        fabric_producer.flush()
        if _delivery_errors:
            logger.error(
                "%d delivery error(s) — offsets NOT committed. Errors: %s",
                len(_delivery_errors), _delivery_errors,
            )
            _delivery_errors.clear()
            batch.clear()
            return
        for msg in batch:
            consumer.commit(msg)
        total_forwarded += len(batch)
        logger.info("Batch committed | size=%d total_forwarded=%d total_skipped=%d",
                    len(batch), total_forwarded, total_skipped)
        batch.clear()

    try:
        while not _shutdown:
            msg: Message | None = consumer.poll(timeout=1.0)

            if msg is None:
                if batch:
                    _flush_and_commit()
                    idle_since = None
                else:
                    if max_idle_secs > 0:
                        if idle_since is None:
                            idle_since = time.monotonic()
                        elif time.monotonic() - idle_since >= max_idle_secs:
                            logger.info(
                                "No new messages for %ds — exiting.", max_idle_secs
                            )
                            break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("EOF | %s [%d] @ %d",
                                 msg.topic(), msg.partition(), msg.offset())
                else:
                    raise KafkaException(msg.error())
                continue

            idle_since = None

            # Decode entity_type header
            headers: dict[str, bytes] = {
                k: v for k, v in (msg.headers() or [])
            }
            entity_type_bytes = headers.get("entity_type")
            if not entity_type_bytes:
                logger.warning(
                    "Message at offset %d has no entity_type header — skipping",
                    msg.offset(),
                )
                total_skipped += 1
                consumer.commit(msg)  # don't re-read unroutable messages
                continue

            entity_type = entity_type_bytes.decode("utf-8")

            # Transcode Avro → JSON
            try:
                json_value = _decode_avro(msg.value(), entity_type)
            except Exception as exc:
                logger.warning(
                    "Avro decode failed | entity_type=%s offset=%d error=%s — skipping",
                    entity_type, msg.offset(), exc,
                )
                total_skipped += 1
                consumer.commit(msg)
                continue

            # Forward to Fabric with original headers preserved
            while True:
                try:
                    fabric_producer.produce(
                        topic=fabric_topic,
                        key=msg.key(),
                        value=json_value,
                        headers=list(headers.items()),
                        on_delivery=_on_delivery,
                    )
                    break
                except BufferError:
                    logger.warning("Fabric producer queue full — draining…")
                    fabric_producer.poll(0.5)

            batch.append(msg)
            fabric_producer.poll(0)

            if len(batch) >= batch_size:
                _flush_and_commit()

    finally:
        logger.info("Shutdown: flushing %d remaining message(s)…", len(batch))
        _flush_and_commit()
        consumer.close()
        logger.info(
            "Bridge stopped. total_forwarded=%d total_skipped=%d",
            total_forwarded, total_skipped,
        )


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    _load_schemas()

    parser = argparse.ArgumentParser(
        description="Avro→JSON bridge: Azure Event Hub → Fabric Eventstream Custom Endpoint"
    )
    parser.add_argument(
        "--group",
        default="avro-bridge",
        help="Kafka consumer group ID (default: avro-bridge)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Flush + commit every N messages (default: 100)",
    )
    parser.add_argument(
        "--max-idle-secs",
        type=int,
        default=0,
        help="Exit after N seconds with no new messages. 0 = run forever (default: 0)",
    )
    args = parser.parse_args()

    try:
        run_bridge(
            consumer_group=args.group,
            batch_size=args.batch_size,
            max_idle_secs=args.max_idle_secs,
        )
    except KafkaException as exc:
        logger.error("Kafka error: %s", exc)
        sys.exit(1)
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)
        sys.exit(1)
