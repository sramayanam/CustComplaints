"""
fabric_consumer.py – Bridge: Event Hubs → Fabric Eventstream Custom Endpoint
=============================================================================
Consumes messages from a source Event Hub (aaaorgehns) and forwards them
verbatim to a Microsoft Fabric Eventstream Custom Endpoint using SASL
OAUTHBEARER on both sides.

No SAS keys or connection strings are used.  Both connections authenticate
via the same UAMI (aaaorguamgdidentity) through DefaultAzureCredential –
falling back to AzureCliCredential for local dev after `az login`.

Architecture
------------

  ┌──────────────────────────────────┐
  │ Event Hubs namespace: aaaorgehns │  (source)
  │  topic: custcomplaints.passengers│
  └────────────────┬─────────────────┘
                   │ SASL OAUTHBEARER (UAMI)
                   ▼
  ┌────────────────────────────────────────────────────────────┐
  │  fabric_consumer.py bridge                                  │
  │  Consumer (aaaorgehns)  →  forward verbatim  →  Producer    │
  │  • key, value, headers (schema-id) all preserved            │
  │  • manual offset commit after Fabric delivery confirmed     │
  └────────────────────────────────────────────────────────────┘
                   │ SASL OAUTHBEARER (UAMI)
                   ▼
  ┌────────────────────────────────────────────────────────────┐
  │ Fabric Eventstream Custom Endpoint                          │
  │  namespace: esehblanjyq4f2v00gtf73.servicebus.windows.net  │
  │  topic:     es_8a5b16b5-9233-4f29-ad18-542dc0e3f74d        │
  └────────────────────────────────────────────────────────────┘
                   │
                   ▼
          Fabric Eventhouse / KQL Database

Delivery semantics
------------------
At-least-once: source offsets are committed in batches AFTER the Fabric
producer has successfully flushed the batch.  If the bridge crashes mid-batch
the same messages will be re-read and re-forwarded on restart (idempotent
upsert in Eventhouse handles duplicates via event_id).

Batching (default: 100 messages)
---------------------------------
Messages are accumulated in memory and flushed to Fabric in one
producer.flush() call before the offset batch is committed.  This keeps
end-to-end latency reasonable while avoiding a flush+commit per message.

IAM requirements
----------------
The identity running this script (UAMI or CLI principal) must hold:
  • Azure Event Hubs Data Receiver  – on aaaorgehns namespace (source)
  • Contributor (or higher)         – on the Fabric workspace (destination)

Usage
-----
  # Run with defaults (passengers topic, group=fabric-bridge, batch=100)
  python -m kafka.fabric_consumer

  # Explicit options
  python -m kafka.fabric_consumer \\
      --topic custcomplaints.passengers \\
      --fabric-topic es_8a5b16b5-9233-4f29-ad18-542dc0e3f74d \\
      --group fabric-bridge \\
      --batch-size 50
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
from functools import partial
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer

from .config import (
    CONSUMER_CONFIG_BASE,
    FABRIC_BOOTSTRAP_SERVERS,
    FABRIC_BOOTSTRAP_SERVERS_CASES,
    FABRIC_BOOTSTRAP_SERVERS_COMPLAINTS,
    FABRIC_BOOTSTRAP_SERVERS_FLIGHTS,
    FABRIC_NAMESPACE_FQDN,
    FABRIC_NAMESPACE_FQDN_CASES,
    FABRIC_NAMESPACE_FQDN_COMPLAINTS,
    FABRIC_NAMESPACE_FQDN_FLIGHTS,
    FABRIC_TOPIC_CASES,
    FABRIC_TOPIC_COMPLAINTS,
    FABRIC_TOPIC_FLIGHTS,
    FABRIC_TOPIC_PASSENGERS,
    TOPIC_CASES,
    TOPIC_COMPLAINTS,
    TOPIC_FLIGHTS,
    TOPIC_PASSENGERS,
    _credential,
    oauth_cb,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
logger = logging.getLogger(__name__)


# ── Graceful shutdown ──────────────────────────────────────────────────────────

_shutdown = False


def _handle_signal(signum: int, frame: Any) -> None:
    global _shutdown
    logger.info("Signal %s received — draining and shutting down…", signum)
    _shutdown = True


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


# ── Delivery callback ──────────────────────────────────────────────────────────

_delivery_errors: list[str] = []


def _on_delivery(err: Any, msg: Any) -> None:
    """
    Called by librdkafka for every message after broker ACK or terminal error.
    Errors are collected; the bridge checks this list after each flush.
    """
    if err:
        _delivery_errors.append(
            f"topic={msg.topic()} key={msg.key()} error={err}"
        )
        logger.error("Fabric delivery FAILED | topic=%s key=%s error=%s",
                     msg.topic(), msg.key(), err)
    else:
        logger.debug("Fabric delivered | topic=%s partition=%d offset=%d",
                     msg.topic(), msg.partition(), msg.offset())


# ── Bridge ─────────────────────────────────────────────────────────────────────

# Map each source topic to its Fabric endpoint (namespace_fqdn, bootstrap, fabric_topic)
_TOPIC_FABRIC_MAP: dict[str, tuple[str, str, str]] = {
    TOPIC_PASSENGERS: (FABRIC_NAMESPACE_FQDN,              FABRIC_BOOTSTRAP_SERVERS,              FABRIC_TOPIC_PASSENGERS),
    TOPIC_FLIGHTS:    (FABRIC_NAMESPACE_FQDN_FLIGHTS,       FABRIC_BOOTSTRAP_SERVERS_FLIGHTS,       FABRIC_TOPIC_FLIGHTS),
    TOPIC_CASES:      (FABRIC_NAMESPACE_FQDN_CASES,         FABRIC_BOOTSTRAP_SERVERS_CASES,         FABRIC_TOPIC_CASES),
    TOPIC_COMPLAINTS: (FABRIC_NAMESPACE_FQDN_COMPLAINTS,    FABRIC_BOOTSTRAP_SERVERS_COMPLAINTS,    FABRIC_TOPIC_COMPLAINTS),
}


def run_bridge(
    source_topic: str = TOPIC_PASSENGERS,
    fabric_topic: str | None = None,
    fabric_namespace_fqdn: str | None = None,
    fabric_bootstrap_servers: str | None = None,
    consumer_group: str = "fabric-bridge",
    batch_size: int = 100,
    max_idle_secs: int = 0,
) -> None:
    """
    Consume from the source Event Hub and forward to the Fabric Custom Endpoint.

    Parameters
    ----------
    source_topic             : Event Hub topic to read from
    fabric_topic             : Fabric Custom Endpoint event hub name
                               (auto-resolved from source_topic if not provided)
    fabric_namespace_fqdn    : Fabric namespace FQDN for token scope
                               (auto-resolved from source_topic if not provided)
    fabric_bootstrap_servers : Fabric bootstrap servers
                               (auto-resolved from source_topic if not provided)
    consumer_group           : Kafka consumer group ID (use a dedicated group per bridge)
    batch_size               : flush Fabric producer + commit source offsets every N messages
    max_idle_secs            : exit cleanly after this many consecutive seconds with no new
                               messages.  0 (default) = run forever (persistent mode).
    """
    # Resolve Fabric endpoint from topic map if not explicitly provided
    if source_topic in _TOPIC_FABRIC_MAP and not all(
        [fabric_topic, fabric_namespace_fqdn, fabric_bootstrap_servers]
    ):
        _fqdn, _bootstrap, _ftopic = _TOPIC_FABRIC_MAP[source_topic]
        fabric_namespace_fqdn    = fabric_namespace_fqdn    or _fqdn
        fabric_bootstrap_servers = fabric_bootstrap_servers or _bootstrap
        fabric_topic             = fabric_topic             or _ftopic

    if not all([fabric_topic, fabric_namespace_fqdn, fabric_bootstrap_servers]):
        raise ValueError(
            f"No Fabric endpoint configured for topic '{source_topic}'. "
            "Pass --fabric-topic, or add the topic to _TOPIC_FABRIC_MAP in fabric_consumer.py."
        )
    # ── Source consumer ────────────────────────────────────────────────────────
    consumer = Consumer({
        **CONSUMER_CONFIG_BASE,
        "group.id": consumer_group,
    })
    consumer.subscribe([source_topic])
    logger.info("Consumer subscribed | source=%s group=%s", source_topic, consumer_group)

    # ── Fabric producer ────────────────────────────────────────────────────────
    fabric_producer = Producer({
        "bootstrap.servers": fabric_bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism":    "OAUTHBEARER",
        "oauth_cb":          partial(oauth_cb, _credential, fabric_namespace_fqdn),
        "acks":              "all",
        "retries":           5,
        "retry.backoff.ms":  1_000,
        "socket.timeout.ms": 60_000,
        "linger.ms":         20,
        "message.max.bytes": 1_000_000,
    })
    logger.info("Fabric producer created | bootstrap=%s topic=%s",
                fabric_bootstrap_servers, fabric_topic)

    total_forwarded = 0
    batch: list[Message] = []          # pending messages awaiting flush+commit
    idle_since: float | None = None    # timestamp when idle period started

    def _flush_and_commit() -> None:
        """Flush Fabric producer then commit all messages in the current batch."""
        nonlocal total_forwarded, batch
        if not batch:
            return

        fabric_producer.flush()        # blocks until all queued messages are ACK'd

        if _delivery_errors:
            # At least one message failed terminal delivery — do NOT commit so
            # the batch is re-read on restart.
            logger.error(
                "%d delivery error(s) in batch — offsets NOT committed. Errors: %s",
                len(_delivery_errors), _delivery_errors,
            )
            _delivery_errors.clear()
            batch.clear()
            return

        # All delivered — safe to commit source offsets
        for msg in batch:
            consumer.commit(msg)

        total_forwarded += len(batch)
        logger.info("Batch committed | size=%d total=%d", len(batch), total_forwarded)
        batch.clear()

    try:
        while not _shutdown:
            msg: Message | None = consumer.poll(timeout=1.0)

            if msg is None:
                # Poll timeout — flush any partial batch that has been waiting
                if batch:
                    _flush_and_commit()
                    idle_since = None
                else:
                    # Track consecutive idle time
                    if max_idle_secs > 0:
                        if idle_since is None:
                            import time
                            idle_since = time.monotonic()
                        elif time.monotonic() - idle_since >= max_idle_secs:
                            logger.info(
                                "No new messages for %ds — exiting (max_idle_secs=%d).",
                                max_idle_secs, max_idle_secs,
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

            # Forward verbatim: key, value, and all headers (schema-id etc.)
            while True:
                try:
                    fabric_producer.produce(
                        topic=fabric_topic,
                        key=msg.key(),
                        value=msg.value(),
                        headers=msg.headers() or [],
                        on_delivery=_on_delivery,
                    )
                    break
                except BufferError:
                    logger.warning("Fabric producer queue full — polling to drain…")
                    fabric_producer.poll(0.5)

            batch.append(msg)
            idle_since = None  # reset idle timer on every message received

            # Trigger delivery callbacks for already-sent messages
            fabric_producer.poll(0)

            if len(batch) >= batch_size:
                _flush_and_commit()

    finally:
        logger.info("Shutdown: flushing remaining %d message(s)…", len(batch))
        _flush_and_commit()
        consumer.close()
        logger.info("Bridge stopped. Total forwarded: %d", total_forwarded)


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    _topic_choices = list(_TOPIC_FABRIC_MAP.keys())

    parser = argparse.ArgumentParser(
        description="Event Hubs → Fabric Eventstream bridge consumer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Supported topics:\n" + "\n".join(f"  {t}" for t in _topic_choices),
    )
    parser.add_argument(
        "--topic",
        default=TOPIC_PASSENGERS,
        choices=_topic_choices,
        help=f"Source Event Hub topic (default: {TOPIC_PASSENGERS})",
    )
    parser.add_argument(
        "--fabric-topic",
        default=None,
        help="Override Fabric Custom Endpoint event hub name (auto-resolved from --topic if omitted)",
    )
    parser.add_argument(
        "--group",
        default="fabric-bridge",
        help="Kafka consumer group ID (default: fabric-bridge)",
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
            source_topic=args.topic,
            fabric_topic=args.fabric_topic,
            consumer_group=args.group,
            batch_size=args.batch_size,
            max_idle_secs=args.max_idle_secs,
        )
    except KafkaException as exc:
        logger.error("Kafka error: %s", exc)
        sys.exit(1)
    except Exception as exc:
        logger.error("Unexpected error: %s", exc)
        sys.exit(1)
