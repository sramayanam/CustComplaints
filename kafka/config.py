"""
config.py – Azure Event Hubs Kafka endpoint configuration (passwordless / UAMI)
================================================================================
Key-based auth is DISABLED on aaaorgehns.
Authentication uses SASL OAUTHBEARER backed by a User-Assigned Managed Identity.

How it works
------------
1. confluent-kafka is configured with sasl.mechanism = OAUTHBEARER.
2. An `oauth_cb` callable is supplied.  Each time librdkafka needs a token it
   calls that function, which uses azure-identity to obtain a short-lived JWT
   from the UAMI endpoint (scope: https://eventhubs.azure.net/.default).
3. No connection string or SAS key is ever stored or transmitted.

Runtime environments
--------------------
- Azure (VM / Container App / ACI / Function with UAMI assigned):
    ManagedIdentityCredential(client_id=UAMI_CLIENT_ID) fetches a V2 app token.
    V2 app tokens are accepted by Event Hubs OAUTHBEARER without needing the
    serviceURL SASL extension.
- Local dev: set USE_MANAGED_IDENTITY=false in .env and ensure the compute
    resource has the UAMI attached, or run from Azure Cloud Shell.

Pre-create four Event Hubs (topics) in namespace aaaorgehns:
  custcomplaints.passengers
  custcomplaints.flights
  custcomplaints.cases
  custcomplaints.complaints

Consumer load-order constraint (FK dependency):
  1. passengers   (no FK)
  2. flights      (no FK)
  3. cases        (FK → passengers, flights)
  4. complaints   (FK → cases, passengers, flights)
"""

import os
import logging
from functools import partial
from typing import Tuple

from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential

load_dotenv()

logger = logging.getLogger(__name__)

# ── Identity ───────────────────────────────────────────────────────────────────

UAMI_CLIENT_ID: str = os.getenv("UAMI_CLIENT_ID", "")

AZURE_TENANT_ID: str = os.getenv("AZURE_TENANT_ID", "")

# DefaultAzureCredential:
#   On Azure compute with UAMI attached  → ManagedIdentityCredential (UAMI)
#   Locally after `az login`             → AzureCliCredential (fallback)
# Pass UAMI_CLIENT_ID only when explicitly configured; None falls back to
# system-assigned identity or AzureCliCredential for local dev.
_credential = DefaultAzureCredential(managed_identity_client_id=UAMI_CLIENT_ID or None)

# ── Namespace / bootstrap ──────────────────────────────────────────────────

EH_NAMESPACE: str = os.getenv("EH_NAMESPACE", "aaaorgehns")
BOOTSTRAP_SERVERS: str = f"{EH_NAMESPACE}.servicebus.windows.net:9093"

# Schema Registry shares the same FQDN as the Event Hubs namespace.
SCHEMA_REGISTRY_ENDPOINT: str = f"https://{EH_NAMESPACE}.servicebus.windows.net"
SCHEMA_GROUP: str = os.getenv("SCHEMA_GROUP", "custcomplaints")

# Namespace FQDN is used BOTH as the kafka bootstrap server AND as the token
# audience scope.  The scope must be namespace-specific:
#   https://<namespace>.servicebus.windows.net/.default
# Using the generic https://eventhubs.azure.net/.default makes Event Hubs
# parse 'eventhubs' as the tenant name and reject the token.
NAMESPACE_FQDN: str = f"{EH_NAMESPACE}.servicebus.windows.net"

# ── Fabric Eventstream Custom Endpoint ────────────────────────────────────────
# Namespace and topic come from the Custom Endpoint panel in Eventstream:
#   Canvas → Custom Endpoint node → Entra ID Authentication tab
FABRIC_EH_NAMESPACE: str = os.getenv("FABRIC_EH_NAMESPACE", "esehblanjyq4f2v00gtf73")
FABRIC_NAMESPACE_FQDN: str = f"{FABRIC_EH_NAMESPACE}.servicebus.windows.net"
FABRIC_BOOTSTRAP_SERVERS: str = f"{FABRIC_NAMESPACE_FQDN}:9093"

# ── Passengers ────────────────────────────────────────────────────────────────
FABRIC_TOPIC_PASSENGERS: str = os.getenv(
    "FABRIC_TOPIC_PASSENGERS", "es_8a5b16b5-9233-4f29-ad18-542dc0e3f74d"
)

# ── Flights ───────────────────────────────────────────────────────────────────
FABRIC_EH_NAMESPACE_FLIGHTS: str = os.getenv("FABRIC_EH_NAMESPACE_FLIGHTS", "esehbloxqn2by5q7b0tzqj")
FABRIC_NAMESPACE_FQDN_FLIGHTS: str = f"{FABRIC_EH_NAMESPACE_FLIGHTS}.servicebus.windows.net"
FABRIC_BOOTSTRAP_SERVERS_FLIGHTS: str = f"{FABRIC_NAMESPACE_FQDN_FLIGHTS}:9093"
FABRIC_TOPIC_FLIGHTS: str = os.getenv(
    "FABRIC_TOPIC_FLIGHTS", "es_8e3eff12-0ab4-43f1-8b80-cd280f0ec557"
)

# ── Cases ───────────────────────────────────────────────────────────────────
FABRIC_EH_NAMESPACE_CASES: str = os.getenv("FABRIC_EH_NAMESPACE_CASES", "esehbls4bhyt0ce9j3pxdm")
FABRIC_NAMESPACE_FQDN_CASES: str = f"{FABRIC_EH_NAMESPACE_CASES}.servicebus.windows.net"
FABRIC_BOOTSTRAP_SERVERS_CASES: str = f"{FABRIC_NAMESPACE_FQDN_CASES}:9093"
FABRIC_TOPIC_CASES: str = os.getenv(
    "FABRIC_TOPIC_CASES", "es_775350b6-9151-4241-a1a1-32bf06736043"
)

# ── Complaints ───────────────────────────────────────────────────────────────
FABRIC_EH_NAMESPACE_COMPLAINTS: str = os.getenv("FABRIC_EH_NAMESPACE_COMPLAINTS", "esehblfbqd47cqpxwx30y4")
FABRIC_NAMESPACE_FQDN_COMPLAINTS: str = f"{FABRIC_EH_NAMESPACE_COMPLAINTS}.servicebus.windows.net"
FABRIC_BOOTSTRAP_SERVERS_COMPLAINTS: str = f"{FABRIC_NAMESPACE_FQDN_COMPLAINTS}:9093"
FABRIC_TOPIC_COMPLAINTS: str = os.getenv(
    "FABRIC_TOPIC_COMPLAINTS", "es_454400f2-20c0-42d2-ab2c-dbefa8cd7cdf"
)


def oauth_cb(cred, namespace_fqdn: str, config: str) -> Tuple[str, float]:
    """
    OAUTHBEARER token callback – matches the official Azure Event Hubs
    for Kafka OAuth2 tutorial pattern exactly.

    confluent-kafka calls this automatically passing sasl.oauthbearer.config
    as `config`.  The scope MUST be namespace-specific so the token aud
    claim is https://<namespace>.servicebus.windows.net, which Event Hubs
    can validate.  The generic https://eventhubs.azure.net scope produces
    a token whose aud is parsed as tenant 'eventhubs' – causing the
    'Invalid tenant name eventhubs' auth error.
    """
    token = cred.get_token(f"https://{namespace_fqdn}/.default")
    return token.token, float(token.expires_on)



# ── Topic names (must match Event Hub names in the namespace) ─────────────────

TOPIC_PASSENGERS: str = os.getenv("TOPIC_PASSENGERS", "custcomplaints.passengers")
TOPIC_FLIGHTS:    str = os.getenv("TOPIC_FLIGHTS",    "custcomplaints.flights")
TOPIC_CASES:      str = os.getenv("TOPIC_CASES",      "custcomplaints.cases")
TOPIC_COMPLAINTS: str = os.getenv("TOPIC_COMPLAINTS", "custcomplaints.complaints")

# Ordered list used by the producer to guarantee FK-safe delivery sequencing
TOPIC_SEND_ORDER: list[str] = [
    TOPIC_PASSENGERS,
    TOPIC_FLIGHTS,
    TOPIC_CASES,
    TOPIC_COMPLAINTS,
]

# ── Producer config ────────────────────────────────────────────────────────────

PRODUCER_CONFIG: dict = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism":    "OAUTHBEARER",
    "oauth_cb":          partial(oauth_cb, _credential, NAMESPACE_FQDN),

    # Reliability
    # NOTE: enable.idempotence is intentionally omitted.
    # Azure Event Hubs' Kafka endpoint does not implement the InitProducerId
    # (PID) API required by idempotent producers.  Setting it True causes
    # librdkafka to fail the PID handshake and then reject every Produce
    # request with UNSUPPORTED_FOR_MESSAGE_FORMAT (error 43).
    "acks":               "all",            # wait for all in-sync replicas
    "retries":            5,
    "retry.backoff.ms":   1_000,

    # Timeouts (Event Hubs-recommended minimums)
    # session.timeout.ms is a CONSUMER-only property – omitted to suppress warning.
    "socket.timeout.ms":  60_000,

    # Throughput (tune per volume)
    "batch.num.messages": 500,
    "linger.ms":          20,
    # compression.type is intentionally omitted (defaults to "none").
    # Event Hubs Standard tier Kafka endpoint does not support compressed
    # record batches – sending gzip/snappy/lz4/zstd produces error 43
    # (UNSUPPORTED_FOR_MESSAGE_FORMAT).  Premium/Dedicated tiers support
    # compression; re-enable here if the namespace is upgraded.

    # Message size limit
    "message.max.bytes":  1_000_000,
}

# ── Consumer config reference (for the downstream PostgreSQL / Fabric loader) ──

CONSUMER_CONFIG_BASE: dict = {
    "bootstrap.servers":  BOOTSTRAP_SERVERS,
    "security.protocol":  "SASL_SSL",
    "sasl.mechanism":     "OAUTHBEARER",
    "oauth_cb":           partial(oauth_cb, _credential, NAMESPACE_FQDN),
    "auto.offset.reset":  "earliest",
    "enable.auto.commit": False,            # manual commit for at-least-once
    "session.timeout.ms": 30_000,
    "socket.timeout.ms":  60_000,
}

