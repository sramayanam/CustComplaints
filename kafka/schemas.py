"""
schemas.py – Pydantic message envelope schemas for all four Event Hub topics
=============================================================================
Design goals
------------
- One envelope per Kafka message.  The `table` field lets the consumer route
  to the correct PostgreSQL / Fabric table without inspecting the topic name.
- All data types are chosen to roundtrip cleanly through:
    JSON (Kafka payload)  →  Python (producer / consumer)
    →  PostgreSQL (BIGINT, VARCHAR, TIMESTAMPTZ, DECIMAL, BOOLEAN)
    →  Microsoft Fabric / Delta Lake (LONG, STRING, TIMESTAMP, DECIMAL)

Type-mapping notes
------------------
  PostgreSQL BIGINT        →  Python int       →  JSON number   →  Fabric LONG
  PostgreSQL VARCHAR(n)    →  Python str        →  JSON string   →  Fabric STRING
  PostgreSQL TIMESTAMPTZ   →  Python str ISO-8601 with offset    →  Fabric TIMESTAMP
  PostgreSQL DECIMAL(2,1)  →  Python float      →  JSON number   →  Fabric DECIMAL(2,1)
  Nullable columns         →  Python Optional[T] (None → JSON null)

Timestamps are always serialised as ISO-8601 strings with UTC offset
(e.g. "2026-03-01T13:00:00+00:00") – this format is unambiguous in every
downstream system and avoids epoch integer precision issues in Fabric Spark.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator


# ── Shared envelope header ────────────────────────────────────────────────────

class EventEnvelope(BaseModel):
    """
    Wraps every record sent to Event Hubs.

    Fields
    ------
    event_id      : UUIDv4 – deduplication key for idempotent consumer upserts.
    table         : target table name; consumer uses this for routing.
    schema_version: semver string; bump when payload shape changes.
    produced_at   : UTC timestamp when the producer serialised this message.
    payload       : the actual record dict (validated by the table-specific model).
    """

    event_id:       str = Field(default_factory=lambda: str(uuid.uuid4()))
    table:          str
    schema_version: str = "1.0"
    produced_at:    str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    payload:        dict


# ── Passengers ────────────────────────────────────────────────────────────────

class PassengerPayload(BaseModel):
    """Maps to custcomplaints.passengers"""

    passenger_id:        int                   # BIGINT PK
    first_name:          str                   # VARCHAR(100)
    last_name:           str                   # VARCHAR(100)
    email:               Optional[str] = None  # VARCHAR(255) nullable
    phone:               Optional[str] = None  # VARCHAR(50)  nullable
    country:             Optional[str] = None  # VARCHAR(100)
    frequent_flyer_tier: str = "None"          # VARCHAR(20) CHECK
    total_flights:       int = 0               # BIGINT
    member_since:        Optional[str] = None  # TIMESTAMPTZ as ISO-8601 string

    @field_validator("frequent_flyer_tier")
    @classmethod
    def _valid_tier(cls, v: str) -> str:
        allowed = {"None", "Bronze", "Silver", "Gold", "Platinum"}
        if v not in allowed:
            raise ValueError(f"frequent_flyer_tier must be one of {allowed}")
        return v


# ── Flights ───────────────────────────────────────────────────────────────────

class FlightPayload(BaseModel):
    """Maps to custcomplaints.flights"""

    flight_id:           int            # BIGINT PK
    flight_number:       str            # VARCHAR(10) UNIQUE
    origin_code:         str            # VARCHAR(3)
    origin_city:         str            # VARCHAR(100)
    destination_code:    str            # VARCHAR(3)
    destination_city:    str            # VARCHAR(100)
    scheduled_departure: str            # TIMESTAMPTZ → ISO-8601 string
    actual_departure:    Optional[str] = None
    scheduled_arrival:   str
    actual_arrival:      Optional[str] = None
    aircraft_type:       str            # VARCHAR(50)
    flight_status:       str            # VARCHAR(20) CHECK
    delay_minutes:       int = 0        # BIGINT

    @field_validator("flight_status")
    @classmethod
    def _valid_status(cls, v: str) -> str:
        allowed = {"On Time", "Delayed", "Cancelled", "Diverted", "Scheduled", "Departed"}
        if v not in allowed:
            raise ValueError(f"flight_status must be one of {allowed}")
        return v


# ── Cases ─────────────────────────────────────────────────────────────────────

class CasePayload(BaseModel):
    """
    Maps to custcomplaints.cases
    FK deps: passenger_id → passengers, flight_id → flights
    Consumer must load passengers + flights before cases.
    """

    case_id:         int             # BIGINT PK
    passenger_id:    int             # FK → passengers
    flight_id:       int             # FK → flights
    flight_number:   str             # VARCHAR(10)
    pnr:             str             # VARCHAR(20)
    case_status:     str = "Open"    # VARCHAR(20) CHECK
    opened_at:       str             # TIMESTAMPTZ → ISO-8601 string
    last_updated_at: str
    closed_at:       Optional[str] = None

    @field_validator("case_status")
    @classmethod
    def _valid_status(cls, v: str) -> str:
        allowed = {"Open", "Under Review", "Resolved", "Closed", "Escalated"}
        if v not in allowed:
            raise ValueError(f"case_status must be one of {allowed}")
        return v


# ── Complaints ────────────────────────────────────────────────────────────────

class ComplaintPayload(BaseModel):
    """
    Maps to custcomplaints.complaints
    FK deps: case_id → cases, passenger_id → passengers, flight_id → flights
    Consumer must load cases (and transitively passengers + flights) first.
    """

    complaint_id:       int                    # BIGINT PK
    case_id:            int                    # FK → cases
    passenger_id:       int                    # FK → passengers
    flight_id:          int                    # FK → flights
    flight_number:      str                    # VARCHAR(10)
    pnr:                str                    # VARCHAR(20)
    complaint_date:     str                    # TIMESTAMPTZ → ISO-8601 string
    category:           str                    # VARCHAR(50)
    subcategory:        str                    # VARCHAR(100)
    description:        str                    # VARCHAR(4000)
    severity:           str                    # VARCHAR(20) CHECK
    status:             str = "Open"           # VARCHAR(20) CHECK
    assigned_agent:     Optional[str] = None   # VARCHAR(100)
    resolution_notes:   Optional[str] = None   # VARCHAR(4000)
    resolution_date:    Optional[str] = None   # TIMESTAMPTZ → ISO-8601 string
    satisfaction_score: Optional[float] = None  # DECIMAL(2,1) – 1.0–5.0

    @field_validator("severity")
    @classmethod
    def _valid_severity(cls, v: str) -> str:
        allowed = {"Low", "Medium", "High", "Critical"}
        if v not in allowed:
            raise ValueError(f"severity must be one of {allowed}")
        return v

    @field_validator("status")
    @classmethod
    def _valid_status(cls, v: str) -> str:
        allowed = {"Open", "Under Review", "Resolved", "Closed", "Escalated"}
        if v not in allowed:
            raise ValueError(f"status must be one of {allowed}")
        return v

    @field_validator("satisfaction_score")
    @classmethod
    def _valid_score(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (1.0 <= v <= 5.0):
            raise ValueError("satisfaction_score must be between 1.0 and 5.0")
        return v


# ── Factory helpers ───────────────────────────────────────────────────────────

def make_envelope(table: str, payload: BaseModel) -> EventEnvelope:
    """Wrap a validated payload model into a sendable EventEnvelope."""
    return EventEnvelope(table=table, payload=payload.model_dump())
