-- ============================================================================
-- Lunar Air Customer Complaints – Table Definitions (Reset + PNR + Cases)
-- ============================================================================

SET search_path TO custcomplaints, public;

-- Hard reset for clean local/remote testing
DROP TABLE IF EXISTS custcomplaints.complaints CASCADE;
DROP TABLE IF EXISTS custcomplaints.cases CASCADE;
DROP TABLE IF EXISTS custcomplaints.flights CASCADE;
DROP TABLE IF EXISTS custcomplaints.passengers CASCADE;

-- ── Passengers ──────────────────────────────────────────────────────────────

CREATE TABLE custcomplaints.passengers (
    passenger_id          BIGINT        PRIMARY KEY,
    first_name            VARCHAR(100)  NOT NULL,
    last_name             VARCHAR(100)  NOT NULL,
    email                 VARCHAR(255),
    phone                 VARCHAR(50),
    country               VARCHAR(100),
    frequent_flyer_tier   VARCHAR(20)   NOT NULL DEFAULT 'None' CHECK (frequent_flyer_tier IN ('None', 'Bronze', 'Silver', 'Gold', 'Platinum')),
    total_flights         BIGINT        NOT NULL DEFAULT 0,
    member_since          TIMESTAMP,
    CHECK (email IS NOT NULL OR phone IS NOT NULL)
);

COMMENT ON TABLE custcomplaints.passengers IS 'Passenger master data with name + phone/email identity for complaint intake';

CREATE UNIQUE INDEX idx_passengers_identity
ON custcomplaints.passengers (
  lower(first_name),
  lower(last_name),
  COALESCE(lower(email), ''),
  COALESCE(phone, '')
);

-- ── Flights ─────────────────────────────────────────────────────────────────

CREATE TABLE custcomplaints.flights (
    flight_id             BIGINT        PRIMARY KEY,
    flight_number         VARCHAR(10)   NOT NULL UNIQUE,
    origin_code           VARCHAR(3)    NOT NULL,
    origin_city           VARCHAR(100)  NOT NULL,
    destination_code      VARCHAR(3)    NOT NULL,
    destination_city      VARCHAR(100)  NOT NULL,
    scheduled_departure   TIMESTAMPTZ   NOT NULL,
    actual_departure      TIMESTAMPTZ,
    scheduled_arrival     TIMESTAMPTZ   NOT NULL,
    actual_arrival        TIMESTAMPTZ,
    aircraft_type         VARCHAR(50)   NOT NULL,
    flight_status         VARCHAR(20)   NOT NULL CHECK (flight_status IN ('On Time', 'Delayed', 'Cancelled', 'Diverted', 'Scheduled', 'Departed')),
    delay_minutes         BIGINT        NOT NULL DEFAULT 0
);

COMMENT ON TABLE custcomplaints.flights IS 'Flight operations data for complaint linkage';

-- ── Cases (one case per passenger+flight+PNR) ─────────────────────────────

CREATE TABLE custcomplaints.cases (
    case_id               BIGINT        PRIMARY KEY,
    passenger_id          BIGINT        NOT NULL REFERENCES custcomplaints.passengers(passenger_id),
    flight_id             BIGINT        NOT NULL REFERENCES custcomplaints.flights(flight_id),
    flight_number         VARCHAR(10)   NOT NULL,
    pnr                   VARCHAR(20)   NOT NULL,
    case_status           VARCHAR(20)   NOT NULL DEFAULT 'Open' CHECK (case_status IN ('Open', 'Under Review', 'Resolved', 'Closed', 'Escalated')),
    opened_at             TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    last_updated_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    closed_at             TIMESTAMPTZ,
    UNIQUE (passenger_id, flight_id, pnr)
);

COMMENT ON TABLE custcomplaints.cases IS 'Case tracking table to identify whether a new complaint is an existing case';

-- ── Complaints ──────────────────────────────────────────────────────────────

CREATE TABLE custcomplaints.complaints (
    complaint_id          BIGINT        PRIMARY KEY,
    case_id               BIGINT        NOT NULL REFERENCES custcomplaints.cases(case_id),
    passenger_id          BIGINT        NOT NULL REFERENCES custcomplaints.passengers(passenger_id),
    flight_id             BIGINT        NOT NULL REFERENCES custcomplaints.flights(flight_id),
    flight_number         VARCHAR(10)   NOT NULL,
    pnr                   VARCHAR(20)   NOT NULL,
    complaint_date        TIMESTAMPTZ   NOT NULL,
    category              VARCHAR(50)   NOT NULL,
    subcategory           VARCHAR(100)  NOT NULL,
    description           VARCHAR(4000) NOT NULL,
    severity              VARCHAR(20)   NOT NULL CHECK (severity IN ('Low', 'Medium', 'High', 'Critical')),
    status                VARCHAR(20)   NOT NULL DEFAULT 'Open' CHECK (status IN ('Open', 'Under Review', 'Resolved', 'Closed', 'Escalated')),
    assigned_agent        VARCHAR(100),
    resolution_notes      VARCHAR(4000),
    resolution_date       TIMESTAMPTZ,
    satisfaction_score    DECIMAL(2,1) CHECK (satisfaction_score >= 1.0 AND satisfaction_score <= 5.0)
);

COMMENT ON TABLE custcomplaints.complaints IS 'Complaint records linked to a case and ticket identity (PNR)';

-- ── Indexes ─────────────────────────────────────────────────────────────────

CREATE INDEX idx_cases_status               ON custcomplaints.cases (case_status);
CREATE INDEX idx_cases_lookup               ON custcomplaints.cases (passenger_id, flight_id, pnr);

CREATE INDEX idx_complaints_case            ON custcomplaints.complaints (case_id);
CREATE INDEX idx_complaints_status          ON custcomplaints.complaints (status);
CREATE INDEX idx_complaints_severity        ON custcomplaints.complaints (severity);
CREATE INDEX idx_complaints_passenger       ON custcomplaints.complaints (passenger_id);
CREATE INDEX idx_complaints_flight          ON custcomplaints.complaints (flight_id);
CREATE INDEX idx_complaints_pnr             ON custcomplaints.complaints (pnr);
CREATE INDEX idx_complaints_category        ON custcomplaints.complaints (category, subcategory);
CREATE INDEX idx_complaints_date            ON custcomplaints.complaints (complaint_date DESC);
