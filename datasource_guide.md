# Data Source Guide for Airline Customer Complaints Agent

## Overview

Every unhappy passenger leaves a trail. Four KQL tables capture that trail —
from the moment a traveller boards (`passengersnew`) to the flight they regret
(`flightsn`), the case they raised (`casesnew`), and the complaint that tells
the real story (`complaintsnew`). Data streams live from Azure Event Hubs through
Fabric Eventstream. Each record carries a deterministic `event_id` (UUID v5) so
re-runs never double-count — the same seat complaint is always the same row.

---

## Entity Relationship

```
passengersnew ──┐
                ├──► casesnew ──► complaintsnew
flightsn ───────┘
```

**FK-safe query order:** `passengersnew` → `flightsn` → `casesnew` → `complaintsnew`

---

## Table: `passengersnew`

**What it represents:** One row per unique airline passenger enrolled in the
loyalty programme.

**Primary key:** `passenger_id` (long)

| Column | Type | Notes |
|--------|------|-------|
| `event_id` | string | UUID v5 dedup key — unique per passenger_id |
| `passenger_id` | long | PK — join target for casesnew, complaintsnew |
| `first_name` | string | |
| `last_name` | string | |
| `email` | string | nullable |
| `phone` | string | nullable |
| `country` | string | 2-letter ISO or full country name |
| `frequent_flyer_tier` | string | `None` \| `Bronze` \| `Silver` \| `Gold` \| `Platinum` |
| `total_flights` | long | Lifetime flight count |
| `member_since` | string | ISO-8601 timestamp with UTC offset |

**Key patterns:**
```kql
// Tier distribution
passengersnew | summarize count() by frequent_flyer_tier

// High-value passengers (Gold+)
passengersnew | where frequent_flyer_tier in ("Gold", "Platinum")

// Parse member_since as datetime
passengersnew | extend joined = todatetime(member_since)
```

**Agent tips:**
- Always use `passenger_id` (not name) as the join key — names are not unique.
- `frequent_flyer_tier = "None"` means the passenger has no tier, not a null value.
- `total_flights` reflects historical data at ingestion time, not a live counter.

---

## Table: `flightsn`

**What it represents:** One row per flight operated by the airline. A flight is a
specific departure on a specific date (not a route template).

**Primary key:** `flight_id` (long)

| Column | Type | Notes |
|--------|------|-------|
| `event_id` | string | UUID v5 dedup key |
| `flight_id` | long | PK — join target for casesnew, complaintsnew |
| `flight_number` | string | e.g. `ZA707` — unique per flight |
| `origin_code` | string | IATA 3-letter airport code |
| `origin_city` | string | |
| `destination_code` | string | IATA 3-letter airport code |
| `destination_city` | string | |
| `scheduled_departure` | string | ISO-8601 UTC offset |
| `actual_departure` | string | nullable — null if not yet departed |
| `scheduled_arrival` | string | ISO-8601 UTC offset |
| `actual_arrival` | string | nullable |
| `aircraft_type` | string | e.g. `Boeing 737-800` |
| `flight_status` | string | `On Time` \| `Delayed` \| `Cancelled` \| `Diverted` \| `Scheduled` \| `Departed` |
| `delay_minutes` | long | 0 if on time; positive integer if delayed |

**Key patterns:**
```kql
// Compute route label
flightsn | extend route = strcat(origin_code, " → ", destination_code)

// Flights with significant delays
flightsn | where delay_minutes > 60

// Parse departure as datetime
flightsn | extend dep_dt = todatetime(scheduled_departure)

// Status breakdown
flightsn | summarize count() by flight_status
```

**Agent tips:**
- `flight_number` is unique per flight but may repeat across dates (same route,
  different day). Use `flight_id` as the authoritative join key.
- `delay_minutes = 0` does not imply `flight_status = "On Time"` — check status
  explicitly for cancelled/diverted flights.
- All timestamps are strings — use `todatetime()` before any time arithmetic.

---

## Table: `casesnew`

**What it represents:** A customer service case opened for a specific passenger
on a specific flight. One passenger can have multiple cases (multiple flights or
multiple incidents on the same flight).

**Primary key:** `case_id` (long)
**Foreign keys:** `passenger_id` → `passengersnew`, `flight_id` → `flightsn`

| Column | Type | Notes |
|--------|------|-------|
| `event_id` | string | UUID v5 dedup key |
| `case_id` | long | PK — join target for complaintsnew |
| `passenger_id` | long | FK → passengersnew |
| `flight_id` | long | FK → flightsn |
| `flight_number` | string | Denormalised copy for convenience |
| `pnr` | string | Booking reference (Passenger Name Record) |
| `case_status` | string | `Open` \| `Under Review` \| `Resolved` \| `Closed` \| `Escalated` |
| `opened_at` | string | ISO-8601 UTC offset |
| `last_updated_at` | string | ISO-8601 UTC offset |
| `closed_at` | string | nullable — null while still open |

**Key patterns:**
```kql
// Open cases by status
casesnew | summarize count() by case_status

// Cases opened per day
casesnew
| extend day = startofday(todatetime(opened_at))
| summarize cases=count() by day | order by day asc

// Multi-case passengers (repeat complainers)
casesnew
| summarize case_count=count() by passenger_id
| where case_count > 1
| join kind=inner passengersnew on passenger_id
| project first_name, last_name, case_count

// Resolution time for closed cases
casesnew
| where isnotempty(closed_at)
| extend hours_open = datetime_diff("hour", todatetime(closed_at), todatetime(opened_at))
| summarize avg_hours=avg(hours_open), p90_hours=percentile(hours_open, 90)
```

**Agent tips:**
- `flight_number` is denormalised here and in `complaintsnew` for convenience, but
  always join on `flight_id` for correctness if the flight's details are needed.
- A passenger with `case_count > 1` is a meaningful signal — high-value passengers
  with repeated issues are escalation candidates.
- `case_status = "Escalated"` is the most urgent bucket — prioritise these in any
  SLA or workload analysis.

---

## Table: `complaintsnew`

**What it represents:** Individual complaint items within a case. One case can
contain multiple complaints (e.g. seat malfunction AND meal quality on the same flight).

**Primary key:** `complaint_id` (long)
**Foreign keys:** `case_id` → `casesnew`, `passenger_id` → `passengersnew`,
`flight_id` → `flightsn`

| Column | Type | Notes |
|--------|------|-------|
| `event_id` | string | UUID v5 dedup key |
| `complaint_id` | long | PK |
| `case_id` | long | FK → casesnew |
| `passenger_id` | long | FK → passengersnew |
| `flight_id` | long | FK → flightsn |
| `flight_number` | string | Denormalised copy |
| `pnr` | string | Booking reference |
| `complaint_date` | string | ISO-8601 UTC offset |
| `category` | string | High-level complaint type (e.g. `Seating`, `Catering`) |
| `subcategory` | string | Specific issue (e.g. `Seat Malfunction`, `Cold Meal`) |
| `description` | string | Free-text passenger narrative |
| `severity` | string | `Low` \| `Medium` \| `High` \| `Critical` |
| `status` | string | `Open` \| `Under Review` \| `Resolved` \| `Closed` \| `Escalated` |
| `assigned_agent` | string | nullable — agent name if assigned |
| `resolution_notes` | string | nullable |
| `resolution_date` | string | nullable — ISO-8601 UTC offset |
| `satisfaction_score` | string | nullable — `"1.0"` – `"5.0"` decimal string |

**Key patterns:**
```kql
// Volume by category + severity (heatmap / stacked bar)
complaintsnew | summarize count() by category, severity

// Parse satisfaction score (stored as string)
complaintsnew
| where isnotempty(satisfaction_score)
| extend score = todouble(satisfaction_score)
| summarize avg_score=round(avg(score), 2) by category

// Critical unresolved complaints (urgent queue)
complaintsnew
| where severity == "Critical" and status in ("Open", "Under Review")

// Full 4-table join
complaintsnew
| join kind=inner (casesnew      | project case_id, case_status, pnr)          on case_id
| join kind=inner (passengersnew | project passenger_id, first_name, last_name,
                                           frequent_flyer_tier)                 on passenger_id
| join kind=inner (flightsn      | project flight_id, flight_number,
                                           origin_code, destination_code)       on flight_id
| project complaint_id, first_name, last_name, frequent_flyer_tier,
          flight_number, origin_code, destination_code,
          category, subcategory, severity, status, case_status
```

**Agent tips:**
- `satisfaction_score` is ingested as a string — always cast with `todouble()` before
  averaging or filtering numerically.
- `severity` and `status` are independent axes: a `Critical` complaint can be `Resolved`,
  and a `Low` complaint can be `Escalated`. Query both when assessing risk.
- `description` is free text — suitable for keyword search with `has` or `contains`,
  or for summarisation by an LLM agent.
- When joining all four tables, always start from `complaintsnew` and join inward
  (left-to-right FK direction) for best query performance in KQL.

---

## Cross-Table Query Cookbook

### Complaint intensity by route
```kql
complaintsnew
| join kind=inner (flightsn | project flight_id, origin_code, destination_code) on flight_id
| extend route = strcat(origin_code, " → ", destination_code)
| summarize complaints=count(), critical=countif(severity=="Critical") by route
| order by complaints desc
```

### Tier-based satisfaction gap
```kql
complaintsnew
| where isnotempty(satisfaction_score)
| extend score = todouble(satisfaction_score)
| join kind=inner (passengersnew | project passenger_id, frequent_flyer_tier) on passenger_id
| summarize avg_score=round(avg(score),2), n=count() by frequent_flyer_tier
| order by avg_score asc
```

### Escalated cases with passenger context
```kql
casesnew
| where case_status == "Escalated"
| join kind=inner passengersnew on passenger_id
| join kind=inner flightsn      on flight_id
| project case_id, first_name, last_name, frequent_flyer_tier,
          flight_number, flight_status, opened_at, last_updated_at
| order by todatetime(opened_at) desc
```

### Duplicate check (should return 0 rows)
```kql
union
  (passengersnew | summarize n=count() by event_id | where n > 1 | extend tbl="passengers"),
  (flightsn      | summarize n=count() by event_id | where n > 1 | extend tbl="flights"),
  (casesnew      | summarize n=count() by event_id | where n > 1 | extend tbl="cases"),
  (complaintsnew | summarize n=count() by event_id | where n > 1 | extend tbl="complaints")
| project tbl, event_id, n
```

---

## Deduplication Contract

Every row carries `event_id` — a **UUID v5** derived deterministically from
`"{topic}:{primary_key}"`. The same source record always produces the same
`event_id` regardless of how many times the producer re-runs. Use `event_id`
as the upsert key when writing consumers or update policies.
