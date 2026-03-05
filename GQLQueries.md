# GQL Queries — Airline Complaints Ontology (Fabric Graph)

## Syntax Rules (confirmed working)
- Labels and relationship types must be **backtick-quoted**: `` `passenger` ``, `` `Case` ``, `` `Complaint` ``, `` `Flight` ``
- `passenger` label is **lowercase** — all others are Title Case
- `GROUP BY` uses **aliases only** (not `node.property`)
- `FILTER` after `GROUP BY` is **not supported** — use `ORDER BY` + `LIMIT` instead
- `TO_JSON_STRING(node)` returns full node/edge as JSON
- **Aliases that match GQL keywords must be backtick-quoted** — `Case`, `Complaint`, `Flight` are reserved; always write `` AS `Case` `` not `AS Case`
- **Direction is strict** — relationships must never be reversed:
  - `(passenger)-[:creates]->(Case)` — never `(Case)-[:creates]->(passenger)`
  - `(Case)-[:generates]->(Complaint)` — never `(Complaint)-[:generates]->(Case)`
  - `(passenger)-[:takes]->(Flight)` — never `(Flight)-[:takes]->(passenger)`

## Relationships
| Relationship | From | To |
|---|---|---|
| `creates` | `passenger` | `Case` |
| `generates` | `Case` | `Complaint` |
| `takes` | `passenger` | `Flight` |

---

## Working Queries

---

### Q1 — Full graph traversal (all 3 relationships)
**What it does:** Returns one row per complaint with full passenger, case, flight and complaint detail as JSON. Good for raw inspection and debugging the graph.

```gql
MATCH (node_Case:`Case`)-[edge1_generates:`generates`]->(node_Complaint:`Complaint`),
      (node_passenger:`passenger`)-[edge2_creates:`creates`]->(node_Case:`Case`),
      (node_passenger:`passenger`)-[edge3_takes:`takes`]->(node_Flight:`Flight`)
RETURN TO_JSON_STRING(node_Case) AS `Case`,
       TO_JSON_STRING(edge1_generates) AS `edge_1_generates`,
       TO_JSON_STRING(node_passenger) AS `passenger`,
       TO_JSON_STRING(edge2_creates) AS `edge_2_creates`,
       TO_JSON_STRING(edge3_takes) AS `edge_3_takes`,
       TO_JSON_STRING(node_Complaint) AS `Complaint`,
       TO_JSON_STRING(node_Flight) AS `Flight`
LIMIT 1000
```

---

### Q2 — Top 5 passengers by complaint volume
**What it does:** Ranks passengers by how many complaints they have filed. Surfaces repeat complainers and high-volume passengers quickly.

```gql
MATCH (node_passenger:`passenger`)-[edge1_creates:`creates`]->(node_Case:`Case`),
      (node_Case:`Case`)-[edge2_generates:`generates`]->(node_Complaint:`Complaint`)
RETURN node_passenger.first_name AS first_name,
       node_passenger.last_name AS last_name,
       node_passenger.frequent_flyer_tier AS tier,
       count(node_Complaint) AS complaints
GROUP BY first_name, last_name, tier
ORDER BY complaints DESC
LIMIT 5
```

---

### Q3 — Multi-case passengers (passengers with more than one case)
**What it does:** Identifies passengers who have opened cases on more than one flight. Uses a double-hop pattern to guarantee at least two distinct cases exist — workaround for missing HAVING support.

```gql
MATCH (node_passenger:`passenger`)-[edge1_creates:`creates`]->(node_Case1:`Case`),
      (node_passenger:`passenger`)-[edge2_creates:`creates`]->(node_Case2:`Case`)
FILTER node_Case1.case_id <> node_Case2.case_id
RETURN DISTINCT node_passenger.first_name AS first_name,
                node_passenger.last_name AS last_name,
                node_passenger.frequent_flyer_tier AS tier
ORDER BY last_name ASC
```

---

### Q4 — All passengers grouped by total case count
**What it does:** Shows every passenger and how many cases they have opened, sorted descending. Passengers with a single case appear at the bottom.

```gql
MATCH (node_passenger:`passenger`)-[edge1_creates:`creates`]->(node_Case:`Case`)
RETURN node_passenger.first_name AS first_name,
       node_passenger.last_name AS last_name,
       node_passenger.frequent_flyer_tier AS tier,
       count(node_Case) AS total_cases
GROUP BY first_name, last_name, tier
ORDER BY total_cases DESC
```

---

### Q5 — Escalated cases with passenger tier
**What it does:** Lists all escalated cases alongside the passenger's loyalty tier. Useful for identifying whether high-value passengers are receiving timely escalation handling.

```gql
MATCH (node_passenger:`passenger`)-[edge1_creates:`creates`]->(node_Case:`Case`)
FILTER node_Case.case_status = 'Escalated'
RETURN node_passenger.first_name AS first_name,
       node_passenger.last_name AS last_name,
       node_passenger.frequent_flyer_tier AS tier,
       node_Case.case_id AS case_id,
       node_Case.case_status AS case_status
ORDER BY tier ASC
```

---

### Q6 — Complaints per category
**What it does:** Counts total complaints by category. Shows which service areas generate the most complaints.

```gql
MATCH (node_Case:`Case`)-[edge1_generates:`generates`]->(node_Complaint:`Complaint`)
RETURN node_Complaint.category AS category, count(*) AS total
GROUP BY category
ORDER BY total DESC
```

---

### Q7 — Complaints per severity
**What it does:** Counts complaints by severity level. Quick health check on how many Critical and High issues are in the system.

```gql
MATCH (node_Case:`Case`)-[edge1_generates:`generates`]->(node_Complaint:`Complaint`)
RETURN node_Complaint.severity AS severity, count(*) AS total
GROUP BY severity
ORDER BY total DESC
```

---

### Q8 — Platinum passengers with unresolved complaints
**What it does:** Surfaces all open, escalated or under-review complaints filed by Platinum tier passengers. Priority queue for customer success teams.

```gql
MATCH (node_passenger:`passenger`)-[edge1_creates:`creates`]->(node_Case:`Case`),
      (node_Case:`Case`)-[edge2_generates:`generates`]->(node_Complaint:`Complaint`)
FILTER node_passenger.frequent_flyer_tier = 'Platinum'
  AND node_Complaint.status IN ['Open', 'Escalated', 'Under Review']
RETURN node_passenger.first_name AS first_name,
       node_passenger.last_name AS last_name,
       node_Complaint.category AS category,
       node_Complaint.severity AS severity,
       node_Complaint.status AS status
ORDER BY severity ASC
```

---

### Q9 — Critical complaints with full node detail
**What it does:** Returns full JSON for every Critical complaint with its parent case and passenger. Useful for agent investigation view.

```gql
MATCH (node_passenger:`passenger`)-[edge1_creates:`creates`]->(node_Case:`Case`),
      (node_Case:`Case`)-[edge2_generates:`generates`]->(node_Complaint:`Complaint`)
FILTER node_Complaint.severity = 'Critical'
RETURN TO_JSON_STRING(node_passenger) AS `passenger`,
       TO_JSON_STRING(node_Case) AS `Case`,
       TO_JSON_STRING(node_Complaint) AS `Complaint`,
       TO_JSON_STRING(node_Flight) AS `Flight`
LIMIT 1000
```

---

### Q10 — Complaints on cancelled or diverted flights
**What it does:** Joins all three relationships to find complaints where the associated flight was cancelled or diverted. Identifies operational failure clusters.

```gql
MATCH (node_Case:`Case`)-[edge1_generates:`generates`]->(node_Complaint:`Complaint`),
      (node_passenger:`passenger`)-[edge2_creates:`creates`]->(node_Case:`Case`),
      (node_passenger:`passenger`)-[edge3_takes:`takes`]->(node_Flight:`Flight`)
FILTER node_Flight.flight_status IN ['Cancelled', 'Diverted']
RETURN node_passenger.first_name AS first_name,
       node_passenger.last_name AS last_name,
       node_passenger.frequent_flyer_tier AS tier,
       node_Flight.flight_number AS flight_number,
       node_Flight.flight_status AS flight_status,
       node_Complaint.category AS category,
       node_Complaint.severity AS severity,
       node_Complaint.status AS status
ORDER BY flight_number ASC
LIMIT 1000
```

---

### Q11 — ZA1818 cancellation cluster
**What it does:** Pulls every passenger and complaint tied to the ZA1818 LHR→SYD cancellation. Shows tier, category, severity and resolution status in one view.

```gql
MATCH (node_Case:`Case`)-[edge1_generates:`generates`]->(node_Complaint:`Complaint`),
      (node_passenger:`passenger`)-[edge2_creates:`creates`]->(node_Case:`Case`),
      (node_passenger:`passenger`)-[edge3_takes:`takes`]->(node_Flight:`Flight`)
FILTER node_Flight.flight_number = 'ZA1818'
RETURN node_passenger.first_name AS first_name,
       node_passenger.last_name AS last_name,
       node_passenger.frequent_flyer_tier AS tier,
       node_Complaint.category AS category,
       node_Complaint.subcategory AS subcategory,
       node_Complaint.severity AS severity,
       node_Complaint.status AS status
LIMIT 1000
```
