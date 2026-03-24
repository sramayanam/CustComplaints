*** Operational Instructions ***
1. Alert me when a complaint with severity Critical is filed by a passenger with frequent_flyer_tier Platinum.

*** Semantic Instructions ***
1. Complaint records are in Complaints table, identified by complaint_id.
2. Passenger records are in Passengers table, identified by passenger_id.
3. Join Complaints to Passengers on passenger_id column.
4. For each alert, extract and provide:
   - complaint_id from Complaints table
   - first_name from Passengers table
   - last_name from Passengers table
   - severity from Complaints table
   - description from Complaints table
   - flight_number from Complaints table
