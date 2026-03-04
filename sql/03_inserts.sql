-- ============================================================================
-- ZavaAir Customer Complaints – Seed Data  (ZA branding)
-- 15 passengers · 10 flights · 15 cases · 20 complaints
-- ============================================================================

SET search_path TO custcomplaints, public;

BEGIN;

TRUNCATE custcomplaints.complaints, custcomplaints.cases, custcomplaints.flights, custcomplaints.passengers RESTART IDENTITY CASCADE;

-- ── Passengers ──────────────────────────────────────────────────────────────

INSERT INTO custcomplaints.passengers (passenger_id, first_name, last_name, email, phone, country, frequent_flyer_tier, total_flights, member_since) VALUES
( 1, 'Ava',     'Carter',   'ava.carter@example.com',    '+1-555-0101', 'United States',  'Silver',   42,  '2019-01-10T00:00:00'),
( 2, 'Noah',    'Patel',    'noah.patel@example.com',    '+1-555-0102', 'Canada',          'Gold',     87,  '2017-06-14T00:00:00'),
( 3, 'Mia',     'Lopez',    'mia.lopez@example.com',     '+1-555-0103', 'Mexico',          'Bronze',   19,  '2022-03-03T00:00:00'),
( 4, 'Liam',    'Kim',      'liam.kim@example.com',      '+1-555-0104', 'United States',  'None',      8,  '2024-02-01T00:00:00'),
( 5, 'Emma',    'Brown',    'emma.brown@example.com',    '+1-555-0105', 'United Kingdom', 'Platinum', 120,  '2015-09-22T00:00:00'),
( 6, 'Amara',   'Adeyemi',  'amara.adeyemi@example.com', '+1-555-0106', 'Nigeria',         'Gold',     63,  '2018-11-05T00:00:00'),
( 7, 'Gabriel', 'Flores',   'gabriel.flores@example.com','+1-555-0107', 'Brazil',          'Silver',   31,  '2021-07-19T00:00:00'),
( 8, 'Priya',   'Hassan',   'priya.hassan@example.com',  '+1-555-0108', 'India',           'Bronze',   12,  '2023-04-28T00:00:00'),
( 9, 'Kenji',   'Iwata',    'kenji.iwata@example.com',   '+1-555-0109', 'Japan',           'Platinum', 205, '2012-08-31T00:00:00'),
(10, 'Marcus',  'Jensen',   'marcus.jensen@example.com', '+1-555-0110', 'Denmark',         'Gold',     74,  '2016-02-17T00:00:00'),
(11, 'Chloe',   'Kim',      'chloe.kim@example.com',     '+1-555-0111', 'South Korea',     'Silver',   38,  '2020-05-09T00:00:00'),
(12, 'Oliver',  'Leclerc',  'oliver.leclerc@example.com','+1-555-0112', 'France',          'Bronze',   7,   '2024-09-14T00:00:00'),
(13, 'Fatima',  'Müller',   'fatima.muller@example.com', '+1-555-0113', 'Germany',         'None',     3,   '2025-01-22T00:00:00'),
(14, 'Diego',   'Nakamura', 'diego.nakamura@example.com','+1-555-0114', 'Argentina',       'Silver',   28,  '2021-03-11T00:00:00'),
(15, 'Elif',    'Okafor',   'elif.okafor@example.com',   '+1-555-0115', 'Turkey',          'Gold',     55,  '2018-06-25T00:00:00');

-- ── Flights ─────────────────────────────────────────────────────────────────

INSERT INTO custcomplaints.flights (flight_id, flight_number, origin_code, origin_city, destination_code, destination_city, scheduled_departure, actual_departure, scheduled_arrival, actual_arrival, aircraft_type, flight_status, delay_minutes) VALUES
( 1, 'ZA101',  'JFK', 'New York',      'LAX', 'Los Angeles', '2026-03-01T12:00:00+00:00', '2026-03-01T12:20:00+00:00', '2026-03-01T18:00:00+00:00', '2026-03-01T18:20:00+00:00', 'Crescent 787-9',   'Delayed',   20),
( 2, 'ZA202',  'LAX', 'Los Angeles',   'SEA', 'Seattle',     '2026-03-02T09:00:00+00:00', '2026-03-02T09:00:00+00:00', '2026-03-02T11:30:00+00:00', '2026-03-02T11:30:00+00:00', 'Selene A320',      'On Time',    0),
( 3, 'ZA303',  'SEA', 'Seattle',       'ORD', 'Chicago',     '2026-03-02T14:00:00+00:00', NULL,                        '2026-03-02T18:00:00+00:00', NULL,                        'Zava Cruiser 737', 'Scheduled',  0),
( 4, 'ZA404',  'ORD', 'Chicago',       'MIA', 'Miami',       '2026-03-02T16:15:00+00:00', NULL,                        '2026-03-02T21:15:00+00:00', NULL,                        'Tycho 777X',       'Cancelled',  0),
( 5, 'ZA505',  'MIA', 'Miami',         'JFK', 'New York',    '2026-03-03T08:00:00+00:00', '2026-03-03T08:00:00+00:00', '2026-03-03T11:15:00+00:00', '2026-03-03T11:15:00+00:00', 'Selene A320',      'On Time',    0),
( 6, 'ZA606',  'DEN', 'Denver',        'LAX', 'Los Angeles', '2026-03-03T11:30:00+00:00', '2026-03-03T13:05:00+00:00', '2026-03-03T13:00:00+00:00', '2026-03-03T14:35:00+00:00', 'Zava Cruiser 737', 'Delayed',   95),
( 7, 'ZA707',  'JFK', 'New York',      'LHR', 'London',      '2026-03-04T18:00:00+00:00', '2026-03-04T18:00:00+00:00', '2026-03-05T06:00:00+00:00', '2026-03-05T06:00:00+00:00', 'Moonbeam A350',    'On Time',    0),
( 8, 'ZA808',  'LAX', 'Los Angeles',   'NRT', 'Tokyo',       '2026-03-05T22:00:00+00:00', '2026-03-05T22:45:00+00:00', '2026-03-07T04:00:00+00:00', '2026-03-07T04:45:00+00:00', 'Moonbeam A350',    'Delayed',   45),
( 9, 'ZA909',  'ORD', 'Chicago',       'DEN', 'Denver',      '2026-03-06T13:00:00+00:00', '2026-03-06T13:00:00+00:00', '2026-03-06T15:30:00+00:00', '2026-03-06T15:30:00+00:00', 'Crescent 787-9',   'On Time',    0),
(10, 'ZA1010', 'SEA', 'Seattle',       'MIA', 'Miami',       '2026-03-07T07:45:00+00:00', '2026-03-07T08:10:00+00:00', '2026-03-07T16:30:00+00:00', '2026-03-07T19:05:00+00:00', 'Tycho 777X',       'Diverted',  155);

-- ── Cases ───────────────────────────────────────────────────────────────────

INSERT INTO custcomplaints.cases (case_id, passenger_id, flight_id, flight_number, pnr, case_status, opened_at, last_updated_at, closed_at) VALUES
( 1,  1,  1, 'ZA101',  'PNR1001', 'Open',          '2026-03-01T13:00:00+00:00', '2026-03-01T14:00:00+00:00', NULL),
( 2,  2,  2, 'ZA202',  'PNR2002', 'Under Review',  '2026-03-02T09:15:00+00:00', '2026-03-02T10:20:00+00:00', NULL),
( 3,  3,  4, 'ZA404',  'PNR3003', 'Resolved',      '2026-02-27T11:00:00+00:00', '2026-02-28T12:00:00+00:00', '2026-02-28T12:00:00+00:00'),
( 4,  4,  3, 'ZA303',  'PNR4004', 'Open',          '2026-03-02T15:00:00+00:00', '2026-03-02T15:00:00+00:00', NULL),
( 5,  5,  7, 'ZA707',  'PNR5005', 'Escalated',     '2026-03-04T19:30:00+00:00', '2026-03-05T08:00:00+00:00', NULL),
( 6,  1,  5, 'ZA505',  'PNR6006', 'Under Review',  '2026-03-03T12:00:00+00:00', '2026-03-03T14:00:00+00:00', NULL),
( 7,  6,  6, 'ZA606',  'PNR7007', 'Open',          '2026-03-03T14:00:00+00:00', '2026-03-03T14:00:00+00:00', NULL),
( 8,  7,  8, 'ZA808',  'PNR8008', 'Under Review',  '2026-03-06T05:00:00+00:00', '2026-03-06T06:30:00+00:00', NULL),
( 9,  8,  1, 'ZA101',  'PNR9009', 'Resolved',      '2026-03-01T19:00:00+00:00', '2026-03-02T10:00:00+00:00', '2026-03-02T10:00:00+00:00'),
(10,  9,  9, 'ZA909',  'PNR1010', 'Open',          '2026-03-06T16:00:00+00:00', '2026-03-06T16:00:00+00:00', NULL),
(11, 10, 10, 'ZA1010', 'PNR1011', 'Escalated',     '2026-03-07T20:00:00+00:00', '2026-03-08T09:00:00+00:00', NULL),
(12, 11,  2, 'ZA202',  'PNR1012', 'Open',          '2026-03-02T12:00:00+00:00', '2026-03-02T12:00:00+00:00', NULL),
(13, 12,  3, 'ZA303',  'PNR1013', 'Under Review',  '2026-03-02T16:30:00+00:00', '2026-03-02T17:00:00+00:00', NULL),
(14, 14,  5, 'ZA505',  'PNR1014', 'Open',          '2026-03-03T11:45:00+00:00', '2026-03-03T11:45:00+00:00', NULL),
(15, 15,  7, 'ZA707',  'PNR1015', 'Escalated',     '2026-03-04T20:00:00+00:00', '2026-03-05T10:00:00+00:00', NULL);

-- ── Complaints ──────────────────────────────────────────────────────────────

INSERT INTO custcomplaints.complaints (
    complaint_id, case_id, passenger_id, flight_id, flight_number, pnr,
    complaint_date, category, subcategory, description, severity, status,
    assigned_agent, resolution_notes, resolution_date, satisfaction_score
) VALUES
( 1,  1,  1,  1, 'ZA101',  'PNR1001', '2026-03-01T13:05:00+00:00', 'Baggage',           'Delayed Baggage',      'My checked bag did not appear on the carousel for over 90 minutes after landing. No staff could provide an update.',                                        'High',     'Open',          'Selene Park',  NULL,                                                               NULL,                        NULL),
( 2,  1,  1,  1, 'ZA101',  'PNR1001', '2026-03-01T13:50:00+00:00', 'Customer Service',  'Staff Attitude',       'Ground staff at carousel desk was dismissive and refused to file a formal delayed baggage report.',                                                    'Medium',   'Under Review',  'Selene Park',  NULL,                                                               NULL,                        NULL),
( 3,  2,  2,  2, 'ZA202',  'PNR2002', '2026-03-02T09:20:00+00:00', 'In-Flight Service', 'Food Quality',         'Meal served cold in the business cabin. The pasta appeared to have been left out far too long.',                                                        'Medium',   'Under Review',  'Orion Bailey', NULL,                                                               NULL,                        NULL),
( 4,  2,  2,  2, 'ZA202',  'PNR2002', '2026-03-02T10:00:00+00:00', 'Seating',           'Seat Malfunction',     'Seat 3A did not recline for the entire flight. Crew acknowledged the issue but could not fix it.',                                                     'High',     'Escalated',     'Orion Bailey', NULL,                                                               NULL,                        NULL),
( 5,  3,  3,  4, 'ZA404',  'PNR3003', '2026-02-27T11:10:00+00:00', 'Flight Operations', 'Flight Cancellation',  'Cancellation notice was sent only 45 minutes before departure, leaving insufficient time to arrange alternative travel.',                               'High',     'Resolved',      'Cosmo Lee',    'ZavaAir issued full compensation and confirmed passenger on next available flight.',  '2026-02-28T12:00:00+00:00', 4.0),
( 6,  4,  4,  3, 'ZA303',  'PNR4004', '2026-03-02T15:10:00+00:00', 'Seating',           'Upgrade Not Applied',  'Requested upgrade to premium economy was confirmed at check-in but seat was given to another passenger when boarding.',                                  'Medium',   'Open',          'Selene Park',  NULL,                                                               NULL,                        NULL),
( 7,  5,  5,  7, 'ZA707',  'PNR5005', '2026-03-04T19:40:00+00:00', 'In-Flight Service', 'Crew Behaviour',       'Crew member was visibly rude to a passenger in 8B and refused to bring water after multiple polite requests during a 7-hour transatlantic flight.',     'Critical', 'Escalated',     'Orion Bailey', NULL,                                                               NULL,                        NULL),
( 8,  5,  5,  7, 'ZA707',  'PNR5005', '2026-03-05T08:30:00+00:00', 'Customer Service',  'Complaint Not Logged', 'Called ZavaAir support after landing and was told no incident record existed. Representative refused to open a case.',                                  'High',     'Escalated',     'Orion Bailey', NULL,                                                               NULL,                        NULL),
( 9,  6,  1,  5, 'ZA505',  'PNR6006', '2026-03-03T12:10:00+00:00', 'Booking',           'Refund Delay',         'Requested refund for cancelled ancillary service 11 days ago. No response received and refund has not appeared on card statement.',                     'Medium',   'Under Review',  'Cosmo Lee',    NULL,                                                               NULL,                        NULL),
(10,  7,  6,  6, 'ZA606',  'PNR7007', '2026-03-03T14:15:00+00:00', 'Flight Operations', 'Excessive Delay',      'Flight ZA606 was delayed by 95 minutes with no communication from the gate. Gate agent left without making any announcements.',                         'High',     'Open',          'Selene Park',  NULL,                                                               NULL,                        NULL),
(11,  7,  6,  6, 'ZA606',  'PNR7007', '2026-03-03T15:00:00+00:00', 'Baggage',           'Damaged Baggage',      'Hard-shell suitcase arrived with a broken wheel and cracked zipper housing after the delayed ZA606 flight.',                                            'Medium',   'Open',          'Selene Park',  NULL,                                                               NULL,                        NULL),
(12,  8,  7,  8, 'ZA808',  'PNR8008', '2026-03-06T05:30:00+00:00', 'Booking',           'Seat Change',          'Booked window seat 22A six months in advance. At check-in was moved to middle seat 34E with no explanation.',                                          'Medium',   'Under Review',  'Orion Bailey', NULL,                                                               NULL,                        NULL),
(13,  9,  8,  1, 'ZA101',  'PNR9009', '2026-03-01T19:10:00+00:00', 'Special Assistance','Wheelchair Not Ready', 'Requested wheelchair assistance at check-in. No wheelchair was available at the gate causing significant distress.',                                    'Critical', 'Resolved',      'Cosmo Lee',    'ZavaAir confirmed procedural failure and issued formal apology and travel voucher.',   '2026-03-02T10:00:00+00:00', 3.5),
(14, 10,  9,  9, 'ZA909',  'PNR1010', '2026-03-06T16:10:00+00:00', 'In-Flight Service', 'Entertainment System', 'Seatback screen was completely non-functional for the 2.5 hour flight. Crew were unable to reset the unit.',                                            'Low',      'Open',          'Selene Park',  NULL,                                                               NULL,                        NULL),
(15, 11, 10, 10, 'ZA1010', 'PNR1011', '2026-03-07T20:30:00+00:00', 'Flight Operations', 'Diversion',            'Flight ZA1010 was diverted to ATL with 2.5 hour delay to Miami. No hotel or meal vouchers provided to passengers stuck overnight.',                    'Critical', 'Escalated',     'Orion Bailey', NULL,                                                               NULL,                        NULL),
(16, 11, 10, 10, 'ZA1010', 'PNR1011', '2026-03-08T07:00:00+00:00', 'Customer Service',  'No Compensation Info', 'ZavaAir staff at ATL could not explain compensation entitlements for diversion. Phone support wait time exceeded 3 hours.',                             'High',     'Escalated',     'Orion Bailey', NULL,                                                               NULL,                        NULL),
(17, 12, 11,  2, 'ZA202',  'PNR1012', '2026-03-02T12:15:00+00:00', 'Baggage',           'Lost Baggage',         'Checked two bags at LAX for ZA202. Only one arrived in Seattle. Baggage claim could not locate the second bag in their system.',                       'High',     'Open',          'Cosmo Lee',    NULL,                                                               NULL,                        NULL),
(18, 13, 12,  3, 'ZA303',  'PNR1013', '2026-03-02T16:40:00+00:00', 'Booking',           'Check-in Issue',       'Online check-in failed repeatedly 24 hours before departure. Was forced to pay airport fee to check in at counter.',                                   'Medium',   'Under Review',  'Selene Park',  NULL,                                                               NULL,                        NULL),
(19, 14, 14,  5, 'ZA505',  'PNR1014', '2026-03-03T11:50:00+00:00', 'In-Flight Service', 'Beverage Service',     'No beverage service offered in economy cabin for the entire JFK-MIA flight. Crew stated supply issue but no water was provided.',                      'Medium',   'Open',          'Cosmo Lee',    NULL,                                                               NULL,                        NULL),
(20, 15, 15,  7, 'ZA707',  'PNR1015', '2026-03-04T20:15:00+00:00', 'Special Assistance','Medical Request',      'Requested a specific meal for severe nut allergy confirmed at booking. A nut-containing meal was served causing a medical incident mid-flight.',        'Critical', 'Escalated',     'Orion Bailey', NULL,                                                               NULL,                        NULL);

COMMIT;
