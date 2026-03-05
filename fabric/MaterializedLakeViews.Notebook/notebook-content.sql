-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "08bf1610-8c6e-407c-be15-44a3d663ca8d",
-- META       "default_lakehouse_name": "airlinesLH",
-- META       "default_lakehouse_workspace_id": "12204d44-688c-40db-9593-4d23c9ad3e51",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "08bf1610-8c6e-407c-be15-44a3d663ca8d"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- # Create materialized lake views 
-- 1. Use this notebook to create materialized lake views. 
-- 2. Select **Run all** to run the notebook. 
-- 3. When the notebook run is completed, return to your lakehouse and refresh your materialized lake views graph. 


-- CELL ********************

-- Welcome to your new notebook 
-- Type here in the cell editor to add code! 
CREATE MATERIALIZED LAKE VIEW mlv_quarterly_summary AS 
SELECT 
    yr as business_year,
    qtr as business_quarter,
    ap1 as origin_airport_code,
    ap2 as destination_airport_code,
    cr1 as primary_carrier_code,
    cr2 as secondary_carrier_code,
    ticket_type as service_type,
    -- Passenger metrics (cast to appropriate precision)
    COUNT(*) as total_flight_count,
    SUM(pax) as total_passenger_count,
    CAST(AVG(pax) AS DECIMAL(10,1)) as average_passengers_per_flight,
    -- Financial metrics (currency precision)
    CAST(AVG(avprc) AS DECIMAL(10,2)) as average_ticket_price_usd,
    CAST(SUM(pax * avprc) AS DECIMAL(15,2)) as total_revenue_usd,
    -- Distance metrics (whole numbers for miles)
    CAST(AVG(nsdst) AS DECIMAL(8,0)) as average_nonstop_distance_miles,
    CAST(AVG(avdst) AS DECIMAL(8,0)) as average_routing_distance_miles,
    CAST(AVG(routing_efficiency) AS DECIMAL(6,3)) as average_routing_efficiency_ratio,
    -- Market metrics
    COUNT(DISTINCT cr1) as unique_carrier_count,
    carrier_type as carrier_category,
    from_major_hub as is_major_hub_origin
FROM airline_market_data
GROUP BY yr, qtr, ap1, ap2, cr1, cr2, ticket_type, carrier_type, from_major_hub

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE MATERIALIZED LAKE VIEW mlv_annual_airport_performance AS
SELECT 
    yr as reporting_year,
    ap1 as airport_iata_code,
    -- Traffic volume metrics
    COUNT(*) as annual_flight_record_count,
    SUM(pax) as annual_passenger_volume,
    COUNT(DISTINCT cr1) as serving_carrier_count,
    COUNT(DISTINCT ap2) as destination_airport_count,
    -- Service type breakdown
    SUM(CASE WHEN cop = 0 THEN pax ELSE 0 END) as direct_flight_passengers,
    SUM(CASE WHEN cop = 1 THEN pax ELSE 0 END) as connecting_flight_passengers,
    -- Financial performance
    CAST(AVG(avprc) AS DECIMAL(10,2)) as average_departure_price_usd,
    CAST(SUM(pax * avprc) AS DECIMAL(18,2)) as total_departure_revenue_usd,
    -- Operational efficiency
    CAST(AVG(nsdst) AS DECIMAL(8,0)) as average_flight_distance_miles,
    CAST(AVG(price_per_mile) AS DECIMAL(8,4)) as average_price_per_mile_usd,
    -- Year-over-year comparison
    LAG(SUM(pax)) OVER (PARTITION BY ap1 ORDER BY yr) as previous_year_passenger_volume,
    -- Market characteristics
    from_major_hub as is_classified_major_hub,
    MAX(CASE WHEN carrier_type = 'Major' THEN 1 ELSE 0 END) as serves_major_carriers_flag,
    MAX(CASE WHEN carrier_type = 'Regional' THEN 1 ELSE 0 END) as serves_regional_carriers_flag
FROM airline_market_data
GROUP BY yr, ap1, from_major_hub

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE MATERIALIZED LAKE VIEW mlv_carrier_market_performance AS
SELECT 
    yr as performance_year,
    qtr as performance_quarter,
    cr1 as airline_carrier_code,
    carrier_type as airline_business_category,
    ap1 as route_origin_airport,
    ap2 as route_destination_airport,
    -- Core performance metrics
    COUNT(*) as route_flight_record_count,
    SUM(pax) as route_passenger_volume,
    CAST(AVG(avprc) AS DECIMAL(10,2)) as route_average_price_usd,
    CAST(SUM(pax * avprc) AS DECIMAL(15,2)) as route_total_revenue_usd,
    -- Operational efficiency
    CAST(AVG(nsdst) AS DECIMAL(8,0)) as route_distance_miles,
    CAST(AVG(routing_efficiency) AS DECIMAL(6,3)) as route_efficiency_ratio,
    -- Market position analysis
    CAST(SUM(pax) * 100.0 / SUM(SUM(pax)) OVER (PARTITION BY yr, qtr, ap1, ap2) AS DECIMAL(8,2)) as route_market_share_percentage,
    -- Service type breakdown
    SUM(CASE WHEN cop = 0 THEN pax ELSE 0 END) as direct_service_passenger_count,
    SUM(CASE WHEN cop = 1 THEN pax ELSE 0 END) as connecting_service_passenger_count,
    -- Hub operations indicator
    from_major_hub as operates_from_major_hub_flag
FROM airline_market_data
WHERE cr1 IS NOT NULL
GROUP BY yr, qtr, cr1, carrier_type, ap1, ap2, from_major_hub

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE MATERIALIZED LAKE VIEW mlv_time_series_market_trends AS
SELECT 
    yr as trend_analysis_year,
    qtr as trend_analysis_quarter,
    -- Industry-wide volume metrics
    COUNT(*) as industry_flight_record_count,
    SUM(pax) as industry_total_passengers,
    CAST(AVG(avprc) AS DECIMAL(10,2)) as industry_average_price_usd,
    CAST(SUM(pax * avprc) AS DECIMAL(18,2)) as industry_total_revenue_usd,
    -- Market structure indicators
    COUNT(DISTINCT ap1) as active_origin_airport_count,
    COUNT(DISTINCT cr1) as active_carrier_count,
    COUNT(DISTINCT CASE WHEN cop = 0 THEN CONCAT(ap1,'-',COALESCE(ap2,'')) END) as direct_route_count,
    COUNT(DISTINCT CASE WHEN cop = 1 THEN CONCAT(ap1,'-',COALESCE(ap2,'')) END) as connecting_route_count,
    -- Service quality benchmarks
    CAST(AVG(nsdst) AS DECIMAL(8,0)) as industry_average_distance_miles,
    CAST(AVG(routing_efficiency) AS DECIMAL(6,3)) as industry_routing_efficiency,
    -- Market segmentation by carrier type
    SUM(CASE WHEN carrier_type = 'Major' THEN pax ELSE 0 END) as major_carrier_passenger_volume,
    SUM(CASE WHEN carrier_type = 'Regional' THEN pax ELSE 0 END) as regional_carrier_passenger_volume,
    SUM(CASE WHEN from_major_hub = true THEN pax ELSE 0 END) as major_hub_passenger_volume,
    -- Price distribution analytics
    CAST(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY avprc) AS DECIMAL(10,2)) as price_25th_percentile_usd,
    CAST(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY avprc) AS DECIMAL(10,2)) as price_median_usd,
    CAST(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY avprc) AS DECIMAL(10,2)) as price_75th_percentile_usd,
    CAST(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY avprc) AS DECIMAL(10,2)) as price_90th_percentile_usd
FROM airline_market_data
GROUP BY yr, qtr

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE MATERIALIZED LAKE VIEW mlv_route_performance_summary AS
SELECT 
    ap1 as origin_airport_code,
    ap2 as destination_airport_code,
    -- Time span analytics
    MIN(yr) as first_service_year,
    MAX(yr) as last_service_year,
    COUNT(DISTINCT yr) as years_of_service,
    -- Traffic volume metrics
    COUNT(*) as total_flight_record_count,
    SUM(pax) as total_passenger_volume,
    CAST(AVG(pax) AS DECIMAL(10,1)) as average_passengers_per_flight,
    -- Market structure analysis
    COUNT(DISTINCT cr1) as competing_carrier_count,
    COUNT(DISTINCT CASE WHEN carrier_type = 'Major' THEN cr1 END) as major_carrier_count,
    COUNT(DISTINCT CASE WHEN carrier_type = 'Regional' THEN cr1 END) as regional_carrier_count,
    -- Service type breakdown
    SUM(CASE WHEN cop = 0 THEN pax ELSE 0 END) as direct_service_passengers,
    SUM(CASE WHEN cop = 1 THEN pax ELSE 0 END) as connecting_service_passengers,
    CAST(AVG(CASE WHEN cop = 0 THEN avprc END) AS DECIMAL(10,2)) as average_direct_price_usd,
    CAST(AVG(CASE WHEN cop = 1 THEN avprc END) AS DECIMAL(10,2)) as average_connecting_price_usd,
    -- Distance and efficiency analytics
    CAST(AVG(nsdst) AS DECIMAL(8,0)) as average_nonstop_distance_miles,
    CAST(AVG(avdst) AS DECIMAL(8,0)) as average_routing_distance_miles,
    CAST(AVG(routing_efficiency) AS DECIMAL(6,3)) as average_routing_efficiency_ratio,
    -- Financial performance summary
    CAST(AVG(avprc) AS DECIMAL(10,2)) as overall_average_price_usd,
    CAST(SUM(pax * avprc) AS DECIMAL(18,2)) as total_route_revenue_usd,
    CAST(AVG(price_per_mile) AS DECIMAL(8,4)) as average_price_per_mile_usd,
    -- Market characteristics
    MAX(from_major_hub) as involves_major_hub_airport
FROM airline_market_data
WHERE ap1 IS NOT NULL
GROUP BY ap1, ap2

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE MATERIALIZED LAKE VIEW mlv_data_quality_metrics AS
SELECT 
    yr as quality_check_year,
    qtr as quality_check_quarter,
    -- Record count metrics
    COUNT(*) as total_record_count,
    -- Missing data analysis
    COUNT(*) - COUNT(ap2) as missing_destination_airport_count,
    COUNT(*) - COUNT(cr2) as missing_secondary_carrier_count,
    COUNT(CASE WHEN pax = 0 THEN 1 END) as zero_passenger_record_count,
    COUNT(CASE WHEN avprc <= 0 THEN 1 END) as invalid_price_record_count,
    COUNT(CASE WHEN nsdst <= 0 THEN 1 END) as invalid_distance_record_count,
    -- Data range validation
    MIN(pax) as minimum_passenger_count,
    MAX(pax) as maximum_passenger_count,
    CAST(MIN(avprc) AS DECIMAL(10,2)) as minimum_price_usd,
    CAST(MAX(avprc) AS DECIMAL(10,2)) as maximum_price_usd,
    MIN(nsdst) as minimum_distance_miles,
    MAX(nsdst) as maximum_distance_miles,
    -- Statistical summaries
    CAST(AVG(pax) AS DECIMAL(10,1)) as average_passenger_count,
    CAST(AVG(avprc) AS DECIMAL(10,2)) as average_price_usd,
    CAST(AVG(nsdst) AS DECIMAL(8,0)) as average_distance_miles,
    -- Data diversity metrics
    COUNT(DISTINCT ap1) as unique_origin_airport_count,
    COUNT(DISTINCT ap2) as unique_destination_airport_count,
    COUNT(DISTINCT cr1) as unique_primary_carrier_count,
    COUNT(DISTINCT cr2) as unique_secondary_carrier_count,
    -- Data completeness percentages
    CAST((COUNT(ap2) * 100.0 / COUNT(*)) AS DECIMAL(5,2)) as destination_completeness_percentage,
    CAST((COUNT(cr2) * 100.0 / COUNT(*)) AS DECIMAL(5,2)) as secondary_carrier_completeness_percentage
FROM airline_market_data
GROUP BY yr, qtr

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT * FROM airlinesLH.dbo.mlv_data_quality_metrics LIMIT 1000

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
