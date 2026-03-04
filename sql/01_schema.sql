-- ============================================================================
-- Lunar Air Customer Complaints – Schema Setup
-- ============================================================================
-- Server : aaaorgpgflexserver.postgres.database.azure.com
-- Database: airlines
-- User    : adminsrram
-- Schema  : custcomplaints
--
-- Connect with:
--   psql "host=aaaorgpgflexserver.postgres.database.azure.com dbname=airlines user=adminsrram sslmode=require"
--
-- Run this file first:
--   \i sql/01_schema.sql
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS custcomplaints;

-- Set default search path for subsequent scripts in same session
SET search_path TO custcomplaints, public;

COMMENT ON SCHEMA custcomplaints IS 'Lunar Air customer complaints system – main schema for complaints, passengers, and flights reference data';
