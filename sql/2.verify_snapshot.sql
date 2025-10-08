-- ============================================================================
-- Copyright 2025 Snowflake Inc.
-- SPDX-License-Identifier: Apache-2.0
-- Licensed under the Apache License, Version 2.0 (the "License");
-- You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0
-- ============================================================================
--
-- OpenFlow PostgreSQL CDC Quickstart - Verify Snapshot Load
--
-- This script verifies that the initial snapshot was loaded correctly
-- Run this in Snowflake after starting the OpenFlow CDC connector
-- ============================================================================

USE ROLE QUICKSTART_ROLE;
USE DATABASE QUICKSTART_PGCDC_DB;
USE SCHEMA "healthcare";
USE WAREHOUSE QUICKSTART_PGCDC_WH;

-- Step 1: Check Record Counts
-- ----------------------------------------------------------------------------

SELECT 'patients' as table_name, COUNT(*) as record_count FROM "patients"
UNION ALL
SELECT 'doctors', COUNT(*) FROM "doctors"
UNION ALL
SELECT 'appointments', COUNT(*) FROM "appointments"
UNION ALL
SELECT 'visits', COUNT(*) FROM "visits"
ORDER BY table_name;

-- Expected Results:
-- appointments: 170 (150 past + 20 upcoming)
-- doctors: 10
-- patients: 100
-- visits: 100 (additional visits will be added during Live CDC)

-- Step 2: Verify CDC Metadata Columns
-- ----------------------------------------------------------------------------

-- Check that CDC metadata columns are present and confirm the snapshot was loaded
SELECT 
    COUNT(*) as total_rows,
    MIN("_SNOWFLAKE_INSERTED_AT") as earliest_inserted,
    MAX("_SNOWFLAKE_INSERTED_AT") as latest_inserted,
    COUNT(DISTINCT "_SNOWFLAKE_INSERTED_AT") as unique_insert_timestamps
FROM "appointments";

-- Expected: 170 rows with timestamps from the snapshot load
-- During Live CDC, _SNOWFLAKE_UPDATED_AT will be populated for modified records

-- Step 3: Sample Data Verification
-- ----------------------------------------------------------------------------

-- View sample patients
SELECT * FROM "patients" LIMIT 10;

-- View sample doctors
SELECT * FROM "doctors" ORDER BY "specialization", "last_name";

-- View sample appointments
SELECT 
    "appointment_id",
    "patient_id",
    "doctor_id",
    "appointment_date",
    "appointment_time",
    "status",
    "reason_for_visit",
    "appointment_type"
FROM "appointments" 
WHERE "status" = 'scheduled'
LIMIT 10;

-- View sample visits
SELECT 
    "visit_id",
    "patient_id",
    "doctor_id",
    "visit_date",
    "diagnosis",
    "total_charge"
FROM "visits" 
LIMIT 10;

-- Step 4: Data Quality Checks
-- ----------------------------------------------------------------------------

-- Check for any NULL values in key fields
SELECT 
    'Patients with NULL names' as check_name,
    COUNT(*) as issue_count
FROM "patients" 
WHERE "first_name" IS NULL OR "last_name" IS NULL

UNION ALL

SELECT 
    'Doctors with NULL names',
    COUNT(*)
FROM "doctors" 
WHERE "first_name" IS NULL OR "last_name" IS NULL

UNION ALL

SELECT 
    'Appointments with NULL dates',
    COUNT(*)
FROM "appointments" 
WHERE "appointment_date" IS NULL OR "appointment_time" IS NULL

UNION ALL

SELECT 
    'Visits with NULL charges',
    COUNT(*)
FROM "visits" 
WHERE "total_charge" IS NULL;

-- All counts should be 0

-- Step 5: Appointment Status Distribution
-- ----------------------------------------------------------------------------

SELECT 
    "status",
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM "appointments"
GROUP BY "status"
ORDER BY count DESC;

-- Expected distribution:
-- completed: 100 (58.82%)
-- cancelled: 40 (23.53%)
-- scheduled: 15 (8.82%)
-- no_show: 10 (5.88%)
-- confirmed: 5 (2.94%)

-- Step 6: Doctor Workload Analysis
-- ----------------------------------------------------------------------------

SELECT 
    d."first_name" || ' ' || d."last_name" as doctor_name,
    d."specialization",
    d."department",
    COUNT(a."appointment_id") as total_appointments,
    SUM(CASE WHEN a."status" = 'completed' THEN 1 ELSE 0 END) as completed_appointments,
    SUM(CASE WHEN a."status" IN ('scheduled', 'confirmed') THEN 1 ELSE 0 END) as upcoming_appointments
FROM "doctors" d
LEFT JOIN "appointments" a ON d."doctor_id" = a."doctor_id"
GROUP BY d."doctor_id", d."first_name", d."last_name", d."specialization", d."department"
ORDER BY total_appointments DESC;

-- Top 5 doctors by appointment volume (for quickstart guide)
SELECT 
    d."first_name" || ' ' || d."last_name" as doctor_name,
    d."specialization",
    d."department",
    COUNT(a."appointment_id") as total_appointments,
    SUM(CASE WHEN a."status" = 'completed' THEN 1 ELSE 0 END) as completed_appointments,
    SUM(CASE WHEN a."status" IN ('scheduled', 'confirmed') THEN 1 ELSE 0 END) as upcoming_appointments
FROM "doctors" d
LEFT JOIN "appointments" a ON d."doctor_id" = a."doctor_id"
GROUP BY d."doctor_id", d."first_name", d."last_name", d."specialization", d."department"
ORDER BY total_appointments DESC
LIMIT 5;

-- Step 7: Patient Demographics Summary
-- ----------------------------------------------------------------------------

-- Insurance provider distribution
SELECT 
    "insurance_provider",
    COUNT(*) as patient_count
FROM "patients"
GROUP BY "insurance_provider"
ORDER BY patient_count DESC;

-- Patients by state
SELECT 
    "state",
    COUNT(*) as patient_count
FROM "patients"
GROUP BY "state"
ORDER BY patient_count DESC
LIMIT 10;

-- Age distribution (approximate)
SELECT 
    CASE 
        WHEN DATEDIFF(year, "date_of_birth", CURRENT_DATE) < 18 THEN 'Pediatric (0-17)'
        WHEN DATEDIFF(year, "date_of_birth", CURRENT_DATE) BETWEEN 18 AND 30 THEN 'Young Adult (18-30)'
        WHEN DATEDIFF(year, "date_of_birth", CURRENT_DATE) BETWEEN 31 AND 50 THEN 'Adult (31-50)'
        WHEN DATEDIFF(year, "date_of_birth", CURRENT_DATE) BETWEEN 51 AND 65 THEN 'Middle Age (51-65)'
        ELSE 'Senior (65+)'
    END as age_group,
    COUNT(*) as patient_count
FROM "patients"
GROUP BY age_group
ORDER BY age_group;

-- Step 8: Upcoming Appointments
-- ----------------------------------------------------------------------------

-- Next 7 days of scheduled appointments
SELECT 
    p."first_name" || ' ' || p."last_name" as patient_name,
    d."first_name" || ' ' || d."last_name" as doctor_name,
    d."specialization",
    a."appointment_date",
    a."appointment_time",
    a."status",
    a."reason_for_visit",
    a."appointment_type"
FROM "appointments" a
JOIN "patients" p ON a."patient_id" = p."patient_id"
JOIN "doctors" d ON a."doctor_id" = d."doctor_id"
WHERE a."appointment_date" >= CURRENT_DATE
  AND a."status" IN ('scheduled', 'confirmed')
ORDER BY a."appointment_date", a."appointment_time";

-- Step 9: Visit Revenue Summary
-- ----------------------------------------------------------------------------

SELECT 
    COUNT(*) as total_visits,
    SUM("total_charge") as total_revenue,
    AVG("total_charge") as average_charge,
    MIN("total_charge") as min_charge,
    MAX("total_charge") as max_charge
FROM "visits";

-- Revenue by doctor
SELECT 
    d."first_name" || ' ' || d."last_name" as doctor_name,
    d."specialization",
    COUNT(v."visit_id") as visit_count,
    SUM(v."total_charge") as total_revenue,
    ROUND(AVG(v."total_charge"), 2) as avg_revenue_per_visit
FROM "doctors" d
JOIN "visits" v ON d."doctor_id" = v."doctor_id"
GROUP BY d."doctor_id", d."first_name", d."last_name", d."specialization"
ORDER BY total_revenue DESC;

-- Step 10: Common Diagnoses
-- ----------------------------------------------------------------------------

SELECT 
    "diagnosis",
    COUNT(*) as frequency
FROM "visits"
GROUP BY "diagnosis"
ORDER BY frequency DESC
LIMIT 10;

-- Step 11: Follow-up and Prescription Statistics
-- ----------------------------------------------------------------------------

SELECT 
    SUM(CASE WHEN "follow_up_required" THEN 1 ELSE 0 END) as visits_requiring_followup,
    SUM(CASE WHEN "prescription_given" THEN 1 ELSE 0 END) as visits_with_prescription,
    COUNT(*) as total_visits,
    ROUND(SUM(CASE WHEN "follow_up_required" THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as followup_percentage,
    ROUND(SUM(CASE WHEN "prescription_given" THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as prescription_percentage
FROM "visits";

-- ============================================================================
-- Snapshot Verification Complete!
-- ============================================================================
-- If all the above queries return expected results, your snapshot load is successful.
-- 
-- Next Steps:
-- 1. Note the current record counts
-- 2. Run 3.live_appointments.sql on PostgreSQL to generate CDC events
-- 3. Watch the changes flow into Snowflake in real-time
-- 4. Re-run these queries to see the updated data
-- ============================================================================
