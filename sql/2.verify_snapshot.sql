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
-- APPOINTMENTS: 170 (150 past + 20 upcoming)
-- DOCTORS: 10
-- PATIENTS: 100
-- VISITS: 100 (additional visits will be added during Live CDC)

-- Step 2: Verify CDC Metadata Columns
-- ----------------------------------------------------------------------------

-- Check that CDC metadata is present
SELECT 
    _CHANGE_TYPE,
    COUNT(*) as count,
    MIN(_COMMIT_TIMESTAMP) as earliest_change,
    MAX(_COMMIT_TIMESTAMP) as latest_change
FROM "appointments"
GROUP BY _CHANGE_TYPE
ORDER BY _CHANGE_TYPE;

-- For snapshot load, all records should have _CHANGE_TYPE = 'read' (initial snapshot)

-- Step 3: Sample Data Verification
-- ----------------------------------------------------------------------------

-- View sample patients
SELECT * FROM "patients" LIMIT 10;

-- View sample doctors
SELECT * FROM "doctors" ORDER BY specialization, last_name;

-- View sample appointments
SELECT 
    APPOINTMENT_ID,
    PATIENT_ID,
    DOCTOR_ID,
    APPOINTMENT_DATE,
    APPOINTMENT_TIME,
    STATUS,
    REASON_FOR_VISIT,
    APPOINTMENT_TYPE
FROM APPOINTMENTS 
WHERE status = 'scheduled'
LIMIT 10;

-- View sample visits
SELECT 
    VISIT_ID,
    PATIENT_ID,
    DOCTOR_ID,
    VISIT_DATE,
    DIAGNOSIS,
    TOTAL_CHARGE
FROM "visits" 
LIMIT 10;

-- Step 4: Data Quality Checks
-- ----------------------------------------------------------------------------

-- Check for any NULL values in key fields
SELECT 
    'Patients with NULL names' as check_name,
    COUNT(*) as issue_count
FROM PATIENTS 
WHERE first_name IS NULL OR last_name IS NULL

UNION ALL

SELECT 
    'Doctors with NULL names',
    COUNT(*)
FROM DOCTORS 
WHERE first_name IS NULL OR last_name IS NULL

UNION ALL

SELECT 
    'Appointments with NULL dates',
    COUNT(*)
FROM APPOINTMENTS 
WHERE appointment_date IS NULL OR appointment_time IS NULL

UNION ALL

SELECT 
    'Visits with NULL charges',
    COUNT(*)
FROM VISITS 
WHERE total_charge IS NULL;

-- All counts should be 0

-- Step 5: Appointment Status Distribution
-- ----------------------------------------------------------------------------

SELECT 
    STATUS,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM "appointments"
GROUP BY STATUS
ORDER BY count DESC;

-- Expected distribution:
-- completed: ~105 (70%)
-- cancelled: ~22 (15%)
-- scheduled/confirmed: ~20-25 (12%)
-- no_show: ~15 (10%)

-- Step 6: Doctor Workload Analysis
-- ----------------------------------------------------------------------------

SELECT 
    d.FIRST_NAME || ' ' || d.LAST_NAME as doctor_name,
    d.SPECIALIZATION,
    d.DEPARTMENT,
    COUNT(a.APPOINTMENT_ID) as total_appointments,
    SUM(CASE WHEN a.STATUS = 'completed' THEN 1 ELSE 0 END) as completed_appointments,
    SUM(CASE WHEN a.STATUS IN ('scheduled', 'confirmed') THEN 1 ELSE 0 END) as upcoming_appointments
FROM "doctors" d
LEFT JOIN "appointments" a ON d.doctor_id = a.doctor_id
GROUP BY d.DOCTOR_ID, d.FIRST_NAME, d.LAST_NAME, d.SPECIALIZATION, d.DEPARTMENT
ORDER BY total_appointments DESC;

-- Step 7: Patient Demographics Summary
-- ----------------------------------------------------------------------------

-- Insurance provider distribution
SELECT 
    INSURANCE_PROVIDER,
    COUNT(*) as patient_count
FROM PATIENTS
-- Use quoted lowercase for schema/table/column names in Snowflake
GROUP BY INSURANCE_PROVIDER
ORDER BY patient_count DESC;

-- Patients by state
SELECT 
    STATE,
    COUNT(*) as patient_count
FROM "patients"
GROUP BY STATE
ORDER BY patient_count DESC
LIMIT 10;

-- Age distribution (approximate)
SELECT 
    CASE 
        WHEN DATEDIFF(year, DATE_OF_BIRTH, CURRENT_DATE) < 18 THEN 'Pediatric (0-17)'
        WHEN DATEDIFF(year, DATE_OF_BIRTH, CURRENT_DATE) BETWEEN 18 AND 30 THEN 'Young Adult (18-30)'
        WHEN DATEDIFF(year, DATE_OF_BIRTH, CURRENT_DATE) BETWEEN 31 AND 50 THEN 'Adult (31-50)'
        WHEN DATEDIFF(year, DATE_OF_BIRTH, CURRENT_DATE) BETWEEN 51 AND 65 THEN 'Middle Age (51-65)'
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
    p.FIRST_NAME || ' ' || p.LAST_NAME as patient_name,
    d.FIRST_NAME || ' ' || d.LAST_NAME as doctor_name,
    d.SPECIALIZATION,
    a.APPOINTMENT_DATE,
    a.APPOINTMENT_TIME,
    a.STATUS,
    a.REASON_FOR_VISIT,
    a.APPOINTMENT_TYPE
FROM "appointments" a
JOIN "patients" p ON a.patient_id = p.patient_id
JOIN "doctors" d ON a.doctor_id = d.doctor_id
WHERE a.appointment_date >= CURRENT_DATE
  AND a.status IN ('scheduled', 'confirmed')
ORDER BY a.appointment_date, a.appointment_time;

-- Step 9: Visit Revenue Summary
-- ----------------------------------------------------------------------------

SELECT 
    COUNT(*) as total_visits,
    SUM(TOTAL_CHARGE) as total_revenue,
    AVG(TOTAL_CHARGE) as average_charge,
    MIN(TOTAL_CHARGE) as min_charge,
    MAX(TOTAL_CHARGE) as max_charge
FROM "visits";

-- Revenue by doctor
SELECT 
    d.FIRST_NAME || ' ' || d.LAST_NAME as doctor_name,
    d.SPECIALIZATION,
    COUNT(v.VISIT_ID) as visit_count,
    SUM(v.TOTAL_CHARGE) as total_revenue,
    ROUND(AVG(v.TOTAL_CHARGE), 2) as avg_revenue_per_visit
FROM "doctors" d
JOIN "visits" v ON d.doctor_id = v.doctor_id
GROUP BY d.DOCTOR_ID, d.FIRST_NAME, d.LAST_NAME, d.SPECIALIZATION
ORDER BY total_revenue DESC;

-- Step 10: Common Diagnoses
-- ----------------------------------------------------------------------------

SELECT 
    DIAGNOSIS,
    COUNT(*) as frequency
FROM "visits"
GROUP BY DIAGNOSIS
ORDER BY frequency DESC
LIMIT 10;

-- Step 11: Follow-up and Prescription Statistics
-- ----------------------------------------------------------------------------

SELECT 
    SUM(CASE WHEN FOLLOW_UP_REQUIRED THEN 1 ELSE 0 END) as visits_requiring_followup,
    SUM(CASE WHEN PRESCRIPTION_GIVEN THEN 1 ELSE 0 END) as visits_with_prescription,
    COUNT(*) as total_visits,
    ROUND(SUM(CASE WHEN FOLLOW_UP_REQUIRED THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as followup_percentage,
    ROUND(SUM(CASE WHEN PRESCRIPTION_GIVEN THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as prescription_percentage
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
