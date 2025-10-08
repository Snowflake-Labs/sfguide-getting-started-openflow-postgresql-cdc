-- ============================================================================
-- Copyright 2025 Snowflake Inc.
-- SPDX-License-Identifier: Apache-2.0
-- Licensed under the Apache License, Version 2.0 (the "License");
-- You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0
-- ============================================================================
--
-- OpenFlow PostgreSQL CDC Quickstart - Analytics Queries
--
-- This script demonstrates the analytical value of real-time CDC data
-- Run these queries in Snowflake to analyze healthcare operations
-- 
-- Query Categories:
-- 1. Real-time Operational Dashboards
-- 2. Patient Flow Analysis
-- 3. Doctor Performance Metrics
-- 4. Revenue Analytics
-- 5. Trend Analysis
-- 6. Data Quality & Audit Trails
-- ============================================================================

USE ROLE QUICKSTART_ROLE;
USE DATABASE QUICKSTART_PGCDC_DB;
USE SCHEMA HEALTHCARE;
USE WAREHOUSE QUICKSTART_PGCDC_WH;

-- ============================================================================
-- 1. Real-time Operational Dashboard Queries
-- ============================================================================

-- Current Day Appointment Status (Real-time Dashboard)
-- ----------------------------------------------------------------------------
SELECT 
    STATUS,
    COUNT(*) as appointment_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage,
    LISTAGG(DISTINCT TO_CHAR(APPOINTMENT_TIME, 'HH24:MI'), ', ') WITHIN GROUP (ORDER BY APPOINTMENT_TIME) as time_slots
FROM APPOINTMENTS
WHERE APPOINTMENT_DATE = CURRENT_DATE
GROUP BY STATUS
ORDER BY appointment_count DESC;

-- Patients Currently in Clinic
-- ----------------------------------------------------------------------------
SELECT 
    p.FIRST_NAME || ' ' || p.LAST_NAME as patient_name,
    p.PHONE,
    d.FIRST_NAME || ' ' || d.LAST_NAME as doctor_name,
    d.SPECIALIZATION,
    a.APPOINTMENT_TIME,
    a.STATUS,
    a.REASON_FOR_VISIT,
    TIMESTAMPDIFF(MINUTE, 
        TO_TIMESTAMP(TO_CHAR(a.APPOINTMENT_DATE, 'YYYY-MM-DD') || ' ' || TO_CHAR(a.APPOINTMENT_TIME, 'HH24:MI:SS')),
        CURRENT_TIMESTAMP()) as minutes_since_appointment
FROM APPOINTMENTS a
JOIN PATIENTS p ON a.PATIENT_ID = p.PATIENT_ID
JOIN DOCTORS d ON a.DOCTOR_ID = d.DOCTOR_ID
WHERE a.APPOINTMENT_DATE = CURRENT_DATE
  AND a.STATUS IN ('checked_in', 'in_progress')
ORDER BY a.STATUS DESC, a.APPOINTMENT_TIME;

-- Doctor Availability Today
-- ----------------------------------------------------------------------------
SELECT 
    d.FIRST_NAME || ' ' || d.LAST_NAME as doctor_name,
    d.SPECIALIZATION,
    d.DEPARTMENT,
    d.ACCEPTING_NEW_PATIENTS,
    COUNT(CASE WHEN a.STATUS = 'completed' THEN 1 END) as completed_today,
    COUNT(CASE WHEN a.STATUS = 'in_progress' THEN 1 END) as currently_seeing,
    COUNT(CASE WHEN a.STATUS IN ('confirmed', 'checked_in') THEN 1 END) as waiting,
    COUNT(a.APPOINTMENT_ID) as total_appointments_today
FROM DOCTORS d
LEFT JOIN APPOINTMENTS a ON d.DOCTOR_ID = a.DOCTOR_ID AND a.APPOINTMENT_DATE = CURRENT_DATE
GROUP BY d.DOCTOR_ID, d.FIRST_NAME, d.LAST_NAME, d.SPECIALIZATION, d.DEPARTMENT, d.ACCEPTING_NEW_PATIENTS
ORDER BY total_appointments_today DESC;

-- ============================================================================
-- 2. Patient Flow Analysis
-- ============================================================================

-- Average Wait Time by Status Transition
-- ----------------------------------------------------------------------------
SELECT 
    APPOINTMENT_DATE,
    AVG(TIMESTAMPDIFF(MINUTE, CREATED_AT, UPDATED_AT)) as avg_minutes_to_update,
    COUNT(*) as appointments
FROM APPOINTMENTS
WHERE STATUS IN ('completed', 'cancelled', 'no_show')
  AND CREATED_AT != UPDATED_AT
  AND APPOINTMENT_DATE >= DATEADD(day, -30, CURRENT_DATE)
GROUP BY APPOINTMENT_DATE
ORDER BY APPOINTMENT_DATE DESC
LIMIT 30;

-- Appointment Completion Rate (Last 30 Days)
-- ----------------------------------------------------------------------------
WITH appointment_metrics AS (
    SELECT 
        APPOINTMENT_DATE,
        COUNT(*) as total_appointments,
        SUM(CASE WHEN STATUS = 'completed' THEN 1 ELSE 0 END) as completed,
        SUM(CASE WHEN STATUS = 'cancelled' THEN 1 ELSE 0 END) as cancelled,
        SUM(CASE WHEN STATUS = 'no_show' THEN 1 ELSE 0 END) as no_shows
    FROM APPOINTMENTS
    WHERE APPOINTMENT_DATE >= DATEADD(day, -30, CURRENT_DATE)
      AND APPOINTMENT_DATE <= CURRENT_DATE
    GROUP BY APPOINTMENT_DATE
)
SELECT 
    APPOINTMENT_DATE,
    total_appointments,
    completed,
    cancelled,
    no_shows,
    ROUND(completed * 100.0 / NULLIF(total_appointments, 0), 1) as completion_rate,
    ROUND(no_shows * 100.0 / NULLIF(total_appointments, 0), 1) as no_show_rate
FROM appointment_metrics
ORDER BY APPOINTMENT_DATE DESC;

-- Peak Hours Analysis
-- ----------------------------------------------------------------------------
SELECT 
    TO_CHAR(APPOINTMENT_TIME, 'HH24:00') as hour_block,
    COUNT(*) as total_appointments,
    AVG(CASE WHEN STATUS = 'completed' THEN 1.0 ELSE 0.0 END) * 100 as completion_rate,
    AVG(CASE WHEN STATUS = 'no_show' THEN 1.0 ELSE 0.0 END) * 100 as no_show_rate
FROM APPOINTMENTS
WHERE APPOINTMENT_DATE >= DATEADD(day, -30, CURRENT_DATE)
GROUP BY hour_block
ORDER BY hour_block;

-- Busiest Days of Week
-- ----------------------------------------------------------------------------
SELECT 
    DAYNAME(APPOINTMENT_DATE) as day_of_week,
    DAYOFWEEK(APPOINTMENT_DATE) as day_num,
    COUNT(*) as total_appointments,
    AVG(CASE WHEN STATUS = 'completed' THEN 1.0 ELSE 0.0 END) * 100 as completion_rate
FROM APPOINTMENTS
WHERE APPOINTMENT_DATE >= DATEADD(day, -90, CURRENT_DATE)
GROUP BY day_of_week, day_num
ORDER BY day_num;

-- ============================================================================
-- 3. Doctor Performance Metrics
-- ============================================================================

-- Doctor Productivity (Last 30 Days)
-- ----------------------------------------------------------------------------
SELECT 
    d.FIRST_NAME || ' ' || d.LAST_NAME as doctor_name,
    d.SPECIALIZATION,
    COUNT(a.APPOINTMENT_ID) as total_appointments,
    SUM(CASE WHEN a.STATUS = 'completed' THEN 1 ELSE 0 END) as completed,
    SUM(CASE WHEN a.STATUS = 'cancelled' THEN 1 ELSE 0 END) as cancelled,
    SUM(CASE WHEN a.STATUS = 'no_show' THEN 1 ELSE 0 END) as no_shows,
    ROUND(SUM(CASE WHEN a.STATUS = 'completed' THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(COUNT(a.APPOINTMENT_ID), 0), 1) as completion_rate,
    COUNT(DISTINCT a.APPOINTMENT_DATE) as days_worked
FROM DOCTORS d
LEFT JOIN APPOINTMENTS a ON d.DOCTOR_ID = a.DOCTOR_ID 
    AND a.APPOINTMENT_DATE >= DATEADD(day, -30, CURRENT_DATE)
    AND a.APPOINTMENT_DATE <= CURRENT_DATE
GROUP BY d.DOCTOR_ID, d.FIRST_NAME, d.LAST_NAME, d.SPECIALIZATION
ORDER BY completed DESC;

-- Average Appointments per Day by Doctor
-- ----------------------------------------------------------------------------
SELECT 
    d.FIRST_NAME || ' ' || d.LAST_NAME as doctor_name,
    d.SPECIALIZATION,
    COUNT(a.APPOINTMENT_ID) as total_appointments,
    COUNT(DISTINCT a.APPOINTMENT_DATE) as days_with_appointments,
    ROUND(COUNT(a.APPOINTMENT_ID) * 1.0 / 
          NULLIF(COUNT(DISTINCT a.APPOINTMENT_DATE), 0), 1) as avg_appointments_per_day
FROM DOCTORS d
LEFT JOIN APPOINTMENTS a ON d.DOCTOR_ID = a.DOCTOR_ID 
    AND a.APPOINTMENT_DATE >= DATEADD(day, -30, CURRENT_DATE)
    AND a.STATUS = 'completed'
GROUP BY d.DOCTOR_ID, d.FIRST_NAME, d.LAST_NAME, d.SPECIALIZATION
HAVING COUNT(DISTINCT a.APPOINTMENT_DATE) > 0
ORDER BY avg_appointments_per_day DESC;

-- Patient Satisfaction Proxy (Follow-up Required Rate)
-- ----------------------------------------------------------------------------
SELECT 
    d.FIRST_NAME || ' ' || d.LAST_NAME as doctor_name,
    d.SPECIALIZATION,
    COUNT(v.VISIT_ID) as total_visits,
    SUM(CASE WHEN v.FOLLOW_UP_REQUIRED THEN 1 ELSE 0 END) as followups_needed,
    ROUND(SUM(CASE WHEN v.FOLLOW_UP_REQUIRED THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(COUNT(v.VISIT_ID), 0), 1) as followup_rate,
    SUM(CASE WHEN v.PRESCRIPTION_GIVEN THEN 1 ELSE 0 END) as prescriptions_written,
    ROUND(SUM(CASE WHEN v.PRESCRIPTION_GIVEN THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(COUNT(v.VISIT_ID), 0), 1) as prescription_rate
FROM DOCTORS d
JOIN VISITS v ON d.DOCTOR_ID = v.DOCTOR_ID
GROUP BY d.DOCTOR_ID, d.FIRST_NAME, d.LAST_NAME, d.SPECIALIZATION
ORDER BY total_visits DESC;

-- ============================================================================
-- 4. Revenue Analytics
-- ============================================================================

-- Daily Revenue Trend
-- ----------------------------------------------------------------------------
SELECT 
    VISIT_DATE,
    COUNT(*) as visit_count,
    SUM(TOTAL_CHARGE) as daily_revenue,
    AVG(TOTAL_CHARGE) as avg_revenue_per_visit,
    MIN(TOTAL_CHARGE) as min_charge,
    MAX(TOTAL_CHARGE) as max_charge
FROM VISITS
WHERE VISIT_DATE >= DATEADD(day, -30, CURRENT_DATE)
GROUP BY VISIT_DATE
ORDER BY VISIT_DATE DESC;

-- Revenue by Department
-- ----------------------------------------------------------------------------
SELECT 
    d.DEPARTMENT,
    COUNT(v.VISIT_ID) as visit_count,
    SUM(v.TOTAL_CHARGE) as total_revenue,
    ROUND(AVG(v.TOTAL_CHARGE), 2) as avg_revenue_per_visit,
    ROUND(SUM(v.TOTAL_CHARGE) * 100.0 / SUM(SUM(v.TOTAL_CHARGE)) OVER (), 1) as revenue_percentage
FROM DOCTORS d
JOIN VISITS v ON d.DOCTOR_ID = v.DOCTOR_ID
GROUP BY d.DEPARTMENT
ORDER BY total_revenue DESC;

-- Revenue by Doctor (Top 10)
-- ----------------------------------------------------------------------------
SELECT 
    d.FIRST_NAME || ' ' || d.LAST_NAME as doctor_name,
    d.SPECIALIZATION,
    d.DEPARTMENT,
    COUNT(v.VISIT_ID) as total_visits,
    SUM(v.TOTAL_CHARGE) as total_revenue,
    ROUND(AVG(v.TOTAL_CHARGE), 2) as avg_charge_per_visit
FROM DOCTORS d
JOIN VISITS v ON d.DOCTOR_ID = v.DOCTOR_ID
GROUP BY d.DOCTOR_ID, d.FIRST_NAME, d.LAST_NAME, d.SPECIALIZATION, d.DEPARTMENT
ORDER BY total_revenue DESC
LIMIT 10;

-- Monthly Revenue Summary
-- ----------------------------------------------------------------------------
SELECT 
    TO_CHAR(VISIT_DATE, 'YYYY-MM') as month,
    COUNT(*) as total_visits,
    SUM(TOTAL_CHARGE) as monthly_revenue,
    ROUND(AVG(TOTAL_CHARGE), 2) as avg_revenue_per_visit,
    COUNT(DISTINCT PATIENT_ID) as unique_patients
FROM VISITS
GROUP BY month
ORDER BY month DESC;

-- ============================================================================
-- 5. Clinical Insights
-- ============================================================================

-- Most Common Diagnoses
-- ----------------------------------------------------------------------------
SELECT 
    DIAGNOSIS,
    COUNT(*) as frequency,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage,
    ROUND(AVG(TOTAL_CHARGE), 2) as avg_cost,
    SUM(CASE WHEN FOLLOW_UP_REQUIRED THEN 1 ELSE 0 END) as followups_needed
FROM VISITS
GROUP BY DIAGNOSIS
ORDER BY frequency DESC
LIMIT 15;

-- Most Common Reasons for Visit
-- ----------------------------------------------------------------------------
SELECT 
    REASON_FOR_VISIT,
    COUNT(*) as frequency,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage_of_appointments,
    SUM(CASE WHEN STATUS = 'completed' THEN 1 ELSE 0 END) as completed,
    SUM(CASE WHEN STATUS = 'no_show' THEN 1 ELSE 0 END) as no_shows
FROM APPOINTMENTS
WHERE APPOINTMENT_DATE >= DATEADD(day, -60, CURRENT_DATE)
GROUP BY REASON_FOR_VISIT
ORDER BY frequency DESC
LIMIT 15;

-- Appointment Type Distribution by Specialization
-- ----------------------------------------------------------------------------
SELECT 
    d.SPECIALIZATION,
    a.APPOINTMENT_TYPE,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY d.SPECIALIZATION), 1) as percentage
FROM APPOINTMENTS a
JOIN DOCTORS d ON a.DOCTOR_ID = d.DOCTOR_ID
WHERE a.APPOINTMENT_DATE >= DATEADD(day, -60, CURRENT_DATE)
GROUP BY d.SPECIALIZATION, a.APPOINTMENT_TYPE
ORDER BY d.SPECIALIZATION, count DESC;

-- ============================================================================
-- 6. Patient Analytics
-- ============================================================================

-- Patient Visit Frequency
-- ----------------------------------------------------------------------------
SELECT 
    p.PATIENT_ID,
    p.FIRST_NAME || ' ' || p.LAST_NAME as patient_name,
    p.INSURANCE_PROVIDER,
    COUNT(v.VISIT_ID) as total_visits,
    MIN(v.VISIT_DATE) as first_visit,
    MAX(v.VISIT_DATE) as most_recent_visit,
    SUM(v.TOTAL_CHARGE) as total_spent,
    ROUND(AVG(v.TOTAL_CHARGE), 2) as avg_per_visit
FROM PATIENTS p
JOIN VISITS v ON p.PATIENT_ID = v.PATIENT_ID
GROUP BY p.PATIENT_ID, p.FIRST_NAME, p.LAST_NAME, p.INSURANCE_PROVIDER
HAVING COUNT(v.VISIT_ID) >= 3
ORDER BY total_visits DESC, total_spent DESC;

-- New vs Returning Patients (Last 30 Days)
-- ----------------------------------------------------------------------------
WITH patient_visits AS (
    SELECT 
        p.PATIENT_ID,
        MIN(v.VISIT_DATE) as first_visit_ever,
        MAX(v.VISIT_DATE) as last_visit
    FROM PATIENTS p
    LEFT JOIN VISITS v ON p.PATIENT_ID = v.PATIENT_ID
    GROUP BY p.PATIENT_ID
)
SELECT 
    CASE 
        WHEN pv.first_visit_ever >= DATEADD(day, -30, CURRENT_DATE) THEN 'New Patient'
        ELSE 'Returning Patient'
    END as patient_type,
    COUNT(DISTINCT v.PATIENT_ID) as patient_count,
    COUNT(v.VISIT_ID) as total_visits,
    SUM(v.TOTAL_CHARGE) as total_revenue
FROM VISITS v
JOIN patient_visits pv ON v.PATIENT_ID = pv.PATIENT_ID
WHERE v.VISIT_DATE >= DATEADD(day, -30, CURRENT_DATE)
GROUP BY patient_type;

-- Patient Demographics Summary
-- ----------------------------------------------------------------------------
SELECT 
    CASE 
        WHEN DATEDIFF(year, DATE_OF_BIRTH, CURRENT_DATE) < 18 THEN 'Pediatric (0-17)'
        WHEN DATEDIFF(year, DATE_OF_BIRTH, CURRENT_DATE) BETWEEN 18 AND 30 THEN 'Young Adult (18-30)'
        WHEN DATEDIFF(year, DATE_OF_BIRTH, CURRENT_DATE) BETWEEN 31 AND 50 THEN 'Adult (31-50)'
        WHEN DATEDIFF(year, DATE_OF_BIRTH, CURRENT_DATE) BETWEEN 51 AND 65 THEN 'Middle Age (51-65)'
        ELSE 'Senior (65+)'
    END as age_group,
    COUNT(DISTINCT p.PATIENT_ID) as patient_count,
    COUNT(v.VISIT_ID) as total_visits,
    ROUND(AVG(v.TOTAL_CHARGE), 2) as avg_visit_cost
FROM PATIENTS p
LEFT JOIN VISITS v ON p.PATIENT_ID = v.PATIENT_ID
GROUP BY age_group
ORDER BY age_group;

-- ============================================================================
-- 7. CDC Audit Trail (Change Tracking)
-- ============================================================================

-- Recent Changes to Appointments (CDC Audit Trail)
-- ----------------------------------------------------------------------------
SELECT 
    APPOINTMENT_ID,
    PATIENT_ID,
    DOCTOR_ID,
    APPOINTMENT_DATE,
    APPOINTMENT_TIME,
    STATUS,
    REASON_FOR_VISIT,
    _CHANGE_TYPE,
    _COMMIT_TIMESTAMP,
    _INGESTION_TIMESTAMP,
    TIMESTAMPDIFF(SECOND, _COMMIT_TIMESTAMP, _INGESTION_TIMESTAMP) as latency_seconds
FROM APPOINTMENTS
WHERE _COMMIT_TIMESTAMP >= DATEADD(hour, -24, CURRENT_TIMESTAMP)
ORDER BY _COMMIT_TIMESTAMP DESC
LIMIT 50;

-- Change Volume by Type (Last 24 Hours)
-- ----------------------------------------------------------------------------
SELECT 
    'APPOINTMENTS' as table_name,
    _CHANGE_TYPE,
    COUNT(*) as change_count,
    MIN(_COMMIT_TIMESTAMP) as first_change,
    MAX(_COMMIT_TIMESTAMP) as last_change
FROM APPOINTMENTS
WHERE _COMMIT_TIMESTAMP >= DATEADD(hour, -24, CURRENT_TIMESTAMP)
GROUP BY _CHANGE_TYPE

UNION ALL

SELECT 
    'VISITS',
    _CHANGE_TYPE,
    COUNT(*),
    MIN(_COMMIT_TIMESTAMP),
    MAX(_COMMIT_TIMESTAMP)
FROM VISITS
WHERE _COMMIT_TIMESTAMP >= DATEADD(hour, -24, CURRENT_TIMESTAMP)
GROUP BY _CHANGE_TYPE

ORDER BY table_name, _CHANGE_TYPE;

-- CDC Replication Latency Analysis
-- ----------------------------------------------------------------------------
SELECT 
    DATE_TRUNC('MINUTE', _COMMIT_TIMESTAMP) as minute_block,
    COUNT(*) as changes,
    AVG(TIMESTAMPDIFF(SECOND, _COMMIT_TIMESTAMP, _INGESTION_TIMESTAMP)) as avg_latency_seconds,
    MAX(TIMESTAMPDIFF(SECOND, _COMMIT_TIMESTAMP, _INGESTION_TIMESTAMP)) as max_latency_seconds
FROM APPOINTMENTS
WHERE _COMMIT_TIMESTAMP >= DATEADD(hour, -4, CURRENT_TIMESTAMP)
GROUP BY minute_block
ORDER BY minute_block DESC;

-- ============================================================================
-- 8. Operational KPIs Dashboard
-- ============================================================================

-- Executive Summary (Last 30 Days)
-- ----------------------------------------------------------------------------
SELECT 
    COUNT(DISTINCT a.PATIENT_ID) as unique_patients_served,
    COUNT(DISTINCT a.APPOINTMENT_ID) as total_appointments,
    SUM(CASE WHEN a.STATUS = 'completed' THEN 1 ELSE 0 END) as completed_appointments,
    ROUND(SUM(CASE WHEN a.STATUS = 'completed' THEN 1 ELSE 0 END) * 100.0 / 
          COUNT(a.APPOINTMENT_ID), 1) as completion_rate,
    SUM(CASE WHEN a.STATUS = 'no_show' THEN 1 ELSE 0 END) as no_shows,
    ROUND(SUM(CASE WHEN a.STATUS = 'no_show' THEN 1 ELSE 0 END) * 100.0 / 
          COUNT(a.APPOINTMENT_ID), 1) as no_show_rate,
    COUNT(DISTINCT v.VISIT_ID) as total_visits,
    SUM(v.TOTAL_CHARGE) as total_revenue,
    ROUND(AVG(v.TOTAL_CHARGE), 2) as avg_revenue_per_visit,
    COUNT(DISTINCT v.DOCTOR_ID) as active_doctors
FROM APPOINTMENTS a
LEFT JOIN VISITS v ON a.APPOINTMENT_ID = v.APPOINTMENT_ID
WHERE a.APPOINTMENT_DATE >= DATEADD(day, -30, CURRENT_DATE)
  AND a.APPOINTMENT_DATE <= CURRENT_DATE;

-- ============================================================================
-- End of Analytics Queries
-- ============================================================================
-- These queries demonstrate the power of real-time CDC data for:
-- - Operational decision making
-- - Performance monitoring
-- - Revenue optimization
-- - Patient care improvement
-- - Compliance and audit trails
-- ============================================================================
