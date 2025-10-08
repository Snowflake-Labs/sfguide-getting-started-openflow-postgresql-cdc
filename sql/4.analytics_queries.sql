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
USE SCHEMA "healthcare";
USE WAREHOUSE QUICKSTART_PGCDC_WH;

-- ============================================================================
-- 1. Real-time Operational Dashboard Queries
-- ============================================================================

-- Current Day Appointment Status (Real-time Dashboard)
-- ----------------------------------------------------------------------------
SELECT 
    "status",
    COUNT(*) as appointment_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage,
    LISTAGG(DISTINCT TO_CHAR("appointment_time", 'HH24:MI'), ', ') WITHIN GROUP (ORDER BY "appointment_time") as time_slots
FROM "appointments"
WHERE "appointment_date" = CURRENT_DATE
GROUP BY "status"
ORDER BY appointment_count DESC;

-- Patients Currently in Clinic
-- ----------------------------------------------------------------------------
SELECT 
    p."first_name" || ' ' || p."last_name" as patient_name,
    p."phone",
    d."first_name" || ' ' || d."last_name" as doctor_name,
    d."specialization",
    a."appointment_time",
    a."status",
    a."reason_for_visit",
    TIMESTAMPDIFF(MINUTE, 
        TO_TIMESTAMP(TO_CHAR(a."appointment_date", 'YYYY-MM-DD') || ' ' || TO_CHAR(a."appointment_time", 'HH24:MI:SS')),
        CURRENT_TIMESTAMP()) as minutes_since_appointment
FROM "appointments" a
JOIN "patients" p ON a."patient_id" = p."patient_id"
JOIN "doctors" d ON a."doctor_id" = d."doctor_id"
WHERE a."appointment_date" = CURRENT_DATE
  AND a."status" IN ('checked_in', 'in_progress')
ORDER BY a."status" DESC, a."appointment_time";

-- Doctor Availability Today
-- ----------------------------------------------------------------------------
SELECT 
    d."first_name" || ' ' || d."last_name" as doctor_name,
    d."specialization",
    d."department",
    d."accepting_new_patients",
    COUNT(CASE WHEN a."status" = 'completed' THEN 1 END) as completed_today,
    COUNT(CASE WHEN a."status" = 'in_progress' THEN 1 END) as currently_seeing,
    COUNT(CASE WHEN a."status" IN ('confirmed', 'checked_in') THEN 1 END) as waiting,
    COUNT(a."appointment_id") as total_appointments_today
FROM "doctors" d
LEFT JOIN "appointments" a ON d."doctor_id" = a."doctor_id" AND a."appointment_date" = CURRENT_DATE
GROUP BY d."doctor_id", d."first_name", d."last_name", d."specialization", d."department", d."accepting_new_patients"
ORDER BY total_appointments_today DESC;

-- ============================================================================
-- 2. Patient Flow Analysis
-- ============================================================================

-- Average Wait Time by Status Transition
-- ----------------------------------------------------------------------------
SELECT 
    "appointment_date",
    AVG(TIMESTAMPDIFF(MINUTE, "created_at", "updated_at")) as avg_minutes_to_update,
    COUNT(*) as appointments
FROM "appointments"
WHERE "status" IN ('completed', 'cancelled', 'no_show')
  AND "created_at" != "updated_at"
  AND "appointment_date" >= DATEADD(day, -30, CURRENT_DATE)
GROUP BY "appointment_date"
ORDER BY "appointment_date" DESC
LIMIT 30;

-- Appointment Completion Rate (Last 30 Days)
-- ----------------------------------------------------------------------------
WITH appointment_metrics AS (
    SELECT 
        "appointment_date",
        COUNT(*) as total_appointments,
        SUM(CASE WHEN "status" = 'completed' THEN 1 ELSE 0 END) as completed,
        SUM(CASE WHEN "status" = 'cancelled' THEN 1 ELSE 0 END) as cancelled,
        SUM(CASE WHEN "status" = 'no_show' THEN 1 ELSE 0 END) as no_shows
    FROM "appointments"
    WHERE "appointment_date" >= DATEADD(day, -30, CURRENT_DATE)
      AND "appointment_date" <= CURRENT_DATE
    GROUP BY "appointment_date"
)
SELECT 
    "appointment_date",
    total_appointments,
    completed,
    cancelled,
    no_shows,
    ROUND(completed * 100.0 / NULLIF(total_appointments, 0), 1) as completion_rate,
    ROUND(no_shows * 100.0 / NULLIF(total_appointments, 0), 1) as no_show_rate
FROM appointment_metrics
ORDER BY "appointment_date" DESC;

-- Peak Hours Analysis
-- ----------------------------------------------------------------------------
SELECT 
    TO_CHAR("appointment_time", 'HH24:00') as hour_block,
    COUNT(*) as total_appointments,
    AVG(CASE WHEN "status" = 'completed' THEN 1.0 ELSE 0.0 END) * 100 as completion_rate,
    AVG(CASE WHEN "status" = 'no_show' THEN 1.0 ELSE 0.0 END) * 100 as no_show_rate
FROM "appointments"
WHERE "appointment_date" >= DATEADD(day, -30, CURRENT_DATE)
GROUP BY hour_block
ORDER BY hour_block;

-- Busiest Days of Week
-- ----------------------------------------------------------------------------
SELECT 
    DAYNAME("appointment_date") as day_of_week,
    DAYOFWEEK("appointment_date") as day_num,
    COUNT(*) as total_appointments,
    AVG(CASE WHEN "status" = 'completed' THEN 1.0 ELSE 0.0 END) * 100 as completion_rate
FROM "appointments"
WHERE "appointment_date" >= DATEADD(day, -90, CURRENT_DATE)
GROUP BY day_of_week, day_num
ORDER BY day_num;

-- ============================================================================
-- 3. Doctor Performance Metrics
-- ============================================================================

-- Doctor Productivity (Last 30 Days)
-- ----------------------------------------------------------------------------
SELECT 
    d."first_name" || ' ' || d."last_name" as doctor_name,
    d."specialization",
    COUNT(a."appointment_id") as total_appointments,
    SUM(CASE WHEN a."status" = 'completed' THEN 1 ELSE 0 END) as completed,
    SUM(CASE WHEN a."status" = 'cancelled' THEN 1 ELSE 0 END) as cancelled,
    SUM(CASE WHEN a."status" = 'no_show' THEN 1 ELSE 0 END) as no_shows,
    ROUND(SUM(CASE WHEN a."status" = 'completed' THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(COUNT(a."appointment_id"), 0), 1) as completion_rate,
    COUNT(DISTINCT a."appointment_date") as days_worked
FROM "doctors" d
LEFT JOIN "appointments" a ON d."doctor_id" = a."doctor_id" 
    AND a."appointment_date" >= DATEADD(day, -30, CURRENT_DATE)
    AND a."appointment_date" <= CURRENT_DATE
GROUP BY d."doctor_id", d."first_name", d."last_name", d."specialization"
ORDER BY completed DESC;

-- Average Appointments per Day by Doctor
-- ----------------------------------------------------------------------------
SELECT 
    d."first_name" || ' ' || d."last_name" as doctor_name,
    d."specialization",
    COUNT(a."appointment_id") as total_appointments,
    COUNT(DISTINCT a."appointment_date") as days_with_appointments,
    ROUND(COUNT(a."appointment_id") * 1.0 / 
          NULLIF(COUNT(DISTINCT a."appointment_date"), 0), 1) as avg_appointments_per_day
FROM "doctors" d
LEFT JOIN "appointments" a ON d."doctor_id" = a."doctor_id" 
    AND a."appointment_date" >= DATEADD(day, -30, CURRENT_DATE)
    AND a."status" = 'completed'
GROUP BY d."doctor_id", d."first_name", d."last_name", d."specialization"
HAVING COUNT(DISTINCT a."appointment_date") > 0
ORDER BY avg_appointments_per_day DESC;

-- Patient Satisfaction Proxy (Follow-up Required Rate)
-- ----------------------------------------------------------------------------
SELECT 
    d."first_name" || ' ' || d."last_name" as doctor_name,
    d."specialization",
    COUNT(v."visit_id") as total_visits,
    SUM(CASE WHEN v."follow_up_required" THEN 1 ELSE 0 END) as followups_needed,
    ROUND(SUM(CASE WHEN v."follow_up_required" THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(COUNT(v."visit_id"), 0), 1) as followup_rate,
    SUM(CASE WHEN v."prescription_given" THEN 1 ELSE 0 END) as prescriptions_written,
    ROUND(SUM(CASE WHEN v."prescription_given" THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(COUNT(v."visit_id"), 0), 1) as prescription_rate
FROM "doctors" d
JOIN "visits" v ON d."doctor_id" = v."doctor_id"
GROUP BY d."doctor_id", d."first_name", d."last_name", d."specialization"
ORDER BY total_visits DESC;

-- ============================================================================
-- 4. Revenue Analytics
-- ============================================================================

-- Daily Revenue Trend
-- ----------------------------------------------------------------------------
SELECT 
    "visit_date",
    COUNT(*) as visit_count,
    SUM("total_charge") as daily_revenue,
    AVG("total_charge") as avg_revenue_per_visit,
    MIN("total_charge") as min_charge,
    MAX("total_charge") as max_charge
FROM "visits"
WHERE "visit_date" >= DATEADD(day, -30, CURRENT_DATE)
GROUP BY "visit_date"
ORDER BY "visit_date" DESC;

-- Revenue by Department
-- ----------------------------------------------------------------------------
SELECT 
    d."department",
    COUNT(v."visit_id") as visit_count,
    SUM(v."total_charge") as total_revenue,
    ROUND(AVG(v."total_charge"), 2) as avg_revenue_per_visit,
    ROUND(SUM(v."total_charge") * 100.0 / SUM(SUM(v."total_charge")) OVER (), 1) as revenue_percentage
FROM "doctors" d
JOIN "visits" v ON d."doctor_id" = v."doctor_id"
GROUP BY d."department"
ORDER BY total_revenue DESC;

-- Revenue by Doctor (Top 10)
-- ----------------------------------------------------------------------------
SELECT 
    d."first_name" || ' ' || d."last_name" as doctor_name,
    d."specialization",
    d."department",
    COUNT(v."visit_id") as total_visits,
    SUM(v."total_charge") as total_revenue,
    ROUND(AVG(v."total_charge"), 2) as avg_charge_per_visit
FROM "doctors" d
JOIN "visits" v ON d."doctor_id" = v."doctor_id"
GROUP BY d."doctor_id", d."first_name", d."last_name", d."specialization", d."department"
ORDER BY total_revenue DESC
LIMIT 10;

-- Monthly Revenue Summary
-- ----------------------------------------------------------------------------
SELECT 
    TO_CHAR("visit_date", 'YYYY-MM') as month,
    COUNT(*) as total_visits,
    SUM("total_charge") as monthly_revenue,
    ROUND(AVG("total_charge"), 2) as avg_revenue_per_visit,
    COUNT(DISTINCT "patient_id") as unique_patients
FROM "visits"
GROUP BY month
ORDER BY month DESC;

-- ============================================================================
-- 5. Clinical Insights
-- ============================================================================

-- Most Common Diagnoses
-- ----------------------------------------------------------------------------
SELECT 
    "diagnosis",
    COUNT(*) as frequency,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage,
    ROUND(AVG("total_charge"), 2) as avg_cost,
    SUM(CASE WHEN "follow_up_required" THEN 1 ELSE 0 END) as followups_needed
FROM "visits"
GROUP BY "diagnosis"
ORDER BY frequency DESC
LIMIT 15;

-- Most Common Reasons for Visit
-- ----------------------------------------------------------------------------
SELECT 
    "reason_for_visit",
    COUNT(*) as frequency,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage_of_appointments,
    SUM(CASE WHEN "status" = 'completed' THEN 1 ELSE 0 END) as completed,
    SUM(CASE WHEN "status" = 'no_show' THEN 1 ELSE 0 END) as no_shows
FROM "appointments"
WHERE "appointment_date" >= DATEADD(day, -60, CURRENT_DATE)
GROUP BY "reason_for_visit"
ORDER BY frequency DESC
LIMIT 15;

-- Appointment Type Distribution by Specialization
-- ----------------------------------------------------------------------------
SELECT 
    d."specialization",
    a."appointment_type",
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY d."specialization"), 1) as percentage
FROM "appointments" a
JOIN "doctors" d ON a."doctor_id" = d."doctor_id"
WHERE a."appointment_date" >= DATEADD(day, -60, CURRENT_DATE)
GROUP BY d."specialization", a."appointment_type"
ORDER BY d."specialization", count DESC;

-- ============================================================================
-- 6. Patient Analytics
-- ============================================================================

-- Patient Visit Frequency
-- ----------------------------------------------------------------------------
SELECT 
    p."patient_id",
    p."first_name" || ' ' || p."last_name" as patient_name,
    p."insurance_provider",
    COUNT(v."visit_id") as total_visits,
    MIN(v."visit_date") as first_visit,
    MAX(v."visit_date") as most_recent_visit,
    SUM(v."total_charge") as total_spent,
    ROUND(AVG(v."total_charge"), 2) as avg_per_visit
FROM "patients" p
JOIN "visits" v ON p."patient_id" = v."patient_id"
GROUP BY p."patient_id", p."first_name", p."last_name", p."insurance_provider"
HAVING COUNT(v."visit_id") >= 3
ORDER BY total_visits DESC, total_spent DESC;

-- New vs Returning Patients (Last 30 Days)
-- ----------------------------------------------------------------------------
WITH patient_visits AS (
    SELECT 
        p."patient_id",
        MIN(v."visit_date") as first_visit_ever,
        MAX(v."visit_date") as last_visit
    FROM "patients" p
    LEFT JOIN "visits" v ON p."patient_id" = v."patient_id"
    GROUP BY p."patient_id"
)
SELECT 
    CASE 
        WHEN pv.first_visit_ever >= DATEADD(day, -30, CURRENT_DATE) THEN 'New Patient'
        ELSE 'Returning Patient'
    END as patient_type,
    COUNT(DISTINCT v."patient_id") as patient_count,
    COUNT(v."visit_id") as total_visits,
    SUM(v."total_charge") as total_revenue
FROM "visits" v
JOIN patient_visits pv ON v."patient_id" = pv."patient_id"
WHERE v."visit_date" >= DATEADD(day, -30, CURRENT_DATE)
GROUP BY patient_type;

-- Patient Demographics Summary
-- ----------------------------------------------------------------------------
SELECT 
    CASE 
        WHEN DATEDIFF(year, "date_of_birth", CURRENT_DATE) < 18 THEN 'Pediatric (0-17)'
        WHEN DATEDIFF(year, "date_of_birth", CURRENT_DATE) BETWEEN 18 AND 30 THEN 'Young Adult (18-30)'
        WHEN DATEDIFF(year, "date_of_birth", CURRENT_DATE) BETWEEN 31 AND 50 THEN 'Adult (31-50)'
        WHEN DATEDIFF(year, "date_of_birth", CURRENT_DATE) BETWEEN 51 AND 65 THEN 'Middle Age (51-65)'
        ELSE 'Senior (65+)'
    END as age_group,
    COUNT(DISTINCT p."patient_id") as patient_count,
    COUNT(v."visit_id") as total_visits,
    ROUND(AVG(v."total_charge"), 2) as avg_visit_cost
FROM "patients" p
LEFT JOIN "visits" v ON p."patient_id" = v."patient_id"
GROUP BY age_group
ORDER BY age_group;

-- ============================================================================
-- 7. CDC Audit Trail (Change Tracking)
-- ============================================================================

-- Recent Changes to Appointments (CDC Audit Trail)
-- ----------------------------------------------------------------------------
-- Shows recently modified appointments based on _SNOWFLAKE_UPDATED_AT timestamp
SELECT 
    "appointment_id",
    "patient_id",
    "doctor_id",
    "appointment_date",
    "appointment_time",
    "status",
    "reason_for_visit",
    "_SNOWFLAKE_INSERTED_AT",
    "_SNOWFLAKE_UPDATED_AT",
    "_SNOWFLAKE_DELETED"
FROM "appointments"
WHERE "_SNOWFLAKE_UPDATED_AT" IS NOT NULL  -- Only show records that have been updated
  AND "_SNOWFLAKE_UPDATED_AT" >= DATEADD(hour, -24, CURRENT_TIMESTAMP)
ORDER BY "_SNOWFLAKE_UPDATED_AT" DESC
LIMIT 50;

-- Change Volume Analysis (Last 24 Hours)
-- ----------------------------------------------------------------------------
-- Count of new inserts vs updates in the last 24 hours
SELECT 
    'appointments' as table_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN "_SNOWFLAKE_UPDATED_AT" IS NULL THEN 1 ELSE 0 END) as insert_only,
    SUM(CASE WHEN "_SNOWFLAKE_UPDATED_AT" IS NOT NULL THEN 1 ELSE 0 END) as updated,
    SUM(CASE WHEN "_SNOWFLAKE_DELETED" THEN 1 ELSE 0 END) as deleted
FROM "appointments"
WHERE "_SNOWFLAKE_INSERTED_AT" >= DATEADD(hour, -24, CURRENT_TIMESTAMP)
   OR "_SNOWFLAKE_UPDATED_AT" >= DATEADD(hour, -24, CURRENT_TIMESTAMP)

UNION ALL

SELECT 
    'visits',
    COUNT(*),
    SUM(CASE WHEN "_SNOWFLAKE_UPDATED_AT" IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN "_SNOWFLAKE_UPDATED_AT" IS NOT NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN "_SNOWFLAKE_DELETED" THEN 1 ELSE 0 END)
FROM "visits"
WHERE "_SNOWFLAKE_INSERTED_AT" >= DATEADD(hour, -24, CURRENT_TIMESTAMP)
   OR "_SNOWFLAKE_UPDATED_AT" >= DATEADD(hour, -24, CURRENT_TIMESTAMP)

ORDER BY table_name;

-- CDC Update Frequency Analysis
-- ----------------------------------------------------------------------------
-- Analyze when records are being updated
SELECT 
    DATE_TRUNC('HOUR', "_SNOWFLAKE_UPDATED_AT") as hour_block,
    COUNT(*) as updates,
    COUNT(DISTINCT "appointment_id") as unique_appointments_updated
FROM "appointments"
WHERE "_SNOWFLAKE_UPDATED_AT" IS NOT NULL
  AND "_SNOWFLAKE_UPDATED_AT" >= DATEADD(day, -7, CURRENT_TIMESTAMP)
GROUP BY hour_block
ORDER BY hour_block DESC;

-- ============================================================================
-- 8. Operational KPIs Dashboard
-- ============================================================================

-- Executive Summary (Last 30 Days)
-- ----------------------------------------------------------------------------
SELECT 
    COUNT(DISTINCT a."patient_id") as unique_patients_served,
    COUNT(DISTINCT a."appointment_id") as total_appointments,
    SUM(CASE WHEN a."status" = 'completed' THEN 1 ELSE 0 END) as completed_appointments,
    ROUND(SUM(CASE WHEN a."status" = 'completed' THEN 1 ELSE 0 END) * 100.0 / 
          COUNT(a."appointment_id"), 1) as completion_rate,
    SUM(CASE WHEN a."status" = 'no_show' THEN 1 ELSE 0 END) as no_shows,
    ROUND(SUM(CASE WHEN a."status" = 'no_show' THEN 1 ELSE 0 END) * 100.0 / 
          COUNT(a."appointment_id"), 1) as no_show_rate,
    COUNT(DISTINCT v."visit_id") as total_visits,
    SUM(v."total_charge") as total_revenue,
    ROUND(AVG(v."total_charge"), 2) as avg_revenue_per_visit,
    COUNT(DISTINCT v."doctor_id") as active_doctors
FROM "appointments" a
LEFT JOIN "visits" v ON a."appointment_id" = v."appointment_id"
WHERE a."appointment_date" >= DATEADD(day, -30, CURRENT_DATE)
  AND a."appointment_date" <= CURRENT_DATE;

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
