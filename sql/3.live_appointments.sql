-- ============================================================================
-- Copyright 2025 Snowflake Inc.
-- SPDX-License-Identifier: Apache-2.0
-- Licensed under the Apache License, Version 2.0 (the "License");
-- You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0
-- ============================================================================
--
-- OpenFlow PostgreSQL CDC Quickstart - Live CDC Demo
--
-- This script simulates a busy day at the clinic with various appointment activities
-- Run this on PostgreSQL to generate CDC events that will flow to Snowflake
-- 
-- What this script does:
-- - Books new appointments (INSERT)
-- - Updates appointment statuses (UPDATE)
-- - Creates visit records (INSERT)
-- - Simulates cancellations and no-shows (UPDATE)
-- - Updates doctor availability (UPDATE)
-- 
-- After running, check Snowflake to see these changes appear in real-time!
-- ============================================================================

SET search_path TO healthcare;

-- ============================================================================
-- Scenario: Morning Operations at DemoClinic Healthcare
-- Time: 8:00 AM - 12:00 PM
-- ============================================================================

-- 8:00 AM - Three patients call to book new appointments
-- ----------------------------------------------------------------------------

\echo '🕐 8:00 AM - New appointment requests coming in...'

INSERT INTO appointments (patient_id, doctor_id, appointment_date, appointment_time, status, reason_for_visit, appointment_type, created_at, updated_at) VALUES
(5, 1, CURRENT_DATE + 3, '09:00:00', 'scheduled', 'Persistent cough and fever', 'urgent', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(17, 2, CURRENT_DATE + 5, '10:30:00', 'scheduled', 'Blood sugar monitoring', 'follow_up', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(29, 3, CURRENT_DATE + 7, '14:00:00', 'scheduled', 'Annual wellness visit', 'routine', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

\echo '✅ 3 new appointments scheduled'
\echo ''

-- Wait a moment to simulate time passing
SELECT pg_sleep(2);

-- 8:15 AM - Front desk confirms scheduled appointments for today
-- ----------------------------------------------------------------------------

\echo '🕐 8:15 AM - Front desk confirming today''s appointments...'

UPDATE appointments 
SET status = 'confirmed', 
    updated_at = CURRENT_TIMESTAMP
WHERE appointment_date = CURRENT_DATE 
  AND status = 'scheduled';

\echo '✅ Today''s appointments confirmed'
\echo ''

SELECT pg_sleep(2);

-- 8:30 AM - First patients arrive and check in
-- ----------------------------------------------------------------------------

\echo '🕐 8:30 AM - Patients checking in for their appointments...'

-- Check in 4 patients who have appointments today
UPDATE appointments 
SET status = 'checked_in', 
    updated_at = CURRENT_TIMESTAMP
WHERE appointment_date = CURRENT_DATE 
  AND status = 'confirmed'
  AND appointment_id IN (
    SELECT appointment_id 
    FROM appointments 
    WHERE appointment_date = CURRENT_DATE AND status = 'confirmed' 
    LIMIT 4
  );

\echo '✅ 4 patients checked in'
\echo ''

SELECT pg_sleep(2);

-- 9:00 AM - Doctors start seeing patients
-- ----------------------------------------------------------------------------

\echo '🕐 9:00 AM - Doctors beginning patient visits...'

-- Move checked-in patients to in_progress status
UPDATE appointments 
SET status = 'in_progress', 
    updated_at = CURRENT_TIMESTAMP
WHERE status = 'checked_in' 
  AND appointment_id IN (
    SELECT appointment_id 
    FROM appointments 
    WHERE status = 'checked_in' 
    LIMIT 2
  );

\echo '✅ 2 visits now in progress'
\echo ''

SELECT pg_sleep(3);

-- 9:30 AM - First visits complete, records created
-- ----------------------------------------------------------------------------

\echo '🕐 9:30 AM - Completing visits and creating visit records...'

-- Complete the appointments that were in progress
WITH completed_appts AS (
    UPDATE appointments 
    SET status = 'completed', 
        updated_at = CURRENT_TIMESTAMP
    WHERE status = 'in_progress'
    RETURNING appointment_id, patient_id, doctor_id, appointment_date, appointment_time
)
-- Create visit records for these completed appointments
INSERT INTO visits (appointment_id, patient_id, doctor_id, visit_date, visit_start_time, visit_end_time, diagnosis, treatment_notes, follow_up_required, prescription_given, total_charge)
SELECT 
    ca.appointment_id,
    ca.patient_id,
    ca.doctor_id,
    ca.appointment_date,
    (ca.appointment_date || ' ' || ca.appointment_time::TEXT)::TIMESTAMP,
    (ca.appointment_date || ' ' || ca.appointment_time::TEXT)::TIMESTAMP + INTERVAL '25 minutes',
    CASE ca.appointment_id % 5
        WHEN 0 THEN 'Acute upper respiratory infection'
        WHEN 1 THEN 'Hypertension, controlled'
        WHEN 2 THEN 'Type 2 Diabetes Mellitus'
        WHEN 3 THEN 'Annual wellness - no acute findings'
        ELSE 'Follow-up visit - stable condition'
    END,
    'Patient examined and vitals recorded. Treatment plan discussed. Patient educated on medication adherence and lifestyle modifications.',
    (ca.appointment_id % 3 = 0),
    (ca.appointment_id % 2 = 0),
    125.00 + (ca.appointment_id % 10) * 15.00
FROM completed_appts ca;

\echo '✅ 2 visits completed with records created'
\echo ''

SELECT pg_sleep(2);

-- 10:00 AM - More patients book walk-in urgent appointments
-- ----------------------------------------------------------------------------

\echo '🕐 10:00 AM - Walk-in patients arriving...'

INSERT INTO appointments (patient_id, doctor_id, appointment_date, appointment_time, status, reason_for_visit, appointment_type, created_at, updated_at) VALUES
(41, 1, CURRENT_DATE, '10:45:00', 'confirmed', 'Severe allergic reaction', 'urgent', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(53, 3, CURRENT_DATE, '11:15:00', 'confirmed', 'Chest pain evaluation', 'urgent', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

\echo '✅ 2 urgent walk-in appointments added'
\echo ''

SELECT pg_sleep(2);

-- 10:30 AM - Patient cancels afternoon appointment
-- ----------------------------------------------------------------------------

\echo '🕐 10:30 AM - Appointment cancellation received...'

UPDATE appointments
SET status = 'cancelled',
    updated_at = CURRENT_TIMESTAMP
WHERE appointment_date = CURRENT_DATE 
  AND status = 'confirmed'
  AND appointment_time > '12:00:00'
LIMIT 1;

\echo '✅ 1 appointment cancelled'
\echo ''

SELECT pg_sleep(2);

-- 11:00 AM - Continue processing remaining appointments
-- ----------------------------------------------------------------------------

\echo '🕐 11:00 AM - Processing more appointments...'

-- Check in waiting patients
UPDATE appointments 
SET status = 'checked_in', 
    updated_at = CURRENT_TIMESTAMP
WHERE status = 'confirmed' 
  AND appointment_date = CURRENT_DATE
  AND appointment_time <= '11:30:00'
LIMIT 2;

\echo '✅ 2 more patients checked in'
\echo ''

SELECT pg_sleep(2);

-- Start new visits
UPDATE appointments 
SET status = 'in_progress', 
    updated_at = CURRENT_TIMESTAMP
WHERE status = 'checked_in'
  AND appointment_date = CURRENT_DATE
LIMIT 2;

\echo '✅ 2 new visits in progress'
\echo ''

SELECT pg_sleep(3);

-- 11:30 AM - Complete more visits
-- ----------------------------------------------------------------------------

\echo '🕐 11:30 AM - Completing more visits...'

WITH completed_appts AS (
    UPDATE appointments 
    SET status = 'completed', 
        updated_at = CURRENT_TIMESTAMP
    WHERE status = 'in_progress'
    RETURNING appointment_id, patient_id, doctor_id, appointment_date, appointment_time
)
INSERT INTO visits (appointment_id, patient_id, doctor_id, visit_date, visit_start_time, visit_end_time, diagnosis, treatment_notes, follow_up_required, prescription_given, total_charge)
SELECT 
    ca.appointment_id,
    ca.patient_id,
    ca.doctor_id,
    ca.appointment_date,
    (ca.appointment_date || ' ' || ca.appointment_time::TEXT)::TIMESTAMP,
    (ca.appointment_date || ' ' || ca.appointment_time::TEXT)::TIMESTAMP + INTERVAL '30 minutes',
    CASE ca.appointment_id % 4
        WHEN 0 THEN 'Allergic rhinitis'
        WHEN 1 THEN 'Acute sinusitis'
        WHEN 2 THEN 'Contact dermatitis'
        ELSE 'Routine checkup - all normal'
    END,
    'Comprehensive examination completed. Lab work ordered as needed. Follow-up scheduled if required.',
    (ca.appointment_id % 4 = 0),
    (ca.appointment_id % 3 = 0),
    150.00 + (ca.appointment_id % 8) * 20.00
FROM completed_appts ca;

\echo '✅ Additional visits completed'
\echo ''

SELECT pg_sleep(2);

-- 12:00 PM - New appointments for next week
-- ----------------------------------------------------------------------------

\echo '🕐 12:00 PM - Scheduling future appointments...'

INSERT INTO appointments (patient_id, doctor_id, appointment_date, appointment_time, status, reason_for_visit, appointment_type, created_at, updated_at) VALUES
(8, 4, CURRENT_DATE + 10, '09:30:00', 'scheduled', 'Follow-up cardiac evaluation', 'follow_up', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(19, 6, CURRENT_DATE + 12, '10:00:00', 'scheduled', 'Child immunization', 'routine', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(31, 8, CURRENT_DATE + 14, '14:30:00', 'scheduled', 'Sports injury follow-up', 'follow_up', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(44, 9, CURRENT_DATE + 15, '11:00:00', 'scheduled', 'Skin condition check', 'routine', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(57, 10, CURRENT_DATE + 17, '13:00:00', 'scheduled', 'Chronic disease management', 'follow_up', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

\echo '✅ 5 new appointments scheduled for next two weeks'
\echo ''

-- 12:15 PM - One patient marked as no-show
-- ----------------------------------------------------------------------------

\echo '🕐 12:15 PM - Marking no-show for missed appointment...'

UPDATE appointments
SET status = 'no_show',
    updated_at = CURRENT_TIMESTAMP
WHERE appointment_date = CURRENT_DATE 
  AND status = 'confirmed'
  AND appointment_time < '12:00:00'
LIMIT 1;

\echo '✅ 1 patient marked as no-show'
\echo ''

-- 12:30 PM - Update doctor availability
-- ----------------------------------------------------------------------------

\echo '🕐 12:30 PM - Updating doctor schedules...'

-- Dr. Anderson temporarily not accepting new patients (on vacation next week)
UPDATE doctors
SET accepting_new_patients = FALSE,
    updated_at = CURRENT_TIMESTAMP
WHERE doctor_id = 9;

\echo '✅ Doctor availability updated'
\echo ''

-- ============================================================================
-- CDC Demo Complete! Summary of Changes
-- ============================================================================

\echo ''
\echo '═══════════════════════════════════════════════════════════'
\echo '📊 CDC Demo Summary - Changes Generated'
\echo '═══════════════════════════════════════════════════════════'

-- Count of changes by type
SELECT 
    'New appointments created' as activity,
    COUNT(*) as count
FROM appointments 
WHERE created_at > CURRENT_TIMESTAMP - INTERVAL '5 minutes'

UNION ALL

SELECT 
    'Appointments updated (status changes)',
    COUNT(*) 
FROM appointments 
WHERE updated_at > created_at 
  AND updated_at > CURRENT_TIMESTAMP - INTERVAL '5 minutes'

UNION ALL

SELECT 
    'New visit records created',
    COUNT(*) 
FROM visits 
WHERE visit_date = CURRENT_DATE

UNION ALL

SELECT 
    'Doctor records updated',
    COUNT(*) 
FROM doctors 
WHERE updated_at > CURRENT_TIMESTAMP - INTERVAL '5 minutes';

\echo ''
\echo 'Current appointment status distribution:'
SELECT status, COUNT(*) as count 
FROM appointments 
GROUP BY status 
ORDER BY count DESC;

\echo ''
\echo '═══════════════════════════════════════════════════════════'
\echo '✅ All CDC events have been generated!'
\echo ''
\echo 'Next Steps:'
\echo '1. Go to your Snowflake account'
\echo '2. Query the APPOINTMENTS and VISITS tables'
\echo '3. Look for _CHANGE_TYPE column values:'
\echo '   - INSERT: New records added'
\echo '   - UPDATE: Existing records modified'
\echo '4. Check _COMMIT_TIMESTAMP to see when changes occurred'
\echo '5. Run 4.analytics_queries.sql to analyze the updated data'
\echo '═══════════════════════════════════════════════════════════'
\echo ''

-- Show some of the recent changes
\echo 'Sample of recent changes:'
SELECT 
    appointment_id,
    patient_id,
    doctor_id,
    appointment_date,
    status,
    reason_for_visit,
    updated_at
FROM appointments 
WHERE updated_at > CURRENT_TIMESTAMP - INTERVAL '5 minutes'
ORDER BY updated_at DESC
LIMIT 10;
