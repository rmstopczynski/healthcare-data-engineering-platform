-- =====================================================================
-- SAMPLE ANALYTICS QUERIES (Gold layer, Databricks)
-- Ported from the original Postgres analytics.* raw-SQL path onto the
-- dbt-built Gold models (workspace.gold.fact_*/dim_*). Table/column
-- names below match the actual dbt models, not the older sql/05
-- version's naming -- that raw-SQL path was a redundant duplicate of
-- the dbt marts and has been removed; these are the same 10 business
-- questions, now run against the one real Gold layer.
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. Average length of stay by hospital
-- ---------------------------------------------------------------------
SELECT
    hospital,
    COUNT(*)                           AS total_visits,
    ROUND(AVG(length_of_stay), 1)      AS avg_length_of_stay_days
FROM workspace.gold.fact_hospital_visit
GROUP BY hospital
ORDER BY avg_length_of_stay_days DESC;


-- ---------------------------------------------------------------------
-- 2. Total billing by insurance provider
-- ---------------------------------------------------------------------
SELECT
    insurance_provider,
    COUNT(*)                           AS total_visits,
    SUM(billing_amount)                AS total_billed,
    ROUND(AVG(billing_amount), 2)      AS avg_billed_per_visit
FROM workspace.gold.fact_hospital_visit
GROUP BY insurance_provider
ORDER BY total_billed DESC;


-- ---------------------------------------------------------------------
-- 3. Top 10 most-prescribed medications
-- ---------------------------------------------------------------------
SELECT
    m.medication_name,
    m.category,
    COUNT(*)                           AS times_prescribed,
    SUM(pf.total_cost)                 AS total_revenue
FROM workspace.gold.fact_prescription pf
JOIN workspace.gold.dim_medication m ON pf.medication_id = m.medication_id
GROUP BY m.medication_name, m.category
ORDER BY times_prescribed DESC
LIMIT 10;


-- ---------------------------------------------------------------------
-- 4. Patient age group breakdown by diagnosis
-- ---------------------------------------------------------------------
SELECT
    p.age_group,
    hv.diagnosis,
    COUNT(*)                           AS visit_count
FROM workspace.gold.fact_hospital_visit hv
JOIN workspace.gold.dim_patient p ON hv.patient_id = p.patient_id
GROUP BY p.age_group, hv.diagnosis
ORDER BY p.age_group, visit_count DESC;


-- ---------------------------------------------------------------------
-- 5. Monthly admission trend (uses the Julian date dimension)
-- ---------------------------------------------------------------------
SELECT
    d.year_num,
    d.month_num,
    d.month_name,
    COUNT(*)                           AS admissions
FROM workspace.gold.fact_hospital_visit hv
JOIN workspace.gold.dim_date d ON hv.admission_date = d.julian_day
GROUP BY d.year_num, d.month_num, d.month_name
ORDER BY d.year_num, d.month_num;


-- ---------------------------------------------------------------------
-- 6. Doctors by patient volume and specialty
-- ---------------------------------------------------------------------
SELECT
    doc.first_name || ' ' || doc.last_name  AS doctor_name,
    doc.specialty,
    doc.hospital_affi                        AS hospital,
    COUNT(DISTINCT hv.patient_id)             AS unique_patients_seen,
    COUNT(*)                                   AS total_visits
FROM workspace.gold.fact_hospital_visit hv
JOIN workspace.gold.dim_doctor doc ON hv.doctor_id = doc.doctor_id
GROUP BY doc.doctor_id, doc.first_name, doc.last_name, doc.specialty, doc.hospital_affi
ORDER BY total_visits DESC
LIMIT 10;


-- ---------------------------------------------------------------------
-- 7. Readmission signal: patients with more than one visit
-- ---------------------------------------------------------------------
SELECT
    p.patient_id,
    p.first_name || ' ' || p.last_name  AS patient_name,
    COUNT(*)                             AS visit_count,
    SUM(hv.billing_amount)                AS total_billed
FROM workspace.gold.fact_hospital_visit hv
JOIN workspace.gold.dim_patient p ON hv.patient_id = p.patient_id
GROUP BY p.patient_id, p.first_name, p.last_name
HAVING COUNT(*) > 1
ORDER BY visit_count DESC;


-- ---------------------------------------------------------------------
-- 8. Revenue by procedure category
-- ---------------------------------------------------------------------
SELECT
    proc.medical_category,
    COUNT(*)                            AS procedure_count,
    SUM(hv.procedure_charge)             AS total_revenue,
    ROUND(AVG(hv.procedure_charge), 2)    AS avg_charge
FROM workspace.gold.fact_hospital_visit hv
JOIN workspace.gold.dim_procedure proc ON hv.procedure_id = proc.procedure_id
GROUP BY proc.medical_category
ORDER BY total_revenue DESC;


-- ---------------------------------------------------------------------
-- 9. Refill rate by medication category
-- ---------------------------------------------------------------------
SELECT
    m.category,
    COUNT(*)                                                          AS total_prescriptions,
    SUM(CASE WHEN pf.refill_allowed THEN 1 ELSE 0 END)                 AS refillable_count,
    ROUND(100.0 * SUM(CASE WHEN pf.refill_allowed THEN 1 ELSE 0 END)
          / COUNT(*), 1)                                                AS refillable_pct
FROM workspace.gold.fact_prescription pf
JOIN workspace.gold.dim_medication m ON pf.medication_id = m.medication_id
GROUP BY m.category
ORDER BY refillable_pct DESC;


-- ---------------------------------------------------------------------
-- 10. Quarterly billing summary (full date-dimension usage)
-- ---------------------------------------------------------------------
SELECT
    d.year_num,
    d.quarter,
    COUNT(*)                            AS visits,
    SUM(hv.billing_amount)               AS total_billed
FROM workspace.gold.fact_hospital_visit hv
JOIN workspace.gold.dim_date d ON hv.admission_date = d.julian_day
GROUP BY d.year_num, d.quarter
ORDER BY d.year_num, d.quarter;


-- ---------------------------------------------------------------------
-- 11. Current bed occupancy from the ADT stream (new in this version --
--     the whole point of the streaming branch existing at all)
-- ---------------------------------------------------------------------
SELECT
    hospital_id,
    room_no,
    event_type,
    event_time,
    ROW_NUMBER() OVER (PARTITION BY hospital_id, room_no ORDER BY event_time DESC) AS rn
FROM workspace.silver.adt_events
QUALIFY rn = 1 AND event_type != 'discharge';
