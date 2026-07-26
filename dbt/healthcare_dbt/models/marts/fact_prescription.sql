select
    rx.prescription_id,
    rx.patient_id,
    rx.doctor_id,
    rx.medication_id,
    cast(date_format(rx.prescribed_date, 'yyyyMMdd') as int) as prescribed_date,
    rx.quantity,
    rx.dosage,
    rx.frequency,
    m.medication_cost,
    rx.quantity * m.medication_cost as total_cost,
    rx.refill_allowed,
    rx.refill_count
from {{ ref('stg_prescriptions') }} rx
left join {{ ref('stg_medications') }} m on rx.medication_id = m.medication_id
