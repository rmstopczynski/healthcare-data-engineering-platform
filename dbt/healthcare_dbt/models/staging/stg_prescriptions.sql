select
    prescription_id,
    patient_id,
    doctor_id,
    medication_id,
    quantity,
    trim(dosage) as dosage,
    trim(frequency) as frequency,
    prescribed_date,
    refill_allowed,
    refill_count
from {{ source('bronze', 'prescriptions') }}
qualify row_number() over (partition by prescription_id order by prescription_id) = 1
