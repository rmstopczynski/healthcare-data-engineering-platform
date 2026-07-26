select
    visit_id,
    admission_date,
    discharge_date,
    patient_id,
    doctor_id,
    hospital_id,
    insurance_provider_id,
    trim(room_no) as room_no,
    trim(admission_type) as admission_type,
    procedure_id,
    trim(diagnosis) as diagnosis
from {{ source('bronze', 'hospital_visits') }}
qualify row_number() over (partition by visit_id order by visit_id) = 1
