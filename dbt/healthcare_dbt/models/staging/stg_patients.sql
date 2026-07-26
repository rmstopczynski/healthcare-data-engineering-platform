select
    patient_id,
    trim(first_name) as first_name,
    trim(last_name) as last_name,
    dob,
    upper(trim(sex)) as sex,
    upper(trim(blood_type)) as blood_type
from {{ source('bronze', 'patients') }}
qualify row_number() over (partition by patient_id order by patient_id) = 1
