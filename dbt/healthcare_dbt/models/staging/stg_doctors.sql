select
    doctor_id,
    trim(first_name) as first_name,
    trim(last_name) as last_name,
    hospital_affi,
    trim(specialty) as specialty,
    trim(doc_phone_no) as doc_phone_no
from {{ source('bronze', 'doctors') }}
qualify row_number() over (partition by doctor_id order by doctor_id) = 1
