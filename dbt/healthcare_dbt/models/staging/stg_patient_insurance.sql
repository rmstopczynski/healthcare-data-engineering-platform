select distinct
    patient_id,
    insurance_provider_id
from {{ source('bronze', 'patient_insurance') }}
