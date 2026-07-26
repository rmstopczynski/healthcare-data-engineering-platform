select
    insurance_provider_id,
    trim(provider_name) as provider_name,
    trim(plan_type) as plan_type,
    trim(phone_no) as phone_no
from {{ source('bronze', 'insurance_providers') }}
qualify row_number() over (partition by insurance_provider_id order by insurance_provider_id) = 1
