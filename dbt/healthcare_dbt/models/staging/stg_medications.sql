select
    medication_id,
    trim(medication_name) as medication_name,
    trim(pharma_company) as pharma_company,
    trim(category) as category,
    medication_cost
from {{ source('bronze', 'medications') }}
qualify row_number() over (partition by medication_id order by medication_id) = 1
