select
    procedure_id,
    trim(procedure_name) as procedure_name,
    trim(medical_category) as medical_category,
    procedure_charge
from {{ source('bronze', 'procedures') }}
qualify row_number() over (partition by procedure_id order by procedure_id) = 1
