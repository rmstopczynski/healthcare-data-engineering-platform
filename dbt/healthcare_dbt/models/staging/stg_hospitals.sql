select
    hospital_id,
    trim(hospital_name) as hospital_name,
    address_id,
    trim(hospital_phone_no) as hospital_phone_no
from {{ source('bronze', 'hospitals') }}
qualify row_number() over (partition by hospital_id order by hospital_id) = 1
