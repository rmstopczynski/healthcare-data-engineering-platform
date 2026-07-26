select
    address_id,
    trim(street_address) as street_address,
    city_id,
    trim(zip) as zip
from {{ source('bronze', 'addresses') }}
qualify row_number() over (partition by address_id order by address_id) = 1
