select
    city_id,
    trim(city_name) as city_name,
    state_id
from {{ source('bronze', 'cities') }}
qualify row_number() over (partition by city_id order by city_id) = 1
