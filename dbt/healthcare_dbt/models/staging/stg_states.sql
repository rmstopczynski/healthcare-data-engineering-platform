select
    state_id,
    trim(state_name) as state_name,
    upper(trim(state_abbr)) as state_abbr
from {{ source('bronze', 'states') }}
qualify row_number() over (partition by state_id order by state_id) = 1
