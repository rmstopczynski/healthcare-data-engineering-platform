{#
    Databricks/Spark SQL port note: no age()/extract(year from age(...))
    here. months_between() gives fractional months between two dates;
    dividing by 12 and flooring reproduces the same "completed years"
    age calculation.
#}

with ranked_insurance as (
    select
        pi.patient_id,
        ip.provider_name,
        ip.plan_type,
        row_number() over (partition by pi.patient_id order by ip.insurance_provider_id) as rn
    from {{ ref('stg_patient_insurance') }} pi
    join {{ ref('stg_insurance_providers') }} ip on pi.insurance_provider_id = ip.insurance_provider_id
),

patient_age as (
    select
        p.*,
        cast(floor(months_between(current_date(), p.dob) / 12) as int) as computed_age
    from {{ ref('stg_patients') }} p
)

select
    p.patient_id,
    p.first_name,
    p.last_name,
    p.dob,
    p.computed_age as age,
    case
        when p.computed_age < 18 then 'Under 18'
        when p.computed_age between 18 and 34 then '18-34'
        when p.computed_age between 35 and 54 then '35-54'
        when p.computed_age between 55 and 74 then '55-74'
        else '75+'
    end as age_group,
    p.sex,
    cast(null as string) as patient_phone_no,  -- not present in source
    p.blood_type,
    pri.provider_name  as primary_insur_prov,
    pri.plan_type      as primary_plan_type,
    sec.provider_name  as secondary_insur_prov,
    sec.plan_type      as secondary_plan_type
from patient_age p
left join ranked_insurance pri on p.patient_id = pri.patient_id and pri.rn = 1
left join ranked_insurance sec on p.patient_id = sec.patient_id and sec.rn = 2
