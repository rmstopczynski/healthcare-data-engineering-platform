select
    hv.visit_id,
    cast(date_format(hv.admission_date, 'yyyyMMdd') as int) as admission_date,
    cast(date_format(hv.discharge_date, 'yyyyMMdd') as int) as discharge_date,
    hv.patient_id,
    hv.doctor_id,
    hosp.hospital_name  as hospital,
    ip.provider_name     as insurance_provider,
    hv.room_no,
    hv.admission_type,
    hv.diagnosis,
    hv.procedure_id,
    proc.procedure_charge,
    cast(null as numeric) as room_charge,  -- not present in source
    cast(null as numeric) as misc_charge,  -- not present in source
    coalesce(proc.procedure_charge, 0) as billing_amount,
    datediff(hv.discharge_date, hv.admission_date) as length_of_stay
from {{ ref('stg_hospital_visits') }} hv
left join {{ ref('stg_hospitals') }} hosp on hv.hospital_id = hosp.hospital_id
left join {{ ref('stg_insurance_providers') }} ip on hv.insurance_provider_id = ip.insurance_provider_id
left join {{ ref('stg_procedures') }} proc on hv.procedure_id = proc.procedure_id
