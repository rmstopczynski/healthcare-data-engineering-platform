{#
    Databricks/Spark SQL port note: Postgres' generate_series() and
    to_char() don't exist here. sequence() produces the same one-row-
    per-day array (exploded below), and date_format()'s pattern letters
    (yyyy/MM/dd/EEEE/QQQ...) replace to_char()'s (YYYY/Month/Day...).
    Range covers 2010-01-01 through 2029-12-31 (~20 years) -- adjust
    if your data falls outside this window.
#}

with days as (
    select explode(sequence(
        to_date('2010-01-01'),
        to_date('2029-12-31'),
        interval 1 day
    )) as actual_dt
)

select
    cast(date_format(actual_dt, 'yyyyMMdd') as int) as julian_day,
    actual_dt,
    trim(date_format(actual_dt, 'EEEE'))            as day_name,
    date_format(actual_dt, 'E')                     as day_abbrev,
    dayofyear(actual_dt)                            as day_in_year,
    dayofmonth(actual_dt)                            as day_in_month,
    ((dayofweek(actual_dt) + 5) % 7) + 1              as day_in_week,  -- ISO: Mon=1..Sun=7
    trim(date_format(actual_dt, 'MMMM'))              as month_name,
    date_format(actual_dt, 'MMM')                     as month_abbrev,
    month(actual_dt)                                  as month_num,
    date_format(actual_dt, 'yyyy')                    as year_name,
    year(actual_dt)                                    as year_num,
    quarter(actual_dt)                                  as quarter
from days
