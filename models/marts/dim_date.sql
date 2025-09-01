select
  date_day as date,
  extract(year from date_day) as year,
  extract(quarter from date_day) as quarter,
  extract(month from date_day) as month,
  format_date('%Y-%m', date_day) as ym,
  extract(week from date_day) as week,
  extract(dayofweek from date_day) as weekday,
  case when extract(dayofweek from date_day) in (1,7) then true else false end as is_weekend
from {{ ref('int_calendar') }}