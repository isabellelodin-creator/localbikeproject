with bounds as (
  select
    date(min(order_timestamp)) as min_d,
    date(max(order_timestamp)) as max_d
  from {{ ref('stg_orders') }}
),
dates as (
  select day as date_day
  from bounds, unnest(generate_date_array(min_d, max_d)) as day
)
select
  date_day
from dates