--pour nombre de commandes, taux annulation, délai livraison
with o as (select * from {{ ref('stg_orders') }}),
m as (select * from {{ ref('status_mapping') }}) --seed

select
  o.order_id,
  o.customer_id,
  o.store_id,
  o.staff_id,
  o.order_timestamp,
  o.required_timestamp,
  o.shipped_timestamp,
  cast(o.order_status as int64) as order_status_code,
  m.status_label,
  m.is_completed,
  m.is_cancelled,
  case
    when o.shipped_timestamp is not null and o.order_timestamp is not null
      then date_diff(o.shipped_timestamp, o.order_timestamp, day)
    else null
  end as lead_time_days
from o
left join m
  on cast(o.order_status as INT) = m.status_code
