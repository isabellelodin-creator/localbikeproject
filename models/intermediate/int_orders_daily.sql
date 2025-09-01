select
  order_timestamp as order_day,
  count(distinct order_id) as orders_count
from {{ ref('int_orders_enriched') }}
where is_completed = true
group by 1