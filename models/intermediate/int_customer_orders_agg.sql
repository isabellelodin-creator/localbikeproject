with lines as (select * from {{ ref('int_order_items_enriched') }}),
orders as (select * from {{ ref('int_orders_enriched') }})

select
  l.customer_id,
  min(l.order_timestamp) as first_order_date,
  max(l.order_timestamp) as last_order_date,
  count(distinct l.order_id)  as orders_count,
  sum(l.net_amount) as total_revenue_net,
  safe_divide(sum(l.net_amount), nullif(count(distinct l.order_id),0)) as avg_basket_value,
  safe_divide(sum(l.quantity),   nullif(count(distinct l.order_id),0)) as avg_basket_items
from lines l
join orders o using(order_id)
where o.is_completed = true         -- garde les commandes terminées pour ces KPI
group by 1