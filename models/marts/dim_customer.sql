select
  c.customer_id,
  c.first_name,
  c.last_name,
  c.email,
  c.phone,
  c.street,
  c.city,
  c.state,
  c.zip_code,
  agg.first_order_date,
  agg.last_order_date,
  agg.orders_count,
  round(agg.total_revenue_net, 2)   as total_revenue_net,
  round(agg.avg_basket_value, 2)    as avg_basket_value,
  round(agg.avg_basket_items, 2)    as avg_basket_items
from {{ ref('stg_customers') }} c
left join {{ ref('int_customer_orders_agg') }} agg
  using (customer_id)