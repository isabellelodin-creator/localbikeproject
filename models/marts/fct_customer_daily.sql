with orders as (
  select
    o.order_id,
    o.customer_id,
    cast(o.order_timestamp as date) as order_day
  from {{ ref('stg_orders') }} o
  where o.order_status = 4 and o.order_timestamp is not null
),
items as (
  select
    oi.order_id,
    oi.quantity,
    oi.list_price,
    oi.discount,
    (oi.list_price * (1 - oi.discount) * oi.quantity) as line_amount
  from {{ ref('stg_order_items') }} oi
)
select
  o.order_day,
  o.customer_id,
  count(distinct o.order_id)        as orders_count,
  sum(oi.quantity)                  as units_sold,
  round(sum(oi.line_amount), 2)     as revenue_net
from orders o
join items oi using(order_id)
group by 1,2