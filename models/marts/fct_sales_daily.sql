with daily as (
  select
    order_day,
    store_id,
    product_id,
    sum(units_sold)        as units_sold,
    sum(revenue_gross)     as revenue_gross,
    sum(discount_total)    as discount_total,
    sum(revenue_net)       as revenue_net
  from {{ ref('int_sales_daily_store_product') }}
  group by 1,2,3
)
select
  order_day,
  store_id,
  product_id,
  units_sold,
  round(revenue_gross,   2) as revenue_gross,
  round(discount_total,  2) as discount_total,
  round(revenue_net,     2) as revenue_net
from daily