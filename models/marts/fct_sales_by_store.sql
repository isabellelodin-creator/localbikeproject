select
  order_day,
  store_id,
  sum(units_sold)              as units_sold,
  round(sum(revenue_gross), 2) as revenue_gross,
  round(sum(discount_total),2) as discount_total,
  round(sum(revenue_net),  2)  as revenue_net
from {{ ref('fct_sales_daily') }}
group by 1,2