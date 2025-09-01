with sales as (
  select order_day, sum(revenue_net) as revenue_net, sum(units_sold) as units_sold
  from {{ ref('fct_sales_daily') }}
  group by 1
),
orders as (
  select order_timestamp as order_day, count(distinct order_id) as orders_count
  from {{ ref('int_orders_enriched') }}
  where is_completed = true
  group by 1
),
j as (
  select s.order_day, s.revenue_net, s.units_sold, o.orders_count
  from sales s
  left join orders o using(order_day)
)
select
  order_day,
  revenue_net,
  units_sold,
  orders_count,
  round(revenue_net / nullif(orders_count, 0), 2) as aov_daily,   -- à utiliser pour la courbe
  -- AOV global (ratio des totaux) calculé sur l’ensemble des lignes (après filtres)
  round(
    sum(revenue_net)  over () / nullif(sum(orders_count) over (), 0), 2
  ) as aov_global                                         -- à utiliser en scorecard (agrégation = MAX)
from j
