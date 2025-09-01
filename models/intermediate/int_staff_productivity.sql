-- pour productivité par employés, impact du management
with o as (select * from {{ ref('int_orders_enriched') }}),
po as (select * from {{ ref('int_orders_per_order') }})  -- pour relier CA par commande

select
  o.staff_id,
  countif(o.is_completed = true)                              as orders_count,
  avg(case when o.lead_time_days is not null then o.lead_time_days end) as avg_lead_time_days,
  sum(case when o.is_completed = true then po.order_net else 0 end)      as revenue_managed
from o
left join po using(order_id)
group by 1