with daily as (
  select * from {{ ref('fct_customer_daily') }}
),
cust as (
  select
    c.customer_id,
    c.first_name,
    c.last_name,
    concat(c.first_name, ' ', c.last_name) as customer_name,
    c.email,
    c.phone,
    c.city,
    c.state
  from {{ ref('stg_customers') }} c
),
agg as (
  select
    d.customer_id,
    cust.customer_name,
    cust.email,
    cust.phone,
    cust.city,
    cust.state,
    min(d.order_day)                          as first_order_date,
    max(d.order_day)                          as last_order_date,
    sum(d.orders_count)                       as orders_count,
    sum(d.units_sold)                         as units_sold,
    round(sum(d.revenue_net), 2)              as revenue_net,
    case when sum(d.orders_count) > 1 then 1 else 0 end as is_recurrent
  from daily d
  left join cust on d.customer_id = cust.customer_id
  group by 1,2,3,4,5,6
)

select
  *,
  row_number() over (order by revenue_net desc) as customer_rank
from agg