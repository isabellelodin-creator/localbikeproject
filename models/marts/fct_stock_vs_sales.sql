with stock as (
  -- stock actuel agrégé sur tous les magasins
  select
    product_id,
    sum(quantity) as stock_qty
  from {{ ref('stg_stocks') }}
  group by 1
),
sales as (
  -- ventes sur la fenêtre glissante (par défaut 30 jours)
  select
    product_id,
    sum(units_sold)   as units_30d,
    sum(revenue_net)  as revenue_30d
  from {{ ref('fct_sales_daily') }}
  where cast(order_day as date) >= date_sub(current_date(), interval 30 day)
  group by 1
),
prod as (
  select * from {{ ref('dim_product') }}
)

select
  p.product_id,
  p.product_name,
  p.category_id,
  p.category_name,
  p.brand_id,
  p.brand_name,
  coalesce(sales.units_30d, 0)        as units_30d,
  round(coalesce(sales.revenue_30d,0),2) as revenue_30d,
  coalesce(stock.stock_qty, 0)         as stock_qty,
  -- rotation = ventes 30j / stock actuel
  round(coalesce(sales.units_30d,0) / nullif(coalesce(stock.stock_qty,0),0), 3) as turnover_ratio_30d,
  -- couverture de stock (jours) = stock / (ventes moyennes par jour)
  case
    when coalesce(sales.units_30d,0) = 0 then null
    else round(coalesce(stock.stock_qty,0) / (sales.units_30d /30.0), 1)
  end as stock_cover_days
from prod p
left join sales on p.product_id = sales.product_id
left join stock on p.product_id = stock.product_id