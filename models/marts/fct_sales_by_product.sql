select
  cast(p.product_id as string) as product_id,
  p.product_name,
  cast(p.category_id as string) as category_id, 
  p.category_name,
  cast(p.brand_id as string) as brand_id,
  sum(f.revenue_net) as revenue_net,
  sum(f.units_sold) as units_sold
from {{ ref('fct_sales_daily') }} f
left join {{ ref('dim_product') }} p using(product_id)
group by 1,2,3,4,5