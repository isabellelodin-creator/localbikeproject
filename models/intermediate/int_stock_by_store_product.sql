-- pour surstock
with s as (select * from {{ ref('stg_stocks') }}),
ph as (select * from {{ ref('int_product_hierarchy') }})

select
  s.store_id,
  s.product_id,
  s.quantity as stock_qty,
  ph.brand_name,
  ph.category_name
from s
left join ph using(product_id)
