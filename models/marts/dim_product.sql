select
  product_id,
  product_name,
  brand_id,
  brand_name,
  category_id,
  category_name,
  model_year,
  product_price
from {{ ref('int_product_hierarchy') }}