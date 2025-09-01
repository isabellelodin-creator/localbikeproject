-- pour définir top produits / marques / catégories
with p as (select * from {{ ref('stg_products') }}),
b as (select * from {{ ref('stg_brands') }}),
c as (select * from {{ ref('stg_categories') }})

select
  p.product_id,
  p.product_name,
  b.brand_id,
  b.brand_name,
  c.category_id,
  c.category_name,
  p.model_year,
  p.product_price
from p
left join b using(brand_id)
left join c using(category_id)