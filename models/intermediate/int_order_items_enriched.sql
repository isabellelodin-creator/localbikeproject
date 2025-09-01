-- pour CA, panier moyen (nb articles & valeur), top produits, kpis par magasins et produit
with oi as (select * from {{ ref('stg_order_items') }}),
o  as (select * from {{ ref('stg_orders') }}),
ph as (select * from {{ ref('int_product_hierarchy') }})

select
  o.order_id,
  o.order_timestamp,
  o.store_id,
  o.customer_id,
  o.staff_id,
  oi.item_id,
  oi.product_id,
  ph.product_name,
  ph.brand_id,
  ph.brand_name,
  ph.category_id,
  ph.category_name,
  oi.quantity,
  oi.list_price,
  oi.discount,
  (oi.list_price * oi.quantity)                         as gross_amount,
  (oi.list_price * oi.discount * oi.quantity)           as discount_amount,
  (oi.list_price * (1 - oi.discount) * oi.quantity)     as net_amount
from oi
join o  using(order_id)
join ph using(product_id)
