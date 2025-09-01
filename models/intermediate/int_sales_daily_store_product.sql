-- pour CA par magasin/produit/jour, top produits, comparatifs stores
with lines as (
  select * from {{ ref('int_order_items_enriched') }}
)
select
  order_timestamp as order_day,
  store_id,
  product_id,
  sum(quantity) as units_sold,
  sum(gross_amount) as revenue_gross,
  sum(discount_amount)as discount_total,
  sum(net_amount) as revenue_net
from lines
group by 1,2,3