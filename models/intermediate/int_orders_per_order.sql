-- pour calculer le panier moyen (valeur & nb articles)
with lines as (
  select * from {{ ref('int_order_items_enriched') }}
)
select
  order_id,
  sum(quantity)      as items_count,
  sum(gross_amount)  as order_gross,
  sum(discount_amount) as order_discount,
  sum(net_amount)    as order_net
from lines
group by 1