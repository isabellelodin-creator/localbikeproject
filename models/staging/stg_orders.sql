select
  order_id,
  customer_id,
  order_status,
  safe_cast(order_date  as timestamp) as order_timestamp,
  safe_cast(required_date as timestamp) as required_timestamp,
  safe_cast(shipped_date  as timestamp) as shipped_timestamp,
  store_id,
  staff_id
from {{ source('localbike_src','orders') }}