select
  brand_id,
  brand_name
from {{ source('localbike_src','brands') }}