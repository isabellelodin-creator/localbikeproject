with src as (
  select * from {{ source('localbike_src','staffs') }}
)
select
  staff_id,
  first_name,
  last_name,
  email,
  phone,
  active,
  store_id,
case
    when upper(trim(manager_id)) in ('', 'NULL') then null
    else safe_cast(manager_id as INT)
  end as manager_id

from src