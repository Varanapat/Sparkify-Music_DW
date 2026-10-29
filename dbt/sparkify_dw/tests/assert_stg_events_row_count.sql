-- staging ต้องไม่ทำแถวหายหรือเพิ่ม: จำนวนแถวต้องเท่ากับ raw
with counts as (
    select
        (select count(*) from {{ source('raw', 'sparkify_events') }}) as raw_rows,
        (select count(*) from {{ ref('stg_events') }})                as stg_rows
)

select *
from counts
where raw_rows <> stg_rows
