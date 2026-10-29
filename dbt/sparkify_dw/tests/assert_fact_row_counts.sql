-- fact ต้องไม่ทำแถวหายหรือเพิ่มจากการ join กับ dimension
with counts as (
    select
        (select count(*) from {{ ref('stg_events') }} where is_songplay) as expected_songplays,
        (select count(*) from {{ ref('fct_songplay') }})                 as fct_songplays,
        (select count(*) from {{ ref('stg_events') }})                   as expected_events,
        (select count(*) from {{ ref('fct_user_event') }})               as fct_events
)

select *
from counts
where expected_songplays <> fct_songplays
   or expected_events <> fct_events
