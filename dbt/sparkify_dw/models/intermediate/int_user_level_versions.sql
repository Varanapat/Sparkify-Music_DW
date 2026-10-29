-- กำหนด SCD2 version ของผู้ใช้ให้ทุก event: version เพิ่ม 1 ทุกครั้งที่ level เปลี่ยน (free <-> paid)
-- ไม่ใช้ dbt snapshot เพราะ snapshot เก็บได้แค่สถานะ ณ ตอนรัน แต่ที่นี่ต้องสร้างประวัติย้อนหลังจาก event
-- เรียงด้วย item_in_session ต่อจาก event_ts เพราะ ts ละเอียดแค่วินาที:
-- มี 4 กรณีที่ Submit Upgrade/Downgrade (level เดิม) กับ event ถัดไป (level ใหม่) เกิดในวินาทีเดียวกัน
with user_events as (
    select
        event_id,
        user_id,
        event_ts,
        item_in_session,
        level
    from {{ ref('stg_events') }}
    where user_id is not null
),

flagged as (
    select
        *,
        case
            when level is distinct from lag(level) over w then 1
            else 0
        end as is_level_change
    from user_events
    window w as (partition by user_id order by event_ts, item_in_session, event_id)
)

select
    event_id,
    user_id,
    event_ts,
    level,
    sum(is_level_change) over (
        partition by user_id
        order by event_ts, item_in_session, event_id
        rows between unbounded preceding and current row
    )::integer as user_version
from flagged
