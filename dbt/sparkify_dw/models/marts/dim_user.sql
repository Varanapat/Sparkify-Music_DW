-- มิติผู้ใช้แบบ SCD Type 2: 1 แถวต่อ 1 ช่วงที่ผู้ใช้อยู่ใน level เดียวกัน
-- level = Type 2 (เก็บประวัติ) / ชื่อ เพศ location วันลงทะเบียน คงที่ต่อผู้ใช้ (ตรวจแล้ว) / สถานะ churn = Type 1
with versions as (
    select
        user_id,
        user_version,
        min(level)    as level,
        min(event_ts) as valid_from
    from {{ ref('int_user_level_versions') }}
    group by user_id, user_version
),

profile as (
    select distinct on (user_id)
        user_id,
        first_name,
        last_name,
        gender,
        location,
        location_state,
        registered_at
    from {{ ref('stg_events') }}
    where user_id is not null
    order by user_id, event_ts desc, event_id desc
),

churn as (
    select
        user_id,
        min(event_ts) as churned_at
    from {{ ref('stg_events') }}
    where page = 'Cancellation Confirmation'
    group by user_id
),

scd as (
    select
        v.*,
        coalesce(
            lead(v.valid_from) over (partition by v.user_id order by v.user_version),
            '9999-12-31'::timestamp
        ) as valid_to,
        v.user_version = max(v.user_version) over (partition by v.user_id) as is_current
    from versions v
)

select
    md5(s.user_id || '|' || s.user_version) as user_key,
    s.user_id,
    s.user_version,
    p.first_name,
    p.last_name,
    p.gender,
    p.location,
    p.location_state,
    p.registered_at,
    s.level,
    s.valid_from,
    s.valid_to,
    s.is_current,
    c.churned_at is not null as is_churned,
    c.churned_at
from scd s
join profile p on s.user_id = p.user_id
left join churn c on s.user_id = c.user_id

union all

-- Unknown member สำหรับ event ของ Guest / Logged Out ใน fct_user_event
select
    '-1', null, 0, null, null, null, null, null, null, null,
    '1900-01-01'::timestamp, '9999-12-31'::timestamp, true, false, null
