-- Grain: 1 แถวต่อ 1 event ทุกประเภท (ทุก page) ใช้วิเคราะห์ funnel, Upgrade/Downgrade และ churn
-- event ของ Guest / Logged Out ไม่มีผู้ใช้ -> user_key = '-1' (Unknown member ใน dim_user)
select
    e.event_id,
    e.event_ts,
    cast(to_char(e.event_ts, 'YYYYMMDD') as integer) as date_key,
    extract(hour from e.event_ts)::integer         as time_key,
    coalesce(u.user_key, '-1')                     as user_key,
    e.user_id || '-' || e.session_id               as user_session_id,
    e.item_in_session,
    e.page,
    e.auth,
    e.level,
    e.http_method,
    e.http_status,
    e.is_songplay
from {{ ref('stg_events') }} e
left join {{ ref('int_user_level_versions') }} v
  on e.event_id = v.event_id
left join {{ ref('dim_user') }} u
  on v.user_id = u.user_id
 and v.user_version = u.user_version
