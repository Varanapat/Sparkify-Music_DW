-- พฤติกรรมรายผู้ใช้ เทียบกลุ่มที่ยกเลิกบริการ (churned) กับกลุ่มที่ยังอยู่
-- ใช้อัตราต่อ 100 songplays แทนจำนวนดิบ เพราะผู้ใช้ที่ churn มีช่วงเวลาในข้อมูลสั้นกว่า
with user_events as (
    select
        u.user_id,
        f.page,
        f.is_songplay,
        f.user_session_id,
        f.event_ts
    from {{ ref('fct_user_event') }} f
    join {{ ref('dim_user') }} u using (user_key)
    where u.user_id is not null
),

per_user as (
    select
        user_id,
        count(*) filter (where is_songplay)                       as songplays,
        count(distinct user_session_id)                           as sessions,
        count(distinct event_ts::date)                            as active_days,
        count(*) filter (where page = 'Thumbs Up')                as thumbs_up,
        count(*) filter (where page = 'Thumbs Down')              as thumbs_down,
        count(*) filter (where page = 'Add to Playlist')          as add_to_playlist,
        count(*) filter (where page = 'Add Friend')               as add_friend,
        count(*) filter (where page = 'Roll Advert')              as roll_advert,
        count(*) filter (where page = 'Error')                    as errors,
        count(*) filter (where page = 'Help')                     as help_visits,
        count(*) filter (where page = 'Downgrade')                as downgrade_visits
    from user_events
    group by user_id
),

-- ไม่ตั้งชื่อ CTE ว่า current_user เพราะเป็นคำสงวนของ PostgreSQL
user_current as (
    select user_id, level as current_level, is_churned, churned_at, registered_at
    from {{ ref('dim_user') }}
    where is_current and user_id is not null
)

select
    p.user_id,
    c.current_level,
    c.is_churned,
    c.churned_at,
    p.songplays,
    p.sessions,
    p.active_days,
    round(p.songplays::numeric / nullif(p.sessions, 0), 2)              as songs_per_session,
    round(100.0 * p.thumbs_up / nullif(p.songplays, 0), 2)              as thumbs_up_per_100,
    round(100.0 * p.thumbs_down / nullif(p.songplays, 0), 2)            as thumbs_down_per_100,
    round(100.0 * p.add_to_playlist / nullif(p.songplays, 0), 2)        as add_to_playlist_per_100,
    round(100.0 * p.add_friend / nullif(p.songplays, 0), 2)             as add_friend_per_100,
    round(100.0 * p.roll_advert / nullif(p.songplays, 0), 2)            as roll_advert_per_100,
    round(100.0 * p.errors / nullif(p.songplays, 0), 2)                 as errors_per_100,
    round(100.0 * p.help_visits / nullif(p.songplays, 0), 2)            as help_per_100,
    round(100.0 * p.downgrade_visits / nullif(p.songplays, 0), 2)       as downgrade_visits_per_100
from per_user p
join user_current c using (user_id)
