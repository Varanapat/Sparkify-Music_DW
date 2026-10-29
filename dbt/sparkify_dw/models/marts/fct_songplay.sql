-- Grain: 1 แถวต่อการเล่นเพลง 1 ครั้ง (page = 'NextSong')
-- user_key เชื่อมกับ dim_user version ที่ใช้อยู่ตอนเล่น ผ่าน user_version (ไม่ใช้ช่วงเวลา เพราะ ts ละเอียดแค่วินาที)
select
    e.event_id                                     as songplay_id,
    e.event_ts,
    cast(to_char(e.event_ts, 'YYYYMMDD') as integer) as date_key,
    extract(hour from e.event_ts)::integer         as time_key,
    u.user_key,
    s.song_key,
    s.artist_key,
    -- degenerate dimensions: sessionId ซ้ำข้ามผู้ใช้ได้ จึงรวม user_id เข้าไปด้วย
    e.user_id || '-' || e.session_id               as user_session_id,
    e.item_in_session,
    e.level,
    e.length_seconds
from {{ ref('stg_events') }} e
join {{ ref('int_user_level_versions') }} v
  on e.event_id = v.event_id
join {{ ref('dim_user') }} u
  on v.user_id = u.user_id
 and v.user_version = u.user_version
join {{ ref('dim_song') }} s
  on e.artist_name = s.artist_name
 and e.song_title = s.song_title
 and e.length_seconds = s.duration_seconds
where e.is_songplay
