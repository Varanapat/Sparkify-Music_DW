-- 1 แถวต่อ 1 event ในไฟล์ต้นทาง: แตก jsonb เป็นคอลัมน์ ปรับชนิดข้อมูล และจัดการค่าว่าง
-- ใน JSON ฟิลด์ที่ไม่มีค่าคือ key ที่หายไป (->> ได้ null อยู่แล้ว) ยกเว้น userId ของ Guest/Logged Out เป็น ''
with source as (
    select line_number, payload as p
    from {{ source('raw', 'sparkify_events') }}
)

select
    line_number                                               as event_id,
    (p->>'ts')::bigint                                        as event_ts_ms,
    to_timestamp((p->>'ts')::bigint / 1000.0) at time zone 'UTC' as event_ts,
    nullif(trim(p->>'userId'), '')                            as user_id,
    (p->>'sessionId')::integer                                as session_id,
    (p->>'itemInSession')::integer                            as item_in_session,
    p->>'page'                                                as page,
    p->>'page' = 'NextSong'                                   as is_songplay,
    p->>'auth'                                                as auth,
    p->>'method'                                              as http_method,
    (p->>'status')::integer                                   as http_status,
    p->>'level'                                               as level,
    nullif(trim(p->>'firstName'), '')                         as first_name,
    nullif(trim(p->>'lastName'), '')                          as last_name,
    p->>'gender'                                              as gender,
    nullif(trim(p->>'location'), '')                          as location,
    -- "Boston-Cambridge-Newton, MA-NH" -> state = ส่วนหลัง comma สุดท้าย
    nullif(trim(split_part(p->>'location', ', ', 2)), '')     as location_state,
    nullif(trim(p->>'userAgent'), '')                         as user_agent,
    to_timestamp((p->>'registration')::bigint / 1000.0) at time zone 'UTC' as registered_at,
    -- 129 แถวมีช่องว่างหัว/ท้ายชื่อ ตัดออกเพื่อให้ key ของเพลง/ศิลปินตรงกัน
    nullif(trim(p->>'artist'), '')                            as artist_name,
    nullif(trim(p->>'song'), '')                              as song_title,
    (p->>'length')::numeric(10, 5)                            as length_seconds
from source
