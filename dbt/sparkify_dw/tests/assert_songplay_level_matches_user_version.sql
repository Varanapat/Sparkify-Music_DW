-- ตรวจว่า fact เชื่อมกับ SCD2 version ถูกต้อง: level ตอนเล่นเพลงต้องเท่ากับ level ของ dim_user version นั้น
select
    f.songplay_id,
    f.level as event_level,
    u.level as dim_level
from {{ ref('fct_songplay') }} f
join {{ ref('dim_user') }} u on f.user_key = u.user_key
where f.level <> u.level
