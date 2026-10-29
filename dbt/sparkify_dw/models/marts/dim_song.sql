-- มิติเพลง: 1 แถวต่อ 1 recording = (ศิลปิน, ชื่อเพลง, ความยาว)
-- ใส่ความยาวใน key เพราะ 514 คู่ (ศิลปิน, ชื่อเพลง) มีหลายความยาว เช่น album version กับ single
-- ถ้าต้องการนับตามชื่อเพลง ให้ group by artist_name, song_title
with recordings as (
    select artist_name, song_title, length_seconds as duration_seconds
    from {{ ref('stg_events') }}
    where is_songplay
    union
    select artist_name, song_title, duration_seconds
    from {{ ref('stg_songs') }}
)

select
    md5(r.artist_name || '|' || r.song_title || '|' || r.duration_seconds) as song_key,
    r.song_title,
    md5(r.artist_name)  as artist_key,
    r.artist_name,
    r.duration_seconds,
    s.song_id           as msd_song_id,
    s.release_year,
    s.song_id is not null as is_in_song_catalog
from recordings r
left join {{ ref('stg_songs') }} s
  on r.artist_name = s.artist_name
 and r.song_title = s.song_title
 and r.duration_seconds = s.duration_seconds
