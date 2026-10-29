-- มิติศิลปิน: 1 แถวต่อ 1 ชื่อศิลปิน รวมจาก event log และ song_data (conformed)
-- event log ไม่มี artist_id จึงใช้ชื่อเป็น business key; ข้อมูลจาก song_data เติมเมื่อชื่อตรงกัน
with names as (
    select artist_name from {{ ref('stg_events') }} where is_songplay
    union
    select artist_name from {{ ref('stg_songs') }}
),

catalog as (
    select distinct on (artist_name)
        artist_name,
        artist_id,
        artist_location,
        artist_latitude,
        artist_longitude
    from {{ ref('stg_songs') }}
    order by artist_name, artist_id
)

select
    md5(n.artist_name)  as artist_key,
    n.artist_name,
    c.artist_id         as msd_artist_id,
    c.artist_location,
    c.artist_latitude,
    c.artist_longitude,
    c.artist_id is not null as is_in_song_catalog
from names n
left join catalog c on n.artist_name = c.artist_name
