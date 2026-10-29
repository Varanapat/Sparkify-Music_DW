-- 1 แถวต่อ 1 เพลงจาก Million Song Dataset subset
with source as (
    select payload as p
    from {{ source('raw', 'songs') }}
)

select
    p->>'song_id'                                    as song_id,
    nullif(trim(p->>'title'), '')                    as song_title,
    p->>'artist_id'                                  as artist_id,
    nullif(trim(p->>'artist_name'), '')              as artist_name,
    nullif(trim(p->>'artist_location'), '')          as artist_location,
    (p->>'artist_latitude')::numeric(9, 5)           as artist_latitude,
    (p->>'artist_longitude')::numeric(9, 5)          as artist_longitude,
    (p->>'duration')::numeric(10, 5)                 as duration_seconds,
    -- ใน Million Song Dataset ปี 0 = ไม่ทราบปี
    nullif((p->>'year')::integer, 0)                 as release_year
from source
