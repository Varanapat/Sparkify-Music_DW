-- หลังแก้ encoding แล้ว ต้องไม่เหลือรูปแบบ mojibake: 'Ã' หรือ 'Â' ตามด้วยอักขระช่วง U+0080-U+00BF
-- (ชื่อจริงที่มี Â/Ã ขึ้นต้นคำ เช่น 'Âme' จะตามด้วยตัวอักษรปกติ จึงไม่ถูกนับ)
select 'artist' as field, artist_name as value
from {{ ref('dim_artist') }}
where artist_name ~ '[ÃÂ][\u0080-¿]'

union all

select 'song', song_title
from {{ ref('dim_song') }}
where song_title ~ '[ÃÂ][\u0080-¿]'
