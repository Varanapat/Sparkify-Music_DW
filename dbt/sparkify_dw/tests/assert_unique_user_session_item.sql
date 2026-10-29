-- ผู้ใช้ที่ login แล้ว: (user_id, session_id, item_in_session) ต้องไม่ซ้ำ
-- ใช้เป็น natural key ของ event ใน fact ได้ (ผู้ใช้ที่ไม่ login ใช้ sessionId ซ้ำกัน จึงไม่นับ)
select
    user_id,
    session_id,
    item_in_session,
    count(*) as n
from {{ ref('stg_events') }}
where user_id is not null
group by 1, 2, 3
having count(*) > 1
