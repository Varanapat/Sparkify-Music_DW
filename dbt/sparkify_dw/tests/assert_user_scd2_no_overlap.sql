-- ช่วงเวลาของ version ต่าง ๆ ของผู้ใช้คนเดียวกันต้องไม่ทับกัน (แนวเดียวกับ assert_customer_scd2_no_overlap ใน lab)
select
    a.user_id,
    a.user_version,
    a.valid_from,
    a.valid_to,
    b.valid_from as next_valid_from
from {{ ref('dim_user') }} a
join {{ ref('dim_user') }} b
  on a.user_id = b.user_id
 and a.user_version < b.user_version
where a.valid_to > b.valid_from
