-- KPI การฟังเพลงรายเดือน แยกตามแผน (free / paid) ณ ตอนที่เล่น ในรูป Long Format
-- หมายเหตุ: ผู้ใช้ที่เปลี่ยนแผนในเดือนเดียวกันถูกนับใน M002 ทั้งสองแผน ผลรวม M002 ข้ามแผนจึงมากกว่าระดับแพลตฟอร์มได้
with monthly_base as (
    select
        date_trunc('month', d.full_date)::date as metric_month,
        f.level,
        count(*)                               as songplays,
        count(distinct u.user_id)              as listening_users,
        count(distinct f.user_session_id)      as listening_sessions
    from {{ ref('fct_songplay') }} f
    join {{ ref('dim_date') }} d using (date_key)
    join {{ ref('dim_user') }} u using (user_key)
    group by 1, 2
),

metric_values as (
    select metric_month, level, 'M001'::varchar as metric_key, songplays::numeric(18, 4) as metric_value from monthly_base
    union all
    select metric_month, level, 'M002', listening_users::numeric(18, 4) from monthly_base
    union all
    select metric_month, level, 'M003', (songplays::numeric / nullif(listening_users, 0))::numeric(18, 4) from monthly_base
    union all
    select metric_month, level, 'M005', (songplays::numeric / nullif(listening_sessions, 0))::numeric(18, 4) from monthly_base
)

select
    v.metric_month,
    v.level,
    v.metric_key,
    d.metric_name,
    d.metric_label,
    d.unit,
    v.metric_value
from metric_values v
join {{ ref('metric_definition') }} d on v.metric_key = d.metric_key
