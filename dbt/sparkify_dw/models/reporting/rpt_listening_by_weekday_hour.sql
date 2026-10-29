-- การฟังเพลงตาม วันในสัปดาห์ x ชั่วโมง (UTC) สำหรับ heatmap "ช่วงไหนคนฟังมากที่สุด"
-- หารด้วยจำนวนวันนั้นที่มีในข้อมูล เพราะแต่ละวันในสัปดาห์มีจำนวนไม่เท่ากันในช่วง 64 วัน
with weekday_days as (
    select day_of_week, count(*) as days_in_data
    from {{ ref('dim_date') }}
    group by day_of_week
),

plays as (
    select
        d.day_of_week,
        d.day_name,
        t.time_key,
        t.hour_label,
        t.day_part,
        count(*) as songplays
    from {{ ref('fct_songplay') }} f
    join {{ ref('dim_date') }} d using (date_key)
    join {{ ref('dim_time') }} t using (time_key)
    group by 1, 2, 3, 4, 5
)

select
    p.day_of_week,
    p.day_name,
    p.time_key,
    p.hour_label,
    p.day_part,
    p.songplays,
    w.days_in_data,
    round(p.songplays::numeric / w.days_in_data, 2) as avg_songplays_per_day
from plays p
join weekday_days w using (day_of_week)
