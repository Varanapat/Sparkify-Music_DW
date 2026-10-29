-- Top 10 ศิลปินรายเดือน ตามจำนวนครั้งที่เล่น พร้อมจำนวนผู้ฟังไม่ซ้ำ
with monthly as (
    select
        date_trunc('month', d.full_date)::date as metric_month,
        a.artist_name,
        count(*)                               as songplays,
        count(distinct u.user_id)              as listeners
    from {{ ref('fct_songplay') }} f
    join {{ ref('dim_date') }} d using (date_key)
    join {{ ref('dim_artist') }} a using (artist_key)
    join {{ ref('dim_user') }} u using (user_key)
    group by 1, 2
),

ranked as (
    select
        *,
        rank() over (partition by metric_month order by songplays desc) as artist_rank
    from monthly
)

select *
from ranked
where artist_rank <= 10
