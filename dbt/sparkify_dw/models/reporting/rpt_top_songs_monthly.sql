-- Top 10 เพลงรายเดือน นับตาม (ศิลปิน, ชื่อเพลง) รวมทุก recording/ความยาวของเพลงเดียวกัน
with monthly as (
    select
        date_trunc('month', d.full_date)::date as metric_month,
        s.artist_name,
        s.song_title,
        count(*)                               as songplays,
        count(distinct u.user_id)              as listeners
    from {{ ref('fct_songplay') }} f
    join {{ ref('dim_date') }} d using (date_key)
    join {{ ref('dim_song') }} s using (song_key)
    join {{ ref('dim_user') }} u using (user_key)
    group by 1, 2, 3
),

ranked as (
    select
        *,
        rank() over (partition by metric_month order by songplays desc) as song_rank
    from monthly
)

select *
from ranked
where song_rank <= 10
