-- SQL ของแต่ละการ์ดใน Metabase dashboard "Sparkify Music KPIs" (database: Sparkify DW -> sparkify_dw)
-- ดึงจาก metric / reporting models เท่านั้น ไม่เขียนสูตร KPI ซ้ำใน dashboard
-- เก็บไว้ใน repo เพื่อสร้าง dashboard ใหม่ได้ เพราะเนื้อหาใน Metabase อยู่ใน volume ไม่ได้อยู่ใน git

-- Q1. Monthly KPIs (full months) | Table
select
    metric_key,
    metric_label,
    unit,
    max(metric_value) filter (where metric_month = date '2018-10-01') as "Oct 2018",
    max(metric_value) filter (where metric_month = date '2018-11-01') as "Nov 2018"
from dbt_metrics.metric_platform_monthly
where not is_partial_month
group by metric_key, metric_label, unit
order by metric_key;

-- Q2. Daily songplays by level | Line (x = full_date, series = level)
select d.full_date, f.level, count(*) as songplays
from dbt_marts.fct_songplay f
join dbt_marts.dim_date d using (date_key)
where d.full_date < date '2018-12-01'
group by 1, 2
order by 1, 2;

-- Q3. Songplays per user: free vs paid | Bar (x = month, series = level)
select to_char(metric_month, 'YYYY-MM') as month, level, metric_value as songplays_per_user
from dbt_metrics.metric_level_monthly
where metric_key = 'M003' and metric_month < date '2018-12-01'
order by 1, 2;

-- Q4. Upgrades and churn by month | Bar (x = month, series = metric)
select to_char(metric_month, 'YYYY-MM') as month, metric_label, metric_value
from dbt_metrics.metric_platform_monthly
where metric_key in ('M007', 'M009') and not is_partial_month
order by 1, 2;

-- Q5. Top 10 artists - Nov 2018 | Row
select artist_name, songplays
from dbt_reporting.rpt_top_artists_monthly
where metric_month = date '2018-11-01'
order by songplays desc;

-- Q6. Top 10 songs - Nov 2018 | Table
select song_rank, artist_name, song_title, songplays, listeners
from dbt_reporting.rpt_top_songs_monthly
where metric_month = date '2018-11-01'
order by song_rank;

-- Q7. Avg songplays per day by hour (UTC) | Line (x = hour, series = day_name)
select time_key as hour_utc, day_name, avg_songplays_per_day
from dbt_reporting.rpt_listening_by_weekday_hour
order by day_of_week, time_key;

-- Q8. Churn signals: churned vs retained users | Table
select
    case when is_churned then 'Churned' else 'Retained' end as user_group,
    count(*)                                as users,
    round(avg(songs_per_session), 1)        as songs_per_session,
    round(avg(thumbs_up_per_100), 2)        as thumbs_up_per_100,
    round(avg(thumbs_down_per_100), 2)      as thumbs_down_per_100,
    round(avg(roll_advert_per_100), 2)      as adverts_per_100,
    round(avg(add_friend_per_100), 2)       as add_friend_per_100,
    round(avg(errors_per_100), 2)           as errors_per_100,
    round(avg(active_days), 1)              as active_days
from dbt_reporting.rpt_user_churn_signals
group by 1
order by 1;
