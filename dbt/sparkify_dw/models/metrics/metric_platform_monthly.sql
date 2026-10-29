-- KPI รายเดือนระดับทั้งแพลตฟอร์ม ในรูป Long Format (1 แถวต่อ เดือน x metric_key)
-- รูปแบบเดียวกับ metric_company_monthly ใน Knowledge Base (Requirement Gathering and KPI Layer)
-- ใช้ fct_user_event เป็นฐานเดียว เพราะมีทั้ง NextSong และ event อื่นที่ต้องใช้กับ Upgrade/Churn
with events as (
    select
        date_trunc('month', d.full_date)::date as metric_month,
        u.user_id,
        f.user_session_id,
        f.page,
        f.level,
        f.is_songplay
    from {{ ref('fct_user_event') }} f
    join {{ ref('dim_date') }} d using (date_key)
    join {{ ref('dim_user') }} u using (user_key)
),

monthly_base as (
    select
        metric_month,
        count(*) filter (where is_songplay)                                as songplays,
        count(*) filter (where is_songplay and level = 'paid')             as paid_songplays,
        count(distinct user_id) filter (where is_songplay)                 as listening_users,
        count(distinct user_session_id) filter (where is_songplay)         as listening_sessions,
        count(distinct user_id)                                            as active_users,
        count(distinct user_id) filter (where level = 'free')              as free_users,
        count(distinct user_id) filter (where page = 'Submit Upgrade')     as upgrade_users,
        count(distinct user_id) filter (where page = 'Cancellation Confirmation') as churned_users
    from events
    group by metric_month
),

metric_values as (
    select metric_month, 'M001'::varchar as metric_key, songplays::numeric(18, 4) as metric_value from monthly_base
    union all
    select metric_month, 'M002', listening_users::numeric(18, 4) from monthly_base
    union all
    select metric_month, 'M003', (songplays::numeric / nullif(listening_users, 0))::numeric(18, 4) from monthly_base
    union all
    select metric_month, 'M004', (paid_songplays::numeric / nullif(songplays, 0))::numeric(18, 4) from monthly_base
    union all
    select metric_month, 'M005', (songplays::numeric / nullif(listening_sessions, 0))::numeric(18, 4) from monthly_base
    union all
    select metric_month, 'M006', active_users::numeric(18, 4) from monthly_base
    union all
    select metric_month, 'M007', upgrade_users::numeric(18, 4) from monthly_base
    union all
    select metric_month, 'M008', (upgrade_users::numeric / nullif(free_users, 0))::numeric(18, 4) from monthly_base
    union all
    select metric_month, 'M009', churned_users::numeric(18, 4) from monthly_base
    union all
    select metric_month, 'M010', (churned_users::numeric / nullif(active_users, 0))::numeric(18, 4) from monthly_base
),

-- เดือนที่ข้อมูลไม่ครบ (ธ.ค. 2018 มีแค่ 2 วัน) ต้องไม่นำไปเทียบกับเดือนเต็ม
month_coverage as (
    select
        date_trunc('month', full_date)::date as metric_month,
        count(*)                             as days_in_data,
        count(*) < extract(day from (date_trunc('month', min(full_date)) + interval '1 month - 1 day'))
                                             as is_partial_month
    from {{ ref('dim_date') }}
    group by 1
)

select
    v.metric_month,
    c.days_in_data,
    c.is_partial_month,
    v.metric_key,
    d.metric_name,
    d.metric_label,
    d.description,
    d.formula,
    d.unit,
    v.metric_value
from metric_values v
join {{ ref('metric_definition') }} d on v.metric_key = d.metric_key
join month_coverage c on v.metric_month = c.metric_month
