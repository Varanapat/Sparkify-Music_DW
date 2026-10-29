-- ตัวเลข metric ต้องกระทบยอดกับ fact/dim ได้:
-- M001 รวมทุกเดือน = จำนวนแถว fct_songplay / M009 รวมทุกเดือน = ผู้ใช้ที่ churn ใน dim_user (churn ได้ครั้งเดียวต่อคน)
-- และ M001 แยกแผน free + paid = M001 ระดับแพลตฟอร์มในแต่ละเดือน
with platform as (
    select
        sum(metric_value) filter (where metric_key = 'M001') as songplays,
        sum(metric_value) filter (where metric_key = 'M009') as churned_users
    from {{ ref('metric_platform_monthly') }}
),

expected as (
    select
        (select count(*) from {{ ref('fct_songplay') }}) as songplays,
        (select count(*) from {{ ref('dim_user') }} where is_current and is_churned) as churned_users
),

level_vs_platform as (
    select l.metric_month
    from (
        select metric_month, sum(metric_value) as songplays
        from {{ ref('metric_level_monthly') }}
        where metric_key = 'M001'
        group by metric_month
    ) l
    join {{ ref('metric_platform_monthly') }} p
      on p.metric_month = l.metric_month
     and p.metric_key = 'M001'
    where p.metric_value <> l.songplays
)

select 'M001 total mismatch' as problem from platform, expected where platform.songplays <> expected.songplays
union all
select 'M009 total mismatch' from platform, expected where platform.churned_users <> expected.churned_users
union all
select 'M001 free+paid mismatch ' || metric_month from level_vs_platform
