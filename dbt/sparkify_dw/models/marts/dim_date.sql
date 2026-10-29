-- มิติวันที่: ทุกวันต่อเนื่องในช่วงข้อมูล (รวมวันที่ไม่มี event) เพื่อให้กราฟรายวันไม่มีช่องโหว่
with bounds as (
    select
        min(event_ts)::date as first_date,
        max(event_ts)::date as last_date
    from {{ ref('stg_events') }}
),

days as (
    select generate_series(first_date, last_date, interval '1 day')::date as full_date
    from bounds
)

select
    cast(to_char(full_date, 'YYYYMMDD') as integer) as date_key,
    full_date,
    extract(year from full_date)::integer    as year,
    extract(quarter from full_date)::integer as quarter,
    extract(month from full_date)::integer   as month_number,
    trim(to_char(full_date, 'Month'))        as month_name,
    extract(week from full_date)::integer    as iso_week,
    extract(day from full_date)::integer     as day_of_month,
    extract(isodow from full_date)::integer  as day_of_week,
    trim(to_char(full_date, 'Day'))          as day_name,
    extract(isodow from full_date) in (6, 7) as is_weekend
from days
