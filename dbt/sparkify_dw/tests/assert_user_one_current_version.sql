-- ผู้ใช้แต่ละคนต้องมี version ปัจจุบัน (is_current) เพียง 1 แถว และ version ติดกันต้องมี level ต่างกัน
with current_counts as (
    select user_id, count(*) filter (where is_current) as current_rows
    from {{ ref('dim_user') }}
    where user_id is not null
    group by user_id
),

same_level_neighbours as (
    select a.user_id
    from {{ ref('dim_user') }} a
    join {{ ref('dim_user') }} b
      on a.user_id = b.user_id
     and b.user_version = a.user_version + 1
    where a.level = b.level
)

select user_id, 'current_rows <> 1' as problem from current_counts where current_rows <> 1
union all
select user_id, 'consecutive versions share a level' from same_level_neighbours
