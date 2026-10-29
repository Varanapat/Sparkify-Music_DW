-- Grain ของ metric model ต้องไม่ซ้ำ: platform = (เดือน, metric_key), level = (เดือน, level, metric_key)
select 'metric_platform_monthly' as model, metric_month, null as level, metric_key, count(*) as row_count
from {{ ref('metric_platform_monthly') }}
group by metric_month, metric_key
having count(*) > 1

union all

select 'metric_level_monthly', metric_month, level, metric_key, count(*)
from {{ ref('metric_level_monthly') }}
group by metric_month, level, metric_key
having count(*) > 1
