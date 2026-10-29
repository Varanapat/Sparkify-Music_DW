-- metric ที่ unit = ratio ต้องอยู่ระหว่าง 0 ถึง 1 / metric อื่นต้องไม่ติดลบ
select metric_month, metric_key, unit, metric_value
from {{ ref('metric_platform_monthly') }}
where (unit = 'ratio' and (metric_value < 0 or metric_value > 1))
   or metric_value < 0
