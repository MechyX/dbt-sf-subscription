with range_values as(
select
date_trunc('month', min(start_date)) as minval,
date_trunc('month', max(end_date)) as maxval
from {{ref('stg_subscriptions')}}),

final as(
SELECT generate_series(minval, maxval, '1 month'::interval) as year_month
from range_values)

select * from final