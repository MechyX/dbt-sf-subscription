with final as(
SELECT account,
sum(net_price) as total,
month_range.year_month
from {{ref('stg_subscriptions')}} as subscriptions
 left join {{ref('dates')}} month_range
on (month_range.year_month < subscriptions.end_date
and month_range.year_month >= subscriptions.start_date)
group by account, month_range.year_month)

select * from final