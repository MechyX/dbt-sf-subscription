with final as(
SELECT month_range.year_month,
       sum(subscriptions.net_price) as MRR,
       count(distinct account) as customer_count,
       sum(subscriptions.net_price) / count(distinct account) as ARPU,
       count(distinct product_id) as product_count
from {{ref('stg_subscriptions')}} as subscriptions left join {{ref('dates')}} month_range on (month_range.year_month < subscriptions.end_date
and month_range.year_month >= subscriptions.start_date)
group by month_range.year_month)

select * from final

