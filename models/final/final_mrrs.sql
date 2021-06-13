with final as(
SELECT total_mrrs.year_month,
       total_mrr.mrr,
       total_mrr.customer_count,
       total_mrrs.new_mrr,
       total_mrrs.expansion_mrr,
       total_churn.churned_mrr*-1 as churned_mrr,
       total_mrrs.contraction_mrr*-1 as contraction_mrr,
       new_mrr + expansion_mrr - churned_mrr - contraction_mrr as net_new_mrr,
       total_churn.mrr_churn,
       total_churn.customer_churn,
       total_mrr.arpu
FROM {{ref('total_mrrs')}} as total_mrrs
LEFT JOIN {{ref('total_churn')}} total_churn on total_mrrs.year_month = total_churn.year_month
JOIN {{ref('total_mrr')}} total_mrr on total_mrr.year_month = total_mrrs.year_month
ORDER BY total_mrrs.year_month)

select * from final