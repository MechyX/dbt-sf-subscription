with final as(
SELECT this_month.year_month + interval '1 month' as year_month,
       sum(
         case
           when next_month.year_month is null then this_month.total
           else 0
         end) as churned_mrr,
       sum(
        case
          when next_month.year_month is null then this_month.total
          else 0
        end) / total_mrr.mrr as mrr_churn,
      (sum(
        case
          when next_month.year_month is null then 1
          else 0
        end) * 1.0) / total_mrr.product_count as product_churn
FROM {{ref('products_charges')}} as this_month
LEFT JOIN {{ref('products_charges')}} next_month ON this_month.product_id = next_month.product_id AND this_month.year_month = next_month.year_month - interval '1 month'
JOIN {{ref('total_mrr')}} total_mrr on total_mrr.year_month = this_month.year_month
group by 1, total_mrr.mrr, total_mrr.product_count)
select * from final