with final as(
SELECT this_month.product_id,
       this_month.year_month,
       case
         when previous_month.year_month is null then this_month.total
         else 0
       end as new_mrr,
       case
         when previous_month.total is null then 0
         when previous_month.total >= this_month.total then previous_month.total - this_month.total
       end as contraction_mrr,
       case
         when previous_month.total is null then 0
         when previous_month.total <= this_month.total then this_month.total  - previous_month.total
       end as expansion_mrr
FROM {{ref('products_charges')}} as this_month
LEFT JOIN {{ref('products_charges')}} previous_month ON this_month.product_id = previous_month.product_id AND this_month.year_month = previous_month.year_month + interval '1 month')

select * from final