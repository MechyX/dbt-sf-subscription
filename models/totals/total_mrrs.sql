with final as(
SELECT year_month,
       sum(new_mrr) as new_mrr,
       sum(contraction_mrr) as contraction_mrr,
       sum(expansion_mrr) as expansion_mrr
FROM {{ref('account_mrrs')}}
GROUP BY year_month)

select * from final
