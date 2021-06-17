with final as(
select sbqq__account__c as account,
       sbqq__productname__c as product_name,
       sbqq__productid__c as product_id,
       sbqq__netprice__c as net_price,
       sbqq__quantity__c as quantity,
       case
        when sbqq__subscriptionstartdate__c__t is null then sbqq__startdate__c__t
        else sbqq__subscriptionstartdate__c__t
       end as start_date,
       case
        when sbqq__subscriptionenddate__c__t is null then sbqq__enddate__c__t
        else sbqq__subscriptionenddate__c__t
       end as end_date
    from {{var('schema')}}.sbqq__subscription__c)

select * from final

