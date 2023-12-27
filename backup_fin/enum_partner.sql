with 
promotion_tab as 
(
    select  
            promotion_id
        ,   promotion_code
        ,   partner_share_array
        ,   SUM(if(partner_id = 1, partner_percent, 0))             foody_percent 
        ,   SUM(if(partner_id = 2, partner_percent, 0))             airpay_percent 
        ,   SUM(if(partner_id = 3, partner_percent, 0))             shopee_percent 
        ,   SUM(if(partner_id not in (1,2,3), partner_percent, 0))  others_partner_percent
        ,   case partner_id
                when 1 then 'Foody'
                when 2 then 'Airpay'
                when 3 then 'Shopee'
                when 4 then 'Visa'
                when 5 then 'MasterCard'
                when 6 then 'JCB'
                when 7 then 'VPBank Credit'
                when 8 then 'Shinhanbank'
                when 9 then 'CitiBank'
                when 11 then 'Standard Chartered'
                when 13 then 'HSBC'
                when 18 then 'Other'
                when 17 then 'ACB'
                when 20 then 'Techcombank'
                when 21 then 'OCB'
                when 22 then 'VIB'
                when 23 then 'MB'
                when 24 then 'FE Credit'
                when 25 then 'VPBank Debit'
                when 31 then 'MB-Hi'
                else 'Unknown'
            end as partner_share_name 
        ,   partner_percent 
    from 
    (
        select  id      promotion_id 
            ,   code    promotion_code 
            ,   json_extract(extra_data, '$.discount_data.partner_share')   partner_share_array 
            ,   partner_share.partner   partner_id
            ,   partner_share.percent   partner_percent 
        from shopeefood.foody_promotion_db__promotion_tab__reg_daily_s0_live t1 
        cross join unnest (cast(json_extract(extra_data, '$.discount_data.partner_share') as array(row(partner int , percent int)))) as  partner_share
        where   1=1 
            and id = 29281436
            -- and partner_share.percent <> 0 
            -- and not (partner_share.partner = 1 and partner_share.percent <> 100)
    )
    group by 1,2,3
)
