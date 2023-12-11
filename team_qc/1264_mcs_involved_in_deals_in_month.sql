with 
ps as 
    (
        select  
                merchant_id 
            ,   brand_id 
            ,   merchant_name 
            ,   browse_list_tag_en as deal_name 
            ,   start_date  
            ,   end_date 
            ,   item_id 
            ,   item_name 
            ,   original_price 
            ,   selling_price discount_price 
            ,   count(distinct order_id) cnt_order 
        from dev_vnfdbi_commercial.shopeefood_vn_food_price_slash_master 
        where   1=1 
            and selling_price = 1000
            and start_date >= date('2023-09-01')
            and end_date <= date('2023-09-30') 
        group by 1,2,3,4,5,6,7,8,9,10
    )
, me as 
    (
        select  
                merchant_id
            ,   brand_id 
            ,   address_text    address
            ,   district_name   district 
            ,   city_name       city 
            ,   contract_rep_name 
            ,   mex_phone  
            ,   bde 
        from dev_vnfdbi_commercial.shopeefood_vn_food_mex_contract_master
    )
select  ps.merchant_id
    ,   ps.brand_id
    ,   ps.merchant_name 
    ,   me.address 
    ,   me.district
    ,   me.city 
    ,   me.contract_rep_name mex_representative
    ,   me.mex_phone phone_number 
    ,   me.bde BD_name 
    ,   ps.deal_name 
    ,   ps.start_date 
    ,   ps.end_date 
    ,   ps.item_id 
    ,   ps.item_name 
    ,   ps.original_price 
    ,   ps.discount_price 
    ,   ps.cnt_order
from  ps 
left join me on ps.merchant_id = me.merchant_id 

-------------------------------------------------------------------------------------

with 
base as 
    (
        select  
                -- *
                b.merchant_id 
            ,   regexp_extract(a.promotion_title_en , '.*: (.*)$', 1) AS code_off
            ,   a.promotion_id 
            ,   a.promotion_code 
            ,   date(f.start_time)  start_date
            ,   date(f.end_time)    end_date
            ,   IF(f.discount_value_type = 'percent', concat(cast(f.discount_value as varchar), ' %'), cast(f.discount_value as varchar)) discount_price
            ,   f.min_order_amount
            ,   f.max_discount_value 
            ,   count(distinct a.order_id) cnt_order 
        from        shopeefood.foody_mart__fact_order_promotion a 
        left join   dev_vnfdbi_opsndrivers.fraud_manual_promocode f on a.promotion_id = f.id
        join   shopeefood.foody_mart__fact_gross_order_join_detail b on a.order_id = b.id and b.order_status_id = 7 and date(b.grass_date) between date('2023-09-01') and date('2023-09-30')
        where   promotion_code IN ('SIEUDEAL144K', 'SIEUDEAL45', 'SIEUDEAL35', 'SIEUDEAL25', 'SIEUDEAL30', 'SIEUDEAL40', 'SIEUDEAL155', 'SIEUDEAL166', 
                        'SIEUTET25', 'SIEUTET30', 'SIEUTET35', 'SIEUTET50', 'CUPID25', 'CUPID30', 'CUPID35', 'CUPID50', 'TYPN25', 'TYPN30', 'TYPN35', 
                        'TYPN50', 'SIEUDEAL177', 'SIEUDEAL88K', 'SIEUDEAL188', '99SIEUDEAL45', '99SIEUDEAL35', '99SIEUDEAL25', '99SIEUDEAL30', '99SIEUDEAL40', 
                        '99SIEUDEAL199', '1010SIEUDEAL45', '1010SIEUDEAL35', '1010SIEUDEAL25', '1010SIEUDEAL30', '1010SIEUDEAL40', '1010SIEUDEAL110'
                    )
        group by 1,2,3,4,5,6,7,8,9
    )
, me as 
    (
        select  
                merchant_id 
            ,   merchant_name 
            ,   brand_id 
            ,   address_text    address
            ,   district_name   district 
            ,   city_name       city 
            ,   contract_rep_name 
            ,   mex_phone  
            ,   bde 
        from dev_vnfdbi_commercial.shopeefood_vn_food_mex_contract_master
    )

select  a.merchant_id
    ,   m.brand_id
    ,   m.merchant_name 
    ,   m.address 
    ,   m.district
    ,   m.city 
    ,   m.contract_rep_name mex_representative
    ,   m.mex_phone phone_number 
    ,   m.bde BD_name 
    ,   a.promotion_id 
    ,   a.promotion_code
    ,   a.code_off
    ,   a.start_date 
    ,   a.end_date 
    ,   a.min_order_amount 
    ,   a.max_discount_value 
    ,   a.discount_price 
    ,   a.cnt_order
from        base a
left join   me m on a.merchant_id = m.merchant_id 
