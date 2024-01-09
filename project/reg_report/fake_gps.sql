/* 
  Row Fake gps 
  - 1: avg daily driver hit rule 
  - 2: avg daily driver hit repacked app rate 
*/
WITH 
base AS 
(
    SELECT  order_date 
        ,   COUNT(DISTINCT order_id)        cnt_order 
        ,   COUNT(DISTINCT driver_now_id)   cnt_driver 
    FROM    dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__order_tags 
    WHERE   1=1 
        AND behavior != '' 
        /* (1) */AND (behavior LIKE 'Rules_%') --OR behavior LIKE 'Proxy_%') 
        /* (2) */--AND behavior = 'Proxy_Repacked App usage' 
        AND grass_date BETWEEN DATE('2023-11-01') AND DATE('2023-12-31')
    GROUP BY 1 
    ORDER BY 1 
)
SELECT  MONTH(order_date) 
    ,   AVG(cnt_driver) avg_driver
FROM base 
group by 1 
order by 1 
;

WITH 
base as 
(
    SELECT  order_created_date  submit_date
            ,   CASE 
                    WHEN fraud_code IN ('dc-011') THEN 'Driver complete order with delivering food' 
                    WHEN fraud_code IN ('dc-004') THEN 'Driver complete order with delivering food' 
                    WHEN fraud_code IN ('dc-018') THEN 'Improper use of edit order function to change order amount' 
                    WHEN fraud_code IN ('dc-007', 'dc-008', 'dc-014', 'dc-017') THEN 'Collusion / Fake / Suspicious Orders' 
                    ELSE 'Others' 
                END fraud_mo 
            ,   case    source 
                    when 'food'             then 'Delivered Orders'
                    when 'now_ship_shopee'  then 'SPX Orders'
                    when 'now_ship_user'    then 'C2C Orders'
                end source
            ,   fraud_code 
            ,   purpose_en 
            ,   order_code 
            ,   shipper_id  cheater 
            ,   balance 
            ,   amount_charge_back 
            ,   exchange_rate 
            ,   (amount_charge_back + balance)/exchange_rate  loss
        FROM    dev_vnfdbi_opsndrivers.shopeefood_vn_food_fraud_driver_bad_debt_fraud_raw_data 
        where    1=1
            AND order_created_date BETWEEN DATE('${date_start}') AND DATE('${date_ended}')
            AND source != 'fresh' 
            AND fraud_code like 'dc-%' 
)
select MONTH(submit_date)   grass_month 
    ,   AVG(cnt_driver)     avg_driver 
    ,   sum(loss)           total_loss
from 
(
    select  
            submit_date
        ,   count(distinct cheater) cnt_driver 
        ,   sum(loss)               loss
    from base 
    group by 1
)
group by 1 
order by 1

