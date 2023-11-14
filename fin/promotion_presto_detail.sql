-- DROP TABLE IF EXISTS        dev_vnfdbi_opsndrivers.fin_accountant__promotion_order_level__daily_presto;
-- CREATE TABLE IF NOT EXISTS  dev_vnfdbi_opsndrivers.fin_accountant__promotion_order_level__daily_presto WITH (partitioned_by = ARRAY['grass_month']) AS 

-- DELETE FROM dev_vnfdbi_opsndrivers.fin_accountant__promotion_order_level__daily_presto 
-- WHERE grass_month BETWEEN CAST(DATE_FORMAT(DATE_TRUNC('MONTH', DATE(${NByesterday}))  - INTERVAL '1' MONTH, '%Y%m') AS INT) 
--     AND CAST(DATE_FORMAT(DATE(${NByesterday}), '%Y%m') AS INT); 
DELETE FROM dev_vnfdbi_opsndrivers.fin_accountant__promotion_order_level__daily_presto 
WHERE grass_month BETWEEN CAST(DATE_FORMAT(DATE_TRUNC('MONTH', DATE('2023-07-01')), '%Y%m') AS INT) AND CAST(DATE_FORMAT(DATE('2023-07-31'), '%Y%m') AS INT); 

INSERT INTO dev_vnfdbi_opsndrivers.fin_accountant__promotion_order_level__daily_presto 

WITH 
city as 
    (
        select  
                id as city_id 
            ,   name as city_name 
        from shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live
        where country_id = 86 
    ),
raw AS  
    (
        SELECT  
                t1.order_id
            ,   t1.order_code 
            ,   CASE WHEN t1.status_id =7 THEN 'Completed' ELSE 'Uncompleted' END status 
            ,   CASE t1.payment_method_id
                    WHEN 1 then 'COD'
                    WHEN 2 then 'Shopeefood Credit'
                    WHEN 4 then 'Credit/Debit Card'
                    WHEN 6 then 'ShopeePay'
                    WHEN 8 then 'ATM/Internet Banking'
                    ELSE 'Unknown' 
                END user_payment_method 
            ,   t1.merchant_payment_method_id 
            ,   t1.city_id
            ,   t1.partner_id 
            ,   t1.partner_run_type 
            ,   t1.partner_type 
            ,   t1.partner_contract_type 
            ,   t1.partner_service
            ,   DATE(FROM_UNIXTIME(t1.delivered_date - 3600))   delivered_date 
            ,   FROM_UNIXTIME(t1.create_time - 3600)            created_time 
            ,   HOUR(FROM_UNIXTIME(t1.create_time - 3600))      created_hour
            ,   t1.merchant_id 
            ,   t1.merchant_name 
            ,   CASE    --  order_source
                    WHEN CAST(JSON_EXTRACT(t1.extra_data, '$.order.app_type') AS INT) = 1004 THEN 'NOW' 
                    WHEN CAST(JSON_EXTRACT(t1.extra_data, '$.order.app_type') AS INT) = 2000 THEN 'FOODY' 
                    WHEN CAST(JSON_EXTRACT(t1.extra_data, '$.order.app_type') AS INT) = 3000 THEN 'SHOPEE' 
                    ELSE 'Other' 
                END AS order_source 
            ,   CASE    --  service_name
                    WHEN CAST(JSON_EXTRACT(t1.extra_data, '$.order.foody_service_id') as int) = 1 then 'Food'
                    ELSE 'Fresh' 
                END AS service_name
            ,   CAST(JSON_EXTRACT(t1.extra_data, '$.order.original_price')  as double)  original_price 
            ,   CAST(JSON_EXTRACT(t1.extra_data, '$.order.total_amount')    as double)  total_amount 
            ,   CAST(JSON_EXTRACT(t1.extra_data, '$.order.shipping_fee')    as double)  shipping_fee

            ,   promotions.promotion_id
            ,   promotions.promotion_code
            ,   promotions.discount_type
            ,   promotions.discount total_discount
            ,   promotions.discount_on_type
            ,   promotions.create_time promotion_create_time 
            ,   CAST(promotions.partner_share AS ARRAY(ROW(partner INT, percent INT, discount DOUBLE))) partner_share 
            ,   promotions.purpose_name 
            ,   promotions.promotion_source 
            ,   promotions.purpose_id 
            ,   promotions.promotion_type 
            ,   promotions.merchant_discount
            ,   promotions.promotion_name 

            ,   JSON_EXTRACT(t1.extra_data, '$.partner')    tab_partner
            ,   JSON_EXTRACT(t1.extra_data, '$.order')      tab_order
            ,   JSON_EXTRACT(t1.extra_data, '$.commission') tab_commission 

        FROM shopeefood.foody_accountant_db__order_delivery_tab__reg_daily_s0_live t1 
        CROSS JOIN UNNEST 
            (
                CAST(JSON_EXTRACT(t1.extra_data, '$.order.promotions') 
                AS ARRAY(ROW(   
                                    promotion_id        BIGINT
                                ,   promotion_code      VARCHAR  
                                ,   discount            DOUBLE 
                                ,   discount_type       INT
                                ,   discount_on_type    INT
                                ,   create_time         BIGINT
                                ,   partner_share       JSON 
                                ,   purpose_name        VARCHAR 
                                ,   promotion_source    INT 
                                ,   purpose_id          INT 
                                ,   promotion_type      INT 
                                ,   merchant_discount   DOUBLE 
                                ,   promotion_name      VARCHAR
                            )
                        )
                    ) 
            ) AS promotions 
        WHERE   1=1
            AND JSON_ARRAY_LENGTH(JSON_EXTRACT(t1.extra_data, '$.order.promotions')) > 0 
            -- AND DATE(FROM_UNIXTIME(t1.delivered_date - 3600)) BETWEEN DATE('2023-08-01') AND DATE('2023-08-31') 

        UNION 

        SELECT  
                t1.order_id
            ,   t1.order_code 
            ,   CASE WHEN t1.status_id =7 THEN 'Completed' ELSE 'Uncompleted' END status 
            ,   CASE t1.payment_method_id
                    WHEN 1 then 'COD'
                    WHEN 2 then 'Shopeefood Credit'
                    WHEN 4 then 'Credit/Debit Card'
                    WHEN 6 then 'ShopeePay'
                    WHEN 8 then 'ATM/Internet Banking'
                    ELSE 'Unknown' 
                END user_payment_method 
            ,   t1.merchant_payment_method_id 
            ,   t1.city_id
            ,   t1.partner_id 
            ,   t1.partner_run_type 
            ,   t1.partner_type 
            ,   t1.partner_contract_type 
            ,   t1.partner_service
            ,   DATE(FROM_UNIXTIME(t1.delivered_date - 3600))   delivered_date 
            ,   FROM_UNIXTIME(t1.create_time - 3600)            created_time 
            ,   HOUR(FROM_UNIXTIME(t1.create_time - 3600))      created_hour
            ,   t1.merchant_id 
            ,   t1.merchant_name 
            ,   CASE    --  order_source
                    WHEN CAST(JSON_EXTRACT(t1.extra_data, '$.order.app_type') AS INT) = 1004 THEN 'NOW' 
                    WHEN CAST(JSON_EXTRACT(t1.extra_data, '$.order.app_type') AS INT) = 2000 THEN 'FOODY' 
                    WHEN CAST(JSON_EXTRACT(t1.extra_data, '$.order.app_type') AS INT) = 3000 THEN 'SHOPEE' 
                    ELSE 'Other' 
                END AS order_source 
            ,   CASE    --  service_name
                    WHEN CAST(JSON_EXTRACT(t1.extra_data, '$.order.foody_service_id') as int) = 1 then 'Food'
                    ELSE 'Fresh' 
                END AS service_name
            ,   CAST(JSON_EXTRACT(t1.extra_data, '$.order.original_price')  as double)  original_price 
            ,   CAST(JSON_EXTRACT(t1.extra_data, '$.order.total_amount')    as double)  total_amount 
            ,   CAST(JSON_EXTRACT(t1.extra_data, '$.order.shipping_fee')    as double)  shipping_fee

            ,   promotions.promotion_id
            ,   promotions.promotion_code
            ,   promotions.discount_type
            ,   promotions.discount total_discount
            ,   promotions.discount_on_type
            ,   promotions.create_time promotion_create_time 
            ,   CAST(promotions.partner_share AS ARRAY(ROW(partner INT, percent INT, discount DOUBLE))) partner_share 
            ,   promotions.purpose_name 
            ,   promotions.promotion_source 
            ,   promotions.purpose_id 
            ,   promotions.promotion_type 
            ,   promotions.merchant_discount
            ,   promotions.promotion_name 

            ,   JSON_EXTRACT(t1.extra_data, '$.partner')    tab_partner
            ,   JSON_EXTRACT(t1.extra_data, '$.order')      tab_order
            ,   JSON_EXTRACT(t1.extra_data, '$.commission') tab_commission 

        FROM shopeefood.foody_accountant_archive_db__order_delivery_tab__reg_daily_s0_live t1 
        CROSS JOIN UNNEST 
            (
                CAST(JSON_EXTRACT(t1.extra_data, '$.order.promotions') 
                AS ARRAY(ROW(   
                                    promotion_id        BIGINT
                                ,   promotion_code      VARCHAR  
                                ,   discount            DOUBLE 
                                ,   discount_type       INT
                                ,   discount_on_type    INT
                                ,   create_time         BIGINT
                                ,   partner_share       JSON 
                                ,   purpose_name        VARCHAR 
                                ,   promotion_source    INT 
                                ,   purpose_id          INT 
                                ,   promotion_type      INT 
                                ,   merchant_discount   DOUBLE 
                                ,   promotion_name      VARCHAR
                            )
                        )
                    ) 
            ) AS promotions 
        WHERE   1=1
            AND JSON_ARRAY_LENGTH(JSON_EXTRACT(t1.extra_data, '$.order.promotions')) > 0 
            -- AND DATE(FROM_UNIXTIME(t1.delivered_date - 3600)) BETWEEN DATE('2023-08-01') AND DATE('2023-10-15') 
    ) 
,
main_query AS 
    (
        SELECT  
                t1.order_id 
            ,   t1.order_code 
            ,   t1.status 
            ,   t1.user_payment_method 
            ,   t1.merchant_payment_method_id 
            ,   t1.city_id
            ,   t2.city_name 
            ,   t1.partner_id 
            ,   t1.partner_run_type 
            ,   t1.partner_type 
            ,   t1.partner_contract_type 
            ,   t1.partner_service 
            ,   t1.delivered_date 
            ,   t1.created_time 
            ,   t1.created_hour
            ,   t1.merchant_id 
            ,   t1.merchant_name 
            ,   t1.order_source 
            ,   t1.service_name
            ,   t1.original_price
            ,   t1.total_amount 
            ,   t1.shipping_fee
            ,   t1.promotion_id
            ,   t1.promotion_code
            ,   t1.discount_type 
            ,   t1.total_discount
            ,   t1.discount_on_type 
            ,   t1.promotion_create_time 
            ,   t1.partner_share 
            ,   t1.purpose_name 
            ,   t1.promotion_source 
            ,   t1.purpose_id 
            ,   t1.promotion_type 
            ,   t1.merchant_discount 
            ,   t1.promotion_name 
            ,   CASE partner_share.partner
                    WHEN 1  THEN 'Foody'
                    WHEN 2  THEN 'Airpay'
                    WHEN 3  THEN 'Shopee'
                    WHEN 4  THEN 'Visa'
                    WHEN 5  THEN 'MasterCard'
                    WHEN 6  THEN 'JCB'
                    WHEN 7  THEN 'VPBank Credit'
                    WHEN 8  THEN 'Shinhanbank'
                    WHEN 9  THEN 'CitiBank'
                    WHEN 11 THEN 'Standard Chartered'
                    WHEN 13 THEN 'HSBC'
                    WHEN 18 THEN 'Other'
                    WHEN 17 THEN 'ACB'
                    WHEN 20 THEN 'Techcombank'
                    WHEN 21 THEN 'OCB'
                    WHEN 22 THEN 'VIB'
                    WHEN 23 THEN 'MB'
                    WHEN 24 THEN 'FE Credit'
                    WHEN 25 THEN 'VPBank Debit'
                    ELSE 'Unknown'
                END                     partner_share_name 
            ,   partner_share.percent   partner_share_percent
            ,   partner_share.discount  partner_share_discount
            ,   t1.delivered_date       grass_date 
            ,   YEAR(t1.delivered_date) grass_year 
            ,   CAST(DATE_FORMAT(t1.delivered_date, '%Y%m') AS INT) grass_month 
        FROM        raw     t1
        LEFT JOIN   city    t2 ON t1.city_id = t2.city_id 
        CROSS JOIN UNNEST(t1.partner_share) AS partner_share
        WHERE   1=1 
            AND partner_share.percent <> 0
            AND NOT(partner_share.partner = 1 and partner_share.percent <> 100) 
            AND t1.delivered_date BETWEEN DATE('2023-07-01') AND DATE('2023-07-31') 
    )
SELECT  * 
FROM main_query 
WHERE   grass_date BETWEEN DATE('2023-07-01') AND DATE('2023-07-31') 