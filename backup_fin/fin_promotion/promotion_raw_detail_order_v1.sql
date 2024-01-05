-- DROP TABLE IF EXISTS        dev_vnfdbi_opsndrivers.fin_accountant__promotion_report__raw;
-- CREATE TABLE IF NOT EXISTS  dev_vnfdbi_opsndrivers.fin_accountant__promotion_report__raw WITH (partitioned_by = ARRAY['grass_month']) AS 

-- DELETE FROM dev_vnfdbi_opsndrivers.fin_accountant__promotion_report__raw WHERE grass_month >= YEAR(DATE_PARSE('${yesterday}','%Y%m%d'));
DELETE FROM dev_vnfdbi_opsndrivers.fin_accountant__promotion_report__raw WHERE grass_month >= date_trunc('month', DATE_PARSE('${yesterday}','%Y%m%d')) - INTERVAL '1' MONTH;
INSERT INTO dev_vnfdbi_opsndrivers.fin_accountant__promotion_report__raw 

WITH 
raw AS  
    (
        SELECT  
                t1.city_id
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
            ,   FROM_UNIXTIME(t1.create_time - 3600) created_time
            ,   t1.order_id
            ,   t1.order_code
            ,   CAST(JSON_EXTRACT(t1.extra_data, '$.order.original_price') AS double) original_price
            ,   CASE WHEN t1.status_id =7 THEN 'Completed' ELSE 'Uncompleted' END status
            ,   promotions.promotion_id
            ,   promotions.promotion_code
            ,   promotions.discount_type
            ,   promotions.discount total_discount
            ,   promotions.discount_on_type
            ,   CAST(promotions.partner_share AS ARRAY(ROW(partner INT, percent INT, discount DOUBLE))) partner_share
            ,   promotions.create_time promotion_create_time
            ,   DATE(from_unixtime(delivered_date - 3600)) delivered_date
            ,   DATE_TRUNC('month', DATE(FROM_UNIXTIME(delivered_date - 3600))) grass_month 
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
                            )
                        )
                    ) 
            ) AS promotions 
        WHERE   1=1
            AND JSON_ARRAY_LENGTH(JSON_EXTRACT(t1.extra_data, '$.order.promotions')) > 0
            AND DATE(from_unixtime(delivered_date - 3600)) >= date_trunc('month', date_parse('${yesterday}','%Y%m%d')) - INTERVAL '1' MONTH
        UNION 

        SELECT 
                t1.city_id
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
            ,   FROM_UNIXTIME(t1.create_time - 3600) created_time
            ,   t1.order_id
            ,   t1.order_code
            ,   CAST(JSON_EXTRACT(t1.extra_data, '$.order.original_price') AS double) original_price
            ,   CASE WHEN t1.status_id =7 THEN 'Completed' ELSE 'Uncompleted' END status
            ,   promotions.promotion_id
            ,   promotions.promotion_code
            ,   promotions.discount_type
            ,   promotions.discount total_discount
            ,   promotions.discount_on_type
            ,   CAST(promotions.partner_share AS ARRAY(ROW(partner INT, percent INT, discount DOUBLE))) partner_share
            ,   promotions.create_time promotion_create_time
            ,   DATE(from_unixtime(delivered_date - 3600)) delivered_date
            ,   DATE_TRUNC('month', DATE(FROM_UNIXTIME(delivered_date - 3600))) grass_month  
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
                            )
                        )
                    ) 
            ) AS promotions 
        WHERE   1=1
            AND JSON_ARRAY_LENGTH(JSON_EXTRACT(t1.extra_data, '$.order.promotions')) > 0
            AND DATE(from_unixtime(delivered_date - 3600)) >= date_trunc('month', date_parse('${yesterday}','%Y%m%d')) - INTERVAL '1' MONTH
    )
select * 
from raw 
