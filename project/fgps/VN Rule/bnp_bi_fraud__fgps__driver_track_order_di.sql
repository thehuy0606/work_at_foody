-- DROP TABLE IF EXISTS    dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__driver_track_order_di;
-- CREATE TABLE            dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__driver_track_order_di WITH (partitioned_by = ARRAY['grass_date']) AS 

DELETE FROM dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__driver_track_order_di WHERE grass_date between date(date_parse('${D7}','%Y%m%d')) and date(date_parse('${yesterday}','%Y%m%d'));
INSERT INTO dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__driver_track_order_di 

SELECT  DISTINCT 
        od.id                                       order_id
    ,   od.shipper_id                               driver_now_id
    ,   sp.driver_spe_id
    ,   sp.driver_name
    ,   FROM_UNIXTIME(od.create_timestamp)          create_time 
    ,   DATE(FROM_UNIXTIME(od.create_timestamp))    order_date
    ,   od.city_name                                order_city
    ,   ac.delivery_assign_time
    ,   FROM_UNIXTIME(od.pick_timestamp)            delivery_pickup_time
    ,   FROM_UNIXTIME(od.deliver_timestamp)         delivery_delivered_time 
    ,   od.order_status_id
    ,   CASE 
            WHEN od.now_service_category_id = 1 THEN 'food' 
            WHEN od.now_service_category_id = 5 THEN 'fresh' 
        END service_type 
    ,   do.store_latitude  
    ,   do.store_longitude 
    ,   ac.accept_latitude  
    ,   ac.accept_longitude 
    ,   DATE(FROM_UNIXTIME(od.create_timestamp))    grass_date 
FROM    shopeefood.foody_mart__fact_gross_order_join_detail od 
LEFT JOIN 
    (
        SELECT  uid          driver_now_id
            ,   shopee_uid   driver_spe_id
            ,   last_name ||' '|| first_name  driver_name 
        FROM    shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live 
    )   sp ON od.shipper_id = sp.driver_now_id 
LEFT JOIN 
    (
        SELECT 
                ref_order_id    order_id
            ,   uid             shipper_id
            ,   pick_latitude   store_latitude
            ,   pick_longitude  store_longitude
        FROM shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live 
        WHERE   ref_order_category = 0 
    ) do ON od.id = do.order_id  AND od.shipper_id = do.shipper_id 
LEFT JOIN 
    (
        SELECT  delivery_assign_time
            ,   order_id 
            ,   shipper_id 
            ,   TRY_CAST(SPLIT_PART(location, ',', 1) AS DOUBLE)    accept_latitude  
            ,   TRY_CAST(SPLIT_PART(location, ',', 2) AS DOUBLE)    accept_longitude 
        FROM 
            (
                SELECT  order_id 
                    ,   shipper_uid shipper_id
                    ,   location 
                    ,   FROM_UNIXTIME(create_time-3600) delivery_assign_time
                FROM    shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                WHERE   status IN (3,4) 
                UNION ALL
                SELECT  order_id 
                    ,   shipper_uid
                    ,   location 
                    ,   FROM_UNIXTIME(create_time-3600) delivery_assign_time
                FROM shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                WHERE   status IN (3,4) 
            )
    ) ac ON od.id = ac.order_id AND od.shipper_id = ac.shipper_id 
WHERE   1 = 1
    AND DATE(FROM_UNIXTIME(od.create_timestamp)) BETWEEN DATE(DATE_PARSE('${D7}','%Y%m%d')) AND DATE(DATE_PARSE('${yesterday}','%Y%m%d'))
    AND od.order_status_id = 7 
    AND od.shipper_id > 0 
    -- AND DATE(FROM_UNIXTIME(od.create_timestamp)) >= DATE('2023-10-20')
ORDER BY 5 
