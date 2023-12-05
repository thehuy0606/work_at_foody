-- DROP TABLE IF EXISTS    dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__driver_track_ping_di;
-- CREATE TABLE            dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__driver_track_ping_di WITH (partitioned_by = ARRAY['grass_date']) AS 

DELETE FROM dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__driver_track_ping_di WHERE grass_date between date(date_parse('${D7}','%Y%m%d')) and date(date_parse('${yesterday}','%Y%m%d'));
INSERT INTO dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__driver_track_ping_di 

WITH 
base as 
    (
        SELECT  
                pg.driver_id    driver_spe_id 
            ,   sp.uid          driver_now_id
            ,   sp.last_name ||' '|| sp.first_name  driver_name 
            ,   ct.name                             driver_city
            ,   pg.latitude
            ,   pg.longitude 
            ,   CAST(pg.latitude  AS VARCHAR)   latitude1
            ,   CAST(pg.longitude AS VARCHAR)   longitude1 
            ,   pg.upload_date
            ,   FROM_UNIXTIME(pg.upload_time/1000-3600) upload_time
            ,   FROM_UNIXTIME(pg.mtime           -3600) modify_time 
            ,   IF((pg.mtime - pg.upload_time/1000) BETWEEN -5 AND 20, FROM_UNIXTIME(pg.upload_time/1000-3600), FROM_UNIXTIME(pg.mtime-3600))       AS ping_time 
            ,   DATE(IF((pg.mtime - pg.upload_time/1000) BETWEEN -5 AND 20, FROM_UNIXTIME(pg.upload_time/1000-3600), FROM_UNIXTIME(pg.mtime-3600))) AS ping_date 
                    
            ,   pg.head

            -- acc_digit
            ,   pg.accuracy 
            ,   FLOOR(pg.accuracy)                                                                          round_accuracy 
            ,   CAST(pg.accuracy  AS VARCHAR)                                                               strin_accuracy 
            ,   LENGTH(SPLIT_PART(SPLIT_PART(SPLIT_PART(CAST(pg.accuracy  AS VARCHAR), '.', 2), '0E', 1), 'E', 1)) + CAST(SPLIT_PART(CAST(pg.accuracy AS VARCHAR), 'E', 2) AS BIGINT) * -1 AS digit_accuracy 

            --  speed_digit 
            ,   pg.speed
            ,   pg.speed*3600/1000 AS speed_kmh 
            ,   CAST(pg.speed*3600/1000 AS VARCHAR)                                                                 strin_speed_kmh
            ,   LENGTH(SPLIT_PART(SPLIT_PART(SPLIT_PART(CAST(pg.speed*3600/1000 AS VARCHAR), '.', 2), '0E', 1), 'E', 1)) + CAST(SPLIT_PART(CAST(pg.speed*3600/1000 AS VARCHAR), 'E', 2) AS BIGINT) * -1 AS digit_speed_kmh
                    
            ,   pg.altitude 
            ,   pg.device_id 
            ,   pg.provider
        FROM        shopeefood.shopeefood_mart_cdm_dwd_vn_driver_track_di                   pg   
        LEFT JOIN   shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live    sp ON pg.driver_id = sp.shopee_uid 
        LEFT JOIN   shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live           ct ON sp.working_city_id = ct.id and ct.country_id = 86 
        WHERE   1=1 
            AND DATE(IF((pg.mtime - pg.upload_time/1000) BETWEEN -5 AND 20, FROM_UNIXTIME(pg.upload_time/1000-3600), FROM_UNIXTIME(pg.mtime-3600))) BETWEEN date(date_parse('${D7}','%Y%m%d')) and date(date_parse('${yesterday}','%Y%m%d'))
        ORDER BY 1, 12 
    )
select  *
    ,   ping_date as grass_date 
from    base 
