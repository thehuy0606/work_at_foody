-- DROP TABLE IF EXISTS    dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__pickup_before;
-- CREATE TABLE            dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__pickup_before WITH (partitioned_by = ARRAY['grass_date']) AS 
DELETE FROM dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__pickup_before WHERE grass_date BETWEEN DATE(DATE_PARSE('${D7}','%Y%m%d')) AND DATE(DATE_PARSE('${yesterday}','%Y%m%d'));
INSERT INTO dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__pickup_before 

WITH 
final_data AS  
    (
        SELECT  DISTINCT 
                od.order_id 
            ,   od.driver_now_id    driver_id
            ,   od.create_time 
            ,   od.order_date 
            ,   od.delivery_assign_time 
            ,   od.delivery_pickup_time 
            ,   od.delivery_delivered_time 
            ,   od.service_type 
            ,   gc.latitude 
            ,   gc.longitude 
            ,   gc.latitude1 
            ,   gc.longitude1 
            ,   gc.speed_kmh 
            ,   gc.head 
            ,   gc.device_id 
            ,   gc.provider 
            ,   gc.ping_time 
            ,   gc.upload_time 
            ,   gc.accuracy 
            ,   gc.round_accuracy 
            ,   gc.digit_accuracy   acc_digit 
            ,   gc.digit_speed_kmh  speed_digit 
            ,   LENGTH(SPLIT_PART(SPLIT_PART(gc.latitude1 , '.', 2), 'E', 1))   lat_par1
            ,   LENGTH(SPLIT_PART(SPLIT_PART(gc.longitude1, '.', 2), 'E', 1))   long_par1
            ,   SPLIT_PART(gc.latitude1 , 'E', 2)   lat_par2
            ,   SPLIT_PART(gc.longitude1, 'E', 2)   long_par2
        FROM        dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__driver_track_order_di od 
        LEFT JOIN   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__driver_track_ping_di  gc 
            ON  od.driver_now_id = gc.driver_now_id 
            AND gc.ping_time    BETWEEN od.delivery_assign_time             - INTERVAL '5' MINUTE   AND od.delivery_pickup_time 
        WHERE   1=1 
            AND od.order_date   BETWEEN DATE(DATE_PARSE('${D7}','%Y%m%d'))                          AND DATE(DATE_PARSE('${yesterday}','%Y%m%d')) 
            AND gc.ping_date    BETWEEN DATE(DATE_PARSE('${D7}','%Y%m%d'))  - INTERVAL '1' DAY      AND DATE(DATE_PARSE('${yesterday}','%Y%m%d')) 
            -- AND od.order_date   BETWEEN DATE('2023-10-20')                          AND DATE('2023-10-31')   
            -- AND gc.ping_date    BETWEEN DATE('2023-10-20')  - INTERVAL '1' DAY      AND DATE('2023-10-31')   
    )   
select  * 
    ,   order_date grass_date 
from    final_data
