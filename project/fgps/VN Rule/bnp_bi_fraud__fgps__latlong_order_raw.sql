-- DROP TABLE IF EXISTS    dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__latlong_order_raw;
-- CREATE TABLE            dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__latlong_order_raw WITH (partitioned_by = ARRAY['grass_date']) AS 

DELETE FROM dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__latlong_order_raw WHERE grass_date between date(date_parse('${D7}','%Y%m%d')) and date(date_parse('${yesterday}','%Y%m%d'));
INSERT INTO dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__latlong_order_raw 


WITH 
datas AS 
    (
        SELECT
                order_id
            ,   driver_id
            ,   create_time
            ,   order_date
            ,   service_type
            ,   CAST(accept_latitude AS VARCHAR) AS accept_latitude1
            ,   CAST(accept_longitude AS VARCHAR) AS accept_longitude1
            ,   CAST(store_latitude AS VARCHAR) As store_latitude1
            ,   CAST(store_longitude AS VARCHAR) AS store_longitude1
            ,   accept_latitude
            ,   accept_longitude
            ,   store_latitude
            ,   store_longitude
            ,   delivery_assign_time
            ,   delivery_pickup_time
            ,   delivery_delivered_time
        FROM 
        (
            SELECT
                    od.order_id
                ,   od.driver_now_id driver_id
                ,   od.create_time
                ,   od.order_date
                ,   od.service_type
                ,   COALESCE(gc.latitude, od.accept_latitude) AS accept_latitude
                ,   COALESCE(gc.longitude,od.accept_longitude) AS accept_longitude
                ,   od.store_longitude
                ,   od.store_latitude
                ,   od.delivery_assign_time
                ,   od.delivery_pickup_time
                ,   od.delivery_delivered_time
                ,   ROW_NUMBER() OVER(PARTITION BY od.order_id ORDER BY gc.ping_time DESC, gc.upload_time DESC) AS rk
            FROM        dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__driver_track_order_di od
            LEFT JOIN   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__driver_track_ping_di  gc ON od.driver_now_id = gc.driver_now_id AND gc.ping_time > od.delivery_assign_time - INTERVAL '5' MINUTE AND gc.ping_time <= od.delivery_assign_time 
            -- LEFT JOIN   algo_pair   ap ON o.order_id = ap.order_id AND o.driver_id = ap.driver_id
            WHERE   1=1
                AND od.order_date   BETWEEN DATE(DATE_PARSE('${D7}','%Y%m%d'))                          AND DATE(DATE_PARSE('${yesterday}','%Y%m%d')) 
                AND gc.ping_date    BETWEEN DATE(DATE_PARSE('${D7}','%Y%m%d'))  - INTERVAL '1' DAY      AND DATE(DATE_PARSE('${yesterday}','%Y%m%d')) 
        )
        WHERE   rk=1
    )

SELECT 
        order_id
    ,   driver_id
    ,   create_time
    ,   order_date
    ,   service_type
    ,   accept_latitude1
    ,   accept_longitude1
    ,   store_latitude1
    ,   store_longitude1
    ,   accept_latitude
    ,   accept_longitude
    ,   store_latitude
    ,   store_longitude
    ,   LENGTH(SPLIT_PART(SPLIT_PART(accept_latitude1 , '.', 2), 'E', 1))   acc_lat_par1
    ,   LENGTH(SPLIT_PART(SPLIT_PART(accept_longitude1, '.', 2), 'E', 1))   acc_long_par1
    ,   LENGTH(SPLIT_PART(SPLIT_PART(store_latitude1  , '.', 2), 'E', 1))   store_lat_par1
    ,   LENGTH(SPLIT_PART(SPLIT_PART(store_longitude1 , '.', 2), 'E', 1))   store_long_par1
    ,   SPLIT_PART(accept_latitude1 , 'E', 2)   acc_lat_par2
    ,   SPLIT_PART(accept_longitude1, 'E', 2)   acc_long_par2
    ,   SPLIT_PART(store_latitude1 , 'E', 2)    store_lat_par2
    ,   SPLIT_PART(store_longitude1, 'E', 2)    store_long_par2
    ,   delivery_assign_time
    ,   delivery_pickup_time
    ,   delivery_delivered_time 
    ,   order_date  grass_date
FROM datas 
