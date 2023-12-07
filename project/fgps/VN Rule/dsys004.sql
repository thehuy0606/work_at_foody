DROP TABLE IF EXISTS    dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys004;
CREATE TABLE            dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys004 --WITH (partitioned_by = ARRAY['grass_date']) AS 
AS 
-- DELETE FROM dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys004 WHERE grass_date BETWEEN DATE(DATE_PARSE('${D7}','%Y%m%d')) AND DATE(DATE_PARSE('${yesterday}','%Y%m%d'));
-- INSERT INTO dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys004 

WITH
final_data AS 
(
    SELECT DISTINCT
            o.order_id
        ,   o.driver_id
        ,   CASE 
                WHEN accept_latitude1 LIKE '%E%' AND (CAST(acc_lat_par1 AS INTEGER) - CAST(acc_lat_par2 AS INTEGER)) > 7 THEN CAST(ROUND(CAST(o.accept_latitude AS DOUBLE), 7) AS VARCHAR)
                WHEN accept_latitude1 NOT LIKE '%E%' AND (LENGTH(accept_latitude1) - STRPOS(accept_latitude1, '.')) > 7  THEN CAST(ROUND(CAST(o.accept_latitude AS DOUBLE), 7) AS VARCHAR) 
                ELSE o.accept_latitude1 
            END AS accept_latitude
        ,   CASE 
                WHEN accept_longitude1 LIKE '%E%' AND (CAST(acc_long_par1 AS INTEGER) - CAST(acc_long_par2 AS INTEGER)) > 7 THEN CAST(ROUND(CAST(o.accept_longitude AS DOUBLE), 7) AS VARCHAR)
                WHEN accept_longitude1 NOT LIKE '%E%' AND (LENGTH(accept_longitude1) - STRPOS(accept_longitude1, '.')) > 7  THEN CAST(ROUND(CAST(o.accept_longitude AS DOUBLE), 7) AS VARCHAR) 
                ELSE o.accept_longitude1 
            END AS accept_longitude
        ,   CASE 
                WHEN store_latitude1 LIKE '%E%' AND (CAST(store_lat_par1 AS INTEGER) - CAST(store_lat_par2 as integer)) > 7 THEN CAST(ROUND(CAST(o.store_latitude AS DOUBLE), 7) AS VARCHAR)
                WHEN store_latitude1 NOT LIKE '%E%' AND (LENGTH(store_latitude1) - STRPOS(store_latitude1, '.')) > 7        THEN CAST(ROUND(CAST(o.store_latitude AS DOUBLE), 7) AS VARCHAR) 
                ELSE o.store_latitude1 
            END AS store_latitude
        , CASE 
                WHEN store_longitude1 LIKE '%E%' AND (CAST(store_long_par1 AS INTEGER) - CAST(store_long_par2 AS INTEGER)) > 7  THEN CAST(ROUND(CAST(o.store_longitude AS DOUBLE), 7) AS VARCHAR)
                WHEN store_longitude1 NOT LIKE '%E%' AND (LENGTH(store_longitude1) - STRPOS(store_longitude1, '.')) > 7         THEN CAST(ROUND(CAST(o.store_longitude AS DOUBLE), 7) AS VARCHAR) 
                ELSE o.store_longitude1 
            END AS store_longitude
        ,   o.create_time
        ,   o.order_date
        ,   o.delivery_assign_time
        ,   o.delivery_pickup_time
        ,   o.delivery_delivered_time
        ,   o.service_type
        ,   gc.latitude
        ,   gc.longitude
        ,   gc.latitude1
        ,   gc.longitude1
        ,   gc.speed_kmh
        ,   gc.accuracy
        ,   gc.head
        ,   gc.upload_time
        ,   gc.ping_time
        ,   LENGTH(SPLIT_PART(SPLIT_PART(gc.latitude1, '.', 2), 'E', 1))    lat_par1
        ,   LENGTH(SPLIT_PART(SPLIT_PART(gc.longitude1, '.', 2), 'E', 1))   long_par1
        ,   SPLIT_PART(gc.latitude1 , 'E', 2)   lat_par2
        ,   SPLIT_PART(gc.longitude1, 'E', 2)   long_par2
    FROM        dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__latlong_order_raw o
    LEFT JOIN   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__driver_track_ping_di gc ON o.driver_id = gc.driver_now_id AND gc.ping_time BETWEEN o.delivery_assign_time - INTERVAL '5' MINUTE AND o.delivery_pickup_time
    WHERE   1=1
        -- AND o.order_date    BETWEEN DATE(DATE_PARSE('${D7}','%Y%m%d'))                          AND DATE(DATE_PARSE('${yesterday}','%Y%m%d')) 
        -- AND gc.ping_date    BETWEEN DATE(DATE_PARSE('${D7}','%Y%m%d'))  - INTERVAL '1' DAY      AND DATE(DATE_PARSE('${yesterday}','%Y%m%d'))
)                                                                                                                                                                                      
    
, susp_1 AS 
(
    SELECT
            order_date
        ,   create_time
        ,   order_id
        ,   driver_id
        ,   speed_kmh
        ,   accuracy
        ,   head
        ,   accept_latitude
        ,   accept_longitude
        ,   store_latitude
        ,   store_longitude
        ,   upload_time
        ,   ping_time
        ,   service_type
        ,   latitude
        ,   longitude
    FROM 
    (
        SELECT DISTINCT
                order_date
            ,   create_time
            ,   order_id
            ,   driver_id
            ,   latitude
            ,   longitude
            ,   accept_latitude
            ,   accept_longitude
            ,   store_latitude
            ,   store_longitude
            ,   speed_kmh
            ,   accuracy
            ,   head
            ,   upload_time
            ,   ping_time
            ,   service_type
        FROM final_data
    )
)
                           
, datas AS 
(
    SELECT 
            DATE(create_time) AS order_date
        ,   driver_id
        ,   accept_latitude
        ,   accept_longitude
        ,   COUNT(DISTINCT order_id) AS count_order_lat_long     -- DSYS004
    FROM final_data
    WHERE   (LENGTH(accept_latitude) - STRPOS(accept_latitude, '.')) = 7 
        AND (LENGTH(accept_longitude) - STRPOS(accept_longitude, '.')) = 7
    GROUP BY DATE(create_time), driver_id, accept_latitude, accept_longitude
)

, susp AS 
(
    SELECT DISTINCT
            order_date
        ,   o.order_id
        ,   driver_id
        ,   accept_latitude
        ,   accept_longitude
        ,   store_longitude
        ,   store_latitude
        ,   service_type                                                                
    FROM susp_1 o
    ORDER BY order_date
)
  
, all_fgps AS 
(
    SELECT 
            a.order_date
        ,   a.order_id
        ,   a.driver_id
        ,   a.service_type

        ,   count_order_lat_long                    -- DSYS004
        
        ,   CASE WHEN count_order_lat_long >= 3 THEN 1 ELSE 0 END AS dsys004
    FROM susp a
    LEFT JOIN datas d ON a.driver_id = d.driver_id AND a.order_date = d.order_date AND a.accept_latitude = d.accept_latitude AND a.accept_longitude = d.accept_longitude
    WHERE count_order_lat_long >= 3                                       
    ORDER BY order_date
)

SELECT DISTINCT 
        order_date
    ,   order_id
    ,   driver_id
    ,   service_type

    ,   count_order_lat_long    cnt_004 -- DSYS004

    ,   dsys004
    ,   order_date  grass_date
FROM all_fgps
