-- ping level
WITH gpscte AS (
    SELECT
        CAST(driver_id AS VARCHAR) AS driver_id
        , latitude
        , longitude
        , speed*3600/1000 AS speed_kmh
        , FROM_UNIXTIME(upload_time/1000-3600) as upload_time
        , CASE 
            WHEN (mtime - upload_time/1000) BETWEEN -5 AND 20 
                THEN FROM_UNIXTIME(upload_time/1000-3600) 
            ELSE FROM_UNIXTIME(mtime-3600) 
            END AS ping_time
        , accuracy
        , head
        , dt
        , CAST(latitude AS VARCHAR) AS latitude1
        , CAST(longitude AS VARCHAR) AS longitude1
    FROM shopeefood.shopeefood_mart_cdm_dwd_id_driver_track_di
    WHERE DATE(CASE WHEN (mtime - upload_time/1000) BETWEEN -5 AND 20 THEN FROM_UNIXTIME(upload_time/1000-3600) ELSE FROM_UNIXTIME(mtime-3600) END) = ${START_TIME}
        OR DATE(FROM_UNIXTIME(mtime-3600)) = ${START_TIME}
)

-- assignment level
, algo_pair AS (
    SELECT DISTINCT
        store_longitude
        , store_latitude
        , driver_latitude AS accept_latitude
        , driver_longitude AS accept_longitude
        , CAST(a.order_id AS VARCHAR) AS order_id
        , CAST(driver_id AS VARCHAR) AS driver_id
    FROM shopeefood.shopeefood_mart_dwd_id_assignment_accept_delivery_da a
        JOIN (
            SELECT
                order_id
                , MAX(dt) AS it
            FROM shopeefood.shopeefood_mart_dwd_id_assignment_accept_delivery_da
            GROUP BY order_id
        ) b 
            ON CAST(a.order_id AS VARCHAR) = CAST(b.order_id AS VARCHAR) 
                AND a.dt = b.it
    WHERE DATE(FROM_UNIXTIME(create_time/1000-3600)) = ${START_TIME}
)

-- order level
, order_raw as (
    SELECT DISTINCT
        CAST(o.order_id AS VARCHAR) AS order_id
        , CAST(o.driver_id AS VARCHAR) AS driver_id
        , create_time
        , DATE(create_time) AS order_date
        , delivery_assign_time
        , delivery_pickup_time
        , delivery_delivered_time
        , 'food' AS service_type
    FROM idpfbi_food.dfs_food_main_dws__order_df o
        JOIN (
            SELECT
                order_id
                , MAX(ingestion_timestamp) AS it
            FROM idpfbi_food.dfs_food_main_dws__order_df
            GROUP BY order_id
        ) b 
            ON CAST(o.order_id AS VARCHAR) = CAST(b.order_id AS VARCHAR)
                AND o.ingestion_timestamp = b.it
    WHERE 1=1
        AND DATE(o.create_time)= ${START_TIME}
        AND order_status IN ('ORDER_DELIVERED','ORDER_COMPLETED')

    UNION ALL

    SELECT DISTINCT
        CAST(o.order_id AS VARCHAR) AS order_id
        , CAST(o.driver_id AS VARCHAR) AS driver_id
        , create_time
        , DATE(create_time) AS order_date
        , assign_time AS delivery_assign_time
        , pickup_time AS delivery_pickup_time
        , delivery_complete_time AS delivery_delivered_time
        , CASE 
            WHEN service_type = 2 
                THEN 'spx_p2p' 
            WHEN service_type = 3 
                THEN 'spx_c2c' 
            END AS service_type
    FROM idpfbi_food.dfs_food_bnp_spx_dwd__spx_order_detail_di o
    WHERE 1=1
        AND DATE(o.create_time) = ${START_TIME}
        AND order_status IN (440,800)
        AND service_type IN (2,3)
)

-- order level
, latlong_order_raw AS (
    -- ping level
    WITH datas AS (
        SELECT
            o.order_id
            , o.driver_id
            , o.create_time
            , o.order_date
            , service_type
            , COALESCE(gc.latitude, ap.accept_latitude) AS accept_latitude
            , COALESCE(gc.longitude,ap.accept_longitude) AS accept_longitude
            , ap.store_longitude
            , ap.store_latitude
            , delivery_assign_time
            , delivery_pickup_time
            , delivery_delivered_time
            , ROW_NUMBER() OVER(
                PARTITION BY o.order_id 
                ORDER BY ping_time DESC, upload_time DESC) AS rk
        FROM order_raw o
        LEFT JOIN algo_pair ap 
            ON o.order_id = ap.order_id 
                AND o.driver_id = ap.driver_id
        LEFT JOIN gpscte gc 
            ON o.driver_id = gc.driver_id 
                AND gc.ping_time <= o.delivery_assign_time 
                AND gc.ping_time > o.delivery_assign_time - INTERVAL '5' MINUTE
    )

    -- order level
    , datas2 AS (
        SELECT
            order_id
            , driver_id
            , create_time
            , order_date
            , service_type
            , CAST(accept_latitude AS VARCHAR) AS accept_latitude1
            , CAST(accept_longitude AS VARCHAR) AS accept_longitude1
            , CAST(store_latitude AS VARCHAR) As store_latitude1
            , CAST(store_longitude AS VARCHAR) AS store_longitude1
            , accept_latitude
            , accept_longitude
            , store_latitude
            , store_longitude
            , delivery_assign_time
            , delivery_pickup_time
            , delivery_delivered_time
        FROM datas
        WHERE rk=1
    )

    -- order level
    SELECT 
        order_id
        , driver_id
        , create_time
        , order_date
        , service_type
        , accept_latitude1
        , accept_longitude1
        , store_latitude1
        , store_longitude1
        , accept_latitude
        , accept_longitude
        , store_latitude
        , store_longitude
        , LENGTH(SPLIT_PART(SPLIT_PART(accept_latitude1, '.', 2), 'E', 1)) AS acc_lat_par1
        , LENGTH(SPLIT_PART(SPLIT_PART(accept_longitude1, '.', 2), 'E', 1)) AS acc_long_par1
        , LENGTH(SPLIT_PART(SPLIT_PART(store_latitude1, '.', 2), 'E', 1)) AS store_lat_par1
        , LENGTH(SPLIT_PART(SPLIT_PART(store_longitude1, '.', 2), 'E', 1)) AS store_long_par1
        , SPLIT_PART(accept_latitude1, 'E', 2) AS acc_lat_par2
        , SPLIT_PART(accept_longitude1, 'E', 2) AS acc_long_par2
        , SPLIT_PART(store_latitude1, 'E', 2) AS store_lat_par2
        , SPLIT_PART(store_longitude1, 'E', 2) AS store_long_par2
        , delivery_assign_time
        , delivery_pickup_time
        , delivery_delivered_time
    FROM datas2
)

, final_data AS (
    SELECT DISTINCT
        o.order_id
        , o.driver_id
        , CASE 
            WHEN accept_latitude1 LIKE '%E%' 
                AND (CAST(acc_lat_par1 AS INTEGER) - CAST(acc_lat_par2 AS INTEGER)) > 7  
                THEN CAST(ROUND(CAST(o.accept_latitude AS DOUBLE), 7) AS VARCHAR)
            WHEN accept_latitude1 NOT LIKE '%E%' 
                AND (LENGTH(accept_latitude1) - STRPOS(accept_latitude1, '.')) > 7 
                THEN CAST(ROUND(CAST(o.accept_latitude AS DOUBLE), 7) AS VARCHAR) 
            ELSE o.accept_latitude1 
            END AS accept_latitude
        , CASE 
            WHEN accept_longitude1 LIKE '%E%' 
                AND (CAST(acc_long_par1 AS INTEGER) - CAST(acc_long_par2 AS INTEGER)) > 7  
                THEN CAST(ROUND(CAST(o.accept_longitude AS DOUBLE), 7) AS VARCHAR)
            WHEN accept_longitude1 NOT LIKE '%E%' 
                AND (LENGTH(accept_longitude1) - STRPOS(accept_longitude1, '.')) > 7 
                THEN CAST(ROUND(CAST(o.accept_longitude AS DOUBLE), 7) AS VARCHAR) 
            ELSE o.accept_longitude1 
            END AS accept_longitude
        , CASE 
            WHEN store_latitude1 LIKE '%E%' 
                AND (CAST(store_lat_par1 AS INTEGER) - CAST(store_lat_par2 as integer)) > 7  
                THEN CAST(ROUND(CAST(o.store_latitude AS DOUBLE), 7) AS VARCHAR)
            WHEN store_latitude1 NOT LIKE '%E%' 
                AND (LENGTH(store_latitude1) - STRPOS(store_latitude1, '.')) > 7 
                THEN CAST(ROUND(CAST(o.store_latitude AS DOUBLE), 7) AS VARCHAR) 
            ELSE o.store_latitude1 
            END AS store_latitude
        , CASE 
            WHEN store_longitude1 LIKE '%E%' 
                AND (CAST(store_long_par1 AS INTEGER) - CAST(store_long_par2 AS INTEGER)) > 7  
                THEN CAST(ROUND(CAST(o.store_longitude AS DOUBLE), 7) AS VARCHAR)
            WHEN store_longitude1 NOT LIKE '%E%' 
                AND (LENGTH(store_longitude1) - STRPOS(store_longitude1, '.')) > 7 
                THEN CAST(ROUND(CAST(o.store_longitude AS DOUBLE), 7) AS VARCHAR) 
            ELSE o.store_longitude1 
            END AS store_longitude
        , o.create_time
        , o.order_date
        , o.delivery_assign_time
        , o.delivery_pickup_time
        , o.delivery_delivered_time
        , o.service_type
        , gc.latitude
        , gc.longitude
        , gc.latitude1
        , gc.longitude1
        , gc.speed_kmh
        , gc.accuracy
        , gc.head
        , gc.upload_time
        , gc.ping_time
        , LENGTH(SPLIT_PART(SPLIT_PART(gc.latitude1, '.', 2), 'E', 1)) AS lat_par1
        , LENGTH(SPLIT_PART(SPLIT_PART(gc.longitude1, '.', 2), 'E', 1)) AS long_par1
        , SPLIT_PART(gc.latitude1, 'E', 2) AS lat_par2
        , SPLIT_PART(gc.longitude1, 'E', 2) AS long_par2
    FROM latlong_order_raw o
        LEFT JOIN gpscte gc 
            ON CAST(o.driver_id AS VARCHAR) = CAST(gc.driver_id AS VARCHAR) 
                AND gc.ping_time BETWEEN o.delivery_assign_time - INTERVAL '5' MINUTE AND o.delivery_pickup_time
)                                                                                                                                                                                      
    
, susp_1 AS (
    SELECT
        order_date
        , create_time
        , order_id
        , driver_id
        , speed_kmh
        , accuracy
        , head
        , accept_latitude
        , accept_longitude
        , store_latitude
        , store_longitude
        , upload_time
        , ping_time
        , service_type
        , latitude
        , longitude
    FROM (
        SELECT DISTINCT
            order_date
            , create_time
            , order_id
            , driver_id
            , latitude
            , longitude
            , accept_latitude
            , accept_longitude
            , store_latitude
            , store_longitude
            , speed_kmh
            , accuracy
            , head
            , upload_time
            , ping_time
            , service_type
        FROM final_data
    )
)
                           
, datas AS (
    SELECT 
        DATE(create_time) AS order_date
        , driver_id
        , accept_latitude
        , accept_longitude
        , COUNT(DISTINCT order_id) AS count_order_lat_long     -- DSYS004
    FROM final_data
    WHERE (LENGTH(accept_latitude) - STRPOS(accept_latitude, '.')) = 7 
        AND (LENGTH(accept_longitude) - STRPOS(accept_longitude, '.')) = 7
    GROUP BY DATE(create_time), driver_id, accept_latitude, accept_longitude
)

, susp AS (
    SELECT DISTINCT
        order_date
        , o.order_id
        , driver_id
        , accept_latitude
        , accept_longitude
        , store_longitude
        , store_latitude
        , service_type                                                                
    FROM susp_1 o
    ORDER BY order_date
)
  
, all_fgps AS (
    SELECT 
        a.order_date
        , a.order_id
        , a.driver_id
        , a.service_type

        , count_order_lat_long                    -- DSYS004
        
        , CASE 
            WHEN count_order_lat_long >= 3 
                THEN 1 
            ELSE 0 
            END AS dsys004
    FROM susp a
        LEFT JOIN datas d 
            ON a.driver_id = d.driver_id 
                AND a.order_date = d.order_date 
                AND a.accept_latitude = d.accept_latitude 
                AND a.accept_longitude = d.accept_longitude
    WHERE count_order_lat_long >= 3                                       
    ORDER BY order_date
)

SELECT DISTINCT 
    order_date
    , order_id
    , driver_id
    , service_type

    , count_order_lat_long AS cnt_004                    -- DSYS004

    , dsys004
FROM all_fgps
