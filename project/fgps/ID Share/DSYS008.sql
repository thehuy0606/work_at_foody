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

, final_data AS (
    SELECT DISTINCT
        o.order_id
        , o.driver_id
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
    FROM order_raw o
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
            , speed_kmh
            , accuracy
            , head
            , upload_time
            , ping_time
            , service_type
        FROM final_data
    )
)

, susp AS (
    SELECT 
        order_date
        , o.order_id
        , driver_id
        , service_type
        , IF(
            COUNT(
                CASE 
                    WHEN speed_kmh IN (0.36, 3.6) 
                        THEN o.order_id 
                    END
            ) >= 3 
            OR COUNT(
                CASE 
                    WHEN speed_kmh IN (0.72, 7.2) 
                        THEN o.order_id 
                    END
            ) >= 3 
            OR COUNT(
                CASE 
                    WHEN speed_kmh IN (10.8) 
                        THEN o.order_id 
                    END
            ) >= 3
            , TRUE, FALSE) AS repetitive_susp_speed     -- DSYS008                                                                               
    FROM susp_1 o                                                              
    GROUP BY 1,2,3,4
    ORDER BY order_date
)
  
, datas2 AS (
    SELECT 
        order_date
        , driver_id
        , COUNT(DISTINCT CASE WHEN repetitive_susp_speed then order_id end ) as count_order_susp_speed     -- DSYS008
    FROM susp a
    GROUP BY order_date, driver_id)

, all_fgps AS (
    SELECT 
        a.order_date
        , a.order_id
        , a.driver_id
        , a.service_type

        , repetitive_susp_speed                   -- DSYS008
        , count_order_susp_speed                  -- DSYS008
        
        , CASE 
            WHEN repetitive_susp_speed 
                AND count_order_susp_speed >= 1  
                THEN 1 
            ELSE 0 
            END AS dsys008
    FROM susp a
        LEFT JOIN datas2 b 
            ON a.order_date = b.order_date 
                AND a.driver_id = b.driver_id
    WHERE (repetitive_susp_speed AND count_order_susp_speed >= 1)
    ORDER BY order_date
)

SELECT DISTINCT 
    order_date
    , order_id
    , driver_id
    , service_type

    , repetitive_susp_speed AS is_008                    -- DSYS008
    , count_order_susp_speed AS cnt_008                  -- DSYS008

    , dsys008
FROM all_fgps
