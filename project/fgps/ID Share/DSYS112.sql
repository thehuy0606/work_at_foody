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
        , gc.accuracy
        , gc.upload_time
        , gc.ping_time
        , FLOOR(gc.accuracy) AS round_accuracy
        -- , MOD(gc.accuracy*10000, 1) AS modacc
        , LENGTH(SPLIT_PART(SPLIT_PART(SPLIT_PART(CAST(accuracy AS VARCHAR), '.', 2), '0E', 1), 'E', 1)) + CAST(SPLIT_PART(CAST(accuracy AS VARCHAR), 'E', 2) AS BIGINT) * -1 AS acc_digit
        , o.service_type
        -- , o.delivery_assign_time
        -- , o.delivery_pickup_time
        -- , o.delivery_delivered_time
        -- , gc.latitude
        -- , gc.longitude
        -- , gc.latitude1
        -- , gc.longitude1
        -- , gc.speed_kmh
    FROM order_raw o
        LEFT JOIN gpscte gc 
            ON CAST(o.driver_id AS VARCHAR) = CAST(gc.driver_id AS VARCHAR) 
                AND gc.ping_time BETWEEN o.delivery_assign_time - INTERVAL '5' MINUTE AND o.delivery_pickup_time
)          

, final_data_middle AS (
    SELECT DISTINCT
        o.order_id
        , o.driver_id
        , o.create_time
        , o.order_date
        , gc.accuracy
        , gc.upload_time
        , gc.ping_time
        , FLOOR(gc.accuracy) AS round_accuracy
        -- , MOD(gc.accuracy*10000, 1) AS modacc
        , LENGTH(SPLIT_PART(SPLIT_PART(SPLIT_PART(CAST(accuracy AS VARCHAR), '.', 2), '0E', 1), 'E', 1)) + CAST(SPLIT_PART(CAST(accuracy AS VARCHAR), 'E', 2) AS BIGINT) * -1 AS acc_digit
        , o.service_type
        -- , o.delivery_assign_time
        -- , o.delivery_pickup_time
        -- , o.delivery_delivered_time
        -- , gc.latitude
        -- , gc.longitude
        -- , gc.latitude1
        -- , gc.longitude1
        -- , gc.speed_kmh
    FROM order_raw o
        LEFT JOIN gpscte gc 
            ON CAST(o.driver_id AS VARCHAR) = CAST(gc.driver_id AS VARCHAR) 
                AND gc.ping_time BETWEEN o.delivery_pickup_time AND o.delivery_delivered_time
)          

, susp_1 AS (
    SELECT
        order_date
        , create_time
        , order_id
        , driver_id
        , accuracy            
        , upload_time 
        , service_type
        , LEAD(count_acc_1, 5) 
            OVER(
                PARTITION BY order_id 
                ORDER BY ping_time, upload_time
            ) AS lead_5_ping_acc_1
        , count_acc_1
    FROM (
        SELECT DISTINCT
            order_date
            , create_time
            , order_id
            , driver_id
            , accuracy
            , upload_time
            , ping_time
            , service_type
            , COUNT(CASE 
                WHEN round_accuracy = 1 
                    -- AND modacc > 0 
                    AND acc_digit >= 5
                    THEN order_id 
                END) 
                OVER(
                    PARTITION BY order_id 
                    ORDER BY ping_time, upload_time
                ) AS count_acc_1
        FROM final_data
    )
)

-- SAFETY NET MIDDLE ACCURACY
-- Exclude orders with >=1 ping w/ acc >= 5 digit in the middle
--  START
, repet_ping_middle AS (
    SELECT DISTINCT 
        order_id
    FROM final_data_middle
    -- WHERE modacc > 0
    WHERE acc_digit >= 5
)  

-- order level
, susp AS (
    SELECT 
        order_date
        , o.order_id
        , driver_id
        , service_type
        , IF(
            COUNT(
                CASE 
                    WHEN (lead_5_ping_acc_1 - count_acc_1 = 5) 
                        AND b.order_id IS NULL 
                        THEN o.order_id 
                    END
                ) >= 1 
            , TRUE, FALSE) AS repet_acc_1
    FROM susp_1 o
    LEFT JOIN repet_ping_middle b 
        ON o.order_id = b.order_id
    GROUP BY order_date, o.order_id, driver_id, service_type
    ORDER BY order_date
  )
-- END

-- driver daily level
, datas2 AS (
    SELECT 
        order_date
        , driver_id
        , COUNT(
            DISTINCT CASE 
                WHEN repet_acc_1
                    THEN order_id 
                END) AS count_repet_acc_1
    FROM susp a
    GROUP BY order_date, driver_id
)
          
, all_fgps AS (
    SELECT 
        a.*
        , count_repet_acc_1

        , CASE 
            WHEN repet_acc_1
                AND count_repet_acc_1 >= 1
                THEN 1 
            ELSE 0 
            END AS dsys112
    FROM susp a
        LEFT JOIN datas2 b 
            ON a.order_date = b.order_date 
                AND a.driver_id=b.driver_id                                                                                                                                                 
    WHERE (repet_acc_1 AND count_repet_acc_1 >= 1)  
    ORDER BY order_date
)

SELECT 
    order_date
    , order_id
    , driver_id
    , service_type

    , repet_acc_1 AS is_112                              -- DSYS112
    , count_repet_acc_1 AS cnt_112                       -- DSYS112

    , dsys112
FROM all_fgps
