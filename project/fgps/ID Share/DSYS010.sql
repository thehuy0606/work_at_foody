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
        , LENGTH(SPLIT_PART(SPLIT_PART(gc.latitude1, '.', 2), 'E', 1)) AS lat_par1
        , LENGTH(SPLIT_PART(SPLIT_PART(gc.longitude1, '.', 2), 'E', 1)) AS long_par1
        , SPLIT_PART(gc.latitude1, 'E', 2) AS lat_par2
        , SPLIT_PART(gc.longitude1, 'E', 2) AS long_par2
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
        , LEAD(count_more_than_7, 5) OVER(
            PARTITION BY order_id 
            ORDER BY ping_time, upload_time
            ) AS lead_5
        , count_more_than_7
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
            , COUNT(
                CASE
                    WHEN latitude1 LIKE '%E%' 
                        AND (CAST(lat_par1 AS INTEGER) - CAST(lat_par2 AS INTEGER)) > 7 
                        AND longitude1 LIKE '%E%' 
                        AND (CAST(long_par1 AS INTEGER) - CAST(long_par2 AS INTEGER)) > 7 
                        THEN order_id
                    WHEN latitude1 NOT LIKE '%E%' 
                        AND (LENGTH(latitude1) - STRPOS(latitude1, '.')) > 7  
                        AND longitude1 NOT LIKE '%E%' 
                        AND (length(longitude1) - strpos(longitude1, '.')) > 7 
                        THEN order_id 
                    END) 
                OVER(
                    PARTITION BY order_id 
                    ORDER BY ping_time, upload_time
                ) AS count_more_than_7
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
                    WHEN (lead_5 - count_more_than_7 = 5) 
                        THEN o.order_id 
                    END
            ) >= 1 
            , TRUE, FALSE) AS repet_ping     -- DSYS010
                                                                                                                                     
    FROM susp_1 o                                                              
    GROUP BY 1,2,3,4
    ORDER BY order_date
)
  
, datas2 AS (
    SELECT 
        order_date
        , driver_id
        , COUNT(DISTINCT CASE WHEN repet_ping then order_id end) as count_repet_ping     -- DSYS010                                                                       
    FROM susp a
    GROUP BY order_date, driver_id)

, all_fgps AS (
    SELECT 
        a.order_date
        , a.order_id
        , a.driver_id
        , a.service_type

        , repet_ping                              -- DSYS010
        , count_repet_ping                        -- DSYS010
        
        , CASE 
            WHEN repet_ping 
                AND count_repet_ping >= 1 
                THEN 1 
            ELSE 0 
            END AS dsys010                                                                          
    FROM susp a
        LEFT JOIN datas2 b 
            ON a.order_date = b.order_date 
                AND a.driver_id = b.driver_id
    WHERE (repet_ping AND count_repet_ping >= 1)                                            
    ORDER BY order_date
)

SELECT DISTINCT 
    order_date
    , order_id
    , driver_id
    , service_type

    , repet_ping AS is_010                               -- DSYS010
    , count_repet_ping AS cnt_010                        -- DSYS010

    , dsys010
FROM all_fgps
