WITH targeted_driver AS (
    SELECT
        driver_id
    FROM (
        SELECT DISTINCT
            -- DATE(event_local_datetime) AS login_date
            -- , event_local_datetime
            CAST(a.user_id AS VARCHAR) AS driver_id
            , CASE WHEN sync_event_hit_result = 'B00' THEN TRUE ELSE FALSE END AS is_blocked
            , CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(JSON_EXTRACT(ENTITIES, '$[""DFPInfoSZ""]'), '$[""tags""]'), 'is_repack_v7') THEN TRUE ELSE FALSE END AS is_repack_v7
            , CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(JSON_EXTRACT(ENTITIES, '$[""DFPInfoSZ""]'), '$[""tags""]'), 'is_repack_v6') THEN TRUE ELSE FALSE END AS is_repack_v6
            , CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(JSON_EXTRACT(ENTITIES, '$[""DFPInfoSZ""]'), '$[""tags""]'), 'is_fake_protocol') THEN TRUE ELSE FALSE END AS is_fake_protocol
            , CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(JSON_EXTRACT(ENTITIES, '$[""DFPInfoSZ""]'), '$[""tags""]'), 'is_cinfo_lost') THEN TRUE ELSE FALSE END AS is_cinfo_lost
            , CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(JSON_EXTRACT(ENTITIES, '$[""DFPInfoSZ""]'), '$[""tags""]'), 'is_repack') THEN TRUE ELSE FALSE END AS is_repack
            , CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(JSON_EXTRACT(ENTITIES, '$[""DFPInfoSZ""]'), '$[""tags""]'), 'is_repack_v2') THEN TRUE ELSE FALSE END AS is_repack_v2
            , CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(JSON_EXTRACT(ENTITIES, '$[""DFPInfoSZ""]'), '$[""tags""]'), 'is_repack_v3') THEN TRUE ELSE FALSE END AS is_repack_v3
            , CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(JSON_EXTRACT(ENTITIES, '$[""DFPInfoSZ""]'), '$[""tags""]'), 'is_repack_v4') THEN TRUE ELSE FALSE END AS is_repack_v4
            , CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(JSON_EXTRACT(ENTITIES, '$[""DFPInfoSZ""]'), '$[""tags""]'), 'is_repack_v5') THEN TRUE ELSE FALSE END AS is_repack_v5
            , CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(JSON_EXTRACT(ENTITIES, '$[""DFPInfoSZ""]'), '$[""tags""]'), 'is_lspatch_repack_tag') THEN TRUE ELSE FALSE END AS is_lspatch_repack_tag
            , CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(JSON_EXTRACT(ENTITIES, '$[""DFPInfoSZ""]'), '$[""tags""]'), 'is_suspicious_so') THEN TRUE ELSE FALSE END AS is_suspicious_so
            , CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(JSON_EXTRACT(ENTITIES, '$[""DFPInfoSZ""]'), '$[""tags""]'), 'is_wrong_pkg_name') THEN TRUE ELSE FALSE END AS is_wrong_pkg_name
            , CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(JSON_EXTRACT(ENTITIES, '$[""DFPInfoSZ""]'), '$[""tags""]'), 'is_no_deviceid_match') THEN TRUE ELSE FALSE END AS is_no_deviceid_match
            , CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(JSON_EXTRACT(ENTITIES, '$[""DFPInfoSZ""]'), '$[""tags""]'), 'is_fake_df') THEN TRUE ELSE FALSE END AS is_fake_df
            , CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(JSON_EXTRACT(ENTITIES, '$[""DFPInfoSZ""]'), '$[""tags""]'), 'is_status_code_dex_fail') THEN TRUE ELSE FALSE END AS is_status_code_dex_fail
            , CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(JSON_EXTRACT(ENTITIES, '$[""DFPInfoSZ""]'), '$[""tags""]'), 'is_status_code_collect_fail') THEN TRUE ELSE FALSE END AS is_status_code_collect_fail
            , CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(JSON_EXTRACT(ENTITIES, '$[""DFPInfoSZ""]'), '$[""tags""]'), 'is_status_code_report_fail') THEN TRUE ELSE FALSE END AS is_status_code_report_fail
            , CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(JSON_EXTRACT(ENTITIES, '$[""DFPInfoSZ""]'), '$[""tags""]'), 'is_fake_android_so') THEN TRUE ELSE FALSE END AS is_fake_android_so
            , CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(JSON_EXTRACT(ENTITIES, '$[""DFPInfoSZ""]'), '$[""tags""]'), 'is_df_length_long') THEN TRUE ELSE FALSE END AS is_df_length_long
            , CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(JSON_EXTRACT(ENTITIES, '$[""DFPInfoSZ""]'), '$[""tags""]'), 'is_running_magisk') THEN TRUE ELSE FALSE END AS is_running_magisk
            , CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(JSON_EXTRACT(ENTITIES, '$[""DFPInfoSZ""]'), '$[""tags""]'), 'is_running_magisk_v2') THEN TRUE ELSE FALSE END AS is_running_magisk_v2
            , CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(JSON_EXTRACT(ENTITIES, '$[""DFPInfoSZ""]'), '$[""tags""]'), 'is_faker_edxposed_running') THEN TRUE ELSE FALSE END AS is_faker_edxposed_running
        FROM antifraud_region.dwd_evt_rule_engine_all_strategies_exec_log_hi__id a
        WHERE DATE(event_local_datetime) =  ${START_TIME}
            AND event_id = 16
            AND grass_region = 'ID'
    )
    WHERE 
        NOT is_blocked
        AND (is_repack_v7
            OR is_repack_v6
            OR is_fake_protocol
            OR is_cinfo_lost
            OR is_repack
            OR is_repack_v2
            OR is_repack_v3
            OR is_repack_v4
            OR is_repack_v5
            OR is_lspatch_repack_tag
            OR is_suspicious_so
            OR is_wrong_pkg_name
            OR is_no_deviceid_match
            OR is_fake_df
            OR is_status_code_dex_fail
            OR is_status_code_collect_fail
            OR is_status_code_report_fail
            OR is_fake_android_so
            OR is_df_length_long
            OR is_running_magisk
            OR is_running_magisk_v2
            OR is_faker_edxposed_running)
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


-- SELECT DISTINCT
-- select distinct b.order_date, b.order_id, a.driver_id, 'autoblocks' as tag, b.service_type
-- from targeted_driver as a
-- left join order_raw as b on cast(a.driver_id as varchar)=b.driver_id

SELECT DISTINCT 
    order_date
    , order_id
    , a.driver_id
    , service_type

    , 1 AS is_proxy_autoblock
FROM targeted_driver a
    JOIN order_raw b
        ON CAST(a.driver_id AS VARCHAR) = b.driver_id
