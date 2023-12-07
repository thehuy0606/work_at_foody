-- DROP TABLE IF EXISTS    dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__repacked_app_usage;
-- CREATE TABLE            dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__repacked_app_usage WITH (partitioned_by = ARRAY['grass_date']) AS 

DELETE FROM dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__repacked_app_usage WHERE grass_date BETWEEN DATE(DATE_PARSE('${D7}','%Y%m%d')) AND DATE(DATE_PARSE('${yesterday}','%Y%m%d'));
INSERT INTO dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__repacked_app_usage

WITH 
targeted_driver AS 
(
    SELECT
        driver_spe_id
    FROM 
    (
        SELECT DISTINCT 
                a.user_id driver_spe_id
            ,   CASE WHEN sync_event_hit_result = 'B00' THEN TRUE ELSE FALSE END AS is_blocked
            ,   CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(entities, '$.DFPInfoSZ.tags'), 'is_repack_v7')                   THEN TRUE ELSE FALSE END AS is_repack_v7
            ,   CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(entities, '$.DFPInfoSZ.tags'), 'is_repack_v6')                   THEN TRUE ELSE FALSE END AS is_repack_v6
            ,   CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(entities, '$.DFPInfoSZ.tags'), 'is_fake_protocol')               THEN TRUE ELSE FALSE END AS is_fake_protocol
            ,   CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(entities, '$.DFPInfoSZ.tags'), 'is_cinfo_lost')                  THEN TRUE ELSE FALSE END AS is_cinfo_lost
            ,   CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(entities, '$.DFPInfoSZ.tags'), 'is_repack')                      THEN TRUE ELSE FALSE END AS is_repack
            ,   CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(entities, '$.DFPInfoSZ.tags'), 'is_repack_v2')                   THEN TRUE ELSE FALSE END AS is_repack_v2
            ,   CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(entities, '$.DFPInfoSZ.tags'), 'is_repack_v3')                   THEN TRUE ELSE FALSE END AS is_repack_v3
            ,   CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(entities, '$.DFPInfoSZ.tags'), 'is_repack_v4')                   THEN TRUE ELSE FALSE END AS is_repack_v4
            ,   CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(entities, '$.DFPInfoSZ.tags'), 'is_repack_v5')                   THEN TRUE ELSE FALSE END AS is_repack_v5
            ,   CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(entities, '$.DFPInfoSZ.tags'), 'is_lspatch_repack_tag')          THEN TRUE ELSE FALSE END AS is_lspatch_repack_tag
            ,   CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(entities, '$.DFPInfoSZ.tags'), 'is_suspicious_so')               THEN TRUE ELSE FALSE END AS is_suspicious_so
            ,   CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(entities, '$.DFPInfoSZ.tags'), 'is_wrong_pkg_name')              THEN TRUE ELSE FALSE END AS is_wrong_pkg_name
            ,   CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(entities, '$.DFPInfoSZ.tags'), 'is_no_deviceid_match')           THEN TRUE ELSE FALSE END AS is_no_deviceid_match
            ,   CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(entities, '$.DFPInfoSZ.tags'), 'is_fake_df')                     THEN TRUE ELSE FALSE END AS is_fake_df
            ,   CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(entities, '$.DFPInfoSZ.tags'), 'is_status_code_dex_fail')        THEN TRUE ELSE FALSE END AS is_status_code_dex_fail
            ,   CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(entities, '$.DFPInfoSZ.tags'), 'is_status_code_collect_fail')    THEN TRUE ELSE FALSE END AS is_status_code_collect_fail
            ,   CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(entities, '$.DFPInfoSZ.tags'), 'is_status_code_report_fail')     THEN TRUE ELSE FALSE END AS is_status_code_report_fail
            ,   CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(entities, '$.DFPInfoSZ.tags'), 'is_fake_android_so')             THEN TRUE ELSE FALSE END AS is_fake_android_so
            ,   CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(entities, '$.DFPInfoSZ.tags'), 'is_df_length_long')              THEN TRUE ELSE FALSE END AS is_df_length_long
            ,   CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(entities, '$.DFPInfoSZ.tags'), 'is_running_magisk')              THEN TRUE ELSE FALSE END AS is_running_magisk
            ,   CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(entities, '$.DFPInfoSZ.tags'), 'is_running_magisk_v2')           THEN TRUE ELSE FALSE END AS is_running_magisk_v2
            ,   CASE WHEN JSON_ARRAY_CONTAINS(JSON_EXTRACT(entities, '$.DFPInfoSZ.tags'), 'is_faker_edxposed_running')      THEN TRUE ELSE FALSE END AS is_faker_edxposed_running

        FROM    antifraud_region.dwd_evt_rule_engine_all_strategies_exec_log_hi__vn a
        WHERE   DATE(event_local_datetime) BETWEEN DATE(DATE_PARSE('${D7}','%Y%m%d')) AND DATE(DATE_PARSE('${yesterday}','%Y%m%d')) 
            AND event_id = 16
            AND grass_region = 'VN' 
    )
    WHERE 
        NOT     is_blocked
        AND (
                    is_repack_v7
                OR  is_repack_v6
                OR  is_fake_protocol
                OR  is_cinfo_lost
                OR  is_repack
                OR  is_repack_v2
                OR  is_repack_v3
                OR  is_repack_v4
                OR  is_repack_v5
                OR  is_lspatch_repack_tag
                OR  is_suspicious_so
                OR  is_wrong_pkg_name
                OR  is_no_deviceid_match
                OR  is_fake_df
                OR  is_status_code_dex_fail
                OR  is_status_code_collect_fail
                OR  is_status_code_report_fail
                OR  is_fake_android_so
                OR  is_df_length_long
                OR  is_running_magisk
                OR  is_running_magisk_v2
                OR  is_faker_edxposed_running
            )
)

-- SELECT DISTINCT
-- select distinct b.order_date, b.order_id, a.driver_id, 'autoblocks' as tag, b.service_type
-- from targeted_driver as a
-- left join order_raw as b on cast(a.driver_id as varchar)=b.driver_id

SELECT DISTINCT 
        order_date
    ,   order_id
    ,   b.driver_id
    ,   service_type

    ,   1 AS is_proxy_autoblock
    ,   order_date  grass_date
FROM targeted_driver a
JOIN 
(
    SELECT  
            driver_now_id   driver_id
        ,   driver_spe_id
        ,   order_id 
        ,   order_date 
        ,   service_type
    FROM    dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__driver_track_order_di 
    WHERE   grass_date BETWEEN DATE(DATE_PARSE('${D7}','%Y%m%d')) AND DATE(DATE_PARSE('${yesterday}','%Y%m%d')) 
) b ON a.driver_spe_id = b.driver_spe_id

