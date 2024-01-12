-- OUTPUT TABLE: shopeefood_vn_food_fraud_buyer_szsdk_daily
DROP TABLE IF EXISTS dev_vnfdbi_opsndrivers.shopeefood_vn_food_fraud_buyer_szsdk_daily_v2;
CREATE TABLE dev_vnfdbi_opsndrivers.shopeefood_vn_food_fraud_buyer_szsdk_daily_v2 WITH (partitioned_by = ARRAY ['grass_month']) AS

-- DELETE FROM dev_vnfdbi_opsndrivers.shopeefood_vn_food_fraud_buyer_szsdk_daily 
-- WHERE grass_date BETWEEN date(date_parse('${D3}','%Y%m%d')) AND DATE(date_parse('${yesterday}','%Y%m%d'));

-- INSERT INTO dev_vnfdbi_opsndrivers.shopeefood_vn_food_fraud_buyer_szsdk_daily

WITH
orders_temp AS 
    (
        SELECT 
                a.id AS order_id
            ,   a.delivery_id
            ,   a.create_timestamp
            ,   from_unixtime(a.create_timestamp) AS create_time
            ,   a.user_id AS buyer_id 
            ,   a.merchant_id AS store_id
            ,   a.merchant_id 
            ,   a.shipper_id AS driver_id 
            ,   a.order_status_id AS delivery_status
            ,   a.app_type_id
            ,   sbt.shopee_id
        FROM shopeefood.foody_mart__fact_gross_order_join_detail a 
        LEFT JOIN shopeefood.foody_account_db__shopee_maping_tab__reg_daily_s0_live sbt ON a.user_id = sbt.uid AND status = 1
        WHERE   1=1 
            AND date(from_unixtime(a.create_timestamp)) BETWEEN DATE(date_parse('20230101','%Y%m%d')) AND DATE(date_parse('20240109','%Y%m%d'))
            AND a.order_status_id in (7,9) 
            AND a.grass_region = 'VN'
    ) 
,sz_sdk as 
    (
        select  a.user_id     shopee_id
            ,   CAST(a.event_local_datetime AS TIMESTAMP) date_time
            ,   a.grass_date  
            ,   a.event_local_datetime  date_ts
            ,   JSON_EXTRACT_SCALAR(a.entities, '$.DFPInfoSZ.SecurityDeviceID')  sz_sdk_df 
            ,   JSON_EXTRACT_SCALAR(a.entities, '$.DFPInfoSZ.tags')              sz_sdk_risk_tag
            ,   a.dfpinfosz__platform platform_id
            ,   a.event_name  
            ,   a.event_id 
        from antifraud_region.dwd_evt_rule_engine_all_strategies_exec_log_hi__vn a 
        join orders_temp b ON a.user_id = b.shopee_id AND DATE(b.create_time) = a.grass_date
        where   1=1
            -- AND user_id IN (SELECT DISTINCT TRY_CAST(shopee_id AS INT) FROM orders_temp) ==> thay bằng join 
            and event_id IN (151)
            -- AND grass_date BETWEEN CURRENT_DATE - INTERVAL '4' DAY AND CURRENT_DATE 
            AND grass_date BETWEEN DATE(date_parse('20230101','%Y%m%d')) - INTERVAL '4' DAY AND DATE(date_parse('20240109','%Y%m%d'))
            
            AND JSON_EXTRACT_SCALAR(a.entities, '$.DFPInfoSZ.SecurityDeviceID') IS NOT NULL AND TRIM(JSON_EXTRACT_SCALAR(a.entities, '$.DFPInfoSZ.SecurityDeviceID')) != ''
        order by CAST(event_local_datetime AS TIMESTAMP) desc 
        
    )   
, orders_sdk AS
    (   
        SELECT DISTINCT
            * 
        FROM 
            (
                SELECT 
                        *
                    ,   IF(regexp_like(sz_sdk_risk_tag,'is_plt_hook|is_inline_hook|is_app_multi_open_system|is_app_multi_open_app|
                        is_app_multi_open_system_vmos|is_emulator|is_system_debuggable|is_debugging|is_auto_framework|is_phone_farm|is_gps_modified|
                        is_cloud_phone|is_repack|is_wrong_pkg_name|is_black_rom|is_fake_protocol|is_fake_df|is_no_deviceinfo|is_fake_deviceinfo|
                        is_suspicious_so|is_newdevice_haverisk|is_ochook|is_fishhook|is_risk_app_running_cheatingapp|is_risk_app_running_automation|is_binder_hook|
                        is_signature_api_hook|is_deviceinfo_lost|is_lsposed_running|is_faker_edxposed_running|is_faker_virtualapp_running|is_faker_lsposed_running|is_app_debuggable|is_java_hook|is_running_acc_autojs|is_lspatch_repack') = true 
                        or (platform = 2 and regexp_like(sz_sdk_risk_tag,'is_newdevice_haverisk|is_fake_deviceinfo') = true), 1, 0) is_high_risk_tag
                    ,   row_number() OVER (PARTITION BY buyer_id, order_create_time ORDER BY device_timestamp DESC) AS rk
                FROM 
                    (
                        SELECT
                                a.order_id
                            ,   a.buyer_id
                            ,   a.app_type_id
                            ,   a.create_time order_create_time 
                            ,   coalesce(b.security_device_id,s.sz_sdk_df) sz_sdk_df
                            ,   coalesce(if(b.security_device_id != '' and b.security_device_id is not null,b.platform, null), s.platform_id) platform
                            ,   coalesce( CASE WHEN b.risk_tags = '[]' THEN NULL ELSE b.risk_tags END, CASE WHEN s.sz_sdk_risk_tag = 'null' THEN NULL ELSE s.sz_sdk_risk_tag END) sz_sdk_risk_tag
                            ,   CASE WHEN b.security_device_id IS NULL THEN s.date_time ELSE a.create_time END device_timestamp
                        FROM orders_temp a 
                        LEFT JOIN shopeefood.foody_fraud_detection_db__order_device_risk_info_tab__reg_daily_s0_live b on a.order_id = b.order_id
                        LEFT JOIN sz_sdk s ON TRY_CAST(a.shopee_id AS INT) = s.shopee_id AND TO_UNIXTIME(s.date_ts) BETWEEN a.create_timestamp - 259200 AND a.create_timestamp AND a.app_type_id IN (50,51)
                        WHERE   1=1
                    )
            )
        WHERE rk = 1
    )

SELECT 
        a.order_id 
    ,   a.delivery_id
    ,   a.create_time
    ,   a.buyer_id 
    ,   a.store_id
    ,   a.merchant_id 
    ,   a.driver_id 
    ,   a.delivery_status
    -- ,   a.app_type_id
    ,   case when 1=2 then 'NULL' end as shopee_df
    ,   case when 1=2 then 'NULL' end as tongdun_df 
    ,   d.sz_sdk_df
    ,   d.sz_sdk_risk_tag
    ,   d.is_high_risk_tag
    ,   date(a.create_time) as grass_date
    ,   DATE_FORMAT(date(a.create_time), '%Y-%m') grass_month
FROM orders_temp a
LEFT JOIN orders_sdk d ON a.order_id = d.order_id AND a.buyer_id = d.buyer_id
WHERE   1=1
    and d.sz_sdk_df is not null or trim(d.sz_sdk_df) != ''

