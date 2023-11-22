WITH 
base AS 
    (
        SELECT  DISTINCT 
                od.order_date 
            ,   od.order_id
            ,   od.driver_now_id 
            ,   od.driver_spe_id 
            ,   od.driver_name 
            ,   od.order_city 
            ,   od.service_type 
            ,   od.store_latitude 
            ,   od.store_longitude 
            ,   od.accept_latitude  
            ,   od.accept_longitude 
            ,   od.delivery_assign_time 
            ,   od.delivery_pickup_time 
            ,   od.delivery_delivered_time
            -- Rule -- Repetitive long decimal digit of accuracy and speed 
            ,   IF(d209.dsys209 IS NOT NULL, d209.dsys209, 0) dsys209 
            ,   IF(d211.dsys211 IS NOT NULL, d211.dsys211, 0) dsys211 
            ,   IF(d212.dsys212 IS NOT NULL, d212.dsys212, 0) dsys212 
            ,   IF(d213.dsys213 IS NOT NULL, d213.dsys213, 0) dsys213 

            -- Rule -- Repetitive same accuracy 
            ,   IF(d109.dsys109 IS NOT NULL, d109.dsys109, 0) dsys109 
            ,   IF(d111.dsys111 IS NOT NULL, d111.dsys111, 0) dsys111
            ,   IF(d112.dsys112 IS NOT NULL, d112.dsys112, 0) dsys112
            ,   IF(d113.dsys113 IS NOT NULL, d113.dsys113, 0) dsys113 

            --  Rule -- Latitude-longitude Jump 
            ,   IF(d006.dsys006 IS NOT NULL, d006.dsys006, 0) dsys006 

            -- Rule -- Repetitive high accuracy 
            ,   IF(d016.dsys016 IS NOT NULL, d016.dsys016, 0) dsys016 
            ,   IF(d017.dsys017 IS NOT NULL, d017.dsys017, 0) dsys017 

            -- Rule -- Repetitive same speed 
            ,   IF(d008.dsys008 IS NOT NULL, d008.dsys008, 0) dsys008 

            -- Rule -- Suspicious latitude-longitude data 
            ,   IF(d010.dsys010 IS NOT NULL, d010.dsys010, 0) dsys010 

            -- Rule -- Same accept latitude-longitude 
            ,   IF(d001.dsys001 IS NOT NULL, d001.dsys001, 0) dsys001 
            ,   IF(d004.dsys004 IS NOT NULL, d004.dsys004, 0) dsys004 

            -- Proxy -- Repetitive long decimal digit of accuracy and speed 
            ,   IF(d214.dsys214 IS NOT NULL, d214.dsys214, 0) dsys214 
            ,   IF(d215.dsys215 IS NOT NULL, d215.dsys215, 0) dsys215  

            -- Proxy -- Repetitive same accuracy
            ,   IF(d114.dsys114 IS NOT NULL, d114.dsys114, 0) dsys114 
            ,   IF(d115.dsys115 IS NOT NULL, d115.dsys115, 0) dsys115 

            -- Proxy -- Repacked App usage 
            ,   IF(dapp.is_proxy_autoblock IS NOT NULL, dapp.is_proxy_autoblock, 0) isproxy 
        FROM        dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__driver_track_order_di od  
        -- Repetitive long decimal digit of accuracy and speed
        LEFT JOIN   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys209 d209 ON od.grass_date = d209.grass_date AND od.order_id = d209.order_id AND od.driver_now_id = d209.driver_id AND od.service_type = d209.service_type
        LEFT JOIN   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys211 d211 ON od.grass_date = d211.grass_date AND od.order_id = d211.order_id AND od.driver_now_id = d211.driver_id AND od.service_type = d211.service_type
        LEFT JOIN   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys212 d212 ON od.grass_date = d212.grass_date AND od.order_id = d212.order_id AND od.driver_now_id = d212.driver_id AND od.service_type = d212.service_type
        LEFT JOIN   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys213 d213 ON od.grass_date = d213.grass_date AND od.order_id = d213.order_id AND od.driver_now_id = d213.driver_id AND od.service_type = d213.service_type
        -- Repetitive same accuracy
        LEFT JOIN   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys109 d109 ON od.grass_date = d109.grass_date AND od.order_id = d109.order_id AND od.driver_now_id = d109.driver_id AND od.service_type = d109.service_type
        LEFT JOIN   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys111 d111 ON od.grass_date = d111.grass_date AND od.order_id = d111.order_id AND od.driver_now_id = d111.driver_id AND od.service_type = d111.service_type
        LEFT JOIN   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys112 d112 ON od.grass_date = d112.grass_date AND od.order_id = d112.order_id AND od.driver_now_id = d112.driver_id AND od.service_type = d112.service_type
        LEFT JOIN   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys113 d113 ON od.grass_date = d113.grass_date AND od.order_id = d113.order_id AND od.driver_now_id = d113.driver_id AND od.service_type = d113.service_type
        -- Latitude-longitude Jump
        LEFT JOIN   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys006 d006 ON od.grass_date = d006.grass_date AND od.order_id = d006.order_id AND od.driver_now_id = d006.driver_id AND od.service_type = d006.service_type
        -- Repetitive high accuracy
        LEFT JOIN   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys016 d016 ON od.grass_date = d016.grass_date AND od.order_id = d016.order_id AND od.driver_now_id = d016.driver_id AND od.service_type = d016.service_type
        LEFT JOIN   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys017 d017 ON od.grass_date = d017.grass_date AND od.order_id = d017.order_id AND od.driver_now_id = d017.driver_id AND od.service_type = d017.service_type
        -- Repetitive same speed 
        LEFT JOIN   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys008 d008 ON od.grass_date = d008.grass_date AND od.order_id = d008.order_id AND od.driver_now_id = d008.driver_id AND od.service_type = d008.service_type
        -- Suspicious latitude-longitude data 
        LEFT JOIN   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys010 d010 ON od.grass_date = d010.grass_date AND od.order_id = d010.order_id AND od.driver_now_id = d010.driver_id AND od.service_type = d010.service_type
        -- Same accept latitude-longitude 
        LEFT JOIN   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys001 d001 ON od.grass_date = d001.grass_date AND od.order_id = d001.order_id AND od.driver_now_id = d001.driver_id AND od.service_type = d001.service_type
        LEFT JOIN   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys004 d004 ON od.grass_date = d004.grass_date AND od.order_id = d004.order_id AND od.driver_now_id = d004.driver_id AND od.service_type = d004.service_type
        -- Repetitive long decimal digit of accuracy and speed 
        LEFT JOIN   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys214 d214 ON od.grass_date = d214.grass_date AND od.order_id = d214.order_id AND od.driver_now_id = d214.driver_id AND od.service_type = d214.service_type
        LEFT JOIN   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys215 d215 ON od.grass_date = d215.grass_date AND od.order_id = d215.order_id AND od.driver_now_id = d215.driver_id AND od.service_type = d215.service_type
        -- Repetitive same accuracy
        LEFT JOIN   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys114 d114 ON od.grass_date = d114.grass_date AND od.order_id = d114.order_id AND od.driver_now_id = d114.driver_id AND od.service_type = d114.service_type
        LEFT JOIN   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys115 d115 ON od.grass_date = d115.grass_date AND od.order_id = d115.order_id AND od.driver_now_id = d115.driver_id AND od.service_type = d115.service_type
        -- Repacked App usage
        LEFT JOIN   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__repacked_app_usage dapp ON od.grass_date = dapp.grass_date AND od.order_id = dapp.order_id AND od.driver_now_id = dapp.driver_id AND od.service_type = dapp.service_type
        WHERE   1=1 
    ),
proxy AS 
    (
        SELECT  b.order_date 
            ,   b.order_id 
            ,   b.driver_now_id driver_id
            ,   b.driver_spe_id 
            ,   b.driver_name 
            ,   b.order_city 
            ,   b.service_type 
            ,   b.store_latitude 
            ,   b.store_longitude 
            ,   b.accept_latitude 
            ,   b.accept_longitude 
            ,   CASE    --  behavior
                    WHEN dsys214 = 1 THEN 'Proxy_Repetitive long decimal digit of accuracy and speed'
                    WHEN dsys215 = 1 THEN 'Proxy_Repetitive long decimal digit of accuracy and speed'
                    WHEN dsys114 = 1 THEN 'Proxy_Repetitive same accuracy'
                    WHEN dsys115 = 1 THEN 'Proxy_Repetitive same accuracy'
                    WHEN isproxy = 1 THEN 'Proxy_Repacked App usage'
                END behavior 
            ,   IF(b.store_latitude = b.accept_latitude AND b.store_longitude = b.accept_longitude, 1, 0) is_store_equal_accept
            ,   b.delivery_assign_time 
            ,   b.delivery_pickup_time 
            ,   b.delivery_delivered_time
        FROM    base b 
        WHERE   1=1
            AND dsys214 + dsys215 + dsys114 + dsys115 + isproxy > 0 
            AND dsys209 + dsys211 + dsys212 + dsys213 + dsys109 + dsys111 + dsys112 + dsys113 + dsys006 + dsys016 + dsys017 + dsys008 + dsys010 + dsys001 + dsys004 = 0
    ),
sample_raw AS 
    (
        SELECT  proxy.* 
            ,   'early' period
            ,   early.ping_time 
            ,   early.latitude 
            ,   early.longitude
            ,   early.speed_kmh 
            ,   early.accuracy
            ,   early.head 
            ,   early.device_id 
        FROM        proxy 
        LEFT JOIN   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__pickup_before early 
                ON  proxy.order_date = early.order_date AND proxy.order_id = early.order_id AND proxy.driver_id = early.driver_id AND proxy.service_type = early.service_type 
        -- ORDER BY 1 DESC,2,14,17

        UNION  

        SELECT  proxy.* 
            ,   'middle' period
            ,   middl.ping_time 
            ,   middl.latitude 
            ,   middl.longitude
            ,   middl.speed_kmh 
            ,   middl.accuracy
            ,   middl.head 
            ,   middl.device_id 
        FROM        proxy 
        LEFT JOIN   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__pickup_after middl 
                ON  proxy.order_date = middl.order_date AND proxy.order_id = middl.order_id AND proxy.driver_id = middl.driver_id AND proxy.service_type = middl.service_type 
        -- ORDER BY 1 DESC,2,14,17

        UNION  

        SELECT  proxy.* 
            ,   'end' period
            ,   ended.ping_time 
            ,   ended.latitude 
            ,   ended.longitude
            ,   ended.speed_kmh 
            ,   ended.accuracy
            ,   ended.head 
            ,   ended.device_id 
        FROM        proxy 
        LEFT JOIN   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__delive_after ended 
                ON  proxy.order_date = ended.order_date AND proxy.order_id = ended.order_id AND proxy.driver_id = ended.driver_id AND proxy.service_type = ended.service_type 
        -- ORDER BY 1 DESC,2,14,17
    )
SELECT  * 
FROM    sample_raw 
ORDER BY 1 DESC,2,14,17
