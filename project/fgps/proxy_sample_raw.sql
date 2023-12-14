DROP TABLE IF EXISTS    dev_vnfdbi_opsndrivers.fgps_hit_proxy_sample;
CREATE TABLE            dev_vnfdbi_opsndrivers.fgps_hit_proxy_sample as
with 
hit_proxy as 
    (
        select  *
        from dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__order_tags 
        where   behavior like 'Proxy_%'
    ),
dri_proxy as 
    (
        select driver_now_id dr, count(*) hit_cnt from hit_proxy group by 1 order by 2 desc limit 200 
    ),
raw_hit_proxy as 
    (
        select  b.order_date 
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
            ,   b.behavior 
            ,   IF(b.store_latitude = b.accept_latitude AND b.store_longitude = b.accept_longitude, 1, 0) is_store_equal_accept
            ,   b.delivery_assign_time 
            ,   b.delivery_pickup_time 
            ,   b.delivery_delivered_time
        from    hit_proxy b
        join    dri_proxy on b.driver_now_id = dri_proxy.dr 
        limit 200
    ),
result as 
    (
        select  a.*
            ,   case 
                    when b.ping_time BETWEEN a.delivery_assign_time - INTERVAL '5' MINUTE   AND a.delivery_pickup_time                          then 'early'
                    when b.ping_time BETWEEN a.delivery_pickup_time                         AND a.delivery_delivered_time                       then 'middle'
                    when b.ping_time BETWEEN a.delivery_delivered_time                      AND a.delivery_delivered_time - INTERVAL '5' MINUTE then 'end'
                end period 
            ,   b.ping_time 
            ,   b.latitude 
            ,   b.longitude 
            ,   b.speed_kmh 
            ,   b.accuracy 
            ,   b.head 
        from        raw_hit_proxy  a 
        left join   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__driver_track_ping_di b 
            ON      a.driver_id = b.driver_now_id 
                AND b.ping_time BETWEEN a.delivery_assign_time - INTERVAL '5' MINUTE AND a.delivery_delivered_time + INTERVAL '5' MINUTE 
    )
select  distinct * 
from result 
order by 1 desc, 2, 14,17
;
DROP TABLE IF EXISTS    dev_vnfdbi_opsndrivers.fgps_non_proxy_sample;
CREATE TABLE            dev_vnfdbi_opsndrivers.fgps_non_proxy_sample as
with 
hit_proxy as 
    (
        select  *
        from dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__order_tags 
        where   behavior like 'Proxy_%'
    ),
non_proxy as 
    (
        select  *
        from dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__order_tags 
        where   behavior = ''
    ),
dri_proxy as 
    (
        select driver_now_id dr, count(*) hit_cnt from hit_proxy group by 1 order by 2 desc limit 200 
    ),
raw_non_proxy as 
    (
        select  b.order_date 
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
                    ,   b.behavior 
                    ,   IF(b.store_latitude = b.accept_latitude AND b.store_longitude = b.accept_longitude, 1, 0) is_store_equal_accept
                    ,   b.delivery_assign_time 
                    ,   b.delivery_pickup_time 
                    ,   b.delivery_delivered_time
        from    non_proxy b
        where   b.driver_now_id not in (select dr from dri_proxy) 
        limit 200 
    ),
result as 
    (
        select  a.*
            ,   case 
                    when b.ping_time BETWEEN a.delivery_assign_time - INTERVAL '5' MINUTE   AND a.delivery_pickup_time                          then 'early'
                    when b.ping_time BETWEEN a.delivery_pickup_time                         AND a.delivery_delivered_time                       then 'middle'
                    when b.ping_time BETWEEN a.delivery_delivered_time                      AND a.delivery_delivered_time - INTERVAL '5' MINUTE then 'end'
                end period 
            ,   b.ping_time 
            ,   b.latitude 
            ,   b.longitude 
            ,   b.speed_kmh 
            ,   b.accuracy 
            ,   b.head 
        from        raw_non_proxy  a 
        left join   dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__driver_track_ping_di b 
            ON      a.driver_id = b.driver_now_id 
                AND b.ping_time BETWEEN a.delivery_assign_time - INTERVAL '5' MINUTE AND a.delivery_delivered_time + INTERVAL '5' MINUTE 
    )   
select  distinct * 
from result 
order by 1 desc, 2, 14,17
