DROP TABLE IF EXISTS    dev_vnfdbi_opsndrivers.fgps_hit_proxy_sample;
CREATE TABLE            dev_vnfdbi_opsndrivers.fgps_hit_proxy_sample as
with 
hit_proxy as 
    (
        select  *
        from dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__order_tags 
        where   behavior like 'Proxy_%'
            and order_date between date('2023-11-01') and date('2023-11-30')
    )
,raw_hit_proxy as 
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
            ,   b.delivery_assign_time 
            ,   b.delivery_pickup_time 
            ,   b.delivery_delivered_time
        from    hit_proxy b 
    )
,result as 
    (
        select  distinct 
                a.*
            ,   case 
                    when b.ping_time BETWEEN a.delivery_assign_time - INTERVAL '5' MINUTE   AND a.delivery_pickup_time                          then 'early'
                    when b.ping_time BETWEEN a.delivery_pickup_time                         AND a.delivery_delivered_time                       then 'middle'
                    when b.ping_time BETWEEN a.delivery_delivered_time                      AND a.delivery_delivered_time + INTERVAL '5' MINUTE then 'end'
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
,agg0 as 
(
    select  order_id 
        ,   behavior
        ,   count(distinct period) cnt_period
    from    result 
    group by 1,2 
    having  count(distinct period) = 3
)   
,lit as 
(
    (select * from agg0 where behavior = 'Proxy_Repetitive long decimal digit of accuracy and speed' limit 200)
    union 
    (select * from agg0 where behavior = 'Proxy_Repacked App usage' limit 200)
    union 
    (select * from agg0 where behavior = 'Proxy_Repetitive same accuracy' limit 200)
)
,report as 
(
    select  a.* 
    from result a
    join lit b on a.order_id = b.order_id and a.behavior = b.behavior  
    order by 1 desc, 2, 14,17
)
select * from report
-- select order_id 
--     ,   count(distinct period) cnt 
-- from report
-- group by 1 
-- having count(distinct period) < 3
;
DROP TABLE IF EXISTS    dev_vnfdbi_opsndrivers.fgps_non_proxy_sample;
CREATE TABLE            dev_vnfdbi_opsndrivers.fgps_non_proxy_sample as
with 
non_proxy as 
    (
        select  *
        from dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__order_tags 
        where   behavior = ''
            and order_date between date('2023-11-01') and date('2023-11-30')
            and driver_now_id not in (select  distinct driver_now_id from dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__order_tags where   behavior != '')
    )
,raw_non_proxy as 
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
            ,   b.delivery_assign_time 
            ,   b.delivery_pickup_time 
            ,   b.delivery_delivered_time
        from    non_proxy b 
    )
,result as 
    (
        select  distinct 
                a.*
            ,   case 
                    when b.ping_time BETWEEN a.delivery_assign_time - INTERVAL '5' MINUTE   AND a.delivery_pickup_time                          then 'early'
                    when b.ping_time BETWEEN a.delivery_pickup_time                         AND a.delivery_delivered_time                       then 'middle'
                    when b.ping_time BETWEEN a.delivery_delivered_time                      AND a.delivery_delivered_time + INTERVAL '5' MINUTE then 'end'
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
,agg0 as 
    (
        select  order_id 
            ,   behavior
            ,   count(distinct period) cnt_period
        from    result 
        group by 1,2 
        having  count(distinct period) = 3 
        limit 200
    )   
,report as 
(
    select  a.* 
    from result a
    join agg0 b on a.order_id = b.order_id 
    order by 1 desc, 2, 14,17
)
select * from report
-- select order_id 
--     ,   count(distinct period) cnt 
-- from report
-- group by 1 
-- having count(distinct period) < 3
