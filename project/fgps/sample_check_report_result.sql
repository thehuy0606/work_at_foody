with 
sample as 
(
    select  t1.* 
        ,   t2.is_hit 
        ,   t2.types 
    from 
        (
            select distinct order_id, driver_id from dev_vnfdbi_opsndrivers.fgps_hit_proxy_sample 
            UNION ALL 
            select distinct order_id, driver_id from dev_vnfdbi_opsndrivers.fgps_non_proxy_sample 
        )   t1 
    left join 
        (
            select  CAST(order_id as int)   order_id
                ,   CAST(is_hit as int)     is_hit
                ,   if(pic='Huy', 'non-proxy', 'proxy') types 
            from dev_vnfdbi_opsndrivers.bnp_bi_fraud__driver_fgps_manual_check_raw 
        )   t2 on t1.order_id = t2.order_id
)
,manual as 
    (
        select  types 
            ,   count(distinct order_id)    sample_order_cnt 
            ,   count(distinct driver_id)   sample_driver_cnt 
            ,   count(distinct if(is_hit = 1,order_id,  null)) fraud_order_cnt 
            ,   count(distinct if(is_hit = 1,driver_id, null)) fraud_driver_cnt 
            ,   (1.0000*count(distinct if(is_hit = 1,order_id,  null))/count(distinct order_id )) order_sample_fraud_rate 
            ,   (1.0000*count(distinct if(is_hit = 1,driver_id, null))/count(distinct driver_id)) driver_sample_fraud_rate  
        from sample 
        group by 1 
    )
select * from manual
-- ,net_order as 
-- (
--     select  count(distinct if(behavior like 'Proxy_%', order_id, null)) cnt_orders_hit_proxy
--     from    dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__order_tags 
--     where   order_date between date('2023-11-01') and date('2023-11-30') 
-- )


-- select  types
--     ,   count(distinct order_id)    sample_order_cnt 
--     ,   count(distinct driver_id)   sample_driver_cnt 
--     ,   count(distinct if(is_hit = 1,order_id,  null)) fraud_order_cnt 
--     ,   count(distinct if(is_hit = 1,driver_id, null)) fraud_driver_cnt 
--     ,   1.0000*count(distinct if(is_hit = 1,order_id,  null))/count(distinct order_id ) order_sample_fraud_rate 
--     ,   1.0000*count(distinct if(is_hit = 1,driver_id, null))/count(distinct driver_id) driver_sample_fraud_rate 
-- from sample 
-- where   types = 'non-proxy'
-- group by 1 
;




