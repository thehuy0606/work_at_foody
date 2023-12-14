with 
sop2 as 
    (
        select  distinct 
                order_date
            ,   order_id 
            ,   driver_id 
            ,   period
            ,   ping_time 
            ,   accuracy 
            ,   is_sop
            ,   if(lead_1_asc = 1 or lead_1_des = 1, 1, 0) is_conti 
        from 
            (
                select  *
                    ,   LEAD(is_sop, 1) OVER(PARTITION BY order_id ORDER BY ping_time ASC ) AS lead_1_asc
                    ,   LEAD(is_sop, 1) OVER(PARTITION BY order_id ORDER BY ping_time DESC) AS lead_1_des
                from 
                    (
                        select  order_date
                            ,   order_id 
                            ,   driver_id 
                            ,   period
                            ,   ping_time 
                            ,   accuracy 
                            ,   if(accuracy <= 1 and period in ('early','end'), 1, 0) is_sop
                        from    dev_vnfdbi_opsndrivers.fgps_hit_proxy_sample 
                        where   1=1
                            -- and accuracy <= 1 
                            -- and period in ('early','end')
                    )
                order by order_date, order_id, ping_time
            )
        where   1=1
            and (lead_1_asc = 1 or lead_1_des = 1)
            and is_sop = 1
    ),
sop3_1 as 
    (
        select  distinct 
                order_date
            ,   order_id 
            ,   driver_id 
            ,   period
            ,   ping_time 
            ,   accuracy 
            ,   is_sop
            ,   if(lead_1_asc = 1 or lead_1_des = 1, 1, 0) is_conti 
        from 
            (
                select  *
                    ,   LEAD(is_sop, 1) OVER(PARTITION BY order_id ORDER BY ping_time ASC ) AS lead_1_asc
                    ,   LEAD(is_sop, 1) OVER(PARTITION BY order_id ORDER BY ping_time DESC) AS lead_1_des
                from 
                    (
                        select  order_date
                            ,   order_id 
                            ,   driver_id 
                            ,   period
                            ,   ping_time 
                            ,   accuracy 
                            ,   if(accuracy <= 5 and period in ('early','end'), 1, 0) is_sop
                        from    dev_vnfdbi_opsndrivers.fgps_hit_proxy_sample 
                    )
                order by order_date, order_id, ping_time
            )
        where   1=1
            and (lead_1_asc = 1 or lead_1_des = 1)
            and is_sop = 1
    ),
sop3_2 as 
    (
        select  distinct 
                order_date
            ,   order_id 
            ,   driver_id 
            ,   period
            ,   ping_time 
            ,   accuracy 
            ,   is_sop
            ,   if(lead_1_asc = 1 or lead_1_des = 1, 1, 0) is_conti 
        from 
            (
                select  *
                    ,   LEAD(is_sop, 1) OVER(PARTITION BY order_id ORDER BY ping_time ASC ) AS lead_1_asc
                    ,   LEAD(is_sop, 1) OVER(PARTITION BY order_id ORDER BY ping_time DESC) AS lead_1_des
                from 
                    (
                        select  order_date
                            ,   order_id 
                            ,   driver_id 
                            ,   period
                            ,   ping_time 
                            ,   accuracy 
                            ,   if(accuracy > 5 and period in ('middle'), 1, 0) is_sop
                        from    dev_vnfdbi_opsndrivers.fgps_hit_proxy_sample 
                    )
                order by order_date, order_id, ping_time
            )
        where   1=1
            and (lead_1_asc = 1 or lead_1_des = 1)
            and is_sop = 1
    ),
sop3 as 
(
    select  a.* 
    from 
         (select distinct order_id from sop3_1) a 
    Join (select distinct order_id from sop3_2) b ON a.order_id = b.order_id
    
)
