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
        
    ),
sop4 as 
    (
        select  distinct 
                order_date
            ,   order_id 
            ,   driver_id 
            ,   period
            ,   ping_time 
            ,   accuracy 
            ,   speed_kmh
            -- ,   is_sop
            ,   if(lead_next = 1 or lead_prev = 1, 1, 0) is_conti 
        from 
            (
                select  *
                    ,   LEAD(speed_kmh, 1) OVER(PARTITION BY order_id ORDER BY ping_time ASC ) lead_1_asc 
                    ,   LEAD(speed_kmh, 1) OVER(PARTITION BY order_id ORDER BY ping_time DESC) lead_1_desc 
                    ,   IF(speed_kmh = LEAD(speed_kmh, 1) OVER(PARTITION BY order_id ORDER BY ping_time ASC ),1,0) AS lead_next 
                    ,   IF(speed_kmh = LEAD(speed_kmh, 1) OVER(PARTITION BY order_id ORDER BY ping_time DESC),1,0) AS lead_prev 
                from 
                    (
                        select  order_date
                            ,   order_id 
                            ,   driver_id 
                            ,   period
                            ,   ping_time 
                            ,   accuracy 
                            ,   speed_kmh
                            ,   if(accuracy <= 5 and period in ('early'), 1, 0) is_sop
                        from    dev_vnfdbi_opsndrivers.fgps_hit_proxy_sample 
                        where   1=1
                            and accuracy <= 5
                            and period in ('early')
                    )
                where   is_sop = 1
                order by order_date, order_id, ping_time
            )
        where   1=1
            and (lead_next = 1 or lead_prev = 1) 
    ),
sop5 as 
    (
        select  distinct 
                order_date
            ,   order_id 
            ,   driver_id 
            ,   period
            ,   ping_time 
            ,   accuracy 
            ,   latitude
            ,   longitude
            ,   if(lead_next = 1 or lead_prev = 1, 1, 0) is_conti 
        from 
            (
                select  *
                    ,   LEAD(latitude , 1) OVER(PARTITION BY order_id ORDER BY ping_time ASC ) next_latitude 
                    ,   LEAD(longitude, 1) OVER(PARTITION BY order_id ORDER BY ping_time ASC ) next_longitude 
                    ,   LEAD(latitude, 1) OVER(PARTITION BY order_id ORDER BY ping_time DESC)  prev_latitude 
                    ,   LEAD(longitude, 1) OVER(PARTITION BY order_id ORDER BY ping_time DESC) prev_longitude 
                    ,   IF( (latitude  = LEAD(latitude , 1) OVER(PARTITION BY order_id ORDER BY ping_time ASC )) AND 
                            (longitude = LEAD(longitude, 1) OVER(PARTITION BY order_id ORDER BY ping_time ASC ))
                            ,1,0
                        ) AS lead_next 
                    ,   IF( (latitude  = LEAD(latitude , 1) OVER(PARTITION BY order_id ORDER BY ping_time DESC)) AND 
                            (longitude = LEAD(longitude, 1) OVER(PARTITION BY order_id ORDER BY ping_time DESC))
                            ,1,0
                        ) AS lead_prev 
                    -- ,   IF(speed_kmh = LEAD(speed_kmh, 1) OVER(PARTITION BY order_id ORDER BY ping_time ASC ),1,0) AS lead_next 
                    -- ,   IF(speed_kmh = LEAD(speed_kmh, 1) OVER(PARTITION BY order_id ORDER BY ping_time DESC),1,0) AS lead_prev 
                from 
                    (
                        select  order_date
                            ,   order_id 
                            ,   driver_id 
                            ,   period
                            ,   ping_time 
                            ,   accuracy 
                            ,   latitude
                            ,   longitude
                            ,   if(accuracy <= 5 and period in ('early'), 1, 0) is_sop
                        from    dev_vnfdbi_opsndrivers.fgps_hit_proxy_sample 
                        where   1=1
                            and accuracy <= 5
                            and period in ('early')
                    )
                where   is_sop = 1
                order by order_date, order_id, ping_time
            )
        where   1=1
            and (lead_next = 1 or lead_prev = 1) 
    ),
sop6 as 
    (
        select  distinct 
                order_date
            ,   order_id 
            ,   driver_id 
            ,   period
            ,   ping_time 
            ,   accuracy 
            ,   latitude
            ,   longitude
            ,   if(lead_next = 1 or lead_prev = 1, 1, 0) is_conti 
        from 
            (
                select  *
                    ,   LEAD(latitude , 1) OVER(PARTITION BY order_id ORDER BY ping_time ASC ) next_latitude 
                    ,   LEAD(longitude, 1) OVER(PARTITION BY order_id ORDER BY ping_time ASC ) next_longitude 
                    ,   LEAD(latitude, 1) OVER(PARTITION BY order_id ORDER BY ping_time DESC)  prev_latitude 
                    ,   LEAD(longitude, 1) OVER(PARTITION BY order_id ORDER BY ping_time DESC) prev_longitude 
                    ,   IF( (latitude  = LEAD(latitude , 1) OVER(PARTITION BY order_id ORDER BY ping_time ASC )) AND 
                            (longitude = LEAD(longitude, 1) OVER(PARTITION BY order_id ORDER BY ping_time ASC ))
                            ,1,0
                        ) AS lead_next 
                    ,   IF( (latitude  = LEAD(latitude , 1) OVER(PARTITION BY order_id ORDER BY ping_time DESC)) AND 
                            (longitude = LEAD(longitude, 1) OVER(PARTITION BY order_id ORDER BY ping_time DESC))
                            ,1,0
                        ) AS lead_prev 
                    -- ,   IF(speed_kmh = LEAD(speed_kmh, 1) OVER(PARTITION BY order_id ORDER BY ping_time ASC ),1,0) AS lead_next 
                    -- ,   IF(speed_kmh = LEAD(speed_kmh, 1) OVER(PARTITION BY order_id ORDER BY ping_time DESC),1,0) AS lead_prev 
                from 
                    (
                        select  order_date
                            ,   order_id 
                            ,   driver_id 
                            ,   period
                            ,   ping_time 
                            ,   accuracy 
                            ,   latitude
                            ,   longitude
                            -- ,   if(accuracy <= 5 and period in ('early'), 1, 0) is_sop
                        from    dev_vnfdbi_opsndrivers.fgps_hit_proxy_sample 
                        where   1=1
                            -- and accuracy <= 5
                            -- and period in ('early')
                    )
                -- where   is_sop = 1 
                order by order_date, order_id, ping_time
            )
        where   1=1
            and (lead_next = 1 or lead_prev = 1) 
    ),
sop7 as 
    (
        select  distinct 
                order_date
            ,   order_id 
            ,   driver_id 
            ,   period
            ,   ping_time 
            ,   accuracy 
            ,   latitude
            ,   longitude
            ,   speed_kmh
            ,   if(lead_next = 1 or lead_prev = 1, 1, 0) is_conti 
        from 
            (
                select  distinct
                        *
                    ,   LEAD(speed_kmh, 1) OVER(PARTITION BY order_id ORDER BY ping_time ASC ) lead_1_asc 
                    ,   LEAD(speed_kmh, 1) OVER(PARTITION BY order_id ORDER BY ping_time DESC) lead_1_desc 
                    ,   IF(speed_kmh = LEAD(speed_kmh, 1) OVER(PARTITION BY order_id ORDER BY ping_time ASC ),1,0) AS lead_next 
                    ,   IF(speed_kmh = LEAD(speed_kmh, 1) OVER(PARTITION BY order_id ORDER BY ping_time DESC),1,0) AS lead_prev 
                from 
                    (
                        select  distinct 
                                order_date
                            ,   order_id 
                            ,   driver_id 
                            ,   period
                            ,   ping_time 
                            ,   accuracy 
                            ,   latitude
                            ,   longitude
                            ,   speed_kmh
                            ,   if(speed_kmh in (0.36,0.72,3.6,7.2,10.8), 1, 0) is_sop 
                        from    dev_vnfdbi_opsndrivers.fgps_hit_proxy_sample 
                        where   1=1
                            -- and accuracy <= 5
                            and period in ('early','end')
                    )
                where   is_sop = 1
                order by order_date, order_id, ping_time
            )
        where   1=1
            and (lead_next = 1 or lead_prev = 1) 
    ),
sop8 as 
    (
        select  distinct 
                order_date
            ,   order_id 
            ,   driver_id 
            ,   period
            ,   ping_time 
            ,   accuracy 
            ,   latitude
            ,   longitude
            ,   speed_kmh
            ,   if(lead_next = 1 or lead_prev = 1, 1, 0) is_conti 
        from 
            (
                select  distinct
                        *
                    ,   LEAD(latitude , 1) OVER(PARTITION BY order_id ORDER BY ping_time ASC ) next_latitude 
                    ,   LEAD(longitude, 1) OVER(PARTITION BY order_id ORDER BY ping_time ASC ) next_longitude 
                    ,   LEAD(latitude, 1) OVER(PARTITION BY order_id ORDER BY ping_time DESC)  prev_latitude 
                    ,   LEAD(longitude, 1) OVER(PARTITION BY order_id ORDER BY ping_time DESC) prev_longitude 
                    ,   IF( (latitude  = LEAD(latitude , 1) OVER(PARTITION BY order_id ORDER BY ping_time ASC )) AND 
                            (longitude = LEAD(longitude, 1) OVER(PARTITION BY order_id ORDER BY ping_time ASC ))
                            ,1,0
                        ) AS lead_next 
                    ,   IF( (latitude  = LEAD(latitude , 1) OVER(PARTITION BY order_id ORDER BY ping_time DESC)) AND 
                            (longitude = LEAD(longitude, 1) OVER(PARTITION BY order_id ORDER BY ping_time DESC))
                            ,1,0
                        ) AS lead_prev 
                from 
                    (
                        select  distinct 
                                order_date
                            ,   order_id 
                            ,   driver_id 
                            ,   period
                            ,   ping_time 
                            ,   accuracy 
                            ,   latitude
                            ,   longitude
                            ,   speed_kmh
                            ,   if(speed_kmh in (0.36,3.6), 1, 0) is_sop 
                        from    dev_vnfdbi_opsndrivers.fgps_hit_proxy_sample 
                        where   1=1
                            and accuracy = 5
                            and head = 4
                            and period in ('early','end')
                    )
                where   is_sop = 1
                order by order_date, order_id, ping_time
            )
        where   1=1
            and (lead_next = 1 or lead_prev = 1) 
    )
select  * 
from    dev_vnfdbi_opsndrivers.fgps_hit_proxy_sample a
left join (select distinct order_id from sop2) s2 on a.order_id = s2.order_id 
where   s2.order_id is not null 
