WITH 
params(period_grp, period, start_date, end_date, days) AS 
    (
        SELECT DISTINCT
                '1. Daily'
            ,   CAST(report_date AS VARCHAR) 
            ,   report_date
            ,   report_date
            ,   CAST(1 AS DOUBLE)
        FROM dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_date_dim 
        WHERE   1=1
            AND report_date BETWEEN DATE('2023-10-20') AND CURRENT_DATE  - INTERVAL '1' DAY
        
        UNION 

        SELECT DISTINCT
                '2. Weekly' 
            ,   CAST(year_week AS VARCHAR) 
            ,   CAST(first_day_of_week AS DATE) 
            ,   IF(DATE_TRUNC('week', report_date) = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1' DAY), CURRENT_DATE - INTERVAL '1' DAY, last_day_of_week) 
            ,   CAST(IF(DATE_TRUNC('week', report_date) = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1' DAY), DAY_OF_WEEK(CURRENT_DATE - INTERVAL '1' DAY),7) AS DOUBLE) 
        FROM dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_date_dim 
        WHERE   1=1
            AND report_date BETWEEN DATE('2023-10-20') AND CURRENT_DATE  - INTERVAL '1' DAY
        
        UNION -- month

        SELECT DISTINCT
                '3. Monthly' 
            ,   CAST(year_month AS VARCHAR) 
            ,   CAST(first_day_of_month AS DATE) 
            ,   IF(DATE_TRUNC('month', report_date) = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' DAY), CURRENT_DATE - INTERVAL '1' DAY, last_day_of_month) 
            ,   CAST(IF(DATE_TRUNC('month', report_date) = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' DAY), DAY_OF_MONTH(CURRENT_DATE - INTERVAL '1' DAY),num_day_in_month) AS DOUBLE)
        FROM dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_date_dim 
        WHERE   1=1 
            AND report_date BETWEEN DATE('2023-10-20') AND CURRENT_DATE - INTERVAL '1' DAY 
    ),  
fgps_daily AS 
    (
        SELECT  order_date 
            ,   IF(behavior LIKE 'Rules_%', 'Rules', 'Proxy') types 
            ,   behavior 
            ,   COUNT(DISTINCT order_id) cnt_order 
            -- ,   COUNT(DISTINCT order_id)*IF(behavior LIKE 'Proxy_%', 0.0033, 1) hit_order 
        FROM    dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__order_tags 
        WHERE   1=1 
            AND behavior != '' AND (behavior LIKE 'Rules_%' OR behavior LIKE 'Proxy_%') 
        GROUP BY 1,2,3  
    )   ,
fgps_agg AS 
    (   
        SELECT  p.period_grp 
            ,   p.period 
            ,   p.days
            ,   f.types 
            ,   f.behavior 
            ,   sum(cnt_order)  cnt_order
            -- ,   IF(f.types = 'Proxy', 0.0033, 1.0000)*sum(cnt_order)  hit_order
            -- ,   sum(hit_order)  hit_order 
        FROM 
            (
                SELECT  order_date 
                    ,   types 
                    ,   behavior 
                    ,   cnt_order 
                    -- ,   IF(types = 'Proxy', 0.0033, 1.0000)*cnt_order   hit_order 
                FROM    fgps_daily 
                UNION   
                SELECT  order_date 
                    ,   types 
                    ,   'All'   behavior 
                    ,   sum(cnt_order)  cnt_order
                    -- ,   IF(types = 'Proxy', 0.0033, 1.0000)*sum(cnt_order)  hit_order 
                FROM    fgps_daily 
                GROUP BY 1,2,3 
                -- UNION   
                -- SELECT  order_date 
                --     ,   'All'   types 
                --     ,   'All'   behavior 
                --     ,   sum(cnt_order)  cnt_order
                --     -- ,   IF(types = 'Proxy', 0.0033, 1.0000)*sum(cnt_order)  hit_order
                -- FROM    fgps_daily 
                -- GROUP BY 1,2,3
            )   f
        INNER JOIN params   p ON f.order_date BETWEEN p.start_date AND p.end_date 
        GROUP BY 1,2,3,4,5 
    )   
    ,
pivot_ords AS 
    (
        SELECT  p.period_grp 
            ,   p.period 
            ,   p.days 
            ,   COUNT(DISTINCT b.order_id) net_order 
        FROM    dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__order_tags b 
        INNER JOIN params   p ON b.order_date BETWEEN p.start_date AND p.end_date 
        WHERE   1=1 
        GROUP BY 1,2,3
        ORDER BY 1, 2 DESC
    )   
    ,
pivot_fgps AS 
    (
        SELECT  p.period_grp
            ,   p.period 
            ,   p.days 
            ,   p.types 
            ,   p.behavior
            ,   p.cnt_order
            ,   o.net_order 
            -- ,   1.000*p.hit_order/o.net_order percent_rule 
            -- ,   p.cnt_order
        FROM        fgps_agg    p
        INNER JOIN  pivot_ords  o ON o.period_grp = p.period_grp AND o.period = p.period AND o.days = p.days
        WHERE   1=1 
    )   
select * from pivot_fgps
