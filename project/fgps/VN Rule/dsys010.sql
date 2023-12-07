DROP TABLE IF EXISTS    dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys010;
CREATE TABLE            dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys010 --WITH (partitioned_by = ARRAY['grass_date']) AS 
AS 
-- DELETE FROM dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys010 WHERE grass_date BETWEEN DATE(DATE_PARSE('${D7}','%Y%m%d')) AND DATE(DATE_PARSE('${yesterday}','%Y%m%d'));
-- INSERT INTO dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys010 

WITH 

susp_1 AS 
(
    SELECT
            order_date
        ,   create_time
        ,   order_id
        ,   driver_id
        ,   speed_kmh
        ,   accuracy
        ,   head
        ,   upload_time
        ,   ping_time
        ,   service_type
        ,   LEAD(count_more_than_7, 5) OVER(PARTITION BY order_id ORDER BY ping_time, upload_time) AS lead_5
        ,   count_more_than_7
    FROM 
    (
        SELECT DISTINCT
                order_date
            ,   create_time
            ,   order_id
            ,   driver_id
            ,   latitude
            ,   longitude
            ,   speed_kmh
            ,   accuracy
            ,   head
            ,   upload_time
            ,   ping_time
            ,   service_type
            ,   COUNT(  CASE 
                        WHEN    latitude1  LIKE '%E%' AND (CAST(lat_par1  AS INTEGER) - CAST(lat_par2  AS INTEGER)) > 7 AND 
                                longitude1 LIKE '%E%' AND (CAST(long_par1 AS INTEGER) - CAST(long_par2 AS INTEGER)) > 7 THEN order_id
                        WHEN    latitude1  NOT LIKE '%E%' AND (LENGTH(latitude1)  - STRPOS(latitude1 , '.')) > 7 AND 
                                longitude1 NOT LIKE '%E%' AND (LENGTH(longitude1) - STRPOS(longitude1, '.')) > 7 THEN order_id 
                        END
                    ) OVER(PARTITION BY order_id ORDER BY ping_time, upload_time) AS count_more_than_7
        FROM dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__pickup_before 
        WHERE   1=1
            -- AND grass_date BETWEEN DATE(DATE_PARSE('${D7}','%Y%m%d')) AND DATE(DATE_PARSE('${yesterday}','%Y%m%d'))
    )
)

, susp AS 
(
    SELECT 
            order_date
        ,   o.order_id
        ,   driver_id
        ,   service_type
        ,   IF(COUNT(CASE WHEN (lead_5 - count_more_than_7 = 5) THEN o.order_id END) >= 1, TRUE, FALSE) AS repet_ping   -- DSYS010                                                                                                          
    FROM susp_1 o                                                              
    GROUP BY 1,2,3,4
    ORDER BY order_date
)
, datas2 AS 
(
    SELECT 
            order_date
        ,   driver_id
        ,   COUNT(DISTINCT CASE WHEN repet_ping then order_id end)  count_repet_ping    -- DSYS010 
    FROM susp a
    GROUP BY order_date, driver_id
)

, all_fgps AS 
(
    SELECT 
            a.order_date
        ,   a.order_id
        ,   a.driver_id
        ,   a.service_type

        ,   repet_ping                              -- DSYS010
        ,   count_repet_ping                        -- DSYS010
        
        ,   CASE WHEN repet_ping AND count_repet_ping >= 1 THEN 1 ELSE 0 END AS dsys010                                                                          
    FROM susp a
    LEFT JOIN datas2 b ON a.order_date = b.order_date AND a.driver_id = b.driver_id
    WHERE   (repet_ping AND count_repet_ping >= 1)                                            
    ORDER BY order_date
)

SELECT DISTINCT 
        order_date
    ,   order_id
    ,   driver_id
    ,   service_type

    ,   repet_ping          is_010  -- DSYS010
    ,   count_repet_ping    cnt_010 -- DSYS010

    ,   dsys010 
    ,   order_date          grass_date 
FROM all_fgps
