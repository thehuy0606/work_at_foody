DROP TABLE IF EXISTS    dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys008;
CREATE TABLE            dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys008 --WITH (partitioned_by = ARRAY['grass_date']) AS 
AS
-- DELETE FROM dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys008 WHERE grass_date BETWEEN DATE(DATE_PARSE('${D7}','%Y%m%d')) AND DATE(DATE_PARSE('${yesterday}','%Y%m%d'));
-- INSERT INTO dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys008 

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
        ,   latitude
        ,   longitude
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
        ,   IF(     COUNT(CASE WHEN speed_kmh IN (0.36, 3.6)    THEN o.order_id END) >= 3 
                OR  COUNT(CASE WHEN speed_kmh IN (0.72, 7.2)    THEN o.order_id END) >= 3 
                OR  COUNT(CASE WHEN speed_kmh IN (10.8)         THEN o.order_id END) >= 3 
                , TRUE, FALSE 
            )   repetitive_susp_speed   -- DSYS008                                                                               
    FROM susp_1 o                                                              
    GROUP BY 1,2,3,4
    ORDER BY order_date
)
  
, datas2 AS 
(
    SELECT 
            order_date 
        ,   driver_id 
        ,   COUNT(DISTINCT CASE WHEN repetitive_susp_speed THEN order_id END)  count_order_susp_speed    -- DSYS008
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
        ,   repetitive_susp_speed   -- DSYS008
        ,   count_order_susp_speed  -- DSYS008
        ,   CASE WHEN repetitive_susp_speed AND count_order_susp_speed >= 1 THEN 1 ELSE 0 END dsys008
    FROM susp a
    LEFT JOIN datas2 b ON a.order_date = b.order_date AND a.driver_id = b.driver_id
    WHERE   (repetitive_susp_speed AND count_order_susp_speed >= 1)
    ORDER BY order_date
)

SELECT DISTINCT 
        order_date
    ,   order_id
    ,   driver_id
    ,   service_type

    ,   repetitive_susp_speed   is_008  -- DSYS008
    ,   count_order_susp_speed  cnt_008 -- DSYS008

    ,   dsys008 
    ,   order_date          grass_date 
FROM all_fgps
