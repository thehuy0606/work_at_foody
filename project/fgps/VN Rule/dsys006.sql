DROP TABLE IF EXISTS    dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys006;
CREATE TABLE            dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys006 --WITH (partitioned_by = ARRAY['grass_date']) AS 
AS
-- DELETE FROM dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys006 WHERE grass_date BETWEEN DATE(DATE_PARSE('${D7}','%Y%m%d')) AND DATE(DATE_PARSE('${yesterday}','%Y%m%d'));
-- INSERT INTO dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys006 

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
        ,   ST_DISTANCE(TO_SPHERICAL_GEOGRAPHY(ST_POINT(longitude, latitude)), TO_SPHERICAL_GEOGRAPHY(ST_POINT(next_longitude, next_latitude)))/1000    distance
        ,   latitude
        ,   longitude
        ,   next_accuracy
        ,   next_upload
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
            ,   LEAD(accuracy)  OVER(PARTITION BY order_id ORDER BY ping_time, upload_time) next_accuracy
            ,   LEAD(ping_time) OVER(PARTITION BY order_id ORDER BY ping_time, upload_time) next_upload
            ,   LEAD(latitude)  OVER(PARTITION BY order_id ORDER BY ping_time, upload_time) next_latitude
            ,   LEAD(longitude) OVER(PARTITION BY order_id ORDER BY ping_time, upload_time) next_longitude
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
        ,   IF(COUNT(CASE WHEN distance > 1 AND next_accuracy < 20 AND DATE_DIFF('second', ping_time, next_upload) = 5 THEN o.order_id END) >= 1, TRUE, FALSE) AS jump_1km_accuracy_20_within5s     -- DSYS006                                                                            
    FROM susp_1 o                                                              
    GROUP BY 1,2,3,4
    ORDER BY order_date
)
  
, datas2 AS 
(
    SELECT 
            order_date
        ,   driver_id
        ,   COUNT(DISTINCT CASE WHEN jump_1km_accuracy_20_within5s then order_id end ) as count_jump_1km_accuracy_20_within5s     -- DSYS006
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
        ,   jump_1km_accuracy_20_within5s           -- DSYS006
        ,   count_jump_1km_accuracy_20_within5s     -- DSYS006
        , CASE WHEN jump_1km_accuracy_20_within5s AND count_jump_1km_accuracy_20_within5s >= 3 THEN 1 ELSE 0 END dsys006     
    FROM susp a
    LEFT JOIN datas2 b ON a.order_date = b.order_date AND a.driver_id = b.driver_id
    WHERE   (jump_1km_accuracy_20_within5s AND count_jump_1km_accuracy_20_within5s >=1) 
    ORDER BY order_date
)

SELECT DISTINCT 
        order_date
    ,   order_id
    ,   driver_id
    ,   service_type

    ,   jump_1km_accuracy_20_within5s       is_006      -- DSYS006
    ,   count_jump_1km_accuracy_20_within5s cnt_006     -- DSYS006

    ,   dsys006
    ,   order_date          grass_date
FROM all_fgps
