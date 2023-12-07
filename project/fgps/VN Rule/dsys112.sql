DROP TABLE IF EXISTS    dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys112;
CREATE TABLE            dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys112 --WITH (partitioned_by = ARRAY['grass_date']) AS 
AS
-- DELETE FROM dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys112 WHERE grass_date BETWEEN DATE(DATE_PARSE('${D7}','%Y%m%d')) AND DATE(DATE_PARSE('${yesterday}','%Y%m%d'));
-- INSERT INTO dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys112 

WITH 
susp_1 AS 
(
    SELECT
            order_date
        ,   create_time
        ,   order_id
        ,   driver_id
        ,   accuracy            
        ,   upload_time 
        ,   service_type
        ,   LEAD(count_acc_1, 5) OVER(PARTITION BY order_id ORDER BY ping_time, upload_time) AS lead_5_ping_acc_1
        ,   count_acc_1
    FROM 
    (
        SELECT DISTINCT
                order_date
            ,   create_time
            ,   order_id
            ,   driver_id
            ,   accuracy
            ,   upload_time
            ,   ping_time
            ,   service_type
            ,   COUNT(CASE WHEN round_accuracy = 1 AND acc_digit >= 5 THEN order_id END) OVER(PARTITION BY order_id ORDER BY ping_time, upload_time) AS count_acc_1
        FROM dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__pickup_before 
        WHERE   1=1
            -- AND grass_date BETWEEN DATE(DATE_PARSE('${D7}','%Y%m%d')) AND DATE(DATE_PARSE('${yesterday}','%Y%m%d'))
    )
)

-- LƯỚI AN TOÀN CHÍNH XÁC TRUNG BÌNH -- Loại trừ các đơn hàng có >=1 ping w/ acc >= 5 chữ số ở giữa
--  START
, repet_ping_middle AS 
(
    SELECT DISTINCT 
        order_id
    FROM dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__pickup_after 
    WHERE   1=1
        -- AND grass_date BETWEEN DATE(DATE_PARSE('${D7}','%Y%m%d')) AND DATE(DATE_PARSE('${yesterday}','%Y%m%d'))
        AND acc_digit >= 5
)  

-- order level
, susp AS 
(
    SELECT 
            order_date
        ,   o.order_id
        ,   driver_id
        ,   service_type
        ,   IF(COUNT(CASE WHEN (lead_5_ping_acc_1 - count_acc_1 = 5) AND b.order_id IS NULL THEN o.order_id END) >= 1, TRUE, FALSE) repet_acc_1
    FROM susp_1 o
    LEFT JOIN repet_ping_middle b ON o.order_id = b.order_id
    GROUP BY order_date, o.order_id, driver_id, service_type
    ORDER BY order_date
)
-- END

-- driver daily level
, datas2 AS 
(
    SELECT 
            order_date
        ,   driver_id
        ,   COUNT(DISTINCT CASE WHEN repet_acc_1 THEN order_id END) AS count_repet_acc_1
    FROM susp a
    GROUP BY order_date, driver_id
)
          
, all_fgps AS 
(
    SELECT 
            a.*
        ,   count_repet_acc_1
        ,   CASE WHEN repet_acc_1 AND count_repet_acc_1 >= 1 THEN 1 ELSE 0 END AS dsys112
    FROM susp a
    LEFT JOIN datas2 b ON a.order_date = b.order_date AND a.driver_id = b.driver_id                                                                                                                                                 
    WHERE   (repet_acc_1 AND count_repet_acc_1 >= 1)  
    ORDER BY order_date
)

SELECT 
        order_date
    ,   order_id
    ,   driver_id
    ,   service_type

    ,   repet_acc_1         is_112  -- DSYS112
    ,   count_repet_acc_1   cnt_112 -- DSYS112

    ,   dsys112 
    ,   order_date          grass_date 
FROM all_fgps
