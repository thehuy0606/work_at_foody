DROP TABLE IF EXISTS    dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys209;
CREATE TABLE            dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys209 --WITH (partitioned_by = ARRAY['grass_date']) AS 
AS
-- DELETE FROM dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys209 WHERE grass_date BETWEEN DATE(DATE_PARSE('${D7}','%Y%m%d')) AND DATE(DATE_PARSE('${yesterday}','%Y%m%d'));
-- INSERT INTO dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__dsys209 

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
            ,   ping_time
            ,   service_type                                                                                         
            ,   LEAD(count_acc_4, 5) OVER(PARTITION BY order_id ORDER BY ping_time, upload_time) AS lead_5_ping_acc_4
            ,   count_acc_4
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
                -- AN TOÀN TỐC ĐỘ SỚM -- Bao gồm các lệnh có >=5 ping liên tiếp với tốc độ >= 5 chữ số ở đầu 
                ,   COUNT(CASE WHEN round_accuracy = 4 AND acc_digit >= 5 AND speed_digit >= 5 THEN order_id END) OVER(PARTITION BY order_id ORDER BY ping_time, upload_time) count_acc_4
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
            ,   IF(COUNT(CASE WHEN (lead_5_ping_acc_4 - count_acc_4 = 5) THEN o.order_id END) >= 1, TRUE, FALSE) AS repet_acc_4
        FROM susp_1 o
        GROUP BY order_date, o.order_id, driver_id, service_type
        ORDER BY order_date
    )
-- driver daily level
, datas2 AS 
    (
        SELECT 
                order_date
            ,   driver_id
            ,   COUNT(DISTINCT CASE WHEN repet_acc_4 THEN order_id END) AS count_repet_acc_4
        FROM susp a
        GROUP BY order_date, driver_id
    )

, all_fgps AS 
    (
        SELECT 
                a.*
            ,   count_repet_acc_4
            ,   CASE WHEN repet_acc_4 AND count_repet_acc_4 >= 1 THEN 1 ELSE 0 END AS dsys209
        FROM susp a
        LEFT JOIN datas2 b ON a.order_date = b.order_date AND a.driver_id=b.driver_id                                                                                                                                                 
        WHERE   (repet_acc_4 AND count_repet_acc_4 >= 1) 
        ORDER BY order_date
    )

SELECT 
        order_date
    ,   order_id
    ,   driver_id
    ,   service_type
    
    ,   repet_acc_4         is_209   -- DSYS209
    ,   count_repet_acc_4   cnt_209  -- DSYS209
    ,   dsys209
    ,   order_date          grass_date
FROM all_fgps
