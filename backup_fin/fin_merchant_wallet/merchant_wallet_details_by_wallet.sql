-- DROP TABLE IF EXISTS        dev_vnfdbi_opsndrivers.fin_accountant__merchant_wallet_details_by_wallet;
-- CREATE TABLE IF NOT EXISTS  dev_vnfdbi_opsndrivers.fin_accountant__merchant_wallet_details_by_wallet WITH (partitioned_by = ARRAY['grass_year']) AS 

DELETE FROM dev_vnfdbi_opsndrivers.fin_accountant__merchant_wallet_details_by_wallet 
WHERE grass_month >= CAST(DATE_FORMAT(DATE_TRUNC('month', DATE(date_parse('${yesterday}','%Y%m%d'))) - INTERVAL '1' MONTH, '%Y%m') AS INT)
; 
INSERT INTO dev_vnfdbi_opsndrivers.fin_accountant__merchant_wallet_details_by_wallet 

WITH 
wallet_info AS 
    (
        SELECT 
                id AS wallet_id
            ,   airpay_uid
            ,   name 
            ,   email
            ,   permanent_address
            ,   IF(status = 1 ,'ACTIVE', 'REVIEW') AS status
        FROM shopeefood.foody_pay_merchant_store_db__merchant_store_tab__reg_daily_s0_live
        WHERE id <> 1001
    )
,calendar AS ( SELECT * FROM UNNEST(SEQUENCE(DATE('2017-09-06'), DATE(DATE_PARSE('${yesterday}','%Y%m%d')) )) t(data_date) )
,wallet AS 
    (
        SELECT
                DATE(FROM_UNIXTIME(create_time -3600)) as date
            ,   uid
            ,   id
            ,   cash_balance/1000000 cash_balance                                                                                                      
            ,   ROW_NUMBER() OVER( PARTITION BY uid, DATE(FROM_UNIXTIME(create_time -3600)) ORDER BY id DESC) AS rn
        FROM shopeefood.foody_pay_txn_db__user_cash_history_tab__reg_daily_s0_live a
        WHERE   1=1
            AND DATE(FROM_UNIXTIME(create_time -3600)) <= DATE(DATE_PARSE('${yesterday}','%Y%m%d'))
    )
,CB_raw AS 
    (
        SELECT 
                a.data_date
            ,   b.uid
            ,   b.cash_balance
            ,   b.date
        FROM calendar a CROSS JOIN wallet b
        WHERE   1=1
            AND b.rn = 1 
            AND a.data_date >= b.date
    )   
,CB_final AS 
    (
        SELECT 
                data_date
            ,   uid
            ,   cash_balance
            ,   ROW_NUMBER() OVER( PARTITION BY data_date, uid ORDER BY date DESC) AS rn
        FROM CB_raw 
        WHERE   1=1
            AND data_date >= date
    )
,CB AS 
    (
        SELECT 
                data_date
            ,   b.id uid
            ,   sum(cash_balance) CB 
        FROM CB_final a
        JOIN shopeefood.foody_pay_merchant_store_db__merchant_store_tab__reg_daily_s0_live b on a.uid = b.airpay_uid
        WHERE   1=1
            AND rn = 1
            AND data_date BETWEEN DATE(DATE_TRUNC('month', DATE(DATE_PARSE('${yesterday}','%Y%m%d'))) - INTERVAL '1' MONTH) - INTERVAL '1' DAY AND DATE(DATE_PARSE('${yesterday}','%Y%m%d')) + INTERVAL '1' DAY
        GROUP BY 1,2
    )
,split_detail AS
    (
        SELECT 
                order_id
            ,   type
            ,   status
            ,   currency_amount /1000000 AS amount_1
            ,   payment_channel_id
            ,   item_amount
            ,   SUM(    CASE    --  amount
                            WHEN payment_channel_id IN      (21104,21106,21204,21206,21102,21100) AND t.amount  < 0 THEN - t.amount 
                            WHEN payment_channel_id IN      (21104,21106,21204,21206,21102,21100) AND t.amount >= 0 THEN 0 
                            WHEN payment_channel_id NOT IN  (21104,21106,21204,21206,21102,21100) AND t.amount >= 0 THEN t.amount 
                            ELSE 0 
                        END 
                    ) AS  amount
            ,   SUM(    CASE    --  refund
                            WHEN payment_channel_id IN      (21104,21106,21204,21206,21102,21100) AND t.amount >= 0 THEN - t.amount
                            WHEN payment_channel_id IN      (21104,21106,21204,21206,21102,21100) AND t.amount  < 0 THEN 0
                            WHEN payment_channel_id NOT IN  (21104,21106,21204,21206,21102,21100) AND t.amount  < 0 THEN t.amount 
                            ELSE 0  
                        END 
                    ) AS  refund
        FROM shopeefood.foody_pay_txn_db__order_tab__reg_daily_s0_live
        CROSS JOIN UNNEST  
            ( 
                CASE 
                    WHEN payment_channel_id IN (21104,21106,21204,21206,21102,21100) THEN CAST(JSON_EXTRACT(extra_data, '$.topup.merchant_order_details.bill_items') AS ARRAY(ROW(amount INT)))
                    ELSE CAST(JSON_EXTRACT(extra_data, '$.payment.merchant_order_details.bill_items') AS ARRAY(ROW(amount INT)))
                END  
            )   AS t
        WHERE   1 = 1 
            AND CAST(uid AS integer) != 1618703 
            AND payment_channel_id in (21107,21101,21103,21105,21104,21106,21204,21206,21102,21100) 
            AND(   
                    (DATE(FROM_UNIXTIME(valid_time -3600)) = DATE(DATE_PARSE('${yesterday}','%Y%m%d'))                   ) 
                OR  (DATE(FROM_UNIXTIME(create_time-3600)) = DATE(DATE_PARSE('${yesterday}','%Y%m%d')) AND valid_time = 0) 
            ) 
        /*
            AND(   (DATE(FROM_UNIXTIME(valid_time-3600))  = DATE(DATE_PARSE('${yesterday}','%Y%m%d'))                   )
                OR (DATE(FROM_UNIXTIME(create_time-3600)) = DATE(DATE_PARSE('${yesterday}','%Y%m%d')) AND valid_time = 0) 
            )
        */
        GROUP BY 1,2,3,4,5,6
    )   
,raw AS 
    (
        SELECT  
                b.id AS wallet_id
            ,   b.name
            ,   c.merchant_ref AS merchant_id
            ,   TRIM(c.name) AS merchant_name
            ,   CASE    --  product_type
                    WHEN a.payment_channel_id = 21019 THEN 'CREDIT_VIA_DIRECT_TOPUP'
                    WHEN a.payment_channel_id = 21043 THEN 'CREDIT_DIRECT_DEDUCT'
                    WHEN a.payment_channel_id = 21041 THEN 'CASH_VIA_DIRECT_TOPUP'
                    WHEN a.payment_channel_id = 21042 THEN 'CASH_DIRECT_DEDUCT'
                    WHEN a.payment_channel_id = 21020 THEN 'PAYMENT_NOW_DELI_PAY'
                    WHEN a.payment_channel_id = 21021 THEN 'CASH_VIA_NOW_DELI_PAY'
                    WHEN a.payment_channel_id = 21022 THEN 'PAYMENT_NOW_DELI_REFUND'
                    WHEN a.payment_channel_id = 21023 THEN 'CREDIT_VIA_NOW_DELI_REFUND'
                    WHEN a.payment_channel_id = 21044 THEN 'PAYMENT_NOW_DELI_CLAIM_COMMISSION' 
                    WHEN a.payment_channel_id = 21045 THEN 'CREDIT_VIA_NOW_DELI_CLAIM_COMMISSION'
                    WHEN a.payment_channel_id = 21046 THEN 'PAYMENT_NOW_DELI_RETURN_COMMISSION'
                    WHEN a.payment_channel_id = 21047 THEN 'CASH_VIA_NOW_DELI_RETURN_COMMISSION'
                    WHEN a.payment_channel_id = 21100 THEN 'PAYMENT_NOW_SHIP_PAY_COD'
                    WHEN a.payment_channel_id = 21101 THEN 'CASH_VIA_NOW_SHIP_PAY_COD' 
                    WHEN a.payment_channel_id = 21102 THEN 'PAYMENT_NOW_SHIP_RECEIVE_COD'
                    WHEN a.payment_channel_id = 21103 THEN 'CREDIT_VIA_NOW_SHIP_RECEIVE_COD'
                    WHEN a.payment_channel_id = 21104 THEN 'PAYMENT_NOW_SHIP_CLAIM_RETURN_FEE'   
                    WHEN a.payment_channel_id = 21105 THEN 'CREDIT_VIA_NOW_SHIP_CLAIM_RETURN_FEE'
                    WHEN a.payment_channel_id = 21106 THEN 'PAYMENT_NOW_SHIP_REFUND_RETURN_FEE'
                    WHEN a.payment_channel_id = 21107 THEN 'CASH_VIA_NOW_SHIP_REFUND_RETURN_FEE'
                    WHEN a.payment_channel_id = 21200 THEN 'PAYMENT_NOW_DELI_GIVE'
                    WHEN a.payment_channel_id = 21201 THEN 'CASH_VIA_NOW_DELI_GIVE'
                    WHEN a.payment_channel_id = 21202 THEN 'PAYMENT_NOW_DELI_RECEIVE'
                    WHEN a.payment_channel_id = 21203 THEN 'CREDIT_VIA_NOW_DELI_RECEIVE'
                    WHEN a.payment_channel_id = 21204 THEN 'PAYMENT_NOW_SHIP_GIVE'
                    WHEN a.payment_channel_id = 21205 THEN 'CASH_VIA_NOW_SHIP_GIVE'
                    WHEN a.payment_channel_id = 21206 THEN 'PAYMENT_NOW_SHIP_RECEIVE'
                    WHEN a.payment_channel_id = 21207 THEN 'CREDIT_VIA_NOW_SHIP_RECEIVE'
                    WHEN a.payment_channel_id = 21018 THEN 'CASH_REMITTANCE'
                    /* logic before 2023-10-25
                    WHEN a.payment_channel_id = 21024 AND JSON_EXTRACT(a.extra_data, '$.payment.__req__') IS NOT NULL THEN 'AUTO_CASH_REMITTANCE'
                    WHEN a.payment_channel_id = 21024 AND JSON_EXTRACT(a.extra_data, '$.payment.__req__') IS     NULL THEN 'MANUAL_CASH_REMITTANCE'
                    */
                    WHEN a.payment_channel_id = 21024 AND  CAST(JSON_EXTRACT(a.extra_data, '$.payment.__req__.withdraw_mode') AS INT) = 1   THEN 'AUTO_CASH_REMITTANCE'
                    WHEN a.payment_channel_id = 21024 AND  CAST(JSON_EXTRACT(a.extra_data, '$.payment.__req__.withdraw_mode') AS INT) = 2   THEN 'MANUAL_CASH_REMITTANCE'
                    WHEN a.payment_channel_id = 21024 AND       JSON_EXTRACT(a.extra_data, '$.payment.__req__') IS NOT NULL                 THEN 'AUTO_CASH_REMITTANCE'
                    WHEN a.payment_channel_id = 21024 AND       JSON_EXTRACT(a.extra_data, '$.payment.__req__') IS NULL                     THEN 'MANUAL_CASH_REMITTANCE'
                    WHEN a.payment_channel_id = 21028 THEN 'CASH_GIRO_WITHDRAWAL'
                    WHEN a.payment_channel_id = 21029 THEN 'WALLET_CASH_DIRECT_TOPUP'
                    WHEN a.payment_channel_id = 21025 THEN 'PAYMENT_WALLET_CASH_TRANSFER_TO_WITHDRAWAL'
                    WHEN a.payment_channel_id = 21026 THEN 'CASH_VIA_WALLET_CASH_TRANSFER_TO_WITHDRAWAL'
                    WHEN a.payment_channel_id = 21027 THEN 'WALLET_CASH_GIRO_WITHDRAWAL'
                    WHEN a.payment_channel_id = 21141 THEN 'CASH_VIA_DIRECT_TOPUP'
                    WHEN a.payment_channel_id = 21142 THEN 'CASH_DIRECT_DEDUCT'
                    else 'UNKNOWN'
                END AS product_type
            ,   CASE    --  status
                    WHEN a.status = -4 THEN 'CANCELED'
                    WHEN a.status = -3 THEN 'EXPIRED_DELETED'
                    WHEN a.status = -2 THEN 'FAILED_DELETED'
                    WHEN a.status = -1 THEN 'FAILED'
                    WHEN a.status = 0  THEN 'INITIAL'
                    WHEN a.status = 1  THEN 'EXECUTE_TOPUP'
                    WHEN a.status = 2  THEN 'EXECUTE_PAYMENT'
                    WHEN a.status = 3  THEN 'NEED_STAFF'
                    WHEN a.status = 4  THEN 'FAIL_TOPUP'
                    WHEN a.status = 5  THEN 'FAIL_PAYMENT'
                    WHEN a.status = 6  THEN 'NEED_ACTION'
                    WHEN a.status = 7  THEN 'NEED_REFUND'
                    WHEN a.status = 8  THEN 'COMPLETED'
                    WHEN a.status = 9  THEN 'REFUNDED'
                    WHEN a.status = 10 THEN 'LOCKED'
                    WHEN a.status = 11 THEN 'COMPLETING'
                    ELSE 'UNKNOWN'
                END AS status
            ,   a.order_id
            ,   coalesce(e.amount,a.currency_amount / 1000000) AS amount 
            ,   coalesce(e.refund,0 ) AS refund
            ,   from_unixtime(a.create_time-3600) AS create_time
            ,   from_unixtime(a.valid_time-3600)  AS payment_time
            ,   from_unixtime(a.update_time-3600) AS update_time
        FROM    shopeefood.foody_pay_txn_db__order_tab__reg_daily_s0_live a
        LEFT JOIN shopeefood.foody_pay_merchant_store_db__merchant_store_tab__reg_daily_s0_live b ON a.uid = b.airpay_uid
        LEFT JOIN shopeefood.foody_pay_merchant_store_db__store_tab__reg_daily_s0_live c ON a.payment_account_id = CAST(c.id AS VARCHAR)
        LEFT JOIN split_detail e ON e.order_id = a.order_id
        WHERE   1 = 1
            AND CAST(a.uid AS integer) != 1618703 
            /*
                -- AND(    
                --         (YEAR(FROM_UNIXTIME(a.valid_time -3600)) = YEAR(DATE_PARSE('${yesterday}','%Y%m%d')) )
                --     OR  (YEAR(FROM_UNIXTIME(a.create_time-3600)) = YEAR(DATE_PARSE('${yesterday}','%Y%m%d')) AND a.valid_time = 0)
                -- )
            */
            AND(    (DATE(from_unixtime(a.valid_time-3600))  >= DATE_TRUNC('month', DATE(DATE_PARSE('${yesterday}','%Y%m%d'))) - INTERVAL '1' MONTH )
                OR  (DATE(from_unixtime(a.create_time-3600)) >= DATE_TRUNC('month', DATE(DATE_PARSE('${yesterday}','%Y%m%d'))) - INTERVAL '1' MONTH and a.valid_time = 0)
            )
        

        UNION  

        SELECT  
                b.id AS wallet_id
            ,   b.name
            ,   c.merchant_ref AS merchant_id
            ,   TRIM(c.name) AS merchant_name
            ,   CASE    --  product_type
                    WHEN a.payment_channel_id = 21019 THEN 'CREDIT_VIA_DIRECT_TOPUP'
                    WHEN a.payment_channel_id = 21043 THEN 'CREDIT_DIRECT_DEDUCT'
                    WHEN a.payment_channel_id = 21041 THEN 'CASH_VIA_DIRECT_TOPUP'
                    WHEN a.payment_channel_id = 21042 THEN 'CASH_DIRECT_DEDUCT'
                    WHEN a.payment_channel_id = 21020 THEN 'PAYMENT_NOW_DELI_PAY'
                    WHEN a.payment_channel_id = 21021 THEN 'CASH_VIA_NOW_DELI_PAY'
                    WHEN a.payment_channel_id = 21022 THEN 'PAYMENT_NOW_DELI_REFUND'
                    WHEN a.payment_channel_id = 21023 THEN 'CREDIT_VIA_NOW_DELI_REFUND'
                    WHEN a.payment_channel_id = 21044 THEN 'PAYMENT_NOW_DELI_CLAIM_COMMISSION' 
                    WHEN a.payment_channel_id = 21045 THEN 'CREDIT_VIA_NOW_DELI_CLAIM_COMMISSION'
                    WHEN a.payment_channel_id = 21046 THEN 'PAYMENT_NOW_DELI_RETURN_COMMISSION'
                    WHEN a.payment_channel_id = 21047 THEN 'CASH_VIA_NOW_DELI_RETURN_COMMISSION'
                    WHEN a.payment_channel_id = 21100 THEN 'PAYMENT_NOW_SHIP_PAY_COD'
                    WHEN a.payment_channel_id = 21101 THEN 'CASH_VIA_NOW_SHIP_PAY_COD' 
                    WHEN a.payment_channel_id = 21102 THEN 'PAYMENT_NOW_SHIP_RECEIVE_COD'
                    WHEN a.payment_channel_id = 21103 THEN 'CREDIT_VIA_NOW_SHIP_RECEIVE_COD'
                    WHEN a.payment_channel_id = 21104 THEN 'PAYMENT_NOW_SHIP_CLAIM_RETURN_FEE'   
                    WHEN a.payment_channel_id = 21105 THEN 'CREDIT_VIA_NOW_SHIP_CLAIM_RETURN_FEE'
                    WHEN a.payment_channel_id = 21106 THEN 'PAYMENT_NOW_SHIP_REFUND_RETURN_FEE'
                    WHEN a.payment_channel_id = 21107 THEN 'CASH_VIA_NOW_SHIP_REFUND_RETURN_FEE'
                    WHEN a.payment_channel_id = 21200 THEN 'PAYMENT_NOW_DELI_GIVE'
                    WHEN a.payment_channel_id = 21201 THEN 'CASH_VIA_NOW_DELI_GIVE'
                    WHEN a.payment_channel_id = 21202 THEN 'PAYMENT_NOW_DELI_RECEIVE'
                    WHEN a.payment_channel_id = 21203 THEN 'CREDIT_VIA_NOW_DELI_RECEIVE'
                    WHEN a.payment_channel_id = 21204 THEN 'PAYMENT_NOW_SHIP_GIVE'
                    WHEN a.payment_channel_id = 21205 THEN 'CASH_VIA_NOW_SHIP_GIVE'
                    WHEN a.payment_channel_id = 21206 THEN 'PAYMENT_NOW_SHIP_RECEIVE'
                    WHEN a.payment_channel_id = 21207 THEN 'CREDIT_VIA_NOW_SHIP_RECEIVE'
                    WHEN a.payment_channel_id = 21018 THEN 'CASH_REMITTANCE'
                    WHEN a.payment_channel_id = 21024 AND  JSON_EXTRACT(a.extra_data, '$.payment.__req__') is not null THEN 'AUTO_CASH_REMITTANCE'
                    WHEN a.payment_channel_id = 21024 AND  JSON_EXTRACT(a.extra_data, '$.payment.__req__') is  null THEN 'MANUAL_CASH_REMITTANCE'
                    WHEN a.payment_channel_id = 21028 THEN 'CASH_GIRO_WITHDRAWAL'
                    WHEN a.payment_channel_id = 21029 THEN 'WALLET_CASH_DIRECT_TOPUP'
                    WHEN a.payment_channel_id = 21025 THEN 'PAYMENT_WALLET_CASH_TRANSFER_TO_WITHDRAWAL'
                    WHEN a.payment_channel_id = 21026 THEN 'CASH_VIA_WALLET_CASH_TRANSFER_TO_WITHDRAWAL'
                    WHEN a.payment_channel_id = 21027 THEN 'WALLET_CASH_GIRO_WITHDRAWAL'
                    WHEN a.payment_channel_id = 21141 THEN 'CASH_VIA_DIRECT_TOPUP'
                    WHEN a.payment_channel_id = 21142 THEN 'CASH_DIRECT_DEDUCT'
                    ELSE 'UNKNOWN'
                END AS product_type
            ,   CASE    --  status
                    WHEN a.status = -4 THEN 'CANCELED'
                    WHEN a.status = -3 THEN 'EXPIRED_DELETED'
                    WHEN a.status = -2 THEN 'FAILED_DELETED'
                    WHEN a.status = -1 THEN 'FAILED'
                    WHEN a.status = 0 THEN 'INITIAL'
                    WHEN a.status = 1 THEN 'EXECUTE_TOPUP'
                    WHEN a.status = 2 THEN 'EXECUTE_PAYMENT'
                    WHEN a.status = 3 THEN 'NEED_STAFF'
                    WHEN a.status = 4 THEN 'FAIL_TOPUP'
                    WHEN a.status = 5 THEN 'FAIL_PAYMENT'
                    WHEN a.status = 6 THEN 'NEED_ACTION'
                    WHEN a.status = 7 THEN 'NEED_REFUND'
                    WHEN a.status = 8 THEN 'COMPLETED'
                    WHEN a.status = 9 THEN 'REFUNDED'
                    WHEN a.status = 10 THEN 'LOCKED'
                    WHEN a.status = 11 THEN 'COMPLETING'
                    ELSE 'UNKNOWN'
                END AS status
            ,   a.order_id
            ,   coalesce(e.amount,a.currency_amount / 1000000) AS amount 
            ,   coalesce(e.refund,0 ) AS refund
            ,   FROM_UNIXTIME(a.create_time-3600) AS create_time
            ,   FROM_UNIXTIME(a.valid_time-3600) AS payment_time
            ,   FROM_UNIXTIME(a.update_time-3600) AS update_time
        FROM    shopeefood.foody_pay_txn_db__order_tab__reg_daily_s0_live a
        LEFT JOIN shopeefood.foody_pay_merchant_store_db__merchant_store_tab__reg_daily_s0_live b ON a.uid = b.airpay_uid
        LEFT JOIN shopeefood.foody_pay_merchant_store_db__store_tab__reg_daily_s0_live c ON a.payment_account_id = CAST(c.id AS VARCHAR)
        LEFT JOIN split_detail e ON e.order_id = a.order_id
        WHERE   1 = 1 
            AND a.status = 9 
            AND CAST(a.uid as integer) != 1618703 
            AND DATE(FROM_UNIXTIME(a.update_time-3600)) <> DATE(FROM_UNIXTIME(a.valid_time-3600)) 
            -- AND YEAR(FROM_UNIXTIME(a.update_time-3600)) =  YEAR(DATE_PARSE('${yesterday}','%Y%m%d')) 
            AND DATE(from_unixtime(a.update_time-3600)) >= DATE_TRUNC('month', DATE(DATE_PARSE('${yesterday}','%Y%m%d'))) - INTERVAL '1' MONTH
        -- this is for the case that auto withdraw late----

        UNION 

        SELECT 
                b.id AS wallet_id
            ,   b.name
            ,   'dummy' AS merchant_id
            ,   'dummy' AS merchant_name
            ,   'AUTO_CASH_REMITTANCE' AS product_type
            ,   'COMPLETED' AS status
            ,   a.order_id
            ,   CASE WHEN DATE(FROM_UNIXTIME(a.create_time-3600)) = DATE(DATE_PARSE('${yesterday}','%Y%m%d')) + INTERVAL '1' DAY THEN -cash_amount / 1000000 ELSE 0 END AS amount 
            ,   0 as refund
            ,   FROM_UNIXTIME(a.create_time-3600) AS create_time
            ,   FROM_UNIXTIME(a.create_time-3600) AS payment_time
            ,   FROM_UNIXTIME(a.create_time-3600) AS update_time
        FROM shopeefood.foody_pay_txn_db__user_cash_history_tab__reg_daily_s0_live a 
        LEFT JOIN shopeefood.foody_pay_merchant_store_db__merchant_store_tab__reg_daily_s0_live b ON a.uid = b.airpay_uid 
        WHERE 1=1 
            AND type IN (541) 
            AND uid = 1897469 
            AND HOUR(FROM_UNIXTIME(a.create_time-3600)) IN (3,4) 
            -- AND YEAR(FROM_UNIXTIME(a.create_time-3600)) = YEAR(DATE_PARSE('${yesterday}','%Y%m%d')) 
            AND DATE(FROM_UNIXTIME(a.create_time-3600)) >= DATE_TRUNC('month', DATE(DATE_PARSE('${yesterday}','%Y%m%d'))) - INTERVAL '1' MONTH
    )

,process_cb AS 
    (
        SELECT 
                a.data_date
            ,   t1.name
            ,   t1.status
            ,   coalesce(t3.CB,0) closing_balance
            ,   t1.wallet_id
            ,   t1.email
            ,   t1.permanent_address
        FROM calendar a
        -- left join CB t2 on  a.data_date = t2.data_date + interval '1' day
        LEFT JOIN CB t3 on  a.data_date = t3.data_date -- and t3.uid = raw.wallet_id
        -- left join raw on t3.uid = raw.wallet_id
        LEFT JOIN wallet_info t1 on t1.wallet_id = t3.uid
        WHERE   1=1
            -- AND YEAR(a.data_date) = YEAR(DATE_PARSE('${yesterday}','%Y%m%d'))
            AND a.data_date >= DATE_TRUNC('month', DATE(DATE_PARSE('${yesterday}','%Y%m%d'))) - INTERVAL '1' MONTH
            -- and t3.uid not in (select raw.wallet_id from raw where status in ('COMPLETED','REFUNDED' ))
            -- and raw.wallet_id is null
            -- AND t3.uid = 33895
        GROUP BY 1,2,3,4,5,6,7 
    )
,process_ob AS 
    (
        SELECT 
                a.data_date
            ,   name
            ,   status
            ,   coalesce(t2.CB,0) opening_balance
            ,   closing_balance
            ,   a.wallet_id
            ,   email
            ,   permanent_address
        FROM process_cb a
        LEFT JOIN CB t2 ON a.data_date = t2.data_date + INTERVAL '1' DAY AND t2.uid = a.wallet_id
        -- LEFT join CB t3 on  a.data_date = t3.data_date -- and t3.uid = raw.wallet_id
        -- left join raw on t3.uid = raw.wallet_id
        -- left join wallet_info t1 on t1.wallet_id = t3.uid
        WHERE   1=1
            -- AND YEAR(a.data_date) = YEAR(DATE_PARSE('${yesterday}','%Y%m%d'))
            AND a.data_date >= DATE_TRUNC('month', DATE(DATE_PARSE('${yesterday}','%Y%m%d'))) - INTERVAL '1' MONTH
            -- and t3.uid not in (select raw.wallet_id from raw where status in ('COMPLETED','REFUNDED' ))
            -- and raw.wallet_id is null
            -- AND t3.uid = 33895
        GROUP BY 1,2,3,4,5,6,7,8 
    )
,result AS  
    (
        SELECT 
                a.data_date date
            ,   a.name
            ,   a.status
            ,   a.opening_balance
            ,   sum(CASE WHEN product_type NOT IN ('CASH_VIA_DIRECT_TOPUP','CREDIT_VIA_DIRECT_TOPUP','MANUAL_CASH_REMITTANCE','PAYMENT_NOW_DELI_CLAIM_COMMISSION','AUTO_CASH_REMITTANCE','CASH_DIRECT_DEDUCT','CREDIT_DIRECT_DEDUCT') THEN amount ELSE 0 END) AS pay_amount 
            ,   sum(CASE WHEN product_type     IN ('CASH_VIA_DIRECT_TOPUP','CREDIT_VIA_DIRECT_TOPUP') THEN amount + refund ELSE 0 END ) AS direct_topup_amount
            ,   sum(CASE 
                        WHEN product_type IN ('MANUAL_CASH_REMITTANCE','AUTO_CASH_REMITTANCE') AND raw.status = 'REFUNDED' AND DATE(raw.create_time) != DATE(raw.payment_time) THEN amount
                        WHEN product_type IN ('MANUAL_CASH_REMITTANCE','AUTO_CASH_REMITTANCE') AND raw.status = 'REFUNDED' AND DATE(raw.create_time)  = DATE(raw.update_time)  THEN amount  
                        ELSE 0 END 
                ) AS withdrawal_refund
            ,   sum(coalesce(refund,0)) - sum(CASE WHEN product_type IN ('PAYMENT_NOW_DELI_CLAIM_COMMISSION','CREDIT_VIA_NOW_DELI_CLAIM_COMMISSION') THEN amount ELSE 0 END) AS payment_refund
            ,   - sum(CASE WHEN product_type IN ('MANUAL_CASH_REMITTANCE') AND DATE(raw.payment_time) = DATE(raw.create_time) THEN amount + refund ELSE 0 END ) as mannual_withdrawal
            ,   - sum(CASE WHEN product_type IN ('AUTO_CASH_REMITTANCE')   AND DATE(raw.payment_time) = DATE(raw.create_time) THEN amount + refund ELSE 0 END ) as auto_withdrawal
            ,   - sum(CASE WHEN product_type IN ('CASH_DIRECT_DEDUCT','CREDIT_DIRECT_DEDUCT') THEN amount + refund ELSE 0 END ) as direct_deduct_amount
            ,   a.closing_balance
            ,   a.wallet_id
            ,   a.email
            ,   a.permanent_address 
        FROM process_ob a 
        LEFT JOIN raw
            ON  CASE 
                    WHEN raw.status = 'REFUNDED'        THEN DATE(payment_time)
                    WHEN raw.status = 'FAILED_DELETED'  THEN DATE( create_time)
                    WHEN raw.status = 'CANCELED'        THEN DATE(create_time)
                    ELSE DATE(payment_time) 
                END = a.data_date
                AND raw.status IN ('COMPLETED','REFUNDED' )
                AND raw.wallet_id = a.wallet_id
        GROUP BY 
                a.data_date
            ,   a.name
            ,   a.status
            ,   a.opening_balance
            ,   a.closing_balance
            ,   a.wallet_id
            ,   a.email
            ,   a.permanent_address
    )
SELECT  *
    ,   date AS grass_date 
    ,   YEAR(date) AS grass_year 
    ,   CAST(DATE_FORMAT(date, '%Y%m') AS INT) AS grass_month 
FROM result
