-- DROP TABLE IF EXISTS        dev_vnfdbi_opsndrivers.fin_accountant__merchant_wallet_details_by_txn;
-- CREATE TABLE IF NOT EXISTS  dev_vnfdbi_opsndrivers.fin_accountant__merchant_wallet_details_by_txn WITH (partitioned_by = ARRAY['grass_year']) AS 

DELETE FROM dev_vnfdbi_opsndrivers.fin_accountant__merchant_wallet_details_by_txn 
WHERE grass_month >= CAST(DATE_FORMAT(DATE_TRUNC('month', DATE(date_parse('${yesterday}','%Y%m%d'))) - INTERVAL '1' MONTH, '%Y%m') AS INT)
;
INSERT INTO dev_vnfdbi_opsndrivers.fin_accountant__merchant_wallet_details_by_txn 

WITH 
order_mapping AS 
    (
        SELECT 
                a.id 
            ,   b.order_code 
            ,   FROM_UNIXTIME(a.create_timestamp) AS grass_date 
            ,   FROM_UNIXTIME(b.final_delivered_time-3600) AS deliver_time 
        FROM    shopeefood.foody_mart__fact_gross_order_join_detail a 
        JOIN    shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live b ON a.id = b.id 
        WHERE   1=1 
            AND a.grass_region != '0' 
            AND DATE(FROM_UNIXTIME(b.final_delivered_time-3600)) >= DATE(DATE_TRUNC('month', DATE(DATE_PARSE('${yesterday}','%Y%m%d'))) - INTERVAL '1' MONTH) - INTERVAL '2' DAY
            -- AND DATE(FROM_UNIXTIME(b.final_delivered_time-3600)) >= DATE_TRUNC('year', DATE(DATE_PARSE('${yesterday}','%Y%m%d'))) - INTERVAL '2' DAY 
            
    ), 
split_detail AS 
    (
        SELECT 
                order_id 
            ,   type 
            ,   status 
            ,   currency_amount /1000000 AS amount_1 
            ,   payment_channel_id 
            ,   item_amount 
            ,   SUM( CASE   --  amount
                        WHEN payment_channel_id IN (21104,21106,21204,21206,21102,21100)     AND t.amount < 0  THEN - t.amount 
                        WHEN payment_channel_id IN (21104,21106,21204,21206,21102,21100)     AND t.amount >= 0 THEN 0 
                        WHEN payment_channel_id NOT IN (21104,21106,21204,21206,21102,21100) AND t.amount >= 0 THEN t.amount 
                        ELSE 0 END 
                    ) AS  amount 
            ,   SUM( CASE   --  refund
                        WHEN payment_channel_id IN (21104,21106,21204,21206,21102,21100)     AND t.amount >= 0 THEN - t.amount
                        WHEN payment_channel_id IN (21104,21106,21204,21206,21102,21100)     AND t.amount < 0  THEN 0
                        WHEN payment_channel_id NOT IN (21104,21106,21204,21206,21102,21100) AND t.amount < 0  THEN t.amount 
                        ELSE 0  END 
                    ) AS  refund 
        FROM shopeefood.foody_pay_txn_db__order_tab__reg_daily_s0_live 
        CROSS JOIN UNNEST 
            ( 
                CASE 
                    WHEN payment_channel_id IN (21104,21106,21204,21206,21102,21100) THEN CAST(json_extract(extra_data, '$.topup.merchant_order_details.bill_items') AS ARRAY(ROW(amount INT)))
                    ELSE CAST(json_extract(extra_data, '$.payment.merchant_order_details.bill_items') AS ARRAY(ROW(amount INT))) 
                END 
            ) AS t 
        WHERE   1 = 1 
            AND payment_channel_id IN (21107,21101,21103,21105,21104,21106,21204,21206,21102,21100) 
            AND CAST(uid AS integer) != 1618703 
            AND (      
                    (DATE(FROM_UNIXTIME(valid_time -3600)) >= DATE_TRUNC('month', DATE(DATE_PARSE('${yesterday}','%Y%m%d'))) - INTERVAL '1' MONTH  ) 
                 OR (DATE(FROM_UNIXTIME(create_time-3600)) >= DATE_TRUNC('month', DATE(DATE_PARSE('${yesterday}','%Y%m%d'))) - INTERVAL '1' MONTH   AND valid_time = 0  ) 
                ) 
                -- AND (      
                --         (YEAR(FROM_UNIXTIME(valid_time -3600)) = YEAR(DATE_PARSE('${yesterday}','%Y%m%d'))) 
                --      OR (YEAR(FROM_UNIXTIME(create_time-3600)) = YEAR(DATE_PARSE('${yesterday}','%Y%m%d')) AND valid_time = 0) 
                --     ) 
        GROUP BY 1,2,3,4,5,6 
    ) 
,result AS 
    (
        SELECT   
                b.id AS wallet_id
            ,   b.name
            ,   c.merchant_ref  AS merchant_id
            ,   TRIM(c.name)    AS merchant_name
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
                    WHEN a.payment_channel_id = 21024 THEN 'MANUAL_CASH_REMITTANCE'
                    WHEN a.payment_channel_id = 21028 THEN 'CASH_GIRO_WITHDRAWAL'
                    WHEN a.payment_channel_id = 21029 THEN 'WALLET_CASH_DIRECT_TOPUP'
                    WHEN a.payment_channel_id = 21025 THEN 'PAYMENT_WALLET_CASH_TRANSFER_TO_WITHDRAWAL'
                    WHEN a.payment_channel_id = 21026 THEN 'CASH_VIA_WALLET_CASH_TRANSFER_TO_WITHDRAWAL'
                    WHEN a.payment_channel_id = 21027 THEN 'WALLET_CASH_GIRO_WITHDRAWAL'
                    WHEN a.payment_channel_id = 21141 THEN 'CASH_VIA_DIRECT_TOPUP'
                    WHEN a.payment_channel_id = 21142 THEN 'CASH_DIRECT_DEDUCT'
                    ELSE 'UNKNOWN'
                END AS product_type
            ,   CASE    --  type
                    WHEN a.type = 0  THEN 'UNKNOWN'
                    WHEN a.type = 1  THEN 'TOPUP'
                    WHEN a.type = 2  THEN 'WITHDRAW'
                    WHEN a.type = 3  THEN 'PAYMENT'
                    WHEN a.type = 4  THEN 'SHOPPING'
                    WHEN a.type = 5  THEN 'TRANSFER'
                    WHEN a.type = 6  THEN 'GIFTING'
                    WHEN a.type = 7  THEN 'LOAN'
                    WHEN a.type = 8  THEN 'REMITTANCE'
                    WHEN a.type = 9  THEN 'SYSTEM'
                    WHEN a.type = 10 THEN 'VIRTUAL_CARD'
                    WHEN a.type = 11 THEN 'MERCHANT_STORE_CREDIT'
                    ELSE 'UNKNOWN'
                END AS type
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
            ,   FROM_UNIXTIME(a.create_time-3600) AS create_time 
            ,   FROM_UNIXTIME(a.valid_time -3600) AS payment_time 
            ,   FROM_UNIXTIME(a.update_time-3600) AS update_time 
            ,   d.grass_date AS order_from_buyer_time 
            ,   d.deliver_time AS deliver_time 
            ,   c.merchant_ref 
            ,   coalesce(f.remark,'') AS remark
            ,   CASE    --  order_code
                    WHEN a.payment_channel_id IN (21102 , 21104 ,21044) THEN a.topup_channel_txn_id 
                    ELSE payment_channel_txn_id 
                END AS order_code 
            ,   a.valid_time
            ,   CASE    --  grass_date
                    WHEN a.status = 8 THEN DATE(FROM_UNIXTIME(a.valid_time -3600)) -- Đơn COMPLETED => lấy valid_time 
                    WHEN a.status = 9 THEN DATE(FROM_UNIXTIME(a.update_time-3600)) -- Đơn REFUNDED  => lấy update_time 
                    WHEN (a.valid_time = 0 AND a.status NOT IN (8,9) ) THEN DATE(FROM_UNIXTIME(a.create_time-3600)) -- Đơn Ngoài COMPLETED, REFUNDED và Không có Refunded time => lấy create_time
                END AS grass_date 
            -- ,   YEAR(CASE    --  grass_date
            --             WHEN a.status = 8 THEN DATE(FROM_UNIXTIME(a.valid_time -3600)) -- Đơn COMPLETED => lấy valid_time 
            --             WHEN a.status = 9 THEN DATE(FROM_UNIXTIME(a.update_time-3600)) -- Đơn REFUNDED  => lấy update_time 
            --             WHEN (a.valid_time = 0 AND a.status NOT IN (8,9) ) THEN DATE(FROM_UNIXTIME(a.create_time-3600)) -- Đơn Ngoài COMPLETED,REFUNDED và Không có Refunded time   => lấy create_time
            --         END) AS grass_year 
            -- ,   CAST(DATE_FORMAT(
            --         CASE    --  grass_date
            --             WHEN a.status = 8 THEN DATE(FROM_UNIXTIME(a.valid_time -3600)) -- Đơn COMPLETED => lấy valid_time 
            --             WHEN a.status = 9 THEN DATE(FROM_UNIXTIME(a.update_time-3600)) -- Đơn REFUNDED  => lấy update_time 
            --             WHEN (a.valid_time = 0 AND a.status NOT IN (8,9) ) THEN DATE(FROM_UNIXTIME(a.create_time-3600)) -- Đơn Ngoài COMPLETED,REFUNDED và Không có Refunded time   => lấy create_time
            --         END, '%Y%m'
            --         ) AS INT) AS grass_month 
        FROM    shopeefood.foody_pay_txn_db__order_tab__reg_daily_s0_live a 
        LEFT JOIN shopeefood.foody_pay_merchant_store_db__merchant_store_tab__reg_daily_s0_live b ON a.uid = b.airpay_uid 
        LEFT JOIN shopeefood.foody_pay_merchant_store_db__store_tab__reg_daily_s0_live c ON a.payment_account_id = cast(c.id as varchar) 
        LEFT JOIN order_mapping d ON a.payment_channel_txn_id = d.order_code 
        LEFT JOIN split_detail e ON e.order_id = a.order_id 
        LEFT JOIN (SELECT DISTINCT order_id, CAST(json_extract(data,'$.remark') AS VARCHAR) AS remark FROM shopeefood.foody_pay_backend_manager_db__manage_log_tab__vn_daily_s0_live) f on a.order_id = f.order_id
        WHERE   1 = 1 
            AND CAST(a.uid AS integer) != 1618703 
            AND(   
                    (DATE(FROM_UNIXTIME(a.valid_time  -3600)) >= DATE_TRUNC('month', DATE(DATE_PARSE('${yesterday}','%Y%m%d'))) - INTERVAL '1' MONTH AND a.status = 8) -- Complete Order and payment_time
                OR  (DATE(FROM_UNIXTIME(a.update_time -3600)) >= DATE_TRUNC('month', DATE(DATE_PARSE('${yesterday}','%Y%m%d'))) - INTERVAL '1' MONTH AND a.status = 9) -- Refunded Order and update_time
                OR  (DATE(FROM_UNIXTIME(a.create_time -3600)) >= DATE_TRUNC('month', DATE(DATE_PARSE('${yesterday}','%Y%m%d'))) - INTERVAL '1' MONTH AND a.valid_time = 0 AND a.status NOT IN (8,9)) -- Order not in (Complete, Refunded) and No payment
                
            )
            /* old logic
                AND(   
                        (DATE(FROM_UNIXTIME(a.valid_time  -3600)) = DATE(DATE_PARSE('${yesterday}','%Y%m%d')) AND a.status = 8) -- Complete Order and payment_time
                    OR  (DATE(FROM_UNIXTIME(a.update_time -3600)) = DATE(DATE_PARSE('${yesterday}','%Y%m%d')) AND a.status = 9) -- Refunded Order and update_time
                    OR  (DATE(FROM_UNIXTIME(a.create_time-3600)) = DATE(DATE_PARSE('${yesterday}','%Y%m%d')) AND a.valid_time = 0 AND a.status NOT IN (8,9)) -- Order not in (Complete, Refunded) and No payment
                    
                )
                and(    (date(from_unixtime(a.valid_time-3600))= current_date - interval'1'day and a.status <> 9)
                    or (date(from_unixtime(a.create_time-3600)) = current_date - interval'1'day and a.valid_time = 0)
                    or (date(from_unixtime(a.update_time-3600))= current_date - interval'1'day) 
                )
            */
    )
select  *
    ,   YEAR(grass_date) grass_year
    ,   CAST(DATE_FORMAT(grass_date, '%Y%m') AS INT) grass_month
from result 
    