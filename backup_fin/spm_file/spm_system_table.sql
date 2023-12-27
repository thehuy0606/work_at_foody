/* spm_driver_wallet_topup */
WITH 
base AS 
    (
        SELECT  DISTINCT 
                a.transaction_id
            ,   first_value(b.channel_ref) over (partition by b.transaction_id order by b.ctime desc, b.vtime desc) channel_ref
            ,   CASE    --  payment_channel
                    WHEN b.channel_id = 5002800 THEN 'iBanking' 
                    WHEN b.channel_id = 5007201 THEN 'VA' 
                    WHEN b.channel_id = 5007400 THEN 'VA' 
                    WHEN b.channel_id = 5001800 THEN 'Airpay Wallet V2'
                    WHEN b.channel_id = 5001300 THEN 'Shopee Wallet'
                    WHEN b.channel_id = 5002900 THEN 'Airpay GIRO'
                    WHEN b.channel_id = 5000101 THEN 'Cybersource Installment'
                    WHEN b.channel_id = 5000102 THEN 'Cybersource New '
                    WHEN b.channel_id = 5002601 THEN 'SPayLater'
                    WHEN b.channel_id = 5014000 THEN 'ApplePay'
                    WHEN b.channel_id = 5011000 THEN 'Airpay Authpay'
                    WHEN b.channel_id = 5003200 THEN 'IS PAYOUT'
                    WHEN b.channel_id = 5016900 THEN 'Vn ShopeePay Wallet Payout'
                    WHEN b.channel_id = 5003100 THEN 'Manual Bank Transfer'
                    WHEN b.channel_id = 5001600 THEN 'Payoneer VN'
                    WHEN b.channel_id = 5015700 THEN 'Merchant Wallet Service Payout'
                    WHEN b.channel_id = 5005100 THEN 'Cash On Delivery'
                    ELSE NULL
                END payment_channel
            ,   a.entity_id_str encrypted_transaction_id 
            ,   a.channel_id 
            ,   b.amount/100000 amount 
            ,   CASE   
                    WHEN b.status = 2 then 'PAYMENT_INIT'
                    WHEN b.status = 8 then 'PENDING'
                    WHEN b.status = 6 then 'USER_PROCESSING'
                    WHEN b.status = 20 then 'SUCCESS'
                    WHEN b.status = 22 then 'FAILED'
                    WHEN b.status = 50 then 'CANCELED'
                    WHEN b.status = 51 then 'CANCEL_READY'
                    WHEN b.status = 54 then 'EXPIRED'	--- new added---
                    WHEN b.status = 100 then 'FRAUD'
                    WHEN b.status = 0 then 'INITIAL'
                    WHEN b.status = 14 then 'RECONCILING'
                    WHEN b.status = 10 then 'BLOCKED'
                    WHEN b.status = 26 then 'SUCCESS_BUT_REJECTED_BY_SERVER'
                    WHEN b.status = 24 then 'LATE_SUCCESS'
                END status 
            ,   FROM_UNIXTIME(a.ctime-3600) ctime 
            ,   FROM_UNIXTIME(a.mtime-3600) mtime 
            ,   DATE(FROM_UNIXTIME(a.ctime-3600)) cdate 
            ,   DATE(FROM_UNIXTIME(a.mtime-3600)) mdate 
            ,   GET_JSON_OBJECT(b.extra_data, '$.payment_code')           spe_va_number
            ,   GET_JSON_OBJECT(a.extra_data, '$.callback_result.ref_id') ref_id
            ,   GET_JSON_OBJECT(a.extra_data, '$.callback_result.txn_id') txn_id 
            ,   IF(b.status = 22, GET_JSON_OBJECT(b.extra_data, '$.pay_result.fail_reason'), NULL) fail_reason 
        FROM    shopeepay.shopee_payment_module_provision_vn_db__provision_v2_tab__vn_daily_s0_live a 
        LEFT JOIN shopeepay.shopee_payment_module_payment_vn_db__payment_v2_tab__vn_daily_s2_live   b   ON a.transaction_id = b.transaction_id 
        WHERE   a.channel_id IN (5010601, 5010801) 
        ORDER BY 8 DESC 
    ) 
SELECT  
        transaction_id 
    ,   channel_ref
    ,   encrypted_transaction_id
    ,   payment_channel 
    ,   CASE payment_channel WHEN 'VA' THEN 'Bank Transfer' WHEN 'Airpay Wallet V2' THEN 'Airpay Wallet' ELSE payment_channel END topup_method
    ,   channel_id 
    ,   amount 
    ,   status 
    ,   CAST(ctime AS TIMESTAMP) ctime
    ,   CAST(mtime AS TIMESTAMP) mtime
    ,   cdate 
    ,   mdate 
    ,   spe_va_number
    ,   ref_id
    ,   txn_id
    ,   fail_reason
    ,   IF(payment_channel = 'VA', txn_id, encrypted_transaction_id) request_id
FROM    base 
WHERE   DATE(CAST(ctime AS TIMESTAMP)) < CURRENT_DATE
;

/* spm_driver_wallet_topup */
WITH  
base AS 
(   
    SELECT 
            reference_id
        ,   source_type 
        ,   amount 
        ,   status 
        ,   CAST(ctime AS TIMESTAMP) ctime
        ,   CAST(mtime AS TIMESTAMP) mtime
        ,   general_payout_status 
        ,   entity_id 
        ,   transaction_id 
        ,   payment_id 
        ,   payment_channel
        ,   payment_status 
        ,   CASE 
                WHEN bank_account       IS NOT NULL THEN 'Banking'
                WHEN shopeepay_wallet   IS NOT NULL THEN 'Airpay Wallet'
                ELSE 'Others'
            END withdraw_method 
        ,   bank_account 
        ,   shopeepay_wallet
        ,   raw_fail_reason 
        ,   GET_JSON_OBJECT(SUBSTRING(raw_fail_reason, INSTR(raw_fail_reason, '{'), INSTR(raw_fail_reason, '}')+1 ), '$.msg') fail_reason 
        ,   error_code
        ,   CASE 
                WHEN error_code = '-1' THEN 'HTTP_METHOD_DISALLOWED' 
                WHEN error_code = '-2' THEN 'CLIENT_ACCESS_DENIED' 
                WHEN error_code = '-3' THEN 'IP_NOT_ALLOWED' 
                WHEN error_code = '-4' THEN 'PARAMETER_REQUIRED' 
                WHEN error_code = '-5' THEN 'INVALID_PARAMETER' 
                WHEN error_code = '-6' THEN 'SIGNATURE_NOT_MATCH' 
                WHEN error_code = '-7' THEN 'BANK_NOT_SUPPORTED' 
                WHEN error_code = '-8' THEN 'CLIENT_NOT_SUPPORTED' 
                WHEN error_code = '0'  THEN 'SUCCESS' 
                WHEN error_code = '1'  THEN 'PENDING' 
                WHEN error_code = '2'  THEN 'ERROR_TXN_FAIL' 
                WHEN error_code = '3'  THEN 'WAIT_FOR_RECON' 
                WHEN error_code = '10' THEN 'ERROR_DUPLICATE_TXN_ID' 
                WHEN error_code = '11' THEN 'ERROR_INVALID_AMOUNT' 
                WHEN error_code = '12' THEN 'ERROR_INSUFFICIENT_AMOUNT' 
                WHEN error_code = '13' THEN 'TXN_NOT_FOUND'
                WHEN error_code = '14' THEN 'TXN_AMOUNT_TOO_BIG_FOR_NAPAS247'
                WHEN error_code = '15' THEN 'TXN_AMOUNT_TOO_HIGH' 
                WHEN error_code = '16' THEN 'TXN_AMOUNT_TOO_LOW' 
                WHEN error_code = '20' THEN 'ERROR_INVALID_BANK_ID'
                WHEN error_code = '21' THEN 'ERROR_INVALID_BANK_ACCOUNT_NO' 
                WHEN error_code = '22' THEN 'ERROR_INVALID_BANK_HOLDER_NAME' 
                WHEN error_code = '23' THEN 'ERROR_INVALID_BANK_ACCOUNT_STATUS' 
                WHEN error_code = '24' THEN 'REQUEST_ACCEPTED'
                WHEN error_code = '30' THEN 'ERROR_BANK_BRANCH_NOT_EXIST'
                WHEN error_code = '31' THEN 'ERROR_INVALID_BANK_ACCOUNT_NO_FOR_CITAD'
                WHEN error_code = '32' THEN 'ERROR_BANK_AMOUNT_LIMIT'
                WHEN error_code = '33' THEN 'ERROR_NO_SUPPORTED_SERVICE'
                WHEN error_code = '34' THEN 'ERROR_NO_SUPPORTED_REMITTANCE_TYPE'
                WHEN error_code = '35' THEN 'ERROR_GATEWAY_INACTIVE' 
                WHEN error_code = '36' THEN 'ERROR_BANK_BRANCH_NOT_MATCH'
                WHEN error_code = '41' THEN 'GATEWAY_TIMEOUT'
                WHEN error_code = '98' THEN 'ERROR_SERVER' 
                WHEN error_code = '99' THEN 'ERROR_BANK_UNKNOWN_ERROR' 
                WHEN error_code = '100' THEN 'ERROR_TXN_TOTAL_AMOUNT_PER_PARTNER_PER_DAY' 
                WHEN error_code = '101' THEN 'ERROR_TXN_AMOUNT_LIMIT' 
                WHEN error_code = '102' THEN 'ERROR_DAILY_TOTAL_AMOUNT_PER_ACCOUNT' 
                WHEN error_code = '103' THEN 'ERROR_DAILY_TOTAL_COUNT_PER_PARTNER' 
                WHEN error_code = '104' THEN 'ERROR_TXN_COUNT_SAME_ACCOUNT_AMOUNT' 
                WHEN error_code = '105' THEN 'ERROR_HOURLY_TXN_COUNT_PER_ACCOUNT' 
                WHEN error_code = '106' THEN 'ERROR_DAILY_TXN_COUNT_PER_ACCOUNT' 
                WHEN error_code = '107' THEN 'ERROR_BLACK_LIST' 
                WHEN error_code = '108' THEN 'INVALID_DEBIT_ACCOUNT' 
                ELSE 'UNKNOWN REASON'
            END error_reason
        ,   IF(
                data_fix_json IS NOT NULL, 
                CONCAT(GET_JSON_OBJECT(data_fix_json, '$.jira'), '-', GET_JSON_OBJECT(data_fix_json, '$.memo'), '-', GET_JSON_OBJECT(data_fix_json, '$.operator')), 
                NULL
            )   data_fix
        ,   DATE(ctime) cdate 
        ,   DATE(mtime) mdate
        ,   extra_data
    FROM 
        (
            SELECT  DISTINCT
                    a.reference_id 
                ,   a.source_type
                ,   a.amount/100000 amount
                ,   CASE a.status WHEN 2 THEN 'DONE' END status
                ,   FROM_UNIXTIME(a.ctime-3600) ctime
                ,   FROM_UNIXTIME(a.mtime-3600) mtime
                ,   GET_JSON_OBJECT(b.extra_data, '$.general_payout_status') as general_payout_status
                ,   b.entity_id
                ,   b.transaction_id
                ,   c.payment_id
                ,   CASE    --  payment_channel
                        WHEN c.channel_id = 5007201 THEN 'VA'
                        WHEN c.channel_id = 5007400 THEN 'VA'
                        WHEN c.channel_id = 5002800 THEN 'iBanking'
                        WHEN c.channel_id = 5001800 THEN 'Airpay Wallet V2'
                        WHEN c.channel_id = 5001300 THEN 'Shopee Wallet'
                        WHEN c.channel_id = 5002900 THEN 'Airpay GIRO'
                        WHEN c.channel_id = 5000101 THEN 'Cybersource Installment'
                        WHEN c.channel_id = 5000102 THEN 'Cybersource New '
                        WHEN c.channel_id = 5002601 THEN 'SPayLater'
                        WHEN c.channel_id = 5014000 THEN 'ApplePay'
                        WHEN c.channel_id = 5011000 THEN 'Airpay Authpay'
                        WHEN c.channel_id = 5003200 THEN 'IS PAYOUT'
                        WHEN c.channel_id = 5016900 THEN 'Vn ShopeePay Wallet Payout'
                        WHEN c.channel_id = 5003100 THEN 'Manual Bank Transfer'
                        WHEN c.channel_id = 5001600 THEN 'Payoneer VN'
                        WHEN c.channel_id = 5015700 THEN 'Merchant Wallet Service Payout'
                        WHEN c.channel_id = 5005100 THEN 'Cash On Delivery'
                        ELSE NULL 
                    END payment_channel
                ,   CASE    --  payment_status
                        WHEN c.status = 2   THEN 'PAYMENT_INIT'
                        WHEN c.status = 8   THEN 'PENDING'
                        WHEN c.status = 6   THEN 'USER_PROCESSING'
                        WHEN c.status = 20  THEN 'SUCCESS'
                        WHEN c.status = 22  THEN 'FAILED'
                        WHEN c.status = 50  THEN 'CANCELED'
                        WHEN c.status = 51  THEN 'CANCEL_READY'
                        WHEN c.status = 54  THEN 'EXPIRED'	--- new added---
                        WHEN c.status = 100 THEN 'FRAUD'
                        WHEN c.status = 0   THEN 'INITIAL'
                        WHEN c.status = 14  THEN 'RECONCILING'
                        WHEN c.status = 10  THEN 'BLOCKED'
                        WHEN c.status = 26  THEN 'SUCCESS_BUT_REJECTED_BY_SERVER'
                        WHEN c.status = 24  THEN 'LATE_SUCCESS'
                    END payment_status
                ,   GET_JSON_OBJECT(a.extra_data, '$.client_payment_data.bank_account')       bank_account 
                ,   GET_JSON_OBJECT(a.extra_data, '$.client_payment_data.shopeepay_wallet')   shopeepay_wallet 
                ,   IF(
                        REGEXP_LIKE(GET_JSON_OBJECT(c.extra_data, '$.fail_reason'), 'u\'') = TRUE , 
                        REPLACE(REPLACE(GET_JSON_OBJECT(c.extra_data, '$.fail_reason'), 'u\'', '\''), '\'', '"'), 
                        GET_JSON_OBJECT(c.extra_data, '$.fail_reason')
                    )   raw_fail_reason
                ,   CAST(
                        GET_JSON_OBJECT(
                            IF(
                                GET_JSON_OBJECT(c.extra_data, '$.commit_data') IS NULL,
                                GET_JSON_OBJECT(c.extra_data, '$.commit_result'),
                                GET_JSON_OBJECT(c.extra_data, '$.commit_data')
                            ), '$.error_code'
                        )   AS STRING 
                    )   error_code  
                ,   GET_JSON_OBJECT(c.extra_data, '$.data_fix') data_fix_json
                ,   c.extra_data
            FROM    shopeepay.shopee_payment_module_vn_db__payment_source_tab__reg_daily_s0_live a 
            LEFT JOIN shopeepay.shopee_payment_module_provision_vn_db__provision_v2_tab__vn_daily_s0_live b  on b.entity_id = a.source_id and b.channel_id = 5003101
            LEFT JOIN shopeepay.shopee_payment_module_payment_vn_db__payment_v2_tab__vn_daily_s2_live c      on b.transaction_id = c.transaction_id
            WHERE   a.source_type = 200003901
            ORDER BY 5 DESC  
        )
)
SELECT 
        reference_id 
    ,   source_type
    ,   amount 
    ,   status
    ,   CAST(ctime AS TIMESTAMP) ctime
    ,   CAST(mtime AS TIMESTAMP) mtime
    ,   general_payout_status 
    ,   entity_id 
    ,   transaction_id 
    ,   payment_id 
    ,   payment_channel 
    ,   payment_status 
    ,   withdraw_method
    ,   error_code
    ,   IF( payment_status = 'FAILED'
            ,   CASE 
                    WHEN fail_reason    IS NOT NULL THEN UPPER( CAST(fail_reason     AS STRING)) 
                    WHEN data_fix       IS NOT NULL THEN        CAST(data_fix        AS STRING)
                    WHEN error_reason   IS NOT NULL THEN UPPER( CAST(error_reason    AS STRING))
                END
            , NULL 
        )   fail_reason
    ,   DATE(ctime) cdate 
    ,   DATE(mtime) mdate 
    ,   extra_data
FROM    base 
WHERE   DATE(ctime) < CURRENT_DATE
;
