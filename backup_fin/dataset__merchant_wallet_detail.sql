-------------------Version 2-----------------
with wallet_info as 
(
select id as wallet_id, airpay_uid, name , email, permanent_address, if(status = 1 ,'ACTIVE', 'REVIEW') AS status
from shopeefood.foody_pay_merchant_store_db__merchant_store_tab__reg_daily_s0_live
where id <> 1001
)
,calendar as (
    select *
    from unnest(sequence(date'2017-09-06',${end_date} - interval '1' day )) t(data_date)
    )
    ,wallet as (
     select
     date(from_unixtime(create_time -3600)) as date
    ,uid
    ,id
    ,cash_balance/1000000 cash_balance                                                                                                      
    , row_number() over( partition by uid,date(from_unixtime(create_time -3600)) order by id desc) as rn
  from shopeefood.foody_pay_txn_db__user_cash_history_tab__reg_daily_s0_live a
   where date(from_unixtime(create_time -3600)) <= ${end_date} - interval '1' day 
    )
    ,CB_raw as (
    select 
        a.data_date,
        b.uid,
        b.cash_balance,
        b.date
    from calendar a cross join wallet b
    where b.rn = 1
    -- and  uid = 1669642
    and data_date >= date
    )
    ,CB_final as (
    select 
    data_date,
    uid,
    cash_balance,
    row_number() over ( partition by data_date, uid order by date desc) as rn
    from CB_raw 
    where data_date >= date
    )
    ,CB as (
    select data_date, b.id uid, sum(cash_balance) CB from CB_final a
    join shopeefood.foody_pay_merchant_store_db__merchant_store_tab__reg_daily_s0_live b on a.uid = b.airpay_uid
     where data_date between ${start_date} - interval '1' day and current_date
    
    and rn = 1
    group by 1,2)

,split_detail as
(select 
  order_id
  ,type
  ,status
  ,currency_amount /1000000 as amount_1
  , payment_channel_id
  , item_amount
  , sum( case when payment_channel_id in (21104,21106,21204,21206,21102,21100) and  t.amount < 0 then  - t.amount 
            when payment_channel_id in (21104,21106,21204,21206,21102,21100) and t.amount >= 0 then 0 
            when  payment_channel_id  not in (21104,21106,21204,21206,21102,21100) and t.amount >= 0 then t.amount else 0 end ) as  amount
    ,sum( case when payment_channel_id in (21104,21106,21204,21206,21102,21100) and  t.amount >= 0 then  - t.amount
            when payment_channel_id in (21104,21106,21204,21206,21102,21100) and  t.amount < 0 then  0
            when  payment_channel_id  not in (21104,21106,21204,21206,21102,21100) and t.amount < 0 then t.amount 
        else 0  end ) as  refund
  from shopeefood.foody_pay_txn_db__order_tab__reg_daily_s0_live
   cross join unnest (  case when payment_channel_id in (21104,21106,21204,21206,21102,21100) then
        cast (json_extract( extra_data, '$.topup.merchant_order_details.bill_items') as array (row(amount int)))
    else
    cast( json_extract( extra_data, '$.payment.merchant_order_details.bill_items')  as array (row(amount int)))
   
        end  ) as t
 where   1 = 1
    and payment_channel_id in (21107,21101,21103,21105                
                                ,21104,21106,21204,21206,21102,21100) 
    and   cast(uid as integer) != 1618703
    and(date(from_unixtime(valid_time-3600)) between ${start_date} and ${end_date}
         or (date(from_unixtime(create_time-3600)) between ${start_date} and ${end_date} and valid_time = 0) )
  group by 1,2,3,4,5,6
  ),

 raw as (
select  b.id as wallet_id
        ,b.name
        ,c.merchant_ref as merchant_id
        ,c.name as merchant_name
        ,case   when a.payment_channel_id = 21019 then 'CREDIT_VIA_DIRECT_TOPUP'
                when a.payment_channel_id = 21043 then 'CREDIT_DIRECT_DEDUCT'
                when a.payment_channel_id = 21041 then 'CASH_VIA_DIRECT_TOPUP'
                when a.payment_channel_id = 21042 then 'CASH_DIRECT_DEDUCT'
                when a.payment_channel_id = 21020 then 'PAYMENT_NOW_DELI_PAY'
                when a.payment_channel_id = 21021 then 'CASH_VIA_NOW_DELI_PAY'
                when a.payment_channel_id = 21022 then 'PAYMENT_NOW_DELI_REFUND'
                when a.payment_channel_id = 21023 then 'CREDIT_VIA_NOW_DELI_REFUND'
                when a.payment_channel_id = 21044 then 'PAYMENT_NOW_DELI_CLAIM_COMMISSION' 
                when a.payment_channel_id = 21045 then 'CREDIT_VIA_NOW_DELI_CLAIM_COMMISSION'
                when a.payment_channel_id = 21046 then 'PAYMENT_NOW_DELI_RETURN_COMMISSION'
                when a.payment_channel_id = 21047 then 'CASH_VIA_NOW_DELI_RETURN_COMMISSION'
                when a.payment_channel_id = 21100 then 'PAYMENT_NOW_SHIP_PAY_COD'
                when a.payment_channel_id = 21101 then 'CASH_VIA_NOW_SHIP_PAY_COD' 
                when a.payment_channel_id = 21102 then 'PAYMENT_NOW_SHIP_RECEIVE_COD'
                when a.payment_channel_id = 21103 then 'CREDIT_VIA_NOW_SHIP_RECEIVE_COD'
                when a.payment_channel_id = 21104 then 'PAYMENT_NOW_SHIP_CLAIM_RETURN_FEE'   
                when a.payment_channel_id = 21105 then 'CREDIT_VIA_NOW_SHIP_CLAIM_RETURN_FEE'
                when a.payment_channel_id = 21106 then 'PAYMENT_NOW_SHIP_REFUND_RETURN_FEE'
                when a.payment_channel_id = 21107 then 'CASH_VIA_NOW_SHIP_REFUND_RETURN_FEE'
                when a.payment_channel_id = 21200 then 'PAYMENT_NOW_DELI_GIVE'
                when a.payment_channel_id = 21201 then 'CASH_VIA_NOW_DELI_GIVE'
                when a.payment_channel_id = 21202 then 'PAYMENT_NOW_DELI_RECEIVE'
                when a.payment_channel_id = 21203 then 'CREDIT_VIA_NOW_DELI_RECEIVE'
                when a.payment_channel_id = 21204 then 'PAYMENT_NOW_SHIP_GIVE'
                when a.payment_channel_id = 21205 then 'CASH_VIA_NOW_SHIP_GIVE'
                when a.payment_channel_id = 21206 then 'PAYMENT_NOW_SHIP_RECEIVE'
                when a.payment_channel_id = 21207 then 'CREDIT_VIA_NOW_SHIP_RECEIVE'
                when a.payment_channel_id = 21018 then 'CASH_REMITTANCE'
                when a.payment_channel_id = 21024 and json_extract(a.extra_data, '$.payment.__req__') is not null then 'AUTO_CASH_REMITTANCE'
                when a.payment_channel_id = 21024 and json_extract(a.extra_data, '$.payment.__req__') is  null then 'MANUAL_CASH_REMITTANCE'
                when a.payment_channel_id = 21028 then 'CASH_GIRO_WITHDRAWAL'
                when a.payment_channel_id = 21029 then 'WALLET_CASH_DIRECT_TOPUP'
                when a.payment_channel_id = 21025 then 'PAYMENT_WALLET_CASH_TRANSFER_TO_WITHDRAWAL'
                when a.payment_channel_id = 21026 then 'CASH_VIA_WALLET_CASH_TRANSFER_TO_WITHDRAWAL'
                when a.payment_channel_id = 21027 then 'WALLET_CASH_GIRO_WITHDRAWAL'
                when a.payment_channel_id = 21141 then 'CASH_VIA_DIRECT_TOPUP'
                when a.payment_channel_id = 21142 then 'CASH_DIRECT_DEDUCT'

                else 'UNKNOWN'
         end as product_type
        ,case   when a.status = -4 then 'CANCELED'
                when a.status = -3 then 'EXPIRED_DELETED'
                when a.status = -2 then 'FAILED_DELETED'
                when a.status = -1 then 'FAILED'
                when a.status = 0 then 'INITIAL'
                when a.status = 1 then 'EXECUTE_TOPUP'
                when a.status = 2 then 'EXECUTE_PAYMENT'
                when a.status = 3 then 'NEED_STAFF'
                when a.status = 4 then 'FAIL_TOPUP'
                when a.status = 5 then 'FAIL_PAYMENT'
                when a.status = 6 then 'NEED_ACTION'
                when a.status = 7 then 'NEED_REFUND'
                when a.status = 8 then 'COMPLETED'
                when a.status = 9 then 'REFUNDED'
                when a.status = 10 then 'LOCKED'
                when a.status = 11 then 'COMPLETING'
                else 'UNKNOWN'
         end as status,
        a.order_id
        -- ,d.id
        ,coalesce(e.amount,a.currency_amount / 1000000) as amount 
        ,coalesce(e.refund,0 ) as refund
        ,from_unixtime(a.create_time-3600) as create_time
        ,from_unixtime(a.valid_time-3600) as payment_time
        ,from_unixtime(a.update_time-3600) as update_time
from    shopeefood.foody_pay_txn_db__order_tab__reg_daily_s0_live a
left join shopeefood.foody_pay_merchant_store_db__merchant_store_tab__reg_daily_s0_live b
on      a.uid = b.airpay_uid
left join shopeefood.foody_pay_merchant_store_db__store_tab__reg_daily_s0_live c
on      a.payment_account_id = cast(c.id as varchar)
left join split_detail e on e.order_id = a.order_id


where   1 = 1
 and((date(from_unixtime(a.valid_time-3600))between ${start_date} and ${end_date} )
         or (date(from_unixtime(a.create_time-3600)) between ${start_date} and ${end_date} and a.valid_time = 0)
          )
  and   cast(a.uid as integer) != 1618703
--   and a.payment_channel_txn_id = '13042-854301314'
union
select  b.id as wallet_id
        ,b.name
        ,c.merchant_ref as merchant_id
        ,c.name as merchant_name
        ,case   when a.payment_channel_id = 21019 then 'CREDIT_VIA_DIRECT_TOPUP'
                when a.payment_channel_id = 21043 then 'CREDIT_DIRECT_DEDUCT'
                when a.payment_channel_id = 21041 then 'CASH_VIA_DIRECT_TOPUP'
                when a.payment_channel_id = 21042 then 'CASH_DIRECT_DEDUCT'
                when a.payment_channel_id = 21020 then 'PAYMENT_NOW_DELI_PAY'
                when a.payment_channel_id = 21021 then 'CASH_VIA_NOW_DELI_PAY'
                when a.payment_channel_id = 21022 then 'PAYMENT_NOW_DELI_REFUND'
                when a.payment_channel_id = 21023 then 'CREDIT_VIA_NOW_DELI_REFUND'
                when a.payment_channel_id = 21044 then 'PAYMENT_NOW_DELI_CLAIM_COMMISSION' 
                when a.payment_channel_id = 21045 then 'CREDIT_VIA_NOW_DELI_CLAIM_COMMISSION'
                when a.payment_channel_id = 21046 then 'PAYMENT_NOW_DELI_RETURN_COMMISSION'
                when a.payment_channel_id = 21047 then 'CASH_VIA_NOW_DELI_RETURN_COMMISSION'
                when a.payment_channel_id = 21100 then 'PAYMENT_NOW_SHIP_PAY_COD'
                when a.payment_channel_id = 21101 then 'CASH_VIA_NOW_SHIP_PAY_COD' 
                when a.payment_channel_id = 21102 then 'PAYMENT_NOW_SHIP_RECEIVE_COD'
                when a.payment_channel_id = 21103 then 'CREDIT_VIA_NOW_SHIP_RECEIVE_COD'
                when a.payment_channel_id = 21104 then 'PAYMENT_NOW_SHIP_CLAIM_RETURN_FEE'   
                when a.payment_channel_id = 21105 then 'CREDIT_VIA_NOW_SHIP_CLAIM_RETURN_FEE'
                when a.payment_channel_id = 21106 then 'PAYMENT_NOW_SHIP_REFUND_RETURN_FEE'
                when a.payment_channel_id = 21107 then 'CASH_VIA_NOW_SHIP_REFUND_RETURN_FEE'
                when a.payment_channel_id = 21200 then 'PAYMENT_NOW_DELI_GIVE'
                when a.payment_channel_id = 21201 then 'CASH_VIA_NOW_DELI_GIVE'
                when a.payment_channel_id = 21202 then 'PAYMENT_NOW_DELI_RECEIVE'
                when a.payment_channel_id = 21203 then 'CREDIT_VIA_NOW_DELI_RECEIVE'
                when a.payment_channel_id = 21204 then 'PAYMENT_NOW_SHIP_GIVE'
                when a.payment_channel_id = 21205 then 'CASH_VIA_NOW_SHIP_GIVE'
                when a.payment_channel_id = 21206 then 'PAYMENT_NOW_SHIP_RECEIVE'
                when a.payment_channel_id = 21207 then 'CREDIT_VIA_NOW_SHIP_RECEIVE'
                when a.payment_channel_id = 21018 then 'CASH_REMITTANCE'
                when a.payment_channel_id = 21024 and json_extract(a.extra_data, '$.payment.__req__') is not null then 'AUTO_CASH_REMITTANCE'
                when a.payment_channel_id = 21024 and json_extract(a.extra_data, '$.payment.__req__') is  null then 'MANUAL_CASH_REMITTANCE'
                when a.payment_channel_id = 21028 then 'CASH_GIRO_WITHDRAWAL'
                when a.payment_channel_id = 21029 then 'WALLET_CASH_DIRECT_TOPUP'
                when a.payment_channel_id = 21025 then 'PAYMENT_WALLET_CASH_TRANSFER_TO_WITHDRAWAL'
                when a.payment_channel_id = 21026 then 'CASH_VIA_WALLET_CASH_TRANSFER_TO_WITHDRAWAL'
                when a.payment_channel_id = 21027 then 'WALLET_CASH_GIRO_WITHDRAWAL'
                when a.payment_channel_id = 21141 then 'CASH_VIA_DIRECT_TOPUP'
                when a.payment_channel_id = 21142 then 'CASH_DIRECT_DEDUCT'

                else 'UNKNOWN'
         end as product_type
        ,case   when a.status = -4 then 'CANCELED'
                when a.status = -3 then 'EXPIRED_DELETED'
                when a.status = -2 then 'FAILED_DELETED'
                when a.status = -1 then 'FAILED'
                when a.status = 0 then 'INITIAL'
                when a.status = 1 then 'EXECUTE_TOPUP'
                when a.status = 2 then 'EXECUTE_PAYMENT'
                when a.status = 3 then 'NEED_STAFF'
                when a.status = 4 then 'FAIL_TOPUP'
                when a.status = 5 then 'FAIL_PAYMENT'
                when a.status = 6 then 'NEED_ACTION'
                when a.status = 7 then 'NEED_REFUND'
                when a.status = 8 then 'COMPLETED'
                when a.status = 9 then 'REFUNDED'
                when a.status = 10 then 'LOCKED'
                when a.status = 11 then 'COMPLETING'
                else 'UNKNOWN'
         end as status,
        a.order_id
        -- ,d.id
        ,coalesce(e.amount,a.currency_amount / 1000000) as amount 
        ,coalesce(e.refund,0 ) as refund
        ,from_unixtime(a.create_time-3600) as create_time
        ,from_unixtime(a.update_time-3600) as payment_time
        ,from_unixtime(a.update_time-3600) as update_time
from    shopeefood.foody_pay_txn_db__order_tab__reg_daily_s0_live a
left join shopeefood.foody_pay_merchant_store_db__merchant_store_tab__reg_daily_s0_live b
on      a.uid = b.airpay_uid
left join shopeefood.foody_pay_merchant_store_db__store_tab__reg_daily_s0_live c
on      a.payment_account_id = cast(c.id as varchar)
left join split_detail e on e.order_id = a.order_id

where   1 = 1
 and date(from_unixtime(a.update_time-3600))between ${start_date} and ${end_date}
 and date(from_unixtime(a.update_time-3600)) <> date(from_unixtime(a.valid_time-3600))
  and   cast(a.uid as integer) != 1618703
  and a.status = 9

-- this is for the case that auto withdraw late----
union
select 
    b.id as wallet_id
    ,b.name
    ,'dummy' as merchant_id
    ,'dummy' as merchant_name
    ,'AUTO_CASH_REMITTANCE' as product_type
    ,'COMPLETED' as status
    ,a.order_id
    , case when date(from_unixtime(a.create_time-3600)) = current_date then  -cash_amount / 1000000 else 0 end as amount 
    , 0 as refund
    , from_unixtime(a.create_time-3600) as create_time
    , from_unixtime(a.create_time-3600) as payment_time
    , from_unixtime(a.create_time-3600) as update_time

 from shopeefood.foody_pay_txn_db__user_cash_history_tab__reg_daily_s0_live a
 left join shopeefood.foody_pay_merchant_store_db__merchant_store_tab__reg_daily_s0_live b
on      a.uid = b.airpay_uid
where 1=1
and type in (541)
and  uid = 1897469
and date(from_unixtime(a.create_time-3600)) between ${start_date} and ${end_date}
and hour(from_unixtime(a.create_time-3600)) in (3,4)

-- this is for the case that auto withdraw late----
union
select 
    b.id as wallet_id
    ,b.name
    ,'dummy' as merchant_id
    ,'dummy' as merchant_name
    ,'AUTO_CASH_REMITTANCE' as product_type
    ,'COMPLETED' as status
    ,a.order_id
    , case when date(from_unixtime(a.create_time-3600)) = current_date  and current_date =  date'2022-07-01' then  -cash_amount / 1000000 else 0 end as amount 
    , 0 as refund
    , from_unixtime(a.create_time-3600) as create_time
    , from_unixtime(a.create_time-3600) as payment_time
    , from_unixtime(a.create_time-3600) as update_time

 from shopeefood.foody_pay_txn_db__user_cash_history_tab__reg_daily_s0_live a
 left join shopeefood.foody_pay_merchant_store_db__merchant_store_tab__reg_daily_s0_live b
on      a.uid = b.airpay_uid
where 1=1
and type in (541)
and  uid = 1915230
and date(from_unixtime(a.create_time-3600)) between ${start_date} and ${end_date}
and hour(from_unixtime(a.create_time-3600)) in (2)

  )
-- select 
-- a.data_date
--     ,t1.name
--     ,t1.status
--     , coalesce(t2.CB,0) opening_balance
--     , sum(case when product_type not in ('CASH_VIA_DIRECT_TOPUP','CREDIT_VIA_DIRECT_TOPUP','MANUAL_CASH_REMITTANCE'
--                                         ,'PAYMENT_NOW_DELI_CLAIM_COMMISSION','AUTO_CASH_REMITTANCE','CASH_DIRECT_DEDUCT','CREDIT_DIRECT_DEDUCT') then amount  else 0 end ) as pay_amount
--     , sum(case when product_type in ('CASH_VIA_DIRECT_TOPUP','CREDIT_VIA_DIRECT_TOPUP') then amount + refund else 0 end ) as direct_topup_amount
--     , sum(case when product_type in ('MANUAL_CASH_REMITTANCE','AUTO_CASH_REMITTANCE') and raw.status = 'REFUNDED' and date(raw.create_time) != date(raw.payment_time) then amount
--                 when product_type in ('MANUAL_CASH_REMITTANCE','AUTO_CASH_REMITTANCE') and raw.status = 'REFUNDED' and date(raw.create_time) = date(raw.update_time)  then amount  
--                 else 0 end) as withdrawal_refund
--     , sum(coalesce(refund,0)) - sum(case when product_type in ('PAYMENT_NOW_DELI_CLAIM_COMMISSION','CREDIT_VIA_NOW_DELI_CLAIM_COMMISSION') then amount else 0 end) as payment_refund
--     , - sum(case when product_type in ('MANUAL_CASH_REMITTANCE') and date(raw.payment_time) = date(raw.create_time) then amount + refund else 0 end ) as mannual_withdrawal
--     , - sum(case when product_type in ('AUTO_CASH_REMITTANCE') and date(raw.payment_time) = date(raw.create_time) then amount + refund else 0 end ) as auto_withdrawal
--     , - sum(case when product_type in ('CASH_DIRECT_DEDUCT','CREDIT_DIRECT_DEDUCT') then amount + refund else 0 end ) as direct_deduct_amount
--     , coalesce(t3.CB,0) closing_balance
--     , t1.wallet_id
--     , t1.email
--     , t1.permanent_address
-- from calendar a
-- left join raw
--     on     date(case 
--             when raw.status = 'REFUNDED' then payment_time
--             when raw.status = 'FAILED_DELETED' then  create_time
--             when raw.status = 'CANCELED' then create_time
--             else date(payment_time) end ) = a.data_date
--             and raw.status in ('COMPLETED','REFUNDED' )
-- left join CB t2 on  a.data_date = t2.data_date + interval '1' day
--                         and T2.uid = raw.wallet_id
-- left join CB t3 on  a.data_date = t3.data_date
--                         and t3.uid = raw.wallet_id


-- left join wallet_info t1 on t1.wallet_id = t3.uid

-- where 1=1
-- and a.data_date between date'2022-06-25' and date'2022-06-27'
-- -- AND t3.uid = 33895
-- group by 1,2,3,4,t1.wallet_id,t1.email,t1.permanent_address,coalesce(t3.CB,0)

-- select * from cb where uid = 33895 and data_date between date'2022-06-24' and date'2022-06-27'
-- 
-- union 
, process_cb as (
select 
    a.data_date
    ,t1.name
    ,t1.status
    -- ,coalesce(t2.CB,0) opening_balance
    , coalesce(t3.CB,0) closing_balance
    , t1.wallet_id
    , t1.email
    , t1.permanent_address
from calendar a
-- left join CB t2 on  a.data_date = t2.data_date + interval '1' day
LEFT join CB t3 on  a.data_date = t3.data_date
                        -- and t3.uid = raw.wallet_id
-- left join raw on t3.uid = raw.wallet_id
left join wallet_info t1 on t1.wallet_id = t3.uid
where 1=1
and a.data_date between ${start_date} and ${end_date}
-- and t3.uid not in (select raw.wallet_id from raw where status in ('COMPLETED','REFUNDED' ))
-- and raw.wallet_id is null
-- AND t3.uid = 33895
group by 1,2,3,4,5,6,7
-- order by t1.wallet_id 
)

, process_ob as (
select 
    a.data_date
    ,name
    ,status
    ,coalesce(t2.CB,0) opening_balance
    , closing_balance
    , a.wallet_id
    , email
    , permanent_address
from process_cb a
left join CB t2 on  a.data_date = t2.data_date + interval '1' day and t2.uid = a.wallet_id
-- LEFT join CB t3 on  a.data_date = t3.data_date
                        -- and t3.uid = raw.wallet_id
-- left join raw on t3.uid = raw.wallet_id
-- left join wallet_info t1 on t1.wallet_id = t3.uid
where 1=1
and a.data_date between ${start_date} and ${end_date}
-- and t3.uid not in (select raw.wallet_id from raw where status in ('COMPLETED','REFUNDED' ))
-- and raw.wallet_id is null
-- AND t3.uid = 33895
group by 1,2,3,4,5,6,7,8
-- order by t1.wallet_id 
)

select 
    a.data_date date
    ,a.name
    ,a.status
    ,a.opening_balance
    , sum(case when product_type not in ('CASH_VIA_DIRECT_TOPUP','CREDIT_VIA_DIRECT_TOPUP','MANUAL_CASH_REMITTANCE'
                                        ,'PAYMENT_NOW_DELI_CLAIM_COMMISSION','AUTO_CASH_REMITTANCE','CASH_DIRECT_DEDUCT','CREDIT_DIRECT_DEDUCT') then amount  else 0 end ) as pay_amount
    , sum(case when product_type in ('CASH_VIA_DIRECT_TOPUP','CREDIT_VIA_DIRECT_TOPUP') then amount + refund else 0 end ) as direct_topup_amount
    , sum(case when product_type in ('MANUAL_CASH_REMITTANCE','AUTO_CASH_REMITTANCE') and raw.status = 'REFUNDED' and date(raw.create_time) != date(raw.payment_time) then amount
                when product_type in ('MANUAL_CASH_REMITTANCE','AUTO_CASH_REMITTANCE') and raw.status = 'REFUNDED' and date(raw.create_time) = date(raw.update_time)  then amount  
                else 0 end) as withdrawal_refund
    , sum(coalesce(refund,0)) - sum(case when product_type in ('PAYMENT_NOW_DELI_CLAIM_COMMISSION','CREDIT_VIA_NOW_DELI_CLAIM_COMMISSION') then amount else 0 end) as payment_refund
    , - sum(case when product_type in ('MANUAL_CASH_REMITTANCE') and date(raw.payment_time) = date(raw.create_time) then amount + refund else 0 end ) as mannual_withdrawal
    , - sum(case when product_type in ('AUTO_CASH_REMITTANCE') and date(raw.payment_time) = date(raw.create_time) then amount + refund else 0 end ) as auto_withdrawal
    , - sum(case when product_type in ('CASH_DIRECT_DEDUCT','CREDIT_DIRECT_DEDUCT') then amount + refund else 0 end ) as direct_deduct_amount
    , a.closing_balance
    , a.wallet_id
    , a.email
    , a.permanent_address
from process_ob a 
left join raw
    on     case 
            when raw.status = 'REFUNDED' then date(payment_time)
            when raw.status = 'FAILED_DELETED' then date( create_time)
            when raw.status = 'CANCELED' then date(create_time)
            else date(payment_time) end  = a.data_date
            and raw.status in ('COMPLETED','REFUNDED' )
            and raw.wallet_id = a.wallet_id
group by 
    a.data_date
    ,a.name
    ,a.status
    ,a.opening_balance
    , a.closing_balance
    , a.wallet_id
    , a.email
    , a.permanent_address



-------------------Version 1-----------------
-- with wallet_info as 
-- (
-- select id as wallet_id, airpay_uid, name , email, permanent_address, if(status = 1 ,'ACTIVE', 'REVIEW') AS status
-- from shopeefood.foody_pay_merchant_store_db__merchant_store_tab__reg_daily_s0_live
-- where id <> 1001
-- )
-- ,
-- split_detail as
-- (select 
--   order_id
--   ,type
--   ,status
--   ,currency_amount /1000000 as amount_1
--   , payment_channel_id
--   , item_amount
--   , sum( case when payment_channel_id in (21104,21106,21204,21206,21102,21100) and  t.amount < 0 then  - t.amount 
--             when payment_channel_id in (21104,21106,21204,21206,21102,21100) and t.amount >= 0 then 0 
--             when  payment_channel_id  not in (21104,21106,21204,21206,21102,21100) and t.amount >= 0 then t.amount else 0 end ) as  amount
--     ,sum( case when payment_channel_id in (21104,21106,21204,21206,21102,21100) and  t.amount >= 0 then  - t.amount
--             when payment_channel_id in (21104,21106,21204,21206,21102,21100) and  t.amount < 0 then  0
--             when  payment_channel_id  not in (21104,21106,21204,21206,21102,21100) and t.amount < 0 then t.amount 
--         else 0  end ) as  refund
--   from shopeefood.foody_pay_txn_db__order_tab__reg_daily_s0_live
--    cross join unnest (  case when payment_channel_id in (21104,21106,21204,21206,21102,21100) then
--         cast (json_extract( extra_data, '$.topup.merchant_order_details.bill_items') as array (row(amount int)))
--     else
--     cast( json_extract( extra_data, '$.payment.merchant_order_details.bill_items')  as array (row(amount int)))
   
--         end  ) as t
--  where   1 = 1
--     and payment_channel_id in (21107,21101,21103,21105                
--                                 ,21104,21106,21204,21206,21102,21100) 
--     and   cast(uid as integer) != 1618703
--     and(date(from_unixtime(valid_time-3600)) between ${start_date} and ${start_date}
--          or (date(from_unixtime(create_time-3600)) between ${start_date} and ${start_date} and valid_time = 0) )
--   group by 1,2,3,4,5,6
--   ),
-- OB as 
-- (
--     select date,wallet_id,sum(cash_balance) as opening_balance 
--     from (
--     select
--     date(${start_date}) - interval '1' day as date
--     ,b.id as wallet_id
--     ,cash_balance/1000000 cash_balance
--     ,a.id
--     , row_number() over( partition by a.uid order by a.id desc) as rn
--   from shopeefood.foody_pay_txn_db__user_cash_history_tab__reg_daily_s0_live a
--   join shopeefood.foody_pay_merchant_store_db__merchant_store_tab__reg_daily_s0_live b on a.uid = b.airpay_uid
--    where date(from_unixtime(a.create_time -3600)) <= date(${start_date}) - interval '1' day
--     )
--     where rn = 1 
--     group by 1,2      
-- ),
-- CB as 
-- (
--     select date,wallet_id,sum(cash_balance) as closing_balance 
--     from (
--     select
--     date(${start_date}) as date
--     ,b.id as wallet_id
--     ,cash_balance/1000000 cash_balance
--     ,a.id
--     , row_number() over( partition by a.uid order by a.id desc) as rn
--   from shopeefood.foody_pay_txn_db__user_cash_history_tab__reg_daily_s0_live a
--   join shopeefood.foody_pay_merchant_store_db__merchant_store_tab__reg_daily_s0_live b on a.uid = b.airpay_uid
--    where date(from_unixtime(a.create_time -3600)) <= ${start_date}  )
--     where rn = 1 
--     group by 1,2    
-- ),
--  raw as (
-- select  b.id as wallet_id
--         ,b.name
--         ,c.merchant_ref as merchant_id
--         ,c.name as merchant_name
--         ,case   when a.payment_channel_id = 21019 then 'CREDIT_VIA_DIRECT_TOPUP'
--                 when a.payment_channel_id = 21043 then 'CREDIT_DIRECT_DEDUCT'
--                 when a.payment_channel_id = 21041 then 'CASH_VIA_DIRECT_TOPUP'
--                 when a.payment_channel_id = 21042 then 'CASH_DIRECT_DEDUCT'
--                 when a.payment_channel_id = 21020 then 'PAYMENT_NOW_DELI_PAY'
--                 when a.payment_channel_id = 21021 then 'CASH_VIA_NOW_DELI_PAY'
--                 when a.payment_channel_id = 21022 then 'PAYMENT_NOW_DELI_REFUND'
--                 when a.payment_channel_id = 21023 then 'CREDIT_VIA_NOW_DELI_REFUND'
--                 when a.payment_channel_id = 21044 then 'PAYMENT_NOW_DELI_CLAIM_COMMISSION' 
--                 when a.payment_channel_id = 21045 then 'CREDIT_VIA_NOW_DELI_CLAIM_COMMISSION'
--                 when a.payment_channel_id = 21046 then 'PAYMENT_NOW_DELI_RETURN_COMMISSION'
--                 when a.payment_channel_id = 21047 then 'CASH_VIA_NOW_DELI_RETURN_COMMISSION'
--                 when a.payment_channel_id = 21100 then 'PAYMENT_NOW_SHIP_PAY_COD'
--                 when a.payment_channel_id = 21101 then 'CASH_VIA_NOW_SHIP_PAY_COD' 
--                 when a.payment_channel_id = 21102 then 'PAYMENT_NOW_SHIP_RECEIVE_COD'
--                 when a.payment_channel_id = 21103 then 'CREDIT_VIA_NOW_SHIP_RECEIVE_COD'
--                 when a.payment_channel_id = 21104 then 'PAYMENT_NOW_SHIP_CLAIM_RETURN_FEE'   
--                 when a.payment_channel_id = 21105 then 'CREDIT_VIA_NOW_SHIP_CLAIM_RETURN_FEE'
--                 when a.payment_channel_id = 21106 then 'PAYMENT_NOW_SHIP_REFUND_RETURN_FEE'
--                 when a.payment_channel_id = 21107 then 'CASH_VIA_NOW_SHIP_REFUND_RETURN_FEE'
--                 when a.payment_channel_id = 21200 then 'PAYMENT_NOW_DELI_GIVE'
--                 when a.payment_channel_id = 21201 then 'CASH_VIA_NOW_DELI_GIVE'
--                 when a.payment_channel_id = 21202 then 'PAYMENT_NOW_DELI_RECEIVE'
--                 when a.payment_channel_id = 21203 then 'CREDIT_VIA_NOW_DELI_RECEIVE'
--                 when a.payment_channel_id = 21204 then 'PAYMENT_NOW_SHIP_GIVE'
--                 when a.payment_channel_id = 21205 then 'CASH_VIA_NOW_SHIP_GIVE'
--                 when a.payment_channel_id = 21206 then 'PAYMENT_NOW_SHIP_RECEIVE'
--                 when a.payment_channel_id = 21207 then 'CREDIT_VIA_NOW_SHIP_RECEIVE'
--                 when a.payment_channel_id = 21018 then 'CASH_REMITTANCE'
--                 when a.payment_channel_id = 21024 and json_extract(a.extra_data, '$.payment.__req__') is not null then 'AUTO_CASH_REMITTANCE'
--                 when a.payment_channel_id = 21024 and json_extract(a.extra_data, '$.payment.__req__') is  null then 'MANUAL_CASH_REMITTANCE'
--                 when a.payment_channel_id = 21028 then 'CASH_GIRO_WITHDRAWAL'
--                 when a.payment_channel_id = 21029 then 'WALLET_CASH_DIRECT_TOPUP'
--                 when a.payment_channel_id = 21025 then 'PAYMENT_WALLET_CASH_TRANSFER_TO_WITHDRAWAL'
--                 when a.payment_channel_id = 21026 then 'CASH_VIA_WALLET_CASH_TRANSFER_TO_WITHDRAWAL'
--                 when a.payment_channel_id = 21027 then 'WALLET_CASH_GIRO_WITHDRAWAL'
--                 when a.payment_channel_id = 21141 then 'CASH_VIA_DIRECT_TOPUP'
--                 when a.payment_channel_id = 21142 then 'CASH_DIRECT_DEDUCT'

--                 else 'UNKNOWN'
--          end as product_type
--         ,case   when a.status = -4 then 'CANCELED'
--                 when a.status = -3 then 'EXPIRED_DELETED'
--                 when a.status = -2 then 'FAILED_DELETED'
--                 when a.status = -1 then 'FAILED'
--                 when a.status = 0 then 'INITIAL'
--                 when a.status = 1 then 'EXECUTE_TOPUP'
--                 when a.status = 2 then 'EXECUTE_PAYMENT'
--                 when a.status = 3 then 'NEED_STAFF'
--                 when a.status = 4 then 'FAIL_TOPUP'
--                 when a.status = 5 then 'FAIL_PAYMENT'
--                 when a.status = 6 then 'NEED_ACTION'
--                 when a.status = 7 then 'NEED_REFUND'
--                 when a.status = 8 then 'COMPLETED'
--                 when a.status = 9 then 'REFUNDED'
--                 when a.status = 10 then 'LOCKED'
--                 when a.status = 11 then 'COMPLETING'
--                 else 'UNKNOWN'
--          end as status,
--         a.order_id
--         -- ,d.id
--         ,coalesce(e.amount,a.currency_amount / 1000000) as amount 
--         ,coalesce(e.refund,0 ) as refund
--         ,from_unixtime(a.create_time-3600) as create_time
--         ,from_unixtime(a.valid_time-3600) as payment_time
--         ,from_unixtime(a.update_time-3600) as update_time
-- from    shopeefood.foody_pay_txn_db__order_tab__reg_daily_s0_live a
-- left join shopeefood.foody_pay_merchant_store_db__merchant_store_tab__reg_daily_s0_live b
-- on      a.uid = b.airpay_uid
-- left join shopeefood.foody_pay_merchant_store_db__store_tab__reg_daily_s0_live c
-- on      a.payment_account_id = cast(c.id as varchar)
-- left join split_detail e on e.order_id = a.order_id


-- where   1 = 1
--  and((date(from_unixtime(a.valid_time-3600))between ${start_date} and ${start_date} )
--          or (date(from_unixtime(a.create_time-3600)) between ${start_date} and ${start_date} and a.valid_time = 0)
--           )
--   and   cast(a.uid as integer) != 1618703
-- --   and a.payment_channel_txn_id = '13042-854301314'
-- union
-- select  b.id as wallet_id
--         ,b.name
--         ,c.merchant_ref as merchant_id
--         ,c.name as merchant_name
--         ,case   when a.payment_channel_id = 21019 then 'CREDIT_VIA_DIRECT_TOPUP'
--                 when a.payment_channel_id = 21043 then 'CREDIT_DIRECT_DEDUCT'
--                 when a.payment_channel_id = 21041 then 'CASH_VIA_DIRECT_TOPUP'
--                 when a.payment_channel_id = 21042 then 'CASH_DIRECT_DEDUCT'
--                 when a.payment_channel_id = 21020 then 'PAYMENT_NOW_DELI_PAY'
--                 when a.payment_channel_id = 21021 then 'CASH_VIA_NOW_DELI_PAY'
--                 when a.payment_channel_id = 21022 then 'PAYMENT_NOW_DELI_REFUND'
--                 when a.payment_channel_id = 21023 then 'CREDIT_VIA_NOW_DELI_REFUND'
--                 when a.payment_channel_id = 21044 then 'PAYMENT_NOW_DELI_CLAIM_COMMISSION' 
--                 when a.payment_channel_id = 21045 then 'CREDIT_VIA_NOW_DELI_CLAIM_COMMISSION'
--                 when a.payment_channel_id = 21046 then 'PAYMENT_NOW_DELI_RETURN_COMMISSION'
--                 when a.payment_channel_id = 21047 then 'CASH_VIA_NOW_DELI_RETURN_COMMISSION'
--                 when a.payment_channel_id = 21100 then 'PAYMENT_NOW_SHIP_PAY_COD'
--                 when a.payment_channel_id = 21101 then 'CASH_VIA_NOW_SHIP_PAY_COD' 
--                 when a.payment_channel_id = 21102 then 'PAYMENT_NOW_SHIP_RECEIVE_COD'
--                 when a.payment_channel_id = 21103 then 'CREDIT_VIA_NOW_SHIP_RECEIVE_COD'
--                 when a.payment_channel_id = 21104 then 'PAYMENT_NOW_SHIP_CLAIM_RETURN_FEE'   
--                 when a.payment_channel_id = 21105 then 'CREDIT_VIA_NOW_SHIP_CLAIM_RETURN_FEE'
--                 when a.payment_channel_id = 21106 then 'PAYMENT_NOW_SHIP_REFUND_RETURN_FEE'
--                 when a.payment_channel_id = 21107 then 'CASH_VIA_NOW_SHIP_REFUND_RETURN_FEE'
--                 when a.payment_channel_id = 21200 then 'PAYMENT_NOW_DELI_GIVE'
--                 when a.payment_channel_id = 21201 then 'CASH_VIA_NOW_DELI_GIVE'
--                 when a.payment_channel_id = 21202 then 'PAYMENT_NOW_DELI_RECEIVE'
--                 when a.payment_channel_id = 21203 then 'CREDIT_VIA_NOW_DELI_RECEIVE'
--                 when a.payment_channel_id = 21204 then 'PAYMENT_NOW_SHIP_GIVE'
--                 when a.payment_channel_id = 21205 then 'CASH_VIA_NOW_SHIP_GIVE'
--                 when a.payment_channel_id = 21206 then 'PAYMENT_NOW_SHIP_RECEIVE'
--                 when a.payment_channel_id = 21207 then 'CREDIT_VIA_NOW_SHIP_RECEIVE'
--                 when a.payment_channel_id = 21018 then 'CASH_REMITTANCE'
--                 when a.payment_channel_id = 21024 and json_extract(a.extra_data, '$.payment.__req__') is not null then 'AUTO_CASH_REMITTANCE'
--                 when a.payment_channel_id = 21024 and json_extract(a.extra_data, '$.payment.__req__') is  null then 'MANUAL_CASH_REMITTANCE'
--                 when a.payment_channel_id = 21028 then 'CASH_GIRO_WITHDRAWAL'
--                 when a.payment_channel_id = 21029 then 'WALLET_CASH_DIRECT_TOPUP'
--                 when a.payment_channel_id = 21025 then 'PAYMENT_WALLET_CASH_TRANSFER_TO_WITHDRAWAL'
--                 when a.payment_channel_id = 21026 then 'CASH_VIA_WALLET_CASH_TRANSFER_TO_WITHDRAWAL'
--                 when a.payment_channel_id = 21027 then 'WALLET_CASH_GIRO_WITHDRAWAL'
--                 when a.payment_channel_id = 21141 then 'CASH_VIA_DIRECT_TOPUP'
--                 when a.payment_channel_id = 21142 then 'CASH_DIRECT_DEDUCT'

--                 else 'UNKNOWN'
--          end as product_type
--         ,case   when a.status = -4 then 'CANCELED'
--                 when a.status = -3 then 'EXPIRED_DELETED'
--                 when a.status = -2 then 'FAILED_DELETED'
--                 when a.status = -1 then 'FAILED'
--                 when a.status = 0 then 'INITIAL'
--                 when a.status = 1 then 'EXECUTE_TOPUP'
--                 when a.status = 2 then 'EXECUTE_PAYMENT'
--                 when a.status = 3 then 'NEED_STAFF'
--                 when a.status = 4 then 'FAIL_TOPUP'
--                 when a.status = 5 then 'FAIL_PAYMENT'
--                 when a.status = 6 then 'NEED_ACTION'
--                 when a.status = 7 then 'NEED_REFUND'
--                 when a.status = 8 then 'COMPLETED'
--                 when a.status = 9 then 'REFUNDED'
--                 when a.status = 10 then 'LOCKED'
--                 when a.status = 11 then 'COMPLETING'
--                 else 'UNKNOWN'
--          end as status,
--         a.order_id
--         -- ,d.id
--         ,coalesce(e.amount,a.currency_amount / 1000000) as amount 
--         ,coalesce(e.refund,0 ) as refund
--         ,from_unixtime(a.create_time-3600) as create_time
--         ,from_unixtime(a.update_time-3600) as payment_time
--         ,from_unixtime(a.update_time-3600) as update_time
-- from    shopeefood.foody_pay_txn_db__order_tab__reg_daily_s0_live a
-- left join shopeefood.foody_pay_merchant_store_db__merchant_store_tab__reg_daily_s0_live b
-- on      a.uid = b.airpay_uid
-- left join shopeefood.foody_pay_merchant_store_db__store_tab__reg_daily_s0_live c
-- on      a.payment_account_id = cast(c.id as varchar)
-- left join split_detail e on e.order_id = a.order_id

-- where   1 = 1
--  and date(from_unixtime(a.update_time-3600))between ${start_date} and ${start_date}
--  and date(from_unixtime(a.update_time-3600)) <> date(from_unixtime(a.valid_time-3600))
--   and   cast(a.uid as integer) != 1618703
--   and a.status = 9

-- -- this is for the case that auto withdraw late----
-- union
-- select 
--     b.id as wallet_id
--     ,b.name
--     ,'dummy' as merchant_id
--     ,'dummy' as merchant_name
--     ,'AUTO_CASH_REMITTANCE' as product_type
--     ,'COMPLETED' as status
--     ,a.order_id
--     , case when date(from_unixtime(a.create_time-3600)) = current_date then  -cash_amount / 1000000 else 0 end as amount 
--     , 0 as refund
--     , from_unixtime(a.create_time-3600) as create_time
--     , from_unixtime(a.create_time-3600) as payment_time
--     , from_unixtime(a.create_time-3600) as update_time

--  from shopeefood.foody_pay_txn_db__user_cash_history_tab__reg_daily_s0_live a
--  left join shopeefood.foody_pay_merchant_store_db__merchant_store_tab__reg_daily_s0_live b
-- on      a.uid = b.airpay_uid
-- where 1=1
-- and type in (541)
-- and  uid = 1897469
-- and date(from_unixtime(a.create_time-3600)) between ${start_date} and ${start_date}
-- and hour(from_unixtime(a.create_time-3600)) in (3,4)
--   )
-- select 
--     date(case 
--             when raw.status = 'REFUNDED' then payment_time
--             when raw.status = 'FAILED_DELETED' then  create_time
--             when raw.status = 'CANCELED' then create_time
--             when raw.status is null then ${start_date}
--             else payment_time end ) as date
--     ,t1.name
--     ,t1.status
--     , coalesce(t2.opening_balance,0) opening_balance
--     , sum(case when product_type not in ('CASH_VIA_DIRECT_TOPUP','CREDIT_VIA_DIRECT_TOPUP','MANUAL_CASH_REMITTANCE'
--                                         ,'PAYMENT_NOW_DELI_CLAIM_COMMISSION','AUTO_CASH_REMITTANCE','CASH_DIRECT_DEDUCT','CREDIT_DIRECT_DEDUCT') then amount  else 0 end ) as pay_amount
--     , sum(case when product_type in ('CASH_VIA_DIRECT_TOPUP','CREDIT_VIA_DIRECT_TOPUP') then amount + refund else 0 end ) as direct_topup_amount
--     , sum(case when product_type in ('MANUAL_CASH_REMITTANCE','AUTO_CASH_REMITTANCE') and raw.status = 'REFUNDED' and date(raw.create_time) != date(raw.payment_time) then amount
--                 when product_type in ('MANUAL_CASH_REMITTANCE','AUTO_CASH_REMITTANCE') and raw.status = 'REFUNDED' and date(raw.create_time) = date(raw.update_time)  then amount  
--                 else 0 end) as withdrawal_refund
--     , sum(coalesce(refund,0)) - sum(case when product_type in ('PAYMENT_NOW_DELI_CLAIM_COMMISSION','CREDIT_VIA_NOW_DELI_CLAIM_COMMISSION') then amount else 0 end) as payment_refund
--     , - sum(case when product_type in ('MANUAL_CASH_REMITTANCE') and date(raw.payment_time) = date(raw.create_time) then amount + refund else 0 end ) as mannual_withdrawal
--     , - sum(case when product_type in ('AUTO_CASH_REMITTANCE') and date(raw.payment_time) = date(raw.create_time) then amount + refund else 0 end ) as auto_withdrawal
--     , - sum(case when product_type in ('CASH_DIRECT_DEDUCT','CREDIT_DIRECT_DEDUCT') then amount + refund else 0 end ) as direct_deduct_amount
--     , coalesce(t3.closing_balance,0) closing_balance
--     , t1.wallet_id
--     , t1.email
--     , t1.permanent_address
-- from wallet_info t1 
-- left join raw  on t1.wallet_id = raw.wallet_id and raw.status in ('COMPLETED','REFUNDED' )
-- left join OB t2 on t1.wallet_id = t2.wallet_id
-- left join CB t3 on t1.wallet_id = t3.wallet_id
-- where 1=1
-- group by 1,2,3,4,t1.wallet_id,t1.email,t1.permanent_address,coalesce(t3.closing_balance,0)
-- order by t1.wallet_id 
