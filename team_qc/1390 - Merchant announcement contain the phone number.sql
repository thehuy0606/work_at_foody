WITH 
base_lv1 AS 
(
    SELECT DISTINCT 
            b.id 
        ,   b.restaurant_id merchant_id 
        ,   c.merchant_name 
        ,   b.content  
        ,   FROM_UNIXTIME(b.start_time  -3600)          start_time
        ,   FROM_UNIXTIME(b.end_time    -3600)          end_time 
        ,   CASE announcement_status 
                WHEN 1 THEN 'PENDING'
                WHEN 2 THEN 'PROCESSING'
                WHEN 3 THEN 'APPROVED'
                WHEN 4 THEN 'DENIED'
            END status_content 
        ,   DATE(FROM_UNIXTIME(b.create_time -3600))    create_date 
        ,   REGEXP_EXTRACT(b.content, '\b\d{10}\b')     ex_number_content 
    from shopeefood.foody_merchant_db__restaurant_announcement_tab__reg_daily_s0_live b 
    JOIN shopeefood.foody_mart__profile_merchant_master c on b.restaurant_id = c.merchant_id 
    WHERE   1=1 
        AND c.grass_date = 'current' 
        AND c.is_active_flag = 1 
        AND b.is_deleted = 0 
        AND REGEXP_EXTRACT(b.content, '\b\d{10}\b') IS NOT NULL 
    ORDER BY 1, 2
) ,
result AS 
(
    SELECT  b.merchant_id 
        ,   c.service 
        ,   c.mex_create_date 
        ,   IF(c.is_active_flag_text = 'Yes', 'Active', 'Inactive') merchant_status 
        ,   IF(c.is_verified_merchant = 1, 'verify', 'non-verify')  merchant_type
        ,   c.merchant_name 
        ,   c.address_text 
        ,   c.district_name 
        ,   c.city_name 
        ,   b.content 
        ,   b.start_time 
        ,   b.end_time 
        ,   b.status_content 
        ,   b.create_date 
        ,   b.ex_number_content 
        ,   c.bde_email bde_email_incharge
        ,   c.segment 
    FROM    base_lv1 b 
    LEFT JOIN dev_vnfdbi_commercial.shopeefood_vn_food_mex_contract_master c ON b.merchant_id = c.merchant_id 
    ORDER BY b.merchant_id, c.mex_create_date, b.create_date, b.start_time
)
select * from result 
