WITH 
base_lv1 AS 
(
    SELECT DISTINCT 
            b.restaurant_id merchant_id
        ,   c.merchant_name
        ,   b.id item_id
        ,   b.name item_name 
        ,   REGEXP_EXTRACT(b.name, '\b\d{10}\b') extracted_number
        ,   b.description description_item
    FROM shopeefood.foody_merchant_db__dish_tab__reg_daily_s0_live b 
    JOIN shopeefood.foody_mart__profile_merchant_master c on b.restaurant_id = c.merchant_id 
    WHERE   1=1 
        AND c.grass_date = 'current' 
        AND c.is_active_flag = 1 
        AND b.is_deleted = 0
        AND REGEXP_EXTRACT(b.name, '\b\d{10}\b') IS NOT NULL  
    ORDER BY 1, 2, 3, 4
)
SELECT  b.merchant_id 
    ,   c.service 
    ,   c.mex_create_date 
    ,   IF(c.is_active_flag_text = 'Yes', 'Active', 'Inactive') merchant_status 
    ,   c.merchant_name 
    ,   c.address_text 
    ,   c.district_name 
    ,   c.city_name 
    ,   b.item_name 
    ,   b.item_id 
    ,   b.extracted_number 
    ,   bde_email 
    ,   c.segment 
    ,   b.description_item 
FROM    base_lv1 b 
LEFT JOIN dev_vnfdbi_commercial.shopeefood_vn_food_mex_contract_master c ON b.merchant_id = c.merchant_id
