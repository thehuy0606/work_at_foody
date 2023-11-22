WITH 
base_lv1 AS 
(
    SELECT DISTINCT 
            b.restaurant_id merchant_id
        ,   c.merchant_name
        ,   d.name  item_category
        ,   b.id item_id
        ,   b.name item_name 
        ,   REGEXP_EXTRACT(b.name, '\b\d{10}\b') ex_number_from_item
        ,   b.description description_item
        ,   REGEXP_EXTRACT(b.description, '\b\d{10}\b') ex_number_from_description
    FROM shopeefood.foody_merchant_db__dish_tab__reg_daily_s0_live b 
    JOIN shopeefood.foody_mart__profile_merchant_master c on b.restaurant_id = c.merchant_id 
    LEFT JOIN shopeefood.foody_merchant_db__dish_type_tab__reg_daily_s0_live d ON b.type_id = d.id
    WHERE   1=1 
        AND c.grass_date = 'current' 
        AND c.is_active_flag = 1 
        AND b.is_deleted = 0
        AND (REGEXP_EXTRACT(b.name, '\b\d{10}\b') IS NOT NULL OR REGEXP_EXTRACT(b.description, '\b\d{10}\b') IS NOT NULL)
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
    ,   b.item_category 
    ,   b.item_name 
    ,   b.item_id 
    ,   b.description_item 
    ,   b.ex_number_from_item 
    ,   b.ex_number_from_description
    ,   bde_email 
    ,   c.segment 
    ,   b.description_item 
FROM    base_lv1 b 
LEFT JOIN dev_vnfdbi_commercial.shopeefood_vn_food_mex_contract_master c ON b.merchant_id = c.merchant_id
