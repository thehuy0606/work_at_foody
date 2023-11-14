with 
raw as 
    (
        select 
                t1.city_id
            ,   case 
                    when cast( json_extract(t1.extra_data, '$.order.app_type') as int) = 1004 then 'NOW'
                    when cast( json_extract(t1.extra_data, '$.order.app_type') as int) = 2000 then 'FOODY' 
                    when cast( json_extract(t1.extra_data, '$.order.app_type') as int) = 3000 then 'SHOPEE'
                    else 'Other' 
                end as order_source
            ,   case 
                    when cast( json_extract(t1.extra_data, '$.order.foody_service_id') as int) = 1 then 'Food'
                    else 'Fresh' 
                end as Service_name
            ,   from_unixtime(t1.create_time - 3600) as created_time
            ,   t1.order_id
            ,   t1.order_code
            ,   cast( json_extract(t1.extra_data, '$.order.original_price') as double) as original_price
            ,   case when t1.status_id =7 then 'Completed' else 'Uncompleted' end as status
            ,   promotions.promotion_id
            ,   promotions.promotion_code
            ,   promotions.discount_type
            ,   promotions.discount as total_discount
            ,   promotions.discount_on_type
            ,   promotions.partner_share
            ,   promotions.create_time Promotion_create_time
        from shopeefood.foody_accountant_db__order_delivery_tab__reg_daily_s0_live t1
        cross join unnest 
            (
                cast(json_extract(t1.extra_data, '$.order.promotions') 
                as array(row(   promotion_id bigint
                            ,   promotion_code varchar
                            ,   discount double 
                            ,   discount_type int
                            ,   discount_on_type int
                            ,   create_time bigint
                            ,   partner_share json
                            )
                        )
                    ) 
            )as promotions
        where 1=1
            and json_array_length(json_extract(t1.extra_data, '$.order.promotions')) > 0
            and date(from_unixtime(delivered_date - 3600)) between date(${param_start_date}) and date(${param_end_date}) -- filter date

        union

        select 
                t1.city_id
            ,   case 
                    when cast( json_extract(t1.extra_data, '$.order.app_type') as int) = 1004 then 'NOW'
                    when cast( json_extract(t1.extra_data, '$.order.app_type') as int) = 2000 then 'FOODY' 
                    when cast( json_extract(t1.extra_data, '$.order.app_type') as int) = 3000 then 'SHOPEE'
                    else 'Other' 
                end as order_source
            ,   case 
                    when cast( json_extract(t1.extra_data, '$.order.foody_service_id') as int) = 1 then 'Food'
                    else 'Fresh' 
                end as Service_name
            ,   from_unixtime(t1.create_time - 3600) as created_time
            ,   t1.order_id
            ,   t1.order_code
            ,   cast( json_extract(t1.extra_data, '$.order.original_price') as double) as original_price
            ,   case when t1.status_id =7 then 'Completed' else 'Uncompleted' end as status
            ,   promotions.promotion_id
            ,   promotions.promotion_code
            ,   promotions.discount_type
            ,   promotions.discount as total_discount
            ,   promotions.discount_on_type
            ,   promotions.partner_share
            ,   promotions.create_time Promotion_create_time
        from shopeefood.foody_accountant_archive_db__order_delivery_tab__reg_daily_s0_live t1
        cross join unnest 
            (
                cast(json_extract(t1.extra_data, '$.order.promotions') 
                as array(row(   promotion_id bigint
                            ,   promotion_code varchar
                            ,   discount double 
                            ,   discount_type int
                            ,   discount_on_type int
                            ,   create_time bigint
                            ,   partner_share json
                            )
                        )
                    ) 
            )as promotions

        where 1=1
            and json_array_length(json_extract(t1.extra_data, '$.order.promotions')) > 0
        and date(from_unixtime(delivered_date - 3600)) between date(${param_start_date}) and date(${param_end_date}) -- filter date
    ),
city as 
    (
        select  
                id as city_id 
            ,   name as city_name 
        from shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live
        where country_id = 86 
    ),
main_query as 
    (
        select 
                t1.order_source
            ,   t1.order_id
            ,   t1.promotion_id
            ,   t1.Service_name
            ,   t1.promotion_code
            ,   t1.Promotion_create_time
            ,   count(t1.order_id) over(partition by t1.order_id, promotion_id) as dup_id
            ,   t1.original_price as gmv
            ,   null as cod
            ,   t1.total_discount as total_discount
            ,   case when partner_share.partner = 1 then partner_share.discount else 0 end as Discount_foody_share
            ,   case when partner_share.partner = 2 then partner_share.discount else 0 end as Discount_airpay_share
            ,   case when partner_share.partner = 3 then partner_share.discount else 0 end as Discount_shopee_share
            ,   case when partner_share.partner not in (1,2,3) then partner_share.discount else 0 end as Discount_others_share
        from raw t1
        join city t2 on t1.city_id = t2.city_id
        cross join unnest (cast(t1.partner_share as array(row(partner int , percent int , discount double)))) as  partner_share
        where   1=1
            and partner_share.percent <> 0
            and status  in ${param_order_status} --'Completed','Uncompleted'
    ), 
agg0 as 
    (
        select 
                t1.order_source
            ,   t1.Service_name
            ,   t1.promotion_code
                -- ,   t2.promocode_note as Promotion_name
                -- ,   coalesce(t4.promotion_note,t2.promocode_note) as Promotion_name -- update 8 sept 2022
                -- ,   coalesce(t4.discount_category,t2.discount_category) as Discount_type_name  -- update 8 sept 2022
            ,   t4.promotion_note as Promotion_name -- update 8 sept 2022
            ,   t4.discount_category as Discount_type_name  -- update 8 sept 2022
                -- ,   t2.discount_category as Discount_type_name
                -- ,   t2.discount_type as Apply_on_name 
                -- ,   coalesce(t4.discount_type, t2.discount_type) Apply_on_name  -- update 8 sept 2022
            ,   case when  t4.discount_type is null and t4.discount_category = 'Price Slash Discount' then 'Total_bill' else t4.discount_type end as  Apply_on_name
            ,   t1.Promotion_create_time
            ,   count(distinct t1.order_id) total_order
            ,   sum(t1.gmv/dup_id) as gmv
            ,   t1.cod
            ,   sum(t1.total_discount/dup_id) as total_discount
            ,   sum(t1.Discount_foody_share) Discount_foody_share
            ,   sum(t1.Discount_airpay_share) Discount_airpay_share
            ,   sum(t1.Discount_shopee_share) Discount_shopee_share
            ,   sum(t1.Discount_others_share) Discount_others_share
        from main_query t1
        left join shopeefood.foody_mart__fact_order_promotion t4 on t4.order_id = t1.order_id and t1.promotion_id = t4.promotion_id 
        group by 1,2,3,4,5,6,7,cod
    )
,agg1 as 
(
	select 
            case 
                when Apply_on_name = 'Shipping Fee' then 'Shipping Fee'
                when Apply_on_name in ('Total_bill', 'Total Dish') then 'Food discount'
            end as "Apply on name"
        ,   case 
                when Apply_on_name = 'Shipping Fee' and Discount_type_name = 'Free Ship' then 'Flatship'
                when Apply_on_name = 'Shipping Fee' and Discount_type_name = 'PromoCode' then 'Freeship'
                else '' 
            end as "Discount type name"
        ,   case 
                when order_source in ('NOW', 'FOODY') then 'Shopeefood'
                when order_source = 'SHOPEE' then 'Shopee'
                else ''
            end as "Source name"
        ,   sum(Discount_foody_share) as foody
        ,   sum(Discount_airpay_share) as airpay 
        ,   sum(Discount_shopee_share) as shopee 
        ,   sum(Discount_others_share) as others 
        ,   sum(Discount_foody_share+Discount_airpay_share+Discount_shopee_share+Discount_others_share) as total
    from agg0     
    group by 1,2,3
    order by 1 desc, 2, 3 desc
)
,report as 
(	
	select a.* 
	from 
	(
		(
			(select * from agg1 where "Apply on name" = 'Shipping Fee')
			union
			select 
					'Shipping Fee Total' as "Apply on name"
				,	'' as "Discount type name"
				,	'' as "Source name"
				,	sum(foody) as foody
				,	sum(airpay) as airpay
				,	sum(shopee) as shopee
				,	sum(others) as others
				,	sum(total) as total
			from agg1 where "Apply on name" = 'Shipping Fee'
		)
		union
		(
			(select * from agg1 where "Apply on name" = 'Food discount' )
			union
			select 
					'Food discount Total' as "Apply on name"
				,	'' as "Discount type name"
				,	'' as "Source name"
				,	sum(foody) as foody
				,	sum(airpay) as airpay
				,	sum(shopee) as shopee
				,	sum(others) as others
				,	sum(total) as total
			from agg1 where "Apply on name" = 'Food discount'
		)
		union 
		(
			select 
					'Total' as "Apply on name"
				,	'' as "Discount type name"
				,	'' as "Source name"
				,	sum(foody) as foody
				,	sum(airpay) as airpay
				,	sum(shopee) as shopee
				,	sum(others) as others
				,	sum(total) as total
			from agg1 
		)
	) a
)
select * from report order by 1 desc, 2, 3 desc
