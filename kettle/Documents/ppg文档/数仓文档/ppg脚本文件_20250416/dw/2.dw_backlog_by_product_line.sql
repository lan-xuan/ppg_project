
/*
目标表：fine_dw.dw_backlog_by_product_line
来源表：
fine_dw.dw_bodyshop_distributor_all
fine_dw.dw_ship_to_list
fine_dw.dw_customer_master_list
fine_dw.dw_item_flag
fine_dw.dw_item_brand
fine_dw.dw_order_filter
fine_dw.dw_transaction_detail_sh_link
fine_dw.dw_ireport_sub_brand 

fine_ods.ods_calendar_info_df
fine_ods.ods_backlog_by_product_line_df
fine_ods.ods_transaction_detail_report_df

更新方式：全量更新


*/
with temp_channel as (
select 		 'MM' as channel_2
union all 
select 		 'MSO' as channel_2
union all 
select 		 'SCHOOL' as channel_2
union all 
select 		 'DISTRIBUTOR' as channel_2
), aa as ( -- 获取vendor信息


			-- 改byd和其他
				select 
					t.plant_code,
					t.warehouse_code,
					t.sbu,
					t.order_entry_date,
					t.sales_date,
					t.promised_date,
					t.schedule_ship_date,
					t.order_no,
					t.line_number,
					t.order_line_status,
					t.salesrep,
					t.customer_code,
					t.customer_name,
					t.customer_gl_class,
					t.customer_po,
					t.ship_to_code,
					t.item_code,
					t.item_description,
					t.brand_name,
					t.product_type,
					t.order_type,
					t.item_gl_class,
					t.inventory_class,
					t.planning_class,
					t.item_gl_prod_line,
					t.user_name,
					t.sales_qty,
					t.order_outstanding_quantity,
					t.onhand_quantity,
					t.committed_quantity,
					t.free_quantity,
					t.total_backlog_volume_qty_coo_l,
					t.total_backlog_volume_fs,
					t.item_cost_usd,
					t.total_item_cost_fs_usd,
					t.currency_code,
					t.unit_selling_price,
					t.backlog_late,
					t.from_1_to_30_days,
					t.from_31_to_60_days,
					t.from_61_to_90_days,
					t.more_than_90_days,
					t.total_backlog_usd,
					t.sales_value,
					case when  REPLACE(t.customer_code, 'CN', '') = '207196' or  REPLACE(t.customer_code, 'CN', '') = '183492' then  REPLACE(bodyshop1.vendor_code, 'CN', '') else REPLACE(bodyshop2.vendor_code, 'CN', '') end as vendor_code,
					REPLACE(t.customer_code, 'CN', '') as customer_code_new

					FROM fine_ods.ods_backlog_by_product_line_df t
			-- 				WHERE customer_code <> '152818'
					left join fine_dw.dw_bodyshop_distributor_all bodyshop1 -- 比亚迪
					on bodyshop1.ship_to_code=t.ship_to_code
					and bodyshop1.ship_to_code is not null
					and  REPLACE(bodyshop1.customer_code, 'CN', '')=REPLACE(t.customer_code, 'CN', '')

					left join fine_dw.dw_bodyshop_distributor_all bodyshop2 -- 其他
					on UPPER(bodyshop2.warehouse_code)=UPPER(t.warehouse_code)
					-- and bodyshop2.customer_code=REPLACE(t.customer_code, 'CN', '')
					and bodyshop2.warehouse_code is not null
					-- where t.customer_code = '207196' -- 领克
					-- where t.customer_code = '166741'
			-- 		WHERE update_date ='20240131'
					where 1=1
			-- 		and update_date in ('202401') -- ,'20230131'
					and REPLACE(t.customer_code, 'CN', '') not in('210481' ,'212488')
-- 					and REPLACE(t.customer_code, 'CN', '') = '179225'


			),temp_dw_ship_to_list as (
select  ship.*
       ,STR_TO_DATE(ship.starting_date, '%Y%m%d') as starting_date_2
       ,STR_TO_DATE(ship.ending_date, '%Y%m%d') as ending_date_2
       ,upper(ship.customer_code) as customer_code_2
	   ,ship.customer_name as customer_name_2
       ,upper(ship.ship_to_code) as ship_to_code_2
 from fine_dw.dw_ship_to_list  ship
),temp_dw_customer_master_list as (

select * 
from fine_dw.dw_customer_master_list
  -- where sales_month in ( ${mysql_yesterday_d_month},${mysql_yesterday_l_month} ) 
--    where sales_month  in ('202408')
where 1=1
and DATE_FORMAT(NOW(), '%Y%m')  = sales_month


)
			, b as (
					select 
					case when customer_code_new = '210481' then 'MM' else channel end as channel,
					district,
					customer_code_2,
					customer_name_2,
					t.*

					from aa t
					-- 第1步：
					-- 匹配ods.ods_transaction_detail_report_df.customer_code=ods_ship_to_list_df.customer_code
					-- and ods.ods_transaction_detail_report_df.ship_to_code=ods_ship_to_list_df.ship_to_code
					-- 获得channel、district其中proj_name、proj_name_en、is_flag字段为空（ship_to只包含distributor和school的信息）
		left join  temp_dw_ship_to_list ship
		on upper(customer_code_new) = ship.customer_code_2
		and upper(t.ship_to_code)=  ship.ship_to_code_2
		and ship.starting_date_2 <= STR_TO_DATE(t.sales_date, '%Y%m%d')
		and ship.ending_date_2 >= STR_TO_DATE(t.sales_date, '%Y%m%d')

			)
			,bb as
			(
					SELECT
					case when t.channel is null then master1.channel else t.channel end as channel1,
					case when master1.channel in ('MM','MSO') then master2.district when master1.channel not in ('MM','MSO') and t.district is null then master1.district else t.district end as district1,
					master1.proj_name,
					master1.proj_name_en,
					master1.is_flag,
		case when customer_code_2 is not null then customer_code_2 else master1.u_customer_code end as u_customer_code,
		case when customer_name_2 is not null then customer_name_2 else master1.u_customer_name end as u_customer_name,
					t.vendor_code,
					t.customer_code_new,
					--
					t.channel,
					t.district,
							t.plant_code,
					t.warehouse_code,
					t.sbu,
					t.order_entry_date,
					t.sales_date,
					t.promised_date,
					t.schedule_ship_date,
					t.order_no,
					t.line_number,
					t.order_line_status,
					t.salesrep,
					master1.u_customer_code as customer_code,
					master1.u_customer_name as customer_name,
					-- t.customer_code,
					-- t.customer_name,
					t.customer_gl_class,
					t.customer_po,
					t.ship_to_code,
					t.item_code,
					t.item_description,
					t.brand_name,
					t.product_type,
					t.order_type,
					t.item_gl_class,
					t.inventory_class,
					t.planning_class,
					t.item_gl_prod_line,
					t.user_name,
					t.sales_qty,
					t.order_outstanding_quantity,
					t.onhand_quantity,
					t.committed_quantity,
					t.free_quantity,
					t.total_backlog_volume_qty_coo_l,
					t.total_backlog_volume_fs,
					t.item_cost_usd,
					t.total_item_cost_fs_usd,
					t.currency_code,
					t.unit_selling_price,
					t.backlog_late,
					t.from_1_to_30_days,
					t.from_31_to_60_days,
					t.from_61_to_90_days,
					t.more_than_90_days,
					t.total_backlog_usd,
					t.sales_value

					from b t

					-- 第2步：剩余的channel、district为空的
					-- 匹配ods.ods_customer_master_list_df.customer_code=ods.ods_transaction_detail_report_df.customer_code
					-- 获得channel、district、proj_name、proj_name_en、is_flag
					left join fine_dw.dw_customer_master_list master1
					on customer_code_new = master1.customer_code 
					-- and STR_TO_DATE(master1.starting_date, '%Y%m%d') <= STR_TO_DATE(t.sales_date, '%Y%m%d')
					-- and STR_TO_DATE(master1.ending_date, '%Y%m%d') >= STR_TO_DATE(t.sales_date, '%Y%m%d')
					and SUBSTR(t.sales_date,1,6) = master1.sales_month

					left join fine_dw.dw_customer_master_list master2 -- mm\mso的根据vendor_code匹配district
					on vendor_code = master2.customer_code 
					-- and STR_TO_DATE(master2.starting_date, '%Y%m%d') <= STR_TO_DATE(t.sales_date, '%Y%m%d')
					-- and STR_TO_DATE(master2.ending_date, '%Y%m%d') >= STR_TO_DATE(t.sales_date, '%Y%m%d')
					and SUBSTR(t.sales_date,1,6) = master2.sales_month

					where 1=1

			)
			, temp_brand  as (
	select distinct
	item_code,
	category,
	category_brand,
	category_product_type
	from fine_dw.dw_item_brand
	where channel is null

),bbb as (
					select 
							channel1 as channel,
							case when t.ship_to_code='333878' OR t.ship_to_code='523444' then  "大众仓"  else t.district1 end as district,
							case when isflag.is_flag = '是' then isflag.is_flag else  t.is_flag end as is_flag,
							brand.category  as category,
							brand.category_brand  as category_brand,
							brand.category_product_type  as category_product_type,

					t.vendor_code,
							m.u_customer_name as vendor_name,

					t.proj_name,
							t.proj_name_en,

							t.u_customer_code,
							t.u_customer_name,

					-- 原表数据

							t.plant_code,
					t.warehouse_code,
					t.sbu,
					t.order_entry_date,
							DATE_FORMAT(STR_TO_DATE(t.sales_date, '%Y%m%d'),'%Y%m%d') as sales_date,
							DATE_FORMAT(STR_TO_DATE(t.sales_date, '%Y%m%d'),'%Y%m')  as sales_month,
					t.promised_date,
					t.schedule_ship_date,
					t.order_no,
					t.line_number,
					t.order_line_status,
					t.salesrep,
					-- t.customer_code_new as customer_code,
					t.u_customer_code as customer_code,
					t.u_customer_name as customer_name,
					t.customer_gl_class,
					t.customer_po,
					t.ship_to_code,
					t.item_code,
					t.item_description,
					t.brand_name,
					t.product_type,
					t.order_type,
					t.item_gl_class,
					t.inventory_class,
					t.planning_class,
					t.item_gl_prod_line,
					t.user_name,
					case when t.order_type = 'RETURN ORDER - PPG PICKUP - SH' then -t.sales_qty else t.sales_qty end as sales_qty,
					t.order_outstanding_quantity,
					t.onhand_quantity,
					t.committed_quantity,
					t.free_quantity,
					t.total_backlog_volume_qty_coo_l,
					t.total_backlog_volume_fs,
					t.item_cost_usd,
					t.total_item_cost_fs_usd,
					t.currency_code,
					t.unit_selling_price,
					t.backlog_late,
					t.from_1_to_30_days,
					t.from_31_to_60_days,
					t.from_61_to_90_days,
					t.more_than_90_days,
					t.total_backlog_usd,
					case when t.order_type = 'RETURN ORDER - PPG PICKUP - SH' then -t.sales_value else t.sales_value end as sales_value,
				'fine_dw.dw_bodyshop_distributor_all/fine_dw.dw_ship_to_list/fine_dw.dw_customer_master_list/fine_dw.dw_item_flag/fine_dw.dw_item_brand/fine_dw.dw_order_filter/fine_dw.dw_transaction_detail_sh_link/fine_dw.dw_ireport_sub_brand/fine_ods.ods_calendar_info_df/fine_ods.ods_backlog_by_product_line_df/fine_ods.ods_transaction_detail_report_df' as data_resource,
				SYSDATE() as etl_time
					from bb t

		-- 第3.1步：根据ods_ireport_sub_brand_df.item_code匹配获得report_brand_group、report_brand_name
		-- channel是MM/MSO的
		-- ods_transaction_detail_report_df.item_no = ods.ods_item_maping_cb_df.item_no
		-- 获得新列名3个category、category_brand、category_product_type
		-- if category_brand isnull，去distributor mapping表匹配获得产品信息
		-- channel是D/School的
		-- ods_transaction_detail_report_df.item_no = ods.ods_item_maping_distributor_df.item_no
		-- 获得新列名3个category、category_brand、category_product_type

        -- 匹配ods_item_flag_df.item_code，如果is_flag='是'则该字段为‘是’
        -- 其他根据项目走，剩余的为否
        left join fine_dw.dw_item_flag isflag
        on upper(isflag.item_code) = upper(t.item_code)

		left join temp_brand brand
		on upper(brand.item_code) = upper(t.item_code)


		-- -- 第10步：when sales_volume isnull, 与dw_item_cost_detail匹配item_code使用最新update_date获得uom_ltr*qty值作为该字段, when UOM_Ltr isnull取quantity值作为该字段
		left join (
		   select item_code,uom_ltr,update_date from 
		            (SELECT item_code,uom_ltr,update_date
		                  ,ROW_NUMBER() OVER (PARTITION BY item_code ORDER BY update_date DESC) AS seq  
		                FROM fine_ods.ods_item_cost_detail_df 
		                ) A
		                WHERE A.seq =1 
		
		      ) cost
		on cost.item_code = t.item_code

		LEFT JOIN temp_dw_customer_master_list m
		on upper(m.customer_code) = upper(t.vendor_code)
        and m.sales_month = SUBSTR(t.sales_date,1,6)
	  -- 第5步：order_type仅包含配置表中的order_type类型，具体查看sheet页-order filter
		inner join  (select order_type 
		                from fine_dw.dw_order_filter) order_filter

		on  t.order_type = order_filter.order_type
		
	  inner join temp_channel
	  on  t.channel1 = temp_channel.channel_2

		where 1=1
		
		-- 第4步：salesrep <>"no sales credit"
		and upper(salesrep) <> upper('no sales credit') -- ------
		-- 第6步：item_code notlike “.%” 但保留“.Price Adj” and “.LINQ IFLOW SOFTWARE FEE“
		and (t.item_code NOT LIKE '.%'  OR upper(t.item_code) = upper('.Price Adj') OR UPPER(t.item_code) = UPPER('.LINQ IFLOW SOFTWARE FEE') )
		-- 第7步：customer_po <> "Shortage"
		and (upper(t.customer_po) <> UPPER('Shortage') or customer_po is null)
		-- 第8步：customer code not in  (195726,203684)--备货单位
		-- and t.u_customer_code not in  ('195726','203684')
		and   (t.u_customer_code <>'195726' and t.u_customer_code <> '203684' )
     -- and t.item_code = 'P565-808/5K-C3'
     -- and t.item_code = 'P565-808/5K-C3'

			)-- SELECT * from bbb;
			,

temp_b3  as (
select DISTINCT UPPER(category_brand) as category_brand ,report_brand_name,report_brand_group FROM fine_dw.dw_ireport_sub_brand where category_brand is not null
),
temp_b1  as (
select DISTINCT UPPER(item_code) as item_code ,report_brand_name,report_brand_group FROM fine_dw.dw_ireport_sub_brand where item_code is not null
),
temp_b2  as (
select DISTINCT UPPER(category) as category ,report_brand_name,report_brand_group FROM fine_dw.dw_ireport_sub_brand where category is not null
)


			select 
					bbb.channel,
					bbb.district,
					bbb.is_flag,
					bbb.category,
					bbb.category_brand,
					bbb.category_product_type,
		case when ship.customer_code_2 is not null then bbb.vendor_code else m.u_customer_code end as vendor_code,
		case when ship.customer_name_2 is not null then bbb.vendor_name else m.u_customer_name end as vendor_name,
					bbb.proj_name,
					bbb.proj_name_en,
					bbb.plant_code,
					bbb.warehouse_code,
					bbb.sbu,
					bbb.order_entry_date,
					bbb.sales_date,
					bbb.sales_month, -- 需要
					bbb.promised_date,
					bbb.schedule_ship_date,
					bbb.order_no,
					bbb.line_number,
					bbb.order_line_status,
					bbb.salesrep,
					bbb.customer_code,
					bbb.customer_name,
					bbb.customer_gl_class,
					bbb.customer_po,
					bbb.ship_to_code,
					bbb.item_code,
					bbb.item_description,
					bbb.brand_name,
					bbb.product_type,
					bbb.order_type,
					bbb.item_gl_class,
					bbb.inventory_class,
					bbb.planning_class,
					bbb.item_gl_prod_line,
					bbb.user_name,
					bbb.sales_qty,
					bbb.order_outstanding_quantity,
					bbb.onhand_quantity,
					bbb.committed_quantity,
					bbb.free_quantity,
					bbb.total_backlog_volume_qty_coo_l,
					bbb.total_backlog_volume_fs,
					bbb.item_cost_usd,
					bbb.total_item_cost_fs_usd,
					bbb.currency_code,
					bbb.unit_selling_price,
					bbb.backlog_late,
					bbb.from_1_to_30_days,
					bbb.from_31_to_60_days,
					bbb.from_61_to_90_days,
					bbb.more_than_90_days,
					bbb.total_backlog_usd,
					bbb.sales_value,
					bbb.data_resource,
					bbb.etl_time,
			-- 				report_brand_name,
			-- 				report_brand_group
			-- 				sales_quarter, -- 需要
			-- 				business_type, -- 需要
			-- DISTINCT
COALESCE(b1.report_brand_name, b2.report_brand_name, b3.report_brand_name,'OTHERS') as report_brand_name,
COALESCE(b1.report_brand_group, b2.report_brand_group, b3.report_brand_group,'OTHERS')  as report_brand_group,
actual_quarter as sales_quarter,
order_type_name as business_type,
SUBSTRING(bbb.sales_month,1,4) as sales_year
			from bbb 
			-- 第3.1步：根据ods_ireport_sub_brand_df.item_code匹配获得report_brand_group、report_brand_name
LEFT JOIN  temp_b3  b3
on UPPER(bbb.category_brand) = b3.category_brand
LEFT JOIN temp_b1  b1
on UPPER(bbb.item_code) = b1.item_code
LEFT JOIN  temp_b2 b2
on UPPER(bbb.category) = b2.category
LEFT JOIN fine_ods.ods_calendar_info_df
on sales_month = actual_month
LEFT JOIN fine_dw.dw_order_filter o
on bbb.order_type = o.order_type

		LEFT JOIN  temp_dw_ship_to_list ship
		on  bbb.vendor_code = ship.customer_code_2
		and bbb.ship_to_code=  ship.ship_to_code_2
		and ship.starting_date_2 <= STR_TO_DATE(bbb.sales_date, '%Y%m%d')
		and ship.ending_date_2 >= STR_TO_DATE(bbb.sales_date, '%Y%m%d')

		LEFT JOIN temp_dw_customer_master_list m
		on m.customer_code = upper(bbb.vendor_code)
        and m.sales_month = SUBSTR(bbb.sales_date,1,6)
			-- where  upper(bbb.category_brand) in( 'IFLOW','BELCO PLUS')
			-- WHERE customer_code = '210481'
			-- WHERE channel = 'Distributor' -- 85872332.1000
			-- and order_type <> 'SALES GAP ORDER-SH'
			-- limit 1
where bbb.sales_month = ${mysql_yesterday_d_month}
-- 只保留当月数据 