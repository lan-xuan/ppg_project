/*
目标表：fine_dw.dw_transaction_detail_report
来源表：

fine_dw.dw_customer_master_list
fine_dw.dw_order_filter
fine_dw.dw_transaction_detail_sh_link 
fine_dw.dw_bodyshop_distributor_all
fine_dw.dw_ship_to_list
fine_dw.dw_item_brand
fine_dw.dw_item_flag 
fine_dw.dw_ireport_sub_brand

fine_ods.ods_transaction_detail_report_df
fine_ods.ods_item_cost_detail_df 
fine_ods.ods_calendar_info

更新方式：增量更新
更新粒度：month
参数：${mysql_yesterday_d_month}
*/
-- 113643 63397 62231 -- 62240 -- 62088 -- 63087

with  temp_001 as (
select '210481' as customer_code
union all
select '212488' as customer_code

),

temp_dw_customer_master_list as (

select * 
from fine_dw.dw_customer_master_list
 where sales_month in ( ${mysql_yesterday_d_month},${mysql_yesterday_l_month} ) 
--   where sales_month  in ('202306')


),

temp_002 as (
select A.*
      ,upper(A.ship_to_code)   AS ship_to_code_2
      ,UPPER(A.warehouse_code) AS warehouse_code_2
from (select
         t.*
		    FROM fine_ods.ods_transaction_detail_report_df t
where SUBSTRING(update_date,1,6)  in ( ${mysql_yesterday_d_month},${mysql_yesterday_l_month} ) 
--  where  SUBSTRING(update_date,1,6) in ('202306')
		    )  A
left join temp_001
on REPLACE(A.customer_code, 'CN', '') = temp_001.customer_code
where temp_001.customer_code is null
and A.warehouse_code is not null
and A.ship_to_code  is not null
),

-- 领克数据
temp_003 as (
	select
     t.*
	FROM fine_dw.dw_transaction_detail_sh_link t
 --  where SUBSTRING(sales_month,1,6)  in  ('202306')
 where SUBSTRING(sales_month,1,6) in ( ${mysql_yesterday_d_month},${mysql_yesterday_l_month} ) 
),

--  比亚迪
temp_bodyshop1 as (
select t.*
     ,upper(t.ship_to_code)  as ship_to_code_2
from fine_dw.dw_bodyshop_distributor_all t 
where ship_to_code is not null
),


--  其他
temp_bodyshop2 as (
select t.*
     ,UPPER(t.warehouse_code)  as warehouse_code_2
from fine_dw.dw_bodyshop_distributor_all t 
where warehouse_code is not null
),

temp_004 as (
-- 改byd和其他
	select
		t.invoice_no,
		t.order_no,
		t.purchase_order,
		t.customer_po,
		t.delivery_number,
		t.batch_source,
		t.invoice_type,
		t.complete,
		t.status_code,
		t.sales_date,
		t.due_date,
		t.terms,
		t.salesrep,
		t.invoice_currency_code,
		t.gl_class,
		t.ship_to_customer_code,
		t.ship_to_customer_name,
		t.ship_to_code,
		t.customer_code,
		t.customer_name,
		t.line_no,
		t.item_code,
		t.aero_platform,
		t.brand_name,
		t.customer_item_description,
		t.invoice_line_description,
		t.sales_qty,
		t.shipped_qty_kg,
		t.sales_volume,
		t.credit_qty,
		t.unit_price,
		t.extended_amount,
		t.tax_amount,
		t.uom,
		t.waybill,
		t.invoice_remark,
		t.local_currency_code,
		t.warehouse_code,
		t.order_date,
		t.actual_ship_date,
		t.sales_value,
		t.local_sales_value,
		t.local_item_cost,
		t.blanket_number,
		t.bsa,
		t.local_pc,
		t.pc_pct,
		t.ppg_sales_class,
		t.order_type,
		t.comments,
		t.update_date,
		case when  REPLACE(t.customer_code, 'CN', '') = '207196' or  REPLACE(t.customer_code, 'CN', '') = '183492' 
		     then  REPLACE(bodyshop1.vendor_code, 'CN', '') 
		          else REPLACE(bodyshop2.vendor_code, 'CN', '') end as vendor_code,
		REPLACE(t.customer_code, 'CN', '') as customer_code_new
		FROM temp_002  t
		left join temp_bodyshop1 as bodyshop1
		on t.ship_to_code_2 = bodyshop1.ship_to_code_2
		left join temp_bodyshop2 as bodyshop2
		on t.warehouse_code_2 = bodyshop2.warehouse_code_2
		
		-- left join fine_dw.dw_bodyshop_distributor_all bodyshop1 -- 比亚迪
		-- on upper(bodyshop1.ship_to_code) = upper(t.ship_to_code)
		-- and bodyshop1.ship_to_code is not null
		-- and  REPLACE(bodyshop1.customer_code, 'CN', '')=REPLACE(t.customer_code, 'CN', '')
		-- left join fine_dw.dw_bodyshop_distributor_all bodyshop2 -- 其他
		-- on UPPER(bodyshop2.warehouse_code) = UPPER(t.warehouse_code)
		-- and bodyshop2.warehouse_code is not null

),


aa as ( -- 获取vendor信息
select * from temp_004 
UNION ALL
SELECT
		null,
		order_no,
		null,
		'link',
		null,
		null,
		null,
		null,
		null,
		CONCAT(sales_month,'01') as sales_date,
		null,
		null,
		'link',
		null,
		null,
		null,
		null,
		ship_to_code,
		customer_code,
		customer_name,
		null,
		item_code,
		null,
		null,
		null,
		null,
		sales_qty,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		warehouse_code,
		null,
		null,
		sales_value,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		'STANDARD ORDER - SH',
		null,
		null,
			vendor_code,
		customer_code
	FROM temp_003
	WHERE 1=1

)


-- 获取渠道 dw_ship_to_list
-- 关联字段  upper(customer_code_new) ,upper(t.ship_to_code),STR_TO_DATE(t.sales_date, '%Y%m%d'),STR_TO_DATE(t.sales_date, '%Y%m%d')
--   dw_ship_to_list 
-- upper(ship.customer_code) ,upper(ship.ship_to_code),STR_TO_DATE(ship.starting_date, '%Y%m%d'),STR_TO_DATE(ship.ending_date, '%Y%m%d') 


,temp_dw_ship_to_list as (
select  ship.*
       ,STR_TO_DATE(ship.starting_date, '%Y%m%d') as starting_date_2
       ,STR_TO_DATE(ship.ending_date, '%Y%m%d') as ending_date_2
       ,upper(ship.customer_code) as customer_code_2
	   ,ship.customer_name as customer_name_2
       ,upper(ship.ship_to_code) as ship_to_code_2
 from fine_dw.dw_ship_to_list  ship
)
, b as (
		select
		case when (customer_code_new = '210481' or customer_code_new = '212488') then 'MM' else channel end as channel,
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



-- 获取  channel1，district1
-- dw_customer_master_list   
-- STR_TO_DATE(master1.starting_date, '%Y%m%d')   STR_TO_DATE(master1.ending_date, '%Y%m%d')
-- upper(master1.customer_code)
,bb as
(
		SELECT
		case when t.channel is null then upper(master1.channel) else upper(t.channel) end as channel1,
		case when master1.channel in ('MM','MSO') then master2.district
		        when master1.channel not in ('MM','MSO') and t.district is null then master1.district else t.district end as district1,
		master1.proj_name,
		master1.proj_name_en,
		master1.is_flag,
		case when customer_code_2 is not null then customer_code_2 else master1.u_customer_code end as u_customer_code,
		case when customer_name_2 is not null then customer_name_2 else master1.u_customer_name end as u_customer_name,
		t.*

		from b t

		-- 第2步：剩余的channel、district为空的
		-- 匹配ods.ods_customer_master_list_df.customer_code=ods.ods_transaction_detail_report_df.customer_code
		-- 获得channel、district、proj_name、proj_name_en、is_flag
		left join temp_dw_customer_master_list master1
        on upper(customer_code_new) = upper(master1.customer_code)
		-- and STR_TO_DATE(master1.starting_date, '%Y%m%d') <= STR_TO_DATE(t.sales_date, '%Y%m%d')
		-- and STR_TO_DATE(master1.ending_date, '%Y%m%d') >= STR_TO_DATE(t.sales_date, '%Y%m%d')
        and SUBSTR(t.sales_date,1,6) = master1.sales_month
		left join temp_dw_customer_master_list master2 -- mm\mso的根据vendor_code匹配district
		on vendor_code = master2.customer_code
		-- and STR_TO_DATE(master2.starting_date, '%Y%m%d') <= STR_TO_DATE(t.sales_date, '%Y%m%d')
		-- and STR_TO_DATE(master2.ending_date, '%Y%m%d') >= STR_TO_DATE(t.sales_date, '%Y%m%d')
        and SUBSTR(t.sales_date,1,6) = master2.sales_month

),


temp_brand  as (
	select distinct
	item_code,
	category,
	category_brand,
	category_product_type
	from fine_dw.dw_item_brand
	where channel is null

),

temp_channel as (
select 		 'MM' as channel_2
union all 
select 		 'MSO' as channel_2
union all 
select 		 'SCHOOL' as channel_2
union all 
select 		 'DISTRIBUTOR' as channel_2
)




, bbb as (
		select
		channel1 as channel,
		case when t.ship_to_code='333878' OR t.ship_to_code='523444' then  "大众仓"  else t.district1 end as district,
		t.proj_name,
		t.proj_name_en,
		case when isflag.is_flag = '是' then isflag.is_flag else  t.is_flag end as is_flag,
		brand.category  as category,
		brand.category_brand  as category_brand,
		brand.category_product_type  as category_product_type,
		t.vendor_code as vendor_code,
		m.u_customer_name as vendor_name,
		-- 原表数据
		t.invoice_no,
		t.order_no,
		t.purchase_order,
		t.customer_po,
		t.delivery_number,
		t.batch_source,
		t.invoice_type,
		t.complete,
		t.status_code,
		DATE_FORMAT(STR_TO_DATE(t.sales_date, '%Y%m%d'),'%Y%m%d') as sales_date,
		DATE_FORMAT(STR_TO_DATE(t.sales_date, '%Y%m%d'),'%Y%m')  as sales_month,
		t.due_date,
		t.terms,
		t.salesrep,
		t.invoice_currency_code,
		t.gl_class,
		t.ship_to_customer_code,
		t.ship_to_customer_name,
		t.ship_to_code,
		t.u_customer_code as customer_code,
		t.u_customer_name as customer_name,
		t.line_no,
		t.item_code,
		t.aero_platform,
		t.brand_name,
		t.customer_item_description,
		t.invoice_line_description,
		t.sales_qty,
		t.shipped_qty_kg,
	  -- case when t.sales_volume is null then uom_ltr*sales_qty else t.sales_volume end as sales_volume,
	
	  case when t.sales_volume is null then COALESCE(uom_ltr,1) * credit_qty 
	       when t.sales_volume = 0    then  COALESCE(uom_ltr,1) * sales_qty
	       else t.sales_volume  end as  sales_volume,
	  -- 标志字段 uom_ltr 是否为null (1是null  0 不是null )
	  case when t.sales_volume is null and  uom_ltr  is null  then 1
	       when t.sales_volume = 0    and  uom_ltr  is null  then 1 
	       else  0  end as  uom_ltr_flag,
		t.credit_qty,
		t.unit_price,
		t.extended_amount,
		t.tax_amount,
		t.uom,
		t.waybill,
		t.invoice_remark,
		t.local_currency_code,
		t.warehouse_code,
		t.sales_date as orderder,
		t.actual_ship_date,
		t.sales_value,
		t.local_sales_value,
		t.local_item_cost,
		t.blanket_number,
		t.bsa,
		t.local_pc,
		t.pc_pct,
		t.ppg_sales_class,
		t.order_type,
		t.comments,
		'fine_ods.ods_transaction_detail_report_df' as data_resource,
		
		NOW()  as etl_time
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
	--		(
	--			select distinct
	--			item_code,
	--			category,
	--			category_brand,
	--			category_product_type
	--			from fine_dw.dw_item_brand
	--			where channel is null
	--			) brand
	 
		on upper(brand.item_code) = upper(t.item_code)


		-- -- 第10步：when sales_volume isnull, 与dw_item_cost_detail匹配item_code使用最新update_date获得uom_ltr*qty值作为该字段, when UOM_Ltr isnull取quantity值作为该字段
		left join ( select item_code,uom_ltr,update_date from 
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
		-- and (upper(t.customer_po) <> UPPER('Shortage') or customer_po is null)
		-- 第8步：customer code not in  (195726,203684)--备货单位
		-- and t.u_customer_code not in  ('195726','203684')
		and   (t.u_customer_code <>'195726' and t.u_customer_code <> '203684' )
     -- and t.item_code = 'P565-808/5K-C3'

),

temp_b3  as (
select DISTINCT UPPER(category_brand) as category_brand ,report_brand_name,report_brand_group FROM fine_dw.dw_ireport_sub_brand where category_brand is not null
),
temp_b1  as (
select DISTINCT UPPER(item_code) as item_code ,report_brand_name,report_brand_group FROM fine_dw.dw_ireport_sub_brand where item_code is not null
),
temp_b2  as (
select DISTINCT UPPER(category) as category ,report_brand_name,report_brand_group FROM fine_dw.dw_ireport_sub_brand where category is not null
),
bbbb as(
select
bbb.*,
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
),

temp_dw_customer_filter as (

SELECT distinct customer_code from fine_dw.dw_customer_filter
)


SELECT
		t.channel,
		t.district,
		t.proj_name,
		t.proj_name_en,
		t.is_flag,
		t.category,
		t.category_brand,
		t.category_product_type,
		case when ship.customer_code_2 is not null then t.vendor_code else m.u_customer_code end as vendor_code,
		case when ship.customer_name_2 is not null then t.vendor_name else m.u_customer_name end as vendor_name,
		t.invoice_no,
		t.order_no,
		t.purchase_order,
		t.customer_po,
		t.delivery_number,
		t.batch_source,
		t.invoice_type,
		t.complete,
		t.status_code,
		t.sales_date,
		t.sales_month,
		t.due_date,
		t.terms,
		t.salesrep,
		t.invoice_currency_code,
		t.gl_class,
		t.ship_to_customer_code,
		t.ship_to_customer_name,
		t.ship_to_code,
		t.customer_code,
		t.customer_name,
		t.line_no,
		t.item_code,
		t.aero_platform,
		t.brand_name,
		t.customer_item_description,
		t.invoice_line_description,
		t.sales_qty,
		t.shipped_qty_kg,
		t.sales_volume,
		t.credit_qty,
		t.unit_price,
		t.extended_amount,
		t.tax_amount,
		t.uom,
		t.waybill,
		t.invoice_remark,
		t.local_currency_code,
		t.warehouse_code,
		t.orderder,
		t.actual_ship_date,
		t.sales_value,
		t.local_sales_value,
		t.local_item_cost,
		t.blanket_number,
		t.bsa,
		t.local_pc,
		t.pc_pct,
		t.ppg_sales_class,
		t.order_type,
		t.comments,
		t.data_resource,
		t.report_brand_name,
		t.report_brand_group,
		t.sales_quarter,
		t.business_type,
		t.sales_year,
		NOW()  as etl_time,
		t.uom_ltr_flag

FROM bbbb t
		LEFT JOIN  temp_dw_ship_to_list ship
		on  t.vendor_code = ship.customer_code_2
		and t.ship_to_code=  ship.ship_to_code_2
		and ship.starting_date_2 <= STR_TO_DATE(t.sales_date, '%Y%m%d')
		and ship.ending_date_2 >= STR_TO_DATE(t.sales_date, '%Y%m%d')

		LEFT JOIN temp_dw_customer_master_list m
		on m.customer_code = upper(t.vendor_code)
        and m.sales_month = SUBSTR(t.sales_date,1,6)
 -- 20240925 新增      
   LEFT JOIN temp_dw_customer_filter 
   on t.customer_code = temp_dw_customer_filter.customer_code
   WHERE temp_dw_customer_filter.customer_code is null 