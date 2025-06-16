/*
目标表：fine_dw.dw_sales_pc_customer
来源表：

fine_dw.dw_ship_to_list
fine_dw.dw_customer_master_list
fine_dw.dw_item_brand
fine_dw.dw_customer_filter
fine_dw.dw_transaction_pc

ods_sales_pc_customer_df
ods_transaction_detail_report_df


更新方式：增量更新
参数 ：${mysql_yesterday_l_month}


*/

-- 85869 -- 85927 95600 98905
with  temp_dw_customer_master_list as 
(select * 
 from fine_dw.dw_customer_master_list
 where substr(sales_month,1,6)  = ${mysql_yesterday_l_month}
 -- where substr(sales_month,1,4)  = '2023'
 ),
 sales_pc_customer_df as (
SELECT

sub,
sub_sub,
cust_sub_segment,
gl_class,
customer_name,
REPLACE(t.customer_code, 'CN', '') customer_code,
ship_to_code,
item_code,
item_description,
item_sbu,
sales_weight,
-- sales_volume,
case when REPLACE(t.customer_code, 'CN', '') in ('210481','212488') and sales_value = 0 then 0 else sales_volume end as  sales_volume,
sales_value,
usd_sales_value,
case when REPLACE(t.customer_code, 'CN', '') in ('210481','212488') and sales_value = 0 then 0 else sales_qty end as  sales_qty,
primary_uom_code,
pc,
pc_pct,
asp,
sales_date,
planning_class,
aero_platform,
brand_name,
sub_brand_name,
product_type,
sub_product_type,
item_gl,
pmc_distributor,
inv_type,
ppg_voc,
ppg_sustainable_product,
ppg_green_product,
ppg_innovation_class,
ppg_sales_class,
ppg_apmf_category,
ind_tech,
ppg_product_usage


FROM fine_ods.ods_sales_pc_customer_df t
where 1=1
 and SUBSTR(sales_date,1,6) = ${mysql_yesterday_l_month} 
-- and SUBSTR(sales_date,1,4) = '2023' -- 限制年份



)
, b as (
		select 
		channel,
		district,
		REPLACE(ship.customer_code, 'CN', '') as customer_code_new,
		ship.customer_name as customer_name_new,
		t.*

		from  sales_pc_customer_df t
		-- 第1步：
		-- 匹配ods.ods_transaction_detail_report_df.customer_code=ods_ship_to_list_df.customer_code
		-- and ods.ods_transaction_detail_report_df.ship_to_code=ods_ship_to_list_df.ship_to_code
		-- 获得channel、district其中proj_name、proj_name_en、is_flag字段为空（ship_to只包含distributor和school的信息）
		left join  fine_dw.dw_ship_to_list ship
		on t.customer_code = ship.customer_code 
		and t.ship_to_code=ship.ship_to_code
		and STR_TO_DATE(ship.starting_date, '%Y%m%d') <= STR_TO_DATE(t.sales_date, '%Y%m%d')
		and STR_TO_DATE(ship.ending_date, '%Y%m%d') >= STR_TO_DATE(t.sales_date, '%Y%m%d')
		WHERE 1=1 
		and case when t.customer_code in('210481','212488') and pc <>0 then 1=1
		else  sales_value <> 0 and sales_qty <> 0  end
)


,bb as
(
		SELECT
		case when t.channel is null then master1.channel else t.channel end as channel1,
		case when t.channel is null then master1.district else t.district end as district1,
		master1.proj_name,
		master1.proj_name_en,
		master1.is_flag,
		case when customer_code_new is not null then customer_code_new else master1.u_customer_code end as u_customer_code,
		case when customer_name_new is not null then customer_name_new else master1.u_customer_name end as u_customer_name,
		t.*

		from b t

		-- 第2步：剩余的channel、district为空的
		-- 匹配ods.ods_customer_master_list_df.customer_code=ods.ods_transaction_detail_report_df.customer_code
		-- 获得channel、district、proj_name、proj_name_en、is_flag
		left join temp_dw_customer_master_list master1
		on t.customer_code = master1.customer_code 
		and SUBSTR(t.sales_date,1,6) = master1.sales_month 
		-- and STR_TO_DATE(master1.starting_date, '%Y%m%d') <= STR_TO_DATE(t.sales_date, '%Y%m%d')
		-- and STR_TO_DATE(master1.ending_date, '%Y%m%d') >= STR_TO_DATE(t.sales_date, '%Y%m%d')
		where 1=1

),

temp_001 as (

select distinct item_code from (
			select distinct  item_code from  fine_ods.ods_sales_pc_customer_df t where UPPER(item_code) NOT LIKE '.%'  and item_code  <> ''
			union all 
			select distinct item_code from  fine_ods.ods_sales_pc_customer_df t where UPPER(item_code) = '.Price Adj' 
			union all 
			select distinct item_code from  fine_ods.ods_sales_pc_customer_df t where UPPER(item_code) = '.LINQ IFLOW SOFTWARE FEE' 
			) a

),
temp_002 as (

SELECT distinct customer_code from fine_dw.dw_customer_filter
)

, bbb as (
		select 
		t.sub,
		t.sub_sub,
		t.cust_sub_segment,
		t.gl_class,
		t.u_customer_name as customer_name,
		t.u_customer_code as customer_code,
		t.ship_to_code,
		t.item_code,
		t.item_description,
		t.item_sbu,
		t.sales_weight,
		t.sales_volume,
		t.sales_value,
		t.usd_sales_value,
		t.sales_qty,
		t.primary_uom_code,
		t.pc,
		t.pc_pct,
		t.asp,
		DATE_FORMAT(STR_TO_DATE(t.sales_date, '%Y%m%d'),'%Y%m%d') as sales_date,
		DATE_FORMAT(STR_TO_DATE(t.sales_date, '%Y%m%d'),'%Y%m')  as sales_month,
		t.planning_class,
		t.aero_platform,
		t.brand_name,
		t.sub_brand_name,
		t.product_type,
		t.sub_product_type,
		t.item_gl,
		t.pmc_distributor,
		t.inv_type,
		t.ppg_voc,
		t.ppg_sustainable_product,
		t.ppg_green_product,
		t.ppg_innovation_class,
		t.ppg_sales_class,
		t.ppg_apmf_category,
		t.ind_tech,
		t.ppg_product_usage,
		brand1.category as category,
		brand1.category_brand as category_brand,
		brand1.category_product_type as category_product_type,
		channel1 as channel,
		case when t.ship_to_code='333878' then  "大众仓"  else t.district1 end as district,
		t.proj_name,
		t.proj_name_en,
		t.is_flag,
		'ods_sales_pc_customer_df' table_name
		from bb t
		-- 第3.1步：根据ods_ireport_sub_brand_df.item_code匹配获得report_brand_group、report_brand_name
		-- channel是MM/MSO的
		-- ods_transaction_detail_report_df.item_no = ods.ods_item_maping_cb_df.item_no
		-- 获得新列名3个category、category_brand、category_product_type
		-- if category_brand isnull，去distributor mapping表匹配获得产品信息
		-- channel是D/School的
		-- ods_transaction_detail_report_df.item_no = ods.ods_item_maping_distributor_df.item_no
		-- 获得新列名3个category、category_brand、category_product_type


		left join 
			(
				select distinct  
				item_code,
				category,
				category_brand,
				category_product_type
				from fine_dw.dw_item_brand
				) brand1
		on brand1.item_code = t.item_code


   inner join   temp_001
   on t.item_code = temp_001.item_code
   left  join  temp_002 
   on t.customer_code = temp_002.customer_code
   
WHERE 1=1
		-- 第5步：gl_class not in ('2741 - REF, AUTOCOLOUR ASIA','2999 - REF, ASIA/PACIFIC, CHINA, IRD')
		-- and t.gl_class not in ('2741 - REF', 'AUTOCOLOUR ASIA','2999 - REF', 'ASIA/PACIFIC', 'CHINA, IRD')
		and upper(gl_class) not in ('2741 - REF, AUTOCOLOUR ASIA','2999 - REF, ASIA/PACIFIC, CHINA, IRD')
		
		and (UPPER(CONCAT(channel1,brand_name)) <> upper('DISTRIBUTORCentral Supply') or brand_name is null)
		and channel1 in ('MM','MSO','SCHOOL','DISTRIBUTOR')
		-- 第6步：剔除item_code like '.%'   and item_code <>'' 但是保留item_code  = '.Price Adj'
		
		-- and t.item_code in
		-- (
		-- 	select distinct  item_code from  fine_ods.ods_sales_pc_customer_df t where UPPER(item_code) NOT LIKE '.%'  and item_code  <> ''
		--	union all 
		--	select distinct item_code from  fine_ods.ods_sales_pc_customer_df t where UPPER(item_code) = '.Price Adj' 
		-- )


		-- 第7步：customer_code not in ods_customer_filter_df.customer_code
		-- and t.u_customer_code not in  (SELECT distinct customer_code from fine_dw.dw_customer_filter)
		and temp_002.customer_code is null 

)


				SELECT bbb.*,		'ods_sales_pc_customer_df' as data_resource, SYSDATE() as etl_time FROM bbb
				union ALL 
				SELECT
						null,
						null,
						null,
						null,
						customer_name,
						customer_code,
						ship_to_code,
						item_code,
						null,
						null,
						null,
						sales_volume,
						sales_value,
						null,
						sales_qty,
						null,
						pc,
						null,
						null,
						sales_date,
						sales_month,
						null,
						null,
						brand_name,
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
						null,
						null,
						null,
						null,
						category,
						category_brand,
						category_product_type,
						channel,
						district,
						proj_name,
						proj_name_en,
						is_flag,
						table_name,
						'dw_transaction_pc' as data_resource, 
						SYSDATE() as etl_time
						
					FROM fine_dw.dw_transaction_pc t
-- 					where customer_code  = '166493' 
 	 	where  SUBSTRING(sales_month,1,6) = ${mysql_yesterday_l_month}
--   	where  SUBSTRING(sales_month,1,4) = '2023'
-- 					and item_code = 'P850-1401/1L-C3'
				
				

