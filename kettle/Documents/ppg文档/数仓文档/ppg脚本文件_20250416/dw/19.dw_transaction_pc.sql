
/*
目标表：fine_dw.dw_transaction_pc
来源表：

fine_dw.dw_transaction_detail_report
fine_dw.dw_svc_detail

更新方式：全量更新

*/


/*

-- dw_transaction_pc>>>dw_sales_pc_customer>>>dw_price_increase>>>dm_price_increase_report
-- 数据了比较小，可以一次性跑完，之后跑dw_sales_pc_customer因为需要union dw_transaction_pc
TRUNCATE TABLE fine_dw.dw_transaction_pc;
INSERT INTO
    fine_dw.dw_transaction_pc (
customer_code,
ship_to_code,
item_code,
sales_volume,
sales_value,
sales_qty,
pc,
sales_date,
sales_month,
brand_name,
category,
category_brand,
category_product_type,
channel,
district,
proj_name,
proj_name_en,
is_flag,
table_name,
data_resource,
etl_time
    )
*/
with  bbb as (

		select 
			*
			from fine_dw.dw_transaction_detail_report t
		where 1=1 
and order_type = 'MM REPLENISH ORDER-SH' 
and (upper(brand_name) <> upper('Central Supply') or brand_name is null)
		-- where t.customer_code = '207196' -- 领克
-- 		and t.customer_code = '166493'
-- 		and sales_month in( '202401','202301')
-- 		and t.item_code = 'P850-1401/1L-C3'

)

	SELECT
	customer_name,
	customer_code,
	ship_to_code,
	t.item_code,
	sum(-1 * sales_volume) as sales_volume,
	sum(-1 * sales_value) as sales_value,
	sum(-1 * sales_qty) as sales_qty,
	sum((sales_value - svc * sales_qty ) * -1) as pc, -- pc=-(invoice sales value-svc*invoice qty)注意这里pc*-1作为结果，所有数值相关注意*-1作为结果
  t.sales_date,
	DATE_FORMAT(STR_TO_DATE(t.sales_date, '%Y%m%d'),'%Y%m')  as sales_month,
	brand_name,
	category,
	category_brand,
	category_product_type,
	channel,
	district,
	proj_name,
	proj_name_en,
	is_flag,
	'dw_transaction_detail_report' as table_name,
	'dw_transaction_detail_report' as data_resource, 
	SYSDATE() as etl_time

	FROM bbb t

	-- 第1步：
	-- 根据item_code按顺序匹配表dw_svc_detail按照日期区间获得svc，其中优先匹配table_name=sh；如果还匹配不到，到当月的sales&pc report中根据item计算svc：svc=(sales_value-pc)/sales_qty（此逻辑已经在dw_svc_detail表中处理，直接使用dw_svc_detail表）
	-- 第2步：
	-- pc=-(invoice sales value-svc*invoice qty)注意这里pc*-1作为结果，所有数值相关注意*-1作为结果
	LEFT JOIN fine_dw.dw_svc_detail s
	ON t.item_code = s.item_code
	and svc_month = sales_month
	-- 第0步：order_type like ‘%MM REPLENISH%’and brand <> 'Central Supply'
	WHERE 1=1
	GROUP BY 
		customer_name,
		customer_code,
		ship_to_code,
		t.item_code,
		t.sales_date,
		DATE_FORMAT(STR_TO_DATE(t.sales_date, '%Y%m%d'),'%Y%m'),
		brand_name,
		category,
		category_brand,
		category_product_type,
		channel,
		district,
		proj_name,
		proj_name_en,
		is_flag
-- limit 1
