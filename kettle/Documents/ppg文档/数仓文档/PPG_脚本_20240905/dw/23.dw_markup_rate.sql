/*
目标表：fine_dw.dw_markup_rate
来源表：fine_dw.dw_cb_detail
        fine_dw.dw_item_mapping_cb


*/


-- TRUNCATE TABLE fine_dw.dw_markup_rate;
-- INSERT INTO fine_dw.dw_markup_rate(
-- 	vendor_code,
-- 	-- vendor_name,
-- 	category,
-- 	category_brand,
-- 	category_product_type,
-- 	proj_name,
-- 	proj_name_en,
-- 	markup_rate_year,
-- 	distributor_price,
-- 	sales_qty,
-- 	sales_value,
-- 	d_sales_value,
-- 	bs_price,
-- 	bs_sales_value,
-- 	data_resource,
-- 	etl_time
-- 
-- )
	-- 加价率1（绝对值）=结算价格/经销商价格 -- 使用上年加价率

	SELECT 
	vendor_code,
	category,
	category_brand,
	category_product_type,
	proj_name,
	proj_name_en,
	SUBSTR(sales_month,1,4) as markup_rate_year,
	distributor_price,
	sum(sales_qty) as sales_qty,
	sum(sales_value) as sales_value,
	sum(distributor_price*sales_qty) as d_sales_value, -- 经销商结算价=distributor_price*sales_qty
	cb1.bs_price,
	sum(cb1.bs_price*sales_qty) as  bs_sales_value,
	'fine_dw.dw_cb_detail/fine_dw.dw_item_mapping_cb' as data_resource,
	now() as etl_time 

	FROM fine_dw.dw_cb_detail t
	-- 加价率1（绝对值）=结算价格/经销商价格
	-- 公式：sales_value/(d_price*sales_qty)
	LEFT JOIN (
				SELECT DISTINCT item_code,max(bs_price) as bs_price,bs_starting_date,bs_ending_date FROM fine_dw.dw_item_mapping_cb GROUP BY item_code,bs_starting_date,bs_ending_date
				)cb1
        ON cb1.item_code = t.item_code
			AND DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(t.sales_month,'01'), '%Y%m%d'), INTERVAL 0 month), '%Y%m') >= DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(cb1.bs_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') 
			AND (
				DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(t.sales_month,'01'), '%Y%m%d'), INTERVAL 0 month), '%Y%m')  <=  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(cb1.bs_ending_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') 
				OR cb1.bs_ending_date IS NULL
			)
	WHERE business_type = '回购'
	GROUP BY
		vendor_code,
		category,
		category_brand,
		category_product_type,
		proj_name,
		proj_name_en,
		SUBSTR(sales_month,1,4),
		distributor_price,
		cb1.bs_price

