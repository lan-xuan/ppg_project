
/*
目标表：fine_dw.dw_stock_replenish_deduction
来源表：

fine_dw.dw_price_increase
 fine_dw.dw_item_mapping_distributor
 fine_dw.dw_item_mapping_cb
fine_dw.dw_item_cost_detail

更新方式：增量更新 year

参数：${mysql_yesterday_d_year}
*/

with t as(
SELECT
	t.sales_month,
	t.proj_name,
	t.proj_name_en,
	t.channel,
	t.item_code,
	t.item_code_ppg,
	t.category,
	t.category_brand,
	t.category_product_type,
-- 	t.report_brand_group,
	sum(t.d_sales) as sales_value,
	sum(t.d_volume) as sales_volume,
	sum(t.d_qty) as sales_qty,
	sum(t.d_pc) as pc
	FROM fine_dw.dw_price_increase t
		WHERE t.channel in ('MM','MSO')
	 		-- and substr(sales_month,1,4)  = '2023'
and substr(sales_month,1,4)  = ${mysql_yesterday_d_year}

	GROUP BY
	t.sales_month,
	t.proj_name,
	t.proj_name_en,
	t.channel,
	t.item_code,
	t.item_code_ppg,
	t.category,
	t.category_brand,
	t.category_product_type


)   -- SELECT * from t ; -- 30343
SELECT 
	t.sales_month,
	t.proj_name,
	t.proj_name_en,
	t.channel,
	t.item_code,
	t.item_code_ppg,
	t.category,
	t.category_brand,
	t.category_product_type,
	t.sales_qty,
	-1*pc as pc, -- pc=-(invoice sales value-svc*invoice qty)注意这里pc*-1作为结果，所有数值相关注意*-1作为结果pc,
	sales_value, -- 第3步：unit_price*invoice_qty
-- 	 第5步：when transaction volume isnull, 与dw_item_cost_detail匹配获得uom_ltr*qty值作为该字段, when UOM_Ltr isnull取quantity值作为该字段
	sales_volume,
	cb2.service_fee,
	cb3.service_rate,
	cb4.rebate_rate,
	cb5.commision_fee_rate,
	cb6.reward_rate,
	cb1.bs_price, -- 根据dw_item_mapping_cb.item_code、proj_name匹配所属时间区间的对应门店价格
	d.distributor_price as service_price, --  dw_item_mapping_distributor.item_code匹配获得经销商价格
	'dw_transaction_detail_report' as data_resource, 
	SYSDATE() as etl_time
	FROM  t

	-- 第2步：
	-- svc匹配规则根据item_code按顺序匹配表dw_svc_sh、dw_svc_sj、dw_svc_bn、dw_svc_fb、dw_svc_js按照日期区间获得svc
	-- pc=(invoice sales value-svc*invoice qty)；如果还匹配不到，到当月的sales&pc report中根据item计算svc：svc=(sales_value-pc)/primary uom qty
	-- dw_svc_detail为逻辑结果表可以直接用

		
-- 	第3步：unit_price*invoice_qty dw_item_mapping_distributor.item_code匹配获得经销商价格

		LEFT JOIN 
		(
			SELECT DISTINCT item_code,max(distributor_price) as distributor_price,starting_date,ending_date FROM fine_dw.dw_item_mapping_distributor group by item_code,starting_date,ending_date
		)  d
		ON t.item_code_ppg = d.item_code
		AND DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(t.sales_month,'01'), '%Y%m%d'), INTERVAL 1 month), '%Y%m') >= DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(d.starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') 
			AND (
				DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(t.sales_month,'01'), '%Y%m%d'), INTERVAL 1 month), '%Y%m')  <=  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(d.ending_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') 
				OR d.ending_date IS NULL
			)

    -- 根据dw_item_mapping_cb.item_code、proj_name匹配所属时间区间的对应 门店价格
        LEFT JOIN (
				SELECT DISTINCT item_code,max(bs_price) as bs_price,bs_starting_date,bs_ending_date FROM fine_dw.dw_item_mapping_cb GROUP BY item_code,bs_starting_date,bs_ending_date
				)cb1
        ON cb1.item_code = t.item_code
			AND DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(t.sales_month,'01'), '%Y%m%d'), INTERVAL 0 month), '%Y%m') >= DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(cb1.bs_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') 
			AND (
				DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(t.sales_month,'01'), '%Y%m%d'), INTERVAL 0 month), '%Y%m')  <=  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(cb1.bs_ending_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') 
				OR cb1.bs_ending_date IS NULL
			)

    -- 根据dw_item_mapping_cb.item_code、proj_name匹配所属时间区间的对应 服务费
        LEFT JOIN (
				SELECT DISTINCT item_code,max(service_fee) as service_fee,service_fee_starting_date,service_fee_ending_date FROM fine_dw.dw_item_mapping_cb GROUP BY item_code,service_fee_starting_date,service_fee_ending_date
				)cb2
        ON cb2.item_code = t.item_code
			AND DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(t.sales_month,'01'), '%Y%m%d'), INTERVAL 0 month), '%Y%m') >= DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(cb2.service_fee_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') 
			AND (
				DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(t.sales_month,'01'), '%Y%m%d'), INTERVAL 0 month), '%Y%m')  <=  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(cb2.service_fee_ending_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') 
				OR cb2.service_fee_ending_date IS NULL
			)
    -- 根据dw_item_mapping_cb.item_code、proj_name匹配所属时间区间的对应 总服务商服务费率
        LEFT JOIN (
				SELECT DISTINCT item_code,max(service_rate) as service_rate,service_rate_starting_date,service_rate_ending_date FROM fine_dw.dw_item_mapping_cb GROUP BY item_code,service_rate_starting_date,service_rate_ending_date
				)cb3
        ON cb3.item_code = t.item_code
		AND DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(t.sales_month,'01'), '%Y%m%d'), INTERVAL 0 month), '%Y%m') >= DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(cb3.service_rate_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') 
			AND (
				DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(t.sales_month,'01'), '%Y%m%d'), INTERVAL 0 month), '%Y%m')  <=  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(cb3.service_rate_ending_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') 
				OR cb3.service_rate_ending_date IS NULL
			)		
    -- 根据dw_item_mapping_cb.item_code、proj_name匹配所属时间区间的对应 主机厂/集团返利率
        LEFT JOIN (
				SELECT DISTINCT item_code,max(rebate_rate) as rebate_rate,rebate_rate_starting_date,rebate_rate_ending_date FROM fine_dw.dw_item_mapping_cb GROUP BY item_code,rebate_rate_starting_date,rebate_rate_ending_date
				)cb4
        ON cb4.item_code = t.item_code
		AND DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(t.sales_month,'01'), '%Y%m%d'), INTERVAL 0 month), '%Y%m') >= DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(cb4.rebate_rate_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') 
			AND (
				DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(t.sales_month,'01'), '%Y%m%d'), INTERVAL 0 month), '%Y%m')  <=  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(cb4.rebate_rate_ending_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') 
				OR cb4.rebate_rate_ending_date IS NULL
			)	
    -- 根据dw_item_mapping_cb.item_code、proj_name匹配所属时间区间的对应 代理商服务费率 -- commision_fee_rate
        LEFT JOIN (
				SELECT DISTINCT item_code,max(commision_fee_rate) as commision_fee_rate,commision_fee_starting_date,commision_fee_ending_date FROM fine_dw.dw_item_mapping_cb GROUP BY item_code,commision_fee_starting_date,commision_fee_ending_date
				)cb5
        ON cb5.item_code = t.item_code
		AND DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(t.sales_month,'01'), '%Y%m%d'), INTERVAL 0 month), '%Y%m') >= DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(cb5.commision_fee_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') 
			AND (
				DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(t.sales_month,'01'), '%Y%m%d'), INTERVAL 0 month), '%Y%m')  <=  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(cb5.commision_fee_ending_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') 
				OR cb5.commision_fee_ending_date IS NULL
			)
    -- 根据dw_item_mapping_cb.item_code、proj_name匹配所属时间区间的对应 奖励金率
        LEFT JOIN(
				SELECT DISTINCT item_code,max(reward_rate) as reward_rate,reward_rate_starting_date,reward_rate_ending_date FROM fine_dw.dw_item_mapping_cb GROUP BY item_code,reward_rate_starting_date,reward_rate_ending_date
				) cb6 
        ON cb6.item_code = t.item_code
		AND DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(t.sales_month,'01'), '%Y%m%d'), INTERVAL 0 month), '%Y%m') >= DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(cb6.reward_rate_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') 
			AND (
				DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(t.sales_month,'01'), '%Y%m%d'), INTERVAL 0 month), '%Y%m')  <=  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(cb6.reward_rate_ending_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') 
				OR cb6.reward_rate_ending_date IS NULL
			)
	-- 第5步：when transaction volume isnull, 与dw_item_cost_detail匹配获得uom_ltr*qty值作为该字段, when UOM_Ltr isnull取quantity值作为该字段
        LEFT JOIN fine_dw.dw_item_cost_detail c
        ON c.item_code = t.item_code
				and sales_month = update_date
				-- 缺少日期限制