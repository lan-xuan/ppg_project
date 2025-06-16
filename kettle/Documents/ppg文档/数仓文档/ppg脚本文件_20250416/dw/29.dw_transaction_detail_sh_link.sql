
/*
目标表： fine_dw.dw_transaction_detail_sh_link
来源表：

ods_transaction_detail_sh

更新方式: 增量更新 month
参数 ： ${mysql_yesterday_d_month}



*/

SELECT
	customer_code, -- 6875
	customer_name,
	vendor_code,
	vendor_name,
	warehouse_code,
	order_no,
	t.item_code,
	ship_to_code,
	sales_month,
	sales_qty,
	sales_qty*mainpart_price as sales_value,
    'fine_ods.ods_transaction_detail_sh' as data_resource, 
     SYSDATE() as etl_time,
	 create_date 
	
FROM
(
	SELECT -- DISTINCT mainaccount_remark
	customer_code,
	customer_name,
	REPLACE(vendor_code, 'CN', '') as vendor_code,
	vendor_name,
	warehouse_code,
	order_no,
	item_code,
	ship_to_code,
	-- REPLACE(mainaccount_remark,'LYNK','') as sales_month,
  -- REGEXP_SUBSTR(mainaccount_remark, '[12][0-9]{3}(0[1-9]|1[0-2])') AS sales_month,
  DATE_FORMAT(mainaccountlast_date, '%Y%m')  AS sales_month,
	sales_qty,
	mainpart_price,
	create_date
	FROM fine_ods.ods_transaction_detail_sh
	WHERE customer_code in ('210481','212488')
  AND  DATE_FORMAT(mainaccountlast_date , '%Y%m')   =  ${mysql_yesterday_d_month}
-- AND  DATE_FORMAT(mainaccountlast_date , '%Y%m')  >= '202407'
 
) t
where sales_month is not  null