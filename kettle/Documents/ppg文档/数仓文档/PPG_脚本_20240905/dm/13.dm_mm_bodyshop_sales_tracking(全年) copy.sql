SELECT 
sid
,sec_1
,sales_year
,'FY' as sales_month
,'FY' as sales_month2
,sum(salse_value) as salse_value
,mm_shop_code
,mm_proj_name
,mm_ship_to_code
,mso_shop_code
,mso_proj_name
,mso_ship_to_code
,won_sales_month
,mm_sales_id
,mso_sales_id
,wc_sales_id
,body_shop_name
,province
,city
,detail
,coating_supplier
,wb_sb
,ppg_brand
,distributor_name
,expected_value
,salse_name
,sales_manager
,bia_sf
,con_start_time
,con_over_time
,con_cycle
,con_sales_volume
,win_reason
,ebit
,createTime
,data_resource
,NOW() as etl_time,
STR_TO_DATE(CONCAT(sales_year ,'0101') , '%Y%m%d') as report_date
FROM `dm_bodyshop_sales_tracking`
WHERE sales_year = ${mysql_yesterday_d_year} -- 时间参数
and sales_month != 'FY'
GROUP BY 
sid
,sec_1
,sales_year
,mm_shop_code
,mm_proj_name
,mm_ship_to_code
,mso_shop_code
,mso_proj_name
,mso_ship_to_code
,won_sales_month
,mm_sales_id
,mso_sales_id
,wc_sales_id
,body_shop_name
,province
,city
,detail
,coating_supplier
,wb_sb
,ppg_brand
,distributor_name
,expected_value
,salse_name
,sales_manager
,bia_sf
,con_start_time
,con_over_time
,con_cycle
,con_sales_volume
,win_reason
,ebit
,createTime
,data_resource
