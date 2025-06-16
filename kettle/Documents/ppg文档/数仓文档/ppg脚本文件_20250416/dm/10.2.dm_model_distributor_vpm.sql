-- DELETE from fine_dm.dm_model_distributor_vpm where order_no >= 191 and report_date = '20240101';
-- INSERT INTO fine_dm.dm_model_distributor_vpm
-- (
-- 	order_type,
-- 	channel,
-- 	customer_name,
-- 	customer_code,
-- 	item_code,
-- 	item_code_ppg,
-- 	category,
-- 	category_brand,
-- 	category_product_type,
-- 	sales_value,
-- 	sales_month,
-- 	report_year,
-- 	order_no,
-- 	sec_1,
-- 	sec_2,
-- 	sec_3,
-- 	report_date,
-- 	etl_time
-- )
with temp_dw_order_report as (
select * 
	 from fine_dw.dw_order_report orderno
	 where orderno.order_report = 'model_vpm'
				 and  orderno.report_year = SUBSTRING(${mysql_yesterday_l_month},1,4)
				 and  orderno.order_no >= 191
 ),
 temp2 as (
 SELECT *,
    case when sec_2 like '%SALES' then 'SALES VPM'
		 When sec_2 like '%PC' then 'PC VPM' end as sec_33 from fine_dm.dm_model_distributor_vpm b
 WHERE report_year = SUBSTRING(${mysql_yesterday_l_month},1,4)
--  and order_no >=9 and order_no <= 15
 and sec_3 like '%VPM'
 and order_no < 191
 )
--  SELECT
--  sum(sales_value),order_no,channel
--  from(
SELECT
b.order_type
,b.channel
,b.customer_name
,b.customer_code
,b.item_code
,b.item_code_ppg
,b.category
,b.category_brand
,b.category_product_type
,sum(b.sales_value) as sales_value
,SUBSTRING(b.sales_month,1,4) as sales_month
,b.report_year
,a.order_no
,a.sec_1
,a.sec_2
,a.sec_3
,b.report_date
,SYSDATE() as etl_time
from temp_dw_order_report a
left join temp2 b
on a.sec_1 = b.sec_1
and a.sec_33 = b.sec_33
where 1=1
GROUP BY
b.order_type
,b.channel
,b.customer_name
,b.customer_code
,b.item_code
,b.item_code_ppg
,b.category
,b.category_brand
,b.category_product_type
,SUBSTRING(b.sales_month,1,4)
,b.report_year
,a.order_no
,a.sec_1
,a.sec_2
,a.sec_3
,b.report_date
-- ) sss
-- group by channel,order_no
-- order by channel,order_no