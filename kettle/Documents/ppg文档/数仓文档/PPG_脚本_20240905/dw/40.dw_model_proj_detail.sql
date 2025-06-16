
/*
目标表：fine_dw.dw_model_proj_detail
来源表：fine_dw.dw_cb_detail/fine_dw.dw_transaction_detail_report/fine_dw.dw_price_increase/fine_dw.dw_markup_rate


*/

/*delete from  fine_dw.dw_model_proj_detail where sales_month = ${mysql_yesterday_l_month};

INSERT INTO fine_dw.dw_model_proj_detail(
vendor_code,
vendor_name,
sales_month,
mm_label_sales,
mm_label_stock,
mm_nonlabel_sales,
mm_nonlabel_stock,
mso_sales,
mso_stock,
分子1,
分子2,
target_sales,
makeup_rate)
*/

WITH temp_002 as (
select * from 
fine_dw.dw_cb_detail
where 1=1
and STR_TO_DATE(CONCAT(sales_month,'01'), '%Y%m%d') <= STR_TO_DATE(CONCAT(${mysql_yesterday_l_month},'01'), '%Y%m%d')
-- and customer_code = '36222'

),
temp_001 as (
select * from 
fine_dw.dw_transaction_detail_report
where 1=1
and channel = upper('Distributor')
and STR_TO_DATE(CONCAT(sales_month,'01'), '%Y%m%d') <= STR_TO_DATE(CONCAT(${mysql_yesterday_l_month},'01'), '%Y%m%d')
-- and sales_month = '202104'
-- and customer_code = '36222'

),temp_dw_ship_to_list as (
select  DISTINCT
       STR_TO_DATE(ship.starting_date, '%Y%m%d') as starting_date_2
       ,STR_TO_DATE(ship.ending_date, '%Y%m%d') as ending_date_2
       ,upper(ship.customer_code) as customer_code_2
	   ,ship.district
 from fine_dw.dw_ship_to_list  ship
 WHERE channel = 'DISTRIBUTOR'
)
,a as(
			SELECT 
		p.channel,
		p.customer_code as vendor_code,
		p.customer_name as vendor_name,
		sum(COALESCE(p.net_sales , 0)) as sales_value, 
		${mysql_yesterday_l_month} as sales_month
	FROM fine_dw.dw_price_increase p
	where 1=1
	and channel = 'DISTRIBUTOR'
	and STR_TO_DATE(CONCAT(sales_month,'01'), '%Y%m%d') <= STR_TO_DATE(CONCAT(${mysql_yesterday_l_month},'01'), '%Y%m%d')
	GROUP BY p.customer_code,p.customer_name,p.channel

	
	)
-- 剔除当年数据最近一条数据
	, makeup as(
select   vendor_code,
	       proj_name,
	       makeup_rate
from (
	select vendor_code,
	       proj_name,
	       makeup_rate,
	       sales_year,
	       ROW_NUMBER() OVER (PARTITION BY vendor_code,proj_name ORDER BY sales_year desc ) as  seq  
	from (
	SELECT 
		vendor_code,
		proj_name,
		markup_rate_year as sales_year,
		sum(COALESCE(sales_value,0))/ NULLIF(sum(COALESCE(d_sales_value,0)),0)  as makeup_rate
		FROM fine_dw.dw_markup_rate
		-- 加价率1（绝对值）=结算价格/经销商价格
		-- 公式：sales_value/(d_price*sales_qty)
		GROUP BY 
		vendor_code,
		proj_name,
		markup_rate_year
	-- 	having  sales_year <> '2024'  -- 剔除当年数据
		
     having   sales_year <>  SUBSTRING(${mysql_yesterday_l_month},1,4)
		) A
		   ) B
	where B.seq =1 	
	)
	, mm_label as (-- transaction表的CENTRAL SUPPLY
SELECT 
			sum(sales_value) as sales_value,
			sum(shipment_sales) as shipment_sales,
			vendor_code,
      		vendor_name,
			proj_name,
			'DISTRIBUTOR' channel,
			sales_month
FROM(
	SELECT 
			0 sales_value,
			0 shipment_sales,
			-- sum(COALESCE(sales_value,0)) shipment_sales,
			customer_code as vendor_code,
			customer_name as vendor_name,
			proj_name,
			channel,
			${mysql_yesterday_l_month}  sales_month
        FROM temp_001
        where 1=2
		and channel = upper('Distributor')
		and brand_name = 'CENTRAL SUPPLY' and order_type <> 'MM REPLENISH ORDER-SH'
        GROUP BY
			customer_code,
            customer_name,
			proj_name,
			channel
			-- sales_month
	UNION ALL
select 
	sum(COALESCE(sales_value , 0)) as sales_value,
	sum(COALESCE(sales_qty * distributor_price,0)) as shipment_sales,
	vendor_code,
	vendor_name,
	proj_name,
	channel,
	${mysql_yesterday_l_month} sales_month
	from temp_002 cb
		where sales_qty is not null
		and is_flag = '是'
		and channel = 'MM'
		and proj_name is not null
		and business_type = '回购'
		and vendor_code <> '191204'
		and vendor_code <> '195726'
		and vendor_code is not null
	GROUP BY 	
	vendor_code,
	vendor_name,
	proj_name,
	channel
	-- sales_month
)s
	GROUP BY 
			vendor_code,
            vendor_name,
			proj_name,
-- 			channel,
			sales_month
			
)   -- SELECT sum(sales_value),sum(shipment_sales) shipment_sales from  mm_label ;
, mm_nonlabel as(  -- 主机厂不带标签cb_detail	
	select 
	sum(COALESCE(sales_value , 0)) as sales_value,
	sum(COALESCE(sales_qty * distributor_price,0) ) as shipment_sales,
	vendor_code,
	vendor_name,
	proj_name,
	channel,
	${mysql_yesterday_l_month}  sales_month
	from temp_002 cb
		where sales_qty is not null
		and is_flag = '否'
		and channel = 'MM'
		and proj_name is not null
		and business_type = '回购'
		and vendor_code <> '191204'
		and vendor_code <> '195726'
		and vendor_code is not null
	GROUP BY 	
	vendor_code,
	vendor_name,
	proj_name,
	channel
) -- SELECT * from mm_nonlabel;
-- SELECT sum(sales_value),sum(shipment_sales) shipment_sales from  mm_nonlabel ;
, mso as (-- 集团cb_detail
	select 
	sum(COALESCE(sales_value , 0)) as sales_value,
	sum(COALESCE(sales_qty*distributor_price , 0)) as shipment_sales,
	vendor_code,
	vendor_name,
	proj_name,
	channel,
	${mysql_yesterday_l_month}  sales_month
	from temp_002 cb
	where sales_qty is not null
		and is_flag = '否'
		and channel = 'MSO'
		and proj_name is not null
		and business_type = '回购'
		and vendor_code <> '191204'
		and vendor_code <> '195726'
	GROUP BY 	
	vendor_code,
	vendor_name,
	proj_name,
	channel
)  -- SELECT * from mso;
,stock as (

	SELECT 
			0 stock_nonlabel,
			sum(COALESCE(sales_value,0)) stock_label,
			customer_code as vendor_code,
			customer_name as vendor_name,
			${mysql_yesterday_l_month}  sales_month
        FROM temp_001
        where channel = upper('Distributor')
-- 				and sales_month = ${mysql_yesterday_d_month}
		and brand_name = 'CENTRAL SUPPLY' and order_type <> 'MM REPLENISH ORDER-SH'
        GROUP BY
			customer_code,
      customer_name

) 
, customer_code_list as(

SELECT vendor_name,vendor_code,${mysql_yesterday_l_month} sales_month from a
UNION 
SELECT vendor_name,vendor_code,${mysql_yesterday_l_month} sales_month from mm_label
UNION 
SELECT vendor_name,vendor_code,${mysql_yesterday_l_month} sales_month from mm_nonlabel
UNION 
SELECT vendor_name,vendor_code,${mysql_yesterday_l_month} sales_month from mso

)
, customer_proj as(
	SELECT vendor_name,vendor_code,proj_name,${mysql_yesterday_l_month} sales_month from mm_label
	UNION
	SELECT vendor_name,vendor_code,proj_name,${mysql_yesterday_l_month} sales_month from mm_nonlabel
	UNION
	SELECT vendor_name,vendor_code,proj_name,${mysql_yesterday_l_month} sales_month from mso
)
, customer_proj_sales_rate as ( -- 计算经销商-proj 级别各销量、备货量、rate
SELECT 
	customer_proj.vendor_code,
	customer_proj.vendor_name,
	customer_proj.sales_month,
	sum(COALESCE(mm_label.sales_value,0)) as mm_label_sales,
	sum(COALESCE(mm_label.shipment_sales,0)) as mm_label_stock,
	sum(COALESCE(mm_nonlabel.sales_value,0)) as mm_nonlabel_sales,
	sum(COALESCE(mm_nonlabel.shipment_sales,0)) as mm_nonlabel_stock,
	sum(COALESCE(mso.sales_value,0)) as mso_sales,
	sum(COALESCE(mso.shipment_sales,0)) as mso_stock,
	sum(COALESCE(COALESCE(mm_label.sales_value,0)/NULLIF(makeup.makeup_rate,0),0) 
	+ COALESCE(COALESCE(mm_nonlabel.sales_value,0)/NULLIF(makeup.makeup_rate,0),0)
	+ COALESCE(COALESCE(mso.sales_value,0)/NULLIF(makeup.makeup_rate,0),0)) as '分子1',
	sum(COALESCE(COALESCE(mm_nonlabel.sales_value,0)/NULLIF(makeup.makeup_rate,0),0)
	+ COALESCE(COALESCE(mso.sales_value,0)/NULLIF(makeup.makeup_rate,0),0)) as '分子2',
	-- 公式1.5*（sum(序号4,序号5)/加价率）-sum(序号8,序号9)
	-- 1.5*(sum(COALESCE(mm_nonlabel.sales_value,0)+COALESCE(mso.sales_value,0)/NULLIF(makeup.makeup_rate,0)))
	-- - (sum(COALESCE(mm_nonlabel.shipment_sales,0) + COALESCE(mso.shipment_sales,0)))
    (1.5*(sum(COALESCE(COALESCE(mm_nonlabel.sales_value,0)/NULLIF(makeup.makeup_rate,0),0)
	+ COALESCE(COALESCE(mso.sales_value,0)/NULLIF(makeup.makeup_rate,0),0)))
    -sum(COALESCE(mm_nonlabel.shipment_sales,0))
    -sum(COALESCE(mso.shipment_sales,0)))
	as target_sales,
	avg(makeup.makeup_rate) as makeup_rate
	
	from customer_proj
	LEFT JOIN mm_label
	on customer_proj.vendor_code = mm_label.vendor_code
	and customer_proj.sales_month = mm_label.sales_month
	and customer_proj.proj_name = mm_label.proj_name
	LEFT JOIN mm_nonlabel
	on customer_proj.vendor_code = mm_nonlabel.vendor_code
	and customer_proj.sales_month = mm_nonlabel.sales_month
	and customer_proj.proj_name = mm_nonlabel.proj_name
	LEFT JOIN mso
	on customer_proj.vendor_code = mso.vendor_code
	and customer_proj.sales_month = mso.sales_month
	and customer_proj.proj_name = mso.proj_name
	LEFT JOIN makeup
	on customer_proj.vendor_code = makeup.vendor_code
	-- and SUBSTR(customer_proj.sales_month,1,4) = makeup.sales_year +1
	and customer_proj.proj_name = makeup.proj_name


GROUP BY 
		customer_proj.vendor_code,
		customer_proj.vendor_name,
		customer_proj.sales_month

)   
SELECT * FROM customer_proj_sales_rate 
