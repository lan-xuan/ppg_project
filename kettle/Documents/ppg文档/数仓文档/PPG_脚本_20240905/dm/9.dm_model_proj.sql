
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
		
		  having   sales_year <> SUBSTRING(${mysql_yesterday_l_month},1,4)
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

)  -- SELECT * FROM customer_proj_sales_rate WHERE vendor_code = '123853';
,result as (
	SELECT 
		list.vendor_code,
		list.vendor_name,
-- 		p.proj_name,
		p.sales_value   as distributor_sales, -- price_increase.net_sales or sales_report.sales_value" -- 1
		COALESCE(mm_label_sales,0) + COALESCE(mm_nonlabel_sales,0) + COALESCE(mso_sales,0) as proj_sales, -- 2
		mm_label_sales, -- 3
		mm_nonlabel_sales, -- 4
		mso_sales, -- 5
		COALESCE(stock_label,0) + COALESCE(mm_nonlabel_stock,0) + COALESCE(mso_stock,0) as proj_stock, -- 6 
		stock_label as mm_label_stock, -- 7 *****************transaction表
		mm_nonlabel_stock, -- 8
		mso_stock, -- 9 
		if(p.sales_value < 0,'Y',null) as isoffset_hanging,  -- 公式if序号1<0 then Y else null -- 10
		-- 	nonlabel_sales	不带标签有销量	不带标签销量-公式	 	公式if序号4 or 5is not null then Y else null
		if(mm_nonlabel_sales+mso_sales is null,'Y',null) as is_nonlabel_sales, -- 11
		-- 	stock_nonlabel_rate	备货库存%（不带标签）	备货库存率 不带标签-公式 	
		--  公式sum(序号8,序号9)*加价率/sum(序号4,序号5)
		(mm_nonlabel_stock+mso_stock)/NULLIF(分子2,0) as stock_nonlabel_rate,	-- 12
		--  stock_nonlabel	备货库存 （不带标签）	备货库存 不带标签-公式	 	
		-- 公式IF 序号12>=1.5. then '大于1.5个月",IF1<序号12<1.5% then "1个月-1.5个月",IF 序号12<1 then "小于1个月"
		case when (mm_nonlabel_stock+mso_stock)/NULLIF(分子2,0) >= 1.5 then '大于1.5个月'
		when (mm_nonlabel_stock+mso_stock)/NULLIF(分子2,0) < 1 then '小于1个月'
		when (mm_nonlabel_stock+mso_stock)/NULLIF(分子2,0) is null then null
		else '1个月-1.5个月' end as stock_nonlabel, -- 13
		-- 		stock_rate	备货库存%（TOTAL）	备货库存率-公式		
		--  公式sum(序号7,序号8,序号9)*加价率/sum(序号3,序号4,序号5)
		-- (mm_nonlabel_stock+stock_label) as 分母1,
		(mm_nonlabel_stock+stock_label+mso_stock)/NULLIF(分子1,0) as stock_rate, -- 14
		--  stock	备货库存  （TOTAL）	备货库存-公式	 	
		--  公式IF 序号14>=1.5. then '大于1.5个月",IF1<序号14<1.5% then "1个月-1.5个月",IF 序号14<1 then "小于1个月"
		case when (mm_nonlabel_stock+stock_label+mso_stock)/NULLIF(分子1,0) >1.5 then '大于1.5个月'
		when (mm_nonlabel_stock+stock_label+mso_stock)/NULLIF(分子1,0)  < 1 then '小于1个月'
		when (mm_nonlabel_stock+stock_label+mso_stock)/NULLIF(分子1,0) is null then null
		else '1个月-1.5个月' end as stock, -- 15
		-- target_stock_nonlabel_rate	目标备货库存%（不带标签）	目标-备货库存率 不带标签-默认150%	 	1.5
		1.5 as target_stock_nonlabel_rate, -- 16
		-- 	target_sales	可压经销商渠道 销量额	可压销售额-公式	
		--  公式1.5*（sum(序号4,序号5)/加价率）-sum(序号8,序号9)
		target_sales, -- 17
		makeup_rate,
		list.sales_month

	FROM customer_code_list list
	LEFT JOIN a p
	ON list.vendor_code = p.vendor_code
	and list.sales_month = p.sales_month
	
	LEFT JOIN customer_proj_sales_rate s
	on list.vendor_code = s.vendor_code
	and list.sales_month = s.sales_month

	LEFT JOIN stock
	ON list.vendor_code = stock.vendor_code
	and list.sales_month = stock.sales_month


)  --  SELECT *  from result WHERE vendor_code = '101164';

SELECT 
	case when ship.district is null then m.district else  ship.district end as district,
-- '111' district,
	result.vendor_code as customer_code,
	result.vendor_name as customer_name,
	cs.team_owner,
	cs.team_owner_id,
	cs.sales_person,
	cs.sales_person_id,
	m.u_customer_name   ,
	sum(result.distributor_sales) as distributor_sales,
	sum(result.proj_sales) as proj_sales,
	sum(result.mm_label_sales) as mm_label_sales,
	sum(result.mm_nonlabel_sales) as mm_nonlabel_sales,
	sum(result.mso_sales) as mso_sales,
	sum(result.proj_stock) as proj_stock,
	sum(result.mm_label_stock) as mm_label_stock,
	sum(result.mm_nonlabel_stock) as mm_nonlabel_stock,
	sum(result.mso_stock) as mso_stock,
	result.isoffset_hanging,
	result.is_nonlabel_sales,
	sum(result.stock_nonlabel_rate) as stock_nonlabel_rate,
	result.stock_nonlabel,
	sum(result.stock_rate) as stock_rate,
	result.stock,
	sum(result.target_stock_nonlabel_rate) as target_stock_nonlabel_rate,
	sum(result.target_sales) as target_sales,
	result.sales_month,
			 '' as data_resource, 
          SYSDATE() as etl_time,
          STR_TO_DATE(CONCAT(result.sales_month ,'01') , '%Y%m%d') as report_date
FROM result
		left join  temp_dw_ship_to_list ship
		on upper(result.vendor_code) = ship.customer_code_2
		and ship.starting_date_2 <= STR_TO_DATE(concat(result.sales_month,'01'), '%Y%m%d')
		and ship.ending_date_2 >= STR_TO_DATE(concat(result.sales_month,'01'), '%Y%m%d')

LEFT JOIN (
	SELECT DISTINCT u_customer_code,district,sales_month,u_customer_name from fine_dw.dw_customer_master_list
             where sales_month = ${mysql_yesterday_l_month}) m
    on result.vendor_code = m.u_customer_code
    and result.sales_month = m.sales_month

LEFT JOIN fine_dw.dw_cs_relationship_info cs
		on result.vendor_code = cs.customer_code
		and m.district = cs.district
    and SUBSTR(result.sales_month,1,4) = cs.s_year

-- WHERE vendor_code = '212848'
-- and customer_code = '212848'

GROUP BY 
	case when ship.district is null then m.district else ship.district  end,
	result.vendor_code,
	result.vendor_name,
	cs.team_owner,
	cs.team_owner_id,
	cs.sales_person,
	cs.sales_person_id,
	m.u_customer_name,
	result.isoffset_hanging,
	result.is_nonlabel_sales,
	result.stock_nonlabel,
	result.stock,
	result.sales_month