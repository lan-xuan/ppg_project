/*TRUNCATE TABLE fine_dm.dm_dashboard;
INSERT INTO fine_dm.dm_dashboard
(category,
category_brand,
category_product_type,
date_type,
channel,
sales_month,
pc,
mtd,
prior,
vs_prior,
vs_target,
mtd_target,
proj_name,
proj_name_en,
eop_rate,
shipment_sales,
sales_quarter,
sales_year,
sales_month_en,
etl_time,
report_date) 
*/
WITH cal as -- dm_dashboard
(
	SELECT 
		DISTINCT
        cal.actual_month sales_month,
				cal.actual_quarter sales_quarter,
				cal.actual_year sales_year
    FROM fine_ods.ods_calendar_info_df cal
	where actual_year = '2024'
)
, dw_distributor_sales_target as(
	SELECT * from fine_dw.dw_distributor_sales_target WHERE target_year = '2024'
)
, dw_mm_sales_target as(
	SELECT * from fine_dw.dw_mm_sales_target WHERE target_year = '2024'
)
, dw_mso_sales_target as(
	SELECT * from fine_dw.dw_mso_sales_target WHERE target_year = '2024'
)
,dw_school_sales_target as(
	SELECT * from fine_dw.dw_school_sales_target WHERE target_year = '2024'
)
,ods_mso_sales_target_df as(
	SELECT * from fine_ods.ods_mso_sales_target_df WHERE target_year = '2024'
)
,target_d as( -- 4
    SELECT
		'DISTRIBUTOR' channel,
        sum(sales_target) as sales_value,
        sales_month,
        sales_quarter,
        sales_year
        FROM cal
        LEFT JOIN
        (
            SELECT customer_code,Q1/3 as sales_target,CONCAT(target_year,'Q1') as target_quarter FROM dw_distributor_sales_target
            UNION ALL
            SELECT customer_code,Q2/3 as sales_target,CONCAT(target_year,'Q2') as target_quarter FROM dw_distributor_sales_target
            UNION ALL
            SELECT customer_code,Q3/3 as sales_target,CONCAT(target_year,'Q3') as target_quarter FROM dw_distributor_sales_target
            UNION ALL
            SELECT customer_code,Q4/3 as sales_target,CONCAT(target_year,'Q4') as target_quarter FROM dw_distributor_sales_target
        ) a 
        on target_quarter = sales_quarter
			WHERE customer_code is not null
			GROUP BY
        sales_month,
        sales_quarter,
        sales_year
		UNION ALL
		SELECT 'MM' as channel,sum(sales_target) as sales_target,target_month,target_quarter,target_year FROM dw_mm_sales_target GROUP BY target_month,target_quarter,target_year
		UNION ALL
		SELECT 'MSO' as channel,sum(sales_target) as sales_target,target_month,target_quarter,target_year FROM dw_mso_sales_target WHERE upper(report_brand_group) = 'TOTAL' GROUP BY target_month,target_quarter,target_year
		UNION ALL
		SELECT 'SCHOOL' as channel,sum(sales_target) as sales_target,target_month,target_quarter,target_year FROM dw_school_sales_target GROUP BY target_month,target_quarter,target_year

) -- SELECT * FROM target;
,target_others as( -- 4
    SELECT
				report_brand_name as  channel,
				report_brand_name,
        sum(sales_target) as sales_value,
        sales_month,
        sales_quarter,
        sales_year
        FROM cal
        LEFT JOIN
        (
			SELECT 'ALLIED' report_brand_name,(yhp_1+yzh_1)/3 as sales_target,CONCAT(target_year,'Q1') as target_quarter FROM dw_distributor_sales_target
            UNION ALL
            SELECT 'ALLIED' report_brand_name,(yhp_2+yzh_2)/3 as sales_target,CONCAT(target_year,'Q2') as target_quarter FROM dw_distributor_sales_target
            UNION ALL
            SELECT 'ALLIED' report_brand_name,(yhp_3+yzh_3)/3 as sales_target,CONCAT(target_year,'Q3') as target_quarter FROM dw_distributor_sales_target
            UNION ALL
            SELECT 'ALLIED' report_brand_name,(yhp_4+yzh_4)/3 as sales_target,CONCAT(target_year,'Q4') as target_quarter FROM dw_distributor_sales_target
						
			UNION ALL
			SELECT 'DIGITAL' report_brand_name,lqhg_1/3 as sales_target,CONCAT(target_year,'Q1') as target_quarter FROM dw_distributor_sales_target
            UNION ALL
            SELECT 'DIGITAL' report_brand_name,lqhg_2/3 as sales_target,CONCAT(target_year,'Q2') as target_quarter FROM dw_distributor_sales_target
            UNION ALL
            SELECT 'DIGITAL' report_brand_name,lqhg_3/3 as sales_target,CONCAT(target_year,'Q3') as target_quarter FROM dw_distributor_sales_target
            UNION ALL
            SELECT 'DIGITAL' report_brand_name,lqhg_4/3 as sales_target,CONCAT(target_year,'Q4') as target_quarter FROM dw_distributor_sales_target
						
			UNION ALL
            SELECT 'ALLIED' report_brand_name,(Q1Sundries+Q1Putty)/3 as sales_target,CONCAT(target_year,'Q1') as target_quarter FROM ods_mso_sales_target_df
            UNION ALL
            SELECT 'ALLIED' report_brand_name,(Q2Sundries+Q2Putty)/3 as sales_target,CONCAT(target_year,'Q2') as target_quarter FROM ods_mso_sales_target_df
            UNION ALL
            SELECT 'ALLIED' report_brand_name,(Q3Sundries+Q3Putty)/3 as sales_target,CONCAT(target_year,'Q3') as target_quarter FROM ods_mso_sales_target_df
            UNION ALL
            SELECT 'ALLIED' report_brand_name,(Q4Sundries+Q4Putty)/3 as sales_target,CONCAT(target_year,'Q4') as target_quarter FROM ods_mso_sales_target_df
          ) a 
        on target_quarter = sales_quarter
			WHERE 1=1 
			GROUP BY
				report_brand_name,
        sales_month,
        sales_quarter,
        sales_year

) -- SELECT * FROM target_others;
,t_1 as(
SELECT sum(net_sales) net_sales,sum(net_pc) net_pc,sales_month,channel  from fine_dw.dw_price_increase t 
where 1=1
and SUBSTR(t.sales_month,1,4) in( '2024','2023')
GROUP BY channel,sales_month
),t_2 as (
		-- DISTRIBUTOR closed
		SELECT sum(sales_value) as sales_value,sales_month,channel,report_brand_group,category,category_brand,category_product_type
		FROM fine_dw.dw_transaction_detail_report
		WHERE 1=1
		and SUBSTR(sales_month,1,4) in( '2024','2023')
		AND order_type <> 'MM REPLENISH ORDER-SH'
		and channel = 'DISTRIBUTOR'
		and (brand_name <> 'CENTRAL SUPPLY' or brand_name is null)
		GROUP BY sales_month,channel,report_brand_group,category,category_brand,category_product_type

		-- 其他渠道
		UNION ALL
		SELECT sum(sales_value) as sales_value,sales_month,channel,report_brand_group,category,category_brand,category_product_type
		FROM fine_dw.dw_transaction_detail_report
		WHERE 1=1
		AND channel <> 'DISTRIBUTOR'
		GROUP BY sales_month,channel,report_brand_group,category,category_brand,category_product_type

		-- DISTRIBUTOR 备货
		union all
		SELECT sum(-1*distributor_price*sales_qty) as sales_value,sales_month,'DISTRIBUTOR' channel,report_brand_group,category,category_brand,category_product_type
		FROM fine_dw.dw_cb_detail
		WHERE 1=1
				and SUBSTR(sales_month,1,4) in( '2024','2023')
				and sales_qty is not null
				and is_flag = '否'
				and business_type = '回购'
				and vendor_code <>'195726'
				and vendor_code <>'191204'
		GROUP BY sales_month,report_brand_group,category,category_brand,category_product_type

		UNION ALL
		-- DISTRIBUTOR open
		SELECT sum(sales_value) as sales_value,sales_month,channel,report_brand_group,category,category_brand,category_product_type
		FROM fine_dw.dw_backlog_by_product_line
		WHERE 1=1
		and DATE_FORMAT(NOW(), '%Y%m')  = sales_month
		AND order_type <> 'MM REPLENISH ORDER-SH'
		and channel = 'DISTRIBUTOR'
		and (brand_name <> 'CENTRAL SUPPLY' or brand_name is null)
		GROUP BY sales_month,channel,report_brand_group,category,category_brand,category_product_type
		-- 其他渠道
		UNION ALL
		SELECT sum(sales_value) as sales_value,sales_month,channel,report_brand_group,category,category_brand,category_product_type
		FROM fine_dw.dw_backlog_by_product_line
		WHERE 1=1
		and DATE_FORMAT(NOW(), '%Y%m')  = sales_month
		AND channel <> 'DISTRIBUTOR'
		GROUP BY sales_month,channel,report_brand_group,category,category_brand,category_product_type


),
t_3 as(
		SELECT sum(t1.sales_value) as sales_value,t1.sales_month,t1.channel
		FROM t_2 t1
		GROUP BY t1.sales_month,t1.channel
),
t_4 as(
		SELECT sum(t1.sales_value) as sales_value,t1.sales_month,t1.channel,report_brand_group
		FROM t_2 t1
		GROUP BY t1.sales_month,t1.channel,report_brand_group
),
t_5 as(
		SELECT sum(t1.sales_value) as sales_value,t1.sales_month,t1.channel,report_brand_group,category,category_brand,category_product_type
		FROM t_2 t1
		GROUP BY t1.sales_month,t1.channel,report_brand_group,category,category_brand,category_product_type
)
 -- sales and pc
, t as(
	SELECT
	sum(net_pc) as net_pc,
	sum(sales_value) as net_sales,
	t2.sales_month,
	t2.channel
	FROM t_2 t2
	LEFT JOIN t_1 t1
	ON t2.sales_month = t1.sales_month
	and t2.channel = t1.channel
	GROUP BY 
		t2.sales_month,
		t2.channel
) -- sales and pc
,mtd as(
SELECT 
t.net_sales,
t.net_pc,
lyear.net_sales as lyear_net_sales,
t.channel,
t.sales_month,
SUBSTR(t.sales_month,1,4) as sales_year,
DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(lyear.sales_month,'01'), '%Y%m%d'), INTERVAL 1 year), '%Y%m') sales_lyear
from t
LEFT JOIN t lyear
ON  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(t.sales_month,'01'), '%Y%m%d'), INTERVAL 1 year), '%Y%m') 
=  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(lyear.sales_month,'01'), '%Y%m%d'), INTERVAL 0 year), '%Y%m')
and t.channel = lyear.channel
)  -- SELECT * FROM t;
,t_others as(
SELECT sum(sales_value) net_sales,null net_pc,sales_month,case when report_brand_group = 'DIGITAL' THEN 'DIGITAL' else 'ALLIED' END as channel  
from t_4 t 
where 1=1
and report_brand_group in ('SUNDRIES','PUTTY','DIGITAL')
GROUP BY sales_month,case when report_brand_group = 'DIGITAL' THEN 'DIGITAL' else 'ALLIED' END 
) -- SELECT * FROM t_others;
,mtd_others as(
SELECT 
t.net_sales,
t.net_pc,
lyear.net_sales as lyear_net_sales,
t.channel,
t.sales_month,
SUBSTR(t.sales_month,1,4) as sales_year,
DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(lyear.sales_month,'01'), '%Y%m%d'), INTERVAL 1 year), '%Y%m') sales_lyear
from t_others t
LEFT JOIN t_others lyear
ON  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(t.sales_month,'01'), '%Y%m%d'), INTERVAL 1 year), '%Y%m') 
=  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(lyear.sales_month,'01'), '%Y%m%d'), INTERVAL 0 year), '%Y%m')
and t.channel = lyear.channel
) --  SELECT * FROM mtd_others;
,t_brand as(
SELECT sum(sales_value) net_sales,sales_month,channel,report_brand_group,category,category_brand,category_product_type  
from t_5 t 
where 1=1
GROUP BY channel,sales_month,report_brand_group,category,category_brand,category_product_type
),t_brand_others as( 
SELECT sum(sales_value) net_sales,sales_month,category,category_brand,category_product_type,case when report_brand_group = 'DIGITAL' THEN 'DIGITAL' else 'ALLIED' END as channel
from t_5 t 
where 1=1
and report_brand_group in ('SUNDRIES','PUTTY','DIGITAL')
GROUP BY sales_month,category,category_brand,category_product_type,case when report_brand_group = 'DIGITAL' THEN 'DIGITAL' else 'ALLIED' END 
),eop as(
    SELECT
--         cs.team_owner,
-- 				cs.sales_person,
				t.channel,
				t.proj_name,
        t.proj_name_en,
        t.pc,
        t.pc/NULLIF(sales_value, 0)  as pc_rate,
        (service_fee * sales_qty) as service_fee, -- 费率*qty
        (service_rate * sales_value) as service_rate,
        case when t.proj_name = '上汽大众' then (service_price * sales_qty) * reward_rate
            else (bs_price * sales_qty) * reward_rate end as  reward_rate, -- 服务商备货价格*qty*费率（其中上汽大众按门店价*qty*费率） 主机厂项目优质服务季度奖励金
        t.rebate_rate * sales_value as rebate_rate,
        adj.adjusted_fee, -- proj_name，所属月份匹配获得
        t.commision_fee_rate * t.sales_qty as  commision_fee,
        t.sales_value,
				sales_month

    FROM fine_dw.dw_stock_replenish_deduction t

            LEFT JOIN fine_dw.dw_adjusted_fee adj
            on adj.proj_name = t.proj_name
-- 	WHERE t.channel = 'MM'
), deduction_sales_1 as(
SELECT 
		category,
		category_brand,
		category_product_type,
		sales_month,
		sum(shipment_sales) as shipment_sales
FROM(
select 
	cb.category,
	cb.category_brand,
	cb.category_product_type,
	sales_month,
	sum(sales_qty*distributor_price) shipment_sales
	from fine_dw.dw_cb_detail cb
	where sales_qty is not null
		and is_flag = '否'
-- 		and channel  in  ('MM','MSO')
		and proj_name is not null
		and business_type = '回购'
		and vendor_code <> '191204'
		and vendor_code <> '195726'
		and vendor_code is not null
GROUP BY 
	cb.category,
	cb.category_brand,
	cb.category_product_type,
	sales_month

UNION ALL
			SELECT 
			category,
			category_brand,
			category_product_type,
			sales_month,
      		sum(sales_value) sales_value
        FROM fine_dw.dw_transaction_detail_report
				WHERE 1=1
				and brand_name = 'CENTRAL SUPPLY' and order_type <> 'MM REPLENISH ORDER-SH'
				and channel = upper('Distributor')
			GROUP BY 
				category,
				category_brand,
				category_product_type,
				sales_month
) a GROUP BY 
		category,
		category_brand,
		category_product_type,
		sales_month
) , deduction_sales_2 as(
SELECT 
		sales_month,
		sum(shipment_sales) as shipment_sales
FROM deduction_sales_1
GROUP BY 
		sales_month

)
, deduction_sales as(
	SELECT 
		sum(t.shipment_sales) as shipment_sales,
		sum(lyear.shipment_sales) as lyear_shipment_saless,
		DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(lyear.sales_month,'01'), '%Y%m%d'), INTERVAL -1 year), '%Y%m') as sales_month,

		SUBSTR(lyear.sales_month,1,4)+1 as sales_year
	from deduction_sales_2  lyear
	LEFT JOIN deduction_sales_2 t
	ON  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(t.sales_month,'01'), '%Y%m%d'), INTERVAL 0 year), '%Y%m') 
	=  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(lyear.sales_month,'01'), '%Y%m%d'), INTERVAL -1 year), '%Y%m')
	GROUP BY
		DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(lyear.sales_month,'01'), '%Y%m%d'), INTERVAL -1 year), '%Y%m'),
		SUBSTR(lyear.sales_month,1,4)+1
),price_pc as (
	SELECT sum(net_sales) net_sales,sum(net_pc) net_pc,sum(net_pc)/sum(net_sales) as pc,sales_month,channel  from fine_dw.dw_price_increase t 
where 1=1
and SUBSTR(t.sales_month,1,4) in( '2024','2023')
GROUP BY channel,sales_month
)

SELECT 
t.category,
t.category_brand,
t.category_product_type,
t.date_type,
t.channel,
t.sales_month,
t.pc,
t.mtd,
t.prior,
t.vs_prior,
t.vs_target,
t.mtd_target,
t.proj_name,
t.proj_name_en,
t.eop_rate,
t.shipment_sales,
cal.sales_quarter,
cal.sales_year,
DATE_FORMAT(STR_TO_DATE(CONCAT(t.sales_month, '01'), '%Y%m%d'), '%b') sales_month_en,
NOW() etl_time,
STR_TO_DATE(CONCAT( cal.sales_year,'0101') , '%Y%m%d') as report_date

	
FROM
(
    SELECT -- 指标卡4个渠道的ytd -- 4个渠道的柱形图月度、季度
	null as category,
	null as category_brand,
	null as category_product_type,
	'channel_sales_y' as date_type,
	target_d.channel,
	'202408' as sales_month, -- 上个月
	null as pc,
	sum(mtd.net_sales) as mtd,
	sum(mtd.lyear_net_sales) as prior,
	-- （actual-target）/target or (actual-prior)/prior
	sum(mtd.net_sales - mtd.lyear_net_sales)/sum(mtd.lyear_net_sales)  as vs_prior,
	sum(mtd.net_sales - target_d.sales_value)/sum(target_d.sales_value)  as vs_target,
	sum(target_d.sales_value) as mtd_target,
	null as proj_name,
  null as proj_name_en,
	null as eop_rate,
	null as shipment_sales
FROM target_d
LEFT JOIN mtd
on mtd.channel = target_d.channel
and mtd.sales_month = target_d.sales_month
WHERE net_sales is not null
and DATE_FORMAT(NOW(), '%Y%m')  <> target_d.sales_month
GROUP BY 
	target_d.channel,
	substr(target_d.sales_month,1,4)

UNION ALL
SELECT -- 指标卡allied、digital的ytd -- allied、digital的柱形图月度、季度
	null as category,
	null as category_brand,
	null as category_product_type,
	'allied_digital_sales_y' as date_type,
	target_d.channel,
	'202408' as sales_month,
	null as pc,
	sum(mtd.net_sales) as mtd,
	sum(mtd.lyear_net_sales) as prior,
	-- （actual-target）/target or (actual-prior)/prior
	sum(mtd.net_sales - mtd.lyear_net_sales)/sum(mtd.lyear_net_sales)  as vs_prior,
	sum(mtd.net_sales - target_d.sales_value)/sum(target_d.sales_value)  as vs_target,
	sum(target_d.sales_value) as mtd_target,
	null as proj_name,
  null as proj_name_en,
	null as eop_rate,
	null as shipment_sales
FROM target_others target_d
LEFT JOIN mtd_others mtd
on mtd.channel = target_d.channel
and mtd.sales_month = target_d.sales_month
WHERE net_sales is not null
and DATE_FORMAT(NOW(), '%Y%m')  <> target_d.sales_month
GROUP BY 
	target_d.channel,
	substr(target_d.sales_month,1,4)

UNION ALL
    SELECT -- 指标卡4个渠道的ytd -- 4个渠道的柱形图月度、季度
	null as category,
	null as category_brand,
	null as category_product_type,
	'channel_sales_y' as date_type,
	'total' as channel,
	'202408' as sales_month, -- 上个月
	null as pc,
	sum(mtd.net_sales) as mtd,
	sum(mtd.lyear_net_sales) as prior,
	-- （actual-target）/target or (actual-prior)/prior
	sum(mtd.net_sales - mtd.lyear_net_sales)/sum(mtd.lyear_net_sales)  as vs_prior,
	sum(mtd.net_sales - target_d.sales_value)/sum(target_d.sales_value)  as vs_target,
	sum(target_d.sales_value) as mtd_target,
	null as proj_name,
  null as proj_name_en,
	null as eop_rate,
	null as shipment_sales
FROM target_d
LEFT JOIN mtd
on mtd.channel = target_d.channel
and mtd.sales_month = target_d.sales_month
WHERE net_sales is not null
and DATE_FORMAT(NOW(), '%Y%m')  <> target_d.sales_month
GROUP BY 
	substr(target_d.sales_month,1,4)

UNION ALL
    SELECT -- 指标卡4个渠道的ytd -- 4个渠道的柱形图月度、季度
	null as category,
	null as category_brand,
	null as category_product_type,
	'channel_sales' as date_type,
	'total' as channel,
	'202409' as sales_month, -- 上个月
	null as pc,
	sum(mtd.net_sales) as mtd,
	sum(mtd.lyear_net_sales) as prior,
	-- （actual-target）/target or (actual-prior)/prior
	sum(mtd.net_sales - mtd.lyear_net_sales)/sum(mtd.lyear_net_sales)  as vs_prior,
	sum(mtd.net_sales - target_d.sales_value)/sum(target_d.sales_value)  as vs_target,
	sum(target_d.sales_value) as mtd_target,
	null as proj_name,
  null as proj_name_en,
	null as eop_rate,
	null as shipment_sales
FROM target_d
LEFT JOIN mtd
on mtd.channel = target_d.channel
and mtd.sales_month = target_d.sales_month
WHERE net_sales is not null
and target_d.sales_month = '202409'
GROUP BY 
	substr(target_d.sales_month,1,4)


UNION ALL
SELECT -- 指标卡4个渠道的mtd -- 4个渠道的柱形图月度、季度
	null as category,
	null as category_brand,
	null as category_product_type,
	'channel_sales' as date_type,
	target_d.channel,
	target_d.sales_month,
	null as pc,
	mtd.net_sales as mtd,
	mtd.lyear_net_sales as prior,
	-- （actual-target）/target or (actual-prior)/prior
	(mtd.net_sales - mtd.lyear_net_sales)/mtd.lyear_net_sales  as vs_prior,
	(mtd.net_sales - target_d.sales_value)/target_d.sales_value  as vs_target,
	target_d.sales_value as mtd_target,
	null as proj_name,
  null as proj_name_en,
	null as eop_rate,
	null as shipment_sales
FROM target_d
LEFT JOIN mtd
on mtd.channel = target_d.channel
and mtd.sales_month = target_d.sales_month
UNION ALL
SELECT -- 指标卡allied、digital的mtd -- allied、digital的柱形图月度、季度
	null as category,
	null as category_brand,
	null as category_product_type,
	'allied_digital_sales' as date_type,
	target_d.channel,
	target_d.sales_month,
	null as pc,
	mtd.net_sales as mtd,
	mtd.lyear_net_sales as prior,
	-- （actual-target）/target or (actual-prior)/prior
	(mtd.net_sales - mtd.lyear_net_sales)/mtd.lyear_net_sales  as vs_prior,
	(mtd.net_sales - target_d.sales_value)/target_d.sales_value  as vs_target,
	target_d.sales_value as mtd_target,
	null as proj_name,
  null as proj_name_en,
	null as eop_rate,
	null as shipment_sales
FROM target_others target_d
LEFT JOIN mtd_others mtd
on mtd.channel = target_d.channel
and mtd.sales_month = target_d.sales_month
UNION ALL
SELECT -- 玫瑰图
	t.category,
	t.category_brand,
	t.category_product_type,
	'brand_sales' as date_type,
	t.channel,
	t.sales_month,
	null as pc,
	t.net_sales as mtd,
	null as prior,
	null as vs_prior,
	null as vs_target,
	null as mtd_target,
	null as proj_name,
  null as proj_name_en,
	null as eop_rate,
	null as shipment_sales
FROM t_brand t
UNION ALL
	SELECT -- 玫瑰图
	t.category,
	t.category_brand,
	t.category_product_type,
	'brand_sales' as date_type,
	t.channel,
	t.sales_month,
	null as pc,
	t.net_sales as mtd,
	null as prior,
	null as vs_prior,
	null as vs_target,
	null as mtd_target,
	null as proj_name,
  null as proj_name_en,
	null as eop_rate,
	null as shipment_sales
FROM t_brand_others t
UNION all
-- SELECT 
-- 	null as category,
-- 	null as category_brand,
-- 	null as category_product_type,
-- 	'pc' as date_type,
-- 	t.channel,
-- 	t.sales_month,
-- 	t.net_pc/t.net_sales as pc,
-- 	null as mtd,
-- 	null as prior,
-- 	null as vs_prior,
-- 	null as vs_target,
-- 	null as mtd_target,
-- 	null as proj_name,
--   null as proj_name_en,
-- 	null as eop_rate,
-- 	null as shipment_sales
-- FROM mtd t
-- UNION ALL
SELECT
	null as category,
	null as category_brand,
	null as category_product_type,
	'eop' as date_type,
	t.channel,
	t.sales_month,
	null as pc,
	sales_value as mtd,
	null as prior,
	null as vs_prior,
	null as vs_target,
	null as mtd_target,
	t.proj_name,
  t.proj_name_en,
	(COALESCE(t.pc , 0) - COALESCE( t.service_fee , 0) - COALESCE( t.service_rate , 0) - COALESCE( t.reward_rate , 0) - COALESCE( t.rebate_rate , 0) - COALESCE( t.adjusted_fee , 0) - COALESCE( t.commision_fee , 0))/NULLIF((COALESCE(sales_value , 0) - COALESCE( t.reward_rate , 0) - COALESCE( t.rebate_rate , 0) - COALESCE( t.adjusted_fee , 0)),0) as eop_rate, -- 公式=ebit/(sales-3-4-5）
	null as shipment_sales
FROM eop t
UNION ALL
SELECT -- 备货扣减trend
	null as category,
	null as category_brand,
	null as category_product_type,
	'channel_sales' as date_type,
	'shipment_sales' as channel,
	t.sales_month,
	null as pc,
	shipment_sales as mtd,
	lyear_shipment_saless as prior,
	null as vs_prior,
	null as vs_target,
	null as mtd_target,
	null as proj_name,
  null as proj_name_en,
	null as eop_rate,
	null as shipment_sales
FROM deduction_sales t
UNION ALL
SELECT -- 备货玫瑰图
	t.category,
	t.category_brand,
	t.category_product_type,
	'brand_sales' as date_type,
	'shipment_sales' as channel,
	t.sales_month,
	null as pc,
	t.shipment_sales as mtd,
	null as prior,
	null as vs_prior,
	null as vs_target,
	null as mtd_target,
	null as proj_name,
  null as proj_name_en,
	null as eop_rate,
	null as shipment_sales
FROM deduction_sales_1 t
UNION ALL
SELECT -- PC
	null as category,
	null as category_brand,
	null as category_product_type,
	'pc' as date_type,
	channel,
	t.sales_month,
	t.pc,
	null as mtd,
	null as prior,
	null as vs_prior,
	null as vs_target,
	null as mtd_target,
	null as proj_name,
  	null as proj_name_en,
	null as eop_rate,
	null as shipment_sales
FROM price_pc t
) t
LEFT JOIN cal
on t.sales_month = cal.sales_month
