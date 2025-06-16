/*目标表：fine_dm.dm_distributor_report
  来源表：

fine_dw.dw_transaction_detail_report
fine_dw.dw_distributor_sales_target
temp_002
fine_dw.dw_backlog_by_product_line
fine_dw.dw_customer_master_lis
fine_dw.dw_cs_relationship_info
fine_dw.dw_order_report
fine_ods.ods_calendar_info_df

更新方式：增量更新
更新粒度：年
更新字段：sales_year

*/

with temp_001 as (
select * from 
fine_dw.dw_transaction_detail_report
where 1=1
-- and SUBSTRING(sales_month,1,4) in (${mysql_yesterday_l_year},${mysql_yesterday_d_year})
and SUBSTRING(sales_month,1,4) in (${mysql_yesterday_l_year},${mysql_yesterday_d_year})
-- and sales_month = '202104'
-- and customer_code = '36222'

),temp_002 as (
select * from 
fine_dw.dw_cb_detail
where 1=1
-- and SUBSTRING(sales_month,1,4) in (${mysql_yesterday_l_year},${mysql_yesterday_d_year})
and SUBSTRING(sales_month,1,4) in (${mysql_yesterday_l_year},${mysql_yesterday_d_year})
-- and sales_month = '202104'
-- and customer_code = '36222'

),temp_003 as(
SELECT * FROM
fine_dw.dw_backlog_by_product_line
WHERE 1=1
and (brand_name <> 'CENTRAL SUPPLY' or brand_name is null)
and DATE_FORMAT(NOW(), '%Y%m')  = sales_month
)
 ,temp_ods_calendar_info_df as (
select * 
		      from  fine_ods.ods_calendar_info_df 
				  where actual_year in (${mysql_yesterday_l_year},${mysql_yesterday_d_year})
),

-- 3870 6190
 cal as 
(
	SELECT 
		DISTINCT
        cal.actual_month sales_month,
				cal.actual_quarter sales_quarter,
				cal.actual_year sales_year
    FROM fine_ods.ods_calendar_info_df cal
	UNION
	SELECT DISTINCT
	        cal.actual_year sales_month,
				cal.actual_year sales_quarter,
				cal.actual_year sales_year
    FROM fine_ods.ods_calendar_info_df cal
	UNION
	SELECT DISTINCT
	        cal.actual_quarter sales_month,
				cal.actual_quarter sales_quarter,
				cal.actual_quarter sales_year
    FROM fine_ods.ods_calendar_info_df cal
), cal_q as 
(
	SELECT 
		DISTINCT
--         cal.actual_month sales_month,
				cal.actual_quarter sales_quarter,
				cal.actual_year sales_year
    FROM fine_ods.ods_calendar_info_df cal

),target as( -- 4
    SELECT
        a.customer_code,
        a.customer_name,
		'DISTRIUBUTOR' channel,
		a.district,
        sales_target as sales_value,
        sales_month,
        sales_quarter,
        sales_year,
				'Total Sales Target' as sec_1
        FROM cal
        LEFT JOIN
        (
            SELECT customer_code,customer_name,district,Q1/3 as sales_target,CONCAT(target_year,'Q1') as target_quarter FROM fine_dw.dw_distributor_sales_target
            UNION ALL
            SELECT customer_code,customer_name,district,Q2/3 as sales_target,CONCAT(target_year,'Q2') as target_quarter FROM fine_dw.dw_distributor_sales_target
            UNION ALL
            SELECT customer_code,customer_name,district,Q3/3 as sales_target,CONCAT(target_year,'Q3') as target_quarter FROM fine_dw.dw_distributor_sales_target
            UNION ALL
            SELECT customer_code,customer_name,district,Q4/3 as sales_target,CONCAT(target_year,'Q4') as target_quarter FROM fine_dw.dw_distributor_sales_target
        ) a 
        on target_quarter = sales_quarter
			WHERE customer_code is not null
			and sales_target is not null
			and sales_month <> sales_quarter

) -- SELECT * FROM target;
,target_others as( -- 5、6
    SELECT
        a.customer_code,
        a.customer_name,
		'DISTRIBUTOR' channel,
		a.district,
        sales_target as sales_value,
        cal.sales_month,
        sales_quarter,
        sales_year,
		'Putty Sales Target' as sec_1
        FROM cal
        LEFT JOIN
        (
            SELECT customer_code,customer_name,district,yzh_1/3 as sales_target,CONCAT(target_year,'Q1') as target_quarter FROM fine_dw.dw_distributor_sales_target
            UNION ALL
            SELECT customer_code,customer_name,district,yzh_2/3 as sales_target,CONCAT(target_year,'Q2') as target_quarter FROM fine_dw.dw_distributor_sales_target
            UNION ALL
            SELECT customer_code,customer_name,district,yzh_3/3 as sales_target,CONCAT(target_year,'Q3') as target_quarter FROM fine_dw.dw_distributor_sales_target
            UNION ALL
            SELECT customer_code,customer_name,district,yzh_4/3 as sales_target,CONCAT(target_year,'Q4') as target_quarter FROM fine_dw.dw_distributor_sales_target
        ) a 
        on target_quarter = sales_quarter
    UNION ALL
    SELECT
        a.customer_code,
        a.customer_name,
		'DISTRIBUTOR' channel,
		a.district,
        sales_target as sales_value,
        cal.sales_month,
        sales_quarter,
        sales_year,
		'Sundries Sales Target' as sec_1
        FROM cal
        LEFT JOIN
        (
            SELECT customer_code,customer_name,district,yhp_1/3 as sales_target,CONCAT(target_year,'Q1') as target_quarter FROM fine_dw.dw_distributor_sales_target
            UNION ALL
            SELECT customer_code,customer_name,district,yhp_2/3 as sales_target,CONCAT(target_year,'Q2') as target_quarter FROM fine_dw.dw_distributor_sales_target
            UNION ALL
            SELECT customer_code,customer_name,district,yhp_3/3 as sales_target,CONCAT(target_year,'Q3') as target_quarter FROM fine_dw.dw_distributor_sales_target
            UNION ALL
            SELECT customer_code,customer_name,district,yhp_4/3 as sales_target,CONCAT(target_year,'Q4') as target_quarter FROM fine_dw.dw_distributor_sales_target
        ) a 
        on target_quarter = sales_quarter


) -- SELECT * from target_others;
,others_y as -- SUNDRIES PUTTY
	(
	select 
		t.customer_code,
        t.customer_name,
		t.channel,
		t.district,
		sum(t.sales_value) as sales_value,
		t.sales_month,
		t.sales_quarter,
		report_brand_name,
		report_brand_group
		FROM
		temp_001 t
		WHERE t.channel = upper('Distributor') -- 85872332.1000
		and order_type <> 'MM REPLENISH ORDER-SH'
		and report_brand_name in( 'SUNDRIES','PUTTY')
		GROUP BY
		t.sales_month ,
		t.sales_quarter,
		t.customer_code,
        t.customer_name,
		t.channel,
		t.district,
		report_brand_name,
		report_brand_group
	UNION ALL
	select 
		t.customer_code,
        t.customer_name,
		t.channel,
		t.district,
		sum(-t.sales_value) as sales_value,
		t.sales_month,
		t.sales_quarter,
		report_brand_name,
		report_brand_group
		FROM
		temp_001 t
		WHERE t.channel = upper('Distributor') -- 85872332.1000
		and order_type <> 'MM REPLENISH ORDER-SH'
		and upper(report_brand_name) in( 'SUNDRIES','PUTTY')
		and business_type = 'gap'
		GROUP BY
		t.sales_month ,
		t.sales_quarter,
		t.customer_code,
        t.customer_name,
		t.channel,
		t.district,
		report_brand_name,
		report_brand_group
)	-- SELECT * FROM others_y;
,others_y_deductoion as -- SUNDRIES PUTTY
	(
		select 
		sum(sales_qty*distributor_price) shipment_sales,
		vendor_code,
        vendor_name,
		channel,
		district,
		sales_month,
		sales_quarter,
		report_brand_name,
		report_brand_group
		from temp_002 cb
		where sales_qty is not null
		and is_flag = '是'
		and channel = 'MM'
		and business_type = '回购'
		and upper(report_brand_name) in( 'SUNDRIES','PUTTY')
		GROUP BY 
		vendor_code,
        vendor_name,
		channel,
		district,
		sales_month,
		sales_quarter,
		report_brand_name,
		report_brand_group
	)	,others_y_net as -- SUNDRIES PUTTY
	(
		select 
		COALESCE(others_y.sales_value, 0) - COALESCE(others_y_deductoion.shipment_sales,0) as sales_value,
		others_y.customer_code,
        others_y.customer_name,
		others_y.channel,
		others_y.district,
		others_y.sales_month,
		others_y.sales_quarter,
		others_y.report_brand_name,
		others_y.report_brand_group
		from others_y
		LEFT JOIN others_y_deductoion
		on others_y.customer_code =  others_y_deductoion.vendor_code
		and others_y.sales_month =  others_y_deductoion.sales_month
		and others_y.report_brand_name =  others_y_deductoion.report_brand_name
		
) -- SELECT * FROM others_y_deductoion;
,offsets as( -- 9 冲抵倒挂
SELECT sum(sales_value) sales_value,customer_code,customer_name,channel,district,sales_month,sales_quarter,sec_1 from (
			SELECT 
			case when order_type = 'MM REPLENISH ORDER-SH' then '补倒挂' else '带标签' end as sec_1,
      sum(sales_value) sales_value,
      customer_code,
      customer_name,
			channel,
			district,
      sales_month,
			sales_quarter
        FROM temp_001
        where channel = upper('Distributor')
				and (brand_name = 'CENTRAL SUPPLY' or order_type = 'MM REPLENISH ORDER-SH')
        GROUP BY
				order_type,
			customer_code,
            customer_name,
			channel,
			district,
			sales_month,
			sales_quarter,
			case when order_type = 'MM REPLENISH ORDER-SH' then '补倒挂' else '带标签' end --  24,173,942 
			)s
			group by customer_code,customer_name,channel,district,sales_month,sales_quarter,sec_1

) -- SELECT * FROM offsets;
,closed as ( -- 7 Closed Order 不包含备货 但还包含mm_label 
SELECT 
		sum(sales_value)   sales_value,
		customer_code,
        customer_name,
		channel,
		district,
		sales_month,
		sales_quarter
FROM
(


	SELECT 
		sum(sales_value)   sales_value,
		customer_code,
        customer_name,
		channel,
		district,
		sales_month,
		sales_quarter
		FROM temp_001 t
		where order_type <> 'MM REPLENISH ORDER-SH'
		and channel = upper('Distributor')
-- 		and customer_code = '13033'
		group by 
		customer_code,
        customer_name,
		channel,
		district,
		sales_month,
		sales_quarter
		UNION ALL 
		SELECT 
			sum(-sales_value)   sales_value,
			customer_code,
            customer_name,
			channel,
			district,
			sales_month,
			sales_quarter
			FROM temp_001 t
			where order_type <> 'MM REPLENISH ORDER-SH'
			and business_type = 'gap'
			and (brand_name <>'CENTRAL SUPPLY' OR brand_name is null)
			and channel = upper('Distributor')
	-- 		and customer_code = '13033'
			group by 
			customer_code,
            customer_name,
			channel,
			district,
			sales_month,
			sales_quarter
) sssss
GROUP BY 
		customer_code,
        customer_name,
		channel,
		district,
		sales_month,
		sales_quarter
)
, open_sales as ( -- 8  Open Orde  待处理中
	SELECT 		
	sum(sales_value)   sales_value,
		customer_code,
        customer_name,
		channel,
		district,
		sales_month,
		sales_quarter
FROM(
	SELECT 
		sum(sales_value)   sales_value,
		customer_code,
        customer_name,
		channel,
		district,
		sales_month,
		sales_quarter
		FROM temp_003 t
		where 1=1
		and channel = upper('Distributor')
		AND MONTH(STR_TO_DATE(concat(t.sales_month,'01'), '%Y%m%d')) = MONTH(CURDATE()) 
		AND YEAR(STR_TO_DATE(concat(t.sales_month,'01'), '%Y%m%d')) = YEAR(CURDATE())

		group by 
		customer_code,
        customer_name,
		channel,
		district,
		sales_month,
		sales_quarter
	UNION ALL
	SELECT 
		sum(-sales_value)   sales_value,
		customer_code,
        customer_name,
		channel,
		district,
		sales_month,
		sales_quarter
		FROM temp_003 t
		where 1=1
		and channel = upper('Distributor')
		AND MONTH(STR_TO_DATE(concat(t.sales_month,'01'), '%Y%m%d')) = MONTH(CURDATE()) 
		AND YEAR(STR_TO_DATE(concat(t.sales_month,'01'), '%Y%m%d')) = YEAR(CURDATE())
		and business_type = 'gap'
		group by 
		customer_code,
        customer_name,
		channel,
		district,
		sales_month,
		sales_quarter
)ssss 
GROUP BY
		customer_code,
        customer_name,
		channel,
		district,
		sales_month,
		sales_quarter

) -- ELECT * FROM open_sales; closed;
, mm_label_off  as ( -- 主机厂带标签项目备货 -- 

SELECT sum(sales_value) sales_value,customer_code,customer_name,channel,district,sales_month,sales_quarter,sec_1 from (
			SELECT 
			case when order_type = 'MM REPLENISH ORDER-SH' then '补倒挂' else '带标签' end as sec_1,
      sum(sales_value) sales_value,
      customer_code,
      customer_name,
			channel,
		district,
      sales_month,
			sales_quarter
        FROM temp_001
        where channel = upper('Distributor')
				and (brand_name = 'CENTRAL SUPPLY' or order_type = 'MM REPLENISH ORDER-SH')
        GROUP BY
				order_type,
			customer_code,
            customer_name,
			channel,
		district,
			sales_month,
			sales_quarter,
			case when order_type = 'MM REPLENISH ORDER-SH' then '补倒挂' else '带标签' end --  24,173,942 
			
			)s
			group by customer_code,customer_name,channel,district,sales_month,sales_quarter,sec_1

) -- SELECT * FROM mm_label_off;
, mm_nonlabel  as ( -- 主机厂不带标签项目备货
		select 
		sum(sales_qty*distributor_price) shipment_sales,
		vendor_code,
        vendor_name,
		channel,
		district,
		sales_month,
		sales_quarter
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
		channel,
		district,
		sales_month,
		sales_quarter

) -- SELECT * FROM mm_nonlabel;
, mso  as ( -- 集团不带标签项目备货
		select 
		sum(sales_qty*distributor_price) shipment_sales,
		vendor_code,
        vendor_name,
		channel,
		district,
		sales_month,
		sales_quarter
		from temp_002 cb
		where sales_qty is not null
		and is_flag = '否'
		and channel = 'MSO'
		and business_type = '回购'
		and vendor_code <>'195726'
		and vendor_code <>'191204'
		GROUP BY 
		vendor_code,
        vendor_name,
		channel,
		district,
		sales_month,
		sales_quarter
) -- SELECT * FROM mm_nonlabel;
, closed_sales as ( --  closed_sales-shipment_sales(主机厂备货）

	SELECT 
			closed.customer_code,
            closed.customer_name,
			closed.channel,
			closed.district,
			COALESCE(closed.sales_value, 0) - COALESCE(mm_label.sales_value, 0) as sales_value,
			-- sales_value,
			-- sales_value,
			closed.sales_month,
			closed.sales_quarter
		FROM closed
		LEFT JOIN mm_label_off mm_label
		on closed.customer_code = mm_label.customer_code
		and closed.sales_month = mm_label.sales_month
        and closed.district = mm_label.district
		and sec_1 ='带标签'
) -- SELECT * FROM closed_sales;
, mtd as ( -- 45 MTD Sales

		SELECT 	customer_code,customer_name,channel,district,sum(sales_value) as mtd_sales, sales_month,sales_quarter from(
			select customer_code,customer_name,channel,district,COALESCE(sales_value, 0) sales_value,sales_month, sales_quarter,1 type FROM closed_sales
			UNION ALL
			select customer_code,customer_name,channel,district,COALESCE(sales_value, 0) sales_value,sales_month, sales_quarter,2 type FROM open_sales
			UNION ALL
			select vendor_code,vendor_name,channel,district,-COALESCE(shipment_sales, 0) sales_value,sales_month, sales_quarter,3 type FROM mm_nonlabel
			UNION ALL
			select vendor_code,vendor_name,channel,district,-COALESCE(shipment_sales, 0) sales_value,sales_month, sales_quarter,4 type FROM mso
		) t
		GROUP by customer_code,customer_name,channel,district,sales_month,sales_quarter

) --  select * from mtd;
, mm_proj as (-- 主机厂不带标签项目备货 明细
		select 
		sum(sales_qty*distributor_price) shipment_sales,
		vendor_code,
        vendor_name,
		channel,
		district,
		proj_name_en as proj_name,
		sales_month,
		sales_quarter
		from temp_002 cb
		where sales_qty is not null
		and is_flag = '否'
		and channel = 'MM'
		and business_type = '回购'
		and vendor_code <> '195726'
		and vendor_code <> '191204'
		and proj_name is not null
		and vendor_code is not null
		GROUP BY 
		vendor_code,
        vendor_name,
		channel,
		district,
		proj_name_en,
		sales_month,
		sales_quarter
) -- select * from mm_proj;
, mso_proj as (--  集团项目备货 
		select 
		sum(sales_qty*distributor_price) shipment_sales,
		vendor_code,
        vendor_name,
		channel,
		district,
		proj_name_en as proj_name,
		sales_month,
		sales_quarter
		from temp_002 cb
		where sales_qty is not null
		and is_flag = '否'
		and channel = 'MSO'
		and business_type = '回购'
		and vendor_code <>'195726'
		and vendor_code <>'191204'
		GROUP BY 
		vendor_code,
        vendor_name,
		channel,
		district,
		proj_name_en,
		sales_month,
		sales_quarter
)
, brand_deduction as (-- brand备货 明细
SELECT
		vendor_code,
        vendor_name,
-- 		channel,
		district,
		report_brand_group,
		report_brand_name,
		sales_month,
		sales_quarter,
		sum(shipment_sales) as shipment_sales
	FROM (
		select 
		sum(sales_qty*distributor_price) shipment_sales,
		vendor_code,
        vendor_name,
-- 		channel,
		district,
		report_brand_group,
		report_brand_name,
		sales_month,
		sales_quarter
		from temp_002 cb
		where sales_qty is not null
		and is_flag = '否'
-- 		and channel = 'MSO'
		and business_type = '回购'
		and vendor_code <>'195726'
		and vendor_code <>'191204'
		and case when report_brand_name in('2K','GRS','EMAXX') then category <> 'PUTTY' else 1=1 end
		GROUP BY 
		vendor_code,
        vendor_name,
-- 		channel,
		district,
		report_brand_group,
		report_brand_name,
		sales_month,
		sales_quarter
        UNION ALL
		select 
		sum(sales_qty*distributor_price) shipment_sales,
		vendor_code,
        vendor_name,
-- 		channel,
		district,
		report_brand_group,
		report_brand_name,
		sales_month,
		sales_quarter
		from temp_002 cb
		where sales_qty is not null
		and is_flag = '否'
-- 		and channel = 'MSO'
		and vendor_code <>'195726'
		and vendor_code <>'191204'
		and business_type = 'gap'
		and case when report_brand_name in('2K','GRS','EMAXX') then category <> 'PUTTY' else 1=1 end
		GROUP BY 
		vendor_code,
        vendor_name,
-- 		channel,
		district,
		report_brand_group,
		report_brand_name,
		sales_month,
		sales_quarter
	)ssss
	GROUP By
		vendor_code,
        vendor_name,
-- 		channel,
		district,
		report_brand_group,
		report_brand_name,
		sales_month,
		sales_quarter
)
, brand as (-- brand 明细
SELECT 
	sum(sales_value) as sales_value,
	customer_code,
    customer_name,
	channel,
	district,
	report_brand_group,
	report_brand_name,
	sales_month,
	sales_quarter
FROM(
		select 
		sum(sales_value) sales_value,
		customer_code,
        customer_name,
		channel,
		district,
		report_brand_group,
		report_brand_name,
		sales_month,
		sales_quarter
		FROM temp_001 t
		where order_type <> 'MM REPLENISH ORDER-SH'
		and brand_name <> 'CENTRAL SUPPLY'
		and channel = 'DISTRIBUTOR'
		and case when report_brand_name in('2K','GRS','EMAXX') then category <> 'PUTTY' else 1=1 end
		GROUP BY 
		customer_code,
        customer_name,
		channel,
		district,
		report_brand_group,
		report_brand_name,
		sales_month,
		sales_quarter
		UNION ALL
		select 
		sum(-1*sales_value) sales_value,
		customer_code,
        customer_name,
		channel,
		district,
		report_brand_group,
		report_brand_name,
		sales_month,
		sales_quarter
		FROM temp_001 t
		where order_type <> 'MM REPLENISH ORDER-SH'
		and brand_name <> 'CENTRAL SUPPLY'
		and channel = 'DISTRIBUTOR'
		and business_type = 'gap'
		GROUP BY 
		customer_code,
        customer_name,
		channel,
		district,
		report_brand_group,
		report_brand_name,
		sales_month,
		sales_quarter

		UNION ALL
		select 
		sum(sales_value) sales_value,
		customer_code,
        customer_name,
		channel,
		district,
		report_brand_group,
		report_brand_name,
		sales_month,
		sales_quarter
		FROM temp_003 t
		where order_type <> 'MM REPLENISH ORDER-SH'
		and brand_name <> 'CENTRAL SUPPLY'
		and channel = 'DISTRIBUTOR'
		and case when report_brand_name in('2K','GRS','EMAXX') then category <> 'PUTTY' else 1=1 end
		GROUP BY 
		customer_code,
        customer_name,
		channel,
		district,
		report_brand_group,
		report_brand_name,
		sales_month,
		sales_quarter

		UNION ALL
		select 
		sum(-1*sales_value) sales_value,
		customer_code,
        customer_name,
		channel,
		district,
		report_brand_group,
		report_brand_name,
		sales_month,
		sales_quarter
		FROM temp_003 t
		where order_type <> 'MM REPLENISH ORDER-SH'
		and brand_name <> 'CENTRAL SUPPLY'
		and channel = 'DISTRIBUTOR'
		and business_type = 'gap'
		GROUP BY 
		customer_code,
        customer_name,
		channel,
		district,
		report_brand_group,
		report_brand_name,
		sales_month,
		sales_quarter
) ssss
	GROUP BY
		customer_code,
        customer_name,
		channel,
		district,
		report_brand_group,
		report_brand_name,
		sales_month,
		sales_quarter

)
, customer_list as(
SELECT DISTINCT
		customer_code,
        customer_name,
-- 		channel,
		district,
		report_brand_name,
		report_brand_group,
		sales_month,
		sales_quarter
FROM (
	SELECT DISTINCT
		customer_code,
        customer_name,
-- 		channel,
		district,
		report_brand_name,
		report_brand_group,
		sales_month,
		sales_quarter
		FROM brand
		UNION all
	SELECT DISTINCT
		vendor_code,
        vendor_name,
-- 		channel,
		district,
		report_brand_name,
		report_brand_group,
		sales_month,
		sales_quarter
		FROM brand_deduction
	)sss
)		
,brand_net as(

	select

		COALESCE(brand.sales_value, 0) - COALESCE(brand_deduction.shipment_sales, 0) as sales_value,

		customer_list.customer_code as vendor_code,
        customer_list.customer_name as vendor_name,
		'DISTRIBUTOR' channel,
		customer_list.district,
		customer_list.report_brand_group,
		customer_list.report_brand_name,
		customer_list.sales_month,
		customer_list.sales_quarter
	
	FROM customer_list
	LEFT JOIN brand
	on brand.report_brand_name = customer_list.report_brand_name
	and brand.report_brand_group = customer_list.report_brand_group
	and brand.customer_code = customer_list.customer_code
	and brand.sales_month = customer_list.sales_month
	and brand.district = customer_list.district
	LEFT JOIN brand_deduction
-- 	on brand.report_brand_group = brand_deduction.report_brand_group 
	on customer_list.report_brand_name = brand_deduction.report_brand_name
	and customer_list.report_brand_group = brand_deduction.report_brand_group
	and customer_list.customer_code = brand_deduction.vendor_code
	and customer_list.sales_month = brand_deduction.sales_month
	and customer_list.district = brand_deduction.district

) -- SELECT * FROM brand_net; -- brand_deduction
,fill_gap as( -- Fill Q1 Sales Gap Vs. Target - APR
		select 
		t.customer_code,
        t.customer_name,
		t.channel,
		t.district,
		sum(t.sales_value) as sales_value,
		t.sales_month as sales_month,
		t.sales_quarter,
		report_brand_name,
		report_brand_group
		FROM
		temp_001 t
		WHERE t.channel = upper('Distributor') -- 85872332.1000
        and (brand_name <> 'CENTRAL SUPPLY' or brand_name is null)
		and business_type = 'gap'
		GROUP BY
		t.sales_month ,
		t.sales_quarter,
		t.customer_code,
        t.customer_name,
		t.channel,
		t.district,
		report_brand_name,
		report_brand_group
-- 		UNION ALL  --差211、212、213
		
),fill_gap_open as( -- Fill Q1 Sales Gap Vs. Target - APR
		select 
		t.customer_code,
        t.customer_name,
		t.channel,
		t.district,
		sum(t.sales_value) as sales_value,
		t.sales_month as sales_month,
		t.sales_quarter,
		report_brand_name,
		report_brand_group
		FROM temp_003 t
		WHERE t.channel = upper('Distributor') -- 85872332.1000
        and (brand_name <> 'CENTRAL SUPPLY' or brand_name is null)
		and business_type = 'gap'
		GROUP BY
		t.sales_month ,
		t.sales_quarter,
		t.customer_code,
        t.customer_name,
		t.channel,
		t.district,
		report_brand_name,
		report_brand_group
-- 		UNION ALL  --差211、212、213
		
)
,fill_gap_adj as (
	SELECT
			customer_code,
            customer_name,
			channel,
			district,
			sales_value,
			sales_month as sales_month,
		    adjusted_quarter as sales_quarter,
			report_brand_name,
			report_brand_group
		FROM  temp_ods_calendar_info_df a 
				LEFT JOIN fill_gap
					on adjusted_month = sales_month

)
,fill_gap_adj_open as (
	SELECT
			customer_code,
            customer_name,
			channel,
			district,
			sales_value,
			sales_month as sales_month,
		    adjusted_quarter as sales_quarter,
			report_brand_name,
			report_brand_group
		FROM temp_ods_calendar_info_df  a 
				LEFT JOIN fill_gap_open
					on adjusted_month = sales_month

),fill_gap_adj_q as (
	SELECT
			customer_code,
            customer_name,
			channel,
			district,
			sales_value,
			sales_month as sales_month,
		    actual_quarter as sales_quarter,
			report_brand_name,
			report_brand_group
		FROM  temp_ods_calendar_info_df  a 
				LEFT JOIN fill_gap
					on adjusted_month = sales_month

)
,fill_gap_adj_open_q as (
	SELECT
			customer_code,
            customer_name,
			channel,
			district,
			sales_value,
			sales_month as sales_month,
		    actual_quarter as sales_quarter,
			report_brand_name,
			report_brand_group
		FROM  (select * 
		          from  fine_ods.ods_calendar_info_df 
				  where actual_year in (${mysql_yesterday_l_year},${mysql_yesterday_d_year})
				  ) a 
				LEFT JOIN fill_gap_open
					on adjusted_month = sales_month

)
-- 	SELECT sum(sales_value),sales_month,sales_quarter from fill_gap_adj_q GROUP BY sales_month,sales_quarter;
,month_actual as(
		SELECT
		-- DISTINCT 		orderno.sec_1,orderno.order_no
				t.customer_code,
                t.customer_name,
				'DISTRIBUTOR' channel,
				t.district,
				t.sec_1,
				t.sec_2,
				t.sec_3,
				t.sec_1 as name_1,
				-- team_owner,
				-- sales_person,
				t.sales_value,
				t.sales_month,
				t.sales_quarter,
				cal.sales_year,
				flag_q
				-- orderno.order_no,
				-- orderno.sec_1,
				-- orderno.sec_2,
				-- orderno.sec_3
			FROM cal
			LEFT JOIN
				(
					select district, channel, customer_code,customer_name,mtd_sales as sales_value,sales_month as sales_month,SUBSTR(sales_month,1,4) as sales_quarter,'Total Sales' as sec_1,CONCAT('FULL YEAR ' , SUBSTR(sales_month,1,4)) as sec_2, null as sec_3,'prior' flag_q from mtd where 1=1 -- total sales FULL YEAR 
					UNION ALL
					SELECT district, channel,customer_code,customer_name,sales_value,sales_month,sales_quarter,'Total Sales Target' as sec_1,null,null,'否' flag_q FROM target -- 4
					UNION ALL
					SELECT district, channel,customer_code,customer_name,sales_value,sales_month,sales_quarter,sec_1,null,null,'否' flag_q FROM target_others -- 5\6
					UNION ALL
					select district, channel, customer_code,customer_name,sales_value,sales_month,SUBSTR(sales_month,1,4) as sales_quarter,'PUTTY SALES' as sec_1,CONCAT('FULL YEAR ' , SUBSTR(sales_month,1,4)) as sec_2, null as sec_3,'PUTTY SALES' flag_q from others_y_net where  report_brand_name  = 'PUTTY' -- Putty sales -- Filler sales
					UNION ALL
					select district, channel, customer_code,customer_name,sales_value,sales_month,SUBSTR(sales_month,1,4) as sales_quarter,'SUNDRIES SALES' as sec_1,CONCAT('FULL YEAR ' , SUBSTR(sales_month,1,4)) as sec_2, null as sec_3,'SUNDRIES SALES' flag_q  from others_y_net where  report_brand_name = 'SUNDRIES' -- Putty sales
					UNION ALL
					select district, channel, customer_code,customer_name,sales_value,sales_month,sales_quarter,'Closed Order' as sec_1,CONCAT(SUBSTR(sales_month,1,4),' ',SUBSTR(sales_month,5,2)) as sec_2, null as sec_3,'Closed Order' flag_q from closed_sales  -- Closed Order
					UNION ALL
					select district, channel, customer_code,customer_name,sales_value,sales_month,sales_quarter,'Open Order' as sec_1,CONCAT(SUBSTR(sales_month,1,4),' ',SUBSTR(sales_month,5,2)) as sec_2, null as sec_3,'Open Order' flag_q  from open_sales  -- Open Order
					UNION ALL
					select district, channel, customer_code,customer_name,sales_value,sales_month,sales_quarter,'历史倒挂清账（不算当月销量）' as sec_1,CONCAT(SUBSTR(sales_month,1,4),' ',SUBSTR(sales_month,5,2)) as sec_2, null as sec_3,'否' flag_q   from mm_label_off where sec_1 = '补倒挂'   -- 冲抵倒挂
-- 					select district, channel, customer_code,offset_hanging,sales_month,sales_quarter,'历史倒挂清账（不算当月销量）' as sec_1,CONCAT(SUBSTR(sales_month,1,4),' ',SUBSTR(sales_month,5,2)) as sec_2, null as sec_3,'否' flag_q   from offsets  -- 冲抵倒挂
					UNION ALL
					select district, channel, customer_code,customer_name,sales_value,sales_month,sales_quarter,'主机厂带标签项目备货' as sec_1,CONCAT(SUBSTR(sales_month,1,4),' ',SUBSTR(sales_month,5,2),'项目备货') as sec_2, null as sec_3,'否' flag_q  from mm_label_off where sec_1 = '带标签'  -- 主机厂带标签项目备货
					UNION ALL
					select district, channel,vendor_code,vendor_name,shipment_sales,sales_month,sales_quarter,proj_name as sec_1,CONCAT(SUBSTR(sales_month,1,4),' ',SUBSTR(sales_month,5,2),'项目备货') as sec_2, null as sec_3,'否' flag_q  from mm_proj  -- 主机厂带标签项目备货 - 明细
					UNION ALL
					select district, channel,vendor_code,vendor_name,shipment_sales,sales_month,sales_quarter,'主机厂不带标签项目备货' as sec_1,CONCAT(SUBSTR(sales_month,1,4),' ',SUBSTR(sales_month,5,2),'项目备货') as sec_2, null as sec_3,'否' flag_q   from mm_nonlabel  -- 主机厂不带标签项目备货
					UNION ALL
					select district, channel,vendor_code,vendor_name,shipment_sales,sales_month,sales_quarter,'集团项目备货' as sec_1,CONCAT(SUBSTR(sales_month,1,4),' ',SUBSTR(sales_month,5,2),'项目备货') as sec_2, null as sec_3,'否' flag_q   from mso  -- 集团项目备货
					UNION ALL
					select district, channel,vendor_code,vendor_name,shipment_sales,sales_month,sales_quarter,proj_name as sec_1,CONCAT(SUBSTR(sales_month,1,4),' ',SUBSTR(sales_month,5,2),'项目备货') as sec_2, null as sec_3,'否' flag_q   from mso_proj  -- 集团项目备货 - 明细
					UNION ALL
					select district, channel, customer_code,customer_name,mtd_sales,sales_month,sales_quarter,CONCAT('MTD',' ',SUBSTR(sales_month,5,2)) as mtd,null, null,'mtd' flag_q from mtd  -- 45 MTD JAN Sales
					UNION ALL
					select district, channel,vendor_code,vendor_name,sales_value,sales_month,sales_quarter,report_brand_name,report_brand_group, null as sec_3,'是' flag_q from brand_net where  report_brand_name <> report_brand_group -- brand实际进货量 - 实际主机厂/集团备货扣减
					UNION ALL
					select district, channel,vendor_code,vendor_name,sales_value,sales_month,sales_quarter,report_brand_group,report_brand_group, null as sec_3,'是' flag_q from brand_net  where  report_brand_name = report_brand_group and report_brand_group not in( 'AQ+','EHP')
					UNION ALL
					select district, channel,vendor_code,vendor_name,sales_value,sales_month,sales_quarter,report_brand_group,report_brand_group, null as sec_3,'是' flag_q from brand_net  where  report_brand_group in( 'DIGITAL' ,'BELCO PLUS','AQ+','EHP')

					UNION ALL
					select district, channel, customer_code,customer_name,sales_value,sales_month,sales_quarter,report_brand_name,'BY CATEGORY AND BRAND' as sec_2,CONCAT('Fill ',SUBSTR(sales_quarter,5,2),' Sales Gap Vs. Target - ',DATE_FORMAT(STR_TO_DATE(CONCAT(sales_month, '01'), '%Y%m%d'), '%b')) as sec_3,'fill_gap_adj' flag_q from fill_gap_adj where  report_brand_name = report_brand_group and report_brand_group not in( 'AQ+','EHP') -- brand实际进货量 - 实际主机厂/集团备货扣减
					UNION ALL
					select district, channel, customer_code,customer_name,sales_value,sales_month,sales_quarter,report_brand_group,'BY CATEGORY AND BRAND' as sec_2,CONCAT('Fill ',SUBSTR(sales_quarter,5,2),' Sales Gap Vs. Target - ',DATE_FORMAT(STR_TO_DATE(CONCAT(sales_month, '01'), '%Y%m%d'), '%b')) as sec_3,'fill_gap_adj' flag_q from fill_gap_adj  where  report_brand_group in( 'DIGITAL' ,'BELCO PLUS','AQ+','EHP') -- brand实际进货量 - 实际主机厂/集团备货扣减
					UNION ALL
					select district, channel, customer_code,customer_name,sales_value,sales_month,sales_quarter,report_brand_name,'BY CATEGORY AND BRAND' as sec_2,CONCAT('Fill ',SUBSTR(sales_quarter,5,2),' Sales Gap Vs. Target - ',DATE_FORMAT(STR_TO_DATE(CONCAT(sales_month, '01'), '%Y%m%d'), '%b')) as sec_3,'fill_gap_adj_open' flag_q from fill_gap_adj_open where  report_brand_name = report_brand_group and report_brand_group not in( 'AQ+','EHP')  -- brand实际进货量 - 实际主机厂/集团备货扣减
					UNION ALL
					select district, channel, customer_code,customer_name,sales_value,sales_month,sales_quarter,report_brand_group,'BY CATEGORY AND BRAND' as sec_2,CONCAT('Fill ',SUBSTR(sales_quarter,5,2),' Sales Gap Vs. Target - ',DATE_FORMAT(STR_TO_DATE(CONCAT(sales_month, '01'), '%Y%m%d'), '%b')) as sec_3,'fill_gap_adj_open' flag_q from fill_gap_adj_open  where  report_brand_group in( 'DIGITAL' ,'BELCO PLUS','AQ+','EHP') -- brand实际进货量 - 实际主机厂/集团备货扣减

					UNION ALL
					select district, channel, customer_code,customer_name,sales_value,sales_month,sales_quarter,report_brand_name,'BY CATEGORY AND BRAND' as sec_2,CONCAT('Fill ',SUBSTR(sales_quarter,5,2),' Sales Gap Vs. Target - ',DATE_FORMAT(STR_TO_DATE(CONCAT(sales_month, '01'), '%Y%m%d'), '%b')) as sec_3,'fill_gap_adj_q' flag_q from fill_gap_adj_q where  report_brand_name = report_brand_group and report_brand_group not in( 'AQ+','EHP') -- brand实际进货量 - 实际主机厂/集团备货扣减
					UNION ALL
					select district, channel, customer_code,customer_name,sales_value,sales_month,sales_quarter,report_brand_group,'BY CATEGORY AND BRAND' as sec_2,CONCAT('Fill ',SUBSTR(sales_quarter,5,2),' Sales Gap Vs. Target - ',DATE_FORMAT(STR_TO_DATE(CONCAT(sales_month, '01'), '%Y%m%d'), '%b')) as sec_3,'fill_gap_adj_q' flag_q from fill_gap_adj_q  where  report_brand_group in( 'DIGITAL' ,'BELCO PLUS','AQ+','EHP') -- brand实际进货量 - 实际主机厂/集团备货扣减
					UNION ALL
					select district, channel, customer_code,customer_name,sales_value,sales_month,sales_quarter,report_brand_name,'BY CATEGORY AND BRAND' as sec_2,CONCAT('Fill ',SUBSTR(sales_quarter,5,2),' Sales Gap Vs. Target - ',DATE_FORMAT(STR_TO_DATE(CONCAT(sales_month, '01'), '%Y%m%d'), '%b')) as sec_3,'fill_gap_adj_open_q' flag_q from fill_gap_adj_open_q where  report_brand_name = report_brand_group and report_brand_group not in( 'AQ+','EHP')  -- brand实际进货量 - 实际主机厂/集团备货扣减
					UNION ALL
					select district, channel, customer_code,customer_name,sales_value,sales_month,sales_quarter,report_brand_group,'BY CATEGORY AND BRAND' as sec_2,CONCAT('Fill ',SUBSTR(sales_quarter,5,2),' Sales Gap Vs. Target - ',DATE_FORMAT(STR_TO_DATE(CONCAT(sales_month, '01'), '%Y%m%d'), '%b')) as sec_3,'fill_gap_adj_open_q' flag_q from fill_gap_adj_open_q  where  report_brand_group in( 'DIGITAL' ,'BELCO PLUS','AQ+','EHP') -- brand实际进货量 - 实际主机厂/集团备货扣减


				)t
					on cal.sales_month = t.sales_month
-- 					LEFT JOIN fine_dw.dw_order_report orderno
-- 					on orderno.name_1 = t.sec_1
-- 					and orderno.order_month = t.sales_month
-- 					and orderno.order_report = 'distributor_sales_report'
					-- LEFT JOIN (SELECT DISTINCT district,u_customer_code,u_customer_name,sales_month FROM temp_dw_customer_master_list ) m
					-- on t.customer_code = m.u_customer_code
					-- and t.sales_month = m.sales_month
					-- and case when LENGTH(t.sales_month)=4 then t.sales_month >= SUBSTR(starting_date,1,4) and  t.sales_month <= SUBSTR(ending_date,1,4)
					-- else STR_TO_DATE(m.starting_date, '%Y%m%d') <= STR_TO_DATE(CONCAT(t.sales_month,'01'), '%Y%m%d')
					-- and STR_TO_DATE(m.ending_date, '%Y%m%d') >= STR_TO_DATE(CONCAT(t.sales_month,'01'), '%Y%m%d') end
					-- LEFT JOIN fine_dw.dw_cs_relationship_info cs
					-- on t.customer_code = cs.customer_code
          			-- and t.district = cs.district
					-- and ${mysql_yesterday_l_year} = cs.s_year
					-- and cs.channel = 'DISTRIBUTOR' 
)  -- SELECT * FROM month_actual where customer_name is null;
,quarter_actual as(

-- --  BY CATEGORY AND BRAND -- Q1 ACTUAL  -- Q1 ACTUAL
SELECT customer_code,customer_name,channel,district,sec_1,'BY CATEGORY AND BRAND' as sec_2,CONCAT(SUBSTR(sales_quarter,5,2),' ACTUAL') as sec_3,name_1,sum(sales_value) as sales_value,sales_quarter as sales_month,sales_quarter,sales_year,null flag_q FROM month_actual WHERE flag_q in ('是','fill_gap_adj_q','fill_gap_adj_open_q') 
and DATE_FORMAT(NOW(), '%Y%m')  <> sales_month
GROUP BY customer_code,customer_name,channel,district,sec_1,name_1,sales_quarter,sales_year
-- UNION ALL
-- SELECT customer_code,customer_name,channel,district,sec_1,sec_2,sec_3,name_1,sum(sales_value) as sales_value,sales_quarter as sales_month,sales_quarter,sales_year,flag_q FROM month_actual WHERE flag_q = '是' GROUP BY customer_code,customer_name,channel,district,sec_1,sec_2,sec_3,name_1,sales_quarter,sales_year,flag_q
UNION ALL -- Q1 ACTUAL Total 
SELECT customer_code,customer_name,channel,district,CONCAT(SUBSTR(sales_quarter,5,2),'Total') as sec_1,'BY CATEGORY AND BRAND' as sec_2,CONCAT(SUBSTR(sales_quarter,5,2),' ACTUAL') as sec_3,CONCAT(SUBSTR(sales_quarter,5,2),'Total') as name_1,sum(sales_value) as sales_value,sales_quarter as sales_month,sales_quarter,sales_year,null flag_q FROM month_actual WHERE 1=1 
and  flag_q in ('mtd','fill_gap_adj_q','fill_gap_adj_open_q')  
and DATE_FORMAT(NOW(), '%Y%m')  <> sales_month
GROUP BY customer_code,customer_name,channel,district,CONCAT(SUBSTR(sales_quarter,5,2),'Total'),CONCAT(SUBSTR(sales_quarter,5,2),'Total'),CONCAT(SUBSTR(sales_quarter,5,2),' ACTUAL'),sales_quarter,sales_year
UNION ALL -- Q1 Adjusted Total
SELECT customer_code,customer_name,channel,district,CONCAT(SUBSTR(sales_quarter,5,2),' Adjusted Total') as sec_1,'BY CATEGORY AND BRAND' as sec_2,CONCAT(SUBSTR(sales_quarter,5,2),' Adjusted (Incl. Fill ',SUBSTR(sales_quarter,5,2),' Sales Gap Vs. Target)') as sec_3,CONCAT(SUBSTR(sales_quarter,5,2),'Total') as name_1,sum(sales_value) as sales_value,sales_quarter as sales_month,sales_quarter,sales_year,flag_q FROM month_actual 
WHERE 1=1 and flag_q in ('mtd','fill_gap_adj') 
and DATE_FORMAT(NOW(), '%Y%m')  <> sales_month
GROUP BY customer_code,customer_name,channel,district,CONCAT(SUBSTR(sales_quarter,5,2),'Total'),CONCAT(SUBSTR(sales_quarter,5,2),'Total'),CONCAT(SUBSTR(sales_quarter,5,2),' ACTUAL'),sales_quarter,sales_year,flag_q
UNION ALL -- Q1 Adjusted (Incl. Fill Q1 Sales Gap Vs. Target)
SELECT customer_code,customer_name,channel,district,sec_1,'BY CATEGORY AND BRAND' as sec_2,CONCAT(SUBSTR(sales_quarter,5,2),' Adjusted (Incl. Fill ',SUBSTR(sales_quarter,5,2),' Sales Gap Vs. Target)') as sec_3,name_1,sum(sales_value) as sales_value,sales_quarter as sales_month,sales_quarter,sales_year,flag_q FROM month_actual 
WHERE 1=1 and flag_q in ('fill_gap_adj' ,'是') 
and DATE_FORMAT(NOW(), '%Y%m')  <> sales_month
GROUP BY customer_code,customer_name,channel,district,sec_1,sec_2,sec_3,name_1,sales_quarter,sales_year,flag_q
UNION ALL -- Closed Order -- Fill Q1 Sales Gap Vs. Target - APR
SELECT customer_code,customer_name,channel,district,'Closed Order' as sec_1,'BY CATEGORY AND BRAND' as sec_2,sec_3,name_1,sum(sales_value) as sales_value,sales_quarter as sales_month,sales_quarter,sales_year,flag_q FROM month_actual WHERE flag_q = 'fill_gap_adj' GROUP BY customer_code,customer_name,channel,district,sec_1,sec_2,sec_3,name_1,sales_quarter,sales_year,flag_q
UNION ALL -- Open Order -- Fill Q1 Sales Gap Vs. Target - APR
SELECT customer_code,customer_name,channel,district,'Open Order' as sec_1,'BY CATEGORY AND BRAND' as sec_2,sec_3,name_1,sum(sales_value) as sales_value,sales_quarter as sales_month,sales_quarter,sales_year,flag_q FROM month_actual WHERE flag_q = 'fill_gap_adj_open' GROUP BY customer_code,customer_name,channel,district,sec_1,sec_2,sec_3,name_1,sales_quarter,sales_year,flag_q
UNION ALL -- Total Sales Order(Fill Q1 Sales Gap Vs. Target) -- Fill Q1 Sales Gap Vs. Target - APR
SELECT customer_code,customer_name,channel,district,CONCAT('Total Sales Order(Fill ',SUBSTR(sales_quarter,5,2),' Sales Gap Vs. Target)') as sec_1,'BY CATEGORY AND BRAND' as sec_2,sec_3,name_1,sum(sales_value) as sales_value,sales_quarter as sales_month,sales_quarter,sales_year,flag_q FROM month_actual WHERE flag_q in ('fill_gap_adj' ,'fill_gap_adj_open') GROUP BY customer_code,customer_name,channel,district,sec_1,sec_2,sec_3,name_1,sales_quarter,sales_year,flag_q
-- UNION ALL -- Fill Q1 Sales Gap Vs. Target - APR
-- select district, channel, customer_code,sales_value,sales_quarter,sales_quarter,report_brand_name,'BY CATEGORY AND BRAND' as sec_2,CONCAT('Fill ',SUBSTR(sales_quarter,5,2),' Sales Gap Vs. Target - ',DATE_FORMAT(STR_TO_DATE(CONCAT(sales_month, '01'), '%Y%m%d'), '%b')) as sec_3,'fill_gap_adj' flag_q from fill_gap_adj where  report_brand_name = report_brand_group  -- brand实际进货量 - 实际主机厂/集团备货扣减
-- UNION ALL
-- select district, channel, customer_code,sales_value,sales_quarter,sales_quarter,report_brand_group,'BY CATEGORY AND BRAND' as sec_2,CONCAT('Fill ',SUBSTR(sales_quarter,5,2),' Sales Gap Vs. Target - ',DATE_FORMAT(STR_TO_DATE(CONCAT(sales_month, '01'), '%Y%m%d'), '%b')) as sec_3,'fill_gap_adj' flag_q from fill_gap_adj  where  report_brand_group in( 'DIGITAL' ,'BELCO PLUS') -- brand实际进货量 - 实际主机厂/集团备货扣减

UNION ALL
select customer_code,customer_name,channel,district,sec_1,'BY CATEGORY AND BRAND' as sec_2,CONCAT('Fill ',SUBSTR(sales_quarter,5,2),' Sales Gap Vs. Target - ',DATE_FORMAT(STR_TO_DATE(CONCAT(sales_month, '01'), '%Y%m%d'), '%b')) as sec_3,name_1,sum(sales_value) as sales_value,sales_quarter as sales_month,sales_quarter,sales_year,flag_q FROM month_actual WHERE flag_q = 'fill_gap_adj' -- where  report_brand_name = report_brand_group  -- brand实际进货量 - 实际主机厂/集团备货扣减
GROUP By customer_code,customer_name,channel,district,sec_1,CONCAT('Fill ',SUBSTR(sales_quarter,5,2),' Sales Gap Vs. Target - ',DATE_FORMAT(STR_TO_DATE(CONCAT(sales_month, '01'), '%Y%m%d'), '%b')),name_1,sales_quarter,sales_year,flag_q
UNION ALL
select customer_code,customer_name,channel,district,sec_1,'BY CATEGORY AND BRAND' as sec_2,CONCAT('Fill ',SUBSTR(sales_quarter,5,2),' Sales Gap Vs. Target - ',DATE_FORMAT(STR_TO_DATE(CONCAT(sales_month, '01'), '%Y%m%d'), '%b')) as sec_3,name_1,sum(sales_value) as sales_value,sales_quarter as sales_month,sales_quarter,sales_year,flag_q FROM month_actual where flag_q = 'fill_gap_adj_open' -- where report_brand_name = report_brand_group  -- brand实际进货量 - 实际主机厂/集团备货扣减
GROUP By customer_code,customer_name,channel,district,sec_1,CONCAT('Fill ',SUBSTR(sales_quarter,5,2),' Sales Gap Vs. Target - ',DATE_FORMAT(STR_TO_DATE(CONCAT(sales_month, '01'), '%Y%m%d'), '%b')),name_1,sales_quarter,sales_year,flag_q




				

)
-- 	SELECT sum(sales_value) as sales_value,sales_month as sales_month
-- 	FROM month_actual 
-- 	WHERE flag_q in ('是','fill_gap_adj_q','fill_gap_adj_open_q') 
-- 	and sec_1 = '2K'
-- 	GROUP BY sales_month
-- 	;
,year_actual as(
	SELECT customer_code,customer_name,channel,district,sec_1,'实际进货量 - 实际主机厂/集团备货扣减' as sec_2,CONCAT(' FULL YEAR ',sales_year) as sec_3,name_1,sum(sales_value) as sales_value,sales_year as sales_month,sales_year as sales_quarter,sales_year,flag_q FROM month_actual WHERE flag_q in ('是','fill_gap_adj_q','fill_gap_adj_open_q') 
	and DATE_FORMAT(NOW(), '%Y%m')  <> sales_month
	GROUP BY customer_code,customer_name,channel,district,sec_1,name_1,sales_year,flag_q
	UNION ALL
	SELECT customer_code,customer_name,channel,district,CONCAT(sales_year,' Total') as sec_1,'实际进货量 - 实际主机厂/集团备货扣减' as sec_2,CONCAT(' FULL YEAR ',sales_year) as sec_3,CONCAT(sales_year,' Total') as name_1,sum(sales_value) as sales_value,sales_year as sales_month,sales_year as sales_quarter,sales_year,flag_q FROM month_actual WHERE flag_q in ('mtd','fill_gap_adj_q','fill_gap_adj_open_q')
  and DATE_FORMAT(NOW(), '%Y%m')  <> sales_month
	GROUP BY customer_code,customer_name,channel,district,sales_year,flag_q

	UNION ALL
	select customer_code,customer_name,channel,district,sec_1,sec_2,sec_3,name_1,sum(sales_value) as sales_value,sales_year as sales_month,sales_year as sales_quarter,sales_year,flag_q FROM month_actual WHERE flag_q ='prior'  -- total sales FULL YEAR 
	GROUP BY customer_code,customer_name,channel,district,sec_1,sec_2,sec_3,name_1,sales_year,flag_q
	UNION ALL
	select customer_code,customer_name,channel,district,sec_1,sec_2,sec_3,name_1,sum(sales_value) as sales_value,sales_year as sales_month,sales_year as sales_quarter,sales_year,flag_q FROM month_actual WHERE flag_q ='PUTTY SALES' or flag_q = 'SUNDRIES SALES'  -- total sales FULL YEAR 
	GROUP BY customer_code,customer_name,channel,district,sec_1,sec_2,sec_3,name_1,sales_year,flag_q
	

)

-- SELECT DISTINCT order_no from fine_dw.dw_order_report tt where not EXISTS (select 1 FROM(
	SELECT 
-- 	sum(sales_value),
-- orderno.order_no,
-- orderno.sec_1
-- orderno.sec_2,
-- orderno.sec_3
-- 	DISTINCT 					orderno.order_no
					t.customer_code,
					t.customer_name,
					t.channel,
					t.district,
					t.name_1,
					cs.team_owner,
					cs.sales_person,
					cs.team_owner_id,
					cs.sales_person_id,
					t.sales_value,
					t.sales_month,
					t.sales_quarter,
					t.sales_year,
					orderno.order_no,
					orderno.sec_1,
					orderno.sec_2,
					orderno.sec_3,
					orderno.sec_33,
					orderno.report_year,
					orderno.is_flag,
					'fine_dw.dw_transaction_detail_report/fine_dw.dw_distributor_sales_target/temp_002/finedw.dw_backlog_by_product_line/fine_dw.dw_customer_master_lis/fine_dw.dw_cs_relationship_info/fine_dw.dw_order_report/fine_ods.ods_calendar_info_df' as data_resource,
					 now() as etl_time,
					 STR_TO_DATE(CONCAT( orderno.report_year,'0101') , '%Y%m%d') as report_date
					
				
			FROM
				(
					SELECT * FROM month_actual WHERE flag_q <> 'fill_gap_adj' and flag_q <> 'fill_gap_adj_open' 
					and flag_q <> 'fill_gap_adj_q' and flag_q <> 'fill_gap_adj_open_q'
					UNION ALL
					SELECT * FROM quarter_actual
					UNION ALL
					SELECT * FROM year_actual
					) t
					LEFT JOIN 
					(select * 
					            from  fine_dw.dw_order_report orderno
								where  orderno.order_report = 'distributor_sales_report' 
								 and  orderno.report_year =   ${mysql_yesterday_d_year} -- 具体报表的年
					) orderno
					on upper(orderno.name_1) = upper(t.sec_1)
					and orderno.order_month = t.sales_month
					-- and orderno.order_report = 'distributor_sales_report'
					and case when SUBSTR(t.sales_month,5,1)= 'Q' then upper(orderno.sec_2) = upper(t.sec_2) and upper(orderno.sec_3) = upper(t.sec_3) else 1=1 end
					LEFT JOIN fine_dw.dw_cs_relationship_info cs
					on t.customer_code = cs.customer_code
          			and t.district = cs.district
					and ${mysql_yesterday_d_year}  = cs.s_year
					and cs.channel = 'DISTRIBUTOR' 

					where 1=1
					and orderno.order_no is not null
					and t.sales_value <> 0
					and t.customer_code is not null
-- 					and t.customer_name is null
-- 					and t.customer_name is null
					-- and t.sales_month = '202104'
-- 					and t.customer_code = '36222'
-- 					and orderno.order_no = 1
-- 					GROUP BY orderno.order_no,orderno.sec_1

					order by orderno.order_no