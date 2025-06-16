/*目标表：dm_school_report
  来源表：

fine_dw.dw_customer_master_list
fine_dw.dw_transaction_detail_report
fine_dw.dw_school_sales_target
fine_dw.dw_transaction_detail_report
fine_dw.dw_order_report

fine_ods.ods_calendar_info_df

更新方式：增量更新
更新粒度：年
更新字段：report_year

*/


-- 128 6630
with cal as 
(
	SELECT 
		DISTINCT
        cal.actual_month sales_month,
        DATE_FORMAT(STR_TO_DATE(CONCAT(cal.actual_month, '01'), '%Y%m%d'), '%b') sales_month_en,
				cal.actual_quarter sales_quarter,
				cal.actual_year sales_year,
				master1.u_customer_code as customer_code,
				master1.u_customer_name as customer_name
    FROM fine_ods.ods_calendar_info_df cal left join 
		(
		SELECT DISTINCT channel,u_customer_code,u_customer_name,starting_date,ending_date FROM fine_dw.dw_customer_master_list where channel = 'SCHOOL' 
		UNION
		SELECT DISTINCT channel,customer_code,customer_name,starting_date,ending_date FROM fine_dw.dw_ship_to_list where channel = 'SCHOOL' 
		)
master1
		on 1=1
		and STR_TO_DATE(master1.starting_date, '%Y%m%d') <= STR_TO_DATE(CONCAT(actual_month,'01'), '%Y%m%d')
		and STR_TO_DATE(master1.ending_date, '%Y%m%d') >= STR_TO_DATE(CONCAT(actual_month,'01'), '%Y%m%d')
		where master1.channel = 'SCHOOL' 
	 and actual_year in( ${mysql_yesterday_d_year},${mysql_yesterday_l_year}) -- 需要同环比
		
		

), temp_dw_transaction_detail_report as(
		select 
		t.channel,
		t.customer_code,
		t.report_brand_name,
		-- t.customer_name,
		sum(t.sales_value) as sales_value,
		t.sales_month,
		c.actual_quarter sales_quarter,
		SUBSTR(t.sales_month,1,4) sales_year
		FROM
		fine_dw.dw_transaction_detail_report t
		LEFT JOIN fine_ods.ods_calendar_info_df c
		on t.sales_month = c.actual_month
		WHERE t.channel = 'SCHOOL'
		GROUP BY
		t.sales_month,
		t.channel,
		t.customer_code,
		t.report_brand_name,
		-- t.customer_name,
		SUBSTR(t.sales_month,1,4) ,
		c.actual_quarter
		-- CONCAT('Y',SUBSTR(t.sales_month,3,2),' ',SUBSTR(t.sales_month,5,2))
		UNION ALL
		select 
		t.channel,
		t.customer_code,
		t.report_brand_name,
		-- t.customer_name,
		sum(t.sales_value) as sales_value,
		t.sales_month,
		c.actual_quarter sales_quarter,
		SUBSTR(t.sales_month,1,4) sales_year
		FROM
		fine_dw.dw_backlog_by_product_line t
		LEFT JOIN fine_ods.ods_calendar_info_df c
		on t.sales_month = c.actual_month
		WHERE t.channel = 'SCHOOL'
		GROUP BY
		t.sales_month,
		t.channel,
		t.customer_code,
		t.report_brand_name,
		-- t.customer_name,
		SUBSTR(t.sales_month,1,4) ,
		c.actual_quarter



)
,actual as
(
	SELECT 
		cal.customer_code,
		cal.customer_name,
		sum(p.sales_value) as sales_value,
		cal.sales_month,
		cal.sales_quarter,
		cal.sales_year,
		UPPER(sales_month_en) sec_1-- FY24 01 FY24 ACTUAL
	
	FROM cal
	LEFT JOIN
	(
		select 
		t.customer_code,
		-- t.customer_name,
		sum(t.sales_value) as sales_value,
		t.sales_month,
		c.actual_quarter sales_quarter,
		SUBSTR(t.sales_month,1,4) sales_year
		FROM
		temp_dw_transaction_detail_report t
		LEFT JOIN fine_ods.ods_calendar_info_df c
		on t.sales_month = c.actual_month
		WHERE t.channel = 'SCHOOL'
		GROUP BY
		t.sales_month,
		t.customer_code,
		-- t.customer_name,
		SUBSTR(t.sales_month,1,4) ,
		c.actual_quarter
		-- CONCAT('Y',SUBSTR(t.sales_month,3,2),' ',SUBSTR(t.sales_month,5,2))
	)p
	on cal.customer_code = p.customer_code
	and cal.sales_month = p.sales_month
	GROUP BY
		cal.customer_code,
		cal.customer_name,
		cal.sales_month,
		cal.sales_quarter,
		cal.sales_year,
		UPPER(sales_month_en)

), actual_q as (

	SELECT
		customer_code,
		customer_name,
		sum(sales_value) as sales_value,
		sales_quarter,
		sales_year,
		SUBSTR(sales_quarter,5,2) sec_1-- FY24 01 FY24 ACTUAL
	FROM actual
	GROUP BY 
	customer_code,
	customer_name,
	sales_quarter,
	sales_year,
	SUBSTR(sales_quarter,5,2)

), actual_y as (

	SELECT
		customer_code,
		customer_name,
		sum(sales_value) as sales_value,
		sales_year,
		'FY' sec_1-- FY24 01 FY24 ACTUAL
	FROM actual
	GROUP BY 
	customer_code,
	customer_name,
	sales_year

)
, target as(
	SELECT 
		cal.customer_code,
		cal.customer_name,
		sum(p.sales_target) sales_target,
		cal.sales_month as target_month,
		cal.sales_quarter as target_quarter,
		cal.sales_year as target_year,
		sec_1-- FY24 01 FY24 ACTUAL
	
	FROM cal
	LEFT JOIN
	(
		SELECT
		customer_code,
		sales_target,
		target_month,
		target_quarter,
		target_year,
		'TARGET' sec_1
		FROM fine_dw.dw_school_sales_target
	)p
	on cal.customer_code = p.customer_code
	and cal.sales_month = p.target_month
	GROUP by 
		cal.customer_code,
		cal.customer_name,
		cal.sales_month,
		cal.sales_quarter,
		cal.sales_year
)
, target_q as (

	SELECT
		customer_code,
		customer_name,
		sum(sales_target) as sales_target,
		target_quarter,
		target_year,
		sec_1
	FROM target
	GROUP BY 
	customer_code,
	customer_name,
	target_quarter,
	target_year,
	sec_1
)
, target_y as (

	SELECT
		customer_code,
		customer_name,
		sum(sales_target) as sales_target,
		target_year,
		sec_1
	FROM target
	GROUP BY 
	customer_code,
	customer_name,
	target_year,
	sec_1
)
, vs_target as-- VS TARGET
(
SELECT
	actual.customer_code,
	actual.customer_name,
	(actual.sales_value-target.sales_target) as sales_value1,
	target.sales_target as sales_value2,
	CASE 
        WHEN target.sales_target = 0 THEN NULL
        ELSE (actual.sales_value-target.sales_target) / NULLIF(target.sales_target, 0)
    END AS  vs_target_rate,
	actual.sales_month,
	actual.sales_quarter,
	actual.sales_year,
	'VS TARGET' sec_1
	FROM actual
	LEFT JOIN target
	on actual.customer_code = target.customer_code
	and actual.sales_month = target.target_month
), vs_target_q as-- VS TARGET
(
SELECT
	actual.customer_code,
	actual.customer_name,
	CASE 
        WHEN sum(actual.sales_value2) = 0 THEN NULL
        ELSE sum(actual.sales_value1) / NULLIF(sum(actual.sales_value2), 0)
    END AS  vs_target_rate,
	actual.sales_quarter,
	actual.sales_year,
	sec_1
	FROM vs_target actual
	GROUP BY 
	actual.customer_code,
	actual.customer_name,
	actual.sales_quarter,
	actual.sales_year,
	sec_1
)
, vs_target_y as-- VS TARGET%
(
SELECT
	actual.customer_code,
	actual.customer_name,
	CASE 
        WHEN sum(actual.sales_value2) = 0 THEN NULL
        ELSE sum(actual.sales_value1) / NULLIF(sum(actual.sales_value2), 0)
    END AS  vs_target_rate,
	actual.sales_year,
	sec_1
	FROM vs_target actual
	GROUP BY 
	actual.customer_code,
	actual.customer_name,
	actual.sales_year,
	sec_1
	)
, actual_prior as -- FY23 ACTUAL
(
	SELECT
		actual.customer_code,
		actual.customer_name,
	  	sum(prior.sales_value)  as actual_prior_sales,
		DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(actual.sales_month,'01'), '%Y%m%d'), INTERVAL 1 year), '%Y%m') as sales_month,
	    CONCAT(actual.sales_year-1,SUBSTR(actual.sales_quarter,5,2)) sales_quarter,
	    actual.sales_year-1 as sales_year,
		'PRIOR'  sec_1
	FROM actual
	LEFT JOIN actual prior
	on actual.customer_code = prior.customer_code 
	and DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(actual.sales_month,'01'), '%Y%m%d'), INTERVAL 1 year), '%Y%m') = DATE_FORMAT(STR_TO_DATE(CONCAT(prior.sales_month,'01'), '%Y%m%d'), '%Y%m') 
	GROUP by 
		actual.customer_code,
		actual.customer_name,
		actual.sales_month,
	  	actual.sales_quarter,
	  	actual.sales_year
	
)
,actual_prior_q as
(
	SELECT 
		customer_code,
		customer_name,
		sum(actual_prior_sales) as actual_prior_sales,
	  	sales_quarter,
	  	sales_year,
        sec_1

	FROM actual_prior
	GROUP BY 
		customer_code,
		customer_name,
	  	sales_quarter,
	  	sales_year ,
        sec_1

)
,actual_prior_y as
(
	SELECT 
		customer_code,
		customer_name,
		sum(actual_prior_sales) as actual_prior_sales,
	  	sales_quarter,
	  	sales_year,
        sec_1

	FROM actual_prior
	GROUP BY 
		customer_code,
		customer_name,
	  	sales_quarter,
	  	sales_year ,
        sec_1

)
, growth as -- YOY GROWTH
(

		SELECT
			actual.customer_code,
			actual.customer_name,
			(actual.sales_value-prior.actual_prior_sales) as sales_value1,
			prior.actual_prior_sales as sales_value2,
			(actual.sales_value-prior.actual_prior_sales)/prior.actual_prior_sales as growth_rate,
			actual.sales_month,
			actual.sales_quarter,
			actual.sales_year,
			'YOY GROWTH' sec_1
			FROM actual
			LEFT JOIN actual_prior prior
			on actual.customer_code = prior.customer_code
			-- and actual.sales_month = actual_prior.sales_month
      and DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(actual.sales_month,'01'), '%Y%m%d'), INTERVAL 1 year), '%Y%m') = DATE_FORMAT(STR_TO_DATE(CONCAT(prior.sales_month,'01'), '%Y%m%d'), '%Y%m') 

), growth_q as(

		SELECT
	        actual.customer_code,
	        actual.customer_name,
			sum(sales_value1)/sum(sales_value2) as growth_rate,
			actual.sales_quarter,
			actual.sales_year,
			sec_1
			FROM growth actual
			GROUP BY 
	        actual.customer_code,
	        actual.customer_name,
			actual.sales_quarter,
			actual.sales_year,
			sec_1
), growth_y as(

		SELECT
	        actual.customer_code,
	        actual.customer_name,
			sum(sales_value1)/sum(sales_value2) as growth_rate,
			actual.sales_year,
			sec_1
			FROM growth actual
			GROUP BY 
	        actual.customer_code,
	        actual.customer_name,
			actual.sales_year,
			sec_1
)
, sundries as -- SUNDRIES PUTTY
(
	SELECT 
		cal.customer_code,
		cal.customer_name,
		p.sales_value,
		cal.sales_month,
		cal.sales_quarter,
		cal.sales_year,
		'SUNDRIES' sec_1-- FY24 ACTUAL
	
	FROM cal
	LEFT JOIN
	(
	select 
		t.customer_code,
		-- t.customer_name,
		sum(t.sales_value) as sales_value,
		t.sales_month,
		c.actual_quarter sales_quarter,
		SUBSTR(t.sales_month,1,4) sales_year
		FROM
		temp_dw_transaction_detail_report t
		LEFT JOIN fine_ods.ods_calendar_info_df c
		on t.sales_month = c.actual_month
		WHERE t.channel = 'SCHOOL'
		and report_brand_name = 'SUNDRIES'
		GROUP BY
		t.sales_month,
		t.customer_code,
		-- t.customer_name,
		SUBSTR(t.sales_month,1,4) ,
		c.actual_quarter
	)p
	on cal.customer_code = p.customer_code
	and cal.sales_month = p.sales_month
	)
	, putty as -- SUNDRIES PUTTY
	(
	SELECT 
		cal.customer_code,
		cal.customer_name,
		p.sales_value,
		cal.sales_month,
		cal.sales_quarter,
		cal.sales_year,
		'PUTTY' sec_1-- FY24 ACTUAL
	
	FROM cal
	LEFT JOIN
	(
	select 
		t.customer_code,
		-- t.customer_name,
		sum(t.sales_value) as sales_value,
		t.sales_month,
		c.actual_quarter sales_quarter,
		SUBSTR(t.sales_month,1,4) sales_year,
		'PUTTY' sec_1-- FY24 ACTUAL
		FROM
		temp_dw_transaction_detail_report t
		LEFT JOIN fine_ods.ods_calendar_info_df c
		on t.sales_month = c.actual_month
		WHERE t.channel = 'SCHOOL'
		and report_brand_name = 'PUTTY'
		GROUP BY
		t.sales_month,
		t.customer_code,
		-- t.customer_name,
		SUBSTR(t.sales_month,1,4) ,
		c.actual_quarter
	)p
	on cal.customer_code = p.customer_code
	and cal.sales_month = p.sales_month
), sundries_q as(
SELECT
		sundries.customer_code,
		sundries.customer_name,
		sum(sundries.sales_value) as sales_value,
		sundries.sales_quarter as sales_month,
		sundries.sales_quarter,
		sundries.sales_year,
		'SUNDRIES' sec_1-- FY24 ACTUAL
FROM sundries
GROUP BY 
		sundries.customer_code,
		sundries.customer_name,
		sundries.sales_quarter,
		sundries.sales_year
)
,sundries_y as(
SELECT
		sundries.customer_code,
		sundries.customer_name,
		sum(sundries.sales_value) as sales_value,
		sundries.sales_year,
		'SUNDRIES' sec_1-- FY24 ACTUAL
FROM sundries
GROUP BY 
		sundries.customer_code,
		sundries.customer_name,
		sundries.sales_year
)
,putty_q as(
SELECT
		putty.customer_code,
		putty.customer_name,
		sum(putty.sales_value) as sales_value,
		putty.sales_quarter as sales_month,
		putty.sales_quarter,
		putty.sales_year,
		'PUTTY' sec_1-- FY24 ACTUAL
FROM putty
GROUP BY 
		putty.customer_code,
		putty.customer_name,
		putty.sales_quarter,
		putty.sales_year
)
,putty_y as(
SELECT
		putty.customer_code,
		putty.customer_name,
		sum(putty.sales_value) as sales_value,
		putty.sales_year,
		'PUTTY' sec_1-- FY24 ACTUAL
FROM putty
GROUP BY 
		putty.customer_code,
		putty.customer_name,
		putty.sales_year
)

	SELECT  
-- 	DISTINCT order_no
		t.customer_name,
		t.customer_code,
		'SCHOOL' channel,
		case when orderno.sec_1 in ('VS TARGET%','YOY GROWTH%') then t.sales_value*100 else t.sales_value end as sales_value,
		t.sales_month,
		t.sales_quarter,
		t.sales_year,
		orderno.order_no,
		orderno.sec_1,
		orderno.sec_2,
		orderno.sec_3,
        orderno.report_year,
        STR_TO_DATE(CONCAT( orderno.report_year,'0101') , '%Y%m%d') as report_date,
		'fine_dw.dw_customer_master_list,fine_dw,dw_transaction_detail_report,fine_dw.dw_school_sales_target,fine_dw.dw_transaction_detail_report,fine_dw.dw_backlog_by_product_line,fine_dw.dw_order_report' as data_resource,
		now() as etl_time

FROM
(
		SELECT customer_name,customer_code,sales_value,sales_month,sales_quarter,sales_year,sec_1 FROM actual
		UNION ALL
		SELECT customer_name,customer_code,sales_target,target_month,target_quarter,target_year,sec_1 FROM target
		UNION ALL
		SELECT customer_name,customer_code,vs_target_rate,sales_month,sales_quarter,sales_year,sec_1 FROM vs_target
		UNION ALL
		SELECT customer_name,customer_code,actual_prior_sales,sales_month,sales_quarter,sales_year,sec_1 FROM actual_prior
		UNION ALL
		SELECT customer_name,customer_code,growth_rate,sales_month,sales_quarter,sales_year,sec_1 FROM growth
		UNION ALL
		SELECT customer_name,customer_code,sales_value,sales_month,sales_quarter,sales_year,sec_1 FROM sundries
		UNION ALL
		SELECT customer_name,customer_code,sales_value,sales_month,sales_quarter,sales_year,sec_1 FROM putty

		UNION ALL
		SELECT customer_name,customer_code,sales_value,sales_quarter,sales_quarter,sales_year,sec_1 FROM actual_q
		UNION ALL
		SELECT customer_name,customer_code,sales_target,target_quarter,target_quarter,target_year,sec_1 FROM target_q
		UNION ALL
		SELECT customer_name,customer_code,vs_target_rate,sales_quarter,sales_quarter,sales_year,sec_1 FROM vs_target_q
		UNION ALL
		SELECT customer_name,customer_code,actual_prior_sales,sales_quarter,sales_quarter,sales_year,sec_1 FROM actual_prior_q
		UNION ALL
		SELECT customer_name,customer_code,growth_rate,sales_quarter,sales_quarter,sales_year,sec_1 FROM growth_q
		UNION ALL
		SELECT customer_name,customer_code,sales_value,sales_quarter,sales_quarter,sales_year,sec_1 FROM sundries_q
		UNION ALL
		SELECT customer_name,customer_code,sales_value,sales_quarter,sales_quarter,sales_year,sec_1 FROM putty_q
		
		UNION ALL
		SELECT customer_name,customer_code,sales_value,sales_year,sales_year,sales_year,sec_1 FROM actual_y
		UNION ALL
		SELECT customer_name,customer_code,sales_target,target_year,target_year,target_year,sec_1 FROM target_y
		UNION ALL
		SELECT customer_name,customer_code,vs_target_rate,sales_year,sales_year,sales_year,sec_1 FROM vs_target_y
		UNION ALL
		SELECT customer_name,customer_code,actual_prior_sales,sales_year,sales_year,sales_year,sec_1 FROM actual_prior_y
		UNION ALL
		SELECT customer_name,customer_code,growth_rate,sales_year,sales_year,sales_year,sec_1 FROM growth_y
		UNION ALL
		SELECT customer_name,customer_code,sales_value,sales_year,sales_year,sales_year,sec_1 FROM sundries_y
		UNION ALL
		SELECT customer_name,customer_code,sales_value,sales_year,sales_year,sales_year,sec_1 FROM putty_y
	) t
inner  JOIN ( select * 
             from fine_dw.dw_order_report orderno
             where 
       orderno.report_year = ${mysql_yesterday_d_year} 

             
              and orderno.order_report = 'mm_report' 
             ) orderno
on UPPER(orderno.name_1) = upper(t.sec_1)
and orderno.order_month = t.sales_month
and orderno.sec_year = t.sales_year


where orderno.order_no is not null

and sales_value is not null
