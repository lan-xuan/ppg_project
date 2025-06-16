/*目标表：dm_mm_report
  来源表：
fine_dw.dw_transaction_detail_report
fine_dw.dw_customer_master_list
fine_dw.dw_mm_sales_target
fine_dw.dw_cs_relationship_info
fine_dw.dw_order_report
fine_ods.ods_calendar_info_df 
更新方式：增量更新
更新粒度：年
更新字段：sales_year

*/

-- 28999 28907 -- 28899
with d as (
	SELECT DISTINCT UPPER(district) as district from fine_dw.dw_customer_master_list 
	       where district is not null
	UNION ALL
	SELECT 'NATIONAL' from dual
	UNION ALL
	SELECT '大众仓' from dual
	UNION ALL
	SELECT 'OTHERS' from dual
),cal as 
(
	SELECT 
		DISTINCT
        cal.actual_month sales_month,
        DATE_FORMAT(STR_TO_DATE(CONCAT(cal.actual_month, '01'), '%Y%m%d'), '%b') sales_month_en,
				cal.actual_quarter sales_quarter,
				cal.actual_year sales_year,
				master1.proj_name,
				master1.proj_name_en,
				d.district
    FROM fine_ods.ods_calendar_info_df cal,fine_dw.dw_customer_master_list master1,d
		where master1.channel = 'MM' 
		 and actual_year in (${mysql_yesterday_l_year},${mysql_yesterday_d_year})
)
, cal_q as 
(
	SELECT 
		DISTINCT
--         cal.actual_month sales_month,
				cal.actual_quarter sales_quarter,
				cal.actual_year sales_year,
				master1.proj_name,
				master1.proj_name_en,
				d.district
    FROM fine_ods.ods_calendar_info_df cal,fine_dw.dw_customer_master_list master1,d
		where master1.channel = 'MM' 
   and actual_year  in (${mysql_yesterday_l_year},${mysql_yesterday_d_year})

), temp_dw_transaction_detail_report as(
		select 
		t.channel,
		t.proj_name,
		t.proj_name_en,
		t.report_brand_name,
		case when t.district is null then 'OTHERS' else t.district end as district,
		sum(t.sales_value) as sales_value,
		t.sales_month,
		SUBSTR(t.sales_month,1,4) sales_year
		FROM
		fine_dw.dw_transaction_detail_report t
		WHERE t.channel = 'MM'
		GROUP BY
		t.channel,
		t.sales_month,
		t.proj_name,
		t.proj_name_en,
		t.report_brand_name,
		case when t.district is null then 'OTHERS' else t.district end,
		SUBSTR(t.sales_month,1,4)
		UNION ALL
		select 
		t.channel,
		t.proj_name,
		t.proj_name_en,
		t.report_brand_name,
		case when t.district is null then 'OTHERS' else t.district end as district,
		sum(t.sales_value) as sales_value,
		t.sales_month,
		SUBSTR(t.sales_month,1,4) sales_year
		FROM
		fine_dw.dw_backlog_by_product_line t
		WHERE t.channel = 'MM'
		GROUP BY
		t.channel,
		t.sales_month,
		t.proj_name,
		t.proj_name_en,
		t.report_brand_name,
		case when t.district is null then 'OTHERS' else t.district end,
		SUBSTR(t.sales_month,1,4)

)
,actual as
(
	SELECT 
		cal.proj_name,
		cal.proj_name_en,
		cal.district,
		sum(p.sales_value) as sales_value,
		cal.sales_month,
		cal.sales_quarter,
		cal.sales_year,
		UPPER(sales_month_en) sec_1-- FY24 01 FY24 ACTUAL
	
	FROM cal
	LEFT JOIN
	(
		select 
		t.proj_name,
		t.proj_name_en,
		case when t.district is null then 'OTHERS' else t.district end as district,
		sum(t.sales_value) as sales_value,
		t.sales_month,
		SUBSTR(t.sales_month,1,4) sales_year
		FROM
		temp_dw_transaction_detail_report t
		WHERE t.channel = 'MM'
		GROUP BY
		t.sales_month,
		t.proj_name,
		t.proj_name_en,
		case when t.district is null then 'OTHERS' else t.district end,
		SUBSTR(t.sales_month,1,4)
		-- CONCAT('Y',SUBSTR(t.sales_month,3,2),' ',SUBSTR(t.sales_month,5,2))
		UNION all
		select 
		t.proj_name,
		t.proj_name_en,
		'NATIONAL' as district,
		sum(t.sales_value) as sales_value,
		t.sales_month,
		SUBSTR(t.sales_month,1,4) sales_year
		FROM
		temp_dw_transaction_detail_report t
		WHERE t.channel = 'MM'
		GROUP BY
		t.sales_month,
		t.proj_name,
		t.proj_name_en,
-- 		t.district,
		SUBSTR(t.sales_month,1,4)
		-- CONCAT('Y',SUBSTR(t.sales_month,3,2),' ',SUBSTR(t.sales_month,5,2))
	)p
			on cal.proj_name = p.proj_name
			and cal.sales_month = p.sales_month
			and cal.district = UPPER(p.district)
	GROUP BY 
		cal.proj_name,
		cal.proj_name_en,
		cal.district,
		cal.sales_month,
		cal.sales_quarter,
		cal.sales_year,
		UPPER(sales_month_en)

), actual_q as(

	SELECT proj_name, proj_name_en, district, sum(sales_value) as sales_value, sales_quarter, sales_year, SUBSTR(sales_quarter,5,2) sec_1 FROM actual GROUP BY proj_name,proj_name_en,district,sales_quarter,sales_year,SUBSTR(sales_quarter,5,2) 
), actual_y as(

	SELECT proj_name, proj_name_en, district, sum(sales_value) as sales_value, sales_year, 'FY' sec_1 FROM actual GROUP BY proj_name,proj_name_en,district,sales_year
)
, target as(
		SELECT
		cal.proj_name,
		cal.proj_name_en,
		'NATIONAL' as district,
		sales_target,
		target_month,
		target_quarter,
		target_year,
		'TARGET' sec_1
		FROM cal
		LEFT JOIN fine_dw.dw_mm_sales_target t
		ON t.proj_name = cal.proj_name
		and target_month = sales_month
		where upper(district) = 'NATIONAL'

)
, target_q as(
		SELECT
		proj_name,
		proj_name_en,
		district,
		sum(sales_target) sales_target,
		target_quarter,
		target_year,
		sec_1
		FROM target
		GROUP BY 
		proj_name,
		proj_name_en,
		district,
		target_quarter,
		target_year,
		sec_1

)
, target_y as(
		SELECT
		proj_name,
		proj_name_en,
		district,
		sum(sales_target) sales_target,
		target_year,
		sec_1
		FROM target
		GROUP BY 
		proj_name,
		proj_name_en,
		district,
		target_year,
		sec_1

)
, vs_target as-- VS TARGET%
(
SELECT
	actual.proj_name,
	actual.proj_name_en,
	actual.district,
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
	on actual.proj_name = target.proj_name
	and actual.sales_month = target.target_month
	and actual.district = target.district
	)
, vs_target_q as-- VS TARGET%
(
SELECT
	actual.proj_name,
	actual.proj_name_en,
	actual.district,
	CASE 
        WHEN sum(actual.sales_value2) = 0 THEN NULL
        ELSE sum(actual.sales_value1) / NULLIF(sum(actual.sales_value2), 0)
    END AS  vs_target_rate,
	actual.sales_quarter,
	actual.sales_year,
	sec_1
	FROM vs_target actual
	GROUP BY 
	actual.proj_name,
	actual.proj_name_en,
	actual.district,
	actual.sales_quarter,
	actual.sales_year,
	sec_1
	)
, vs_target_y as-- VS TARGET%
(
SELECT
	actual.proj_name,
	actual.proj_name_en,
	actual.district,
	CASE 
        WHEN sum(actual.sales_value2) = 0 THEN NULL
        ELSE sum(actual.sales_value1) / NULLIF(sum(actual.sales_value2), 0)
    END AS  vs_target_rate,
	actual.sales_year,
	sec_1
	FROM vs_target actual
	GROUP BY 
	actual.proj_name,
	actual.proj_name_en,
	actual.district,
	actual.sales_year,
	sec_1
	)
, actual_prior as -- FY23 ACTUAL
(
	SELECT
		actual.proj_name,
		actual.proj_name_en,
		actual.district,
	    prior.sales_value  as actual_prior_sales,
		DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(actual.sales_month,'01'), '%Y%m%d'), INTERVAL 1 year), '%Y%m') as sales_month,
	    CONCAT(actual.sales_year-1,SUBSTR(actual.sales_quarter,5,2)) sales_quarter,
	    actual.sales_year-1 as sales_year,
	    'PRIOR' sec_1
	FROM actual
	LEFT JOIN actual prior
	on actual.proj_name = prior.proj_name 
	and DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(actual.sales_month,'01'), '%Y%m%d'), INTERVAL 1 year), '%Y%m') = DATE_FORMAT(STR_TO_DATE(CONCAT(prior.sales_month,'01'), '%Y%m%d'), '%Y%m') 
    and actual.district = prior.district 
	
) -- SELECT * from actual_prior;
,actual_prior_q as -- FY23 ACTUAL
(
	SELECT
		actual.proj_name,actual.proj_name_en,actual.district,sum(actual.actual_prior_sales) as actual_prior_sales,actual.sales_quarter,actual.sales_year,sec_1 FROM actual_prior actual GROUP BY actual.proj_name,actual.proj_name_en,actual.district,actual.sales_quarter,actual.sales_year,sec_1
)  -- SELECT * from actual_prior_q;
,actual_prior_y as -- FY23 ACTUAL
(
	SELECT
		actual.proj_name,actual.proj_name_en,actual.district,sum(actual.actual_prior_sales) as actual_prior_sales,actual.sales_year,sec_1 FROM actual_prior actual GROUP BY actual.proj_name,actual.proj_name_en,actual.district,actual.sales_year,sec_1
)  -- SELECT * from actual_prior_q;
,growth as -- YOY GROWTH%
(

		SELECT
			actual.proj_name,
			actual.proj_name_en,
			actual.district,
			(actual.sales_value-prior.actual_prior_sales) as sales_value1,
			prior.actual_prior_sales as sales_value2,
			(actual.sales_value-prior.actual_prior_sales)/prior.actual_prior_sales as growth_rate,
			actual.sales_month,
			actual.sales_quarter,
			actual.sales_year,
			'YOY GROWTH' sec_1
			FROM actual
			LEFT JOIN actual_prior prior
			on actual.proj_name = prior.proj_name
			-- and actual.sales_month = actual_prior.sales_month
      and DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(actual.sales_month,'01'), '%Y%m%d'), INTERVAL 1 year), '%Y%m') = DATE_FORMAT(STR_TO_DATE(CONCAT(prior.sales_month,'01'), '%Y%m%d'), '%Y%m') 
			and actual.district = prior.district
		
	
) 
,growth_q as -- YOY GROWTH%
(

		SELECT
			actual.proj_name,
			actual.proj_name_en,
			actual.district,
			sum(sales_value1)/sum(sales_value2) as growth_rate,
			actual.sales_quarter,
			actual.sales_year,
			sec_1
			FROM growth actual
			GROUP BY 
			actual.proj_name,
			actual.proj_name_en,
			actual.district,
			actual.sales_quarter,
			actual.sales_year,
			sec_1

		
	
)  
,growth_y as -- YOY GROWTH%
(

		SELECT
			actual.proj_name,
			actual.proj_name_en,
			actual.district,
			sum(sales_value1)/sum(sales_value2) as growth_rate,
			actual.sales_year,
			sec_1
			FROM growth actual
			GROUP BY 
			actual.proj_name,
			actual.proj_name_en,
			actual.district,
			actual.sales_year,
			sec_1
)
,sundries as -- SUNDRIES PUTTY
(
		SELECT 
				cal.proj_name,
				cal.proj_name_en,
				cal.district,
				sum(p.sales_value) as sales_value,
				cal.sales_month,
				cal.sales_quarter,
				cal.sales_year,
				'SUNDRIES' sec_1-- FY24 01 FY24 ACTUAL
			
			FROM cal
			LEFT JOIN
			(
				select 
				t.proj_name,
				t.proj_name_en,
				case when t.district is null then 'OTHERS' else t.district end as district,
				sum(t.sales_value) as sales_value,
				t.sales_month,
				SUBSTR(t.sales_month,1,4) sales_year
				FROM
				temp_dw_transaction_detail_report t
				WHERE t.channel = 'MM'
				and upper(report_brand_name) = 'SUNDRIES'
				GROUP BY
				t.sales_month,
				t.proj_name,
				t.proj_name_en,
				case when t.district is null then 'OTHERS' else t.district end,
				SUBSTR(t.sales_month,1,4)
				-- CONCAT('Y',SUBSTR(t.sales_month,3,2),' ',SUBSTR(t.sales_month,5,2))
				UNION all
				select 
				t.proj_name,
				t.proj_name_en,
				'NATIONAL' as district,
				sum(t.sales_value) as sales_value,
				t.sales_month,
				SUBSTR(t.sales_month,1,4) sales_year
				FROM
				temp_dw_transaction_detail_report t
				WHERE t.channel = 'MM'
				and upper(report_brand_name) = 'SUNDRIES'
				GROUP BY
				t.sales_month,
				t.proj_name,
				t.proj_name_en,
		-- 		t.district,
				SUBSTR(t.sales_month,1,4)
				-- CONCAT('Y',SUBSTR(t.sales_month,3,2),' ',SUBSTR(t.sales_month,5,2))
			)p
					on cal.proj_name = p.proj_name
					and cal.sales_month = p.sales_month
					and cal.district = UPPER(p.district)
			GROUP BY 
				cal.proj_name,
				cal.proj_name_en,
				cal.district,
				cal.sales_month,
				cal.sales_quarter,
				cal.sales_year
)
, sundries_q as (
		SELECT proj_name,proj_name_en,district,sum(sales_value) as sales_value,sales_quarter,sales_year,sec_1 FROM sundries GROUP BY proj_name,proj_name_en,district,sales_quarter,sales_year,sec_1 
)
,sundries_y as (
		SELECT proj_name,proj_name_en,district,sum(sales_value) as sales_value,sales_year,sec_1 FROM sundries GROUP BY proj_name ,proj_name_en, district, sales_year, sec_1 
)
, putty as -- SUNDRIES PUTTY
(
			SELECT 
				cal.proj_name,
				cal.proj_name_en,
				cal.district,
				sum(p.sales_value) as sales_value,
				cal.sales_month,
				cal.sales_quarter,
				cal.sales_year,
				'PUTTY' sec_1-- FY24 01 FY24 ACTUAL
			
			FROM cal
			LEFT JOIN
			(
				select 
				t.proj_name,
				t.proj_name_en,
				case when t.district is null then 'OTHERS' else t.district end as district,
				sum(t.sales_value) as sales_value,
				t.sales_month,
				SUBSTR(t.sales_month,1,4) sales_year
				FROM
				temp_dw_transaction_detail_report t
				WHERE t.channel = 'MM'
				and upper(report_brand_name) = 'PUTTY'
				GROUP BY
				t.sales_month,
				t.proj_name,
				t.proj_name_en,
				case when t.district is null then 'OTHERS' else t.district end,
				SUBSTR(t.sales_month,1,4)
				-- CONCAT('Y',SUBSTR(t.sales_month,3,2),' ',SUBSTR(t.sales_month,5,2))
				UNION all
				select 
				t.proj_name,
				t.proj_name_en,
				'NATIONAL' as district,
				sum(t.sales_value) as sales_value,
				t.sales_month,
				SUBSTR(t.sales_month,1,4) sales_year
				FROM
				temp_dw_transaction_detail_report t
				WHERE t.channel = 'MM'
				and upper(report_brand_name) = 'PUTTY'
				GROUP BY
				t.sales_month,
				t.proj_name,
				t.proj_name_en,
		-- 		t.district,
				SUBSTR(t.sales_month,1,4)
				-- CONCAT('Y',SUBSTR(t.sales_month,3,2),' ',SUBSTR(t.sales_month,5,2))
			)p
					on cal.proj_name = p.proj_name
					and cal.sales_month = p.sales_month
					and cal.district = UPPER(p.district)
			GROUP BY 
				cal.proj_name,
				cal.proj_name_en,
				cal.district,
				cal.sales_month,
				cal.sales_quarter,
				cal.sales_year
)
, putty_q as (
		SELECT 
				proj_name,
				proj_name_en,
				district,
				sum(sales_value) as sales_value,
				sales_quarter,
				sales_year,
				sec_1 

		FROM putty
		GROUP BY

			proj_name,
			proj_name_en,
			district,
			sales_quarter,
			sales_year,
			sec_1 
), putty_y as (
		SELECT 
				proj_name,
				proj_name_en,
				district,
				sum(sales_value) as sales_value,
				sales_year,
				sec_1 

		FROM putty
		GROUP BY
			proj_name,
			proj_name_en,
			district,
			sales_year,
			sec_1 
)
, m as(

	SELECT  -- DISTINCT order_no
			t.proj_name,
			t.proj_name_en,
			t.district,
			t.sales_value,
			t.sales_month,
			t.sales_quarter,
			t.sales_year,
			t.sec_1
	FROM
	(
		SELECT proj_name,proj_name_en,district,sales_value,sales_month,sales_quarter,sales_year,sec_1 FROM actual
		UNION ALL
		SELECT proj_name,proj_name_en,district,sales_target,target_month,target_quarter,target_year,sec_1 FROM target
		UNION ALL
		SELECT proj_name,proj_name_en,district,vs_target_rate,sales_month,sales_quarter,sales_year,sec_1 FROM vs_target
		UNION ALL
		SELECT proj_name,proj_name_en,district,actual_prior_sales,sales_month,sales_quarter,sales_year,sec_1 FROM actual_prior
		UNION ALL
		SELECT proj_name,proj_name_en,district,growth_rate,sales_month,sales_quarter,sales_year,sec_1 FROM growth
		UNION ALL
		SELECT proj_name,proj_name_en,district,sales_value,sales_month,sales_quarter,sales_year,sec_1 FROM sundries
		UNION ALL
		SELECT proj_name,proj_name_en,district,sales_value,sales_month,sales_quarter,sales_year,sec_1 FROM putty

		UNION ALL
		SELECT proj_name,proj_name_en,district,sales_value,sales_quarter,sales_quarter,sales_year,sec_1 FROM actual_q
		UNION ALL
		SELECT proj_name,proj_name_en,district,sales_target,target_quarter,target_quarter,target_year,sec_1 FROM target_q
		UNION ALL
		SELECT proj_name,proj_name_en,district,vs_target_rate,sales_quarter,sales_quarter,sales_year,sec_1 FROM vs_target_q
		UNION ALL
		SELECT proj_name,proj_name_en,district,actual_prior_sales,sales_quarter,sales_quarter,sales_year,sec_1 FROM actual_prior_q
		UNION ALL
		SELECT proj_name,proj_name_en,district,growth_rate,sales_quarter,sales_quarter,sales_year,sec_1 FROM growth_q
		UNION ALL
		SELECT proj_name,proj_name_en,district,sales_value,sales_quarter,sales_quarter,sales_year,sec_1 FROM sundries_q
		UNION ALL
		SELECT proj_name,proj_name_en,district,sales_value,sales_quarter,sales_quarter,sales_year,sec_1 FROM putty_q
		
		UNION ALL
		SELECT proj_name,proj_name_en,district,sales_value,sales_year,sales_year,sales_year,sec_1 FROM actual_y
		UNION ALL
		SELECT proj_name,proj_name_en,district,sales_target,target_year,target_year,target_year,sec_1 FROM target_y
		UNION ALL
		SELECT proj_name,proj_name_en,district,vs_target_rate,sales_year,sales_year,sales_year,sec_1 FROM vs_target_y
		UNION ALL
		SELECT proj_name,proj_name_en,district,actual_prior_sales,sales_year,sales_year,sales_year,sec_1 FROM actual_prior_y
		UNION ALL
		SELECT proj_name,proj_name_en,district,growth_rate,sales_year,sales_year,sales_year,sec_1 FROM growth_y
		UNION ALL
		SELECT proj_name,proj_name_en,district,sales_value,sales_year,sales_year,sales_year,sec_1 FROM sundries_y
		UNION ALL
		SELECT proj_name,proj_name_en,district,sales_value,sales_year,sales_year,sales_year,sec_1 FROM putty_y
		
) t

)


	SELECT  
	-- DISTINCT order_no
			t.proj_name,
			t.proj_name_en,
			cs.sales_person as team_owner,
			cs.sales_person_id as team_owner_id,
			t.district,
			case when orderno.sec_1 in ('VS TARGET%','YOY GROWTH%') then t.sales_value*100 else t.sales_value end as sales_value,
			t.sales_month,
			t.sales_quarter,
			t.sales_year,
			orderno.order_no,
			orderno.sec_1,
			orderno.sec_2,
			orderno.sec_3,
			'fine_dw.dw_transaction_detail_report/fine_dw.dw_backlog_by_product_line/fine_dw.dw_customer_master_list/fine_dw.dw_mm_sales_target/fine_dw.dw_cs_relationship_info/fine_dw.dw_order_report/fine_ods.ods_calendar_info_df' as data_resource,
			 now() as etl_time,
			 orderno.report_year,
			 	 STR_TO_DATE(CONCAT( orderno.report_year,'0101') , '%Y%m%d') as report_date

	FROM(
		SELECT  
				proj_name,
				proj_name_en,
				district,
				sales_value,
				sales_month,
				sales_quarter,
				sales_year,
				sec_1
		FROM m			
	)t

		LEFT JOIN fine_dw.dw_order_report orderno
		on upper(orderno.name_1) = upper(t.sec_1)
		and orderno.order_month = t.sales_month
-- 		and orderno.sec_year = t.sales_year
		and orderno.order_report = 'mm_report'
		LEFT JOIN fine_dw.dw_cs_relationship_info cs
		ON orderno.report_year = cs.s_year
		and t.proj_name = cs.proj_name
		where orderno.order_no is not null
		and sales_value is not null
		and orderno.order_report = 'mm_report'
		and orderno.report_year = ${mysql_yesterday_d_year} -- 具体报表的年
		ORDER BY orderno.order_no